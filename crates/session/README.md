# ascetic-ddd-session

Unit of Work for a DDD application: session scopes, nested transactions
(savepoints) and an identity map. A port of `ascetic_ddd.session`, adapted to
Rust rather than transliterated.

**Status: in progress.** Done: the session traits, the identity map, the
observer, the REST and composite sessions, an in-memory session for testing and
the PostgreSQL adapter.

## Design

Two decisions shape the whole crate.

### The domain sees exactly one operation

```rust
pub trait Session: Clone + Sync {
    fn atomic<T, E, F>(&self, scope: F) -> impl Future<Output = Result<T, E>>
    where F: AsyncFnOnce(&Self) -> Result<T, E>, E: From<SessionError>;
}
```

A session is a handle, not a resource: `Clone` gives a second name for the same
connection, identity map and scope flag, and costs a few reference counts.
Requiring it is what makes a composite session expressible — it owns clones of
the sessions its delegates hand out, so its own type carries no lifetime and a
plain borrow can be handed to the scope. Because the scope flag is shared, a
clone cannot open a scope beside the one it was cloned from.

`atomic` is closed under itself: a nested scope hands the closure another
session of the same type, which the implementation turns into a `SAVEPOINT`.
Nothing else is reachable from the domain — not the connection, not the
identity map.

Infrastructure asks for a *capability* instead of downcasting a supertype:

```rust
pub trait PgAccess { fn connection(&self) -> &PgConnection; }

impl<S: Session + PgAccess> OrderRepository<S> for PgOrderRepository { … }
```

Python's `ISession` / `IPgSession` pair plus `extract_connection()` becomes
`Session` plus a `PgAccess` bound, checked by the compiler. A repository that
names only the capability works with any session that offers it: transactional,
flat, instrumented, test.

### Values, not mutation

Sessions are immutable values; a nested scope creates a new one rather than
mutating a counter, and every public method takes `&self`. Mutation is confined
to three places, all in the infrastructure and all behind a lock or an atomic:
the connection (a socket), the savepoint-name counter, and the identity map
(a cache by definition).

Taking `&self` is not only a matter of taste — it lets independent work inside
one scope run concurrently, which `&mut self` forbids:

```rust
session.atomic(async |session| {
    futures::try_join!(lines.save(session, &a), lines.save(session, &b))?;
    Ok::<_, Error>(())
}).await?;
```

## Identity map

Guarantees that one entity is loaded once per session, and — at
`Serializable` — that a *missing* row is queried once too.

```rust
#[derive(Clone, PartialEq, Eq, Hash)]
struct OrderKey(i64);

impl IdentityKey for OrderKey {
    type Entity = Order;    // the key pins the entity type
}

match map.get(&OrderKey(7)) {
    Lookup::Found(order) => Ok(Some(order)),  // already loaded
    Lookup::Absent       => Ok(None),         // known not to exist: no query
    Lookup::Unknown      => query_database(),
}
```

Three points where it differs from the other ports:

* **The key carries the entity type.** Python pairs `(type, id)` at run time;
  Go marks the pairing with a phantom method. Here `type Entity` makes the
  lookup return `Arc<K::Entity>` with no type argument and no cast.
* **Entries are weak, anchored by an LRU window.** An entity stays reachable
  while the domain holds it *or* while it is inside the window. The Go port,
  lacking weak references, degraded this to a plain LRU cache — there an entity
  evicted from the window is gone even though the domain still holds it, which
  breaks the "one instance per session" guarantee. `Weak<T>` restores the
  Python semantics.
* **Three outcomes instead of two exceptions.** `KeyError` and `ObjectNotFound`
  become `Lookup::Unknown` and `Lookup::Absent`, so the caller matches on the
  answer rather than catching.

The isolation level decides what may be remembered, because caching a row the
transaction is not allowed to see twice would manufacture stale reads:

| level | entities | absences |
| --- | --- | --- |
| Read Uncommitted / Read Committed | — | — |
| Repeatable Read | cached | — |
| Serializable | cached | cached |

## Observing a session

Python and Go give every session six signals with `attach` / `detach` /
`observer_id` / `Disposable`. Across the whole Python code base there is exactly
one real `attach` — guarded by a "subscribe once" flag — no `detach` at all, and
nothing subscribes to the session's own signals. The dynamic registry is
capability nobody uses, and its notion of observer identity (the address of a
function object) has no counterpart in Rust.

So a signal is modelled as what it is: a function. A signal with several
subscribers is the composition of several functions, and composition is spelled
with values:

```rust
let pool = PgSessionPool::new(pool).observed_by((Log, Metrics));
```

`AsyncSignal` becomes a method, `CompositeSignal` becomes a tuple or a `Vec`,
and `attach` / `detach` / `Disposable` disappear — the wiring is fixed when the
pool is built. If dynamic subscription is ever needed, a mutable registry
becomes *one implementation* of `SessionObserver` rather than the base
abstraction.

Observers are synchronous and infallible on purpose: they observe, they do not
participate. In the Go port a failing `Notify` aborts the surrounding
transaction, which turns logging into a source of business failures.

## Testing a domain without a database

The layering only pays off if the domain can actually be exercised without
infrastructure, so the tool ships with the crate:

```rust
let pool = MemorySessionPool::new();
let journal = pool.journal();

pool.session(async |session| place_order(&repository, session, order).await).await?;

assert_eq!(journal.entries(), [
    "BEGIN",
    "INSERT INTO orders (id) VALUES (7)",
    "SAVEPOINT sp1",
    "INSERT INTO orders (id) VALUES (7)",
    "RELEASE SAVEPOINT sp1",
    "COMMIT",
]);
```

## REST and composite sessions

A REST session is the same shape with no transaction behind it: it bounds an
identity map and reports itself, and it does not pretend HTTP calls can be
rolled back. The HTTP client is a type parameter, so the crate depends on no
HTTP library — requests are timed by wrapping the call, which replaces
`aiohttp.TraceConfig` and a custom `http.RoundTripper`.

A composite runs two sessions as one, innermost closing first, and nests for
three or more. Capability impls are deliberately *not* provided: a blanket impl
would have to fix a direction and would then take the first delegate that fits,
silently — which is exactly what Python's `__getattr__` does, and getting the
wrong database out of a composite of two is not a failure worth inheriting. The
application names the delegate in a newtype it owns (which the orphan rule
requires anyway).

## PostgreSQL

Behind the `pg` feature, on `tokio-postgres` and `deadpool-postgres`:

```toml
ascetic-ddd-session = { version = "0.1", features = ["pg"] }
```

```rust
let sessions = PgSessionPool::new(pool).observed_by(QueryLog);

sessions.session(async |session| {
    session.atomic(async |session| {
        repository.save(session, &order).await?;      // BEGIN
        session.atomic(async |session| {              // SAVEPOINT sp1
            outbox.publish(session, &event).await
        }).await?;                                    // RELEASE SAVEPOINT sp1
        Ok(())
    }).await                                          // COMMIT
}).await?;
```

The driver's own `Transaction` type is deliberately unused: it borrows the
client mutably, which would force `&mut self` through every signature and rule
out both an immutable session value and concurrent work inside one scope. The
boundary is issued as plain statements — `BEGIN`, `SAVEPOINT spN`, `COMMIT`,
`RELEASE` / `ROLLBACK TO` — which is what the driver would send anyway, while
queries go through `Client`, whose methods take `&self`.

Savepoint names come from a counter shared by the session tree rather than from
the nesting depth: two scopes may be opened at once, and depth alone would give
them the same name.

`&mut self` would let the compiler rule out two scopes living side by side on
one connection; `&self` does not. That case is genuinely broken — the two scopes
share a savepoint stack, and releasing the older one silently destroys the newer
— so it is refused at run time instead:

```rust
session.atomic(async |child| {
    child.atomic(async |_| Ok(())).await?;      // nesting: fine
    session.atomic(async |_| Ok(())).await      // Err(SessionError::ScopeAlreadyOpen)
}).await
```

The flag belongs to one session *value* and is released however the scope ends —
returned, failed early or unwound — so sequential scopes on one session are
unaffected.

A failing `COMMIT` becomes `SessionError::Commit` — the caller believes the work
is durable and it is not. A failing `ROLLBACK` does *not* replace the error that
caused it; the observer has already seen it as a failed statement.

## Testing

```bash
cargo test -p ascetic-ddd-session

# integration tests need a live database and are ignored by default
ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
    cargo test -p ascetic-ddd-session --features pg -- --ignored
```

27 integration tests and 4 documentation tests:

* 19 on the identity map — the 18 from the Python suite plus the LRU-eviction
  case the Go port adds, with two extra cases for weak-reference behaviour that
  only this port can express;
* 13 on the session — nesting, both failure paths, observer notifications,
  identity-map sharing, concurrent work inside one scope, error conversion, and
  the scope guard (refusal, nesting still allowed, sequential scopes allowed,
  release after failure);
* 5 on the REST session — capability access, logical scopes, failure, the
  identity map, the scope guard;
* 5 on the composite — one use case driving both delegates through their
  capabilities, nesting across delegates, rollback of the transactional delegate
  only, the guard, and three delegates composed;
* 6 against a real PostgreSQL — durable nested commit, a savepoint rolled back
  inside a live transaction, full rollback, pipelined statements in one scope,
  the statements the observer actually sees, and two concurrent scopes refused.
