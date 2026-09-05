# ADR-0001: A session is a cloneable handle

## Status

Accepted (2026-09-06).

## Context

Repositories reach infrastructure through capabilities — `S: Session + PgAccess` —
so the application layer never names a concrete session type
(`crates/session/README.md`). A composite session must therefore implement
`Session` itself: a use case is written against one session, and whether that
session is one backend or two is a decision of the composition root.

With the scope receiving a borrow (`F: AsyncFnOnce(&Self)`) and nothing further
required of `Self`, a composite cannot be written. If it owns its delegates, the
nested value built from the borrowed children has type `Composite<&A, &B>`, not
`Self` (`E0308`). If it holds them by reference so that the shapes match, the
children live only for the length of the delegates' scopes:

```text
error[E0521]: borrowed data escapes outside of closure
              `b` is a reference that is only valid in the closure body
```

Two ways out were implemented in full and run against the test suite and a live
PostgreSQL: handing the scope an owned session, and requiring `Clone` of a
session.

## Decision

`Session: Clone + Sync`, and the scope receives `&Self`
(`crates/session/src/session.rs`).

A session is a handle: an `Arc` to the connection, an `Arc` to the identity map,
and a `ScopeFlag` shared by clones. Cloning one costs reference counts, not a
resource.

The composite owns clones of the sessions its delegates hand out
(`crates/session/src/composite.rs`), so its own type carries no lifetime.

## Consequences

- The borrow states what is true of a session's lifetime: it is on loan for the
  scope, not owned by it.
- A session can still be carried out of its scope, but only by writing
  `.clone()`, which is visible where domain code is read. The rejected owned
  form allowed the same by a plain move. There is no gate against it.
- Every implementation of `Session` must be a cheap handle; one that owns a
  resource outright has to put it behind an `Arc` first.
- A clone must share the scope flag, or two scopes run side by side on one
  connection and releasing the older savepoint destroys the newer. `ScopeFlag`
  owns that sharing, so `#[derive(Clone)]` is correct by construction; two tests
  state the rule from both sides (`crates/session/tests/session.rs`).
- One `Arc` allocation per session value.

## Alternatives rejected

**An owned session handed to the scope** (`F: AsyncFnOnce(Self)`,
`Session: Sized`). Implemented and committed as `1812c0e`, then replaced by
`17efecd`. It expresses the composite equally well. Rejected because the type
says the session belongs to the scope, which is not true — it is valid only
until the scope closes — and carrying it out is then an ordinary move that reads
like any other code, where `.clone()` does not. Judgment: the two are otherwise
close; the `&session` this form adds at every repository call (25 sites in the
test suite at the time) was not the deciding argument.

**A borrow with no `Clone`, and no composite** — a `both(a, b, scope)`
combinator instead. The only variant in which the borrow checker makes escape
impossible. Rejected because a composite was required, and because the
combinator puts the number of backends into the application-layer signature: a
use case names every backend it touches, and nothing generic over `Session` can
consume the pair.
