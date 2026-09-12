# ADR-0004: The scope receives its session by value, and the session promises `Send`

## Status

Accepted (2026-09-11). Supersedes the borrow decision of ADR-0001; the handle
decision of ADR-0001 stands. Replaces the threads ADR-0003 listed among its
consequences.

## Context

`Session::atomic` and `SessionPool::session` returned `impl Future` without
`Send`. Concrete callers never noticed: the compiler infers `Send` from the
scope. Code generic over the pool, `impl<P: SessionPool>`, sees only what the
trait promises, and both `tokio::spawn` and the bus's `BoxFuture` handlers
demand `Send`. The outbox and inbox channels (ADR-0003) are such code; they ran
their loops on threads blocking on the runtime, and stored inbox messages from
blocking tasks, to work around a proof the contract did not give.

The trait could not promise `Send` because the scope was an `async |tx| …`
closure receiving `&Self`: the future of an async closure has no nameable type
on stable Rust, and a borrow handed to a closure is quantified over every
lifetime, `for<'s>`. Four forms were compiled against the crate and against a
probe of the application's own composition pattern, a newtype over a two-level
composite:

| form | outcome |
| --- | --- |
| borrow, future named through a helper trait (diesel-async's `AsyncFunc`) | leaves compile; the newtype over a composite fails: rustc #100013, "`Send` is not general enough" |
| session by value, `FnOnce(Self) -> Fut` | the pool fails once the closure captures references: "one type is more general than the other" |
| helper trait over the pool's `Self::Session` | the projection does not normalize inside impls |
| transaction as a guard value, `begin` / `commit` | compiles; rejected below |

## Decision

1. **`atomic` hands the scope its session by value**, a clone of the handle,
   and names the scope's future through `AsyncScope`, so that it can require
   `Send` of it and promise `Send` in return (`crates/session/src/session.rs`):

   ```rust
   fn atomic<T, E, F>(&self, scope: F) -> impl Future<Output = Result<T, E>> + Send
   where
       F: AsyncScope<Self, Result<T, E>, Fut: Send> + Send,
       T: Send,
       E: From<SessionError> + Send;
   ```

   `AsyncScope<T, R>: AsyncFnOnce(T) -> R + FnOnce(T) -> Self::Fut` holds for
   every async closure; nothing implements it by hand. Call sites keep
   `async |tx| { … }`. A repository still takes `&S`, so the session is passed
   on as `&tx`.

2. **The pool provides `acquire`; `session` is written once in the trait**, as
   the bracket over it, with `scope_opened` and `scope_closed` for observers:
   `Resource.make(acquire).use(scope)`. A composite pool acquires from each
   delegate. The recursive `open_sessions` and the `open_sessions!` macro go.

3. **The channels are tasks.** The outbox and inbox dispatchers are spawned on
   the runtime; the inbox producer stores directly.

In FP terms the scope is `withTransaction conn (\tx -> …)`: the callback
receives the resource as a value for its whole length, and the bracket
releases it. ADR-0001's objection that a value "says the session belongs to the
scope" is answered by that reading; its own record calls the `&session` at
repository calls "not the deciding argument".

## Consequences

- A scope must be `Send`: an `Rc` held across an `.await` is refused at compile
  time (doc-test on `Session`). The test asserting the opposite is removed.
- `Session: Send + 'static` and `SessionPool: Send + Sync`; results and errors
  are `Send`. One clone of the handle per scope.
- `&tx` where the session is handed to a repository, 26 sites in the tests.
- Pool implementations shrink to `acquire` and two hooks; observer bookkeeping
  lives in one place. Session-scope events keep their order: delegates open
  left to right and close right to left.
- Rule for this crate: a trait that takes an async closure names its future
  through `AsyncScope`; a trait whose closure is over an associated type
  implements the bracket as a provided method over a primitive, because the
  compiler does not normalize `Self::Session` under such a bound in impls.
- Both channels lose their threads and blocking tasks.

## Alternatives rejected

**Transaction as a guard value**, `let tx = session.begin().await?; …;
tx.commit().await?` (sqlx, tokio-postgres). Removes the closure and every bound
above, and gives rollback on drop. Rejected: a transaction boundary in this code
base is a bracket, a function over the scope; a manual `commit()` is not one.

**Keeping `Send` inferred**, with threads in the generic code, the interim. A
thread per subscription and a blocking-pool hop per message to work around a
missing line in a contract, and the wrong shape for every future generic
worker: sagas, bridges.

**The session type as a parameter of the pool**, `SessionPool<S>`. Compiles and
avoids the projection; rejected for the type parameter it puts on every
consumer, `PgOutbox<P, S>`.
