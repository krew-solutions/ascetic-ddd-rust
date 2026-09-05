//! The session: the transaction boundary as the domain sees it.

use crate::error::SessionError;

/// A unit of work.
///
/// This is everything the application and domain layers know about a session:
/// a single operation that runs a scope inside a transaction. The operation is
/// closed under itself — a nested scope receives another session of the same
/// type — so a saga step, a repository call and a nested savepoint all read the
/// same way.
///
/// The session is an immutable value: `atomic` takes `&self` and hands the
/// scope a *new* session rather than mutating this one. That is what lets
/// independent work inside one scope run concurrently.
///
/// ```
/// # use ascetic_ddd_session::{Session, SessionError};
/// # async fn example<S: Session>(session: &S) -> Result<i64, SessionError> {
/// session.atomic(async |session| {
///     // … repositories take `session` …
///     session.atomic(async |_session| Ok(())).await?;   // вложенный => SAVEPOINT
///     Ok(42)
/// }).await
/// # }
/// ```
pub trait Session: Sync {
    /// Runs `scope` inside a transaction, committing it if the scope succeeds
    /// and rolling it back if it fails.
    ///
    /// A nested call opens a savepoint, so a failing nested scope leaves the
    /// surrounding transaction alive.
    ///
    /// The scope chooses its own error type; the session only requires that it
    /// can carry a [`SessionError`], because opening or closing the scope may
    /// fail on its own.
    fn atomic<T, E, F>(&self, scope: F) -> impl Future<Output = Result<T, E>>
    where
        F: AsyncFnOnce(&Self) -> Result<T, E>,
        E: From<SessionError>;
}

/// A source of sessions.
///
/// The counterpart of `ISessionPool`: it takes a connection from the pool,
/// hands a session to the scope and releases the connection afterwards.
/// A session scope is not a transaction — call [`Session::atomic`] for that.
pub trait SessionPool {
    /// The kind of session this pool hands out.
    type Session: Session;

    /// Runs `scope` with a session taken from the pool.
    fn session<T, E, F>(&self, scope: F) -> impl Future<Output = Result<T, E>>
    where
        F: AsyncFnOnce(&Self::Session) -> Result<T, E>,
        E: From<SessionError>;
}
