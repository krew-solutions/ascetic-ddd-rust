//! The session: the transaction boundary as the domain sees it.

use std::sync::atomic::{AtomicBool, Ordering};

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
/// A session is a handle, not a resource: [`Clone`] gives a second name for the
/// same connection, identity map and scope flag, and costs a few reference
/// counts. Requiring it is what makes a composite session expressible — it owns
/// clones of the sessions its delegates hand out, so its own type carries no
/// lifetime and a plain borrow can be handed to the scope. Sharing the scope
/// flag also means a clone cannot open a scope beside the one it was cloned
/// from. See [`CompositeSession`][crate::composite::CompositeSession].
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
pub trait Session: Clone + Sync {
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

/// Refuses a second scope on a session that already has one open.
///
/// `&mut self` would let the compiler rule this out, at the price of an
/// immutable session and of concurrency inside a scope — see the crate
/// documentation. The flag restores the guarantee at run time, and turns a
/// confusing driver error into [`SessionError::ScopeAlreadyOpen`].
///
/// The flag belongs to one session *value*: a nested scope runs on the session
/// the outer scope handed out, which has a flag of its own.
pub(crate) struct ScopeGuard<'a>(&'a AtomicBool);

impl<'a> ScopeGuard<'a> {
    /// Claims the session, or reports that it is already claimed.
    pub(crate) fn acquire(flag: &'a AtomicBool) -> Result<Self, SessionError> {
        flag.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .map(|_| ScopeGuard(flag))
            .map_err(|_| SessionError::ScopeAlreadyOpen)
    }
}

impl Drop for ScopeGuard<'_> {
    /// Releases the session however the scope ended - returned, failed early or
    /// unwound.
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}
