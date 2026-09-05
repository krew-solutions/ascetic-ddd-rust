//! The session: the transaction boundary as the domain sees it.

use std::sync::Arc;
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

/// Marks a session while a scope opened on it is running.
///
/// `&mut self` would let the compiler refuse a second scope, at the price of an
/// immutable session and of concurrency inside a scope — see the crate
/// documentation. This flag restores the guarantee at run time, and turns a
/// confusing driver error into [`SessionError::ScopeAlreadyOpen`].
///
/// A flag belongs to one session, and **clones of that session share it**: a
/// clone is a second name for the same session, not a way past the guard. That
/// is why the flag is a type of its own rather than a bare `AtomicBool`: an
/// implementation that derives [`Clone`] gets the sharing by construction,
/// where a hand-written clone could have quietly given the copy a flag of its
/// own and let two scopes run side by side.
///
/// A nested scope runs on the session its parent handed out, which carries a
/// flag of its own — so nesting is unaffected.
///
/// ```
/// use ascetic_ddd_session::ScopeFlag;
///
/// #[derive(Clone)]
/// struct MySession {
///     scope_open: ScopeFlag,
/// }
///
/// let session = MySession { scope_open: ScopeFlag::new() };
/// let guard = session.scope_open.acquire().expect("free");
///
/// assert!(session.clone().scope_open.acquire().is_err());
/// drop(guard);
/// assert!(session.clone().scope_open.acquire().is_ok());
/// ```
#[derive(Clone, Debug, Default)]
pub struct ScopeFlag(Arc<AtomicBool>);

impl ScopeFlag {
    /// Creates a flag that is not set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Claims the session, or reports that it is already claimed.
    ///
    /// The claim is released when the returned guard is dropped, however the
    /// scope ended — returned, failed early or unwound.
    pub fn acquire(&self) -> Result<ScopeGuard<'_>, SessionError> {
        self.0
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .map(|_| ScopeGuard(&self.0))
            .map_err(|_| SessionError::ScopeAlreadyOpen)
    }
}

/// Holds a claim on a session for the length of a scope.
///
/// Created by [`ScopeFlag::acquire`].
#[derive(Debug)]
pub struct ScopeGuard<'a>(&'a AtomicBool);

impl Drop for ScopeGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}
