//! The session: the transaction boundary as the domain sees it.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::error::SessionError;

/// An asynchronous scope whose future has a name.
///
/// An `async |session| { … }` closure implements [`AsyncFnOnce`], but the
/// future it returns cannot be named on stable Rust, so nothing can be
/// required of it. The same closure also implements [`FnOnce`] returning that
/// future, and there the type *can* be named. This trait joins the two and
/// calls the future `Fut`, so that a session can require `Fut: Send` and, in
/// return, promise that its own future is `Send` (ADR-0004).
///
/// Implemented for every async closure of the right shape; nothing implements
/// it by hand. The technique is diesel-async's `AsyncFunc`.
pub trait AsyncScope<T, R>:
    AsyncFnOnce(T) -> R + FnOnce(T) -> <Self as AsyncScope<T, R>>::Fut
{
    /// The future the scope returns.
    type Fut: Future<Output = R>;
}

impl<F, T, R, Fut> AsyncScope<T, R> for F
where
    F: AsyncFnOnce(T) -> R + FnOnce(T) -> Fut,
    Fut: Future<Output = R>,
{
    type Fut = Fut;
}

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
/// lifetime, and the scope receives a clone of it by value. Sharing the scope
/// flag also means a clone cannot open a scope beside the one it was cloned
/// from. See [`composite`][crate::composite] for the shapes a composite takes.
///
/// The scope receives the session **by value** — it is a handle, and the one
/// the scope is given belongs to it for its whole length. That is what lets
/// `atomic` promise a `Send` future: a borrow handed to the scope would have to
/// be quantified over every lifetime, and a future built from such a
/// higher-ranked borrow cannot be shown `Send` through a composite on stable
/// Rust (ADR-0004). A repository still takes `&S`, so pass the session on as
/// `&session`.
///
/// ```
/// # use ascetic_ddd_session::{Session, SessionError};
/// # async fn example<S: Session>(session: &S) -> Result<i64, SessionError> {
/// session.atomic(async |session| {
///     // … repositories take `&session` …
///     session.atomic(async |_session| { Ok(()) }).await?;   // nested => SAVEPOINT
///     Ok(42)
/// }).await
/// # }
/// ```
///
/// The future is `Send`, so the scope must be too: a scope holding an `Rc`
/// across an `.await` is refused at compile time.
///
/// ```compile_fail
/// # use ascetic_ddd_session::{Session, SessionError};
/// # async fn example<S: Session>(session: &S) -> Result<(), SessionError> {
/// let local = std::rc::Rc::new(());
/// session.atomic(async |_session| {
///     let _held_across_await = &local;
///     Ok(())
/// }).await
/// # }
/// ```
pub trait Session: Clone + Send + Sync + 'static {
    /// Runs `scope` inside a transaction, committing it if the scope succeeds
    /// and rolling it back if it fails.
    ///
    /// A nested call opens a savepoint, so a failing nested scope leaves the
    /// surrounding transaction alive.
    ///
    /// The scope chooses its own error type; the session only requires that it
    /// can carry a [`SessionError`], because opening or closing the scope may
    /// fail on its own.
    fn atomic<T, E, F>(&self, scope: F) -> impl Future<Output = Result<T, E>> + Send
    where
        F: AsyncScope<Self, Result<T, E>, Fut: Send> + Send,
        T: Send,
        E: From<SessionError> + Send;
}

/// A source of sessions.
///
/// The counterpart of `ISessionPool`. Implementations provide one primitive,
/// [`acquire`][SessionPool::acquire], and may report a scope opening and
/// closing; [`session`][SessionPool::session], the bracket over them, is
/// written once here. In FP terms this is `Resource.make(acquire).use(scope)`:
/// what is specific to a resource lives with the resource, the bracket lives
/// with the trait (ADR-0004).
///
/// A session scope is not a transaction — call [`Session::atomic`] for that.
pub trait SessionPool: Send + Sync {
    /// The kind of session this pool hands out.
    type Session: Session;

    /// Takes a connection from the pool and wraps it in a session. The
    /// connection returns to the pool when the session is dropped.
    fn acquire(&self) -> impl Future<Output = Result<Self::Session, SessionError>> + Send;

    /// Runs once a session has been acquired, before the scope.
    fn scope_opened(&self) {}

    /// Runs after the scope, with whether it succeeded.
    fn scope_closed(&self, succeeded: bool) {
        let _ = succeeded;
    }

    /// Runs `scope` with a session taken from the pool, then returns the
    /// connection. The session is handed over by value and the future is
    /// `Send`, as in [`Session::atomic`].
    fn session<T, E, F>(&self, scope: F) -> impl Future<Output = Result<T, E>> + Send
    where
        F: AsyncScope<Self::Session, Result<T, E>, Fut: Send> + Send,
        T: Send,
        E: From<SessionError> + Send,
    {
        async move {
            let session = self.acquire().await?;
            self.scope_opened();
            let outcome = scope(session).await;
            self.scope_closed(outcome.is_ok());
            outcome
        }
    }
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
