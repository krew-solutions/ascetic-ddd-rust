//! Errors of the session lifecycle.

use std::fmt;

/// Boxed error reported by the underlying driver.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A failure of the session machinery itself, as opposed to a failure of the
/// work done inside a scope.
///
/// Domain errors never become a `SessionError`: a scope returns its own error
/// type, and the session only requires that it can carry a `SessionError`
/// (`E: From<SessionError>`), because opening or closing a scope can fail on
/// its own.
#[derive(Debug)]
#[non_exhaustive]
pub enum SessionError {
    /// A connection could not be taken from the pool.
    Acquire(BoxError),
    /// `BEGIN` or `SAVEPOINT` failed.
    Begin(BoxError),
    /// `COMMIT` or `RELEASE SAVEPOINT` failed.
    Commit(BoxError),
    /// `ROLLBACK` or `ROLLBACK TO SAVEPOINT` failed.
    Rollback(BoxError),

    /// A second scope was opened on a session that already has one open.
    ///
    /// Nesting is expressed by opening a scope on the session the previous
    /// scope handed out, not on the one that opened it. Two scopes living
    /// side by side on the same connection would share one savepoint stack:
    /// releasing the older one silently destroys the newer, and the newer then
    /// fails to release with a driver error that says nothing about the cause.
    /// Since the session is shared by `&`, the compiler cannot rule this out —
    /// so it is refused at run time instead.
    ScopeAlreadyOpen,
    /// A scope's future was dropped before the scope closed — a timeout, the
    /// losing branch of a `select!` — so its transaction was never committed
    /// or rolled back. The session is poisoned: an enclosing scope is refused
    /// at commit and rolled back, a handle carried out of the dropped scope
    /// refuses new scopes, and the connection is discarded rather than
    /// returned to the pool.
    Abandoned,
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SessionError::Acquire(error) => write!(f, "cannot acquire a connection: {error}"),
            SessionError::Begin(error) => write!(f, "cannot open a scope: {error}"),
            SessionError::Commit(error) => write!(f, "cannot commit a scope: {error}"),
            SessionError::Rollback(error) => write!(f, "cannot roll a scope back: {error}"),
            SessionError::ScopeAlreadyOpen => write!(
                f,
                "a scope is already open on this session: open a nested scope on \
                 the session the outer scope handed out, or use another session",
            ),
            SessionError::Abandoned => write!(
                f,
                "a scope was dropped before it closed: its transaction is abandoned \
                 and the connection is discarded",
            ),
        }
    }
}

impl std::error::Error for SessionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SessionError::Acquire(error)
            | SessionError::Begin(error)
            | SessionError::Commit(error)
            | SessionError::Rollback(error) => Some(&**error),
            SessionError::ScopeAlreadyOpen | SessionError::Abandoned => None,
        }
    }
}
