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
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SessionError::Acquire(error) => write!(f, "cannot acquire a connection: {error}"),
            SessionError::Begin(error) => write!(f, "cannot open a scope: {error}"),
            SessionError::Commit(error) => write!(f, "cannot commit a scope: {error}"),
            SessionError::Rollback(error) => write!(f, "cannot roll a scope back: {error}"),
        }
    }
}

impl std::error::Error for SessionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        let (SessionError::Acquire(error)
        | SessionError::Begin(error)
        | SessionError::Commit(error)
        | SessionError::Rollback(error)) = self;
        Some(&**error)
    }
}
