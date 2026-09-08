//! What can go wrong in the outbox.

use std::fmt;

use ascetic_ddd_session::SessionError;

/// An error of a subscriber, boxed: the outbox does not know what it does.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Errors of the outbox.
#[derive(Debug)]
pub enum Error {
    /// The session could not be opened or closed.
    Session(SessionError),
    /// The database refused a statement.
    Database(tokio_postgres::Error),
    /// The subscriber failed; the batch is rolled back and redelivered.
    Subscriber(BoxError),
    /// A stored value could not be read back, such as a transaction id that
    /// is not a number.
    Malformed(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Session(error) => write!(f, "session: {error}"),
            Error::Database(error) => write!(f, "database: {error}"),
            Error::Subscriber(error) => write!(f, "subscriber: {error}"),
            Error::Malformed(what) => write!(f, "malformed value in the outbox: {what}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Session(error) => Some(error),
            Error::Database(error) => Some(error),
            Error::Subscriber(error) => Some(error.as_ref()),
            Error::Malformed(_) => None,
        }
    }
}

impl From<SessionError> for Error {
    fn from(error: SessionError) -> Self {
        Error::Session(error)
    }
}

impl From<tokio_postgres::Error> for Error {
    fn from(error: tokio_postgres::Error) -> Self {
        Error::Database(error)
    }
}
