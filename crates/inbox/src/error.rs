//! What can go wrong in the inbox.

use std::fmt;

use ascetic_ddd_session::SessionError;

/// An error of a subscriber, boxed: the inbox does not know what it does.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Errors of the inbox.
#[derive(Debug)]
pub enum Error {
    /// The session could not be opened or closed.
    Session(SessionError),
    /// The database refused a statement.
    Database(tokio_postgres::Error),
    /// The subscriber failed; the message stays unprocessed and is retried.
    Subscriber(BoxError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Session(error) => write!(f, "session: {error}"),
            Error::Database(error) => write!(f, "database: {error}"),
            Error::Subscriber(error) => write!(f, "subscriber: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Session(error) => Some(error),
            Error::Database(error) => Some(error),
            Error::Subscriber(error) => Some(error.as_ref()),
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
