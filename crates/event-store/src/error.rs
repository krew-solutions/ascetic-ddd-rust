//! What can go wrong in the event store.

use std::fmt;

use ascetic_ddd_session::SessionError;

use crate::domain::StreamId;

/// An error of a codec, boxed: the store does not know what it does.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Errors of the event store.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// Another writer appended to the stream after the position the caller
    /// expected: the stream was loaded, decided on, and changed meanwhile.
    /// Load again and decide again.
    ConcurrentUpdate {
        /// The stream.
        stream: StreamId,
        /// The position the append collided at.
        position: i32,
    },
    /// The stream's data-encryption key could not be had.
    Dek(ascetic_ddd_dek::Error),
    /// The payload could not be encoded or decoded: sealing, inflating,
    /// JSON.
    Codec(BoxError),
    /// An event could not be turned into a record, or a record back into an
    /// event: an event type or version the format does not know.
    Format(BoxError),
    /// The session could not be opened or closed.
    Session(SessionError),
    /// The database refused a statement.
    #[cfg(feature = "pg")]
    Database(tokio_postgres::Error),
    /// A stored value could not be read back: metadata that is not the
    /// event's, a version that does not fit its column.
    Malformed(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::ConcurrentUpdate { stream, position } => write!(
                f,
                "stream {stream} was appended to by another writer at position {position}"
            ),
            Error::Dek(error) => write!(f, "dek: {error}"),
            Error::Codec(error) => write!(f, "codec: {error}"),
            Error::Format(error) => write!(f, "format: {error}"),
            Error::Session(error) => write!(f, "session: {error}"),
            #[cfg(feature = "pg")]
            Error::Database(error) => write!(f, "database: {error}"),
            Error::Malformed(what) => write!(f, "malformed: {what}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Dek(error) => Some(error),
            Error::Codec(error) | Error::Format(error) => Some(error.as_ref()),
            Error::Session(error) => Some(error),
            #[cfg(feature = "pg")]
            Error::Database(error) => Some(error),
            Error::ConcurrentUpdate { .. } | Error::Malformed(_) => None,
        }
    }
}

impl From<ascetic_ddd_dek::Error> for Error {
    fn from(error: ascetic_ddd_dek::Error) -> Self {
        Error::Dek(error)
    }
}

impl From<SessionError> for Error {
    fn from(error: SessionError) -> Self {
        Error::Session(error)
    }
}

#[cfg(feature = "pg")]
impl From<tokio_postgres::Error> for Error {
    fn from(error: tokio_postgres::Error) -> Self {
        Error::Database(error)
    }
}
