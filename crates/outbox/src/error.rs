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
    /// The tables could not be migrated (ADR-0029).
    Migration(ascetic_ddd_migration::MigrationError),
    /// The transaction already wrote a message under this one's identity to
    /// its channel (ADR-0030): the inbox would keep the first and drop this
    /// one as a redelivery of it. Named from the message refused; the
    /// transaction is aborted, so nothing of it commits.
    IdentityTaken(Identity),
}

/// A message's identity on a channel (`ascetic_ddd_bus::identity`), as its
/// headers spell it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    /// The channel: the destination without its key.
    pub channel: String,
    /// The Tenant.
    pub tenant_id: String,
    /// The stream's type.
    pub stream_type: String,
    /// The stream's id.
    pub stream_id: String,
    /// The position in the stream.
    pub stream_position: String,
}

impl fmt::Display for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Tenant {}, stream {} {} at {}, on `{}`",
            self.tenant_id, self.stream_type, self.stream_id, self.stream_position, self.channel
        )
    }
}

impl Error {
    /// Whether the error is of the moment — a lock cycle the server broke, a
    /// connection lost, a server going down — so that a loop meeting it
    /// waits and goes on, rather than a defect to stop on
    /// (`ascetic_ddd_session::pg::transient`). A subscriber's error is
    /// neither: the loop treats it on its own.
    pub fn is_transient(&self) -> bool {
        match self {
            Error::Session(error) => error.is_transient(),
            Error::Database(error) => ascetic_ddd_session::pg::transient(error),
            Error::Subscriber(_)
            | Error::Malformed(_)
            | Error::Migration(_)
            | Error::IdentityTaken(_) => false,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Session(error) => write!(f, "session: {error}"),
            Error::Database(error) => write!(f, "database: {error}"),
            Error::Subscriber(error) => write!(f, "subscriber: {error}"),
            Error::Malformed(what) => write!(f, "malformed value in the outbox: {what}"),
            Error::Migration(error) => write!(f, "{error}"),
            Error::IdentityTaken(identity) => write!(
                f,
                "the transaction already wrote a message under this one's identity ({identity}): \
                 the inbox would drop it as a repeat of the first"
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Session(error) => Some(error),
            Error::Database(error) => Some(error),
            Error::Subscriber(error) => Some(error.as_ref()),
            Error::Malformed(_) | Error::IdentityTaken(_) => None,
            Error::Migration(error) => Some(error),
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
