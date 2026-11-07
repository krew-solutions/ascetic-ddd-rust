//! What can go wrong in the inbox.

use std::fmt;

use ascetic_ddd_session::SessionError;

/// An error of a subscriber, boxed: the inbox does not know what it does.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// What a subscriber says when it fails: whether trying again can help.
///
/// By default it can: any error converts into `Transient`, so `?` keeps
/// working in a subscriber, and the message is tried again after its backoff
/// and parked after its attempts (ADR-0005). `Permanent` is the subscriber's
/// verdict that no retry will ever succeed — a payload it cannot read, an
/// invariant the message breaks — and the message is parked at once,
/// whatever attempts it had left (ADR-0010).
#[derive(Debug)]
pub enum Failure {
    /// Trying again may succeed.
    Transient(BoxError),
    /// Trying again never will.
    Permanent(BoxError),
}

impl Failure {
    /// The verdict that no retry will succeed.
    pub fn permanent(error: impl Into<BoxError>) -> Self {
        Failure::Permanent(error.into())
    }

    /// The subscriber's error.
    pub fn error(&self) -> &BoxError {
        match self {
            Failure::Transient(error) | Failure::Permanent(error) => error,
        }
    }

    /// Whether the subscriber's verdict is that no retry will succeed.
    pub fn is_permanent(&self) -> bool {
        matches!(self, Failure::Permanent(_))
    }
}

impl<E: Into<BoxError>> From<E> for Failure {
    fn from(error: E) -> Self {
        Failure::Transient(error.into())
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.error())
    }
}

/// Errors of the inbox.
#[derive(Debug)]
pub enum Error {
    /// The session could not be opened or closed.
    Session(SessionError),
    /// The database refused a statement.
    Database(tokio_postgres::Error),
    /// The subscriber failed; the message stays unprocessed, and is retried
    /// or parked as the failure says.
    Subscriber(Failure),
    /// The table is not what the inbox expects: a value that cannot be read
    /// back, a row that is gone, a cut other than the configured one.
    Malformed(String),
    /// The tables could not be migrated (ADR-0029).
    Migration(ascetic_ddd_migration::MigrationError),
    /// A message's insert met a conflict whose holder was gone before it
    /// could be read: forgotten by the sweep between the two statements.
    /// The message is tried again, and the retry stores it (ADR-0036 R2).
    Conflict(String),
}

impl Error {
    /// Whether the error is of the moment — a lock cycle the server broke, a
    /// connection lost, a server going down — so that a loop meeting it
    /// waits and goes on, rather than a defect to stop on
    /// (`ascetic_ddd_session::pg::transient`).
    pub fn is_transient(&self) -> bool {
        match self {
            Error::Session(error) => error.is_transient(),
            Error::Database(error) => ascetic_ddd_session::pg::transient(error),
            Error::Conflict(_) => true,
            Error::Subscriber(_) | Error::Malformed(_) | Error::Migration(_) => false,
        }
    }
}

/// The library's words and the server's codes, never text from outside it:
/// a refusal is named by its SQLSTATE and its constraint, without the
/// server's message, whose DETAIL echoes a key's values and whose primary
/// line may quote a refused value; a subscriber's failure is its source,
/// not its text. The text stays reachable through
/// [`std::error::Error::source`], for a host that judges it safe to print
/// (ADR-0036 R9).
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Session(error) => write!(f, "session: {error}"),
            Error::Database(error) => {
                write!(f, "database: {error}")?;
                if let Some(code) = error.code() {
                    write!(f, ", SQLSTATE {}", code.code())?;
                }
                match error.as_db_error().and_then(|db| db.constraint()) {
                    Some(constraint) => write!(f, ", on {constraint}"),
                    None => Ok(()),
                }
            }
            Error::Subscriber(failure) if failure.is_permanent() => {
                f.write_str("the subscriber failed, for good")
            }
            Error::Subscriber(_) => f.write_str("the subscriber failed"),
            Error::Malformed(what) => write!(f, "malformed inbox: {what}"),
            Error::Migration(error) => write!(f, "{error}"),
            Error::Conflict(what) => write!(f, "conflict: {what}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Session(error) => Some(error),
            Error::Database(error) => Some(error),
            Error::Subscriber(failure) => Some(failure.error().as_ref()),
            Error::Malformed(_) | Error::Conflict(_) => None,
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
