//! What can go wrong on the bus.

use std::fmt;

/// An error of a transport, boxed: the bus does not name its drivers.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Errors of the bus and its adapters.
#[derive(Debug)]
pub enum Error {
    /// The URI's scheme has no registered adapter, or the URI has no scheme.
    UnknownScheme(String),
    /// The scheme is already bound to an adapter.
    AlreadyRegistered(String),
    /// A second consumer joined the same `(uri, group)` on an adapter that
    /// allows one — a configuration bug in a monolithic deployment.
    AlreadyInGroup {
        /// The topic.
        uri: String,
        /// The consumer group.
        group: String,
    },
    /// The transport failed.
    Transport(BoxError),
    /// A stage of the wire refused the message on its way out.
    Stage(BoxError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::UnknownScheme(scheme) => {
                write!(f, "no adapter registered for scheme `{scheme}`")
            }
            Error::AlreadyRegistered(scheme) => {
                write!(f, "scheme `{scheme}` is already registered")
            }
            Error::AlreadyInGroup { uri, group } => {
                write!(f, "a consumer already exists in group `{group}` on `{uri}`")
            }
            Error::Transport(error) => write!(f, "transport: {error}"),
            Error::Stage(error) => write!(f, "stage: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Transport(error) | Error::Stage(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

/// A failure of handling that no retry will mend — a message that cannot
/// be opened, a key that is gone — as opposed to one of the moment. The bus
/// knows no verdicts of its own; it carries this one from whoever can tell,
/// a stage or a handler, to a transport that can act on it: the inbox parks
/// such a message at once instead of retrying it (ADR-0010).
#[derive(Debug)]
pub struct Permanent(BoxError);

impl Permanent {
    /// Marks `error` as permanent.
    pub fn new(error: impl Into<BoxError>) -> Self {
        Permanent(error.into())
    }

    /// The error itself.
    pub fn into_inner(self) -> BoxError {
        self.0
    }
}

impl fmt::Display for Permanent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "permanent: {}", self.0)
    }
}

impl std::error::Error for Permanent {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
