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
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Transport(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}
