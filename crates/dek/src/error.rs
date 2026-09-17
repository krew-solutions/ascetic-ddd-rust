//! What can go wrong with data-encryption keys.

use std::fmt;

use ascetic_ddd_session::SessionError;

use crate::domain::Resource;

/// Errors of the DEK store.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The resource has no DEK, or none of that version.
    DekNotFound {
        /// The resource asked for.
        resource: Resource,
        /// The version asked for; `None` when any would do.
        version: Option<u32>,
    },
    /// The key management service refused, or a cipher over a DEK did.
    Kms(ascetic_ddd_kms::Error),
    /// The session could not be opened or closed.
    Session(SessionError),
    /// The database refused a statement.
    #[cfg(feature = "pg")]
    Database(tokio_postgres::Error),
    /// A stored value could not be read back: a version that is not a
    /// count.
    Malformed(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::DekNotFound {
                resource,
                version: None,
            } => write!(f, "{resource} has no data-encryption key"),
            Error::DekNotFound {
                resource,
                version: Some(version),
            } => write!(
                f,
                "{resource} has no data-encryption key of version {version}"
            ),
            Error::Kms(error) => write!(f, "kms: {error}"),
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
            Error::Kms(error) => Some(error),
            Error::Session(error) => Some(error),
            #[cfg(feature = "pg")]
            Error::Database(error) => Some(error),
            Error::DekNotFound { .. } | Error::Malformed(_) => None,
        }
    }
}

impl From<ascetic_ddd_kms::Error> for Error {
    fn from(error: ascetic_ddd_kms::Error) -> Self {
        Error::Kms(error)
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
