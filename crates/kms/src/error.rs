//! What can go wrong with keys.

use std::fmt;

use ascetic_ddd_session::SessionError;

/// An error of a transport this crate does not know the inside of, boxed.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Errors of key management.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The tenant has no key-encryption key, or none of that version.
    KekNotFound {
        /// The tenant asked for.
        tenant_id: String,
        /// The version asked for; `None` when any would do.
        key_version: Option<u32>,
    },
    /// The ciphertext did not open: the wrong key, another tenant's, or bytes
    /// changed since it was sealed. Authenticated encryption tells no more
    /// than that, by design.
    Decrypt,
    /// The ciphertext was sealed under another version of the key than the
    /// one asked to open it.
    WrongKeyVersion {
        /// The version of the key that was asked.
        expected: u32,
        /// The version the ciphertext names.
        found: u32,
    },
    /// An algorithm name this crate does not implement, read from storage.
    UnsupportedAlgorithm(String),
    /// Bytes that are not what they were taken for: a ciphertext too short to
    /// carry its version, a nonce and a tag; a key of the wrong length for
    /// its algorithm; a version that is not a count.
    Malformed(String),
    /// The operating system gave no randomness.
    Entropy(getrandom::Error),
    /// The session could not be opened or closed.
    Session(SessionError),
    /// The database refused a statement.
    #[cfg(feature = "pg")]
    Database(tokio_postgres::Error),
    /// Vault could not be reached, or its answer could not be read.
    #[cfg(feature = "vault")]
    Transport(BoxError),
    /// Vault answered, and refused.
    #[cfg(feature = "vault")]
    Vault {
        /// The HTTP status.
        status: u16,
        /// The method of the request refused.
        method: &'static str,
        /// The path of the request refused, under the mount.
        path: String,
        /// What Vault said, joined; empty when it said nothing.
        message: String,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::KekNotFound {
                tenant_id,
                key_version: None,
            } => write!(f, "tenant `{tenant_id}` has no key-encryption key"),
            Error::KekNotFound {
                tenant_id,
                key_version: Some(version),
            } => write!(
                f,
                "tenant `{tenant_id}` has no key-encryption key of version {version}"
            ),
            Error::Decrypt => write!(
                f,
                "the ciphertext did not open: wrong key, wrong tenant, or changed bytes"
            ),
            Error::WrongKeyVersion { expected, found } => write!(
                f,
                "the ciphertext was sealed under key version {found}, this key is version {expected}"
            ),
            Error::UnsupportedAlgorithm(name) => write!(f, "unsupported algorithm `{name}`"),
            Error::Malformed(what) => write!(f, "malformed: {what}"),
            Error::Entropy(error) => write!(f, "no randomness from the operating system: {error}"),
            Error::Session(error) => write!(f, "session: {error}"),
            #[cfg(feature = "pg")]
            Error::Database(error) => write!(f, "database: {error}"),
            #[cfg(feature = "vault")]
            Error::Transport(error) => write!(f, "vault unreachable: {error}"),
            #[cfg(feature = "vault")]
            Error::Vault {
                status,
                method,
                path,
                message,
            } => write!(f, "vault: {status} for {method} {path}: {message}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Entropy(error) => Some(error),
            Error::Session(error) => Some(error),
            #[cfg(feature = "pg")]
            Error::Database(error) => Some(error),
            #[cfg(feature = "vault")]
            Error::Transport(error) => Some(error.as_ref()),
            _ => None,
        }
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
