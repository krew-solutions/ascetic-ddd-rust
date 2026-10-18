//! What can go wrong with keys.

use std::fmt;

use ascetic_ddd_session::SessionError;

use crate::domain::SignatureAlgorithm;

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
    /// A version of the key named — by a ciphertext, a signature, or a
    /// request to sign with it — that the holder does not have, or no
    /// longer serves for that operation: a keyring without that version, a
    /// version an operator retired, a version below the one the key signs
    /// from (Transit's `min_encryption_version`), which may still verify.
    NoKeyOfVersion(u32),
    /// The tenant has no signing key of the algorithm.
    SigningKeyNotFound {
        /// The tenant asked for.
        tenant_id: String,
        /// The algorithm asked for.
        algorithm: SignatureAlgorithm,
    },
    /// The tenant has no MAC key for the purpose.
    MacKeyNotFound {
        /// The tenant asked for.
        tenant_id: String,
        /// The purpose asked for.
        purpose: String,
    },
    /// The signature is not the key's over these bytes: tampering, another
    /// message, or another key of the same version.
    InvalidSignature,
    /// An algorithm name this crate does not implement, read from storage.
    UnsupportedAlgorithm(String),
    /// Bytes that are not what they were taken for: a ciphertext too short to
    /// carry its version, a nonce and a tag; a key of the wrong length for
    /// its algorithm; a version that is not a count.
    Malformed(String),
    /// The key exists with other options than the ones asked for — derived
    /// where a plain one was asked, or the reverse — and an option that is
    /// fixed at creation cannot be changed on it.
    Conflict {
        /// The tenant whose key it is.
        tenant_id: String,
        /// What differs.
        detail: String,
    },
    /// The KMS refused the operation under this identity: the policy does
    /// not allow it on this key — wrapping, unwrapping, signing — or the
    /// derivation context is not one the policy admits. The key exists; no
    /// retry under the same identity and context will mend it.
    Forbidden {
        /// The tenant whose key it is.
        tenant_id: String,
        /// The operation refused.
        operation: &'static str,
    },
    /// An option this adapter cannot execute: it has no scheduler for
    /// automatic rotation, say. Named so that it is refused, not ignored.
    Unsupported(&'static str),
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
            Error::NoKeyOfVersion(version) => write!(f, "no key of version {version} at hand"),
            Error::SigningKeyNotFound {
                tenant_id,
                algorithm,
            } => write!(f, "tenant `{tenant_id}` has no {algorithm} signing key"),
            Error::MacKeyNotFound { tenant_id, purpose } => {
                write!(f, "tenant `{tenant_id}` has no MAC key for `{purpose}`")
            }
            Error::InvalidSignature => {
                write!(f, "the signature is not the key's over these bytes")
            }
            Error::UnsupportedAlgorithm(name) => write!(f, "unsupported algorithm `{name}`"),
            Error::Conflict { tenant_id, detail } => {
                write!(f, "tenant `{tenant_id}`'s key exists otherwise: {detail}")
            }
            Error::Forbidden {
                tenant_id,
                operation,
            } => write!(
                f,
                "the KMS refused {operation} on tenant `{tenant_id}`'s key under this identity or context"
            ),
            Error::Unsupported(what) => write!(f, "unsupported here: {what}"),
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
