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
    /// A version of the key named — by a ciphertext, a signature, a request
    /// to sign with it, or a retirement's bound — that the holder does not
    /// have, or no longer serves for that operation: a keyring without that
    /// version, a version an operator retired, a version below the one the
    /// key signs from (Transit's `min_encryption_version`), which may still
    /// verify; a retirement's bound above the latest version.
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
    /// The key exists with other options than the ones asked for — another
    /// type under the name, derived where a plain one was asked, or the
    /// reverse — and an option that is fixed at creation cannot be changed
    /// on it.
    Conflict {
        /// The tenant whose key it is.
        tenant_id: String,
        /// What differs.
        detail: String,
    },
    /// The KMS refused the operation under this identity: the policy does
    /// not allow it on this key — wrapping, unwrapping, signing, or a
    /// custody operation the identity does not hold — or the derivation
    /// context is not one the policy admits. The key exists, or the
    /// operation refused is its making; no retry under the same identity
    /// and context will mend it.
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
    /// The tables could not be migrated (ADR-0029).
    #[cfg(feature = "pg")]
    Migration(ascetic_ddd_migration::MigrationError),
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

/// What an [`Error`] means for whoever acts on it (seedwork ADR-0039
/// decision 8): the table is this crate's, total over every variant and
/// the fields that tell them apart, so that no caller matches a
/// `#[non_exhaustive]` enum with a `_` arm that would read a variant added
/// later as anything at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    /// The key the operation needs is not there — never made, or destroyed
    /// (crypto-shredding): only a key-encryption key not found at any
    /// version, or a signing or MAC key not found. Asking again finds
    /// nothing until the key is made.
    Gone,
    /// The KMS answered and refused this operation for a reason of the
    /// request, the key's version or the identity's policy: asking again
    /// unchanged is refused again. Whether that refusal is the message's
    /// fault or a deployment's is the caller's to say: at sealing a
    /// policy's drift is mended by a deploy, at opening a ciphertext that
    /// does not open never will.
    Refused,
    /// The KMS, or what reaches it, could not answer now: unreachable, rate
    /// limited, sealed, failing over, a connection the pool could not hand
    /// out. Asking again later may succeed.
    Unavailable,
    /// Something the caller cannot attribute: a refusal Vault did not
    /// explain, a route no mount serves, a database's refusal that is not
    /// of the connection, the operating system's randomness. Of the moment
    /// for a retry's purposes; a defect or a misconfiguration for the
    /// operator's.
    Moment,
}

impl Error {
    /// Whether this is a refusal of the caller's own credentials: a `403`
    /// the key's read did not explain, a token expired or revoked.
    #[must_use]
    pub fn unauthorized(&self) -> bool {
        #[cfg(feature = "vault")]
        if let Error::Vault { status: 403, .. } = self {
            return true;
        }
        false
    }

    /// What this error means for whoever acts on it.
    #[must_use]
    pub fn class(&self) -> Class {
        match self {
            Error::KekNotFound {
                key_version: None, ..
            }
            | Error::SigningKeyNotFound { .. }
            | Error::MacKeyNotFound { .. } => Class::Gone,
            // A version retired or never made: the message's, not the
            // Tenant's — its other keys still serve.
            Error::KekNotFound {
                key_version: Some(_),
                ..
            }
            | Error::Decrypt
            | Error::WrongKeyVersion { .. }
            | Error::NoKeyOfVersion(_)
            | Error::InvalidSignature
            | Error::UnsupportedAlgorithm(_)
            | Error::Malformed(_)
            | Error::Conflict { .. }
            | Error::Forbidden { .. }
            | Error::Unsupported(_) => Class::Refused,
            Error::Entropy(_) => Class::Moment,
            Error::Session(error) => {
                if error.is_transient() {
                    Class::Unavailable
                } else {
                    Class::Moment
                }
            }
            #[cfg(feature = "pg")]
            Error::Database(error) => {
                if ascetic_ddd_session::pg::transient(error) {
                    Class::Unavailable
                } else {
                    Class::Moment
                }
            }
            #[cfg(feature = "pg")]
            Error::Migration(_) => Class::Moment,
            #[cfg(feature = "vault")]
            Error::Transport(_) => Class::Unavailable,
            // 412: a node not yet consistent (Enterprise); 429: a quota;
            // 5xx: sealed, failing over, overloaded.
            #[cfg(feature = "vault")]
            Error::Vault {
                status: 412 | 429 | 500..=599,
                ..
            } => Class::Unavailable,
            #[cfg(feature = "vault")]
            Error::Vault { .. } => Class::Moment,
        }
    }
}

/// An error's text names no tenant and no address (seedwork ADR-0046): it
/// reaches logs, stored failures and, through a host, a client's answer. The
/// tenant stays a field, for code that matches on it.
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::KekNotFound {
                key_version: None, ..
            } => write!(f, "the tenant has no key-encryption key"),
            Error::KekNotFound {
                key_version: Some(version),
                ..
            } => write!(
                f,
                "the tenant has no key-encryption key of version {version}"
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
            Error::SigningKeyNotFound { algorithm, .. } => {
                write!(f, "the tenant has no {algorithm} signing key")
            }
            Error::MacKeyNotFound { purpose, .. } => {
                write!(f, "the tenant has no MAC key for `{purpose}`")
            }
            Error::InvalidSignature => {
                write!(f, "the signature is not the key's over these bytes")
            }
            Error::UnsupportedAlgorithm(name) => write!(f, "unsupported algorithm `{name}`"),
            Error::Conflict { detail, .. } => {
                write!(f, "the tenant's key exists otherwise: {detail}")
            }
            Error::Forbidden { operation, .. } => write!(
                f,
                "the KMS refused {operation} on the tenant's key under this identity or context"
            ),
            Error::Unsupported(what) => write!(f, "unsupported here: {what}"),
            Error::Malformed(what) => write!(f, "malformed: {what}"),
            Error::Entropy(error) => write!(f, "no randomness from the operating system: {error}"),
            Error::Session(error) => write!(f, "session: {error}"),
            #[cfg(feature = "pg")]
            Error::Database(error) => write!(f, "database: {error}"),
            #[cfg(feature = "pg")]
            Error::Migration(error) => write!(f, "{error}"),
            #[cfg(feature = "vault")]
            Error::Transport(error) => write!(f, "vault unreachable: {error}"),
            #[cfg(feature = "vault")]
            // No path: a key's name is its tenant's (seedwork ADR-0042
            // decision 9), and this text reaches logs and stored errors.
            Error::Vault {
                status,
                method,
                message,
                ..
            } => write!(f, "vault: {status} for {method}: {message}"),
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
            #[cfg(feature = "pg")]
            Error::Migration(error) => Some(error),
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The variant an error is, by an exhaustive match: a variant added
    /// without a case below fails to compile here first, then fails the
    /// test's count.
    fn variant(error: &Error) -> usize {
        match error {
            Error::KekNotFound { .. } => 0,
            Error::Decrypt => 1,
            Error::WrongKeyVersion { .. } => 2,
            Error::NoKeyOfVersion(_) => 3,
            Error::SigningKeyNotFound { .. } => 4,
            Error::MacKeyNotFound { .. } => 5,
            Error::InvalidSignature => 6,
            Error::UnsupportedAlgorithm(_) => 7,
            Error::Malformed(_) => 8,
            Error::Conflict { .. } => 9,
            Error::Forbidden { .. } => 10,
            Error::Unsupported(_) => 11,
            Error::Entropy(_) => 12,
            Error::Session(_) => 13,
            #[cfg(feature = "pg")]
            Error::Database(_) => 14,
            #[cfg(feature = "pg")]
            Error::Migration(_) => 15,
            #[cfg(feature = "vault")]
            Error::Transport(_) => 16,
            #[cfg(feature = "vault")]
            Error::Vault { .. } => 17,
        }
    }

    /// No error's text names its tenant (seedwork ADR-0046). The variants
    /// that wrap another library's error — `Entropy`, `Session`, `Database`,
    /// `Migration`, `Transport` — carry no tenant of this crate's; `Vault`'s
    /// words are written over where Transit answers (`vault::without_key_name`,
    /// tested there and through `vault_mapping`). Every variant with a tenant
    /// field is built here.
    #[test]
    fn no_error_s_text_names_its_tenant() {
        const TENANT: &str = "0190a01a-7e5b-7d80-a000-00000000e001";
        let tenant = || TENANT.to_owned();
        let named: Vec<Error> = vec![
            Error::KekNotFound {
                tenant_id: tenant(),
                key_version: None,
            },
            Error::KekNotFound {
                tenant_id: tenant(),
                key_version: Some(2),
            },
            Error::SigningKeyNotFound {
                tenant_id: tenant(),
                algorithm: SignatureAlgorithm::Ed25519,
            },
            Error::MacKeyNotFound {
                tenant_id: tenant(),
                purpose: "p".to_owned(),
            },
            Error::Conflict {
                tenant_id: tenant(),
                detail: "x".to_owned(),
            },
            Error::Forbidden {
                tenant_id: tenant(),
                operation: "datakey",
            },
        ];
        for error in &named {
            assert!(!error.to_string().contains(TENANT), "{error}");
        }
        let with_a_tenant: std::collections::BTreeSet<usize> = named.iter().map(variant).collect();
        assert_eq!(
            with_a_tenant,
            [0, 4, 5, 9, 10].into_iter().collect(),
            "every variant that carries a tenant is built above"
        );
    }

    #[test]
    fn the_class_of_every_error() {
        let tenant = || "t".to_owned();
        let cases: Vec<(Error, Class)> = vec![
            (
                Error::KekNotFound {
                    tenant_id: tenant(),
                    key_version: None,
                },
                Class::Gone,
            ),
            (
                Error::KekNotFound {
                    tenant_id: tenant(),
                    key_version: Some(2),
                },
                Class::Refused,
            ),
            (
                Error::SigningKeyNotFound {
                    tenant_id: tenant(),
                    algorithm: SignatureAlgorithm::Ed25519,
                },
                Class::Gone,
            ),
            (
                Error::MacKeyNotFound {
                    tenant_id: tenant(),
                    purpose: "p".to_owned(),
                },
                Class::Gone,
            ),
            (Error::Decrypt, Class::Refused),
            (
                Error::WrongKeyVersion {
                    expected: 1,
                    found: 2,
                },
                Class::Refused,
            ),
            (Error::NoKeyOfVersion(1), Class::Refused),
            (Error::InvalidSignature, Class::Refused),
            (Error::UnsupportedAlgorithm("x".to_owned()), Class::Refused),
            (Error::Malformed("x".to_owned()), Class::Refused),
            (
                Error::Conflict {
                    tenant_id: tenant(),
                    detail: "x".to_owned(),
                },
                Class::Refused,
            ),
            (
                Error::Forbidden {
                    tenant_id: tenant(),
                    operation: "datakey",
                },
                Class::Refused,
            ),
            (Error::Unsupported("x"), Class::Refused),
            (
                Error::Session(SessionError::Acquire("pool".into())),
                Class::Unavailable,
            ),
            (Error::Session(SessionError::Abandoned), Class::Moment),
        ];
        for (error, class) in cases {
            assert_eq!(error.class(), class, "{error:?}");
        }
    }

    #[cfg(feature = "vault")]
    #[test]
    fn the_class_of_vault_s_answers() {
        let vault = |status| Error::Vault {
            status,
            method: "POST",
            path: "/datakey/plaintext/k".to_owned(),
            message: String::new(),
        };
        for (status, class) in [
            (400, Class::Moment),
            (403, Class::Moment),
            (404, Class::Moment),
            (412, Class::Unavailable),
            (429, Class::Unavailable),
            (500, Class::Unavailable),
            (503, Class::Unavailable),
            (599, Class::Unavailable),
        ] {
            assert_eq!(vault(status).class(), class, "{status}");
        }
        assert_eq!(Error::Transport("reset".into()).class(), Class::Unavailable);
    }
}
