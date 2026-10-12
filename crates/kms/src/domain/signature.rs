//! Signatures and MACs under a tenant's keys: the algorithm a signature was
//! made with, the version of the key that made it, and the bytes in the one
//! form this crate fixes per algorithm — so that two adapters never put two
//! encodings of one algorithm side by side.

use std::fmt;

use crate::error::Error;

/// The signature algorithms there are. A signature's wire form is fixed
/// per algorithm: a verifier that reads the algorithm knows the bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SignatureAlgorithm {
    /// RFC 8032 Ed25519 over the message itself, no prehash: 64 bytes.
    Ed25519,
    /// ECDSA over NIST P-256 with SHA-256 of the message: 64 bytes,
    /// `r || s`, never DER.
    EcdsaP256,
}

impl SignatureAlgorithm {
    /// The algorithm's id, stable text: `ed25519`, `ecdsa-p256`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            SignatureAlgorithm::Ed25519 => "ed25519",
            SignatureAlgorithm::EcdsaP256 => "ecdsa-p256",
        }
    }

    /// The algorithm from its id.
    ///
    /// # Errors
    /// [`Error::UnsupportedAlgorithm`] for any other text.
    pub fn parse(text: &str) -> Result<Self, Error> {
        match text {
            "ed25519" => Ok(SignatureAlgorithm::Ed25519),
            "ecdsa-p256" => Ok(SignatureAlgorithm::EcdsaP256),
            other => Err(Error::UnsupportedAlgorithm(other.to_owned())),
        }
    }
}

impl fmt::Display for SignatureAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A signature: the algorithm pinned when it was made, the version of the
/// tenant's key of that algorithm that made it, and the bytes in the
/// algorithm's form.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Signature {
    algorithm: SignatureAlgorithm,
    key_version: u32,
    bytes: Vec<u8>,
}

impl Signature {
    /// A signature as a verifier receives it.
    #[must_use]
    pub fn new(algorithm: SignatureAlgorithm, key_version: u32, bytes: impl Into<Vec<u8>>) -> Self {
        Signature {
            algorithm,
            key_version,
            bytes: bytes.into(),
        }
    }

    /// The algorithm.
    #[must_use]
    pub fn algorithm(&self) -> SignatureAlgorithm {
        self.algorithm
    }

    /// The version of the key that made it.
    #[must_use]
    pub fn key_version(&self) -> u32 {
        self.key_version
    }

    /// The bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The bytes, taken.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

/// A MAC: HMAC-SHA-256, thirty-two bytes, and the version of the key that
/// made it. Checked by recomputation, so it carries no algorithm.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mac {
    key_version: u32,
    bytes: Vec<u8>,
}

impl Mac {
    /// A MAC as the key's holder made it.
    #[must_use]
    pub fn new(key_version: u32, bytes: impl Into<Vec<u8>>) -> Self {
        Mac {
            key_version,
            bytes: bytes.into(),
        }
    }

    /// The version of the key that made it.
    #[must_use]
    pub fn key_version(&self) -> u32 {
        self.key_version
    }

    /// The bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The bytes, taken.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

/// What a MAC key is for, as part of the key's name: letters, digits, `_`
/// and `-`, one to sixty-four of them, parsed once.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct KeyLabel(String);

impl KeyLabel {
    /// The label, checked.
    ///
    /// # Errors
    /// [`Error::Malformed`] when the text is empty, too long, or carries a
    /// character a key name cannot.
    pub fn new(text: impl Into<String>) -> Result<Self, Error> {
        let text = text.into();
        if text.len() > 64 || !is_key_part(&text) {
            return Err(Error::Malformed(format!(
                "a key label is one to sixty-four of letters, digits, `_` and `-`, not `{text}`"
            )));
        }
        Ok(KeyLabel(text))
    }

    /// The text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for KeyLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Whether the text can be one part of a dotted key name: non-empty, of
/// letters, digits, `_` and `-` only — so the parts stay apart.
pub(crate) fn is_key_part(text: &str) -> bool {
    !text.is_empty()
        && text
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_algorithm_reads_back_from_its_id() {
        for algorithm in [SignatureAlgorithm::Ed25519, SignatureAlgorithm::EcdsaP256] {
            assert_eq!(
                SignatureAlgorithm::parse(algorithm.as_str()).unwrap(),
                algorithm
            );
        }
        assert!(matches!(
            SignatureAlgorithm::parse("rsa-2048"),
            Err(Error::UnsupportedAlgorithm(name)) if name == "rsa-2048"
        ));
    }

    #[test]
    fn a_label_is_one_key_part() {
        assert_eq!(KeyLabel::new("draft-etag").unwrap().as_str(), "draft-etag");
        for bad in ["", "a.b", "a/b", "a b", &"x".repeat(65)] {
            assert!(
                matches!(KeyLabel::new(bad), Err(Error::Malformed(_))),
                "{bad:?}"
            );
        }
    }
}
