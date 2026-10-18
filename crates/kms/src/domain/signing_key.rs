//! The signing key: one version of a tenant's key of an algorithm, the
//! private half that signs and the public half that verifies.

use std::fmt;

use ed25519_dalek::{Signer as _, Verifier as _};

use super::key::Key;
use super::signature::{Signature, SignatureAlgorithm};
use crate::error::Error;

/// The public half of a signing key, in the algorithm's one form —
/// Ed25519's thirty-two bytes, P-256's SEC1 uncompressed point of
/// sixty-five: what a verifier holds, and what storage keeps beside the
/// wrapped private half.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicKey {
    algorithm: SignatureAlgorithm,
    bytes: Vec<u8>,
}

impl PublicKey {
    /// A public key from its bytes, checked to be one of the algorithm's,
    /// and held in the algorithm's one form: Ed25519's thirty-two bytes;
    /// P-256's uncompressed SEC1 point, `0x04 || x || y`, sixty-five bytes,
    /// whichever SEC1 form it was given in — so that a key's bytes are one
    /// form per algorithm, as a signature's are.
    ///
    /// # Errors
    /// [`Error::Malformed`] when the bytes are not a key of the algorithm.
    pub fn new(algorithm: SignatureAlgorithm, bytes: impl Into<Vec<u8>>) -> Result<Self, Error> {
        let bytes = bytes.into();
        let bytes = match algorithm {
            SignatureAlgorithm::Ed25519 => {
                ed25519_verifying_key(&bytes)?;
                bytes
            }
            SignatureAlgorithm::EcdsaP256 => p256_verifying_key(&bytes)?.to_sec1_bytes().into_vec(),
        };
        Ok(PublicKey { algorithm, bytes })
    }

    /// The algorithm.
    #[must_use]
    pub fn algorithm(&self) -> SignatureAlgorithm {
        self.algorithm
    }

    /// The bytes, in the algorithm's form: Ed25519's thirty-two; P-256's
    /// sixty-five, `0x04` then the coordinates `x` and `y`, big-endian,
    /// thirty-two bytes each — what a JWK's `x` and `y` are cut from.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Whether `signature` is this key's over `message`: `Ok(())` when it
    /// is, [`Error::InvalidSignature`] when it is not — another message,
    /// another key, another algorithm, bytes that are no signature.
    ///
    /// # Errors
    /// [`Error::InvalidSignature`].
    pub fn verify(&self, message: &[u8], signature: &Signature) -> Result<(), Error> {
        if signature.algorithm() != self.algorithm {
            return Err(Error::InvalidSignature);
        }
        match self.algorithm {
            SignatureAlgorithm::Ed25519 => {
                let bytes: &[u8; 64] = signature
                    .as_bytes()
                    .try_into()
                    .map_err(|_| Error::InvalidSignature)?;
                ed25519_verifying_key(&self.bytes)?
                    .verify(message, &ed25519_dalek::Signature::from_bytes(bytes))
                    .map_err(|_| Error::InvalidSignature)
            }
            SignatureAlgorithm::EcdsaP256 => {
                let parsed = p256::ecdsa::Signature::from_slice(signature.as_bytes())
                    .map_err(|_| Error::InvalidSignature)?;
                p256_verifying_key(&self.bytes)?
                    .verify(message, &parsed)
                    .map_err(|_| Error::InvalidSignature)
            }
        }
    }
}

fn ed25519_verifying_key(bytes: &[u8]) -> Result<ed25519_dalek::VerifyingKey, Error> {
    let bytes: &[u8; 32] = bytes
        .try_into()
        .map_err(|_| Error::Malformed("an Ed25519 public key is thirty-two bytes".into()))?;
    ed25519_dalek::VerifyingKey::from_bytes(bytes)
        .map_err(|_| Error::Malformed("not an Ed25519 public key".into()))
}

fn p256_verifying_key(bytes: &[u8]) -> Result<p256::ecdsa::VerifyingKey, Error> {
    p256::ecdsa::VerifyingKey::from_sec1_bytes(bytes)
        .map_err(|_| Error::Malformed("not a P-256 public key".into()))
}

/// One version of a tenant's signing key of an algorithm: the private half
/// — thirty-two bytes either way, an Ed25519 seed or a P-256 scalar — wiped
/// when dropped, and the public half derived from it. Made fresh, from a
/// seed, or loaded from the private half storage kept.
pub struct SigningKey {
    tenant_id: String,
    algorithm: SignatureAlgorithm,
    version: u32,
    private: Key,
    public: PublicKey,
}

impl SigningKey {
    /// A fresh key of `algorithm` for `tenant_id`, version `version`, from
    /// the operating system's randomness.
    ///
    /// # Errors
    /// [`Error::Entropy`] when the operating system gives none.
    pub fn generate(
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        version: u32,
    ) -> Result<Self, Error> {
        let mut seed = [0u8; 32];
        loop {
            getrandom::fill(&mut seed).map_err(Error::Entropy)?;
            // A draw that is not a key of the algorithm — a P-256 scalar
            // outside the field, with negligible probability — is drawn
            // again; an Ed25519 seed always is one.
            if let Ok(key) = Self::from_seed(tenant_id, algorithm, version, &seed) {
                return Ok(key);
            }
        }
    }

    /// The key a seed makes — the same seed, the same key — so that a
    /// test's signatures are the same from run to run. Any seed is an
    /// Ed25519 key; a P-256 seed must be a scalar of the field.
    ///
    /// # Errors
    /// [`Error::Malformed`] when the seed is not a key of the algorithm.
    pub fn from_seed(
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        version: u32,
        seed: &[u8; 32],
    ) -> Result<Self, Error> {
        Self::load(tenant_id, algorithm, version, Key::new(seed.to_vec()))
    }

    /// The key from its private half, as storage kept it; the public half
    /// is derived, not trusted to storage.
    ///
    /// # Errors
    /// [`Error::Malformed`] when the bytes are not a private key of the
    /// algorithm.
    pub fn load(
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        version: u32,
        private: Key,
    ) -> Result<Self, Error> {
        let public = match algorithm {
            SignatureAlgorithm::Ed25519 => ed25519_signing_key(&private)?
                .verifying_key()
                .to_bytes()
                .to_vec(),
            SignatureAlgorithm::EcdsaP256 => p256_signing_key(&private)?
                .verifying_key()
                .to_sec1_bytes()
                .to_vec(),
        };
        Ok(SigningKey {
            tenant_id: tenant_id.to_owned(),
            algorithm,
            version,
            private,
            public: PublicKey {
                algorithm,
                bytes: public,
            },
        })
    }

    /// The tenant this key belongs to.
    #[must_use]
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// The algorithm.
    #[must_use]
    pub fn algorithm(&self) -> SignatureAlgorithm {
        self.algorithm
    }

    /// The version, from 1.
    #[must_use]
    pub fn version(&self) -> u32 {
        self.version
    }

    /// The private half: what storage wraps and keeps.
    #[must_use]
    pub fn private(&self) -> &Key {
        &self.private
    }

    /// The public half.
    #[must_use]
    pub fn public(&self) -> &PublicKey {
        &self.public
    }

    /// The name of a tenant's key of an algorithm, the same on every
    /// adapter: `{tenant}.sign.{algorithm}` — Transit's key name, and the
    /// associated data the PostgreSQL adapter wraps the private half under,
    /// so that a key wrapped for one tenant or algorithm does not open as
    /// another's.
    #[must_use]
    pub fn name_of(tenant_id: &str, algorithm: SignatureAlgorithm) -> String {
        format!("{tenant_id}.sign.{algorithm}")
    }

    /// This key's name.
    #[must_use]
    pub fn name(&self) -> String {
        Self::name_of(&self.tenant_id, self.algorithm)
    }

    /// Signs `message`: a signature of this algorithm and version, in the
    /// algorithm's form — Ed25519's sixty-four bytes, ECDSA's `r || s` with
    /// SHA-256 of the message, never DER.
    ///
    /// # Errors
    /// [`Error::Malformed`] when the private half is not a key of the
    /// algorithm.
    pub fn sign(&self, message: &[u8]) -> Result<Signature, Error> {
        let bytes = match self.algorithm {
            SignatureAlgorithm::Ed25519 => ed25519_signing_key(&self.private)?
                .sign(message)
                .to_bytes()
                .to_vec(),
            SignatureAlgorithm::EcdsaP256 => {
                let signature: p256::ecdsa::Signature =
                    p256_signing_key(&self.private)?.sign(message);
                signature.to_bytes().to_vec()
            }
        };
        Ok(Signature::new(self.algorithm, self.version, bytes))
    }
}

impl fmt::Debug for SigningKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SigningKey")
            .field("tenant_id", &self.tenant_id)
            .field("algorithm", &self.algorithm)
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

fn ed25519_signing_key(private: &Key) -> Result<ed25519_dalek::SigningKey, Error> {
    let seed: &[u8; 32] = private
        .as_bytes()
        .try_into()
        .map_err(|_| Error::Malformed("an Ed25519 private key is thirty-two bytes".into()))?;
    Ok(ed25519_dalek::SigningKey::from_bytes(seed))
}

fn p256_signing_key(private: &Key) -> Result<p256::ecdsa::SigningKey, Error> {
    p256::ecdsa::SigningKey::from_slice(private.as_bytes())
        .map_err(|_| Error::Malformed("a P-256 private key is a scalar of the field".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_algorithm_signs_in_its_form_and_verifies() {
        for (algorithm, public_len) in [
            (SignatureAlgorithm::Ed25519, 32),
            (SignatureAlgorithm::EcdsaP256, 65),
        ] {
            let key = SigningKey::generate("t", algorithm, 3).unwrap();
            assert_eq!(key.public().as_bytes().len(), public_len, "{algorithm}");
            let signature = key.sign(b"hello").unwrap();
            assert_eq!(signature.algorithm(), algorithm);
            assert_eq!(signature.key_version(), 3);
            assert_eq!(signature.as_bytes().len(), 64, "{algorithm}");
            key.public().verify(b"hello", &signature).unwrap();
            assert!(matches!(
                key.public().verify(b"hellp", &signature),
                Err(Error::InvalidSignature)
            ));
            let other = SigningKey::generate("t", algorithm, 3).unwrap();
            assert!(matches!(
                other.public().verify(b"hello", &signature),
                Err(Error::InvalidSignature)
            ));
        }
    }

    #[test]
    fn a_seed_makes_the_same_key_and_a_loaded_half_derives_the_same_public() {
        for algorithm in [SignatureAlgorithm::Ed25519, SignatureAlgorithm::EcdsaP256] {
            let a = SigningKey::from_seed("t", algorithm, 1, &[7u8; 32]).unwrap();
            let b = SigningKey::from_seed("t", algorithm, 1, &[7u8; 32]).unwrap();
            assert_eq!(a.public(), b.public());
            assert_eq!(a.sign(b"x").unwrap(), b.sign(b"x").unwrap());
            let loaded = SigningKey::load("t", algorithm, 1, a.private().clone()).unwrap();
            assert_eq!(loaded.public(), a.public());
            assert_eq!(a.name(), format!("t.sign.{algorithm}"));
        }
    }

    #[test]
    fn a_public_key_is_checked_and_a_signature_of_another_algorithm_is_not_its() {
        assert!(matches!(
            PublicKey::new(SignatureAlgorithm::Ed25519, vec![0u8; 31]),
            Err(Error::Malformed(_))
        ));
        assert!(matches!(
            PublicKey::new(SignatureAlgorithm::EcdsaP256, vec![0u8; 65]),
            Err(Error::Malformed(_))
        ));
        let ed = SigningKey::generate("t", SignatureAlgorithm::Ed25519, 1).unwrap();
        let p = SigningKey::generate("t", SignatureAlgorithm::EcdsaP256, 1).unwrap();
        let signature = p.sign(b"hello").unwrap();
        assert!(matches!(
            ed.public().verify(b"hello", &signature),
            Err(Error::InvalidSignature)
        ));
        let again = PublicKey::new(SignatureAlgorithm::EcdsaP256, p.public().as_bytes()).unwrap();
        again.verify(b"hello", &signature).unwrap();
    }

    #[test]
    fn a_compressed_p256_key_is_held_as_the_same_sixty_five_bytes() {
        let key = SigningKey::generate("t", SignatureAlgorithm::EcdsaP256, 1).unwrap();
        let compressed = p256::ecdsa::VerifyingKey::from_sec1_bytes(key.public().as_bytes())
            .unwrap()
            .to_sec1_point(true)
            .as_bytes()
            .to_vec();
        assert_eq!(compressed.len(), 33);
        let held = PublicKey::new(SignatureAlgorithm::EcdsaP256, compressed).unwrap();
        assert_eq!(held.as_bytes(), key.public().as_bytes());
        assert_eq!(held.as_bytes().len(), 65);
        assert_eq!(held.as_bytes()[0], 0x04);
    }
}
