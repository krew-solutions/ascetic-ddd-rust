//! Signatures and MACs made in this process, for the adapters that hold the
//! key bytes themselves: the PostgreSQL adapter, under its master key, and
//! the in-memory keys of `testing`. One place per algorithm, so that the
//! two adapters cannot drift apart: the private key's bytes, the public
//! key's, the signature's — Ed25519's sixty-four, ECDSA's `r || s` with
//! SHA-256 of the message — are the forms [`SignatureAlgorithm`] fixes,
//! the same the Vault adapter asks Transit for.

use ed25519_dalek::{Signer as _, Verifier as _};
use hmac::{Hmac, Mac as _};
use sha2::Sha256;

use crate::domain::{Key, SignatureAlgorithm};
use crate::error::Error;

/// A signing key's halves: the private bytes, wiped when dropped, and the
/// public bytes in the algorithm's form — Ed25519's thirty-two, P-256's
/// SEC1 uncompressed point of sixty-five.
pub(crate) struct KeyPair {
    pub(crate) private: Key,
    pub(crate) public: Vec<u8>,
}

/// A fresh key pair of the algorithm, from the operating system's
/// randomness.
pub(crate) fn generate(algorithm: SignatureAlgorithm) -> Result<KeyPair, Error> {
    let mut seed = [0u8; 32];
    match algorithm {
        SignatureAlgorithm::Ed25519 => {
            getrandom::fill(&mut seed).map_err(Error::Entropy)?;
            Ok(from_seed(algorithm, &seed))
        }
        SignatureAlgorithm::EcdsaP256 => {
            // A scalar is in the field with overwhelming probability; a
            // draw outside it is drawn again.
            loop {
                getrandom::fill(&mut seed).map_err(Error::Entropy)?;
                if let Ok(key) = p256::ecdsa::SigningKey::from_slice(&seed) {
                    return Ok(KeyPair {
                        private: Key::new(seed.to_vec()),
                        public: key.verifying_key().to_sec1_bytes().to_vec(),
                    });
                }
            }
        }
    }
}

/// The key pair an Ed25519 seed makes; the same seed makes the same pair.
pub(crate) fn from_seed(algorithm: SignatureAlgorithm, seed: &[u8; 32]) -> KeyPair {
    debug_assert_eq!(algorithm, SignatureAlgorithm::Ed25519);
    let key = ed25519_dalek::SigningKey::from_bytes(seed);
    KeyPair {
        private: Key::new(seed.to_vec()),
        public: key.verifying_key().to_bytes().to_vec(),
    }
}

/// The signature of `message` by the private key, in the algorithm's form.
pub(crate) fn sign(
    algorithm: SignatureAlgorithm,
    private: &Key,
    message: &[u8],
) -> Result<Vec<u8>, Error> {
    match algorithm {
        SignatureAlgorithm::Ed25519 => {
            let seed: &[u8; 32] = private.as_bytes().try_into().map_err(|_| {
                Error::Malformed("an Ed25519 private key is thirty-two bytes".into())
            })?;
            Ok(ed25519_dalek::SigningKey::from_bytes(seed)
                .sign(message)
                .to_bytes()
                .to_vec())
        }
        SignatureAlgorithm::EcdsaP256 => {
            let key = p256::ecdsa::SigningKey::from_slice(private.as_bytes())
                .map_err(|_| Error::Malformed("a P-256 private key is a scalar".into()))?;
            let signature: p256::ecdsa::Signature = key.sign(message);
            Ok(signature.to_bytes().to_vec())
        }
    }
}

/// Whether `signature` is the public key's over `message`.
pub(crate) fn verify(
    algorithm: SignatureAlgorithm,
    public: &[u8],
    message: &[u8],
    signature: &[u8],
) -> Result<(), Error> {
    match algorithm {
        SignatureAlgorithm::Ed25519 => {
            let public: &[u8; 32] = public.try_into().map_err(|_| {
                Error::Malformed("an Ed25519 public key is thirty-two bytes".into())
            })?;
            let key = ed25519_dalek::VerifyingKey::from_bytes(public)
                .map_err(|_| Error::Malformed("not an Ed25519 public key".into()))?;
            let bytes: &[u8; 64] = signature.try_into().map_err(|_| Error::InvalidSignature)?;
            key.verify(message, &ed25519_dalek::Signature::from_bytes(bytes))
                .map_err(|_| Error::InvalidSignature)
        }
        SignatureAlgorithm::EcdsaP256 => {
            let key = p256::ecdsa::VerifyingKey::from_sec1_bytes(public)
                .map_err(|_| Error::Malformed("not a P-256 public key".into()))?;
            let signature = p256::ecdsa::Signature::from_slice(signature)
                .map_err(|_| Error::InvalidSignature)?;
            key.verify(message, &signature)
                .map_err(|_| Error::InvalidSignature)
        }
    }
}

/// A fresh MAC key: thirty-two bytes from the operating system's randomness.
pub(crate) fn generate_mac_key() -> Result<Key, Error> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(Error::Entropy)?;
    Ok(Key::new(bytes.to_vec()))
}

/// HMAC-SHA-256 of `message` under the key.
pub(crate) fn mac(key: &Key, message: &[u8]) -> Result<Vec<u8>, Error> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key.as_bytes())
        .map_err(|error| Error::Malformed(error.to_string()))?;
    mac.update(message);
    Ok(mac.finalize().into_bytes().to_vec())
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
            let pair = generate(algorithm).unwrap();
            assert_eq!(pair.public.len(), public_len, "{algorithm}");
            let signature = sign(algorithm, &pair.private, b"hello").unwrap();
            assert_eq!(signature.len(), 64, "{algorithm}");
            verify(algorithm, &pair.public, b"hello", &signature).unwrap();
            assert!(matches!(
                verify(algorithm, &pair.public, b"hellp", &signature),
                Err(Error::InvalidSignature)
            ));
            let other = generate(algorithm).unwrap();
            assert!(matches!(
                verify(algorithm, &other.public, b"hello", &signature),
                Err(Error::InvalidSignature)
            ));
        }
    }

    #[test]
    fn a_seed_makes_the_same_ed25519_pair() {
        let a = from_seed(SignatureAlgorithm::Ed25519, &[7u8; 32]);
        let b = from_seed(SignatureAlgorithm::Ed25519, &[7u8; 32]);
        assert_eq!(a.public, b.public);
        assert_eq!(
            sign(SignatureAlgorithm::Ed25519, &a.private, b"x").unwrap(),
            sign(SignatureAlgorithm::Ed25519, &b.private, b"x").unwrap()
        );
    }

    #[test]
    fn a_mac_is_thirty_two_bytes_and_a_function_of_key_and_message() {
        let key = generate_mac_key().unwrap();
        let first = mac(&key, b"hello").unwrap();
        assert_eq!(first.len(), 32);
        assert_eq!(first, mac(&key, b"hello").unwrap());
        assert_ne!(first, mac(&key, b"hellp").unwrap());
        assert_ne!(first, mac(&generate_mac_key().unwrap(), b"hello").unwrap());
    }
}
