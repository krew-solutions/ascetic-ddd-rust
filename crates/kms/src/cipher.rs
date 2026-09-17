//! Ciphers: the algorithm, its key, and AES-256-GCM.
//!
//! A [`Cipher`] seals bytes so that they open only under the same key and the
//! same associated data — here the tenant's id, which binds every ciphertext
//! to its tenant at every level of the key hierarchy. Sealing draws a fresh
//! nonce from the operating system each time; opening is a pure function of
//! the bytes. [`Aes256Gcm::seal`] is the pure half of sealing, given the
//! nonce, so that the implementation can be checked against known answers.

use std::fmt;
use std::str::FromStr;

use aes_gcm::KeyInit;
use aes_gcm::aead::{Aead, Payload};
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::error::Error;

/// A symmetric key: bytes that are wiped when dropped and never printed.
#[derive(Clone, PartialEq, Eq, Zeroize, ZeroizeOnDrop)]
pub struct Key(Vec<u8>);

impl Key {
    /// A key of the given bytes.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Key(bytes.into())
    }

    /// The bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Key({} bytes)", self.0.len())
    }
}

/// The algorithms a key may be for. The name is what storage records.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Algorithm {
    /// AES with a 256-bit key in Galois/Counter Mode: a 96-bit nonce and a
    /// 128-bit tag.
    Aes256Gcm,
}

impl Algorithm {
    /// The name, as storage records it.
    pub const fn as_str(self) -> &'static str {
        match self {
            Algorithm::Aes256Gcm => "AES-256-GCM",
        }
    }

    /// The length of a key, in bytes.
    pub const fn key_size(self) -> usize {
        match self {
            Algorithm::Aes256Gcm => 32,
        }
    }

    /// A fresh key of this algorithm's size, from the operating system.
    pub fn generate_key(self) -> Result<Key, Error> {
        let mut bytes = vec![0; self.key_size()];
        getrandom::fill(&mut bytes).map_err(Error::Entropy)?;
        Ok(Key(bytes))
    }

    /// The cipher of this algorithm over `key`, binding what it seals to
    /// `aad`. One arm per algorithm: the strategy is chosen here and nowhere
    /// else. Refuses a key of the wrong length.
    pub fn cipher(self, key: &Key, aad: impl Into<Vec<u8>>) -> Result<Box<dyn Cipher>, Error> {
        match self {
            Algorithm::Aes256Gcm => Ok(Box::new(Aes256Gcm::new(key, aad)?)),
        }
    }
}

impl fmt::Display for Algorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Algorithm {
    type Err = Error;

    fn from_str(name: &str) -> Result<Self, Error> {
        match name {
            "AES-256-GCM" => Ok(Algorithm::Aes256Gcm),
            other => Err(Error::UnsupportedAlgorithm(other.to_owned())),
        }
    }
}

/// Seals bytes under a key and associated data, and opens them again.
pub trait Cipher: Send + Sync {
    /// Seals `plaintext` under a fresh nonce. The result opens only with this
    /// cipher's key and associated data.
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, Error>;

    /// Opens what [`Cipher::encrypt`] sealed; [`Error::Decrypt`] for anything
    /// else.
    fn decrypt(&self, sealed: &[u8]) -> Result<Vec<u8>, Error>;
}

/// The length of an AES-GCM nonce, in bytes.
pub const NONCE_SIZE: usize = 12;

/// The length of an AES-GCM authentication tag, in bytes.
pub const TAG_SIZE: usize = 16;

/// The 96-bit nonce of AES-GCM.
///
/// Made only from fresh randomness, and moved into the one sealing it is for:
/// there is no way to seal twice under one nonce, which is the one thing
/// AES-GCM demands of its caller.
pub struct Nonce([u8; NONCE_SIZE]);

impl Nonce {
    /// A nonce from the operating system's randomness.
    pub fn fresh() -> Result<Self, Error> {
        let mut bytes = [0; NONCE_SIZE];
        getrandom::fill(&mut bytes).map_err(Error::Entropy)?;
        Ok(Nonce(bytes))
    }

    /// A nonce of known bytes, for checking against known answers only.
    #[cfg(test)]
    pub(crate) fn from_bytes(bytes: [u8; NONCE_SIZE]) -> Self {
        Nonce(bytes)
    }
}

/// AES-256-GCM over one key and one associated data.
///
/// The sealed form is `nonce || ciphertext || tag`, the layout the Python and
/// Go ports write.
pub struct Aes256Gcm {
    aead: aes_gcm::Aes256Gcm,
    aad: Vec<u8>,
}

impl Aes256Gcm {
    /// The cipher over `key`, binding what it seals to `aad`. Refuses a key
    /// that is not thirty-two bytes.
    pub fn new(key: &Key, aad: impl Into<Vec<u8>>) -> Result<Self, Error> {
        let aead = aes_gcm::Aes256Gcm::new_from_slice(key.as_bytes()).map_err(|_| {
            Error::Malformed(format!(
                "a key for {} is {} bytes, this one is {}",
                Algorithm::Aes256Gcm,
                Algorithm::Aes256Gcm.key_size(),
                key.as_bytes().len()
            ))
        })?;
        Ok(Aes256Gcm {
            aead,
            aad: aad.into(),
        })
    }

    /// Seals `plaintext` under `nonce`: `nonce || ciphertext || tag`. A pure
    /// function of its inputs; [`Cipher::encrypt`] is this under a fresh
    /// nonce.
    pub fn seal(&self, nonce: Nonce, plaintext: &[u8]) -> Result<Vec<u8>, Error> {
        let payload = Payload {
            msg: plaintext,
            aad: &self.aad,
        };
        let sealed = self
            .aead
            .encrypt(&aes_gcm::Nonce::from(nonce.0), payload)
            .map_err(|_| Error::Malformed("the plaintext is longer than AES-GCM seals".into()))?;
        let mut out = Vec::with_capacity(NONCE_SIZE + sealed.len());
        out.extend_from_slice(&nonce.0);
        out.extend_from_slice(&sealed);
        Ok(out)
    }

    /// Opens what [`Aes256Gcm::seal`] made. Bytes too short to hold a nonce
    /// and a tag are [`Error::Malformed`]; anything else that does not open
    /// is [`Error::Decrypt`].
    pub fn open(&self, sealed: &[u8]) -> Result<Vec<u8>, Error> {
        let Some((nonce, rest)) = sealed.split_first_chunk::<NONCE_SIZE>() else {
            return Err(too_short(sealed.len()));
        };
        if rest.len() < TAG_SIZE {
            return Err(too_short(sealed.len()));
        }
        let payload = Payload {
            msg: rest,
            aad: &self.aad,
        };
        self.aead
            .decrypt(&aes_gcm::Nonce::from(*nonce), payload)
            .map_err(|_| Error::Decrypt)
    }
}

fn too_short(len: usize) -> Error {
    Error::Malformed(format!(
        "{len} bytes cannot be sealed: a nonce and a tag alone are {}",
        NONCE_SIZE + TAG_SIZE
    ))
}

impl Cipher for Aes256Gcm {
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, Error> {
        self.seal(Nonce::fresh()?, plaintext)
    }

    fn decrypt(&self, sealed: &[u8]) -> Result<Vec<u8>, Error> {
        self.open(sealed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Made with the Python port's `cryptography.hazmat` AES-GCM: key
    /// `00 01 … 1f`, nonce `a0 a1 … ab`, associated data `tenant-1`,
    /// plaintext `the quick brown fox`.
    const SEALED_BY_PYTHON: &str = "a0a1a2a3a4a5a6a7a8a9aaab9270190d34be6bdc0945e5a1680daefe16c321dd95c879131f961ff6e0cb3098c8aa0a";

    fn hex(text: &str) -> Vec<u8> {
        (0..text.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
            .collect()
    }

    fn known_key() -> Key {
        Key::new((0..32).collect::<Vec<u8>>())
    }

    fn known_nonce() -> Nonce {
        Nonce::from_bytes([
            0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9, 0xaa, 0xab,
        ])
    }

    #[test]
    fn seals_what_the_python_port_sealed() {
        let cipher = Aes256Gcm::new(&known_key(), "tenant-1").unwrap();
        let sealed = cipher.seal(known_nonce(), b"the quick brown fox").unwrap();
        assert_eq!(sealed, hex(SEALED_BY_PYTHON));
    }

    #[test]
    fn opens_what_the_python_port_sealed() {
        let cipher = Aes256Gcm::new(&known_key(), "tenant-1").unwrap();
        let opened = cipher.open(&hex(SEALED_BY_PYTHON)).unwrap();
        assert_eq!(opened, b"the quick brown fox");
    }

    #[test]
    fn another_tenant_does_not_open_it() {
        let cipher = Aes256Gcm::new(&known_key(), "tenant-2").unwrap();
        assert!(matches!(
            cipher.open(&hex(SEALED_BY_PYTHON)),
            Err(Error::Decrypt)
        ));
    }

    #[test]
    fn a_changed_byte_does_not_open() {
        let cipher = Aes256Gcm::new(&known_key(), "tenant-1").unwrap();
        let mut sealed = hex(SEALED_BY_PYTHON);
        sealed[NONCE_SIZE] ^= 1;
        assert!(matches!(cipher.open(&sealed), Err(Error::Decrypt)));
    }

    #[test]
    fn bytes_too_short_to_be_sealed_are_malformed() {
        let cipher = Aes256Gcm::new(&known_key(), "tenant-1").unwrap();
        for len in [0, NONCE_SIZE, NONCE_SIZE + TAG_SIZE - 1] {
            assert!(
                matches!(cipher.open(&vec![0; len]), Err(Error::Malformed(_))),
                "{len} bytes"
            );
        }
    }

    #[test]
    fn a_key_of_the_wrong_length_is_refused() {
        assert!(matches!(
            Aes256Gcm::new(&Key::new(vec![0; 16]), "tenant-1"),
            Err(Error::Malformed(_))
        ));
        assert!(matches!(
            Algorithm::Aes256Gcm.cipher(&Key::new(vec![0; 31]), "tenant-1"),
            Err(Error::Malformed(_))
        ));
    }

    #[test]
    fn the_algorithm_name_round_trips_through_storage() {
        assert_eq!(
            Algorithm::Aes256Gcm.as_str().parse::<Algorithm>().unwrap(),
            Algorithm::Aes256Gcm
        );
        assert!(matches!(
            "AES-128-GCM".parse::<Algorithm>(),
            Err(Error::UnsupportedAlgorithm(name)) if name == "AES-128-GCM"
        ));
    }

    #[test]
    fn a_key_never_shows_its_bytes() {
        assert_eq!(format!("{:?}", known_key()), "Key(32 bytes)");
    }
}
