//! The primitive: a cipher that seals bytes under a key.
//!
//! [`Cipher`] is the port — seal, open, and make a key for a cipher of the
//! same kind. [`Algorithm`] names the ciphers there are and makes one from a
//! key. [`Aes256Gcm`] is the adapter of the `aes-gcm` crate; everything the
//! algorithm knows — the sizes of its key, nonce and tag, how to draw a key
//! — lives here, taken from the library rather than restated.

use std::fmt;
use std::str::FromStr;

use aes_gcm::KeyInit;
use aes_gcm::aead::{self, Aead, Payload};

use super::key::Key;
use crate::error::Error;

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

    /// A fresh key for a cipher of this algorithm, for when there is no
    /// cipher yet to ask: a master key for a configuration, a key for a test.
    pub fn generate_key(self) -> Result<Key, Error> {
        match self {
            Algorithm::Aes256Gcm => Aes256Gcm::random_key(),
        }
    }

    /// The cipher of this algorithm over `key`, binding what it seals to
    /// `aad`. One arm per algorithm: the adapter is chosen here and nowhere
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

/// Seals bytes under a key and associated data, opens them again, and makes
/// keys for ciphers of its kind.
pub trait Cipher: Send + Sync {
    /// Seals `plaintext` under a fresh nonce. The result opens only with this
    /// cipher's key and associated data.
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, Error>;

    /// Opens what [`Cipher::encrypt`] sealed; [`Error::Decrypt`] for anything
    /// else.
    fn decrypt(&self, sealed: &[u8]) -> Result<Vec<u8>, Error>;

    /// A fresh key for a cipher of this kind, from the operating system.
    /// This cipher's own key plays no part; the kind does — a key made here
    /// is for this algorithm and no other.
    fn generate_key(&self) -> Result<Key, Error>;
}

/// AES-256-GCM over one key and one associated data: the adapter of the
/// `aes-gcm` crate to [`Cipher`].
///
/// The sealed form is `nonce || ciphertext || tag`, the layout the Python and
/// Go ports write.
pub struct Aes256Gcm {
    aead: aes_gcm::Aes256Gcm,
    aad: Vec<u8>,
}

impl Aes256Gcm {
    /// The length of a key, in bytes: what the library's key type holds.
    pub const KEY_SIZE: usize = size_of::<aead::Key<aes_gcm::Aes256Gcm>>();

    /// The length of a nonce, in bytes: what the library's nonce type holds.
    pub const NONCE_SIZE: usize = size_of::<aead::Nonce<aes_gcm::Aes256Gcm>>();

    /// The length of an authentication tag, in bytes: what the library's
    /// tag type holds.
    pub const TAG_SIZE: usize = size_of::<aead::Tag<aes_gcm::Aes256Gcm>>();

    /// The cipher over `key`, binding what it seals to `aad`. Refuses a key
    /// of the wrong length.
    pub fn new(key: &Key, aad: impl Into<Vec<u8>>) -> Result<Self, Error> {
        let aead = aes_gcm::Aes256Gcm::new_from_slice(key.as_bytes()).map_err(|_| {
            Error::Malformed(format!(
                "a key for {} is {} bytes, this one is {}",
                Algorithm::Aes256Gcm,
                Self::KEY_SIZE,
                key.as_bytes().len()
            ))
        })?;
        Ok(Aes256Gcm {
            aead,
            aad: aad.into(),
        })
    }

    /// A fresh key for this algorithm, from the operating system.
    pub(super) fn random_key() -> Result<Key, Error> {
        let mut bytes = vec![0; Self::KEY_SIZE];
        getrandom::fill(&mut bytes).map_err(Error::Entropy)?;
        Ok(Key::new(bytes))
    }

    /// Seals `plaintext` under `nonce`: `nonce || ciphertext || tag`. The pure
    /// half of [`Cipher::encrypt`], for checking against known answers; a
    /// nonce must never be used twice under one key, which is why this is
    /// not public.
    pub(crate) fn encrypt_with(
        &self,
        nonce: [u8; Self::NONCE_SIZE],
        plaintext: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let payload = Payload {
            msg: plaintext,
            aad: &self.aad,
        };
        let sealed = self
            .aead
            .encrypt(&aes_gcm::Nonce::from(nonce), payload)
            .map_err(|_| Error::Malformed("the plaintext is longer than AES-GCM seals".into()))?;
        let mut out = Vec::with_capacity(Self::NONCE_SIZE + sealed.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&sealed);
        Ok(out)
    }
}

fn too_short(len: usize) -> Error {
    Error::Malformed(format!(
        "{len} bytes cannot be sealed: a nonce and a tag alone are {}",
        Aes256Gcm::NONCE_SIZE + Aes256Gcm::TAG_SIZE
    ))
}

impl Cipher for Aes256Gcm {
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, Error> {
        let mut nonce = [0; Self::NONCE_SIZE];
        getrandom::fill(&mut nonce).map_err(Error::Entropy)?;
        self.encrypt_with(nonce, plaintext)
    }

    fn decrypt(&self, sealed: &[u8]) -> Result<Vec<u8>, Error> {
        let Some((nonce, rest)) = sealed.split_first_chunk::<{ Self::NONCE_SIZE }>() else {
            return Err(too_short(sealed.len()));
        };
        if rest.len() < Self::TAG_SIZE {
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

    fn generate_key(&self) -> Result<Key, Error> {
        Aes256Gcm::random_key()
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

    const KNOWN_NONCE: [u8; 12] = [
        0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9, 0xaa, 0xab,
    ];

    #[test]
    fn seals_what_the_python_port_sealed() {
        let cipher = Aes256Gcm::new(&known_key(), "tenant-1").unwrap();
        let sealed = cipher
            .encrypt_with(KNOWN_NONCE, b"the quick brown fox")
            .unwrap();
        assert_eq!(sealed, hex(SEALED_BY_PYTHON));
    }

    #[test]
    fn opens_what_the_python_port_sealed() {
        let cipher = Aes256Gcm::new(&known_key(), "tenant-1").unwrap();
        let opened = cipher.decrypt(&hex(SEALED_BY_PYTHON)).unwrap();
        assert_eq!(opened, b"the quick brown fox");
    }

    #[test]
    fn another_tenant_does_not_open_it() {
        let cipher = Aes256Gcm::new(&known_key(), "tenant-2").unwrap();
        assert!(matches!(
            cipher.decrypt(&hex(SEALED_BY_PYTHON)),
            Err(Error::Decrypt)
        ));
    }

    #[test]
    fn a_changed_byte_does_not_open() {
        let cipher = Aes256Gcm::new(&known_key(), "tenant-1").unwrap();
        let mut sealed = hex(SEALED_BY_PYTHON);
        sealed[Aes256Gcm::NONCE_SIZE] ^= 1;
        assert!(matches!(cipher.decrypt(&sealed), Err(Error::Decrypt)));
    }

    #[test]
    fn bytes_too_short_to_be_sealed_are_malformed() {
        let cipher = Aes256Gcm::new(&known_key(), "tenant-1").unwrap();
        for len in [
            0,
            Aes256Gcm::NONCE_SIZE,
            Aes256Gcm::NONCE_SIZE + Aes256Gcm::TAG_SIZE - 1,
        ] {
            assert!(
                matches!(cipher.decrypt(&vec![0; len]), Err(Error::Malformed(_))),
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
    fn the_sizes_are_the_library_s() {
        assert_eq!(
            (
                Aes256Gcm::KEY_SIZE,
                Aes256Gcm::NONCE_SIZE,
                Aes256Gcm::TAG_SIZE
            ),
            (32, 12, 16)
        );
    }

    #[test]
    fn a_cipher_makes_keys_of_its_own_kind() {
        let cipher = Aes256Gcm::new(&known_key(), "tenant-1").unwrap();
        let key = cipher.generate_key().unwrap();
        assert_eq!(key.as_bytes().len(), Aes256Gcm::KEY_SIZE);
        assert_ne!(key, known_key());
        assert!(Aes256Gcm::new(&key, "tenant-1").is_ok());
    }

    #[test]
    fn a_key_never_shows_its_bytes() {
        assert_eq!(format!("{:?}", known_key()), "Key(32 bytes)");
    }
}
