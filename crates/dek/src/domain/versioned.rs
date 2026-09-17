//! Ciphers over a resource's DEKs that carry the key version along.
//!
//! What they seal is `version || sealed`: four big-endian bytes, then the
//! sealed bytes in the cipher's own layout — the layout of the Python and
//! Go ports, and of a wrapped key in the KMS. A codec takes either as a
//! [`Cipher`] and sees bytes; the version is read here, in memory, to pick
//! the key, where the KMS reads it to fetch one from its store.

use std::collections::BTreeMap;
use std::fmt;

use ascetic_ddd_kms::domain::VERSION_SIZE;
use ascetic_ddd_kms::{Cipher, Error, Key};

/// One version of a resource's DEK: seals under that version, opens only
/// that. What the write path uses.
pub struct VersionedCipher {
    version: u32,
    cipher: Box<dyn Cipher>,
}

impl VersionedCipher {
    /// `cipher` over DEK version `version`.
    pub fn new(version: u32, cipher: Box<dyn Cipher>) -> Self {
        VersionedCipher { version, cipher }
    }

    /// The version.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// The version and the cipher, apart: what a [`Keyring`] is built from.
    pub fn into_parts(self) -> (u32, Box<dyn Cipher>) {
        (self.version, self.cipher)
    }
}

impl Cipher for VersionedCipher {
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, Error> {
        Ok(stamp(self.version, self.cipher.encrypt(plaintext)?))
    }

    /// Opens what this version sealed; another version's work is
    /// [`Error::WrongKeyVersion`] before the key is tried.
    fn decrypt(&self, sealed: &[u8]) -> Result<Vec<u8>, Error> {
        let (version, sealed) = read(sealed)?;
        if version != self.version {
            return Err(Error::WrongKeyVersion {
                expected: self.version,
                found: version,
            });
        }
        self.cipher.decrypt(sealed)
    }

    fn generate_key(&self) -> Result<Key, Error> {
        self.cipher.generate_key()
    }
}

impl fmt::Debug for VersionedCipher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VersionedCipher")
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

/// Every version of a resource's DEK: seals under the newest, opens
/// whatever version a ciphertext names. What the read path uses.
///
/// Never empty: made with one version, and given more. The newest is held
/// apart from the rest, so sealing needs no lookup that could miss.
pub struct Keyring {
    newest: VersionedCipher,
    older: BTreeMap<u32, Box<dyn Cipher>>,
}

impl Keyring {
    /// A keyring of one version.
    pub fn new(version: u32, cipher: Box<dyn Cipher>) -> Self {
        Keyring {
            newest: VersionedCipher::new(version, cipher),
            older: BTreeMap::new(),
        }
    }

    /// The same keyring with one more version. A newer version becomes the
    /// one sealing is done under; a version already there is replaced.
    pub fn with(mut self, version: u32, cipher: Box<dyn Cipher>) -> Self {
        if version >= self.newest.version {
            let previous =
                std::mem::replace(&mut self.newest, VersionedCipher::new(version, cipher));
            if previous.version < version {
                self.older.insert(previous.version, previous.cipher);
            }
        } else {
            self.older.insert(version, cipher);
        }
        self
    }

    /// The version sealing is done under.
    pub fn newest(&self) -> u32 {
        self.newest.version
    }

    /// Every version, oldest first.
    pub fn versions(&self) -> impl Iterator<Item = u32> + '_ {
        self.older
            .keys()
            .copied()
            .chain(std::iter::once(self.newest.version))
    }
}

impl Cipher for Keyring {
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, Error> {
        self.newest.encrypt(plaintext)
    }

    /// Opens with the version the ciphertext names;
    /// [`Error::NoKeyOfVersion`] when the keyring has none.
    fn decrypt(&self, sealed: &[u8]) -> Result<Vec<u8>, Error> {
        let (version, rest) = read(sealed)?;
        if version == self.newest.version {
            return self.newest.cipher.decrypt(rest);
        }
        self.older
            .get(&version)
            .ok_or(Error::NoKeyOfVersion(version))?
            .decrypt(rest)
    }

    fn generate_key(&self) -> Result<Key, Error> {
        self.newest.generate_key()
    }
}

impl fmt::Debug for Keyring {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Keyring")
            .field("versions", &self.versions().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

/// `version || sealed`.
fn stamp(version: u32, sealed: Vec<u8>) -> Vec<u8> {
    let mut out = Vec::with_capacity(VERSION_SIZE + sealed.len());
    out.extend_from_slice(&version.to_be_bytes());
    out.extend_from_slice(&sealed);
    out
}

/// The version and the rest; bytes too short to name a version are
/// [`Error::Malformed`].
fn read(bytes: &[u8]) -> Result<(u32, &[u8]), Error> {
    let Some((version, rest)) = bytes.split_first_chunk::<VERSION_SIZE>() else {
        return Err(Error::Malformed(format!(
            "{} bytes cannot be versioned: the version alone is {VERSION_SIZE}",
            bytes.len()
        )));
    };
    Ok((u32::from_be_bytes(*version), rest))
}
