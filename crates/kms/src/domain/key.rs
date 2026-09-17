//! A key in the clear, and a key wrapped by another.

use std::fmt;

use zeroize::{Zeroize, ZeroizeOnDrop};

use super::cipher::Cipher;
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

/// The length of the version prefix of a [`WrappedKey`], in bytes.
pub const VERSION_SIZE: usize = 4;

/// A key wrapped by version `key_version` of another key.
///
/// On the wire: the version as four big-endian bytes, then the sealed bytes
/// in the cipher's own layout. What the KEK table stores as `encrypted_key`
/// and what the port hands out as a wrapped DEK — the layout of the Python
/// and Go ports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WrappedKey {
    key_version: u32,
    sealed: Vec<u8>,
}

impl WrappedKey {
    /// `sealed`, sealed under key version `key_version`.
    pub fn new(key_version: u32, sealed: Vec<u8>) -> Self {
        WrappedKey {
            key_version,
            sealed,
        }
    }

    /// The version of the key that wrapped this one.
    pub fn key_version(&self) -> u32 {
        self.key_version
    }

    /// The sealed bytes, in the cipher's layout.
    pub fn sealed(&self) -> &[u8] {
        &self.sealed
    }

    /// Reads the wire form. Bytes too short to name a version are
    /// [`Error::Malformed`]; whether the rest unwraps is known only when a
    /// key tries.
    pub fn parse(bytes: &[u8]) -> Result<Self, Error> {
        let Some((version, sealed)) = bytes.split_first_chunk::<VERSION_SIZE>() else {
            return Err(Error::Malformed(format!(
                "{} bytes cannot be a wrapped key: the key version alone is {VERSION_SIZE}",
                bytes.len()
            )));
        };
        Ok(WrappedKey {
            key_version: u32::from_be_bytes(*version),
            sealed: sealed.to_vec(),
        })
    }

    /// The wire form.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(VERSION_SIZE + self.sealed.len());
        bytes.extend_from_slice(&self.key_version.to_be_bytes());
        bytes.extend_from_slice(&self.sealed);
        bytes
    }

    /// Wraps `key` with `cipher`, as key version `version` of the wrapping
    /// key.
    pub(super) fn wrap(version: u32, cipher: &dyn Cipher, key: &Key) -> Result<Self, Error> {
        Ok(WrappedKey::new(version, cipher.encrypt(key.as_bytes())?))
    }

    /// Unwraps with `cipher`, which is key version `version` of the wrapping
    /// key: a wrapped key of another version is [`Error::WrongKeyVersion`]
    /// before the cipher is tried.
    pub(super) fn unwrap(&self, version: u32, cipher: &dyn Cipher) -> Result<Key, Error> {
        if self.key_version != version {
            return Err(Error::WrongKeyVersion {
                expected: version,
                found: self.key_version,
            });
        }
        Ok(Key::new(cipher.decrypt(&self.sealed)?))
    }
}
