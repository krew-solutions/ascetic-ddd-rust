//! The key-encryption key: one version of a tenant's key, wrapping its DEKs.

use std::fmt;

use super::cipher::{Algorithm, Cipher};
use super::key::{Key, WrappedKey};
use crate::error::Error;

/// One version of a tenant's key-encryption key, with the form the master
/// key wrapped it in, which is what storage keeps.
pub struct Kek {
    tenant_id: String,
    version: u32,
    algorithm: Algorithm,
    cipher: Box<dyn Cipher>,
    wrapped: WrappedKey,
}

impl Kek {
    /// The KEK `key` of `tenant_id`, version `version`, for `algorithm`,
    /// which the master key wrapped as `wrapped`. The bytes are not kept:
    /// the cipher holds what it needs and wipes it when dropped.
    pub(super) fn new(
        tenant_id: &str,
        version: u32,
        algorithm: Algorithm,
        key: &Key,
        wrapped: WrappedKey,
    ) -> Result<Self, Error> {
        Ok(Kek {
            tenant_id: tenant_id.to_owned(),
            version,
            algorithm,
            cipher: algorithm.cipher(key, tenant_id.as_bytes())?,
            wrapped,
        })
    }

    /// The tenant this key belongs to.
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// The version, from 1.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// The algorithm of the key, and so of every DEK it makes.
    pub fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    /// The key as the master key wrapped it.
    pub fn wrapped(&self) -> &WrappedKey {
        &self.wrapped
    }

    /// Wraps `dek`, naming this version.
    pub fn wrap(&self, dek: &Key) -> Result<WrappedKey, Error> {
        WrappedKey::wrap(self.version, &*self.cipher, dek)
    }

    /// Unwraps what [`Kek::wrap`] wrapped. A key wrapped by another version
    /// is [`Error::WrongKeyVersion`] before the cipher is tried.
    pub fn unwrap(&self, wrapped: &WrappedKey) -> Result<Key, Error> {
        wrapped.unwrap(self.version, &*self.cipher)
    }

    /// Wraps again, under this version, what `from` — an earlier version of
    /// the tenant's key — wrapped: what a DEK goes through after a rotation.
    pub fn rewrap(&self, wrapped: &WrappedKey, from: &Kek) -> Result<WrappedKey, Error> {
        self.wrap(&from.unwrap(wrapped)?)
    }

    /// A fresh DEK of this KEK's algorithm — made by its cipher, so of its
    /// kind — in the clear and wrapped by this KEK.
    pub fn generate_dek(&self) -> Result<(Key, WrappedKey), Error> {
        let dek = self.cipher.generate_key()?;
        let wrapped = self.wrap(&dek)?;
        Ok((dek, wrapped))
    }
}

impl fmt::Debug for Kek {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Kek")
            .field("tenant_id", &self.tenant_id)
            .field("version", &self.version)
            .field("algorithm", &self.algorithm)
            .finish_non_exhaustive()
    }
}
