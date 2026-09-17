//! The master key: wraps a tenant's key-encryption keys.

use super::cipher::{Algorithm, Cipher};
use super::kek::Kek;
use super::key::{Key, WrappedKey};
use crate::error::Error;

/// The version of every master key.
pub const MASTER_KEY_VERSION: u32 = 1;

/// The one key of the system, from configuration. It makes, loads and
/// rotates a tenant's KEKs, and is what wraps them; the tenant is named at
/// each operation, and is the associated data of the wrapping.
pub struct MasterKey {
    key: Key,
    algorithm: Algorithm,
}

impl MasterKey {
    /// The master key `key`, for `algorithm`. Refuses a key of the wrong
    /// length for the algorithm.
    pub fn new(key: Key, algorithm: Algorithm) -> Result<Self, Error> {
        algorithm.cipher(&key, Vec::new())?;
        Ok(MasterKey { key, algorithm })
    }

    /// The algorithm of the key, and so of every KEK it makes.
    pub fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    /// Wraps `key` for `tenant_id`.
    pub fn wrap(&self, tenant_id: &str, key: &Key) -> Result<WrappedKey, Error> {
        WrappedKey::wrap(MASTER_KEY_VERSION, &*self.cipher_for(tenant_id)?, key)
    }

    /// Unwraps what [`MasterKey::wrap`] wrapped for `tenant_id`.
    pub fn unwrap(&self, tenant_id: &str, wrapped: &WrappedKey) -> Result<Key, Error> {
        wrapped.unwrap(MASTER_KEY_VERSION, &*self.cipher_for(tenant_id)?)
    }

    /// The tenant's first KEK: a fresh key of this master key's algorithm,
    /// wrapped by it, version 1.
    pub fn generate_kek(&self, tenant_id: &str) -> Result<Kek, Error> {
        self.make_kek(tenant_id, 1)
    }

    /// A KEK read back from storage: `wrapped` unwrapped by this master key.
    pub fn load_kek(
        &self,
        tenant_id: &str,
        wrapped: WrappedKey,
        version: u32,
        algorithm: Algorithm,
    ) -> Result<Kek, Error> {
        let key = self.unwrap(tenant_id, &wrapped)?;
        Kek::new(tenant_id, version, algorithm, &key, wrapped)
    }

    /// The next version of `kek`: a fresh key of this master key's
    /// algorithm, wrapped by it, one version up.
    pub fn rotate_kek(&self, kek: &Kek) -> Result<Kek, Error> {
        let version = kek
            .version()
            .checked_add(1)
            .ok_or_else(|| Error::Malformed(format!("no key version after {}", kek.version())))?;
        self.make_kek(kek.tenant_id(), version)
    }

    /// The master key as `tenant_id` sees it: bound to the tenant as
    /// associated data.
    fn cipher_for(&self, tenant_id: &str) -> Result<Box<dyn Cipher>, Error> {
        self.algorithm.cipher(&self.key, tenant_id.as_bytes())
    }

    /// A fresh key of this master key's algorithm — made by its cipher, so
    /// of its kind — wrapped for the tenant, as KEK version `version`.
    fn make_kek(&self, tenant_id: &str, version: u32) -> Result<Kek, Error> {
        let cipher = self.cipher_for(tenant_id)?;
        let key = cipher.generate_key()?;
        let wrapped = WrappedKey::wrap(MASTER_KEY_VERSION, &*cipher, &key)?;
        Kek::new(tenant_id, version, self.algorithm, &key, wrapped)
    }
}

impl std::fmt::Debug for MasterKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MasterKey")
            .field("algorithm", &self.algorithm)
            .finish_non_exhaustive()
    }
}
