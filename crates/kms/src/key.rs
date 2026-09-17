//! Keys that wrap keys: the master key, and the key-encryption keys it wraps.
//!
//! Every ciphertext made here names the version of the key that made it: four
//! big-endian bytes before the sealed bytes, [`Ciphertext`]. The master key is
//! version [`MASTER_KEY_VERSION`] and wraps key-encryption keys, [`Kek`]; a
//! KEK is versioned per tenant — rotation makes the next version — and wraps
//! data-encryption keys. The tenant's id is the associated data at both
//! levels, so a ciphertext of one tenant does not open under another's key,
//! even a key of the same bytes.

use std::fmt;

use crate::cipher::{Algorithm, Cipher, Key};
use crate::error::Error;

/// The length of the version prefix of a [`Ciphertext`], in bytes.
pub const VERSION_SIZE: usize = 4;

/// The version of every master key.
pub const MASTER_KEY_VERSION: u32 = 1;

/// Sealed bytes, and the version of the key that sealed them.
///
/// On the wire: the version as four big-endian bytes, then the sealed bytes
/// in the cipher's own layout. The layout of the Python and Go ports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ciphertext {
    key_version: u32,
    sealed: Vec<u8>,
}

impl Ciphertext {
    /// `sealed`, sealed under key version `key_version`.
    pub fn new(key_version: u32, sealed: Vec<u8>) -> Self {
        Ciphertext {
            key_version,
            sealed,
        }
    }

    /// The version of the key that sealed the bytes.
    pub fn key_version(&self) -> u32 {
        self.key_version
    }

    /// The sealed bytes, in the cipher's layout.
    pub fn sealed(&self) -> &[u8] {
        &self.sealed
    }

    /// Reads the wire form. Bytes too short to name a version are
    /// [`Error::Malformed`]; whether the rest is sealed anything is known
    /// only when a key opens it.
    pub fn parse(bytes: &[u8]) -> Result<Self, Error> {
        let Some((version, sealed)) = bytes.split_first_chunk::<VERSION_SIZE>() else {
            return Err(Error::Malformed(format!(
                "{} bytes cannot be a ciphertext: the key version alone is {VERSION_SIZE}",
                bytes.len()
            )));
        };
        Ok(Ciphertext {
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
}

/// What a master key and a KEK have in common: a cipher over the key's bytes,
/// bound to a tenant, at a version. The bytes themselves are not kept; the
/// cipher holds what it needs and wipes it when dropped.
struct Material {
    tenant_id: String,
    algorithm: Algorithm,
    version: u32,
    cipher: Box<dyn Cipher>,
}

impl Material {
    fn new(
        tenant_id: impl Into<String>,
        key: &Key,
        algorithm: Algorithm,
        version: u32,
    ) -> Result<Self, Error> {
        let tenant_id = tenant_id.into();
        let cipher = algorithm.cipher(key, tenant_id.as_bytes())?;
        Ok(Material {
            tenant_id,
            algorithm,
            version,
            cipher,
        })
    }

    fn encrypt(&self, plaintext: &[u8]) -> Result<Ciphertext, Error> {
        Ok(Ciphertext::new(
            self.version,
            self.cipher.encrypt(plaintext)?,
        ))
    }

    fn decrypt(&self, ciphertext: &Ciphertext) -> Result<Vec<u8>, Error> {
        if ciphertext.key_version != self.version {
            return Err(Error::WrongKeyVersion {
                expected: self.version,
                found: ciphertext.key_version,
            });
        }
        self.cipher.decrypt(&ciphertext.sealed)
    }
}

impl fmt::Debug for Material {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Material")
            .field("tenant_id", &self.tenant_id)
            .field("algorithm", &self.algorithm)
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

/// The master key as one tenant sees it: the master's bytes bound to that
/// tenant, at version [`MASTER_KEY_VERSION`]. It makes, loads and rotates the
/// tenant's KEKs, and is what wraps them.
#[derive(Debug)]
pub struct MasterKey {
    material: Material,
}

impl MasterKey {
    /// The master key `key`, for `algorithm`, bound to `tenant_id`. Refuses a
    /// key of the wrong length for the algorithm.
    pub fn new(
        tenant_id: impl Into<String>,
        key: &Key,
        algorithm: Algorithm,
    ) -> Result<Self, Error> {
        Ok(MasterKey {
            material: Material::new(tenant_id, key, algorithm, MASTER_KEY_VERSION)?,
        })
    }

    /// The tenant this key is bound to.
    pub fn tenant_id(&self) -> &str {
        &self.material.tenant_id
    }

    /// [`MASTER_KEY_VERSION`].
    pub fn version(&self) -> u32 {
        self.material.version
    }

    /// The algorithm of the key.
    pub fn algorithm(&self) -> Algorithm {
        self.material.algorithm
    }

    /// Seals `plaintext` under this key, for this tenant.
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<Ciphertext, Error> {
        self.material.encrypt(plaintext)
    }

    /// Opens what [`MasterKey::encrypt`] sealed.
    pub fn decrypt(&self, ciphertext: &Ciphertext) -> Result<Vec<u8>, Error> {
        self.material.decrypt(ciphertext)
    }

    /// The tenant's first KEK: fresh bytes for `algorithm`, wrapped by this
    /// key, version 1.
    pub fn generate_kek(&self, algorithm: Algorithm) -> Result<Kek, Error> {
        self.wrap(algorithm.generate_key()?, algorithm, 1)
    }

    /// A KEK read back from storage: `encrypted_key` unwrapped by this key.
    pub fn load_kek(
        &self,
        encrypted_key: Ciphertext,
        version: u32,
        algorithm: Algorithm,
    ) -> Result<Kek, Error> {
        let key = Key::new(self.decrypt(&encrypted_key)?);
        Ok(Kek {
            material: Material::new(self.tenant_id(), &key, algorithm, version)?,
            encrypted_key,
        })
    }

    /// The next version of `kek`: fresh bytes for the same algorithm, wrapped
    /// by this key, one version up.
    pub fn rotate_kek(&self, kek: &Kek) -> Result<Kek, Error> {
        let version = kek
            .version()
            .checked_add(1)
            .ok_or_else(|| Error::Malformed(format!("no key version after {}", kek.version())))?;
        self.wrap(kek.algorithm().generate_key()?, kek.algorithm(), version)
    }

    fn wrap(&self, key: Key, algorithm: Algorithm, version: u32) -> Result<Kek, Error> {
        let encrypted_key = self.encrypt(key.as_bytes())?;
        Ok(Kek {
            material: Material::new(self.tenant_id(), &key, algorithm, version)?,
            encrypted_key,
        })
    }
}

/// A key-encryption key: one version of a tenant's key, with the form the
/// master key wrapped it in, which is what storage keeps.
#[derive(Debug)]
pub struct Kek {
    material: Material,
    encrypted_key: Ciphertext,
}

impl Kek {
    /// The tenant this key is bound to.
    pub fn tenant_id(&self) -> &str {
        &self.material.tenant_id
    }

    /// The version, from 1.
    pub fn version(&self) -> u32 {
        self.material.version
    }

    /// The algorithm of the key.
    pub fn algorithm(&self) -> Algorithm {
        self.material.algorithm
    }

    /// The key as the master key wrapped it.
    pub fn encrypted_key(&self) -> &Ciphertext {
        &self.encrypted_key
    }

    /// Seals `plaintext` under this key, for this tenant, naming this
    /// version.
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<Ciphertext, Error> {
        self.material.encrypt(plaintext)
    }

    /// Opens what [`Kek::encrypt`] sealed. A ciphertext of another version is
    /// [`Error::WrongKeyVersion`] before any key is tried.
    pub fn decrypt(&self, ciphertext: &Ciphertext) -> Result<Vec<u8>, Error> {
        self.material.decrypt(ciphertext)
    }
}
