//! The MAC key: one version of a tenant's key for a purpose, making the
//! tags a holder of the same key checks by recomputation.

use std::fmt;

use hmac::{Hmac, Mac as _};
use sha2::Sha256;

use super::key::Key;
use super::signature::{KeyLabel, Mac};
use crate::error::Error;

/// One version of a tenant's MAC key for a purpose: thirty-two bytes,
/// wiped when dropped, making HMAC-SHA-256 tags.
pub struct MacKey {
    tenant_id: String,
    purpose: KeyLabel,
    version: u32,
    key: Key,
}

impl MacKey {
    /// A fresh key for `tenant_id` and `purpose`, version `version`, from
    /// the operating system's randomness.
    ///
    /// # Errors
    /// [`Error::Entropy`] when the operating system gives none.
    pub fn generate(tenant_id: &str, purpose: &KeyLabel, version: u32) -> Result<Self, Error> {
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes).map_err(Error::Entropy)?;
        Ok(Self::load(
            tenant_id,
            purpose,
            version,
            Key::new(bytes.to_vec()),
        ))
    }

    /// The key from its bytes, as storage kept them.
    #[must_use]
    pub fn load(tenant_id: &str, purpose: &KeyLabel, version: u32, key: Key) -> Self {
        MacKey {
            tenant_id: tenant_id.to_owned(),
            purpose: purpose.clone(),
            version,
            key,
        }
    }

    /// The tenant this key belongs to.
    #[must_use]
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// The purpose.
    #[must_use]
    pub fn purpose(&self) -> &KeyLabel {
        &self.purpose
    }

    /// The version, from 1.
    #[must_use]
    pub fn version(&self) -> u32 {
        self.version
    }

    /// The bytes: what storage wraps and keeps.
    #[must_use]
    pub fn key(&self) -> &Key {
        &self.key
    }

    /// The name of a tenant's key for a purpose, the same on every adapter:
    /// `{tenant}.mac.{purpose}` — Transit's key name, and the associated
    /// data the PostgreSQL adapter wraps the key under.
    #[must_use]
    pub fn name_of(tenant_id: &str, purpose: &KeyLabel) -> String {
        format!("{tenant_id}.mac.{purpose}")
    }

    /// This key's name.
    #[must_use]
    pub fn name(&self) -> String {
        Self::name_of(&self.tenant_id, &self.purpose)
    }

    /// The HMAC-SHA-256 of `message` under this key: a [`Mac`] of this
    /// version, thirty-two bytes.
    ///
    /// # Errors
    /// None in practice: HMAC takes a key of any length; the `Result` is the
    /// library's.
    pub fn mac(&self, message: &[u8]) -> Result<Mac, Error> {
        let mut mac = Hmac::<Sha256>::new_from_slice(self.key.as_bytes())
            .map_err(|error| Error::Malformed(error.to_string()))?;
        mac.update(message);
        Ok(Mac::new(self.version, mac.finalize().into_bytes().to_vec()))
    }
}

impl fmt::Debug for MacKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MacKey")
            .field("tenant_id", &self.tenant_id)
            .field("purpose", &self.purpose)
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_mac_is_thirty_two_bytes_of_this_version_and_a_function_of_key_and_message() {
        let purpose = KeyLabel::new("etag").unwrap();
        let key = MacKey::generate("t", &purpose, 2).unwrap();
        let first = key.mac(b"hello").unwrap();
        assert_eq!(first.key_version(), 2);
        assert_eq!(first.as_bytes().len(), 32);
        assert_eq!(first, key.mac(b"hello").unwrap());
        assert_ne!(first.as_bytes(), key.mac(b"hellp").unwrap().as_bytes());
        let other = MacKey::generate("t", &purpose, 2).unwrap();
        assert_ne!(first.as_bytes(), other.mac(b"hello").unwrap().as_bytes());
        let loaded = MacKey::load("t", &purpose, 2, key.key().clone());
        assert_eq!(first, loaded.mac(b"hello").unwrap());
        assert_eq!(key.name(), "t.mac.etag");
    }
}
