//! Keys in memory, for the tests of what consumes the signing and MAC
//! ports: the adapter over them is exercised as it is in production, with
//! the network behind the port and nothing else replaced.
//!
//! A key per tenant and algorithm, registered from a seed so that a test's
//! signatures are the same from run to run (Ed25519), or made from the
//! operating system's randomness on first signing, as Vault would; MAC keys
//! are made on first use. The cryptography is the PostgreSQL adapter's,
//! `software`. Nothing persists past the process.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use ascetic_ddd_session::Session;

use crate::domain::{Key, KeyLabel, Mac, Signature, SignatureAlgorithm};
use crate::error::Error;
use crate::port::{MacService, SigningService};
use crate::software::{self, KeyPair};

/// Keyrings by tenant and `By`: the versions of each key, counted from one.
type Keyrings<K, By> = Mutex<HashMap<(String, By), Vec<K>>>;

/// The tenants' keys: signing keys by tenant and algorithm, MAC keys by
/// tenant and purpose, each a keyring whose versions count from one.
#[derive(Default)]
pub struct InMemoryKeys {
    signing: Keyrings<KeyPair, SignatureAlgorithm>,
    mac: Keyrings<Key, String>,
}

impl std::fmt::Debug for InMemoryKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InMemoryKeys")
    }
}

impl InMemoryKeys {
    /// No keys yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers the next version of the tenant's Ed25519 key, derived from
    /// `seed`; answers the version (one for the first).
    pub fn register(&self, tenant_id: &str, seed: &[u8; 32]) -> u32 {
        self.push(
            tenant_id,
            SignatureAlgorithm::Ed25519,
            software::from_seed(SignatureAlgorithm::Ed25519, seed),
        )
    }

    fn push(&self, tenant_id: &str, algorithm: SignatureAlgorithm, pair: KeyPair) -> u32 {
        let mut keys = self.signing.lock().unwrap_or_else(PoisonError::into_inner);
        let ring = keys.entry((tenant_id.to_owned(), algorithm)).or_default();
        ring.push(pair);
        version_of(ring.len())
    }
}

/// The version a ring of that length is at.
fn version_of(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

impl<S: Session> SigningService<S> for InMemoryKeys {
    async fn sign(
        &self,
        _session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        message: &[u8],
    ) -> Result<Signature, Error> {
        let fresh = software::generate(algorithm)?;
        let mut keys = self.signing.lock().unwrap_or_else(PoisonError::into_inner);
        let ring = keys.entry((tenant_id.to_owned(), algorithm)).or_default();
        if ring.is_empty() {
            ring.push(fresh);
        }
        let pair = ring.last().ok_or(Error::NoKeyOfVersion(1))?;
        Ok(Signature::new(
            algorithm,
            version_of(ring.len()),
            software::sign(algorithm, &pair.private, message)?,
        ))
    }

    async fn verify(
        &self,
        _session: &S,
        tenant_id: &str,
        message: &[u8],
        signature: &Signature,
    ) -> Result<(), Error> {
        let algorithm = signature.algorithm();
        let keys = self.signing.lock().unwrap_or_else(PoisonError::into_inner);
        let ring = keys
            .get(&(tenant_id.to_owned(), algorithm))
            .ok_or_else(|| Error::SigningKeyNotFound {
                tenant_id: tenant_id.to_owned(),
                algorithm,
            })?;
        let version = signature.key_version();
        let pair = usize::try_from(version)
            .ok()
            .and_then(|v| v.checked_sub(1))
            .and_then(|index| ring.get(index))
            .ok_or(Error::NoKeyOfVersion(version))?;
        software::verify(algorithm, &pair.public, message, signature.as_bytes())
    }

    async fn rotate_signing_key(
        &self,
        _session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> Result<u32, Error> {
        let pair = software::generate(algorithm)?;
        Ok(self.push(tenant_id, algorithm, pair))
    }

    async fn delete_signing_key(
        &self,
        _session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> Result<(), Error> {
        self.signing
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&(tenant_id.to_owned(), algorithm));
        Ok(())
    }
}

impl<S: Session> MacService<S> for InMemoryKeys {
    async fn mac(
        &self,
        _session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
        message: &[u8],
    ) -> Result<Mac, Error> {
        let fresh = software::generate_mac_key()?;
        let mut keys = self.mac.lock().unwrap_or_else(PoisonError::into_inner);
        let ring = keys
            .entry((tenant_id.to_owned(), purpose.as_str().to_owned()))
            .or_default();
        if ring.is_empty() {
            ring.push(fresh);
        }
        let key = ring.last().ok_or(Error::NoKeyOfVersion(1))?;
        Ok(Mac::new(
            version_of(ring.len()),
            software::mac(key, message)?,
        ))
    }

    async fn delete_mac_key(
        &self,
        _session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
    ) -> Result<(), Error> {
        self.mac
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&(tenant_id.to_owned(), purpose.as_str().to_owned()));
        Ok(())
    }
}
