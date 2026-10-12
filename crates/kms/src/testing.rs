//! Keys in memory, for the tests of what consumes the signing and MAC
//! ports: the adapter over them is exercised as it is in production, with
//! the network behind the port and nothing else replaced.
//!
//! Ed25519 only: a key per tenant, registered from a seed so that a test's
//! signatures are the same from run to run, or made from the operating
//! system's randomness on first signing, as Vault would. MAC keys are made
//! on first use. Nothing persists past the process.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use ascetic_ddd_session::Session;
use ed25519_dalek::{Signer as _, SigningKey, Verifier as _};
use hmac::{Hmac, Mac as _};
use sha2::Sha256;

use crate::domain::{KeyLabel, Mac, Signature, SignatureAlgorithm};
use crate::error::Error;
use crate::port::{MacService, SigningService};

/// Keyrings by tenant and `By`: the versions of each key, counted from one.
type Keyrings<K, By> = Mutex<HashMap<(String, By), Vec<K>>>;

/// The tenants' keys: signing keys by tenant and algorithm, MAC keys by
/// tenant and purpose, each a keyring whose versions count from one.
#[derive(Default)]
pub struct InMemoryKeys {
    signing: Keyrings<SigningKey, SignatureAlgorithm>,
    mac: Keyrings<[u8; 32], String>,
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
        let mut keys = self.signing.lock().unwrap_or_else(PoisonError::into_inner);
        let ring = keys
            .entry((tenant_id.to_owned(), SignatureAlgorithm::Ed25519))
            .or_default();
        ring.push(SigningKey::from_bytes(seed));
        version_of(ring.len())
    }

    fn random_seed() -> Result<[u8; 32], Error> {
        let mut seed = [0u8; 32];
        getrandom::fill(&mut seed).map_err(Error::Entropy)?;
        Ok(seed)
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
        if algorithm != SignatureAlgorithm::Ed25519 {
            return Err(Error::UnsupportedAlgorithm(algorithm.as_str().to_owned()));
        }
        let seed = Self::random_seed()?;
        let mut keys = self.signing.lock().unwrap_or_else(PoisonError::into_inner);
        let ring = keys.entry((tenant_id.to_owned(), algorithm)).or_default();
        if ring.is_empty() {
            ring.push(SigningKey::from_bytes(&seed));
        }
        let key = ring.last().ok_or(Error::NoKeyOfVersion(1))?;
        Ok(Signature::new(
            algorithm,
            version_of(ring.len()),
            key.sign(message).to_bytes().to_vec(),
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
        if algorithm != SignatureAlgorithm::Ed25519 {
            return Err(Error::UnsupportedAlgorithm(algorithm.as_str().to_owned()));
        }
        let keys = self.signing.lock().unwrap_or_else(PoisonError::into_inner);
        let ring = keys
            .get(&(tenant_id.to_owned(), algorithm))
            .ok_or_else(|| Error::SigningKeyNotFound {
                tenant_id: tenant_id.to_owned(),
                algorithm,
            })?;
        let version = signature.key_version();
        let key = usize::try_from(version)
            .ok()
            .and_then(|v| v.checked_sub(1))
            .and_then(|index| ring.get(index))
            .ok_or(Error::NoKeyOfVersion(version))?;
        let bytes: [u8; 64] = signature
            .as_bytes()
            .try_into()
            .map_err(|_| Error::InvalidSignature)?;
        key.verifying_key()
            .verify(message, &ed25519_dalek::Signature::from_bytes(&bytes))
            .map_err(|_| Error::InvalidSignature)
    }

    async fn rotate_signing_key(
        &self,
        _session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> Result<u32, Error> {
        if algorithm != SignatureAlgorithm::Ed25519 {
            return Err(Error::UnsupportedAlgorithm(algorithm.as_str().to_owned()));
        }
        let seed = Self::random_seed()?;
        Ok(self.register(tenant_id, &seed))
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
        let fresh = Self::random_seed()?;
        let mut keys = self.mac.lock().unwrap_or_else(PoisonError::into_inner);
        let ring = keys
            .entry((tenant_id.to_owned(), purpose.as_str().to_owned()))
            .or_default();
        if ring.is_empty() {
            ring.push(fresh);
        }
        let key = ring.last().ok_or(Error::NoKeyOfVersion(1))?;
        let mut mac = Hmac::<Sha256>::new_from_slice(key)
            .map_err(|error| Error::Malformed(error.to_string()))?;
        mac.update(message);
        Ok(Mac::new(
            version_of(ring.len()),
            mac.finalize().into_bytes().to_vec(),
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
