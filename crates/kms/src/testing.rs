//! Keys in memory, for the tests of what consumes the signing and MAC
//! ports: the adapter over them is exercised as it is in production, with
//! the network behind the port and nothing else replaced.
//!
//! The keys are the model's — a [`SigningKey`] per tenant, algorithm and
//! version, registered from a seed so that a test's signatures are the
//! same from run to run, or made from the operating system's randomness on
//! first signing, as Vault would; a [`MacKey`] per tenant and purpose, made
//! on first use. Versions are retired as an operator retires them
//! ([`InMemoryKeys::retire_signing_versions_before`]), off the port; a
//! version once used is never numbered again. Nothing persists past the
//! process.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU32;
use std::sync::{Mutex, PoisonError};

use ascetic_ddd_session::Session;

use crate::domain::{
    KeyLabel, Mac, MacKey, PublicKeyring, Signature, SignatureAlgorithm, SigningKey,
};
use crate::error::Error;
use crate::port::{MacService, PublicKeyService, SigningService};

/// Signing keyrings by tenant and algorithm: the versions that serve, by
/// version, and the next version a rotation makes.
type SigningRings = Mutex<HashMap<(String, SignatureAlgorithm), SigningRing>>;

/// One tenant's key of one algorithm.
#[derive(Default)]
struct SigningRing {
    versions: BTreeMap<u32, SigningKey>,
    made: u32,
}

impl SigningRing {
    /// The key a signature names, if that version serves.
    fn version(&self, version: u32) -> Result<&SigningKey, Error> {
        self.versions
            .get(&version)
            .ok_or(Error::NoKeyOfVersion(version))
    }
}

/// MAC keyrings by tenant and purpose: the versions of each key, counted
/// from one.
type MacRings = Mutex<HashMap<(String, String), Vec<MacKey>>>;

/// The tenants' keys: signing keys by tenant and algorithm, MAC keys by
/// tenant and purpose, each a keyring whose versions count from one.
#[derive(Default)]
pub struct InMemoryKeys {
    signing: SigningRings,
    mac: MacRings,
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
        self.push(tenant_id, SignatureAlgorithm::Ed25519, |version| {
            SigningKey::from_seed(tenant_id, SignatureAlgorithm::Ed25519, version, seed)
        })
        .unwrap_or_else(|_| unreachable!("any seed is an Ed25519 key"))
    }

    /// Retires every version of the tenant's key of `algorithm` below
    /// `version`, as an operator does on a KMS (Transit's
    /// `min_decryption_version`): they neither sign, verify nor are
    /// published again, and their numbers are not reused. The latest is
    /// never retired.
    ///
    /// # Errors
    /// [`Error::SigningKeyNotFound`] when the tenant has no key of the
    /// algorithm; [`Error::NoKeyOfVersion`] for a version above the latest.
    pub fn retire_signing_versions_before(
        &self,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        version: u32,
    ) -> Result<(), Error> {
        let mut keys = self.signing.lock().unwrap_or_else(PoisonError::into_inner);
        let ring = keys
            .get_mut(&(tenant_id.to_owned(), algorithm))
            .ok_or_else(|| Error::SigningKeyNotFound {
                tenant_id: tenant_id.to_owned(),
                algorithm,
            })?;
        if version > ring.made {
            return Err(Error::NoKeyOfVersion(version));
        }
        ring.versions.retain(|kept, _| *kept >= version);
        Ok(())
    }

    /// The next version of the tenant's keyring of `algorithm`, made by
    /// `make` for that version.
    fn push(
        &self,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        make: impl FnOnce(u32) -> Result<SigningKey, Error>,
    ) -> Result<u32, Error> {
        let mut keys = self.signing.lock().unwrap_or_else(PoisonError::into_inner);
        let ring = keys.entry((tenant_id.to_owned(), algorithm)).or_default();
        let version = ring.made.saturating_add(1);
        ring.versions.insert(version, make(version)?);
        ring.made = version;
        Ok(version)
    }

    /// What `f` answers of the tenant's keyring of `algorithm`, without
    /// making one.
    fn with_ring<T>(
        &self,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        f: impl FnOnce(&SigningRing) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let keys = self.signing.lock().unwrap_or_else(PoisonError::into_inner);
        let ring = keys
            .get(&(tenant_id.to_owned(), algorithm))
            .filter(|ring| !ring.versions.is_empty())
            .ok_or_else(|| Error::SigningKeyNotFound {
                tenant_id: tenant_id.to_owned(),
                algorithm,
            })?;
        f(ring)
    }
}

impl<S: Session> SigningService<S> for InMemoryKeys {
    async fn sign(
        &self,
        _session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        message: &[u8],
    ) -> Result<Signature, Error> {
        let mut keys = self.signing.lock().unwrap_or_else(PoisonError::into_inner);
        let ring = keys.entry((tenant_id.to_owned(), algorithm)).or_default();
        if ring.versions.is_empty() {
            let version = ring.made.saturating_add(1);
            ring.versions.insert(
                version,
                SigningKey::generate(tenant_id, algorithm, version)?,
            );
            ring.made = version;
        }
        let (_, key) = ring
            .versions
            .last_key_value()
            .ok_or(Error::NoKeyOfVersion(ring.made))?;
        key.sign(message)
    }

    async fn sign_with_version(
        &self,
        _session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        key_version: NonZeroU32,
        message: &[u8],
    ) -> Result<Signature, Error> {
        self.with_ring(tenant_id, algorithm, |ring| {
            ring.version(key_version.get())?.sign(message)
        })
    }

    async fn verify(
        &self,
        _session: &S,
        tenant_id: &str,
        message: &[u8],
        signature: &Signature,
    ) -> Result<(), Error> {
        self.with_ring(tenant_id, signature.algorithm(), |ring| {
            ring.version(signature.key_version())?
                .public()
                .verify(message, signature)
        })
    }

    async fn rotate_signing_key(
        &self,
        _session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> Result<u32, Error> {
        self.push(tenant_id, algorithm, |version| {
            SigningKey::generate(tenant_id, algorithm, version)
        })
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

impl<S: Session> PublicKeyService<S> for InMemoryKeys {
    async fn public_keys(
        &self,
        _session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> Result<PublicKeyring, Error> {
        self.with_ring(tenant_id, algorithm, |ring| {
            PublicKeyring::new(
                algorithm,
                ring.versions
                    .iter()
                    .map(|(version, key)| (*version, key.public().clone())),
            )
        })
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
        let mut keys = self.mac.lock().unwrap_or_else(PoisonError::into_inner);
        let ring = keys
            .entry((tenant_id.to_owned(), purpose.as_str().to_owned()))
            .or_default();
        if ring.is_empty() {
            ring.push(MacKey::generate(tenant_id, purpose, 1)?);
        }
        let key = ring.last().ok_or(Error::NoKeyOfVersion(1))?;
        key.mac(message)
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
