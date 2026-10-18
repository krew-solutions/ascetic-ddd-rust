//! Keys in memory, for the tests of what consumes the signing and MAC
//! ports: the adapter over them is exercised as it is in production, with
//! the network behind the port and nothing else replaced.
//!
//! The keys are the model's — a [`SigningKey`] per tenant, algorithm and
//! version, registered from a seed so that a test's signatures are the
//! same from run to run, or made from the operating system's randomness on
//! first signing, as Vault would; a [`MacKey`] per tenant, purpose and
//! version, made on first use. Built [`managed_keys`](InMemoryKeys::managed_keys),
//! nothing is made on first use, as with an adapter whose keys are made by
//! custody. The keys' custody is served as the adapters serve it
//! ([`SigningKeyCustody`], [`MacKeyCustody`]): made, rotated, retired and
//! deleted through the ports, with their rules (ADR-0020); a version once
//! used is never numbered again. The KEK is not held here: the stage's and
//! the custody's tests of the KEK run on the PostgreSQL adapter (ADR-0017).
//! Nothing persists past the process.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU32;
use std::sync::{Mutex, PoisonError};

use ascetic_ddd_session::Session;

use crate::domain::{
    KeyLabel, Mac, MacKey, PublicKeyring, Signature, SignatureAlgorithm, SigningKey,
};
use crate::error::Error;
use crate::port::{
    KeyOptions, MacKeyCustody, MacService, PublicKeyService, SigningKeyCustody, SigningService,
};

/// Signing keyrings by tenant and algorithm: the versions that serve, by
/// version, and the next version a rotation makes.
type SigningRings = Mutex<HashMap<(String, SignatureAlgorithm), SigningRing>>;

/// One tenant's key: the versions that serve, by version, and the last
/// version made — the next rotation makes the one after it, so a version
/// retired is never numbered again.
struct Ring<K> {
    versions: BTreeMap<u32, K>,
    made: u32,
}

impl<K> Default for Ring<K> {
    fn default() -> Self {
        Ring {
            versions: BTreeMap::new(),
            made: 0,
        }
    }
}

impl<K> Ring<K> {
    /// The key of a version, if that version serves.
    fn version(&self, version: u32) -> Result<&K, Error> {
        self.versions
            .get(&version)
            .ok_or(Error::NoKeyOfVersion(version))
    }

    /// The latest version's key.
    fn latest(&self) -> Result<&K, Error> {
        self.versions
            .last_key_value()
            .map(|(_, key)| key)
            .ok_or(Error::NoKeyOfVersion(self.made))
    }

    /// The next version, made by `make`; answers it.
    fn push(&mut self, make: impl FnOnce(u32) -> Result<K, Error>) -> Result<u32, Error> {
        let version = self.made.saturating_add(1);
        self.versions.insert(version, make(version)?);
        self.made = version;
        Ok(version)
    }

    /// Retires the versions below `version`: above the latest is
    /// [`Error::NoKeyOfVersion`] and nothing goes; at or below the oldest
    /// kept, nothing goes either. The latest always stays.
    fn retire_before(&mut self, version: u32) -> Result<(), Error> {
        if version > self.made {
            return Err(Error::NoKeyOfVersion(version));
        }
        self.versions.retain(|kept, _| *kept >= version);
        Ok(())
    }
}

/// One tenant's key of one algorithm.
type SigningRing = Ring<SigningKey>;

/// MAC keyrings by tenant and purpose.
type MacRings = Mutex<HashMap<(String, String), Ring<MacKey>>>;

/// The tenants' keys: signing keys by tenant and algorithm, MAC keys by
/// tenant and purpose, each a keyring whose versions count from one.
#[derive(Default)]
pub struct InMemoryKeys {
    signing: SigningRings,
    mac: MacRings,
    managed: bool,
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

    /// The same keys making none on first use: `sign` and `mac` for a
    /// tenant without a key are [`Error::SigningKeyNotFound`] and
    /// [`Error::MacKeyNotFound`], as with an adapter built `managed_keys()`;
    /// the keys are made through custody, or registered.
    #[must_use]
    pub fn managed_keys(self) -> Self {
        InMemoryKeys {
            managed: true,
            ..self
        }
    }

    /// Registers the next version of the tenant's Ed25519 key, derived from
    /// `seed`; answers the version (one for the first).
    pub fn register(&self, tenant_id: &str, seed: &[u8; 32]) -> u32 {
        self.push(tenant_id, SignatureAlgorithm::Ed25519, |version| {
            SigningKey::from_seed(tenant_id, SignatureAlgorithm::Ed25519, version, seed)
        })
        .unwrap_or_else(|_| unreachable!("any seed is an Ed25519 key"))
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
        keys.entry((tenant_id.to_owned(), algorithm))
            .or_default()
            .push(make)
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
            .ok_or_else(|| signing_key_not_found(tenant_id, algorithm))?;
        f(ring)
    }

    /// What `f` makes of the tenant's keyring of `algorithm` that has a
    /// key, without making one.
    fn with_ring_mut<T>(
        &self,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        f: impl FnOnce(&mut SigningRing) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut keys = self.signing.lock().unwrap_or_else(PoisonError::into_inner);
        let ring = keys
            .get_mut(&(tenant_id.to_owned(), algorithm))
            .filter(|ring| !ring.versions.is_empty())
            .ok_or_else(|| signing_key_not_found(tenant_id, algorithm))?;
        f(ring)
    }

    /// What `f` makes of the tenant's MAC keyring for `purpose` that has a
    /// key, without making one.
    fn with_mac_ring_mut<T>(
        &self,
        tenant_id: &str,
        purpose: &KeyLabel,
        f: impl FnOnce(&mut Ring<MacKey>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut keys = self.mac.lock().unwrap_or_else(PoisonError::into_inner);
        let ring = keys
            .get_mut(&(tenant_id.to_owned(), purpose.as_str().to_owned()))
            .filter(|ring| !ring.versions.is_empty())
            .ok_or_else(|| mac_key_not_found(tenant_id, purpose))?;
        f(ring)
    }
}

/// The refusal of a tenant without a signing key of the algorithm.
fn signing_key_not_found(tenant_id: &str, algorithm: SignatureAlgorithm) -> Error {
    Error::SigningKeyNotFound {
        tenant_id: tenant_id.to_owned(),
        algorithm,
    }
}

/// The refusal of a tenant without a MAC key for the purpose.
fn mac_key_not_found(tenant_id: &str, purpose: &KeyLabel) -> Error {
    Error::MacKeyNotFound {
        tenant_id: tenant_id.to_owned(),
        purpose: purpose.as_str().to_owned(),
    }
}

/// A rotation period, which nothing here could keep.
fn no_scheduler(options: &KeyOptions) -> Result<(), Error> {
    match options.auto_rotate_period {
        Some(_) => Err(Error::Unsupported(
            "automatic rotation: the in-memory keys rotate on request only",
        )),
        None => Ok(()),
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
        if self.managed {
            return self.with_ring(tenant_id, algorithm, |ring| ring.latest()?.sign(message));
        }
        let mut keys = self.signing.lock().unwrap_or_else(PoisonError::into_inner);
        let ring = keys.entry((tenant_id.to_owned(), algorithm)).or_default();
        if ring.versions.is_empty() {
            ring.push(|version| SigningKey::generate(tenant_id, algorithm, version))?;
        }
        ring.latest()?.sign(message)
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
}

impl<S: Session> SigningKeyCustody<S> for InMemoryKeys {
    async fn create_signing_key(
        &self,
        _session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        options: &KeyOptions,
    ) -> Result<u32, Error> {
        no_scheduler(options)?;
        let mut keys = self.signing.lock().unwrap_or_else(PoisonError::into_inner);
        let ring = keys.entry((tenant_id.to_owned(), algorithm)).or_default();
        if ring.versions.is_empty() {
            ring.push(|version| SigningKey::generate(tenant_id, algorithm, version))
        } else {
            Ok(ring.made)
        }
    }

    async fn rotate_signing_key(
        &self,
        _session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> Result<u32, Error> {
        self.with_ring_mut(tenant_id, algorithm, |ring| {
            ring.push(|version| SigningKey::generate(tenant_id, algorithm, version))
        })
    }

    async fn retire_signing_versions_before(
        &self,
        _session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        version: u32,
    ) -> Result<(), Error> {
        self.with_ring_mut(tenant_id, algorithm, |ring| ring.retire_before(version))
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
        if self.managed {
            return self.with_mac_ring_mut(tenant_id, purpose, |ring| ring.latest()?.mac(message));
        }
        let mut keys = self.mac.lock().unwrap_or_else(PoisonError::into_inner);
        let ring = keys
            .entry((tenant_id.to_owned(), purpose.as_str().to_owned()))
            .or_default();
        if ring.versions.is_empty() {
            ring.push(|version| MacKey::generate(tenant_id, purpose, version))?;
        }
        ring.latest()?.mac(message)
    }
}

impl<S: Session> MacKeyCustody<S> for InMemoryKeys {
    async fn create_mac_key(
        &self,
        _session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
        options: &KeyOptions,
    ) -> Result<u32, Error> {
        no_scheduler(options)?;
        let mut keys = self.mac.lock().unwrap_or_else(PoisonError::into_inner);
        let ring = keys
            .entry((tenant_id.to_owned(), purpose.as_str().to_owned()))
            .or_default();
        if ring.versions.is_empty() {
            ring.push(|version| MacKey::generate(tenant_id, purpose, version))
        } else {
            Ok(ring.made)
        }
    }

    async fn rotate_mac_key(
        &self,
        _session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
    ) -> Result<u32, Error> {
        self.with_mac_ring_mut(tenant_id, purpose, |ring| {
            ring.push(|version| MacKey::generate(tenant_id, purpose, version))
        })
    }

    async fn retire_mac_versions_before(
        &self,
        _session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
        version: u32,
    ) -> Result<(), Error> {
        self.with_mac_ring_mut(tenant_id, purpose, |ring| ring.retire_before(version))
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
