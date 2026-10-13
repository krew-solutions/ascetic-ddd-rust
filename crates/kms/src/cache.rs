//! A cache of unwrapped DEKs in front of a key management service.
//!
//! Unwrapping is a function of the tenant and the wrapped bytes, so its
//! result may be kept: a consumer that opens a thousand messages sealed
//! under one DEK, or a store that loads every version of a stream's keys,
//! asks the KMS once instead of a thousand times — which matters when the
//! KMS is a network away, Vault. Everything else passes through, and
//! deleting a tenant's KEK forgets what was cached for the tenant here;
//! elsewhere the cache's time to live is the bound, so a shredded tenant's
//! keys open for that long at most. That, and keys held in memory for that
//! long, is the price; the capacity bounds the memory, and a key evicted
//! is wiped.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use ascetic_ddd_session::Session;

use crate::domain::{Key, KeyLabel, Mac, Signature, SignatureAlgorithm};
use crate::error::Error;
use crate::port::{KeyManagementService, MacService, SigningService};

/// A key management service that keeps what it unwraps, for a while.
pub struct Cached<K> {
    kms: K,
    capacity: usize,
    ttl: Duration,
    entries: Mutex<HashMap<(String, Vec<u8>), Entry>>,
}

struct Entry {
    key: Key,
    since: Instant,
}

impl<K> Cached<K> {
    /// A cache of a thousand keys, each kept five minutes, in front of
    /// `kms`.
    pub fn new(kms: K) -> Self {
        Cached {
            kms,
            capacity: 1_000,
            ttl: Duration::from_secs(5 * 60),
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// The same cache holding at most `capacity` keys; the oldest goes
    /// when one more comes.
    pub fn with_capacity(self, capacity: usize) -> Self {
        Cached { capacity, ..self }
    }

    /// The same cache keeping a key `ttl` after it was unwrapped; zero
    /// keeps nothing.
    pub fn with_ttl(self, ttl: Duration) -> Self {
        Cached { ttl, ..self }
    }

    /// The service behind the cache.
    pub fn inner(&self) -> &K {
        &self.kms
    }

    /// How many keys are held now, expired or not.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether nothing is held.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<(String, Vec<u8>), Entry>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The key unwrapped for `tenant_id` from `wrapped`, if still kept.
    fn get(&self, tenant_id: &str, wrapped: &[u8]) -> Option<Key> {
        let entries = self.lock();
        let entry = entries.get(&(tenant_id.to_owned(), wrapped.to_vec()))?;
        (entry.since.elapsed() < self.ttl).then(|| entry.key.clone())
    }

    /// Keeps `key` as unwrapped for `tenant_id` from `wrapped`, making room
    /// first: expired entries go, then the oldest.
    fn put(&self, tenant_id: &str, wrapped: &[u8], key: Key) {
        if self.capacity == 0 || self.ttl.is_zero() {
            return;
        }
        let mut entries = self.lock();
        entries.retain(|_, entry| entry.since.elapsed() < self.ttl);
        while entries.len() >= self.capacity {
            let oldest = entries
                .iter()
                .min_by_key(|(_, entry)| entry.since)
                .map(|(id, _)| id.clone());
            match oldest {
                Some(id) => entries.remove(&id),
                None => break,
            };
        }
        entries.insert(
            (tenant_id.to_owned(), wrapped.to_vec()),
            Entry {
                key,
                since: Instant::now(),
            },
        );
    }

    fn forget(&self, tenant_id: &str) {
        self.lock().retain(|(tenant, _), _| tenant != tenant_id);
    }
}

impl<S, K> KeyManagementService<S> for Cached<K>
where
    S: Session,
    K: KeyManagementService<S>,
{
    async fn encrypt_dek(&self, session: &S, tenant_id: &str, dek: &Key) -> Result<Vec<u8>, Error> {
        self.kms.encrypt_dek(session, tenant_id, dek).await
    }

    /// The cached key, or the service's, kept.
    async fn decrypt_dek(
        &self,
        session: &S,
        tenant_id: &str,
        encrypted_dek: &[u8],
    ) -> Result<Key, Error> {
        if let Some(key) = self.get(tenant_id, encrypted_dek) {
            return Ok(key);
        }
        let key = self
            .kms
            .decrypt_dek(session, tenant_id, encrypted_dek)
            .await?;
        self.put(tenant_id, encrypted_dek, key.clone());
        Ok(key)
    }

    async fn generate_dek(&self, session: &S, tenant_id: &str) -> Result<(Key, Vec<u8>), Error> {
        self.kms.generate_dek(session, tenant_id).await
    }

    async fn rotate_kek(&self, session: &S, tenant_id: &str) -> Result<u32, Error> {
        self.kms.rotate_kek(session, tenant_id).await
    }

    async fn rewrap_dek(
        &self,
        session: &S,
        tenant_id: &str,
        encrypted_dek: &[u8],
    ) -> Result<Vec<u8>, Error> {
        self.kms.rewrap_dek(session, tenant_id, encrypted_dek).await
    }

    /// Deletes through the service and forgets the tenant's keys held here.
    async fn delete_kek(&self, session: &S, tenant_id: &str) -> Result<(), Error> {
        self.kms.delete_kek(session, tenant_id).await?;
        self.forget(tenant_id);
        Ok(())
    }
}

impl<K> std::fmt::Debug for Cached<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cached")
            .field("capacity", &self.capacity)
            .field("ttl", &self.ttl)
            .field("held", &self.len())
            .finish_non_exhaustive()
    }
}

/// The signing port passed through: a signature and a verification are not
/// functions of the cache's key, so nothing is cached — the wrapper serves
/// the port so that one `Cached<K>` stands for every port `K` serves.
impl<S, K> SigningService<S> for Cached<K>
where
    S: Session,
    K: SigningService<S>,
{
    async fn sign(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        message: &[u8],
    ) -> Result<Signature, Error> {
        self.kms.sign(session, tenant_id, algorithm, message).await
    }

    async fn verify(
        &self,
        session: &S,
        tenant_id: &str,
        message: &[u8],
        signature: &Signature,
    ) -> Result<(), Error> {
        self.kms
            .verify(session, tenant_id, message, signature)
            .await
    }

    async fn rotate_signing_key(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> Result<u32, Error> {
        self.kms
            .rotate_signing_key(session, tenant_id, algorithm)
            .await
    }

    async fn delete_signing_key(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> Result<(), Error> {
        self.kms
            .delete_signing_key(session, tenant_id, algorithm)
            .await
    }
}

/// The MAC port passed through, likewise.
impl<S, K> MacService<S> for Cached<K>
where
    S: Session,
    K: MacService<S>,
{
    async fn mac(
        &self,
        session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
        message: &[u8],
    ) -> Result<Mac, Error> {
        self.kms.mac(session, tenant_id, purpose, message).await
    }

    async fn delete_mac_key(
        &self,
        session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
    ) -> Result<(), Error> {
        self.kms.delete_mac_key(session, tenant_id, purpose).await
    }
}
