//! Identity map: one entity instance per session.
//!
//! Two responsibilities, as in the Python implementation:
//!
//! * **identity** - a repository that loads the same row twice within one
//!   session hands out the same [`Arc`], not two copies;
//! * **negative caching** - at [`IsolationLevel::Serializable`] the map also
//!   remembers that a row does *not* exist, so the query is not repeated.
//!
//! Entries are held weakly, anchored by a small LRU window. An entity stays
//! reachable while the domain holds it *or* while it is inside the window; once
//! both are gone the entry disappears on its own. The map therefore never grows
//! without bound and never keeps entities alive longer than the session needs
//! them.
//!
//! Every method takes `&self`: the mutation lives behind a lock, so the map is
//! shared by a transaction scope and its nested scopes without any `&mut`
//! travelling through the domain.

use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, Weak};

use crate::identity_key::{DynKey, IdentityKey, KeyBox};
use crate::isolation::IsolationLevel;

/// Default size of the anchor window.
pub const DEFAULT_CACHE_SIZE: usize = 100;

/// Outcome of a lookup.
///
/// The three cases are what Python expresses with a returned object, an
/// `ObjectNotFound` and a `KeyError` respectively.
#[derive(Debug)]
pub enum Lookup<T> {
    /// The entity is known and still alive.
    Found(Arc<T>),
    /// The map knows the entity does not exist; there is no point in querying.
    Absent,
    /// The map knows nothing about this key; the caller has to query.
    Unknown,
}

impl<T> Lookup<T> {
    /// The entity, if it was found.
    pub fn found(self) -> Option<Arc<T>> {
        match self {
            Lookup::Found(entity) => Some(entity),
            _ => None,
        }
    }

    /// True if the map answered the question, either way.
    pub fn is_known(&self) -> bool {
        !matches!(self, Lookup::Unknown)
    }
}

impl<T> PartialEq for Lookup<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Lookup::Found(left), Lookup::Found(right)) => Arc::ptr_eq(left, right),
            (Lookup::Absent, Lookup::Absent) | (Lookup::Unknown, Lookup::Unknown) => true,
            _ => false,
        }
    }
}

type AnyArc = Arc<dyn Any + Send + Sync>;

struct Entry {
    /// Strong reference, held while the key is inside the LRU window. This is
    /// what keeps an entity alive when the domain has already dropped it.
    anchor: Option<AnyArc>,
    /// Weak reference: the entity may outlive the window if the domain still
    /// holds it.
    value: Weak<dyn Any + Send + Sync>,
    /// True for a remembered absence rather than a remembered entity.
    absent: bool,
}

impl Entry {
    fn upgrade(&self) -> Option<AnyArc> {
        self.value.upgrade()
    }
}

#[derive(Default)]
struct Inner {
    entries: HashMap<KeyBox, Entry>,
    /// Keys in access order, oldest first.
    order: VecDeque<KeyBox>,
    size: usize,
}

impl Inner {
    fn touch(&mut self, key: &(dyn DynKey + 'static)) {
        if let Some(position) = self.order.iter().position(|k| k.as_dyn() == key) {
            let key = self.order.remove(position).expect("position is valid");
            self.order.push_back(key);
        }
    }

    fn insert(&mut self, key: KeyBox, entry: Entry) {
        if self.entries.insert(key.clone(), entry).is_some() {
            self.touch(key.as_dyn());
            return;
        }
        self.order.push_back(key);
        self.evict();
    }

    /// Drops anchors beyond the window; entries themselves survive while the
    /// domain still holds the entity.
    fn evict(&mut self) {
        while self.order.len() > self.size {
            let Some(evicted) = self.order.pop_front() else {
                break;
            };
            if let Some(entry) = self.entries.get_mut(evicted.as_dyn()) {
                entry.anchor = None;
                if entry.upgrade().is_none() {
                    self.entries.remove(evicted.as_dyn());
                }
            }
        }
    }

    fn remove(&mut self, key: &(dyn DynKey + 'static)) {
        self.entries.remove(key);
        if let Some(position) = self.order.iter().position(|k| k.as_dyn() == key) {
            self.order.remove(position);
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
    }
}

/// Tracks entity instances so that each is loaded only once per session.
pub struct IdentityMap {
    isolation: IsolationLevel,
    inner: Mutex<Inner>,
}

impl IdentityMap {
    /// Creates a map with the given anchor window and isolation level.
    pub fn new(cache_size: usize, isolation: IsolationLevel) -> Self {
        IdentityMap {
            isolation,
            inner: Mutex::new(Inner {
                size: cache_size,
                ..Inner::default()
            }),
        }
    }

    /// Creates a map with [`DEFAULT_CACHE_SIZE`] and the given isolation level.
    pub fn with_isolation(isolation: IsolationLevel) -> Self {
        IdentityMap::new(DEFAULT_CACHE_SIZE, isolation)
    }

    /// The isolation level this map was created with.
    pub fn isolation(&self) -> IsolationLevel {
        self.isolation
    }

    /// Remembers an entity.
    ///
    /// A no-op below [`IsolationLevel::RepeatableRead`].
    pub fn add<K: IdentityKey>(&self, key: K, entity: Arc<K::Entity>) {
        if !self.isolation.caches_present() {
            return;
        }
        let anchor: AnyArc = entity;
        let entry = Entry {
            value: Arc::downgrade(&anchor),
            anchor: Some(anchor),
            absent: false,
        };
        self.lock().insert(KeyBox::new(key), entry);
    }

    /// Remembers that the entity does not exist.
    ///
    /// A no-op below [`IsolationLevel::Serializable`].
    pub fn add_absent<K: IdentityKey>(&self, key: K) {
        if !self.isolation.caches_absent() {
            return;
        }
        // The marker has no entity to anchor, so it is anchored by a unit value
        // and therefore lives exactly as long as the LRU window keeps it.
        let anchor: AnyArc = Arc::new(());
        let entry = Entry {
            value: Arc::downgrade(&anchor),
            anchor: Some(anchor),
            absent: true,
        };
        self.lock().insert(KeyBox::new(key), entry);
    }

    /// Looks the key up.
    pub fn get<K: IdentityKey>(&self, key: &K) -> Lookup<K::Entity> {
        let mut inner = self.lock();
        let Some(entry) = inner.entries.get(key as &dyn DynKey) else {
            return Lookup::Unknown;
        };
        let (Some(value), absent) = (entry.upgrade(), entry.absent) else {
            // The entity is gone and the anchor has been evicted.
            inner.remove(key as &dyn DynKey);
            return Lookup::Unknown;
        };
        inner.touch(key as &dyn DynKey);
        drop(inner);

        if absent {
            return Lookup::Absent;
        }
        match value.downcast::<K::Entity>() {
            Ok(entity) => Lookup::Found(entity),
            // Unreachable: the key type pins the entity type.
            Err(_) => Lookup::Unknown,
        }
    }

    /// True if the map can answer for this key, either way.
    pub fn has<K: IdentityKey>(&self, key: &K) -> bool {
        self.get(key).is_known()
    }

    /// Forgets the key.
    pub fn remove<K: IdentityKey>(&self, key: &K) {
        self.lock().remove(key as &dyn DynKey);
    }

    /// Forgets everything. Called when the outermost transaction scope ends.
    pub fn clear(&self) {
        self.lock().clear();
    }

    /// Number of keys currently remembered.
    pub fn len(&self) -> usize {
        self.lock().entries.len()
    }

    /// True if the map remembers nothing.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Resizes the anchor window, dropping anchors that no longer fit.
    pub fn set_size(&self, size: usize) {
        let mut inner = self.lock();
        inner.size = size;
        inner.evict();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Default for IdentityMap {
    fn default() -> Self {
        IdentityMap::with_isolation(IsolationLevel::default())
    }
}

impl std::fmt::Debug for IdentityMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdentityMap")
            .field("isolation", &self.isolation)
            .field("len", &self.len())
            .finish()
    }
}
