//! Identity map: one entity instance per session.
//!
//! Two responsibilities, as in the Python and Go ports: a repository that
//! loads the same row twice within one session hands out the same [`Arc`], and
//! at [`IsolationLevel::Serializable`] the map also remembers that a row does
//! *not* exist, so the query is not repeated. How it is built:
//!
//! * The isolation policy is a single predicate over (level, kind of fact),
//!   applied to what is written and to what is read alike, so an answer never
//!   depends on the level a fact was remembered under. See `policy`.
//! * What the map knows about a key is a value with two states, `Slot`:
//!   anchored in the recency window, or released and alive only while the
//!   domain holds the entity. Every change of knowledge is a function from one
//!   state to the next; the map only applies them. See `slot`.
//! * Recency is exact — a `BTreeMap` from tick to key — so there is nothing to
//!   compact and every operation is O(log n) with n bounded by the window.
//! * An entity found after it was released is anchored again: what has just
//!   been used is, by definition, recently used.
//!
//! Entries are held weakly and anchored by the window: an entity
//! stays reachable while the domain holds it *or* while it is in the window,
//! and disappears once neither is true. Every method takes `&self`; the
//! mutation lives behind a lock.

mod key;
mod policy;
mod slot;
mod state;

use std::sync::{Arc, Mutex, MutexGuard};

pub use self::key::IdentityKey;
use self::key::KeyBox;
use self::slot::{AnyLookup, Fact, Kind};
use self::state::State;
use crate::isolation::IsolationLevel;

/// Default size of the recency window.
pub const DEFAULT_CACHE_SIZE: usize = 100;

/// Outcome of a lookup.
#[derive(Debug)]
pub enum Lookup<T: ?Sized> {
    /// The entity is known and still alive.
    Found(Arc<T>),
    /// The map knows the entity does not exist; there is no point in querying.
    Absent,
    /// The map knows nothing about this key; the caller has to query.
    Unknown,
}

impl<T: ?Sized> Lookup<T> {
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

    /// Keeps the answer only if the policy admits its kind.
    fn admitted(self, admits: impl Fn(Kind) -> bool) -> Self {
        match self {
            Lookup::Found(_) if !admits(Kind::Entity) => Lookup::Unknown,
            Lookup::Absent if !admits(Kind::Absence) => Lookup::Unknown,
            answer => answer,
        }
    }
}

impl<T: ?Sized> PartialEq for Lookup<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Lookup::Found(left), Lookup::Found(right)) => Arc::ptr_eq(left, right),
            (Lookup::Absent, Lookup::Absent) | (Lookup::Unknown, Lookup::Unknown) => true,
            _ => false,
        }
    }
}

/// Tracks entity instances so that each is loaded only once per session.
pub struct IdentityMap {
    isolation: IsolationLevel,
    state: Mutex<State>,
}

impl IdentityMap {
    /// Creates a map with the given window and isolation level.
    pub fn new(cache_size: usize, isolation: IsolationLevel) -> Self {
        IdentityMap {
            isolation,
            state: Mutex::new(State::new(cache_size)),
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

    /// Remembers an entity, if the level admits remembering entities.
    pub fn add<K: IdentityKey>(&self, key: K, entity: Arc<K::Entity>) {
        self.remember(key, Fact::Entity(entity));
    }

    /// Remembers that the entity does not exist, if the level admits that.
    pub fn add_absent<K: IdentityKey>(&self, key: K) {
        self.remember(key, Fact::Absence);
    }

    /// Looks the key up.
    pub fn get<K: IdentityKey>(&self, key: &K) -> Lookup<K::Entity> {
        match self.recall(key) {
            Lookup::Found(entity) => match entity.downcast::<K::Entity>() {
                Ok(entity) => Lookup::Found(entity),
                Err(_) => unreachable!("the key type pins the entity type"),
            },
            Lookup::Absent => Lookup::Absent,
            Lookup::Unknown => Lookup::Unknown,
        }
    }

    /// True if the map can answer for this key, either way.
    pub fn has<K: IdentityKey>(&self, key: &K) -> bool {
        self.recall(key).is_known()
    }

    /// Forgets the key.
    pub fn remove<K: IdentityKey>(&self, key: &K) {
        self.lock().forget(key);
    }

    /// Forgets everything. Called when the outermost transaction scope ends.
    pub fn clear(&self) {
        self.lock().forget_all();
    }

    /// Number of keys the map can still answer for.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// True if the map remembers nothing.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Resizes the window, releasing what no longer fits.
    pub fn set_size(&self, size: usize) {
        self.lock().resize(size);
    }

    fn remember<K: IdentityKey>(&self, key: K, fact: Fact) {
        if policy::admits(self.isolation, fact.kind()) {
            self.lock().remember(KeyBox::new(key), fact);
        }
    }

    fn recall<K: IdentityKey>(&self, key: &K) -> AnyLookup {
        let isolation = self.isolation;
        self.lock()
            .recall(key)
            .admitted(|kind| policy::admits(isolation, kind))
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
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
