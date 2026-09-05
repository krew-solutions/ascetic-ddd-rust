//! Keys of the identity map.
//!
//! A key carries the type of the entity it identifies, so a lookup needs no
//! cast and no type argument of its own:
//!
//! ```
//! use ascetic_ddd_session::IdentityKey;
//!
//! struct Order;
//!
//! #[derive(Clone, PartialEq, Eq, Hash)]
//! struct OrderKey(i64);
//!
//! impl IdentityKey for OrderKey {
//!     type Entity = Order;
//! }
//! ```
//!
//! Python pairs an entity type with an id at run time
//! (`IdentityKey(Model, pk)`); here the pairing is a property of the key type
//! itself, checked at compile time. Two key types carrying the same id are
//! distinct keys, because the key's own type takes part in equality and
//! hashing.

use std::any::{Any, TypeId};
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};

/// A key identifying an entity of type [`IdentityKey::Entity`].
pub trait IdentityKey: Clone + Eq + Hash + Send + Sync + 'static {
    /// The entity this key identifies.
    type Entity: Send + Sync + 'static;
}

/// Type-erased key, so that one map can hold keys of many types.
pub(crate) trait DynKey: Send + Sync {
    fn dyn_eq(&self, other: &dyn DynKey) -> bool;
    fn dyn_hash(&self, state: &mut dyn Hasher);
    fn dyn_clone(&self) -> Box<dyn DynKey>;
    fn as_any(&self) -> &dyn Any;
}

impl<K: IdentityKey> DynKey for K {
    fn dyn_eq(&self, other: &dyn DynKey) -> bool {
        other
            .as_any()
            .downcast_ref::<K>()
            .is_some_and(|other| self == other)
    }

    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        // The key type takes part in the hash, so that two keys of different
        // types carrying the same id do not collide.
        TypeId::of::<K>().hash(&mut state);
        self.hash(&mut state);
    }

    fn dyn_clone(&self) -> Box<dyn DynKey> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl PartialEq for dyn DynKey {
    fn eq(&self, other: &Self) -> bool {
        self.dyn_eq(other)
    }
}

impl Eq for dyn DynKey {}

impl Hash for dyn DynKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dyn_hash(state);
    }
}

/// Owned erased key, used as the map's key type.
pub(crate) struct KeyBox(Box<dyn DynKey>);

impl KeyBox {
    pub(crate) fn new<K: IdentityKey>(key: K) -> Self {
        KeyBox(Box::new(key))
    }

    pub(crate) fn as_dyn(&self) -> &(dyn DynKey + 'static) {
        &*self.0
    }
}

impl Clone for KeyBox {
    fn clone(&self) -> Self {
        KeyBox(self.0.dyn_clone())
    }
}

/// Lets the map be probed with a borrowed key, without boxing it first.
impl Borrow<dyn DynKey> for KeyBox {
    fn borrow(&self) -> &(dyn DynKey + 'static) {
        self.as_dyn()
    }
}

impl PartialEq for KeyBox {
    fn eq(&self, other: &Self) -> bool {
        self.0.dyn_eq(&*other.0)
    }
}

impl Eq for KeyBox {}

impl Hash for KeyBox {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.dyn_hash(state);
    }
}
