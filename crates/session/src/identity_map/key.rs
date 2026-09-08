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
use std::sync::Arc;

/// A key identifying an entity of type [`IdentityKey::Entity`].
pub trait IdentityKey: Clone + Eq + Hash + Send + Sync + 'static {
    /// The entity this key identifies.
    type Entity: Send + Sync + 'static;
}

/// Type-erased key, so that one map can hold keys of many types.
pub(super) trait DynKey: Send + Sync {
    fn dyn_eq(&self, other: &dyn DynKey) -> bool;
    fn dyn_hash(&self, state: &mut dyn Hasher);
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
///
/// Shared rather than cloned: the window records a handle per touch, and a
/// reference count is cheaper than boxing a copy of the key each time.
#[derive(Clone)]
pub(super) struct KeyBox(Arc<dyn DynKey>);

impl KeyBox {
    pub(super) fn new<K: IdentityKey>(key: K) -> Self {
        KeyBox(Arc::new(key))
    }

    pub(super) fn as_dyn(&self) -> &(dyn DynKey + 'static) {
        &*self.0
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

/// Declares a key for the identity map: a newtype over an id, paired with the
/// entity it identifies.
///
/// A key lives in the infrastructure, next to the repository that uses it. The
/// id itself cannot implement [`IdentityKey`]: the id is declared in the
/// domain, the trait in this crate, and an `impl` may only be written in one
/// of those two crates. The newtype is the infrastructure's own type, so the
/// `impl` is allowed there.
///
/// ```
/// use ascetic_ddd_session::identity_key;
///
/// struct Order;
///
/// #[derive(Clone, PartialEq, Eq, Hash)]
/// struct OrderId(i64);
///
/// identity_key!(OrderKey(OrderId) => Order);
///
/// let key = OrderKey(OrderId(7));
/// ```
///
/// This is the whole expansion:
///
/// ```ignore
/// #[derive(Clone, PartialEq, Eq, Hash)]
/// struct OrderKey(OrderId);
///
/// impl IdentityKey for OrderKey {
///     type Entity = Order;
/// }
/// ```
///
/// Attributes and a visibility go through: `identity_key!(#[derive(Debug)] pub
/// OrderKey(OrderId) => Order)`.
#[macro_export]
macro_rules! identity_key {
    ($(#[$meta:meta])* $vis:vis $key:ident($id:ty) => $entity:ty) => {
        $(#[$meta])*
        #[derive(Clone, PartialEq, Eq, Hash)]
        $vis struct $key($vis $id);

        impl $crate::IdentityKey for $key {
            type Entity = $entity;
        }
    };
}
