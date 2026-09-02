//! Activity type resolver - bidirectional mapping between names and activity types.
//!
//! Used to serialize and deserialize a [`RoutingSlip`][crate::routing_slip::RoutingSlip]
//! across distributed services. An [`ActivityType`] is not directly
//! JSON-serializable, so each side of the wire needs a registry that translates
//! a name (the JSON value) into the activity type to instantiate.

use std::any::TypeId;
use std::collections::HashMap;

use crate::activity::{Activity, ActivityType};
use crate::error::{Result, SagaError};

/// Bidirectional mapping between activity type names and activity types.
///
/// Used by [`routing_slip_serialization`][crate::routing_slip_serialization] to
/// translate between activity types (which are not JSON-serializable) and their
/// canonical names (which are). Dependency injection is preferred over a global
/// registry: each resolver is independent, preventing state pollution across
/// tests and services.
pub trait ActivityTypeResolver: Send + Sync {
    /// Returns the activity type registered under the given name.
    ///
    /// # Errors
    ///
    /// [`SagaError::ActivityTypeNotRegistered`] if no activity type is
    /// registered under this name.
    fn resolve(&self, type_name: &str) -> Result<ActivityType>;

    /// Returns the registered name for the given activity type.
    ///
    /// If the type is not registered but reports a canonical name through
    /// [`Activity::type_name()`], that name is returned instead.
    ///
    /// # Errors
    ///
    /// [`SagaError::ActivityTypeNotRegistered`] if the activity type is neither
    /// registered nor named.
    fn get_name(&self, activity_type: ActivityType) -> Result<String>;
}

/// Simple in-memory implementation of [`ActivityTypeResolver`].
///
/// Maintains a pair of maps (name -> type, type -> name) for O(1) lookup in
/// both directions. Register all activity types at startup, before
/// serialization begins.
///
/// ```
/// use ascetic_ddd_saga::{ActivityTypeResolver, MapBasedResolver};
/// use ascetic_ddd_saga::examples::ReserveCarActivity;
///
/// let mut resolver = MapBasedResolver::new();
/// resolver.register_type::<ReserveCarActivity>("ReserveCarActivity");
///
/// let activity_type = resolver.resolve("ReserveCarActivity").unwrap();
///
/// assert_eq!(resolver.get_name(activity_type).unwrap(), "ReserveCarActivity");
/// ```
#[derive(Debug, Default)]
pub struct MapBasedResolver {
    name_to_type: HashMap<String, ActivityType>,
    type_to_name: HashMap<TypeId, String>,
}

impl MapBasedResolver {
    /// Creates an empty resolver.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers an activity type under the given name.
    ///
    /// Re-registering the same name overwrites the previous entry.
    pub fn register(&mut self, name: impl Into<String>, activity_type: ActivityType) {
        let name = name.into();
        self.type_to_name
            .insert(activity_type.type_id(), name.clone());
        self.name_to_type.insert(name, activity_type);
    }

    /// Registers the activity implementation `A` under the given name.
    pub fn register_type<A: Activity + Default>(&mut self, name: impl Into<String>) {
        self.register(name, ActivityType::of::<A>());
    }
}

impl ActivityTypeResolver for MapBasedResolver {
    fn resolve(&self, type_name: &str) -> Result<ActivityType> {
        self.name_to_type
            .get(type_name)
            .copied()
            .ok_or_else(|| SagaError::ActivityTypeNotRegistered(type_name.to_owned()))
    }

    fn get_name(&self, activity_type: ActivityType) -> Result<String> {
        if let Some(name) = self.type_to_name.get(&activity_type.type_id()) {
            return Ok(name.clone());
        }
        // Fallback: instantiate the activity and ask it for its name. Allows
        // serialization of named activities even without explicit
        // registration. Deserialization still requires registration (no
        // name -> type mapping exists otherwise).
        activity_type
            .create()
            .type_name()
            .map(str::to_owned)
            .ok_or_else(|| {
                SagaError::ActivityTypeNotRegistered(activity_type.short_name().to_owned())
            })
    }
}
