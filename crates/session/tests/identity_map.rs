//! Tests for `IdentityMap`.
//!
//! Ported from the Python suite, plus the LRU-eviction case the Go port adds.
//! Python's `KeyError` becomes [`Lookup::Unknown`] and its `ObjectNotFound`
//! becomes [`Lookup::Absent`], so the three-way outcome is asserted directly
//! instead of through raised exceptions.

use std::sync::Arc;

use ascetic_ddd_session::{IdentityKey, IdentityMap, IsolationLevel, Lookup};

#[derive(Debug, PartialEq)]
struct Model {
    id: i64,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct ModelKey(i64);

impl IdentityKey for ModelKey {
    type Entity = Model;
}

#[derive(Debug, PartialEq)]
struct AnotherModel {
    id: i64,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct AnotherModelKey(i64);

impl IdentityKey for AnotherModelKey {
    type Entity = AnotherModel;
}

fn model(id: i64) -> Arc<Model> {
    Arc::new(Model { id })
}

fn serializable() -> IdentityMap {
    IdentityMap::with_isolation(IsolationLevel::Serializable)
}

// --------------------------- basic behaviour ---------------------------

#[test]
fn get_returns_the_very_same_instance() {
    let map = serializable();
    let entity = model(3);
    map.add(ModelKey(3), Arc::clone(&entity));

    let Lookup::Found(found) = map.get(&ModelKey(3)) else {
        panic!("expected the entity to be found");
    };

    assert!(Arc::ptr_eq(&entity, &found));
    assert!(matches!(map.get(&ModelKey(10)), Lookup::Unknown));
}

/// The map holds entities weakly: an entity dropped by the domain survives
/// inside the anchor window.
#[test]
fn entity_survives_being_dropped_by_the_domain() {
    let map = IdentityMap::new(10, IsolationLevel::Serializable);
    let entity = model(3);
    let address = Arc::as_ptr(&entity);
    map.add(ModelKey(3), entity);
    // The domain no longer holds it; only the map's anchor does.

    let Lookup::Found(found) = map.get(&ModelKey(3)) else {
        panic!("expected the entity to be found");
    };

    assert_eq!(Arc::as_ptr(&found), address);
}

/// Once the anchor window pushes it out and nobody else holds it, the entry
/// disappears.
#[test]
fn entity_is_forgotten_when_crowded_out_of_the_window() {
    let map = IdentityMap::new(1, IsolationLevel::Serializable);
    map.add(ModelKey(3), model(3));
    map.add(ModelKey(10), model(10));

    assert!(matches!(map.get(&ModelKey(3)), Lookup::Unknown));
    assert!(matches!(map.get(&ModelKey(10)), Lookup::Found(_)));
}

/// But an entity the domain still holds stays reachable even after eviction —
/// this is what a plain LRU cache cannot do.
#[test]
fn entity_held_by_the_domain_survives_eviction() {
    let map = IdentityMap::new(1, IsolationLevel::Serializable);
    let entity = model(3);
    map.add(ModelKey(3), Arc::clone(&entity));
    map.add(ModelKey(10), model(10));

    let Lookup::Found(found) = map.get(&ModelKey(3)) else {
        panic!("the domain still holds the entity, so the map must find it");
    };

    assert!(Arc::ptr_eq(&entity, &found));
}

#[test]
fn has_answers_for_known_keys_only() {
    let map = serializable();
    map.add(ModelKey(3), model(3));

    assert!(map.has(&ModelKey(3)));
    assert!(!map.has(&ModelKey(10)));
}

#[test]
fn remove_forgets_the_key() {
    let map = serializable();
    map.add(ModelKey(3), model(3));

    map.remove(&ModelKey(3));

    assert!(matches!(map.get(&ModelKey(3)), Lookup::Unknown));
    assert!(map.is_empty());
}

#[test]
fn clear_forgets_everything() {
    let map = serializable();
    map.add(ModelKey(3), model(3));

    map.clear();

    assert!(matches!(map.get(&ModelKey(3)), Lookup::Unknown));
    assert!(map.is_empty());
}

/// Two key types carrying the same id are distinct keys.
#[test]
fn different_entity_types_with_the_same_id() {
    let map = serializable();
    let model = model(1);
    let another = Arc::new(AnotherModel { id: 1 });
    map.add(ModelKey(1), Arc::clone(&model));
    map.add(AnotherModelKey(1), Arc::clone(&another));

    let Lookup::Found(found_model) = map.get(&ModelKey(1)) else {
        panic!("expected the model to be found");
    };
    let Lookup::Found(found_another) = map.get(&AnotherModelKey(1)) else {
        panic!("expected the other model to be found");
    };

    assert!(Arc::ptr_eq(&model, &found_model));
    assert!(Arc::ptr_eq(&another, &found_another));
}

// ------------------------- serializable level -------------------------

#[test]
fn serializable_remembers_that_an_entity_is_absent() {
    let map = serializable();
    map.add_absent(ModelKey(1));

    assert!(matches!(map.get(&ModelKey(1)), Lookup::Absent));
}

#[test]
fn serializable_has_reports_a_remembered_absence() {
    let map = serializable();
    map.add_absent(ModelKey(1));

    assert!(map.has(&ModelKey(1)));
}

#[test]
fn serializable_has_is_false_for_an_unloaded_key() {
    let map = serializable();

    assert!(!map.has(&ModelKey(1)));
}

// ------------------------ repeatable read level ------------------------

#[test]
fn repeatable_read_caches_entities() {
    let map = IdentityMap::with_isolation(IsolationLevel::RepeatableRead);
    let entity = model(1);
    map.add(ModelKey(1), Arc::clone(&entity));

    let Lookup::Found(found) = map.get(&ModelKey(1)) else {
        panic!("expected the entity to be found");
    };

    assert!(Arc::ptr_eq(&entity, &found));
}

/// Remembering an absence would be a phantom read below serializable.
#[test]
fn repeatable_read_ignores_absences() {
    let map = IdentityMap::with_isolation(IsolationLevel::RepeatableRead);
    map.add_absent(ModelKey(1));

    assert!(matches!(map.get(&ModelKey(1)), Lookup::Unknown));
}

#[test]
fn repeatable_read_has() {
    let map = IdentityMap::with_isolation(IsolationLevel::RepeatableRead);

    assert!(!map.has(&ModelKey(1)));
    map.add(ModelKey(1), model(1));
    assert!(map.has(&ModelKey(1)));
}

// ------------------------- disabled levels -------------------------

#[test]
fn read_uncommitted_disables_the_map() {
    let map = IdentityMap::with_isolation(IsolationLevel::ReadUncommitted);
    map.add(ModelKey(1), model(1));

    assert!(matches!(map.get(&ModelKey(1)), Lookup::Unknown));
    assert!(!map.has(&ModelKey(1)));
}

#[test]
fn read_committed_disables_the_map() {
    let map = IdentityMap::with_isolation(IsolationLevel::ReadCommitted);
    map.add(ModelKey(1), model(1));
    map.add_absent(ModelKey(2));

    assert!(matches!(map.get(&ModelKey(1)), Lookup::Unknown));
    assert!(matches!(map.get(&ModelKey(2)), Lookup::Unknown));
}

// ----------------------------- window -----------------------------

#[test]
fn touching_a_key_keeps_it_in_the_window() {
    let map = IdentityMap::new(2, IsolationLevel::Serializable);
    map.add(ModelKey(1), model(1));
    map.add(ModelKey(2), model(2));

    // Touch the oldest, so that the next insert evicts the other one.
    assert!(matches!(map.get(&ModelKey(1)), Lookup::Found(_)));
    map.add(ModelKey(3), model(3));

    assert!(matches!(map.get(&ModelKey(1)), Lookup::Found(_)));
    assert!(matches!(map.get(&ModelKey(2)), Lookup::Unknown));
}

#[test]
fn re_adding_a_key_replaces_the_entity() {
    let map = serializable();
    map.add(ModelKey(1), model(1));
    let replacement = model(1);
    map.add(ModelKey(1), Arc::clone(&replacement));

    let Lookup::Found(found) = map.get(&ModelKey(1)) else {
        panic!("expected the entity to be found");
    };

    assert!(Arc::ptr_eq(&replacement, &found));
    assert_eq!(map.len(), 1);
}

#[test]
fn shrinking_the_window_drops_anchors() {
    let map = IdentityMap::new(10, IsolationLevel::Serializable);
    map.add(ModelKey(1), model(1));
    map.add(ModelKey(2), model(2));

    map.set_size(1);

    assert!(matches!(map.get(&ModelKey(1)), Lookup::Unknown));
    assert!(matches!(map.get(&ModelKey(2)), Lookup::Found(_)));
}
