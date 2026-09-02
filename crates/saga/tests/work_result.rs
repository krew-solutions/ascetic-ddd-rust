//! Tests for `WorkResult`.

use ascetic_ddd_saga::{Value, WorkResult};

#[test]
fn create_empty() {
    let result = WorkResult::new();

    assert_eq!(result.len(), 0);
}

#[test]
fn create_with_data() {
    let result = WorkResult::from([
        ("reservationId", Value::from(12345)),
        ("status", Value::from("confirmed")),
    ]);

    assert_eq!(result.get_i64("reservationId").unwrap(), 12345);
    assert_eq!(result.get_str("status").unwrap(), "confirmed");
}

/// Python subclasses `dict`; Rust dereferences to the underlying map.
#[test]
fn behaves_like_a_map() {
    let result = WorkResult::from([("key", "value")]);

    assert_eq!(result.as_map().len(), 1);
    assert_eq!(result.get("key"), Some(&Value::from("value")));
}

#[test]
fn set_and_get_items() {
    let mut result = WorkResult::new();
    result.insert("key", "value");

    assert_eq!(result.get_str("key").unwrap(), "value");
    assert!(result.contains_key("key"));
}

#[test]
fn update_from_another_map() {
    let mut result = WorkResult::from([("a", 1)]);
    result
        .as_map_mut()
        .extend(WorkResult::from([("b", 2), ("c", 3)]));

    assert_eq!(result.get_i64("a").unwrap(), 1);
    assert_eq!(result.get_i64("b").unwrap(), 2);
    assert_eq!(result.get_i64("c").unwrap(), 3);
}
