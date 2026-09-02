//! Tests for `WorkItemArguments`.

use ascetic_ddd_saga::{SagaError, Value, WorkItemArguments};

#[test]
fn create_empty() {
    let arguments = WorkItemArguments::new();

    assert_eq!(arguments.len(), 0);
}

#[test]
fn create_with_data() {
    let arguments = WorkItemArguments::from([
        ("vehicleType", Value::from("Compact")),
        ("days", Value::from(5)),
    ]);

    assert_eq!(arguments.get_str("vehicleType").unwrap(), "Compact");
    assert_eq!(arguments.get_i64("days").unwrap(), 5);
}

/// Python subclasses `dict`; Rust dereferences to the underlying map.
#[test]
fn behaves_like_a_map() {
    let arguments = WorkItemArguments::from([("a", 1)]);

    assert_eq!(arguments.as_map().len(), 1);
    assert_eq!(
        arguments.keys().map(String::as_str).collect::<Vec<_>>(),
        ["a"],
    );
}

#[test]
fn set_and_get_items() {
    let mut arguments = WorkItemArguments::new();
    arguments.insert("destination", "Paris");

    assert_eq!(arguments.get_str("destination").unwrap(), "Paris");
    assert!(arguments.contains_key("destination"));
}

#[test]
fn missing_key_is_reported() {
    let arguments = WorkItemArguments::from([("a", 1)]);

    assert!(matches!(
        arguments.require("missing"),
        Err(SagaError::MissingKey(key)) if key == "missing",
    ));
    assert_eq!(arguments.get("missing"), None);
}

/// Rust-specific: a key holding the wrong type is reported, not coerced.
#[test]
fn unexpected_type_is_reported() {
    let arguments = WorkItemArguments::from([("days", 5)]);

    assert!(matches!(
        arguments.get_str("days"),
        Err(SagaError::UnexpectedType { key, expected }) if key == "days" && expected == "string",
    ));
}
