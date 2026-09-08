//! Tests for the bus itself — the scheme registry and its error paths. The
//! messaging behaviour lives in the adapters and is exercised by their tests.

use ascetic_ddd_bus::adapters::in_memory::InMemoryBroker;
use ascetic_ddd_bus::{Bus, Error, Message};

fn as_text(message: &Message) -> Result<String, ascetic_ddd_bus::BoxError> {
    Ok(String::from_utf8(message.payload().to_vec())?)
}

#[tokio::test]
async fn an_unknown_scheme_is_refused() {
    let mut bus = Bus::new();
    bus.register("in-memory", InMemoryBroker::new()).unwrap();

    let refused = bus.consumer("kafka://x", "g", as_text).err();
    assert!(matches!(refused, Some(Error::UnknownScheme(scheme)) if scheme == "kafka"));
}

#[tokio::test]
async fn a_scheme_is_registered_once() {
    let mut bus = Bus::new();
    bus.register("in-memory", InMemoryBroker::new()).unwrap();

    let refused = bus.register("in-memory", InMemoryBroker::new()).err();
    assert!(matches!(refused, Some(Error::AlreadyRegistered(scheme)) if scheme == "in-memory"));
}

#[tokio::test]
async fn a_uri_without_a_scheme_is_refused() {
    let mut bus = Bus::new();
    bus.register("in-memory", InMemoryBroker::new()).unwrap();

    let refused = bus.consumer("no-scheme-here", "g", as_text).err();
    assert!(matches!(refused, Some(Error::UnknownScheme(uri)) if uri == "no-scheme-here"));
}

/// The registry is per bus, not global.
#[tokio::test]
async fn buses_do_not_share_their_registry() {
    let mut first = Bus::new();
    first.register("in-memory", InMemoryBroker::new()).unwrap();
    let second = Bus::new();

    let refused = second.consumer("in-memory://x", "g", as_text).err();
    assert!(matches!(refused, Some(Error::UnknownScheme(scheme)) if scheme == "in-memory"));
}
