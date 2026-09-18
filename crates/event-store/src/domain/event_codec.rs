//! An event as a record, and back: the typed half of storage.

use serde_json::Value;

use crate::error::BoxError;

/// An event as the table keeps it: a type name, a version of that type's
/// shape, and the payload as JSON, before any byte codec.
#[derive(Clone, Debug, PartialEq)]
pub struct Encoded {
    /// The event's type, `OrderPlaced`.
    pub event_type: String,
    /// The version of the type's shape, from one.
    pub event_version: i32,
    /// The event's fields.
    pub payload: Value,
}

/// Turns the application's events into records and records back into
/// events. Two functions over values: what the sources did with an exporter
/// per event type and a reconstitutor per type and version.
pub trait EventCodec<E>: Send + Sync {
    /// The record of `event`.
    fn encode(&self, event: &E) -> Result<Encoded, BoxError>;

    /// The event of `encoded`; an error for a type or a version this codec
    /// does not know.
    fn decode(&self, encoded: Encoded) -> Result<E, BoxError>;
}
