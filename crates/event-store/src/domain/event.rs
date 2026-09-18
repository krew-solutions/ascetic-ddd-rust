//! What is known about an event besides the event: its metadata, and its
//! place in the stream.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// About an event: what the sources' `EventMeta` holds, as the `metadata`
/// column keeps it. Ids are text; `occurred_at` is ISO 8601 text, the form
/// every serializable contract carries a date in.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EventMeta {
    /// The event's own id, minted when it is appended.
    #[serde(default)]
    pub event_id: Option<String>,
    /// What caused it: the command, or the event before it in the same
    /// append.
    #[serde(default)]
    pub causation_id: Option<String>,
    /// What it belongs to, across services.
    #[serde(default)]
    pub correlation_id: Option<String>,
    /// Why, in words.
    #[serde(default)]
    pub reason: Option<String>,
    /// When, as ISO 8601 text.
    #[serde(default)]
    pub occurred_at: Option<String>,
    /// The events elsewhere that must be processed before this one: what
    /// the inbox reads as `causal_dependencies`.
    #[serde(default)]
    pub causal_dependencies: Vec<CausalDependency>,
}

/// An event this one depends on: the identity of a row in a stream, in the
/// shape the inbox keeps.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CausalDependency {
    /// The tenant of the event depended on.
    pub tenant_id: String,
    /// Its stream type.
    pub stream_type: String,
    /// Its stream id.
    pub stream_id: Value,
    /// Its position in the stream.
    pub stream_position: i32,
}

/// What an append recorded for one event: its position, and its metadata
/// with the id minted and the cause chained.
#[derive(Clone, Debug, PartialEq)]
pub struct Appended {
    /// The event's position in the stream, from one.
    pub position: i32,
    /// The event's metadata as stored.
    pub meta: EventMeta,
}

/// An event read back: the event, its position and its metadata.
#[derive(Clone, Debug, PartialEq)]
pub struct Recorded<E> {
    /// The event's position in the stream, from one.
    pub position: i32,
    /// The event.
    pub event: E,
    /// Its metadata.
    pub meta: EventMeta,
}
