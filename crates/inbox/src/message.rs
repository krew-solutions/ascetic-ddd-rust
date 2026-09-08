//! What the inbox stores.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// An incoming message.
///
/// Its identity is `(tenant_id, stream_type, stream_id, stream_position)`:
/// the same message arriving twice is the same row, which is what makes
/// processing idempotent. The stream is usually an aggregate — its type, its
/// id and its version — but may as well be a topic, a partition key and an
/// offset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboxMessage {
    /// The tenant; a fixed value when there is only one.
    pub tenant_id: String,
    /// The kind of stream: `context.Aggregate`, or a topic.
    pub stream_type: String,
    /// Which stream: an aggregate id, simple or composite, as JSON.
    pub stream_id: Value,
    /// Where in the stream: the aggregate's version, or an offset.
    pub stream_position: i32,
    /// Where the message came from: `kafka://orders/order-123`.
    pub uri: String,
    /// The message itself. By convention carries a `type`.
    pub payload: Value,
    /// About the message: `event_id`, and `causal_dependencies` — messages
    /// that must be processed before this one.
    pub metadata: Option<Value>,
    /// Order of arrival, assigned by the database.
    pub received_position: Option<i64>,
    /// Order of processing, assigned when processed; `None` until then.
    pub processed_position: Option<i64>,
}

impl InboxMessage {
    /// A message to receive.
    pub fn new(
        tenant_id: impl Into<String>,
        stream_type: impl Into<String>,
        stream_id: Value,
        stream_position: i32,
        uri: impl Into<String>,
        payload: Value,
    ) -> Self {
        InboxMessage {
            tenant_id: tenant_id.into(),
            stream_type: stream_type.into(),
            stream_id,
            stream_position,
            uri: uri.into(),
            payload,
            metadata: None,
            received_position: None,
            processed_position: None,
        }
    }

    /// The same message with metadata.
    pub fn with_metadata(self, metadata: Value) -> Self {
        InboxMessage {
            metadata: Some(metadata),
            ..self
        }
    }

    /// The same message, to be processed only after `dependencies`.
    pub fn depending_on(self, dependencies: &[CausalDependency]) -> Self {
        let mut metadata = self
            .metadata
            .unwrap_or_else(|| Value::Object(Default::default()));
        if let Value::Object(fields) = &mut metadata {
            fields.insert(
                "causal_dependencies".to_owned(),
                serde_json::to_value(dependencies).unwrap_or(Value::Array(Vec::new())),
            );
        }
        InboxMessage {
            metadata: Some(metadata),
            ..self
        }
    }

    /// The messages this one must wait for. Entries that are not
    /// dependencies are ignored.
    pub fn causal_dependencies(&self) -> Vec<CausalDependency> {
        self.metadata
            .as_ref()
            .and_then(|metadata| metadata.get("causal_dependencies"))
            .and_then(Value::as_array)
            .map(|entries| {
                entries
                    .iter()
                    .filter_map(|entry| serde_json::from_value(entry.clone()).ok())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The event id, if the metadata carries one.
    pub fn event_id(&self) -> Option<&str> {
        self.metadata
            .as_ref()
            .and_then(|metadata| metadata.get("event_id"))
            .and_then(Value::as_str)
    }
}

/// A message that must be processed before another: the identity of a row
/// in the inbox.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CausalDependency {
    /// The tenant of the message depended on.
    pub tenant_id: String,
    /// Its stream type.
    pub stream_type: String,
    /// Its stream id.
    pub stream_id: Value,
    /// Its position in the stream.
    pub stream_position: i32,
}

impl CausalDependency {
    /// The identity of `message`, as a dependency on it.
    pub fn on(message: &InboxMessage) -> Self {
        CausalDependency {
            tenant_id: message.tenant_id.clone(),
            stream_type: message.stream_type.clone(),
            stream_id: message.stream_id.clone(),
            stream_position: message.stream_position,
        }
    }
}
