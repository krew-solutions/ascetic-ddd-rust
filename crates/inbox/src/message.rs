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
    /// The message as it came off the wire: serialized, and encrypted where
    /// the deployment requires it (ADR-0002).
    pub payload: Vec<u8>,
    /// About the message: `message_id`, and `causal_dependencies` — messages
    /// that must be processed before this one.
    pub metadata: Option<Value>,
    /// Order of arrival, assigned by the database.
    pub received_position: Option<i64>,
    /// Order of processing, assigned when processed; `None` until then.
    pub processed_position: Option<i64>,
    /// Failed attempts so far (ADR-0005).
    pub attempts: u32,
    /// What the subscriber returned the last time it failed.
    pub last_error: Option<String>,
    /// The dependency the row is set aside to wait for, if any (ADR-0006).
    pub waiting_for: Option<CausalDependency>,
}

impl InboxMessage {
    /// A message to receive.
    pub fn new(
        tenant_id: impl Into<String>,
        stream_type: impl Into<String>,
        stream_id: Value,
        stream_position: i32,
        uri: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        InboxMessage {
            tenant_id: tenant_id.into(),
            stream_type: stream_type.into(),
            stream_id,
            stream_position,
            uri: uri.into(),
            payload: payload.into(),
            metadata: None,
            received_position: None,
            processed_position: None,
            attempts: 0,
            last_error: None,
            waiting_for: None,
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

    /// The messages this one must wait for, read as the intake reads them
    /// (ADR-0037 decision 4): none when the metadata names none, and the
    /// reason when the list is not a list of dependencies. The list is the
    /// header's JSON text, or, in a row stored before, the list itself.
    ///
    /// # Errors
    /// The reason, in the library's words, when the list does not parse,
    /// an entry is not a dependency, or there are more than
    /// [`MAX_DEPENDENCIES`].
    pub fn checked_causal_dependencies(&self) -> Result<Vec<CausalDependency>, &'static str> {
        match self
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("causal_dependencies"))
        {
            None => Ok(Vec::new()),
            Some(Value::String(text)) => crate::intake::dependencies_of(text),
            Some(structured) => crate::intake::dependencies_of(&structured.to_string()),
        }
    }

    /// The messages this one must wait for; none when the list is not one
    /// ([`InboxMessage::checked_causal_dependencies`] says why).
    pub fn causal_dependencies(&self) -> Vec<CausalDependency> {
        self.checked_causal_dependencies().unwrap_or_default()
    }

    /// The message id, if the metadata carries one.
    pub fn message_id(&self) -> Option<&str> {
        self.metadata
            .as_ref()
            .and_then(|metadata| metadata.get("message_id"))
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

/// The most causal dependencies one message names (ADR-0037 decision 4):
/// dispatch looks each up in turn and starts again after every wake, so
/// dependencies arriving one by one cost a square of their number.
pub const MAX_DEPENDENCIES: usize = 64;

/// The most bytes an identity's stored form may take (ADR-0037 decision 4):
/// a btree's item on an 8 KiB page is 2704 bytes, the index tuple's header
/// is 8, and `__waiting_idx` indexes a dependency's identity as `jsonb`.
/// The row's own key is smaller than its waiting form, so an identity
/// within it can be stored and depended on.
pub const MAX_IDENTITY_BYTES: usize = 2696;

/// The bytes of the identity's four keys.
const IDENTITY_KEY_BYTES: usize =
    "tenant_id".len() + "stream_type".len() + "stream_id".len() + "stream_position".len();

/// A number's most bytes inside `jsonb`: a `numeric` of up to 20 digits is
/// 16 bytes, after up to 3 of alignment.
const NUMBER_BYTES: usize = 3 + 16;

/// A position's most bytes: an `i32` is a `numeric` of at most 12 bytes.
const POSITION_BYTES: usize = 3 + 12;

/// An upper bound on the bytes of `value` inside a `jsonb` container: a
/// string its bytes; a number its largest `numeric`; a container its header,
/// an entry of 4 bytes per element and of 8 per member, alignment, and what
/// it holds.
fn stored_bytes(value: &Value) -> usize {
    match value {
        Value::String(text) => text.len(),
        Value::Number(_) => NUMBER_BYTES,
        Value::Bool(_) | Value::Null => 0,
        Value::Array(items) => {
            3 + 4 + 4 * items.len() + items.iter().map(stored_bytes).sum::<usize>()
        }
        Value::Object(fields) => {
            3 + 4
                + 8 * fields.len()
                + fields
                    .iter()
                    .map(|(name, field)| name.len() + stored_bytes(field))
                    .sum::<usize>()
        }
    }
}

/// An upper bound on the bytes of an identity as the `jsonb` datum
/// `__waiting_idx` indexes: its varlena and object headers, four members,
/// their keys and values, the position at its largest.
pub(crate) fn identity_bytes(tenant_id: &str, stream_type: &str, stream_id: &Value) -> usize {
    4 + 4
        + 8 * 4
        + IDENTITY_KEY_BYTES
        + tenant_id.len()
        + stream_type.len()
        + stored_bytes(stream_id)
        + POSITION_BYTES
}

impl CausalDependency {
    /// An upper bound on the bytes this identity takes where the inbox
    /// indexes it, to hold against [`MAX_IDENTITY_BYTES`].
    #[must_use]
    pub fn stored_bytes(&self) -> usize {
        identity_bytes(&self.tenant_id, &self.stream_type, &self.stream_id)
    }

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
