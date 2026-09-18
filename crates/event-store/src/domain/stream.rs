//! What a stream of events is named by.

use std::fmt;

use ascetic_ddd_dek::Resource;
use serde_json::Value;

/// The name of an aggregate's stream: the tenant, the aggregate's type —
/// `bounded_context.aggregate` in the sources — and its id, as JSON so a
/// composite id fits. The identity the inbox and the outbox carry as
/// headers, and the resource a data-encryption key belongs to.
#[derive(Clone, Debug, PartialEq)]
pub struct StreamId {
    tenant_id: String,
    stream_type: String,
    stream_id: Value,
}

impl StreamId {
    /// The stream `stream_id` of type `stream_type`, of tenant `tenant_id`.
    pub fn new(
        tenant_id: impl Into<String>,
        stream_type: impl Into<String>,
        stream_id: impl Into<Value>,
    ) -> Self {
        StreamId {
            tenant_id: tenant_id.into(),
            stream_type: stream_type.into(),
            stream_id: stream_id.into(),
        }
    }

    /// The tenant.
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// The aggregate's type.
    pub fn stream_type(&self) -> &str {
        &self.stream_type
    }

    /// The aggregate's id.
    pub fn stream_id(&self) -> &Value {
        &self.stream_id
    }
}

/// The stream as the resource its data-encryption key belongs to.
impl From<&StreamId> for Resource {
    fn from(stream: &StreamId) -> Self {
        Resource::new(
            stream.tenant_id.clone(),
            stream.stream_type.clone(),
            stream.stream_id.clone(),
        )
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}/{}",
            self.tenant_id, self.stream_type, self.stream_id
        )
    }
}
