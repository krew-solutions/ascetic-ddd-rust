//! What a data-encryption key protects.

use std::fmt;

use serde_json::Value;

use super::canonical;

/// One thing of one tenant, named by its kind and its id: what a DEK is
/// for. An event store names its aggregate's stream this way, a document
/// store its document; the id is JSON, so a composite id fits.
#[derive(Clone, Debug, PartialEq)]
pub struct Resource {
    tenant_id: String,
    kind: String,
    id: Value,
}

impl Resource {
    /// The resource `id` of `kind`, of tenant `tenant_id`.
    pub fn new(
        tenant_id: impl Into<String>,
        kind: impl Into<String>,
        id: impl Into<Value>,
    ) -> Self {
        Resource {
            tenant_id: tenant_id.into(),
            kind: kind.into(),
            id: id.into(),
        }
    }

    /// The tenant.
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// The kind: the aggregate type, the table, the collection.
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// The id within the kind.
    pub fn id(&self) -> &Value {
        &self.id
    }

    /// The canonical text: the three as a JSON array, the id's object keys
    /// in order wherever they occur, so that one resource has one text
    /// however its id was built. This is the associated data of the
    /// resource's ciphers — what binds a ciphertext to its resource — and
    /// so must never change for a resource that has data.
    pub fn canonical(&self) -> String {
        canonical::json(&Value::Array(vec![
            Value::String(self.tenant_id.clone()),
            Value::String(self.kind.clone()),
            self.id.clone(),
        ]))
    }
}

impl fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.canonical())
    }
}
