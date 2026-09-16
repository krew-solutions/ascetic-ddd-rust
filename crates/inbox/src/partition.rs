//! How messages are shared between slots.
//!
//! A row's slot is the hash of its partition key, a SQL expression over the
//! inbox row, computed once at insert and stored with the row; the choice
//! is made when the table is created, and the database does the hashing.

/// A partition key, as a SQL expression over the columns of the inbox.
pub trait PartitionKey: Send + Sync {
    /// The expression; `hashtext(<expression>) % slots` is the row's slot.
    fn sql_expression(&self) -> &str;
}

/// Partition by URI: messages of one URI go to one slot. For messages
/// whose order comes from the broker's topic and partition.
#[derive(Clone, Copy, Debug, Default)]
pub struct ByUri;

impl PartitionKey for ByUri {
    fn sql_expression(&self) -> &str {
        "uri"
    }
}

/// Partition by stream: messages of one `(tenant, stream_type, stream_id)`
/// go to one slot. For messages whose order is the stream's own — the
/// usual case for aggregate events, and the right one when causal
/// dependencies stay within a stream.
#[derive(Clone, Copy, Debug, Default)]
pub struct ByStream;

impl PartitionKey for ByStream {
    fn sql_expression(&self) -> &str {
        "tenant_id || ':' || stream_type || ':' || stream_id::text"
    }
}
