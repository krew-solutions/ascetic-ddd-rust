//! How messages are shared between workers.
//!
//! A worker takes the messages whose partition key hashes to it. The key is
//! a SQL expression over the inbox row, so the choice is made once, at
//! construction, and the database does the hashing.

/// A partition key, as a SQL expression over the columns of the inbox.
pub trait PartitionKey: Send + Sync {
    /// The expression; `hashtext(<expression>)` picks the worker.
    fn sql_expression(&self) -> &str;
}

/// Partition by URI: messages of one URI go to one worker. For messages
/// whose order comes from the broker's topic and partition.
#[derive(Clone, Copy, Debug, Default)]
pub struct ByUri;

impl PartitionKey for ByUri {
    fn sql_expression(&self) -> &str {
        "uri"
    }
}

/// Partition by stream: messages of one `(tenant, stream_type, stream_id)`
/// go to one worker. For messages whose order is the stream's own — the
/// usual case for aggregate events, and the right one when causal
/// dependencies stay within a stream.
#[derive(Clone, Copy, Debug, Default)]
pub struct ByStream;

impl PartitionKey for ByStream {
    fn sql_expression(&self) -> &str {
        "tenant_id || ':' || stream_type || ':' || stream_id::text"
    }
}
