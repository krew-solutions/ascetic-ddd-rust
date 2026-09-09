//! What the outbox stores.

use serde_json::Value;

/// A message in the outbox.
///
/// The three fields a producer sets are the routing URI, the payload and the
/// metadata; the rest the database assigns on insert and the dispatcher reads
/// back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxMessage {
    /// Where the message goes: `kafka://orders`, `kafka://orders/order-123`.
    /// The part after the topic is a partition key: messages with one full
    /// URI are dispatched by one worker, in order.
    pub uri: String,
    /// The message as it goes on the wire: serialized, and encrypted where
    /// the deployment requires it, before it reaches the outbox (ADR-0002).
    pub payload: Vec<u8>,
    /// About the message: must carry an `message_id` (a UUID) for idempotency,
    /// and may carry `correlation_id`, `causation_id` and the like.
    pub metadata: Value,
    /// When the row was inserted, as the database prints it.
    pub created_at: Option<String>,
    /// Order within the transaction that inserted it.
    pub position: Option<i64>,
    /// The inserting transaction, `pg_current_xact_id()`.
    pub transaction_id: Option<u64>,
}

impl OutboxMessage {
    /// A message to publish.
    pub fn new(uri: impl Into<String>, payload: impl Into<Vec<u8>>, metadata: Value) -> Self {
        OutboxMessage {
            uri: uri.into(),
            payload: payload.into(),
            metadata,
            created_at: None,
            position: None,
            transaction_id: None,
        }
    }
}

/// Where a consumer group is: the last transaction it acknowledged, and the
/// position within it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Position {
    /// The last acknowledged transaction; `0` before any.
    pub transaction_id: u64,
    /// The last acknowledged position within that transaction.
    pub offset: i64,
}
