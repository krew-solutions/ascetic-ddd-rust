//! A stage of the wire: what a message goes through between the typed layer
//! and the transport.
//!
//! A producer encodes a value into a message, then the message goes out
//! through its stages, in order; a consumer's message comes in through the
//! same stages, in reverse, before it is decoded. Sealing is one stage —
//! ADR-0002 places it exactly here, so that nothing between the two ends
//! sees a payload in the clear; compression could be another. The bus knows
//! nothing of what a stage does: bytes in, bytes out, headers if the stage
//! needs them.
//!
//! A stage's failure is the message's. Outbound, the publish fails and the
//! caller's transaction with it. Inbound, the message is not decoded and not
//! skipped: the handling fails, and a transport that can, redelivers — a key
//! service away for a moment must not lose a message. A failure that no
//! retry can mend is marked [`Permanent`][crate::Permanent].

use std::sync::Arc;

use futures::future::BoxFuture;

use crate::error::BoxError;
use crate::message::Message;

/// One transformation of wire messages, out and back in.
pub trait Stage: Send + Sync {
    /// The message as it goes out, transformed.
    fn outbound(&self, message: Message) -> BoxFuture<'_, Result<Message, BoxError>>;

    /// The message as it came in, transformed back.
    fn inbound(&self, message: Message) -> BoxFuture<'_, Result<Message, BoxError>>;
}

/// The stages of a producer or a consumer, in the order they are passed.
pub(crate) type Stages = Vec<Arc<dyn Stage>>;

/// Through every stage, first to last.
pub(crate) async fn outbound(
    stages: &[Arc<dyn Stage>],
    message: Message,
) -> Result<Message, BoxError> {
    let mut message = message;
    for stage in stages {
        message = stage.outbound(message).await?;
    }
    Ok(message)
}

/// Back through every stage, last to first.
pub(crate) async fn inbound(
    stages: &[Arc<dyn Stage>],
    message: Message,
) -> Result<Message, BoxError> {
    let mut message = message;
    for stage in stages.iter().rev() {
        message = stage.inbound(message).await?;
    }
    Ok(message)
}
