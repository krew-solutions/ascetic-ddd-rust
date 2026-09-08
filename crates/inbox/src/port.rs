//! The port: what the receiving side sees.

use crate::error::Error;
use crate::message::InboxMessage;

/// Receiving into the inbox.
///
/// This is all a message handler at the edge — a bus subscriber, a webhook
/// — needs: it hands the message over and is done. Processing is the
/// business of a separate loop, and lives on the adapter. A test double
/// implements this by collecting messages.
pub trait Inbox: Send + Sync {
    /// Stores the message. A message with the identity of one already
    /// stored is ignored: receiving is idempotent.
    ///
    /// Whether the returned future is `Send` follows from the implementation,
    /// as with `Session::atomic`: through a concrete inbox it is, through a
    /// generic `I: Inbox` it is not promised.
    fn publish(&self, message: &InboxMessage) -> impl Future<Output = Result<(), Error>>;
}
