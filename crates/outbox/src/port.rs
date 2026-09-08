//! The port: what the application layer sees.

use ascetic_ddd_session::Session;

use crate::error::Error;
use crate::message::OutboxMessage;

/// Publishing into the outbox, inside the caller's transaction.
///
/// This is all the application layer needs: it publishes; dispatching is
/// the business of a separate process, and lives on the adapter. A test
/// double implements this by collecting messages.
pub trait Outbox<S: Session>: Send + Sync {
    /// Stores the message within the transaction of `session`, so that it
    /// is committed with the state change or not at all.
    fn publish(
        &self,
        session: &S,
        message: &OutboxMessage,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}
