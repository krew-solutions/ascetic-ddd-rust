//! Watching an outbox: what it publishes and what its dispatchers do.
//!
//! The same shape as the session observers: synchronous, infallible values,
//! composed as tuples, fixed when the outbox is built. An observer observes;
//! one that has to do IO enqueues and lets a task of its own do the awaiting.
//!
//! The five events are the actions of the protocol model in
//! `verify/tla/Outbox.tla` — publish, fetch, handle, acknowledge, and the
//! close of the dispatcher's transaction — so a recording observer yields a
//! trace the model can be checked against.

use std::sync::Arc;

use crate::error::{BoxError, Error};
use crate::message::{OutboxMessage, Position};
use crate::pg::Worker;

/// What the outbox knows about a message once it is written: the id of the
/// writing transaction and the message's position in the table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Receipt {
    /// `pg_current_xact_id()` of the publishing transaction.
    pub transaction_id: u64,
    /// The message's serial.
    pub position: i64,
}

/// A message was written into the outbox, inside the caller's transaction.
pub struct Published<'a> {
    /// The message as published.
    pub message: &'a OutboxMessage,
    /// Where it landed.
    pub receipt: Receipt,
}

/// A dispatcher read a batch.
pub struct Fetched<'a> {
    /// The consumer group, worker suffix included.
    pub group: &'a str,
    /// The worker that read.
    pub worker: Worker,
    /// The visibility horizon of the statement that read the batch,
    /// `pg_snapshot_xmin`: every transaction below it had ended. Unknown when
    /// the batch is empty, because the horizon travels with the rows.
    pub horizon: Option<u64>,
    /// The batch, in `(transaction_id, position)` order; empty when there was
    /// nothing to dispatch.
    pub messages: &'a [OutboxMessage],
}

/// The subscriber was handed a message of the batch.
pub struct Handled<'a> {
    /// The consumer group, worker suffix included.
    pub group: &'a str,
    /// The worker that handed it over.
    pub worker: Worker,
    /// The message.
    pub message: &'a OutboxMessage,
    /// What the subscriber returned.
    pub outcome: Result<(), &'a BoxError>,
}

/// The position of the group moved past the batch, inside the dispatcher's
/// transaction: not durable until [`Dispatched`] reports a commit.
pub struct Acked<'a> {
    /// The consumer group, worker suffix included.
    pub group: &'a str,
    /// The worker that acknowledged.
    pub worker: Worker,
    /// The last message of the batch.
    pub position: Position,
}

/// The dispatcher's transaction closed: committed, with whether there was a
/// batch, or rolled back, with the error.
pub struct Dispatched<'a> {
    /// The consumer group, worker suffix included.
    pub group: &'a str,
    /// The worker whose transaction closed.
    pub worker: Worker,
    /// `Ok(true)`: a batch was acknowledged; `Ok(false)`: there was nothing;
    /// `Err`: the batch was rolled back and will come again.
    pub outcome: Result<bool, &'a Error>,
}

/// What an outbox reports. Every method has an empty default, so an observer
/// implements only what it cares about.
pub trait OutboxObserver: Send + Sync {
    /// A message was written, inside the caller's transaction.
    fn on_published(&self, _event: &Published<'_>) {}
    /// A dispatcher read a batch.
    fn on_fetched(&self, _event: &Fetched<'_>) {}
    /// A message was handed to the subscriber.
    fn on_handled(&self, _event: &Handled<'_>) {}
    /// The position was moved past the batch.
    fn on_acked(&self, _event: &Acked<'_>) {}
    /// The dispatcher's transaction closed.
    fn on_dispatched(&self, _event: &Dispatched<'_>) {}
}

/// Observes nothing.
impl OutboxObserver for () {}

/// Two observers, told in order.
impl<A: OutboxObserver, B: OutboxObserver> OutboxObserver for (A, B) {
    fn on_published(&self, event: &Published<'_>) {
        self.0.on_published(event);
        self.1.on_published(event);
    }
    fn on_fetched(&self, event: &Fetched<'_>) {
        self.0.on_fetched(event);
        self.1.on_fetched(event);
    }
    fn on_handled(&self, event: &Handled<'_>) {
        self.0.on_handled(event);
        self.1.on_handled(event);
    }
    fn on_acked(&self, event: &Acked<'_>) {
        self.0.on_acked(event);
        self.1.on_acked(event);
    }
    fn on_dispatched(&self, event: &Dispatched<'_>) {
        self.0.on_dispatched(event);
        self.1.on_dispatched(event);
    }
}

impl<O: OutboxObserver + ?Sized> OutboxObserver for Arc<O> {
    fn on_published(&self, event: &Published<'_>) {
        (**self).on_published(event);
    }
    fn on_fetched(&self, event: &Fetched<'_>) {
        (**self).on_fetched(event);
    }
    fn on_handled(&self, event: &Handled<'_>) {
        (**self).on_handled(event);
    }
    fn on_acked(&self, event: &Acked<'_>) {
        (**self).on_acked(event);
    }
    fn on_dispatched(&self, event: &Dispatched<'_>) {
        (**self).on_dispatched(event);
    }
}

/// Any number of observers, told in order.
impl<O: OutboxObserver> OutboxObserver for Vec<O> {
    fn on_published(&self, event: &Published<'_>) {
        self.iter().for_each(|o| o.on_published(event));
    }
    fn on_fetched(&self, event: &Fetched<'_>) {
        self.iter().for_each(|o| o.on_fetched(event));
    }
    fn on_handled(&self, event: &Handled<'_>) {
        self.iter().for_each(|o| o.on_handled(event));
    }
    fn on_acked(&self, event: &Acked<'_>) {
        self.iter().for_each(|o| o.on_acked(event));
    }
    fn on_dispatched(&self, event: &Dispatched<'_>) {
        self.iter().for_each(|o| o.on_dispatched(event));
    }
}
