//! Watching an inbox: what it receives and what its dispatchers do.
//!
//! The same shape as the session and outbox observers: synchronous,
//! infallible values, composed as tuples, fixed when the inbox is built. An
//! observer observes; one that has to do IO enqueues and lets a task of its
//! own do the awaiting.
//!
//! The events are the actions of the protocol model in
//! `verify/tla/Inbox.tla` — receive, fetch, fail, and the close of the
//! dispatcher's transaction, plus the operator's unpark and resolve — with
//! the steps the model folds into one made visible: the rows stepped over,
//! the row that holds the partition while it waits for its backoff, the
//! subscriber's outcome, the mark.
//!
//! A dispatcher's events name the worker and the number of the `dispatch`
//! call: unlike the outbox, whose position lock allows one transaction per
//! worker, the inbox runs several calls of one worker at once, kept apart by
//! `FOR UPDATE SKIP LOCKED`, and the call number is what tells their events
//! apart.
//!
//! A receipt names the transaction that stored a row, and every step of a
//! walk over the table carries the [`Snapshot`] its statement ran under, so
//! that a recorded run says which rows each walk could see. The order events
//! were logged in is the order the observer was called in, on one thread;
//! two tasks' events may be logged in the other order than the database saw
//! them, and the snapshots settle it.

use std::sync::Arc;
use std::time::Duration;

use crate::error::{BoxError, Error};
use crate::message::InboxMessage;
use crate::pg::{Outcome, Worker};
use crate::snapshot::Snapshot;

/// What the inbox knows about a row once it is stored: the transaction that
/// wrote it and its order of arrival.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Receipt {
    /// `pg_current_xact_id()` of the storing transaction.
    pub transaction_id: u64,
    /// The row's order of arrival.
    pub received_position: i64,
}

/// A message was handed to the inbox, in a transaction of its own.
pub struct Received<'a> {
    /// The message as handed over.
    pub message: &'a InboxMessage,
    /// Where it landed; `None` when a message of the same identity was
    /// already stored and this one was ignored.
    pub receipt: Option<Receipt>,
}

/// A dispatcher stepped over a row whose causal dependencies are not yet
/// processed; it will be looked at again next time.
pub struct Skipped<'a> {
    /// The worker that stepped over it.
    pub worker: Worker,
    /// The `dispatch` call, numbered per inbox.
    pub call: u64,
    /// The row.
    pub message: &'a InboxMessage,
    /// What the statement that returned the row could see.
    pub snapshot: &'a Snapshot,
}

/// A dispatcher found the oldest row of its partition waiting for its
/// backoff, and took nothing: the row holds the partition, so that the order
/// survives the failure.
pub struct Deferred<'a> {
    /// The worker that found it.
    pub worker: Worker,
    /// The `dispatch` call, numbered per inbox.
    pub call: u64,
    /// The row that waits.
    pub message: &'a InboxMessage,
    /// What the statement that returned the row could see.
    pub snapshot: &'a Snapshot,
}

/// A dispatcher took a row, locked for the length of its transaction.
pub struct Fetched<'a> {
    /// The worker that took it.
    pub worker: Worker,
    /// The `dispatch` call, numbered per inbox.
    pub call: u64,
    /// The row; `None` when nothing was eligible.
    pub message: Option<&'a InboxMessage>,
    /// What the statement that returned the row, or found none, could see.
    pub snapshot: &'a Snapshot,
}

/// The subscriber was given the message and the dispatcher's transaction.
pub struct Handled<'a> {
    /// The worker.
    pub worker: Worker,
    /// The `dispatch` call, numbered per inbox.
    pub call: u64,
    /// The message.
    pub message: &'a InboxMessage,
    /// What the subscriber returned.
    pub outcome: Result<(), &'a BoxError>,
}

/// The message was marked processed, inside the dispatcher's transaction:
/// not durable until [`Dispatched`] reports a commit.
pub struct Marked<'a> {
    /// The worker.
    pub worker: Worker,
    /// The `dispatch` call, numbered per inbox.
    pub call: u64,
    /// The message.
    pub message: &'a InboxMessage,
    /// Its order of processing.
    pub processed_position: i64,
}

/// The subscriber failed: its writes were rolled back to the savepoint and
/// the attempt was recorded, inside the dispatcher's transaction. Not
/// durable until [`Dispatched`] reports a commit.
pub struct Failed<'a> {
    /// The worker.
    pub worker: Worker,
    /// The `dispatch` call, numbered per inbox.
    pub call: u64,
    /// The message.
    pub message: &'a InboxMessage,
    /// What the subscriber returned.
    pub error: &'a BoxError,
    /// Failed attempts so far, this one included.
    pub attempts: u32,
    /// Whether this attempt was the last: the message is parked.
    pub parked: bool,
    /// How long the message waits before it may be taken again.
    pub retry_after: Duration,
}

/// The dispatcher's transaction closed: committed, with what it did, or
/// rolled back, with the error.
pub struct Dispatched<'a> {
    /// The worker whose transaction closed.
    pub worker: Worker,
    /// The `dispatch` call, numbered per inbox.
    pub call: u64,
    /// `Ok`: what was committed; `Err`: nothing was, and whatever row was
    /// held is free again.
    pub outcome: Result<Outcome, &'a Error>,
}

/// An operator gave a parked message another go.
pub struct Unparked<'a> {
    /// The message.
    pub message: &'a InboxMessage,
}

/// An operator marked a parked message processed, without the subscriber's
/// effects.
pub struct Resolved<'a> {
    /// The message.
    pub message: &'a InboxMessage,
    /// Its order of processing.
    pub processed_position: i64,
}

/// What an inbox reports. Every method has an empty default, so an observer
/// implements only what it cares about.
pub trait InboxObserver: Send + Sync {
    /// A message was handed to the inbox.
    fn on_received(&self, _event: &Received<'_>) {}
    /// A row was stepped over for its dependencies.
    fn on_skipped(&self, _event: &Skipped<'_>) {}
    /// The oldest row waits for its backoff; nothing was taken.
    fn on_deferred(&self, _event: &Deferred<'_>) {}
    /// A dispatcher took a row, or found none.
    fn on_fetched(&self, _event: &Fetched<'_>) {}
    /// The subscriber returned.
    fn on_handled(&self, _event: &Handled<'_>) {}
    /// The message was marked processed.
    fn on_marked(&self, _event: &Marked<'_>) {}
    /// The attempt was recorded after the subscriber failed.
    fn on_failed(&self, _event: &Failed<'_>) {}
    /// The dispatcher's transaction closed.
    fn on_dispatched(&self, _event: &Dispatched<'_>) {}
    /// A parked message was given another go.
    fn on_unparked(&self, _event: &Unparked<'_>) {}
    /// A parked message was marked processed by hand.
    fn on_resolved(&self, _event: &Resolved<'_>) {}
}

/// Observes nothing.
impl InboxObserver for () {}

macro_rules! forward_to_each {
    ($($method:ident: $event:ident),* $(,)?) => {
        /// Two observers, told in order.
        impl<A: InboxObserver, B: InboxObserver> InboxObserver for (A, B) {
            $(fn $method(&self, event: &$event<'_>) {
                self.0.$method(event);
                self.1.$method(event);
            })*
        }

        impl<O: InboxObserver + ?Sized> InboxObserver for Arc<O> {
            $(fn $method(&self, event: &$event<'_>) {
                (**self).$method(event);
            })*
        }

        /// Any number of observers, told in order.
        impl<O: InboxObserver> InboxObserver for Vec<O> {
            $(fn $method(&self, event: &$event<'_>) {
                self.iter().for_each(|o| o.$method(event));
            })*
        }
    };
}

forward_to_each! {
    on_received: Received,
    on_skipped: Skipped,
    on_deferred: Deferred,
    on_fetched: Fetched,
    on_handled: Handled,
    on_marked: Marked,
    on_failed: Failed,
    on_dispatched: Dispatched,
    on_unparked: Unparked,
    on_resolved: Resolved,
}
