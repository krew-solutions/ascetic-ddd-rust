//! Watching an inbox: what it receives and what its dispatchers do.
//!
//! The same shape as the session and outbox observers: synchronous,
//! infallible values, composed as tuples, fixed when the inbox is built. An
//! observer observes; one that has to do IO enqueues and lets a task of its
//! own do the awaiting.
//!
//! The events are the actions of the protocol model in
//! `verify/tla/Inbox.tla` — receive, fetch, and the close of the
//! dispatcher's transaction — with the steps the model folds into one made
//! visible: the rows stepped over, the subscriber's outcome, the mark.
//!
//! A dispatcher's events name the worker and the number of the `dispatch`
//! call: unlike the outbox, whose position lock allows one transaction per
//! worker, the inbox runs several calls of one worker at once, kept apart by
//! `FOR UPDATE SKIP LOCKED`, and the call number is what tells their events
//! apart.

use std::sync::Arc;

use crate::error::{BoxError, Error};
use crate::message::InboxMessage;
use crate::pg::Worker;

/// A message was handed to the inbox, in a transaction of its own.
pub struct Received<'a> {
    /// The message as handed over.
    pub message: &'a InboxMessage,
    /// Its order of arrival; `None` when a message of the same identity was
    /// already stored and this one was ignored.
    pub received_position: Option<i64>,
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
}

/// A dispatcher took a row, locked for the length of its transaction.
pub struct Fetched<'a> {
    /// The worker that took it.
    pub worker: Worker,
    /// The `dispatch` call, numbered per inbox.
    pub call: u64,
    /// The row; `None` when nothing was eligible.
    pub message: Option<&'a InboxMessage>,
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

/// The dispatcher's transaction closed: committed, with whether there was a
/// message, or rolled back, with the error.
pub struct Dispatched<'a> {
    /// The worker whose transaction closed.
    pub worker: Worker,
    /// The `dispatch` call, numbered per inbox.
    pub call: u64,
    /// `Ok(true)`: a message was marked; `Ok(false)`: there was nothing;
    /// `Err`: the subscriber's writes and the mark were rolled back and the
    /// message will be taken again.
    pub outcome: Result<bool, &'a Error>,
}

/// What an inbox reports. Every method has an empty default, so an observer
/// implements only what it cares about.
pub trait InboxObserver: Send + Sync {
    /// A message was handed to the inbox.
    fn on_received(&self, _event: &Received<'_>) {}
    /// A row was stepped over for its dependencies.
    fn on_skipped(&self, _event: &Skipped<'_>) {}
    /// A dispatcher took a row, or found none.
    fn on_fetched(&self, _event: &Fetched<'_>) {}
    /// The subscriber returned.
    fn on_handled(&self, _event: &Handled<'_>) {}
    /// The message was marked processed.
    fn on_marked(&self, _event: &Marked<'_>) {}
    /// The dispatcher's transaction closed.
    fn on_dispatched(&self, _event: &Dispatched<'_>) {}
}

/// Observes nothing.
impl InboxObserver for () {}

/// Two observers, told in order.
impl<A: InboxObserver, B: InboxObserver> InboxObserver for (A, B) {
    fn on_received(&self, event: &Received<'_>) {
        self.0.on_received(event);
        self.1.on_received(event);
    }
    fn on_skipped(&self, event: &Skipped<'_>) {
        self.0.on_skipped(event);
        self.1.on_skipped(event);
    }
    fn on_fetched(&self, event: &Fetched<'_>) {
        self.0.on_fetched(event);
        self.1.on_fetched(event);
    }
    fn on_handled(&self, event: &Handled<'_>) {
        self.0.on_handled(event);
        self.1.on_handled(event);
    }
    fn on_marked(&self, event: &Marked<'_>) {
        self.0.on_marked(event);
        self.1.on_marked(event);
    }
    fn on_dispatched(&self, event: &Dispatched<'_>) {
        self.0.on_dispatched(event);
        self.1.on_dispatched(event);
    }
}

impl<O: InboxObserver + ?Sized> InboxObserver for Arc<O> {
    fn on_received(&self, event: &Received<'_>) {
        (**self).on_received(event);
    }
    fn on_skipped(&self, event: &Skipped<'_>) {
        (**self).on_skipped(event);
    }
    fn on_fetched(&self, event: &Fetched<'_>) {
        (**self).on_fetched(event);
    }
    fn on_handled(&self, event: &Handled<'_>) {
        (**self).on_handled(event);
    }
    fn on_marked(&self, event: &Marked<'_>) {
        (**self).on_marked(event);
    }
    fn on_dispatched(&self, event: &Dispatched<'_>) {
        (**self).on_dispatched(event);
    }
}

/// Any number of observers, told in order.
impl<O: InboxObserver> InboxObserver for Vec<O> {
    fn on_received(&self, event: &Received<'_>) {
        self.iter().for_each(|o| o.on_received(event));
    }
    fn on_skipped(&self, event: &Skipped<'_>) {
        self.iter().for_each(|o| o.on_skipped(event));
    }
    fn on_fetched(&self, event: &Fetched<'_>) {
        self.iter().for_each(|o| o.on_fetched(event));
    }
    fn on_handled(&self, event: &Handled<'_>) {
        self.iter().for_each(|o| o.on_handled(event));
    }
    fn on_marked(&self, event: &Marked<'_>) {
        self.iter().for_each(|o| o.on_marked(event));
    }
    fn on_dispatched(&self, event: &Dispatched<'_>) {
        self.iter().for_each(|o| o.on_dispatched(event));
    }
}
