//! Watching an outbox: what it publishes and what its dispatchers do.
//!
//! The same shape as the session observers: synchronous, infallible values,
//! composed as tuples, fixed when the outbox is built. An observer observes;
//! one that has to do IO enqueues and lets a task of its own do the awaiting.
//!
//! Five events are the actions of the protocol model in
//! `verify/tla/Outbox.tla` — publish, fetch, handle, acknowledge, and the
//! close of the dispatcher's transaction — so a recording observer yields a
//! trace the model can be checked against; an epoch's start and a pass of
//! the ring's reclamation are reported beside them. A dispatcher is named by
//! the slot it holds, not by who runs it: it has no other identity.

use std::sync::Arc;
use std::time::Duration;

use crate::error::{BoxError, Error};
use crate::message::{OutboxMessage, Snapshot};

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

/// A dispatcher read a batch of an epoch, or found no slot with work
/// (ADR-0035).
pub struct Fetched<'a> {
    /// The consumer group.
    pub group: &'a str,
    /// The slot whose position row the dispatcher locked; `None` when no
    /// slot of the selection had work.
    pub slot: Option<u32>,
    /// How many slots the table is cut into.
    pub slots: u32,
    /// The `xmin` of the snapshot the batch was read under: kept for the
    /// trace tools, which do not follow epochs yet.
    pub horizon: u64,
    /// The snapshot the batch was read under, taken after the lock; `None`
    /// when no slot had work.
    pub statement: Option<&'a Snapshot>,
    /// What the slot's group had delivered before this batch.
    pub delivered: Option<&'a Snapshot>,
    /// The epoch the batch belongs to.
    pub epoch: Option<&'a Snapshot>,
    /// The last position handed on within the epoch before this batch.
    pub epoch_offset: i64,
    /// The epoch's highest position.
    pub epoch_end: i64,
    /// The most the statement would return: a shorter batch drains the
    /// epoch.
    pub limit: usize,
    /// The batch, in the order its positions were drawn; empty when there was
    /// nothing to hand on.
    pub messages: &'a [OutboxMessage],
}

/// A dispatcher started an epoch with rows of its selection (ADR-0035
/// decision 3), inside its transaction.
pub struct EpochStarted<'a> {
    /// The consumer group.
    pub group: &'a str,
    /// The slot.
    pub slot: u32,
    /// The epoch's snapshot.
    pub epoch: &'a Snapshot,
    /// What the slot's group had delivered.
    pub delivered: &'a Snapshot,
    /// The lowest position of its rows.
    pub first: i64,
    /// The highest: its end.
    pub last: i64,
}

/// The subscriber was handed a message of the batch.
pub struct Handled<'a> {
    /// The consumer group.
    pub group: &'a str,
    /// The slot of the batch.
    pub slot: u32,
    /// The message.
    pub message: &'a OutboxMessage,
    /// What the subscriber returned.
    pub outcome: Result<(), &'a BoxError>,
}

/// The position of the group moved past the batch, inside the dispatcher's
/// transaction: not durable until [`Dispatched`] reports a commit.
pub struct Acked<'a> {
    /// The consumer group.
    pub group: &'a str,
    /// The slot whose position moved.
    pub slot: u32,
    /// The last message of the batch; `None` for an epoch with nothing of
    /// the selection's, delivered at once.
    pub last: Option<Receipt>,
    /// Whether the batch drained its epoch, so that delivered became it.
    pub drained: bool,
    /// The epoch drained before its end: its last row was not found, which
    /// only a row deleted by hand explains. Reported, not waited for: waiting
    /// would wedge the slot.
    pub short: bool,
}

/// The dispatcher's transaction closed: committed, with whether there was a
/// batch, or rolled back, with the error.
pub struct Dispatched<'a> {
    /// The consumer group.
    pub group: &'a str,
    /// The slot whose batch the transaction held, whether it committed or
    /// rolled back; `None` when no slot had work, or nothing was taken yet.
    pub slot: Option<u32>,
    /// `Ok(true)`: a batch was acknowledged; `Ok(false)`: there was nothing;
    /// `Err`: the batch was rolled back and will come again.
    pub outcome: Result<bool, &'a Error>,
}

/// A pass of the ring's reclamation (ADR-0033 decision 3), and how it ended.
#[derive(Clone, Debug, PartialEq)]
pub enum Reclamation {
    /// A generation was emptied.
    Emptied {
        /// The generation.
        generation: u32,
        /// The lowest and highest windows of its rows, `id / window`: every
        /// lap it held.
        windows: (u64, u64),
        /// About how many rows it held, as the planner estimates them.
        rows: i64,
        /// No row is left below it, and none will be written.
        gone_below: u64,
        /// Every row from it on is kept.
        intact_from: u64,
    },
    /// Nothing was emptied, and why.
    Skipped(Skipped),
}

/// Why a pass of the reclamation emptied nothing.
#[derive(Clone, Debug, PartialEq)]
pub enum Skipped {
    /// Another pass holds the reclamation's key.
    Busy,
    /// The ring holds no row.
    Empty,
    /// The generation of the oldest row left may still be written: its newest
    /// window lies within the margin of the next id's.
    NotPast {
        /// The candidate.
        generation: u32,
        /// Its newest row's window.
        newest_window: u64,
        /// The next id's.
        next_window: u64,
    },
    /// A transaction of this database that holds an id in the generation is
    /// open, and could still write into it.
    NotClosed {
        /// The candidate.
        generation: u32,
        /// The open transaction.
        transaction: u64,
    },
    /// A position has not handed on every row of the generation: the group
    /// pinning the ring.
    NotPassed {
        /// The candidate.
        generation: u32,
        /// The pinning position's group.
        group: String,
        /// Its selection.
        uri: String,
        /// Its slot.
        slot: u32,
        /// How long ago its `delivered` last moved; `None` if it never did.
        since: Option<Duration>,
    },
    /// Every group passed the generation less than the margin's age ago: a
    /// row relayed a moment ago can still be re-injected.
    NotOldEnough {
        /// The candidate.
        generation: u32,
        /// How long since a pass first found it passed.
        passed_for: Duration,
    },
    /// A reader held the partition past the lock's timeout.
    Held {
        /// The candidate.
        generation: u32,
    },
    /// Read again under the partition's lock, the generation was not passed:
    /// a row committed between the checks and the lock.
    PassedNoMore {
        /// The candidate.
        generation: u32,
    },
}

/// What an outbox reports. Every method has an empty default, so an observer
/// implements only what it cares about.
pub trait OutboxObserver: Send + Sync {
    /// A message was written, inside the caller's transaction.
    fn on_published(&self, _event: &Published<'_>) {}
    /// A dispatcher read a batch.
    fn on_fetched(&self, _event: &Fetched<'_>) {}
    /// A dispatcher started an epoch.
    fn on_epoch_started(&self, _event: &EpochStarted<'_>) {}
    /// A message was handed to the subscriber.
    fn on_handled(&self, _event: &Handled<'_>) {}
    /// The position was moved past the batch.
    fn on_acked(&self, _event: &Acked<'_>) {}
    /// The dispatcher's transaction closed.
    fn on_dispatched(&self, _event: &Dispatched<'_>) {}
    /// A pass of the ring's reclamation ended.
    fn on_reclamation(&self, _event: &Reclamation) {}
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
    fn on_epoch_started(&self, event: &EpochStarted<'_>) {
        self.0.on_epoch_started(event);
        self.1.on_epoch_started(event);
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
    fn on_reclamation(&self, event: &Reclamation) {
        self.0.on_reclamation(event);
        self.1.on_reclamation(event);
    }
}

impl<O: OutboxObserver + ?Sized> OutboxObserver for Arc<O> {
    fn on_published(&self, event: &Published<'_>) {
        (**self).on_published(event);
    }
    fn on_fetched(&self, event: &Fetched<'_>) {
        (**self).on_fetched(event);
    }
    fn on_epoch_started(&self, event: &EpochStarted<'_>) {
        (**self).on_epoch_started(event);
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
    fn on_reclamation(&self, event: &Reclamation) {
        (**self).on_reclamation(event);
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
    fn on_epoch_started(&self, event: &EpochStarted<'_>) {
        self.iter().for_each(|o| o.on_epoch_started(event));
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
    fn on_reclamation(&self, event: &Reclamation) {
        self.iter().for_each(|o| o.on_reclamation(event));
    }
}
