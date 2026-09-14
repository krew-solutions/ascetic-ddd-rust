//! The PostgreSQL outbox.
//!
//! # Ordering and visibility
//!
//! A `BIGSERIAL` position does not order messages across concurrent
//! transactions: a transaction may take position 1, run for a while, and
//! commit after the one that took position 2. A dispatcher that has passed
//! position 2 would never see 1. So every row also records the inserting
//! transaction, `pg_current_xact_id()`, and a dispatcher reads only rows
//! whose transaction is older than every transaction still running —
//! `transaction_id < pg_snapshot_xmin(pg_current_snapshot())` — in
//! `(transaction_id, position)` order.
//!
//! # Consumer groups and workers
//!
//! Each `(consumer_group, uri)` keeps its own position, and the dispatcher
//! locks that row for the length of a batch, so two dispatchers of one group
//! never process a message twice. With several workers the group name is
//! extended with the worker id, `broker:0`, and each worker takes the URIs
//! whose hash falls to it. The hash is `hashtext(uri)` with the sign bit
//! cleared: `hashtext` is signed and `%` keeps the sign of the dividend, so
//! without that a negative hash would match no worker and the message would
//! never be dispatched.
//!
//! # Delivery
//!
//! At least once. The subscriber runs inside the dispatcher's transaction
//! and the position is acknowledged after the batch; a crash in between
//! redelivers the batch. Consumers deduplicate on `metadata.message_id`.
//!
//! # Observing
//!
//! [`PgOutbox::observed_by`] attaches an [`OutboxObserver`]: it is told of
//! every message published and of every step a dispatcher takes, in the
//! vocabulary of the protocol model. The SQL lives in `store`, the loop in
//! `dispatch`.

mod dispatch;
mod store;

use std::time::Duration;

use crate::observer::OutboxObserver;

/// Messages fetched per dispatch by default.
pub const DEFAULT_BATCH_SIZE: usize = 100;

/// What a dispatcher takes: a consumer group, and optionally one URI with
/// everything under it (`kafka://orders` covers `kafka://orders/order-1`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Selection {
    /// The consumer group; each keeps its own position.
    pub consumer_group: String,
    /// The URI prefix; empty means every URI.
    pub uri: String,
}

impl Selection {
    /// Everything, for `consumer_group`.
    pub fn group(consumer_group: impl Into<String>) -> Self {
        Selection {
            consumer_group: consumer_group.into(),
            uri: String::new(),
        }
    }

    /// The same group, narrowed to one URI and everything under it.
    pub fn uri(self, uri: impl Into<String>) -> Self {
        Selection {
            uri: uri.into(),
            ..self
        }
    }
}

/// This worker among `of` sharing one selection: it takes the URIs whose
/// hash falls to `id`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Worker {
    /// This worker, `0..of`.
    pub id: u32,
    /// How many workers share the selection.
    pub of: u32,
}

impl Worker {
    /// The only worker.
    pub const ALONE: Worker = Worker { id: 0, of: 1 };
}

impl Default for Worker {
    fn default() -> Self {
        Worker::ALONE
    }
}

/// How [`PgOutbox::run`] spreads the work: `concurrency` loops in this
/// process, `num_processes` processes in all, so that worker
/// `process_id * concurrency + local` of `num_processes * concurrency`
/// runs each loop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Workers {
    /// This process, `0..num_processes`.
    pub process_id: u32,
    /// How many processes run the same selection.
    pub num_processes: u32,
    /// How many loops this process runs.
    pub concurrency: u32,
    /// How long a loop waits when there was nothing to dispatch.
    pub poll_interval: Duration,
}

impl Default for Workers {
    fn default() -> Self {
        Workers {
            process_id: 0,
            num_processes: 1,
            concurrency: 1,
            poll_interval: Duration::from_secs(1),
        }
    }
}

/// The outbox over a PostgreSQL session pool, watched by `O`.
pub struct PgOutbox<P, O = ()> {
    pool: P,
    observer: O,
    outbox_table: String,
    offsets_table: String,
    batch_size: i64,
    poll_interval: Duration,
}

impl<P> PgOutbox<P> {
    /// An outbox in tables `outbox` and `outbox_offsets`, observed by nobody.
    pub fn new(pool: P) -> Self {
        PgOutbox {
            pool,
            observer: (),
            outbox_table: "outbox".to_owned(),
            offsets_table: "outbox_offsets".to_owned(),
            batch_size: DEFAULT_BATCH_SIZE as i64,
            poll_interval: Duration::from_secs(1),
        }
    }
}

impl<P, O> PgOutbox<P, O> {
    /// The same outbox, watched by `observer`; see [`crate::observer`].
    pub fn observed_by<O2: OutboxObserver>(self, observer: O2) -> PgOutbox<P, O2> {
        PgOutbox {
            pool: self.pool,
            observer,
            outbox_table: self.outbox_table,
            offsets_table: self.offsets_table,
            batch_size: self.batch_size,
            poll_interval: self.poll_interval,
        }
    }

    /// The same outbox whose channel consumer waits `interval` when there is
    /// nothing to dispatch.
    pub fn with_poll_interval(self, interval: Duration) -> Self {
        PgOutbox {
            poll_interval: interval,
            ..self
        }
    }

    /// The same outbox in other tables.
    pub fn with_tables(self, outbox: impl Into<String>, offsets: impl Into<String>) -> Self {
        PgOutbox {
            outbox_table: outbox.into(),
            offsets_table: offsets.into(),
            ..self
        }
    }

    /// The same outbox fetching `size` messages per dispatch.
    pub fn with_batch_size(self, size: usize) -> Self {
        PgOutbox {
            batch_size: size.max(1) as i64,
            ..self
        }
    }

    pub(crate) fn poll_interval(&self) -> Duration {
        self.poll_interval
    }
}
