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
//! # Consumer groups and slots
//!
//! A row carries its slot, `hashtext(uri) % slots` with the sign bit cleared
//! — `hashtext` is signed and `%` keeps the sign of the dividend, so without
//! that a negative hash would match no slot — computed once at insert and
//! stored, and the number of slots is fixed for the life of the table
//! (ADR-0007). Each `(consumer_group, uri, slot)` keeps its own position. A
//! dispatcher has no identity: a fetch takes whichever slot of the selection
//! has visible work and is held by nobody, locks that slot's position row
//! for the length of the batch, `FOR UPDATE SKIP LOCKED`, and so two
//! dispatchers never process a message twice and any number of them share a
//! selection without being told who they are. A dispatcher that dies
//! releases its slot with its transaction. One slot, the default, is one
//! position per group and the order of the whole selection; more slots are
//! parallelism, and order within a URI still, since a URI is in one slot.
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

/// How [`PgOutbox::run`] runs: `concurrency` loops in this process, each
/// taking whatever slot has work. Processes need no identity and no count;
/// start as many as the slots make useful.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Loops {
    /// How many loops this process runs.
    pub concurrency: u32,
    /// How long a loop waits when there was nothing to dispatch.
    pub poll_interval: Duration,
    /// The longest a loop waits after a failure — the subscriber's, or an
    /// error of the moment in the database, a connection lost — before it
    /// tries again: the wait starts at `poll_interval` and doubles with each
    /// failure in a row.
    pub max_pause: Duration,
}

impl Loops {
    /// The wait after the `n`th failure in a row, `n` from 1.
    pub fn pause_after(&self, n: u32) -> Duration {
        self.poll_interval
            .checked_mul(1u32 << n.saturating_sub(1).min(31))
            .map_or(self.max_pause, |pause| pause.min(self.max_pause))
    }
}

impl Default for Loops {
    fn default() -> Self {
        Loops {
            concurrency: 1,
            poll_interval: Duration::from_secs(1),
            max_pause: Duration::from_secs(60),
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
    loops: Loops,
    slots: u32,
}

impl<P> PgOutbox<P> {
    /// An outbox in tables `outbox` and `outbox_offsets`, observed by nobody,
    /// cut into one slot: one position per group.
    pub fn new(pool: P) -> Self {
        PgOutbox {
            pool,
            observer: (),
            outbox_table: "outbox".to_owned(),
            offsets_table: "outbox_offsets".to_owned(),
            batch_size: DEFAULT_BATCH_SIZE as i64,
            loops: Loops::default(),
            slots: 1,
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
            loops: self.loops,
            slots: self.slots,
        }
    }

    /// The same outbox, whose dispatcher on the bus runs `loops`: how many
    /// loops in this process, how long one waits with nothing to dispatch,
    /// how long after a failure. [`PgOutbox::run`] takes its own.
    pub fn with_loops(self, loops: Loops) -> Self {
        PgOutbox { loops, ..self }
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

    /// The same outbox cutting its table into `slots` slots, `1..=32767`,
    /// each with a position of its own per group. Read by [`PgOutbox::setup`]
    /// when it creates the table; a table already cut otherwise is refused,
    /// because the rows carry their slot for good (ADR-0007).
    pub fn with_slots(self, slots: u32) -> Self {
        PgOutbox {
            slots: slots.clamp(1, i16::MAX as u32),
            ..self
        }
    }

    /// How many slots this outbox cuts its table into.
    pub fn slots(&self) -> u32 {
        self.slots
    }

    pub(crate) fn loops(&self) -> Loops {
        self.loops
    }
}
