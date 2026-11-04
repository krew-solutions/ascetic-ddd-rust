//! The PostgreSQL outbox.
//!
//! # Ordering and visibility
//!
//! A `BIGSERIAL` position does not order messages across concurrent
//! transactions: a transaction may take position 1, run for a while, and
//! commit after the one that took position 2. And a transaction's id is
//! assigned at its first write, not at its commit, so ordering by it puts
//! a writer that wrote something else first ahead of one that committed
//! before it (ADR-0035). So a row records the inserting transaction,
//! `pg_current_xact_id()`, and a dispatcher hands rows on by epochs.
//!
//! Each position row holds `delivered`, a snapshot in which every visible
//! row was handed on, and the epoch in flight: a later snapshot, the rows
//! visible in it and not in `delivered`, handed on in the order their
//! positions were drawn, `epoch_offset` the last handed on. A dispatcher's
//! transaction locks the row in one statement — finding none with work, it
//! locks nothing and writes nothing — and reads the epoch's start and its
//! batch in later statements, under snapshots taken after the lock, so that
//! a stored epoch is read whole. A batch shorter than its size drains the
//! epoch: `delivered` becomes it. One transaction's rows, and a stream's
//! versions written each after the last was committed, leave in commit
//! order; a transaction left open delays its own rows only.
//!
//! For a relay of ADR-0007 still running in a rolling deploy, the pair it
//! reads, `(last_processed_transaction_id, offset_acked)`, is kept up as
//! `(xmin(delivered) - 1, i64::MAX)`: it hands on again, never loses.
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
//! # The ring
//!
//! The table is a ring of partitions by generation, [`Ring`] (ADR-0033): a
//! transaction's rows land in generation `(id / window) % generations`. A
//! look, an epoch's start and a batch each name the generations they read,
//! as constants worked out from the snapshots, so that the planner reads
//! and locks those alone; a rewind below the ring's `intact_from` is
//! refused, and a new group starts there.
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
mod reclaim;
mod ring;
mod store;

pub use ring::Ring;

use std::time::Duration;

use ascetic_ddd_session::pg::Identifier;

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

/// Where a selection stands in one slot, for an operator: see
/// [`PgOutbox::backlog`].
#[derive(Clone, Debug, PartialEq)]
pub struct Backlog {
    /// The slot.
    pub slot: u32,
    /// How long the epoch in flight has been handed on; `None` when none is.
    pub epoch_age: Option<Duration>,
    /// The last position handed on within it.
    pub epoch_offset: i64,
    /// Its highest position.
    pub epoch_end: Option<i64>,
    /// How long ago `delivered` last moved; `None` before it ever did.
    pub delivered_age: Option<Duration>,
}

/// How the ring is emptied (ADR-0033 decision 3): how often a process's
/// dispatcher tries, between its batches; how far below the next id's
/// window a generation's newest must lie; how long after every group passed
/// it the generation is kept, so that a row relayed a moment ago can still
/// be re-injected; and how long a pass waits for the partition's lock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reclaiming {
    /// How often a process's dispatcher tries.
    pub interval: Duration,
    /// How many windows below the next id's a generation's newest must lie.
    pub margin: u32,
    /// How long after every group passed it a generation is kept.
    pub margin_age: Duration,
    /// How long a pass waits for the partition's lock; a pass that waits
    /// longer gives up until the next.
    pub lock_timeout: Duration,
}

impl Default for Reclaiming {
    fn default() -> Self {
        Reclaiming {
            interval: Duration::from_secs(60),
            margin: 1,
            margin_age: Duration::from_secs(3600),
            lock_timeout: Duration::from_secs(1),
        }
    }
}

/// What [`PgOutbox::rebase`] did after a counter reset.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rebased {
    /// The rows it wrote again, owed to every group.
    pub kept: u64,
    /// The rows it emptied: every group had handed them on, or they repeated
    /// an identity kept.
    pub emptied: u64,
    /// Both bounds of the ring after it.
    pub bound: u64,
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
    outbox_table: Identifier,
    offsets_table: Identifier,
    batch_size: i64,
    loops: Loops,
    slots: u32,
    ring: Ring,
    reclaiming: Reclaiming,
    replay_horizon: Option<Duration>,
}

impl<P> PgOutbox<P> {
    /// An outbox in tables `outbox` and `outbox_offsets`, observed by nobody,
    /// cut into one slot: one position per group.
    pub fn new(pool: P) -> Self {
        PgOutbox {
            pool,
            observer: (),
            outbox_table: Identifier::new("outbox").expect("a literal identifier"),
            offsets_table: Identifier::new("outbox_offsets").expect("a literal identifier"),
            batch_size: DEFAULT_BATCH_SIZE as i64,
            loops: Loops::default(),
            slots: 1,
            ring: Ring::default(),
            reclaiming: Reclaiming::default(),
            replay_horizon: None,
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
            ring: self.ring,
            reclaiming: self.reclaiming,
            replay_horizon: self.replay_horizon,
        }
    }

    /// The same outbox, whose dispatcher on the bus runs `loops`: how many
    /// loops in this process, how long one waits with nothing to dispatch,
    /// how long after a failure. [`PgOutbox::run`] takes its own.
    pub fn with_loops(self, loops: Loops) -> Self {
        PgOutbox { loops, ..self }
    }

    /// The same outbox in other tables. The names go into SQL as text, so
    /// they are [`Identifier`]s: parsed once, safe after.
    pub fn with_tables(self, outbox: Identifier, offsets: Identifier) -> Self {
        PgOutbox {
            outbox_table: outbox,
            offsets_table: offsets,
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
    /// each with a position of its own per group. Read by [`PgOutbox::migrate`]
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

    /// The same outbox with its table as `ring` (ADR-0033). Read by
    /// [`PgOutbox::migrate`] when it makes the ring; a ring already made
    /// otherwise is refused, because the rows carry their generation for
    /// good.
    pub fn with_ring(self, ring: Ring) -> Self {
        PgOutbox { ring, ..self }
    }

    /// The ring this outbox's table is cut into.
    pub fn ring(&self) -> Ring {
        self.ring
    }

    /// The same outbox, declaring how far back a rewind may replay its rows
    /// (ADR-0033 decision 4): [`PgOutbox::set_position`] refuses a rewind to
    /// a transaction whose first row is older. Its consumers' inboxes keep a
    /// processed message for a window that covers it, so that a replay
    /// within it is dropped as a repeat. None, the default, refuses nothing,
    /// and its consumers cannot bound their windows.
    pub fn with_replay_horizon(self, horizon: Duration) -> Self {
        PgOutbox {
            replay_horizon: Some(horizon),
            ..self
        }
    }

    /// The same outbox, its ring emptied as `reclaiming` says.
    pub fn with_reclaiming(self, reclaiming: Reclaiming) -> Self {
        PgOutbox { reclaiming, ..self }
    }

    pub(crate) fn loops(&self) -> Loops {
        self.loops
    }
}
