//! The PostgreSQL inbox.
//!
//! # Idempotency
//!
//! A message is a row whose primary key is its identity,
//! `(tenant_id, stream_type, stream_id, stream_position)`. Receiving the
//! same message again is `INSERT … ON CONFLICT DO NOTHING`. A `message_id` in
//! the metadata is unique in the table too.
//!
//! # Processing
//!
//! A dispatcher takes the oldest unprocessed message — by arrival — whose
//! causal dependencies have all been processed, locks it with
//! `FOR UPDATE SKIP LOCKED` so that other dispatchers pass it by, runs the
//! subscriber with the same transaction, and marks the message processed in
//! it. The subscriber's own writes, through the session it is given, commit
//! with the mark or not at all: a message is processed exactly once as far
//! as this database can see. A message whose dependencies are not yet
//! processed is skipped for now and looked at again next time.
//!
//! # Slots
//!
//! A row carries its slot, `hashtext(<partition key>) % slots` with the sign
//! bit cleared so that no key is left to nobody, computed once at insert and
//! stored; the number of slots and the key are fixed for the life of the
//! table (ADR-0008). A dispatcher has no identity: a call takes whichever
//! slot has a due head and is held by nobody, least recently served first,
//! locks that slot's row for the length of its transaction, and so one
//! dispatcher at a time works a slot — the order of arrival within a slot
//! is a guarantee, not a condition. A dispatcher that dies releases its
//! slot with its transaction. With causal dependencies, cut by stream, so
//! that a message and what it depends on land in one slot.
//!
//! # Dependencies
//!
//! The walk looks at the head of the queue only. A head whose causal
//! dependencies are not all processed is set aside to wait for the first
//! missing one, `waiting_for`, out of the queue; the transaction that marks
//! that dependency processed puts every row waiting for it back, in the
//! same statement. Nothing polls for a dependency. With
//! [`PgInbox::with_max_wait`] a row waiting longer is parked with the
//! dependency named; by default it waits for ever (ADR-0006).
//!
//! # Failures
//!
//! The subscriber runs in a savepoint. When it fails, its writes roll back to
//! the savepoint and the transaction goes on to record the attempt:
//! `attempts`, `last_error`, `next_attempt_at`. Until that time the row is
//! not taken, and it holds its slot, so that the order of arrival survives
//! the failure; a row whose dependencies are not processed is set aside
//! instead. After [`Retries::max_attempts`] failures
//! the row is parked, `parked_at`: out of the queue, in the table, so that a
//! later arrival of the same message is still a duplicate. An operator lists
//! the parked rows and either [`PgInbox::unpark`]s one or
//! [`PgInbox::resolve`]s it, marking it processed without the subscriber's
//! effects. Off by default: unlimited attempts, no backoff (ADR-0005).
//!
//! # Observing
//!
//! [`PgInbox::observed_by`] attaches an [`InboxObserver`]: it is told of
//! every message received and of every step a dispatcher takes, in the
//! vocabulary of the protocol model. The SQL lives in `store`, the loop in
//! `dispatch`.

mod dispatch;
mod store;

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::observer::InboxObserver;
use crate::partition::{ByUri, PartitionKey};

/// How [`PgInbox::run`] runs: `concurrency` loops in this process, each
/// taking whatever slot has work. Processes need no identity and no count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Loops {
    /// How many loops this process runs.
    pub concurrency: u32,
    /// How long a loop waits when there was nothing to process.
    pub poll_interval: Duration,
}

impl Default for Loops {
    fn default() -> Self {
        Loops {
            concurrency: 1,
            poll_interval: Duration::from_secs(1),
        }
    }
}

/// What one `dispatch` call did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Nothing was taken: no slot had a due head, or the slot taken had its
    /// head waiting for its backoff once what was ahead of it was set aside.
    Nothing,
    /// A message was processed and marked.
    Processed,
    /// The subscriber failed; its writes were rolled back and the attempt
    /// recorded.
    Failed {
        /// Failed attempts so far, this one included.
        attempts: u32,
        /// Whether the message was parked by this attempt.
        parked: bool,
    },
}

/// What happens to a message whose subscriber fails (ADR-0005).
#[derive(Clone)]
pub struct Retries {
    /// Failed attempts after which the message is parked; `0`: never, it is
    /// retried for ever.
    pub max_attempts: u32,
    /// How long the message waits after its `n`th failed attempt, `n` from 1.
    /// While it waits it holds its slot.
    pub backoff: Arc<dyn Fn(u32) -> Duration + Send + Sync>,
}

impl Retries {
    /// Unlimited attempts, no backoff: the message is taken again at the
    /// next call. The default.
    pub fn unlimited() -> Self {
        Retries {
            max_attempts: 0,
            backoff: Arc::new(|_| Duration::ZERO),
        }
    }

    /// Parked after `max_attempts` failures, no backoff.
    pub fn up_to(max_attempts: u32) -> Self {
        Retries {
            max_attempts,
            ..Retries::unlimited()
        }
    }

    /// The same, waiting `backoff(n)` after the `n`th failure.
    pub fn with_backoff(self, backoff: impl Fn(u32) -> Duration + Send + Sync + 'static) -> Self {
        Retries {
            backoff: Arc::new(backoff),
            ..self
        }
    }

    /// `base`, doubled with every failure, at most `cap`.
    pub fn exponential(base: Duration, cap: Duration) -> impl Fn(u32) -> Duration {
        move |attempt| {
            base.checked_mul(1u32 << attempt.saturating_sub(1).min(31))
                .map_or(cap, |wait| wait.min(cap))
        }
    }
}

impl Default for Retries {
    fn default() -> Self {
        Retries::unlimited()
    }
}

impl fmt::Debug for Retries {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Retries")
            .field("max_attempts", &self.max_attempts)
            .finish_non_exhaustive()
    }
}

/// The inbox over a PostgreSQL session pool, watched by `O`.
pub struct PgInbox<P, O = ()> {
    pool: P,
    observer: O,
    table: String,
    sequence: String,
    partition: Box<dyn PartitionKey>,
    poll_interval: Duration,
    retries: Retries,
    max_wait: Option<Duration>,
    slots: u32,
}

impl<P> PgInbox<P> {
    /// An inbox in table `inbox`, one slot partitioned by URI, observed by
    /// nobody, retrying a failed message for ever and letting a message wait
    /// for its dependencies for ever.
    pub fn new(pool: P) -> Self {
        PgInbox {
            pool,
            observer: (),
            table: "inbox".to_owned(),
            sequence: "inbox_received_position_seq".to_owned(),
            partition: Box::new(ByUri),
            poll_interval: Duration::from_secs(1),
            retries: Retries::default(),
            max_wait: None,
            slots: 1,
        }
    }
}

impl<P, O> PgInbox<P, O> {
    /// The same inbox, watched by `observer`; see [`crate::observer`].
    pub fn observed_by<O2: InboxObserver>(self, observer: O2) -> PgInbox<P, O2> {
        PgInbox {
            pool: self.pool,
            observer,
            table: self.table,
            sequence: self.sequence,
            partition: self.partition,
            poll_interval: self.poll_interval,
            retries: self.retries,
            max_wait: self.max_wait,
            slots: self.slots,
        }
    }

    /// The same inbox, whose channel consumer waits `poll_interval` when
    /// there was nothing to process.
    pub fn with_poll_interval(self, poll_interval: Duration) -> Self {
        PgInbox {
            poll_interval,
            ..self
        }
    }

    pub(crate) fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    /// The same inbox in another table, with its own position sequence.
    pub fn with_table(self, table: impl Into<String>, sequence: impl Into<String>) -> Self {
        PgInbox {
            table: table.into(),
            sequence: sequence.into(),
            ..self
        }
    }

    /// The same inbox, sharing messages between slots by `key`. Read by
    /// [`PgInbox::setup`] when it creates the table, and fixed with it.
    pub fn partitioned_by(self, key: impl PartitionKey + 'static) -> Self {
        PgInbox {
            partition: Box::new(key),
            ..self
        }
    }

    /// The same inbox cutting its table into `slots` slots, `1..=32767`.
    /// Read by [`PgInbox::setup`] when it creates the table; a table already
    /// cut otherwise is refused, because the rows carry their slot for good
    /// (ADR-0008).
    pub fn with_slots(self, slots: u32) -> Self {
        PgInbox {
            slots: slots.clamp(1, i16::MAX as u32),
            ..self
        }
    }

    /// How many slots this inbox cuts its table into.
    pub fn slots(&self) -> u32 {
        self.slots
    }

    /// The same inbox, treating a failed message as `retries` says.
    pub fn with_retries(self, retries: Retries) -> Self {
        PgInbox { retries, ..self }
    }

    /// The same inbox, parking a message that has waited longer than
    /// `max_wait` for a dependency, with the dependency named.
    pub fn with_max_wait(self, max_wait: Duration) -> Self {
        PgInbox {
            max_wait: Some(max_wait),
            ..self
        }
    }
}
