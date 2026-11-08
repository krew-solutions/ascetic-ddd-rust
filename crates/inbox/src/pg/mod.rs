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
//! missing one, `waiting_for`, out of the queue, and that is the whole
//! transaction, [`Outcome::SetAside`]; the transaction that marks that
//! dependency processed puts every row waiting for it back, in the same
//! statement. Nothing polls for a dependency. The wait and the mark take a
//! transaction-level advisory lock on the dependency's identity, so that a
//! mark cannot slip between the check and the wait (ADR-0009). With
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
//! A subscriber's error is a [`Failure`](crate::Failure): any error is `Transient`, so `?`
//! works, and the message is tried again; `Permanent` is the verdict that no
//! retry will ever succeed, and the message is parked at once. Errors of the
//! database are the loop's business, not the subscriber's: a loop of
//! [`PgInbox::run`] that meets an error of the moment — a lock cycle the
//! server broke, a connection lost — waits and goes on, one that meets a
//! defect stops every loop (ADR-0010).
//!
//! # Forgetting
//!
//! A channel that a host gives a window, [`InboxRetention`], forgets its
//! processed rows once they are older than it, by a sweep beside the loops;
//! every other channel keeps them. Within the window a repeat is a duplicate;
//! after it, it is processed again (ADR-0033 decision 4).
//!
//! # Observing
//!
//! [`PgInbox::observed_by`] attaches an [`InboxObserver`]: it is told of
//! every message received and of every step a dispatcher takes, in the
//! vocabulary of the protocol model. The SQL lives in `store`, the loop in
//! `dispatch`.

mod dispatch;
mod intake;
mod store;
mod surface;

pub use intake::Admission;
pub use surface::{
    Readmission, Readmitted, SetAsideCount, SetAsideCursor, SetAsidePage, SetAsideRow,
};
mod sweep;

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ascetic_ddd_session::pg::Identifier;

use crate::intake::Bounds;
use crate::message::CausalDependency;
use crate::observer::InboxObserver;
use crate::partition::{ByUri, PartitionKey};

/// How [`PgInbox::run`] runs: `concurrency` loops in this process, each
/// taking whatever slot has work. Processes need no identity and no count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Loops {
    /// How many loops this process runs.
    pub concurrency: u32,
    /// How long a loop waits when there was nothing to take.
    pub poll_interval: Duration,
    /// The longest a loop waits after an error of the moment — a lock cycle
    /// the server broke, a connection lost — before it goes on: the wait
    /// starts at `poll_interval` and doubles with each such error in a row.
    pub max_pause: Duration,
}

impl Loops {
    /// The wait after the `n`th error of the moment in a row, `n` from 1.
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

/// What one `dispatch` call did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Nothing was done: no slot had a due head; or the slot taken had none
    /// once its head was read after the lock; or the head was about to wait
    /// for a dependency that was marked meanwhile, and the next call takes
    /// it again.
    Nothing,
    /// The head of the slot taken was set aside to wait for a dependency
    /// (ADR-0006), and that wait is committed (ADR-0009); the next call
    /// takes the slot's next head.
    SetAside,
    /// A message was processed and marked.
    Processed,
    /// The subscriber failed; its writes were rolled back and the attempt
    /// recorded.
    Failed {
        /// Failed attempts so far, this one included.
        attempts: u32,
        /// Whether the message was parked by this attempt: its attempts ran
        /// out, or the subscriber's verdict was permanent.
        parked: bool,
    },
    /// The head could not be read, or its wait, its failure or its mark
    /// written, for its own values, and was parked at once in the
    /// dispatcher's transaction (ADR-0037 decision 6); the next call takes
    /// the slot's next head.
    Parked,
}

/// A parked row as the operator's listing gives it (ADR-0037 decision 6):
/// by its place, and by its identity when that reads — a row stored before
/// the intake's checks may not, and it is listed all the same, so that one
/// row does not close the listing for every Tenant. Never its payload or
/// metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Parked {
    /// Its order of arrival: what unpark and resolve name it by.
    pub received_position: i64,
    /// The slot it is in.
    pub slot: u32,
    /// Its failed attempts.
    pub attempts: u32,
    /// Why it was parked.
    pub last_error: Option<String>,
    /// The channel it arrived on, its key included.
    pub uri: String,
    /// Its identity; `None` when its stream id does not read back in the
    /// bus's domain.
    pub identity: Option<CausalDependency>,
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

/// How long the inbox remembers a processed message, by channel — the
/// destination without its key, `kafka://orders` (ADR-0033 decision 4).
///
/// A channel's window runs from the message's processing. A host sets it at
/// least to the longest a repeat can reach the inbox: the broker's whole
/// retention, tiers included, the time a segment outlives it, and the
/// producer's replay horizon. A channel without a window keeps its rows, as
/// must one whose stream types a message names as causal dependencies, and
/// one whose repeats a host cannot bound. Within its window a repeat is
/// dropped; after it, it is processed again. The rule in force when the
/// sweep runs applies, to rows processed before it changed too.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboxRetention {
    windows: BTreeMap<String, Duration>,
    /// How long a row set aside without a Tenant lives, whatever its
    /// channel: no forgetting of a Tenant reaches it (ADR-0036 R6).
    untenanted: Option<Duration>,
    /// How often the sweep runs.
    pub interval: Duration,
    /// How many rows a statement of the sweep forgets at most.
    pub batch: u32,
    /// Nothing is forgotten for this long after the process starts, nor
    /// after the sweep could not reach the database: intakes and relays
    /// rejoining after an outage redeliver first.
    pub grace: Duration,
    /// How long a statement of the sweep waits for a lock.
    pub lock_timeout: Duration,
}

impl InboxRetention {
    /// Every row kept: the default.
    pub fn keep_everything() -> Self {
        InboxRetention {
            windows: BTreeMap::new(),
            untenanted: None,
            interval: Duration::from_secs(60),
            batch: 1000,
            grace: Duration::from_secs(600),
            lock_timeout: Duration::from_secs(1),
        }
    }

    /// The same, `channel`'s processed rows forgotten `window` after they
    /// were processed.
    pub fn window(mut self, channel: impl Into<String>, window: Duration) -> Self {
        self.windows.insert(channel.into(), window);
        self
    }

    /// The same, a row set aside without a Tenant living `bound` from its
    /// first sight, whatever its channel (ADR-0036 R6).
    pub fn untenanted(mut self, bound: Duration) -> Self {
        self.untenanted = Some(bound);
        self
    }

    /// The same, `channel`'s rows kept.
    pub fn keep(mut self, channel: &str) -> Self {
        self.windows.remove(channel);
        self
    }

    /// The channels with a window, and their windows.
    pub fn windows(&self) -> impl Iterator<Item = (&str, Duration)> {
        self.windows
            .iter()
            .map(|(channel, window)| (channel.as_str(), *window))
    }
}

impl Default for InboxRetention {
    fn default() -> Self {
        InboxRetention::keep_everything()
    }
}

/// The inbox over a PostgreSQL session pool, watched by `O`.
pub struct PgInbox<P, O = ()> {
    pool: P,
    observer: O,
    table: Identifier,
    sequence: Identifier,
    partition: Box<dyn PartitionKey>,
    loops: Loops,
    retries: Retries,
    max_wait: Option<Duration>,
    slots: u32,
    retention: InboxRetention,
    /// Per channel, the latest `processed_at` the sweep forgot: where its
    /// next statement starts, so that dead index entries a held vacuum
    /// horizon keeps are not walked again.
    swept_to: Mutex<HashMap<String, String>>,
    /// Whether a message of a stream type may arrive on a channel (ADR-0036
    /// R5): [`PgInbox::admitting`].
    admission: Admits,
    /// Whether a channel's rows set aside may be readmitted (ADR-0036
    /// decision 4): [`PgInbox::readmitting`].
    readmission: Readmits,
    /// The largest message the intake reads (ADR-0037 decision 3):
    /// [`PgInbox::with_bounds`].
    bounds: Bounds,
}

/// The rule [`PgInbox::admitting`] holds a message to: the channel it
/// arrived on, without its key, and the stream type its headers name.
type Admits = Arc<dyn Fn(&str, &str) -> bool + Send + Sync>;

/// The rule [`PgInbox::readmitting`] holds a channel to.
type Readmits = Arc<dyn Fn(&str) -> bool + Send + Sync>;

impl<P> PgInbox<P> {
    /// An inbox in table `inbox`, one slot partitioned by URI, observed by
    /// nobody, retrying a failed message for ever and letting a message wait
    /// for its dependencies for ever.
    pub fn new(pool: P) -> Self {
        PgInbox {
            pool,
            observer: (),
            table: Identifier::new("inbox").expect("a literal identifier"),
            sequence: Identifier::new("inbox_received_position_seq").expect("a literal identifier"),
            partition: Box::new(ByUri),
            loops: Loops::default(),
            retries: Retries::default(),
            max_wait: None,
            slots: 1,
            retention: InboxRetention::default(),
            swept_to: Mutex::new(HashMap::new()),
            admission: Arc::new(|_, _| true),
            readmission: Arc::new(|_| false),
            bounds: Bounds::default(),
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
            loops: self.loops,
            retries: self.retries,
            max_wait: self.max_wait,
            slots: self.slots,
            retention: self.retention,
            swept_to: self.swept_to,
            admission: self.admission,
            readmission: self.readmission,
            bounds: self.bounds,
        }
    }

    /// The same inbox, admitting a message only when `admits(channel,
    /// stream_type)` holds — the channel it arrived on, without its key, and
    /// the stream type its headers name — and setting it aside as *foreign*
    /// otherwise (ADR-0036 R5). A stream type is its aggregate's, named by
    /// the service that owns it; a host passes its topology's rule, that the
    /// owner is a producer of the channel, so that no producer claims
    /// another's identities. Every message is admitted unless told.
    #[must_use]
    pub fn admitting(
        mut self,
        admits: impl Fn(&str, &str) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.admission = Arc::new(admits);
        self
    }

    /// The same inbox, letting the operator readmit the rows set aside of a
    /// channel only when `readmits(channel)` holds (ADR-0036 decision 4). A
    /// readmit is a disposition the channel declares: one folded by its
    /// producer's positions readmits over a higher position, one that depends
    /// on the order of arrival may refuse it. No channel readmits unless
    /// told.
    #[must_use]
    pub fn readmitting(mut self, readmits: impl Fn(&str) -> bool + Send + Sync + 'static) -> Self {
        self.readmission = Arc::new(readmits);
        self
    }

    /// The same inbox, whose consumer on the bus runs `loops`: how many
    /// loops in this process, how long one waits with nothing to take, how
    /// long after an error of the moment. [`PgInbox::run`] takes its own.
    pub fn with_loops(self, loops: Loops) -> Self {
        PgInbox { loops, ..self }
    }

    pub(crate) fn loops(&self) -> Loops {
        self.loops
    }

    /// The same inbox in another table, with its own position sequence. The
    /// names go into SQL as text, so they are [`Identifier`]s: parsed once,
    /// safe after.
    pub fn with_table(self, table: Identifier, sequence: Identifier) -> Self {
        PgInbox {
            table,
            sequence,
            ..self
        }
    }

    /// The same inbox, sharing messages between slots by `key`. Read by
    /// [`PgInbox::migrate`] when it creates the table, and fixed with it.
    pub fn partitioned_by(self, key: impl PartitionKey + 'static) -> Self {
        PgInbox {
            partition: Box::new(key),
            ..self
        }
    }

    /// The same inbox cutting its table into `slots` slots, `1..=32767`.
    /// Read by [`PgInbox::migrate`] when it creates the table; a table already
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

    /// The same inbox, reading no message past `bounds` (ADR-0037 decision
    /// 3): one past them is set aside as malformed and kept cut. A host
    /// sets them to what its contracts carry, and its broker's fetch to the
    /// same.
    #[must_use]
    pub fn with_bounds(self, bounds: Bounds) -> Self {
        PgInbox { bounds, ..self }
    }

    /// The same inbox, treating a failed message as `retries` says.
    pub fn with_retries(self, retries: Retries) -> Self {
        PgInbox { retries, ..self }
    }

    /// The same inbox, forgetting processed messages as `retention` says.
    pub fn with_retention(self, retention: InboxRetention) -> Self {
        PgInbox { retention, ..self }
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
