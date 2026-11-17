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
//! slot has a queue row, is awake and is held by nobody, least recently
//! served first, locks that slot's row for the length of its transaction,
//! and so one dispatcher at a time works a slot. It serves the slot's
//! Tenants in turn, from the cursor its row keeps (ADR-0041 decision 2):
//! the order of arrival is kept per (slot, Tenant), a guarantee, not a
//! condition, and Tenants have no order between them. A dispatcher that
//! dies releases its slot with its transaction. With causal dependencies,
//! cut by stream, so that a message and what it depends on land in one
//! slot.
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
//! mark cannot slip between the check and the wait (ADR-0009). A row that
//! has waited longer than [`PgInbox::with_max_wait`], an hour by default, is
//! parked with the reason named when its dependency never arrived, is
//! parked, or reaches no queued row within 8 hops of waiting rows; one whose
//! dependency is there and on its way waits on. The expiry runs on a timer
//! of [`PgInbox::run`], off the dispatch path, and [`PgInbox::expire`] is
//! its call for a host that drives the dispatch itself (ADR-0042 decision
//! 7).
//!
//! # Failures
//!
//! The subscriber runs in a savepoint. When it fails, its writes roll back to
//! the savepoint and the transaction goes on to record the attempt:
//! `attempts`, `last_error`, `next_attempt_at`. Until that time the row is
//! not taken, and it holds its (slot, Tenant), so that the order of arrival
//! survives the failure, while the slot's other Tenants go on; a row whose
//! dependencies are not processed is set aside instead. After [`Retries::max_attempts`] failures
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
#[cfg(test)]
mod exit_tests;
mod expiry;
mod intake;
#[cfg(test)]
mod measure_tests;
#[cfg(test)]
mod plan_tests;
#[cfg(test)]
mod release_tests;
mod stalls;
mod store;
mod surface;

pub use expiry::Expiry;
pub use intake::Admission;
pub use stalls::Stall;
pub use surface::{
    ParkedFor, ParkedUnder, Readmission, Readmitted, SetAsideCount, SetAsideCursor, SetAsidePage,
    SetAsideRow, UnparkedAll, Unparking,
};
mod sweep;
mod verbs;

pub use verbs::{GivenUp, HeldHead, ParkingHeads, ParkingQueue};

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ascetic_ddd_session::pg::Identifier;

use crate::breaker::{Breaker, Breaking};
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
    pub max_error_wait: Duration,
}

impl Loops {
    /// The wait after the `n`th error of the moment in a row, `n` from 1.
    pub fn error_wait_after(&self, n: u32) -> Duration {
        self.poll_interval
            .checked_mul(1u32 << n.saturating_sub(1).min(31))
            .map_or(self.max_error_wait, |pause| pause.min(self.max_error_wait))
    }
}

impl Default for Loops {
    fn default() -> Self {
        Loops {
            concurrency: 1,
            poll_interval: Duration::from_secs(1),
            max_error_wait: Duration::from_secs(60),
        }
    }
}

/// How a slot's holder serves its Tenants in turn (ADR-0041 decision 2): a
/// setting of the inbox, read by every dispatch, as [`Retries`] is — not of
/// [`Loops`], which only [`PgInbox::run`] reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rotation {
    /// How many of a slot's Tenants one walk examines after its cursor, at
    /// most, before it gives up the dispatch as [`Outcome::Passed`]: with nine
    /// heads in ten waiting for their backoff, a walk of 32 comes back empty
    /// 3 % of the time, one of 8 43 %.
    pub walk: u32,
    /// The longest a slot sleeps once a round of walks found every head of
    /// its Tenants waiting for its backoff. It wakes sooner, at the earliest
    /// of those backoffs or for a row arriving after the round began, or
    /// when an operator's bulk verb moves a row into it and no dispatcher
    /// holds it (ADR-0042 decision 8); this bounds the rest — an insert that
    /// was in flight when the round began, a row a mark woke, a row
    /// unparked by hand, a row a verb moved while a dispatcher held the slot. A second by default: the poll's own
    /// granularity, and well inside the host's tightest hop (ADR-0041 § 5).
    pub max_dormancy: Duration,
}

impl Default for Rotation {
    fn default() -> Self {
        Rotation {
            walk: 32,
            max_dormancy: Duration::from_secs(1),
        }
    }
}

/// What one `dispatch` call did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Nothing was done: no slot had a queue row and was awake; or the slot
    /// taken had no queue row once it was read after the lock; or its walk
    /// ended a round over its Tenants with no due head, and the slot sleeps
    /// until the earliest backoff it saw, a row arriving, or `max_dormancy`
    /// (ADR-0041 decision 2); or the head was about to wait for a dependency
    /// that was marked meanwhile.
    Nothing,
    /// The slot taken had queue rows, and its walk found no due head among
    /// the Tenants it examined: the cursor moved past them, and the next
    /// call goes on from there, without a pause (ADR-0041 decision 2).
    Passed,
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
        /// Whether the subscriber's verdict was `Unavailable`, which holds
        /// the message and spends no attempt (ADR-0042): its Tenant's rows
        /// of its slot wait behind it.
        held: bool,
    },
    /// The head was not asked: its Tenant's `Unavailable` is remembered, or
    /// an ask of it is in flight, or its channel is paused in this process,
    /// and it is held under that cause without its subscriber called
    /// (ADR-0042 decision 2).
    Held,
    /// The slot's walk, a guest's, found no head due for this process: heads
    /// of channels it has paused, or of Tenants its memo keeps from it at the
    /// stage's scope (ADR-0042 decisions 2 and 5): the loop waits its poll,
    /// and the slot's sleep and cursor are left to the processes that ask
    /// them.
    Paused,
    /// The head could not be read, or its wait, its failure or its mark
    /// written, for its own values, and was parked at once in the
    /// dispatcher's transaction (ADR-0037 decision 6); the next call takes
    /// the slot's next head.
    Parked,
}

/// Why a row is parked, read from its columns, never from its last error,
/// which a subscriber writes (ADR-0036 decision 6, ADR-0037 decision 6).
/// Each asks for its own remedy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Ground {
    /// Its subscriber failed: its attempts ran out, or the failure was
    /// permanent. Mended, it is unparked.
    Failure,
    /// Its wait for a causal dependency ran out, and it still names the
    /// dependency: the dependency's mark wakes it, and an unpark only sets it
    /// waiting again.
    Expired,
    /// Its own values could not be read or written, and it was parked at
    /// once: an arrival at its identity supersedes it, and an unpark reads
    /// the same values again.
    AtOnce,
}

impl Ground {
    /// The ground's name: `failure`, `expired` or `at_once`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Ground::Failure => "failure",
            Ground::Expired => "expired",
            Ground::AtOnce => "at_once",
        }
    }

    /// The ground a name names.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        [Ground::Failure, Ground::Expired, Ground::AtOnce]
            .into_iter()
            .find(|ground| ground.as_str() == name)
    }
}

/// The parked rows of one channel, ground and cause (ADR-0036 decision 6,
/// ADR-0042 decision 6): what a host's gauge pages on, read from the table,
/// so that a page holds as long as a row stays parked, whatever the process
/// that parked it did since.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParkedCount {
    /// The channel the rows arrived on, without the key.
    pub channel: String,
    /// Why they are parked.
    pub ground: Ground,
    /// The cause they were parked under: a subscriber's failure's name;
    /// `None` for a ground that is its own reason.
    pub cause: Option<String>,
    /// How many rows are parked.
    pub open: u64,
    /// When the oldest of them was parked, as PostgreSQL prints a
    /// `timestamptz`: the database's clock, never a replica's.
    pub oldest: String,
}

/// What holds a head (ADR-0041 decision 5, ADR-0042 decision 3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Hold {
    /// The head failed on its own and waits for its backoff, holding its
    /// (slot, Tenant): retried, it is processed or parked.
    Backoff,
    /// The head was told `Unavailable`, or held for a Tenant's or a
    /// channel's `Unavailable` without being asked: it spends no attempt and
    /// never parks for it.
    Unavailable,
}

impl Hold {
    /// The kind's name, as the row keeps it: `backoff` or `unavailable`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Hold::Backoff => "backoff",
            Hold::Unavailable => "unavailable",
        }
    }

    /// The kind a name names.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        [Hold::Backoff, Hold::Unavailable]
            .into_iter()
            .find(|hold| hold.as_str() == name)
    }
}

/// The heads of one channel held by one kind and cause (ADR-0041 decision 5,
/// ADR-0042 decision 6): what a host pages on — the oldest `Unavailable`
/// hold over a threshold — and opens a ticket on — the count; a count rising
/// across Tenants is a common cause. Read from the table, as [`ParkedCount`]
/// is. No Tenant is named: only how many there are.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeldCount {
    /// The channel the rows arrived on, without the key.
    pub channel: String,
    /// What kind of hold.
    pub hold: Hold,
    /// The cause the heads are held under.
    pub cause: Option<String>,
    /// How many Tenants' heads are held.
    pub tenants: u64,
    /// How many heads are held.
    pub heads: u64,
    /// When the oldest hold began, as PostgreSQL prints a `timestamptz`.
    pub oldest: String,
}

/// The rows of one channel waiting for a dependency (ADR-0042 decision 6):
/// those the expiry spares — their dependency queued, or waiting on one
/// queued — wait as long as it is on its way, and this is their count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WaitingCount {
    /// The channel the rows arrived on, without the key.
    pub channel: String,
    /// How many rows wait.
    pub waiting: u64,
    /// When the oldest wait began, as PostgreSQL prints a `timestamptz`.
    pub oldest: String,
}

/// A parked row as the operator's listing gives it (ADR-0037 decision 6):
/// by its place, and by its identity when that reads — a row the intake did
/// not write may not, and it is listed all the same, so that one
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
    /// Why it is parked.
    pub ground: Ground,
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
    /// While it waits it holds its Tenant's rows of its slot (ADR-0041
    /// decision 2). An `Unavailable` spends no attempt: its hold is the
    /// breaker's ([`crate::breaker::Breaking`]).
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
    rotation: Rotation,
    retries: Retries,
    max_wait: Option<Duration>,
    slots: u32,
    retention: InboxRetention,
    /// The process's breaker and Tenants' memo (ADR-0042): what this
    /// process does not ask, and when it asks again.
    breaker: Arc<std::sync::Mutex<Breaker>>,
    /// The stalls this process reports (ADR-0042 decision 6).
    stalls: Arc<std::sync::Mutex<stalls::Stalls>>,
    /// Per channel, the latest `processed_at` the sweep forgot: where its
    /// next statement starts, so that dead index entries a held vacuum
    /// horizon keeps are not walked again.
    swept_to: Mutex<HashMap<String, String>>,
    /// Where this process's pass of the expiry stands ([`PgInbox::expire`]).
    expired_to: expiry::Cursor,
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
    /// nobody, retrying a failed message for ever and parking a message that
    /// has waited an hour for its dependencies (ADR-0037 decision 6): a
    /// dependency refused or set aside is then paged, never waited for in
    /// silence.
    pub fn new(pool: P) -> Self {
        PgInbox {
            pool,
            observer: (),
            table: Identifier::new("inbox").expect("a literal identifier"),
            sequence: Identifier::new("inbox_received_position_seq").expect("a literal identifier"),
            partition: Box::new(ByUri),
            loops: Loops::default(),
            rotation: Rotation::default(),
            retries: Retries::default(),
            max_wait: Some(Duration::from_secs(3600)),
            slots: 1,
            retention: InboxRetention::default(),
            breaker: Arc::new(std::sync::Mutex::new(Breaker::new(Breaking::default()))),
            stalls: Arc::new(std::sync::Mutex::new(stalls::Stalls::new())),
            swept_to: Mutex::new(HashMap::new()),
            expired_to: expiry::Cursor::default(),
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
            rotation: self.rotation,
            retries: self.retries,
            max_wait: self.max_wait,
            slots: self.slots,
            retention: self.retention,
            breaker: self.breaker,
            stalls: self.stalls,
            swept_to: self.swept_to,
            expired_to: self.expired_to,
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

    /// The same inbox, its slots serving their Tenants by `rotation`: how
    /// many Tenants a walk examines, how long a slot sleeps at most.
    #[must_use]
    pub fn with_rotation(self, rotation: Rotation) -> Self {
        PgInbox { rotation, ..self }
    }

    pub(crate) fn rotation(&self) -> Rotation {
        self.rotation
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

    /// The same inbox, its breaker and Tenants' memo set by `breaking`
    /// (ADR-0042 decisions 1 and 2): a fresh breaker, every channel closed.
    #[must_use]
    pub fn with_breaking(self, breaking: Breaking) -> Self {
        PgInbox {
            breaker: Arc::new(std::sync::Mutex::new(Breaker::new(breaking))),
            ..self
        }
    }

    /// What this process's memo keeps from its walk at the stage's scope, by
    /// channel and cause: how many Tenants, and since when the oldest's run
    /// of `Unavailable` verdicts began — never which (ADR-0042 decisions 2
    /// and 6). A process cut off from a stage alone writes nothing on the
    /// rows it leaves to the others, and pauses no channel where one Tenant
    /// alone fails: this is where a host sees it, as a gauge of its own.
    #[must_use]
    pub fn excluded(&self) -> Vec<crate::breaker::Excluded> {
        self.breaker().excluded_counts(dispatch::now())
    }

    /// The process's breaker, under its lock; a poisoned lock is taken as it
    /// is: the core's state is whole after every call.
    pub(crate) fn breaker(&self) -> std::sync::MutexGuard<'_, Breaker> {
        self.breaker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The same inbox, forgetting processed messages as `retention` says.
    pub fn with_retention(self, retention: InboxRetention) -> Self {
        PgInbox { retention, ..self }
    }

    /// The same inbox, parking a message that has waited longer than
    /// `max_wait` for a dependency that never arrived, is parked, or reaches
    /// no queued row within 8 hops, with the dependency and the reason named
    /// ([`PgInbox::expire`]).
    pub fn with_max_wait(self, max_wait: Duration) -> Self {
        PgInbox {
            max_wait: Some(max_wait),
            ..self
        }
    }
}
