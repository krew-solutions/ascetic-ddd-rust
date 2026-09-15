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
//! # Workers
//!
//! Workers share the messages by the hash of a partition key — the URI, or
//! the stream — with the sign bit of `hashtext` cleared, so that no key is
//! left to nobody. With causal dependencies, partition by stream, so that a
//! message and what it depends on land with one worker.
//!
//! # Observing
//!
//! [`PgInbox::observed_by`] attaches an [`InboxObserver`]: it is told of
//! every message received and of every step a dispatcher takes, in the
//! vocabulary of the protocol model. The SQL lives in `store`, the loop in
//! `dispatch`.

mod dispatch;
mod store;

use std::sync::atomic::AtomicU64;
use std::time::Duration;

use crate::observer::InboxObserver;
use crate::partition::{ByUri, PartitionKey};

/// This worker among `of`: it takes the messages whose partition key hashes
/// to `id`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Worker {
    /// This worker, `0..of`.
    pub id: u32,
    /// How many workers share the inbox.
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

/// How [`PgInbox::run`] spreads the work: `concurrency` loops in this
/// process, `num_processes` processes in all, so that worker
/// `process_id * concurrency + local` of `num_processes * concurrency`
/// runs each loop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Workers {
    /// This process, `0..num_processes`.
    pub process_id: u32,
    /// How many processes run the inbox.
    pub num_processes: u32,
    /// How many loops this process runs.
    pub concurrency: u32,
    /// How long a loop waits when there was nothing to process.
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

/// The inbox over a PostgreSQL session pool, watched by `O`.
pub struct PgInbox<P, O = ()> {
    pool: P,
    observer: O,
    /// Numbers the `dispatch` calls, for the observer.
    calls: AtomicU64,
    table: String,
    sequence: String,
    partition: Box<dyn PartitionKey>,
    poll_interval: Duration,
}

impl<P> PgInbox<P> {
    /// An inbox in table `inbox`, partitioned by URI, observed by nobody.
    pub fn new(pool: P) -> Self {
        PgInbox {
            pool,
            observer: (),
            calls: AtomicU64::new(0),
            table: "inbox".to_owned(),
            sequence: "inbox_received_position_seq".to_owned(),
            partition: Box::new(ByUri),
            poll_interval: Duration::from_secs(1),
        }
    }
}

impl<P, O> PgInbox<P, O> {
    /// The same inbox, watched by `observer`; see [`crate::observer`].
    pub fn observed_by<O2: InboxObserver>(self, observer: O2) -> PgInbox<P, O2> {
        PgInbox {
            pool: self.pool,
            observer,
            calls: self.calls,
            table: self.table,
            sequence: self.sequence,
            partition: self.partition,
            poll_interval: self.poll_interval,
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

    /// The same inbox, sharing messages between workers by `key`.
    pub fn partitioned_by(self, key: impl PartitionKey + 'static) -> Self {
        PgInbox {
            partition: Box::new(key),
            ..self
        }
    }
}
