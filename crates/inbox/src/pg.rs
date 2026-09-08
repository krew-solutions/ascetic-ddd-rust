//! The PostgreSQL inbox.
//!
//! # Idempotency
//!
//! A message is a row whose primary key is its identity,
//! `(tenant_id, stream_type, stream_id, stream_position)`. Receiving the
//! same message again is `INSERT … ON CONFLICT DO NOTHING`. An `event_id` in
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

use std::time::Duration;

use ascetic_ddd_session::{PgAccess, Session, SessionPool};
use futures::future::join_all;
use tokio::sync::watch;
use tokio_postgres::Row;

use crate::error::{BoxError, Error};
use crate::message::{CausalDependency, InboxMessage};
use crate::partition::{ByUri, PartitionKey};
use crate::port::Inbox;

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

/// The inbox over a PostgreSQL session pool.
pub struct PgInbox<P> {
    pool: P,
    table: String,
    sequence: String,
    partition: Box<dyn PartitionKey>,
}

impl<P> PgInbox<P> {
    /// An inbox in table `inbox`, partitioned by URI.
    pub fn new(pool: P) -> Self {
        PgInbox {
            pool,
            table: "inbox".to_owned(),
            sequence: "inbox_received_position_seq".to_owned(),
            partition: Box::new(ByUri),
        }
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

impl<P> PgInbox<P>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
{
    /// Processes the next eligible message with `subscriber`, as `worker`.
    ///
    /// Returns whether there was one. The subscriber runs inside the
    /// transaction that marks the message processed, and is given that
    /// transaction; if it fails, nothing is marked and the message is
    /// retried.
    pub async fn dispatch<F, Fut>(&self, subscriber: F, worker: Worker) -> Result<bool, Error>
    where
        F: Fn(&P::Session, &InboxMessage) -> Fut + Sync,
        Fut: Future<Output = Result<(), BoxError>>,
    {
        self.pool
            .session(async |session| {
                session
                    .atomic(async |tx| {
                        let Some(message) = self.next_processable(tx, worker).await? else {
                            return Ok(false);
                        };
                        subscriber(tx, &message).await.map_err(Error::Subscriber)?;
                        self.mark_processed(tx, &message).await?;
                        Ok(true)
                    })
                    .await
            })
            .await
    }

    /// Processes messages until `shutdown` completes, with `workers`.
    ///
    /// A loop that finds nothing waits `poll_interval`. Shutdown is
    /// cooperative: a loop finishes its message, commits, and only then
    /// stops. If a loop fails, the others are stopped the same way and its
    /// error is returned.
    pub async fn run<F, Fut>(
        &self,
        subscriber: F,
        workers: Workers,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), Error>
    where
        F: Fn(&P::Session, &InboxMessage) -> Fut + Sync,
        Fut: Future<Output = Result<(), BoxError>>,
    {
        let (stop, _) = watch::channel(false);
        let concurrency = workers.concurrency.max(1);
        let total = workers.num_processes.max(1) * concurrency;
        let loops = (0..concurrency).map(|local| {
            let worker = Worker {
                id: workers.process_id * concurrency + local,
                of: total,
            };
            let mut stopped = stop.subscribe();
            let stop = stop.clone();
            let subscriber = &subscriber;
            async move {
                loop {
                    if *stopped.borrow() {
                        return Ok(());
                    }
                    match self.dispatch(subscriber, worker).await {
                        Ok(true) => {}
                        Ok(false) => {
                            tokio::select! {
                                _ = stopped.changed() => return Ok(()),
                                _ = tokio::time::sleep(workers.poll_interval) => {}
                            }
                        }
                        Err(error) => {
                            let _ = stop.send(true);
                            return Err(error);
                        }
                    }
                }
            }
        });
        let loops = join_all(loops);
        tokio::pin!(loops);
        let outcomes = tokio::select! {
            outcomes = &mut loops => outcomes,
            _ = shutdown => {
                let _ = stop.send(true);
                loops.await
            }
        };
        outcomes.into_iter().collect::<Result<Vec<()>, Error>>()?;
        Ok(())
    }

    /// Creates the sequence, the table and its indexes if they do not exist.
    pub async fn setup(&self, session: &P::Session) -> Result<(), Error> {
        let (table, sequence) = (&self.table, &self.sequence);
        let ddl = format!(
            r#"
            CREATE SEQUENCE IF NOT EXISTS {sequence};
            CREATE TABLE IF NOT EXISTS {table} (
                tenant_id varchar(128) NOT NULL,
                stream_type varchar(128) NOT NULL,
                stream_id jsonb NOT NULL,
                stream_position integer NOT NULL,
                uri varchar(255) NOT NULL,
                payload jsonb NOT NULL,
                metadata jsonb NULL,
                received_position bigint NOT NULL UNIQUE DEFAULT nextval('{sequence}'),
                processed_position bigint NULL,
                CONSTRAINT {table}_pk PRIMARY KEY (tenant_id, stream_type, stream_id, stream_position)
            );
            CREATE INDEX IF NOT EXISTS {table}__received_position_idx ON {table} (received_position);
            CREATE INDEX IF NOT EXISTS {table}__processed_position_idx
                ON {table} (processed_position) WHERE processed_position IS NULL;
            CREATE UNIQUE INDEX IF NOT EXISTS {table}__event_id_uniq
                ON {table} (((metadata->>'event_id')::uuid));
            "#
        );
        session.connection().batch_execute(&ddl).await?;
        Ok(())
    }

    /// The oldest unprocessed message whose dependencies are processed.
    /// Messages that are not yet eligible are stepped over.
    async fn next_processable(
        &self,
        session: &P::Session,
        worker: Worker,
    ) -> Result<Option<InboxMessage>, Error> {
        let mut skipped = 0i64;
        loop {
            let Some(message) = self.unprocessed(session, skipped, worker).await? else {
                return Ok(None);
            };
            if self.dependencies_processed(session, &message).await? {
                return Ok(Some(message));
            }
            skipped += 1;
        }
    }

    async fn unprocessed(
        &self,
        session: &P::Session,
        skipped: i64,
        worker: Worker,
    ) -> Result<Option<InboxMessage>, Error> {
        let sql = format!(
            r#"
            SELECT tenant_id, stream_type, stream_id, stream_position, uri, payload, metadata,
                   received_position, processed_position
            FROM {table}
            WHERE processed_position IS NULL
              AND ($1 <= 1 OR (hashtext({key}) & 2147483647) % $1 = $2)
            ORDER BY received_position ASC
            LIMIT 1 OFFSET $3
            FOR UPDATE SKIP LOCKED
            "#,
            table = self.table,
            key = self.partition.sql_expression(),
        );
        let row = session
            .connection()
            .query_opt(&sql, &[&(worker.of as i32), &(worker.id as i32), &skipped])
            .await?;
        Ok(row.map(message_of))
    }

    async fn dependencies_processed(
        &self,
        session: &P::Session,
        message: &InboxMessage,
    ) -> Result<bool, Error> {
        for dependency in message.causal_dependencies() {
            if !self.is_processed(session, &dependency).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn is_processed(
        &self,
        session: &P::Session,
        dependency: &CausalDependency,
    ) -> Result<bool, Error> {
        let sql = format!(
            "SELECT 1 FROM {} WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3 \
             AND stream_position = $4 AND processed_position IS NOT NULL LIMIT 1",
            self.table
        );
        let row = session
            .connection()
            .query_opt(
                &sql,
                &[
                    &dependency.tenant_id,
                    &dependency.stream_type,
                    &dependency.stream_id,
                    &dependency.stream_position,
                ],
            )
            .await?;
        Ok(row.is_some())
    }

    async fn mark_processed(
        &self,
        session: &P::Session,
        message: &InboxMessage,
    ) -> Result<(), Error> {
        let sql = format!(
            "UPDATE {} SET processed_position = nextval('{}') \
             WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3 AND stream_position = $4",
            self.table, self.sequence
        );
        session
            .connection()
            .execute(
                &sql,
                &[
                    &message.tenant_id,
                    &message.stream_type,
                    &message.stream_id,
                    &message.stream_position,
                ],
            )
            .await?;
        Ok(())
    }
}

impl<P> Inbox for PgInbox<P>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
{
    /// Stores the message in a transaction of its own.
    async fn publish(&self, message: &InboxMessage) -> Result<(), Error> {
        let sql = format!(
            "INSERT INTO {} (tenant_id, stream_type, stream_id, stream_position, uri, payload, metadata) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (tenant_id, stream_type, stream_id, stream_position) DO NOTHING",
            self.table
        );
        self.pool
            .session(async |session| {
                session
                    .atomic(async |tx| {
                        tx.connection()
                            .execute(
                                &sql,
                                &[
                                    &message.tenant_id,
                                    &message.stream_type,
                                    &message.stream_id,
                                    &message.stream_position,
                                    &message.uri,
                                    &message.payload,
                                    &message.metadata,
                                ],
                            )
                            .await?;
                        Ok::<(), Error>(())
                    })
                    .await
            })
            .await
    }
}

fn message_of(row: Row) -> InboxMessage {
    InboxMessage {
        tenant_id: row.get(0),
        stream_type: row.get(1),
        stream_id: row.get(2),
        stream_position: row.get(3),
        uri: row.get(4),
        payload: row.get(5),
        metadata: row.get(6),
        received_position: row.get(7),
        processed_position: row.get(8),
    }
}
