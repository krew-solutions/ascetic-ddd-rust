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

use std::time::Duration;

use ascetic_ddd_session::{PgAccess, Session, SessionPool};
use futures::future::join_all;
use tokio::sync::watch;
use tokio_postgres::Row;

use crate::error::{BoxError, Error};
use crate::message::{OutboxMessage, Position};
use crate::port::Outbox;

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

/// The outbox over a PostgreSQL session pool.
pub struct PgOutbox<P> {
    pool: P,
    outbox_table: String,
    offsets_table: String,
    batch_size: i64,
    poll_interval: Duration,
}

impl<P> PgOutbox<P> {
    /// An outbox in tables `outbox` and `outbox_offsets`.
    pub fn new(pool: P) -> Self {
        PgOutbox {
            pool,
            outbox_table: "outbox".to_owned(),
            offsets_table: "outbox_offsets".to_owned(),
            batch_size: DEFAULT_BATCH_SIZE as i64,
            poll_interval: Duration::from_secs(1),
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

    pub(crate) fn outbox_table(&self) -> &str {
        &self.outbox_table
    }

    pub(crate) fn poll_interval(&self) -> Duration {
        self.poll_interval
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
}

impl<P> PgOutbox<P>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
{
    /// Dispatches the next batch of `selection` to `subscriber`, as `worker`.
    ///
    /// Returns whether there was anything to dispatch. The subscriber runs
    /// inside the dispatcher's transaction, under the lock of the group's
    /// position; if it fails, the batch is rolled back and redelivered.
    pub async fn dispatch<F, Fut>(
        &self,
        subscriber: F,
        selection: &Selection,
        worker: Worker,
    ) -> Result<bool, Error>
    where
        F: Fn(&OutboxMessage) -> Fut + Sync,
        Fut: Future<Output = Result<(), BoxError>>,
    {
        let group = effective_group(&selection.consumer_group, worker);
        self.pool
            .session(async |session| self.ensure_group(session, &group, &selection.uri).await)
            .await?;
        self.pool
            .session(async |session| {
                session
                    .atomic(async |tx| {
                        let messages = self.fetch(tx, &group, &selection.uri, worker).await?;
                        let Some(last) = messages.last() else {
                            return Ok(false);
                        };
                        for message in &messages {
                            subscriber(message).await.map_err(Error::Subscriber)?;
                        }
                        let acked = Position {
                            transaction_id: last.transaction_id.unwrap_or_default(),
                            offset: last.position.unwrap_or_default(),
                        };
                        self.ack(tx, &group, &selection.uri, acked).await?;
                        Ok(true)
                    })
                    .await
            })
            .await
    }

    /// Dispatches `selection` until `shutdown` completes, with `workers`.
    ///
    /// A loop that finds nothing waits `poll_interval`. Shutdown is
    /// cooperative: a loop finishes its batch, commits, and only then stops,
    /// so no transaction is cut in the middle. If a loop fails, the others
    /// are stopped the same way and its error is returned.
    pub async fn run<F, Fut>(
        &self,
        subscriber: F,
        selection: &Selection,
        workers: Workers,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), Error>
    where
        F: Fn(&OutboxMessage) -> Fut + Sync,
        Fut: Future<Output = Result<(), BoxError>>,
    {
        let (stop, _) = watch::channel(false);
        let total = workers.num_processes.max(1) * workers.concurrency.max(1);
        let loops = (0..workers.concurrency.max(1)).map(|local| {
            let worker = Worker {
                id: workers.process_id * workers.concurrency.max(1) + local,
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
                    match self.dispatch(subscriber, selection, worker).await {
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

    /// Where `selection` is: the last acknowledged transaction and offset,
    /// zero before anything was acknowledged.
    pub async fn position(
        &self,
        session: &P::Session,
        selection: &Selection,
    ) -> Result<Position, Error> {
        let sql = format!(
            "SELECT last_processed_transaction_id::text, offset_acked FROM {} \
             WHERE consumer_group = $1 AND uri = $2",
            self.offsets_table
        );
        let row = session
            .connection()
            .query_opt(&sql, &[&selection.consumer_group, &selection.uri])
            .await?;
        row.map(|row| {
            Ok(Position {
                transaction_id: transaction_id(row.get::<_, String>(0))?,
                offset: row.get::<_, i64>(1),
            })
        })
        .unwrap_or(Ok(Position::default()))
    }

    /// Moves `selection` to `position`: back to zero to replay, or ahead
    /// to skip.
    pub async fn set_position(
        &self,
        session: &P::Session,
        selection: &Selection,
        position: Position,
    ) -> Result<(), Error> {
        self.ack(session, &selection.consumer_group, &selection.uri, position)
            .await
    }

    /// Creates the tables and indexes if they do not exist.
    pub async fn setup(&self, session: &P::Session) -> Result<(), Error> {
        let (outbox, offsets) = (&self.outbox_table, &self.offsets_table);
        let ddl = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {outbox} (
                "position" BIGSERIAL,
                "uri" VARCHAR(255) NOT NULL,
                "payload" BYTEA NOT NULL,
                "metadata" JSONB NOT NULL,
                "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                "transaction_id" xid8 NOT NULL,
                PRIMARY KEY ("transaction_id", "position")
            );
            CREATE INDEX IF NOT EXISTS {outbox}_position_idx ON {outbox} ("position");
            CREATE INDEX IF NOT EXISTS {outbox}_uri_idx ON {outbox} ("uri");
            CREATE UNIQUE INDEX IF NOT EXISTS {outbox}_message_id_uniq
                ON {outbox} (((metadata->>'message_id')::uuid));
            CREATE TABLE IF NOT EXISTS {offsets} (
                "consumer_group" VARCHAR(255) NOT NULL,
                "uri" VARCHAR(255) NOT NULL DEFAULT '',
                "offset_acked" BIGINT NOT NULL DEFAULT 0,
                "last_processed_transaction_id" xid8 NOT NULL DEFAULT '0',
                "updated_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY ("consumer_group", "uri")
            );
            "#
        );
        session.connection().batch_execute(&ddl).await?;
        Ok(())
    }

    /// The position row must exist before it can be locked.
    async fn ensure_group(
        &self,
        session: &P::Session,
        group: &str,
        uri: &str,
    ) -> Result<(), Error> {
        let sql = format!(
            "INSERT INTO {} (consumer_group, uri, offset_acked, last_processed_transaction_id) \
             VALUES ($1, $2, 0, '0'::xid8) ON CONFLICT DO NOTHING",
            self.offsets_table
        );
        session.connection().execute(&sql, &[&group, &uri]).await?;
        Ok(())
    }

    async fn fetch(
        &self,
        session: &P::Session,
        group: &str,
        uri: &str,
        worker: Worker,
    ) -> Result<Vec<OutboxMessage>, Error> {
        let sql = format!(
            r#"
            SELECT "position", transaction_id::text, uri, payload, metadata, created_at::text
            FROM (
                WITH last_processed AS (
                    SELECT offset_acked, last_processed_transaction_id
                    FROM {offsets}
                    WHERE consumer_group = $1 AND uri = $2
                    FOR UPDATE
                )
                SELECT "position", transaction_id, uri, payload, metadata, created_at
                FROM {outbox}
                WHERE (
                    (transaction_id = (SELECT last_processed_transaction_id FROM last_processed)
                     AND "position" > (SELECT offset_acked FROM last_processed))
                    OR transaction_id > (SELECT last_processed_transaction_id FROM last_processed)
                )
                AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())
                AND ($3 = '' OR uri = $3 OR uri LIKE $4)
                AND ($5 <= 1 OR (hashtext(uri) & 2147483647) % $5 = $6)
            ) AS messages
            ORDER BY transaction_id, "position"
            LIMIT $7
            "#,
            outbox = self.outbox_table,
            offsets = self.offsets_table,
        );
        let prefix = format!("{uri}/%");
        let rows = session
            .connection()
            .query(
                &sql,
                &[
                    &group,
                    &uri,
                    &uri,
                    &prefix,
                    &(worker.of as i32),
                    &(worker.id as i32),
                    &self.batch_size,
                ],
            )
            .await?;
        rows.into_iter().map(message_of).collect()
    }

    async fn ack(
        &self,
        session: &P::Session,
        group: &str,
        uri: &str,
        position: Position,
    ) -> Result<(), Error> {
        let sql = format!(
            "INSERT INTO {} (consumer_group, uri, offset_acked, last_processed_transaction_id, updated_at) \
             VALUES ($1, $2, $3, $4::text::xid8, CURRENT_TIMESTAMP) \
             ON CONFLICT (consumer_group, uri) DO UPDATE SET \
                offset_acked = EXCLUDED.offset_acked, \
                last_processed_transaction_id = EXCLUDED.last_processed_transaction_id, \
                updated_at = EXCLUDED.updated_at",
            self.offsets_table
        );
        session
            .connection()
            .execute(
                &sql,
                &[
                    &group,
                    &uri,
                    &position.offset,
                    &position.transaction_id.to_string(),
                ],
            )
            .await?;
        Ok(())
    }
}

impl<P> Outbox<P::Session> for PgOutbox<P>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
{
    async fn publish(&self, session: &P::Session, message: &OutboxMessage) -> Result<(), Error> {
        let sql = format!(
            "INSERT INTO {} (uri, payload, metadata, transaction_id) \
             VALUES ($1, $2, $3, pg_current_xact_id())",
            self.outbox_table
        );
        session
            .connection()
            .execute(&sql, &[&message.uri, &message.payload, &message.metadata])
            .await?;
        Ok(())
    }
}

/// Several workers of one group each keep a position of their own.
fn effective_group(group: &str, worker: Worker) -> String {
    if worker.of > 1 {
        format!("{group}:{}", worker.id)
    } else {
        group.to_owned()
    }
}

/// `xid8` has no driver type; it travels as decimal text and is unsigned.
fn transaction_id(text: String) -> Result<u64, Error> {
    text.parse()
        .map_err(|_| Error::Malformed(format!("transaction id `{text}`")))
}

fn message_of(row: Row) -> Result<OutboxMessage, Error> {
    Ok(OutboxMessage {
        position: Some(row.get::<_, i64>(0)),
        transaction_id: Some(transaction_id(row.get::<_, String>(1))?),
        uri: row.get(2),
        payload: row.get(3),
        metadata: row.get(4),
        created_at: Some(row.get::<_, String>(5)),
    })
}
