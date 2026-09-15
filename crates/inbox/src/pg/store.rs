//! The table and the statements: what the inbox does to PostgreSQL. The
//! loop that drives them is in `dispatch`.

use std::time::Duration;

use ascetic_ddd_session::{PgAccess, Session, SessionPool};
use tokio_postgres::Row;

use super::{PgInbox, Worker};
use crate::error::{BoxError, Error};
use crate::message::{CausalDependency, InboxMessage};
use crate::observer::{InboxObserver, Receipt, Received};
use crate::port::Inbox;
use crate::snapshot::Snapshot;

/// What one walk statement returned: the row at the offset, with whether it
/// still waits for its backoff, or no row; and the snapshot the statement
/// ran under, read in the same statement because under `READ COMMITTED`
/// every statement takes a snapshot of its own.
pub(super) struct Step {
    pub(super) row: Option<(InboxMessage, bool)>,
    pub(super) snapshot: Snapshot,
}

/// The columns of a row, in the order `message_of` reads them.
const COLUMNS: &str = "tenant_id, stream_type, stream_id, stream_position, uri, payload, metadata, \
                       received_position, processed_position, attempts, last_error";

impl<P, O> PgInbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: InboxObserver,
{
    /// Creates the sequence, the table and its indexes if they do not exist,
    /// adds the columns of ADR-0005 to a table from before it, and replaces
    /// the indexes of an earlier layout.
    ///
    /// The queue index, `received_position` over the unprocessed and
    /// unparked rows only, is the walk's order: the oldest such row is its
    /// first entry whatever the backlog, and a processed row leaves it. The
    /// `UNIQUE` on `received_position` already indexes the column as a whole;
    /// the dependency check goes by the primary key.
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
                payload bytea NOT NULL,
                metadata jsonb NULL,
                received_position bigint NOT NULL UNIQUE DEFAULT nextval('{sequence}'),
                processed_position bigint NULL,
                attempts integer NOT NULL DEFAULT 0,
                last_error text NULL,
                next_attempt_at timestamptz NULL,
                parked_at timestamptz NULL,
                CONSTRAINT {table}_pk PRIMARY KEY (tenant_id, stream_type, stream_id, stream_position)
            );
            ALTER TABLE {table}
                ADD COLUMN IF NOT EXISTS attempts integer NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS last_error text NULL,
                ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz NULL,
                ADD COLUMN IF NOT EXISTS parked_at timestamptz NULL;
            DROP INDEX IF EXISTS {table}__received_position_idx;
            DROP INDEX IF EXISTS {table}__processed_position_idx;
            CREATE INDEX IF NOT EXISTS {table}__queue_idx
                ON {table} (received_position) WHERE processed_position IS NULL AND parked_at IS NULL;
            CREATE UNIQUE INDEX IF NOT EXISTS {table}__message_id_uniq
                ON {table} (((metadata->>'message_id')::uuid));
            "#
        );
        session.connection().batch_execute(&ddl).await?;
        Ok(())
    }

    /// The unprocessed, unparked row of the worker's share at `skipped` from
    /// the oldest, locked for the transaction and stepped over by every
    /// other: `FOR UPDATE SKIP LOCKED`. With it, whether the row still waits
    /// for its backoff, and the statement's snapshot. The outer join makes
    /// the statement return a row even when there is none, so that the
    /// snapshot is always known.
    pub(super) async fn unprocessed(
        &self,
        session: &P::Session,
        skipped: i64,
        worker: Worker,
    ) -> Result<Step, Error> {
        let sql = format!(
            r#"
            WITH r AS (
                SELECT {COLUMNS},
                       next_attempt_at IS NOT NULL AND next_attempt_at > CURRENT_TIMESTAMP AS deferred
                FROM {table}
                WHERE processed_position IS NULL AND parked_at IS NULL
                  AND ($1 <= 1 OR (hashtext({key}) & 2147483647) % $1 = $2)
                ORDER BY received_position ASC
                LIMIT 1 OFFSET $3
                FOR UPDATE SKIP LOCKED
            )
            SELECT r.*, pg_current_snapshot()::text AS snapshot
            FROM (SELECT 1) AS one
            LEFT JOIN r ON true
            "#,
            table = self.table,
            key = self.partition.sql_expression(),
        );
        let row = session
            .connection()
            .query_one(&sql, &[&(worker.of as i32), &(worker.id as i32), &skipped])
            .await?;
        let snapshot = row.get::<_, String>(12).parse().map_err(
            |error: crate::snapshot::MalformedSnapshot| Error::Subscriber(Box::new(error)),
        )?;
        let found = row.get::<_, Option<i64>>(7).is_some();
        Ok(Step {
            row: found.then(|| (message_of(&row), row.get::<_, bool>(11))),
            snapshot,
        })
    }

    /// Whether every causal dependency of `message` has a committed mark.
    pub(super) async fn dependencies_processed(
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

    /// Marks `message` processed and returns its order of processing.
    pub(super) async fn mark_processed(
        &self,
        session: &P::Session,
        message: &InboxMessage,
    ) -> Result<i64, Error> {
        let sql = format!(
            "UPDATE {} SET processed_position = nextval('{}') \
             WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3 AND stream_position = $4 \
             RETURNING processed_position",
            self.table, self.sequence
        );
        let row = session
            .connection()
            .query_one(&sql, &identity(message))
            .await?;
        Ok(row.get(0))
    }

    /// Records a failed attempt: one more, the error, when the row is due
    /// again, and whether that was the last attempt. Returns the attempts so
    /// far and whether the row is now parked.
    pub(super) async fn record_failure(
        &self,
        session: &P::Session,
        message: &InboxMessage,
        error: &BoxError,
        retry_after: Duration,
    ) -> Result<(u32, bool), Error> {
        let sql = format!(
            "UPDATE {} SET attempts = attempts + 1, last_error = $5, \
                next_attempt_at = CURRENT_TIMESTAMP + make_interval(secs => $6::double precision), \
                parked_at = CASE WHEN $7::integer > 0 AND attempts + 1 >= $7::integer \
                                 THEN CURRENT_TIMESTAMP END \
             WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3 AND stream_position = $4 \
             RETURNING attempts, parked_at IS NOT NULL",
            self.table
        );
        let [tenant, stream_type, stream_id, position] = identity(message);
        let row = session
            .connection()
            .query_one(
                &sql,
                &[
                    tenant,
                    stream_type,
                    stream_id,
                    position,
                    &error.to_string(),
                    &retry_after.as_secs_f64(),
                    &(self.retries.max_attempts as i32),
                ],
            )
            .await?;
        Ok((row.get::<_, i32>(0) as u32, row.get(1)))
    }

    pub(super) async fn parked_rows(
        &self,
        session: &P::Session,
    ) -> Result<Vec<InboxMessage>, Error> {
        let sql = format!(
            "SELECT {COLUMNS} FROM {} WHERE parked_at IS NOT NULL ORDER BY received_position",
            self.table
        );
        let rows = session.connection().query(&sql, &[]).await?;
        Ok(rows.iter().map(message_of).collect())
    }

    pub(super) async fn unpark_row(
        &self,
        session: &P::Session,
        message: &InboxMessage,
    ) -> Result<bool, Error> {
        let sql = format!(
            "UPDATE {} SET parked_at = NULL, attempts = 0, next_attempt_at = NULL \
             WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3 AND stream_position = $4 \
               AND parked_at IS NOT NULL",
            self.table
        );
        let changed = session
            .connection()
            .execute(&sql, &identity(message))
            .await?;
        Ok(changed == 1)
    }

    pub(super) async fn resolve_row(
        &self,
        session: &P::Session,
        message: &InboxMessage,
    ) -> Result<Option<i64>, Error> {
        let sql = format!(
            "UPDATE {} SET parked_at = NULL, processed_position = nextval('{}') \
             WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3 AND stream_position = $4 \
               AND parked_at IS NOT NULL \
             RETURNING processed_position",
            self.table, self.sequence
        );
        let row = session
            .connection()
            .query_opt(&sql, &identity(message))
            .await?;
        Ok(row.map(|row| row.get(0)))
    }
}

impl<P, O> Inbox for PgInbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: InboxObserver,
{
    /// Stores the message in a transaction of its own and tells the
    /// observer where it landed, or that it was already there.
    async fn publish(&self, message: &InboxMessage) -> Result<(), Error> {
        let sql = format!(
            "INSERT INTO {} (tenant_id, stream_type, stream_id, stream_position, uri, payload, metadata) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (tenant_id, stream_type, stream_id, stream_position) DO NOTHING \
             RETURNING pg_current_xact_id()::text, received_position",
            self.table
        );
        let receipt = self
            .pool
            .session(async |session| {
                session
                    .atomic(async |tx| {
                        let row = tx
                            .connection()
                            .query_opt(
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
                        row.map(|row| {
                            Ok::<_, Error>(Receipt {
                                transaction_id: transaction_id(row.get::<_, String>(0))?,
                                received_position: row.get::<_, i64>(1),
                            })
                        })
                        .transpose()
                    })
                    .await
            })
            .await?;
        self.observer.on_received(&Received { message, receipt });
        Ok(())
    }
}

/// `xid8` has no driver type; it travels as decimal text and is unsigned.
fn transaction_id(text: String) -> Result<u64, Error> {
    text.parse()
        .map_err(|_| Error::Subscriber(format!("not a transaction id: `{text}`").into()))
}

/// The primary key of `message`, as statement parameters `$1`..`$4`.
fn identity(message: &InboxMessage) -> [&(dyn tokio_postgres::types::ToSql + Sync); 4] {
    [
        &message.tenant_id,
        &message.stream_type,
        &message.stream_id,
        &message.stream_position,
    ]
}

fn message_of(row: &Row) -> InboxMessage {
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
        attempts: row.get::<_, i32>(9) as u32,
        last_error: row.get(10),
    }
}
