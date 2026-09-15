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
                       received_position, processed_position, attempts, last_error, waiting_for";
/// How many columns `COLUMNS` names.
const WIDTH: usize = 12;

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
    /// The head index, `received_position` over the queue — the rows neither
    /// processed, parked nor waiting — is the walk's order: the oldest such
    /// row is its first entry whatever the backlog. The waiting rows have an
    /// index by the dependency they wait for, which the mark uses to wake
    /// them, and one by when they started waiting, which expiry uses. The
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
                waiting_for jsonb NULL,
                waiting_since timestamptz NULL,
                CONSTRAINT {table}_pk PRIMARY KEY (tenant_id, stream_type, stream_id, stream_position)
            );
            ALTER TABLE {table}
                ADD COLUMN IF NOT EXISTS attempts integer NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS last_error text NULL,
                ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz NULL,
                ADD COLUMN IF NOT EXISTS parked_at timestamptz NULL,
                ADD COLUMN IF NOT EXISTS waiting_for jsonb NULL,
                ADD COLUMN IF NOT EXISTS waiting_since timestamptz NULL;
            DROP INDEX IF EXISTS {table}__received_position_idx;
            DROP INDEX IF EXISTS {table}__processed_position_idx;
            DROP INDEX IF EXISTS {table}__queue_idx;
            CREATE INDEX IF NOT EXISTS {table}__head_idx
                ON {table} (received_position)
                WHERE processed_position IS NULL AND parked_at IS NULL AND waiting_for IS NULL;
            CREATE INDEX IF NOT EXISTS {table}__waiting_idx
                ON {table} (waiting_for) WHERE waiting_for IS NOT NULL;
            CREATE INDEX IF NOT EXISTS {table}__waiting_since_idx
                ON {table} (waiting_since) WHERE waiting_for IS NOT NULL;
            CREATE UNIQUE INDEX IF NOT EXISTS {table}__message_id_uniq
                ON {table} (((metadata->>'message_id')::uuid));
            "#
        );
        session.connection().batch_execute(&ddl).await?;
        Ok(())
    }

    /// The head of the worker's queue: the oldest row neither processed,
    /// parked nor waiting, locked for the transaction and stepped over by
    /// every other: `FOR UPDATE SKIP LOCKED`. With it, whether the row still
    /// waits for its backoff, and the statement's snapshot. The outer join
    /// makes the statement return a row even when there is none, so that the
    /// snapshot is always known.
    pub(super) async fn head(&self, session: &P::Session, worker: Worker) -> Result<Step, Error> {
        let sql = format!(
            r#"
            WITH r AS (
                SELECT {COLUMNS},
                       next_attempt_at IS NOT NULL AND next_attempt_at > CURRENT_TIMESTAMP AS deferred
                FROM {table}
                WHERE processed_position IS NULL AND parked_at IS NULL AND waiting_for IS NULL
                  AND ($1 <= 1 OR (hashtext({key}) & 2147483647) % $1 = $2)
                ORDER BY received_position ASC
                LIMIT 1
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
            .query_one(&sql, &[&(worker.of as i32), &(worker.id as i32)])
            .await?;
        let snapshot = row.get::<_, String>(WIDTH + 1).parse().map_err(
            |error: crate::snapshot::MalformedSnapshot| Error::Subscriber(Box::new(error)),
        )?;
        let found = row.get::<_, Option<i64>>(7).is_some();
        Ok(Step {
            row: found.then(|| (message_of(&row, 0), row.get::<_, bool>(WIDTH))),
            snapshot,
        })
    }

    /// The first causal dependency of `message` without a committed mark.
    pub(super) async fn first_unprocessed_dependency(
        &self,
        session: &P::Session,
        message: &InboxMessage,
    ) -> Result<Option<CausalDependency>, Error> {
        for dependency in message.causal_dependencies() {
            if !self.is_processed(session, &dependency).await? {
                return Ok(Some(dependency));
            }
        }
        Ok(None)
    }

    /// Sets the row aside to wait for `dependency`: out of the queue until
    /// the mark of that dependency puts it back.
    pub(super) async fn set_waiting(
        &self,
        session: &P::Session,
        message: &InboxMessage,
        dependency: &CausalDependency,
    ) -> Result<(), Error> {
        let sql = format!(
            "UPDATE {} SET waiting_for = $5::jsonb, waiting_since = CURRENT_TIMESTAMP \
             WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3 AND stream_position = $4",
            self.table
        );
        let [tenant, stream_type, stream_id, position] = identity(message);
        session
            .connection()
            .execute(
                &sql,
                &[
                    tenant,
                    stream_type,
                    stream_id,
                    position,
                    &identity_json(dependency),
                ],
            )
            .await?;
        Ok(())
    }

    /// Parks the rows that have waited longer than `max_wait`, naming the
    /// dependency in their `last_error`, and returns them, oldest first.
    pub(super) async fn expire_waiting(
        &self,
        session: &P::Session,
        max_wait: Duration,
    ) -> Result<Vec<InboxMessage>, Error> {
        let sql = format!(
            "UPDATE {table} SET parked_at = CURRENT_TIMESTAMP, \
                last_error = 'dependency ' || waiting_for::text || ' never arrived', \
                waiting_for = NULL, waiting_since = NULL \
             WHERE ctid IN ( \
                SELECT ctid FROM {table} \
                WHERE waiting_for IS NOT NULL \
                  AND waiting_since < CURRENT_TIMESTAMP - make_interval(secs => $1::double precision) \
                FOR UPDATE SKIP LOCKED) \
             RETURNING {COLUMNS}",
            table = self.table
        );
        let rows = session
            .connection()
            .query(&sql, &[&max_wait.as_secs_f64()])
            .await?;
        let mut expired: Vec<InboxMessage> = rows.iter().map(|row| message_of(row, 0)).collect();
        expired.sort_by_key(|message| message.received_position);
        Ok(expired)
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

    /// Marks `message` processed and, in the same statement, puts back into
    /// the queue every row that waited for it. Returns the order of
    /// processing and the rows woken, oldest first.
    pub(super) async fn mark_processed(
        &self,
        session: &P::Session,
        message: &InboxMessage,
    ) -> Result<(i64, Vec<InboxMessage>), Error> {
        let sql = format!(
            "WITH marked AS ( \
                UPDATE {table} SET processed_position = nextval('{sequence}') \
                WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3 AND stream_position = $4 \
                RETURNING processed_position \
             ), woken AS ( \
                UPDATE {table} SET waiting_for = NULL, waiting_since = NULL \
                WHERE waiting_for = $5::jsonb \
                RETURNING {COLUMNS} \
             ) \
             SELECT marked.processed_position, woken.* FROM marked LEFT JOIN woken ON true",
            table = self.table,
            sequence = self.sequence
        );
        let [tenant, stream_type, stream_id, position] = identity(message);
        let rows = session
            .connection()
            .query(
                &sql,
                &[
                    tenant,
                    stream_type,
                    stream_id,
                    position,
                    &identity_json(&CausalDependency::on(message)),
                ],
            )
            .await?;
        let Some(first) = rows.first() else {
            return Err(Error::Subscriber("the row to mark is gone".into()));
        };
        Ok((first.get(0), woken_of(&rows, 1)))
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
        Ok(rows.iter().map(|row| message_of(row, 0)).collect())
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

    /// Marks a parked row processed by hand and wakes what waited for it, in
    /// one statement; `None` when the row was not parked.
    pub(super) async fn resolve_row(
        &self,
        session: &P::Session,
        message: &InboxMessage,
    ) -> Result<Option<(i64, Vec<InboxMessage>)>, Error> {
        let sql = format!(
            "WITH resolved AS ( \
                UPDATE {table} SET parked_at = NULL, processed_position = nextval('{sequence}') \
                WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3 AND stream_position = $4 \
                  AND parked_at IS NOT NULL \
                RETURNING processed_position \
             ), woken AS ( \
                UPDATE {table} SET waiting_for = NULL, waiting_since = NULL \
                WHERE waiting_for = $5::jsonb AND EXISTS (SELECT 1 FROM resolved) \
                RETURNING {COLUMNS} \
             ) \
             SELECT resolved.processed_position, woken.* FROM resolved LEFT JOIN woken ON true",
            table = self.table,
            sequence = self.sequence
        );
        let [tenant, stream_type, stream_id, position] = identity(message);
        let rows = session
            .connection()
            .query(
                &sql,
                &[
                    tenant,
                    stream_type,
                    stream_id,
                    position,
                    &identity_json(&CausalDependency::on(message)),
                ],
            )
            .await?;
        Ok(rows.first().map(|first| (first.get(0), woken_of(&rows, 1))))
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

/// A row's identity as the `jsonb` a waiting row names it by.
fn identity_json(dependency: &CausalDependency) -> serde_json::Value {
    serde_json::to_value(dependency).unwrap_or(serde_json::Value::Null)
}

/// The message in `COLUMNS`, read from column `at` on.
fn message_of(row: &Row, at: usize) -> InboxMessage {
    InboxMessage {
        tenant_id: row.get(at),
        stream_type: row.get(at + 1),
        stream_id: row.get(at + 2),
        stream_position: row.get(at + 3),
        uri: row.get(at + 4),
        payload: row.get(at + 5),
        metadata: row.get(at + 6),
        received_position: row.get(at + 7),
        processed_position: row.get(at + 8),
        attempts: row.get::<_, i32>(at + 9) as u32,
        last_error: row.get(at + 10),
        waiting_for: row
            .get::<_, Option<serde_json::Value>>(at + 11)
            .and_then(|value| serde_json::from_value(value).ok()),
    }
}

/// The woken rows of a mark or a resolve: the rows of the statement whose
/// row columns, from `at` on, are not the outer join's nulls; oldest first.
fn woken_of(rows: &[Row], at: usize) -> Vec<InboxMessage> {
    let mut woken: Vec<InboxMessage> = rows
        .iter()
        .filter(|row| row.get::<_, Option<i64>>(at + 7).is_some())
        .map(|row| message_of(row, at))
        .collect();
    woken.sort_by_key(|message| message.received_position);
    woken
}
