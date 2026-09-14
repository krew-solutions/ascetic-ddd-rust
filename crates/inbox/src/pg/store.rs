//! The table and the statements: what the inbox does to PostgreSQL. The
//! loop that drives them is in `dispatch`.

use ascetic_ddd_session::{PgAccess, Session, SessionPool};
use tokio_postgres::Row;

use super::{PgInbox, Worker};
use crate::error::Error;
use crate::message::{CausalDependency, InboxMessage};
use crate::observer::{InboxObserver, Received};
use crate::port::Inbox;

impl<P, O> PgInbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: InboxObserver,
{
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
                payload bytea NOT NULL,
                metadata jsonb NULL,
                received_position bigint NOT NULL UNIQUE DEFAULT nextval('{sequence}'),
                processed_position bigint NULL,
                CONSTRAINT {table}_pk PRIMARY KEY (tenant_id, stream_type, stream_id, stream_position)
            );
            CREATE INDEX IF NOT EXISTS {table}__received_position_idx ON {table} (received_position);
            CREATE INDEX IF NOT EXISTS {table}__processed_position_idx
                ON {table} (processed_position) WHERE processed_position IS NULL;
            CREATE UNIQUE INDEX IF NOT EXISTS {table}__message_id_uniq
                ON {table} (((metadata->>'message_id')::uuid));
            "#
        );
        session.connection().batch_execute(&ddl).await?;
        Ok(())
    }

    /// The unprocessed row of the worker's share at `skipped` from the
    /// oldest, locked for the transaction and stepped over by every other:
    /// `FOR UPDATE SKIP LOCKED`.
    pub(super) async fn unprocessed(
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
            .query_one(
                &sql,
                &[
                    &message.tenant_id,
                    &message.stream_type,
                    &message.stream_id,
                    &message.stream_position,
                ],
            )
            .await?;
        Ok(row.get(0))
    }
}

impl<P, O> Inbox for PgInbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: InboxObserver,
{
    /// Stores the message in a transaction of its own and tells the
    /// observer its order of arrival, or that it was already there.
    async fn publish(&self, message: &InboxMessage) -> Result<(), Error> {
        let sql = format!(
            "INSERT INTO {} (tenant_id, stream_type, stream_id, stream_position, uri, payload, metadata) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (tenant_id, stream_type, stream_id, stream_position) DO NOTHING \
             RETURNING received_position",
            self.table
        );
        let received_position = self
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
                        Ok::<_, Error>(row.map(|row| row.get::<_, i64>(0)))
                    })
                    .await
            })
            .await?;
        self.observer.on_received(&Received {
            message,
            received_position,
        });
        Ok(())
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
