//! The tables and the statements: what the outbox does to PostgreSQL. The
//! loop that drives them is in `dispatch`.

use ascetic_ddd_session::{PgAccess, SessionPool};
use tokio_postgres::Row;

use super::{PgOutbox, Worker};
use crate::error::Error;
use crate::message::{OutboxMessage, Position};
use crate::observer::{OutboxObserver, Published, Receipt};
use crate::port::Outbox;

/// What one fetch read: the rows, and the visibility horizon of the statement
/// that read them, `pg_snapshot_xmin`, which travels with the rows and is
/// therefore unknown when there are none.
pub(super) struct Batch {
    pub(super) messages: Vec<OutboxMessage>,
    pub(super) horizon: Option<u64>,
}

impl<P, O> PgOutbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: OutboxObserver,
{
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
    pub(super) async fn ensure_group(
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

    pub(super) async fn read_position(
        &self,
        session: &P::Session,
        group: &str,
        uri: &str,
    ) -> Result<Position, Error> {
        let sql = format!(
            "SELECT last_processed_transaction_id::text, offset_acked FROM {} \
             WHERE consumer_group = $1 AND uri = $2",
            self.offsets_table
        );
        let row = session
            .connection()
            .query_opt(&sql, &[&group, &uri])
            .await?;
        row.map(|row| {
            Ok(Position {
                transaction_id: transaction_id(row.get::<_, String>(0))?,
                offset: row.get::<_, i64>(1),
            })
        })
        .unwrap_or(Ok(Position::default()))
    }

    /// The next batch, under the lock of the group's position row: committed
    /// rows past the position, visible below the snapshot's `xmin`, of the
    /// worker's share of the selection, in `(transaction_id, position)` order.
    pub(super) async fn fetch(
        &self,
        session: &P::Session,
        group: &str,
        uri: &str,
        worker: Worker,
    ) -> Result<Batch, Error> {
        let sql = format!(
            r#"
            SELECT "position", transaction_id::text, uri, payload, metadata, created_at::text,
                   pg_snapshot_xmin(pg_current_snapshot())::text
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
        let horizon = match rows.first() {
            Some(row) => Some(transaction_id(row.get::<_, String>(6))?),
            None => None,
        };
        let messages = rows.into_iter().map(message_of).collect::<Result<_, _>>()?;
        Ok(Batch { messages, horizon })
    }

    pub(super) async fn ack(
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

impl<P, O> Outbox<P::Session> for PgOutbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: OutboxObserver,
{
    /// Writes the message inside `session`'s transaction, stamped with that
    /// transaction's id, and tells the observer where it landed.
    async fn publish(&self, session: &P::Session, message: &OutboxMessage) -> Result<(), Error> {
        let sql = format!(
            "INSERT INTO {} (uri, payload, metadata, transaction_id) \
             VALUES ($1, $2, $3, pg_current_xact_id()) \
             RETURNING transaction_id::text, \"position\"",
            self.outbox_table
        );
        let row = session
            .connection()
            .query_one(&sql, &[&message.uri, &message.payload, &message.metadata])
            .await?;
        let receipt = Receipt {
            transaction_id: transaction_id(row.get::<_, String>(0))?,
            position: row.get::<_, i64>(1),
        };
        self.observer.on_published(&Published { message, receipt });
        Ok(())
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
