//! The tables and the statements: what the outbox does to PostgreSQL. The
//! loop that drives them is in `dispatch`.

use ascetic_ddd_bus::{identity, uri};
use ascetic_ddd_migration::{
    Applied, Component, Migration, Options as MigrationOptions, forget, migrate,
};
use ascetic_ddd_session::{PgAccess, SessionPool};
use serde_json::Value;
use tokio_postgres::Row;
use tokio_postgres::error::SqlState;

use super::PgOutbox;
use crate::error::{Error, Identity};
use crate::message::{OutboxMessage, Position};
use crate::observer::{OutboxObserver, Published, Receipt};
use crate::port::Outbox;

/// What one fetch read, all in one statement, because under `READ COMMITTED`
/// every statement takes a snapshot of its own: the slot whose position row
/// the statement locked, or none when no slot of the selection had visible
/// work; the rows of that slot past its position, in `(transaction_id,
/// position)` order; the visibility horizon of the statement,
/// `pg_snapshot_xmin`; how many slots the table is cut into; and whether the
/// selection has position rows at all, so that the first contact can create
/// them.
pub(super) struct Batch {
    pub(super) slot: Option<u32>,
    pub(super) messages: Vec<OutboxMessage>,
    pub(super) horizon: u64,
    pub(super) slots: u32,
    pub(super) known: bool,
}

/// The outbox's tables and indexes as created, `{outbox}`, `{offsets}` and
/// `{slots}` standing for their names and the cut: the baseline of its
/// component, its checksum the template's.
const BASELINE: &str = r#"
    CREATE TABLE IF NOT EXISTS {outbox} (
        "position" BIGSERIAL,
        "uri" VARCHAR(255) NOT NULL,
        "payload" BYTEA NOT NULL,
        "metadata" JSONB NOT NULL,
        "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        "transaction_id" xid8 NOT NULL,
        "slot" SMALLINT GENERATED ALWAYS AS ((hashtext("uri") & 2147483647) % {slots}) STORED,
        PRIMARY KEY ("transaction_id", "position")
    );
    CREATE INDEX IF NOT EXISTS {outbox}_slot_idx ON {outbox} ("slot", "transaction_id", "position");
    CREATE INDEX IF NOT EXISTS {outbox}_uri_position_idx
        ON {outbox} ("uri" varchar_pattern_ops, "transaction_id", "position");
    CREATE UNIQUE INDEX IF NOT EXISTS {outbox}_message_id_uniq
        ON {outbox} (((metadata->>'message_id')::uuid));
    CREATE TABLE IF NOT EXISTS {outbox}_meta ("slots" INTEGER NOT NULL);
    INSERT INTO {outbox}_meta ("slots") SELECT {slots} WHERE NOT EXISTS (SELECT 1 FROM {outbox}_meta);
    CREATE TABLE IF NOT EXISTS {offsets} (
        "consumer_group" VARCHAR(255) NOT NULL,
        "uri" VARCHAR(255) NOT NULL DEFAULT '',
        "slot" SMALLINT NOT NULL,
        "offset_acked" BIGINT NOT NULL DEFAULT 0,
        "last_processed_transaction_id" xid8 NOT NULL DEFAULT '0',
        "updated_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY ("consumer_group", "uri", "slot")
    );
"#;

/// One message per identity and channel in a transaction (ADR-0030): a
/// unique index on the writing transaction and a digest of the channel —
/// the destination without its key — and the four identity fields as one
/// JSON array, so that no two spellings run together; a row missing any of
/// them is exempt (`NULL`). The digest is SHA-256, the one a FIPS-mode
/// server computes, over the text's UTF-8 bytes (`decode(…, 'escape')`
/// with its backslashes doubled, which is IMMUTABLE where `convert_to` is
/// not), its first sixteen bytes kept as a `uuid`. Before it, a check that
/// names every identity a transaction already wrote twice: a unique index
/// would refuse to build over them and name only a digest.
const IDENTITY: &str = r#"
    DO $$
    DECLARE
        taken TEXT;
    BEGIN
        SELECT string_agg(format('%s on %s in transaction %s (messages %s)',
                                 identity, channel, transaction_id, messages), '; ')
        INTO taken
        FROM (
            SELECT transaction_id,
                   split_part(uri, '/', 1) || '//' || split_part(uri, '/', 3) AS channel,
                   ((metadata->'tenant_id') || (metadata->'stream_type')
                     || (metadata->'stream_id') || (metadata->'stream_position'))::text AS identity,
                   string_agg(metadata->>'message_id', ', ') AS messages
            FROM {outbox}
            WHERE metadata ?& ARRAY['tenant_id', 'stream_type', 'stream_id', 'stream_position']
            GROUP BY 1, 2, 3
            HAVING count(*) > 1
            LIMIT 10
        ) AS twice;
        IF taken IS NOT NULL THEN
            RAISE EXCEPTION 'outbox {outbox}: a transaction wrote a second message under an identity on its channel, which the inbox dropped as a repeat (ADR-0030): %', taken;
        END IF;
    END $$;
    CREATE UNIQUE INDEX {outbox}_identity_uniq ON {outbox} (
        transaction_id,
        (encode(substring(sha256(decode(replace(
            split_part(uri, '/', 1) || '//' || split_part(uri, '/', 3) || ' ' ||
            ((metadata->'tenant_id') || (metadata->'stream_type')
              || (metadata->'stream_id') || (metadata->'stream_position'))::text,
            '\', '\\'), 'escape')) for 16), 'hex')::uuid)
    );
"#;

impl<P, O> PgOutbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: OutboxObserver,
{
    /// The outbox's tables as a component of the database's ledger
    /// (ADR-0029), `outbox:<table>`: its baseline is the tables and indexes
    /// below, its checksum over their template, whatever the names and the
    /// cut.
    ///
    /// The primary key `(transaction_id, position)` is the order a fetch
    /// reads in. The index on `(slot, transaction_id, position)` serves a
    /// slot's range from its position on; the one on `(uri, transaction_id,
    /// position)` serves a selection by URI, equal or by prefix —
    /// `varchar_pattern_ops` is what makes `LIKE 'prefix/%'` a range. The
    /// slot is a stored column computed from the URI, so the rows carry
    /// their slot for good and no statement recomputes it; `<outbox>_meta`
    /// holds the number of slots the table was cut into.
    ///
    /// Its second migration holds a transaction to one message per identity
    /// and channel (ADR-0030).
    pub fn component(&self) -> Component {
        let sql = BASELINE
            .replace("{outbox}", self.outbox_table.as_str())
            .replace("{offsets}", self.offsets_table.as_str())
            .replace("{slots}", &self.slots.to_string());
        let identity = IDENTITY.replace("{outbox}", self.outbox_table.as_str());
        Component::new(
            format!("outbox:{}", self.outbox_table),
            vec![
                Migration::new("20261027T000000Z_baseline", sql).checked_over(BASELINE),
                Migration::new("20261027T120000Z_one_message_per_identity", identity)
                    .on(self.outbox_table.clone())
                    .checked_over(IDENTITY),
            ],
        )
        .unwrap_or_else(|_| unreachable!("two migrations with ids, under a name"))
    }

    /// Whether `error` is the refusal of a second message under an identity
    /// the transaction already wrote to the channel.
    fn identity_taken(&self, error: &tokio_postgres::Error) -> bool {
        let index = format!("{}_identity_uniq", self.outbox_table);
        error.code() == Some(&SqlState::UNIQUE_VIOLATION)
            && error
                .as_db_error()
                .and_then(|db| db.constraint())
                .is_some_and(|constraint| constraint == index)
    }

    /// Brings the outbox's tables up to its migrations — a read of the
    /// ledger when they are current, so that a process starts beside live
    /// traffic without locking them — and refuses to go on when the table
    /// was cut into another number of slots: that is a migration, not a
    /// restart.
    pub async fn migrate(&self) -> Result<Applied, Error> {
        let applied = migrate(&self.pool, &self.component(), &MigrationOptions::default())
            .await
            .map_err(Error::Migration)?;
        let (outbox, slots) = (&self.outbox_table, self.slots);
        self.pool
            .session(async |session: P::Session| {
                let pinned: i32 = session
                    .connection()
                    .query_one(&format!("SELECT slots FROM {outbox}_meta"), &[])
                    .await?
                    .get(0);
                if pinned != slots as i32 {
                    return Err(Error::Malformed(format!(
                        "table `{outbox}` is cut into {pinned} slots, this outbox asks for {slots}; \
                         the number is fixed for the life of the table"
                    )));
                }
                Ok(())
            })
            .await?;
        Ok(applied)
    }

    /// Drops the outbox's tables and forgets them in the ledger: for an
    /// outbox that lives as long as a test.
    pub async fn teardown(&self) -> Result<(), Error> {
        let (outbox, offsets) = (&self.outbox_table, &self.offsets_table);
        self.pool
            .session(async |session: P::Session| {
                session
                    .connection()
                    .batch_execute(&format!(
                        "DROP TABLE IF EXISTS {outbox}; DROP TABLE IF EXISTS {outbox}_meta; \
                         DROP TABLE IF EXISTS {offsets};"
                    ))
                    .await?;
                Ok::<_, Error>(())
            })
            .await?;
        forget(&self.pool, &format!("outbox:{outbox}"))
            .await
            .map_err(Error::Migration)
    }

    /// The position rows of a selection, one per slot, must exist before a
    /// fetch can lock one: created at the first contact, at the beginning.
    pub(super) async fn ensure_positions(
        &self,
        session: &P::Session,
        group: &str,
        uri: &str,
    ) -> Result<(), Error> {
        let sql = format!(
            "INSERT INTO {offsets} (consumer_group, uri, slot) \
             SELECT $1, $2, s FROM generate_series(0, (SELECT slots FROM {outbox}_meta) - 1) AS s \
             ON CONFLICT DO NOTHING",
            offsets = self.offsets_table,
            outbox = self.outbox_table
        );
        session.connection().execute(&sql, &[&group, &uri]).await?;
        Ok(())
    }

    /// The positions of a selection, by slot; empty before the first contact.
    pub(super) async fn read_positions(
        &self,
        session: &P::Session,
        group: &str,
        uri: &str,
    ) -> Result<Vec<Position>, Error> {
        let sql = format!(
            "SELECT last_processed_transaction_id::text, offset_acked FROM {} \
             WHERE consumer_group = $1 AND uri = $2 ORDER BY slot",
            self.offsets_table
        );
        let rows = session.connection().query(&sql, &[&group, &uri]).await?;
        rows.iter()
            .map(|row| {
                Ok(Position {
                    transaction_id: transaction_id(row.get::<_, String>(0))?,
                    offset: row.get::<_, i64>(1),
                })
            })
            .collect()
    }

    /// The next batch: one statement that takes the slot of the selection
    /// least recently served among those with visible work — committed rows
    /// past the slot's position, below the snapshot's `xmin`, matching the
    /// selection's URI — locks its position row, `FOR UPDATE SKIP LOCKED`, so
    /// that a slot another dispatcher holds is passed by, and reads that
    /// slot's batch under the same snapshot. The outer join makes the
    /// statement return a row even when no slot had work, so that the horizon
    /// is always known.
    ///
    /// "Past the position" is the row comparison
    /// `(transaction_id, position) > (last, offset)`: the same predicate as
    /// the disjunction "a later transaction, or the same one at a later
    /// position", but one the planner takes as the start of the index range.
    /// Whether a slot has work is asked as "its first row past the position,
    /// or none", ordered and limited to one, rather than as `EXISTS`: with
    /// one slot the planner answered `EXISTS` with a bitmap over the whole
    /// tail, 2.4 ms for a batch of a hundred against 0.06 ms this way.
    pub(super) async fn fetch(
        &self,
        session: &P::Session,
        group: &str,
        uri: &str,
    ) -> Result<Batch, Error> {
        let sql = format!(
            r#"
            WITH taken AS (
                SELECT o.slot, o.offset_acked, o.last_processed_transaction_id
                FROM {offsets} o
                WHERE o.consumer_group = $1 AND o.uri = $2
                  AND (SELECT m."position" FROM {outbox} m
                       WHERE m.slot = o.slot
                         AND (m.transaction_id, m."position") > (o.last_processed_transaction_id, o.offset_acked)
                         AND m.transaction_id < pg_snapshot_xmin(pg_current_snapshot())
                         AND ($2 = '' OR m.uri = $2 OR m.uri LIKE $3)
                       ORDER BY m.transaction_id, m."position"
                       LIMIT 1) IS NOT NULL
                ORDER BY o.updated_at
                LIMIT 1
                FOR UPDATE OF o SKIP LOCKED
            )
            SELECT t.slot, m."position", m.transaction_id::text, m.uri, m.payload, m.metadata,
                   m.created_at::text,
                   pg_snapshot_xmin(pg_current_snapshot())::text,
                   (SELECT slots FROM {outbox}_meta),
                   EXISTS (SELECT 1 FROM {offsets} WHERE consumer_group = $1 AND uri = $2)
            FROM (SELECT 1) AS one
            LEFT JOIN taken t ON true
            LEFT JOIN LATERAL (
                SELECT "position", transaction_id, uri, payload, metadata, created_at
                FROM {outbox}
                WHERE slot = t.slot
                  AND (transaction_id, "position") > (t.last_processed_transaction_id, t.offset_acked)
                  AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())
                  AND ($2 = '' OR uri = $2 OR uri LIKE $3)
                ORDER BY transaction_id, "position"
                LIMIT $4
            ) AS m ON true
            ORDER BY m.transaction_id, m."position"
            "#,
            outbox = self.outbox_table,
            offsets = self.offsets_table,
        );
        let prefix = format!("{uri}/%");
        let rows = session
            .connection()
            .query(&sql, &[&group, &uri, &prefix, &self.batch_size])
            .await?;
        let Some(first) = rows.first() else {
            return Err(Error::Malformed(
                "a fetch returned no row at all".to_owned(),
            ));
        };
        let horizon = transaction_id(first.get::<_, String>(7))?;
        let slots = first.get::<_, i32>(8) as u32;
        let known: bool = first.get(9);
        let slot = first.get::<_, Option<i16>>(0).map(|slot| slot as u32);
        let messages = rows
            .iter()
            .filter(|row| row.get::<_, Option<i64>>(1).is_some())
            .map(message_of)
            .collect::<Result<_, _>>()?;
        Ok(Batch {
            slot,
            messages,
            horizon,
            slots,
            known,
        })
    }

    /// Moves a slot's position: the row is the one the fetch locked.
    pub(super) async fn ack(
        &self,
        session: &P::Session,
        group: &str,
        uri: &str,
        slot: u32,
        position: Position,
    ) -> Result<(), Error> {
        let sql = format!(
            "UPDATE {} SET offset_acked = $4, last_processed_transaction_id = $5::text::xid8, \
                updated_at = CURRENT_TIMESTAMP \
             WHERE consumer_group = $1 AND uri = $2 AND slot = $3",
            self.offsets_table
        );
        let moved = session
            .connection()
            .execute(
                &sql,
                &[
                    &group,
                    &uri,
                    &(slot as i16),
                    &position.offset,
                    &position.transaction_id.to_string(),
                ],
            )
            .await?;
        if moved != 1 {
            return Err(Error::Malformed(format!(
                "the position of slot {slot} of `{group}` is gone"
            )));
        }
        Ok(())
    }

    /// Moves every slot of a selection to `position`.
    pub(super) async fn move_all(
        &self,
        session: &P::Session,
        group: &str,
        uri: &str,
        position: Position,
    ) -> Result<(), Error> {
        let sql = format!(
            "UPDATE {} SET offset_acked = $3, last_processed_transaction_id = $4::text::xid8, \
                updated_at = CURRENT_TIMESTAMP \
             WHERE consumer_group = $1 AND uri = $2",
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
    /// transaction's id, and tells the observer where it landed. A second
    /// message under an identity the transaction already wrote to the
    /// channel is refused, [`Error::IdentityTaken`], and leaves the
    /// transaction aborted (ADR-0030).
    async fn publish(&self, session: &P::Session, message: &OutboxMessage) -> Result<(), Error> {
        let sql = format!(
            "INSERT INTO {} (uri, payload, metadata, transaction_id) \
             VALUES ($1, $2, $3, pg_current_xact_id()) \
             RETURNING transaction_id::text, \"position\"",
            self.outbox_table
        );
        let row = match session
            .connection()
            .query_one(&sql, &[&message.uri, &message.payload, &message.metadata])
            .await
        {
            Ok(row) => row,
            Err(error) if self.identity_taken(&error) => {
                return Err(Error::IdentityTaken(identity_of(message)));
            }
            Err(error) => return Err(error.into()),
        };
        let receipt = Receipt {
            transaction_id: transaction_id(row.get::<_, String>(0))?,
            position: row.get::<_, i64>(1),
        };
        self.observer.on_published(&Published { message, receipt });
        Ok(())
    }
}

/// The identity `message` carries on its channel, as its metadata spells it.
fn identity_of(message: &OutboxMessage) -> Identity {
    let field = |name: &str| match message.metadata.get(name) {
        Some(Value::String(text)) => text.clone(),
        Some(other) => other.to_string(),
        None => String::new(),
    };
    Identity {
        channel: uri::without_key(&message.uri).to_owned(),
        tenant_id: field(identity::TENANT_ID),
        stream_type: field(identity::STREAM_TYPE),
        stream_id: field(identity::STREAM_ID),
        stream_position: field(identity::STREAM_POSITION),
    }
}

/// `xid8` has no driver type; it travels as decimal text and is unsigned.
fn transaction_id(text: String) -> Result<u64, Error> {
    text.parse()
        .map_err(|_| Error::Malformed(format!("transaction id `{text}`")))
}

/// The message of a fetched row: columns 1 to 6, after the slot.
fn message_of(row: &Row) -> Result<OutboxMessage, Error> {
    Ok(OutboxMessage {
        position: Some(row.get::<_, i64>(1)),
        transaction_id: Some(transaction_id(row.get::<_, String>(2))?),
        uri: row.get(3),
        payload: row.get(4),
        metadata: row.get(5),
        created_at: Some(row.get::<_, String>(6)),
    })
}

#[cfg(test)]
mod tests {
    use super::IDENTITY;

    /// The guard's digest is one a FIPS-mode server computes: `md5()`
    /// raises there, and the index is evaluated at every write, so every
    /// message of the deployment would be refused (ADR-0030).
    #[test]
    fn the_guard_s_digest_is_computed_under_fips() {
        assert!(IDENTITY.contains("sha256("));
        assert!(!IDENTITY.to_lowercase().contains("md5("));
    }
}
