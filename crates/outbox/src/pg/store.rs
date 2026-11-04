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
use crate::message::{Epoch, Move, OutboxMessage, Position, Snapshot};
use crate::observer::{OutboxObserver, Published, Receipt};
use crate::port::Outbox;

/// The format of the position rows this binary writes and reads: 2, the
/// snapshots of ADR-0035. A table in a later format is refused.
pub(super) const FORMAT: i32 = 2;

/// What a look took: the slot whose position row it locked, or none when no
/// slot of the selection had work; that row as last committed; how many slots
/// the table is cut into; and whether the selection has position rows at
/// all, so that the first contact can create them.
pub(super) struct Taken {
    pub(super) slot: Option<u32>,
    pub(super) position: Position,
    pub(super) slots: u32,
    pub(super) known: bool,
}

/// An epoch's start, or the end of one converted from ADR-0007's positions:
/// its snapshot, and the lowest and highest positions of its rows.
pub(super) struct Start {
    pub(super) epoch: Snapshot,
    pub(super) first: Option<i64>,
    pub(super) last: Option<i64>,
}

/// A batch of an epoch, and the snapshot it was read under.
pub(super) struct Read {
    pub(super) statement: Snapshot,
    pub(super) messages: Vec<OutboxMessage>,
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

/// The position rows become snapshots (ADR-0035 decision 10), in new columns
/// only: an old relay still running on `(last_processed_transaction_id,
/// offset_acked)` reads the pair the new one keeps up, and no column means
/// two things. `(L, off)` becomes `delivered = 'L:L:'` and the epoch
/// `'L+1:L+1:'` at offset `off` — the rest of transaction `L`, exactly — its
/// end left to the next look, so that this step reads no other table
/// (ADR-0029). A position never acknowledged, `L = 0`, keeps the default,
/// `'1:1:'`: nothing delivered.
const POSITIONS: &str = r#"
    ALTER TABLE {offsets}
        ADD COLUMN "delivered" pg_snapshot NOT NULL DEFAULT '1:1:',
        ADD COLUMN "epoch" pg_snapshot,
        ADD COLUMN "epoch_offset" BIGINT NOT NULL DEFAULT 0,
        ADD COLUMN "epoch_end" BIGINT,
        ADD COLUMN "epoch_at" TIMESTAMPTZ,
        ADD COLUMN "delivered_at" TIMESTAMPTZ;
    UPDATE {offsets}
       SET "delivered" = (last_processed_transaction_id::text || ':'
                          || last_processed_transaction_id::text || ':')::pg_snapshot,
           "epoch" = ((last_processed_transaction_id::text::bigint + 1)::text || ':'
                      || (last_processed_transaction_id::text::bigint + 1)::text || ':')::pg_snapshot,
           "epoch_offset" = offset_acked
     WHERE last_processed_transaction_id <> '0';
"#;

/// The format of the position rows, in the meta table, which every binary
/// from this one on reads at its start: so that the next change of their
/// shape can refuse a binary that would misread it.
const FORMAT_COLUMN: &str = r#"
    ALTER TABLE {outbox}_meta ADD COLUMN "format" INTEGER NOT NULL DEFAULT 2;
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
        let positions = POSITIONS.replace("{offsets}", self.offsets_table.as_str());
        let format = FORMAT_COLUMN.replace("{outbox}", self.outbox_table.as_str());
        Component::new(
            format!("outbox:{}", self.outbox_table),
            vec![
                Migration::new("20261027T000000Z_baseline", sql).checked_over(BASELINE),
                Migration::new("20261027T120000Z_one_message_per_identity", identity)
                    .on(self.outbox_table.clone())
                    .checked_over(IDENTITY),
                Migration::new("20261104T000000Z_positions_as_snapshots", positions)
                    .on(self.offsets_table.clone())
                    .checked_over(POSITIONS),
                Migration::new("20261104T000100Z_format", format)
                    .on(self.meta_table())
                    .checked_over(FORMAT_COLUMN),
            ],
        )
        .unwrap_or_else(|_| unreachable!("four migrations with ids, under a name"))
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

    /// The meta table's name.
    fn meta_table(&self) -> ascetic_ddd_session::pg::Identifier {
        ascetic_ddd_session::pg::Identifier::new(format!("{}_meta", self.outbox_table))
            .unwrap_or_else(|_| unreachable!("an identifier with a suffix is one"))
    }

    /// Brings the outbox's tables up to its migrations — a read of the
    /// ledger when they are current, so that a process starts beside live
    /// traffic without locking them — and refuses to go on when the table
    /// was cut into another number of slots, which is a migration, not a
    /// restart; when its positions are in a format this binary does not
    /// know; or when its positions are not drawn one at a time, which it
    /// checks before it applies anything (ADR-0035 decision 6).
    pub async fn migrate(&self) -> Result<Applied, Error> {
        self.pool
            .session(async |session: P::Session| self.check_sequence(&session, true).await)
            .await?;
        let applied = migrate(&self.pool, &self.component(), &MigrationOptions::default())
            .await
            .map_err(Error::Migration)?;
        let (outbox, slots) = (&self.outbox_table, self.slots);
        self.pool
            .session(async |session: P::Session| {
                let row = session
                    .connection()
                    .query_one(&format!("SELECT slots, format FROM {outbox}_meta"), &[])
                    .await?;
                let (pinned, format): (i32, i32) = (row.get(0), row.get(1));
                if pinned != slots as i32 {
                    return Err(Error::Malformed(format!(
                        "table `{outbox}` is cut into {pinned} slots, this outbox asks for {slots}; \
                         the number is fixed for the life of the table"
                    )));
                }
                if format > FORMAT {
                    return Err(Error::Format {
                        found: format,
                        known: FORMAT,
                    });
                }
                Ok(())
            })
            .await?;
        Ok(applied)
    }

    /// Refuses an outbox whose `position` column owns no sequence, or one
    /// that caches more than one value: a cached block is drawn ahead of
    /// another session's later value, and the order of an epoch rests on
    /// positions drawn in real time. Before the table exists, there is
    /// nothing to check when `before_migrating`.
    pub(super) async fn check_sequence(
        &self,
        session: &P::Session,
        before_migrating: bool,
    ) -> Result<(), Error> {
        let outbox = self.outbox_table.as_str();
        let exists: bool = session
            .connection()
            .query_one("SELECT to_regclass($1::text) IS NOT NULL", &[&outbox])
            .await?
            .get(0);
        if !exists && before_migrating {
            return Ok(());
        }
        let row = session
            .connection()
            .query_one(
                "SELECT s.seq, (SELECT seqcache FROM pg_sequence WHERE seqrelid = to_regclass(s.seq)) \
                 FROM (SELECT pg_get_serial_sequence($1::text, 'position') AS seq) AS s",
                &[&outbox],
            )
            .await?;
        let (sequence, cache): (Option<String>, Option<i64>) = (row.get(0), row.get(1));
        match (sequence, cache) {
            (None, _) => Err(Error::SequenceCache(format!(
                "no sequence is owned by `{outbox}`.position"
            ))),
            (Some(_), Some(1)) => Ok(()),
            (Some(sequence), cache) => Err(Error::SequenceCache(format!(
                "`{sequence}` caches {} values",
                cache.map_or_else(|| "an unknown number of".to_owned(), |c| c.to_string())
            ))),
        }
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
            "SELECT delivered::text, epoch::text, epoch_offset, epoch_end FROM {} \
             WHERE consumer_group = $1 AND uri = $2 ORDER BY slot",
            self.offsets_table
        );
        let rows = session.connection().query(&sql, &[&group, &uri]).await?;
        rows.iter().map(|row| position_of(row, 0)).collect()
    }

    /// A look: one statement that takes the slot of the selection least
    /// recently served among those with work, and locks its position row,
    /// `FOR UPDATE SKIP LOCKED`, so that a slot another dispatcher holds is
    /// passed by (ADR-0035 decision 2). A slot has work when an epoch is in
    /// flight, or when a row of any URI is new since its `delivered` —
    /// visible now and not in it — so that an epoch with nothing of this
    /// selection's is delivered at once and not walked again (decision 5).
    /// A look that finds none locks nothing, writes nothing, and takes no
    /// transaction id. The row is read as last committed; what the slot holds
    /// is read by later statements, under snapshots taken after the lock.
    ///
    /// A row is new in two ways, asked apart rather than as one `OR`, which
    /// the planner answers by walking every delivered row: an id from
    /// `delivered`'s `xmax` on, through the slot's index on `(slot,
    /// transaction_id, position)`; or an id of its in-progress list that has
    /// ended since, worked out from the two snapshots before any row is read.
    pub(super) async fn take(
        &self,
        session: &P::Session,
        group: &str,
        uri: &str,
    ) -> Result<Taken, Error> {
        let sql = format!(
            r#"
            WITH taken AS (
                SELECT o.slot, o.delivered, o.epoch, o.epoch_offset, o.epoch_end
                FROM {offsets} o
                WHERE o.consumer_group = $1 AND o.uri = $2
                  AND (o.epoch IS NOT NULL
                       OR EXISTS (SELECT 1 FROM {outbox} m
                                  WHERE m.slot = o.slot
                                    AND m.transaction_id >= pg_snapshot_xmax(o.delivered)
                                    AND pg_visible_in_snapshot(m.transaction_id, pg_current_snapshot()))
                       OR EXISTS (SELECT 1 FROM pg_snapshot_xip(o.delivered) AS x(xid)
                                  WHERE pg_visible_in_snapshot(x.xid, pg_current_snapshot())
                                    AND EXISTS (SELECT 1 FROM {outbox} m
                                                WHERE m.slot = o.slot AND m.transaction_id = x.xid)))
                ORDER BY o.updated_at
                LIMIT 1
                FOR UPDATE OF o SKIP LOCKED
            )
            SELECT t.slot, t.delivered::text, t.epoch::text, t.epoch_offset, t.epoch_end,
                   (SELECT slots FROM {outbox}_meta),
                   EXISTS (SELECT 1 FROM {offsets} WHERE consumer_group = $1 AND uri = $2)
            FROM (SELECT 1) AS one
            LEFT JOIN taken t ON true
            "#,
            outbox = self.outbox_table,
            offsets = self.offsets_table,
        );
        let row = session
            .connection()
            .query_one(&sql, &[&group, &uri])
            .await?;
        let slot = row.get::<_, Option<i16>>(0).map(|slot| slot as u32);
        let position = match slot {
            Some(_) => position_of(&row, 1)?,
            None => Position {
                delivered: Snapshot::nothing(),
                epoch: None,
            },
        };
        Ok(Taken {
            slot,
            position,
            slots: row.get::<_, i32>(5) as u32,
            known: row.get(6),
        })
    }

    /// An epoch's start (ADR-0035 decision 3): `pg_current_snapshot()`,
    /// taken in this statement, after the lock; and the lowest and highest
    /// positions of the slot's rows of the selection new in it — visible in
    /// it and not in `delivered`. Given a stored epoch instead, converted from
    /// ADR-0007's positions with no end yet, the same of its rows. The
    /// snapshots are bound as parameters, `delivered`'s `xmax` and its
    /// in-progress list apart, so that no row's walk unpacks one. The
    /// sequence's cache is read again, so that an `ALTER SEQUENCE` made while
    /// the service runs stops the relay rather than reorders it.
    pub(super) async fn start_epoch(
        &self,
        session: &P::Session,
        slot: u32,
        uri: &str,
        delivered: &Snapshot,
        stored: Option<&Snapshot>,
    ) -> Result<Start, Error> {
        let sql = format!(
            r#"
            WITH e AS MATERIALIZED (
                SELECT COALESCE($2::text::pg_snapshot, pg_current_snapshot()) AS snap
            ),
            members AS (
                SELECT o."position" AS p FROM {outbox} o, e
                WHERE o.slot = $1 AND ($3 = '' OR o.uri = $3 OR o.uri LIKE $4)
                  AND o.transaction_id >= $5::text::xid8
                  AND o.transaction_id < pg_snapshot_xmax(e.snap)
                  AND pg_visible_in_snapshot(o.transaction_id, e.snap)
                UNION ALL
                SELECT o."position" FROM {outbox} o, e
                WHERE o.slot = $1 AND ($3 = '' OR o.uri = $3 OR o.uri LIKE $4)
                  AND o.transaction_id IN (SELECT x::xid8 FROM unnest($6::text[]) AS x
                                           WHERE pg_visible_in_snapshot(x::xid8, e.snap))
            )
            SELECT (SELECT snap::text FROM e),
                   (SELECT min(p) FROM members),
                   (SELECT max(p) FROM members)
            "#,
            outbox = self.outbox_table,
        );
        let prefix = format!("{uri}/%");
        let xip: Vec<String> = delivered.xip.iter().map(u64::to_string).collect();
        let row = session
            .connection()
            .query_one(
                &sql,
                &[
                    &(slot as i16),
                    &stored.map(ToString::to_string),
                    &uri,
                    &prefix,
                    &delivered.xmax.to_string(),
                    &xip,
                ],
            )
            .await?;
        if stored.is_none() {
            self.check_sequence(session, false).await?;
        }
        Ok(Start {
            epoch: snapshot(row.get::<_, String>(0))?,
            first: row.get(1),
            last: row.get(2),
        })
    }

    /// A batch of an epoch (ADR-0035 decision 4): the slot's rows of the
    /// selection past the epoch's offset and up to its end, visible in it and
    /// not in `delivered`, in the order their positions were drawn. The
    /// statement's snapshot, taken after the lock, contains the epoch, so
    /// every row of it can be read here.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn read_batch(
        &self,
        session: &P::Session,
        slot: u32,
        uri: &str,
        offset: i64,
        end: i64,
        epoch: &Snapshot,
        delivered: &Snapshot,
    ) -> Result<Read, Error> {
        let sql = format!(
            r#"
            WITH s AS MATERIALIZED (SELECT pg_current_snapshot() AS snap)
            SELECT (SELECT snap::text FROM s), m."position", m.transaction_id::text, m.uri,
                   m.payload, m.metadata, m.created_at::text
            FROM (SELECT 1) AS one
            LEFT JOIN LATERAL (
                SELECT "position", transaction_id, uri, payload, metadata, created_at
                FROM {outbox}
                WHERE slot = $1 AND ($2 = '' OR uri = $2 OR uri LIKE $3)
                  AND "position" > $4 AND "position" <= $5
                  AND pg_visible_in_snapshot(transaction_id, $6::text::pg_snapshot)
                  AND NOT pg_visible_in_snapshot(transaction_id, $7::text::pg_snapshot)
                ORDER BY "position"
                LIMIT $8
            ) AS m ON true
            ORDER BY m."position"
            "#,
            outbox = self.outbox_table,
        );
        let prefix = format!("{uri}/%");
        let rows = session
            .connection()
            .query(
                &sql,
                &[
                    &(slot as i16),
                    &uri,
                    &prefix,
                    &offset,
                    &end,
                    &epoch.to_string(),
                    &delivered.to_string(),
                    &self.batch_size,
                ],
            )
            .await?;
        let Some(first) = rows.first() else {
            return Err(Error::Malformed(
                "a batch returned no row at all".to_owned(),
            ));
        };
        Ok(Read {
            statement: snapshot(first.get::<_, String>(0))?,
            messages: rows
                .iter()
                .filter(|row| row.get::<_, Option<i64>>(1).is_some())
                .map(message_of)
                .collect::<Result<_, _>>()?,
        })
    }

    /// Moves a slot's position, the row the look locked: the epoch's offset,
    /// or, when the epoch drained, `delivered` to it and no epoch. It keeps
    /// ADR-0007's pair up for a relay of the old rule still running, as
    /// `(xmin(delivered) - 1, i64::MAX)`: every transaction below
    /// `delivered`'s `xmin` is visible in it, so all its rows were handed on
    /// (ADR-0035 decision 10).
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn ack(
        &self,
        session: &P::Session,
        group: &str,
        uri: &str,
        slot: u32,
        drained_to: Option<&Snapshot>,
        epoch: Option<&Epoch>,
        started: bool,
    ) -> Result<(), Error> {
        let sql = format!(
            "UPDATE {} SET \
                delivered = COALESCE($4::text::pg_snapshot, delivered), \
                epoch = $5::text::pg_snapshot, \
                epoch_offset = $6, epoch_end = $7, \
                epoch_at = CASE WHEN $8 THEN CURRENT_TIMESTAMP ELSE epoch_at END, \
                delivered_at = CASE WHEN $4::text IS NULL THEN delivered_at ELSE CURRENT_TIMESTAMP END, \
                last_processed_transaction_id = \
                    (pg_snapshot_xmin(COALESCE($4::text::pg_snapshot, delivered))::text::bigint - 1)::text::xid8, \
                offset_acked = 9223372036854775807, \
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
                    &drained_to.map(ToString::to_string),
                    &epoch.map(|epoch| epoch.snapshot.to_string()),
                    &epoch.map_or(0, |epoch| epoch.offset),
                    &epoch.and_then(|epoch| epoch.end),
                    &started,
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

    /// Moves every slot of a selection (ADR-0035 decision 11), waiting for
    /// each row's lock, and clears the epoch in flight: left, it would drain
    /// and undo the move. A rewind keeps of `delivered` only what lies below
    /// the transaction it goes back to; the old pair follows.
    pub(super) async fn move_all(
        &self,
        session: &P::Session,
        group: &str,
        uri: &str,
        to: Move,
    ) -> Result<(), Error> {
        let delivered = match to {
            Move::Start => "'1:1:'::pg_snapshot".to_owned(),
            Move::Now => "pg_current_snapshot()".to_owned(),
            Move::From(_) => "(least(pg_snapshot_xmin(delivered), $3::text::xid8)::text || ':' \
                 || least(pg_snapshot_xmax(delivered), $3::text::xid8)::text || ':' \
                 || coalesce((SELECT string_agg(x::text, ',' ORDER BY x) \
                              FROM pg_snapshot_xip(delivered) AS x WHERE x < $3::text::xid8), '') \
                 )::pg_snapshot"
                .to_owned(),
        };
        let offsets = &self.offsets_table;
        let sql = format!(
            "UPDATE {offsets} SET delivered = {delivered}, epoch = NULL, epoch_offset = 0, \
                epoch_end = NULL, delivered_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP \
             WHERE consumer_group = $1 AND uri = $2"
        );
        let keep_up = format!(
            "UPDATE {offsets} SET \
                last_processed_transaction_id = (pg_snapshot_xmin(delivered)::text::bigint - 1)::text::xid8, \
                offset_acked = 9223372036854775807 \
             WHERE consumer_group = $1 AND uri = $2"
        );
        match to {
            Move::From(xid) => {
                session
                    .connection()
                    .execute(&sql, &[&group, &uri, &xid.to_string()])
                    .await?
            }
            Move::Start | Move::Now => session.connection().execute(&sql, &[&group, &uri]).await?,
        };
        session
            .connection()
            .execute(&keep_up, &[&group, &uri])
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

/// A snapshot as PostgreSQL prints it.
fn snapshot(text: String) -> Result<Snapshot, Error> {
    text.parse().map_err(Error::Malformed)
}

/// A position from four columns of `row` from `at`: `delivered`, `epoch`,
/// `epoch_offset`, `epoch_end`.
fn position_of(row: &Row, at: usize) -> Result<Position, Error> {
    let epoch = match row.get::<_, Option<String>>(at + 1) {
        Some(text) => Some(Epoch {
            snapshot: snapshot(text)?,
            offset: row.get(at + 2),
            end: row.get(at + 3),
        }),
        None => None,
    };
    Ok(Position {
        delivered: snapshot(row.get::<_, String>(at))?,
        epoch,
    })
}

/// The message of a read row: columns 1 to 6, after the statement's
/// snapshot.
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
