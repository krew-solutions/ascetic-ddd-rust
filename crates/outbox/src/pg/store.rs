//! The tables and the statements: what the outbox does to PostgreSQL. The
//! loop that drives them is in `dispatch`.

use std::collections::BTreeSet;

use ascetic_ddd_bus::{identity, uri};
use ascetic_ddd_migration::{
    Applied, Component, LEDGER, Migration, Options as MigrationOptions, forget, migrate,
};
use ascetic_ddd_session::{PgAccess, Session, SessionPool};
use serde_json::Value;
use tokio_postgres::Row;
use tokio_postgres::error::SqlState;

use super::PgOutbox;
use crate::error::{Error, Identity};
use crate::message::{Epoch, Generation, Move, OutboxMessage, Position, Snapshot};
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
    /// A counter reset recorded earlier, which sticks.
    pub(super) refused: Option<String>,
    /// A position delivered past the server's ids: a counter reset, now.
    pub(super) reset: bool,
}

/// An epoch's start, or the end of one converted from ADR-0007's positions:
/// its snapshot, the lowest and highest positions of its rows, and the
/// generations of the ring it found them in.
pub(super) struct Start {
    pub(super) epoch: Snapshot,
    pub(super) first: Option<i64>,
    pub(super) last: Option<i64>,
    pub(super) generations: Vec<Generation>,
}

/// The ring's step: the one a transaction fixing the copy's bound runs
/// before (ADR-0033).
const RING_STEP: &str = "20261104T000500Z_ring";

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

/// A counter reset, once found, is recorded in the meta table and sticks:
/// the server's ids pass a restored position's again in time, and a check
/// made afresh would then let every row written meanwhile count as
/// delivered (ADR-0035 decision 12).
const REFUSAL_COLUMN: &str = r#"
    ALTER TABLE {outbox}_meta ADD COLUMN "refused" TEXT;
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

/// The ring's numbers, its two bounds and its generations' times, in the
/// meta table beside the cut (ADR-0033 decision 3): `{generations}` and
/// `{window}` stand for the ring this process asks for, against which a
/// process configured otherwise is refused at its start.
const RING_META: &str = r#"
    ALTER TABLE {outbox}_meta
        ADD COLUMN "generations" INTEGER NOT NULL DEFAULT {generations},
        ADD COLUMN "window_ids" BIGINT NOT NULL DEFAULT {window},
        ADD COLUMN "gone_below" xid8,
        ADD COLUMN "intact_from" xid8,
        ADD COLUMN "passed_at" TIMESTAMPTZ[];
"#;

/// The generations of the ring an epoch found rows in, each with its first
/// and last position, beside the epoch: a batch names those alone.
const EPOCH_GENERATIONS: &str = r#"
    ALTER TABLE {offsets} ADD COLUMN "epoch_generations" JSONB;
"#;

/// The outbox becomes a ring of `{generations}` partitions by `generation`,
/// each taking in turn the rows of `{window}` consecutive transaction ids
/// (ADR-0033 decision 3). The step reads no table but its target: it copies
/// the rows from `{bound}` on — fixed before it by a transaction of its own,
/// rendered here — each with its own transaction id, position and
/// generation; moves the position sequence to the new column before the old
/// table is dropped, which would drop a sequence it still owned; and builds
/// the indexes on the ring, ADR-0035's `(slot, position)` among them, each
/// partition's named after it. Every unique index includes the generation,
/// which is a function of the transaction: ADR-0030's guard is no weaker,
/// `message_id` is unique within a generation. The guard takes ADR-0034's
/// part already, absent as `""`, nested so that a part that is an array is
/// not spliced into the identity.
const RING: &str = r#"
    CREATE TABLE {outbox}_ring (
        LIKE {outbox} INCLUDING DEFAULTS INCLUDING GENERATED,
        "generation" SMALLINT NOT NULL
            DEFAULT ((pg_current_xact_id()::text::bigint / {window}) % {generations}),
        CONSTRAINT {outbox}_generation_check
            CHECK ("generation" = ("transaction_id"::text::bigint / {window}) % {generations})
    ) PARTITION BY LIST ("generation");
    DO $$
    BEGIN
        FOR g IN 0..{generations} - 1 LOOP
            EXECUTE format('CREATE TABLE {outbox}_g%s PARTITION OF {outbox}_ring FOR VALUES IN (%s)', g, g);
        END LOOP;
    END $$;
    INSERT INTO {outbox}_ring ("position", uri, payload, metadata, created_at, transaction_id, "generation")
        SELECT "position", uri, payload, metadata, created_at, transaction_id,
               (transaction_id::text::bigint / {window}) % {generations}
        FROM {outbox}
        WHERE transaction_id >= '{bound}'::xid8;
    DO $$
    BEGIN
        EXECUTE format('ALTER SEQUENCE %s OWNED BY {outbox}_ring."position"',
                       pg_get_serial_sequence('{outbox}', 'position'));
    END $$;
    DROP TABLE {outbox};
    ALTER TABLE {outbox}_ring RENAME TO {outbox};
    ALTER TABLE {outbox} ADD CONSTRAINT {outbox}_pkey
        PRIMARY KEY ("generation", "transaction_id", "position");
    CREATE INDEX {outbox}_slot_idx ON {outbox} ("slot", "transaction_id", "position");
    CREATE INDEX {outbox}_slot_position_idx ON {outbox} ("slot", "position") INCLUDE ("transaction_id");
    CREATE INDEX {outbox}_uri_position_idx
        ON {outbox} ("uri" varchar_pattern_ops, "transaction_id", "position");
    CREATE UNIQUE INDEX {outbox}_message_id_uniq
        ON {outbox} ("generation", ((metadata->>'message_id')::uuid));
    CREATE UNIQUE INDEX {outbox}_identity_uniq ON {outbox} (
        "generation",
        transaction_id,
        (encode(substring(sha256(decode(replace(
            split_part(uri, '/', 1) || '//' || split_part(uri, '/', 3) || ' ' ||
            ((metadata->'tenant_id') || (metadata->'stream_type')
              || (metadata->'stream_id') || (metadata->'stream_position')
              || jsonb_set('[null]', '{0}', coalesce(metadata->'part', '""')))::text,
            '\', '\\'), 'escape')) for 16), 'hex')::uuid)
    );
    DO $$
    DECLARE
        leaf RECORD;
    BEGIN
        FOR leaf IN
            SELECT ci.relname AS index, ct.relname AS partition,
                   substr(pi.relname, length('{outbox}') + 2) AS suffix
            FROM pg_index p
            JOIN pg_class pi ON pi.oid = p.indexrelid
            JOIN pg_inherits h ON h.inhparent = p.indexrelid
            JOIN pg_class ci ON ci.oid = h.inhrelid
            JOIN pg_index x ON x.indexrelid = ci.oid
            JOIN pg_class ct ON ct.oid = x.indrelid
            WHERE p.indrelid = '{outbox}'::regclass
        LOOP
            IF leaf.index <> leaf.partition || '_' || leaf.suffix THEN
                EXECUTE format('ALTER INDEX %I RENAME TO %I', leaf.index, leaf.partition || '_' || leaf.suffix);
            END IF;
        END LOOP;
    END $$;
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
    /// and channel (ADR-0030); its last makes the table a ring (ADR-0033),
    /// and copies every row as rendered here: [`PgOutbox::migrate`] fixes
    /// the copy's bound before it.
    pub fn component(&self) -> Component {
        self.component_at(0)
    }

    /// The component, the ring's step copying from transaction `bound` on.
    fn component_at(&self, bound: u64) -> Component {
        let ring = |template: &str| {
            template
                .replace("{outbox}", self.outbox_table.as_str())
                .replace("{generations}", &self.ring.generations.to_string())
                .replace("{window}", &self.ring.window.to_string())
        };
        let sql = BASELINE
            .replace("{outbox}", self.outbox_table.as_str())
            .replace("{offsets}", self.offsets_table.as_str())
            .replace("{slots}", &self.slots.to_string());
        let identity = IDENTITY.replace("{outbox}", self.outbox_table.as_str());
        let positions = POSITIONS.replace("{offsets}", self.offsets_table.as_str());
        let format = FORMAT_COLUMN.replace("{outbox}", self.outbox_table.as_str());
        let refusal = REFUSAL_COLUMN.replace("{outbox}", self.outbox_table.as_str());
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
                Migration::new("20261104T000200Z_refusal", refusal)
                    .on(self.meta_table())
                    .checked_over(REFUSAL_COLUMN),
                Migration::new("20261104T000300Z_ring_meta", ring(RING_META))
                    .on(self.meta_table())
                    .checked_over(RING_META),
                Migration::new(
                    "20261104T000400Z_epoch_generations",
                    EPOCH_GENERATIONS.replace("{offsets}", self.offsets_table.as_str()),
                )
                .on(self.offsets_table.clone())
                .checked_over(EPOCH_GENERATIONS),
                Migration::new(RING_STEP, ring(RING).replace("{bound}", &bound.to_string()))
                    .on(self.outbox_table.clone())
                    .checked_over(RING),
            ],
        )
        .unwrap_or_else(|_| unreachable!("eight migrations with ids, under a name"))
    }

    /// Whether `error` is the refusal of a second message under an identity
    /// the transaction already wrote to the channel: a violation names the
    /// partition's index, `{outbox}_g<n>_identity_uniq`.
    fn identity_taken(&self, error: &tokio_postgres::Error) -> bool {
        let leaf = |constraint: &str| {
            constraint
                .strip_prefix(&format!("{}_g", self.outbox_table))
                .and_then(|rest| rest.strip_suffix("_identity_uniq"))
                .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
        };
        error.code() == Some(&SqlState::UNIQUE_VIOLATION)
            && error
                .as_db_error()
                .and_then(|db| db.constraint())
                .is_some_and(leaf)
    }

    /// The SQL of the reclamation's advisory key, which rewinds take
    /// exclusively and new groups shared, so that no emptying of the ring
    /// commits between their read of `intact_from` and their write
    /// (ADR-0033 decision 3).
    pub(super) fn reclamation_key(&self) -> String {
        format!("hashtextextended('{} reclamation', 0)", self.outbox_table)
    }

    /// Takes the reclamation's key in `session`'s transaction, exclusively
    /// or shared, and reads the ring's `intact_from`: none before the ring.
    pub(super) async fn hold_reclamation(
        &self,
        session: &P::Session,
        exclusively: bool,
    ) -> Result<Option<u64>, Error> {
        let lock = if exclusively {
            "pg_advisory_xact_lock"
        } else {
            "pg_advisory_xact_lock_shared"
        };
        let sql = format!(
            "SELECT {lock}({key}), (SELECT intact_from::text FROM {outbox}_meta)",
            key = self.reclamation_key(),
            outbox = self.outbox_table
        );
        let row = session.connection().query_one(&sql, &[]).await?;
        row.get::<_, Option<String>>(1)
            .map(transaction_id)
            .transpose()
    }

    /// The meta table's name.
    fn meta_table(&self) -> ascetic_ddd_session::pg::Identifier {
        ascetic_ddd_session::pg::Identifier::new(format!("{}_meta", self.outbox_table))
            .unwrap_or_else(|_| unreachable!("an identifier with a suffix is one"))
    }

    /// Brings the outbox's tables up to its migrations — a read of the
    /// ledger when they are current, so that a process starts beside live
    /// traffic without locking them — and refuses to go on when the table
    /// was cut into another number of slots, or made a ring of another
    /// shape, which is a migration, not a restart; when its positions are in
    /// a format this binary does not know; or when its positions are not
    /// drawn one at a time, which it checks before it applies anything
    /// (ADR-0035 decision 6).
    ///
    /// The steps before the ring's are applied first, and the table checked
    /// against this outbox's numbers, so that a process configured otherwise
    /// never makes the ring; then, unless the ring is made, a transaction of
    /// its own fixes the copy's bound (ADR-0033 § Consequences); then the
    /// ring's step copies from it.
    pub async fn migrate(&self) -> Result<Applied, Error> {
        self.check_names()?;
        self.pool
            .session(async |session: P::Session| self.check_sequence(&session, true).await)
            .await?;
        let options = MigrationOptions::default();
        let whole = self.component_at(0);
        let steps = whole.migrations();
        let before_ring = Component::new(whole.name(), steps[..steps.len() - 1].to_vec())
            .unwrap_or_else(|_| unreachable!("the steps before the ring's, under a name"));
        let mut applied = migrate(&self.pool, &before_ring, &options)
            .await
            .map_err(Error::Migration)?;
        self.check_meta().await?;
        let bound = if applied.unknown.iter().any(|id| id == RING_STEP) {
            None
        } else {
            self.fix_copy_bound(whole.name()).await?
        };
        let rest = migrate(&self.pool, &self.component_at(bound.unwrap_or(0)), &options)
            .await
            .map_err(Error::Migration)?;
        applied.applied.extend(rest.applied);
        applied.unknown = rest.unknown;
        self.check_ring().await?;
        Ok(applied)
    }

    /// Refuses a table name whose ring's names would pass PostgreSQL's 63
    /// bytes: the server would cut them short, and two could meet.
    fn check_names(&self) -> Result<(), Error> {
        let longest = format!(
            "{}_g{}_slot_position_idx",
            self.outbox_table,
            self.ring.generations - 1
        );
        if longest.len() > 63 {
            return Err(Error::Malformed(format!(
                "table `{}` is too long a name for its ring's: `{longest}` passes 63 bytes",
                self.outbox_table
            )));
        }
        Ok(())
    }

    /// Refuses a table cut into another number of slots, made a ring of
    /// other numbers, or with positions in a format this binary does not
    /// know.
    async fn check_meta(&self) -> Result<(), Error> {
        let (outbox, slots, ring) = (&self.outbox_table, self.slots, self.ring);
        self.pool
            .session(async |session: P::Session| {
                let row = session
                    .connection()
                    .query_one(
                        &format!(
                            "SELECT slots, format, generations, window_ids FROM {outbox}_meta"
                        ),
                        &[],
                    )
                    .await?;
                let (pinned, format): (i32, i32) = (row.get(0), row.get(1));
                let (generations, window): (i32, i64) = (row.get(2), row.get(3));
                if pinned != slots as i32 {
                    return Err(Error::Malformed(format!(
                        "table `{outbox}` is cut into {pinned} slots, this outbox asks for {slots}; \
                         the number is fixed for the life of the table"
                    )));
                }
                if generations as u32 != ring.generations || window as u64 != ring.window {
                    return Err(Error::Malformed(format!(
                        "table `{outbox}` is a ring of {generations} generations of {window} ids, \
                         this outbox asks for {} of {}; the ring is fixed for the life of the table",
                        ring.generations, ring.window
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
            .await
    }

    /// Refuses a ring whose partitions or `CHECK` are not this outbox's
    /// numbers: one component's steps may be applied by processes
    /// configured apart.
    async fn check_ring(&self) -> Result<(), Error> {
        let (outbox, ring) = (&self.outbox_table, self.ring);
        self.pool
            .session(async |session: P::Session| {
                let row = session
                    .connection()
                    .query_one(
                        &format!(
                            "SELECT (SELECT count(*) FROM pg_inherits \
                                     WHERE inhparent = '{outbox}'::regclass), \
                                    (SELECT pg_get_constraintdef(oid) FROM pg_constraint \
                                     WHERE conrelid = '{outbox}'::regclass \
                                       AND conname = '{outbox}_generation_check')"
                        ),
                        &[],
                    )
                    .await?;
                let partitions: i64 = row.get(0);
                let check: Option<String> = row.get(1);
                let window = check.as_deref().and_then(|check| {
                    // a window past 2³¹ prints as a quoted bigint literal
                    let after = check
                        .split("bigint / ")
                        .nth(1)?
                        .trim_start_matches(['\'', '(']);
                    let digits: String = after.chars().take_while(char::is_ascii_digit).collect();
                    digits.parse::<u64>().ok()
                });
                if partitions as u32 != ring.generations || window != Some(ring.window) {
                    return Err(Error::Malformed(format!(
                        "table `{outbox}` has {partitions} partitions and the check `{}`, \
                         this outbox asks for {} generations of {} ids",
                        check.unwrap_or_default(),
                        ring.generations,
                        ring.window
                    )));
                }
                Ok(())
            })
            .await
    }

    /// Fixes the bound the ring's step copies from, in a transaction of its
    /// own under the reclamation's key, unless the ring is made already
    /// (ADR-0033 § Consequences). It first advances every position with no
    /// epoch and no row of its selection new to it to the current snapshot,
    /// as an idle look would, so that a slot its selection never matched
    /// does not pull the bound down to nothing; then takes the least of
    /// every `delivered`'s `xmax`, every id of a `delivered`'s in-progress
    /// list that has a row, and this database's oldest open id, widened
    /// against the next. Every row some position has not handed on, then or
    /// at the ring's step, has an id at or above it: from its commit a
    /// rewind below it is refused and a new group starts at it. Recorded as
    /// both bounds, each only ever moved the way it stays true.
    async fn fix_copy_bound(&self, component: &str) -> Result<Option<u64>, Error> {
        let (outbox, offsets) = (&self.outbox_table, &self.offsets_table);
        let selected = "(o.uri = '' OR m.uri = o.uri OR m.uri LIKE o.uri || '/%')";
        let advance = format!(
            "UPDATE {offsets} o SET \
                delivered = pg_current_snapshot(), \
                delivered_at = CURRENT_TIMESTAMP, \
                last_processed_transaction_id = \
                    (pg_snapshot_xmin(pg_current_snapshot())::text::bigint - 1)::text::xid8, \
                offset_acked = 9223372036854775807 \
             WHERE o.epoch IS NULL \
               AND NOT EXISTS (SELECT 1 FROM {outbox} m \
                               WHERE m.slot = o.slot AND {selected} \
                                 AND m.transaction_id >= pg_snapshot_xmax(o.delivered) \
                                 AND pg_visible_in_snapshot(m.transaction_id, pg_current_snapshot())) \
               AND NOT EXISTS (SELECT 1 FROM pg_snapshot_xip(o.delivered) AS x(xid) \
                               WHERE pg_visible_in_snapshot(x.xid, pg_current_snapshot()) \
                                 AND EXISTS (SELECT 1 FROM {outbox} m \
                                             WHERE m.slot = o.slot AND {selected} \
                                               AND m.transaction_id = x.xid))"
        );
        let bound = format!(
            "WITH n AS (SELECT pg_snapshot_xmax(pg_current_snapshot())::text::bigint AS next), \
             b AS ( \
                SELECT least( \
                    (SELECT min(pg_snapshot_xmax(delivered)::text::bigint) FROM {offsets}), \
                    (SELECT min(x::text::bigint) FROM {offsets} o, pg_snapshot_xip(o.delivered) AS x \
                     WHERE EXISTS (SELECT 1 FROM {outbox} m WHERE m.transaction_id = x)), \
                    (SELECT min({sessions}) FROM pg_stat_activity a \
                     WHERE a.datname = current_database() AND a.backend_xid IS NOT NULL), \
                    (SELECT min({prepared}) FROM pg_prepared_xacts p \
                     WHERE p.database = current_database()), \
                    n.next) AS bound \
                FROM n) \
             SELECT b.bound, (SELECT count(*) FROM {outbox} WHERE transaction_id >= b.bound::text::xid8) \
             FROM b",
            sessions = widened("a.backend_xid"),
            prepared = widened("p.transaction"),
        );
        let record = format!(
            "UPDATE {outbox}_meta SET \
                intact_from = greatest(coalesce(intact_from, $1::text::xid8), $1::text::xid8), \
                gone_below = least(coalesce(gone_below, $1::text::xid8), $1::text::xid8)"
        );
        let budget = MigrationOptions::default().budget;
        let key = self.reclamation_key();
        self.pool
            .session(async |session: P::Session| {
                session
                    .atomic(async |tx: P::Session| {
                        let connection = tx.connection();
                        connection
                            .batch_execute(&format!(
                                "SET LOCAL lock_timeout = '{}ms'; SELECT pg_advisory_xact_lock({key})",
                                budget.as_millis()
                            ))
                            .await?;
                        let made: bool = connection
                            .query_one(
                                &format!(
                                    "SELECT EXISTS (SELECT 1 FROM {LEDGER} \
                                     WHERE component = $1 AND migration = $2)"
                                ),
                                &[&component, &RING_STEP],
                            )
                            .await?
                            .get(0);
                        if made {
                            return Ok(None);
                        }
                        // every position row, waited for: the advance reads after the lock
                        connection
                            .execute(&format!("SELECT 1 FROM {offsets} FOR UPDATE"), &[])
                            .await?;
                        connection.execute(&advance, &[]).await?;
                        let row = connection.query_one(&bound, &[]).await?;
                        let (bound, copied): (i64, i64) = (row.get(0), row.get(1));
                        connection
                            .execute(&record, &[&bound.to_string()])
                            .await?;
                        log::info!(
                            "outbox {outbox}: the ring's step copies {copied} rows, \
                             from transaction {bound} on"
                        );
                        Ok::<_, Error>(Some(bound as u64))
                    })
                    .await
            })
            .await
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
    /// fetch can lock one: created at the first contact, under the
    /// reclamation's key, shared. A lane — a selection of some URIs — starts
    /// where its group's selection of everything stands, slot by slot
    /// (ADR-0033 decision 4). Any other starts at the beginning the ring
    /// still holds: the current snapshot kept below `intact_from`, so that a
    /// transaction below it still running stays owed, or nothing delivered
    /// before the ring is made.
    pub(super) async fn ensure_positions(
        &self,
        session: &P::Session,
        group: &str,
        uri: &str,
    ) -> Result<(), Error> {
        self.hold_reclamation(session, false).await?;
        let start = format!(
            "CASE WHEN m.intact_from IS NULL THEN '1:1:'::pg_snapshot ELSE {} END",
            below("pg_current_snapshot()", "m.intact_from")
        );
        let sql = format!(
            "INSERT INTO {offsets} (consumer_group, uri, slot, delivered) \
             SELECT $1, $2, s.slot, COALESCE(e.delivered, {start}) \
             FROM {outbox}_meta m \
             CROSS JOIN generate_series(0, m.slots - 1) AS s(slot) \
             LEFT JOIN {offsets} e \
               ON $2 <> '' AND e.consumer_group = $1 AND e.uri = '' AND e.slot = s.slot \
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
            "SELECT delivered::text, epoch::text, epoch_offset, epoch_end, epoch_generations \
             FROM {} WHERE consumer_group = $1 AND uri = $2 ORDER BY slot",
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
    ///
    /// The look names the generations of the ring it reads, as constants
    /// worked out by a statement before it from the selection's positions
    /// and the current snapshot, so that the planner reads and locks those
    /// alone (ADR-0033 decision 3). A relation's lock is held to the
    /// transaction's end, through the batch: a look naming every generation
    /// would hold off an emptying's lock for as long as the relay is busy. A
    /// row of a window opened after that statement waits for the next look.
    pub(super) async fn take(
        &self,
        session: &P::Session,
        group: &str,
        uri: &str,
    ) -> Result<Taken, Error> {
        let row = session
            .connection()
            .query_one(
                &format!(
                    "SELECT pg_current_snapshot()::text, \
                            coalesce(array_agg(delivered::text) FILTER (WHERE epoch IS NULL), '{{}}') \
                     FROM {} WHERE consumer_group = $1 AND uri = $2",
                    self.offsets_table
                ),
                &[&group, &uri],
            )
            .await?;
        let now = snapshot(row.get::<_, String>(0))?;
        let mut names = BTreeSet::new();
        for delivered in row.get::<_, Vec<String>>(1) {
            names.extend(self.ring.generations_new(&snapshot(delivered)?, &now));
        }
        let new_rows = match in_list(&names) {
            None => "false".to_owned(),
            Some(names) => format!(
                "EXISTS (SELECT 1 FROM {outbox} m
                         WHERE m.\"generation\" IN ({names}) AND m.slot = o.slot
                           AND m.transaction_id >= pg_snapshot_xmax(o.delivered)
                           AND pg_visible_in_snapshot(m.transaction_id, pg_current_snapshot()))
                 OR EXISTS (SELECT 1 FROM pg_snapshot_xip(o.delivered) AS x(xid)
                            WHERE pg_visible_in_snapshot(x.xid, pg_current_snapshot())
                              AND EXISTS (SELECT 1 FROM {outbox} m
                                          WHERE m.\"generation\" IN ({names}) AND m.slot = o.slot
                                            AND m.transaction_id = x.xid))",
                outbox = self.outbox_table,
            ),
        };
        let sql = format!(
            r#"
            WITH taken AS (
                SELECT o.slot, o.delivered, o.epoch, o.epoch_offset, o.epoch_end, o.epoch_generations
                FROM {offsets} o
                WHERE o.consumer_group = $1 AND o.uri = $2
                  AND (o.epoch IS NOT NULL OR {new_rows})
                ORDER BY o.updated_at
                LIMIT 1
                FOR UPDATE OF o SKIP LOCKED
            )
            SELECT t.slot, t.delivered::text, t.epoch::text, t.epoch_offset, t.epoch_end,
                   t.epoch_generations,
                   (SELECT slots FROM {outbox}_meta),
                   EXISTS (SELECT 1 FROM {offsets} WHERE consumer_group = $1 AND uri = $2),
                   (SELECT refused FROM {outbox}_meta),
                   EXISTS (SELECT 1 FROM {offsets}
                           WHERE pg_snapshot_xmax(delivered) > pg_snapshot_xmax(pg_current_snapshot()))
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
            slots: row.get::<_, i32>(6) as u32,
            known: row.get(7),
            refused: row.get(8),
            reset: row.get(9),
        })
    }

    /// Forgets when each generation was first found passed: a rewind owes
    /// their rows again (ADR-0033 decision 3).
    pub(super) async fn clear_passed(&self, session: &P::Session) -> Result<(), Error> {
        let sql = format!("UPDATE {}_meta SET passed_at = NULL", self.outbox_table);
        session.connection().execute(&sql, &[]).await?;
        Ok(())
    }

    /// Records a counter reset in the meta table, in a transaction of its
    /// own, so that it sticks whatever the dispatcher's does.
    pub(super) async fn refuse(&self, reason: &str) -> Result<(), Error> {
        let sql = format!(
            "UPDATE {}_meta SET refused = $1 WHERE refused IS NULL",
            self.outbox_table
        );
        self.pool
            .session(async |session: P::Session| {
                session.connection().execute(&sql, &[&reason]).await?;
                Ok::<_, Error>(())
            })
            .await
    }

    /// Clears a recorded counter reset, once an operator has rebased the
    /// positions; the relay resumes at its next look.
    pub async fn clear_counter_reset(&self, session: &P::Session) -> Result<(), Error> {
        let sql = format!("UPDATE {}_meta SET refused = NULL", self.outbox_table);
        session.connection().execute(&sql, &[]).await?;
        Ok(())
    }

    /// Where `selection` stands, by slot, for an operator: how long the
    /// epoch in flight has been handed on and how far, and how long ago
    /// `delivered` last moved. Snapshots have no order, so the positions of
    /// several slots have no minimum; ages are the lag (ADR-0035
    /// decision 13).
    pub async fn backlog(
        &self,
        session: &P::Session,
        selection: &super::Selection,
    ) -> Result<Vec<super::Backlog>, Error> {
        let sql = format!(
            "SELECT slot, \
                    CASE WHEN epoch IS NULL THEN NULL \
                         ELSE extract(epoch FROM CURRENT_TIMESTAMP - epoch_at)::float8 END, \
                    epoch_offset, epoch_end, \
                    extract(epoch FROM CURRENT_TIMESTAMP - delivered_at)::float8 \
             FROM {} WHERE consumer_group = $1 AND uri = $2 ORDER BY slot",
            self.offsets_table
        );
        let age = |seconds: Option<f64>| {
            seconds.map(|seconds| std::time::Duration::from_secs_f64(seconds.max(0.0)))
        };
        let rows = session
            .connection()
            .query(&sql, &[&selection.consumer_group, &selection.uri])
            .await?;
        Ok(rows
            .iter()
            .map(|row| super::Backlog {
                slot: row.get::<_, i16>(0) as u32,
                epoch_age: age(row.get(1)),
                epoch_offset: row.get(2),
                epoch_end: row.get(3),
                delivered_age: age(row.get(4)),
            })
            .collect())
    }

    /// An epoch's start (ADR-0035 decision 3): `pg_current_snapshot()`,
    /// taken in a statement of its own, after the lock; and the lowest and
    /// highest positions of the slot's rows of the selection new in it —
    /// visible in it and not in `delivered` — in each generation of the ring
    /// they can lie in, named as constants worked out from the two snapshots
    /// (ADR-0033 decision 3). Given a stored epoch instead, converted from
    /// ADR-0007's positions with no end yet, the same of its rows. The
    /// snapshots are bound as parameters, `delivered`'s `xmax` and the ids
    /// of its in-progress list the epoch sees apart, so that no row's walk
    /// unpacks one. The sequence's cache is read again, so that an `ALTER
    /// SEQUENCE` made while the service runs stops the relay rather than
    /// reorders it.
    pub(super) async fn start_epoch(
        &self,
        session: &P::Session,
        slot: u32,
        uri: &str,
        delivered: &Snapshot,
        stored: Option<&Snapshot>,
    ) -> Result<Start, Error> {
        let epoch = match stored {
            Some(stored) => stored.clone(),
            None => {
                let row = session
                    .connection()
                    .query_one("SELECT pg_current_snapshot()::text", &[])
                    .await?;
                self.check_sequence(session, false).await?;
                snapshot(row.get::<_, String>(0))?
            }
        };
        let Some(names) = in_list(&self.ring.generations_new(delivered, &epoch)) else {
            return Ok(Start {
                epoch,
                first: None,
                last: None,
                generations: Vec::new(),
            });
        };
        let sql = format!(
            r#"
            WITH members AS (
                SELECT o."generation" AS g, o."position" AS p FROM {outbox} o
                WHERE o."generation" IN ({names}) AND o.slot = $1
                  AND ($2 = '' OR o.uri = $2 OR o.uri LIKE $3)
                  AND o.transaction_id >= $4::text::xid8
                  AND o.transaction_id < $5::text::xid8
                  AND pg_visible_in_snapshot(o.transaction_id, $6::text::pg_snapshot)
                UNION ALL
                SELECT o."generation", o."position" FROM {outbox} o
                WHERE o."generation" IN ({names}) AND o.slot = $1
                  AND ($2 = '' OR o.uri = $2 OR o.uri LIKE $3)
                  AND o.transaction_id = ANY($7::text[]::xid8[])
            )
            SELECT g, min(p), max(p) FROM members GROUP BY g ORDER BY g
            "#,
            outbox = self.outbox_table,
        );
        let prefix = format!("{uri}/%");
        let ended: Vec<String> = delivered
            .xip
            .iter()
            .filter(|xid| epoch.sees(**xid))
            .map(u64::to_string)
            .collect();
        let rows = session
            .connection()
            .query(
                &sql,
                &[
                    &(slot as i16),
                    &uri,
                    &prefix,
                    &delivered.xmax.to_string(),
                    &epoch.xmax.to_string(),
                    &epoch.to_string(),
                    &ended,
                ],
            )
            .await?;
        let generations: Vec<Generation> = rows
            .iter()
            .map(|row| Generation {
                generation: row.get::<_, i16>(0) as u32,
                first: row.get(1),
                last: row.get(2),
            })
            .collect();
        Ok(Start {
            epoch,
            first: generations.iter().map(|g| g.first).min(),
            last: generations.iter().map(|g| g.last).max(),
            generations,
        })
    }

    /// A batch of an epoch (ADR-0035 decision 4): the slot's rows of the
    /// selection past the epoch's offset, visible in it and not in
    /// `delivered`, in the order their positions were drawn. Each generation
    /// the epoch found rows in is a branch of its own, named as a constant,
    /// scanned from the later of the offset and its first position up to its
    /// last, with its own limit, and passed by once read; the branches are
    /// merged by position (ADR-0033 decision 3). Without a branch's limit,
    /// every batch read the whole remaining epoch. The statement's snapshot,
    /// taken after the lock, contains the epoch, so every row of it can be
    /// read here.
    pub(super) async fn read_batch(
        &self,
        session: &P::Session,
        slot: u32,
        uri: &str,
        epoch: &Epoch,
        delivered: &Snapshot,
    ) -> Result<Read, Error> {
        let branches: Vec<String> = epoch
            .generations
            .iter()
            .filter(|generation| generation.last > epoch.offset)
            .map(|generation| {
                format!(
                    r#"(SELECT "position", transaction_id, uri, payload, metadata, created_at
                        FROM {outbox}
                        WHERE "generation" = {name} AND slot = $1
                          AND ($2 = '' OR uri = $2 OR uri LIKE $3)
                          AND "position" > {from} AND "position" <= {last}
                          AND pg_visible_in_snapshot(transaction_id, $4::text::pg_snapshot)
                          AND NOT pg_visible_in_snapshot(transaction_id, $5::text::pg_snapshot)
                        ORDER BY "position"
                        LIMIT $6)"#,
                    outbox = self.outbox_table,
                    name = generation.generation,
                    from = epoch.offset.max(generation.first - 1),
                    last = generation.last,
                )
            })
            .collect();
        if branches.is_empty() {
            let row = session
                .connection()
                .query_one("SELECT pg_current_snapshot()::text", &[])
                .await?;
            return Ok(Read {
                statement: snapshot(row.get::<_, String>(0))?,
                messages: Vec::new(),
            });
        }
        let sql = format!(
            r#"
            WITH s AS MATERIALIZED (SELECT pg_current_snapshot() AS snap)
            SELECT (SELECT snap::text FROM s), m."position", m.transaction_id::text, m.uri,
                   m.payload, m.metadata, m.created_at::text
            FROM (SELECT 1) AS one
            LEFT JOIN LATERAL (
                SELECT * FROM ({branches}) AS b ORDER BY "position" LIMIT $6
            ) AS m ON true
            ORDER BY m."position"
            "#,
            branches = branches.join(" UNION ALL "),
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
                    &epoch.snapshot.to_string(),
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
                epoch_offset = $6, epoch_end = $7, epoch_generations = $9::jsonb, \
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
                    &epoch.map(|epoch| generations_json(&epoch.generations)),
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
    /// the transaction it goes back to; the old pair follows. The caller
    /// holds the reclamation's key and has checked the ring's bound.
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
            Move::From(_) => below("delivered", "$3::text::xid8"),
        };
        let offsets = &self.offsets_table;
        let sql = format!(
            "UPDATE {offsets} SET delivered = {delivered}, epoch = NULL, epoch_offset = 0, \
                epoch_end = NULL, epoch_generations = NULL, delivered_at = CURRENT_TIMESTAMP, \
                updated_at = CURRENT_TIMESTAMP \
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
pub(super) fn transaction_id(text: String) -> Result<u64, Error> {
    text.parse()
        .map_err(|_| Error::Malformed(format!("transaction id `{text}`")))
}

/// A snapshot as PostgreSQL prints it.
fn snapshot(text: String) -> Result<Snapshot, Error> {
    text.parse().map_err(Error::Malformed)
}

/// A position from five columns of `row` from `at`: `delivered`, `epoch`,
/// `epoch_offset`, `epoch_end`, `epoch_generations`.
fn position_of(row: &Row, at: usize) -> Result<Position, Error> {
    let epoch = match row.get::<_, Option<String>>(at + 1) {
        Some(text) => Some(Epoch {
            snapshot: snapshot(text)?,
            offset: row.get(at + 2),
            end: row.get(at + 3),
            generations: generations_of(row.get(at + 4))?,
        }),
        None => None,
    };
    Ok(Position {
        delivered: snapshot(row.get::<_, String>(at))?,
        epoch,
    })
}

/// An epoch's generations as stored, `[[generation, first, last], …]`.
fn generations_json(generations: &[Generation]) -> Value {
    Value::Array(
        generations
            .iter()
            .map(|g| Value::from(vec![i64::from(g.generation), g.first, g.last]))
            .collect(),
    )
}

/// An epoch's generations read back; none stored, none.
fn generations_of(stored: Option<Value>) -> Result<Vec<Generation>, Error> {
    let malformed = || Error::Malformed(format!("an epoch's generations `{stored:?}`"));
    let Some(Value::Array(entries)) = &stored else {
        return match stored {
            None => Ok(Vec::new()),
            Some(_) => Err(malformed()),
        };
    };
    entries
        .iter()
        .map(|entry| match entry.as_array().map(Vec::as_slice) {
            Some([generation, first, last]) => Some(Generation {
                generation: u32::try_from(generation.as_u64()?).ok()?,
                first: first.as_i64()?,
                last: last.as_i64()?,
            }),
            _ => None,
        })
        .collect::<Option<_>>()
        .ok_or_else(malformed)
}

/// Generations as constants for an `IN` list; none for none, which no list
/// can say.
fn in_list(names: &BTreeSet<u32>) -> Option<String> {
    (!names.is_empty()).then(|| {
        names
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    })
}

/// The SQL of what `snapshot` sees below transaction `xid` and nothing from
/// it on — a rewind's intersection (ADR-0035 decision 11) — with `xid` no
/// lower than 1, since `'0:0:'` is no snapshot.
fn below(snapshot: &str, xid: &str) -> String {
    let xid = format!("greatest({xid}, '1'::xid8)");
    format!(
        "(least(pg_snapshot_xmin({snapshot}), {xid})::text || ':' \
          || least(pg_snapshot_xmax({snapshot}), {xid})::text || ':' \
          || coalesce((SELECT string_agg(x::text, ',' ORDER BY x) \
                       FROM pg_snapshot_xip({snapshot}) AS x WHERE x < {xid}), '') \
         )::pg_snapshot"
    )
}

/// The SQL of a 32-bit transaction id `xid`, as `pg_stat_activity` and
/// `pg_prepared_xacts` show it, widened to the full id nearest the next one,
/// `n.next`: an open transaction's id lies within 2³¹ of it.
pub(super) fn widened(xid: &str) -> String {
    format!(
        "(n.next + (({xid}::text::bigint - n.next % 4294967296 + 6442450944) % 4294967296) \
          - 2147483648)"
    )
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
