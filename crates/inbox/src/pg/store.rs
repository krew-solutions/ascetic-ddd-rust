//! The tables and the statements: what the inbox does to PostgreSQL. The
//! loop that drives them is in `dispatch`.

use std::time::Duration;

use ascetic_ddd_migration::{
    Applied, Component, Migration, Options as MigrationOptions, forget, migrate,
};
use ascetic_ddd_session::{PgAccess, Session, SessionPool};
use tokio_postgres::Row;

use super::{Ground, Outage, Parked, PgInbox};
use crate::error::{Error, Failure};
use crate::message::{CausalDependency, InboxMessage};
use crate::observer::{InboxObserver, Receipt};
use crate::port::Inbox;
use crate::snapshot::Snapshot;

/// The columns of a row, in the order `message_of` reads them.
const COLUMNS: &str = "tenant_id, stream_type, stream_id, stream_position, uri, payload, metadata, \
                       received_position, processed_position, attempts, last_error, waiting_for";
/// A row's identity, its channel, its order of arrival and what it waits
/// for, in the order `narrow_of` reads them: never its payload or metadata,
/// which a statement returning many rows must not carry (ADR-0037 decision
/// 6).
const NARROW: &str =
    "tenant_id, stream_type, stream_id, stream_position, uri, received_position, waiting_for";
/// A row's channel, its `uri` without the key: the expression the parked
/// index and the sweep's index are built over, spelled as they spell it. The
/// key is the producer's text and may hold a line break; the channel is the
/// inbox's own name.
pub(super) const CHANNEL_OF_ROW: &str =
    "(split_part(uri, '/', 1) || '//' || split_part(uri, '/', 3))";
/// A parked row's ground ([`super::Ground`]), from its columns: a row parked
/// at once never waits (the park takes it from the queue), and every exit
/// from the parked state clears `unreadable_at` with `parked_at`.
const GROUND_OF_ROW: &str = "CASE WHEN unreadable_at IS NOT NULL THEN 'at_once' \
     WHEN waiting_for IS NOT NULL THEN 'expired' ELSE 'failure' END";

/// The parked rows by channel and ground, with the oldest park: one statement
/// over the parked index's predicate, so that its work is the parked rows',
/// not the table's.
pub(super) fn parked_counts_sql(table: &str) -> String {
    format!(
        "SELECT {CHANNEL_OF_ROW}, {GROUND_OF_ROW}, count(*), min(parked_at)::text FROM {table} \
         WHERE parked_at IS NOT NULL GROUP BY 1, 2 ORDER BY 1, 2"
    )
}
/// The heads held by their own failure, by channel and cause, with how
/// many Tenants they are of and the oldest hold: one statement over the held
/// index's predicate, so that its work is the held rows', not the table's
/// (ADR-0041 decision 5). No Tenant's identifier leaves the database.
pub(super) fn held_counts_sql(table: &str) -> String {
    format!(
        "SELECT {CHANNEL_OF_ROW}, 'backoff', count(DISTINCT tenant_id), min(held_since)::text \
         FROM {table} WHERE held_since IS NOT NULL GROUP BY 1, 2 ORDER BY 1, 2"
    )
}
/// The take's statement over `table` ([`PgInbox::take`]): a function, so
/// that a test explains the library's own statement. Its look at the whole
/// queue is ordered as `__head_idx` is: an `EXISTS` the planner takes for a
/// scan of the table, the queue's rows spread evenly in its estimate, and
/// they lie past the processed history — 35 ms on 100,000 rows against 0.1
/// by the index (E8). Each slot's look is one probe of the same index, its
/// newest queue row, in a lateral subquery whose `LIMIT` keeps the planner
/// from pulling it up: an `EXISTS` it pulls up into a semi-join that reads
/// the whole queue — every row of it, 2,250 at 64 slots in E8 — where one
/// probe a slot reads one. That row answers both questions of the slot:
/// whether it has a queue row, and whether one arrived after its round
/// ended: one reference to the table fewer than a probe for each. The body of
/// `{table}__take()` ([`take_body`]).
pub(super) fn take_sql(table: &str) -> String {
    format!(
        r#"
            WITH taken AS (
                SELECT s.slot FROM {table}_slots s
                CROSS JOIN LATERAL (
                    SELECT r.received_position AS newest FROM {table} r
                    WHERE r.slot = s.slot AND {QUEUE}
                    ORDER BY r.received_position DESC LIMIT 1
                ) q
                WHERE (SELECT true FROM {table} q WHERE {QUEUE}
                       ORDER BY q.slot, q.received_position LIMIT 1) IS TRUE
                  AND (s.round_from IS NOT NULL
                       OR s.dormant_until IS NULL
                       OR s.dormant_until <= CURRENT_TIMESTAMP
                       OR q.newest > s.dormant_after)
                ORDER BY s.served_at
                LIMIT 1
                FOR UPDATE OF s SKIP LOCKED
            )
            SELECT t.slot, pg_current_snapshot()::text, (SELECT slots FROM {table}_meta)
            FROM (SELECT 1) AS one
            LEFT JOIN taken t ON true
            "#
    )
}

/// The walk's statement over `table` ([`PgInbox::locate_head`]), its
/// parameters the slot, the walk's bound and the longest sleep in seconds.
///
/// The body of `{table}__walk` ([`walk_body`]), planned once a connection
/// and again at each change of its tables' definition or statistics. Sent as
/// text it was parsed and planned at every call, its references to the inbox
/// table most of the planning (ADR-0041 decision 6); it keeps them few, which
/// also cut its execution, 0.87 ms to 0.60 on M9's shape.
/// - The walk starts from a step of its own at the cursor, `n = 0`, which
///   names no head, so that the first step is any other: one recursive
///   term, not a first term repeating its probes.
/// - A step looks twice: at the next Tenant after the one it is at, kept
///   in the second phase only while it is not past the cursor — a probe
///   whose `LIMIT` stands before that test reads one index entry, where a
///   bound inside it would read on past the cursor; and, in the first
///   phase, at the lowest Tenant, the walk wrapping round.
/// - The verdict reads the locked head by a join, not by a subquery for
///   each of its uses: the planner writes the verdict into the `UPDATE`,
///   and a subquery there is planned at every place it lands.
/// - The slot's newest queue row is a probe of `__head_idx`, not `max()`,
///   which the planner plans twice, as an aggregate and as that probe.
pub(super) fn walk_sql(table: &str) -> String {
    format!(
        r#"
            WITH RECURSIVE here AS (
                SELECT coalesce(last_tenant, '') AS c, round_from, dormant_until, dormant_after
                FROM {table}_slots WHERE slot = $1
            ), walk (n, phase, tenant_id, place, due, retry_at) AS (
                SELECT 0, 0, c, NULL::tid, false, NULL::timestamptz FROM here
                UNION ALL
                SELECT w.n + 1, h.phase, h.tenant_id, h.place, h.due, h.retry_at
                FROM walk w CROSS JOIN here CROSS JOIN LATERAL (
                    SELECT * FROM (
                        (SELECT w.phase AS phase, after.* FROM (
                             SELECT {WALKED} FROM {table} r
                             WHERE r.slot = $1 AND {QUEUE} AND r.tenant_id > w.tenant_id
                             ORDER BY r.tenant_id, r.received_position LIMIT 1) after
                         WHERE w.phase = 0 OR after.tenant_id <= here.c)
                        UNION ALL
                        (SELECT 1, {WALKED} FROM {table} r
                         WHERE w.phase = 0 AND r.slot = $1 AND {QUEUE} AND r.tenant_id <= here.c
                         ORDER BY r.tenant_id, r.received_position LIMIT 1)
                    ) next ORDER BY phase LIMIT 1
                ) h
                WHERE w.n < $2 AND NOT w.due
            ), last AS (
                SELECT * FROM walk WHERE n > 0 AND n = (SELECT max(n) FROM walk)
            ), head AS (
                SELECT r.received_position, r.ctid::text AS place, r.tenant_id
                FROM {table} r JOIN last ON r.ctid = last.place
                WHERE last.due AND {QUEUE}
                FOR UPDATE OF r
            ), verdict AS (
                SELECT h.tenant_id IS NOT NULL AS taken, h.tenant_id AS taken_tenant,
                       l.tenant_id AS u, l.phase AS u_phase,
                       coalesce(l.due, false) AND h.tenant_id IS NULL AS vanished,
                       seen.n < $2 AND NOT coalesce(l.due, false) AS exhausted,
                       seen.earliest, here.c, here.round_from, here.dormant_until, here.dormant_after,
                       here.round_from IS NULL AS starts
                FROM here
                CROSS JOIN (SELECT max(n) AS n, min(retry_at) FILTER (WHERE NOT due) AS earliest
                            FROM walk) seen
                LEFT JOIN last l ON true
                LEFT JOIN head h ON true
            ), decided AS (
                SELECT v.*,
                       CASE WHEN v.starts THEN v.c ELSE v.round_from END AS r_from,
                       v.exhausted OR (NOT v.starts AND
                           ROW(CASE WHEN v.round_from > v.c THEN 0 ELSE 1 END, v.round_from)
                               <= ROW(v.u_phase, v.u)) AS ends,
                       CASE WHEN v.starts THEN v.earliest
                            ELSE least(v.dormant_until, v.earliest) END AS round_earliest,
                       CASE WHEN v.starts
                            THEN (SELECT q.received_position FROM {table} q
                                  WHERE q.slot = $1 AND {QUEUE}
                                  ORDER BY q.received_position DESC LIMIT 1)
                            ELSE v.dormant_after END AS round_after
                FROM verdict v
            ), updated AS (
                UPDATE {table}_slots s SET
                    served_at = CURRENT_TIMESTAMP,
                    last_tenant = CASE WHEN d.taken THEN d.taken_tenant
                                       WHEN d.u IS NOT NULL AND NOT d.vanished THEN d.u
                                       ELSE s.last_tenant END,
                    round_from = CASE WHEN d.taken OR d.u IS NULL OR d.vanished OR d.ends THEN NULL
                                      ELSE d.r_from END,
                    dormant_until = CASE WHEN d.taken OR d.u IS NULL OR d.vanished THEN NULL
                                         WHEN d.ends THEN least(d.round_earliest,
                                             CURRENT_TIMESTAMP + make_interval(secs => $3))
                                         ELSE d.round_earliest END,
                    dormant_after = CASE WHEN d.taken OR d.u IS NULL OR d.vanished THEN NULL
                                         ELSE d.round_after END
                FROM decided d
                WHERE s.slot = $1
                RETURNING CASE WHEN d.taken THEN 'taken'
                               WHEN d.u IS NULL OR d.vanished OR d.ends THEN 'nothing'
                               ELSE 'passed' END AS outcome
            )
            SELECT h.received_position, h.place, u.outcome, pg_current_snapshot()::text
            FROM updated u LEFT JOIN head h ON true
            "#
    )
}
/// How many rows one statement of the expiry parks.
const EXPIRY_BATCH: i64 = 100;
/// The most characters of a failure's text a row keeps (ADR-0037 decision
/// 7): `last_error` is stored unsealed, beside the row.
const FAILURE_WIDTH: usize = 4096;
/// A row's identity as the server writes it from the row's own columns:
/// the form a dependency on it is waited for under.
const IDENTITY_OF_ROW: &str = "jsonb_build_object('tenant_id', tenant_id, 'stream_type', stream_type, \
                               'stream_id', stream_id, 'stream_position', stream_position)";
/// The rows a dispatcher may take: neither processed, parked nor waiting.
pub(super) const QUEUE: &str =
    "processed_position IS NULL AND parked_at IS NULL AND waiting_for IS NULL";

/// The inbox's sequence, tables, indexes and functions as created,
/// `{table}`, `{sequence}`, `{slots}` and `{key}` standing for their names,
/// the cut and the partition key's expression, `{key_text}` for that
/// expression as a literal, `{queue}` for [`QUEUE`], and `{take}` and
/// `{walk}` for the functions' bodies: the baseline of its component, its
/// checksum the template's with the queue and the bodies written out
/// ([`baseline_template`]), so that an edit of the take's or the walk's
/// statement is an edit of the baseline.
///
/// The take and the walk are functions the dispatch calls, not statements
/// it sends (ADR-0041 decision 6): a statement sent as text is parsed and
/// planned at every call, which cost the walk 3.3 ms against its 0.4 of
/// execution; a function's statement is parsed once a connection and its
/// plan kept until its tables' definition or statistics change. The walk's plan is forced generic, so that it is planned once
/// whatever the session asks — the specification crate's queries need custom
/// plans (ADR-0027) — and its generic plan is the one E8 holds to the
/// indexes. The take's statement has no parameter, and a plan without one
/// is generic already. `CREATE`, not `CREATE OR REPLACE`: a function left
/// behind by a teardown that missed it fails the next baseline.
const BASELINE: &str = r#"
    CREATE SEQUENCE IF NOT EXISTS {sequence};
    CREATE TABLE IF NOT EXISTS {table} (
        tenant_id varchar(128) NOT NULL,
        stream_type varchar(128) NOT NULL,
        stream_id jsonb NOT NULL,
        stream_position integer NOT NULL,
        uri varchar(255) NOT NULL,
        payload bytea NOT NULL,
        metadata jsonb NULL,
        received_position bigint NOT NULL DEFAULT nextval('{sequence}'),
        processed_position bigint NULL,
        attempts integer NOT NULL DEFAULT 0,
        last_error text NULL,
        next_attempt_at timestamptz NULL,
        parked_at timestamptz NULL,
        waiting_for jsonb NULL,
        waiting_since timestamptz NULL,
        slot smallint GENERATED ALWAYS AS ((hashtext({key}) & 2147483647) % {slots}) STORED,
        received_at timestamptz NOT NULL DEFAULT statement_timestamp(),
        held_since timestamptz NULL,
        CONSTRAINT {table}_pk PRIMARY KEY (tenant_id, stream_type, stream_id, stream_position)
    );
    CREATE INDEX IF NOT EXISTS {table}__head_idx
        ON {table} (slot, received_position) WHERE {queue};
    CREATE INDEX IF NOT EXISTS {table}__tenant_head_idx
        ON {table} (slot, tenant_id, received_position) WHERE {queue};
    CREATE INDEX IF NOT EXISTS {table}__held_idx
        ON {table} ((split_part(uri, '/', 1) || '//' || split_part(uri, '/', 3)), held_since)
        INCLUDE (tenant_id) WHERE held_since IS NOT NULL;
    CREATE INDEX IF NOT EXISTS {table}__waiting_idx
        ON {table} (waiting_for) WHERE waiting_for IS NOT NULL;
    CREATE INDEX IF NOT EXISTS {table}__waiting_since_idx
        ON {table} (waiting_since) WHERE waiting_for IS NOT NULL;
    CREATE UNIQUE INDEX IF NOT EXISTS {table}__message_id_uniq
        ON {table} (((metadata->>'message_id')::uuid));
    CREATE TABLE IF NOT EXISTS {table}_meta (slots integer NOT NULL, partition_key text NOT NULL);
    INSERT INTO {table}_meta (slots, partition_key) SELECT {slots}, {key_text}
        WHERE NOT EXISTS (SELECT 1 FROM {table}_meta);
    CREATE TABLE IF NOT EXISTS {table}_slots (
        slot smallint PRIMARY KEY,
        served_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        last_tenant varchar(128) NULL,
        round_from varchar(128) NULL,
        dormant_until timestamptz NULL,
        dormant_after bigint NULL
    );
    INSERT INTO {table}_slots (slot) SELECT s FROM generate_series(0, {slots} - 1) AS s
        ON CONFLICT DO NOTHING;
    CREATE FUNCTION {table}__take(
        OUT taken_slot smallint, OUT take_snapshot text, OUT slot_count integer)
        LANGUAGE plpgsql VOLATILE
        AS $fn${take}$fn$;
    CREATE FUNCTION {table}__walk(smallint, integer, double precision,
        OUT head_position bigint, OUT head_place text, OUT walk_outcome text,
        OUT walk_snapshot text)
        LANGUAGE plpgsql VOLATILE SET plan_cache_mode = force_generic_plan
        AS $fn${walk}$fn$;
"#;

/// The baseline with the queue and the functions' bodies written out,
/// `{table}` and the cut's placeholders left: what its checksum is over.
fn baseline_template() -> String {
    BASELINE
        .replace("{queue}", QUEUE)
        .replace("{take}", &take_body("{table}"))
        .replace("{walk}", &walk_body("{table}"))
}

/// The body of `{table}__take`: the take's statement, its one row into the
/// function's columns. `use_column`, so that a name the statement shares with
/// a column of the function means what it means in the statement alone,
/// which E8 explains; `STRICT`, so that a statement that returned no row
/// fails, as the statement sent alone failed `query_one`.
pub(super) fn take_body(table: &str) -> String {
    format!(
        "\n    #variable_conflict use_column\n    BEGIN\n{}\n    INTO STRICT taken_slot, take_snapshot, \
         slot_count;\n    END\n    ",
        take_sql(table).trim_end()
    )
}

/// The body of `{table}__walk`, as [`take_body`] is the take's: `$1`–`$3`
/// are the function's own arguments.
pub(super) fn walk_body(table: &str) -> String {
    format!(
        "\n    #variable_conflict use_column\n    BEGIN\n{}\n    INTO STRICT head_position, head_place, \
         walk_outcome, walk_snapshot;\n    END\n    ",
        walk_sql(table).trim_end()
    )
}

/// A function the dispatch calls, as the start check requires it: its name
/// after the table's; its arguments, in and out, as the catalogue holds them
/// — their types, modes and names, as array literals — not as the server
/// renders them, a rendering that `quote_all_identifiers` changes (ADR-0037's
/// lesson); and the setting it carries. Written here, not read from
/// [`BASELINE`], so that the baseline cannot drop what the check requires.
struct Called {
    suffix: &'static str,
    identity: &'static str,
    types: &'static str,
    modes: &'static str,
    names: &'static str,
    setting: Option<&'static str>,
}

/// `{table}__take()`.
const TAKE_FUNCTION: Called = Called {
    suffix: "__take",
    identity: "()",
    types: "{smallint,text,integer}",
    modes: "{o,o,o}",
    names: "{taken_slot,take_snapshot,slot_count}",
    setting: None,
};

/// `{table}__walk(slot, walk, max_dormancy)`.
const WALK_FUNCTION: Called = Called {
    suffix: "__walk",
    identity: "(smallint, integer, double precision)",
    types: "{smallint,integer,\"double precision\",bigint,text,text,text}",
    modes: "{i,i,i,o,o,o,o}",
    names: "{\"\",\"\",\"\",head_position,head_place,walk_outcome,walk_snapshot}",
    setting: Some("plan_cache_mode=force_generic_plan"),
};

/// What a failure did to its row.
pub(super) struct Recorded {
    /// Failed attempts so far, this one included when it spent one.
    pub(super) attempts: u32,
    /// Whether the row is now parked.
    pub(super) parked: bool,
    /// Whether it holds for its stage's outage.
    pub(super) held: bool,
    /// Whether it has held past its budget.
    pub(super) past_budget: bool,
    /// How long it waits before it may be taken again.
    pub(super) retry_after: Duration,
}

/// What taking a slot returned: the slot whose row the statement locked, or
/// none when no slot had a queue row and was awake; the statement's
/// snapshot, read in the same statement because under `READ COMMITTED` every
/// statement takes a snapshot of its own; and how many slots the table is
/// cut into.
pub(super) struct Taken {
    pub(super) slot: Option<u32>,
    pub(super) snapshot: Snapshot,
    pub(super) slots: u32,
}

/// What one walk of a held slot returned: the due head it took, by its
/// order of arrival and its place in the table (`ctid`); or none, and then
/// whether the walk only passed the Tenants it examined, the round going on,
/// rather than ending the round or finding no queue row; and the statement's
/// snapshot.
pub(super) struct Located {
    pub(super) row: Option<(i64, String)>,
    pub(super) passed: bool,
    pub(super) snapshot: Snapshot,
}

/// One head of the walk: its Tenant, its place, whether it is due and when
/// its backoff ends. `r` is the queue row read.
const WALKED: &str = "r.tenant_id, r.ctid AS place, \
     (r.next_attempt_at IS NULL OR r.next_attempt_at <= CURRENT_TIMESTAMP) AS due, \
     r.next_attempt_at AS retry_at";

/// The mark's time, and an index over it by channel — the destination
/// without its key — for the processed rows alone, which the sweep walks
/// (ADR-0033 decision 4). Rows processed before carry none and are kept
/// until [`PgInbox::stamp_processed`] stamps them; the index starts empty.
const PROCESSED_AT: &str = r#"
    ALTER TABLE {table} ADD COLUMN processed_at timestamptz NULL;
    CREATE INDEX {table}__forgettable_idx ON {table}
        ((split_part(uri, '/', 1) || '//' || split_part(uri, '/', 3)), processed_at)
        WHERE processed_at IS NOT NULL;
"#;

/// When a row was parked at once for its own values (ADR-0037 decision 6):
/// such a row is superseded by the next arrival at its identity. Nullable,
/// without a default, as every column the library adds (decision 5).
const UNREADABLE_AT: &str = r#"
    ALTER TABLE {table} ADD COLUMN unreadable_at timestamptz NULL;
"#;

/// Why a row failed, why it is parked, and since when it holds for its
/// stage's outage (ADR-0040 decisions 2, 4 and 6): a cause is a
/// name of at most 64 characters, never a message's text. Nullable, without
/// a default, as every column the library adds.
const CAUSES: &str = r#"
    ALTER TABLE {table} ADD COLUMN last_cause varchar(64) NULL,
        ADD COLUMN parked_cause varchar(64) NULL,
        ADD COLUMN unavailable_since timestamptz NULL;
"#;

/// What the intake sets aside beside the inbox, never stored as a row
/// (ADR-0036 decisions 2 and R4): the message's frame as `bytea`, under the
/// digest of the frame; the columns derived from it as bounded text without
/// a NUL, never `jsonb`, `uuid` or a bounded `varchar` of its text, so that
/// what the inbox refused this table takes; its identity as a digest of it;
/// the holder of a `message_id` taken as its channel and received position.
/// A row keeps its bytes until it is disposed of or its life ends, and its
/// tombstone after.
const REJECTS: &str = r#"
    CREATE TABLE {table}_rejects (
        digest uuid PRIMARY KEY,
        channel text NOT NULL,
        tenant_id text NULL,
        stream_type text NULL,
        identity uuid NULL,
        reason text NOT NULL,
        detail text NOT NULL,
        holder_channel text NULL,
        holder_position bigint NULL,
        message bytea NULL,
        seen integer NOT NULL DEFAULT 1,
        first_seen_at timestamptz NOT NULL DEFAULT statement_timestamp(),
        last_seen_at timestamptz NOT NULL DEFAULT statement_timestamp(),
        disposed_at timestamptz NULL,
        disposition text NULL,
        disposed_by text NULL,
        disposed_because text NULL
    );
    CREATE INDEX {table}_rejects__open_idx ON {table}_rejects (channel, reason, first_seen_at)
        WHERE disposed_at IS NULL;
    CREATE INDEX {table}_rejects__closed_idx ON {table}_rejects (channel, disposed_at)
        WHERE disposed_at IS NOT NULL;
    CREATE INDEX {table}_rejects__identity_idx ON {table}_rejects (identity)
        WHERE identity IS NOT NULL;
"#;

/// The indexes the live inbox gets while it is small (ADR-0036 R11): the
/// `message_id` unique within its Tenant and stream type (R5), and the
/// parked rows by channel, for the gauge and the operator's page of Part B.
/// The runner holds the table in ACCESS EXCLUSIVE mode for both builds.
const INDEXES_BEFORE_LAUNCH: &str = r#"
    DROP INDEX {table}__message_id_uniq;
    CREATE UNIQUE INDEX {table}__message_id_uniq
        ON {table} (tenant_id, stream_type, ((metadata->>'message_id')::uuid));
    CREATE INDEX {table}__parked_idx ON {table}
        ((split_part(uri, '/', 1) || '//' || split_part(uri, '/', 3)), received_position)
        INCLUDE (parked_at) WHERE parked_at IS NOT NULL;
"#;

impl<P, O> PgInbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: InboxObserver,
{
    /// The inbox's tables as a component of the database's ledger
    /// (ADR-0029), `inbox:<table>`: its baseline is the sequence, the
    /// tables and the indexes below, its checksum over their template,
    /// whatever the names, the cut and the key; its second step gives the
    /// mark its time (ADR-0033).
    ///
    /// The head index, `(slot, received_position)` over the queue — the rows
    /// neither processed, parked nor waiting — is the walk's order: the
    /// oldest row of a slot is its first entry whatever the backlog. It is
    /// the only index in `received_position` order on purpose: given a
    /// second one over the whole column, the planner, taking the queue rows
    /// for evenly spread, walks that one from the first row and finds the
    /// head only past the whole processed history — 89 ms against 0.4 on
    /// 200,000 rows a quarter of them processed, and worse the longer the
    /// history. The sequence keeps `received_position` unique without a
    /// constraint. The waiting rows have an index by the dependency they
    /// wait for, which the mark uses to wake them, and one by when they
    /// started waiting, which expiry uses; the dependency check goes by the
    /// primary key. The slot is a stored column computed from the partition
    /// key, so the rows carry their slot for good and no statement
    /// recomputes it; `<table>_meta` holds the number of slots and the key
    /// the table was cut by, and `<table>_slots` one row per slot, the row a
    /// dispatcher locks to hold the slot.
    pub fn component(&self) -> Component {
        let key = self.partition.sql_expression();
        let template = baseline_template();
        let sql = template
            .replace("{table}", self.table.as_str())
            .replace("{sequence}", self.sequence.as_str())
            .replace("{slots}", &self.slots.to_string())
            .replace("{key_text}", &format!("'{}'", key.replace('\'', "''")))
            .replace("{key}", key);
        Component::new(
            format!("inbox:{}", self.table),
            vec![
                Migration::new("20261027T000000Z_baseline", sql).checked_over(&template),
                Migration::new(
                    "20261104T000000Z_processed_at",
                    PROCESSED_AT.replace("{table}", self.table.as_str()),
                )
                .on(self.table.clone())
                .checked_over(PROCESSED_AT),
                Migration::new(
                    "20261107T000000Z_rejects",
                    REJECTS.replace("{table}", self.table.as_str()),
                )
                .checked_over(REJECTS),
                Migration::new(
                    "20261107T010000Z_indexes_before_launch",
                    INDEXES_BEFORE_LAUNCH.replace("{table}", self.table.as_str()),
                )
                .on(self.table.clone())
                .checked_over(INDEXES_BEFORE_LAUNCH),
                Migration::new(
                    "20261108T000000Z_unreadable_at",
                    UNREADABLE_AT.replace("{table}", self.table.as_str()),
                )
                .on(self.table.clone())
                .checked_over(UNREADABLE_AT),
                Migration::new(
                    "20261116T000000Z_causes",
                    CAUSES.replace("{table}", self.table.as_str()),
                )
                .on(self.table.clone())
                .checked_over(CAUSES),
            ],
        )
        .unwrap_or_else(|_| unreachable!("six migrations with ids, under a name"))
    }

    /// [`INBOX_TWIN`] for this inbox's sequence, slots and key.
    pub(super) fn inbox_twin(&self) -> String {
        INBOX_TWIN
            .replace("{sequence}", self.sequence.as_str())
            .replace("{slots}", &self.slots.to_string())
            .replace("{key}", self.partition.sql_expression())
    }

    /// Brings the inbox's tables up to its migrations — a read of the
    /// ledger when they are current, so that a process starts beside live
    /// traffic without locking them — and refuses to go on when the table
    /// is cut into another number of slots or by another key: that is a
    /// migration, not a restart; when the tables hold what the library did
    /// not make (ADR-0037 decision 5); or when a function the dispatch calls
    /// is not as the library makes it (ADR-0041 decision 6).
    pub async fn migrate(&self) -> Result<Applied, Error> {
        let applied = migrate(&self.pool, &self.component(), &MigrationOptions::default())
            .await
            .map_err(Error::Migration)?;
        let (table, slots) = (&self.table, self.slots);
        let key = self.partition.sql_expression();
        let twins = [self.inbox_twin(), REJECTS_TWIN.to_owned()];
        self.pool
            .session(async |session: P::Session| {
                let pinned = session
                    .connection()
                    .query_one(&format!("SELECT slots, partition_key FROM {table}_meta"), &[])
                    .await?;
                let (pinned_slots, pinned_key): (i32, String) = (pinned.get(0), pinned.get(1));
                if pinned_slots != slots as i32 || pinned_key != key {
                    return Err(Error::Malformed(format!(
                        "table `{table}` is cut into {pinned_slots} slots by `{pinned_key}`, this inbox \
                         asks for {slots} by `{key}`; both are fixed for the life of the table"
                    )));
                }
                refuse_a_page_the_bounds_do_not_fit(&session).await?;
                refuse_a_function_the_library_did_not_make(&session, table.as_str()).await?;
                // The twins live for one transaction, and are dropped at its end.
                session
                    .atomic(async |tx| {
                        refuse_what_the_intake_cannot_read(&tx, table.as_str(), &twins).await
                    })
                    .await
            })
            .await?;
        Ok(applied)
    }

    /// Drops the inbox's tables, its sequence and its functions and forgets
    /// them in the ledger: for an inbox that lives as long as a test. A
    /// function goes by its own statement: one written in PL/pgSQL depends
    /// on no table it reads, and outlives them.
    pub async fn teardown(&self) -> Result<(), Error> {
        let (table, sequence) = (&self.table, &self.sequence);
        let (take, walk) = (&TAKE_FUNCTION, &WALK_FUNCTION);
        self.pool
            .session(async |session: P::Session| {
                session
                    .connection()
                    .batch_execute(&format!(
                        "DROP TABLE IF EXISTS {table}; DROP SEQUENCE IF EXISTS {sequence}; \
                         DROP TABLE IF EXISTS {table}_meta; DROP TABLE IF EXISTS {table}_slots; \
                         DROP TABLE IF EXISTS {table}_rejects; \
                         DROP FUNCTION IF EXISTS {table}{}{}; DROP FUNCTION IF EXISTS {table}{}{};",
                        take.suffix, take.identity, walk.suffix, walk.identity
                    ))
                    .await?;
                Ok::<_, Error>(())
            })
            .await?;
        forget(&self.pool, &format!("inbox:{table}"))
            .await
            .map_err(Error::Migration)
    }

    /// Takes a slot: the one least recently served among those with a queue
    /// row and awake, locking its row in `<table>_slots` for the length of the
    /// transaction, `FOR UPDATE SKIP LOCKED`, so that a slot another
    /// dispatcher holds is passed by. A slot is awake while a round of walks
    /// goes on in it, when it never slept or its sleep ran out, and when a
    /// row arrived after its round began (ADR-0041 decision 2): the last is
    /// one descent of `__head_idx`. One look at the queue first, so that an
    /// idle inbox is one probe. Which head is due is the walk's matter, in
    /// [`Self::locate_head`], not this statement's.
    ///
    /// The head is located by a statement of its own: a statement's snapshot
    /// predates the lock it takes, and the previous holder of the slot may
    /// commit between the two. The walk read after the lock, under a fresh
    /// snapshot, sees what that holder committed.
    ///
    /// Called as `{table}__take()`, whose body is [`take_sql`]: a call of its
    /// own, so that the dispatch knows the slot it holds before it walks it,
    /// and a failure of the walk is the slot's (`Dispatched::slot`).
    pub(super) async fn take(&self, session: &P::Session) -> Result<Taken, Error> {
        let sql = format!(
            "SELECT * FROM {}{}{}",
            self.table, TAKE_FUNCTION.suffix, TAKE_FUNCTION.identity
        );
        let row = session.connection().query_one(&sql, &[]).await?;
        Ok(Taken {
            slot: row.get::<_, Option<i16>>(0).map(|slot| slot as u32),
            snapshot: snapshot_of(&row, 1)?,
            slots: row.get::<_, i32>(2) as u32,
        })
    }

    /// A walk of a slot the dispatcher holds (ADR-0041 decision 2), in one
    /// statement, read after the slot was taken so that what its previous
    /// holder committed is in view:
    /// - from the first Tenant after the slot's cursor, cyclically, wrapping
    ///   once and ending at the cursor's own, it examines at most
    ///   `Rotation::walk` Tenants — one descent of `__tenant_head_idx` each — and
    ///   stops at the first whose head, its oldest queue row in the slot, is
    ///   due; that row it locks;
    /// - the slot's row is written once: `served_at`, the cursor at the
    ///   Tenant served or the last one examined, and the round's state. A
    ///   round begins at the walk that finds no due head with none going on:
    ///   it keeps where it began, the queue's position then, and the earliest
    ///   backoff it sees; it ends at the walk that has seen every Tenant or
    ///   passed the place it began at, and the slot sleeps until that backoff
    ///   or `Rotation::max_dormancy`, whichever comes first. A head taken clears
    ///   the round.
    ///
    /// No row's value but its Tenant, its place and its backoff is read here
    /// (ADR-0037 decision 6): a value that cannot be read fails
    /// [`Self::read_at`], a statement of its own, for one known row. The
    /// cursor and the round compare `tenant_id` in the column's collation,
    /// the index's own; any order serves the rotation, and the host's
    /// databases order by bytes.
    ///
    /// Called as `{table}__walk(slot, walk, max_dormancy)`, whose body is
    /// [`walk_sql`]: in a function that is not stable, each statement takes
    /// a snapshot of its own, so the walk's is taken at the call, after the
    /// slot's lock, and `pg_current_snapshot()` in it is that one. The
    /// arguments are cast to the function's own types: untyped, they would
    /// resolve to a function of the same name taking text before this one.
    pub(super) async fn locate_head(
        &self,
        session: &P::Session,
        slot: u32,
    ) -> Result<Located, Error> {
        let sql = format!(
            "SELECT * FROM {}{}($1::smallint, $2::integer, $3::double precision)",
            self.table, WALK_FUNCTION.suffix
        );
        let rotation = self.rotation();
        let row = session
            .connection()
            .query_one(
                &sql,
                &[
                    &(slot as i16),
                    &(rotation.walk.max(1) as i32),
                    &rotation.max_dormancy.as_secs_f64(),
                ],
            )
            .await?;
        Ok(Located {
            row: row
                .get::<_, Option<i64>>(0)
                .map(|position| (position, row.get(1))),
            passed: row.get::<_, String>(2) == "passed",
            snapshot: snapshot_of(&row, 3)?,
        })
    }

    /// The row at `place`, read: the message, or why it cannot be one — a
    /// value the client cannot decode, or a stream id outside the bus's
    /// domain, which a row the intake did not write may hold — an older
    /// release's, or one written by hand — and which would reach the
    /// handler as another id (ADR-0037 decisions 1 and 6).
    /// A refusal of the server's is the caller's to classify.
    pub(super) async fn read_at(
        &self,
        session: &P::Session,
        place: &str,
    ) -> Result<Result<InboxMessage, &'static str>, Error> {
        let sql = format!(
            "SELECT {COLUMNS} FROM {table} WHERE ctid = $1::text::tid",
            table = self.table
        );
        let row = session.connection().query_one(&sql, &[&place]).await?;
        Ok(message_checked(&row, 0))
    }

    /// Parks the row at `place`, still in the queue and held by this
    /// transaction, at once, with `reason` as its last error, and returns
    /// its channel: a statement that binds none of the row's values, and
    /// returns only the channel, text the inbox named (ADR-0037 decision 6).
    pub(super) async fn park_at(
        &self,
        session: &P::Session,
        place: &str,
        reason: &'static str,
    ) -> Result<String, Error> {
        let sql = format!(
            "UPDATE {table} SET parked_at = CURRENT_TIMESTAMP, unreadable_at = CURRENT_TIMESTAMP, \
                 last_error = $2, held_since = NULL \
             WHERE ctid = $1::text::tid AND {QUEUE} \
             RETURNING {CHANNEL_OF_ROW}",
            table = self.table
        );
        let rows = session.connection().query(&sql, &[&place, &reason]).await?;
        match rows.as_slice() {
            [row] => Ok(row.try_get(0)?),
            _ => Err(Error::Malformed(format!(
                "the row to park was not in the queue: {} rows changed",
                rows.len()
            ))),
        }
    }

    /// The first of `dependencies` without a committed mark.
    pub(super) async fn first_unprocessed_dependency(
        &self,
        session: &P::Session,
        dependencies: &[CausalDependency],
    ) -> Result<Option<CausalDependency>, Error> {
        for dependency in dependencies {
            if !self.is_processed(session, dependency).await? {
                return Ok(Some(dependency.clone()));
            }
        }
        Ok(None)
    }

    /// Takes the lock that orders a wait for `identity` against its mark
    /// (ADR-0009): a transaction-level advisory lock, keyed by the table and
    /// the identity's canonical `jsonb` text, the one expression for every
    /// side. Held to the end of the transaction, so a transaction takes it
    /// last and takes nothing else after it: that is what rules cycles out.
    pub(super) async fn lock_identity(
        &self,
        session: &P::Session,
        identity: &CausalDependency,
    ) -> Result<(), Error> {
        session
            .connection()
            .execute(
                "SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2::jsonb::text))",
                &[&self.table.as_str(), &identity_json(identity)],
            )
            .await?;
        Ok(())
    }

    /// Sets the row aside to wait for `dependency` unless that dependency
    /// has a committed mark by now — one statement, under the lock of
    /// [`Self::lock_identity`], so the check and the wait are one step as in
    /// the model. Returns whether the row was set aside.
    pub(super) async fn set_waiting_unless_processed(
        &self,
        session: &P::Session,
        place: &str,
        dependency: &CausalDependency,
    ) -> Result<bool, Error> {
        let sql = format!(
            "UPDATE {table} SET waiting_for = $2::jsonb, waiting_since = CURRENT_TIMESTAMP, held_since = NULL \
             WHERE ctid = $1::text::tid AND {QUEUE} \
               AND NOT EXISTS (SELECT 1 FROM {table} d \
                               WHERE d.tenant_id = $3 AND d.stream_type = $4 AND d.stream_id = $5 \
                                 AND d.stream_position = $6 AND d.processed_position IS NOT NULL)",
            table = self.table
        );
        let changed = session
            .connection()
            .execute(
                &sql,
                &[
                    &place,
                    &identity_json(dependency),
                    &dependency.tenant_id,
                    &dependency.stream_type,
                    &dependency.stream_id,
                    &dependency.stream_position,
                ],
            )
            .await?;
        Ok(changed == 1)
    }

    /// Parks the rows that have waited longer than `max_wait`, naming the
    /// dependency in their `last_error`, and returns them, oldest first. A
    /// row parked so keeps the identity it waited for, so that the mark of
    /// that identity — the hole readmitted, or arrived late — puts it back
    /// in the queue (ADR-0036 R8); without the time of its wait, it does not
    /// expire again. A row whose dependency is present and holds for its
    /// stage's outage does not expire: the dependency did arrive, and
    /// the outage that holds it holds what waits for it (ADR-0040 decision
    /// 7). Its dependency is found by the primary key's stream, its identity
    /// compared whole, so that no value is cast.
    pub(super) async fn expire_waiting(
        &self,
        session: &P::Session,
        max_wait: Duration,
    ) -> Result<Vec<InboxMessage>, Error> {
        let sql = format!(
            "UPDATE {table} SET parked_at = CURRENT_TIMESTAMP, \
                last_error = 'dependency ' || waiting_for::text || ' never arrived', \
                waiting_since = NULL \
             WHERE ctid IN ( \
                SELECT w.ctid FROM {table} w \
                WHERE w.waiting_for IS NOT NULL \
                  AND w.waiting_since < CURRENT_TIMESTAMP - make_interval(secs => $1::double precision) \
                  AND NOT EXISTS ( \
                    SELECT 1 FROM {table} \
                    WHERE tenant_id = w.waiting_for->>'tenant_id' \
                      AND stream_type = w.waiting_for->>'stream_type' \
                      AND stream_id = w.waiting_for->'stream_id' \
                      AND {IDENTITY_OF_ROW} = w.waiting_for \
                      AND unavailable_since IS NOT NULL) \
                LIMIT $2 \
                FOR UPDATE OF w SKIP LOCKED) \
             RETURNING {NARROW}",
            table = self.table
        );
        let rows = session
            .connection()
            .query(&sql, &[&max_wait.as_secs_f64(), &EXPIRY_BATCH])
            .await?;
        let mut expired: Vec<InboxMessage> = rows.iter().map(|row| narrow_of(row, 0)).collect();
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

    /// Takes the lock of [`Self::lock_identity`] for the row at `place`, its
    /// identity read by the server from the row's own columns: a row read
    /// back otherwise than it is stored would lock another key than its
    /// waiters take (ADR-0037 decision 6).
    pub(super) async fn lock_own_identity(
        &self,
        session: &P::Session,
        place: &str,
    ) -> Result<(), Error> {
        let sql = format!(
            "SELECT pg_advisory_xact_lock(hashtext($1), hashtext({IDENTITY_OF_ROW}::text)) \
             FROM {table} WHERE ctid = $2::text::tid",
            table = self.table
        );
        session
            .connection()
            .execute(&sql, &[&self.table.as_str(), &place])
            .await?;
        Ok(())
    }

    /// Marks the row at `place` processed and, in the same statement, puts
    /// back into the queue every row that waited for it, its identity read
    /// by the server from the row's own columns (ADR-0037 decision 6).
    /// Returns the order of processing and the rows woken, oldest first.
    pub(super) async fn mark_processed(
        &self,
        session: &P::Session,
        place: &str,
    ) -> Result<(i64, Vec<InboxMessage>), Error> {
        let sql = format!(
            "WITH marked AS ( \
                UPDATE {table} SET processed_position = nextval('{sequence}'), \
                    processed_at = statement_timestamp(), held_since = NULL, \
                    last_cause = NULL, parked_cause = NULL, unavailable_since = NULL \
                WHERE ctid = $1::text::tid \
                RETURNING processed_position, {IDENTITY_OF_ROW} AS identity \
             ), woken AS ( \
                UPDATE {table} SET waiting_for = NULL, waiting_since = NULL, parked_at = NULL \
                WHERE waiting_for = (SELECT identity FROM marked) \
                RETURNING {NARROW} \
             ) \
             SELECT marked.processed_position, woken.* FROM marked LEFT JOIN woken ON true",
            table = self.table,
            sequence = self.sequence
        );
        let rows = session.connection().query(&sql, &[&place]).await?;
        let Some(first) = rows.first() else {
            return Err(Error::Malformed("the row to mark is gone".to_owned()));
        };
        Ok((first.get(0), woken_of(&rows, 1)))
    }

    /// Records a failure under its cause (ADR-0040). A failure unavailable
    /// for its stage's outage holds the row's slot, its attempts untouched,
    /// until its budget runs out and the channel's own evidence refutes the
    /// outage; any other spends an attempt and parks the row when it was the
    /// last, or when the verdict is permanent. `attempts` is the row's count
    /// before this failure.
    pub(super) async fn record_failure(
        &self,
        session: &P::Session,
        place: &str,
        failure: &Failure,
        attempts: u32,
    ) -> Result<Recorded, Error> {
        let error = crate::intake::storable_text(&failure.to_string(), FAILURE_WIDTH);
        let cause = failure.cause().as_str();
        let outage = self.retries.outage;
        let table = &self.table;
        // Past its budget a hold is weighed against the channel's evidence:
        // a row processed within the last `cap` says the stage serves
        // (ADR-0040 decision 4), read off the sweep's index.
        let weigh_outage = format!(
            "WITH held AS ( \
                SELECT COALESCE(CURRENT_TIMESTAMP - unavailable_since \
                                    >= make_interval(secs => $2::double precision), false) AS past, \
                       {CHANNEL_OF_ROW} AS channel \
                FROM {table} WHERE ctid = $1::text::tid \
             ) \
             SELECT held.past, held.past AND EXISTS ( \
                SELECT 1 FROM {table} \
                WHERE {CHANNEL_OF_ROW} = held.channel AND processed_at IS NOT NULL \
                  AND processed_at > CURRENT_TIMESTAMP - make_interval(secs => $3::double precision)) \
             FROM held"
        );
        // A hold spends no attempt: it is asked again at the teller's
        // `retry_after`, else after as long as it has held, between the floor
        // and the cap; its clock starts at its first hold. The outage holds
        // the head now, not its own failure: `held_since`, the clock of a
        // hold by the head's own failure (ADR-0041 decision 5), ends, so that
        // `held_counts` never counts an outage's hold as a back-off.
        let hold_failure = format!(
            "UPDATE {table} SET last_error = $2, last_cause = $3::text, held_since = NULL, \
                 unavailable_since = COALESCE(unavailable_since, CURRENT_TIMESTAMP), \
                 next_attempt_at = CURRENT_TIMESTAMP + make_interval(secs => LEAST($6::double precision, \
                     GREATEST($5::double precision, COALESCE($4::double precision, \
                         EXTRACT(EPOCH FROM CURRENT_TIMESTAMP \
                                 - COALESCE(unavailable_since, CURRENT_TIMESTAMP))::double precision)))) \
             WHERE ctid = $1::text::tid \
             RETURNING attempts, EXTRACT(EPOCH FROM next_attempt_at - CURRENT_TIMESTAMP)::double precision"
        );
        // Any other failure spends an attempt; the last one, or a permanent
        // verdict, parks the row under its cause. A hold refuted keeps its
        // clock, so that it is weighed again at once, until it parks; every
        // other verdict ends the hold. A parked row holds nothing.
        let spend_failure = format!(
            "UPDATE {table} SET attempts = LEAST(attempts, 2147483646) + 1, last_error = $2, \
                last_cause = $6::text, \
                next_attempt_at = CURRENT_TIMESTAMP + make_interval(secs => $3::double precision), \
                parked_at = CASE WHEN $5::boolean \
                                   OR ($4::integer > 0 AND LEAST(attempts, 2147483646) + 1 >= $4::integer) \
                                 THEN CURRENT_TIMESTAMP END, \
                parked_cause = CASE WHEN $5::boolean \
                                   OR ($4::integer > 0 AND LEAST(attempts, 2147483646) + 1 >= $4::integer) \
                                 THEN $6::text END, \
                unavailable_since = CASE WHEN $7::boolean AND NOT \
                                   ($4::integer > 0 AND LEAST(attempts, 2147483646) + 1 >= $4::integer) \
                                 THEN unavailable_since END, \
                held_since = CASE WHEN $5::boolean \
                                    OR ($4::integer > 0 AND LEAST(attempts, 2147483646) + 1 >= $4::integer) \
                                  THEN NULL ELSE coalesce(held_since, statement_timestamp()) END \
             WHERE ctid = $1::text::tid \
             RETURNING attempts, parked_at IS NOT NULL"
        );
        let (past_budget, refuted) = if failure.holds() {
            let weighed = session
                .connection()
                .query_one(
                    &weigh_outage,
                    &[
                        &place,
                        &outage.budget.as_secs_f64(),
                        &outage.cap.as_secs_f64(),
                    ],
                )
                .await?;
            (weighed.get::<_, bool>(0), weighed.get::<_, bool>(1))
        } else {
            (false, false)
        };
        if failure.holds() && !refuted {
            let told = match failure {
                Failure::Unavailable { retry_after, .. } => retry_after.map(|at| at.as_secs_f64()),
                _ => None,
            };
            let floor = (self.retries.backoff)(1).max(Outage::FLOOR);
            let row = session
                .connection()
                .query_one(
                    &hold_failure,
                    &[
                        &place,
                        &error,
                        &cause,
                        &told,
                        &floor.as_secs_f64(),
                        &outage.cap.max(floor).as_secs_f64(),
                    ],
                )
                .await?;
            return Ok(Recorded {
                attempts: row.get::<_, i32>(0) as u32,
                parked: false,
                held: true,
                past_budget,
                retry_after: Duration::try_from_secs_f64(row.get::<_, f64>(1))
                    .unwrap_or(Duration::ZERO),
            });
        }
        let backoff = (self.retries.backoff)(attempts.saturating_add(1));
        let retry_after = match failure {
            Failure::Unavailable {
                retry_after: Some(told),
                ..
            } => backoff.max(*told),
            _ => backoff,
        };
        let row = session
            .connection()
            .query_one(
                &spend_failure,
                &[
                    &place,
                    &error,
                    &retry_after.as_secs_f64(),
                    &(self.retries.max_attempts as i32),
                    &failure.is_permanent(),
                    &cause,
                    &refuted,
                ],
            )
            .await?;
        Ok(Recorded {
            attempts: row.get::<_, i32>(0) as u32,
            parked: row.get(1),
            held: false,
            past_budget,
            retry_after,
        })
    }

    /// A page of the parked rows, after `after`'s order of arrival, at most
    /// `limit`, oldest first: each by its place and, when it reads, its
    /// identity — never its values, so that one row that does not decode
    /// leaves the page whole (ADR-0037 decision 6).
    pub(super) async fn parked_rows(
        &self,
        session: &P::Session,
        after: Option<i64>,
        limit: u32,
    ) -> Result<Vec<Parked>, Error> {
        let sql = format!(
            "SELECT received_position, slot, attempts, last_error, uri, \
                    tenant_id, stream_type, stream_id, stream_position, {GROUND_OF_ROW} \
             FROM {} WHERE parked_at IS NOT NULL AND received_position > $1 \
             ORDER BY received_position LIMIT $2",
            self.table
        );
        let rows = session
            .connection()
            .query(&sql, &[&after.unwrap_or(i64::MIN), &i64::from(limit)])
            .await?;
        Ok(rows
            .iter()
            .map(|row| Parked {
                received_position: row.get(0),
                slot: u32::try_from(row.get::<_, i16>(1)).unwrap_or_default(),
                attempts: u32::try_from(row.get::<_, i32>(2)).unwrap_or_default(),
                last_error: row.get(3),
                uri: row.get(4),
                ground: Ground::parse(row.get(9)).unwrap_or(Ground::Failure),
                identity: row
                    .try_get::<_, serde_json::Value>(7)
                    .ok()
                    .filter(in_domain)
                    .map(|stream_id| CausalDependency {
                        tenant_id: row.get(5),
                        stream_type: row.get(6),
                        stream_id,
                        stream_position: row.get(8),
                    }),
            })
            .collect())
    }

    /// Gives the parked row at `received_position` another go, the identity
    /// it waited for cleared; the row, narrow, when it was parked.
    pub(super) async fn unpark_at(
        &self,
        session: &P::Session,
        received_position: i64,
    ) -> Result<Option<InboxMessage>, Error> {
        let sql = format!(
            "UPDATE {} SET parked_at = NULL, unreadable_at = NULL, attempts = 0, \
                 next_attempt_at = NULL, waiting_for = NULL, waiting_since = NULL, held_since = NULL, \
                 last_cause = NULL, parked_cause = NULL, unavailable_since = NULL \
             WHERE received_position = $1 AND parked_at IS NOT NULL \
             RETURNING {NARROW}",
            self.table
        );
        let row = session
            .connection()
            .query_opt(&sql, &[&received_position])
            .await?;
        Ok(row.map(|row| narrow_of(&row, 0)))
    }

    /// Takes the lock a mark of the parked row at `received_position` takes,
    /// its identity read by the server from the row's own columns.
    pub(super) async fn lock_parked_identity(
        &self,
        session: &P::Session,
        received_position: i64,
    ) -> Result<(), Error> {
        let sql = format!(
            "SELECT pg_advisory_xact_lock(hashtext($1), hashtext({IDENTITY_OF_ROW}::text)) \
             FROM {table} WHERE received_position = $2 AND parked_at IS NOT NULL",
            table = self.table
        );
        session
            .connection()
            .execute(&sql, &[&self.table.as_str(), &received_position])
            .await?;
        Ok(())
    }

    /// Marks the parked row at `received_position` processed by hand and
    /// wakes what waited for it, in one statement, its identity read by the
    /// server from its own columns; `None` when the row was not parked.
    pub(super) async fn resolve_at(
        &self,
        session: &P::Session,
        received_position: i64,
    ) -> Result<Option<(i64, InboxMessage, Vec<InboxMessage>)>, Error> {
        let sql = format!(
            "WITH resolved AS ( \
                UPDATE {table} SET parked_at = NULL, unreadable_at = NULL, \
                    processed_position = nextval('{sequence}'), \
                    processed_at = statement_timestamp(), waiting_for = NULL, waiting_since = NULL, \
                    held_since = NULL, last_cause = NULL, parked_cause = NULL, unavailable_since = NULL \
                WHERE received_position = $1 AND parked_at IS NOT NULL \
                RETURNING processed_position, {IDENTITY_OF_ROW} AS identity, {NARROW} \
             ), woken AS ( \
                UPDATE {table} SET waiting_for = NULL, waiting_since = NULL, parked_at = NULL \
                WHERE waiting_for = (SELECT identity FROM resolved) \
                RETURNING {NARROW} \
             ) \
             SELECT r.processed_position, r.tenant_id, r.stream_type, r.stream_id, \
                    r.stream_position, r.uri, r.received_position, r.waiting_for, woken.* \
             FROM resolved r LEFT JOIN woken ON true",
            table = self.table,
            sequence = self.sequence
        );
        let rows = session
            .connection()
            .query(&sql, &[&received_position])
            .await?;
        Ok(rows
            .first()
            .map(|first| (first.get(0), narrow_of(first, 1), woken_of(&rows, 8))))
    }
}

/// What the insert of a row came to, before the intake tells the observer
/// and sets aside what it must (ADR-0036 R2).
pub(super) enum Admitted {
    /// Stored, where it landed.
    Stored(Receipt),
    /// Held by itself: its identity, its `message_id` and its payload.
    Redelivered,
    /// Held under its identity by a message with another `message_id`, or
    /// none: a republication or one fact in two forms (ADR-0030).
    Shadowed(Option<String>),
    /// Held under its identity with its own `message_id` and another
    /// payload: a re-emission (R1).
    Differs,
    /// Its `message_id` is held at another identity of its Tenant and stream
    /// type: the holder's channel and received position (R5).
    IdentityTaken {
        channel: String,
        received_position: i64,
    },
    /// PostgreSQL refused a value of its own, in the library's words.
    Unstorable(&'static str),
}

/// What nothing stored comes to, on positive evidence alone (ADR-0036 R2):
/// the holder of the identity — its `message_id`, and whether its payload
/// is the message's — or else the holder of the `message_id` in the
/// message's Tenant and stream type. Neither means the holder went before
/// it could be read, and the message is tried again: never acknowledged as
/// stored when it is stored nowhere.
pub(super) fn verdict(
    holder: Option<(Option<String>, bool)>,
    own: Option<&str>,
    taken: Option<(String, i64)>,
) -> Result<Admitted, Error> {
    match (holder, taken) {
        (Some((held_by, _)), _) if held_by.as_deref() != own => Ok(Admitted::Shadowed(held_by)),
        (Some((_, true)), _) => Ok(Admitted::Redelivered),
        (Some((_, false)), _) => Ok(Admitted::Differs),
        (None, Some((channel, received_position))) => Ok(Admitted::IdentityTaken {
            channel,
            received_position,
        }),
        (None, None) => Err(Error::Conflict(
            "the holder of the conflict was gone before it could be read".to_owned(),
        )),
    }
}

/// A message set aside, as its row is written (ADR-0036 R4).
pub(super) struct Aside<'a> {
    pub(super) digest: &'a str,
    pub(super) channel: &'a str,
    pub(super) tenant_id: Option<&'a str>,
    pub(super) stream_type: Option<&'a str>,
    pub(super) identity: Option<&'a str>,
    pub(super) reason: &'static str,
    pub(super) detail: &'static str,
    pub(super) holder_channel: Option<&'a str>,
    pub(super) holder_position: Option<i64>,
    pub(super) message: &'a [u8],
}

impl<P, O> PgInbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: InboxObserver,
{
    /// Inserts the row in a transaction of its own, under no conflict
    /// target: every unique index is an arbiter, so that a `message_id` held
    /// elsewhere, or a duplicate arriving at once, is no error (ADR-0036
    /// R2). Where nothing was stored the cause is read, and acknowledged only
    /// on positive evidence: the holder of its identity, by a second
    /// statement that, under `READ COMMITTED`, sees a holder that committed
    /// while the insert waited on it; else the holder of its `message_id` in
    /// its Tenant and stream type, through the index's own expression. With
    /// neither, the holder went before it could be read, and the message is
    /// tried again. A value of its own that PostgreSQL refuses is
    /// `Unstorable`: the failed transaction is gone, and the intake writes
    /// the row set aside in another.
    pub(super) async fn admit(&self, message: &InboxMessage) -> Result<Admitted, Error> {
        let table = self.table.as_str();
        let insert = format!(
            "INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, payload, metadata) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT DO NOTHING \
             RETURNING pg_current_xact_id()::text, received_position, slot"
        );
        let holder = format!(
            "SELECT metadata->>'message_id', payload = $5, unreadable_at IS NOT NULL, received_position \
             FROM {table} \
             WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3 AND stream_position = $4"
        );
        // A row parked at once for its own values, superseded by this
        // arrival's (ADR-0037 decision 6): back in the queue, readable, and
        // arrived now, so that its wait is the new arrival's (ADR-0041
        // decision 5).
        let supersede = format!(
            "UPDATE {table} SET payload = $5, metadata = $6, parked_at = NULL, unreadable_at = NULL, \
                 last_error = NULL, attempts = 0, next_attempt_at = NULL, \
                 waiting_for = NULL, waiting_since = NULL, received_at = statement_timestamp(), \
                 held_since = NULL, last_cause = NULL, parked_cause = NULL, unavailable_since = NULL \
             WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3 AND stream_position = $4 \
               AND unreadable_at IS NOT NULL AND processed_position IS NULL \
             RETURNING pg_current_xact_id()::text, received_position, slot"
        );
        let taken = format!(
            "SELECT uri, received_position FROM {table} \
             WHERE tenant_id = $1 AND stream_type = $2 \
               AND (metadata->>'message_id')::uuid = $3::text::uuid"
        );
        let admitted = self
            .pool
            .session(async |session| {
                session
                    .atomic(async |tx| {
                        let row = tx
                            .connection()
                            .query_opt(
                                &insert,
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
                        if let Some(row) = row {
                            return Ok(Admitted::Stored(Receipt {
                                transaction_id: transaction_id(row.get::<_, String>(0))?,
                                received_position: row.get::<_, i64>(1),
                                slot: row.get::<_, i16>(2) as u32,
                            }));
                        }
                        let found = tx
                            .connection()
                            .query_opt(
                                &holder,
                                &[
                                    &message.tenant_id,
                                    &message.stream_type,
                                    &message.stream_id,
                                    &message.stream_position,
                                    &message.payload,
                                ],
                            )
                            .await?;
                        let unreadable = found
                            .as_ref()
                            .filter(|found| found.get::<_, bool>(2))
                            .map(|found| found.get::<_, i64>(3));
                        let holder = found.map(|found| {
                            (found.get::<_, Option<String>>(0), found.get::<_, bool>(1))
                        });
                        if let Some(holder_position) = unreadable {
                            let held_elsewhere = match message.message_id() {
                                Some(id) => tx
                                    .connection()
                                    .query_opt(
                                        &taken,
                                        &[&message.tenant_id, &message.stream_type, &id],
                                    )
                                    .await?
                                    .map(|row| {
                                        (
                                            ascetic_ddd_bus::uri::without_key(
                                                row.get::<_, &str>(0),
                                            )
                                            .to_owned(),
                                            row.get::<_, i64>(1),
                                        )
                                    })
                                    .filter(|(_, at)| *at != holder_position),
                                None => None,
                            };
                            if let Some((channel, received_position)) = held_elsewhere {
                                return Ok(Admitted::IdentityTaken {
                                    channel,
                                    received_position,
                                });
                            }
                            if let Some(row) = tx
                                .connection()
                                .query_opt(
                                    &supersede,
                                    &[
                                        &message.tenant_id,
                                        &message.stream_type,
                                        &message.stream_id,
                                        &message.stream_position,
                                        &message.payload,
                                        &message.metadata,
                                    ],
                                )
                                .await?
                            {
                                return Ok(Admitted::Stored(Receipt {
                                    transaction_id: transaction_id(row.get::<_, String>(0))?,
                                    received_position: row.get::<_, i64>(1),
                                    slot: row.get::<_, i16>(2) as u32,
                                }));
                            }
                        }
                        let taken = match (&holder, message.message_id()) {
                            (None, Some(id)) => tx
                                .connection()
                                .query_opt(&taken, &[&message.tenant_id, &message.stream_type, &id])
                                .await?
                                .map(|row| {
                                    (
                                        ascetic_ddd_bus::uri::without_key(row.get::<_, &str>(0))
                                            .to_owned(),
                                        row.get::<_, i64>(1),
                                    )
                                }),
                            _ => None,
                        };
                        verdict(holder, message.message_id(), taken)
                    })
                    .await
            })
            .await;
        match admitted {
            Err(Error::Database(error)) => {
                let own = error.as_db_error().and_then(|db| {
                    crate::intake::unstorable(db.code().code(), db.constraint(), table)
                });
                match own {
                    Some(why) => Ok(Admitted::Unstorable(why)),
                    None => Err(Error::Database(error)),
                }
            }
            other => other,
        }
    }

    /// Writes a row set aside, in a transaction of its own: a repeat of the
    /// same bytes, the same digest, counts once more, saturating, and an open
    /// row takes the verdict this release gives the bytes — its reason,
    /// detail and holder — keeping its first sight; a disposed row stays as
    /// it was (ADR-0037 decision 7). Whether it was a repeat.
    pub(super) async fn set_aside(&self, aside: &Aside<'_>) -> Result<bool, Error> {
        let table = self.table.as_str();
        let sql = format!(
            "INSERT INTO {table}_rejects (digest, channel, tenant_id, stream_type, identity, reason, \
                 detail, holder_channel, holder_position, message) \
             VALUES ($1::text::uuid, $2, $3, $4, $5::text::uuid, $6, $7, $8, $9, $10) \
             ON CONFLICT (digest) DO UPDATE SET \
                 seen = LEAST({table}_rejects.seen, 2147483646) + 1, \
                 last_seen_at = statement_timestamp(), \
                 reason = CASE WHEN {table}_rejects.disposed_at IS NULL \
                               THEN EXCLUDED.reason ELSE {table}_rejects.reason END, \
                 detail = CASE WHEN {table}_rejects.disposed_at IS NULL \
                               THEN EXCLUDED.detail ELSE {table}_rejects.detail END, \
                 holder_channel = CASE WHEN {table}_rejects.disposed_at IS NULL \
                               THEN EXCLUDED.holder_channel ELSE {table}_rejects.holder_channel END, \
                 holder_position = CASE WHEN {table}_rejects.disposed_at IS NULL \
                               THEN EXCLUDED.holder_position ELSE {table}_rejects.holder_position END \
             RETURNING seen > 1"
        );
        self.pool
            .session(async |session| {
                session
                    .atomic(async |tx| {
                        let row = tx
                            .connection()
                            .query_one(
                                &sql,
                                &[
                                    &aside.digest,
                                    &aside.channel,
                                    &aside.tenant_id,
                                    &aside.stream_type,
                                    &aside.identity,
                                    &aside.reason,
                                    &aside.detail,
                                    &aside.holder_channel,
                                    &aside.holder_position,
                                    &aside.message,
                                ],
                            )
                            .await?;
                        Ok::<_, Error>(row.get::<_, bool>(0))
                    })
                    .await
            })
            .await
    }
}

impl<P, O> Inbox for PgInbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: InboxObserver,
{
    /// Takes the message through the intake, as a wire message would come
    /// (ADR-0036): stored, found already stored, or set aside beside the
    /// inbox. An error only when the environment fails.
    async fn publish(&self, message: &InboxMessage) -> Result<(), Error> {
        self.intake(&message.uri, &crate::channel::wire_of(message))
            .await
            .map(|_| ())
    }
}

/// Refuses a function the dispatch calls that is not as the library makes
/// it (ADR-0041 decision 6): missing; not a function, or of other argument
/// types, modes or names, or another result, by the catalogue's values; of
/// another language, body, volatility or setting; run with its owner's
/// rights; not executable by
/// this role; in another schema than its table; or beside another function
/// of its name in its schema or visible, which a call may resolve to — an
/// overload with a default makes the call ambiguous (42725) and would stop
/// the loops at their first dispatch; one a call cannot reach is refused as
/// well, the check counting rather than resolving. Each of these the server
/// would run, or refuse, without a word at the start: a body replaced by
/// `CREATE OR REPLACE` runs another statement, a `STABLE` one refuses the
/// walk's lock at every dispatch, a dropped setting brings the planning at
/// every call back. A procedure of the same name has no result, and is
/// refused by its signature. Other functions named after the table are not
/// the dispatch's, and are let be, as a newer release's would be.
async fn refuse_a_function_the_library_did_not_make<S: PgAccess>(
    session: &S,
    table: &str,
) -> Result<(), Error> {
    for (called, body) in [
        (&TAKE_FUNCTION, take_body(table)),
        (&WALK_FUNCTION, walk_body(table)),
    ] {
        let name = format!("{table}{}{}", called.suffix, called.identity);
        let row = session
            .connection()
            .query_opt(
                "SELECT p.prokind = 'f' AND p.prorettype = 'record'::regtype \
                        AND p.proallargtypes IS NOT DISTINCT FROM $3::text::regtype[]::oid[] \
                        AND p.proargmodes IS NOT DISTINCT FROM $4::text::\"char\"[] \
                        AND p.proargnames IS NOT DISTINCT FROM $5::text::text[], \
                        coalesce(pg_get_function_arguments(p.oid), ''), \
                        l.lanname::text, p.prosrc, p.provolatile::text, p.prosecdef, \
                        array_to_string(p.proconfig, ','), has_function_privilege(p.oid, 'EXECUTE'), \
                        p.pronamespace IS NOT DISTINCT FROM \
                            (SELECT relnamespace FROM pg_class WHERE oid = to_regclass($2)), \
                        (SELECT string_agg(o.oid::regprocedure::text, ', ') FROM pg_proc o \
                            WHERE o.proname = p.proname AND o.oid <> p.oid \
                              AND (o.pronamespace = p.pronamespace OR pg_function_is_visible(o.oid))) \
                 FROM pg_proc p JOIN pg_language l ON l.oid = p.prolang \
                 WHERE p.oid = to_regprocedure($1)",
                &[&name, &table, &called.types, &called.modes, &called.names],
            )
            .await?;
        let Some(row) = row else {
            return Err(Error::Malformed(format!(
                "function `{name}` is missing: the dispatch calls it"
            )));
        };
        let setting = row.get::<_, Option<String>>(6);
        let others = row.get::<_, Option<String>>(9);
        let wrong: Vec<String> = [
            (
                !row.get::<_, bool>(0),
                format!("its signature ({})", row.get::<_, String>(1)),
            ),
            (
                row.get::<_, String>(2) != "plpgsql",
                "its language".to_owned(),
            ),
            (row.get::<_, String>(3) != body, "its body".to_owned()),
            (row.get::<_, String>(4) != "v", "its volatility".to_owned()),
            (
                row.get::<_, bool>(5),
                "its owner's rights (SECURITY DEFINER)".to_owned(),
            ),
            (
                setting.as_deref() != called.setting,
                format!(
                    "its settings ({}, the library's {})",
                    setting.as_deref().unwrap_or("none"),
                    called.setting.unwrap_or("none")
                ),
            ),
            (
                !row.get::<_, bool>(7),
                "no EXECUTE for this role".to_owned(),
            ),
            (
                !row.get::<_, bool>(8),
                "its schema, not its table's".to_owned(),
            ),
            (
                others.is_some(),
                format!(
                    "another function of its name ({})",
                    others.as_deref().unwrap_or_default()
                ),
            ),
        ]
        .into_iter()
        .filter_map(|(differs, what)| differs.then_some(what))
        .collect();
        if !wrong.is_empty() {
            return Err(Error::Malformed(format!(
                "function `{name}` is not as the library makes it: {}",
                wrong.join(", ")
            )));
        }
    }
    Ok(())
}

/// Refuses a server whose pages are not 8 KiB: the bound on an identity's
/// stored form (`MAX_IDENTITY_BYTES`) is a btree item's on such a page
/// (ADR-0037 decision 4).
async fn refuse_a_page_the_bounds_do_not_fit<S: PgAccess>(session: &S) -> Result<(), Error> {
    let block_size: String = session
        .connection()
        .query_one("SHOW block_size", &[])
        .await?
        .get(0);
    if block_size == "8192" {
        Ok(())
    } else {
        Err(Error::Malformed(format!(
            "the server's pages are {block_size} bytes; the inbox's bound on an identity rests on 8192"
        )))
    }
}

/// The inbox table's columns as this release's migrations make them, in the
/// order they add them, declared once more as a temporary twin. `migrate`
/// has the server render the twin's columns beside the table's, in one
/// session, and compares the two (ADR-0037 decision 5). A default's or a
/// generation's text is the server's rendering: it depends on the major, on
/// the session's settings (`quote_all_identifiers`) and, for `slot`, on the
/// key and the columns' types; the twin is rendered under all of them alike.
/// Measured identical to the tables the library made, on 14.24 and 18.6, for
/// every key the tests cut by. The order is the table's: `slot` names its
/// key's columns by their place. `STORED` is written out: 18 makes a
/// generated column virtual without it. `{sequence}`, `{key}` and `{slots}`
/// are the table's own.
const INBOX_TWIN: &str = r#"
    CREATE TEMP TABLE ascetic_inbox_twin (
        tenant_id varchar(128) NOT NULL,
        stream_type varchar(128) NOT NULL,
        stream_id jsonb NOT NULL,
        stream_position integer NOT NULL,
        uri varchar(255) NOT NULL,
        payload bytea NOT NULL,
        metadata jsonb NULL,
        received_position bigint NOT NULL DEFAULT nextval('{sequence}'),
        processed_position bigint NULL,
        attempts integer NOT NULL DEFAULT 0,
        last_error text NULL,
        next_attempt_at timestamptz NULL,
        parked_at timestamptz NULL,
        waiting_for jsonb NULL,
        waiting_since timestamptz NULL,
        slot smallint GENERATED ALWAYS AS ((hashtext({key}) & 2147483647) % {slots}) STORED,
        received_at timestamptz NOT NULL DEFAULT statement_timestamp(),
        held_since timestamptz NULL,
        processed_at timestamptz NULL,
        unreadable_at timestamptz NULL,
        last_cause varchar(64) NULL,
        parked_cause varchar(64) NULL,
        unavailable_since timestamptz NULL
    ) ON COMMIT DROP;
"#;

/// The rows set aside's columns, as [`INBOX_TWIN`] has the inbox table's;
/// `digest` NOT NULL as its primary key makes it.
const REJECTS_TWIN: &str = r#"
    CREATE TEMP TABLE ascetic_rejects_twin (
        digest uuid NOT NULL,
        channel text NOT NULL,
        tenant_id text NULL,
        stream_type text NULL,
        identity uuid NULL,
        reason text NOT NULL,
        detail text NOT NULL,
        holder_channel text NULL,
        holder_position bigint NULL,
        message bytea NULL,
        seen integer NOT NULL DEFAULT 1,
        first_seen_at timestamptz NOT NULL DEFAULT statement_timestamp(),
        last_seen_at timestamptz NOT NULL DEFAULT statement_timestamp(),
        disposed_at timestamptz NULL,
        disposition text NULL,
        disposed_by text NULL,
        disposed_because text NULL
    ) ON COMMIT DROP;
"#;

/// A column of a table and of its twin, paired by name: on which side it
/// is, and what binds an insert on each — NOT NULL, identity, generation, and
/// the default's or the generation's text.
struct Paired {
    name: String,
    live: Option<Bound>,
    twin: Option<Bound>,
}

#[derive(PartialEq)]
struct Bound {
    not_null: bool,
    identity: String,
    generated: String,
    expression: Option<String>,
}

impl Bound {
    fn binds(&self) -> bool {
        self.not_null
            || !self.identity.is_empty()
            || !self.generated.is_empty()
            || self.expression.is_some()
    }
}

/// Refuses on `relation` what the library did not make and that would
/// change what the intake's statements can raise (ADR-0037 decision 5): an
/// index — a conflict on a unique one leaves a message stored nowhere, an
/// expression raises for its value —, a constraint, a trigger, a rule, a
/// policy or row security; and, column by column against `twin`, what binds
/// an insert otherwise than the library makes it. The library names its own
/// objects `<relation>__…` beside its primary key, so that an older release
/// starts beside a newer one's (R12); a release that adds a column adds it
/// nullable, without a default.
///
/// The columns: one the twin lacks may be nullable, without a default, a
/// generation or an identity, as a newer release's is; one the twin has
/// must be there, NOT NULL no more than the twin's — a NOT NULL the library
/// does not make raises 23502 for an insert that leaves the column out —, and
/// filled as the twin's: the same identity, generation and text. A NOT NULL
/// is judged with its column, never by a constraint's name: 18 records each
/// in `pg_constraint` (`contype` `n`), named by the server, truncated or kept
/// through a rename; `attnotnull` is the column's on every major, 18 setting
/// it for a NOT VALID one too (measured on 18.6). Both sides are rendered
/// against the twin, which names a column by its place as the table does:
/// so no lock is taken on the live table (ADR-0029).
pub(super) async fn refuse_what_the_library_did_not_make<S: PgAccess>(
    session: &S,
    relation: &str,
    primary_key: &str,
    twin: &str,
) -> Result<(), Error> {
    let connection = session.connection();
    let own = |name: &String| name == primary_key || name.starts_with(&format!("{relation}__"));
    let names = async |sql: &str| -> Result<Vec<String>, Error> {
        Ok(connection
            .query(sql, &[&relation])
            .await?
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect())
    };
    let kinds = [
        (
            "indexes",
            "SELECT i.relname::text FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid \
             WHERE x.indrelid = $1::text::regclass",
        ),
        (
            "constraints",
            "SELECT conname::text FROM pg_constraint \
             WHERE conrelid = $1::text::regclass AND contype <> 'n'",
        ),
        (
            "triggers",
            "SELECT tgname::text FROM pg_trigger \
             WHERE tgrelid = $1::text::regclass AND NOT tgisinternal",
        ),
        (
            "rules",
            "SELECT rulename::text FROM pg_rewrite WHERE ev_class = $1::text::regclass",
        ),
        (
            "policies",
            "SELECT polname::text FROM pg_policy WHERE polrelid = $1::text::regclass",
        ),
    ];
    for (kind, sql) in kinds {
        let foreign: Vec<String> = names(sql)
            .await?
            .into_iter()
            .filter(|name| !own(name))
            .collect();
        if !foreign.is_empty() {
            return Err(Error::Malformed(format!(
                "table `{relation}` carries {kind} the inbox does not own ({}): they would change \
                 what its statements can raise",
                foreign.join(", ")
            )));
        }
    }
    let secured: bool = connection
        .query_one(
            "SELECT relrowsecurity FROM pg_class WHERE oid = $1::text::regclass",
            &[&relation],
        )
        .await?
        .get(0);
    if secured {
        return Err(Error::Malformed(format!(
            "table `{relation}` has row security, which the inbox does not own: it would refuse \
             a row for a session's sake"
        )));
    }
    let side = "SELECT a.attname::text AS name, a.attnotnull AS not_null, \
                       a.attidentity::text AS identity, a.attgenerated::text AS generated, \
                       pg_get_expr(d.adbin, $2::text::regclass) AS expression \
                FROM pg_attribute a \
                LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
                WHERE a.attrelid = {of}::text::regclass AND a.attnum > 0 AND NOT a.attisdropped";
    let paired: Vec<Paired> = connection
        .query(
            &format!(
                "WITH live AS ({}), twin AS ({}) \
                 SELECT coalesce(l.name, t.name), l.name IS NOT NULL, l.not_null, l.identity, \
                        l.generated, l.expression, t.name IS NOT NULL, t.not_null, t.identity, \
                        t.generated, t.expression \
                 FROM live l FULL JOIN twin t ON t.name = l.name ORDER BY 1",
                side.replace("{of}", "$1"),
                side.replace("{of}", "$2"),
            ),
            &[&relation, &twin],
        )
        .await?
        .iter()
        .map(|row| {
            let bound = |at: usize| {
                row.get::<_, bool>(at).then(|| Bound {
                    not_null: row.get(at + 1),
                    identity: row.get(at + 2),
                    generated: row.get(at + 3),
                    expression: row.get(at + 4),
                })
            };
            Paired {
                name: row.get(0),
                live: bound(1),
                twin: bound(6),
            }
        })
        .collect();
    let refuse = |what: &str, why: &str, columns: Vec<String>| -> Result<(), Error> {
        if columns.is_empty() {
            return Ok(());
        }
        Err(Error::Malformed(format!(
            "table `{relation}` carries {what} ({}): {why}",
            columns.join(", ")
        )))
    };
    let named = |pick: &dyn Fn(&Bound, &Bound) -> bool| -> Vec<String> {
        paired
            .iter()
            .filter_map(|column| match (&column.live, &column.twin) {
                (Some(live), Some(twin)) if pick(live, twin) => Some(column.name.clone()),
                _ => None,
            })
            .collect()
    };
    refuse(
        "columns the inbox does not own that bind an insert",
        "not null, defaulted, generated or an identity",
        paired
            .iter()
            .filter(|column| {
                column.twin.is_none() && column.live.as_ref().is_some_and(Bound::binds)
            })
            .map(|column| column.name.clone())
            .collect(),
    )?;
    refuse(
        "none of the columns the inbox makes",
        "its statements name them",
        paired
            .iter()
            .filter(|column| column.live.is_none())
            .map(|column| column.name.clone())
            .collect(),
    )?;
    refuse(
        "columns the inbox makes nullable made NOT NULL",
        "an insert that leaves them out would raise",
        named(&|live, twin| live.not_null && !twin.not_null),
    )?;
    refuse(
        "identity columns the inbox does not make",
        "an insert that names them would raise",
        named(&|live, twin| live.identity != twin.identity),
    )?;
    refuse(
        "columns generated otherwise than the inbox makes them",
        "a row would land in another slot, or none",
        named(&|live, twin| {
            (!live.generated.is_empty() || !twin.generated.is_empty())
                && (live.generated != twin.generated || live.expression != twin.expression)
        }),
    )?;
    refuse(
        "defaults the inbox does not make, on",
        "an insert would fill them otherwise, or raise",
        named(&|live, twin| {
            live.generated.is_empty()
                && twin.generated.is_empty()
                && live.expression != twin.expression
        }),
    )
}

/// Refuses a table the intake cannot read the conflicts of (ADR-0036 R2):
/// what the library did not make on its tables (ADR-0037 decision 5), a
/// unique index of a host's among them, whose conflict would leave a
/// message stored nowhere and acknowledged; a deferrable constraint on
/// either table, which a conflict cannot arbitrate (55000 on every insert);
/// and a server encoding other than UTF8, under which 22P05 is the
/// environment's, not the message's. `twins` are [`INBOX_TWIN`] and
/// [`REJECTS_TWIN`] for this table, made here in the caller's transaction
/// and dropped at its end: so a start needs TEMPORARY on the database, and
/// runs no longer on a read-only one.
async fn refuse_what_the_intake_cannot_read<S: PgAccess>(
    session: &S,
    table: &str,
    twins: &[String; 2],
) -> Result<(), Error> {
    let connection = session.connection();
    let encoding: String = connection
        .query_one("SELECT current_setting('server_encoding')", &[])
        .await?
        .get(0);
    if encoding != "UTF8" {
        return Err(Error::Malformed(format!(
            "the server encoding is {encoding}: the inbox reads PostgreSQL's refusals as UTF8's"
        )));
    }
    for twin in twins {
        connection.batch_execute(twin).await?;
    }
    let rejects = format!("{table}_rejects");
    refuse_what_the_library_did_not_make(
        session,
        table,
        &format!("{table}_pk"),
        "pg_temp.ascetic_inbox_twin",
    )
    .await?;
    refuse_what_the_library_did_not_make(
        session,
        &rejects,
        &format!("{table}_rejects_pkey"),
        "pg_temp.ascetic_rejects_twin",
    )
    .await?;
    for relation in [table, rejects.as_str()] {
        let deferrable: Vec<String> = connection
            .query(
                "SELECT conname::text FROM pg_constraint \
                 WHERE conrelid = $1::text::regclass AND condeferrable",
                &[&relation],
            )
            .await?
            .iter()
            .map(|row| row.get(0))
            .collect();
        if !deferrable.is_empty() {
            return Err(Error::Malformed(format!(
                "table `{relation}` carries deferrable constraints ({}): a conflict cannot \
                 arbitrate them",
                deferrable.join(", ")
            )));
        }
    }
    Ok(())
}

/// `xid8` has no driver type; it travels as decimal text and is unsigned.
fn transaction_id(text: String) -> Result<u64, Error> {
    text.parse()
        .map_err(|_| Error::Malformed(format!("not a transaction id: `{text}`")))
}

fn snapshot_of(row: &Row, at: usize) -> Result<Snapshot, Error> {
    row.get::<_, String>(at)
        .parse()
        .map_err(|error: crate::snapshot::MalformedSnapshot| Error::Malformed(error.to_string()))
}

/// A row's identity as the `jsonb` a waiting row names it by.
fn identity_json(dependency: &CausalDependency) -> serde_json::Value {
    serde_json::to_value(dependency).unwrap_or(serde_json::Value::Null)
}

/// The message in `COLUMNS`, read from column `at` on, or why it cannot be
/// one: a value the client cannot decode, or a stream id outside the bus's
/// domain, which a row the intake did not write may hold (ADR-0037
/// decisions 1 and 6).
fn message_checked(row: &Row, at: usize) -> Result<InboxMessage, &'static str> {
    fn get<'a, T: tokio_postgres::types::FromSql<'a>>(
        row: &'a Row,
        at: usize,
    ) -> Result<T, &'static str> {
        row.try_get(at)
            .map_err(|_| "a value of the row does not decode")
    }
    let message = InboxMessage {
        tenant_id: get(row, at)?,
        stream_type: get(row, at + 1)?,
        stream_id: get(row, at + 2)?,
        stream_position: get(row, at + 3)?,
        uri: get(row, at + 4)?,
        payload: get(row, at + 5)?,
        metadata: get(row, at + 6)?,
        received_position: get(row, at + 7)?,
        processed_position: get(row, at + 8)?,
        attempts: u32::try_from(get::<i32>(row, at + 9)?).unwrap_or_default(),
        last_error: get(row, at + 10)?,
        waiting_for: get::<Option<serde_json::Value>>(row, at + 11)?
            .and_then(|value| serde_json::from_value(value).ok()),
    };
    if !in_domain(&message.stream_id) {
        return Err("the row's stream id is outside the bus's domain");
    }
    Ok(message)
}

/// Whether `stream_id`, read back, is in the bus's domain: it writes and
/// reads back as itself.
fn in_domain(stream_id: &serde_json::Value) -> bool {
    let text = ascetic_ddd_bus::identity::write_stream_id(stream_id);
    ascetic_ddd_bus::identity::read_stream_id(&text).as_ref() == Ok(stream_id)
}

/// The row in `NARROW`, read from column `at` on, never failing: a value that
/// does not decode reads as its type's empty one, the stream id as null. Its
/// payload and metadata are not read.
fn narrow_of(row: &Row, at: usize) -> InboxMessage {
    InboxMessage {
        tenant_id: row.try_get(at).unwrap_or_default(),
        stream_type: row.try_get(at + 1).unwrap_or_default(),
        stream_id: row.try_get(at + 2).unwrap_or(serde_json::Value::Null),
        stream_position: row.try_get(at + 3).unwrap_or_default(),
        uri: row.try_get(at + 4).unwrap_or_default(),
        payload: Vec::new(),
        metadata: None,
        received_position: row.try_get(at + 5).ok().flatten(),
        processed_position: None,
        attempts: 0,
        last_error: None,
        waiting_for: row
            .try_get::<_, Option<serde_json::Value>>(at + 6)
            .ok()
            .flatten()
            .and_then(|value| serde_json::from_value(value).ok()),
    }
}

/// The woken rows of a mark or a resolve, narrow: the rows of the statement
/// whose row columns, from `at` on, are not the outer join's nulls; oldest
/// first.
fn woken_of(rows: &[Row], at: usize) -> Vec<InboxMessage> {
    let mut woken: Vec<InboxMessage> = rows
        .iter()
        .filter(|row| {
            row.try_get::<_, Option<i64>>(at + 5)
                .ok()
                .flatten()
                .is_some()
        })
        .map(|row| narrow_of(row, at))
        .collect();
    woken.sort_by_key(|message| message.received_position);
    woken
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The channel a statement reads is the one the parked index and the
    /// sweep's index are built over, spelled as they spell it: another
    /// spelling would group or label otherwise, and use neither index.
    #[test]
    fn a_row_s_channel_is_the_indexes_expression() {
        assert!(INDEXES_BEFORE_LAUNCH.contains(&format!("({CHANNEL_OF_ROW}, received_position)")));
        assert!(PROCESSED_AT.contains(&format!("({CHANNEL_OF_ROW}, processed_at)")));
    }

    #[test]
    fn nothing_stored_is_acknowledged_only_on_positive_evidence() {
        let own = Some("m-1");
        let held = |id: &str, same| Some((Some(id.to_owned()), same));
        assert!(matches!(
            verdict(held("m-1", true), own, None),
            Ok(Admitted::Redelivered)
        ));
        assert!(matches!(
            verdict(held("m-1", false), own, None),
            Ok(Admitted::Differs)
        ));
        assert!(matches!(
            verdict(held("m-2", true), own, None),
            Ok(Admitted::Shadowed(Some(_)))
        ));
        assert!(matches!(
            verdict(None, own, Some(("inbox://orders".to_owned(), 7))),
            Ok(Admitted::IdentityTaken {
                received_position: 7,
                ..
            })
        ));
        assert!(
            matches!(verdict(None, own, None), Err(Error::Conflict(_))),
            "a holder gone before it was read is tried again"
        );
        assert!(matches!(verdict(None, None, None), Err(Error::Conflict(_))));
    }
}
