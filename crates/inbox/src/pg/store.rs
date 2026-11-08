//! The tables and the statements: what the inbox does to PostgreSQL. The
//! loop that drives them is in `dispatch`.

use std::time::Duration;

use ascetic_ddd_migration::{
    Applied, Component, Migration, Options as MigrationOptions, forget, migrate,
};
use ascetic_ddd_session::{PgAccess, Session, SessionPool};
use tokio_postgres::Row;

use super::PgInbox;
use crate::error::{Error, Failure};
use crate::message::{CausalDependency, InboxMessage};
use crate::observer::{InboxObserver, Receipt};
use crate::port::Inbox;
use crate::snapshot::Snapshot;

/// The columns of a row, in the order `message_of` reads them.
const COLUMNS: &str = "tenant_id, stream_type, stream_id, stream_position, uri, payload, metadata, \
                       received_position, processed_position, attempts, last_error, waiting_for";
/// The most characters of a failure's text a row keeps (ADR-0037 decision
/// 7): `last_error` is stored unsealed, beside the row.
const FAILURE_WIDTH: usize = 4096;
/// A row's identity as the server writes it from the row's own columns:
/// the form a dependency on it is waited for under.
const IDENTITY_OF_ROW: &str = "jsonb_build_object('tenant_id', tenant_id, 'stream_type', stream_type, \
                               'stream_id', stream_id, 'stream_position', stream_position)";
/// The rows a dispatcher may take: neither processed, parked nor waiting.
const QUEUE: &str = "processed_position IS NULL AND parked_at IS NULL AND waiting_for IS NULL";

/// The inbox's sequence, tables and indexes as created, `{table}`,
/// `{sequence}`, `{slots}` and `{key}` standing for their names, the cut and
/// the partition key's expression, `{key_text}` for that expression as a
/// literal, and `{queue}` for [`QUEUE`]: the baseline of its component, its
/// checksum the template's with the queue written out.
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
        CONSTRAINT {table}_pk PRIMARY KEY (tenant_id, stream_type, stream_id, stream_position)
    );
    CREATE INDEX IF NOT EXISTS {table}__head_idx
        ON {table} (slot, received_position) WHERE {queue};
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
        served_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    INSERT INTO {table}_slots (slot) SELECT s FROM generate_series(0, {slots} - 1) AS s
        ON CONFLICT DO NOTHING;
"#;

/// What taking a slot returned: the slot whose row the statement locked, or
/// none when no slot had a due head; the statement's snapshot, read in the
/// same statement because under `READ COMMITTED` every statement takes a
/// snapshot of its own; and how many slots the table is cut into.
pub(super) struct Taken {
    pub(super) slot: Option<u32>,
    pub(super) snapshot: Snapshot,
    pub(super) slots: u32,
}

/// What one look at the head of a held slot returned: the row, with whether
/// it still waits for its backoff, or no row; and the statement's snapshot.
pub(super) struct Located {
    /// The head's order of arrival, its place in the table (`ctid`), and
    /// whether it still waits for its backoff; none when the slot's queue
    /// is empty.
    pub(super) row: Option<(i64, String, bool)>,
    pub(super) snapshot: Snapshot,
}

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
        let sql = BASELINE
            .replace("{queue}", QUEUE)
            .replace("{table}", self.table.as_str())
            .replace("{sequence}", self.sequence.as_str())
            .replace("{slots}", &self.slots.to_string())
            .replace("{key_text}", &format!("'{}'", key.replace('\'', "''")))
            .replace("{key}", key);
        Component::new(
            format!("inbox:{}", self.table),
            vec![
                Migration::new("20261027T000000Z_baseline", sql)
                    .checked_over(&BASELINE.replace("{queue}", QUEUE)),
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
            ],
        )
        .unwrap_or_else(|_| unreachable!("four migrations with ids, under a name"))
    }

    /// Brings the inbox's tables up to its migrations — a read of the
    /// ledger when they are current, so that a process starts beside live
    /// traffic without locking them — and refuses to go on when the table
    /// is cut into another number of slots or by another key: that is a
    /// migration, not a restart.
    pub async fn migrate(&self) -> Result<Applied, Error> {
        let applied = migrate(&self.pool, &self.component(), &MigrationOptions::default())
            .await
            .map_err(Error::Migration)?;
        let (table, slots) = (&self.table, self.slots);
        let key = self.partition.sql_expression();
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
                refuse_what_the_intake_cannot_read(&session, table.as_str()).await
            })
            .await?;
        Ok(applied)
    }

    /// Drops the inbox's tables and its sequence and forgets them in the
    /// ledger: for an inbox that lives as long as a test.
    pub async fn teardown(&self) -> Result<(), Error> {
        let (table, sequence) = (&self.table, &self.sequence);
        self.pool
            .session(async |session: P::Session| {
                session
                    .connection()
                    .batch_execute(&format!(
                        "DROP TABLE IF EXISTS {table}; DROP SEQUENCE IF EXISTS {sequence}; \
                         DROP TABLE IF EXISTS {table}_meta; DROP TABLE IF EXISTS {table}_slots; \
                         DROP TABLE IF EXISTS {table}_rejects;"
                    ))
                    .await?;
                Ok::<_, Error>(())
            })
            .await?;
        forget(&self.pool, &format!("inbox:{table}"))
            .await
            .map_err(Error::Migration)
    }

    /// Takes a slot: the one least recently served among those whose head —
    /// the oldest row of the queue in the slot — is due, locking its row in
    /// `<table>_slots` for the length of the transaction, `FOR UPDATE SKIP
    /// LOCKED`, so that a slot another dispatcher holds is passed by. A slot
    /// whose head waits for its backoff is not taken: the head holds the
    /// slot, and the others go on.
    ///
    /// The head itself is located by [`Self::locate_head`], a statement of its own:
    /// a statement's snapshot predates the lock it takes, and the previous
    /// holder of the slot may commit between the two — a head it processed
    /// then fails the re-check `FOR UPDATE` makes on the latest version, and
    /// the slot comes back without a head, or with the wrong one. The head
    /// read after the lock, under a fresh snapshot, is the head.
    pub(super) async fn take(&self, session: &P::Session) -> Result<Taken, Error> {
        let sql = format!(
            r#"
            WITH taken AS (
                SELECT s.slot FROM {table}_slots s
                WHERE (SELECT r.next_attempt_at IS NULL OR r.next_attempt_at <= CURRENT_TIMESTAMP
                       FROM {table} r
                       WHERE r.slot = s.slot AND {QUEUE}
                       ORDER BY r.received_position LIMIT 1) IS TRUE
                ORDER BY s.served_at
                LIMIT 1
                FOR UPDATE OF s SKIP LOCKED
            ), touched AS (
                UPDATE {table}_slots SET served_at = CURRENT_TIMESTAMP WHERE slot IN (SELECT slot FROM taken)
            )
            SELECT t.slot, pg_current_snapshot()::text, (SELECT slots FROM {table}_meta)
            FROM (SELECT 1) AS one
            LEFT JOIN taken t ON true
            "#,
            table = self.table,
        );
        let row = session.connection().query_one(&sql, &[]).await?;
        Ok(Taken {
            slot: row.get::<_, Option<i16>>(0).map(|slot| slot as u32),
            snapshot: snapshot_of(&row, 1)?,
            slots: row.get::<_, i32>(2) as u32,
        })
    }

    /// The head of a slot the dispatcher holds, located: the oldest row of
    /// its queue, locked, by its order of arrival and its place, with
    /// whether it still waits for its backoff, and the statement's
    /// snapshot; no row when the slot's queue is empty. Read after the slot
    /// was taken, so that what its previous holder committed is in view.
    /// None of the row's values is read here (ADR-0037 decision 6): a value
    /// that cannot be read fails [`Self::read_at`], a statement of its own,
    /// for one known row.
    pub(super) async fn locate_head(
        &self,
        session: &P::Session,
        slot: u32,
    ) -> Result<Located, Error> {
        let sql = format!(
            r#"
            WITH r AS (
                SELECT received_position, ctid::text AS place,
                       next_attempt_at IS NOT NULL AND next_attempt_at > CURRENT_TIMESTAMP AS deferred
                FROM {table}
                WHERE slot = $1 AND {QUEUE}
                ORDER BY received_position ASC
                LIMIT 1
                FOR UPDATE
            )
            SELECT r.received_position, r.place, r.deferred, pg_current_snapshot()::text AS snapshot
            FROM (SELECT 1) AS one
            LEFT JOIN r ON true
            "#,
            table = self.table,
        );
        let row = session
            .connection()
            .query_one(&sql, &[&(slot as i16)])
            .await?;
        Ok(Located {
            row: row
                .get::<_, Option<i64>>(0)
                .map(|position| (position, row.get(1), row.get(2))),
            snapshot: snapshot_of(&row, 3)?,
        })
    }

    /// The row at `place`, read: the message, or why it cannot be one — a
    /// value the client cannot decode, or a stream id outside the bus's
    /// domain, which a row stored before the domain may hold and which
    /// would reach the handler as another id (ADR-0037 decisions 1 and 6).
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
    /// transaction, at once, with `reason` as its last error: a statement
    /// that neither reads nor binds the row's values (ADR-0037 decision 6).
    pub(super) async fn park_at(
        &self,
        session: &P::Session,
        place: &str,
        reason: &'static str,
    ) -> Result<(), Error> {
        let sql = format!(
            "UPDATE {table} SET parked_at = CURRENT_TIMESTAMP, last_error = $2 \
             WHERE ctid = $1::text::tid AND {QUEUE}",
            table = self.table
        );
        let changed = session
            .connection()
            .execute(&sql, &[&place, &reason])
            .await?;
        if changed == 1 {
            Ok(())
        } else {
            Err(Error::Malformed(format!(
                "the row to park was not in the queue: {changed} rows changed"
            )))
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
            "UPDATE {table} SET waiting_for = $2::jsonb, waiting_since = CURRENT_TIMESTAMP \
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
    /// expire again.
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
                    processed_at = statement_timestamp() \
                WHERE ctid = $1::text::tid \
                RETURNING processed_position, {IDENTITY_OF_ROW} AS identity \
             ), woken AS ( \
                UPDATE {table} SET waiting_for = NULL, waiting_since = NULL, parked_at = NULL \
                WHERE waiting_for = (SELECT identity FROM marked) \
                RETURNING {COLUMNS} \
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

    /// Records a failed attempt: one more, the error, when the row is due
    /// again, and whether that was the last attempt — or the failure was
    /// permanent. Returns the attempts so far and whether the row is now
    /// parked.
    pub(super) async fn record_failure(
        &self,
        session: &P::Session,
        place: &str,
        failure: &Failure,
        retry_after: Duration,
    ) -> Result<(u32, bool), Error> {
        let sql = format!(
            "UPDATE {} SET attempts = attempts + 1, last_error = $2, \
                next_attempt_at = CURRENT_TIMESTAMP + make_interval(secs => $3::double precision), \
                parked_at = CASE WHEN $5::boolean OR ($4::integer > 0 AND attempts + 1 >= $4::integer) \
                                 THEN CURRENT_TIMESTAMP END \
             WHERE ctid = $1::text::tid \
             RETURNING attempts, parked_at IS NOT NULL",
            self.table
        );
        let row = session
            .connection()
            .query_one(
                &sql,
                &[
                    &place,
                    &crate::intake::storable_text(&failure.to_string(), FAILURE_WIDTH),
                    &retry_after.as_secs_f64(),
                    &(self.retries.max_attempts as i32),
                    &failure.is_permanent(),
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
            "UPDATE {} SET parked_at = NULL, attempts = 0, next_attempt_at = NULL, \
                 waiting_for = NULL, waiting_since = NULL \
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
                UPDATE {table} SET parked_at = NULL, processed_position = nextval('{sequence}'), \
                    processed_at = statement_timestamp(), waiting_for = NULL, waiting_since = NULL \
                WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3 AND stream_position = $4 \
                  AND parked_at IS NOT NULL \
                RETURNING processed_position \
             ), woken AS ( \
                UPDATE {table} SET waiting_for = NULL, waiting_since = NULL, parked_at = NULL \
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
            "SELECT metadata->>'message_id', payload = $5 FROM {table} \
             WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3 AND stream_position = $4"
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
                        let holder = found.map(|found| {
                            (found.get::<_, Option<String>>(0), found.get::<_, bool>(1))
                        });
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
    /// same bytes, the same digest, counts once more and keeps the row as it
    /// is, disposed of or not. Whether it was a repeat.
    pub(super) async fn set_aside(&self, aside: &Aside<'_>) -> Result<bool, Error> {
        let table = self.table.as_str();
        let sql = format!(
            "INSERT INTO {table}_rejects (digest, channel, tenant_id, stream_type, identity, reason, \
                 detail, holder_channel, holder_position, message) \
             VALUES ($1::text::uuid, $2, $3, $4, $5::text::uuid, $6, $7, $8, $9, $10) \
             ON CONFLICT (digest) DO UPDATE SET seen = {table}_rejects.seen + 1, \
                 last_seen_at = statement_timestamp() \
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

/// The inbox table's columns as this release makes them.
const INBOX_COLUMNS: [&str; 17] = [
    "tenant_id",
    "stream_type",
    "stream_id",
    "stream_position",
    "uri",
    "payload",
    "metadata",
    "received_position",
    "processed_position",
    "attempts",
    "last_error",
    "next_attempt_at",
    "parked_at",
    "waiting_for",
    "waiting_since",
    "slot",
    "processed_at",
];

/// The rows set aside's columns as this release makes them.
const REJECTS_COLUMNS: [&str; 17] = [
    "digest",
    "channel",
    "tenant_id",
    "stream_type",
    "identity",
    "reason",
    "detail",
    "holder_channel",
    "holder_position",
    "message",
    "seen",
    "first_seen_at",
    "last_seen_at",
    "disposed_at",
    "disposition",
    "disposed_by",
    "disposed_because",
];

/// Refuses on `relation` what the library did not make and that would
/// change what the intake's statements can raise (ADR-0037 decision 5): an
/// index — a conflict on a unique one leaves a message stored nowhere, an
/// expression raises for its value —, a constraint, a trigger, a rule, a
/// policy or row security, and a column a row cannot be inserted without.
/// The library names its own objects `<relation>__…` beside its primary
/// key, so that an older release starts beside a newer one's (R12); a
/// release that adds a column adds it nullable, without a default.
async fn refuse_what_the_library_did_not_make<S: PgAccess>(
    session: &S,
    relation: &str,
    primary_key: &str,
    columns: &[&str],
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
            "SELECT conname::text FROM pg_constraint WHERE conrelid = $1::text::regclass",
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
    let binding: Vec<String> = connection
        .query(
            "SELECT a.attname::text FROM pg_attribute a \
             LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
             WHERE a.attrelid = $1::text::regclass AND a.attnum > 0 AND NOT a.attisdropped \
               AND (a.attnotnull OR d.adbin IS NOT NULL OR a.attgenerated <> '')",
            &[&relation],
        )
        .await?
        .iter()
        .map(|row| row.get::<_, String>(0))
        .filter(|name| !columns.contains(&name.as_str()))
        .collect();
    if !binding.is_empty() {
        return Err(Error::Malformed(format!(
            "table `{relation}` carries columns the inbox does not own that bind an insert ({}): \
             not null, defaulted or generated",
            binding.join(", ")
        )));
    }
    Ok(())
}

/// Refuses a table the intake cannot read the conflicts of (ADR-0036 R2):
/// what the library did not make on its tables (ADR-0037 decision 5), a
/// unique index of a host's among them, whose conflict would leave a
/// message stored nowhere and acknowledged; a deferrable constraint, which
/// an untargeted conflict cannot arbitrate (55000 on every insert); and a
/// server encoding other than UTF8, under which 22P05 is the environment's,
/// not the message's.
async fn refuse_what_the_intake_cannot_read<S: PgAccess>(
    session: &S,
    table: &str,
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
    refuse_what_the_library_did_not_make(session, table, &format!("{table}_pk"), &INBOX_COLUMNS)
        .await?;
    refuse_what_the_library_did_not_make(
        session,
        &format!("{table}_rejects"),
        &format!("{table}_rejects_pkey"),
        &REJECTS_COLUMNS,
    )
    .await?;
    let deferrable: Vec<String> = connection
        .query(
            "SELECT conname::text FROM pg_constraint \
             WHERE conrelid = $1::text::regclass AND condeferrable",
            &[&table],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();
    if !deferrable.is_empty() {
        return Err(Error::Malformed(format!(
            "table `{table}` carries deferrable constraints ({}): an untargeted conflict cannot \
             arbitrate them",
            deferrable.join(", ")
        )));
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

/// The message in `COLUMNS`, read from column `at` on, or why it cannot be
/// one: a value the client cannot decode, or a stream id outside the bus's
/// domain, which a row stored before the domain may hold (ADR-0037
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
    let id = ascetic_ddd_bus::identity::write_stream_id(&message.stream_id);
    if ascetic_ddd_bus::identity::read_stream_id(&id).as_ref() != Ok(&message.stream_id) {
        return Err("the row's stream id is outside the bus's domain");
    }
    Ok(message)
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

#[cfg(test)]
mod tests {
    use super::*;

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
