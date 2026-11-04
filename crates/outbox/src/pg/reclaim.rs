//! The ring's reclamation (ADR-0033 decision 3): the generation of the
//! oldest row left is emptied whole, by `TRUNCATE`, once no row of it can
//! be written any more and every position has handed every row of it on.

use std::time::Duration;

use ascetic_ddd_session::{PgAccess, Session, SessionPool};
use tokio_postgres::error::SqlState;

use super::store::{transaction_id, widened};
use super::{PgOutbox, Rebased};
use crate::error::Error;
use crate::observer::{OutboxObserver, Reclamation, Skipped};

/// Where a pass stands once it chose its candidate: the generation and the
/// ids of its oldest and newest rows.
struct Candidate {
    generation: u32,
    oldest: u64,
    newest: u64,
}

impl<P, O> PgOutbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: OutboxObserver,
{
    /// One pass of the ring's reclamation, in a `READ COMMITTED`
    /// transaction of its own, reported to the observer. A dispatcher's
    /// [`run`](PgOutbox::run) works one between its batches once an
    /// interval; a host may call it too.
    ///
    /// Under a try-lock of the reclamation's key — another holder means the
    /// pass is skipped — it stops on a counter reset, recorded or found, as
    /// the relay does; then takes the generation of the oldest row left and
    /// checks, each in a statement of its own and without the partition's
    /// lock, so that a pass rarely queues for nothing:
    /// - past: its newest window lies the margin below the next id's, so no
    ///   id can be assigned in its windows any more;
    /// - closed: no transaction of this database holding an id in it is
    ///   open, sessions read before prepared transactions;
    /// - passed: every position's `delivered` sees every row of it;
    /// - old enough: a pass first found it passed the margin's age ago.
    ///
    /// Then it takes the partition's lock, under the configured timeout, reads
    /// "passed" again under it — this recheck is what keeps the rows
    /// (`OutboxPg.tla`) — empties it, and records both bounds. A lock not
    /// granted is a skipped pass, not an error.
    pub async fn reclaim(&self) -> Result<Reclamation, Error> {
        let outcome = self
            .pool
            .session(async |session: P::Session| {
                session
                    .atomic(async |tx: P::Session| self.pass(&tx).await)
                    .await
            })
            .await;
        if let Ok(reclamation) = &outcome {
            self.observer.on_reclamation(reclamation);
        }
        outcome
    }

    async fn pass(&self, tx: &P::Session) -> Result<Reclamation, Error> {
        let (outbox, offsets, ring) = (&self.outbox_table, &self.offsets_table, self.ring);
        let connection = tx.connection();
        connection
            .batch_execute(&format!(
                "SET LOCAL lock_timeout = '{}ms'",
                self.reclaiming.lock_timeout.as_millis()
            ))
            .await?;
        let held: bool = connection
            .query_one(
                &format!(
                    "SELECT pg_try_advisory_xact_lock({})",
                    self.reclamation_key()
                ),
                &[],
            )
            .await?
            .get(0);
        if !held {
            return Ok(Reclamation::Skipped(Skipped::Busy));
        }
        // a counter reset stops the reclamation as it stops the relay, found
        // here, not taken from a relay's report (ADR-0035 decision 12)
        let row = connection
            .query_one(
                &format!(
                    "SELECT (SELECT refused FROM {outbox}_meta), \
                            EXISTS (SELECT 1 FROM {offsets} \
                                    WHERE pg_snapshot_xmax(delivered) \
                                          > pg_snapshot_xmax(pg_current_snapshot()))"
                ),
                &[],
            )
            .await?;
        if let Some(reason) = row.get::<_, Option<String>>(0) {
            return Err(Error::CounterReset(reason));
        }
        if row.get::<_, bool>(1) {
            let reason = "a position delivered past the server's transaction ids: \
                          the database was moved by dump and restore"
                .to_owned();
            self.refuse(&reason).await?;
            return Err(Error::CounterReset(reason));
        }

        let Some(candidate) = self.candidate(tx).await? else {
            return Ok(Reclamation::Skipped(Skipped::Empty));
        };
        let generation = candidate.generation;
        let next: u64 = transaction_id(
            connection
                .query_one("SELECT pg_snapshot_xmax(pg_current_snapshot())::text", &[])
                .await?
                .get(0),
        )?;
        let (newest_window, next_window) = (candidate.newest / ring.window, next / ring.window);
        if next_window.saturating_sub(newest_window) < u64::from(self.reclaiming.margin) {
            return Ok(Reclamation::Skipped(Skipped::NotPast {
                generation,
                newest_window,
                next_window,
            }));
        }
        if let Some(transaction) = self
            .open_here(tx)
            .await?
            .into_iter()
            .find(|xid| ring.generation_of(*xid) == generation)
        {
            return Ok(Reclamation::Skipped(Skipped::NotClosed {
                generation,
                transaction,
            }));
        }
        if let Some(pinned) = self.pinned(tx, &candidate).await? {
            if next_window.saturating_sub(candidate.oldest / ring.window)
                >= u64::from(ring.generations)
            {
                log::warn!(
                    "outbox {outbox}: group `{}` pins the ring a lap behind, at generation \
                     {generation}",
                    match &pinned {
                        Skipped::NotPassed { group, .. } => group.as_str(),
                        _ => "",
                    }
                );
            }
            return Ok(Reclamation::Skipped(pinned));
        }
        let passed_for: f64 = connection
            .query_one(
                &format!(
                    "UPDATE {outbox}_meta \
                     SET passed_at[{at}] = coalesce(passed_at[{at}], CURRENT_TIMESTAMP) \
                     RETURNING extract(epoch FROM CURRENT_TIMESTAMP - passed_at[{at}])::float8",
                    at = generation + 1
                ),
                &[],
            )
            .await?
            .get(0);
        let passed_for = Duration::from_secs_f64(passed_for.max(0.0));
        if passed_for < self.reclaiming.margin_age {
            return Ok(Reclamation::Skipped(Skipped::NotOldEnough {
                generation,
                passed_for,
            }));
        }
        // the lock, the recheck and the emptying in a savepoint: a lock not
        // granted leaves the time recorded above
        let emptied = tx
            .atomic(async |inner: P::Session| self.empty(&inner, &candidate).await)
            .await;
        match emptied {
            Err(Error::Database(error)) if error.code() == Some(&SqlState::LOCK_NOT_AVAILABLE) => {
                Ok(Reclamation::Skipped(Skipped::Held { generation }))
            }
            other => other,
        }
    }

    /// The generation of the oldest row left, read on each generation's
    /// primary key, with its oldest and newest ids; none when the ring is
    /// empty.
    async fn candidate(&self, tx: &P::Session) -> Result<Option<Candidate>, Error> {
        let row = tx
            .connection()
            .query_one(&self.least_per_generation(), &[])
            .await?;
        let mut oldest: Option<(u32, u64)> = None;
        for generation in 0..self.ring.generations {
            if let Some(text) = row.get::<_, Option<String>>(generation as usize) {
                let xid = transaction_id(text)?;
                if oldest.is_none_or(|(_, least)| xid < least) {
                    oldest = Some((generation, xid));
                }
            }
        }
        let Some((generation, oldest)) = oldest else {
            return Ok(None);
        };
        let newest = self.newest_of(tx, generation).await?.unwrap_or(oldest);
        Ok(Some(Candidate {
            generation,
            oldest,
            newest,
        }))
    }

    /// The least id of every generation, each named as a constant so that
    /// its partition's primary key answers.
    fn least_per_generation(&self) -> String {
        let columns: Vec<String> = (0..self.ring.generations)
            .map(|generation| {
                format!(
                    "(SELECT transaction_id::text FROM {} WHERE \"generation\" = {generation} \
                      ORDER BY transaction_id LIMIT 1)",
                    self.outbox_table
                )
            })
            .collect();
        format!("SELECT {}", columns.join(", "))
    }

    /// The newest id of `generation`.
    async fn newest_of(&self, tx: &P::Session, generation: u32) -> Result<Option<u64>, Error> {
        tx.connection()
            .query_one(
                &format!(
                    "SELECT (SELECT transaction_id::text FROM {} WHERE \"generation\" = {generation} \
                             ORDER BY transaction_id DESC LIMIT 1)",
                    self.outbox_table
                ),
                &[],
            )
            .await?
            .get::<_, Option<String>>(0)
            .map(transaction_id)
            .transpose()
    }

    /// The ids of this database's open transactions, widened against the
    /// next: its sessions' first, then its prepared transactions', since a
    /// transaction being prepared is listed as prepared before it leaves
    /// its session. The statistics snapshot is cleared first, since
    /// `pg_stat_activity` is read once per transaction. A transaction is
    /// bound to its database: one left open elsewhere holds no ring back.
    async fn open_here(&self, tx: &P::Session) -> Result<Vec<u64>, Error> {
        let connection = tx.connection();
        connection
            .execute("SELECT pg_stat_clear_snapshot()", &[])
            .await?;
        let next =
            "WITH n AS (SELECT pg_snapshot_xmax(pg_current_snapshot())::text::bigint AS next)";
        let sessions = connection
            .query(
                &format!(
                    "{next} SELECT {}::text FROM pg_stat_activity a, n \
                     WHERE a.datname = current_database() AND a.backend_xid IS NOT NULL",
                    widened("a.backend_xid")
                ),
                &[],
            )
            .await?;
        let prepared = connection
            .query(
                &format!(
                    "{next} SELECT {}::text FROM pg_prepared_xacts p, n \
                     WHERE p.database = current_database()",
                    widened("p.transaction")
                ),
                &[],
            )
            .await?;
        sessions
            .iter()
            .chain(prepared.iter())
            .map(|row| transaction_id(row.get(0)))
            .collect()
    }

    /// A position whose `delivered` does not see every row of the
    /// candidate — its `xmax` not past the newest, or an id of its
    /// in-progress list with a row in it — as the reason it is kept; none
    /// when every position passed it. A row of an epoch in flight is not
    /// passed yet.
    async fn pinned(
        &self,
        tx: &P::Session,
        candidate: &Candidate,
    ) -> Result<Option<Skipped>, Error> {
        let row = tx
            .connection()
            .query_opt(
                &format!(
                    "SELECT o.consumer_group, o.uri, o.slot, \
                            extract(epoch FROM CURRENT_TIMESTAMP - o.delivered_at)::float8 \
                     FROM {offsets} o \
                     WHERE pg_snapshot_xmax(o.delivered) <= $1::text::xid8 \
                        OR EXISTS (SELECT 1 FROM pg_snapshot_xip(o.delivered) AS x \
                                   WHERE x <= $1::text::xid8 \
                                     AND EXISTS (SELECT 1 FROM {outbox} m \
                                                 WHERE m.\"generation\" = {generation} \
                                                   AND m.transaction_id = x)) \
                     LIMIT 1",
                    offsets = self.offsets_table,
                    outbox = self.outbox_table,
                    generation = candidate.generation,
                ),
                &[&candidate.newest.to_string()],
            )
            .await?;
        Ok(row.map(|row| Skipped::NotPassed {
            generation: candidate.generation,
            group: row.get(0),
            uri: row.get(1),
            slot: row.get::<_, i16>(2) as u32,
            since: row
                .get::<_, Option<f64>>(3)
                .map(|seconds| Duration::from_secs_f64(seconds.max(0.0))),
        }))
    }

    /// Under the partition's lock: "passed" read again, the partition
    /// emptied, and both bounds recorded — `gone_below` the least of the
    /// least id left, this database's oldest open id and the next id, set
    /// afresh; `intact_from` one past the highest id emptied, only ever
    /// raised.
    async fn empty(&self, tx: &P::Session, candidate: &Candidate) -> Result<Reclamation, Error> {
        let (outbox, ring, generation) = (&self.outbox_table, self.ring, candidate.generation);
        let connection = tx.connection();
        connection
            .batch_execute(&format!(
                "LOCK TABLE {outbox}_g{generation} IN ACCESS EXCLUSIVE MODE"
            ))
            .await?;
        // every writer of the generation has ended: its rows are final
        let newest = self
            .newest_of(tx, generation)
            .await?
            .unwrap_or(candidate.newest);
        let candidate = Candidate {
            newest,
            ..*candidate
        };
        if self.pinned(tx, &candidate).await?.is_some() {
            return Ok(Reclamation::Skipped(Skipped::PassedNoMore { generation }));
        }
        let rows: f32 = connection
            .query_one(
                &format!(
                    "SELECT reltuples FROM pg_class WHERE oid = '{outbox}_g{generation}'::regclass"
                ),
                &[],
            )
            .await?
            .get(0);
        connection
            .batch_execute(&format!("TRUNCATE {outbox}_g{generation}"))
            .await?;
        let mut gone_below = transaction_id(
            connection
                .query_one("SELECT pg_snapshot_xmax(pg_current_snapshot())::text", &[])
                .await?
                .get(0),
        )?;
        let least = connection
            .query_one(&self.least_per_generation(), &[])
            .await?;
        for column in 0..self.ring.generations as usize {
            if let Some(text) = least.get::<_, Option<String>>(column) {
                gone_below = gone_below.min(transaction_id(text)?);
            }
        }
        if let Some(open) = self.open_here(tx).await?.into_iter().min() {
            gone_below = gone_below.min(open);
        }
        let intact_from: String = connection
            .query_one(
                &format!(
                    "UPDATE {outbox}_meta SET \
                        gone_below = $1::text::xid8, \
                        intact_from = greatest(coalesce(intact_from, $2::text::xid8), $2::text::xid8), \
                        passed_at[{at}] = NULL \
                     RETURNING intact_from::text",
                    at = generation + 1
                ),
                &[&gone_below.to_string(), &(newest + 1).to_string()],
            )
            .await?
            .get(0);
        Ok(Reclamation::Emptied {
            generation,
            windows: (candidate.oldest / ring.window, newest / ring.window),
            rows: rows.max(0.0) as i64,
            gone_below,
            intact_from: transaction_id(intact_from)?,
        })
    }

    /// Drops an abandoned group's positions, so that it no longer pins the
    /// ring; a group that moved a position within the margin's age is
    /// refused, [`Error::GroupActive`]. Under the reclamation's key, held
    /// exclusively.
    pub async fn forget_group(&self, session: &P::Session, group: &str) -> Result<(), Error> {
        self.forget_positions(session, group, None).await
    }

    /// Drops the positions of one selection of a group, the others kept: a
    /// selection a relay no longer runs — the selection of everything,
    /// once its lanes have taken over and no older binary relays it
    /// (ADR-0033 decision 4) — would otherwise pin the ring for good. One
    /// that moved within the margin's age is refused,
    /// [`Error::GroupActive`]. Under the reclamation's key, held
    /// exclusively.
    pub async fn forget_selection(
        &self,
        session: &P::Session,
        selection: &super::Selection,
    ) -> Result<(), Error> {
        self.forget_positions(session, &selection.consumer_group, Some(&selection.uri))
            .await
    }

    async fn forget_positions(
        &self,
        session: &P::Session,
        group: &str,
        uri: Option<&str>,
    ) -> Result<(), Error> {
        let offsets = &self.offsets_table;
        let selected = "consumer_group = $1 AND ($2::text IS NULL OR uri = $2)";
        session
            .atomic(async |tx: P::Session| {
                self.hold_reclamation(&tx, true).await?;
                let moved: Option<f64> = tx
                    .connection()
                    .query_one(
                        &format!(
                            "SELECT extract(epoch FROM CURRENT_TIMESTAMP - max(updated_at))::float8 \
                             FROM {offsets} WHERE {selected}"
                        ),
                        &[&group, &uri],
                    )
                    .await?
                    .get(0);
                if let Some(moved) = moved.map(|seconds| Duration::from_secs_f64(seconds.max(0.0)))
                    && moved < self.reclaiming.margin_age
                {
                    return Err(Error::GroupActive {
                        group: group.to_owned(),
                        moved,
                    });
                }
                tx.connection()
                    .execute(
                        &format!("DELETE FROM {offsets} WHERE {selected}"),
                        &[&group, &uri],
                    )
                    .await?;
                Ok(())
            })
            .await
    }

    /// Rebases the outbox after a counter reset (ADR-0033 decision 3): the
    /// database was moved by dump and restore, its transaction ids restarted
    /// below the restored positions', and relay and reclamation stopped.
    /// None when no counter reset is recorded or found. In one transaction,
    /// under the reclamation's key:
    /// 1. the outbox locked, so that no publish lands while it works;
    /// 2. its snapshot `S` taken before it writes anything, so that its own id
    ///    is not visible in `S` — a transaction's snapshot leaves its own id
    ///    out of the in-progress list;
    /// 3. the rows no position handed on kept: those written in the new ids
    ///    since the restore, below the restored `gone_below`, and the
    ///    restored rows some `delivered` does not see. Once the new ids reach
    ///    the restored `gone_below`, the two cannot be told apart: refused;
    /// 4. of rows sharing an identity and a channel, the first kept: the
    ///    guard admits one per transaction, and the inbox drops the rest as
    ///    repeats;
    /// 5. they are written again under its own id, in their positions'
    ///    order, and everything else emptied, the current generation
    ///    included;
    /// 6. every `delivered` set to `S`, every epoch cleared, both bounds set
    ///    to the least of its own id and this database's oldest open id —
    ///    the bounds move back here, and only here — and the refusal cleared.
    pub async fn rebase(&self) -> Result<Option<Rebased>, Error> {
        let (outbox, offsets) = (&self.outbox_table, &self.offsets_table);
        let kept_rows = format!(
            "CREATE TEMP TABLE {outbox}_rebase ON COMMIT DROP AS \
             SELECT DISTINCT ON (k) uri, payload, metadata, created_at, \"position\" FROM ( \
                 SELECT m.uri, m.payload, m.metadata, m.created_at, m.\"position\", \
                        coalesce(split_part(m.uri, '/', 1) || '//' || split_part(m.uri, '/', 3) || ' ' \
                                 || ((m.metadata->'tenant_id') || (m.metadata->'stream_type') \
                                     || (m.metadata->'stream_id') || (m.metadata->'stream_position') \
                                     || jsonb_set('[null]', '{{0}}', coalesce(m.metadata->'part', '\"\"')))::text, \
                                 '#' || m.\"position\") AS k \
                 FROM {outbox} m \
                 WHERE m.transaction_id < $1::text::xid8 \
                    OR EXISTS (SELECT 1 FROM {offsets} o \
                               WHERE NOT pg_visible_in_snapshot(m.transaction_id, o.delivered)) \
             ) AS r \
             ORDER BY k, \"position\""
        );
        self.pool
            .session(async |session: P::Session| {
                session
                    .atomic(async |tx: P::Session| {
                        let connection = tx.connection();
                        connection
                            .execute(
                                &format!("SELECT pg_advisory_xact_lock({})", self.reclamation_key()),
                                &[],
                            )
                            .await?;
                        connection
                            .batch_execute(&format!("LOCK TABLE {outbox} IN EXCLUSIVE MODE"))
                            .await?;
                        // S, before anything is written
                        let row = connection
                            .query_one(
                                &format!(
                                    "SELECT pg_current_snapshot()::text, \
                                            (SELECT refused FROM {outbox}_meta), \
                                            (SELECT gone_below::text FROM {outbox}_meta), \
                                            EXISTS (SELECT 1 FROM {offsets} \
                                                    WHERE pg_snapshot_xmax(delivered) \
                                                          > pg_snapshot_xmax(pg_current_snapshot()))"
                                ),
                                &[],
                            )
                            .await?;
                        let s: String = row.get(0);
                        let (refused, restored, reset): (Option<String>, Option<String>, bool) =
                            (row.get(1), row.get(2), row.get(3));
                        if refused.is_none() && !reset {
                            return Ok(None);
                        }
                        let next = s
                            .split(':')
                            .nth(1)
                            .and_then(|xmax| xmax.parse::<u64>().ok())
                            .ok_or_else(|| Error::Malformed(format!("snapshot `{s}`")))?;
                        let restored = restored.map(transaction_id).transpose()?;
                        let Some(restored) = restored.filter(|restored| next < *restored) else {
                            return Err(Error::CounterReset(
                                "the server's transaction ids have reached the restored ring's: \
                                 rows written since the restore cannot be told from restored ones"
                                    .to_owned(),
                            ));
                        };
                        connection
                            .execute(&kept_rows, &[&restored.to_string()])
                            .await?;
                        let emptied: i64 = connection
                            .query_one(
                                &format!(
                                    "SELECT (SELECT count(*) FROM {outbox}) \
                                            - (SELECT count(*) FROM {outbox}_rebase)"
                                ),
                                &[],
                            )
                            .await?
                            .get(0);
                        connection
                            .batch_execute(&format!("TRUNCATE {outbox}"))
                            .await?;
                        let kept = connection
                            .execute(
                                &format!(
                                    "INSERT INTO {outbox} (uri, payload, metadata, created_at, transaction_id) \
                                     SELECT uri, payload, metadata, created_at, pg_current_xact_id() \
                                     FROM {outbox}_rebase ORDER BY \"position\""
                                ),
                                &[],
                            )
                            .await?;
                        connection
                            .execute(
                                &format!(
                                    "UPDATE {offsets} SET \
                                        delivered = $1::text::pg_snapshot, epoch = NULL, epoch_offset = 0, \
                                        epoch_end = NULL, epoch_generations = NULL, \
                                        delivered_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP, \
                                        last_processed_transaction_id = \
                                            (pg_snapshot_xmin($1::text::pg_snapshot)::text::bigint - 1)::text::xid8, \
                                        offset_acked = 9223372036854775807"
                                ),
                                &[&s],
                            )
                            .await?;
                        let own: u64 = transaction_id(
                            connection
                                .query_one("SELECT pg_current_xact_id()::text", &[])
                                .await?
                                .get(0),
                        )?;
                        let bound = self
                            .open_here(&tx)
                            .await?
                            .into_iter()
                            .min()
                            .map_or(own, |open| open.min(own));
                        connection
                            .execute(
                                &format!(
                                    "UPDATE {outbox}_meta SET gone_below = $1::text::xid8, \
                                        intact_from = $1::text::xid8, passed_at = NULL, refused = NULL"
                                ),
                                &[&bound.to_string()],
                            )
                            .await?;
                        log::info!(
                            "outbox {outbox}: rebased after a counter reset: {kept} rows kept, \
                             {emptied} emptied, the ring's bounds at {bound}"
                        );
                        Ok(Some(Rebased {
                            kept,
                            emptied: emptied.max(0) as u64,
                            bound,
                        }))
                    })
                    .await
            })
            .await
    }
}
