//! The operator's verbs over the queue (ADR-0042 decision 8): a page of the
//! held heads; the heads held under a cause given up; a (channel, Tenant)'s
//! queue given up whole. Each takes a principal and is told to the observer
//! once it commits, so that a host audits who did what; each batch is one
//! autocommitted statement that locks the rows it takes `SKIP LOCKED`, so
//! that a head a dispatcher holds is passed by and counted; each wakes the
//! slots whose rows it moved, those no dispatcher holds, so that a slot
//! asleep goes on at once and one held within its holder's next sleep.
//!
//! A verb locks a queue head's row for one statement (ADR-0009 decision 4
//! as ADR-0042 amends it): a walk that waits on it reads it, once the verb
//! commits, as vanished, and takes the slot's next head at its next call.
//! The verb waits for nothing — its rows and its slots are `SKIP LOCKED` —
//! so no cycle passes through it.

use ascetic_ddd_bus::Cause;
use ascetic_ddd_session::{PgAccess, SessionPool};

use super::store::{CHANNEL_OF_ROW, LEAVE_QUEUE, QUEUE, in_domain};
use super::{Hold, PgInbox};
use crate::error::Error;
use crate::intake;
use crate::message::CausalDependency;
use crate::observer::{InboxObserver, Operation};

/// How many rows a statement of a verb moves at most.
const VERB_BATCH: u32 = 100;

/// A CTE `woke` that wakes the slots of the rows `moved` returns — a CTE
/// with a `slot` column — that no dispatcher holds: their round, their sleep
/// and the arrival their sleep waits for cleared, as a processed head's are.
/// A slot a dispatcher holds is passed by, `SKIP LOCKED`, and sleeps at most
/// its holder's next sleep.
pub(super) fn wake(table: &str, moved: &str) -> String {
    format!(
        "woke AS ( \
            UPDATE {table}_slots s SET round_from = NULL, dormant_until = NULL, dormant_after = NULL \
            WHERE s.slot IN (SELECT l.slot FROM {table}_slots l \
                             WHERE l.slot IN (SELECT slot FROM {moved}) \
                             FOR UPDATE OF l SKIP LOCKED) \
            RETURNING 1 \
         )"
    )
}

/// The heads held under a cause that [`PgInbox::park_heads`] gives up.
#[derive(Clone, Copy, Debug)]
pub struct ParkingHeads<'a> {
    /// Their channel, without its key.
    pub channel: &'a str,
    /// The cause they are held under.
    pub cause: &'a str,
    /// Their Tenant, when only one's.
    pub tenant: Option<&'a str>,
    /// The most heads parked.
    pub limit: u32,
}

/// The queue of one (channel, Tenant) that [`PgInbox::park_queue`] gives up.
#[derive(Clone, Copy, Debug)]
pub struct ParkingQueue<'a> {
    /// The channel, without its key.
    pub channel: &'a str,
    /// The Tenant.
    pub tenant: &'a str,
    /// The cause the rows are parked under: what [`PgInbox::unpark_all`]
    /// re-arms them by.
    pub cause: Cause,
    /// The most rows parked.
    pub limit: u32,
}

/// What a verb that gives rows up did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GivenUp {
    /// Rows parked.
    pub parked: u64,
    /// Rows it named and passed by, a dispatcher holding them: asked again,
    /// it takes those its holder has let go.
    pub passed: u64,
    /// Whether rows it names remain past `limit`.
    pub more: bool,
}

/// A held head as the operator's page gives it (ADR-0042 decision 8): by
/// its place and, when it reads, its identity; never its values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeldHead {
    /// Its order of arrival.
    pub received_position: i64,
    /// The slot it is in.
    pub slot: u32,
    /// The channel it arrived on, its key included.
    pub uri: String,
    /// What holds it.
    pub hold: Hold,
    /// The cause it is held under.
    pub cause: Option<String>,
    /// When its hold began, as PostgreSQL prints a `timestamptz`.
    pub since: String,
    /// When it is due again, likewise.
    pub until: Option<String>,
    /// How many `Unavailable` verdicts it was told in a row; none for a
    /// back-off.
    pub holds: Option<u32>,
    /// Its failed attempts.
    pub attempts: u32,
    /// Its identity; `None` when its stream id does not read back in the
    /// bus's domain.
    pub identity: Option<CausalDependency>,
}

/// The last part of a verb's statement: the rows `candidates` names — a
/// CTE of `place`s read unlocked — locked `SKIP LOCKED`, their selection
/// `still` tested again on the row locked, parked with `set`, and their
/// slots woken; it reads how many it parked, woke and examined.
fn park_the_candidates(table: &str, still: &str, set: &str) -> String {
    format!(
        "chosen AS ( \
            SELECT r.ctid AS place FROM {table} r \
            WHERE r.ctid = ANY (ARRAY(SELECT place FROM candidates)) AND {still} \
            FOR UPDATE OF r SKIP LOCKED \
         ), parked AS ( \
            UPDATE {table} SET parked_at = CURRENT_TIMESTAMP, {set}{LEAVE_QUEUE} \
            WHERE ctid IN (SELECT place FROM chosen) \
            RETURNING slot \
         ), {wake} \
         SELECT (SELECT count(*) FROM parked), (SELECT count(*) FROM woke), \
                (SELECT count(*) FROM candidates)",
        wake = wake(table, "parked")
    )
}

/// The heads [`PgInbox::park_heads`] names, as `r`: of a channel (`$1`),
/// held under a cause (`$2`), of one Tenant when named (`$3`).
const HELD_UNDER: &str = "held_since IS NOT NULL AND {channel} = $1 AND cause = $2 \
     AND ($3::text IS NULL OR tenant_id = $3::text) AND {queue}";

/// The rows [`PgInbox::park_queue`] names, as `r`: of a channel (`$1`) and
/// a Tenant (`$2`), in the queue.
const QUEUE_OF: &str = "tenant_id = $2 AND {channel} = $1 AND {queue}";

fn written(selection: &str) -> String {
    selection
        .replace("{channel}", CHANNEL_OF_ROW)
        .replace("{queue}", QUEUE)
}

/// A statement of [`PgInbox::park_heads`]: a batch (`$4`) of the heads it
/// names, the oldest hold first, in the order of `__held_idx`, so that a
/// batch reads a batch of the index and sorts nothing; parked under their
/// cause. One head a (slot, Tenant): their order among themselves is no
/// stream's.
pub(super) fn park_heads_batch(table: &str) -> String {
    let held = written(HELD_UNDER);
    format!(
        "WITH candidates AS MATERIALIZED ( \
            SELECT r.ctid AS place FROM {table} r WHERE {held} \
            ORDER BY r.held_since LIMIT $4 \
         ), {}",
        park_the_candidates(table, &held, "")
    )
}

/// A statement of [`PgInbox::park_queue`]: a batch (`$4`) of a (channel,
/// Tenant)'s queue rows, the newest first, parked under the operator's
/// cause (`$3`). The newest first, so that what the verb has parked is
/// always the end of each stream: a dispatcher that serves the Tenant
/// between two batches takes its oldest row, older than every row parked,
/// and the re-arm, oldest first, overtakes nothing. The Tenant's rows are
/// probed slot by slot (`$5`), each probe the newest of its slot by
/// `__tenant_head_idx`, as the walk probes: a probe of the Tenant by another
/// index reads its processed history, which a stream waited for keeps whole
/// (ADR-0033 decision 4).
pub(super) fn park_queue_batch(table: &str) -> String {
    let of = written(QUEUE_OF);
    format!(
        "WITH candidates AS MATERIALIZED ( \
            SELECT q.place FROM unnest($5::smallint[]) AS s(slot) \
            CROSS JOIN LATERAL ( \
                SELECT r.ctid AS place, r.received_position FROM {table} r \
                WHERE r.slot = s.slot AND {of} \
                ORDER BY r.received_position DESC LIMIT $4 \
            ) q \
            ORDER BY q.received_position DESC LIMIT $4 \
         ), {}",
        park_the_candidates(table, &of, "cause = $3, ")
    )
}

impl<P, O> PgInbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: InboxObserver,
{
    /// A page of the held heads, after `after`'s order of arrival, at most
    /// `limit`, oldest first, each by its place, its hold and, when it
    /// reads, its identity (ADR-0042 decision 8), as [`PgInbox::parked`]
    /// pages the parked rows. Told to the observer as `principal`'s read.
    ///
    /// # Errors
    /// [`Error`] when the database cannot be read.
    pub async fn held(
        &self,
        principal: &str,
        after: Option<i64>,
        limit: u32,
    ) -> Result<Vec<HeldHead>, Error> {
        let sql = format!(
            "SELECT received_position, slot, uri, coalesce(hold_kind, 'backoff'), cause, \
                    held_since::text, next_attempt_at::text, holds, attempts, \
                    tenant_id, stream_type, stream_id, stream_position \
             FROM {} WHERE held_since IS NOT NULL AND received_position > $1 \
             ORDER BY received_position LIMIT $2",
            self.table
        );
        let rows = self
            .pool
            .session(async |session: P::Session| {
                Ok::<_, Error>(
                    session
                        .connection()
                        .query(&sql, &[&after.unwrap_or(i64::MIN), &i64::from(limit)])
                        .await?,
                )
            })
            .await?;
        let page = rows
            .iter()
            .map(|row| {
                let hold: String = row.try_get(3)?;
                Ok(HeldHead {
                    received_position: row.try_get(0)?,
                    slot: u32::try_from(row.try_get::<_, i16>(1)?).unwrap_or_default(),
                    uri: row.try_get(2)?,
                    hold: Hold::parse(&hold).ok_or_else(|| {
                        Error::Malformed(format!("the hold `{hold}` is not the library's"))
                    })?,
                    cause: row.try_get(4)?,
                    since: row.try_get(5)?,
                    until: row.try_get(6)?,
                    holds: row
                        .try_get::<_, Option<i32>>(7)?
                        .and_then(|holds| u32::try_from(holds).ok()),
                    attempts: u32::try_from(row.try_get::<_, i32>(8)?).unwrap_or_default(),
                    identity: row
                        .try_get::<_, serde_json::Value>(11)
                        .ok()
                        .filter(in_domain)
                        .map(|stream_id| CausalDependency {
                            tenant_id: row.get(9),
                            stream_type: row.get(10),
                            stream_id,
                            stream_position: row.get(12),
                        }),
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        self.operated(principal, Operation::PagedHeld, None, None, page.len());
        Ok(page)
    }

    /// Parks the heads `heads` names (ADR-0042 decision 8): those of a
    /// channel held under a cause — of one Tenant when named — oldest first,
    /// a batch at a time, each its own statement, the observer told after
    /// each commits. A head keeps its cause and its attempts, and parks on
    /// the failure ground: [`PgInbox::unpark_all`] re-arms it by its cause.
    /// The rows behind it go on. A head a dispatcher holds is passed by.
    ///
    /// # Errors
    /// [`Error`] when the environment fails, the batches before it parked.
    pub async fn park_heads(
        &self,
        principal: &str,
        heads: &ParkingHeads<'_>,
    ) -> Result<GivenUp, Error> {
        let sql = park_heads_batch(self.table.as_str());
        let remains = format!(
            "SELECT EXISTS (SELECT 1 FROM {} WHERE {})",
            self.table,
            written(HELD_UNDER)
        );
        let given = self
            .give_up(
                heads.limit,
                async |batch: i64| {
                    let done = self
                        .batch_of(&sql, &[&heads.channel, &heads.cause, &heads.tenant, &batch])
                        .await?;
                    self.operated_on(
                        principal,
                        Operation::ParkedHeads,
                        Some(heads.channel),
                        None,
                        heads.tenant,
                        Some(heads.cause),
                        usize::try_from(done.0).unwrap_or(usize::MAX),
                    );
                    Ok(done)
                },
                async || {
                    self.remains(&remains, &[&heads.channel, &heads.cause, &heads.tenant])
                        .await
                },
            )
            .await?;
        self.given_up_logged(principal, "held heads", heads.channel, &given);
        Ok(given)
    }

    /// Parks the queue `queue` names (ADR-0042 decision 8): every queue row
    /// of a (channel, Tenant), held or not, the newest first, a batch at a
    /// time, each its own statement, under the operator's cause, the
    /// observer told after each commits: the queue given up whole — but for
    /// the rows a dispatcher processed meanwhile, each older than every row
    /// parked — and re-armed in its order by [`PgInbox::unpark_all`] under
    /// that cause, overtaking nothing. A head a dispatcher holds is passed
    /// by; the rows set aside to wait are not the queue's.
    ///
    /// # Errors
    /// [`Error`] when the environment fails, the batches before it parked.
    pub async fn park_queue(
        &self,
        principal: &str,
        queue: &ParkingQueue<'_>,
    ) -> Result<GivenUp, Error> {
        let sql = park_queue_batch(self.table.as_str());
        let slots: Vec<i16> = (0..i16::try_from(self.slots).unwrap_or(i16::MAX)).collect();
        let cause = queue.cause.as_str();
        let remains = format!(
            "SELECT EXISTS (SELECT 1 FROM unnest($3::smallint[]) AS s(slot) \
                            CROSS JOIN LATERAL (SELECT 1 FROM {} r \
                                                WHERE r.slot = s.slot AND {} LIMIT 1) q)",
            self.table,
            written(QUEUE_OF)
        );
        let given = self
            .give_up(
                queue.limit,
                async |batch: i64| {
                    let done = self
                        .batch_of(
                            &sql,
                            &[&queue.channel, &queue.tenant, &cause, &batch, &slots],
                        )
                        .await?;
                    self.operated_on(
                        principal,
                        Operation::ParkedQueue,
                        Some(queue.channel),
                        None,
                        Some(queue.tenant),
                        Some(cause),
                        usize::try_from(done.0).unwrap_or(usize::MAX),
                    );
                    Ok(done)
                },
                async || {
                    self.remains(&remains, &[&queue.channel, &queue.tenant, &slots])
                        .await
                },
            )
            .await?;
        self.given_up_logged(principal, "queue rows", queue.channel, &given);
        Ok(given)
    }

    /// Runs `batch` for at most `limit` rows, a batch at a time. Each
    /// examines a batch of the rows the verb names, and parks those no
    /// dispatcher holds, so a held one is examined again by the next: the
    /// verb ends when a batch examined fewer than it was asked — the rows it
    /// did not park are the held, passed by — or parked none, every row it
    /// examined held; or at the limit, `remains` saying whether rows it
    /// names are left.
    async fn give_up(
        &self,
        limit: u32,
        batch: impl AsyncFn(i64) -> Result<(u64, u64), Error>,
        remains: impl AsyncFn() -> Result<bool, Error>,
    ) -> Result<GivenUp, Error> {
        let mut given = GivenUp::default();
        loop {
            let room = u64::from(limit).saturating_sub(given.parked);
            let asked = room.min(u64::from(VERB_BATCH));
            if asked == 0 {
                given.more = remains().await?;
                return Ok(given);
            }
            let (parked, examined) = batch(i64::try_from(asked).unwrap_or(i64::MAX)).await?;
            given.parked += parked;
            if examined < asked {
                given.passed = examined.saturating_sub(parked);
                return Ok(given);
            }
            if parked == 0 {
                given.passed = examined;
                given.more = true;
                return Ok(given);
            }
        }
    }

    /// One statement of a verb: how many rows it parked and examined.
    async fn batch_of(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<(u64, u64), Error> {
        self.pool
            .session(async |session: P::Session| {
                let row = session.connection().query_one(sql, params).await?;
                let count = |at: usize| u64::try_from(row.get::<_, i64>(at)).unwrap_or_default();
                Ok::<_, Error>((count(0), count(2)))
            })
            .await
    }

    /// Whether rows a verb names remain, read unlocked after it.
    async fn remains(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<bool, Error> {
        self.pool
            .session(async |session: P::Session| {
                Ok::<_, Error>(session.connection().query_one(sql, params).await?.get(0))
            })
            .await
    }

    fn given_up_logged(&self, principal: &str, what: &str, channel: &str, given: &GivenUp) {
        log::warn!(
            "inbox: {} parked {} {what} of {}, {} passed by{}",
            intake::sanitized(Some(principal.as_bytes()), intake::DERIVED_WIDTH)
                .unwrap_or_default(),
            given.parked,
            intake::sanitized(Some(channel.as_bytes()), intake::CHANNEL_WIDTH).unwrap_or_default(),
            given.passed,
            if given.more { ", more remain" } else { "" },
        );
    }
}
