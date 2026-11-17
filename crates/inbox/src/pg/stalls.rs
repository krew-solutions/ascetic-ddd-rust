//! The report of a stall (ADR-0042 decision 6): a row a (channel, cause,
//! process) in `<table>_stalls`, written when this process's breaker finds
//! its channel stalled and refreshed every tick while it stays so, valid for
//! the breaker's `stall_ttl` from the database's clock. It is a report for
//! people and their alerts, never a gate: no statement of the inbox reads it
//! to decide. A resumed channel's row is no longer refreshed and lapses, so
//! that a stage that comes back and goes again keeps its row valid across
//! the gap, its openings counted — the flapping a host pages on.

use std::collections::{BTreeMap, HashMap};
use std::hash::BuildHasher;
use std::time::{Duration, Instant, SystemTime};

use ascetic_ddd_session::{PgAccess, Session, SessionPool};

use super::PgInbox;
use crate::error::Error;
use crate::observer::InboxObserver;

/// How long a row lapsed stays before any process's report removes it: a
/// row of a process that died lapses and goes.
const LAPSED_KEPT: &str = "1 hour";

/// The longest a report's statements run or wait for a lock: a report that
/// cannot be written now is written by the next tick.
pub(super) const REPORT_WITHIN: Duration = Duration::from_secs(1);

/// This process's stalls: its label, the refreshes the breaker asked for
/// and not yet written, and the pause each (channel, cause) last wrote.
pub(super) struct Stalls {
    reporter: String,
    pending: Vec<Pending>,
    written: HashMap<(String, String), Instant>,
}

/// A refresh the breaker asked for.
struct Pending {
    channel: String,
    cause: String,
    since: Instant,
    ttl: Duration,
}

impl Stalls {
    /// The stalls of a process, its label drawn at random now: a label of
    /// the report, not an owner (ADR-0008 decision 3).
    pub(super) fn new() -> Self {
        let drawn = std::collections::hash_map::RandomState::new()
            .hash_one((std::process::id(), SystemTime::now()));
        Stalls {
            reporter: format!("{drawn:016x}"),
            pending: Vec::new(),
            written: HashMap::new(),
        }
    }

    /// A refresh of `channel`'s stall under `cause`, the pause begun at
    /// `since`, valid for `ttl`.
    pub(super) fn stalled(&mut self, channel: &str, cause: &str, since: Instant, ttl: Duration) {
        self.pending.push(Pending {
            channel: channel.to_owned(),
            cause: cause.to_owned(),
            since,
            ttl,
        });
    }
}

/// A stall as `<table>_stalls` holds it (ADR-0042 decision 6).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stall {
    /// The channel paused, without its key.
    pub channel: String,
    /// The pause's cause.
    pub cause: String,
    /// The process that reports it: a label drawn at its start
    /// ([`PgInbox::reporter`]).
    pub reporter: String,
    /// When the stall began, as PostgreSQL prints a `timestamptz`.
    pub since: String,
    /// Until when the report holds without a refresh, likewise.
    pub valid_until: String,
    /// How many times the channel was paused under this cause while the
    /// report held: more than one is a stage that came back and went again.
    pub openings: u32,
    /// Whether the report holds now, by the database's clock.
    pub valid: bool,
}

impl<P, O> PgInbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: InboxObserver,
{
    /// This process's label in the stalls it reports.
    pub fn reporter(&self) -> String {
        self.stall_state().reporter.clone()
    }

    pub(super) fn stall_state(&self) -> std::sync::MutexGuard<'_, Stalls> {
        self.stalls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Writes the refreshes the breaker asked for, on a session of its own,
    /// each row valid for its `ttl` from the database's clock: a row lapsed
    /// is a new stall, from the pause's start; one still valid keeps its
    /// start, and counts an opening when the pause is a new one. The rows are
    /// written in one order, so that two loops of a process reporting the
    /// same stalls wait for each other rather than cycle; the pause each
    /// writes is marked when it is taken, so that two loops count one
    /// opening once. Then other processes' rows lapsed for an hour are
    /// removed, those another statement holds passed by. A report is for
    /// people, never a gate: it is bounded by [`REPORT_WITHIN`], and its
    /// failure is logged and leaves the dispatch to go on (ADR-0042
    /// decision 6). Nothing to do when nothing stalled.
    pub(super) async fn report(&self) {
        let (reporter, rows) = {
            let mut stalls = self.stall_state();
            let mut latest: BTreeMap<(String, String), Pending> = BTreeMap::new();
            for pending in stalls.pending.drain(..) {
                latest.insert((pending.channel.clone(), pending.cause.clone()), pending);
            }
            let rows: Vec<(Pending, bool)> = latest
                .into_iter()
                .map(|(key, pending)| {
                    let opening = stalls.written.insert(key, pending.since) != Some(pending.since);
                    (pending, opening)
                })
                .collect();
            (stalls.reporter.clone(), rows)
        };
        if rows.is_empty() {
            return;
        }
        if let Err(error) = self.write_stalls(&reporter, &rows).await {
            log::warn!(
                "inbox: the stalls could not be reported, the next tick reports them: {error}"
            );
        }
    }

    async fn write_stalls(&self, reporter: &str, rows: &[(Pending, bool)]) -> Result<(), Error> {
        let now = super::dispatch::now();
        let channels: Vec<&str> = rows.iter().map(|(p, _)| p.channel.as_str()).collect();
        let causes: Vec<&str> = rows.iter().map(|(p, _)| p.cause.as_str()).collect();
        let ago: Vec<f64> = rows
            .iter()
            .map(|(p, _)| now.saturating_duration_since(p.since).as_secs_f64())
            .collect();
        let ttls: Vec<f64> = rows.iter().map(|(p, _)| p.ttl.as_secs_f64()).collect();
        let openings: Vec<i32> = rows
            .iter()
            .map(|(_, opening)| i32::from(*opening))
            .collect();
        let table = &self.table;
        let upsert = format!(
            "WITH reported AS ( \
                SELECT * FROM unnest($1::text[], $2::text[], $3::double precision[], \
                                     $4::double precision[], $5::integer[]) \
                    AS r(channel, cause, ago, ttl, opening) \
             ) \
             INSERT INTO {table}_stalls AS s (channel, cause, reporter, since, valid_until, openings) \
             SELECT channel, cause, $6, clock_timestamp() - make_interval(secs => ago), \
                    clock_timestamp() + make_interval(secs => ttl), 1 \
             FROM reported ORDER BY channel, cause \
             ON CONFLICT (channel, cause, reporter) DO UPDATE SET \
                since = CASE WHEN s.valid_until < clock_timestamp() THEN excluded.since \
                             ELSE s.since END, \
                openings = CASE WHEN s.valid_until < clock_timestamp() THEN 1 \
                                ELSE s.openings + (SELECT r.opening FROM reported r \
                                                   WHERE r.channel = excluded.channel \
                                                     AND r.cause = excluded.cause) END, \
                valid_until = excluded.valid_until"
        );
        let purge = format!(
            "DELETE FROM {table}_stalls WHERE ctid IN ( \
                SELECT ctid FROM {table}_stalls \
                WHERE reporter <> $1 AND valid_until < clock_timestamp() - interval '{LAPSED_KEPT}' \
                FOR UPDATE SKIP LOCKED)"
        );
        let millis = REPORT_WITHIN.as_millis();
        self.pool
            .session(async |session: P::Session| {
                session
                    .atomic(async |tx: P::Session| {
                        let connection = tx.connection();
                        connection
                            .batch_execute(&format!(
                                "SET LOCAL statement_timeout = {millis}; \
                                 SET LOCAL lock_timeout = {millis}"
                            ))
                            .await?;
                        connection
                            .execute(
                                &upsert,
                                &[&channels, &causes, &ago, &ttls, &openings, &reporter],
                            )
                            .await?;
                        connection.execute(&purge, &[&reporter]).await?;
                        Ok::<_, Error>(())
                    })
                    .await
            })
            .await
    }

    /// The stalls every process reports, by channel, cause and reporter:
    /// what a host pages on (ADR-0042 decision 6). In a transaction of its
    /// own on the inbox's pool, bounded by `within`.
    ///
    /// # Errors
    /// [`Error`] when the database cannot be read within `within`.
    pub async fn stalls(&self, within: Duration) -> Result<Vec<Stall>, Error> {
        let millis = i32::try_from(within.as_millis()).unwrap_or(i32::MAX).max(1);
        let sql = format!(
            "SELECT channel, cause, reporter, since::text, valid_until::text, openings, \
                    valid_until > clock_timestamp() \
             FROM {}_stalls ORDER BY channel, cause, reporter",
            self.table
        );
        let rows = self
            .pool
            .session(async |session: P::Session| {
                session
                    .atomic(async |tx| {
                        tx.connection()
                            .batch_execute(&format!("SET LOCAL statement_timeout = {millis}"))
                            .await?;
                        Ok::<_, Error>(tx.connection().query(&sql, &[]).await?)
                    })
                    .await
            })
            .await?;
        rows.iter()
            .map(|row| {
                Ok(Stall {
                    channel: row.try_get(0)?,
                    cause: row.try_get(1)?,
                    reporter: row.try_get(2)?,
                    since: row.try_get(3)?,
                    valid_until: row.try_get(4)?,
                    openings: u32::try_from(row.try_get::<_, i32>(5)?).unwrap_or_default(),
                    valid: row.try_get(6)?,
                })
            })
            .collect()
    }
}
