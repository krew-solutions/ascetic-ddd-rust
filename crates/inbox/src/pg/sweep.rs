//! The sweep: processed rows forgotten once their channel's window has
//! passed (ADR-0033 decision 4), and the rows set aside given a bounded life
//! (ADR-0036 R6). The rules are in [`InboxRetention`].

use std::time::{Duration, Instant};

use ascetic_ddd_session::pg::tokio_postgres::types::ToSql;
use ascetic_ddd_session::{PgAccess, Session, SessionPool};

use super::{InboxRetention, PgInbox};
use crate::error::Error;
use crate::observer::{InboxObserver, Swept};

/// How many statements one pass gives a channel at most, so that a channel
/// with a deep backlog does not hold the others back.
const BATCHES_PER_PASS: u32 = 100;

/// When the sweep may forget: not for `grace` after the process starts, nor
/// after a pass that could not reach the database, so that intakes and
/// relays rejoining after an outage redeliver first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Grace {
    grace: Duration,
    armed_at: Instant,
}

impl Grace {
    /// Armed `grace` after `now`.
    pub(super) fn new(grace: Duration, now: Instant) -> Self {
        Grace {
            grace,
            armed_at: now + grace,
        }
    }

    /// Whether a pass may forget at `now`.
    pub(super) fn allows(&self, now: Instant) -> bool {
        now >= self.armed_at
    }

    /// A pass failed at `now`: the grace starts again.
    pub(super) fn failed(self, now: Instant) -> Self {
        Grace::new(self.grace, now)
    }
}

impl<P, O> PgInbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: InboxObserver,
{
    /// One pass of the sweep: every channel with a window forgets its rows
    /// processed longer ago than the window, in statements of at most
    /// [`InboxRetention::batch`] rows, each a transaction of its own under a
    /// try-lock of the inbox's sweep — another holder skips the statement —
    /// and the configured lock timeout, rows locked elsewhere passed by.
    /// Never an unprocessed, parked or waiting row, nor a processed row that
    /// a waiting row names. Each statement starts where the last one of its
    /// channel stopped. Returns how many rows it forgot; tells the observer
    /// per channel.
    pub async fn sweep(&self) -> Result<u64, Error> {
        let mut forgotten = 0;
        for (channel, window) in self.retention.windows() {
            let mut swept = 0;
            for _ in 0..BATCHES_PER_PASS {
                let (rows, full) = self.forget_batch(channel, window).await?;
                swept += rows;
                if !full {
                    break;
                }
            }
            if swept > 0 {
                self.observer.on_swept(&Swept {
                    channel,
                    forgotten: swept,
                });
            }
            forgotten += swept;
            self.forget_set_aside(Some(channel), window).await?;
        }
        if let Some(bound) = self.retention.untenanted {
            self.forget_set_aside(None, bound).await?;
        }
        Ok(forgotten)
    }

    /// The rows set aside of `channel` — or, with none, those without a
    /// Tenant, of every channel — given their bounded life (ADR-0036 R6): an
    /// open row first seen longer ago than `window` is closed as expired,
    /// its bytes gone, a tombstone; a closed row is forgotten `window` after
    /// it closed. Under the sweep's try-lock, in transactions of at most
    /// [`InboxRetention::batch`] rows each. Returns how many rows it closed
    /// or forgot.
    async fn forget_set_aside(
        &self,
        channel: Option<&str>,
        window: Duration,
    ) -> Result<u64, Error> {
        let table = &self.table;
        let whose =
            "(($1::text IS NOT NULL AND channel = $1) OR ($1::text IS NULL AND tenant_id IS NULL))";
        let past = "statement_timestamp() - make_interval(secs => $2::double precision)";
        let expire = format!(
            "UPDATE {table}_rejects SET disposed_at = statement_timestamp(),                  disposition = 'expired', message = NULL              WHERE ctid IN (SELECT ctid FROM {table}_rejects                             WHERE {whose} AND disposed_at IS NULL AND first_seen_at < {past}                             LIMIT $3 FOR UPDATE SKIP LOCKED)"
        );
        let forget = format!(
            "DELETE FROM {table}_rejects              WHERE ctid IN (SELECT ctid FROM {table}_rejects                             WHERE {whose} AND disposed_at < {past}                             LIMIT $3 FOR UPDATE SKIP LOCKED)"
        );
        let batch = i64::from(self.retention.batch.max(1));
        let mut done = 0;
        for sql in [&expire, &forget] {
            for _ in 0..BATCHES_PER_PASS {
                let rows = self
                    .under_the_sweep_lock(sql, &[&channel, &window.as_secs_f64(), &batch])
                    .await?;
                done += rows;
                if rows < u64::try_from(batch).unwrap_or(u64::MAX) {
                    break;
                }
            }
        }
        Ok(done)
    }

    /// Executes `sql` in a transaction of its own under a try-lock of the
    /// inbox's sweep — another holder skips it, and it reports nothing done
    /// — and the configured lock timeout. Returns the rows it touched.
    async fn under_the_sweep_lock(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, Error> {
        let key = format!("{} sweep", self.table);
        self.pool
            .session(async |session: P::Session| {
                session
                    .atomic(async |tx: P::Session| {
                        let connection = tx.connection();
                        connection
                            .batch_execute(&format!(
                                "SET LOCAL lock_timeout = '{}ms'",
                                self.retention.lock_timeout.as_millis()
                            ))
                            .await?;
                        let held: bool = connection
                            .query_one(
                                "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))",
                                &[&key],
                            )
                            .await?
                            .get(0);
                        if !held {
                            return Ok(0);
                        }
                        Ok::<_, Error>(connection.execute(sql, params).await?)
                    })
                    .await
            })
            .await
    }

    /// One statement of a channel's sweep: how many rows it forgot, and
    /// whether it forgot a whole batch, so that more may be waiting.
    async fn forget_batch(&self, channel: &str, window: Duration) -> Result<(u64, bool), Error> {
        let table = &self.table;
        let from = self
            .swept_to
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(channel)
            .cloned();
        let sql = format!(
            "WITH gone AS (DELETE FROM {table} WHERE ctid IN ( \
                SELECT r.ctid FROM {table} r \
                WHERE (split_part(r.uri, '/', 1) || '//' || split_part(r.uri, '/', 3)) = $1 \
                  AND r.processed_at < statement_timestamp() - make_interval(secs => $2::double precision) \
                  AND ($3::text IS NULL OR r.processed_at >= $3::text::timestamptz) \
                  AND r.processed_position IS NOT NULL \
                  AND NOT EXISTS (SELECT 1 FROM {table} w \
                                  WHERE w.waiting_for = jsonb_build_object( \
                                      'tenant_id', r.tenant_id, 'stream_type', r.stream_type, \
                                      'stream_id', r.stream_id, 'stream_position', r.stream_position)) \
                ORDER BY r.processed_at \
                LIMIT $4 \
                FOR UPDATE OF r SKIP LOCKED) \
             RETURNING processed_at) \
             SELECT count(*), max(processed_at)::text FROM gone"
        );
        let batch = i64::from(self.retention.batch.max(1));
        let key = format!("{table} sweep");
        let (forgotten, latest) = self
            .pool
            .session(async |session: P::Session| {
                session
                    .atomic(async |tx: P::Session| {
                        let connection = tx.connection();
                        connection
                            .batch_execute(&format!(
                                "SET LOCAL lock_timeout = '{}ms'",
                                self.retention.lock_timeout.as_millis()
                            ))
                            .await?;
                        let held: bool = connection
                            .query_one(
                                "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))",
                                &[&key],
                            )
                            .await?
                            .get(0);
                        if !held {
                            return Ok((0, None));
                        }
                        let row = connection
                            .query_one(&sql, &[&channel, &window.as_secs_f64(), &from, &batch])
                            .await?;
                        Ok::<_, Error>((row.get::<_, i64>(0), row.get::<_, Option<String>>(1)))
                    })
                    .await
            })
            .await?;
        // the latest time forgotten, as the database prints it, read back as
        // it was
        if let Some(latest) = latest {
            self.swept_to
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(channel.to_owned(), latest);
        }
        Ok((forgotten as u64, forgotten >= batch))
    }

    /// Stamps the processed rows that carry no time — processed before the
    /// mark kept one, or by a binary from before it in a rolling deploy —
    /// with the current one, so that the sweep may forget them a window from
    /// now; until then they are kept (ADR-0033 § Consequences). Walks the
    /// table by its primary key in transactions of [`InboxRetention::batch`]
    /// rows, to its end. Returns how many it stamped.
    pub async fn stamp_processed(&self) -> Result<u64, Error> {
        let table = &self.table;
        let sql = format!(
            "WITH page AS ( \
                SELECT tenant_id, stream_type, stream_id, stream_position, \
                       processed_position IS NOT NULL AND processed_at IS NULL AS unstamped \
                FROM {table} \
                WHERE $1::text IS NULL \
                   OR (tenant_id, stream_type, stream_id, stream_position) \
                      > ($1::text, $2::text, $3::text::jsonb, $4::integer) \
                ORDER BY tenant_id, stream_type, stream_id, stream_position \
                LIMIT $5 \
             ), stamped AS ( \
                UPDATE {table} t SET processed_at = statement_timestamp() \
                FROM page \
                WHERE page.unstamped AND t.processed_at IS NULL \
                  AND (t.tenant_id, t.stream_type, t.stream_id, t.stream_position) \
                      = (page.tenant_id, page.stream_type, page.stream_id, page.stream_position) \
                RETURNING 1 \
             ) \
             SELECT (SELECT count(*) FROM stamped), \
                    last.tenant_id::text, last.stream_type::text, last.stream_id::text, \
                    last.stream_position \
             FROM (SELECT 1) AS one \
             LEFT JOIN LATERAL (SELECT * FROM page \
                                ORDER BY tenant_id DESC, stream_type DESC, stream_id DESC, \
                                         stream_position DESC LIMIT 1) AS last ON true"
        );
        let batch = i64::from(self.retention.batch.max(1));
        let mut after: Option<(String, String, String, i32)> = None;
        let mut stamped = 0;
        loop {
            let row = self
                .pool
                .session(async |session: P::Session| {
                    let (tenant, stream_type, stream_id, position) = match &after {
                        Some((tenant, stream_type, stream_id, position)) => (
                            Some(tenant.as_str()),
                            Some(stream_type.as_str()),
                            Some(stream_id.as_str()),
                            Some(*position),
                        ),
                        None => (None, None, None, None),
                    };
                    Ok::<_, Error>(
                        session
                            .connection()
                            .query_one(
                                &sql,
                                &[&tenant, &stream_type, &stream_id, &position, &batch],
                            )
                            .await?,
                    )
                })
                .await?;
            stamped += row.get::<_, i64>(0) as u64;
            match row.get::<_, Option<String>>(1) {
                Some(tenant) => {
                    after = Some((tenant, row.get(2), row.get(3), row.get(4)));
                }
                None => return Ok(stamped),
            }
        }
    }

    /// The sweep of a process, until `stopped`: a pass once
    /// [`InboxRetention::interval`], forgetting nothing until the grace has
    /// passed, the grace started again after a pass that failed. Its errors
    /// are logged and never touch the inbox's loops. Nothing to do when no
    /// channel has a window.
    pub(super) async fn sweep_until(&self, mut stopped: tokio::sync::watch::Receiver<bool>) {
        let InboxRetention {
            interval, grace, ..
        } = self.retention.clone();
        if self.retention.windows().next().is_none() {
            return;
        }
        let mut grace = Grace::new(grace, Instant::now());
        loop {
            tokio::select! {
                _ = stopped.changed() => return,
                _ = tokio::time::sleep(interval) => {}
            }
            if *stopped.borrow() {
                return;
            }
            if !grace.allows(Instant::now()) {
                continue;
            }
            if let Err(error) = self.sweep().await {
                grace = grace.failed(Instant::now());
                log::warn!("inbox: the sweep failed, nothing forgotten for its grace: {error}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Nothing is forgotten within the grace after the start, nor within it
    /// after a pass that failed.
    #[test]
    fn the_grace_starts_again_after_a_failed_pass() {
        let start = Instant::now();
        let grace = Grace::new(Duration::from_secs(10), start);
        assert!(!grace.allows(start + Duration::from_secs(9)));
        assert!(grace.allows(start + Duration::from_secs(10)));
        let failed = grace.failed(start + Duration::from_secs(20));
        assert!(!failed.allows(start + Duration::from_secs(29)));
        assert!(failed.allows(start + Duration::from_secs(30)));
    }
}
