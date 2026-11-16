//! The operator's surface of the rows set aside (ADR-0036 decision 4, R7):
//! counts, a page and the kept bytes; readmit, readmit in bulk and discard.
//! Every call names its principal, reads included, and the observer is told
//! of it once it has committed, so that a host audits who read and did
//! what. Each disposition runs in a transaction of its own, never the
//! caller's, whose commit the library could not see.

use std::time::Duration;

use ascetic_ddd_session::{PgAccess, SessionPool};

use super::store::{CHANNEL_OF_ROW, QUEUE};
use super::{Admission, PgInbox};
use crate::error::Error;
use crate::intake::{self, Reason};
use crate::observer::{InboxObserver, Operated, Operation};

/// The data key's fields, which a page never projects: what they wrap opens
/// the payload (ADR-0036 R7).
const SEALED: [&str; 3] = ["dek", "dek_algorithm", "dek_bound_to"];

/// How many characters of a header's value a page gives.
const VALUE_WIDTH: usize = 255;

/// The open rows set aside of one channel and reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetAsideCount {
    /// The channel the messages arrived on.
    pub channel: String,
    /// Why they were set aside.
    pub reason: Reason,
    /// How many rows are open.
    pub open: u64,
    /// When the oldest of them was first seen, as PostgreSQL prints a
    /// `timestamptz`.
    pub oldest: String,
}

/// Where a page of rows set aside starts: past the row of this first sight
/// and digest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetAsideCursor {
    /// The row's first sight, as a page gave it.
    pub first_seen_at: String,
    /// The row's digest.
    pub digest: String,
}

/// What a page of rows set aside asks for.
#[derive(Clone, Debug, Default)]
pub struct SetAsidePage<'a> {
    /// The channel.
    pub channel: &'a str,
    /// One reason, or all.
    pub reason: Option<Reason>,
    /// Past this row, or from the first.
    pub after: Option<SetAsideCursor>,
    /// At most this many rows.
    pub limit: u32,
    /// The metadata keys to give from each row's message, never the data
    /// key's.
    pub metadata: &'a [&'a str],
}

/// An open row set aside, as a page gives it: never its payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetAsideRow {
    /// The digest it is kept under.
    pub digest: String,
    /// The channel the message arrived on.
    pub channel: String,
    /// Its Tenant, when its header was a valid one.
    pub tenant_id: Option<String>,
    /// Its stream type, when its header was a valid one.
    pub stream_type: Option<String>,
    /// Why it was set aside.
    pub reason: Reason,
    /// What the intake found, in the library's words.
    pub detail: String,
    /// The channel and arrival position of the row that holds its
    /// `message_id`, for an identity taken.
    pub holder: Option<(String, i64)>,
    /// How many times the same bytes were set aside.
    pub seen: u64,
    /// When they were first seen.
    pub first_seen_at: String,
    /// When they were last seen.
    pub last_seen_at: String,
    /// The metadata keys asked for, each with its value when the message has
    /// it, text and bounded.
    pub metadata: Vec<(String, Option<String>)>,
}

impl SetAsideRow {
    /// Where the next page starts.
    #[must_use]
    pub fn cursor(&self) -> SetAsideCursor {
        SetAsideCursor {
            first_seen_at: self.first_seen_at.clone(),
            digest: self.digest.clone(),
        }
    }
}

impl<P, O> PgInbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: InboxObserver,
{
    /// The open rows set aside, by channel and reason, with the oldest
    /// one's first sight: what a host's gauge pages on.
    ///
    /// # Errors
    /// [`Error`] when the database cannot be read.
    pub async fn set_aside_counts(&self, principal: &str) -> Result<Vec<SetAsideCount>, Error> {
        let sql = format!(
            "SELECT channel, reason, count(*), min(first_seen_at)::text FROM {}_rejects \
             WHERE disposed_at IS NULL GROUP BY channel, reason ORDER BY channel, reason",
            self.table
        );
        let rows = self
            .pool
            .session(async |session: P::Session| {
                Ok::<_, Error>(session.connection().query(&sql, &[]).await?)
            })
            .await?;
        let counts = rows
            .iter()
            .map(|row| {
                Ok(SetAsideCount {
                    channel: row.get(0),
                    reason: reason_of(row.get(1))?,
                    open: u64::try_from(row.get::<_, i64>(2)).unwrap_or_default(),
                    oldest: row.get(3),
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        self.operated(principal, Operation::Counted, None, None, counts.len());
        Ok(counts)
    }

    /// A page of the open rows set aside of a channel, oldest first: each
    /// with its digest, reason, detail, holder, sightings and times, and the
    /// metadata keys asked for, never the payload.
    ///
    /// # Errors
    /// [`Error::Refused`] when the page asks for a data key's field;
    /// [`Error`] when the database cannot be read.
    pub async fn set_aside_page(
        &self,
        principal: &str,
        page: &SetAsidePage<'_>,
    ) -> Result<Vec<SetAsideRow>, Error> {
        if page.metadata.iter().any(|key| SEALED.contains(key)) {
            return Err(Error::Refused("a page never gives the data key's fields"));
        }
        let sql = format!(
            "SELECT digest::text, channel, tenant_id, stream_type, reason, detail, \
                    holder_channel, holder_position, seen::bigint, first_seen_at::text, \
                    last_seen_at::text, CASE WHEN $6 THEN message END \
             FROM {}_rejects \
             WHERE disposed_at IS NULL AND channel = $1 \
               AND ($2::text IS NULL OR reason = $2) \
               AND ($3::text IS NULL \
                    OR (first_seen_at, digest) > ($3::text::timestamptz, $4::text::uuid)) \
             ORDER BY first_seen_at, digest \
             LIMIT $5",
            self.table
        );
        let reason = page.reason.map(Reason::as_str);
        let (after_at, after_digest) = page
            .after
            .as_ref()
            .map(|cursor| (cursor.first_seen_at.as_str(), cursor.digest.as_str()))
            .unzip();
        let limit = i64::from(page.limit);
        let framed = !page.metadata.is_empty();
        let rows = self
            .pool
            .session(async |session: P::Session| {
                Ok::<_, Error>(
                    session
                        .connection()
                        .query(
                            &sql,
                            &[
                                &page.channel,
                                &reason,
                                &after_at,
                                &after_digest,
                                &limit,
                                &framed,
                            ],
                        )
                        .await?,
                )
            })
            .await?;
        let rows = rows
            .iter()
            .map(|row| {
                let message = row
                    .get::<_, Option<Vec<u8>>>(11)
                    .and_then(|bytes| intake::unframe(&bytes));
                Ok(SetAsideRow {
                    digest: row.get(0),
                    channel: row.get(1),
                    tenant_id: row.get(2),
                    stream_type: row.get(3),
                    reason: reason_of(row.get(4))?,
                    detail: row.get(5),
                    holder: row
                        .get::<_, Option<String>>(6)
                        .zip(row.get::<_, Option<i64>>(7)),
                    seen: u64::try_from(row.get::<_, i64>(8)).unwrap_or_default(),
                    first_seen_at: row.get(9),
                    last_seen_at: row.get(10),
                    metadata: page
                        .metadata
                        .iter()
                        .map(|key| {
                            let value = message.as_ref().and_then(|(_, message)| {
                                intake::sanitized(message.header(key), VALUE_WIDTH)
                            });
                            ((*key).to_owned(), value)
                        })
                        .collect(),
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        self.operated(
            principal,
            Operation::Paged,
            Some(page.channel),
            None,
            rows.len(),
        );
        Ok(rows)
    }

    /// The bytes kept of the row set aside under `digest`, framed as the
    /// intake received them, for forensics: a capability of its own, since
    /// they hold the payload and its wrapped data key. None when there is no
    /// such row or its bytes are gone.
    ///
    /// # Errors
    /// [`Error`] when the database cannot be read.
    pub async fn set_aside_message(
        &self,
        principal: &str,
        digest: &str,
    ) -> Result<Option<Vec<u8>>, Error> {
        let sql = format!(
            "SELECT message FROM {}_rejects WHERE digest = $1::text::uuid",
            self.table
        );
        if !intake::is_uuid(digest) {
            return Err(Error::Refused("a digest is a uuid"));
        }
        let bytes = self
            .pool
            .session(async |session: P::Session| {
                Ok::<_, Error>(
                    session
                        .connection()
                        .query_opt(&sql, &[&digest])
                        .await?
                        .and_then(|row| row.get::<_, Option<Vec<u8>>>(0)),
                )
            })
            .await?;
        self.operated(
            principal,
            Operation::Read,
            None,
            Some(digest),
            usize::from(bytes.is_some()),
        );
        Ok(bytes)
    }

    /// Tells the observer of a call of the surface, once it has committed.
    pub(super) fn operated(
        &self,
        principal: &str,
        operation: Operation,
        channel: Option<&str>,
        digest: Option<&str>,
        rows: usize,
    ) {
        self.observer.on_operated(&Operated {
            principal,
            operation,
            channel,
            digest,
            rows: u64::try_from(rows).unwrap_or(u64::MAX),
        });
    }
}

/// What a readmit did with a row set aside.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Readmission {
    /// The intake took the message, stored or found stored: the row is closed
    /// as readmitted, its bytes gone.
    Closed(Admission),
    /// The intake set it aside again: the row stays open, seen once more.
    StillSetAside,
}

/// What a bulk readmit did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Readmitted {
    /// Rows the intake took, now closed.
    pub closed: u64,
    /// Rows the intake set aside again, still open.
    pub still_set_aside: u64,
}

/// How many rows a bulk readmit reads at a time.
const READMIT_BATCH: i64 = 100;

/// The cause a parked row was parked under, for [`PgInbox::unpark_all`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParkedUnder<'a> {
    /// This cause.
    Cause(&'a str),
    /// None recorded: parked before causes were (ADR-0040 decision 8).
    Unrecorded,
}

/// Which parked rows [`PgInbox::unpark_all`] re-arms, and how.
#[derive(Clone, Copy, Debug)]
pub struct Unparking<'a> {
    /// Their channel, without its key.
    pub channel: &'a str,
    /// The cause they were parked under.
    pub cause: ParkedUnder<'a>,
    /// Their Tenant, when only one's.
    pub tenant: Option<&'a str>,
    /// Parked within this long before the call.
    pub within: Duration,
    /// The failures each may have before it parks again: a canary of one
    /// re-parks a row at its first failure, so that an early re-arm costs
    /// one attempt a row. Under unlimited attempts, none parks again.
    pub tries: u32,
    /// The most rows re-armed.
    pub limit: u32,
    /// Whether a row overtaken by a processed row of its stream at a higher
    /// position is re-armed too; otherwise it stays parked, counted.
    pub overtaken: bool,
}

/// What [`PgInbox::unpark_all`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UnparkedAll {
    /// Rows re-armed.
    pub unparked: u64,
    /// Rows left parked because they were overtaken.
    pub overtaken: u64,
    /// The end of the window, the database's clock at the call.
    pub as_of: String,
}

/// A batch of a bulk unpark: the parked rows of a channel on the failure
/// ground, parked within the window ending at `$3`, after the cursor, by
/// the parked index; each told whether a processed row of its stream at a
/// higher position overtook it. It reads no clock: the window is the
/// call's.
pub(super) fn unpark_batch(table: &str) -> String {
    format!(
        "WITH chosen AS ( \
            SELECT r.ctid AS place, r.received_position, \
                   EXISTS (SELECT 1 FROM {table} later \
                           WHERE later.tenant_id = r.tenant_id AND later.stream_type = r.stream_type \
                             AND later.stream_id = r.stream_id \
                             AND later.stream_position > r.stream_position \
                             AND later.processed_position IS NOT NULL) AS overtaken \
            FROM {table} r \
            WHERE {channel} = $1 AND r.parked_at IS NOT NULL AND r.received_position > $2 \
              AND r.unreadable_at IS NULL AND r.waiting_for IS NULL \
              AND r.parked_at <= $3::text::timestamptz \
              AND r.parked_at >= $3::text::timestamptz - make_interval(secs => $4::double precision) \
              AND CASE WHEN $5::text IS NULL THEN r.parked_cause IS NULL \
                       ELSE r.parked_cause = $5::text END \
              AND ($6::text IS NULL OR r.tenant_id = $6::text) \
            ORDER BY r.received_position LIMIT $7 \
            FOR UPDATE OF r SKIP LOCKED \
         ), unparked AS ( \
            UPDATE {table} SET parked_at = NULL, parked_cause = NULL, unavailable_since = NULL, \
                next_attempt_at = NULL, attempts = GREATEST(attempts, $8::integer) \
            WHERE ctid IN (SELECT place FROM chosen WHERE $9::boolean OR NOT overtaken) \
            RETURNING 1 \
         ) \
         SELECT (SELECT count(*) FROM chosen), (SELECT count(*) FROM unparked), \
                (SELECT count(*) FROM chosen WHERE overtaken AND NOT $9::boolean), \
                (SELECT max(received_position) FROM chosen)",
        channel = CHANNEL_OF_ROW.replace("uri", "r.uri")
    )
}

/// A batch of a bulk readmit: the open rows of a channel and a reason first
/// seen within the window ending at `$3`, after the cursor. It reads no
/// clock: the window is the call's, read once.
fn readmit_batch(table: &str) -> String {
    format!(
        "SELECT digest::text, first_seen_at::text FROM {table}_rejects \
         WHERE disposed_at IS NULL AND channel = $1 AND reason = $2 \
           AND first_seen_at <= $3::text::timestamptz \
           AND first_seen_at >= $3::text::timestamptz \
                                - make_interval(secs => $4::double precision) \
           AND ($5::text IS NULL \
                OR (first_seen_at, digest) > ($5::text::timestamptz, $6::text::uuid)) \
         ORDER BY first_seen_at, digest \
         LIMIT $7"
    )
}

impl<P, O> PgInbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: InboxObserver,
{
    /// Takes the message kept under `digest` through the intake again: after
    /// a fix on the consuming side, or for an identity taken whose holder
    /// has gone (ADR-0036 R7). Stored or found stored, the row is closed as
    /// readmitted, in a transaction of its own after the intake's. None when
    /// there is no open row under `digest`.
    ///
    /// # Errors
    /// [`Error::Refused`] when the row's channel does not readmit, or its
    /// bytes are gone; [`Error`] when the environment fails, the row left
    /// open.
    pub async fn readmit(
        &self,
        principal: &str,
        digest: &str,
    ) -> Result<Option<Readmission>, Error> {
        if !intake::is_uuid(digest) {
            return Err(Error::Refused("a digest is a uuid"));
        }
        let sql = format!(
            "SELECT channel, message FROM {}_rejects              WHERE digest = $1::text::uuid AND disposed_at IS NULL",
            self.table
        );
        let row = self
            .pool
            .session(async |session: P::Session| {
                Ok::<_, Error>(
                    session
                        .connection()
                        .query_opt(&sql, &[&digest])
                        .await?
                        .map(|row| (row.get::<_, String>(0), row.get::<_, Option<Vec<u8>>>(1))),
                )
            })
            .await?;
        let Some((channel, bytes)) = row else {
            return Ok(None);
        };
        if !(self.readmission)(&channel) {
            return Err(Error::Refused(
                "the channel does not readmit its rows set aside",
            ));
        }
        if bytes.as_deref().is_some_and(intake::is_cut) {
            return Err(Error::Refused(
                "the row was past the intake's bounds and is kept cut",
            ));
        }
        let (destination, message) = bytes
            .as_deref()
            .and_then(intake::unframe)
            .ok_or(Error::Refused("the row's bytes are gone"))?;
        let readmission = match self.intake(&destination, &message).await? {
            Admission::SetAside { .. } => Readmission::StillSetAside,
            admission => {
                self.close(digest, "readmitted", principal, None).await?;
                Readmission::Closed(admission)
            }
        };
        let closed = matches!(readmission, Readmission::Closed(_));
        self.operated(
            principal,
            Operation::Readmitted,
            Some(&channel),
            Some(digest),
            usize::from(closed),
        );
        log::info!(
            "inbox: {} readmitted the message {digest} of {channel}: {}",
            intake::sanitized(Some(principal.as_bytes()), intake::DERIVED_WIDTH)
                .unwrap_or_default(),
            if closed { "taken" } else { "set aside again" }
        );
        Ok(Some(readmission))
    }

    /// Readmits the open rows of `channel` set aside for `reason` and first
    /// seen within `window` before the call, oldest first, each message in a
    /// transaction of its own: for a flood whose cause on the consuming side
    /// was fixed.
    ///
    /// The window's both ends are read once, from the database's clock, at
    /// the call: a row set aside while the call runs is not the flood it
    /// was called for, and a flood still arriving cannot keep it running.
    ///
    /// # Errors
    /// As [`PgInbox::readmit`], at the first row that fails; the rows
    /// before it stay readmitted.
    pub async fn readmit_all(
        &self,
        principal: &str,
        channel: &str,
        reason: Reason,
        window: Duration,
    ) -> Result<Readmitted, Error> {
        if !(self.readmission)(channel) {
            return Err(Error::Refused(
                "the channel does not readmit its rows set aside",
            ));
        }
        let as_of = self
            .pool
            .session(async |session: P::Session| {
                Ok::<_, Error>(
                    session
                        .connection()
                        .query_one("SELECT statement_timestamp()::text", &[])
                        .await?
                        .get::<_, String>(0),
                )
            })
            .await?;
        let sql = readmit_batch(self.table.as_str());
        let mut done = Readmitted::default();
        let mut after: Option<(String, String)> = None;
        loop {
            let (after_digest, after_at) = after.clone().unzip();
            let batch = self
                .pool
                .session(async |session: P::Session| {
                    Ok::<_, Error>(
                        session
                            .connection()
                            .query(
                                &sql,
                                &[
                                    &channel,
                                    &reason.as_str(),
                                    &as_of,
                                    &window.as_secs_f64(),
                                    &after_at,
                                    &after_digest,
                                    &READMIT_BATCH,
                                ],
                            )
                            .await?
                            .iter()
                            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
                            .collect::<Vec<_>>(),
                    )
                })
                .await?;
            for (digest, _) in &batch {
                match self.readmit(principal, digest).await? {
                    Some(Readmission::Closed(_)) => done.closed += 1,
                    Some(Readmission::StillSetAside) => done.still_set_aside += 1,
                    None => {}
                }
            }
            if i64::try_from(batch.len()).unwrap_or(i64::MAX) < READMIT_BATCH {
                return Ok(done);
            }
            after = batch.last().cloned();
        }
    }

    /// Parks the heads of `channel` held for their stage's outage under
    /// `cause` — of `tenant` only, when given — so that the rows behind them
    /// go on (ADR-0040 decision 8): the operator's choice of out-of-order
    /// over the stall, each row parked under its cause and re-armed later by
    /// an unpark. Only a head can hold, and a slot has a head for each of its
    /// Tenants (ADR-0041 decision 2), so the call reads the heads alone, one
    /// probe of the Tenants' head index each, from one Tenant to the next as
    /// a walk goes. Under rotation a hold stops its own Tenant's rows of its
    /// slot, not the slot: the rows a park lets go on are that Tenant's, and
    /// while the outage lasts they meet it and hold in turn (ADR-0040 § Owed
    /// 6). A slot a dispatcher holds right now is passed by, so a call may
    /// park part of what holds; a second call parks what it passed, and the
    /// rows held since. The slots it parks in it holds itself, and a row is
    /// parked only if it is still in the queue and still held when the
    /// statement reaches it: a row a dispatcher marked or let go after the
    /// statement's snapshot is left.
    /// Returns how many rows were parked.
    ///
    /// # Errors
    /// [`Error`] when the environment fails, nothing parked.
    pub async fn park_held(
        &self,
        principal: &str,
        channel: &str,
        cause: &str,
        tenant: Option<&str>,
    ) -> Result<u64, Error> {
        let sql = format!(
            "WITH RECURSIVE heads AS ( \
                SELECT h.* FROM {table}_slots s \
                CROSS JOIN LATERAL ( \
                    SELECT r.slot, r.tenant_id, r.ctid AS place, r.unavailable_since, \
                           r.last_cause, {CHANNEL_OF_ROW} AS channel \
                    FROM {table} r WHERE r.slot = s.slot AND {QUEUE} \
                    ORDER BY r.tenant_id, r.received_position LIMIT 1) h \
                UNION ALL \
                SELECT n.* FROM heads p \
                CROSS JOIN LATERAL ( \
                    SELECT r.slot, r.tenant_id, r.ctid AS place, r.unavailable_since, \
                           r.last_cause, {CHANNEL_OF_ROW} AS channel \
                    FROM {table} r \
                    WHERE r.slot = p.slot AND {QUEUE} AND r.tenant_id > p.tenant_id \
                    ORDER BY r.tenant_id, r.received_position LIMIT 1) n \
             ), held AS ( \
                SELECT h.place FROM heads h JOIN {table}_slots s ON s.slot = h.slot \
                WHERE h.unavailable_since IS NOT NULL AND h.last_cause = $2 AND h.channel = $1 \
                  AND ($3::text IS NULL OR h.tenant_id = $3::text) \
                FOR UPDATE OF s SKIP LOCKED \
             ) \
             UPDATE {table} SET parked_at = CURRENT_TIMESTAMP, parked_cause = last_cause, \
                 unavailable_since = NULL, held_since = NULL \
             WHERE ctid IN (SELECT place FROM held) AND {QUEUE} AND unavailable_since IS NOT NULL",
            table = self.table
        );
        let parked = self
            .pool
            .session(async |session: P::Session| {
                Ok::<_, Error>(
                    session
                        .connection()
                        .execute(&sql, &[&channel, &cause, &tenant])
                        .await?,
                )
            })
            .await?;
        self.operated(
            principal,
            Operation::ParkedHeld,
            Some(channel),
            None,
            usize::try_from(parked).unwrap_or(usize::MAX),
        );
        log::warn!(
            "inbox: {} parked {parked} rows of {} held for their stage's outage",
            intake::sanitized(Some(principal.as_bytes()), intake::DERIVED_WIDTH)
                .unwrap_or_default(),
            intake::sanitized(Some(channel.as_bytes()), intake::CHANNEL_WIDTH).unwrap_or_default(),
        );
        Ok(parked)
    }

    /// Re-arms the parked rows `unparking` names (ADR-0040 decision 8):
    /// those of a channel, on the failure ground, parked under a cause — of
    /// one Tenant, when named — within a window before the call, oldest
    /// first, a batch at a time, each its own transaction, the observer
    /// told after each commits. A row re-armed loses its park and its cause,
    /// keeps its attempts, and has at most `tries` failures left. A row a
    /// later row of its stream overtook stays parked unless asked for: its
    /// stream has gone on without it. The window's ends are read once, as
    /// [`PgInbox::readmit_all`]'s are.
    ///
    /// # Errors
    /// [`Error::Refused`] when the window is longer than the channel keeps
    /// its processed rows and an overtaken row is to stay parked: an
    /// overtaking row forgotten could not be told. Asked to re-arm the
    /// overtaken too, it needs no overtaker, and takes any window: so a park
    /// older than the channel's retention has a way back in bulk.
    /// [`Error`] when the environment fails, the batches before it re-armed.
    pub async fn unpark_all(
        &self,
        principal: &str,
        unparking: &Unparking<'_>,
    ) -> Result<UnparkedAll, Error> {
        if !unparking.overtaken
            && self
                .retention
                .windows
                .get(unparking.channel)
                .is_some_and(|kept| unparking.within > *kept)
        {
            return Err(Error::Refused(
                "the window is longer than the channel keeps its processed rows",
            ));
        }
        let as_of = self
            .pool
            .session(async |session: P::Session| {
                Ok::<_, Error>(
                    session
                        .connection()
                        .query_one("SELECT statement_timestamp()::text", &[])
                        .await?
                        .get::<_, String>(0),
                )
            })
            .await?;
        let sql = unpark_batch(self.table.as_str());
        let cause = match unparking.cause {
            ParkedUnder::Cause(cause) => Some(cause),
            ParkedUnder::Unrecorded => None,
        };
        let left = i32::try_from(self.retries.max_attempts.saturating_sub(unparking.tries))
            .unwrap_or(i32::MAX);
        let mut done = UnparkedAll {
            as_of: as_of.clone(),
            ..UnparkedAll::default()
        };
        let mut after = i64::MIN;
        // Each batch reads at most what the limit leaves, so that the last
        // reads none and ends the walk.
        loop {
            let room = u64::from(unparking.limit).saturating_sub(done.unparked);
            let batch =
                i64::try_from(room.min(READMIT_BATCH.unsigned_abs())).unwrap_or(READMIT_BATCH);
            let (chosen, unparked, overtaken, last) = self
                .pool
                .session(async |session: P::Session| {
                    let row = session
                        .connection()
                        .query_one(
                            &sql,
                            &[
                                &unparking.channel,
                                &after,
                                &as_of,
                                &unparking.within.as_secs_f64(),
                                &cause,
                                &unparking.tenant,
                                &batch,
                                &left,
                                &unparking.overtaken,
                            ],
                        )
                        .await?;
                    Ok::<_, Error>((
                        row.get::<_, i64>(0),
                        row.get::<_, i64>(1),
                        row.get::<_, i64>(2),
                        row.get::<_, Option<i64>>(3),
                    ))
                })
                .await?;
            let unparked = u64::try_from(unparked).unwrap_or_default();
            done.unparked += unparked;
            done.overtaken += u64::try_from(overtaken).unwrap_or_default();
            self.operated(
                principal,
                Operation::UnparkedAll,
                Some(unparking.channel),
                None,
                usize::try_from(unparked).unwrap_or(usize::MAX),
            );
            match last {
                Some(last) if chosen >= batch => after = last,
                _ => break,
            }
        }
        log::warn!(
            "inbox: {} re-armed {} parked rows of {}, {} overtaken left parked",
            intake::sanitized(Some(principal.as_bytes()), intake::DERIVED_WIDTH)
                .unwrap_or_default(),
            done.unparked,
            intake::sanitized(Some(unparking.channel.as_bytes()), intake::CHANNEL_WIDTH)
                .unwrap_or_default(),
            done.overtaken,
        );
        Ok(done)
    }

    /// Closes the open row under `digest` as discarded, for `because`: a
    /// tombstone, its bytes gone. A later repeat of the same bytes stays
    /// discarded, and is counted. Returns whether an open row was closed.
    ///
    /// # Errors
    /// [`Error`] when the database cannot be written.
    pub async fn discard(
        &self,
        principal: &str,
        digest: &str,
        because: &str,
    ) -> Result<bool, Error> {
        if !intake::is_uuid(digest) {
            return Err(Error::Refused("a digest is a uuid"));
        }
        let closed = self
            .close(digest, "discarded", principal, Some(because))
            .await?;
        self.operated(
            principal,
            Operation::Discarded,
            None,
            Some(digest),
            usize::from(closed),
        );
        if closed {
            log::info!(
                "inbox: {} discarded the message {digest} set aside",
                intake::sanitized(Some(principal.as_bytes()), intake::DERIVED_WIDTH)
                    .unwrap_or_default()
            );
        }
        Ok(closed)
    }

    /// Closes an open row set aside, in a transaction of its own; its bytes
    /// go. Returns whether it was open.
    async fn close(
        &self,
        digest: &str,
        disposition: &str,
        principal: &str,
        because: Option<&str>,
    ) -> Result<bool, Error> {
        let sql = format!(
            "UPDATE {}_rejects SET disposed_at = statement_timestamp(), disposition = $2,                  disposed_by = $3, disposed_because = $4, message = NULL              WHERE digest = $1::text::uuid AND disposed_at IS NULL",
            self.table
        );
        let closed = self
            .pool
            .session(async |session: P::Session| {
                Ok::<_, Error>(
                    session
                        .connection()
                        .execute(&sql, &[&digest, &disposition, &principal, &because])
                        .await?,
                )
            })
            .await?;
        Ok(closed == 1)
    }
}

/// The reason a row names, which the inbox wrote.
fn reason_of(text: &str) -> Result<Reason, Error> {
    Reason::parse(text).ok_or_else(|| Error::Malformed(format!("a row set aside for `{text}`")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A batch reads no clock, so that the window cannot move while a bulk
    /// readmit or unpark runs: its ends are the call's.
    #[test]
    fn a_bulk_readmit_s_batch_reads_no_clock() {
        for batch in [readmit_batch("inbox"), unpark_batch("inbox")] {
            reads_no_clock(&batch.to_lowercase());
        }
    }

    fn reads_no_clock(batch: &str) {
        for clock in [
            "statement_timestamp",
            "clock_timestamp",
            "transaction_timestamp",
            "now(",
            "current_timestamp",
            "localtimestamp",
            "timeofday",
        ] {
            assert!(!batch.contains(clock), "{clock} in {batch}");
        }
    }
}
