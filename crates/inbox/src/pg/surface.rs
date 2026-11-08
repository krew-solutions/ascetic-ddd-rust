//! The operator's surface of the rows set aside (ADR-0036 decision 4, R7):
//! counts, a page and the kept bytes; readmit, readmit in bulk and discard.
//! Every call names its principal, reads included, and the observer is told
//! of it once it has committed, so that a host audits who read and did
//! what. Each disposition runs in a transaction of its own, never the
//! caller's, whose commit the library could not see.

use std::time::Duration;

use ascetic_ddd_session::{PgAccess, SessionPool};

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
    /// seen within `window`, oldest first, each message in a transaction of
    /// its own: for a flood whose cause on the consuming side was fixed.
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
        let sql = format!(
            "SELECT digest::text, first_seen_at::text FROM {}_rejects              WHERE disposed_at IS NULL AND channel = $1 AND reason = $2                AND first_seen_at >= statement_timestamp() - make_interval(secs => $3::double precision)                AND ($4::text IS NULL                     OR (first_seen_at, digest) > ($4::text::timestamptz, $5::text::uuid))              ORDER BY first_seen_at, digest              LIMIT $6",
            self.table
        );
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
