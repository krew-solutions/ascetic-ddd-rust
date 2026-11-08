//! The census of what an inbox holds that ADR-0037 refuses, parks or reads
//! otherwise, taken before a host adopts it (decision 8): by ground, so
//! that each count is a producer, or a row, an operator looks at. It reads
//! no value that may amplify — a structured metadata's numbers — beyond
//! what a predicate selects, and the values it reads it decodes without
//! panicking.

use ascetic_ddd_session::{PgAccess, SessionPool};
use serde_json::Value;

use super::PgInbox;
use super::store::in_domain;
use crate::error::Error;
use crate::message::{CausalDependency, MAX_IDENTITY_BYTES};
use crate::observer::InboxObserver;

/// What a census found, by ground (ADR-0037 decision 8).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Census {
    /// Rows whose stream id holds a number anywhere. `jsonb` kept no
    /// producer's spelling (`1e5` is stored as `100000`), so each such
    /// stream's producer is one to check against the bus's domain.
    pub numbered_stream_ids: u64,
    /// Rows whose stream id reads back outside the bus's domain: the
    /// dispatcher parks them at once, and a dependency naming them is
    /// refused.
    pub stream_ids_outside: u64,
    /// Rows not processed whose dependency list the intake would refuse:
    /// the dispatcher parks them at once.
    pub dependencies_refused: u64,
    /// Rows not processed whose metadata, stored structured before
    /// decision 2, holds a number, serde_json's private keys, or nests past
    /// what the client reads back: they amplify on read, or are parked.
    pub structured_metadata: u64,
    /// Rows waiting for an identity past `MAX_IDENTITY_BYTES`.
    pub waits_past_bound: u64,
}

impl Census {
    /// Whether nothing was found: the release may go.
    #[must_use]
    pub fn is_clear(&self) -> bool {
        *self == Census::default()
    }
}

impl<P, O> PgInbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: InboxObserver,
{
    /// The census of this inbox's table, under the caller's session: the
    /// role that sees every Tenant's rows, since a count under forced row
    /// security would see none of them.
    ///
    /// # Errors
    /// [`Error`] when the database cannot be read.
    pub async fn census(&self, session: &P::Session) -> Result<Census, Error> {
        let table = self.table.as_str();
        let connection = session.connection();
        let count = |rows: &[tokio_postgres::Row]| u64::try_from(rows.len()).unwrap_or(u64::MAX);
        let numbered = connection
            .query(
                &format!(
                    "SELECT stream_id FROM {table} \
                     WHERE jsonb_path_exists(stream_id, 'strict $.** ? (@.type() == \"number\")')"
                ),
                &[],
            )
            .await?;
        let outside = numbered
            .iter()
            .filter(|row| {
                row.try_get::<_, Value>(0)
                    .map_or(true, |stream_id| !in_domain(&stream_id))
            })
            .count();
        let lists = connection
            .query(
                &format!(
                    "SELECT metadata->'causal_dependencies' FROM {table} \
                     WHERE processed_position IS NULL AND metadata ? 'causal_dependencies'"
                ),
                &[],
            )
            .await?;
        let refused = lists
            .iter()
            .filter(|row| match row.try_get::<_, Value>(0) {
                Ok(Value::String(text)) => crate::intake::dependencies_of(&text).is_err(),
                Ok(structured) => crate::intake::dependencies_of(&structured.to_string()).is_err(),
                Err(_) => true,
            })
            .count();
        let structured = connection
            .query_one(
                &format!(
                    "SELECT count(*) FROM {table} WHERE processed_position IS NULL AND ( \
                       jsonb_path_exists(metadata, 'strict $.** ? (@.type() == \"number\")') \
                       OR jsonb_path_exists(metadata, 'strict $.**{{126 to last}}') \
                       OR jsonb_path_exists(metadata, \
                            'strict $.** ? (@.type() == \"object\").keyvalue() \
                             ? (@.key starts with \"$serde_json::private::\")'))"
                ),
                &[],
            )
            .await?
            .get::<_, i64>(0);
        let waits = connection
            .query(
                &format!("SELECT waiting_for FROM {table} WHERE waiting_for IS NOT NULL"),
                &[],
            )
            .await?;
        let past = waits
            .iter()
            .filter(|row| {
                row.try_get::<_, Value>(0)
                    .ok()
                    .and_then(|value| serde_json::from_value::<CausalDependency>(value).ok())
                    .is_none_or(|dependency| dependency.stored_bytes() > MAX_IDENTITY_BYTES)
            })
            .count();
        Ok(Census {
            numbered_stream_ids: count(&numbered),
            stream_ids_outside: u64::try_from(outside).unwrap_or(u64::MAX),
            dependencies_refused: u64::try_from(refused).unwrap_or(u64::MAX),
            structured_metadata: u64::try_from(structured).unwrap_or(u64::MAX),
            waits_past_bound: u64::try_from(past).unwrap_or(u64::MAX),
        })
    }
}
