//! The PostgreSQL event store: one row per event in `event_log`.
//!
//! # The table
//!
//! `event_log (tenant_id, stream_type, stream_id, stream_position,
//! event_type, event_version, payload, metadata)`, primary key `(tenant_id,
//! stream_type, stream_id, stream_position)` — the sources' table. The
//! primary key is the optimistic lock: an append after position `n` writes
//! `n + 1`, and if another writer wrote `n + 1` first, the key refuses it
//! and the append is [`Error::ConcurrentUpdate`]; the caller's transaction
//! rolls back with it. `metadata->>'event_id'` is unique across the table.
//! `payload` is what the stream's codec made: ciphertext, under
//! [`Encrypted`][crate::Encrypted].

use ascetic_ddd_session::Session;
use ascetic_ddd_session::pg::{Identifier, PgAccess};
use serde_json::Value;
use tokio_postgres::Row;
use tokio_postgres::error::SqlState;

use crate::codecs::Codecs;
use crate::domain::{Appended, Encoded, EventCodec, EventMeta, Recorded, StreamId};
use crate::error::Error;
use crate::port::EventStore;

/// The event store over a table, with codecs `C` for the payload and `F`
/// for the events.
pub struct PgEventStore<C, F> {
    codecs: C,
    format: F,
    table: Identifier,
}

impl<C, F> PgEventStore<C, F> {
    /// A store in table `event_log`, writing payloads with `codecs` and
    /// events with `format`.
    pub fn new(codecs: C, format: F) -> Self {
        PgEventStore {
            codecs,
            format,
            table: Identifier::new("event_log").expect("a literal identifier"),
        }
    }

    /// The same store in another table. The name goes into SQL as text, so
    /// it is an [`Identifier`]: parsed once, safe after.
    pub fn with_table(self, table: Identifier) -> Self {
        PgEventStore { table, ..self }
    }

    /// The table the events are in.
    pub fn table(&self) -> &Identifier {
        &self.table
    }

    /// Creates the table if it is not there, in one transaction under an
    /// advisory lock on the table's name.
    pub async fn setup<S>(&self, session: &S) -> Result<(), Error>
    where
        S: Session + PgAccess + Sync,
    {
        let table = &self.table;
        let ddl = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table} (
                "tenant_id" VARCHAR(128) NOT NULL,
                "stream_type" VARCHAR(128) NOT NULL,
                "stream_id" JSONB NOT NULL,
                "stream_position" INTEGER NOT NULL,
                "event_type" VARCHAR(60) NOT NULL,
                "event_version" SMALLINT NOT NULL,
                "payload" BYTEA NOT NULL,
                "metadata" JSONB NULL,
                CONSTRAINT {table}_pk PRIMARY KEY ("tenant_id", "stream_type", "stream_id", "stream_position")
            );
            CREATE UNIQUE INDEX IF NOT EXISTS {table}__event_id_uniq
                ON {table} (((metadata->>'event_id')::uuid));
            "#
        );
        session
            .atomic(async |tx| {
                tx.connection()
                    .execute(
                        "SELECT pg_advisory_xact_lock(hashtext($1))",
                        &[&table.as_str()],
                    )
                    .await?;
                tx.connection().batch_execute(&ddl).await?;
                Ok::<_, Error>(())
            })
            .await
    }
}

/// The version column is `SMALLINT`.
fn stored_event_version(version: i32) -> Result<i16, Error> {
    i16::try_from(version)
        .map_err(|_| Error::Malformed(format!("event version {version} does not fit the column")))
}

/// A row of the table, read back into an event.
fn recorded<E>(
    row: &Row,
    codec: &dyn crate::domain::PayloadCodec,
    format: &impl EventCodec<E>,
) -> Result<Recorded<E>, Error> {
    let position: i32 = row.try_get("stream_position")?;
    let event_type: String = row.try_get("event_type")?;
    let event_version: i16 = row.try_get("event_version")?;
    let payload: Vec<u8> = row.try_get("payload")?;
    let metadata: Option<Value> = row.try_get("metadata")?;
    let payload = codec.decode(&payload).map_err(Error::Codec)?;
    let event = format
        .decode(Encoded {
            event_type,
            event_version: i32::from(event_version),
            payload,
        })
        .map_err(Error::Format)?;
    let meta = match metadata {
        Some(metadata) => serde_json::from_value(metadata).map_err(|error| {
            Error::Malformed(format!("the metadata is not an event's: {error}"))
        })?,
        None => EventMeta::default(),
    };
    Ok(Recorded {
        position,
        event,
        meta,
    })
}

impl<S, E, C, F> EventStore<S, E> for PgEventStore<C, F>
where
    S: Session + PgAccess + Sync,
    E: Send + Sync,
    C: Codecs<S>,
    F: EventCodec<E>,
{
    async fn append(
        &self,
        session: &S,
        stream: &StreamId,
        expected: i32,
        events: &[E],
        meta: &EventMeta,
    ) -> Result<Vec<Appended>, Error> {
        let codec = self.codecs.for_writing(session, stream).await?;
        let sql = format!(
            "INSERT INTO {} (tenant_id, stream_type, stream_id, stream_position, \
             event_type, event_version, payload, metadata) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            self.table
        );
        let pk = format!("{}_pk", self.table);
        let mut appended = Vec::with_capacity(events.len());
        let mut causation_id = meta.causation_id.clone();
        for (i, event) in events.iter().enumerate() {
            let position = expected + 1 + i as i32;
            let encoded = self.format.encode(event).map_err(Error::Format)?;
            let payload = codec.encode(&encoded.payload).map_err(Error::Codec)?;
            let event_meta = EventMeta {
                event_id: Some(uuid::Uuid::new_v4().to_string()),
                causation_id: causation_id.clone(),
                ..meta.clone()
            };
            let metadata = serde_json::to_value(&event_meta).map_err(|error| {
                Error::Malformed(format!("the metadata cannot be written: {error}"))
            })?;
            let written = session
                .connection()
                .execute(
                    &sql,
                    &[
                        &stream.tenant_id(),
                        &stream.stream_type(),
                        stream.stream_id(),
                        &position,
                        &encoded.event_type,
                        &stored_event_version(encoded.event_version)?,
                        &payload,
                        &metadata,
                    ],
                )
                .await;
            if let Err(error) = written {
                let on_the_key = error.code() == Some(&SqlState::UNIQUE_VIOLATION)
                    && error
                        .as_db_error()
                        .and_then(|db| db.constraint())
                        .is_some_and(|constraint| constraint == pk);
                return Err(if on_the_key {
                    Error::ConcurrentUpdate {
                        stream: stream.clone(),
                        position,
                    }
                } else {
                    Error::Database(error)
                });
            }
            causation_id = event_meta.event_id.clone();
            appended.push(Appended {
                position,
                meta: event_meta,
            });
        }
        Ok(appended)
    }

    async fn load(
        &self,
        session: &S,
        stream: &StreamId,
        since: i32,
    ) -> Result<Vec<Recorded<E>>, Error> {
        let sql = format!(
            "SELECT stream_position, event_type, event_version, payload, metadata FROM {} \
             WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3 AND stream_position > $4 \
             ORDER BY stream_position",
            self.table
        );
        let rows = session
            .connection()
            .query(
                &sql,
                &[
                    &stream.tenant_id(),
                    &stream.stream_type(),
                    stream.stream_id(),
                    &since,
                ],
            )
            .await?;
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let codec = self.codecs.for_reading(session, stream).await?;
        rows.iter()
            .map(|row| recorded(row, codec.as_ref(), &self.format))
            .collect()
    }
}
