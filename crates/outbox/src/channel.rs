//! The outbox as a channel of the bus (ADR-0003).
//!
//! Registered under [`OUTBOX_SCHEME`] as [`OutboxChannel`], the outbox offers two things. A
//! *transactional producer*, from [`PgOutbox::producer`]: it publishes inside
//! the caller's transaction, and the URI it was built for is the destination
//! stored with the row — `kafka://orders/order-7`, key included. And a
//! *consumer*, from the bus: `bus.consumer("outbox://all", group, …)` runs
//! the dispatcher for that consumer group and hands every committed row to
//! the handler as a wire message whose `destination` header is the row's
//! URI. A [`Bridge`][ascetic_ddd_bus::Bridge] from that consumer to
//! `Target::Header("destination")` is the dispatcher process.
//!
//! Headers become `metadata`: each header is a string field of the JSONB
//! object, so `message_id` keeps its unique index, and every metadata field
//! comes back as a header. The channel name after `outbox://` is not read
//! yet: the whole outbox is one channel.

use std::sync::Arc;

use ascetic_ddd_bus::{
    Adapter, Error as BusError, Handler, Message, Subscription, TransactionalProducer,
    TransactionalWireProducer, WireConsumer, WireProducer, uri,
};
use ascetic_ddd_session::{PgAccess, SessionPool};
use futures::future::BoxFuture;
use serde_json::{Map, Value};
use tokio::runtime::Handle;
use tokio::sync::Notify;

use crate::message::OutboxMessage;
use crate::pg::{PgOutbox, Selection, Workers};

/// The scheme the outbox is registered under.
pub const OUTBOX_SCHEME: &str = "outbox";

impl<P> PgOutbox<P>
where
    P: SessionPool + Send + Sync + 'static,
    P::Session: PgAccess + Sync,
{
    /// A producer to `destination` that publishes inside the caller's
    /// transaction. `destination` is a URI of another channel,
    /// `kafka://orders/order-7`, stored with the row for the dispatcher.
    pub fn producer<T>(
        self: &Arc<Self>,
        destination: &str,
        encode: impl Fn(&T) -> Message + Send + Sync + 'static,
    ) -> TransactionalProducer<T, P::Session> {
        let wire = OutboxProducer {
            table: self.outbox_table().to_owned(),
            destination: destination.to_owned(),
        };
        TransactionalProducer::new(Box::new(wire), encode)
    }
}

struct OutboxProducer {
    table: String,
    destination: String,
}

impl<S: PgAccess + Sync> TransactionalWireProducer<S> for OutboxProducer {
    fn publish<'a>(
        &'a self,
        session: &'a S,
        message: Message,
    ) -> BoxFuture<'a, Result<(), BusError>> {
        Box::pin(async move {
            let destination = match (uri::key(&self.destination), message.key()) {
                (None, Some(key)) => format!("{}/{}", self.destination, text(key)?),
                _ => self.destination.clone(),
            };
            let metadata = metadata_of(&message)?;
            let sql = format!(
                "INSERT INTO {} (uri, payload, metadata, transaction_id) \
                 VALUES ($1, $2, $3, pg_current_xact_id())",
                self.table
            );
            session
                .connection()
                .execute(&sql, &[&destination, &message.payload(), &metadata])
                .await
                .map_err(|error| BusError::Transport(Box::new(error)))?;
            Ok(())
        })
    }
}

/// The outbox as a bus adapter: what [`Bus::register`][ascetic_ddd_bus::Bus::register] takes.
pub struct OutboxChannel<P>(Arc<PgOutbox<P>>);

impl<P> PgOutbox<P> {
    /// The outbox as a channel of the bus.
    pub fn channel(self: &Arc<Self>) -> OutboxChannel<P> {
        OutboxChannel(Arc::clone(self))
    }
}

impl<P> Adapter for OutboxChannel<P>
where
    P: SessionPool + Send + Sync + 'static,
    P::Session: PgAccess + Sync,
{
    /// A consumer of the outbox channel: the dispatcher for `group`.
    fn consumer(&self, _uri: &str, group: &str) -> Result<Box<dyn WireConsumer>, BusError> {
        Ok(Box::new(OutboxConsumer {
            outbox: Arc::clone(&self.0),
            group: group.to_owned(),
        }))
    }

    /// The outbox publishes only inside a transaction: see [`PgOutbox::producer`].
    fn producer(&self, uri: &str) -> Result<Box<dyn WireProducer>, BusError> {
        Err(BusError::Transport(
            format!(
                "`{uri}`: the outbox publishes only inside a transaction; use PgOutbox::producer"
            )
            .into(),
        ))
    }
}

struct OutboxConsumer<P> {
    outbox: Arc<PgOutbox<P>>,
    group: String,
}

impl<P> WireConsumer for OutboxConsumer<P>
where
    P: SessionPool + Send + Sync + 'static,
    P::Session: PgAccess + Sync,
{
    /// Runs the dispatcher as a task on the current runtime until cancelled.
    /// A batch whose handler fails is rolled back and retried after the poll
    /// interval; the position of the group moves only past messages the
    /// handler accepted. Outside a runtime this is an error, not a panic.
    fn subscribe(&self, handler: Handler) -> Result<Subscription, BusError> {
        let stop = Arc::new(Notify::new());
        let (outbox, group, stopped) = (
            Arc::clone(&self.outbox),
            self.group.clone(),
            Arc::clone(&stop),
        );
        let runtime =
            Handle::try_current().map_err(|error| BusError::Transport(Box::new(error)))?;
        runtime.spawn(async move {
            let selection = Selection::group(&group);
            let workers = Workers {
                poll_interval: outbox.poll_interval(),
                ..Workers::default()
            };
            let subscriber = |row: &OutboxMessage| {
                let handler = Arc::clone(&handler);
                let message = wire_of(row);
                async move { handler(message).await }
            };
            loop {
                let outcome = outbox
                    .run(&subscriber, &selection, workers, stopped.notified())
                    .await;
                match outcome {
                    Ok(()) => break,
                    Err(error) => {
                        log::warn!("outbox[{group}]: dispatch failed, retrying: {error}");
                        tokio::select! {
                            _ = stopped.notified() => break,
                            _ = tokio::time::sleep(workers.poll_interval) => {}
                        }
                    }
                }
            }
        });
        Ok(Subscription::new(move || stop.notify_one()))
    }
}

/// The wire message of a row: payload, key from the destination, headers
/// from the metadata plus the destination itself.
fn wire_of(row: &OutboxMessage) -> Message {
    let message = Message::new(row.payload.clone()).with_header("destination", row.uri.as_bytes());
    let message = match uri::key(&row.uri) {
        Some(key) => message.with_key(key.as_bytes()),
        None => message,
    };
    match &row.metadata {
        Value::Object(fields) => fields.iter().fold(message, |message, (name, value)| {
            let value = match value {
                Value::String(text) => text.clone(),
                other => other.to_string(),
            };
            message.with_header(name.clone(), value)
        }),
        _ => message,
    }
}

/// The metadata of a wire message: one string field per header.
fn metadata_of(message: &Message) -> Result<Value, BusError> {
    message
        .headers()
        .iter()
        .map(|(name, value)| Ok((name.clone(), Value::String(text(value)?.to_owned()))))
        .collect::<Result<Map<_, _>, _>>()
        .map(Value::Object)
}

fn text(bytes: &[u8]) -> Result<&str, BusError> {
    std::str::from_utf8(bytes)
        .map_err(|_| BusError::Transport("the outbox keeps headers and keys as UTF-8 text".into()))
}
