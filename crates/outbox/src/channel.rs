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
//! comes back as a header.
//!
//! The channel name after `outbox://` is the consumer's selection: `all`
//! takes every row; a scheme takes the rows addressed to it,
//! `outbox://inbox` those to `inbox://…`. Each is a lane with positions of
//! its own, so that a destination's outage holds back no other lane's rows
//! (ADR-0033).

use std::sync::Arc;

use ascetic_ddd_bus::{
    Adapter, Error as BusError, Handler, Message, Subscription, TransactionalProducer,
    TransactionalWireProducer, WireConsumer, WireProducer, handling, uri,
};
use ascetic_ddd_session::{PgAccess, SessionPool};
use futures::future::BoxFuture;
use serde_json::{Map, Value};
use tokio::runtime::Handle;
use tokio::sync::Notify;

use crate::message::OutboxMessage;
use crate::observer::OutboxObserver;
use crate::pg::{PgOutbox, Selection};
use crate::port::Outbox;

/// The scheme the outbox is registered under.
pub const OUTBOX_SCHEME: &str = "outbox";

impl<P, O> PgOutbox<P, O>
where
    P: SessionPool + Send + Sync + 'static,
    P::Session: PgAccess + Sync,
    O: OutboxObserver + 'static,
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
            outbox: Arc::clone(self),
            destination: destination.to_owned(),
        };
        TransactionalProducer::new(Box::new(wire), encode)
    }
}

/// Publishes through [`Outbox::publish`], so that the row is written in one
/// place and the observer sees it.
struct OutboxProducer<P, O> {
    outbox: Arc<PgOutbox<P, O>>,
    destination: String,
}

impl<P, O> TransactionalWireProducer<P::Session> for OutboxProducer<P, O>
where
    P: SessionPool + Send + Sync + 'static,
    P::Session: PgAccess + Sync,
    O: OutboxObserver + 'static,
{
    fn publish<'a>(
        &'a self,
        session: &'a P::Session,
        message: Message,
    ) -> BoxFuture<'a, Result<(), BusError>> {
        Box::pin(async move {
            // What the outbox would keep otherwise than the inbox reads it is
            // refused here, the only place that sees the wire's headers, and
            // aborts the transaction as a refusal at the outbox does
            // (ADR-0038).
            let refused = async |reason| {
                let message_id = message
                    .header("message_id")
                    .and_then(|id| std::str::from_utf8(id).ok())
                    .map(str::to_owned);
                BusError::Transport(Box::new(
                    self.outbox
                        .refuse_message(session, &self.destination, message_id, reason)
                        .await,
                ))
            };
            let destination = match (uri::key(&self.destination), message.key()) {
                (None, Some(key)) => match std::str::from_utf8(key) {
                    Ok(key) => format!("{}/{key}", self.destination),
                    Err(_) => return Err(refused("the key is not UTF-8").await),
                },
                _ => self.destination.clone(),
            };
            let metadata = match metadata_of(&message) {
                Ok(metadata) => metadata,
                Err(reason) => return Err(refused(reason).await),
            };
            let row = OutboxMessage::new(destination, message.payload().to_vec(), metadata);
            self.outbox
                .publish(session, &row)
                .await
                .map_err(|error| BusError::Transport(Box::new(error)))
        })
    }
}

/// The outbox as a bus adapter: what [`Bus::register`][ascetic_ddd_bus::Bus::register] takes.
pub struct OutboxChannel<P, O = ()>(Arc<PgOutbox<P, O>>);

impl<P, O> PgOutbox<P, O> {
    /// The outbox as a channel of the bus.
    pub fn channel(self: &Arc<Self>) -> OutboxChannel<P, O> {
        OutboxChannel(Arc::clone(self))
    }
}

impl<P, O> Adapter for OutboxChannel<P, O>
where
    P: SessionPool + Send + Sync + 'static,
    P::Session: PgAccess + Sync,
    O: OutboxObserver + 'static,
{
    /// A consumer of the outbox channel: the dispatcher for `group`, over
    /// the lane `uri` names.
    fn consumer(&self, uri: &str, group: &str) -> Result<Box<dyn WireConsumer>, BusError> {
        Ok(Box::new(OutboxConsumer {
            outbox: Arc::clone(&self.0),
            group: group.to_owned(),
            lane: lane_of(uri)?,
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

struct OutboxConsumer<P, O> {
    outbox: Arc<PgOutbox<P, O>>,
    group: String,
    lane: String,
}

/// The selection's URI prefix a consumer of `uri` takes: `outbox://all`,
/// every row; `outbox://<scheme>`, the rows addressed to `<scheme>://…`.
fn lane_of(uri: &str) -> Result<String, BusError> {
    Ok(match uri::channel(uri)? {
        "all" => String::new(),
        scheme => format!("{scheme}:"),
    })
}

impl<P, O> WireConsumer for OutboxConsumer<P, O>
where
    P: SessionPool + Send + Sync + 'static,
    P::Session: PgAccess + Sync,
    O: OutboxObserver + 'static,
{
    /// Runs the dispatcher as a task on the current runtime until cancelled.
    /// A batch whose handler fails is rolled back and retried after the poll
    /// interval; the position of the group moves only past messages the
    /// handler accepted. Outside a runtime this is an error, not a panic.
    fn subscribe(&self, handler: Handler) -> Result<Subscription, BusError> {
        let stop = Arc::new(Notify::new());
        let (outbox, group, lane, stopped) = (
            Arc::clone(&self.outbox),
            self.group.clone(),
            self.lane.clone(),
            Arc::clone(&stop),
        );
        let runtime =
            Handle::try_current().map_err(|error| BusError::Transport(Box::new(error)))?;
        let task = async move {
            let selection = Selection::group(&group).uri(lane);
            let loops = outbox.loops();
            let subscriber = |row: &OutboxMessage| {
                let handler = Arc::clone(&handler);
                let message = wire_of(row);
                async move { handling::within(handler(message)).await }
            };
            loop {
                let outcome = outbox
                    .run(&subscriber, &selection, loops, stopped.notified())
                    .await;
                match outcome {
                    Ok(()) => break,
                    Err(error) => {
                        // a defect, not a moment: it persists across polls until
                        // someone acts, so it is logged for the host's alarm
                        if error.is_transient() {
                            log::warn!("outbox[{group}]: dispatch failed, retrying: {error}");
                        } else {
                            log::error!("outbox[{group}]: dispatch stopped, retrying: {error}");
                        }
                        tokio::select! {
                            _ = stopped.notified() => break,
                            _ = tokio::time::sleep(loops.poll_interval) => {}
                        }
                    }
                }
            }
        };
        // Cancelling waits for the loop: the batch in hand acknowledged and
        // committed, the connection given back (ADR-0016 of the OCaml port).
        Ok(handling::serve(&runtime, stop, task))
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

/// The metadata of a wire message: one string field per header, or why it
/// cannot be — a header named twice, which the map would keep the last of
/// and the inbox refuses, or one that is not UTF-8.
fn metadata_of(message: &Message) -> Result<Value, &'static str> {
    let mut fields = Map::new();
    for (name, value) in message.headers() {
        let text = std::str::from_utf8(value).map_err(|_| "a header is not UTF-8")?;
        if fields
            .insert(name.clone(), Value::String(text.to_owned()))
            .is_some()
        {
            return Err("a header is named twice");
        }
    }
    Ok(Value::Object(fields))
}
