//! The inbox as a channel of the bus (ADR-0003).
//!
//! Registered under [`INBOX_SCHEME`] as [`InboxChannel`], the inbox offers
//! two things. A *producer*, from the bus: `bus.producer("inbox://orders", …)`
//! stores each wire message under the identity its headers name, so a
//! [`Bridge`][ascetic_ddd_bus::Bridge] from a broker channel to
//! `Target::Fixed("inbox://orders")` is the intake. And a *transactional
//! consumer*, from [`PgInbox::consumer`]: it runs the handler inside the
//! transaction that marks a message processed, and hands it that
//! transaction. A bus consumer of `inbox://…` is refused, because through
//! it the handler's writes would fall outside that transaction.
//!
//! Headers become columns: `tenant_id`, `stream_type`, `stream_id` — the
//! JSON text of the id, or the id as a string when it is not JSON —
//! `stream_position`, and `destination` for `uri`: the channel the message
//! was sent to, as the outbox stamps it; without one, the inbox channel it
//! was published to, key included. Every other header is a field of
//! `metadata`, as text, or structured again when the text is a JSON array
//! or object, so `causal_dependencies` come back as they left. The channel
//! name after `inbox://` is not read yet: the whole inbox is one channel.
//!
//! The consumer's loop is a task on the current tokio runtime; cancel the
//! subscription to stop it. Both it and the producer are ordinary `Send`
//! futures, which [`SessionPool::session`] promises (ADR-0004).

use std::sync::Arc;

use ascetic_ddd_bus::{
    Adapter, BoxError, Error as BusError, Message, Subscription, TransactionalConsumer,
    TransactionalHandler, TransactionalWireConsumer, WireConsumer, WireProducer, uri,
};
use ascetic_ddd_session::{PgAccess, SessionPool};
use futures::future::BoxFuture;
use serde_json::{Map, Value};
use tokio::runtime::Handle;
use tokio::sync::Notify;

use crate::message::InboxMessage;
use crate::pg::{PgInbox, Workers};
use crate::port::Inbox;

/// The scheme the inbox is registered under.
pub const INBOX_SCHEME: &str = "inbox";

/// The headers that are columns of the inbox: its identity, and the channel
/// the message was sent to, which the outbox stamps.
const TENANT_ID: &str = "tenant_id";
const STREAM_TYPE: &str = "stream_type";
const STREAM_ID: &str = "stream_id";
const STREAM_POSITION: &str = "stream_position";
const DESTINATION: &str = "destination";
const COLUMNS: [&str; 5] = [
    TENANT_ID,
    STREAM_TYPE,
    STREAM_ID,
    STREAM_POSITION,
    DESTINATION,
];

impl<P> PgInbox<P>
where
    P: SessionPool + Send + Sync + 'static,
    P::Session: PgAccess + Sync + 'static,
{
    /// A consumer whose handler runs inside the transaction that marks each
    /// message processed, reading values with `decode`.
    pub fn consumer<T: 'static>(
        self: &Arc<Self>,
        decode: impl Fn(&Message) -> Result<T, BoxError> + Send + Sync + 'static,
    ) -> TransactionalConsumer<T, P::Session> {
        TransactionalConsumer::new(Box::new(InboxConsumer(Arc::clone(self))), decode)
    }
}

/// The inbox as a bus adapter: what [`Bus::register`][ascetic_ddd_bus::Bus::register] takes.
pub struct InboxChannel<P>(Arc<PgInbox<P>>);

impl<P> PgInbox<P> {
    /// The inbox as a channel of the bus.
    pub fn channel(self: &Arc<Self>) -> InboxChannel<P> {
        InboxChannel(Arc::clone(self))
    }
}

impl<P> Adapter for InboxChannel<P>
where
    P: SessionPool + Send + Sync + 'static,
    P::Session: PgAccess + Sync + 'static,
{
    /// The inbox hands the handler its transaction, which a bus consumer
    /// cannot: see [`PgInbox::consumer`].
    fn consumer(&self, uri: &str, _group: &str) -> Result<Box<dyn WireConsumer>, BusError> {
        Err(BusError::Transport(
            format!("`{uri}`: the inbox hands the handler its transaction; use PgInbox::consumer")
                .into(),
        ))
    }

    /// A producer into the inbox: the intake of `uri`.
    fn producer(&self, uri: &str) -> Result<Box<dyn WireProducer>, BusError> {
        Ok(Box::new(InboxProducer {
            inbox: Arc::clone(&self.0),
            uri: uri.to_owned(),
        }))
    }
}

struct InboxProducer<P> {
    inbox: Arc<PgInbox<P>>,
    uri: String,
}

impl<P> WireProducer for InboxProducer<P>
where
    P: SessionPool + Send + Sync + 'static,
    P::Session: PgAccess + Sync,
{
    /// Stores the message in a transaction of its own; the same identity
    /// again is ignored.
    fn publish(&self, message: Message) -> BoxFuture<'_, Result<(), BusError>> {
        Box::pin(async move {
            let row = inbox_message_of(&self.uri, &message)?;
            self.inbox.publish(&row).await.map_err(transport)
        })
    }
}

struct InboxConsumer<P>(Arc<PgInbox<P>>);

impl<P> TransactionalWireConsumer<P::Session> for InboxConsumer<P>
where
    P: SessionPool + Send + Sync + 'static,
    P::Session: PgAccess + Sync + 'static,
{
    /// Runs the processing loop as a task on the current runtime until
    /// cancelled. A message whose handler fails is left unprocessed and
    /// retried after the poll interval. Outside a runtime this is an error.
    fn subscribe(
        &self,
        handler: TransactionalHandler<P::Session>,
    ) -> Result<Subscription, BusError> {
        let stop = Arc::new(Notify::new());
        let (inbox, stopped) = (Arc::clone(&self.0), Arc::clone(&stop));
        let runtime = Handle::try_current().map_err(transport)?;
        runtime.spawn(async move {
            let workers = Workers {
                poll_interval: inbox.poll_interval(),
                ..Workers::default()
            };
            let subscriber =
                |tx: &P::Session, row: &InboxMessage| handler(tx.clone(), wire_of(row));
            loop {
                match inbox.run(&subscriber, workers, stopped.notified()).await {
                    Ok(()) => break,
                    Err(error) => {
                        log::warn!("inbox: processing failed, retrying: {error}");
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

/// The row a wire message becomes, published to `channel`.
fn inbox_message_of(channel: &str, message: &Message) -> Result<InboxMessage, BusError> {
    let column = |name: &str| {
        message
            .header(name)
            .ok_or_else(|| malformed(format!("the inbox needs a `{name}` header")))
            .and_then(text)
    };
    let stream_id = column(STREAM_ID)?;
    let stream_id =
        serde_json::from_str(stream_id).unwrap_or_else(|_| Value::String(stream_id.to_owned()));
    let stream_position = column(STREAM_POSITION)?
        .parse::<i32>()
        .map_err(|error| malformed(format!("`{STREAM_POSITION}` is not an integer: {error}")))?;
    let uri = match message.header(DESTINATION) {
        Some(destination) => text(destination)?.to_owned(),
        None => {
            let key = match message.key() {
                Some(key) => Some(text(key)?),
                None => uri::key(channel),
            };
            match key {
                Some(key) => format!("{}/{key}", uri::without_key(channel)),
                None => channel.to_owned(),
            }
        }
    };
    let metadata = message
        .headers()
        .iter()
        .filter(|(name, _)| !COLUMNS.contains(&name.as_str()))
        .map(|(name, value)| Ok((name.clone(), structured(text(value)?))))
        .collect::<Result<Map<_, _>, BusError>>()?;
    Ok(InboxMessage::new(
        column(TENANT_ID)?,
        column(STREAM_TYPE)?,
        stream_id,
        stream_position,
        uri,
        message.payload(),
    )
    .with_metadata(Value::Object(metadata)))
}

/// The wire message of a row: payload, key from the URI, the columns and
/// the metadata as headers.
fn wire_of(row: &InboxMessage) -> Message {
    let message = Message::new(row.payload.clone())
        .with_header(TENANT_ID, row.tenant_id.as_bytes())
        .with_header(STREAM_TYPE, row.stream_type.as_bytes())
        .with_header(STREAM_ID, header_text(&row.stream_id))
        .with_header(STREAM_POSITION, row.stream_position.to_string())
        .with_header(DESTINATION, row.uri.as_bytes());
    let message = match uri::key(&row.uri) {
        Some(key) => message.with_key(key.as_bytes()),
        None => message,
    };
    match &row.metadata {
        Some(Value::Object(fields)) => fields.iter().fold(message, |message, (name, value)| {
            message.with_header(name.clone(), header_text(value))
        }),
        _ => message,
    }
}

/// A header's text as a metadata field: structured again when it is a JSON
/// array or object, text otherwise.
fn structured(text: &str) -> Value {
    match serde_json::from_str::<Value>(text) {
        Ok(value @ (Value::Array(_) | Value::Object(_))) => value,
        _ => Value::String(text.to_owned()),
    }
}

/// A metadata value as header text: a string as it is, anything else as
/// JSON.
fn header_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

fn text(bytes: &[u8]) -> Result<&str, BusError> {
    std::str::from_utf8(bytes)
        .map_err(|_| malformed("the inbox keeps headers and keys as UTF-8 text".to_owned()))
}

fn malformed(reason: String) -> BusError {
    BusError::Transport(reason.into())
}

fn transport(error: impl std::error::Error + Send + Sync + 'static) -> BusError {
    BusError::Transport(Box::new(error))
}
