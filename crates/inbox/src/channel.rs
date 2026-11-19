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
//! JSON text of the id, read and written by the bus's identity function
//! (ADR-0037 decision 1) — `stream_position`; `uri` is the inbox channel the message was published
//! to, key included — the intake's name for what it received, and the
//! `destination` header the row goes out with (ADR-0018). A `destination`
//! header on the way in is the producer's address and is not kept: for the
//! relay out of an outbox it equals the channel anyway, since the bridge
//! builds the producer for it; from a broker it would name the producer's
//! topic, and two producers of one topic would name one row two ways. Every
//! other header is a field of `metadata`, as its text (ADR-0037 decision
//! 2), so it comes back as it left, byte for byte; a value a message
//! built in memory holds structured (`depending_on`, `with_metadata`) is
//! written as its JSON text, the text the intake keeps. The channel
//! name after `inbox://` is not a filter: the
//! whole inbox is one channel with one subscriber, which routes by the
//! row's `destination` and `stream_type` — a consumer that saw one address
//! would wait for ever on a causal dependency published under another
//! (ADR-0006, ADR-0018). One subscriber is a rule, not a habit: a second
//! subscription while one is live is refused (seedwork ADR-0044), since
//! each would run over every row, and one going in without the other's
//! stages would hand its handler what it cannot read.
//!
//! The consumer's loop is a task on the current tokio runtime; cancel the
//! subscription to stop it. Both it and the producer are ordinary `Send`
//! futures, which [`SessionPool::session`] promises (ADR-0004).

use std::sync::Arc;

use ascetic_ddd_bus::identity::{self, STREAM_ID, STREAM_POSITION, STREAM_TYPE, TENANT_ID};
use ascetic_ddd_bus::{
    Adapter, BoxError, Composition, Dependencies, Error as BusError, Message, Subscription,
    TransactionalConsumer, TransactionalHandler, TransactionalWireConsumer, WireConsumer,
    WireProducer, handling, uri,
};
use ascetic_ddd_session::{PgAccess, SessionPool};
use futures::future::BoxFuture;
use serde_json::Value;
use tokio::runtime::Handle;
use tokio::sync::Notify;

use crate::error::Failure;
use crate::intake::DESTINATION;
use crate::message::InboxMessage;
use crate::observer::InboxObserver;
use crate::pg::PgInbox;

/// The scheme the inbox is registered under.
pub const INBOX_SCHEME: &str = "inbox";

/// The longest channel an intake takes: its rows' `uri` is 255 characters,
/// and a channel past them would set every message aside (ADR-0036 R3).
const CHANNEL_WIDTH: usize = 200;

impl<P, O> PgInbox<P, O>
where
    P: SessionPool + Send + Sync + 'static,
    P::Session: PgAccess + Sync + 'static,
    O: InboxObserver + 'static,
{
    /// A consumer whose handler runs inside the transaction that marks each
    /// message processed, reading values with `decode`.
    ///
    /// It goes in through [`InboundStage`](ascetic_ddd_bus::InboundStage)s
    /// only, and its subscription registers what they depend on (seedwork
    /// ADR-0044): the channels they declare are weighed by their dependency
    /// from then on, for the inbox's life. One subscription is live at a
    /// time; another is refused [`Composition::AlreadySubscribed`] until the
    /// first's task has ended. A cancel from inside a handler returns at once
    /// ([`Subscription::cancel`]), so a subscription made right after it, in
    /// that handler, is refused.
    pub fn consumer<T: 'static>(
        self: &Arc<Self>,
        decode: impl Fn(&Message) -> Result<T, BoxError> + Send + Sync + 'static,
    ) -> TransactionalConsumer<T, P::Session> {
        TransactionalConsumer::new(Box::new(InboxConsumer(Arc::clone(self))), decode)
    }
}

/// The inbox as a bus adapter: what [`Bus::register`][ascetic_ddd_bus::Bus::register] takes.
pub struct InboxChannel<P, O = ()>(Arc<PgInbox<P, O>>);

impl<P, O> PgInbox<P, O> {
    /// The inbox as a channel of the bus.
    pub fn channel(self: &Arc<Self>) -> InboxChannel<P, O> {
        InboxChannel(Arc::clone(self))
    }
}

impl<P, O> Adapter for InboxChannel<P, O>
where
    P: SessionPool + Send + Sync + 'static,
    P::Session: PgAccess + Sync + 'static,
    O: InboxObserver + 'static,
{
    /// The inbox hands the handler its transaction, which a bus consumer
    /// cannot: see [`PgInbox::consumer`].
    fn consumer(&self, uri: &str, _group: &str) -> Result<Box<dyn WireConsumer>, BusError> {
        Err(BusError::Transport(
            format!("`{uri}`: the inbox hands the handler its transaction; use PgInbox::consumer")
                .into(),
        ))
    }

    /// A producer into the inbox: the intake of `uri`. A channel too long
    /// for the inbox's rows is refused here, once, rather than every
    /// message of it set aside.
    fn producer(&self, uri: &str) -> Result<Box<dyn WireProducer>, BusError> {
        if uri::without_key(uri).chars().count() > CHANNEL_WIDTH {
            return Err(BusError::Transport(
                format!("an inbox channel is at most {CHANNEL_WIDTH} characters").into(),
            ));
        }
        Ok(Box::new(InboxProducer {
            inbox: Arc::clone(&self.0),
            uri: uri.to_owned(),
        }))
    }
}

struct InboxProducer<P, O> {
    inbox: Arc<PgInbox<P, O>>,
    uri: String,
}

impl<P, O> WireProducer for InboxProducer<P, O>
where
    P: SessionPool + Send + Sync + 'static,
    P::Session: PgAccess + Sync,
    O: InboxObserver + 'static,
{
    /// Takes the message through the intake (ADR-0036): stored, found
    /// already stored, or set aside beside the inbox. An error, which holds
    /// the source, only when the environment fails.
    fn publish(&self, message: Message) -> BoxFuture<'_, Result<(), BusError>> {
        Box::pin(async move {
            self.inbox
                .intake(&self.uri, &message)
                .await
                .map(|_| ())
                .map_err(transport)
        })
    }
}

struct InboxConsumer<P, O>(Arc<PgInbox<P, O>>);

impl<P, O> TransactionalWireConsumer<P::Session> for InboxConsumer<P, O>
where
    P: SessionPool + Send + Sync + 'static,
    P::Session: PgAccess + Sync + 'static,
    O: InboxObserver + 'static,
{
    /// Runs the processing loop as a task on the current runtime until
    /// cancelled, its stages' refusals weighed by `dependencies`. A message
    /// whose handler fails is left unprocessed and retried after the poll
    /// interval.
    ///
    /// In this order, so that a refusal keeps nothing of the call (seedwork
    /// ADR-0044): the runtime — outside one this is an error —, the inbox's
    /// one live subscription, the dependencies, all of them or none, and
    /// only then the task, which holds the subscription to its end.
    fn subscribe(
        &self,
        handler: TransactionalHandler<P::Session>,
        dependencies: Dependencies,
    ) -> Result<Subscription, BusError> {
        let runtime = Handle::try_current().map_err(transport)?;
        let live = self
            .0
            .subscription()
            .ok_or(Composition::AlreadySubscribed)?;
        self.0.register(&dependencies)?;
        let stop = Arc::new(Notify::new());
        let (inbox, stopped) = (Arc::clone(&self.0), Arc::clone(&stop));
        let task = async move {
            // Let go when the task ends, however it ends.
            let _live = live;
            let loops = inbox.loops();
            // A handler's error is a failure of the moment, unless the bus
            // carries a verdict from a stage or a handler that can tell:
            // permanent, parked at once; unavailable, held or spent by its
            // scope (ADR-0040) — each under its cause.
            let subscriber = |tx: &P::Session, row: &InboxMessage| {
                let handled = handler(tx.clone(), wire_of(row));
                async move { handling::within(handled).await.map_err(Failure::of) }
            };
            loop {
                match inbox.run(&subscriber, loops, stopped.notified()).await {
                    Ok(()) => break,
                    Err(error) => {
                        log::warn!("inbox: processing failed, retrying: {error}");
                        tokio::select! {
                            _ = stopped.notified() => break,
                            _ = tokio::time::sleep(loops.poll_interval) => {}
                        }
                    }
                }
            }
        };
        // Cancelling waits for the loop: the message in hand marked and
        // committed, the connection given back (ADR-0016 of the OCaml port).
        Ok(handling::serve(&runtime, stop, task))
    }
}

/// The wire message of a row: payload, key from the URI, the columns and
/// the metadata as headers.
pub(crate) fn wire_of(row: &InboxMessage) -> Message {
    let message = Message::new(row.payload.clone())
        .with_header(TENANT_ID, row.tenant_id.as_bytes())
        .with_header(STREAM_TYPE, row.stream_type.as_bytes())
        .with_header(STREAM_ID, identity::write_stream_id(&row.stream_id))
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

/// The headers of a row, as a stage reads them on the way in, without its
/// payload: what a check of the channel's dependency is given of a head
/// (ADR-0042 decision 1).
pub(crate) fn head_of(tenant: &str, uri: &str, metadata: Option<&Value>) -> Message {
    let message = Message::new(Vec::new())
        .with_header(TENANT_ID, tenant.as_bytes())
        .with_header(DESTINATION, uri.as_bytes());
    match metadata {
        Some(Value::Object(fields)) => fields.iter().fold(message, |message, (name, value)| {
            message.with_header(name.clone(), header_text(value))
        }),
        _ => message,
    }
}

/// A metadata value as header text: a string as it is; a value held
/// structured — by a message built in memory, or a row an older release
/// wrote — as its JSON text, which the intake keeps as that string.
fn header_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

fn transport(error: impl std::error::Error + Send + Sync + 'static) -> BusError {
    BusError::Transport(Box::new(error))
}
