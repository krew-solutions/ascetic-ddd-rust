//! Scheme-dispatched message bus.
//!
//! The bus is a registry of transports keyed by URI scheme. A call site
//! writing `bus.consumer("in-memory://orders", "billing", decode)` does not
//! name the transport: the scheme picks the adapter registered for it.
//! Replacing the transport is one line at the composition root — the
//! [`Bus::register`] call — and nothing else moves.
//!
//! The bus carries opaque wire messages. A producer encodes its values on
//! the way out and a consumer decodes them on the way in, each with a
//! function of its own, so two consumers of one topic may read the same
//! bytes as different types: the wire format is the contract, the types are
//! local to each side. That is what lets two bounded contexts share a topic
//! without a translation layer between them.
//!
//! ```
//! use ascetic_ddd_bus::adapters::in_memory::InMemoryBroker;
//! use ascetic_ddd_bus::{Bus, Message};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), ascetic_ddd_bus::Error> {
//! let mut bus = Bus::new();
//! bus.register("in-memory", InMemoryBroker::new())?;
//!
//! let (seen, mut inbox) = tokio::sync::mpsc::unbounded_channel();
//! let orders = bus.consumer("in-memory://orders", "billing", |message: &Message| {
//!     Ok(String::from_utf8(message.payload().to_vec())?)
//! })?;
//! let _subscription = orders.subscribe(move |order: String| {
//!     let seen = seen.clone();
//!     async move { seen.send(order).ok(); }
//! })?;
//!
//! let producer = bus.producer("in-memory://orders", |order: &String| Message::new(order.as_bytes()))?;
//! producer.publish(&"order-7".to_owned()).await?;
//!
//! assert_eq!(inbox.recv().await.as_deref(), Some("order-7"));
//! # Ok(())
//! # }
//! ```
//!
//! # Stages
//!
//! A message may go through [`Stage`]s between the typed layer and the
//! transport — out after encoding, back in before decoding — which is where
//! sealing goes (ADR-0002): `producer.through(stage)`,
//! `consumer.through(stage)`. The bus does not know what a stage does.
//!
//! # Adapters
//!
//! [`adapters::in_memory`] is the monolithic transport: topics in a
//! process-local registry, one consumer per group. [`adapters::kafka`],
//! behind the `kafka` feature, is the same surface over a Kafka cluster.
//! Any other transport implements [`Adapter`].
//!
//! # Deviations from the OCaml source
//!
//! * Handlers are asynchronous, since consumers do their work through
//!   sessions and sockets; the OCaml ones are plain functions on Eio fibers.
//! * A wire message carries an optional key, [`Message::with_key`]. OCaml's
//!   producers serialize to a string; a transport that partitions needs a key
//!   to keep an aggregate's messages in order.
//! * A [`Subscription`] is cancelled explicitly, never by being dropped: the
//!   composition root discards most handles, as the OCaml code does.
//! * Errors are values, not exceptions: [`Error`].

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::all)]

mod adapter;
pub mod adapters;
mod bridge;
mod error;
mod message;
mod stage;
pub mod uri;

use std::collections::HashMap;
use std::sync::Arc;

pub use crate::adapter::{
    Adapter, Handler, Subscription, TransactionalHandler, TransactionalWireConsumer,
    TransactionalWireProducer, WireConsumer, WireProducer,
};
pub use crate::bridge::{Bridge, Target};
pub use crate::error::{BoxError, Error, Permanent};
pub use crate::message::Message;
pub use crate::stage::Stage;

/// A registry of transports by URI scheme.
///
/// Built once at the composition root, then shared. There is no global bus:
/// tests run on buses of their own.
#[derive(Default)]
pub struct Bus {
    adapters: HashMap<String, Arc<dyn Adapter>>,
}

impl Bus {
    /// A bus with no transports.
    pub fn new() -> Self {
        Bus::default()
    }

    /// Binds `scheme` to `adapter`: every `scheme://…` URI resolves to it.
    pub fn register(&mut self, scheme: &str, adapter: impl Adapter + 'static) -> Result<(), Error> {
        if self.adapters.contains_key(scheme) {
            return Err(Error::AlreadyRegistered(scheme.to_owned()));
        }
        self.adapters.insert(scheme.to_owned(), Arc::new(adapter));
        Ok(())
    }

    /// A consumer of `uri` in `group`, reading values with `decode`.
    ///
    /// A message that `decode` rejects is reported and skipped.
    pub fn consumer<T, D>(&self, uri: &str, group: &str, decode: D) -> Result<Consumer<T>, Error>
    where
        D: Fn(&Message) -> Result<T, BoxError> + Send + Sync + 'static,
    {
        let wire = self.adapter(uri)?.consumer(uri, group)?;
        Ok(Consumer {
            uri: uri.to_owned(),
            group: group.to_owned(),
            wire,
            decode: Arc::new(decode),
            stages: Vec::new(),
        })
    }

    /// A producer to `uri`, writing values with `encode`.
    pub fn producer<T, E>(&self, uri: &str, encode: E) -> Result<Producer<T>, Error>
    where
        E: Fn(&T) -> Message + Send + Sync + 'static,
    {
        let wire = self.adapter(uri)?.producer(uri)?;
        Ok(Producer {
            wire,
            encode: Box::new(encode),
            stages: Vec::new(),
        })
    }

    fn adapter(&self, uri: &str) -> Result<&dyn Adapter, Error> {
        let scheme = uri::scheme(uri)?;
        self.adapters
            .get(scheme)
            .map(Arc::as_ref)
            .ok_or_else(|| Error::UnknownScheme(scheme.to_owned()))
    }
}

/// How a consumer reads a wire message.
type Decoder<T> = Arc<dyn Fn(&Message) -> Result<T, BoxError> + Send + Sync>;

/// A typed consumer of one `(uri, group)`.
pub struct Consumer<T> {
    uri: String,
    group: String,
    wire: Box<dyn WireConsumer>,
    decode: Decoder<T>,
    stages: stage::Stages,
}

impl<T: 'static> Consumer<T> {
    /// The same consumer with one more [`Stage`] on the way in: messages
    /// come back through the stages in the reverse of the order they are
    /// given, before they are decoded.
    pub fn through(mut self, stage: Arc<dyn Stage>) -> Self {
        self.stages.push(stage);
        self
    }

    /// Runs `handler` for every message, in order, until the subscription is
    /// cancelled. Subscribing again replaces the handler.
    ///
    /// A handler that cannot fail returns `()`; one that can returns a
    /// `Result`, and an error means the message was not handled — a transport
    /// that can, redelivers it. So does a stage that fails on the way in.
    pub fn subscribe<F, Fut, O>(&self, handler: F) -> Result<Subscription, Error>
    where
        F: Fn(T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = O> + Send + 'static,
        O: Outcome,
    {
        let decode = Arc::clone(&self.decode);
        let stages = self.stages.clone();
        let handler = Arc::new(handler);
        let (uri, group) = (self.uri.clone(), self.group.clone());
        let handler: Handler = Arc::new(move |message: Message| {
            let (decode, stages, handler) =
                (Arc::clone(&decode), stages.clone(), Arc::clone(&handler));
            let (uri, group) = (uri.clone(), group.clone());
            Box::pin(async move {
                let message = stage::inbound(&stages, message).await?;
                // The value is handed to the handler before the await, so
                // that only the handler's future, which promises `Send`,
                // lives across it.
                let handling = match decode(&message) {
                    Ok(value) => handler(value),
                    Err(error) => {
                        log::warn!("bus[{uri}/{group}]: decoding failed: {error}");
                        return Ok(());
                    }
                };
                handling.await.into_result()
            })
        });
        self.wire.subscribe(handler)
    }
}

impl<T> std::fmt::Debug for Consumer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("uri", &self.uri)
            .field("group", &self.group)
            .finish_non_exhaustive()
    }
}

/// What a handler may return: nothing, or a result.
pub trait Outcome: Send + 'static {
    /// The outcome as a result.
    fn into_result(self) -> Result<(), BoxError>;
}

impl Outcome for () {
    fn into_result(self) -> Result<(), BoxError> {
        Ok(())
    }
}

impl<E: Into<BoxError> + Send + 'static> Outcome for Result<(), E> {
    fn into_result(self) -> Result<(), BoxError> {
        self.map_err(Into::into)
    }
}

/// A typed producer to one URI.
pub struct Producer<T> {
    wire: Box<dyn WireProducer>,
    encode: Box<dyn Fn(&T) -> Message + Send + Sync>,
    stages: stage::Stages,
}

impl<T> Producer<T> {
    /// The same producer with one more [`Stage`] on the way out: messages
    /// go through the stages in the order they are given, after encoding.
    pub fn through(mut self, stage: Arc<dyn Stage>) -> Self {
        self.stages.push(stage);
        self
    }

    /// Sends one value. Waits while the transport applies back-pressure. A
    /// stage that fails on the way out fails the publish.
    pub async fn publish(&self, value: &T) -> Result<(), Error> {
        let message = stage::outbound(&self.stages, (self.encode)(value))
            .await
            .map_err(Error::Stage)?;
        self.wire.publish(message).await
    }
}

/// A typed producer that publishes inside the caller's transaction.
///
/// Built once, at the composition root, from the adapter that offers it —
/// the outbox — and used with the transaction of the moment: the command
/// handler's, or the inbox's. The extra argument is the guarantee itself,
/// named at the call site (ADR-0003).
pub struct TransactionalProducer<T, S> {
    wire: Box<dyn TransactionalWireProducer<S>>,
    encode: Box<dyn Fn(&T) -> Message + Send + Sync>,
    stages: stage::Stages,
}

impl<T, S> TransactionalProducer<T, S> {
    /// A typed producer over a transactional wire producer.
    pub fn new(
        wire: Box<dyn TransactionalWireProducer<S>>,
        encode: impl Fn(&T) -> Message + Send + Sync + 'static,
    ) -> Self {
        TransactionalProducer {
            wire,
            encode: Box::new(encode),
            stages: Vec::new(),
        }
    }

    /// The same producer with one more [`Stage`] on the way out, as
    /// [`Producer::through`].
    pub fn through(mut self, stage: Arc<dyn Stage>) -> Self {
        self.stages.push(stage);
        self
    }

    /// Sends one value within `session`'s transaction. A stage that fails on
    /// the way out fails the publish, and so the transaction.
    pub async fn publish(&self, session: &S, value: &T) -> Result<(), Error> {
        let message = stage::outbound(&self.stages, (self.encode)(value))
            .await
            .map_err(Error::Stage)?;
        self.wire.publish(session, message).await
    }
}

/// A typed consumer that runs the handler inside the transport's own
/// transaction.
///
/// Obtained from the adapter that offers it — the inbox — as the
/// transactional producer is obtained from the outbox (ADR-0003). The
/// handler is given the session of the transaction that acknowledges the
/// message, so its writes and the acknowledgement commit together.
pub struct TransactionalConsumer<T, S> {
    wire: Box<dyn TransactionalWireConsumer<S>>,
    decode: Decoder<T>,
    stages: stage::Stages,
}

// The session crosses the stages' awaits on its way to the handler, so it
// must be `Send`, which every session is (ADR-0004).
impl<T: 'static, S: Send + 'static> TransactionalConsumer<T, S> {
    /// A typed consumer over a transactional wire consumer.
    pub fn new(
        wire: Box<dyn TransactionalWireConsumer<S>>,
        decode: impl Fn(&Message) -> Result<T, BoxError> + Send + Sync + 'static,
    ) -> Self {
        TransactionalConsumer {
            wire,
            decode: Arc::new(decode),
            stages: Vec::new(),
        }
    }

    /// The same consumer with one more [`Stage`] on the way in, as
    /// [`Consumer::through`].
    pub fn through(mut self, stage: Arc<dyn Stage>) -> Self {
        self.stages.push(stage);
        self
    }

    /// Runs `handler` for every message, with the transaction the message is
    /// acknowledged in, until the subscription is cancelled.
    ///
    /// A message that `decode` rejects is reported and acknowledged without a
    /// handler, as [`Consumer::subscribe`] does: a poison message must not
    /// stop the rest. A stage that fails on the way in fails the handling
    /// instead, so the message is kept and tried again.
    pub fn subscribe<F, Fut, O>(&self, handler: F) -> Result<Subscription, Error>
    where
        F: Fn(S, T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = O> + Send + 'static,
        O: Outcome,
    {
        let decode = Arc::clone(&self.decode);
        let stages = self.stages.clone();
        let handler = Arc::new(handler);
        let handler: TransactionalHandler<S> = Arc::new(move |session: S, message: Message| {
            let (decode, stages, handler) =
                (Arc::clone(&decode), stages.clone(), Arc::clone(&handler));
            Box::pin(async move {
                let message = stage::inbound(&stages, message).await?;
                let handling = match decode(&message) {
                    Ok(value) => handler(session, value),
                    Err(error) => {
                        log::warn!("bus[transactional]: decoding failed: {error}");
                        return Ok(());
                    }
                };
                handling.await.into_result()
            })
        });
        self.wire.subscribe(handler)
    }
}

impl<T, S> std::fmt::Debug for TransactionalConsumer<T, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionalConsumer")
            .finish_non_exhaustive()
    }
}

impl<T, S> std::fmt::Debug for TransactionalProducer<T, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionalProducer")
            .finish_non_exhaustive()
    }
}

impl<T> std::fmt::Debug for Producer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer").finish_non_exhaustive()
    }
}
