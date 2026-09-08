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
mod error;
mod message;

use std::collections::HashMap;
use std::sync::Arc;

pub use crate::adapter::{Adapter, Handler, Subscription, WireConsumer, WireProducer};
pub use crate::error::{BoxError, Error};
pub use crate::message::Message;

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
        })
    }

    fn adapter(&self, uri: &str) -> Result<&dyn Adapter, Error> {
        let scheme = scheme_of(uri)?;
        self.adapters
            .get(scheme)
            .map(Arc::as_ref)
            .ok_or_else(|| Error::UnknownScheme(scheme.to_owned()))
    }
}

/// The scheme of a URI: what comes before the first `:`.
fn scheme_of(uri: &str) -> Result<&str, Error> {
    uri.split_once(':')
        .map(|(scheme, _)| scheme)
        .ok_or_else(|| Error::UnknownScheme(uri.to_owned()))
}

/// How a consumer reads a wire message.
type Decoder<T> = Arc<dyn Fn(&Message) -> Result<T, BoxError> + Send + Sync>;

/// A typed consumer of one `(uri, group)`.
pub struct Consumer<T> {
    uri: String,
    group: String,
    wire: Box<dyn WireConsumer>,
    decode: Decoder<T>,
}

impl<T: 'static> Consumer<T> {
    /// Runs `handler` for every message, in order, until the subscription is
    /// cancelled. Subscribing again replaces the handler.
    pub fn subscribe<F, Fut>(&self, handler: F) -> Result<Subscription, Error>
    where
        F: Fn(T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let decode = Arc::clone(&self.decode);
        let (uri, group) = (self.uri.clone(), self.group.clone());
        let handler: Handler = Arc::new(move |message: Message| match decode(&message) {
            Ok(value) => Box::pin(handler(value)),
            Err(error) => {
                log::warn!("bus[{uri}/{group}]: decoding failed: {error}");
                Box::pin(async {})
            }
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

/// A typed producer to one URI.
pub struct Producer<T> {
    wire: Box<dyn WireProducer>,
    encode: Box<dyn Fn(&T) -> Message + Send + Sync>,
}

impl<T> Producer<T> {
    /// Sends one value. Waits while the transport applies back-pressure.
    pub async fn publish(&self, value: &T) -> Result<(), Error> {
        self.wire.publish((self.encode)(value)).await
    }
}

impl<T> std::fmt::Debug for Producer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer").finish_non_exhaustive()
    }
}
