//! The contract a transport implements.
//!
//! An adapter deals in wire messages only; the typed layer in the crate root
//! encodes and decodes around it. This is OCaml's `Adapter` module type, with
//! its abstract types made into trait objects: the registry picks an adapter
//! by URI scheme at run time, so dispatch is dynamic by nature.

use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;

use crate::error::{BoxError, Error};
use crate::message::Message;

/// A wire-level handler: what a consumer runs for each message. An error
/// means the message was not handled: a transport that can, redelivers it.
pub type Handler = Arc<dyn Fn(Message) -> BoxFuture<'static, Result<(), BoxError>> + Send + Sync>;

/// A transport, bound to a URI scheme by [`Bus::register`][crate::Bus::register].
pub trait Adapter: Send + Sync {
    /// A consumer of `uri` in `group`.
    fn consumer(&self, uri: &str, group: &str) -> Result<Box<dyn WireConsumer>, Error>;

    /// A producer to `uri`.
    fn producer(&self, uri: &str) -> Result<Box<dyn WireProducer>, Error>;
}

/// A consumer of wire messages.
pub trait WireConsumer: Send + Sync {
    /// Starts delivering messages to `handler`, replacing the previous handler
    /// of this consumer if there was one.
    fn subscribe(&self, handler: Handler) -> Result<Subscription, Error>;
}

/// A producer of wire messages.
pub trait WireProducer: Send + Sync {
    /// Sends one message.
    fn publish(&self, message: Message) -> BoxFuture<'_, Result<(), Error>>;
}

/// A producer of wire messages that publishes inside a transaction the
/// caller holds — the outbox. `S` is whatever the caller's transaction is;
/// the bus does not know sessions, it only passes one through.
pub trait TransactionalWireProducer<S>: Send + Sync {
    /// Sends one message within `session`'s transaction: it is committed
    /// with the caller's state change, or not at all.
    fn publish<'a>(&'a self, session: &'a S, message: Message) -> BoxFuture<'a, Result<(), Error>>;
}

/// A handle on a subscription. [`cancel`][Subscription::cancel] detaches the
/// handler; a second `cancel` does nothing. Dropping the handle does *not*
/// cancel — a subscription made at the composition root lives with the
/// process, and its handle is usually discarded.
pub struct Subscription {
    cancel: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl Subscription {
    /// A subscription that `cancel` detaches by running `detach` once.
    pub fn new(detach: impl FnOnce() + Send + 'static) -> Self {
        Subscription {
            cancel: Mutex::new(Some(Box::new(detach))),
        }
    }

    /// Detaches the handler. Idempotent.
    pub fn cancel(&self) {
        let detach = self
            .cancel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(detach) = detach {
            detach();
        }
    }
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription").finish_non_exhaustive()
    }
}
