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

/// A wire-level handler that is given a transaction along with the message:
/// what a transactional consumer runs for each message. The session is a
/// handle (ADR-0001), handed over by value so that the handler's future owns
/// it; the bus does not know sessions, it only passes one through.
pub type TransactionalHandler<S> =
    Arc<dyn Fn(S, Message) -> BoxFuture<'static, Result<(), BoxError>> + Send + Sync>;

/// A consumer of wire messages that runs the handler inside a transaction
/// of its own — the inbox. The handler's writes through the session it is
/// given commit with the acknowledgement of the message, or not at all.
pub trait TransactionalWireConsumer<S>: Send + Sync {
    /// Starts delivering messages, each with its transaction, to `handler`.
    fn subscribe(&self, handler: TransactionalHandler<S>) -> Result<Subscription, Error>;
}

/// A handle on a subscription. [`cancel`][Subscription::cancel] detaches the
/// handler and waits until no call of it is in flight; a second `cancel`
/// detaches nothing and waits like the first. Dropping the handle does *not*
/// cancel — a subscription made at the composition root lives with the
/// process, and its handle is usually discarded.
///
/// A subscription cancelled is a handler that is not running and will not
/// be run: once `cancel` has returned, what the handler uses — a pool, a
/// connection, a broker — may be taken down. The adapters of this crate
/// keep that through [`handling`][crate::handling]; a channel over a
/// database ends its cancel on the batch in hand committed and its
/// connection given back.
pub struct Subscription {
    detach: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    quiesce: Option<Box<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>>,
}

impl Subscription {
    /// A subscription that `cancel` detaches by running `detach` once, with
    /// nothing to wait for after: for a handler that is never in flight when
    /// it is detached.
    pub fn new(detach: impl FnOnce() + Send + 'static) -> Self {
        Subscription {
            detach: Mutex::new(Some(Box::new(detach))),
            quiesce: None,
        }
    }

    /// A subscription that `cancel` detaches by running `detach` once, and
    /// then, every time it is called, waits on with the future `quiesce`
    /// makes, which must complete when no call of the handler is in flight.
    pub fn with_quiesce(
        detach: impl FnOnce() + Send + 'static,
        quiesce: impl Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) -> Self {
        Subscription {
            detach: Mutex::new(Some(Box::new(detach))),
            quiesce: Some(Box::new(quiesce)),
        }
    }

    /// Detaches the handler and returns when no call of it is in flight: it
    /// waits for a handler that is running, for as long as that takes.
    /// Called from inside the handler — a handler cancelling itself — it
    /// detaches and returns: the message in hand is the last. Idempotent.
    pub async fn cancel(&self) {
        self.detach();
        if crate::handling::inside() {
            return;
        }
        if let Some(quiesce) = &self.quiesce {
            quiesce().await;
        }
    }

    /// Detaches once, under the lock, so a second canceller finds it done.
    fn detach(&self) {
        let detach = self
            .detach
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
