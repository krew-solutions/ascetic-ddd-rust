//! In-memory transport: topics in a process-local registry, no network.
//!
//! The monolithic counterpart of the Kafka adapter — same surface, same
//! group semantics. Each [`InMemoryBroker`] is an isolated world: two brokers
//! do not share topics or subscribers, so tests run in parallel on brokers of
//! their own.
//!
//! One consumer per `(uri, group)` is an invariant: in a monolith each
//! logical role is one instance, and a second consumer in the same group is a
//! configuration bug. Kafka has no such rule — there a group *is* several
//! instances sharing the work.
//!
//! Delivery is a task per topic: it takes messages off a bounded queue and
//! hands each, in order, to the handler of every group. A handler that panics
//! is reported and the next message is delivered; a handler that is slow
//! holds the topic, which is the back-pressure the queue exists for.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, MutexGuard};

use futures::FutureExt;
use futures::future::BoxFuture;
use tokio::sync::mpsc;

use crate::adapter::{Adapter, Handler, Subscription, WireConsumer, WireProducer};
use crate::error::Error;
use crate::message::Message;

/// Messages a topic queues before producers wait.
pub const DEFAULT_CAPACITY: usize = 1024;

type Groups = Arc<Mutex<HashMap<String, Option<Handler>>>>;

/// A process-local broker: a registry of topics with a delivery task each.
///
/// Cloning a broker gives a second handle on the same topics; use
/// [`InMemoryBroker::new`] for an isolated one.
#[derive(Clone)]
pub struct InMemoryBroker {
    inner: Arc<Inner>,
}

struct Inner {
    capacity: usize,
    topics: Mutex<HashMap<String, Topic>>,
}

struct Topic {
    queue: mpsc::Sender<Message>,
    groups: Groups,
}

impl InMemoryBroker {
    /// A broker whose topics queue [`DEFAULT_CAPACITY`] messages.
    pub fn new() -> Self {
        InMemoryBroker::with_capacity(DEFAULT_CAPACITY)
    }

    /// A broker whose topics queue `capacity` messages before producers wait.
    pub fn with_capacity(capacity: usize) -> Self {
        InMemoryBroker {
            inner: Arc::new(Inner {
                capacity: capacity.max(1),
                topics: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// The topic for `uri`, started on first use. Must be called on a Tokio
    /// runtime: the delivery task is spawned here.
    fn topic(&self, uri: &str) -> (mpsc::Sender<Message>, Groups) {
        let mut topics = lock(&self.inner.topics);
        let topic = topics.entry(uri.to_owned()).or_insert_with(|| {
            let (queue, inbox) = mpsc::channel(self.inner.capacity);
            let groups: Groups = Arc::new(Mutex::new(HashMap::new()));
            tokio::spawn(deliver(uri.to_owned(), inbox, Arc::clone(&groups)));
            Topic { queue, groups }
        });
        (topic.queue.clone(), Arc::clone(&topic.groups))
    }
}

impl Default for InMemoryBroker {
    fn default() -> Self {
        InMemoryBroker::new()
    }
}

/// The delivery task of one topic. Ends when the last producer and the
/// broker are gone, because the queue closes.
async fn deliver(uri: String, mut inbox: mpsc::Receiver<Message>, groups: Groups) {
    while let Some(message) = inbox.recv().await {
        let handlers: Vec<(String, Handler)> = lock(&groups)
            .iter()
            .filter_map(|(group, handler)| handler.clone().map(|h| (group.clone(), h)))
            .collect();
        for (group, handler) in handlers {
            if AssertUnwindSafe(handler(message.clone()))
                .catch_unwind()
                .await
                .is_err()
            {
                log::warn!("in-memory[{uri}/{group}]: handler panicked");
            }
        }
    }
}

impl Adapter for InMemoryBroker {
    /// Joins `group` on `uri`; refuses a second consumer in the same group.
    fn consumer(&self, uri: &str, group: &str) -> Result<Box<dyn WireConsumer>, Error> {
        let (_, groups) = self.topic(uri);
        {
            let mut groups = lock(&groups);
            if groups.contains_key(group) {
                return Err(Error::AlreadyInGroup {
                    uri: uri.to_owned(),
                    group: group.to_owned(),
                });
            }
            groups.insert(group.to_owned(), None);
        }
        Ok(Box::new(InMemoryConsumer {
            groups,
            group: group.to_owned(),
        }))
    }

    fn producer(&self, uri: &str) -> Result<Box<dyn WireProducer>, Error> {
        let (queue, _) = self.topic(uri);
        Ok(Box::new(InMemoryProducer { queue }))
    }
}

struct InMemoryConsumer {
    groups: Groups,
    group: String,
}

impl WireConsumer for InMemoryConsumer {
    fn subscribe(&self, handler: Handler) -> Result<Subscription, Error> {
        lock(&self.groups).insert(self.group.clone(), Some(handler));
        let groups = Arc::clone(&self.groups);
        let group = self.group.clone();
        Ok(Subscription::new(move || {
            if let Some(slot) = lock(&groups).get_mut(&group) {
                *slot = None;
            }
        }))
    }
}

struct InMemoryProducer {
    queue: mpsc::Sender<Message>,
}

impl WireProducer for InMemoryProducer {
    /// Queues the message; waits while the topic's queue is full.
    fn publish(&self, message: Message) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.queue
                .send(message)
                .await
                .map_err(|_| Error::Transport("the topic has been shut down".into()))
        })
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
