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
//! Delivery (ADR-0036 decision 3): a task per topic takes messages off a
//! bounded queue and adds each to the backlog of every group that has a
//! handler attached; a task per group hands its backlog, in order, to the
//! handler attached at each try. A handler's error means the message was not
//! taken: the group tries it again after a pause, and what follows waits. A
//! handler that panics is reported and the next message is delivered.
//!
//! A group holds only itself. Producers wait while a group that is taking
//! its messages has `capacity` of them in its backlog: a handler that is
//! slow holds the topic, which is the back-pressure the queue exists for. A
//! group whose head keeps failing stops counting, and its backlog grows
//! until the head is taken; past `capacity`, and at each doubling after, it
//! is reported.
//!
//! The message in hand and the backlog belong to the group, not to a
//! subscription: a cancel leaves them to the next handler of the group, which
//! is tried at once. What is published while no handler is attached is not
//! delivered to the group — unlike Kafka, where a group's position waits on
//! the log. A group ends when its handler is detached and its consumer
//! dropped, which frees its name, or when its topic is gone and its backlog
//! delivered.

use std::collections::{HashMap, VecDeque};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use futures::FutureExt;
use futures::future::BoxFuture;
use tokio::sync::{Notify, mpsc};

use crate::adapter::{Adapter, Handler, Subscription, WireConsumer, WireProducer};
use crate::error::Error;
use crate::handling::{self, InFlight};
use crate::message::Message;
use crate::uri;

/// Messages a topic queues, and a group taking its messages holds in its
/// backlog, before producers wait.
pub const DEFAULT_CAPACITY: usize = 1024;

/// How long a group waits before it tries again a message whose handler
/// failed.
pub const DEFAULT_RETRY_AFTER: Duration = Duration::from_millis(100);

/// A process-local broker: a registry of topics, each with a task that
/// fans its messages out and a task per group that delivers them.
///
/// Cloning a broker gives a second handle on the same topics; use
/// [`InMemoryBroker::new`] for an isolated one.
#[derive(Clone)]
pub struct InMemoryBroker {
    inner: Arc<Inner>,
}

struct Inner {
    capacity: usize,
    retry_after: Duration,
    topics: Mutex<HashMap<String, Topic>>,
}

struct Topic {
    queue: mpsc::Sender<Message>,
    shared: Arc<Shared>,
}

/// What a topic's task and its groups' tasks share.
struct Shared {
    uri: String,
    capacity: usize,
    retry_after: Duration,
    state: Mutex<State>,
    /// A group took a message, let one go, stopped counting or ended: the
    /// topic's task may add the next.
    room: Notify,
    /// What a group's task waits for changed: a message, a handler, the end.
    changed: Notify,
}

struct State {
    groups: HashMap<String, Group>,
    /// The topic's task is still running: messages may come.
    open: bool,
}

/// A group of a topic: its backlog, the message in hand first, and the
/// handler of its subscription, if one is attached.
struct Group {
    backlog: VecDeque<Arc<Message>>,
    attached: Option<Attached>,
    /// Its head failed and has not been taken since: it does not hold
    /// producers back.
    holding: bool,
    /// Its consumer has not been dropped.
    consumer: bool,
    /// The backlog's length when it was last reported, while holding.
    reported: usize,
}

/// A group's handler, with the calls of it in flight.
struct Attached {
    handler: Handler,
    in_flight: InFlight,
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
                retry_after: DEFAULT_RETRY_AFTER,
                topics: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// The same broker, its groups waiting `pause` before they try a failed
    /// message again. Set before the broker is used: topics opened before
    /// are not carried over.
    pub fn retrying_after(self, pause: Duration) -> Self {
        InMemoryBroker {
            inner: Arc::new(Inner {
                capacity: self.inner.capacity,
                retry_after: pause,
                topics: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// The topic for `uri` — its channel, whatever key the URI carries —
    /// started on first use. Must be called on a Tokio runtime: the topic's
    /// task is spawned here.
    fn topic(&self, uri: &str) -> (mpsc::Sender<Message>, Arc<Shared>) {
        let uri = uri::without_key(uri);
        let mut topics = lock(&self.inner.topics);
        let topic = topics.entry(uri.to_owned()).or_insert_with(|| {
            let (queue, received) = mpsc::channel(self.inner.capacity);
            let shared = Arc::new(Shared {
                uri: uri.to_owned(),
                capacity: self.inner.capacity,
                retry_after: self.inner.retry_after,
                state: Mutex::new(State {
                    groups: HashMap::new(),
                    open: true,
                }),
                room: Notify::new(),
                changed: Notify::new(),
            });
            tokio::spawn(fan_out(received, Arc::clone(&shared)));
            Topic { queue, shared }
        });
        (topic.queue.clone(), Arc::clone(&topic.shared))
    }
}

impl Default for InMemoryBroker {
    fn default() -> Self {
        InMemoryBroker::new()
    }
}

/// The task of one topic: each message to the backlog of every group with a
/// handler attached, once no group taking its messages has a full one. Ends
/// when the last producer and the broker are gone, because the queue closes.
async fn fan_out(mut received: mpsc::Receiver<Message>, shared: Arc<Shared>) {
    while let Some(message) = received.recv().await {
        let message = Arc::new(message);
        loop {
            // Registered before the look, so that room made after it wakes us.
            let room = shared.room.notified();
            tokio::pin!(room);
            room.as_mut().enable();
            {
                let mut state = lock(&shared.state);
                let full = state.groups.values().any(|group| {
                    group.attached.is_some()
                        && !group.holding
                        && group.backlog.len() >= shared.capacity
                });
                if !full {
                    for (name, group) in state.groups.iter_mut() {
                        if group.attached.is_some() {
                            group.backlog.push_back(Arc::clone(&message));
                            report_backlog(&shared, name, group);
                        }
                    }
                    break;
                }
            }
            room.await;
        }
        shared.changed.notify_waiters();
    }
    lock(&shared.state).open = false;
    shared.changed.notify_waiters();
}

/// A holding group's backlog past the capacity, and at each doubling after.
fn report_backlog(shared: &Shared, name: &str, group: &mut Group) {
    let next = group.reported.max(shared.capacity / 2).max(1) * 2;
    if group.holding && group.backlog.len() >= next {
        group.reported = group.backlog.len();
        log::warn!(
            "in-memory[{}/{name}]: {} messages wait behind the message in hand",
            shared.uri,
            group.backlog.len() - 1
        );
    }
}

/// The task of one group: the head of its backlog to the handler attached,
/// until it is taken, then the next.
async fn deliver(shared: Arc<Shared>, name: String) {
    let mut failures: u32 = 0;
    loop {
        let changed = shared.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let call = {
            let mut state = lock(&shared.state);
            let open = state.open;
            let Some(group) = state.groups.get_mut(&name) else {
                return;
            };
            if group.attached.is_none() && !group.consumer {
                state.groups.remove(&name);
                drop(state);
                shared.room.notify_waiters();
                return;
            }
            if group.backlog.is_empty() && !open {
                return;
            }
            // Admitted under the lock handlers are detached under: a call is
            // either waited for by a cancel, or not made.
            match (group.backlog.front(), &group.attached) {
                (Some(message), Some(attached)) => Some((
                    Arc::clone(message),
                    attached.handler.clone(),
                    attached.in_flight.admit(),
                    attached.in_flight.clone(),
                )),
                _ => None,
            }
        };
        let Some((message, handler, admitted, by)) = call else {
            changed.await;
            continue;
        };
        let outcome = AssertUnwindSafe(handling::within(handler(Message::clone(&message))))
            .catch_unwind()
            .await;
        drop(admitted);
        let failed = match outcome {
            Ok(Ok(())) => false,
            Ok(Err(_)) => true,
            Err(_) => {
                log::warn!(
                    "in-memory[{}/{name}]: handler panicked, the message is skipped",
                    shared.uri
                );
                false
            }
        };
        {
            let mut state = lock(&shared.state);
            let Some(group) = state.groups.get_mut(&name) else {
                return;
            };
            if failed {
                group.holding = true;
                failures += 1;
            } else {
                // Only this task takes from the front: the head is the message tried.
                group.backlog.pop_front();
                group.holding = false;
                group.reported = 0;
                failures = 0;
            }
        }
        shared.room.notify_waiters();
        if failed {
            // The handler's own error is the handler's to report: it may
            // carry the message's values (ADR-0036 R9).
            if failures.is_power_of_two() {
                log::warn!(
                    "in-memory[{}/{name}]: handler failed {failures} times on the message in \
                     hand, retrying every {:?}",
                    shared.uri,
                    shared.retry_after
                );
            }
            tokio::select! {
                () = tokio::time::sleep(shared.retry_after) => {}
                () = replaced(&shared, &name, &by) => {}
            }
        }
    }
}

/// Returns once a handler other than `by` is attached to the group: the
/// pause before the next try ends, and the new handler is tried at once.
async fn replaced(shared: &Shared, name: &str, by: &InFlight) {
    loop {
        let changed = shared.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let other = lock(&shared.state)
            .groups
            .get(name)
            .and_then(|group| group.attached.as_ref())
            .is_some_and(|attached| !attached.in_flight.same(by));
        if other {
            return;
        }
        changed.await;
    }
}

impl Adapter for InMemoryBroker {
    /// Joins `group` on `uri`; refuses a second consumer in the same group.
    fn consumer(&self, uri: &str, group: &str) -> Result<Box<dyn WireConsumer>, Error> {
        let (_, shared) = self.topic(uri);
        {
            let mut state = lock(&shared.state);
            if state.groups.contains_key(group) {
                return Err(Error::AlreadyInGroup {
                    uri: uri::without_key(uri).to_owned(),
                    group: group.to_owned(),
                });
            }
            state.groups.insert(
                group.to_owned(),
                Group {
                    backlog: VecDeque::new(),
                    attached: None,
                    holding: false,
                    consumer: true,
                    reported: 0,
                },
            );
        }
        tokio::spawn(deliver(Arc::clone(&shared), group.to_owned()));
        Ok(Box::new(InMemoryConsumer {
            shared,
            group: group.to_owned(),
        }))
    }

    fn producer(&self, uri: &str) -> Result<Box<dyn WireProducer>, Error> {
        let (queue, _) = self.topic(uri);
        Ok(Box::new(InMemoryProducer {
            queue,
            key: uri::key(uri).map(|key| key.as_bytes().to_vec()),
        }))
    }
}

struct InMemoryConsumer {
    shared: Arc<Shared>,
    group: String,
}

impl WireConsumer for InMemoryConsumer {
    fn subscribe(&self, handler: Handler) -> Result<Subscription, Error> {
        let in_flight = InFlight::new();
        if let Some(group) = lock(&self.shared.state).groups.get_mut(&self.group) {
            group.attached = Some(Attached {
                handler,
                in_flight: in_flight.clone(),
            });
        }
        self.shared.changed.notify_waiters();
        let shared = Arc::clone(&self.shared);
        let group = self.group.clone();
        let mine = in_flight.clone();
        Ok(Subscription::with_quiesce(
            // Only this subscription's handler: a later one of the group stays.
            move || {
                if let Some(slot) = lock(&shared.state).groups.get_mut(&group)
                    && slot
                        .attached
                        .as_ref()
                        .is_some_and(|a| a.in_flight.same(&mine))
                {
                    slot.attached = None;
                }
                shared.changed.notify_waiters();
                shared.room.notify_waiters();
            },
            move || {
                let in_flight = in_flight.clone();
                Box::pin(async move { in_flight.quiesce().await })
            },
        ))
    }
}

/// A consumer dropped with its handler attached goes on delivering — a
/// subscription made at the composition root outlives the consumer it was
/// made on; with none attached, its group ends.
impl Drop for InMemoryConsumer {
    fn drop(&mut self) {
        if let Some(group) = lock(&self.shared.state).groups.get_mut(&self.group) {
            group.consumer = false;
        }
        self.shared.changed.notify_waiters();
    }
}

struct InMemoryProducer {
    queue: mpsc::Sender<Message>,
    /// The key the producer's URI carries; a message without one gets it.
    key: Option<Vec<u8>>,
}

impl WireProducer for InMemoryProducer {
    /// Queues the message; waits while the topic's queue is full.
    fn publish(&self, message: Message) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let message = match (&self.key, message.key()) {
                (Some(key), None) => message.with_key(key.clone()),
                _ => message,
            };
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
