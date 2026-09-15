//! Integration tests for the PostgreSQL inbox. They need a live database:
//!
//! ```text
//! ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
//!     cargo test -p ascetic-ddd-inbox -- --ignored
//! ```
//!
//! With `ASCETIC_DDD_TRACE_DIR` set, every test writes what its inbox
//! reported as `inbox-<name>.jsonl` into that directory, one event per line,
//! for validation against the protocol model: see `verify/tla/README.md`.

use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ascetic_ddd_inbox::observer::{
    Deferred, Dispatched, Expired, Failed, Fetched, Handled, InboxObserver, Marked, Received,
    Resolved, Unparked, Waiting,
};
use ascetic_ddd_inbox::{
    BoxError, ByStream, CausalDependency, Error, Inbox, InboxMessage, Outcome, PgInbox, Receipt,
    Retries, Worker, Workers,
};
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};
use ascetic_ddd_session::{PgAccess, PgSession, PgSessionPool, SessionPool};
use ascetic_ddd_trace::{JsonTrace, TraceFile};
use serde_json::json;

const DEFAULT_URL: &str = "postgresql://devel:devel@localhost:5432/devel_karmabot_test";

fn pool() -> Pool {
    let url = std::env::var("ASCETIC_DDD_TEST_PG_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned());
    let manager = Manager::from_config(
        Config::from_str(&url).expect("a valid PostgreSQL URL"),
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("the pool can be built")
}

struct Fixture<O = ()> {
    sessions: PgSessionPool,
    inbox: PgInbox<PgSessionPool, (O, Arc<JsonTrace>)>,
    table: String,
    /// Numbers the fixture's `dispatch` calls, which run one at a time.
    calls: AtomicU64,
    /// Written when the fixture is dropped, if a trace directory is set.
    _trace: TraceFile,
}

/// A table of its own per test, so tests run in parallel.
async fn fixture(name: &str) -> Fixture {
    fixture_observed(name, ()).await
}

/// The same, watched by `observer`.
async fn fixture_observed<O: InboxObserver>(name: &str, observer: O) -> Fixture<O> {
    let table = format!("inbox_{name}");
    let sequence = format!("inbox_{name}_seq");
    let sessions = PgSessionPool::new(pool());
    let trace = TraceFile::from_env(&format!("inbox-{name}"));
    let inbox = PgInbox::new(PgSessionPool::new(pool()))
        .with_table(&table, &sequence)
        .observed_by((observer, trace.recorder()));
    let drop = format!("DROP TABLE IF EXISTS {table}; DROP SEQUENCE IF EXISTS {sequence};");
    sessions
        .session(async |session| {
            session.connection().batch_execute(&drop).await.unwrap();
            inbox.setup(&session).await
        })
        .await
        .unwrap();
    Fixture {
        sessions,
        inbox,
        table,
        calls: AtomicU64::new(0),
        _trace: trace,
    }
}

impl<O: InboxObserver> Fixture<O> {
    fn partitioned_by_stream(self) -> Self {
        Fixture {
            inbox: self.inbox.partitioned_by(ByStream),
            ..self
        }
    }

    fn with_retries(self, retries: Retries) -> Self {
        Fixture {
            inbox: self.inbox.with_retries(retries),
            ..self
        }
    }

    fn with_max_wait(self, max_wait: Duration) -> Self {
        Fixture {
            inbox: self.inbox.with_max_wait(max_wait),
            ..self
        }
    }

    /// One `dispatch` call, as the only worker.
    async fn dispatch<F, Fut>(&self, subscriber: F) -> Outcome
    where
        F: Fn(&PgSession, &InboxMessage) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<(), BoxError>> + Send,
    {
        let call = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
        self.inbox
            .dispatch(subscriber, Worker::ALONE, call)
            .await
            .unwrap()
    }

    async fn parked(&self) -> Vec<String> {
        self.sessions
            .session(async |session| self.inbox.parked(&session).await)
            .await
            .unwrap()
            .iter()
            .map(label)
            .collect()
    }

    async fn publish(&self, messages: &[InboxMessage]) {
        for message in messages {
            self.inbox.publish(message).await.unwrap();
        }
    }

    async fn rows(&self) -> i64 {
        let sql = format!("SELECT count(*) FROM {}", self.table);
        self.sessions
            .session(async |session| {
                Ok::<_, Error>(session.connection().query_one(&sql, &[]).await?.get(0))
            })
            .await
            .unwrap()
    }

    async fn processed(&self) -> i64 {
        let sql = format!(
            "SELECT count(*) FROM {} WHERE processed_position IS NOT NULL",
            self.table
        );
        self.sessions
            .session(async |session| {
                Ok::<_, Error>(session.connection().query_one(&sql, &[]).await?.get(0))
            })
            .await
            .unwrap()
    }
}

/// A message of stream `stream` at `position`, with a unique event id.
fn message(stream: &str, position: i32) -> InboxMessage {
    static EVENT: AtomicU64 = AtomicU64::new(1);
    let event = EVENT.fetch_add(1, Ordering::Relaxed);
    InboxMessage::new(
        "tenant-1",
        "orders.Order",
        json!({ "id": stream }),
        position,
        format!("kafka://orders/{stream}"),
        format!("{stream}@{position}"),
    )
    .with_metadata(json!({ "message_id": format!("00000000-0000-4000-8000-{event:012x}") }))
}

fn label(message: &InboxMessage) -> String {
    format!(
        "{}@{}",
        message.stream_id["id"].as_str().unwrap(),
        message.stream_position
    )
}

type Seen = Arc<Mutex<Vec<String>>>;

/// A subscriber that collects what it was given.
fn collector() -> (
    Seen,
    impl Fn(&PgSession, &InboxMessage) -> std::future::Ready<Result<(), BoxError>>,
) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    (seen, move |_: &PgSession, message: &InboxMessage| {
        sink.lock().unwrap().push(label(message));
        std::future::ready(Ok(()))
    })
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_received_message_is_processed_once_in_its_transaction() {
    let f = fixture("roundtrip").await;
    f.publish(&[message("a", 1)]).await;
    let table = f.table.clone();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    // The subscriber sees its message through the transaction it is given,
    // still unprocessed.
    let subscriber = move |tx: &PgSession, message: &InboxMessage| {
        let sql =
            format!("SELECT processed_position IS NULL FROM {table} WHERE received_position = $1");
        let tx = tx.clone();
        let (sink, position) = (Arc::clone(&sink), message.received_position);
        let label = label(message);
        async move {
            let unprocessed: bool = tx.connection().query_one(&sql, &[&position]).await?.get(0);
            assert!(unprocessed);
            sink.lock().unwrap().push(label);
            Ok::<(), BoxError>(())
        }
    };

    assert_eq!(f.dispatch(&subscriber).await, Outcome::Processed);
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Nothing);

    assert_eq!(*seen.lock().unwrap(), ["a@1"]);
    assert_eq!(f.processed().await, 1);
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn receiving_the_same_message_twice_stores_it_once() {
    let f = fixture("idempotent").await;
    let once = message("a", 1);
    let mut again = once.clone();
    again.payload = b"a duplicate with a different payload".to_vec();

    f.publish(&[once, again]).await;

    assert_eq!(f.rows().await, 1);
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn an_message_id_is_stored_once() {
    let f = fixture("message_id").await;
    let first = message("a", 1);
    let mut other = message("a", 2);
    other.metadata = first.metadata.clone();
    f.publish(&[first]).await;

    assert!(matches!(
        f.inbox.publish(&other).await,
        Err(Error::Database(_))
    ));
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn messages_are_processed_in_order_of_arrival() {
    let f = fixture("order").await;
    f.publish(&[message("b", 1), message("a", 1), message("a", 2)])
        .await;
    let (seen, subscriber) = collector();

    while f.dispatch(&subscriber).await != Outcome::Nothing {}

    assert_eq!(*seen.lock().unwrap(), ["b@1", "a@1", "a@2"]);
}

/// A message that depends on one not yet processed waits, even though it
/// arrived first; a message that depends on nothing goes ahead of it.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_message_waits_for_its_causal_dependencies() {
    let f = fixture("causal").await;
    let cause = message("user", 5);
    let effect = message("order", 1).depending_on(&[CausalDependency::on(&cause)]);
    let unrelated = message("other", 1);
    f.publish(&[effect, unrelated]).await; // the cause has not arrived

    let (seen, subscriber) = collector();
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Processed);
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Nothing);
    assert_eq!(*seen.lock().unwrap(), ["other@1"], "the effect waits");

    f.publish(&[cause]).await;
    while f.dispatch(&subscriber).await != Outcome::Nothing {}

    assert_eq!(*seen.lock().unwrap(), ["other@1", "user@5", "order@1"]);
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_failing_subscriber_leaves_the_message_unprocessed() {
    let f = fixture("failure").await;
    f.publish(&[message("a", 1)]).await;

    let failing = |_: &PgSession, _: &InboxMessage| {
        std::future::ready(Err::<(), BoxError>("handler down".into()))
    };
    assert_eq!(
        f.dispatch(&failing).await,
        Outcome::Failed {
            attempts: 1,
            parked: false
        }
    );
    assert_eq!(f.processed().await, 0);

    let (seen, subscriber) = collector();
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Processed);
    assert_eq!(*seen.lock().unwrap(), ["a@1"]);
}

/// A subscriber that fails on one message and succeeds on the others.
fn failing_on(
    poison: &str,
) -> (
    Seen,
    impl Fn(&PgSession, &InboxMessage) -> std::future::Ready<Result<(), BoxError>>,
) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (sink, poison) = (Arc::clone(&seen), poison.to_owned());
    (seen, move |_: &PgSession, message: &InboxMessage| {
        std::future::ready(if label(message) == poison {
            Err(format!("{poison} is poison").into())
        } else {
            sink.lock().unwrap().push(label(message));
            Ok(())
        })
    })
}

/// A subscriber that fails the first time it is called and succeeds after.
fn failing_once() -> (
    Seen,
    impl Fn(&PgSession, &InboxMessage) -> std::future::Ready<Result<(), BoxError>>,
) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let calls = AtomicUsize::new(0);
    (seen, move |_: &PgSession, message: &InboxMessage| {
        std::future::ready(if calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Err("the first attempt fails on purpose".into())
        } else {
            sink.lock().unwrap().push(label(message));
            Ok(())
        })
    })
}

/// After `max_attempts` failures the message is parked, in place, and the
/// partition behind it flows (ADR-0005).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_poison_message_is_parked_after_its_attempts_and_the_partition_flows() {
    let f = fixture("parking").await.with_retries(Retries::up_to(2));
    f.publish(&[message("a", 1), message("b", 1)]).await;
    let (seen, subscriber) = failing_on("a@1");

    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Failed {
            attempts: 1,
            parked: false
        }
    );
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Failed {
            attempts: 2,
            parked: true
        }
    );
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Processed);
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Nothing);

    assert_eq!(*seen.lock().unwrap(), ["b@1"]);
    assert_eq!(f.parked().await, ["a@1"]);
    assert_eq!(f.rows().await, 2, "a parked row stays in the table");
    let parked = f
        .sessions
        .session(async |session| f.inbox.parked(&session).await)
        .await
        .unwrap();
    assert_eq!(parked[0].attempts, 2);
    assert_eq!(parked[0].last_error.as_deref(), Some("a@1 is poison"));
}

/// While a failed message waits for its backoff it holds its partition: the
/// message behind it is not processed first.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_message_in_backoff_holds_its_partition() {
    let f = fixture("backoff")
        .await
        .with_retries(Retries::unlimited().with_backoff(|_| Duration::from_millis(300)));
    f.publish(&[message("a", 1), message("b", 1)]).await;
    let (seen, subscriber) = failing_once();

    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Failed {
            attempts: 1,
            parked: false
        }
    );
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Nothing,
        "b@1 waits behind a@1"
    );
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Processed);
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Processed);

    assert_eq!(*seen.lock().unwrap(), ["a@1", "b@1"]);
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn an_unparked_message_is_tried_again() {
    let f = fixture("unpark").await.with_retries(Retries::up_to(1));
    let poison = message("a", 1);
    f.publish(std::slice::from_ref(&poison)).await;
    let (seen, subscriber) = failing_once();

    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Failed {
            attempts: 1,
            parked: true
        }
    );
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Nothing);

    let unparked = f
        .sessions
        .session(async |session| f.inbox.unpark(&session, &poison).await)
        .await
        .unwrap();
    assert!(unparked);
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Processed);
    assert_eq!(*seen.lock().unwrap(), ["a@1"]);
    assert!(f.parked().await.is_empty());
}

/// Resolving a parked message marks it processed without its effects, and
/// what depended on it goes ahead.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_resolved_message_releases_its_dependents() {
    let f = fixture("resolve").await.with_retries(Retries::up_to(1));
    let cause = message("a", 1);
    let effect = message("b", 1).depending_on(&[CausalDependency::on(&cause)]);
    f.publish(&[cause.clone(), effect]).await;
    let (seen, subscriber) = failing_on("a@1");

    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Failed {
            attempts: 1,
            parked: true
        }
    );
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Nothing, "b@1 waits");

    let resolved = f
        .sessions
        .session(async |session| f.inbox.resolve(&session, &cause).await)
        .await
        .unwrap();
    assert!(resolved);
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Processed,
        "b@1 was woken"
    );

    assert_eq!(*seen.lock().unwrap(), ["b@1"], "a@1 never had its effects");
    assert_eq!(f.processed().await, 2);
    assert!(f.parked().await.is_empty());
}

/// Two dispatchers at once: `SKIP LOCKED` gives each its own message, and
/// nothing is processed twice.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn concurrent_dispatchers_do_not_share_a_message() {
    let f = fixture("skip_locked").await;
    f.publish(&[message("a", 1), message("b", 1)]).await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let slow = |sink: Seen| {
        move |_: &PgSession, message: &InboxMessage| {
            let (sink, label) = (Arc::clone(&sink), label(message));
            async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                sink.lock().unwrap().push(label);
                Ok::<(), BoxError>(())
            }
        }
    };

    let (a, b) = tokio::join!(
        f.inbox.dispatch(slow(Arc::clone(&seen)), Worker::ALONE, 1),
        f.inbox.dispatch(slow(Arc::clone(&seen)), Worker::ALONE, 2),
    );

    assert_eq!(
        (a.unwrap(), b.unwrap()),
        (Outcome::Processed, Outcome::Processed)
    );
    let mut all = seen.lock().unwrap().clone();
    all.sort();
    assert_eq!(all, ["a@1", "b@1"]);
}

/// Every stream lands with exactly one of the workers — including the half
/// whose `hashtext` is negative, which the Python source lost.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn workers_share_the_streams_without_gaps_or_overlap() {
    let f = fixture("workers").await.partitioned_by_stream();
    f.publish(
        &(1..=40)
            .map(|i| message(&format!("stream-{i}"), 1))
            .collect::<Vec<_>>(),
    )
    .await;
    let (seen, subscriber) = collector();

    for id in 0..3 {
        let worker = Worker { id, of: 3 };
        let mut call = 0;
        while {
            call += 1;
            f.inbox.dispatch(&subscriber, worker, call).await.unwrap() != Outcome::Nothing
        } {}
    }

    let mut all = seen.lock().unwrap().clone();
    all.sort();
    let mut expected: Vec<String> = (1..=40).map(|i| format!("stream-{i}@1")).collect();
    expected.sort();
    assert_eq!(all, expected);
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn run_processes_until_shutdown() {
    let f = fixture("run").await;
    f.publish(
        &(1..=6)
            .map(|i| message(&format!("s{i}"), 1))
            .collect::<Vec<_>>(),
    )
    .await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let done = Arc::new(tokio::sync::Notify::new());
    let subscriber = {
        let (seen, done) = (Arc::clone(&seen), Arc::clone(&done));
        move |_: &PgSession, message: &InboxMessage| {
            let mut seen = seen.lock().unwrap();
            seen.push(label(message));
            if seen.len() == 6 {
                done.notify_one();
            }
            std::future::ready(Ok::<(), BoxError>(()))
        }
    };
    let workers = Workers {
        concurrency: 2,
        poll_interval: Duration::from_millis(20),
        ..Workers::default()
    };

    tokio::time::timeout(
        Duration::from_secs(10),
        f.inbox
            .run(subscriber, workers, async { done.notified().await }),
    )
    .await
    .expect("run stops after shutdown")
    .unwrap();

    assert_eq!(seen.lock().unwrap().len(), 6);
    assert_eq!(f.processed().await, 6);
}

/// The port is what the edge depends on; a fake collects what it received.
#[tokio::test]
async fn the_port_is_implementable_without_a_database() {
    struct Fake(Mutex<Vec<String>>);

    impl Inbox for Fake {
        async fn publish(&self, message: &InboxMessage) -> Result<(), Error> {
            self.0.lock().unwrap().push(label(message));
            Ok(())
        }
    }

    let fake = Fake(Mutex::new(Vec::new()));
    fake.publish(&message("a", 1)).await.unwrap();
    assert_eq!(*fake.0.lock().unwrap(), ["a@1"]);
}

/// Records what the inbox reports, in the words of the protocol model.
#[derive(Default)]
struct Recorder {
    events: Mutex<Vec<String>>,
    received: Mutex<Vec<(String, Option<Receipt>)>>,
    /// Each fetch of a row, with whether its snapshot saw the row's transaction.
    fetches_saw_their_row: Mutex<Vec<bool>>,
    marked: Mutex<Vec<(String, i64)>>,
}

impl Recorder {
    fn note(&self, event: impl Into<String>) {
        self.events.lock().unwrap().push(event.into());
    }
}

impl InboxObserver for Recorder {
    fn on_received(&self, event: &Received<'_>) {
        self.received
            .lock()
            .unwrap()
            .push((label(event.message), event.receipt));
        let how = match event.receipt {
            Some(_) => "stored",
            None => "duplicate",
        };
        self.note(format!("received {} {how}", label(event.message)));
    }
    fn on_waiting(&self, event: &Waiting<'_>) {
        self.note(format!(
            "waiting {} for {}@{}",
            label(event.message),
            event.dependency.stream_id["id"].as_str().unwrap(),
            event.dependency.stream_position
        ));
    }
    fn on_expired(&self, event: &Expired<'_>) {
        let ids: Vec<String> = event.messages.iter().map(label).collect();
        self.note(format!("expired {}", ids.join(" ")));
    }
    fn on_fetched(&self, event: &Fetched<'_>) {
        if let Some(message) = event.message {
            let stored = self
                .received
                .lock()
                .unwrap()
                .iter()
                .find_map(|(name, receipt)| {
                    (*name == label(message)).then_some(*receipt).flatten()
                });
            self.fetches_saw_their_row
                .lock()
                .unwrap()
                .push(stored.is_some_and(|receipt| event.snapshot.sees(receipt.transaction_id)));
        }
        self.note(match event.message {
            Some(message) => format!("fetched {}", label(message)),
            None => "fetched nothing".to_owned(),
        });
    }
    fn on_handled(&self, event: &Handled<'_>) {
        let outcome = if event.outcome.is_ok() {
            "ok"
        } else {
            "failed"
        };
        self.note(format!("handled {} {outcome}", label(event.message)));
    }
    fn on_marked(&self, event: &Marked<'_>) {
        self.marked
            .lock()
            .unwrap()
            .push((label(event.message), event.processed_position));
        let woken: Vec<String> = event.woken.iter().map(label).collect();
        self.note(match woken.is_empty() {
            true => format!("marked {}", label(event.message)),
            false => format!("marked {} woke {}", label(event.message), woken.join(" ")),
        });
    }
    fn on_failed(&self, event: &Failed<'_>) {
        self.note(format!(
            "failed {} {}{}",
            label(event.message),
            event.attempts,
            if event.parked { " parked" } else { "" }
        ));
    }
    fn on_deferred(&self, event: &Deferred<'_>) {
        self.note(format!("deferred {}", label(event.message)));
    }
    fn on_dispatched(&self, event: &Dispatched<'_>) {
        self.note(match event.outcome {
            Ok(Outcome::Processed) => "dispatched a message",
            Ok(Outcome::Failed { .. }) => "dispatched a failure",
            Ok(Outcome::Nothing) => "dispatched nothing",
            Err(_) => "rolled back",
        });
    }
    fn on_unparked(&self, event: &Unparked<'_>) {
        self.note(format!("unparked {}", label(event.message)));
    }
    fn on_resolved(&self, event: &Resolved<'_>) {
        let woken: Vec<String> = event.woken.iter().map(label).collect();
        self.note(match woken.is_empty() {
            true => format!("resolved {}", label(event.message)),
            false => format!("resolved {} woke {}", label(event.message), woken.join(" ")),
        });
    }
}

/// The observer sees the protocol the model in verify/tla/Inbox.tla is
/// written in: a message received with its order of arrival, or ignored as
/// a duplicate; a row set aside to wait for its dependency, and woken by the
/// mark of that dependency; the subscriber's outcome; the mark with its order
/// of processing; the close of the transaction — a recorded attempt when the
/// subscriber failed, and the message again afterwards.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_observer_sees_the_protocol() {
    let recorder = Arc::new(Recorder::default());
    let f = fixture_observed("observed", Arc::clone(&recorder)).await;
    let first = message("a", 1);
    let second = message("b", 1).depending_on(&[CausalDependency::on(&first)]);
    let (_, subscriber) = collector();

    // The dependent arrives first and is stepped over until the other is
    // processed.
    f.publish(&[second.clone(), first.clone()]).await;
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Processed);
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Processed);
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Nothing);

    // The same identity again is not a step.
    f.publish(&[first]).await;

    // The subscriber fails once: nothing is marked, the message comes again.
    f.publish(&[message("c", 1)]).await;
    let attempts = AtomicUsize::new(0);
    let flaky = |_: &PgSession, _: &InboxMessage| {
        let first = attempts.fetch_add(1, Ordering::SeqCst) == 0;
        std::future::ready(if first {
            Err::<(), BoxError>("the first attempt fails on purpose".into())
        } else {
            Ok(())
        })
    };
    assert_eq!(
        f.dispatch(&flaky).await,
        Outcome::Failed {
            attempts: 1,
            parked: false
        }
    );
    assert_eq!(f.dispatch(&flaky).await, Outcome::Processed);

    assert_eq!(
        *recorder.events.lock().unwrap(),
        [
            "received b@1 stored",
            "received a@1 stored",
            "waiting b@1 for a@1",
            "fetched a@1",
            "handled a@1 ok",
            "marked a@1 woke b@1",
            "dispatched a message",
            "fetched b@1",
            "handled b@1 ok",
            "marked b@1",
            "dispatched a message",
            "fetched nothing",
            "dispatched nothing",
            "received a@1 duplicate",
            "received c@1 stored",
            "fetched c@1",
            "handled c@1 failed",
            "failed c@1 1",
            "dispatched a failure",
            "fetched c@1",
            "handled c@1 ok",
            "marked c@1",
            "dispatched a message",
        ]
    );

    // Arrival order and processing order are what the sequences say.
    let received: Vec<Option<i64>> = recorder
        .received
        .lock()
        .unwrap()
        .iter()
        .map(|(_, receipt)| receipt.map(|receipt| receipt.received_position))
        .collect();
    assert_eq!(received.len(), 4);
    assert!(received[0].unwrap() < received[1].unwrap());
    assert_eq!(received[2], None);
    assert!(received[1].unwrap() < received[3].unwrap());

    // Every fetch ran under a snapshot that saw the transaction which stored
    // the row it returned: visibility, as the dispatcher saw it.
    let saw = recorder.fetches_saw_their_row.lock().unwrap().clone();
    assert_eq!(saw.len(), 4, "a@1, b@1, c@1 twice");
    assert!(saw.iter().all(|saw| *saw));
    let marked = recorder.marked.lock().unwrap().clone();
    assert_eq!(
        marked
            .iter()
            .map(|(label, _)| label.as_str())
            .collect::<Vec<_>>(),
        ["a@1", "b@1", "c@1"]
    );
    assert!(marked.windows(2).all(|pair| pair[0].1 < pair[1].1));
}

/// A message whose dependency never arrives waits out of the queue, costing
/// the walk nothing, and with `max_wait` set is parked with the dependency
/// named (ADR-0006).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_dependency_that_never_arrives_parks_the_message_after_max_wait() {
    let f = fixture("expire")
        .await
        .with_max_wait(Duration::from_millis(300));
    let cause = message("user", 5);
    let effect = message("order", 1).depending_on(&[CausalDependency::on(&cause)]);
    f.publish(std::slice::from_ref(&effect)).await;
    let (seen, subscriber) = collector();

    assert_eq!(f.dispatch(&subscriber).await, Outcome::Nothing, "set aside");
    let waiting = f
        .sessions
        .session(async |session| {
            let sql = format!(
                "SELECT count(*) FROM {} WHERE waiting_for IS NOT NULL",
                f.table
            );
            Ok::<i64, Error>(session.connection().query_one(&sql, &[]).await?.get(0))
        })
        .await
        .unwrap();
    assert_eq!(waiting, 1);
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Nothing,
        "still waiting"
    );

    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Nothing,
        "expired and parked"
    );
    assert_eq!(f.parked().await, ["order@1"]);
    let parked = f
        .sessions
        .session(async |session| f.inbox.parked(&session).await)
        .await
        .unwrap();
    assert!(
        parked[0]
            .last_error
            .as_deref()
            .unwrap()
            .contains("never arrived"),
        "{:?}",
        parked[0].last_error
    );
    assert!(seen.lock().unwrap().is_empty());

    // The dependency arrives after all: unparked, the message waits again,
    // and is woken by the dependency's mark.
    f.publish(std::slice::from_ref(&cause)).await;
    let unparked = f
        .sessions
        .session(async |session| f.inbox.unpark(&session, &effect).await)
        .await
        .unwrap();
    assert!(unparked);
    while f.dispatch(&subscriber).await != Outcome::Nothing {}
    assert_eq!(*seen.lock().unwrap(), ["user@5", "order@1"]);
}
