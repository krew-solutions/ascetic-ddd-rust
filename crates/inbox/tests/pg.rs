//! Integration tests for the PostgreSQL inbox. They need a live database:
//!
//! ```text
//! ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
//!     cargo test -p ascetic-ddd-inbox -- --ignored
//! ```

use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ascetic_ddd_inbox::{
    BoxError, ByStream, CausalDependency, Error, Inbox, InboxMessage, PgInbox, Worker, Workers,
};
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};
use ascetic_ddd_session::{PgAccess, PgSession, PgSessionPool, SessionPool};
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

struct Fixture {
    sessions: PgSessionPool,
    inbox: PgInbox<PgSessionPool>,
    table: String,
}

/// A table of its own per test, so tests run in parallel.
async fn fixture(name: &str) -> Fixture {
    let table = format!("inbox_{name}");
    let sequence = format!("inbox_{name}_seq");
    let sessions = PgSessionPool::new(pool());
    let inbox = PgInbox::new(PgSessionPool::new(pool())).with_table(&table, &sequence);
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
    }
}

impl Fixture {
    fn partitioned_by_stream(mut self) -> Self {
        let sequence = format!("{}_seq", self.table);
        self.inbox = PgInbox::new(PgSessionPool::new(pool()))
            .with_table(&self.table, &sequence)
            .partitioned_by(ByStream);
        self
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

    assert!(f.inbox.dispatch(&subscriber, Worker::ALONE).await.unwrap());
    assert!(!f.inbox.dispatch(&subscriber, Worker::ALONE).await.unwrap());

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

    while f.inbox.dispatch(&subscriber, Worker::ALONE).await.unwrap() {}

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
    assert!(f.inbox.dispatch(&subscriber, Worker::ALONE).await.unwrap());
    assert!(!f.inbox.dispatch(&subscriber, Worker::ALONE).await.unwrap());
    assert_eq!(*seen.lock().unwrap(), ["other@1"], "the effect waits");

    f.publish(&[cause]).await;
    while f.inbox.dispatch(&subscriber, Worker::ALONE).await.unwrap() {}

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
    assert!(matches!(
        f.inbox.dispatch(&failing, Worker::ALONE).await,
        Err(Error::Subscriber(_))
    ));
    assert_eq!(f.processed().await, 0);

    let (seen, subscriber) = collector();
    assert!(f.inbox.dispatch(&subscriber, Worker::ALONE).await.unwrap());
    assert_eq!(*seen.lock().unwrap(), ["a@1"]);
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
        f.inbox.dispatch(slow(Arc::clone(&seen)), Worker::ALONE),
        f.inbox.dispatch(slow(Arc::clone(&seen)), Worker::ALONE),
    );

    assert!(a.unwrap() && b.unwrap());
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
        while f.inbox.dispatch(&subscriber, worker).await.unwrap() {}
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
