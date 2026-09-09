//! Integration tests for the PostgreSQL outbox. They need a live database:
//!
//! ```text
//! ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
//!     cargo test -p ascetic-ddd-outbox -- --ignored
//! ```

use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ascetic_ddd_outbox::{
    BoxError, Error, Outbox, OutboxMessage, PgOutbox, Position, Selection, Worker, Workers,
};
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};
use ascetic_ddd_session::{PgAccess, PgSessionPool, Session, SessionPool};
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
    outbox: PgOutbox<PgSessionPool>,
    tables: (String, String),
}

/// Tables of its own per test, so tests run in parallel.
async fn fixture(name: &str) -> Fixture {
    let tables = (format!("outbox_{name}"), format!("outbox_{name}_offsets"));
    let sessions = PgSessionPool::new(pool());
    let outbox = PgOutbox::new(PgSessionPool::new(pool())).with_tables(&tables.0, &tables.1);
    let drop = format!(
        "DROP TABLE IF EXISTS {}; DROP TABLE IF EXISTS {};",
        tables.0, tables.1
    );
    sessions
        .session(async |session| {
            session.connection().batch_execute(&drop).await.unwrap();
            outbox.setup(session).await
        })
        .await
        .unwrap();
    Fixture {
        sessions,
        outbox,
        tables,
    }
}

impl Fixture {
    fn with_batch_size(mut self, size: usize) -> Self {
        self.outbox = PgOutbox::new(PgSessionPool::new(pool()))
            .with_tables(&self.tables.0, &self.tables.1)
            .with_batch_size(size);
        self
    }

    /// Publishes each message in a transaction of its own, then waits until
    /// the dispatcher may see them: a message is visible only once every
    /// transaction older than its own has ended, and tests run in parallel,
    /// each with transactions of its own.
    async fn publish(&self, messages: &[OutboxMessage]) {
        for message in messages {
            self.sessions
                .session(async |session| {
                    session
                        .atomic(async |tx| self.outbox.publish(tx, message).await)
                        .await
                })
                .await
                .unwrap();
        }
        self.wait_visible().await;
    }

    /// Waits until everything in the table is older than every running
    /// transaction.
    async fn wait_visible(&self) {
        let sql = format!(
            "SELECT coalesce((SELECT transaction_id FROM {} ORDER BY transaction_id DESC LIMIT 1) \
             < pg_snapshot_xmin(pg_current_snapshot()), true)",
            self.tables.0
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let visible: bool = self
                .sessions
                .session(async |session| {
                    Ok::<_, Error>(session.connection().query_one(&sql, &[]).await?.get(0))
                })
                .await
                .unwrap();
            if visible {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "messages never became visible"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn position(&self, selection: &Selection) -> Position {
        self.sessions
            .session(async |session| self.outbox.position(session, selection).await)
            .await
            .unwrap()
    }
}

fn message(uri: &str, id: u64) -> OutboxMessage {
    static EVENT: AtomicU64 = AtomicU64::new(1);
    let event = EVENT.fetch_add(1, Ordering::Relaxed);
    OutboxMessage::new(
        uri,
        id.to_string(),
        json!({ "event_id": format!("00000000-0000-4000-8000-{event:012x}") }),
    )
}

fn id_of(message: &OutboxMessage) -> u64 {
    std::str::from_utf8(&message.payload)
        .unwrap()
        .parse()
        .unwrap()
}

type Seen = Arc<Mutex<Vec<u64>>>;

/// A subscriber that collects the ids it was given.
fn collector() -> (
    Seen,
    impl Fn(&OutboxMessage) -> std::future::Ready<Result<(), BoxError>>,
) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    (seen, move |message: &OutboxMessage| {
        sink.lock().unwrap().push(id_of(message));
        std::future::ready(Ok(()))
    })
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn published_messages_are_dispatched_in_order_once() {
    let f = fixture("roundtrip").await;
    f.publish(&[message("kafka://orders", 1), message("kafka://orders", 2)])
        .await;
    let (seen, subscriber) = collector();
    let selection = Selection::group("broker");

    assert!(
        f.outbox
            .dispatch(&subscriber, &selection, Worker::ALONE)
            .await
            .unwrap()
    );
    assert!(
        !f.outbox
            .dispatch(&subscriber, &selection, Worker::ALONE)
            .await
            .unwrap()
    );

    assert_eq!(*seen.lock().unwrap(), [1, 2]);
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn every_consumer_group_gets_every_message() {
    let f = fixture("groups").await;
    f.publish(&[message("kafka://orders", 1)]).await;
    let (seen_a, a) = collector();
    let (seen_b, b) = collector();

    f.outbox
        .dispatch(&a, &Selection::group("a"), Worker::ALONE)
        .await
        .unwrap();
    f.outbox
        .dispatch(&b, &Selection::group("b"), Worker::ALONE)
        .await
        .unwrap();

    assert_eq!(*seen_a.lock().unwrap(), [1]);
    assert_eq!(*seen_b.lock().unwrap(), [1]);
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_position_follows_the_last_acknowledged_message_and_can_be_moved() {
    let f = fixture("position").await;
    let selection = Selection::group("broker");
    assert_eq!(f.position(&selection).await, Position::default());

    f.publish(&[message("kafka://orders", 1), message("kafka://orders", 2)])
        .await;
    let (seen, subscriber) = collector();
    f.outbox
        .dispatch(&subscriber, &selection, Worker::ALONE)
        .await
        .unwrap();

    let position = f.position(&selection).await;
    assert!(position.transaction_id > 0);
    assert_eq!(position.offset, 2);

    // Back to the beginning: everything is delivered again.
    f.sessions
        .session(async |session| {
            f.outbox
                .set_position(session, &selection, Position::default())
                .await
        })
        .await
        .unwrap();
    f.outbox
        .dispatch(&subscriber, &selection, Worker::ALONE)
        .await
        .unwrap();
    assert_eq!(*seen.lock().unwrap(), [1, 2, 1, 2]);
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_uri_selects_itself_and_everything_under_it() {
    let f = fixture("uri").await;
    f.publish(&[
        message("kafka://orders", 1),
        message("kafka://orders/order-7", 2),
        message("kafka://payments", 3),
    ])
    .await;
    let (orders_seen, orders) = collector();
    let (payments_seen, payments) = collector();

    f.outbox
        .dispatch(
            &orders,
            &Selection::group("broker").uri("kafka://orders"),
            Worker::ALONE,
        )
        .await
        .unwrap();
    f.outbox
        .dispatch(
            &payments,
            &Selection::group("broker").uri("kafka://payments"),
            Worker::ALONE,
        )
        .await
        .unwrap();

    assert_eq!(*orders_seen.lock().unwrap(), [1, 2]);
    assert_eq!(*payments_seen.lock().unwrap(), [3]);
    // Each (group, uri) has a position of its own.
    assert_eq!(
        f.position(&Selection::group("broker").uri("kafka://payments"))
            .await
            .offset,
        3
    );
    assert_eq!(
        f.position(&Selection::group("broker")).await,
        Position::default()
    );
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_batch_is_at_most_batch_size_messages() {
    let f = fixture("batch").await.with_batch_size(2);
    f.publish(
        &(1..=5)
            .map(|i| message("kafka://orders", i))
            .collect::<Vec<_>>(),
    )
    .await;
    let (seen, subscriber) = collector();
    let selection = Selection::group("broker");

    let mut batches = 0;
    while f
        .outbox
        .dispatch(&subscriber, &selection, Worker::ALONE)
        .await
        .unwrap()
    {
        batches += 1;
    }

    assert_eq!(batches, 3);
    assert_eq!(*seen.lock().unwrap(), [1, 2, 3, 4, 5]);
}

/// A message of a transaction that is still open is not visible, and neither
/// is a later transaction's message that has already committed — the order
/// would be lost otherwise.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn nothing_is_dispatched_past_an_open_transaction() {
    let f = fixture("visibility").await;
    let (seen, subscriber) = collector();
    let selection = Selection::group("broker");

    f.sessions
        .session(async |session| {
            session
                .atomic(async |open| {
                    f.outbox
                        .publish(open, &message("kafka://orders", 1))
                        .await?;
                    // A later transaction commits while the first is open.
                    f.sessions
                        .session(async |session| {
                            session
                                .atomic(async |tx| {
                                    f.outbox.publish(tx, &message("kafka://orders", 2)).await
                                })
                                .await
                        })
                        .await?;
                    assert!(
                        !f.outbox
                            .dispatch(&subscriber, &selection, Worker::ALONE)
                            .await?
                    );
                    Ok::<_, Error>(())
                })
                .await
        })
        .await
        .unwrap();

    f.wait_visible().await;
    assert!(
        f.outbox
            .dispatch(&subscriber, &selection, Worker::ALONE)
            .await
            .unwrap()
    );
    assert_eq!(*seen.lock().unwrap(), [1, 2]);
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_failing_subscriber_rolls_the_batch_back() {
    let f = fixture("failure").await;
    f.publish(&[message("kafka://orders", 1)]).await;
    let selection = Selection::group("broker");

    let failing = |_: &OutboxMessage| std::future::ready(Err::<(), BoxError>("broker down".into()));
    assert!(matches!(
        f.outbox.dispatch(&failing, &selection, Worker::ALONE).await,
        Err(Error::Subscriber(_))
    ));
    assert_eq!(f.position(&selection).await, Position::default());

    let (seen, subscriber) = collector();
    assert!(
        f.outbox
            .dispatch(&subscriber, &selection, Worker::ALONE)
            .await
            .unwrap()
    );
    assert_eq!(*seen.lock().unwrap(), [1]);
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn an_event_id_is_published_once() {
    let f = fixture("event_id").await;
    let first = message("kafka://orders", 1);
    let mut duplicate = message("kafka://orders", 2);
    duplicate.metadata = first.metadata.clone();
    f.publish(&[first]).await;

    let refused = f
        .sessions
        .session(async |session| {
            session
                .atomic(async |tx| f.outbox.publish(tx, &duplicate).await)
                .await
        })
        .await;
    assert!(matches!(refused, Err(Error::Database(_))));
}

/// Two dispatchers of one group: the second waits for the first's lock and
/// then finds nothing, so no message is processed twice.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn two_dispatchers_of_one_group_do_not_overlap() {
    let f = fixture("lock").await;
    f.publish(
        &(1..=3)
            .map(|i| message("kafka://orders", i))
            .collect::<Vec<_>>(),
    )
    .await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let slow = |sink: Arc<Mutex<Vec<u64>>>| {
        move |message: &OutboxMessage| {
            let id = id_of(message);
            let sink = Arc::clone(&sink);
            async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                sink.lock().unwrap().push(id);
                Ok::<(), BoxError>(())
            }
        }
    };
    let selection = Selection::group("broker");

    let (a, b) = tokio::join!(
        f.outbox
            .dispatch(slow(Arc::clone(&seen)), &selection, Worker::ALONE),
        f.outbox
            .dispatch(slow(Arc::clone(&seen)), &selection, Worker::ALONE),
    );

    assert!(a.unwrap() ^ b.unwrap(), "exactly one of them had the batch");
    assert_eq!(*seen.lock().unwrap(), [1, 2, 3]);
}

/// Every keyed URI lands with exactly one of the workers — including the
/// half whose `hashtext` is negative, which the Python source lost.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn workers_share_the_uris_without_gaps_or_overlap() {
    let f = fixture("workers").await;
    f.publish(
        &(1..=40)
            .map(|i| message(&format!("kafka://orders/order-{i}"), i))
            .collect::<Vec<_>>(),
    )
    .await;
    let (seen, subscriber) = collector();
    let selection = Selection::group("broker").uri("kafka://orders");

    for id in 0..3 {
        let worker = Worker { id, of: 3 };
        while f
            .outbox
            .dispatch(&subscriber, &selection, worker)
            .await
            .unwrap()
        {}
    }

    let mut all = seen.lock().unwrap().clone();
    all.sort_unstable();
    assert_eq!(all, (1..=40).collect::<Vec<_>>());
}

/// `run` dispatches until told to stop, and stops between batches.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn run_dispatches_until_shutdown() {
    let f = fixture("run").await;
    f.publish(
        &(1..=6)
            .map(|i| message(&format!("kafka://orders/order-{i}"), i))
            .collect::<Vec<_>>(),
    )
    .await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let done = Arc::new(tokio::sync::Notify::new());
    let subscriber = {
        let (seen, done) = (Arc::clone(&seen), Arc::clone(&done));
        move |message: &OutboxMessage| {
            let mut seen = seen.lock().unwrap();
            seen.push(id_of(message));
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

    let shutdown = async { done.notified().await };
    tokio::time::timeout(
        Duration::from_secs(10),
        f.outbox
            .run(subscriber, &Selection::group("broker"), workers, shutdown),
    )
    .await
    .expect("run stops after shutdown")
    .unwrap();

    let mut all = seen.lock().unwrap().clone();
    all.sort_unstable();
    assert_eq!(all, [1, 2, 3, 4, 5, 6]);
}

/// The port is what a use case depends on; a fake collects what it would
/// have published.
#[tokio::test]
async fn the_port_is_implementable_without_a_database() {
    struct Fake(Mutex<Vec<String>>);

    impl<S: Session> Outbox<S> for Fake {
        async fn publish(&self, _: &S, message: &OutboxMessage) -> Result<(), Error> {
            self.0.lock().unwrap().push(message.uri.clone());
            Ok(())
        }
    }

    let fake = Fake(Mutex::new(Vec::new()));
    let sessions = ascetic_ddd_session::testing::MemorySessionPool::new();
    sessions
        .session(async |session| {
            session
                .atomic(async |tx| fake.publish(tx, &message("kafka://orders", 1)).await)
                .await
        })
        .await
        .unwrap();

    assert_eq!(*fake.0.lock().unwrap(), ["kafka://orders"]);
}
