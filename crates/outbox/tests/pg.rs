//! Integration tests for the PostgreSQL outbox. They need a live database:
//!
//! ```text
//! ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
//!     cargo test -p ascetic-ddd-outbox -- --ignored
//! ```
//!
//! With `ASCETIC_DDD_TRACE_DIR` set, every test writes what its outbox
//! reported as `outbox-<name>.jsonl` into that directory, one event per
//! line, for validation against the protocol model: see
//! `verify/tla/README.md`.

use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ascetic_ddd_outbox::observer::{
    Acked, Dispatched, Fetched, Handled, OutboxObserver, Published, Receipt,
};
use ascetic_ddd_outbox::{
    BoxError, Error, Identity, Loops, Outbox, OutboxMessage, PgOutbox, Position, Selection,
};
use ascetic_ddd_session::pg::Identifier;
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};
use ascetic_ddd_session::{PgAccess, PgSessionPool, Session, SessionPool};
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
    outbox: PgOutbox<PgSessionPool, (O, Arc<JsonTrace>)>,
    tables: (String, String),
    /// Written when the fixture is dropped, if a trace directory is set.
    _trace: TraceFile,
}

/// Tables of its own per test, so tests run in parallel; one slot.
async fn fixture(name: &str) -> Fixture {
    fixture_observed(name, (), 1).await
}

/// The same, cut into `slots` slots: a property of the table, so set before
/// it is created.
async fn fixture_slotted(name: &str, slots: u32) -> Fixture {
    fixture_observed(name, (), slots).await
}

/// The same, watched by `observer`.
async fn fixture_observed<O: OutboxObserver>(name: &str, observer: O, slots: u32) -> Fixture<O> {
    fixture_built(
        name,
        observer,
        slots,
        TraceFile::from_env(&format!("outbox-{name}")),
    )
    .await
}

/// The same for a test that runs several dispatchers at once, which records
/// no trace: the order its events are logged in is not the order of their
/// commits.
async fn fixture_concurrent(name: &str, slots: u32) -> Fixture {
    fixture_built(name, (), slots, TraceFile::off()).await
}

async fn fixture_built<O: OutboxObserver>(
    name: &str,
    observer: O,
    slots: u32,
    trace: TraceFile,
) -> Fixture<O> {
    let tables = (format!("outbox_{name}"), format!("outbox_{name}_offsets"));
    let sessions = PgSessionPool::new(pool());
    let outbox = PgOutbox::new(PgSessionPool::new(pool()))
        .with_tables(
            Identifier::new(&tables.0).unwrap(),
            Identifier::new(&tables.1).unwrap(),
        )
        .with_slots(slots)
        .observed_by((observer, trace.recorder()));
    outbox.teardown().await.unwrap();
    outbox.migrate().await.unwrap();
    Fixture {
        sessions,
        outbox,
        tables,
        _trace: trace,
    }
}

impl<O: OutboxObserver> Fixture<O> {
    fn with_batch_size(self, size: usize) -> Self {
        Fixture {
            outbox: self.outbox.with_batch_size(size),
            ..self
        }
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
                        .atomic(async |tx| self.outbox.publish(&tx, message).await)
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

    /// The positions of the selection, by slot.
    /// Two URIs that land in different slots, by the table's cut, for tests
    /// that need two slots busy at once.
    async fn uris_in_different_slots(&self) -> (String, String) {
        let slots = self.outbox.slots() as i32;
        let slot_of = |uri: String| async move {
            self.sessions
                .session(async |session| {
                    let row = session
                        .connection()
                        .query_one("SELECT (hashtext($1) & 2147483647) % $2", &[&uri, &slots])
                        .await?;
                    Ok::<i32, Error>(row.get(0))
                })
                .await
                .unwrap()
        };
        let first = "kafka://orders/order-1".to_owned();
        let slot = slot_of(first.clone()).await;
        for i in 2.. {
            let candidate = format!("kafka://orders/order-{i}");
            if slot_of(candidate.clone()).await != slot {
                return (first, candidate);
            }
        }
        unreachable!("a second slot has some URI")
    }

    async fn positions(&self, selection: &Selection) -> Vec<Position> {
        self.sessions
            .session(async |session| self.outbox.positions(&session, selection).await)
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
        json!({ "message_id": format!("00000000-0000-4000-8000-{event:012x}") }),
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

    assert!(f.outbox.dispatch(&subscriber, &selection).await.unwrap());
    assert!(!f.outbox.dispatch(&subscriber, &selection).await.unwrap());

    assert_eq!(*seen.lock().unwrap(), [1, 2]);
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn every_consumer_group_gets_every_message() {
    let f = fixture("groups").await;
    f.publish(&[message("kafka://orders", 1)]).await;
    let (seen_a, a) = collector();
    let (seen_b, b) = collector();

    f.outbox.dispatch(&a, &Selection::group("a")).await.unwrap();
    f.outbox.dispatch(&b, &Selection::group("b")).await.unwrap();

    assert_eq!(*seen_a.lock().unwrap(), [1]);
    assert_eq!(*seen_b.lock().unwrap(), [1]);
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_position_follows_the_last_acknowledged_message_and_can_be_moved() {
    let f = fixture("position").await;
    let selection = Selection::group("broker");
    assert!(f.positions(&selection).await.is_empty(), "no contact yet");

    f.publish(&[message("kafka://orders", 1), message("kafka://orders", 2)])
        .await;
    let (seen, subscriber) = collector();
    f.outbox.dispatch(&subscriber, &selection).await.unwrap();

    let position = f.positions(&selection).await[0];
    assert!(position.transaction_id > 0);
    assert_eq!(position.offset, 2);

    // Back to the beginning: everything is delivered again.
    f.sessions
        .session(async |session| {
            f.outbox
                .set_position(&session, &selection, Position::default())
                .await
        })
        .await
        .unwrap();
    f.outbox.dispatch(&subscriber, &selection).await.unwrap();
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
        .dispatch(&orders, &Selection::group("broker").uri("kafka://orders"))
        .await
        .unwrap();
    f.outbox
        .dispatch(
            &payments,
            &Selection::group("broker").uri("kafka://payments"),
        )
        .await
        .unwrap();

    assert_eq!(*orders_seen.lock().unwrap(), [1, 2]);
    assert_eq!(*payments_seen.lock().unwrap(), [3]);
    // Each (group, uri) has a position of its own.
    assert_eq!(
        f.positions(&Selection::group("broker").uri("kafka://payments"))
            .await[0]
            .offset,
        3
    );
    assert!(f.positions(&Selection::group("broker")).await.is_empty());
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
    while f.outbox.dispatch(&subscriber, &selection).await.unwrap() {
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
                        .publish(&open, &message("kafka://orders", 1))
                        .await?;
                    // A later transaction commits while the first is open.
                    f.sessions
                        .session(async |session| {
                            session
                                .atomic(async |tx| {
                                    f.outbox.publish(&tx, &message("kafka://orders", 2)).await
                                })
                                .await
                        })
                        .await?;
                    assert!(!f.outbox.dispatch(&subscriber, &selection).await?);
                    Ok::<_, Error>(())
                })
                .await
        })
        .await
        .unwrap();

    f.wait_visible().await;
    assert!(f.outbox.dispatch(&subscriber, &selection).await.unwrap());
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
        f.outbox.dispatch(&failing, &selection).await,
        Err(Error::Subscriber(_))
    ));
    // The first contact created the positions inside the transaction that
    // rolled back, so there are none yet; either way nothing is acknowledged.
    assert!(
        f.positions(&selection)
            .await
            .iter()
            .all(|p| *p == Position::default())
    );

    let (seen, subscriber) = collector();
    assert!(f.outbox.dispatch(&subscriber, &selection).await.unwrap());
    assert_eq!(*seen.lock().unwrap(), [1]);
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn an_message_id_is_published_once() {
    let f = fixture("message_id").await;
    let first = message("kafka://orders", 1);
    let mut duplicate = message("kafka://orders", 2);
    duplicate.metadata = first.metadata.clone();
    f.publish(&[first]).await;

    let refused = f
        .sessions
        .session(async |session| {
            session
                .atomic(async |tx| f.outbox.publish(&tx, &duplicate).await)
                .await
        })
        .await;
    assert!(matches!(refused, Err(Error::Database(_))));
}

/// Two dispatchers of one group and one slot: the second finds the slot
/// held and nothing else with work, so no message is processed twice.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn two_dispatchers_of_one_group_do_not_overlap() {
    let f = fixture_concurrent("lock", 1).await;
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
        f.outbox.dispatch(slow(Arc::clone(&seen)), &selection),
        f.outbox.dispatch(slow(Arc::clone(&seen)), &selection),
    );

    assert!(a.unwrap() ^ b.unwrap(), "exactly one of them had the batch");
    assert_eq!(*seen.lock().unwrap(), [1, 2, 3]);
}

/// Two slots, two dispatchers, a subscriber that takes its time: the locks
/// are per slot, so the two run side by side, not one after the other. The
/// selection's position rows exist already: their creation, at first
/// contact, is one transaction's work and would serialize the pair.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn two_slots_are_dispatched_at_once() {
    let f = fixture_concurrent("parallel", 2).await;
    let selection = Selection::group("broker");
    let quick = |_: &OutboxMessage| async { Ok::<(), BoxError>(()) };
    assert!(!f.outbox.dispatch(quick, &selection).await.unwrap());
    let (first, second) = f.uris_in_different_slots().await;
    f.publish(&[message(&first, 1), message(&second, 2)]).await;
    let slow = |_: &OutboxMessage| async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok::<(), BoxError>(())
    };

    let started = std::time::Instant::now();
    let (a, b) = tokio::join!(
        f.outbox.dispatch(slow, &selection),
        f.outbox.dispatch(slow, &selection),
    );
    let elapsed = started.elapsed();

    assert_eq!((a.unwrap(), b.unwrap()), (true, true));
    assert!(
        elapsed < Duration::from_millis(550),
        "two slots took {elapsed:?}: one after the other"
    );
}

/// Two URIs in two slots, four messages each, two loops, a subscriber that
/// takes its time and notes when it ran: the messages of one URI are never
/// in the subscriber at the same time, while the two URIs are — the run is
/// shorter than the sum of the pauses, so the check could have seen an
/// overlap. One partition key, one dispatcher at a time.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn messages_of_one_uri_are_never_dispatched_at_once() {
    const PAUSE: Duration = Duration::from_millis(40);
    let f = fixture_concurrent("serial_uri", 2).await;
    let selection = Selection::group("broker");
    // the selection's position rows first: their creation at first contact is
    // one transaction's work, and the other loop would wait for its batch
    let quick = |_: &OutboxMessage| async { Ok::<(), BoxError>(()) };
    assert!(!f.outbox.dispatch(quick, &selection).await.unwrap());
    let (first, second) = f.uris_in_different_slots().await;
    let messages: Vec<OutboxMessage> = (1..=4)
        .flat_map(|i| [message(&first, i), message(&second, 10 + i)])
        .collect();
    f.publish(&messages).await;
    let spans: Arc<Mutex<Vec<(String, std::time::Instant, std::time::Instant)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let done = Arc::new(tokio::sync::Notify::new());
    let subscriber = {
        let (spans, done) = (Arc::clone(&spans), Arc::clone(&done));
        move |message: &OutboxMessage| {
            let (spans, done) = (Arc::clone(&spans), Arc::clone(&done));
            let uri = message.uri.clone();
            async move {
                let started = std::time::Instant::now();
                tokio::time::sleep(PAUSE).await;
                let mut spans = spans.lock().unwrap();
                spans.push((uri, started, std::time::Instant::now()));
                if spans.len() == 8 {
                    done.notify_one();
                }
                Ok::<(), BoxError>(())
            }
        }
    };
    let loops = Loops {
        concurrency: 2,
        poll_interval: Duration::from_millis(5),
        ..Loops::default()
    };

    let started = std::time::Instant::now();
    let shutdown = async { done.notified().await };
    tokio::time::timeout(
        Duration::from_secs(10),
        f.outbox.run(subscriber, &selection, loops, shutdown),
    )
    .await
    .expect("run stops after shutdown")
    .unwrap();
    let elapsed = started.elapsed();

    let spans = spans.lock().unwrap().clone();
    for uri in [&first, &second] {
        let mut own: Vec<_> = spans.iter().filter(|(u, _, _)| u == uri).collect();
        own.sort_by_key(|(_, started, _)| *started);
        assert_eq!(own.len(), 4);
        for pair in own.windows(2) {
            assert!(
                pair[0].2 <= pair[1].1,
                "two messages of {uri} were in the subscriber at once"
            );
        }
    }
    assert!(
        elapsed < PAUSE * 7,
        "the two URIs did not run side by side: {elapsed:?} for 8 pauses of {PAUSE:?}"
    );
}

/// Every keyed URI lands in exactly one of the slots — including the half
/// whose `hashtext` is negative, which the Python source lost — and a
/// dispatcher with no identity drains them all, one slot per call.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn slots_share_the_uris_without_gaps_or_overlap() {
    let f = fixture_slotted("workers", 3).await;
    f.publish(
        &(1..=40)
            .map(|i| message(&format!("kafka://orders/order-{i}"), i))
            .collect::<Vec<_>>(),
    )
    .await;
    let (seen, subscriber) = collector();
    let selection = Selection::group("broker").uri("kafka://orders");

    let mut batches = 0;
    while f.outbox.dispatch(&subscriber, &selection).await.unwrap() {
        batches += 1;
    }

    let mut all = seen.lock().unwrap().clone();
    all.sort_unstable();
    assert_eq!(all, (1..=40).collect::<Vec<_>>());
    assert_eq!(batches, 3, "one batch per slot with work");
    let positions = f.positions(&selection).await;
    assert_eq!(positions.len(), 3);
    assert!(positions.iter().all(|p| p.offset > 0));
}

/// `run` dispatches until told to stop, and stops between batches; two loops
/// share four slots through the locks alone.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn run_dispatches_until_shutdown() {
    let f = fixture_concurrent("run", 4).await;
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
    let loops = Loops {
        concurrency: 2,
        poll_interval: Duration::from_millis(20),
        ..Loops::default()
    };

    let shutdown = async { done.notified().await };
    tokio::time::timeout(
        Duration::from_secs(10),
        f.outbox
            .run(subscriber, &Selection::group("broker"), loops, shutdown),
    )
    .await
    .expect("run stops after shutdown")
    .unwrap();

    let mut all = seen.lock().unwrap().clone();
    all.sort_unstable();
    assert_eq!(all, [1, 2, 3, 4, 5, 6]);
}

/// A subscriber that fails does not stop `run`: the batch rolls back, the
/// loop waits and delivers it again (ADR-0010).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn run_outlives_a_failing_subscriber() {
    let f = fixture("flaky_run").await;
    f.publish(
        &(1..=3)
            .map(|i| message("kafka://orders", i))
            .collect::<Vec<_>>(),
    )
    .await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let done = Arc::new(tokio::sync::Notify::new());
    let calls = AtomicUsize::new(0);
    let subscriber = {
        let (seen, done) = (Arc::clone(&seen), Arc::clone(&done));
        move |message: &OutboxMessage| {
            let call = calls.fetch_add(1, Ordering::SeqCst);
            let mut seen = seen.lock().unwrap();
            let outcome = if call < 2 {
                Err::<(), BoxError>(format!("the broker is away, call {call}").into())
            } else {
                seen.push(id_of(message));
                Ok(())
            };
            if seen.len() == 3 {
                done.notify_one();
            }
            std::future::ready(outcome)
        }
    };
    let loops = Loops {
        concurrency: 1,
        poll_interval: Duration::from_millis(20),
        max_pause: Duration::from_millis(50),
    };

    let shutdown = async { done.notified().await };
    tokio::time::timeout(
        Duration::from_secs(10),
        f.outbox
            .run(subscriber, &Selection::group("broker"), loops, shutdown),
    )
    .await
    .expect("run goes on after the failures")
    .unwrap();

    assert_eq!(*seen.lock().unwrap(), [1, 2, 3]);
}

/// Two processes set the same tables up at once: both succeed, and the cut
/// is written once.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn two_migrations_at_once_agree() {
    let (outbox, offsets) = ("outbox_twice", "outbox_twice_offsets");
    let sessions = PgSessionPool::new(pool());
    let build = || {
        PgOutbox::new(PgSessionPool::new(pool()))
            .with_tables(
                Identifier::new(outbox).unwrap(),
                Identifier::new(offsets).unwrap(),
            )
            .with_slots(2)
    };
    let (first, second) = (build(), build());
    first.teardown().await.unwrap();

    let (a, b) = tokio::join!(first.migrate(), second.migrate());

    a.unwrap();
    b.unwrap();
    let cuts: i64 = sessions
        .session(async |session| {
            let sql = format!("SELECT count(*) FROM {outbox}_meta");
            Ok::<i64, Error>(session.connection().query_one(&sql, &[]).await?.get(0))
        })
        .await
        .unwrap();
    assert_eq!(cuts, 1);
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
                .atomic(async |tx| fake.publish(&tx, &message("kafka://orders", 1)).await)
                .await
        })
        .await
        .unwrap();

    assert_eq!(*fake.0.lock().unwrap(), ["kafka://orders"]);
}

/// Records what the outbox reports, in the words of the protocol model.
#[derive(Default)]
struct Recorder {
    events: Mutex<Vec<String>>,
    receipts: Mutex<Vec<Receipt>>,
    fetches: Mutex<Vec<(Option<u64>, Option<u64>)>>,
    acked: Mutex<Vec<Position>>,
}

impl Recorder {
    fn note(&self, event: impl Into<String>) {
        self.events.lock().unwrap().push(event.into());
    }
}

impl OutboxObserver for Recorder {
    fn on_published(&self, event: &Published<'_>) {
        self.receipts.lock().unwrap().push(event.receipt);
        self.note(format!("published {}", id_of(event.message)));
    }
    fn on_fetched(&self, event: &Fetched<'_>) {
        let newest = event.messages.iter().filter_map(|m| m.transaction_id).max();
        self.fetches
            .lock()
            .unwrap()
            .push((Some(event.horizon), newest));
        let ids: Vec<u64> = event.messages.iter().map(id_of).collect();
        self.note(format!("fetched {ids:?}"));
    }
    fn on_handled(&self, event: &Handled<'_>) {
        let outcome = if event.outcome.is_ok() {
            "ok"
        } else {
            "failed"
        };
        self.note(format!("handled {} {outcome}", id_of(event.message)));
    }
    fn on_acked(&self, event: &Acked<'_>) {
        self.acked.lock().unwrap().push(event.position);
        self.note("acked");
    }
    fn on_dispatched(&self, event: &Dispatched<'_>) {
        self.note(match event.outcome {
            Ok(true) => "dispatched a batch",
            Ok(false) => "dispatched nothing",
            Err(_) => "rolled back",
        });
    }
}

/// The observer sees the protocol the model in verify/tla/Outbox.tla is
/// written in: publish with the writing transaction's id, fetch with the
/// visibility horizon, one handling per message, the acknowledgement, the
/// close of the transaction — a rollback when the subscriber failed, and the
/// batch again afterwards.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_observer_sees_the_protocol() {
    let recorder = Arc::new(Recorder::default());
    let f = fixture_observed("observed", Arc::clone(&recorder), 1).await;
    let selection = Selection::group("broker");

    f.publish(&[message("kafka://orders", 1), message("kafka://orders", 2)])
        .await;
    f.wait_visible().await;
    let (_, subscriber) = collector();
    assert!(f.outbox.dispatch(&subscriber, &selection).await.unwrap());

    // The subscriber fails once: the batch is rolled back and comes again.
    f.publish(&[message("kafka://orders", 3)]).await;
    f.wait_visible().await;
    let failed_once = AtomicUsize::new(0);
    let flaky = |_: &OutboxMessage| {
        let first = failed_once.fetch_add(1, Ordering::SeqCst) == 0;
        std::future::ready(if first {
            Err::<(), BoxError>("the first attempt fails on purpose".into())
        } else {
            Ok(())
        })
    };
    assert!(f.outbox.dispatch(&flaky, &selection).await.is_err());
    assert!(f.outbox.dispatch(&flaky, &selection).await.unwrap());

    assert_eq!(
        *recorder.events.lock().unwrap(),
        [
            "published 1",
            "published 2",
            "fetched [1, 2]",
            "handled 1 ok",
            "handled 2 ok",
            "acked",
            "dispatched a batch",
            "published 3",
            "fetched [3]",
            "handled 3 failed",
            "rolled back",
            "fetched [3]",
            "handled 3 ok",
            "acked",
            "dispatched a batch",
        ]
    );

    // Every batch was read below a horizon past its newest transaction: the
    // visibility rule, as the dispatcher saw it.
    let fetches = recorder.fetches.lock().unwrap().clone();
    assert_eq!(fetches.len(), 3);
    assert!(
        fetches
            .iter()
            .all(|(horizon, newest)| match (horizon, newest) {
                (Some(horizon), Some(newest)) => newest < horizon,
                _ => false,
            })
    );

    // The receipt names the row the dispatcher later acknowledges.
    let receipts = recorder.receipts.lock().unwrap().clone();
    let acked = recorder.acked.lock().unwrap().clone();
    assert_eq!(receipts.len(), 3);
    assert!(
        receipts
            .windows(2)
            .all(|pair| pair[0].position < pair[1].position)
    );
    assert_eq!(
        acked,
        [
            Position {
                transaction_id: receipts[1].transaction_id,
                offset: receipts[1].position
            },
            Position {
                transaction_id: receipts[2].transaction_id,
                offset: receipts[2].position
            },
        ]
    );
}

/// A message under the identity `(tenant-1, orders.Order, {"id": stream},
/// position)` to `uri` (ADR-0030).
fn identified(uri: &str, id: u64, stream: &str, position: i32) -> OutboxMessage {
    let mut message = message(uri, id);
    let metadata = message.metadata.as_object_mut().unwrap();
    metadata.insert("tenant_id".to_owned(), json!("tenant-1"));
    metadata.insert("stream_type".to_owned(), json!("orders.Order"));
    metadata.insert(
        "stream_id".to_owned(),
        json!(format!(r#"{{"id":"{stream}"}}"#)),
    );
    metadata.insert("stream_position".to_owned(), json!(position.to_string()));
    message
}

impl<O: OutboxObserver> Fixture<O> {
    /// Writes `messages` in one transaction; what it answered.
    async fn publish_together(&self, messages: &[OutboxMessage]) -> Result<(), Error> {
        self.sessions
            .session(async |session| {
                session
                    .atomic(async |tx| {
                        for message in messages {
                            self.outbox.publish(&tx, message).await?;
                        }
                        Ok::<_, Error>(())
                    })
                    .await
            })
            .await
    }

    async fn rows(&self) -> i64 {
        let sql = format!("SELECT count(*) FROM {}", self.tables.0);
        self.sessions
            .session(async |session| {
                Ok::<_, Error>(session.connection().query_one(&sql, &[]).await?.get(0))
            })
            .await
            .unwrap()
    }
}

/// The incident ADR-0030 closes: a transaction writes two messages under one
/// identity to one channel — two keys of a topic are one channel — and the
/// inbox would keep the first and drop the second. The second is refused,
/// named, and nothing of the transaction commits.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_transaction_writes_one_message_per_identity_on_a_channel() {
    let f = fixture("identity").await;
    let refused = f
        .publish_together(&[
            identified("kafka://orders/order-1", 1, "a", 1),
            identified("kafka://orders/order-2", 2, "a", 1),
        ])
        .await;
    let Err(Error::IdentityTaken(identity)) = refused else {
        panic!("a second message under one identity is refused: {refused:?}");
    };
    assert_eq!(
        identity,
        Identity {
            channel: "kafka://orders".to_owned(),
            tenant_id: "tenant-1".to_owned(),
            stream_type: "orders.Order".to_owned(),
            stream_id: r#"{"id":"a"}"#.to_owned(),
            stream_position: "1".to_owned(),
        }
    );
    assert!(!Error::IdentityTaken(identity).is_transient());
    assert_eq!(f.rows().await, 0, "nothing of the transaction commits");
}

/// What the guard leaves alone: one identity on two channels (one fact in
/// two forms), in two transactions (a republication), a message carrying no
/// identity, and a write whose savepoint was rolled back; and what it holds
/// to: a write whose savepoint was released.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn an_identity_recurs_on_another_channel_in_another_transaction_or_after_a_rollback() {
    let f = fixture("identity_scope").await;
    f.publish_together(&[
        identified("kafka://orders", 1, "a", 1),
        identified("kafka://audit", 2, "a", 1),
    ])
    .await
    .expect("one identity on two channels");
    f.publish_together(&[identified("kafka://orders", 3, "a", 1)])
        .await
        .expect("one identity in another transaction");
    f.publish_together(&[message("kafka://orders", 4), message("kafka://orders", 5)])
        .await
        .expect("messages without an identity");
    assert_eq!(f.rows().await, 5);

    let rolled_back = f
        .sessions
        .session(async |session| {
            session
                .atomic(async |tx| {
                    let inner = tx
                        .atomic(async |savepoint| {
                            f.outbox
                                .publish(&savepoint, &identified("kafka://orders", 6, "b", 1))
                                .await?;
                            Err::<(), _>(Error::Malformed("roll the savepoint back".to_owned()))
                        })
                        .await;
                    assert!(inner.is_err());
                    f.outbox
                        .publish(&tx, &identified("kafka://orders", 7, "b", 1))
                        .await
                })
                .await
        })
        .await;
    assert!(
        rolled_back.is_ok(),
        "a rolled-back write frees its identity: {rolled_back:?}"
    );

    let released = f
        .sessions
        .session(async |session| {
            session
                .atomic(async |tx| {
                    tx.atomic(async |savepoint| {
                        f.outbox
                            .publish(&savepoint, &identified("kafka://orders", 8, "c", 1))
                            .await
                    })
                    .await?;
                    f.outbox
                        .publish(&tx, &identified("kafka://orders", 9, "c", 1))
                        .await
                })
                .await
        })
        .await;
    assert!(
        matches!(released, Err(Error::IdentityTaken(_))),
        "a released write keeps its identity: {released:?}"
    );
}

/// An outbox that took two messages under one identity before the guard:
/// the guard's migration names them — the identity, the channel and both
/// ids — rather than failing on an index that names a digest.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_guard_s_migration_names_what_a_transaction_wrote_twice_before_it() {
    let f = fixture("identity_history").await;
    let (first, second) = (
        identified("kafka://orders/order-1", 1, "a", 1),
        identified("kafka://orders/order-2", 2, "a", 1),
    );
    let table = f.tables.0.clone();
    let component = format!("outbox:{table}");
    f.sessions
        .session(async |session| {
            session
                .connection()
                .batch_execute(&format!(
                    "DROP INDEX {table}_identity_uniq; \
                     DELETE FROM ascetic_ddd_migrations \
                     WHERE component = '{component}' AND migration <> '20261027T000000Z_baseline'"
                ))
                .await?;
            Ok::<_, Error>(())
        })
        .await
        .unwrap();
    f.publish_together(&[first.clone(), second.clone()])
        .await
        .expect("no guard yet");
    let refused = f
        .outbox
        .migrate()
        .await
        .expect_err("the history breaks the guard");
    // The server's words are the cause's, under the runner's.
    let text = format!("{refused:?}");
    for named in [
        "kafka://orders",
        "orders.Order",
        first.metadata["message_id"].as_str().unwrap(),
        second.metadata["message_id"].as_str().unwrap(),
    ] {
        assert!(text.contains(named), "`{named}` is not named: {text}");
    }
}

/// A released migration is never edited (ADR-0029): every database that
/// applied it would refuse to start. A new shape is a new migration,
/// written here with its checksum when it is released.
#[test]
fn the_released_migrations_keep_their_checksums() {
    let outbox = PgOutbox::new(PgSessionPool::new(pool()));
    let checksums: Vec<_> = outbox
        .component()
        .migrations()
        .iter()
        .map(|migration| (migration.id().to_owned(), migration.checksum().to_owned()))
        .collect();
    assert_eq!(
        checksums,
        [
            (
                "20261027T000000Z_baseline".to_owned(),
                "fnv1a64:76ac925a2b946934:1394".to_owned()
            ),
            (
                "20261027T120000Z_one_message_per_identity".to_owned(),
                "fnv1a64:77bd7c9c075ddfa7:1511".to_owned()
            )
        ]
    );
}
