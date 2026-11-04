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
    Acked, Dispatched, EpochStarted, Fetched, Handled, OutboxObserver, Published, Receipt,
};
use ascetic_ddd_outbox::{
    BoxError, Error, Identity, Loops, Move, Outbox, OutboxMessage, PgOutbox, Position, Reclaiming,
    Reclamation, Ring, Selection, Skipped, Snapshot,
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

/// The same, its table a ring of its own numbers (ADR-0033).
async fn fixture_ring(name: &str, ring: Ring) -> Fixture {
    fixture_shaped(name, (), 1, ring, TraceFile::off()).await
}

async fn fixture_built<O: OutboxObserver>(
    name: &str,
    observer: O,
    slots: u32,
    trace: TraceFile,
) -> Fixture<O> {
    fixture_shaped(name, observer, slots, Ring::default(), trace).await
}

async fn fixture_shaped<O: OutboxObserver>(
    name: &str,
    observer: O,
    slots: u32,
    ring: Ring,
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
        .with_ring(ring)
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

    fn with_reclaiming(self, reclaiming: Reclaiming) -> Self {
        Fixture {
            outbox: self.outbox.with_reclaiming(reclaiming),
            ..self
        }
    }

    /// The ids of the messages the table holds, each with its writing
    /// transaction.
    async fn held(&self) -> Vec<(u64, u64)> {
        let mut held: Vec<(u64, u64)> = self
            .texts(
                "SELECT convert_from(payload, 'UTF8') || ' ' || transaction_id::text FROM {outbox}",
            )
            .await
            .iter()
            .map(|row| {
                let (id, xid) = row.split_once(' ').unwrap();
                (id.parse().unwrap(), xid.parse().unwrap())
            })
            .collect();
        held.sort_unstable();
        held
    }

    /// The ring's two bounds, as the meta table holds them.
    async fn bounds(&self) -> (u64, u64) {
        let row = &self
            .texts("SELECT gone_below::text || ' ' || intact_from::text FROM {outbox}_meta")
            .await[0];
        let (gone_below, intact_from) = row.split_once(' ').unwrap();
        (gone_below.parse().unwrap(), intact_from.parse().unwrap())
    }

    /// The next transaction id the server will assign, about.
    async fn next_xid(&self) -> u64 {
        self.texts("SELECT pg_snapshot_xmax(pg_current_snapshot())::text")
            .await[0]
            .parse()
            .unwrap()
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

    /// Publishes each message in a transaction of its own, without waiting:
    /// for a test that holds a transaction open, under which the table would
    /// never be older than every running one.
    async fn publish_now(&self, messages: &[OutboxMessage]) {
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
    }

    /// The writing transaction of the message whose payload is `id`.
    async fn xid_of(&self, id: u64) -> u64 {
        let sql = format!(
            "SELECT transaction_id::text FROM {} WHERE payload = $1",
            self.tables.0
        );
        let payload = id.to_string().into_bytes();
        self.sessions
            .session(async |session| {
                let row = session.connection().query_one(&sql, &[&payload]).await?;
                Ok::<u64, Error>(row.get::<_, String>(0).parse().unwrap())
            })
            .await
            .unwrap()
    }

    /// Dispatches until nothing is left.
    async fn drain<F, Fut>(&self, subscriber: F, selection: &Selection)
    where
        F: Fn(&OutboxMessage) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<(), BoxError>> + Send,
    {
        for _ in 0..20 {
            if !self.outbox.dispatch(&subscriber, selection).await.unwrap() {
                return;
            }
        }
        panic!("the outbox never ran dry");
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

    /// Runs `sql`, `{outbox}` standing for the outbox's table.
    async fn execute(&self, sql: &str) -> Result<(), Error> {
        let sql = sql.replace("{outbox}", &self.tables.0);
        self.sessions
            .session(async |session| {
                Ok::<_, Error>(session.connection().batch_execute(&sql).await?)
            })
            .await
    }

    /// The first column of every row of `sql`, as text, `{outbox}` standing
    /// for the outbox's table.
    async fn texts(&self, sql: &str) -> Vec<String> {
        let sql = sql.replace("{outbox}", &self.tables.0);
        self.sessions
            .session(async |session| {
                let rows = session.connection().query(&sql, &[]).await?;
                Ok::<_, Error>(rows.iter().map(|row| row.get(0)).collect())
            })
            .await
            .unwrap()
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

    // A batch shorter than its size drained the epoch: delivered sees both.
    let position = f.positions(&selection).await[0].clone();
    assert!(position.epoch.is_none());
    assert!(position.delivered.sees(f.xid_of(1).await));
    assert!(position.delivered.sees(f.xid_of(2).await));

    // Back to the start: everything is delivered again.
    f.sessions
        .session(async |session| {
            f.outbox
                .set_position(&session, &selection, Move::Start)
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
    assert!(
        f.positions(&Selection::group("broker").uri("kafka://payments"))
            .await[0]
            .delivered
            .sees(f.xid_of(3).await)
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
async fn an_open_transaction_delays_only_its_own_rows() {
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
                    // ADR-0007 handed nothing on past it; an epoch hands on
                    // the committed one (ADR-0035).
                    assert!(f.outbox.dispatch(&subscriber, &selection).await?);
                    assert_eq!(*seen.lock().unwrap(), [2]);
                    Ok::<_, Error>(())
                })
                .await
        })
        .await
        .unwrap();

    assert!(f.outbox.dispatch(&subscriber, &selection).await.unwrap());
    assert_eq!(*seen.lock().unwrap(), [2, 1]);
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
            .all(|p| p.delivered == Snapshot::nothing() && p.epoch.is_none())
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
    assert!(positions.iter().all(|p| p.delivered != Snapshot::nothing()));
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

/// What a fetch reported: the snapshot its batch was read under, the epoch,
/// what was delivered before it, and its rows' transactions.
#[derive(Clone)]
struct Fetch {
    statement: Option<Snapshot>,
    epoch: Option<Snapshot>,
    delivered: Option<Snapshot>,
    xids: Vec<u64>,
}

/// Records what the outbox reports, in the words of the protocol.
#[derive(Default)]
struct Recorder {
    events: Mutex<Vec<String>>,
    receipts: Mutex<Vec<Receipt>>,
    fetches: Mutex<Vec<Fetch>>,
    acked: Mutex<Vec<Option<Receipt>>>,
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
        self.fetches.lock().unwrap().push(Fetch {
            statement: event.statement.cloned(),
            epoch: event.epoch.cloned(),
            delivered: event.delivered.cloned(),
            xids: event
                .messages
                .iter()
                .filter_map(|m| m.transaction_id)
                .collect(),
        });
        let ids: Vec<u64> = event.messages.iter().map(id_of).collect();
        self.note(format!("fetched {ids:?}"));
    }
    fn on_epoch_started(&self, _event: &EpochStarted<'_>) {
        self.note("epoch started");
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
        self.acked.lock().unwrap().push(event.last);
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

/// The observer sees the protocol: publish with the writing transaction's
/// id, fetch with the epoch and what was delivered, one handling per
/// message, the acknowledgement, the close of the transaction — a rollback
/// when the subscriber failed, and the batch again afterwards.
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
            "epoch started",
            "fetched [1, 2]",
            "handled 1 ok",
            "handled 2 ok",
            "acked",
            "dispatched a batch",
            "published 3",
            "epoch started",
            "fetched [3]",
            "handled 3 failed",
            "rolled back",
            "epoch started",
            "fetched [3]",
            "handled 3 ok",
            "acked",
            "dispatched a batch",
        ]
    );

    // Every batch's rows are new in its epoch — visible in it, not in what
    // was delivered — and readable under the batch's own snapshot (ADR-0035).
    let fetches = recorder.fetches.lock().unwrap().clone();
    assert_eq!(fetches.len(), 3);
    assert!(fetches.iter().all(|fetch| {
        match (&fetch.statement, &fetch.epoch, &fetch.delivered) {
            (Some(statement), Some(epoch), Some(delivered)) => fetch
                .xids
                .iter()
                .all(|x| statement.sees(*x) && epoch.sees(*x) && !delivered.sees(*x)),
            _ => false,
        }
    }));

    // The receipt names the row the dispatcher later acknowledges.
    let receipts = recorder.receipts.lock().unwrap().clone();
    let acked = recorder.acked.lock().unwrap().clone();
    assert_eq!(receipts.len(), 3);
    assert!(
        receipts
            .windows(2)
            .all(|pair| pair[0].position < pair[1].position)
    );
    assert_eq!(acked, [Some(receipts[1]), Some(receipts[2])]);
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

/// A stream's versions leave in their writers' commit order, whatever took
/// its transaction id first (ADR-0035). A transaction that takes its id
/// early — the inbox's take writes its slot first — and writes version 6
/// after another wrote version 5 and committed: ADR-0007 handed 6 on first,
/// by transaction id.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_stream_s_versions_leave_in_commit_order_whatever_took_its_id_first() {
    let f = fixture("commit_order").await;
    let (seen, subscriber) = collector();
    let selection = Selection::group("broker");

    f.sessions
        .session(async |session| {
            session
                .atomic(async |early| {
                    early
                        .connection()
                        .query_one("SELECT pg_current_xact_id()", &[])
                        .await?;
                    f.publish_now(&[message("kafka://orders", 5)]).await;
                    f.outbox
                        .publish(&early, &message("kafka://orders", 6))
                        .await
                })
                .await
        })
        .await
        .unwrap();

    // The writer of 6 holds the lower id: ADR-0007's order would be 6, 5.
    assert!(f.xid_of(6).await < f.xid_of(5).await);
    f.drain(&subscriber, &selection).await;
    assert_eq!(*seen.lock().unwrap(), [5, 6]);
}

/// A transaction that commits while an epoch is handed on in several
/// batches is handed on once, in a later epoch: the drain stores the epoch,
/// not the snapshot of the moment (ADR-0035 decision 4).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_transaction_committing_between_an_epoch_s_batches_is_handed_on_once() {
    let f = fixture("between").await.with_batch_size(1);
    let (seen, subscriber) = collector();
    let selection = Selection::group("broker");

    f.sessions
        .session(async |session| {
            session
                .atomic(async |open| {
                    open.connection()
                        .query_one("SELECT pg_current_xact_id()", &[])
                        .await?;
                    f.publish_now(&[message("kafka://orders", 1), message("kafka://orders", 2)])
                        .await;
                    assert!(f.outbox.dispatch(&subscriber, &selection).await?);
                    f.outbox.publish(&open, &message("kafka://orders", 3)).await
                })
                .await
        })
        .await
        .unwrap();

    f.drain(&subscriber, &selection).await;
    assert_eq!(*seen.lock().unwrap(), [1, 2, 3]);
}

/// The sequence must draw one position at a time (ADR-0035 decision 6):
/// `migrate` refuses a cache of two, and so does an epoch's start, so that
/// an `ALTER SEQUENCE` made while the service runs stops the relay; and
/// `migrate` refuses a `position` that owns no sequence.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_sequence_must_draw_one_position_at_a_time() {
    let f = fixture("sequence").await;
    let selection = Selection::group("broker");
    let sequence_sql = format!(
        "SELECT pg_get_serial_sequence('{}', 'position')",
        f.tables.0
    );
    let sequence: String = f
        .sessions
        .session(async |session| {
            Ok::<_, Error>(
                session
                    .connection()
                    .query_one(&sequence_sql, &[])
                    .await?
                    .get(0),
            )
        })
        .await
        .unwrap();
    let alter = async |change: &str| {
        let sql = format!("ALTER SEQUENCE {sequence} {change}");
        f.sessions
            .session(async |session| {
                Ok::<_, Error>(session.connection().batch_execute(&sql).await?)
            })
            .await
            .unwrap();
    };

    alter("CACHE 2").await;
    assert!(matches!(
        f.outbox.migrate().await,
        Err(Error::SequenceCache(_))
    ));
    f.publish(&[message("kafka://orders", 1)]).await;
    let (seen, subscriber) = collector();
    assert!(matches!(
        f.outbox.dispatch(&subscriber, &selection).await,
        Err(Error::SequenceCache(_))
    ));
    assert!(seen.lock().unwrap().is_empty());

    alter("CACHE 1").await;
    f.outbox.migrate().await.unwrap();
    let owner = format!("{}.position", f.tables.0);
    alter("OWNED BY NONE").await;
    assert!(matches!(
        f.outbox.migrate().await,
        Err(Error::SequenceCache(_))
    ));
    alter(&format!("OWNED BY {owner}")).await;
    f.outbox.migrate().await.unwrap();
}

/// ADR-0007's tables, as its released baseline made them.
const RELEASED_BASELINE: &str = r#"
    CREATE TABLE IF NOT EXISTS {outbox} (
        "position" BIGSERIAL,
        "uri" VARCHAR(255) NOT NULL,
        "payload" BYTEA NOT NULL,
        "metadata" JSONB NOT NULL,
        "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        "transaction_id" xid8 NOT NULL,
        "slot" SMALLINT GENERATED ALWAYS AS ((hashtext("uri") & 2147483647) % 1) STORED,
        PRIMARY KEY ("transaction_id", "position")
    );
    CREATE INDEX IF NOT EXISTS {outbox}_slot_idx ON {outbox} ("slot", "transaction_id", "position");
    CREATE INDEX IF NOT EXISTS {outbox}_uri_position_idx
        ON {outbox} ("uri" varchar_pattern_ops, "transaction_id", "position");
    CREATE UNIQUE INDEX IF NOT EXISTS {outbox}_message_id_uniq
        ON {outbox} (((metadata->>'message_id')::uuid));
    CREATE TABLE IF NOT EXISTS {outbox}_meta ("slots" INTEGER NOT NULL);
    INSERT INTO {outbox}_meta ("slots") SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM {outbox}_meta);
    CREATE TABLE IF NOT EXISTS {offsets} (
        "consumer_group" VARCHAR(255) NOT NULL,
        "uri" VARCHAR(255) NOT NULL DEFAULT '',
        "slot" SMALLINT NOT NULL,
        "offset_acked" BIGINT NOT NULL DEFAULT 0,
        "last_processed_transaction_id" xid8 NOT NULL DEFAULT '0',
        "updated_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY ("consumer_group", "uri", "slot")
    );
"#;

/// ADR-0007's fetch, as its released binary runs it: a relay of the old rule
/// still running in a rolling deploy.
const RELEASED_FETCH: &str = r#"
    WITH taken AS (
        SELECT o.slot, o.offset_acked, o.last_processed_transaction_id
        FROM {offsets} o
        WHERE o.consumer_group = $1 AND o.uri = $2
          AND (SELECT m."position" FROM {outbox} m
               WHERE m.slot = o.slot
                 AND (m.transaction_id, m."position") > (o.last_processed_transaction_id, o.offset_acked)
                 AND m.transaction_id < pg_snapshot_xmin(pg_current_snapshot())
                 AND ($2 = '' OR m.uri = $2 OR m.uri LIKE $3)
               ORDER BY m.transaction_id, m."position"
               LIMIT 1) IS NOT NULL
        ORDER BY o.updated_at
        LIMIT 1
        FOR UPDATE OF o SKIP LOCKED
    )
    SELECT convert_from(m.payload, 'UTF8'), m.transaction_id::text
    FROM (SELECT 1) AS one
    LEFT JOIN taken t ON true
    LEFT JOIN LATERAL (
        SELECT "position", transaction_id, payload
        FROM {outbox}
        WHERE slot = t.slot
          AND (transaction_id, "position") > (t.last_processed_transaction_id, t.offset_acked)
          AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())
          AND ($2 = '' OR uri = $2 OR uri LIKE $3)
        ORDER BY transaction_id, "position"
        LIMIT 100
    ) AS m ON true
    WHERE m."position" IS NOT NULL
    ORDER BY m.transaction_id, m."position"
"#;

/// ADR-0007's positions convert exactly (ADR-0035 decision 10): `(L, off)`
/// hands on the rest of transaction `L` and what came after, nothing again;
/// a position never acknowledged, `L = 0`, hands on everything. Then a relay
/// of the old rule still running hands on from the pair the new one keeps
/// up, `(xmin(delivered) - 1, i64::MAX)`: nothing delivered, and what comes
/// later.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn adr_0007_s_positions_convert_exactly_and_its_relay_still_runs() {
    let tables = (
        "outbox_convert".to_owned(),
        "outbox_convert_offsets".to_owned(),
    );
    let sessions = PgSessionPool::new(pool());
    let outbox = PgOutbox::new(PgSessionPool::new(pool())).with_tables(
        Identifier::new(&tables.0).unwrap(),
        Identifier::new(&tables.1).unwrap(),
    );
    outbox.teardown().await.unwrap();
    let execute = async |sql: String| {
        sessions
            .session(async |session| {
                Ok::<_, Error>(session.connection().batch_execute(&sql).await?)
            })
            .await
            .unwrap();
    };
    execute(
        RELEASED_BASELINE
            .replace("{outbox}", &tables.0)
            .replace("{offsets}", &tables.1),
    )
    .await;

    // Transaction L writes 1 and 2; a later one writes 3.
    sessions
        .session(async |session| {
            session
                .atomic(async |tx| {
                    outbox.publish(&tx, &message("kafka://orders", 1)).await?;
                    outbox.publish(&tx, &message("kafka://orders", 2)).await
                })
                .await
        })
        .await
        .unwrap();
    sessions
        .session(async |session| {
            session
                .atomic(async |tx| outbox.publish(&tx, &message("kafka://orders", 3)).await)
                .await
        })
        .await
        .unwrap();
    // The old relay of `broker` acknowledged 1 and stopped; `fresh` never
    // acknowledged anything.
    execute(format!(
        "INSERT INTO {offsets} (consumer_group, uri, slot, offset_acked, last_processed_transaction_id) \
         SELECT 'broker', '', 0, \"position\", transaction_id FROM {outbox} \
         WHERE payload = convert_to('1', 'UTF8'); \
         INSERT INTO {offsets} (consumer_group, uri, slot) VALUES ('fresh', '', 0);",
        outbox = tables.0,
        offsets = tables.1
    ))
    .await;

    outbox.migrate().await.unwrap();
    let f = Fixture {
        sessions,
        outbox: outbox.observed_by(((), TraceFile::off().recorder())),
        tables,
        _trace: TraceFile::off(),
    };
    let (broker, broker_seen) = {
        let (seen, subscriber) = collector();
        (subscriber, seen)
    };
    f.drain(&broker, &Selection::group("broker")).await;
    assert_eq!(*broker_seen.lock().unwrap(), [2, 3]);
    let (fresh, fresh_seen) = {
        let (seen, subscriber) = collector();
        (subscriber, seen)
    };
    f.drain(&fresh, &Selection::group("fresh")).await;
    assert_eq!(*fresh_seen.lock().unwrap(), [1, 2, 3]);

    // The old relay, on the pair kept up: it takes nothing below delivered's
    // xmin — every transaction below it was handed on whole — and loses
    // nothing after; what it hands on again, identities drop.
    let from = f.positions(&Selection::group("broker")).await[0]
        .delivered
        .xmin;
    let fetch = RELEASED_FETCH
        .replace("{outbox}", &f.tables.0)
        .replace("{offsets}", &f.tables.1);
    let old_relay = async || {
        f.sessions
            .session(async |session| {
                let rows = session
                    .connection()
                    .query(&fetch, &[&"broker", &"", &"/%"])
                    .await?;
                Ok::<Vec<(String, u64)>, Error>(
                    rows.iter()
                        .map(|row| (row.get(0), row.get::<_, String>(1).parse().unwrap()))
                        .collect(),
                )
            })
            .await
            .unwrap()
    };
    assert!(old_relay().await.iter().all(|(_, xid)| *xid >= from));
    f.publish(&[message("kafka://orders", 4)]).await;
    let later = old_relay().await;
    assert!(later.iter().all(|(_, xid)| *xid >= from));
    assert!(later.iter().any(|(id, _)| id == "4"));
}

/// A rewind keeps of what was delivered only what lies below the
/// transaction it goes back to (ADR-0035 decision 11): a lower transaction
/// still running stays owed, where `'F:F:'` would mark it delivered.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_rewind_keeps_a_lower_transaction_still_running_owed() {
    let f = fixture("rewind_open").await;
    let (seen, subscriber) = collector();
    let selection = Selection::group("broker");

    f.sessions
        .session(async |session| {
            session
                .atomic(async |open| {
                    open.connection()
                        .query_one("SELECT pg_current_xact_id()", &[])
                        .await?;
                    f.publish_now(&[message("kafka://orders", 1)]).await;
                    assert!(f.outbox.dispatch(&subscriber, &selection).await?);
                    let from = f.xid_of(1).await;
                    f.sessions
                        .session(async |session| {
                            f.outbox
                                .set_position(&session, &selection, Move::From(from))
                                .await
                        })
                        .await?;
                    f.outbox.publish(&open, &message("kafka://orders", 2)).await
                })
                .await
        })
        .await
        .unwrap();

    f.drain(&subscriber, &selection).await;
    assert_eq!(*seen.lock().unwrap(), [1, 1, 2]);
}

/// A rewind clears the epoch in flight (ADR-0035 decision 11): left, the
/// epoch would drain and mark delivered what the rewind owes again.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_rewind_during_an_epoch_clears_it() {
    let f = fixture("rewind_epoch").await.with_batch_size(1);
    let (seen, subscriber) = collector();
    let selection = Selection::group("broker");

    f.publish(&[message("kafka://orders", 1), message("kafka://orders", 2)])
        .await;
    assert!(f.outbox.dispatch(&subscriber, &selection).await.unwrap());
    assert!(f.positions(&selection).await[0].epoch.is_some());
    let from = f.xid_of(1).await;
    f.sessions
        .session(async |session| {
            f.outbox
                .set_position(&session, &selection, Move::From(from))
                .await
        })
        .await
        .unwrap();
    assert!(f.positions(&selection).await[0].epoch.is_none());

    f.drain(&subscriber, &selection).await;
    assert_eq!(*seen.lock().unwrap(), [1, 1, 2]);
}

/// A look that finds nothing writes nothing (ADR-0035 decision 5): the
/// position row keeps its version.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn an_idle_look_writes_nothing() {
    let f = fixture("idle").await;
    let (_, subscriber) = collector();
    let selection = Selection::group("broker");
    f.publish(&[message("kafka://orders", 1)]).await;
    f.drain(&subscriber, &selection).await;

    let sql = format!(
        "SELECT xmin::text, updated_at::text FROM {} WHERE consumer_group = 'broker'",
        f.tables.1
    );
    let version = async || {
        f.sessions
            .session(async |session| {
                let row = session.connection().query_one(&sql, &[]).await?;
                Ok::<(String, String), Error>((row.get(0), row.get(1)))
            })
            .await
            .unwrap()
    };
    let before = version().await;
    assert!(!f.outbox.dispatch(&subscriber, &selection).await.unwrap());
    assert_eq!(version().await, before);
}

/// A counter reset (ADR-0035 decision 12): a position delivered past the
/// server's transaction ids stops the relay, and the refusal sticks — the
/// positions set right again do not lift it — until an operator clears it.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_counter_reset_stops_the_relay_until_an_operator_clears_it() {
    let f = fixture("reset").await;
    let (seen, subscriber) = collector();
    let selection = Selection::group("broker");
    f.publish(&[message("kafka://orders", 1)]).await;
    f.drain(&subscriber, &selection).await;
    let set = async |delivered: &str| {
        let sql = format!(
            "UPDATE {} SET delivered = '{delivered}'::pg_snapshot",
            f.tables.1
        );
        f.sessions
            .session(async |session| {
                Ok::<_, Error>(session.connection().batch_execute(&sql).await?)
            })
            .await
            .unwrap();
    };

    // As if restored into a server whose ids are far younger.
    set("4000000000:4000000000:").await;
    f.publish(&[message("kafka://orders", 2)]).await;
    assert!(matches!(
        f.outbox.dispatch(&subscriber, &selection).await,
        Err(Error::CounterReset(_))
    ));
    // Rebased by hand: still refused, until cleared.
    set("1:1:").await;
    assert!(matches!(
        f.outbox.dispatch(&subscriber, &selection).await,
        Err(Error::CounterReset(_))
    ));
    f.sessions
        .session(async |session| f.outbox.clear_counter_reset(&session).await)
        .await
        .unwrap();
    f.drain(&subscriber, &selection).await;
    assert_eq!(*seen.lock().unwrap(), [1, 1, 2]);
}

/// The backlog an operator reads: the epoch in flight with its age and
/// progress, and when delivered last moved (ADR-0035 decision 13). A drain
/// that finds the epoch's last row gone is reported, not waited for.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_backlog_shows_the_epoch_and_a_drain_short_of_its_end_is_reported() {
    #[derive(Default)]
    struct Shorts(Mutex<Vec<bool>>);
    impl OutboxObserver for Shorts {
        fn on_acked(&self, event: &Acked<'_>) {
            self.0.lock().unwrap().push(event.short);
        }
    }
    let shorts = Arc::new(Shorts::default());
    let f = fixture_observed("backlog", Arc::clone(&shorts), 1)
        .await
        .with_batch_size(1);
    let (seen, subscriber) = collector();
    let selection = Selection::group("broker");
    f.publish(&[
        message("kafka://orders", 1),
        message("kafka://orders", 2),
        message("kafka://orders", 3),
    ])
    .await;
    assert!(f.outbox.dispatch(&subscriber, &selection).await.unwrap());

    let backlog = f
        .sessions
        .session(async |session| f.outbox.backlog(&session, &selection).await)
        .await
        .unwrap();
    assert_eq!(backlog.len(), 1);
    assert!(backlog[0].epoch_age.is_some());
    assert!(backlog[0].epoch_end.unwrap() > backlog[0].epoch_offset);

    // The epoch's last row deleted by hand: the drain reaches short of it.
    let sql = format!("DELETE FROM {} WHERE payload = $1", f.tables.0);
    f.sessions
        .session(async |session| {
            Ok::<_, Error>(
                session
                    .connection()
                    .execute(&sql, &[&b"3".to_vec()])
                    .await?,
            )
        })
        .await
        .unwrap();
    f.drain(&subscriber, &selection).await;
    assert_eq!(*seen.lock().unwrap(), [1, 2]);
    assert!(shorts.0.lock().unwrap().contains(&true));

    let backlog = f
        .sessions
        .session(async |session| f.outbox.backlog(&session, &selection).await)
        .await
        .unwrap();
    assert!(backlog[0].epoch_age.is_none());
    assert!(backlog[0].delivered_age.is_some());
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
            ),
            (
                "20261104T000000Z_positions_as_snapshots".to_owned(),
                "fnv1a64:688e10e1ac7109ee:780".to_owned()
            ),
            (
                "20261104T000100Z_format".to_owned(),
                "fnv1a64:7c51ffb5c37066b8:79".to_owned()
            ),
            (
                "20261104T000200Z_refusal".to_owned(),
                "fnv1a64:8424073d750527ed:58".to_owned()
            ),
            (
                "20261104T000300Z_ring_meta".to_owned(),
                "fnv1a64:0a15fafee48cbcc5:293".to_owned()
            ),
            (
                "20261104T000400Z_epoch_generations".to_owned(),
                "fnv1a64:7505d909673d5216:65".to_owned()
            ),
            (
                "20261104T000500Z_ring".to_owned(),
                "fnv1a64:ff56d83b01572fb8:3084".to_owned()
            )
        ]
    );
}

/// Each row lands in its transaction's generation, in the partition named
/// after it, and a row stamped with another is refused (ADR-0033 decision 3).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_ring_holds_each_row_to_its_transaction_s_generation() {
    let ring = Ring::new(3, 2);
    let f = fixture_ring("ring_check", ring).await;
    f.publish(&[
        message("kafka://orders", 1),
        message("kafka://orders", 2),
        message("kafka://orders", 3),
    ])
    .await;
    let rows = f
        .texts(
            "SELECT transaction_id::text || ' ' || \"generation\" || ' ' || tableoid::regclass::text \
             FROM {outbox}",
        )
        .await;
    assert_eq!(rows.len(), 3);
    for row in rows {
        let [xid, generation, partition] = row.split(' ').collect::<Vec<_>>()[..] else {
            panic!("{row}");
        };
        let expected = ring.generation_of(xid.parse().unwrap());
        assert_eq!(generation, expected.to_string());
        assert_eq!(partition, format!("{}_g{expected}", f.tables.0));
    }
    let refused = f
        .execute(
            "INSERT INTO {outbox} (uri, payload, metadata, transaction_id, \"generation\") \
             VALUES ('kafka://orders', '\\x', '{}', pg_current_xact_id(), \
                     (pg_current_xact_id()::text::bigint / 2 + 1) % 3)",
        )
        .await;
    let Err(Error::Database(error)) = refused else {
        panic!("{refused:?}");
    };
    assert_eq!(
        error.code(),
        Some(&ascetic_ddd_session::pg::tokio_postgres::error::SqlState::CHECK_VIOLATION)
    );
}

/// The ring is fixed for the table's life: a process asking for other
/// numbers is refused at its start, against the meta table and against the
/// ring itself, each alone.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_process_with_another_ring_is_refused_at_start() {
    let f = fixture_ring("ring_refused", Ring::new(3, 4)).await;
    let other = |ring: Ring| {
        PgOutbox::new(PgSessionPool::new(pool()))
            .with_tables(
                Identifier::new(&f.tables.0).unwrap(),
                Identifier::new(&f.tables.1).unwrap(),
            )
            .with_ring(ring)
    };
    assert!(matches!(
        other(Ring::new(2, 4)).migrate().await,
        Err(Error::Malformed(_))
    ));
    assert!(matches!(
        other(Ring::new(3, 8)).migrate().await,
        Err(Error::Malformed(_))
    ));
    f.outbox.migrate().await.unwrap();

    f.execute("UPDATE {outbox}_meta SET window_ids = 8")
        .await
        .unwrap();
    assert!(matches!(f.outbox.migrate().await, Err(Error::Malformed(_))));
    f.execute("UPDATE {outbox}_meta SET window_ids = 4")
        .await
        .unwrap();
    f.execute("ALTER TABLE {outbox} DETACH PARTITION {outbox}_g2")
        .await
        .unwrap();
    let detached = f.outbox.migrate().await;
    f.execute("ALTER TABLE {outbox} ATTACH PARTITION {outbox}_g2 FOR VALUES IN (2)")
        .await
        .unwrap();
    assert!(matches!(detached, Err(Error::Malformed(_))));
    f.outbox.migrate().await.unwrap();
}

/// A ring's names stay within PostgreSQL's 63 bytes, or the table is
/// refused before anything is made: the server would cut them short.
#[tokio::test]
async fn a_table_name_too_long_for_its_ring_s_names_is_refused() {
    let outbox = PgOutbox::new(PgSessionPool::new(pool()))
        .with_tables(
            Identifier::new("o".repeat(40)).unwrap(),
            Identifier::new("o_offsets").unwrap(),
        )
        .with_ring(Ring::new(10_000, 1));
    assert!(matches!(outbox.migrate().await, Err(Error::Malformed(_))));
}

/// A group that lags a full lap of the ring, and more, still receives every
/// message, in order: an epoch spanning every window names every
/// generation, and each batch reads its generations from their first
/// positions.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_group_lagging_a_full_lap_receives_every_message() {
    let f = fixture_ring("ring_lap", Ring::new(2, 2))
        .await
        .with_batch_size(3);
    let (seen_ahead, ahead) = collector();
    let (seen_behind, behind) = collector();
    let (first, second) = (Selection::group("ahead"), Selection::group("behind"));
    f.publish(&[message("kafka://orders", 1)]).await;
    f.drain(&ahead, &first).await;
    f.drain(&behind, &second).await;
    let batch = |ids: std::ops::RangeInclusive<u64>| {
        ids.map(|id| message("kafka://orders", id))
            .collect::<Vec<_>>()
    };
    f.publish(&batch(2..=12)).await;
    f.drain(&ahead, &first).await;
    f.publish(&batch(13..=20)).await;
    f.drain(&ahead, &first).await;
    f.drain(&behind, &second).await;
    let all: Vec<u64> = (1..=20).collect();
    assert_eq!(*seen_ahead.lock().unwrap(), all);
    assert_eq!(*seen_behind.lock().unwrap(), all);
}

/// A look and its batch name the generations they read: a relation's lock
/// is held to the end of the dispatcher's transaction, and a fetch naming
/// every generation would hold off an emptying for as long as the relay is
/// busy (ADR-0033 decision 3).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_look_and_its_batch_lock_the_generations_they_name_alone() {
    let ring = Ring::new(4, 1 << 23);
    let f = fixture_ring("ring_locks", ring).await;
    f.publish(&[message("kafka://orders", 1)]).await;
    let named = format!("{}_g{}", f.tables.0, ring.generation_of(f.xid_of(1).await));
    let inside = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let subscriber = {
        let (inside, release) = (inside.clone(), release.clone());
        move |_: &OutboxMessage| {
            let (inside, release) = (inside.clone(), release.clone());
            async move {
                inside.notify_one();
                release.notified().await;
                Ok::<(), BoxError>(())
            }
        }
    };
    let pattern = format!("^{}_g[0-9]+$", f.tables.0);
    let watch = async {
        inside.notified().await;
        let locked = f
            .sessions
            .session(async |session| {
                let rows = session
                    .connection()
                    .query(
                        "SELECT DISTINCT c.relname::text FROM pg_locks l \
                         JOIN pg_class c ON c.oid = l.relation \
                         WHERE l.granted AND c.relname ~ $1 ORDER BY 1",
                        &[&pattern],
                    )
                    .await?;
                Ok::<Vec<String>, Error>(rows.iter().map(|row| row.get(0)).collect())
            })
            .await
            .unwrap();
        release.notify_one();
        locked
    };
    let broker = Selection::group("broker");
    let (dispatched, locked) = tokio::join!(f.outbox.dispatch(&subscriber, &broker), watch);
    assert!(dispatched.unwrap());
    assert_eq!(locked, [named]);
}

/// ADR-0007's tables become the ring from a bound fixed before its step
/// (ADR-0033 § Consequences): every row below it is gone, every row from
/// it on kept, in its own generation, with its position; a slot its
/// selection never matched does not pull the bound down to nothing; a
/// rewind below it is refused, and back to the start lands on it; a group
/// created after it receives what the ring holds; the new table owns the
/// position sequence.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_ring_is_copied_from_a_bound_fixed_before_its_step() {
    let tables = (
        "outbox_ring_copy".to_owned(),
        "outbox_ring_copy_offsets".to_owned(),
    );
    let sessions = PgSessionPool::new(pool());
    let outbox = PgOutbox::new(PgSessionPool::new(pool()))
        .with_tables(
            Identifier::new(&tables.0).unwrap(),
            Identifier::new(&tables.1).unwrap(),
        )
        .with_slots(2);
    outbox.teardown().await.unwrap();
    let f = Fixture {
        sessions,
        outbox: outbox.observed_by(((), TraceFile::off().recorder())),
        tables,
        _trace: TraceFile::off(),
    };
    f.execute(
        &RELEASED_BASELINE
            .replace("% 1)", "% 2)")
            .replace("SELECT 1 WHERE", "SELECT 2 WHERE")
            .replace("{offsets}", &f.tables.1),
    )
    .await
    .unwrap();
    let (uri, _) = f.uris_in_different_slots().await;
    f.publish_now(&[message(&uri, 1), message(&uri, 2), message(&uri, 3)])
        .await;
    let slot: i32 = f
        .texts(&format!(
            "SELECT ((hashtext('{uri}') & 2147483647) % 2)::text"
        ))
        .await[0]
        .parse()
        .unwrap();
    // `broker` handed on the first two; its other slot never matched a row
    f.execute(&format!(
        "INSERT INTO {offsets} (consumer_group, uri, slot, offset_acked, last_processed_transaction_id) \
         SELECT 'broker', '', {slot}, 9223372036854775807, transaction_id FROM {{outbox}} \
         WHERE payload = convert_to('2', 'UTF8'); \
         INSERT INTO {offsets} (consumer_group, uri, slot) VALUES ('broker', '', {other});",
        offsets = f.tables.1,
        other = 1 - slot,
    ))
    .await
    .unwrap();
    let last_position: i64 = f
        .texts("SELECT max(\"position\")::text FROM {outbox}")
        .await[0]
        .parse()
        .unwrap();

    f.outbox.migrate().await.unwrap();
    let intact_from: u64 = f.texts("SELECT intact_from::text FROM {outbox}_meta").await[0]
        .parse()
        .unwrap();
    assert!(intact_from > 1, "the idle slot pulled the bound to nothing");
    assert!(
        intact_from <= f.xid_of(2).await,
        "the bound passed a row owed"
    );
    let mut kept = Vec::new();
    for id in 1..=3 {
        let left = f
            .texts(&format!(
                "SELECT count(*)::text FROM {{outbox}} WHERE payload = convert_to('{id}', 'UTF8')"
            ))
            .await[0]
            == "1";
        let xid = if left { f.xid_of(id).await } else { 0 };
        assert_eq!(left, left && xid >= intact_from);
        if left {
            kept.push(id);
        }
    }
    assert!(kept.ends_with(&[2, 3]));
    let gone_below: u64 = f.texts("SELECT gone_below::text FROM {outbox}_meta").await[0]
        .parse()
        .unwrap();
    assert!(gone_below <= intact_from);

    let (seen, subscriber) = collector();
    let broker = Selection::group("broker");
    f.drain(&subscriber, &broker).await;
    assert_eq!(*seen.lock().unwrap(), [3]);
    let below = f
        .sessions
        .session(async |session| {
            f.outbox
                .set_position(&session, &broker, Move::From(intact_from - 1))
                .await
        })
        .await;
    assert!(
        matches!(below, Err(Error::Reclaimed { intact_from: bound, .. }) if bound == intact_from)
    );
    f.sessions
        .session(async |session| f.outbox.set_position(&session, &broker, Move::Start).await)
        .await
        .unwrap();
    seen.lock().unwrap().clear();
    f.drain(&subscriber, &broker).await;
    assert_eq!(*seen.lock().unwrap(), kept);

    let (fresh_seen, fresh) = collector();
    f.drain(&fresh, &Selection::group("fresh")).await;
    assert_eq!(*fresh_seen.lock().unwrap(), kept);

    f.publish(&[message(&uri, 4)]).await;
    let position: i64 = f
        .texts("SELECT \"position\"::text FROM {outbox} WHERE payload = convert_to('4', 'UTF8')")
        .await[0]
        .parse()
        .unwrap();
    assert!(position > last_position);
    assert_eq!(
        f.texts(
            "SELECT coalesce(pg_get_serial_sequence('{outbox}', 'position'), '') \
             WHERE to_regclass('{outbox}_ring') IS NULL"
        )
        .await
        .len(),
        1
    );
}

/// A lane — a selection of some URIs — starts where its group's selection
/// of everything stands: switching a relay to lanes hands nothing on again
/// (ADR-0033 decision 4). A group with no selection of everything starts
/// its lane at the beginning.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_lane_starts_where_its_group_s_selection_of_everything_stands() {
    let f = fixture("lane_start").await;
    let (seen, everything) = collector();
    f.publish(&[message("kafka://orders", 1), message("inbox://own", 2)])
        .await;
    f.drain(&everything, &Selection::group("relay")).await;
    assert_eq!(*seen.lock().unwrap(), [1, 2]);
    f.publish(&[message("kafka://orders", 3)]).await;
    let (lane_seen, lane) = collector();
    f.drain(&lane, &Selection::group("relay").uri("kafka:"))
        .await;
    assert_eq!(*lane_seen.lock().unwrap(), [3]);
    let (other_seen, other) = collector();
    f.drain(&other, &Selection::group("other").uri("kafka:"))
        .await;
    assert_eq!(*other_seen.lock().unwrap(), [1, 3]);
}

/// A pass of `outbox`'s reclamation, tried again while a transaction of
/// another test is open: "closed" is judged per database, and the tests
/// share one.
async fn reclaim_settled<O: OutboxObserver>(outbox: &PgOutbox<PgSessionPool, O>) -> Reclamation {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let outcome = outbox.reclaim().await.unwrap();
        if !matches!(outcome, Reclamation::Skipped(Skipped::NotClosed { .. }))
            || std::time::Instant::now() >= deadline
        {
            return outcome;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// How the reclamation's tests empty the ring: at once, a pass waiting a
/// fifth of a second for a partition's lock.
fn eager(margin: u32) -> Reclaiming {
    Reclaiming {
        interval: Duration::from_millis(10),
        margin,
        margin_age: Duration::ZERO,
        lock_timeout: Duration::from_millis(200),
    }
}

/// Generations every group passed go one by one, the generation of the
/// oldest row left first, until the generation still written into. A
/// generation holds every lap that wrote into it, so emptying it leaves
/// older windows of the others below `intact_from`: `gone_below` is the
/// least row left, `intact_from` one past the highest emptied, and every row
/// from it on is kept. A rewind below it is refused; back to the start, and
/// a group created after, receive what lies from it on and nothing below
/// (ADR-0033 decision 3).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn generations_every_group_passed_go_in_order_and_the_bounds_hold() {
    let ring = Ring::new(3, 2);
    let f = fixture_ring("reclaim_laps", ring)
        .await
        .with_reclaiming(eager(1));
    let batch = |ids: std::ops::RangeInclusive<u64>| {
        ids.map(|id| message("kafka://orders", id))
            .collect::<Vec<_>>()
    };
    f.publish(&batch(1..=30)).await;
    // the id written into now in another generation than the oldest row's
    let mut next_id = 31;
    loop {
        let oldest = f.held().await.iter().map(|(_, xid)| *xid).min().unwrap();
        if ring.generation_of(f.next_xid().await) != ring.generation_of(oldest) {
            break;
        }
        f.publish(&batch(next_id..=next_id)).await;
        next_id += 1;
    }
    let (a_seen, a) = collector();
    let (b_seen, b) = collector();
    let (first, second) = (Selection::group("a"), Selection::group("b"));
    f.drain(&a, &first).await;
    f.drain(&b, &second).await;

    // one generation emptied: the oldest row's, every lap of it
    let reclaim_checked = async || {
        let before = f.held().await;
        let oldest = before.iter().map(|(_, xid)| *xid).min();
        let outcome = reclaim_settled(&f.outbox).await;
        if let Reclamation::Emptied {
            generation,
            gone_below,
            intact_from,
            ..
        } = outcome
        {
            assert_eq!(Some(generation), oldest.map(|xid| ring.generation_of(xid)));
            let after = f.held().await;
            assert!(
                after
                    .iter()
                    .all(|(_, xid)| ring.generation_of(*xid) != generation)
            );
            assert!(
                before
                    .iter()
                    .filter(|(_, xid)| *xid >= intact_from)
                    .all(|row| after.contains(row))
            );
            assert!(after.iter().all(|(_, xid)| gone_below <= *xid));
            assert_eq!(f.bounds().await, (gone_below, intact_from));
        }
        outcome
    };
    let first_pass = reclaim_checked().await;
    assert!(
        matches!(first_pass, Reclamation::Emptied { .. }),
        "{first_pass:?}"
    );

    let (_, intact_from) = f.bounds().await;
    let held = f.held().await;
    let kept: Vec<u64> = held
        .iter()
        .filter(|(_, xid)| *xid >= intact_from)
        .map(|(id, _)| *id)
        .collect();
    assert!(
        held.iter().any(|(_, xid)| *xid < intact_from),
        "no older lap left below intact_from to tell the forms apart"
    );
    let below = f
        .sessions
        .session(async |session| {
            f.outbox
                .set_position(&session, &first, Move::From(intact_from - 1))
                .await
        })
        .await;
    assert!(matches!(below, Err(Error::Reclaimed { .. })));
    f.sessions
        .session(async |session| f.outbox.set_position(&session, &first, Move::Start).await)
        .await
        .unwrap();
    a_seen.lock().unwrap().clear();
    f.drain(&a, &first).await;
    assert_eq!(*a_seen.lock().unwrap(), kept);
    let (late_seen, late) = collector();
    let late_group = Selection::group("late");
    f.drain(&late, &late_group).await;
    assert_eq!(*late_seen.lock().unwrap(), kept);

    // the rest go in the same order, each the oldest row's, as the ids pass
    for _ in 0..6 {
        match reclaim_checked().await {
            Reclamation::Emptied { .. } => {}
            Reclamation::Skipped(Skipped::NotPast { .. } | Skipped::Empty) => break,
            other => panic!("{other:?}"),
        }
    }
    drop(b_seen);
}

/// A generation is kept while a transaction of this database holding an id
/// in it is open, while a group has not handed on its rows — the group
/// named — and while every group passed it for less than the margin's age,
/// a rewind restarting that time; a reader holding its partition past the
/// lock's timeout skips the pass, and dispatch goes on (ADR-0033
/// decision 3).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_generation_is_kept_while_open_unpassed_young_or_held() {
    let f = fixture_ring("reclaim_kept", Ring::new(2, 1 << 40))
        .await
        .with_reclaiming(eager(0));
    let (a_seen, a) = collector();
    let (_, b) = collector();
    let (first, second) = (Selection::group("a"), Selection::group("b"));
    f.publish(&[message("kafka://orders", 1), message("kafka://orders", 2)])
        .await;
    f.drain(&a, &first).await;
    f.sessions
        .session(async |session| f.outbox.set_position(&session, &second, Move::Start).await)
        .await
        .unwrap();
    // still written into: every row lies in the window of the next id
    let distant = beside(&f, Ring::new(2, 1 << 40)).with_reclaiming(eager(1));
    let near = distant.reclaim().await.unwrap();
    assert!(
        matches!(
            near,
            Reclamation::Skipped(Skipped::NotPast { generation: 0, .. })
        ),
        "{near:?}"
    );
    let pinned = reclaim_settled(&f.outbox).await;
    assert!(
        matches!(&pinned, Reclamation::Skipped(Skipped::NotPassed { group, .. }) if group == "b"),
        "{pinned:?}"
    );
    f.drain(&b, &second).await;

    // an open transaction holding an id in the generation
    let opened = Arc::new(tokio::sync::Notify::new());
    let close = Arc::new(tokio::sync::Notify::new());
    let holder = async {
        f.sessions
            .session(async |session| {
                session
                    .atomic(async |tx| {
                        let xid: String = tx
                            .connection()
                            .query_one("SELECT pg_current_xact_id()::text", &[])
                            .await?
                            .get(0);
                        opened.notify_one();
                        close.notified().await;
                        Ok::<u64, Error>(xid.parse().unwrap())
                    })
                    .await
            })
            .await
            .unwrap()
    };
    let check = async {
        opened.notified().await;
        let outcome = f.outbox.reclaim().await.unwrap();
        close.notify_one();
        outcome
    };
    let (_, outcome) = tokio::join!(holder, check);
    assert!(
        matches!(
            outcome,
            Reclamation::Skipped(Skipped::NotClosed { generation: 0, .. })
        ),
        "{outcome:?}"
    );

    // passed, but not long enough; a rewind restarts the time
    let patient = beside(&f, Ring::new(2, 1 << 40)).with_reclaiming(Reclaiming {
        margin_age: Duration::from_secs(3600),
        ..eager(0)
    });
    let young = |outcome: Reclamation| match outcome {
        Reclamation::Skipped(Skipped::NotOldEnough { passed_for, .. }) => passed_for,
        other => panic!("{other:?}"),
    };
    young(reclaim_settled(&patient).await);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let rewound_from = f.xid_of(1).await;
    patient_rewind(&f, &patient, &first, rewound_from).await;
    f.drain(&a, &first).await;
    assert!(young(reclaim_settled(&patient).await) < Duration::from_secs(1));

    // a reader holding the partition: the pass gives up, dispatch goes on
    let partition = format!("{}_g0", f.tables.0);
    let reading = Arc::new(tokio::sync::Notify::new());
    let done = Arc::new(tokio::sync::Notify::new());
    let reader = async {
        f.sessions
            .session(async |session| {
                session
                    .atomic(async |tx| {
                        tx.connection()
                            .query(&format!("SELECT 1 FROM {partition} LIMIT 1"), &[])
                            .await?;
                        reading.notify_one();
                        done.notified().await;
                        Ok::<(), Error>(())
                    })
                    .await
            })
            .await
            .unwrap()
    };
    let attempt = async {
        reading.notified().await;
        let outcome = reclaim_settled(&f.outbox).await;
        f.publish_now(&[message("kafka://orders", 3)]).await;
        a_seen.lock().unwrap().clear();
        let dispatched = f.outbox.dispatch(&a, &first).await.unwrap();
        done.notify_one();
        (outcome, dispatched)
    };
    let ((), (outcome, dispatched)) = tokio::join!(reader, attempt);
    assert_eq!(
        outcome,
        Reclamation::Skipped(Skipped::Held { generation: 0 })
    );
    assert!(dispatched);
    assert_eq!(*a_seen.lock().unwrap(), [3]);

    // nothing holds it any more
    f.drain(&b, &second).await;
    let emptied = reclaim_settled(&f.outbox).await;
    assert!(
        matches!(emptied, Reclamation::Emptied { generation: 0, .. }),
        "{emptied:?}"
    );
    assert!(f.held().await.is_empty());
    f.publish(&[message("kafka://orders", 4)]).await;
    a_seen.lock().unwrap().clear();
    f.drain(&a, &first).await;
    assert_eq!(*a_seen.lock().unwrap(), [4]);
}

/// Another outbox on the fixture's tables, of the same `ring`.
fn beside<O>(f: &Fixture<O>, ring: Ring) -> PgOutbox<PgSessionPool> {
    PgOutbox::new(PgSessionPool::new(pool()))
        .with_tables(
            Identifier::new(&f.tables.0).unwrap(),
            Identifier::new(&f.tables.1).unwrap(),
        )
        .with_ring(ring)
}

/// A rewind of `selection` to `from` through `outbox`.
async fn patient_rewind<O: OutboxObserver>(
    f: &Fixture,
    outbox: &PgOutbox<PgSessionPool, O>,
    selection: &Selection,
    from: u64,
) {
    f.sessions
        .session(async |session| {
            outbox
                .set_position(&session, selection, Move::From(from))
                .await
        })
        .await
        .unwrap();
}

/// An id of `delivered`'s in-progress list that committed a row into the
/// generation after it was taken: the generation is not passed, whatever
/// `delivered`'s `xmax` says, and the row is still handed on.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn an_in_progress_id_that_committed_into_the_generation_keeps_it() {
    let f = fixture_ring("reclaim_xip", Ring::new(2, 1 << 40))
        .await
        .with_reclaiming(eager(0));
    let (seen, subscriber) = collector();
    let selection = Selection::group("a");
    f.publish(&[message("kafka://orders", 1)]).await;
    f.drain(&subscriber, &selection).await;

    let written = Arc::new(tokio::sync::Notify::new());
    let commit = Arc::new(tokio::sync::Notify::new());
    let writer = async {
        f.sessions
            .session(async |session| {
                session
                    .atomic(async |tx| {
                        f.outbox.publish(&tx, &message("kafka://orders", 2)).await?;
                        written.notify_one();
                        commit.notified().await;
                        Ok::<(), Error>(())
                    })
                    .await
            })
            .await
            .unwrap()
    };
    let relay = async {
        written.notified().await;
        f.publish_now(&[message("kafka://orders", 3)]).await;
        f.drain(&subscriber, &selection).await;
        let delivered = f.positions(&selection).await[0].delivered.clone();
        commit.notify_one();
        delivered
    };
    let ((), delivered) = tokio::join!(writer, relay);
    assert!(delivered.xip.contains(&f.xid_of(2).await));
    let outcome = reclaim_settled(&f.outbox).await;
    assert!(
        matches!(outcome, Reclamation::Skipped(Skipped::NotPassed { .. })),
        "{outcome:?}"
    );
    f.drain(&subscriber, &selection).await;
    assert_eq!(*seen.lock().unwrap(), [1, 3, 2]);
}

/// An abandoned group is forgotten, so that it no longer pins the ring; a
/// group that moved a position within the margin's age is refused.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn an_abandoned_group_is_forgotten_and_a_live_one_refused() {
    let f = fixture_ring("reclaim_forget", Ring::new(2, 1 << 40))
        .await
        .with_reclaiming(eager(0));
    let (_, subscriber) = collector();
    f.publish(&[message("kafka://orders", 1)]).await;
    f.drain(&subscriber, &Selection::group("live")).await;
    f.sessions
        .session(async |session| {
            f.outbox
                .set_position(&session, &Selection::group("gone"), Move::Start)
                .await
        })
        .await
        .unwrap();
    let patient = beside(&f, Ring::new(2, 1 << 40)).with_reclaiming(Reclaiming {
        margin_age: Duration::from_secs(3600),
        ..eager(0)
    });
    let refused = f
        .sessions
        .session(async |session| patient.forget_group(&session, "gone").await)
        .await;
    assert!(matches!(refused, Err(Error::GroupActive { .. })));
    f.sessions
        .session(async |session| f.outbox.forget_group(&session, "gone").await)
        .await
        .unwrap();
    assert!(f.positions(&Selection::group("gone")).await.is_empty());
    let outcome = reclaim_settled(&f.outbox).await;
    assert!(
        matches!(outcome, Reclamation::Emptied { .. }),
        "{outcome:?}"
    );
}

/// A counter reset stops the reclamation as it stops the relay, found by
/// the reclamation itself, and recorded so that it sticks.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_counter_reset_stops_the_reclamation_without_a_relay_s_report() {
    let f = fixture_ring("reclaim_reset", Ring::new(2, 1 << 40))
        .await
        .with_reclaiming(eager(0));
    let (_, subscriber) = collector();
    f.publish(&[message("kafka://orders", 1)]).await;
    f.drain(&subscriber, &Selection::group("a")).await;
    f.execute(&format!(
        "UPDATE {} SET delivered = '900000000000:900000000000:'",
        f.tables.1
    ))
    .await
    .unwrap();
    assert!(matches!(
        f.outbox.reclaim().await,
        Err(Error::CounterReset(_))
    ));
    assert_eq!(
        f.texts("SELECT (refused IS NOT NULL)::text FROM {outbox}_meta")
            .await,
        ["true"]
    );
    assert_eq!(f.held().await.len(), 1);
}

/// A dispatcher's loops work the reclamation between their batches.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_dispatcher_s_loops_work_the_reclamation() {
    #[derive(Default)]
    struct Passes(AtomicUsize);
    impl OutboxObserver for Passes {
        fn on_reclamation(&self, _: &Reclamation) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let passes = Arc::new(Passes::default());
    let f = fixture_observed("reclaim_run", passes.clone(), 1)
        .await
        .with_reclaiming(eager(0));
    let (_, subscriber) = collector();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let selection = Selection::group("a");
    let run = f.outbox.run(
        &subscriber,
        &selection,
        Loops {
            concurrency: 2,
            poll_interval: Duration::from_millis(20),
            max_pause: Duration::from_millis(100),
        },
        async {
            let _ = stopped.await;
        },
    );
    let stopper = async {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while passes.0.load(Ordering::SeqCst) < 2 && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let _ = stop.send(());
    };
    let (ran, ()) = tokio::join!(run, stopper);
    ran.unwrap();
    assert!(passes.0.load(Ordering::SeqCst) >= 2);
}
