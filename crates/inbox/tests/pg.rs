//! Integration tests for the PostgreSQL inbox. They need a live database:
//!
//! ```text
//! ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
//!     cargo test -p ascetic-ddd-inbox -- --ignored
//! ```
//!
//! With `ASCETIC_DDD_TRACE_DIR` set, every test with one dispatcher at a
//! time writes what its inbox reported as `inbox-<name>.jsonl` into that
//! directory, one event per line, for validation against the protocol model:
//! see `verify/tla/README.md`. A test with several dispatchers at once
//! records nothing: the order its events are logged in is not the order of
//! their commits; it asserts its outcome and the table's state instead.

use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ascetic_ddd_inbox::observer::{
    Dispatched, Expired, Failed, Fetched, Handled, InboxObserver, Marked, Received, Resolved,
    Unparked, Waiting,
};
use ascetic_ddd_inbox::{
    ByStream, CausalDependency, Error, Failure, Inbox, InboxMessage, Loops, Outcome, PartitionKey,
    PgInbox, Receipt, Retries,
};
use ascetic_ddd_session::pg::Identifier;
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
    /// Written when the fixture is dropped, if a trace directory is set.
    _trace: TraceFile,
}

/// A table of its own per test, so tests run in parallel; one slot.
async fn fixture(name: &str) -> Fixture {
    fixture_observed(name, ()).await
}

/// The same, watched by `observer`.
async fn fixture_observed<O: InboxObserver>(name: &str, observer: O) -> Fixture<O> {
    fixture_cut(name, observer, 1, recorded(name)).await
}

/// A table cut into `slots` by stream, the cut that keeps causal order.
async fn fixture_slotted(name: &str, slots: u32) -> Fixture {
    fixture_cut(name, (), slots, recorded(name)).await
}

/// The same for a test that runs several dispatchers at once, which records
/// no trace.
async fn fixture_concurrent(name: &str, slots: u32) -> Fixture {
    fixture_cut(name, (), slots, TraceFile::off()).await
}

fn recorded(name: &str) -> TraceFile {
    TraceFile::from_env(&format!("inbox-{name}"))
}

/// The number of slots and the key are read at `setup`, so they are set
/// before the table exists.
async fn fixture_cut<O: InboxObserver>(
    name: &str,
    observer: O,
    slots: u32,
    trace: TraceFile,
) -> Fixture<O> {
    let table = format!("inbox_{name}");
    let sequence = format!("inbox_{name}_seq");
    let sessions = PgSessionPool::new(pool());
    let inbox = PgInbox::new(PgSessionPool::new(pool()))
        .with_table(
            Identifier::new(&table).unwrap(),
            Identifier::new(&sequence).unwrap(),
        )
        .with_slots(slots)
        .partitioned_by(ByStream)
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
        _trace: trace,
    }
}

impl<O: InboxObserver> Fixture<O> {
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

    /// One `dispatch` call that must not fail on the database.
    async fn dispatch<F, Fut>(&self, subscriber: F) -> Outcome
    where
        F: Fn(&PgSession, &InboxMessage) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<(), Failure>> + Send,
    {
        self.inbox.dispatch(subscriber).await.unwrap()
    }

    async fn parked_messages(&self) -> Vec<InboxMessage> {
        self.sessions
            .session(async |session| self.inbox.parked(&session).await)
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

    /// Two stream ids that land in different slots, by the cut's own
    /// expression, for tests that need two slots busy at once.
    async fn streams_in_different_slots(&self) -> (String, String) {
        let sql = format!(
            "SELECT ((hashtext({}) & 2147483647) % $2)::int \
             FROM (SELECT 'tenant-1'::varchar AS tenant_id, 'orders.Order'::varchar AS stream_type, \
                          $1::jsonb AS stream_id) AS r",
            ByStream.sql_expression()
        );
        let slots = self.inbox.slots() as i32;
        let slot_of = |stream: String| {
            let sql = sql.clone();
            async move {
                self.sessions
                    .session(async |session| {
                        let row = session
                            .connection()
                            .query_one(&sql, &[&json!({ "id": stream }), &slots])
                            .await?;
                        Ok::<i32, Error>(row.get(0))
                    })
                    .await
                    .unwrap()
            }
        };
        let first = "s1".to_owned();
        let slot = slot_of(first.clone()).await;
        for i in 2.. {
            let candidate = format!("s{i}");
            if slot_of(candidate.clone()).await != slot {
                return (first, candidate);
            }
        }
        unreachable!("a second slot has some stream")
    }

    /// Makes the slot of `stream` the least recently served, so that the
    /// next take picks it.
    async fn serve_first(&self, stream: &str) {
        let sql = format!(
            "UPDATE {table}_slots SET served_at = served_at - interval '1 hour' \
             WHERE slot = (SELECT slot FROM {table} WHERE stream_id = $1 LIMIT 1)",
            table = self.table
        );
        self.sessions
            .session(async |session| {
                session
                    .connection()
                    .execute(&sql, &[&json!({ "id": stream })])
                    .await?;
                Ok::<(), Error>(())
            })
            .await
            .unwrap();
    }

    /// Rows waiting for a dependency that is processed: wakes lost. Must be
    /// none whatever the interleaving (ADR-0009).
    async fn lost_wakes(&self) -> i64 {
        let sql = format!(
            "SELECT count(*) FROM {table} w \
             WHERE w.waiting_for IS NOT NULL AND EXISTS ( \
                SELECT 1 FROM {table} d \
                WHERE d.tenant_id = w.waiting_for->>'tenant_id' \
                  AND d.stream_type = w.waiting_for->>'stream_type' \
                  AND d.stream_id = w.waiting_for->'stream_id' \
                  AND d.stream_position = (w.waiting_for->>'stream_position')::int \
                  AND d.processed_position IS NOT NULL)",
            table = self.table
        );
        self.sessions
            .session(async |session| {
                Ok::<i64, Error>(session.connection().query_one(&sql, &[]).await?.get(0))
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
    impl Fn(&PgSession, &InboxMessage) -> std::future::Ready<Result<(), Failure>>,
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
            Ok::<(), Failure>(())
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
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::SetAside,
        "the effect"
    );
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
        std::future::ready(Err::<(), Failure>("handler down".into()))
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
    impl Fn(&PgSession, &InboxMessage) -> std::future::Ready<Result<(), Failure>>,
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
    impl Fn(&PgSession, &InboxMessage) -> std::future::Ready<Result<(), Failure>>,
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
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::SetAside,
        "b@1 waits"
    );

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

/// Two dispatchers at once on one slot: the slot's row is locked by the
/// first, `SKIP LOCKED` passes the second by, and nothing is processed
/// twice or out of order.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_held_slot_is_passed_by() {
    let f = fixture_concurrent("skip_locked", 1).await;
    f.publish(&[message("a", 1), message("b", 1)]).await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let slow = |sink: Seen| {
        move |_: &PgSession, message: &InboxMessage| {
            let (sink, label) = (Arc::clone(&sink), label(message));
            async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                sink.lock().unwrap().push(label);
                Ok::<(), Failure>(())
            }
        }
    };

    let (a, b) = tokio::join!(
        f.inbox.dispatch(slow(Arc::clone(&seen))),
        f.inbox.dispatch(slow(Arc::clone(&seen))),
    );

    let mut outcomes = [a.unwrap(), b.unwrap()];
    outcomes.sort_by_key(|outcome| *outcome == Outcome::Processed);
    assert_eq!(outcomes, [Outcome::Nothing, Outcome::Processed]);
    assert_eq!(*seen.lock().unwrap(), ["a@1"]);
    assert_eq!(
        f.dispatch(slow(Arc::clone(&seen))).await,
        Outcome::Processed
    );
    assert_eq!(*seen.lock().unwrap(), ["a@1", "b@1"]);
}

/// Every stream lands in exactly one of the slots — including the half
/// whose `hashtext` is negative, which the Python source lost — and a
/// dispatcher without identity drains them all.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn slots_share_the_streams_without_gaps_or_overlap() {
    let f = fixture_slotted("slots", 3).await;
    f.publish(
        &(1..=40)
            .map(|i| message(&format!("stream-{i}"), 1))
            .collect::<Vec<_>>(),
    )
    .await;
    let (seen, subscriber) = collector();

    while f.dispatch(&subscriber).await != Outcome::Nothing {}

    let mut all = seen.lock().unwrap().clone();
    all.sort();
    let mut expected: Vec<String> = (1..=40).map(|i| format!("stream-{i}@1")).collect();
    expected.sort();
    assert_eq!(all, expected);
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn run_processes_until_shutdown() {
    let f = fixture_concurrent("run", 4).await;
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
            std::future::ready(Ok::<(), Failure>(()))
        }
    };
    let loops = Loops {
        concurrency: 2,
        poll_interval: Duration::from_millis(20),
        ..Loops::default()
    };

    tokio::time::timeout(
        Duration::from_secs(10),
        f.inbox
            .run(subscriber, loops, async { done.notified().await }),
    )
    .await
    .expect("run stops after shutdown")
    .unwrap();

    assert_eq!(seen.lock().unwrap().len(), 6);
    assert_eq!(f.processed().await, 6);
}

/// Two slots, two dispatchers, a subscriber that takes its time: the locks
/// are per slot, so the two run side by side, not one after the other.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn two_slots_are_worked_at_once() {
    let f = fixture_concurrent("parallel", 2).await;
    let (first, second) = f.streams_in_different_slots().await;
    f.publish(&[message(&first, 1), message(&second, 1)]).await;
    let slow = |_: &PgSession, _: &InboxMessage| async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok::<(), Failure>(())
    };

    let started = std::time::Instant::now();
    let (a, b) = tokio::join!(f.inbox.dispatch(slow), f.inbox.dispatch(slow));
    let elapsed = started.elapsed();

    assert_eq!(
        (a.unwrap(), b.unwrap()),
        (Outcome::Processed, Outcome::Processed)
    );
    assert!(
        elapsed < Duration::from_millis(550),
        "two slots took {elapsed:?}: one after the other"
    );
}

/// Tells the test when a row was set aside to wait: the moment the races
/// below are opened.
struct OnWaiting(Arc<tokio::sync::Notify>);

impl InboxObserver for OnWaiting {
    fn on_waiting(&self, _: &Waiting<'_>) {
        self.0.notify_one();
    }
}

/// A dispatcher that finds a dependency unprocessed sets its row aside and
/// commits, under a lock on the dependency's identity that the mark of the
/// dependency takes too. However the two interleave, the row is woken
/// (ADR-0009). Here the dependency has not arrived when the row is set
/// aside, and arrives and is processed in another slot right after.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_wait_set_while_its_dependency_arrives_and_is_marked_elsewhere_is_woken() {
    let waited = Arc::new(tokio::sync::Notify::new());
    let f = fixture_cut(
        "lost_wake",
        OnWaiting(Arc::clone(&waited)),
        2,
        TraceFile::off(),
    )
    .await;
    let (dependent, dependency) = f.streams_in_different_slots().await;
    let d = message(&dependency, 1);
    let m = message(&dependent, 1).depending_on(&[CausalDependency::on(&d)]);
    let behind = message(&dependent, 2);
    f.publish(&[m, behind]).await;
    let (seen, subscriber) = collector();

    let (a, b) = tokio::join!(f.inbox.dispatch(&subscriber), async {
        // m is being set aside; the dependency arrives and is processed in
        // its own slot, racing the commit of the wait
        waited.notified().await;
        f.publish(&[d]).await;
        f.inbox.dispatch(&subscriber).await
    });

    assert_eq!(
        (a.unwrap(), b.unwrap()),
        (Outcome::SetAside, Outcome::Processed)
    );
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Processed,
        "the dependent message was woken by the mark of its dependency"
    );
    assert_eq!(
        *seen.lock().unwrap(),
        [format!("{dependency}@1"), format!("{dependent}@1")]
    );
    assert_eq!(f.lost_wakes().await, 0);
}

/// The same race with the dependency already in the table, unprocessed, in
/// its own slot: the dependent's slot is served first, the dependency's
/// right after.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_wait_set_while_its_dependency_is_being_marked_elsewhere_is_woken() {
    let waited = Arc::new(tokio::sync::Notify::new());
    let f = fixture_cut(
        "lost_wake_in_flight",
        OnWaiting(Arc::clone(&waited)),
        2,
        TraceFile::off(),
    )
    .await;
    let (dependent, dependency) = f.streams_in_different_slots().await;
    let d = message(&dependency, 1);
    let m = message(&dependent, 1).depending_on(&[CausalDependency::on(&d)]);
    let behind = message(&dependent, 2);
    f.publish(&[d, m, behind]).await;
    // The dependent's slot is the least recently served, so it is taken first.
    f.serve_first(&dependent).await;
    let (seen, subscriber) = collector();

    let (a, b) = tokio::join!(f.inbox.dispatch(&subscriber), async {
        waited.notified().await;
        f.inbox.dispatch(&subscriber).await
    });

    assert_eq!(
        (a.unwrap(), b.unwrap()),
        (Outcome::SetAside, Outcome::Processed)
    );
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Processed,
        "the dependent message was woken by the mark of its dependency"
    );
    assert_eq!(
        *seen.lock().unwrap(),
        [format!("{dependency}@1"), format!("{dependent}@1")]
    );
    assert_eq!(f.lost_wakes().await, 0);
}

/// Several loops over several slots; messages with dependencies across
/// slots, published in random order while the loops run, so dependencies
/// arrive before and after their dependents; one poison message, parked and
/// resolved by hand. Everything ends processed, no lock cycle stops the
/// loops, and no row waits for a processed dependency: no wake was lost
/// (ADR-0009).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn loops_over_slots_with_dependencies_across_them_lose_no_wake() {
    const N: usize = 120;
    let f = fixture_concurrent("stress", 4)
        .await
        .with_retries(Retries::up_to(1))
        .with_max_wait(Duration::from_secs(60));
    // A fixed pseudo-random sequence, so the run is the same every time.
    let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = move || {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (seed >> 33) as usize
    };
    let mut messages: Vec<InboxMessage> = Vec::with_capacity(N);
    for i in 0..N {
        let mut m = message(&format!("s{}", i % 7), (i / 7) as i32 + 1);
        if i > 0 && next() % 2 == 0 {
            let earlier = &messages[next() % i];
            m = m.depending_on(&[CausalDependency::on(earlier)]);
        }
        messages.push(m);
    }
    let mut order: Vec<usize> = (0..N).collect();
    for i in (1..N).rev() {
        order.swap(i, next() % (i + 1));
    }
    let poison = label(&messages[N / 3]);
    let (seen, subscriber) = failing_on(&poison);
    let subscriber = |session: &PgSession, message: &InboxMessage| {
        let handled = subscriber(session, message);
        let pause = Duration::from_millis((message.received_position.unwrap_or(0) % 3) as u64);
        async move {
            tokio::time::sleep(pause).await;
            handled.await
        }
    };
    let done = Arc::new(tokio::sync::Notify::new());
    let loops = Loops {
        concurrency: 3,
        poll_interval: Duration::from_millis(5),
        ..Loops::default()
    };

    let publishing = async {
        for i in order {
            f.publish(std::slice::from_ref(&messages[i])).await;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    let operating = async {
        // resolves the parked poison as an operator would, and stops the
        // loops once everything is processed
        loop {
            tokio::time::sleep(Duration::from_millis(20)).await;
            for parked in f.parked_messages().await {
                f.sessions
                    .session(async |session| f.inbox.resolve(&session, &parked).await)
                    .await
                    .unwrap();
            }
            if f.processed().await == N as i64 {
                done.notify_one();
                return;
            }
        }
    };
    let running = tokio::time::timeout(
        Duration::from_secs(60),
        f.inbox
            .run(&subscriber, loops, async { done.notified().await }),
    );
    let ((), ran) = tokio::join!(publishing, async {
        // the operator stops with the loops, whichever way they stop
        tokio::pin!(running);
        tokio::select! {
            () = operating => running.await,
            ran = &mut running => ran,
        }
    });

    ran.expect("everything is processed within the minute")
        .expect("no loop fails, in particular on a lock cycle");
    assert_eq!(f.processed().await, N as i64);
    assert_eq!(f.lost_wakes().await, 0);
    assert_eq!(
        seen.lock().unwrap().len(),
        N - 1,
        "every message but the poison had its effects once"
    );
    assert!(f.parked().await.is_empty());
}

/// Two streams in two slots, four messages each, two loops, a subscriber
/// that takes its time and notes when it ran: the messages of one stream
/// are never in the subscriber at the same time, while the two streams are
/// — the run is shorter than the sum of the pauses, so the check could have
/// seen an overlap. One partition key, one dispatcher at a time.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn messages_of_one_stream_are_never_processed_at_once() {
    const PAUSE: Duration = Duration::from_millis(40);
    let f = fixture_concurrent("serial_stream", 2).await;
    let (first, second) = f.streams_in_different_slots().await;
    let messages: Vec<InboxMessage> = (1..=4)
        .flat_map(|i| [message(&first, i), message(&second, i)])
        .collect();
    f.publish(&messages).await;
    // each run of the subscriber: the stream and the span it took
    let spans: Arc<Mutex<Vec<(String, std::time::Instant, std::time::Instant)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let done = Arc::new(tokio::sync::Notify::new());
    let subscriber = {
        let (spans, done) = (Arc::clone(&spans), Arc::clone(&done));
        move |_: &PgSession, message: &InboxMessage| {
            let (spans, done) = (Arc::clone(&spans), Arc::clone(&done));
            let stream = message.stream_id["id"].as_str().unwrap().to_owned();
            async move {
                let started = std::time::Instant::now();
                tokio::time::sleep(PAUSE).await;
                let mut spans = spans.lock().unwrap();
                spans.push((stream, started, std::time::Instant::now()));
                if spans.len() == 8 {
                    done.notify_one();
                }
                Ok::<(), Failure>(())
            }
        }
    };
    let loops = Loops {
        concurrency: 2,
        poll_interval: Duration::from_millis(5),
        ..Loops::default()
    };

    let started = std::time::Instant::now();
    tokio::time::timeout(
        Duration::from_secs(10),
        f.inbox
            .run(subscriber, loops, async { done.notified().await }),
    )
    .await
    .expect("run stops after shutdown")
    .unwrap();
    let elapsed = started.elapsed();

    let spans = spans.lock().unwrap().clone();
    for stream in [&first, &second] {
        let mut own: Vec<_> = spans.iter().filter(|(s, _, _)| s == stream).collect();
        own.sort_by_key(|(_, started, _)| *started);
        assert_eq!(own.len(), 4);
        for pair in own.windows(2) {
            assert!(
                pair[0].2 <= pair[1].1,
                "two messages of stream {stream} were in the subscriber at once"
            );
        }
    }
    assert!(
        elapsed < PAUSE * 7,
        "the two streams did not run side by side: {elapsed:?} for 8 pauses of {PAUSE:?}"
    );
}

/// A subscriber's verdict that no retry will succeed parks the message at
/// once, attempts left or not, and the slot flows (ADR-0010).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_permanent_failure_parks_the_message_at_once() {
    let f = fixture("permanent").await;
    f.publish(&[message("a", 1), message("b", 1)]).await;
    let (seen, subscriber) = collector();
    let judging = |tx: &PgSession, message: &InboxMessage| {
        // the collector notes the message when called, so only the readable ones reach it
        let handled = (label(message) != "a@1").then(|| subscriber(tx, message));
        async move {
            match handled {
                Some(handled) => handled.await,
                None => Err(Failure::permanent("the payload cannot be read")),
            }
        }
    };

    assert_eq!(
        f.dispatch(&judging).await,
        Outcome::Failed {
            attempts: 1,
            parked: true
        }
    );
    assert_eq!(f.dispatch(&judging).await, Outcome::Processed, "b@1 flows");
    assert_eq!(f.dispatch(&judging).await, Outcome::Nothing);

    assert_eq!(*seen.lock().unwrap(), ["b@1"]);
    assert_eq!(f.parked().await, ["a@1"]);
    let parked = f.parked_messages().await;
    assert_eq!(
        parked[0].last_error.as_deref(),
        Some("the payload cannot be read")
    );
}

/// A loop that loses its connection — here the subscriber has the server
/// terminate it — waits and goes on; the message comes back and is
/// processed (ADR-0010).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_loop_outlives_a_lost_connection() {
    let f = fixture("lost_connection").await;
    f.publish(&[message("a", 1)]).await;
    let (seen, subscriber) = collector();
    let done = Arc::new(tokio::sync::Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let cutting = {
        let (calls, done) = (Arc::clone(&calls), Arc::clone(&done));
        move |tx: &PgSession, message: &InboxMessage| {
            let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
            // the collector notes the message when called: only the call that will succeed reaches it
            let handled = (!first).then(|| subscriber(tx, message));
            let (connection, done) = (tx.connection().clone(), Arc::clone(&done));
            async move {
                match handled {
                    None => {
                        connection
                            .execute("SELECT pg_terminate_backend(pg_backend_pid())", &[])
                            .await?;
                        Ok::<(), Failure>(())
                    }
                    Some(handled) => {
                        handled.await?;
                        done.notify_one();
                        Ok(())
                    }
                }
            }
        }
    };
    let loops = Loops {
        concurrency: 1,
        poll_interval: Duration::from_millis(20),
        max_pause: Duration::from_millis(100),
    };

    tokio::time::timeout(
        Duration::from_secs(10),
        f.inbox
            .run(&cutting, loops, async { done.notified().await }),
    )
    .await
    .expect("the loop goes on after the lost connection")
    .unwrap();

    assert_eq!(*seen.lock().unwrap(), ["a@1"]);
    assert_eq!(f.processed().await, 1);
}

/// A defect — here the subscriber renames the table under the dispatcher, so
/// the mark fails on an undefined table — stops the loops with the error;
/// the transaction rolled back, the table is as it was (ADR-0010).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_defect_stops_the_loops() {
    let f = fixture("defect").await;
    f.publish(&[message("a", 1)]).await;
    let table = f.table.clone();
    let breaking = move |tx: &PgSession, _: &InboxMessage| {
        let connection = tx.connection().clone();
        let rename = format!("ALTER TABLE {table} RENAME TO {table}_gone");
        async move {
            connection.batch_execute(&rename).await?;
            Ok::<(), Failure>(())
        }
    };

    let stopped = f
        .inbox
        .run(&breaking, Loops::default(), std::future::pending())
        .await;

    assert!(
        matches!(stopped, Err(Error::Database(_))),
        "the loops stop on the undefined table: {stopped:?}"
    );
    assert_eq!(f.processed().await, 0, "the table is as it was");
}

/// Two processes set the same table up at once: both succeed, and the cut
/// is written once. Without the lock, both would see no cut and write theirs.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn two_setups_at_once_agree() {
    let (table, sequence) = ("inbox_twice", "inbox_twice_seq");
    let sessions = PgSessionPool::new(pool());
    let build = || {
        PgInbox::new(PgSessionPool::new(pool()))
            .with_table(
                Identifier::new(table).unwrap(),
                Identifier::new(sequence).unwrap(),
            )
            .with_slots(2)
    };
    let (first, second) = (build(), build());
    let drop = format!(
        "DROP TABLE IF EXISTS {table}, {table}_meta, {table}_slots; DROP SEQUENCE IF EXISTS {sequence};"
    );
    sessions
        .session(async |session| {
            session.connection().batch_execute(&drop).await?;
            Ok::<(), Error>(())
        })
        .await
        .unwrap();

    let (a, b) = tokio::join!(
        sessions.session(async |session| first.setup(&session).await),
        sessions.session(async |session| second.setup(&session).await),
    );

    a.unwrap();
    b.unwrap();
    let cuts: i64 = sessions
        .session(async |session| {
            let sql = format!("SELECT count(*) FROM {table}_meta");
            Ok::<i64, Error>(session.connection().query_one(&sql, &[]).await?.get(0))
        })
        .await
        .unwrap();
    assert_eq!(cuts, 1);
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
    fn on_dispatched(&self, event: &Dispatched<'_>) {
        self.note(match event.outcome {
            Ok(Outcome::Processed) => "dispatched a message",
            Ok(Outcome::Failed { .. }) => "dispatched a failure",
            Ok(Outcome::SetAside) => "dispatched a set-aside",
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

    // The dependent arrives first and is set aside until the other is
    // processed.
    f.publish(&[second.clone(), first.clone()]).await;
    assert_eq!(f.dispatch(&subscriber).await, Outcome::SetAside);
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
            Err::<(), Failure>("the first attempt fails on purpose".into())
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
            "dispatched a set-aside",
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

    assert_eq!(f.dispatch(&subscriber).await, Outcome::SetAside);
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
