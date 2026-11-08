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
    Swept, Unparked, Waiting,
};
use ascetic_ddd_inbox::{
    ByStream, CausalDependency, Error, Failure, Inbox, InboxMessage, InboxRetention, Loops,
    MAX_IDENTITY_BYTES, Outcome, PartitionKey, PgInbox, Receipt, Retries,
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

/// The number of slots and the key are read at `migrate`, so they are set
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
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
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

    fn with_retention(self, retention: InboxRetention) -> Self {
        Fixture {
            inbox: self.inbox.with_retention(retention),
            ..self
        }
    }

    /// Runs `sql`, `{table}` standing for the inbox's table.
    async fn execute(&self, sql: &str) {
        let sql = sql.replace("{table}", &self.table);
        self.sessions
            .session(async |session| {
                Ok::<_, Error>(session.connection().batch_execute(&sql).await?)
            })
            .await
            .unwrap();
    }

    /// The labels of the rows the table holds, in order of arrival.
    async fn held(&self) -> Vec<String> {
        let sql = format!(
            "SELECT stream_id->>'id' || '@' || stream_position FROM {} ORDER BY received_position",
            self.table
        );
        self.sessions
            .session(async |session| {
                let rows = session.connection().query(&sql, &[]).await?;
                Ok::<Vec<String>, Error>(rows.iter().map(|row| row.get(0)).collect())
            })
            .await
            .unwrap()
    }

    /// Dispatches until nothing is left.
    async fn drain<F, Fut>(&self, subscriber: F)
    where
        F: Fn(&PgSession, &InboxMessage) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<(), Failure>> + Send,
    {
        for _ in 0..50 {
            if self.dispatch(&subscriber).await == Outcome::Nothing {
                return;
            }
        }
        panic!("the inbox never ran dry");
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

/// The `message_id` the message carries in its metadata.
fn message_id(message: &InboxMessage) -> String {
    message.metadata.as_ref().unwrap()["message_id"]
        .as_str()
        .unwrap()
        .to_owned()
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

    f.publish(&[once.clone(), once]).await;

    assert_eq!(f.rows().await, 1);
}

/// A message ignored under a stored identity names the one holding it
/// (ADR-0030): itself for a redelivery, the first for a second message the
/// producer gave one identity.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn an_ignored_message_names_the_one_holding_its_identity() {
    let recorder = Arc::new(Recorder::default());
    let f = fixture_observed("held_by", Arc::clone(&recorder)).await;
    let first = message("a", 1);
    let second = message("a", 1);
    f.publish(&[first.clone(), first.clone(), second.clone()])
        .await;
    let id = |message: &InboxMessage| message_id(message);
    assert_eq!(
        *recorder.held_by.lock().unwrap(),
        [None, Some(id(&first)), Some(id(&first))]
    );
    assert_ne!(id(&first), id(&second));
    assert_eq!(f.rows().await, 1);
}

/// The holder committed while the message's insert waited on it: the
/// insert's snapshot does not see it, the second statement's does.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_holder_committed_while_the_insert_waited_is_named() {
    let recorder = Arc::new(Recorder::default());
    let f = fixture_observed("held_by_concurrent", Arc::clone(&recorder)).await;
    let holder = message("a", 1);
    let late = message("a", 1);
    let mut client = pool().get().await.unwrap();
    let tx = client.transaction().await.unwrap();
    tx.execute(
        &format!(
            "INSERT INTO {} (tenant_id, stream_type, stream_id, stream_position, uri, payload, metadata) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
            f.table
        ),
        &[
            &holder.tenant_id,
            &holder.stream_type,
            &holder.stream_id,
            &holder.stream_position,
            &holder.uri,
            &holder.payload,
            &holder.metadata,
        ],
    )
    .await
    .unwrap();
    let inbox = &f.inbox;
    let waiting = async { inbox.publish(&late).await.unwrap() };
    let committing = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        tx.commit().await.unwrap();
    };
    tokio::join!(waiting, committing);
    assert_eq!(
        *recorder.held_by.lock().unwrap(),
        [Some(message_id(&holder))]
    );
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
/// A `message_id` held at another identity of its stream type is set aside,
/// never an error that would hold the source (ADR-0036 R2, R5).
async fn an_message_id_is_stored_once() {
    let f = fixture("message_id").await;
    let first = message("a", 1);
    let mut other = message("a", 2);
    other.metadata = first.metadata.clone();
    f.publish(&[first, other]).await;

    assert_eq!(f.rows().await, 1);
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
/// are per slot, so the two run side by side, not one after the other —
/// seen by the two runs overlapping, not by how long the pair took, a bound
/// that held the dispatchers' overhead too and failed on a busy machine
/// (2026-11-07).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn two_slots_are_worked_at_once() {
    let f = fixture_concurrent("parallel", 2).await;
    let (first, second) = f.streams_in_different_slots().await;
    f.publish(&[message(&first, 1), message(&second, 1)]).await;
    let spans: Arc<Mutex<Vec<(std::time::Instant, std::time::Instant)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let slow = {
        let spans = Arc::clone(&spans);
        move |_: &PgSession, _: &InboxMessage| {
            let spans = Arc::clone(&spans);
            async move {
                let started = std::time::Instant::now();
                tokio::time::sleep(Duration::from_millis(300)).await;
                spans
                    .lock()
                    .unwrap()
                    .push((started, std::time::Instant::now()));
                Ok::<(), Failure>(())
            }
        }
    };

    let (a, b) = tokio::join!(f.inbox.dispatch(&slow), f.inbox.dispatch(&slow));

    assert_eq!(
        (a.unwrap(), b.unwrap()),
        (Outcome::Processed, Outcome::Processed)
    );
    let spans = spans.lock().unwrap().clone();
    let [(a_from, a_to), (b_from, b_to)] = spans[..] else {
        panic!("two runs: {spans:?}");
    };
    assert!(
        a_from < b_to && b_from < a_to,
        "the two slots ran one after the other: {spans:?}"
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
/// — a run of one overlaps a run of the other, so the check could have seen
/// an overlap. One partition key, one dispatcher at a time.
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

    tokio::time::timeout(
        Duration::from_secs(10),
        f.inbox
            .run(subscriber, loops, async { done.notified().await }),
    )
    .await
    .expect("run stops after shutdown")
    .unwrap();

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
    // Measured by the runs themselves, not by the run's length: the length
    // bound held the dispatch's overhead too, and failed alone three times
    // in four (2026-11-07).
    let side_by_side = spans.iter().any(|(one, started, ended)| {
        spans
            .iter()
            .any(|(other, from, to)| one != other && started < to && from < ended)
    });
    assert!(
        side_by_side,
        "the two streams never ran side by side, so no overlap could have been seen: {spans:?}"
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

/// Two processes migrate the same table at once: both succeed, and the cut
/// is written once. Without the ledger's lock, both would see no cut and
/// write theirs.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn two_migrations_at_once_agree() {
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
    first.teardown().await.unwrap();

    let (a, b) = tokio::join!(first.migrate(), second.migrate());

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

/// A table cut into some number of slots is not re-cut by a restart asking
/// for another: that is a migration, and the start refuses.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn another_cut_is_refused() {
    let (table, sequence) = ("inbox_recut", "inbox_recut_seq");
    let build = |slots| {
        PgInbox::new(PgSessionPool::new(pool()))
            .with_table(
                Identifier::new(table).unwrap(),
                Identifier::new(sequence).unwrap(),
            )
            .with_slots(slots)
            .partitioned_by(ByStream)
    };
    let cut = build(2);
    cut.teardown().await.unwrap();
    cut.migrate().await.unwrap();

    let refused = build(3).migrate().await;

    assert!(
        matches!(&refused, Err(Error::Malformed(what)) if what.contains("2 slots")),
        "{refused:?}"
    );
    cut.migrate().await.unwrap();
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
    /// Each receipt's holder, when the message was ignored (ADR-0030).
    held_by: Mutex<Vec<Option<String>>>,
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
        self.held_by
            .lock()
            .unwrap()
            .push(event.held_by.map(str::to_owned));
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

/// A hole filled after its dependent's wait ran out — readmitted, or
/// arrived late — wakes the dependent, parked with the identity it waited
/// for: it is processed without an unpark (ADR-0036 R8). Before, the expiry
/// forgot the identity, and the dependent stayed parked for good.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_hole_filled_after_the_wait_ran_out_wakes_its_dependent() {
    let f = fixture("hole_after_expiry")
        .await
        .with_max_wait(Duration::from_millis(300));
    let cause = message("user", 5);
    let effect = message("order", 1).depending_on(&[CausalDependency::on(&cause)]);
    f.publish(std::slice::from_ref(&effect)).await;
    let (seen, subscriber) = collector();
    assert_eq!(f.dispatch(&subscriber).await, Outcome::SetAside);
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Nothing);
    assert_eq!(f.parked().await, ["order@1"], "the wait ran out");

    f.publish(std::slice::from_ref(&cause)).await;
    while f.dispatch(&subscriber).await != Outcome::Nothing {}

    assert_eq!(*seen.lock().unwrap(), ["user@5", "order@1"]);
    assert!(f.parked().await.is_empty(), "woken, not unparked by hand");
}

/// A row whose wait ran out, unparked by hand while its dependency is still
/// missing, waits again, and its wait runs out again: the unpark clears the
/// identity it kept (ADR-0036 R8), or the row would be neither in the queue
/// nor parked nor waiting with a time.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn an_unparked_row_whose_wait_ran_out_waits_again() {
    let f = fixture("unparked_after_expiry")
        .await
        .with_max_wait(Duration::from_millis(300));
    let cause = message("user", 5);
    let effect = message("order", 1).depending_on(&[CausalDependency::on(&cause)]);
    f.publish(std::slice::from_ref(&effect)).await;
    let (_, subscriber) = collector();
    assert_eq!(f.dispatch(&subscriber).await, Outcome::SetAside);
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Nothing);
    assert_eq!(f.parked().await, ["order@1"]);

    let unparked = f
        .sessions
        .session(async |session| f.inbox.unpark(&session, &effect).await)
        .await
        .unwrap();
    assert!(unparked);
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::SetAside,
        "waits again"
    );
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Nothing);
    assert_eq!(f.parked().await, ["order@1"], "and runs out again");
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

/// A released migration is never edited (ADR-0029): every database that
/// applied it would refuse to start. A new shape is a new migration,
/// written here with its checksum when it is released.
#[test]
fn the_released_migrations_keep_their_checksums() {
    let inbox = PgInbox::new(PgSessionPool::new(pool()));
    let checksums: Vec<_> = inbox
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
                "fnv1a64:e803015b7116f1bd:1917".to_owned()
            ),
            (
                "20261104T000000Z_processed_at".to_owned(),
                "fnv1a64:6452eff40ba9b9ec:245".to_owned()
            ),
            (
                "20261107T000000Z_rejects".to_owned(),
                "fnv1a64:b8887c3ee7dc490a:1046".to_owned()
            ),
            (
                "20261107T010000Z_indexes_before_launch".to_owned(),
                "fnv1a64:85998ae59b7e3b60:366".to_owned()
            )
        ]
    );
}

/// A message of `stream` on the channel `kafka://audit`, which the tests'
/// windows leave alone.
fn audited(stream: &str, position: i32) -> InboxMessage {
    InboxMessage {
        uri: format!("kafka://audit/{stream}"),
        ..message(stream, position)
    }
}

/// Forgetting at once: no window on `kafka://orders`, no grace, two rows a
/// statement.
fn forgetful() -> InboxRetention {
    let mut retention = InboxRetention::keep_everything().window("kafka://orders", Duration::ZERO);
    retention.interval = Duration::from_millis(20);
    retention.batch = 2;
    retention.grace = Duration::ZERO;
    retention
}

/// Another inbox on the fixture's table, forgetting as `retention` says.
fn beside<O>(f: &Fixture<O>, retention: InboxRetention) -> PgInbox<PgSessionPool> {
    PgInbox::new(PgSessionPool::new(pool()))
        .with_table(
            Identifier::new(&f.table).unwrap(),
            Identifier::new(format!("{}_seq", f.table)).unwrap(),
        )
        .partitioned_by(ByStream)
        .with_retention(retention)
}

/// A channel's processed rows go once its window has passed, in statements
/// of the batch's size; another channel's stay. A repeat that arrives after
/// is processed again: that is the window's meaning (ADR-0033 decision 4).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_channel_s_rows_go_after_its_window_and_a_later_repeat_is_processed_again() {
    let f = fixture("window_passed").await.with_retention(forgetful());
    let orders: Vec<InboxMessage> = (1..=5).map(|n| message(&format!("o{n}"), 1)).collect();
    f.publish(&orders).await;
    f.publish(&[audited("a1", 1)]).await;
    let (seen, subscriber) = collector();
    f.drain(&subscriber).await;
    assert_eq!(f.processed().await, 6);

    assert_eq!(f.inbox.sweep().await.unwrap(), 5);
    assert_eq!(f.held().await, ["a1@1"]);

    seen.lock().unwrap().clear();
    f.publish(&[orders[0].clone()]).await;
    f.drain(&subscriber).await;
    assert_eq!(*seen.lock().unwrap(), ["o1@1"]);
}

/// Within its window a repeat is dropped, as before.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn within_its_window_a_repeat_is_dropped() {
    let mut retention =
        InboxRetention::keep_everything().window("kafka://orders", Duration::from_secs(3600));
    retention.grace = Duration::ZERO;
    let f = fixture("window_open").await.with_retention(retention);
    let first = message("o1", 1);
    f.publish(std::slice::from_ref(&first)).await;
    let (seen, subscriber) = collector();
    f.drain(&subscriber).await;
    assert_eq!(f.inbox.sweep().await.unwrap(), 0);
    f.publish(&[first]).await;
    f.drain(&subscriber).await;
    assert_eq!(*seen.lock().unwrap(), ["o1@1"]);
    assert_eq!(f.rows().await, 1);
}

/// The sweep never forgets an unprocessed, parked or waiting row, nor a
/// processed row that a waiting row names; a parked row resolved by hand is
/// processed, and goes with its window.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn unprocessed_parked_waiting_and_named_rows_are_never_forgotten() {
    let f = fixture("window_kept")
        .await
        .with_retention(forgetful())
        .with_retries(Retries::up_to(1));
    let named = message("t", 1);
    let parked = message("p", 1);
    f.publish(&[named.clone(), message("x", 1), parked.clone()])
        .await;
    let (_, subscriber) = failing_on("p@1");
    f.drain(&subscriber).await;
    assert_eq!(f.parked().await, ["p@1"]);
    // a row set aside for a dependency not yet received, then made to name
    // the processed row `t@1`, as a mark racing the sweep would leave it
    let waiting = message("w", 1).depending_on(&[CausalDependency::on(&message("d", 1))]);
    f.publish(&[waiting]).await;
    assert_eq!(f.dispatch(&subscriber).await, Outcome::SetAside);
    f.execute(
        "UPDATE {table} SET waiting_for = jsonb_build_object( \
            'tenant_id', 'tenant-1', 'stream_type', 'orders.Order', \
            'stream_id', '{\"id\": \"t\"}'::jsonb, 'stream_position', 1) \
         WHERE stream_id = '{\"id\": \"w\"}'",
    )
    .await;
    f.publish(&[message("u", 1)]).await;

    assert_eq!(f.inbox.sweep().await.unwrap(), 1);
    assert_eq!(f.held().await, ["t@1", "p@1", "w@1", "u@1"]);

    assert!(
        f.sessions
            .session(async |session| f.inbox.resolve(&session, &parked).await)
            .await
            .unwrap()
    );
    assert_eq!(f.inbox.sweep().await.unwrap(), 1);
    assert_eq!(f.held().await, ["t@1", "w@1", "u@1"]);
}

/// The rule in force when the sweep runs applies, to rows processed before
/// it changed too.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_changed_rule_applies_to_rows_already_processed() {
    let f = fixture("window_changed").await;
    f.publish(&[message("o1", 1)]).await;
    let (_, subscriber) = collector();
    f.drain(&subscriber).await;
    assert_eq!(f.inbox.sweep().await.unwrap(), 0);
    assert_eq!(beside(&f, forgetful()).sweep().await.unwrap(), 1);
    assert_eq!(f.rows().await, 0);
}

/// Rows processed before the mark kept its time carry none and are kept
/// until the one-time pass stamps them, page by page; then they go with
/// their window.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn rows_processed_before_the_mark_s_time_are_kept_until_stamped() {
    let f = fixture("window_stamped").await.with_retention(forgetful());
    let orders: Vec<InboxMessage> = (1..=5).map(|n| message(&format!("o{n}"), 1)).collect();
    f.publish(&orders).await;
    f.publish(&[message("u", 1)]).await;
    let (_, subscriber) = failing_on("u@1");
    f.dispatch(&subscriber).await;
    f.dispatch(&subscriber).await;
    f.dispatch(&subscriber).await;
    f.dispatch(&subscriber).await;
    f.dispatch(&subscriber).await;
    f.execute("UPDATE {table} SET processed_at = NULL").await;
    assert_eq!(f.inbox.sweep().await.unwrap(), 0);
    assert_eq!(f.inbox.stamp_processed().await.unwrap(), 5);
    assert_eq!(f.inbox.stamp_processed().await.unwrap(), 0);
    assert_eq!(f.inbox.sweep().await.unwrap(), 5);
    assert_eq!(f.held().await, ["u@1"]);
}

/// The inbox's run sweeps beside its loops, and forgets nothing within its
/// grace.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_inbox_s_run_sweeps_beside_its_loops_after_its_grace() {
    #[derive(Default)]
    struct Sweeps(AtomicUsize);
    impl InboxObserver for Sweeps {
        fn on_swept(&self, event: &Swept<'_>) {
            self.0.fetch_add(event.forgotten as usize, Ordering::SeqCst);
        }
    }
    let run_for = async |inbox: &PgInbox<PgSessionPool, (Arc<Sweeps>, Arc<JsonTrace>)>,
                         sweeps: &Sweeps,
                         until: usize| {
        let (_, subscriber) = collector();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let run = inbox.run(
            &subscriber,
            Loops {
                concurrency: 1,
                poll_interval: Duration::from_millis(10),
                max_pause: Duration::from_millis(50),
            },
            async {
                let _ = stopped.await;
            },
        );
        let stopper = async {
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            while sweeps.0.load(Ordering::SeqCst) < until && std::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let _ = stop.send(());
        };
        let (ran, ()) = tokio::join!(run, stopper);
        ran.unwrap();
    };

    let sweeps = Arc::new(Sweeps::default());
    let mut patient = forgetful();
    patient.grace = Duration::from_secs(3600);
    let f = fixture_observed("window_run", Arc::clone(&sweeps))
        .await
        .with_retention(patient);
    f.publish(&[message("o1", 1)]).await;
    run_for(&f.inbox, &sweeps, usize::MAX).await;
    assert_eq!(f.processed().await, 1);
    assert_eq!(f.rows().await, 1, "forgotten within the grace");

    let f = f.with_retention(forgetful());
    run_for(&f.inbox, &sweeps, 1).await;
    assert_eq!(sweeps.0.load(Ordering::SeqCst), 1);
    assert_eq!(f.rows().await, 0);
}

/// A row stored before ADR-0037, its dependency list structured in its
/// metadata, still waits for its cause and is woken by it: the intake now
/// keeps the list as text, and dispatch reads both forms.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_row_stored_with_structured_dependencies_still_waits_for_them() {
    let f = fixture("causal_structured").await;
    let cause = message("user", 5);
    let effect = message("order", 1).depending_on(&[CausalDependency::on(&cause)]);
    assert!(
        effect
            .metadata
            .as_ref()
            .and_then(|m| m.get("causal_dependencies"))
            .is_some_and(serde_json::Value::is_array),
        "the list as the intake stored it before"
    );
    let client = pool().get().await.unwrap();
    client
        .execute(
            &format!(
                "INSERT INTO {} (tenant_id, stream_type, stream_id, stream_position, uri, payload, metadata) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
                f.table
            ),
            &[
                &effect.tenant_id,
                &effect.stream_type,
                &effect.stream_id,
                &effect.stream_position,
                &effect.uri,
                &effect.payload,
                &effect.metadata,
            ],
        )
        .await
        .unwrap();
    let (seen, subscriber) = collector();
    assert_eq!(f.dispatch(&subscriber).await, Outcome::SetAside, "it waits");

    f.publish(&[cause]).await;
    while f.dispatch(&subscriber).await != Outcome::Nothing {}

    assert_eq!(*seen.lock().unwrap(), ["user@5", "order@1"]);
}

/// Text no compression shrinks, `length` characters drawn from `alphabet`
/// by a fixed sequence: the btree compresses an item over about 510 bytes,
/// so a repeated filler would pass far past the bound (ADR-0037 decision 4).
fn incompressible(alphabet: &[char], length: usize, seed: u64) -> String {
    let mut state = seed | 1;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            alphabet[usize::try_from(state % alphabet.len() as u64).unwrap()]
        })
        .collect()
}

/// A message of `tenant`'s stream `stream_id` of `stream_type`, at the
/// largest position, whose size bound is what the waiting index takes.
fn cause_of(tenant: &str, stream_type: &str, stream_id: serde_json::Value) -> InboxMessage {
    static EVENT: AtomicU64 = AtomicU64::new(1);
    let event = EVENT.fetch_add(1, Ordering::Relaxed);
    InboxMessage::new(
        tenant,
        stream_type,
        stream_id,
        i32::MAX,
        "kafka://orders/cause",
        "cause",
    )
    .with_metadata(json!({ "message_id": format!("00000000-0000-4000-9000-{event:012x}") }))
}

/// The largest identity the intake admits, of each shape, is stored, waited
/// for and woken: its stored form fits the waiting index, whatever its
/// content (ADR-0037 decision 4). Mutation run on 2026-11-08: the bound
/// loosened by 8 bytes makes the string shape's wait fail with 54000. A
/// number counted short is not seen here, since the index compresses the
/// integers' item; the measured test below holds that term.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_largest_identity_admitted_is_stored_waited_for_and_woken() {
    let f = fixture("identity_bound").await;
    let alnum: Vec<char> = ('a'..='z').chain('A'..='Z').chain('0'..='9').collect();
    let cyrillic: Vec<char> = ('\u{0410}'..='\u{044F}').collect();
    let at_the_bound = |tenant: &str, stream_type: &str, filler: &dyn Fn(usize) -> String| {
        let empty = CausalDependency::on(&cause_of(tenant, stream_type, json!({ "id": "" })));
        let room = MAX_IDENTITY_BYTES - empty.stored_bytes();
        let id = filler(room);
        assert_eq!(id.len(), room, "the filler is exactly the room");
        cause_of(tenant, stream_type, json!({ "id": id }))
    };
    let shapes = [
        (
            "a string",
            at_the_bound("tenant-1", "orders.Order", &|room| {
                incompressible(&alnum, room, 7)
            }),
        ),
        (
            "two-byte characters",
            at_the_bound("tenant-1", "orders.Order", &|room| {
                let text = incompressible(&cyrillic, room / 2, 11);
                if room % 2 == 1 {
                    format!("{text}x")
                } else {
                    text
                }
            }),
        ),
        (
            "a tenant and a stream type at their columns",
            at_the_bound(
                &"t".repeat(128),
                &format!("orders.{}", "O".repeat(121)),
                &|room| incompressible(&alnum, room, 13),
            ),
        ),
        ("integers", {
            // Twenty digits each, every one another: alike, they compress.
            let dense = |count: usize| {
                let mut state = 0x9E37_79B9_7F4A_7C15_u64;
                let numbers: Vec<u64> = (0..count)
                    .map(|_| {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        10_000_000_000_000_000_000 + state % (u64::MAX - 10_000_000_000_000_000_000)
                    })
                    .collect();
                cause_of(
                    "tenant-1",
                    "orders.Order",
                    json!({ "id": "dense", "n": numbers }),
                )
            };
            let count = (1..)
                .take_while(|count| {
                    CausalDependency::on(&dense(*count)).stored_bytes() <= MAX_IDENTITY_BYTES
                })
                .last()
                .unwrap();
            dense(count)
        }),
    ];
    for (at, (shape, cause)) in shapes.into_iter().enumerate() {
        assert!(
            CausalDependency::on(&cause).stored_bytes() <= MAX_IDENTITY_BYTES,
            "{shape}"
        );
        let effect =
            message(&format!("effect{at}"), 1).depending_on(&[CausalDependency::on(&cause)]);
        let rows = f.rows().await;
        f.publish(std::slice::from_ref(&effect)).await;
        assert_eq!(f.rows().await, rows + 1, "{shape}: the effect is stored");
        let (seen, subscriber) = collector();
        assert_eq!(
            f.dispatch(&subscriber).await,
            Outcome::SetAside,
            "{shape}: it waits"
        );
        f.publish(std::slice::from_ref(&cause)).await;
        assert_eq!(f.rows().await, rows + 2, "{shape}: the cause is stored");
        while f.dispatch(&subscriber).await != Outcome::Nothing {}
        assert_eq!(seen.lock().unwrap().len(), 2, "{shape}: both processed");
        assert_eq!(
            seen.lock().unwrap()[1],
            label(&effect),
            "{shape}: the effect after"
        );
    }
}

/// One byte past the bound, a message and a message depending on it are
/// set aside at the intake, never left to fail at dispatch.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn an_identity_past_the_bound_is_set_aside_at_the_intake() {
    let f = fixture("identity_past").await;
    let alnum: Vec<char> = ('a'..='z').collect();
    let empty = CausalDependency::on(&cause_of("tenant-1", "orders.Order", json!({ "id": "" })));
    let room = MAX_IDENTITY_BYTES - empty.stored_bytes();
    let past = cause_of(
        "tenant-1",
        "orders.Order",
        json!({ "id": incompressible(&alnum, room + 1, 5) }),
    );
    let effect = message("effect", 1).depending_on(&[CausalDependency::on(&past)]);
    f.publish(&[past, effect]).await;
    assert_eq!(f.rows().await, 0);
}

/// A row stored before whose dependency list the intake would refuse is
/// parked for good, never waited on, and the rows behind it go on.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_row_stored_with_a_list_the_intake_refuses_is_parked_and_the_slot_goes_on() {
    let f = fixture("causal_refused").await;
    let legacy = message("legacy", 1).with_metadata(json!({
        "message_id": "00000000-0000-4000-a000-000000000001",
        "causal_dependencies": [{ "tenant_id": "tenant-1", "stream_type": "orders.Order",
                                  "stream_id": 1.5, "stream_position": 1 }],
    }));
    let client = pool().get().await.unwrap();
    client
        .execute(
            &format!(
                "INSERT INTO {} (tenant_id, stream_type, stream_id, stream_position, uri, payload, metadata) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
                f.table
            ),
            &[
                &legacy.tenant_id,
                &legacy.stream_type,
                &legacy.stream_id,
                &legacy.stream_position,
                &legacy.uri,
                &legacy.payload,
                &legacy.metadata,
            ],
        )
        .await
        .unwrap();
    f.publish(&[message("next", 1)]).await;
    let (seen, subscriber) = collector();
    while f.dispatch(&subscriber).await != Outcome::Nothing {}
    assert_eq!(
        *seen.lock().unwrap(),
        ["next@1"],
        "the subscriber never saw it"
    );
    assert_eq!(f.parked().await, ["legacy@1"]);
}

/// The bound on an identity's stored form dominates what PostgreSQL makes of
/// it, uncompressed, for every shape: strings of every width of character,
/// integers of every count of digits, booleans, nulls, containers within
/// containers, positions at both ends (ADR-0037 decision 4). The waiting
/// index compresses an item past about 510 bytes, so a wait at the bound
/// cannot tell a number counted short; this can. For a string the bound is
/// within a few bytes of the measure, so a looser one would admit an
/// identity the index refuses.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_bound_on_an_identity_dominates_its_stored_size() {
    let client = pool().get().await.unwrap();
    let measured = async |dependency: &CausalDependency| -> usize {
        let text = serde_json::to_string(dependency).unwrap();
        let row = client
            .query_one("SELECT pg_column_size($1::text::jsonb)", &[&text])
            .await
            .unwrap();
        usize::try_from(row.get::<_, i32>(0)).unwrap()
    };
    let identity = |stream_id: serde_json::Value, position: i32| CausalDependency {
        tenant_id: "tenant-1".to_owned(),
        stream_type: "orders.Order".to_owned(),
        stream_id,
        stream_position: position,
    };
    let mut shapes = vec![
        identity(json!("o-1"), 1),
        identity(json!(""), 0),
        identity(json!("ж".repeat(300)), i32::MAX),
        identity(json!("\u{1F600}".repeat(100)), i32::MIN),
        identity(json!(true), 1),
        identity(json!(null), 1),
        identity(json!([]), 1),
        identity(json!({}), 1),
        identity(json!([[[[["deep"]]]]]), 1),
        identity(json!({ "a": { "b": [1, "x", null, { "c": false }] } }), 1),
    ];
    for number in [
        json!(0),
        json!(1),
        json!(9999),
        json!(10000),
        json!(123_456_789),
        json!(i64::MIN),
        json!(i64::MAX),
        json!(u64::MAX),
    ] {
        shapes.push(identity(number.clone(), i32::MAX));
        shapes.push(identity(json!(vec![number; 50]), i32::MAX));
    }
    for dependency in &shapes {
        let (bound, size) = (dependency.stored_bytes(), measured(dependency).await);
        assert!(
            size <= bound,
            "{dependency:?}: measured {size}, bounded {bound}"
        );
    }
    for length in [0, 1, 100, 2_000] {
        let dependency = identity(json!("k".repeat(length)), i32::MAX);
        let (bound, size) = (dependency.stored_bytes(), measured(&dependency).await);
        assert!(
            bound - size <= 4,
            "a string's bound is tight: {bound} against {size}"
        );
    }
}
