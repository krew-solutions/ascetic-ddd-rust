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
    ByStream, CausalDependency, Error, Expiry, Failure, Ground, Hold, Inbox, InboxMessage,
    InboxRetention, Loops, MAX_IDENTITY_BYTES, Outcome, Parked, PartitionKey, PgInbox, Receipt,
    Retries, Rotation,
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

    fn with_rotation(self, rotation: Rotation) -> Self {
        Fixture {
            inbox: self.inbox.with_rotation(rotation),
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

    /// Dispatches until nothing is left, and fails the test, rather than
    /// hanging it, when the inbox has not run dry within fifty dispatches
    /// and three a row the table holds. The bound is generous, not a count
    /// the protocol guarantees: the largest drain here takes 1,001 rows in
    /// as many dispatches. An unbounded loop let a walk that never ended its
    /// round hang the suite for hours, its trace growing in memory.
    async fn drain<F, Fut>(&self, subscriber: F)
    where
        F: Fn(&PgSession, &InboxMessage) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<(), Failure>> + Send,
    {
        let sql = format!("SELECT count(*) FROM {}", self.table);
        let rows = self
            .sessions
            .session(async |session| {
                Ok::<i64, Error>(session.connection().query_one(&sql, &[]).await?.get(0))
            })
            .await
            .unwrap();
        let bound = 50 + 3 * rows;
        let mut last = Outcome::Nothing;
        for _ in 0..bound {
            last = self.dispatch(&subscriber).await;
            if last == Outcome::Nothing {
                return;
            }
        }
        panic!("the inbox never ran dry: {bound} dispatches over {rows} rows, the last {last:?}");
    }

    /// One `dispatch` call that must not fail on the database.
    async fn dispatch<F, Fut>(&self, subscriber: F) -> Outcome
    where
        F: Fn(&PgSession, &InboxMessage) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<(), Failure>> + Send,
    {
        self.inbox.dispatch(subscriber).await.unwrap()
    }

    /// One statement of the expiry that must not fail on the database.
    async fn expire(&self) -> Expiry {
        self.inbox.expire().await.unwrap()
    }

    async fn parked_messages(&self) -> Vec<Parked> {
        self.sessions
            .session(async |session| self.inbox.parked(&session, None, 1000).await)
            .await
            .unwrap()
    }

    async fn parked(&self) -> Vec<String> {
        self.parked_messages()
            .await
            .iter()
            .map(|parked| match &parked.identity {
                Some(identity) => format!(
                    "{}@{}",
                    identity.stream_id["id"].as_str().unwrap_or("?"),
                    identity.stream_position
                ),
                None => format!("?@{}", parked.received_position),
            })
            .collect()
    }

    /// The order of arrival of `message`, parked.
    async fn position_of(&self, message: &InboxMessage) -> i64 {
        let wanted = label(message);
        let labels = self.parked().await;
        let at = labels
            .iter()
            .position(|parked| *parked == wanted)
            .unwrap_or_else(|| panic!("{wanted} is not parked: {labels:?}"));
        self.parked_messages().await[at].received_position
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

    f.drain(&subscriber).await;

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
    f.drain(&subscriber).await;

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
            parked: false,
            held: false
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
            parked: false,
            held: false
        }
    );
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Failed {
            attempts: 2,
            parked: true,
            held: false
        }
    );
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Processed);
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Nothing);

    assert_eq!(*seen.lock().unwrap(), ["b@1"]);
    assert_eq!(f.parked().await, ["a@1"]);
    assert_eq!(f.rows().await, 2, "a parked row stays in the table");
    let parked = f
        .sessions
        .session(async |session| f.inbox.parked(&session, None, 1000).await)
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
            parked: false,
            held: false
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
            parked: true,
            held: false
        }
    );
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Nothing);

    let at = f.position_of(&poison).await;
    let unparked = f
        .sessions
        .session(async |session| f.inbox.unpark(&session, at).await)
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
            parked: true,
            held: false
        }
    );
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::SetAside,
        "b@1 waits"
    );

    let at = f.position_of(&cause).await;
    let resolved = f
        .sessions
        .session(async |session| f.inbox.resolve(&session, at).await)
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

    f.drain(&subscriber).await;

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
                    .session(async |session| {
                        f.inbox.resolve(&session, parked.received_position).await
                    })
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
            parked: true,
            held: false
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
        max_error_wait: Duration::from_millis(100),
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
            Ok(Outcome::Parked) => "dispatched a park",
            Ok(Outcome::Nothing) => "dispatched nothing",
            Ok(Outcome::Passed) => "dispatched a pass",
            Ok(Outcome::Held) => "dispatched a hold",
            Ok(Outcome::Paused) => "dispatched a paused walk",
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
            parked: false,
            held: false
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
    assert_eq!(f.expire().await.parked, 1);
    assert_eq!(f.parked().await, ["order@1"], "the wait ran out");

    f.publish(std::slice::from_ref(&cause)).await;
    f.drain(&subscriber).await;

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
    assert_eq!(f.expire().await.parked, 1);
    assert_eq!(f.parked().await, ["order@1"]);

    let at = f.position_of(&effect).await;
    let unparked = f
        .sessions
        .session(async |session| f.inbox.unpark(&session, at).await)
        .await
        .unwrap();
    assert!(unparked);
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::SetAside,
        "waits again"
    );
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(f.expire().await.parked, 1);
    assert_eq!(f.parked().await, ["order@1"], "and runs out again");
}

/// A message whose dependency never arrives waits out of the queue, costing
/// the walk nothing, and with `max_wait` set is parked with the dependency
/// named (ADR-0006) — by the expiry, not by a dispatch, which takes its
/// slot's work and no other (ADR-0042 decision 7).
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
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Nothing);
    assert!(f.parked().await.is_empty(), "no dispatch expires a row");
    assert_eq!(
        f.expire().await,
        Expiry {
            parked: 1,
            more: false
        },
        "expired and parked"
    );
    assert_eq!(f.parked().await, ["order@1"]);
    let parked = f
        .sessions
        .session(async |session| f.inbox.parked(&session, None, 1000).await)
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
    let at = f.position_of(&effect).await;
    let unparked = f
        .sessions
        .session(async |session| f.inbox.unpark(&session, at).await)
        .await
        .unwrap();
    assert!(unparked);
    f.drain(&subscriber).await;
    assert_eq!(*seen.lock().unwrap(), ["user@5", "order@1"]);
}

/// A released migration is never edited (ADR-0029): every database that
/// applied it would refuse to start. A new shape is a new migration,
/// written here with its checksum when it is released. One exception, while
/// nothing is deployed: the baseline took ADR-0041's schema on 2026-11-15,
/// by the owner's decision (ADR-0041 decision 6), and the take's and the
/// walk's functions the same day; on 2026-11-17 it took ADR-0042's rows —
/// a row's cause, the count and kind of its holds, their CHECKs, the held
/// index, the guest's cursor and the walk's paused channels — and ADR-0040's
/// causes migration with them, and the same day the expiry's function, its
/// index's key, the CHECK that a row with a time of wait waits, and the
/// stalls' table; the
/// databases made before are recreated. Once the library is live, the
/// exception is gone: the checksum covers the functions' bodies, so an edit
/// of a statement is then a new function by a new migration, and ADR-0042
/// § Owed lists the steps.
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
                "fnv1a64:c65014f42a4e75f5:19613".to_owned()
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
            ),
            (
                "20261108T000000Z_unreadable_at".to_owned(),
                "fnv1a64:d9799e13a5481289:68".to_owned()
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

    let at = f.position_of(&parked).await;
    assert!(
        f.sessions
            .session(async |session| f.inbox.resolve(&session, at).await)
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
                max_error_wait: Duration::from_millis(50),
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
    // the row is written behind the observer: no trace (no run of the model)
    let f = fixture_cut("causal_structured", (), 1, TraceFile::off()).await;
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
    f.drain(&subscriber).await;

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
                &"t".repeat(ascetic_ddd_bus::identity::TENANT_ID_WIDTH),
                &format!(
                    "orders.{}",
                    "O".repeat(ascetic_ddd_bus::identity::STREAM_TYPE_WIDTH - "orders.".len())
                ),
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
        f.drain(&subscriber).await;
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
    // the row is written behind the observer: no trace (no run of the model)
    let f = fixture_cut("causal_refused", (), 1, TraceFile::off()).await;
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
    f.drain(&subscriber).await;
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

/// Inserts a row as a release before the bus's domain and ADR-0037 could
/// have stored it, its values whatever SQL writes.
/// When the row of `stream_id` arrived, in seconds since the epoch, as the
/// table holds it.
async fn received_at<O>(f: &Fixture<O>, stream_id: &str) -> f64 {
    let client = pool().get().await.unwrap();
    client
        .query_one(
            &format!(
                "SELECT extract(epoch FROM received_at)::float8 FROM {} \
                 WHERE stream_id = $1::text::jsonb",
                f.table
            ),
            &[&stream_id],
        )
        .await
        .unwrap()
        .get(0)
}

async fn stored_before<O>(f: &Fixture<O>, stream_id: &str, metadata: &str) {
    let client = pool().get().await.unwrap();
    client
        .execute(
            &format!(
                "INSERT INTO {} (tenant_id, stream_type, stream_id, stream_position, uri, payload, metadata) \
                 VALUES ('tenant-1', 'orders.Order', $1::text::jsonb, 1, 'kafka://orders/old', 'old', $2::text::jsonb)",
                f.table
            ),
            &[&stream_id, &metadata],
        )
        .await
        .unwrap();
}

/// The channels and orders of arrival the observer was told rows were
/// parked at once under.
#[derive(Clone, Default)]
struct AtOnce(Arc<Mutex<Vec<(String, i64)>>>);

impl InboxObserver for AtOnce {
    fn on_parked_at_once(&self, event: &ascetic_ddd_inbox::observer::ParkedAtOnce<'_>) {
        self.0
            .lock()
            .unwrap()
            .push((event.channel.to_owned(), event.received_position));
    }
}

/// The parked rows' order of arrival and last error, read without their
/// values, which may not decode.
async fn parked_places<O>(f: &Fixture<O>) -> Vec<(i64, Option<String>)> {
    let client = pool().get().await.unwrap();
    client
        .query(
            &format!(
                "SELECT received_position, last_error FROM {} WHERE parked_at IS NOT NULL \
                 ORDER BY received_position",
                f.table
            ),
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

/// A head the dispatcher cannot read for its own values is parked at once
/// in its transaction, its subscriber not run, and the next row of its slot
/// is processed (ADR-0037 decision 6): a metadata 128 deep, which the
/// client's reader refuses, and a stream id past 64 bits, which would reach
/// the handler as another id. Before, the first panicked the dispatcher's
/// task and the second stopped every loop at its mark.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_head_that_cannot_be_read_is_parked_at_once_and_the_slot_goes_on() {
    for (ground, stream_id, metadata) in [
        (
            "a metadata 128 deep",
            r#"{"id": "deep"}"#.to_owned(),
            format!(r#"{{"h": {}{}}}"#, "[".repeat(127), "]".repeat(127)),
        ),
        (
            "a stream id past 64 bits",
            "12345678901234567890123".to_owned(),
            "{}".to_owned(),
        ),
    ] {
        // No trace: the row is stored by SQL, and a trace names only rows it
        // saw received.
        let at_once = AtOnce::default();
        let f = fixture_cut("unreadable", at_once.clone(), 1, TraceFile::off()).await;
        stored_before(&f, &stream_id, &metadata).await;
        // Its key holds a line break: the event names the channel, never the
        // key, which is the producer's text.
        pool()
            .get()
            .await
            .unwrap()
            .batch_execute(&format!(
                "UPDATE {} SET uri = 'kafka://orders/t1' || chr(10) || 'inbox: forged'",
                f.table
            ))
            .await
            .unwrap();
        f.publish(&[message("next", 1)]).await;
        let (seen, subscriber) = collector();
        assert_eq!(f.dispatch(&subscriber).await, Outcome::Parked, "{ground}");
        assert_eq!(
            *at_once.0.lock().unwrap(),
            [("kafka://orders".to_owned(), 1)],
            "{ground}: the park names its channel"
        );
        f.drain(&subscriber).await;
        assert_eq!(
            *seen.lock().unwrap(),
            ["next@1"],
            "{ground}: the slot goes on"
        );
        let parked = parked_places(&f).await;
        assert_eq!(parked.len(), 1, "{ground}: {parked:?}");
        assert!(
            parked[0].1.as_deref().is_some_and(|why| !why.is_empty()),
            "{ground}: the reason is kept"
        );
    }
}

/// A failure's text is kept without NUL and within its width, never failing
/// the record: a NUL raised 22021 and stopped every loop at every poll
/// (ADR-0037 decision 7).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_failure_with_a_nul_is_recorded_and_the_row_tried_again() {
    let f = fixture("nul_failure").await;
    f.publish(&[message("a", 1)]).await;
    let failing = |_: &PgSession, _: &InboxMessage| {
        std::future::ready(Err::<(), Failure>(Failure::from(format!(
            "unknown variant `\0{}`",
            "x".repeat(10_000)
        ))))
    };
    assert!(matches!(
        f.dispatch(failing).await,
        Outcome::Failed {
            attempts: 1,
            parked: false,
            held: false
        }
    ));
    let client = pool().get().await.unwrap();
    let kept: String = client
        .query_one(&format!("SELECT last_error FROM {}", f.table), &[])
        .await
        .unwrap()
        .get(0);
    assert!(!kept.contains('\0'));
    assert!(kept.chars().count() <= 4096, "{}", kept.chars().count());
}

/// A parked row whose values do not read is listed by its place, its
/// identity none where it does not read back, and is resolved and unparked
/// by its place; its resolve wakes what waited for it, the identity read by
/// the server from the row (ADR-0037 decision 6). Before, the listing
/// decoded every row and one such row failed it for every Tenant, and
/// unpark and resolve bound an identity read back into Rust.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_parked_row_that_does_not_read_is_listed_resolved_and_unparked_by_its_place() {
    let f = fixture_cut("unreadable_operated", (), 1, TraceFile::off()).await;
    stored_before(&f, "12345678901234567890123", "{}").await;
    stored_before(
        &f,
        r#"{"id": "deep"}"#,
        &format!(r#"{{"h": {}{}}}"#, "[".repeat(127), "]".repeat(127)),
    )
    .await;
    // a row stored before, waiting for the first
    let client = pool().get().await.unwrap();
    client
        .execute(
            &format!(
                "INSERT INTO {} (tenant_id, stream_type, stream_id, stream_position, uri, payload, \
                     metadata, waiting_for, waiting_since) \
                 VALUES ('tenant-1', 'orders.Order', '{{\"id\": \"waiter\"}}', 1, 'kafka://orders/w', \
                     'w', '{{}}', jsonb_build_object('tenant_id', 'tenant-1', 'stream_type', 'orders.Order', \
                     'stream_id', '12345678901234567890123'::jsonb, 'stream_position', 1), now())",
                f.table
            ),
            &[],
        )
        .await
        .unwrap();
    let (seen, subscriber) = collector();
    f.drain(&subscriber).await;
    let parked = f.parked_messages().await;
    assert_eq!(parked.len(), 2, "{parked:?}");
    assert!(
        parked[0].identity.is_none(),
        "past 64 bits, it reads back as no identity"
    );
    assert!(
        parked[1].identity.is_some(),
        "a deep metadata is not read by the listing"
    );
    let resolved = f
        .sessions
        .session(async |session| f.inbox.resolve(&session, parked[0].received_position).await)
        .await
        .unwrap();
    assert!(resolved);
    f.drain(&subscriber).await;
    assert_eq!(*seen.lock().unwrap(), ["waiter@1"], "woken by the resolve");
    let unparked = f
        .sessions
        .session(async |session| f.inbox.unpark(&session, parked[1].received_position).await)
        .await
        .unwrap();
    assert!(unparked);
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Parked,
        "parked at once again"
    );
    assert_eq!(f.parked_messages().await.len(), 1);
}

/// What an expiry was told: each batch's size, and whether every row came
/// without its payload.
#[derive(Default)]
struct ExpiredBatches(Mutex<Vec<(usize, bool)>>);

impl InboxObserver for ExpiredBatches {
    fn on_expired(&self, event: &Expired<'_>) {
        self.0.lock().unwrap().push((
            event.messages.len(),
            event
                .messages
                .iter()
                .all(|message| message.payload.is_empty()),
        ));
    }
}

/// An expiry parks at most a batch per statement and returns its rows
/// without their payloads: rows that wait for what never arrives come back
/// a batch at a time, never all of them with their payloads at once
/// (ADR-0037 decision 6).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn an_expiry_parks_a_batch_at_a_time_without_the_payloads() {
    let batches = Arc::new(ExpiredBatches::default());
    // the rows are written behind the observer: no trace (no run of the model)
    let f = fixture_cut("expiry_batches", Arc::clone(&batches), 1, TraceFile::off())
        .await
        .with_max_wait(Duration::from_secs(1));
    let client = pool().get().await.unwrap();
    client
        .execute(
            &format!(
                "INSERT INTO {} (tenant_id, stream_type, stream_id, stream_position, uri, payload, \
                     metadata, waiting_for, waiting_since) \
                 SELECT 'tenant-1', 'orders.Order', jsonb_build_object('id', 'w' || n), 1, \
                     'kafka://orders/w', repeat('p', 1000)::bytea, '{{}}', \
                     '{{\"tenant_id\": \"tenant-1\", \"stream_type\": \"orders.Order\", \
                       \"stream_id\": {{\"id\": \"never\"}}, \"stream_position\": 1}}', \
                     now() - interval '1 hour' \
                 FROM generate_series(1, 105) n",
                f.table
            ),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        [f.expire().await, f.expire().await],
        [
            Expiry {
                parked: 100,
                more: true
            },
            Expiry {
                parked: 5,
                more: false
            }
        ]
    );
    assert_eq!(*batches.0.lock().unwrap(), [(100, true), (5, true)]);
}

/// A row parked at once for its own values is superseded by the next
/// arrival at its identity: the producer's repaired message replaces its
/// values, is processed, and wakes what waited for it (ADR-0037 decision 6).
/// Before, a republication at a held identity was acknowledged as a
/// redelivery or a shadow and dropped, and the row could leave only by a
/// resolve that skipped its subscriber.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_row_parked_at_once_is_superseded_by_the_next_arrival_at_its_identity() {
    let f = fixture_cut("superseded", (), 1, TraceFile::off()).await;
    // the row stored before carries the very message_id its repair will:
    // the supersede does not take its own row for another's
    stored_before(
        &f,
        r#"{"id": "deep"}"#,
        &format!(
            r#"{{"message_id": "00000000-0000-4000-b000-000000000001", "h": {}{}}}"#,
            "[".repeat(127),
            "]".repeat(127)
        ),
    )
    .await;
    let (seen, subscriber) = collector();
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Parked);
    let waiter = message("waiter", 1).depending_on(&[CausalDependency::on(&InboxMessage::new(
        "tenant-1",
        "orders.Order",
        json!({ "id": "deep" }),
        1,
        "kafka://orders/old",
        "",
    ))]);
    f.publish(std::slice::from_ref(&waiter)).await;
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::SetAside,
        "the waiter waits"
    );

    // A row arrives when it is stored, by the table's default, and again
    // when a repaired message supersedes it: its wait is the new arrival's
    // (ADR-0041 decision 5).
    let first = received_at(&f, r#"{"id": "deep"}"#).await;
    let repaired = InboxMessage::new(
        "tenant-1",
        "orders.Order",
        json!({ "id": "deep" }),
        1,
        "kafka://orders/old",
        "repaired",
    )
    .with_metadata(json!({ "message_id": "00000000-0000-4000-b000-000000000001" }));
    // as a row that failed under a cause before a host's lower bounds made
    // it unreadable: the supersede ends what its failures wrote (ADR-0042
    // decision 4); a parked row holds nothing, which the baseline's CHECK
    // keeps
    f.execute("UPDATE {table} SET cause = 'kms.unavailable' WHERE unreadable_at IS NOT NULL")
        .await;
    f.publish(std::slice::from_ref(&repaired)).await;
    assert_eq!(f.rows().await, 2, "superseded in place");
    let causes = format!(
        "SELECT count(*) FROM {} WHERE cause IS NOT NULL OR holds > 0 OR hold_kind IS NOT NULL",
        f.table
    );
    let left: i64 = f
        .sessions
        .session(async |session| {
            Ok::<_, Error>(session.connection().query_one(&causes, &[]).await?.get(0))
        })
        .await
        .unwrap();
    assert_eq!(left, 0, "the supersede clears the causes and the hold");
    assert!(
        received_at(&f, r#"{"id": "deep"}"#).await > first,
        "a superseded row arrives again"
    );
    f.drain(&subscriber).await;
    assert_eq!(*seen.lock().unwrap(), ["deep@1", "waiter@1"]);
    assert!(f.parked_messages().await.is_empty());
}

/// The parked rows are counted by channel — the `uri` without its key — and
/// by ground, read from the columns, and each exit takes a row out of its
/// count, though only an unpark and a resolve tell an observer (ADR-0036
/// decision 6): two keys of one channel are one count.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_parked_rows_are_counted_by_channel_and_ground_and_each_exit_leaves_its_count() {
    let f = fixture_cut("parked_counts", (), 1, TraceFile::off()).await;
    stored_before(
        &f,
        r#"{"id": "deep"}"#,
        &format!(
            r#"{{"message_id": "00000000-0000-4000-b000-000000000001", "h": {}{}}}"#,
            "[".repeat(127),
            "]".repeat(127)
        ),
    )
    .await;
    let (_, subscriber) = collector();
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Parked);
    let on = |id: &str, key: &str| {
        InboxMessage::new(
            "tenant-1",
            "pay.Payment",
            json!({ "id": id }),
            1,
            format!("kafka://pay/{key}"),
            id,
        )
    };
    f.publish(&[on("p1", "k1"), on("p2", "k2"), on("p3", "k1")])
        .await;
    // As the library parks them: a failure keeps no wait; an expiry keeps
    // the dependency it waited for.
    pool()
        .get()
        .await
        .unwrap()
        .batch_execute(&format!(
            "UPDATE {t} SET parked_at = now(), attempts = 3 \
                 WHERE stream_id IN ('{{\"id\": \"p1\"}}', '{{\"id\": \"p2\"}}'); \
             UPDATE {t} SET parked_at = now(), \
                 waiting_for = jsonb_build_object('tenant_id', 'tenant-1', 'stream_type', \
                     'pay.Payment', 'stream_id', '\"gone\"'::jsonb, 'stream_position', 1) \
                 WHERE stream_id = '{{\"id\": \"p3\"}}'",
            t = f.table
        ))
        .await
        .unwrap();
    let counts = async || {
        f.inbox
            .parked_counts(Duration::from_secs(5))
            .await
            .unwrap()
            .into_iter()
            .map(|count| (count.channel, count.ground, count.open))
            .collect::<Vec<_>>()
    };
    let at = |channel: &str, ground, open| (channel.to_owned(), ground, open);
    assert_eq!(
        counts().await,
        [
            at("kafka://orders", Ground::AtOnce, 1),
            at("kafka://pay", Ground::Expired, 1),
            at("kafka://pay", Ground::Failure, 2),
        ]
    );
    let parked = f
        .sessions
        .session(async |session| f.inbox.parked(&session, None, 10).await)
        .await
        .unwrap();
    assert_eq!(
        parked
            .iter()
            .map(|row| (row.received_position, row.ground))
            .collect::<Vec<_>>(),
        [
            (1, Ground::AtOnce),
            (2, Ground::Failure),
            (3, Ground::Failure),
            (4, Ground::Expired),
        ],
        "the listing names each row's ground"
    );
    assert!(
        f.sessions
            .session(async |session| f.inbox.unpark(&session, 2).await)
            .await
            .unwrap()
    );
    assert!(
        f.sessions
            .session(async |session| f.inbox.resolve(&session, 4).await)
            .await
            .unwrap()
    );
    let repaired = InboxMessage::new(
        "tenant-1",
        "orders.Order",
        json!({ "id": "deep" }),
        1,
        "kafka://orders/old",
        "repaired",
    )
    .with_metadata(json!({ "message_id": "00000000-0000-4000-b000-000000000001" }));
    f.publish(std::slice::from_ref(&repaired)).await;
    assert_eq!(counts().await, [at("kafka://pay", Ground::Failure, 1)]);
}

/// A supersede whose `message_id` another identity holds is that
/// identity's taken, set aside, the row left parked: the supersede claims no
/// one else's `message_id` (ADR-0036 R5).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_supersede_whose_message_id_is_held_elsewhere_is_set_aside() {
    let f = fixture_cut("superseded_taken", (), 1, TraceFile::off()).await;
    stored_before(
        &f,
        r#"{"id": "deep"}"#,
        &format!(r#"{{"h": {}{}}}"#, "[".repeat(127), "]".repeat(127)),
    )
    .await;
    let (_, subscriber) = collector();
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Parked);
    let holder = message("holder", 1);
    f.publish(std::slice::from_ref(&holder)).await;
    let repaired = InboxMessage::new(
        "tenant-1",
        "orders.Order",
        json!({ "id": "deep" }),
        1,
        "kafka://orders/old",
        "repaired",
    )
    .with_metadata(json!({ "message_id": message_id(&holder) }));
    let admission = f
        .inbox
        .intake(&repaired.uri, &as_sent(&repaired))
        .await
        .unwrap();
    assert!(
        matches!(
            admission,
            ascetic_ddd_inbox::Admission::SetAside {
                reason: ascetic_ddd_inbox::Reason::IdentityTaken,
                ..
            }
        ),
        "{admission:?}"
    );
    assert_eq!(f.parked_messages().await.len(), 1, "still parked");
}

/// The wire message of a row, as a producer would send it.
fn as_sent(message: &InboxMessage) -> ascetic_ddd_bus::Message {
    use ascetic_ddd_bus::identity::{
        STREAM_ID, STREAM_POSITION, STREAM_TYPE, TENANT_ID, write_stream_id,
    };
    let wire = ascetic_ddd_bus::Message::new(message.payload.clone())
        .with_header(TENANT_ID, message.tenant_id.as_bytes())
        .with_header(STREAM_TYPE, message.stream_type.as_bytes())
        .with_header(STREAM_ID, write_stream_id(&message.stream_id))
        .with_header(STREAM_POSITION, message.stream_position.to_string());
    match message
        .metadata
        .as_ref()
        .and_then(|m| m.get("message_id"))
        .and_then(|v| v.as_str())
    {
        Some(id) => wire.with_header("message_id", id),
        None => wire,
    }
}

/// A failure past `attempts`' range saturates instead of failing the
/// record, which would stop every loop (ADR-0037 decision 7): the library's
/// default retries for ever.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_failure_past_the_attempts_range_saturates() {
    let f = fixture("attempts_saturate").await;
    f.publish(&[message("a", 1)]).await;
    let client = pool().get().await.unwrap();
    client
        .execute(
            &format!("UPDATE {} SET attempts = 2147483647", f.table),
            &[],
        )
        .await
        .unwrap();
    let failing = |_: &PgSession, _: &InboxMessage| {
        std::future::ready(Err::<(), Failure>(Failure::from("again")))
    };
    assert!(matches!(
        f.dispatch(failing).await,
        Outcome::Failed {
            attempts: 2_147_483_647,
            parked: false,
            held: false
        }
    ));
}

// ---------------------------------------------------------------------------
// ADR-0041: a slot serves its Tenants in turn (E1–E4)

/// A message of `tenant`, otherwise as [`message`] makes one.
fn of(tenant: &str, stream: &str, position: i32) -> InboxMessage {
    let base = message(stream, position);
    InboxMessage::new(
        tenant,
        base.stream_type,
        base.stream_id,
        position,
        base.uri,
        base.payload,
    )
    .with_metadata(base.metadata.unwrap())
}

/// Streams named `{prefix}N` whose rows of their Tenants land in one slot of
/// the fixture's `slots`, under its key (`ByStream`): the first of
/// `wanted`'s at `N = 0`, each other at the first `N` beside it.
async fn streams_in_one_slot<O: InboxObserver>(
    f: &Fixture<O>,
    slots: u32,
    wanted: &[(&str, &str)],
) -> Vec<String> {
    let slot_of = "(hashtext($1 || ':orders.Order:' || jsonb_build_object('id', x.s)::text) \
                   & 2147483647) % $3";
    f.sessions
        .session(async |session| {
            let mut streams: Vec<String> = Vec::new();
            let mut slot: Option<i32> = None;
            for (tenant, prefix) in wanted {
                let row = session
                    .connection()
                    .query_one(
                        &format!(
                            "SELECT x.s, ({slot_of})::int FROM generate_series(0, 100000) n, \
                             LATERAL (SELECT $2 || n AS s) x \
                             WHERE $4::int IS NULL OR ({slot_of}) = $4::int ORDER BY n LIMIT 1"
                        ),
                        &[tenant, prefix, &(slots as i32), &slot],
                    )
                    .await?;
                streams.push(row.get(0));
                slot = Some(row.get(1));
            }
            Ok::<_, Error>(streams)
        })
        .await
        .unwrap()
}

/// Counts the dispatcher's closed transactions, by outcome.
#[derive(Clone, Default)]
struct Dispatches(Arc<Mutex<Vec<String>>>);

impl InboxObserver for Dispatches {
    fn on_dispatched(&self, event: &Dispatched<'_>) {
        self.0.lock().unwrap().push(match event.outcome {
            Ok(outcome) => format!("{outcome:?}"),
            Err(_) => "rolled back".to_owned(),
        });
    }
}

/// E1: a slot serves its Tenants in turn. Four slots and one loop; Tenant A
/// has 1 000 rows across them, Tenant B one, stored after all of A's. B is
/// processed within 2S dispatches whether it sorts before A or after, and
/// every row is drained. As a FIFO, B waited for every A row of its slot.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn one_tenant_s_row_is_served_beside_another_s_thousand() {
    const S: u32 = 4;
    for (name, a, b) in [
        ("turn_first", "tenant-m", "tenant-a"),
        ("turn_last", "tenant-m", "tenant-z"),
    ] {
        let f = fixture_cut(name, (), S, TraceFile::off()).await;
        f.execute(&format!(
            "INSERT INTO {{table}} (tenant_id, stream_type, stream_id, stream_position, uri, payload, metadata) \
             SELECT '{a}', 'orders.Order', jsonb_build_object('id', 'a' || n), 1, 'kafka://orders/a', 'a', \
                    jsonb_build_object('message_id', gen_random_uuid()) \
             FROM generate_series(1, 1000) n"
        ))
        .await;
        f.publish(&[of(b, "b", 1)]).await;
        let (seen, subscriber) = collector();
        let mut dispatches = 0;
        while !seen.lock().unwrap().iter().any(|label| label == "b@1") {
            f.dispatch(&subscriber).await;
            dispatches += 1;
            assert!(dispatches <= 2 * S, "{b} waited {dispatches} dispatches");
        }
        f.drain(&subscriber).await;
        assert_eq!(seen.lock().unwrap().len(), 1001, "every row is drained");
    }
}

/// E2: a lone Tenant uses the loops. Four slots, four loops, a subscriber
/// that takes its time: four of its messages are in the subscriber at once,
/// and the loops drain more than S·C of its rows. A slot keyed by Tenant
/// would hold it to one at a time.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_lone_tenant_is_served_by_every_loop() {
    const S: u32 = 4;
    const C: u32 = 4;
    const ROWS: usize = 40;
    let f = fixture_cut("lone_tenant", (), S, TraceFile::off()).await;
    f.execute(&format!(
        "INSERT INTO {{table}} (tenant_id, stream_type, stream_id, stream_position, uri, payload, metadata) \
         SELECT 'tenant-1', 'orders.Order', jsonb_build_object('id', 's' || n), 1, 'kafka://orders/s', 's', \
                jsonb_build_object('message_id', gen_random_uuid()) \
         FROM generate_series(1, {ROWS}) n"
    ))
    .await;
    let (in_flight, peak, done) = (
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    );
    let slow = {
        let (in_flight, peak, done) =
            (Arc::clone(&in_flight), Arc::clone(&peak), Arc::clone(&done));
        move |_: &PgSession, _: &InboxMessage| {
            let (in_flight, peak, done) =
                (Arc::clone(&in_flight), Arc::clone(&peak), Arc::clone(&done));
            async move {
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(100)).await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
                done.fetch_add(1, Ordering::SeqCst);
                Ok::<(), Failure>(())
            }
        }
    };
    let drained = {
        let done = Arc::clone(&done);
        async move {
            while done.load(Ordering::SeqCst) < ROWS {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    };
    let loops = Loops {
        concurrency: C,
        poll_interval: Duration::from_millis(10),
        ..Loops::default()
    };
    tokio::time::timeout(Duration::from_secs(60), f.inbox.run(&slow, loops, drained))
        .await
        .expect("drained")
        .unwrap();
    assert_eq!(done.load(Ordering::SeqCst), ROWS);
    assert!(ROWS > (S * C) as usize);
    assert_eq!(
        peak.load(Ordering::SeqCst),
        C as usize,
        "four in the subscriber at once"
    );
}

/// E3: a head in its backoff holds its own (slot, Tenant), not the slot.
/// Tenant A's head fails in a slot it shares with B; B's row is processed
/// at the next dispatch, and A's second stream there waits behind A's head.
/// At one slot and at 64, where the rows are placed side by side.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_head_in_backoff_holds_its_tenant_and_not_its_slot() {
    for slots in [1, 64] {
        let f = fixture_cut(&format!("held_tenant_{slots}"), (), slots, TraceFile::off())
            .await
            .with_retries(Retries::unlimited().with_backoff(|_| Duration::from_secs(3600)));
        let streams = streams_in_one_slot(
            &f,
            slots,
            &[("tenant-a", "a"), ("tenant-a", "c"), ("tenant-b", "b")],
        )
        .await;
        let [a1, a2, b1] = &streams[..] else {
            unreachable!()
        };
        f.publish(&[
            of("tenant-a", a1, 1),
            of("tenant-a", a2, 1),
            of("tenant-b", b1, 1),
        ])
        .await;
        f.execute("DO $$ BEGIN ASSERT (SELECT count(DISTINCT slot) FROM {table}) = 1; END $$")
            .await;
        let poison = format!("{a1}@1");
        let (seen, subscriber) = failing_on(&poison);
        assert_eq!(
            f.dispatch(&subscriber).await,
            Outcome::Failed {
                attempts: 1,
                parked: false,
                held: false
            }
        );
        assert_eq!(
            f.dispatch(&subscriber).await,
            Outcome::Processed,
            "B goes on, slots {slots}"
        );
        assert_ne!(
            f.dispatch(&subscriber).await,
            Outcome::Processed,
            "slots {slots}"
        );
        assert_eq!(
            *seen.lock().unwrap(),
            [format!("{b1}@1")],
            "A's second stream waits"
        );
    }
}

/// E4, K: with a walk of two, five Tenants whose heads wait for their
/// backoff ahead of one that is due are passed in exactly two dispatches,
/// and the third takes the due head.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_walk_passes_at_most_its_bound_of_tenants() {
    let f = fixture_cut("walk_bound", (), 1, TraceFile::off())
        .await
        .with_rotation(Rotation {
            walk: 2,
            ..Rotation::default()
        });
    let tenants = ["t1", "t2", "t3", "t4", "t5", "t6"];
    let messages: Vec<_> = tenants
        .iter()
        .map(|t| of(t, &format!("s-{t}"), 1))
        .collect();
    f.publish(&messages).await;
    f.execute(
        "UPDATE {table} SET next_attempt_at = now() + interval '1 hour', attempts = 1 \
         WHERE tenant_id <> 't6'",
    )
    .await;
    let (seen, subscriber) = collector();
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Passed);
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Passed);
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Processed);
    assert_eq!(*seen.lock().unwrap(), ["s-t6@1"]);
}

/// E4, the bound on writes: five Tenants, every head waiting for its
/// backoff, a walk of two. A round is ⌈5/2⌉ = 3 walks, then the slot sleeps:
/// a loop polling every 200 ms for a second dispatches at most 3 + 5 times.
/// A round that never ended would pass for ever, without a pause.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_slot_whose_every_head_waits_sleeps_after_one_round() {
    let dispatches = Dispatches::default();
    let f = fixture_cut("round_sleeps", dispatches.clone(), 1, TraceFile::off())
        .await
        .with_rotation(Rotation {
            walk: 2,
            max_dormancy: Duration::from_secs(60),
        });
    let messages: Vec<_> = ["t1", "t2", "t3", "t4", "t5"]
        .iter()
        .map(|t| of(t, &format!("s-{t}"), 1))
        .collect();
    f.publish(&messages).await;
    f.execute("UPDATE {table} SET next_attempt_at = now() + interval '1 hour', attempts = 1")
        .await;
    let (_, subscriber) = collector();
    let loops = Loops {
        poll_interval: Duration::from_millis(200),
        ..Loops::default()
    };
    f.inbox
        .run(
            &subscriber,
            loops,
            tokio::time::sleep(Duration::from_secs(1)),
        )
        .await
        .unwrap();
    let seen = dispatches.0.lock().unwrap().clone();
    assert_eq!(
        seen.iter().filter(|o| *o == "Passed").count(),
        2,
        "{seen:?}"
    );
    assert!(seen.len() <= 3 + 5, "{seen:?}");
}

/// E4, a row arriving after the round began wakes a sleeping slot: the next
/// dispatch takes it.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_row_arriving_in_a_sleeping_slot_is_taken_at_once() {
    let f = fixture_cut("arrival_wakes", (), 1, TraceFile::off())
        .await
        .with_rotation(Rotation {
            max_dormancy: Duration::from_secs(3600),
            ..Rotation::default()
        });
    f.publish(&[of("tenant-p", "p", 1)]).await;
    f.execute("UPDATE {table} SET next_attempt_at = now() + interval '1 hour', attempts = 1")
        .await;
    let (seen, subscriber) = collector();
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Nothing,
        "the slot sleeps"
    );
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Nothing,
        "and stays asleep"
    );
    f.publish(&[of("tenant-n", "n", 1)]).await;
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Processed);
    assert_eq!(*seen.lock().unwrap(), ["n@1"]);
}

/// E4, the round's own position: with a walk of one, Tenants 2 and 3 whose
/// heads wait, and the cursor at 3, a round begins at 3 and passes Tenant 2;
/// a row of Tenant 1, behind the walk, commits; the next walk passes the
/// round's start and the slot sleeps. The row came after the round began,
/// so it wakes the slot at once — not after `max_dormancy`, as it would
/// were the queue's position read when the round ends
/// (`InboxPgDormantAfterAtWalk.cfg`).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_row_committed_during_a_round_behind_its_walk_wakes_the_slot() {
    let f = fixture_cut("round_behind", (), 1, TraceFile::off())
        .await
        .with_rotation(Rotation {
            walk: 1,
            max_dormancy: Duration::from_secs(3600),
        });
    f.publish(&[of("t2", "s-t2", 1), of("t3", "s-t3", 1)]).await;
    f.execute(
        "UPDATE {table} SET next_attempt_at = now() + interval '1 hour', attempts = 1; \
         UPDATE {table}_slots SET last_tenant = 't3'",
    )
    .await;
    let (seen, subscriber) = collector();
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Passed,
        "Tenant 2 passed"
    );
    f.publish(&[of("t1", "s-t1", 1)]).await;
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Nothing,
        "the round ends at 3"
    );
    assert_eq!(f.dispatch(&subscriber).await, Outcome::Processed);
    assert_eq!(*seen.lock().unwrap(), ["s-t1@1"]);
}

/// E4, the cap: a row made due behind a sleeping slot's back is taken
/// within `max_dormancy` — here a parked row unparked, and an insert that
/// was in flight when the round began, whose position lies below the
/// round's. Neither wakes the slot by itself; the cap does. The single
/// unpark runs in the caller's session, maybe in its transaction, and
/// writes no slot row; the operator's bulk verbs, each its own statement,
/// wake the slots they move rows into (ADR-0042 decision 8).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_row_made_due_behind_a_sleeping_slot_is_taken_within_the_cap() {
    const CAP: Duration = Duration::from_secs(2);
    let rotation = Rotation {
        max_dormancy: CAP,
        ..Rotation::default()
    };
    let (seen, subscriber) = collector();
    let within_the_cap = async |f: &Fixture, label: &str| {
        let started = std::time::Instant::now();
        while !seen.lock().unwrap().iter().any(|seen| seen == label) {
            assert!(
                started.elapsed() < CAP + Duration::from_secs(8),
                "{label} was not taken within the cap"
            );
            f.dispatch(&subscriber).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };

    // Unparked.
    let f = fixture_cut("cap_unparked", (), 1, TraceFile::off())
        .await
        .with_rotation(rotation);
    f.publish(&[of("tenant-q", "q", 1), of("tenant-p", "p", 1)])
        .await;
    f.execute(
        "UPDATE {table} SET parked_at = now(), attempts = 3 WHERE tenant_id = 'tenant-q'; \
         UPDATE {table} SET next_attempt_at = now() + interval '1 hour', attempts = 1 \
         WHERE tenant_id = 'tenant-p'",
    )
    .await;
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Nothing,
        "the slot sleeps"
    );
    let at = f.parked_messages().await[0].received_position;
    assert!(
        f.sessions
            .session(async |session| f.inbox.unpark(&session, at).await)
            .await
            .unwrap()
    );
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Nothing,
        "an unpark wakes nothing"
    );
    within_the_cap(&f, "q@1").await;

    // In flight when the round began.
    let f = fixture_cut("cap_in_flight", (), 1, TraceFile::off())
        .await
        .with_rotation(rotation);
    let holder = pool().get().await.unwrap();
    holder
        .batch_execute(&format!(
            "BEGIN; INSERT INTO {} (tenant_id, stream_type, stream_id, stream_position, uri, payload, metadata) \
             VALUES ('tenant-x', 'orders.Order', '{{\"id\": \"x\"}}', 1, 'kafka://orders/x', 'x', \
                     '{{\"message_id\": \"00000000-0000-4000-c000-000000000001\"}}')",
            f.table
        ))
        .await
        .unwrap();
    f.publish(&[of("tenant-y", "y", 1)]).await;
    f.execute("UPDATE {table} SET next_attempt_at = now() + interval '1 hour', attempts = 1")
        .await;
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Nothing,
        "the slot sleeps"
    );
    holder.batch_execute("COMMIT").await.unwrap();
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Nothing,
        "below the round's position"
    );
    within_the_cap(&f, "x@1").await;

    // Woken by a mark in another slot's transaction, which writes no slot
    // row (ADR-0009's lock order): w waits for d, which lands in the other
    // slot, beside p, whose head waits for its backoff.
    let f = fixture_cut("cap_woken", (), 2, TraceFile::off())
        .await
        .with_rotation(rotation);
    let beside = streams_in_one_slot(&f, 2, &[("tenant-w", "w"), ("tenant-p", "p")]).await;
    let [w, p] = &beside[..] else { unreachable!() };
    let d = f
        .sessions
        .session(async |session| {
            let row = session
                .connection()
                .query_one(
                    "SELECT 'd' || n FROM generate_series(0, 1000) n \
                     WHERE (hashtext('tenant-d:orders.Order:' || jsonb_build_object('id', 'd' || n)::text) \
                            & 2147483647) % 2 \
                       <> (hashtext('tenant-w:orders.Order:' || jsonb_build_object('id', $1::text)::text) \
                            & 2147483647) % 2 \
                     ORDER BY n LIMIT 1",
                    &[w],
                )
                .await?;
            Ok::<String, Error>(row.get(0))
        })
        .await
        .unwrap();
    let dependency = of("tenant-d", &d, 1);
    let waiter = of("tenant-w", w, 1).depending_on(&[CausalDependency::on(&dependency)]);
    f.publish(&[waiter, of("tenant-p", p, 1)]).await;
    f.execute(
        "UPDATE {table} SET next_attempt_at = now() + interval '1 hour', attempts = 1 \
         WHERE tenant_id = 'tenant-p'",
    )
    .await;
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::SetAside,
        "w waits for d"
    );
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Nothing,
        "the slot sleeps"
    );
    f.publish(std::slice::from_ref(&dependency)).await;
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Processed,
        "d in its own slot"
    );
    assert_eq!(
        f.dispatch(&subscriber).await,
        Outcome::Nothing,
        "a wake wakes nothing"
    );
    within_the_cap(&f, &format!("{w}@1")).await;
}

/// E9: the heads held by their own failure are counted from the table, by
/// channel and cause, as how many Tenants and the oldest hold (ADR-0041
/// decision 5). A second inbox over the table — another process — counts
/// the same, so the gauge is the table's, not an observer's. A retry keeps
/// the hold's start; a mark ends it; a failure that parks leaves no hold; a
/// count the database cannot give within its bound fails.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_held_heads_are_counted_by_channel_and_cause_from_the_table() {
    let f = fixture_cut("held_counts", (), 1, TraceFile::off())
        .await
        .with_retries(Retries::unlimited().with_backoff(|_| Duration::from_secs(3600)));
    let audit = |tenant: &str, stream: &str| {
        let m = of(tenant, stream, 1);
        InboxMessage {
            uri: format!("kafka://audit/{stream}"),
            ..m
        }
    };
    f.publish(&[
        of("tenant-a", "a1", 1),
        of("tenant-a", "a2", 1),
        of("tenant-b", "b1", 1),
        audit("tenant-c", "c1"),
    ])
    .await;
    let failing = |_: &PgSession, _: &InboxMessage| {
        std::future::ready(Err::<(), Failure>("held on purpose".into()))
    };
    f.drain(failing).await;
    let counts = f.inbox.held_counts(Duration::from_secs(5)).await.unwrap();
    let seen: Vec<_> = counts
        .iter()
        .map(|c| (c.channel.as_str(), c.hold, c.tenants))
        .collect();
    assert_eq!(
        seen,
        [
            ("kafka://audit", Hold::Backoff, 1),
            ("kafka://orders", Hold::Backoff, 2)
        ],
        "a's second stream is behind its head, not held"
    );
    let other = PgInbox::new(PgSessionPool::new(pool())).with_table(
        Identifier::new(&f.table).unwrap(),
        Identifier::new(format!("{}_seq", f.table)).unwrap(),
    );
    assert_eq!(
        other.held_counts(Duration::from_secs(5)).await.unwrap(),
        counts
    );

    // A retry that fails again keeps when the hold began. b's backoff is
    // made to pass, and the slot, asleep since its round saw every head
    // wait, wakes at that backoff, as it would.
    let b_due = "UPDATE {table} SET next_attempt_at = now() WHERE tenant_id = 'tenant-b'; \
                 UPDATE {table}_slots SET dormant_until = now()";
    // b's own hold, not the channel's oldest: a's began earlier.
    let b_held_since = async || -> String {
        let sql = format!(
            "SELECT held_since::text FROM {} WHERE tenant_id = 'tenant-b'",
            f.table
        );
        f.sessions
            .session(async |session| {
                Ok::<String, Error>(session.connection().query_one(&sql, &[]).await?.get(0))
            })
            .await
            .unwrap()
    };
    let before = b_held_since().await;
    f.execute(b_due).await;
    f.drain(failing).await;
    f.execute(
        "DO $$ BEGIN \
           ASSERT (SELECT attempts FROM {table} WHERE tenant_id = 'tenant-b') = 2, 'b was retried'; \
         END $$",
    )
    .await;
    assert_eq!(
        b_held_since().await,
        before,
        "the hold began at the first failure"
    );

    // A mark ends the hold.
    f.execute(b_due).await;
    let (_, subscriber) = collector();
    f.drain(&subscriber).await;
    let counts = f.inbox.held_counts(Duration::from_secs(5)).await.unwrap();
    assert_eq!(counts[1].tenants, 1, "b's mark ended its hold: {counts:?}");

    // Tenants, not heads: one Tenant held in two slots is one.
    let two = fixture_cut("held_two_slots", (), 2, TraceFile::off())
        .await
        .with_retries(Retries::unlimited().with_backoff(|_| Duration::from_secs(3600)));
    let (first, second) = two.streams_in_different_slots().await;
    two.publish(&[of("tenant-1", &first, 1), of("tenant-1", &second, 1)])
        .await;
    two.drain(failing).await;
    two.execute(
        "DO $$ BEGIN ASSERT (SELECT count(*) FROM {table} WHERE held_since IS NOT NULL) = 2; END $$",
    )
    .await;
    let held = two.inbox.held_counts(Duration::from_secs(5)).await.unwrap();
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].tenants, 1, "two heads of one Tenant: {held:?}");

    // A failure that parks leaves no hold.
    let parking = fixture_cut("held_parks", (), 1, TraceFile::off())
        .await
        .with_retries(Retries::up_to(1));
    parking.publish(&[of("tenant-a", "a1", 1)]).await;
    parking.drain(failing).await;
    assert_eq!(parking.parked_messages().await.len(), 1);
    assert!(
        parking
            .inbox
            .held_counts(Duration::from_secs(5))
            .await
            .unwrap()
            .is_empty()
    );

    // Bounded: a table another session holds is a failure within the bound.
    let holder = pool().get().await.unwrap();
    holder
        .batch_execute(&format!(
            "BEGIN; LOCK TABLE {} IN ACCESS EXCLUSIVE MODE",
            f.table
        ))
        .await
        .unwrap();
    let started = std::time::Instant::now();
    assert!(
        f.inbox
            .held_counts(Duration::from_millis(200))
            .await
            .is_err()
    );
    assert!(started.elapsed() < Duration::from_secs(5));
    holder.batch_execute("ROLLBACK").await.unwrap();
}
