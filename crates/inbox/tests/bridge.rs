//! The inbox as a channel of the bus: a bridge from a broker channel is the
//! intake, and the transactional consumer processes each message once, in
//! the transaction that marks it. Needs a live PostgreSQL, like `pg.rs`.

use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use ascetic_ddd_bus::adapters::in_memory::InMemoryBroker;
use ascetic_ddd_bus::{BoxError, Bridge, Bus, Message, Subscription, Target};
use ascetic_ddd_inbox::{Error, INBOX_SCHEME, PgInbox};
use ascetic_ddd_outbox::{OUTBOX_SCHEME, PgOutbox};
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};
use ascetic_ddd_session::{PgAccess, PgSession, PgSessionPool, Session, SessionPool};
use tokio::sync::mpsc;

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
    inbox: Arc<PgInbox<PgSessionPool>>,
    table: String,
}

/// A table of its own per test, plus one the handler writes into.
async fn fixture(name: &str) -> Fixture {
    let table = format!("inbox_bridge_{name}");
    let sequence = format!("{table}_seq");
    let sessions = PgSessionPool::new(pool());
    let inbox = Arc::new(
        PgInbox::new(PgSessionPool::new(pool()))
            .with_table(&table, &sequence)
            .with_poll_interval(Duration::from_millis(20)),
    );
    let reset = format!(
        "DROP TABLE IF EXISTS {table}; DROP SEQUENCE IF EXISTS {sequence}; \
         DROP TABLE IF EXISTS {table}_handled; \
         CREATE TABLE {table}_handled (payload text NOT NULL);"
    );
    sessions
        .session(async |session| {
            session.connection().batch_execute(&reset).await.unwrap();
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
    async fn one<T>(&self, sql: String) -> T
    where
        T: for<'a> tokio_postgres::types::FromSql<'a> + Send + 'static,
    {
        self.sessions
            .session(async |session| {
                Ok::<_, Error>(session.connection().query_one(&sql, &[]).await?.get(0))
            })
            .await
            .unwrap()
    }

    async fn processed(&self) -> i64 {
        self.one(format!(
            "SELECT count(*) FROM {} WHERE processed_position IS NOT NULL",
            self.table
        ))
        .await
    }

    /// The handler reports before the mark commits, so the count is waited
    /// for rather than read at once.
    async fn processed_soon(&self, expected: i64) -> i64 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let processed = self.processed().await;
            if processed == expected || tokio::time::Instant::now() > deadline {
                return processed;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn handled(&self) -> i64 {
        self.one(format!("SELECT count(*) FROM {}_handled", self.table))
            .await
    }

    async fn uri(&self) -> String {
        self.one(format!("SELECT uri FROM {} LIMIT 1", self.table))
            .await
    }

    /// Processes with a handler that writes through the transaction it is
    /// given and reports the payload.
    fn consume(&self) -> (mpsc::UnboundedReceiver<String>, Subscription) {
        let (seen, received) = mpsc::unbounded_channel();
        let handled = format!("INSERT INTO {}_handled (payload) VALUES ($1)", self.table);
        let orders = self
            .inbox
            .consumer(|message: &Message| Ok(String::from_utf8(message.payload().to_vec())?));
        let processing = orders
            .subscribe(move |tx: PgSession, order: String| {
                let (seen, handled) = (seen.clone(), handled.clone());
                async move {
                    tx.connection().execute(&handled, &[&order]).await?;
                    seen.send(order).ok();
                    Ok::<(), BoxError>(())
                }
            })
            .unwrap();
        (received, processing)
    }
}

/// A wire message of order 7 with the headers the inbox needs.
fn order(payload: &str, position: i32, message_id: &str) -> Message {
    Message::new(payload)
        .with_key("order-7")
        .with_header("tenant_id", "t1")
        .with_header("stream_type", "orders.Order")
        .with_header("stream_id", "7")
        .with_header("stream_position", position.to_string())
        .with_header("message_id", message_id)
}

async fn next(received: &mut mpsc::UnboundedReceiver<String>) -> String {
    tokio::time::timeout(Duration::from_secs(20), received.recv())
        .await
        .expect("the message is processed")
        .unwrap()
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_message_from_a_broker_is_processed_once_in_the_marking_transaction() {
    let fixture = fixture("broker").await;
    let mut bus = Bus::new();
    bus.register("in-memory", InMemoryBroker::new()).unwrap();
    bus.register(INBOX_SCHEME, fixture.inbox.channel()).unwrap();
    let bus = Arc::new(bus);
    let intake = Bridge::new(Arc::clone(&bus))
        .run(
            "in-memory://orders",
            "intake",
            Target::Fixed("inbox://orders".into()),
        )
        .unwrap();
    let (mut received, processing) = fixture.consume();

    let producer = bus
        .producer("in-memory://orders", |message: &Message| message.clone())
        .unwrap();
    let placed = order("placed", 1, "00000000-0000-4000-8000-000000000001");
    producer.publish(&placed).await.unwrap();
    producer.publish(&placed).await.unwrap(); // delivered twice: the same identity

    assert_eq!(next(&mut received).await, "placed");
    assert!(
        tokio::time::timeout(Duration::from_millis(300), received.recv())
            .await
            .is_err(),
        "the duplicate is not processed again"
    );
    assert_eq!(fixture.processed_soon(1).await, 1);
    assert_eq!(
        fixture.handled().await,
        1,
        "the handler's write committed with the mark"
    );
    assert_eq!(fixture.uri().await, "inbox://orders/order-7");
    intake.cancel();
    processing.cancel();
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_outbox_feeds_the_inbox_without_a_broker() {
    let fixture = fixture("outbox").await;
    let outbox = Arc::new(
        PgOutbox::new(PgSessionPool::new(pool()))
            .with_tables("inbox_bridge_outbox_out", "inbox_bridge_outbox_out_offsets")
            .with_poll_interval(Duration::from_millis(20)),
    );
    fixture
        .sessions
        .session(async |session| {
            session
                .connection()
                .batch_execute(
                    "DROP TABLE IF EXISTS inbox_bridge_outbox_out; \
                     DROP TABLE IF EXISTS inbox_bridge_outbox_out_offsets;",
                )
                .await
                .unwrap();
            outbox.setup(&session).await
        })
        .await
        .unwrap();
    let mut bus = Bus::new();
    bus.register(OUTBOX_SCHEME, outbox.channel()).unwrap();
    bus.register(INBOX_SCHEME, fixture.inbox.channel()).unwrap();
    let bus = Arc::new(bus);
    let dispatcher = Bridge::new(Arc::clone(&bus))
        .run(
            "outbox://all",
            "dispatcher",
            Target::Header("destination".into()),
        )
        .unwrap();
    let (mut received, processing) = fixture.consume();

    let shipped = outbox.producer("inbox://orders/order-7", |payload: &String| {
        order(payload, 2, "00000000-0000-4000-8000-000000000002")
    });
    fixture
        .sessions
        .session(async |session| {
            session
                .atomic(async |tx| {
                    shipped.publish(&tx, &"shipped".to_owned()).await.unwrap();
                    Ok::<(), ascetic_ddd_outbox::Error>(())
                })
                .await
        })
        .await
        .unwrap();

    assert_eq!(next(&mut received).await, "shipped");
    assert_eq!(fixture.processed_soon(1).await, 1);
    assert_eq!(fixture.handled().await, 1);
    assert_eq!(
        fixture.uri().await,
        "inbox://orders/order-7",
        "the destination the outbox stamped"
    );
    dispatcher.cancel();
    processing.cancel();
}

/// A handler that fails leaves the message unprocessed and its own writes
/// rolled back; the loop retries after the poll interval, and the second
/// attempt goes through.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_failing_handler_is_retried_and_its_writes_are_rolled_back() {
    let fixture = fixture("retry").await;
    let mut bus = Bus::new();
    bus.register("in-memory", InMemoryBroker::new()).unwrap();
    bus.register(INBOX_SCHEME, fixture.inbox.channel()).unwrap();
    let bus = Arc::new(bus);
    let intake = Bridge::new(Arc::clone(&bus))
        .run(
            "in-memory://orders",
            "intake",
            Target::Fixed("inbox://orders".into()),
        )
        .unwrap();

    let attempts = Arc::new(AtomicUsize::new(0));
    let (seen, mut received) = mpsc::unbounded_channel();
    let handled = format!(
        "INSERT INTO {}_handled (payload) VALUES ($1)",
        fixture.table
    );
    let orders = fixture
        .inbox
        .consumer(|message: &Message| Ok(String::from_utf8(message.payload().to_vec())?));
    let processing = orders
        .subscribe({
            let attempts = Arc::clone(&attempts);
            move |tx: PgSession, order: String| {
                let (seen, handled, attempts) =
                    (seen.clone(), handled.clone(), Arc::clone(&attempts));
                async move {
                    tx.connection().execute(&handled, &[&order]).await?;
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Err::<(), BoxError>("the first attempt fails on purpose".into());
                    }
                    seen.send(order).ok();
                    Ok(())
                }
            }
        })
        .unwrap();

    bus.producer("in-memory://orders", |message: &Message| message.clone())
        .unwrap()
        .publish(&order("retried", 3, "00000000-0000-4000-8000-000000000003"))
        .await
        .unwrap();

    assert_eq!(next(&mut received).await, "retried");
    assert_eq!(fixture.processed_soon(1).await, 1);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(
        fixture.handled().await,
        1,
        "the failed attempt's write was rolled back with it"
    );
    intake.cancel();
    processing.cancel();
}
