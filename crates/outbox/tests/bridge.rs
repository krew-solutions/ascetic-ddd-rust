//! The outbox as a channel of the bus: what is published inside a committed
//! transaction reaches its destination through a bridge; what is rolled back
//! never leaves the outbox. Needs a live PostgreSQL, like `pg.rs`.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use ascetic_ddd_bus::adapters::in_memory::InMemoryBroker;
use ascetic_ddd_bus::{Bridge, Bus, Message, Target};
use ascetic_ddd_outbox::{OUTBOX_SCHEME, PgOutbox};
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};
use ascetic_ddd_session::{PgAccess, PgSessionPool, Session, SessionPool};
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

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_committed_message_crosses_the_bridge_and_a_rolled_back_one_does_not() {
    let sessions = PgSessionPool::new(pool());
    let outbox = Arc::new(
        PgOutbox::new(PgSessionPool::new(pool()))
            .with_tables("outbox_bridge", "outbox_bridge_offsets")
            .with_poll_interval(Duration::from_millis(20)),
    );
    sessions
        .session(async |session| {
            session
                .connection()
                .batch_execute("DROP TABLE IF EXISTS outbox_bridge; DROP TABLE IF EXISTS outbox_bridge_offsets;")
                .await
                .unwrap();
            outbox.setup(&session).await
        })
        .await
        .unwrap();

    let mut bus = Bus::new();
    bus.register(OUTBOX_SCHEME, outbox.channel()).unwrap();
    bus.register("in-memory", InMemoryBroker::new()).unwrap();
    let bus = Arc::new(bus);

    let orders = bus
        .consumer("in-memory://orders", "billing", |m: &Message| Ok(m.clone()))
        .unwrap();
    let (seen, mut inbox) = mpsc::unbounded_channel();
    orders
        .subscribe(move |message: Message| {
            let seen = seen.clone();
            async move {
                seen.send(message).ok();
            }
        })
        .unwrap();
    let bridge = Bridge::new(Arc::clone(&bus));
    let dispatcher = bridge
        .run(
            "outbox://all",
            "dispatcher",
            Target::Header("destination".into()),
        )
        .unwrap();

    let producer = outbox.producer("in-memory://orders/order-7", |order: &String| {
        Message::new(order.as_bytes())
            .with_header("message_id", "00000000-0000-4000-8000-000000000001")
    });

    // Rolled back: never leaves the outbox.
    let _: Result<(), ascetic_ddd_outbox::Error> = sessions
        .session(async |session| {
            session
                .atomic(async |tx| {
                    producer.publish(&tx, &"lost".to_owned()).await.unwrap();
                    Err(ascetic_ddd_outbox::Error::Malformed(
                        "rolled back on purpose".into(),
                    ))
                })
                .await
        })
        .await;

    // Committed: crosses the bridge.
    sessions
        .session(async |session| {
            session
                .atomic(async |tx| {
                    producer.publish(&tx, &"placed".to_owned()).await.unwrap();
                    Ok::<(), ascetic_ddd_outbox::Error>(())
                })
                .await
        })
        .await
        .unwrap();

    let message = tokio::time::timeout(Duration::from_secs(20), inbox.recv())
        .await
        .expect("the committed message reaches the in-memory channel")
        .unwrap();
    assert_eq!(message.payload(), b"placed");
    assert_eq!(message.key(), Some(&b"order-7"[..]));
    assert_eq!(
        message.header("destination"),
        Some(&b"in-memory://orders/order-7"[..])
    );
    assert_eq!(
        message.header("message_id"),
        Some(&b"00000000-0000-4000-8000-000000000001"[..])
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), inbox.recv())
            .await
            .is_err(),
        "the rolled-back message never arrives"
    );
    dispatcher.cancel();
}
