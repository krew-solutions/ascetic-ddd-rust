//! Integration tests for the Kafka adapter.
//!
//! They need a live broker, so they are ignored by default:
//!
//! ```text
//! ASCETIC_DDD_TEST_KAFKA_BROKERS=localhost:9092 \
//!     cargo test -p ascetic-ddd-bus --features kafka -- --ignored
//! ```

#![cfg(feature = "kafka")]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ascetic_ddd_bus::adapters::kafka::KafkaBroker;
use ascetic_ddd_bus::{BoxError, Bus, Message};
use tokio::sync::mpsc;

const DEFAULT_BROKERS: &str = "localhost:9092";

/// Each test gets topics and groups of its own, so runs do not see each
/// other's messages.
fn unique(name: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("{name}-{nanos}-{}", COUNTER.fetch_add(1, Ordering::Relaxed))
}

fn setup() -> Bus {
    let brokers = std::env::var("ASCETIC_DDD_TEST_KAFKA_BROKERS")
        .unwrap_or_else(|_| DEFAULT_BROKERS.to_owned());
    let mut bus = Bus::new();
    bus.register(
        "kafka",
        KafkaBroker::new(&brokers)
            .set("auto.offset.reset", "earliest")
            .set("allow.auto.create.topics", "true"),
    )
    .unwrap();
    bus
}

fn as_text(message: &Message) -> Result<String, BoxError> {
    Ok(String::from_utf8(message.payload().to_vec())?)
}

fn keyed(value: &(String, String)) -> Message {
    Message::new(value.1.as_bytes()).with_key(value.0.as_bytes())
}

fn collect(bus: &Bus, uri: &str, group: &str) -> mpsc::UnboundedReceiver<String> {
    let consumer = bus.consumer(uri, group, as_text).unwrap();
    let (seen, inbox) = mpsc::unbounded_channel();
    consumer
        .subscribe(move |value: String| {
            let seen = seen.clone();
            async move {
                seen.send(value).ok();
            }
        })
        .unwrap();
    inbox
}

async fn next(inbox: &mut mpsc::UnboundedReceiver<String>) -> Option<String> {
    tokio::time::timeout(Duration::from_secs(30), inbox.recv())
        .await
        .ok()
        .flatten()
}

#[tokio::test]
#[ignore = "requires Kafka; run with --ignored"]
async fn a_message_goes_from_producer_to_consumer() {
    let bus = setup();
    let uri = format!("kafka://{}", unique("roundtrip"));
    let mut inbox = collect(&bus, &uri, &unique("g"));

    bus.producer(&uri, keyed)
        .unwrap()
        .publish(&("order-1".to_owned(), "hello".to_owned()))
        .await
        .unwrap();

    assert_eq!(next(&mut inbox).await.as_deref(), Some("hello"));
}

#[tokio::test]
#[ignore = "requires Kafka; run with --ignored"]
async fn every_group_receives_the_message() {
    let bus = setup();
    let uri = format!("kafka://{}", unique("fanout"));
    let mut first = collect(&bus, &uri, &unique("g1"));
    let mut second = collect(&bus, &uri, &unique("g2"));

    bus.producer(&uri, keyed)
        .unwrap()
        .publish(&("order-1".to_owned(), "x".to_owned()))
        .await
        .unwrap();

    assert_eq!(next(&mut first).await.as_deref(), Some("x"));
    assert_eq!(next(&mut second).await.as_deref(), Some("x"));
}

/// Messages with one key arrive in the order they were published.
#[tokio::test]
#[ignore = "requires Kafka; run with --ignored"]
async fn messages_with_one_key_keep_their_order() {
    let bus = setup();
    let uri = format!("kafka://{}", unique("order"));
    let mut inbox = collect(&bus, &uri, &unique("g"));
    let producer = bus.producer(&uri, keyed).unwrap();

    for i in 0..20 {
        producer
            .publish(&("order-1".to_owned(), i.to_string()))
            .await
            .unwrap();
    }

    for i in 0..20 {
        assert_eq!(next(&mut inbox).await, Some(i.to_string()));
    }
}

#[tokio::test]
#[ignore = "requires Kafka; run with --ignored"]
async fn a_cancelled_subscription_receives_nothing_more() {
    let bus = setup();
    let uri = format!("kafka://{}", unique("cancel"));
    let consumer = bus.consumer(&uri, &unique("g"), as_text).unwrap();
    let (seen, mut inbox) = mpsc::unbounded_channel();
    let subscription = consumer
        .subscribe(move |value: String| {
            let seen = seen.clone();
            async move {
                seen.send(value).ok();
            }
        })
        .unwrap();
    let producer = bus.producer(&uri, keyed).unwrap();

    producer
        .publish(&("k".to_owned(), "before".to_owned()))
        .await
        .unwrap();
    assert_eq!(next(&mut inbox).await.as_deref(), Some("before"));

    subscription.cancel();
    producer
        .publish(&("k".to_owned(), "after".to_owned()))
        .await
        .unwrap();
    // Nothing arrives, or the channel closed with the handler: both are silence.
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), inbox.recv()).await,
        Err(_) | Ok(None)
    ));
}
