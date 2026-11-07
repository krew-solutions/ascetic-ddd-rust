//! Integration tests for the Kafka adapter.
//!
//! They need a live broker, so they are ignored by default:
//!
//! ```text
//! ASCETIC_DDD_TEST_KAFKA_BROKERS=localhost:9092 \
//!     cargo test -p ascetic-ddd-bus --features kafka -- --ignored
//! ```
//!
//! A broker behind SASL takes `ASCETIC_DDD_TEST_KAFKA_SASL_MECHANISM`,
//! `ASCETIC_DDD_TEST_KAFKA_SASL_USERNAME` and
//! `ASCETIC_DDD_TEST_KAFKA_SASL_PASSWORD` as well.

#![cfg(feature = "kafka")]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ascetic_ddd_bus::adapters::kafka::KafkaBroker;
use ascetic_ddd_bus::{BoxError, Bus, Consumer, Message, Subscription};
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
    setup_with(&[])
}

/// The test broker: at `ASCETIC_DDD_TEST_KAFKA_BROKERS`, and behind SASL
/// when `ASCETIC_DDD_TEST_KAFKA_SASL_MECHANISM`, `..._USERNAME` and
/// `..._PASSWORD` are set, as a stand with access lists has it.
fn test_broker() -> KafkaBroker {
    let brokers = std::env::var("ASCETIC_DDD_TEST_KAFKA_BROKERS")
        .unwrap_or_else(|_| DEFAULT_BROKERS.to_owned());
    let broker = KafkaBroker::new(&brokers)
        .set_consumer("auto.offset.reset", "earliest")
        .set("allow.auto.create.topics", "true");
    let sasl = |name: &str| std::env::var(format!("ASCETIC_DDD_TEST_KAFKA_SASL_{name}")).ok();
    match (sasl("MECHANISM"), sasl("USERNAME"), sasl("PASSWORD")) {
        (Some(mechanism), Some(username), Some(password)) => broker
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.mechanism", &mechanism)
            .set("sasl.username", &username)
            .set("sasl.password", &password),
        _ => broker,
    }
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

    subscription.cancel().await;
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

/// A consumer of `uri` handing each message's key and payload on.
fn collect_keyed(bus: &Bus, uri: &str, group: &str) -> mpsc::UnboundedReceiver<(String, String)> {
    let consumer = bus
        .consumer(uri, group, |message: &Message| {
            let key = String::from_utf8(message.key().unwrap_or_default().to_vec())?;
            Ok((key, as_text(message)?))
        })
        .unwrap();
    let (seen, inbox) = mpsc::unbounded_channel();
    consumer
        .subscribe(move |keyed: (String, String)| {
            let seen = seen.clone();
            async move {
                seen.send(keyed).ok();
            }
        })
        .unwrap();
    inbox
}

/// Producers of one broker — two topics, each destination's key in its URI
/// as an outbox stores it — send through the broker's one client, and every
/// message arrives at its topic under its key, in order per key (ADR-0032).
#[tokio::test]
#[ignore = "requires Kafka; run with --ignored"]
async fn producers_of_one_broker_deliver_to_their_topics_under_their_keys() {
    let bus = setup();
    let topics = [unique("orders"), unique("payments")];
    let mut inboxes: Vec<_> = topics
        .iter()
        .map(|topic| collect_keyed(&bus, &format!("kafka://{topic}"), &unique("g")))
        .collect();
    for topic in &topics {
        for key in 0..10 {
            let producer = bus
                .producer(
                    &format!("kafka://{topic}/tenant-{key}"),
                    |value: &String| Message::new(value.as_bytes()),
                )
                .unwrap();
            for i in 0..3 {
                producer.publish(&format!("{key}:{i}")).await.unwrap();
            }
        }
    }
    for inbox in &mut inboxes {
        let mut arrived: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for _ in 0..30 {
            let (key, value) = tokio::time::timeout(Duration::from_secs(30), inbox.recv())
                .await
                .ok()
                .flatten()
                .expect("every message arrives");
            arrived.entry(key).or_default().push(value);
        }
        for key in 0..10 {
            let expected: Vec<String> = (0..3).map(|i| format!("{key}:{i}")).collect();
            assert_eq!(arrived[&format!("tenant-{key}")], expected);
        }
    }
}

/// A bus on the test broker, its consumers given `settings` as well.
fn setup_with(settings: &[(&str, &str)]) -> Bus {
    let broker = settings.iter().fold(test_broker(), |broker, (key, value)| {
        broker.set_consumer(key, value)
    });
    let mut bus = Bus::new();
    bus.register("kafka", broker).unwrap();
    bus
}

/// Publishes `values` under one key, so to one partition, in order.
async fn publish_all(bus: &Bus, uri: &str, values: &[&str]) {
    let producer = bus.producer(uri, keyed).unwrap();
    for value in values {
        producer
            .publish(&("k".to_owned(), (*value).to_owned()))
            .await
            .unwrap();
    }
}

/// Subscribes a handler that counts its calls and fails, for good.
fn failing_for_good(consumer: &Consumer<String>) -> (Arc<AtomicUsize>, Subscription) {
    let calls = Arc::new(AtomicUsize::new(0));
    let subscription = consumer
        .subscribe({
            let calls = Arc::clone(&calls);
            move |_: String| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Err::<(), BoxError>("down".into()) }
            }
        })
        .unwrap();
    (calls, subscription)
}

/// Subscribes a handler that hands each value on.
fn collecting(consumer: &Consumer<String>) -> (mpsc::UnboundedReceiver<String>, Subscription) {
    let (seen, inbox) = mpsc::unbounded_channel();
    let subscription = consumer
        .subscribe(move |value: String| {
            let seen = seen.clone();
            async move {
                seen.send(value).ok();
            }
        })
        .unwrap();
    (inbox, subscription)
}

async fn called(calls: &AtomicUsize, n: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while calls.load(Ordering::SeqCst) < n {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the handler was never called {n} times"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A fresh member of `group` on `uri`, after the others have left: the
/// first message it is given, `m2` published for it, says where the group's
/// committed offset stood.
async fn first_for_a_fresh_member(bus: &Bus, uri: &str, group: &str) -> Option<String> {
    publish_all(bus, uri, &["m2"]).await;
    let fresh = bus.consumer(uri, group, as_text).unwrap();
    let (mut inbox, _subscription) = collecting(&fresh);
    next(&mut inbox).await
}

/// A handler's error means the message was not taken: it is tried again, in
/// order, and the next waits until it is (ADR-0036 decision 3).
#[tokio::test]
#[ignore = "requires Kafka; run with --ignored"]
async fn a_failing_handler_is_retried_in_order() {
    let bus = setup();
    let uri = format!("kafka://{}", unique("retried"));
    let consumer = bus.consumer(&uri, &unique("g"), as_text).unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let (seen, mut inbox) = mpsc::unbounded_channel();
    let _subscription = consumer
        .subscribe({
            let attempts = Arc::clone(&attempts);
            move |value: String| {
                let (seen, attempts) = (seen.clone(), Arc::clone(&attempts));
                async move {
                    if value == "m0" && attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                        return Err::<(), BoxError>("refused".into());
                    }
                    seen.send(value).ok();
                    Ok(())
                }
            }
        })
        .unwrap();
    publish_all(&bus, &uri, &["m0", "m1"]).await;

    assert_eq!(next(&mut inbox).await.as_deref(), Some("m0"));
    assert_eq!(next(&mut inbox).await.as_deref(), Some("m1"));
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        3,
        "two failures and a success"
    );
}

/// A subscription that replaces a failing one is given the message in hand,
/// then the next; once both are taken, their offsets are committed: a fresh
/// member of the group starts after them. Before, the replaced loop gave the
/// message up and the driver's position had passed it: it was lost.
#[tokio::test]
#[ignore = "requires Kafka; run with --ignored"]
async fn a_replacement_while_retrying_takes_the_message_in_hand() {
    let bus = setup();
    let (uri, group) = (format!("kafka://{}", unique("replaced")), unique("g"));
    let consumer = bus.consumer(&uri, &group, as_text).unwrap();
    let (calls, _failing) = failing_for_good(&consumer);
    publish_all(&bus, &uri, &["m0", "m1"]).await;
    called(&calls, 1).await;

    let (mut inbox, replacement) = collecting(&consumer);
    assert_eq!(next(&mut inbox).await.as_deref(), Some("m0"));
    assert_eq!(next(&mut inbox).await.as_deref(), Some("m1"));

    replacement.cancel().await;
    drop(consumer);
    assert_eq!(
        first_for_a_fresh_member(&bus, &uri, &group)
            .await
            .as_deref(),
        Some("m2")
    );
}

/// A cancel in the pause between two tries, then a subscription on the same
/// consumer: the message in hand is given to it first.
#[tokio::test]
#[ignore = "requires Kafka; run with --ignored"]
async fn a_subscription_after_a_cancel_takes_the_message_in_hand() {
    let bus = setup();
    let uri = format!("kafka://{}", unique("resubscribed"));
    let consumer = bus.consumer(&uri, &unique("g"), as_text).unwrap();
    let (calls, failing) = failing_for_good(&consumer);
    publish_all(&bus, &uri, &["m0", "m1"]).await;
    called(&calls, 1).await;
    failing.cancel().await;
    let after_cancel = calls.load(Ordering::SeqCst);

    let (mut inbox, _subscription) = collecting(&consumer);
    assert_eq!(next(&mut inbox).await.as_deref(), Some("m0"));
    assert_eq!(next(&mut inbox).await.as_deref(), Some("m1"));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        after_cancel,
        "no call of the cancelled handler"
    );
}

/// A message never taken keeps its offset unstored: cancelled and dropped,
/// the consumer leaves the group, and the next member is given it.
#[tokio::test]
#[ignore = "requires Kafka; run with --ignored"]
async fn a_message_never_taken_goes_to_the_next_member() {
    let bus = setup();
    let (uri, group) = (format!("kafka://{}", unique("untaken")), unique("g"));
    let consumer = bus.consumer(&uri, &group, as_text).unwrap();
    let (calls, failing) = failing_for_good(&consumer);
    publish_all(&bus, &uri, &["m0"]).await;
    called(&calls, 1).await;

    failing.cancel().await;
    drop(consumer);
    let next_member = bus.consumer(&uri, &group, as_text).unwrap();
    let (mut inbox, _subscription) = collecting(&next_member);
    assert_eq!(next(&mut inbox).await.as_deref(), Some("m0"));
}

/// A member that holds a message past `max.poll.interval.ms` loses its
/// partitions: the other member of the group is given the message and
/// takes it, and the group's committed offset passes it. Nothing is lost.
#[tokio::test]
#[ignore = "requires Kafka; run with --ignored"]
async fn a_member_holding_past_the_poll_interval_lets_the_message_go_to_another() {
    let bus = setup_with(&[
        ("max.poll.interval.ms", "6000"),
        ("session.timeout.ms", "6000"),
    ]);
    let (uri, group) = (format!("kafka://{}", unique("evicted")), unique("g"));
    let stuck = bus.consumer(&uri, &group, as_text).unwrap();
    let (calls, stuck_subscription) = failing_for_good(&stuck);
    publish_all(&bus, &uri, &["m0", "m1"]).await;
    called(&calls, 1).await;

    let healthy = bus.consumer(&uri, &group, as_text).unwrap();
    let taken = Arc::new(Mutex::new(Vec::new()));
    let healthy_subscription = healthy
        .subscribe({
            let taken = Arc::clone(&taken);
            move |value: String| {
                taken.lock().unwrap().push(value);
                async {}
            }
        })
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !taken.lock().unwrap().contains(&"m1".to_owned()) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the healthy member took {:?}; the stuck one was called {} times",
            taken.lock().unwrap(),
            calls.load(Ordering::SeqCst)
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        taken.lock().unwrap()[..2],
        ["m0".to_owned(), "m1".to_owned()]
    );

    healthy_subscription.cancel().await;
    drop(healthy);
    stuck_subscription.cancel().await;
    drop(stuck);
    assert_eq!(
        first_for_a_fresh_member(&bus, &uri, &group)
            .await
            .as_deref(),
        Some("m2"),
        "the committed offset passed what the healthy member took"
    );
}
