//! Tests for the in-memory adapter. Each test builds a bus and a broker of
//! its own, so tests run in parallel without sharing state.

use std::time::Duration;

use ascetic_ddd_bus::adapters::in_memory::InMemoryBroker;
use ascetic_ddd_bus::{BoxError, Bus, Consumer, Error, Message, Producer};
use tokio::sync::mpsc;

fn setup() -> Bus {
    let mut bus = Bus::new();
    bus.register("in-memory", InMemoryBroker::new()).unwrap();
    bus
}

fn as_text(message: &Message) -> Result<String, BoxError> {
    Ok(String::from_utf8(message.payload().to_vec())?)
}

fn text(value: &String) -> Message {
    Message::new(value.as_bytes())
}

fn consumer(bus: &Bus, uri: &str, group: &str) -> Consumer<String> {
    bus.consumer(uri, group, as_text).unwrap()
}

fn producer(bus: &Bus, uri: &str) -> Producer<String> {
    bus.producer(uri, text).unwrap()
}

/// Subscribes and returns where the handler puts what it receives.
fn collect<T: Send + 'static>(consumer: &Consumer<T>) -> mpsc::UnboundedReceiver<T> {
    let (seen, inbox) = mpsc::unbounded_channel();
    consumer
        .subscribe(move |value| {
            let seen = seen.clone();
            async move {
                seen.send(value).ok();
            }
        })
        .unwrap();
    inbox
}

async fn next<T>(inbox: &mut mpsc::UnboundedReceiver<T>) -> Option<T> {
    tokio::time::timeout(Duration::from_secs(1), inbox.recv())
        .await
        .ok()
        .flatten()
}

/// Gives delivery a chance, then reports whether anything arrived. A closed
/// channel - the handler, and the sender it owned, are gone - is nothing too.
async fn nothing_arrives<T>(inbox: &mut mpsc::UnboundedReceiver<T>) -> bool {
    matches!(
        tokio::time::timeout(Duration::from_millis(50), inbox.recv()).await,
        Err(_) | Ok(None)
    )
}

#[tokio::test]
async fn a_message_goes_from_producer_to_consumer() {
    let bus = setup();
    let mut inbox = collect(&consumer(&bus, "in-memory://test.t1", "g"));

    producer(&bus, "in-memory://test.t1")
        .publish(&"hello".to_owned())
        .await
        .unwrap();

    assert_eq!(next(&mut inbox).await.as_deref(), Some("hello"));
}

#[tokio::test]
async fn publishing_without_a_consumer_is_not_an_error() {
    let bus = setup();
    producer(&bus, "in-memory://test.t1")
        .publish(&"abandoned".to_owned())
        .await
        .unwrap();
}

#[tokio::test]
async fn every_group_receives_the_message() {
    let bus = setup();
    let mut first = collect(&consumer(&bus, "in-memory://test.t1", "g1"));
    let mut second = collect(&consumer(&bus, "in-memory://test.t1", "g2"));

    producer(&bus, "in-memory://test.t1")
        .publish(&"x".to_owned())
        .await
        .unwrap();

    assert_eq!(next(&mut first).await.as_deref(), Some("x"));
    assert_eq!(next(&mut second).await.as_deref(), Some("x"));
}

#[tokio::test]
async fn one_consumer_per_group() {
    let bus = setup();
    let _first = consumer(&bus, "in-memory://test.t1", "g");

    let refused = bus.consumer("in-memory://test.t1", "g", as_text).err();
    assert!(matches!(
        refused,
        Some(Error::AlreadyInGroup { uri, group }) if uri == "in-memory://test.t1" && group == "g"
    ));
}

#[tokio::test]
async fn topics_do_not_leak_into_each_other() {
    let bus = setup();
    let mut inbox = collect(&consumer(&bus, "in-memory://test.uri-A", "g"));

    producer(&bus, "in-memory://test.uri-B")
        .publish(&"wrong-topic".to_owned())
        .await
        .unwrap();

    assert!(nothing_arrives(&mut inbox).await);
}

#[tokio::test]
async fn brokers_do_not_leak_into_each_other() {
    let bus_a = setup();
    let bus_b = setup();
    let mut inbox = collect(&consumer(&bus_a, "in-memory://shared", "g"));

    producer(&bus_b, "in-memory://shared")
        .publish(&"from-b".to_owned())
        .await
        .unwrap();

    assert!(nothing_arrives(&mut inbox).await);
}

/// The wire is the contract: two consumers of one topic read the same bytes
/// as different types.
#[tokio::test]
async fn consumers_of_one_topic_may_decode_differently() {
    let bus = setup();
    let as_number = bus
        .consumer("in-memory://test.t1", "as-int", |message: &Message| {
            Ok(std::str::from_utf8(message.payload())?.parse::<i32>()?)
        })
        .unwrap();
    let mut numbers = collect(&as_number);
    let mut texts = collect(&consumer(&bus, "in-memory://test.t1", "as-str"));

    bus.producer("in-memory://test.t1", |n: &i32| Message::new(n.to_string()))
        .unwrap()
        .publish(&42)
        .await
        .unwrap();

    assert_eq!(next(&mut numbers).await, Some(42));
    assert_eq!(next(&mut texts).await.as_deref(), Some("42"));
}

#[tokio::test]
async fn cancelling_twice_is_harmless() {
    let bus = setup();
    let consumer = consumer(&bus, "in-memory://test.t1", "g");
    let subscription = consumer.subscribe(|_| async {}).unwrap();

    subscription.cancel();
    subscription.cancel();
}

// ------------------------- beyond the OCaml suite -------------------------

#[tokio::test]
async fn a_cancelled_subscription_receives_nothing_more() {
    let bus = setup();
    let consumer = consumer(&bus, "in-memory://test.t1", "g");
    let (seen, mut inbox) = mpsc::unbounded_channel();
    let subscription = consumer
        .subscribe(move |value: String| {
            let seen = seen.clone();
            async move {
                seen.send(value).ok();
            }
        })
        .unwrap();
    let producer = producer(&bus, "in-memory://test.t1");

    producer.publish(&"before".to_owned()).await.unwrap();
    assert_eq!(next(&mut inbox).await.as_deref(), Some("before"));

    subscription.cancel();
    producer.publish(&"after".to_owned()).await.unwrap();
    assert!(nothing_arrives(&mut inbox).await);
}

/// A message the consumer cannot decode is skipped, not fatal.
#[tokio::test]
async fn an_undecodable_message_is_skipped() {
    let bus = setup();
    let as_number = bus
        .consumer("in-memory://test.t1", "g", |message: &Message| {
            Ok(std::str::from_utf8(message.payload())?.parse::<i32>()?)
        })
        .unwrap();
    let mut numbers = collect(&as_number);
    let producer = producer(&bus, "in-memory://test.t1");

    producer.publish(&"not a number".to_owned()).await.unwrap();
    producer.publish(&"7".to_owned()).await.unwrap();

    assert_eq!(next(&mut numbers).await, Some(7));
}

/// A handler that panics loses its message, not the topic.
#[tokio::test]
async fn a_panicking_handler_does_not_stop_delivery() {
    let bus = setup();
    let consumer = consumer(&bus, "in-memory://test.t1", "g");
    let (seen, mut inbox) = mpsc::unbounded_channel();
    consumer
        .subscribe(move |value: String| {
            let seen = seen.clone();
            async move {
                assert_ne!(value, "boom");
                seen.send(value).ok();
            }
        })
        .unwrap();
    let producer = producer(&bus, "in-memory://test.t1");

    producer.publish(&"boom".to_owned()).await.unwrap();
    producer.publish(&"after".to_owned()).await.unwrap();

    assert_eq!(next(&mut inbox).await.as_deref(), Some("after"));
}

/// Messages of one topic arrive in the order they were published.
#[tokio::test]
async fn messages_keep_their_order() {
    let bus = setup();
    let mut inbox = collect(&consumer(&bus, "in-memory://test.t1", "g"));
    let producer = producer(&bus, "in-memory://test.t1");

    for i in 0..100 {
        producer.publish(&i.to_string()).await.unwrap();
    }

    for i in 0..100 {
        assert_eq!(next(&mut inbox).await, Some(i.to_string()));
    }
}

/// `scheme://channel/key`: the key does not make a topic of its own, and a
/// message published with it carries it.
#[tokio::test]
async fn a_key_in_the_uri_selects_the_channel_and_keys_the_message() {
    let bus = setup();
    let consumer = bus
        .consumer("in-memory://orders", "g", |m: &Message| Ok(m.clone()))
        .unwrap();
    let (seen, mut inbox) = mpsc::unbounded_channel();
    consumer
        .subscribe(move |message: Message| {
            let seen = seen.clone();
            async move {
                seen.send(message).ok();
            }
        })
        .unwrap();

    bus.producer("in-memory://orders/order-7", |m: &Message| m.clone())
        .unwrap()
        .publish(&Message::new("x"))
        .await
        .unwrap();

    let message = tokio::time::timeout(Duration::from_secs(1), inbox.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.key(), Some(&b"order-7"[..]));
}

/// A handler that fails does not stop delivery of what follows.
#[tokio::test]
async fn a_failing_handler_does_not_stop_delivery() {
    let bus = setup();
    let consumer = consumer(&bus, "in-memory://test.t1", "g");
    let (seen, mut inbox) = mpsc::unbounded_channel();
    consumer
        .subscribe(move |value: String| {
            let seen = seen.clone();
            async move {
                if value == "bad" {
                    return Err::<(), BoxError>("refused".into());
                }
                seen.send(value).ok();
                Ok(())
            }
        })
        .unwrap();
    let producer = producer(&bus, "in-memory://test.t1");

    producer.publish(&"bad".to_owned()).await.unwrap();
    producer.publish(&"good".to_owned()).await.unwrap();

    assert_eq!(next(&mut inbox).await.as_deref(), Some("good"));
}
