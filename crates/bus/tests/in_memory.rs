//! Tests for the in-memory adapter. Each test builds a bus and a broker of
//! its own, so tests run in parallel without sharing state.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ascetic_ddd_bus::adapters::in_memory::{DEFAULT_RETRY_AFTER, InMemoryBroker};
use ascetic_ddd_bus::{BoxError, Bus, Consumer, Error, Message, Producer, Subscription};
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

    subscription.cancel().await;
    subscription.cancel().await;
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

    subscription.cancel().await;
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

/// A handler's error means the message was not taken: it is tried again,
/// in order, and what follows waits until it is taken (ADR-0036 decision 3).
#[tokio::test(start_paused = true)]
async fn a_failing_handler_is_retried_and_holds_what_follows() {
    let bus = setup();
    let consumer = consumer(&bus, "in-memory://test.t1", "g");
    let attempts = Arc::new(AtomicUsize::new(0));
    let (seen, mut inbox) = mpsc::unbounded_channel();
    consumer
        .subscribe({
            let attempts = Arc::clone(&attempts);
            move |value: String| {
                let (seen, attempts) = (seen.clone(), Arc::clone(&attempts));
                async move {
                    if value == "bad" && attempts.fetch_add(1, Ordering::SeqCst) < 3 {
                        return Err::<(), BoxError>("refused".into());
                    }
                    seen.send(value).ok();
                    Ok(())
                }
            }
        })
        .unwrap();
    let producer = producer(&bus, "in-memory://test.t1");

    producer.publish(&"bad".to_owned()).await.unwrap();
    producer.publish(&"good".to_owned()).await.unwrap();

    assert_eq!(next(&mut inbox).await.as_deref(), Some("bad"));
    assert_eq!(next(&mut inbox).await.as_deref(), Some("good"));
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        4,
        "three failures and a success"
    );
}

/// A broker of `capacity`.
fn small(capacity: usize) -> Bus {
    let mut bus = Bus::new();
    bus.register("in-memory", InMemoryBroker::with_capacity(capacity))
        .unwrap();
    bus
}

/// Subscribes a handler that notes what it is given and fails, for good.
fn failing_for_good(consumer: &Consumer<String>) -> (Arc<Mutex<Vec<String>>>, Subscription) {
    let tried = Arc::new(Mutex::new(Vec::new()));
    let subscription = consumer
        .subscribe({
            let tried = Arc::clone(&tried);
            move |value: String| {
                tried.lock().unwrap().push(value);
                async { Err::<(), BoxError>("down".into()) }
            }
        })
        .unwrap();
    (tried, subscription)
}

/// A group whose head fails for good holds itself alone: another group of
/// the topic gets every message, in order, and producers are not held,
/// however far past the capacity they publish.
#[tokio::test(start_paused = true)]
async fn a_group_failing_for_good_holds_neither_another_group_nor_producers() {
    let bus = small(4);
    let failing = consumer(&bus, "in-memory://orders", "failing");
    let (tried, _held) = failing_for_good(&failing);
    let healthy = consumer(&bus, "in-memory://orders", "healthy");
    let mut inbox = collect(&healthy);
    let producer = producer(&bus, "in-memory://orders");

    for i in 0..17 {
        tokio::time::timeout(Duration::from_secs(1), producer.publish(&i.to_string()))
            .await
            .expect("a producer is not held by a group that fails")
            .unwrap();
    }

    for i in 0..17 {
        assert_eq!(next(&mut inbox).await, Some(i.to_string()));
    }
    let tried = tried.lock().unwrap().clone();
    assert!(
        !tried.is_empty() && tried.iter().all(|value| value == "0"),
        "the failing group is tried on its head alone: {tried:?}"
    );
}

/// A handler that does not return holds the producers, once its group's
/// backlog and the topic's queue are full: the back-pressure the queue
/// exists for.
#[tokio::test(start_paused = true)]
async fn a_slow_handler_holds_producers_back() {
    let bus = small(4);
    let blocked = consumer(&bus, "in-memory://orders", "blocked");
    let _subscription = blocked
        .subscribe(|_: String| std::future::pending::<()>())
        .unwrap();
    let producer = producer(&bus, "in-memory://orders");

    let mut published = 0;
    while published < 32
        && tokio::time::timeout(
            Duration::from_secs(1),
            producer.publish(&published.to_string()),
        )
        .await
        .is_ok()
    {
        published += 1;
    }

    assert!(
        (4..=10).contains(&published),
        "{published} published before the producer waited"
    );
}

/// Waits until `calls` has counted at least `n`.
async fn called(calls: &AtomicUsize, n: usize) {
    while calls.load(Ordering::SeqCst) < n {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A cancel during the pause between two tries returns at once and makes no
/// further call; the message in hand stays with the group, and the next
/// handler is given it first, then what followed it.
#[tokio::test(start_paused = true)]
async fn a_cancel_in_the_pause_leaves_the_message_in_hand_to_the_next_handler() {
    let bus = setup();
    let consumer = consumer(&bus, "in-memory://orders", "g");
    let calls = Arc::new(AtomicUsize::new(0));
    let first = consumer
        .subscribe({
            let calls = Arc::clone(&calls);
            move |_: String| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Err::<(), BoxError>("down".into()) }
            }
        })
        .unwrap();
    let producer = producer(&bus, "in-memory://orders");
    producer.publish(&"head".to_owned()).await.unwrap();
    producer.publish(&"next".to_owned()).await.unwrap();
    called(&calls, 1).await;

    tokio::time::timeout(Duration::from_millis(1), first.cancel())
        .await
        .expect("nothing is in flight in the pause");
    let after_cancel = calls.load(Ordering::SeqCst);
    tokio::time::sleep(DEFAULT_RETRY_AFTER * 5).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        after_cancel,
        "no call after the cancel"
    );

    let mut inbox = collect(&consumer);
    assert_eq!(next(&mut inbox).await.as_deref(), Some("head"));
    assert_eq!(next(&mut inbox).await.as_deref(), Some("next"));
}

/// A handler that replaces a failing one is given the message in hand at
/// once, not after the pause; cancelling the earlier subscription leaves the
/// later one attached.
#[tokio::test(start_paused = true)]
async fn a_replacing_handler_takes_the_message_in_hand_at_once() {
    let bus = setup();
    let consumer = consumer(&bus, "in-memory://orders", "g");
    let (tried, earlier) = failing_for_good(&consumer);
    let producer = producer(&bus, "in-memory://orders");
    producer.publish(&"head".to_owned()).await.unwrap();
    while tried.lock().unwrap().is_empty() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let replaced_at = tokio::time::Instant::now();
    let mut inbox = collect(&consumer);
    assert_eq!(next(&mut inbox).await.as_deref(), Some("head"));
    assert!(
        replaced_at.elapsed() < DEFAULT_RETRY_AFTER / 10,
        "tried after {:?}",
        replaced_at.elapsed()
    );

    earlier.cancel().await;
    producer.publish(&"after".to_owned()).await.unwrap();
    assert_eq!(next(&mut inbox).await.as_deref(), Some("after"));
}

/// A consumer dropped with its handler attached goes on delivering, as a
/// subscription made at the composition root does; once the subscription is
/// cancelled as well, the group ends and its name is free again.
#[tokio::test(start_paused = true)]
async fn a_group_ends_with_both_its_subscription_and_its_consumer() {
    let bus = setup();
    let (seen, mut inbox) = mpsc::unbounded_channel();
    let subscription = consumer(&bus, "in-memory://orders", "g")
        .subscribe(move |value: String| {
            let seen = seen.clone();
            async move {
                seen.send(value).ok();
            }
        })
        .unwrap();
    producer(&bus, "in-memory://orders")
        .publish(&"kept".to_owned())
        .await
        .unwrap();
    assert_eq!(next(&mut inbox).await.as_deref(), Some("kept"));
    assert!(
        bus.consumer("in-memory://orders", "g", as_text).is_err(),
        "the group lives with its subscription"
    );

    subscription.cancel().await;
    let mut freed = false;
    for _ in 0..10 {
        if bus.consumer("in-memory://orders", "g", as_text).is_ok() {
            freed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(freed, "the group's name is free once both are gone");
}
