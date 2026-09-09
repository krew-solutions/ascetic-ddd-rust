//! Tests for the bridge, over two in-memory brokers: what arrives on `a://`
//! is published on `b://`.

use std::sync::Arc;
use std::time::Duration;

use ascetic_ddd_bus::adapters::in_memory::InMemoryBroker;
use ascetic_ddd_bus::{Bridge, Bus, Message, Target};
use tokio::sync::mpsc;

fn two_brokers() -> Arc<Bus> {
    let mut bus = Bus::new();
    bus.register("a", InMemoryBroker::new()).unwrap();
    bus.register("b", InMemoryBroker::new()).unwrap();
    Arc::new(bus)
}

/// A consumer on `uri` that hands every wire message to a channel.
fn collect(bus: &Bus, uri: &str) -> mpsc::UnboundedReceiver<Message> {
    let consumer = bus
        .consumer(uri, "collector", |m: &Message| Ok(m.clone()))
        .unwrap();
    let (seen, inbox) = mpsc::unbounded_channel();
    consumer
        .subscribe(move |message: Message| {
            let seen = seen.clone();
            async move {
                seen.send(message).ok();
            }
        })
        .unwrap();
    inbox
}

async fn next(inbox: &mut mpsc::UnboundedReceiver<Message>) -> Option<Message> {
    tokio::time::timeout(Duration::from_secs(1), inbox.recv())
        .await
        .ok()
        .flatten()
}

async fn nothing_arrives(inbox: &mut mpsc::UnboundedReceiver<Message>) -> bool {
    matches!(
        tokio::time::timeout(Duration::from_millis(50), inbox.recv()).await,
        Err(_) | Ok(None)
    )
}

/// The destination is read from a header, so one channel feeds many; key,
/// payload and headers arrive untouched.
#[tokio::test]
async fn a_bridge_forwards_to_the_destination_a_header_names() {
    let bus = two_brokers();
    let mut orders = collect(&bus, "b://orders");
    let mut payments = collect(&bus, "b://payments");
    let bridge = Bridge::new(Arc::clone(&bus));
    let _run = bridge
        .run(
            "a://outbox",
            "dispatcher",
            Target::Header("destination".into()),
        )
        .unwrap();

    let producer = bus.producer("a://outbox", |m: &Message| m.clone()).unwrap();
    producer
        .publish(&Message::new("order placed").with_header("destination", "b://orders/order-7"))
        .await
        .unwrap();
    producer
        .publish(&Message::new("paid").with_header("destination", "b://payments"))
        .await
        .unwrap();

    let order = next(&mut orders).await.unwrap();
    assert_eq!(order.payload(), b"order placed");
    assert_eq!(
        order.key(),
        Some(&b"order-7"[..]),
        "the key comes from the destination URI"
    );
    assert_eq!(
        order.header("destination"),
        Some(&b"b://orders/order-7"[..])
    );
    assert_eq!(next(&mut payments).await.unwrap().payload(), b"paid");
}

#[tokio::test]
async fn a_bridge_forwards_to_a_fixed_target() {
    let bus = two_brokers();
    let mut mirror = collect(&bus, "b://mirror");
    let bridge = Bridge::new(Arc::clone(&bus));
    let _run = bridge
        .run("a://events", "mirror", Target::Fixed("b://mirror".into()))
        .unwrap();

    bus.producer("a://events", |m: &Message| m.clone())
        .unwrap()
        .publish(&Message::new("x"))
        .await
        .unwrap();

    assert_eq!(next(&mut mirror).await.unwrap().payload(), b"x");
}

/// A message without a destination cannot be forwarded: the handler fails,
/// and nothing is published anywhere.
#[tokio::test]
async fn a_message_without_a_destination_is_not_forwarded() {
    let bus = two_brokers();
    let mut orders = collect(&bus, "b://orders");
    let bridge = Bridge::new(Arc::clone(&bus));
    let _run = bridge
        .run(
            "a://outbox",
            "dispatcher",
            Target::Header("destination".into()),
        )
        .unwrap();

    bus.producer("a://outbox", |m: &Message| m.clone())
        .unwrap()
        .publish(&Message::new("lost"))
        .await
        .unwrap();

    assert!(nothing_arrives(&mut orders).await);
}
