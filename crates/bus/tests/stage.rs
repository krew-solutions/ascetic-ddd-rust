//! Stages of the wire: a message goes out through them and comes back in
//! through them, and a stage's failure is the message's.

use std::sync::{Arc, Mutex};

use ascetic_ddd_bus::adapters::in_memory::InMemoryBroker;
use ascetic_ddd_bus::{
    BoxError, Bus, Error, Message, Permanent, Stage, Subscription, TransactionalConsumer,
    TransactionalHandler, TransactionalProducer, TransactionalWireConsumer,
    TransactionalWireProducer,
};
use futures::future::BoxFuture;
use tokio::sync::mpsc;

/// Reverses the payload on the way out and back on the way in, and marks
/// the message with a header while it is reversed.
struct Reversing;

impl Stage for Reversing {
    fn outbound(&self, message: Message) -> BoxFuture<'_, Result<Message, BoxError>> {
        Box::pin(async move {
            let reversed: Vec<u8> = message.payload().iter().rev().copied().collect();
            Ok(message
                .with_payload(reversed)
                .with_header("reversed", "yes"))
        })
    }

    fn inbound(&self, message: Message) -> BoxFuture<'_, Result<Message, BoxError>> {
        Box::pin(async move {
            if message.header("reversed").is_none() {
                return Err("the message was not reversed".into());
            }
            let restored: Vec<u8> = message.payload().iter().rev().copied().collect();
            Ok(message.with_payload(restored).without_header("reversed"))
        })
    }
}

/// Appends a byte on the way out and strips it on the way in.
struct Tagging(u8);

impl Stage for Tagging {
    fn outbound(&self, message: Message) -> BoxFuture<'_, Result<Message, BoxError>> {
        Box::pin(async move {
            let mut payload = message.payload().to_vec();
            payload.push(self.0);
            Ok(message.with_payload(payload))
        })
    }

    fn inbound(&self, message: Message) -> BoxFuture<'_, Result<Message, BoxError>> {
        Box::pin(async move {
            let mut payload = message.payload().to_vec();
            match payload.pop() {
                Some(tag) if tag == self.0 => Ok(message.with_payload(payload)),
                _ => Err(Permanent::new("not tagged by this stage").into()),
            }
        })
    }
}

/// Refuses every message.
struct Refusing;

impl Stage for Refusing {
    fn outbound(&self, _: Message) -> BoxFuture<'_, Result<Message, BoxError>> {
        Box::pin(async { Err("refused on the way out".into()) })
    }

    fn inbound(&self, _: Message) -> BoxFuture<'_, Result<Message, BoxError>> {
        Box::pin(async { Err(Permanent::new("refused on the way in").into()) })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_message_goes_out_through_the_stages_and_comes_back_in_through_them() {
    let mut bus = Bus::new();
    bus.register("in-memory", InMemoryBroker::new()).unwrap();
    let (seen, mut inbox) = mpsc::unbounded_channel();

    let consumer = bus
        .consumer("in-memory://orders", "billing", |message: &Message| {
            Ok(String::from_utf8(message.payload().to_vec())?)
        })
        .unwrap()
        .through(Arc::new(Reversing))
        .through(Arc::new(Tagging(b'!')));
    let _subscription = consumer
        .subscribe(move |order: String| {
            let seen = seen.clone();
            async move {
                seen.send(order).ok();
            }
        })
        .unwrap();

    // Without the stages, the wire carries the reversed, tagged bytes.
    let (raw, mut raw_inbox) = mpsc::unbounded_channel();
    let _raw = bus
        .consumer("in-memory://orders", "audit", |message: &Message| {
            Ok(message.clone())
        })
        .unwrap()
        .subscribe(move |message: Message| {
            let raw = raw.clone();
            async move {
                raw.send(message).ok();
            }
        })
        .unwrap();

    let producer = bus
        .producer("in-memory://orders", |order: &String| {
            Message::new(order.as_bytes())
        })
        .unwrap()
        .through(Arc::new(Reversing))
        .through(Arc::new(Tagging(b'!')));
    producer.publish(&"order-7".to_owned()).await.unwrap();

    assert_eq!(inbox.recv().await.as_deref(), Some("order-7"));
    let on_the_wire = raw_inbox.recv().await.unwrap();
    assert_eq!(on_the_wire.payload(), b"7-redro!");
    assert_eq!(on_the_wire.header("reversed"), Some(&b"yes"[..]));
}

#[tokio::test(flavor = "current_thread")]
async fn a_stage_that_refuses_fails_the_publish() {
    let mut bus = Bus::new();
    bus.register("in-memory", InMemoryBroker::new()).unwrap();
    let producer = bus
        .producer("in-memory://orders", |order: &String| {
            Message::new(order.as_bytes())
        })
        .unwrap()
        .through(Arc::new(Refusing));
    assert!(matches!(
        producer.publish(&"order-7".to_owned()).await,
        Err(Error::Stage(error)) if error.to_string() == "refused on the way out"
    ));
}

// The transactional twins, over fake wires that hand the handler over.

#[derive(Clone, Debug, PartialEq, Eq)]
struct Tx(&'static str);

struct Recording(Arc<Mutex<Vec<(Tx, Message)>>>);

impl TransactionalWireProducer<Tx> for Recording {
    fn publish<'a>(
        &'a self,
        session: &'a Tx,
        message: Message,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.0.lock().unwrap().push((session.clone(), message));
            Ok(())
        })
    }
}

struct Handing(Arc<Mutex<Option<TransactionalHandler<Tx>>>>);

impl TransactionalWireConsumer<Tx> for Handing {
    fn subscribe(&self, handler: TransactionalHandler<Tx>) -> Result<Subscription, Error> {
        *self.0.lock().unwrap() = Some(handler);
        Ok(Subscription::new(|| {}))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_transactional_producer_publishes_what_its_stages_made() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let producer =
        TransactionalProducer::new(Box::new(Recording(Arc::clone(&seen))), |order: &u32| {
            Message::new(order.to_string())
        })
        .through(Arc::new(Reversing));
    producer.publish(&Tx("tx-1"), &42).await.unwrap();
    let published = seen.lock().unwrap();
    assert_eq!(published[0].0, Tx("tx-1"));
    assert_eq!(published[0].1.payload(), b"24");
}

#[tokio::test(flavor = "current_thread")]
async fn a_stage_that_refuses_on_the_way_in_fails_the_handling_and_keeps_its_verdict() {
    let slot = Arc::new(Mutex::new(None));
    let handled = Arc::new(Mutex::new(Vec::new()));
    let consumer =
        TransactionalConsumer::new(Box::new(Handing(Arc::clone(&slot))), |message: &Message| {
            Ok(String::from_utf8(message.payload().to_vec())?)
        })
        .through(Arc::new(Reversing))
        .through(Arc::new(Tagging(b'!')));
    let seen = Arc::clone(&handled);
    let _subscription = consumer
        .subscribe(move |tx: Tx, order: String| {
            let seen = Arc::clone(&seen);
            async move {
                seen.lock().unwrap().push((tx, order));
            }
        })
        .unwrap();
    let handler = slot.lock().unwrap().clone().unwrap();

    // What the stages made on the way out comes back in, in reverse order.
    let outcome = handler(
        Tx("tx-1"),
        Message::new(b"7-redro!".to_vec()).with_header("reversed", "yes"),
    )
    .await;
    assert!(outcome.is_ok());
    assert_eq!(
        handled.lock().unwrap().as_slice(),
        &[(Tx("tx-1"), "order-7".to_owned())]
    );

    // A message the stages cannot take back is not handled, and not skipped.
    let outcome = handler(Tx("tx-2"), Message::new(b"untagged".to_vec())).await;
    let error = outcome.unwrap_err();
    assert!(error.is::<Permanent>(), "{error}");
    assert_eq!(handled.lock().unwrap().len(), 1);
}
