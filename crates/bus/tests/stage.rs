//! Stages of the wire: a message goes out through them and comes back in
//! through them, and a stage's failure is the message's.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use ascetic_ddd_bus::adapters::in_memory::InMemoryBroker;
use ascetic_ddd_bus::{
    BoxError, Bus, Cause, Check, Composition, Dependencies, Dependency, Dependent, Error,
    InboundStage, Message, Permanent, Scope, Stage, Subscription, TransactionalConsumer,
    TransactionalHandler, TransactionalProducer, TransactionalWireConsumer,
    TransactionalWireProducer, Unavailable, cause,
};
use futures::future::BoxFuture;
use tokio::sync::mpsc;

/// Reverses the payload on the way out and back on the way in, and marks
/// the message with a header while it is reversed.
struct Reversing;

impl Dependent for Reversing {
    fn dependencies(&self) -> Dependencies {
        Dependencies::none()
    }
}

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

impl Dependent for Tagging {
    fn dependencies(&self) -> Dependencies {
        Dependencies::none()
    }
}

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
    fn subscribe(
        &self,
        handler: TransactionalHandler<Tx>,
        _: Dependencies,
    ) -> Result<Subscription, Error> {
        *self.0.lock().unwrap() = Some(handler);
        Ok(Subscription::new(|| {}))
    }
}

/// Keeps the dependencies it is subscribed with, as an inbox registers them.
struct Registering(Arc<Mutex<Option<Dependencies>>>);

impl TransactionalWireConsumer<Tx> for Registering {
    fn subscribe(
        &self,
        _: TransactionalHandler<Tx>,
        dependencies: Dependencies,
    ) -> Result<Subscription, Error> {
        *self.0.lock().unwrap() = Some(dependencies);
        Ok(Subscription::new(|| {}))
    }
}

/// A dependency that is never asked: told from another only by identity.
struct Kms;

impl Dependency for Kms {
    fn served(&self) -> u64 {
        0
    }

    fn bound(&self) -> Duration {
        Duration::from_secs(1)
    }

    fn answers_for(&self, _: Cause) -> bool {
        true
    }

    fn check<'a>(&'a self, _: Option<&'a Message>) -> BoxFuture<'a, Check> {
        Box::pin(async { Check::Unasked })
    }
}

/// Passes every message through, and declares a dependency for each of its
/// destinations.
struct Opening(Vec<(&'static str, Arc<dyn Dependency>)>);

impl Dependent for Opening {
    fn dependencies(&self) -> Dependencies {
        Dependencies::by_destination(self.0.iter().map(|(d, k)| (*d, Arc::clone(k))))
            .expect("one dependency a destination")
    }
}

impl Stage for Opening {
    fn outbound(&self, message: Message) -> BoxFuture<'_, Result<Message, BoxError>> {
        Box::pin(async move { Ok(message) })
    }

    fn inbound(&self, message: Message) -> BoxFuture<'_, Result<Message, BoxError>> {
        Box::pin(async move { Ok(message) })
    }
}

fn registering(
    stages: Vec<Arc<dyn InboundStage>>,
) -> (Result<Subscription, Error>, Option<Dependencies>) {
    let registered = Arc::new(Mutex::new(None));
    let consumer = stages.into_iter().fold(
        TransactionalConsumer::new(
            Box::new(Registering(Arc::clone(&registered))),
            |message: &Message| Ok(message.payload().to_vec()),
        ),
        TransactionalConsumer::through,
    );
    let subscribed = consumer.subscribe(|_: Tx, _: Vec<u8>| async {});
    let registered = registered.lock().unwrap().take();
    (subscribed, registered)
}

/// A consumer hands the wire consumer what every stage it goes in through
/// declares, keys left aside (seedwork ADR-0044, S1): the inbox learns its
/// stages' dependencies from the subscription, not from a composition
/// root. Red when the consumer hands none, or one stage's only.
#[test]
fn a_consumer_hands_the_inbox_what_its_stages_depend_on() {
    let (orders, invoices) = (
        Arc::new(Kms) as Arc<dyn Dependency>,
        Arc::new(Kms) as Arc<dyn Dependency>,
    );
    let (subscribed, registered) = registering(vec![
        Arc::new(Opening(vec![("inbox://orders/k1", Arc::clone(&orders))])),
        Arc::new(Reversing),
        Arc::new(Opening(vec![("inbox://invoices", Arc::clone(&invoices))])),
    ]);
    subscribed.unwrap();
    let registered = registered.expect("subscribed with its dependencies");
    let each: Vec<_> = registered.iter().collect();
    assert_eq!(
        each.iter().map(|(d, _)| *d).collect::<Vec<_>>(),
        ["inbox://invoices", "inbox://orders"]
    );
    assert!(Arc::ptr_eq(each[0].1, &invoices) && Arc::ptr_eq(each[1].1, &orders));
}

/// Two stages that declare different dependencies for one destination are a
/// composition refused before the wire consumer is asked (seedwork ADR-0044,
/// S2b): nothing is subscribed. The same dependency twice is one.
#[test]
fn two_stages_declaring_two_dependencies_for_one_destination_subscribe_nothing() {
    let kms = Arc::new(Kms) as Arc<dyn Dependency>;
    let (subscribed, registered) = registering(vec![
        Arc::new(Opening(vec![("inbox://orders/k1", Arc::clone(&kms))])),
        Arc::new(Opening(vec![("inbox://orders/k2", Arc::new(Kms))])),
    ]);
    assert!(
        matches!(
            subscribed,
            Err(Error::Composition(Composition::Conflict { ref destination })) if destination == "inbox://orders"
        ),
        "{subscribed:?}"
    );
    assert!(registered.is_none(), "the wire consumer was asked");
    let (subscribed, registered) = registering(vec![
        Arc::new(Opening(vec![("inbox://orders/k1", Arc::clone(&kms))])),
        Arc::new(Opening(vec![("inbox://orders", kms)])),
    ]);
    subscribed.unwrap();
    assert_eq!(registered.unwrap().iter().count(), 1);
}

/// A transactional consumer goes in only through a stage that states what it
/// depends on (seedwork ADR-0044, S5a): `through` takes an `InboundStage`,
/// and a plain `Stage` does not compile there. Red at compile time when
/// `through` takes any `Stage` again.
#[test]
fn a_transactional_consumer_goes_in_only_through_a_stage_that_declares() {
    type Consumer = TransactionalConsumer<Vec<u8>, Tx>;
    let through: fn(Consumer, Arc<dyn InboundStage>) -> Consumer = Consumer::through;
    let (subscribed, _) = registering(vec![]);
    subscribed.unwrap();
    let _ = through;
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

/// Refuses every message on the way in as unavailable over the stage.
struct Down;

impl Dependent for Down {
    fn dependencies(&self) -> Dependencies {
        Dependencies::none()
    }
}

impl Stage for Down {
    fn outbound(&self, message: Message) -> BoxFuture<'_, Result<Message, BoxError>> {
        Box::pin(async move { Ok(message) })
    }

    fn inbound(&self, _: Message) -> BoxFuture<'_, Result<Message, BoxError>> {
        Box::pin(async {
            Err(Unavailable::new(cause!("kms.unavailable"), Scope::Stage, None, "down").into())
        })
    }
}

/// A stage's `Unavailable` on the way in comes out marked as the stage's, a
/// handler's as not: the inbox weighs only a stage's refusal against that
/// stage's dependency (seedwork ADR-0042 decision 1). Red when the bus marks
/// none, or every one.
#[tokio::test(flavor = "current_thread")]
async fn a_stage_s_unavailable_is_marked_the_stage_s_and_a_handler_s_is_not() {
    let told_by = |stages: bool| async move {
        let slot = Arc::new(Mutex::new(None));
        let consumer = TransactionalConsumer::new(
            Box::new(Handing(Arc::clone(&slot))),
            |message: &Message| Ok(String::from_utf8(message.payload().to_vec())?),
        );
        let consumer = if stages {
            consumer.through(Arc::new(Down))
        } else {
            consumer
        };
        let _subscription = consumer
            .subscribe(|_: Tx, _: String| async {
                Err::<(), _>(Unavailable::new(
                    cause!("crm.unavailable"),
                    Scope::Stage,
                    None,
                    "the handler's own",
                ))
            })
            .unwrap();
        let handler = slot.lock().unwrap().clone().unwrap();
        let error = handler(Tx("tx"), Message::new(b"m".to_vec()))
            .await
            .unwrap_err();
        let unavailable = error.downcast_ref::<Unavailable>().expect("an Unavailable");
        (unavailable.cause().as_str(), unavailable.told_by_stage())
    };
    assert_eq!(told_by(true).await, ("kms.unavailable", true));
    assert_eq!(told_by(false).await, ("crm.unavailable", false));
}
