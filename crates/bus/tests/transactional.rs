//! The transactional producer passes the caller's transaction through to the
//! wire producer, untouched and unknown to the bus.

use std::sync::{Arc, Mutex};

use ascetic_ddd_bus::{
    Error, Message, Subscription, TransactionalConsumer, TransactionalHandler,
    TransactionalProducer, TransactionalWireConsumer, TransactionalWireProducer,
};
use futures::future::BoxFuture;

/// Stands in for a session: the bus only passes it through.
struct Tx(&'static str);

type Seen = Arc<Mutex<Vec<(&'static str, Vec<u8>)>>>;

/// Records what it was asked to publish, and in which transaction.
struct Recording(Seen);

impl TransactionalWireProducer<Tx> for Recording {
    fn publish<'a>(
        &'a self,
        session: &'a Tx,
        message: Message,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.0
                .lock()
                .unwrap()
                .push((session.0, message.into_payload()));
            Ok(())
        })
    }
}

/// Keeps the handler it is given, so the test can call it with transactions
/// of its own.
struct Handing(Arc<Mutex<Option<TransactionalHandler<Tx>>>>);

impl TransactionalWireConsumer<Tx> for Handing {
    fn subscribe(&self, handler: TransactionalHandler<Tx>) -> Result<Subscription, Error> {
        *self.0.lock().unwrap() = Some(handler);
        Ok(Subscription::new(|| {}))
    }
}

#[tokio::test]
async fn the_handler_gets_the_transaction_with_the_decoded_value() {
    let slot = Arc::new(Mutex::new(None));
    let consumer =
        TransactionalConsumer::new(Box::new(Handing(Arc::clone(&slot))), |message: &Message| {
            Ok(std::str::from_utf8(message.payload())?.parse::<u32>()?)
        });
    let seen: Arc<Mutex<Vec<(&'static str, u32)>>> = Arc::default();
    let recorded = Arc::clone(&seen);
    consumer
        .subscribe(move |tx: Tx, order: u32| {
            let recorded = Arc::clone(&recorded);
            async move {
                recorded.lock().unwrap().push((tx.0, order));
                Ok::<(), Error>(())
            }
        })
        .unwrap();
    let handler = slot.lock().unwrap().clone().unwrap();

    handler(Tx("tx-1"), Message::new("7")).await.unwrap();
    // undecodable: reported and acknowledged, not an error of the transport
    handler(Tx("tx-2"), Message::new("not a number"))
        .await
        .unwrap();
    handler(Tx("tx-3"), Message::new("8")).await.unwrap();

    assert_eq!(*seen.lock().unwrap(), [("tx-1", 7), ("tx-3", 8)]);
}

#[tokio::test]
async fn the_value_is_encoded_and_published_in_the_given_transaction() {
    let seen: Seen = Arc::default();
    let producer =
        TransactionalProducer::new(Box::new(Recording(Arc::clone(&seen))), |order: &u32| {
            Message::new(order.to_string())
        });

    producer.publish(&Tx("tx-1"), &7).await.unwrap();
    producer.publish(&Tx("tx-2"), &8).await.unwrap();

    assert_eq!(
        *seen.lock().unwrap(),
        [("tx-1", b"7".to_vec()), ("tx-2", b"8".to_vec())]
    );
}
