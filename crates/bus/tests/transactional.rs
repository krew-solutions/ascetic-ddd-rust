//! The transactional producer passes the caller's transaction through to the
//! wire producer, untouched and unknown to the bus.

use std::sync::{Arc, Mutex};

use ascetic_ddd_bus::{Error, Message, TransactionalProducer, TransactionalWireProducer};
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
