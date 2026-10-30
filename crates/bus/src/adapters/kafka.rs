//! Kafka transport, on `rdkafka` (librdkafka). Available under the `kafka`
//! feature.
//!
//! A URI `kafka://orders` names the topic `orders`. A consumer in `group` is
//! a member of the Kafka consumer group of that name, so several processes
//! in one group share the topic's partitions; the in-memory adapter's
//! one-consumer-per-group rule does not apply here.
//!
//! Delivery is at least once. Offsets are stored after the handler returns
//! and committed by the driver in the background (`enable.auto.commit` on,
//! `enable.auto.offset.store` off), so a crash mid-handler redelivers the
//! message on restart. A handler that panics is reported and its message is
//! skipped, as is a message the consumer cannot decode: a poison message must
//! not stop the partition.
//!
//! Every other driver setting comes from the [`ClientConfig`] the broker was
//! built with, so TLS, SASL and tuning are the caller's, not this crate's.
//! A setting of one role — a producer's `linger.ms`, a consumer's
//! `session.timeout.ms` — goes to that role alone ([`KafkaBroker::set_producer`],
//! [`KafkaBroker::set_consumer`]): the driver ignores a setting of the other
//! role and warns at every client it creates with one (ADR-0031).
//!
//! A broker's producers share one driver client, made with the first of
//! them (ADR-0032): a client sends to any topic under any key, the topic and
//! the key given with each message. A client per producer would be a client
//! per destination — per channel and key — each with its threads and its
//! connections to every broker of the cluster.

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::FutureExt;
use futures::future::BoxFuture;
use rdkafka::Message as _;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer as _, StreamConsumer};
use rdkafka::message::{Header, Headers as _, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use tokio::runtime::Handle;
use tokio::sync::Notify;

use crate::adapter::{Adapter, Handler, Subscription, WireConsumer, WireProducer};
use crate::error::Error;
use crate::handling;
use crate::message::Message;
use crate::uri;

// Re-exported so that a user of this crate configures the driver without
// matching versions with it independently.
pub use rdkafka;

/// A Kafka cluster, as a client configuration to build consumers and
/// producers from: the settings of both, then each role's own. Its
/// producers send through one driver client; a clone shares it, and a
/// broker given another setting is another broker, with a client of its own.
#[derive(Clone)]
pub struct KafkaBroker {
    config: ClientConfig,
    producer: BTreeMap<String, String>,
    consumer: BTreeMap<String, String>,
    send_timeout: Duration,
    /// The driver client the producers share, made with the first of them.
    client: Arc<Mutex<Option<FutureProducer>>>,
}

impl KafkaBroker {
    /// A broker at `bootstrap_servers` (`host:port,host:port`).
    pub fn new(bootstrap_servers: &str) -> Self {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", bootstrap_servers);
        Self::from_config(config)
    }

    /// A broker from a full driver configuration, shared by its producers
    /// and its consumers.
    pub fn from_config(config: ClientConfig) -> Self {
        KafkaBroker {
            config,
            producer: BTreeMap::new(),
            consumer: BTreeMap::new(),
            send_timeout: Duration::from_secs(30),
            client: Arc::default(),
        }
    }

    /// Returns a broker with one more driver setting, for its producers and
    /// its consumers alike: the connection's — the brokers, TLS, SASL.
    pub fn set(mut self, key: &str, value: &str) -> Self {
        self.config.set(key, value);
        self.client = Arc::default();
        self
    }

    /// Returns a broker with one more setting of its producers alone, over
    /// a shared one of the same name.
    pub fn set_producer(mut self, key: &str, value: &str) -> Self {
        self.producer.insert(key.to_owned(), value.to_owned());
        self.client = Arc::default();
        self
    }

    /// Returns a broker with one more setting of its consumers alone, over
    /// a shared one of the same name. The settings the adapter's delivery
    /// rests on — the group, the offsets stored after the handler — stay
    /// the adapter's.
    pub fn set_consumer(mut self, key: &str, value: &str) -> Self {
        self.consumer.insert(key.to_owned(), value.to_owned());
        self.client = Arc::default();
        self
    }

    /// What a producer is created from: the shared settings, then the
    /// producers' own.
    pub fn producer_config(&self) -> ClientConfig {
        let mut config = self.config.clone();
        for (key, value) in &self.producer {
            config.set(key, value);
        }
        config
    }

    /// What a consumer in `group` is created from: the shared settings,
    /// then the consumers' own, then the adapter's — last, so no setting
    /// of the caller's breaks delivery at least once.
    pub fn consumer_config(&self, group: &str) -> ClientConfig {
        let mut config = self.config.clone();
        for (key, value) in &self.consumer {
            config.set(key, value);
        }
        config
            .set("group.id", group)
            .set("enable.auto.commit", "true")
            .set("enable.auto.offset.store", "false");
        config
    }

    /// Returns a broker whose producers give up on a send after `timeout`.
    pub fn with_send_timeout(self, timeout: Duration) -> Self {
        KafkaBroker {
            send_timeout: timeout,
            client: Arc::default(),
            ..self
        }
    }

    /// The driver client this broker's producers send through: made from
    /// [`producer_config`](Self::producer_config) with the first of them,
    /// the same client for every one after.
    fn client(&self) -> Result<FutureProducer, Error> {
        let mut client = lock(&self.client);
        if let Some(made) = client.as_ref() {
            return Ok(made.clone());
        }
        let made: FutureProducer = self.producer_config().create().map_err(transport)?;
        *client = Some(made.clone());
        Ok(made)
    }
}

impl Adapter for KafkaBroker {
    fn consumer(&self, uri: &str, group: &str) -> Result<Box<dyn WireConsumer>, Error> {
        let topic = uri::channel(uri)?.to_owned();
        let consumer: StreamConsumer = self.consumer_config(group).create().map_err(transport)?;
        consumer.subscribe(&[&topic]).map_err(transport)?;
        Ok(Box::new(KafkaConsumer {
            consumer: Arc::new(consumer),
            uri: uri.to_owned(),
            group: group.to_owned(),
            delivery: Mutex::new(None),
        }))
    }

    fn producer(&self, uri: &str) -> Result<Box<dyn WireProducer>, Error> {
        Ok(Box::new(self.kafka_producer(uri)?))
    }
}

impl KafkaBroker {
    /// A producer to `uri`'s topic, its key the URI's, sending through the
    /// broker's client.
    fn kafka_producer(&self, uri: &str) -> Result<KafkaProducer, Error> {
        Ok(KafkaProducer {
            producer: self.client()?,
            topic: uri::channel(uri)?.to_owned(),
            key: uri::key(uri).map(|key| key.as_bytes().to_vec()),
            timeout: self.send_timeout,
        })
    }
}

struct KafkaConsumer {
    consumer: Arc<StreamConsumer>,
    uri: String,
    group: String,
    /// The order to stop the delivery task, while a handler is attached.
    delivery: Mutex<Option<Arc<Notify>>>,
}

impl WireConsumer for KafkaConsumer {
    /// Starts a delivery task; a previous one is told to stop first. The
    /// task stops between messages, never inside the handler: the receive
    /// races with the order to stop, and so does the pause between two
    /// tries of a handler that fails.
    fn subscribe(&self, handler: Handler) -> Result<Subscription, Error> {
        let consumer = Arc::clone(&self.consumer);
        let uri = self.uri.clone();
        let group = self.group.clone();
        let stop = Arc::new(Notify::new());
        let stopped = Arc::clone(&stop);
        let task = async move {
            'messages: loop {
                let received = tokio::select! {
                    _ = stopped.notified() => break 'messages,
                    received = consumer.recv() => received,
                };
                match received {
                    Ok(received) => {
                        let message = Message::new(received.payload().unwrap_or_default());
                        let message = match received.key() {
                            Some(key) => message.with_key(key),
                            None => message,
                        };
                        let message = received
                            .headers()
                            .map(|headers| headers.iter())
                            .into_iter()
                            .flatten()
                            .fold(message, |message, header| {
                                message.with_header(header.key, header.value.unwrap_or_default())
                            });
                        // A handler that fails is retried until it succeeds: the
                        // partition waits, which is what keeps its order.
                        loop {
                            match AssertUnwindSafe(handling::within(handler(message.clone())))
                                .catch_unwind()
                                .await
                            {
                                Ok(Ok(())) => break,
                                Ok(Err(error)) => {
                                    log::warn!(
                                        "kafka[{uri}/{group}]: handler failed, retrying: {error}"
                                    );
                                    tokio::select! {
                                        _ = stopped.notified() => break 'messages,
                                        _ = tokio::time::sleep(RETRY_AFTER) => {}
                                    }
                                }
                                Err(_) => {
                                    log::warn!("kafka[{uri}/{group}]: handler panicked, skipping");
                                    break;
                                }
                            }
                        }
                        if let Err(error) = consumer.store_offset_from_message(&received) {
                            log::warn!("kafka[{uri}/{group}]: storing the offset failed: {error}");
                        }
                    }
                    Err(error) => log::warn!("kafka[{uri}/{group}]: receiving failed: {error}"),
                }
            }
        };
        let runtime = Handle::try_current().map_err(|error| Error::Transport(Box::new(error)))?;
        if let Some(previous) = lock(&self.delivery).replace(Arc::clone(&stop)) {
            previous.notify_one();
        }
        Ok(handling::serve(&runtime, stop, task))
    }
}

/// How long a consumer waits before retrying a handler that failed.
const RETRY_AFTER: Duration = Duration::from_secs(1);

struct KafkaProducer {
    /// The broker's client, shared with its other producers.
    producer: FutureProducer,
    topic: String,
    /// The key the producer's URI carries; a message without one gets it.
    key: Option<Vec<u8>>,
    timeout: Duration,
}

impl WireProducer for KafkaProducer {
    fn publish(&self, message: Message) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let record = FutureRecord::<[u8], [u8]>::to(&self.topic).payload(message.payload());
            let record = match message.key().or(self.key.as_deref()) {
                Some(key) => record.key(key),
                None => record,
            };
            let headers =
                message
                    .headers()
                    .iter()
                    .fold(OwnedHeaders::new(), |headers, (name, value)| {
                        headers.insert(Header {
                            key: name,
                            value: Some(value.as_slice()),
                        })
                    });
            let record = record.headers(headers);
            self.producer
                .send(record, Timeout::After(self.timeout))
                .await
                .map(|_| ())
                .map_err(|(error, _)| transport(error))
        })
    }
}

fn transport(error: rdkafka::error::KafkaError) -> Error {
    Error::Transport(Box::new(error))
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use rdkafka::producer::Producer as _;

    use super::*;

    fn broker() -> KafkaBroker {
        KafkaBroker::new("localhost:9092")
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("linger.ms", "5")
            .set_producer("linger.ms", "0")
            .set_producer("message.timeout.ms", "10000")
            .set_consumer("session.timeout.ms", "10000")
            .set_consumer("enable.auto.offset.store", "true")
            .set_consumer("group.id", "not-the-callers")
    }

    #[test]
    fn a_producer_has_the_shared_settings_and_its_own_over_them() {
        let producer = broker().producer_config();
        assert_eq!(producer.get("bootstrap.servers"), Some("localhost:9092"));
        assert_eq!(producer.get("security.protocol"), Some("SASL_PLAINTEXT"));
        assert_eq!(producer.get("linger.ms"), Some("0"));
        assert_eq!(producer.get("message.timeout.ms"), Some("10000"));
        assert_eq!(producer.get("session.timeout.ms"), None);
        assert_eq!(producer.get("group.id"), None);
    }

    /// The client of a broker the tests make producers of: no broker
    /// answers, the client is made all the same.
    fn reachable_nowhere() -> KafkaBroker {
        KafkaBroker::new("localhost:1").set_producer("linger.ms", "0")
    }

    fn native(producer: &FutureProducer) -> usize {
        producer.client().native_ptr() as usize
    }

    #[test]
    fn the_producers_of_one_broker_share_one_driver_client() {
        let broker = reachable_nowhere();
        let orders = broker.kafka_producer("kafka://orders/order-7").unwrap();
        // Another topic, another key: the same client.
        let payments = broker.kafka_producer("kafka://payments/payment-9").unwrap();
        assert_eq!(native(&payments.producer), native(&orders.producer));
        assert_eq!(payments.topic, "payments");
        assert_eq!(payments.key.as_deref(), Some(&b"payment-9"[..]));
        // A clone of the broker shares its client.
        let cloned = broker.clone().kafka_producer("kafka://orders").unwrap();
        assert_eq!(native(&cloned.producer), native(&orders.producer));
    }

    #[test]
    fn a_broker_given_another_setting_has_a_client_of_its_own() {
        let broker = reachable_nowhere();
        let first = broker.kafka_producer("kafka://orders").unwrap();
        for other in [
            broker.clone().set("client.id", "another"),
            broker.clone().set_producer("linger.ms", "1"),
            broker.clone().set_consumer("session.timeout.ms", "10000"),
            broker.clone().with_send_timeout(Duration::from_secs(5)),
        ] {
            let its_own = other.kafka_producer("kafka://orders").unwrap();
            assert_ne!(native(&its_own.producer), native(&first.producer));
        }
    }

    #[test]
    fn a_consumer_has_the_shared_settings_its_own_and_the_adapters_last() {
        let consumer = broker().consumer_config("orders");
        assert_eq!(consumer.get("security.protocol"), Some("SASL_PLAINTEXT"));
        assert_eq!(consumer.get("session.timeout.ms"), Some("10000"));
        assert_eq!(consumer.get("message.timeout.ms"), None);
        // A shared setting of the other role reaches it, as the caller set it.
        assert_eq!(consumer.get("linger.ms"), Some("5"));
        // Delivery at least once is the adapter's, whatever the caller set.
        assert_eq!(consumer.get("group.id"), Some("orders"));
        assert_eq!(consumer.get("enable.auto.offset.store"), Some("false"));
        assert_eq!(consumer.get("enable.auto.commit"), Some("true"));
    }
}
