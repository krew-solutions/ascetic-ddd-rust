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
use tokio::task::AbortHandle;

use crate::adapter::{Adapter, Handler, Subscription, WireConsumer, WireProducer};
use crate::error::Error;
use crate::message::Message;
use crate::uri;

// Re-exported so that a user of this crate configures the driver without
// matching versions with it independently.
pub use rdkafka;

/// A Kafka cluster, as a client configuration to build consumers and
/// producers from.
#[derive(Clone)]
pub struct KafkaBroker {
    config: ClientConfig,
    send_timeout: Duration,
}

impl KafkaBroker {
    /// A broker at `bootstrap_servers` (`host:port,host:port`).
    pub fn new(bootstrap_servers: &str) -> Self {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", bootstrap_servers);
        KafkaBroker {
            config,
            send_timeout: Duration::from_secs(30),
        }
    }

    /// A broker from a full driver configuration.
    pub fn from_config(config: ClientConfig) -> Self {
        KafkaBroker {
            config,
            send_timeout: Duration::from_secs(30),
        }
    }

    /// Returns a broker with one more driver setting.
    pub fn set(mut self, key: &str, value: &str) -> Self {
        self.config.set(key, value);
        self
    }

    /// Returns a broker whose producers give up on a send after `timeout`.
    pub fn with_send_timeout(self, timeout: Duration) -> Self {
        KafkaBroker {
            send_timeout: timeout,
            ..self
        }
    }
}

impl Adapter for KafkaBroker {
    fn consumer(&self, uri: &str, group: &str) -> Result<Box<dyn WireConsumer>, Error> {
        let topic = uri::channel(uri)?.to_owned();
        let mut config = self.config.clone();
        config
            .set("group.id", group)
            .set("enable.auto.commit", "true")
            .set("enable.auto.offset.store", "false");
        let consumer: StreamConsumer = config.create().map_err(transport)?;
        consumer.subscribe(&[&topic]).map_err(transport)?;
        Ok(Box::new(KafkaConsumer {
            consumer: Arc::new(consumer),
            uri: uri.to_owned(),
            group: group.to_owned(),
            delivery: Mutex::new(None),
        }))
    }

    fn producer(&self, uri: &str) -> Result<Box<dyn WireProducer>, Error> {
        let topic = uri::channel(uri)?.to_owned();
        let producer: FutureProducer = self.config.create().map_err(transport)?;
        Ok(Box::new(KafkaProducer {
            producer,
            topic,
            key: uri::key(uri).map(|key| key.as_bytes().to_vec()),
            timeout: self.send_timeout,
        }))
    }
}

struct KafkaConsumer {
    consumer: Arc<StreamConsumer>,
    uri: String,
    group: String,
    /// The delivery task, while a handler is attached.
    delivery: Mutex<Option<AbortHandle>>,
}

impl WireConsumer for KafkaConsumer {
    /// Starts a delivery task; a previous one is stopped first.
    fn subscribe(&self, handler: Handler) -> Result<Subscription, Error> {
        let consumer = Arc::clone(&self.consumer);
        let uri = self.uri.clone();
        let group = self.group.clone();
        let task = tokio::spawn(async move {
            loop {
                match consumer.recv().await {
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
                            match AssertUnwindSafe(handler(message.clone()))
                                .catch_unwind()
                                .await
                            {
                                Ok(Ok(())) => break,
                                Ok(Err(error)) => {
                                    log::warn!(
                                        "kafka[{uri}/{group}]: handler failed, retrying: {error}"
                                    );
                                    tokio::time::sleep(RETRY_AFTER).await;
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
        });
        let handle = task.abort_handle();
        if let Some(previous) = lock(&self.delivery).replace(handle.clone()) {
            previous.abort();
        }
        Ok(Subscription::new(move || handle.abort()))
    }
}

/// How long a consumer waits before retrying a handler that failed.
const RETRY_AFTER: Duration = Duration::from_secs(1);

struct KafkaProducer {
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
