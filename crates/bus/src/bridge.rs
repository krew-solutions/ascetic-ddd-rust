//! A Messaging Bridge: what arrives on one channel is published on another.
//!
//! The bridge subscribes to a channel and, for each message, publishes it —
//! bytes, key and headers untouched — to a target: one fixed URI, or the URI
//! a header names, so that one channel may feed many. It acknowledges a
//! message only after the target accepted it: a failed publish is an error
//! of the handler, and the source keeps the message for another try.
//!
//! An outbox dispatcher is a bridge from the outbox channel to the
//! destination each message names; an inbox intake is a bridge from a broker
//! channel to the inbox channel.
//!
//! A destination may carry a key (`kafka://orders/order-7`); the bridge
//! publishes through one producer per channel, whatever the keys, and a
//! message without a key of its own gets the destination's (ADR-0032).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::adapter::Subscription;
use crate::error::{BoxError, Error};
use crate::message::Message;
use crate::uri;
use crate::{Bus, Producer};

/// Where a bridge publishes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Target {
    /// Every message goes to this URI.
    Fixed(String),
    /// Each message goes to the URI in the header of this name.
    Header(String),
}

/// A bridge over a bus.
pub struct Bridge {
    bus: Arc<Bus>,
    producers: Mutex<HashMap<String, Arc<Producer<Message>>>>,
}

impl Bridge {
    /// A bridge that publishes through `bus`.
    pub fn new(bus: Arc<Bus>) -> Arc<Self> {
        Arc::new(Bridge {
            bus,
            producers: Mutex::new(HashMap::new()),
        })
    }

    /// Starts moving messages from `from`, as consumer group `group`, to
    /// `target`. Runs until the subscription is cancelled.
    pub fn run(
        self: &Arc<Self>,
        from: &str,
        group: &str,
        target: Target,
    ) -> Result<Subscription, Error> {
        let consumer = self
            .bus
            .consumer(from, group, |message: &Message| Ok(message.clone()))?;
        let bridge = Arc::clone(self);
        consumer.subscribe(move |message: Message| {
            let bridge = Arc::clone(&bridge);
            let target = target.clone();
            async move { bridge.forward(&message, &target).await }
        })
    }

    async fn forward(&self, message: &Message, target: &Target) -> Result<(), BoxError> {
        let uri = match target {
            Target::Fixed(uri) => uri.clone(),
            Target::Header(name) => {
                let value = message
                    .header(name)
                    .ok_or_else(|| format!("no `{name}` header names a destination"))?;
                String::from_utf8(value.to_vec())?
            }
        };
        let producer = self.producer(uri::without_key(&uri))?;
        match (message.key(), uri::key(&uri)) {
            (None, Some(key)) => producer.publish(&message.clone().with_key(key)).await?,
            _ => producer.publish(message).await?,
        }
        Ok(())
    }

    /// One producer per channel, made on first use: the key is the
    /// message's, so the producers do not grow with the keys.
    fn producer(&self, channel: &str) -> Result<Arc<Producer<Message>>, Error> {
        let mut producers = self
            .producers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(producer) = producers.get(channel) {
            return Ok(Arc::clone(producer));
        }
        let producer = Arc::new(
            self.bus
                .producer(channel, |message: &Message| message.clone())?,
        );
        producers.insert(channel.to_owned(), Arc::clone(&producer));
        Ok(producer)
    }
}
