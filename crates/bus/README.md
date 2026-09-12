# ascetic-ddd-bus

Scheme-dispatched message bus: typed producers and consumers over opaque wire
messages, with an in-memory adapter and a Kafka one. A port of `trading.bus`
(OCaml), adapted to Rust rather than transliterated.

```rust
let mut bus = Bus::new();
bus.register("in-memory", InMemoryBroker::new())?;   // or KafkaBroker::new("localhost:9092")

let orders = bus.consumer("in-memory://orders", "billing", decode_order)?;
let _subscription = orders.subscribe(|order: Order| async move { bill(order).await })?;

let producer = bus.producer("in-memory://orders", encode_order)?;
producer.publish(&order).await?;
```

## Design

The bus is a registry of transports keyed by URI scheme; a call site names
the scheme, never the transport, so replacing Kafka with the in-memory broker
is one `register` line at the composition root. The bus carries opaque wire
messages: each producer encodes with a function of its own and each consumer
decodes with one of its own, so two consumers of one topic may read the same
bytes as different types. The wire format is the contract; the types are local
to each side.

Two decisions taken in the port:

* **Push, not pull.** A consumer runs an asynchronous handler for every
  message, as the OCaml bus runs a callback, because that is how the sixty
  subscriptions of the source project are written. A `Stream` would be more
  idiomatic and would have inverted every composition root.
* **A key on the wire.** A `Message` carries a payload and an optional key.
  The OCaml producer serializes to a string; a transport that partitions
  needs a key to keep an aggregate's messages in order, and the key belongs
  to the message, not to the URI, because a producer serves many aggregates.

A subscription is cancelled explicitly, never by dropping its handle: the
composition root discards most handles. Errors are values.

A producer or consumer that is transactional by nature — the outbox, the
inbox — is obtained from its adapter rather than from the registry, and
names the transaction at the call: `TransactionalProducer::publish(&session,
&value)` publishes inside the caller's transaction,
`TransactionalConsumer::subscribe(|session, value| …)` runs the handler
inside the transaction that acknowledges the message. The bus never sees a
session; it passes one through (ADR-0003).

## Adapters

`InMemoryBroker` is the monolithic transport: topics in a process-local
registry, a delivery task per topic, one consumer per `(uri, group)`, ordered
delivery, a panicking handler loses its message and not the topic.

`KafkaBroker`, behind the `kafka` feature, is the same surface over
`rdkafka`. A consumer group of the same name shares the partitions; delivery
is at least once — offsets are stored after the handler returns — and a
message that cannot be decoded or whose handler panics is skipped, because a
poison message must not stop the partition. TLS, SASL and tuning come from
the `ClientConfig` the broker is built with.

```toml
ascetic-ddd-bus = { version = "0.1", features = ["kafka"] }
```

## Testing

```bash
cargo test -p ascetic-ddd-bus

# the Kafka tests need a live broker and are ignored by default
ASCETIC_DDD_TEST_KAFKA_BROKERS=localhost:9092 \
    cargo test -p ascetic-ddd-bus --features kafka -- --ignored
```
