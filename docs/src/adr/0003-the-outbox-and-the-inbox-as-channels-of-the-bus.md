# ADR-0003: The outbox and the inbox as channels of the bus, joined by bridges

## Status

Accepted (2026-09-12); proposed 2026-09-09. Depends on ADR-0002. Supersedes
the first draft, which nested reliability into URI schemes
(`outbox://kafka/…`). Implemented in `crates/bus/src/bridge.rs`,
`crates/outbox/src/channel.rs` and `crates/inbox/src/channel.rs`, with the
outbox-to-inbox path without a broker covered by `crates/inbox/tests/bridge.rs`.

## Context

ADR-0002 makes the outbox and the inbox wire-level stages that store and
relay bytes plus metadata. The open question was how a transaction reaches
them, given that the bus is also used by services without a database.

Watermill's *transactional events* example treats the database as a pub/sub
of its own: a table is a channel with the same publisher and subscriber
interfaces as Kafka, a publisher is built *with* the transaction, and a
router handler subscribes to the table's channel and publishes to Kafka's —
Hohpe's Messaging Bridge. Its `forwarder` variant carries the destination
inside the message, so one table serves every topic.

## Decision

- **The outbox and the inbox are adapters of the bus**, each with a channel
  of its own, beside Kafka and the in-memory broker. Reliability is not a
  scheme over another scheme; it is which channel a message is published to.
- **The transaction enters at the call.** Producers are built once, at
  the composition root, before any transaction exists; the transaction is
  opened later, by the command handler's decorator or by the inbox. So
  `outbox.producer(uri)` gives a `TransactionalProducer<T, S>` whose
  `publish(&S, &T)` takes the session. It is obtained from the outbox
  adapter, not from the registry, so `Bus` stays non-generic and plain
  adapters do not change; a service without a database never meets a
  session. (Watermill builds its publisher with the transaction inside the
  request handler; that fits Go's cheap constructors, not our factories.)
- **The destination travels in the message.** An outbox row keeps the
  final URI, `kafka://orders/order-1`, as the Python source does; on the
  wire it is a header, so a bridge can read it.
- **A bridge is one generic component**: a consumer of one channel and a
  producer of another, with a pass-through handler; the outbox's consumer
  group acknowledges after the producer has accepted the message, so
  delivery is at least once. The outbox dispatcher is a bridge from the
  outbox channel to the destination named in each message; the inbox intake
  is the same bridge from a Kafka channel to the inbox channel.
- **The inbox's consumer hands the handler its transaction.**
  `inbox.consumer(decode)` gives a `TransactionalConsumer<T, S>` whose
  `subscribe(Fn(S, T))` runs the handler inside the transaction that marks
  the message processed, with a clone of that session — a handle
  (ADR-0001), so the handler's future owns it and crosses the wire boundary
  boxed. It comes from the inbox adapter, as the transactional producer
  comes from the outbox's; a bus consumer of `inbox://…` is refused, since
  through it the handler's writes would fall outside the mark's
  transaction. This is where the port goes beyond Watermill, whose SQL
  subscriber acknowledges in a transaction of its own and leaves the
  handler's writes outside it.

## Refutations attempted

- The session in every `publish` call is noise for a producer that is
  transactional by nature. — It is the only moment the session exists; a
  producer bound at construction would have to be rebuilt per transaction,
  which is the same argument in a longer form.
- Two kinds of producer, `bus.producer(uri)` with `publish(&T)` and
  `outbox.producer(uri)` with `publish(&S, &T)`. — The extra argument is
  the guarantee itself, named at the call site, which is what the
  scheme-nesting draft failed to do.
- The destination header is a second source of truth beside the channel. —
  It is the only source: the outbox channel has no destination of its own,
  exactly as the forwarder's envelope.
- The consumer hands the session by value where the producer takes it by
  reference. — The producer's call has the caller's borrow at hand. The
  consumer's handler is a trait object whose future outlives the call that
  made it; a borrow would need a lifetime the object cannot name on stable
  Rust, and a clone of a handle costs reference counts.

## Consequences

- `Message` carries headers: `destination`, stamped by the outbox and read
  by the bridge and the inbox; the inbox's identity, `tenant_id`,
  `stream_type`, `stream_id`, `stream_position`; and whatever else the
  metadata holds — `message_id`, `causal_dependencies` — as text,
  structured again on the way into the inbox.
- Both channels are tasks on the runtime: the session promises `Send`
  (ADR-0004).
- `bus` gains a `Bridge`; `outbox` and `inbox` implement `Adapter` for
  their channels and depend on `bus`; `bus` stays free of database
  dependencies.
- Encryption remains a wire-level stage keyed per tenant from a header,
  placed before the outbox channel, as ADR-0002 decides.
