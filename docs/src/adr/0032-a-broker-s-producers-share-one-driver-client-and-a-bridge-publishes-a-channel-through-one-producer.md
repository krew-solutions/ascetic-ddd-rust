# ADR-0032: A broker's producers share one driver client, and a bridge publishes a channel through one producer

## Status

Proposed (2026-10-31), implemented with it. Decided in the round on the
relay's latency under bulk load in the host project that uses this library
(the blind solver and the discriminator of ADR-0031), which named it a
production blocker independent of load: "producers per channel, a client
per bridge, never one per Tenant". Measured again on the host's stand on
2026-10-30, the day before this record.

## Context

**What a host's outbox sends.** A message is published to a destination
URI, `scheme://channel[/key]`; a keyed message written through the outbox
is stored under its channel and key (`kafka://rbac.assignment/<tenant>`),
because the outbox channel appends the message's key to the destination it
was built for (`crates/outbox/src/channel.rs`). The host keys every
Integration Event by its Tenant, so that one Tenant's events keep their
order in one partition: its outbox holds one destination per channel and
Tenant.

**What the dispatcher did with them.** The dispatcher is a
[`Bridge`](../../../crates/bus/src/bridge.rs) from the outbox to the
destination each row names. It kept one producer per destination URI, the
key included, made on first use and never released. The Kafka adapter made
each producer a driver client of its own (`FutureProducer` from the
broker's settings): librdkafka's main thread, a thread per broker of the
cluster and one for the bootstrap address, the binding's polling thread —
and a connection to every broker.

**What that cost.** The host's tenancy service, sixteen Tenants after its
database was reset: 114 clients, 456 threads; each polling thread wakes ten
times a second, about 1 % of a CPU, and the idle service ran at 108 %. The
round counted 74 clients and 296 threads at fewer Tenants. The count is
channels × Tenants per process, never released: 10 000 Tenants on nine
channels would be 90 000 clients, their threads, and their connections to
each broker — the brokers' connection limits fall before the CPU does.

**What the driver allows.** A librdkafka client sends to any topic under
any key; both are given with each record (`FutureRecord::to(topic)`,
`.key(…)`), and a `FutureProducer` is cheaply cloned onto the same client.
Nothing in the order of a partition depends on which client object sent a
record: the order is the order the client enqueues records of one
partition. A message read from the outbox already carries its key — the
outbox puts the row's key on the wire message it hands the dispatcher
(`wire_of`).

## Decision

1. **A broker's producers share one driver client.** `KafkaBroker` makes
   its client with its first producer, from `producer_config()`, and every
   later producer — any topic, any key — sends through it. A clone of the
   broker shares the client; a broker given another setting (`set`,
   `set_producer`, `set_consumer`, `with_send_timeout`) is another broker,
   with a client of its own made from its settings. A host that wants lanes
   isolated from each other — a queue, a connection, a fatal state not
   shared — gives each lane a broker of its own, on a bus of its own.
2. **A bridge publishes a channel through one producer.** The bridge keeps
   its producers by channel — the destination without its key
   (`uri::without_key`) — and publishes each message under its own key; a
   message without one is given the destination's, so a destination whose
   key only its URI carries is published as before.

## Consequences

- A host's producers are one client per broker it registers, whatever its
  Tenants and channels: the host's tenancy service holds four producer
  threads instead of four per channel and Tenant.
- The producers of one broker share the driver's queue
  (`queue.buffering.max.messages`, `queue.buffering.max.kbytes`) and its
  connections. A partition that does not accept holds its own records until
  `message.timeout.ms`; the others are sent meanwhile — librdkafka queues
  per partition.
- The bridge's map grows with the channels, not with the keys.
- No signature changes. `Adapter::producer(uri)` still returns a producer
  bound to a URI; with Kafka it is a topic, a default key and a handle on
  the broker's client.

## Rule of Three

- *A stuck topic blocks the others through the shared queue.* The queue is
  per partition inside one client, and a record that cannot be delivered
  fails after `message.timeout.ms` (the host sets 10 s, ADR-0031): it does
  not hold the others' partitions. What a stuck partition does hold is a
  dispatcher that awaits each send — that is the dispatcher's batching,
  the round's next record, and it was the same with a client per
  destination.
- *A fatal producer state reaches every producer of the broker.* An
  idempotent producer can enter fatal states
  (`OUT_OF_ORDER_SEQUENCE_NUMBER`, `UNKNOWN_PRODUCER_ID`) after which it
  refuses everything. The host does not enable idempotence; the batched
  relay that will, rebuilds the client on a fatal error — its own record.
  Until then a fatal state is no more shared than a connection is: a broker
  per lane isolates lanes (decision 1).
- *A reconfigured broker sends through a client made for other settings.*
  A broker's client is made from its settings at the first producer; a
  setting given after would not reach it. Every setting method therefore
  returns a broker with an empty client. Tests make a client, change each
  kind of setting on a clone, and find a client of its own each time.
- *The key is lost between the outbox and the broker.* The bridge now asks
  for a producer of the channel, whose URI carries no key. The outbox puts
  the key on the message; a source that does not, has the destination's put
  there by the bridge, and a message's own key stands over the
  destination's. Tests send fifty keys of one channel and each arrives
  under its own; a message with a key of its own keeps it.

## Alternatives Considered

- **One client per bridge (the round's wording).** The bridge cannot reach
  the driver: it asks the bus for producers by URI, and the adapter
  behind the scheme makes them. A client per bridge needs a new adapter
  method — a sender to any destination — and every adapter implementing
  it. A client per broker reaches the same isolation by registration: the
  host's lanes are bridges over buses of their own, each with its broker.
- **Only the bridge's map by channel.** It bounds the clients by channels
  (a dozen in the host's tenancy service: forty-eight threads, a dozen
  polling threads) but not by brokers: every producer a host makes
  directly, through `Bus::producer`, still had a client of its own.
- **A client shared by every broker of the process.** Settings differ per
  broker; a process-wide client would make one broker's settings every
  broker's.
- **Release idle producers (a cache with eviction).** It bounds the count
  by recent destinations, not by channels, and pays a client's creation —
  a connection to every broker — on the first message of a returning key.

## Executors

Unit tests in `crates/bus/src/adapters/kafka.rs` (`make test`):

- The producers of one broker — other topics, other keys — send through
  the client its first producer made; a clone of the broker shares it.
- A broker given another setting of each kind has a client of its own.

`crates/bus/tests/bridge.rs` (`make test`):

- Fifty keys of one channel go through one producer, each message under its
  own key, in order.
- A message's own key stands over the destination's.

`crates/bus/tests/kafka.rs` (`make test-kafka`): producers of one broker to
two topics under many keys deliver every message to its topic under its key.

The host's stand (its own record): the producer threads of its tenancy
service do not grow with its Tenants.

## References

librdkafka 2.12.1 `INTRODUCTION.md` (one client per process is the
recommended use; the queue and the partitions), rust-rdkafka 0.39
`FutureProducer` (cheaply cloned onto the same client, its polling
thread). ADR-0003 (the outbox and the inbox as channels of the bus, joined
by bridges). ADR-0031 (a broker's settings by role).

## Change Log

- 2026-10-31 — proposed: the producers of a broker share its client; a
  bridge's producers by channel, a message's key its own or the
  destination's.
