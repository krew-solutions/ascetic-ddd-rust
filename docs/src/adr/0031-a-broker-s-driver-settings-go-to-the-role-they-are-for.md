# ADR-0031: A broker's driver settings go to the role they are for

## Status

Proposed (2026-10-29), implemented with it. Decided in a round on the relay's
latency under bulk load in the host project that uses this library — a blind
solver and a discriminator working from measurements on its stand and from
librdkafka 2.12.1's own documentation and sources. This record holds the one
decision of that round that is this library's and small enough to stand
alone; the round's other findings for this library (a dispatcher holding no
transaction id while it sends, producers per channel, the batched relay,
batched consumption, consumer settings per subscription) wait for their own
records, each with its executor.

## Context

**What the host measured.** The host's outbox relay sent 174 messages a
second, one at a time: each `publish` awaits its delivery, and librdkafka's
`linger.ms` defaults to 5 ms, which was 98 % of the relay's time. A queue of
access snapshots, filled by a bulk provisioning of 50 000 people, held a
Tenant's suspension at the relay for 54.9 s. Set to 0, a serial send took
0.09 ms. The host needs `linger.ms=0` and a bounded `message.timeout.ms` on
its producers, and a shorter `session.timeout.ms` on its consumers, so that
a crashed process's partition returns in seconds rather than 45 s.

**What the adapter offers.** A `KafkaBroker` is one `ClientConfig`, cloned
into every consumer and every producer it creates. A setting is therefore
either everyone's or no one's.

**What the driver does with a setting of the other role.** It ignores the
setting and logs a warning at every client it creates with it
(`rdkafka_conf.c`, `CONFWARN`: "Configuration property %s is a producer
property and will be ignored by this consumer instance"). A consumer is
created once per subscription. A producer, today, is created once per
destination URI, including the URI's key — in the host, once per channel and
Tenant. So a producer setting given to everyone means a warning at every
consumer's start, and a consumer setting given to everyone means warnings
that grow with the host's Tenants. Warnings that fire on correct
configuration teach operators to ignore the driver's warnings.

## Decision

1. **A broker carries the settings of both roles and those of each.**
   `set(key, value)` is shared: the connection's settings — the brokers,
   TLS, SASL. `set_producer(key, value)` reaches producers alone, and
   `set_consumer(key, value)` reaches consumers alone. A role's setting is
   applied after the shared ones and wins over a shared setting of the same
   name.
2. **The adapter's delivery settings are applied last.** A consumer's
   `group.id`, `enable.auto.commit=true` and `enable.auto.offset.store=false`
   are what at-least-once delivery rests on (the module's contract: offsets
   are stored after the handler returns). The adapter sets them after the
   caller's, so no consumer setting of the caller's replaces them.
3. **What each role is created from is readable.** `producer_config()` and
   `consumer_config(group)` return the settings a client of that role is
   created with, so a host tests its own configuration without a broker.

## Consequences

- A host sets `linger.ms` and `message.timeout.ms` on its producers and
  `session.timeout.ms` on its consumers, and the driver warns about neither.
- `from_config` and `set` keep their meaning; a host that never uses the
  role methods sees no change.
- A setting given to the wrong role is still the caller's to avoid: the
  adapter does not know librdkafka's table of roles and does not copy it.
  Copying the table would make every driver upgrade a change to this crate.

## Rule of Three

- *A caller's consumer setting breaks delivery.* `enable.auto.offset.store`
  or `group.id` given to `set_consumer` would move the commit before the
  handler, or join another group. The adapter's own settings come last, and
  a test sets both through `set_consumer` and reads the adapter's back.
- *The same key, shared and role-specific.* The role's value wins, so a
  host can keep a shared default and override it for one role. A test sets
  `linger.ms` both ways and reads the producer's.
- *A role setting leaks to the other role.* A test reads a consumer's and a
  producer's settings and finds neither carries the other's.
- *Per-subscription settings are what the host needs.* A consumer's
  `auto.offset.reset` per subscription — the copy channel the host's round
  designed reads from its start — is not met by a broker-wide setting. That
  is a larger change to the adapter's API (`Adapter::consumer` takes no
  settings), with an executor of its own; it is left to its own record. This
  decision does not prevent it.

## Alternatives Considered

- **Two brokers under one scheme, one per role.** The `Bus` registers one
  adapter per scheme, and a bridge's producer and a subscription's consumer
  reach the adapter through the same scheme. Splitting them would change
  every destination URI the host stores in its outboxes.
- **Filter the shared settings by librdkafka's table of roles.** This
  repeats the driver's own table in this crate, and it goes stale at the
  driver's next release.
- **Lower the driver's log level.** This would silence every warning, the
  real ones with the false.

## Executors

Unit tests in `crates/bus/src/adapters/kafka.rs` (`make test`):

- A producer's settings are the shared ones with its own over them. They
  carry no consumer setting, and no `group.id`.
- A consumer's settings are the shared ones with its own over them, and the
  adapter's delivery settings last, whatever the caller set.

`crates/bus/tests/kafka.rs` (`make test-kafka`) sets `auto.offset.reset` on
its consumers alone.

## References

librdkafka 2.12.1 `CONFIGURATION.md` (the role column: `P`, `C`, `*`) and
`src/rdkafka_conf.c` (the `CONFWARN` warning). ADR-0003 (the outbox and the
inbox as channels of the bus, joined by bridges: the producer a bridge sends
through is the adapter's).

## Change Log

- 2026-10-29 — proposed, with `set_producer`, `set_consumer`,
  `producer_config` and `consumer_config`.
