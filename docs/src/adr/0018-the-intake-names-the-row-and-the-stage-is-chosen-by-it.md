# ADR-0018: The intake names the row, and at an inbox that receives several channels the stage is chosen by the row

## Status

Accepted (2026-10-14, the owner's decision): held by `tests/bridge.rs` of
`ascetic-ddd-inbox` (a message from a broker carrying a producer's
`destination` lands under the intake's uri; the relay out of an outbox
still lands under the destination the outbox stamped) and `tests/stage.rs`
of `ascetic-ddd-dek` (one stage opens the rows of two channels by their
destination, the key left aside; a row under another channel's destination
does not open and is refused for good; a destination outside the map, or
none, is refused before any KMS is asked; a map naming a channel the
sealing lacks is refused at construction).

The consumer's decision this serves is the platform's plan to take
BC-DEVICE's events into BC-POLICY's inbox beside its own Commands — the
first inbox on this seedwork to receive more than one channel, and the
first to receive a channel sealed by another producer (its ADR-0071).

## Context

Three decisions met at that inbox, two of this seedwork's and one of the
platform's, and what each had left unsaid became visible only together.

**The inbox has one subscriber for every row.** The Python source first
had `@inbox.subscribe("kafka://orders")`, a route by address inside the
inbox. It was dropped to give the inbox the outbox's shape,
`dispatch(subscriber)`, with routing outside; then a filter by address in
`dispatch` was considered and rejected, for a reason worth more than the
convenience it refused: causal dependencies cross addresses. A message on
`kafka://shipments` may depend on one on `kafka://orders`; a worker that
saw `shipments` alone would wait for ever on a dependency it never
processes. This port kept the decision — `PgInbox::consumer(decode)` takes
no address, the dispatcher takes a slot and hands over whatever it holds,
the row's `uri` is a column and not a filter — and kept the reason
nowhere: `crates/inbox/README.md` and the first consumer's own docs said
"one consumer per inbox" without saying why, which is how the next person
would have added the filter and broken the wait of ADR-0006, resumed as it
is by the transaction that marks the dependency, which needs one loop over
every row.

**The stage is the channel's.** ADR-0017 decision 2: one `EnvelopeStage`
per channel, over a KMS instance serving the channel's address as the
derivation context, at the producer's end and at the consumer's; a data
key drawn for one channel does not unwrap under another, by design, and a
policy at the KMS pins the context a caller may pass. A consumer applies
its stage with `through(stage)`: one stage per consumer, before `decode`,
before anything of the subscriber's runs. For an inbox of one channel that
is one stage. For an inbox of three — the context's own Commands under
`inbox://policy-commands`, BC-DEVICE's events under `device.lifecycle`,
telemetry's under `telemetry.semantic` — it is a stage that must be chosen
per row, and nothing chose it: the Commands' stage would meet a
BC-DEVICE row, fail to unwrap, and park it as `Permanent`, every one.

**The row was named by the sender.** ADR-0003: "the destination travels in
the message" — an outbox row keeps its final uri, the wire carries it as a
`destination` header, a bridge reads it, and the inbox intake took it as
the row's `uri` whenever it was present, falling back to the channel the
intake's producer was built for. That was written for the relay out of an
outbox, where the bridge's target *is* the header, so the producer is
built for it and the two never differ. The Kafka adapter forwards headers
both ways. So a producer on this seedwork stamps `kafka://orders/<key>` on
what it publishes to a broker, the receiver's intake takes that as the
row's name, and a producer on another stack, stamping nothing, leaves the
row under `inbox://orders/<key>`: one channel, two names for its rows, and
which one a row gets is the sender's doing. A stage chosen by the row's
name cannot be built on a name the sender chooses.

## Decision

1. **The intake names the row.** A row's `uri` is the inbox channel the
   intake published it to — the channel the producer was built for — with
   the message's key, or the channel's own key when the message has none;
   a `destination` header on an incoming message is the producer's address
   and is not kept. For the relay out of an outbox nothing changes: the
   bridge's target is the header, the producer is built for it, the row's
   name is the same text. ADR-0003 is refined, not reversed: the header is
   the only source *for the relay*; at the intake the receiver's channel is
   the source, and the header the row goes out with (`wire_of`) is the
   receiver's name, as before.
2. **At an inbox that receives several channels, the stage is chosen by
   the row.** `Sealing::by_destination(map)` in `ascetic-ddd-dek`: for each
   `(destination, channel)`, a row whose `destination`, key left aside, is
   the first opens with the stage of the second — the channel it was
   sealed under at the producer's end. Built up front, as the sealing is: a
   channel the sealing was not built for is refused at construction, so a
   composition root does not start with a destination routed to nothing. A
   row without the header, or with a destination the stage was not built
   for, is refused for good before any stage, and so before any KMS, is
   asked: nothing is opened under a guessed context. `outbound` delegates
   the same way, so the type is a stage in full. The convention for the
   intake's bridges: `Target::Fixed("inbox://<the producer's channel
   address>")`, one destination per channel received.
3. **The inbox keeps one subscriber and no filter by address, and says
   why.** The reason of the Python source is written into
   `crates/inbox/README.md` and the channel's docs: a consumer that saw one
   address would wait for ever on a causal dependency published under
   another. Routing is the subscriber's, by the row's `destination` and
   `stream_type`.

## Consequences

- A row's name is the receiver's and deterministic per channel, whatever
  the sender's stack; the intake's bridges and the stage's map are written
  from one list, the channels the receiver's contracts say it receives.
- A producer's `destination` header is lost at the intake. Nothing read it:
  the relay's header equals the channel, and a receiver that wants a
  producer's address has the channel it bridged from.
- One header read and one map lookup per row before the stage; a row the
  map does not know costs no KMS call.
- A consumer composes its intake as one subscriber over one
  `ByDestination`, and branches by `stream_type` after the row is open —
  own Commands to their channel, foreign events to their anti-corruption
  handlers.
- `crates/inbox/tests/bridge.rs` gains the foreign-destination test;
  `crates/dek/tests/stage.rs` the four of the stage.

What can go wrong, and what holds it:

- *Two channels bridged to one destination.* Their rows would share a name
  and one stage, and one of them would never open. Held by the convention
  of decision 2 — the destination is the producer's channel address under
  `inbox://` — and by the consumer's generated access declaration, which
  names each received channel once.
- *A row's key is not the message's.* The intake takes the message's key
  first, the channel's second, exactly as before; the stage leaves the key
  aside, so a key of either origin maps alike.
- *A producer relied on the receiver routing by its header.* No producer
  may: the receiver's route is its bridge's target. A bridge with
  `Target::Header` into an inbox still builds the producer for that header,
  so even that row carries it.

## Alternatives rejected

**A filter by address on the inbox's consumer.** The Python source's
rejection stands: cross-address causal dependencies stall, and the wait of
ADR-0006 is resumed by one loop. Two loops over one table would also take
two slot locks for one slot.

**A map with both names per channel, `kafka://…` and `inbox://…`.** Makes
the receiver's routing depend on the producer's stack, and silently so: a
producer that starts stamping headers renames every row it sends.

**Stripping the scheme and guessing.** `inbox://policy-commands` is a
catalogue address with its scheme; `device.lifecycle` is one without. A
rule that strips would have to know which is which — that knowledge is
the map.

**One inbox per channel.** A table, a loop, a slot lock and a bridge per
channel, and a causal dependency across channels made impossible by
construction — the thing the single subscriber exists to allow.

**Keeping the producer's destination in the row's metadata.** `wire_of`
would emit two `destination` headers, and nothing reads the second.
