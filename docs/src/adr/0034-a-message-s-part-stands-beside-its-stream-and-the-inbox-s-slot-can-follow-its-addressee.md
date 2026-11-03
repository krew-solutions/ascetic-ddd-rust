# ADR-0034: A message's part stands beside its stream, and the inbox's slot can follow its addressee

## Status

Proposed (2026-11-03), not implemented.

**The first draft** came from a dialogue between the owner and the author:
- **the owner:** a cause's part has no business in the hash that picks a
  message's slot, and belongs in the identity's key;
- **the author:** the slot then follows the aggregate a message addresses.

**Revised the same day** after a blind solver and a discriminator. The first
draft also tried to answer the order in which an aggregate's messages leave
an outbox, and argued that only transaction ids give a complete order to an
ordinary role. The discriminator refuted that: a snapshot gives commit order
to any role. That half is ADR-0035's.

**This record keeps the other half:** an inbox processes the messages
addressed to one aggregate one at a time, as a partition processes one
aggregate's stream.

**Owed before implementation:** a review by roles.

## Context

**Identity and partition, as built.**
- **The inbox's identity.** A message is stored under `(tenant_id,
  stream_type, stream_id, stream_position)`, its primary key (ADR-0003,
  ADR-0030); the same identity again is ignored as a redelivery.
- **The inbox's slot.** It is `hashtext(<partition key>) % slots`, the key a
  SQL expression over the row (`PartitionKey`: `ByStream`, `ByUri`), fixed
  with the cut for the table's life (ADR-0008).
- **A slot's order.** A slot is worked by one loop at a time, in arrival
  order (`ArrivalOrder`).

**A partition is the unit of order and of isolation.** Order is kept within
a partition, and a partition is chosen as a single aggregate's stream (Oskar
Dudycz, "Outbox, Inbox patterns and delivery guarantees explained": "we get
the guarantee of ordering for the events within the same partition"). An
aggregate whose messages are spread over partitions is processed by several
loops at once.

**For an event from another service, `ByStream` partitions by the right
aggregate.** Its stream is its source aggregate, and that is what its
consumer writes: a Device's events, the consumer's record of that Device.

**For a Command, the stream is its cause.** The host names a Command by the
event it comes after, so that the sender, not the receiver, mints its
position. Only the sender's aggregate can number its stream, and a stream
named by the receiver would collide between two senders (the host's
ADR-0069, 2026-10-14; its ADR-0065, 2026-10-16). The identity is then:
- `stream_type` — the Command's;
- `stream_id` — `{after: {stream_type, stream_id}, part}`;
- `stream_position` — the cause's.

A cause that sends several Commands of one type at one position tells them
apart by `part`, put inside `stream_id`. One cause can address one aggregate
twice — ADR-0030's incident: two lineages of one Device. So a part is not an
addressee. `ByStream` hashes all of `stream_id`, so the slot follows the
cause and the part. The Commands addressed to one aggregate are spread over
slots: the host's drill found one Device's requests in slot 45 (338 of
them, after its context) and slot 16 (one, after a bundle).

**What concurrent processing of one aggregate's messages costs.**
- **Version conflicts.** Two transactions write the aggregate under its
  version. The loser fails and is retried with a backoff, holding its slot
  (ADR-0005), and after its attempts it is parked. A legitimate message is
  then parked by the service's own loops' contention, and its effect waits
  for an operator.
- **Decisions on mixed reads.** The aggregate's write is serialised; the
  handler's other reads are not, and each decides on what it read at its
  own moment.
- **Blocked loops.** Under a locking read one loop waits, transaction open,
  for another.

The host's drill retried no inbox row. Contention is unmeasured at a launch
Tenant's load (§ Open measurements).

**The order in which the aggregate's messages leave its outbox** was the
first draft's other concern. ADR-0035 orders it for every writer, so this
record depends on none of it.

## Decision

1. **A part is a component of the identity, not of the stream.**
   - **The identity** is `(tenant_id, stream_type, stream_id, stream_position,
     part)`.
   - **`part`** is text, `''` by default — a message with no siblings carries
     none, and an event's identity is what it was.
   - **`stream_id`** names the stream alone; for a Command to self, its cause.
   - **Everywhere the identity is used,** `part` is named:
     - the inbox's primary key;
     - the outbox's guard of one message per identity per transaction
       (ADR-0030: its digest takes the part, or a fan-out of one transaction
       is refused as one identity twice);
     - the bus's identity names (`identity::PART`, a header, and `HEADERS`);
     - `CausalDependency`;
     - the holder read of ADR-0030 decision 4.

2. **A message may name its addressee, and the inbox is cut by it.**
   - **The header.** `identity::ADDRESSEE` names the aggregate a message
     addresses, as text, and the inbox stores it in a column of its own.
   - **The partition key.** `ByAddressee` is
     `tenant_id || ':' || coalesce(addressee, stream_type || ':' || stream_id::text)`.
     A message that names no addressee falls back to its stream, as
     `ByStream` does — right for an event, whose source aggregate is the one
     its consumer writes. A part never enters any slot's key.
   - **The addressee may be coarser than the aggregate** — a Device for every
     translation of it — provided every message that writes the aggregate
     maps to it. Coarser serialises more, never less.
   - **A handler that writes several aggregates** names the one it is for.
     The others it writes are written as today, under their locks and
     versions.
   - **What it guarantees.** Within one inbox, the messages addressed to one
     aggregate are processed one at a time, in arrival order — with
     ADR-0035, their senders' commit order. Their transactions do not
     overlap, so they neither conflict on its version nor block each
     other's loops.
   - **What it does not.**
     - A writer outside this inbox's processing — a request's handler,
       another service — meets these messages at the aggregate's lock and
       version, as today.
     - The order in which the aggregate's own messages leave its outbox is
       ADR-0035's.

3. **The tables change once.**
   - **The inbox.** Its primary key and its slot's key are fixed for the
     table's life (ADR-0008), so a host recreates its inbox, as it does for a
     new cut.
   - **The outbox.** Its guard is rebuilt by a migration of its component
     (ADR-0029), the part taken into its digest.

4. **What a host does.**
   - It writes `part` and `addressee` as headers of their own; its writer of
     a Command to self no longer packs the part into `stream_id`.
   - Each sender names the aggregate its Command addresses.
   - It cuts its inbox `ByAddressee`.

## Consequences

- **One fan-out's Commands still spread** across the slots, by their
  addressees: the parallelism a fan-out has today is kept.
- **Contention ends within one inbox.** Version conflicts, retries, parks
  and blocked loops between its loops on one aggregate disappear.
- **A hot addressee is worked serially.**
- **An event's identity and slot are unchanged:** no part, no addressee, its
  stream.
- **A causal dependency on one sibling of a fan-out names its part,** and
  waits for that sibling alone.
- **A message carrying a part to an inbox that predates this decision** is
  taken for its stream without the part: siblings collide there as one
  identity. A host recreates its inbox before its senders write parts
  (decision 3).
- **No order on the wire.** This record promises nothing about it; ADR-0035
  does.

## Alternatives Considered

- **An ordinal of the Command within its cause's fan-out, as the
  discriminator** (the owner's first form). It is unstable when the cause's
  workflow runs again after a redelivery and enumerates its fan-out anew —
  its members changed, its plan made again:
  - the same Command gets another number and is processed twice;
  - another Command gets its old number and is dropped as a repeat, without
    a word.

  And it does not, alone, bring one aggregate's Commands into one slot.
- **The part out of the hash, the slot by stream** (`ByStream` over the
  cause).
  - It partitions by the cause, not the aggregate: two causes addressing one
    aggregate are still two slots.
  - And the many Commands one event sends at one position collapse into one
    slot, worked by one loop.
- **The stream named by the addressee.** The sender cannot mint the
  receiver's positions, and two senders collide on them (the host's ADR-0065
  and ADR-0069).
- **A locking read of the aggregate on every handler** (`FOR UPDATE` where
  today a plain read meets an optimistic write; the discriminator's cheaper
  form).
  - It serialises the writes and ends the conflicts.
  - But the second loop still runs its transaction until the lock, then
    waits there with it open, its loop taken.
  - A partition makes it wait in its slot's queue instead, no transaction
    open.

  Kept as the measured comparison (§ Open measurements).
- **The addressee's partition as the fix for the order on the wire** (this
  record's first draft). It serialises only the writers that are messages
  addressed to that aggregate in one inbox; a change's transaction, a
  request's handler or another service still invert the stream. ADR-0035
  orders the wire for all of them.
- **Only the consumer's comparison of positions.** It concerns what a
  consumer applies, not how a service processes one aggregate's messages;
  ADR-0035 decision 7 keeps it.

## Executors

Owed, with the implementation.
- **The inbox.** Two messages of one stream and position with different
  parts are both stored and processed; one part twice is dropped; a message
  with no part is stored as before.
- **`ByAddressee`.**
  - Two messages of two streams with one addressee share a slot.
  - A message with no addressee takes its stream's slot.
  - A part changes no slot.
  - A table cut by another key is refused, as today.
- **Concurrency**, with two loops: messages addressed to one aggregate from
  two causes are never in flight together, confirmed by a lock probe in
  `pg_locks`. The same test under `ByStream` fails.
- **The outbox's guard.** Two siblings of one cause in one transaction are
  accepted; one part twice is refused (ADR-0030's test, extended).
- **Causal dependencies.** A dependency on one sibling's part waits for that
  sibling only.
- **TLA+.** `Inbox.tla` with slots by addressee and an invariant
  `AddresseeExclusive`: no two messages of one addressee in flight. A
  configuration that must fail: slots by stream with two causes of one
  addressee.
- **The host.** Its writer of a Command to self, tested to write the part
  and the addressee as headers; each sender's addressee named.

## Open measurements

- **Contention at a launch Tenant's load,** under today's `ByStream`: version
  conflicts, retries and parks with context changes, a publication and an
  import running together. Then the same under a locking read, and under
  `ByAddressee`.
- **The host's drill with `ByAddressee`.** A publication's fan-out, an
  import's placements and a change's tiles: throughput against `ByStream`.
- **How often a fan-out's siblings share an addressee,** and how long a hot
  addressee's slot is held.

## References

- ADR-0003, ADR-0005, ADR-0006, ADR-0008, ADR-0029, ADR-0030, ADR-0033,
  ADR-0035.
- `crates/inbox/src/partition.rs`, `crates/bus/src/identity.rs`,
  `crates/inbox/src/message.rs` (`CausalDependency`).
- `verify/tla/Inbox.tla`.
- Oskar Dudycz, "Outbox, Inbox patterns and delivery guarantees explained"
  (event-driven.io).

## Change Log

- 2026-11-03 — Proposed: a message's part beside its stream in the identity,
  never in a slot's key; an addressee a message may name, and a partition
  key by it; the order the library promises stated as transaction-id order.
  A blind solver and a discriminator are owed before implementation.
- 2026-11-03 — Revised after a blind solver and a discriminator.
  - **Moved to ADR-0035:** the order on the wire. The premise that only
    transaction ids give an ordinary role a complete order was refuted.
  - **What remains:** the processing partition — one aggregate's messages
    in one slot — with its guarantees restated: isolation within one inbox,
    no promise of order.
  - **Also:** the contention's costs named, a locking read added as the
    measured comparison, and the host's decisions dated by their ADRs.
