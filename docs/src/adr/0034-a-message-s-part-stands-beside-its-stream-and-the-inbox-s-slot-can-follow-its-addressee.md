# ADR-0034: A message's part stands beside its stream, and the inbox's slot can follow its addressee

## Status

Proposed (2026-11-03), not implemented.
- **The problem.** The distributed-systems refutation of ADR-0033 found it,
  and it was measured on a scratch PostgreSQL 14.24 (§ Context).
- **The decision.** It was reached in a dialogue between the owner and the
  author:
  - the owner: a cause's part has no business in the hash that picks a
    message's slot, and belongs in the identity's key instead;
  - the author: the slot then has to follow the aggregate a message
    addresses, not its cause.
- **Owed before implementation.** A blind solver and a discriminator, as for
  any change of this library. Their findings revise this record.

## Context

**Identity and order, as built.**
- **The inbox's identity.** A message is stored under `(tenant_id,
  stream_type, stream_id, stream_position)`, its primary key (ADR-0003,
  ADR-0030). The same identity again is ignored as a redelivery.
- **The inbox's slot.** It is `hashtext(<partition key>) % slots`, the key a
  SQL expression over the row (`PartitionKey`: `ByStream`, `ByUri`), fixed
  with the cut for the table's life (ADR-0008). A slot is worked by one loop
  at a time, in arrival order (`ArrivalOrder`).
- **The outbox's relay.** It hands rows on in `(transaction_id, position)`
  order, below the snapshot's `xmin`, and never passes one over
  (`NoPassedOver`, ADR-0007). The order the bridge proves end to end
  (`EndToEndOrder`) is therefore the order of the writers' transaction ids.

**How a host names a Command it sends itself.** The host that uses this
library names a Command to self by the event it comes after, so that its
cause, not its receiver, mints its position. That rule was the host's own
decision (2026-10-15): only the sender's aggregate can number its stream,
and a stream named by the receiver would collide between two senders. The
identity is then:
- `stream_type` — the Command's;
- `stream_id` — `{after: {stream_type, stream_id}, part}`;
- `stream_position` — the cause's.

A cause that sends several Commands of one type at one position (a fan-out)
tells them apart by `part`, put inside `stream_id`. This record keeps the
host's rule. What it changes is that `stream_id` does two jobs:
- **it discriminates the siblings of one cause**, which it must;
- **it decides their slot,** which no one chose: `ByStream` hashes all of
  `stream_id`, `part` included.

**What that does to order.**
- **Two Commands addressed to one aggregate from two causes get two slots.**
  In the host, a Device's projection is requested after a publication's
  range of its cohort and after the Device's placement: two streams, two
  slots, measured on the host's drill (slots 16 and 45 for one Device and
  lineage). Two loops take them at once.
- **The writes stay serial**, under the aggregate's row lock and version.
- **The transactions' ids do not.** An id is assigned at a transaction's
  first write, and the inbox's dispatch writes first: `take` updates its
  slot's row before the handler reads anything.

Measured on the scratch server:
1. Transaction A took a slot (id 10222) and waited.
2. Transaction B (id 10223) took another, wrote version 5 of an aggregate and
   its outbox row, and committed.
3. A then wrote version 6 and its row.
4. The relay handed on version 6, then version 5.

Every component kept its promise: the outbox its transaction-id order, the
inbox its arrival order. What breaks is the assumption that transaction-id
order is a stream's version order. With one loop no two transactions
overlapped and the two coincided. The host runs eight loops per replica.

**Why the relay cannot order by commit.** Commit order is the version order
of any one aggregate: a writer of version 6 read version 5 committed. But a
role reads commit order only through logical decoding — `wal_level = logical`,
a replication slot, the `REPLICATION` attribute. A service's role holds none
of them, and some of the host's targets cannot grant them. Transaction ids
are the order an ordinary role can prove complete.

**A cause can address one aggregate twice.** ADR-0030's incident was one:
an event requested a bundle for each lineage of each Device it touched, and
two lineages of one Device would have made two Commands. So a part is not
an addressee. The part tells siblings apart; the addressee says what they
write.

## Decision

1. **The order the library promises is stated as what it is.** End to end,
   messages keep the order of their writers' transaction ids. A stream's
   messages can arrive out of their positions' order when its writers'
   transactions overlap. A consumer that keeps state from a stream compares
   positions and ignores a position at or below the one it applied. This is
   written into both READMEs and beside `EndToEndOrder`.

2. **A part is a component of the identity, not of the stream.**
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

3. **A message may name its addressee, and the inbox can be cut by it.**
   - **The header.** `identity::ADDRESSEE` names the aggregate a message
     addresses, as text, and the inbox stores it in a column of its own.
   - **The partition key.** `ByAddressee` is
     `tenant_id || ':' || coalesce(addressee, stream_type || ':' || stream_id::text)`.
     A message that names no addressee falls back to its stream, as
     `ByStream` does. That is right for an event, whose source aggregate is
     the one its consumer writes. A part never enters any slot's key.
   - **The addressee may be coarser than the aggregate** — a Device for every
     translation of it — provided every writer of the aggregate maps to it.
     Coarser serialises more, never less.
   - **What it guarantees.** Within one inbox, two messages addressed to one
     aggregate are never processed at once. A slot is worked by one loop at a
     time, in arrival order, so their transactions do not overlap, and the
     aggregate's own outbox rows leave in its versions' order — insofar as
     those messages are its only writers.
   - **What it does not.**
     - A handler that writes several aggregates names one addressee.
     - A writer outside the inbox, such as a request served over HTTP, takes
       no slot.
     - A producer in another service is its own.

     There decision 1 holds.

4. **The tables change once.**
   - **The inbox.** Its primary key and its slot's key are fixed for the
     table's life (ADR-0008), so a host recreates its inbox, as it does for a
     new cut.
   - **The outbox.** Its guard is rebuilt by a migration of its component
     (ADR-0029), the part taken into its digest.

5. **What a host does.**
   - It writes `part` and `addressee` as headers of their own; its writer of
     a Command to self no longer packs the part into `stream_id`.
   - Each sender names the aggregate its Command addresses.
   - Its consumers that keep state from a stream compare positions
     (decision 1).

## Consequences

- **One fan-out's Commands still spread** across the slots, by their
  addressees: the parallelism a fan-out has today is kept.
- **Concurrent writers of one aggregate in one inbox disappear,** and with
  them their version conflicts and retries.
- **A hot addressee is worked serially.**
- **An event's identity and slot are unchanged:** no part, no addressee, its
  stream.
- **A causal dependency on one sibling of a fan-out names its part,** and
  waits for that sibling alone.
- **A message carrying a part to an inbox that predates this decision** is
  taken for its stream without the part: siblings collide there as one
  identity. A host recreates its inbox before its senders write parts
  (decision 4).

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
  - It keeps a cause's messages in order, not an aggregate's: two causes
    addressing one aggregate are still two slots.
  - And the many Commands one event sends at one position collapse into one
    slot, worked by one loop.
- **The stream named by the addressee.** The sender cannot mint the
  receiver's positions, and two senders collide on them (the host,
  2026-10-15).
- **Commit order, by logical decoding.** It is the right order, out of an
  ordinary role's reach (§ Context).
- **The transaction's id taken at the aggregate's write** — a slot claimed
  by an advisory lock, which takes no id.
  - Any earlier write anywhere undoes it.
  - A handler that writes two aggregates cannot satisfy both.
- **Only the consumer's comparison of positions.** It is needed anyway
  (decision 1). But alone it leaves concurrent writers of one aggregate in
  one inbox, with their conflicts and retries, and every inverted stream for
  every consumer to defend against.

## Executors

Owed, with the implementation:
- **The inbox.**
  - Two messages of one stream and position with different parts are both
    stored and processed; one part twice is dropped; a message with no part
    is stored as before.
- **`ByAddressee`.**
  - Two messages of two streams with one addressee share a slot.
  - A message with no addressee takes its stream's slot.
  - A part changes no slot.
  - A table cut by another key is refused, as today.
- **Concurrency**, with two loops: Commands addressed to one aggregate from
  two causes —
  - are never in flight together, confirmed by a lock probe in `pg_locks`;
  - leave the aggregate's outbox rows in its versions' order.

  The scratch measurement of § Context, as a test that fails under
  `ByStream`.
- **The outbox's guard.** Two siblings of one cause in one transaction are
  accepted; one part twice is refused (ADR-0030's test, extended).
- **Causal dependencies.** A dependency on one sibling's part waits for that
  sibling only.
- **TLA+.**
  - `Inbox.tla` with slots by addressee and an invariant
    `AddresseeExclusive`: no two messages of one addressee in flight.
  - `Bridge.tla` with `AddresseeVersionOrder` over a producer whose writers
    are inbox messages.
  - A configuration that must fail: slots by stream with two causes of one
    addressee.
- **The host.**
  - Its writer of a Command to self, tested to write the part and the
    addressee as headers.
  - Each of its consumers that keeps state from a stream, shown to compare
    positions.

## Open measurements

- **The host's drill with `ByAddressee`.** A publication's fan-out, an
  import's placements and a change's tiles: throughput against `ByStream`,
  and version-conflict retries counted.
- **How often a fan-out's siblings share an addressee,** and how long a hot
  addressee's slot is held.

## References

- ADR-0003, ADR-0005, ADR-0006, ADR-0007, ADR-0008, ADR-0029, ADR-0030,
  ADR-0033.
- `crates/inbox/src/partition.rs`, `crates/bus/src/identity.rs`,
  `crates/inbox/src/message.rs` (`CausalDependency`).
- `verify/tla/Inbox.tla`, `verify/tla/Bridge.tla` (`EndToEndOrder`).

## Change Log

- 2026-11-03 — Proposed: a message's part beside its stream in the identity,
  never in a slot's key; an addressee a message may name, and a partition
  key by it; the order the library promises stated as transaction-id order.
  A blind solver and a discriminator are owed before implementation.
