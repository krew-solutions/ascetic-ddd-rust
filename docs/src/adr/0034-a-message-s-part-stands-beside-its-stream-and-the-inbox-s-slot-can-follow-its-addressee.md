# ADR-0034: A message's part stands beside its stream, and the inbox's slot can follow its addressee

## Status

Proposed (2026-11-03), not implemented.

**Deferred (2026-11-05) by the host's measurement** (§ Open measurements).
In the one host, at a launch Tenant's load, the messages for one aggregate
meet almost never. The cut would remove none of the waits measured, and
the identity's change alone moves no message. Decision 2 lands when a host
measures its loops meeting on one aggregate: waits or retries between one
aggregate's messages, past what its retries absorb.

**Decision 1 withdrawn (2026-11-05):** under it, a causal dependency on a
message of many parts has no completion (§ Why decision 1 is withdrawn).
Decision 2 does not rest on it.

**The first draft** came from a dialogue between the owner and the author:
- **the owner:** a cause's part has no business in the hash that picks a
  message's slot, and belongs in the identity's key;
- **the author:** the slot then follows the aggregate a message addresses.

**Revised the same day** after a blind solver and a discriminator. The first
draft also tried to answer the order in which an aggregate's messages leave
an outbox. That half is ADR-0035's.

**Revised again (2026-11-04) after a review by four roles** — software
design, distributed systems, database with performance, evolution. Both
ideas stood: the part in the identity, and an inbox cut by a key that
follows an aggregate. Four of the first draft's mechanisms did not:
- the sender named the addressee, which another service's event cannot;
- a message without one fell back to its stream without its part, which
  put a whole fan-out into one slot;
- the inbox was to be recreated, which loses what it holds;
- released binaries were not fenced, and would mark a fan-out's siblings
  processed.

**This record keeps one half of the first problem:** an inbox processes the
messages for one aggregate one at a time, as a partition processes one
aggregate's stream — where the receiver can say which aggregate a message
is for.

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

**For a Command, the stream is its cause.** The host names a Command by the
event it comes after, so that the sender, not the receiver, mints its
position. Only the sender's aggregate can number its stream, and a stream
named by the receiver would collide between two senders (the host's
ADR-0069, 2026-10-14; its ADR-0065, 2026-10-16). The identity is then:
- `stream_type` — the Command's;
- `stream_id` — `{after: {stream_type, stream_id}, part}`;
- `stream_position` — the cause's.

A cause that sends several Commands of one type at one position tells them
apart by `part`, put inside `stream_id`. One cause can send two Commands
for one aggregate — ADR-0030's incident: two lineages of one Device. So a
part is not an addressee.

**Who writes one aggregate, in the host.** One Device's aggregates —
`DeviceContext`, its `CohortAssignment`s, its `PlatformTranslation`s — are
written by:
- its own Commands to self (`ResolveBundleForDevice`);
- another service's events about it, on two streams: `device.Device` and
  `device.Facts`;
- Commands that write a set decided at run time: a change's tiles and a
  publication's ranges, 250 Devices each (the host's ADR-0084 decisions 4
  and 12);
- a request's handler, outside the inbox.

They meet at the aggregate's lock and version, and at the Tenant's hold
(ADR-0084 decisions 3 and 10). That is where the aggregate's consistency
lives (`aggregate_version`, an optimistic offline lock).

**What concurrent processing of one aggregate's messages costs.**
- **Version conflicts.** The loser is retried with a backoff, holding its
  slot (ADR-0005), and after its attempts it is parked.
- **Decisions on mixed reads.** The aggregate's write is serialised; the
  handler's other reads are not.
- **Blocked loops.** Under a locking read one loop waits, transaction open,
  for another.

The host's drill retried no inbox row. Which pairs of message types meet at
a launch Tenant's load was measured on 2026-11-05 (§ Open measurements).

**Measured by the review, on the host's drill and a scratch server:**
- **One change's 800 tiles** occupy 64 slots under `ByStream`, and one slot
  when the part is taken out of the key.
- **A Device's two foreign streams** share a slot for 1 999 of 127 506
  Devices (1.6%). Its Commands share a slot with its events only if the key
  spells the event's stream byte for byte: `device.Device:"probe-0"`, with
  the quotes, falls into slot 41; without them, slot 31.
- **A key as text** splits 9 842 of 10 000 composite ids written with their
  keys in two orders; as canonical `jsonb::text`, none.
- **ADR-0030's guard** with the part appended by `||` guards no row of the
  drill's outbox (0 of 101 164): `jsonb || NULL` is NULL, and a NULL digest
  is exempt. With `coalesce(metadata->'part', '""')`, every row.
- **Every row's stored slot** equals what the key below computes for a row
  with no addressee and no part (0 of 228 984 differ): no row moves.
- **A released binary's statements** name four columns. Against a table
  keyed by five, its `mark_processed` marks every sibling of an identity
  processed after one handler; its insert fails closed (`no unique or
  exclusion constraint matching the ON CONFLICT specification`).
- **Recreating the inbox** loses what it holds: 107 709 of the drill's
  228 984 rows were unprocessed, and dependency targets are kept for ever
  (ADR-0033).
- **The primary key** is 134 MB for 229 000 rows; the unique index on the
  16-byte `message_id`, 9.1 MB. Sorting the key took 5.5–5.9 s; a 16-byte
  digest of it, 0.59 s.

## Decision

1. **A part is a component of the identity, not of the stream.**
   - **The identity** is `(tenant_id, stream_type, stream_id, stream_position,
     part)`.
   - **`part`** is `jsonb`, as `stream_id` is, compared as canonical JSON.
     Absent and `""` are one value everywhere: the column's default `'""'`,
     the guard's `coalesce(metadata->'part', '""')`, `CausalDependency`'s
     `#[serde(default, skip_serializing_if = …)]`. An identity without a
     part is then spelled byte for byte as today — in `waiting_for`, in
     ADR-0009's lock key, in ADR-0033's sweep.
   - **`stream_id`** names the stream alone; for a Command to self, its
     cause.
   - **Every statement that names the identity names the part:**
     - the inbox's primary key and its insert;
     - `mark_processed`, `record_failure`, `set_waiting_unless_processed`,
       `is_processed`, `unpark_row`, `resolve_row`;
     - the holder read of ADR-0030 decision 4, and ADR-0033's sweep;
     - the outbox's guard of one message per identity per transaction
       (ADR-0030), or a fan-out of one transaction is refused as one
       identity twice;
     - the bus's identity names (`identity::PART`, a header, and
       `HEADERS`), and `CausalDependency`.

2. **The receiver names the aggregate a message is for, and the inbox is cut
   by it.**
   - **The addressee is the receiver's.** It is the aggregate the receiver's
     handler writes, in the receiver's own words. Another service's event
     cannot carry it: its publisher does not know the consumer's model, and
     must not (the host's CLAUDE.md, the direction of awareness).
   - **The intake names it.** A host gives the inbox, per channel, a
     function from a message's clear headers to its addressee, or none
     (`AddresseeOf`). The intake stores the result in a column of its own,
     `jsonb`, null when none — an empty one is null too. For another
     service's events, the function reads their stream: the Device of
     `device.Device` and of `device.Facts` alike. For its own Commands, it
     reads the addressee header its senders wrote with one kernel function
     per aggregate type, so that every writer of an aggregate spells it
     alike.
   - **The slot's key** is
     `tenant_id || ':' || coalesce(addressee::text, <stream, then part>)`,
     the part appended only when it is not `""`. A message with an addressee
     falls in its aggregate's slot. One without keeps its stream's slot, and
     siblings of one cause are spread by their parts, as today.
   - **A set-writer names no addressee.** A tile, a range, a batch writes
     aggregates decided at run time; no one key covers them but the
     Tenant's, which would put a Tenant on one loop. Its part spreads it.
   - **The slot is computed from the key at insert,** and a `CHECK` holds it
     to the key, as ADR-0033 holds a generation. The key now reads a column
     the intake fills, which a generation expression fixed for the table's
     life cannot follow.
   - **The addressee is not part of the identity:** it lives in the inbox's
     own module, not in `identity`, and enters neither the primary key nor
     the guard. Two writers of one identity with two addressees would
     otherwise pass the outbox's guard while the inbox drops one (ADR-0030,
     class K).

3. **What it guarantees, and what it does not.**
   - **It guarantees:** within one inbox, messages with one key are processed
     one at a time, in arrival order. Their transactions do not overlap, so
     they neither conflict on a version nor block each other's loops.
   - **For one aggregate,** that holds where every writer of it is a message
     of this inbox naming it: its own Commands and another service's events
     about it, once the intake names them.
   - **It does not:**
     - **keep a set-writer and an aggregate's own messages apart.** A tile
       and a Device's event run in two slots and meet at the aggregate's
       lock;
     - **keep out writers outside the inbox:** a request's handler, another
       inbox;
     - **order messages from several senders.** They come from several
       streams, and ADR-0035 orders one stream on one URI; across URIs,
       slots or a broker's partitions, none;
     - **order past a message that waits.** A parked or waiting message
       leaves the queue, and later ones pass it;
     - **replace the aggregate's consistency.** That remains its lock and
       its version. The key turns some lock waits and version conflicts into
       queue waits; no lock or hold may be dropped because of it.

4. **The tables change in place, after ADR-0033 and ADR-0035 are out.**
   - **A release of their own, earlier.** No old relay runs when this record
     lands, so ADR-0035's repeats from an old position never meet a changed
     identity.
   - **The inbox, step by step, each step one table (ADR-0029):**
     - `part` and `addressee` added, neither rewriting the table;
     - a unique index on the five columns, swapped in as the primary key;
     - the slot's expression dropped, its values kept, and the slot computed
       at insert from then on: every row's slot already equals its key's;
     - the meta table's key, in a step of its own.

     Every row keeps its identity, its slot, its repeat memory and its
     dependency targets. The inbox is not recreated: that loses its rows in
     flight, its repeat memory, and the dependency targets ADR-0033 keeps.
   - **The key's build** holds the table's lock while it sorts: 5.5–5.9 s at
     229 000 rows, past the host's one-second write budget. So ADR-0033's M5
     — the key as a 16-byte digest, over the five fields from the start — is
     decided before this record, and the key changes once. If the hold is
     still over budget, the step lands before launch, or once ADR-0033's
     windows have bounded the inbox.
   - **The outbox's guard** is made part-aware in ADR-0033's ring step, a
     no-op until a sender writes a part. It is not rebuilt again: a finer
     key is unique wherever the coarser one was, and the ring names its
     leaf indexes as ADR-0030's refusal expects.
   - **Released binaries are fenced.** The migration renames what a released
     `take` reads, so that an old replica fails loudly instead of running
     one sibling and marking the rest processed. Its insert already fails
     closed.
   - **The rollback boundary is this migration.** A released binary refuses
     to start: the meta table's key is not its own. The host's runbook names
     the boundary per orchestrator, and deploys stop-first or with the
     fences in place.

5. **What a host does.**
   - **Its writer of a Command to self** writes `stream_id = {after}`, and
     `part` and `addressee` as headers of their own.
   - **Its senders** spell an addressee with the kernel's one function per
     aggregate type; its set-writers name none.
   - **Its intake** gives the inbox an `AddresseeOf` per channel, its foreign
     events included, and counts the rows of each channel that name none.
   - **Its holds stay:** ADR-0084's `FOR SHARE` on a Device's context and the
     Tenant's hold.
   - **A Command between contexts carries no part** until its receiver's
     catalogue declares the header, or a new major does. An older receiver
     would store it as metadata, and siblings would collide.
   - **Its contracts change:** ADR-0071 decision 8's clear headers gain
     `part` and `addressee`, platform-minted identifiers both; the command
     catalogues describe `stream_id` without the part. No gate reads a
     header today (`buf breaking` covers payloads), so the change is owed a
     check of its own.
   - **It cuts its inbox by the key** where the measurement below shows its
     loops meet on one aggregate. The host's own does not (2026-11-05).

## Consequences

- **A fan-out keeps its spread:** a set-writer by its part, a per-aggregate
  fan-out by its addressees (a million addressees over 64 slots: at most
  2.5% above the mean).
- **Within one inbox, the messages for one aggregate stop meeting** where
  the intake names them alike — a Device's requests, its lifecycle events
  and its facts — and still meet a set-writer at its lock.
- **A hot addressee is worked serially,** as a hot stream is today.
- **An event without an addressee** keeps its slot, its identity, its order.
- **A causal dependency on one sibling of a fan-out names its part,** and
  waits for that sibling alone. A dependency that names no part, from a
  producer on an older library, still parses and waits.
- **The transition:** rows written before it keep their stream's slot, rows
  after it take their addressee's. One Device's old and new Commands may run
  apart until the old ones drain.
- **No order on the wire.** This record promises nothing about it; ADR-0035
  does.

## Why decision 1 is withdrawn

Found by the owner on 2026-11-05, after the deferral, and checked against
the code.

**A dependency names a stream position, and the inbox resolves it by one
row.** `CausalDependency` holds `tenant_id`, `stream_type`, `stream_id` and
`stream_position` (`crates/inbox/src/message.rs:128-137`). `is_processed`
looks for one processed row with those four values (`LIMIT 1`,
`crates/inbox/src/pg/store.rs:383-389`).

**Today a position names one message.** A cause's siblings differ in
`stream_id`, which holds the part (§ Context). A dependency on one sibling
names that sibling's whole stream id. A dependency on the fan-out as a whole
cannot be written, and nothing pretends it can.

**Under decision 1, a position names a set.** Every sibling of a cause shares
`(stream_type, stream_id, stream_position)` and differs in `part` alone. A
dependency on the cause has no part; it is the natural way to write "after
that message". It then has no completion:
- **Resolved by today's four-column lookup,** it is satisfied by the first
  sibling processed while the others are not: woken early.
- **Resolved part-aware,** with an absent part read as `""` (decision 1), it
  waits for a sibling that does not exist, since a fan-out's siblings all
  have parts. It waits until `max_wait` expires it, and is then parked
  (ADR-0006 decision 4).
- **To be satisfied correctly** it needs the number or the list of the
  cause's parts: a barrier. Neither the dependency nor the siblings carry
  one. The sender knows its parts only while it writes them: a range's part
  is cut from a set read at run time (§ Alternatives, the ordinal).

§ Consequences' "a dependency that names no part … still parses and waits"
is the second failure stated as compatibility. So decision 1 makes a stream
position ambiguous for the one mechanism that reads positions across
messages (ADR-0006), and it adds nothing a dependency can say: one sibling
can already be named today.

**Decision 2 stands without it.** With the part inside `stream_id`, the key
`coalesce(addressee, stream)` spreads a set-writer's fan-out as today and
puts one aggregate's addressed messages together. It stays deferred under
its own trigger.

## Alternatives Considered

- **The addressee written by the sender** (the first draft). Another
  service's event cannot carry the consumer's aggregate, a sender between
  contexts would have to know the receiver's boundaries and its coarsening,
  and two senders spelling one aggregate differently run it in two slots
  without a word.
- **No part in the key of a message without an addressee** (the first
  draft). One change's 800 tiles went from 64 slots to one: at launch, 4 000
  tiles of 0.6–1.9 s each in one queue, and a sixty-fourth of the Tenant
  behind them. It is the defect the next alternative was rejected for.
- **The part out of the hash, the slot by stream** (`ByStream` over the
  cause). It partitions by the cause, not the aggregate, and a fan-out's
  Commands collapse into one slot.
- **The Tenant as the key of a set-writer.** It covers every aggregate a
  tile writes, and puts the Tenant on one loop.
- **The bus's message key** (`Message::key`, kept in the inbox's `uri`, which
  `ByUri` cuts by). It is the transport's key, for order on the wire, and the
  host fixes it to the Tenant (its ADR-0071 decision 8). The processing key
  is another thing.
- **An ordinal of the Command within its cause's fan-out, as the
  discriminator** (the owner's first form). It numbers by enumeration, not
  by what a member is: a cause run again over a changed set renumbers it.
  The same holds of a part taken from a run-time set — a range's lower key —
  so a part's stability is the host's to keep.
- **The stream named by the addressee.** The sender cannot mint the
  receiver's positions, and two senders collide on them (the host's ADR-0065
  and ADR-0069).
- **The inbox recreated** (the first draft). It loses its rows in flight,
  its repeat memory and its dependency targets; a copy into a new table is a
  second table, which the runner refuses (ADR-0029).
- **A locking read of the aggregate on every handler** (`FOR UPDATE` where
  today a plain read meets an optimistic write). For a set-writer it is not
  an alternative but the mechanism: the host's holds already are. For one
  aggregate's messages a partition is cheaper — the second waits in its
  slot's queue, no transaction open — and the locking read is the measured
  comparison.
- **The addressee's partition as the fix for the order on the wire** (this
  record's first form). ADR-0035 orders the wire for every writer.

## Executors

Owed, with the implementation.
- **The identity.**
  - Two messages of one stream and position with different parts are both
    stored, and each handler runs once — exactly twice in all.
  - One part twice is dropped; a message with no part is stored as before.
  - One failing sibling alone is retried, parked, unparked, resolved.
  - Each statement of decision 1 with its `part` condition removed fails a
    test.
- **The outbox's guard.** Two part-less messages of one identity in one
  transaction are still refused; two siblings with different parts are
  accepted; one part twice is refused.
- **Dependencies.** A dependency without a `part` field, from another
  producer — the host's BC-APPLICATION form — still waits; a row set waiting
  under today's serialisation is woken by the new mark; a dependency on one
  sibling waits for that sibling alone.
- **The key.**
  - Two messages with one addressee from two streams share a slot.
  - A message with no addressee takes its stream's slot; an empty addressee
    is none.
  - Two siblings of one cause with no addressee land apart; a part changes
    no slot of a message with an addressee.
  - A table cut by another key is refused, as today.
- **Concurrency,** with two loops: messages with one key are never in
  flight together, confirmed by a lock probe in `pg_locks`; the same test
  under `ByStream` fails.
- **The migration.** On a copy of an inbox in flight: no row lost, moved, or
  processed again; the released `take`, `head_of`, `mark_processed` and
  insert, kept as text and run against the migrated table, each refused.
- **TLA+.** `Inbox.tla` with a key per message and an invariant
  `KeyExclusive`: no two messages of one key in flight. A configuration that
  must fail: a key without the part, two siblings of a set-writer meeting.
  The model's `part`, its partition function today, is renamed `cut`.
- **The host.**
  - Its writer of a Command to self writes the part and the addressee as
    headers.
  - For each aggregate type, every message type that writes it yields one
    key for one id, its foreign streams included.
  - The drill's tiles spread over the slots.

## Open measurements

- **Which messages meet, under `ByStream`, at a launch Tenant's load** —
  measured 2026-11-05 (the host's ADR-0084 § Owed).
  - **The setup.** The host's drill ran twice: 100 000 Devices, eight
    loops, its five waves. Each Device's first facts were reported right
    after it was managed, and its facts again during a fan-out, meeting
    its own projection. The server logged every lock wait past 20 ms; a
    sampler took the waits and their holders every 100 ms.
  - **The pairs:**
    - **Tile with tile:** 398 waits (263 s and 270 s), at the compile hold
      of the one cohort every tile entered. A set-writer's lock, which no
      key moves.
    - **Request with tile:** 32 waits in one run, at a Device's assignment
      row that a tile held.
    - **Tile or placement with a change:** three or four a run, at the
      Tenant's partition.
    - **Event with request, one Device:** once a run, 11 ms and 21 ms, at
      the Device's context.
    - **Request with request, one Device:** one version conflict a run, of
      516 000 and 706 000 rows, at the stand's start; retried.
  - **Order.** 53% of the first facts reports were processed before their
    Device's lifecycle event, and let go by the host's handler. The next
    report made them good. A cut by the key would not order these either:
    the two streams arrive on two channels, and arrival keeps no order
    between them (§ Context). A causal dependency does (ADR-0006).
  - **So the waits are tile and range pairs,** the cut removes none of
    them, and the host keeps `ByStream`.
  - **Where the order is lost** (measured the same day, 20 000 Devices, each
    Device's first report published after its awaited `device.managed`):
    - 19 796 reports were stored in the inbox before their Device's
      lifecycle event, inverted on the way by two channels and two bridges;
    - 100 arrived after it and were still processed before it, inverted by
      two slots.

    A cut by the key orders what arrives in order. An order lost between
    routes is restored only by what the producer stamps on the message.
- **The key's build and the slot's expression dropped,** on a copy of an
  inbox at launch size, under the runner's lock: the hold against the
  one-second budget, and the table not rewritten.
- **The host's drill by the key:** a publication's fan-out, a change's
  tiles, an import's placements, against `ByStream`.
- **How long a hot addressee's slot is held.**

## References

- ADR-0003, ADR-0005, ADR-0006, ADR-0008, ADR-0009, ADR-0029, ADR-0030,
  ADR-0033, ADR-0035.
- `crates/inbox/src/partition.rs`, `crates/inbox/src/pg/store.rs`,
  `crates/inbox/src/channel.rs`, `crates/inbox/src/message.rs`
  (`CausalDependency`), `crates/bus/src/identity.rs`,
  `crates/bus/src/message.rs`, `crates/migration/src/lib.rs`.
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
- 2026-11-03 — Corrected by ADR-0035's review of roles: arrival keeps no
  order between the senders of one aggregate's messages, nor past a message
  that waits; the record lands after ADR-0033 and ADR-0035.
- 2026-11-04 — Revised after a review by four roles (software design,
  distributed systems, database with performance, evolution).
  - **The addressee is the receiver's,** named at intake per channel, for
    another service's events too; the sender's own Commands spell it with
    one function per aggregate type.
  - **A message without an addressee keeps its part in its key,** so a
    set-writer's fan-out keeps its spread (measured: 64 slots, not one).
  - **The guarantee restated:** one key, one at a time; an aggregate's
    consistency stays its lock and version; set-writers and writers outside
    the inbox still meet it there.
  - **The part as `jsonb`,** absent and `""` one value; every statement that
    names the identity names it; the guard by `coalesce`.
  - **In place, not recreated;** released binaries fenced; the rollback
    boundary named; M5 decided first; the guard made part-aware in
    ADR-0033's ring step; a release after ADR-0033 and ADR-0035 are out.
  - **The host's work** extended: its intake's functions, its holds kept,
    no part across contexts until the receiver declares it, its contracts.
- 2026-11-05 — The host's measurement: one aggregate's messages meet
  almost never at a launch Tenant's load. The waits are set-writers'. The
  record is deferred until a host measures otherwise, or a dependency must
  name one sibling of a fan-out.
- 2026-11-05 — Decision 1 withdrawn. Under it, a dependency on a message of
  many parts has no completion: woken by its first sibling, or waiting for
  a sibling that does not exist. Found by the owner. Also:
  - the deferral's second trigger dropped, since one sibling can already be
    named;
  - where the order is lost was measured: on the routes, not in the slots.
