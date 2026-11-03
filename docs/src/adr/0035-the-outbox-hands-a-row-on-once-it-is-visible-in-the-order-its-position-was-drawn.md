# ADR-0035: The outbox hands a row on once it is visible, in the order its position was drawn

## Status

Proposed (2026-11-03), not implemented.

**Decided by** a blind solver and a discriminator, working from the problem
alone. The discriminator checked the solver's proof against PostgreSQL 14's
source and the batch rule against PgQ's (Skytools).

**Owed before implementation:** a review by roles — database, distributed
systems, evolution, operability, verifiability. This changes the relay's
release protocol and the shape of its positions.

It supersedes the order ADR-0007 gives the relay. It leaves ADR-0034 the
other half of the problem: which messages an inbox processes together.

## Context

**The relay's rule today** (ADR-0007; the design of Oskar Dudycz's
"Ordering in Postgres Outbox").
- A row carries `pg_current_xact_id()` of the transaction that wrote it.
- A dispatcher hands on the rows past its position whose transaction id is
  below the snapshot's `xmin`, in `(transaction_id, position)` order
  (`crates/outbox/src/pg/store.rs`, `fetch`).
- No committed row is ever passed over (`NoPassedOver`).

The article that gave the rule names its two limits. The order "respects
when the transaction was started, not when the message was appended", which
"may create problems if forgotten"; and "our queries may be a bit delayed if
our transactions last long". Both were met in the host project.

**A transaction's id is assigned at its first write, not at its commit.** Two
writers of one aggregate are serialised by their commits: the writer of
version 6 read version 5 committed. Their ids follow their first writes,
and nothing ties those to the aggregate. The inbox's dispatch writes first:
`take` updates its slot's row before the handler reads anything. Measured on
a scratch PostgreSQL 14.24:
1. Transaction A took a slot (id 10222) and waited.
2. Transaction B (id 10223) wrote version 5 of an aggregate and its outbox row,
   and committed.
3. A wrote version 6.
4. The relay handed on version 6, then version 5.

The same holds for any writer that writes before it reads an aggregate's
latest state: a permit recorded before a request's handler runs, a tile or a
change writing an aggregate after its slot's claim. More inbox loops and
more replicas make it common, not possible.

**`xmin` is the cluster's, not the database's.** Measured: a transaction
holding id 10235 open in another database of the cluster, a snapshot taken
in this one read `10235:10237:10235`. Id 10236 had committed; its rows were
visible but not below `xmin`. Under today's rule, one transaction left open
anywhere on the server stops every relay of every service on it.

**Commit order is readable by an ordinary role.** A snapshot names what has
committed: `pg_current_snapshot()`, `pg_visible_in_snapshot()`,
`pg_snapshot_xmin/xmax/xip` are executable by `PUBLIC` (measured). Logical
decoding, which would also give commit order, needs the `REPLICATION`
attribute and `wal_level = logical`, which a service's role and some hosts
cannot have. PgQ has released batches by snapshot visibility for about
fifteen years: a batch is what is visible in this tick's snapshot and was
not in the last one's, ordered by the event's id (Skytools,
`pgq.batch_event_sql`).

**Why visibility and a draw order give a stream's versions in order.** Let
T1 write version v of a stream S and T2 version w > v. Then:
- **T2 read T1's state.** T2 wrote w only after reading S at v or later —
  after T1 was visible. An optimistic write succeeds only on the latest read
  (it re-checks its predicate on the newer row), and a locking read waits for
  T1.
- **T2 drew later.** T1 drew its rows' positions before its commit; T2 draws
  its rows of S after that read. With a sequence of cache 1, `nextval` draws
  in real time across sessions (PostgreSQL's `CREATE SEQUENCE`: "with a cache
  setting of one it is safe to assume that nextval values are generated
  sequentially"). So T1's positions are lower.
- **A snapshot that sees T2 sees T1.** PostgreSQL 14's `CommitTransaction`
  leaves the ProcArray before it releases its locks (`xact.c`, "must be done
  before releasing locks"), so a waiter wakes only after the committer is
  visible. A snapshot lists top-level transactions only, so subtransactions
  do not matter.
- **Hence** T1's rows are released in an epoch no later than T2's. Within one
  epoch they go in position order, so T1's go first.

**Measured on the host's drill** (100 664 outbox rows, 752 transactions):

| Statement | Plan | Time |
| --- | --- | --- |
| Today's fetch of a batch | index scan | 0.19 ms |
| An epoch's batch, no position index | parallel scan and sort | 19–22 ms |
| An epoch's start | index-only scan | 17 ms |

The position sequence's cache is 1.

## Decision

1. **A dispatcher releases rows by epochs.** Per `(consumer group, uri,
   slot)`, the position row holds:
   - `delivered` — a snapshot; every row visible in it has been handed on;
   - `epoch` — the snapshot being handed on, or none;
   - `epoch_offset` — the last position handed on within it;
   - `epoch_end` — the highest position of the epoch.

2. **An epoch starts with `epoch := pg_current_snapshot()`.** Its rows are
   the slot's rows new in it: visible in `epoch` and not in `delivered`. They
   are found by the transaction id, through the existing `(slot,
   transaction_id, position)` index: ids at or above `delivered`'s `xmax`, or
   in its in-progress list, and below `epoch`'s `xmax`. `epoch_offset` is set
   to their lowest position less one and `epoch_end` to their highest. An
   epoch with no rows is delivered at once, so that `delivered` keeps up with
   the server.

3. **A batch is the epoch's next rows by position.** It takes the rows of the
   slot with `epoch_offset < position <= epoch_end`, visible in `epoch` and
   not in `delivered`, by position, up to the batch's size, through a new
   index `(slot, position) INCLUDE (transaction_id)`. The batch is handed on
   in that order and the epoch's offset moved to its last position, in the
   dispatcher's transaction, as today. When the offset reaches `epoch_end`,
   `delivered := epoch` and `epoch := NULL`, in the same transaction.

4. **The position sequence draws one at a time.** `migrate` refuses an
   outbox whose `position` sequence has a cache other than 1. A sequence
   reset backward is a defect the library does not repair.

5. **What a producer owes.** A message of a stream at a position is written
   by the transaction that wrote that version of the stream, or under a lock
   that makes the stream's writers wait, after that lock or read (P1). A
   producer that writes a stream's message from another transaction — a
   request relayed by a range of a fan-out, after the event it follows — gets
   no order for that stream. That is right where nothing depends on it, and
   wrong where something does.

6. **What the library then promises.**
   - **It promises:**
     - each stream's messages on one URI are first handed on in their
       writers' commit order — for an aggregate's stream, its versions'
       order — whatever wrote them: an inbox's loops, replicas, a request's
       handler, a transaction that writes several aggregates;
     - no committed row is passed over;
     - a transaction left open delays its own rows only.
   - **It does not promise:**
     - an order between streams (that is ADR-0006's causal dependencies);
     - an order for messages written against decision 5;
     - an order after the outbox: a topic repartitioned, a replay past what
       the receiving inbox still holds, an inbox that forgot the processed
       row (ADR-0033);
     - against a lost suffix of the write-ahead log in an asynchronous
       failover, the same as today.

7. **What a consumer still does.** Every consumer deduplicates — the inbox
   does — and treats gaps as its channel's contract says. Beyond that, by
   kind:
   - **A consumer that keeps a stream's latest state** ignores a position at
     or below the one it applied. It is then indifferent to order, and safe
     against what decision 6 leaves out.
   - **A consumer that applies transitions or merges deltas** cannot ignore
     an older position without losing it. Its channel is kept whole by the
     receiving inbox (ADR-0033, "keep"), or its contract tolerates a loss.

8. **The positions move.** A migration of the outbox component turns each
   `(last transaction, offset)` into `delivered = 'L:L:'`:
   - every transaction below `L` was delivered whole;
   - the rows of `L` itself are handed on again, and their identities drop
     them at the inbox.

   The migration bumps the component's version in its meta table, so a relay
   of the old rule refuses to start on the new table rather than share a
   position row with a new one.
   - `set_position` takes a snapshot: back to a transaction `F` is
     `'F:F:'`.
   - The observer's `horizon` becomes the epoch.
   - `Position` becomes the delivered snapshot and the epoch's offset.

9. **Corrected where the library claimed more.**
   - `crates/bus/src/identity.rs` and ADR-0030 say the inbox "orders a
     stream's messages by their positions". It orders a slot's messages by
     arrival.
   - The outbox's README says "order within a URI", meaning transaction-id
     order. It will mean decision 6.

## Consequences

- **The order a stream's consumer relies on is the producer's commit order.**
  It no longer depends on how many loops or replicas the producer runs, or
  on what each writes first.
- **A server's open transaction no longer stops every relay on it.**
- **Each outbox row costs one more index entry.** An epoch's walk covers
  every row positioned within it, so a long transaction's epoch re-walks
  what was written during its life. That cost is linear in the rows, not
  quadratic: each row is walked once per epoch it lies in.
- **ADR-0033's ring is amended on top of this record**, so that its fetch is
  written once:
  - a generation is "passed" when every row of it is visible in every
    position's `delivered`;
  - the fetch walks an epoch across the generations it names;
  - a rewind or a new group is a snapshot from the floor;
  - the counter-reset check reads `delivered`'s `xmax` against the server's;
  - its fetch costs are measured again;
  - its Context paragraphs on order and on the withdrawn per-stream record
    are rewritten.
- **The host's corrections.**
  - Comments that promised a Device's Commands are processed in order.
  - A contract sentence that a withdrawal "always follows" its delivery: true
    once this lands, for writers under decision 5.
  - Its consumers sorted by decision 7. `DeviceContext` keeps no source
    position and must, or stays on "keep".

## Alternatives Considered

- **Today's rule, kept and documented** (ADR-0007; Dudycz's "Ordering in
  Postgres Outbox"). It loses nothing, but it orders by first write and
  stalls on any open transaction — the two limits its own article names.
- **A gapless sequence in a singleton table.** It serialises every writer on
  one row; rejected by the same article for the backbone of messaging.
- **Gap detection with a high-water mark and tombstones** (Marten). It
  orders by position, but it has to tell a rolled-back gap from a slow writer
  by waiting, which is a timeout guessed for every server; and it is
  non-trivial, in the same article's words.
- **Logical decoding.** It gives commit order, out of a service role's reach
  and not carried across a physical failover in PostgreSQL 14.
- **Commit timestamps** (`track_commit_timestamp`). A server setting, off on
  the measured servers; wall-clock ties and steps break the order; and it
  still needs a visibility rule.
- **The batch re-sorted by the stream's position.** A third transaction whose
  id lies between two writers' releases the later version in one fetch and
  the earlier in the next.
- **An inbox partition by the aggregate a message addresses, as the order's
  fix** (ADR-0034's first draft). It serialises the writers that are messages
  addressed to that aggregate in one inbox, and no others. A change's or a
  tile's transaction in another slot, a request's handler, another
  service's producer still invert the stream. ADR-0034 keeps that partition
  for what it does serve: isolating the processing of one aggregate.
- **The slot claimed without a transaction id** (an advisory lock). The
  handler's first row lock assigns one anyway. And a transaction writing two
  aggregates cannot put both writes first.
- **Only the consumer's comparison of positions.** It is kept (decision 7),
  but alone it leaves every producer's streams inverted for every consumer to
  defend against, and a consumer of transitions cannot.

## Executors

Owed, with the implementation.

**TLA+.** `Outbox.tla` gains:
- aggregates with versions and a lock per aggregate;
- a transaction id assigned at a first write that may precede the lock;
- a position drawn after the lock;
- snapshots as the set of committed transactions;
- the release rule as a parameter, `xmin` or `epoch`.

Its invariants:
- `VersionOrder` — one stream's versions first handed on in order;
- `NoPassedOver`, over `delivered`;
- `NoHeadOfLineBlocking` — one transaction never ending, the others' rows
  still handed on.

Configurations that must fail:
- today's rule (`VersionOrder`, the measured trace; and
  `NoHeadOfLineBlocking`);
- an epoch ordered by transaction id;
- an epoch's membership by a range of ids;
- the in-progress list of `delivered` ignored (a row lost);
- decision 5 broken;
- a sequence cache above 1.

`OrderPerUri` and `EndToEndOrder`, today defined by the implementation's own
key, are restated over versions. `TraceOutbox.tla` and `trace2tla.py` follow
the observer's epoch.

**The library's tests.**
- The measured interleaving: the earlier version handed on first (red
  today).
- A third transaction open between the two.
- A transaction holding an id in another database of the server: the others'
  rows still handed on (red today).
- An epoch larger than a batch: neither a row lost nor one handed on twice.
- A transaction open across an epoch's start, committed after it.
- `migrate` with a sequence cache of 2: refused.
- The positions' migration from `(L, off)`: nothing lost.
- The old relay refused on the new table.
- The batch's plan uses `(slot, position)`.

**The host.** Two requests for one Device's projection from two causes, the
first held at a barrier after its slot's claim: its projections leave in
version order (red today).

## Open measurements

- **The new index.** A publish's latency and WAL with and without `(slot,
  position)`, against 4.76 ms today.
- **The epoch's cost.** Its batch with the index at a million rows, and with a
  long transaction's epoch of a hundred thousand.
- **The drill.** The relay's lag — an outbox row's `created_at` to the
  inbox's receipt — today against epochs, at eight loops.
- **An inventory of the host's outbox writers** by stream type, marking those
  that write against decision 5.

## References

- ADR-0006, ADR-0007, ADR-0008, ADR-0030, ADR-0033, ADR-0034.
- `crates/outbox/src/pg/store.rs` (`fetch`, `ack`, `publish`),
  `crates/outbox/src/pg/dispatch.rs` (`set_position`),
  `crates/inbox/src/pg/store.rs` (`take`), `crates/bus/src/identity.rs`.
- `verify/tla/Outbox.tla`, `verify/tla/Bridge.tla`, `verify/tla/TraceOutbox.tla`.
- Oskar Dudycz, "Ordering in Postgres Outbox" (event-driven.io).
- PgQ (Skytools): `pgq.batch_event_sql`, `pgq.next_batch`.
- PostgreSQL 14: `CREATE SEQUENCE`; Functions and Operators, Transaction ID
  and Snapshot Information; `src/backend/access/transam/xact.c`.

## Change Log

- 2026-11-03 — Proposed: the relay releases by snapshot epochs, ordered by the
  position drawn, the sequence's cache held at 1; producers owe writing a
  stream's message in its version's transaction; consumers defend by kind.
  A review by roles is owed before implementation.
