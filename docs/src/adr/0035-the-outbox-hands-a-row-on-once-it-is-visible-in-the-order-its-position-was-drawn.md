# ADR-0035: The outbox hands a row on once it is visible, in the order its position was drawn

## Status

Proposed (2026-11-03), not implemented. Decided in two rounds:
- **A blind solver and a discriminator.** The solver worked from the problem
  alone. The discriminator checked the proof against PostgreSQL 14's source
  and the batch rule against PgQ's (Skytools).
- **A review by five roles,** each refuting from its own position: database
  with performance, distributed systems, evolution, operability,
  verifiability. None refuted the release rule. Each found, on its own, that
  the first draft's fetch, migration, rewind or idle path would lose rows,
  fail to start or write on every look. Their findings are taken in below.

It supersedes the order ADR-0007 gives the relay. It leaves ADR-0034 the
other half of the problem: which messages an inbox processes together. It
lands with ADR-0033's amendment (§ Consequences).

## Context

**The relay's rule today** (ADR-0007; the design of Oskar Dudycz's
"Ordering in Postgres Outbox").
- A row carries `pg_current_xact_id()` of the transaction that wrote it.
- A dispatcher hands on the rows past its position whose transaction id is
  below the snapshot's `xmin`, in `(transaction_id, position)` order, in one
  statement that also locks the position row (`crates/outbox/src/pg/store.rs`,
  `fetch`).
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

**Commit order is readable by an ordinary role.**
- `pg_current_snapshot()`, `pg_visible_in_snapshot()` and
  `pg_snapshot_xmin/xmax/xip` are executable by `PUBLIC` (measured).
- Logical decoding would also give commit order, but needs the `REPLICATION`
  attribute and `wal_level = logical`, which a service's role and some hosts
  cannot have.
- PgQ has released batches by snapshot visibility for about fifteen years: a
  batch is what is visible in this tick's snapshot and was not in the last
  one's, ordered by the event's id (`pgq.batch_event_sql`).

**Why visibility and a draw order give a stream's versions in order.** Let
T1 write version v of a stream S and T2 version w > v. Then:
- **T2 read T1's state.** T2 wrote w only after reading S at v or later —
  after T1 was visible. An optimistic write succeeds only on the latest read
  (it re-checks its predicate on the newer row), and a locking read waits for
  T1.
- **T2 drew later.** T1 drew its rows' positions before its commit; T2 draws
  its rows of S after that read (decision 7). With a sequence of cache 1,
  `nextval` draws in real time across sessions (PostgreSQL's `CREATE
  SEQUENCE`: "with a cache setting of one it is safe to assume that nextval
  values are generated sequentially"). So T1's positions are lower.
- **A snapshot that sees T2 sees T1.** PostgreSQL 14's `CommitTransaction`
  leaves the ProcArray before it releases its locks (`xact.c`), so a waiter
  wakes only after the committer is visible. A snapshot lists top-level
  transactions only, and `pg_current_xact_id()` is the top-level id, so
  subtransactions do not matter.
- **Hence** T1's rows are released in an epoch no later than T2's. Within one
  epoch they go in position order, so T1's go first.

**What the proof needs of the statements.** A row's membership in an epoch
is judged against a stored snapshot; whether the row can be read at all is
judged by the reading statement's own snapshot. Today's rule tests both
against one snapshot, so a stale statement only shortens a batch. Two
snapshots can disagree:
- **A statement's snapshot predates the lock it takes.** Under `READ
  COMMITTED`, `FOR UPDATE` follows an updated row to its latest version
  (`nodeLockRows.c`). A dispatcher that waited, or merely reached the lock
  late, gets the epoch another stored after its snapshot was taken — and
  reads it with a snapshot that cannot see all of it. The library's inbox
  documents the same hazard above `take`.
- **A transaction's own id is visible in its own snapshot.** Measured: inside
  transaction 10241 the snapshot read `10241:10243:`, and
  `pg_visible_in_snapshot` of its own id was true (`procarray.c`: "we don't
  include our own XIDs").

**Measured** — on the host's drill (100 664 outbox rows, 752 transactions)
and on a scratch outbox of 4 million rows, about a thousand per transaction:

| Statement | Plan | Time |
| --- | --- | --- |
| Today's fetch of a batch (drill) | index scan | 0.19 ms |
| An epoch's start (drill) | parallel index-only scan | 17 ms |
| An epoch's start, 1 M rows | parallel; serial | 58–64 ms; ~145 ms |
| An epoch's batch without the position index (drill) | parallel scan and sort | 19–22 ms |
| One batch of 100 in a 1 M-row epoch, no position index | heap fetches, top-N sort | 270–282 ms |
| An epoch's start, 100 ids (100 000 rows) still in progress, their rows filtered one by one | index scan | 42.5 ms |
| The same, the finished ids worked out from the two snapshots first | index scan | 0.42 ms |
| The same as one `OR` | lower bound dropped, 1 M rows filtered | 219 ms |
| Walking 1 M delivered rows to an epoch's next row | heap, cold; warm | 2.66 s; 0.31 s |
| The same, every page all-visible | index-only | 68 ms |

- **A statement that locks rows plans no parallel workers.**
- **A snapshot grows with the server's running transactions:** 824 bytes at
  100 ids, 2 024 at 250, 8 024 at 1 000. Two of them push a position row
  past the TOAST threshold at about 120 running transactions.
- **A row lock assigns a transaction id.** After `FOR UPDATE SKIP LOCKED`,
  `pg_current_xact_id_if_assigned()` read 10251; after a plain read, nothing.
- **`'0:0:'::pg_snapshot` is invalid input;** `'1:1:'` is valid and shows
  nothing.
- **`pg_snapshot` has no operators,** not even `=`.
- **The drill's position sequence has a cache of 1.**

## Decision

1. **A dispatcher releases rows by epochs.** Per `(consumer group, uri,
   slot)`, the position row holds:
   - `delivered` — a snapshot; every row visible in it has been handed on;
   - `epoch` — the snapshot being handed on, or none;
   - `epoch_offset` — the last position handed on within it;
   - `epoch_end` — the highest position of it;
   - `epoch_at` and `delivered_at` — when each was taken.

2. **A probe, then the lock, then the reads, each with its own snapshot.**
   - A look first asks, without a lock, whether the slot holds a row new
     since `delivered` — of its selection, or of any. A row lock assigns the
     transaction an id (§ Context), so a look that locked first would take
     one on every poll. A probe that read a `delivered` already moved costs
     one look more, nothing else.
   - Only then one statement takes the position row, `FOR UPDATE SKIP
     LOCKED`, as the inbox's `take` does (ADR-0008).
   - The epoch's start and each batch are later statements of the same
     transaction. Under `READ COMMITTED` each takes its snapshot after the
     lock, so every transaction of a stored epoch is visible to it.
   - The snapshots are bound as parameters, not read from the position row
     for every row walked, so a large one is not unpacked again and again.
   - The start and the batch apply the same selection (the uri and its
     prefix).

3. **An epoch starts with `epoch := pg_current_snapshot()`.** Its rows are
   the slot's rows new in it — visible in `epoch` and not in `delivered`.
   They are found as two branches joined by `UNION ALL`, not by `OR`:
   - ids from `delivered`'s `xmax` up to `epoch`'s `xmax`, through the
     existing `(slot, transaction_id, position)` index;
   - the ids of `delivered`'s in-progress list that are visible in `epoch`,
     worked out from the two snapshots before any row is read (PgQ's own
     filter).

   `epoch_offset` is set to their lowest position less one and `epoch_end`
   to their highest.

4. **A batch is the epoch's next rows by position.** It takes the slot's rows
   with `epoch_offset < position <= epoch_end`, visible in `epoch` and not in
   `delivered`, in position order, through a new index `(slot, position)
   INCLUDE (transaction_id)`. It picks positions through the index and reads
   the payloads by key; the index's included id spares the heap only that
   way. The batch is handed on in that order and `epoch_offset` moved to its
   last position, in the dispatcher's transaction, as today.
   - **An epoch drains on a batch shorter than its size,** an empty one
     included: `delivered := epoch`, `epoch := NULL`, in the same
     transaction. The batch was read with a snapshot that contains the
     epoch (decision 2), so no row of it was invisible there.
   - **A drain whose offset had not reached `epoch_end`** is reported: the
     epoch's last row was not found. Waiting for it would wedge the slot if
     that row were ever deleted by hand.

5. **An idle look writes nothing.** A look that finds no row new since
   `delivered` neither starts nor delivers an epoch, and takes no
   transaction id: it ends at decision 2's probe, a read, as today. Over an
   idle slot `delivered` is advanced at most once per interval, so that
   ADR-0033's generations pass.
   - **A look that finds rows new since `delivered`, none of its
     selection's,** takes the lock and starts its epoch as decision 3 does.
     Found empty, the epoch is delivered at once, so that the next look
     does not walk the same rows again. The write is paid only while
     another selection of the slot has work — ADR-0033's lanes, which share
     the table and read each other's rows once.

6. **The position sequence draws one at a time.**
   - `migrate` refuses an outbox whose `position` column owns no sequence,
     or one whose cache is not 1, before it applies any migration. The
     service's own role can repair it: `ALTER SEQUENCE … CACHE 1`.
   - Each epoch's start reads the cache again (`pg_sequence.seqcache`), so
     an `ALTER SEQUENCE` made while the service runs stops the relay with a
     report instead of reordering it.
   - A sequence reset backward is a defect the library does not repair.

7. **What a producer owes (P1).** Every writer of a stream's messages draws
   their positions after the point that serialises it with every other
   writer of that stream: its write of the version, its wait for the
   stream's lock, or its read of the predecessor's committed state.
   - A writer that draws first — writing its message, then waiting for the
     version row — can be overtaken in one epoch by a re-announcer holding
     the lock.
   - A producer that writes a stream's message from another transaction (a
     request relayed by a range of a fan-out, after the event it follows)
     gets no order for that stream. That is right where nothing depends on
     it.
   - **A dispatcher's or `set_position`'s transaction never writes to the
     outbox it relays.** Its own id is visible in its own snapshot, so such
     rows would count as delivered without being handed on. Writing an inbox
     there (ADR-0033) is not affected.

8. **What the library then promises.**
   - **It promises:**
     - each stream's messages on one URI are first handed on in their
       writers' commit order — for an aggregate's stream, its versions'
       order — whatever wrote them under decision 7: an inbox's loops,
       replicas, a request's handler, a transaction that writes several
       aggregates;
     - no committed row is passed over;
     - a transaction left open delays its own rows only.
   - **It does not promise:**
     - an order between streams; within an epoch they go by drawn position,
       and an epoch done again after a rolled-back batch may interleave them
       otherwise (that is ADR-0006's causal dependencies);
     - an order for messages written against decision 7;
     - an order after the outbox: a topic repartitioned, a replay past what
       the receiving inbox still holds, an inbox that forgot the processed
       row (ADR-0033), a parked or waiting inbox row that later ones pass;
     - anything against a suffix of the write-ahead log lost in an
       asynchronous failover (decision 9).

9. **What a consumer still does.** Every consumer deduplicates — the inbox
   does — and treats gaps as its channel's contract says. Beyond that, by
   kind:
   - **A consumer that keeps a stream's latest state** ignores a position at
     or below the one it applied. It is then indifferent to order, and safe
     against redelivery, a replay, an inbox that forgot.
   - **A consumer that applies transitions or merges deltas** cannot ignore
     an older position without losing it. Its channel is kept whole by the
     receiving inbox (ADR-0033, "keep"), or its contract tolerates a loss.
   - **Neither survives a producer that lost a committed suffix.** The
     relay may have handed on rows whose commits never reached the standby.
     The new primary writes its next change at a version a consumer already
     holds, and the inbox drops it as a repeat. A consumer cannot recover
     from that by itself; the producer recovers after the failover:
     - it republishes above any version a consumer may hold;
     - or the consumers' records of it are reset.

     Where a synchronous standby exists, an epoch's start is committed with
     `synchronous_commit = on`. The log is ordered, so every commit visible
     in the epoch is then on the standby before any of its rows leave.

10. **The positions move without loss, and an old relay keeps working.**
    - **New columns only.** No old column is reinterpreted: an old relay and
      a new one sharing a reused offset would each skip what the other
      handed on.
    - **The conversion.** `(L, off)` becomes `delivered = 'L:L:'`, `epoch =
      'L+1:L+1:'`, `epoch_offset = off`: the rest of transaction L, exactly,
      nothing handed on again. `epoch_end` is left unset and computed by the
      relay's next look, as at an epoch's start, so that the migration reads
      no other table (ADR-0029).
    - **A position never acknowledged** (`L = 0`) becomes `'1:1:'`, nothing
      delivered. So do the new column's default — an old binary inserts
      position rows naming only their key — and the "zero" of
      `set_position`.
    - **The old pair is kept up.** The new relay also writes `(L, off)` as
      `(pg_snapshot_xmin(delivered) − 1, i64::MAX)`: every transaction below
      `delivered`'s `xmin` is visible in it, so all its rows were handed on.
      An old relay still running in a rolling deploy, or a binary rolled
      back, hands on from there: duplicates, which identities drop, in the
      old order. The promise of decision 8 starts when the last old relay
      stops, not at the migration.
    - **A format in `{outbox}_meta`,** checked from this binary on, so that
      the next change of the positions' shape can refuse an older binary.
      It is read from the meta table, never from the position table, which a
      booting service may find locked.
    - **Separate migrations, in this order:**
      1. the position table's new columns and the conversion;
      2. the meta table's format;
      3. the index on the outbox, built by ADR-0033's ring when it recreates
         the table, so that publishers wait for one lock, not two.

11. **A rewind and a new group are snapshots.**
    - **`set_position` clears the epoch.** Otherwise the epoch in flight
      drains and stores `delivered := epoch`, undoing the rewind.
    - **A rewind to a transaction `F`** keeps of `delivered` only what lies
      below `F`: `'min(xmin, F) : min(xmax, F) : {in-progress ids below F}'`.
      `'F:F:'` alone would mark delivered every transaction below `F` still
      running, or still in the epoch.
    - **"Skip to now"** is `pg_current_snapshot()`. **"Zero"** is `'1:1:'`,
      or ADR-0033's `intact_from`.
    - **`Position` becomes the delivered snapshot.** It is no longer `Copy`.
      `positions()` then `set_position()` drops the epoch in flight, whose
      rows are handed on again.

12. **A counter reset stops the relay until an operator rebases it.**
    - **The trigger.** A position whose `delivered`'s `xmax` lies above the
      server's next transaction id: the database was moved by dump and
      restore, and its ids restarted.
    - **It sticks.** The refusal is recorded in `{outbox}_meta`, so that it
      still holds when the counter passes the old `xmax` — otherwise every
      row written meanwhile would count as delivered. An operator clears it.
    - **The library's rebase.** It re-stamps the undelivered rows under a
      current transaction and sets every `delivered` to the current
      snapshot. The restored rows would otherwise stay invisible until the
      counter passed their old ids.
    - **The host's runbook names it** for each deployment target.

13. **What an operator sees.**
    - **The observer reports:**
      - an epoch started — its rows, its range of positions;
      - an epoch drained, and one drained short of `epoch_end`;
      - `delivered` advanced over an idle slot;
      - a refused cache, a counter reset.
    - **`Fetched`** carries `delivered` and `epoch` as `xmin`, `xmax` and the
      in-progress list, `epoch_offset`, `epoch_end`, and the batch
      statement's own snapshot.
    - **`Published`** carries the stream's identity and version, so that a
      trace can check the order.
    - **`backlog(selection)`** returns, per slot, the epoch's age and its
      progress. `positions()`' minimum no longer gives a lag, since
      snapshots have no order; a row's `created_at`, its transaction's
      start, would alarm on a long producer whose rows left at once.
    - **A relay failure that persists across polls** is reported to the
      observer and logged at error, for the host's alarm. It does not reach
      readiness: a relay's fault must not take a service's interface out.

14. **Corrected where the library claimed more.**
    - **The inbox's order.** `crates/bus/src/identity.rs` and ADR-0030 say the
      inbox "orders a stream's messages by their positions". It orders a
      slot's messages by arrival.
    - **The outbox's own text,** restated by decision 8:
      - its README's "Order", "Observing" and fetch costs;
      - `pg/mod.rs` § "Ordering and visibility";
      - the docs of `message.rs`, `observer.rs` and `store.rs`'s `Batch`;
      - `verify/tla/README.md`;
      - ADR-0007's watermark.

## Consequences

- **The order a stream's consumer relies on is the producer's commit order,**
  however many loops or replicas the producer runs and whatever each writes
  first — once no relay of the old rule runs.
- **A server's open transaction no longer stops every relay on it.**
  ADR-0033's reclamation still reads the server's `xmin` for "closed", so the
  table still grows meanwhile; its amendment judges "closed" per database.
- **Each outbox row costs one more index entry,** about 36 bytes and 60–80
  bytes of WAL against the row's 1.3 KB (inferred; measurement owed).
- **A row is walked by every epoch whose range of positions spans it** —
  about a committer's time from draw to commit over an epoch's length. A
  long transaction's epoch re-walks what was written during its life, and
  the included id helps only on all-visible pages, which that transaction
  keeps vacuum from marking.
- **ADR-0033's ring is amended on top of this record,** in the same release:
  - a generation "passed" when every row of it is visible in every
    position's `delivered`, and "closed" judged per database;
  - the fetch bounding each named generation's scan — its first and last
    positions in the epoch, recorded at the epoch's start; each generation a
    branch of a `UNION ALL`, dropped once passed — rather than re-walking a
    passed generation on every batch (measured: 208 001 rows, 96.6 ms, for
    a generation that contributed nothing);
  - the ring's copy keeping the rows past the live positions, restated over
    `delivered` and the epoch;
  - the copied sequence owned by the new table, and the outbox's new index;
  - rewinds and new groups from `intact_from`, apart from `gone_below`;
  - the counter reset by decision 12;
  - its statement that an older binary relays on the ring, held by decision
    10's old pair;
  - its fetch costs measured again;
  - its Context on order, its withdrawn per-stream record, and its
    Consequences' reference to ADR-0034's first draft rewritten.
- **ADR-0034 lands after both,** on ADR-0033's migration runner and named
  leaf indexes.
- **The host's corrections.**
  - Six readers of the old position columns: the testkit's
    `outbox_progress`; the drill's `backlog` and `relay_behind`; the BDD's
    wait for the relay (`bundle_projection.rs`, `pending`); two stand drills
    computing lag. Each is rewritten over `backlog`. Left on the old pair,
    they would wait on the cluster's `xmin` — the stall this record removes.
  - Comments sized around "a transaction holds every relay's horizon",
    revisited: the broker's message timeout (the relay still holds its slot
    while it waits), the decision flush's samples, the Role retirement's
    batches, its ADR-0083's "not yet" item.
  - The claims that a Tenant's events keep their order
    (`crates/bus/src/announcement.rs`), or a Device's Commands.
  - The contract sentence that a withdrawal "always follows" its delivery:
    true once no old relay runs, under decision 7.
  - Its consumers sorted by decision 9: `DeviceContext` keeps no source
    position and must, or stays on "keep".
  - Its executor of this record uses a writer ADR-0034 does not serialise —
    a tile, a request's handler — or ADR-0034 alone would turn it green.

## Alternatives Considered

- **Today's rule, kept and documented** (ADR-0007; Dudycz's "Ordering in
  Postgres Outbox"). It loses nothing, but it orders by first write and
  stalls on any open transaction — the two limits its own article names.
- **A gapless sequence in a singleton table.** It serialises every writer on
  one row; rejected by the same article for the backbone of messaging.
- **Gap detection with a high-water mark and tombstones** (Marten). It has to
  tell a rolled-back gap from a slow writer by waiting, a timeout guessed for
  every server.
- **Logical decoding.** It gives commit order, out of a service role's reach
  and not carried across a physical failover in PostgreSQL 14.
- **Commit timestamps** (`track_commit_timestamp`). A server setting, off on
  the measured servers; wall-clock ties and steps break the order; and it
  still needs a visibility rule.
- **The batch re-sorted by the stream's position.** A third transaction whose
  id lies between two writers' releases the later version in one fetch and
  the earlier in the next.
- **The epoch read in the statement that locks** (the first draft: "as
  today"). It reads a stored epoch with an older snapshot. The rows committed
  in between are skipped, then marked delivered when the epoch drains.
- **The epoch's membership as one `OR`.** The planner dropped the range's
  lower bound and filtered every delivered row (219 ms at a million).
- **The drain when the offset reaches `epoch_end`** (the first draft).
  Correct under decision 2, but a row at `epoch_end` deleted by hand wedges
  the slot. A short batch drains as surely, a look sooner.
- **An empty epoch delivered at once** (the first draft). Every idle look
  becomes a write transaction: an id, a commit, a dead tuple — five a second
  per loop at the host's poll, where three idle services ran 62 read-only
  transactions a second (the host's ADR-0083).
- **The old offset reused as the epoch's offset.** An old relay and a new one
  sharing it each skip what the other handed on: rows lost.
- **The old columns dropped, so that an old relay fails.** Already-released
  binaries check nothing they could be refused by, and a failing relay is
  restarted with a warning forever: a rollback becomes a silent stop of the
  relay while the service accepts commands.
- **An inbox partition by the aggregate a message addresses, as the order's
  fix** (ADR-0034's first draft). It serialises only messages addressed to
  that aggregate in one inbox; a change's or a tile's transaction, a
  request's handler, another service still invert the stream. ADR-0034
  keeps that partition for isolating the processing of one aggregate.
- **The slot claimed without a transaction id** (an advisory lock). The
  handler's first row lock assigns one anyway, and a transaction writing two
  aggregates cannot put both writes first.
- **Only the consumer's comparison of positions.** Kept (decision 9), but
  alone it leaves every producer's streams inverted, and a consumer of
  transitions cannot defend.

## Executors

Owed, with the implementation, but for the model's first part.

**Built (2026-11-04): `verify/tla/OutboxPg.tla`,** the relay of this record
statement by statement, with its lanes and rewinds.
- **Must hold:** the decision, two dispatchers racing; rewinds; a
  transaction of another database open for ever; two lanes with the broker
  down for good.
- **Must fail:** every configuration of the table below, each with its
  property, and one selection of everything with the broker down,
  breaking `NoHeadOfLineBlocking`. `check.sh` greps each one.
- **Then the migration** — exact, from `L = 0` too, an old relay beside a new
  one on the old pair kept up — and the sequence's cache, with their own
  must-fail configurations: `'L:L:'` alone, `'L+1:L+1:'` alone, the epoch's
  offset in the old column, a cache of two.
- **Still owed in it:** a new group; ADR-0033's ring; and the refinement to
  `Outbox.tla`, once that module is restated over versions.

**A statement-level model.** `OutboxPg.tla`, refining the abstract
`Outbox.tla` as `InboxPg.tla` refines `Inbox.tla`. It models:
- snapshots as PostgreSQL computes them — `xmin`, `xmax`, the in-progress
  list, the own id left out — with `SnapshotSound` relating them to commits;
- a fetch as a snapshot taken, then a lock returning the latest position
  row;
- several dispatchers racing for one slot;
- aggregates with versions and a lock each; transactions of many rows; a
  re-announcer holding a stream's lock;
- `SetPosition`, `NewGroup` and `Migrate`, from `L = 0` too.

Its properties:
- `VersionOrder`; `NoPassedOver`; `NoRedeliveryWithoutFailure`, so that a
  duplicate with no batch rolled back is a violation; `DeliveredMonotone`;
  `EpochCoversDelivered`; `RewindReplays`;
- `NoHeadOfLineBlocking`, as liveness under fairness on the dispatchers
  alone.

Configurations that must fail, each with the property it violates:

| Configuration | Violates |
| --- | --- |
| today's rule | `VersionOrder`; `NoHeadOfLineBlocking` with one transaction left open |
| the epoch read in the locking statement | `NoPassedOver` |
| an epoch ordered by transaction id | `VersionOrder` |
| the in-progress list ignored | `NoPassedOver` |
| membership by a range of ids | `NoPassedOver` |
| the visibility filter dropped | `NoRedeliveryWithoutFailure` |
| that and `epoch_end` dropped | `VersionOrder` |
| a drain storing the current snapshot | `NoPassedOver` |
| the offset carried across epochs | `NoPassedOver` |
| `'F:F:'` above `xmin` | `NoPassedOver` |
| the epoch kept across a rewind | `RewindReplays` |
| a position drawn before the serialising point | `VersionOrder` |
| a sequence cache of 2, two sessions | `VersionOrder` |

`check.sh` greps each violation by name. `OrderPerUri` and `EndToEndOrder`,
today defined by the implementation's own key, are restated over versions.

**The gate.** The models and the trace checks join the library's `make all`.
Until they do, a mutation held by a model alone also gets a test.

**Traces.**
- `TraceOutbox.tla` and `trace2tla.py` follow the observer's epoch. A row
  visible in the epoch and not handed on is undecided until the epoch
  drains, then aborted.
- The recorded traces are recorded again.
- The forged `outbox-passed-over` trace, legal under epochs, is forged again
  as the in-progress list ignored.

**The library's tests,** each fixing the interleaving its mutation needs:
- the measured interleaving, dispatched once after both commit: the earlier
  version first (red today);
- a third transaction open between the two;
- an id held in another database of the server: the others' rows handed on
  (red today);
- a second dispatcher whose statement snapshot predates the first's stored
  epoch: nothing lost;
- a transaction X that draws first and stays open across an epoch's start,
  that epoch draining while X is open; X committing between two batches of
  a multi-batch epoch; a third transaction committing after: each row
  exactly once. A variant ends on X's commit;
- `migrate` with a cache of 2, and with no owned sequence: refused before any
  migration; a cache altered while running: the relay stops and reports;
- the conversion from a transaction `L` split by a batch, and from `L = 0`:
  nothing lost, nothing handed on again;
- the released fetch, kept as text, run against the converted table: an old
  relay hands on from the old pair;
- a rewind above `delivered`'s `xmin` while a lower transaction is open, and
  a rewind during an epoch: nothing lost, the rewound rows handed on again;
- an idle look: no transaction id taken, no row written;
- `a_failing_subscriber_rolls_the_batch_back`, kept;
- the batch's plan on `(slot, position)`, at a size where the planner would
  choose it.

**The host.**
- Two requests for one Device's projection — one from a tile — the first held
  at a barrier after its slot's claim: its projections leave in version order
  (red today).
- Decision 7's executor: a kernel test that a reaction writes the outbox only
  after the versioned store; each writer outside the kernel (republication,
  a sweep) with a test that it takes the stream's lock before it draws.

## Open measurements

- **The index at the upgrade.** Its build at the host's outbox size under the
  runner's lock, against the host's one-second write budget (its ADR-0083);
  a scan and sort alone took 6.7 s over 4 M rows. And a publish's latency
  and WAL with the index, against 4.76 ms.
- **The batch's walk in the drill at eight loops.** Rows removed per row
  handed on. Above about ten, or a relay slower than today's ~680 rows a
  second, changes the batch's shape — a merge over each transaction's
  positions.
- **A snapshot bound against one read from the position row,** over a
  100 000-row walk with 300 ids in progress.
- **A transaction holding a million uncommitted rows** while the relay polls:
  an epoch's start and a look.
- **The relay's lag** — an outbox row's `created_at` to the inbox's receipt —
  today against epochs.
- **An inventory of the host's outbox writers** against decision 7.
- **The model checker's time** with aggregates, locks and snapshots added.
  First measured: the decision with two dispatchers racing, about 6.6
  million states in two and a half minutes; liveness, minutes on tens of
  thousands of states. The ring is still to be added.

## References

- ADR-0006, ADR-0007, ADR-0008, ADR-0029, ADR-0030, ADR-0033, ADR-0034.
- `crates/outbox/src/pg/store.rs`, `crates/outbox/src/pg/dispatch.rs`,
  `crates/outbox/src/channel.rs`, `crates/inbox/src/pg/store.rs` (`take`),
  `crates/bus/src/identity.rs`, `crates/migration/src/lib.rs`.
- `verify/tla/Outbox.tla`, `verify/tla/InboxPg.tla`, `verify/tla/Bridge.tla`,
  `verify/tla/TraceOutbox.tla`, `verify/tla/check.sh`.
- Oskar Dudycz, "Ordering in Postgres Outbox" (event-driven.io).
- PgQ (Skytools): `pgq.batch_event_sql`, `pgq.next_batch`.
- PostgreSQL 14: `CREATE SEQUENCE`; Transaction ID and Snapshot Information;
  `xact.c`, `xid8funcs.c`, `procarray.c`, `nodeLockRows.c`, `sequence.c`.

## Change Log

- 2026-11-03 — Proposed: the relay releases by snapshot epochs, ordered by the
  position drawn, the sequence's cache held at 1; producers owe writing a
  stream's message in its version's transaction; consumers defend by kind.
- 2026-11-03 — Refuted from five roles; the release rule stands, its
  mechanism changes:
  - the lock taken by one statement, the epoch read by later ones with the
    snapshots bound;
  - membership by `UNION ALL`, the in-progress list filtered by the two
    snapshots;
  - drained by a short batch, a drain short of `epoch_end` reported;
  - an idle look that writes nothing;
  - the cache checked before migrating and at each epoch;
  - P1 stated as a draw after the serialising point; the dispatcher never
    writing its own outbox;
  - the positions moved by new columns only, the old pair kept up for old
    relays, `L = 0` as `'1:1:'`, the index built by ADR-0033's ring, a
    format in the meta table;
  - rewinds as intersections that clear the epoch;
  - a counter reset that sticks until a rebase;
  - the failover claim corrected, with the producer's recovery;
  - the observer's events, `backlog`, and an alarm on a persisting relay
    failure;
  - a statement-level model with paired must-fail configurations, on the
    library's gate.
- 2026-11-03 — ADR-0033's review of roles found a lock-first look taking a
  transaction id on every poll, against decision 5: a look now probes
  without a lock first. The ring's floor is ADR-0033's `intact_from`.
- 2026-11-04 — ADR-0033's lanes: a look that finds only another selection's
  rows new delivers its empty epoch at once, so that lanes sharing a table
  read each other's rows once, not on every poll.
