# ADR-0033: A row the bus no longer needs goes, by the range on the outbox and by the row on the inbox

## Status

Proposed (2026-11-03), not implemented. Decided in three rounds:
- **A blind solver and a discriminator.** The solver worked from the problem
  alone. The discriminator was given the solver's answer and the owner's
  direction: rotation over deletion for an append-only log, and a window for
  what lives only for a window. The author synthesised the two against the
  code and PostgreSQL 14.24.
- **Measurement of the two forks the first draft left open.** The outbox's
  mechanism was decided for the ring, and its size for eight generations
  (§ Context).
- **Refutation from four roles, each from its own position: database,
  distributed systems, operability and verifiability.** No finding refuted
  either mechanism. Their findings are taken into the decisions below, each
  with its executor. One finding was confirmed by measurement and withdrew
  the first draft's per-stream record (§ Context, order on the bus).

The partition and lock behaviour below was measured on a scratch PostgreSQL
14.24. § Executors lists what will hold each decision.

## Context

**Nothing removes a row.** The outbox keeps every message it relayed and the
inbox every message it processed. The outbox's README leaves retention to the
deployment, "rotated by partitions". No deployment does, and none could
without the knowledge of § Decision 3.

Measured in the host project, on its drill of one service:
- at 100 000 Devices of one Tenant, five waves left 1.2 million outbox rows
  (1 747 MB) and 503 000 inbox rows (880 MB) of a 2 976 MB database;
- a run at 1 000 000 Devices was at 7.3 GB after its second wave;
- there, the inbox's primary key took 1 345 MB, about 0.85 KB a row: the
  identity carries the stream id as JSON, and the stream ids of Commands to
  self are digests, so its inserts land at random;
- Commands to self were four fifths or more of the inbox's rows;
- the host's launch is several such Tenants per service under continuous
  traffic.

**What a kept row is still for.**

*The outbox:*
- (O1) A row is read until every position of its slot has passed it
  (ADR-0007; `NoPassedOver`, `verify/tla/Outbox.tla`).
- (O2) A new consumer group starts at `(0, '0')`, so it reads all history.
- (O3) `set_position` rewinds to replay (ADR-0005 decision 4).
- (O4) `message_id` is unique in the table. The README promises consumers a
  `message_id` to de-duplicate on; the library's own inbox de-duplicates by
  identity instead.

The guard of one message per identity per transaction (ADR-0030) does not
need old rows: its key leads with the writer's transaction.

*The inbox:*
- (I1) The identity is the primary key, and `ON CONFLICT DO NOTHING` is the
  de-duplication (`EffectsOnce`, `verify/tla/Inbox.tla`).
- (I2) On a conflict, the inbox reports the holder's `message_id`
  (ADR-0030 decision 4).
- (I3) `message_id` is unique. A collision is an error that its source
  retries without end.
- (I4) A causal dependency is "processed" only if its row is found
  processed (ADR-0006, ADR-0009). Otherwise the dependent waits and is
  parked after `max_wait`, as a dependency that never arrived.
  - Such dependencies live as long as their target. In the host, every
    assignment of an Application depends on the Application's creation at
    position 1.
  - Every dependency is checked again each time a dependent is taken. A
    dependent woken from one dependency waits again for another whose row
    is gone, and the same happens to one examined late behind a backlog, or
    unparked after its target went.
- (I5) A republication repeats an identity with a new `message_id`, at any
  later time (ADR-0030).
- (I6) A mark needs its row.
- (I7) Parked and waiting rows wait for an operator or a dependency
  (ADR-0005, ADR-0006).

**Order on the bus.** The relay hands rows on in `(transaction_id,
position)` order (ADR-0007). A transaction's id is assigned at its first
write, not at its commit or its read of an aggregate. The inbox's dispatch
writes first: `take` updates its slot's row before the handler loads
anything. So two dispatches can write one stream in the reverse of their ids.
Measured on the scratch server:
1. Transaction A took its slot (id 10222) and waited.
2. Transaction B (id 10223) took another, wrote version 5 of an aggregate and
   its outbox row, and committed.
3. A then wrote version 6.
4. The relay's order was version 6, then version 5.

The library delivers in that order end to end (`EndToEndOrder`), so a
stream's messages can arrive out of their positions' order. Any record that
infers "every position up to n was seen" from a forgotten position n takes
such a late arrival for a repeat.

**How long each obligation lives.** Each ends by one of three signals:
- *an event in the same database* — every position past the row; the outbox
  row that carried a Command to self reclaimed, so that no rewind can carry
  it again;
- *a window* — the longest a repeat can still reach the inbox: a broker's
  whole retention, its tiers included, plus the time a segment outlives
  `retention.ms`; a producer's own rewind of its outbox; a republication;
- *never* — a dependency target; protection against a stale replay of a
  stream the domain does not version.

A unit of storage can go when every obligation of every row in it has
ended. A range is that unit only where the rows of a range end together:
where the storage order is the obligation's order.

**The constraints.**
- The service's role is neither superuser nor `BYPASSRLS`, and owns no
  extension.
- The host deploys to four orchestrators: ECS, Kubernetes, Docker Swarm, and
  systemd with Podman on an air-gapped host. They share no scheduler, and
  one of the four has none.
- Everything the library runs is transaction-scoped (ADR-0029).
- The inbox's cut is fixed for a table's life (ADR-0008), but recreating the
  table is a migration, not a restart.
- Transaction ids are the cluster's, every database's: a burst in one
  context's database shortens a span counted in ids for all of them.

**Measured on PostgreSQL 14.24** (scratch, 2026-11-03):
- A `STORED` generated column cannot be a partition key. A plain column
  whose `DEFAULT` reads `pg_current_xact_id()` can, and a `CHECK` over the
  row's own `transaction_id` holds it: the expression is immutable.
- On the partitioned parent, these all build and hold, each including the
  partition key:
  - a primary key;
  - ADR-0030's unique expression index — a second message under one identity
    in one transaction is still refused;
  - a unique `message_id` index.

  A violation names the leaf index, which the server names by itself unless
  told: ADR-0030's typed refusal, which matches the guard's name, would see
  a generic error.
- `TRUNCATE` of one partition locks that partition and its indexes, not the
  parent.
- An `INSERT` through the parent locks the parent and its target partition
  only.
- A fetch that names no generations takes `ACCESS SHARE` on every partition
  and its indexes, 34 relation locks over eight. Naming them as literal
  constants, or as a parameter of a custom plan, locks the named ones only
  (10 for two). A generic plan, or names computed in the statement, lock all
  of them.
- Naming the generations by the ring's arithmetic, from the position's to
  the current one, loses rows once a group lags a full lap. A position in
  generation 7, the current transaction a lap on in generation 7 again: the
  next row in generation 7 was 1 640 001 rows past the next row of the
  table.
- The migration runner (ADR-0029) counts the partitions its lock on the
  parent reaches as other tables, and refuses the migration.

Measured on the scratch server, the outbox's table as the library makes it
beside the same table as a ring, 2 million rows each, about 1.3 KB a row:

| One million rows reclaimed | Time | WAL | Full-page images |
| --- | --- | --- | --- |
| `DELETE` below the floor, 1 000 a transaction | 25.7 s | 965 MB | 125 125 |
| then `VACUUM` | 50.0 s | 246 MB | 160 328 |
| `TRUNCATE` of the eight generations that held them | 0.5 s | 92 kB | 51 |

- Inserting the same million wrote 1 321 MB: deleting them, with their
  vacuum, writes about nine tenths as much again. The space a vacuum frees
  is reused: a million more rows grew the table by 128 MB, its indexes.
- The relay's fetch of 100 rows past a position, planned and executed:
  0.9 ms on the table as it is, 5.6 ms over sixteen generations, 2.6 ms
  over eight, every partition planned; with two generations named,
  planning 0.28 ms. Against the relay's batch, 100 to 300 ms of handing
  rows on, even the unpruned eight add about one per cent.
- A publish — one row, its own transaction — took 4.76 ms on the table and
  4.78 ms on the ring.

Measured on the host's drill:
- The outbox is insert-only.
- The inbox's mark is never a HOT update (0 of 1.6 million): the head index's
  predicate covers the marked columns. So every processed row already leaves
  one dead tuple for vacuum.

## Decision

1. **Rows go; smaller indexes do not replace that.** A shorter key, ids
   ordered by time, or partial indexes shrink what an insert touches, but
   bound neither the heap nor a backup, nor the restore it sets. A partial
   index cannot say "recent": its predicate must be immutable.

2. **The unit follows the obligation.** The outbox reclaims ranges, because
   its order — the transaction id, then the position — is its obligation's
   order. The inbox reclaims rows, because one range of it holds obligations
   of minutes, of weeks and of forever.

3. **The outbox is a ring of generations, emptied whole, in order.**
   - **The table.** It is partitioned `BY LIST (generation)` into `K`
     partitions, all made by the component's migration, none ever after.
     - `generation` is a plain column. By default it is
       `(pg_current_xact_id()::text::bigint / R) % K`, one value for every
       row of a transaction (the top-level id, savepoints included).
     - A `CHECK` holds it to the same function of the row's own
       `transaction_id`. A row written with another generation is refused,
       and so is a migration's copy that lets the default stamp old rows.
     - `K` and `R` are recorded in `{outbox}_meta` beside the cut. A process
       configured otherwise is refused at start, as a cut is (ADR-0008).
     - `K` is eight and `R` 2²³ by default: a generation spans about 84
       minutes at the host's measured 1 654 ids a second, a lap about eleven
       hours. `K·R` and the margin stay below `autovacuum_freeze_max_age`, so
       no partition needs a freezing vacuum before it is emptied.
     - The migration names every leaf index, so that ADR-0030's refusal still
       recognises the guard.
   - **The keys.** The primary key becomes `(generation, transaction_id,
     position)` and ADR-0030's guard `(generation, transaction_id, digest)`.
     Neither is weaker, since the generation is a function of the
     transaction. `message_id` is unique within a generation.
   - **The floor.** `{outbox}_meta` records the floor: the absolute window
     (`transaction_id / R`) below which every row is gone. Generations are
     emptied strictly in order from the floor. A new group starts at the
     floor. `set_position` below it is refused, with a typed error naming
     it, so "back to zero" means back to the floor. It reads the floor and
     moves the positions in one transaction, under the reclamation's
     advisory lock, so that a truncation cannot commit between them.
   - **The fetch names its generations.**
     - The windows from the floor to the current transaction's, counted
       absolutely; all `K` when they span `K − 1` or more.
     - Computed before the statement and passed as constants, so that the
       planner prunes and locks the named partitions alone.
     - Named from the floor, which only moves forward, so a stale reading
       names a superset, and no rewind goes below what is named.
   - **Reclamation.** The dispatcher works it between its batches, at most
     once an interval, in a `READ COMMITTED` transaction of its own:
     1. `pg_try_advisory_xact_lock` on the outbox's reclamation; another
        holder means this pass is skipped.
     2. The floor's generation is checked without its partition's lock:
        - closed — no transaction that could still write into it is open:
          the ids from the snapshot's `xmin` to the next meet none of its
          windows;
        - passed — every selection's position, in every slot, is past the
          generation's last row of that slot that matches the selection;
        - old enough — `margin` generations newer than it exist, and its
          newest `created_at` is older than `margin_age`, so that a row
          relayed a moment ago can still be re-injected whatever the
          cluster's rate of ids.
     3. Only then the partition's lock, under `SET LOCAL lock_timeout`. The
        conditions are checked again under it; then `TRUNCATE`, and the floor
        moves one generation.

     A lock not granted, a statement cancelled, or any other error of the
     reclamation is reported and retried at the next interval. It never
     stops, delays or restarts dispatch.
   - **Nothing is made ahead.** A generation that cannot be emptied — a
     lagging or abandoned group, maintenance not running — takes the next
     lap's rows as well. The table grows, as today; no insert is ever
     refused for want of a partition.
   - **A counter reset.** A position whose transaction id is at or above the
     current snapshot's `xmax` means the database was moved by dump and
     restore or by logical replication, and its ids restarted. Relay and
     reclamation both stop, and report it. No row is passed over and no
     generation emptied until an operator drains, truncates and resets the
     positions.
   - **Groups that pin the floor.** The group pinning the floor is reported:
     its name, its last acknowledgement, the generations it pins.
     `forget_group` drops an abandoned group's positions, and refuses one
     acknowledged within the margin.

4. **The inbox forgets a processed row by its channel's rule.**
   - **The mark stores `processed_at`** (`statement_timestamp()`). The sweep
     computes expiry from the rule in force when it runs, so a changed rule
     applies to rows already processed, and rows processed before any rule
     are covered.
   - **The rules.** A host declares one per channel (the destination without
     its key) in `InboxRetention`:
     - **Its carrier reclaimed** — for the inbox channel a service's own outbox
       relays to: Commands to self. The relay stamps each such row with the
       `(transaction_id, position)` of the outbox row that carried it. The
       sweep forgets the row once that position lies below the outbox's
       floor, a comparison within one database. No rewind reaches below the
       floor, so no Command to self is carried again after its row went.
     - **A window**, from `processed_at`. A host sets it at least to the
       longest a repeat can reach the inbox: the broker's whole retention,
       tiers included, the time a segment outlives it, and the producer's
       replay horizon. A producer on this library bounds its own horizon by
       its ring's span.
     - **Keep** — the default:
       - a channel without a rule;
       - a channel whose stream types any message names as causal
         dependencies;
       - a channel whose re-consumption a host cannot bound.
   - **The sweep.** A loop of the library deletes forgotten rows in small
     batches:
     - `FOR UPDATE SKIP LOCKED`, under `SET LOCAL lock_timeout` and a try
       advisory lock on the inbox's sweep;
     - never an unprocessed, parked or waiting row, nor a processed row that
       a waiting row names;
     - a keyset lower bound on what it has swept, so that a held vacuum
       horizon does not make it re-walk dead index entries;
     - nothing deleted for a grace period after its process starts, and after
       any interval in which it could not reach the database, so that intakes
       and relays rejoining after an outage redeliver first;
     - its errors are reported and retried, and never touch the inbox's
       loops.
   - **What de-duplication then means.** A repeat that arrives after its row
     went is processed again: that is the rule's meaning. The holder read of
     ADR-0030 may race the sweep and find no holder. It reports an unknown
     holder, not none.

5. **The library does it, with no extension.**
   - **pg_partman.** Installing it needs a privileged role. Its retention is
     by a partition's age or id, not the bus's predicate of decision 3. And
     it is not in the stand's image.
   - **pg_pathman.** It is in maintenance mode, needs
     `shared_preload_libraries`, and supports PostgreSQL 14 only with a
     patched core.
   - **pg_cron or orchestrator jobs.** pg_cron needs preload and superuser.
     The four orchestrators share no scheduler. And the invariants would live
     outside the library that holds them.

6. **The migration runner counts a partition as its parent's.** A migration
   of the outbox locks its parent, and the lock reaches every partition.
   ADR-0029's after-check counts a partition and its indexes as the target
   (`pg_partition_root`), not as a second table.

7. **What an operator sees.** The observer reports:
   - a generation reclaimed (its window, its rows, the floor);
   - a pass skipped, with why: a group pinning it and since when, a reader
     holding the partition, the margin, not closed;
   - rows swept, and the oldest expiry left;
   - the replay horizon: the `created_at` of the oldest kept row.

   Logs, rate-limited:
   - a warning for a floor pinned over a number of generations;
   - a warning for a sweep behind its expiries by more than a bound;
   - an error for a counter reset.

   Routing them to the host's alarms is the host's.

## Consequences

- The outbox's README changes: "unique in the table" becomes "unique within
  a generation", and "retention is the deployment's" becomes decision 3.
- The inbox's README changes: "a later arrival is still a duplicate" now
  holds within the channel's rule.
- **A host's work:**
  - It declares a rule per inbox channel. A channel left without one keeps
    its rows, as today.
  - Its own gates hold the rules: a window covers the channel class's
    re-consumption horizon, and no channel whose stream types are dependency
    targets has a window.
- One more session per process, for the sweep.
- **Migrations.**
  - The outbox is recreated as the ring by a migration (ADR-0029). The
    migration keeps every row's `transaction_id` and position, continues the
    position sequence, and stamps each row's own generation. It copies under
    the table's lock, so a database whose history must be kept copies only
    rows past its live groups' positions, or drains first.
  - The inbox gains `processed_at`. Its old processed rows carry none, so
    they are kept until a one-time pass stamps them.
  - Stands made before are recreated.
- **The rollback boundary is the ring's migration.** A binary from before it
  runs on the ring — it relays, unpruned, and reclaims nothing — but the
  table does not go back.
- **The host's runbook for a database moved by dump and restore:** drain,
  truncate, reset the positions (decision 3, a counter reset).
- `idle_in_transaction_session_timeout` for the service's role belongs to the
  host: a transaction left open holds back every vacuum, deletes included.
- **Outside this decision, found by it: a stream's messages can leave the
  outbox out of their positions' order** (§ Context, order on the bus). The
  library promises arrival order and transaction-id order, not a stream's.
  A consumer whose state follows a stream's versions must compare them; one
  that does not can be set back by a reordering. Decided in ADR-0034: the
  order stated as transaction-id order, a message's part beside its stream,
  and the inbox's slot by the aggregate a message addresses.

## Alternatives Considered

- **Keep every row and shrink the indexes.** The heap, the backups and the
  restore still grow; decision 1.
- **Partitions by time, made ahead, by pg_partman or by hand.** The
  owner's first form.
  - A runway of partitions made ahead stops every publishing transaction
    when maintenance stops. A burst of transaction ids elsewhere in the
    cluster can exhaust a runway counted in ids.
  - `DETACH … CONCURRENTLY` cannot run in a transaction block, so it cannot
    carry a transaction-scoped `lock_timeout`. Its second phase queues
    behind any long reader.
  - On the inbox, a unique key must include the partition key, so a repeat
    in another partition is not refused.
  - (A key of `CURRENT_TIMESTAMP` is one value per transaction and keeps
    ADR-0030's guard; `clock_timestamp()` would not. The first draft argued
    from the second.)
- **A sweep that deletes on both tables** (the solver's answer). On the
  outbox, an insert-only log, it turns every row into a dead tuple and
  makes vacuum pass every index, two of them random-keyed. Measured
  (§ Context): deleting a million rows and vacuuming them writes about
  1.2 GB of WAL and takes 76 s; truncating their generations, 92 kB and
  0.5 s. The first draft kept it as decision 3's fallback, to be taken if
  deleting wrote under a fifth of the inserts' WAL; it wrote nine tenths.
- **A record per stream** (`<inbox>_streams (…, through)`, the first draft's
  answer for channels "ordered per stream"): one row per stream, advanced to
  each forgotten position, answering de-duplication and dependencies for the
  stream's life.
  - Withdrawn. No channel fed by this library is ordered per stream
    (§ Context, order on the bus), so it would take a late lower position for
    a repeat and drop a first arrival.
  - Advanced only over a gapless prefix it is safe, but it compacts nothing
    past the first position a channel does not carry, and channels carry a
    subset of a stream's versions.
  - Kept for a reordering window, it is unsafe: a transaction may stay open
    for as long as it likes.
  - It would also make a stream re-created under the same id lose every
    message for good.
- **A window for Commands to self.** A Kafka outage keeps a mixed batch
  retrying, each retry inserting its Command to self again, and an
  operator's rewind of the relay carries Commands again. Both outlive any
  window that is not tied to the outbox's floor. Their rule is the carrier's
  reclamation (decision 4).
- **De-duplication by a trigger that looks back over a window** (the
  owner's earlier work on a partitioned journal). Under READ COMMITTED, two
  concurrent inserts of one identity each miss the other's uncommitted row,
  and both commit. A Kafka rebalance does deliver one message twice at once.
  It holds with an advisory lock per identity, and still only within its
  window.
- **A record of forgotten identities' digests.** About fifty times smaller,
  and still growing forever.
- **Deleting in the relay's or the mark's own transaction.** It adds work to
  the relay, which bounds the host's fan-out. It also breaks other groups and
  replay, and leaves the inbox no memory even for the uncommitted window.
- **Locking the partition before checking it** (the first draft). While a
  group lags, the check fails each time, and each attempt's queued exclusive
  lock holds back the fetches of that generation: catching up slows exactly
  when it matters.

## Executors

Owed, with the implementation.

**The library's tests.** They run at small `R` and `K` (`R` a few ids, `K` two
or three), two groups and two slots. Each asserts an invariant, not which
generation went, because parallel tests move the cluster's ids. Each refusal
test holds the other conditions true. Concurrency is forced and the wait
confirmed in `pg_locks`, as `verify/pg/lost-wake` does.
- **The outbox.**
  - The `CHECK` refuses a row whose generation is not its transaction's; the
    migration's copy keeps every row's own generation.
  - A process with another `R` or `K` is refused at start.
  - A group lagging a full lap receives every message.
  - A rewind racing a fetch: every row delivered.
  - Generations go in order; a rewind racing a truncation never lands below
    the floor; "back to zero" lands on the floor.
  - A truncation is refused when it is not closed, not passed (a second group
    lagging), or not old enough.
  - A reclamation's error or lock timeout never stops dispatch.
  - A fetch locks the partitions it names and no others.
  - ADR-0030's refusal is named on the ring (the existing test, run on it).
  - A later migration of the ring is accepted by the runner.
  - A position above the snapshot's `xmax` stops relay and reclamation.
  - A full lap without reclamation refuses no insert.
- **The inbox.**
  - Unprocessed, parked, waiting and kept rows, and rows a waiting row names,
    are never deleted.
  - Within a window a repeat is ignored; after it, it is processed.
  - A Command to self is forgotten only after its carrier is reclaimed, and
    a rewind within the floor finds its row and drops the repeat.
  - A changed rule applies to rows already processed.
  - The grace is re-armed after an outage of the database.

**TLA+.**
- **`Outbox.tla` gains:**
  - generations at small `R` and `K` that lap;
  - a reclaimed set that the fetch skips;
  - `Reclaim(g)` with its conditions as switches;
  - a `Rewind` that checks the floor;
  - two groups.
- **Its invariants:**
  - `NoPositionBelowGone`;
  - `ReclaimedWasReceived` for every group.
- **Configurations that must fail:**
  - "passed" dropped;
  - the fetch's arc by the ring's arithmetic;
  - one group checked;
  - a rewind racing a reclamation.
- **`Inbox.tla` gains:**
  - re-arrival after `Forget`;
  - a ghost of everything ever processed, over which `WaitingIsAside` and
    `CausalOrder` are restated.
- **Its configurations that must fail:**
  - forgetting early;
  - forgetting a dependency target.
- **`Bridge.tla`:** an outbox `Rewind` with an inbox `Forget` at the mark must
  fail `EffectsOnce`, and hold with the carrier's rule.

**Traces.** The observer's `reclaimed` and `forgotten` events, mapped by
`trace2tla.py`, and producers' commits logged, so that a lost row is not
explained away as an aborted transaction.

**The host's gates** (owed in the host):
- every inbox channel has a rule;
- a window covers its channel class's re-consumption horizon;
- a channel whose stream types are dependency targets keeps its rows.

## Open measurements

On a scratch database, each with the result that changes the decision:
- **M5.** The inbox's primary key as a 16-byte digest of the identity:
  adopted if the index shrinks more than tenfold with no slower publish,
  mark or dependency lookup.
- **M6.** The relay writing its own inbox's rows in its own transaction,
  against today's about 680 rows a second; it would also end the mixed
  batch's re-inserts.
- **M8.** The inbox at 410 inserts, marks and deletes a second for an hour,
  and again with the vacuum horizon held for ten minutes: whether dead
  tuples and size level off.
- **M9.** The ring's migration copying 5 million rows: how long publishers
  wait.
- **M10.** The cluster's rate of transaction ids at the launch's load, every
  context's database on one server: the margin in generations against
  `margin_age`.

## References

- ADR-0003, ADR-0005, ADR-0006, ADR-0007, ADR-0008, ADR-0009, ADR-0010,
  ADR-0029, ADR-0030.
- `crates/outbox/README.md`, `crates/inbox/README.md`.
- `verify/tla/Outbox.tla`, `verify/tla/Inbox.tla`, `verify/tla/Bridge.tla`.
- PostgreSQL 14 documentation: 5.11 Table Partitioning; `ALTER TABLE`
  (`ATTACH`, `DETACH … CONCURRENTLY`); `TRUNCATE`; 73.7 Heap-Only Tuples.

## Change Log

- 2026-11-03 — Proposed: the outbox as a ring of generations emptied whole,
  the inbox's rows forgotten by a per-channel rule, with a stream record for
  channels ordered per stream. Two forks are left to measurements.
- 2026-11-03 — The forks measured: deleting a million outbox rows and
  vacuuming them writes about 1.2 GB of WAL, truncating their generations
  92 kB. The ring is decided and the fallback dropped: eight generations,
  not sixteen, the fetch's planning halved and pruned to the generations it
  needs.
- 2026-11-03 — Refuted from four roles (database, distributed systems,
  operability, verifiability); every finding taken in:
  - **The ring:**
    - the generation held by a `CHECK`;
    - `R` and `K` pinned;
    - a floor emptied in order and serialised with rewinds;
    - the fetch named from the floor in absolute windows, all of them past a
      lap;
    - conditions checked before the lock;
    - a time margin beside the count;
    - a counter-reset guard;
    - named leaf indexes;
    - the runner made to see partitions.
  - **The inbox:**
    - `processed_at`, the expiry computed at sweep time;
    - Commands to self forgotten once their carrier is reclaimed;
    - windows over the re-consumption horizon;
    - dependency targets kept;
    - the grace re-armed after outages.
  - **Withdrawn:** the stream record, on a measurement of the relay emitting a
    stream out of order. That order is recorded as a finding of its own.
