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
- **Amended on top of ADR-0035,** whose review of roles named what changes
  here. The relay's positions are snapshots now, so the fetch, "passed",
  the floor's forms, the copy and the counter reset are restated over them.
  "Closed" is judged per database. The two land in one release.
- **The amendment refuted from two roles, database and distributed
  systems.** Its rules stood; three of its mechanisms did not — one floor
  for two meanings, the counter reset's rebase, and a bound handed to a
  migration through the session. Their findings are taken in below.
- **Revised by the owner (2026-11-04):** the inbox's carrier rule withdrawn
  to the alternatives. Commands to self are held by a window, as every
  channel is, and their repeats are bounded where they arise.

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
- (O1) A row is read until it is visible in every position's `delivered`
  (ADR-0035; `NoPassedOver`, `verify/tla/Outbox.tla`).
- (O2) A new consumer group starts with nothing delivered, so it reads all
  history.
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

**Order on the bus.** This record found the relay handing a stream's
versions on out of order: it released by transaction id, which is assigned
at a first write, and the inbox's dispatch writes first. Measured: version 6
relayed before version 5. ADR-0035 now releases a stream's messages on one
URI in their writers' commit order. It promises nothing past the outbox:
- a broker's partitions;
- a channel fed by several URIs;
- a re-done epoch interleaving streams;
- an inbox row parked or waiting while later ones pass it.

Any record that infers "every position up to n was seen" from a forgotten
position n takes a late arrival for a repeat, and nothing rules a late
arrival out past the outbox.

**How long each obligation lives.** Each ends by one of three signals:
- *an event in the same database* — for an outbox row, the row visible in
  every position's `delivered`;
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
  parent reaches as other tables, and refuses the migration. It refuses a
  step that locks any pre-existing table but its target, in any mode: a
  read of the position table from the outbox's step is refused
  (`crates/migration/src/lib.rs`).
- A batch by position over the ring, as one `Merge Append`, walked a
  generation with no row left in the epoch from the offset to the epoch's
  end before it could return anything: 208 001 rows, 96.6 ms, on every
  batch.
- `pg_get_serial_sequence` finds no sequence that a column uses but does not
  own, as a copied table's position does.
- An ordinary role sees every session's database and `backend_xid` in
  `pg_stat_activity`, a superuser's in another database included, and reads
  `pg_prepared_xacts`. The view is read once per transaction: a second read
  in it missed a transaction opened since, until the statistics snapshot was
  cleared.
- A batch over an epoch that spans two generations, 100 rows, on `ring8`:

  | Shape | Plan | Rows read | Time |
  | --- | --- | --- | --- |
  | `UNION ALL`, no limit per branch | Append, top-N sort | 464 200 | 108 ms |
  | One statement, the generations named | Append, top-N sort | 464 200 | 71.5 ms |
  | Each branch `ORDER BY … LIMIT 100` | Merge Append over the limits | 101 | 0.097 ms |

- The newest `created_at` of a generation has no index: 60 ms over 208 000
  rows, 9.4 s over 4 million, cold.
- A custom setting made by an earlier transaction of a pooled connection
  reads back empty in a later one, and a fresh connection reads none; a
  predicate defended against both matched no row.
- A named generation costs one relation lock and one per index: ten locks
  for two generations of three indexes, twelve of four. With the ring's six,
  a statement naming two reaches the sixteen a backend takes on its fast
  path before the position and meta tables (inferred).

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
- The relay's fetch of 100 rows past a position, ADR-0007's, planned and
  executed:
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
   its obligation ends in transaction-id order: a row is passed once its id
   is visible in every position's `delivered`, and a snapshot sees every id
   below its `xmin`. The inbox reclaims rows, because one range of it holds
   obligations of minutes, of weeks and of forever.

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
     transaction. `message_id` is unique within a generation. ADR-0035's
     `(slot, position) INCLUDE (transaction_id)` is built on the ring, its
     leaves named too.
   - **Two bounds, recorded in `{outbox}_meta`.** A generation holds every
     lap that wrote into it, so emptying it removes windows above older ones
     still kept. One number cannot say both "below it all is gone" and "from
     it all is kept".
     - **`gone_below`** — an id below which no row is left and none will be
       written: the least of the ring's least remaining id (read on each
       generation's primary key), this database's oldest open id, and the
       next id. Each emptying sets it again; nothing counts it forward. The
       reclamation starts from it, and an operator sees what pins it.
     - **`intact_from`** — an id from which every row written is still kept:
       one past the highest id of any generation emptied, raised by each
       emptying. Rewinds and new groups read it.
   - **Rewinds and new groups start no lower than `intact_from`.**
     - **`set_position` below it is refused,** with a typed error naming it,
       so "back to zero" means back to it. A rewind above it is ADR-0035's
       intersection.
     - **A new group starts as that rewind of the current snapshot** to
       `intact_from`. Ids below it may still be running: a window between two
       emptied ones need not be closed.
     - Both read `intact_from` and write the positions in one transaction
       under the reclamation's key — a rewind exclusively, a new group or
       `ensure_positions` shared — so that no emptying commits between them.
   - **The fetch names its generations.**
     - **An epoch's start names exactly the windows its rows can lie in,**
       worked out after `epoch` is taken, in a statement of its own: those of
       `[delivered.xmax, epoch.xmax)`, and those of `delivered`'s in-progress
       ids that `epoch` sees. All `K` when they span `K − 1` windows or more.
       Neither bound nor `delivered`'s `xmin` enters it: that is the server's
       oldest transaction, and would name — and lock — the partitions an
       emptying is waiting for.
     - **It records the generations it found rows in,** each with its first
       and last position in the epoch, in the position row beside the epoch.
       A converted epoch (ADR-0035 decision 10) is recorded at the relay's
       next look, with its `epoch_end`.
     - **A batch names those generations only.** Each is a branch of a
       `UNION ALL` scanned from the later of the offset and its first
       position up to its last, with its own `ORDER BY position LIMIT n`;
       the branches are merged by position and limited to `n`. The branch's
       limit does the work: without it every batch read the whole remaining
       epoch (§ Context).
     - The names are passed as constants, so that the planner prunes and
       locks the named partitions alone.
   - **Reclamation.** The dispatcher works it between its batches, at most
     once an interval, in a `READ COMMITTED` transaction of its own:
     1. `pg_try_advisory_xact_lock` on the outbox's reclamation; another
        holder means this pass is skipped.
     2. **A counter reset** (below) is checked here too, not taken from a
        relay's report: a `delivered` whose `xmax` lies above the next id
        stops the pass.
     3. **The candidate** is the generation holding `gone_below`. Its newest
        id is read on its primary key.
     4. **The checks, without its partition's lock, each in a statement of
        its own, in this order:**
        - past — its newest window lies `margin` windows below the next
          id's, so no id can be assigned in its windows any more;
        - closed — no transaction of this database that could still write
          into it is open: no `backend_xid` of its sessions, widened against
          the next id, then none of its prepared transactions, belongs to
          the generation, at any lap. The statistics snapshot is cleared first:
          `pg_stat_activity` is read once per transaction. Sessions come
          before prepared transactions, since a transaction being prepared is
          listed as prepared before it leaves its session. A transaction is
          bound to its database, so one left open elsewhere on the server
          holds no ring back;
        - passed — read after "closed", so that a transaction committing in
          between is seen committed and not yet delivered, never gone and
          invisible at once: every row of the generation is visible in every
          position's `delivered`; each `delivered`'s `xmax` lies past its
          windows, and no id of its in-progress list within them has a row
          in the generation. A row of an epoch in flight is not passed yet. An
          idle slot's `delivered` still advances (ADR-0035 decision 5);
        - old enough — the first pass to find it passed records the time,
          and it is emptied only `margin_age` after, so that a row relayed a
          moment ago can still be re-injected. A rewind clears the times of
          the generations it reaches back into. A row's `created_at` would
          not do: it is its transaction's start, and reading the newest one
          scans the generation (§ Context).
     5. **Only then the partition's lock,** under `SET LOCAL lock_timeout`.
        "Passed" is read again in a statement after it; then `TRUNCATE`, and
        both bounds and the generation's time are recorded.

     A lock not granted, a statement cancelled, or any other error of the
     reclamation is reported and retried at the next interval. It never
     stops, delays or restarts dispatch.
   - **Nothing is made ahead.** A generation that cannot be emptied — a
     lagging or abandoned group, maintenance not running — takes the next
     lap's rows as well. The table grows, as today; no insert is ever
     refused for want of a partition.
   - **`K` and `R` are checked against the ring itself** — its partitions and
     its `CHECK` — as well as against the meta table: one component's steps
     may be applied by processes configured apart.
   - **A counter reset** (ADR-0035 decision 12) stops relay and reclamation
     alike, and the refusal sticks until the rebase. The rebase, in one
     transaction:
     1. locks the outbox, so that no publish lands while it works;
     2. takes its snapshot `S` before it writes anything, so that its own id
        is not visible in `S`;
     3. keeps the rows no position has handed on: every row below the
        restored `gone_below`, written in the new ids since the restore, and
        every restored row not visible in some `delivered`. If the new ids
        have reached the restored `gone_below`, the two cannot be told
        apart, and it refuses;
     4. keeps the first of rows that share an identity and a channel:
        ADR-0030's guard admits one per transaction, and the inbox drops the
        rest as repeats;
     5. writes them again under its own id, in their positions' order, and
        empties every generation of everything else, the current one
        included;
     6. sets every `delivered` to `S` and clears every epoch, and sets both
        bounds to the least of its own id and this database's oldest open
        id. The bounds move back here, and only here.
   - **Groups that pin the ring.** The group pinning `gone_below` is
     reported: its name, its last acknowledgement, the generations it pins.
     `forget_group` drops an abandoned group's positions, and refuses one
     acknowledged within the margin.

4. **The inbox forgets a processed row by its channel's rule.**
   - **The mark stores `processed_at`** (`statement_timestamp()`). The sweep
     computes expiry from the rule in force when it runs, so a changed rule
     applies to rows already processed, and rows processed before any rule
     are covered.
   - **The rules.** A host declares one per channel (the destination without
     its key) in `InboxRetention`:
     - **A window**, from `processed_at`. A host sets it at least to the
       longest a repeat can reach the inbox: the broker's whole retention,
       tiers included, the time a segment outlives it, and the producer's
       replay horizon. Every channel is held by this rule, whoever sent its
       messages and however — another service, a tool, the service's own
       outbox. The inbox knows its channels' windows, and nothing of their
       senders.
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
   - **Each source of repeats bounds them where they arise,** so that a
     window can cover them:
     - **A producer on this library declares its replay horizon,** a time.
       `set_position` refuses a rewind to a transaction whose first row at
       or above it is older than the horizon. That row's `created_at` is its
       transaction's start, so the refusal comes a little early, never late.
       A host's windows cover its producers' horizons.
     - **A relay hands its own inbox's channel on in a lane of its own:** a
       selection with its own position and its own loops, never in one batch
       with a broker's destinations. Today one selection takes the whole
       outbox (`outbox://all`). A broker's outage then retries the whole
       batch, and each retry stores the batch's Commands to self in the
       inbox again, for as long as the outage lasts: past any window. A
       retry of the inbox's lane lasts as long as a database's error.
       - **The subscriber names its lane by the destinations' prefix**
         (`outbox://inbox`, `outbox://kafka`). The outbox's channel reads the
         name after `outbox://` as its selection's prefix, which its store
         already filters by. Neither the outbox nor the bridge learns what
         an inbox is: the host's composition root, which registers both
         channels, puts them in lanes.
       - **A broker's events no longer wait behind the service's own
         Commands.** In one lane, an Integration Event written after a
         publication's fan-out waits until the relay has handed the whole
         fan-out on: at about 680 rows a second, two and a half minutes for
         100 000 Devices, about 25 for a million (inferred; M14). And a
         broker's outage no longer holds the service's own asynchronous
         work — a delay only, since its results leave through the broker
         anyway.
       - **Lanes land with ADR-0035.** Under ADR-0007's rule a selection's
         position moves only past rows it hands on, so a lane whose rows lie
         behind another's would walk the other's rows again on every poll.
         Under epochs `delivered` moves by snapshot.
       - **Each lane reads the other's rows once.** The prefix is no index's
         key: on the host's drill one slot held 101 140 rows to the inbox and
         24 to the broker. A look that finds rows new since `delivered`, none
         of its lane's, delivers that epoch at once (ADR-0035 decision 5), so
         that the next look does not walk them again. Reading the outbox
         twice is the lanes' price (M14).
       - **Switching:** the lanes start at the position the selection of
         everything holds, not from the ring's start, and its positions are
         forgotten once no old binary relays them. Until then both relay, and
         identities drop the repeats.
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
   - a generation reclaimed (its windows, its rows, both bounds);
   - a pass skipped, with why: a group pinning it and since when, a reader
     holding the partition, the margin, not closed;
   - rows swept, and the oldest expiry left;
   - the replay horizon: the `created_at` of the oldest kept row.

   Logs, rate-limited:
   - a warning for `gone_below` pinned over a number of generations;
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
  - It declares its producers' replay horizons, and relays its own inbox's
    channel in a lane of its own: two bridges, its pool sized for both.
  - Its own gates hold the rules: a window covers the channel class's
    re-consumption horizon, and no channel whose stream types are dependency
    targets has a window.
- One more session per process, for the sweep.
- **Migrations,** after ADR-0035's conversion of the positions:
  - **`K`, `R`, the two bounds and the generations' times** go into
    `{outbox}_meta` by a step of their own, beside ADR-0035's format.
  - **The copy's bound is fixed before the ring's step,** by an ordinary
    transaction of `migrate` between the runner's steps, skipped once the
    ring's step is recorded. Under the reclamation's key, held for that
    transaction only, it first advances every position with no row new to
    it to the current snapshot, as an idle look would (ADR-0035 decision
    5) — else a slot its selection never matched, still at nothing
    delivered, would make the copy take everything. Then it takes the
    least of:
    - every `delivered`'s `xmax`;
    - every id of a `delivered`'s in-progress list that has a row;
    - this database's oldest open id.

    It records that id as both bounds. From its commit, a rewind below it is
    refused and a new group starts at it; positions otherwise only advance.
    So every row some position has not handed on, then or at the ring's
    step, has an id at or above it: a row not visible in a `delivered` lies
    at or above its `xmax` or in its in-progress list, and a transaction
    open then or begun since has an id at or above the bound. `xmin` would
    not do: it is the server's oldest transaction.
  - **The ring's step renders the bound into its text** as a literal, its
    checksum over the template (`checked_over`). The runner hands a step
    nothing session-scoped — its connection is a pooled one (ADR-0029) —
    and refuses a step that reads a second table. The step:
    - copies the rows at or above the bound, keeping each row's
      `transaction_id` and position, and stamps each row's own generation;
    - continues the position sequence and moves its ownership to the new
      column before it drops the old table. The runner finds its target by
      name after the step, so the old table is dropped, not set aside, and
      a sequence it still owned would go with it;
    - builds ADR-0035's position index, so that publishers wait for one
      lock, not two.
  - **Before the step,** the library reports how many rows it will copy. A
    host drains its relays first, which keeps the bound near the head: on
    the host's drill, one row of 101 164. An abandoned group holds the bound
    low: `forget_group` first, as for a pinned ring.
  - **Old binaries in the rolling window:** their `set_position` moves only
    ADR-0035's old pair and checks no bound, so their replays may come up
    short; their new groups take the default, nothing delivered, and read
    the history the ring holds.
  - **The inbox gains `processed_at`.** Its old processed rows carry none, so
    they are kept until a one-time pass stamps them.
  - Stands made before are recreated.
- **The rollback boundary is the ring's migration.** A binary from before it
  runs on the ring — it relays by ADR-0035's old pair, unpruned, and
  reclaims nothing — but the table does not go back.
- **A database moved by dump and restore** is rebased by an operator
  (decision 3, a counter reset); nothing is drained or truncated by hand.
- `idle_in_transaction_session_timeout` for the service's role belongs to the
  host: a transaction left open holds back every vacuum, deletes included,
  and the ring's generation it writes into.
- **The order on the wire, which this record found broken, is ADR-0035's;**
  the processing of one aggregate's messages is ADR-0034's. This record and
  ADR-0035 land in one release: the ring's migration builds ADR-0035's
  index, and the ring's fetch is the epochs'. ADR-0034 lands after both, on
  this record's runner.

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
  - Withdrawn. ADR-0035 orders a stream's first delivery on one URI, but
    nothing past the outbox (§ Context, order on the bus). The record would
    take a late lower position for a repeat and drop a first arrival,
    silently.
  - Advanced only over a gapless prefix it is safe, but it compacts nothing
    past the first position a channel does not carry, and channels carry a
    subset of a stream's versions.
  - It would also make a stream re-created under the same id lose every
    message for good.
  - A channel that needs to compact what "keep" holds is a decision of its
    own, with the order past the outbox as its premise.
- **"Closed" by the server's `xmin`** (the first decision). A transaction
  left open in any database of the server held every ring on it back. A
  transaction is bound to its database, and every session's id is readable
  by an ordinary role (§ Context).
- **The copy bounded by a read of the positions inside its step.** The
  runner refuses it (§ Context).
- **Bounds handed to the ring's step as a session setting, the key held
  across both** (the amendment's first form). A pooled connection carries
  neither: a defensive read of a setting another transaction made copied
  nothing (§ Context), and a session lock leaks to the next borrower
  (ADR-0029).
- **A bound per slot** (the same form). It copies less, but each slot then
  has its own hole for rewinds and new groups. One bound, with idle
  positions advanced first, copied one row on the host's drained drill.
- **One floor, counted forward by each emptying** (the first decision). A
  generation emptied with a later lap leaves kept windows below removed
  ones, so the one number either loses rows to the fetch or lets rewinds
  and new groups fall into holes. And counted forward, it never catches up
  once it starts behind: one window an emptying, one emptying a window.
- **An epoch named from the floor or `delivered`'s `xmin`** (the
  amendment's first form). `xmin` is the server's oldest transaction: one
  left open in another database named the floor's partition on every
  epoch, and the emptying queued behind the fetches' locks.
- **A batch's branches without their own limit.** The planner sorted the
  whole remaining epoch for each batch (§ Context).
- **Commands to self through the broker** (raised by the owner,
  2026-11-04), so that the relay has one failure domain. It would make
  Commands to self travel as Commands between contexts do (the host's
  ADR-0065), and let a customer's systems observe or take part in them —
  monitoring such as Zabbix, integration with the customer's own systems.
  Not chosen:
  - the inbox's window for Commands to self would follow the command
    topic's retention;
  - a fan-out of a million Devices would be a million messages through the
    broker, and the service's own work would stop with it;
  - a customer subscribed to a step of a process freezes that process's
    shape: the host changed its fan-out's form more than once (ADR-0084).
    Observation has Integration Events, and control the published Commands.

  The events' lane is needed on that path too: everything in one lane to
  the broker, events still wait behind the fan-out.
- **Lanes that never read each other's rows:** a second outbox table for
  Commands to self, or the destination's class in the slot's hash. Not
  chosen while reading the outbox twice is cheap (M14): the first doubles a
  host's outbox components, the second recreates the table and makes a
  selection name its slots.
- **"Old enough" by a generation's newest `created_at`** (the first
  decision). It scans the generation, twice, the second time under its lock;
  and it is a transaction's start, not its relay.
- **Commands to self forgotten once their carrier is reclaimed** (decided
  in the earlier rounds, withdrawn by the owner on 2026-11-04).
  - **The rule.** The relay stamped each inbox row with the
    `(transaction_id, position)` of the outbox row that carried it. The
    sweep forgot the row once that id fell below the outbox's `gone_below`:
    no row below it is left, so neither a retry nor a rewind could carry
    the Command again. It kept a row exactly as long as needed, with no
    window to set.
  - **Why it was withdrawn.** It makes the inbox know the outbox's ring, and
    it serves one origin. A Command from another service (the host's
    ADR-0065), or one written to a broker without an outbox, is not covered
    and needs a window anyway. A rule that holds for one sender and not the
    others is not the inbox's.
  - **The two reasons it was chosen over a window** — a broker's outage
    retrying a mixed batch, and an operator's rewind — are bounded where
    they arise instead (decision 4).
  - **Its premise, to check if it returns:** a Command to self is repeated
    only through the outbox row that carried it. A writer that sends the
    same identity again from another transaction breaks it once the carrier
    is gone.
  - **It also owed a rule across a rebase:** the bounds move back, and the
    carriers stamped before it never fall below `gone_below` again.
- **The inbox as generations, after Storm's `GenerationalCache`** (raised by
  the owner, 2026-11-04; the library's identity map keeps one,
  `crates/session/src/identity_map/generations.rs`).
  - **The idea.** The inbox partitioned by generation. A generation is let
    go of whole by `TRUNCATE`; the rows still alive in it — unprocessed,
    parked, waiting, kept, named by a waiting row — are carried into the
    young one first, as a key in use survives a bump. It answers decision
    2's premise another way: a range holding obligations of minutes and of
    forever can go whole if its long-lived minority is carried forward.
    Forgetting would cost a `TRUNCATE` and the survivors' copy, not a
    `DELETE` and a vacuum per row.
  - **With the carrier rule,** a Command to self's generation could have been
    its carrier's window: its repeats come through the same carrier, so a
    unique index within the generation would have sufficed.
  - **Without it,** no unique index refuses a repeat whose original lies in
    the other generation. Every insert would look the other generation up,
    and a switch of generations take a lock that inserts in flight hold: a
    change to the inbox's de-duplication, its most critical part
    (`EffectsOnce`).
  - **What is carried forward is what is used, not what may be:** a
    dependency target still needs "keep".
  - **Taken up again** if M8 shows that the per-row sweep's dead tuples or
    size do not level off; then with a blind solver and a discriminator.
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
    `intact_from`; "back to zero" lands on it.
  - A generation holding two laps emptied: a rewind or a new group between
    the laps is refused or starts above them; `gone_below` is the least row
    left, not a count of emptyings.
  - A new group created while a transaction with an id below `intact_from`
    is open receives that transaction's rows.
  - A truncation is refused when it is not past, not closed, not passed (a
    second group lagging), or not old enough; a rewind into it clears its
    time.
  - "Passed" read before "closed", or prepared transactions before
    sessions, with a transaction committing or preparing between the reads:
    a row lost (each red with its order swapped).
  - A reclamation's error or lock timeout never stops dispatch.
  - A fetch locks the partitions it names and no others.
  - An epoch spanning generations: a batch names only those the epoch found
    rows in, each from its first position with its own limit, and drops one
    once passed; the rows a batch reads stay within a bound (red without the
    branch's limit).
  - An epoch's names worked out from its own `xmax`: a transaction
    committing into the next window between a name computed early and the
    snapshot is handed on (red when computed before).
  - A converted epoch on the ring: the rest of its transaction handed on.
  - A transaction held open in another database holds no reclamation and
    adds no partition to a fetch; one in this database, with no row written
    yet, holds its generation.
  - An id of `delivered`'s in-progress list that committed rows into the
    generation after it was taken: the generation is not passed.
  - ADR-0030's refusal is named on the ring (the existing test, run on it).
  - A later migration of the ring is accepted by the runner.
  - The ring's copy, from a bound fixed before its step, with a rewind and
    a new group attempted in between and a transaction of this database
    open: every row not handed on is copied; the rewind is refused.
  - A slot its selection never matched does not pull the bound down.
  - The new table owns the position sequence; the old one is dropped.
  - A counter reset stops relay and reclamation until the rebase, and the
    reclamation stops on it without a relay's report. The rebase keeps every
    row not handed on — those written since the restore included — in
    order; empties the current generation's restored rows; survives two
    rows of one identity; and, with a transaction committing after it takes
    its id, leaves its rows undelivered (red when `S` is taken after its
    first write).
  - A full lap without reclamation refuses no insert.
- **The inbox.**
  - Unprocessed, parked, waiting and kept rows, and rows a waiting row names,
    are never deleted.
  - Within a window a repeat is ignored; after it, it is processed.
  - A Command to self within its window: a relay's retry and a rewind
    within the producer's horizon are dropped as repeats.
  - A rewind deeper than the producer's horizon is refused.
  - Two lanes: a broker's outage stalls only its lane, and the inbox's lane
    hands its Commands to self on once; an Integration Event written after a
    fan-out leaves without waiting for it.
  - A lane whose epoch held only the other lane's rows delivers it at once,
    and its next look walks none of them (red when it waits).
  - The lanes start at the position the selection of everything held.
  - A changed rule applies to rows already processed.
  - The grace is re-armed after an outage of the database.

**TLA+.**
- **ADR-0035's statement-level `OutboxPg.tla` gains:**
  - generations at small `R` and `K` that lap;
  - a reclaimed set that the fetch skips;
  - `Reclaim(g)` with its conditions as switches;
  - a `Rewind` and a `NewGroup` that check `intact_from`;
  - two groups, and a transaction of another database;
  - the reclamation's reads as separate steps, a commit or a prepare able to
    fall between them.
- **Its invariants:**
  - `NoPositionBelowGone`, over the windows emptied, not over a bound;
  - `ReclaimedWasReceived` for every group.
- **Configurations that must fail:**
  - "passed" dropped;
  - "passed" by `delivered`'s `xmax` alone, its in-progress list ignored;
  - "closed" dropped, a transaction writing its first row after the check;
  - "passed" read before "closed"; prepared transactions read before
    sessions;
  - the fetch's arc by the ring's arithmetic;
  - an epoch's names computed before its snapshot;
  - one group checked;
  - a rewind racing a reclamation;
  - a rewind into a lap emptied together with an older one;
  - one bound for both meanings.
- **`Inbox.tla` gains:**
  - re-arrival after `Forget`;
  - a ghost of everything ever processed, over which `WaitingIsAside` and
    `CausalOrder` are restated.
- **Its configurations that must fail:**
  - forgetting early;
  - forgetting a dependency target.
- **`Bridge.tla`:** an outbox `Rewind` with an inbox `Forget` at the mark must
  fail `EffectsOnce`, and hold when the window covers the producer's horizon
  and a deeper `Rewind` is refused; a mixed selection retried across a
  broker's outage must fail it too.

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
  against today's about 680 rows a second: it would make the inbox's own
  selection hand on exactly once, ending even a database error's
  re-insert.
- **M8.** The inbox at 410 inserts, marks and deletes a second for an hour,
  and again with the vacuum horizon held for ten minutes: whether dead
  tuples and size level off.
- **M9.** The ring's migration copying 5 million rows, ADR-0035's index
  built in it: how long publishers wait, against the host's one-second
  write budget (its ADR-0083). Above it, the copy's bound is what keeps the
  upgrade within budget, and a host drains its relays first.
- **M11.** The fetch on the ring under epochs — an epoch's start over its
  named windows, a batch over its per-generation branches with their limits,
  on ADR-0035's `(slot, position)` index — against ADR-0035's figures on one
  table.
- **M12.** Waits on the server's lock manager (`LWLock:lock_manager`) at the
  launch's concurrency, with fetches naming two generations of six indexes:
  above a few per cent of a fetch's time, the ring drops an index or a
  fetch names fewer generations.
- **M13.** Eight loops relaying, a transaction held open in another
  database, the reclamation passing: emptyings at their usual rate and the
  fetch's p99 unchanged, or the coupling to the server's oldest transaction
  is back.
- **M10.** The cluster's rate of transaction ids at the launch's load, every
  context's database on one server: the margin in generations against
  `margin_age`.
- **M14.** On the drill, an Integration Event's relay latency behind a fan-out
  of 100 000 and of 1 000 000, one lane against two; and the rows the
  broker's lane walks for one fan-out. If the walk costs the broker's lane
  more than a few per cent of its time, the lanes stop sharing a table or a
  slot (§ Alternatives).

## References

- ADR-0003, ADR-0005, ADR-0006, ADR-0007, ADR-0008, ADR-0009, ADR-0010,
  ADR-0029, ADR-0030, ADR-0034, ADR-0035.
- `crates/outbox/README.md`, `crates/inbox/README.md`,
  `crates/migration/src/lib.rs`.
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
- 2026-11-03 — Amended on top of ADR-0035, from its review of roles:
  - **The ring under epochs:**
    - "passed" over every position's `delivered`;
    - a new group from the floor's snapshot;
    - an epoch's start named from `delivered`'s `xmin`;
    - a batch over per-generation branches bounded by the epoch's own
      positions;
    - the counter reset's rebase on the ring.
  - **"Closed" per database,** by its sessions' ids (measured readable).
  - **The migrations:**
    - the meta in a step of its own;
    - the copy bounded per slot from outside the step, since the runner
      refuses a read of the position table (measured);
    - the sequence owned;
    - ADR-0035's index built in the ring.
  - **The stream record** stays withdrawn, its reason restated.
- 2026-11-03 — The amendment refuted from two roles (database, distributed
  systems); every finding taken in:
  - **Two bounds for the floor's two meanings:** `gone_below`, the least row
    left or id open, set again by each emptying; `intact_from`, one past the
    highest id emptied, for rewinds and new groups.
  - **The fetch:** an epoch named exactly from its two snapshots, after its
    own; a batch's branches each with its own limit; a converted epoch
    recorded at the relay's next look.
  - **The reclamation:** past, closed, passed read in that order, sessions
    before prepared transactions, the statistics snapshot cleared; "old
    enough" by the time a generation was first found passed; the counter
    reset checked by the reclamation itself.
  - **The rebase:** the outbox locked, its snapshot taken before its first
    write, rows written since the restore kept, one row per identity, the
    current generation emptied of restored rows, the bounds to the oldest
    open id.
  - **The migration:** one bound fixed by an ordinary transaction before the
    ring's step, idle positions advanced first, rendered into the step's
    text; the old table dropped after its sequence moved.
  - **New groups** under the reclamation's key; `K` and `R` checked against
    the ring itself.
  - **Owed:** the inbox's carrier rule across a rebase.
- 2026-11-04 — Revised by the owner: the carrier rule withdrawn to the
  alternatives. It made the inbox know the outbox's ring and served one
  origin.
  - **Every channel is held by a window,** Commands to self included.
  - **Repeats bounded where they arise:**
    - a producer's declared replay horizon, which `set_position` holds;
    - the inbox's own channel relayed by a selection of its own, so that a
      broker's outage no longer retries it.
  - **Recorded as alternatives:** the carrier rule with its premise, and the
    inbox as generations after Storm's `GenerationalCache`, taken up again
    if M8 fails.
  - **Closed:** the carrier rule's owed rule across a rebase.
- 2026-11-04 — Lanes, by the owner's choice: the inbox's own channel relayed
  by a selection of its own, named by the subscriber's destination prefix.
  - **Two reasons:** a broker's outage no longer retries Commands to self
    past their window; a broker's events no longer wait behind the
    service's own fan-out.
  - **Its conditions:** it lands with ADR-0035; a lane reads the other's rows
    once, a look that finds only theirs delivering the epoch at once; the
    lanes start at the old selection's position.
  - **Recorded as alternatives:** Commands to self through the broker, with
    the owner's reasons for it; lanes in separate tables or slots, taken up
    if M14 shows the double read matters.
