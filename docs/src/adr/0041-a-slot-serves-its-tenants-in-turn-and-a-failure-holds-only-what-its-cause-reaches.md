# ADR-0041: A slot serves its Tenants in turn, and a failure holds only what its cause reaches

## Status

Proposed (2026-11-14). Slices 0 and 1 built (2026-11-16). Numbered 0039
until 2026-11-16, when the data keys' ADR took that number on the main line
(§ Change Log).

Slice 2, the failure's scope (decision 3), moved to ADR-0040's revision,
which owns the verdicts a subscriber returns; decision 4's deferral goes
with it (§ Change Log, 2026-11-16). That revision is ADR-0042, which
supersedes decision 3's environment's verdict; the deferral is its own
decision, still owed.

Decided under the library's gate:
- **A blind solver.** It worked from the problem alone.
- **A discriminator.** It checked the solver's position and the author's
  against the code, the host's records and the sources of librdkafka 2.12.1
  and rust-rdkafka 0.39.0, the versions the host's lock file pins.
- **A synthesis.** The synthesis was then refuted by five roles: distributed
  systems, database with evolution, performance, operability and
  verifiability. Their findings are taken in below, each with its executor.
- **Measurements by the author** on a scratch PostgreSQL 14.24 (§ Measured).

The owner's decisions still open are listed in § The owner's decisions. None
of them changes the library's part.

It amends:
- ADR-0005 decision 2 (what a failed message holds);
- ADR-0008 decisions 2 and 4 (which head a slot's holder takes, and the
  order kept at one slot);
- ADR-0010 decision 4 (the subscriber's verdicts).

It leaves the cut by the stream (ADR-0008 decision 1), the dispatcher
without identity (ADR-0007, ADR-0008 decision 3), the waits (ADR-0006,
ADR-0009) and the relay's epochs (ADR-0035) as they are.

## Context

### What the host found

While one large Tenant has a backlog in an inbox, a message of a small Tenant
waits for most of that backlog. The host's drill runs its outbox, relay and
inbox of 64 slots at launch size (the host's ADR-0084 § Owed, slice 1g-3b):
- **Three Tenants of a million Devices changed at once.** That put up to
  170 645 rows in the inbox and 12 000 tiles in flight. Another Tenant's one
  change waited up to **21 minutes**, while the oldest tile waited 23.
- **One such Tenant alone:** the neighbour waited up to 430 s.
- **The probe is closed-loop**, one change a second, so the p95 of 206 ms
  hides the stall: weighted by time, 7.7 % of the wave fell inside waits
  longer than a minute.
- **The requirements:** FR-CONFIG-0004 asks 60 s at p95, and access changes
  are asked in 5 s (the host's ADR-0010 § 4).

### Why it happens: the stages

**S1. A slot is one FIFO across Tenants.**
- A row's slot is `hashtext(<stream>) % S` (`partition.rs`, `store.rs` DDL).
- `take` serves the least recently served slot whose oldest row is due.
- `locate_head` takes that slot's oldest row.
- A launch Tenant's tiles and Commands to self are a stream each, so its
  backlog B lies spread over every slot. A newcomer waits behind B/S rows of
  its slot, which is served at μ/S, so it waits (B/S)/(μ/S) = **B/μ**,
  whatever S is.
- More slots shrink a slot's share of the backlog, and shrink by the same
  factor how often the slot is served. The host's TD-1 (its ADR-0035), which
  said the share shrinks as the slots grow, is wrong on this point.

**S2. A head in backoff holds its slot** (ADR-0005 decision 2; `take`'s
due-head predicate).
- With the host's retries — ten attempts, 2 s doubling to 300 s — a failing
  head holds its slot 2 + 4 + … + 256 + 300 = 810 s before it parks.
- Every Tenant hashed to that slot waits with it. At one slot, the default
  that three of the host's four services use, that is every Tenant of the
  service.
- Measured below: with 90 % of the (slot, Tenant) heads in backoff, today's
  `take` can take 4 slots of 64 while 151 589 rows are due.

**S3. A loop waits inside a handler on its Tenant's lock.**
- A host's handler may wait for its Tenant's exclusive lock: the host's
  partition hold, 12.9–15 s at a million Devices.
- Nothing bounds how many loops one Tenant's messages hold.
- That is the host's lock, and the host's to schedule. The library can only
  give the handler a way not to wait (decision 4).

**S4. Before the inbox, every stage is a FIFO that cannot be reordered.**
- **A broker's partitions.** The consumer must move its offsets in order.
- **librdkafka's consumer queue.** The high-level consumer forwards every
  assigned partition into one queue (`rdkafka_assignment.c:513`;
  `rdkafka_partition.c:2293-2295`). The thresholds are checked on the
  forwarded queue (`rdkafka_fetcher.c:1366-1381`; `CONFIGURATION.md`:
  `queued.max.messages.kbytes` "applies to the single consumer queue,
  regardless of the number of partitions"). With the defaults, 100 000
  messages or 64 MiB, and the bridge's serial intake of about 134 messages a
  second, a message on any partition can wait about 746 s behind another
  partition's prefetch.
- **The outbox's relay** hands rows on in epochs by position (ADR-0035). A
  row committed after an epoch starts waits for that epoch whatever order is
  read inside it. All of a service's Commands to self share one URI, so
  cutting the outbox by URI cannot separate Tenants.

### The principle the decision rests on

**A backlog sits at the slowest FIFO stage. Fairness is possible only where
the queue can be reordered.** Of the stages above, only the inbox can be
reordered: it keeps its rows and orders them itself.

So the inbox becomes the one place where a backlog is scheduled. Every stage
before it is kept faster than its arrivals, so that no backlog persists
there. Where that cannot be kept, a stage isolates by key, and that is
isolation, not fairness.

### What the contract licenses

Only one reordering: across Tenants.
- The host does not promise cross-Tenant order on its bus (its ADR-0035
  Part 2).
- The inbox's identity carries the Tenant: `tenant_id` begins the primary
  key.
- No reliance on cross-Tenant order was found in the host's four services.
  - The only declared dependency is within one Tenant (an application
    assignment on its creation).
  - Chains from the platform's sentinel Tenant to a Tenant are causal by
    construction: each is written by its predecessor's transaction.

A reordering within a Tenant is licensed by nothing, at one slot least of
all. ADR-0008 decision 4 promises arrival order over the whole table there,
and a service may rely on order between its channels within a Tenant.

### The owner's two objections, which the decision answers

1. **"The throttle only moves."** It does, to the slowest FIFO stage. That
   is the principle above. A broker's key finer than the Tenant, such as the
   Device, buys no processing parallelism:
   - a channel is one bridge task per replica, across all its partitions;
   - the inbox is one table every replica shares.

   And when intake lags, a finer key spreads one Tenant's backlog over every
   partition. So durable channels stay keyed by Tenant, which is the host's
   to record.
2. **"No opposite result."** A large Tenant alone must use all the
   capacity. Rejected for that reason:
   - a slot per Tenant;
   - a URI per Tenant;
   - a cap or reserve of loops per Tenant;
   - priority lanes.

   The slots stay cut by stream, so a lone Tenant has heads in every slot,
   and every loop works for it as today.

## Decision

### 1. The contract: arrival order is kept per (slot, Tenant)

- Within one slot, the messages of one Tenant are processed one at a time,
  in arrival order.
- The messages of different Tenants have no order between them.
- Under `ByStream` this is per stream, since the stream contains the Tenant.
  Under `ByUri` it is per (URI, Tenant). There is one rule for both keys and
  no mode without it, so there is one code path.
- ADR-0008 decision 4's order "over the whole table" at one slot becomes
  arrival order per Tenant over the whole table.
- The library's README and the partition keys' documentation state it.
  Every host that relies on order across Tenants is told it no longer holds.

### 2. Rotation: a slot's holder serves its Tenants in turn

**The slot row.** `<table>_slots` gains four nullable columns:
- `last_tenant`, the cursor;
- `round_from`, where the current round over the slot's Tenants began;
- `dormant_until` and `dormant_after`, explained below.

**The index.** A partial index `(slot, tenant_id, received_position) WHERE
<queue>`. The head of (slot, Tenant) is that Tenant's oldest queue row in the
slot.

**The walk** runs inside `locate_head`, under the slot's lock and a snapshot
taken after it (ADR-0008 as amended by ADR-0009):
- It starts at the first Tenant after the cursor, wraps once, and examines
  at most `K` Tenants.
- It is a loose index scan: one descent per Tenant.
- It takes the first due head it finds, and locks that row `FOR UPDATE`.
- In the same statement it writes `served_at`, `last_tenant` and the round's
  state in **one** UPDATE of the slot row it already holds. That is one row
  version per dispatch, not two.
- `served_at` is **not** indexed, so the update stays HOT, as today.

**When the walk finds no due head among the `K` Tenants**, the cursor moves
past them and the dispatch returns `Passed`, which does not sleep. A due head
behind n Tenants in backoff is reached within ⌈n/K⌉ + 1 dispatches.

**A round** is the walks of a slot between a due head and the next. With
T_s Tenants it takes ⌈T_s/K⌉ walks, so it outlives a dispatch whenever T_s >
K: at M7's shape of 10 000 Tenants over 64 slots, about 156 a slot against
K = 32. Its state is the four columns:
- the walk that begins a round records `round_from`, the cursor, and
  `dormant_after`, the highest `received_position` in the slot's queue as
  its snapshot sees it;
- each walk of the round lowers `dormant_until` to the earliest backoff it
  saw. While `round_from` is set, `dormant_until` is the round's, and no
  dormancy;
- a walk that takes a head clears all three.

**When the walk wraps past `round_from` without a due head in the round**,
the slot goes dormant: `round_from` is cleared, and `dormant_until` becomes
the earlier of the earliest backoff the round saw and now +
`Rotation::max_dormancy`.

`dormant_after` is the queue's position **when the round began**, not when it
ended. A row of a Tenant the round has already passed, committed during the
round, then lies after it and wakes the slot. Read at the walk that ends the
round, it would lie below, and the slot would sleep past it until
`max_dormancy`. E13's model found that (`InboxPgDormantAfterAtWalk.cfg`).

The dispatch then returns `Nothing`, and the loop sleeps. The slot wakes at
`dormant_until`, or when a row arrives after `dormant_after` — one descent of
`__head_idx`, which `take` makes. A row whose position was drawn before the
round began but which committed after it lies below `dormant_after`:
positions are drawn at the insert, not at the commit. Such a row waits for
`dormant_until`, at most `max_dormancy` (§ Rule of Three, 3). In a slot whose
every head is in backoff, the writes are bounded: at most ⌈T_s/K⌉ `Passed`
per round per slot, and one `Nothing`, then nothing until the slot wakes.
They do not run continuously. The last `Passed` may be one walk more than
the Tenants need: when the Tenant served last has no row left, its place
has no Tenant, and the walk that examined the last Tenant before it cannot
know it is past.

**`take`** is guarded by one `EXISTS` over the queue, so an idle look costs
less than today. Busy, it takes the least recently served slot that has a
queue row and is not dormant: `round_from` is set, or `dormant_until` is null
or past, or a row arrived after `dormant_after`. The walk is in
`locate_head`, not in `take`'s correlated predicate.

**`__head_idx` stays.** Two reasons:
- the dormancy check needs a slot's newest queue rows in arrival order;
- an older binary, rolled back, keeps its plan. Dropping the index would
  turn its `take` into a sort of every slot's queue, a silent slowdown.

`K` is at least 32. With 90 % of heads in backoff, a walk of K comes back
empty with probability 0.9^K: 43 % at 8, 3 % at 32.

**`Rotation`** holds both, a setting of the inbox (`PgInbox::with_rotation`),
as `Retries` is: every dispatch reads it, `run`'s or a host's own. `K` is
`walk`, 32 by default. `max_dormancy` is a second by default: it bounds a row
made due behind a sleeping slot's back (§ Rule of Three, 3), the poll's own
granularity is a second, and the host's tightest budget, an access
suspension's 5 s (its ADR-0010 § 4), crosses more than one hop. A slot asleep
is walked again at most once a second, ⌈T_s/K⌉ walks.

### 3. What a failure holds is what its cause reaches

The unit a failure holds is the scope of its cause, not a constant.

**The message's own failure** holds its (slot, Tenant). This is today's
`Failure::Transient` or `Failure::Permanent`, and `From<E> for Failure`
still gives `Transient`.
- It backs off and parks after its attempts, as ADR-0005 decides.
- The Tenant's other heads in other slots, and the other Tenants of its
  slot, go on.

**The environment's failure** is a new verdict, `Failure::Environment`: a
dependency every Tenant shares is down (a key service, a database, a
downstream service). It is handled as an error of the moment (ADR-0010
decision 2):
- the transaction rolls back;
- no attempt is counted and nothing parks;
- the loop waits, longer with each such error in a row, up to `max_pause`.

This is ADR-0036's principle: only the environment holds the source. It
prevents the storm that the (slot, Tenant) unit alone would cause under a
common cause. There, every (slot, Tenant) head would be tried and parked: Σ
T_s heads, ten attempts each. On the operability role's estimate that is
about 128 000 parked rows if 2 000 Tenants have heads in each of 64 slots,
against 64 today. The host classifies the failure, since only
it knows its dependencies.

**The Tenant's own resource** (its lock, a key service of its own) is
deferred (decision 4).

**A rollback that is not the subscriber's verdict** — a connection killed
mid-handler, the mark's transient error — moves the cursor past the Tenant
whose head was located. That is done in a statement of its own, guarded by
`FOR UPDATE SKIP LOCKED` and by the cursor's old value. Without it, the same
head is taken again and the unit falls back to the slot. A crash is accepted
as ADR-0005 accepts it.

### 4. A deferral: `Failure::later(after)`, bounded

A subscriber that meets a resource of the Tenant held elsewhere returns
`Failure::later(after)`:
- the row's `next_attempt_at` is set `after` ahead;
- no attempt is counted;
- the loop is freed at once.

**The row's hold is durable.** A nullable column, `held_since`, is set at a
head's first failure or deferral. It is cleared at its mark, its park or its
unpark. It is handed to the subscriber in `InboxMessage`, so that the host
can stop deferring. The host's rule is its own. The host's is expected to be:
past about one hold, take the lock blocking and join its queue.

**Measured below:** PostgreSQL grants a lock in queue order, but a try never
joins the queue. It fails while an exclusive request waits, even when only
shared holders hold the lock. So try-and-defer alone starves under exclusive
requests that keep coming. Blocking requests interleave in queue order.

**The library bounds it.** Past `max_deferral` (an inbox setting, as
`max_wait` is), the row parks under a new ground, `Ground::Deferred`. So
`unpark`, `resolve`, the parked counts and the host's parked gauges apply to
it. ADR-0010's rejection of "try again on every error, for ever, with
nothing but the log" stands: no deferral is silent or unbounded.

**Its outcome and its observer event** are `Deferred`. ADR-0010 gains the
class.

### 5. What an operator sees

- **When a row arrived.** `received_at`, the table's default at the
  intake's insert, set again when a row is superseded. A sampled
  histogram of a row's wait, by channel, is for diagnosis, not for paging.
  It is dominated by a launch Tenant's own backlog.
- **Which heads are held.**
  - `held_counts(within)` reads them the way `parked_counts` reads the
    parked rows: by channel and cause (`backoff`, `deferred`), the number of
    distinct Tenants and the oldest `held_since`.
  - No Tenant identifier leaves the database (ADR-0036 R9).
  - It is in a transaction of its own and bounded by `within`, over a
    partial index of the held rows, `__held_idx`, so that its work is theirs,
    not the table's: at most one a (slot, Tenant).
  - A head is held from its first failure, kept so across its retries, and
    let go at its mark, its park, its unpark or its resolve; a failure that
    parks it holds nothing. A hold for its stage's outage (ADR-0040) ends
    it too: that hold is not the head's own failure's, and is counted
    elsewhere (ADR-0040 § Owed 7).
  - The host pages on the oldest against the channel's budget, and opens a
    ticket on the count. A count rising across Tenants is a common cause.
- **Every outcome is counted:** `Passed`, `Nothing` and `Deferred`, beside
  today's outcomes.
- **Bulk unpark:** ADR-0040's `unpark_all`, by channel, the cause a row was
  parked under, a Tenant and a window ending at the call, in batches, a
  canary of `tries` failures each. It replaced this decision's
  `unpark_where(channel, ground, parked)` (§ Change Log, 2026-11-16).

### 6. The schema: in the baseline, while nothing is deployed

The library has no deployed client yet: its only one is the host's tests
(the owner, 2026-11-15). So the schema of this decision is written into the
inbox's baseline, not added by steps:
- `<table>_slots` gains its four columns;
- `<table>` gains `received_at timestamptz NOT NULL DEFAULT
  statement_timestamp()` — every row has one, and a superseded row's is
  set again — and `held_since`, nullable without a default;
- the new partial indexes `__tenant_head_idx`, the walk's, and
  `__held_idx`, the held rows', `held_counts`'s.

The baseline's checksum changes, so a database made before is refused at
`migrate` (`MigrationError::Changed`) and made again (ADR-0029 decision 2).
CI makes its databases afresh; the host's own must be recreated.

**Why not steps.** A step on `<table>_slots` needs that table as its target,
an `Identifier`, which holds at most 40 characters (room for the adapters'
suffixes); `<table>_slots` reaches 46. The outbox's `meta_table()` builds
such a target the same way and unwraps it (crates/outbox/src/pg/store.rs),
so an outbox table of 36 to 40 characters would panic at `migrate` — read,
not measured; owed outside this decision.

**When the library goes live,** a change like this one is two steps, one a
table, its index built before the host's launch behind its release-order
gate, or by ADR-0029 decision 5's concurrent build, as ADR-0036 R11 owes.
Then an older binary must keep working on the new schema — it does, with
order per slot, which is stronger than the new contract — and a new binary
must fail to boot on a schema it cannot migrate.

**The take and the walk are functions of the baseline** (2026-11-15, the
owner's choice after the SQL alone fell short; a blind solver, a
discriminator and four roles, Change Log). Sent as text, the walk was parsed
and planned at every call: 3.3 ms against 0.4 of execution, most of the
happy path's regression. So the baseline makes, beside each inbox table:
- `<table>__take()` and `<table>__walk(smallint, integer, double
  precision)`, PL/pgSQL, volatile, run with the caller's rights;
- each body is `take_sql`'s or `walk_sql`'s statement, its one row into the
  function's columns (`INTO STRICT`, `#variable_conflict use_column`), so
  that E8 explains the statement the function runs;
- the walk's function carries `SET plan_cache_mode = force_generic_plan`:
  planned once a connection, whatever the session asks — the specification
  crate's queries need custom plans (ADR-0027), so the setting cannot be the
  connection's. The take's statement has no parameter, and its plan is
  generic anyway;
- `CREATE FUNCTION`, not `OR REPLACE`, and the checksum covers the bodies.

The dispatch calls them as two calls, as it sent two statements:
- a function that is not stable takes a snapshot for each statement, so
  the walk's is taken at its call, after the slot's lock, and the walk's
  `pg_current_snapshot()` is that one;
- one call running both would save two round trips, but the dispatch would
  no longer know the slot it holds when the walk fails (`Dispatched::slot`,
  "`None` when it took none"). The round trips are owed apart (§ Owed).

`migrate` refuses, at the start, either function that is missing or not as
the library makes it:
- not a function, or its argument types, modes and names or its result
  other than the library's, by the catalogue's values — the server's
  rendering of them changes under `quote_all_identifiers` (ADR-0037's
  lesson);
- its language, body, volatility or setting;
- run with its owner's rights;
- not executable by the pool's role;
- in another schema than its table;
- beside another function of its name, in its schema or visible on the
  search path. One with a default makes the call ambiguous (42725) and
  would stop the loops at their first dispatch; one a call cannot reach is
  refused as well, the check counting rather than resolving.

The refusal names what it found. The walk is called with its arguments cast
to its own types: untyped, they would resolve to a function of the name
taking text.

What the check requires is written in the check, not read from the
baseline, so that the baseline cannot drop it. Other functions named
`<table>__…` are let be: a newer release's (ADR-0037 R12). `teardown` drops
both; a PL/pgSQL body depends on no table it reads. When the library is
live, a changed statement is a new function under a new name by a new step,
and an older binary keeps calling its own.

### 7. The stages before the inbox: short, not fair

These are the library's parts of the principle. Each lands as its own slice,
and none reorders a FIFO stage.

**a. Batched intake.**
- The bridge into an inbox hands a batch to a new `intake_batch`: one
  multi-row insert, `ON CONFLICT DO NOTHING RETURNING`, positions drawn in
  arrival order.
- **One bad row must not stop the source** (the intake exists for that,
  ADR-0036). A multi-row insert aborts whole on a row's own refusal. When the
  batch fails on a code `intake::unstorable` names, the intake runs that
  batch again row by row through today's `admit`, each row in its own
  transaction.
- Each partition's offset is stored after the commit, with the list
  semantics. `store_offsets` stores the offset as given and `store_offset`
  stores offset + 1 (`rdkafka_offset.c:647-648, 680-681`): stored the wrong
  way, one message per partition per batch is lost.

**b. A bounded consumer queue.**
- The settings by role (ADR-0031) bound the merged queue:
  - `queued.min.messages`;
  - `queued.max.messages.kbytes`;
  - `fetch.message.max.bytes`, since the bound may be overshot by one fetch
    of each partition;
  - `fetch.queue.backoff.ms`, lowered with them.
- So a message waits behind at most about Q / R_intake. The host sets the
  values in its kernel, from its rates.
- Split partition queues (`split_partition_queue`) are a further lever. Use
  them only if the bounded queue is measured not to be enough (M5). The
  cost:
  - a re-split after every assignment;
  - the main queue polled for what was already buffered;
  - one message in hand and one offset per partition.

**c. The own lane of the relay** writes a batch's inbox rows in the
dispatcher's own transaction, one commit per batch, with the position or not
at all.

**d. The broker lane pipelines across keys only.** Within a key, m(k+1) is
produced only after m(k)'s delivery report. librdkafka documents why
("Message timeout considerations" in its INTRODUCTION.md). A message timed
out in the queue leaves a gap — "a final written message sequence of 1, 4,
5" — and `enable.gapless.guarantee` does not cover timeouts. The relay
then resends the batch, and the receiver stores 4–5 before 2–3. A lone Tenant
on one key keeps today's one message in flight.

The outbox is not made fair. A newcomer's row lies in a later epoch, and a
fair order within an epoch helps it only when the epochs are short, that is,
when the relay already keeps up.

### 8. The gates the executors need

The library has no pipeline file, and `make all` leaves out `proofs` and
`test-kafka`. The ignored tests under `src/` run in no tier: `run_ignored`
reads `crates/*/tests/*.rs` alone, and `crates/inbox/src/pg/plan_tests.rs`
is one of them. Traces are replayed, never recorded again.

This decision's first slice is the gate:
- **`.github/workflows/ci.yml`**, on every push and pull request, runs
  `make all` and `make traces`.
  - `make all` runs against PostgreSQL 14, the major the host's stand runs
    and § Measured ran on, and 18, the current one. 14's life ended on
    2026-11-12, and the host has not chosen its production major.
  - The role is neither superuser nor BYPASSRLS, as a host's service role
    is. Its database is created as the host creates its own: text ordered
    by bytes, case mapped by the server's locale.
- **`test-pg`** also runs the `--lib` ignored tests of every crate whose
  sources give its reason, `plan_tests.rs` among them.
- **`traces`** records the traces with the live tests, then runs
  `check.sh traces`, the trace checks alone.
- **`.github/workflows/proofs.yml`** runs `make proofs`, one job per group of
  `check.sh`. The whole took seven hours on four cores, and a hosted job may
  run six, so no one job can run it. The groups are the script's own list,
  and the longest took two hours.
  - It runs on a change under `verify/tla`, weekly, and on demand. The
    models read no code of the crates, and a change to the code reaches
    them through `traces`.
  - TLC is pinned: tla2tools v1.7.4, checked against its digest, the
    version every recorded result came from.

## Measured

On a scratch PostgreSQL 14.24, on a laptop of four CPUs with a warm cache.
The plan's structure and its buffers are evidence; its times only compare
with each other.

**Comparisons are made on one machine.** The host's figures cited in this
record were measured on its drill machine, which has twelve threads; this
laptop is another machine. A time is compared only within one machine:
- as an A/B on this machine — today's statement against the decision's, as
  below;
- or as a run after the change on the machine of the figure it is held
  against.

A count of states of TLC, or of rows, buffers and dispatches, depends on the
model or on the data, not on the machine, and may be compared anywhere.

**The inbox.** `BASELINE`'s shape, `ByStream`, 64 slots, with the new index
beside `__head_idx`:
- 220 000 rows: 60 000 processed at the front, and 159 990 in the queue;
- one Tenant with 150 000 queue rows, each a stream of its own;
- 999 Tenants with 10 each;
- 146 Tenants per slot on average.

**Under ordinary load:**
- today's `take` (its read part) evaluates its due-head predicate for all 64
  slots: 1.7 ms, 260 buffers;
- `locate_head`: 0.06 ms, 4 buffers;
- the walk from the start: one step, 0.26 ms, 7 buffers.

**With 90 % of the (slot, Tenant) heads in backoff:**
- today **4 of 64 slots can be taken, while 151 589 rows are due** (S2);
- under the walk, **all 64 slots reach a due Tenant head** within K = 32,
  in 8.2 steps on average (522 in all). One walk costs 0.45 ms and 47
  buffers.

**Locks.** A shared advisory lock was held and an exclusive request waited:
- `pg_try_advisory_lock_shared` returned false;
- a blocking shared request queued behind the exclusive, and was cancelled
  by a `lock_timeout` of 2 s.

**librdkafka 2.12.1.** The merged consumer queue, its aggregate thresholds,
and the gap on a timeout were read in its source and its documents, as cited
in § Context S4 and decision 7.

## Consequences

**The neighbour's wait no longer depends on B.**
- Per inbox hop: W ≤ t_in + (k + 1) · S / X.
  - k is the number of other Tenants with due heads in the slot.
  - X is the inbox's throughput in messages a second. It does not grow with
    the loops on one database: the host measured 653 → 563 Devices a second
    at 16 loops.
- Along a path, the hops add up.

**S becomes a latency parameter.** It is still fixed for the table's life. A
host sizes it from the planned maximum of replicas × loops. It chooses the
cost of its long messages, its tile size, from the bound, not from its
throughput alone.

**A cheap message is not protected from expensive ones by fairness.**
- While a slot's heads take about a second each, no non-preemptive scheduler
  that uses all its capacity can bound a cheap message by a few seconds:
  about 29 s per hop at k = 3, S = 64 and C = 8.
- A latency class needs its own inbox. The host's access lane (its ADR-0083)
  is that.

**A one-slot service keeps per-Tenant FIFO** across its channels. A poison
row stalls its own Tenant there, for 810 s with the host's retries. That
stall is what `held_counts` shows.

**New columns, a new index, and a second index on the queue.**
- Each intake writes one more index entry, so the happy path pays it.
- Measured before the slice is accepted (M9).

**`Outcome` gains `Passed` and `Deferred`; `Failure` gains `Environment` and
`later`.** Every host's match over them is exhaustive, so its build forces
the new arms.

## Rule of Three

1. **The bound is thin at launch.**
   - The figures: while tiles are the slots' heads, the bound is about
     1 + (k + 1) · 64 / 8.7 per hop on the host's figures. FR-CONFIG-0004's
     path crosses the host's inbox twice. At k ≥ 4 launch Tenants at once it
     passes 60 s.
   - Checked: the figure 8.7 tiles a second restates the oldest tile's age,
     so the bound is unproven, not refuted.
   - It is decided by the host's open-loop drill with the per-probe breakdown
     (M1, M2), at k = 3 and k = 6. The host's lever is its tile size.
2. **A shared outage is classified as the message's own.** Then today's
   storm returns per (slot, Tenant): every head tried and parked.
   - Checked: `From<E> for Failure` gives `Transient`, so an unclassified
     error is the message's own. The host's verdict chain has two classes
     today: its kernel's `workflow_verdict`.
   - Executor: the failure-injection drill (M8). A shared dependency's
     outage parks nothing and pauses the loops.
3. **A dormant slot sleeps past a row made due.** Three causes:
   - a dependency's mark that woke a row;
   - an unpark, which makes a row due at once;
   - an arrival whose transaction was in flight when the round began. Its
     position was drawn at its insert, below `dormant_after`, and its commit
     came after the snapshot of the round's first walk. The class is
     ADR-0035's: the order a position is drawn in is not the order of
     commits.

   A fourth cause was in the first text and is closed: `dormant_after` read
   at the walk that ends the round strands a row committed during the round
   for a Tenant the round has passed. It is read at the round's start
   instead (decision 2), and `InboxPgDormantAfterAtWalk.cfg` must fail.

   None of them is seen as a newer arrival. An expired wait parks its row,
   so it never enters the queue. Checked:
   - the wake writes no slot row, and must not: it runs in another slot's
     transaction, and writing the woken rows' slots would break ADR-0009's
     lock order;
   - the third cause is found by reading the statements, not by today's
     `InboxPg.tla`, whose `Receive` is atomic.

   So dormancy is capped by `max_dormancy`. A wake exact for the third cause
   needs each row's transaction id and the walk's snapshot, as the outbox's
   epochs have. That is decided only if the cap is measured to cost too much.

   Executors:
   - a woken, an unparked and an in-flight arrival in a dormant slot are
     each taken within the cap;
   - E13's model splits `Receive` into the draw of the position and the
     commit, and proves the bound.
4. **Deferral starves under exclusive requests that keep coming.** Checked:
   measured above. The library's `max_deferral` parks it, and the host's
   fallback to a blocking lock gives it its turn. Executor: alternating
   exclusive holds; a deferred placement finishes within one hold plus the
   host's bound.

## Alternatives Considered

- **Accept it and declare a window.** Rejected:
  - the neighbour did nothing to be given a window;
  - the wait grows with the launch Tenants;
  - S2 needs no large Tenant at all.
- **More slots.** Rejected: the wait is B/μ whatever S is.
- **Selection by readiness, whatever the arrival** (the host's TD-1).
  Rejected: a launch backlog is already ready, a million streams with one
  message each. A choice among ready heads that knows no Tenant is FIFO
  again, or B/2μ when random.
- **A slot or a URI per Tenant, or shuffle-sharding Tenants onto k slots.**
  Rejected: a lone Tenant gets one or k loops (objection 2).
- **A cap or a reserve of loops per Tenant.** Rejected on two grounds:
  - Neither uses all its capacity when a Tenant is alone.
  - A reserve cannot be one statement. A dispatcher has no identity, no one
    knows the number of loops, and a holder's uncommitted writes are
    invisible.
- **A `served_at` per Tenant, or a global cursor.** Rejected: the dispatch
  is one transaction with the handler inside, so the row would be held
  through every handler, and a lone Tenant's loops would serialize on it.
  Out of the transaction, it costs a commit per message.
- **Virtual-time stamps at the intake** (start-time fair queuing). Rejected:
  a per-Tenant row is locked at every insert, or capacity is estimated, and
  concurrent intakes may stamp one stream out of order.
- **Priority lanes, live against bulk.** Rejected: a live message may
  overtake the one it follows, which breaks causal consistency, and they do
  nothing for bulk against bulk across Tenants.
- **An inbox per large Tenant.** Rejected: the set of large Tenants changes,
  and a deployment has thousands of Tenants.
- **A failing head holds only its stream** (the author's first position).
  Rejected:
  - it reorders within a Tenant, which nothing licenses;
  - it needs the walk by key that ADR-0008 rejected;
  - under a common cause it tries every stream's head.

  It can be decided on its own later, with a failure-injection drill.
- **A failing head holds its (slot, Tenant), whatever the cause** (the
  solver's position). Rejected as incomplete: under a common cause it parks
  S·T rows. Decision 3 keeps it for the message's own failures.
- **Try-and-defer with no bound.** Rejected: it starves (measured), and it
  is the endless silent retry ADR-0010 refused.
- **Fairness at the broker's consumer** — split partition queues polled in
  turn — as the first lever. Rejected as first: it reorders a FIFO stage's
  service, which the principle reserves for the inbox, and its cost is high.
  The bounded merged queue comes first; split queues only if M5 shows they
  are needed.
- **A finer broker key, such as the Device.** Rejected (objection 1). The
  host records it.
- **A fair outbox, or an outbox cut by URI.** Rejected:
  - fairness within an epoch helps only when the relay already keeps up;
  - a cut by URI confines a lone Tenant's broker rows to one loop.
- **An index on `served_at`, so that `take` stops at the first slot.**
  Rejected: it defeats HOT on the slot rows, and its dead entries lie ahead
  of the scan, more with every horizon held open. The `EXISTS` guard and the
  walk in `locate_head` make it unnecessary.
- **Dropping `__head_idx`.** Rejected: a rolled-back binary would sort every
  slot's queue, a silent slowdown, and dormancy needs the arrival order.
- **Re-cutting a host's S now.** Rejected:
  - a cut is one-way;
  - the stand's teardown loses Commands to self in flight, the repeat
    memory, the dependency targets and the parked evidence;
  - on PostgreSQL 14 a stored generated column cannot change its expression
    in place.

## Executors

Each is listed with the mutation that turns it red.

**The library's tests, against PostgreSQL** — run by `test-pg` once
decision 8 lands:
- **E1, rotation.** S = 4 and one loop; Tenant A with 1 000 rows across the
  slots, Tenant B with one. B is processed within 2S dispatches, and every
  row is drained. It runs twice, with B sorting first and B sorting last.
  The generalised form takes k Tenants and the bound (k + 1) · S. Red when:
  - `locate_head` returns to the slot's oldest row;
  - the wrap branch is missing;
  - the wrap excludes the cursor's own Tenant;
  - the cursor is never written or never read.
- **E2, a lone Tenant uses the capacity.** C = 4 and a slow subscriber; at
  least four messages in flight at once; more than S·C rows drained in all.
  Red when the slot's key is `tenant_id`.
- **E3, a head in backoff holds its (slot, Tenant)**, at S = 1 and at
  S = 64:
  - B's row in A's slot is processed at the next dispatch;
  - A's second stream in that slot waits.

  Red when the due check returns to the slot's oldest row, or when a head
  holds only its stream.
- **E4, K and dormancy.**
  - K = 2, five backed-off Tenants ahead of one that is due: exactly two
    `Passed`, then `Processed`.
  - T = 5 > K, every head in backoff, `run` for 1 s at a 200 ms poll: at
    most ⌈T/K⌉ + 5 dispatches.
  - A woken and an unparked row in a dormant slot are each taken within
    `max_dormancy`.
  - A row arriving in a dormant slot after the walk is taken at the next
    dispatch.
  - K = 1, three Tenants with heads in backoff: a row of a Tenant the round
    has passed, committed between its walks, is taken once the round ends,
    not after `max_dormancy`.
  - A row whose insert was in flight when the round began is taken within
    `max_dormancy`.

  Red when:
  - `take` qualifies a slot on "a due head found";
  - `Passed` never ends a round;
  - the cap is ignored;
  - the walk reads K + 1 Tenants, or reads without a bound.
- **E5, the environment's failure.** It parks nothing, counts no attempt and
  pauses the loop. Red when `Environment` is counted as an attempt.
- **E6, a rollback moves the cursor.** A's backend is terminated inside its
  handler; B's row in the same slot is processed within two dispatches. Red
  without the statement of its own.
- **E7, a deferral, through the host's whole verdict chain:**
  engine → `CommandFailure` → `workflow_verdict` → inbox. The row fails once
  (attempts = 1), then is deferred `max_attempts + 1` times:
  - attempts stay 1;
  - the row does not park before `max_deferral`;
  - `next_attempt_at` is at least now + `after`;
  - past `max_deferral` the row is parked under `Deferred`.

  This is a pin in the host's `seedwork_pins`. Red when a link maps a
  deferral to `Transient`, or when `later` resets attempts to 0.
- **E8, the plans** (`plan_tests.rs`), at S = 1 and S = 64:
  - one Tenant with a long processed history;
  - a Tenant the last `ANALYZE` did not see.

  `take` and the walk go by `__head_idx` and the new index, with no sort and
  no primary key, under the custom plan and under the generic one the
  functions run. Red when the new index is dropped.
  The generic half asserts its plan holds the slot as a parameter, `$1`, so
  that it cannot pass on a custom plan.
- **E8b, the take and the walk planned once a connection**
  (`plan_tests.rs`). On a connection of the test's, asking for custom plans,
  ten calls of each function make twenty-two plans by the server's own
  report (`debug_print_plan`): each call's, and each function's statement
  once. Red when the walk's `SET plan_cache_mode` is dropped, from the
  baseline and from the check alike: thirty-one (measured, as twenty on the
  walk alone).
- **E8c, the functions as the library makes them, and called**
  (`intake.rs`). `migrate` refuses:
  - a walk function dropped, without its setting, stable, or with a column
    of another type;
  - a take function with its owner's rights, without EXECUTE, its body
    edited, in another language, replaced by a procedure, beside an
    overload, or in another schema than its table;
  - an overload in a schema the search path puts first.

  It passes under `quote_all_identifiers` and lets a `<table>__walk_v2` be.
  The dispatch fails, 42883, without either function; the walk of a slot
  without its row fails, P0002; and the dispatch reaches the walk past a
  function of its name taking text, made after the start. Each part is red
  under its mutation (Change Log). A teardown that keeps the functions
  fails the next baseline (measured).
- **E9, `held_counts`.** By channel and cause, distinct Tenants and the
  oldest, bounded by `within`. Red when the gauge reads the observer.
- **E9b, `unpark_where`.** Rows of one channel parked on one ground within a
  window are unparked and processed; one parked after the window, one on
  another ground and one of another channel stay. Red without any one of the
  three filters.
- **E10, batched intake.** In `crates/inbox/tests/bridge.rs`:
  - a batch of three whose middle row carries a NUL in its JSON stores two,
    sets one aside, and stores the offsets;
  - a process killed after the commit is restarted and finds each message
    once.

  Red without the row-by-row fallback, or with `store_offset(last + 1)`.
- **E11, the own lane.** A batch's inbox rows commit with the position or
  not at all. Red when rows commit one session each.
- **E12, the broker lane.** On a stand, one key, the leader paused 10.5 s:
  no inversion of `stream_position`. Red when sends pipeline within a key.

**The models** (`proofs`):
- **E13, the TLA+ models** (`verify/tla`, run by `check.sh`).
  - **Built: the rotation in `Inbox.tla`.**
    - Every message has a Tenant (`TenantOf`), and every slot a `cursor`.
    - Rotating, `Fetch` and `Wait` take the oldest row of the first Tenant
      after the cursor, cyclically, whose head is due. Both move the cursor.
      A crash leaves it moved, as decision 3 has it on a rollback.
    - A head in its backoff holds its (slot, Tenant), not the slot.
    - `TenantOf` and `Rotates` are definitions, with one Tenant and a FIFO
      by default. So every configuration and every instance that does not
      replace them checks what it checked before: `InboxPg`, `Bridge`,
      `InboxForget`, and the recorded traces. The whole suite passes.
  - **Built: the properties.**
    - `ArrivalOrder` is per (slot, Tenant).
    - `BoundedOvertaking`, in `MCInboxRotation.tla` over a history variable:
      while a message is a due head, its slot takes or sets aside at most
      one head of each other Tenant. One large Tenant overtakes it once,
      whatever its backlog. NT − 1 in all follows, which is what the per-hop
      bound of § Consequences, W ≤ t_in + (k + 1) · S / X, counts.
    - `EventuallyProcessedBesidePoison`: a message that shares no (slot,
      Tenant) with a poison message is processed or parked, even when the
      poison is never parked.
  - **Built: the configurations.** In each, m1 and m2 are of one Tenant and
    m3 of another.
    - These hold: `InboxRotation.cfg` (the instance of `Inbox.cfg`),
      `InboxRotationPoison.cfg` and `InboxRotationPoisonNoParking.cfg`.
    - `InboxFifoSlot.cfg`, the slot as one FIFO, must fail
      `BoundedOvertaking`.
    - `InboxCursorStays.cfg`, a cursor never written, must fail it too: the
      rotation has become a priority by Tenant.
    - `InboxPoisonHoldsSlot.cfg`, the FIFO behind a poison head never
      parked, must fail `EventuallyProcessedBesidePoison`.
    - `InboxNewestOfTenant.cfg`, a walk taking its Tenant's newest row, must
      fail `ArrivalOrder`, its cfg listing that property alone.
  - **Built: three Tenants, in `MCInboxThree.tla`.** Two Tenants cannot
    tell "once each" from "NT − 1 in all", nor a walk after the cursor from
    one that goes by Tenant number and skips only the cursor's Tenant
    (`tenant_id <> last_tenant ORDER BY tenant_id`). One slot holds m1 and
    m2 of Tenant 1, m2 depending on m1, m3 of Tenant 2 and m4 of Tenant 3.
    - `InboxRotationThree.cfg` holds, `BoundedOvertaking` included.
    - `InboxWalkFromLowest.cfg`, that walk, must fail `BoundedOvertaking`.
      On `InboxRotation.cfg`'s two Tenants the same walk holds.
  - **Built: the statements, in `InboxPg.tla`.** These are what the
    abstract walk, which sees every Tenant, has no need of.
    - **The statements.** The walk inside the head's statement examines at
      most K Tenants after the cursor and wraps once. `Passed` comes when
      none of their heads is due. The round runs from the cursor it began
      at to the walk that passes it, and its end is the dormancy. The take
      qualifies a slot by a queue row and its not being asleep.
    - **What the rotation brings in.** A subscriber's own failure and the
      backoff it leaves come in with it (`Fails`). So does `Receive` split
      into the draw of the position and the commit (`SplitReceive`), so
      that the in-flight arrival of § Rule of Three 3 is in the state space.
    - **The refinement.** `InboxPg` refines the rotating `Inbox`:
      `last_tenant` maps to `cursor`, and a walk that found nothing to
      `Inbox.tla`'s `Pass`, which skips no due head. A set-aside is the
      protocol's `Wait` at the walk that chose the head. The cursor the
      walk moved then commits, even when the dependency was marked before
      the wait was set, as the implementation commits it. In the protocol
      that is a head set aside and woken at once, and it spends the
      Tenant's turn.
    - **`NoDueHeadAsleep`.** A slot sleeps past a due head only when the
      head's insert was in flight when the round began, or a dependency's
      mark woke it after.
    - **`PassedPerRound`**, over a history variable in
      `MCInboxPgRotation.tla`: while nothing in the slot changes, at most
      ⌈T_s/K⌉ `Passed` in a row, then the `Nothing`. The bound is not
      ⌈T_s/K⌉ − 1: TLC found the extra walk of decision 2. E4 holds the
      rate in time.
    - **`InboxPgRotation.cfg`** holds: one slot, one loop, three Tenants of
      a row each, K = 1, two failures, inserts committed out of order.
      `Refines`, `EventuallyProcessed` and every invariant hold.
    - **`InboxPgRotationWide.cfg`** holds: the same with K = 3, a round of
      one walk.
    - **`InboxPgRotationDeps.cfg`** holds: rotating beside a dependency, m2
      depending on m1 of the same Tenant, in a cut that puts m1 in another
      slot and in one that puts all three together, with two loops and one
      failure. `Refines` holds with the set-aside at the walk, and so do
      `CausalOrder`, `WaitingIsAside`, `NoDeadlock` and
      `EventuallyProcessed`.
    - **Must fail:**
      - `InboxPgDormantAfterAtWalk.cfg` must fail `NoDueHeadAsleep`. It is
        the first text of decision 2.
      - `InboxPgNoRoundEnd.cfg` and `InboxPgPassedStays.cfg`, a walk that
        finds nothing and leaves the cursor, must fail `PassedPerRound`.
      - `InboxPgNewestOfTenant.cfg`, the walk taking its Tenant's newest
        row, must fail `ArrivalOrder`. It stands where the plan had
        `InboxPgAnyRowOfTenant.cfg`.
      - `InboxPgSleepsPastBackoff.cfg`, a dormancy that the backoffs its
        round saw do not end, as a `dormant_until` set to the cap would be,
        must fail `EventuallyProcessed` without the cap. It shows that
        `InboxPgRotationNoCap.cfg` could fail.
  - **Not a must-fail configuration: the cap.** The plan had
    `InboxPgNoDormancyCap.cfg` failing a liveness property, and it cannot. A
    dormancy ends when the earliest backoff its round saw passes, and every
    head the round examined was in backoff. `InboxPgRotationNoCap.cfg`,
    `InboxPgRotation.cfg` without `max_dormancy`, holds
    `EventuallyProcessed`, so liveness is checked not to rest on the cap. A
    model without time cannot tell `max_dormancy` from a backoff of 300 s.
    The cap's executor is E4's timed test.
  - **Owed with slice 2, in `InboxPg.tla`.** The rollback of a rotating
    dispatch is not modelled. The cursor the walk wrote vanishes with it, so
    the refinement needs decision 3's statement, which writes it again.
    - It must be modelled together with a mutation that leaves the cursor
      unwritten.
    - It must also cover the race of that statement with another loop's
      take: `served_at` rolls back too, so the slot is again the least
      recently served.
  - **Owed with slices 2 and 3, in `Inbox.tla`: the failure's scope.**
    - `NoParkOnEnvironment`: an environment failure neither parks nor counts
      an attempt. `InboxEnvCounted.cfg` must fail it.
    - `EventuallyDisposed`: a deferred row is processed or parked within
      `MaxDeferral`. `InboxUnboundedDeferral.cfg` must fail it.
- **E14, trace validation.** It follows `passed`, `deferred` and the
  environment's rollback. Receive records carry the Tenant.

## The owner's decisions

These are the host's; the library's part does not wait for them.
1. **Decided (the owner, 2026-11-14).** QAS-SCALE-0001's "±5 %" for
   co-Tenants cannot be met under saturation by any scheduler that uses all
   its capacity. It is replaced by an absolute bound per hop, summed along
   the path: each co-Tenant flow's own budget end to end, allocated over its
   hops, each hop's share independent of the burst's backlog. The host's
   QAS-SCALE-0001 records it.
2. The access lane as a precondition of the host's adoption, not a priority.
3. Latency within a Tenant: the launch Tenant's own enrolment waited 19.5
   minutes behind its own change. Nothing here addresses it.
4. Work under the platform's sentinel Tenant takes one share of the
   rotation: a lane of its own, or a fixed weight.

## Owed, outside this decision

**Owed by the host**, each in its own records:
- its locks:
  - placements and tiles try their Tenant's shared hold and defer, falling
    back to the blocking lock past one hold;
  - a change first tries a per-Tenant change key and defers, then takes the
    exclusive partition hold;
- its S, sized from replicas × loops with an upper-bound assert, and its tile
  size from the bound;
- the access lane;
- the receiver of a per-Device flow — BC-DEVICE first — cut into slots and
  batched before any producer lever raises its input;
- the consumer's settings by role, pinned in the kernel's unit test;
- the production canary Tenant, end to end and on access, and the scaled
  drill counted in rows rather than time;
- its ADR-0035: TD-1 rewritten, and Part 2 keeping the Tenant key with its
  quotas as protection against abuse;
- the alarms' quantities, sources, thresholds and owners.

**Owed by the library, found by decision 8's gate** (verify/tla/README.md,
Trace validation):
- trace validation of the outbox's epoch relay (ADR-0035): eleven traces of
  its tests are recorded and not covered, since `TraceOutbox.tla` follows
  the relay that releases below `xmin`;
- two inbox traces that do not fit, not yet diagnosed
  (`inbox-held_by_concurrent`, `inbox-window_passed`); a third,
  `inbox-identity_past`, maps to no step and is refused;
- **the rotation in the traces.** `TraceInbox.tla` substitutes a cursor that
  never moves, so it follows a slot served in arrival order. A recorded walk
  that passed a Tenant, or served a later Tenant's head first, does not fit,
  and no test of the rotation is covered. Since every test that records
  serves one Tenant a slot or fits the FIFO order, a walk that wrongly found
  nothing fits whenever the slot's oldest row waits for its back-off. The
  refinement of a rotating run is owed with E13's trace part;
- **a positive bridge trace with a row set aside.** `bridge-outbox-set-aside`
  was withdrawn when its test began writing its row by SQL (ADR-0038), so
  the bridge's `set_aside` and `acknowledge` steps are exercised only by the
  forged run that must not fit: a mapping that never produces them would
  pass. A test that sets a row aside through the producer, recorded and
  covered, is owed;
- the outbox's `meta_table()` unwraps `Identifier::new("{table}_meta")`,
  which refuses a name past 40 characters: an outbox table of 36 to 40
  would panic at `migrate` (read, not measured).

**Owed by the library, found by the round on the functions** (decision 6,
2026-11-15). Each is older than this decision, which neither causes nor
cures it; each is a decision under the library's gate.
- **ADR-0009's premise is not met.** It keeps nothing session-scoped for
  PgBouncer's transaction pooling. But tokio-postgres sends every statement
  given as text as a named statement:
  - Parse, Describe and Sync, awaited, then Bind and Execute
    (tokio-postgres 0.7.18, `prepare.rs`, `query.rs`);
  - outside a transaction, the Sync ends it, and a pooler may run the Bind
    on another server connection: 26000, or another client's statement of
    the same name, the names being a per-process counter.

  Outside a transaction today:
  - the ledger's read and `forget` (migration);
  - `migrate`'s read of the cut, `SHOW block_size`;
  - the dispatch's expiry;
  - the operator's surface (`surface.rs`) and the sweep;
  - three of the outbox's statements;
  - `parked`, `unpark` and `unpark_where` on a session outside one.

  Measurement: PgBouncer in transaction mode, `max_prepared_statements` 0
  against 200, `migrate` and 1,000 dispatches, counting 26000, 42P05 and
  08P01. Candidates: one-message calls in the session crate, unnamed and
  typed, which also halve every statement's round trips; or the pooler's
  support recorded as a precondition.
- **READ COMMITTED is assumed and not enforced.** `BEGIN` is bare
  (`session.rs`), and a role's default of REPEATABLE READ:
  - loses a wake in ADR-0009's protocol: the wait's re-check and the mark's
    wake read the transaction's first snapshot;
  - turns the take and the walk's fresh snapshot into the transaction's:
    the take then raises 40001 on a slot changed since, which is
    transient — noise, not a wrong head.

  Measurement: the dependency tests under `default_transaction_isolation =
  'repeatable read'`. Candidates: the session crate begins READ COMMITTED,
  or the dispatch refuses another level.
- **Round trips.** A happy-path dispatch makes about 18. At a network's
  0.3 ms each, that is 5 ms a message, more than the regression was. The
  one-message calls above halve them. Measurement: M9 under a delay on
  loopback (`tc netem`, the owner's sudo).
- **A role with CREATE on a schema in the search path** can, after the
  start check, plant an overload of either function — the next call is
  ambiguous, 42725, and the loops stop — or an operator the walk then
  resolves to (CVE-2018-1058). This is today's exposure for operators
  already. It is the host's to close: no CREATE on the schemas in the search
  path, PostgreSQL 15's default for `public`. The library refuses an
  overload at the start.
- **The functions' owner is not compared** (the Security role). Such a role
  could also make a function the library's text exactly, in place of one
  gone missing, and own it: the start check passes, and the owner may
  replace its body after. No owner is compared today on the tables either.
  A check of `proowner` against the table's owner needs a second role in
  the tests to hold it, which the test role cannot create. The same role
  would hold the README's precondition that `migrate` runs as the role that
  dispatches: `has_function_privilege` checks the role that migrates, and
  every test has one.
- **A repair of the functions without the queue's loss** (the Operability
  role). A body, signature or language refused at the start has no remedy
  but a database made again (ADR-0029 decision 2), which drops the queue,
  the parked rows and the duplicates' window. The functions hold no data
  and nothing depends on them, so an operator's repair — both dropped and
  made again in one transaction — loses nothing. It is not done at the
  start: two releases rolling out would make each other's in turn.
- **The live day's rule has no executor** (the Operability role). A
  changed statement is a new function under a new name; no step after the
  baseline may replace `<table>__take` or `<table>__walk`, or a release
  rolling out runs the other's body. Until the library is live the
  baseline's pinned checksum holds it. Then a test is owed that no later
  step names them, which needs a step's SQL: `Migration` gives its id and
  checksum only.

**Open measurements.**

Each is an A/B on one machine (§ Measured): before and after the change on
the same machine, or after the change on the machine whose figure it is held
against. The host's drill figures — 21 minutes, 430 s, its rates — are held
only against runs on the host's drill machine.

**Where each runs.** A launch-size drill is commit-bound. The 3×1M slice wrote
112 GB of WAL in its import wave, and its database was about 33 GB. A machine
with a spinning disk and less memory than that measures its disk, not the
scheduler. So:
- **On any machine, as an A/B or as a count:**
  - the models;
  - the library's tests;
  - the plans (M7);
  - the happy path's cost (M9);
  - the scaled drill, counted in rows, not time (E5 in `test-workspace`).
- **On any machine, as a ratio:** the structural drills (M1, M2, M8) at
  3 × 10 000 Devices. Each is run at a backlog B and at 3B, before and after,
  with the same database settings in all four runs. Under the FIFO the wait
  grows with B; under the rotation it does not.
- **On the host's drill machine,** once, after the change: the 3×1M shape of
  slice 1g-3b, against that slice's own figures; and M6.
- **On a production-shaped environment** (the host's ADR-0084 § Owed): the
  absolute thresholds and M5 on Kafka.

| | What | Decides |
| --- | --- | --- |
| M1 | the breakdown per probe: sent, row stored, processed, and the rows ahead | which stage holds a wait |
| M2 | an open-loop probe, weighted by time | FR-CONFIG-0004's p95 |
| M3 | an end-to-end probe through the neighbour's own outbox | whether the relay holds it |
| M4 | relay rows a second per lane, on Redpanda, against peak inserts | decision 7c and 7d's sufficiency |
| M5 | partition A flooded beyond intake, against a message on B, under the bounded queue | whether split queues are needed |
| M6 | the new index's build on a retained, production-sized inbox | the host's release order |
| M7 | `take` and the walk on the scratch shape above, at 10 000 Tenants | `K` and the busy cost |
| M8 | failure injection: one poison row, one Tenant's outage, a shared outage | decision 3, in dispatches a second, rows parked and the neighbours' latency |
| M9 | the happy path with two queue indexes | the second index's price, at most 0.1 ms a message |

## Slices

0. The gates (decision 8).
1. The contract, the rotation and dormancy, the migration, `received_at` and
   `held_since` with `held_counts` (decisions 1, 2, 5, 6), and E1–E4, E8,
   E9, E13.
2. The failure's scope: `Environment` and the cursor on rollback (decision
   3), with E5, E6, E14 and E13's `NoParkOnEnvironment`. Moved to ADR-0040's
   revision, 2026-11-16.
3. The deferral (decision 4), with E7 and E13's `EventuallyDisposed`. Moved
   with slice 2.
4. Batched intake and the consumer's bounds (decision 7a, 7b), with E10.
5. The relay's lanes (decision 7c, 7d), with E11, E12.

## References

- ADR-0005, ADR-0006, ADR-0007, ADR-0008, ADR-0009, ADR-0010 (the classes of
  failure), ADR-0029 (migrations), ADR-0031 (settings by role), ADR-0033,
  ADR-0035 (the relay's epochs), ADR-0036 (the environment holds the source;
  R9, R11), ADR-0037.
- The host's ADR-0035 (Part 2 and TD-1), ADR-0083 (the relay's settings and
  the access lane), ADR-0084 (§ Owed: fairness between Tenants at the inbox;
  slice 1g-3b's drill).
- librdkafka 2.12.1: `CONFIGURATION.md` (`queued.min.messages`,
  `queued.max.messages.kbytes`, `fetch.queue.backoff.ms`),
  `INTRODUCTION.md` ("Message timeout considerations"),
  `src/rdkafka_assignment.c`, `src/rdkafka_partition.c`,
  `src/rdkafka_fetcher.c`, `src/rdkafka_offset.c`. rust-rdkafka 0.39.0:
  `split_partition_queue`.
- PostgreSQL 14: the lock manager's queue (`LockAcquireExtended` against
  `waitMask`), advisory locks, HOT updates.

## Change Log

- 2026-11-14 — Proposed: a blind solver and a discriminator, then five
  roles. The synthesis after the roles, with the author's measurements on a
  scratch PostgreSQL 14.24.
- 2026-11-14 — **The in-flight arrival.** Found while planning E13's model,
  and not by the round. A row whose position was drawn before a walk and
  committed after it does not wake a dormant slot; it waits for the cap.
  - § Rule of Three 3 and decision 2 now say so.
  - E13 splits `InboxPg`'s `Receive`, which was atomic, and gains
    `PassedPerRound`, `EventuallyTaken`, `NoParkOnEnvironment` and
    `EventuallyDisposed`, each with a configuration that must fail.
- 2026-11-14 — **E13's abstract part built.** `Inbox.tla` rotates, with the
  properties and configurations E13 lists as built. The whole suite passes.
  Three changes from the plan:
  - The mutation of the head's row is checked first at the abstract level,
    as `InboxNewestOfTenant.cfg`. `InboxPgAnyRowOfTenant.cfg` stays owed for
    the statement.
  - `InboxCursorStays.cfg` is added: without it nothing showed that the
    cursor matters.
  - `BoundedOvertaking` is stated as what it checks, NT − 1 heads in all,
    not "one head of each other Tenant", which is stronger from three
    Tenants up. The instance has two, so an instance of three is owed.
- 2026-11-14 — **E13's statements built, and what TLC found in decision 2.**
  `InboxPg.tla` rotates, with K, `Passed`, the round, dormancy, failures and
  a split `Receive`, and refines the rotating `Inbox.tla`, which gains
  `Pass`. What follows from it:
  - **`dormant_after` is read when the round begins, not when it ends.** At
    the end, a row committed during the round for a Tenant the round has
    passed lies below it, and the slot sleeps past it until `max_dormancy`.
    A round outlives a dispatch whenever T_s > K. TLC's counterexample:
    three Tenants, K = 1. Decision 2 now says so, and states where the
    round's running earliest backoff lives: in `dormant_until`, read as no
    dormancy while `round_from` is set. Rule of Three 3 names the closed
    cause, and the class left to the cap becomes the inserts in flight when
    the round began.
  - **At most ⌈T_s/K⌉ `Passed` a round, not fewer.** When the Tenant served
    last has no row left, the walk that examined the last Tenant before its
    place cannot know the round is complete. This edit's own first wording,
    "fewer than", was refuted by TLC; the earlier "about ⌈T_s/K⌉" stands.
  - **A dispatch that found its dependency marked commits the cursor.** That
    is no defect: in the protocol the head was set aside and woken at once,
    and its turn is spent. The refinement maps the set-aside at the walk.
  - **No must-fail configuration for the cap.** Liveness cannot fail
    without it, and `InboxPgRotationNoCap.cfg` holds to show it; the cap's
    executor is E4. The plan's `EventuallyTaken` is not a property of its
    own: `EventuallyProcessed`, which every rotating configuration checks,
    implies it.
  - **The rollback of a rotating dispatch moves to slice 2's model**, with
    decision 3's statement and its race with another loop's take.
  - **The head's row, statement by statement**, owed by the entry above, is
    `InboxPgNewestOfTenant.cfg`.
- 2026-11-15 — **`BoundedOvertaking` per Tenant, over three Tenants.** It
  now bounds each other Tenant to one head while a message is a due head,
  which is the rotation's point: one large Tenant overtakes it once. NT − 1
  in all follows, so the per-hop bound is unchanged. `MCInboxThree.tla` has
  three Tenants in one slot, `InboxRotationThree.cfg` holds, and
  `InboxWalkFromLowest.cfg`, a walk by Tenant number that skips only the
  cursor's Tenant, must fail it; on two Tenants that walk holds. E13's
  last debt of slice 1 is closed.
- 2026-11-15 — **Decision 8's pipeline, as measured.** "A pipeline file runs
  `make all proofs`" cannot hold: the whole of `check.sh` took seven hours on
  four cores, and a hosted job may run six. So:
  - `make all` and `make traces` run on every push;
  - `make proofs` runs in a workflow of its own, a job per group of
    `check.sh`, on a change under `verify/tla`, weekly and on demand.

  `check.sh` gains its groups (`--groups`, `check.sh <group>`), and `traces`
  checks the traces alone after recording them. Two more choices:
  - The live tests run against PostgreSQL 14 and 18. 14's life ended on
    2026-11-12, and the host's production major is not chosen.
  - The test database orders text by bytes, as the host's databases do.

  The gate's first run, before any code of this decision, found a test
  whose time was not its claim's. `a_current_schema_starts_beside_its_
  tables_held_exclusively` (`crates/migration`) bounds a read of a current
  schema by 300 ms. The time included a new connection's SCRAM handshake,
  which in a debug build, beside the file's other tests on four cores, took
  most of that: it failed 6 runs in 6 at 303–430 ms, passed alone, and
  passed with the handshake removed (trust: the file in 0.8 s against 5.2).
  The test now reads over a warm pool, so the time is the read's, the lock
  wait it asserts against included.

  So did `two_slots_are_dispatched_at_once` (`crates/outbox`): it bounded two
  dispatches of 300 ms by 550 ms, and the second needed a new connection,
  whose handshake fell inside. It failed 2 runs in 2 in its file, at 640 and
  854 ms, on 14 and on 18 alike, and passed alone. It now asserts what it
  claims, as its neighbour does: the two subscribers' spans overlap. Run in
  series, the pair fails it.

  The run on 18 found a defect of the library's own: `migrate` refuses every
  inbox there, since 18 records a column's NOT NULL in `pg_constraint` and
  ADR-0037 decision 5's check reads the library's own as a host's. 82 live
  tests fail on 18 for it, and nothing else. It is mended under ADR-0037.

  With the inbox mended, the run on 18 failed once on
  `messages_of_one_uri_are_never_dispatched_at_once` (`crates/outbox`),
  whose evidence is already an overlap: its eight spans ran in a row, 345 ms
  in all, the second loop never in. That loop's connection was still in its
  handshake. Both tests of two dispatchers at once now open two connections
  first, by two dispatches that find nothing, and race without a handshake.

  `make traces` passed first without checking anything new. It set
  `ASCETIC_DDD_TRACE_DIR` relative, as the README's command did, and cargo
  runs a test in its crate's directory: the traces landed under
  `crates/inbox/verify/` and `crates/outbox/verify/`, and `check.sh traces`
  checked the committed ones. That is why the traces were "replayed, never
  recorded again". The directory is absolute now, in the target and in the
  README.

  `plan_tests.rs`, run by a tier for the first time, failed on 18 alone: 18
  prints `EXPLAIN`'s "Actual Rows" with its fraction (20.0), and the test
  read it as an integer. The plan was the one it asserts. It reads a number
  now, still exactly 20.

  Recorded again for the first time, 27 of the 29 traces the repository
  held fit; the other two do not, on 14 as on 18, three runs in three, and
  neither is a defect of the code:
  - `outbox-visibility`: the epoch relay of ADR-0035 lets a committed
    version leave past an open one, as its test claims; `TraceOutbox.tla`
    follows the relay before it;
  - `bridge-outbox-set-aside`: its test writes a row by SQL since ADR-0038,
    behind the observer, so its trace lacks a publication.

  39 tests recorded traces the repository never held: 23 fit and are
  covered now; 16 are not. `make traces` covers what the repository holds,
  replaces each by its recording, fails on one no test recorded again, and
  names the recordings it does not cover. A test that writes rows behind the
  observers records none — that one and three of the inbox's. What is left
  uncovered is owed (§ Owed).
- 2026-11-15 — **Decision 6: the schema in the baseline.** The owner: no
  compatibility or migration is required yet, the library's only client
  being the host's tests. A step on `<table>_slots` cannot name its target,
  since `Identifier` holds 40 characters and the name reaches 46; of the
  three ways round it — the schema in the baseline, a derived `Identifier`
  in the session crate, an inbox table name bounded at 34 — the owner chose
  the first. Databases made before are recreated. `received_at` becomes NOT
  NULL with its default, since no older binary's insert must be served; a
  superseded row's is set again (`a_row_parked_at_once_is_superseded_…`,
  red without it). Found on the way: the outbox's `meta_table()` would panic
  for a table name of 36 to 40 characters (§ Owed).
- 2026-11-15 — **Slice 1: the walk and the take built (decision 2).**
  - `locate_head` is the walk, in one statement: a recursive descent of
    `__tenant_head_idx`, Tenants after the cursor and then from the start to
    the cursor's own, at most `K`, stopping at the first due head, which it
    locks; then the round's verdict, as `InboxPg.tla` states it (it ends when
    the walk has seen every Tenant or passed the cursor it began at); then
    one `UPDATE` of the slot row: `served_at`, the cursor, the round. A cursor
    never set reads as `''`, so the first round is the others' case.
  - `take` is a `SELECT … FOR UPDATE SKIP LOCKED` of the least recently
    served slot that has a queue row and is awake, behind one look at the
    whole queue. It writes nothing; the walk writes `served_at`.
  - `Outcome::Passed`; a loop goes on after it without a pause.
  - `K` and `max_dormancy` are `Rotation`, a setting of the inbox, not
    `Loops::max_dormancy` as decision 2 first had it: `run` takes `Loops` of
    its own, and a dispatch outside `run` has none, so a walk reading the
    loops would read the builder's while `run` was given others — an
    executor showed it.
  - Executors, on 14 and 18: E1 (both sort orders, 1 000 rows), E2, E3 (one
    slot and 64), and E4's six cases — the bound K, the bound on writes, an
    arrival waking a sleeping slot, a row committed behind a round's walk
    (`InboxPgDormantAfterAtWalk.cfg`'s case, in PostgreSQL), and the cap
    over an unparked row, an insert in flight when the round began and a
    row woken from another slot. Each of the twelve mutations E1–E4 name
    turns one of them red. E1's generalised form, k Tenants and the bound
    (k + 1) · S, is not built.
  - Every live test the library had passes unchanged, and every covered
    trace still fits: they keep one Tenant per slot.
- 2026-11-15 — **Slice 1: decision 5's holds and bulk unpark built.**
  `held_since` is set by a head's first failure (`coalesce`), kept across
  its retries, and cleared by its mark, its park, its unpark, its resolve,
  its set-aside and a supersede; a failure that parks the row sets none.
  `held_counts(within)` and `Hold` mirror `parked_counts` and `Ground`, over
  a new partial index `__held_idx` in the baseline; one cause, `backoff`,
  until slice 3 adds the deferral. `unpark_where(channel, ground, parked)`
  is one statement. E9's executor and E9b's, which § Executors did not have,
  are red under each mutation they exist for: a hold restarted at each
  failure, a mark or a parking failure leaving one, heads counted instead of
  Tenants, and each of `unpark_where`'s three filters dropped. Two of the
  first versions of these executors passed vacuously — a slot asleep never
  retried, a channel's oldest hid one Tenant's restart — and were made to
  fail first.
- 2026-11-15 — **E8 built, and the take's plan mended twice by it.**
  `plan_tests.rs` explains the library's own statements, now functions
  (`take_sql`, `walk_sql`), at one slot and at 64, over a Tenant with
  100,000 processed rows beside a thousand others and a Tenant the last
  `ANALYZE` did not see: no read of the inbox's table but by its queue's
  indexes or a row's place, no scan of it, no read of its primary key, no
  sort of more than a step's three candidates or the slots. It found:
  - the take's one look at the whole queue, an `EXISTS`, planned as a scan
    of the table — the queue's rows spread evenly in the estimate, and lying
    past the history: 35 ms on 100,000 rows. Ordered as `__head_idx` is, with
    `LIMIT 1`, it is an index probe: 0.1 ms;
  - each slot's `EXISTS` pulled up into a semi-join that reads the whole
    queue — 2,250 rows at 64 slots, and at a launch's backlog every row of
    it on every take. A scalar probe a slot, as the take before this
    decision had, reads one;
  - the walk's last step picked by a sort, now by `max(n)`.
  Red when `__tenant_head_idx` is dropped: the walk then goes by
  `__message_id_uniq`.

  `make traces` records one test at a time: a sibling's open transaction
  stood in a recorded snapshot, and the bridge's trace fitted or not as the
  siblings ran (49 of 50 once); run singly, three recordings in three fit.
- 2026-11-15 — **M7 and M9 run, and a regression of the happy path found.**
  On this record's laptop, PostgreSQL 14.24 and 18.6 as the library's test
  clusters, a debug build of the client: times compare only with each other.
  The runs are `crates/inbox/src/pg/measure_tests.rs`, by hand, behind
  `ASCETIC_DDD_MEASURE`.
  - **M7**, 64 slots by stream, 60,000 rows processed, one Tenant with
    150,000 queued, 10,000 Tenants with ten each — 1,459 Tenants a slot, not
    the 156 the shape was meant to have, since a Tenant's ten streams fall
    into ten slots. With every head due: the take 1.2 ms and 265 buffers, a
    walk 0.9 ms, 15 buffers, one step. With nine heads in ten in backoff:
    the take 1.3 ms, a walk 1.6–1.9 ms, 84–99 buffers, 9–10 steps, 61–63
    slots of 64 reaching a due head within K = 32 (0.9³² ≈ 3 % would leave
    two). 18 alike within noise.
  - **The planning.** A walk is planned at every dispatch, the library
    issuing every statement as text: 5.0 ms to plan against 0.9–1.8 to
    execute; the take 0.8 to plan. Prepared once on the connection, a walk
    is 2.4 ms a call against 5.9 issued as text.
  - **M9**, an A/B of messages stored and processed one at a time, 500 a
    run, three runs each, on this machine: before slice 1 (`c6ab21e`)
    18.0–18.6 ms a message processed, slice 1 as built 27.5–28.3 — **9.5 ms
    more, half again**, the walk's planning most of it. Storing is unchanged
    (3.3–3.7). The two partial indexes' price is below what these runs
    resolve, their spread being ±0.6 ms a message; the 0.1 ms bound is not
    shown.
  Slice 1 is not accepted with the regression. The cure measured is a
    prepared walk, which the session crate does not offer: a decision under
    the library's gate.
- 2026-11-15 — **The happy path's regression cured: the statements cut,
  then made functions** (decision 6).
  - **Where the time went.** The server's log, every statement's parse,
    bind and execute (`log_min_duration_statement = 0`, 40 messages):
    - the walk: parse 1.15 ms, plan 2.16, execute 0.44;
    - the take: 0.29, 0.60, 0.11;
    - before slice 1: the take 0.27, 0.51, 0.23; the head's read 0.20, 0.31.

    Parse and plan of the two make 74 % of the regression. EXPLAIN's
    planning time, which the previous entry read, leaves the parse out.
  - **The statements cut first.** The walk starts from a step at the
    cursor; a step probes twice, not three times; the verdict reads the head
    by a join, not by a subquery at each of its uses; the slot's newest row
    is a probe, not `max()`. The take probes once a slot. Their outcomes were
    the previous text's on 1,400 random slot states of the walk and 300 of
    the take. The walk: plan 2.86 → 2.06 ms, execute 0.87 → 0.60. End to
    end, release, old and new code interleaved: 6.9–7.3 ms a message
    processed before slice 1, 11.9–12.0 as built, 10.8–11.0 cut. Not enough.
  - **Then functions,** the owner's second choice. A blind solver given the
    problem, not the options, chose them too; a discriminator checked both
    positions against the code, tokio-postgres's source and the manual:
    - two calls, not one: the dispatch keeps the slot it holds;
    - the walk's plan forced generic, so that the heuristic cannot plan it
      at every call;
    - EXECUTE left to PUBLIC, the check requiring the caller's rights;
    - the check strict on the two names it calls, silent on others.

    Its findings older than the decision are owed (§ Owed).
  - **Measured beside it,** on the scratch shapes:
    - calls on one connection: the walk 0.05 + 0.54 ms, the take
      0.04 + 0.28; the first call 12 and 6.5 ms;
    - the walk's generic and custom plans, M7's shape, all 64 slots, every
      head due and 95 % in backoff: the same reads of the inbox's table, the
      same steps, the generic one's buffers never more;
    - under `plan_cache_mode = auto`, a prepared walk goes generic after
      five custom plans (15 of 20). The previous entry's "prepared once,
      2.4 ms against 5.9" read no such count;
    - the setting's price: within noise, 0.92–1.04 ms a call against
      0.93–1.01 without, interleaved;
    - an overload with a default made the call ambiguous, 42725, exact
      casts or not.
  - **M9 after**, release, 500 messages, old and new code interleaved three
    times, ms a message processed:
    - on a scratch cluster with `fsync` on: 7.35, 7.52 and 9.65 before
      slice 1; 7.30, 7.39 and 11.63 with the functions. The last pair ran
      under a disk stall: its storing took 9.5 ms a message, not 1.6;
    - on the test cluster, `fsync` off: 6.99, 7.14, 7.42 before; 6.61,
      6.88, 7.24 after.

    The regression is gone: the happy path is 0.1–0.4 ms a message cheaper
    than before slice 1. A run made while a filesystem-wide search held the
    disk gave 9.2–9.7 ms on both sides: a time compares only within its own
    interleaved series. The two indexes' price, M9's bound of 0.1 ms, is
    still below what these runs resolve.
  - **Executors:** E8 under the generic plans; E8b, the walk planned once;
    E8c, the functions as made (§ Executors).
- 2026-11-16 — **The functions refuted by four roles, and their executors
  by mutation** (decision 6).
  - **Accepted, each with its executor:**
    - The signature compared as the catalogue's values, not as
      `pg_get_function_arguments` renders them (Database, Verifiability).
      Under `quote_all_identifiers` the rendering quotes names and `text`,
      and every start was refused: the mistake ADR-0037's column check
      already made. E8c passes under the setting.
    - A procedure of a function's name refused, not a panic of `migrate`
      (Database, Security, Verifiability): the result's rendering is NULL
      for a procedure. E8c.
    - The walk called with casts to its own types (Security). Untyped, the
      call resolves to a function of the name taking text, in any schema of
      the path. E8c plants one after the start, and the dispatch reaches the
      library's.
    - The dispatch proven to call the functions (Verifiability): the
      statements sent as text passed every test, the regression free to
      return. E8c: without the functions the dispatch fails, 42883.
    - The signature, the visible overload in an earlier schema and `INTO
      STRICT` each given a case (Verifiability); STRICT's: the walk of a
      slot without its row fails, P0002.
    - The refusal names what it found: the setting found and the library's,
      the other function's identity (Operability).
    - E8's generic half proven generic by `$1` in its plan; E8b counts the
      take too (Verifiability, Database).
    - The operator's runbook in the README: the walk's plan seen inside
      `BEGIN … ROLLBACK`, and a call by hand, which commits a move of the
      cursor (Operability).
    - The doc comments that still said "planned anew at every call"
      (Database).
  - **Measured, and closed:** a plan cached on a small table, kept as it
    grows (Database M1, Operability F4). Plans made at 500 rows, the table
    grown to 310,500 without ANALYZE: the cached plans still go by the
    indexes. A take and a walk, median of 20, 2.06 ms on the stale
    connection, 1.18 on a fresh one, 1.76 after ANALYZE. Staleness costs
    until autovacuum, it does not scan.
  - **Owed** (§ Owed): the owner not compared, and the precondition that
    `migrate` runs as the dispatching role, both needing a second test
    role; a repair without the queue's loss; the live day's rule's
    executor; CREATE on the schemas in the search path, the host's to
    close. On the host the class is live today: each database grants
    CONNECT, and PostgreSQL 14's `public` grants CREATE, to PUBLIC
    (`tools/pg_role.sh`, the Security role's reading).
  - **A redundant guard, recorded as such:** `#variable_conflict
    use_column` (Verifiability). No OUT name is a column the statements
    read, and the signature pins the names; it keeps the body immune to the
    server's `plpgsql.variable_conflict`. No executor of its own.
  - **Kept as decided:** the count of other functions refuses one in the
    schema or visible, more than a call could resolve to (Security). A
    harmless `public.<table>__walk()` refuses every start; a narrower rule
    would compare resolution, the server's to do.
  - **Mutations,** each restored after; red, the tests named:
    - E4, the walk's bound `w.n <= $2`: three tests;
    - E1, the wrap arm deleted: thirty-four;
    - E1, `<= c` made `< c` in both arms: thirty-one;
    - E1, the cursor never read: four, E1 and E4's;
    - E4, `ends` forced false: six;
    - E8c, the walk called without casts; INTO without STRICT; the
      statements sent as text; the visible overload not counted; the
      signature not compared: E8c each;
    - E8, the generic half run under `auto`: E8;
    - E8b, the walk's SET dropped from the baseline and the check: 31 plans
      against 22.

    The E1–E4 mutations were re-run because they had been measured on the
    walk's text before it was cut.
  - **The drains bounded.** Under `ends` forced false one test did not go
    red: it hung 4 h 39 min, its test binary at 4.8 GB, until stopped by
    hand. Nineteen loops in `tests/pg.rs` dispatched until `Nothing`,
    unbounded, and the trace recorder of a test that writes none
    (`TraceFile::off()`) still keeps every line in memory. Each is now
    `drain`, which fails after fifty dispatches and three a row the table
    holds; M9's loop is bounded alike. The mutation re-run: six tests red,
    the hung one among them, in 87 s. A test-only change: no solver round.
- 2026-11-16 — **Renumbered 0041, and rebased onto ADR-0040's branch.**
  - **The number.** The main line took 0039 for the data keys' ADR while
    this one waited on its branch. The host's QAS-SCALE-0001 cites this one
    by its new number.
  - **Why onto ADR-0040's branch.** That branch holds the verdicts a
    subscriber returns (`bus::Cause`, `Unavailable` and its scope), the
    hold for a stage's outage, `unpark_all`, and the data keys' slice 4,
    which already returns `Unavailable`. A fresh discriminator compared it
    with this ADR's slice 2 positions, which two rounds of roles had
    refuted. It found ADR-0040's vocabulary, `unpark_all` and the
    evidence of a hold better. It also found that under this ADR's rotation
    ADR-0040's hold holds a (slot, Tenant), not a slot, and that its tests,
    one Tenant to a slot, cannot see it. Hence one owner for the failure's
    scope: slice 2 and decision 4 move to ADR-0040's revision.
  - **What the rebase changed in either.**
    - `park_held` read one head a slot, the slot's oldest. Under rotation a
      slot has a head for each Tenant, so it parked the oldest held head
      and left the others. It now reads every Tenant's head, one probe of
      `__tenant_head_idx` each, as a walk goes. Its executor holds four
      Tenants of one slot. The old statement put back fails it, and only
      it: ADR-0040's own tests, one Tenant to a slot, pass under it.
    - `unpark_where` is withdrawn for `unpark_all`. The latter reads its
      window off the database's clock and goes in batches; it re-arms with
      a canary rather than at zero attempts; it leaves an overtaken row
      parked and tells the principal. What `unpark_where` alone did, a
      window of absolute times, a cause recorded on each row makes needless.
    - `park_held` clears `held_since`, as every other park does.
    - The twin of decision 6 gains ADR-0040's three columns in the order
      its migration adds them, and ADR-0040's release test now renders the
      twin without them rather than a list of names.
    - Three of ADR-0040's tests assumed a slot served in arrival order
      across Tenants, or a slot awake after a round that found no head due.
      Each now says the verdict by the message, or wakes the slot it moved
      the clock of. No behaviour of ADR-0040's changed for it.
    - One `make traces`: this ADR's, which runs the tests one at a time and
      replaces the covered traces. ADR-0040's `check.sh --traces` and its
      `RECORDED_ONCE` are dropped; the one run that file named,
      `bridge-outbox-set-aside`, was already withdrawn here, no test
      recording it. `trace2tla.py` maps a walk that passed to no step.
      `InboxOutage.tla`'s checks are the group `inbox-outage`.
  - **The gate.** No solver round of its own: the rebase decides nothing
    the comparison had not. Three roles refuted the merged tree before the
    commit: distributed systems with the database, operability with
    evolution, verifiability. What they found against the merge is mended
    with an executor each, or owed, in ADR-0040's Change Log of this day and
    its § Owed 6–10. Here:
    - `trace2tla.py` maps a walk that passed (the branch first written for
      it was unreachable, the fetch before it having set the slot), and
      refuses a run of no step; `inbox-identity_past` is such a run.
    - `InboxRotationPermanent.cfg` runs a permanent verdict beside the
      rotation: `FailPermanent` resetting the cursor breaks
      `BoundedOvertaking`, dropped from fairness breaks liveness, and
      `Commit` without its guard breaks `ArrivalOrder`.
    - § Owed gains the rotation in the traces and a positive bridge trace
      with a row set aside.
    - Decision 5's `held_since` is the clock of a hold by the head's own
      failure: a hold for the stage's outage ends it, so `held_counts`
      never counts an outage as a back-off.
- 2026-11-16 — Decision 3's environment's verdict superseded by ADR-0042: a
  stage's outage pauses its channel in the process that sees it, rather
  than rolling back and pausing every loop of the process.
