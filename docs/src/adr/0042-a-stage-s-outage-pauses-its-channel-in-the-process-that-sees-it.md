# ADR-0042: A stage's outage pauses its channel in the process that sees it, and nothing is parked for it

## Status

Proposed (2026-11-16). Built (2026-11-17): slice 1, the breaker's and the memo's pure core
(`crates/inbox/src/breaker.rs`), C1–C13 its tests; slice 2, the DEK stage's causes and its sentinel read (§ 9,
D1); slice 3, the breaker in the dispatch — the rows, the exits and their CHECKs, the walk's paused channels and
the guest, the trial, the isolation per transaction (R1, X1, G1, P1, C5, C8, C10, I1); slice 4, the expiry off
the dispatch path (E1); slice 5, the reads, the stalls' report and the operator's verbs (V1, § 6, C14), and the
names; slice 6, the model; slice 7, the traces and B1's gates (T1, B1). Every slice is built; § Owed keeps the
measurements, the host's adoption and a live deployment's steps.

It supersedes ADR-0040's decisions 4, 5, 7, 9 and its `park_held` (decision 8), and ADR-0041's decision 3 (the
environment's verdict, its slice 2). It keeps ADR-0040's verdicts and causes (decisions 1–3, 6) and
`unpark_all` (decision 8, widened here), and ADR-0041's rotation (decisions 1, 2, 5, 6).

Decided under the library's gate:
- a blind solver, given the problem and the code and not ADR-0040's or ADR-0041's decisions;
- a main position, written before the solver's was read;
- a discriminator, who refuted the main position's shared record and corrected the solver's in seven places;
- the synthesis refuted by three rounds of roles: distributed systems with the database, performance,
  operability with evolution and design, and verifiability (four roles, then three, then three), each round on
  the revision the last one forced.

After the third round the breaker's rules had been revised three times in one place, each round finding an edge
of the rule the last had added. The decision stops refining them by reading: § 1 states the breaker as the
standard circuit breaker, and § Executors makes every scenario of the three rounds a property test of its pure
core, so that a contradiction between two rules fails a test before any database code is written.

## Context

**What the stage's outage costs today, under the rotation** (ADR-0040 § Owed 6, verified):
- ADR-0040 keeps a hold for a stage's outage on each row, and ADR-0041 gives each Tenant a head in each slot. So
  each (slot, Tenant) head has its own clock and is asked on it: about 427 asks a second at 128,000 heads and a
  300 s re-ask cap (`crates/inbox/src/pg/store.rs:998-1007`).
- With every head held, a slot walks its Tenants K at a time at least once a `max_dormancy`: 64 × ⌈2,000/32⌉ =
  4,032 walk transactions a second (`store.rs:84-87`, `177-192`).
- Past the budget any row of the channel processed within the cap refutes every head asked in the next cap, and
  each spends and parks: up to one head per (slot, Tenant) (`store.rs:979-991`, `1012-1030`).
- One error line per held head per ask past the budget (`crates/inbox/src/pg/dispatch.rs:308-311`).

**What no position may rely on** (each found by the round and checked in the code):
- **The evidence of a stage's reach lives in one process.** A key the opening end has cached serves a row with no
  call to the KMS (`crates/dek/src/opens.rs:212`), so rows of Tenants with cached keys are processed while the
  KMS is down. "A row of the channel was processed" says nothing of the KMS, in either direction.
- **`Stage` is the stage's default reading; `Tenant` is positive evidence.** `scope_by` reads `Tenant` only when
  another Tenant's key landed after this one's trouble began, and `Stage` otherwise (`crates/dek/src/pace.rs:
  110-115`). With caches warm, nearly every failure at the opening end reads `Stage`.
- **A consumer may decrypt on the channels it receives and draw keys only on those it sends.** The host's
  generated policy grants `datakey` on sent channels and `decrypt` on received ones; every consumer may read a
  key (`transit-bus/keys/+`). A check that draws a sentinel's key is refused on six of the policy service's seven
  inbound channels.
- **One replica's local cause** — cut off, its token, its stage's own `FlightsFull` read as `Stage`
  (`crates/dek/src/supply.rs:333-334`) — must stop that replica alone.

## Decision

**Principle:** a cause is recorded once, at the extent its evidence proves, in the place that holds the
evidence, and ends by evidence of the same kind. The inbox does not ask what a stalled cause reaches.

| Evidence | Extent | Recorded in | Projected as | Ends by |
|---|---|---|---|---|
| `Permanent`, `Transient` | the head | the row: attempts, back-off | back-off; parks at `max_attempts` | time; a person |
| `Unavailable`, `Tenant` scope | (channel, Tenant), this process | the process's memo | holds on the Tenant's heads this process meets; one ask in flight | time, then one ask |
| `Unavailable`, `Stage` scope, broad | (channel), this process | the process's breaker | the walk treats the channel's heads as not due | a trial's verdict |

Two corollaries:
- **`Unavailable` never spends an attempt and never parks**, at any scope. A row parks for its own fault or by a
  person's hand.
- **No state of a stage's extent in the database gates anything.** The database holds the rows' own facts and a
  report for people.

### 1. The breaker: per process, per channel, a standard circuit breaker

A pure core, the time an argument, returning effects; one per `PgInbox` behind a mutex; told after each
subscriber verdict and driven by every `dispatch`, a host's own as well as `run`'s.

**Closed → open.** Over a window W = 30 s, F is the number of distinct Tenants whose latest verdict on the
channel was a `Stage`-scoped `Unavailable`, and S the number whose latest was processed (a bounded set of 1,024,
read as "at least 1,024" beyond). The channel opens (is **paused**) when

    F ≥ 2  and  (F ≥ 0.75 · (F + S)  or  (F ≥ 32  and  F ≥ 0.25 · (F + S)))

- A `Tenant`-scoped `Unavailable` and the row's own verdicts count in neither F nor S.
- One Tenant never opens it: its memo bounds it (§ 2), and a deployment of one Tenant is paged by the oldest
  hold (§ 6).
- The second arm opens a broad outage while Tenants with cached keys still flow. It needs its own share: a fast
  failure reads `Stage` (no other key has landed since its trouble began, an instant before), so a brownout of
  5 % at 100 asks a second gives about 150 distinct `Stage` Tenants in W, against S near its bound.
- The pause's cause is the modal cause of the `Stage` verdicts within W, fixed for the pause.

**Open.** A tick every 1 s doubling to 15 s, never sooner than a `retry_after` told (itself capped at 15 s). The
number of failed ticks f is kept across a close shorter than W and reset after W closed. The channel is
**stalled** at f ≥ 2.

**Half-open: the trial.** At a tick, one ask of a **witness**:
1. a head of the channel whose Tenant has no memo, met by a guest walk (§ 5); meeting one issues a trial at once,
   at most one a second;
2. else a memoised Tenant's head whose memo has run out: the trial honours the Tenant's back-off;
3. else none at this tick.

The trial locks the witness's slot as `take` does, locates the Tenant's head in a statement after the lock with
`FOR UPDATE`, and runs the subscriber under `trial_wait` (the stage's own wait), past which its future is dropped
and the transaction rolled back. It never writes the slot's cursor; a processed trial clears the slot's round.
Its verdict:
- processed → closed;
- a `Stage`-scoped `Unavailable` of the pause's cause → open, f + 1;
- a `Tenant`-scoped `Unavailable` → that Tenant's memo, and the witness drops out;
- the row's own verdict, a head gone, or the timeout → inconclusive; **three inconclusive verdicts in a row, on
  distinct witnesses, close the channel**;
- **no witness**: a guest's full circle that meets no head of the channel closes it.

Every effect carries an id and a deadline: one unanswered by its deadline is a failed tick; an answer after it
changes nothing but a close. At most 64 witnesses a channel, oldest out.

### 2. A Tenant's memo

Per process, per channel, the (channel, Tenant)s whose latest verdict was `Unavailable` of either scope, and only
those: a healthy Tenant never enters it.
- An entry holds the Tenant's slot, since, the count of its `Unavailable` verdicts, `until` and an in-flight mark.
- The exponent is the greater of that count and the row's `holds` (the `Unavailable` verdicts the subscriber
  told on that row). A Tenant past the memo's capacity (1,024 a channel) is so still paced, per head. Capacity
  pauses nothing.
- `until = now + min(cap, max(told, floor · 2^exponent))`, cap 300 s.
- One ask in flight per (channel, Tenant) per process, after the Tenant's first `Unavailable`. The mark is a guard
  released on every exit of the dispatch, with a deadline.
- A head of a memoised Tenant met before `until`, or while an ask is in flight, is held to the later of `until`
  and the in-flight deadline, always ahead of now, and the subscriber is not called. A Tenant without an entry is
  never held.

### 3. The rows

- Columns: `cause` (replacing `last_cause` and `parked_cause`), `held_since`, `holds integer NULL` (`NULL` is
  none: nullable without a default, as every column the library adds, so that it binds no insert of a release
  that does not know it — ADR-0037 decision 5), `hold_kind` (`backoff` or `unavailable`), `next_attempt_at`.
  `unavailable_since` is dropped.
- `Permanent` and `Transient` as today; a `Transient` sets `holds = NULL`, `hold_kind = backoff`.
- An `Unavailable` told by the subscriber: `cause`, `last_error = NULL`, `held_since = COALESCE(held_since,
  now)`, `holds + 1`, `hold_kind = unavailable`, `next_attempt_at = clock_timestamp() + interval`. The interval is
  the memo's `until` for a `Tenant` scope, and **at most the tick cap** for a `Stage` scope, the longer clock kept
  in memory only: a replica cut off alone delays a healthy replica's head by at most 15 s.
- A held head (§ 2) writes `cause`, `held_since`, `hold_kind` and `next_attempt_at` as above, and not `holds`.
- Every interval is bound against the database's clock, never a process's.

### 4. The exits

One fragment, `held_since = NULL, holds = NULL, hold_kind = NULL, next_attempt_at = NULL`, in every statement that
takes a row out of the queue, conditional where the statement may keep it queued. `cause` is cleared by every
exit but a park, which writes its own. The baseline carries the invariant:

```
CHECK ((held_since IS NULL AND next_attempt_at IS NULL AND holds IS NULL AND hold_kind IS NULL)
       OR (processed_position IS NULL AND parked_at IS NULL AND waiting_for IS NULL))
CHECK (cause IS NULL OR processed_position IS NULL)
```

A violation (23514) is a defect that stops the loops: the fault of the library's statement, not the message's.

### 5. The walk and the guest

- `<table>__walk` gains the paused channels; a head is due when its own back-off has passed and its channel is
  not paused. With none paused, today's statement.
- A process with a paused channel walks as a **guest**, from the slot's own `guest_tenant` (a column not indexed,
  so the write stays HOT), shared by the guests of every replica. It serves a due head it finds, clearing an
  unpaused round as `take` does. Finding none, it writes `served_at` and `guest_tenant` only, never the cursor,
  the round or the dormancy, returns `Outcome::Paused` with the heads of paused channels it met, and the loop
  sleeps its poll.
- A head of another channel waits for a guest at most N / (R · L · K / (poll + w)) per message, w a walk's own
  time, some 18 ms (M3): 36 s at the host's 200 ms poll. Under a KMS outage every channel of a host's inbox is
  paused, so nothing waits on a guest.
- A trial is the dispatch's own statements, not a function of the baseline: the slot locked `SKIP LOCKED`, then
  the witness's head by its Tenant and channel `FOR UPDATE`, never the slot's head. The take and the walk are
  functions, planned once a connection (ADR-0027), because every dispatch runs them; a trial runs once a tick a
  paused channel in a process, and a function for it would add a baseline object, its signature check and its
  teardown for no cost measured.

### 6. The report and the page

- `<table>_stalls (channel, cause, reporter, since, valid_until, openings)`, one row a (channel, cause, process).
  A process writes it when its channel stalls and refreshes it every tick, `valid_until = clock_timestamp() + ttl`
  with ttl at least twice the tick cap plus `trial_wait`. The reporter is a random value drawn at the process's
  start: a label of the report, not an owner (ADR-0008 decision 3). As built: the breaker's stall effects are
  written by the next dispatch, on a session of its own, its statements bounded by a second (statement and lock
  timeout); a failure is logged and never fails the dispatch — the report is for people. A row lapsed is a new
  stall, from the pause's start; one still valid keeps its start and counts an opening when its pause is a new
  one, the pause marked when a loop takes it, so that two loops count it once; the rows are written in one
  order, so that two loops wait rather than cycle. A channel opened again within the window after a stall is
  still stalled and reports so at its opening, not at its first tick, which its failed ticks put up to the cap
  away; a resumed channel's row is refreshed no more and lapses. So a stage back for less than the ttl less the
  asks that open it again keeps one stall with two openings — the flap rule (b) reads. Other processes' rows
  lapsed for an hour are removed by a statement of their own, `SKIP LOCKED`. No statement of the inbox reads the
  table to decide.
- **The host pages** a channel on (a) a valid stall row unless its processed counter rose over the window, or
  (b) a valid stall row of every up replica continuously for N minutes (a flapping stage), grouped by cause and
  naming the channels, with `keep_firing_for`, and the sampler's companion rules as the parked rows have.
- **The oldest `Unavailable` hold** over a threshold pages unless the channel is stalled on every replica, with a
  `for` longer than the hold cap: the net under a fault mislabelled `Unavailable` and under one Tenant's long
  trouble.
- `stalls(within)`; `held_counts(within)` by (channel, kind, cause); `waiting_counts(within)` by channel, by
  `__waiting_since_idx`; `parked_counts(within)` by (channel, ground, cause); each in a transaction of its own
  bounded by `within`.
- `__held_idx` carries every column `held_counts` reads, the row's `uri` its channel is computed from among
  them, so that the count can be answered from the index alone, on 14 as on 18. The planner takes that plan
  when the heap's pages are visible enough: on a vacuumed table of 100,000 rows, 4,000 held, 18 read 70 pages
  against 3,386 by their heap, where 14 priced the heap lower; under open transactions elsewhere, 18 read the
  heap too. Either way no row but the held is read.

### 7. The expiry leaves the dispatch path

- A timer of `run`: a pass at the start, its statements one after another while each examined a whole batch,
  then a rest of `max_wait / 10`; an error of the moment waits as a loop's does, a defect stops the loops, as
  when the expiry ran in the dispatch (ADR-0010). `PgInbox::expire()`, which takes no session, is one statement
  of it for a host that drives `dispatch` itself, and returns `Expiry { parked, more }`: call again at once while
  `more`. The process keeps the pass's key.
- One statement, `<table>__expire(max_wait, batch, after_since, after_position)`, a function of the baseline
  planned once a connection, its plan forced generic, as the walk is: a single `WITH RECURSIVE` over the
  dependency chain, an inner unlocked `LIMIT` over the candidates, and `FOR UPDATE … SKIP LOCKED` on the expiring
  rows only, their wait tested again on the row locked; no cast a malformed `waiting_for` could abort — the
  position is read as an integer only when it is an integral JSON number within the column's range, so that a
  hop is one probe of the primary key and not a walk of its stream's history; a keyset on `(waiting_since,
  received_position)` by `__waiting_since_idx` on those columns `WHERE waiting_since IS NOT NULL`; it returns
  the last key examined and how many, and `batch` (100) bounds the rows examined.
- A row with a `waiting_since` is a waiter in wait: `CHECK (waiting_since IS NULL OR (waiting_for IS NOT NULL AND
  parked_at IS NULL AND processed_position IS NULL))`, so that the index holds the waiters alone. A row the
  expiry parked keeps its `waiting_for`, without the time, and is not examined again.
- A waiter is **spared** while its dependency is unparked and either queued or itself waiting on a spared one,
  within 8 hops; a ninth hop or a cycle expires it. A spared row is neither written nor locked, and a statement
  over spared rows takes no transaction id. `max_wait` so means "the dependency never arrived", which a held
  head, a row behind one, an untried head and a long backlog are not. A row expired names where its chain broke,
  after the dependency in `last_error`: it `never arrived` or `is parked`; the waiter `waits on` another
  identity through waiting rows, `which never arrived` or `which is parked`; or it `reaches no queued row within 8
  hops`, a chain too long or a cycle.
- A waiter whose dependency is missing or parked expires within `max_wait + max_wait/10` and the time of
  ⌈W/batch⌉ statements, W the waiters past `max_wait`, spared or not, itself among them: a waiter that becomes a
  candidate during a pass lies past its key and is reached by that pass, one that becomes it during a rest by the
  next. On the reference machine (12 cores, NVMe, PostgreSQL 14): 20,000 spared at one hop, 201 statements of
  1.1 ms each (E1); 220,001 waiters, 200,000 of them at one hop and 20,000 on chains of 8, 2,201 statements in
  2.3 s, those over the chains 3.4 ms each — a pass of a process.
- Under a held vacuum horizon a pass also reads the heap of every waiter woken since the hold began: the wake is
  not HOT, and its old version's entry in the index cannot be removed while the horizon holds. 20,000 woken
  cost a statement 798 buffers against 6 (counted on E1's table), each statement of each pass, until a vacuum
  after the hold. The walk pays the same for every message processed since. A `statement_timeout` a host sets is
  then a defect of the expiry, which stops the loops, as it is of a dispatch.

### 8. The operator's verbs

Each takes a principal and is audited (`Operated`); each batch is one autocommitted statement with `SKIP LOCKED`,
reporting what it did, what it passed by and whether more remain (`GivenUp { parked, passed, more }`,
`UnparkedAll::more`); each wakes the slots it touched that no dispatcher holds, `SKIP LOCKED` in the same
statement, so that a slot asleep goes on at once and a held one within its holder's next sleep. The single
`unpark` and `resolve` write no slot row: `unpark` runs in the caller's session, maybe its transaction, where a
slot's lock held to its commit would keep the slot from every take, and `resolve` holds a mark's advisory lock,
after which ADR-0009 acquires nothing but woken rows; their rows are taken within `max_dormancy`, as a mark's
woken rows are (ADR-0041 decision 3). The audit is told after each batch, with the Tenant and the cause the
verb names.
- `held(principal, after, limit)`: a page of the held heads with their hold, cause, count of holds and identity,
  as `parked` pages the parked rows.
- `park_heads(principal, &ParkingHeads { channel, cause, tenant, limit })`: the heads keep their cause and
  attempts and park on the failure ground, the oldest hold first, in `__held_idx`'s order.
- `park_queue(principal, &ParkingQueue { channel, tenant, cause, limit })`: a (channel, Tenant)'s queue rows,
  given up whole, under the operator's cause, the newest first: a dispatcher that serves the Tenant between two
  batches takes its oldest row, older than every row parked, so what is parked is the end of each stream and the
  re-arm, oldest first, overtakes nothing. The Tenant's rows are probed slot by slot by `__tenant_head_idx`, as
  the walk probes: a probe of the Tenant by its key read its processed history.
- A verb's batch reads a batch of candidates unlocked, then locks those still named `SKIP LOCKED` and parks them:
  a held one is examined again by the next batch, and the verb ends when a batch examined fewer than it asked
  (the rows not parked are the held, `passed`), parked none, or reached its limit (`more`).
- `unpark_all(principal, &Unparking { channel, ground: ParkedFor::Failure(cause) | Expired | AtOnce, tenant,
  within, tries, limit, overtaken })`: `attempts = max(0, max_attempts − tries)`, `cause = NULL`, and the wait
  and the unreadable mark cleared, each row re-armed by its ground's remedy.
- ADR-0009 decision 4 is amended: a verb may lock a queue head's row for one statement; a walk that waits on it
  reads it as vanished.
- `park_held` is withdrawn: under the rotation it freed nothing.

### 9. The DEK stage

- The opening end tells a Tenant's key gone from a mount wiped by **reading whether its sentinel's key exists**,
  an operation every consumer holds, rather than drawing a key it may not. A shredded Tenant stays `Permanent
  kms.gone`; a wiped mount reads `Unavailable`.
- `Forbidden` with the key present stays `Permanent`: no retry mends it, and the host pages on it.
- A cause per remedy (`dek::causes::of`): `kms.forbidden`, `kms.key_version`, `kms.ciphertext`, `kms.malformed`,
  `kms.unsupported`, `kms.conflict` (a key made otherwise than asked: custody's), `kms.gone`, `kms.unavailable`,
  `kms.unauthorized` (a 403 the key's read does not explain), and `kms.unexplained` (the rest). A Vault error's
  text names its status and method, never a path: a key's name is its tenant's.
- The KMS port gains `key_exists`, a read: Vault reads the key, PostgreSQL asks whether a row of the tenant is
  there. A wrapped key the KMS refused keeps its cause by the wrapped key, so the opening end's memory of it
  tells the same cause.
- `last_error` keeps a text only for `Cause::UNEXPLAINED`. No Tenant identifier in a log line of the dek.

### 10. Isolation, per transaction

`<table>__take` returns the transaction's isolation; a trial and a `resolve` read it as their first statement;
anything but READ COMMITTED is refused for that transaction (`Error::Refused`), since a session's level may
change between two. `unpark` is one statement that waits for no lock it then reads past, and needs no check; so
is a statement of the expiry, which locks `SKIP LOCKED`: under REPEATABLE READ a waiter woken between its read
and its lock fails it with 40001, an error of the moment, where READ COMMITTED passes the row by.

### 11. Names

A channel a process does not ask is **paused**; the reported state is a **stall**; a **trial** asks a paused
channel's **witness**; a **guest** is the walk of a process with paused channels; the **memo** keeps a Tenant's
`Unavailable`. `Loops::max_pause` and `pause_after` become `max_error_wait` and `error_wait_after`, the
inbox's and the outbox's both, since a host reads the two together.

### 12. What the host's adoption needs, in order

1. The shared kernel carries the bus's verdict whole: a verdict of three classes in place of
   `CommandFailure::is_transient -> bool`, and a `Refusal` carrying the cause, the scope and `retry_after`. Today
   every stage error that is not `Permanent` becomes a transient one, and a `Permanent` one is re-wrapped
   unexplained. A public change of the host's kernel, under its own regime.
2. The sealing built with a sentinel, now safe on the channels a service only receives (§ 9).
3. The observer's events (a hold, a pause, a stall, a resume), a gauge family for them, samplers of
   `held_counts`, `waiting_counts` and the stalls, and `cause` on the parked gauge as a closed set.
4. The alert rules of § 6, each with test series: every replica stalled; one cut off; every replica restarted
   mid-stall; a stage up 20 s and down 40 s; a quiet channel with one Tenant held; the sampler stale; one Tenant
   held 30 minutes.
5. An operator's command per service for the verbs of § 8; the renamed fields of both `Loops` (§ 11).
6. A KMS quota sized for the drain's rate of unwraps.

## Consequences

At 2,000 Tenants in each of 64 slots, 3 replicas of 8 loops, K = 32:
- **A full outage**: nothing parks and no attempt is spent; before the pause, at most max(32, ⌈S/3⌉) distinct
  Tenants asked and failed a channel a process (S, the Tenants processed within W, at most 1,024: so 342),
  each once before its memo holds it, and L in flight; then one trial a tick a channel a process (at most 240 an hour); guests R · L / poll; the expiry's
  re-reads grow with the waiters, not the Tenants. Detection after recovery takes at most the tick cap and one
  trial. **No term in T.** Today: 427 asks and 4,032 walks a second.
- **A misread** pauses the process that made it; a healthy replica's head waits at most the tick cap for its
  holds; at one slot a trial holds the slot at most `trial_wait` a tick.
- **A brownout** of 5 % reaches neither threshold; a flapping stage pages by § 6 (b).
- **One Tenant's trouble**: one ask in flight a process, `log2(cap/floor) + t/cap` asks; a quiet channel with one
  failing Tenant is never paused.
- **A partial outage read as Tenants'** (one shard of the KMS): no pause; each failing Tenant paced on its own.
- A host sees: `Unavailable` never spends nor parks (a `Tenant` scope spends and parks today); a mislabelled fault
  holds until a person acts, paged by the oldest hold; `max_wait` no longer expires a dependent whose chain is
  present; the expiry runs on a timer; a missed exit stops the inbox; the causes of the KMS split by remedy;
  `last_error` is empty for named causes; `Outage { budget, cap }` and `past_budget` are gone; a session not READ
  COMMITTED is refused.
- Tenants whose keys are cached stall while their channel is paused, and on a channel not paused once one of their
  heads is refused: the memo holds a (channel, Tenant) on a verdict of either scope (decision 2), its cached heads
  with it. They would have flowed at most `keep` (5 min by default) longer; M6 measured half of a launch's backlog
  so held for the outage's length (§ Measured).

## Rule of Three

1. **Another channel's head waits for a guest**, 36 s per message at the host's poll, when one channel alone is
   paused. Checked: every channel of a host's inbox is sealed by one KMS, so a KMS outage pauses all of them; the
   case waits for a channel-specific dependency, and M3 measured it.
2. **A fault of the message's own, labelled `Unavailable`, never parks.** Checked: the dek keeps every fault of
   the message `Permanent` (§ 9, executor D1); the oldest-hold page and `park_queue` give it up.
3. **A missed exit stops every loop with 23514.** Kept: the CHECK makes the omission loud and the executors find
   it before a release (X1–X2).
4. **The breaker's thresholds are judgment**: 2 Tenants and three quarters, or 32 and a quarter, over 30 s.
   Checked against the scenarios of the three rounds by the core's tests, each with a mutation that turns it red
   (C1–C13); M5 decides them at scale. On a window nearly empty — a process just started, a quiet channel — two
   Tenants failing at `Stage` scope pause it: the cost is a tick, since a trial then asks a head a guest meets.

## Alternatives Considered

- **A hold kept on each row** (ADR-0040 as built). Refuted: asks, walks, logs and parks all scale with the heads.
- **A shared gate in the database, opened when no row of the channel was processed for a while, and closed by a
  processed row** (the main position). Refuted: a cached key processes during an outage, a quiet channel lets one
  replica's trouble open it for everyone, and with warm caches one failure would open its local hold.
- **A probe port on the stage, checking a sentinel's key** (the solver's, kept through revision 2). Withdrawn: a
  consumer may not draw a key on the channels it receives, the inbox has no path to a stage inside a typed
  consumer, and a stage is a transform of messages, not a health check. The trial asks a real row, which is the
  operation the rows need.
- **Breadth read from the memo, and a full memo read as breadth.** Refuted: the memo keeps failing Tenants and
  sheds healthy ones, so the ratio is biased, and the two capacity rules contradict each other.
- **Renewing a spared waiter by a write.** Refuted: a non-HOT update and a dead index entry in the expiry's range
  per waiter per `max_wait`.
- **Rolling back and pausing the loops on an environment's verdict** (ADR-0041 decision 3). Superseded: it stops
  every channel and Tenant of the process for one channel's dependency.

## Executors

Every scenario of the three rounds is a test, each with the mutation that turns it red. The core's run over random
histories with a manual clock; the shell's with a `Clock` and a paused runtime; the database's under `test-pg`.

| Id | Scenario | Assertion | Mutation |
|---|---|---|---|
| C1 | a full outage on a warm window, 2,000 Tenants, a quarter and a half of them on cached keys | pause once max(32, ⌈S/3⌉) distinct Tenants failed (S ≤ 1,024), and no more; then one trial a tick | no broad arm |
| C2 | a brownout of 1 % and 5 % fast failures read `Stage`, 100 asks a second; and the dek stage on the scripted KMS at 1 %, 5 % and 2 % hanging | no pause | the broad arm without its share |
| C3 | one Tenant throttled, `retry_after` 60 s, a quiet channel; another Tenant arrives at 5 min | no pause; the first asked ≤ ⌈t/60 s⌉ + c times; the second within the poll | open on one Tenant |
| C4 | 70 % of Tenants failing at `Tenant` scope, four times the memo's capacity | no pause; no healthy head delayed; asks ≤ F · S · (log2(cap/floor) + t/cap + c) | pause at capacity; the exponent from the memo only |
| C5 | one Tenant hanging 10 s, 64 heads, 2,000 healthy Tenants | ≤ 1 concurrent ask; ≤ 63 row versions during one ask | no in-flight mark; hold to a past `until` |
| C6 | recovery, then every row `Transient` | closed within 3 trials | close only on processed |
| C7 | paused, every head parked by a person | closed after one guest circle; the stall lapses within its ttl | keep refreshing |
| C8 | one replica cut off, caches warm | a healthy replica's head delayed ≤ the tick cap; at one slot its throughput ≥ (tick − `trial_wait`)/tick | hold to `until` on the row; no trial timeout |
| C9 | a trial answered after its deadline | f unchanged | count a stale answer |
| C10 | one head of the channel among T ≫ K | processed within 2 ticks of recovery, the slot's oldest rows another channel's | the trial takes the slot's head rather than its witness's |
| C11 | mixed causes during a pause | one row a (channel, reporter); a stable cause | a cause per verdict |
| C12 | effects unanswered | `tick(now + d)` re-issues within d | in flight without a deadline |
| C13 | more memo keys than capacity | an entry in flight or not run out is never evicted | plain LRU |
| G1 | at S = 1, K = 4, a memo of 16, at 8 and 64 Tenants, driven until every slot is paused, then n more dispatches | calls ≤ trials; changed rows ≤ calls; no head held unasked; a guest's step ≤ K; only the pause and the stall logged, as many lines at 64 as at 8; onset calls = `fewest` | drop the walk's filter (a hold on each paused head met, the same observable); no K; a held row logged at `warn` |
| E1 | the expiry (`tests/expiry.rs`, the expiry's plan in `pg/plan_tests.rs`): a present chain, an untried head, a row behind a held head, a missing and a parked dependency, 8 and 9 hops, a cycle under `statement_timeout`, 20,000 spared with one missing last, ties at one `waiting_since` with rows moved in the heap, a malformed `waiting_for`, parked-expired rows; the spared rows' `(ctid, xmin, xmax)` and no xid in one transaction; `run`'s timer, and no dispatch expiring; a defect stopping `run`; the CHECK of a time of wait; the function as the start check requires it, planned once a connection | spared or expired as stated, each with its reason; 201 statements for 20,000 spared; ≤ `batch` rows examined by `__waiting_since_idx`, a hop one probe of the key | drop `present`; drop `parked_at IS NULL`; bounds 7, 16 and none; the cursor on expired rows; `LIMIT` after the filter; a `ctid` tiebreak; the key `waiting_since` alone; a plain cast; renew by write; lock before the spare test; the dispatch expiring; `run` without the timer; a defect logged; `more` by the rows parked; the observer untold; the start check without the set or the function; the CHECK dropped; the index's old predicate or no tiebreak; the exit's fragment; the plan not forced generic |
| R1 | the rows: 20 asks, no spend; `holds` at its cap; a `Transient` resets, its back-off kept; every wait bound to `clock_timestamp()`, read from the source (a process's clock an hour fast agrees with the server's on one host, so no run tells the two apart); a hold after a 1.5 s subscriber; the kinds of a held head asked and of one held unasked; the counts by `__held_idx`, and answerable from it alone, the heap's scans set off | as stated | spend; no cap; the holds kept by a spend; the back-off dropped; `CURRENT_TIMESTAMP`; the kind from `holds`; `uri` out of the index |
| X1 | the exits: random operation histories on a live database; every `UPDATE` that sets `processed_position`, `parked_at` or `waiting_for` carries the fragment; the spend that keeps a row queued keeps its back-off; `unpark_all` clears `cause` | no 23514 | drop the fragment; an unconditional fragment; keep `cause` |
| V1 | the verbs (the last tests of `tests/outage.rs`, the verbs' plans in `pg/plan_tests.rs`): the held page with its hold and identity; heads given up by cause and Tenant, the rows behind going on at once, their slot woken from an hour's sleep, and re-armed by their cause; a locked head passed by and the verb waiting for nothing, its rows and its slots; a walk waiting on a verb's lock reads its head vanished; a queue given up whole, the newest first across slots, re-armed in order; a queue given up while it is served ends a prefix processed and a suffix parked; the canary with `tries` above `max_attempts`; a bulk unpark by each ground's remedy; the waiting counted; a stall reported, refreshed with its start kept, lapsing after its resume, an opening again counted; a report that cannot be written failing no dispatch; the audit with its Tenant and cause; the batches' plans, a batch of `__held_idx` and the Tenant's queue alone by `__tenant_head_idx`; C14 in the core | as stated | park_heads without its cause; no wake; a wake or a lock without `SKIP LOCKED`; the queue oldest first, outer or inner; the queue probed without its slots; the canary below none; the ground not selected; an expired row re-armed keeping its wait; a refresh resetting the start; an opening not counted; a resumed row refreshed; the waiting counted with the expired; the audit untold, or without its Tenant; no `more`; nothing passed; the park leaving a hold; the walk passing a verb's lock by; a report's failure failing the dispatch; the heads read out of the index's order; a reopened stall waiting for its tick |
| P1 | the guest: more than K paused Tenants and a due head beyond K; three backlogged Tenants of an unpaused channel in turn; a guest that serves clears the round, one that does not leaves it; at 128-character ids, no index of the slots' table reads what a guest writes, the condition of its write's being HOT | as stated | drop the cursor; keep it on a take; keep the round; index `guest_tenant` |
| I1 | a dispatch, a trial and a `resolve` inside REPEATABLE READ, the trial's session set to it after the process began | refused | a trial or a `resolve` without the check; the check at start only |
| D1 | the dek: a distinct cause per class, `Forbidden` `Permanent`; the sentinel by a key read on a channel the consumer only receives; `kms.unexplained`'s detail without a path; no Tenant in a log line | as stated | draw the sentinel's key; collapse two causes; store the text |
| T1 | `outage.rs` recorded by `make traces`; a recording not covered fails unless `traces/uncovered.tsv` lists it with its reason: for a run the converter refuses, the converter's, word for word, the steps before the refusal fitting; for one with a step TLC refuses, the record that owes the diagnosis; a statement of a test's own that writes rows behind the observers noted in the trace, and refused | as stated | a recording test not listed; a stale entry, recorded by no test or covered; a reason changed; a run listed as not fitting that fits; the recorder without the verbs' `operated`; a write behind the observers not noted; a step before a refusal that does not fit |
| B1 | the buffers the take and the walk read to find a message at 64 slots, the walk's to pass two Tenants, an idle take's, and the walks' steps, the take's looks by its index alone (`pg/plan_tests.rs`, under `test-pg`); a happy dispatch's statements, at one slot and at 64; the take, walk and expiry planned once a connection, a reset of the plan cache counted by a canary | 198, 24, 19 and 2 buffers, 3 and 2 steps, 10 and 11 statements, on 14 and 18 alike | an added heap fetch; a second slot-row read; a take's look reading outside its index; an idle take looking at every slot; the take's isolation read as its own statement; the mark locking its identity twice; the walk's or the expiry's plan not forced generic |

**The model.** `InboxOutage.tla` is rewritten over (slot, Tenant) heads with processes of workers, an ask split
into its start and its verdict with a rollback between, the breaker per process, the memo, a process cut off and
cached keys, the expiry's spare and the operator's verbs. Properties: `UnavailableNeverSpends`,
`UnavailableNeverParks`, `HoldIsQueued`, `CauseIsCurrent`, `KeyOrder` (the order of a stream; the rows of a
(slot, Tenant) are its streams'), `OneAskPerReach`, `PresentDependencyNeverExpires`, `CanaryExact`,
`GivenUpIsTheEnd`; and the liveness of the others going on (a process cut off for good), of a held Tenant asked
again (a Tenant's fault that ends), and of every row processed or parked. The bound on asks is not the model's:
TLC has no rates, and the core's tests are its executor. `Inbox.tla` gains the guests' cursor, and
`BoundedOvertaking` becomes at most two overtakings by each other Tenant for a head no guest skips. Each
configuration that must fail lists one property and has a twin, its `Mutant` set to `"none"`, which `check.sh`
generates and runs; `check.sh` pins each passing configuration's and each twin's count of distinct states
(`verify/tla/README.md` § Inbox outage).

**Measurements owed.** M4 the expiry at the host's count of spared waiters, on the host's machine, against 220,001
in 2.3 s a pass on the reference machine (§ 7): the host's, with its adoption (§ 12).

## Measured

On the reference machine: 12 threads (Intel Core i5-1235U), NVMe, PostgreSQL 14.24 as packaged, its settings
the defaults (`shared_buffers` 160 MB). Times compare only with each other on it; counts compare anywhere.

**M1, the happy path's statements before the decision and after** (R9: nothing new above 0.1 ms a message;
`pg/measure_tests.rs`, `breaker/tests.rs`). The statements of 47d6848, the library before slice 3, beside today's,
on one table of the host's launch shape: 64 slots, 2,000 Tenants of a hundred streams each, 1,584 Tenants a slot,
a processed history of 200,000. The server times blocks of 5,000 calls, each call committed as a dispatch commits,
`synchronous_commit` off; A B B A, four rounds, the median of each. Two runs:

| | before, µs | after, µs | the change, µs |
|---|---|---|---|
| the take | 278.7, 281.6 | 286.4, 280.8 | +7.8, −0.8 |
| the walk, every head due | 82.9, 82.0 | 102.2, 108.5 | +19.3, +26.5 |
| the walk, nine heads in ten in back-off | 225.9, 224.6 | 254.0, 256.4 | +28.1, +31.8 |
| the mark, against one that sets as many columns as 47d6848's, on a twin without the three CHECKs | 113.8, 119.9 | 119.5, 115.9 | +5.7, −4.0 |
| the breaker's work in the process, an optimised build | — | 1.1 | +1.1 |
| the expiry's statement a dispatch ran before every message, on no waiter | 150.5 | — | −150.5, and a round trip |

The take's and the mark's changes lie within their blocks' spread (some 8 µs); the walk's do not: it reads the
channel of each row it examines, tests it against the paused ones, and gathers the heads met, 20 to 30 µs a call,
a quarter of its cost. The new work on a happy message is so some 30 µs, under R9's 0.1 ms; and a dispatch no
longer runs the expiry's statement, 150 µs and a round trip, so the happy path costs less than before the
decision.

**M7, the operator's verbs at the host's scale** (`pg/measure_tests.rs`). The launch shape in a full outage: 64
slots, 2,000 Tenants of a hundred streams over seven channels, a Tenant with a backlog of 70,000 rows, a processed
history of 200,000, every head held under one cause: 101,452 held heads. Timed at the client:
- a page of 100 held heads: 39 ms the first, 31 ms one from the middle — the page sorts the held heads each time;
- `park_heads` of one channel: 13,741 heads in 784 ms, 5.7 ms a batch of 100;
- `park_queue` of the backlogged Tenant on one channel: 10,000 rows in 5.3 s, 52 ms a batch. Each batch probes
  the Tenant's newest rows in each of the 64 slots by `__tenant_head_idx` and reads their channel from the heap:
  some 45,000 rows a batch, 450 for each row parked, the batch's cost the Tenant's queue in its slots, not the
  table's. A Tenant's backlog of a million would take some 75 s a channel; an operator's verb, each batch
  committed, holding nothing between them. Kept;
- `unpark_all` of what `park_heads` parked: 13,741 rows in 861 ms;
- eight dispatchers on a channel re-armed and due, five seconds each: alone, 6,775 dispatches, 5.20 ms median,
  11.4 at the 99th centile, 13.8 at most; beside `park_heads` of the five other held channels one after another
  (72,189 heads in 6.2 s), 7,858 dispatches, 5.33 ms, 11.7, 40.0. A walk that meets a head a batch locks waits
  for the batch, some 6 ms; a verb does not stall the dispatch.

**M3, a backlog of another channel beside a paused one** (`pg/measure_tests.rs`). One process of eight loops, a
200 ms poll, K = 32, 64 slots, 32,000 Tenants with a head on a channel whose every ask fails at the stage's scope;
the process paused it after 0.3 s, and three seconds later 20 rows arrived on another channel, each a Tenant's
head after every paused head in its slot's order of Tenants. Two runs: the first processed 21.6 and 22.9 s after
they arrived, the median 23.9 and 24.2 s, the last 27.4 and 27.5 s; the guests made 36.7 walks a second that found
nothing, each of K steps and a slot row's HOT write. N / (L · K / poll) is 25.0 s; a walk's own time, some 18 ms,
lengthens the circle to N · (poll + w) / (L · K) = 27.2 s, which the last met. At the host's 128,000 heads, three
replicas of eight loops, the wait is so 36 s, not the 33 s decision 5 first stated; decision 5 and the Rule of
Three now say 36.

**M5 and M6, an outage of the KMS drilled for an hour** (`crates/dek/tests/drill.rs`). The path a host's service
runs: producers sealing each of 7 channels' messages with the channel's stage (`Sealing::over`, a key reused for
1,000 messages or 60 s) and storing them through the inbox's channel; three consuming processes of eight loops, a
200 ms poll, each opening through a sealing of its own by destination (keys kept 5 minutes), a handler of 50 ms;
64 slots, 500 Tenants at 20 messages a second, and a launch Tenant's 100,000 messages in the two minutes before
the outage, faster than the consumers drain them. The KMS follows a script: up 10 minutes, down 20 (every call
refused as unreachable), then up under a quota of 60 calls a second — above the steady 40, below what draining
asks; a call past it is refused as a 429 is, of the class `Unavailable`. A producer cannot seal while the KMS is
down, so the outage meets the launch's backlog and nothing new. Two runs, the second counting the order's prefix:
- **Nothing parked, no attempt spent**: 171,998 and 171,997 messages stored, none left queued at the end; 16,000
  holds in each run.
- **The KMS while down**: 253 and 308 calls in 20 minutes, 196 and 250 of them unwraps — a Tenant's held heads
  asked again at its memo's pace — not a term in the Tenants.
- **Paused while down**: one channel of seven, in two or three processes, within some seventy seconds: where two
  Tenants or more failed a channel in the window. A channel where the launch Tenant alone failed was never paused, as decision
  1 has it of a quiet channel.
- **Recovery under the quota**: twelve pauses and resumes in its first 25 s, the quota's refusals read for a
  moment as the outage still on, the stall reported; then none, a call past the quota refused and its row held and
  asked again. The launch's backlog of 54,453 and 58,464 rows drained in some 11 minutes, the producers' 24,000
  sealed and drained in 20; the KMS answered 58 calls a second against its 60 meanwhile. The drain runs at the
  quota's rate, without a herd: a host sizes the quota by the drain it accepts (§ 12, item 6).
- **M6, what the cache could have served while the KMS was down.** At the outage's start 62,214 and 64,415 rows
  were queued; 61 % were under a key a consumer held and 53 % before the first row of their (slot, Tenant) whose
  key none held — what the order across channels lets through (ADR-0041 decision 2). The consumers served 9,932
  and 6,040 of them in the first minutes, then none, on the channels never paused as on the paused one: at one
  minute 30,498 rows (50 %) lay before such a head under a key a consumer held, at three 28,654 (49 %), and at five
  none, their keys out of the cache. What held them is the memo: a head refused for want of its key holds its
  (channel, Tenant) in that process, every head of it, for a hold doubling to 300 s (decision 2) — the cached ones
  with it. They waited for the outage's end and then their turn in the drain, some 25 minutes, where the cache
  would have served them in its first five. The revision this asks for is owed (§ Owed, 1).

**M4** is the host's: the expiry at its own count of spared waiters, on its machine. Here, 220,001 waiters took
2.3 s a pass on the reference machine (§ 7).

**The two-session run of a verb against a concurrent hold** (`pg/measure_tests.rs`). `park_heads` of 4,000 held
heads, a batch a call, beside four sessions that held a head again or processed one, each change committed:
63 calls parked 3,975 and passed 1,699 by, beside 427 heads held again and 25 processed; at the end no head was
left held, none was both parked and processed, and every one was parked or processed. A head changed between a
batch's read and its lock is locked at its newest version, its selection tested there, and passed by — counted —
or parked; the next batch reads it again. The concern that such a head is neither parked nor counted, and `more`
false, does not hold: `passed` is what a batch examined and did not park, `more` an `EXISTS` after.

## Owed

1. A revision of decision 2 for a verdict at the stage's scope, under the library's gate (a blind solver, a
   discriminator, roles): whether the memo holds the Tenant when the stage, not the Tenant, refused, or only the
   row. M6 measured its cost — half of a launch's backlog, cached and let through by the order, held for the
   outage's length on channels never paused — and M5 its gain, the asks of a KMS down bounded at some 250 in 20
   minutes; the core's C1–C13, M5's drill and the trace and the model are the revision's executors.
2. The host's adoption (§ 12), in its own records.
3. A live deployment's steps, when there is a live client: expand and contract for `cause` and the dropped
   columns; the CHECKs added `NOT VALID` after a backfill; the take, walk, expiry and trial under new names while
   old binaries run; the indexes rebuilt concurrently; old and new replicas side by side spend, park and expire
   what new ones hold and spare.
4. The trace check of this decision's steps (T1 lists the runs it leaves out, `verify/tla/traces/uncovered.tsv`):
   - `Inbox.tla` gains a hold — a row held, no attempt spent, its next attempt later — and the operator's verbs
     over named rows; the recorder names each row a verb moves, an event of the observer's a row, where `Operated`
     only counts them;
   - `TraceInbox.tla` reads a row's permanence by the attempt, not by the message: a row that fails for a moment
     and then for good does not fit today (`inbox-outage_exits`), its `Permanent` a constant of `Inbox.tla`.

   Taken up with the rotation in the traces (ADR-0041 § Owed), without which most of the outage's runs, rotating,
   would not fit anyway; or sooner, should a defect of a hold or a verb pass the tests, the model `InboxOutage.tla`
   and the mutations that hold them today. A model that learns a hold makes the list's entries stale, and
   `make traces` fails until they are covered (T1).

## Slices

1. The breaker's and the memo's pure core, with C1–C13.
2. The dek's causes and its sentinel by a key read (D1).
3. The breaker in the dispatch: the rows, the exits and their CHECKs (R1, X1); the walk's paused channels, the
   guest, the trial and `Outcome::Paused` (G1, P1, C5, C8, C10, I1); the text the inbox keeps for a named cause
   whose detail the library composes (`kms.unexplained`). The rows and the dispatch are one slice: a row's hold is
   the interval the core gives.
4. The expiry off the dispatch path (E1).
5. The verbs and the reads, the stalls table (V1, § 6).
6. The model (§ Executors).
7. The traces (T1), and B1's gates.

## References

ADR-0005, ADR-0008, ADR-0009, ADR-0010, ADR-0036, ADR-0039, ADR-0040, ADR-0041. The host's ADR-0071 (sealed
payloads), its generated Vault policies.

## Change Log

- 2026-11-16: Proposed. A blind solver and a main position; a discriminator; three rounds of roles; the breaker
  stated as a circuit breaker after the third.
- 2026-11-17: Slice 1 built: the breaker's and the memo's pure core, C1–C13 its tests, each with its mutation,
  every one red (13 of 13). What building it found:
  - **The broad arm needed a share of its own.** A fast failure reads `Stage`, so a brownout of 5 % at 100 asks a
    second gives some 150 distinct `Stage` Tenants within W, and 32 alone would pause it (C2). The arm now
    needs a quarter as well; the rule's text above says so.
  - **The bound before a pause** is max(32, ⌈S/3⌉) Tenants, S at most 1,024, not 32: a warm window holds the
    share down (C1 measured it). Still no term in T.
  - **A witness tried inconclusively was tried again**: a row's own verdict now drops its Tenant's memo, and an
    inconclusive witness is not tried twice while its channel stays open (C6).
  - **Two mutants survived the first tests**: without the broad arm C1 still paused, because its window began
    empty and two failures made a share of one; and C11's channel opened before its causes could differ. Both
    tests now begin on processed Tenants, as an outage begins, and both mutants die.
- 2026-11-17: Slice 2 built: the DEK stage's causes per remedy, its sentinel read, and its log lines without a
  Tenant. Seven mutations, each red. What building it found:
  - **A remembered refusal lost its cause.** The opening core keeps a note for a Tenant with evidence of failures,
    not for one wrapped key the KMS refused, which is the message's: its memory answered with no cause. The shell
    now keeps the cause by the wrapped key, bounded.
  - **`kms.conflict`** joins the causes: a key made otherwise than asked is custody's remedy, no other's.
  - **A test counted log lines by the Tenant's name.** With no Tenant in a line it read the other tests' lines;
    it now reads its own by its thread, and asserts that no line names a Tenant.
- 2026-11-17: Slice 3 built: the breaker in the dispatch — the rows, the exits and their CHECKs, the walk's
  paused channels, the guest, the trial, `Outcome::Held` and `Outcome::Paused`, the isolation per transaction.
  Its executors: the database's tests (`tests/outage.rs`, R1, X1, G1, P1, C5, C8, C10, I1, the kinds and the
  causes of the holds, the canary), a gate over the library's own statements (`pg/exit_tests.rs`: every exit's
  fragment, every wait's clock) and the held counts' plan; 34 mutations, each red, and the core's 13 again. What
  building it found:
  - **`holds` is nullable, without a default** (§ 3). `NOT NULL DEFAULT 0` binds every insert of a release that
    does not know the column, and the release test refused the table (ADR-0037 decision 5). `NULL` is none; the
    fragment and the CHECK say so.
  - **The trial is statements, not a function** (§ 5): it runs once a tick a paused channel a process, where the
    take and the walk, planned once a connection, run every dispatch.
  - **The held counts read a heap page for every held row**: the index lacked the `uri` the channel is computed
    from. With it both versions can answer from the index alone (§ 6). The plan test first asserted that 18
    does; under `make all` it did not, the other tests' open transactions holding the visibility map down, so
    the test now asserts what the index offers — the index-only plan with the heap's scans set off — and that
    no row but the held is read, which is the library's, and leaves the choice to the planner's cost.
  - **`unpark` needs no isolation check** (§ 10): one statement, no lock it waits for and reads past. The
    refusal is the call's (`Error::Refused`), not the table's (`Malformed`).
  - **The canary's exactness had no executor.** `unpark_all` now sets a row's attempts to `max − tries`, where
    the higher of that and the row's own count parked a row spent to its last attempt again at its first failure,
    whatever `tries`; the test's rows had all parked at their first attempt, below both. One now parks at its
    last first, and the old form fails it.
  - **Three tests passed or failed by their timing, not by the rule.** The core's C10 advanced past the tick the
    pause had scheduled, so a meeting that brought no tick forward passed; it now meets the witness before that
    tick. G1 bounded a guest's step from above, so a walk without its bound, a whole circle back to its cursor,
    stepped zero and passed; a walk that is not a trial now steps K exactly. C5 asked for every healthy Tenant
    processed within four seconds, which `make all`'s load missed, and which the healthy Tenants met before x
    hung at all; they now arrive while x hangs, and one asked meanwhile is the assertion.
  - **A trial's timeout was told apart by its message**: the call now flags it, and the transaction rolls back.
  - **A test that woke every slot from four dispatchers at once deadlocked** on the slots' rows: the test's own
    statement, by the server's log, not the library's. One waker now.
  - **The migrations' checksums** were the baseline's before this slice and listed ADR-0040's causes migration,
    which the baseline took (ADR-0041 decision 6's exception, nothing deployed; § Owed 3 for a live one).
- 2026-11-17: Slice 4 built: the expiry off the dispatch path — `<table>__expire`, a function of the baseline;
  `run`'s timer; `PgInbox::expire()` and `Expiry`. Its executors: `tests/expiry.rs` (E1), the expiry's plan and
  its planning once a connection (`pg/plan_tests.rs`), the start check's cases for it (`tests/intake.rs`), the
  tests that waited for an expiry by a dispatch now asking `expire()` and one asserting that no dispatch
  expires; 29 mutations, each red. What building it found:
  - **The bound of decision 7 was wrong.** `max_wait + max(max_wait/10, ⌈spared/batch⌉ · interval)` takes the
    larger of the rest and the pass, where a waiter that becomes a candidate during a rest waits for the rest
    and then for the next pass's statements before it: the sum. Decision 7 now states the sum, and its pass
    runs its statements one after another, not an interval apart: at an interval a pass over 20,000 spared at
    the host's `max_wait` of 600 s and a rest of 60 s would take 200 rests, over three hours. One after another,
    220,001 waiters take 2.3 s a pass on the reference machine (§ 7).
  - **A hop walked its dependency's stream.** The probe by tenant, stream type and stream id, the identity then
    compared whole, reads every row of the stream, and a stream that is waited for keeps its rows (ADR-0033
    decision 4). The position is now part of the probe, read as an integer only when it is an integral number
    within the column's range: `1.0` is the number 1, as the mark's comparison of identities has it, and `"x"`,
    `1.5`, `1e12` match nothing. The plan test holds every hop to one probe of the key.
  - **A row with a time of wait had no invariant.** The index's predicate, `waiting_since IS NOT NULL`, holds
    the waiters alone only if no statement leaves a time on a row that does not wait; the baseline now refuses
    one (`__wait_is_open`). It found a test that wrote a parked row with a time of wait, a state the library
    never makes, and X1's step that aged every row with a `waiting_for`, the parked among them.
  - **A defect of the expiry stopped the loops when it ran in the dispatch**, and stops them still: the timer
    returns it to `run`, and an error of the moment waits as a loop's does.
  - **X1's histories expired nothing under the new rule**: each waiter waited for a message the history had
    published, queued and so spared. Every fifth step's publish now depends on a message that never arrives.
  - **A row expired names its reason**, the dependency's absence, its park or the chain's length, where one text
    named every case as never arrived.
  - **The plan check took a write for a read**: `ModifyTable` names the table it writes, and the rows it writes
    are its child's, which the check reads.
  - **The baseline took the expiry's function, its index's key and the CHECK** (ADR-0041 decision 6's exception,
    nothing deployed; § Owed 3 for a live one): the checksum test says so.
  - A round of roles (database with distributed systems) on the built slice found no row parked that should
    wait, no lost wake and no deadlock, and these, each taken:
    - **The reason named the direct dependency only**: a waiter whose chain ended at a message that never
      arrived read "no queued row within 8 hops", the text of a cycle. It now names where the chain broke.
    - **A pass's statements waited a millisecond each**: the timer slept `Duration::ZERO` between them, which
      tokio rounds up to its next tick. It now yields.
    - **A host that drives `dispatch` itself expires nothing** unless it calls `expire()`: `dispatch`'s
      documentation says so. Every service of the host runs `run`.
    - **A held vacuum horizon** costs a pass the heap of every waiter woken since (§ 7), counted.
    - **The expiry is not checked for READ COMMITTED**, and needs not be (decision 10).
    - **Nothing counts the spared waiters** until slice 5's `waiting_counts` (§ 6): a dependency that fails for
      ever without a permanent verdict spares its waiters for ever, where they expired before. The dependency
      shows in `held_counts` as a back-off.
- 2026-11-17: Slice 5 built: the reads and the operator's verbs (§§ 6, 8) and the names (§ 11). Its executors:
  the last tests of `tests/outage.rs` (V1, the stalls, the waiting counts), the verbs' and the waiting count's
  plans (`pg/plan_tests.rs`), C14 in the core, the exit gate over `verbs.rs`; 24 mutations, each red. What
  building it found:
  - **A probe of a Tenant's queue read its history.** By `slot = ANY(…)` the planner priced 64 descents of
    `__tenant_head_idx` above one bitmap scan of `__message_id_uniq`, which reads every row of the Tenant, its
    processed history among them: 10,050 for 50 queued. The batch probes slot by slot in a lateral, as the walk
    does, and reads the Tenant's queue alone.
  - **A lateral probe cannot lock as it fills its limit**, so a batch reads its candidates unlocked and then locks
    those still named; the verb counts by the rows examined, a held one examined again by the next batch.
  - **The canary was guarded twice**, in the call and in the statement, and a mutation of either survived; the
    statement's guard is gone.
  - **ADR-0040's `Operation::ParkedHeld`** outlived `park_held`; `PagedHeld`, `ParkedHeads` and `ParkedQueue`
    replace it, and `Operated` names the Tenant and the cause a verb moves rows by.
  - **The stall test waited for a pause in the whole run's effects**, so the second pause read the first's row; it
    waits for the nth.
  - **`Loops`' names are renamed in the outbox as well**: a host reads the two together.
  - **The baseline took the stalls' table** (ADR-0041 decision 6's exception, nothing deployed).
  - A round of roles (database, distributed systems, operability, security) on the built slice found, each
    taken:
    - **`park_queue` reordered a stream.** Parking the oldest first, a dispatcher took the row behind a batch
      between two, and the rows parked before it were then overtaken at their re-arm. The verb parks the newest
      first: what it parks is the end of each stream, every row processed meanwhile older. The test splits the
      verb at a batch and dispatches between.
    - **The stall report could stop every channel**: written on the dispatch path, its error failed the dispatch
      and a defect stopped the loops, against "a report for people, never a gate". It is bounded and its failure
      logged; a missed refresh is the next tick's.
    - **Two loops reporting one stall could cycle on its rows, or count one opening twice**, and the purge ran in
      the same statement as the upsert, where replicas could cycle. Rows in one order, a pause marked when taken,
      the purge its own statement `SKIP LOCKED`.
    - **A stall opened again reported at its first tick**, up to the tick's cap later, so a flapping stage's report
      could lapse between two openings. It is reported at its opening (C14).
    - **The audit of a verb that failed half-way was lost**, and it named neither the Tenant nor the cause. It is
      told after each batch, with both.
    - **A single `unpark` in a caller's transaction would hold the slots it woke** to that commit, and every take
      would pass them by. `unpark` writes no slot row again; its row is taken within `max_dormancy`, as
      ADR-0041's test asserts.
    - **`park_heads` read every held head of its cause each batch**, sorted by arrival, where `__held_idx` orders
      by the hold's start. It takes the oldest hold first, a batch of the index (plan test: 100 read, against
      4,000 in arrival order).
    - **`park_queue`'s test of what remains read the Tenant's history**: it probes slot by slot too.
    - Kept as they are, with their reasons: `unpark_all(AtOnce)` clears the unreadable mark, so a repaired
      arrival before the row is read again is set aside rather than superseding it — as the single `unpark` has
      always done, and readmitted; `held` and the verbs' cost at the host's scale is M7.
- 2026-11-17: Slice 6 built: the model. `InboxOutage.tla` rewritten for this decision (the ADR-0040 model and its
  nine configurations withdrawn), five configurations that hold and eight refuted designs, each with its twin;
  `Inbox.tla`'s guests and their bound; `check.sh`'s pinned counts and twins. What building it found:
  - **A bulk unpark could reorder a stream.** It read whether a later row of the stream had overtaken a row from
    its own snapshot, without holding the slot: an ask of that later row in flight was not yet processed, the row
    was re-armed, and the later row then committed past it (TLC's counterexample on the decided design, before
    the fix). A batch now reads its window, holds the window's slots `SKIP LOCKED`, and re-arms in a statement of
    its own snapshot, after the hold, only the rows in the slots it holds: an ask holds its slot from its start
    to its verdict, so none is in flight and none commits meanwhile. The rows in a slot a dispatcher holds are
    passed by (`UnparkedAll::passed`). The test dispatches the later row and re-arms beside it; the model's
    `InboxOutageRearmBesideAnAsk.cfg` is the refuted design. A batch is so a transaction of three statements, and
    reads its isolation first (decision 10).
  - **The liveness needed strong fairness, twice.** A process cut off whose breaker the model kept closed, or
    whose trial asked the same held head again, took the slot from another process at every turn under weak
    fairness, and kept a held row's hold from running out. The fairness is now strong for a worker's ask — the
    take serves the least recently served slot `SKIP LOCKED` — and for a hold's running out: a stage-scoped hold
    is at most a tick's cap and the trials a tick apart (decision 3).
  - **`OneAskPerReach` is a property of the steps**, not of the states: an ask started before the memo held its
    Tenant is in flight when the memo begins, as in the library; once the memo holds a Tenant, no ask of it
    starts while one is in flight.
  - **`TenantOrder` is `KeyOrder`**: the rows of a (slot, Tenant) are its streams', and their order is what a
    host relies on; with the operator's verbs, the re-arm's overtaking test and the queue given up from its end
    keep it.
  - **The guests' bound holds for a head no guest skips**: a paused channel's heads are skipped by its guests and
    walked by the slot's cursor alone; a guest without a cursor of its own overtakes a head more than twice
    (`InboxRotationGuestFromLowest.cfg`).
- 2026-11-17: Slice 7 built: the traces (T1) and B1's gates. `make traces` records `tests/outage.rs`, and a run
  it records that no trace covers fails it unless `verify/tla/traces/uncovered.tsv` lists the run with its reason
  (`check.sh --uncovered`); B1's figures are tests of `pg/plan_tests.rs`, with slice 4's test of the plans kept a
  connection, rewritten. Sixteen mutations, each red: eight of T1, eight of B1. What building it found:
  - **The recorder wrote neither a head held unasked nor an operator's verb.** A run in which a verb parked or
    re-armed rows showed only those rows' later steps, and could fit the model for a reason not the run's. Both
    are written now; the converter refuses a verb over many rows that moved some, as it refuses a hold, and names
    neither a slot nor a count of rows, which a run's timing moves.
  - **T1's reason is the converter's only for a run the converter refuses.** A run with a step TLC refuses has
    no reason of the converter's; it is listed as `unfit`, held to TLC's refusal, its reason naming the record
    that owes the diagnosis; one that comes to fit goes stale, and is covered.
  - **ADR-0041's two undiagnosed inbox traces are diagnosed.** `held_by_concurrent` wrote its holder's row by
    SQL, and records no trace now; `window_passed` stores again a row the sweep forgot, at a new position, which
    the converter refuses, `Inbox.tla` forgetting nothing. Three runs of the epoch relay crashed the converter on
    an acknowledgement without a position, an epoch delivered with nothing of its selection's: refused, with that
    reason.
  - **The covered traces had not been recorded since before this decision.** Recorded again, they fit, and
    `inbox-outage_rearm_beside` (a re-arm beside an ask of its slot) joins them.
  - **X1 passed by the machine's speed.** Under a breaker of 40 ms to 200 ms, a fast run — `make traces` runs
    the tests one at a time — spent a seed's whole history inside the holds and a pause, and processed nothing.
    Its breaker now runs out within a step, 1 ms to 5 ms, so that a history meets what its seed drew; slice 3's
    mutation that drops an exit's fragment still turns it red.
  - **B1's second slot-row read survived the walk that takes a head**, which reads no round. The walk that
    passes two Tenants is a figure of its own, and the mutation turns it red.
  - **The figures are 14's and 18's alike**, decomposed in the test: the take 198 at 64 slots, 3 a slot — an
    index's root and leaf and the newest queue row's heap page, a queue's new rows not all-visible; the walk 24;
    the walk that passes 19; the idle take 2. 18 prints a node's rows with two decimals, which the steps read as
    a float.
  - **A round of roles on the built slice** (verifiability, performance, database), its findings taken:
    - *The statements were counted at one slot*; a host cuts its table, and the mark then first locks its row's
      identity, which a waiter in another slot locks too: 10 at one slot, 11 at 64, both pinned.
    - *The test that the take, the walk and the expiry are planned once a connection could fail with nothing
      wrong.* A session that creates a schema — a backend's first temporary table in a database does, and the
      suite's siblings make theirs at their start — resets every kept plan of every session of the database:
      beside a schema created in a loop, each statement was planned at every call, 60 plans for 33. The test now
      counts the plans call by call and the resets by a canary, a function's statement that each reset plans
      again as it does the others': each of the three is planned in at most one call more than the canary, and
      its ten rounds run again, up to five times, while resets reach more than half of them. A schema every half
      second passes; a schema in a loop fails it, saying the count cannot tell. The walk's or the expiry's plan
      not forced generic still turns it red, by the count.
    - *A covered trace checked nothing*: `inbox-outage_isolation` was one receive, the refused dispatch another
      inbox's. It records nothing now.
    - *The buffer figures could not see a look turned into a heap fetch*, the table's visibility map empty: the
      take's looks are asserted index-only. *The idle take had no figure*, though it is the host's most frequent:
      2 buffers, its one look at an empty queue and the meta's, no slot's.
    - *A refusal hid the steps before it.* The converter writes the steps before a refusal, and they must fit.
      Seven did not: three runs of the epoch relay, three rotating runs of `outage.rs` (ADR-0041 § Owed), and one
      whose row failed for a moment and then for good, which `TraceInbox.tla`, reading off a run which messages
      fail for good, cannot say. Each is listed as not fitting, with its reason. A prefix with no dispatch named
      no count of slots, and the checker now takes the slots its rows landed in.
    - *That a run written behind the observers records no trace was a convention*, and two covered traces broke
      it: `attempts_saturate`, its attempts set by SQL, fit since the check of attempts is skipped when nothing
      parks; `window_kept` had its waits rewritten by SQL. The fixtures' `execute` now notes such a statement in
      the trace, and the converter refuses the run at it; a statement that only moves a time is `elapse`, the
      step of time the model takes unseen. Both traces are no longer covered; nine runs are listed for it.
    - *A forgotten row was told by its identity alone*; it is told now by a new position, so that a row
      superseded in place, at its own, is not.
    - *Whether the runs listed as not fitting stay so* was measured: the eight epoch runs recorded eight times,
      four of them beside a stream of short transactions in another database of the cluster: 63 of 64 verdicts
      unfit, and one a TLC run that ended with neither outcome, unfit when run again. Such a run is now run once
      more.
    - Not taken, as measurements by hand on one machine: the figures at the host's 2,000 Tenants a slot (three
      levels of index), the walk that wraps and a guest's walk, and a table with churned index entries — an
      absolute figure for those would move in a parallel suite with the cluster's oldest snapshot (M1, M3).
- 2026-11-17: Measured (§ Measured), on the reference machine: M1, the happy path's statements before the decision
  and after — the new work some 30 µs a message, under R9, and the expiry's statement no longer run by a dispatch;
  M7, the verbs at the host's scale, and the two-session run of a verb against a concurrent hold; M3, a backlog
  beside a paused channel. What they found:
  - **A walk's own time lengthens the guests' circle**: 36 s at the host's scale, not 33; decision 5 and the Rule
    of Three say so.
  - **`park_queue` reads some 450 rows for each it parks**, its batches probing the Tenant's newest rows in every
    slot and reading their channel from the heap: 5.3 s for 10,000 rows of a Tenant with 70,000. Kept, an
    operator's verb whose batches hold nothing between them; a third queue index by channel would cost every
    write (R9).
  - **ADR-0041's M7 measurement called the walk with three arguments** since slice 3 added the fourth; it passes
    an empty list of paused channels now.
- 2026-11-17: Measured M5 and M6, the outage of the KMS drilled for an hour, twice (§ Measured): nothing parked, no
  attempt spent, the KMS asked some 250 times while down, the backlog drained at the quota's rate. M6 found what
  the decision had not stated: the memo holds a (channel, Tenant) on a verdict at the stage's scope too, so a
  Tenant's cached heads stall on a channel never paused once one of its heads is refused — half of a launch's
  backlog for the outage's length. The consequence now says so, and a revision of decision 2 for the stage's
  scope is owed, under the library's gate (§ Owed, 1). M4 is the host's, at its count.
- 2026-11-17: § Owed gains the trace check of this decision's steps (4): what `Inbox.tla`, the recorder and
  `TraceInbox.tla` lack to check the runs of holds and verbs, and when it is taken up.
