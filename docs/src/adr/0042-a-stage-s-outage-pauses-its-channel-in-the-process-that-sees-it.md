# ADR-0042: A stage's outage pauses its channel in the process that sees it, and nothing is parked for it

## Status

Proposed (2026-11-16). Built (2026-11-17): slice 1, the breaker's and the memo's pure core
(`crates/inbox/src/breaker.rs`), C1–C13 its tests; slice 2, the DEK stage's causes and its sentinel read (§ 9,
D1); slice 3, the breaker in the dispatch — the rows, the exits and their CHECKs, the walk's paused channels and
the guest, the trial, the isolation per transaction (R1, X1, G1, P1, C5, C8, C10, I1); slice 4, the expiry off
the dispatch path (E1); slice 5, the reads, the stalls' report and the operator's verbs (V1, § 6, C14), and the
names; slice 6, the model; slice 7, the traces and B1's gates (T1, B1). Every slice is built; § Owed keeps the
measurements, the host's adoption and a live deployment's steps.

Revised (2026-11-17): the revision of decision 2 that M6 asked for — whether a verdict at the stage's scope holds
the row and not the Tenant — was decided under the library's gate (a blind solver, a main position, a
discriminator, three rounds of roles) and rejected (§ Alternatives Considered). The round found defects of the
build: slice 8 fixes those of decision 1's trials (one a tick call, hastened at most once a tick, a busy slot that
asks nothing, the close with no witness left) and brings the model's memo to the stage's scope; the memo's clock
written on rows it did not ask was owed, with its two remedies and the measurements that choose.

Revised (2026-11-18), slice 9: of the two, M8 chose. A replica cut off alone holding a launch Tenant's backlog on a
channel it does not pause: as built, 551 of 20,000 rows served in 20 minutes; with the memo's clock capped at the
tick on the rows, 8,894; with the memo's stage-scoped Tenants kept from the process's walk, its walk a guest's,
all 20,000 in 160 s against 120 s with no replica cut off (§ Measured). Decisions 2, 3, 5 and 6 say so.

Revised (2026-11-18), slice 10: § Owed 2's first risk, the hanging KMS, drilled (M10). The flapping it found is not
the timeout's: a trial whose witness's head another process had finished counted as inconclusive, its memo entry
kept and picked again at every pause; and a trial processed from the opening's cache closes a pause with the KMS
down. Slice 10 fixes the first and the rows' own path of decision 1 (a head gone counts nothing; a close no
processed trial made puts the channel on probation). The second — what evidence ends a stage's pause — was decided
under the library's gate (a blind solver, a main position, two discriminators, six roles in two rounds, two checks
of the synthesis); slices 11 and 12 build it.

Revised (2026-11-18), slices 11–12: what ends a stage's pause. The stage's dependency, registered by channel at the
composition root, is the only witness of its own outage: its refusals, broad enough, make the channel suspect; a
check of the dependency past the stage's memory confirms or refutes; and the pause ends only on a key the dependency
opened by a call. A row processed is no evidence of it (decision 1, the dependency's breaker; decision 9, the
opening end's dependency). Every other `Stage` refusal keeps decision 1's breaker on the rows. M10 again: no channel
resumed while the KMS was down, where a run at slice 9 flapped 309–537 times; the first stall 36–39 s into a hanging
outage at 500 and at 5,000 Tenants, against 88–122 s and 279–292 s; a shard of 30 % refusing paused nothing, its
healthy Tenants' p99 3.1 s against 240 s (§ Measured).

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
| `Unavailable`, `Stage` scope | (channel, Tenant), this process | the process's memo | the row refused held a tick at most; the walk treats the Tenant's other heads as not due in this process, writing nothing on them; one ask in flight | time, then one ask |
| `Unavailable`, `Stage` scope, broad, told by a stage, of a cause the channel's registered dependency answers for | (channel), this process | the dependency's breaker, once a check of the dependency confirms | the walk treats the channel's heads as not due | a check: a key the dependency opened by a call |
| `Unavailable`, `Stage` scope, broad, any other | (channel), this process | the rows' breaker | the walk treats the channel's heads as not due | a trial's verdict |

Two corollaries:
- **`Unavailable` never spends an attempt and never parks**, at any scope. A row parks for its own fault or by a
  person's hand.
- **No state of a stage's extent in the database gates anything.** The database holds the rows' own facts and a
  report for people.

### 1. The breaker: per process, per channel, a standard circuit breaker

A pure core, the time an argument, returning effects; one per `PgInbox` behind a mutex; told after each
subscriber verdict and driven by every `dispatch`, a host's own as well as `run`'s.

**Two sources of evidence, a breaker each** (slice 12). A row processed is evidence that the rows are served, not
that a stage's dependency serves: the opening end opens a key it holds with the KMS down (Context), and M10 found
pauses closed by such trials while the KMS was down. So a channel has a breaker for each source of evidence, and is
paused in a process while either is open:
- the **dependency's**, on a channel whose dependency the composition root registered (`PgInbox::depending_on`,
  from a bus `Dependent`; decision 9): it weighs the `Stage`-scoped `Unavailable`s the inbound stages told — the
  bus marks them — of a cause the dependency answers for, and its positive evidence is a key the dependency opened
  by a call: what it `served`, and a check served;
- the **rows'**, for every other `Stage`-scoped `Unavailable` — a handler's own, a stage's of a cause its dependency
  does not answer for, any on a channel with no dependency registered: its positive evidence is a row processed.
  The rules below, to the probation, are its.

Each closes by its own evidence alone. A channel whose dependency is not registered falls back to the rows'
breaker, so the library's composition test (W1) and the host's (§ 12) hold the registration.

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
1. a head of the channel whose Tenant has no memo, met by a guest walk (§ 5); meeting one brings the next trial
   forward to now — at most once a tick's first step, and not again after a hastened trial that left the channel
   open until a scheduled one has been issued: heads met add at most one trial a tick;
2. else a memoised Tenant's head whose memo has run out, the earliest due first, ties by the Tenant: the trial
   honours the Tenant's back-off;
3. else none at this tick.

**One trial a tick call.** A dispatch runs one trial, so a tick issues at most one: the channels due take their
turns, the one due longest first, and a channel whose tick finds no witness waits its next tick and leaves the
call's trial to another. A stalled channel is reported at least once a tick's cap, whether or not a call's trial
is its, so that its report outlives the turns of many channels.

The trial locks the witness's slot as `take` does, locates the Tenant's head in a statement after the lock with
`FOR UPDATE`, and runs the subscriber under `trial_wait` (the stage's own wait), past which its future is dropped
and the transaction rolled back. It never writes the slot's cursor; a processed trial clears the slot's round.
Its verdict:
- processed → closed;
- a `Stage`-scoped `Unavailable` of the pause's cause → open, f + 1;
- a `Tenant`-scoped `Unavailable` → that Tenant's memo, and the witness drops out;
- the row's own verdict, a head that cannot be asked (its row unreadable, or waiting on a dependency not yet
  processed), or the timeout → inconclusive; **three inconclusive verdicts in a row, on distinct witnesses, close
  the channel**;
- **a head gone** — its row finished, by another process as like as not, or the Tenant's head in the slot another
  channel's → nothing counted: the witness tried for this pause, its memo entry kept and its back-off renewed, not
  grown (what it remembered is stale, and its exponent still paces the Tenant for the other processes), and the
  next witness asked at the tick's first step. Until slice 10 it counted as inconclusive and its entry, kept, was
  picked again at every pause: three such closed a channel in two or three seconds without asking a Tenant that
  failed, 783 trials of M10's 1,888;
- a busy slot — a dispatch holds it — or a transaction that failed before a verdict was told → nothing: no failed
  tick and no inconclusive verdict, the witness again at the tick's first step, a hastened trial's haste not
  spent;
- **no witness left**: a tick that finds no witness and none to wait for — every head met and every memoised
  Tenant tried, inconclusively or gone — closes the channel; a head still refused at the stage's scope opens it
  again, its failing Tenants kept across the close. A process's guests cannot see a whole circle of a slot: their
  cursor is shared by every replica's, and a walk covers K Tenants.

**Probation.** A close that no processed trial made — three inconclusive, or no witness left — puts the channel on
probation until a row of it is processed: a pause while on probation counts a failed tick at its opening, whatever
the time since the close, and a stalled channel on probation keeps its report though closed, refreshed once a
tick's cap. A handler that waits on a hanging dependency past `trial_wait` closes its pauses inconclusively, and
its next refusal comes a client's timeout later, past the window: probation, not the window, tells its pauses from
a slow handler's, which never pause again. Such a handler stalls at its third pause, some 3 × (its timeout + three
trials) into the outage, and its report holds until a row is processed. The log says what closed a pause.

Every effect carries an id and a deadline: one unanswered by its deadline is a failed tick; an answer after it
changes nothing but a close. At most 64 witnesses a channel, oldest out.

**The dependency's breaker.** Its rule is the rows', in its own currency: F the distinct Tenants whose latest
verdict within W was the dependency's refusal, S the growth of the dependency's `served` since its oldest sample
within W (sampled at each dispatch, a sample a second kept; with no older sample S is none, an under-count that a
check corrects).
- **Suspect.** The rule trips: a **check** is due at the suspicion's step — 1 s, doubling at each refutation to the
  tick's cap, starting over after W without a trip — and stays due though no further verdict comes; the channel is
  still asked. A check served refutes the suspicion: F cleared, `Effect::Refuted` under the refusals' modal cause,
  so that a shard of the KMS shows in the log. So does a check unserved or unasked while `served` grew since its
  issue: the dependency served the rows meanwhile. Unserved otherwise → paused with a failed tick (those of a close
  shorter than W kept), its cause the check's; unasked → still suspect, the step doubled. A suspicion with no
  refusal of the dependency's left within W lapses: no check after, and a check in flight then pauses nothing at
  its deadline.
- **Paused.** A check a tick, the tick 1 s doubling to 15 s. The head it is given is the first met of the heads not
  yet asked this round whose Tenant the memo does not hold, else of the memoised ones; once every head met was
  asked a round begins anew, so that no head met is left unasked for good; with no head met, none. A head met
  brings the check forward, at most once a tick's first step, as it does a trial. Unserved → a failed tick, stalled at two,
  the stall's cause the latest unserved check's; served, late too → closed (`Resumption::Served`), F cleared, the
  memo kept; unasked → held, its report refreshed. A pause whose checks had nothing to ask and met no head for W
  closes (`Resumption::NothingToHold`): it holds nothing, and the next refusal trips the rule again. A row
  processed closes nothing, from the stage's memory or not.
- **A check** asks the dependency now, past the stage's memory and pacing (decision 9 says how the opening end
  does): served as soon as a key is opened, unserved when one is refused at the stage's scope or runs past its
  bound, unasked when nothing was asked or every answer was about the key itself. Its deadline is its issue, its
  bound and a tick's first step: unanswered by then, a failed tick, and at a suspicion the pause; an answer after
  counts only to close, the suspicion's too. One check at a time a channel — one still running when its pause
  closed is stale, its answer finding no check — and one check and one trial a tick call: a check takes nothing
  from the call's trial. The stall's ttl is twice the tick's cap and the bound: 35 s
  for the DEK.
- **Beside the dispatch.** A check runs spawned on the runtime: it holds no slot, no transaction, no connection
  and no loop. It is started before the dispatch's other work, the stall's report among it, so that its deadline is
  its own. The head's headers are read in a statement before it, never its payload, within a tick's first step —
  past it the check asks without them; the answer is queued and applied at the next dispatch's tick. A check that
  cannot answer — the dispatch dropped before it started, the check panicking (logged), no runtime — is answered
  unasked: its silence is no evidence, where its deadline would read it as the dependency's failure. Its effect
  carries the head's slot and Tenant for the shell, as a trial's does; no log line names a Tenant.
- **A rows' trial refused by the dependency** — the rows' breaker open on a handler's own refusals, its witness's
  row refused by the stage on the way in — tells the rows nothing: it is inconclusive for them, and the dependency's
  breaker weighs the refusal as any other of its kind.
- **The first stall** after the dependency last opened a key comes W for S to empty, a step, a check's bound, a
  tick and a check's bound later: some 43 s for the DEK. M10 measured 36–39 s, at 500 and at 5,000 Tenants.

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
- At the Tenant's scope, a head of a memoised Tenant met before `until`, or while an ask is in flight, is held to
  the later of `until` and the in-flight deadline, always ahead of now, and the subscriber is not called: written
  on the row (§ 3), the Tenant's own trouble for every process. A Tenant without an entry is never held.
- **At the stage's scope the process keeps its evidence to itself.** While the entry's back-off runs or an ask of
  it is in flight, the walk treats the Tenant's heads on that channel as not due in this process (§ 5), and
  nothing is written on them: the other processes, which did not see the stage refuse, take them. A head met
  anyway — the memo took the Tenant after the walk began, or the keys the walk was given missed it — is passed
  (`Before::Pass`), unasked and unwritten, and the keys rebuilt by it; so is a head of a channel paused after the
  walk began, the pause one process's evidence too. An entry's run of refusals begins anew after a back-off that
  ended more than a window before the next. Until
  slice 9 such a head was held to the memo's `until` on the row, up to 300 s for every process: M8 measured a
  replica cut off alone so holding a Tenant's whole backlog from the two healthy ones (§ Measured).

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
- A head held unasked at the Tenant's scope (§ 2) writes `cause`, `held_since`, `hold_kind` and
  `next_attempt_at`, and not `holds`; its interval is the memo's, `until` or the in-flight deadline. At the
  stage's scope nothing is written but the row the subscriber refused, so the 15 s above is the most a replica
  cut off alone delays a healthy replica's head, one row a re-ask.
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

- `<table>__walk` gains the paused channels and the (channel, Tenant)s the memo keeps to the process at the
  stage's scope (§ 2), a `jsonb` object of `"<channel>/<tenant>"` keys — a channel is `scheme://authority`, so the
  first slash after it begins the Tenant — looked up with `?`, flat in its size; a head is due when its own
  back-off has passed, its channel is not paused and its key is not in the object. With none paused and the object
  empty, today's statement. The object is kept built while the memo and the channels' states do not change and no
  excluded entry's back-off or flight ends (C18).
- A process with a paused channel, or with any key in the object, walks as a **guest**, from the slot's own `guest_tenant` (a column not indexed,
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
  with ttl at least twice the tick cap plus `trial_wait`, or plus the check's bound for the dependency's breaker.
  The reporter is a random value drawn at the process's start: a label of the report, not an owner (ADR-0008
  decision 3). As built: the breaker's stall effects are
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
- `excluded()`, the process's own: the Tenants its memo keeps from its walk at the stage's scope, by (channel,
  cause), how many and since when the oldest's run began, never which — what a host reports of a replica cut off
  alone on a channel it does not pause, which writes nothing on the rows it leaves. A gauge of the host's; a run
  begins anew after a quiet window, so that a Tenant refused once a week does not read as cut off for a week.
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
- **The opening end's dependency** (slice 11; decision 1's dependency's breaker reads it since slice 12): the
  KMS, as the bus's `Dependency` names what a stage depends on, handed to the inbox by destination (`Dependent`,
  which `ByDestination` is). It counts the keys the KMS opened for flights (`served`: never a key held, never a
  check's); it answers for every `kms.*` cause; its bound is `draw`. A **check** asks the KMS now, past the
  opening's memory and pacing, the head's key (or without a head the last key refused at the stage's scope) and the
  **control** — the last key the KMS opened by a call, a flight's or a check's of a head, aged from that opening and
  gone past `keep` or with its Tenant's key gone — concurrently, each judged by the code a flight's landing goes
  through, the sentinel's read included, and cut at `draw`. Served as soon as one is opened; unserved, under the
  control's cause first, when one is refused at the stage's scope or runs past the bound; unasked when nothing was
  asked, no session was had within the bound, or every answer was about the key itself (refused, gone with the
  sentinel there): only an opened key is evidence that the KMS serves — a KMS may answer a garbage ciphertext's
  400 while it opens nothing. A check's keys are dropped and wiped, it lands nothing in the opening's core (a
  failing Tenant's refusals still read the stage's scope after it), and it logs and keeps no error's text. A
  Tenant's recently opened key is the canary because a consumer may not draw a sentinel's key: its decrypts show
  in the KMS's audit, at most one a tick a channel a process while a channel is suspect or paused.
- **The bus marks a stage's `Unavailable`** told on the way in (`Unavailable::told_by_stage`), so that the inbox
  weighs only a stage's refusal against that stage's dependency, and a handler's own `Stage` refusal against the
  rows.

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
7. The opening stage's dependencies registered with each inbox it opens for — `PgInbox::depending_on(&opening)`
   beside `consumer.through(opening)` — and a composition test of the host's own as W1 is the library's: without
   the registration a sealed channel's pauses are the rows', and a row its cache serves closes them (§ Owed 2).

## Consequences

At 2,000 Tenants in each of 64 slots, 3 replicas of 8 loops, K = 32:
- **A full outage**: nothing parks and no attempt is spent; before the pause, at most max(32, ⌈S/3⌉) distinct
  Tenants asked and failed a channel a process (S, the Tenants processed within W, at most 1,024: so 342),
  each once before its memo holds it, and L in flight; then a trial a tick a channel a process, and one more a
  tick that a head met brought forward (at most 480 an hour); guests R · L / poll; the expiry's
  re-reads grow with the waiters, not the Tenants. Detection after recovery takes at most the tick cap and one
  trial. **No term in T.** Today: 427 asks and 4,032 walks a second.
- **A misread** pauses the process that made it, or, on a channel where one Tenant alone fails, keeps that Tenant
  to itself: a healthy replica's head waits at most the tick cap, for the one row the process refused a re-ask
  (M8); at one slot a trial holds the slot at most `trial_wait` a tick.
- **A full outage of a registered dependency** (M10, slice 12): no channel resumed while the KMS was down; the
  first stall 36–39 s into it, at 500 and at 5,000 Tenants; every channel resumed 1–5 s after the return, by a
  check served. A check a tick a channel a process while paused, each at most two decrypts: some 330 a process in
  ten minutes of a hanging KMS over seven channels, 460 of a refusing one.
- **A shard of the KMS refusing 30 % of Tenants**: no pause; the suspicions refuted by checks, 75–88 a process in
  ten minutes; the spared Tenants' p99 3.1 s, against the control's 4.2 s, where the rows' breaker gave 240 s.
  Hanging, nothing pauses before or after, and the loops are eaten as before (§ Owed 2).
- **A brownout** of 5 % reaches neither threshold; a flapping stage pages by § 6 (b). A KMS answering every call
  in 3 s — past the opening's 2 s wait, within its draw — pauses nothing under the dependency's breaker (two
  suspicions in ten minutes, both refuted), where the rows' breaker paused each process 13–17 times and stalled one:
  a page for a KMS that served every call. The spared Tenants wait out the opening's back-offs either way, p99 80 s
  against 85 s (M10).
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
  so held for the outage's length (§ Measured). Kept: the alternatives that freed them cost more than they freed
  (§ Alternatives Considered). Since slice 12 a row processed from the cache no longer closes the pause either, so
  that the rows made during a full outage wait it out: M10's p50 603 s of a 600 s outage, against 97 s when the
  flapping let some through, with 48–68 resumes a process while the KMS was down (§ Measured).

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
5. **A dependency left unregistered** falls back to the rows' breaker, and a row the stage's memory serves closes
   its pauses again. Checked: the library's composition test (W1) is red without `depending_on` and with the
   dependencies keyed by the producers' channel; the host repeats it in its own (§ 12).
6. **The canary is a Tenant's key.** The control is the last key the dependency opened, a Tenant's, so a Tenant
   whose own key is the trouble — a customer-managed key revoked — makes a check unserved while the KMS serves the
   others. Bounded: a check asks the control and the head's key together and is served by either, and the control
   is the last key opened, so it moves to another Tenant's at the next one opened; whether a host keeps such
   Tenants out of the control is owed (§ Owed 2).
7. **The suspicion's step starts over only after W without a trip.** A brownout that trips every few seconds keeps
   it at 15 s, so a real outage beginning then is checked, and stalls, up to 15 s later. Kept: a suspicion costs a
   check, a pause costs the cached rows; the step trades the KMS's decrypts against that delay, and the page's
   bound stays within a minute (M10).

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
- **A verdict at the stage's scope holding the row, not the Tenant** (the revision M6 asked for, decided
  2026-11-17 under the library's gate). Three forms, each refuted by roles against the code:
  - *a short stage clock per Tenant, and a Tenant's never-refused heads asked under a stage entry*: some ten times
    the KMS's calls and twenty times the rows written below the pause's threshold; the damper of M5's recovery
    under the quota lost; cached witnesses closing a pause while the stage is down, so that nothing pages for the
    cache's `keep`; a Tenant late at the dek filling the stage's flights, its neighbours then refused at the
    stage's scope;
  - *every stage entry forgotten on a success proven on a row the stage refused, the count restarted at a change
    of scope*: a herd on every success under a quota; and the dek's scope alternates within one Tenant's trouble —
    `Stage` right after each failed call, `Tenant` later in its back-off once another key has landed — so the
    exponent never grew;
  - *a memoised Tenant made not due for its process's walk, the process walking as a guest*, at either scope: a
    guest's fruitless walk sleeps the poll, which a healthy Tenant's head then waits in a partial outage (some 25 s
    at C4's shape, the Tenant's scope), and the keys sent with every walk cost some 0.75 µs a key. Kept to the
    stage's scope, it is decision 2 since slice 9: a Tenant-scoped entry's Tenant is held on the rows, and the
    stage-scoped keys are few but while a stage is down with caches warm (M9).
  The root: the inbox cannot tell a cache's success from the KMS's; the dek's scope is one process's and
  alternates; and at the host's scale a shared outage pauses every channel within seconds, so that the cached
  rows freed are few. The memo keeps both scopes, its count combined across them: the combination is what absorbs
  the dek's alternation.
- **The memo's clock on the rows capped at the tick** (§ Owed's first remedy, slice 9's measurement). Refuted by
  M8: a replica cut off alone re-holds each head it meets for 15 s, 4,636 heads in 20 minutes, and the launch's
  backlog it holds from the healthy replicas stays unserved — 8,894 of 20,000 rows.
- **The memo's stage-scoped Tenants kept from the walk, the process not a guest** (its cursor, round and sleep
  written as with nothing kept, a slot of kept heads sleeping a poll). Refuted by reading before M8 measured it:
  the shared cursor passes a head due for the other processes (`Inbox.tla`'s `PassTo` and the refinement no
  longer hold, `BoundedOvertaking` unbounded for the Tenant kept), the slot sleeps on one process's view, and
  fruitless walks multiply; M8 found it no better than the guest's (200 s against 180 s, a crowd's p99 1.8 s
  against 1.9 s).
- **What ends a stage's pause** (the round of 2026-11-18, M10). Refuted on the way to decision 1's two sources:
  - *the trial's wait above the stage's own, a timeout inconclusive* (the round's first main position): at a 5 s
    wait no trial timed out and the channels still flapped, 309–361 pauses a run — closed by heads gone and by
    trials processed from the opening's cache;
  - *the subscriber split in two, a trial whose stages it never reached a failed tick* (the first blind solver): a
    trial the stage answers from its cache reaches the handler and is processed; the split tells nothing of the
    KMS;
  - *memoised witnesses first, a close only by a witness the stage had refused* (the first synthesis), refuted by
    roles: while paused the dek's scope is read by trials alone, so a partial outage locks the pause; a gone head
    costs a tick; the memo is keyed by (channel, Tenant), the cache by (Tenant, wrapped key); at the host's scale
    the pause comes `keep` and W late;
  - *a processed verdict telling a key drawn from one remembered, a drawn one closing* (the second main position),
    refuted by the discriminator: in a full outage the cached trials starve the refused ones, and the stall waits
    the cache's `keep`; it needs a stage's declared wait, and a handler's of 30 s holds a slot 30 s of every 45;
    its surface on the bus is the probe's;
  - *a probe of the dependency counting any answer of the KMS as reach* (the third synthesis), refuted by roles: a
    KMS answers a wiped mount's `Gone` and a quota's 429 while it opens nothing; only an opened key is evidence;
  - *the dependencies returned `#[must_use]` for the host to register* (the fourth synthesis), refuted by its
    check: `let _ =` silences it. The composition test is the executor instead (W1).

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
| C7 | paused, the heads of its memoised Tenants given up by a person | closed at the tick that finds no witness and none to wait for; no stall after | a tick without a witness only waits; one closing at once (C10, C11, C14 red) |
| C8 | one replica cut off, caches warm | a row it refused held ≤ the tick cap, a `Tenant`-scoped one to its back-off; at one slot its throughput ≥ (tick − `trial_wait`)/tick; its other heads left to the others (K1, M8) | hold to `until` on the row; no trial timeout |
| C9 | a trial answered after its deadline | f unchanged | count a stale answer |
| C10 | one head of the channel among T ≫ K | processed within 2 ticks of recovery, the slot's oldest rows another channel's | the trial takes the slot's head rather than its witness's |
| C11 | mixed causes during a pause | one row a (channel, reporter); a stable cause | a cause per verdict |
| C12 | effects unanswered | `tick(now + d)` re-issues within d | in flight without a deadline |
| C13 | more memo keys than capacity | an entry in flight or not run out is never evicted | plain LRU |
| C15 | ten minutes of a stage down, a fresh Tenant's head met every 200 ms | trials 30–90: at most one hastened a tick | a head met hastens whenever a second has passed |
| C16 | twenty channels paused in one process; then one caller, a trial of 2 s at a time, ten minutes; in the database, two channels paused, their ticks due together | one trial a tick call, the next call the next channel's; every channel's stall reported within its ttl; every trial issued asked its witness | a trial for every channel due; a stall reported only at its channel's trial |
| C17 | trials whose slot a dispatch held, after a failed tick | the witness again within the tick's first step, first in line; no channel closed, no failed tick | a busy slot counted as a head gone; the busy witness waiting the schedule; a busy trial a failed tick |
| C18 | random histories of asks of both scopes, rollbacks, ticks and their trials of every outcome, heads met, asks held across steps, time alone, two channels; looked at also while an ask is in flight | the walk's keys, kept built, are what the memo says now | the keys outliving a back-off; a flight marked or abandoned, a verdict or a trial, not rebuilding them |
| C19 | a stage-scoped and a Tenant-scoped entry, a head of each met; the keys left stale; a head of a channel paused after its walk began | the first passed, nothing written, its key in the walk's object; the second held; the stale keys rebuilt by the pass; the paused one passed | a stage-scoped entry holding its heads; a pass not rebuilding the keys; a paused channel's head held |
| C20 | two Tenants kept at the stage's scope among processed ones, one at the Tenant's, a run renewed; a refusal after a quiet window | counted by channel and cause, the oldest run's start, no Tenant; none once the back-offs ran out; the late run's start its own | a Tenant-scoped entry counted; the latest verdict's time for the run's start; a run never begun anew |
| C21 | a Tenant asked twice at once, paused with another, both tried inconclusively, its late ask refused after, the channel closed for want of a witness | its renewed entry's key in the walk's object | a tick's close not rebuilding the keys (C18's histories do not reach it) |
| C31 | paused, a memoised witness's head gone at a failed tick, then two inconclusive trials and a processed one | not closed by the two; the next witness at the tick's first step; the gone witness not tried again; its entry's renewed back-off keeps its Tenant from the walk after the close | a head gone counted as inconclusive; it waits the scheduled tick; its entry's back-off left as it was; it not tried for the pause (C7, C21) |
| C32 | a handler waiting on a hanging dependency past `trial_wait`: pauses closed by three timeouts, each next refusal a window and a millisecond after the close | the third pause stalled at its opening; its report refreshed while closed; a processed row ends the probation | the failed ticks reset by the window's horizon; no refresh while closed; the probation not ended by a processed row |
| C33 | the dependency's refusals broad enough, of two causes; its check served; one refusal after; two right after | suspect, not paused: a check issued, the channel asked; refuted under the modal cause, F forgotten; the one trips nothing; the two no check within the doubled step, one at its end | a trip pausing; a refutation keeping F, or under another cause |
| C34 | a suspicion's check unserved, then the next | paused with a failed tick; then stalled, its report valid 35 s | an opening counting no failed tick; the rows' ttl |
| C35 | paused by the dependency, a thousand rows processed of asks begun before | still paused | a row processed closing it |
| C36 | paused and stalled, a hundred checks of a head unasked | still paused, its report refreshed as each is issued | an unasked check closing it; a pause that holds reporting nothing |
| C37 | paused, its checks with nothing to ask and no head met for W; a head met meanwhile | closed, nothing to hold; held while a head is met | never closed; closed with a head met |
| C38 | a trip within the step after a refutation, no verdict after | checked at the step's end | the pending trip dropped |
| C39 | a quiet channel's single sample; the dependency serving thirty keys within W; rows processed | S none, two refusals trip; eight do not; the rows count nothing for it | S from the rows processed; S ignored |
| C40 | paused, three heads met, one of a memoised Tenant, the first met again before each of four checks | asked in the order met, memo-less first, each once a round, then the first again | the newest taken; the memo's first; a head twice a round; none once each was asked |
| C41 | checks unserved under an outage's cause, then a credential's | the stall reported under the credential's | the cause fixed at the opening |
| C42 | a suspicion's check unserved while `served` grew | refuted | opened regardless |
| C43 | a check unanswered by its deadline, at a suspicion and paused, a millisecond before it and at it; its answers after; a second check due meanwhile | paused at the deadline, not before; its late answer served closing the pause; a failed tick that stalls; an unserved answer after counting nothing, a served one closing; one check at a time | an expired check counting nothing, at a suspicion or paused; the deadline the bound alone; the suspicion's late answer dropped; two checks at once |
| C44 | refutations in a row; then W without a trip | the step doubling to the cap, then 1 s again | never starting over; never growing |
| C45 | one channel paused by its dependency and by a handler's own `Stage` refusals | a check served closes only the dependency's, a trial processed only the rows' | one breaker a channel |
| C46 | random histories of the dependency's refusals, rows processed and of their own, what it served, heads met, time, checks answered every way or not at all | a pause of the dependency's only of a check unserved or unanswered; a close only of a check served or of nothing to hold; a check of the pause's own served closing it; a check issued only once the one before was answered or expired | a verdict pausing or closing it; a tick closing it; two checks in flight |
| C47 | a handler's one `Stage` refusal beside the dependency's two | nothing paused | the dependency's refusals counted for the rows too |
| C48 | paused through a long outage, every head met asked, the dependency's own keys gone, the heads met again | the next check asks them anew; the first served closes | a head asked once never asked again in the pause |
| C49 | the rows' breaker open on a handler's refusals, three trials refused by the stage's dependency | the dependency's breaker trips and checks; the rows' pause closed as inconclusive, no failed tick, no stall | a trial's refusal by the dependency counted as the rows' failed tick |
| C50 | a suspicion's checks unasked, no refusal after; another's check in flight when its last refusal leaves W | checked at 0, 2, 6, 14 and 29 s, then lapsed; the other not paused at the deadline | the step not doubled by an unasked check; a suspicion never lapsing, checked for ever or paused on nothing |
| C51 | a dependency's check and another channel's trial due at one tick call, the check first | both issued | the call ending at its check |
| C52 | paused, closed, paused again within W; then a window later | stalled at the reopening; one failed tick after a window | failed ticks never kept; never forgotten |
| H1 | the database (`tests/outage.rs`): a check hanging beside dispatches; the dependency refusing, its checks unserved under two causes, then served; two processes, one paused by its dependency, its checks unasked while the other serves the channel past the report's ttl twice over; the checks panicking; inside the crate (`pg/check_tests.rs`), a check started with the inbox's one session held, and one of a channel with no dependency | the slot another session's at once and another channel's row served by the next dispatch; paused only once a check confirms, the stall reported under the second cause, resumed by a check served, the rows going on; the first's stall row valid throughout, lapsing within its ttl after the resume; never paused, checked again; answered served, asked without the head's headers, and unasked | the dispatch running the check itself; the dependency's refusals weighed as the rows'; the checks' answers never applied; a pause that holds keeping no report (refreshed only at a failed tick); a check that cannot answer left unanswered; the head's read unbounded |
| W1 | the composition (`crates/dek/tests/composition.rs`): the opening stage by destination, two destinations under one sealed channel, one with a key; `depending_on`; the consumer through the stage; three Tenants' keys held, the KMS down, new Tenants' rows refused | both channels paused, none resumed while the KMS is down, the held keys' rows processed meanwhile; resumed once it is up | no `depending_on`; the dependencies keyed by the producers' channel |
| M10 | the drill (`crates/dek/tests/drill.rs`), asserted under `ASCETIC_DDD_MEASURE`, run by hand on the reference machine | no attempt spent, nothing parked (read from the table); in a full outage a stall row valid at every sample from the first stall to the return, the first stall within a minute, no resume while down and the last within 30 s of the return; no stall row valid three minutes after; under a brownout no pause; the spared Tenants' p99 within 8.5 s under a refusing shard, within 371 s under a hanging one (§ Owed 2's guard); the stage's calls tallied by class, by path and by the message's history (ADR-0043) | — |
| U1 | the database (`tests/outage.rs`): a paused channel whose three witnesses' heads cannot be asked, their causal dependencies unreadable | closed by three inconclusive trials | the shell telling an unreadable or waiting head as gone (closed on no witness left) |
| D2 | the dek's dependency (`crates/dek/tests/opens.rs`): a check of a head's key beside a cold open of its message, over opened, refused, gone × the sentinel there / gone / unread, out of reach, unexplained, past `draw`, and later than the wait within `draw`; the control on a fresh opening end, after a flight, asked again, past `keep`, a check's head, its Tenant's key gone; the last refused key asked without a head; `served` over a flight, a hit and a check; a failing Tenant's refusal after a served check; one key hanging and one answering, each way; no session within the bound; a check's lines with a Tenant's name in its errors; `ByDestination`'s dependencies by destination | as the cold open judges, the late one served; none, served, served, unasked, served, unasked; asked, then the control; 1, 1, 1; still the stage's scope; served at once; unasked; no line names the Tenant; keyed without the key, one channel's two destinations one dependency | a refusal read as the KMS's; the check cut at the wait; the control renewed by its check, never aged, not a head's, kept past its Tenant's gone; the refused key not asked; a check counted; a check's key landed in the core; asked one after the other; no session read as unserved; an error's text logged; keyed by channel, or with the key |
| D3 | the bus (`crates/bus/tests/stage.rs`): a stage's and a handler's `Unavailable` | the stage's marked, the handler's not | marked none; marked all |
| K1 | the database (`tests/outage.rs`): a process that heard a stage refuse a Tenant; a Tenant alone so refused; Tenants named with a slash, a space, a quote and a backslash | the first process takes no head of it, writes no row, walks a guest's walk (cursor, round, sleep untouched) and counts it in `excluded()`, and another process takes it at once; the lone one asked again once its back-off runs out; the odd one kept and `t/1` beside it taken, no head of it walked to | the walk not told the keys; the process not a guest; the keys outliving the back-off; a key unescaped, or of another separator than the walk's |
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
into its start and its verdict with a rollback between, the breaker per process, the memo — an ask's verdict of
either scope remembered, a trial's at the stage's scope the breaker's, as the core has it since slice 3 and the
model since slice 8, a stage-scoped one's Tenant left to the other processes, nothing written on its heads (slice 9,
`UnaskedHoldIsTheTenants`, refuted design `stage_held_unasked`) — a process cut off and cached keys, the expiry's spare and the operator's verbs. The trials'
rates (one a tick call, one hastened a tick) are not the model's: it has no time, and C15–C17 are their executors. Properties: `UnavailableNeverSpends`,
`UnavailableNeverParks`, `HoldIsQueued`, `CauseIsCurrent`, `KeyOrder` (the order of a stream; the rows of a
(slot, Tenant) are its streams'), `OneAskPerReach`, `PresentDependencyNeverExpires`, `CanaryExact`,
`GivenUpIsTheEnd`; and the liveness of the others going on (a process cut off for good), of a held Tenant asked
again (a Tenant's fault that ends), and of every row processed or parked. The bound on asks is not the model's:
TLC has no rates, and the core's tests are its executor. `Inbox.tla` gains the guests' cursor, and
`BoundedOvertaking` becomes at most two overtakings by each other Tenant for a head no guest skips. Each
configuration that must fail lists one property and has a twin, its `Mutant` set to `"none"`, which `check.sh`
generates and runs; `check.sh` pins each passing configuration's and each twin's count of distinct states
(`verify/tla/README.md` § Inbox outage). Since slice 12 the model has the dependency's source (`Depends`): a stage's
refusal remembers the key refused and may make the process's gate suspect; a check — past the process's memory,
asking the head met or the key remembered — opens the gate or closes it, served when the stage is up for the
process whatever it holds cached; a gate with nothing to ask and no head closes; the dependency may forget its
keys. Properties: `GateClosesOnItsEvidence`, an action property — the gate goes from open to closed only with the
stage up for its process, or with nothing to ask and no head — and `GateResumes`, the liveness of an open gate's
close once the stage is up for its process. Refuted designs: `cached_check_closes`, a check the stage answers from
its memory, and `check_needs_a_head`, a check that asks a head alone. The check's rates and order — one a tick,
the suspicion's step and its lapse, the deadline, the rounds of heads — are not the model's: it has no time, and
C38, C40, C43, C44, C48 and C50 are their executors.

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
  would have served them in its first five. The memo held them by three paths, read from the code since: in each
  process that refused a head; in the rows, its holds written on the heads it met, which every other process then
  skips (until slice 9, § 2); and on the paused channel by the pause, its heads first in their queues' order across
  channels. The revision this asked for was decided and rejected (§ Alternatives Considered).

**M8, a replica cut off alone** (`crates/dek/tests/drill.rs`, `a_replica_cut_off_alone_drilled`; slice 9). Three
consuming processes of eight loops, a 50 ms handler, 64 slots, steady traffic of 500 Tenants on six channels; a
launch Tenant's 20,000 messages alone on the seventh, a minute in, so that a process cut off never pauses that
channel — one Tenant fails it; a cap of 20 minutes. On the reference machine, PostgreSQL 14, A/B:

| Build | Drained | Served | p50 / p99 | Heads held unasked by the process cut off |
|---|---|---|---|---|
| no process cut off | 120 s | 20,000 | 9 s / 50 s | — |
| one cut off, as built (slice 8) | not within 20 min | 551 | 95 s / 1,109 s | 458, each to its memo's `until` |
| one cut off, the memo's clock on the rows capped at the tick | not within 20 min | 8,894 | 539 s / 1,103 s | 4,636 |
| one cut off, the stage-scoped Tenants kept from the walk (slice 9) | 160 s | 20,000 | 33 s / 87 s | 8 (races of the prototype) |

The process cut off there paused the six other channels and so walked as a guest. The same with only the launch
Tenant's keys refused, nothing paused anywhere, a crowd of 200 Tenants × 50 messages sharing its slots on another
channel: as built, 724 served in 20 minutes, the crowd's p99 24 s; kept from the walk with the process a guest
(slice 9, its walks a guest's 4,743 times), drained in 180 s, p99 121 s, the crowd's 1.9 s; kept from the walk
with the process not a guest, 200 s, p99 128 s, the crowd's 1.8 s. With two of three serving, 120 s × 3/2 = 180 s
is the drain's own bound.

**M9, the walk's keys at the memo's bound** (`breaker/tests.rs`, `m9_the_walk_s_keys_at_the_memo_s_bound`; slice
9). Seven channels, 1,024 Tenants each kept at the stage's scope among 20 processed a second, none paused: 7,168
keys, 423 KB; read kept built 0.033 µs a dispatch, rebuilt 1.3 ms (9.2 ms as first written, a string and a set's
entry a key). M1 on the same machine with and without the keys read: 5.33 µs and 5.35 µs a happy message.

**M10, an outage of the KMS hanging** (`crates/dek/tests/drill.rs`, `a_kms_hanging_drilled`; the round of
2026-11-18). Three consuming processes of eight loops, a 50 ms handler, 64 slots, 500 Tenants at 20 messages a
second over 7 channels, 40 launch Tenants' 20,000 messages in the minute before the outage; the KMS down 10 minutes
— every call hanging 30 s (a client's timeout) and then refused, or refused at once — then up 3 minutes. Nothing
parked and no attempt spent in any run. Runs of an arm differ by tens of percent. As built at slice 9:

| KMS while down | `trial_wait` | First stall (3 processes) | Pauses = resumes | Closed by a trial processed | By three inconclusive |
|---|---|---|---|---|---|
| hanging | 2 s | 88–122 s | 322–463 | 159–171 | 252–292 |
| hanging | 5 s | 90–125 s | 309–361 | 136–149 | 173–205 |
| refusing | 2 s | 7–11 s | 537 | 517 | 20 |

Trials by outcome (counters added for the runs only), hanging: at 2 s, 159 processed, 597 refused at the stage's
scope, 783 heads gone, 253 slots busy, 96 timed out; at 5 s, 136, 716, 610, 152 and 0. The flapping is not the
timeout's: heads gone counted as inconclusive, their witnesses' entries picked again at every pause (slice 10), and
trials processed from the opening's cache with the KMS down (§ Owed 2).

The same drill in other shapes (`ASCETIC_DDD_M10_PARTIAL`, `_TENANTS`, `_RATE`), as built at slice 9; "spared"
are the messages of the Tenants the outage does not touch, made while down, aged from made to handled:

| Shape | KMS while down | First stall | Pauses | Spared: p50 / p99 |
|---|---|---|---|---|
| no outage (the control) | — | — | 0 | 89 ms / 4.2 s |
| 30 % of Tenants' unwraps refused | refusing | 27–36 s | 2,227 | 21 s / 240 s |
| the same | hanging | never | 0 | 57 s / 297 s |
| 5,000 Tenants at 240 a second | hanging | 279–292 s | 309 | — |
| the same | refusing | 4.7–5.2 s | 426, every one closed by a trial processed | — |

The partial outage, refusing, holds the healthy Tenants by the pause's flapping. Hanging, nothing pauses, and a
failing Tenant's probe, trusted once another key landed, holds a loop for the opening's wait: 6,690 such waits of 2
s in 600 s are some 22 of the 24 loops. At 5,000 Tenants a hang pauses late: S counts the Tenants processed from
the cache, at its bound of 1,024 (§ Owed 2).

As built at slice 12, the opening's dependencies registered with each process's inbox (one run each, the full outages
at 500 Tenants two; "resumed while down" counts the resumes before the KMS returned, "after" when the last came):

| Shape | KMS while down | First stall | Pauses = resumes a process | Resumed while down; the last after the return | Checks a process; refuted | Spared: p50 / p99 |
|---|---|---|---|---|---|---|
| full, 500 Tenants | hanging | 36.3–37.9 s | 7, every one by a check served | 0; 0.9–5.2 s | 332–336; 0 | — |
| the same | refusing | 6.5–8.9 s | 7, by a check served | 0; 2.7–4.8 s | 457–471; 0 | — |
| 5,000 Tenants at 240 a second | hanging | 37.6–39.1 s | 7, by a check served | 0; 1.8–4.2 s | 331–334; 0 | — |
| 30 % of Tenants' unwraps refused | refusing | never | 0 | — | 75–88; every one | 368 ms / 3.1 s |
| the same | hanging | never | 0 | — | 0–1; every one | 59 s / 293 s |
| every call answered in 3 s (a brownout) | slow | never | 0 | — | 0–2; every one | 11 s / 80 s |

Nothing parked and no attempt spent in any run. Seven channels, so a full outage paused each once a process and
resumed it after the return, where a run at slice 9 flapped 309–537 times. The table's runs measured slice 12 before
the review of its code (§ Change Log); two shapes again on the code as committed: hanging, full, the first stall
36.8–38.3 s in, 7 pauses a process, none resumed while down, the last 0.6–3.3 s after the return, 333–335 checks;
30 % refusing, no pause, 79–84 checks every one refuted, the spared p50 368 ms, p99 2.9 s. The partial hang is unchanged: its
loops are eaten by the opening's trusted probes, which no breaker sees (§ Owed 2).

The reference, the same code with the dependencies not registered (`ASCETIC_DDD_M10_UNREGISTERED`), the rows'
breaker deciding as at slice 10:
- hanging, full: the first stall 82–113 s in; 55–75 pauses a process, 48–68 of them resumed while the KMS was
  down, 52–70 by a trial processed from the opening's cache. Slice 10's fix of the heads gone cut the flapping by
  some half (322–463 at slice 9), and the cache did the rest;
- the brownout: 13–17 pauses a process, every one closed while the KMS was slow, and one process stalled 220 s
  in — a page for a KMS that served every call; the spared p50 20 s, p99 85 s.

What the dependency's breaker costs: the messages made while a full outage lasts wait it out (p50 603 s, the
outage 600 s), where the flapping let some through from the cache (p50 97 s). It is the cost Consequences states
for the cached Tenants, now measured.

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

1. **The walk's keys at the memo's bound** (§ 5, slice 9). While a stage is down with caches warm, keys run out
   one by one within the opening's `keep`, and the Tenants refused trickle in without reaching the pause's
   threshold (F counts a 30 s window, an entry lives 300 s): up to 1,024 kept a channel, 7,168 over seven. M9
   measured the process's side there — kept built 0.033 µs a dispatch, rebuilt after a verdict on a memoised
   Tenant 1.3 ms under the breaker's lock, the object 423 KB at 36-character Tenants — and the walk-shaped
   statement the server's: parsed from the parameter some 0.75 µs a key, 5.5–6.1 ms a walk at 7,200 keys. Bounded
   in time by `keep`, and paid by the process that keeps the Tenants alone. Owed: the walk end to end through
   `locate_head` at 0, 300, 2,400 and 7,200 keys on the reference machine — messages a second, the backend's CPU,
   bytes sent, time under the lock — and, should the lock's share or the walk's time matter at the host's scale,
   the keys kept incrementally (an entry's key added or dropped at its change, not the object rebuilt) or a channel
   paused past a count of kept Tenants, the coarser grain of the same exclusion. And the shape where every
   process keeps a Tenant — one Tenant's trouble read at the stage's scope by every replica that asks it — so that
   every walk is a guest's and no slot sleeps: each loop's fruitless walk sleeps the poll, R · L / poll walks a
   second at most (M3 measured some 37 a process), against the rounds and the sleep the slots keep today; M8 had
   one guest of three. Owed: that shape's walks and slot-row writes a second, beside a healthy load.
2. **The risks the round found, each with the measurement that decides it.**
   - *The hanging KMS*, drilled by M10: decided by the round of 2026-11-18 and built in slices 10–12 (decision 1's
     two sources, decision 9's dependency). As built at slice 9 a channel flapped in either kind of outage — 322–463
     pauses a run hanging, 537 refusing — closed by heads gone and by trials processed from the opening's cache with
     the KMS down; at slice 12 none resumes while it is down, and the first stall comes 36–39 s in, at 500 and at
     5,000 Tenants (§ Measured). Owed with it, each measured before it is designed:
     - *a partial hang eats the loops*: M10's spared p50 59 s, p99 293 s at slice 12 as at slice 9. The attribution
       first written here — a failing Tenant's probe, trusted once another key landed — was wrong in proportion:
       ADR-0043 measured the trusted probe at most a tenth of the waits' loop-seconds, a Tenant the opening end held
       no evidence of (first met, or its failure forgotten while the memo spaced its asks) the rest. At M10's 500
       Tenants 62 % of them are on messages the stage had refused before; at 5,000 Tenants 82 % are on first
       contacts. ADR-0043 fixes the evidence and owes the remedy in four steps: among them a count of asks in doubt
       in this crate's breaker, for the rows asked again, and a bound on first contacts, their order its next round's;
     - *the recovery under a quota or an overloaded KMS*: decision 1's close is stated for a KMS that returns
       unthrottled; a close then lets the asks of every held Tenant through at once. A recovery arm before any ramp
       is designed;
     - *a 95 % hang*: few keys opened, S near none, checks served now and then; whether the pauses close on lucky
       checks and the page holds. An arm of M10;
     - *a replica cut off alone* pauses and stalls in that replica only, and pages by a host's rule on one
       reporter's stall row (§ 6, rule (c)); the library's executor is H1's single reporter;
     - *the checks' decrypts share the flights' sessions*: a check takes a session of the opening's supply; under a
       shard hanging, whether checks starve flights or the reverse. Measured under M10's partial hanging shape at
       the host's supply;
     - *the host's composition test and a start-time check* that every sealed channel of an inbox has its
       dependency registered (§ 12, 7);
     - *a Tenant on a customer-managed key as the canary* (Rule of Three, 6): whether a host keeps such Tenants out
       of the control.
   - A `Tenant`-scoped hold of up to 300 s on a row rests on one process's evidence — the dek reads `Tenant` once a
     key landed in that process: a replica partly failing (a lossy link, a cold start under the quota) holds rows
     the others would serve. The drill's cut-off variant at 50 % failing calls decides it.
   - At the stage's return the memo's cap, 300 s, is the stall M5 measured; its count, combined across scopes,
     carries the outage into a `Tenant`-scoped verdict at the return (`Waited`, `Late`). The cap's trade-off — the
     stall against asks ∝ 1/cap — is a setting (`Breaking::with_hold`), decided by a drill at the host's scale: at
     least 2,000 Tenants with backlog, the quota a stated multiple of the steady rate, A/B, reporting the asks
     offered, the pauses, the stall rows and the drain's time.
3. The host's adoption (§ 12), in its own records.
4. A live deployment's steps, when there is a live client: expand and contract for `cause` and the dropped
   columns; the CHECKs added `NOT VALID` after a backfill; the take, walk, expiry and trial under new names while
   old binaries run; the indexes rebuilt concurrently; old and new replicas side by side spend, park and expire
   what new ones hold and spare.
5. The trace check of this decision's steps (T1 lists the runs it leaves out, `verify/tla/traces/uncovered.tsv`):
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
8. The revision's round (2026-11-17): decision 1's trials one a tick call, hastened at most once a tick, a busy
   slot that asks nothing, the close with no witness left (C7, C15–C17, the two channels' test of C16); the
   model's memo at the stage's scope.
9. The memo's stage-scoped Tenants kept from the process's walk, the process a guest while it keeps any; a head
   met anyway passed; the keys kept built, rebuilt by change and by time; `excluded()` (decisions 2, 3, 5, 6;
   C18–C20, K1, M8, M9; the model's `UnaskedHoldIsTheTenants`).
10. The rows' path of decision 1 fixed: a head gone counts nothing and renews its entry's back-off, a head that
    cannot be asked is inconclusive, a close no processed trial made puts the channel on probation, the witnesses
    due ordered the same every run, the log saying what closed a pause (C31, C32, U1); M10 and its shapes.
11. The stage's dependency: the bus's `Dependency`, `Check` and `Dependent`, and its mark on a stage's
    `Unavailable`; the opening end's dependency — `served`, the control, the last refused key, the check — and
    `ByDestination`'s dependencies by destination (decision 9; D2, D3).
12. The dependency's breaker beside the rows' (decision 1): the verdict's source, the suspicion and its checks, the
    pause and its close by a check or with nothing to hold, `Effect::Check` and `Effect::Refuted`, `Resumption::
    Served` and `NothingToHold`; the checks spawned beside the dispatch, their heads' headers read, their answers
    applied at the next tick; `PgInbox::depending_on` (C33–C52, H1, W1); the model's gate; M10's pass lines and
    its brownout.

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
- 2026-11-17: The revision of decision 2 owed for the stage's scope, decided under the library's gate: a blind
  solver, a main position written before it, a discriminator, then three rounds of roles (distributed systems with
  the database and performance; verifiability with operability), each on the synthesis the last refuted.
  Rejected, in its three forms (§ Alternatives Considered); the memo keeps both scopes. What the round found in
  the build:
  - *Trials were hastened up to once a second for ever*: a head met whose Tenant the memo did not hold brought the
    tick forward whenever a second had passed, and a trial refused at the stage's scope memoises no witness — up
    to 3,600 trials an hour a channel a process, where this record said both "at most one a second" and "at most
    240 an hour". Now at most one hastened a tick (decision 1; C15).
  - *A tick issued a trial for every channel due, and a dispatch ran the first*: the others expired as failed
    ticks that asked nothing, counting toward a stall. Now one a call, the channel due longest first, and a stall
    reported at least once a tick's cap whatever the turns (decision 1; C16, and in the database two channels
    paused together).
  - *A slot a dispatch held was a head gone*: inconclusive, three of them closing a channel whose stage was down,
    a hastened trial's haste spent. And a trial whose transaction failed told the breaker nothing, expiring as a
    failed tick. Both are now a busy trial that asks nothing (decision 1; C17).
  - *The close with no witness had no caller*: `Breaker::circled` was called by the core's tests alone, and a
    process's guests cannot see a whole circle of a slot, their cursor shared. A tick that finds no witness and
    none to wait for now closes the channel (decision 1; C7), and `circled` is gone.
  - *The model held no memo at the stage's scope*, which the core has kept since slice 3: an ask's verdict of
    either scope is remembered now, a trial's at the stage's scope left to the breaker (§ Executors). Every
    property holds and every mutant still breaks its own; the instances with the stage down grew, the outage's
    from 17,368 states to 446,210 and the operators' from 23,986 to 3,685,902, whose liveness takes some 42
    minutes on the reference machine (`verify/tla/README.md` § Inbox outage).
  - *A process's memo writes its own clock on rows it did not ask*, up to 300 s for every process, where decision
    3 bounded a stage-scoped hold at 15 s for the row refused; decision 2's text and the build agreed, decision
    3's "as above" and C8 did not, and no test saw it. Decisions 2 and 3, the consequence of a misread and C8 now
    say what the build does; the remedy is owed with its two candidates and the measurements that choose (§ Owed,
    1), the round's other risks with theirs (§ Owed, 2). M6's paragraph names the three paths by which the memo
    held the cached rows.
  - *C5's database test failed once in a gate's run on 18*, "no healthy Tenant asked while x hung". Measured under
    six busy loops of the CPU: a run in ten failed, before slice 8 as after it. The test's own wake updated every
    slot, waited on the slot the hanging ask held, and kept locked meanwhile the slots it had already woken, which
    the other dispatchers' takes then skipped. Its waker now wakes the slots no dispatch holds (`SKIP LOCKED`);
    and its bound on holds counts the window before the first hanging ask, where x's first `Unavailable` already
    holds each other head once — 7 · (asks + 1), which a waker that works reaches (8 and 9 holds over one hanging
    ask). No failure in thirty runs under the same load; its two mutations still red (two asks of x at once; 421
    holds over three asks with the hold met in flight cut to the back-off).
- 2026-11-18: Slice 9, the remedy § Owed 1 kept open, chosen by M8 (§ Measured). A replica cut off from the KMS
  alone, holding a launch Tenant's backlog on a channel it does not pause, kept it from the two healthy ones: as
  built, 551 of 20,000 rows served in 20 minutes. The memo's clock capped at the tick on the rows changed little
  (8,894; each head re-held for 15 s, 4,636 times). Kept from the process's walk, the process a guest while it
  keeps any, the backlog drained in 160 s against 120 s with no replica cut off; with nothing paused anywhere,
  in 180 s, the process a guest. The same exclusion with the process not a guest was refuted by the roles' reading
  of the rotation (its shared cursor passes a head due for the others; `Inbox.tla`'s `PassTo` no longer holds)
  and measured no better. What changed:
  - decision 2: at the stage's scope the process keeps its evidence to itself; a head met anyway is passed
    (`Before::Pass`), unasked and unwritten; an entry carries its scope and its run's start;
  - decision 3: nothing is written at the stage's scope but the row refused, ≤ the tick's cap;
  - decision 5: the walk takes the kept (channel, Tenant)s as a `jsonb` object of `"<channel>/<tenant>"` keys,
    and a process with any walks as a guest; the object is kept built and rebuilt by a change of the memo or of a
    channel's state and by an entry's back-off or flight ending (C18 checks it against the memo over random
    histories with steps of time alone — a set rebuilt only at verdicts would keep a Tenant from the walk for
    good, which the roles found in the first design of it);
  - decision 6: `excluded()`, the count a host reports of a replica cut off alone, which writes nothing on rows;
  - the model: a stage-scoped entry's heads untouched by its process, `UnaskedHoldIsTheTenants`, and the design
    before as `stage_held_unasked`, which breaks it; the instances shrank (191,126 states, the operators'
    830,194), the group some 11 minutes on the reference machine;
  - M9: the keys at the memo's bound, 7,168, cost 1.3 ms a rebuild under the lock (9.2 ms as first written) and the
    server some 5.5 ms a walk; § Owed 1 now holds that bound's end-to-end measurement and its levers. The roles'
    other findings on the exclusion (the first design's stale set, its nested keys copied per row, its 200 ms
    sleep with no source, its race written on the row) are answered in its form as built.
- 2026-11-18: A review of slice 9's code found four things, fixed before it was committed:
  - *a tick's close did not rebuild the walk's keys*, on a reading that no entry could exclude then; an ask begun
    before the pause and refused after its Tenant's trial renews the entry, and the close leaves it out of a
    stale object, its heads then met and passed loop after loop. The close rebuilds them (C21: C18's random
    histories never reached it), and a pass rebuilds them too, so that a missed change heals at its first head;
  - *a run's start was carried over any gap*: a Tenant refused once a week read as cut off for a week. It begins
    anew after a back-off that ended more than a window before (C20);
  - *a paused channel's head met by a walk begun before the pause was held on the row*, under the pause's cause:
    one process's evidence on a shared row. It is passed (C19);
  - *with every process keeping a Tenant no slot sleeps*: owed with its measurement (§ Owed, 1).
- 2026-11-18: Slice 10, § Owed 2's first risk drilled (M10) and the rows' path of decision 1 fixed. What the drill
  and the round found:
  - **The flapping was not the timeout's.** Counted by outcome, a hanging KMS's trials were mostly heads gone (783
    of 1,888) and refusals (597); 96 timed out at a 2 s `trial_wait`, none at 5 s, and the flapping stayed. A trial
    whose witness's head another process had finished counted as inconclusive, and the witness's memo entry, kept,
    was picked again at every pause: three such closed a channel in two or three seconds. A head gone now counts
    nothing, its entry's back-off renewed rather than dropped — the round's roles refuted the drop: a replica cut
    off alone would then ask the healthy Tenants' heads anew, holding each a tick on the row (C31).
  - **A head that cannot be asked is not a head gone.** The shell told an unreadable row and one waiting on a
    dependency as gone; they are inconclusive (U1, C6 split).
  - **A handler hanging on its dependency never stalled.** Its pauses closed by three timeouts and its next
    refusal came a client's timeout later, past the window that keeps the failed ticks: probation keeps them, and
    the stall's report, until a row is processed (C32; the first form, counting a reopening within the window, was
    refuted by the second check — a 30 s timeout is never within a 30 s window).
  - **C21 passed or failed by a map's order**: two memoised witnesses due at one instant were taken in the
    `HashMap`'s order. The earliest due is taken first, ties by the Tenant, the same every run.
  - **The trials processed from the cache** (159–171 a run hanging, 517 of 537 refusing) and the late pause at
    5,000 Tenants are decision 1's evidence, not the rows' path: decided by the round, owed to slices 11–12.
  Seven mutations of the core and one of the shell, each red.
- 2026-11-18: Slice 11, the stage's dependency (decision 9), which slice 12 gives the breaker. What building it
  found:
  - **A check judges as a flight lands**: the flight's unwrap and its judgement — a key gone the Tenant's only
    while the sentinel says the KMS keeps its keys — are one function both call, so that a check cannot read a
    wiped mount as served; D2 sets the two beside each other over every outcome.
  - **Only an opened key is evidence**: the second check of the synthesis found that a refused answer as the
    control vetoes every pause while a KMS answers its cheap paths, and that a control renewed by its own checks
    never ages; the control is a key opened by a call, aged from that opening.
  - **No session is no answer**: a check whose session did not come within the bound asked nothing; read as the
    KMS unserved, a pool exhausted by hung flights would pause a channel whose healthy Tenants are served. The
    no-session path ends at the judgement's own timeout, not the check's — the first mutation of it survived for
    that, and the test now holds both.
  Sixteen mutations, each red.
- 2026-11-18: Slice 12, the dependency's breaker beside the rows' (decision 1), the round's decision built whole.
  What building it found:
  - **A refutation went untold.** Its cause was read from the breadth rule, which answers none once the refusals
    within W are no longer broad — by the time a check is served they often are not. It is the refusals' modal
    cause.
  - **The core's harness asked memoised Tenants.** Each trip refused the same Tenants, whose memo entries kept them
    from the walk, so the checks were given no head and C40's order was never met; each trip now refuses Tenants
    of its own.
  - **The composition test reproduces M10's defect**: without `depending_on` the channel paused by the KMS resumed
    while it was down, by a trial the held keys processed. The test's first mutation is that defect itself (W1).
  - **H1's single reporter passed without a refresh.** It served the channel for less than the stall row's ttl, and
    read the row its first stall wrote. It now serves twice the ttl with the checks unasked meanwhile, and a pause
    that holds without its report turns it red.
  - **The model's first mutant of a cached check broke nothing**: a check served by any cached head queued never
    opened the gate while such a head was there, and once none was, the mutant was the design. A check now asks the
    head met — any — or the key remembered, as the core's does, and the mutant breaks `GateClosesOnItsEvidence`.
  - **M10 asserts its pass lines** (§ Executors), and gains a brownout (`ASCETIC_DDD_M10_SLOW`) and its
    reference, the same code unregistered (`ASCETIC_DDD_M10_UNREGISTERED`), which found the rows' breaker paging
    under a brownout (§ Measured).
  - **The model** gains the dependency's source: `InboxOutageDependency.cfg` 275 296 states, its cut-off twin
    4 484, the refuted designs' twins 42 674 and 120 959; the configurations without it keep their counts.
  - **A review of the code found four defects**, fixed before the commit:
    - *a pause that never closed*: a head given to a check was not given again within the pause, so after a long
      outage — the dependency's own keys gone, the control past `keep` and the refused key answered about itself,
      every head met asked once — its checks were unasked for good, and the heads met kept it from closing for want
      of something to hold. The heads are asked round after round (C48, C40);
    - *a suspicion's late answer dropped*: the pause its check's deadline opened did not know the check, so a served
      answer after could not close it, against "an answer after counts only to close" — and C43 had pinned the
      defect as a stale answer;
    - *a check's deadline spent on the dispatch's other work*: the check started after the stall's report and a head
      read with no bound, and one that panicked, or whose dispatch was dropped before it started, was never
      answered, its deadline reading the KMS unserved — a pause, or a stall, on no evidence. It starts first, its
      head read within a tick's first step, and a check that cannot answer is answered unasked (H1);
    - *a rows' trial refused by the stage's dependency* counted as the rows' failed tick, and the dependency's
      breaker never heard it (C49).
    Two behaviours the decision did not state are stated, each with its executor: a suspicion lapses once no
    refusal is left within W (C50; it was checked for ever, and its check's deadline paused it under no cause), and
    a tick call issues a check and a trial both (C51). Tests that asserted less than they claimed — C33's one
    refusal after a refutation, which could not trip; C36's report; C40's memoised head; C46's closes and checks in
    flight — assert it now, and rules no test held have one each: the gate's ttl (C34), its failed ticks across a
    short close (C52), the deadline to the millisecond (C43), the step an unasked check doubles (C50), the
    refutation's modal cause (C33).
  Thirty-three mutations of the core, each red, the review's fourteen among them; one more, the dependency's
  refusals weighed by the rows' rule as well, equivalent — the rows' F holds none of the dependency's Tenants, and
  C47 tells the counting apart. Six of the shell (H1) and two of the composition (W1), each red; the model's two
  refuted designs. A seed C46 kept from a mutation's run failed on the built code once the property learnt to
  count checks in flight: a check still running when its pause closed on a late answer is stale, and the property
  had counted it in flight; it no longer does.
- 2026-11-19: § Owed 2's partial hang: its attribution corrected by ADR-0043's measurement, its remedy owed there
  in four steps. The drill gains a tally of the stage's calls and knobs: the healthy latency, the slow shard, the
  slow return and the launch.
