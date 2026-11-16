# ADR-0042: A stage's outage pauses its channel in the process that sees it, and nothing is parked for it

## Status

Proposed (2026-11-16). Built (2026-11-17): slice 1, the breaker's and the memo's pure core
(`crates/inbox/src/breaker.rs`), C1–C13 its tests; slice 2, the DEK stage's causes and its sentinel read (§ 9,
D1). § Slices orders the rest.

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

- Columns: `cause` (replacing `last_cause` and `parked_cause`), `held_since`, `holds integer NOT NULL DEFAULT 0`,
  `hold_kind` (`backoff` or `unavailable`), `next_attempt_at`. `unavailable_since` is dropped.
- `Permanent` and `Transient` as today; a `Transient` sets `holds = 0`, `hold_kind = backoff`.
- An `Unavailable` told by the subscriber: `cause`, `last_error = NULL`, `held_since = COALESCE(held_since,
  now)`, `holds + 1`, `hold_kind = unavailable`, `next_attempt_at = clock_timestamp() + interval`. The interval is
  the memo's `until` for a `Tenant` scope, and **at most the tick cap** for a `Stage` scope, the longer clock kept
  in memory only: a replica cut off alone delays a healthy replica's head by at most 15 s.
- A held head (§ 2) writes `cause`, `held_since`, `hold_kind` and `next_attempt_at` as above, and not `holds`.
- Every interval is bound against the database's clock, never a process's.

### 4. The exits

One fragment, `held_since = NULL, holds = 0, hold_kind = NULL, next_attempt_at = NULL`, in every statement that
takes a row out of the queue, conditional where the statement may keep it queued. `cause` is cleared by every
exit but a park, which writes its own. The baseline carries the invariant:

```
CHECK ((held_since IS NULL AND next_attempt_at IS NULL AND holds = 0 AND hold_kind IS NULL)
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
- A head of another channel waits for a guest at most N / (R · L · K / poll) per message: 33 s at the host's
  200 ms poll. Under a KMS outage every channel of a host's inbox is paused, so nothing waits on a guest.
- `<table>__trial(slot, tenant, channel)` is a function of the baseline beside the take and the walk.

### 6. The report and the page

- `<table>_stalls (channel, cause, reporter, since, valid_until, openings)`, one row a (channel, cause, process).
  A process writes it when its channel stalls and refreshes it every tick, `valid_until = clock_timestamp() + ttl`
  with ttl at least twice the tick cap plus `trial_wait`. The reporter is a random value drawn at the process's
  start: a label of the report, not an owner (ADR-0008 decision 3).
- **The host pages** a channel on (a) a valid stall row unless its processed counter rose over the window, or
  (b) a valid stall row of every up replica continuously for N minutes (a flapping stage), grouped by cause and
  naming the channels, with `keep_firing_for`, and the sampler's companion rules as the parked rows have.
- **The oldest `Unavailable` hold** over a threshold pages unless the channel is stalled on every replica, with a
  `for` longer than the hold cap: the net under a fault mislabelled `Unavailable` and under one Tenant's long
  trouble.
- `stalls()`; `held_counts(within)` by (channel, kind, cause); `waiting_counts(within)` by channel;
  `parked_counts` by (channel, ground, cause).

### 7. The expiry leaves the dispatch path

- A timer of `run`, and `PgInbox::expire()`, which takes no session, for a host that drives `dispatch` itself.
- One statement, `<table>__expire(max_wait, batch, after)`: a single `WITH RECURSIVE` over the dependency chain,
  an inner unlocked `LIMIT` over the candidates, and `FOR UPDATE … SKIP LOCKED` on the expiring rows only; no cast
  a malformed `waiting_for` could abort; a keyset on `(waiting_since, received_position)`; it returns the last key
  examined, and `batch` bounds the rows examined.
- A waiter is **spared** while its dependency is unparked and either queued or itself waiting on a spared one,
  within 8 hops; a ninth hop or a cycle expires it. A spared row is neither written nor locked. `max_wait` so
  means "the dependency never arrived", which a held head, a row behind one, an untried head and a long backlog
  are not.
- A waiter whose dependency is missing or parked expires within `max_wait + max(max_wait/10, ⌈spared/batch⌉ ·
  interval)`.

### 8. The operator's verbs

Each takes a principal and is audited (`Operated`); each batch is one autocommitted statement with `SKIP LOCKED`,
reporting what it did, what it passed by and whether more remain; each wakes the slots it touched.
- `held(after, limit)`: a page of the held heads with their identity, as `parked` pages the parked rows.
- `park_heads(channel, cause, tenant, limit)`.
- `park_queue(channel, tenant, cause, limit)`: a (channel, Tenant)'s queue rows in position order, given up whole.
- `unpark_all(channel, ground: Failure(cause) | Expired | AtOnce, tenant, within, tries, limit, overtaken)`:
  `attempts = max(0, max_attempts − tries)`, `cause = NULL`.
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

`<table>__take`, `<table>__trial` and the identity lock of `resolve` and `unpark` return the transaction's
isolation, and anything but READ COMMITTED is refused for that transaction.

### 11. Names

A channel a process does not ask is **paused**; the reported state is a **stall**; a **trial** asks a paused
channel's **witness**; a **guest** is the walk of a process with paused channels; the **memo** keeps a Tenant's
`Unavailable`. `Loops::max_pause` and `pause_after` become `max_error_wait` and `error_wait_after`.

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
5. An operator's command per service for the verbs of § 8.
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
- Tenants whose keys are cached stall while their channel is paused; they would have flowed at most `keep` (5 min
  by default) longer.

## Rule of Three

1. **Another channel's head waits for a guest**, 33 s per message at the host's poll, when one channel alone is
   paused. Checked: every channel of a host's inbox is sealed by one KMS, so a KMS outage pauses all of them; the
   case waits for a channel-specific dependency, and M3 measures it.
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
| C10 | one head of the channel among T ≫ K | processed within 2 ticks of recovery | trial through the general walk |
| C11 | mixed causes during a pause | one row a (channel, reporter); a stable cause | a cause per verdict |
| C12 | effects unanswered | `tick(now + d)` re-issues within d | in flight without a deadline |
| C13 | more memo keys than capacity | an entry in flight or not run out is never evicted | plain LRU |
| G1 | at S = 1, K = 4, a memo of 16, at 8 and 64 Tenants, driven until every slot is paused, then n more dispatches | calls = trials; changed rows = trial rows; walk steps = K; equal log lines; onset calls ≤ a figure | drop the walk's filter; a hold on each head met; no K; a log line per hold |
| E1 | the expiry: a present chain, an untried head, a row behind a held head, a missing and a parked dependency, 8 and 9 hops, a cycle under `statement_timeout`, 20,000 spared with one missing last, ties at one `waiting_since`, a malformed `waiting_for`, parked-expired rows; the spared rows' `(ctid, xmin, xmax)` and no xid in one transaction | spared or expired as stated, within the bound, ≤ `batch` rows examined | drop `present`; drop `parked_at IS NULL`; bounds 7 and 16; the cursor on expired rows; `LIMIT` after the filter; a `ctid` tiebreak; a plain cast; renew by write; lock before the spare test |
| R1 | the rows: 20 asks, no spend; `holds` at its cap; a `Transient` resets; the clock an hour fast and slow; a hold after a 3 s subscriber; four rows' kinds by an index-only scan | as stated | spend; no cap; an absolute time; `CURRENT_TIMESTAMP`; the kind from `holds` |
| X1 | the exits: random operation histories on a live database; every `UPDATE` that sets `processed_position`, `parked_at` or `waiting_for` carries the fragment; the spend that keeps a row queued keeps its back-off; `unpark_all` clears `cause` | no 23514 | drop the fragment; an unconditional fragment; keep `cause` |
| V1 | the verbs: a locked head passed by; a walk waiting on a verb's lock reads vanished; a slot asleep after a verb wakes within `max_dormancy` and the poll; the canary with `tries` above `max_attempts`; `park_queue` then `unpark_all` in order; the audit | as stated | as stated |
| P1 | the guest: more than K paused Tenants and a due head beyond K; three backlogged Tenants of an unpaused channel in turn; a guest that serves clears the round, one that does not leaves it; HOT with 128-character ids | as stated | drop the cursor; keep it on a take; keep the round; index `guest_tenant` |
| I1 | a dispatch, a trial and a `resolve` inside REPEATABLE READ | refused | check at start only |
| D1 | the dek: a distinct cause per class, `Forbidden` `Permanent`; the sentinel by a key read on a channel the consumer only receives; `kms.unexplained`'s detail without a path; no Tenant in a log line | as stated | draw the sentinel's key; collapse two causes; store the text |
| T1 | `outage.rs` recorded by `make traces`; a recording not covered fails unless listed with its reason, the reason the converter's | as stated | a recording test not listed; a stale entry |
| B1 | buffers a message and walk steps under `test-pg`; statements a happy dispatch at an absolute count; the take, walk, trial and expiry planned once a connection | absolute figures | a heap fetch; the isolation read as its own statement |

**The model.** `InboxOutage.tla` is rewritten over (slot, Tenant) heads with processes of two loops, an ask split
into its start and its verdict with a rollback between, the breaker per process, the memo, a process cut off and
cached keys. Properties: `UnavailableNeverSpends`, `UnavailableNeverParks`, `HoldIsQueued`, `CauseIsCurrent`,
`TenantOrder`, `OneAskPerReach`, `PresentDependencyNeverExpires`, `CanaryExact`; and the liveness of the others
going on, of a held Tenant eventually asked, and of every row processed or parked. The bound on asks is not the
model's: TLC has no rates, and the core's tests are its executor. `Inbox.tla` gains the guest's cursor, and
`BoundedOvertaking` becomes at most two overtakings by each other Tenant for a head due at every process. Each
configuration that must fail lists one property and has a twin generated from it; `check.sh` pins each
configuration's count of distinct states.

**Measurements owed.** M1 the walk with nothing paused, server side, committed per call; M3 the guest at a backlog
of 20 rows on an unpaused channel; M4 the expiry at the host's count of spared waiters; M5 a one-hour scaled drill
at 7 channels and a 200 ms poll, with a KMS quota below the drain's rate; M6 the share of opens served from the
cache during an outage; a two-session run of a verb's `ctid` join against a concurrent hold.

## Owed

1. The slices below, each with its executors.
2. The host's adoption (§ 12), in its own records.
3. A live deployment's steps, when there is a live client: expand and contract for `cause` and the dropped
   columns; the CHECKs added `NOT VALID` after a backfill; the take, walk, expiry and trial under new names while
   old binaries run; the indexes rebuilt concurrently; old and new replicas side by side spend, park and expire
   what new ones hold and spare.

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
