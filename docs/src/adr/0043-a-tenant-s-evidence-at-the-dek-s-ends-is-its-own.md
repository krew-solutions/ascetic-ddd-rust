# ADR-0043: A Tenant's evidence at the DEK's ends is its own: a trusted probe keeps its failure, a late landing restores no trust

## Status

Proposed and built (2026-11-19): step 1 of five (§ Owed). Built in `crates/dek`:
- the shared pacing (`src/pace.rs`);
- the opening end's core and shell (`src/unwraps.rs`, `src/opens.rs`);
- the sealing end's core (`src/supply.rs`) and shell (`src/draws.rs`);
- the drill M10's tally of the stage's calls and its knobs (`tests/drill.rs`).

Revises ADR-0039: decision 7 (a probe's trust) and the Consequences' "a Vault that stalls holds a caller for `wait`
at most, once per slot", which a partial outage refutes (§ Context).

Corrects ADR-0042 § Owed 2's attribution of "a partial hang eats the loops". It does not mend that harm: the harm
is measured here by its paths, and mending it is owed in steps 2–5, each with the measurement that decides it.

Decided under the library's gate, in two rounds:
- **Round 1:** a blind solver, a main position, a discriminator, and four roles (Distributed Systems, Operability,
  Performance, Verifiability) on the first synthesis.
- **Between the rounds:** three revisions of how long an open waits, each refuted by measurement or by a checker.
- **Round 2:** the problem re-posed to a fresh blind solver, a main position, a discriminator.

§ Alternatives Considered keeps every refuted design with its reason.

## Context

The opening end of the envelope stage (ADR-0039 slice 4) unwraps a sealed message's data key with the KMS.
- An open that finds no key starts an unwrap, or joins one in flight, and waits for it at most `wait`, 2 s.
- The unwrap is given up past `draw`, 5 s.
- The inbox awaits the stage inside the dispatch (`crates/inbox/src/pg/dispatch.rs`), so a wait holds a loop, a
  pooled connection, the slot's lease and the row's transaction.

**The failure** (ADR-0042 § Owed 2, M10's partial shape). The setup:
- 3 processes of 8 loops, 7 channels, 500 Tenants at 20 messages a second;
- a launch of 20,000 messages in the minute before;
- for 600 s the unwraps of 30 % of the Tenants hang, every other call answered at once.

On the reference machine:
- the spared Tenants' messages made while down waited p50 52–59 s, p99 267–293 s;
- the control waited 89 ms and 4.2 s;
- the same shape refusing at once instead of hanging waited 368 ms and 2.9 s.

**Where the loops went** (the drill's tally of the stage's calls, this ADR's first measurement, § Measured). The
broken Tenants' calls refused `Waited` took 94 % of the outage's loop-seconds, at 2,001 ms each; 22–24 of the 24
loops were in the stage at every sample. The spared Tenants' unwraps landed in 1 ms. Each wait was on an open that
found its Tenant trusted, by three paths:

| Path | What it is | Share of the waits' loop-seconds |
|---|---|---|
| (a) first contact | a (process, channel, Tenant) the opening end holds nothing of is trusted by default | 36 % |
| (b) the failure forgotten | the opening end remembers a failure some 39 s; the inbox's memo asks a held Tenant up to 300 s apart, from any process | ≥ 48 % |
| (c) the trusted probe | ADR-0039 decision 7: a failed Tenant's probe trusted once any other Tenant's unwrap landed in time after its failure — always, in a partial outage | ≤ 10 % |

The same waits split by the message's history: 62 % of their loop-seconds were on messages the stage had refused
before, and 38 % on a message's first ask. The evidence that would spare most of them is the row's, which the inbox
holds and every process reads; the stage never sees it.

**What this step fixes: three defects of ADR-0039 decision 7's evidence**, at both ends.

1. **A trusted probe erased the failure count.** Starting it set the Tenant trusted, so its back-off never doubled.
   The result: a wait of `wait` every 8 s a slot for as long as the outage lasts, where ADR-0039's Consequences
   promise one. The reproducer: a Tenant hanging beside another that lands; every probe trusted, "failures before
   0" at every cycle.
2. **Any landing, however late, restored the Tenant's full trust**, though only landings within `wait` count for
   the stage. A Tenant whose unwraps take 3 s was waited on 2 s at every cold key, and refused anyway.
3. **A refusal's scope read only the flight's own window.** A late Tenant's cold open, which starts a flight and is
   refused at once, read the stage's scope always (the roles' finding against the first synthesis).

They are path (c) and the conditions of the rest, not the bulk of the harm. This step does not mend M10
(§ Measured): it makes a Tenant's evidence its own, which the later steps rest on.

## Decision

**Principle:** a Tenant's evidence is changed only by what the Tenant's own unwraps do. Another Tenant's landing
may make the stage's case; it never erases this Tenant's.

1. **A Tenant's failure count is reset only by its own landing in time** (both ends).
   - A trusted probe is waited on, and keeps the failure and its count. While the Tenant's unwraps keep failing,
     its back-off doubles to its cap.
   - The flight a trusted probe starts carries the trust: its joiners wait on it as on any trusted flight
     (`Flying::trusted`).
   - A waiter that gave up after a whole wait makes the flight waited on no more, and a trusted Tenant late.
2. **A landing past `wait` restores no trust** (both ends).
   - The Tenant, or the slot, is late. The key it brings serves its wrapped key. Its next unwrap is started with no
     waiter, the open refused at once.
   - A late landing ends a run of failures: the KMS answered.
   - At the opening end, late evidence is forgotten a back-off cap after it was set, as a failure is. At the
     sealing end a late slot goes with its key.
3. **A refusal's scope reads the stage's landings over the last `wait`** (both ends).
   - It reads them from `min(flight's start, now − wait)`, and with no flight from `now − wait`.
   - A late Tenant's refusal with no flight yet is the Tenant's when another Tenant's unwrap landed in time within
     the last `wait`, as a waiter's was over its whole wait.

## Consequences

- **A partial hang at either end now backs a failing slot off to its cap.** At the sealing end some 2.2 draws a
  minute instead of 10.4, by the round's arithmetic; at the opening end by the same rule.
- **A Tenant slower than `wait` is not waited on again** until its own unwrap lands in time or its late evidence is
  forgotten. Before, it was refused after a 2 s wait; now it is refused at once. Measured in the brownout
  (§ Measured).
- **M10's partial hang is not mended by this step:** measured (§ Measured), and owed (§ Owed).
- **The drill reports the tally of the stage's calls**: by class, by the gap since the same key's last refusal, and
  by whether the message was refused before. The knobs: a healthy latency with a tail, a slow shard, a slow return
  and the launch's size.

## Rule of Three

1. **A Tenant slower than `wait` refused at every cold key while it is late.** The outcome is as before (refused),
   less the 2 s a loop spent on it. The brownout's arm against its baseline decides it (§ Measured).
2. **A trusted probe that fails after a full outage backs off longer**, its count kept, and may delay the return.
   The full outage's last resume within 30 s decides it (§ Measured).
3. **The scope window's first `wait` of a full outage.** The landings just before it read the Tenant's, delaying the
   breaker's breadth by at most a wait. The first stall within a minute decides it (§ Measured).

## Alternatives Considered

**Four rules for how long an open waits**, each refuted, which is why this step takes none:

1. **Latency-led** (`clamp(8 · smoothed in-time landings, 250 ms, wait)`). Refuted by the roles:
   - priced by the healthy latency: at 100 ms a fruitless wait costs 800 ms, two thirds of the loops;
   - one slow landing lengthens every Tenant's patience;
   - a quiet opener forgets.
2. **The grace (250 ms) once a quarter of the waiters gave up.** Refuted by measurement: a give-up at the grace
   counted as fruitless kept the grace on a slow but healthy KMS, and 901 healthy opens were refused at 300 ms.
3. **The grace only past a budget of two waiters a process**, a wait judged by its unwrap. Refuted by measurement:
   the budget's waiters were always busy on hung unwraps, and M10's spared p99 was 21.4 s.
4. **Twice the 90th percentile of the repaid unwraps, once the waits hang.** Refuted by a checker's arithmetic, and
   measured after (§ Measured):
   - the trigger is a share and the harm a rate;
   - priced by the healthy latency again;
   - a tail inside `wait` is refused;
   - a quiet opener refuses healthy keys.

   The drill's two lines it was held to, "no spared refused" and "at most a quarter of the loop-seconds at any
   latency", cannot both hold for any rule of that family.

**Round 2's positions.**
- **The main position** sent the row's history into the stage by a header, and waited the grace on a held row.
  Refuted by the discriminator:
  - "no wait at all" never opens a row held at the memo's cap of 300 s, past its key's `keep` of 300 s;
  - "the grace" is a price again, some 36 % of the loops at 5,000 Tenants;
  - a key landed in one process serves a row that returns to any.
- **The solver** deferred a head in doubt to a fetch beside the loops (`Dependency::fetch`), its row asked at the
  landing. "The landing, not the memo's clock, brings the row back" is forced by the facts; the new bus method and
  a budget of fetches are not.

The synthesis is the order of § Owed.

**Longer failure memory at the opening end.** Refuted: the memo's pacing is the row's, shared by the processes,
while the memory is one process's. A pair asked every 300 s meets the same process a third of the time.

## Executors

| Id | Scenario | Assertion | Mutation |
|---|---|---|---|
| U1 | a Tenant hanging beside another that lands (`unwraps.rs`; the sealing end in `supply.rs`) | the trusted probe joined, its failure kept, the back-off doubled | the probe sets the Tenant trusted |
| U2 | a trusted probe and a second open of its key (both ends) | the second joins | a join reads the Tenant's evidence |
| U3 | an unwrap landing past `wait` (both ends) | its key serves; the next key unwrapped unwaited; at the opening end forgotten a back-off cap later; at the sealing end the refusal the slot's own | any landing trusts; late evidence never forgotten |
| U4 | a late Tenant's cold open, another's landing within the last `wait`, then none (`unwraps.rs`) | the Tenant's scope, then the stage's | a refusal with no flight reads no window |
| U5 | a give-up after a whole wait, and after half (`unwraps.rs`) | the Tenant late and the flight waited on no more; no evidence after half | the flight's trust kept after the give-up; `>=` read as `>` |
| P2, P3 | the opening core over random histories (`unwraps.rs` property) | a Tenant's failures change only at its own landing or past its memory; an open that starts a late Tenant's unwrap does not wait on it; late landings and failed trusted probes all met | the probe resets; a late landing trusts |
| F | the sealing core's pinned digest (`supply.rs`) | re-pinned with its reason | — |

Twelve mutations, every one red.

The drill's tally and knobs measure the paths; its pass lines for the partial shape are unchanged, the hang's owed,
and hold for M10's own Tenants and rate only (§ Measured).

## Measured

On the reference machine (12 cores), 2026-11-19, one run an arm, M10 (`tests/drill.rs`,
`a_kms_hanging_drilled`). The shape: 3 processes of 8 loops, 7 channels, 500 Tenants at 20 messages a second, a
launch of 20,000 messages in the minute before a 600 s outage, unless an arm says otherwise. "Spared" are the
messages of the Tenants the outage does not touch, made while down; "the waits" are the broken Tenants' calls
refused `Waited`, in loop-seconds of the outage's 14,400. The baseline is the library's main with this step's
drill.

**This step, against its pass lines:**

| Arm | Baseline | This step | Pass line |
|---|---|---|---|
| partial 30 %, hanging | spared p50 52–59 s, p99 267–293 s (two runs, before this step's drill) | spared 58 s / 289 s; the waits 93 %, 24 of 24 loops in the stage | spared p99 ≤ 371 s: kept |
| brownout, every call 3 s | spared 20.3 s / 88 s, 241 handled; the waits 14.0 % | spared 24.7 s / 91 s, 301 handled; the waits 13.5 %; one open refused `Late` | nothing paused: kept in both |
| full outage, hanging | — | first stall 36.6 s; the last resume 0.8 s after the return | ≤ 60 s; ≤ 30 s: kept |
| full outage, refusing | — | first stall 7.9 s; the last resume 13.3 s after the return | ≤ 60 s; ≤ 30 s: kept |
| control, no outage | — | spared 90 ms / 3.9 s | — |
| 5,000 Tenants at 240 a second, partial 30 % hanging | spared 296 s / 719 s; the waits 90.3 %; no `FlightsFull` | spared 303 s / 717 s; the waits 90.4 %; no `FlightsFull` | none: a measurement (below) |

Rule of Three, decided:
1. **A Tenant slower than `wait`** is refused `Late` once in the brownout: 13.2 of its 13.5 % of waits are on a
   (process, channel, Tenant) never refused before, first contacts, which this step leaves as they were. The two
   runs' difference is not this step's: the brownout's spared p50 has been 11 s, 18 s, 20 s and 25 s across runs
   of nearby code. So the drill does not exercise the rule; U3 holds it, and no arm has a late Tenant's keys go
   cold again while it is late.
2. **A trusted probe that fails after a full outage**: the last resume 0.8 s after the return.
3. **The scope window's first `wait`**: the first stall 36.6 s, refusing 7.9 s.

**Where the loops go, by the message's history and the opener's** (this step; at 5,000 Tenants the baseline
within a point):

| Shape | The message's first ask | Asked again after a refusal | No refusal of the (process, channel, Tenant) before |
|---|---|---|---|
| 500 Tenants | 29.8 % | 63.4 % | 36.1 % |
| 5,000 Tenants | 85.0 % | 5.6 % | 82.0 % |

At 5,000 Tenants the first contacts alone outrun the loops. Each process builds one stage, and so one opening end,
a channel (`src/sealing.rs`), so a troubled Tenant is first met up to processes × channels times: up to 3 × 7 ×
1,500 = 31,500 first contacts, each a whole `wait` of 2 s, 63,000 loop-seconds against the outage's 14,400; the run
reached 5,911 of them. Step 2, which counts heads in doubt by the row and the memo, does not see a first contact.
Step 5's condition (§ Owed) is refuted by this.

The drill's pass line for the partial hang (spared p99 ≤ 371 s) is the 500-Tenant shape's: at 5,000 Tenants the
baseline's spared p99 is 719 s, longer than the outage, so the line holds only for M10's own Tenants and rate.

**The revisions refuted between the rounds**, the same arms:

| Arm | Rev 1: the grace whenever waits hang | Rev 3: the grace past two waiters | Rev 4: twice the repaid p90 |
|---|---|---|---|
| partial 30 %, hanging | spared 425 ms / 5.3 s; the waits 20 %; `FlightsFull` 444 broken | 542 ms / 21.4 s; 40 %; `FlightsFull` 659 broken, 1 spared | 447 ms / 19.6 s; 24 %; `FlightsFull` 617 broken, 1 spared |
| the same, healthy unwraps 100 ms | — | the waits 38 %; `FlightsFull` 790 broken, 4 spared | spared 565 ms / 20.9 s; `FlightsFull` 754 broken, 5 spared |
| slow shard, the broken answered after 3 s | — | spared 179 ms / 18.1 s; broken 8.8 s / 38.8 s | 157 ms / 21.2 s; broken 7.8 s / 44 s |
| quiet: 20 Tenants at 1 a second, no launch, every unwrap 300 ms | — | 4 spared refused at the grace | 27 spared refused at 252 ms |
| 5,000 Tenants, partial 30 %, hanging | — | spared 129 s / 547 s; `FlightsFull` 15,916 spared, 10,752 broken | not run |

Every revision bought the spared Tenants' p50 back with refusals of healthy opens, and its `FlightsFull` grew as
its waits shortened. Rev 2's "quiet" arm with the launch, on a 4-CPU box, refused 901 healthy opens whose unwraps
took 300 ms, past its grace of 250 ms.

## Owed

The partial hang, in steps. Each step has its executors and a green gate, and goes through the library's regime
where it changes code. The order is round 2's discriminator's.

2. **A count of asks in doubt, in the inbox** (`crates/inbox`: the breaker's core and the dispatch).
   - A head is in doubt when its (channel, Tenant) has a memo entry in this process, or its row was told
     `Unavailable` under a cause its channel's dependency answers for. Where the cause cannot be had, by
     `holds ≥ 1`.
   - At most `doubted` loops a process ask heads in doubt, each for the whole `wait`. One over is held at the
     entry's step, not counted.
   - No bus API and no registration are needed, and the sealing end is covered by the same rule.
   - The walk returns the row's cause if it is taken: a function's signature, for the Database role.
   - The fact it rests on: 62 % of the waits are on messages refused before. That holds for M10 only because its
     handler never refuses: `holds` counts a handler's `Unavailable` too.
3. **The flights' places** (`crates/dek`). A flight in doubt yields its place to a trusted miss while another
   unwrap landed in time within `wait`: the place of a Tenant that failed before first, then the oldest given up.
   A cut is an outcome of its own and fails nobody. It is read after step 2, which frees the loops; at 5,000
   Tenants it decides `FlightsFull`.
4. **The seam**, designed with the open task on registering a stage's dependencies. A head in doubt that is not
   admitted has its flight started or joined without a waiter, and is asked at the landing, in the process that
   landed it. Roles: Software Design, Distributed Systems, Operability, Evolution, Verifiability. It must not
   precede the registration.
5. **First contacts**: a count past the grace, and over which unit the Tenant's evidence is kept. No longer
   conditional: at 5,000 Tenants they hold 82 % of the waits, and steps 2–4 do not see them (§ Measured). Keeping
   the evidence a process, across its channels, divides them by 7 and leaves up to 9,000 loop-seconds of 14,400,
   so the unit alone does not bound them. Whether step 5 goes before step 2 is step 2's round's to decide, with these
   numbers.

The measurements these need, not yet run:
- a third of the broken Tenants returning at 300 s, their delay after it against the baseline's;
- the cross-table of the opener's "never saw" against the message's "first ask" (§ Measured has each alone);
- a simulation in the pure core of the order of the cut.

The sealing end's own partial hang (M11, `generate_dek` hanging for a share of Tenants) is not drillable yet.

## References

ADR-0039 (decision 7, Consequences), ADR-0040, ADR-0041, ADR-0042 (§ Owed 2, M10).

## Change Log

- 2026-11-19: Proposed and built, step 1 of five.
  - Round 1's discriminator found the late landing's trust.
  - Its roles refuted the first rule for the wait.
  - The tally put paths (a) and (b) ahead of (c).
  - Three more rules for the wait were refuted by measurement or by a checker.
  - Round 2 placed the harm's remedy beside the inbox's evidence, in four steps.
  - The reference machine's arms: this step keeps the pass lines and does not move the partial hang; at 5,000
    Tenants first contacts hold 82 % of the waits, which makes step 5 unconditional.
