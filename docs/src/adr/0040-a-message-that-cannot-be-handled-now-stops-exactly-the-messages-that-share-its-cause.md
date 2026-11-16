# ADR-0040: A message that cannot be handled now stops exactly the messages that share its cause

## Status

Proposed (2026-11-16). S0 (a bulk readmit's window), S1a (the verdicts,
the hold and its budget, the causes) and S1b (expiry, `park_held`) are
built. S1c–S4 are owed, each with its executor (§ Executors, § Owed).

Decided under the library's gate:
- a blind solver and a discriminator;
- the synthesis refuted by two pairs of roles, each finding folded in
  below or owed:
  - distributed systems with the database;
  - verifiability with the TLA+ models.

This ADR is ADR-0039's Owed 4, the inbox's `Unavailable` marker. It
revises ADR-0005 and ADR-0010 decision 4 when S1 lands. The consumer's
need is its ADR-0091 (architecture-docs): a KMS that fails for longer than
the inbox's attempts.

## Context

**Two verdicts, one of them blind.** A subscriber's failure is either
`Permanent`, which parks at once, or anything else, which spends an
attempt (ADR-0010 d4). A host's schedule is typically `up_to(10)` with a
back-off from 2 s doubling to 300 s, about 810 s in all, and then the row
parks. While a row waits it holds its slot (ADR-0005): `take` reads only
the slot's head (`store.rs:335`), and `locate_head` sees the deferral
(`dispatch.rs:126`).

**What a stage outage does today.** A KMS that is down for an hour fails
every message that needs a new key.
- Each slot's head spends its ten attempts and parks.
- The next head does the same. Parking frees nothing, because the next
  row meets the same outage.
- On recovery, each parked row's stream has gone on without it: ByStream
  keeps a stream in one slot (`partition.rs:28-34`), so the stream's later
  rows run first and the parked row is overtaken (ADR-0036 decision 6).
- The operator gets hundreds of parked rows, unparks them one by one, and
  has to choose between out-of-order handling and a resolve.

**Nothing tells one cause from another.**
- `Permanent` carries a boxed error and nothing else. A Tenant shredded
  (Gone), a policy that refuses a context (Refused) and a malformed
  payload all park the same way.
- Once the cause is mended, the operator cannot re-arm "the rows parked
  for a refused context" as one set.
- Re-arming a shredded Tenant's rows retries a key that will never come
  back.

**One slot holds many Tenants.** A Tenant's streams spread over every
slot: about 300 streams in a backlog reach nearly all 64. A failure that
is one Tenant's (its own key, its own throttle) held at a head stops every
other Tenant behind it in that slot.

**A bulk readmit that may not end** (`surface.rs:424` before S0).
- `readmit_all` recomputed its lower bound against
  `statement_timestamp()` in every batch and had no upper bound.
- A flood still arriving kept every batch full, so the loop did not end.
- It also readmitted rows first seen after the operator's call.

## Decision

**Principle:** a message that cannot be handled now stops exactly the
messages that share its cause. Hold when every row shares it (the stage
is down); spend and move on when only the key's rows share it. The stage
reports facts, and the inbox owns the policy (ADR-0005 d5).

1. **A bulk readmit's window is the call's** (S0). `readmit_all` reads
   `as_of` once, from the database's clock (`statement_timestamp()`), and
   every batch reads `[as_of − window, as_of]` as parameters. A batch's
   statement reads no clock. A row set aside while the call runs is not
   the flood it was called for, and a flood still arriving cannot keep the
   call running. `unpark_all` (decision 8) bounds its window the same way.

2. **Every verdict carries a cause.**
   - `bus::Cause` is a `&'static str` of `[a-z0-9_.]{1,64}`, constructed
     by a `const fn` that panics on an invalid name. A `const` built from
     an invalid name therefore fails to compile.
   - Because a cause is never message text, it is safe as a column and as
     a metric label.
   - `Permanent::because(cause, error)` replaces the anonymous parking.
     `Permanent::new` keeps a default cause, `bus.unexplained`.
   - A failure of the moment carries a cause too, so a row that exhausts
     its attempts parks with one.

3. **`Unavailable { cause, scope, retry_after }` is a third verdict.**
   `scope` is `Stage` (no row of the channel can be handled now) or
   `Tenant` (only this Tenant's rows cannot).
   - The stage decides scope by evidence.
     - A landing for another Tenant since this failure began reads
       `Tenant`.
     - With no fresh landing, the stage probes its sentinel context: the
       host's own account, on the same mount and under the same token. It
       probes at most once per `wait`. If the sentinel lands, the scope is
       `Tenant`; if it fails, `Stage`.
     - Passive landings alone misread three cases: a mount-wide 429, one
       standby down behind a balancer, and a slow KMS whose draws land
       after `wait`. That is why the stage probes.
   - A remembered Gone is not `Unavailable`. It is
     `Permanent::because(kms.gone)`. A refusal by policy is
     `Permanent::because(kms.refused)`, a different cause.

4. **`Stage`: the head holds its slot and spends no attempt.**
   - `next_attempt_at` is deferred to `retry_after`, clamped to the
     host's back-off bounds. `unavailable_since` is set once.
   - Past the host's `Outage::budget` (default 1 h), the budget is an
     evidence check, not a park. The inbox asks whether the channel has
     processed any row within the last `Outage::cap` (default 300 s, the
     longest a held row waits to be asked again), with one probe on
     `__forgettable_idx`. "Since the hold began" would count rows a key
     cache served in an outage's first minutes, and refute every outage
     longer than the cache.
     - If it has, the `Stage` claim is refuted. The row takes the
       `Tenant` path (decision 5) with its cause.
     - If it has not, the row keeps holding, and the observer says "held
       past budget" (a page).
   - A refuted hold keeps its clock, so that each later `Stage` verdict is
     weighed at once and spends.
   - A held row is asked again at the teller's `retry_after`, else after as
     long as it has held, never sooner than the first back-off nor a second
     (`Outage::FLOOR`: each asking is a call to the stage's dependency) and
     never later than `cap`.
   - A true stage outage therefore parks nothing, however long it lasts,
     and its rows keep their order.
   - A misread costs at most one budget per row before the evidence
     refutes it. Operator verbs (decision 8) cut that short.

5. **`Tenant`: the row spends an attempt, never sooner than today.**
   `next_attempt_at = max(backoff(attempts + 1), retry_after)`. A clamp
   to `backoff(1)` would spend ten attempts in two minutes against the
   dek's own back-off (1 s doubling to 30 s), against today's 810 s.
   Exhausted, the row parks with its cause.

6. **A cause lives as long as the state it explains.**
   - Every failure writes `last_cause`, in the same `UPDATE` as today
     (`store.rs:630`), with no index on it, so HOT eligibility is
     unchanged.
   - A park writes `parked_cause`.
   - `unavailable_since` is set by the first `Stage` verdict. It and the
     causes are cleared by:
     - every non-`Stage` verdict, except a refuted hold (decision 4);
     - the mark (a row that failed and then succeeded);
     - an unpark, a resolve and a supersede (a row that failed before a
       host's lower bounds made it unreadable), and `unpark_all`.
   - A second outage never inherits the first one's clock.
   - The wake exits (`store.rs:611`, `:763`) touch only rows that waited
     for a dependency. Those rows never failed, so the exits have nothing
     to clear.

7. **Expiry spares a dependent of a held row.** `expire_waiting` skips a
   row whose dependency is present and held: `unavailable_since` set,
   which a park, a mark and every other exit clear. It finds the dependency by the primary
   key's stream and compares its identity whole, so that no value is
   cast. Without this, the host's 600 s `max_wait` parks every dependent
   behind a held head as expired, and pages. Once the dependency is
   parked, its dependents expire as any other.

8. **The operator acts by cause.** Every verb is audited with its
   principal, like ADR-0036's surface.
   - `unpark_all(principal, because, Unparking { channel, cause, tenant,
     within, tries, limit, overtaken })` re-arms parked rows of the
     failure ground:
     - its window is read once (decision 1);
     - `cause` may be `unrecorded`, for rows parked before this ADR;
     - `attempts` is set to `GREATEST(attempts, max_attempts − tries)`,
       so an early unpark re-parks at its first failure (a canary);
     - overtaken rows stay parked unless `overtaken` is set;
     - each batch is its own transaction, and the observer is told after
       commit.
   - `park_held(principal, channel, cause, tenant)` parks held rows with
     their cause, so their slots proceed. The operator chooses
     out-of-order handling over the stall. Only a slot's head can hold, so
     it reads one row a slot, and passes by a slot a dispatcher holds
     (`FOR UPDATE OF s SKIP LOCKED`): a second call parks that head if it
     still holds. A fairness change that lets a slot serve past its head
     changes this, and is to say so (decision 9).
   - `forget_tenant(principal, tenant)` closes a Tenant's open rows. A
     host's off-boarding calls it before shredding, so a shredded
     backlog never drains one Gone at a time.

9. **Yielding per key is the next owner's** (slice B). While a `Tenant`
   verdict spends attempts at the head, the rows behind it in the slot
   wait (k × 810 s). Deferring the head and only the later rows of its
   key needs `take`/`locate_head`, which the fat-Tenant fairness change
   rewrites. Slice B is built by that change's owner, on the model this
   ADR extends.
   - Both branches keep two invariants: probes per wait ≤ slots, and
     budget outcomes per hour ≤ slots.
   - Per-Tenant heads would otherwise probe the whole backlog during an
     outage, and park it all at once when the budget ran out.

## Consequences

- A KMS outage of any length parks nothing that a recovery would handle.
  It pages "held past budget" after an hour.
- One Tenant's failure stops its slot's other Tenants for at most its
  attempts, as today, until slice B.
- A cause is a column and a label. A bulk re-arm after a mended cause is
  one call, and it never re-arms Gone.
- The schema gains three nullable columns: `last_cause`, `parked_cause`
  and `unavailable_since`. The previous release tolerates them
  (`store.rs:1226-1243`).
- The bus's `Permanent` and `Failure` change shape. Hosts map their errors
  once, in their verdict function.

## Rule of Three

- **Each slot's sentinel probe adds load to a failing KMS.** It is at most
  one per `wait` per stage, about 0.4 per second per replica at
  `wait = 2.5 s`, against the draws it replaces.
- **A quiet channel where one Tenant's rows hold every slot** never
  refutes `Stage` from inbox evidence. The sentinel probe answers
  `Tenant` first: its key lands. If the sentinel fails too, every row
  shares the cause, and holding is right.
- **The evidence probe could see rows the outage did not touch**, for
  example rows that need no key. That refutation is correct: those rows
  were handled, so not every row shares the cause.
- **A migration under `ACCESS EXCLUSIVE` waits for in-flight dispatch
  transactions**, which include KMS waits. A deploy during a KMS incident
  may miss the 500 ms lock timeout and fail its start. Owed: a
  measurement (§ Owed 3).

## Alternatives Considered

- **Hold every `Unavailable`** (the solver). This is right for a stage
  outage, and it makes one Tenant's throttle stall everyone in its slots.
  Scope keeps the hold and bounds the harm.
- **Yield every `Unavailable`** (our first position). This is right for
  one Tenant. For a stage outage it is a retry storm over the backlog
  that frees nothing. Scope keeps the yield for slice B.
- **A budget that parks.** Under a true outage it parks one head per slot
  per hour, frees nothing, and the parked rows are overtaken on recovery.
  An evidence check keeps the bound on a misread without that harm.
- **Scope from passive landings only.** This misreads partial outages and
  slow KMSes (decision 3).

## Executors

S0, built:
- `a_flood_still_arriving_neither_keeps_a_bulk_readmit_running_nor_is_readmitted`
  (`crates/inbox/tests/surface.rs`).
  - Each readmission synchronously sets one more row aside, so after the
    first batch exactly 100 later rows exist.
  - The call readmits the 150 rows set aside before it and ends. The flood
    stays set aside.
  - A missing upper bound is killed: the call never ends, and the test
    times out at 60 s. So is `as_of` re-read per batch.
- `a_bulk_readmit_s_batch_reads_no_clock` (unit): a clock in the batch's
  statement is killed.

S1a, built (bus and inbox; `crates/inbox/tests/outage.rs`, each test
killing the mutations named):
- `a_stage_outage_holds_its_slot_and_spends_no_attempt`: a hold that
  spends an attempt; a hold whose clock restarts; a mark that keeps the
  cause.
- `a_tenant_s_unavailability_spends_an_attempt_never_sooner_than_its_back_off`
  (`retry_after` 3 s against a 10 s back-off, then 45 s against 20 s): the
  teller's while taken over the back-off; an exhausted park without its
  cause.
- `a_hold_is_asked_again_between_its_floor_and_its_cap`: no floor.
- `a_hold_past_its_budget_holds_on_unrefuted_and_spends_once_the_channel_refutes_it`:
  never refuted; a refuted hold that loses its clock.
- `two_outages_each_under_the_budget_never_park`: another verdict that
  keeps the hold.
- `a_cause_is_kept_while_its_state_lasts_and_cleared_at_every_exit`: an
  unpark or a resolve that keeps the causes.
- `a_row_parked_at_once_is_superseded_by_the_next_arrival_at_its_identity`
  (`tests/pg.rs`) now also holds a supersede to clearing them.
- `a_verdict_is_read_off_the_error_s_top`: a bus verdict wrapped in
  another error is of the moment.
- `the_release_before_the_causes_admits_the_table_they_were_added_to`
  (`src/pg/release_tests.rs`): the real migration, against the previous
  release's own list of columns; a binding column is killed.
- `cause!`'s `compile_fail,E0080` doctest: a check that admits any name is
  killed. Pinning the error code mattered: without it the doctest passed
  for a format string's error.
- A trace refuses a hold (`trace2tla.py`) until S1d models it.

S1b, built (`tests/outage.rs`):
- `a_row_waiting_for_a_held_dependency_does_not_expire`: a dependent 20
  minutes past `max_wait` stays waiting while its dependency holds, and
  expires once that is parked.
- `an_operator_parks_the_rows_held_for_a_cause_and_their_slots_go_on`:
  - a Tenant's failure, another cause, another Tenant and another channel
    each park nothing;
  - the right one parks the head under its cause, and the row behind it
    goes on;
  - the call is told with its principal.

S1c–S1d, owed (bus and inbox):
- pg tests:
  - `unpark_all`:
    - filters by cause, Tenant and `unrecorded`;
    - its canary includes a row parked without a hold and fails a row
      twice;
    - its window is read once;
    - overtaken rows stay;
    - the observer is told after commit;
  - `forget_tenant`.
- Plan: `EXPLAIN (ANALYZE, BUFFERS)` of an `unpark_all` batch at ≥10M rows,
  1M parked across 5k Tenants, a Tenant filter matching 100 rows and a
  cause matching 1%. The batch uses `__parked_idx`, with buffers in
  proportion to the channel's parked rows. The index is widened only if
  this says so.
- Migration: a test reads the migration's real DDL. Every new column is
  nullable with no default that binds an insert, and every object is
  `{table}__`-prefixed.
- Cause: a `compile_fail` doctest for an invalid `const` cause.
- `Inbox.tla`:
  - Constants:
    - `KeyOf` and `TenantOf`, with `part = slotOf ∘ KeyOf`;
    - `PermanentSet`;
    - `Budget`;
    - `OutageEnds` and `OutageLen`;
    - `Tries`.
  - Variables: `stageDown`, `tenantDown`, `cause`, `held`, a ghost
    `spent`, and `canary`.
  - Actions:
    - `Defer`, where the scope is a free choice when only a Tenant is
      down, since misclassification is modelled;
    - `FailTenant` and `FailPermanent`;
    - `Outlast`, with the evidence check;
    - `UnparkAll` and `ParkHeld`;
    - `Commit`, guarded by the stage and the Tenant being up.
  - Invariants:
    - `HoldSpendsNothing`;
    - `ParkedIsAside` by cause;
    - `CauseIsCurrent`;
    - `CanaryReparks`;
    - `KeyOrder` beside `ArrivalOrder`;
    - probes per wait ≤ slots.
  - Cfgs:
    - `InboxOutage` holds;
    - `Counts` violates `HoldSpendsNothing`;
    - `NoBudget` violates `EventuallyAside` with `OutageEnds = FALSE`;
    - `ShortOutageProcesses` holds, and `Counts` violates it;
    - one must-fail cfg per clearing exit, each violating
      `CauseIsCurrent`;
    - `SlotStallWithinBudget` must violate, which documents the
      k × budget stall.
- Traces:
  - `trace2tla.py` refuses an unknown outcome;
  - `MaxAttempts` is read from exhausted parks only;
  - attempts are always compared;
  - `defer`, `outlast`, `permanent` and `tenant` steps;
  - per-row `unparked` events;
  - `hold` traces recorded from pg tests.
- A new gate, `make traces`, re-records the pg traces into a temporary
  directory and checks those. Today `make proofs` checks only the
  committed traces, which still fit after the code regresses.

S2, owed (dek):
- the scope's evidence, with core tests:
  - another Tenant's landing reads `Tenant`;
  - the sentinel lands → `Tenant`;
  - the sentinel fails → `Stage`;
  - at most one probe per `wait`;
- a typed `last`;
- the opening verdict by class;
- end to end through the stage and a host's verdict: Gone for Tenant A and
  Refused for Tenant B; `unpark_all(kms.refused)` re-arms B's rows only.

S3: with ADR-0039's slice 4 (the opening side).

S4: slice B (decision 9), with the fat-Tenant change's owner.

## Owed

1. S1–S4, each with the executors above.
2. **Mixed releases** (to production, by the consumer's rule that
   compatibility waits for launch):
   1. A release older than S1 that parks a row holding a stale
      `last_cause` leaves `parked_cause` untouched.
   2. `unpark_all` by that cause would then re-arm it.
   3. The harm: the row is retried and re-parks at Gone.
   - The fix: stamp `cause_parked_at = parked_at` and filter on equality.
   - The executor: a frozen copy of the old statement text run against the
     new schema.
3. **The migration's lock under load**: the age of dispatch transactions
   under the consumer's S6 drill, against the 500 ms lock timeout
   (`migration/lib.rs:177-178`).
4. **The previous release run against the migrated schema**, pinned as a
   renamed dev-dependency (to production, as 2).

## References

- ADR-0005, ADR-0010 (revised by S1), ADR-0036 (the operator's surface,
  overtaken rows), ADR-0039 (the stage's evidence, Owed 4).
- `verify/tla/Inbox.tla`, `TraceInbox.tla`, `trace2tla.py`.

## Change Log

- 2026-11-16: Proposed. S0 built: `readmit_all`'s window read once.
- 2026-11-16: S1a built. Decision 4's evidence is a row processed within
  the last `cap`, not since the hold began: a key cache serving an
  outage's first minutes would otherwise refute every outage longer than
  the cache. A refuted hold keeps its clock; the hold's floor is named.
- 2026-11-16: S1b built: expiry spares a held dependency's dependents, and
  `park_held` parks held heads by channel, cause and Tenant. Decision 8's
  `park_held` takes no `because`: the cause it parks under is the row's
  own. Its mutations found two guards that masked each other. A refuted
  hold's exhausted park kept its clock. It now ends the hold, so that
  "held" alone means unparked, and each guard is now the only one of its
  kind.
