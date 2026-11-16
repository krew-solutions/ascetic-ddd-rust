# ADR-0040: A message that cannot be handled now stops exactly the messages that share its cause

## Status

Proposed (2026-11-16). Built in the library:
- S0, a bulk readmit's window;
- S1a, the verdicts, the hold and its budget, the causes;
- S1b, expiry and `park_held`;
- S1c, `unpark_all`;
- S1d, the outage model and the traces checked afresh;
- S2, the stage's scope and its sentinel.

S3 is built with ADR-0039's slice 4. S4 and the host's adoption are owed,
each with its executor (§ Executors, § Owed).

Since 2026-11-16 the inbox's slots serve their Tenants in turn (ADR-0041),
and a hold for a stage's outage holds its (slot, Tenant), not its slot.
Decision 9's invariants are broken on this branch until the revision
restores them, and S4 waits for it (§ Owed 6). ADR-0041's slice 2, the
failure's scope, and its deferral move here: this ADR's revision decides
them (§ Change Log).

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
   - `unpark_all(principal, Unparking { channel, cause, tenant, within,
     tries, limit, overtaken })` re-arms parked rows of the failure
     ground:
     - its window is read once (decision 1);
     - `cause` may be `ParkedUnder::Unrecorded`, for rows parked before
       this ADR;
     - `attempts` is set to `GREATEST(attempts, max_attempts − tries)`,
       so an early unpark re-parks at its `tries`th failure (a canary);
     - a window longer than the channel keeps its processed rows is
       refused: an overtaking row forgotten could not be told;
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
   - Forgetting a Tenant is ADR-0036 Part B's (decision 6): its rows are
     deleted in batches, and the Tenant is recorded, so that the intake
     acknowledges its later messages without storing them. A host's
     off-boarding runs it before shredding, so that a shredded backlog
     never drains one Gone at a time. Until Part B is built, that backlog
     parks under `kms.gone`, and `unpark_all` never re-arms it (its cause
     is not `kms.refused`).

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
   - **Broken since 2026-11-16 on this branch** (§ Owed 6): ADR-0041's
     rotation landed with the hold kept per row, so a hold holds a (slot,
     Tenant) and both invariants count Tenants' heads, not slots. Restoring
     them, or replacing them with bounds of the same strength, is the
     revision's first decision; S4 waits for it.

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

S1c, built (`tests/outage.rs`, `src/pg/plan_tests.rs`):
- `an_operator_re_arms_the_rows_parked_for_a_cause_and_a_canary_parks_again`:
  - a Tenant's filter;
  - never a shredded Tenant's rows (`kms.gone`), nor the expired or
    unreadable ground;
  - `Unrecorded` only by name;
  - a canary of one parks again at its first failure, one of two at its
    second;
  - each call is told with its principal.
- `a_bulk_unpark_keeps_to_its_window_and_leaves_an_overtaken_row_unless_asked`:
  - rows parked before the window, or stamped after the call, are left;
  - an overtaken row stays parked and is counted, unless asked for;
  - a window longer than the retention is refused.
- `a_bulk_unpark_goes_batch_after_batch_to_its_limit`: 120 of 250 to its
  limit, then the rest past one batch.
- `a_bulk_unpark_walks_the_parked_index_and_reads_no_other_row`
  (`EXPLAIN ANALYZE`, 20,000 rows, 200 parked on the channel beside 2,000
  on another): no sequential scan, the parked index, and at most the
  channel's 200 rows walked.
- The no-clock check covers the unpark batch too.
- 14 mutations are killed, among them each bound of the window, each
  filter, each ground, the canary reset to zero, the overtaken check, one
  batch only, the limit, the retention refusal, and the channel spelled
  so that the index is passed by. Two guards were redundant: the loop's
  limit check beside the batch's own, and the plan test's single channel
  walked whole. The first was dropped; the test parks on a second
  channel.

S1d, built (`verify/tla`, `make proofs`, `make traces`):
- `InboxOutage.tla`, a module of its own. `Inbox.tla` is instanced by
  four modules, `InboxPg.tla`'s refinement mapping among them, and a hold
  changes none of what they check (dependencies, waits, crashes,
  supersedes). The outage's variables there would have rippled through
  every mapping for nothing.
  - Constants: `TenantOf`, `KeyOf` (a key's rows share a slot),
    `Outages`, `OutageEnds`, `TenantFaults`, `TenantEnds`, `Permanent`,
    `Budget`, `Tries`.
  - Actions: `Defer` (the verdict's scope is a free choice when only a
    Tenant is down), `Outlast` with the evidence, `FailTenant`,
    `FailPermanent`, `Commit` guarded by the stage and the Tenant,
    `ParkHeld`, `UnparkAll`, `Unpark`.
  - Properties:
    - `HoldSpendsNothing`, `CauseIsCurrent`, `HeldIsQueued`,
      `ParkedIsAside`, `CanaryReparks`, `NoStageParks` and `KeyOrder`;
    - `EventuallyProcessed` and `EventuallyOutOfTheWay`.
  - Nine configurations (`verify/tla/README.md`):
    - a hold that counts parks a row for the stage (`NoStageParks`, an
      invariant independent of `Defer`);
    - a mark, and an unpark, that keep the cause break `CauseIsCurrent`;
    - a bulk unpark that resets the attempts breaks `CanaryReparks`;
    - a stage down for good, and a misread Tenant fault on a channel
      serving nothing else, each hold for good, by design.
  - Every action is reached (`-coverage`): `Outlast` in the Tenant's and
    the operator's instances.
  - At most one probe per slot per wait is structural, since only a head
    is asked (`Asked`), and is written there rather than as an invariant.
    Under ADR-0041's rotation a slot has a head per Tenant, so the code no
    longer has this structure; the model still has it (§ Owed 6, 8).
    The k × budget stall is not a configuration: the quiet misread is its
    unbounded case.
- Traces:
  - `Inbox.tla` gains `Permanent` and `FailPermanent`.
    `TraceInbox.tla` reads `MaxAttempts` off the parks of exhausted
    attempts alone, and maps a permanent failure to `FailPermanent`.
    `inbox-permanent.jsonl` had fitted as `MaxAttempts = 1`, for the wrong
    reason. Mapped to `Fail` now, it does not fit.
  - `trace2tla.py` refuses a dispatch closed with an outcome it does not
    map, and a hold.
  - `make traces` records the pg tests' runs afresh into a directory of
    its own, and `check.sh --traces DIR` checks the fresh run of every
    inbox scenario committed under `traces/`. Committing a run declares
    its scenario covered.
    - The outbox's runs, and the bridges' over it, are ADR-0035's: its
      own Owed names the refinement to `Outbox.tla` and its traces.
      `TraceOutbox.tla` follows the outbox before its epochs, which hand
      on a later transaction's row while an earlier one is open, as
      `an_open_transaction_delays_only_its_own_rows` requires. A fresh
      `outbox-visibility` run did so and did not fit. The committed run
      predates the epochs.
    - A committed scenario that no test records any more fails the gate,
      unless `traces/RECORDED_ONCE` names it with its reason.
      `bridge-outbox-set-aside` is named there: since ADR-0038 its test
      writes the first row around the producer, which no recorder sees.
  - Its first run found the tests recording 39 runs that were never
    committed, so never checked:
    - 24 fit, and are committed now: the checked scenarios grow from 29
      to 53;
    - the other 15 are owed below.

S1d, owed:
- the 15 scenarios the tests record and the trace models do not yet
  follow, each classified (the converter or the model extended, or a
  defect of the library found and fixed) and then committed:
  - not converted by `trace2tla.py`:
    - `inbox-causal_refused`, `inbox-causal_structured` and
      `inbox-expiry_batches` name rows written by SQL around the intake;
    - `outbox-backlog`, `outbox-between` and `outbox-rewind_epoch`
      carry null fields the converter cannot write;
  - converted, but not fitting: `inbox-held_by_concurrent`,
    `inbox-window_passed`; and, ADR-0035's with its refinement,
    `outbox-commit_order`, `outbox-lane_start`, `outbox-position`,
    `outbox-replay_horizon`, `outbox-reset`, `outbox-rewind_open`,
    `outbox-uri`;
- a trace refinement of a hold:
  - `defer`, `outlast` and `tenant` steps, and per-row events of a bulk
    unpark;
  - attempts compared under unlimited retries;
  - a `hold` run recorded from a pg test.
  Until then a hold is checked by `InboxOutage.tla` and the pg tests, and a
  recorded hold is refused rather than fitted wrongly.

S2, built (`dek`, `kms`):
- The core's refusal carries its scope (`supply::Scope`):
  - the stage's for a full stage or one with no runtime;
  - otherwise the tenant's when a draw landed in time after its trouble
    began (its failure, or its flight's start), and the stage's when none
    did.
  - `a_refusal_is_the_tenant_s_when_another_landed_in_time_since_its_trouble_began`
    covers a landing after, one before, and a full stage.
- The sentinel (`EnvelopeStage::with_sentinel`, and `Sealing::over_paced`'s
  `sentinel`): after a refusal of the stage's scope on evidence (waited,
  backing off, late), the shell asks the core for a draw of the sentinel's.
  - The core allows one when none is in flight, the last started a `wait`
    ago or more, the sentinel's own failure is not backing off, and a
    probe's place is free.
  - `a_sentinel_is_drawn_for_evidence_at_most_once_per_wait` tests this,
    and the property `no_history_breaks_the_core_s_invariants` gains a
    probe step and its pacing; the histories reach both scopes and the
    probe.
  - `a_sentinel_tells_a_tenant_s_trouble_from_the_kms_s`, through the
    shell: a tenant's refusal is the stage's before the sentinel lands,
    the tenant's after; without a sentinel it stays the stage's and no
    draw is made for evidence.
- `last` is typed (`Last { landed, text }`). `Unavailable::verdict` is the
  bus's verdict on a seal not served:
  - `Permanent::because(kms.refused)` after a policy's refusal;
  - otherwise `bus::Unavailable(kms.unavailable)` over the refusal's scope
    and `retry_after`.
  - A key gone is not among these. Since ADR-0039 decision 8 lost its
    interim rule (slice 4's S4e), the core tells every seal within a
    Gone's memory `Permanent(kms.gone)`. Past the memory a stale note of
    Gone is no verdict: a probe finds out.
- The opening end's verdict is by class:
  - a key gone is `kms.gone`;
  - a refusal is `kms.refused`;
  - the KMS out of reach is `Unavailable` over the stage, held by the
    inbox;
  - the rest are of the moment.
  - Tested by `an_opening_failure_comes_to_its_class_s_verdict` and the
    pg tests of `tests/stage.rs`.
- The stage's own refusals of a message's values are `dek.malformed`.
- A defect the pg tests found, fixed (`kms/src/pg.rs`):
  - The PG KMS answered a shredded tenant's ciphertext
    `KekNotFound { key_version: Some(v) }`. That is `Class::Refused`, the
    message's, so its rows would have parked under `kms.refused`, and
    `unpark_all(kms.refused)` after a policy's fix would have re-armed
    them.
  - A version missing from a tenant with no key left is now
    `key_version: None`, `Class::Gone`.
  - `deleting_the_keks_shreds_what_they_wrapped` holds both: a version
    the tenant never had beside one it has is `Refused`; after the
    deletion, `Gone`.
- Mutations killed:
  - the scope always the stage's, and the tenant's on any landing;
  - the probe unpaced, ignoring the sentinel's back-off, and never
    launched;
  - a refused read as unavailable;
  - the opening's unavailable read as of the moment, and its gone as
    refused;
  - a keyless tenant read as a version.

The host's adoption, owed (with the merge of this branch):
- its handlers' verdict maps `dek::Unavailable::verdict`;
- its sealing passes its sentinel account, and an `Opening` to
  `Sealing::over_paced` (ADR-0039 slice 4), its `keep` within ADR-0071's
  window;
- the end to end: Gone for Tenant A and Refused for Tenant B, through the
  stage and the host's verdict; `unpark_all(kms.refused)` re-arms B's rows
  only.

S3, built with ADR-0039's slice 4: the opening end's verdicts come from its
shell, by evidence as the sealing end's do:
- a key gone is `kms.gone`, once the sentinel's key is there;
- a remembered refused wrapped key is `kms.refused`;
- a refusal is unavailable over its scope, with its `retry_after`.
Tested by `tests/opens.rs` and the pg tests of `tests/stage.rs`.

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
5. **`unpark_all` at the host's scale**: `EXPLAIN (ANALYZE, BUFFERS)` of a
   batch at ≥10M rows, 1M parked across 5k Tenants, a Tenant filter
   matching 100 rows and a cause matching 1%, on the host's drill. The
   shape is held at 20,000 rows today. The parked index is widened only
   if this says so.
4. **The previous release run against the migrated schema**, pinned as a
   renamed dev-dependency (to production, as 2).
6. **The extent of a stage's hold under rotation** — the revision's first
   decision, and S4 (the host's adoption) waits for it. ADR-0041 serves a
   slot's Tenants in turn, and the hold is kept per row, so a hold holds its
   (slot, Tenant) and each Tenant's head is asked again on its own clock.
   Decision 9's invariants count Tenants' heads now, not slots.
   - **What that buys:** the other Tenants of a slot go on beside an outage.
     A Tenant's own rows of other channels in the slot do not: they wait
     behind its head, which is its oldest row of every channel.
   - **What it costs, found by the roles on 2026-11-16** (derived, not
     measured):
     - **A refuted budget parks a head per Tenant.** Past the budget, any
       row of the channel processed within the cap (an operator's
       `resolve`, a flap that lets a few heads through) refutes every head
       asked in the next cap; each spends at each ask and parks at its
       attempts. Before the rotation that was at most a slot's head each.
       So also for a replica cut off from the stage alone: the healthy
       replicas' processing refutes the heads it keeps holding, and they
       park under the stage's cause, `past_budget` never told.
     - **A recovery after an outage past its budget** brings every head due
       within a cap; a flapping stage then parks the backlog's heads.
     - **The walks.** With every head held, a slot walks its Tenants, K at a
       time, in `Passed` dispatches that do not pause, at least once a
       `max_dormancy`, and a head taken ends the round: about 4,000 walk
       transactions a second at 128,000 heads and K = 32, each updating a
       `_slots` row, beside 427 asks a second at a 300 s cap. Every
       dispatch runs `expire_waiting` first, whose spare re-reads, each
       time, every waiter past `max_wait` whose dependency holds.
     - **The log.** Past the budget each held head logs an error at each
       ask: about 427 lines a second at 128,000 heads.
     - The dependency's own calls stay bounded by the stage's pacing
       (ADR-0039 slice 4: a tenant backing off is answered without one).
   - **Measurements** for the revision, on the host's drill:
     - T Tenants held past the budget, the stage answering `Stage` to 20 %
       of asks for 15 minutes after recovery: count `parked_cause =
       'kms.unavailable'`;
     - with every head held, the rate of `Passed`, and `n_tup_upd`,
       `n_tup_hot_upd` and `n_dead_tup` of `<table>_slots` sampled over
       ten minutes with one long transaction open;
     - `EXPLAIN (ANALYZE, BUFFERS)` of `expire_waiting`'s statement with
       10,000 waiters whose dependency holds.
   - **The executor of today's extent:**
     `under_rotation_a_stage_outage_holds_each_tenant_s_head_and_an_operator_parks_them_all`.
7. **A head held for its stage's outage is not in `held_counts`.** The hold
   ends `held_since`, the clock of a hold by the head's own failure
   (ADR-0041 decision 5), so an outage's hold is never counted as a
   back-off aged by a failure it superseded
   (`a_head_held_for_its_stage_is_not_counted_as_its_own_back_off`). The
   host sees the outage's holds by the observer's `held` and
   `past_budget`, and by the log, not by a count from the table; and
   `park_held` tells a count, not a row, so a gauge derived from `held`
   cannot come down when it parks. A `Hold` of the stage's outage, counted
   from the table, is owed with 6. So is a queue predicate in
   `held_counts_sql`, which a release that leaves `held_since` on a row it
   marks or parks would otherwise inflate (to production, as 2).
8. **`InboxOutage.tla` holds a slot whole**, as the inbox did before
   ADR-0041. The rotating extent is not modelled; the trace refinement of a
   hold was already owed (§ Executors). Its group, `inbox-outage`, says so.
9. **`park_held` under rotation** — owed with 6:
   - **Its purpose.** It let a stalled slot go on. Under rotation the slot's
     other Tenants go on without it, and a park lets only the parked head's
     Tenant's later rows be asked; while the outage lasts they hold in turn,
     their clocks fresh, so `past_budget` goes quiet for a budget. Its doc
     says so; whether it keeps a use beyond a misread Tenant is the
     revision's.
   - **Partial, and told as a count.** A slot a dispatcher holds is passed
     by (`SKIP LOCKED`), so with loops busy on a stage that times out a call
     parks little, and 0 does not tell "none held" from "all passed by".
   - **Out of order on recovery.** The later rows of each stream process
     first once the stage is back, so every parked head is overtaken, and
     `unpark_all` leaves it parked unless asked for the overtaken.
   - **Its cost and its window.** One statement walks every Tenant's head
     of every slot, the Tenant filter included, holding the slots it parks
     in. Its rows are read under a snapshot older than its slot locks; a
     row is parked only if it is still in the queue and still held when the
     `UPDATE` reaches it, so a row a dispatcher marked meanwhile is left
     (a guard, beside the `ctid` recheck, for a minor whose TID recheck the
     roles could not vouch for). To measure: `EXPLAIN (ANALYZE, BUFFERS)`
     at 64 slots × 2,000 Tenants all held with ≥ 1M processed rows, with and
     without the Tenant filter; and, on the lowest supported minor, whether
     the `UPDATE`'s recheck can park a row marked between the snapshot and
     the lock.
   - **The slot's sleep after an operator's verb has no executor of its
     own.** A slot whose round found no head due sleeps up to
     `max_dormancy`, and neither `park_held` nor `unpark_all` wakes it; the
     tests wake it by hand. The cap in general has its executor (ADR-0041
     E4: a row made due behind a sleeping slot's back); a sleep to the
     earliest back-off without the cap would pass this ADR's tests, a
     hold's back-off reaching the cap.
10. **Found by the roles in this ADR's own code**, older than the rotation:
    - **A hold left by a row that leaves the queue another way.**
      `park_at` and `set_waiting_unless_processed` clear `held_since` but not
      `unavailable_since` or `last_cause`, and `expire_waiting` neither. A
      held row whose processed dependency was swept is set waiting, expires,
      and is parked on the `expired` ground still "held": its own
      dependents are then spared from expiry for ever, and `unpark_all`,
      which takes the failure ground alone, cannot reach it.
    - **The spare covers a held head only.** A row waiting for a row behind
      a held head (s1@5 behind s1@4) is not spared, and expires, paging,
      during a true outage; it heals at s1@5's mark.
    - **`unpark_all` leaves `last_cause`** where decision 6 says the
      re-armed row loses its cause; `unpark_at` clears it.
    - **A row parked by its attempts re-arms with one try**, whatever
      `tries`: `GREATEST(attempts, max − tries)` keeps it at `max`. Under
      rotation such rows are a Tenant's heads by the thousand (6), and the
      single unpark is the only way to give one its attempts afresh.
    - **No bulk verb for the other grounds**: `expired` rows (a hole that
      will never fill) and rows parked at once (after a release loosens a
      bound) are re-armed one at a time. ADR-0041's `unpark_where` took any
      ground; it was withdrawn for `unpark_all`.

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
- 2026-11-16: Decision 8's `forget_tenant` withdrawn: forgetting a Tenant is
  ADR-0036 Part B's decision 6, a richer design (the intake drops a
  forgotten Tenant's later messages) waiting on the host's off-boarding
  events. One mechanism, one owner.
- 2026-11-16: S1c built: `unpark_all` by channel, cause (or `Unrecorded`),
  Tenant and a window read once, re-arming with a canary of `tries`
  failures, leaving overtaken rows parked unless asked, refusing a window
  longer than the retention. It takes no `because`, as `park_held` does
  not: the library's audit is the principal, and a reason is the host's
  record. The measurement at the host's scale moved to § Owed 5.
- 2026-11-16: S1d built: `InboxOutage.tla` with nine configurations, a
  permanent verdict in `Inbox.tla` so that a recorded permanent park fits
  for its own reason, an unknown outcome refused, and `make traces`. The
  model is a module of its own rather than `Inbox.tla` grown, and the
  trace refinement of a hold is owed (§ Executors).
- 2026-11-16: S2 built: the core's refusals carry their scope, a sentinel
  is drawn for evidence at most once per wait, `last` is typed and
  `Unavailable::verdict` maps it, and the opening end's verdict is by
  class. The PG KMS answered a shredded tenant as a missing version, the
  class a policy's fix would have re-armed; fixed.
- 2026-11-16: S3 built with ADR-0039's slice 4: the opening end's verdicts
  come from its shell by evidence. The host's adoption adds
  `Sealing::over_paced`'s `Opening` to its list.
- 2026-11-16: Rebased under ADR-0041's rotation, this branch carrying both.
  - `park_held` read one head a slot, the slot's oldest. Under rotation a
    slot has a head for each Tenant, and the others were left holding. It
    now reads every Tenant's head, one probe of the Tenants' head index
    each, and clears `held_since` as every park does. Its executor holds
    four Tenants of one slot; the old statement put back fails it alone,
    since this ADR's tests have one Tenant to a slot.
  - Three tests assumed a slot served in arrival order across Tenants, or
    a slot awake after a round that found no head due. Each now says the
    verdict by the message, or wakes the slot whose clock it moved. No
    behaviour changed for them.
  - The release test renders ADR-0041's twin without the three columns
    rather than a list of names; the twin gains them in the migration's
    order.
  - ADR-0041's `make traces` replaces this ADR's: it runs the tests one at
    a time, and a sibling's open transaction no longer stands in a
    recorded snapshot. `check.sh --traces` and `RECORDED_ONCE` are dropped;
    the run that file named had no test recording it, and ADR-0041 had
    already withdrawn it. The outage model's checks are the group
    `inbox-outage`.
  - ADR-0041's `unpark_where` is withdrawn for `unpark_all`.
  - § Owed 6–10: the extent of a hold under rotation, the holds' count,
    the model's extent, `park_held` under rotation, and what the roles found
    in this ADR's own code. ADR-0041's slice 2 and its deferral move here.
    The revision's round takes as its requirements the failure scenarios of
    ADR-0041's two refuted rounds, of the comparison that chose this
    branch, and of § Owed 6–10.
  - **Refuted by three roles before the commit** (distributed systems with
    the database, operability with evolution, verifiability; read-only, on
    the merged tree). What they found against the merge itself, mended:
    - Decision 9's invariants, broken by a hold kept per row under
      rotation, were not said to be: decision 9, S1d's executor and § Owed 6
      now say so, with the costs the roles derived, their measurements, and
      S4 waiting.
    - `unpark_all` refused any window past the channel's retention, so a
      park older than it had no way back in bulk once `unpark_where` was
      withdrawn. Asked for the overtaken too, it needs no overtaker and now
      takes any window.
    - A hold for the stage's outage kept a `held_since` an earlier failure
      had set, so `held_counts` paged on it as a back-off with the age of a
      failure the outage superseded. The hold now ends it.
    - `park_held`'s `UPDATE` parks a row only if it is still in the queue
      and still held; its doc says what it does under rotation.
    - The hold's log lines and docs said it held its slot. They now say it
      holds its Tenant's rows of its slot.
    - The executor of `park_held` parked no Tenant above the lowest with a
      processed row ahead of its head, nor a row carrying `held_since`; it
      now does both.
    - `trace2tla.py` still refused a dispatch that passed (the branch meant
      to map it was unreachable), and took a run of no step as fitting:
      `inbox-identity_past`, committed here, fit with none, and fails the
      rotating model's `TypeOK` on an empty set of messages. A pass is now
      mapped, a run of no step refused, and that run no longer covered.
    - No configuration enabled `FailPermanent` beside the rotation.
      `InboxRotationPermanent.cfg` does.
  - **Mutations, each red:** the recursive arm of `park_held` without the
    queue's predicate; `park_held` keeping `held_since`; the hold keeping
    `held_since`; the retention refused whatever `overtaken`. In the model,
    under `InboxRotationPermanent.cfg`: `FailPermanent` resetting the cursor
    (`BoundedOvertaking`), dropped from fairness (liveness), and `Commit`
    without its `Permanent` guard (`ArrivalOrder`).

