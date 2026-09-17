# ADR-0009: A wait is a transaction of its own, serialized with the dependency's mark by a lock on its identity

## Status

Accepted (2026-09-16), in the shape V2, `Outcome::SetAside`, of the decision
matrix below. Reached through a discriminator pass: five designs from
different premises, eleven two-session experiments in PostgreSQL
(`verify/pg/lost-wake/`), checked objections, synthesis. Code references
are to the commit that accepted this ADR, before the change; the two tests
named under Consequences fail at that commit on purpose.

## Context

ADR-0006 made a message ahead of its dependency wait out of the queue, woken
by the dependency's mark in the same statement. The implementation decides
and records that wait in two statements of a transaction that then goes on:
the check is a plain `SELECT` (`crates/inbox/src/pg/store.rs:299`), the set
an `UPDATE` (`store.rs:243`), both from the walk (`pg/dispatch.rs:294-298`),
which takes the next head of the slot and runs its subscriber
(`dispatch.rs:100`) before the transaction opened at `dispatch.rs:61`
commits. Transactions are plain `BEGIN` (`crates/session/src/pg/session.rs:104`),
READ COMMITTED. Under READ COMMITTED an `UPDATE` "will only find target rows
that were committed as of the command start time" (the manual's section
"Read Committed Isolation Level", § 13.2.1 of chapter 13, Concurrency
Control, unchanged from PostgreSQL 14, where the experiments ran, to 18:
https://www.postgresql.org/docs/current/transaction-iso.html). This is the
definition of the isolation level, not a defect of a version.
So a dispatcher B marking the dependency `d` in another slot runs its
`woken` update (`store.rs:337`) against a version of `m` in which
`waiting_for` is still null, wakes nothing, and commits; A commits its wait
afterwards; `m` waits for a processed dependency for ever, or, with
`max_wait`, is parked with "dependency never arrived", which is false. The
test `a_wait_set_while_its_dependency_is_marked_elsewhere_is_woken` shows it
(experiment 1 shows it in SQL alone). Two paths: `d` present and in flight,
and `d` not yet arrived at A's check. Preconditions: two or more slots and a
dependency across slots; with one slot the dispatchers serialize on it. The
model does not see it: `WaitIn` in `verify/tla/Inbox.tla` checks and sets in
one step.

Two facts bound every fix. **F1**: blocking inside a statement does not
refresh its snapshot — an advisory lock taken in a CTE of the mark statement
blocked until A committed, and the `woken` update of the same statement still
missed `m`; the next statement saw it (experiment 2a). **F2**: the one read
that returns the latest committed version is a row lock on the row itself,
`FOR SHARE` (experiment 7a), and a qualification on the changing column
defeats it (7b); a row that does not exist yet cannot be locked.

Constraints: no lost wake on either path; a deadlock is a database error and
stops every loop of `run` (`dispatch.rs:218-221`), so lock cycles must be
impossible by argument; PgBouncer in transaction pooling, so nothing
session-scoped; slots stay independent; the happy path counts round trips
(ADR-0005: ~75 µs per statement); the model must describe what happens.

## Decision

1. **A wait is a transaction of its own.** A dispatch that finds its head
   `m` ahead of an unprocessed dependency `d` takes
   `pg_advisory_xact_lock(hashtext(<table>), hashtext(<identity of d>))`,
   then in one fresh statement re-checks and sets:
   `UPDATE ... SET waiting_for = d WHERE <m> AND NOT EXISTS (<d processed>)
   RETURNING 1`, and commits. An empty `RETURNING` means `d` was marked in
   between: nothing to wait for, take again. One advisory lock per
   transaction, taken last; after it, nothing else is acquired.
2. **The mark takes the same lock on its own identity**, as a statement of
   its own before the mark statement (F1 forbids folding it in), only when
   the table has more than one slot; `resolve` takes it always and runs in a
   transaction, since it is the one marker that runs beside a slot holder
   even with one slot. The lock orders {re-check, set, commit} against
   {mark, commit} totally: whoever comes second reads, in a fresh statement,
   what the first committed. Experiments 8a (dependency not yet arrived —
   the marker blocked 1.19 s and woke `m`) and 8b (marker first — the
   re-check saw the mark).
3. **The expiry sweep leaves the dispatch transaction** and runs as a
   statement of its own. Otherwise a cycle exists: an expirer holds a row
   that waited for `d` while its own head needs the lock of `d`, and the
   marker of `d` waits in `woken` for that row.
4. **Deadlock-freedom, by argument.** The slot row is `SKIP LOCKED` and
   never waits. A queue head is locked by nobody but its slot's holder:
   `publish` takes no lock on an existing row (`ON CONFLICT DO NOTHING`
   returned at once against a row held `FOR UPDATE`, experiment 3a);
   expire, resolve and unpark touch waiting or parked rows only. The
   `woken` update can wait only for an expirer, which holds expired rows
   and waits for nothing (`FOR UPDATE SKIP LOCKED`, `store.rs:286`). Every
   transaction holds at most one advisory lock and acquires only woken-row
   locks after it. No cycle.

Who waits for whom: a wait for the marker of its `d` between the marker's
lock and its commit; a marker for a wait between its lock and its commit —
a re-check, one update, `COMMIT`; an operator's `resolve` for as long as the
operator's transaction. Unrelated slots share no lock. Transaction-level
advisory locks end with the transaction (the manual's "Advisory Locks",
§ 13.3.5, https://www.postgresql.org/docs/current/explicit-locking.html);
PgBouncer's transaction pooling forbids only session-level ones.

## The shape of `dispatch`: two variants and the decision matrix

**V1 — the signature stays.** `dispatch` runs wait-transactions in a loop
until a head is processable or nothing is due, then the processing
transaction; `Outcome` is unchanged.

**V2 — `Outcome::SetAside`.** One call, one transaction: a dispatch that
sets its head aside commits and returns the new outcome; the next call
takes the slot's next head. `run` treats it as work done: no poll sleep.

| Criterion | V1: loop inside | V2: `Outcome::SetAside` |
|---|---|---|
| Transactions per call | several: waits, then the processing | one, as the docs say today |
| What an `Err` leaves behind | waits already committed, unseen by the caller | only what the outcomes already reported |
| Outcome as data (FP) | a set-aside is hidden inside the call | a set-aside is an outcome |
| Public API | unchanged | one new variant; breaking on a 0.1.0 crate nobody depends on |
| `run` loop | unchanged | one more arm that does not sleep |
| Trace and generator | unchanged | `waiting` opens a dispatch, `dispatched set_aside` closes it |
| Tests | unchanged | three assertions change, `Nothing` → `SetAside` |
| Model | `Wait` and `Fetch` are separate steps already: both fit | the same; the trace shows one step per call |
| Rust | a loop of transactions inside one call; `head_of` stays for the walk | straight line; `head_of` and `Step` go |
| Round trips per set-aside | take, check, lock, update, commit, take | the same |

Decision: **V2**. It keeps "one transaction per call" true, it makes the
set-aside visible where every other thing a dispatch does is visible, and
the breaking change costs nothing today. V1 would be the choice if the
signature could not move.

## Consequences

- One statement more per mark on tables with more than one slot (~75 µs,
  ADR-0005's figure; experiment 9 measured the lock statement at the cost of
  `SELECT 1`). Pipelining lock and mark on one round trip is possible with
  tokio-postgres at the price of bypassing the session observer; measure
  first.
- Advisory locks are a database-global namespace; a foreign application
  using the same key stalls a wait or a mark, never corrupts. Bounded by the
  two-key form with the table's name as the class, documented in the README.
- One SQL expression for the identity hash, shared by wait, mark and
  resolve; two expressions drifting apart would reopen the race silently.
- The `waiting_since` stamp is now the commit of the wait, so `max_wait`
  counts from when the wait became visible, not from a call that may still
  be running a subscriber.
- Model: `WaitIn` and `Commit` are now literally the implementation. A
  constant `AtomicWait`, `TRUE` everywhere but in `InboxSplitWait.cfg`,
  where a wait is settled a step after its check and a mark in between
  cannot see it: TLC finds `WaitingIsAside` violated, a message waiting
  for a processed dependency. The model records why the lock exists, as
  `InboxNoWaiting.cfg` records why waiting exists; with `TRUE` the state
  space is unchanged.
- `verify/tla/InboxPg.tla`, the statements against PostgreSQL under READ
  COMMITTED — snapshots per statement, versions stamped by commits, the
  slot's lock, the advisory lock — refines `Inbox.tla` by a mapping TLC
  checks, so the protocol model's two assumptions, one holder per slot and
  an atomic wait, are earned rather than posited. Its switches replay the
  designs this ADR weighed: without the lock, the lost wake; with the head
  read in the take's statement, the stale head of ADR-0008; walking on
  after a set-aside with the lock held, the lock cycle — each a required
  violation in `check.sh`.
- Tests, all in `crates/inbox/tests/pg.rs`:
  `a_wait_set_while_its_dependency_arrives_and_is_marked_elsewhere_is_woken`
  (dependency not yet arrived) and
  `a_wait_set_while_its_dependency_is_being_marked_elsewhere_is_woken`
  (dependency in flight; `served_at` biased so the dependent's slot is taken
  first) failed at the commit that accepted this ADR and pass now;
  `two_slots_are_worked_at_once` shows the slots stay independent; the wait
  visible from another session once the call returned is the first
  assertion of `a_dependency_that_never_arrives_parks_the_message_after_max_wait`;
  `loops_over_slots_with_dependencies_across_them_lose_no_wake` is the
  stress run — three loops over four slots, 120 messages with random
  acyclic dependencies across slots, published in random order while the
  loops run, small random sleeps, `max_wait`, one poison message parked and
  resolved by hand — which must end in `Ok` with everything processed and
  no row waiting for a processed dependency: the lost wake as one query.
- The stress run found the take of ADR-0008 wrong: one statement locks the
  slot and reads its head, but its snapshot predates the lock, and a head
  the previous holder processed in between fails the re-check `FOR UPDATE`
  makes on the latest version — the slot came back without a head, a
  database error that stopped the loops. The head is now read in a
  statement of its own after the lock, one round trip more per dispatch;
  ADR-0008's point 2 is amended so.
- A receipt names the slot the row landed in, so the trace model places
  every arrived message exactly instead of guessing the untouched ones.
  Runs with several dispatchers at once are not trace-checked any more: the
  order their events are logged in is not the order of their commits, and
  the rules that reordered the log by snapshots and transaction ids kept
  growing and missing cases while finding no defect of the library; such
  runs assert their outcome and the table's state instead
  (`verify/tla/README.md`).

## Alternatives rejected

| Design | Both paths | No deadlock | Slots independent | Happy path | Model exact |
|---|---|---|---|---|---|
| SERIALIZABLE + retry on 40001 | yes | yes | **no** | retries | yes |
| Lock the dependency inside the long transaction | advisory: yes | **no** | **no** | 0 | yes |
| **This decision** | yes | yes | yes | +1 statement, slots > 1 | yes |
| The waiter re-checks after its own commit | yes | yes | yes | 0 | **no** |
| The marker finds dependents by their dependency list | yes | **no** | **no** | GIN index | yes |

**SERIALIZABLE.** Two takes of *different* slots both read `<table>_slots`
and both write `served_at`; the second commit fails as a pivot (experiment
5a), so every concurrent pair of dispatchers conflicts and ADR-0008's
parallelism is gone. `FOR UPDATE SKIP LOCKED` on a slot row updated after
the snapshot fails with "concurrent update" (5b). The subscriber inherits
the level and can abort the dispatch with its own reads.

**A lock on the dependency held through the dispatcher's transaction** —
the first proposal. The walk sets several heads aside in one transaction,
so a checker accumulates locks; two checkers each holding one and wanting
the other's deadlock without any cycle in the dependencies (experiment 6:
"deadlock detected"). A row lock instead of the advisory one cannot cover a
dependency not yet arrived, and even `FOR KEY SHARE` waits for the whole
processing transaction of the dependency (7c).

**The waiter wakes itself after committing**: re-check `d` with `FOR SHARE`
outside any transaction, un-wait on success. Zero cost and no marker
change, but the wait is no longer atomic: `WaitingIsAside` holds only
eventually, the trace check of exact `woken` sets fails for a mark whose
snapshot predates the wait's commit, `max_wait` can park a row whose
dependency was processed because `waiting_since` is stamped before a
subscriber runs, and the re-check blocks on the dependency's processing
transaction for its whole length (7c).

**The marker finds dependents by their immutable dependency list**,
`metadata->'causal_dependencies' @> [d]`, and lets EvalPlanQual re-check the
new version. It blocks for the checker's whole transaction on every mark
with an unfinished dependent (experiment 11), rewrites every unprocessed
dependent on every mark, needs a GIN index the DDL lacks, and deadlocks with
two dependents of `d` in one slot separated by a waiting head.
