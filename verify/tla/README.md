# Specifications

TLA+ models of the protocols the crates implement, checked with TLC. A model
describes the participants and their steps — transactions, dispatchers,
workers, crashes, retries — and TLC explores every interleaving on a small
instance, looking for a state that breaks a property. What is checked is the
*protocol*, not the Rust code: the gap between the two is closed by tests, and
by trace validation, where recorded test runs are checked against the model
(see the section at the end).

```bash
./verify/tla/check.sh          # needs Java, tla2tools.jar and Python 3, see the script
./verify/tla/check.sh --groups # the groups the checks fall into, one a line
./verify/tla/check.sh inbox-pg traces   # those groups alone
```

`make proofs` runs the script, `make proofs PROOFS='<group> ...'` some groups
of it. The whole took seven hours on a four-core machine, longer than a
hosted CI job may run (six), so `.github/workflows/proofs.yml` runs each
group as a job of its own; the longest took two hours there. The groups
follow the models, except the rotation beside a dependency
(`inbox-pg-rotation-deps`), a group of its own for its length alone.

## Outbox — `Outbox.tla`

Producers publish inside transactions; a dispatcher per (group, slot)
fetches committed messages of its slot in `(transaction_id, position)`
order, hands each to a subscriber, acknowledges the last of the batch; any of
them crashes at any point, and a subscriber may fail.

PostgreSQL is reduced to five facts: a transaction id is assigned at the first
write and increases; commits happen in any order; a row is visible once its
transaction committed; `pg_snapshot_xmin` is the smallest open id, or the next
one; the position row is locked per dispatcher. The header of the module says
why "visible iff committed" is a sound stand-in for a snapshot under the
visibility rule.

Properties, on three messages, two URIs, one group, two slots, batches of
two and one crash:

| property | kind | meaning |
| --- | --- | --- |
| `DeliveredCommitted` | invariant | a subscriber only ever receives committed messages |
| `NoPassedOver` | invariant | a position never passes a live message of its partition that was not received |
| `OrderPerUri` | invariant | messages of one URI reach a group in `(xid, pos)` order, counting first receipts |
| `EventuallyDelivered` | liveness | every committed message is received by every group |

`OutboxNoRule.cfg` runs the same model with the visibility rule switched off,
fetching by transaction id alone. TLC finds `NoPassedOver` violated in a few
steps: a transaction takes id 1 and stays open, a later one takes id 2 and
commits, the dispatcher delivers and acknowledges 2, then 1 commits behind the
position and is never fetched. That trace is the reason the rule exists
(`crates/outbox/src/pg/store.rs`), and `check.sh` requires it to appear.

The fairness assumption spells out a known property: `EventuallyDelivered`
needs every transaction to end. One transaction left open stalls every later
message, of every group.

Not modelled: the URI prefix of a selection, several messages in one
transaction, and the arithmetic of `hashtext` — the assignment of URIs to
slots is an arbitrary function, so the sign bug fixed in the Rust port is out
of scope here and covered by a test instead. Who serves a slot is not
modelled either: a dispatcher is whoever holds the lock on the slot's
position, and has no other identity (ADR-0007).

`OutboxRehash.cfg` lets the assignment change under the positions, `Rehash`:
what a deployment did when it changed the number of workers with `hash % n`
at start-up. TLC finds `NoPassedOver` violated on two messages: a URI moves to
a slot whose position is already past its message. That is the deployment
level loss made visible, and the reason a row carries its slot for good;
`check.sh` requires the violation.

## Outbox, statement by statement — `OutboxPg.tla`

`Outbox.tla` is the relay as implemented today: released below `xmin`, in
`(transaction_id, position)` order. ADR-0035 replaces both, and
`OutboxPg.tla` is its relay one level down: the statements of a
dispatcher's transaction against PostgreSQL under READ COMMITTED.

- **Snapshots** are `xmin:xmax:xip` as `GetSnapshotData` computes them, and
  visibility is `pg_visible_in_snapshot`'s arithmetic. An epoch's rows —
  visible in the epoch, not in `delivered` — are found as the implementation
  finds them, in-progress lists included.
- **A statement's snapshot is taken before the lock it asks for,** and
  `FOR UPDATE` returns the position row's latest committed version.
- **A transaction takes its id at its first write,** or earlier, when it
  writes something else first (the inbox's take). A stream's version `v` is
  written once `v - 1` has committed (ADR-0035 decision 7). One transaction
  may hold an id for ever and write nothing here: one of another database.
- **Selections:** one of everything, raced by two dispatchers, or two lanes
  (ADR-0033), each with its own position row.
- **The migration of decision 10:** before it, relays of ADR-0007 run on the
  old pair; it converts every position at once; after it, old relays may go
  on beside new ones until they retire, on the old pair the new ones keep up.
- **The position sequence** hands each session a block of `Cache` values.
- **ADR-0033's ring:** rows live in `K` generations by their transaction's
  window of `R` ids. A reclaimer works one generation at a time, its checks
  each a step — past, closed, passed — then the partition's lock, which
  waits for every open writer of a row in it, the recheck of "passed", and
  `TRUNCATE`. It records `gone_below` and `intact_from`. A group may be
  created later, from the current snapshot kept below `intact_from`; a
  rewind below it is refused. An epoch names the generations its rows can
  lie in, from its two snapshots, and records those it found rows in.

Left out:
- the probe of decision 2, which decides whether a look happens, not what
  it does;
- the refinement to `Outbox.tla`, which waits for that module to be restated
  over versions.

`MCOutboxPg.tla` fixes the instance: two versions of one stream, to the
inbox, written by two transactions; one row of another stream, to the
broker, by a third that may roll back; a fourth transaction of another
database. For the migration, one transaction writes two rows, so that an old
relay can acknowledge it halfway, and one new relay runs beside one old.

| property | kind | meaning |
| --- | --- | --- |
| `NoPassedOver` | invariant | a live row the position has passed — visible in `delivered`, or an epoch's row at or below its offset — was received |
| `RewindReplays` | invariant | after a rewind, every row the position passes again was received again |
| `VersionOrder` | invariant | a stream's versions are first received in their order |
| `NoRedeliveryWithoutFailure` | invariant | a row is received twice only after a rollback or a rewind |
| `EpochCoversDelivered` | invariant | an epoch in flight sees everything delivered |
| `DeliveredMonotone` | action | `delivered` only grows, but at a rewind |
| `NoPositionBelowGone` | invariant | a row emptied with its generation was passed by every group's position |
| `IntactFromSound` | invariant | no row at or above `intact_from` was emptied |
| `GoneBelowSound` | invariant | no row is left below `gone_below` |
| `OldPairSound` | invariant | during a migration, the old pair, which old relays read, passes no row unreceived |
| `NoHeadOfLineBlocking` | liveness | a committed row is received whatever transaction stays open for ever, unless its own lane's destination is down |

Must hold:
- `OutboxPg.cfg`: as decided, two dispatchers racing, one rollback. About
  6.6 million states, two and a half minutes.
- `OutboxPgRewind.cfg`: rewinds as decided. About 2.5 million states, under
  a minute.
- `OutboxPgLive.cfg`: a transaction of another database open for ever;
  `NoHeadOfLineBlocking` holds. Four minutes: TLC checks liveness slowly.
- `OutboxPgLanes.cfg`: two lanes, the broker down for good; the inbox's lane
  still flows. Without early ids or rollbacks, which the others hold: about
  48 000 states, a minute and a half.
- `OutboxPgMigrate.cfg`: the migration as decided, an old relay going on
  beside a new one until it retires; nothing lost, by either position.
- `OutboxPgMigrateExact.cfg`: the old relay stopping at the migration;
  nothing handed on twice, and the order holds across it.
- `OutboxPgRing.cfg`, `OutboxPgRingRewind.cfg`, `OutboxPgRingLate.cfg`: the
  ring as decided, two generations of one id each — alone, with a rewind,
  with a group created later. About 136 000, 863 000 and 1.35 million
  states; under a minute each.
- `OutboxPgRingNoClosed.cfg`, `OutboxPgRingPassedFirst.cfg`: the ring without
  "closed", or with "passed" read before it. Both hold: the recheck of
  "passed" under the partition's lock keeps the rows, and the checks before
  the lock only spare a lock queued in vain. ADR-0033's first reading had
  them as safety checks.

Must fail, each with the property it breaks (`check.sh` greps it):

| configuration | the design | breaks |
| --- | --- | --- |
| `OutboxPgXmin` | ADR-0007's rule | `VersionOrder` |
| `OutboxPgXminOpen` | the same, a transaction open for ever | `NoHeadOfLineBlocking` |
| `OutboxPgOneStatement` | the epoch read in the lock's statement | `NoPassedOver` |
| `OutboxPgByXid` | an epoch handed on by transaction id | `VersionOrder` |
| `OutboxPgNoXip` | `delivered`'s in-progress list ignored | `NoPassedOver` |
| `OutboxPgRange` | membership by a range of ids alone | `NoPassedOver` |
| `OutboxPgUnfiltered` | a batch not kept to the epoch's rows | `NoRedeliveryWithoutFailure` |
| `OutboxPgUnbounded` | nor to its end | `VersionOrder` |
| `OutboxPgDrainNow` | a drain storing the current snapshot | `NoPassedOver` |
| `OutboxPgOffsetCarried` | a new epoch keeping the last one's offset | `NoPassedOver` |
| `OutboxPgNaiveRewind` | a rewind to `'F:F:'` | `NoPassedOver` |
| `OutboxPgRewindKeepsEpoch` | a rewind leaving the epoch in flight | `RewindReplays` |
| `OutboxPgDrawEarly` | a row written before its predecessor committed | `VersionOrder` |
| `OutboxPgMixedDown` | one selection, the broker down | `NoHeadOfLineBlocking` |
| `OutboxPgCache2` | the sequence's cache of two, two sessions | `VersionOrder` |
| `OutboxPgMigrateDelivered` | positions converted to `'L:L:'` alone | `NoRedeliveryWithoutFailure` |
| `OutboxPgMigrateTooFar` | positions converted to `'L+1:L+1:'` alone | `NoPassedOver` |
| `OutboxPgMigrateShared` | the epoch's offset in the old column, which an old relay moves | `NoPassedOver` |
| `OutboxPgRingNoRecheck` | the ring truncating without the recheck under the lock | `NoPositionBelowGone` |
| `OutboxPgRingPassedFirstNoRecheck` | "passed" read first, and no recheck | `NoPositionBelowGone` |
| `OutboxPgRingNamesEarly` | an epoch's generations named before its snapshot | `NoPassedOver` |
| `OutboxPgRingOneBound` | one bound for both meanings | `IntactFromSound` |
| `OutboxPgRingNewGroupNaive` | a group created at `'F:F:'`, F = `intact_from` | `NoPassedOver` |
| `OutboxPgRingRewindBelow` | a rewind below `intact_from` | `NoPositionBelowGone` |
| `OutboxPgRingNoRecord` | a converted epoch with no generations recorded | `NoPassedOver` |

Two of the traces, as TLC prints them:

- **`OutboxPgOneStatement`** is the review's finding, found by the model.
  1. Dispatcher 1's statement takes its snapshot (`2:2:`) while transaction
     2 is open.
  2. Transaction 2 commits.
  3. Dispatcher 2 starts the epoch `3:3:`, hands row 1 on, and commits
     offset 1.
  4. Dispatcher 1 reaches the lock and reads the stored epoch under its old
     snapshot. Row 2 is invisible to it, so the batch is empty and drains
     the epoch: `delivered` becomes `3:3:`, and row 2 is lost.
- **`OutboxPgNoXip`.**
  1. Transaction 1 is open and transaction 2 rolled back.
  2. An empty epoch is delivered as `1:3:{1}`.
  3. Transaction 1 commits.
  4. The next epoch counts only ids from `delivered`'s `xmax` on, so it
     misses transaction 1, and drains to `3:3:`. Its row is lost.

## Inbox — `Inbox.tla`

Messages arrive under an identity, in some order, each in a slot; a
dispatcher takes a slot nobody holds and the oldest unprocessed row in it,
runs the subscriber inside its own transaction and marks the row in that
transaction; a row whose causal dependencies are not processed is set aside
to wait, and the transaction that marks the dependency puts it back; a
dispatcher is the slot it holds, so one at a time serves a slot; any of them
crashes, and a subscriber may fail.

PostgreSQL is reduced to five facts: `ON CONFLICT DO NOTHING` makes an
identity one row however often it arrives; `FOR UPDATE SKIP LOCKED` makes a
slot held by an open transaction invisible to the others; the dependency check
reads committed marks; the subscriber's writes, the mark and the waking of
what waited for the message commit together or not at all; a savepoint lets
the subscriber's writes roll back while the transaction goes on to record the
attempt. `MCInbox.tla` fixes the instance:
three messages, the second depending on the first, two slots, one crash or
transient failure. Which message lands in which slot is left open, so every
cut is checked, including dependencies across slots.

| property | kind | meaning |
| --- | --- | --- |
| `EffectsOnce` | invariant | the subscriber's writes for a message are committed at most once |
| `EffectsIffProcessed` | invariant | ... and exactly when the message is marked: writes and mark are one transaction, unless an operator resolved it |
| `CausalOrder` | invariant | a message is processed only after everything it depends on |
| `HeldOnce` | invariant | a row is held by one slot at a time, and a slot holds one row |
| `ParkedIsAside` | invariant | a parked message is out of the queue, unprocessed, and got there by exhausting its attempts or its wait |
| `WaitingIsAside` | invariant | a message set aside waits for a dependency of its own that is not processed, and is neither processed nor parked meanwhile, unless its wait ran out: then it is parked and keeps the identity |
| `WokenAfterExpiry` | liveness | a row whose wait ran out does not stay parked once what it depends on is processed: the mark of the hole unparks it (ADR-0036 R8) |
| `ArrivalOrder` | invariant | messages without dependencies of one slot and one Tenant are processed in order of arrival; with one Tenant, the whole slot's order |
| `BoundedOvertaking` | invariant | rotating, while a message is a due head, its slot takes or sets aside at most one head of each other Tenant, NT − 1 in all, NT the number of Tenants (ADR-0041; `MCInboxRotation.tla`) |
| `EventuallyProcessedBesidePoison` | liveness | a poison message never parked holds only what its failure holds: rotating, its own (slot, Tenant) |
| `EventuallyProcessed` | liveness | a received message whose dependencies are processed is eventually processed, unless its subscriber never succeeds |
| `EventuallyParked` | liveness | with parking on, a message whose subscriber never succeeds is eventually parked or resolved |
| `EventuallyAside` | liveness | with parking and expiring waits, every received message ends up processed or parked |

`EffectsIffProcessed` is the guarantee the inbox exists for, and the one the
Go channel API of the earlier port broke by marking a message on delivery
rather than on completion.

Dependencies (ADR-0006). The walk looks at the head of the queue only. A head
whose dependencies are not processed is set aside, `Wait`, out of the queue,
and the `Commit` or `Resolve` of the dependency puts back everything that
waited for it, in the same step. Three configurations pin the decisions:

- `InboxMissingDep.cfg`: the first message never arrives. The second is set
  aside and waits for ever, by default, and everything else still holds, the
  third message included.
- `InboxMissingDepExpires.cfg`: the same with waits that run out. The second
  message is parked with its dependency named, `Expire`, and
  `EventuallyAside` holds: every received message ends up processed or
  parked.
- `InboxLateDep.cfg`: waits run out and the first message arrives all the
  same, perhaps after the second's wait ran out. The second keeps the
  identity it waited for while parked, the first's mark unparks it, and
  `WokenAfterExpiry` holds with every property above (ADR-0036 R8).
- `InboxLateDepForgets.cfg`: the same with the expiry as it was before R8,
  forgetting the identity (`Expire <- ExpireForgetting`). TLC finds the
  second message parked for good once the first is processed.
- `InboxNoWaiting.cfg`: the head is not set aside but waits in place. TLC
  finds the slot held: the second message arrives before the first,
  sits at the head, and nothing behind it, its own dependency included, is
  ever taken. `check.sh` requires this violation.
- `InboxSplitWait.cfg`: the check of a dependency and the wait are two
  steps, `AtomicWait = FALSE`, and a mark in between cannot see a wait not
  yet settled — the implementation before ADR-0009. TLC finds
  `WaitingIsAside` violated: a message waits for a processed dependency,
  and nothing will wake it. `check.sh` requires this violation; the lock of
  ADR-0009 is what makes the two steps one.

Failures and parking (ADR-0005). A subscriber that fails rolls its writes back
to a savepoint; the attempt is recorded in the same transaction, and the row
is not due again until its backoff has passed. While it waits it holds its
slot: `ArrivalOrder` is why. After `MaxAttempts` failures the row is
parked, out of the queue but in the table, and the slot flows; an
operator may unpark it or resolve it, marking it processed without effects.
`Poison` names the messages whose subscriber never succeeds. Three
configurations, on one slot and no dependencies:

- `InboxPoison.cfg`: `m1` is poison, `MaxAttempts = 2`, one operator action.
  Everything holds: `m1` is parked, `m2` and `m3` are processed in order, and
  resolving `m1` marks it without effects.
- `InboxPoisonNoParking.cfg`: the same with `MaxAttempts = 0`. TLC finds
  `EventuallyProcessed` violated: `m2` and `m3` wait behind `m1` for ever. This
  is the inbox before ADR-0005, and `check.sh` requires the violation.
- `InboxSkipNotDue.cfg`: a message in backoff is stepped over instead of
  holding its slot. TLC finds `ArrivalOrder` violated: `m1` fails once,
  `m2` is processed meanwhile, `m1` after it. The order loss of a retry topic;
  `check.sh` requires the violation.

Attempts are counted in the model only where the count decides something:
with `MaxAttempts = 0` an unbounded counter would make the state space
infinite.

**Tenants in turn (ADR-0041).** Every message has a Tenant, `TenantOf`,
numbered from 1. By default there is one Tenant and a slot is one FIFO, as
before; `TenantOf` and `Rotates` are definitions, not constants, so every
configuration and every instance of the module keeps that behaviour unless it
replaces them (`TenantOf <- MCTwoTenants`, `Rotates <- MCTrue`).

Rotating:
- the head of (slot, Tenant) is the Tenant's oldest row in the slot's queue;
- the slot's `cursor` names the Tenant served last, and the holder takes the
  head of the first Tenant after it, cyclically, whose head is due;
- the cursor moves when a head is taken or set aside, and stays when the
  dispatcher crashes, as decision 3 has it on a rollback;
- a head in its backoff holds its own (slot, Tenant), not the slot.

The walk's bound K and a slot's dormancy are the statements' matter, in
`InboxPg.tla`. Here a walk sees every Tenant of its slot, which takes heads
in the same order. A step of its own, `Pass`, moves the cursor past Tenants
with no due head and takes nothing, as a walk bounded by K does when it
finds none, so that `InboxPg.tla` can refine this module.

`MCInboxRotation.tla` extends `MCInbox.tla` with a history variable,
`overtaken`: for each message and each other Tenant, the heads of that Tenant
a slot took or set aside while the message was a due head of its (slot,
Tenant). It counts dispatches, not time.
It is kept out of `MCInbox.tla` so that every specification there keeps its
variables, and out of `Inbox.tla` so that `InboxPg.tla`'s refinement does not
have to produce it.

Configurations, with m1 and m2 of Tenant 1 and m3 of Tenant 2:
- `InboxRotation.cfg` (`MCInboxRotation.tla`): the instance of `Inbox.cfg`,
  rotating. Everything holds, `BoundedOvertaking` included.
- `InboxRotationPoison.cfg` (`MCInboxRotation.tla`): the instance of
  `InboxPoison.cfg`, rotating. Everything holds.
- `InboxRotationPoisonNoParking.cfg` (`MCInbox.tla`): m1 poison and never
  parked. m2 waits behind it by design, and m3, of the other Tenant,
  is processed: `EventuallyProcessedBesidePoison` holds.
- `InboxFifoSlot.cfg` (`MCInboxRotation.tla`): the FIFO, before ADR-0041. TLC
  finds `BoundedOvertaking` violated: m1 and m2 are taken while m3 is a due
  head.
- `InboxCursorStays.cfg` (`MCInboxRotation.tla`): a rotation whose cursor is
  never written (`Turned <- MCCursorStays`), so the walk always starts from
  the same Tenant. TLC finds `BoundedOvertaking` violated the same way: the
  rotation has become a priority by Tenant.
- `InboxPoisonHoldsSlot.cfg` (`MCInbox.tla`): the FIFO behind a poison head
  never parked. TLC finds `EventuallyProcessedBesidePoison` violated: m3
  waits behind m1 for ever. It runs on `Spec`: there the history counter
  would grow without end.
- `InboxNewestOfTenant.cfg` (`MCInbox.tla`): a rotation taking a Tenant's
  newest row (`HeadOf <- MCNewestOfTenant`). TLC finds `ArrivalOrder`
  violated.

`BoundedOvertaking` bounds each other Tenant to one head, not only all of
them to NT − 1: one large Tenant overtakes a due head once, whatever its
backlog, and that is what the rotation is for. Two Tenants cannot tell the
two forms apart, nor a walk that starts after the cursor from one that goes
by Tenant number and skips only the cursor's Tenant, as `tenant_id <>
last_tenant ORDER BY tenant_id` would. `MCInboxThree.tla` has three, in one
slot: m1 and m2 of Tenant 1, m2 depending on m1, m3 of Tenant 2, m4 of
Tenant 3.
- `InboxRotationThree.cfg`: one crash. Everything holds, `BoundedOvertaking`
  included: 15 382 states.
- `InboxWalkFromLowest.cfg`: that walk (`Dist <- MCWalkFromLowest`). TLC
  finds `BoundedOvertaking` violated: Tenant 1 comes first from every cursor
  but its own, so m1 is taken, its dispatcher dies, a walk passes Tenant 2,
  which has no row, and m1 is taken again while m4 is due. On
  `InboxRotation.cfg`'s two Tenants the same walk holds, with the same
  16 766 states as the rotation.

**Guests** (ADR-0042 decision 5). A process with a paused channel walks a
slot as a guest, from a cursor of its own, `guest`, which it alone moves:
it takes the head of the first Tenant after that cursor whose head is due,
among the Tenants it does not skip (`GuestSkips`, standing for a paused
channel's heads), and leaves the slot's cursor where it is (`GuestFetch`).
`Guests`, `GuestSkips` and `GuestFromLowest` are definitions, off in every
other instance, each of which maps `guest` to a constant. With guests a
head due at every process — of a Tenant no guest skips — is overtaken at
most twice by each other Tenant, once by each cursor's turn:
`BoundedOvertaking` bounds it so (`OvertakeBound`).
- `InboxRotationGuest.cfg` (`MCInboxThree.tla`): guests that skip Tenant 3.
  Everything holds.
- `InboxRotationGuestFromLowest.cfg`: guests without a cursor of their own,
  from the lowest Tenant each time. TLC finds `BoundedOvertaking` violated:
  Tenant 1 comes first at every guest's walk.

`check.sh` requires each of the six violations.

## Inbox forgetting — `InboxForget.tla`

The inbox of `Inbox.tla`, forgetting processed rows by its channel's window
(ADR-0033 decision 4), in a module of its own over `Inbox.tla` unchanged. An
identity may arrive again while its horizon is open — a relay's retry, a
broker's retention, a producer's rewind within its replay horizon — and a
repeat of a forgotten identity is a new row, processed again; the horizon
closes once, for good. A ghost of everything ever processed restates the
properties that speak of what was processed. `MCInboxForget.tla` fixes the
instance: three messages, the second depending on the first, all of them
able to arrive again; two slots.

| Configuration | Rule | Expected |
| --- | --- | --- |
| `InboxForget.cfg` | after the horizon closes, dependency targets kept | every invariant holds |
| `InboxForgetEarly.cfg` | before the horizon closes | `EffectsOnce` violated |
| `InboxForgetTarget.cfg` | dependency targets forgotten | `WaitingIsAsideEver` violated |

## Inbox outage — `InboxOutage.tla`

ADR-0042: a stage's outage pauses its channel in the process that sees it,
and nothing is parked for it. The inbox of `Inbox.tla` reduced to what the
outage protocol turns on, over ADR-0041's heads: a (slot, Tenant)'s head is
its oldest queue row, and a head held holds its own (slot, Tenant) alone.

Processes share the table, each with workers (its loops) and its own breaker
and memo of Tenants:
- an ask is split into its start, which locks the head and its slot, and its
  verdict, which commits; between the two a worker may roll back, writing
  nothing (`AskStart`, `AskEnd`, `Rollback`);
- the verdict is the environment's: a row is served when the stage is up for
  that process (not down, not cut off from it) or when the process holds its
  Tenant's key cached; a Tenant whose keys fail alone answers `Unavailable`
  at Tenant scope, the stage at Stage scope; a permanent row parks at once; a
  flaky one fails its first time, spending an attempt;
- an `Unavailable` holds its row and spends nothing; a Tenant's is
  remembered in the process's memo, which holds the Tenant's other heads
  unasked (`HoldUnasked`) and then lets one ask through, in flight, at a time;
- a Stage `Unavailable` may open the process's breaker (the thresholds are
  the core's, so the model takes either branch); an open breaker asks only by
  its trial, one at a time, which a processed verdict closes, and a circle
  that finds no head closes it (`Circle`).

A head whose dependency is not processed is set aside, and its dependency's
mark wakes it. The expiry parks a waiter whose chain reaches no queued row
and spares the others (`Spared`); an expired row keeps the identity it waited
for. The operator re-arms parked rows with exactly `Tries` failures left
(`UnparkAll`, holding the row's slot), gives a held head up (`ParkHeads`), or
gives a Tenant's queue up from its newest row, a row a step (`ParkQueue`).

The fairness is strong for a worker's ask and for a hold's running out: a
slot another process keeps taking and letting go is taken by this one in
turn, as the take serves the least recently served slot `SKIP LOCKED`, and a
hold a trial asks again runs out between two trials, its stage-scoped
interval at most a tick's cap and the trials a tick apart.

`Mutant` names a refuted design, `"none"` the decided one. Each refuted
configuration names one property, and `check.sh` runs its twin — the same
configuration with `Mutant = "none"` — where that property must hold, so
that the property and no other tells the two apart. `check.sh` pins every
passing configuration's and every twin's count of distinct states.

`MCInboxOutage.tla` fixes the instances: m1 and m2 of Tenant t1 under one
key in slot s1, m3 of t2 in s2; m4 and x where an instance names them.

| Configuration | Rule | Expected |
| --- | --- | --- |
| `InboxOutage.cfg` | the stage down once and back, p2 cut off once and back, t1's keys cached in p1, m2 failing once, a rollback | every invariant and property holds; 17 368 states |
| `InboxOutageTenant.cfg` | t1 failing alone, its rows in both slots, two workers of one process | `OneAskPerReach`, `EventuallyProcessed`; 1 936 |
| `InboxOutageCutOff.cfg` | p2 cut off for good, the stage up | the other process processes every row; 544 |
| `InboxOutageDeps.cfg` | m3 waits for m2, queued; m4 for x, which never arrives | m3 spared and processed, m4 expires; 36 |
| `InboxOutageOperators.cfg` | m1 failing and parking, m3 permanent, the operator acting | `CanaryExact`, `GivenUpIsTheEnd`, `KeyOrder`; 23 986 |
| `InboxOutageUnavailableSpends.cfg` | an `Unavailable` that spends | `UnavailableNeverSpends` violated |
| `InboxOutageNoFlightMark.cfg` | a memo that lets every worker ask a Tenant in flight | `OneAskPerReach` violated |
| `InboxOutageExpirePresent.cfg` | an expiry that parks a waiter whose dependency is queued | `PresentDependencyNeverExpires` violated |
| `InboxOutageMarkKeepsCause.cfg` | a mark that keeps the cause | `CauseIsCurrent` violated |
| `InboxOutageMarkKeepsHold.cfg` | a mark that keeps the hold | `HoldIsQueued` violated |
| `InboxOutageCanaryKeeps.cfg` | a re-arm that keeps a higher count | `CanaryExact` violated |
| `InboxOutageQueueOldest.cfg` | a queue given up from its oldest row | `GivenUpIsTheEnd` violated |
| `InboxOutageRearmBesideAnAsk.cfg` | a re-arm beside an ask of its slot | `KeyOrder` violated: a later row of the key, in flight while the overtaking is read, is processed past the row re-armed |

The last was the model's finding, not a design the decision refuted: the
library's bulk unpark read whether a later row of the stream had overtaken a
row without holding the slot, and an ask in flight was then processed past
it. The library now holds the slots of a batch before it reads.

The bound on asks is not the model's: TLC has no rates, and the breaker's
core tests (`crates/inbox/src/breaker/tests.rs`) are its executor.

## Inbox, statement by statement — `InboxPg.tla`

`Inbox.tla` has one step per protocol action and assumes what the
statements must deliver: that a slot has one holder, that a wait is set in
the same step as the check that decided it. `InboxPg.tla` is the inbox one
level down, the statements of `crates/inbox` against PostgreSQL under
READ COMMITTED, and it earns those assumptions. A commit advances a clock
and stamps the versions it publishes; a statement sees the versions stamped
no later than its snapshot, and its own transaction's pending writes.
Taking a slot is two steps, the snapshot and then the lock, because that is
where the two come apart. The slot's row is locked `FOR UPDATE SKIP LOCKED`;
the advisory lock on a dependency's identity is taken by the wait and by the
mark, held to the end of the transaction, re-entrant for its holder. Left
out, as matters `Inbox.tla` settles: failures and backoff (except where
the rotation needs them, below), parking after attempts, operators.
Expiring waits are in, behind `Expires`. One abstraction is declared in the module: the dependency
check is taken with the head's snapshot, where the implementation runs it
one statement later — a mark committed in that window is then seen, and a
head this model would set aside is processed instead.

The module refines `Inbox.tla`: an `INSTANCE Inbox` with the protocol's
variables replaced by what this state shows of them — `processed` the
committed marks, `holding` the head of a dispatcher between its dependency
check and its commit, `waiting` the committed waits and the one a dispatcher
has set after its re-check and not yet committed — and `Refines`, every
step of this module a step or a stutter of the protocol, checked by TLC as a
property. The protocol's invariants are read through the same mapping.
`MCInboxPg.tla` fixes the instance: three messages, the second depending on
the first, every cut into two slots, two dispatchers, one rollback.

Three switches name the designs the module was written to tell apart, and
`check.sh` requires each to fail as it did:

- `InboxPg.cfg`: as implemented. `Refines`, `EventuallyProcessed`, the
  protocol's invariants, `NoDeadlock` and `NoDispatcherDied` all hold;
  about a million states, two and a half minutes.
- `InboxPgNoLock.cfg`: `WithLock = FALSE`, the wait of ADR-0006 as it was
  implemented. TLC finds `WaitingIsAside` violated: a wait set in one
  statement, the dependency marked and its waiters woken in another
  transaction that could not see the wait, the wait committed — a message
  waiting for a processed dependency, which nothing wakes. The lost wake
  ADR-0009 fixed, found by the model rather than by a test.
- `InboxPgHeadInTake.cfg`: `HeadInTake = TRUE`, the head read in the take's
  statement, the take of ADR-0008 as it was implemented. TLC finds
  `NoDispatcherDied` violated: the statement's snapshot predates its lock,
  the previous holder commits in between, and the head the snapshot chose
  fails the re-check the lock makes — a slot without a head, which the
  implementation took for an error that stopped the loops.
- `InboxPgWalkOn.cfg`: `WalkOn = TRUE`, a dispatcher that set a head aside
  walks on in the same transaction with the lock held, the first design of
  ADR-0009. Two slots of two messages each, every head depending on a
  message that never arrives, crosswise: TLC finds `NoDeadlock` violated,
  each dispatcher holding the lock the other wants.
- `InboxPgExpires.cfg`: as implemented, with waits that run out (`Expires =
  TRUE`): the expiry's statement runs beside the dispatchers, FOR UPDATE
  SKIP LOCKED, so that a row an in-flight transaction holds — its wait being
  set, its wake by a mark — is skipped, and any other is re-checked against
  its latest version. A parked row keeps the identity it waited for
  (ADR-0036 R8). A mark that meets the expiry in flight waits for its
  commit and re-checks the row the same way, so the expiry first, as the
  model has it, covers that interleaving. `Refines` holds with `parked` and
  `expired` read through the mapping, and `EventuallyProcessed` holds: a
  message parked when its wait ran out is woken by its dependency's mark.
  About 2.4 million states, six and a half minutes.
- `InboxPgExpiresForgets.cfg`: the same with `ForgetsOnExpiry = TRUE`, the
  expiry as it was before R8, clearing `waiting_for`. TLC finds
  `EventuallyProcessed` violated: the second message parked when its wait
  ran out, the first marked, and the second never woken.

**Tenants in turn, statement by statement (ADR-0041).** `Rotates` is the
slot of ADR-0041 decision 2 as statements:
- the slot row's cursor, `last_tenant`;
- the walk inside the head's statement, under the slot's lock and a fresh
  snapshot, which examines at most `K` Tenants after the cursor and wraps
  once;
- `Passed` when none of their heads is due;
- the round, from the cursor it began at to the walk that passes it, and the
  dormancy that ends it;
- a take that qualifies a slot by a queue row and its not being asleep.

The cursor the walk writes is pending in its transaction and commits with
it. A walk that finds nothing commits the slot row at once.

`TenantOf`, `Rotates`, `K` and the switches are definitions, as in
`Inbox.tla`. A rotating configuration replaces `Rotates` and `TenantOf` here
and in the instanced `Inbox.tla` alike (`Rotates <- [Inbox] MCTrue`), so that
the refinement maps onto a rotating protocol. Two things come in with the
rotation that the FIFO had no need of here:
- a subscriber's own failure and the backoff it leaves (`Fails`);
- an insert whose position is drawn by its statement and committed later
  (`SplitReceive`), so that rows of different Tenants commit out of the
  order of their positions. The rows of one Tenant arrive one at a time, as
  a stream's do.

The world's clock is two events, not a variable: a backoff passing
(`Elapse`) and `max_dormancy` passing for a dormant slot (`Cap`). The model
has no time, so it cannot tell `max_dormancy` from a backoff of 300 s. The
cap's bound in time is ADR-0041's E4.

The refinement:
- `cursor` is the holder's pending cursor once its walk has chosen a head,
  the committed one otherwise.
- A walk that found nothing is `Inbox.tla`'s `Pass`.
- A set-aside is the protocol's `Wait` at the walk that chose the head, not
  at the statement that sets the wait. The walk moved the cursor, so a
  dependency marked before the wait is set wakes it in the protocol, and the
  transaction commits the cursor alone, as the implementation commits it.
- An insert committed after a later one is `ReceiveAs` with the position
  the sequence drew.

A rollback of a rotating dispatch is not modelled. Decision 3 moves the
cursor after it by a statement of its own, which is ADR-0041's slice 2.

`MCInboxPgRotation.tla` adds a history variable, `passes`: a slot's walks
that returned `Passed` in a row. Two properties are new:
- `NoDueHeadAsleep`: a slot sleeps past a due head only when that head could
  not be seen by the round that put it to sleep — its insert was in flight
  when the round began, or a dependency's mark woke it after.
- `PassedPerRound`: while nothing in the slot changes, at most ⌈T_s/K⌉
  `Passed` in a row. The last may be one walk more than the Tenants need.
  When the Tenant served last has no row left, the place the round began at
  is no Tenant's, and the walk that examined the last Tenant before it
  cannot know it is past.

Configurations:
- `InboxPgRotation.cfg` (`MCInboxPgRotation.tla`): one slot, one loop,
  three Tenants of a row each, numbered so that a round beginning at Tenant 3
  passes Tenant 2 first. `K = 1`, two failures, inserts committed out of
  order. `Refines`, `EventuallyProcessed` and every invariant hold, the two
  new ones included: 431 643 states.
- `InboxPgRotationWide.cfg`: the same with `K = 3`, a walk that examines
  every Tenant, so that a round is one walk: 360 127 states.
- `InboxPgRotationDeps.cfg` (`MCInboxPg.tla`): rotating beside a dependency.
  m2 depends on m1; m1 and m2 are Tenant 1's, m3 Tenant 2's. Two cuts: m1
  in one slot and m2 with m3 in the other, or all three in one. Two loops,
  one failure. `Refines` holds with the set-aside at the walk, and so
  do `EventuallyProcessed`, `CausalOrder`, `WaitingIsAside`, `NoDeadlock`
  and the rest: 1 914 637 states, about twice `InboxPg.cfg`.
- `InboxPgRotationNoCap.cfg` (`MCInboxPg.tla`): `InboxPgRotation.cfg`
  without `Cap`. `EventuallyProcessed` still holds: a dormancy ends when a
  backoff its round saw passes, every head the round examined was in
  backoff, and that passing is fair. So no liveness property fails for want
  of the cap, which bounds a wait in time only; this configuration holds so
  that liveness is seen not to rest on it: 287 281 states.
- `InboxPgSleepsPastBackoff.cfg`: the same with a dormancy that the backoffs
  its round saw do not end, as a `dormant_until` set to the cap would be.
  TLC finds `EventuallyProcessed` violated: a backoff the round saw passes,
  and the slot sleeps on past its head, the take finding it asleep for
  ever. With the cap the same mutation breaks `NoDueHeadAsleep`.
- `InboxPgDormantAfterAtWalk.cfg`: `dormant_after` read by the walk that ends
  the round, as ADR-0041 first had it. TLC finds `NoDueHeadAsleep` violated:
  - m1 of Tenant 2 and m2 of Tenant 3 fail in turn, and the cursor is at 3;
  - a walk examines Tenant 2 and passes, and the round begins at 3;
  - m3 of Tenant 1 commits behind the walk;
  - the next walk examines Tenant 3, passes the round's start, and puts the
    slot to sleep with m3 below `dormant_after`, due and never in flight.
- `InboxPgNoRoundEnd.cfg`: a round that never ends. TLC finds
  `PassedPerRound` violated.
- `InboxPgPassedStays.cfg`: a walk that finds nothing and leaves the cursor.
  TLC finds `PassedPerRound` violated: the next walk examines the same
  Tenants.
- `InboxPgNewestOfTenant.cfg` (`MCInboxPg.tla`): a walk taking its Tenant's
  newest row, m1 and m2 of one Tenant in one slot. TLC finds `ArrivalOrder`
  violated.

## Bridge — `Bridge.tla`

The composition: a source outbox, the bridge, a destination inbox, built with
`INSTANCE` from the two modules above so that what each proved on its own is
inherited, not restated. A source dispatcher hands each message of a batch to
the bridge, whose subscriber stores it in the inbox in a transaction of the
inbox's own before returning; the batch is acknowledged after the last one.
Either process dies at any point, in particular between the store and the
acknowledgement, which delivers the batch again. `MCBridge.tla` fixes the
instance: three messages over two URIs, one source slot, two destination
partitions with a dispatcher each, batches of two, one crash per side; which
URI a message has and which partition a URI lands on are left open.

The intake takes every message (ADR-0036). One message of the instance is
one it can never store: it is set aside, its row committed in a transaction
of its own, and only then does the subscriber return, a step of its own, so
that a death falls between the two and the message, delivered again, is the
same row set aside. An environment hold, the inbox's database out of reach
once, stops both the store and the setting aside until it passes, with
fairness to the recovery.

| property | kind | meaning |
| --- | --- | --- |
| `SourceInvariants`, `DestinationInvariants` | invariants | everything the outbox and the inbox proved alone still holds in the composition |
| `ReceivedWasCommitted` | invariant | the inbox holds rows only for messages committed at the source |
| `EventuallyProcessed` | liveness | every message committed at the source that the intake can store is eventually processed at the destination |
| `EventuallySetAside` | liveness | every committed message the intake can never store is eventually set aside |
| `SetAsideOnlyUnstorable` | invariant | only a message the intake can never store is set aside: an environment hold diverts nothing |
| `SetAsideNeverStored` | invariant | a message set aside was never stored as a row |
| `EndToEndOrder` | invariant | messages of one URI are processed in the order they were published |

`EventuallyProcessed` with the inbox's `EffectsOnce` is the theorem the whole
construction exists for, and neither half can state it alone: however often a
crash makes the bridge cross the same message again, its effects at the
destination happen exactly once. `EndToEndOrder` holds under the conditions
the instance states — the inbox cut by URI, no causal dependencies — and not
otherwise: a dependency may hold a message back while a later one of the same
URI goes through, which `Inbox.tla` allows.

`BridgeAckFirst.cfg` turns `StoreBeforeAck` off: the bridge counts a message
handled on delivery and stores it afterwards, the shape of the channel API in
the earlier Go port. TLC finds the loss on a single message: delivered,
acknowledged, the process dies, the store never happens. `check.sh` requires
that violation, and three more, one for each way the setting aside could be
wrong:

| configuration | the mutation | what TLC finds |
| --- | --- | --- |
| `BridgeAckWithoutSetAside.cfg` | the subscriber returns on an unstorable message without setting it aside | a committed message set nowhere (`EventuallySetAside`) |
| `BridgeSetAsideOnHold.cfg` | a message is set aside because the inbox's database holds | a storable message diverted (`SetAsideOnlyUnstorable`) |
| `BridgeAckBeforeReject.cfg` | the subscriber returns before the row set aside commits | acknowledged, the position moved, the process dies before the commit: a message set nowhere (`EventuallySetAside`) |

The main configuration explores 1.6 million distinct states and takes about
seven minutes; the rest run in seconds.


## Trace validation — `TraceOutbox.tla`, `TraceInbox.tla`, `TraceBridge.tla`

A model proves the protocol; a trace check shows the code follows it. The
tests attach the recorder of `crates/trace`, an observer of both the outbox
and the inbox that writes every event as a line of JSON. For the outbox: what
was published, with the transaction id and serial PostgreSQL assigned; what a
fetch returned, with the `pg_snapshot_xmin` it ran under and its `LIMIT`;
each message handed to the subscriber and the outcome; the acknowledged
position; how the dispatcher's transaction closed. For the inbox: what was
received, with its arrival position and dependencies, or that it was a
duplicate; each slot taken and each row set aside, taken, handed over,
marked, and how the slot's transaction closed. One recorder
watching an outbox that feeds an inbox keeps the order across both.

```bash
make traces    # records every trace again, at ASCETIC_DDD_TEST_PG_URL, and checks the covered ones
```

**What is covered.** The traces under `traces/` are the covered ones, and
the list. `make traces` records every test's trace into a directory of its
own, then:
- replaces each covered trace by its new recording, and fails when no test
  recorded one again: its test is gone or renamed;
- holds each recording that is not covered to its entry in
  `traces/uncovered.tsv` (`check.sh --uncovered`, ADR-0042 T1), and fails on
  a recording not listed, and on an entry whose recording no test made, that
  is covered, or that stands otherwise than it says. An entry is `refused`,
  its reason the one `trace2tla.py` gives for not translating the run, word
  for word: what the run holds that the model does not describe yet. Or it is
  `unfit`: the run translates and TLC refuses one of its steps, and the
  reason names the record that owes the diagnosis. A refused run's steps
  before its refusal are checked all the same, and must fit: a refusal
  hides no step before it. A model that learns a step so makes the entries
  it explains stale, and the runs it can now check are covered or
  diagnosed, not forgotten;
- runs `check.sh traces` over the covered ones.

A failure's park says whether its verdict was permanent: `MaxAttempts` is
read off the parks of exhausted attempts alone, and a permanent one is the
model's `FailPermanent`. Read as an exhausted one, a single permanent failure
would fit as `MaxAttempts = 1`, for the wrong reason. A dispatch closed with
an outcome the mapping does not know is refused, rather than ended without a
step; a walk that passed took no head, and is no step. What ADR-0042 added
and `Inbox.tla` does not describe yet is refused, each with its reason: a
head held unasked, a row held for an `Unavailable`, a guest's walk among
paused heads, and a verb over many rows that moved some, whose rows the
trace does not name. A read of the operator's surface moves no row, and is
no step.

A recording that fits is covered by committing it. A run with a step the
trace lacks fits no run of the model, or fits one for the wrong reason — a
row held by SQL is no row held in the trace. So the tests' fixtures note in
the trace each statement of their own that writes rows behind the
observers (by SQL, as an older producer or a hand would), and
`trace2tla.py` refuses the run at it; a statement that only moves a time —
a back-off's, a wait's, a park's, a slot's sleep — is the step of time the
model takes unseen, and is not noted. A test that moves rows by an inbox
the trace does not watch records no trace.

Not covered, as of 2026-11-17 (`traces/uncovered.tsv` lists each):
- **ADR-0042's steps, refused: 19 runs of `tests/outage.rs`.** A row held for
  an `Unavailable` (15), and a verb over many rows that moved some (4). They
  are covered once `Inbox.tla` holds a row and a verb's rows are named in
  the trace; until then a run of them that the converter takes goes stale in
  the list and is looked at.
- **Rows written behind the observers, refused: nine runs.** Seven of
  `tests/outage.rs`, whose holds, parks or waits a test sets by SQL, and two
  of `tests/pg.rs` that were covered and fit for the wrong reason:
  `inbox-attempts_saturate`, its attempts set by SQL, fit because the check of
  attempts is skipped when nothing parks; `inbox-window_kept` has its waits
  rewritten by SQL.
- **A row forgotten and stored again, refused: `inbox-window_passed`.** The
  sweep forgets a processed row, and a repeat past its window is stored
  again, at a new position; `Inbox.tla` forgets nothing (`InboxForget.tla`
  does).
- **The outbox's epochs (ADR-0035), not fitting: eleven runs.**
  `TraceOutbox.tla` follows `Outbox.tla`, which releases below `xmin`.
  `outbox-visibility`, checked: a committed version leaves past an open one
  in an epoch of its own, which that model refuses. Ten more, of the tests
  of the epochs and what they brought, not checked one by one:
  `outbox-backlog`, `outbox-between` and `outbox-rewind_epoch` go on to
  deliver an epoch with nothing of their selection's, which the converter
  refuses, after steps that already do not fit; `outbox-commit_order`,
  `outbox-lane_start`, `outbox-position`, `outbox-replay_horizon`,
  `outbox-reset`, `outbox-rewind_open`, `outbox-uri`. The epoch relay is
  checked statement by statement, in `OutboxPg.tla`; its trace validation is
  owed (ADR-0041 § Owed). Recorded eight times, four of them beside a stream
  of short transactions in another database of the cluster, the eight that
  translate stood not fitting 63 times of 64, and the 64th was a TLC run that
  ended with neither outcome, which `check.sh` runs once more.
- **Rotating runs, not fitting: three of `tests/outage.rs`**
  (`history_7c15`, `sparse`, `unparkall`). `TraceInbox.tla` substitutes a
  cursor that never moves, so it follows a slot served in arrival order: a
  walk that serves a later Tenant's head before an earlier one's does not
  fit (ADR-0041 § Owed). The tests of the rotation in `tests/pg.rs` record
  none.
- **A row failing for a moment, then for good, not fitting:
  `inbox-outage_exits`.** `TraceInbox.tla` reads off a run which messages
  fail for good, a message's verdict, not an attempt's; the row's first
  failure, transient, is refused for it.
- **Two runs of no step, refused: each would fit whatever the model
  said.** `inbox-identity_past`: its test's intake sets both messages
  aside. `outbox-refused_wire`: its producer refuses each message at the
  commit, which aborts, and nothing is recorded.

Not recorded, each for its reason in its test: the tests with several
dispatchers at once (above), and those that move rows by an inbox the trace
does not watch, or write them by a session of their own — among them
`held_by_concurrent` of `tests/pg.rs`, whose holder's row is inserted by SQL
(it had been recorded, and did not fit), and five of `tests/outage.rs`.

`trace2tla.py` maps the lines onto the model's steps — an acknowledgement and
the commit that follows it become one `Ack`, a rolled-back batch a `Crash`, a
failed handling nothing — and `TraceOutbox.tla` makes the model take exactly
those steps with the logged values in place of its own: `PublishAs` takes the
logged id, `FetchWith` the logged horizon and limit. What was not logged, the
producers' commits and aborts, the trace determines: at a fetch with horizon
`h`, a message with an id below `h` in the dispatcher's slot past its
position has ended, and it committed if it is in the batch, aborted if it is
missing before the batch's end. The behaviour is therefore one chain, and TLC
walks it checking that each logged step is a step of the model and that the
model's invariants hold after it. A run that fits ends with the trace consumed,
which `check.sh` reads off a violated `NotFinished`; one that does not fit
deadlocks at the refused step, and TLC prints the state, `i` naming the step.

`TraceInbox.tla` is the same for the inbox. Receiving, setting aside, taking
a row, failing, committing, expiring, unparking and resolving are all logged,
the rows a mark or a resolve woke with it; the one thing that is not is
time, so the passing of a backoff is the hidden step before the failed row is
taken again. A store carries the id of its transaction and the slot the row
landed in, so the model places every arrived message where the table did,
and every step of a walk over the table the snapshot its statement ran
under, and the checks go by what the snapshot could see, as the outbox's go
by the horizon: a walk is checked over the stored rows visible to it, and a
store logged after a walk that saw it is taken as the hidden step before
that walk. A dispatcher is the slot it holds, whoever ran it: a slot has one
holder at a time, so the events between a fetch, or a set-aside, naming a
slot and the close naming it are one dispatch; a set-aside is a dispatch of
its own (ADR-0009).

Only runs with one dispatcher at a time are recorded and checked. With
several, the order events are logged in is not the order of their commits:
a close is logged after its COMMIT, a slot is taken in one statement and its
head read in the next, a mark on one slot is logged before a walk of another
whose snapshot predates it. Each of those needed a rule of its own to
reorder the log by snapshots and transaction ids; the rules grew, kept
missing cases, and found no defect of the library, while the tests' own
assertions found two. So a test with several dispatchers asserts its outcome
and the table's state — in particular that no row waits for a processed
dependency, the lost wake of ADR-0009 as one query — and leaves the
interleavings to the model. The close is logged after the COMMIT, and the COMMIT is what frees
the slot, so the next holder's fetch may be logged before it; the take
proves the close came first, and `trace2tla.py` moves the close to just
before the take, for both crates. `traces/reordered/` holds two recorded
runs with a close moved past the next take by hand, and `check.sh` requires
them to fit. An empty fetch and a handling are checks rather than steps: a
fetch that took a slot and found its head waiting for its backoff must find
nothing due in that slot, one that took no slot nothing due in any slot
nobody holds — the held ones were passed by, `SKIP LOCKED`; the handled
message must be the one held; a commit's woken rows must be exactly those the
model has waiting for the message. The
parking policy is read off the trace: `MaxAttempts` is the count at which a
message was parked, so a run that parked one message at two attempts and let
another fail twice does not fit. `ArrivalOrder` is asked for when the table
had one slot. A dependency that never arrives is a message of
the instance all the same, as in `InboxMissingDep.cfg`.

`TraceBridge.tla` checks a run of an outbox feeding an inbox against
`Bridge.tla`. It instances the two checkers above over the same trace, each
seeing the records of its side, and the composition supplies the frame of the
other side and the invariants of the whole. The one step of its own is
`Forward`: the bridge's subscriber stores the message in the inbox and
returns, and the outbox counts it handled. The trace shows two events, the
inbox's receive and the outbox's handle, and `trace2tla.py` joins them into
one step placed where the store happened. A handle without a store, or a
store outside a handling, is refused: with the store before the
acknowledgement those are not steps of the bridge. A message the intake set
aside is two steps of the model, `SetAside` and `Acknowledge`; the trace shows
the inbox's rejection, which names no message — its identity may be what
could not be read — and the outbox's next handling with no store, which is
the message set aside. `trace2tla.py` names them `set_aside` and
`acknowledge`, and a rejection no handling follows, or a handling before its
rejection, is refused. A message set aside is a message of the instance that
never arrives, as a dependency that never arrives is.

A fetch that took no slot names the group only, and the check then speaks for
every slot: none may have had visible work. `traces/` holds one recorded run
per test: the outbox and inbox tests, the
inbox's bridge tests from an in-memory broker, and the one run of an outbox
feeding an inbox, and the one where the inbox sets aside a row the outbox
holds without a position. Two outbox tests are not recorded there, because what they
exercise the model does not describe: moving the position by hand
(`set_position`), and the URI filter of a selection. `traces/forged/` holds
five runs edited by hand: an outbox dispatcher without the visibility rule,
fetching the later, committed transaction while the earlier one is open; an
inbox dispatcher that takes a message before the one it depends on; an inbox
fetch that returns a row its snapshot could not have seen; a bridge that
stores the message after acknowledging the batch, the shape
`BridgeAckFirst.cfg` shows loses messages; a bridge that counts a message
handled before the inbox set it aside, the shape `BridgeAckBeforeReject.cfg`
shows loses messages. `check.sh` requires TLC to refuse all five.

What a trace check cannot see: the order of lines is the order the observer
was called in, on one thread, and two tasks' lines may come in the other order
than the database saw their statements. For stores against walks the
snapshots settle it, on both sides. What they do not settle is the visibility
of marks to the inbox's dependency check, a statement of its own whose
snapshot is not logged: a run with two dispatchers, dependencies between
their messages and a mark committed between one's walk and the other's check
could show as not fitting, and would be a false alarm, not a false pass.

## Notes on TLC

What the checker itself taught while these models were written, each with
the place that carries the scar. TLC 2.19, Java 17.

- **`:>` and `@@` need `EXTENDS TLC`.** The function constructors used to
  write `Deps` and other small maps in the instance modules live in the `TLC`
  standard module, not in `Naturals` or `Sequences`: `MCInbox.tla`,
  `MCBridge.tla`.
- **An action under fairness must fix every variable, both sides included.**
  `WF_vars(A)` evaluates `ENABLED <<A>>_vars`; if `A` leaves a variable of
  the other side unmentioned, TLC reports "identifier X is either undefined
  or not an operator", pointing at the `vars` tuple, not at `A`. Every action
  of `Bridge.tla` therefore conjoins the other side's `UNCHANGED`.
- **`UNCHANGED I!vars` does not work.** TLC cannot prime a tuple that reaches
  it through an instance; the error is again "identifier … is either
  undefined or not an operator", pointing at the `INSTANCE` line. The trace
  modules spell the tuple out where they leave the state unchanged:
  `TraceOutbox.tla`, `TraceInbox.tla`.
- **A counter without a bound is an infinite state space.** TLC does not
  finish, and says nothing about why. `Inbox.tla` counts attempts only where
  the count decides something, since with parking off a poison message fails
  for ever.
- **Bind a variable before you constrain it in `Init`.** `uriOf = UriOf`
  comes before `O!Init`, whose `uriOf \in [Msgs -> Uris]` would otherwise
  enumerate every function of that set: `TraceOutbox.tla`, `TraceBridge.tla`.
- **A `.cfg` holds simple values only.** Model values, numbers, strings and
  sets of them; a sequence of records, such as a trace, goes into a generated
  module and is bound with `Trace <- TraceDef`: `trace2tla.py`.
- **Modules are looked up in one directory.** `-DTLA-Library` is ignored, so
  the models live flat here, and `check.sh` copies what a trace check needs
  into a temporary directory next to the generated module.
- **Deadlock detection is a tool, not only a nuisance.** The model checks run
  with `-deadlock`, since their behaviours are meant to end; the trace checks
  run without it, so that a run the model refuses stops as a deadlock at the
  refused step, with the state printed and `i` naming the step.

