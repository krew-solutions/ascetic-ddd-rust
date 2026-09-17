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
```

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
| `WaitingIsAside` | invariant | a message set aside waits for a dependency of its own that is not processed, and is neither processed nor parked meanwhile |
| `ArrivalOrder` | invariant | messages without dependencies of one slot are processed in order of arrival |
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
out, as matters `Inbox.tla` settles: failures, backoff, parking, expiring
waits, operators. One abstraction is declared in the module: the dependency
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

| property | kind | meaning |
| --- | --- | --- |
| `SourceInvariants`, `DestinationInvariants` | invariants | everything the outbox and the inbox proved alone still holds in the composition |
| `ReceivedWasCommitted` | invariant | the inbox holds rows only for messages committed at the source |
| `EventuallyProcessed` | liveness | every message committed at the source is eventually processed at the destination |
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
that violation.

The main configuration explores three quarters of a million states and takes
a few minutes; the rest run in seconds.


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
ASCETIC_DDD_TRACE_DIR=verify/tla/traces \
    cargo test -p ascetic-ddd-outbox -p ascetic-ddd-inbox --test pg --test bridge -- --include-ignored
./verify/tla/check.sh
```

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
acknowledgement those are not steps of the bridge.

A fetch that took no slot names the group only, and the check then speaks for
every slot: none may have had visible work. `traces/` holds one recorded run
per test: the outbox and inbox tests, the
inbox's bridge tests from an in-memory broker, and the one run of an outbox
feeding an inbox. Two outbox tests are not recorded there, because what they
exercise the model does not describe: moving the position by hand
(`set_position`), and the URI filter of a selection. `traces/forged/` holds
four runs edited by hand: an outbox dispatcher without the visibility rule,
fetching the later, committed transaction while the earlier one is open; an
inbox dispatcher that takes a message before the one it depends on; an inbox
fetch that returns a row its snapshot could not have seen; a bridge that
stores the message after acknowledging the batch, the shape
`BridgeAckFirst.cfg` shows loses messages. `check.sh` requires TLC to refuse
all four.

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

