----------------------------- MODULE Inbox -----------------------------
(***************************************************************************)
(* The Transactional Inbox of `crates/inbox`, as a protocol.               *)
(*                                                                         *)
(* Messages arrive, each under an identity, and each in a slot, a share of *)
(* the partition keys by hash stored with the row.  A dispatcher is the    *)
(* holder of a slot's lock, one at a time per slot and with no other       *)
(* identity: it takes the oldest unprocessed message of the slot, runs the *)
(* subscriber inside its own transaction, and marks the message processed *)
(* in that transaction.  A message whose causal dependencies are not       *)
(* processed is not taken: it is set aside to wait, out of the queue, and  *)
(* the transaction that marks the dependency processed puts it back.       *)
(* Dispatchers crash and subscribers fail at any point.                    *)
(*                                                                         *)
(* PostgreSQL is abstracted to what the protocol relies on:                *)
(*   - INSERT ... ON CONFLICT DO NOTHING: an identity is one row, however  *)
(*     often it arrives, and rows are ordered by arrival;                  *)
(*   - FOR UPDATE SKIP LOCKED: a row held by an open transaction is        *)
(*     stepped over by every other one;                                    *)
(*   - the dependency check reads committed marks only;                    *)
(*   - the subscriber's writes and the mark commit together, or not at     *)
(*     all; so do the mark and the waking of what waited for it.           *)
(*                                                                         *)
(* Setting a row aside is a step of its own here, durable at once; in the  *)
(* implementation it is written in the dispatcher's transaction, and a     *)
(* rollback of that transaction puts the row back in the queue, where it   *)
(* is set aside again.  The model has no such detour; the properties do    *)
(* not depend on it.  A wait may run out, WaitExpires, and the row is then *)
(* parked with the dependency named; by default it waits for ever, and a   *)
(* message whose dependency never arrives is outside the liveness claims   *)
(* but in nobody's way.  A subscriber that fails rolls its writes back to a *)
(* savepoint; the attempt is recorded in the same transaction, and the     *)
(* message is not due again until its backoff has passed, during which it  *)
(* holds its slot: stepping over it would lose the order.  After           *)
(* MaxAttempts failures the message is parked, taken out of the queue but  *)
(* kept in the table, and the slot flows; with MaxAttempts = 0 it          *)
(* never is, and a message whose subscriber never succeeds blocks its      *)
(* slot for ever, which InboxPoisonNoParking.cfg shows.  An operator       *)
(* may unpark a message, to try again, or resolve it, to mark it processed *)
(* without effects.  The model bounds failures and operator actions so     *)
(* that liveness can be stated.                                            *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
  Msgs,           \* messages, by identity
  Deps,           \* [Msgs -> SUBSET Msgs], causal dependencies; acyclic
  Arrives,        \* SUBSET Msgs: the messages that ever arrive
  Slots,          \* shares of the partition keys by hash; a dispatcher per slot, at a time
  MaxCrashes,     \* crashes and subscriber failures, in all
  WaitsOnDependencies, \* TRUE: a message whose dependencies are not processed is set aside to wait; FALSE: it holds its slot
  WaitExpires,    \* TRUE: a wait may run out and park the message
  Poison,         \* SUBSET Msgs: messages whose subscriber never succeeds
  MaxAttempts,    \* failed attempts after which a message is parked; 0: never, retry for ever
  BlockOnBackoff, \* TRUE: a failed message not yet due holds its slot; FALSE: it is stepped over
  AtomicWait,     \* TRUE: the check of a dependency and the wait are one step, as under the lock of
                  \* ADR-0009; FALSE: the wait is settled in a later step, and a mark in between cannot see it
  MaxAdmin,       \* unpark and resolve actions, in all
  None            \* a model value: no row held

ASSUME Deps \in [Msgs -> SUBSET Msgs]
ASSUME Arrives \subseteq Msgs
ASSUME MaxCrashes \in Nat
ASSUME Poison \subseteq Msgs
ASSUME MaxAttempts \in Nat
ASSUME BlockOnBackoff \in BOOLEAN
ASSUME MaxAdmin \in Nat
ASSUME WaitsOnDependencies \in BOOLEAN
ASSUME WaitExpires \in BOOLEAN

VARIABLES
  part,       \* [Msgs -> Slots], fixed at the start: any cut is allowed
  received,   \* SUBSET Msgs, the rows
  recvPos,    \* [Msgs -> Nat], arrival order, 0 until received
  nextRecv,
  processed,  \* SUBSET Msgs, the committed marks
  effects,    \* [Msgs -> Nat], the subscriber's committed writes, per message
  holding,    \* [Slots -> Msgs \cup {None}], the row the slot's open transaction holds
  procOrder,  \* Seq(Msgs), the order marks were committed in: processed_position
  crashes,
  attempts,   \* [Msgs -> Nat], failed attempts recorded
  due,        \* [Msgs -> BOOLEAN], FALSE while a failed message waits for its backoff
  parked,     \* SUBSET Msgs, taken out of the queue after MaxAttempts
  resolved,   \* SUBSET Msgs, marked processed by an operator, without effects
  admin,      \* operator actions taken
  waiting,    \* [Msgs -> Msgs \cup {None}], the dependency a row set aside waits for
  expired     \* SUBSET Msgs, parked because their wait ran out

vars == <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes,
          attempts, due, parked, resolved, admin, waiting, expired>>

Locked == {holding[s] : s \in Slots} \ {None}

Init ==
  /\ part \in [Msgs -> Slots]
  /\ received = {}
  /\ recvPos = [m \in Msgs |-> 0]
  /\ nextRecv = 1
  /\ processed = {}
  /\ effects = [m \in Msgs |-> 0]
  /\ holding = [s \in Slots |-> None]
  /\ procOrder = <<>>
  /\ crashes = 0
  /\ attempts = [m \in Msgs |-> 0]
  /\ due = [m \in Msgs |-> TRUE]
  /\ parked = {}
  /\ resolved = {}
  /\ admin = 0
  /\ waiting = [m \in Msgs |-> None]
  /\ expired = {}

(* ------------------------------------------------------------------------ *)
(* Receiving                                                                  *)

\* INSERT ... ON CONFLICT DO NOTHING, in a transaction of its own.  An
\* identity that arrives again is the same row: nothing happens, so the
\* model has no step for it.  ReceiveAs takes the arrival position as an
\* argument so that a trace of the implementation can supply the one the
\* sequence assigned; Receive is the model's own, dense numbering.
ReceiveAs(m, p) ==
  /\ m \in Arrives
  /\ m \notin received
  /\ received' = received \cup {m}
  /\ recvPos' = [recvPos EXCEPT ![m] = p]
  /\ nextRecv' = IF p >= nextRecv THEN p + 1 ELSE nextRecv
  /\ UNCHANGED <<part, processed, effects, holding, procOrder, crashes, attempts, due, parked, resolved, admin,
                waiting, expired>>

Receive(m) == ReceiveAs(m, nextRecv)

(* ------------------------------------------------------------------------ *)
(* Processing                                                                 *)

DepsProcessed(m) == Deps[m] \subseteq processed

\* The queue: unprocessed, unparked rows among `rows` that wait for nothing
\* and that no other transaction holds — FOR UPDATE SKIP LOCKED, parked_at
\* IS NULL, waiting_for IS NULL.  A failed row not yet due is in the queue
\* and holds it, unless BlockOnBackoff is off and it is stepped over.  The
\* rows are the received ones; a trace of the implementation passes those
\* its statement's snapshot could see, as the outbox's FetchWith takes the
\* logged horizon.
CandidatesIn(d, rows) ==
  {m \in rows : /\ m \notin processed /\ m \notin parked /\ waiting[m] = None
               /\ m \notin Locked /\ part[m] = d
               /\ (BlockOnBackoff \/ due[m])}
Candidates(d) == CandidatesIn(d, received)

Oldest(S) == CHOOSE m \in S : \A n \in S : recvPos[m] <= recvPos[n]

\* The walk looks at the head of the queue only: it is taken when due and
\* its dependencies are processed; set aside when they are not; left where
\* it is, holding the queue, when it waits for its backoff.
Ready(m) == due[m] /\ DepsProcessed(m)
TakesIn(d, rows) == {m \in CandidatesIn(d, rows) : m = Oldest(CandidatesIn(d, rows)) /\ Ready(m)}
Takes(d) == TakesIn(d, received)

\* SELECT ... ORDER BY received_position LIMIT 1 FOR UPDATE SKIP LOCKED, then
\* the dependency check, inside the dispatcher's transaction.
FetchFrom(d, rows) ==
  /\ holding[d] = None
  /\ TakesIn(d, rows) # {}
  /\ holding' = [holding EXCEPT ![d] = CHOOSE m \in TakesIn(d, rows) : TRUE]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, procOrder, crashes,
                attempts, due, parked, resolved, admin, waiting, expired>>
Fetch(d) == FetchFrom(d, received)

\* The head of the queue depends on a message not yet processed: it is set
\* aside to wait for that one, out of the queue, and the walk goes on to
\* the next head.  Which unprocessed dependency is named is the
\* implementation's choice; a trace supplies it.
WaitIn(d, rows, m, dep) ==
  /\ WaitsOnDependencies
  /\ holding[d] = None
  /\ m \in CandidatesIn(d, rows)
  /\ m = Oldest(CandidatesIn(d, rows))
  /\ dep \in Deps[m] \ processed
  /\ waiting' = [waiting EXCEPT ![m] = dep]
  \* without the lock the wait is not committed yet: the row stays held
  /\ holding' = IF AtomicWait THEN holding ELSE [holding EXCEPT ![d] = m]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, procOrder, crashes,
                attempts, due, parked, resolved, admin, expired>>
Wait(d) == \E m \in Msgs, dep \in Msgs : WaitIn(d, received, m, dep)

\* The wait commits, whatever was marked in between (AtomicWait = FALSE).
Settle(d) ==
  /\ ~AtomicWait
  /\ holding[d] # None
  /\ waiting[holding[d]] # None
  /\ holding' = [holding EXCEPT ![d] = None]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, procOrder, crashes,
                attempts, due, parked, resolved, admin, waiting, expired>>

\* What waited for m comes back into the queue: waiting_for IS NULL again.
\* A wait not committed yet -- its row still held -- is not seen.
Woken(m) == [n \in Msgs |-> IF waiting[n] = m /\ (AtomicWait \/ n \notin Locked) THEN None ELSE waiting[n]]

\* The subscriber ran with the dispatcher's transaction; its writes, the
\* mark and the waking of what waited for the message commit together.  A
\* poison message never gets here.
Commit(d) ==
  /\ holding[d] # None
  /\ waiting[holding[d]] = None
  /\ holding[d] \notin Poison
  /\ processed' = processed \cup {holding[d]}
  /\ effects' = [effects EXCEPT ![holding[d]] = @ + 1]
  /\ procOrder' = Append(procOrder, holding[d])
  /\ waiting' = Woken(holding[d])
  /\ holding' = [holding EXCEPT ![d] = None]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, crashes, attempts, due, parked, resolved, admin, expired>>

\* The subscriber returned an error: its writes roll back to the savepoint,
\* the attempt is recorded in the same transaction, and the row is not due
\* again until its backoff has passed.  At MaxAttempts the row is parked.  A
\* poison message always fails; any other may, within the budget.  Attempts
\* are counted only where the count decides something: with MaxAttempts = 0
\* a poison message fails for ever, and an unbounded counter would make the
\* state space infinite.
Fail(d) ==
  /\ holding[d] # None
  /\ waiting[holding[d]] = None
  /\ LET m == holding[d] IN
     /\ m \in Poison \/ crashes < MaxCrashes
     /\ crashes' = IF m \in Poison THEN crashes ELSE crashes + 1
     /\ attempts' = [attempts EXCEPT ![m] = IF MaxAttempts > 0 THEN @ + 1 ELSE @]
     /\ due' = [due EXCEPT ![m] = FALSE]
     /\ parked' = IF MaxAttempts > 0 /\ attempts[m] + 1 >= MaxAttempts THEN parked \cup {m} ELSE parked
  /\ holding' = [holding EXCEPT ![d] = None]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, procOrder, resolved, admin,
                waiting, expired>>

\* The backoff of a failed row has passed.
Elapse(m) ==
  /\ ~due[m]
  /\ due' = [due EXCEPT ![m] = TRUE]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes,
                attempts, parked, resolved, admin, waiting, expired>>

\* The wait of a row set aside has run out: it is parked, with the
\* dependency named, and the partition owes it nothing more.
Expire(m) ==
  /\ WaitExpires
  /\ waiting[m] # None
  /\ waiting' = [waiting EXCEPT ![m] = None]
  /\ parked' = parked \cup {m}
  /\ expired' = expired \cup {m}
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes,
                attempts, due, resolved, admin>>

\* The dispatcher dies: the transaction rolls back, writes and mark alike,
\* nothing is recorded, and the row is free again.
Crash(d) ==
  /\ holding[d] # None
  /\ waiting[holding[d]] = None
  /\ crashes < MaxCrashes
  /\ crashes' = crashes + 1
  /\ holding' = [holding EXCEPT ![d] = None]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, procOrder,
                attempts, due, parked, resolved, admin, waiting, expired>>

(* ------------------------------------------------------------------------ *)
(* Operators                                                                  *)

\* A parked row is given another go: attempts reset, due at once, back in
\* the queue; one whose wait ran out waits again if its dependency is still
\* unprocessed.
Unpark(m) ==
  /\ m \in parked
  /\ admin < MaxAdmin
  /\ admin' = admin + 1
  /\ parked' = parked \ {m}
  /\ expired' = expired \ {m}
  /\ attempts' = [attempts EXCEPT ![m] = 0]
  /\ due' = [due EXCEPT ![m] = TRUE]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes, resolved, waiting>>

\* A parked row is marked processed by hand, without the subscriber's
\* effects; what waited for it comes back into the queue.
Resolve(m) ==
  /\ m \in parked
  /\ admin < MaxAdmin
  /\ admin' = admin + 1
  /\ parked' = parked \ {m}
  /\ expired' = expired \ {m}
  /\ processed' = processed \cup {m}
  /\ resolved' = resolved \cup {m}
  /\ procOrder' = Append(procOrder, m)
  /\ waiting' = Woken(m)
  /\ UNCHANGED <<part, received, recvPos, nextRecv, effects, holding, crashes, attempts, due>>

(* ------------------------------------------------------------------------ *)

Next ==
  \/ \E m \in Msgs : Receive(m) \/ Elapse(m) \/ Expire(m) \/ Unpark(m) \/ Resolve(m)
  \/ \E d \in Slots : Fetch(d) \/ Wait(d) \/ Settle(d) \/ Commit(d) \/ Fail(d) \/ Crash(d)

\* Every message that arrives at all arrives eventually; every backoff
\* passes, and every wait runs out where waits do; every dispatcher keeps
\* walking its queue and finishes what it took, one way or the other.
Fairness ==
  /\ \A m \in Msgs : WF_vars(Receive(m)) /\ WF_vars(Elapse(m)) /\ WF_vars(Expire(m))
  /\ \A d \in Slots : WF_vars(Fetch(d) \/ Wait(d) \/ Settle(d)) /\ WF_vars(Commit(d) \/ Fail(d))

Spec == Init /\ [][Next]_vars /\ Fairness

(* ------------------------------------------------------------------------ *)
(* Properties                                                                 *)

TypeOK ==
  /\ part \in [Msgs -> Slots]
  /\ received \subseteq Msgs
  /\ recvPos \in [Msgs -> Nat]
  /\ nextRecv \in Nat
  /\ processed \subseteq Msgs
  /\ effects \in [Msgs -> Nat]
  /\ holding \in [Slots -> Msgs \cup {None}]
  /\ procOrder \in Seq(Msgs)
  /\ crashes \in Nat
  /\ attempts \in [Msgs -> Nat]
  /\ due \in [Msgs -> BOOLEAN]
  /\ parked \subseteq received
  /\ resolved \subseteq processed
  /\ admin \in Nat
  /\ waiting \in [Msgs -> Msgs \cup {None}]
  /\ expired \subseteq parked

\* The subscriber's writes for a message are committed at most once, however
\* many dispatchers compete and however often the message arrives.
EffectsOnce == \A m \in Msgs : effects[m] <= 1

\* ... and exactly when the message is marked: writes and mark are one
\* transaction.  This is the guarantee the inbox exists for.  A message an
\* operator resolved is marked without effects, by that operator's decision.
EffectsIffProcessed == \A m \in Msgs : (m \in processed /\ m \notin resolved) <=> (effects[m] = 1)

\* A parked message is out of the queue and not processed, and got there by
\* exhausting its attempts or its wait.
ParkedIsAside ==
  /\ parked \cap processed = {}
  /\ \A m \in parked : (MaxAttempts > 0 /\ attempts[m] = MaxAttempts) \/ m \in expired

\* A message set aside waits for a dependency of its own that is not yet
\* processed — never for one already processed, which nothing would wake it
\* from — and is neither processed nor parked meanwhile.
WaitingIsAside ==
  \A m \in Msgs : waiting[m] # None =>
    /\ waiting[m] \in Deps[m] /\ waiting[m] \notin processed
    /\ m \notin processed /\ m \notin parked

\* A message is processed only after everything it depends on.
CausalOrder == \A m \in processed : Deps[m] \subseteq processed

\* Two slots never hold the same row.
HeldOnce ==
  \A d1, d2 \in Slots :
    (d1 # d2 /\ holding[d1] # None) => holding[d1] # holding[d2]

ProcessedWasReceived == processed \subseteq received

Index(m) == CHOOSE i \in 1..Len(procOrder) : procOrder[i] = m

\* Messages of one slot without dependencies, processed by the subscriber
\* rather than resolved by hand, are processed in order of arrival: one
\* dispatcher per slot at a time is what makes this a guarantee rather than
\* a condition.  A failed message waiting for its backoff holds the slot for
\* this reason; stepping over it, BlockOnBackoff = FALSE, breaks the order,
\* which InboxSkipNotDue.cfg shows.
ArrivalOrder ==
  \A a, b \in processed \ resolved :
    (part[a] = part[b] /\ Deps[a] = {} /\ Deps[b] = {} /\ recvPos[a] < recvPos[b]) => Index(a) < Index(b)

\* A received message whose dependencies are processed is eventually
\* processed itself, unless its subscriber never succeeds — or parked, where
\* a wait that ran out or attempts that ran out put it there.  A message
\* waiting on something that never arrives is outside this claim, by design;
\* what the claim does cover is that neither it nor a poison message stands
\* in the way of the others: the first is set aside, the second parked.
\* Without setting aside, a message ahead of its dependency holds its
\* partition for ever, which InboxNoWaiting.cfg shows; without parking a
\* poison message does, which InboxPoisonNoParking.cfg shows.
EventuallyProcessed ==
  \A m \in Msgs \ Poison : (m \in received /\ Deps[m] \subseteq processed) ~> (m \in processed \/ m \in parked)

\* With parking, a poison message is eventually out of the way.
EventuallyParked ==
  \A m \in Poison : (m \in received /\ Deps[m] \subseteq processed) ~> (m \in parked \/ m \in resolved)

\* With both parking and expiring waits, every received message ends up
\* processed or parked: nothing waits or fails for ever.
EventuallyAside ==
  (MaxAttempts > 0 /\ WaitExpires) => \A m \in Msgs : (m \in received) ~> (m \in processed \/ m \in parked)

=============================================================================
