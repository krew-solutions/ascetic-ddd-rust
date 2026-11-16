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
(*                                                                         *)
(* Every message belongs to a Tenant, TenantOf, numbered from 1.  By       *)
(* default all belong to one, and a slot is one FIFO, as before ADR-0041. *)
(* With Rotates, a slot's holder serves its Tenants in turn (ADR-0041     *)
(* decision 2):                                                            *)
(*   - the head of (slot, Tenant) is that Tenant's oldest row in the       *)
(*     slot's queue;                                                       *)
(*   - the slot's cursor names the Tenant served last;                    *)
(*   - the holder takes the head of the first Tenant after the cursor,    *)
(*     cyclically, whose head is due;                                      *)
(*   - a head in its backoff holds only its own (slot, Tenant), so the    *)
(*     other Tenants of the slot go on (decision 3).                       *)
(* The cursor moves when a head is taken or set aside, and is kept when    *)
(* the dispatcher crashes, as decision 3 has it on a rollback.  The bound *)
(* on a walk, K, and the dormancy of a slot are the statements' matter,    *)
(* in InboxPg.tla: here a walk sees every Tenant of its slot, and the      *)
(* order in which heads are taken is the same.  A step of its own, Pass,   *)
(* moves the cursor past Tenants with no due head, as a walk bounded by K  *)
(* does when it finds none.  Rotates and TenantOf are                      *)
(* definitions, so that every instance of this module keeps the FIFO       *)
(* unless a configuration replaces them.                                   *)
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
  Unreadable,     \* SUBSET Msgs: rows the dispatcher cannot read or write for their own values,
                  \* which the intake did not foresee or did not write (ADR-0037 decision 6);
                  \* parked at once
  Permanent,      \* SUBSET Msgs: messages whose subscriber says no retry will succeed
                  \* (ADR-0010): parked at once, the attempt recorded
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
ASSUME Unreadable \subseteq Msgs
ASSUME Permanent \subseteq Msgs

\* The Tenant of each message, numbered from 1, and whether a slot serves
\* its Tenants in turn.  Definitions, not constants: a configuration
\* replaces them (`TenantOf <- ...`, `Rotates <- ...`), and every other
\* instance keeps one Tenant and the FIFO.
TenantOf == [m \in Msgs |-> 1]
Rotates == FALSE

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
\* The highest Tenant's number: the cursor cycles over 1..NT.
NT == Max({TenantOf[m] : m \in Msgs})

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
  expired,    \* SUBSET Msgs, parked because their wait ran out
  unread,     \* SUBSET Msgs, rows the dispatcher cannot read yet: Unreadable until superseded
  cursor      \* [Slots -> 0..NT], the Tenant a slot served last; 0: none yet (ADR-0041)

vars == <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes,
          attempts, due, parked, resolved, admin, waiting, expired, unread, cursor>>

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
  /\ unread = Unreadable
  /\ cursor = [s \in Slots |-> 0]

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
                waiting, expired, unread, cursor>>

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

\* The head of (slot, Tenant): the Tenant's oldest row in the slot's queue.
HeadOf(d, rows, t) == Oldest({m \in CandidatesIn(d, rows) : TenantOf[m] = t})

\* The Tenants with rows in the slot's queue, and those whose head is due:
\* a head in its backoff holds its own (slot, Tenant), not the slot.
TenantsIn(d, rows) == {TenantOf[m] : m \in CandidatesIn(d, rows)}
DueTenants(d, rows) == {t \in TenantsIn(d, rows) : due[HeadOf(d, rows, t)]}

\* How far Tenant t lies after the cursor c, cyclically over 1..NT.
Dist(c, t) == IF t > c THEN t - c ELSE t + NT - c

\* The first Tenant after the slot's cursor whose head is due.
NextTenant(d, rows) ==
  CHOOSE t \in DueTenants(d, rows) : \A u \in DueTenants(d, rows) : Dist(cursor[d], t) <= Dist(cursor[d], u)

\* The head the walk looks at.  As a FIFO: the slot's oldest row, which
\* holds the slot when it waits for its backoff.  Rotating: the head of the
\* first Tenant after the cursor whose head is due.  Empty when there is
\* none.
SlotHeads(d, rows) ==
  IF Rotates
  THEN IF DueTenants(d, rows) = {} THEN {} ELSE {HeadOf(d, rows, NextTenant(d, rows))}
  ELSE IF CandidatesIn(d, rows) = {} THEN {} ELSE {Oldest(CandidatesIn(d, rows))}

\* The cursor once m has been taken or set aside in slot d.
Turned(d, m) == IF Rotates THEN [cursor EXCEPT ![d] = TenantOf[m]] ELSE cursor

\* The walk looks at the head only: it is taken when due and its
\* dependencies are processed; set aside when they are not; left where it
\* is, holding the queue -- the slot's, or with Rotates its Tenant's -- when
\* it waits for its backoff.
Ready(m) == due[m] /\ DepsProcessed(m)
TakesIn(d, rows) == {m \in SlotHeads(d, rows) : Ready(m)}
Takes(d) == TakesIn(d, received)

\* SELECT ... ORDER BY received_position LIMIT 1 FOR UPDATE SKIP LOCKED, then
\* the dependency check, inside the dispatcher's transaction.
FetchFrom(d, rows) ==
  /\ holding[d] = None
  /\ TakesIn(d, rows) # {}
  /\ LET m == CHOOSE m \in TakesIn(d, rows) : TRUE IN
     /\ holding' = [holding EXCEPT ![d] = m]
     /\ cursor' = Turned(d, m)
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, procOrder, crashes,
                attempts, due, parked, resolved, admin, waiting, expired, unread>>
Fetch(d) == FetchFrom(d, received)

\* The head of the queue depends on a message not yet processed: it is set
\* aside to wait for that one, out of the queue, and the walk goes on to
\* the next head.  Which unprocessed dependency is named is the
\* implementation's choice; a trace supplies it.
WaitIn(d, rows, m, dep) ==
  /\ WaitsOnDependencies
  /\ holding[d] = None
  /\ m \in SlotHeads(d, rows)
  /\ dep \in Deps[m] \ processed
  /\ waiting' = [waiting EXCEPT ![m] = dep]
  \* without the lock the wait is not committed yet: the row stays held
  /\ holding' = IF AtomicWait THEN holding ELSE [holding EXCEPT ![d] = m]
  /\ cursor' = Turned(d, m)
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, procOrder, crashes,
                attempts, due, parked, resolved, admin, expired, unread>>
Wait(d) == \E m \in Msgs, dep \in Msgs : WaitIn(d, received, m, dep)

\* Rotating, a walk that examined some Tenants after the cursor and found no
\* due head among them moves the cursor past them and takes nothing: the
\* outcome `Passed`, or the end of a round (ADR-0041 decision 2).  A walk of
\* this module sees every Tenant and never needs it; a walk bounded by K
\* does, and InboxPg.tla's refinement maps it here.  It passes no due head:
\* every Tenant with one lies beyond t.
PassTo(d, t) ==
  /\ Rotates
  /\ holding[d] = None
  /\ \A u \in DueTenants(d, received) : Dist(cursor[d], t) < Dist(cursor[d], u)
  /\ cursor' = [cursor EXCEPT ![d] = t]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes,
                attempts, due, parked, resolved, admin, waiting, expired, unread>>
Pass(d) == \E t \in 1..NT : PassTo(d, t)

\* The wait commits, whatever was marked in between (AtomicWait = FALSE).
Settle(d) ==
  /\ ~AtomicWait
  /\ holding[d] # None
  /\ waiting[holding[d]] # None
  /\ holding' = [holding EXCEPT ![d] = None]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, procOrder, crashes,
                attempts, due, parked, resolved, admin, waiting, expired, unread, cursor>>

\* What waited for m comes back into the queue: waiting_for IS NULL again.
\* A wait not committed yet -- its row still held -- is not seen.
Woken(m) == [n \in Msgs |-> IF waiting[n] = m /\ (AtomicWait \/ n \notin Locked) THEN None ELSE waiting[n]]

\* What was parked when its wait for m ran out, keeping m: the same
\* statement unparks it (ADR-0036 R8).
WokenFromExpiry(m) == {n \in expired : waiting[n] = m}

\* The subscriber ran with the dispatcher's transaction; its writes, the
\* mark and the waking of what waited for the message commit together.  A
\* poison message never gets here.
Commit(d) ==
  /\ holding[d] # None
  /\ waiting[holding[d]] = None
  /\ holding[d] \notin Poison
  /\ holding[d] \notin Permanent
  /\ holding[d] \notin unread
  /\ processed' = processed \cup {holding[d]}
  /\ effects' = [effects EXCEPT ![holding[d]] = @ + 1]
  /\ procOrder' = Append(procOrder, holding[d])
  /\ waiting' = Woken(holding[d])
  /\ parked' = parked \ WokenFromExpiry(holding[d])
  /\ expired' = expired \ WokenFromExpiry(holding[d])
  /\ holding' = [holding EXCEPT ![d] = None]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, crashes, attempts, due, resolved, admin, unread, cursor>>

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
  /\ holding[d] \notin unread
  /\ holding[d] \notin Permanent
  /\ LET m == holding[d] IN
     /\ m \in Poison \/ crashes < MaxCrashes
     /\ crashes' = IF m \in Poison THEN crashes ELSE crashes + 1
     /\ attempts' = [attempts EXCEPT ![m] = IF MaxAttempts > 0 THEN @ + 1 ELSE @]
     /\ due' = [due EXCEPT ![m] = FALSE]
     /\ parked' = IF MaxAttempts > 0 /\ attempts[m] + 1 >= MaxAttempts THEN parked \cup {m} ELSE parked
  /\ holding' = [holding EXCEPT ![d] = None]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, procOrder, resolved, admin,
                waiting, expired, unread, cursor>>

\* The subscriber's verdict is that no retry will succeed: its writes roll
\* back, the attempt is recorded, and the row is parked at once, whatever
\* attempts it had left (ADR-0010).  Attempts are counted as Fail counts
\* them.
FailPermanent(d) ==
  /\ holding[d] # None
  /\ waiting[holding[d]] = None
  /\ holding[d] \notin unread
  /\ holding[d] \in Permanent
  /\ LET m == holding[d] IN
     /\ attempts' = [attempts EXCEPT ![m] = IF MaxAttempts > 0 THEN @ + 1 ELSE @]
     /\ due' = [due EXCEPT ![m] = FALSE]
     /\ parked' = parked \cup {m}
  /\ holding' = [holding EXCEPT ![d] = None]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, procOrder, crashes, resolved, admin,
                waiting, expired, unread, cursor>>

\* A row the dispatcher cannot read, or whose wait, failure or mark it
\* cannot write, for the row's own values, is parked at once in the
\* transaction that holds it, under its lock: the subscriber does not run,
\* nothing is recorded but the park, and the slot goes on (ADR-0037
\* decision 6).  One step: the park is a statement of the same transaction.
ParkAtOnce(d) ==
  /\ holding[d] # None
  /\ waiting[holding[d]] = None
  /\ holding[d] \in unread
  /\ parked' = parked \cup {holding[d]}
  /\ holding' = [holding EXCEPT ![d] = None]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, procOrder, crashes,
                attempts, due, resolved, admin, waiting, expired, unread, cursor>>

\* The backoff of a failed row has passed.
Elapse(m) ==
  /\ ~due[m]
  /\ due' = [due EXCEPT ![m] = TRUE]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes,
                attempts, parked, resolved, admin, waiting, expired, unread, cursor>>

\* The wait of a row set aside has run out: it is parked, with the
\* dependency named, and the partition owes it nothing more.  It keeps the
\* identity it waited for, so that the mark of that identity -- the hole
\* readmitted, or arrived late -- unparks it (ADR-0036 R8).
Expire(m) ==
  /\ WaitExpires
  /\ waiting[m] # None
  /\ m \notin expired
  /\ parked' = parked \cup {m}
  /\ expired' = expired \cup {m}
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes,
                attempts, due, resolved, admin, waiting, unread, cursor>>

\* The expiry as it was before ADR-0036 R8, forgetting the identity waited
\* for: InboxLateDepForgets.cfg puts it in Expire's place, and TLC finds the
\* dependent parked for good once the hole is filled.
ExpireForgetting(m) ==
  /\ WaitExpires
  /\ waiting[m] # None
  /\ m \notin expired
  /\ waiting' = [waiting EXCEPT ![m] = None]
  /\ parked' = parked \cup {m}
  /\ expired' = expired \cup {m}
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes,
                attempts, due, resolved, admin, unread, cursor>>

\* The dispatcher dies: the transaction rolls back, writes and mark alike,
\* nothing is recorded, and the row is free again.
Crash(d) ==
  /\ holding[d] # None
  /\ waiting[holding[d]] = None
  /\ crashes < MaxCrashes
  /\ crashes' = crashes + 1
  /\ holding' = [holding EXCEPT ![d] = None]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, procOrder,
                attempts, due, parked, resolved, admin, waiting, expired, unread, cursor>>

(* ------------------------------------------------------------------------ *)
(* Operators                                                                  *)

\* A parked row is given another go: attempts reset, due at once, back in
\* the queue, the identity it waited for cleared; one whose wait ran out
\* waits again if its dependency is still unprocessed.
Unpark(m) ==
  /\ m \in parked
  /\ admin < MaxAdmin
  /\ admin' = admin + 1
  /\ parked' = parked \ {m}
  /\ expired' = expired \ {m}
  /\ attempts' = [attempts EXCEPT ![m] = 0]
  /\ due' = [due EXCEPT ![m] = TRUE]
  /\ waiting' = [waiting EXCEPT ![m] = None]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes, resolved, unread, cursor>>

\* A parked row is marked processed by hand, without the subscriber's
\* effects; what waited for it comes back into the queue.
Resolve(m) ==
  /\ m \in parked
  /\ admin < MaxAdmin
  /\ admin' = admin + 1
  /\ parked' = (parked \ {m}) \ WokenFromExpiry(m)
  /\ expired' = (expired \ {m}) \ WokenFromExpiry(m)
  /\ processed' = processed \cup {m}
  /\ resolved' = resolved \cup {m}
  /\ procOrder' = Append(procOrder, m)
  /\ waiting' = [Woken(m) EXCEPT ![m] = None]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, effects, holding, crashes, attempts, due, unread, cursor>>

\* A row parked at once is superseded by an arrival at its identity: the
\* producer's repaired message replaces its values, and it is back in the
\* queue, readable (ADR-0037 decision 6).  The intake's statement takes the
\* row's lock, so a row a dispatcher holds is not superseded.  An outside
\* action, within the operators' budget.
Supersede(m) ==
  /\ m \in parked
  /\ m \in unread
  /\ m \notin Locked
  /\ admin < MaxAdmin
  /\ admin' = admin + 1
  /\ unread' = unread \ {m}
  /\ parked' = parked \ {m}
  /\ attempts' = [attempts EXCEPT ![m] = 0]
  /\ due' = [due EXCEPT ![m] = TRUE]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes,
                resolved, waiting, expired, cursor>>

(* ------------------------------------------------------------------------ *)

Next ==
  \/ \E m \in Msgs : Receive(m) \/ Elapse(m) \/ Expire(m) \/ Unpark(m) \/ Resolve(m) \/ Supersede(m)
  \/ \E d \in Slots : Fetch(d) \/ Wait(d) \/ Settle(d) \/ Commit(d) \/ Fail(d) \/ Crash(d) \/ ParkAtOnce(d)
                    \/ FailPermanent(d) \/ Pass(d)

\* Every message that arrives at all arrives eventually; every backoff
\* passes, and every wait runs out where waits do; every dispatcher keeps
\* walking its queue and finishes what it took, one way or the other.
Fairness ==
  /\ \A m \in Msgs : WF_vars(Receive(m)) /\ WF_vars(Elapse(m)) /\ WF_vars(Expire(m))
  /\ \A d \in Slots : WF_vars(Fetch(d) \/ Wait(d) \/ Settle(d)) /\ WF_vars(Commit(d) \/ Fail(d) \/ ParkAtOnce(d) \/ FailPermanent(d))

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
  /\ unread \subseteq Unreadable
  /\ cursor \in [Slots -> 0..NT]

\* The subscriber's writes for a message are committed at most once, however
\* many dispatchers compete and however often the message arrives.
EffectsOnce == \A m \in Msgs : effects[m] <= 1

\* ... and exactly when the message is marked: writes and mark are one
\* transaction.  This is the guarantee the inbox exists for.  A message an
\* operator resolved is marked without effects, by that operator's decision.
EffectsIffProcessed == \A m \in Msgs : (m \in processed /\ m \notin resolved) <=> (effects[m] = 1)

\* A parked message is out of the queue and not processed, and got there by
\* exhausting its attempts or its wait, by its own values, or by a verdict
\* that no retry will succeed.
ParkedIsAside ==
  /\ parked \cap processed = {}
  /\ \A m \in parked : (MaxAttempts > 0 /\ attempts[m] = MaxAttempts) \/ m \in expired \/ m \in Unreadable
                        \/ m \in Permanent

\* A message set aside waits for a dependency of its own that is not yet
\* processed — never for one already processed, which nothing would wake it
\* from — and is neither processed nor parked meanwhile, unless its wait ran
\* out: then it is parked, keeping the identity (ADR-0036 R8).
WaitingIsAside ==
  \A m \in Msgs : waiting[m] # None =>
    /\ waiting[m] \in Deps[m] /\ waiting[m] \notin processed
    /\ m \notin processed /\ (m \notin parked \/ m \in expired)

\* A row whose wait ran out does not stay parked once what it depends on is
\* processed: the hole filled, it is back in the queue (ADR-0036 R8).
WokenAfterExpiry ==
  \A m \in Msgs : (m \in expired /\ Deps[m] \subseteq processed) ~> (m \notin expired)

\* A message is processed only after everything it depends on.
CausalOrder == \A m \in processed : Deps[m] \subseteq processed

\* Two slots never hold the same row.
HeldOnce ==
  \A d1, d2 \in Slots :
    (d1 # d2 /\ holding[d1] # None) => holding[d1] # holding[d2]

ProcessedWasReceived == processed \subseteq received

Index(m) == CHOOSE i \in 1..Len(procOrder) : procOrder[i] = m

\* Messages of one slot and one Tenant without dependencies, processed by
\* the subscriber rather than resolved by hand, are processed in order of
\* arrival: one dispatcher per slot at a time is what makes this a guarantee
\* rather than a condition.  With one Tenant, as every instance has unless it
\* rotates, this is the order of the whole slot.  Across Tenants nothing is
\* ordered (ADR-0041 decision 1).  A failed message waiting for its backoff
\* holds its slot's queue, or its Tenant's share of it, for this reason;
\* stepping over it, BlockOnBackoff = FALSE, breaks the order, which
\* InboxSkipNotDue.cfg shows.  A row parked at once steps out of the order as
\* a parked row does: the slot flows past it, and superseded, it is processed
\* after the rows behind it (ADR-0037 decision 6).
ArrivalOrder ==
  \A a, b \in (processed \ resolved) \ Unreadable :
    (part[a] = part[b] /\ TenantOf[a] = TenantOf[b] /\ Deps[a] = {} /\ Deps[b] = {}
       /\ recvPos[a] < recvPos[b]) => Index(a) < Index(b)

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

\* A poison message that is never parked holds what its failure holds: as a
\* FIFO, its slot; rotating, its own (slot, Tenant) alone (ADR-0041
\* decision 3).  Every other message whose dependencies are processed is
\* eventually processed or parked.  InboxPoisonHoldsSlot.cfg, the FIFO,
\* must fail it.
HeldByPoison(m) == \E p \in Poison : part[p] = part[m] /\ TenantOf[p] = TenantOf[m]
EventuallyProcessedBesidePoison ==
  \A m \in Msgs \ Poison :
    ~HeldByPoison(m) => ((m \in received /\ Deps[m] \subseteq processed) ~> (m \in processed \/ m \in parked))

\* With parking, a poison message is eventually out of the way.
EventuallyParked ==
  \A m \in Poison : (m \in received /\ Deps[m] \subseteq processed) ~> (m \in parked \/ m \in resolved)

\* With both parking and expiring waits, every received message ends up
\* processed or parked: nothing waits or fails for ever.
EventuallyAside ==
  (MaxAttempts > 0 /\ WaitExpires) => \A m \in Msgs : (m \in received) ~> (m \in processed \/ m \in parked)

=============================================================================
