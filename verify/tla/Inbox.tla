----------------------------- MODULE Inbox -----------------------------
(***************************************************************************)
(* The Transactional Inbox of `crates/inbox`, as a protocol.               *)
(*                                                                         *)
(* Messages arrive, each under an identity; a dispatcher takes the oldest  *)
(* unprocessed message of its partition whose causal dependencies are      *)
(* processed, runs the subscriber inside its own transaction, and marks    *)
(* the message processed in that transaction.  Several dispatchers may     *)
(* serve one partition.  Dispatchers crash and subscribers fail at any     *)
(* point.                                                                  *)
(*                                                                         *)
(* PostgreSQL is abstracted to what the protocol relies on:                *)
(*   - INSERT ... ON CONFLICT DO NOTHING: an identity is one row, however  *)
(*     often it arrives, and rows are ordered by arrival;                  *)
(*   - FOR UPDATE SKIP LOCKED: a row held by an open transaction is        *)
(*     stepped over by every other one;                                    *)
(*   - the dependency check reads committed marks only;                    *)
(*   - the subscriber's writes and the mark commit together, or not at     *)
(*     all.                                                                *)
(*                                                                         *)
(* A message whose dependencies never arrive is stepped over for ever, by  *)
(* design for now.  A subscriber that fails rolls its writes back to a     *)
(* savepoint; the attempt is recorded in the same transaction, and the     *)
(* message is not due again until its backoff has passed, during which it  *)
(* holds its partition: stepping over it would lose the order.  After      *)
(* MaxAttempts failures the message is parked, taken out of the queue but  *)
(* kept in the table, and the partition flows; with MaxAttempts = 0 it     *)
(* never is, and a message whose subscriber never succeeds blocks its      *)
(* partition for ever, which InboxPoisonNoParking.cfg shows.  An operator  *)
(* may unpark a message, to try again, or resolve it, to mark it processed *)
(* without effects.  The model bounds failures and operator actions so     *)
(* that liveness can be stated.                                            *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
  Msgs,           \* messages, by identity
  Deps,           \* [Msgs -> SUBSET Msgs], causal dependencies; acyclic
  Arrives,        \* SUBSET Msgs: the messages that ever arrive
  Workers,        \* partitions, by the hash of the partition key
  Dispatchers,    \* SUBSET (Workers \X Nat): several dispatchers may serve one worker
  MaxCrashes,     \* crashes and subscriber failures, in all
  SkipIneligible, \* TRUE: step over a message whose dependencies are not processed (LIMIT 1 OFFSET n)
  Poison,         \* SUBSET Msgs: messages whose subscriber never succeeds
  MaxAttempts,    \* failed attempts after which a message is parked; 0: never, retry for ever
  BlockOnBackoff, \* TRUE: a failed message not yet due holds its partition; FALSE: it is stepped over
  MaxAdmin,       \* unpark and resolve actions, in all
  None            \* a model value: no row held

ASSUME Deps \in [Msgs -> SUBSET Msgs]
ASSUME Arrives \subseteq Msgs
ASSUME Dispatchers \subseteq Workers \X Nat
ASSUME MaxCrashes \in Nat
ASSUME Poison \subseteq Msgs
ASSUME MaxAttempts \in Nat
ASSUME BlockOnBackoff \in BOOLEAN
ASSUME MaxAdmin \in Nat

VARIABLES
  part,       \* [Msgs -> Workers], fixed at the start: any partitioning is allowed
  received,   \* SUBSET Msgs, the rows
  recvPos,    \* [Msgs -> Nat], arrival order, 0 until received
  nextRecv,
  processed,  \* SUBSET Msgs, the committed marks
  effects,    \* [Msgs -> Nat], the subscriber's committed writes, per message
  holding,    \* [Dispatchers -> Msgs \cup {None}], the row a dispatcher's open transaction holds
  procOrder,  \* Seq(Msgs), the order marks were committed in: processed_position
  crashes,
  attempts,   \* [Msgs -> Nat], failed attempts recorded
  due,        \* [Msgs -> BOOLEAN], FALSE while a failed message waits for its backoff
  parked,     \* SUBSET Msgs, taken out of the queue after MaxAttempts
  resolved,   \* SUBSET Msgs, marked processed by an operator, without effects
  admin       \* operator actions taken

vars == <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes,
          attempts, due, parked, resolved, admin>>

Locked == {holding[d] : d \in Dispatchers} \ {None}

Init ==
  /\ part \in [Msgs -> Workers]
  /\ received = {}
  /\ recvPos = [m \in Msgs |-> 0]
  /\ nextRecv = 1
  /\ processed = {}
  /\ effects = [m \in Msgs |-> 0]
  /\ holding = [d \in Dispatchers |-> None]
  /\ procOrder = <<>>
  /\ crashes = 0
  /\ attempts = [m \in Msgs |-> 0]
  /\ due = [m \in Msgs |-> TRUE]
  /\ parked = {}
  /\ resolved = {}
  /\ admin = 0

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
  /\ UNCHANGED <<part, processed, effects, holding, procOrder, crashes, attempts, due, parked, resolved, admin>>

Receive(m) == ReceiveAs(m, nextRecv)

(* ------------------------------------------------------------------------ *)
(* Processing                                                                 *)

DepsProcessed(m) == Deps[m] \subseteq processed

\* Unprocessed, unparked rows among `rows` that no other transaction holds:
\* FOR UPDATE SKIP LOCKED, and parked_at IS NULL.  The rows are the received
\* ones; a trace of the implementation passes those its statement's snapshot
\* could see, as the outbox's FetchWith takes the logged horizon.
CandidatesIn(d, rows) == {m \in rows : m \notin processed /\ m \notin parked /\ m \notin Locked /\ part[m] = d[1]}
Candidates(d) == CandidatesIn(d, received)

Oldest(S) == CHOOSE m \in S : \A n \in S : recvPos[m] <= recvPos[n]

\* A row may be taken when it is due and its dependencies are processed.
Ready(m) == due[m] /\ DepsProcessed(m)

\* Nothing not yet due stands before m: a failed row waiting for its backoff
\* holds everything behind it, so that the order survives the failure.
AheadIn(d, m, rows) == \A n \in CandidatesIn(d, rows) : (BlockOnBackoff /\ ~due[n]) => recvPos[m] < recvPos[n]

\* The row the dispatcher takes: the oldest ready candidate nothing holds
\* back, stepping over those whose dependencies are not processed; or the
\* oldest candidate alone, when not stepping over.
TakesIn(d, rows) ==
  IF SkipIneligible
  THEN {m \in CandidatesIn(d, rows) :
          /\ Ready(m) /\ AheadIn(d, m, rows)
          /\ \A n \in CandidatesIn(d, rows) : (Ready(n) /\ AheadIn(d, n, rows)) => recvPos[m] <= recvPos[n]}
  ELSE {m \in CandidatesIn(d, rows) : m = Oldest(CandidatesIn(d, rows)) /\ Ready(m)}
Takes(d) == TakesIn(d, received)

\* SELECT ... ORDER BY received_position LIMIT 1 OFFSET n FOR UPDATE SKIP LOCKED,
\* then the dependency check, inside the dispatcher's transaction.
FetchFrom(d, rows) ==
  /\ holding[d] = None
  /\ CandidatesIn(d, rows) # {}
  /\ TakesIn(d, rows) # {}
  /\ holding' = [holding EXCEPT ![d] = CHOOSE m \in TakesIn(d, rows) : TRUE]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, procOrder, crashes,
                attempts, due, parked, resolved, admin>>
Fetch(d) == FetchFrom(d, received)

\* The subscriber ran with the dispatcher's transaction; its writes and the
\* mark commit together.  A poison message never gets here.
Commit(d) ==
  /\ holding[d] # None
  /\ holding[d] \notin Poison
  /\ processed' = processed \cup {holding[d]}
  /\ effects' = [effects EXCEPT ![holding[d]] = @ + 1]
  /\ procOrder' = Append(procOrder, holding[d])
  /\ holding' = [holding EXCEPT ![d] = None]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, crashes, attempts, due, parked, resolved, admin>>

\* The subscriber returned an error: its writes roll back to the savepoint,
\* the attempt is recorded in the same transaction, and the row is not due
\* again until its backoff has passed.  At MaxAttempts the row is parked.  A
\* poison message always fails; any other may, within the budget.  Attempts
\* are counted only where the count decides something: with MaxAttempts = 0
\* a poison message fails for ever, and an unbounded counter would make the
\* state space infinite.
Fail(d) ==
  /\ holding[d] # None
  /\ LET m == holding[d] IN
     /\ m \in Poison \/ crashes < MaxCrashes
     /\ crashes' = IF m \in Poison THEN crashes ELSE crashes + 1
     /\ attempts' = [attempts EXCEPT ![m] = IF MaxAttempts > 0 THEN @ + 1 ELSE @]
     /\ due' = [due EXCEPT ![m] = FALSE]
     /\ parked' = IF MaxAttempts > 0 /\ attempts[m] + 1 >= MaxAttempts THEN parked \cup {m} ELSE parked
  /\ holding' = [holding EXCEPT ![d] = None]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, procOrder, resolved, admin>>

\* The backoff of a failed row has passed.
Elapse(m) ==
  /\ ~due[m]
  /\ due' = [due EXCEPT ![m] = TRUE]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes,
                attempts, parked, resolved, admin>>

\* The dispatcher dies: the transaction rolls back, writes and mark alike,
\* nothing is recorded, and the row is free again.
Crash(d) ==
  /\ holding[d] # None
  /\ crashes < MaxCrashes
  /\ crashes' = crashes + 1
  /\ holding' = [holding EXCEPT ![d] = None]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, procOrder,
                attempts, due, parked, resolved, admin>>

(* ------------------------------------------------------------------------ *)
(* Operators                                                                  *)

\* A parked row is given another go: attempts reset, due at once.
Unpark(m) ==
  /\ m \in parked
  /\ admin < MaxAdmin
  /\ admin' = admin + 1
  /\ parked' = parked \ {m}
  /\ attempts' = [attempts EXCEPT ![m] = 0]
  /\ due' = [due EXCEPT ![m] = TRUE]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes, resolved>>

\* A parked row is marked processed by hand, without the subscriber's
\* effects; what depends on it may go ahead.
Resolve(m) ==
  /\ m \in parked
  /\ admin < MaxAdmin
  /\ admin' = admin + 1
  /\ parked' = parked \ {m}
  /\ processed' = processed \cup {m}
  /\ resolved' = resolved \cup {m}
  /\ procOrder' = Append(procOrder, m)
  /\ UNCHANGED <<part, received, recvPos, nextRecv, effects, holding, crashes, attempts, due>>

(* ------------------------------------------------------------------------ *)

Next ==
  \/ \E m \in Msgs : Receive(m) \/ Elapse(m) \/ Unpark(m) \/ Resolve(m)
  \/ \E d \in Dispatchers : Fetch(d) \/ Commit(d) \/ Fail(d) \/ Crash(d)

\* Every message that arrives at all arrives eventually; every backoff
\* passes; every dispatcher keeps polling and finishes what it took, one
\* way or the other.
Fairness ==
  /\ \A m \in Msgs : WF_vars(Receive(m)) /\ WF_vars(Elapse(m))
  /\ \A d \in Dispatchers : WF_vars(Fetch(d)) /\ WF_vars(Commit(d) \/ Fail(d))

Spec == Init /\ [][Next]_vars /\ Fairness

(* ------------------------------------------------------------------------ *)
(* Properties                                                                 *)

TypeOK ==
  /\ part \in [Msgs -> Workers]
  /\ received \subseteq Msgs
  /\ recvPos \in [Msgs -> Nat]
  /\ nextRecv \in Nat
  /\ processed \subseteq Msgs
  /\ effects \in [Msgs -> Nat]
  /\ holding \in [Dispatchers -> Msgs \cup {None}]
  /\ procOrder \in Seq(Msgs)
  /\ crashes \in Nat
  /\ attempts \in [Msgs -> Nat]
  /\ due \in [Msgs -> BOOLEAN]
  /\ parked \subseteq received
  /\ resolved \subseteq processed
  /\ admin \in Nat

\* The subscriber's writes for a message are committed at most once, however
\* many dispatchers compete and however often the message arrives.
EffectsOnce == \A m \in Msgs : effects[m] <= 1

\* ... and exactly when the message is marked: writes and mark are one
\* transaction.  This is the guarantee the inbox exists for.  A message an
\* operator resolved is marked without effects, by that operator's decision.
EffectsIffProcessed == \A m \in Msgs : (m \in processed /\ m \notin resolved) <=> (effects[m] = 1)

\* A parked message is out of the queue and not processed, and got there by
\* exhausting its attempts.
ParkedIsAside ==
  /\ parked \cap processed = {}
  /\ \A m \in parked : MaxAttempts > 0 /\ attempts[m] = MaxAttempts

\* A message is processed only after everything it depends on.
CausalOrder == \A m \in processed : Deps[m] \subseteq processed

\* Two dispatchers never hold the same row.
HeldOnce ==
  \A d1, d2 \in Dispatchers :
    (d1 # d2 /\ holding[d1] # None) => holding[d1] # holding[d2]

ProcessedWasReceived == processed \subseteq received

Index(m) == CHOOSE i \in 1..Len(procOrder) : procOrder[i] = m

\* Messages of one partition without dependencies, processed by the
\* subscriber rather than resolved by hand, are processed in order of
\* arrival, given one dispatcher per partition.  A failed message waiting
\* for its backoff holds the partition for this reason; stepping over it,
\* BlockOnBackoff = FALSE, breaks the order, which InboxSkipNotDue.cfg shows.
ArrivalOrder ==
  \A a, b \in processed \ resolved :
    (part[a] = part[b] /\ Deps[a] = {} /\ Deps[b] = {} /\ recvPos[a] < recvPos[b]) => Index(a) < Index(b)

\* A received message whose dependencies are processed is eventually
\* processed itself, unless its subscriber never succeeds.  A message waiting
\* on something that never arrives is outside this claim, by design; what
\* the claim does cover is that neither it nor a poison message stands in
\* the way of the others: the first is stepped over, the second parked.
\* Without parking a poison message holds its partition for ever, which
\* InboxPoisonNoParking.cfg shows.
EventuallyProcessed ==
  \A m \in Msgs \ Poison : (m \in received /\ Deps[m] \subseteq processed) ~> (m \in processed)

\* With parking, a poison message is eventually out of the way.
EventuallyParked ==
  \A m \in Poison : (m \in received /\ Deps[m] \subseteq processed) ~> (m \in parked \/ m \in resolved)

=============================================================================
