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
(* design for now; a message whose subscriber never succeeds blocks its    *)
(* partition, because the oldest eligible row is taken first.  The model   *)
(* bounds failures so that liveness can be stated; both facts are          *)
(* recorded in verify/tla/README.md.                                       *)
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
  None            \* a model value: no row held

ASSUME Deps \in [Msgs -> SUBSET Msgs]
ASSUME Arrives \subseteq Msgs
ASSUME Dispatchers \subseteq Workers \X Nat
ASSUME MaxCrashes \in Nat

VARIABLES
  part,       \* [Msgs -> Workers], fixed at the start: any partitioning is allowed
  received,   \* SUBSET Msgs, the rows
  recvPos,    \* [Msgs -> Nat], arrival order, 0 until received
  nextRecv,
  processed,  \* SUBSET Msgs, the committed marks
  effects,    \* [Msgs -> Nat], the subscriber's committed writes, per message
  holding,    \* [Dispatchers -> Msgs \cup {None}], the row a dispatcher's open transaction holds
  procOrder,  \* Seq(Msgs), the order marks were committed in: processed_position
  crashes

vars == <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes>>

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

(* ------------------------------------------------------------------------ *)
(* Receiving                                                                  *)

\* INSERT ... ON CONFLICT DO NOTHING, in a transaction of its own.  An
\* identity that arrives again is the same row: nothing happens, so the
\* model has no step for it.
Receive(m) ==
  /\ m \in Arrives
  /\ m \notin received
  /\ received' = received \cup {m}
  /\ recvPos' = [recvPos EXCEPT ![m] = nextRecv]
  /\ nextRecv' = nextRecv + 1
  /\ UNCHANGED <<part, processed, effects, holding, procOrder, crashes>>

(* ------------------------------------------------------------------------ *)
(* Processing                                                                 *)

DepsProcessed(m) == Deps[m] \subseteq processed

\* Unprocessed rows of the dispatcher's partition that no other transaction
\* holds: FOR UPDATE SKIP LOCKED.
Candidates(d) == {m \in received : m \notin processed /\ m \notin Locked /\ part[m] = d[1]}

Oldest(S) == CHOOSE m \in S : \A n \in S : recvPos[m] <= recvPos[n]

\* The row the dispatcher takes: the oldest candidate whose dependencies are
\* processed, stepping over those whose are not; or the oldest candidate
\* alone, when not stepping over.
Takes(d) ==
  IF SkipIneligible
  THEN {m \in Candidates(d) :
          /\ DepsProcessed(m)
          /\ \A n \in Candidates(d) : DepsProcessed(n) => recvPos[m] <= recvPos[n]}
  ELSE {m \in Candidates(d) : m = Oldest(Candidates(d)) /\ DepsProcessed(m)}

\* SELECT ... ORDER BY received_position LIMIT 1 OFFSET n FOR UPDATE SKIP LOCKED,
\* then the dependency check, inside the dispatcher's transaction.
Fetch(d) ==
  /\ holding[d] = None
  /\ Candidates(d) # {}
  /\ Takes(d) # {}
  /\ holding' = [holding EXCEPT ![d] = CHOOSE m \in Takes(d) : TRUE]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, procOrder, crashes>>

\* The subscriber ran with the dispatcher's transaction; its writes and the
\* mark commit together.
Commit(d) ==
  /\ holding[d] # None
  /\ processed' = processed \cup {holding[d]}
  /\ effects' = [effects EXCEPT ![holding[d]] = @ + 1]
  /\ procOrder' = Append(procOrder, holding[d])
  /\ holding' = [holding EXCEPT ![d] = None]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, crashes>>

\* The dispatcher dies, or the subscriber fails: the transaction rolls back,
\* writes and mark alike, and the row is free again.
Crash(d) ==
  /\ holding[d] # None
  /\ crashes < MaxCrashes
  /\ crashes' = crashes + 1
  /\ holding' = [holding EXCEPT ![d] = None]
  /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, procOrder>>

(* ------------------------------------------------------------------------ *)

Next ==
  \/ \E m \in Msgs : Receive(m)
  \/ \E d \in Dispatchers : Fetch(d) \/ Commit(d) \/ Crash(d)

\* Every message that arrives at all arrives eventually; every dispatcher
\* keeps polling and finishes what it took.
Fairness ==
  /\ \A m \in Msgs : WF_vars(Receive(m))
  /\ \A d \in Dispatchers : WF_vars(Fetch(d)) /\ WF_vars(Commit(d))

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

\* The subscriber's writes for a message are committed at most once, however
\* many dispatchers compete and however often the message arrives.
EffectsOnce == \A m \in Msgs : effects[m] <= 1

\* ... and exactly when the message is marked: writes and mark are one
\* transaction.  This is the guarantee the inbox exists for.
EffectsIffProcessed == \A m \in Msgs : (m \in processed) <=> (effects[m] = 1)

\* A message is processed only after everything it depends on.
CausalOrder == \A m \in processed : Deps[m] \subseteq processed

\* Two dispatchers never hold the same row.
HeldOnce ==
  \A d1, d2 \in Dispatchers :
    (d1 # d2 /\ holding[d1] # None) => holding[d1] # holding[d2]

ProcessedWasReceived == processed \subseteq received

\* A received message whose dependencies are processed is eventually
\* processed itself.  A message waiting on something that never arrives is
\* outside this claim, by design; what the claim does cover is that it does
\* not stand in the way of the others.
EventuallyProcessed ==
  \A m \in Msgs : (m \in received /\ Deps[m] \subseteq processed) ~> (m \in processed)

=============================================================================
