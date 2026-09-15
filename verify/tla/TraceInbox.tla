----------------------------- MODULE TraceInbox -----------------------------
(***************************************************************************)
(* Checks a recorded run of `crates/inbox` against Inbox.tla.              *)
(*                                                                         *)
(* The inbox observer of the tests writes every event as a line of JSON;   *)
(* trace2tla.py turns the file into `Trace`, a sequence of the model's     *)
(* steps: receive (or duplicate), skip, fetch (or nofetch), handle, commit, *)
(* crash, each with what PostgreSQL assigned.  Unlike the outbox, nothing   *)
(* is hidden: receiving, taking a row and closing the transaction are all   *)
(* logged, so the trace is the behaviour, and TLC walks it checking that    *)
(* every step is a step of the model and that the invariants hold.         *)
(*                                                                         *)
(* A dispatcher is <<worker, slot>>: several `dispatch` calls of one worker *)
(* run at once under FOR UPDATE SKIP LOCKED, and trace2tla.py gives each    *)
(* open call the lowest free slot of its worker, so that the instance has   *)
(* as many dispatchers as ever ran together.                               *)
(*                                                                         *)
(* Skip, nofetch and handle are checks, not steps of the model: a row       *)
(* stepped over must be a candidate whose dependencies are not processed;   *)
(* an empty fetch must find no eligible candidate; a handled message must   *)
(* be the one held.  The model folds skipping into Takes(d).                *)
(*                                                                         *)
(* A run that fits ends with i past the trace, which violates NotFinished  *)
(* — the violation check.sh requires.  A run that does not fit deadlocks   *)
(* at the first step the model refuses, and TLC prints that state.         *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANT Trace  \* Seq of records, see trace2tla.py

Range(s) == {s[j] : j \in 1..Len(s)}
Events == Range(Trace)
\* Every record names its side, so that a bridge trace, which holds both an
\* outbox and an inbox, can be checked by this module and TraceOutbox at once.
InboxEvents == {r \in Events : r.side = "inbox"}
\* In a bridge trace a store is inside a `forward`, the bridge's own step; one
\* that was not a duplicate is a receive for the purposes of this module.
Receives == {r \in Events : \/ r.side = "inbox" /\ r.event = "receive"
                            \/ r.side = "bridge" /\ r.event = "forward" /\ ~r.dup}
Dispatching == {r \in InboxEvents : r.event \in {"skip", "fetch", "nofetch", "handle", "commit", "crash"}}

Arrives == {r.msg : r \in Receives}
\* A dependency that never arrives is a message all the same: it is what the
\* dependent one waits for.
Msgs == Arrives \cup UNION {Range(r.deps) : r \in Receives}
Deps == [m \in Msgs |-> IF m \in Arrives THEN Range((CHOOSE r \in Receives : r.msg = m).deps) ELSE {}]
Workers == IF Dispatching = {} THEN {0} ELSE 0..((CHOOSE r \in Dispatching : TRUE).of - 1)
Dispatchers == {r.d : r \in Dispatching}
MaxCrashes == Cardinality({j \in 1..Len(Trace) : Trace[j].side = "inbox" /\ Trace[j].event = "crash"})
SkipIneligible == TRUE
None == "None"

\* The worker a message's partition key hashes to, read off the fetches and
\* skips; a message nobody touched may go anywhere.
TouchedBy(m) == {r.d[1] : r \in {e \in InboxEvents : e.event \in {"skip", "fetch"} /\ e.msg = m}}
Part == [m \in Msgs |-> IF TouchedBy(m) = {} THEN CHOOSE w \in Workers : TRUE
                                            ELSE CHOOSE w \in TouchedBy(m) : TRUE]

VARIABLES part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes,
          i  \* the next step of the trace

vars == <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes, i>>

O == INSTANCE Inbox WITH Msgs <- Msgs, Deps <- Deps, Arrives <- Arrives, Workers <- Workers,
                         Dispatchers <- Dispatchers, MaxCrashes <- MaxCrashes,
                         SkipIneligible <- SkipIneligible, None <- None

Pending == i <= Len(Trace)
Step == Trace[i]

Eligible(d) == {m \in O!Candidates(d) : O!DepsProcessed(m)}

(* ------------------------------------------------------------------------ *)

Init ==
  /\ part = Part
  /\ O!Init
  /\ i = 1

Next ==
  /\ Pending
  /\ Step.side = "inbox"
  /\ i' = i + 1
  /\ \/ /\ Step.event = "receive"
        /\ O!ReceiveAs(Step.msg, Step.pos)
     \/ /\ Step.event = "duplicate"
        /\ Step.msg \in received
        \* the tuple in full: TLC cannot prime an instance's tuple, and TraceBridge instances this module
        /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes>>
     \/ /\ Step.event = "skip"
        /\ Step.msg \in O!Candidates(Step.d)
        /\ ~O!DepsProcessed(Step.msg)
        \* the tuple in full: TLC cannot prime an instance's tuple, and TraceBridge instances this module
        /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes>>
     \/ /\ Step.event = "fetch"
        /\ O!Fetch(Step.d)
        /\ holding'[Step.d] = Step.msg
     \/ /\ Step.event = "nofetch"
        /\ holding[Step.d] = None
        /\ Eligible(Step.d) = {}
        \* the tuple in full: TLC cannot prime an instance's tuple, and TraceBridge instances this module
        /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes>>
     \/ /\ Step.event = "handle"
        /\ holding[Step.d] = Step.msg
        \* the tuple in full: TLC cannot prime an instance's tuple, and TraceBridge instances this module
        /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes>>
     \/ /\ Step.event = "commit"
        /\ O!Commit(Step.d)
     \/ /\ Step.event = "crash"
        /\ O!Crash(Step.d)

Spec == Init /\ [][Next]_vars

(* ------------------------------------------------------------------------ *)

TypeOK == O!TypeOK /\ i \in 1..(Len(Trace) + 1)
EffectsOnce == O!EffectsOnce
EffectsIffProcessed == O!EffectsIffProcessed
CausalOrder == O!CausalOrder
HeldOnce == O!HeldOnce
ProcessedWasReceived == O!ProcessedWasReceived

\* Violated exactly when the whole trace was matched.
NotFinished == Pending

=============================================================================
