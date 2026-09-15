----------------------------- MODULE TraceInbox -----------------------------
(***************************************************************************)
(* Checks a recorded run of `crates/inbox` against Inbox.tla.              *)
(*                                                                         *)
(* The inbox observer of the tests writes every event as a line of JSON;   *)
(* trace2tla.py turns the file into `Trace`, a sequence of the model's     *)
(* steps: receive (or duplicate), wait, fetch (or nofetch, or blocked),     *)
(* handle, commit, fail, crash, expire, unpark, resolve, each with what     *)
(* PostgreSQL assigned.  Receiving, taking a row and closing the transaction are all   *)
(* logged; the one thing that is not is time: a failed row's backoff has    *)
(* passed by the time the row is taken again, and only then, so Elapse is   *)
(* the hidden step before such a fetch.  TLC walks the trace checking that  *)
(* every step is a step of the model and that the invariants hold.         *)
(*                                                                         *)
(* The order of lines is the order the observer was called in, and the     *)
(* intake and a dispatcher are two tasks: a store may be logged before a    *)
(* walk whose statement ran under a snapshot from before the store's       *)
(* commit, or after a walk that saw it.  Every receive carries the id of    *)
(* the storing transaction and every walk step the snapshot it ran under,  *)
(* and the checks go by what the snapshot could see, as the outbox's go by  *)
(* the horizon: a walk is checked over the stored rows visible to it, and   *)
(* a store logged later but visible to a walk is taken as the hidden step   *)
(* before that walk, its logged line then being a step that changes         *)
(* nothing.  What the checks do not cover is the visibility of marks: the   *)
(* dependency check is a statement of its own, whose snapshot is not        *)
(* logged.                                                                 *)
(*                                                                         *)
(* The parking policy is read off the trace: MaxAttempts is the count at   *)
(* which a message was parked, or 0 when none was; a run that parked one    *)
(* message at two attempts and let another fail twice unparked does not     *)
(* fit.  Every recorded failure is a transient one, so the budget of        *)
(* failures is their number; Poison is empty.                              *)
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
Dispatching == {r \in InboxEvents : r.event \in {"wait", "fetch", "nofetch", "blocked", "handle", "commit", "fail", "crash"}}
Parks == {r \in InboxEvents : r.event = "fail" /\ r.parked}
Expires == {r \in InboxEvents : r.event = "expire"}

Arrives == {r.msg : r \in Receives}
Stored(m) == CHOOSE r \in Receives : r.msg = m
XidOf == [m \in Arrives |-> Stored(m).xid]
PosOf == [m \in Arrives |-> Stored(m).pos]
\* A dependency that never arrives is a message all the same: it is what the
\* dependent one waits for.
Msgs == Arrives \cup UNION {Range(r.deps) : r \in Receives}
Deps == [m \in Msgs |-> IF m \in Arrives THEN Range((CHOOSE r \in Receives : r.msg = m).deps) ELSE {}]
Workers == IF Dispatching = {} THEN {0} ELSE 0..((CHOOSE r \in Dispatching : TRUE).of - 1)
Dispatchers == {r.d : r \in Dispatching}
MaxCrashes == Cardinality({j \in 1..Len(Trace) : Trace[j].side = "inbox" /\ Trace[j].event \in {"crash", "fail"}})
WaitsOnDependencies == TRUE
WaitExpires == Expires # {}
None == "None"
Poison == {}
MaxAttempts == IF Parks = {} THEN 0 ELSE (CHOOSE r \in Parks : TRUE).attempts
BlockOnBackoff == TRUE
MaxAdmin == Cardinality({j \in 1..Len(Trace) : Trace[j].side = "inbox" /\ Trace[j].event \in {"unpark", "resolve"}})

\* The worker a message's partition key hashes to, read off the fetches and
\* skips; a message nobody touched may go anywhere.
TouchedBy(m) == {r.d[1] : r \in {e \in InboxEvents : e.event \in {"wait", "fetch"} /\ e.msg = m}}
Part == [m \in Msgs |-> IF TouchedBy(m) = {} THEN CHOOSE w \in Workers : TRUE
                                            ELSE CHOOSE w \in TouchedBy(m) : TRUE]

VARIABLES part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes,
          attempts, due, parked, resolved, admin, waiting, expired,
          i  \* the next step of the trace

vars == <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes,
          attempts, due, parked, resolved, admin, waiting, expired, i>>

O == INSTANCE Inbox WITH Msgs <- Msgs, Deps <- Deps, Arrives <- Arrives, Workers <- Workers,
                         Dispatchers <- Dispatchers, MaxCrashes <- MaxCrashes,
                         WaitsOnDependencies <- WaitsOnDependencies, WaitExpires <- WaitExpires, None <- None,
                         Poison <- Poison, MaxAttempts <- MaxAttempts,
                         BlockOnBackoff <- BlockOnBackoff, MaxAdmin <- MaxAdmin

Pending == i <= Len(Trace)
Step == Trace[i]

\* What a statement under snapshot `snap` could see of the stored rows: the
\* transaction had ended before xmin, or was neither in progress nor yet
\* started.
Sees(snap, xid) == xid < snap.xmin \/ (xid < snap.xmax /\ \A j \in 1..Len(snap.xip) : snap.xip[j] # xid)
Visible(snap) == {m \in received : Sees(snap, XidOf[m])}

EligibleIn(d, rows) == {m \in O!CandidatesIn(d, rows) : O!DepsProcessed(m)}

(* ------------------------------------------------------------------------ *)

Init ==
  /\ part = Part
  /\ O!Init
  /\ i = 1

Walks == {"wait", "fetch", "nofetch", "blocked"}

\* Time is not logged: the backoff of a failed row has passed by the time
\* the row is taken again, and only then.
Elapsing ==
  /\ Pending
  /\ Step.side = "inbox"
  /\ Step.event = "fetch"
  /\ ~due[Step.msg]
  /\ O!Elapse(Step.msg)
  /\ UNCHANGED i

\* A store logged after a walk that could see it, or after a duplicate of
\* it, happened before: it is received here, in one fixed order, and its own
\* line later changes nothing.
Unlogged(snap) == {m \in Arrives \ received : Sees(snap, XidOf[m])}
FirstStored(S) == CHOOSE m \in S : \A n \in S : XidOf[m] <= XidOf[n]
Receiving ==
  /\ Pending
  /\ Step.side = "inbox"
  /\ UNCHANGED i
  /\ \/ /\ Step.event \in Walks
        /\ Unlogged(Step.snap) # {}
        /\ LET m == FirstStored(Unlogged(Step.snap)) IN O!ReceiveAs(m, PosOf[m])
     \/ /\ Step.event = "duplicate"
        /\ Step.msg \notin received
        /\ O!ReceiveAs(Step.msg, PosOf[Step.msg])

Hidden == Elapsing \/ Receiving

Logged ==
  /\ Pending
  /\ Step.side = "inbox"
  /\ i' = i + 1
  /\ \/ /\ Step.event = "receive"
        /\ Step.msg \notin received
        /\ O!ReceiveAs(Step.msg, Step.pos)
     \* received already, as the hidden step before a walk that saw it
     \/ /\ Step.event = "receive"
        /\ Step.msg \in received
        /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes,
                      attempts, due, parked, resolved, admin, waiting, expired>>
     \/ /\ Step.event = "duplicate"
        /\ Step.msg \in received
        \* the tuple in full: TLC cannot prime an instance's tuple, and TraceBridge instances this module
        /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes,
                      attempts, due, parked, resolved, admin, waiting, expired>>
     \/ /\ Step.event = "wait"
        /\ O!WaitIn(Step.d, Visible(Step.snap), Step.msg, Step.dep)
     \/ /\ Step.event = "fetch"
        /\ O!FetchFrom(Step.d, Visible(Step.snap))
        /\ holding'[Step.d] = Step.msg
     \/ /\ Step.event = "nofetch"
        /\ holding[Step.d] = None
        /\ EligibleIn(Step.d, Visible(Step.snap)) = {}
        /\ \A m \in O!CandidatesIn(Step.d, Visible(Step.snap)) : due[m]
        \* the tuple in full: TLC cannot prime an instance's tuple, and TraceBridge instances this module
        /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes,
                      attempts, due, parked, resolved, admin, waiting, expired>>
     \* the head of the queue waits for its backoff, and holds the partition
     \/ /\ Step.event = "blocked"
        /\ holding[Step.d] = None
        /\ Step.msg \in O!CandidatesIn(Step.d, Visible(Step.snap))
        /\ Step.msg = O!Oldest(O!CandidatesIn(Step.d, Visible(Step.snap)))
        /\ ~due[Step.msg]
        /\ O!TakesIn(Step.d, Visible(Step.snap)) = {}
        /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes,
                      attempts, due, parked, resolved, admin, waiting, expired>>
     \/ /\ Step.event = "handle"
        /\ holding[Step.d] = Step.msg
        \* the tuple in full: TLC cannot prime an instance's tuple, and TraceBridge instances this module
        /\ UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes,
                      attempts, due, parked, resolved, admin, waiting, expired>>
     \* the mark wakes exactly what waited for the message, in the same transaction
     \/ /\ Step.event = "commit"
        /\ {n \in Msgs : waiting[n] = holding[Step.d]} = Range(Step.woken)
        /\ O!Commit(Step.d)
     \/ /\ Step.event = "fail"
        /\ holding[Step.d] = Step.msg
        /\ O!Fail(Step.d)
        /\ (MaxAttempts > 0 => attempts'[Step.msg] = Step.attempts)
        /\ (Step.msg \in parked') = Step.parked
     \/ /\ Step.event = "crash"
        /\ O!Crash(Step.d)
     \/ /\ Step.event = "expire"
        /\ O!Expire(Step.msg)
     \/ /\ Step.event = "unpark"
        /\ O!Unpark(Step.msg)
     \/ /\ Step.event = "resolve"
        /\ {n \in Msgs : waiting[n] = Step.msg} = Range(Step.woken)
        /\ O!Resolve(Step.msg)

Next == Hidden \/ Logged

Spec == Init /\ [][Next]_vars

(* ------------------------------------------------------------------------ *)

TypeOK == O!TypeOK /\ i \in 1..(Len(Trace) + 1)
EffectsOnce == O!EffectsOnce
EffectsIffProcessed == O!EffectsIffProcessed
CausalOrder == O!CausalOrder
HeldOnce == O!HeldOnce
ProcessedWasReceived == O!ProcessedWasReceived
ParkedIsAside == O!ParkedIsAside
WaitingIsAside == O!WaitingIsAside
\* Only with one dispatcher per partition; trace2tla.py asks for it then.
ArrivalOrder == O!ArrivalOrder

\* Violated exactly when the whole trace was matched.
NotFinished == Pending

=============================================================================
