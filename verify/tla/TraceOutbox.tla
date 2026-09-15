---------------------------- MODULE TraceOutbox ----------------------------
(***************************************************************************)
(* Checks a recorded run of `crates/outbox` against Outbox.tla.            *)
(*                                                                         *)
(* The outbox observer of the tests writes every event as a line of JSON;  *)
(* trace2tla.py turns the file into `Trace`, a sequence of the model's     *)
(* steps: publish, fetch (or nofetch, an empty one), handle, ack, crash,   *)
(* each with what PostgreSQL assigned — transaction ids, serials, the      *)
(* snapshot's xmin.  This module lets the model take exactly those steps   *)
(* in that order, with the logged values in place of the model's own.      *)
(*                                                                         *)
(* Producers' Begin, Commit and Abort are not logged.  The trace           *)
(* determines them all the same: a message begins just before it is        *)
(* published; at a fetch with horizon h, a message with an id below h in   *)
(* the dispatcher's partition past its position has ended — in the batch   *)
(* it committed, before the batch's end or in an empty batch it aborted.   *)
(* Beyond the end of a full batch it is undecided and stays open for a     *)
(* later fetch.  So the behaviour is one chain, and TLC checks it in one   *)
(* pass: every logged step must be a step of the model, and the model's    *)
(* invariants must hold along the way.                                     *)
(*                                                                         *)
(* A run that fits ends with i past the trace, which violates NotFinished  *)
(* — the violation check.sh requires.  A run that does not fit deadlocks   *)
(* at the first step the model refuses, and TLC prints that state, i       *)
(* included.                                                               *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANT Trace  \* Seq of records, see trace2tla.py

Range(s) == {s[j] : j \in 1..Len(s)}
Events == Range(Trace)
\* Every record names its side, so that a bridge trace, which holds both an
\* outbox and an inbox, can be checked by this module and TraceInbox at once.
OutboxEvents == {r \in Events : r.side = "outbox"}
Publishes == {r \in OutboxEvents : r.event = "publish"}
Fetches == {r \in OutboxEvents : r.event \in {"fetch", "nofetch"}}

Msgs == {r.msg : r \in Publishes}
Uris == {r.uri : r \in Publishes}
Groups == {r.d[1] : r \in Fetches}
Workers == IF Fetches = {} THEN {0} ELSE 0..((CHOOSE r \in Fetches : TRUE).of - 1)
MaxCrashes == Cardinality({j \in 1..Len(Trace) : Trace[j].side = "outbox" /\ Trace[j].event = "crash"})
BatchSize == 1  \* unused: every fetch brings the limit it ran with
VisibilityRule == TRUE

UriOf == [m \in Msgs |-> (CHOOSE r \in Publishes : r.msg = m).uri]

\* The worker a URI hashes to, read off the fetches: hashtext is not
\* reproduced, the assignment it made is.  A URI nobody fetched may go anywhere.
FetchedBy(u) == {r.d[2] : r \in {f \in Fetches : \E m \in Range(f.msgs) : UriOf[m] = u}}
Assign == [u \in Uris |-> IF FetchedBy(u) = {} THEN CHOOSE w \in Workers : TRUE
                                              ELSE CHOOSE w \in FetchedBy(u) : TRUE]

VARIABLES uriOf, assign, txState, xid, pos, nextXid, nextPos, position, batch, done, delivered, crashes,
          i  \* the next step of the trace

O == INSTANCE Outbox WITH Msgs <- Msgs, Uris <- Uris, Groups <- Groups, Workers <- Workers,
                          BatchSize <- BatchSize, MaxCrashes <- MaxCrashes,
                          VisibilityRule <- VisibilityRule

vars == <<uriOf, assign, txState, xid, pos, nextXid, nextPos, position, batch, done, delivered, crashes, i>>

Pending == i <= Len(Trace)
Step == Trace[i]

(* ------------------------------------------------------------------------ *)
(* What a fetch says about the transactions it did not see                     *)

Relevant(r) ==
  {m \in Msgs : /\ xid[m] # O!NoXid
                /\ xid[m] < r.horizon
                /\ assign[uriOf[m]] = r.d[2]
                /\ O!Before(position[r.d], O!Key(m))}

InBatch(r) == Range(r.msgs)

\* A full batch says nothing about what lies beyond its last message.
Beyond(r, m) == Len(r.msgs) = r.limit /\ O!Before(O!Key(r.msgs[Len(r.msgs)]), O!Key(m))

MustCommit(r) == {m \in InBatch(r) : txState[m] = "open"}
MustAbort(r) == {m \in Relevant(r) \ InBatch(r) : txState[m] = "open" /\ ~Beyond(r, m)}
Settled(r) == MustCommit(r) = {} /\ MustAbort(r) = {}

\* One fixed order, so that the hidden steps form a single chain.
First(S) == CHOOSE m \in S : \A n \in S : m = n \/ O!Before(O!Key(m), O!Key(n))

(* ------------------------------------------------------------------------ *)

Init ==
  /\ uriOf = UriOf
  /\ assign = Assign
  /\ O!Init
  /\ i = 1

\* The steps the trace does not record but determines.
Hidden ==
  /\ Pending
  /\ Step.side = "outbox"
  /\ UNCHANGED i
  /\ \/ /\ Step.event = "publish"
        /\ O!Begin(Step.msg)
     \/ /\ Step.event \in {"fetch", "nofetch"}
        /\ \/ /\ MustCommit(Step) # {}
              /\ O!Commit(First(MustCommit(Step)))
           \/ /\ MustCommit(Step) = {}
              /\ MustAbort(Step) # {}
              /\ O!Abort(First(MustAbort(Step)))

\* The steps the trace records, with the logged values.
Logged ==
  /\ Pending
  /\ Step.side = "outbox"
  /\ i' = i + 1
  /\ \/ /\ Step.event = "publish"
        /\ txState[Step.msg] = "open"
        /\ O!PublishAs(Step.msg, Step.xid, Step.pos)
     \/ /\ Step.event = "fetch"
        /\ Settled(Step)
        /\ O!FetchWith(Step.d, Step.horizon, Step.limit)
        /\ batch'[Step.d] = Step.msgs
     \/ /\ Step.event = "nofetch"
        /\ Settled(Step)
        /\ O!Eligible(Step.d, Step.horizon) = {}
        \* the tuple in full: TLC cannot prime an instance's tuple, and TraceBridge instances this module
        /\ UNCHANGED <<uriOf, assign, txState, xid, pos, nextXid, nextPos, position, batch, done, delivered, crashes>>
     \/ /\ Step.event = "handle"
        /\ O!Handle(Step.d)
        /\ batch[Step.d][done[Step.d] + 1] = Step.msg
     \/ /\ Step.event = "ack"
        /\ O!Ack(Step.d)
        /\ position'[Step.d] = <<Step.xid, Step.pos>>
     \/ /\ Step.event = "crash"
        /\ O!Crash(Step.d)

Next == Hidden \/ Logged

Spec == Init /\ [][Next]_vars

(* ------------------------------------------------------------------------ *)

TypeOK == O!TypeOK /\ i \in 1..(Len(Trace) + 1)
DeliveredCommitted == O!DeliveredCommitted
NoPassedOver == O!NoPassedOver
OrderPerUri == O!OrderPerUri

\* Violated exactly when the whole trace was matched.
NotFinished == Pending

=============================================================================
