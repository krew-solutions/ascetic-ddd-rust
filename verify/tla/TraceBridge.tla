---------------------------- MODULE TraceBridge ----------------------------
(***************************************************************************)
(* Checks a recorded run of an outbox feeding an inbox against Bridge.tla. *)
(*                                                                         *)
(* One recorder watches both sides, so the trace keeps the order across    *)
(* them.  The outbox records are checked by TraceOutbox and the inbox      *)
(* records by TraceInbox, both instanced here over the same trace and the  *)
(* same variables; the composition, Bridge.tla, supplies the frame of the  *)
(* other side and the invariants of the whole.                             *)
(*                                                                         *)
(* The one step that belongs to neither side is Forward: the bridge's      *)
(* subscriber stores the message in the inbox and returns, and the outbox  *)
(* counts it handled.  The trace shows two events, the inbox's receive     *)
(* and the outbox's handle; trace2tla.py joins them into one `forward`,    *)
(* placed where the store happened.  A handle without a store, or a store  *)
(* outside a handling, is refused: with StoreBeforeAck those are not steps *)
(* of the bridge, and a run that acknowledged before storing — the shape   *)
(* BridgeAckFirst.cfg shows loses messages — does not fit.                 *)
(*                                                                         *)
(* A message the intake sets aside is two steps of Bridge.tla, SetAside    *)
(* and Acknowledge: the row set aside commits, then the outbox counts the  *)
(* message handled.  The trace shows the inbox's rejection and the         *)
(* outbox's handling, which trace2tla.py names `set_aside` and             *)
(* `acknowledge`.  A rejection no handling follows, or a handling before   *)
(* its rejection, does not fit (ADR-0036).                                 *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANT Trace  \* Seq of records, see trace2tla.py

VARIABLES uriOf, srcAssign, txState, xid, pos, nextXid, nextPos, position, batch, done, delivered, srcCrashes,
          part, received, recvPos, nextRecv, processed, effects, holding, procOrder, dstCrashes,
          attempts, due, parked, resolved, admin, waiting, expired,
          pending, rejected, rejecting, held, holds,
          i  \* the next step of the trace

vars == <<uriOf, srcAssign, txState, xid, pos, nextXid, nextPos, position, batch, done, delivered, srcCrashes,
          part, received, recvPos, nextRecv, processed, effects, holding, procOrder, dstCrashes,
          attempts, due, parked, resolved, admin, waiting, expired, pending, rejected, rejecting, held, holds, i>>

\* The checkers of the two sides, each seeing the records of its side.
TO == INSTANCE TraceOutbox WITH assign <- srcAssign, crashes <- srcCrashes
TI == INSTANCE TraceInbox WITH crashes <- dstCrashes

Msgs == TO!Msgs
SrcGroup == IF TO!Groups = {} THEN "bridge" ELSE CHOOSE g \in TO!Groups : TRUE

Pending == i <= Len(Trace)
Step == Trace[i]

\* The messages the trace sets aside: what the intake could not store.
SetAsideInTrace ==
  {Trace[k].msg : k \in {k \in 1..Len(Trace) : Trace[k].side = "bridge" /\ Trace[k].event = "set_aside"}}

B == INSTANCE Bridge WITH Msgs <- Msgs, Uris <- TO!Uris, SrcGroup <- SrcGroup, SrcSlots <- TO!Slots,
                          DstSlots <- TI!Slots,
                          BatchSize <- 1, MaxCrashes <- TO!MaxCrashes + TI!MaxCrashes,
                          StoreBeforeAck <- TRUE, None <- TI!None,
                          Unstorable <- SetAsideInTrace, MaxHolds <- 0, RejectBeforeAck <- TRUE,
                          AckWithoutSetAside <- FALSE, SetAsideOnHold <- FALSE

\* Nothing the steps of either side touch of the bridge's own.
BridgeUnchanged == UNCHANGED <<pending, rejected, rejecting, held, holds>>

\* Which inbox slot a message landed in, as its receipt says; the inbox is
\* cut by URI, which Bridge's Init requires the placement to respect.  A
\* message the intake set aside has no receipt: its slot is that of a stored
\* message of its URI, or any.
Part == [m \in Msgs |->
           IF m \in TI!Msgs THEN TI!Part[m]
           ELSE IF \E s \in TI!Msgs : TO!UriOf[s] = TO!UriOf[m]
                THEN TI!Part[CHOOSE s \in TI!Msgs : TO!UriOf[s] = TO!UriOf[m]]
                ELSE CHOOSE w \in TI!Slots : TRUE]

(* ------------------------------------------------------------------------ *)

Init ==
  /\ uriOf = TO!UriOf
  /\ srcAssign = TO!Assign
  /\ part = Part
  /\ B!Init
  /\ i = 1

\* Src!Handle and Dst!Receive as one step, as in Bridge's Forward.  A message
\* stored before, delivered again after a crash, is the same row.
Forward ==
  /\ Pending
  /\ Step.side = "bridge"
  /\ Step.event = "forward"
  /\ i' = i + 1
  /\ B!Src!Handle(Step.d)
  /\ batch[Step.d][done[Step.d] + 1] = Step.msg
  /\ (Step.msg \in received) = Step.dup
  /\ IF Step.dup THEN B!DstUnchanged ELSE B!Dst!ReceiveAs(Step.msg, Step.pos)
  /\ BridgeUnchanged

\* The row set aside commits, and then the outbox counts the message handled.
SetAside ==
  /\ Pending
  /\ Step.side = "bridge"
  /\ Step.event = "set_aside"
  /\ i' = i + 1
  /\ B!SetAside(Step.d)
  /\ B!Upcoming(Step.d) = Step.msg

Acknowledge ==
  /\ Pending
  /\ Step.side = "bridge"
  /\ Step.event = "acknowledge"
  /\ i' = i + 1
  /\ B!Acknowledge(Step.d)
  /\ B!Upcoming(Step.d) = Step.msg

Next ==
  \/ TO!Hidden /\ B!DstUnchanged /\ BridgeUnchanged
  \/ TO!Logged /\ Step.event # "handle" /\ B!DstUnchanged /\ BridgeUnchanged
  \/ (TI!Elapsing \/ TI!Logged) /\ Step.event \notin {"receive", "duplicate"} /\ B!SrcUnchanged /\ BridgeUnchanged
  \/ Forward
  \/ SetAside
  \/ Acknowledge

Spec == Init /\ [][Next]_vars

(* ------------------------------------------------------------------------ *)

TypeOK == B!TypeOK /\ i \in 1..(Len(Trace) + 1)
SourceInvariants == B!SourceInvariants
DestinationInvariants == B!DestinationInvariants
ReceivedWasCommitted == B!ReceivedWasCommitted
EndToEndOrder == B!EndToEndOrder
SetAsideNeverStored == B!SetAsideNeverStored

\* Violated exactly when the whole trace was matched.
NotFinished == Pending

=============================================================================
