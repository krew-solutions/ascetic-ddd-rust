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
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANT Trace  \* Seq of records, see trace2tla.py

VARIABLES uriOf, srcAssign, txState, xid, pos, nextXid, nextPos, position, batch, done, delivered, srcCrashes,
          part, received, recvPos, nextRecv, processed, effects, holding, procOrder, dstCrashes,
          attempts, due, parked, resolved, admin, waiting, expired,
          pending,
          i  \* the next step of the trace

vars == <<uriOf, srcAssign, txState, xid, pos, nextXid, nextPos, position, batch, done, delivered, srcCrashes,
          part, received, recvPos, nextRecv, processed, effects, holding, procOrder, dstCrashes,
          attempts, due, parked, resolved, admin, waiting, expired, pending, i>>

\* The checkers of the two sides, each seeing the records of its side.
TO == INSTANCE TraceOutbox WITH assign <- srcAssign, crashes <- srcCrashes
TI == INSTANCE TraceInbox WITH crashes <- dstCrashes

Msgs == TO!Msgs
SrcGroup == IF TO!Groups = {} THEN "bridge" ELSE CHOOSE g \in TO!Groups : TRUE

B == INSTANCE Bridge WITH Msgs <- Msgs, Uris <- TO!Uris, SrcGroup <- SrcGroup, SrcSlots <- TO!Slots,
                          DstSlots <- TI!Slots,
                          BatchSize <- 1, MaxCrashes <- TO!MaxCrashes + TI!MaxCrashes,
                          StoreBeforeAck <- TRUE, None <- TI!None

Pending == i <= Len(Trace)
Step == Trace[i]

\* Which inbox slot a message landed in, as its receipt says; the inbox is
\* cut by URI, which Bridge's Init requires the placement to respect.
Part == TI!Part

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
  /\ UNCHANGED pending

Next ==
  \/ TO!Hidden /\ B!DstUnchanged /\ UNCHANGED pending
  \/ TO!Logged /\ Step.event # "handle" /\ B!DstUnchanged /\ UNCHANGED pending
  \/ (TI!Elapsing \/ TI!Logged) /\ Step.event \notin {"receive", "duplicate"} /\ B!SrcUnchanged /\ UNCHANGED pending
  \/ Forward

Spec == Init /\ [][Next]_vars

(* ------------------------------------------------------------------------ *)

TypeOK == B!TypeOK /\ i \in 1..(Len(Trace) + 1)
SourceInvariants == B!SourceInvariants
DestinationInvariants == B!DestinationInvariants
ReceivedWasCommitted == B!ReceivedWasCommitted
EndToEndOrder == B!EndToEndOrder

\* Violated exactly when the whole trace was matched.
NotFinished == Pending

=============================================================================
