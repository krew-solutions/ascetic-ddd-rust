----------------------------- MODULE Bridge -----------------------------
(***************************************************************************)
(* The Messaging Bridge of `crates/bus`, composed from the outbox and the  *)
(* inbox of the two modules beside this one (ADR-0003).                    *)
(*                                                                         *)
(* A dispatcher of the source outbox hands each message to the bridge,     *)
(* whose subscriber stores it in the destination inbox, in a transaction   *)
(* of the inbox's own, before it returns; the batch is acknowledged after  *)
(* the last one.  Inbox dispatchers process the rows.  Either side crashes *)
(* at any point, in particular between the store and the acknowledgement,  *)
(* which delivers the batch again.                                         *)
(*                                                                         *)
(* Storing and returning are one step here.  A crash between the two, in   *)
(* the implementation, is the outbox crashing after a message was handed   *)
(* over and before the batch was acknowledged, a step this model has; a    *)
(* failure of the store is the subscriber failing, another step it has.    *)
(*                                                                         *)
(* The inbox partitions by URI, as the bridge target does, with one        *)
(* dispatcher per partition and no causal dependencies: the conditions     *)
(* under which order is claimed to survive the crossing.                   *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
  Msgs,
  Uris,
  SrcWorkers,      \* workers of the source dispatcher
  DstWorkers,      \* partitions of the destination inbox
  DstDispatchers,  \* SUBSET (DstWorkers \X Nat)
  BatchSize,
  MaxCrashes,      \* per side
  StoreBeforeAck,  \* TRUE: a message is stored in the inbox before the subscriber returns, as implemented
  None

Group == "bridge"
SrcDispatchers == {Group} \X SrcWorkers

\* The source outbox, then the destination inbox.
VARIABLES uriOf, srcAssign, txState, xid, pos, nextXid, nextPos, position, batch, done, delivered, srcCrashes,
          part, received, recvPos, nextRecv, processed, effects, holding, procOrder, dstCrashes,
          pending  \* SUBSET Msgs: handed over but not yet stored, when the store is deferred

vars == <<uriOf, srcAssign, txState, xid, pos, nextXid, nextPos, position, batch, done, delivered, srcCrashes,
          part, received, recvPos, nextRecv, processed, effects, holding, procOrder, dstCrashes, pending>>

Src == INSTANCE Outbox WITH
  Groups <- {Group}, Workers <- SrcWorkers, VisibilityRule <- TRUE,
  assign <- srcAssign, crashes <- srcCrashes

Dst == INSTANCE Inbox WITH
  Deps <- [m \in Msgs |-> {}], Arrives <- Msgs, Workers <- DstWorkers,
  Dispatchers <- DstDispatchers, SkipIneligible <- TRUE,
  crashes <- dstCrashes

(* ------------------------------------------------------------------------ *)

Init ==
  /\ Src!Init
  /\ Dst!Init
  \* the inbox partitions by URI: messages of one URI share a partition
  /\ \A a, b \in Msgs : uriOf[a] = uriOf[b] => part[a] = part[b]
  /\ pending = {}

SrcUnchanged == UNCHANGED <<uriOf, srcAssign, txState, xid, pos, nextXid, nextPos, position, batch, done, delivered, srcCrashes>>
DstUnchanged == UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, dstCrashes>>

\* Every action names both sides, so that fairness over it is well defined.

Produce(m) == (Src!Begin(m) \/ Src!Publish(m) \/ Src!Commit(m) \/ Src!Abort(m)) /\ DstUnchanged /\ UNCHANGED pending
Ends(m) == (Src!Commit(m) \/ Src!Abort(m)) /\ DstUnchanged /\ UNCHANGED pending

SrcFetch(d) == Src!Fetch(d) /\ DstUnchanged /\ UNCHANGED pending
SrcAck(d) == Src!Ack(d) /\ DstUnchanged /\ UNCHANGED pending

\* The dispatcher process dies, or its subscriber fails: an in-flight batch
\* rolls back and the position stays; whatever was still to be stored is
\* gone with the process.
Die(d) ==
  /\ srcCrashes < MaxCrashes
  /\ batch[d] # <<>> \/ pending # {}
  /\ srcCrashes' = srcCrashes + 1
  /\ batch' = [batch EXCEPT ![d] = <<>>]
  /\ done' = [done EXCEPT ![d] = 0]
  /\ pending' = {}
  /\ UNCHANGED <<uriOf, srcAssign, txState, xid, pos, nextXid, nextPos, position, delivered>>
  /\ DstUnchanged

\* The bridge: the next message of the batch is stored in the inbox, in a
\* transaction of the inbox's own, and the outbox counts it handled.  A
\* message stored before, redelivered after a crash, is the same row.
\* With StoreBeforeAck off, the message only counts as handled here and is
\* stored later, by Store: the shape of the Go channel API, where a message
\* was acknowledged on delivery to the consumer rather than on completion.
Forward(d) ==
  /\ Src!Handle(d)
  /\ LET m == batch[d][done[d] + 1]
     IN IF StoreBeforeAck
        THEN /\ UNCHANGED pending
             /\ IF m \in received THEN DstUnchanged ELSE Dst!Receive(m)
        ELSE /\ pending' = pending \cup {m}
             /\ DstUnchanged

Store(m) ==
  /\ m \in pending
  /\ pending' = pending \ {m}
  /\ IF m \in received THEN DstUnchanged ELSE Dst!Receive(m)
  /\ SrcUnchanged

DstFetch(d) == Dst!Fetch(d) /\ SrcUnchanged /\ UNCHANGED pending
DstCommit(d) == Dst!Commit(d) /\ SrcUnchanged /\ UNCHANGED pending
DstCrash(d) == Dst!Crash(d) /\ SrcUnchanged /\ UNCHANGED pending

Next ==
  \/ \E m \in Msgs : Produce(m) \/ Store(m)
  \/ \E d \in SrcDispatchers : SrcFetch(d) \/ Forward(d) \/ SrcAck(d) \/ Die(d)
  \/ \E d \in DstDispatchers : DstFetch(d) \/ DstCommit(d) \/ DstCrash(d)

Fairness ==
  /\ \A m \in Msgs : WF_vars(Ends(m))
  /\ \A m \in Msgs : WF_vars(Store(m))
  /\ \A d \in SrcDispatchers : WF_vars(SrcFetch(d)) /\ WF_vars(Forward(d)) /\ WF_vars(SrcAck(d))
  /\ \A d \in DstDispatchers : WF_vars(DstFetch(d)) /\ WF_vars(DstCommit(d))

Spec == Init /\ [][Next]_vars /\ Fairness

(* ------------------------------------------------------------------------ *)
(* Properties                                                                 *)

TypeOK == Src!TypeOK /\ Dst!TypeOK /\ pending \subseteq Msgs

\* Each side keeps what it proved on its own.
SourceInvariants == Src!DeliveredCommitted /\ Src!NoPassedOver /\ Src!OrderPerUri
DestinationInvariants == Dst!EffectsOnce /\ Dst!EffectsIffProcessed /\ Dst!HeldOnce /\ Dst!ProcessedWasReceived

\* The inbox holds rows only for messages committed at the source.
ReceivedWasCommitted == \A m \in received : txState[m] = "committed"

\* Exactly once, end to end.  Every message committed at the source is
\* eventually processed at the destination; with Dst!EffectsOnce, its
\* effects happen once, however often the crossing was repeated.
EventuallyProcessed == \A m \in Msgs : (txState[m] = "committed") ~> (m \in processed)

Index(m) == CHOOSE i \in 1..Len(procOrder) : procOrder[i] = m

\* Order survives the crossing: messages of one URI are processed in the
\* order they were published, given one dispatcher per partition and no
\* causal dependencies.
EndToEndOrder ==
  \A a, b \in processed :
    (uriOf[a] = uriOf[b] /\ Src!Before(Src!Key(a), Src!Key(b))) => Index(a) < Index(b)

=============================================================================
