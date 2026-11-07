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
(* The inbox is cut by URI, as the bridge target addresses it, and has no  *)
(* causal dependencies: the conditions under which order is claimed to     *)
(* survive the crossing.                                                   *)
(*                                                                         *)
(* The intake takes every message (ADR-0036): a message it can never store *)
(* is set aside, its row written and committed in a transaction of its     *)
(* own, and only then does the bridge's subscriber return; the outbox      *)
(* counts it handled in a step after that, so a death can fall between the *)
(* two, and the message, delivered again, is the same row set aside.  An   *)
(* environment hold -- the inbox's database out of reach -- stops both the *)
(* store and the setting aside until it passes: it is never a reason to    *)
(* set a message aside.                                                    *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
  Msgs,
  Uris,
  SrcSlots,        \* slots of the source outbox
  DstSlots,        \* slots of the destination inbox, a dispatcher each
  BatchSize,
  MaxCrashes,      \* per side
  StoreBeforeAck,  \* TRUE: a message is stored in the inbox before the subscriber returns, as implemented
  SrcGroup,        \* the consumer group of the source dispatchers
  Unstorable,      \* SUBSET Msgs: the messages the intake can never store
  MaxHolds,        \* environment holds of the inbox's database
  RejectBeforeAck, \* TRUE: the row set aside commits before the subscriber returns, as implemented
  AckWithoutSetAside, \* a mutation: an unstorable message is acknowledged and set nowhere
  SetAsideOnHold,  \* a mutation: a message is set aside while the environment holds
  None

ASSUME Unstorable \subseteq Msgs

SrcDispatchers == {SrcGroup} \X SrcSlots

\* The source outbox, then the destination inbox.
VARIABLES uriOf, srcAssign, txState, xid, pos, nextXid, nextPos, position, batch, done, delivered, srcCrashes,
          part, received, recvPos, nextRecv, processed, effects, holding, procOrder, dstCrashes,
          attempts, due, parked, resolved, admin, waiting, expired,
          pending,  \* SUBSET Msgs: handed over but not yet stored, when the store is deferred
          rejected, \* SUBSET Msgs: set aside, the row committed
          rejecting, \* SUBSET Msgs: counted handled, the row set aside not yet committed (a mutation's)
          held,     \* the inbox's database is out of reach
          holds

Aside == <<rejected, rejecting, held, holds>>

vars == <<uriOf, srcAssign, txState, xid, pos, nextXid, nextPos, position, batch, done, delivered, srcCrashes,
          part, received, recvPos, nextRecv, processed, effects, holding, procOrder, dstCrashes,
          attempts, due, parked, resolved, admin, waiting, expired, pending, rejected, rejecting, held, holds>>

Src == INSTANCE Outbox WITH
  Groups <- {SrcGroup}, Slots <- SrcSlots, VisibilityRule <- TRUE, RehashAllowed <- FALSE,
  assign <- srcAssign, crashes <- srcCrashes

\* A destination failure is a crash here; recorded attempts, backoff and
\* parking are the inbox's own matter, checked in Inbox.tla.
Dst == INSTANCE Inbox WITH
  Deps <- [m \in Msgs |-> {}], Arrives <- Msgs \ Unstorable, Slots <- DstSlots,
  WaitsOnDependencies <- TRUE, WaitExpires <- FALSE,
  Poison <- {}, MaxAttempts <- 0, BlockOnBackoff <- TRUE, MaxAdmin <- 0, AtomicWait <- TRUE,
  crashes <- dstCrashes

(* ------------------------------------------------------------------------ *)

Init ==
  /\ Src!Init
  /\ Dst!Init
  \* the inbox is cut by URI: messages of one URI share a slot
  /\ \A a, b \in Msgs : uriOf[a] = uriOf[b] => part[a] = part[b]
  /\ pending = {}
  /\ rejected = {} /\ rejecting = {}
  /\ held = FALSE /\ holds = 0

SrcUnchanged == UNCHANGED <<uriOf, srcAssign, txState, xid, pos, nextXid, nextPos, position, batch, done, delivered, srcCrashes>>
DstUnchanged == UNCHANGED <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, dstCrashes,
                            attempts, due, parked, resolved, admin, waiting, expired>>

\* Every action names both sides, so that fairness over it is well defined.

Produce(m) == (Src!Begin(m) \/ Src!Publish(m) \/ Src!Commit(m) \/ Src!Abort(m)) /\ DstUnchanged /\ UNCHANGED <<pending, rejected, rejecting, held, holds>>
Ends(m) == (Src!Commit(m) \/ Src!Abort(m)) /\ DstUnchanged /\ UNCHANGED <<pending, rejected, rejecting, held, holds>>

SrcFetch(d) == Src!Fetch(d) /\ DstUnchanged /\ UNCHANGED <<pending, rejected, rejecting, held, holds>>
SrcAck(d) == Src!Ack(d) /\ DstUnchanged /\ UNCHANGED <<pending, rejected, rejecting, held, holds>>

\* The dispatcher process dies, or its subscriber fails: an in-flight batch
\* rolls back and the position stays; whatever was still to be stored is
\* gone with the process.
Die(d) ==
  /\ srcCrashes < MaxCrashes
  /\ batch[d] # <<>> \/ pending # {} \/ rejecting # {}
  /\ srcCrashes' = srcCrashes + 1
  /\ batch' = [batch EXCEPT ![d] = <<>>]
  /\ done' = [done EXCEPT ![d] = 0]
  /\ pending' = {}
  /\ rejecting' = {}
  /\ UNCHANGED <<uriOf, srcAssign, txState, xid, pos, nextXid, nextPos, position, delivered>>
  /\ DstUnchanged
  /\ UNCHANGED <<rejected, held, holds>>

\* The bridge: the next message of the batch is stored in the inbox, in a
\* transaction of the inbox's own, and the outbox counts it handled.  A
\* message stored before, redelivered after a crash, is the same row.
\* With StoreBeforeAck off, the message only counts as handled here and is
\* stored later, by Store: the shape of the Go channel API, where a message
\* was acknowledged on delivery to the consumer rather than on completion.
Upcoming(d) == batch[d][done[d] + 1]

Forward(d) ==
  /\ Src!Handle(d)
  /\ ~held
  /\ Upcoming(d) \notin Unstorable
  /\ UNCHANGED <<rejected, rejecting, held, holds>>
  /\ LET m == Upcoming(d)
     IN IF StoreBeforeAck
        THEN /\ UNCHANGED pending
             /\ IF m \in received THEN DstUnchanged ELSE Dst!Receive(m)
        ELSE /\ pending' = pending \cup {m}
             /\ DstUnchanged

Store(m) ==
  /\ m \in pending
  /\ ~held
  /\ pending' = pending \ {m}
  /\ IF m \in received THEN DstUnchanged ELSE Dst!Receive(m)
  /\ SrcUnchanged
  /\ UNCHANGED <<rejected, rejecting, held, holds>>

\* The intake sets the next message of the batch aside: its row commits in a
\* transaction of its own, and the outbox has not yet counted it.  A message
\* set aside before, delivered again after a death, is the same row, whose
\* count of sightings the model does not keep.  With SetAsideOnHold, the
\* mutation, a message is set aside because the environment holds.
SetAside(d) ==
  /\ done[d] < Len(batch[d])
  /\ RejectBeforeAck
  /\ Upcoming(d) \notin rejected
  /\ IF SetAsideOnHold THEN Upcoming(d) \in Unstorable \/ held ELSE Upcoming(d) \in Unstorable /\ ~held
  /\ rejected' = rejected \cup {Upcoming(d)}
  /\ UNCHANGED <<pending, rejecting, held, holds>>
  /\ SrcUnchanged /\ DstUnchanged

\* The subscriber returns on a message set aside, and the outbox counts it
\* handled.  With RejectBeforeAck off, the mutation, it is counted first and
\* its row written later, by Reject; with AckWithoutSetAside, the other
\* mutation, it is counted and set nowhere.
Acknowledge(d) ==
  /\ Src!Handle(d)
  /\ UNCHANGED pending /\ DstUnchanged /\ UNCHANGED <<held, holds>>
  /\ \/ /\ Upcoming(d) \in rejected
        /\ UNCHANGED <<rejected, rejecting>>
     \/ /\ ~RejectBeforeAck
        /\ Upcoming(d) \in Unstorable /\ ~held
        /\ rejecting' = rejecting \cup {Upcoming(d)}
        /\ UNCHANGED rejected
     \/ /\ AckWithoutSetAside
        /\ Upcoming(d) \in Unstorable
        /\ UNCHANGED <<rejected, rejecting>>

Reject(m) ==
  /\ m \in rejecting
  /\ ~held
  /\ rejecting' = rejecting \ {m}
  /\ rejected' = rejected \cup {m}
  /\ UNCHANGED <<pending, held, holds>>
  /\ SrcUnchanged /\ DstUnchanged

\* The inbox's database goes out of reach, and comes back.
Hold ==
  /\ ~held /\ holds < MaxHolds
  /\ held' = TRUE /\ holds' = holds + 1
  /\ UNCHANGED <<pending, rejected, rejecting>>
  /\ SrcUnchanged /\ DstUnchanged

Recover ==
  /\ held
  /\ held' = FALSE
  /\ UNCHANGED <<pending, rejected, rejecting, holds>>
  /\ SrcUnchanged /\ DstUnchanged

DstFetch(d) == Dst!Fetch(d) /\ SrcUnchanged /\ UNCHANGED <<pending, rejected, rejecting, held, holds>>
DstCommit(d) == Dst!Commit(d) /\ SrcUnchanged /\ UNCHANGED <<pending, rejected, rejecting, held, holds>>
DstCrash(d) == Dst!Crash(d) /\ SrcUnchanged /\ UNCHANGED <<pending, rejected, rejecting, held, holds>>

Step ==
  \/ \E m \in Msgs : Produce(m) \/ Store(m) \/ Reject(m)
  \/ \E d \in SrcDispatchers : SrcFetch(d) \/ Forward(d) \/ SetAside(d) \/ Acknowledge(d) \/ SrcAck(d) \/ Die(d)
  \/ \E d \in DstSlots : DstFetch(d) \/ DstCommit(d) \/ DstCrash(d)
  \/ Hold \/ Recover

Fairness ==
  /\ \A m \in Msgs : WF_vars(Ends(m))
  /\ \A m \in Msgs : WF_vars(Store(m)) /\ WF_vars(Reject(m))
  /\ \A d \in SrcDispatchers :
       WF_vars(SrcFetch(d)) /\ WF_vars(Forward(d)) /\ WF_vars(SetAside(d)) /\ WF_vars(Acknowledge(d)) /\ WF_vars(SrcAck(d))
  /\ \A d \in DstSlots : WF_vars(DstFetch(d)) /\ WF_vars(DstCommit(d))
  /\ WF_vars(Recover)

Spec == Init /\ [][Step]_vars /\ Fairness

(* ------------------------------------------------------------------------ *)
(* Properties                                                                 *)

TypeOK ==
  /\ Src!TypeOK /\ Dst!TypeOK /\ pending \subseteq Msgs
  /\ rejected \subseteq Msgs /\ rejecting \subseteq Msgs
  /\ held \in BOOLEAN /\ holds \in 0..MaxHolds

\* Each side keeps what it proved on its own.
SourceInvariants == Src!DeliveredCommitted /\ Src!NoPassedOver /\ Src!OrderPerUri
DestinationInvariants == Dst!EffectsOnce /\ Dst!EffectsIffProcessed /\ Dst!HeldOnce /\ Dst!ProcessedWasReceived

\* The inbox holds rows only for messages committed at the source.
ReceivedWasCommitted == \A m \in received : txState[m] = "committed"

\* Exactly once, end to end.  Every message committed at the source that the
\* intake can store is eventually processed at the destination; with
\* Dst!EffectsOnce, its effects happen once, however often the crossing was
\* repeated.
EventuallyProcessed ==
  \A m \in Msgs \ Unstorable : (txState[m] = "committed") ~> (m \in processed)

\* Every committed message the intake can never store is eventually set
\* aside: never lost, and never holding the source for ever (ADR-0036).
EventuallySetAside ==
  \A m \in Unstorable : (txState[m] = "committed") ~> (m \in rejected)

\* Only a message the intake can never store is set aside: an environment
\* hold, however long, diverts nothing (ADR-0036 decision 1).
SetAsideOnlyUnstorable == rejected \subseteq Unstorable

\* What is set aside was never stored, and the other way round.
SetAsideNeverStored == rejected \cap received = {}

Index(m) == CHOOSE i \in 1..Len(procOrder) : procOrder[i] = m

\* Order survives the crossing: messages of one URI are processed in the
\* order they were published, given a cut by URI and no causal
\* dependencies.
EndToEndOrder ==
  \A a, b \in processed :
    (uriOf[a] = uriOf[b] /\ Src!Before(Src!Key(a), Src!Key(b))) => Index(a) < Index(b)

=============================================================================
