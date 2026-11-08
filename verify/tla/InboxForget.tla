--------------------------- MODULE InboxForget ---------------------------
(***************************************************************************)
(* The inbox of Inbox.tla, forgetting processed rows (ADR-0033 decision   *)
(* 4).                                                                     *)
(*                                                                         *)
(* A processed row may be forgotten: its row goes, and with it the memory  *)
(* that its identity was processed.  An identity may arrive again while    *)
(* its horizon is open -- a broker's retention, a relay's retry, a         *)
(* producer's rewind within its declared replay horizon -- and a repeat of *)
(* a forgotten identity is a new row, processed again.  The horizon of a   *)
(* message closes once, for good: a deeper rewind is refused, a lane's     *)
(* retry ends with the database's error.  Inbox.tla has no repeats: a      *)
(* message arrives once, and a repeat of a stored identity changes         *)
(* nothing, so the model has no step for it.                               *)
(*                                                                         *)
(* The rule decides when a row may go:                                     *)
(*   "window"  -- once its horizon has closed, the window covering it, and *)
(*                never a row whose stream another message names as a      *)
(*                causal dependency: such a channel keeps its rows;        *)
(*   "early"   -- a window shorter than the horizon;                       *)
(*   "targets" -- a window over a channel of dependency targets;           *)
(*   "never"   -- nothing is forgotten, as before.                         *)
(* Never a row that is unprocessed, parked, held or named by a waiting     *)
(* row: the sweep's own conditions.                                        *)
(*                                                                         *)
(* A ghost, everProcessed, keeps what was ever marked, so that the         *)
(* properties of Inbox.tla that speak of what was processed are restated   *)
(* over what was, not over what the table still holds.  Safety only: the   *)
(* liveness of the inbox is Inbox.tla's.                                   *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
  Msgs, Deps, Arrives, Slots, MaxCrashes, WaitsOnDependencies, WaitExpires, Poison, MaxAttempts,
  BlockOnBackoff, AtomicWait, MaxAdmin, None,
  Repeats,    \* SUBSET Msgs: the messages that may arrive again while their horizon is open
  ForgetRule  \* "window", "early", "targets" or "never"

ASSUME Repeats \subseteq Msgs
ASSUME ForgetRule \in {"window", "early", "targets", "never"}

VARIABLES
  part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes,
  attempts, due, parked, resolved, admin, waiting, expired,
  forgotten,     \* SUBSET Msgs: forgotten and not received since
  horizonOpen,   \* [Msgs -> BOOLEAN]: a repeat may still arrive
  everProcessed  \* the ghost: every message ever marked

inboxVars == <<part, received, recvPos, nextRecv, processed, effects, holding, procOrder, crashes,
               attempts, due, parked, resolved, admin, waiting, expired>>
vars == <<inboxVars, forgotten, horizonOpen, everProcessed>>

I == INSTANCE Inbox WITH Unreadable <- {}, unread <- {}

\* The streams other messages depend on: a channel of them keeps its rows.
Targets == UNION {Deps[n] : n \in Msgs}

Init ==
  /\ I!Init
  /\ forgotten = {}
  /\ horizonOpen = [m \in Msgs |-> m \in Repeats]
  /\ everProcessed = {}

\* A step of Inbox.tla.  A row received here for an identity forgotten
\* before is a repeat: it arrives only while its horizon is open.
InboxStep ==
  /\ I!Next
  /\ \A m \in received' \ received : m \in forgotten => (m \in Repeats /\ horizonOpen[m])
  /\ forgotten' = forgotten \ received'
  /\ everProcessed' = everProcessed \cup processed'
  /\ UNCHANGED horizonOpen

\* The horizon of m closes, for good.
Close(m) ==
  /\ horizonOpen[m]
  /\ horizonOpen' = [horizonOpen EXCEPT ![m] = FALSE]
  /\ UNCHANGED <<inboxVars, forgotten, everProcessed>>

\* The sweep forgets a processed row, by the rule.
Forget(m) ==
  /\ ForgetRule # "never"
  /\ m \in processed
  /\ m \notin I!Locked
  /\ \A n \in Msgs : waiting[n] # m
  /\ ForgetRule \in {"window", "targets"} => ~horizonOpen[m]
  /\ ForgetRule \in {"window", "early"} => m \notin Targets
  /\ received' = received \ {m}
  /\ processed' = processed \ {m}
  /\ forgotten' = forgotten \cup {m}
  /\ UNCHANGED <<part, recvPos, nextRecv, effects, holding, procOrder, crashes, attempts, due, parked,
                resolved, admin, waiting, expired, horizonOpen, everProcessed>>

Next ==
  \/ InboxStep
  \/ \E m \in Msgs : Close(m) \/ Forget(m)

Spec == Init /\ [][Next]_vars

(* ------------------------------------------------------------------------ *)
(* Properties                                                                 *)

TypeOK ==
  /\ I!TypeOK
  /\ forgotten \subseteq Msgs
  /\ horizonOpen \in [Msgs -> BOOLEAN]
  /\ everProcessed \subseteq Msgs

\* Inbox.tla's, as they stand: a forgotten row is gone from the table.
EffectsOnce == I!EffectsOnce
HeldOnce == I!HeldOnce
ProcessedWasReceived == I!ProcessedWasReceived

\* Restated over what was ever processed.
EffectsIffEverProcessed ==
  \A m \in Msgs : (m \in everProcessed /\ m \notin resolved) <=> (effects[m] >= 1)

CausalOrderEver == \A m \in processed : Deps[m] \subseteq everProcessed

\* A row set aside waits for a dependency never processed: one processed and
\* forgotten would never wake it.
WaitingIsAsideEver ==
  \A m \in Msgs : waiting[m] # None =>
    /\ waiting[m] \in Deps[m] /\ waiting[m] \notin everProcessed
    /\ m \notin processed /\ m \notin parked

=============================================================================
