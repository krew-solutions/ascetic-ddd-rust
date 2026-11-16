---------------------------- MODULE InboxOutage ----------------------------
(***************************************************************************)
(* ADR-0040: a message that cannot be handled now stops exactly the        *)
(* messages that share its cause.                                          *)
(*                                                                         *)
(* The inbox of Inbox.tla reduced to what the outage protocol turns on --  *)
(* slots and their heads, attempts, parks -- with a stage that goes down,  *)
(* Tenants whose keys fail alone, and the subscriber's verdict and its     *)
(* scope.  A dispatch takes a slot's head and ends in one transaction:     *)
(*   - processed, when the stage and the head's Tenant serve;              *)
(*   - held (Defer), when the verdict is the stage's: the head keeps its   *)
(*     slot, its attempts untouched, until Budget holds; past it the       *)
(*     channel's evidence is weighed -- a row processed while it held --   *)
(*     and an unrefuted hold holds on, a refuted one spends (Outlast);     *)
(*   - spent, when the verdict is the Tenant's (FailTenant), parking the   *)
(*     row at MaxAttempts;                                                  *)
(*   - parked at once, when no retry mends it (FailPermanent).             *)
(* A Tenant's fault may be read as the stage's -- the verdict's scope is a *)
(* free choice then -- since the stage cannot always tell.  Every failure  *)
(* writes its cause, and every exit clears it.  An operator parks a held   *)
(* row (ParkHeld), re-arms parked ones with a canary of Tries failures     *)
(* (UnparkAll), or unparks one afresh (Unpark).                            *)
(*                                                                         *)
(* Dependencies, waits, crashes and supersedes are Inbox.tla's and left    *)
(* out: a hold changes none of them -- a held row is in the queue, at its  *)
(* slot's head, as a row waiting for its backoff is.  Every message is in  *)
(* the table from the start, in Order.                                     *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
  Msgs,
  Order,        \* a sequence of Msgs, each once: the order of arrival
  Slots,
  Tenants,
  TenantOf,     \* [Msgs -> Tenants]
  KeyOf,        \* [Msgs -> keys]: the partition key; a key's messages share a slot
  Outages,      \* how many times the stage may go down
  OutageEnds,   \* TRUE: a stage that is down comes back up
  TenantFaults, \* SUBSET Tenants: whose keys may fail alone, once each
  TenantEnds,   \* TRUE: a Tenant's fault ends
  Permanent,    \* SUBSET Msgs: messages no retry mends, such as a key gone
  MaxAttempts,  \* failed attempts after which a row is parked, > 0
  Budget,       \* holds before the channel's evidence is weighed, > 0
  Tries,        \* the failures a re-armed row has left
  MaxAdmin,     \* operator actions, in all
  None

ASSUME Order \in Seq(Msgs) /\ Len(Order) = Cardinality(Msgs)
ASSUME TenantOf \in [Msgs -> Tenants]
ASSUME TenantFaults \subseteq Tenants /\ Permanent \subseteq Msgs
ASSUME MaxAttempts > 0 /\ Budget > 0 /\ Tries \in Nat /\ MaxAdmin \in Nat /\ Outages \in Nat

Causes == {"stage", "tenant", "permanent"}

VARIABLES
  part,        \* [Msgs -> Slots], fixed at the start, a key's messages together
  processed,   \* SUBSET Msgs
  procOrder,   \* Seq(Msgs), the order of the marks
  due,         \* [Msgs -> BOOLEAN], FALSE while a row waits to be asked again
  attempts,    \* [Msgs -> Nat]
  parked,      \* SUBSET Msgs
  byHand,      \* SUBSET parked: parked by the operator from a hold
  cause,       \* [Msgs -> Causes \cup {None}]: last_cause
  held,        \* [Msgs -> 0..Budget]: holds since the hold began; 0 when none
  served,      \* [Msgs -> BOOLEAN]: a row was processed while this one held
  stageDown,
  outages,     \* the stage's outages so far
  tenantDown,  \* SUBSET Tenants
  faulted,     \* SUBSET Tenants: whose fault has happened
  rearmed,     \* SUBSET Msgs: re-armed by UnparkAll, not unparked afresh since
  admin

vars == <<part, processed, procOrder, due, attempts, parked, byHand, cause, held, served,
          stageDown, outages, tenantDown, faulted, rearmed, admin>>

Pos(m) == CHOOSE i \in 1..Len(Order) : Order[i] = m

Init ==
  /\ part \in {p \in [Msgs -> Slots] : \A a, b \in Msgs : KeyOf[a] = KeyOf[b] => p[a] = p[b]}
  /\ processed = {}
  /\ procOrder = <<>>
  /\ due = [m \in Msgs |-> TRUE]
  /\ attempts = [m \in Msgs |-> 0]
  /\ parked = {}
  /\ byHand = {}
  /\ cause = [m \in Msgs |-> None]
  /\ held = [m \in Msgs |-> 0]
  /\ served = [m \in Msgs |-> FALSE]
  /\ stageDown = FALSE
  /\ outages = 0
  /\ tenantDown = {}
  /\ faulted = {}
  /\ rearmed = {}
  /\ admin = 0

(* ------------------------------------------------------------------------ *)
(* The slot's head, and what the environment lets its dispatch do            *)

Queue(d) == {m \in Msgs : part[m] = d /\ m \notin processed /\ m \notin parked}
\* The oldest row of the queue, due or not: a row waiting to be asked again
\* holds its slot (ADR-0005), and only a head is ever asked.
HeadOf(d) == CHOOSE m \in Queue(d) : \A n \in Queue(d) : Pos(m) <= Pos(n)
Asked(d, m) == Queue(d) # {} /\ m = HeadOf(d) /\ due[m]

Serves(m) == ~stageDown /\ TenantOf[m] \notin tenantDown
Down(m) == stageDown \/ TenantOf[m] \in tenantDown

\* A failure that spends an attempt, under cause c; the last one parks.  A
\* hold refuted keeps its clock (keep), so that its next verdict is weighed
\* at once, until it parks.
Spend(m, c, keep) ==
  LET parks == attempts[m] + 1 >= MaxAttempts IN
  /\ attempts' = [attempts EXCEPT ![m] = @ + 1]
  /\ parked' = IF parks THEN parked \cup {m} ELSE parked
  /\ cause' = [cause EXCEPT ![m] = c]
  /\ held' = [held EXCEPT ![m] = IF keep /\ ~parks THEN @ ELSE 0]
  /\ due' = [due EXCEPT ![m] = FALSE]

(* ------------------------------------------------------------------------ *)
(* Dispatching                                                               *)

\* The head is processed: its cause and its hold cleared, and every row
\* holding meanwhile has the channel's evidence that rows are served.
Commit(d) ==
  \E m \in Msgs :
    /\ Asked(d, m)
    /\ m \notin Permanent
    /\ Serves(m)
    /\ processed' = processed \cup {m}
    /\ procOrder' = Append(procOrder, m)
    /\ cause' = [cause EXCEPT ![m] = None]
    /\ held' = [held EXCEPT ![m] = 0]
    /\ served' = [n \in Msgs |-> served[n] \/ held[n] > 0]
    /\ UNCHANGED <<part, due, attempts, parked, byHand, stageDown, outages, tenantDown, faulted,
                  rearmed, admin>>

\* The verdict is the stage's: the head holds its slot, its attempts
\* untouched, under Budget or past it unrefuted.  A hold that begins starts
\* its evidence afresh.
Defer(d) ==
  \E m \in Msgs :
    /\ Asked(d, m)
    /\ m \notin Permanent
    /\ Down(m)
    /\ (held[m] < Budget \/ ~served[m])
    /\ held' = [held EXCEPT ![m] = IF @ < Budget THEN @ + 1 ELSE Budget]
    /\ served' = IF held[m] = 0 THEN [served EXCEPT ![m] = FALSE] ELSE served
    /\ cause' = [cause EXCEPT ![m] = "stage"]
    /\ due' = [due EXCEPT ![m] = FALSE]
    /\ UNCHANGED <<part, processed, procOrder, attempts, parked, byHand, stageDown, outages,
                  tenantDown, faulted, rearmed, admin>>

\* Past its budget, refuted by the channel's evidence: the stage's verdict
\* spends an attempt, as a Tenant's does, and keeps the hold's clock.
Outlast(d) ==
  \E m \in Msgs :
    /\ Asked(d, m)
    /\ m \notin Permanent
    /\ Down(m)
    /\ held[m] = Budget /\ served[m]
    /\ Spend(m, "stage", TRUE)
    /\ UNCHANGED <<part, processed, procOrder, byHand, served, stageDown, outages, tenantDown,
                  faulted, rearmed, admin>>

\* The verdict is the Tenant's, read right: an attempt spent, the hold over.
FailTenant(d) ==
  \E m \in Msgs :
    /\ Asked(d, m)
    /\ m \notin Permanent
    /\ ~stageDown /\ TenantOf[m] \in tenantDown
    /\ Spend(m, "tenant", FALSE)
    /\ UNCHANGED <<part, processed, procOrder, byHand, served, stageDown, outages, tenantDown,
                  faulted, rearmed, admin>>

\* No retry mends it: parked at once.
FailPermanent(d) ==
  \E m \in Msgs :
    /\ Asked(d, m)
    /\ m \in Permanent
    /\ attempts' = [attempts EXCEPT ![m] = @ + 1]
    /\ parked' = parked \cup {m}
    /\ cause' = [cause EXCEPT ![m] = "permanent"]
    /\ held' = [held EXCEPT ![m] = 0]
    /\ due' = [due EXCEPT ![m] = FALSE]
    /\ UNCHANGED <<part, processed, procOrder, byHand, served, stageDown, outages, tenantDown,
                  faulted, rearmed, admin>>

Dispatch(d) == Commit(d) \/ Defer(d) \/ Outlast(d) \/ FailTenant(d) \/ FailPermanent(d)

\* The while a row waits to be asked again has passed.
Elapse(m) ==
  /\ ~due[m]
  /\ due' = [due EXCEPT ![m] = TRUE]
  /\ UNCHANGED <<part, processed, procOrder, attempts, parked, byHand, cause, held, served,
                stageDown, outages, tenantDown, faulted, rearmed, admin>>

(* ------------------------------------------------------------------------ *)
(* The environment                                                           *)

StageFails ==
  /\ ~stageDown /\ outages < Outages
  /\ stageDown' = TRUE /\ outages' = outages + 1
  /\ UNCHANGED <<part, processed, procOrder, due, attempts, parked, byHand, cause, held, served,
                tenantDown, faulted, rearmed, admin>>

StageMends ==
  /\ stageDown /\ stageDown' = FALSE
  /\ UNCHANGED <<part, processed, procOrder, due, attempts, parked, byHand, cause, held, served,
                outages, tenantDown, faulted, rearmed, admin>>

TenantFails(t) ==
  /\ t \in TenantFaults \ faulted
  /\ tenantDown' = tenantDown \cup {t} /\ faulted' = faulted \cup {t}
  /\ UNCHANGED <<part, processed, procOrder, due, attempts, parked, byHand, cause, held, served,
                stageDown, outages, rearmed, admin>>

TenantMends(t) ==
  /\ t \in tenantDown /\ tenantDown' = tenantDown \ {t}
  /\ UNCHANGED <<part, processed, procOrder, due, attempts, parked, byHand, cause, held, served,
                stageDown, outages, faulted, rearmed, admin>>

(* ------------------------------------------------------------------------ *)
(* The operator                                                              *)

\* park_held: a held row parked under its cause, so that its slot goes on.
ParkHeld(m) ==
  /\ held[m] > 0 /\ m \notin parked /\ m \notin processed
  /\ admin < MaxAdmin /\ admin' = admin + 1
  /\ parked' = parked \cup {m} /\ byHand' = byHand \cup {m}
  /\ held' = [held EXCEPT ![m] = 0]
  /\ UNCHANGED <<part, processed, procOrder, due, attempts, cause, served, stageDown, outages,
                tenantDown, faulted, rearmed>>

\* unpark_all: a parked row re-armed with a canary of Tries failures left;
\* it keeps its last cause, since it keeps its attempts.
UnparkAll(m) ==
  /\ m \in parked
  /\ admin < MaxAdmin /\ admin' = admin + 1
  /\ parked' = parked \ {m} /\ byHand' = byHand \ {m}
  /\ attempts' = [attempts EXCEPT ![m] = IF @ >= MaxAttempts - Tries THEN @ ELSE MaxAttempts - Tries]
  /\ held' = [held EXCEPT ![m] = 0]
  /\ due' = [due EXCEPT ![m] = TRUE]
  /\ rearmed' = rearmed \cup {m}
  /\ UNCHANGED <<part, processed, procOrder, cause, served, stageDown, outages, tenantDown,
                faulted>>

\* unpark: a parked row given another go afresh, its cause and attempts gone.
Unpark(m) ==
  /\ m \in parked
  /\ admin < MaxAdmin /\ admin' = admin + 1
  /\ parked' = parked \ {m} /\ byHand' = byHand \ {m}
  /\ attempts' = [attempts EXCEPT ![m] = 0]
  /\ cause' = [cause EXCEPT ![m] = None]
  /\ held' = [held EXCEPT ![m] = 0]
  /\ due' = [due EXCEPT ![m] = TRUE]
  /\ rearmed' = rearmed \ {m}
  /\ UNCHANGED <<part, processed, procOrder, served, stageDown, outages, tenantDown, faulted>>

(* ------------------------------------------------------------------------ *)

Next ==
  \/ \E d \in Slots : Dispatch(d)
  \/ \E m \in Msgs : Elapse(m) \/ ParkHeld(m) \/ UnparkAll(m) \/ Unpark(m)
  \/ StageFails \/ StageMends
  \/ \E t \in Tenants : TenantFails(t) \/ TenantMends(t)

\* Every dispatcher keeps asking its head; every while passes; a stage or a
\* Tenant that comes back, comes back where the instance says so.
Fairness ==
  /\ \A d \in Slots : WF_vars(Dispatch(d))
  /\ \A m \in Msgs : WF_vars(Elapse(m))
  /\ IF OutageEnds THEN WF_vars(StageMends) ELSE TRUE
  /\ IF TenantEnds THEN \A t \in Tenants : WF_vars(TenantMends(t)) ELSE TRUE

Spec == Init /\ [][Next]_vars /\ Fairness

(* ------------------------------------------------------------------------ *)
(* Properties                                                                *)

TypeOK ==
  /\ part \in [Msgs -> Slots]
  /\ processed \subseteq Msgs
  /\ due \in [Msgs -> BOOLEAN]
  /\ attempts \in [Msgs -> Nat]
  /\ parked \subseteq Msgs /\ byHand \subseteq parked
  /\ cause \in [Msgs -> Causes \cup {None}]
  /\ held \in [Msgs -> 0..Budget]
  /\ served \in [Msgs -> BOOLEAN]
  /\ rearmed \subseteq Msgs

\* A hold spends no attempt (decision 4).  An action property, so that an
\* instance whose Defer counts is caught at its first hold.
HoldSpendsNothing == [][\A d \in Slots : Defer(d) => attempts' = attempts]_vars

\* A cause explains a state that lasts: a row not processed that is parked,
\* has spent attempts, holds, or was re-armed keeping its attempts and so its
\* last cause.  Every exit that ends that state clears it.
CauseIsCurrent ==
  \A m \in Msgs : cause[m] # None =>
    /\ m \notin processed
    /\ (m \in parked \/ attempts[m] > 0 \/ held[m] > 0 \/ m \in rearmed)

\* A held row is in the queue: neither parked nor processed.
HeldIsQueued == \A m \in Msgs : held[m] > 0 => m \notin parked /\ m \notin processed

\* A parked row got there by its attempts, a permanent verdict, or the
\* operator's hand from a hold.
ParkedIsAside ==
  \A m \in parked : attempts[m] >= MaxAttempts \/ m \in Permanent \/ m \in byHand

\* A row re-armed has at most Tries failures left before it parks again.
CanaryReparks == \A m \in rearmed : attempts[m] >= MaxAttempts - Tries

\* With no Tenant faulting alone and no operator, the stage's outage parks
\* nothing: a stage's verdict holds, never spends, unless refuted -- and
\* with one outage no row is served while another holds.
NoStageParks == \A m \in parked : cause[m] # "stage"

Index(m) == CHOOSE i \in 1..Len(procOrder) : procOrder[i] = m

\* Rows of one partition key are processed in their order of arrival: the
\* order a host can rely on, since which keys share a slot is the hash's
\* accident.  An instance without parks keeps it through any outage.
KeyOrder ==
  \A a, b \in processed :
    (KeyOf[a] = KeyOf[b] /\ Pos(a) < Pos(b)) => Index(a) < Index(b)

\* Every row no retry fails for good is processed, however long the stage
\* is down, once it comes back.
EventuallyProcessed == \A m \in Msgs \ Permanent : <>(m \in processed)

\* Every row ends processed or parked.  A stage down for good holds its
\* rows for good, by design: parking frees nothing, and the hold pages.
EventuallyOutOfTheWay == \A m \in Msgs : <>(m \in processed \/ m \in parked)

=============================================================================
