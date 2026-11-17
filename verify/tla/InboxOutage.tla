---------------------------- MODULE InboxOutage ----------------------------
(***************************************************************************)
(* ADR-0042: a stage's outage pauses its channel in the process that sees  *)
(* it, and nothing is parked for it.                                       *)
(*                                                                         *)
(* The inbox of Inbox.tla reduced to what the outage protocol turns on,    *)
(* over ADR-0041's heads: a (slot, Tenant)'s head is its oldest queue row, *)
(* and a head held holds its own (slot, Tenant) alone.  Several processes  *)
(* share the table, each with workers -- its loops -- and each with its   *)
(* own breaker over the channel and its own memo of Tenants:               *)
(*   - an ask is split into its start, which locks the head and its slot,  *)
(*     and its verdict, which commits; between the two a worker may roll  *)
(*     back, writing nothing (AskStart, AskEnd, Rollback);                 *)
(*   - the verdict is the environment's: a row is served when the stage is *)
(*     up for that process -- not down, not cut off from it -- or when the *)
(*     process holds its Tenant's key cached; a Tenant whose keys fail     *)
(*     alone answers `Unavailable` at Tenant scope; the stage, at Stage    *)
(*     scope; a permanent row parks at once; a flaky one fails its first   *)
(*     time on its own, spending an attempt;                               *)
(*   - an `Unavailable` holds its row and spends nothing, at either scope; *)
(*     an ask's, of either scope, is remembered in the process's memo,     *)
(*     which holds the Tenant's other heads unasked (HoldUnasked) until it *)
(*     runs out, and then lets one ask through, in flight, at a time; a    *)
(*     trial's Stage verdict is the breaker's, not the memo's;             *)
(*   - a Stage `Unavailable` may open the process's breaker -- the         *)
(*     thresholds are the core's, and the model takes either branch; an   *)
(*     open breaker asks no head but by its trial, one at a time, which a  *)
(*     processed verdict closes; a circle that finds no head closes it.    *)
(* A head whose dependency is not processed is set aside to wait, and the  *)
(* mark of the dependency puts it back.  The expiry parks a waiter whose   *)
(* chain reaches no queued row -- its dependency never arrived, is parked, *)
(* or waits on one that does not reach one -- and spares the others        *)
(* (ADR-0042 decision 7); an expired row keeps the identity it waited for, *)
(* and its dependency's mark still wakes it.  The operator re-arms parked  *)
(* rows with exactly Tries failures left, gives a held head up, or gives a *)
(* Tenant's queue up from its newest row, a row a step, as the verb's      *)
(* batches interleave with the dispatch.  Every row is in the table from   *)
(* the start, in Order, but those of Missing, which never arrive.          *)
(*                                                                         *)
(* Mutant names a refuted design, "none" the decided one: check.sh runs    *)
(* each refuted configuration's twin with "none", where the one property   *)
(* it names must hold.                                                     *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
  Msgs,
  Order,        \* a sequence of the rows that arrive, each once: the order of arrival
  Missing,      \* SUBSET Msgs: rows that never arrive, named by a dependency only
  Slots,
  SlotOf,       \* [Msgs -> Slots]: a key's rows share a slot
  Tenants,
  TenantOf,     \* [Msgs -> Tenants]
  KeyOf,        \* [Msgs -> keys]: a stream; its rows in its order
  Dep,          \* [Msgs -> Msgs \cup {None}]: the causal dependency, one at most
  Procs,
  Workers,
  ProcOf,       \* [Workers -> Procs]
  Outages,      \* how many times the stage may go down
  OutageEnds,   \* TRUE: a stage that is down comes back up
  CutOffs,      \* SUBSET Procs: processes that may be cut off from the stage alone, once each
  CutEnds,      \* TRUE: a cut-off process reaches the stage again
  Cached,       \* [Procs -> SUBSET Tenants]: whose keys a process holds cached
  TenantFaults, \* SUBSET Tenants: whose keys may fail alone, once each
  TenantEnds,   \* TRUE: a Tenant's fault ends
  Permanent,    \* SUBSET Msgs: rows no retry mends
  Flaky,        \* SUBSET Msgs: rows whose first verdict is their own failure, spending an attempt
  MaxAttempts,  \* failed attempts after which a row parks, > 0
  Tries,        \* the failures a re-armed row has left, 1..MaxAttempts
  HoldCap,      \* the most holds a row counts
  MaxAdmin,     \* operator actions, in all
  MaxRollbacks, \* rollbacks between an ask's start and its verdict, in all
  Mutant,
  None

Arrives == Msgs \ Missing

ASSUME Order \in Seq(Arrives) /\ Len(Order) = Cardinality(Arrives)
ASSUME TenantOf \in [Msgs -> Tenants] /\ SlotOf \in [Msgs -> Slots] /\ ProcOf \in [Workers -> Procs]
ASSUME Dep \in [Msgs -> Msgs \cup {None}]
ASSUME MaxAttempts > 0 /\ Tries \in 1..MaxAttempts /\ HoldCap \in Nat \ {0}

Causes == {"stage", "tenant", "own", "permanent", "given"}
Kinds == {"none", "backoff", "unavailable"}
Memos == {"none", "held", "due", "flight"}

VARIABLES
  processed,   \* SUBSET Arrives
  procOrder,   \* Seq(Arrives): the order of the marks
  attempts,    \* [Msgs -> Nat]
  parked,      \* SUBSET Arrives
  byHand,      \* SUBSET parked: given up by the operator, its cause kept
  expired,     \* SUBSET parked: parked by the expiry, its dependency kept
  waiting,     \* SUBSET Arrives: set aside for its dependency
  cause,       \* [Msgs -> Causes \cup {None}]
  kind,        \* [Msgs -> Kinds]: what holds a row
  holds,       \* [Msgs -> 0..HoldCap]: the Unavailable verdicts it was told
  due,         \* [Msgs -> BOOLEAN]: FALSE while a hold or a back-off runs
  flaked,      \* SUBSET Flaky: whose own failure has happened
  rearmed,     \* SUBSET Arrives: re-armed by the operator, not told a verdict since
  stageDown, outages,
  cut,         \* SUBSET Procs: cut off from the stage
  cutDone,     \* SUBSET Procs: whose cut has happened
  tenantDown, faulted,
  breaker,     \* [Procs -> {"closed", "open"}]
  memo,        \* [Procs -> [Tenants -> Memos]]
  flight,      \* [Workers -> Arrives \cup {None}]: the head a worker's open ask holds
  trial,       \* [Workers -> BOOLEAN]: whether that ask is its process's trial
  admin, rollbacks

rows == <<processed, procOrder, attempts, parked, byHand, expired, waiting, cause, kind, holds,
          due, flaked, rearmed>>
env == <<stageDown, outages, cut, cutDone, tenantDown, faulted>>
vars == <<rows, env, breaker, memo, flight, trial, admin, rollbacks>>

Pos(m) == CHOOSE i \in 1..Len(Order) : Order[i] = m

Init ==
  /\ processed = {} /\ procOrder = <<>>
  /\ attempts = [m \in Msgs |-> 0]
  /\ parked = {} /\ byHand = {} /\ expired = {} /\ waiting = {}
  /\ cause = [m \in Msgs |-> None]
  /\ kind = [m \in Msgs |-> "none"]
  /\ holds = [m \in Msgs |-> 0]
  /\ due = [m \in Msgs |-> TRUE]
  /\ flaked = {} /\ rearmed = {}
  /\ stageDown = FALSE /\ outages = 0 /\ cut = {} /\ cutDone = {}
  /\ tenantDown = {} /\ faulted = {}
  /\ breaker = [p \in Procs |-> "closed"]
  /\ memo = [p \in Procs |-> [t \in Tenants |-> "none"]]
  /\ flight = [w \in Workers |-> None]
  /\ trial = [w \in Workers |-> FALSE]
  /\ admin = 0 /\ rollbacks = 0

(* ------------------------------------------------------------------------ *)
(* The queue and its heads                                                   *)

Queued(m) == m \in Arrives /\ m \notin processed /\ m \notin parked /\ m \notin waiting
QueueOf(d, t) == {m \in Arrives : Queued(m) /\ SlotOf[m] = d /\ TenantOf[m] = t}
\* A (slot, Tenant)'s head, due or not: a head held holds its own.
Heads == {m \in Arrives : Queued(m) /\ \A n \in QueueOf(SlotOf[m], TenantOf[m]) : Pos(m) <= Pos(n)}
Asking == {w \in Workers : flight[w] # None}
InFlight == {flight[w] : w \in Asking}
SlotBusy(d) == \E w \in Asking : SlotOf[flight[w]] = d
DepDone(m) == Dep[m] = None \/ Dep[m] \in processed

\* Whether a waiter's chain reaches a queued row: its dependency queued, or
\* itself waiting on one that reaches it (ADR-0042 decision 7).
RECURSIVE Spared(_, _)
Spared(m, n) ==
  /\ n > 0
  /\ Dep[m] # None
  /\ Dep[m] \in Arrives /\ Dep[m] \notin parked /\ Dep[m] \notin processed
  /\ (Dep[m] \notin waiting \/ Spared(Dep[m], n - 1))

Reachable(p, m) == (~stageDown /\ p \notin cut) \/ TenantOf[m] \in Cached[p]

(* ------------------------------------------------------------------------ *)
(* A worker's ask                                                            *)

\* A head is asked: by a closed breaker when it is due and its Tenant's memo
\* lets it through -- one ask in flight once the memo holds the Tenant; by
\* an open one only as its trial, one at a time, held or not.  The head's
\* slot and row are locked until the verdict.
AskStart(w) ==
  LET p == ProcOf[w] IN
  /\ flight[w] = None
  /\ \E m \in Heads :
       /\ ~SlotBusy(SlotOf[m])
       /\ DepDone(m)
       /\ \/ /\ breaker[p] = "closed" /\ due[m]
             /\ IF Mutant = "no_flight_mark"
                  THEN memo[p][TenantOf[m]] # "held"
                  ELSE memo[p][TenantOf[m]] \in {"none", "due"}
             /\ memo' = IF memo[p][TenantOf[m]] \in {"due", "flight"}
                          THEN [memo EXCEPT ![p][TenantOf[m]] = "flight"] ELSE memo
             /\ trial' = trial
          \/ /\ breaker[p] = "open"
             /\ \A v \in Workers : ProcOf[v] = p => ~trial[v]
             /\ trial' = [trial EXCEPT ![w] = TRUE]
             /\ memo' = memo
       /\ flight' = [flight EXCEPT ![w] = m]
  /\ UNCHANGED <<rows, env, breaker, admin, rollbacks>>

\* A head whose dependency is not processed is set aside, the whole
\* transaction (ADR-0009).
SetAside(w) ==
  /\ flight[w] = None /\ breaker[ProcOf[w]] = "closed"
  /\ \E m \in Heads :
       /\ ~SlotBusy(SlotOf[m]) /\ due[m] /\ ~DepDone(m)
       /\ waiting' = waiting \cup {m}
  /\ UNCHANGED <<processed, procOrder, attempts, parked, byHand, expired, cause, kind, holds, due,
                flaked, rearmed, env, breaker, memo, flight, trial, admin, rollbacks>>

\* A head of a Tenant the process's memo holds is held unasked: its row
\* written held under the memo's cause, its count of verdicts untouched.  The
\* memo's cause is its latest verdict's, of either scope; the model writes an
\* Unavailable's cause, "tenant", the only fact of it the properties read.
HoldUnasked(w) ==
  LET p == ProcOf[w] IN
  /\ flight[w] = None /\ breaker[p] = "closed"
  /\ \E m \in Heads :
       /\ ~SlotBusy(SlotOf[m]) /\ due[m]
       /\ memo[p][TenantOf[m]] \in {"held", "flight"}
       /\ kind' = [kind EXCEPT ![m] = "unavailable"]
       /\ cause' = [cause EXCEPT ![m] = "tenant"]
       /\ due' = [due EXCEPT ![m] = FALSE]
  /\ UNCHANGED <<processed, procOrder, attempts, parked, byHand, expired, waiting, holds, flaked,
                rearmed, env, breaker, memo, flight, trial, admin, rollbacks>>

\* The rows woken by the mark of m: its waiters, and the waiters the expiry
\* parked, which kept the identity they waited for.
Woken(m) == {n \in waiting \cup expired : Dep[n] = m}

\* A verdict that spends an attempt under cause c; the last one parks.
Spend(m, c) ==
  LET parks == attempts[m] + 1 >= MaxAttempts IN
  /\ attempts' = [attempts EXCEPT ![m] = @ + 1]
  /\ parked' = IF parks THEN parked \cup {m} ELSE parked
  /\ cause' = [cause EXCEPT ![m] = c]
  /\ kind' = [kind EXCEPT ![m] = IF parks THEN "none" ELSE "backoff"]
  /\ holds' = [holds EXCEPT ![m] = 0]
  /\ due' = [due EXCEPT ![m] = FALSE]
  /\ rearmed' = rearmed \ {m}

\* An `Unavailable`: the row held and counted, nothing spent.
Hold(m, c) ==
  /\ kind' = [kind EXCEPT ![m] = "unavailable"]
  /\ holds' = [holds EXCEPT ![m] = IF @ < HoldCap THEN @ + 1 ELSE @]
  /\ cause' = [cause EXCEPT ![m] = c]
  /\ due' = [due EXCEPT ![m] = FALSE]
  /\ rearmed' = rearmed \ {m}
  /\ IF Mutant = "unavailable_spends"
       THEN /\ attempts' = [attempts EXCEPT ![m] = @ + 1]
            /\ parked' = IF attempts[m] + 1 >= MaxAttempts THEN parked \cup {m} ELSE parked
       ELSE UNCHANGED <<attempts, parked>>

\* The verdict, committed with the ask's transaction.
AskEnd(w) ==
  LET p == ProcOf[w]
      m == flight[w]
      t == TenantOf[flight[w]]
  IN
  /\ flight[w] # None
  /\ flight' = [flight EXCEPT ![w] = None]
  /\ trial' = [trial EXCEPT ![w] = FALSE]
  /\ \/ \* processed: its hold and its cause cleared, its waiters woken
        /\ Reachable(p, m) /\ t \notin tenantDown /\ m \notin Permanent
        /\ (m \notin Flaky \/ m \in flaked)
        /\ processed' = processed \cup {m}
        /\ procOrder' = Append(procOrder, m)
        /\ waiting' = waiting \ Woken(m)
        /\ parked' = parked \ Woken(m)
        /\ expired' = expired \ Woken(m)
        /\ cause' = [cause EXCEPT ![m] = IF Mutant = "mark_keeps_cause" THEN @ ELSE None]
        /\ kind' = [kind EXCEPT ![m] = IF Mutant = "mark_keeps_hold" THEN @ ELSE "none"]
        /\ holds' = [holds EXCEPT ![m] = IF Mutant = "mark_keeps_hold" THEN @ ELSE 0]
        /\ due' = [n \in Msgs |-> IF n \in Woken(m) THEN TRUE ELSE due[n]]
        /\ rearmed' = rearmed \ {m}
        /\ memo' = [memo EXCEPT ![p][t] = "none"]
        /\ breaker' = IF trial[w] THEN [breaker EXCEPT ![p] = "closed"] ELSE breaker
        /\ UNCHANGED <<attempts, byHand, flaked>>
     \/ \* its own failure, once
        /\ Reachable(p, m) /\ m \in Flaky \ flaked /\ m \notin Permanent
        /\ flaked' = flaked \cup {m}
        /\ Spend(m, "own")
        /\ memo' = [memo EXCEPT ![p][t] = "none"]
        /\ UNCHANGED <<processed, procOrder, waiting, byHand, expired, breaker>>
     \/ \* no retry mends it
        /\ Reachable(p, m) /\ m \in Permanent
        /\ attempts' = [attempts EXCEPT ![m] = @ + 1]
        /\ parked' = parked \cup {m}
        /\ cause' = [cause EXCEPT ![m] = "permanent"]
        /\ kind' = [kind EXCEPT ![m] = "none"]
        /\ holds' = [holds EXCEPT ![m] = 0]
        /\ rearmed' = rearmed \ {m}
        /\ memo' = [memo EXCEPT ![p][t] = "none"]
        /\ UNCHANGED <<processed, procOrder, waiting, byHand, expired, due, flaked, breaker>>
     \/ \* the Tenant's Unavailable, remembered by the process
        /\ Reachable(p, m) /\ t \in tenantDown /\ m \notin Permanent
        /\ Hold(m, "tenant")
        /\ memo' = [memo EXCEPT ![p][t] = "held"]
        /\ UNCHANGED <<processed, procOrder, waiting, byHand, expired, flaked, breaker>>
     \/ \* the stage's Unavailable: remembered by the process as a Tenant's is,
        \* but for a trial's, which is the breaker's; the breaker may open, or
        \* stay as it is
        /\ ~Reachable(p, m)
        /\ Hold(m, "stage")
        /\ breaker' \in {breaker, [breaker EXCEPT ![p] = "open"]}
        /\ memo' = IF trial[w] THEN memo ELSE [memo EXCEPT ![p][t] = "held"]
        /\ UNCHANGED <<processed, procOrder, waiting, byHand, expired, flaked>>
  /\ UNCHANGED <<env, admin, rollbacks>>

\* Between the start and the verdict the transaction rolls back: nothing
\* written, the flight's mark abandoned.
Rollback(w) ==
  LET p == ProcOf[w] IN
  /\ flight[w] # None /\ rollbacks < MaxRollbacks
  /\ rollbacks' = rollbacks + 1
  /\ flight' = [flight EXCEPT ![w] = None]
  /\ trial' = [trial EXCEPT ![w] = FALSE]
  /\ memo' = IF memo[p][TenantOf[flight[w]]] = "flight"
               THEN [memo EXCEPT ![p][TenantOf[flight[w]]] = "due"] ELSE memo
  /\ UNCHANGED <<rows, env, breaker, admin>>

\* A guest's circle that finds no head of the channel closes the breaker.
Circle(p) ==
  /\ breaker[p] = "open" /\ Heads = {}
  /\ \A w \in Workers : ProcOf[w] = p => ~trial[w]
  /\ breaker' = [breaker EXCEPT ![p] = "closed"]
  /\ UNCHANGED <<rows, env, memo, flight, trial, admin, rollbacks>>

(* ------------------------------------------------------------------------ *)
(* Time                                                                      *)

Elapse(m) ==
  /\ ~due[m] /\ m \notin InFlight
  /\ due' = [due EXCEPT ![m] = TRUE]
  /\ UNCHANGED <<processed, procOrder, attempts, parked, byHand, expired, waiting, cause, kind,
                holds, flaked, rearmed, env, breaker, memo, flight, trial, admin, rollbacks>>

\* A Tenant's memo runs out: one ask may go through.
MemoElapse(p, t) ==
  /\ memo[p][t] = "held"
  /\ memo' = [memo EXCEPT ![p][t] = "due"]
  /\ UNCHANGED <<rows, env, breaker, flight, trial, admin, rollbacks>>

\* A waiter whose chain reaches no queued row is parked, the identity it
\* waited for kept, so that a late mark still wakes it.
Expire(m) ==
  /\ m \in waiting
  /\ IF Mutant = "expire_present" THEN TRUE ELSE ~Spared(m, Cardinality(Msgs))
  /\ waiting' = waiting \ {m}
  /\ parked' = parked \cup {m}
  /\ expired' = expired \cup {m}
  /\ UNCHANGED <<processed, procOrder, attempts, byHand, cause, kind, holds, due, flaked, rearmed,
                env, breaker, memo, flight, trial, admin, rollbacks>>

(* ------------------------------------------------------------------------ *)
(* The environment                                                           *)

StageFails ==
  /\ ~stageDown /\ outages < Outages
  /\ stageDown' = TRUE /\ outages' = outages + 1
  /\ UNCHANGED <<rows, cut, cutDone, tenantDown, faulted, breaker, memo, flight, trial, admin,
                rollbacks>>

StageMends ==
  /\ stageDown /\ OutageEnds /\ stageDown' = FALSE
  /\ UNCHANGED <<rows, outages, cut, cutDone, tenantDown, faulted, breaker, memo, flight, trial,
                admin, rollbacks>>

CutOff(p) ==
  /\ p \in CutOffs \ cutDone
  /\ cut' = cut \cup {p} /\ cutDone' = cutDone \cup {p}
  /\ UNCHANGED <<rows, stageDown, outages, tenantDown, faulted, breaker, memo, flight, trial,
                admin, rollbacks>>

CutMends(p) ==
  /\ p \in cut /\ CutEnds /\ cut' = cut \ {p}
  /\ UNCHANGED <<rows, stageDown, outages, cutDone, tenantDown, faulted, breaker, memo, flight,
                trial, admin, rollbacks>>

TenantFails(t) ==
  /\ t \in TenantFaults \ faulted
  /\ tenantDown' = tenantDown \cup {t} /\ faulted' = faulted \cup {t}
  /\ UNCHANGED <<rows, stageDown, outages, cut, cutDone, breaker, memo, flight, trial, admin,
                rollbacks>>

TenantMends(t) ==
  /\ t \in tenantDown /\ TenantEnds /\ tenantDown' = tenantDown \ {t}
  /\ UNCHANGED <<rows, stageDown, outages, cut, cutDone, faulted, breaker, memo, flight, trial,
                admin, rollbacks>>

(* ------------------------------------------------------------------------ *)
(* The operator                                                              *)

\* A row a later row of its key overtook: processed past it.
Overtaken(m) == \E n \in processed : KeyOf[n] = KeyOf[m] /\ Pos(n) > Pos(m)

\* unpark_all: a parked row on the failure ground re-armed with exactly
\* Tries failures left, its cause gone; one a later row of its key overtook
\* stays parked, unless asked for, which the model never asks.  Its slot is
\* locked for the statement, `SKIP LOCKED`: an ask holds its slot from its
\* start to its verdict, so no later row of the key is in flight while the
\* overtaking is read, and none can be processed past the row re-armed.
UnparkAll(m) ==
  /\ m \in parked \ expired /\ m \notin Permanent /\ ~Overtaken(m)
  /\ (Mutant = "rearm_beside_an_ask" \/ ~SlotBusy(SlotOf[m]))
  /\ admin < MaxAdmin /\ admin' = admin + 1
  /\ parked' = parked \ {m} /\ byHand' = byHand \ {m}
  /\ attempts' = [attempts EXCEPT ![m] =
                    IF Mutant = "canary_keeps" /\ @ > MaxAttempts - Tries
                      THEN @ ELSE MaxAttempts - Tries]
  /\ cause' = [cause EXCEPT ![m] = None]
  /\ due' = [due EXCEPT ![m] = TRUE]
  /\ rearmed' = rearmed \cup {m}
  /\ UNCHANGED <<processed, procOrder, expired, waiting, kind, holds, flaked, env, breaker, memo,
                flight, trial, rollbacks>>

\* park_heads: a held head given up under its cause, the rows behind it
\* going on.  A head an ask holds is passed by.
ParkHeads(m) ==
  /\ m \in Heads /\ kind[m] = "unavailable" /\ m \notin InFlight
  /\ admin < MaxAdmin /\ admin' = admin + 1
  /\ parked' = parked \cup {m} /\ byHand' = byHand \cup {m}
  /\ kind' = [kind EXCEPT ![m] = "none"]
  /\ holds' = [holds EXCEPT ![m] = 0]
  /\ due' = [due EXCEPT ![m] = TRUE]
  /\ UNCHANGED <<processed, procOrder, attempts, expired, waiting, cause, flaked, rearmed, env,
                breaker, memo, flight, trial, rollbacks>>

\* park_queue, a row at a time as its batches interleave with the dispatch:
\* a Tenant's newest queue row given up under the operator's cause, so that
\* what is given up is always the end of each key.  A row an ask holds is
\* passed by.
ParkQueue(t) ==
  LET q == {m \in Arrives : Queued(m) /\ TenantOf[m] = t /\ m \notin InFlight} IN
  /\ q # {}
  /\ admin < MaxAdmin /\ admin' = admin + 1
  /\ \E m \in q :
       /\ IF Mutant = "queue_oldest"
            THEN \A n \in q : Pos(m) <= Pos(n)
            ELSE \A n \in q : Pos(m) >= Pos(n)
       /\ parked' = parked \cup {m} /\ byHand' = byHand \cup {m}
       /\ cause' = [cause EXCEPT ![m] = "given"]
       /\ kind' = [kind EXCEPT ![m] = "none"]
       /\ holds' = [holds EXCEPT ![m] = 0]
       /\ due' = [due EXCEPT ![m] = TRUE]
  /\ UNCHANGED <<processed, procOrder, attempts, expired, waiting, flaked, rearmed, env, breaker,
                memo, flight, trial, rollbacks>>

(* ------------------------------------------------------------------------ *)

Next ==
  \/ \E w \in Workers : AskStart(w) \/ AskEnd(w) \/ Rollback(w) \/ SetAside(w) \/ HoldUnasked(w)
  \/ \E p \in Procs : Circle(p) \/ CutOff(p) \/ CutMends(p)
  \/ \E p \in Procs, t \in Tenants : MemoElapse(p, t)
  \/ \E m \in Msgs : Elapse(m) \/ Expire(m) \/ UnparkAll(m) \/ ParkHeads(m)
  \/ \E t \in Tenants : ParkQueue(t) \/ TenantFails(t) \/ TenantMends(t)
  \/ StageFails \/ StageMends

\* Every worker keeps asking and ends what it asks -- strongly: a slot
\* another process's asks keep taking and letting go is taken by this one
\* in turn, as the take serves the least recently served slot `SKIP
\* LOCKED`; every while passes -- strongly too: a hold a trial asks again
\* runs out between two trials, its stage-scoped interval at most a tick's
\* cap and the trials a tick apart (ADR-0042 decision 3); the expiry runs; a
\* stage, a cut and a Tenant's fault that end, end.
Fairness ==
  /\ \A w \in Workers : SF_vars(AskStart(w) \/ SetAside(w) \/ HoldUnasked(w)) /\ WF_vars(AskEnd(w))
  /\ \A p \in Procs : WF_vars(Circle(p)) /\ WF_vars(CutMends(p))
  /\ \A p \in Procs, t \in Tenants : WF_vars(MemoElapse(p, t))
  /\ \A m \in Msgs : SF_vars(Elapse(m)) /\ WF_vars(Expire(m))
  /\ WF_vars(StageMends)
  /\ \A t \in Tenants : WF_vars(TenantMends(t))

Spec == Init /\ [][Next]_vars /\ Fairness

(* ------------------------------------------------------------------------ *)
(* Properties                                                                *)

TypeOK ==
  /\ processed \subseteq Arrives /\ parked \subseteq Arrives /\ waiting \subseteq Arrives
  /\ byHand \subseteq parked /\ expired \subseteq parked
  /\ attempts \in [Msgs -> Nat]
  /\ cause \in [Msgs -> Causes \cup {None}]
  /\ kind \in [Msgs -> Kinds] /\ holds \in [Msgs -> 0..HoldCap]
  /\ due \in [Msgs -> BOOLEAN]
  /\ breaker \in [Procs -> {"closed", "open"}]
  /\ memo \in [Procs -> [Tenants -> Memos]]
  /\ flight \in [Workers -> Arrives \cup {None}] /\ trial \in [Workers -> BOOLEAN]

\* An Unavailable spends no attempt, at either scope (ADR-0042 decision 3):
\* a verdict that leaves its row held for one spends nothing.
UnavailableNeverSpends ==
  [][\A w \in Workers :
       (flight[w] # None /\ flight'[w] = None /\ cause'[flight[w]] \in {"stage", "tenant"})
         => attempts' = attempts]_vars

\* No row parks for an Unavailable but by the operator's hand.
UnavailableNeverParks == \A m \in parked : cause[m] \in {"stage", "tenant"} => m \in byHand

\* A held row is in the queue.
HoldIsQueued ==
  \A m \in Msgs : kind[m] # "none" => m \notin processed /\ m \notin parked /\ m \notin waiting

\* A cause explains a state that lasts: a row not processed that is parked,
\* held or in its back-off, has spent attempts, or was re-armed.
CauseIsCurrent ==
  \A m \in Msgs : cause[m] # None =>
    /\ m \notin processed
    /\ (m \in parked \/ kind[m] # "none" \/ attempts[m] > 0 \/ m \in rearmed)

\* Once a process's memo holds a Tenant, no ask of the Tenant starts while
\* one is in flight or the memo runs (ADR-0042 decision 2); a trial is the
\* breaker's, not the memo's.
OneAskPerReach ==
  [][\A w \in Workers :
       (flight[w] = None /\ flight'[w] # None /\ ~trial'[w])
         => memo[ProcOf[w]][TenantOf[flight'[w]]] \in {"none", "due"}]_vars

\* No waiter expires while its chain reaches a queued row.
PresentDependencyNeverExpires ==
  [][\A m \in Msgs : (m \notin expired /\ m \in expired') => ~Spared(m, Cardinality(Msgs))]_vars

\* A re-armed row has exactly Tries failures left until it is told a verdict.
CanaryExact == \A m \in rearmed : attempts[m] = MaxAttempts - Tries

\* What a Tenant's queue gave up is the end of each key: no row of a key is
\* processed past one given up.
GivenUpIsTheEnd ==
  \A a \in processed, b \in byHand :
    (KeyOf[a] = KeyOf[b] /\ cause[b] = "given") => Pos(a) < Pos(b)

Index(m) == CHOOSE i \in 1..Len(procOrder) : procOrder[i] = m

\* The rows of a key are processed in their order of arrival.
KeyOrder ==
  \A a, b \in processed :
    (KeyOf[a] = KeyOf[b] /\ Pos(a) < Pos(b)) => Index(a) < Index(b)

\* Every row whose dependency arrives ends processed or parked.
EventuallyOutOfTheWay ==
  \A m \in Arrives : (Dep[m] = None \/ Dep[m] \in Arrives) => <>(m \in processed \/ m \in parked)

\* Every row no retry fails for good and no operator gives up, whose
\* dependency arrives, is processed once what holds it ends.
EventuallyProcessed ==
  \A m \in Arrives \ Permanent : (Dep[m] = None \/ Dep[m] \in Arrives) => <>(m \in processed)

=============================================================================
