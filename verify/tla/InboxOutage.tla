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
(*     which lets one ask through, in flight, at a time once it runs out;  *)
(*     at the Tenant's scope it holds the Tenant's other heads unasked     *)
(*     (HoldUnasked), at the stage's -- one process's evidence -- the      *)
(*     process leaves them to the others, writing nothing; a trial's Stage *)
(*     verdict is the breaker's, not the memo's;                           *)
(*   - a Stage `Unavailable` may open the process's breaker -- the         *)
(*     thresholds are the core's, and the model takes either branch; an   *)
(*     open breaker asks no head but by its trial, one at a time, which a  *)
(*     processed verdict closes; a circle that finds no head closes it.    *)
(*     That is the rows' source, a channel's with no dependency registered;*)
(*     with one (Depends), the stage's refusal is its dependency's, and    *)
(*     its own breaker -- the gate -- is suspected at a refusal, opened by *)
(*     a check of the dependency refused, and closed by a check served --  *)
(*     past the process's memory: a key held cached is no evidence -- or  *)
(*     with nothing left to ask and no head to hold (ADR-0042 decision 1).*)
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
  Depends,      \* TRUE: the stage's refusals are its dependency's, registered for the channel
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
\* A memo entry of the Tenant's scope ("held", "due", "flight"), or of the
\* stage's ("_s"): the latter one process's evidence, kept to the process.
Memos == {"none", "held", "due", "flight", "held_s", "due_s", "flight_s"}
Held(v) == v \in {"held", "held_s"}
Due(v) == v \in {"due", "due_s"}
InFlightOf(v) == v \in {"flight", "flight_s"}
Stage(v) == v \in {"held_s", "due_s", "flight_s"}

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
  breaker,     \* [Procs -> {"closed", "open"}]: the rows' source
  gate,        \* [Procs -> {"closed", "suspect", "open"}]: the dependency's source
  remembers,   \* [Procs -> BOOLEAN]: the dependency holds a key to ask, its control or its last refused
  memo,        \* [Procs -> [Tenants -> Memos]]
  flight,      \* [Workers -> Arrives \cup {None}]: the head a worker's open ask holds
  trial,       \* [Workers -> BOOLEAN]: whether that ask is its process's trial
  admin, rollbacks

rows == <<processed, procOrder, attempts, parked, byHand, expired, waiting, cause, kind, holds,
          due, flaked, rearmed>>
env == <<stageDown, outages, cut, cutDone, tenantDown, faulted>>
deps == <<gate, remembers>>
vars == <<rows, env, breaker, deps, memo, flight, trial, admin, rollbacks>>

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
  /\ gate = [p \in Procs |-> "closed"] /\ remembers = [p \in Procs |-> FALSE]
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

\* A channel is paused in a process while either source's breaker is open; a
\* suspected gate does not pause it.
Paused(p) == breaker[p] = "open" \/ gate[p] = "open"

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
       /\ \/ /\ ~Paused(p) /\ due[m]
             /\ IF Mutant = "no_flight_mark"
                  THEN ~Held(memo[p][TenantOf[m]])
                  ELSE memo[p][TenantOf[m]] = "none" \/ Due(memo[p][TenantOf[m]])
             /\ memo' = IF Due(memo[p][TenantOf[m]]) \/ InFlightOf(memo[p][TenantOf[m]])
                          THEN [memo EXCEPT ![p][TenantOf[m]] =
                                  IF Stage(@) THEN "flight_s" ELSE "flight"]
                          ELSE memo
             /\ trial' = trial
          \/ /\ breaker[p] = "open"
             /\ \A v \in Workers : ProcOf[v] = p => ~trial[v]
             /\ trial' = [trial EXCEPT ![w] = TRUE]
             /\ memo' = memo
       /\ flight' = [flight EXCEPT ![w] = m]
  /\ UNCHANGED <<rows, env, breaker, deps, admin, rollbacks>>

\* A head whose dependency is not processed is set aside, the whole
\* transaction (ADR-0009).
SetAside(w) ==
  /\ flight[w] = None /\ ~Paused(ProcOf[w])
  /\ \E m \in Heads :
       /\ ~SlotBusy(SlotOf[m]) /\ due[m] /\ ~DepDone(m)
       /\ waiting' = waiting \cup {m}
  /\ UNCHANGED <<processed, procOrder, attempts, parked, byHand, expired, cause, kind, holds, due,
                flaked, rearmed, env, breaker, deps, memo, flight, trial, admin, rollbacks>>

\* A head of a Tenant the process's memo holds at the Tenant's scope is held
\* unasked: its row written held under the memo's cause, its count of verdicts
\* untouched.  At the stage's scope the process's walk does not take the head
\* (ADR-0042 decisions 2 and 5): nothing is written, and the other processes
\* take it -- but for the refuted design that writes it ("stage_held_unasked").
HoldUnasked(w) ==
  LET p == ProcOf[w] IN
  /\ flight[w] = None /\ ~Paused(p)
  /\ \E m \in Heads :
       /\ ~SlotBusy(SlotOf[m]) /\ due[m]
       /\ IF Mutant = "stage_held_unasked"
            THEN Held(memo[p][TenantOf[m]]) \/ InFlightOf(memo[p][TenantOf[m]])
            ELSE memo[p][TenantOf[m]] \in {"held", "flight"}
       /\ kind' = [kind EXCEPT ![m] = "unavailable"]
       /\ cause' = [cause EXCEPT ![m] = IF Stage(memo[p][TenantOf[m]]) THEN "stage" ELSE "tenant"]
       /\ due' = [due EXCEPT ![m] = FALSE]
  /\ UNCHANGED <<processed, procOrder, attempts, parked, byHand, expired, waiting, holds, flaked,
                rearmed, env, breaker, deps, memo, flight, trial, admin, rollbacks>>

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
        /\ UNCHANGED <<attempts, byHand, flaked, deps>>
     \/ \* its own failure, once
        /\ Reachable(p, m) /\ m \in Flaky \ flaked /\ m \notin Permanent
        /\ flaked' = flaked \cup {m}
        /\ Spend(m, "own")
        /\ memo' = [memo EXCEPT ![p][t] = "none"]
        /\ UNCHANGED <<processed, procOrder, waiting, byHand, expired, breaker, deps>>
     \/ \* no retry mends it
        /\ Reachable(p, m) /\ m \in Permanent
        /\ attempts' = [attempts EXCEPT ![m] = @ + 1]
        /\ parked' = parked \cup {m}
        /\ cause' = [cause EXCEPT ![m] = "permanent"]
        /\ kind' = [kind EXCEPT ![m] = "none"]
        /\ holds' = [holds EXCEPT ![m] = 0]
        /\ rearmed' = rearmed \ {m}
        /\ memo' = [memo EXCEPT ![p][t] = "none"]
        /\ UNCHANGED <<processed, procOrder, waiting, byHand, expired, due, flaked, breaker, deps>>
     \/ \* the Tenant's Unavailable, remembered by the process
        /\ Reachable(p, m) /\ t \in tenantDown /\ m \notin Permanent
        /\ Hold(m, "tenant")
        /\ memo' = [memo EXCEPT ![p][t] = "held"]
        /\ UNCHANGED <<processed, procOrder, waiting, byHand, expired, flaked, breaker, deps>>
     \/ \* the stage's Unavailable: remembered by the process as a Tenant's is,
        \* but for a trial's, which is the breaker's; the breaker may open, or
        \* stay as it is.  With a dependency, the refusal is its: the key
        \* refused is remembered to ask again, and the gate may be suspected
        \* -- the thresholds' branch -- and nothing else.
        /\ ~Reachable(p, m)
        /\ Hold(m, "stage")
        /\ IF Depends
             THEN /\ gate' \in {gate, [gate EXCEPT ![p] = IF @ = "closed" THEN "suspect" ELSE @]}
                  /\ remembers' = [remembers EXCEPT ![p] = TRUE]
                  /\ breaker' = breaker
             ELSE /\ breaker' \in {breaker, [breaker EXCEPT ![p] = "open"]}
                  /\ UNCHANGED deps
        /\ memo' = IF trial[w] THEN memo ELSE [memo EXCEPT ![p][t] = "held_s"]
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
  /\ memo' = IF InFlightOf(memo[p][TenantOf[flight[w]]])
               THEN [memo EXCEPT ![p][TenantOf[flight[w]]] = IF Stage(@) THEN "due_s" ELSE "due"]
               ELSE memo
  /\ UNCHANGED <<rows, env, breaker, deps, admin>>

\* A guest's circle that finds no head of the channel closes the breaker.
Circle(p) ==
  /\ breaker[p] = "open" /\ Heads = {}
  /\ \A w \in Workers : ProcOf[w] = p => ~trial[w]
  /\ breaker' = [breaker EXCEPT ![p] = "closed"]
  /\ UNCHANGED <<rows, env, deps, memo, flight, trial, admin, rollbacks>>

\* The dependency's check, beside the dispatch (ADR-0042 decision 1): it
\* asks the dependency past the stage's memory -- its control or its last
\* refused key, and the head met, if any -- and is served when the stage is
\* up for the process, whatever the process holds cached; refused, it opens
\* the gate or keeps it open.  With nothing to ask it is unasked and changes
\* nothing.  The refuted designs: a check the stage answers from its memory,
\* a head of a Tenant whose keys the process holds cached served while the
\* stage is down ("cached_check_closes"); and one that asks a head alone
\* ("check_needs_a_head").
Served(p, h) ==
  IF Mutant = "cached_check_closes" /\ h # None
    THEN Reachable(p, h)
    ELSE ~stageDown /\ p \notin cut

Check(p) ==
  /\ gate[p] \in {"suspect", "open"}
  /\ \E h \in Heads \cup {None} :
       /\ h = None => remembers[p] /\ Mutant # "check_needs_a_head"
       /\ gate' = [gate EXCEPT ![p] = IF Served(p, h) THEN "closed" ELSE "open"]
  /\ UNCHANGED <<rows, env, breaker, remembers, memo, flight, trial, admin, rollbacks>>

\* An open gate with nothing to ask and no head to hold closes: the core
\* closes it after W with no head met at the tick's cap.
NothingToHold(p) ==
  /\ gate[p] = "open" /\ ~remembers[p] /\ Heads = {}
  /\ gate' = [gate EXCEPT ![p] = "closed"]
  /\ UNCHANGED <<rows, env, breaker, remembers, memo, flight, trial, admin, rollbacks>>

(* ------------------------------------------------------------------------ *)
(* Time                                                                      *)

Elapse(m) ==
  /\ ~due[m] /\ m \notin InFlight
  /\ due' = [due EXCEPT ![m] = TRUE]
  /\ UNCHANGED <<processed, procOrder, attempts, parked, byHand, expired, waiting, cause, kind,
                holds, flaked, rearmed, env, breaker, deps, memo, flight, trial, admin, rollbacks>>

\* A Tenant's memo runs out: one ask may go through.
MemoElapse(p, t) ==
  /\ Held(memo[p][t])
  /\ memo' = [memo EXCEPT ![p][t] = IF Stage(@) THEN "due_s" ELSE "due"]
  /\ UNCHANGED <<rows, env, breaker, deps, flight, trial, admin, rollbacks>>

\* The dependency's keys run out: its control past its keep, its refused key
\* dropped.
Forget(p) ==
  /\ remembers[p]
  /\ remembers' = [remembers EXCEPT ![p] = FALSE]
  /\ UNCHANGED <<rows, env, breaker, gate, memo, flight, trial, admin, rollbacks>>

\* A waiter whose chain reaches no queued row is parked, the identity it
\* waited for kept, so that a late mark still wakes it.
Expire(m) ==
  /\ m \in waiting
  /\ IF Mutant = "expire_present" THEN TRUE ELSE ~Spared(m, Cardinality(Msgs))
  /\ waiting' = waiting \ {m}
  /\ parked' = parked \cup {m}
  /\ expired' = expired \cup {m}
  /\ UNCHANGED <<processed, procOrder, attempts, byHand, cause, kind, holds, due, flaked, rearmed,
                env, breaker, deps, memo, flight, trial, admin, rollbacks>>

(* ------------------------------------------------------------------------ *)
(* The environment                                                           *)

StageFails ==
  /\ ~stageDown /\ outages < Outages
  /\ stageDown' = TRUE /\ outages' = outages + 1
  /\ UNCHANGED <<rows, cut, cutDone, tenantDown, faulted, breaker, deps, memo, flight, trial, admin,
                rollbacks>>

StageMends ==
  /\ stageDown /\ OutageEnds /\ stageDown' = FALSE
  /\ UNCHANGED <<rows, outages, cut, cutDone, tenantDown, faulted, breaker, deps, memo, flight, trial,
                admin, rollbacks>>

CutOff(p) ==
  /\ p \in CutOffs \ cutDone
  /\ cut' = cut \cup {p} /\ cutDone' = cutDone \cup {p}
  /\ UNCHANGED <<rows, stageDown, outages, tenantDown, faulted, breaker, deps, memo, flight, trial,
                admin, rollbacks>>

CutMends(p) ==
  /\ p \in cut /\ CutEnds /\ cut' = cut \ {p}
  /\ UNCHANGED <<rows, stageDown, outages, cutDone, tenantDown, faulted, breaker, deps, memo, flight,
                trial, admin, rollbacks>>

TenantFails(t) ==
  /\ t \in TenantFaults \ faulted
  /\ tenantDown' = tenantDown \cup {t} /\ faulted' = faulted \cup {t}
  /\ UNCHANGED <<rows, stageDown, outages, cut, cutDone, breaker, deps, memo, flight, trial, admin,
                rollbacks>>

TenantMends(t) ==
  /\ t \in tenantDown /\ TenantEnds /\ tenantDown' = tenantDown \ {t}
  /\ UNCHANGED <<rows, stageDown, outages, cut, cutDone, faulted, breaker, deps, memo, flight, trial,
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
  /\ UNCHANGED <<processed, procOrder, expired, waiting, kind, holds, flaked, env, breaker, deps,
                memo, flight, trial, rollbacks>>

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
                breaker, deps, memo, flight, trial, rollbacks>>

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
                deps, memo, flight, trial, rollbacks>>

(* ------------------------------------------------------------------------ *)

Next ==
  \/ \E w \in Workers : AskStart(w) \/ AskEnd(w) \/ Rollback(w) \/ SetAside(w) \/ HoldUnasked(w)
  \/ \E p \in Procs : Circle(p) \/ Check(p) \/ NothingToHold(p) \/ Forget(p) \/ CutOff(p) \/ CutMends(p)
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
\* gate's check runs, a tick apart, and so does its close with nothing to
\* hold -- not the dependency's forgetting, which a keep that outlasts the
\* outage never reaches; a stage, a cut and a Tenant's fault that end, end.
Fairness ==
  /\ \A w \in Workers : SF_vars(AskStart(w) \/ SetAside(w) \/ HoldUnasked(w)) /\ WF_vars(AskEnd(w))
  /\ \A p \in Procs : WF_vars(Circle(p)) /\ WF_vars(CutMends(p))
  /\ \A p \in Procs : WF_vars(Check(p)) /\ WF_vars(NothingToHold(p))
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
  /\ gate \in [Procs -> {"closed", "suspect", "open"}] /\ remembers \in [Procs -> BOOLEAN]
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
         => memo[ProcOf[w]][TenantOf[flight'[w]]] = "none"
              \/ Due(memo[ProcOf[w]][TenantOf[flight'[w]]])]_vars

\* A row held without a verdict told on it carries a Tenant's cause: a stage's
\* refusal, one process's evidence, is written on the row refused alone.
UnaskedHoldIsTheTenants ==
  \A m \in Msgs : (kind[m] = "unavailable" /\ holds[m] = 0) => cause[m] = "tenant"

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

\* The gate closes from open on its own evidence alone: a check served while
\* the stage is up for its process, or nothing left to ask and no head to
\* hold -- never a key the process holds cached (ADR-0042 decision 1).
GateClosesOnItsEvidence ==
  [][\A p \in Procs :
       (gate[p] = "open" /\ gate'[p] = "closed")
         => (~stageDown /\ p \notin cut) \/ (~remembers[p] /\ Heads = {})]_vars

\* An open gate closes once the stage is up for its process, a head left to
\* ask or not.
GateResumes == \A p \in Procs : (gate[p] = "open") ~> (gate[p] = "closed" \/ stageDown \/ p \in cut)

\* Every row whose dependency arrives ends processed or parked.
EventuallyOutOfTheWay ==
  \A m \in Arrives : (Dep[m] = None \/ Dep[m] \in Arrives) => <>(m \in processed \/ m \in parked)

\* Every row no retry fails for good and no operator gives up, whose
\* dependency arrives, is processed once what holds it ends.
EventuallyProcessed ==
  \A m \in Arrives \ Permanent : (Dep[m] = None \/ Dep[m] \in Arrives) => <>(m \in processed)

=============================================================================
