----------------------------- MODULE InboxPg -----------------------------
(***************************************************************************)
(* The inbox one level below Inbox.tla: the statements of `crates/inbox`,  *)
(* run against PostgreSQL under READ COMMITTED.  Where Inbox.tla has one   *)
(* atomic step per protocol action, this module has the statements that   *)
(* make it up, each under a snapshot of its own, and the locks between     *)
(* them: the slot's row FOR UPDATE SKIP LOCKED, and the advisory lock on a *)
(* dependency's identity that a wait and a mark both take (ADR-0009).      *)
(*                                                                         *)
(* PostgreSQL is modelled as far as the statements rely on it.  A commit   *)
(* advances a clock and stamps the versions it publishes; a statement sees *)
(* the versions stamped no later than its snapshot, and its own            *)
(* transaction's pending writes.  Taking a slot is two steps, the snapshot *)
(* and then the lock, because that is where the snapshot of a statement    *)
(* and the lock it takes come apart.  A row lock is taken only on a slot's *)
(* head, and only by the slot's holder, so no other row lock is modelled.  *)
(*                                                                         *)
(* Three switches name the designs the module was written to tell apart:  *)
(*   WithLock    FALSE: no advisory lock -- the wait of ADR-0006 as it was  *)
(*               implemented, and the lost wake ADR-0009 fixed;            *)
(*   HeadInTake  TRUE: the head read in the take's statement, under a      *)
(*               snapshot older than the lock -- the take of ADR-0008 as   *)
(*               it was implemented;                                       *)
(*   WalkOn      TRUE: a dispatcher that set a head aside goes on to the   *)
(*               next head in the same transaction, holding the lock --    *)
(*               the first design of ADR-0009, refused for its lock cycle. *)
(* With all three as implemented, the module refines Inbox.tla: the        *)
(* mapping at the end, checked by TLC as a property.                       *)
(*                                                                         *)
(* Left out, as protocol-level matters Inbox.tla settles: failures,        *)
(* backoff, parking, expiring waits, operators.  The dependency check is   *)
(* taken with the head's snapshot; in the implementation it is a later     *)
(* statement, which can only see more marks, never fewer.                  *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
  Msgs,           \* messages, by identity
  Deps,           \* [Msgs -> SUBSET Msgs], causal dependencies; acyclic, at most one each
  Arrives,        \* SUBSET Msgs: the messages that ever arrive
  Slots,          \* the slots the table is cut into
  Placements,     \* SUBSET [Msgs -> Slots]: the cuts to check
  Dispatchers,    \* the dispatchers: loops, in any number of processes
  MaxCrashes,     \* rollbacks of a processing transaction, in all
  WithLock,       \* the advisory lock on the dependency's identity, both sides
  HeadInTake,     \* the head read in the take's own statement
  WalkOn,         \* after a set-aside, the next head in the same transaction
  None            \* a model value

ASSUME \A m \in Msgs : Cardinality(Deps[m]) <= 1

VARIABLES
  \* the table, as committed versions stamped by the clock
  clock,      \* commits so far; a commit publishes versions stamped clock + 1
  recvAt,     \* [Msgs -> Nat], the stamp of the row's insert; 0: not received
  pos,        \* [Msgs -> Nat], received_position
  nextPos,
  procAt,     \* [Msgs -> Nat], the stamp of the mark; 0: unprocessed
  procOrder,  \* Seq(Msgs), marks in commit order
  waitAt,     \* [Msgs -> Nat], the stamp of the row's wait; 0: never set aside
  waitDep,    \* [Msgs -> Msgs \cup {None}], what it waits for
  wokeAt,     \* [Msgs -> Nat], the stamp of the wake; 0: not woken
  \* the dispatchers' transactions
  pc,         \* [Dispatchers -> STRING], where each one is
  slot,       \* [Dispatchers -> Slots \cup {None}], the slot held
  snap,       \* [Dispatchers -> Nat], the take's snapshot
  msg,        \* [Dispatchers -> Msgs \cup {None}], the head read
  dep,        \* [Dispatchers -> Msgs \cup {None}], the dependency it waits on
  pendWait,   \* [Dispatchers -> SUBSET Msgs], rows set aside, not committed
  pendWoken,  \* [Dispatchers -> SUBSET Msgs], rows the mark wakes, not committed
  slotLock,   \* [Slots -> Dispatchers \cup {None}], FOR UPDATE on the slot's row
  adv,        \* [Msgs -> Dispatchers \cup {None}], the advisory lock by identity
  dead,       \* SUBSET Dispatchers: stopped by a database error
  crashes

vars == <<clock, recvAt, pos, nextPos, procAt, procOrder, waitAt, waitDep, wokeAt,
          pc, slot, snap, msg, dep, pendWait, pendWoken, slotLock, adv, dead, crashes>>
table == <<clock, recvAt, pos, nextPos, procAt, procOrder, waitAt, waitDep, wokeAt>>

VARIABLE part  \* [Msgs -> Slots], the cut, fixed at the start

(* ------------------------------------------------------------------------ *)
(* Visibility                                                                 *)

\* A version stamped `at` is committed as of snapshot `t`.
Seen(t, at) == at > 0 /\ at <= t

ReceivedAt(t) == {m \in Msgs : Seen(t, recvAt[m])}
ProcessedAt(t) == {m \in Msgs : Seen(t, procAt[m])}
WaitingAt(t, m) == IF Seen(t, waitAt[m]) /\ ~Seen(t, wokeAt[m]) THEN waitDep[m] ELSE None

\* The queue of slot s as dispatcher d's statement under snapshot t sees it:
\* committed rows neither processed nor waiting, and not what d's own
\* transaction has set aside or marked -- a transaction sees its own writes.
QueueAt(t, s, d) ==
  {m \in ReceivedAt(t) : /\ part[m] = s
                         /\ m \notin ProcessedAt(t) /\ WaitingAt(t, m) = None
                         /\ m \notin pendWait[d] /\ ~(pc[d] \in {"marked"} /\ msg[d] = m)}
Oldest(S) == CHOOSE m \in S : \A n \in S : pos[m] <= pos[n]
HeadAt(t, s, d) == IF QueueAt(t, s, d) = {} THEN None ELSE Oldest(QueueAt(t, s, d))

Now == clock

(* ------------------------------------------------------------------------ *)

Init ==
  /\ part \in Placements
  /\ clock = 0
  /\ recvAt = [m \in Msgs |-> 0] /\ pos = [m \in Msgs |-> 0] /\ nextPos = 1
  /\ procAt = [m \in Msgs |-> 0] /\ procOrder = <<>>
  /\ waitAt = [m \in Msgs |-> 0] /\ waitDep = [m \in Msgs |-> None] /\ wokeAt = [m \in Msgs |-> 0]
  /\ pc = [d \in Dispatchers |-> "idle"]
  /\ slot = [d \in Dispatchers |-> None] /\ snap = [d \in Dispatchers |-> 0]
  /\ msg = [d \in Dispatchers |-> None] /\ dep = [d \in Dispatchers |-> None]
  /\ pendWait = [d \in Dispatchers |-> {}] /\ pendWoken = [d \in Dispatchers |-> {}]
  /\ slotLock = [s \in Slots |-> None] /\ adv = [m \in Msgs |-> None]
  /\ dead = {} /\ crashes = 0

Dispatching == <<pc, slot, snap, msg, dep, pendWait, pendWoken, slotLock, adv, dead, crashes>>

\* INSERT ... ON CONFLICT DO NOTHING in a transaction of its own, committed at once.
Receive(m) ==
  /\ m \in Arrives /\ recvAt[m] = 0
  /\ clock' = clock + 1
  /\ recvAt' = [recvAt EXCEPT ![m] = clock']
  /\ pos' = [pos EXCEPT ![m] = nextPos] /\ nextPos' = nextPos + 1
  /\ UNCHANGED <<procAt, procOrder, waitAt, waitDep, wokeAt, part>>
  /\ UNCHANGED Dispatching

(* ------------------------------------------------------------------------ *)
(* One dispatcher's transaction, statement by statement                       *)

Alive(d) == d \notin dead

\* The transaction ends: every lock goes, nothing pending stays.
Release(d, next) ==
  /\ pc' = [pc EXCEPT ![d] = next]
  /\ slot' = [slot EXCEPT ![d] = None] /\ msg' = [msg EXCEPT ![d] = None] /\ dep' = [dep EXCEPT ![d] = None]
  /\ pendWait' = [pendWait EXCEPT ![d] = {}] /\ pendWoken' = [pendWoken EXCEPT ![d] = {}]
  /\ slotLock' = [s \in Slots |-> IF slotLock[s] = d THEN None ELSE slotLock[s]]
  /\ adv' = [m \in Msgs |-> IF adv[m] = d THEN None ELSE adv[m]]

\* The take's statement begins: its snapshot is taken before any lock.
TakeSnap(d) ==
  /\ Alive(d) /\ pc[d] = "idle"
  /\ snap' = [snap EXCEPT ![d] = Now] /\ pc' = [pc EXCEPT ![d] = "snapped"]
  /\ UNCHANGED <<slot, msg, dep, pendWait, pendWoken, slotLock, adv, dead, crashes, part>>
  /\ UNCHANGED table

\* A slot whose head, as the snapshot sees it, is due; the row is locked now,
\* FOR UPDATE SKIP LOCKED, and a slot another holds is passed by.
DueAt(t, d) == {s \in Slots : HeadAt(t, s, d) # None}

\* With the head read in the same statement: the head as the snapshot sees it
\* is re-checked, FOR UPDATE, against the latest committed version, and the
\* first row of the snapshot's queue that still qualifies is the head -- or
\* none is, and the statement returned a slot without a head, which the
\* implementation took for an error that stops the loops.
StaleHead(d, s) ==
  LET still == {m \in QueueAt(snap[d], s, d) : m \in QueueAt(Now, s, d)}
  IN IF still = {} THEN None ELSE Oldest(still)

Free(d) == DueAt(snap[d], d) \cap {s \in Slots : slotLock[s] = None}

Take(d) ==
  /\ Alive(d) /\ pc[d] = "snapped"
  /\ UNCHANGED <<snap, dep, pendWait, pendWoken, adv, crashes, part>> /\ UNCHANGED table
  /\ \/ /\ Free(d) = {}
        /\ pc' = [pc EXCEPT ![d] = "idle"]
        /\ UNCHANGED <<slot, msg, slotLock, dead>>
     \/ \E s \in Free(d) :
        /\ slotLock' = [slotLock EXCEPT ![s] = d] /\ slot' = [slot EXCEPT ![d] = s]
        /\ \/ /\ ~HeadInTake
              /\ pc' = [pc EXCEPT ![d] = "taken"] /\ UNCHANGED <<msg, dead>>
           \/ /\ HeadInTake /\ StaleHead(d, s) = None
              /\ dead' = dead \cup {d} /\ pc' = [pc EXCEPT ![d] = "dead"] /\ UNCHANGED msg
           \/ /\ HeadInTake /\ StaleHead(d, s) # None
              /\ msg' = [msg EXCEPT ![d] = StaleHead(d, s)] /\ pc' = [pc EXCEPT ![d] = "headed"]
              /\ UNCHANGED dead

\* The dependency check: the first one without a committed mark decides.
Decided(d, h) ==
  LET missing == Deps[h] \ ProcessedAt(Now) IN
  IF missing = {}
  THEN pc' = [pc EXCEPT ![d] = "ready"] /\ UNCHANGED dep
  ELSE /\ dep' = [dep EXCEPT ![d] = CHOOSE z \in missing : TRUE]
       /\ pc' = [pc EXCEPT ![d] = "aside"]

\* The head of the slot held, read under a fresh snapshot after the lock, and
\* its dependencies checked in the same step.  The implementation checks
\* them one statement later; a mark committed in between is then seen, and
\* the head is processed where this step would set it aside -- and if that
\* mark woke an older row of the slot, processed ahead of it.  Inbox.tla has
\* no such step, though its invariants allow the outcome: the woken row had
\* a dependency, and ArrivalOrder speaks of rows without one.
ReadHead(d) ==
  /\ Alive(d) /\ pc[d] = "taken"
  /\ LET h == HeadAt(Now, slot[d], d) IN
     IF h = None
     THEN Release(d, "idle") /\ UNCHANGED <<dead, crashes>>
     ELSE /\ msg' = [msg EXCEPT ![d] = h] /\ Decided(d, h)
          /\ UNCHANGED <<slot, pendWait, pendWoken, slotLock, adv, dead, crashes>>
  /\ UNCHANGED <<snap, part>> /\ UNCHANGED table

\* The same check for a head the take's statement read (HeadInTake).
Decide(d) ==
  /\ Alive(d) /\ pc[d] = "headed"
  /\ Decided(d, msg[d])
  /\ UNCHANGED <<slot, snap, msg, pendWait, pendWoken, slotLock, adv, dead, crashes, part>>
  /\ UNCHANGED table

\* pg_advisory_xact_lock on the dependency's identity; held to the end of the
\* transaction, re-entrant for its holder, waited for otherwise.
LockDep(d) ==
  /\ Alive(d) /\ pc[d] = "aside"
  /\ WithLock => adv[dep[d]] \in {None, d}
  /\ adv' = IF WithLock THEN [adv EXCEPT ![dep[d]] = d] ELSE adv
  /\ pc' = [pc EXCEPT ![d] = "locked"]
  /\ UNCHANGED <<slot, snap, msg, dep, pendWait, pendWoken, slotLock, dead, crashes, part>>
  /\ UNCHANGED table

\* UPDATE ... SET waiting_for WHERE ... AND NOT EXISTS (the dependency
\* processed): under a fresh snapshot.  Nothing to wait for any more: the
\* transaction ends with nothing done.
SetWait(d) ==
  /\ Alive(d) /\ pc[d] = "locked"
  /\ IF dep[d] \in ProcessedAt(Now)
     THEN Release(d, "idle") /\ UNCHANGED <<dead, crashes>>
     ELSE /\ pendWait' = [pendWait EXCEPT ![d] = @ \cup {msg[d]}]
          /\ pc' = [pc EXCEPT ![d] = IF WalkOn THEN "taken" ELSE "waitset"]
          /\ UNCHANGED <<slot, msg, dep, pendWoken, slotLock, adv, dead, crashes>>
  /\ UNCHANGED <<snap, part>> /\ UNCHANGED table

\* COMMIT of a transaction that set rows aside and nothing else.
CommitWait(d) ==
  /\ Alive(d) /\ pc[d] = "waitset"
  /\ clock' = clock + 1
  /\ waitAt' = [m \in Msgs |-> IF m \in pendWait[d] THEN clock' ELSE waitAt[m]]
  /\ waitDep' = [m \in Msgs |-> IF m \in pendWait[d] THEN dep[d] ELSE waitDep[m]]
  /\ Release(d, "idle")
  /\ UNCHANGED <<recvAt, pos, nextPos, procAt, procOrder, wokeAt, snap, dead, crashes, part>>

\* With WalkOn the rows set aside have different dependencies: the wait of
\* each is the dependency it was decided on.  This module keeps one `dep`
\* per dispatcher, so WalkOn commits are modelled for the last decision only
\* -- enough for the lock cycle it exists to show, and no more.

\* The subscriber ran and returned; its writes are in the transaction.
Handle(d) ==
  /\ Alive(d) /\ pc[d] = "ready"
  /\ pc' = [pc EXCEPT ![d] = "handled"]
  /\ UNCHANGED <<slot, snap, msg, dep, pendWait, pendWoken, slotLock, adv, dead, crashes, part>>
  /\ UNCHANGED table

\* The mark's lock on the message's own identity, before the mark.
LockOwn(d) ==
  /\ Alive(d) /\ pc[d] = "handled"
  /\ WithLock => adv[msg[d]] \in {None, d}
  /\ adv' = IF WithLock THEN [adv EXCEPT ![msg[d]] = d] ELSE adv
  /\ pc' = [pc EXCEPT ![d] = "ownlocked"]
  /\ UNCHANGED <<slot, snap, msg, dep, pendWait, pendWoken, slotLock, dead, crashes, part>>
  /\ UNCHANGED table

\* The mark and the wake in one statement, under a fresh snapshot: the rows
\* woken are those whose committed wait names this message.
Mark(d) ==
  /\ Alive(d) /\ pc[d] = "ownlocked"
  /\ pendWoken' = [pendWoken EXCEPT ![d] = {n \in Msgs : WaitingAt(Now, n) = msg[d]}]
  /\ pc' = [pc EXCEPT ![d] = "marked"]
  /\ UNCHANGED <<slot, snap, msg, dep, pendWait, slotLock, adv, dead, crashes, part>>
  /\ UNCHANGED table

\* COMMIT: the mark, the wakes and the subscriber's writes together.
CommitMark(d) ==
  /\ Alive(d) /\ pc[d] = "marked"
  /\ clock' = clock + 1
  /\ procAt' = [procAt EXCEPT ![msg[d]] = clock']
  /\ procOrder' = Append(procOrder, msg[d])
  /\ wokeAt' = [n \in Msgs |-> IF n \in pendWoken[d] THEN clock' ELSE wokeAt[n]]
  /\ Release(d, "idle")
  /\ UNCHANGED <<recvAt, pos, nextPos, waitAt, waitDep, snap, dead, crashes, part>>

\* ROLLBACK of a processing transaction: the dispatcher died, or the commit
\* failed.  Pending writes vanish, locks go.
Abort(d) ==
  /\ Alive(d) /\ pc[d] \in {"ready", "handled", "ownlocked", "marked"}
  /\ crashes < MaxCrashes /\ crashes' = crashes + 1
  /\ Release(d, "idle")
  /\ UNCHANGED <<snap, dead, part>> /\ UNCHANGED table

Next ==
  \/ \E m \in Msgs : Receive(m)
  \/ \E d \in Dispatchers : TakeSnap(d) \/ Take(d) \/ ReadHead(d) \/ Decide(d) \/ LockDep(d) \/ SetWait(d)
                            \/ CommitWait(d) \/ Handle(d) \/ LockOwn(d) \/ Mark(d) \/ CommitMark(d) \/ Abort(d)

Step(d) == TakeSnap(d) \/ Take(d) \/ ReadHead(d) \/ Decide(d) \/ LockDep(d) \/ SetWait(d)
           \/ CommitWait(d) \/ Handle(d) \/ LockOwn(d) \/ Mark(d) \/ CommitMark(d)

Fairness ==
  /\ \A m \in Msgs : WF_vars(Receive(m))
  /\ \A d \in Dispatchers : WF_vars(Step(d))

Spec == Init /\ [][Next]_<<vars, part>> /\ Fairness

(* ------------------------------------------------------------------------ *)
(* Properties of this level                                                   *)

TypeOK ==
  /\ clock \in Nat /\ nextPos \in Nat /\ crashes \in Nat
  /\ recvAt \in [Msgs -> Nat] /\ pos \in [Msgs -> Nat] /\ procAt \in [Msgs -> Nat]
  /\ waitAt \in [Msgs -> Nat] /\ wokeAt \in [Msgs -> Nat] /\ waitDep \in [Msgs -> Msgs \cup {None}]
  /\ procOrder \in Seq(Msgs)
  /\ pc \in [Dispatchers -> {"idle", "snapped", "taken", "headed", "ready", "aside", "locked", "waitset",
                             "handled", "ownlocked", "marked", "dead"}]
  /\ slot \in [Dispatchers -> Slots \cup {None}] /\ snap \in [Dispatchers -> Nat]
  /\ msg \in [Dispatchers -> Msgs \cup {None}] /\ dep \in [Dispatchers -> Msgs \cup {None}]
  /\ slotLock \in [Slots -> Dispatchers \cup {None}] /\ adv \in [Msgs -> Dispatchers \cup {None}]
  /\ dead \subseteq Dispatchers

\* No dispatcher was stopped by a database error.
NoDispatcherDied == dead = {}

\* The lock a dispatcher waits for, and its holder.
Wants(d) == IF pc[d] = "aside" /\ WithLock THEN dep[d]
            ELSE IF pc[d] = "handled" /\ WithLock THEN msg[d] ELSE None
BlockedOn(d) == IF Wants(d) = None \/ adv[Wants(d)] \in {None, d} THEN None ELSE adv[Wants(d)]

\* No two dispatchers wait for each other's lock; a longer cycle fails
\* liveness, since PostgreSQL would abort one and the loops would stop.
NoDeadlock == \A d1, d2 \in Dispatchers : ~(BlockedOn(d1) = d2 /\ BlockedOn(d2) = d1)

\* Every message that arrives is processed, eventually.
EventuallyProcessed == \A m \in Arrives : <>(procAt[m] > 0)

(* ------------------------------------------------------------------------ *)
(* The refinement: what the protocol model sees of this state                 *)

Received == ReceivedAt(Now)
Processed == ProcessedAt(Now)
Effects == [m \in Msgs |-> IF procAt[m] > 0 THEN 1 ELSE 0]
Holding == [s \in Slots |-> IF \E d \in Dispatchers : slot[d] = s /\ pc[d] \in {"ready", "handled", "ownlocked", "marked"}
                            THEN msg[CHOOSE d \in Dispatchers : slot[d] = s /\ pc[d] \in {"ready", "handled", "ownlocked", "marked"}]
                            ELSE None]
\* A wait is one step of the protocol; here it becomes one at the statement
\* that sets it, after the re-check, and stays until its commit is woken.
Waiting == [m \in Msgs |-> IF WaitingAt(Now, m) # None THEN WaitingAt(Now, m)
                           ELSE IF \E d \in Dispatchers : m \in pendWait[d]
                           THEN dep[CHOOSE d \in Dispatchers : m \in pendWait[d]]
                           ELSE None]
Zero == [m \in Msgs |-> 0]
AllDue == [m \in Msgs |-> TRUE]

Abstract == INSTANCE Inbox WITH
  Arrives <- Arrives, WaitsOnDependencies <- TRUE, WaitExpires <- FALSE, Poison <- {},
  MaxAttempts <- 0, BlockOnBackoff <- TRUE, MaxAdmin <- 0, AtomicWait <- TRUE,
  received <- Received, recvPos <- pos, nextRecv <- nextPos, processed <- Processed,
  effects <- Effects, holding <- Holding, waiting <- Waiting,
  attempts <- Zero, due <- AllDue, parked <- {}, resolved <- {}, admin <- 0, expired <- {}

\* Every step here is a step, or a stutter, of Inbox.tla.
Refines == Abstract!Init /\ [][Abstract!Next]_(Abstract!vars)

\* The protocol's invariants, read through the mapping.
EffectsOnce == Abstract!EffectsOnce
CausalOrder == Abstract!CausalOrder
HeldOnce == Abstract!HeldOnce
WaitingIsAside == Abstract!WaitingIsAside
ArrivalOrder == Abstract!ArrivalOrder

=============================================================================
