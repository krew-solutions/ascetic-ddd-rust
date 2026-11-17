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
(* Expires turns on the expiry of waits, a statement of its own beside the *)
(* dispatchers', so that its race with a mark is checked statement by      *)
(* statement (ADR-0036 R8); ForgetsOnExpiry makes it forget the identity   *)
(* waited for, the expiry as it was before R8.                             *)
(*                                                                         *)
(* Rotates is ADR-0041's slot, statement by statement: the slot row's      *)
(* cursor, the walk bounded by K inside the head's statement, `Passed`,    *)
(* the round and the dormancy that ends it, and a take that qualifies a    *)
(* slot by a queue row and its not being asleep.  It brings two things     *)
(* the FIFO has no need of here: a subscriber's own failure and the        *)
(* backoff it leaves (Fails), and an insert whose position is drawn by its *)
(* statement and committed later (SplitReceive), so that rows commit in    *)
(* another order than their positions.  Each is a definition, off by       *)
(* default, so that every configuration written before checks the same    *)
(* statements.  A rollback of a rotating dispatch is left out: decision 3  *)
(* moves the cursor after it by a statement of its own, which is slice 2's *)
(* and not modelled yet.                                                   *)
(*                                                                         *)
(* Left out, as protocol-level matters Inbox.tla settles: parking after    *)
(* attempts, operators.  The dependency check is taken with the head's     *)
(* snapshot; in the implementation it is a later statement, which can only *)
(* see more marks, never fewer.                                            *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
  Msgs,           \* messages, by identity
  Deps,           \* [Msgs -> SUBSET Msgs], causal dependencies; acyclic, at most one each
  Arrives,        \* SUBSET Msgs: the messages that ever arrive
  Slots,          \* the slots the table is cut into
  Placements,     \* SUBSET [Msgs -> Slots]: the cuts to check
  Dispatchers,    \* the dispatchers: loops, in any number of processes
  MaxCrashes,     \* rollbacks of a processing transaction and subscriber failures, in all
  WithLock,       \* the advisory lock on the dependency's identity, both sides
  HeadInTake,     \* the head read in the take's own statement
  WalkOn,         \* after a set-aside, the next head in the same transaction
  Expires,        \* waits run out: the expiry's statement runs beside the dispatchers
  ForgetsOnExpiry, \* the expiry clears waiting_for, as before ADR-0036 R8
  None            \* a model value

ASSUME \A m \in Msgs : Cardinality(Deps[m]) <= 1

(* ------------------------------------------------------------------------ *)
(* The rotation of ADR-0041, as definitions a configuration replaces          *)

\* As in Inbox.tla: one Tenant and the FIFO unless a configuration replaces
\* them.  A configuration that rotates replaces them here and in Inbox.tla
\* alike (`Rotates <- MCTrue`, `Rotates <- [Inbox] MCTrue`), so that the
\* refinement maps onto a rotating protocol.
TenantOf == [m \in Msgs |-> 1]
Rotates == FALSE
\* The Tenants a walk examines at most.
K == 1
Fails == FALSE
SplitReceive == FALSE
\* The designs the rotation's statements were checked against, each a
\* configuration that must fail:
\*   DormantAfterAtWalk  dormant_after read by the walk that ends the round,
\*                       not at the round's start;
\*   PassedMoves FALSE   a walk that finds nothing leaves the cursor;
\*   EndsRound FALSE     a round never ends, so the slot never sleeps.
DormantAfterAtWalk == FALSE
PassedMoves == TRUE
EndsRound == TRUE

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
NT == Max({TenantOf[m] : m \in Msgs})
\* How far Tenant t lies after the cursor c, cyclically over 1..NT; the
\* cursor's own Tenant lies farthest, at NT.
Dist(c, t) == IF t > c THEN t - c ELSE t + NT - c
Nearest(c, T) == CHOOSE t \in T : \A v \in T : Dist(c, t) <= Dist(c, v)
Farthest(c, T) == CHOOSE t \in T : \A v \in T : Dist(c, v) <= Dist(c, t)
\* The first n Tenants of T after c, cyclically: the walk wraps once.
Ahead(c, T, n) == {t \in T : Cardinality({v \in T : Dist(c, v) < Dist(c, t)}) < n}

ASSUME Rotates => ~HeadInTake /\ ~WalkOn
ASSUME Fails => Rotates
ASSUME K \in Nat \ {0}

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
  parkAt,     \* [Msgs -> Nat], the stamp of the expiry that parked the row; 0: never
  forgotAt,   \* [Msgs -> Nat], the stamp of an expiry that cleared the wait (ForgetsOnExpiry)
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
  crashes,
  \* the slot rows of ADR-0041, as committed.  No snapshot reads them: the
  \* walk holds the row's lock, and the take re-checks the row FOR UPDATE
  \* against its latest version.
  lastTenant, \* [Slots -> 0..NT], last_tenant: the cursor
  round,      \* [Slots -> record \cup {None}], round_from and the backoffs the round saw
  slept,      \* [Slots -> record \cup {None}], dormant_until and dormant_after
  pendLast,   \* [Dispatchers -> 0..NT \cup {None}], the cursor the holder's walk wrote, not committed
  \* the world's clock, not the database's
  capped,     \* SUBSET Slots: max_dormancy has passed since the slot went dormant
  failAt,     \* [Msgs -> Nat], the stamp of the failed attempt's commit; 0: never failed
  ended,      \* SUBSET Msgs: rows whose backoff has passed
  drawn       \* SUBSET Msgs: rows whose insert has drawn its position

vars == <<clock, recvAt, pos, nextPos, procAt, procOrder, waitAt, waitDep, wokeAt, parkAt, forgotAt,
          pc, slot, snap, msg, dep, pendWait, pendWoken, slotLock, adv, dead, crashes,
          lastTenant, round, slept, pendLast, capped, failAt, ended, drawn>>
table == <<clock, recvAt, pos, nextPos, procAt, procOrder, waitAt, waitDep, wokeAt, parkAt, forgotAt>>
slotRow == <<lastTenant, round, slept, pendLast, capped>>
world == <<failAt, ended, drawn>>
rotVars == <<lastTenant, round, slept, pendLast, capped, failAt, ended, drawn>>

VARIABLE part  \* [Msgs -> Slots], the cut, fixed at the start

(* ------------------------------------------------------------------------ *)
(* Visibility                                                                 *)

\* A version stamped `at` is committed as of snapshot `t`.
Seen(t, at) == at > 0 /\ at <= t

ReceivedAt(t) == {m \in Msgs : Seen(t, recvAt[m])}
ProcessedAt(t) == {m \in Msgs : Seen(t, procAt[m])}
WaitingAt(t, m) == IF Seen(t, waitAt[m]) /\ ~Seen(t, wokeAt[m]) /\ ~Seen(t, forgotAt[m])
                   THEN waitDep[m] ELSE None
\* Parked when its wait ran out, and not woken since: the wake unparks it.
ParkedAt(t, m) == Seen(t, parkAt[m]) /\ ~Seen(t, wokeAt[m])
\* Due as snapshot t sees it: no failure committed, or its backoff passed.
DueSeen(t, m) == ~Seen(t, failAt[m]) \/ m \in ended

\* The queue of slot s as dispatcher d's statement under snapshot t sees it:
\* committed rows neither processed nor waiting, and not what d's own
\* transaction has set aside or marked -- a transaction sees its own writes.
QueueAt(t, s, d) ==
  {m \in ReceivedAt(t) : /\ part[m] = s
                         /\ m \notin ProcessedAt(t) /\ WaitingAt(t, m) = None /\ ~ParkedAt(t, m)
                         /\ m \notin pendWait[d] /\ ~(pc[d] \in {"marked"} /\ msg[d] = m)}
Oldest(S) == CHOOSE m \in S : \A n \in S : pos[m] <= pos[n]
HeadAt(t, s, d) == IF QueueAt(t, s, d) = {} THEN None ELSE Oldest(QueueAt(t, s, d))

\* Rotating: the head of Tenant t among rows Q, its oldest by position.
HeadIn(Q, t) == Oldest({m \in Q : TenantOf[m] = t})
MaxPos(Q) == Max({pos[m] : m \in Q})

\* A dormant slot is asleep until the first of the backoffs its round saw
\* passes -- dormant_until is the earliest of them -- or max_dormancy does,
\* or a row of its queue lies after dormant_after.
Asleep(Q, s) ==
  /\ slept[s] # None
  /\ s \notin capped
  /\ slept[s].saw \cap ended = {}
  /\ \A m \in Q : pos[m] <= slept[s].after

Now == clock

(* ------------------------------------------------------------------------ *)

Init ==
  /\ part \in Placements
  /\ clock = 0
  /\ recvAt = [m \in Msgs |-> 0] /\ pos = [m \in Msgs |-> 0] /\ nextPos = 1
  /\ procAt = [m \in Msgs |-> 0] /\ procOrder = <<>>
  /\ waitAt = [m \in Msgs |-> 0] /\ waitDep = [m \in Msgs |-> None] /\ wokeAt = [m \in Msgs |-> 0]
  /\ parkAt = [m \in Msgs |-> 0] /\ forgotAt = [m \in Msgs |-> 0]
  /\ pc = [d \in Dispatchers |-> "idle"]
  /\ slot = [d \in Dispatchers |-> None] /\ snap = [d \in Dispatchers |-> 0]
  /\ msg = [d \in Dispatchers |-> None] /\ dep = [d \in Dispatchers |-> None]
  /\ pendWait = [d \in Dispatchers |-> {}] /\ pendWoken = [d \in Dispatchers |-> {}]
  /\ slotLock = [s \in Slots |-> None] /\ adv = [m \in Msgs |-> None]
  /\ dead = {} /\ crashes = 0
  /\ lastTenant = [s \in Slots |-> 0] /\ round = [s \in Slots |-> None] /\ slept = [s \in Slots |-> None]
  /\ pendLast = [d \in Dispatchers |-> None] /\ capped = {}
  /\ failAt = [m \in Msgs |-> 0] /\ ended = {} /\ drawn = {}

Dispatching == <<pc, slot, snap, msg, dep, pendWait, pendWoken, slotLock, adv, dead, crashes>>

\* INSERT ... ON CONFLICT DO NOTHING in a transaction of its own, committed at once.
Receive(m) ==
  /\ ~SplitReceive
  /\ m \in Arrives /\ recvAt[m] = 0
  /\ clock' = clock + 1
  /\ recvAt' = [recvAt EXCEPT ![m] = clock']
  /\ pos' = [pos EXCEPT ![m] = nextPos] /\ nextPos' = nextPos + 1
  /\ drawn' = drawn \cup {m}
  /\ UNCHANGED <<procAt, procOrder, waitAt, waitDep, wokeAt, parkAt, forgotAt, part>>
  /\ UNCHANGED Dispatching /\ UNCHANGED slotRow /\ UNCHANGED <<failAt, ended>>

\* With SplitReceive, the insert draws its position from the sequence, and
\* its transaction commits later: another insert may draw after it and
\* commit before it (ADR-0035's class).  The rows of one Tenant arrive one
\* at a time, as a stream's do from its partition: the order of arrival
\* within a Tenant is then the order of positions, and only rows of
\* different Tenants commit out of it.
Draw(m) ==
  /\ SplitReceive
  /\ m \in Arrives /\ m \notin drawn
  /\ \A n \in drawn : TenantOf[n] = TenantOf[m] => recvAt[n] > 0
  /\ drawn' = drawn \cup {m}
  /\ pos' = [pos EXCEPT ![m] = nextPos] /\ nextPos' = nextPos + 1
  /\ UNCHANGED <<clock, recvAt, procAt, procOrder, waitAt, waitDep, wokeAt, parkAt, forgotAt, part>>
  /\ UNCHANGED Dispatching /\ UNCHANGED slotRow /\ UNCHANGED <<failAt, ended>>

CommitRecv(m) ==
  /\ m \in drawn /\ recvAt[m] = 0
  /\ clock' = clock + 1
  /\ recvAt' = [recvAt EXCEPT ![m] = clock']
  /\ UNCHANGED <<pos, nextPos, procAt, procOrder, waitAt, waitDep, wokeAt, parkAt, forgotAt, part>>
  /\ UNCHANGED Dispatching /\ UNCHANGED rotVars

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

\* The slot row the walk wrote commits with the transaction: the cursor,
\* and the round and the dormancy cleared, since a head was found.
CommitSlot(d) ==
  IF pendLast[d] = None
  THEN UNCHANGED slotRow
  ELSE /\ lastTenant' = [lastTenant EXCEPT ![slot[d]] = pendLast[d]]
       /\ round' = [round EXCEPT ![slot[d]] = None]
       /\ slept' = [slept EXCEPT ![slot[d]] = None]
       /\ capped' = capped \ {slot[d]}
       /\ pendLast' = [pendLast EXCEPT ![d] = None]

\* The take's statement begins: its snapshot is taken before any lock.
TakeSnap(d) ==
  /\ Alive(d) /\ pc[d] = "idle"
  /\ snap' = [snap EXCEPT ![d] = Now] /\ pc' = [pc EXCEPT ![d] = "snapped"]
  /\ UNCHANGED <<slot, msg, dep, pendWait, pendWoken, slotLock, adv, dead, crashes, part>>
  /\ UNCHANGED table /\ UNCHANGED rotVars

\* A slot the take qualifies, as its snapshot sees it; the row is locked
\* now, FOR UPDATE SKIP LOCKED, and a slot another holds is passed by.  As a
\* FIFO, a slot with a head.  Rotating, a slot with a queue row that is not
\* asleep: whether a head is due is the walk's to find (ADR-0041 decision 2).
Qualifies(t, s, d) ==
  IF Rotates THEN QueueAt(t, s, d) # {} /\ ~Asleep(QueueAt(t, s, d), s)
  ELSE HeadAt(t, s, d) # None
DueAt(t, d) == {s \in Slots : Qualifies(t, s, d)}

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
  /\ UNCHANGED <<snap, dep, pendWait, pendWoken, adv, crashes, part>> /\ UNCHANGED table /\ UNCHANGED rotVars
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
  /\ Alive(d) /\ pc[d] = "taken" /\ ~Rotates
  /\ LET h == HeadAt(Now, slot[d], d) IN
     IF h = None
     THEN Release(d, "idle") /\ UNCHANGED <<dead, crashes>>
     ELSE /\ msg' = [msg EXCEPT ![d] = h] /\ Decided(d, h)
          /\ UNCHANGED <<slot, pendWait, pendWoken, slotLock, adv, dead, crashes>>
  /\ UNCHANGED <<snap, part>> /\ UNCHANGED table /\ UNCHANGED rotVars

\* Rotating, the head's statement is the walk (ADR-0041 decision 2), under
\* a fresh snapshot after the slot's lock.  It starts at the first Tenant
\* after the cursor, wraps once, and examines at most K Tenants of the
\* slot's queue, the head of each its oldest row.
\*   - A due head among them: the nearest is the head, its dependencies are
\*     checked, and the cursor is written in the transaction, to commit
\*     with it.
\*   - None: the cursor moves past them, the round goes on, and the slot row
\*     commits at once.  The round began at the cursor of its first walk;
\*     when a walk passes that place, or examined every Tenant, the round
\*     ends and the slot goes dormant: dormant_until the earliest backoff
\*     the round saw, or max_dormancy; dormant_after the highest position of
\*     the queue when the round began (DormantAfterAtWalk: at this walk).
\* A walk that took no head and rolls back leaves the slot as if it had not
\* run, so its rollback is not modelled.
\* Two fields of the round are the model's, not columns: the rows whose
\* insert was in flight when the round began, and the round's snapshot.
\* NoDueHeadAsleep reads them.
Walk(d) ==
  /\ Alive(d) /\ pc[d] = "taken" /\ Rotates
  /\ UNCHANGED <<snap, part>> /\ UNCHANGED table /\ UNCHANGED world
  /\ LET s == slot[d]
         Q == QueueAt(Now, s, d)
         c == lastTenant[s]
         TS == {TenantOf[m] : m \in Q}
         E == Ahead(c, TS, K)
         DueE == {t \in E : DueSeen(Now, HeadIn(Q, t))}
     IN
     IF Q = {}
     THEN Release(d, "idle") /\ UNCHANGED <<dead, crashes>> /\ UNCHANGED slotRow
     ELSE IF DueE # {}
     THEN LET t == Nearest(c, DueE) IN
          /\ msg' = [msg EXCEPT ![d] = HeadIn(Q, t)] /\ Decided(d, HeadIn(Q, t))
          /\ pendLast' = [pendLast EXCEPT ![d] = t]
          /\ UNCHANGED <<slot, pendWait, pendWoken, slotLock, adv, dead, crashes>>
          /\ UNCHANGED <<lastTenant, round, slept, capped>>
     ELSE LET u == Farthest(c, E)
              r == IF round[s] = None
                   THEN [from |-> c, saw |-> {}, after |-> MaxPos(Q),
                         inflight |-> drawn \ ReceivedAt(Now), at |-> Now]
                   ELSE round[s]
              saw == r.saw \cup {HeadIn(Q, t) : t \in E}
              ends == EndsRound /\ (E = TS \/ (r.from # c /\ Dist(c, r.from) <= Dist(c, u)))
          IN
          /\ lastTenant' = [lastTenant EXCEPT ![s] = IF PassedMoves THEN u ELSE c]
          /\ IF ends
             THEN /\ round' = [round EXCEPT ![s] = None]
                  /\ slept' = [slept EXCEPT ![s] =
                                 [saw |-> saw, after |-> IF DormantAfterAtWalk THEN MaxPos(Q) ELSE r.after,
                                  inflight |-> r.inflight, at |-> r.at]]
                  /\ capped' = capped \ {s}
             ELSE /\ round' = [round EXCEPT ![s] = [r EXCEPT !.saw = saw]]
                  /\ UNCHANGED <<slept, capped>>
          /\ Release(d, "idle") /\ UNCHANGED <<dead, crashes, pendLast>>

\* The same check for a head the take's statement read (HeadInTake).
Decide(d) ==
  /\ Alive(d) /\ pc[d] = "headed"
  /\ Decided(d, msg[d])
  /\ UNCHANGED <<slot, snap, msg, pendWait, pendWoken, slotLock, adv, dead, crashes, part>>
  /\ UNCHANGED table /\ UNCHANGED rotVars

\* pg_advisory_xact_lock on the dependency's identity; held to the end of the
\* transaction, re-entrant for its holder, waited for otherwise.
LockDep(d) ==
  /\ Alive(d) /\ pc[d] = "aside"
  /\ WithLock => adv[dep[d]] \in {None, d}
  /\ adv' = IF WithLock THEN [adv EXCEPT ![dep[d]] = d] ELSE adv
  /\ pc' = [pc EXCEPT ![d] = "locked"]
  /\ UNCHANGED <<slot, snap, msg, dep, pendWait, pendWoken, slotLock, dead, crashes, part>>
  /\ UNCHANGED table /\ UNCHANGED rotVars

\* UPDATE ... SET waiting_for WHERE ... AND NOT EXISTS (the dependency
\* processed): under a fresh snapshot.  Nothing to wait for any more: the
\* transaction ends with nothing done but what the walk wrote -- rotating,
\* the cursor past the head's Tenant, which commits with it as the
\* implementation commits that transaction.
SetWait(d) ==
  /\ Alive(d) /\ pc[d] = "locked"
  /\ IF dep[d] \in ProcessedAt(Now)
     THEN Release(d, "idle") /\ UNCHANGED <<dead, crashes>> /\ CommitSlot(d)
     ELSE /\ pendWait' = [pendWait EXCEPT ![d] = @ \cup {msg[d]}]
          /\ pc' = [pc EXCEPT ![d] = IF WalkOn THEN "taken" ELSE "waitset"]
          /\ UNCHANGED <<slot, msg, dep, pendWoken, slotLock, adv, dead, crashes>>
          /\ UNCHANGED slotRow
  /\ UNCHANGED <<snap, part>> /\ UNCHANGED table /\ UNCHANGED world

\* COMMIT of a transaction that set rows aside and nothing else.
CommitWait(d) ==
  /\ Alive(d) /\ pc[d] = "waitset"
  /\ clock' = clock + 1
  /\ waitAt' = [m \in Msgs |-> IF m \in pendWait[d] THEN clock' ELSE waitAt[m]]
  /\ waitDep' = [m \in Msgs |-> IF m \in pendWait[d] THEN dep[d] ELSE waitDep[m]]
  /\ CommitSlot(d)
  /\ Release(d, "idle")
  /\ UNCHANGED <<recvAt, pos, nextPos, procAt, procOrder, wokeAt, snap, dead, crashes, parkAt, forgotAt, part>>
  /\ UNCHANGED world

\* With WalkOn the rows set aside have different dependencies: the wait of
\* each is the dependency it was decided on.  This module keeps one `dep`
\* per dispatcher, so WalkOn commits are modelled for the last decision only
\* -- enough for the lock cycle it exists to show, and no more.

\* The subscriber ran and returned; its writes are in the transaction.
Handle(d) ==
  /\ Alive(d) /\ pc[d] = "ready"
  /\ pc' = [pc EXCEPT ![d] = "handled"]
  /\ UNCHANGED <<slot, snap, msg, dep, pendWait, pendWoken, slotLock, adv, dead, crashes, part>>
  /\ UNCHANGED table /\ UNCHANGED rotVars

\* With Fails, the subscriber returned its own failure instead: its writes
\* roll back to the savepoint, record_failure sets the backoff in the same
\* transaction, and the transaction commits with the slot row the walk
\* wrote (ADR-0005).  A message fails at most once here, within the budget
\* the protocol's Fail shares with crashes.
FailAttempt(d) ==
  /\ Fails
  /\ Alive(d) /\ pc[d] = "ready"
  /\ failAt[msg[d]] = 0
  /\ crashes < MaxCrashes /\ crashes' = crashes + 1
  /\ clock' = clock + 1
  /\ failAt' = [failAt EXCEPT ![msg[d]] = clock']
  /\ CommitSlot(d)
  /\ Release(d, "idle")
  /\ UNCHANGED <<recvAt, pos, nextPos, procAt, procOrder, waitAt, waitDep, wokeAt, parkAt, forgotAt,
                snap, dead, part>>
  /\ UNCHANGED <<ended, drawn>>

\* The mark's lock on the message's own identity, before the mark.
LockOwn(d) ==
  /\ Alive(d) /\ pc[d] = "handled"
  /\ WithLock => adv[msg[d]] \in {None, d}
  /\ adv' = IF WithLock THEN [adv EXCEPT ![msg[d]] = d] ELSE adv
  /\ pc' = [pc EXCEPT ![d] = "ownlocked"]
  /\ UNCHANGED <<slot, snap, msg, dep, pendWait, pendWoken, slotLock, dead, crashes, part>>
  /\ UNCHANGED table /\ UNCHANGED rotVars

\* The mark and the wake in one statement, under a fresh snapshot: the rows
\* woken are those whose committed wait names this message.
Mark(d) ==
  /\ Alive(d) /\ pc[d] = "ownlocked"
  /\ pendWoken' = [pendWoken EXCEPT ![d] = {n \in Msgs : WaitingAt(Now, n) = msg[d]}]
  /\ pc' = [pc EXCEPT ![d] = "marked"]
  /\ UNCHANGED <<slot, snap, msg, dep, pendWait, slotLock, adv, dead, crashes, part>>
  /\ UNCHANGED table /\ UNCHANGED rotVars

\* COMMIT: the mark, the wakes and the subscriber's writes together.
CommitMark(d) ==
  /\ Alive(d) /\ pc[d] = "marked"
  /\ clock' = clock + 1
  /\ procAt' = [procAt EXCEPT ![msg[d]] = clock']
  /\ procOrder' = Append(procOrder, msg[d])
  /\ wokeAt' = [n \in Msgs |-> IF n \in pendWoken[d] THEN clock' ELSE wokeAt[n]]
  /\ CommitSlot(d)
  /\ Release(d, "idle")
  /\ UNCHANGED <<recvAt, pos, nextPos, waitAt, waitDep, snap, dead, crashes, parkAt, forgotAt, part>>
  /\ UNCHANGED world

\* The expiry's UPDATE ... WHERE waiting_for IS NOT NULL AND waiting_since is
\* old ... FOR UPDATE SKIP LOCKED, in a transaction of its own, committed at
\* once.  A row an in-flight transaction holds -- its wait being set, or its
\* wake by a mark -- is skipped; any other is re-checked against its latest
\* committed version, so a row woken meanwhile is not parked.  Parked, the
\* row keeps waiting_for (ADR-0036 R8), or, with ForgetsOnExpiry, loses it.
\* A mark's statement that meets the expiry in flight waits for its commit
\* and re-checks the row the same way: the expiry first, as here.
Expire(m) ==
  /\ Expires
  /\ WaitingAt(Now, m) # None /\ ~ParkedAt(Now, m)
  /\ \A d \in Dispatchers : m \notin pendWait[d] /\ m \notin pendWoken[d]
  /\ clock' = clock + 1
  /\ parkAt' = [parkAt EXCEPT ![m] = clock']
  /\ forgotAt' = IF ForgetsOnExpiry THEN [forgotAt EXCEPT ![m] = clock'] ELSE forgotAt
  /\ UNCHANGED <<recvAt, pos, nextPos, procAt, procOrder, waitAt, waitDep, wokeAt, part>>
  /\ UNCHANGED Dispatching /\ UNCHANGED rotVars

\* ROLLBACK of a processing transaction: the dispatcher died, or the commit
\* failed.  Pending writes vanish, locks go.  Not while rotating: there the
\* cursor the walk wrote vanishes with them, and decision 3's statement that
\* writes it again is not modelled yet.
Abort(d) ==
  /\ ~Rotates
  /\ Alive(d) /\ pc[d] \in {"ready", "handled", "ownlocked", "marked"}
  /\ crashes < MaxCrashes /\ crashes' = crashes + 1
  /\ Release(d, "idle")
  /\ UNCHANGED <<snap, dead, part>> /\ UNCHANGED table /\ UNCHANGED rotVars

(* ------------------------------------------------------------------------ *)
(* The world's clock                                                          *)

\* The backoff of a failed row passes.
Elapse(m) ==
  /\ failAt[m] > 0 /\ m \notin ended
  /\ ended' = ended \cup {m}
  /\ UNCHANGED table /\ UNCHANGED Dispatching /\ UNCHANGED part
  /\ UNCHANGED <<lastTenant, round, slept, pendLast, capped, failAt, drawn>>

\* max_dormancy passes for a dormant slot.
Cap(s) ==
  /\ slept[s] # None /\ s \notin capped
  /\ capped' = capped \cup {s}
  /\ UNCHANGED table /\ UNCHANGED Dispatching /\ UNCHANGED part
  /\ UNCHANGED <<lastTenant, round, slept, pendLast, failAt, ended, drawn>>

(* ------------------------------------------------------------------------ *)

Next ==
  \/ \E m \in Msgs : Receive(m) \/ Draw(m) \/ CommitRecv(m) \/ Expire(m) \/ Elapse(m)
  \/ \E s \in Slots : Cap(s)
  \/ \E d \in Dispatchers : TakeSnap(d) \/ Take(d) \/ ReadHead(d) \/ Walk(d) \/ Decide(d) \/ LockDep(d)
                            \/ SetWait(d) \/ CommitWait(d) \/ Handle(d) \/ FailAttempt(d) \/ LockOwn(d)
                            \/ Mark(d) \/ CommitMark(d) \/ Abort(d)

Step(d) == TakeSnap(d) \/ Take(d) \/ ReadHead(d) \/ Walk(d) \/ Decide(d) \/ LockDep(d) \/ SetWait(d)
           \/ CommitWait(d) \/ Handle(d) \/ LockOwn(d) \/ Mark(d) \/ CommitMark(d)

Fairness ==
  /\ \A m \in Msgs : WF_vars(Receive(m)) /\ WF_vars(Draw(m)) /\ WF_vars(CommitRecv(m)) /\ WF_vars(Elapse(m))
  /\ \A s \in Slots : WF_vars(Cap(s))
  /\ \A d \in Dispatchers : WF_vars(Step(d))

Spec == Init /\ [][Next]_<<vars, part>> /\ Fairness

(* ------------------------------------------------------------------------ *)
(* Properties of this level                                                   *)

TypeOK ==
  /\ clock \in Nat /\ nextPos \in Nat /\ crashes \in Nat
  /\ recvAt \in [Msgs -> Nat] /\ pos \in [Msgs -> Nat] /\ procAt \in [Msgs -> Nat]
  /\ waitAt \in [Msgs -> Nat] /\ wokeAt \in [Msgs -> Nat] /\ waitDep \in [Msgs -> Msgs \cup {None}]
  /\ parkAt \in [Msgs -> Nat] /\ forgotAt \in [Msgs -> Nat]
  /\ procOrder \in Seq(Msgs)
  /\ pc \in [Dispatchers -> {"idle", "snapped", "taken", "headed", "ready", "aside", "locked", "waitset",
                             "handled", "ownlocked", "marked", "dead"}]
  /\ slot \in [Dispatchers -> Slots \cup {None}] /\ snap \in [Dispatchers -> Nat]
  /\ msg \in [Dispatchers -> Msgs \cup {None}] /\ dep \in [Dispatchers -> Msgs \cup {None}]
  /\ slotLock \in [Slots -> Dispatchers \cup {None}] /\ adv \in [Msgs -> Dispatchers \cup {None}]
  /\ dead \subseteq Dispatchers
  /\ lastTenant \in [Slots -> 0..NT] /\ pendLast \in [Dispatchers -> (0..NT) \cup {None}]
  /\ \A s \in Slots : round[s] = None \/ (round[s].from \in 0..NT /\ round[s].saw \subseteq Msgs)
  /\ \A s \in Slots : slept[s] = None \/ (slept[s].saw \subseteq Msgs /\ slept[s].after \in Nat)
  /\ capped \subseteq Slots /\ failAt \in [Msgs -> Nat] /\ ended \subseteq Msgs /\ drawn \subseteq Msgs

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

\* The committed queue of slot s.
Queue(s) == {m \in ReceivedAt(Now) : /\ part[m] = s /\ m \notin ProcessedAt(Now)
                                     /\ WaitingAt(Now, m) = None /\ ~ParkedAt(Now, m)}

\* A slot sleeps past a due head only when that head could not be seen by
\* the round that put it to sleep: its insert was in flight when the round
\* began, or a dependency's mark woke it after (ADR-0041, § Rule of Three
\* 3).  Any other due head wakes the slot -- by its position after
\* dormant_after, or by the end of the backoff the round saw.
NoDueHeadAsleep ==
  \A s \in Slots :
    (slept[s] # None /\ Asleep(Queue(s), s)) =>
      \A t \in {TenantOf[m] : m \in Queue(s)} :
        LET h == Oldest({m \in Queue(s) : TenantOf[m] = t}) IN
        DueSeen(Now, h) => (h \in slept[s].inflight \/ wokeAt[h] > slept[s].at)

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
\* Rotating, it is one at the walk that chose the head instead, since the
\* walk moved the cursor: a dependency marked before the wait is set then
\* wakes it in the protocol, and the transaction commits the cursor alone.
SetsAside(d) == Rotates /\ pc[d] \in {"aside", "locked"} /\ dep[d] \notin Processed
Waiting == [m \in Msgs |-> IF WaitingAt(Now, m) # None THEN WaitingAt(Now, m)
                           ELSE IF \E d \in Dispatchers : m \in pendWait[d]
                           THEN dep[CHOOSE d \in Dispatchers : m \in pendWait[d]]
                           ELSE IF \E d \in Dispatchers : SetsAside(d) /\ msg[d] = m
                           THEN dep[CHOOSE d \in Dispatchers : SetsAside(d) /\ msg[d] = m]
                           ELSE None]
Zero == [m \in Msgs |-> 0]
Due == [m \in Msgs |-> failAt[m] = 0 \/ m \in ended]
\* Parked only when a wait ran out: parking after attempts is not modelled here.
Parked == {m \in Msgs : ParkedAt(Now, m)}
RecvPos == [m \in Msgs |-> IF recvAt[m] > 0 THEN pos[m] ELSE 0]
NextRecv == IF Received = {} THEN 1 ELSE Max({pos[m] : m \in Received}) + 1
\* The cursor as the protocol has it: the holder's, once its walk chose a
\* head; the committed one otherwise.
Walked(d) == pendLast[d] # None /\ pc[d] \in {"ready", "handled", "ownlocked", "marked", "aside", "locked", "waitset"}
Cursor == [s \in Slots |-> IF \E d \in Dispatchers : slot[d] = s /\ Walked(d)
                           THEN pendLast[CHOOSE d \in Dispatchers : slot[d] = s /\ Walked(d)]
                           ELSE lastTenant[s]]

Abstract == INSTANCE Inbox WITH Unreadable <- {}, unread <- {}, Permanent <- {}, cursor <- Cursor, guest <- [s \in Slots |-> 0],
  Arrives <- Arrives, WaitsOnDependencies <- TRUE, WaitExpires <- Expires, Poison <- {},
  MaxAttempts <- 0, BlockOnBackoff <- TRUE, MaxAdmin <- 0, AtomicWait <- TRUE,
  received <- Received, recvPos <- RecvPos, nextRecv <- NextRecv, processed <- Processed,
  effects <- Effects, holding <- Holding, waiting <- Waiting,
  attempts <- Zero, due <- Due, parked <- Parked, resolved <- {}, admin <- 0, expired <- Parked

\* Every step here is a step, or a stutter, of Inbox.tla.  An insert that
\* commits after another drew a later position is the protocol's ReceiveAs,
\* with the position the sequence drew.
Refines == Abstract!Init /\ [][Abstract!Next \/ \E m \in Msgs : Abstract!ReceiveAs(m, pos[m])]_(Abstract!vars)

\* The protocol's invariants, read through the mapping.
EffectsOnce == Abstract!EffectsOnce
CausalOrder == Abstract!CausalOrder
HeldOnce == Abstract!HeldOnce
WaitingIsAside == Abstract!WaitingIsAside
ArrivalOrder == Abstract!ArrivalOrder

=============================================================================
