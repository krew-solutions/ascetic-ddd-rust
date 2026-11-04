---------------------------- MODULE OutboxPg ----------------------------
(***************************************************************************)
(* The relay of `crates/outbox` one level below the protocol: the          *)
(* statements of ADR-0035, run against PostgreSQL under READ COMMITTED.    *)
(*                                                                         *)
(* PostgreSQL is modelled as far as the relay relies on it:                *)
(*   - a transaction's id is assigned at its first write, or earlier when  *)
(*     it writes something else first (the inbox's take writes its slot);  *)
(*   - a snapshot is `xmin:xmax:xip` as GetSnapshotData computes it:       *)
(*     `xmax` one past the latest completed id, `xip` the ids still        *)
(*     running below it, `xmin` the least of them or `xmax`;               *)
(*   - visibility is that arithmetic, `pg_visible_in_snapshot`, and a row  *)
(*     can be read when its transaction committed and is visible;          *)
(*   - a statement's snapshot is taken before the lock it asks for, and    *)
(*     FOR UPDATE returns the row's latest committed version;              *)
(*   - the position sequence draws one value at a time, in real time.      *)
(*                                                                         *)
(* A stream's versions are written in order: a row of version v is written *)
(* once version v - 1 has committed (decision 7, P1).  A transaction may   *)
(* hold an id and write nothing here, open for ever: one of another        *)
(* database.                                                               *)
(*                                                                         *)
(* A position row belongs to a selection: the lanes of ADR-0033, or one    *)
(* selection of everything.  Dispatchers of one selection race for its     *)
(* row, FOR UPDATE SKIP LOCKED.  A dispatcher's transaction: the lock's    *)
(* statement, then the epoch's start and the batch under a later snapshot, *)
(* the hand-on, the commit.  The probe of decision 2, which only spares a  *)
(* lock, is left out: it decides whether a look happens, not what it does. *)
(*                                                                         *)
(* Switches name the designs the reviews of ADR-0035 refuted, and today's  *)
(* rule; each must-fail configuration sets one and names the property it   *)
(* breaks (check.sh).                                                      *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
  Txs,              \* transactions
  EarlyXid,         \* SUBSET Txs: those that may take their id before their first row
  MayAbort,         \* SUBSET Txs: those that may roll back
  Msgs,             \* outbox rows
  TxOf,             \* [Msgs -> Txs], the transaction that writes the row
  StreamOf,         \* [Msgs -> Streams]
  VersionOf,        \* [Msgs -> Nat], the row's version within its stream
  LaneOf,           \* [Msgs -> Lanes], the class of its destination
  Broker,           \* the lane a broker takes
  Sels,             \* the relay's selections, each with a position row
  SelLanes,         \* [Sels -> SUBSET Lanes]
  Dispatchers,
  SelOf,            \* [Dispatchers -> Sels]
  BatchSize,
  MaxCrashes,       \* rollbacks of a dispatcher's transaction, in all
  MaxRewinds,       \* set_position, in all
  Rule,             \* "epoch": ADR-0035; "xmin": ADR-0007, released below xmin
  OneStatement,     \* the epoch read in the statement that takes the lock
  Membership,       \* "visible"; "noxip": delivered's in-progress list ignored; "range": ids only
  Filtered,         \* a batch keeps the epoch's rows only
  Bounded,          \* a batch stops at epoch_end
  Order,            \* "position", or "xid" within an epoch
  DrainTo,          \* "epoch", or "now": a drain stores the current snapshot
  OffsetCarried,    \* a new epoch keeps the last epoch's offset
  RewindForm,       \* "intersect", or "naive": 'F:F:'
  RewindKeepsEpoch, \* set_position leaves the epoch in flight
  DrawEarly,        \* P1 broken: a row written before its predecessor committed
  BrokerDown,       \* the broker never takes a row
  None              \* a model value

ASSUME BatchSize \in Nat \ {0}
ASSUME Rule \in {"epoch", "xmin"}
ASSUME Membership \in {"visible", "noxip", "range"}
ASSUME Order \in {"position", "xid"}
ASSUME DrainTo \in {"epoch", "now"}
ASSUME RewindForm \in {"intersect", "naive"}

VARIABLES
  \* PostgreSQL
  txState,   \* [Txs -> {"idle", "open", "committed", "aborted"}]
  xid,       \* [Txs -> Nat], 0 until assigned
  nextXid,
  latest,    \* the latest completed id, 0 at first
  written,   \* [Msgs -> BOOLEAN]
  pos,       \* [Msgs -> Nat], the position drawn, 0 until then
  nextPos,
  \* the position rows, as committed
  dlv,       \* [Sels -> Snapshot], delivered
  ep,        \* [Sels -> Snapshot], the epoch, NoSnap when none
  off,       \* [Sels -> Nat], epoch_offset
  endp,      \* [Sels -> Nat], epoch_end
  old,       \* [Sels -> Nat \X Nat], ADR-0007's (transaction, offset)
  lock,      \* [Sels -> Dispatchers \cup {None}], FOR UPDATE on the row
  \* the dispatchers' transactions
  pc,        \* [Dispatchers -> STRING]
  stmt,      \* [Dispatchers -> Snapshot], the lock's statement's snapshot
  wDlv, wEp, wOff, wEnd, wOld, \* the row as the transaction holds it
  drain,     \* [Dispatchers -> BOOLEAN], the batch drains its epoch
  batch,     \* [Dispatchers -> Seq(Msgs)]
  \* what the subscribers received
  received,  \* [Sels -> Seq(Msgs)], in order, repeats included
  since,     \* [Sels -> SUBSET Msgs], received since the last rewind
  crashes,
  rewinds

pg == <<txState, xid, nextXid, latest, written, pos, nextPos>>
rows == <<dlv, ep, off, endp, old, lock>>
work == <<pc, stmt, wDlv, wEp, wOff, wEnd, wOld, drain, batch>>
ghost == <<received, since, crashes, rewinds>>
vars == <<pg, rows, work, ghost>>

NoSnap == [xmin |-> 0, xmax |-> 0, xip |-> {}]
\* Nothing delivered: '1:1:' ('0:0:' is not a valid snapshot).
Zero == [xmin |-> 1, xmax |-> 1, xip |-> {}]

Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Max(S) == CHOOSE x \in S : \A y \in S : x >= y

MaxXid == Cardinality(Txs)
Ids == 1..MaxXid

(* ------------------------------------------------------------------------ *)
(* PostgreSQL                                                                 *)

Running == {xid[t] : t \in {u \in Txs : txState[u] = "open" /\ xid[u] # 0}}

\* GetSnapshotData: ids at or above xmax are not listed, and not visible.
Now ==
  LET xmax == latest + 1
      xip == {x \in Running : x < xmax}
  IN [xmin |-> Min(xip \cup {xmax}), xmax |-> xmax, xip |-> xip]

\* pg_visible_in_snapshot.
Vis(x, s) == x # 0 /\ (x < s.xmin \/ (x < s.xmax /\ x \notin s.xip))

XidOf(m) == xid[TxOf[m]]
Committed(m) == txState[TxOf[m]] = "committed"
Readable(m, s) == written[m] /\ Committed(m) /\ Vis(XidOf(m), s)
InSel(m, s) == LaneOf[m] \in SelLanes[s]

Key(m) == <<XidOf(m), pos[m]>>
Before(a, b) == a[1] < b[1] \/ (a[1] = b[1] /\ a[2] < b[2])
ByPos(a, b) == pos[a] < pos[b]
ByKey(a, b) == Before(Key(a), Key(b))

Less(a, b, byKey) == IF byKey THEN ByKey(a, b) ELSE ByPos(a, b)

RECURSIVE Sorted(_, _)
Sorted(S, byKey) ==
  IF S = {} THEN <<>>
  ELSE LET first == CHOOSE m \in S : \A n \in S : m = n \/ Less(m, n, byKey)
       IN <<first>> \o Sorted(S \ {first}, byKey)

Take(seq, n) == SubSeq(seq, 1, IF Len(seq) < n THEN Len(seq) ELSE n)

(* ------------------------------------------------------------------------ *)

Init ==
  /\ txState = [t \in Txs |-> "idle"]
  /\ xid = [t \in Txs |-> 0] /\ nextXid = 1 /\ latest = 0
  /\ written = [m \in Msgs |-> FALSE] /\ pos = [m \in Msgs |-> 0] /\ nextPos = 1
  /\ dlv = [s \in Sels |-> Zero] /\ ep = [s \in Sels |-> NoSnap]
  /\ off = [s \in Sels |-> 0] /\ endp = [s \in Sels |-> 0]
  /\ old = [s \in Sels |-> <<0, 0>>]
  /\ lock = [s \in Sels |-> None]
  /\ pc = [d \in Dispatchers |-> "idle"]
  /\ stmt = [d \in Dispatchers |-> NoSnap]
  /\ wDlv = [d \in Dispatchers |-> NoSnap] /\ wEp = [d \in Dispatchers |-> NoSnap]
  /\ wOff = [d \in Dispatchers |-> 0] /\ wEnd = [d \in Dispatchers |-> 0]
  /\ wOld = [d \in Dispatchers |-> <<0, 0>>]
  /\ drain = [d \in Dispatchers |-> FALSE]
  /\ batch = [d \in Dispatchers |-> <<>>]
  /\ received = [s \in Sels |-> <<>>] /\ since = [s \in Sels |-> {}]
  /\ crashes = 0 /\ rewinds = 0

(* ------------------------------------------------------------------------ *)
(* Producers                                                                  *)

Begin(t) ==
  /\ txState[t] = "idle"
  /\ txState' = [txState EXCEPT ![t] = "open"]
  /\ UNCHANGED <<xid, nextXid, latest, written, pos, nextPos>>
  /\ UNCHANGED <<rows, work, ghost>>

\* A write before any outbox row assigns the id: the inbox's take, a permit.
TakeXid(t) ==
  /\ t \in EarlyXid
  /\ txState[t] = "open" /\ xid[t] = 0
  /\ xid' = [xid EXCEPT ![t] = nextXid] /\ nextXid' = nextXid + 1
  /\ UNCHANGED <<txState, latest, written, pos, nextPos>>
  /\ UNCHANGED <<rows, work, ghost>>

Predecessors(m) == {n \in Msgs : StreamOf[n] = StreamOf[m] /\ VersionOf[n] < VersionOf[m]}
Serialised(m) == \A n \in Predecessors(m) : Committed(n)

\* INSERT into the outbox: the id if none yet, the position drawn now.  Under
\* P1 the row of version v is written once v - 1 is committed; with DrawEarly
\* it is written first and the transaction waits for the version at commit.
Write(m) ==
  LET t == TxOf[m] IN
  /\ txState[t] = "open" /\ ~written[m]
  /\ DrawEarly \/ Serialised(m)
  /\ written' = [written EXCEPT ![m] = TRUE]
  /\ pos' = [pos EXCEPT ![m] = nextPos] /\ nextPos' = nextPos + 1
  /\ IF xid[t] = 0
     THEN xid' = [xid EXCEPT ![t] = nextXid] /\ nextXid' = nextXid + 1
     ELSE UNCHANGED <<xid, nextXid>>
  /\ UNCHANGED <<txState, latest>>
  /\ UNCHANGED <<rows, work, ghost>>

Completed(t) == IF xid[t] # 0 /\ xid[t] > latest THEN xid[t] ELSE latest

Commit(t) ==
  /\ txState[t] = "open"
  /\ \A m \in Msgs : TxOf[m] = t => written[m] /\ Serialised(m)
  /\ txState' = [txState EXCEPT ![t] = "committed"]
  /\ latest' = Completed(t)
  /\ UNCHANGED <<xid, nextXid, written, pos, nextPos>>
  /\ UNCHANGED <<rows, work, ghost>>

Abort(t) ==
  /\ t \in MayAbort
  /\ txState[t] = "open"
  /\ txState' = [txState EXCEPT ![t] = "aborted"]
  /\ latest' = Completed(t)
  /\ UNCHANGED <<xid, nextXid, written, pos, nextPos>>
  /\ UNCHANGED <<rows, work, ghost>>

(* ------------------------------------------------------------------------ *)
(* A dispatcher's transaction, statement by statement                         *)

\* The transaction ends: the lock goes, the working copy with it.
Release(d) ==
  /\ pc' = [pc EXCEPT ![d] = "idle"]
  /\ lock' = [s \in Sels |-> IF lock[s] = d THEN None ELSE lock[s]]
  /\ batch' = [batch EXCEPT ![d] = <<>>]
  /\ drain' = [drain EXCEPT ![d] = FALSE]

\* Only a design that reads under the lock's statement's snapshot keeps it.
KeepsStmt == OneStatement \/ Rule = "xmin"

\* The lock's statement begins: its snapshot is taken before the lock.
Snap(d) ==
  /\ KeepsStmt
  /\ pc[d] = "idle"
  /\ stmt' = [stmt EXCEPT ![d] = Now]
  /\ pc' = [pc EXCEPT ![d] = "snapped"]
  /\ UNCHANGED <<wDlv, wEp, wOff, wEnd, wOld, drain, batch>>
  /\ UNCHANGED <<pg, rows, ghost>>

\* ADR-0007: the batch in the lock's own statement, below its xmin, in
\* (transaction id, position) order.
OldBatch(d, s) ==
  Take(Sorted({m \in Msgs : /\ InSel(m, s) /\ Readable(m, stmt[d])
                              /\ XidOf(m) < stmt[d].xmin
                              /\ Before(old[s], Key(m))}, TRUE), BatchSize)

\* FOR UPDATE SKIP LOCKED: a row another holds is passed by; a row taken is
\* read as last committed.
Lock(d) ==
  LET s == SelOf[d] IN
  /\ pc[d] = IF KeepsStmt THEN "snapped" ELSE "idle"
  /\ IF lock[s] # None
     THEN /\ pc' = [pc EXCEPT ![d] = "idle"]
          /\ UNCHANGED <<lock, wDlv, wEp, wOff, wEnd, wOld, batch>>
     ELSE /\ lock' = [lock EXCEPT ![s] = d]
          /\ wDlv' = [wDlv EXCEPT ![d] = dlv[s]] /\ wEp' = [wEp EXCEPT ![d] = ep[s]]
          /\ wOff' = [wOff EXCEPT ![d] = off[s]] /\ wEnd' = [wEnd EXCEPT ![d] = endp[s]]
          /\ wOld' = [wOld EXCEPT ![d] = old[s]]
          /\ IF Rule = "epoch"
             THEN pc' = [pc EXCEPT ![d] = "locked"] /\ UNCHANGED batch
             ELSE /\ batch' = [batch EXCEPT ![d] = OldBatch(d, s)]
                  /\ pc' = [pc EXCEPT ![d] = "batched"]
  /\ UNCHANGED <<stmt, drain, dlv, ep, off, endp, old>>
  /\ UNCHANGED <<pg, ghost>>

\* Decision 3: new in the epoch -- or a refuted form of it.
IsNew(m, dd, e) ==
  LET x == XidOf(m) IN
  CASE Membership = "visible" -> Vis(x, e) /\ ~Vis(x, dd)
    [] Membership = "noxip"   -> Vis(x, e) /\ x >= dd.xmax
    [] Membership = "range"   -> x >= dd.xmax /\ x < e.xmax

\* The epoch's start, if none is stored, and the batch, each under a
\* snapshot taken after the lock -- or, with OneStatement, under the lock's
\* statement's own, older one.  An epoch with nothing of its selection is
\* delivered at once, or the row is left as it was (decision 5).
Read(d) ==
  LET s == SelOf[d]
      r == IF OneStatement THEN stmt[d] ELSE Now
      fresh == wEp[d] = NoSnap
      e == IF fresh THEN r ELSE wEp[d]
      dd == wDlv[d]
      mem == {m \in Msgs : InSel(m, s) /\ Readable(m, r) /\ IsNew(m, dd, e)}
      o == IF ~fresh THEN wOff[d]
           ELSE IF OffsetCarried THEN wOff[d]
           ELSE IF mem = {} THEN 0 ELSE Min({pos[m] : m \in mem}) - 1
      en == IF ~fresh THEN wEnd[d]
            ELSE IF mem = {} THEN 0 ELSE Max({pos[m] : m \in mem})
      cand == {m \in Msgs : /\ InSel(m, s) /\ Readable(m, r) /\ pos[m] > o
                            /\ (Bounded => pos[m] <= en)
                            /\ (Filtered => IsNew(m, dd, e))}
      b == Take(Sorted(cand, Order = "xid"), BatchSize)
  IN
  /\ pc[d] = "locked"
  /\ IF fresh /\ mem = {}
     THEN \/ /\ wEp' = [wEp EXCEPT ![d] = e]
             /\ drain' = [drain EXCEPT ![d] = TRUE]
             /\ batch' = [batch EXCEPT ![d] = <<>>]
             /\ pc' = [pc EXCEPT ![d] = "handed"]
             /\ UNCHANGED <<lock, wOff, wEnd>>
          \/ /\ Release(d)
             /\ UNCHANGED <<wEp, wOff, wEnd>>
     ELSE /\ wEp' = [wEp EXCEPT ![d] = e]
          /\ wOff' = [wOff EXCEPT ![d] = o] /\ wEnd' = [wEnd EXCEPT ![d] = en]
          /\ batch' = [batch EXCEPT ![d] = b]
          /\ drain' = [drain EXCEPT ![d] = Len(b) < BatchSize]
          /\ pc' = [pc EXCEPT ![d] = "batched"]
          /\ UNCHANGED lock
  /\ UNCHANGED <<stmt, wDlv, wOld>>
  /\ UNCHANGED <<pg, dlv, ep, off, endp, old, ghost>>

\* The subscriber takes the batch; what it took stays taken, whatever the
\* transaction does next.  A broker that is down takes nothing.
Hand(d) ==
  LET s == SelOf[d] IN
  /\ pc[d] = "batched"
  /\ ~(BrokerDown /\ \E i \in 1..Len(batch[d]) : LaneOf[batch[d][i]] = Broker)
  /\ received' = [received EXCEPT ![s] = @ \o batch[d]]
  /\ since' = [since EXCEPT ![s] = @ \cup {batch[d][i] : i \in 1..Len(batch[d])}]
  /\ pc' = [pc EXCEPT ![d] = "handed"]
  /\ UNCHANGED <<crashes, rewinds>>
  /\ UNCHANGED <<stmt, wDlv, wEp, wOff, wEnd, wOld, drain, batch>>
  /\ UNCHANGED <<pg, rows>>

\* COMMIT: the offset moves to the batch's last row; a short batch drains the
\* epoch -- delivered becomes the epoch, or the current snapshot (DrainTo).
Ack(d) ==
  LET s == SelOf[d]
      last == batch[d][Len(batch[d])]
  IN
  /\ pc[d] = "handed"
  /\ IF Rule = "xmin"
     THEN /\ old' = [old EXCEPT ![s] = IF batch[d] = <<>> THEN @ ELSE Key(last)]
          /\ UNCHANGED <<dlv, ep, off, endp>>
     ELSE /\ IF drain[d]
             THEN /\ dlv' = [dlv EXCEPT ![s] = IF DrainTo = "epoch" THEN wEp[d] ELSE Now]
                  /\ ep' = [ep EXCEPT ![s] = NoSnap]
                  /\ off' = [off EXCEPT ![s] = IF OffsetCarried /\ batch[d] # <<>> THEN pos[last]
                                                ELSE IF OffsetCarried THEN wOff[d] ELSE 0]
                  /\ endp' = [endp EXCEPT ![s] = 0]
             ELSE /\ ep' = [ep EXCEPT ![s] = wEp[d]]
                  /\ off' = [off EXCEPT ![s] = pos[last]]
                  /\ endp' = [endp EXCEPT ![s] = wEnd[d]]
                  /\ UNCHANGED dlv
          /\ UNCHANGED old
  /\ Release(d)
  /\ UNCHANGED <<stmt, wDlv, wEp, wOff, wEnd, wOld>>
  /\ UNCHANGED <<pg, ghost>>

\* The dispatcher dies, or its subscriber fails: ROLLBACK.
Crash(d) ==
  /\ pc[d] \in {"locked", "batched", "handed"}
  /\ crashes < MaxCrashes /\ crashes' = crashes + 1
  /\ Release(d)
  /\ UNCHANGED <<stmt, wDlv, wEp, wOff, wEnd, wOld>>
  /\ UNCHANGED <<pg, dlv, ep, off, endp, old, received, since, rewinds>>

(* ------------------------------------------------------------------------ *)
(* An operator                                                                *)

\* Decision 11: keep of delivered only what lies below F -- or 'F:F:'.
Below(sn, f) == [xmin |-> Min({sn.xmin, f}), xmax |-> Min({sn.xmax, f}),
                 xip |-> {x \in sn.xip : x < f}]

\* set_position back to transaction f, waiting for the row's lock.  What the
\* subscribers received at or above f is owed again.
Rewind(s, f) ==
  /\ Rule = "epoch" /\ rewinds < MaxRewinds /\ lock[s] = None
  /\ rewinds' = rewinds + 1
  /\ dlv' = [dlv EXCEPT ![s] = IF RewindForm = "intersect" THEN Below(@, f)
                                ELSE [xmin |-> f, xmax |-> f, xip |-> {}]]
  /\ IF RewindKeepsEpoch THEN UNCHANGED <<ep, off, endp>>
     ELSE /\ ep' = [ep EXCEPT ![s] = NoSnap]
          /\ off' = [off EXCEPT ![s] = 0] /\ endp' = [endp EXCEPT ![s] = 0]
  /\ since' = [since EXCEPT ![s] = {m \in @ : XidOf(m) < f}]
  /\ UNCHANGED <<old, lock, received, crashes>>
  /\ UNCHANGED <<pg, work>>

(* ------------------------------------------------------------------------ *)

Step(d) == Snap(d) \/ Lock(d) \/ Read(d) \/ Hand(d) \/ Ack(d)

Next ==
  \/ \E t \in Txs : Begin(t) \/ TakeXid(t) \/ Commit(t) \/ Abort(t)
  \/ \E m \in Msgs : Write(m)
  \/ \E d \in Dispatchers : Step(d) \/ Crash(d)
  \/ \E s \in Sels, f \in Ids : Rewind(s, f)

\* Every dispatcher keeps looking; no transaction need ever end.
Fairness == \A d \in Dispatchers : WF_vars(Step(d))

Spec == Init /\ [][Next]_vars /\ Fairness

(* ------------------------------------------------------------------------ *)
(* Properties                                                                 *)

Snapshot == [xmin : 0..(MaxXid + 1), xmax : 0..(MaxXid + 1), xip : SUBSET Ids]

TypeOK ==
  /\ txState \in [Txs -> {"idle", "open", "committed", "aborted"}]
  /\ xid \in [Txs -> Nat] /\ nextXid \in Nat /\ latest \in Nat
  /\ written \in [Msgs -> BOOLEAN] /\ pos \in [Msgs -> Nat] /\ nextPos \in Nat
  /\ dlv \in [Sels -> Snapshot] /\ ep \in [Sels -> Snapshot]
  /\ off \in [Sels -> Nat] /\ endp \in [Sels -> Nat] /\ old \in [Sels -> Nat \X Nat]
  /\ lock \in [Sels -> Dispatchers \cup {None}]
  /\ pc \in [Dispatchers -> {"idle", "snapped", "locked", "batched", "handed"}]
  /\ received \in [Sels -> Seq(Msgs)] /\ since \in [Sels -> SUBSET Msgs]

ReceivedSet(s) == {received[s][i] : i \in 1..Len(received[s])}

FirstIndex(s, m) ==
  CHOOSE i \in 1..Len(received[s]) :
    received[s][i] = m /\ \A j \in 1..(i - 1) : received[s][j] # m

Live(m) == written[m] /\ txState[TxOf[m]] # "aborted"

\* What the position says was handed on: visible in delivered, or an epoch's
\* row at or below its offset; under ADR-0007, at or below (transaction, offset).
Passed(s, m) ==
  IF Rule = "xmin" THEN ~Before(old[s], Key(m))
  ELSE \/ Vis(XidOf(m), dlv[s])
       \/ /\ ep[s] # NoSnap
          /\ Vis(XidOf(m), ep[s]) /\ ~Vis(XidOf(m), dlv[s]) /\ pos[m] <= off[s]

\* No row the position has passed was left unreceived: lost for good.
NoPassedOver ==
  \A s \in Sels, m \in Msgs : InSel(m, s) /\ Live(m) /\ Passed(s, m) => m \in ReceivedSet(s)

\* After a rewind, every row the position passes again was received again.
RewindReplays ==
  \A s \in Sels, m \in Msgs : InSel(m, s) /\ Live(m) /\ Passed(s, m) => m \in since[s]

\* A stream's versions are first received in their order (decision 8).
VersionOrder ==
  \A s \in Sels : \A a, b \in Msgs :
    (/\ InSel(a, s) /\ InSel(b, s)
     /\ StreamOf[a] = StreamOf[b] /\ VersionOf[a] < VersionOf[b]
     /\ b \in ReceivedSet(s))
    => a \in ReceivedSet(s) /\ FirstIndex(s, a) < FirstIndex(s, b)

\* A row is received twice only after a rollback or a rewind.
NoRedeliveryWithoutFailure ==
  crashes = 0 /\ rewinds = 0 => \A s \in Sels : Len(received[s]) = Cardinality(ReceivedSet(s))

\* An epoch in flight sees everything delivered.
EpochCoversDelivered ==
  \A s \in Sels : ep[s] # NoSnap => \A x \in Ids : Vis(x, dlv[s]) => Vis(x, ep[s])

\* Delivered only grows, but at a rewind.
DeliveredMonotone ==
  [][rewinds' = rewinds =>
       \A s \in Sels, x \in Ids : Vis(x, dlv[s]) => Vis(x, dlv'[s])]_vars

Up(m) == ~(BrokerDown /\ LaneOf[m] = Broker)

\* A committed row is received, whatever transaction stays open for ever,
\* unless its own lane's destination is down.
NoHeadOfLineBlocking ==
  \A s \in Sels, m \in Msgs :
    InSel(m, s) /\ Up(m) => (Committed(m) ~> m \in ReceivedSet(s))

=============================================================================
