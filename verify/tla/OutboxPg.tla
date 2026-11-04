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
(*   - the position sequence hands each session a block of `Cache` values; *)
(*     with a cache of one, values are drawn in real time.                 *)
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
(* The migration of decision 10: before it, the relays of ADR-0007 run on  *)
(* the old pair; it converts every position at once; after it, the relays *)
(* of ADR-0035 run, and old ones may go on in a rolling deploy until they  *)
(* retire, on the old pair the new relays keep up.                         *)
(*                                                                         *)
(* The ring of ADR-0033: rows live in K generations by their transaction's *)
(* window of R ids; a reclaimer empties a generation whole once its checks  *)
(* pass, statement by statement, and records two bounds.  A group may be   *)
(* created later, and rewinds and new groups keep above `intact_from`.     *)
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
  Sessions,         \* the pool's sessions
  SessionOf,        \* [Txs -> Sessions]
  Cache,            \* the position sequence's cache
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
  OldDispatchers,   \* SUBSET Dispatchers: binaries of ADR-0007, on the old pair
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
  Migration,        \* the positions start in ADR-0007's form and are converted
  Conversion,       \* "exact"; "delivered": 'L:L:' alone; "toofar": 'L+1:L+1:' alone
  OldPair,          \* "kept"; "frozen": not written; "shared": the epoch's offset in the old column
  RetireAtMigration,\* every old relay stops when the positions are converted
  Ring,             \* the outbox is ADR-0033's ring
  R,                \* ids a window holds
  K,                \* generations
  Margin,           \* windows a generation's newest must lie below the next id's
  OtherDb,          \* SUBSET Txs: transactions of another database
  LateSels,         \* SUBSET Sels: groups created later
  NewGroupForm,     \* "below": the current snapshot below intact_from; "naive": 'F:F:'
  RewindFloor,      \* a rewind below intact_from is refused
  ClosedCheck,      \* the reclaimer checks no open transaction of this database is in it
  PassedFirst,      \* "passed" read before "closed"
  Recheck,          \* "passed" read again under the partition's lock
  OneBound,         \* one bound for both meanings: intact_from is gone_below
  NamesEarly,       \* an epoch's generations named before its snapshot
  RecordConverted,  \* a converted epoch's generations: "next" look computes them, or "none"
  None              \* a model value

ASSUME BatchSize \in Nat \ {0}
ASSUME Cache \in Nat \ {0}
ASSUME Rule \in {"epoch", "xmin"}
ASSUME Membership \in {"visible", "noxip", "range"}
ASSUME Order \in {"position", "xid"}
ASSUME DrainTo \in {"epoch", "now"}
ASSUME RewindForm \in {"intersect", "naive"}
ASSUME Conversion \in {"exact", "delivered", "toofar"}
ASSUME OldPair \in {"kept", "frozen", "shared"}
ASSUME R \in Nat \ {0} /\ K \in Nat \ {0}
ASSUME NewGroupForm \in {"below", "naive"}
ASSUME RecordConverted \in {"next", "none"}

VARIABLES
  \* PostgreSQL
  txState,   \* [Txs -> {"idle", "open", "committed", "aborted"}]
  xid,       \* [Txs -> Nat], 0 until assigned
  nextXid,
  latest,    \* the latest completed id, 0 at first
  written,   \* [Msgs -> BOOLEAN]
  pos,       \* [Msgs -> Nat], the position drawn, 0 until then
  nextPos,   \* the sequence's next value
  cNext,     \* [Sessions -> Nat], a session's next cached value
  cEnd,      \* [Sessions -> Nat], its last cached value
  \* the position rows, as committed, and the schema
  dlv,       \* [Sels -> Snapshot], delivered
  ep,        \* [Sels -> Snapshot], the epoch, NoSnap when none
  off,       \* [Sels -> Nat], epoch_offset
  endp,      \* [Sels -> Nat], epoch_end
  old,       \* [Sels -> Nat \X Nat], ADR-0007's (transaction, offset)
  lock,      \* [Sels -> Dispatchers \cup {None}], FOR UPDATE on the row
  migrated,  \* the positions are in ADR-0035's form
  retired,   \* SUBSET OldDispatchers: stopped
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
  rewinds,
  \* the ring
  gone,      \* SUBSET Msgs: rows emptied with their generation
  goneBelow, \* no row is left, or will be written, below it
  intactFrom,\* every row written from it is kept
  exists,    \* [Sels -> BOOLEAN]: the group's position rows exist
  hist,      \* [Sels -> SUBSET Msgs]: rows a late group started past
  rpc,       \* the reclaimer: "idle", or how far its pass has got
  rg,        \* the generation it works on
  gens,      \* [Sels -> SUBSET Gens]: the epoch's generations, recorded
  gensSet,   \* [Sels -> BOOLEAN]: recorded
  wGens, wGensSet  \* the same, as the dispatcher's transaction holds them

pg == <<txState, xid, nextXid, latest, written, pos, nextPos, cNext, cEnd>>
rows == <<dlv, ep, off, endp, old, lock, migrated, retired>>
work == <<pc, stmt, wDlv, wEp, wOff, wEnd, wOld, drain, batch>>
ghost == <<received, since, crashes, rewinds>>
ringv == <<gone, goneBelow, intactFrom, exists, hist, rpc, rg>>
genv == <<gens, gensSet>>
wgenv == <<wGens, wGensSet>>
vars == <<pg, rows, work, ghost, ringv, genv, wgenv>>

NoSnap == [xmin |-> 0, xmax |-> 0, xip |-> {}]
\* Nothing delivered: '1:1:' ('0:0:' is not a valid snapshot).
Zero == [xmin |-> 1, xmax |-> 1, xip |-> {}]
Upto(l) == [xmin |-> l, xmax |-> l, xip |-> {}]  \* 'l:l:'
\* i64::MAX, past every position.
MaxPos == 1000

Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Max(S) == CHOOSE x \in S : \A y \in S : x >= y

MaxXid == Cardinality(Txs)
Ids == 1..MaxXid

\* A transaction's window, and the generation it falls in.
Win(x) == x \div R
Gen(x) == (x \div R) % K
Gens == 0..(K - 1)

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
Readable(m, s) == written[m] /\ m \notin gone /\ Committed(m) /\ Vis(XidOf(m), s)
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

\* With the old column shared, the epoch's offset lives in it.
OffOf(s) == IF Migration /\ OldPair = "shared" THEN old[s][2] ELSE off[s]

(* ------------------------------------------------------------------------ *)

Init ==
  /\ txState = [t \in Txs |-> "idle"]
  /\ xid = [t \in Txs |-> 0] /\ nextXid = 1 /\ latest = 0
  /\ written = [m \in Msgs |-> FALSE] /\ pos = [m \in Msgs |-> 0] /\ nextPos = 1
  /\ cNext = [x \in Sessions |-> 1] /\ cEnd = [x \in Sessions |-> 0]
  /\ dlv = [s \in Sels |-> Zero] /\ ep = [s \in Sels |-> NoSnap]
  /\ off = [s \in Sels |-> 0] /\ endp = [s \in Sels |-> 0]
  /\ old = [s \in Sels |-> <<0, 0>>]
  /\ lock = [s \in Sels |-> None]
  /\ migrated = ~Migration /\ retired = {}
  /\ pc = [d \in Dispatchers |-> "idle"]
  /\ stmt = [d \in Dispatchers |-> NoSnap]
  /\ wDlv = [d \in Dispatchers |-> NoSnap] /\ wEp = [d \in Dispatchers |-> NoSnap]
  /\ wOff = [d \in Dispatchers |-> 0] /\ wEnd = [d \in Dispatchers |-> 0]
  /\ wOld = [d \in Dispatchers |-> <<0, 0>>]
  /\ drain = [d \in Dispatchers |-> FALSE]
  /\ batch = [d \in Dispatchers |-> <<>>]
  /\ received = [s \in Sels |-> <<>>] /\ since = [s \in Sels |-> {}]
  /\ crashes = 0 /\ rewinds = 0
  /\ gone = {} /\ goneBelow = 1 /\ intactFrom = 1
  /\ exists = [s \in Sels |-> s \notin LateSels]
  /\ hist = [s \in Sels |-> {}]
  /\ rpc = "idle" /\ rg = 0
  /\ gens = [s \in Sels |-> {}] /\ gensSet = [s \in Sels |-> TRUE]
  /\ wGens = [d \in Dispatchers |-> {}] /\ wGensSet = [d \in Dispatchers |-> TRUE]

(* ------------------------------------------------------------------------ *)
(* Producers                                                                  *)

Begin(t) ==
  /\ txState[t] = "idle"
  /\ txState' = [txState EXCEPT ![t] = "open"]
  /\ UNCHANGED <<xid, nextXid, latest, written, pos, nextPos, cNext, cEnd>>
  /\ UNCHANGED <<rows, work, ghost, ringv, genv, wgenv>>

\* A write before any outbox row assigns the id: the inbox's take, a permit.
TakeXid(t) ==
  /\ t \in EarlyXid
  /\ txState[t] = "open" /\ xid[t] = 0
  /\ xid' = [xid EXCEPT ![t] = nextXid] /\ nextXid' = nextXid + 1
  /\ UNCHANGED <<txState, latest, written, pos, nextPos, cNext, cEnd>>
  /\ UNCHANGED <<rows, work, ghost, ringv, genv, wgenv>>

Predecessors(m) == {n \in Msgs : StreamOf[n] = StreamOf[m] /\ VersionOf[n] < VersionOf[m]}
Serialised(m) == \A n \in Predecessors(m) : Committed(n)

\* INSERT into the outbox: the id if none yet, the position drawn now from
\* the session's cache or a new block of the sequence.  Under P1 the row of
\* version v is written once v - 1 is committed; with DrawEarly it is
\* written first and the transaction waits for the version at commit.
Write(m) ==
  LET t == TxOf[m]
      x == SessionOf[t]
      cached == Cache > 1 /\ cNext[x] <= cEnd[x]
  IN
  /\ txState[t] = "open" /\ ~written[m]
  /\ DrawEarly \/ Serialised(m)
  /\ written' = [written EXCEPT ![m] = TRUE]
  /\ pos' = [pos EXCEPT ![m] = IF cached THEN cNext[x] ELSE nextPos]
  /\ IF Cache = 1
     THEN nextPos' = nextPos + 1 /\ UNCHANGED <<cNext, cEnd>>
     ELSE IF cached
     THEN cNext' = [cNext EXCEPT ![x] = @ + 1] /\ UNCHANGED <<nextPos, cEnd>>
     ELSE /\ nextPos' = nextPos + Cache
          /\ cNext' = [cNext EXCEPT ![x] = nextPos + 1]
          /\ cEnd' = [cEnd EXCEPT ![x] = nextPos + Cache - 1]
  /\ IF xid[t] = 0
     THEN xid' = [xid EXCEPT ![t] = nextXid] /\ nextXid' = nextXid + 1
     ELSE UNCHANGED <<xid, nextXid>>
  /\ UNCHANGED <<txState, latest>>
  /\ UNCHANGED <<rows, work, ghost, ringv, genv, wgenv>>

Completed(t) == IF xid[t] # 0 /\ xid[t] > latest THEN xid[t] ELSE latest

Commit(t) ==
  /\ txState[t] = "open"
  /\ \A m \in Msgs : TxOf[m] = t => written[m] /\ Serialised(m)
  /\ txState' = [txState EXCEPT ![t] = "committed"]
  /\ latest' = Completed(t)
  /\ UNCHANGED <<xid, nextXid, written, pos, nextPos, cNext, cEnd>>
  /\ UNCHANGED <<rows, work, ghost, ringv, genv, wgenv>>

Abort(t) ==
  /\ t \in MayAbort
  /\ txState[t] = "open"
  /\ txState' = [txState EXCEPT ![t] = "aborted"]
  /\ latest' = Completed(t)
  /\ UNCHANGED <<xid, nextXid, written, pos, nextPos, cNext, cEnd>>
  /\ UNCHANGED <<rows, work, ghost, ringv, genv, wgenv>>

(* ------------------------------------------------------------------------ *)
(* A dispatcher's transaction, statement by statement                         *)

IsOld(d) == d \in OldDispatchers
RuleOf(d) == IF IsOld(d) THEN "xmin" ELSE Rule
\* Only a design that reads under the lock's statement's snapshot keeps it.
KeepsStmt(d) == OneStatement \/ NamesEarly \/ RuleOf(d) = "xmin"
\* New relays wait for the migration; old ones run until they retire.
Active(d) == exists[SelOf[d]] /\ IF IsOld(d) THEN d \notin retired ELSE migrated

\* The transaction ends: the lock goes, the working copy with it.
Release(d) ==
  /\ pc' = [pc EXCEPT ![d] = "idle"]
  /\ lock' = [s \in Sels |-> IF lock[s] = d THEN None ELSE lock[s]]
  /\ batch' = [batch EXCEPT ![d] = <<>>]
  /\ drain' = [drain EXCEPT ![d] = FALSE]

\* The lock's statement begins: its snapshot is taken before the lock.
Snap(d) ==
  /\ Active(d) /\ KeepsStmt(d)
  /\ pc[d] = "idle"
  /\ stmt' = [stmt EXCEPT ![d] = Now]
  /\ pc' = [pc EXCEPT ![d] = "snapped"]
  /\ UNCHANGED <<wDlv, wEp, wOff, wEnd, wOld, drain, batch>>
  /\ UNCHANGED <<pg, rows, ghost, ringv, genv, wgenv>>

\* ADR-0007: the batch in the lock's own statement, below its xmin, in
\* (transaction id, position) order, past the old pair.
OldBatch(d, s) ==
  Take(Sorted({m \in Msgs : /\ InSel(m, s) /\ Readable(m, stmt[d])
                            /\ XidOf(m) < stmt[d].xmin
                            /\ Before(old[s], Key(m))}, TRUE), BatchSize)

\* FOR UPDATE SKIP LOCKED: a row another holds is passed by; a row taken is
\* read as last committed.
Lock(d) ==
  LET s == SelOf[d] IN
  /\ Active(d)
  /\ pc[d] = IF KeepsStmt(d) THEN "snapped" ELSE "idle"
  /\ IF lock[s] # None
     THEN /\ pc' = [pc EXCEPT ![d] = "idle"]
          /\ UNCHANGED <<lock, wDlv, wEp, wOff, wEnd, wOld, batch, wgenv>>
     ELSE /\ lock' = [lock EXCEPT ![s] = d]
          /\ wDlv' = [wDlv EXCEPT ![d] = dlv[s]] /\ wEp' = [wEp EXCEPT ![d] = ep[s]]
          /\ wOff' = [wOff EXCEPT ![d] = OffOf(s)] /\ wEnd' = [wEnd EXCEPT ![d] = endp[s]]
          /\ wOld' = [wOld EXCEPT ![d] = old[s]]
          /\ wGens' = [wGens EXCEPT ![d] = gens[s]]
          /\ wGensSet' = [wGensSet EXCEPT ![d] = gensSet[s]]
          /\ IF RuleOf(d) = "epoch"
             THEN pc' = [pc EXCEPT ![d] = "locked"] /\ UNCHANGED batch
             ELSE /\ batch' = [batch EXCEPT ![d] = OldBatch(d, s)]
                  /\ pc' = [pc EXCEPT ![d] = "batched"]
  /\ UNCHANGED <<stmt, drain, dlv, ep, off, endp, old, migrated, retired>>
  /\ UNCHANGED <<pg, ghost, ringv, genv>>

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
      \* ADR-0033: the generations an epoch's rows can lie in, from its two
      \* snapshots -- or from the lock's statement's, taken earlier.
      ne == IF NamesEarly THEN stmt[d] ELSE e
      names == {Gen(x) : x \in dd.xmax..(ne.xmax - 1)} \cup {Gen(x) : x \in {y \in dd.xip : Vis(y, ne)}}
      Named(m) == ~Ring \/ Gen(XidOf(m)) \in names
      mem == {m \in Msgs : InSel(m, s) /\ Readable(m, r) /\ IsNew(m, dd, e) /\ Named(m)}
      \* the generations recorded: at a start, those it found rows in; for a
      \* converted epoch with none recorded, its next look's, or none
      rec == IF fresh THEN {Gen(XidOf(m)) : m \in mem}
             ELSE IF wGensSet[d] THEN wGens[d]
             ELSE IF RecordConverted = "next"
                  THEN {Gen(XidOf(m)) : m \in {n \in Msgs : InSel(n, s) /\ Readable(n, r) /\ IsNew(n, dd, e)}}
                  ELSE {}
      o == IF ~fresh THEN wOff[d]
           ELSE IF OffsetCarried THEN wOff[d]
           ELSE IF mem = {} THEN 0 ELSE Min({pos[m] : m \in mem}) - 1
      en == IF ~fresh THEN wEnd[d]
            ELSE IF mem = {} THEN 0 ELSE Max({pos[m] : m \in mem})
      cand == {m \in Msgs : /\ InSel(m, s) /\ Readable(m, r) /\ pos[m] > o
                            /\ (Bounded => pos[m] <= en)
                            /\ (Filtered => IsNew(m, dd, e))
                            /\ (Ring => Gen(XidOf(m)) \in rec)}
      b == Take(Sorted(cand, Order = "xid"), BatchSize)
  IN
  /\ pc[d] = "locked"
  /\ IF fresh /\ mem = {}
     THEN \/ /\ wEp' = [wEp EXCEPT ![d] = e]
             /\ drain' = [drain EXCEPT ![d] = TRUE]
             /\ batch' = [batch EXCEPT ![d] = <<>>]
             /\ pc' = [pc EXCEPT ![d] = "handed"]
             /\ UNCHANGED <<lock, wOff, wEnd, wgenv>>
          \/ /\ Release(d)
             /\ UNCHANGED <<wEp, wOff, wEnd, wgenv>>
     ELSE /\ wEp' = [wEp EXCEPT ![d] = e]
          /\ wOff' = [wOff EXCEPT ![d] = o] /\ wEnd' = [wEnd EXCEPT ![d] = en]
          /\ batch' = [batch EXCEPT ![d] = b]
          /\ drain' = [drain EXCEPT ![d] = Len(b) < BatchSize]
          /\ pc' = [pc EXCEPT ![d] = "batched"]
          /\ wGens' = [wGens EXCEPT ![d] = rec] /\ wGensSet' = [wGensSet EXCEPT ![d] = TRUE]
          /\ UNCHANGED lock
  /\ UNCHANGED <<stmt, wDlv, wOld>>
  /\ UNCHANGED <<pg, dlv, ep, off, endp, old, migrated, retired, ghost, ringv, genv>>

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
  /\ UNCHANGED <<pg, rows, ringv, genv, wgenv>>

\* COMMIT.  An old relay moves the old pair to the batch's last row.  A new
\* one moves the offset to it; a short batch drains the epoch -- delivered
\* becomes the epoch, or the current snapshot (DrainTo) -- and, during a
\* migration, it keeps the old pair up as (xmin(delivered) - 1, i64::MAX).
Ack(d) ==
  LET s == SelOf[d]
      last == batch[d][Len(batch[d])]
      lastOff == IF batch[d] = <<>> THEN wOff[d] ELSE pos[last]
      newDlv == IF ~drain[d] THEN dlv[s]
                ELSE IF DrainTo = "epoch" THEN wEp[d] ELSE Now
      newOff == IF drain[d] /\ ~OffsetCarried THEN 0 ELSE lastOff
  IN
  /\ pc[d] = "handed"
  /\ IF RuleOf(d) = "xmin"
     THEN /\ old' = [old EXCEPT ![s] = IF batch[d] = <<>> THEN @ ELSE Key(last)]
          /\ UNCHANGED <<dlv, ep, off, endp>>
     ELSE /\ dlv' = [dlv EXCEPT ![s] = newDlv]
          /\ ep' = [ep EXCEPT ![s] = IF drain[d] THEN NoSnap ELSE wEp[d]]
          /\ endp' = [endp EXCEPT ![s] = IF drain[d] THEN 0 ELSE wEnd[d]]
          /\ IF Migration /\ OldPair = "shared"
             THEN /\ old' = [old EXCEPT ![s] = <<@[1], newOff>>]
                  /\ UNCHANGED off
             ELSE /\ off' = [off EXCEPT ![s] = newOff]
                  /\ old' = [old EXCEPT ![s] = IF Migration /\ OldPair = "kept"
                                                 THEN <<newDlv.xmin - 1, MaxPos>> ELSE @]
  /\ gens' = [gens EXCEPT ![s] = IF drain[d] THEN {} ELSE wGens[d]]
  /\ gensSet' = [gensSet EXCEPT ![s] = IF drain[d] THEN TRUE ELSE wGensSet[d]]
  /\ Release(d)
  /\ UNCHANGED <<stmt, wDlv, wEp, wOff, wEnd, wOld, migrated, retired>>
  /\ UNCHANGED <<pg, ghost, ringv, wgenv>>

\* The dispatcher dies, or its subscriber fails: ROLLBACK.
Crash(d) ==
  /\ pc[d] \in {"locked", "batched", "handed"}
  /\ crashes < MaxCrashes /\ crashes' = crashes + 1
  /\ Release(d)
  /\ UNCHANGED <<stmt, wDlv, wEp, wOff, wEnd, wOld>>
  /\ UNCHANGED <<pg, dlv, ep, off, endp, old, migrated, retired, received, since, rewinds>>
  /\ UNCHANGED <<ringv, genv, wgenv>>

(* ------------------------------------------------------------------------ *)
(* The migration of decision 10, and the old relays' end                      *)

RowsOf(s, l) == {m \in Msgs : InSel(m, s) /\ written[m] /\ XidOf(m) = l}

\* (L, off) becomes 'L:L:', the epoch 'L+1:L+1:' at offset off -- the rest of
\* transaction L -- or a refuted form; L = 0, nothing acknowledged, becomes
\* '1:1:'.  The epoch's end is the relay's to compute at its next look; here
\* it is computed at once, which gives the same value: L had ended.
Converted(s) ==
  LET l == old[s][1] IN
  CASE l = 0 -> [d |-> Zero, e |-> NoSnap, o |-> 0, n |-> 0]
    [] Conversion = "exact" ->
         [d |-> Upto(l), e |-> Upto(l + 1), o |-> old[s][2],
          n |-> Max({pos[m] : m \in RowsOf(s, l)} \cup {0})]
    [] Conversion = "delivered" -> [d |-> Upto(l), e |-> NoSnap, o |-> 0, n |-> 0]
    [] Conversion = "toofar" -> [d |-> Upto(l + 1), e |-> NoSnap, o |-> 0, n |-> 0]

\* The migration takes the position table's lock: no row is held.
Migrate ==
  /\ ~migrated
  /\ \A s \in Sels : lock[s] = None
  /\ migrated' = TRUE
  /\ dlv' = [s \in Sels |-> Converted(s).d]
  /\ ep' = [s \in Sels |-> Converted(s).e]
  /\ endp' = [s \in Sels |-> Converted(s).n]
  /\ IF OldPair = "shared" THEN UNCHANGED off ELSE off' = [s \in Sels |-> Converted(s).o]
  /\ IF RetireAtMigration
     THEN /\ retired' = OldDispatchers
          /\ pc' = [d \in Dispatchers |-> IF IsOld(d) THEN "idle" ELSE pc[d]]
     ELSE UNCHANGED <<retired, pc>>
  /\ gens' = [s \in Sels |-> {}]
  /\ gensSet' = [s \in Sels |-> Converted(s).e = NoSnap \/ RecordConverted = "none"]
  /\ UNCHANGED <<old, lock>>
  /\ UNCHANGED <<stmt, wDlv, wEp, wOff, wEnd, wOld, drain, batch>>
  /\ UNCHANGED <<pg, ghost, ringv, wgenv>>

\* An old relay stops, between its transactions.
Retire(d) ==
  /\ IsOld(d) /\ migrated /\ d \notin retired /\ pc[d] = "idle"
  /\ retired' = retired \cup {d}
  /\ UNCHANGED <<dlv, ep, off, endp, old, lock, migrated>>
  /\ UNCHANGED <<pg, work, ghost, ringv, genv, wgenv>>

(* ------------------------------------------------------------------------ *)
(* An operator                                                                *)

\* Decision 11: keep of delivered only what lies below F -- or 'F:F:'.
Below(sn, f) == [xmin |-> Min({sn.xmin, f}), xmax |-> Min({sn.xmax, f}),
                 xip |-> {x \in sn.xip : x < f}]

\* set_position back to transaction f, waiting for the row's lock.  What the
\* subscribers received at or above f is owed again.
Rewind(s, f) ==
  /\ Rule = "epoch" /\ migrated /\ rewinds < MaxRewinds /\ lock[s] = None
  /\ exists[s]
  \* the reclaimer's key, exclusively; never below intact_from
  /\ Ring => rpc = "idle" /\ (RewindFloor => f >= intactFrom)
  /\ rewinds' = rewinds + 1
  /\ dlv' = [dlv EXCEPT ![s] = IF RewindForm = "intersect" THEN Below(@, f) ELSE Upto(f)]
  /\ IF RewindKeepsEpoch THEN UNCHANGED <<ep, off, endp>>
     ELSE /\ ep' = [ep EXCEPT ![s] = NoSnap]
          /\ off' = [off EXCEPT ![s] = 0] /\ endp' = [endp EXCEPT ![s] = 0]
  /\ since' = [since EXCEPT ![s] = {m \in @ : XidOf(m) < f}]
  /\ gens' = [gens EXCEPT ![s] = {}] /\ gensSet' = [gensSet EXCEPT ![s] = TRUE]
  /\ UNCHANGED <<old, lock, migrated, retired, received, crashes>>
  /\ UNCHANGED <<pg, work, ringv, wgenv>>

(* ------------------------------------------------------------------------ *)
(* The ring of ADR-0033: a group created later, and the reclaimer             *)

\* A group's position rows are created under the reclaimer's key, shared:
\* the current snapshot kept below intact_from -- or 'F:F:'.  What its
\* position passes then is history it does not receive.
NewGroup(s) ==
  LET nd == IF NewGroupForm = "below" THEN Below(Now, intactFrom) ELSE Upto(intactFrom) IN
  /\ Ring /\ migrated /\ s \in LateSels /\ ~exists[s] /\ rpc = "idle"
  /\ exists' = [exists EXCEPT ![s] = TRUE]
  /\ dlv' = [dlv EXCEPT ![s] = nd]
  /\ hist' = [hist EXCEPT ![s] = {m \in Msgs : written[m] /\ Committed(m) /\ Vis(XidOf(m), nd)}]
  /\ UNCHANGED <<ep, off, endp, old, lock, migrated, retired>>
  /\ UNCHANGED <<gone, goneBelow, intactFrom, rpc, rg>>
  /\ UNCHANGED <<pg, work, ghost, genv, wgenv>>

RowsIn(g) == {m \in Msgs : written[m] /\ m \notin gone /\ Gen(XidOf(m)) = g}
OpenHere == {xid[t] : t \in {u \in Txs \ OtherDb : txState[u] = "open" /\ xid[u] # 0}}

\* Past: the generation's newest window lies Margin windows below the next id's.
Past(g) ==
  LET ws == {Win(XidOf(m)) : m \in RowsIn(g)} IN
  ws # {} /\ Win(nextXid) - Max(ws) >= Margin

\* Closed: no transaction of this database holds an id in it.
Closed(g) == ~ClosedCheck \/ \A x \in OpenHere : Gen(x) # g

\* Passed, under the statement's snapshot p: every row of it that p can read
\* is visible in every group's delivered.
PassedAt(g, p) ==
  \A s \in Sels : exists[s] =>
    \A m \in RowsIn(g) : Readable(m, p) => Vis(XidOf(m), dlv[s])

RecIdle == rpc' = "idle" /\ UNCHANGED rg

\* The candidate: the generation of the oldest row left -- the one holding
\* gone_below when no transaction holds it lower -- if past.
RBegin ==
  LET left == {XidOf(m) : m \in {n \in Msgs : written[n] /\ n \notin gone}}
      g == Gen(Min(left))
  IN
  /\ Ring /\ migrated /\ rpc = "idle" /\ left # {} /\ Past(g)
  /\ rpc' = "begun" /\ rg' = g
  /\ UNCHANGED <<gone, goneBelow, intactFrom, exists, hist>>
  /\ UNCHANGED <<pg, rows, work, ghost, genv, wgenv>>

\* Each check its own statement, in the decided order or the refuted one.
RFirst ==
  /\ rpc = "begun"
  /\ IF (IF PassedFirst THEN PassedAt(rg, Now) ELSE Closed(rg))
     THEN rpc' = "first" /\ UNCHANGED rg ELSE RecIdle
  /\ UNCHANGED <<gone, goneBelow, intactFrom, exists, hist>>
  /\ UNCHANGED <<pg, rows, work, ghost, genv, wgenv>>

RSecond ==
  /\ rpc = "first"
  /\ IF (IF PassedFirst THEN Closed(rg) ELSE PassedAt(rg, Now))
     THEN rpc' = "checked" /\ UNCHANGED rg ELSE RecIdle
  /\ UNCHANGED <<gone, goneBelow, intactFrom, exists, hist>>
  /\ UNCHANGED <<pg, rows, work, ghost, genv, wgenv>>

\* The partition's lock waits for every open writer of a row in it -- or the
\* pass gives up (lock_timeout).  Under it, "passed" is read again, then
\* TRUNCATE, and both bounds are set.
RTruncate ==
  LET g == rg
      trunc == RowsIn(g)
      rest == {XidOf(m) : m \in {n \in Msgs : written[n] /\ n \notin gone \cup trunc}}
      gb == Min(rest \cup OpenHere \cup {nextXid})
      top == Max({XidOf(m) : m \in trunc} \cup {0})
  IN
  /\ rpc = "checked"
  /\ \/ /\ \A m \in trunc : txState[TxOf[m]] # "open"
        /\ IF Recheck /\ ~PassedAt(g, Now)
           THEN RecIdle /\ UNCHANGED <<gone, goneBelow, intactFrom>>
           ELSE /\ gone' = gone \cup trunc
                /\ goneBelow' = gb
                /\ intactFrom' = IF OneBound THEN gb ELSE Max({intactFrom, top + 1})
                /\ RecIdle
     \/ RecIdle /\ UNCHANGED <<gone, goneBelow, intactFrom>>
  /\ UNCHANGED <<exists, hist>>
  /\ UNCHANGED <<pg, rows, work, ghost, genv, wgenv>>

(* ------------------------------------------------------------------------ *)

Step(d) == Snap(d) \/ Lock(d) \/ Read(d) \/ Hand(d) \/ Ack(d)

Next ==
  \/ \E t \in Txs : Begin(t) \/ TakeXid(t) \/ Commit(t) \/ Abort(t)
  \/ \E m \in Msgs : Write(m)
  \/ \E d \in Dispatchers : Step(d) \/ Crash(d) \/ Retire(d)
  \/ Migrate
  \/ \E s \in Sels, f \in Ids : Rewind(s, f)
  \/ \E s \in Sels : NewGroup(s)
  \/ RBegin \/ RFirst \/ RSecond \/ RTruncate

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
  /\ migrated \in BOOLEAN /\ retired \subseteq OldDispatchers
  /\ pc \in [Dispatchers -> {"idle", "snapped", "locked", "batched", "handed"}]
  /\ received \in [Sels -> Seq(Msgs)] /\ since \in [Sels -> SUBSET Msgs]

ReceivedSet(s) == {received[s][i] : i \in 1..Len(received[s])}

FirstIndex(s, m) ==
  CHOOSE i \in 1..Len(received[s]) :
    received[s][i] = m /\ \A j \in 1..(i - 1) : received[s][j] # m

Live(m) == written[m] /\ txState[TxOf[m]] # "aborted"

\* What a position says was handed on.  ADR-0035's: visible in delivered,
\* or an epoch's row at or below its offset.  ADR-0007's: at or below the
\* old pair.
PassedNew(s, m) ==
  \/ Vis(XidOf(m), dlv[s])
  \/ /\ ep[s] # NoSnap
     /\ Vis(XidOf(m), ep[s]) /\ ~Vis(XidOf(m), dlv[s]) /\ pos[m] <= OffOf(s)
PassedOld(s, m) == ~Before(old[s], Key(m))
Passed(s, m) == IF Rule = "epoch" /\ migrated THEN PassedNew(s, m) ELSE PassedOld(s, m)

\* No row the position has passed was left unreceived: lost for good.
NoPassedOver ==
  \A s \in Sels, m \in Msgs :
    exists[s] /\ InSel(m, s) /\ Live(m) /\ Passed(s, m) /\ m \notin hist[s] => m \in ReceivedSet(s)

\* A row emptied with its generation was passed by every group's position:
\* one it was not can never be received.
NoPositionBelowGone ==
  \A s \in Sels, m \in gone : exists[s] /\ InSel(m, s) /\ Committed(m) => Passed(s, m)

\* intact_from: every row written from it is kept.  gone_below: no row is
\* left below it.
IntactFromSound == \A m \in gone : XidOf(m) < intactFrom
GoneBelowSound == \A m \in Msgs : written[m] /\ m \notin gone => XidOf(m) >= goneBelow

\* During a migration the old pair, which old relays still read, passes
\* nothing unreceived either.
OldPairSound ==
  \A s \in Sels, m \in Msgs : InSel(m, s) /\ Live(m) /\ PassedOld(s, m) => m \in ReceivedSet(s)

\* After a rewind, every row the position passes again was received again.
RewindReplays ==
  \A s \in Sels, m \in Msgs :
    exists[s] /\ InSel(m, s) /\ Live(m) /\ Passed(s, m) /\ m \notin hist[s] => m \in since[s]

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
