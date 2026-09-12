---------------------------- MODULE Outbox ----------------------------
(***************************************************************************)
(* The Transactional Outbox of `crates/outbox`, as a protocol.             *)
(*                                                                         *)
(* Producers publish messages inside transactions.  A dispatcher per       *)
(* (group, worker) fetches the committed messages of its partition in      *)
(* (transaction id, position) order, hands each one to a subscriber, and   *)
(* acknowledges the last of the batch.  Dispatchers crash at any point,    *)
(* and a subscriber may fail, which rolls the batch back the same way.     *)
(*                                                                         *)
(* PostgreSQL is abstracted to what the protocol relies on:                *)
(*   - a transaction id (xid8) is assigned at the first write, increasing; *)
(*   - transactions commit in any order, independent of their ids;         *)
(*   - a row is visible once its transaction has committed;                *)
(*   - pg_snapshot_xmin is the smallest id still open, or the next id;     *)
(*   - the position row is locked per dispatcher: one batch at a time.     *)
(*                                                                         *)
(* A real snapshot is taken at the start of the fetch, so a transaction    *)
(* committing during the fetch is not seen.  The abstraction "visible iff  *)
(* committed now" is sound under the visibility rule, because such a       *)
(* transaction was open at snapshot time and so has an id >= xmin, which   *)
(* the rule excludes anyway.                                               *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
  Msgs,           \* messages, one per producing transaction
  Uris,           \* partition keys: the URI a message is addressed to
  Groups,         \* consumer groups
  Workers,        \* the workers every group is spread over
  BatchSize,
  MaxCrashes,     \* crashes and subscriber failures, in all
  VisibilityRule  \* TRUE: `transaction_id < pg_snapshot_xmin(pg_current_snapshot())`

ASSUME BatchSize \in Nat \ {0}
ASSUME MaxCrashes \in Nat

NoXid == 0

VARIABLES
  uriOf,      \* [Msgs -> Uris], fixed at the start: any addressing is allowed
  assign,     \* [Uris -> Workers], fixed: `hashtext(uri) % n`, made explicit
  txState,    \* [Msgs -> {"idle", "open", "committed", "aborted"}]
  xid,        \* [Msgs -> Nat], NoXid until the write
  pos,        \* [Msgs -> Nat], the serial, NoXid until the write
  nextXid,
  nextPos,
  position,   \* [Dispatchers -> Nat \X Nat], the last acknowledged (xid, pos)
  batch,      \* [Dispatchers -> Seq(Msgs)], fetched and not yet acknowledged
  done,       \* [Dispatchers -> Nat], how many of the batch reached the subscriber
  delivered,  \* [Groups -> Seq(Msgs)], what the group's subscribers received, in order, duplicates included
  crashes

vars == <<uriOf, assign, txState, xid, pos, nextXid, nextPos, position, batch, done, delivered, crashes>>

Dispatchers == Groups \X Workers

(* ------------------------------------------------------------------------ *)
(* PostgreSQL                                                                 *)

Active == {xid[m] : m \in {n \in Msgs : txState[n] = "open" /\ xid[n] # NoXid}}

Xmin == IF Active = {} THEN nextXid ELSE CHOOSE x \in Active : \A y \in Active : x <= y

Key(m) == <<xid[m], pos[m]>>

Before(a, b) == a[1] < b[1] \/ (a[1] = b[1] /\ a[2] < b[2])

(* ------------------------------------------------------------------------ *)

Init ==
  /\ uriOf \in [Msgs -> Uris]
  /\ assign \in [Uris -> Workers]
  /\ txState = [m \in Msgs |-> "idle"]
  /\ xid = [m \in Msgs |-> NoXid]
  /\ pos = [m \in Msgs |-> NoXid]
  /\ nextXid = 1
  /\ nextPos = 1
  /\ position = [d \in Dispatchers |-> <<0, 0>>]
  /\ batch = [d \in Dispatchers |-> <<>>]
  /\ done = [d \in Dispatchers |-> 0]
  /\ delivered = [g \in Groups |-> <<>>]
  /\ crashes = 0

(* ------------------------------------------------------------------------ *)
(* Producers                                                                  *)

Begin(m) ==
  /\ txState[m] = "idle"
  /\ txState' = [txState EXCEPT ![m] = "open"]
  /\ UNCHANGED <<uriOf, assign, xid, pos, nextXid, nextPos, position, batch, done, delivered, crashes>>

\* INSERT ... transaction_id = pg_current_xact_id(): the id is assigned at the write.
Publish(m) ==
  /\ txState[m] = "open"
  /\ xid[m] = NoXid
  /\ xid' = [xid EXCEPT ![m] = nextXid]
  /\ nextXid' = nextXid + 1
  /\ pos' = [pos EXCEPT ![m] = nextPos]
  /\ nextPos' = nextPos + 1
  /\ UNCHANGED <<uriOf, assign, txState, position, batch, done, delivered, crashes>>

Commit(m) ==
  /\ txState[m] = "open"
  /\ xid[m] # NoXid
  /\ txState' = [txState EXCEPT ![m] = "committed"]
  /\ UNCHANGED <<uriOf, assign, xid, pos, nextXid, nextPos, position, batch, done, delivered, crashes>>

Abort(m) ==
  /\ txState[m] = "open"
  /\ txState' = [txState EXCEPT ![m] = "aborted"]
  /\ UNCHANGED <<uriOf, assign, xid, pos, nextXid, nextPos, position, batch, done, delivered, crashes>>

(* ------------------------------------------------------------------------ *)
(* Dispatchers                                                                *)

Visible(m) ==
  /\ txState[m] = "committed"
  /\ (VisibilityRule => xid[m] < Xmin)

Eligible(d) ==
  {m \in Msgs : /\ Visible(m)
                /\ assign[uriOf[m]] = d[2]
                /\ Before(position[d], Key(m))}

RECURSIVE Sorted(_)
Sorted(S) ==
  IF S = {} THEN <<>>
  ELSE LET first == CHOOSE m \in S : \A n \in S : m = n \/ Before(Key(m), Key(n))
       IN <<first>> \o Sorted(S \ {first})

Take(seq, n) == SubSeq(seq, 1, IF Len(seq) < n THEN Len(seq) ELSE n)

\* SELECT ... ORDER BY transaction_id, position LIMIT BatchSize FOR UPDATE on the position row.
Fetch(d) ==
  /\ batch[d] = <<>>
  /\ Eligible(d) # {}
  /\ batch' = [batch EXCEPT ![d] = Take(Sorted(Eligible(d)), BatchSize)]
  /\ done' = [done EXCEPT ![d] = 0]
  /\ UNCHANGED <<uriOf, assign, txState, xid, pos, nextXid, nextPos, position, delivered, crashes>>

\* The subscriber receives the next message of the batch.  Its effect is
\* outside the database and is not rolled back with the batch.
Handle(d) ==
  /\ done[d] < Len(batch[d])
  /\ delivered' = [delivered EXCEPT ![d[1]] = Append(@, batch[d][done[d] + 1])]
  /\ done' = [done EXCEPT ![d] = @ + 1]
  /\ UNCHANGED <<uriOf, assign, txState, xid, pos, nextXid, nextPos, position, batch, crashes>>

\* Every message of the batch was handled: the position moves to the last one, COMMIT.
Ack(d) ==
  /\ batch[d] # <<>>
  /\ done[d] = Len(batch[d])
  /\ position' = [position EXCEPT ![d] = Key(batch[d][Len(batch[d])])]
  /\ batch' = [batch EXCEPT ![d] = <<>>]
  /\ done' = [done EXCEPT ![d] = 0]
  /\ UNCHANGED <<uriOf, assign, txState, xid, pos, nextXid, nextPos, delivered, crashes>>

\* The dispatcher dies, or a subscriber fails: the transaction rolls back and
\* the position stays where it was.  What subscribers already received stays
\* received.
Crash(d) ==
  /\ batch[d] # <<>>
  /\ crashes < MaxCrashes
  /\ crashes' = crashes + 1
  /\ batch' = [batch EXCEPT ![d] = <<>>]
  /\ done' = [done EXCEPT ![d] = 0]
  /\ UNCHANGED <<uriOf, assign, txState, xid, pos, nextXid, nextPos, position, delivered>>

(* ------------------------------------------------------------------------ *)

Next ==
  \/ \E m \in Msgs : Begin(m) \/ Publish(m) \/ Commit(m) \/ Abort(m)
  \/ \E d \in Dispatchers : Fetch(d) \/ Handle(d) \/ Ack(d) \/ Crash(d)

\* Every transaction ends, and every dispatcher keeps polling.  A transaction
\* that never ends stalls every later message of every group: drop the first
\* conjunct and EventuallyDelivered fails.
Fairness ==
  /\ \A m \in Msgs : WF_vars(Commit(m) \/ Abort(m))
  /\ \A d \in Dispatchers : WF_vars(Fetch(d)) /\ WF_vars(Handle(d)) /\ WF_vars(Ack(d))

Spec == Init /\ [][Next]_vars /\ Fairness

(* ------------------------------------------------------------------------ *)
(* Properties                                                                 *)

TypeOK ==
  /\ uriOf \in [Msgs -> Uris]
  /\ assign \in [Uris -> Workers]
  /\ txState \in [Msgs -> {"idle", "open", "committed", "aborted"}]
  /\ xid \in [Msgs -> Nat]
  /\ pos \in [Msgs -> Nat]
  /\ nextXid \in Nat
  /\ nextPos \in Nat
  /\ position \in [Dispatchers -> Nat \X Nat]
  /\ batch \in [Dispatchers -> Seq(Msgs)]
  /\ done \in [Dispatchers -> Nat]
  /\ delivered \in [Groups -> Seq(Msgs)]
  /\ crashes \in Nat

Received(g, m) == \E i \in 1..Len(delivered[g]) : delivered[g][i] = m

\* Only committed messages reach a subscriber.
DeliveredCommitted ==
  \A g \in Groups : \A i \in 1..Len(delivered[g]) : txState[delivered[g][i]] = "committed"

\* The position of a dispatcher never passes a live message of its partition
\* that no subscriber of the group has received.  Without the visibility rule
\* this fails: a message whose transaction took an earlier id but commits
\* later is passed over, and lost for good.
NoPassedOver ==
  \A d \in Dispatchers : \A m \in Msgs :
    (/\ xid[m] # NoXid
     /\ txState[m] \in {"open", "committed"}
     /\ assign[uriOf[m]] = d[2]
     /\ Before(Key(m), position[d]))
    => Received(d[1], m)

FirstIndex(g, m) ==
  CHOOSE i \in 1..Len(delivered[g]) :
    /\ delivered[g][i] = m
    /\ \A j \in 1..(i - 1) : delivered[g][j] # m

\* Messages of one URI reach a group's subscribers in (xid, pos) order, the
\* first time each is received.  Redeliveries after a crash repeat a prefix
\* and do not disturb it.
OrderPerUri ==
  \A g \in Groups : \A a, b \in Msgs :
    (/\ Received(g, a)
     /\ Received(g, b)
     /\ uriOf[a] = uriOf[b]
     /\ Before(Key(a), Key(b)))
    => FirstIndex(g, a) < FirstIndex(g, b)

\* Every committed message is eventually received by every group: at least once.
EventuallyDelivered ==
  \A g \in Groups : \A m \in Msgs : (txState[m] = "committed") ~> Received(g, m)

=============================================================================
