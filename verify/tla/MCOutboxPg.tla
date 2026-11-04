---------------------------- MODULE MCOutboxPg ----------------------------
\* The instances TLC checks.  Three rows: two versions of one stream, written
\* by two transactions, to the inbox; one row of another stream, to the
\* broker, by a third.  A fourth transaction holds an id and writes nothing
\* here: one of another database.  Either one selection of everything, raced
\* by two dispatchers, or two lanes with a dispatcher each.
\*
\* For the migration: one transaction writes two rows, of two streams, so
\* that an old relay can acknowledge it halfway; a second writes the next
\* version of the first stream.  One new relay, one old.
\*
\* For the sequence's cache: two sessions, the broker's writer and the
\* later version's writer sharing one.
EXTENDS OutboxPg, TLC

CONSTANTS t1, t2, t3, o1, m1, m2, m3, sA, sB, inbox, broker, all, si, sk, d1, d2,
          ses1, ses2

MCTxs == {t1, t2, t3, o1}
\* The writer of the later version, and the other database's transaction,
\* may hold an id before writing here; the broker's writer may roll back.
MCEarly == {t2, o1}
MCMayAbort == {t3}
\* Where neither matters: no early id, no rollback.
MCNoTx == {}
MCMsgs == {m1, m2, m3}
MCTxOf == (m1 :> t1) @@ (m2 :> t2) @@ (m3 :> t3)
MCStreamOf == (m1 :> sA) @@ (m2 :> sA) @@ (m3 :> sB)
MCVersionOf == (m1 :> 1) @@ (m2 :> 2) @@ (m3 :> 1)
MCDestOf == (m1 :> inbox) @@ (m2 :> inbox) @@ (m3 :> broker)

MCAll == {all}
MCAllLanes == (all :> {inbox, broker})
MCRacing == {d1, d2}
MCRacingOf == (d1 :> all) @@ (d2 :> all)
MCAlone == {d1}
MCAloneOf == (d1 :> all)
MCNoDisp == {}

MCLanes == {si, sk}
MCLaneLanes == (si :> {inbox}) @@ (sk :> {broker})
MCLaneOf == (d1 :> si) @@ (d2 :> sk)

\* One session, for every configuration but the cache's.
MCOneSession == {ses1}
MCOneSessionOf == [t \in MCTxs |-> ses1]
MCTwoSessions == {ses1, ses2}
MCTwoSessionsOf == (t1 :> ses2) @@ (t2 :> ses1) @@ (t3 :> ses1) @@ (o1 :> ses1)

\* The migration's instance.
MCMigTxs == {t1, t2, o1}
MCMigSessionOf == [t \in MCMigTxs |-> ses1]
MCMigTxOf == (m1 :> t1) @@ (m2 :> t1) @@ (m3 :> t2)
MCMigStreamOf == (m1 :> sA) @@ (m2 :> sB) @@ (m3 :> sA)
MCMigVersionOf == (m1 :> 1) @@ (m2 :> 1) @@ (m3 :> 2)
MCMigDestOf == (m1 :> inbox) @@ (m2 :> inbox) @@ (m3 :> inbox)
MCOld == {d2}
\* The next version's writer holding its id before the first transaction's:
\* xid order and position order then differ.
MCMigEarly == {t2}

=============================================================================
