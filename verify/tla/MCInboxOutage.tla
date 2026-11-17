--------------------------- MODULE MCInboxOutage ---------------------------
\* The instances TLC checks for ADR-0042.  Rows m1 and m2 of Tenant t1
\* under one key, in slot s1; m3 of Tenant t2 under its own, in slot s2;
\* m4, in the instances that name it, in slot s2; x never arrives.  Two
\* processes, p1 with worker a1 and p2 with worker b1, unless an instance
\* gives p1 two.
EXTENDS InboxOutage, TLC

CONSTANTS m1, m2, m3, m4, x, s1, s2, t1, t2, p1, p2, a1, a2, b1

MCSlots == {s1, s2}
MCTenants == {t1, t2}
MCProcs == {p1, p2}

\* Three rows, no dependency.
MCMsgs3 == {m1, m2, m3}
MCOrder3 == <<m1, m2, m3>>
MCSlotOf3 == (m1 :> s1) @@ (m2 :> s1) @@ (m3 :> s2)
MCTenantOf3 == (m1 :> t1) @@ (m2 :> t1) @@ (m3 :> t2)
MCKeyOf3 == (m1 :> "k1") @@ (m2 :> "k1") @@ (m3 :> "k2")
MCNoDep3 == [m \in MCMsgs3 |-> None]

\* t1 in both slots, m1 in s1 and m4 in s2, so that two workers of one
\* process can ask it at once.
MCMsgsSpread == {m1, m4, m3}
MCOrderSpread == <<m1, m4, m3>>
MCSlotOfSpread == (m1 :> s1) @@ (m4 :> s2) @@ (m3 :> s2)
MCTenantOfSpread == (m1 :> t1) @@ (m4 :> t1) @@ (m3 :> t2)
MCKeyOfSpread == (m1 :> "k1") @@ (m4 :> "k4") @@ (m3 :> "k2")
MCNoDepSpread == [m \in MCMsgsSpread |-> None]

\* Dependencies: m3 waits for m2, queued behind m1; m4 for x, which never
\* arrives.
MCMsgsDeps == {m1, m2, m3, m4, x}
MCOrderDeps == <<m1, m2, m3, m4>>
MCMissingDeps == {x}
MCSlotOfDeps == (m1 :> s1) @@ (m2 :> s1) @@ (m3 :> s2) @@ (m4 :> s2) @@ (x :> s2)
MCTenantOfDeps == (m1 :> t1) @@ (m2 :> t1) @@ (m3 :> t2) @@ (m4 :> t2) @@ (x :> t2)
MCKeyOfDeps == (m1 :> "k1") @@ (m2 :> "k1") @@ (m3 :> "k2") @@ (m4 :> "k4") @@ (x :> "kx")
MCDeps == (m1 :> None) @@ (m2 :> None) @@ (m3 :> m2) @@ (m4 :> x) @@ (x :> None)

MCNone == {}
MCWorkers2 == {a1, b1}
MCProcOf2 == (a1 :> p1) @@ (b1 :> p2)
MCProcsOne == {p1}
MCWorkersTwoInP1 == {a1, a2}
MCProcOfTwoInP1 == (a1 :> p1) @@ (a2 :> p1)
MCCutP2 == {p2}
MCCachedNone == [p \in MCProcs |-> {}]
MCCachedT1InP1 == (p1 :> {t1}) @@ (p2 :> {})
MCT1 == {t1}
MCM1 == {m1}
MCM2 == {m2}
MCM3 == {m3}

=============================================================================
