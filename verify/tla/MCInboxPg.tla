---------------------------- MODULE MCInboxPg ----------------------------
\* The instances TLC checks.  As implemented: three messages, the second
\* depending on the first, two slots in every cut, two dispatchers, one
\* rollback.  For the lock cycle: two slots of two messages each, every head
\* depending on a message that never arrives, crosswise.
EXTENDS InboxPg, TLC

CONSTANTS m1, m2, m3, a1, a2, b1, b2, x, y, w1, w2, d1, d2

MCMsgs == {m1, m2, m3}
MCDeps == (m1 :> {}) @@ (m2 :> {m1}) @@ (m3 :> {})
MCSlots == {w1, w2}
MCPlacements == [MCMsgs -> MCSlots]
MCDispatchers == {d1, d2}

MCCycleMsgs == {a1, a2, b1, b2, x, y}
MCCycleDeps == (a1 :> {x}) @@ (a2 :> {y}) @@ (b1 :> {y}) @@ (b2 :> {x}) @@ (x :> {}) @@ (y :> {})
MCCycleArrives == {a1, a2, b1, b2}
MCCyclePlacements == {(a1 :> w1) @@ (a2 :> w1) @@ (b1 :> w2) @@ (b2 :> w2) @@ (x :> w1) @@ (y :> w2)}

=============================================================================
