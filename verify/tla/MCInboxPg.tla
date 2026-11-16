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

(* ------------------------------------------------------------------------ *)
(* The rotation of ADR-0041, statement by statement                           *)

MCTrue == TRUE
MCFalse == FALSE
MCNoDeps == (m1 :> {}) @@ (m2 :> {}) @@ (m3 :> {})
MCOneSlot == {w1}
MCOnePlacement == {[m \in MCMsgs |-> w1]}
MCOneDispatcher == {d1}

\* A Tenant each, numbered so that a round beginning at Tenant 3 passes
\* Tenant 2 first, and a row of Tenant 1 arriving then lies behind the walk.
MCThreeTenants == (m1 :> 2) @@ (m2 :> 3) @@ (m3 :> 1)
\* m1 and m2 of Tenant 1, m3 of Tenant 2, as in MCInbox.tla.
MCTwoTenants == (m1 :> 1) @@ (m2 :> 1) @@ (m3 :> 2)

\* The cuts the rotation needs beside the dependency: m2's dependency in the
\* other slot, with both Tenants sharing m2's; and all three in one slot.
MCRotationPlacements == {(m1 :> w1) @@ (m2 :> w2) @@ (m3 :> w2), [m \in MCMsgs |-> w1]}

\* A walk that examines every Tenant of the instance: a round is one walk.
MCWideWalk == 3

\* For InboxPgRotationNoCap.cfg: max_dormancy never passes.
MCNoCap(s) == FALSE

\* A mutation for InboxPgSleepsPastBackoff.cfg: a dormancy that the backoffs
\* its round saw do not end, as a dormant_until set to the cap would be.
\* Without the cap TLC must find EventuallyProcessed violated.
MCSleepsPastBackoff(Q, s) ==
  /\ slept[s] # None
  /\ s \notin capped
  /\ \A m \in Q : pos[m] <= slept[s].after

\* A mutation for InboxPgNewestOfTenant.cfg: the walk takes its Tenant's
\* newest row.  TLC must find ArrivalOrder violated.
MCNewestIn(Q, t) ==
  LET S == {m \in Q : TenantOf[m] = t} IN CHOOSE m \in S : \A n \in S : pos[m] >= pos[n]

=============================================================================
