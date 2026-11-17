-------------------------- MODULE MCInboxThree --------------------------
\* The rotation of ADR-0041 over three Tenants in one slot, for what two
\* cannot show: that each other Tenant's head is taken at most once before a
\* due head, and that the walk starts after the cursor rather than skipping
\* the cursor's Tenant alone.  A module of its own, so that the instances of
\* MCInbox.tla keep their constants.
EXTENDS MCInboxRotation

CONSTANT m4

\* m1 and m2 of Tenant 1, m3 of Tenant 2, m4 of Tenant 3; m2 depending on
\* m1, as in MCInbox.tla.
MCFourMsgs == {m1, m2, m3, m4}
MCFourDeps == MCDeps @@ (m4 :> {})
MCThreeTenants == (m1 :> 1) @@ (m2 :> 1) @@ (m3 :> 2) @@ (m4 :> 3)

\* A mutation for InboxWalkFromLowest.cfg: the walk goes by Tenant number
\* from the lowest and skips only the cursor's Tenant, as
\* "tenant_id <> last_tenant ORDER BY tenant_id" would, instead of starting
\* after the cursor and wrapping.  With two Tenants it is the rotation.  With
\* three, Tenant 1 comes first from every cursor but its own, and Tenant 3
\* only from Tenant 2's place.  TLC must find BoundedOvertaking violated: m1
\* taken, its dispatcher dead, a walk passing Tenant 2, which has no row,
\* and m1 taken again while m4 is due.  The cursor's Tenant lies past every
\* other, Tenant NT included.
MCWalkFromLowest(c, t) == IF t = c THEN NT + 1 ELSE t

\* Guests for InboxRotationGuest.cfg: Tenant 3's head stands for a paused
\* channel's, which a guest never takes; the others are due at every
\* process, and each other Tenant overtakes them at most twice.
MCSkipThree == {3}

=============================================================================
