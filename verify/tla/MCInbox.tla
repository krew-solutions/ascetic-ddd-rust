---------------------------- MODULE MCInbox ----------------------------
\* The instance TLC checks: three messages, the second depending on the
\* first; two slots, a dispatcher each.
EXTENDS Inbox, TLC

CONSTANTS m1, m2, m3, w1, w2

MCMsgs == {m1, m2, m3}
MCDeps == (m1 :> {}) @@ (m2 :> {m1}) @@ (m3 :> {})
MCSlots == {w1, w2}
MCArrivesAll == MCMsgs
MCArrivesWithoutM1 == {m2, m3}
MCNoPoison == {}

\* The instance for failures and parking: one slot, no dependencies, m1
\* poison.
MCNoDeps == (m1 :> {}) @@ (m2 :> {}) @@ (m3 :> {})
MCOneSlot == {w1}
MCPoisonM1 == {m1}

\* Rows the dispatcher cannot read: none, or m1, which the intake did not
\* foresee or did not write (ADR-0037 decision 6).
MCNoUnreadable == {}

\* Messages whose subscriber says no retry will succeed: none, in the
\* instances above; a recorded run names its own (TraceInbox.tla).
MCNoPermanent == {}
\* m1's verdict permanent beside the rotation, for InboxRotationPermanent.cfg.
MCPermanentM1 == {m1}
MCUnreadableM1 == {m1}

\* The park at once taken away, for InboxUnreadableHolds.cfg.
MCParkNever(d) == FALSE

(* ------------------------------------------------------------------------ *)
(* Tenants and the rotation of ADR-0041                                       *)

\* m1 and m2 of Tenant 1, m3 of Tenant 2; the slot serving them in turn.
MCTwoTenants == (m1 :> 1) @@ (m2 :> 1) @@ (m3 :> 2)
MCTrue == TRUE

\* A mutation for InboxNewestOfTenant.cfg: the walk takes the Tenant's
\* newest row instead of its oldest.  TLC must find ArrivalOrder violated.
MCNewestOfTenant(d, rows, t) ==
  LET S == {n \in CandidatesIn(d, rows) : TenantOf[n] = t}
  IN CHOOSE m \in S : \A n \in S : recvPos[m] >= recvPos[n]

\* A mutation for InboxCursorStays.cfg: the cursor is never written, so the
\* walk always starts from the same Tenant and becomes a priority by Tenant.
\* TLC must find BoundedOvertaking violated.
MCCursorStays(d, m) == cursor

\* The history variable that BoundedOvertaking reads lives in
\* MCInboxRotation.tla, so that the specifications of this module keep their
\* variables.

=============================================================================
