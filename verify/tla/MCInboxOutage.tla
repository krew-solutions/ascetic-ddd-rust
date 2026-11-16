--------------------------- MODULE MCInboxOutage ---------------------------
\* The instances TLC checks for ADR-0040: three rows, two slots; m1 and m2
\* of Tenant t1 under one key, so in one slot, m3 of t2 under its own.
EXTENDS InboxOutage, TLC

CONSTANTS m1, m2, m3, w1, w2, t1, t2

MCMsgs == {m1, m2, m3}
MCOrder == <<m1, m2, m3>>
MCSlots == {w1, w2}
MCTenants == {t1, t2}
MCTenantOf == (m1 :> t1) @@ (m2 :> t1) @@ (m3 :> t2)
MCKeyOf == (m1 :> "k1") @@ (m2 :> "k1") @@ (m3 :> "k2")
MCNoTenants == {}
MCTenantT1 == {t1}
MCNoPermanent == {}
MCPermanentM3 == {m3}

\* A hold that spends an attempt, as the inbox did before ADR-0040: a
\* stage's verdict counted as any failure, parked at MaxAttempts.
MCDeferCounting(d) ==
  \E m \in Msgs :
    /\ Asked(d, m)
    /\ m \notin Permanent
    /\ Down(m)
    /\ Spend(m, "stage", FALSE)
    /\ UNCHANGED <<part, processed, procOrder, byHand, served, stageDown, outages, tenantDown,
                  faulted, rearmed, admin>>

\* Exits that forget to clear the cause.
MCCommitKeepingCause(d) ==
  \E m \in Msgs :
    /\ Asked(d, m)
    /\ m \notin Permanent
    /\ Serves(m)
    /\ processed' = processed \cup {m}
    /\ procOrder' = Append(procOrder, m)
    /\ held' = [held EXCEPT ![m] = 0]
    /\ served' = [n \in Msgs |-> served[n] \/ held[n] > 0]
    /\ UNCHANGED <<part, due, attempts, parked, byHand, cause, stageDown, outages, tenantDown,
                  faulted, rearmed, admin>>

MCUnparkKeepingCause(m) ==
  /\ m \in parked
  /\ admin < MaxAdmin /\ admin' = admin + 1
  /\ parked' = parked \ {m} /\ byHand' = byHand \ {m}
  /\ attempts' = [attempts EXCEPT ![m] = 0]
  /\ held' = [held EXCEPT ![m] = 0]
  /\ due' = [due EXCEPT ![m] = TRUE]
  /\ rearmed' = rearmed \ {m}
  /\ UNCHANGED <<part, processed, procOrder, cause, served, stageDown, outages, tenantDown, faulted>>

\* A bulk unpark that resets the attempts, as a single unpark does: no canary.
MCUnparkAllResetting(m) ==
  /\ m \in parked
  /\ admin < MaxAdmin /\ admin' = admin + 1
  /\ parked' = parked \ {m} /\ byHand' = byHand \ {m}
  /\ attempts' = [attempts EXCEPT ![m] = 0]
  /\ held' = [held EXCEPT ![m] = 0]
  /\ due' = [due EXCEPT ![m] = TRUE]
  /\ rearmed' = rearmed \cup {m}
  /\ UNCHANGED <<part, processed, procOrder, cause, served, stageDown, outages, tenantDown, faulted>>

=============================================================================
