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

=============================================================================
