------------------------- MODULE MCInboxForget -------------------------
\* The instance TLC checks: three messages, the second depending on the
\* first, every one of them able to arrive again; two slots.
EXTENDS InboxForget, TLC

CONSTANTS m1, m2, m3, w1, w2

MCMsgs == {m1, m2, m3}
MCDeps == (m1 :> {}) @@ (m2 :> {m1}) @@ (m3 :> {})
MCSlots == {w1, w2}
MCNoPoison == {}

=============================================================================
