---------------------------- MODULE MCInbox ----------------------------
\* The instance TLC checks: three messages, the second depending on the
\* first; two workers, one of them served by two dispatchers at once.
EXTENDS Inbox, TLC

CONSTANTS m1, m2, m3, w1, w2

MCMsgs == {m1, m2, m3}
MCDeps == (m1 :> {}) @@ (m2 :> {m1}) @@ (m3 :> {})
MCWorkers == {w1, w2}
MCDispatchers == {<<w1, 1>>, <<w1, 2>>, <<w2, 1>>}
MCArrivesAll == MCMsgs
MCArrivesWithoutM1 == {m2, m3}

=============================================================================
