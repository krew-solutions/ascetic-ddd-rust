---------------------------- MODULE MCBridge ----------------------------
\* The instance TLC checks: three messages over two URIs, one source worker,
\* two destination partitions with one dispatcher each, batches of two, one
\* crash on each side.  Addressing and partitioning are left open.
EXTENDS Bridge, TLC

CONSTANTS m1, m2, m3, u1, u2, w1, v1, v2

MCMsgs == {m1, m2, m3}
MCUris == {u1, u2}
MCSrcWorkers == {w1}
MCDstWorkers == {v1, v2}
MCDstDispatchers == {<<v1, 1>>, <<v2, 1>>}

\* The small instance for the acknowledge-before-store variant: one message is enough to lose it.
MCOneMsg == {m1}
MCOneUri == {u1}
MCOneDst == {v1}
MCOneDispatcher == {<<v1, 1>>}

=============================================================================
