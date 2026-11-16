------------------------- MODULE MCInboxRotation -------------------------
\* The instances of MCInbox with a history variable beside the protocol, for
\* the bound of ADR-0041 decision 2: while a message is a due head, its slot
\* takes or sets aside at most one head of each other Tenant.
EXTENDS MCInbox

\* Not part of the protocol: for each message and each Tenant, how many heads
\* of that Tenant its slot has taken or set aside while the message was a due
\* head of its (slot, Tenant), waiting; its own Tenant's entry stays 0.  It
\* counts dispatches, not time: the bound ADR-0041 states per hop is this
\* count times the time a slot takes per head.  Reset whenever the message
\* is not such a head.
VARIABLE overtaken

\* m is the due head of its (slot, Tenant): in the queue, held by nobody,
\* out of its backoff, and its Tenant's oldest row there.
DueHead(m) ==
  /\ m \in received /\ m \notin processed /\ m \notin parked /\ waiting[m] = None
  /\ m \notin Locked /\ due[m]
  /\ m = HeadOf(part[m], received, TenantOf[m])

\* The heads this step takes or sets aside.
TakenNow ==
  {n \in Msgs : \/ \E s \in Slots : holding[s] = None /\ holding'[s] = n
                \/ waiting[n] = None /\ waiting'[n] # None}

NoneOvertaken == [u \in 1..NT |-> 0]

Overtake ==
  overtaken' = [m \in Msgs |->
                 IF DueHead(m) /\ m \notin TakenNow
                 THEN [u \in 1..NT |->
                        overtaken[m][u]
                        + Cardinality({n \in TakenNow : part[n] = part[m] /\ TenantOf[n] = u /\ u # TenantOf[m]})]
                 ELSE NoneOvertaken]

MCInit == Init /\ overtaken = [m \in Msgs |-> NoneOvertaken]
MCSpec == MCInit /\ [][Next /\ Overtake]_<<vars, overtaken>> /\ Fairness

\* The cursor reaches a due head's Tenant within one turn of the others,
\* whatever their backlog: each other Tenant's head is taken or set aside at
\* most once before it, NT - 1 in all.  As a FIFO a head waits for every
\* older row of its slot, which InboxFifoSlot.cfg shows.  Two Tenants cannot
\* tell "once each" from "NT - 1 in all", nor a walk after the cursor from
\* one that skips only the cursor's Tenant; InboxRotationThree.cfg has three.
\* The counter is bounded only where this holds: a configuration whose heads
\* are overtaken without end -- a FIFO behind a poison head never parked --
\* has an infinite state space here, and checks its liveness with Spec in
\* MCInbox.tla instead.
BoundedOvertaking == \A m \in Msgs : \A u \in 1..NT : overtaken[m][u] <= 1

=============================================================================
