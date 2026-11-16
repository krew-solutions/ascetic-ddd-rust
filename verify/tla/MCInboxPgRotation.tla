------------------------ MODULE MCInboxPgRotation ------------------------
\* The instances of MCInboxPg with a history variable beside the statements,
\* for ADR-0041 decision 2's bound on the writes of a slot whose every head
\* waits for its backoff.
EXTENDS MCInboxPg

\* Not part of the protocol: the walks of each slot that returned Passed in
\* a row -- since its queue, or which of its rows are due, last changed, or
\* since a head was taken there or the slot went dormant.
VARIABLE passes

PassedStep(s) ==
  \E d \in Dispatchers :
    /\ pc[d] = "taken" /\ slot[d] = s /\ pc'[d] = "idle"
    /\ Queue(s) # {} /\ round'[s] # None

QueueView(s) == <<Queue(s), {m \in Queue(s) : DueSeen(Now, m)}>>

Count ==
  passes' = [s \in Slots |->
              IF PassedStep(s) THEN passes[s] + 1
              ELSE IF round'[s] = None \/ (QueueView(s))' # QueueView(s) THEN 0
              ELSE passes[s]]

MCInit == Init /\ passes = [s \in Slots |-> 0]
MCSpec == MCInit /\ [][Next /\ Count]_<<vars, part, passes>> /\ Fairness

\* A round over the T_s Tenants of a slot examines them in ceil(T_s / K)
\* walks.  It ends at the walk that passes the cursor it began at: when that
\* place is no Tenant's any more -- the Tenant served last had no row left --
\* the walk that examined the last Tenant before it cannot know, and one
\* walk more ends the round.  So while nothing in the slot changes, at most
\* ceil(T_s / K) Passed come in a row, then the Nothing that puts the slot
\* to sleep: a slot whose every head waits writes a bounded number of row
\* versions, then nothing until it wakes.  The rate in time is E4's.
PassedPerRound ==
  \A s \in Slots :
    passes[s] = 0 \/ (passes[s] - 1) * K < Cardinality({TenantOf[m] : m \in Queue(s)})

=============================================================================
