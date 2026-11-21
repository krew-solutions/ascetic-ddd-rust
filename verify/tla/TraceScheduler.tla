---------------------------- MODULE TraceScheduler ----------------------------
(***************************************************************************)
(* Checks a run of `crates/scheduler`'s machine in `sim` against           *)
(* Scheduler.tla.                                                          *)
(*                                                                         *)
(* The run's test writes its log, the inputs the machine took in the order *)
(* it took them, each with its stamp — a line read, an attempt's bytes     *)
(* left, an attempt's answer or its handler's deadline — and the journal  *)
(* it wrote.  trace2tla.py turns the log into the model's constants and   *)
(* the inputs into `Trace`.  The model takes exactly those inputs, at      *)
(* their stamps, and its own steps — decisions, dispatches, frees — as it *)
(* may between them; the machine's wakes are its own steps and are not     *)
(* recorded.  Time moves only up to the next input's stamp.                *)
(*                                                                         *)
(* A run that fits ends with i past the trace, nothing due, and the        *)
(* model's journal the machine's, line by line: that state violates        *)
(* NotFinished — the violation check.sh requires.  A run that does not     *)
(* fit deadlocks at the first input the model refuses, or ends with        *)
(* another journal, and TLC finds no state that violates NotFinished.     *)
(***************************************************************************)
EXTENDS Scheduler, TLC

CONSTANTS
  Trace,    \* Seq of [input, at, line, attempt, judgement], see trace2tla.py
  Journal   \* Journal[l]: the machine's entry for line l

VARIABLE i  \* the next input of the trace

Pending == i <= Len(Trace)
Step == Trace[i]

TraceInit == Init /\ i = 1

\* The inputs, at their stamps.
Logged ==
  /\ Pending
  /\ now = Step.at
  /\ i' = i + 1
  /\ \/ Step.input = "read" /\ next = Step.line /\ ReadLine
     \/ Step.input = "sent" /\ out[Step.line] = Step.attempt /\ Sent(Step.line)
     \/ /\ Step.input = "ended"
        /\ out[Step.line] = Step.attempt
        /\ \/ Answer(Step.line, Step.judgement)
           \/ Step.judgement = "unknown" /\ GiveUp(Step.line)

\* Time moves to the next input's stamp, or to what falls due before it;
\* past the trace, to what falls due, or to the end.
TraceTick ==
  /\ Tick
  /\ now' = Min({IF Pending THEN Step.at ELSE TMax} \cup {d \in Dues : d > now})

TraceNext == (Internal /\ UNCHANGED i) \/ Logged \/ (TraceTick /\ UNCHANGED i)

TraceSpec == TraceInit /\ [][TraceNext]_<<vars, i>>

(* ------------------------------------------------------------------------ *)
(* The model's journal                                                       *)

\* A step never released has its end's instants for its plan's.
Entry(l) ==
  [end |-> IF end[l] = "skipped" THEN <<"skipped", skp[l][1], skp[l][2]>> ELSE <<end[l]>>,
   read |-> read[l],
   intended |-> IF int[l] = None THEN ended[l] ELSE int[l],
   released |-> IF rel[l] = None THEN ended[l] ELSE rel[l],
   dispatched |-> disp[l],
   sent |-> snt[l],
   ended |-> ended[l],
   attempts |-> att[l],
   bound |-> bnd[l]]

\* Once the trace is taken and nothing is due, every line ended and the fold
\* marks what the definition does: the model's EveryReadLineEnds and
\* ConditionalExact, at the end of a run that need not reach the model's
\* last instant with nothing left.
EndsAsModelled == (~Pending /\ ~Urgent) => AllEnded /\ ProvenanceExact

\* Violated exactly when the whole trace was taken and the journals agree.
NotFinished == ~(~Pending /\ ~Urgent /\ \A l \in Lines : Entry(l) = Journal[l])
=============================================================================
