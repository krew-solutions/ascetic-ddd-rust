---------------------------- MODULE MCScheduler ----------------------------
\* The instances TLC checks, four lines each, W below L.
EXTENDS Scheduler, TLC

MCJudgements == {"expected", "bare", "unexpected", "not_taken", "unknown"}
\* What each instance's handlers answer: what its properties need, so that
\* TLC ends within minutes.
WinJudgements == {"expected", "bare", "not_taken", "unknown"}
BarJudgements == {"expected", "unexpected", "unknown"}

(* ------------------------------------------------------------------------ *)
(* The window and a captured value                                           *)
(*   1  at 0, captures a value, resent when not taken                        *)
(*   2  at 1, awaits 1 expected and sends its value                          *)
(*   3  at 1, awaits 1 settled, idempotent                                   *)
(*   4  at 2: read with the read-ahead, the cursor at 2 lets 1 go            *)
(* 2 may still wait for its value when the window has let its producer go;  *)
(* read early, 2 is ready before its plan.                                  *)

WinN == 4
WinAt == <<0, 1, 1, 2>>
WinThink == <<0, 0, 0, 0>>
WinAfter == <<
  <<>>,
  << [k |-> "step", dep |-> 1, on |-> "expected"] >>,
  << [k |-> "step", dep |-> 1, on |-> "settled"] >>,
  <<>>
>>
WinKey == <<"a", "b", "c", "d">>
WinProducer == <<0, 1, 0, 0>>
WinCaptures == <<TRUE, FALSE, FALSE, FALSE>>
WinTimeout == <<2, 1, 1, 1>>
WinSafety == <<"not_taken", "never", "idempotent", "never">>
WinRetained == <<FALSE, FALSE, FALSE, FALSE>>
WinTimes == 0..5

(* ------------------------------------------------------------------------ *)
(* A barrier and provenance                                                  *)
(*   1  at 0, key a                                                          *)
(*   2  at 1, key a, awaits 1 sent                                           *)
(*   3  at 2, holds the barrier of key a settled: members 1 and 2            *)
(*   4  at 2, key a, think 1, awaits 2 expected: a member read after 3      *)
(* 2 may end before 1, and 4 after it: a count of key a's members reaches   *)
(* 3's two with 1 still out.  1 unknown makes 2 conditional, and 4 through  *)
(* it.  Read with the read-ahead, 3 and 4 move the cursor to 2 and the      *)
(* window lets 1 go: 1 failed and ended, its cell may go while 2 and 4 are  *)
(* still to sweep key a's prefixes and fire 3.                              *)

BarN == 4
BarAt == <<0, 1, 2, 2>>
BarThink == <<0, 0, 0, 1>>
BarAfter == <<
  <<>>,
  << [k |-> "step", dep |-> 1, on |-> "sent"] >>,
  << [k |-> "each", key |-> "a", on |-> "settled"] >>,
  << [k |-> "step", dep |-> 2, on |-> "expected"] >>
>>
BarKey == <<"a", "a", "h", "a">>
BarProducer == <<0, 0, 0, 0>>
BarCaptures == <<FALSE, FALSE, FALSE, FALSE>>
BarTimeout == <<3, 1, 1, 1>>
BarSafety == <<"never", "idempotent", "never", "not_taken">>
BarRetained == <<FALSE, FALSE, FALSE, FALSE>>
BarTimes == 0..6

(* ------------------------------------------------------------------------ *)
(* The plan's own bound (ADR-0047 K1e-2): an instance whose bound turns on   *)
(* each of its terms, W = 1, L = 1                                           *)
(*   1  at 0, key a, timeout 3                                               *)
(*   2  at 0, captures a value                                               *)
(*   3  at 1, key a: held by 1's end, its key's, past its own                *)
(*   4  at 1, awaits 2 expected and 1 settled, and sends 2's value: it pins  *)
(*      2 until its release is decided, as late as 1's end                   *)
(*   5  at 4, 6 at 5: the cursor moves on                                    *)
(* At the read of 5, 2 is held only by its pin and 3 only by its key; at    *)
(* the read of 6 the bound holds 3 of the 6 lines read.  The same plan as a *)
(* command log (`crates/scheduler/tests/bound.rs`) gives the validator's     *)
(* bound these numbers too.                                                 *)

BndN == 6
BndAt == <<0, 0, 1, 1, 4, 5>>
BndThink == <<0, 0, 0, 0, 0, 0>>
BndAfter == << <<>>, <<>>, <<>>,
               << [k |-> "step", dep |-> 2, on |-> "expected"], [k |-> "step", dep |-> 1, on |-> "settled"] >>,
               <<>>, <<>> >>
BndKey == <<"a", "p", "a", "r", "x", "y">>
BndProducer == <<0, 0, 0, 2, 0, 0>>
BndCaptures == <<FALSE, TRUE, FALSE, FALSE, FALSE, FALSE>>
BndTimeout == <<3, 1, 1, 1, 1, 1>>
BndSafety == <<"never", "never", "never", "never", "never", "never">>
BndRetained == <<FALSE, FALSE, FALSE, FALSE, FALSE, FALSE>>
BndTimes == 0..6
BndJudgements == {"expected"}

\* The bound's tables on it, as the validator computes them, and a read at
\* which it holds fewer than the lines read.
BndTables ==
  /\ RelB = <<0, 0, 1, 3, 4, 5>>
  /\ EndB = <<3, 1, 2, 4, 5, 6>>
  /\ HoldB = <<3, 3, 3, 4, 5, 6>>
  /\ HeldAt = <<1, 2, 3, 4, 5, 3>>
  /\ PendingAt = <<1, 2, 3, 4, 3, 3>>
  /\ \E j \in 1..BndN : HeldAt[j] < j
=============================================================================
