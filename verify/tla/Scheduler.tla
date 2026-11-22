------------------------------ MODULE Scheduler ------------------------------
(***************************************************************************)
(* The dependency machine of `crates/scheduler` (ADR-0047 decisions 2-8),  *)
(* as a protocol.                                                          *)
(*                                                                         *)
(* A command log is lines 1..N in the order of their planned instants.  A  *)
(* reader gives them to the machine in that order, each no earlier than    *)
(* its instant less the read-ahead L.  When a line is read, its step       *)
(* dependencies bind through the window: the lines read whose instant is   *)
(* within W behind the read cursor, the latest instant read, or retained.  *)
(* A barrier waits for its key's members read before its holder, by        *)
(* prefix: per key and strength, how many members in line order met it,   *)
(* the latest instant among them, and the first that ended otherwise than *)
(* expected.  A step's handler sends its attempt and answers it, or gives*)
(* up at the deadline; every input is stamped, and the machine takes them *)
(* at their stamps: `now` is the instant of the inputs it takes, and it    *)
(* moves on only when nothing is due at it.                                *)
(*                                                                         *)
(* A dependency is met: Sent at the step's first send, or at its end if   *)
(* it was never sent; Settled at its end; Expected at an expected end,     *)
(* and never at any other — the dependent is then skipped.  A step whose   *)
(* dependencies are met is released at max(at, ready + think) and not      *)
(* before it was read, `ready` the latest met instant.  A captured value  *)
(* lives in its producer's cell and is taken when the reader is           *)
(* dispatched; missing, the reader is skipped.  A deadline runs from the  *)
(* release; a not-taken answer is resent unless the step is never resent, *)
(* an unknown one only if it is idempotent.                                *)
(*                                                                         *)
(* A cell is freed once its step ended, the window let it go, no reader   *)
(* of its captured value is still to be decided and every prefix of       *)
(* its barrier key passed it.  Nothing else reads an ended step's cell:   *)
(* a dependent folds a met instant in when it is told, a barrier holder   *)
(* reads its key's prefix and latest instant, and the prefix of members   *)
(* expected is not swept again once a member failed it.  The machine      *)
(* follows this rule since ADR-0047 K1e; before it, it kept every cell,    *)
(* swept to the failed member again and walked a holder's members, which   *)
(* ResweepFailed and WalkMembers keep as mutants.                          *)
(*                                                                         *)
(*                                                                         *)
(* Mutant names a refuted design (ADR-0047 § Alternatives Considered):    *)
(*   BindAtRelease       dependencies bound when the step is released     *)
(*   EvictByClock        the window evicted by the machine's clock        *)
(*   FreeOnEvict         a cell freed as the window lets it go            *)
(*   CounterBarrier      a barrier counts the members that met it         *)
(*   ReadyWithoutAt      released at ready + think, its plan ignored       *)
(*   NoSkipPropagation   a dependency that failed skips nothing           *)
(*   DeadlineByArrival   the machine's own timer ends a step at its       *)
(*                       deadline, before an answer stamped by it         *)
(*   NoProvenance        the verdict's fold marks nothing conditional     *)
(*   ProvenanceOneLevel  the fold marks only a failure's direct dependents *)
(*   ResweepFailed       a prefix of members expected swept again from the *)
(*                       member that failed it                             *)
(*   WalkMembers         a holder takes its members' instants from their    *)
(*                       cells                                             *)
(*   PendingByRate       the plan's bound by rate alone: a step ends its   *)
(*                       timeout after its plan, whatever it awaits         *)
(*   BoundWithoutPins, BoundWithoutPrefix, BoundWithoutReadAhead            *)
(*                       the bound with one of its terms left out           *)
(* Every one is "none" in the decided design.                             *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
  N,           \* the log's steps: lines 1..N
  At,          \* At[l]: its planned instant
  Think,       \* Think[l]
  After,       \* After[l]: its dependencies in order, each
               \*   [k |-> "step", dep |-> line, on |-> strength] or
               \*   [k |-> "each", key |-> key, on |-> strength]
  Key,         \* Key[l]: the barrier key it is a member of
  Producer,    \* Producer[l]: the line whose captured value it sends, or 0
  Captures,    \* Captures[l]: whether its expected answer captures a value
  Timeout,     \* Timeout[l]: its deadline after its release
  Safety,      \* Safety[l]: "never", "not_taken" or "idempotent"
  Retained,    \* Retained[l]: kept by the window for the whole run
  W,           \* the window
  L,           \* the read-ahead
  Lag,         \* the reader gives a line by its instant plus Lag
  Times,       \* the instants time takes
  Judgements,  \* what the handlers may answer
  MaxAttempts, \* the attempts a not-taken or unknown answer may lead to
  Mutant

Lines == 1..N
None == -1
Never == -2
Strengths == {"sent", "settled", "expected"}
Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y
TMax == Max(Times)

Entries(l) == 1..Len(After[l])
IsStep(l, i) == After[l][i].k = "step"
Keys == {Key[l] : l \in Lines} \cup UNION {{After[l][i].key : i \in {x \in Entries(l) : ~IsStep(l, x)}} : l \in Lines}
Members(b) == {m \in Lines : Key[m] = b}
\* Tables of the log, which TLC computes once: a member's ordinal among its
\* key's members, in line order, from 1; the member at an ordinal; how many
\* of a key's members lie before a line, N + 1 standing for past the log.
OrdOf == [m \in Lines |-> Cardinality({x \in Members(Key[m]) : x <= m})]
MemberOf == [b \in Keys |-> [p \in 1..Cardinality(Members(b)) |-> CHOOSE m \in Members(b) : OrdOf[m] = p]]
CountOf == [h \in 1..(N + 1) |-> [b \in Keys |-> Cardinality({m \in Members(b) : m < h})]]
Ord(m) == OrdOf[m]
MemberAt(b, p) == MemberOf[b][p]
\* The members a holder at line h waits for: its key's lines before it.
Count(h, b) == CountOf[h][b]
\* What a dependency awaits, by the log: its step, or its barrier's members
\* read before the holder.
Awaited(l, i) == IF IsStep(l, i) THEN {After[l][i].dep} ELSE {m \in Members(After[l][i].key) : m < l}

\* The validator's rules the machine relies on: lines in the order of their
\* instants; every step dependency an earlier line within W of the
\* dependent's instant, or retained; a producer awaited as expected and
\* capturing; `feasible`.
ASSUME \A l \in Lines : l > 1 => At[l - 1] <= At[l]
ASSUME \A l \in Lines : \A i \in Entries(l) : IsStep(l, i) =>
         /\ After[l][i].dep < l
         /\ Retained[After[l][i].dep] \/ At[l] - At[After[l][i].dep] <= W
ASSUME \A l \in Lines : Producer[l] # 0 =>
         /\ Captures[Producer[l]]
         /\ \E i \in Entries(l) : IsStep(l, i) /\ After[l][i].dep = Producer[l] /\ After[l][i].on = "expected"
ASSUME \A l \in Lines : \A i \in Entries(l) : \A m \in Awaited(l, i) : At[l] >= At[m] + Think[l]
ASSUME \A l \in Lines : Timeout[l] > 0

VARIABLES
  now,      \* the machine's instant: the stamp of the inputs it takes
  next,     \* the line the reader gives next
  kept,     \* the window: the lines a dependency read now may bind
  read,     \* read[l]: when it was read
  bnd,      \* bnd[l][i]: the line its step dependency i bound, 0 for none
  ph,       \* "unread", "waiting", "due", "flight", "done"
  wait,     \* wait[l]: its dependencies not yet met
  rdy,      \* rdy[l]: the latest met instant it was told of
  skp,      \* skp[l]: why it is skipped, <<"dependency" | "member" | "missing_capture", line>>, or <<>>
  int,      \* int[l]: its intended instant, once released
  rel,      \* rel[l]: its release, once released
  dl,       \* dl[l]: its deadline, once dispatched
  att,      \* att[l]: its attempts
  disp,     \* disp[l]: its first dispatch
  snt,      \* snt[l]: its first send
  out,      \* out[l]: the attempt its handler holds, 0 for none
  left,     \* left[l]: whether that attempt's bytes left
  end,      \* end[l]: "none", "expected", "unexpected", "unknown", "skipped"
  ended,    \* ended[l]: when
  cap,      \* cap[l]: its expected answer gave the value it captures
  pre,      \* pre[b][s]: the members of key b, in line order, that met s
  lat,      \* lat[b][s]: the latest instant among them
  fail,     \* fail[b]: the ordinal of its first member that ended otherwise than expected, 0 for none
  freed,    \* freed[l]: its cell is gone
  err,      \* the machine refused a line: a dependency bound to nothing
  uaf,      \* a freed cell was read
  heard     \* heard[l]: what its handler answered its attempt, "none" before

vars == <<now, next, kept, read, bnd, ph, wait, rdy, skp, int, rel, dl, att, disp, snt, out, left,
          end, ended, cap, pre, lat, fail, freed, err, uaf, heard>>

(* ------------------------------------------------------------------------ *)
(* When a dependency is met                                                  *)

\* For a step with first send s, end e at t: when it met `on`, None if not
\* yet, Never if it never will.
MetOf(s, e, t, on) ==
  IF on = "sent" /\ s # None THEN s
  ELSE IF e = "none" THEN None
  ELSE IF on = "expected" /\ e # "expected" THEN Never
  ELSE t

Met(m, on) == MetOf(snt[m], end[m], ended[m], on)

\* The window keeps a line within W behind the cursor c, or retained.
KeepAt(c, j) == Retained[j] \/ c - At[j] <= W

\* How far a sweep of key b's prefix for `on` goes from p, over the nr
\* members read, j's state being s, e, t.
RECURSIVE SweepFrom(_, _, _, _, _, _, _, _)
SweepFrom(j, s, e, t, b, on, p, nr) ==
  IF p < nr /\ (IF MemberAt(b, p + 1) = j THEN MetOf(s, e, t, on) ELSE Met(MemberAt(b, p + 1), on)) >= 0
  THEN SweepFrom(j, s, e, t, b, on, p + 1, nr)
  ELSE p

\* A barrier dependency of holder h: met at an instant, None, or Never,
\* given its key's prefixes np, latest instants nl and first failure nf, and
\* Mn, when a member met a strength.
HolderMet(h, i, np, nl, nf, Mn(_, _)) ==
  LET b == After[h][i].key
      on == After[h][i].on
      n == Count(h, b)
      counted == {m \in Members(b) : m < next /\ Mn(m, on) >= 0}
  IN IF on = "expected" /\ nf # 0 /\ nf <= n THEN Never
     ELSE IF Mutant = "CounterBarrier"
          THEN IF Cardinality(counted) >= n THEN Max({0} \cup {Mn(m, on) : m \in counted}) ELSE None
          ELSE IF np[on] >= n THEN nl[on] ELSE None

(* ------------------------------------------------------------------------ *)
(* A step's state changes: its dependents and its barrier are told          *)

\* j's first send, end and end instant become s, e, t; why is its skip, u
\* whether the step read a freed cell.  Its dependents fold in what it now
\* met, or are to be skipped; its key's prefixes sweep on.
Settle(j, s, e, t, why, u) ==
  LET b == Key[j]
      nr == Count(next, b)
      Mn(m, on) == IF m = j THEN MetOf(s, e, t, on) ELSE Met(m, on)
      \* A prefix of members expected stops for good at one that ended
      \* otherwise: it is not swept again, and that member's cell not read.
      Stuck(on) == on = "expected" /\ fail[b] # 0 /\ Mutant # "ResweepFailed"
      np == [on \in Strengths |-> IF Stuck(on) THEN pre[b][on] ELSE SweepFrom(j, s, e, t, b, on, pre[b][on], nr)]
      nl == [on \in Strengths |->
               Max({lat[b][on]} \cup {Mn(MemberAt(b, x), on) : x \in (pre[b][on] + 1)..np[on]})]
      stop == np["expected"] + 1
      nf == IF fail[b] = 0 /\ stop <= nr /\ Mn(MemberAt(b, stop), "expected") = Never THEN stop ELSE fail[b]
      \* The members the sweeps read: each up to the one that stopped it.
      swept == UNION {{MemberAt(b, x) : x \in (pre[b][on] + 1)..Min({np[on] + 1, nr})} : on \in {o \in Strengths : ~Stuck(o)}}
      StepHit(r, i) == IsStep(r, i) /\ bnd[r][i] = j
      BarHit(r, i) == ~IsStep(r, i) /\ After[r][i].key = b
      NewMet(r, i) == IF StepHit(r, i) THEN Mn(j, After[r][i].on)
                      ELSE IF BarHit(r, i) THEN HolderMet(r, i, np, nl, nf, Mn)
                      ELSE None
      Told(r) == ph[r] = "waiting" /\ r # j
      met(r) == {i \in wait[r] : NewMet(r, i) >= 0}
      bad(r) == {i \in wait[r] : NewMet(r, i) = Never}
      Why(r) == IF \E i \in bad(r) : StepHit(r, i) THEN <<"dependency", j>> ELSE <<"member", MemberAt(b, nf)>>
  IN /\ snt' = [snt EXCEPT ![j] = s]
     /\ end' = [end EXCEPT ![j] = e]
     /\ ended' = [ended EXCEPT ![j] = t]
     /\ pre' = [pre EXCEPT ![b] = np]
     /\ lat' = [lat EXCEPT ![b] = nl]
     /\ fail' = [fail EXCEPT ![b] = nf]
     /\ wait' = [r \in Lines |-> IF Told(r) THEN wait[r] \ met(r) ELSE wait[r]]
     /\ rdy' = [r \in Lines |-> IF Told(r) THEN Max({rdy[r]} \cup {NewMet(r, i) : i \in met(r)}) ELSE rdy[r]]
     /\ skp' = [r \in Lines |->
                  IF r = j THEN why
                  ELSE IF Told(r) /\ skp[r] = <<>> /\ bad(r) # {} /\ Mutant # "NoSkipPropagation"
                       THEN Why(r)
                       ELSE skp[r]]
     /\ uaf' = \/ uaf \/ u
               \/ \E m \in swept : m # j /\ freed[m]
               \/ Mutant = "WalkMembers" /\ \E r \in Lines : Told(r) /\
                    \E i \in met(r) : BarHit(r, i) /\ \E m \in Awaited(r, i) : m # j /\ freed[m]

\* l ends now as e, skipped for why.
Finish(l, e, why, u) ==
  /\ ph' = [ph EXCEPT ![l] = "done"]
  /\ Settle(l, snt[l], e, now, why, u)

(* ------------------------------------------------------------------------ *)
(* The reader                                                                *)

\* Under BindAtRelease nothing is bound when it is read; under EvictByClock
\* the window lets go what lies W behind the clock.
ReadLine ==
  /\ ~err
  /\ next <= N
  /\ now >= At[next] - L
  /\ LET l == next
         c == IF Mutant = "EvictByClock" THEN now ELSE At[l]
         k1 == {j \in kept : KeepAt(c, j)}
         b == [i \in Entries(l) |->
                 IF IsStep(l, i) /\ Mutant # "BindAtRelease" /\ After[l][i].dep \in k1
                 THEN After[l][i].dep ELSE 0]
         unbound == Mutant # "BindAtRelease" /\ \E i \in Entries(l) : IsStep(l, i) /\ b[i] = 0
         m == [i \in Entries(l) |->
                 IF IsStep(l, i)
                 THEN IF b[i] = 0 THEN None ELSE Met(b[i], After[l][i].on)
                 ELSE HolderMet(l, i, pre[After[l][i].key], lat[After[l][i].key], fail[After[l][i].key], Met)]
         failed == {i \in Entries(l) : m[i] = Never}
         Inst(i) == IF IsStep(l, i) THEN ended[b[i]]
                    ELSE ended[MemberAt(After[l][i].key, fail[After[l][i].key])]
         \* The dependency that failed first; on a tie, the first in its list.
         first == LET t0 == Min({Inst(i) : i \in failed}) IN Min({i \in failed : Inst(i) = t0})
         why == IF IsStep(l, first) THEN <<"dependency", b[first]>>
                ELSE <<"member", MemberAt(After[l][first].key, fail[After[l][first].key])>>
     IN /\ next' = next + 1
        /\ kept' = IF KeepAt(c, l) THEN k1 \cup {l} ELSE k1
        /\ read' = [read EXCEPT ![l] = now]
        /\ bnd' = [bnd EXCEPT ![l] = b]
        /\ ph' = [ph EXCEPT ![l] = "waiting"]
        /\ wait' = [wait EXCEPT ![l] = {i \in Entries(l) : m[i] = None
                                          \/ (m[i] = Never /\ Mutant = "NoSkipPropagation")}]
        /\ rdy' = [rdy EXCEPT ![l] = Max({0} \cup {m[i] : i \in {x \in Entries(l) : m[x] >= 0}})]
        /\ skp' = [skp EXCEPT ![l] = IF failed = {} \/ Mutant = "NoSkipPropagation" THEN <<>> ELSE why]
        /\ err' = unbound
        /\ uaf' = \/ uaf
                  \/ \E i \in Entries(l) : IsStep(l, i) /\ b[i] # 0 /\ freed[b[i]]
                  \/ Mutant = "WalkMembers" /\ \E i \in Entries(l) :
                       ~IsStep(l, i) /\ m[i] >= 0 /\ \E x \in Awaited(l, i) : freed[x]
  /\ UNCHANGED <<now, int, rel, dl, att, disp, snt, out, left, end, ended, cap, pre, lat, fail, freed, heard>>

(* ------------------------------------------------------------------------ *)
(* The machine's own steps                                                   *)

\* Under BindAtRelease a waiting step binds through the window as it stands.
LateBound(l) == [i \in Entries(l) |->
                   IF IsStep(l, i) /\ After[l][i].dep \in kept THEN After[l][i].dep ELSE 0]
LateUnbound(l) == \E i \in Entries(l) : IsStep(l, i) /\ LateBound(l)[i] = 0
LateMet(l) == \A i \in Entries(l) : IsStep(l, i) => Met(After[l][i].dep, After[l][i].on) >= 0
LateFailed(l) == {i \in Entries(l) : IsStep(l, i) /\ Met(After[l][i].dep, After[l][i].on) = Never}
LateWaits(l) == {i \in wait[l] : ~IsStep(l, i)}

CanDecide(l) ==
  /\ ~err
  /\ ph[l] = "waiting"
  /\ IF Mutant = "BindAtRelease"
     THEN LateUnbound(l) \/ skp[l] # <<>> \/ LateFailed(l) # {} \/ (LateMet(l) /\ LateWaits(l) = {})
     ELSE skp[l] # <<>> \/ wait[l] = {}

Release(l, r0) ==
  LET i == IF Mutant = "ReadyWithoutAt" THEN r0 + Think[l] ELSE Max({At[l], r0 + Think[l]})
  IN /\ int' = [int EXCEPT ![l] = i]
     /\ rel' = [rel EXCEPT ![l] = Max({i, read[l]})]
     /\ ph' = [ph EXCEPT ![l] = "due"]

Decide(l) ==
  /\ CanDecide(l)
  /\ IF Mutant = "BindAtRelease" /\ LateUnbound(l)
     THEN /\ err' = TRUE
          /\ UNCHANGED <<now, next, kept, read, bnd, ph, wait, rdy, skp, int, rel, dl, att, disp, snt, out,
                         left, end, ended, cap, pre, lat, fail, freed, uaf, heard>>
     ELSE IF skp[l] # <<>> \/ (Mutant = "BindAtRelease" /\ LateFailed(l) # {})
     THEN /\ Finish(l, "skipped",
                    IF skp[l] # <<>> THEN skp[l]
                    ELSE <<"dependency", After[l][Min(LateFailed(l))].dep>>, FALSE)
          /\ bnd' = IF Mutant = "BindAtRelease" THEN [bnd EXCEPT ![l] = LateBound(l)] ELSE bnd
          /\ UNCHANGED <<now, next, kept, read, int, rel, dl, att, disp, out, left, cap, freed, err, heard>>
     ELSE /\ LET r0 == IF Mutant = "BindAtRelease"
                       THEN Max({rdy[l]} \cup {Met(After[l][i].dep, After[l][i].on) : i \in {x \in Entries(l) : IsStep(l, x)}})
                       ELSE rdy[l]
             IN Release(l, r0)
          /\ bnd' = IF Mutant = "BindAtRelease" THEN [bnd EXCEPT ![l] = LateBound(l)] ELSE bnd
          \* Its release decided, it takes the value it sends from its producer's
          \* cell and lets go of it: missing, it is skipped for it when dispatched.
          /\ LET p == Producer[l] IN
             /\ skp' = [skp EXCEPT ![l] = IF p # 0 /\ (freed[p] \/ ~cap[p]) THEN <<"missing_capture", p>> ELSE <<>>]
             /\ uaf' = (uaf \/ (p # 0 /\ freed[p]))
          /\ UNCHANGED <<now, next, kept, read, wait, rdy, dl, att, disp, snt, out, left, end, ended,
                         cap, pre, lat, fail, freed, err, heard>>

CanDispatch(l) == ~err /\ ph[l] = "due" /\ rel[l] = now

\* The value its captured hole takes was taken when its release was decided.
Dispatch(l) ==
  /\ CanDispatch(l)
  /\ LET d == rel[l] + Timeout[l]
     IN IF skp[l] # <<>>
        THEN /\ Finish(l, "skipped", skp[l], FALSE)
             /\ UNCHANGED <<now, next, kept, read, bnd, int, rel, dl, att, disp, out, left, cap, freed, err, heard>>
        ELSE /\ dl' = [dl EXCEPT ![l] = d]
             /\ att' = [att EXCEPT ![l] = 1]
             /\ disp' = [disp EXCEPT ![l] = now]
             /\ out' = [out EXCEPT ![l] = 1]
             /\ left' = [left EXCEPT ![l] = FALSE]
             /\ ph' = [ph EXCEPT ![l] = "flight"]
             /\ UNCHANGED <<now, next, kept, read, bnd, wait, rdy, skp, int, rel, snt, end, ended, cap,
                            pre, lat, fail, freed, err, uaf, heard>>

\* The refuted timer: the machine ends a step at its deadline by its own
\* clock, whatever its handler holds.
CanTimer(l) == Mutant = "DeadlineByArrival" /\ ~err /\ ph[l] = "flight" /\ end[l] = "none" /\ now >= dl[l]

Timer(l) ==
  /\ CanTimer(l)
  /\ Finish(l, "unknown", <<>>, FALSE)
  /\ UNCHANGED <<now, next, kept, read, bnd, int, rel, dl, att, disp, out, left, cap, freed, err, heard>>

\* Readers of a captured value whose release is still to be decided.
NoReader(l) == \A r \in Lines : Producer[r] = l /\ r < next => ph[r] # "waiting"
\* Every prefix of its key passed it, or, for expected, stopped at or
\* before it for good.
Passed(l) == \A s \in Strengths :
               \/ pre[Key[l]][s] >= Ord(l)
               \/ s = "expected" /\ fail[Key[l]] # 0 /\ fail[Key[l]] <= Ord(l)

CanFree(l) ==
  /\ ~err
  /\ ~freed[l]
  /\ end[l] # "none"
  /\ l < next
  /\ l \notin kept
  /\ Mutant = "FreeOnEvict" \/ (NoReader(l) /\ Passed(l))

Free(l) ==
  /\ CanFree(l)
  /\ freed' = [freed EXCEPT ![l] = TRUE]
  /\ UNCHANGED <<now, next, kept, read, bnd, ph, wait, rdy, skp, int, rel, dl, att, disp, snt, out, left,
                 end, ended, cap, pre, lat, fail, err, uaf, heard>>

(* ------------------------------------------------------------------------ *)
(* The handlers: an attempt's bytes leave, it is answered, or its handler   *)
(* gives up at the deadline                                                  *)

Sent(l) ==
  /\ ~err
  /\ out[l] # 0
  /\ ~left[l]
  /\ now < dl[l]
  /\ left' = [left EXCEPT ![l] = TRUE]
  /\ IF snt[l] = None /\ end[l] = "none" /\ att[l] = out[l]
     THEN Settle(l, now, end[l], ended[l], skp[l], FALSE)
     ELSE UNCHANGED <<snt, end, ended, wait, rdy, skp, pre, lat, fail, uaf>>
  /\ UNCHANGED <<now, next, kept, read, bnd, ph, int, rel, dl, att, disp, out, cap, freed, err, heard>>

\* "bare" is an expected answer without the value the step captures.
Answer(l, j) ==
  /\ ~err
  /\ out[l] # 0
  /\ left[l]
  /\ now <= dl[l]
  /\ j \in {"not_taken", "unknown"} => out[l] < MaxAttempts
  /\ heard' = [heard EXCEPT ![l] = j]
  /\ IF end[l] # "none" \/ att[l] # out[l]
     THEN \* An answer the machine no longer waits for changes nothing.
          /\ out' = [out EXCEPT ![l] = 0]
          /\ UNCHANGED <<now, next, kept, read, bnd, ph, wait, rdy, skp, int, rel, dl, att, disp, snt, left,
                         end, ended, cap, pre, lat, fail, freed, err, uaf>>
     ELSE IF j \in {"expected", "bare"}
     THEN /\ out' = [out EXCEPT ![l] = 0]
          /\ cap' = [cap EXCEPT ![l] = (j = "expected" /\ Captures[l])]
          /\ Finish(l, "expected", <<>>, FALSE)
          /\ UNCHANGED <<now, next, kept, read, bnd, int, rel, dl, att, disp, left, freed, err>>
     ELSE IF (j = "not_taken" /\ Safety[l] # "never" /\ now < dl[l])
             \/ (j = "unknown" /\ Safety[l] = "idempotent" /\ now < dl[l])
     THEN \* Resent at once, under the same deadline.
          /\ att' = [att EXCEPT ![l] = out[l] + 1]
          /\ out' = [out EXCEPT ![l] = out[l] + 1]
          /\ left' = [left EXCEPT ![l] = FALSE]
          /\ UNCHANGED <<now, next, kept, read, bnd, ph, wait, rdy, skp, int, rel, dl, disp, snt, end, ended,
                         cap, pre, lat, fail, freed, err, uaf>>
     ELSE /\ out' = [out EXCEPT ![l] = 0]
          /\ Finish(l, IF j = "unknown" THEN "unknown" ELSE "unexpected", <<>>, FALSE)
          /\ UNCHANGED <<now, next, kept, read, bnd, int, rel, dl, att, disp, left, cap, freed, err>>

\* The handler's own deadline: unknown, stamped at it.
GiveUp(l) ==
  /\ ~err
  /\ out[l] # 0
  /\ now = dl[l]
  /\ heard' = [heard EXCEPT ![l] = "deadline"]
  /\ out' = [out EXCEPT ![l] = 0]
  /\ IF end[l] = "none" /\ att[l] = out[l]
     THEN /\ Finish(l, "unknown", <<>>, FALSE)
          /\ UNCHANGED <<now, next, kept, read, bnd, int, rel, dl, att, disp, left, cap, freed, err>>
     ELSE UNCHANGED <<now, next, kept, read, bnd, ph, wait, rdy, skp, int, rel, dl, att, disp, snt, left,
                      end, ended, cap, pre, lat, fail, freed, err, uaf>>

(* ------------------------------------------------------------------------ *)
(* Time                                                                      *)

\* What must happen at `now` before it moves on: a decision, a dispatch, a
\* handler at its deadline, the refuted timer, a line the reader owes, and
\* a free: a cell freed as early as it may be is the worst case for a read
\* of it.
Urgent ==
  \/ \E l \in Lines : CanDecide(l) \/ CanDispatch(l) \/ CanTimer(l) \/ CanFree(l) \/ (out[l] # 0 /\ now = dl[l])
  \/ next <= N /\ now >= At[next] + Lag

\* The instants it may not pass.
Dues ==
  {rel[l] : l \in {x \in Lines : ph[x] = "due"}}
  \cup {dl[l] : l \in {x \in Lines : out[x] # 0 \/ (ph[x] = "flight" /\ end[x] = "none")}}
  \cup (IF next <= N THEN {At[next] + Lag} ELSE {})

Tick ==
  /\ ~err
  /\ ~Urgent
  /\ \E t \in Times :
       /\ t > now
       /\ \A d \in Dues : d > now => t <= d
       /\ now' = t
  /\ UNCHANGED <<next, kept, read, bnd, ph, wait, rdy, skp, int, rel, dl, att, disp, snt, out, left,
                 end, ended, cap, pre, lat, fail, freed, err, uaf, heard>>

Init ==
  /\ now = Min(Times)
  /\ next = 1
  /\ kept = {}
  /\ read = [l \in Lines |-> None]
  /\ bnd = [l \in Lines |-> [i \in Entries(l) |-> 0]]
  /\ ph = [l \in Lines |-> "unread"]
  /\ wait = [l \in Lines |-> {}]
  /\ rdy = [l \in Lines |-> 0]
  /\ skp = [l \in Lines |-> <<>>]
  /\ int = [l \in Lines |-> None]
  /\ rel = [l \in Lines |-> None]
  /\ dl = [l \in Lines |-> None]
  /\ att = [l \in Lines |-> 0]
  /\ disp = [l \in Lines |-> None]
  /\ snt = [l \in Lines |-> None]
  /\ out = [l \in Lines |-> 0]
  /\ left = [l \in Lines |-> FALSE]
  /\ end = [l \in Lines |-> "none"]
  /\ ended = [l \in Lines |-> None]
  /\ cap = [l \in Lines |-> FALSE]
  /\ pre = [b \in Keys |-> [s \in Strengths |-> 0]]
  /\ lat = [b \in Keys |-> [s \in Strengths |-> 0]]
  /\ fail = [b \in Keys |-> 0]
  /\ freed = [l \in Lines |-> FALSE]
  /\ err = FALSE
  /\ uaf = FALSE
  /\ heard = [l \in Lines |-> "none"]

\* The machine's own steps, which no trace records.  The machine takes an
\* input with everything it sets off — decisions, dispatches, frees — before
\* the next one, and those steps decide what they decide in any order; so
\* they are taken one at a time, the lowest line's first.
CanStep(l) == CanDecide(l) \/ CanDispatch(l) \/ CanTimer(l) \/ CanFree(l)
Busy == \E l \in Lines : CanStep(l)
Internal ==
  LET l == Min({x \in Lines : CanStep(x)})
  IN /\ Busy
     /\ IF CanDecide(l) THEN Decide(l)
        ELSE IF CanDispatch(l) THEN Dispatch(l)
        ELSE IF CanTimer(l) THEN Timer(l)
        ELSE Free(l)
\* The inputs: a line read, an attempt's bytes left, its answer, its deadline.
Input ==
  /\ ~Busy
  /\ \/ ReadLine
     \/ \E l \in Lines : Sent(l) \/ GiveUp(l) \/ \E j \in Judgements : Answer(l, j)

Next == Internal \/ Input \/ Tick

Spec == Init /\ [][Next]_vars

(* ------------------------------------------------------------------------ *)
(* Properties                                                                *)

TypeOK ==
  /\ now \in Times
  /\ next \in 1..(N + 1)
  /\ kept \subseteq Lines
  /\ ph \in [Lines -> {"unread", "waiting", "due", "flight", "done"}]
  /\ end \in [Lines -> {"none", "expected", "unexpected", "unknown", "skipped"}]
  /\ \A l \in Lines : ph[l] = "due" => rel[l] >= now

Released(l) == rel[l] # None

\* Released no earlier than planned, than read, than its dependencies met.
ReleaseSafe ==
  \A l \in Lines : Released(l) =>
    /\ rel[l] >= At[l]
    /\ rel[l] >= read[l]
    /\ \A i \in Entries(l) : IsStep(l, i) =>
         LET m == Met(After[l][i].dep, After[l][i].on) IN m >= 0 /\ m + Think[l] <= rel[l]

\* A barrier's holder released only once every member read before it met it.
BarrierSafe ==
  \A l \in Lines : Released(l) =>
    \A i \in Entries(l) : ~IsStep(l, i) =>
      \A m \in Awaited(l, i) : LET x == Met(m, After[l][i].on) IN x >= 0 /\ x + Think[l] <= rel[l]

NoUseAfterFree == ~uaf

\* Every dependency bound, to the step the validator bound it to.
BoundAsChecked ==
  /\ ~err
  /\ \A l \in Lines : \A i \in Entries(l) : bnd[l][i] # 0 => bnd[l][i] = After[l][i].dep

\* An answer stamped by its deadline decides its attempt.
NoCensored ==
  \A l \in Lines : heard[l] \in {"expected", "bare", "unexpected"} => end[l] \in {"expected", "unexpected"}

\* The run is over: the last instant, nothing due.
Over == now = TMax /\ ~Urgent

AllEnded == \A l \in Lines : read[l] # None => end[l] # "none"

EveryReadLineEnds == Over => AllEnded

\* Provenance by its definition, over the log: a step is conditional when it
\* awaits sent or settled a step that ended unknown or skipped, or awaits a
\* conditional one.
RECURSIVE Conditional(_)
Conditional(l) ==
  \E i \in Entries(l) : \E m \in Awaited(l, i) :
    \/ After[l][i].on \in {"sent", "settled"} /\ end[m] \in {"unknown", "skipped"}
    \/ Conditional(m)

\* The verdict's fold over the journal, in line order, by the lines each
\* step bound.
JournalAwaited(l, i) == IF IsStep(l, i) THEN {bnd[l][i]} \ {0} ELSE Awaited(l, i)
RECURSIVE FoldTo(_)
FoldTo(n) ==
  IF n = 0 THEN {}
  ELSE LET s == FoldTo(n - 1) IN
       IF \E i \in Entries(n) : \E m \in JournalAwaited(n, i) :
            \/ After[n][i].on \in {"sent", "settled"} /\ end[m] \in {"unknown", "skipped"}
            \/ Mutant # "ProvenanceOneLevel" /\ m \in s
       THEN s \cup {n} ELSE s
Folded == IF Mutant = "NoProvenance" THEN {} ELSE FoldTo(N)

ProvenanceExact == Folded = {l \in Lines : read[l] # None /\ Conditional(l)}

ConditionalExact == Over => ProvenanceExact

(* ------------------------------------------------------------------------ *)
(* The plan's own bound (ADR-0047 decision 7, K1e-2), from the constants    *)
(* alone, as the validator computes it: every action ending at its          *)
(* deadline, every line read at its instant less L at the earliest.  The    *)
(* refuted PendingByRate ends a step its timeout after its plan, whatever   *)
(* it awaits.                                                               *)

RECURSIVE BoundOf(_)
\* <<latest release, latest end>> of line l.
BoundOf(l) ==
  LET deps == {After[l][i].dep : i \in {x \in Entries(l) : IsStep(l, x)}}
      members == UNION {Awaited(l, i) : i \in {x \in Entries(l) : ~IsStep(l, x)}}
      ready == Max({0} \cup {BoundOf(d)[2] : d \in deps} \cup {BoundOf(m)[2] : m \in members})
      r == Max({At[l], ready + Think[l]})
  IN <<r, IF Mutant = "PendingByRate" THEN At[l] + Timeout[l] ELSE r + Timeout[l]>>

RelB == [l \in Lines |-> BoundOf(l)[1]]
EndB == [l \in Lines |-> BoundOf(l)[2]]
\* The latest instant a machine may hold a step: its end, its key's latest
\* end up to it, the release of a reader of its value.
HoldB == [l \in Lines |->
            Max({EndB[l]}
                \cup (IF Mutant = "BoundWithoutPrefix" THEN {}
                     ELSE {EndB[m] : m \in {x \in Lines : x <= l /\ Key[x] = Key[l]}})
                \cup (IF Mutant = "BoundWithoutPins" THEN {}
                     ELSE {RelB[r] : r \in {x \in Lines : Producer[x] = l}}))]
Tau(j) == IF Mutant = "BoundWithoutReadAhead" THEN At[j] ELSE Max({At[j] - L, 0})
\* At the read of line j: the steps held — the window's, and those it let go
\* whose hold has not ended — and those not ended.
HeldAt == [j \in Lines |-> Cardinality({k \in 1..j : Retained[k] \/ At[j] - At[k] <= W \/ HoldB[k] >= Tau(j)})]
PendingAt == [j \in Lines |-> Cardinality({k \in 1..j : EndB[k] >= Tau(j)})]

\* No step released or ended past the bound.
EndsWithinBound ==
  \A l \in Lines : (rel[l] # None => rel[l] <= RelB[l]) /\ (end[l] # "none" => ended[l] <= EndB[l])

\* No more held, nor not ended, than the bound at the latest read, nothing due.
HeldWithinBound ==
  (~Busy /\ next > 1) => Cardinality({l \in Lines : read[l] # None /\ ~freed[l]}) <= HeldAt[next - 1]
PendingWithinBound ==
  (~Busy /\ next > 1) => Cardinality({l \in Lines : read[l] # None /\ end[l] = "none"}) <= PendingAt[next - 1]
=============================================================================
