#!/usr/bin/env sh
# Model-checks every specification under verify/tla with TLC.
# Needs Java and tla2tools.jar; point TLA2TOOLS at the jar, or put it at the
# default path:
#   curl -L -o ~/.local/share/tla/tla2tools.jar \
#     https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar
set -eu
cd "$(dirname "$0")"
TLA2TOOLS="${TLA2TOOLS:-$HOME/.local/share/tla/tla2tools.jar}"
# Each run gets a state directory of its own: TLC names it from the clock to
# the second, and two runs started within one second collide.
tlc() { java -XX:+UseParallelGC -cp "$TLA2TOOLS" tlc2.TLC -workers auto -deadlock -metadir "$(mktemp -d)" "$@"; }

# Checks one recorded run against its model: trace2tla.py turns the JSON lines
# into a module over TraceOutbox.tla, TraceInbox.tla or TraceBridge.tla, and
# TLC follows it step by step.  A run that fits ends past the trace and violates NotFinished, which
# is the success here; one that does not fit deadlocks at the step the model
# refuses, and TLC prints that state.  Deadlock detection stays on for that
# reason.
check_trace() {
  work=$(mktemp -d); trap 'rm -rf "$work"' EXIT
  cp Outbox.tla TraceOutbox.tla Inbox.tla TraceInbox.tla Bridge.tla TraceBridge.tla Scheduler.tla TraceScheduler.tla "$work"
  if ! name=$(python3 trace2tla.py "$1" "$work"); then
    echo "   $1: refused by trace2tla.py"; return 1
  fi
  log="$work/tlc.log"
  if java -XX:+UseParallelGC -cp "$TLA2TOOLS" tlc2.TLC -workers 1 -metadir "$work/states" -config "$work/$name.cfg" "$work/$name.tla" > "$log" 2>&1; then
    echo "   $1: TLC found nothing, which cannot happen; see $log"; return 1
  fi
  if grep -q "Invariant NotFinished is violated" "$log"; then
    echo "   $1: fits"; rm -rf "$work"; return 0
  fi
  echo "   $1: does not fit the model:"; grep -E "^Error|^/\\\\ i = " "$log" | head -4; return 1
}

# How a recorded run stands against its model, one line: `fits`; `unfit`,
# TLC deadlocking at a step the model refuses; or `refused`, a tab, and the
# reason trace2tla.py printed for refusing to translate it. A refused run's
# steps before the refusal are checked all the same: when they do not fit,
# it stands `refused, its steps before unfit`, which no entry lists.
verdict() {
  work=$(mktemp -d)
  cp Outbox.tla TraceOutbox.tla Inbox.tla TraceInbox.tla Bridge.tla TraceBridge.tla Scheduler.tla TraceScheduler.tla "$work"
  if name=$(python3 trace2tla.py "$1" "$work" 2> "$work/refused"); then status=0; else status=$?; fi
  case $status in
    0|3) ;;
    *) echo "broken: trace2tla.py: $(head -1 "$work/refused")"; rm -rf "$work"; return ;;
  esac
  fit=none
  # TLC once more when it ends with neither outcome: a run of it that does
  # not start is no verdict on the trace, and an error of the model's comes
  # back the second time.
  for run in 1 2; do
    [ -n "$name" ] || break
    java -XX:+UseParallelGC -cp "$TLA2TOOLS" tlc2.TLC -workers 1 -metadir "$work/states$run" \
      -config "$work/$name.cfg" "$work/$name.tla" > "$work/tlc.log" 2>&1 < /dev/null || true
    if grep -q "Invariant NotFinished is violated" "$work/tlc.log"; then
      fit=fits; break
    elif grep -q "Deadlock reached" "$work/tlc.log"; then
      fit=unfit; break
    else
      fit="broken: $(grep -E '^Error' "$work/tlc.log" | head -1)"
    fi
  done
  if [ "$status" -eq 3 ]; then
    case $fit in
      fits|none) printf 'refused\t%s\n' "$(cat "$work/refused")" ;;
      *) printf 'refused, its steps before %s\t%s\n' "$fit" "$(cat "$work/refused")" ;;
    esac
  else
    echo "$fit"
  fi
  rm -rf "$work"
}

# The recordings `make traces` made into the directory $1 that no trace
# under traces/ covers. Each is listed in traces/uncovered.tsv, a line a
# recording: its file, `refused` or `unfit`, and why, tab-separated. A
# `refused` one's reason is trace2tla.py's, word for word: what the
# recording holds that the model does not describe yet; its steps before
# the refusal fit. An `unfit` one has a step TLC refuses, before any
# refusal of trace2tla.py's; its reason names the record that owes its
# diagnosis. A recording neither covered nor listed fails, and so does an
# entry whose recording no test made, that is covered, or that stands
# otherwise than its entry says: one that fits now, or is refused for
# another reason.
check_uncovered() {
  list=traces/uncovered.tsv
  bad=0
  tab=$(printf '\t')
  for made in "$1"/*.jsonl; do
    [ -e "$made" ] || continue
    base=${made##*/}
    [ -f "traces/$base" ] && continue
    stands=$(verdict "$made")
    listed=$(awk -F '\t' -v n="$base" '$1 == n { print $2 "\t" $3 }' "$list")
    case $listed in
      "") echo "traces: $base is not covered and not listed; it stands: $stands"; bad=1 ;;
      unfit*) case $stands in
          unfit|"refused, its steps before unfit$tab"*) ;;
          *) echo "traces: $base is listed as not fitting, and stands: $stands"; bad=1 ;;
        esac ;;
      *) [ "$stands" = "$listed" ] || {
        echo "traces: $base is listed as: $listed"; echo "   and stands: $stands"; bad=1; } ;;
    esac
  done
  while IFS="$tab" read -r base kind reason; do
    case $base in ''|'#'*) continue ;; esac
    [ -f "$1/$base" ] || { echo "traces: $base is listed, and no test recorded it"; bad=1; }
    [ ! -f "traces/$base" ] || { echo "traces: $base is listed, and covered"; bad=1; }
  done < "$list"
  return "$bad"
}

# A configuration that must fail, and the property it must break.
must_fail() {
  log="${TMPDIR:-/tmp}/tlc-$1.log"
  if tlc -config "$1.cfg" "$2" > "$log" 2>&1; then
    echo "unexpected: $1 broke nothing"; exit 1
  fi
  grep -q "$3" "$log" || { echo "unexpected: $1 broke something else:"; grep -E "^Error" "$log" | head -3; exit 1; }
  echo "   $3, as expected"
}

# A configuration that must hold, and the count of distinct states TLC
# finds in it: a count that moves says the model or its instance changed,
# which its author re-reads before pinning the new one.
must_hold() {
  log="${TMPDIR:-/tmp}/tlc-$1.log"
  if ! tlc -config "$1.cfg" "$2" > "$log" 2>&1; then
    echo "unexpected: $1 broke:"; grep -E "^Error" "$log" | head -3; exit 1
  fi
  found=$(grep -oE "[0-9]+ distinct states found" "$log" | tail -1 | cut -d' ' -f1)
  [ "$found" = "$3" ] || { echo "unexpected: $1 found $found distinct states, pinned $3"; exit 1; }
  echo "   holds, $found distinct states"
}

# A refuted design that must break the one property its configuration
# names, and its twin -- the same configuration with the decided design,
# Mutant "none" -- which must hold it: so the property, not another, tells
# the two apart.
must_fail_with_twin() {
  must_fail "$1" "$2" "$3"
  work=$(mktemp -d)
  sed 's/^  Mutant = .*/  Mutant = "none"/' "$1.cfg" > "$work/$1Twin.cfg"
  log="${TMPDIR:-/tmp}/tlc-$1Twin.log"
  if ! tlc -config "$work/$1Twin.cfg" "$2" > "$log" 2>&1; then
    echo "unexpected: $1's twin broke:"; grep -E "^Error" "$log" | head -3; exit 1
  fi
  found=$(grep -oE "[0-9]+ distinct states found" "$log" | tail -1 | cut -d' ' -f1)
  [ "$found" = "$4" ] || { echo "unexpected: $1's twin found $found distinct states, pinned $4"; exit 1; }
  echo "   its twin holds, $found distinct states"
  rm -rf "$work"
}

# The checks fall into groups, each a function below, in the order a full
# run takes them: `check.sh` runs every group, `check.sh <group> ...` those
# alone, and `check.sh --groups` names them, one a line. A pipeline runs
# each group as a job of its own (.github/workflows/proofs.yml): the whole
# outlives a hosted job, and no group may. So the groups follow the models,
# except where one would grow too long: the rotation beside a dependency is
# a group of its own for its length alone. The traces are one group, the
# one `make traces` runs after recording them again.
GROUPS="outbox inbox inbox-pg inbox-pg-rotation inbox-pg-rotation-deps inbox-outage bridge scheduler traces"

group_outbox() {
  echo "== outbox: with the visibility rule, every property holds"
  tlc -config Outbox.cfg Outbox.tla

  echo "== outbox: without the rule, TLC must find a message passed over"
  if tlc -config OutboxNoRule.cfg Outbox.tla > "${TMPDIR:-/tmp}/tlc-outbox-norule.log" 2>&1; then
    echo "unexpected: no violation without the visibility rule"; exit 1
  fi
  grep -q "Invariant NoPassedOver is violated" "${TMPDIR:-/tmp}/tlc-outbox-norule.log"
  echo "   violation found, as expected"

  echo "== outbox: with the assignment of URIs to slots changing under the positions, TLC must find a message passed over"
  if tlc -config OutboxRehash.cfg Outbox.tla > "${TMPDIR:-/tmp}/tlc-outbox-rehash.log" 2>&1; then
    echo "unexpected: no violation when rehashing under the positions"; exit 1
  fi
  grep -q "Invariant NoPassedOver is violated" "${TMPDIR:-/tmp}/tlc-outbox-rehash.log"
  echo "   violation found, as expected"


  echo "== outbox, statement by statement: the relay of ADR-0035 as decided, every property holds"
  tlc -config OutboxPg.cfg MCOutboxPg.tla
  echo "== outbox, statement by statement: rewinds as decided, every property holds"
  tlc -config OutboxPgRewind.cfg MCOutboxPg.tla
  echo "== outbox, statement by statement: a transaction open for ever holds no other row back"
  tlc -config OutboxPgLive.cfg MCOutboxPg.tla
  echo "== outbox, statement by statement: in lanes, the inbox's flows while the broker is down"
  tlc -config OutboxPgLanes.cfg MCOutboxPg.tla
  echo "== outbox, statement by statement: the migration, an old relay beside a new one, loses nothing"
  tlc -config OutboxPgMigrate.cfg MCOutboxPg.tla
  echo "== outbox, statement by statement: the migration, the old relay stopping, hands nothing on twice"
  tlc -config OutboxPgMigrateExact.cfg MCOutboxPg.tla
  echo "== outbox, statement by statement: ADR-0033's ring loses nothing, and both bounds hold"
  tlc -config OutboxPgRing.cfg MCOutboxPg.tla
  echo "== outbox, statement by statement: the ring with a rewind, refused below intact_from"
  tlc -config OutboxPgRingRewind.cfg MCOutboxPg.tla
  echo "== outbox, statement by statement: the ring with a group created later"
  tlc -config OutboxPgRingLate.cfg MCOutboxPg.tla
  echo "== outbox, statement by statement: the ring without \"closed\", or with \"passed\" read first, still loses nothing: the recheck keeps the rows"
  tlc -config OutboxPgRingNoClosed.cfg MCOutboxPg.tla
  tlc -config OutboxPgRingPassedFirst.cfg MCOutboxPg.tla
  for case in \
    "OutboxPgMixedDown:Temporal properties were violated" \
    "OutboxPgXmin:Invariant VersionOrder is violated" \
    "OutboxPgXminOpen:Temporal properties were violated" \
    "OutboxPgOneStatement:Invariant NoPassedOver is violated" \
    "OutboxPgByXid:Invariant VersionOrder is violated" \
    "OutboxPgNoXip:Invariant NoPassedOver is violated" \
    "OutboxPgRange:Invariant NoPassedOver is violated" \
    "OutboxPgUnfiltered:Invariant NoRedeliveryWithoutFailure is violated" \
    "OutboxPgUnbounded:Invariant VersionOrder is violated" \
    "OutboxPgDrainNow:Invariant NoPassedOver is violated" \
    "OutboxPgOffsetCarried:Invariant NoPassedOver is violated" \
    "OutboxPgNaiveRewind:Invariant NoPassedOver is violated" \
    "OutboxPgRewindKeepsEpoch:Invariant RewindReplays is violated" \
    "OutboxPgDrawEarly:Invariant VersionOrder is violated" \
    "OutboxPgCache2:Invariant VersionOrder is violated" \
    "OutboxPgMigrateDelivered:Invariant NoRedeliveryWithoutFailure is violated" \
    "OutboxPgMigrateTooFar:Invariant NoPassedOver is violated" \
    "OutboxPgMigrateShared:Invariant NoPassedOver is violated" \
    "OutboxPgRingNoRecheck:Invariant NoPositionBelowGone is violated" \
    "OutboxPgRingPassedFirstNoRecheck:Invariant NoPositionBelowGone is violated" \
    "OutboxPgRingNamesEarly:Invariant NoPassedOver is violated" \
    "OutboxPgRingOneBound:Invariant IntactFromSound is violated" \
    "OutboxPgRingNewGroupNaive:Invariant NoPassedOver is violated" \
    "OutboxPgRingRewindBelow:Invariant NoPositionBelowGone is violated" \
    "OutboxPgRingNoRecord:Invariant NoPassedOver is violated"; do
    echo "== outbox, statement by statement: ${case%%:*}, a refuted design, must fail"
    must_fail "${case%%:*}" MCOutboxPg.tla "${case#*:}"
  done
}

group_inbox() {
  echo "== inbox: as implemented, every property holds"
  tlc -config Inbox.cfg MCInbox.tla

  echo "== inbox: a dependency that never arrives leaves its message waiting aside; the rest still flows"
  tlc -config InboxMissingDep.cfg MCInbox.tla

  echo "== inbox: with waits that run out, every received message ends up processed or parked"
  tlc -config InboxMissingDepExpires.cfg MCInbox.tla

  echo "== inbox: a dependency that arrives after the wait ran out wakes the dependent, every property holds"
  tlc -config InboxLateDep.cfg MCInbox.tla

  echo "== inbox: an expiry forgetting the identity waited for, TLC must find the dependent parked for good"
  must_fail InboxLateDepForgets MCInbox.tla "Temporal properties were violated"

  echo "== inbox: without setting a message aside for its dependency, TLC must find the slot held"
  if tlc -config InboxNoWaiting.cfg MCInbox.tla > "${TMPDIR:-/tmp}/tlc-inbox-nowaiting.log" 2>&1; then
    echo "unexpected: no violation without waiting"; exit 1
  fi
  grep -q "Temporal properties were violated" "${TMPDIR:-/tmp}/tlc-inbox-nowaiting.log"
  echo "   violation found, as expected"

  echo "== inbox: a poison message is parked after its attempts and the slot flows"
  tlc -config InboxPoison.cfg MCInbox.tla

  echo "== inbox: a row the dispatcher cannot read is parked at once and the slot flows"
  tlc -config InboxUnreadable.cfg MCInbox.tla

  echo "== inbox: without the park at once, TLC must find the slot held by the unreadable row"
  must_fail InboxUnreadableHolds MCInbox.tla "Temporal properties were violated"

  echo "== inbox: without parking, TLC must find the slot held for ever"
  if tlc -config InboxPoisonNoParking.cfg MCInbox.tla > "${TMPDIR:-/tmp}/tlc-inbox-poison.log" 2>&1; then
    echo "unexpected: no violation without parking"; exit 1
  fi
  grep -q "Temporal properties were violated" "${TMPDIR:-/tmp}/tlc-inbox-poison.log"
  echo "   violation found, as expected"

  echo "== inbox: stepping over a message in backoff, TLC must find the order lost"
  if tlc -config InboxSkipNotDue.cfg MCInbox.tla > "${TMPDIR:-/tmp}/tlc-inbox-skipnotdue.log" 2>&1; then
    echo "unexpected: no violation when stepping over a message in backoff"; exit 1
  fi
  grep -q "Invariant ArrivalOrder is violated" "${TMPDIR:-/tmp}/tlc-inbox-skipnotdue.log"
  echo "   violation found, as expected"

  echo "== inbox: with the check of a dependency and the wait two steps apart, TLC must find a message waiting for a processed dependency"
  if tlc -config InboxSplitWait.cfg MCInbox.tla > "${TMPDIR:-/tmp}/tlc-inbox-splitwait.log" 2>&1; then
    echo "unexpected: no violation with the wait settled after the check"; exit 1
  fi
  grep -q "Invariant WaitingIsAside is violated" "${TMPDIR:-/tmp}/tlc-inbox-splitwait.log"
  echo "   violation found, as expected"

  echo "== inbox, ADR-0041: a slot serving its Tenants in turn, every property holds, and while a message is a due head its slot takes at most one head of each other Tenant"
  tlc -config InboxRotation.cfg MCInboxRotation.tla
  echo "== inbox, ADR-0041: the same over three Tenants in one slot, every property holds"
  tlc -config InboxRotationThree.cfg MCInboxThree.tla
  echo "== inbox, ADR-0042 decision 5: the same with guests that skip a Tenant, walking from a cursor of their own: a head no guest skips is overtaken at most twice by each other Tenant"
  must_hold InboxRotationGuest MCInboxThree.tla 57367
  echo "== inbox, ADR-0042: guests without a cursor of their own, TLC must find a head overtaken more than twice"
  must_fail InboxRotationGuestFromLowest MCInboxThree.tla "Invariant BoundedOvertaking is violated"
  echo "== inbox, ADR-0041: rotating, a poison message parked after its attempts, every property holds"
  tlc -config InboxRotationPoison.cfg MCInboxRotation.tla
  echo "== inbox, ADR-0041 beside ADR-0010: rotating, a permanent verdict parks its message at once, takes the cursor nowhere, every property holds"
  tlc -config InboxRotationPermanent.cfg MCInboxRotation.tla
  echo "== inbox, ADR-0041: rotating, a poison message never parked holds its own Tenant's share of the slot and nothing else"
  tlc -config InboxRotationPoisonNoParking.cfg MCInbox.tla
  echo "== inbox, ADR-0041: a slot as one FIFO across Tenants, TLC must find a due head waiting for more than one head of another Tenant"
  must_fail InboxFifoSlot MCInboxRotation.tla "Invariant BoundedOvertaking is violated"
  echo "== inbox, ADR-0041: a rotation whose cursor is never written, TLC must find a due head waiting for two heads of another Tenant"
  must_fail InboxCursorStays MCInboxRotation.tla "Invariant BoundedOvertaking is violated"
  echo "== inbox, ADR-0041: as a FIFO, TLC must find another Tenant's message held for ever behind a poison head"
  must_fail InboxPoisonHoldsSlot MCInbox.tla "Temporal properties were violated"
  echo "== inbox, ADR-0041: a rotation taking a Tenant's newest row, TLC must find the order of arrival lost"
  must_fail InboxNewestOfTenant MCInbox.tla "Invariant ArrivalOrder is violated"
  echo "== inbox, ADR-0041: a walk by Tenant number that skips only the cursor's Tenant, TLC must find a due head overtaken twice by one Tenant"
  must_fail InboxWalkFromLowest MCInboxThree.tla "Invariant BoundedOvertaking is violated"

  echo "== inbox forgetting by its window (ADR-0033 decision 4): every property holds"
  tlc -config InboxForget.cfg MCInboxForget.tla

  echo "== inbox: a window shorter than the horizon of repeats, TLC must find a message processed twice"
  if tlc -config InboxForgetEarly.cfg MCInboxForget.tla > "${TMPDIR:-/tmp}/tlc-inbox-forget-early.log" 2>&1; then
    echo "unexpected: no repeat processed twice"; exit 1
  fi
  grep -q "Invariant EffectsOnce is violated" "${TMPDIR:-/tmp}/tlc-inbox-forget-early.log"
  echo "   violation found, as expected"

  echo "== inbox: a window over dependency targets, TLC must find a wait nothing will wake"
  if tlc -config InboxForgetTarget.cfg MCInboxForget.tla > "${TMPDIR:-/tmp}/tlc-inbox-forget-target.log" 2>&1; then
    echo "unexpected: no wait for a forgotten dependency"; exit 1
  fi
  grep -q "Invariant WaitingIsAsideEver is violated" "${TMPDIR:-/tmp}/tlc-inbox-forget-target.log"
  echo "   violation found, as expected"
}

group_inbox_pg() {
  echo "== inbox, statement by statement: as implemented, every step refines Inbox.tla and every property holds"
  tlc -config InboxPg.cfg MCInboxPg.tla

  echo "== inbox, statement by statement: with waits that run out beside the dispatchers, every step refines Inbox.tla and every property holds"
  tlc -config InboxPgExpires.cfg MCInboxPg.tla

  echo "== inbox, statement by statement: an expiry clearing the identity waited for, TLC must find a message never processed"
  must_fail InboxPgExpiresForgets MCInboxPg.tla "Temporal properties were violated"

  echo "== inbox, statement by statement: without the advisory lock, TLC must find a message waiting for a processed dependency"
  if tlc -config InboxPgNoLock.cfg MCInboxPg.tla > "${TMPDIR:-/tmp}/tlc-inboxpg-nolock.log" 2>&1; then
    echo "unexpected: no lost wake without the lock"; exit 1
  fi
  grep -q "Invariant WaitingIsAside is violated" "${TMPDIR:-/tmp}/tlc-inboxpg-nolock.log"
  echo "   violation found, as expected"

  echo "== inbox, statement by statement: with the head read in the take's statement, TLC must find a dispatcher stopped by a slot without a head"
  if tlc -config InboxPgHeadInTake.cfg MCInboxPg.tla > "${TMPDIR:-/tmp}/tlc-inboxpg-headintake.log" 2>&1; then
    echo "unexpected: no stale head with the head read in the take"; exit 1
  fi
  grep -q "Invariant NoDispatcherDied is violated" "${TMPDIR:-/tmp}/tlc-inboxpg-headintake.log"
  echo "   violation found, as expected"

  echo "== inbox, statement by statement: walking on after a set-aside with the lock held, TLC must find two dispatchers waiting for each other"
  if tlc -config InboxPgWalkOn.cfg MCInboxPg.tla > "${TMPDIR:-/tmp}/tlc-inboxpg-walkon.log" 2>&1; then
    echo "unexpected: no lock cycle when walking on"; exit 1
  fi
  grep -q "Invariant NoDeadlock is violated" "${TMPDIR:-/tmp}/tlc-inboxpg-walkon.log"
  echo "   violation found, as expected"
}

group_inbox_pg_rotation() {
  echo "== inbox, statement by statement, ADR-0041: the walk bounded by K, its rounds and the dormancy that ends them, with failures and inserts committed out of order; every step refines the rotating Inbox.tla and every property holds"
  tlc -config InboxPgRotation.cfg MCInboxPgRotation.tla
  echo "== inbox, statement by statement, ADR-0041: the same with a walk that examines every Tenant, a round of one walk"
  tlc -config InboxPgRotationWide.cfg MCInboxPgRotation.tla
  echo "== inbox, statement by statement, ADR-0041: without max_dormancy every message is still processed; the cap bounds a wait in time, and liveness does not rest on it"
  tlc -config InboxPgRotationNoCap.cfg MCInboxPg.tla
  echo "== inbox, statement by statement, ADR-0041: the same with a dormancy that the backoffs its round saw do not end, TLC must find a message never processed"
  must_fail InboxPgSleepsPastBackoff MCInboxPg.tla "Temporal properties were violated"
  echo "== inbox, statement by statement, ADR-0041: dormant_after read at the walk that ends the round, TLC must find a slot asleep past a due head"
  must_fail InboxPgDormantAfterAtWalk MCInboxPgRotation.tla "Invariant NoDueHeadAsleep is violated"
  echo "== inbox, statement by statement, ADR-0041: a round that never ends, TLC must find its walks passing without bound"
  must_fail InboxPgNoRoundEnd MCInboxPgRotation.tla "Invariant PassedPerRound is violated"
  echo "== inbox, statement by statement, ADR-0041: a walk that finds nothing and leaves the cursor, TLC must find its walks passing without bound"
  must_fail InboxPgPassedStays MCInboxPgRotation.tla "Invariant PassedPerRound is violated"
  echo "== inbox, statement by statement, ADR-0041: a walk taking its Tenant's newest row, TLC must find the order of arrival lost"
  must_fail InboxPgNewestOfTenant MCInboxPg.tla "Invariant ArrivalOrder is violated"
}

group_inbox_pg_rotation_deps() {
  echo "== inbox, statement by statement, ADR-0041: rotating beside a dependency, two loops; every step refines the rotating Inbox.tla"
  tlc -config InboxPgRotationDeps.cfg MCInboxPg.tla
}

# InboxOutage.tla is ADR-0042's: (slot, Tenant) heads, processes with
# workers, a breaker and a memo each, an ask split into its start and its
# verdict with a rollback between, a process cut off and keys cached; the
# expiry's spare and the operator's verbs; and, with a dependency registered,
# its gate, opened and closed by checks of the dependency.
group_inbox_outage() {
  echo "== inbox outage (ADR-0042): the stage down once and back, a process cut off once, a Tenant's keys cached, a row failing once, a rollback: an Unavailable spends nothing and never parks, holds stay queued, causes current, keys in order, every row processed"
  must_hold InboxOutage MCInboxOutage.tla 191126
  echo "== inbox outage: a Tenant failing alone, its rows in two slots, two workers of one process: one ask of it at a time once remembered, every row processed"
  must_hold InboxOutageTenant MCInboxOutage.tla 1936
  echo "== inbox outage: a process cut off for good: it pauses, and the other processes every row"
  must_hold InboxOutageCutOff MCInboxOutage.tla 2336
  echo "== inbox outage: a waiter on a queued row is spared and processed, one on a row that never arrives expires"
  must_hold InboxOutageDeps MCInboxOutage.tla 36
  echo "== inbox outage: the operator re-arming with a canary, giving heads and a queue up: the canary exact, what is given up the end of each key"
  must_hold InboxOutageOperators MCInboxOutage.tla 830194
  echo "== inbox outage, the stage's dependency registered: the stage down once and back, a process cut off once, a Tenant's keys cached, a rollback: the gate closes on its own evidence, and once the stage is up; every row processed"
  must_hold InboxOutageDependency MCInboxOutage.tla 275296
  echo "== inbox outage, the stage's dependency registered: a process cut off for good: its gate stays open, and the other processes every row"
  must_hold InboxOutageDependencyCutOff MCInboxOutage.tla 4484
  for case in \
    "InboxOutageUnavailableSpends:Action property UnavailableNeverSpends is violated:55966" \
    "InboxOutageNoFlightMark:Action property OneAskPerReach is violated:1936" \
    "InboxOutageExpirePresent:Action property PresentDependencyNeverExpires is violated:36" \
    "InboxOutageMarkKeepsCause:Invariant CauseIsCurrent is violated:93044" \
    "InboxOutageMarkKeepsHold:Invariant HoldIsQueued is violated:55966" \
    "InboxOutageCanaryKeeps:Invariant CanaryExact is violated:830194" \
    "InboxOutageQueueOldest:Invariant GivenUpIsTheEnd is violated:228" \
    "InboxOutageRearmBesideAnAsk:Invariant KeyOrder is violated:830194" \
    "InboxOutageStageHeldUnasked:Invariant UnaskedHoldIsTheTenants is violated:2336" \
    "InboxOutageCachedCheckCloses:Action property GateClosesOnItsEvidence is violated:42674" \
    "InboxOutageCheckNeedsAHead:Temporal properties were violated:120959"; do
    name=${case%%:*}; rest=${case#*:}; property=${rest%:*}; twin=${rest##*:}
    echo "== inbox outage: $name, a refuted design, must fail, and its twin hold"
    must_fail_with_twin "$name" MCInboxOutage.tla "$property" "$twin"
  done
}

group_bridge() {
  echo "== bridge: outbox -> inbox with crashes on both sides, every property holds"
  tlc -config Bridge.cfg MCBridge.tla

  echo "== bridge: acknowledging before the store, TLC must find a committed message lost"
  if tlc -config BridgeAckFirst.cfg MCBridge.tla > "${TMPDIR:-/tmp}/tlc-bridge-ackfirst.log" 2>&1; then
    echo "unexpected: no loss when acknowledging before the store"; exit 1
  fi
  grep -q "Temporal properties were violated" "${TMPDIR:-/tmp}/tlc-bridge-ackfirst.log"
  echo "   violation found, as expected"

  echo "== bridge: an unstorable message acknowledged without being set aside, TLC must find it set nowhere"
  must_fail BridgeAckWithoutSetAside MCBridge.tla "Temporal properties were violated"

  echo "== bridge: a message set aside because the inbox's database holds, TLC must find a storable one diverted"
  must_fail BridgeSetAsideOnHold MCBridge.tla "Invariant SetAsideOnlyUnstorable is violated"

  echo "== bridge: acknowledged before the row set aside commits, TLC must find a message set nowhere"
  must_fail BridgeAckBeforeReject MCBridge.tla "Temporal properties were violated"
}

group_scheduler() {
  echo "== scheduler: the window and a captured value, as decided, every property holds"
  must_hold SchedulerWindow MCScheduler.tla 556786

  echo "== scheduler: a barrier and provenance, as decided, every property holds"
  must_hold SchedulerBarrier MCScheduler.tla 115196

  echo "== scheduler: dependencies bound at release, TLC must find one the window let go bound to nothing"
  must_fail_with_twin SchedulerBindAtRelease MCScheduler.tla "Invariant BoundAsChecked is violated" 556786

  echo "== scheduler: the window evicted by the clock, TLC must find a late reader's dependency bound to nothing"
  must_fail_with_twin SchedulerEvictByClock MCScheduler.tla "Invariant BoundAsChecked is violated" 1665393

  echo "== scheduler: a cell freed as the window lets it go, TLC must find a waiting reader reading it"
  must_fail_with_twin SchedulerFreeOnEvict MCScheduler.tla "Invariant NoUseAfterFree is violated" 556786

  echo "== scheduler: released at ready + think, TLC must find a step released before its plan"
  must_fail_with_twin SchedulerReadyWithoutAt MCScheduler.tla "Invariant ReleaseSafe is violated" 556786

  echo "== scheduler: a barrier counting its members, TLC must find its holder released before a member"
  must_fail_with_twin SchedulerCounterBarrier MCScheduler.tla "Invariant BarrierSafe is violated" 115196

  echo "== scheduler: a failed dependency skipping nothing, TLC must find a read line that never ends"
  must_fail_with_twin SchedulerNoSkipPropagation MCScheduler.tla "Invariant EveryReadLineEnds is violated" 115196

  echo "== scheduler: a fold marking nothing conditional, TLC must find a conditional step it missed"
  must_fail_with_twin SchedulerNoProvenance MCScheduler.tla "Invariant ConditionalExact is violated" 115196

  echo "== scheduler: a fold marking direct dependents only, TLC must find a step conditional through another"
  must_fail_with_twin SchedulerProvenanceOneLevel MCScheduler.tla "Invariant ConditionalExact is violated" 115196

  echo "== scheduler: the machine's own timer at the deadline, TLC must find an answer stamped by it censored"
  must_fail_with_twin SchedulerDeadlineByArrival MCScheduler.tla "Invariant NoCensored is violated" 115196

  echo "== scheduler: a failed prefix swept again, as the machine did before K1e, TLC must find the failed member's freed cell read"
  must_fail_with_twin SchedulerResweepFailed MCScheduler.tla "Invariant NoUseAfterFree is violated" 115196

  echo "== scheduler: a holder walking its members' cells, as the machine did before K1e, TLC must find a freed member read"
  must_fail_with_twin SchedulerWalkMembers MCScheduler.tla "Invariant NoUseAfterFree is violated" 115196

  echo "== scheduler: the plan's own bound, its tables the validator's, every property holds"
  must_hold SchedulerBound MCScheduler.tla 468858

  echo "== scheduler: the bound by rate alone, TLC must find more held than it counts"
  must_fail_with_twin SchedulerPendingByRate MCScheduler.tla "Invariant HeldWithinBound is violated" 468858

  echo "== scheduler: the bound without its pins, TLC must find a producer held past it"
  must_fail_with_twin SchedulerBoundWithoutPins MCScheduler.tla "Invariant HeldWithinBound is violated" 468858

  echo "== scheduler: the bound without its key's prefix, TLC must find a member held past it"
  must_fail_with_twin SchedulerBoundWithoutPrefix MCScheduler.tla "Invariant HeldWithinBound is violated" 468858

  echo "== scheduler: the bound without the read-ahead, TLC must find lines read early held past it"
  must_fail_with_twin SchedulerBoundWithoutReadAhead MCScheduler.tla "Invariant HeldWithinBound is violated" 468858
}

group_traces() {
  echo "== outbox: recorded runs of the tests fit the model"
  for trace in traces/outbox-*.jsonl; do
    check_trace "$trace"
  done

  echo "== outbox: a forged run, fetching by position alone, must not fit"
  if check_trace traces/forged/outbox-passed-over.jsonl > "${TMPDIR:-/tmp}/tlc-outbox-forged.log" 2>&1; then
    echo "unexpected: the forged run fits the model"; exit 1
  fi
  grep -q "Deadlock reached" "${TMPDIR:-/tmp}/tlc-outbox-forged.log" || { cat "${TMPDIR:-/tmp}/tlc-outbox-forged.log"; exit 1; }
  echo "   refused, as expected"

  echo "== inbox: recorded runs of the tests fit the model"
  for trace in traces/inbox-*.jsonl; do
    check_trace "$trace"
  done

  echo "== inbox: a forged run, taking a message before its dependency, must not fit"
  if check_trace traces/forged/inbox-ignores-dependency.jsonl > "${TMPDIR:-/tmp}/tlc-inbox-forged.log" 2>&1; then
    echo "unexpected: the forged run fits the model"; exit 1
  fi
  grep -q "Deadlock reached" "${TMPDIR:-/tmp}/tlc-inbox-forged.log" || { cat "${TMPDIR:-/tmp}/tlc-inbox-forged.log"; exit 1; }
  echo "   refused, as expected"

  echo "== inbox: a forged run, fetching a row its snapshot could not see, must not fit"
  if check_trace traces/forged/inbox-fetch-beyond-snapshot.jsonl > "${TMPDIR:-/tmp}/tlc-inbox-forged-snapshot.log" 2>&1; then
    echo "unexpected: the forged run fits the model"; exit 1
  fi
  grep -q "Deadlock reached" "${TMPDIR:-/tmp}/tlc-inbox-forged-snapshot.log" || { cat "${TMPDIR:-/tmp}/tlc-inbox-forged-snapshot.log"; exit 1; }
  echo "   refused, as expected"

  echo "== scheduler: runs of the machine in sim fit the model"
  for trace in traces/scheduler-*.jsonl; do
    check_trace "$trace"
  done

  echo "== scheduler: a forged run, a step sent before the one it awaits settled, must not fit"
  if check_trace traces/forged/scheduler-sent-before-settled.jsonl > "${TMPDIR:-/tmp}/tlc-scheduler-forged.log" 2>&1; then
    echo "unexpected: the forged run fits the model"; exit 1
  fi
  grep -q "Deadlock reached" "${TMPDIR:-/tmp}/tlc-scheduler-forged.log" || { cat "${TMPDIR:-/tmp}/tlc-scheduler-forged.log"; exit 1; }
  echo "   refused, as expected"

  echo "== scheduler: a forged journal, a barrier's holder released before its last member settled, must not fit"
  if check_trace traces/forged/scheduler-holder-before-member.jsonl > "${TMPDIR:-/tmp}/tlc-scheduler-forged-journal.log" 2>&1; then
    echo "unexpected: the forged run fits the model"; exit 1
  fi
  grep -q "Deadlock reached" "${TMPDIR:-/tmp}/tlc-scheduler-forged-journal.log" || { cat "${TMPDIR:-/tmp}/tlc-scheduler-forged-journal.log"; exit 1; }
  echo "   refused, as expected"

  echo "== bridge: a recorded run of an outbox feeding an inbox fits the model"
  for trace in traces/bridge-*.jsonl; do
    check_trace "$trace"
  done

  echo "== bridge: a forged run, storing after the acknowledgement, must not fit"
  if check_trace traces/forged/bridge-ack-before-store.jsonl > "${TMPDIR:-/tmp}/tlc-bridge-forged.log" 2>&1; then
    echo "unexpected: the forged run fits the model"; exit 1
  fi
  grep -q "Deadlock reached" "${TMPDIR:-/tmp}/tlc-bridge-forged.log" || { cat "${TMPDIR:-/tmp}/tlc-bridge-forged.log"; exit 1; }
  echo "   refused, as expected"

  echo "== bridge: a forged run, handling a message before its rejection, must not fit"
  if check_trace traces/forged/bridge-ack-before-reject.jsonl > "${TMPDIR:-/tmp}/tlc-bridge-forged-reject.log" 2>&1; then
    echo "unexpected: the forged run fits the model"; exit 1
  fi
  grep -q "Deadlock reached" "${TMPDIR:-/tmp}/tlc-bridge-forged-reject.log" || { cat "${TMPDIR:-/tmp}/tlc-bridge-forged-reject.log"; exit 1; }
  echo "   refused, as expected"
}

if [ "${1:-}" = --groups ]; then
  printf '%s\n' $GROUPS
  exit 0
fi
if [ "${1:-}" = --uncovered ]; then
  check_uncovered "$2"
  exit 0
fi
[ $# -gt 0 ] || set -- $GROUPS
for group in "$@"; do
  case " $GROUPS " in
    *" $group "*) ;;
    *) echo "check.sh: no group '$group'; the groups are: $GROUPS" >&2; exit 2 ;;
  esac
done
for group in "$@"; do
  echo "#### $group"
  "group_$(printf '%s' "$group" | tr - _)"
done
