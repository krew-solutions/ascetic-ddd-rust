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
  cp Outbox.tla TraceOutbox.tla Inbox.tla TraceInbox.tla Bridge.tla TraceBridge.tla "$work"
  name=$(python3 trace2tla.py "$1" "$work")
  log="$work/tlc.log"
  if java -XX:+UseParallelGC -cp "$TLA2TOOLS" tlc2.TLC -workers 1 -metadir "$work/states" -config "$work/$name.cfg" "$work/$name.tla" > "$log" 2>&1; then
    echo "   $1: TLC found nothing, which cannot happen; see $log"; return 1
  fi
  if grep -q "Invariant NotFinished is violated" "$log"; then
    echo "   $1: fits"; rm -rf "$work"; return 0
  fi
  echo "   $1: does not fit the model:"; grep -E "^Error|^/\\\\ i = " "$log" | head -4; return 1
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

# The checks fall into groups, each a function below, in the order a full
# run takes them: `check.sh` runs every group, `check.sh <group> ...` those
# alone, and `check.sh --groups` names them, one a line. A pipeline runs
# each group as a job of its own (.github/workflows/proofs.yml): the whole
# outlives a hosted job, and no group may. So the groups follow the models,
# except where one would grow too long: the rotation beside a dependency is
# a group of its own for its length alone. The traces are one group, the
# one `make traces` runs after recording them again.
GROUPS="outbox inbox inbox-pg inbox-pg-rotation inbox-pg-rotation-deps inbox-outage bridge traces"

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

# InboxOutage.tla holds a slot whole for its stage's outage, as the inbox
# did before its slots served their Tenants in turn: since then a hold holds
# its (slot, Tenant), the extent ADR-0040's revision owes (ADR-0040 § Owed).
group_inbox_outage() {
  echo "== inbox outage (ADR-0040), a slot held whole as before ADR-0041: the stage down once, a hold spends nothing, no row parks for it, keys keep their order, every row processed once it is back"
  tlc -config InboxOutage.cfg MCInboxOutage.tla

  echo "== inbox outage: with a hold that spends as any failure, TLC must find a row parked for the stage"
  must_fail InboxOutageCounts MCInboxOutage.tla "Invariant NoStageParks is violated"

  echo "== inbox outage: the stage down for good holds its rows for good, by design"
  must_fail InboxOutageForever MCInboxOutage.tla "Temporal properties were violated"

  echo "== inbox outage, with a Tenant failing alone and the operator: causes stay current, parks have their reason, a re-armed row has its canary"
  tlc -config InboxOutageOperators.cfg MCInboxOutage.tla

  echo "== inbox outage: a mark, or an unpark, that keeps the cause must break CauseIsCurrent; a bulk unpark that resets the attempts, CanaryReparks"
  must_fail InboxOutageCommitKeepingCause MCInboxOutage.tla "Invariant CauseIsCurrent is violated"
  must_fail InboxOutageUnparkKeepingCause MCInboxOutage.tla "Invariant CauseIsCurrent is violated"
  must_fail InboxOutageUnparkAllResetting MCInboxOutage.tla "Invariant CanaryReparks is violated"

  echo "== inbox outage, a Tenant failing alone for a while, read either way: every row ends processed or parked"
  tlc -config InboxOutageTenant.cfg MCInboxOutage.tla

  echo "== inbox outage: a Tenant failing for good, read as the stage's on a channel serving nothing else, holds for good -- the sentinel's and park_held's case"
  must_fail InboxOutageQuietMisread MCInboxOutage.tla "Temporal properties were violated"
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
