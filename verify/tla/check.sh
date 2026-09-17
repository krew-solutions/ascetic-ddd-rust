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

echo "== inbox: as implemented, every property holds"
tlc -config Inbox.cfg MCInbox.tla

echo "== inbox: a dependency that never arrives leaves its message waiting aside; the rest still flows"
tlc -config InboxMissingDep.cfg MCInbox.tla

echo "== inbox: with waits that run out, every received message ends up processed or parked"
tlc -config InboxMissingDepExpires.cfg MCInbox.tla

echo "== inbox: without setting a message aside for its dependency, TLC must find the slot held"
if tlc -config InboxNoWaiting.cfg MCInbox.tla > "${TMPDIR:-/tmp}/tlc-inbox-nowaiting.log" 2>&1; then
  echo "unexpected: no violation without waiting"; exit 1
fi
grep -q "Temporal properties were violated" "${TMPDIR:-/tmp}/tlc-inbox-nowaiting.log"
echo "   violation found, as expected"

echo "== inbox: a poison message is parked after its attempts and the slot flows"
tlc -config InboxPoison.cfg MCInbox.tla

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

echo "== inbox, statement by statement: as implemented, every step refines Inbox.tla and every property holds"
tlc -config InboxPg.cfg MCInboxPg.tla

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

echo "== bridge: outbox -> inbox with crashes on both sides, every property holds"
tlc -config Bridge.cfg MCBridge.tla

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

echo "== bridge: acknowledging before the store, TLC must find a committed message lost"
if tlc -config BridgeAckFirst.cfg MCBridge.tla > "${TMPDIR:-/tmp}/tlc-bridge-ackfirst.log" 2>&1; then
  echo "unexpected: no loss when acknowledging before the store"; exit 1
fi
grep -q "Temporal properties were violated" "${TMPDIR:-/tmp}/tlc-bridge-ackfirst.log"
echo "   violation found, as expected"
