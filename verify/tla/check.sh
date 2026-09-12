#!/usr/bin/env sh
# Model-checks every specification under verify/tla with TLC.
# Needs Java and tla2tools.jar; point TLA2TOOLS at the jar, or put it at the
# default path:
#   curl -L -o ~/.local/share/tla/tla2tools.jar \
#     https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar
set -eu
cd "$(dirname "$0")"
TLA2TOOLS="${TLA2TOOLS:-$HOME/.local/share/tla/tla2tools.jar}"
tlc() { java -XX:+UseParallelGC -cp "$TLA2TOOLS" tlc2.TLC -workers auto -deadlock "$@"; }

echo "== outbox: with the visibility rule, every property holds"
tlc -config Outbox.cfg Outbox.tla

echo "== outbox: without the rule, TLC must find a message passed over"
if tlc -config OutboxNoRule.cfg Outbox.tla > "${TMPDIR:-/tmp}/tlc-outbox-norule.log" 2>&1; then
  echo "unexpected: no violation without the visibility rule"; exit 1
fi
grep -q "Invariant NoPassedOver is violated" "${TMPDIR:-/tmp}/tlc-outbox-norule.log"
echo "   violation found, as expected"

echo "== inbox: as implemented, every property holds"
tlc -config Inbox.cfg MCInbox.tla

echo "== inbox: a dependency that never arrives is stepped over; the rest still flows"
tlc -config InboxMissingDep.cfg MCInbox.tla

echo "== inbox: without stepping over, TLC must find the partition blocked"
if tlc -config InboxNoSkip.cfg MCInbox.tla > "${TMPDIR:-/tmp}/tlc-inbox-noskip.log" 2>&1; then
  echo "unexpected: no violation without stepping over"; exit 1
fi
grep -q "Temporal properties were violated" "${TMPDIR:-/tmp}/tlc-inbox-noskip.log"
echo "   violation found, as expected"

echo "== bridge: outbox -> inbox with crashes on both sides, every property holds"
tlc -config Bridge.cfg MCBridge.tla

echo "== bridge: acknowledging before the store, TLC must find a committed message lost"
if tlc -config BridgeAckFirst.cfg MCBridge.tla > "${TMPDIR:-/tmp}/tlc-bridge-ackfirst.log" 2>&1; then
  echo "unexpected: no loss when acknowledging before the store"; exit 1
fi
grep -q "Temporal properties were violated" "${TMPDIR:-/tmp}/tlc-bridge-ackfirst.log"
echo "   violation found, as expected"
