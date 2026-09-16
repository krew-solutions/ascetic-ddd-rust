#!/usr/bin/env python3
"""Turns a recorded outbox or inbox trace into a TLA+ module for its checker.

    trace2tla.py <trace.jsonl> <out dir>

Reads the lines `ascetic-ddd-trace` writes, one JSON object per event, and
writes `<out dir>/<Name>.tla` defining `TraceDef`, the sequence of steps the
model takes, plus `<Name>.cfg`. Prints `<Name>`. Which model is read off the
file name: `outbox-*.jsonl` checks against TraceOutbox.tla, `inbox-*.jsonl`
against TraceInbox.tla, `bridge-*.jsonl`, a run of an outbox feeding an inbox
recorded by one observer, against TraceBridge.tla. Every step names its side,
`outbox`, `inbox` or `bridge`.

The outbox mapping:

    published                 -> publish  (msg, uri, xid, pos)
    fetched, a slot           -> fetch    (d, of, horizon, limit, msgs)
    fetched, no slot          -> nofetch  (group, of, horizon, limit): no slot had work
    handled ok                -> handle   (d, msg)
    handled failed            -> nothing: the subscriber declined
    acked + dispatched batch  -> ack      (d, xid, pos): position and COMMIT, one step
    dispatched nothing        -> nothing: the nofetch already said it
    dispatched rolled_back    -> crash    (d), if a batch was fetched

Messages are named m1, m2, ... in order of publication; a dispatcher is
<<group, slot>>: the slot whose position it holds, whoever runs it.

The inbox mapping:

    received, stored          -> receive   (msg, pos, xid, deps)
    received, duplicate       -> duplicate (msg)
    waiting                   -> wait      (slot, of, msg, dep, snap): set aside for the dependency
    fetched, a row            -> fetch     (slot, of, msg, snap)
    fetched, a slot, no row   -> nofetch   (slot, of, snap): the slot's head waits for its backoff
    fetched, no slot          -> nofetch   (of, snap): no slot had a due head
    handled ok                -> handle    (slot, of, msg)
    handled failed            -> nothing: the subscriber declined
    marked                    -> nothing: the mark commits with the transaction
    failed + dispatched failed
                              -> fail      (slot, of, msg, attempts, parked): the attempt commits
    marked + dispatched processed
                              -> commit    (slot, of, woken): the mark and the rows it woke
    dispatched nothing        -> nothing: the nofetch already said it
    dispatched rolled_back    -> crash     (slot, of), if a row was fetched
    expired                   -> expire    (msg), one per row parked
    unparked                  -> unpark    (msg)
    resolved                  -> resolve   (msg, woken)

Messages are named in order of first mention, a dependency that never arrives
included; a dispatcher is the slot it holds, whoever ran it. A slot has one
holder at a time, so the events between a fetch naming a slot and the close
of a transaction naming it are one dispatch; the close is logged after the
COMMIT, so the next holder's fetch may be logged before it, and the closes of
a slot are matched to its fetches in order.

The bridge mapping is both of the above, with messages named by `message_id`
on both sides and one step of its own:

    inbox received + outbox handled ok of the same message
                              -> forward   (d, msg, dup, pos, xid, deps): Bridge's Forward, at the store
    a handled ok with no store, or a store no handling follows
                              -> handle / receive, as above: not steps of the bridge, refused
"""
import json
import pathlib
import re
import sys


def tla(v):
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, str):
        return '"' + v.replace('"', '\\"') + '"'
    if isinstance(v, (tuple, list)):
        return "<<" + ", ".join(tla(x) for x in v) + ">>"
    if isinstance(v, dict):
        return "[" + ", ".join(f"{k} |-> {tla(x)}" for k, x in v.items()) + "]"
    raise TypeError(v)


class Step(dict):
    """A step: its fields, rendered as a TLA+ record."""

    def __init__(self, fields):
        super().__init__(fields=fields)

    def __str__(self):
        return "[" + ", ".join(f"{k} |-> {tla(v)}" for k, v in self["fields"].items()) + "]"


def record(**fields):
    return Step(fields)


class Names:
    """m1, m2, ... in order of first mention."""

    def __init__(self):
        self.names = {}

    def of(self, key):
        return self.names.setdefault(key, f"m{len(self.names) + 1}")

    def known(self, key, what):
        if key not in self.names:
            raise SystemExit(f"{what} names a message never seen before: {key}")
        return self.names[key]


def outbox_steps(events, names=None):
    names = names or Names()
    pending = {}  # dispatcher -> a batch was fetched and not yet closed
    acked = {}    # dispatcher -> the position acknowledged, awaiting COMMIT

    slots = {}    # group -> how many slots, from the fetches

    for e in events:
        kind = e["event"]
        if kind == "published":
            yield record(side="outbox", event="publish", msg=names.of(e["message_id"]), uri=e["uri"], xid=e["xid"], pos=e["position"])
            continue
        group = e["group"]
        d = (group, e["slot"]) if e.get("slot") is not None else None
        if kind == "fetched":
            slots[group] = e["slots"]
            if d is None:
                yield record(side="outbox", event="nofetch", group=group, of=e["slots"],
                             horizon=e["horizon"], limit=e["limit"])
                continue
            msgs = [names.known(m, "a fetch") for m in e["messages"]]
            pending[d] = bool(msgs)
            yield record(side="outbox", event="fetch", d=d, of=e["slots"],
                         horizon=e["horizon"], limit=e["limit"], msgs=msgs)
        elif d is None:
            pass  # a dispatched-nothing, or a rollback before any slot was taken
        elif kind == "handled":
            if e["ok"]:
                yield record(side="outbox", event="handle", d=d, msg=names.known(e["message_id"], "a handling"))
        elif kind == "acked":
            acked[d] = (e["xid"], e["position"])
        elif kind == "dispatched":
            outcome = e["outcome"]
            if outcome == "batch":
                xid, pos = acked.pop(d)
                yield record(side="outbox", event="ack", d=d, xid=xid, pos=pos)
            elif outcome == "rolled_back":
                acked.pop(d, None)
                if pending.get(d):
                    yield record(side="outbox", event="crash", d=d)
            pending[d] = False
        else:
            raise SystemExit(f"unknown outbox event: {kind}")


def snapshot(e):
    """The snapshot a walk statement ran under, as the model reads it."""
    s = e["snapshot"]
    return {"xmin": s["xmin"], "xmax": s["xmax"], "xip": list(s["xip"])}


def inbox_steps(events, names=None, by_message_id=False):
    """`by_message_id`: name rows by the `message_id` of their metadata, as
    the outbox does, so that a bridge trace names a message once."""
    names = names or Names()
    rows = {}     # row identity -> name
    # how many slots the table is cut into: one number, logged with every fetch
    slots = next((e["slots"] for e in events if e["event"] == "fetched"), 1)
    # slot -> the dispatches that took it and have not been closed, oldest
    # first: each is the row fetched (or None), the failure recorded and the
    # rows the mark woke, all awaiting the close of the transaction
    opened = {}

    def dependency(identity):
        if by_message_id:
            raise SystemExit("a dependency in a bridge trace: rows are named by message_id there, dependencies by identity")
        return names.of(identity)

    def row(identity, what):
        if identity not in rows:
            raise SystemExit(f"{what} names a row never received: {identity}")
        return rows[identity]

    def current(slot, what):
        if not opened.get(slot):
            raise SystemExit(f"{what} names slot {slot}, which no open dispatch holds")
        return opened[slot][-1]

    for e in events:
        kind = e["event"]
        if kind == "unparked":
            yield record(side="inbox", event="unpark", msg=row(e["id"], "an unpark"))
            continue
        if kind == "resolved":
            yield record(side="inbox", event="resolve", msg=row(e["id"], "a resolve"),
                         woken=[row(w, "a waking") for w in e["woken"]])
            continue
        if kind == "expired":
            for identity in e["ids"]:
                yield record(side="inbox", event="expire", msg=row(identity, "an expiry"))
            continue
        if kind == "received":
            if by_message_id:
                if e["message_id"] is None:
                    raise SystemExit(f"a row without a message_id in a bridge trace: {e['id']}")
                rows[e["id"]] = names.of(e["message_id"])
            else:
                rows[e["id"]] = names.of(e["id"])
            msg = rows[e["id"]]
            deps = [names.of(dep) for dep in e["deps"]]
            if e["received_position"] is None:
                yield record(side="inbox", event="duplicate", msg=msg)
            else:
                yield record(side="inbox", event="receive", msg=msg, pos=e["received_position"], xid=e["xid"], deps=deps)
            continue
        slot = e["slot"]
        if kind == "fetched":
            if slot is None:
                yield record(side="inbox", event="nofetch", of=slots, snap=snapshot(e))
                continue
            held = row(e["id"], "a fetch") if e["id"] is not None else None
            opened.setdefault(slot, []).append({"holding": held, "failed": None, "woken": []})
            if held is None:
                yield record(side="inbox", event="nofetch", slot=slot, of=slots, snap=snapshot(e))
            else:
                yield record(side="inbox", event="fetch", slot=slot, of=slots, msg=held, snap=snapshot(e))
        elif kind == "waiting":
            # the wait is inside the dispatch that took the slot, before its fetch
            yield record(side="inbox", event="wait", slot=slot, of=slots, msg=row(e["id"], "a wait"),
                         dep=dependency(e["dependency"]), snap=snapshot(e))
        elif kind == "failed":
            current(slot, "a failure")["failed"] = (e["attempts"], e["parked"])
        elif kind == "handled":
            if e["ok"]:
                yield record(side="inbox", event="handle", slot=slot, of=slots, msg=row(e["id"], "a handling"))
        elif kind == "marked":
            dispatch = current(slot, "a mark")
            if dispatch["holding"] != row(e["id"], "a mark"):
                raise SystemExit(f"slot {slot} marked a row it did not fetch: {e['id']}")
            dispatch["woken"] = [row(w, "a waking") for w in e["woken"]]
        elif kind == "dispatched":
            outcome = e["outcome"]
            if slot is None:
                continue  # nothing taken: the nofetch said it, or a rollback before any slot was taken
            if not opened.get(slot):
                raise SystemExit(f"slot {slot} closed a dispatch that never opened")
            dispatch = opened[slot].pop(0)
            if outcome == "processed":
                yield record(side="inbox", event="commit", slot=slot, of=slots, woken=dispatch["woken"])
            elif outcome == "failed":
                attempts, parked = dispatch["failed"]
                yield record(side="inbox", event="fail", slot=slot, of=slots, msg=dispatch["holding"],
                             attempts=attempts, parked=parked)
            elif outcome == "rolled_back" and dispatch["holding"] is not None:
                yield record(side="inbox", event="crash", slot=slot, of=slots)
        else:
            raise SystemExit(f"unknown inbox event: {kind}")


def bridge_steps(events):
    """Both sides through one naming, with a store and the handling that
    follows it joined into `forward`, placed where the store happened.

    Line by line: each side's mapping is re-run on its lines so far and the
    newly produced steps are appended, so the two sides interleave as they
    did; a receive stays a placeholder until the handling of the same message
    joins it, and one nothing joins is refused by the model."""
    names = Names()
    out = []
    stores = {}
    outbox_seen, inbox_seen = [], []
    produced_outbox = produced_inbox = 0
    for e in events:
        if e["observer"] == "outbox":
            outbox_seen.append(e)
            steps = list(outbox_steps(outbox_seen, names))
            new = steps[produced_outbox:]
            produced_outbox = len(steps)
        else:
            inbox_seen.append(e)
            steps = list(inbox_steps(inbox_seen, names, by_message_id=True))
            new = steps[produced_inbox:]
            produced_inbox = len(steps)
        for step in new:
            fields = step["fields"]
            if fields["side"] == "inbox" and fields["event"] in ("receive", "duplicate"):
                stores[fields["msg"]] = len(out)
                out.append(step)
            elif fields["side"] == "outbox" and fields["event"] == "handle" and fields["msg"] in stores:
                at = stores.pop(fields["msg"])
                store = out[at]["fields"]
                out[at] = record(side="bridge", event="forward", d=fields["d"], msg=fields["msg"],
                                 dup=store["event"] == "duplicate", pos=store.get("pos", 0),
                                 xid=store.get("xid", 0), deps=store.get("deps", []))
            else:
                out.append(step)
    return out


CHECKERS = {
    "outbox": (outbox_steps, "TraceOutbox",
               ["TypeOK", "DeliveredCommitted", "NoPassedOver", "OrderPerUri", "NotFinished"]),
    "inbox": (inbox_steps, "TraceInbox",
              ["TypeOK", "EffectsOnce", "EffectsIffProcessed", "CausalOrder", "HeldOnce",
               "ProcessedWasReceived", "ParkedIsAside", "NotFinished"]),
    "bridge": (bridge_steps, "TraceBridge",
               ["TypeOK", "SourceInvariants", "DestinationInvariants", "ReceivedWasCommitted",
                "EndToEndOrder", "NotFinished"]),
}


def main(trace, out):
    trace, out = pathlib.Path(trace), pathlib.Path(out)
    crate = trace.stem.split("-", 1)[0]
    if crate not in CHECKERS:
        raise SystemExit(f"the file name must start with one of {sorted(CHECKERS)}: {trace.name}")
    steps, checker, invariants = CHECKERS[crate]
    events = [json.loads(line) for line in trace.read_text().splitlines() if line.strip()]
    if crate != "bridge":
        foreign = [e["observer"] for e in events if e["observer"] != crate]
        if foreign:
            raise SystemExit(f"{trace.name} holds {foreign[0]} events; name it bridge-*.jsonl to check both sides")
    name = "Trace_" + re.sub(r"[^A-Za-z0-9_]", "_", trace.stem)
    produced = list(steps(events))
    body = ",\n  ".join(str(step) for step in produced)
    # The order of arrival is a promise only with one slot.
    if crate == "inbox" and all(step["fields"].get("of", 1) == 1 for step in produced):
        invariants = invariants[:-1] + ["ArrivalOrder", "NotFinished"]
    (out / f"{name}.tla").write_text(
        f"---- MODULE {name} ----\n"
        f"\\* Generated by trace2tla.py from {trace.name}; do not edit.\n"
        f"EXTENDS {checker}\n\n"
        f"TraceDef == <<\n  {body}\n>>\n\n"
        "====\n"
    )
    (out / f"{name}.cfg").write_text(
        "SPECIFICATION Spec\nCONSTANT Trace <- TraceDef\nINVARIANTS\n"
        + "".join(f"  {inv}\n" for inv in invariants)
    )
    print(name)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit(__doc__)
    main(sys.argv[1], sys.argv[2])
