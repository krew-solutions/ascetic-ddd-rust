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
    fetched, some messages    -> fetch    (d, of, horizon, limit, msgs)
    fetched, none             -> nofetch  (d, of, horizon, limit, msgs = <<>>)
    handled ok                -> handle   (d, msg)
    handled failed            -> nothing: the subscriber declined
    acked + dispatched batch  -> ack      (d, xid, pos): position and COMMIT, one step
    dispatched nothing        -> nothing: the nofetch already said it
    dispatched rolled_back    -> crash    (d), if a batch was fetched

Messages are named m1, m2, ... in order of publication; a dispatcher is
<<group, worker>>, the group without the `:worker` suffix several workers add.

The inbox mapping:

    received, stored          -> receive   (msg, pos, xid, deps)
    received, duplicate       -> duplicate (msg)
    waiting                   -> wait      (d, of, msg, dep, snap): set aside for the dependency
    fetched, a row            -> fetch     (d, of, msg, snap)
    fetched, none             -> nofetch   (d, of, snap)
    deferred + fetched, none  -> blocked   (d, of, msg, snap): the oldest row waits for its backoff
    handled ok                -> handle    (d, of, msg)
    handled failed            -> nothing: the subscriber declined
    marked                    -> nothing: the mark commits with the transaction
    failed + dispatched failed
                              -> fail      (d, of, msg, attempts, parked): the attempt commits
    marked + dispatched processed
                              -> commit    (d, of, woken): the mark and the rows it woke
    dispatched nothing        -> nothing: the nofetch already said it
    dispatched rolled_back    -> crash     (d, of), if a row was fetched
    expired                   -> expire    (msg), one per row parked
    unparked                  -> unpark    (msg)
    resolved                  -> resolve   (msg, woken)

Messages are named in order of first mention, a dependency that never arrives
included; a dispatcher is <<worker, slot>>, where each open `dispatch` call
takes the lowest slot of its worker not held by another open call.

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

    def dispatcher(e):
        group, worker, of = e["group"], e["worker"], e["of"]
        if of > 1 and group.endswith(f":{worker}"):
            group = group[: -len(f":{worker}")]
        return (group, worker), of

    for e in events:
        kind = e["event"]
        if kind == "published":
            yield record(side="outbox", event="publish", msg=names.of(e["message_id"]), uri=e["uri"], xid=e["xid"], pos=e["position"])
            continue
        d, of = dispatcher(e)
        if kind == "fetched":
            msgs = [names.known(m, "a fetch") for m in e["messages"]]
            pending[d] = bool(msgs)
            yield record(side="outbox", event="fetch" if msgs else "nofetch", d=d, of=of,
                         horizon=e["horizon"], limit=e["limit"], msgs=msgs)
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
    slots = {}    # (worker, call) -> slot
    holding = {}  # (worker, call) -> the row fetched, if any
    deferred = {} # (worker, call) -> the row found waiting for its backoff
    failed = {}   # (worker, call) -> (attempts, parked) recorded, awaiting COMMIT
    woken = {}    # (worker, call) -> the rows the mark woke, awaiting COMMIT

    def dependency(identity):
        if by_message_id:
            raise SystemExit("a dependency in a bridge trace: rows are named by message_id there, dependencies by identity")
        return names.of(identity)

    def row(identity, what):
        if identity not in rows:
            raise SystemExit(f"{what} names a row never received: {identity}")
        return rows[identity]

    def dispatcher(e):
        key = (e["worker"], e["call"])
        if key not in slots:
            taken = {slot for (worker, _), slot in slots.items() if worker == e["worker"]}
            slots[key] = min(s for s in range(len(taken) + 1) if s not in taken)
        return key, (e["worker"], slots[key]), e["of"]

    for e in events:
        kind = e["event"]
        if kind == "unparked":
            yield record(side="inbox", event="unpark", msg=row(e["id"], "an unpark"))
            continue
        if kind == "resolved":
            yield record(side="inbox", event="resolve", msg=row(e["id"], "a resolve"),
                         woken=[row(w, "a waking") for w in e["woken"]])
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
        key, d, of = dispatcher(e)
        if kind == "waiting":
            yield record(side="inbox", event="wait", d=d, of=of, msg=row(e["id"], "a wait"),
                         dep=dependency(e["dependency"]), snap=snapshot(e))
        elif kind == "expired":
            for identity in e["ids"]:
                yield record(side="inbox", event="expire", msg=row(identity, "an expiry"))
        elif kind == "deferred":
            deferred[key] = (row(e["id"], "a deferral"), snapshot(e))
        elif kind == "fetched":
            if e["id"] is None and key in deferred:
                msg, snap = deferred.pop(key)
                yield record(side="inbox", event="blocked", d=d, of=of, msg=msg, snap=snap)
            elif e["id"] is None:
                yield record(side="inbox", event="nofetch", d=d, of=of, snap=snapshot(e))
            else:
                holding[key] = row(e["id"], "a fetch")
                yield record(side="inbox", event="fetch", d=d, of=of, msg=holding[key], snap=snapshot(e))
        elif kind == "failed":
            failed[key] = (e["attempts"], e["parked"])
        elif kind == "handled":
            if e["ok"]:
                yield record(side="inbox", event="handle", d=d, of=of, msg=row(e["id"], "a handling"))
        elif kind == "marked":
            if holding.get(key) != row(e["id"], "a mark"):
                raise SystemExit(f"call {key} marked a row it did not fetch: {e['id']}")
            woken[key] = [row(w, "a waking") for w in e["woken"]]
        elif kind == "dispatched":
            outcome = e["outcome"]
            if outcome == "processed":
                yield record(side="inbox", event="commit", d=d, of=of, woken=woken.get(key, []))
            elif outcome == "failed":
                attempts, parked = failed.pop(key)
                yield record(side="inbox", event="fail", d=d, of=of, msg=holding[key], attempts=attempts, parked=parked)
            elif outcome == "rolled_back" and key in holding:
                yield record(side="inbox", event="crash", d=d, of=of)
            failed.pop(key, None)
            woken.pop(key, None)
            holding.pop(key, None)
            deferred.pop(key, None)
            del slots[key]
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
    # The order of arrival is a promise only with one dispatcher per partition.
    if crate == "inbox" and all(step["fields"].get("d", (0, 0))[1] == 0 for step in produced):
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
