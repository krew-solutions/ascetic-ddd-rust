#!/usr/bin/env python3
"""Turns a recorded outbox or inbox trace into a TLA+ module for its checker.

    trace2tla.py <trace.jsonl> <out dir>

Reads the lines the observer of the crate's tests writes, one JSON object per
event, and writes `<out dir>/<Name>.tla` defining `TraceDef`, the sequence of
steps the model takes, plus `<Name>.cfg`. Prints `<Name>`. Which crate is
read off the file name: `outbox-*.jsonl` checks against TraceOutbox.tla,
`inbox-*.jsonl` against TraceInbox.tla.

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

    received, stored          -> receive   (msg, pos, deps)
    received, duplicate       -> duplicate (msg)
    skipped                   -> skip      (d, of, msg)
    fetched, a row            -> fetch     (d, of, msg)
    fetched, none             -> nofetch   (d, of)
    handled ok                -> handle    (d, msg)
    handled failed            -> nothing: the subscriber declined
    marked                    -> nothing: the mark commits with the transaction
    dispatched message        -> commit    (d)
    dispatched nothing        -> nothing: the nofetch already said it
    dispatched rolled_back    -> crash     (d), if a row was fetched

Messages are named in order of first mention, a dependency that never arrives
included; a dispatcher is <<worker, slot>>, where each open `dispatch` call
takes the lowest slot of its worker not held by another open call.
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
    raise TypeError(v)


def record(**fields):
    return "[" + ", ".join(f"{k} |-> {tla(v)}" for k, v in fields.items()) + "]"


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


def outbox_steps(events):
    names = Names()
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
            yield record(event="publish", msg=names.of(e["message_id"]), uri=e["uri"], xid=e["xid"], pos=e["position"])
            continue
        d, of = dispatcher(e)
        if kind == "fetched":
            msgs = [names.known(m, "a fetch") for m in e["messages"]]
            pending[d] = bool(msgs)
            yield record(event="fetch" if msgs else "nofetch", d=d, of=of,
                         horizon=e["horizon"], limit=e["limit"], msgs=msgs)
        elif kind == "handled":
            if e["ok"]:
                yield record(event="handle", d=d, msg=names.known(e["message_id"], "a handling"))
        elif kind == "acked":
            acked[d] = (e["xid"], e["position"])
        elif kind == "dispatched":
            outcome = e["outcome"]
            if outcome == "batch":
                xid, pos = acked.pop(d)
                yield record(event="ack", d=d, xid=xid, pos=pos)
            elif outcome == "rolled_back":
                acked.pop(d, None)
                if pending.get(d):
                    yield record(event="crash", d=d)
            pending[d] = False
        else:
            raise SystemExit(f"unknown outbox event: {kind}")


def inbox_steps(events):
    names = Names()
    slots = {}    # (worker, call) -> slot
    holding = {}  # (worker, call) -> the row fetched, if any

    def dispatcher(e):
        key = (e["worker"], e["call"])
        if key not in slots:
            taken = {slot for (worker, _), slot in slots.items() if worker == e["worker"]}
            slots[key] = min(s for s in range(len(taken) + 1) if s not in taken)
        return key, (e["worker"], slots[key]), e["of"]

    for e in events:
        kind = e["event"]
        if kind == "received":
            msg = names.of(e["id"])
            deps = [names.of(dep) for dep in e["deps"]]
            if e["received_position"] is None:
                yield record(event="duplicate", msg=msg)
            else:
                yield record(event="receive", msg=msg, pos=e["received_position"], deps=deps)
            continue
        key, d, of = dispatcher(e)
        if kind == "skipped":
            yield record(event="skip", d=d, of=of, msg=names.known(e["id"], "a skip"))
        elif kind == "fetched":
            if e["id"] is None:
                yield record(event="nofetch", d=d, of=of)
            else:
                holding[key] = names.known(e["id"], "a fetch")
                yield record(event="fetch", d=d, of=of, msg=holding[key])
        elif kind == "handled":
            if e["ok"]:
                yield record(event="handle", d=d, of=of, msg=names.known(e["id"], "a handling"))
        elif kind == "marked":
            if holding.get(key) != names.known(e["id"], "a mark"):
                raise SystemExit(f"call {key} marked a row it did not fetch: {e['id']}")
        elif kind == "dispatched":
            outcome = e["outcome"]
            if outcome == "message":
                yield record(event="commit", d=d, of=of)
            elif outcome == "rolled_back" and key in holding:
                yield record(event="crash", d=d, of=of)
            holding.pop(key, None)
            del slots[key]
        else:
            raise SystemExit(f"unknown inbox event: {kind}")


CHECKERS = {
    "outbox": (outbox_steps, "TraceOutbox",
               ["TypeOK", "DeliveredCommitted", "NoPassedOver", "OrderPerUri", "NotFinished"]),
    "inbox": (inbox_steps, "TraceInbox",
              ["TypeOK", "EffectsOnce", "EffectsIffProcessed", "CausalOrder", "HeldOnce",
               "ProcessedWasReceived", "NotFinished"]),
}


def main(trace, out):
    trace, out = pathlib.Path(trace), pathlib.Path(out)
    crate = trace.stem.split("-", 1)[0]
    if crate not in CHECKERS:
        raise SystemExit(f"the file name must start with one of {sorted(CHECKERS)}: {trace.name}")
    steps, checker, invariants = CHECKERS[crate]
    events = [json.loads(line) for line in trace.read_text().splitlines() if line.strip()]
    name = "Trace_" + re.sub(r"[^A-Za-z0-9_]", "_", trace.stem)
    body = ",\n  ".join(steps(events))
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
