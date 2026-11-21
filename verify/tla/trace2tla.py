#!/usr/bin/env python3
"""Turns a recorded outbox or inbox trace into a TLA+ module for its checker.

    trace2tla.py <trace.jsonl> <out dir>

Exits 3 when it refuses the run, printing the reason on stderr, and the
module of the steps before the refusal, if there are any, as for a run it
takes.

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
<<group, slot>>: the slot whose position it holds, whoever runs it. A run
with several dispatchers at once is not checked: the order its events are
logged in is not the order of their commits (see verify/tla/README.md).

The inbox mapping:

    received, stored          -> receive   (msg, pos, xid, slot, deps)
    received, duplicate       -> duplicate (msg)
    waiting                   -> wait      (slot, of, msg, dep, snap): set aside for the dependency;
                                            the whole dispatch, closed by `dispatched set_aside`
    fetched, a row            -> fetch     (slot, of, msg, snap)
    fetched, a slot, no row   -> nofetch   (slot, of, snap): nothing due in the slot once taken
    fetched, no slot          -> nofetch   (of, snap): no slot had a due head
    handled ok                -> handle    (slot, of, msg)
    handled failed            -> nothing: the subscriber declined
    marked                    -> nothing: the mark commits with the transaction
    failed + dispatched failed
                              -> fail      (slot, of, msg, attempts, parked): the attempt commits
    marked + dispatched processed
                              -> commit    (slot, of, woken): the mark and the rows it woke
    dispatched set_aside      -> nothing: the wait already said it
    dispatched nothing        -> nothing: the nofetch already said it, or a wait was not
                                          set because the dependency was marked meanwhile
    dispatched rolled_back    -> crash     (slot, of), if a row was fetched
    expired                   -> expire    (msg), one per row parked
    unparked                  -> unpark    (msg)
    resolved                  -> resolve   (msg, woken)
    operated, a read or a call on the rows the intake set aside
                              -> nothing: no row of the inbox moved

Refused, what the models do not describe: a row stored again after the
sweep forgot it (Inbox.tla forgets nothing; InboxForget.tla does), and an
outbox's epoch delivered with nothing of its selection's, an acknowledgement
that moves no position (ADR-0035's relay).

Refused, what the model does not describe yet (ADR-0042), named without the
slot or the count of rows, which a run's timing may move: a head held
without its subscriber called (`withheld`), a row held for an `Unavailable`
(`failed`, held), a guest's walk among paused heads (`dispatched paused`),
and a verb over many rows that moved some (`operated` parked_heads,
parked_queue or unparked_all), whose rows the trace does not name. The
reason refused is the one `traces/uncovered.tsv` lists, word for word.

Messages are named in order of first mention, a dependency that never arrives
included; a dispatcher is the slot it holds, whoever ran it: the events
between a fetch, or a set-aside, naming a slot and the close naming it are
one dispatch. As for the outbox, a run with several dispatchers at once is
not checked.

The bridge mapping is both of the above, with messages named by `message_id`
on both sides and one step of its own:

    inbox received + outbox handled ok of the same message
                              -> forward   (d, msg, dup, pos, xid, slot, deps): Bridge's Forward, at the store
    a handled ok with no store, or a store no handling follows
                              -> handle / receive, as above: not steps of the bridge, refused
    inbox rejected + the outbox's next handled ok with no store
                              -> set_aside (d, msg), at the rejection, and acknowledge (d, msg), at the
                                 handling: Bridge's SetAside and Acknowledge, two steps (ADR-0036)
    a rejected no handling follows, or a handling before its rejection
                              -> rejected / handle: not steps of the bridge, refused

A message set aside never became a row, so the inbox's own mapping skips a
`rejected`: the inbox's model has nothing of it to check.

The scheduler mapping (`scheduler-*.jsonl`, written by crates/scheduler's
trace test, checked against TraceScheduler.tla): the first line is the log
— the window, the read-ahead and every step's instant, think, dependencies,
barrier key, producer, whether it captures, timeout, safety and whether it
is retained — the next ones the inputs the machine took, in its order, and
the last ones its journal. The machine's lines are numbered from 2, the
header's being 1; the model's from 1.

    log                       -> the model's constants
    input read                -> read     (at, line)
    input sent                -> sent     (at, line, attempt)
    input ended               -> ended    (at, line, attempt, judgement): an expected answer
                                 without the value its step captures is "bare"
    journal                   -> Journal[line]: compared once the trace is taken
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
            if e["position"] is None:
                # an epoch with nothing of the selection's, delivered at once:
                # the epoch relay's step, which Outbox.tla does not take
                raise SystemExit(f"{group} delivered an epoch with nothing of its selection's, moving no position: "
                                 "TraceOutbox.tla follows the relay without epochs (ADR-0035)")
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
    stored = {}  # the row identities stored so far, and where
    # how many slots the table is cut into: one number, logged with every fetch
    slots = next((e["slots"] for e in events if e["event"] == "fetched"), 1)
    holding = {}  # slot -> the row its open dispatch fetched, if any
    failed = {}   # slot -> (attempts, parked) recorded, awaiting the close
    woken = {}    # slot -> the rows the mark woke, awaiting the close

    def dependency(identity):
        if by_message_id:
            raise SystemExit("a dependency in a bridge trace: rows are named by message_id there, dependencies by identity")
        return names.of(identity)

    def row(identity, what):
        if identity not in rows:
            raise SystemExit(f"{what} names a row never received: {identity}")
        return rows[identity]

    for e in events:
        kind = e["event"]
        if kind == "rejected":
            continue
        if kind == "parked_at_once":
            # its row was stored before the intake's checks, by SQL in the
            # test, and no receive names it; the model holds the step,
            # InboxUnreadable.cfg (ADR-0037 decision 6)
            raise SystemExit("a park at once in a trace: its row was never received, so no step can name it")
        if kind == "unparked":
            yield record(side="inbox", event="unpark", msg=row(e["id"], "an unpark"))
            continue
        if kind == "behind":
            raise SystemExit("rows written behind the observers, by the test's own statement: "
                             "the model takes no step for them")
        if kind == "operated":
            if e["operation"] in ("parked_heads", "parked_queue", "unparked_all") and e["rows"] > 0:
                raise SystemExit(f"an operator's {e['operation']} moved rows the trace does not name: "
                                 "Inbox.tla has no verb over many rows yet (ADR-0042)")
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
            if e["received_position"] not in (None, stored.get(e["id"], e["received_position"])):
                # its row was forgotten by the sweep, and a repeat past its
                # window stored it again, at a new position (ADR-0033):
                # Inbox.tla forgets nothing
                raise SystemExit(f"{msg} was stored again after its row was forgotten: TraceInbox.tla follows "
                                 "Inbox.tla, which forgets nothing (InboxForget.tla does)")
            if e["received_position"] is not None:
                stored[e["id"]] = e["received_position"]
            if e["received_position"] is None:
                yield record(side="inbox", event="duplicate", msg=msg)
            else:
                yield record(side="inbox", event="receive", msg=msg, pos=e["received_position"], xid=e["xid"],
                             slot=e["slot"], deps=deps)
            continue
        slot = e["slot"]
        if kind == "fetched":
            if slot is None:
                yield record(side="inbox", event="nofetch", of=slots, snap=snapshot(e))
            elif slot in holding:
                raise SystemExit(f"slot {slot} was taken while its dispatch was open")
            elif e["id"] is None:
                holding[slot] = None
                yield record(side="inbox", event="nofetch", slot=slot, of=slots, snap=snapshot(e))
            else:
                holding[slot] = row(e["id"], "a fetch")
                yield record(side="inbox", event="fetch", slot=slot, of=slots, msg=holding[slot], snap=snapshot(e))
        elif kind == "waiting":
            # the wait is the dispatch: it holds no row and is closed by `set_aside`
            if slot in holding:
                raise SystemExit(f"slot {slot} set a row aside while holding {holding[slot]}")
            holding[slot] = None
            yield record(side="inbox", event="wait", slot=slot, of=slots, msg=row(e["id"], "a wait"),
                         dep=dependency(e["dependency"]), snap=snapshot(e))
        elif kind == "withheld":
            raise SystemExit(f"a head held unasked under {e['cause']}: Inbox.tla has no hold yet (ADR-0042)")
        elif kind == "failed":
            if e.get("held"):
                # an Unavailable holds its row and spends no attempt (ADR-0042
                # decision 3): Inbox.tla has no hold yet, and a hold read as a
                # failure would fit wrongly
                raise SystemExit(f"a row held under {e['cause']}, spending nothing: "
                                 "Inbox.tla has no hold yet (ADR-0042)")
            failed[slot] = (e["attempts"], e["parked"], e["permanent"])
        elif kind == "handled":
            if e["ok"]:
                yield record(side="inbox", event="handle", slot=slot, of=slots, msg=row(e["id"], "a handling"))
        elif kind == "marked":
            if holding.get(slot) != row(e["id"], "a mark"):
                raise SystemExit(f"slot {slot} marked a row it did not fetch: {e['id']}")
            woken[slot] = [row(w, "a waking") for w in e["woken"]]
        elif kind == "dispatched":
            if slot is None:
                continue  # nothing taken: the nofetch said it, or a rollback before any slot was taken
            outcome = e["outcome"]
            if slot not in holding:
                if outcome in ("nothing", "rolled_back"):
                    continue  # a slot taken and nothing durable done: the dependency was marked meanwhile
                raise SystemExit(f"slot {slot} closed a dispatch that never opened")
            if outcome == "processed":
                yield record(side="inbox", event="commit", slot=slot, of=slots, woken=woken.get(slot, []))
            elif outcome == "failed":
                attempts, parked, permanent = failed[slot]
                yield record(side="inbox", event="fail", slot=slot, of=slots, msg=holding[slot],
                             attempts=attempts, parked=parked, permanent=permanent)
            elif outcome == "rolled_back" and holding[slot] is not None:
                yield record(side="inbox", event="crash", slot=slot, of=slots)
            elif outcome == "paused":
                raise SystemExit("a guest's walk among paused heads: Inbox.tla has no pause yet (ADR-0042)")
            # "passed": a walk that found no due head among the Tenants it
            # examined took none, its fetch an empty one; the cursor it moved is
            # not followed, TraceInbox.tla being FIFO (ADR-0041 § Owed)
            elif outcome not in ("rolled_back", "nothing", "set_aside", "parked", "passed"):
                # an outcome the mapping does not know would otherwise end the
                # dispatch without a step, and a run would fit for the wrong reason
                raise SystemExit(f"slot {slot} closed a dispatch with an outcome not mapped: {outcome}")
            del holding[slot]
            failed.pop(slot, None)
            woken.pop(slot, None)
        else:
            raise SystemExit(f"unknown inbox event: {kind}")


def bridge_steps(events):
    """Both sides through one naming, with a store and the handling that
    follows it joined into `forward`, placed where the store happened.

    Line by line: each side's mapping is re-run on its lines so far and the
    newly produced steps are appended, so the two sides interleave as they
    did; a receive stays a placeholder until the handling of the same message
    joins it, and one nothing joins is refused by the model. A rejection
    names no message, its identity being perhaps what could not be read: it
    stays a placeholder until the outbox's next handling with no store joins
    it, which is the message the intake set aside."""
    names = Names()
    out = []
    stores = {}
    asides = []  # the places of rejections no handling has joined yet
    outbox_seen, inbox_seen = [], []
    produced_outbox = produced_inbox = 0
    for e in events:
        if e["observer"] == "outbox":
            outbox_seen.append(e)
            steps = list(outbox_steps(outbox_seen, names))
            new = steps[produced_outbox:]
            produced_outbox = len(steps)
        elif e["event"] == "rejected":
            asides.append(len(out))
            out.append(record(side="inbox", event="rejected"))
            continue
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
                                 xid=store.get("xid", 0), slot=store.get("slot", 0), deps=store.get("deps", []))
            elif fields["side"] == "outbox" and fields["event"] == "handle" and asides:
                out[asides.pop(0)] = record(side="bridge", event="set_aside", d=fields["d"], msg=fields["msg"])
                out.append(record(side="bridge", event="acknowledge", d=fields["d"], msg=fields["msg"]))
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
                "EndToEndOrder", "SetAsideNeverStored", "NotFinished"]),
}


def scheduler(trace, out, events):
    """The scheduler's trace: its log as constants, its inputs as Trace."""
    foreign = [e["observer"] for e in events if e["observer"] != "scheduler"]
    if foreign:
        raise SystemExit(f"{trace.name} holds {foreign[0]} events")
    log = [e for e in events if e["event"] == "log"]
    if len(log) != 1 or events[0]["event"] != "log":
        raise SystemExit(f"{trace.name} must start with its log, once")
    log = log[0]
    lines = log["lines"]
    inputs = [e for e in events if e["event"] == "input"]
    journal = {e["line"]: e for e in events if e["event"] == "journal"}
    if not inputs:
        print(f"{trace.name} maps to no step: there is nothing to check", file=sys.stderr)
        sys.exit(3)
    if sorted(journal) != list(range(2, len(lines) + 2)):
        raise SystemExit(f"{trace.name}: the journal must hold every line once")

    def none(v):
        return -1 if v is None else v

    def model_line(v):
        return v - 1

    def after(entry):
        if "step" in entry:
            return {"k": "step", "dep": model_line(entry["step"]), "on": entry["on"]}
        return {"k": "each", "key": str(entry["each"]), "on": entry["on"]}

    def end(v):
        if isinstance(v, list):
            return [v[0], v[1], model_line(v[2])]
        return [v]

    def bound(line):
        it = iter(journal[line + 2]["bound"])
        return [model_line(next(it)) if "step" in a else 0 for a in lines[line]["after"]]

    steps = [record(input=e["input"], at=e["at"], line=model_line(e["line"]),
                    attempt=e.get("attempt", 0), judgement=e.get("judgement", "")) for e in inputs]
    entries = []
    for line in range(len(lines)):
        j = journal[line + 2]
        entries.append(record(end=end(j["end"]), read=j["read"], intended=j["intended"],
                              released=j["released"], dispatched=none(j["dispatched"]),
                              sent=none(j["sent"]), ended=j["ended"], attempts=j["attempts"],
                              bound=bound(line)))
    times = {0} | {e["at"] for e in inputs}
    for j in journal.values():
        times |= {j[f] for f in ("read", "intended", "released", "dispatched", "sent", "ended") if j[f] is not None}
    attempts = max(j["attempts"] for j in journal.values())
    name = "Trace_" + re.sub(r"[^A-Za-z0-9_]", "_", trace.stem)
    consts = {
        "TrAt": [l["at"] for l in lines],
        "TrThink": [l["think"] for l in lines],
        "TrAfter": [[after(a) for a in l["after"]] for l in lines],
        "TrKey": [str(l["key"]) for l in lines],
        "TrProducer": [0 if l["producer"] == 0 else model_line(l["producer"]) for l in lines],
        "TrCaptures": [l["captures"] for l in lines],
        "TrTimeout": [l["timeout"] for l in lines],
        "TrSafety": [l["safety"] for l in lines],
        "TrRetained": [l["retained"] for l in lines],
    }
    body = ",\n  ".join(str(step) for step in steps)
    (out / f"{name}.tla").write_text(
        f"---- MODULE {name} ----\n"
        f"\\* Generated by trace2tla.py from {trace.name}; do not edit.\n"
        "EXTENDS TraceScheduler\n\n"
        + "".join(f"{k} == {tla(v)}\n" for k, v in consts.items())
        + "TrTimes == {" + ", ".join(str(t) for t in sorted(times)) + "}\n"
        + 'TrJudgements == {"expected", "bare", "unexpected", "not_taken", "unknown"}\n'
        + "TrJournal == <<\n  " + ",\n  ".join(str(e) for e in entries) + "\n>>\n"
        + f"TraceDef == <<\n  {body}\n>>\n\n"
        "====\n"
    )
    (out / f"{name}.cfg").write_text(
        "SPECIFICATION TraceSpec\nCONSTANTS\n"
        f"  N = {len(lines)}\n"
        + "".join(f"  {k[2:]} <- {k}\n" for k in consts)
        + f"  W = {log['w']}\n  L = {log['read_ahead']}\n  Lag = {max(times) + 1}\n"
        "  Times <- TrTimes\n  Judgements <- TrJudgements\n"
        f"  MaxAttempts = {attempts + 1}\n"
        '  Mutant = "none"\n'
        "  Trace <- TraceDef\n  Journal <- TrJournal\n"
        "INVARIANTS\n"
        + "".join(f"  {inv}\n" for inv in ["TypeOK", "ReleaseSafe", "BarrierSafe", "NoUseAfterFree",
                                              "BoundAsChecked", "NoCensored", "EndsAsModelled",
                                              "NotFinished"])
    )
    print(name)


def main(trace, out):
    trace, out = pathlib.Path(trace), pathlib.Path(out)
    crate = trace.stem.split("-", 1)[0]
    if crate == "scheduler":
        events = [json.loads(line) for line in trace.read_text().splitlines() if line.strip()]
        return scheduler(trace, out, events)
    if crate not in CHECKERS:
        raise SystemExit(f"the file name must start with one of {sorted(CHECKERS)}: {trace.name}")
    steps, checker, invariants = CHECKERS[crate]
    events = [json.loads(line) for line in trace.read_text().splitlines() if line.strip()]
    if crate != "bridge":
        foreign = [e["observer"] for e in events if e["observer"] != crate]
        if foreign:
            raise SystemExit(f"{trace.name} holds {foreign[0]} events; name it bridge-*.jsonl to check both sides")
    name = "Trace_" + re.sub(r"[^A-Za-z0-9_]", "_", trace.stem)
    # A run refused at a step the model does not describe is still checked up
    # to that step: its prefix is written, its name printed, and the reason
    # is the exit's, with status 3, so that a refusal hides no step before it
    # that would not fit.
    produced, refusal = [], None
    try:
        for step in steps(events):
            produced.append(step)
    except SystemExit as refused:
        refusal = refused.code
    # A run without a step fits whatever the model says: covered, it would
    # check nothing (an intake that only set messages aside, say).
    if not produced and refusal is None:
        refusal = f"{trace.name} maps to no step: there is nothing to check"
    if not produced:
        print(refusal, file=sys.stderr)
        sys.exit(3)
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
    if refusal is not None:
        print(refusal, file=sys.stderr)
        sys.exit(3)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit(__doc__)
    main(sys.argv[1], sys.argv[2])
