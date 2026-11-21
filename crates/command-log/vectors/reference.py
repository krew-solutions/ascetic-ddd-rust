"""The validator's rules decided from their definitions (ADR-0046, V2).

Reads the corpus `examples/corpus.rs` prints — JSON lines of a name, a log and
the crate's verdict — and decides each log again: every rule by scanning all
earlier lines, quadratic by definition, with no state carried from line to
line. Exits non-zero on the first verdict that differs.

    cargo run -q -p ascetic-ddd-command-log --example corpus | python3 -I crates/command-log/vectors/reference.py

The rules (ADR-0046 decisions 5, 7, 9):
- header: a kind both rendered and captured; an operation named twice.
- order: (at, step id) not above the line before; a step id orders by its
  subject's kind, scope bytes and index, then its label, then its occurrence.
- duplicate: a step id an earlier line has.
- edge: a dependency whose latest earlier step with that id is missing, not
  planned strictly before, or more than W before and of a label not retained.
- barrier: a count other than the earlier steps of its kind, scope, label and
  occurrence.
- producer: a captured value whose producer the line does not await as expected,
  or whose producer, the slot's latest earlier writer, captures no value of it.
- stale: a captured value whose producer is not the latest earlier writer of its
  slot. A writer of a slot is a line that captures it, or a REST call whose
  operation writes it on the line's subject.
- writers: a writer that does not await the slot's latest earlier writer as at
  least settled, and each line that read from that writer since, as at least
  settled (ADR-0047 decision 5: the server's order, not the client's).
- stream: a position outside [1, 2^31 - 1], not above the latest earlier
  message's on the same subject and stream, or that message not awaited as
  expected; gap: on a stream the profile names gapless, a position other than
  the one before plus one, or a first position other than 1.
- span: a span starting before the end of one listed before it, past its
  payload, or a JSON token outside JSON (raw bytes inside it).
- profile: a rendered `id` of a kind the profile does not render; a captured
  `id` of a kind it does not capture.
- operation: a REST call (the action's, an observation's probe) of an operation
  not in the table, or with path values other than its parameters; an
  observation polling every 0.
- deviation: a line's declared deviation exempts it from that rule; one naming
  no rule, or a rule the line keeps, is itself a violation. A `key`, `order`,
  `duplicate`, `edge`, `producer` or `deviation` violation is never exempt,
  and a deviation naming one is itself a violation (ADR-0047 decision 13). A `rate` deviation makes its line's arrivals spend, refused
  or not, and is kept only if the lower model refuses one of them.
- header (limits): a zero parameter, a `path` place naming no parameter of its
  template, one bucket name of two shapes (by, requests, per_seconds, burst), no
  source, a reserve of a thousand per thousand; a refused limit — every limit of
  a bucket of two shapes among them — is never evaluated.
- key: a REST call (an action, an observation's probe) gives no value for a
  limit of its operation: a time, an absent place, a body field without a span,
  no caller or not exactly one name of the kind, a source past the profile's.
- header (deadlines): an operation whose timeout is 0 or not below the
  profile's route timeout by more than its jitter; a channel or a host call
  named twice, or with a timeout of 0.
- operation (tables): a bus message, or an observation of the bus, on a channel
  not in the channels table; a host call not in the calls table.
- feasible: a line planned before the latest earlier step a dependency binds
  (within reach, planned before it) plus its think; or before the latest
  earlier member of a barrier it awaits plus its think.
- measure: a measure from a step no earlier line has.
- cause: an observation whose cause is neither itself nor a step it awaits.
- until: an observation of the bus none of whose predicates is an `equals` on
  a value of its cause: rendered of the cause step, or captured from it.
- sent: a line, not an observation, that awaits a dependency as sent and has a
  measure or an expectation.
- rate: arrivals — a REST action at the running maximum of `at` up to its line,
  an observation with a REST probe polling at that time plus k * every, k = 0
  to timeout // every (fewer than 2^20 + 1 polls, else k = 0 alone) — taken per
  bucket (its name, the call's value) in (time, line, poll) order; an arrival is
  refused when, with the arrivals spent before it, some window of planned width
  under J holds more than B, or a wider window from a spent arrival i holds more
  than B + R(1 - reserve/1000)(t - t_i - J)/P; refused arrivals spend nothing.
  The lower model: the same windows with J = 0, the full rate and a burst of
  B + ceil(R * J / P) + 1.
"""

import json
import sys

AWAIT = {"sent": 0, "settled": 1, "expected": 2}
NON_DEVIABLE = {"order", "duplicate", "edge", "producer", "key", "deviation"}
RULES = {
    "header", "order", "duplicate", "edge", "barrier", "producer", "stale", "writers",
    "stream", "gap", "span", "profile", "operation", "required", "deviation", "rate", "key",
    "feasible", "measure", "cause", "until", "sent",
}
MAX_POLLS = 2**20


def key(step_id):
    s = step_id["subject"]
    return (s["kind"].encode(), b64(s["scope"]), int(s["index"]), step_id["label"].encode(), step_id["n"])


def b64(text):
    import base64
    return base64.b64decode(text, validate=True)


def walk(value):
    yield value
    if isinstance(value, dict):
        for v in value.values():
            yield from walk(v)
    elif isinstance(value, list):
        for v in value:
            yield from walk(v)


def holes(step):
    return [v["hole"] for v in walk(step) if isinstance(v, dict) and "hole" in v]


def payloads(step):
    return [v for v in walk(step["action"]) if isinstance(v, dict) and {"codec", "bytes", "holes"} <= v.keys()]


def awaits(step, other, on):
    return any(
        "step" in d and d["step"]["step"] == other and AWAIT[d["step"]["on"]] >= AWAIT[on]
        for d in step["after"]
    )


def writes(header, step):
    out = {(json.dumps(c["subject"], sort_keys=True), c["slot"]) for c in step["capture"]}
    rest = step["action"].get("rest")
    if rest is not None:
        for op in header["operations"]:
            if op["name"] == rest["op"]:
                out |= {(json.dumps(step["step"]["subject"], sort_keys=True), s) for s in op["writes"]}
    return out


def reads(step):
    out = set()
    for h in holes(step):
        if "captured" in h:
            c = h["captured"]
            out.add((json.dumps(c["subject"], sort_keys=True), c["slot"], json.dumps(c["from"], sort_keys=True)))
    return out


def place_value(limit, op, rest, sources):
    """The value a call counts by, or None."""
    by = limit["by"]
    if by == "all":
        return ("all",)
    if by == "source":
        return ("source", rest["source"]) if rest["source"] < sources else None
    place = by["at"]
    kind, name = next(iter(place.items()))
    if kind == "path":
        names = [part.split("}")[0] for part in op["path"].split("{")[1:] if "}" in part]
        if name not in names or names.index(name) >= len(rest["path"]):
            return None
        return reduced(rest["path"][names.index(name)])
    if kind in ("query", "header"):
        same = (lambda a, b: a == b) if kind == "query" else (lambda a, b: a.lower() == b.lower())
        found = [f for f in rest[kind if kind == "query" else "headers"] if same(f["name"], name)]
        return reduced(found[0]["value"]) if found else None
    if kind == "body":
        spans = [sp for sp in (rest["body"] or {"holes": []})["holes"] if sp["field"] == name]
        return named(spans[0]["hole"]) if spans else None
    caller = rest["caller"]
    if caller is None:
        return None
    of_kind = [n for n in caller["names"] if n["kind"] == name]
    return ("subject", json.dumps(of_kind[0], sort_keys=True)) if len(of_kind) == 1 else None


def reduced(value):
    if "literal" in value:
        return ("literal", value["literal"])
    return named(value["hole"])


def named(hole):
    if "rendered" in hole:
        of = hole["rendered"]["of"]
        if "subject" in of:
            return ("subject", json.dumps(of["subject"], sort_keys=True))
        return ("step", json.dumps(of["step"], sort_keys=True))
    if "captured" in hole:
        return ("subject", json.dumps(hole["captured"]["subject"], sort_keys=True))
    return None


def refused_limits(header):
    """The header's limit violations, and the limits never evaluated."""
    profile, found, refused, shapes, misshapen = header["profile"], False, set(), {}, set()
    if profile["sources"] == 0 or profile["reserve"] >= 1000:
        found = True
    for op in header["operations"]:
        for i, l in enumerate(op["limits"]):
            if min(l["requests"], l["per_seconds"], l["burst"]) == 0:
                found = True
                refused.add((op["name"], i))
            by = l["by"]
            if isinstance(by, dict) and "path" in by["at"]:
                names = [part.split("}")[0] for part in op["path"].split("{")[1:] if "}" in part]
                if by["at"]["path"] not in names:
                    found = True
                    refused.add((op["name"], i))
            shape = (json.dumps(by, sort_keys=True), l["requests"], l["per_seconds"], l["burst"])
            if shapes.setdefault(l["bucket"], shape) != shape:
                found = True
                misshapen.add(l["bucket"])
    for op in header["operations"]:
        for i, l in enumerate(op["limits"]):
            if l["bucket"] in misshapen:
                refused.add((op["name"], i))
    return found, refused


def under_curve(t, spent, l, jitter, reserve):
    """Whether an arrival at t keeps every window from the spent ones under the curve."""
    rate = l["requests"] * (1000 - reserve)
    period = l["per_seconds"] * 10**9
    burst = l["burst"]
    if sum(1 for s in spent if t - s < jitter) + 1 > burst:
        return False
    for i, s in enumerate(spent):
        if t - s >= jitter:
            n = len(spent) - i + 1
            if n * period > burst * period + rate * (t - s - jitter):
                return False
    return True


def under_lower(t, spent, l, jitter):
    rate, period = l["requests"], l["per_seconds"] * 10**6
    burst = l["burst"] + -(-(rate * jitter) // period) + 1
    for i, s in enumerate(spent):
        n = len(spent) - i + 1
        if n * period > burst * period + rate * (t - s):
            return False
    return True


def offered_load(header, steps, raw):
    """rate and key, into raw; the lines whose `rate` deviation a certain refusal used."""
    profile = header["profile"]
    sources, jitter, reserve = profile["sources"], int(profile["jitter"]), profile["reserve"]
    _, refused = refused_limits(header)
    ops = {op["name"]: op for op in header["operations"]}
    buckets = {}
    clamp = 0
    for i, r in enumerate(steps):
        line = i + 2
        clamp = max(clamp, int(r["at"]))
        action = r["action"]
        if "rest" in action:
            rest, times = action["rest"], [clamp]
        elif "observe" in action and "rest" in action["observe"]["probe"]:
            o = action["observe"]
            rest, every, timeout = o["probe"]["rest"], int(o["every"]), int(o["timeout"])
            last = timeout // every if every > 0 and timeout // every < MAX_POLLS else 0
            times = [clamp + k * every for k in range(last + 1)]
        else:
            continue
        op = ops.get(rest["op"])
        if op is None:
            continue
        for n, l in enumerate(op["limits"]):
            if (op["name"], n) in refused:
                continue
            value = place_value(l, op, rest, sources)
            if value is None:
                raw.setdefault(line, set()).add("key")
                continue
            for k, t in enumerate(times):
                buckets.setdefault((l["bucket"], value), []).append((t, line, k, l))
    forced = {i + 2 for i, r in enumerate(steps) if r["deviation"] is not None and r["deviation"]["rule"] == "rate"}
    used = set()
    for arrivals in buckets.values():
        arrivals.sort(key=lambda a: a[:3])
        spent, spent_low = [], []
        for t, line, _, l in arrivals:
            admitted = under_curve(t, spent, l, jitter, reserve)
            low = under_lower(t, spent_low, l, jitter)
            if low:
                spent_low.append(t)
            if line in forced:
                spent.append(t)
                if not low:
                    used.add(line)
            elif admitted:
                spent.append(t)
            else:
                raw.setdefault(line, set()).add("rate")
    return used


def verdict(text, clock="cursor"):
    """The (line, rule) pairs a log breaks. W is measured behind the read cursor,
    the running maximum of `at` up to the line (decision 16); `clock="line"` is the
    rule before it, from the line's own `at`, kept to show the corpus tells them
    apart."""
    lines = text.split("\n")[:-1]
    header = json.loads(lines[0])["header"]
    steps = [json.loads(line)["step"] for line in lines[1:-1]]
    profile, w = header["profile"], int(header["window"])
    found = set()
    for k in profile["rendered"]:
        if k in profile["captured"]:
            found.add((1, "header"))
    names = [op["name"] for op in header["operations"]]
    if len(names) != len(set(names)):
        found.add((1, "header"))
    if refused_limits(header)[0]:
        found.add((1, "header"))
    route, jitter = int(profile["route_timeout"]), int(profile["jitter"])
    for op in header["operations"]:
        if int(op["timeout"]) == 0 or int(op["timeout"]) + jitter >= route:
            found.add((1, "header"))
    for table in ("channels", "calls"):
        names = [t["name"] for t in header[table]]
        if len(names) != len(set(names)) or any(int(t["timeout"]) == 0 for t in header[table]):
            found.add((1, "header"))
    channels = {c["name"] for c in header["channels"]}
    calls_table = {c["name"] for c in header["calls"]}
    raw = {}
    for i, r in enumerate(steps):
        line, at, earlier = i + 2, int(r["at"]), steps[:i]
        cursor = max(int(e["at"]) for e in steps[: i + 1])
        behind = cursor if clock == "cursor" else at
        v = raw.setdefault(line, set())
        if earlier and (at, key(r["step"])) <= (int(earlier[-1]["at"]), key(earlier[-1]["step"])):
            v.add("order")
        if any(e["step"] == r["step"] for e in earlier):
            v.add("duplicate")
        for d in r["after"]:
            if "step" in d:
                match = [e for e in earlier if e["step"] == d["step"]["step"]]
                e = match[-1] if match else None
                near = e is not None and (behind - int(e["at"]) <= w or e["step"]["label"] in header["retained"])
                if e is None or not int(e["at"]) < at or not near:
                    v.add("edge")
                elif int(e["at"]) + int(r["think"]) > at:
                    v.add("feasible")
            else:
                b = d["each"]
                members = [
                    e for e in earlier
                    if (e["step"]["subject"]["kind"], e["step"]["subject"]["scope"], e["step"]["label"], e["step"]["n"])
                    == (b["kind"], b["scope"], b["label"], b["n"])
                ]
                if members and max(int(e["at"]) for e in members) + int(r["think"]) > at:
                    v.add("feasible")
                if b["count"] is not None:
                    planned = sum(
                        1 for e in earlier
                        if (e["step"]["subject"]["kind"], e["step"]["subject"]["scope"], e["step"]["label"], e["step"]["n"])
                        == (b["kind"], b["scope"], b["label"], b["n"])
                    )
                    if int(b["count"]) != planned:
                        v.add("barrier")
        for subject, slot, source in reads(r):
            producer = json.loads(source)
            if not awaits(r, producer, "expected"):
                v.add("producer")
            writers = [e for e in earlier if (subject, slot) in writes(header, e)]
            if not writers or writers[-1]["step"] != producer:
                v.add("stale")
            elif not any(json.dumps(c["subject"], sort_keys=True) == subject and c["slot"] == slot
                         for c in writers[-1]["capture"]):
                v.add("producer")
        for subject, slot in writes(header, r):
            writers = [j for j, e in enumerate(earlier) if (subject, slot) in writes(header, e)]
            if writers:
                j = writers[-1]
                previous = earlier[j]["step"]
                readers = [
                    e["step"] for e in earlier[j + 1:]
                    if (subject, slot, json.dumps(previous, sort_keys=True)) in reads(e) and e["step"] != r["step"]
                ]
                if not awaits(r, previous, "settled") or not all(awaits(r, x, "settled") for x in readers):
                    v.add("writers")
        bus = r["action"].get("bus")
        if bus is not None and bus["stream"] is not None:
            name, position = bus["stream"]["name"], bus["stream"]["position"]
            if not 1 <= position <= 2**31 - 1:
                v.add("stream")
            before = [
                e for e in earlier
                if e["step"]["subject"] == r["step"]["subject"] and "bus" in e["action"]
                and e["action"]["bus"]["stream"] is not None and e["action"]["bus"]["stream"]["name"] == name
            ]
            gapless = name in profile["gapless"]
            if before:
                p = before[-1]["action"]["bus"]["stream"]["position"]
                if position <= p or not awaits(r, before[-1]["step"], "expected"):
                    v.add("stream")
                if gapless and position != p + 1:
                    v.add("gap")
            elif gapless and position != 1:
                v.add("gap")
        for p in payloads(r):
            size = len(b64(p["bytes"]))
            for n, s in enumerate(p["holes"]):
                start, stop = s["offset"], s["offset"] + s["len"]
                if any(start < t["offset"] + t["len"] for t in p["holes"][:n]):
                    v.add("span")
                if stop > size:
                    v.add("span")
                if (s["form"] == "json_token") != (p["codec"] == "json"):
                    v.add("span")
        for h in holes(r):
            if "rendered" in h and "subject" in h["rendered"]["of"] and h["rendered"]["role"] == "id":
                if h["rendered"]["of"]["subject"]["kind"] not in profile["rendered"]:
                    v.add("profile")
            if "captured" in h and h["captured"]["slot"] == "id":
                if h["captured"]["subject"]["kind"] not in profile["captured"]:
                    v.add("profile")
        for m in r["measure"]:
            if not any(e["step"] == m["from"] for e in earlier):
                v.add("measure")
        o = r["action"].get("observe")
        if o is not None:
            cause = o["cause"]
            if cause != r["step"] and not any("step" in d and d["step"]["step"] == cause for d in r["after"]):
                v.add("cause")
            if "bus" in o["probe"]:
                def of_cause(value):
                    hole = value.get("hole", {})
                    if "rendered" in hole:
                        return hole["rendered"]["of"].get("step") == cause
                    if "captured" in hole:
                        return hole["captured"]["from"] == cause
                    return False
                if not any("equals" in p["test"] and of_cause(p["test"]["equals"]) for p in o["until"]):
                    v.add("until")
        elif r["measure"] or r["expect"]:
            sent = any(
                (d["step"]["on"] if "step" in d else d["each"]["on"]) == "sent"
                for d in r["after"]
            )
            if sent:
                v.add("sent")
        if "bus" in r["action"] and r["action"]["bus"]["channel"] not in channels:
            v.add("operation")
        if o is not None and "bus" in o["probe"] and o["probe"]["bus"]["channel"] not in channels:
            v.add("operation")
        if "host" in r["action"] and r["action"]["host"]["op"] not in calls_table:
            v.add("operation")
        action = r["action"]
        calls = [action["rest"]] if "rest" in action else []
        if "observe" in action:
            if "rest" in action["observe"]["probe"]:
                calls.append(action["observe"]["probe"]["rest"])
            every = int(action["observe"]["every"])
            if every == 0 or int(action["observe"]["timeout"]) // every >= MAX_POLLS:
                v.add("operation")
        for c in calls:
            ops = [op for op in header["operations"] if op["name"] == c["op"]]
            if not ops or ops[0]["path"].count("{") != len(c["path"]):
                v.add("operation")
    used = offered_load(header, steps, raw)
    for i, r in enumerate(steps):
        if r["deviation"] is None:
            continue
        line, rule = i + 2, r["deviation"]["rule"]
        v = raw.setdefault(line, set())
        if rule not in RULES:
            v.add("deviation")
        elif rule == "rate":
            if line not in used:
                v.add("deviation")
        elif rule in NON_DEVIABLE:
            v.add("deviation")
        elif rule in v:
            v.discard(rule)
        else:
            v.add("deviation")
    for line, rules in raw.items():
        found |= {(line, rule) for rule in rules}
    return sorted(found)


def peaks(text):
    """What a run must hold, from the definitions: the most completions kept at
    once (steps whose latest occurrence is within W behind the cursor, or
    retained), the most live captures (slots whose latest writer is), and the
    polls the observations imply, summed."""
    lines = text.split("\n")[:-1]
    header = json.loads(lines[0])["header"]
    steps = [json.loads(line)["step"] for line in lines[1:-1]]
    w, retained = int(header["window"]), header["retained"]
    completions = captures = polls = 0
    for i in range(len(steps)):
        cursor = max(int(e["at"]) for e in steps[: i + 1])
        latest, writer = {}, {}
        for e in steps[: i + 1]:
            latest[key(e["step"])] = e
            for slot in writes(header, e):
                writer[slot] = e
        live = lambda e: e["step"]["label"] in retained or cursor - int(e["at"]) <= w
        completions = max(completions, sum(1 for e in latest.values() if live(e)))
        captures = max(captures, sum(1 for e in writer.values() if live(e)))
    for r in steps:
        o = r["action"].get("observe")
        if o is not None:
            every, timeout = int(o["every"]), int(o["timeout"])
            polls += timeout // every + 1 if every > 0 and timeout // every < MAX_POLLS else 1
    return [completions, captures, polls]


def main():
    count, apart, rules = 0, 0, {}
    for entry in sys.stdin:
        entry = json.loads(entry)
        mine = verdict(entry["log"])
        theirs = sorted((line, rule) for line, rule in entry["verdict"])
        if mine != theirs:
            raise SystemExit(f"{entry['name']}: the reference finds {mine}, the validator {theirs}")
        held = peaks(entry["log"])
        if held != entry["peaks"]:
            raise SystemExit(f"{entry['name']}: the reference's peaks are {held}, the validator's {entry['peaks']}")
        if verdict(entry["log"], clock="line") != mine:
            apart += 1
        count += 1
        for _, rule in mine:
            rules[rule] = rules.get(rule, 0) + 1
    if count == 0:
        raise SystemExit("no corpus on stdin")
    if apart == 0:
        raise SystemExit("no log tells the cursor from the line's own time: the corpus does not execute decision 16's clock")
    print(f"{count} logs agree, {apart} of them only on the cursor's clock; violations by rule: {dict(sorted(rules.items()))}")


if __name__ == "__main__":
    main()
