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
  not in the table, or with path values other than its parameters, or from a
  source past the profile's (ADR-0047 decision 18); an observation polling
  every 0.
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
  earlier member of a barrier it awaits plus its think; or before its think
  (from the run's start, whatever it awaits).
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

import base64
import importlib.util
import json
import pathlib
import re
import sys

sys.dont_write_bytecode = True
_HERE = pathlib.Path(__file__).resolve().parent


def _load(name):
    """A vector by definition beside this one, under its own name."""
    spec = importlib.util.spec_from_file_location(name, _HERE / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# The frame's static half (ADR-0047 decision 15) and the answers' JSON, by
# their own definitions.
request = _load("request")
answers = _load("json")

AWAIT = {"sent": 0, "settled": 1, "expected": 2}
NON_DEVIABLE = {
    "order", "duplicate", "edge", "producer", "key", "deviation", "frame", "predicate", "expect",
    "secret",
}
RULES = {
    "header", "order", "duplicate", "edge", "barrier", "producer", "stale", "writers",
    "stream", "gap", "span", "profile", "operation", "required", "deviation", "rate", "key",
    "feasible", "measure", "cause", "until", "sent", "frame", "predicate", "expect", "secret",
}
BYTES = {"uuid_bytes", "secret32"}
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
    pointer = profile["problem"]
    if pointer and (not pointer.startswith("/") or re.search(r"~(?![01])", pointer)):
        found.add((1, "header"))
    tchar = set("!#$%&'*+-.^_`|~0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
    corr = profile["correlation"]
    reserved = {"host", "content-length", "transfer-encoding", "connection", "content-type"}
    if not corr or any(c not in tchar for c in corr) or corr.lower() in reserved:
        found.add((1, "header"))
    if any(int(profile[f]) == 0 for f in ("resend", "answer_cap", "idle")):
        found.add((1, "header"))
    # A body is read into a tape whose places are 32-bit (ADR-0047 K2b-7e-4b-2).
    if int(profile["answer_cap"]) > 2**32 - 1:
        found.add((1, "header"))
    route, jitter = int(profile["route_timeout"]), int(profile["jitter"])
    for op in header["operations"]:
        if int(op["timeout"]) == 0 or int(op["timeout"]) + jitter >= route:
            found.add((1, "header"))
        if op["safety"] == "not_taken" and not op["not_run"]:
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
        if int(r["think"]) > at:
            v.add("feasible")
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
        producers = {}
        for subject, slot, _ in reads(r):
            producers[(subject, slot)] = producers.get((subject, slot), 0) + 1
        if any(n > 1 for n in producers.values()):
            v.add("producer")
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
                if s["form"] == "raw" and "captured" in s["hole"]:
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
        subject_of = json.dumps(r["step"]["subject"], sort_keys=True)
        for c in calls:
            if int(c["source"]) >= int(profile["sources"]):
                v.add("operation")
            plain = c["path"] + [f["value"] for f in c["query"] + c["headers"]]
            if any("hole" in x and "rendered" in x["hole"] and x["hole"]["rendered"]["as"] in BYTES for x in plain):
                v.add("frame")
            ops = [op for op in header["operations"] if op["name"] == c["op"]]
            if not ops:
                v.add("operation")
                continue
            if ops[0]["safety"] == "idempotent" and any(
                subject == subject_of and slot in ops[0]["writes"] for subject, slot, _ in reads(r)
            ):
                v.add("operation")
            try:
                request.check(ops[0], c, profile["correlation"])
            except request.Refused:
                v.add("frame")
        # Predicates that can be judged: an RFC 6901 pointer, a literal that is
        # JSON text, a value that is no bytes.
        bodies = [p for o in r["expect"] if isinstance(o, dict) for p in o["response"]["body"]]
        until = action["observe"]["until"] if "observe" in action else []
        # A capture's pointer as a predicate's (K2b-7e-4b-2).
        for c in r["capture"]:
            if isinstance(c["source"], dict) and "json" in c["source"]:
                pointer = c["source"]["json"]
                if pointer and (not pointer.startswith("/") or re.search(r"~(?![01])", pointer)):
                    v.add("predicate")
        for p in bodies + until:
            pointer = p["pointer"]
            if pointer and (not pointer.startswith("/") or re.search(r"~(?![01])", pointer)):
                v.add("predicate")
            if p["test"] != "exists":
                (value,) = p["test"].values()
                if "literal" in value:
                    try:
                        answers.read(value["literal"].encode())
                    except answers.Unreadable:
                        v.add("predicate")
                elif "rendered" in value["hole"] and value["hole"]["rendered"]["as"] in BYTES:
                    v.add("predicate")
        # An expectation fits its action; only a step awaiting a dependency
        # only sent, a host call, expects nothing.
        kind = next(iter(action))
        fits = {"rest": "response", "bus": "delivered", "observe": "observed"}.get(kind)
        outcome = lambda o: next(iter(o)) if isinstance(o, dict) else o  # noqa: E731
        if any(outcome(o) != fits for o in r["expect"]):
            v.add("expect")
        sent_only = any((d["step"]["on"] if "step" in d else d["each"]["on"]) == "sent" for d in r["after"])
        if not r["expect"] and (kind == "observe" or (kind in ("rest", "bus") and not sent_only)):
            v.add("expect")
        # No secret in a request's target or a message's key.
        logged = [x for c in calls for x in c["path"] + [f["value"] for f in c["query"]]]
        if kind == "bus":
            logged.append(action["bus"]["key"])
        if kind == "observe" and "bus" in action["observe"]["probe"]:
            logged.append(action["observe"]["probe"]["bus"]["key"])
        for x in logged:
            hole = x.get("hole", {})
            if "rendered" in hole and hole["rendered"]["as"] == "secret32":
                v.add("secret")
            if "captured" in hole:
                c = hole["captured"]
                key_ = (json.dumps(c["subject"], sort_keys=True), c["slot"])
                writers = [e for e in earlier if key_ in writes(header, e)]
                if writers and any(
                    json.dumps(w["subject"], sort_keys=True) == key_[0] and w["slot"] == key_[1] and w["secret"]
                    for w in writers[-1]["capture"]
                ):
                    v.add("secret")
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


# What the corpus reaches of the bound's terms (ADR-0047 K1e-2): reads at which a
# step the window let go is held only because of one term. Each must be reached.
REACHED = ("pin", "prefix", "read_ahead", "tie")


def bound(text, seen, out=None):
    """The plan's own bound (ADR-0047 K1e-2), by definition, quadratic: every
    line read at its earliest, at - L with L the header's read-ahead, and every
    action ending at its deadline. Per line: R, the latest release — its plan,
    or the latest end of what it awaits plus its think; E, the latest end — R
    plus its timeout (an operation's, a channel's, a host call's; an
    observation's from its cause's R), or R if past it; H, the latest instant
    a machine may hold it — E, the latest E of its barrier key's members up to
    it, the R of a later line it is the captured producer or the cause of. At
    the read of line j, tau the cursor less L: the steps held are those the
    window keeps (latest of their id, within W behind the cursor or retained)
    and those it let go with H >= tau; those not ended have E >= tau. Returns
    the per-line [line, R, E, held, pending] and the folds [held, pending,
    horizon, barriers]."""
    lines = text.split("\n")[:-1]
    header = json.loads(lines[0])["header"]
    steps = [json.loads(line)["step"] for line in lines[1:-1]]
    w, retained, ahead = int(header["window"]), header["retained"], int(header["read_ahead"])
    tables = {}
    for kind, table, name in (("rest", "operations", "name"), ("bus", "channels", "name"), ("host", "calls", "name")):
        tables[kind] = {}
        for t in header[table]:
            tables[kind].setdefault(t[name], int(t["timeout"]))

    def timeout(r):
        a = r["action"]
        if "rest" in a:
            return tables["rest"].get(a["rest"]["op"], 0)
        if "bus" in a:
            return tables["bus"].get(a["bus"]["channel"], 0)
        if "host" in a:
            return tables["host"].get(a["host"]["op"], 0)
        return int(a["observe"]["timeout"])

    cursors = []
    for r in steps:
        cursors.append(max([int(r["at"])] + cursors[-1:]))

    def bind(i, step_id):
        match = [j for j in range(i) if steps[j]["step"] == step_id]
        if not match:
            return None
        j = match[-1]
        e = steps[j]
        if e["step"]["label"] in retained or cursors[i] - int(e["at"]) <= w:
            return j
        return None

    def member(s):
        return (s["subject"]["kind"], s["subject"]["scope"], s["label"], s["n"])

    R, E, P, Q = [], [], [], []
    for i, r in enumerate(steps):
        ready = 0
        for d in r["after"]:
            if "step" in d:
                b = bind(i, d["step"]["step"])
                if b is not None:
                    ready = max(ready, E[b])
            else:
                e = d["each"]
                for j in range(i):
                    if member(steps[j]["step"]) == (e["kind"], e["scope"], e["label"], e["n"]):
                        ready = max(ready, E[j])
        rel = max(int(r["at"]), ready + int(r["think"]))
        start = rel
        o = r["action"].get("observe")
        if o is not None and o["cause"] != r["step"]:
            c = bind(i, o["cause"])
            if c is not None:
                start = R[c]
        R.append(rel)
        E.append(max(rel, start + timeout(r)))
        P.append(max(E[j] for j in range(i + 1) if member(steps[j]["step"]) == member(r["step"])))
        Q.append(0)
    for i, r in enumerate(steps):
        pinned = [h["captured"]["from"] for h in holes(r) if "captured" in h]
        o = r["action"].get("observe")
        if o is not None and o["cause"] != r["step"]:
            pinned.append(o["cause"])
        for p in pinned:
            b = bind(i, p)
            if b is not None:
                Q[b] = max(Q[b], R[i])
    H = [max(E[k], P[k], Q[k]) for k in range(len(steps))]
    per_line, held, pending = [], 0, 0
    for j in range(len(steps)):
        tau = max(cursors[j] - ahead, 0)
        now_held = now_pending = 0
        for k in range(j + 1):
            latest = max(x for x in range(j + 1) if steps[x]["step"] == steps[k]["step"]) == k
            kept = latest and (steps[k]["step"]["label"] in retained or cursors[j] - int(steps[k]["at"]) <= w)
            if kept or H[k] >= tau:
                now_held += 1
            if E[k] >= tau:
                now_pending += 1
            if not kept and H[k] >= tau:
                if max(E[k], P[k]) < tau <= Q[k]:
                    seen["pin"] += 1
                if max(E[k], Q[k]) < tau <= P[k]:
                    seen["prefix"] += 1
                if H[k] < cursors[j]:
                    seen["read_ahead"] += 1
                if H[k] == tau:
                    seen["tie"] += 1
        per_line.append([j + 2, R[j], E[j], now_held, now_pending])
        held, pending = max(held, now_held), max(pending, now_pending)
    keys = {member(r["step"]) for r in steps}
    if out is not None:
        out["E"], out["H"] = E, H
    return per_line, [held, pending, max(E, default=0), len(keys)]


# A shape's counts as the corpus carries them (ADR-0047 decision 22): records,
# bytes, byte strings, then (elements, sequences) of deps, values, fields,
# outcomes, predicates, captures, the captures' bytes, (elements, sequences) of
# measures, spans, subjects, then holes, hole bytes, key bytes, a request's
# target, headers and body bytes each with its holes (K2b-7e-2b-3a), the barrier
# dependencies' key bytes (K2b-7e-2b-2b), the largest predicate literal and the
# longest pointer (K2b-7e-2b-2c).
SHAPE = 34
KEY_BYTES, CAPTURES, CAPTURE_BYTES, EACH_BYTES, LITERAL_BYTES, POINTER_BYTES = 24, 13, 15, 31, 32, 33
REQUEST = 25


def request_counts(r):
    """A call's or a probe's request by class, by definition (K2b-7e-2b-3a):
    its target's literal bytes — path values, the query's names and values —
    and holes, its headers' likewise, its body's bytes and holes; none, zero."""
    a = r["action"]
    rest = a["rest"] if "rest" in a else a.get("observe", {}).get("probe", {}).get("rest")
    if rest is None:
        return (0,) * 6
    utf8 = lambda t: len(t.encode())

    def values(vs):
        return (sum(utf8(v["literal"]) for v in vs if "literal" in v), sum(1 for v in vs if "hole" in v))

    path = values(rest["path"])
    query = values([q["value"] for q in rest["query"]])
    names = sum(utf8(q["name"]) for q in rest["query"])
    headers = values([h["value"] for h in rest["headers"]])
    header_names = sum(utf8(h["name"]) for h in rest["headers"])
    body = rest.get("body")
    body_bytes, body_holes = (len(base64.b64decode(body["bytes"])), len(body["holes"])) if body else (0, 0)
    return (
        path[0] + query[0] + names,
        path[1] + query[1],
        headers[0] + header_names,
        headers[1],
        body_bytes,
        body_holes,
    )


def socket(r):
    """The source whose socket a step takes: a call's, a probe's; none, -1."""
    a = r["action"]
    if "rest" in a:
        return int(a["rest"]["source"])
    probe = a.get("observe", {}).get("probe", {})
    if "rest" in probe:
        return int(probe["rest"]["source"])
    return -1


def byte_peaks(text, shapes):
    """The plan's byte peaks (ADR-0047 decision 22), by definition, quadratic,
    from the shapes the validator read: at the read of line j, tau the cursor
    less L, J the profile's jitter — the shapes of the steps not ended
    (E >= tau) summed, of those not ended a jitter later (E >= tau - J), what
    the held keep (the held as `bound` defines them), each source's steps not
    ended; each count's peak over the reads, and over the records one at a
    time (the largest record's, K2b-7e-2b-2). A shape's counts this reference
    can derive itself — its key's and its captures' bytes, its captures, its
    barrier dependencies' key bytes, its source — are checked against the
    record first."""
    lines = text.split("\n")[:-1]
    header = json.loads(lines[0])["header"]
    steps = [json.loads(line)["step"] for line in lines[1:-1]]
    if len(shapes) != len(steps):
        raise SystemExit(f"{len(shapes)} shapes for {len(steps)} steps")
    w, retained, ahead = int(header["window"]), header["retained"], int(header["read_ahead"])
    jitter = int(header["profile"]["jitter"])
    utf8 = lambda t: len(t.encode())
    raw = lambda t: len(base64.b64decode(t))
    for k, (r, s) in enumerate(zip(steps, shapes)):
        line, src, counts = s[0], s[1], s[2:]
        subject = r["step"]["subject"]
        key_bytes = utf8(subject["kind"]) + raw(subject["scope"]) + utf8(r["step"]["label"])
        took = sum(utf8(c["subject"]["kind"]) + raw(c["subject"]["scope"]) + utf8(c["slot"]) for c in r["capture"])
        each = sum(utf8(d["each"]["kind"]) + raw(d["each"]["scope"]) + utf8(d["each"]["label"]) for d in r["after"] if "each" in d)
        predicates = [p for o in r["expect"] if isinstance(o, dict) for p in o["response"]["body"]]
        predicates += r["action"]["observe"]["until"] if "observe" in r["action"] else []
        literal = max((utf8(t[test]["literal"]) for p in predicates for t in [p["test"]] if isinstance(t, dict) for test in t if "literal" in t[test]), default=0)
        pointers = [p["pointer"] for p in predicates] + [c["source"]["json"] for c in r["capture"] if isinstance(c["source"], dict) and "json" in c["source"]]
        pointer = max((utf8(p) for p in pointers), default=0)
        want = (k + 2, socket(r), 1, key_bytes, len(r["capture"]), took, each, literal, pointer, request_counts(r))
        have = (line, src, counts[0], counts[KEY_BYTES], counts[CAPTURES], counts[CAPTURE_BYTES], counts[EACH_BYTES], counts[LITERAL_BYTES], counts[POINTER_BYTES], tuple(counts[REQUEST:REQUEST + 6]))
        if len(counts) != SHAPE or want != have:
            raise SystemExit(f"line {k + 2}: the shape's (line, source, records, key, captures, their bytes, barrier keys' bytes, largest literal, longest pointer, request by class) are {have}, by definition {want}")
    out = {}
    bound(text, {term: 0 for term in REACHED}, out)
    E, H = out["E"], out["H"]
    cursors = []
    for r in steps:
        cursors.append(max([int(r["at"])] + cursors[-1:]))
    late = 0
    pending_shape, late_shape, kept = [0] * SHAPE, [0] * SHAPE, [0, 0, 0]
    sockets = {}
    for j in range(len(steps)):
        tau = max(cursors[j] - ahead, 0)
        now_p, now_l, now_k, now_s, n_late = [0] * SHAPE, [0] * SHAPE, [0, 0, 0], {}, 0
        for k in range(j + 1):
            counts = shapes[k][2:]
            if E[k] >= tau:
                now_p = [a + b for a, b in zip(now_p, counts)]
                src = shapes[k][1]
                if src >= 0:
                    now_s[src] = now_s.get(src, 0) + 1
            if E[k] >= tau - jitter:
                now_l = [a + b for a, b in zip(now_l, counts)]
                n_late += 1
            latest = max(x for x in range(j + 1) if steps[x]["step"] == steps[k]["step"]) == k
            kept_k = latest and (steps[k]["step"]["label"] in retained or cursors[j] - int(steps[k]["at"]) <= w)
            if kept_k or H[k] >= tau:
                now_k = [a + b for a, b in zip(now_k, (counts[KEY_BYTES], counts[CAPTURES], counts[CAPTURE_BYTES]))]
        late = max(late, n_late)
        pending_shape = [max(a, b) for a, b in zip(pending_shape, now_p)]
        late_shape = [max(a, b) for a, b in zip(late_shape, now_l)]
        kept = [max(a, b) for a, b in zip(kept, now_k)]
        for src, n in now_s.items():
            sockets[src] = max(sockets.get(src, 0), n)
    top = max(sockets, default=-1)
    keys = {(r["step"]["subject"]["kind"], r["step"]["subject"]["scope"], r["step"]["label"], r["step"]["n"]) for r in steps}
    return {
        "late": late,
        "pending_shape": pending_shape,
        "late_shape": late_shape,
        "kept": kept,
        "sockets": [sockets.get(i, 0) for i in range(top + 1)],
        "barrier_bytes": sum(utf8(kind) + raw(scope) + utf8(label) for kind, scope, label, _ in keys),
        "record": [max((s[2:][i] for s in shapes), default=0) for i in range(SHAPE)],
    }


def main():
    count, apart, rules = 0, 0, {}
    # What the corpus reaches of the byte peaks (decision 22): each, or the
    # agreement on it says nothing.
    reached = {"late": 0, "socket": 0, "kept_capture": 0, "hole": 0}
    seen = {k: 0 for k in REACHED}
    for entry in sys.stdin:
        entry = json.loads(entry)
        mine = verdict(entry["log"])
        theirs = sorted((line, rule) for line, rule in entry["verdict"])
        if mine != theirs:
            raise SystemExit(f"{entry['name']}: the reference finds {mine}, the validator {theirs}")
        per_line, folds = bound(entry["log"], seen)
        held = peaks(entry["log"]) + folds
        if held != entry["peaks"]:
            raise SystemExit(f"{entry['name']}: the reference's peaks are {held}, the validator's {entry['peaks']}")
        if per_line != entry["bounds"]:
            diff = next(((m, t) for m, t in zip(per_line, entry["bounds"]) if m != t), (per_line[-1:], entry["bounds"][-1:]))
            raise SystemExit(f"{entry['name']}: the bound by definition {diff[0]}, the validator's {diff[1]} ([line, R, E, held, pending])")
        mine_bytes = byte_peaks(entry["log"], entry["shapes"])
        if mine_bytes != entry["byte_peaks"]:
            diff = {k: (mine_bytes[k], entry["byte_peaks"][k]) for k in mine_bytes if mine_bytes[k] != entry["byte_peaks"].get(k)}
            raise SystemExit(f"{entry['name']}: the byte peaks by definition differ from the validator's: {diff}")
        p = entry["peaks"]
        reached["late"] += mine_bytes["late"] > p[4]
        reached["socket"] += any(mine_bytes["sockets"])
        reached["kept_capture"] += mine_bytes["kept"][1] > 0
        reached["hole"] += mine_bytes["pending_shape"][22] > 0
        if verdict(entry["log"], clock="line") != mine:
            apart += 1
        count += 1
        for _, rule in mine:
            rules[rule] = rules.get(rule, 0) + 1
    if count == 0:
        raise SystemExit("no corpus on stdin")
    if apart == 0:
        raise SystemExit("no log tells the cursor from the line's own time: the corpus does not execute decision 16's clock")
    unreached = [k for k, n in reached.items() if n == 0]
    if unreached:
        raise SystemExit(f"the corpus reaches no byte peak's {unreached}: the agreement on them says nothing ({reached})")
    missed = [k for k, n in seen.items() if n == 0]
    if missed:
        raise SystemExit(f"the corpus reaches no read held only by {missed}: the bound's executors do not execute them")
    print(f"{count} logs agree, {apart} of them only on the cursor's clock; violations by rule: {dict(sorted(rules.items()))}; the bound's terms reached at {seen}; the byte peaks' at {reached}")


if __name__ == "__main__":
    main()
