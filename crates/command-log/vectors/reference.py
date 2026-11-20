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
- producer: a captured value whose producer the line does not await as expected.
- stale: a captured value whose producer is not the latest earlier writer of its
  slot. A writer of a slot is a line that captures it, or a REST call whose
  operation writes it on the line's subject.
- writers: a writer that does not await the slot's latest earlier writer as at
  least settled, and each line that read from that writer since, as at least
  sent.
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
  no rule, or a rule the line keeps, is itself a violation.
"""

import json
import sys

AWAIT = {"sent": 0, "settled": 1, "expected": 2}
RULES = {
    "header", "order", "duplicate", "edge", "barrier", "producer", "stale", "writers",
    "stream", "gap", "span", "profile", "operation", "required", "deviation",
}


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


def verdict(text):
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
    raw = {}
    for i, r in enumerate(steps):
        line, at, earlier = i + 2, int(r["at"]), steps[:i]
        v = raw.setdefault(line, set())
        if earlier and (at, key(r["step"])) <= (int(earlier[-1]["at"]), key(earlier[-1]["step"])):
            v.add("order")
        if any(e["step"] == r["step"] for e in earlier):
            v.add("duplicate")
        for d in r["after"]:
            if "step" in d:
                match = [e for e in earlier if e["step"] == d["step"]["step"]]
                e = match[-1] if match else None
                near = e is not None and (at - int(e["at"]) <= w or e["step"]["label"] in header["retained"])
                if e is None or not int(e["at"]) < at or not near:
                    v.add("edge")
            else:
                b = d["each"]
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
        for subject, slot in writes(header, r):
            writers = [j for j, e in enumerate(earlier) if (subject, slot) in writes(header, e)]
            if writers:
                j = writers[-1]
                previous = earlier[j]["step"]
                readers = [
                    e["step"] for e in earlier[j + 1:]
                    if (subject, slot, json.dumps(previous, sort_keys=True)) in reads(e) and e["step"] != r["step"]
                ]
                if not awaits(r, previous, "settled") or not all(awaits(r, x, "sent") for x in readers):
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
        action = r["action"]
        calls = [action["rest"]] if "rest" in action else []
        if "observe" in action:
            if "rest" in action["observe"]["probe"]:
                calls.append(action["observe"]["probe"]["rest"])
            if int(action["observe"]["every"]) == 0:
                v.add("operation")
        for c in calls:
            ops = [op for op in header["operations"] if op["name"] == c["op"]]
            if not ops or ops[0]["path"].count("{") != len(c["path"]):
                v.add("operation")
        if r["deviation"] is not None:
            rule = r["deviation"]["rule"]
            if rule not in RULES:
                v.add("deviation")
            elif rule in v:
                v.discard(rule)
            else:
                v.add("deviation")
    for line, rules in raw.items():
        found |= {(line, rule) for rule in rules}
    return sorted(found)


def main():
    count, rules = 0, {}
    for entry in sys.stdin:
        entry = json.loads(entry)
        mine = verdict(entry["log"])
        theirs = sorted((line, rule) for line, rule in entry["verdict"])
        if mine != theirs:
            raise SystemExit(f"{entry['name']}: the reference finds {mine}, the validator {theirs}")
        count += 1
        for _, rule in mine:
            rules[rule] = rules.get(rule, 0) + 1
    if count == 0:
        raise SystemExit("no corpus on stdin")
    print(f"{count} logs agree; violations by rule: {dict(sorted(rules.items()))}")


if __name__ == "__main__":
    main()
