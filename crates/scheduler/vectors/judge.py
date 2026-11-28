"""An attempt judged by definition (ADR-0047 decision 15), outside the crates.

Reads golden/answers.jsonl — one case a line, as crates/scheduler/tests/judge.rs
writes them: a step's record, what came back (an answer's status, header
lines in base64 and body in base64, null past the cap; a reset; no answer by
the deadline), the not-run codes, the problem's pointer, the values captured
before — and prints golden/judgements.txt.

The rules, first match: a reset or no answer, Unknown; an `expect` response
of equal status, equal problem code (none when there is no readable code) and
holding predicates, Expected; a poll's 2xx whose `until` holds, Expected; a
code in `not_run`, NotTaken, with `Retry-After`; a status of 500 or more, or
below 200, Unknown; an empty `expect` but a poll's, Expected; else
Unexpected. The code: the string at the pointer of an object body, read from
a status of 400 or more whose one Content-Type's essence is
application/problem+json. A value taken: a number as written, at most 8 KiB, or a string of
visible ASCII with inner blanks, not empty, `.` or `..`, at most 8 KiB, as a
JSON string; from a header's one line, a body's pointer, or a Location's last
non-empty path segment, its query and fragment dropped, percent-decoded
strictly. Retry-After in delta-seconds, from one line, saturating. A code is
journaled named if it is a dotted lower-case name of at most 64 bytes.

JSON is read by crates/command-log/vectors/json.py. The standard library
only. Run: python3 -I crates/scheduler/vectors/judge.py
"""

import base64
import importlib.util
import json as std_json
import pathlib
import re
import sys
import urllib.parse

sys.dont_write_bytecode = True
HERE = pathlib.Path(__file__).resolve().parent

# The command log's reader by definition, loaded under its own name: the
# standard library's `json`, imported above, is the one it imports.
_spec = importlib.util.spec_from_file_location("reader", HERE.parents[1] / "command-log" / "vectors" / "json.py")
reader = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(reader)

U64 = 2**64 - 1
MISSING = object()


def at(doc, pointer):
    """RFC 6901: the value there, MISSING if none; raises on a malformed pointer."""
    if pointer == "":
        return doc
    if not pointer.startswith("/"):
        raise ValueError("pointer")
    here = doc
    for raw in pointer[1:].split("/"):
        if re.search(r"~(?![01])", raw):
            raise ValueError("pointer")
        token = raw.replace("~1", "/").replace("~0", "~")
        if isinstance(here, dict):
            if token not in here:
                return MISSING
            here = here[token]
        elif isinstance(here, list):
            if not re.fullmatch(r"0|[1-9][0-9]*", token) or int(token) >= len(here):
                return MISSING
            here = here[int(token)]
        else:
            return MISSING
    return here


def parse(raw):
    try:
        return reader.read(raw)
    except reader.Unreadable:
        return MISSING


def lines(answer, name):
    return [base64.b64decode(v) for n, v in answer["headers"] if n.lower() == name.lower()]


def one(answer, name):
    ls = lines(answer, name)
    return ls[0] if len(ls) == 1 else None


def body(answer):
    return MISSING if answer["body"] is None else parse(base64.b64decode(answer["body"]))


def code(answer, pointer):
    if answer["status"] < 400:
        return None
    media = one(answer, "content-type")
    if media is None:
        return None
    try:
        media = media.decode()
    except UnicodeDecodeError:
        return None
    if media.split(";")[0].strip().lower() != "application/problem+json":
        return None
    doc = body(answer)
    if not isinstance(doc, dict):
        return None
    v = at(doc, pointer)
    return v if isinstance(v, str) and not isinstance(v, reader.Num) else None


def captured_value(case, hole):
    for c in case["captured"]:
        if c["subject"] == hole["subject"] and c["slot"] == hole["slot"]:
            return parse(base64.b64decode(c["value"]))
    raise ValueError("missing")


def expected_value(case, value):
    if "literal" in value:
        v = parse(value["literal"].encode())
        if v is MISSING:
            raise ValueError("literal")
        return v
    return captured_value(case, value["hole"]["captured"])


def holds(case, answer, predicates):
    doc = body(answer)
    ok = True
    for p in predicates:
        test = p["test"]
        want = None if test == "exists" else expected_value(case, next(iter(test.values())))
        at({}, p["pointer"])  # a malformed pointer raises, whatever the body
        found = MISSING if doc is MISSING else at(doc, p["pointer"])
        if found is MISSING:
            ok = False
        elif test == "exists":
            pass
        elif "equals" in test:
            ok = ok and reader.equal(found, want)
        else:
            ok = ok and isinstance(found, list) and any(reader.equal(x, want) for x in found)
    return ok


def carried(s):
    b = s.encode()
    visible = lambda c: 0x21 <= c <= 0x7E  # noqa: E731
    return (
        0 < len(b) <= 8192
        and s not in (".", "..")
        and all(visible(c) or c in (0x20, 0x09) for c in b)
        and visible(b[0])
        and visible(b[-1])
    )


def quoted(s):
    return std_json.dumps(s).encode() if carried(s) else None


def decoded(s):
    if re.search(r"%(?![0-9A-Fa-f]{2})", s):
        return None
    try:
        return urllib.parse.unquote_to_bytes(s).decode("utf-8")
    except UnicodeDecodeError:
        return None


def take(answer, capture):
    source = capture["source"]
    if source == "location":
        v = one(answer, "location")
        if v is None:
            return None
        try:
            s = v.decode()
        except UnicodeDecodeError:
            return None
        s = s.split("#")[0].split("?")[0]
        if "://" in s:
            after = s[s.index("://") + 3:]
            s = after[after.index("/"):] if "/" in after else ""
        segments = [p for p in s.split("/") if p]
        if not segments:
            return None
        d = decoded(segments[-1])
        return None if d is None else quoted(d)
    if "header" in source:
        v = one(answer, source["header"])
        if v is None:
            return None
        try:
            return quoted(v.decode())
        except UnicodeDecodeError:
            return None
    doc = body(answer)
    v = MISSING if doc is MISSING else at(doc, source["json"])
    if isinstance(v, reader.Num):
        return str(v).encode() if carried(str(v)) else None
    if isinstance(v, str):
        return quoted(v)
    return None


def retry_after(answer):
    v = one(answer, "retry-after")
    if v is None or not re.fullmatch(rb"[0-9]+", v):
        return None
    s = int(v)
    return U64 if s > U64 else min(s * 1_000_000, U64)


def journaled(c):
    if c is None:
        return "-"
    named = len(c.encode()) <= 64 and re.fullmatch(r"[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+", c)
    return f"named {c}" if named else "other"


def judge(case):
    record, transport = case["record"], case["transport"]
    if transport in ("broken", "deadline"):
        return ("unknown", transport, None, None, None, [], [])
    answer = transport["answered"]
    status = answer["status"]
    c = code(answer, case["problem"])

    def expected(why):
        taken, missing = [], []
        for cap in record["capture"]:
            v = take(answer, cap)
            if v is None:
                missing.append(cap["slot"])
            else:
                taken.append(f"{cap['slot']}={v.decode()}")
        return ("expected", why, status, c, None, taken, missing)

    for i, o in enumerate(record["expect"]):
        if isinstance(o, dict) and "response" in o:
            r = o["response"]
            if r["status"] == status and r["problem"] == c and holds(case, answer, r["body"]):
                return expected(f"listed {i}")
    action = record["action"]
    poll = "observe" in action and "rest" in action["observe"]["probe"]
    if poll and 200 <= status < 300 and holds(case, answer, action["observe"]["until"]):
        return expected("polled")
    if c is not None and c in case["not_run"]:
        return ("not_taken", "not_run", status, c, retry_after(answer), [], [])
    if not 200 <= status < 500:
        return ("unknown", "uncertain", status, c, None, [], [])
    if not record["expect"] and not poll:
        return expected("sent")
    return ("unexpected", "unlisted", status, c, None, [], [])


def main():
    for raw in (HERE.parent / "golden" / "answers.jsonl").read_text().splitlines():
        case = std_json.loads(raw)
        kind, why, status, c, after, taken, missing = judge(case)
        dash = lambda v: "-" if v is None else str(v)  # noqa: E731
        print(f"case {case['name']}")
        print(
            f"{kind} | {why} | status {dash(status)} | code {journaled(c)} | retry_after {dash(after)}"
            f" | taken [{' '.join(taken)}] | missing [{' '.join(missing)}]"
        )


if __name__ == "__main__":
    main()
