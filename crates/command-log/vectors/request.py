"""A REST step's frame by definition (ADR-0047 decision 15), outside the crate.

Reads golden/frames.jsonl — one case a line, as crates/command-log/tests/request.rs
writes them: an operation, a step's call, the fills (the namespace, the run's
secret in hex, the run's start and the frame's send in microseconds since the
epoch, the captured values in base64) and the correlation header — and prints
golden/frames.txt: each frame, or the class of its refusal.

The rules, as the decision states them:
- the method a token; the path template `/`-rooted literals and `{name}`s, each
  literal of RFC 3986 `pchar` but `%`, or `/`; as many values as placeholders;
- each value — a literal, a rendered value, a time, a captured value (one JSON
  value's text: a string's content, a number's text) — every byte outside
  unreserved percent-encoded in upper-case hex (`urllib.parse.quote`), never
  empty, `.` or `..`; the query `name=value` in order, both encoded, joined by
  `&`;
- planned headers: a token name, never reserved (`host`, `content-length`,
  `transfer-encoding`, `connection`, `content-type`), decorated or the
  correlation header's, compared without case; a value of visible ASCII with
  inner blanks only; the decorated names and the correlation header tokens,
  not reserved, apart; `content-type` the operation's after them, with a body;
- the body: each span replaced — a JSON token by the value as JSON (`json.dumps`;
  a captured number as written), raw bytes at the span's length; key material
  HMAC-SHA256 under the run's secret (render.py).

The standard library only. Run: python3 -I crates/command-log/vectors/request.py
"""

import base64
import json
import pathlib
import re
import struct
import sys
import uuid
import urllib.parse

sys.dont_write_bytecode = True
HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import render as r  # noqa: E402

RESERVED = {"host", "content-length", "transfer-encoding", "connection", "content-type"}
TCHAR = set(b"!#$%&'*+-.^_`|~0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
UNRESERVED = set(b"-._~0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
LITERAL = UNRESERVED | set(b"!$&'()*+,;=:@/")


class Refused(Exception):
    pass


def token(s):
    return bool(s) and all(b in TCHAR for b in s.encode())


def field_value(s):
    b = s.encode()
    visible = lambda c: 0x21 <= c <= 0x7E  # noqa: E731
    return all(visible(c) or c in (0x20, 0x09) for c in b) and (not b or (visible(b[0]) and visible(b[-1])))


def encode(s):
    return urllib.parse.quote(s, safe="")


def subject_of(s):
    return s["kind"], base64.b64decode(s["scope"]), int(s["index"])


def rendered(case, hole):
    of, role, rendering = hole["of"], hole["role"], hole["as"]
    ns = case["namespace"]
    if "subject" in of:
        kind, scope, index = subject_of(of["subject"])
        step = None
        of_bytes = r.of_subject(kind, scope, index)
    else:
        kind, scope, index = subject_of(of["step"]["subject"])
        step = (of["step"]["label"], of["step"]["n"])
        of_bytes = r.of_step(kind, scope, index, *step)
    if rendering == "secret32":
        return ("bytes", r.secret32(bytes.fromhex(case["secret"]), kind, scope, index, step, role))
    kk = r.key(ns, of_bytes, role)
    if rendering == "uuid_text":
        return ("text", r.k.rendered(kk, ns))
    if rendering == "uuid_bytes":
        return ("bytes", uuid.UUID(r.k.rendered(kk, ns)).bytes)
    return ("text", r.words(kk, "hex", rendering["hex"]["len"]).hex())


def time_of(case, hole):
    base = case["run_start"] if hole["base"] == "run_start" else case["sent"]
    t = base + int(hole["offset"])
    if hole["as"] == "epoch_millis":
        return ("integer", t // 1000)
    return ("text", r.rfc3339(t))


def captured(case, hole):
    for c in case["captured"]:
        if c["subject"] == hole["subject"] and c["slot"] == hole["slot"]:
            raw = base64.b64decode(c["value"])
            break
    else:
        raise Refused("hole missing")
    try:
        v = json.loads(raw.decode(), parse_int=lambda s: ("json", s), parse_float=lambda s: ("json", s))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise Refused("hole not_json")
    if isinstance(v, str):
        return ("text", v)
    if isinstance(v, tuple):
        return v
    raise Refused("hole not_scalar")


def filled(case, hole):
    (kind, h), = hole.items()
    if kind == "rendered":
        return rendered(case, h)
    if kind == "time":
        return time_of(case, h)
    return captured(case, h)


def text(case, value):
    if "literal" in value:
        return value["literal"]
    kind, v = filled(case, value["hole"])
    if kind == "bytes":
        raise Refused("hole not_text")
    return str(v)


def written(span, value):
    kind, v = value
    if span["form"] == "json_token":
        if kind == "bytes":
            raise Refused("fill form")
        if kind == "text":
            return json.dumps(v, ensure_ascii=False).encode()
        return str(v).encode()
    b = {"bytes": lambda: v, "text": lambda: v.encode(), "json": lambda: v.encode(),
         "integer": lambda: struct.pack("<q", v)}[kind]()
    if len(b) != span["len"]:
        raise Refused("fill length")
    return b


def check(op, rest, corr):
    """The frame's static half: what the step's values cannot change. Returns
    the template's pieces."""
    if not token(op["method"]):
        raise Refused("method")
    path = op["path"]
    pieces = re.findall(r"\{[A-Za-z0-9_]+\}|[^{}]+|[{}]", path)
    if not path.startswith("/") or any(p in ("{", "}") for p in pieces):
        raise Refused("template")
    if any(not p.startswith("{") and not all(b in LITERAL for b in p.encode()) for p in pieces):
        raise Refused("literal")
    holes = [p for p in pieces if p.startswith("{")]
    if len(holes) != len(rest["path"]):
        raise Refused("arity")
    if any("literal" in v and v["literal"] in ("", ".", "..") for v in rest["path"]):
        raise Refused("segment")
    decorated = [d.lower() for d in op["decorated"]]
    for name in op["decorated"] + [corr]:
        if not token(name):
            raise Refused("header_name")
        if name.lower() in RESERVED:
            raise Refused("reserved")
    if corr.lower() in decorated:
        raise Refused("reserved")
    for h in rest["headers"]:
        if not token(h["name"]):
            raise Refused("header_name")
        if h["name"].lower() in RESERVED | set(decorated) | {corr.lower()}:
            raise Refused("reserved")
        if "literal" in h["value"] and not field_value(h["value"]["literal"]):
            raise Refused("header_value")
    if rest["body"] is not None and (not op["content_type"] or not field_value(op["content_type"])):
        raise Refused("content_type")
    return pieces


def build(case):
    op, rest, corr = case["op"], case["rest"], case["correlation"]
    pieces = check(op, rest, corr)
    target = ""
    values = iter(rest["path"])
    for p in pieces:
        if p.startswith("{"):
            v = text(case, next(values))
            if v in ("", ".", ".."):
                raise Refused("segment")
            target += encode(v)
        else:
            target += p
    for i, q in enumerate(rest["query"]):
        target += ("?" if i == 0 else "&") + encode(q["name"]) + "=" + encode(text(case, q["value"]))
    headers = []
    for h in rest["headers"]:
        v = text(case, h["value"])
        if not field_value(v):
            raise Refused("header_value")
        headers.append((h["name"], v))
    body = None
    if rest["body"] is not None:
        headers.append(("content-type", op["content_type"]))
        payload = rest["body"]
        raw = base64.b64decode(payload["bytes"])
        values = [filled(case, s["hole"]) for s in payload["holes"]]
        body, at = b"", 0
        for span, value in zip(payload["holes"], values):
            if span["offset"] < at or span["offset"] + span["len"] > len(raw):
                raise Refused("fill span")
            body += raw[at:span["offset"]] + written(span, value)
            at = span["offset"] + span["len"]
        body += raw[at:]
    return op["method"], target, headers, body


def main():
    for raw in (HERE.parent / "golden" / "frames.jsonl").read_text().splitlines():
        case = json.loads(raw)
        print(f"case {case['name']}")
        try:
            method, target, headers, body = build(case)
        except Refused as e:
            print(f"error {e}")
            continue
        print(f"method {method}\ntarget {target}")
        for n, v in headers:
            print(f"header {n}: {v}")
        print("body none" if body is None else f"body {body.hex()}")


if __name__ == "__main__":
    main()
