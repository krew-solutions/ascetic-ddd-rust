"""An answer's JSON by definition (ADR-0047 decision 15), outside the crate.

Reads, on stdin, one JSON object a line, as crates/command-log/tests/json.rs
writes them:

- {"doc": base64, "canon": text or null, "at": [[pointer, result], ...]}: the
  crate's reading of a document — its canonical form, or null when it found
  it unreadable — and what each pointer named in it ("absent", "malformed",
  or the canonical form of the value there);
- {"a": base64, "b": base64, "equal": bool}: whether the crate found two
  readable documents' values equal.

Decides each by Python's `json` (strict, its hooks refusing a member named
twice and `NaN`), `decimal` for numbers, and the rules below written here:
UTF-8 strictly, no byte order mark, no half of a surrogate pair, nesting at
most DEPTH, an exponent of at most EXPONENT_DIGITS digits once its leading
zeros are gone; RFC 6901 pointers. Stops at the first difference; prints what
it reached.

The canonical form, shared with the test only as a protocol: null `z`, true
`t`, false `f`, a number `n:0` or `n:[-]digits e exponent` (digits without
leading or trailing zeros), a string `s:` and its UTF-8 in hex, an array
`[a,b]`, an object `{name:value,...}` by name, each name in hex.

The standard library only. Run by crates/command-log/tests/json.rs.
"""

import base64
import collections
import decimal
import json
import re
import sys

sys.dont_write_bytecode = True

DEPTH = 128
EXPONENT_DIGITS = 18

REACHED = collections.Counter()


class Unreadable(Exception):
    pass


class Num(str):
    """A number, by its text."""


def number(text):
    m = re.fullmatch(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][-+]?([0-9]+))?", text)
    if m is None:
        raise Unreadable("a number json accepted against RFC 8259")
    if m.group(1) is not None and len(m.group(1).lstrip("0")) > EXPONENT_DIGITS:
        REACHED["exponent"] += 1
        raise Unreadable("exponent")
    return Num(text)


def members(pairs):
    names = [k for k, _ in pairs]
    if len(set(names)) != len(names):
        REACHED["duplicate"] += 1
        raise Unreadable("duplicate")
    return dict(pairs)


def constant(name):
    raise Unreadable(name)


def walk(v, depth):
    """The nesting, and no half of a surrogate pair in any string."""
    if isinstance(v, str) and not isinstance(v, Num):
        if any(0xD800 <= ord(c) <= 0xDFFF for c in v):
            REACHED["surrogate"] += 1
            raise Unreadable("surrogate")
        return
    if isinstance(v, (list, dict)):
        depth += 1
        if depth > DEPTH:
            REACHED["depth"] += 1
            raise Unreadable("depth")
        items = v.items() if isinstance(v, dict) else enumerate(v)
        for k, x in items:
            if isinstance(k, str):
                walk(k, depth - 1)
            walk(x, depth)


def read(raw):
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        REACHED["utf8"] += 1
        raise Unreadable("utf-8")
    try:
        v = json.loads(
            text,
            object_pairs_hook=members,
            parse_int=number,
            parse_float=number,
            parse_constant=constant,
        )
    except json.JSONDecodeError:
        REACHED["syntax"] += 1
        raise Unreadable("syntax")
    except RecursionError:
        REACHED["depth"] += 1
        raise Unreadable("depth")
    walk(v, 0)
    return v


def value(n):
    """A number's exact decimal value: (negative, digits, exponent), or zero."""
    sign, digits, exp = decimal.Decimal(n).as_tuple()
    ds = "".join(map(str, digits)).lstrip("0")
    if not ds:
        return (False, "", 0)
    stripped = ds.rstrip("0")
    return (bool(sign), stripped, exp + len(ds) - len(stripped))


def canon(v):
    if v is None:
        return "z"
    if v is True:
        return "t"
    if v is False:
        return "f"
    if isinstance(v, Num):
        neg, ds, exp = value(v)
        return "n:0" if not ds else f"n:{'-' if neg else ''}{ds}e{exp}"
    if isinstance(v, str):
        return "s:" + v.encode().hex()
    if isinstance(v, list):
        return "[" + ",".join(canon(x) for x in v) + "]"
    return "{" + ",".join(f"{k.encode().hex()}:{canon(v[k])}" for k in sorted(v)) + "}"


def equal(a, b):
    if isinstance(a, Num) or isinstance(b, Num):
        return isinstance(a, Num) and isinstance(b, Num) and value(a) == value(b)
    if isinstance(a, bool) or isinstance(b, bool) or a is None or b is None:
        return a is b
    if isinstance(a, str) or isinstance(b, str):
        return isinstance(a, str) and isinstance(b, str) and a == b
    if isinstance(a, list) or isinstance(b, list):
        return (
            isinstance(a, list)
            and isinstance(b, list)
            and len(a) == len(b)
            and all(equal(x, y) for x, y in zip(a, b))
        )
    return set(a) == set(b) and all(equal(a[k], b[k]) for k in a)


def pointer(doc, p):
    """RFC 6901."""
    if p == "":
        return canon(doc)
    if not p.startswith("/"):
        REACHED["malformed"] += 1
        return "malformed"
    here = doc
    for raw in p[1:].split("/"):
        if re.search(r"~(?![01])", raw):
            REACHED["malformed"] += 1
            return "malformed"
        token = raw.replace("~1", "/").replace("~0", "~")
        if isinstance(here, dict) and not isinstance(here, Num):
            if token not in here:
                REACHED["absent"] += 1
                return "absent"
            here = here[token]
        elif isinstance(here, list):
            if not re.fullmatch(r"0|[1-9][0-9]*", token) or int(token) >= len(here):
                REACHED["absent"] += 1
                return "absent"
            here = here[int(token)]
        else:
            REACHED["absent"] += 1
            return "absent"
    REACHED["found"] += 1
    return canon(here)


def main():
    cases = 0
    for raw in sys.stdin:
        case = json.loads(raw)
        cases += 1
        if "doc" in case:
            doc = base64.b64decode(case["doc"])
            try:
                v = read(doc)
                mine = canon(v)
                REACHED["readable"] += 1
            except Unreadable:
                v, mine = None, None
                REACHED["unreadable"] += 1
            if mine != case["canon"]:
                print(f"{doc!r}: by definition {mine}, the crate {case['canon']}")
                sys.exit(1)
            for p, got in case["at"]:
                want = pointer(v, p)
                if want != got:
                    print(f"{doc!r} at {p!r}: by definition {want}, the crate {got}")
                    sys.exit(1)
        else:
            a, b = (read(base64.b64decode(case[k])) for k in "ab")
            want = equal(a, b)
            REACHED["equal" if want else "unequal"] += 1
            if want != case["equal"]:
                print(f"{case['a']} {case['b']}: by definition {want}, the crate {case['equal']}")
                sys.exit(1)
    print(f"{cases} cases agree; reached {dict(REACHED)}")


if __name__ == "__main__":
    main()
