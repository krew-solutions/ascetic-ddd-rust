"""The command log's rendered values, computed outside its implementation (ADR-0046, H2).

A rendered value is a function of the run's namespace, what it is rendered of
(a subject or a step) and a role:

    key = root(namespace) . child("ascetic-ddd-command-log/rendered") . part(of) . child(role)

with faker's derivations, computed here by faker's own independent vectors
(crates/faker/vectors/key_vectors.py, SipHash-2-4-128 from its specification).
A UUID is the key's id under the namespace, the same for its text and its
bytes. Hex is the first words of `key.child("hex").at(0)`, `.at(1)`, ...,
little-endian, cut to its length.

Key material is not drawn from the namespace (ADR-0047 decision 15): a
`Secret32` is HMAC-SHA256 under the run's secret of
`b"ascetic-ddd-command-log/secret32"`, the variant's byte (0 a subject, 1 a
step), the subject's kind, scope and index, a step's label and occurrence, and
the role — each text or bytes after its length as eight bytes little-endian,
an index as eight bytes and an occurrence as four. A step's correlation id is
the first 16 bytes, in hex, of the words of
`root(namespace).child("ascetic-ddd-command-log/correlation").part(step).at(attempt).child("id")`.

A time is microseconds since the Unix epoch, written as RFC 3339 in UTC with
six decimals (years 0000 to 9999, proleptic Gregorian) or as epoch milliseconds
rounded down; computed here by `datetime`, a year 0 by its 400-year cycle.

Prints `golden/render.txt`, which `tests/render.rs` checks the crate against
and `make vectors` checks this against.

Run: python3 -I crates/command-log/vectors/render.py
"""

import hashlib
import hmac
import pathlib
import struct
import sys
import uuid
from datetime import datetime, timedelta, timezone

sys.dont_write_bytecode = True
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "faker" / "vectors"))
import key_vectors as k  # noqa: E402

PATH = "ascetic-ddd-command-log/rendered"


def text(v):
    data = v.encode()
    return k.leb128(len(data)) + data


def subject(kind, scope, index):
    return text(kind) + k.leb128(len(scope)) + scope + struct.pack("<Q", index)


def of_subject(kind, scope, index):
    return bytes([0]) + subject(kind, scope, index)


def of_step(kind, scope, index, label, n):
    return bytes([1]) + subject(kind, scope, index) + text(label) + struct.pack("<I", n)


def key(namespace, of, role):
    return k.child(k.part(k.child(k.root(namespace), PATH), of), role)


def words(key_, family, n):
    base = k.child(key_, family)
    out = b""
    i = 0
    while len(out) < n:
        out += struct.pack("<Q", k.at(base, i)[0])
        i += 1
    return out[:n]


def field(b):
    return struct.pack("<Q", len(b)) + b


def secret32(secret, kind, scope, index, step, role):
    msg = b"ascetic-ddd-command-log/secret32" + bytes([0 if step is None else 1])
    msg += field(kind.encode()) + field(scope) + struct.pack("<Q", index)
    if step is not None:
        msg += field(step[0].encode()) + struct.pack("<I", step[1])
    msg += field(role.encode())
    return hmac.new(secret, msg, hashlib.sha256).digest()


def correlation(namespace, kind, scope, index, label, n, attempt):
    step = subject(kind, scope, index) + text(label) + struct.pack("<I", n)
    kk = k.at(k.part(k.child(k.root(namespace), "ascetic-ddd-command-log/correlation"), step), attempt)
    return words(kk, "id", 16).hex()


SECRETS = {"A": bytes(range(32)), "B": bytes(range(32, 64))}


EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
CYCLE = timedelta(days=146097)


def rfc3339(micros):
    t = EPOCH + timedelta(microseconds=micros) if micros >= -62135596800000000 else None
    if t is None:
        # Year 0 is year 400 less one 400-year cycle.
        t = EPOCH + (timedelta(microseconds=micros) + CYCLE)
        assert t.year == 400, t
        return "0000" + t.strftime("-%m-%dT%H:%M:%S.%fZ")
    return t.strftime("%Y-%m-%dT%H:%M:%S.%fZ").rjust(27, "0")


def times():
    """Microseconds at the calendar's edges: the epoch and its neighbours, the
    years' turns and the end of February in every hundredth year — where the
    century corrections of a civil-date algorithm act — and the range's ends."""
    def at(year, month, day, h=0, m=0, s=0, us=0):
        if year == 0:
            t = datetime(400, month, day, h, m, s, us, tzinfo=timezone.utc) - EPOCH - CYCLE
            return t // timedelta(microseconds=1)
        return (datetime(year, month, day, h, m, s, us, tzinfo=timezone.utc) - EPOCH) // timedelta(microseconds=1)

    out = [0, 1, -1, 1763640000123456, 32472144000000000]
    years = sorted(set(range(0, 10000, 100)) | {1, 4, 99, 101, 1969, 1970, 1999, 2001, 2024, 2025, 9999})
    for y in years:
        leap = y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)
        if y > 0:
            out.append(at(y, 1, 1))
        out.append(at(y, 2, 28, 23, 59, 59, 999999))
        if leap:
            out.append(at(y, 2, 29))
        out.append(at(y, 3, 1))
        out.append(at(y, 12, 31, 23, 59, 59, 999999))
    out.append(at(0, 1, 1))
    return out


def main():
    ofs = {
        "device 7 of tenant [1,2]": ("device", bytes([1, 2]), 7, None),
        "device 8 of tenant [1,2]": ("device", bytes([1, 2]), 8, None),
        "step managed#0 of device 7": ("device", bytes([1, 2]), 7, ("managed", 0)),
    }

    def of(kind, scope, index, step):
        if step is None:
            return of_subject(kind, scope, index)
        return of_step(kind, scope, index, step[0], step[1])

    for namespace in [1, 2]:
        for name, o in ofs.items():
            for role in ["id", "message"]:
                kk = key(namespace, of(*o), role)
                u = k.rendered(kk, namespace)
                print(f"ns {namespace} | {name} | {role} | uuid {u} | bytes {uuid.UUID(u).bytes.hex()}")
                print(f"ns {namespace} | {name} | {role} | hex 5 {words(kk, 'hex', 5).hex()}")
    for s_name, secret in SECRETS.items():
        for name, o in ofs.items():
            for role in ["id", "message"]:
                print(f"secret {s_name} | {name} | {role} | secret32 {secret32(secret, *o, role).hex()}")
    for namespace in [1, 2]:
        for attempt in [1, 2]:
            c = correlation(namespace, "device", bytes([1, 2]), 7, "managed", 0, attempt)
            print(f"correlation ns {namespace} | step managed#0 of device 7 | attempt {attempt} | {c}")
    for micros in times():
        print(f"time {micros} | rfc3339 {rfc3339(micros)} | millis {micros // 1000}")


if __name__ == "__main__":
    main()
