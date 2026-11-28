"""The whole-run measurement's rows (ADR-0047 decision 22, K2b-7e-2b-3d-4a).

Usage: rows.py BINARY OUT.jsonl [ROW ...]

BINARY is load-run's `whole` test binary, built with --release. Each row runs
in a scope of its own without swap and with an explicit memory.max, its stand
in another scope; the row's line (`WHOLE {...}`) and its journal are read
back, and each window's counted peak is set beside the heap-basis terms of its
phase as the journal's header carries them, with the realised counts that
guard it. Prints a row a line; appends the readings to OUT.jsonl. The
standard library only.
"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time

STAND = r'''
import socket, sys, threading
port, mode, n, size = int(sys.argv[1]), sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
lock, out, released = threading.Lock(), [0], threading.Event()
body = b'"' + b"a" * max(size - 2, 0) + b'"'
def sent_all_but_the_last():
    with lock:
        out[0] += 1
        if out[0] >= n:
            released.set()
    released.wait()
def serve(c):
    buf = b""
    try:
        while True:
            d = c.recv(65536)
            if not d:
                break
            buf += d
            while b"\r\n\r\n" in buf:
                head, _, buf = buf.partition(b"\r\n\r\n")
                length = 0
                for h in head.split(b"\r\n")[1:]:
                    k, _, v = h.partition(b":")
                    if k.strip().lower() == b"content-length":
                        length = int(v)
                while len(buf) < length:
                    buf += c.recv(65536)
                buf = buf[length:]
                if mode == "never":
                    threading.Event().wait()
                elif mode == "now":
                    c.sendall(b"HTTP/1.1 200 X\r\ncontent-type: application/json\r\ncontent-length: "
                              + str(len(body)).encode() + b"\r\n\r\n" + body)
                elif mode == "held":
                    c.sendall(b"HTTP/1.1 200 X\r\ncontent-type: application/json\r\ncontent-length: "
                              + str(len(body)).encode() + b"\r\n\r\n" + body[:-1])
                    sent_all_but_the_last()
                    c.sendall(body[-1:])
                elif mode == "chunked":
                    c.sendall(b"HTTP/1.1 200 X\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\n\r\n"
                              + hex(len(body))[2:].encode() + b"\r\n" + body + b"\r\n")
                    sent_all_but_the_last()
                    c.sendall(b"0\r\n\r\n")
    except OSError:
        pass
    finally:
        c.close()
l = socket.socket()
l.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
l.bind(("127.0.0.1", port))
l.listen(8192)
while True:
    c, _ = l.accept()
    threading.Thread(target=serve, args=(c,), daemon=True).start()
'''

MIB = 1 << 20

# A row: the stand's mode, the reads at one instant, each answer's bytes, each
# request's body, the sockets' cap, the operation's timeout (µs), a zstd log,
# the scope's memory.max.
ROWS = {
    "L0": dict(mode="now", n=1, size=64, body=0, cap=16, timeout=3_000_000, zstd=0, most="1G"),
    "L5": dict(mode="now", n=1, size=64, body=0, cap=16, timeout=3_000_000, zstd=1, most="1G"),
    "L1-125": dict(mode="held", n=125, size=MIB, body=0, cap=256, timeout=20_000_000, zstd=0, most="3G"),
    "L1-250": dict(mode="held", n=250, size=MIB, body=0, cap=256, timeout=20_000_000, zstd=0, most="3G"),
    "L1c-250": dict(mode="chunked", n=250, size=2, body=0, cap=256, timeout=20_000_000, zstd=0, most="3G"),
    "L2-200-1k": dict(mode="never", n=200, size=0, body=1024, cap=512, timeout=500_000, zstd=0, most="2G"),
    "L2-400-1k": dict(mode="never", n=400, size=0, body=1024, cap=512, timeout=500_000, zstd=0, most="2G"),
    "L2-400-64k": dict(mode="never", n=400, size=0, body=65536, cap=512, timeout=500_000, zstd=0, most="2G"),
}


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def listening(port, within=10.0):
    end = time.monotonic() + within
    while time.monotonic() < end:
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.05)
    sys.exit(f"the stand never listened on {port}")


def journal_of(path):
    """The header's memory and the realised counts: the attempts in flight at
    once (sent, not answered) and the steps ended while their attempt was
    not yet answered, each at its most."""
    with open(path) as f:
        lines = [json.loads(l) for l in f]
    memory = lines[0]["header"]["admission"]["memory"]
    inflight = most_inflight = late = most_late = 0
    ended, answered = set(), set()
    for l in lines[1:-1]:
        (kind, v), = l.items()
        if kind == "sent":
            inflight += 1
            most_inflight = max(most_inflight, inflight)
        elif kind == "answer":
            if v.get("ignored") is None:
                inflight = max(0, inflight - 1)
            key = v["line"]
            answered.add(key)
            if key in ended:
                late = max(0, late - 1)
        elif kind == "end":
            key = v["line"]
            ended.add(key)
            if key not in answered:
                late += 1
                most_late = max(most_late, late)
    return memory, most_inflight, most_late


def sums(memory):
    """The heap-basis terms by phase, and named terms' counts and costs."""
    out = {p: 0 for p in ("fixed", "stages", "gates", "run", "pool")}
    named = {}
    for t in memory["terms"]:
        if t["basis"] == "heap":
            out[t["phase"]] += int(t["bytes"])
        named.setdefault(t["part"], []).append((int(t["count"]), int(t["cost"]), int(t["rest"]), int(t["bytes"])))
    return out, named


def row(binary, name):
    r = ROWS[name]
    port = free_port()
    with tempfile.TemporaryDirectory(dir=os.environ.get("WHOLE_TMP")) as d:
        script = os.path.join(d, "stand.py")
        with open(script, "w") as f:
            f.write(STAND)
        stand = subprocess.Popen(
            ["systemd-run", "--user", "--scope", "-q", "--", "python3", "-I", script, str(port), r["mode"], str(r["n"]), str(r["size"])],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            listening(port)
            env = {"WHOLE_PORT": str(port), "WHOLE_N": str(r["n"]), "WHOLE_BODY": str(r["body"]), "WHOLE_CAP": str(r["cap"]),
                   "WHOLE_TIMEOUT": str(r["timeout"]), "WHOLE_ZSTD": str(r["zstd"]), "WHOLE_DIR": d}
            cmd = ["systemd-run", "--user", "--scope", "-q", "-p", f"MemoryMax={r['most']}", "-p", "MemorySwapMax=0", "--", "env"]
            cmd += [f"{k}={v}" for k, v in env.items()]
            cmd += [binary, "--ignored", "--exact", "row", "--nocapture", "--test-threads=1"]
            done = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            got = [l[l.index("WHOLE {") + len("WHOLE "):] for l in done.stdout.splitlines() if "WHOLE {" in l]
            if done.returncode or not got:
                sys.exit(f"{name}: {done.returncode}\n{done.stdout[-3000:]}\n{done.stderr[-3000:]}")
            line = json.loads(got[0])
            memory, inflight, late = journal_of(line["journal"])
        finally:
            stand.terminate()
            stand.wait(timeout=10)
    phases, named = sums(memory)
    return {"row": name, **r, "need": line["need"], "edges": line["edges"], "phases": phases,
            "named": named, "inflight": inflight, "late": late}


def window(reading, edge):
    """An edge's window: its counted peak past the base's heap."""
    base = reading["edges"][0]["now"]
    e = next(x for x in reading["edges"] if x["edge"] == edge)
    return e["peak"] - base


def main():
    binary, out, names = sys.argv[1], sys.argv[2], sys.argv[3:] or list(ROWS)
    for name in names:
        reading = row(binary, name)
        with open(out, "a") as f:
            f.write(json.dumps(reading) + "\n")
        p = reading["phases"]
        fixed = p["fixed"]
        print(f"{name}: need {reading['need']}, in flight {reading['inflight']}, late {reading['late']}")
        for edge, phase in (("stage 2", "stages"), ("stage 3", "gates"), ("ran", "run")):
            sum_ = fixed + p[phase] + (p["pool"] if phase == "run" else 0)
            got = window(reading, edge)
            print(f"  {edge}: counted {got} of {sum_} ({got / sum_:.3f})")


if __name__ == "__main__":
    main()
