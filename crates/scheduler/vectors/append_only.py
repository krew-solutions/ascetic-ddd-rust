"""A pinned file appended to, never edited (ADR-0047 K2b-7e-2b-3d-3b-4a).

Usage: append_only.py BASE FILE...

Each FILE's bytes at the git revision BASE must be a prefix of its bytes in
the working tree: a line appended passes, a line changed, removed or moved
fails, and so does a last line's end removed. A FILE absent at BASE passes. The standard library only; run by the
Makefile's `append-only`, which CI runs against the branch's base.
"""

import subprocess
import sys


def lines_at(base, path):
    shown = subprocess.run(["git", "show", f"{base}:{path}"], capture_output=True)
    if shown.returncode == 0:
        return shown.stdout
    exists = subprocess.run(["git", "cat-file", "-e", f"{base}^{{commit}}"], capture_output=True)
    if exists.returncode != 0:
        sys.exit(f"{base}: no such revision here")
    return None


def main():
    base, paths = sys.argv[1], sys.argv[2:]
    for path in paths:
        before = lines_at(base, path)
        if before is None:
            print(f"{path}: new since {base}")
            continue
        with open(path, "rb") as f:
            now = f.read()
        if not now.startswith(before):
            at = next((i for i, (a, b) in enumerate(zip(before, now)) if a != b), min(len(before), len(now)))
            line = before[:at].count(b"\n") + 1
            sys.exit(f"{path}: line {line} of {base}'s changed or removed; a pinned line is appended to, never edited")
        kept = before.count(b"\n")
        added = now.count(b"\n") - kept
        print(f"{path}: {kept} lines of {base} kept, {added} appended")


if __name__ == "__main__":
    main()
