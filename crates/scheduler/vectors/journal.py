"""The scheduler's journal's byte form, checked outside its implementation
(ADR-0047 decision 14, K1e-3c).

Reads a journal, and for every line:
- validates it against the normative schema (schema/journal.schema.json), by
  the command log's validator of the schema's subset
  (crates/command-log/vectors/format.py), the standard library only;
- requires every object's keys in the schema's properties' order: the canonical
  order;
- encodes the parsed line again, compactly, and requires the same bytes;
then requires the header first and only there, and recomputes the trailer's
counts and SHA-256. Exits non-zero on the first difference.

Run: python3 -I crates/scheduler/vectors/journal.py crates/scheduler/golden/journal.jsonl
"""

import hashlib
import importlib.util
import json
import pathlib
import sys

# The command log's checker is loaded, not imported: no bytecode beside it.
sys.dont_write_bytecode = True
HERE = pathlib.Path(__file__).resolve().parent
SCHEMA = json.loads((HERE.parent / "schema" / "journal.schema.json").read_text())
_spec = importlib.util.spec_from_file_location(
    "format", HERE.parent.parent / "command-log" / "vectors" / "format.py"
)
fmt = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fmt)


def main(path):
    # The schema's title names the version its header's `const` holds: a bump
    # that moves one and not the other is refused (K2b-7e-2b-3e).
    version = SCHEMA["$defs"]["header"]["properties"]["journal"]["const"]
    if not SCHEMA["title"].endswith(f"version {version}"):
        raise SystemExit(f"the schema's title is not its version {version}'s: {SCHEMA['title']}")
    data = pathlib.Path(path).read_bytes()
    if not data.endswith(b"\n"):
        raise SystemExit("the journal ends without its trailer, or mid-line")
    lines = data[:-1].split(b"\n")
    digest = hashlib.sha256()
    events = steps = 0
    for number, raw in enumerate(lines, start=1):
        try:
            line = json.loads(raw.decode("utf-8"), object_pairs_hook=fmt.no_duplicates)
            fmt.check(line, SCHEMA, f"line {number}", SCHEMA)
        except (fmt.Invalid, ValueError) as e:
            raise SystemExit(str(e))
        again = json.dumps(line, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if again != raw:
            raise SystemExit(f"line {number}: not canonical\n  read  {raw[:120]!r}\n  wrote {again[:120]!r}")
        kind = next(iter(line))
        if (number == 1) != (kind == "header"):
            raise SystemExit(f"line {number}: the header must be the first line, and only it")
        if kind == "trailer":
            if number != len(lines):
                raise SystemExit("a line follows the trailer")
            expected = {"events": str(events), "steps": str(steps), "sha256": digest.hexdigest()}
            if line["trailer"] != expected:
                raise SystemExit(f"the trailer says {line['trailer']}, the journal is {expected}")
            print(f"{path}: {events} lines, {steps} steps, sha256 of the file {hashlib.sha256(data).hexdigest()}")
            return
        if kind != "header":
            events += 1
        if kind == "end":
            steps += 1
        digest.update(raw + b"\n")
    raise SystemExit("the journal ends without its trailer")


if __name__ == "__main__":
    main(sys.argv[1])
