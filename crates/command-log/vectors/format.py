"""The command log's byte form, checked outside its implementation (ADR-0046, F2).

Reads a log, and for every line:
- validates it against the normative schema (schema/line.schema.json), by a
  validator of the schema's own subset written here, the standard library only;
- requires every object's keys in the schema's properties' order: the canonical
  order;
- encodes the parsed line again, compactly, and requires the same bytes;
then recomputes the trailer's count and SHA-256. Exits non-zero on the first
difference.

Run: python3 -I crates/command-log/vectors/format.py crates/command-log/golden/c1.jsonl
"""

import hashlib
import json
import pathlib
import re
import sys

SCHEMA = json.loads((pathlib.Path(__file__).parent.parent / "schema" / "line.schema.json").read_text())


class Invalid(Exception):
    pass


def resolve(schema):
    while "$ref" in schema:
        schema = SCHEMA["$defs"][schema["$ref"].rsplit("/", 1)[1]]
    return schema


def kind_of(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "other"


def check(value, schema, where):
    """Valid against `schema`, objects' keys in its order; raises Invalid."""
    schema = resolve(schema)
    if "oneOf" in schema:
        passing = []
        for branch in schema["oneOf"]:
            try:
                check(value, branch, where)
                passing.append(branch)
            except Invalid:
                pass
        if len(passing) != 1:
            raise Invalid(f"{where}: {len(passing)} of oneOf's branches hold")
        return
    if "const" in schema and value != schema["const"]:
        raise Invalid(f"{where}: {value!r} is not {schema['const']!r}")
    if "enum" in schema and value not in schema["enum"]:
        raise Invalid(f"{where}: {value!r} not in {schema['enum']}")
    expected = schema.get("type")
    if expected and kind_of(value) != expected:
        raise Invalid(f"{where}: a {kind_of(value)}, not a {expected}")
    if "pattern" in schema and not re.fullmatch(schema["pattern"], value):
        raise Invalid(f"{where}: {value!r} does not match {schema['pattern']}")
    if "minimum" in schema and value < schema["minimum"]:
        raise Invalid(f"{where}: below {schema['minimum']}")
    if "maximum" in schema and value > schema["maximum"]:
        raise Invalid(f"{where}: above {schema['maximum']}")
    if isinstance(value, dict):
        props = schema.get("properties", {})
        keys = list(value)
        for k in schema.get("required", []):
            if k not in value:
                raise Invalid(f"{where}: {k} missing")
        if schema.get("additionalProperties") is False:
            extra = [k for k in keys if k not in props]
            if extra:
                raise Invalid(f"{where}: unknown {extra}")
        if keys != [p for p in props if p in value]:
            raise Invalid(f"{where}: keys {keys} out of the schema's order {list(props)}")
        for k, v in value.items():
            check(v, props.get(k, {}), f"{where}/{k}")
    if isinstance(value, list):
        for i, v in enumerate(value):
            check(v, schema.get("items", {}), f"{where}/{i}")


def no_duplicates(items):
    keys = [k for k, _ in items]
    if len(keys) != len(set(keys)):
        raise Invalid(f"a duplicate key in {keys}")
    return dict(items)


def main(path):
    data = pathlib.Path(path).read_bytes()
    if not data.endswith(b"\n"):
        raise SystemExit("the log ends without its trailer, or mid-line")
    lines = data[:-1].split(b"\n")
    digest = hashlib.sha256()
    records = 0
    for number, raw in enumerate(lines, start=1):
        try:
            line = json.loads(raw.decode("utf-8"), object_pairs_hook=no_duplicates)
            check(line, SCHEMA, f"line {number}")
        except (Invalid, ValueError) as e:
            raise SystemExit(str(e))
        again = json.dumps(line, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if again != raw:
            raise SystemExit(f"line {number}: not canonical\n  read  {raw[:120]!r}\n  wrote {again[:120]!r}")
        kind = next(iter(line))
        if (number == 1) != (kind == "header"):
            raise SystemExit(f"line {number}: the header must be the first line, and only it")
        if kind == "step":
            records += 1
        if kind == "trailer":
            if number != len(lines):
                raise SystemExit("a line follows the trailer")
            expected = {"records": str(records), "sha256": digest.hexdigest()}
            if line["trailer"] != expected:
                raise SystemExit(f"the trailer says {line['trailer']}, the log is {expected}")
            print(f"{path}: {records} steps, sha256 of the file {hashlib.sha256(data).hexdigest()}")
            return
        digest.update(raw + b"\n")
    raise SystemExit("the log ends without its trailer")


if __name__ == "__main__":
    main(sys.argv[1])
