# ADR-0024: A GIN index on a path of a document serves containment of the value there

## Status

Accepted (2026-10-21). Discharges the step ADR-0023 named among its
consequences.

## Context

ADR-0022 writes an equality where only truth counts, and the equalities of
`any`, as containment of the whole document, `"g"."document" @> …`, which a
GIN index on the document serves. The target services index an array of a
document by an expression of its own, `CREATE INDEX … USING gin ((document
-> 'subgroups'))`: smaller than an index of the whole document, and built
for the one question asked of the array - which groups nest this one.
Measured on PostgreSQL 14.24, over 50 000 documents:

- `("g"."document" -> 'subgroups') @> '["s7"]'` is a bitmap scan of that
  index, 100 rows read for 100 selected; `"g"."document" @> '{"subgroups":
  ["s7"]}'` is a sequential scan, whatever the setting of `enable_seqscan`.
- The same with a path two deep and `jsonb_path_ops`.
- A scalar value contains itself, `'"x"'::jsonb @> '"x"'`.

The server matches an index by the expression, so the compiler must write
the one the index is on.

## Decision

1. **A GIN index on a path is the schema's to say**:
   `Schema::gin_at(table, column, "targeting.device_groups")` beside
   `Schema::gin(table, column)`, the path dotted as a path is written.

2. **Containment is written of the value at the path** where the member of
   an equality, or the collection of `any`, is under it:
   `("p"."document" -> 'targeting' -> 'device_groups') @> (…)`, the pattern
   being what the pattern of the whole document asks at that path - the
   array, the object, the constant. Where it asks something beside the
   path on the way there, which an equality and `any` do not, nothing is
   written of that path.

3. **Of several declared paths on the way, the deepest is written**: the
   index built for that member. A member under none of them is as it was.

Held by: the text of an array, an object and a constant at a path, of a
member not under it, and of the deepest of two in `tests/pg_compile.rs`; on
a live server in `tests/pg.rs`,
`a_specification_selects_the_rows_it_is_satisfied_by`, whose stores kept as
documents are read a sixth time under a schema that declares a GIN index on
their tags, against the evaluator; and
`a_gin_index_on_a_path_serves_the_containment_of_the_value_there`, which
has the server use the index on `(document -> 'subgroups')` with the
pattern folded, and select the rows the same condition without the index
selects.

## Consequences

- The rules of ADR-0022 stand: where only truth counts, of a text, an
  integer or a boolean, of a constant that is not null.
- A schema that declares an index the table does not have gets a condition
  no index serves, as under ADR-0022; the text is what it would be with the
  index.

## Alternatives rejected

**Containment of the whole document beside containment at the path.** One
condition counted twice by the planner (ADR-0022, 1 row estimated of 50).

**`?` of the array, as the services query it today.** True of a key of an
object and of a string that is no array, false of a number; and not served
by `jsonb_path_ops` (ADR-0023).
