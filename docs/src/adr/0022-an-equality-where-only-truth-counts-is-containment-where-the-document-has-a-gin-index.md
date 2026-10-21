# ADR-0022: An equality where only truth counts is containment, where the document has a GIN index

## Status

Accepted (2026-10-21). Discharges the step ADR-0021 named among its
consequences.

## Context

ADR-0021 reads a member of a document as `(("p"."document" ->> 'status')
COLLATE "C") = $1`. A btree index on that very expression serves it; a GIN
index on the document as a whole does not, and a GIN index is what serves a
document filtered by any of its members without an index for each. The
faker of ascetic-ddd-python reads an equality as `@>` for that reason. The
two are not one condition, measured on PostgreSQL 14.24:

- Of a member that is a JSON null or is not there, the equality is null and
  containment false. Where only truth counts the two select alike; under
  `NOT`, `NOT (document @> …)` is true where `NOT (… = $1)` is null.
- A null constant: `= NULL` is null of every row, and `{"status": null}` is
  contained in a document whose status is a JSON null.
- A kind of several spellings: `to_jsonb` of a `timestamptz` is written in
  the session's time zone, `"2026-09-01T03:00:00+03:00"`; of a `double
  precision`, `-0` is `0` and `NaN` a string, and `0.10000000000000001` is
  `0.1` as a cast and not as JSON; a UUID has its case. A text, an integer
  and a boolean have one spelling.
- With a btree index on the expression and no GIN index, `@>` is a
  sequential scan where the equality was an index scan.
- Both side by side, `document @> … AND … = $1`, is a `BitmapAnd` of the two
  indexes estimated at 1 row of 50: the planner counts one condition twice.
- `jsonb_build_object` is stable: over 200 000 documents a sequential scan
  with it took 94 ms, with the same pattern of immutable functions 35 ms,
  which the server folds into a constant, `'{"status": "active"}'::jsonb`;
  the equality as it was, 24 to 34 ms.
- `any` over a collection with containment of its equalities beside its
  `EXISTS` read 7 143 rows of 50 000 through the GIN index; without it, a
  sequential scan of all 50 000 with the subquery on each.

Two designs, made independently from the task, arrived at the decision
below; they differ on `any` (see the alternatives).

## Decision

1. **A GIN index is the schema's to say**: `Schema::gin(table, column)` of a
   column declared a document. Without it the text of every query is what it
   was, byte for byte.

2. **Containment is written where only truth counts**: the condition as a
   whole, the operands of `AND` and `OR` there, and the predicate of `any`,
   where an item whose predicate is null is no witness. The operand of `NOT`
   and of every other operator is a value, and nothing there is containment.

3. **There, an equality of a member of such a document with a constant is
   containment** - `"p"."document" @> jsonb_object(ARRAY['status'],
   ARRAY[$1::text])` - if the member is a text, an integer or a boolean, and
   the constant is not null. The document is a column, of the candidate's
   row or of the row of an item of a table; an item of a document's array is
   no column, and no index has its rows.

4. **There, `any` over a collection of such a document has beside its
   `EXISTS` what the document contains if it is true**: `document @> … AND
   EXISTS (…)`, the pattern made of the equalities its predicate joins by
   `AND`, of the item and, under `any`, of the items of the item's own
   collections, in one object of the array. Two `any` over one collection are
   two objects of it; of two constants at one member the first is kept. What
   is left out - an `OR`, a range, a member of the candidate - is in the
   `EXISTS`, which is as it was. The pattern follows from the `EXISTS`, and
   the `EXISTS` is true or false, so the two are the `EXISTS` exactly.

5. **The pattern is of immutable functions**: `jsonb_object` of a text,
   `jsonb_set` of `'{}'` with `$n::bigint::text::jsonb` or
   `$n::boolean::text::jsonb`, `'[]'::jsonb ||` of an array, `||` between
   members. Its constants are parameters, as every constant is.

Held by: the text of each case in `tests/pg_compile.rs` (an equality of each
kind and of a nested member; `AND`, `OR`; `NOT`, `!=`, a null constant,
another kind, two members, an equality compared as a value, which stay;
`any` with equalities alone, with a range and a nested `any`, with two `any`
over one collection, with two constants at one member, with `OR`, under
`NOT`, `all`; a table's document); on a live server, in `tests/pg.rs`,
`a_specification_selects_the_rows_it_is_satisfied_by`, whose stores kept as
documents are read a fifth time under a schema that declares their GIN
index, with equalities in every position, null constants built by hand
among them; and `a_gin_index_serves_the_equalities_where_only_truth_counts`,
which has the server use the GIN index and fold the pattern into a constant.
Each was checked to fail under a mutation of what it guards: containment
under `NOT`, a null constant contained, `jsonb_build_object`.

## Consequences

- A btree index on a member's expression no longer serves its equality where
  the document is declared to have a GIN index; it serves order still. A
  member that wants both is a column beside the document.
- The compiled condition is what goes after `WHERE` and nothing else: the
  text negated selects the rows where the specification is null. A
  specification is negated in the tree; `Query::sql` says so already.
- Under a GIN index a member declared an integer, a boolean or a text must be
  a JSON number without a fraction, a boolean, a string. The JSON mapping of
  proto3 writes an `int64` as a string, which `->>` reads and containment does
  not. The repository's test that writes an aggregate and selects it by each
  member runs under the schema the repository compiles with.
- The planner estimates `@>` by its own rule, and an `EXISTS` beside
  containment at half the rows the containment has.
- Left for when they are wanted, each sound by the same argument: a boolean
  member alone as containment, `IS` with a constant (true or false in any
  position), `NOT NOT`, equalities of the candidate and of enclosing items in
  the pattern of `any`.

## Alternatives rejected

**Containment whether or not the schema says so.** A table indexed by a
btree on the expression goes from an index scan to a sequential one.

**Both, containment and the equality side by side.** The planner multiplies
their selectivities: 1 row estimated of 50.

**`any` of equalities alone as containment alone**, without its `EXISTS`.
Exact as well, and estimated better - the `EXISTS` beside containment
halves the estimate, 3 535 rows where the containment alone has 7 071, of
7 143 - and a second form: two constants at one member
would then have to be found and refused rather than one of them left out, and
a collection that is a JSON null would go from an error to a row not
selected. One form, whose exactness is the `EXISTS`'s, is kept.

**`jsonb_build_object`.** Stable: called for every row a sequential scan
reads, three times the time of the scan.

**JSONPath, `@@` or `@?`.** The same index serves the same equalities; the
operators take no variables, so the constants would be written into the
text.

**A pattern built by the compiler and sent as one `jsonb` parameter.** The
parameters are the tree's values: a value type would have to write itself as
JSON, and `Value` grow a variant for it.
