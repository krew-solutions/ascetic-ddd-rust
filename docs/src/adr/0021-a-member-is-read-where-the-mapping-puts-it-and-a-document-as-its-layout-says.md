# ADR-0021: A member is read where the mapping puts it, and a document as its layout says

## Status

Accepted (2026-10-21).

Amended 2026-10-21 by ADR-0022: the consequence on equality and a GIN index
is discharged - where the schema says a document has a GIN index, an
equality where only truth counts is containment, and `any` has the
equalities of its predicate beside it as containment.

Amended 2026-10-21 by ADR-0023: the consequence on collections of scalars is
discharged - the element of a collection of values is the item itself,
`Expr::Item`, and a document's array of values is `Layout::values`.

Amended 2026-10-22 by ADR-0027: the consequence on a collection that is a
JSON null is withdrawn - whether `jsonb_array_elements` fails the query is
the plan's to decide, measured; under ADR-0027 a collection that is not an
array has no items.

## Context

A service that keeps an aggregate as one `jsonb` document has the
aggregate's members in two places: inside the document, and, for the few
that a listing filters or orders by, in columns beside it
(`aggregate_version`, `status`, `created_at`). The compiler read members of
columns only. A collection was `unnest` of an array of a composite type or a
table joined by its keys; "a `jsonb` array is not supported, as it is not in
the sources".

The question came as "compile a specification to JSONPath". JSONPath is the
notation of a document, and the crate parses templates of it; the query
language of the server is another matter. Measured on PostgreSQL 14.24:

- A null is a value of JSONPath. `$.a != 5` and `!($.a == 5)` are true of a
  null `a`, where requirement 16 of ADR-0011 has the row unselected; `!($.m ==
  1)` is true of a member that is not there. In lax mode an array under a
  scalar comparison is unwrapped: `$.t == "a"` is true of `["a", "b"]`.
- Arithmetic is `numeric`: `7 / 2` is `3.5`, and `i64::MAX + 1` is a value.
- An enclosing item has no name: there is `$`, `@` and a variable, and
  `Path::outer` cannot be written.
- `.datetime()` refuses the points in time the services write,
  `2026-09-01T12:00:00.123456789Z`: nine digits of a fraction, and `Z`.
- The JSONPath operators take no variables; `jsonb_path_match` with them is a
  sequential scan. A GIN `jsonb_path_ops` index narrows `@@` by equality and
  by `exists`, and not by order: `$.priority > 48` read all 20 000 rows of the
  index and rechecked them. A btree serves no JSONPath at all. The faker of
  ascetic-ddd-python met the same, and reads equality by `@>` for the index.

A document does not say the kinds of its members. A point in time, a date
and a UUID are strings in it; `3.0` is `3`. Compared as text, the midnight
`"2026-01-10T00:00:00.000000000Z"` is greater than `"2026-01-10"`, the same
instant: over a minute-by-minute table, 7041 rows against 7040 compared as
points in time.

And a column beside a document is not always the member it is named after.
In the target services an absent UPN is the empty text in its column and
`None` in the aggregate; a token's `spent` is a state derived when it is
written; a point in time is `TEXT`. A query that took such a column for the
member would select other rows than the evaluator, in silence.

Two designs, made independently from the task, arrived at the decision
below.

## Decision

1. **Where a member is read from is the `Mapping`'s, member by member.** Its
   answer is a column, `aggregate_version`, or a path in the document,
   `document.status`, as for any other storage (ADR-0014). Nothing reads it
   from a name: a column is taken for a member because the mapping says so,
   and a member moved between the document and a column is one answer of
   the mapping changed, no specification.

2. **What a document holds is the `Schema`'s**: `Schema::document(table,
   column, layout)`, beside `Schema::composite`. A `Layout` declares the
   members of each object of the document - a value of a `Kind`, an object, a
   collection of objects - and is what the encoding of the document writes,
   nothing of any aggregate. A path into a document that the layout does not
   declare as what the tree takes it for is `CompileError::UndeclaredMember`.

3. **A member of a document is SQL over `jsonb`, read as its kind.** `->`
   through the objects, `->>` of the member, then a cast - `bigint`, `double
   precision`, `boolean`, `timestamptz`, `date`, `uuid` - or, for a text,
   `COLLATE "C"`, which orders by code point as the evaluator does. The
   member is then a value of a type, as a column is: `->>` has SQL's null for
   a JSON null, the logic of nulls, the arithmetic, and a string constant
   read as the member's kind (ADR-0015) are the server's, as for a column.
   A null test reads `->>` of an object or a collection, and an object is no
   value to compare.

4. **A collection of a document is `EXISTS` over `jsonb_array_elements`**,
   whose rows are the objects, under the alias the compiler makes; the
   aliases of the enclosing queries are in scope, so `Path::outer` is what it
   is for the other storages. A document may be a column of the query's
   table, of a collection's table, or of an object's kept by a key.

Held by: the `UndeclaredMember` cases and the text of each kind, of null
tests, of nested and enclosing collections, and of a document in a child
table or behind a key, in `tests/pg_compile.rs`; on a live server in
`tests/pg.rs`: `a_specification_selects_the_rows_it_is_satisfied_by` over
the stores kept as documents, read wholly from the document and with two
members read from columns the server generates from it, against the
evaluator; `a_member_of_a_document_is_compared_as_its_declared_kind` for the
kinds a document does not say, at the midnight boundary, with a UUID in
capitals and a text ordered by code point; and
`an_index_on_the_expression_of_a_member_serves_its_comparisons`, which has
the server use an index on the expression the compiler writes, and refuse
one on a cast to `timestamptz`.

## Consequences

- A member compared by order is a column: one the server generates from the
  document, `GENERATED ALWAYS AS ((document ->> 'priority')::bigint) STORED`,
  is the member by construction and has an index of its own; or an index on
  the very expression the compiler writes, `((document ->> 'priority')::bigint)`,
  `((document ->> 'status') COLLATE "C")`. An index on the text without the
  collation does not serve the query. A point in time or a date has neither:
  `timestamptz_in` and `date_in` are `STABLE`, and the server refuses them in
  an index and in a generated column. It is a column the repository writes,
  of its type.
- A member of a document that is not in it reads as null, where the
  evaluator says it is missing; a layout that differs from the encoding has a
  query select nothing, in silence. A repository holds the two together by a
  test that writes an aggregate and selects it by each member the layout
  declares. A document of an older shape that the repository upcasts on the
  way to the aggregate is rewritten before a query goes into it.
- A point in time written with nanoseconds disagrees at a boundary: the
  server rounds to microseconds, `.999999999Z` is the next second, and
  `Timestamp` has microseconds. The encoding writes microseconds.
- A collection that is a JSON null fails the whole query in
  `jsonb_array_elements`, where the evaluator fails that candidate. An empty
  collection is an empty array.
- Equality is not yet served by a GIN index: `(("p"."document" ->> 'status')
  COLLATE "C") = $1` is no `@>`. Where only truth counts - the root, its
  `AND` and `OR`, the predicate of `any` - an equality of a text, an integer
  or a boolean is true exactly when `"document" @> {"status": …}` is, and
  that is the next step, held by the same agreement test. Under `NOT` it is
  not: `NOT (document @> …)` is true of a null.
- A collection of scalars - the groups a policy targets - cannot be named:
  the tree names a member of an item, never the item itself. That is a
  change of the tree, and a decision of its own.
- `CompileError` has a new variant: an exhaustive `match` on it outside the
  crate stops compiling.

## Alternatives rejected

**JSONPath as the query.** Each difference above can be mended by itself -
a guard for nulls, a pair of conditions per node for the three values,
`(a - a % b) / b` for division, the constants rewritten as the encoding
writes them - and the mends together are a second compiler of other
semantics, in which an enclosing item still cannot be named, and whose index
serves equality alone, which `@>` serves too.

**A column found by its name**, the schema looking for a column of the
member's name before it looks into the document. It reads the empty UPN, the
derived `spent` and the `TEXT` point in time as the members, and selects
other rows in silence; the mapping is where a member is said to be a column,
because only a statement can say the column is the member.

**The kind from the constant beside the member.** There is none beside a
member compared with a member, nor under a null test; and a string beside a
point in time is the case ADR-0015 reads as a point in time, which would be
compared as text.

**The kind in the tree**, a cast node. The tree is the domain's, and a kind
of the storage in it is what ADR-0014 keeps in one place.

**The layout in the mapping.** The mapping is of the aggregate's members and
is asked one member at a time; the kinds are the encoding's, and the
compiler needs them where the mapping is not asked - an item of a table's
document, an object behind a key.
