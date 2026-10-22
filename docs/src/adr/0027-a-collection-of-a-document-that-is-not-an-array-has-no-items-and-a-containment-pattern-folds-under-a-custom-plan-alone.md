# ADR-0027: A collection of a document that is not an array has no items, and a containment pattern folds under a custom plan alone

## Status

Accepted (2026-10-23), decisions 1 and 2; decision 3 is deferred, the
owner's to decide. Proposed 2026-10-22. A review of ADR-0021 to ADR-0026
from a premise the two designs behind them did not start from - the
construction of interpreters for a DSL - read against the code, the tests
and PostgreSQL 14.24. Each finding below is a measurement, and the SQL that
makes it is in the text.

Decision 1 is held by the tests named under it. Decision 2 bounds a
measurement and changes no query; the crate's README and its `containment`
module state the bound. Decision 3 is deferred: a test gate is the weakest
of three executors for the finding it answers, and no repository in this
workspace declares an index yet; the choice among the three is under the
decision, with the design of the gate kept for when it is taken up.

Amends ADR-0021 (the consequence on a collection that is a JSON null) and
ADR-0022 (the reach of its measurement of the fold).

## Context

What the review found sound, so that the record says it was looked at:

- `Stand` is an analysis of the polarity of null. A sub-condition may be
  replaced by one that is false where it was null only where that changes
  no row: at the root, under `AND` and `OR` there, and as the predicate of
  `any`, which `EXISTS` makes true or false anyway. `NOT`, a comparison,
  `IS` and arithmetic are no such places. The compiler passes
  `Stand::Truth` through exactly those and nothing else; every arm of
  `render` was checked against this.
- `Pattern` is a lower bound on the predicate: what a document contains if
  the predicate is true of it. Each rule of its `and` keeps the implication
  or weakens it - objects merge by conjunction, two `any` over one array ask
  for two elements, of two constants at one member the first is kept - so
  `pattern AND EXISTS` is the `EXISTS`, and the `EXISTS` alone carries the
  exactness. The mutation checks of ADR-0022 hold this.
- The count of parameters and aliases goes into every step and comes out of
  it, parentheses follow associativity, and no name reaches the text
  unchecked.

The findings, measured in a database made for them and dropped.

**A collection that is a JSON null fails the query only when the plan says
so.** ADR-0021 has it fail in `jsonb_array_elements`, and the README's
limits say so. Over four documents - `"items": [{"s": "a"}]`, `"items":
null`, no `items`, `"items": []` - with a GIN index on the document:

```sql
-- the EXISTS alone, as compiled without the index declared
SELECT id FROM docs
WHERE EXISTS (SELECT 1 FROM jsonb_array_elements(document -> 'items') i
              WHERE (i ->> 's') COLLATE "C" = 'a');
-- ERROR:  cannot extract elements from a scalar

-- containment beside it, as compiled with the index declared (ADR-0022)
SELECT id FROM docs
WHERE document @> '{"items": [{"s": "a"}]}'
  AND EXISTS (SELECT 1 FROM jsonb_array_elements(document -> 'items') i
              WHERE (i ->> 's') COLLATE "C" = 'a');
-- id = 1, no error
```

The planner orders the operands of `AND` by cost and evaluates `@>` first,
whatever their order in the text and with a sequential scan forced; where
it is false the `EXISTS` is not evaluated, and the row with the null is not
selected, in silence. So the same specification over the same rows fails
under a schema without `gin` and selects under a schema with it. Without
`gin` the failure is not reliable either: any cheaper condition beside the
`EXISTS` - `"published" AND EXISTS (…)` - covers the rows it is false of.
What fails is the plan's to decide, not the text's.

**The pattern folds under a custom plan and not under a generic one.**
ADR-0022 chose `jsonb_object` and `jsonb_set`, immutable, so that the
server folds the pattern into a constant, and measured the fold on
literals. A parameter is a constant to the planner under a custom plan
only. Under a generic plan - a named prepared statement after its fifth
execution with `plan_cache_mode = auto`, or `force_generic_plan` -
`jsonb_object(…, ARRAY[$1])` stays a call in the filter and is evaluated
for every row a sequential scan reads. Over 200 000 documents, a sequential
scan forced, no parallel workers, the second of two executions:

| Condition | custom plan | generic plan |
| --- | --- | --- |
| `document @> jsonb_object(ARRAY['status'], ARRAY[$1::text])`, the pattern as written | 29 ms, folded | 58 ms, not folded |
| `document @> jsonb_build_object('status', $1::text)` | 85 ms | 86 ms |
| `document @> $1`, one `jsonb` parameter | - | 26 ms |
| `(document ->> 'status') COLLATE "C" = $1`, the equality as it was | 18 ms | 20 ms |

Where the index serves the condition the key is computed once per scan
under either plan: 0.35 ms for the same count through the GIN index. The
penalty is paid where the index does not serve - declared in the schema and
not built, or a value the planner takes for common enough to scan - and
there, under a generic plan, the pattern costs three times the equality it
replaced.

**A kind the layout declares and the encoding does not write diverges in
both directions.** ADR-0022 names one: an integer written as a string, as
the JSON mapping of proto3 writes an `int64`, is read by `->>` and not
contained. The mirror holds as well: `"n": 100.0` is contained by `{"n":
100}`, jsonb comparing numbers by value, and `(document ->> 'n')::bigint`
is "invalid input syntax for type bigint". Under `gin` the row is selected,
without it the query fails.

**An index the schema declares is a claim nothing checks.** ADR-0024 says
so of itself. The crate holds its own claims by `EXPLAIN` in `tests/pg.rs`;
a repository of a service that declares `gin` or indexes an expression has
no such test, and after the measurement above the cost of a claim that is
false is not a missed speed-up but a condition three times dearer than the
one it replaced.

## Decision

1. **A collection of a document that is not an array has no items.** The
   compiler reads an array of a document through a guard,
   `jsonb_array_elements(CASE WHEN jsonb_typeof(x) = 'array' THEN x END)`,
   and `jsonb_array_elements_text` likewise. A JSON null, an object, a
   scalar and a member that is not there are alike no rows: `any` is false
   of them and `all` true, as both are of a member that is not there
   today, and as a member that is not there reads as null (ADR-0021). The
   text says what a row is; the plan does not. The guard is immutable and
   costs about a sixth of a full scan of the subquery, 133 ms against 115
   ms over 200 000 documents with a sequential scan forced, and nothing
   where the index serves. Held by: the text in `tests/pg_compile.rs`,
   `a_collection_in_a_document_is_read_with_jsonb_array_elements` and every
   `EXISTS` of a document there; in `tests/pg.rs`,
   `a_collection_of_a_document_that_is_not_an_array_has_no_items` - seven
   documents, an array, an array with a null member, a JSON null, no
   member, an empty array, an object and a scalar, under the schema with
   and without `gin`, under `any`, `all` and `NOT any`, with
   `enable_seqscan` either way, the same rows selected; and the evaluator's
   side in the same test - it refuses the four candidates whose collection
   is no collection or is not there, and evaluates the three arrays as the
   server selects them. Checked to fail without the guard: "cannot extract
   elements from a scalar" on the `EXISTS` alone, at the null.

2. **The fold is a property of a custom plan, and the pattern stays as it
   is.** `jsonb_object` and `jsonb_set` remain right: never dearer than
   `jsonb_build_object`, folded where a fold happens, and the key of an
   index scan computed once under either plan. ADR-0022's measurement is
   bounded to the custom plan in its status, with the generic plan's
   numbers beside it. A repository that keeps named prepared statements and
   has no index for a condition pays the penalty, which decision 3 is for.

3. **An index the schema declares is verified where the schema is used.**
   The crate offers, behind the `pg` feature, a function for a repository's
   tests: given a client, a schema, a specification and the name of an
   index, it compiles, runs `EXPLAIN` with the parameters and requires the
   plan to name the index. Each repository runs it over the members its
   mapping exposes under a GIN or an expression index, in the test ADR-0021
   already asks of it - the one that writes an aggregate and selects it by
   every member the layout declares. To be held by: the three `EXPLAIN`
   tests of `tests/pg.rs` rewritten through the function, which are its
   test; and a test in each service's repository that declares an index.

   Deferred (2026-10-23), the owner's to decide between three executors
   of the finding. The gate above, test infrastructure: it catches a
   declaration without its migration in CI, where a repository writes the
   call. A schema that reads its GIN indexes from the catalog at the
   composition root: the README defines a schema by what `\d` shows, and
   a missing index then degrades the condition to the plain equality,
   with no claim left to be false - the text of a query becomes the
   environment's, and a forgotten migration is seen in a log line or by a
   test of the indexes expected, which is a test again, of intent and not
   of plans. DDL generated from the schema, the declaration and the
   migration one artifact: the services' migrations to decide. Worked for
   the gate, a blind solver against a discriminator, measured on
   PostgreSQL 14.24, and kept here: a function of `pg::testing` behind
   `pg`, which gains `tokio-postgres` as the `pg` features of kms, dek and
   event-store have it; it takes a `GenericClient`, the schema, the
   specification and the index's name, derives `FROM` from the schema's
   table and alias, which the compiler qualified the columns with, runs
   `EXPLAIN (FORMAT JSON)` with the parameters under `SET LOCAL
   enable_seqscan = off` in a savepoint rolled back on every path - in a
   transaction of its own where there is none, `SAVEPOINT` outside one
   being SQLSTATE 25P01 - and requires `"Index Name": "<name>"` in the
   plan, the closing quote against a name that is a prefix of another. A
   table of one row without statistics takes the index so, and a generic
   plan takes the same indexes. Existence in `pg_indexes` is not serving:
   an index on the text without `COLLATE "C"` exists and serves nothing.

## Consequences

- The README's limit on a collection that is a JSON null is rewritten with
  decision 1, and the mirror case of `100.0` is added beside the proto3
  case: the repository's test runs under the schema it compiles with
  (ADR-0022), which catches either direction.
- The evaluator and the compiler differ on a collection that is no
  collection as they do on a member that is not there: a refusal in memory,
  no rows in SQL. A domain's `Context` never has one - the aggregate holds
  a `Vec` - so the difference is of `Record` candidates and of a document
  whose encoding is wrong, which the repository's test is for.
- Under decision 1 an encoding that writes `null` for an empty collection
  is no longer found by a failing query. It is found by the repository's
  test or not at all; the failing query found it only on the rows the plan
  reached.
- One `jsonb` parameter for the pattern, which ADR-0022 rejected because a
  `Value` would have to carry JSON, is the one form that costs the same
  under either plan, 26 ms against 29 ms folded. The rejection stands for
  the reason it gave; the measurement is recorded for the repository that
  finds the penalty on its tables, and is the step to take then.
- The compiled form of a specification serves the read model first: a
  listing's filter is a query of a projection, not of the aggregate, so the
  mapping, the layout and the index declared are the projection's
  storage's. The aggregate's own table is queried by the few rules that
  select aggregates; the domain evaluates its rules in memory.
- Left for when they are wanted, each a step of its own: a `Layout`
  derived from what writes the document, rather than written by hand - on
  the write side the persistence adapter's record of the aggregate's state,
  which is no contract and which a derive could read; on the read side the
  view model's contract, from which the projection's type is generated at
  build time already, and where the trap named above is real whenever the
  projection is written in the JSON mapping of proto3, an `int64` a
  string, and not by serde of the generated type, a number - the generator
  knows which; the same layout reading a document into a `Record` of its
  kinds, so that a projection has both readers from one declaration and the
  agreement test of `tests/pg.rs` for free; and the root's stand in the
  API, a compilation of the condition as a value, for a caller that puts it
  in a select list or a `CASE`, where the text of ADR-0022 selects other
  rows.

## Alternatives rejected

**Keep the failure and document it as the plan's.** A contract the planner
decides is none; and no condition beside the `EXISTS` can be forbidden to
be cheaper than it.

**A failure that does not depend on the plan.** SQL has no expression that
raises; a `CASE` whose other branch fails - a cast of a text to an integer -
is still an operand of the `AND` the planner orders, and is short-circuited
as the `EXISTS` was. And one row's failure fails the query, where the
evaluator refuses one candidate: the failure was the wrong answer in a
`WHERE` to begin with.

**The contract at the encoding alone**, a collection never null, held by
the repository's test. Kept, as complementary; not sufficient, since the
query's text should say what a row is whatever the plan, and a document of
another writer than the repository - a migration, a fixture - is not under
the test.

**`jsonb_build_object`, `to_jsonb`, `jsonb_build_array`** for the pattern.
Stable: never folded, 85 ms under either plan.
