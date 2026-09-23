# ADR-0011: What a specification must do

## Status

Accepted (2026-09-18).

## Context

`crates/specification` ports `ascetic_ddd/specification` of ascetic-ddd-python
and its mirror in ascetic-ddd-go, `asceticddd/specification` with
`cmd/specgen`. A word-for-word port was not asked for, so what is carried
over had to be said first: the capabilities, not the classes.

A capability counts as a requirement if a user of the sources can watch it
work. Three things follow. What is declared and never acted on is not one
(`IN`, `BETWEEN`, `ASC`, `DESC`, `PERIOD` in Python's `OPERATOR`; a
collection "slice" other than `*`; the three JSONPath parsers of Python are
one capability, not three). What works in one source only is one (the
three-valued logic of Go's `OperatorRegistry`). And what the sources do
wrongly is a requirement stated rightly: each such case below was reproduced
by running the source, and is listed with its evidence in the crate's README,
"Deviations".

## Decision

The crate must do the following. Each line names the test that holds it.

**The tree** (`tests/specification.rs`)

1. A specification is a tree of: a constant; the value of a member reached
   from the candidate, or from the item under test, through nested objects;
   a prefix operator (`NOT`, unary `-`), an infix one (`= != > < >= <=`, `IS`,
   `AND`, `OR`, `+ - * / %`, `<< >>`), a postfix one (`IS NULL`,
   `IS NOT NULL`); "some item of a collection satisfies a predicate", nested
   to any depth.
2. It can be built by hand, several operands of `AND`/`OR` nesting to the
   left, and compared with another tree for equality.

**Evaluation** (`tests/specification.rs`, `tests/pg.rs`)

3. A tree is evaluated against a candidate that gives, by name, a value, a
   nested object, or the items of a collection; a member that is not there
   is an error. A candidate made of plain data is provided.
4. Nulls follow SQL: null operands make null results; `AND`, `OR`, `NOT` are
   three-valued; `IS`, `IS NULL`, `IS NOT NULL` and the collection predicate
   are never null; a candidate satisfies what evaluates to true.
5. Values are booleans, integers, floats, text, points and spans of time —
   a point less a point is a span, a point and a span make a point — and any
   type of the domain's own that says how it compares and computes.
6. An operator applied to what it is not defined for, a division by zero and
   an overflow are errors that name the operator and the operands.

**Typed terms** (`tests/dsl.rs`, the doctests of `dsl`)

7. Terms declared boolean, number, text or datetime, nullable or not, offer
   only the operators of their sort, and build the same tree.

**Templates** (`tests/jsonpath.rs`)

8. A specification is parsed from a JSONPath filter: `$[?p]` on the
   candidate, `$.a.b[*][?p]` on a collection, the same nested inside `p`;
   `== != < <= > >=`, `&&` over `||`, `!`, parentheses; numbers, strings,
   `true`, `false`, `null`.
9. Placeholders `%s %d %f` and `%(name)s …` stand for values; a template is
   parsed once and bound to positional or named parameters for each match,
   from any thread.
10. A syntax error gives its position, what was expected, and the template
    with a caret. Text outside the grammar is refused, not read as something
    else; parameters that do not fit are refused, not left unbound.

**Host-language predicates** (`tests/macros.rs`, the unit tests of
`specification-macros`)

11. A predicate written as an ordinary function yields the tree of the same
    predicate without being written twice, the function staying the native
    check: comparisons, logic, arithmetic, nested members, `any`/`all` over
    collections nested to any depth, null tests, the comparison methods of
    Value Objects.
12. What cannot be a tree is reported where it stands, not replaced by a
    null.

**From the domain's terms to the storage's** (`tests/mapping.rs`)

13. A mapping says what each member and each value becomes; one of either
    may become several, a composite, nested if need be. Equality of two
    composites becomes the conjunction of the equalities of their parts,
    inequality its negation; shapes that differ and other operators on
    composites are errors.

**SQL** (`tests/pg_compile.rs`, `tests/pg.rs`)

14. A tree compiles to a PostgreSQL condition with numbered parameters,
    numbered from a given offset if asked, parenthesised so that the text
    means what the tree means.
15. A collection predicate is `EXISTS` over `unnest` of an embedded
    collection, or over a child table joined by a simple or composite
    foreign key, as a schema says; aliases are distinct and readable; nested
    collections join to the enclosing item.
16. A row is selected by the compiled condition exactly when its object
    satisfies the tree in memory.

## Consequences

- Requirement 16 is what makes the rest worth having, and the sources do not
  test it; here `tests/pg.rs` does, on a live database, for constants and for
  rows with nulls, in both storages of a collection.
- The defects found are to be fixed in the Python and Go sources; the
  README's list is the work order.
- Requirement 11 reads the sources' two mechanisms — run-time reading of a
  lambda's source, a generator run before compilation — as one capability,
  and leaves the mechanism to ADR-0012.

## Alternatives rejected

**Port the classes and let the capabilities follow.** The visitor, the
operator registry keyed by dynamic type, the placeholder marker hidden in a
value, the composite node whose `accept` raises — each is how Python or Go
gets a capability, and a port of them would carry the defects found with
them, several of which live in exactly those mechanisms.

**Take the OCaml port's feature set** (`ascetic-ddd-ml`, `lib/specification`):
function calls, `IN`, many-to-many relations, a CEL-like text syntax. It is
another design with other requirements; the sources named for this port are
the Python and Go ones.
