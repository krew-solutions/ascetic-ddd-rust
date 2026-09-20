# ascetic-ddd-specification

The Specification pattern: a predicate on a domain object, kept as a tree, so
that one statement of a business rule answers two questions — *does this
object in memory satisfy it?* and *which rows of the table do?*

The Rust port of `ascetic_ddd/specification` (Python) and
`asceticddd/specification` with `cmd/specgen` (Go). The design, the
requirements it answers and the alternatives rejected are in
[ADR-0011](../../docs/src/adr/0011-what-a-specification-must-do.md),
[ADR-0012](../../docs/src/adr/0012-a-specification-is-a-tree-over-its-values-read-by-functions.md) and
[ADR-0013](../../docs/src/adr/0013-a-null-is-tested-not-compared-where-null-is-a-value.md).

## Four ways to write a specification

All four build the same tree, [`Expr`].

**By hand**, with the functions of [`ast`]:

```rust
use ascetic_ddd_specification::ast::{and, any, field, greater_than, value};
use ascetic_ddd_specification::{Expr, Path, Value};

let specification: Expr<Value> = and(
    field("active"),
    any("items", greater_than(field(Path::item("price")), value(500))),
);
```

**With typed terms**, [`dsl`], which refuse at compile time what the tree
would accept and the evaluator refuse later — a number conjoined, a text
multiplied, `is_null` of a member not declared nullable:

```rust
use ascetic_ddd_specification::dsl::{Boolean, NullDatetime, Number};

let cheap = (Number::field("price") - Number::field("discount")).lt(Number::value(100));
let listed = Boolean::field("active") & NullDatetime::field("deleted_at").is_null();
let specification = (cheap & listed).into_expr();
```

**As a JSONPath template**, [`jsonpath`], parsed once and bound to
parameters for each use:

```rust
use ascetic_ddd_specification::jsonpath::{Params, Template};
use ascetic_ddd_specification::{Record, Value};

let dear: Template = "$.items[*][?@.price > %(price)f && @.active == true]".parse()?;
let store = Record::<Value>::object([(
    "items",
    Record::collection([Record::object([
        ("price", Record::value(999.0)),
        ("active", Record::value(true)),
    ])]),
)]);
assert!(dear.matches(&store, &Params::named([("price", 500.0)]))?);
# Ok::<(), Box<dyn std::error::Error>>(())
```

**As a Rust function**, with `#[specification]`. The function stays what it
is, the fastest way to check an object in memory; beside it appears
`<name>_ast`, the same predicate as a tree. What the Python source does at
run time by reading the lambda's source, and the Go source with a generator
before compilation, a procedural macro does during it:

```rust
# #[cfg(feature = "macros")]
# mod example {
use ascetic_ddd_specification::{pg, specification};

pub struct Item { pub price: i64, pub active: bool }
pub struct Store { pub active: bool, pub items: Vec<Item> }

#[specification]
pub fn has_dear_items(store: &Store, price: i64) -> bool {
    store.active && store.items.iter().any(|item| item.price > price && item.active)
}

# pub fn main() {
let store = Store { active: true, items: vec![Item { price: 999, active: true }] };
assert!(has_dear_items(&store, 500));

let query = pg::compile(&has_dear_items_ast(500)).unwrap();
assert_eq!(
    query.sql,
    concat!(
        r#""active" AND EXISTS (SELECT 1 FROM unnest("items") AS "item_1" "#,
        r#"WHERE "item_1"."price" > $1 AND "item_1"."active")"#,
    ),
);
# }
# }
# #[cfg(feature = "macros")]
# example::main();
```

The body is one expression of: members of the candidate and of the item;
literals, the other parameters of the function and constants, which become
values; `== != < <= > >=`, `&& || !`, `+ - * / % << >>`, unary `-`;
`.is_none()` and `.is_some()`, and `== None`, which is the same; `Some(x)`,
which is `x`; the comparison methods `.eq(&x)` … `.ge(&x)` a Value Object
compares by; `.iter().any(|item| …)` and `.all(…)`, nested as deep as the
collections are; `&`, `*`, `.clone()`, `.as_str()`, `.as_ref()`,
`.as_deref()`, which change how a value is held and not the value. Anything
else is a compile error at the place it stands.

## Two ways to read one

**In memory**: [`is_satisfied_by`] against anything that implements
[`Context`] — a domain object saying which of its members are values, which
objects, which collections; or a [`Record`] of plain data.

**As SQL**: [`pg::compile`], to a condition with numbered parameters; with a
[`pg::Schema`] for the collections kept in tables of their own. With the
`pg` feature, [`Value`] goes to `tokio-postgres` as a parameter as it is.

The two agree, nulls included: the logic of the evaluator is PostgreSQL's
three-valued one, and `tests/pg.rs` holds them against each other on a live
database — the same value or the same error for each constant expression,
the same rows selected for each specification.

Between the two stands [`transform`], for a domain whose members and values
are not the table's columns and scalars: a composite identity compared as
one value in the domain and column by column in the query.

## What maps to what

| Python, Go | here |
| --- | --- |
| `nodes`: `Visitable`, `Visitor`, the node classes | [`Expr`], an enum; a reader is a function that matches on it |
| `Field(Object(Object(GlobalScope(), "a"), "b"), "c")` | `field("a.b.c")`: a [`Path`], never empty, from a [`Root`] |
| `Wildcard(parent, predicate)`, `Item()` | [`Expr::Any`], `Root::Item` |
| `OPERATOR`, `ASSOCIATIVITY` | [`Prefix`], [`Infix`], [`Postfix`]; associativity is the notation's, not the tree's |
| `EvaluateVisitor`, `Context.get` | [`evaluate`], [`is_satisfied_by`]; [`Context`] with `field`, `object`, `collection` |
| Go `OperatorRegistry`, `EqualOperand`, … | [`Operand`], implemented by [`Value`] and by a domain's own value type |
| `DictContext`, `NestedDictContext`, `CollectionContext` | [`Record`] |
| `public`: `Boolean`, `NullNumber`, … | [`dsl`] |
| `jsonpath_parser` (Python had two library-backed twins of it, since removed) | [`jsonpath`]: one parser, no dependency |
| `lambda_filter`, Go `cmd/specgen` | `#[specification]` |
| `TransformVisitor`, `ITransformContext`, `CompositeExpression` | [`transform`], [`Mapping`], [`Mapped`] |
| `PostgresqlVisitor`, `compile_to_sql`, `SchemaRegistry` | [`pg::compile`], [`pg::Compiler`], [`pg::Schema`] |

## Deviations from the sources

What this port does otherwise than the sources did when it was made. Each is
a decision, and each has since been carried back to the Python and Go
sources, which now hold a regression test of each and a test of their own
against a live PostgreSQL; the list stays as the record of what was found.
Those that corrected a result are marked **fix**, and "the sources" below are
the sources as they were.

* **fix** — `!=` of two composites is `NOT (a1 = b1 AND a2 = b2)`. The
  sources have `NOT (a1 != b1 AND a2 != b2)`, by which a composite is unequal
  to itself.
* **fix** — `all(…)` means "no item fails". The sources read `all` as `any`.
  It is written `NOT any(NOT p)`, so the tree has no second quantifier.
* **fix** — SQL parentheses follow associativity: `a - (b - c)` stays so, and
  `(a = b) = c` is not written `a = b = c`, which PostgreSQL does not parse.
  The Go source compares precedences alone. The Python source writes no
  parentheses at all — its precedence key, `"%s %s" % (operator,
  associativity)`, is `"OPERATOR.AND ASSOCIATIVITY.LEFT_ASSOCIATIVE"` for its
  `str`-and-`Enum` operators and matches no row of its table — so
  `(a OR b) AND c` compiles to `a OR b AND c`.
* **fix** — `IS` compiles to `IS NOT DISTINCT FROM`; `x IS $1` is a syntax
  error in PostgreSQL. In memory it is equality in which null is a value.
* **fix** — the predicate of a relational collection is parenthesised after
  its keys; `fk AND p OR q` selects rows of other parents.
* **fix** — a collection of the candidate named inside another collection's
  predicate joins to the root row, not to the enclosing item.
* **fix** — `transform` rewrites the predicate of a collection, and tells a
  mapping whether a path starts at the candidate or at the item.
* **fix** — a template's placeholders are bound in the order they stand;
  the sources list the named ones first and bind a mixed template wrongly.
  Mixing the styles is a syntax error here, as it is in Python's `%`.
* A null is tested, not compared, where a notation has null for a value
  (ADR-0013, [`null_test`]): `@.a == null` of a template, spelled out or
  bound to a placeholder, `x == None` of a Rust function, a parameter of an
  `Option` type that is none, `eq` of a typed term with a null constant are
  `IS NULL`, and `!=` is `IS NOT NULL`. Written into the tree as they stand
  they are `a = NULL`, true of nothing once nulls follow SQL — which is what
  the tree built by hand still means.
* The template grammar is closed: what the sources skip over "if present" is
  required or refused. See [`jsonpath`] for the grammar and the list.
* A placeholder's letter is checked: `%d` takes an integer, `%f` a number,
  `%s` anything. An unbound placeholder is an error, not a value that equals
  nothing.
* The evaluator is three-valued in both languages' stead: Python's has no
  null logic, Go's has it everywhere but in collections. `AND`, `OR` and
  `any` stop as soon as they are decided.
* Arithmetic is PostgreSQL's and checked: no wrapping, no `inf`, truncating
  integer division; booleans are ordered, false first.
* A schema names a collection by its whole path, `categories.items`, not by
  its last name, so two collections of one name are two.
* A name is written into the query between double quotes, as it is, and one
  that is not of ASCII letters, digits and `_` is refused. The sources write
  any name into the query as it stands, where `user` is the session's user
  and not a column, and `order` does not parse.
* Unary minus exists. Python's `NEG = "-"` is an alias of `SUB` in its
  `Enum`; Go's prints as `-neg`; Go's generator emits a `spec.Neg` that is
  not defined. Unary plus is dropped: nothing produced it.
* `<<` and `>>` shift the bits of an integer, as in the evaluator of Python
  and in PostgreSQL. The sources' `public` package types them as
  comparisons.
* The macro generates `<name>_ast` and no `<name>_sql`. A query cannot be
  written without knowing the table, and what a member is called there is the
  repository's to say: it applies `transform` with its [`Mapping`] and
  compiles with its `Schema` (ADR-0014). Go's generated `...SQL()` compiled
  the tree under the names of Go's fields. The macro takes the function's
  other parameters as values, which neither source could: a specification
  with a parameter is the usual kind.
* Declared and never used, so not ported: `IN`, `BETWEEN`, `ASC`, `DESC`,
  `PERIOD` of Python's `OPERATOR`; the collection "slice" other than `*`.

## Limits

* A template's tree has at most 128 levels, and a template nests at most 32
  deep - groups, `!`, the filters of collections: a text that is not trusted
  must not be a stack overflow, which is an abort. A chain of `&&` or `||`
  nests to the left, so it has at most 128 operands. A tree built by hand or
  by the macro is as deep as its author made it.
* Text is ordered by code point here and by the column's collation in the
  database; equality agrees, order may not.
* A name is the column's to the letter: `createdAt` is the column created as
  `"createdAt"` and not `createdat`, which is what PostgreSQL makes of the
  word without quotes. Where the domain's names are not the storage's, a
  [`Mapping`] says what they are. A name with a space or a letter beyond
  ASCII cannot be written.
* A path from the candidate is written into the query unqualified. Inside the
  subquery of a relational collection an unqualified name can be captured by
  a column of the child table: qualify columns in the [`Mapping`].
* An embedded collection is read with `unnest`, so it is an array of a
  composite type. A `jsonb` array is not supported, as it is not in the
  sources.
* `numeric` parameters are not written by the `pg` feature.
* From the predicate of an inner collection the item of an outer one cannot
  be named: the tree has one `@`, the nearest. The macro says so.
* The null test is of constants, not of data: `@.a == @.b` with both members
  null is null, where RFC 9535 has true. The equality in which null is a
  value is `IS`, which a template cannot spell.
* A function and its tree agree on candidates without nulls. With them,
  Rust's `!(x == Some(5))` is true of a `None` and SQL's `NOT x = 5` is not.

## Tests

```text
cargo test -p ascetic-ddd-specification
ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
    cargo test -p ascetic-ddd-specification --features pg --test pg
```
