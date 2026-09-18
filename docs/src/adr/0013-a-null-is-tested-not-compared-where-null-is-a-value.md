# ADR-0013: A null is tested, not compared, where null is a value

## Status

Accepted (2026-09-19).

## Context

ADR-0012 gave the evaluator PostgreSQL's semantics, so `a = NULL` is null in
both readers and true of nothing. That is right for the tree, and it took a
capability away from two of the four ways to write one.

A JSONPath template finds a null by `@.deleted_at == null`; RFC 9535 has null
for a value like any other, and the grammar has no `IS NULL` to write
instead. A Rust predicate finds it by `u.deleted_at == None`, and a parameter
of an `Option` type is none or not only when the tree is asked for. Written
into the tree word for word, each finds nothing, in memory and in the
database alike, and says nothing of it.

The Python port met this first: carrying the null logic back broke two of its
tests, `@.value == %s` bound to `None`, which had matched a `None` by
Python's own equality.

## Decision

1. **The frontends of notations in which null is a value build the null
   test.** `left = right` with the null constant on either side is
   `IS NULL` of the other, and `!=` is `IS NOT NULL`: `null_test::equal`,
   `null_test::not_equal`. SQLAlchemy reads `column == None` the same way.

2. **Each frontend applies it where its constants become known.** A
   template when it is bound, `null_test::throughout` over the bound tree —
   a literal `null` and a placeholder bound to null are the same value by
   then, and `Template::expr` stays what was written. `#[specification]` in
   the code it generates, which runs with the parameters; there `None` is the
   null constant and `Some(x)` is `x`. The typed DSL in `eq` and `ne`.

3. **The tree, the evaluator and the compiler are untouched.**
   `ast::equal(a, null)` built by hand is `a = NULL` and is null. The rule
   is a frontend's reading of its notation, not a meaning of the tree.

## Consequences

- The rule is of constants, not of data. `@.a == @.b` with both members null
  is null, where RFC 9535 has true: the one place a template departs from
  it. The equality in which null is a value is `IS`; a template has no
  spelling for it.
- An order with null, `@.a < null`, stays null, which RFC 9535 has as false:
  the same at the top of a filter, and different under `!`.
- The text of a query now depends on whether a parameter is null:
  `a = $1` or `a IS NULL`. A statement cache keyed by the text holds both.
- A function and its tree agree on `s.closed_at == at` for every `at`, none
  included (`tests/macros.rs`); `tests/pg.rs` holds bound templates with
  nulls against the database in both storages of a collection.
- `== None` was a compile error of the macro that pointed at `is_none()`;
  it is accepted now, as the Python lambda parser accepts `is None`.

## Alternatives rejected

**Keep SQL's meaning in the frontends too.** Consistent, and it leaves a
template unable to ask whether a member is null at all.

**Read every `==` of a template as `IS`**, null-safe equality, which is RFC
9535 exactly for equality. Every equality of every template would compile to
`IS NOT DISTINCT FROM`, which PostgreSQL does not serve from an index: on
14.24, with a btree on `a`, `EXPLAIN` has an index scan for `a = 5` and for
`a IS NULL`, and a sequential scan for `a IS NOT DISTINCT FROM 5`. The rule
chosen compiles to the first two. And `<` and `!` would keep two meanings.

**Apply the rule in the tree**, so that `ast::equal(a, null)` is the null
test. The tree would stop being what the compiler writes, and the agreement
of the two readers would be of something other than SQL.
