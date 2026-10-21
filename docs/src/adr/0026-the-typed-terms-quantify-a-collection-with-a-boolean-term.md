# ADR-0026: The typed terms quantify a collection with a boolean term

## Status

Accepted (2026-10-21). Discharges the step ADR-0023 named among its
consequences.

## Context

The typed terms of `dsl` (ADR-0011, requirement 7) refuse at compile time a
tree that means nothing: a number conjoined, a text multiplied, the null
test of a member not declared nullable. They had no quantifier. A
collection was quantified by leaving them for the tree and coming back,
`Boolean::from_expr(ast::any("items", term.into_expr()))`, and at that
border nothing was checked: a number for the predicate compiled, and failed
when evaluated, "a boolean was expected"; `from_expr` takes any tree for the
sort it is told. The item itself of a collection of values (ADR-0023) had no
term at all. The Python and Go sources the terms were ported from have no
quantifier either.

## Decision

1. **`dsl::any(source, predicate)` and `dsl::all(source, predicate)`**, the
   predicate a boolean term, nullable or not, the result a `Boolean`: never
   null, as `any` and `all` of the tree are.

2. **`Term::item()` and `Term::outer_item(up)`**: the item itself, declared
   of the term's sort as a field is - `Text::item()` of the tags,
   `NullText::item()` where a tag may be null. A member of an item stays a
   field of a path from it, `Number::field(Path::item("price"))`.

Held by: `tests/dsl.rs`, the trees built against the tree's own and
evaluated and compiled; the doctests of `dsl`, a number for a predicate a
compile error.

## Consequences

- What the terms check is the sorts of a specification among themselves,
  not against the candidate: `Text::item()` of a collection of numbers is
  the author's declaration, as `Number::field("name")` is.
- An item of an enclosing collection is named by how far out it is,
  `Text::outer_item(1)`, as the tree names it.

## Alternatives rejected

**A collection as a term of its own, quantified with a closure that is
given the item**, `tags.any(|tag| tag.eq(Text::value("sale")))`: the sort
of the element said once, at the collection, and the item named as Rust
names it. The item given to a closure is the item of the nearest
collection, `Item(0)`; a term of an outer item, taken into the closure of
an inner collection, would still be `Item(0)` there and name the inner
item, in silence - unless the tree carried a mark for each item to be
resolved when the quantifier is built, which it does not. The macro does
that resolution with the names Rust gives (ADR-0012), and is the way to
write such a predicate with Rust's own types.
