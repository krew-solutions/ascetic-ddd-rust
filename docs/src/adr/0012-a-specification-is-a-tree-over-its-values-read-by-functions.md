# ADR-0012: A specification is a tree over its values, read by functions

## Status

Accepted (2026-09-18).

## Context

ADR-0011 says what `crates/specification` must do; this says how, in the
functional style asked of the port. Two questions decide the public API, and
each was answered from two premises.

*What is a value?* In the sources it is `any`: a constant may be a Value
Object, which the transformer later takes apart, and Go serves the open set
with a registry keyed by dynamic type. Premise A: values are a closed set of
scalars, as in SQL, and a composite is one more scalar. Premise B: the tree
does not know its values, and each reader asks of them what it needs.

*What is a specification?* Premise C: data, a tree (the initial encoding).
Premise D: a program generic in its interpretation — a trait with a method
per operator, a reader per implementation (tagless final).

## Decision

1. **The tree is an enum, `Expr<V>`, and a reader is a function that matches
   on it.** A visitor is how a language without sum types spells `match`;
   new readers still need no change to the tree. A path is a root and names,
   never empty, instead of a linked chain of `Object` nodes. Operators are
   grouped in the type by what they need of their operands, so no reader has
   an arm for "cannot happen". Precedence and associativity are not in the
   tree: they belong to a notation, and the parser and the SQL compiler each
   have their own.

2. **The tree is generic in its values (premise B), and the stage of a
   specification is in its type.** A parsed template is an `Expr<Slot>`, a
   bound one an `Expr<Value>`: binding is `try_map_values`, and a tree with
   placeholders cannot reach the evaluator. A domain specification is an
   `Expr<D>`, a transformed one an `Expr<S>`: one that was not transformed
   does not pass for one that was.

3. **What evaluation needs of a value is a trait, `Operand`**, implemented
   by the scalar `Value` and by a domain's own value type: the static form
   of Go's registry and operand interfaces. It holds only what depends on
   the value type; null propagation and the three-valued connectives are the
   evaluator's, written once.

4. **One semantics, PostgreSQL's, for both readers**, held by a differential
   test on a live database (`tests/pg.rs`). Where Python, Go and PostgreSQL
   disagree — integer division, overflow, shifts, `NaN`, the order of
   booleans — the evaluator does what the database does.

5. **A composite is not a node.** A mapping returns `Mapped`: one expression
   or several. `transform` returns an `Expr`, which has no such case, so a
   composite left outside `=` and `!=` is an error there and cannot reach the
   compiler.

6. **Nothing is mutated across calls.** The parser's rules take the input
   left and return what they read with the input left after; what `@` means
   is an argument. The compiler takes the count of parameters and aliases so
   far and returns the count after, where the sources share counters.

7. **The host-language frontend is a procedural macro**, `#[specification]`,
   in `crates/specification-macros` because Rust requires such a crate to be
   of its own kind. Rust cannot read a closure's source at run time, as
   Python does; a generator run before compilation, as in Go, is what a
   procedural macro is, with the compiler's spans for its errors.

## Consequences

- Three objections were raised against premise B and checked. *Generics
  will spread to every user*: the typed DSL, the templates and the macro are
  fixed to `Value`; only `transform` and a domain with Value Objects see
  another `V`. What remains is that a tree without a constant, `field("a")`
  alone, says nothing of its `V`, which then has to be named. *Agreement of the two readers cannot be tested*: it is, see
  4. *Stopping `AND`, `OR` and `any` early departs from the sources, which
  evaluate everything*: it does, where the part not evaluated would have
  failed; it is what the same operators do in all three host languages, and
  what lets a function and its tree agree on `a != 0 && 10 / a > 1`.
- The sources' mechanisms that carried defects are gone with the defects:
  the placeholder marker (unbound and misbound placeholders), the composite
  node (`accept` that raises), precedence keyed by formatted strings (no
  parentheses at all in Python), node-carried associativity (a part of the
  table's key, never of the decision where a parenthesis goes).
- The tree is recursive and so are its readers. A template is refused beyond
  128 levels; a tree built by hand is as deep as its author made it.
- `Root::Item` is the nearest item only, as JSONPath's `@` and the sources'
  `Item()`. Naming an outer item would take an index or a name on `Item` and
  on `Any`; no source can express it, and the macro refuses it by name.

## Alternatives rejected

**Premise A, a closed `Value` with a tuple variant.** Simpler signatures,
and enough for the composite identity. But an unbound placeholder becomes a
node the evaluator must refuse at run time, and a Value Object cannot be a
constant of a domain specification — the case the sources' transformer
exists for.

**Premise D, tagless final.** `transform` rewrites a tree, tests compare
trees, a template must be kept between matches: each needs the specification
as data, and a final encoding would reify it for them anyway.

**`Box<dyn Any>` values and Go's registry.** Open the same way as `V`, with
the failures moved to run time.

**A parser library, or the sources' regular expressions.** Twelve rules and
twenty-three kinds of token; the tree, its readers and the parser depend on
nothing, and a library would be their first dependency.

**Keep `all` as a second node.** `NOT any(NOT p)` is what it means in both
readers, nulls included; a second node would be a second case in each.
