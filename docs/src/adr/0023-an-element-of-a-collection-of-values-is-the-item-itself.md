# ADR-0023: An element of a collection of values is the item itself

## Status

Accepted (2026-10-21). Discharges the step ADR-0021 named among its
consequences.

Amended 2026-10-21 by ADR-0024: the consequence on a GIN index declared at a
path of a document is discharged - `Schema::gin_at`.

Amended 2026-10-21 by ADR-0025: the consequence on an element the domain
holds as a composite and the storage keeps in part in the candidate's row
is discharged - `Mapping::element`; decision 6 is amended: the mapping is
asked about an element, and says the element itself unless it says
otherwise.

## Context

The commonest array of the target services holds identifiers: the device
groups a policy targets, the subgroups of a device group, the members of a
cohort. A specification could not say "some group a policy targets is G".
A `Path` names a member of an item and never the item itself;
`Context::collection` gave contexts, and an element that is a string has
no members to give; a template required a name after `@`; the macro read
the members of the closure's item.

What the storages do, measured on PostgreSQL 14.24:

- `unnest` of an array of a scalar type names the element by its alias,
  `"tag_1"`, of the array's type; a null element is a null row.
- `jsonb_array_elements_text` gives an element of a document's array as
  text, a JSON null as the null of SQL; an object or an array in it as its
  text, which a layout rules out.
- Containment holds for arrays of scalars, `{"g": ["a", "b"]} @> {"g":
  ["a"]}`, and its element is built of immutable functions,
  `jsonb_object(ARRAY['v'], ARRAY[$1::text]) -> 'v'`; `to_jsonb` and
  `jsonb_build_array` are stable.
- `?` is no equality of an element: it is true of the key of an object, and
  false of a number, `'[1, 2]'::jsonb ? '1'`.

A domain's element is often a type of its own - a tenant-scoped identifier -
and a service cannot implement this crate's `Context` for it, nor for
`String`: a foreign trait for a foreign type is E0117. Measured on a
prototype built in a crate of its own.

Two designs, made independently from the task, arrived at the decision
below; the second also prototyped it across every reader and frontend.

## Decision

1. **The item itself is a node of the tree**, `Expr::Item(up)`: the item
   `up` collections out, JSONPath's `@` alone. Built by `ast::item()` and
   `ast::outer_item(up)`. A member of the item stays a `Path`,
   `Path::item("price")`.

2. **`Context::collection` gives `Element`s**: `Element::Object(&dyn
   Context<V>)`, whose members a path names, or `Element::Value(V)`, by
   value, as `Context::field` gives a value. One question, one method.

3. **The evaluator** reads `Expr::Item` of a value, and refuses it of an
   object, `ContextError::NotAValue("items[*]")`; a member of a value is
   `NotAnObject("tags[*]")`; outside a collection, `NoCurrentItem`.

4. **A template** reads `@` alone in the predicate of a collection as the
   item itself, `$.tags[*][?@ == %s]`, and bound to a null as the null test.
   `@` alone of the candidate, `$` alone, and `@[*]` are refused.

5. **The macro** reads the closure's item named alone as the item itself,
   `|tag| *tag == x`; `collection.contains(&x)` as `any` of the equality,
   with `x` seen from inside the collection, so an enclosing item is one
   further out; and `.is_some_and` of an element.

6. **`transform`** does not ask the mapping about an element: the element is
   what the collection holds, where the mapping put the collection; a
   constant beside it is the mapping's `value`, and a composite one there is
   `TransformError::NotComposite`.

7. **The compiler** writes the element of an array of a scalar type as its
   `unnest` alias; of a document's array of values, declared
   `Layout::values(name, kind)`, as the text of `jsonb_array_elements_text`
   read as its kind, `("device_group_1" COLLATE "C")`, `"rank_1"::bigint`.
   The item itself of a row of a table, or of an object of a document, is
   `CompileError::NotAValue`; a member of a value of a document is
   `UndeclaredMember`. Where the document has a GIN index (ADR-0022), the
   equalities of an element with constants of a text, an integer or a
   boolean are what its array contains, beside the `EXISTS`.

Held by: the evaluator in `tests/specification.rs`, templates in
`tests/jsonpath.rs`, `transform` in `tests/mapping.rs`, the macro in
`tests/macros.rs` (the trees, and each function and its tree agreeing on
every candidate), the text of each storage and each refusal in
`tests/pg_compile.rs`; on a live server in `tests/pg.rs`, the stores' tags -
an array of text, and an array of strings in their documents - selected by
thirteen specifications, nulls among the elements and the constants, in each
of the five storages against the evaluator, and the GIN index used with the
pattern of an element folded. The equalities of an element under `OR` were
checked to fail when read as under `AND`; a pattern that asks for more than
the predicate does loses rows, which the test sees.

## Consequences

- Breaking: an exhaustive `match` on `Expr` outside the crate stops
  compiling; every `Context` implements `collection` anew, an object as
  `Element::Object(item)`. `CompileError` has a new variant.
- An element the domain holds as a composite and the storage as a part of
  it - a tenant-scoped identifier kept as its local key, its tenant in the
  candidate's row - is given by the domain's `Context` as the part, and is
  queried within its tenant. A mapping that maps a composite constant to a
  part compares other values than the evaluator, in silence. Saying where
  each part is kept - a part of the element in the candidate's row - is a
  change of `Mapping` of its own, prototyped by the second design and left
  for when such a specification is written.
- The services index an array by an expression, `GIN ((document ->
  'subgroups'))`, which containment of the whole document does not use. A
  GIN index declared at a path of a document is a step of its own.
- `.contains` of a text is a substring to Rust and `any` to the macro, which
  has no types; the evaluator and the server refuse a text for a collection,
  at run time.
- The typed terms of `dsl` have no quantifier, of objects or of values.
- An array of arrays is not read.

## Alternatives rejected

**A `Path` without names for the item itself.** Every reader that takes a
path apart would do something with an empty one, silently; the mapping
would be asked about an element by the path of its collection. A node of
its own makes each reader say what it does with it, and the compiler showed
which.

**A second method beside `collection`, `values(name)`, with the evaluator
trying one and then the other.** Two methods for one question, and a
decision taken on an error.

**A `Context` for each element.** Not implementable where the element's type
and `Context` are both foreign to the service.

**`?`** for an element of a text array of a document: true of a key, false
of a number, and not served by `jsonb_path_ops`.
