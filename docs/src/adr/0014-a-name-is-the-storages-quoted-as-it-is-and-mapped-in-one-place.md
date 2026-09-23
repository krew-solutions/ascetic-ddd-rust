# ADR-0014: A name is the storage's, quoted as it is, and mapped in one place

## Status

Accepted (2026-09-20).

## Context

The compiler wrote a name into the query as it stood, and PostgreSQL reads a
word it knows as what it knows. `field("user")` compiled to `user = $1`, which
parses, compares the user of the session, and selects other rows than the
evaluator is satisfied by: requirement 16 of ADR-0011 broken in silence.
`order` did not parse. Measured on PostgreSQL 14: of its 100 reserved words,
14 are read so in silence and 86 are syntax errors. Which words these are is a
property of the server asked (`pg_get_keywords()`), and an open-source library
does not know the servers of its users.

The same question had a second half in the ports. Go's `specgen` generated,
beside the tree of a predicate, an `XSQL()` that compiled it as it was, under
the names of Go's fields. `Price` found the column `price` only because
PostgreSQL folds an unquoted word, and only a word of one part: `CreatedAt`
folds to `createdat`, not `created_at`. And in Python and Go the entry point
that takes a mapping took no schema, and the one that takes a schema no
mapping.

## Decision

1. **Every name is written between double quotes, as it is**, a quote of its
   own doubled; there is no list of words. The alphabet of names — ASCII
   letters, digits, `_` — stays, so neither guard rests on the other. A name
   is quoted where it is written, `pg/identifier.rs`: a `Schema` keeps its
   names as given, and the aliases the compiler makes are quoted like any
   name.
2. **A name in the tree the compiler is given is the storage's name.** What a
   member of the domain is called in the storage is said by the `Mapping`, and
   nowhere else; the compiler has no option for names. A naming convention, if
   one is wanted, is a `Mapping`.
3. **A query is compiled by what knows the table**: the repository, which
   applies `transform` with its mapping and then `Compiler` with its schema.
   The schema names a collection as the mapping left it. A frontend yields the
   tree and no SQL: the macro generates `<name>_ast`, `specgen` `XAST`.

Held by: `tests/pg.rs` (columns `"user"`, `"order"`, `"createdAt"` on a live
server), the tests of `pg/identifier.rs`, and
`tests/mapping.rs::a_mapping_and_a_schema_are_given_together`; in the ports
by their agreement tests, and in Go by `TestOnlyTheTreeIsGenerated`.

## Consequences

- A name of mixed case compiled without a mapping no longer finds a folded
  column: the query fails with "column does not exist", where it used to
  depend on folding.
- A mapping written as a list of members is also the list of what a
  specification may filter by, which matters for a tree that arrives as data.
  A mapping by convention maps every name; the library ships none.
- A name with a space or a letter beyond ASCII cannot be written. Lifting the
  alphabet is a decision of its own: the ports rewrite `$n` and `%` over
  finished text, and a dot in a table's name is a separator.
- The text of a query is longer by two characters for each name.

## Alternatives rejected

**Refuse reserved words.** Needs the list, and forbids a column named
`order`.

**Quote reserved words only.** Keeps the text of other queries unchanged, and
a list gone stale fails in silence on the very words that are dangerous.

**Quote the name in lower case.** Keeps today's meaning of an unquoted name,
and leaves a column created as `"createdAt"` unreachable by any means.

**An option of the compiler for how a name becomes a column's.** A second
place for what the `Mapping` already says, and a rule from name to name cannot
say `owner.name → owner_name` or `id → three columns`.

**One pass: a transformer that wraps the compiler as a visitor.** A
transformation changes the shape of the tree — one equality of composites
becomes a conjunction — so it has to return a tree; the two are composed.
