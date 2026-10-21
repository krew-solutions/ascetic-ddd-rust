# ADR-0025: An element is what the mapping says it is, from the collection's predicate

## Status

Accepted (2026-10-21). Discharges the step ADR-0023 named among its
consequences; amends its decision 6.

## Context

A policy targets device groups, and a group's identity is a pair, its
tenant and its key: an identity is the pair as one value. The policy's
document keeps the keys in an array and the tenant once, in the row - every
group a policy targets is its tenant's. So an element of the collection is a
composite in the domain and two places in the storage: one of them the
element of the array, the other a column of the candidate's row.

Under ADR-0023 the mapping was not asked about an element. A mapping had
two ways with "the policy targets G", and both were wrong. It could map `G`
to its two parts, a composite that had no parts to be compared with beside
one element: `TransformError::NotComposite`, loud. Or it could map `G` to
its key alone, which compiled and selected, for a group of tenant 3, the
policies of tenant 1 that target a group of the same key - where the
evaluator selected none. The prototype of the first design below measured
it: `{p1, p4}` against `{}`. An identity of another tenant arriving in a request finds
groups by their key: the very confusion an identity of two parts is there to
prevent.

The two designs, made from different premises, differed in where the
answer is said. The first had the mapping answer as it answers `field`, by
paths from the candidate, with a new case of `Mapped` to mark a part kept in
the candidate's row, and a rule that a path equal to the collection's place
is the element. The second had the mapping answer from where the
collection's predicate stands, as the tree itself says it there: the
element is `item()`, a column of the row is a path from the candidate.

## Decision

1. **`Mapping::element(&self, collection: &Path) -> Result<Mapped<S>, _>`**,
   asked with the collection's whole path from the candidate, as `field` is.
   By default the element itself, `Mapped::Scalar(item())`: a mapping that
   says nothing is what it was.

2. **The answer is said from the collection's predicate**, in the tree's own
   terms there: `Expr::Item(0)` is the element, `Item(n)` and a path from it
   the item `n` collections further out, a path from the candidate the
   candidate's. A tenant-scoped group kept as its key in the array and its
   tenant in the row is `Composite([Scalar(field("tenant_id")),
   Scalar(item())])`. No new case of `Mapped`, and no rule about paths.

3. **`transform` asks it for `Expr::Item(up)`** and puts the answer where
   the element was: every item named in it `up` collections further out,
   but those of a collection inside it; a path from the candidate where it
   is. An equality with a constant mapped to the same parts is the
   conjunction of the equalities of the parts (ADR-0011, requirement 13):
   `"t"."tenant_id" = $1 AND "member_1"::bigint = $2` in the `EXISTS`.

Held by: `tests/mapping.rs`, an element of the tenant's members and of the
members of its teams compared part by part, the element two collections out
put where it was, and the null test of a composite refused, as of a member;
on a live server, `tests/pg.rs`,
`an_element_kept_in_part_in_the_candidates_row_is_compared_part_by_part`:
policies of two tenants with groups of the same key, a group of a third
tenant, and a negation, against the evaluator. A mapping that maps the group
to its key alone was checked to fail it: `[1, 2]` selected where the
evaluator selects `[1]`.

## Consequences

- `Mapping` has a method more, with a default: no implementation stops
  compiling, and none changes what it compiles to.
- Two conventions in one trait: `field` answers by paths from the candidate,
  which `transform` places; `element` answers from the collection's
  predicate, which `transform` only moves out. The second is how the tree is
  written there, which is where the author of the answer stands.
- A mapping that maps an identity to its key alone still compiles and still
  selects the groups of another tenant. The guard against it is the mapping
  saying where the tenant is; the README says so.
- A composite element has no one value to be null: `IS NULL` of it is
  `UnexpectedComposite`, as of a composite member.
- The domain's `Context` gives the group as the domain holds it, the pair;
  it no longer has to give the storage's part.

## Alternatives rejected

**The answer by paths from the candidate, with `Mapped::Candidate` to mark a
part kept in the row and a rule that a path equal to the collection's place
is the element** (the first design, prototyped). Two concepts to say what
the tree already says: a new case every reader of `Mapped` must match, and
a path that means the element only by being equal to another.

**The element given by the domain's `Context` as the storage keeps it**, the
key, and the tenant left to the repository's filter (ADR-0023 as it was).
Exact within a tenant, and the knowledge of the storage in the domain; an
identity of another tenant in a specification finds groups by their key.
