# ADR-0015: A string constant is read as the kind of the member beside it

## Status

Accepted (2026-09-30): the tests named below hold in the three ports.

## Context

A template has the literals of RFC 9535 — a string, a number, `true`,
`false`, `null` — and no others: a point in time, a date or a UUID in a
template is a string, `@.created_at > '2026-09-01'`. The two readers then
part. PostgreSQL reads an untyped parameter by the type of the column beside
it, so the query selects the rows; the evaluator compares a string with a
point in time and refuses (Python `TypeError`, Rust `OperandError`). Measured
on PostgreSQL 14: `timestamptz` and `date` take `2026-09-01`,
`2026-09-01T12:00:00`, with a space for the `T`, with fractions, with `Z` or
`+03:00`, and beyond these `20260901`, `Sep 1 2026`, `yesterday`; `uuid`
takes the canonical form in either case, without hyphens, in braces.

A date bound as a named parameter agrees in both readers, but in a REST API
that means a typed parameter beside the filter for every date, declared in
the contract — a burden on every client for a value it can write in the
filter. Numbers and booleans have literals of their own, in the template and
in JSON; a string beside a number is the author's choice.

## Decision

1. **A string compared with a value of a kind that has no literal of its own
   — a point in time, a date, a UUID — is read as that kind by the
   evaluator**, on either side of the operator, before the comparison, as the
   server reads an untyped parameter by the column. A string that does not
   read is an error, as the server's "invalid input syntax". A string beside
   a number or a boolean stays a string, and does not compare.
2. **What is read is a subset of what the server reads**, so that nothing the
   evaluator accepts fails on the server: for a point in time and a date,
   ISO 8601 — `YYYY-MM-DD`, or that followed by `T` or a space and
   `HH:MM[:SS[.ffffff]]`, then nothing, `Z`, or `±HH:MM`; a date takes the
   date of a full timestamp, as the server does. A string without an offset
   is read as UTC: the server reads it in the session's time zone, so the
   two agree where the session's zone is UTC, and the agreement test sets
   it. For a UUID, the canonical `8-4-4-4-12` in either case.
3. **A UUID is a kind of value in every port.** Python's `uuid.UUID`
   already is; Go's is `github.com/google/uuid`'s, which the operators
   learn to compare; Rust's `Value` gains `Uuid(uuid::Uuid)`, a pure crate
   that `postgres-types` writes as `uuid`. A UUID constant with nothing
   beside it has its type said in the text, `$1::uuid`, as a timestamp has.
   Rust reads a point in time with `chrono` (`default-features = false`: no
   clock in the domain), the crate the target project is on; `Timestamp`
   stays the domain's own type.
4. The compiler is not touched: the text of the query is as it was, and the
   server keeps reading the parameter by the column.

Held by, in each port: a text test of what reads and what does not, and a
live test over `timestamptz`, `date` and `uuid` columns — a string of each
form of the subset, one beyond it, and one that does not read — against the
evaluator, with the session's time zone set.

## Consequences

- A filter from a REST client carries its dates and identifiers inline, and
  the same text means the same rows in memory and on the server.
- Outside the subset the evaluator is loud where the server is not:
  `'yesterday'` is an error in memory and a row on the server. Never the
  other way.
- A point in time without an offset means UTC to the evaluator; a service
  whose session zone is not UTC has the readers disagree by that offset. The
  README says so.
- `Value::Uuid` is a new variant: an exhaustive `match` on `Value` outside
  the crate stops compiling, and `uuid` becomes a dependency of the crate.
- A member holding a string compared with a member of another kind is still
  an error: the reading is of constants, which are the author's, not of the
  candidate's data - and the server refuses two columns of those types.
- Rust's driver asks a value to write itself in the type the server
  inferred, where Python's and Go's hand a text to the server: so in Rust
  the text is read on its way to the server as well, in the same subset, and
  what the server would read beyond it is an error on both sides there.
- A date is a kind of its own where a midnight would agree with the server
  on a string that is a date alone and part from it on `< '2026-09-02T12:00'`
  in silence: Python's `datetime.date`, and Rust's `Value::Date`, days since
  the Unix epoch as `Timestamp` is microseconds, so that a date of any
  calendar library converts into it. Go has no date type of its own and
  does not choose one for the domain: the reader is registered by the type
  it reads, `operators.RegisterReader`, as an operator is by the types it
  applies to, and the default registry reads `time.Time` and `uuid.UUID`
  that way; the domain registers a reader, and the order, of the type it
  holds a date in. A date held as a `time.Time` midnight agrees on a string
  that is a date alone. Only Python has a time without a zone; Rust and Go
  apply an offset where the server would drop it.

## Alternatives rejected

**Dates as parameters only.** Agrees today, and costs every client a typed
parameter per date beside the filter.

**A typed literal in the template, as OData writes `2026-09-30T12:00:00Z`
bare.** A syntax RFC 9535 does not have; to be decided with an OData
frontend, if one comes, not for this.

**Read a string beside a number or a boolean as well.** They have literals;
a string there is meant, and the server's coercion of `'100'` is an accident
of untyped parameters, not a meaning to copy.

**Copy the server's input syntax.** Unbounded and a property of the server's
version; `'yesterday'` is not a value.

**Require an offset on a point in time.** Agrees in every session zone, and
refuses the commonest filter, `> '2026-09-01'`.

**A UUID as sixteen bytes of the crate's own.** No dependency, and a second
UUID type every user converts to and from.
