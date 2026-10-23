# ADR-0028: A scope that absorbed a failed statement is aborted, not committed

## Status

Proposed (2026-10-23), implemented with it. Decided by a blind solver and a
discriminator working from the problem alone, and a synthesis of the two
against the code, the driver's source and PostgreSQL 14.24. Every claim
about the server below is made by `verify/pg/silent-rollback/exp.sh` (EXP 1
to 5), and the cost by `verify/pg/silent-rollback/bench.sh`.

Amends the close of a scope as the README described it ("a failing `COMMIT`
becomes `SessionError::Commit`; a failing `ROLLBACK` does not replace the
error that caused it") and the shape of `SessionError`.

## Context

**The incident.** An application stored a role assignment inside a larger
transaction — a user's sign-in, several aggregates. A unique index refused
the row: the user held the role already. The store mapped the refusal to
"already assigned", the reaction treated that as expected and returned
`Ok`, and `atomic` closed the scope with `COMMIT`. The caller was told the
sign-in was durable. None of it was.

**What the server does.** PostgreSQL aborts a transaction on its first
failed statement, whatever the failure: every later statement fails with
`25P02` (`in_failed_sql_transaction`) until the block ends (EXP 1, 2). A
`COMMIT` sent to an aborted block is answered with the command tag
`ROLLBACK` and no error (EXP 1): everything the transaction did is gone,
and the only trace on the wire is a tag. `INSERT … ON CONFLICT (id) DO
NOTHING` does not cover a second unique index (EXP 1): the refusal is an
error, so the abort follows.

**What the driver lets through.** tokio-postgres 0.7.18 keeps both traces
to itself: `batch_execute` discards the `CommandComplete` tag and the
`ReadyForQuery` status byte (`src/simple_query.rs`, the loop that waits for
`ReadyForQuery(_)`), and `simple_query` reduces the tag to a row count, `0`
for `COMMIT` and `ROLLBACK` alike. No public API exposes the transaction
status; the driver's own `Transaction::commit` has the same blind spot.

**What the session did.** `atomic` sent `COMMIT` when the closure returned
`Ok` and reported success unless the statement errored — which it does not
in that state. Three further holes sat beside it:

- a `RELEASE SAVEPOINT` refused in an aborted block (EXP 3) became
  `SessionError::Commit`, but the savepoint was not rolled back, so the
  enclosing transaction stayed aborted — against the contract that a
  failing nested scope leaves the surrounding transaction alive;
- a failed `ROLLBACK` was ignored and the connection went back to the pool;
  deadpool's `Fast` recycling checks only that the socket is open, and a
  `BEGIN` inside an open block is a mere warning (EXP 4), so the next user
  would have joined the previous user's transaction unseen;
- the observer heard `ScopeEnded { Succeeded }` for a scope whose `COMMIT`
  failed: the outcome was the closure's, not the close's.

**What is legitimate.** Some refusals are expected and must be absorbable
while the transaction goes on: a role already held, a uniqueness race whose
winner will be found. The server allows exactly one way: a savepoint around
the statement, rolled back to when the statement is refused (EXP 3).

**Why the classification moves.** Consumers decide whether to retry by
`SessionError` — an inbox retries a failure of the moment and parks a
defect. The verdict lived in `pg::transient_session`, which downcasts to the
driver's error; an application layer that must not name a driver could not
ask it, and stringified the error instead, calling every session failure
transient.

## Decision

1. **The server's verdict closes every scope.** A scope whose closure
   succeeded is closed with one message, `SELECT 1; COMMIT`: in a healthy
   transaction both run and it commits; in an aborted one the server
   refuses the probe with `25P02` and never reaches the `COMMIT`, leaving
   the block open (EXP 2). A nested scope is closed with `RELEASE
   SAVEPOINT`, which the server refuses the same way (EXP 3). An empty
   query would not do: it succeeds in an aborted block (EXP 4).

2. **A refused close leaves nothing open behind it.** Whatever refused it,
   the scope is rolled back — `ROLLBACK`, or `ROLLBACK TO SAVEPOINT` so that
   the enclosing transaction goes on. After a `COMMIT` the server refused
   for a reason of its own (a deferred constraint, a serialization failure)
   the block is already ended, and the `ROLLBACK` is a warning (EXP 5).

3. **A refusal of `25P02` is `SessionError::Aborted { cause }`**: the scope
   reported success over a failure it absorbed. It is never transient:
   whatever the absorbed failure was, retrying runs the same code, which
   absorbs it again — and an absorbed failure is a defect whether or not a
   retry happens to pass. Any other refusal stays `SessionError::Commit`.

4. **The connection names the cause; it never decides.** Every statement
   goes through `PgConnection`, which remembers the first failure the
   server reported since the transaction began or a scope last closed: its
   SQLSTATE, message and constraint (`AbortCause`). A `25P02` is a
   placeholder the first real failure replaces — two statements pipelined
   in one scope may be reported in either order. The verdict stays the
   server's: a failure the scope recovered from itself, to a savepoint of
   its own, leaves the memory set and the transaction whole, and it commits.

5. **A failed rollback poisons the connection.** Its transaction state is
   then unknown; the connection is discarded as a dropped scope's is, and
   the enclosing scope is refused with `SessionError::Abandoned`. A refused
   `COMMIT` does not poison: the server has ended the block.

6. **The classification travels with the error.** `Begin`, `Commit` and
   `Rollback` carry `transient: bool`, set by the adapter where it sees the
   driver's error (`pg::transient`); `SessionError::is_transient()` answers
   at the crate's root, without a driver. `pg::transient_session` is
   withdrawn; the inbox and the outbox ask the error.

7. **The driver's client is no longer handed out.** `PgConnection::client()`
   is withdrawn: a statement issued past the wrapper would be neither
   observed nor remembered. Nothing used it. What it offered — prepared
   statements, `COPY` — comes back as wrappers when a consumer needs one.

8. **The observer hears the close.** The probe is reported as `COMMIT`, the
   statement it is (a trace that counts statement texts stays stable); a
   refused close is `QueryEnded { failed: true }`, followed by the rollback;
   `ScopeEnded` is `Succeeded` only when the closure succeeded and the close
   did.

The rule this gives a consumer: **a server error inside a transaction is
never absorbed in the scope that met it.** A refusal meant to be absorbed is
met in a nested `atomic` of its own — the scope fails, its savepoint is
rolled back, and the enclosing scope maps the failure to its own meaning
and goes on. Or the statement makes the refusal an outcome rather than an
error (`ON CONFLICT … DO NOTHING` covering the index that refuses), where
the outcome alone tells what happened.

## Consequences

- Code that absorbed a server error and carried on — the defect class — now
  fails loudly with the cause named, instead of losing its transaction. The
  first consumer found one: a role store, fixed by a nested scope.
- The probe costs one trivial statement per commit in the same round trip:
  measured 54–55 µs → 61–63 µs on an empty transaction, and within the
  run-to-run spread of a writing one (3.9–4.6 ms; `bench.sh`, pgbench 14.24,
  one client). A server log or `pg_stat_statements` shows a `SELECT 1` per
  commit; the observer does not.
- A nested scope that absorbed a failure is rolled back to its savepoint and
  refused, so the enclosing scope stays alive — as the trait promised and
  the old close broke.
- No connection returns to the pool inside a transaction.
- Breaking (the library is at version 0, used by one application):
  `SessionError::{Begin, Commit, Rollback}` became struct variants;
  `SessionError::Aborted` and `AbortCause` are new; `pg::transient_session`
  and `PgConnection::client()` are gone.
- A refused probe returns `Aborted { cause: None }` only if a failure
  reached the server without passing through the wrapper; with the client
  withdrawn, none can.

## Alternatives rejected

- **The connection's memory as the verdict.** Exact for statements through
  the wrapper and free of the probe, but a second state machine of the
  server's: any statement that restores a block the session did not issue —
  a `ROLLBACK TO` of the scope's own savepoint, an `ABORT` — leaves it
  stale, and a stale memory would refuse a healthy transaction. It names the
  cause; the server decides.
- **Reading the tag or the status byte from the driver.** The direct signal,
  and free — but tokio-postgres exposes neither, and a fork pinned beneath
  the pool is a dependency to carry for one byte. The upstream path stays
  open; the probe goes when it lands.
- **`pg_current_xact_id()` after `BEGIN` and `pg_xact_status()` after
  `COMMIT`.** Exact, but two more round trips and a transaction id spent on
  every transaction, read-only ones included.
- **A savepoint around every transaction's body**, released before the
  commit. It detects the abort at the release, but costs a subtransaction
  per transaction, more than the probe, and adds nothing the probe does not.
- **An absorbed failure classified by its cause** (a deadlock retried, a
  unique violation parked). It makes a retry pass where the deadlock does
  not recur, and so hides the code that absorbed it; the cause is named in
  the error, which is what fixing it needs.
- **`ON CONFLICT DO NOTHING` without a target as the general remedy** for a
  store's expected refusals, classifying a zero-row outcome by reading the
  row afterwards. Right for a store whose outcome alone is decisive; as a
  rule for every versioned write it turns a refusal the server named into an
  inference from two reads that can both find nothing — a concurrent change
  between them leaves "refused" with nobody holding. A nested scope keeps
  the server's named constraint.

## Executors

`crates/session/tests/pg.rs`, against a live PostgreSQL:

- `a_scope_that_absorbed_a_refused_statement_is_aborted_and_keeps_nothing`
  — `Aborted` with the `23505` and its constraint, nothing durable;
- `a_nested_scope_that_absorbed_a_refusal_is_aborted_and_the_outer_one_goes_on`
  — the nested scope refused, the outer one committing the rest;
- `a_failed_rollback_poisons_the_connection` — a scope that released its own
  savepoint: the rollback finds none, the connection is poisoned, the
  enclosing scope refused with `Abandoned`;
- `the_observer_sees_an_absorbed_failure_end_the_scope_as_failed`.

Each of the four fails with the close as it was (measured: the probe
replaced by `COMMIT`, the rollback after a refused close and the poisoning
removed — four failures, eleven passes). Three guards pass either way and
hold the decision's edges: `a_failure_the_scope_recovered_from_itself_does_not_abort_it`
(the memory decides nothing), `a_commit_the_server_refuses_is_a_failed_commit_and_the_connection_stays`,
`a_failure_before_a_scope_does_not_abort_it`. The classification is held
by the unit tests of `error.rs`.
