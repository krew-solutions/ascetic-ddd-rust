# ADR-0010: Which failures stop the loops, which wait, and which park at once

## Status

Accepted (2026-09-17).

## Context

`run` stopped every loop on any `Err` from `dispatch`. In the inbox that was
any error of the database; in the outbox any error of the database and any
error of the subscriber, since `dispatch` returned the subscriber's error
(`crates/outbox/src/pg/dispatch.rs`, `handled.map_err(Error::Subscriber)?`).
So a broker away for a second stopped the outbox dispatcher process, and a
lock cycle between the subscriber's own writes and application traffic —
possible after ADR-0009 all the same, since the subscriber writes inside the
dispatcher's transaction — stopped the inbox loops with `40P01`. Supervisors
restart processes, so the failure mode was churn rather than loss; but a
loop that cannot tell an error of the moment from a defect can do no better
than die on both.

A subscriber's failure in the inbox was of one kind: tried again after a
backoff, parked after `max_attempts` (ADR-0005). A payload that cannot be
read was tried that many times, holding its slot's head meanwhile, for an
outcome known at the first attempt.

## Decision

1. **Errors of the database are told apart by SQLSTATE**, in one place for
   both adapters, `ascetic_ddd_session::pg::transient`. Of the moment:
   class `08` (connection), class `53` (resources), `40001`, `40P01`,
   `40003`, `57P01`–`57P03` (the server going down or not yet up); and,
   without a code, the connection's — closed, an I/O error, a connect or
   a timeout. Everything else is a defect: a statement, the schema, a value.
   `Error::is_transient` in both crates reads the table; a connection that
   could not be taken from the pool is of the moment too.

2. **A loop meeting an error of the moment waits and goes on.** The wait
   starts at `poll_interval` and doubles with each such error in a row, up
   to `Loops::max_pause`, sixty seconds by default. The transaction was
   rolled back, so the message, or the batch, comes back; the attempt is not
   counted, as ADR-0005 counts only what the subscriber returned. A defect
   stops every loop, and `run` returns it.

3. **In the outbox a failing subscriber is, for the loop, a failure of the
   moment**: the batch rolled back, the loop waits the same way and delivers
   it again. `dispatch` still returns `Err(Subscriber)` to a caller of its
   own. A message no subscriber will ever take stalls its slot, as before; a
   dead-letter for the outbox is a decision not taken, the case being judged
   unlikely.

4. **The inbox subscriber returns `Result<(), Failure>`.** Any error
   converts into `Failure::Transient`, so `?` keeps working and the message
   is tried again as before. `Failure::Permanent` is the subscriber's verdict
   that no retry will succeed — a payload it cannot read, an invariant the
   message breaks — and the message is parked at once, the attempt recorded,
   `last_error` set; the observer's `Failed` carries `permanent`.

## Consequences

- Tests: `a_loop_outlives_a_lost_connection` — the subscriber has the server
  terminate its connection; the loop waits, goes on and processes the
  message; `a_defect_stops_the_loops` — the subscriber renames the table, the
  mark fails on an undefined table, `run` returns `Err(Database)` and the
  rolled-back transaction leaves the table as it was;
  `a_permanent_failure_parks_the_message_at_once`; in the outbox,
  `run_outlives_a_failing_subscriber`; the SQLSTATE table has unit tests.
- Bounds of a wrong class: an unknown code is a defect and stops the loops,
  which is loud; an error of the moment that never ends is a warning per
  pause up to `max_pause`, and every `Dispatched` carries the error to the
  observer. The bus channel's consumer keeps its retry loop around `run`, as
  before.
- `Loops` gains `max_pause`; the inbox's `Error::Subscriber` carries a
  `Failure`; `Error::Malformed` names what the inbox reads back and cannot
  use — a transaction id, a snapshot, a table cut otherwise — which was
  filed under `Subscriber` before.
- The models are unchanged. A permanent verdict is the model's `Fail` with
  parking at the first attempt, `MaxAttempts = 1`; the loop's waits are the
  process's, outside the protocol.

## Alternatives rejected

**Stop on every error**, as before: churn under errors of the moment, and
the outbox process dying on a broker's hiccup.

**Try again on every error**: a defect — the schema drifted, a value of the
wrong type — would loop for ever, making no progress, with nothing but the
log to say so.

**Count an error of the database as an attempt**: ADR-0005 counts only what
the subscriber returned; an outage would park messages that nobody failed.

**Leave the classification to the subscriber**, returning "later" for
errors of the database it meets: it sees its own statements only, and the
dispatcher's mark can fail on the same connection.
