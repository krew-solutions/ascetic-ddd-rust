# ADR-0006: A message ahead of its dependency waits out of the queue, and is woken by the dependency's mark

## Status

Accepted (2026-09-15).

## Context

A message may name causal dependencies, messages that must be processed
before it, and may arrive before them. The inbox stepped over such a row on
every walk of its partition: `LIMIT 1 OFFSET n`, one dependency check per
stepped-over row per poll, the row locked until the call's transaction ended,
for as long as the dependency was missing — for ever, if it never arrived,
which `InboxMissingDep.cfg` recorded as the contract for now. Ten such rows at
the head of a partition are ten statements on every poll before the first
eligible row.

## Decision

1. **Waiting is a state of the row.** A row whose dependencies are not
   processed gets `waiting_for`, the identity of the first missing one, and
   `waiting_since`; the queue is the rows with neither `processed_position`,
   `parked_at` nor `waiting_for`, and its index says so. The row stays where
   it is, so a later arrival of the same message is still a duplicate.

2. **The walk looks at the head only.** The oldest row of the queue is taken
   when it is due and its dependencies are processed, set aside when they are
   not, and left holding the queue when it waits for its backoff. The
   `OFFSET n` loop goes.

3. **The dependency's mark wakes what waited for it**, in the same
   statement: a data-modifying CTE updates the mark and clears `waiting_for`
   of every row that names the message, and the observer is told which. So
   does `resolve`. Nothing polls for a dependency.

4. **A wait may run out.** With `max_wait` set, a row waiting longer is
   parked with `last_error` naming the dependency, and `unpark` and `resolve`
   apply to it as to any parked row. By default a row waits for ever, as
   before, and costs nothing meanwhile.

5. **The observer reports `Waiting`, the woken rows with `Marked` and
   `Resolved`, and `Expired`.** `Skipped` goes: a row is set aside once, not
   stepped over again and again.

## Consequences

- Columns `waiting_for`, `waiting_since`; a head index on `received_position`
  over the queue; indexes on `waiting_for` and `waiting_since` over the
  waiting rows. `setup` adds them to an existing table.
- The mark is one statement still, so the happy path keeps its round trips:
  draining 200 messages with a subscriber that does nothing, 1.43–1.76 ms per
  message over three runs against 1.54–1.83 ms before, no difference the
  measurement can tell. Expiry is one statement per call, only where
  `max_wait` is set.
- Setting a row aside is written in the dispatcher's transaction; a rollback
  of that transaction puts the row back in the queue, where it is set aside
  again. The model treats the step as durable at once and says so.
- The model gains `waiting`, `Wait`, `Expire`, the invariant
  `WaitingIsAside` — a row waits only for a dependency of its own that is
  not processed — and the liveness `EventuallyAside`: with parking and
  expiring waits, every received message ends up processed or parked.
  `InboxNoWaiting.cfg`, the head holding the partition, must fail;
  `InboxMissingDepExpires.cfg` holds. The caveat about stepped-over rows
  staying locked goes with the stepping over.
- Trace validation follows: `wait`, `expire`, and the woken set checked
  against the model's on `commit` and `resolve`.

## Alternatives rejected

**A second table or a channel of the bus for waiting messages.** The row
would leave the table, and the next arrival of the same message would be a
new row: the idempotency the inbox exists for, gone. The same reason parking
is a column.

**Stepping over, with an index.** The cost per poll stays proportional to the
waiting rows, and the rows stay locked for the length of the call. Waiting
is a state, not a query.

**Waking by polling the waiting rows.** A statement per poll to re-check
every waiting row's dependency; the mark already knows what it releases.

**Expiring by default.** A missing dependency is a fact of the deployment,
not of the library; the timeout is the operator's choice, as `max_attempts`
is.
