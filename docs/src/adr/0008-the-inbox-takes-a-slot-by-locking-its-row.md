# ADR-0008: The inbox takes a slot by locking its row, and its dispatcher has no identity

## Status

Accepted (2026-09-16). ADR-0007 applied to the inbox: the question that ADR
left open. Point 2 amended by ADR-0009: the head is read in a statement of
its own after the slot's lock, the one statement's snapshot being older
than the lock it takes.

## Context

The inbox spread the queue over workers as the outbox did: `Worker { id, of }`
given at start-up, every walk statement filtering rows by
`hashtext(<partition key>) % of = id`. The state is per row, so a change of
`of` lost nothing; the other defects stayed. A worker that died took its share
with it until it was restarted, and nothing noticed. A rolling change of `of`
had two layouts running at once, and two workers then served one stream: the
order of arrival held only "given one dispatcher per partition", a condition
the model stated and nothing enforced. And to tell one dispatcher's events
from another's in a trace, the caller had to number its `dispatch` calls — an
identity the protocol never needed, carried through the API for the observer.

## Decision

1. **The division is a column.** A row carries its slot,
   `(hashtext(<key>) & 2147483647) % S`, a stored generated column computed
   once at insert; the key is `uri` or the stream, as `partitioned_by`
   names it. `S` and the key's expression are a row of `<table>_meta`, fixed
   for the life of the table; `setup` refuses a table cut otherwise.

2. **Ownership is a transactional lock.** `<table>_slots` has one row per
   slot. A take is one statement: the slot least recently served among those
   whose head — the oldest row neither processed, parked nor waiting — is
   due, `FOR UPDATE OF s SKIP LOCKED`; then that head, locked, under the same
   snapshot. A head in its backoff is not due, so its slot is not taken: it
   holds its slot, as ADR-0005 decided, and the others flow. Setting a head
   aside for its dependency (ADR-0006) and looking at the next happens within
   the held slot.

3. **A dispatcher has no identity.** `dispatch(subscriber)`;
   `run(subscriber, Loops { concurrency, poll_interval }, shutdown)`. Any
   number of loops in any number of processes share the slots through the
   locks; a process that dies releases its slot with its transaction, and the
   next poll of any survivor takes it.

4. **One slot by default.** The order of arrival over the whole table; more
   slots are parallelism, and order within a stream still, since a stream is
   in one slot. Changing `S` or the key is a migration, not a restart.

## Consequences

- `Worker`, `Workers` and the call number go; `Loops` is the layout of one
  process. Two dispatchers on one slot: one works, the other finds nothing,
  where before row-level `SKIP LOCKED` gave each a message of the same
  stream. The order of arrival within a slot is a guarantee now, not a
  condition; the model's `ArrivalOrder` asks for one slot, not for one
  dispatcher per partition.
- The `Deferred` event goes: a slot whose head waits for its backoff is not
  taken. `Fetched` names the slot taken, or none, and the count of slots;
  `Dispatched` the slot held. In a trace a dispatcher is its slot — a slot
  has one holder at a time, so the events between a fetch naming it and the
  close naming it are one dispatch. The close is logged after the COMMIT,
  which is what frees the slot, so the next holder's fetch may be logged
  first; the take proves the close came first, and `trace2tla.py` moves it
  before the take, for the outbox too. A fetch that took no slot must find
  nothing due in the slots nobody holds; the held ones were passed by.
- The head index, `(slot, received_position)` over the queue, is the only
  index in `received_position` order. Measured on 200,000 rows, a quarter of
  them processed and at the front, as in any inbox with a history: with
  `received_position UNIQUE` beside it the planner, taking the queue rows for
  evenly spread, walked that index from the first row — 8.8 ms for one slot,
  89 ms for sixteen, and worse the longer the history. Without it 0.07, 0.21
  and 0.43 ms for one, sixteen and sixty-four slots; idle 0.05–0.08 ms. The
  sequence keeps the column unique.
- The expiry of waits (ADR-0006) is a sweep over the whole table before the
  take; one dispatch is a sweep, a take, a walk within the slot, the outcome.
- Model: `Slots` replaces `Workers` and `Dispatchers`, a dispatcher being a
  slot; `Bridge.tla` takes `DstSlots`. The state space is unchanged.
- No migration path from a table before slots: no such table exists, in
  this repository or in the project the crate is for, and a mechanism with
  no executor is not a decision. `setup` creates the current shape; a table
  without the column would fail its `CREATE INDEX` aloud.

## Alternatives rejected

**Keep `Worker { id, of }`**, the source's design. A dead worker's share stands
still; two layouts overlap during a change; the order holds by convention.
See Context.

**Take the head under row-level `SKIP LOCKED` and hold its key with
`pg_try_advisory_xact_lock(hashtext(key))`.** No table of slots, no fixed
`S`. But the second row of a key is visible and unlocked while the first is
worked, so the walk must still skip by key — the filter this decision
removes — and a dispatcher that finds a key held steps to the next head, one
statement per held key ahead of it. Nothing is served least recently, and the
lock names a key after the row was chosen rather than choosing the row.

**A lease table with heartbeats.** Process identity and timeouts, rejected in
ADR-0007 for the same reasons.
