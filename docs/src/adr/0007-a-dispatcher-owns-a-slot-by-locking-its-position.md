# ADR-0007: A dispatcher owns a slot by locking its position, and has no identity

## Status

Accepted (2026-09-16). Reached through a discriminator pass: three designs
from different premises, three checked objections each, a synthesis.

## Context

The outbox spread a group's work over workers by `hashtext(uri) % n`, `n`
and the worker's id given at start-up, with a position per worker,
`group:id`. Two defects. A worker that died took its share with it until it
was restarted, and nothing noticed. And changing `n` re-hashed every URI onto
a different worker while each position kept the value it had for the old
share: a position ahead of a message that used to be a slower neighbour's
passed it over, and the message was lost — `NoPassedOver` broken at the level
of the deployment, out of the model's sight, since the model fixes the
assignment for the whole behaviour.

A first fix moved the division into configuration, `with_partitions(P)`,
with a position per partition and processes taking partitions by
`p % num_processes`. It relocated the loss rather than removing it — `P` was
a runtime parameter of the fetch, and a process built with another `P`
re-sliced silently — and it put partitions into the API: "Всё, что нужно
знать Outbox — это сколько воркеров и какой воркер спрашивает."

## Decision

1. **The division is a column.** A row carries its slot,
   `(hashtext(uri) & 2147483647) % S`, a stored generated column computed
   once at insert; `S` is a literal of the DDL and a row of `<outbox>_meta`,
   fixed for the life of the table. No statement recomputes the hash, so
   nothing at run time can re-slice. `with_slots(S)` is read by `setup` only,
   which refuses a table cut otherwise, or a table from before slots.

2. **Ownership is a transactional lock.** Positions are rows
   `(consumer_group, uri, slot)`. A fetch is one statement: it takes the
   slot of the selection least recently served among those with visible
   work — asked as "its first row past the position, or none", ordered and
   limited to one — locks that slot's position row `FOR UPDATE SKIP LOCKED`,
   and reads the slot's batch under the same snapshot. The ack moves that
   one row.

3. **A dispatcher has no identity.** `dispatch(subscriber, selection)`;
   `run(subscriber, selection, Loops { concurrency, poll_interval }, ..)`.
   Any number of loops in any number of processes share a selection through
   the locks; a process that dies releases its slot with its transaction,
   and the next poll of any survivor takes it. Rolling deployments are
   indistinguishable from steady state. The worker's identity was only ever
   needed to name a position row; a lock names it.

4. **One slot by default.** One position per group, the order of the whole
   selection, the behaviour before this decision. More slots are
   parallelism, and order within a URI still, since a URI is in one slot.
   Changing `S` is a migration of the table, not a restart.

## Consequences

- `Worker` and `Workers` go; `Loops` stays as the layout of one process.
  `positions(selection)` returns a position per slot, whose minimum is a
  watermark for the selection; `set_position` moves every slot.
- The position rows of a selection are created at its first contact, inside
  the transaction of that dispatch; a rollback there recreates them next
  time. On the steady path the fetch is the only statement before the
  batch.
- Measured on 200,000 rows, one statement, best of runs: with work at three
  quarters of the table 0.06 ms for one slot, 0.20 ms for sixteen, 0.26 ms
  for sixty-four; idle 0.03, 0.10 and 0.11 ms. With `EXISTS` as the probe
  the one-slot case cost 2.4 ms, a bitmap over the tail.
- A batch is one slot's; at low traffic batches fragment across slots. A
  fetch taking several unlocked slots at once, with an ack per slot, is the
  refinement if that matters, and needs a model change.
- Observer events name the slot; `Slots` replaces `Workers` in the model,
  the state space unchanged. The model gains `Rehash`, the assignment
  changing under the positions, and `OutboxRehash.cfg`, which must violate
  `NoPassedOver`: the deployment-level loss made visible, and the reason the
  slot is stored with the row.
- `hashtext` is `IMMUTABLE` in the catalogue; should a PostgreSQL major
  change its output, stored slots keep the rows where they are, and only
  rows inserted after the upgrade would land elsewhere — an order break at
  that instant, not a loss.
- The inbox keeps `Worker { id, of }`: its state is per row, and its loss
  was never possible. Whether it too should drop identity — a dispatcher
  taking the head of the queue and `pg_try_advisory_xact_lock`ing its key —
  is the user's open decision.

## Alternatives rejected

**Partitions by configuration, positions per partition** (the first fix).
The loss relocated to a runtime parameter; the API burdened. See Context.

**Positions keyed by layout, `group:id/of`, a new layout starting from the
minimum of the last.** Keeps `Worker { id, of }` literally. Redelivery on a
layout change is bounded only by the slowest worker's lag; a rollback to an
earlier layout replays from its stale rows, and resetting the other layout's
rows during an overlap livelocks; the ack was an upsert, so a zombie of a
retired layout recreated its rows and kept consuming. Four coupled rules to
keep two integers in the API.

**Per-message acks, as the inbox.** An ack row per message per group, rotated
in lockstep with the outbox, and an anti-join that needs a low watermark to
stay `O(batch)` — the watermark, and its problem, return. The outbox's
parallelism is the division of its watermark.

**Ownership through session advisory locks or a lease table.** Both take
partitions rather than processes and would stand on this decision; the
transactional lock on the position row makes neither necessary here, and
survives a connection pooler in transaction mode, which session locks do not.
