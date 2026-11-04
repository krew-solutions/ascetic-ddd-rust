# ascetic-ddd-outbox

Transactional Outbox on PostgreSQL: a message is committed in the same
transaction as the state change it announces, and a dispatcher sends what
was committed, in order, at least once. A port of `ascetic_ddd.outbox`
(Python), adapted to Rust rather than transliterated.

```rust
session.atomic(async |tx| {
    orders.save(tx, &order).await?;
    outbox.publish(tx, &OutboxMessage::new("kafka://orders", payload, metadata)).await?;
    Ok(())
}).await?;

// a dispatcher process
outbox.run(send_to_broker, &Selection::group("broker"), Workers::default(), ctrl_c).await?;
```

## What it guarantees

* **Atomicity.** `publish` writes into the caller's transaction. Either the
  state change and the message are committed, or neither is.
* **Order** (ADR-0035). Rows carry the inserting transaction's id (`xid8`). A
  dispatcher hands rows on by epochs: an epoch is what became visible between
  the snapshot it last delivered and the current one, handed on in the order
  the positions were drawn. So each stream's messages on one URI first leave
  in their writers' commit order — for an aggregate, its versions' order —
  whatever each writer wrote first, provided its rows are written after the
  point that serialises it with the stream's other writers (decision 7); and
  a transaction left open delays its own rows only. A serial alone cannot do
  this: a slow transaction commits after a fast one that took the next
  number. The position sequence must draw one value at a time (`CACHE 1`),
  which `migrate` and every epoch's start check.
* **At least once.** The subscriber runs inside the dispatcher's transaction
  and the position is acknowledged after the batch. A crash in between
  redelivers; consumers deduplicate on `metadata.message_id`, which is unique
  within a generation of the ring.
* **One dispatcher at a time per slot.** A slot's position row is locked for
  the length of a batch, and a dispatcher is whoever holds that lock.
* **Slots.** `kafka://orders/order-7` names a key; the messages of one full
  URI are in one slot, `hashtext(uri) % slots`, stored with the row, and go
  out in order. One slot by default.

## The port and the adapter

The application layer sees `Outbox`, one method, `publish`. A test double
collects messages. Everything else — `dispatch`, `run`, positions, `migrate` —
is on `PgOutbox`, because dispatching is the business of a separate process.

## As a channel of the bus

The outbox is also an adapter of `ascetic-ddd-bus` (ADR-0003). Its producer
is transactional, built once at the composition root for a destination on
another channel, and publishing takes the session of the current
transaction. Its consumer is the dispatcher: every committed row reaches the
handler as a wire message whose `destination` header is the row's URI, and a
`Bridge` to that header is the whole dispatcher process.

```rust
let mut bus = Bus::new();
bus.register(OUTBOX_SCHEME, outbox.channel())?;
bus.register("kafka", kafka)?;
let bus = Arc::new(bus);

// in the command handler, inside the transaction
let placed = outbox.producer("kafka://orders/order-7", encode_order_placed);
session.atomic(async |tx| placed.publish(tx, &event).await).await?;

// the dispatcher process
let dispatcher = Bridge::new(bus).run("outbox://all", "dispatcher", Target::Header("destination".into()))?;
```

Headers travel as string fields of `metadata`, so `message_id` keeps its unique
index. The dispatcher is a task on the tokio runtime; cancel the subscription
to stop it. `with_loops` on the outbox says how it runs — how many loops in
this process, how long one waits — the same `Loops` that `run` takes.

## Lanes

`bus.consumer("outbox://all", group, …)` takes every row;
`outbox://<scheme>` takes the rows addressed to that scheme —
`outbox://inbox` those to `inbox://…`, `outbox://kafka` those to
`kafka://…` — with positions of their own. A destination that is down then
holds back its own lane only: a service's Commands to itself are not
retried behind a broker's outage, and its events do not wait behind its own
fan-out (ADR-0033). A lane reads the other lanes' rows once, as it passes
them.

## When the relay stops

A database moved by dump and restore restarts its transaction ids below
what the positions delivered: every row written meanwhile would count as
delivered. A look that finds a position delivered past the server's ids
records the refusal in the meta table and stops; the refusal sticks, also
once the ids pass the old ones again, until an operator calls `rebase()`
(ADR-0035 decision 12, ADR-0033 decision 3). The rebase keeps, in position
order, what no position handed on — the rows written since the restore, and
the restored rows some group's `delivered` does not see, the first of each
identity — writes them again under its own id, owed to every group, sets
every position to its snapshot, taken before it wrote anything, and moves
the ring's bounds back. Once the server's ids reach the restored ring's,
the two kinds of rows cannot be told apart, and it refuses.
`clear_counter_reset` lifts the refusal alone, for an operator who moved the
positions by other means. A
sequence that caches positions, or a format newer than the binary, stops it
too. Each is an error, not a moment: logged at `error`, for the host's alarm.

`backlog(session, selection)` tells, per slot, how long the epoch in flight
has been handed on and how far, and how long ago `delivered` last moved:
the lag, since snapshots have no minimum.

## Observing the outbox

`PgOutbox::observed_by(observer)` attaches an `OutboxObserver`, in the shape of
the session observers: a synchronous, infallible value, composed as a tuple,
fixed when the outbox is built. It is told of six things — a message
published, with the id of the writing transaction and the row's position; a
batch fetched, with the epoch, what was delivered before it and the snapshot
it was read under; a message handed to the subscriber, with the outcome; the
position acknowledged, with whether the epoch drained and whether short of
its end; an epoch started, with its range of positions; the dispatcher's
transaction closed, committed or rolled back. They were the actions of the
protocol model in `verify/tla/Outbox.tla`, written for ADR-0007's rule; the
statement-level model of the epochs is `verify/tla/OutboxPg.tla`, and the
trace tools follow epochs once that model refines the protocol's. A recording
observer yields a trace. The tests do
exactly that, through the recorder of `ascetic-ddd-trace`: with
`ASCETIC_DDD_TRACE_DIR` set they write one JSON line per event, and
`verify/tla/check.sh` replays those files through the model.

## Slots

A row carries its slot, `hashtext(uri) % slots` with the sign bit cleared,
stored at insert; the number of slots is fixed for the life of the table,
`with_slots(S)` before `migrate` creates it, one by default. Each
`(consumer_group, uri, slot)` keeps a position of its own. A dispatcher has
no identity: `dispatch(subscriber, selection)` takes whichever slot of the
selection has visible work and is held by nobody, least recently served
first, locks that slot's position for the length of the batch,
`FOR UPDATE SKIP LOCKED`, and moves it. Any number of loops in any number of
processes share a selection through the locks alone, `run(subscriber,
selection, Loops { concurrency, poll_interval }, shutdown)`; a process that
dies releases its slot with its transaction, and the next poll of a survivor
takes it. One slot is one position per group and the order of the whole
selection; more slots are parallelism, and order within a URI still, since a
URI is in one slot. Changing the number of slots moves rows between
positions and is a migration, not a restart: `migrate` refuses a table cut
otherwise (ADR-0007).

The tables are a component of the database's migration ledger, `outbox:<table>`
(ADR-0029): `migrate` creates them once and is a read of the ledger after,
so that a process starts beside live traffic without locking them;
`teardown` drops them and forgets them, for an outbox that lives as long as
a test.

The fetch is one statement. On 200,000 rows, with work at three quarters of
the table: 0.06 ms for one slot, 0.20 ms for sixteen, 0.26 ms for
sixty-four; idle, 0.03, 0.10 and 0.11 ms.

## The ring

The table is a ring of `K` partitions by generation (ADR-0033), eight of
2²³ transaction ids by default, `with_ring(Ring::new(K, R))` before
`migrate` makes it: a transaction's rows land in generation `(id / R) % K`,
held there by a `CHECK`, and the ring is fixed for the table's life, as the
slots are. Every look, epoch start and batch names the generations it reads,
as constants, so that it reads and locks those alone: a relation's lock
lasts to the dispatcher's commit, and a fetch naming them all would hold off
the emptying of a generation for as long as the relay is busy.

`migrate` makes the ring of an existing table: the steps before its own;
the numbers checked against the table's; a transaction of its own that fixes
the copy's bound — every position with no epoch and nothing new of its
selection advanced first, as an idle look would, so that a slot never
matched does not pull it down — and logs how many rows the step will copy;
then the ring's step, copying the rows from the bound on. A host drains its
relays first, which keeps the bound near the head. A rewind below the ring's
`intact_from` is refused, `Error::Reclaimed`; back to the start lands on it,
and a new group starts there. A producer that declares its replay horizon,
`with_replay_horizon(d)`, has a rewind refused, `Error::BeyondReplayHorizon`,
when the first row it would replay is older: its consumers' inboxes remember
a processed message only for a window that covers the horizon (ADR-0033
decision 4). A lane starts where its group's selection of
everything stands, so that switching a relay to lanes hands nothing on again.

## Emptying the ring

A dispatcher's loops work a pass of the reclamation between their batches,
once an interval of the process, `with_reclaiming(Reclaiming { interval,
margin, margin_age, lock_timeout })`; `reclaim()` works one on demand. A
pass takes the generation of the oldest row left and empties it, by
`TRUNCATE`, once it is
- past: its newest window lies `margin` windows below the next id's;
- closed: no transaction of this database holding an id in it is open — one
  left open in another database holds nothing;
- passed: every position's `delivered` sees every row of it;
- old enough: every group passed it `margin_age` ago, so that a row relayed
  a moment ago can still be re-injected.

Under the partition's lock, taken within `lock_timeout`, it reads "passed"
again — this is what keeps the rows — then records `gone_below`, the least
row left, and `intact_from`, one past the highest id emptied. Its outcome
goes to the observer, `on_reclamation`: a generation emptied, or the pass
skipped and why, the position pinning the ring among the reasons. A group
that pins it for good is dropped with `forget_group`, which refuses one that
moved a position within the margin's age. Its errors are logged and never
stop dispatch; a counter reset stops it as it stops the relay.


## Deviations from the Python source

* The slot of a row is `hashtext(uri) % slots` with the sign bit cleared,
  stored with the row. In the source a negative hash matched no worker, so
  about half of all keyed URIs were never dispatched once `num_workers > 1`.
  A test publishes forty keyed messages and checks that three slots deliver
  each exactly once.
* Dispatchers have no identity (ADR-0007). The source split a group's work by
  `hash % num_workers` per worker with a position per worker, so changing the
  number of workers moved messages between positions and lost some; here a
  fetch takes whichever slot has work under a lock on that slot's position.
* `run` stops cooperatively, between batches, on a future the caller passes.
  A subscriber returns a `Result`; a batch whose subscriber failed is rolled
  back and delivered again after a pause that grows with each failure in a
  row, and so is one that met an error of the moment in the database; a
  defect of the database stops the loops and `run` returns it (ADR-0010).
* No async iterator: an iterator cannot know when the consumer is done with
  a message, and the acknowledgement must follow the processing.
* The transaction id is a `u64`, which is exactly `xid8`.
* `payload` is bytes, not JSONB: serialized, and encrypted where required,
  before the outbox (ADR-0002); `metadata` stays JSONB.
* The identifier is `message_id`, not `event_id`: the outbox carries command
  messages as well as event messages, and the receiver deduplicates on the
  message, whatever it says. This port is the reference now; the other
  ports follow.

## Testing

```bash
ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
    cargo test -p ascetic-ddd-outbox -- --ignored
```
