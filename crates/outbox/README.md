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
* **Order.** Rows carry the inserting transaction's id (`xid8`) and are read
  only once that transaction is older than every running one, in
  `(transaction_id, position)` order. A serial alone cannot do this: a slow
  transaction commits after a fast one that took the next number.
* **At least once.** The subscriber runs inside the dispatcher's transaction
  and the position is acknowledged after the batch. A crash in between
  redelivers; consumers deduplicate on `metadata.message_id`, which is unique
  in the table.
* **One dispatcher at a time per slot.** A slot's position row is locked for
  the length of a batch, and a dispatcher is whoever holds that lock.
* **Slots.** `kafka://orders/order-7` names a key; the messages of one full
  URI are in one slot, `hashtext(uri) % slots`, stored with the row, and go
  out in order. One slot by default.

## The port and the adapter

The application layer sees `Outbox`, one method, `publish`. A test double
collects messages. Everything else — `dispatch`, `run`, positions, `setup` —
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
to stop it.

## Observing the outbox

`PgOutbox::observed_by(observer)` attaches an `OutboxObserver`, in the shape of
the session observers: a synchronous, infallible value, composed as a tuple,
fixed when the outbox is built. It is told of five things — a message
published, with the id of the writing transaction and the row's position; a
batch fetched, with the visibility horizon of the statement that read it; a
message handed to the subscriber, with the outcome; the position acknowledged;
the dispatcher's transaction closed, committed or rolled back. These are the
actions of the protocol model in `verify/tla/Outbox.tla`, so a recording
observer yields a trace the model can be checked against. The tests do
exactly that, through the recorder of `ascetic-ddd-trace`: with
`ASCETIC_DDD_TRACE_DIR` set they write one JSON line per event, and
`verify/tla/check.sh` replays those files through the model.

## Slots

A row carries its slot, `hashtext(uri) % slots` with the sign bit cleared,
stored at insert; the number of slots is fixed for the life of the table,
`with_slots(S)` before `setup` creates it, one by default. Each
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
positions and is a migration, not a restart: `setup` refuses a table cut
otherwise (ADR-0007).

The fetch is one statement. On 200,000 rows, with work at three quarters of
the table: 0.06 ms for one slot, 0.20 ms for sixteen, 0.26 ms for
sixty-four; idle, 0.03, 0.10 and 0.11 ms.

## What is not here

The outbox keeps every row it ever stored; nothing deletes acknowledged
messages. Retention is the deployment's: the table is meant to be rotated by
partitions, which is simpler and cheaper than a cleaner inside the library.
The fetch does not slow down with the history, because it reads from the
group's position on.

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
