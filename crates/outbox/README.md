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
* **One dispatcher at a time per group.** The group's position row is locked
  for the length of a batch.
* **Partitioning.** `kafka://orders/order-7` names a partition key; the
  messages of one full URI go to one worker, in order. Workers share a
  selection by the hash of the URI.

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
index. The dispatcher runs on a thread of its own that blocks on the tokio
runtime: code generic over the session pool cannot show its future is
`Send`, so it cannot be a spawned task. Cancel the subscription before the
runtime shuts down.

## Deviations from the Python source

* The worker filter clears the sign bit of `hashtext(uri)`. In the source a
  negative hash matched no worker, so about half of all keyed URIs were never
  dispatched once `num_workers > 1`. A test publishes forty keyed messages
  and checks that three workers deliver each exactly once.
* `run` stops cooperatively, between batches, on a future the caller passes.
  A subscriber returns a `Result`; `run` returns the first error after
  stopping the other loops.
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
