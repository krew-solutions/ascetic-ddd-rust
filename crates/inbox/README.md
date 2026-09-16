# ascetic-ddd-inbox

Transactional Inbox on PostgreSQL: incoming messages stored under their
identity, processed once each in the transaction that marks them processed,
and held back until what they causally depend on has been processed. A port
of `ascetic_ddd.inbox` (Python), adapted to Rust rather than transliterated.

```rust
// at the edge: a bus subscriber hands the message over
inbox.publish(&message).await?;

// a processing loop; `tx` is the transaction the mark commits in
inbox.run(|tx, message| handle(tx, message), Loops::default(), ctrl_c).await?;
```

## What it guarantees

* **Idempotency.** A message's identity is
  `(tenant_id, stream_type, stream_id, stream_position)` and is the primary
  key; receiving it again is `ON CONFLICT DO NOTHING`. A `message_id` in the
  metadata is unique in the table as well.
* **Once, with the work.** The subscriber runs inside the transaction that
  marks the message processed, and is given that transaction. Its writes and
  the mark commit together or not at all.
* **Causal order.** A message may name, in `metadata.causal_dependencies`,
  the messages that must be processed first. It is stepped over until they
  are, and looked at again next time.
* **One dispatcher at a time per slot.** A row carries its slot, the hash of
  a partition key, the URI or the stream, and a dispatcher is whoever holds
  the lock on that slot's row for the length of a transaction. With causal
  dependencies, cut by stream.

## The port and the adapter

The edge sees `Inbox`, one method, `publish`. Processing — `dispatch`,
`run`, `setup` — is on `PgInbox`, because it is the business of a separate
loop.

## As a channel of the bus

The inbox is also an adapter of `ascetic-ddd-bus` (ADR-0003). A bridge from
a broker channel to the inbox channel is the intake: every wire message is
stored under the identity its headers name, and the same identity again is
ignored. The consumer is transactional and comes from the inbox itself: its
handler runs inside the transaction that marks the message processed, and is
given that transaction.

```rust
let mut bus = Bus::new();
bus.register("kafka", kafka)?;
bus.register(INBOX_SCHEME, inbox.channel())?;
let bus = Arc::new(bus);

// the intake
let intake = Bridge::new(Arc::clone(&bus))
    .run("kafka://orders", "orders-intake", Target::Fixed("inbox://orders".into()))?;

// the processing; `tx` is the transaction the mark commits in
let orders = inbox.consumer(decode_order_placed);
let processing = orders.subscribe(|tx: PgSession, order: OrderPlaced| async move {
    place(&tx, order).await
})?;
```

Headers become columns: `tenant_id`, `stream_type`, `stream_id`,
`stream_position`; `destination`, the channel the message was sent to as
the outbox stamps it, becomes `uri`, and without one the inbox channel the
message was published to, key included, does. Every other header is a
field of `metadata`, structured again when its text is a JSON array or
object. Published from an outbox to `inbox://orders/order-7`, a message
crosses the outbox dispatcher's bridge into the inbox without a broker:
after-commit delivery and processing in the marking transaction, in one
database.

The processing loop is a task on the tokio runtime; cancel the subscription
to stop it.

## Dependencies

A message may name causal dependencies, messages that must be processed
before it, and may arrive before them. The walk looks at the head of the
queue only: a head whose dependencies are not all processed is set aside to
wait for the first missing one, `waiting_for`, out of the queue, and the
statement that marks that dependency processed puts every row waiting for it
back, in the same transaction. Nothing polls for a dependency, and a waiting
row costs the walk nothing. The row stays in the table, so a later arrival of
the same message is still a duplicate. With `with_max_wait` a row waiting
longer is parked with the dependency named in `last_error`; by default it
waits for ever (ADR-0006).

The mark and the waking are one statement, so the happy path keeps its round
trips. Draining 200 messages with a subscriber that does nothing, over
localhost, three runs: 1.43–1.76 ms per message, against 1.54–1.83 ms before
the waking was added; no difference the measurement can tell.

## Failures, backoff and parking

A subscriber that fails does not roll the whole transaction back. It runs in a
savepoint; its writes roll back to it, and the transaction goes on to record
the attempt: `attempts`, `last_error`, and `next_attempt_at`. Until that time
the message is not taken, and it holds its partition — the rows behind it wait
— so that the order of arrival survives the failure; a retry topic would let
them pass and lose it. After `max_attempts` failures the message is parked:
`parked_at` is set, the selection passes it by, the partition flows, and the
row stays where it is, so a later arrival of the same message is still a
duplicate. Off by default: unlimited attempts, no backoff, the message is
taken again at the next call (ADR-0005).

```rust,ignore
let inbox = PgInbox::new(pool)
    .with_retries(Retries::up_to(5).with_backoff(Retries::exponential(
        Duration::from_secs(1),
        Duration::from_secs(300),
    )));

match inbox.dispatch(&subscriber).await? {
    Outcome::Processed => {}
    Outcome::Failed { attempts, parked } => {}
    Outcome::Nothing => {}
}

for message in inbox.parked(&session).await? {
    inbox.unpark(&session, &message).await?;  // another go, attempts reset
    inbox.resolve(&session, &message).await?; // or: processed by hand, no effects
}
```

Only errors the subscriber returns are counted. A message that kills the
process is retried on restart with the count unchanged; counting at delivery
would need a lease with a timeout, which the inbox does without. A message
depending on a parked one waits, as it waits for a dependency that never
arrived, until the parked one is unparked and processed or resolved.

The savepoint costs two statements per message on the happy path. Draining
200 messages with a subscriber that does nothing, on one PostgreSQL over
localhost, best of three runs: 1.39 ms per message before, 1.54 ms after;
the other runs 1.69–1.74 ms before and 1.80–1.83 ms after. About 150 µs, a
tenth with a subscriber that does nothing, less with one that does anything.

## Slots

A row carries its slot, `hashtext(<partition key>) % slots` with the sign bit
cleared, stored at insert; the key, `ByUri` or `ByStream`, and the number of
slots are fixed for the life of the table, `partitioned_by` and
`with_slots(S)` before `setup` creates it, the URI and one slot by default.
The table `<table>_slots` has one row per slot. A dispatcher has no identity:
`dispatch(subscriber)` takes whichever slot has a due head and is held by
nobody, least recently served first, locks that slot's row for the length of
the transaction, `FOR UPDATE SKIP LOCKED`, and works the head of that slot.
Any number of loops in any number of processes share the slots through the
locks alone, `run(subscriber, Loops { concurrency, poll_interval },
shutdown)`; a process that dies releases its slot with its transaction, and
the next poll of a survivor takes it. One slot is the order of arrival over
the whole table; more slots are parallelism, and order within a stream
still, since a stream is in one slot. A head waiting for its backoff holds
its slot, and the other slots flow. Changing the number of slots or the key
moves rows between slots and is a migration, not a restart: `setup` refuses a
table cut otherwise, or one from before slots (ADR-0008).

The take is one statement. On 200,000 rows, a quarter of them processed:
0.07 ms for one slot, 0.21 ms for sixteen, 0.43 ms for sixty-four; idle
0.05–0.08 ms. The head index, `(slot, received_position)` over the rows
neither processed, parked nor waiting, is the only index in
`received_position` order on purpose: beside a second one over the whole
column the planner walked that one from the first row, past the whole
processed history, 89 ms at sixteen slots.

A table from before slots is migrated by hand, with the inbox stopped; for a
table `inbox` cut by URI into one slot:

```sql
ALTER TABLE inbox ADD COLUMN slot smallint
    GENERATED ALWAYS AS ((hashtext(uri) & 2147483647) % 1) STORED;
DROP INDEX inbox__head_idx;
ALTER TABLE inbox DROP CONSTRAINT inbox_received_position_key;
```

By stream the key is `tenant_id || ':' || stream_type || ':' || stream_id::text`.
The next `setup` adds the head index, `inbox_meta` and `inbox_slots`.

## Observing the inbox

`PgInbox::observed_by(observer)` attaches an `InboxObserver`, in the shape of
the session and outbox observers: a synchronous, infallible value, composed as
a tuple, fixed when the inbox is built. It is told of a message
received, with its order of arrival or that the identity was already there; a
row set aside to wait for a dependency; a slot taken and its head, or no
slot, or a slot whose head waits for its backoff; the subscriber's outcome;
the mark, with its order of processing and the rows it woke; the attempt
recorded after a failure, with whether it parked the message; rows whose
wait ran out, parked; the dispatcher's transaction closed, committed or
rolled back; a message unparked or resolved by an operator, with the rows
the resolve woke. A dispatcher's events name the slot it holds, which is
what tells one dispatcher's events from another's when several run at once:
a slot has one holder at a time. A stored message comes with the id of the
transaction that stored it, and every step of a walk over the table with the
snapshot its statement ran under, so that a recorded run says which rows
each walk could see whatever order two tasks' events were logged in. These
are the actions of the protocol model in `verify/tla/Inbox.tla`, with the
steps the model folds into one made visible, so a recording observer yields a
trace the model can be checked against. The tests do exactly that, through
the recorder of `ascetic-ddd-trace`: with `ASCETIC_DDD_TRACE_DIR` set they
write one JSON line per event, and `verify/tla/check.sh` replays those files
through the model.

## Deviations from the Python source

* A failed message is retried with a recorded attempt, may wait for a
  backoff and may be parked (ADR-0005); the source rolls the transaction back
  and retries at once, for ever.
* A message ahead of its dependency is set aside and woken by the
  dependency's mark (ADR-0006); the source steps over it on every walk.

* A causal dependency is a type, `CausalDependency`, with `serde`; entries
  in the metadata that are not dependencies are ignored.
* The slot of a row is `hashtext(<partition key>) % slots` with the sign bit
  cleared, stored with the row: in the source a negative hash matched no
  worker, so about half of all keys were never processed once
  `num_workers > 1`. A test spreads forty streams over three slots and checks
  that each is processed exactly once.
* Dispatchers have no identity (ADR-0008). The source told each worker its
  share, `hash % num_workers`, at start-up, so a dead worker's share stood
  still until it was restarted; here a dispatch takes whichever slot has
  work under a lock on that slot's row, and a survivor takes over.
* `run` stops cooperatively, between messages, on a future the caller
  passes. A subscriber returns a `Result`. There is no async iterator.
* `payload` is bytes, not JSONB, mirroring the outbox (ADR-0002).
* `tenant_id` is a `String`; `uri` is 255 characters rather than 60.
* The identifier is `message_id`, not `event_id`, as in the outbox.

## Testing

```bash
ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
    cargo test -p ascetic-ddd-inbox -- --ignored
```
