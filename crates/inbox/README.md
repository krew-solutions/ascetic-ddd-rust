# ascetic-ddd-inbox

Transactional Inbox on PostgreSQL: incoming messages stored under their
identity, processed once each in the transaction that marks them processed,
and held back until what they causally depend on has been processed. A port
of `ascetic_ddd.inbox` (Python), adapted to Rust rather than transliterated.

```rust
// at the edge: a bus subscriber hands the message over
inbox.publish(&message).await?;

// a processing loop; `tx` is the transaction the mark commits in
inbox.run(|tx, message| handle(tx, message), Workers::default(), ctrl_c).await?;
```

## What it guarantees

* **Idempotency.** A message's identity is
  `(tenant_id, stream_type, stream_id, stream_position)` and is the primary
  key; receiving it again is `ON CONFLICT DO NOTHING`. An `event_id` in the
  metadata is unique in the table as well.
* **Once, with the work.** The subscriber runs inside the transaction that
  marks the message processed, and is given that transaction. Its writes and
  the mark commit together or not at all.
* **Causal order.** A message may name, in `metadata.causal_dependencies`,
  the messages that must be processed first. It is stepped over until they
  are, and looked at again next time.
* **Concurrent dispatchers.** `FOR UPDATE SKIP LOCKED`: each takes a message
  of its own. Workers share the messages by the hash of a partition key,
  the URI or the stream; with causal dependencies, partition by stream.

## The port and the adapter

The edge sees `Inbox`, one method, `publish`. Processing — `dispatch`,
`run`, `setup` — is on `PgInbox`, because it is the business of a separate
loop.

## Deviations from the Python source

* A causal dependency is a type, `CausalDependency`, with `serde`; entries
  in the metadata that are not dependencies are ignored.
* The worker filter clears the sign bit of `hashtext`: in the source a
  negative hash matched no worker, so about half of all keys were never
  processed once `num_workers > 1`. A test spreads forty streams over three
  workers and checks that each is processed exactly once.
* `run` stops cooperatively, between messages, on a future the caller
  passes. A subscriber returns a `Result`. There is no async iterator.
* `payload` is bytes, not JSONB, mirroring the outbox (ADR-0002).
* `tenant_id` is a `String`; `uri` is 255 characters rather than 60.

## Testing

```bash
ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
    cargo test -p ascetic-ddd-inbox -- --ignored
```
