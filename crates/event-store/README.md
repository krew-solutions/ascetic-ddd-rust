# ascetic-ddd-event-store

An event store on PostgreSQL: a stream of events per aggregate, appended
after the position the writer expects and read back in order, the payload
sealed with the stream's data-encryption key from `ascetic-ddd-dek`. The
storing half of the `EventStore` of
`ascetic_ddd.seedwork.infrastructure.repository` (Python), in the shape of
functional event sourcing: events are values, the aggregate is a fold over
its stream, a decision is a function of the state, and the store stores.

```rust,no_run
# #[cfg(feature = "pg")]
# async fn example(sessions: ascetic_ddd_session::PgSessionPool, deks: ascetic_ddd_dek::PgDekStore<ascetic_ddd_kms::PgKeyManagementService>)
# -> Result<(), ascetic_ddd_event_store::Error> {
use ascetic_ddd_event_store::{Encoded, Encrypted, EventCodec, EventMeta, EventStore, PgEventStore, StreamId};
use ascetic_ddd_session::{Session, SessionPool};
use serde_json::json;

#[derive(Clone, Debug, PartialEq)]
enum Order { Placed { total: u32 }, Shipped }

/// The typed half: an event as a record, and back.
struct OrderCodec;
impl EventCodec<Order> for OrderCodec {
    fn encode(&self, event: &Order) -> Result<Encoded, ascetic_ddd_event_store::BoxError> {
        Ok(match event {
            Order::Placed { total } => Encoded { event_type: "OrderPlaced".into(), event_version: 1, payload: json!({ "total": total }) },
            Order::Shipped => Encoded { event_type: "OrderShipped".into(), event_version: 1, payload: json!({}) },
        })
    }
    fn decode(&self, encoded: Encoded) -> Result<Order, ascetic_ddd_event_store::BoxError> {
        Ok(match encoded.event_type.as_str() {
            "OrderPlaced" => Order::Placed { total: encoded.payload["total"].as_u64().unwrap_or(0) as u32 },
            "OrderShipped" => Order::Shipped,
            other => return Err(format!("unknown event type {other}").into()),
        })
    }
}

let store = PgEventStore::new(Encrypted::new(deks), OrderCodec);
let order = StreamId::new("tenant-1", "Order", "order-7");
sessions.session(async |session| {
    store.setup(&session).await?;
    session.atomic(async |tx| {
        // load, fold, decide, append
        let history = store.load(&tx, &order, 0).await?;
        let expected = history.last().map_or(0, |recorded| recorded.position);
        let decided = vec![Order::Placed { total: 42 }];
        let recorded = store.append(&tx, &order, expected, &decided, &EventMeta::default()).await?;
        assert_eq!(recorded[0].position, 1);
        Ok(())
    }).await
}).await
# }
# fn main() {}
```

## The model

A stream is named by a `StreamId` — the tenant, the aggregate's type, its
id as JSON — the identity the inbox and the outbox carry as headers, and
the `Resource` its data-encryption key belongs to. Events are numbered
from one. `append` takes the position the writer expects — the last it
loaded, zero for a new stream — and writes the next ones; another writer's
events in between are `ConcurrentUpdate`, the primary key's refusal, and the
transaction rolls back with it. `load` reads a stream from a position, in
order. Each event carries its `EventMeta`: an id minted at the append, its
cause — what `append` was given for the first, the event before it for the
rest — a correlation id, a reason, when as ISO 8601 text, and the
`causal_dependencies` the inbox reads, in the inbox's shape.

An event is a value of the application's own type. An `EventCodec` turns
one into an `Encoded` record — type name, version of the shape, JSON
payload — and back; that is the whole of what the sources did with an
exporter per event type and a reconstitutor per type and version. A
`PayloadCodec` turns the payload into the bytes the table keeps: `JsonCodec`,
`ZlibCodec` over it, `EncryptionCodec` over that with a `Cipher` from the
DEK store — the chain of the sources.

## Where the keys come from

`Codecs` says which payload codec a stream is written with and which it is
read with. `Encrypted` takes them from a `DekStore`: the write path seals
under the stream's current key, made on first contact, the read path opens
with every version the stream has — `Keyring` — so what an older key sealed
still opens. `Plain` keeps JSON in the clear, for development and for tests
of what is not encryption.

## What is not here

The aggregate. A fold `evolve(state, event)` and a decision
`decide(state, command)` are a bounded context's own functions; the store
neither knows them nor needs to. Nor the publishing of what was appended:
that is the command workflow's, through the outbox in the same
transaction. Nor a snapshot: a long stream is read whole.

## Deviations from the Python and Go sources

* The store stores. The sources' `EventStore._save` took the aggregate,
  emptied its pending events, and published them through a mediator after
  writing; here `append` takes events as values and returns what it
  recorded, and the workflow publishes. Exporters and reconstitutors are
  one `EventCodec`; queries registered per event type are gone.
* `append` takes the expected position and numbers the events itself,
  where the sources wrote the aggregate's own version into each event. The
  primary key is the lock in both.
* The first event's cause is what the caller names; the sources dropped it
  and chained from `None`.
* `causal_dependencies` are written in the shape the inbox reads —
  `tenant_id`, `stream_type`, `stream_id`, `stream_position` — where the
  Python sources wrote `aggregate_id`, `aggregate_type`, `aggregate_version`
  and the Python inbox read something else again.
* Ids are text, not UUID values; `occurred_at` is ISO 8601 text, as every
  serializable contract carries a date.
* `setup` is the adapter's, under an advisory lock, as in the other stores.

## Testing

```bash
cargo test -p ascetic-ddd-event-store                                    # the codecs, no database
ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
    cargo test -p ascetic-ddd-event-store --features pg -- --ignored    # the store on PostgreSQL
```
