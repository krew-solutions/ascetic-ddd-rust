//! Integration tests for the PostgreSQL event store: streams appended and
//! read back, the optimistic lock, and ciphertext at rest. They need a
//! live database:
//!
//! ```text
//! ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
//!     cargo test -p ascetic-ddd-event-store --features pg -- --ignored
//! ```

#![cfg(feature = "pg")]

use std::str::FromStr;
use std::sync::Arc;

use ascetic_ddd_dek::PgDekStore;
use ascetic_ddd_event_store::{
    BoxError, CausalDependency, Codecs, Encoded, Encrypted, Error, EventCodec, EventMeta,
    EventStore, PgEventStore, Plain, StreamId,
};
use ascetic_ddd_kms::{Algorithm, PgKeyManagementService};
use ascetic_ddd_session::pg::Identifier;
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};
use ascetic_ddd_session::{PgAccess, PgSession, PgSessionPool, Session, SessionPool};
use serde_json::{Value, json};

const DEFAULT_URL: &str = "postgresql://devel:devel@localhost:5432/devel_karmabot_test";

fn pool() -> Pool {
    let url = std::env::var("ASCETIC_DDD_TEST_PG_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned());
    let manager = Manager::from_config(
        Config::from_str(&url).expect("a valid PostgreSQL URL"),
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("the pool can be built")
}

/// The events of an order, as an application would have them.
#[derive(Clone, Debug, PartialEq)]
enum Order {
    Placed { total: u32 },
    Shipped { carrier: String },
}

struct OrderCodec;

impl EventCodec<Order> for OrderCodec {
    fn encode(&self, event: &Order) -> Result<Encoded, BoxError> {
        Ok(match event {
            Order::Placed { total } => Encoded {
                event_type: "OrderPlaced".into(),
                event_version: 1,
                payload: json!({ "total": total }),
            },
            Order::Shipped { carrier } => Encoded {
                event_type: "OrderShipped".into(),
                event_version: 2,
                payload: json!({ "carrier": carrier }),
            },
        })
    }

    fn decode(&self, encoded: Encoded) -> Result<Order, BoxError> {
        Ok(match (encoded.event_type.as_str(), encoded.event_version) {
            ("OrderPlaced", 1) => Order::Placed {
                total: encoded.payload["total"].as_u64().ok_or("no total")? as u32,
            },
            ("OrderShipped", 2) => Order::Shipped {
                carrier: encoded.payload["carrier"]
                    .as_str()
                    .ok_or("no carrier")?
                    .to_owned(),
            },
            (other, version) => return Err(format!("unknown event {other} v{version}").into()),
        })
    }
}

type Deks = PgDekStore<PgKeyManagementService>;

struct Fixture<C> {
    sessions: PgSessionPool,
    store: Arc<PgEventStore<C, OrderCodec>>,
    table: String,
}

fn order(id: &str) -> StreamId {
    StreamId::new("tenant-1", "Order", id)
}

/// Tables of their own per test: the event log, the DEKs, the KEKs.
async fn encrypted(name: &str) -> Fixture<Encrypted<Deks>> {
    let sessions = PgSessionPool::new(pool());
    let kms = PgKeyManagementService::new(Algorithm::Aes256Gcm.generate_key().unwrap())
        .with_table(Identifier::new(format!("kms_keys_es_{name}")).unwrap());
    let deks = PgDekStore::new(kms).with_table(Identifier::new(format!("deks_es_{name}")).unwrap());
    sessions
        .session(async |session| {
            session
                .connection()
                .batch_execute(&format!(
                    "DROP TABLE IF EXISTS deks_es_{name}; DROP TABLE IF EXISTS kms_keys_es_{name};"
                ))
                .await
                .unwrap();
            deks.kms().setup(&session).await.unwrap();
            deks.setup(&session).await.unwrap();
            Ok::<_, Error>(())
        })
        .await
        .unwrap();
    fixture(name, Encrypted::new(deks)).await
}

async fn fixture<C: Codecs<PgSession>>(name: &str, codecs: C) -> Fixture<C> {
    let table = format!("event_log_{name}");
    let sessions = PgSessionPool::new(pool());
    let store = PgEventStore::new(codecs, OrderCodec).with_table(Identifier::new(&table).unwrap());
    sessions
        .session(async |session| {
            session
                .connection()
                .batch_execute(&format!("DROP TABLE IF EXISTS {table}"))
                .await
                .unwrap();
            store.setup(&session).await
        })
        .await
        .unwrap();
    Fixture {
        sessions,
        store: Arc::new(store),
        table,
    }
}

impl<C: Codecs<PgSession> + 'static> Fixture<C> {
    /// Runs `scope` in one transaction; the session goes by value (ADR-0004).
    async fn atomic<T, Fut>(
        &self,
        scope: impl FnOnce(PgSession, Arc<PgEventStore<C, OrderCodec>>) -> Fut + Send,
    ) -> Result<T, Error>
    where
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        let store = Arc::clone(&self.store);
        self.sessions
            .session(async |session| session.atomic(async |tx| scope(tx, store).await).await)
            .await
    }

    /// The raw rows of a stream: position, payload bytes, metadata.
    async fn rows(&self, stream: &StreamId) -> Vec<(i32, Vec<u8>, Value)> {
        let sql = format!(
            "SELECT stream_position, payload, metadata FROM {} \
             WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3 ORDER BY stream_position",
            self.table
        );
        self.sessions
            .session(async |session| {
                let rows = session
                    .connection()
                    .query(
                        &sql,
                        &[
                            &stream.tenant_id(),
                            &stream.stream_type(),
                            stream.stream_id(),
                        ],
                    )
                    .await?;
                Ok::<_, Error>(
                    rows.iter()
                        .map(|r| (r.get(0), r.get(1), r.get(2)))
                        .collect(),
                )
            })
            .await
            .unwrap()
    }
}

fn placed(total: u32) -> Order {
    Order::Placed { total }
}

fn shipped(carrier: &str) -> Order {
    Order::Shipped {
        carrier: carrier.to_owned(),
    }
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn events_appended_are_read_back_in_order_with_their_metadata() {
    let f = encrypted("round_trip").await;
    let meta = EventMeta {
        causation_id: Some("the-command".into()),
        correlation_id: Some("the-saga".into()),
        reason: Some("a customer ordered".into()),
        occurred_at: Some("2026-09-18T10:00:00Z".into()),
        causal_dependencies: vec![CausalDependency {
            tenant_id: "tenant-1".into(),
            stream_type: "Customer".into(),
            stream_id: json!("customer-3"),
            stream_position: 4,
        }],
        ..EventMeta::default()
    };
    let appended = f
        .atomic(|tx, store| {
            let meta = meta.clone();
            async move {
                store
                    .append(
                        &tx,
                        &order("order-7"),
                        0,
                        &[placed(42), shipped("DHL")],
                        &meta,
                    )
                    .await
            }
        })
        .await
        .unwrap();
    assert_eq!(
        appended.iter().map(|a| a.position).collect::<Vec<_>>(),
        [1, 2]
    );
    let ids: Vec<&String> = appended
        .iter()
        .map(|a| a.meta.event_id.as_ref().unwrap())
        .collect();
    assert_ne!(ids[0], ids[1], "each event has an id of its own");
    assert_eq!(
        appended[0].meta.causation_id.as_deref(),
        Some("the-command")
    );
    assert_eq!(
        appended[1].meta.causation_id.as_ref(),
        Some(ids[0]),
        "the second is caused by the first"
    );
    assert_eq!(appended[1].meta.correlation_id.as_deref(), Some("the-saga"));

    let history = f
        .atomic(|tx, store| async move { store.load(&tx, &order("order-7"), 0).await })
        .await
        .unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!((history[0].position, &history[0].event), (1, &placed(42)));
    assert_eq!(
        (history[1].position, &history[1].event),
        (2, &shipped("DHL"))
    );
    assert_eq!(history[0].meta, appended[0].meta);
    assert_eq!(
        history[1].meta.causal_dependencies,
        meta.causal_dependencies
    );
    assert_eq!(
        history[1].meta.occurred_at.as_deref(),
        Some("2026-09-18T10:00:00Z")
    );

    let since = f
        .atomic(|tx, store| async move { store.load(&tx, &order("order-7"), 1).await })
        .await
        .unwrap();
    assert_eq!(since.iter().map(|r| r.position).collect::<Vec<_>>(), [2]);
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn the_next_append_continues_where_the_last_one_ended() {
    let f = encrypted("continues").await;
    f.atomic(|tx, store| async move {
        store
            .append(
                &tx,
                &order("order-7"),
                0,
                &[placed(1)],
                &EventMeta::default(),
            )
            .await
    })
    .await
    .unwrap();
    let appended = f
        .atomic(|tx, store| async move {
            let history = store.load(&tx, &order("order-7"), 0).await?;
            let expected = history.last().map_or(0, |r| r.position);
            store
                .append(
                    &tx,
                    &order("order-7"),
                    expected,
                    &[shipped("UPS")],
                    &EventMeta::default(),
                )
                .await
        })
        .await
        .unwrap();
    assert_eq!(appended[0].position, 2);
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_second_writer_on_the_same_position_is_refused_and_its_transaction_with_it() {
    let f = encrypted("concurrent").await;
    f.atomic(|tx, store| async move {
        store
            .append(
                &tx,
                &order("order-7"),
                0,
                &[placed(1)],
                &EventMeta::default(),
            )
            .await
    })
    .await
    .unwrap();
    // Both decided on position 1; one appended first.
    f.atomic(|tx, store| async move {
        store
            .append(
                &tx,
                &order("order-7"),
                1,
                &[shipped("DHL")],
                &EventMeta::default(),
            )
            .await
    })
    .await
    .unwrap();
    let outcome = f
        .atomic(|tx, store| async move {
            store
                .append(
                    &tx,
                    &order("order-7"),
                    1,
                    &[placed(2), shipped("UPS")],
                    &EventMeta::default(),
                )
                .await
        })
        .await;
    assert!(
        matches!(outcome, Err(Error::ConcurrentUpdate { position: 2, .. })),
        "{outcome:?}"
    );
    let rows = f.rows(&order("order-7")).await;
    assert_eq!(rows.len(), 2, "the loser's events were rolled back");
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn the_payload_rests_as_ciphertext_under_the_stream_s_key() {
    let f = encrypted("at_rest").await;
    f.atomic(|tx, store| async move {
        store
            .append(
                &tx,
                &order("order-7"),
                0,
                &[shipped("a very visible carrier name")],
                &EventMeta::default(),
            )
            .await?;
        store
            .append(
                &tx,
                &order("order-8"),
                0,
                &[shipped("a very visible carrier name")],
                &EventMeta::default(),
            )
            .await
    })
    .await
    .unwrap();
    let seven = f.rows(&order("order-7")).await;
    let eight = f.rows(&order("order-8")).await;
    for (_, payload, metadata) in seven.iter().chain(&eight) {
        assert!(
            !payload.windows(7).any(|w| w == b"carrier"),
            "ciphertext at rest"
        );
        assert!(
            metadata.get("event_id").and_then(Value::as_str).is_some(),
            "{metadata}"
        );
    }
    assert_ne!(seven[0].1, eight[0].1, "each stream under its own key");
    // The stream's own history opens; a row moved to another stream would not.
    let history = f
        .atomic(|tx, store| async move { store.load(&tx, &order("order-8"), 0).await })
        .await
        .unwrap();
    assert_eq!(history[0].event, shipped("a very visible carrier name"));
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_plain_store_keeps_json_in_the_clear() {
    let f = fixture("plain", Plain).await;
    f.atomic(|tx, store| async move {
        store
            .append(
                &tx,
                &order("order-7"),
                0,
                &[placed(42)],
                &EventMeta::default(),
            )
            .await
    })
    .await
    .unwrap();
    let rows = f.rows(&order("order-7")).await;
    assert_eq!(rows[0].1, br#"{"total":42}"#);
    let history = f
        .atomic(|tx, store| async move { store.load(&tx, &order("order-7"), 0).await })
        .await
        .unwrap();
    assert_eq!(history[0].event, placed(42));
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn an_empty_stream_loads_as_nothing_and_needs_no_key() {
    let f = encrypted("empty").await;
    let history = f
        .atomic(|tx, store| async move { store.load(&tx, &order("nobody"), 0).await })
        .await
        .unwrap();
    assert!(history.is_empty());
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn two_setups_at_once_agree() {
    let f = fixture("setups_at_once", Plain).await;
    f.sessions
        .session(async |session| {
            session
                .connection()
                .batch_execute(&format!("DROP TABLE IF EXISTS {}", f.table))
                .await
                .unwrap();
            Ok::<_, Error>(())
        })
        .await
        .unwrap();
    let setup = || {
        f.sessions
            .session(async |session| f.store.setup(&session).await)
    };
    let (a, b) = tokio::join!(setup(), setup());
    a.unwrap();
    b.unwrap();
    f.atomic(|tx, store| async move {
        store
            .append(
                &tx,
                &order("order-7"),
                0,
                &[placed(1)],
                &EventMeta::default(),
            )
            .await
    })
    .await
    .unwrap();
}
