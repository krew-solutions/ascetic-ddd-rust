//! The intake (ADR-0036 Part A): every message handed to the inbox is
//! stored, found already stored, or set aside beside it, and the source is
//! held only when the environment fails. They need a live database:
//!
//! ```text
//! ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
//!     cargo test -p ascetic-ddd-inbox --test intake -- --ignored
//! ```

use std::str::FromStr;
use std::sync::{Arc, Mutex};

use ascetic_ddd_bus::identity::{STREAM_ID, STREAM_POSITION, STREAM_TYPE, TENANT_ID};
use ascetic_ddd_bus::{Adapter, Message};
use ascetic_ddd_inbox::observer::{InboxObserver, Rejected};
use ascetic_ddd_inbox::{Admission, ByStream, Error, PgInbox, Reason};
use ascetic_ddd_session::pg::Identifier;
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};
use ascetic_ddd_session::{PgAccess, PgSessionPool, SessionPool};

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
        .max_size(4)
        .build()
        .expect("the pool can be built")
}

/// What the observer was told was set aside: the channel, the reason, the
/// Tenant's header and whether it was a repeat.
#[derive(Default)]
struct Seen(Mutex<Vec<Told>>);

/// One row set aside, as the observer was told it.
type Told = (String, Reason, Option<String>, bool);

impl InboxObserver for Seen {
    fn on_rejected(&self, event: &Rejected<'_>) {
        self.0.lock().unwrap().push((
            event.channel.to_owned(),
            event.reason,
            event.tenant_id.map(str::to_owned),
            event.repeat,
        ));
    }
}

struct Fixture {
    inbox: Arc<PgInbox<PgSessionPool, Arc<Seen>>>,
    seen: Arc<Seen>,
    table: String,
    sessions: PgSessionPool,
}

/// A table of its own per test; admitting only `orders.*` on `orders`.
async fn fixture(name: &str) -> Fixture {
    let table = format!("intake_{name}");
    let seen = Arc::new(Seen::default());
    let inbox = PgInbox::new(PgSessionPool::new(pool()))
        .with_table(
            Identifier::new(&table).unwrap(),
            Identifier::new(format!("{table}_seq")).unwrap(),
        )
        .partitioned_by(ByStream)
        .observed_by(Arc::clone(&seen))
        .admitting(|channel, stream_type| {
            channel != "inbox://orders" || stream_type.starts_with("orders.")
        });
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    Fixture {
        inbox: Arc::new(inbox),
        seen,
        table,
        sessions: PgSessionPool::new(pool()),
    }
}

impl Fixture {
    async fn count(&self, sql: &str) -> i64 {
        let sql = sql.replace("{table}", &self.table);
        self.sessions
            .session(async |session| {
                Ok::<_, Error>(session.connection().query_one(&sql, &[]).await?.get(0))
            })
            .await
            .unwrap()
    }

    async fn rows(&self) -> i64 {
        self.count("SELECT count(*) FROM {table}").await
    }

    async fn set_aside(&self, reason: &str) -> i64 {
        self.count(&format!(
            "SELECT count(*) FROM {{table}}_rejects WHERE reason = '{reason}'"
        ))
        .await
    }

    async fn intake(&self, message: &Message) -> Admission {
        self.inbox
            .intake("inbox://orders/k", message)
            .await
            .unwrap()
    }
}

/// A message of a full identity on `orders`, each header as given, one
/// replaced by `name` and `value` when named.
fn order(position: &str, message_id: &str, payload: &str) -> Message {
    Message::new(payload.to_owned())
        .with_header(TENANT_ID, "t1")
        .with_header(STREAM_TYPE, "orders.Order")
        .with_header(STREAM_ID, "o-1")
        .with_header(STREAM_POSITION, position)
        .with_header("message_id", message_id)
}

const ID_1: &str = "0190a01a-7e5b-7d80-a000-000000000001";
const ID_2: &str = "0190a01a-7e5b-7d80-a000-000000000002";

fn reason_of(admission: &Admission) -> Option<Reason> {
    match admission {
        Admission::SetAside { reason, .. } => Some(*reason),
        _ => None,
    }
}

/// A message that can never be stored is set aside, its bytes kept, and the
/// one after it is stored: the source is never held by it.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_message_that_can_never_be_stored_is_set_aside_and_the_next_is_stored() {
    let f = fixture("poison").await;
    let poisons = [
        Message::new("p").with_header(STREAM_TYPE, "orders.Order"),
        order("not-a-position", ID_1, "p"),
        order("1", "not-a-uuid", "p"),
        order("1", ID_1, "p").with_header("traceparent", vec![0xff_u8, 0xfe]),
        Message::new("p")
            .with_header(TENANT_ID, "t".repeat(300))
            .with_header(STREAM_TYPE, "orders.Order")
            .with_header(STREAM_ID, "o-1")
            .with_header(STREAM_POSITION, "1"),
        Message::new("p")
            .with_header(TENANT_ID, "t1")
            .with_header(STREAM_TYPE, "orders.Order")
            .with_header(STREAM_ID, "o\0")
            .with_header(STREAM_POSITION, "1"),
        Message::new("p")
            .with_header(TENANT_ID, "t\0")
            .with_header(STREAM_TYPE, "orders.Order")
            .with_header(STREAM_ID, "o-1")
            .with_header(STREAM_POSITION, "1"),
    ];
    for poison in &poisons {
        assert_eq!(reason_of(&f.intake(poison).await), Some(Reason::Malformed));
    }
    assert!(matches!(
        f.intake(&order("1", ID_1, "good")).await,
        Admission::Stored(_)
    ));
    assert_eq!(f.rows().await, 1);
    assert_eq!(f.set_aside("malformed").await, poisons.len() as i64);
    let seen = f.seen.0.lock().unwrap();
    assert_eq!(seen.len(), poisons.len(), "the observer told once each");
    assert!(seen.iter().all(|(channel, reason, _, repeat)| {
        channel == "inbox://orders" && *reason == Reason::Malformed && !repeat
    }));
}

/// The same bytes again are one row set aside, counted twice.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_repeat_of_the_same_bytes_is_one_row_set_aside_counted_twice() {
    let f = fixture("repeat").await;
    let poison = order("x", ID_1, "p");
    let first = f.intake(&poison).await;
    let second = f.intake(&poison).await;
    let digest = |admission: &Admission| match admission {
        Admission::SetAside { digest, repeat, .. } => (digest.clone(), *repeat),
        other => panic!("{other:?}"),
    };
    assert_eq!(digest(&first).0, digest(&second).0);
    assert_eq!((digest(&first).1, digest(&second).1), (false, true));
    assert_eq!(
        f.count("SELECT seen::bigint FROM {table}_rejects").await,
        2,
        "seen twice"
    );
}

/// A redelivery of the same bytes is a duplicate; the same identity and
/// `message_id` with another payload is a re-emission, set aside as
/// differing, never read as a redelivery (ADR-0036 R1).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_redelivery_is_a_duplicate_and_another_payload_under_its_id_differs() {
    let f = fixture("differs").await;
    assert!(matches!(
        f.intake(&order("1", ID_1, "first")).await,
        Admission::Stored(_)
    ));
    assert_eq!(
        f.intake(&order("1", ID_1, "first")).await,
        Admission::Duplicate
    );
    assert_eq!(
        reason_of(&f.intake(&order("1", ID_1, "corrected")).await),
        Some(Reason::Differs)
    );
    assert_eq!(f.rows().await, 1);
    assert_eq!(f.set_aside("differs").await, 1);
}

/// A second message under one identity with another `message_id` is a
/// shadow, acknowledged as ADR-0030 has it; nothing is set aside.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn another_message_id_under_one_identity_is_a_shadow() {
    let f = fixture("shadow").await;
    f.intake(&order("1", ID_1, "first")).await;
    assert_eq!(
        f.intake(&order("1", ID_2, "second")).await,
        Admission::Duplicate
    );
    assert_eq!(f.count("SELECT count(*) FROM {table}_rejects").await, 0);
}

/// A `message_id` held at another identity of its Tenant and stream type is
/// set aside as taken, with its holder's channel and position; under another
/// stream type it is no conflict (ADR-0036 R5).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_message_id_held_in_its_stream_type_is_taken_and_free_in_another() {
    let f = fixture("taken").await;
    f.intake(&order("1", ID_1, "first")).await;
    assert_eq!(
        reason_of(&f.intake(&order("2", ID_1, "re-stamped")).await),
        Some(Reason::IdentityTaken)
    );
    assert_eq!(
        f.count(
            "SELECT count(*) FROM {table}_rejects \
             WHERE holder_channel = 'inbox://orders' AND holder_position IS NOT NULL"
        )
        .await,
        1
    );
    let other_type = Message::new("other")
        .with_header(TENANT_ID, "t1")
        .with_header(STREAM_TYPE, "orders.Invoice")
        .with_header(STREAM_ID, "i-1")
        .with_header(STREAM_POSITION, "1")
        .with_header("message_id", ID_1);
    assert!(matches!(f.intake(&other_type).await, Admission::Stored(_)));
    assert_eq!(f.rows().await, 2);
}

/// A stream type whose owner does not produce the channel is set aside as
/// foreign (ADR-0036 R5).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_stream_type_its_owner_does_not_produce_here_is_foreign() {
    let f = fixture("foreign").await;
    let claimed = Message::new("claim")
        .with_header(TENANT_ID, "t1")
        .with_header(STREAM_TYPE, "tenancy.Tenant")
        .with_header(STREAM_ID, "t1")
        .with_header(STREAM_POSITION, "7")
        .with_header("message_id", ID_2);
    assert_eq!(reason_of(&f.intake(&claimed).await), Some(Reason::Foreign));
    assert_eq!(f.rows().await, 0);
}

/// The intake through the bus: the inbox channel's producer acknowledges a
/// message it sets aside, and refuses a channel too long for its rows.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_channel_s_producer_acknowledges_what_it_sets_aside() {
    let f = fixture("producer").await;
    let channel = f.inbox.channel();
    let producer = channel.producer("inbox://orders").unwrap();
    producer.publish(order("x", ID_1, "p")).await.unwrap();
    assert_eq!(f.set_aside("malformed").await, 1);
    assert!(
        channel
            .producer(&format!("inbox://{}", "c".repeat(250)))
            .is_err()
    );
}

/// A table the intake cannot read the conflicts of is refused at start.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_table_with_an_index_the_inbox_does_not_own_is_refused() {
    let f = fixture("foreign_index").await;
    f.sessions
        .session(async |session| {
            session
                .connection()
                .batch_execute(&format!(
                    "CREATE UNIQUE INDEX {t}_host_idx ON {t} (uri, stream_position)",
                    t = f.table
                ))
                .await?;
            Ok::<_, Error>(())
        })
        .await
        .unwrap();
    let refused = f.inbox.migrate().await.unwrap_err();
    assert!(refused.to_string().contains("does not own"), "{refused}");
}
