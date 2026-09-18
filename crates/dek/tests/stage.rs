//! The envelope stage on a PostgreSQL KMS: a message sealed on the way out
//! opens on the way in, only for its tenant; and end to end, the outbox and
//! the inbox hold nothing but ciphertext while the two ends see the clear.
//! Needs a live database:
//!
//! ```text
//! ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
//!     cargo test -p ascetic-ddd-dek --all-features -- --ignored
//! ```

#![cfg(all(feature = "bus", feature = "pg"))]

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use ascetic_ddd_bus::{Bridge, Bus, Message, Permanent, Stage, Target};
use ascetic_ddd_dek::EnvelopeStage;
use ascetic_ddd_dek::stage::{DEK, DEK_ALGORITHM, DEK_BOUND_TO, MESSAGE_ID, TENANT_ID};
use ascetic_ddd_inbox::{INBOX_SCHEME, Loops as InboxLoops, PgInbox};
use ascetic_ddd_kms::{Algorithm, KeyManagementService, PgKeyManagementService};
use ascetic_ddd_outbox::{Loops as OutboxLoops, OUTBOX_SCHEME, PgOutbox};
use ascetic_ddd_session::pg::Identifier;
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};
use ascetic_ddd_session::{PgAccess, PgSessionPool, Session, SessionError, SessionPool};
use tokio::sync::mpsc;

const DEFAULT_URL: &str = "postgresql://devel:devel@localhost:5432/devel_karmabot_test";

fn pool_at(url: &str) -> Pool {
    let manager = Manager::from_config(
        Config::from_str(url).expect("a valid PostgreSQL URL"),
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

fn pool() -> Pool {
    pool_at(&std::env::var("ASCETIC_DDD_TEST_PG_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned()))
}

type Sealing = EnvelopeStage<PgSessionPool, PgKeyManagementService>;

/// A KMS in a table of its own per test, and the stage over it.
async fn stage(name: &str) -> Arc<Sealing> {
    let table = format!("kms_keys_stage_{name}");
    let kms = PgKeyManagementService::new(Algorithm::Aes256Gcm.generate_key().unwrap())
        .with_table(Identifier::new(&table).unwrap());
    let sessions = PgSessionPool::new(pool());
    sessions
        .session(async |session| {
            session
                .connection()
                .batch_execute(&format!("DROP TABLE IF EXISTS {table}"))
                .await
                .unwrap();
            kms.setup(&session).await
        })
        .await
        .unwrap();
    Arc::new(EnvelopeStage::new(sessions, kms))
}

fn message(tenant_id: &str, payload: &str) -> Message {
    identified(tenant_id, "00000000-0000-4000-8000-000000000001", payload)
}

fn identified(tenant_id: &str, message_id: &str, payload: &str) -> Message {
    Message::new(payload.as_bytes())
        .with_header(TENANT_ID, tenant_id)
        .with_header(MESSAGE_ID, message_id)
}

/// The sealed payload and its key headers of `from`, under the identity of
/// `to`: what a substitution in a store or a broker looks like.
fn moved(from: &Message, to: Message) -> Message {
    let mut moved = to.with_payload(from.payload().to_vec());
    for name in [DEK, DEK_ALGORITHM, DEK_BOUND_TO] {
        moved = moved.with_header(name, from.header(name).unwrap().to_vec());
    }
    moved
}

fn text(bytes: &[u8]) -> &str {
    std::str::from_utf8(bytes).unwrap()
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_message_is_sealed_on_the_way_out_and_opened_on_the_way_in() {
    let sealing = stage("round_trip").await;
    let sealed = sealing
        .outbound(message("tenant-1", "the order's events"))
        .await
        .unwrap();
    assert_ne!(sealed.payload(), b"the order's events");
    assert!(
        !sealed.payload().windows(5).any(|w| w == b"order"),
        "no plaintext on the wire"
    );
    assert_eq!(text(sealed.header(DEK_ALGORITHM).unwrap()), "AES-256-GCM");
    assert_eq!(
        text(sealed.header(DEK_BOUND_TO).unwrap()),
        "tenant_id,message_id"
    );
    assert!(sealed.header(DEK).is_some());
    assert_eq!(text(sealed.header(TENANT_ID).unwrap()), "tenant-1");

    let opened = sealing.inbound(sealed.clone()).await.unwrap();
    assert_eq!(opened.payload(), b"the order's events");
    for name in [DEK, DEK_ALGORITHM, DEK_BOUND_TO] {
        assert!(opened.header(name).is_none(), "{name} is gone");
    }
    assert_eq!(text(opened.header(TENANT_ID).unwrap()), "tenant-1");
    assert!(opened.header(MESSAGE_ID).is_some());

    // Two messages of one tenant get two keys.
    let again = sealing
        .outbound(message("tenant-1", "the order's events"))
        .await
        .unwrap();
    assert_ne!(again.header(DEK), sealed.header(DEK));
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn another_tenant_does_not_open_it() {
    let sealing = stage("other_tenant").await;
    let sealed = sealing
        .outbound(message("tenant-1", "secret"))
        .await
        .unwrap();
    // The other tenant has keys of its own.
    sealing
        .outbound(message("tenant-2", "its own"))
        .await
        .unwrap();
    let relabelled = sealed
        .without_header(TENANT_ID)
        .with_header(TENANT_ID, "tenant-2");
    let error = sealing.inbound(relabelled).await.unwrap_err();
    assert!(error.is::<Permanent>(), "{error}");
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_payload_moved_under_another_message_does_not_open() {
    let sealing = stage("moved").await;
    let a = sealing
        .outbound(identified(
            "tenant-1",
            "00000000-0000-4000-8000-00000000000a",
            "shipped",
        ))
        .await
        .unwrap();
    let b = identified(
        "tenant-1",
        "00000000-0000-4000-8000-00000000000b",
        "cancelled",
    );
    let error = sealing.inbound(moved(&a, b)).await.unwrap_err();
    assert!(error.is::<Permanent>(), "{error}");
    // The same message again, as at-least-once delivery brings it, opens.
    assert_eq!(
        sealing.inbound(a.clone()).await.unwrap().payload(),
        b"shipped"
    );
    assert_eq!(sealing.inbound(a).await.unwrap().payload(), b"shipped");
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn the_binding_is_the_sealing_side_s_and_travels_with_the_message() {
    // Two stages over one KMS: one bound to the tenant and the id, one to
    // the tenant alone.
    let master_key = Algorithm::Aes256Gcm.generate_key().unwrap();
    let kms = || {
        PgKeyManagementService::new(master_key.clone())
            .with_table(Identifier::new("kms_keys_stage_binding").unwrap())
    };
    PgSessionPool::new(pool())
        .session(async |session| {
            session
                .connection()
                .batch_execute("DROP TABLE IF EXISTS kms_keys_stage_binding")
                .await
                .unwrap();
            kms().setup(&session).await
        })
        .await
        .unwrap();
    let by_identity = EnvelopeStage::new(PgSessionPool::new(pool()), kms());
    let by_tenant = EnvelopeStage::new(PgSessionPool::new(pool()), kms()).bound_to([TENANT_ID]);

    // A message with no id is refused under the default binding …
    let unidentified = Message::new(b"secret".to_vec()).with_header(TENANT_ID, "tenant-1");
    let error = by_identity
        .outbound(unidentified.clone())
        .await
        .unwrap_err();
    assert!(error.is::<Permanent>(), "{error}");
    // … and sealed by the stage bound to the tenant alone, which the other
    // opens, reading the binding from the message.
    let sealed = by_tenant.outbound(unidentified).await.unwrap();
    assert_eq!(text(sealed.header(DEK_BOUND_TO).unwrap()), "tenant_id");
    assert_eq!(
        by_identity.inbound(sealed).await.unwrap().payload(),
        b"secret"
    );
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_message_without_a_tenant_is_refused_for_good() {
    let sealing = stage("no_tenant").await;
    let error = sealing
        .outbound(Message::new(b"secret".to_vec()))
        .await
        .unwrap_err();
    assert!(error.is::<Permanent>(), "{error}");
    let error = sealing
        .inbound(Message::new(b"secret".to_vec()))
        .await
        .unwrap_err();
    assert!(error.is::<Permanent>(), "{error}");
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_shredded_tenant_s_messages_are_refused_for_good() {
    let sealing = stage("shredded").await;
    let sealed = sealing
        .outbound(message("tenant-1", "secret"))
        .await
        .unwrap();
    let kms = PgKeyManagementService::new(Algorithm::Aes256Gcm.generate_key().unwrap())
        .with_table(Identifier::new("kms_keys_stage_shredded").unwrap());
    PgSessionPool::new(pool())
        .session(async |session| kms.delete_kek(&session, "tenant-1").await)
        .await
        .unwrap();
    let error = sealing.inbound(sealed).await.unwrap_err();
    assert!(error.is::<Permanent>(), "{error}");
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_kms_out_of_reach_is_a_failure_of_the_moment() {
    let kms = PgKeyManagementService::new(Algorithm::Aes256Gcm.generate_key().unwrap());
    let unreachable =
        PgSessionPool::new(pool_at("postgresql://nobody:nothing@127.0.0.1:1/nowhere"));
    let sealing = EnvelopeStage::new(unreachable, kms);
    let error = sealing
        .outbound(message("tenant-1", "secret"))
        .await
        .unwrap_err();
    assert!(!error.is::<Permanent>(), "{error}");
}

/// The whole path of ADR-0002: the command's transaction seals into the
/// outbox, a bridge carries the bytes into the inbox, and the inbox's
/// consumer opens them for the handler. Both tables hold ciphertext; both
/// ends see the clear.
#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn the_outbox_and_the_inbox_hold_nothing_but_ciphertext() {
    let sealing = stage("end_to_end").await;
    let sessions = PgSessionPool::new(pool());
    let outbox = Arc::new(
        PgOutbox::new(PgSessionPool::new(pool()))
            .with_tables(
                Identifier::new("dek_stage_outbox").unwrap(),
                Identifier::new("dek_stage_outbox_offsets").unwrap(),
            )
            .with_loops(OutboxLoops {
                poll_interval: Duration::from_millis(20),
                ..OutboxLoops::default()
            }),
    );
    let inbox = Arc::new(
        PgInbox::new(PgSessionPool::new(pool()))
            .with_table(
                Identifier::new("dek_stage_inbox").unwrap(),
                Identifier::new("dek_stage_inbox_seq").unwrap(),
            )
            .with_loops(InboxLoops {
                poll_interval: Duration::from_millis(20),
                ..InboxLoops::default()
            }),
    );
    sessions
        .session(async |session| {
            session
                .connection()
                .batch_execute(
                    "DROP TABLE IF EXISTS dek_stage_outbox; DROP TABLE IF EXISTS dek_stage_outbox_meta; \
                     DROP TABLE IF EXISTS dek_stage_outbox_offsets; DROP TABLE IF EXISTS dek_stage_inbox; \
                     DROP SEQUENCE IF EXISTS dek_stage_inbox_seq;",
                )
                .await
                .unwrap();
            outbox.setup(&session).await.unwrap();
            inbox.setup(&session).await.unwrap();
            Ok::<_, SessionError>(())
        })
        .await
        .unwrap();

    let mut bus = Bus::new();
    bus.register(OUTBOX_SCHEME, outbox.channel()).unwrap();
    bus.register(INBOX_SCHEME, inbox.channel()).unwrap();
    let bus = Arc::new(bus);
    let dispatcher = Bridge::new(Arc::clone(&bus))
        .run(
            "outbox://all",
            "dispatcher",
            Target::Header("destination".into()),
        )
        .unwrap();

    // The consuming end opens what it receives.
    let (received, mut inbound) = mpsc::unbounded_channel();
    let processing = inbox
        .consumer(|message: &Message| Ok(text(message.payload()).to_owned()))
        .through(Arc::clone(&sealing) as Arc<dyn Stage>)
        .subscribe(move |_tx, payload: String| {
            let received = received.clone();
            async move {
                received.send(payload).ok();
            }
        })
        .unwrap();

    // The producing end seals inside the command's transaction.
    let placed = outbox
        .producer("inbox://orders/order-7", |payload: &String| {
            Message::new(payload.as_bytes())
                .with_header(TENANT_ID, "tenant-1")
                .with_header("stream_type", "Order")
                .with_header("stream_id", "\"order-7\"")
                .with_header("stream_position", "1")
                .with_header(MESSAGE_ID, "00000000-0000-4000-8000-000000000007")
        })
        .through(Arc::clone(&sealing) as Arc<dyn Stage>);
    sessions
        .session(async |session| {
            session
                .atomic(async |tx| {
                    placed
                        .publish(&tx, &"order placed in the clear".to_owned())
                        .await
                        .unwrap();
                    Ok::<_, SessionError>(())
                })
                .await
        })
        .await
        .unwrap();

    let delivered = tokio::time::timeout(Duration::from_secs(5), inbound.recv())
        .await
        .expect("delivered in time")
        .unwrap();
    assert_eq!(delivered, "order placed in the clear");

    // Neither table holds the clear; both hold the wrapped key beside it.
    let at_rest: Vec<(Vec<u8>, serde_json::Value)> = sessions
        .session(async |session| {
            let rows = session
                .connection()
                .query(
                    "SELECT payload, metadata FROM dek_stage_outbox \
                     UNION ALL SELECT payload, metadata FROM dek_stage_inbox",
                    &[],
                )
                .await
                .unwrap();
            Ok::<_, SessionError>(rows.iter().map(|row| (row.get(0), row.get(1))).collect())
        })
        .await
        .unwrap();
    assert_eq!(at_rest.len(), 2, "one row in the outbox, one in the inbox");
    for (payload, metadata) in &at_rest {
        assert!(
            !payload.windows(5).any(|w| w == b"clear"),
            "ciphertext at rest"
        );
        assert!(
            metadata.get(DEK).and_then(|v| v.as_str()).is_some(),
            "{metadata}"
        );
        assert_eq!(metadata[DEK_ALGORITHM], "AES-256-GCM");
    }
    dispatcher.cancel();
    processing.cancel();
}
