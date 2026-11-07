//! The operator's surface of the rows set aside (ADR-0036 decision 4, R7):
//! counts, a page and the kept bytes, readmit and discard, every call with
//! its principal. They need a live database:
//!
//! ```text
//! ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
//!     cargo test -p ascetic-ddd-inbox --test surface -- --ignored
//! ```

use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ascetic_ddd_bus::Message;
use ascetic_ddd_bus::identity::{STREAM_ID, STREAM_POSITION, STREAM_TYPE, TENANT_ID};
use ascetic_ddd_inbox::observer::{InboxObserver, Operated, Operation};
use ascetic_ddd_inbox::{
    Admission, ByStream, Error, InboxRetention, PgInbox, Readmission, Readmitted, Reason,
    SetAsidePage,
};
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

/// Every call of the surface, as the observer was told it.
#[derive(Default)]
struct Audit(Mutex<Vec<(String, Operation, u64)>>);

impl InboxObserver for Audit {
    fn on_operated(&self, event: &Operated<'_>) {
        self.0
            .lock()
            .unwrap()
            .push((event.principal.to_owned(), event.operation, event.rows));
    }
}

type Inbox = PgInbox<PgSessionPool, Arc<Audit>>;

/// An inbox on `table`: `orders.*` admitted on `inbox://orders` unless the
/// consumer's fix admits everything, the channel readmitting.
fn inbox(table: &str, audit: &Arc<Audit>, fixed: bool) -> Inbox {
    PgInbox::new(PgSessionPool::new(pool()))
        .with_table(
            Identifier::new(table).unwrap(),
            Identifier::new(format!("{table}_seq")).unwrap(),
        )
        .partitioned_by(ByStream)
        .observed_by(Arc::clone(audit))
        .admitting(move |channel, stream_type| {
            fixed || channel != "inbox://orders" || stream_type.starts_with("orders.")
        })
        .readmitting(|channel| channel == "inbox://orders")
}

struct Fixture {
    inbox: Inbox,
    audit: Arc<Audit>,
    table: String,
    sessions: PgSessionPool,
}

async fn fixture(name: &str) -> Fixture {
    let table = format!("surface_{name}");
    let audit = Arc::new(Audit::default());
    let inbox = inbox(&table, &audit, false);
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    Fixture {
        inbox,
        audit,
        table,
        sessions: PgSessionPool::new(pool()),
    }
}

impl Fixture {
    async fn one(&self, sql: &str) -> i64 {
        let sql = sql.replace("{table}", &self.table);
        self.sessions
            .session(async |session| {
                Ok::<_, Error>(session.connection().query_one(&sql, &[]).await?.get(0))
            })
            .await
            .unwrap()
    }

    async fn intake(&self, message: &Message) -> Admission {
        self.inbox
            .intake("inbox://orders/k", message)
            .await
            .unwrap()
    }

    fn operations(&self) -> Vec<(String, Operation, u64)> {
        self.audit.0.lock().unwrap().clone()
    }
}

fn order(stream_type: &str, position: &str, message_id: &str) -> Message {
    Message::new("payload, never paged")
        .with_header(TENANT_ID, "t1")
        .with_header(STREAM_TYPE, stream_type)
        .with_header(STREAM_ID, "o-1")
        .with_header(STREAM_POSITION, position)
        .with_header("message_id", message_id)
        .with_header("traceparent", format!("trace-{position}"))
        .with_header("dek", "wrapped key")
}

fn digest_of(admission: &Admission) -> String {
    match admission {
        Admission::SetAside { digest, .. } => digest.clone(),
        other => panic!("{other:?}"),
    }
}

const ID: &str = "0190a01a-7e5b-7d80-a000-00000000aa01";

/// The open rows are counted by channel and reason, and paged oldest first
/// with a cursor, with the metadata keys asked for and never the payload or
/// the data key's fields; each call is told with its principal.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_rows_set_aside_are_counted_and_paged_without_their_payload() {
    let f = fixture("paged").await;
    for position in ["x", "y"] {
        f.intake(&order("orders.Order", position, ID)).await;
    }
    f.intake(&order("tenancy.Tenant", "1", ID)).await;

    let counts = f.inbox.set_aside_counts("ops").await.unwrap();
    let mut counted: Vec<_> = counts.iter().map(|c| (c.reason, c.open)).collect();
    counted.sort_by_key(|(reason, _)| reason.as_str());
    assert_eq!(counted, [(Reason::Foreign, 1), (Reason::Malformed, 2)]);

    let mut page = SetAsidePage {
        channel: "inbox://orders",
        reason: Some(Reason::Malformed),
        limit: 1,
        metadata: &["traceparent", "absent"],
        ..SetAsidePage::default()
    };
    let first = f.inbox.set_aside_page("ops", &page).await.unwrap();
    page.after = Some(first[0].cursor());
    let second = f.inbox.set_aside_page("ops", &page).await.unwrap();
    page.after = Some(second[0].cursor());
    let none = f.inbox.set_aside_page("ops", &page).await.unwrap();
    assert_eq!((first.len(), second.len(), none.len()), (1, 1, 0));
    assert_eq!(
        first[0].metadata,
        [
            ("traceparent".to_owned(), Some("trace-x".to_owned())),
            ("absent".to_owned(), None)
        ]
    );
    assert_eq!(second[0].metadata[0].1.as_deref(), Some("trace-y"));
    assert_eq!(first[0].tenant_id.as_deref(), Some("t1"));

    let sealed = SetAsidePage {
        metadata: &["dek"],
        ..page
    };
    assert!(matches!(
        f.inbox.set_aside_page("ops", &sealed).await,
        Err(Error::Refused(_))
    ));
    assert_eq!(
        f.operations(),
        [
            ("ops".to_owned(), Operation::Counted, 2),
            ("ops".to_owned(), Operation::Paged, 1),
            ("ops".to_owned(), Operation::Paged, 1),
            ("ops".to_owned(), Operation::Paged, 0),
        ]
    );
}

/// The kept bytes read back as the very message set aside.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_kept_bytes_are_the_message_set_aside() {
    let f = fixture("bytes").await;
    let poison = order("orders.Order", "x", ID);
    let digest = digest_of(&f.intake(&poison).await);
    let bytes = f
        .inbox
        .set_aside_message("forensics", &digest)
        .await
        .unwrap()
        .unwrap();
    assert!(
        bytes
            .windows(b"payload, never paged".len())
            .any(|w| w == b"payload, never paged"),
        "the frame holds the payload"
    );
    assert_eq!(
        f.operations(),
        [("forensics".to_owned(), Operation::Read, 1)]
    );
}

/// A readmit after a fix on the consuming side takes the message and closes
/// the row, its bytes gone; bytes that are malformed stay set aside, seen
/// once more; a channel that does not readmit refuses.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_readmit_after_a_fix_takes_the_message_and_closes_the_row() {
    let f = fixture("readmit").await;
    let foreign = digest_of(&f.intake(&order("billing.Invoice", "1", ID)).await);
    let malformed = digest_of(&f.intake(&order("orders.Order", "x", ID)).await);

    // no channel readmits unless told
    let untold = PgInbox::new(PgSessionPool::new(pool())).with_table(
        Identifier::new(&f.table).unwrap(),
        Identifier::new(format!("{}_seq", f.table)).unwrap(),
    );
    assert!(matches!(
        untold.readmit("ops", &foreign).await,
        Err(Error::Refused(_))
    ));

    // the same table, behind a consumer whose admission was fixed
    let fixed = inbox(&f.table, &f.audit, true);
    assert!(matches!(
        fixed.readmit("ops", &foreign).await.unwrap(),
        Some(Readmission::Closed(Admission::Stored(_)))
    ));
    assert_eq!(
        fixed.readmit("ops", &foreign).await.unwrap(),
        None,
        "closed"
    );
    assert_eq!(
        f.one("SELECT count(*) FROM {table}_rejects WHERE disposition = 'readmitted' AND message IS NULL")
            .await,
        1
    );
    assert_eq!(
        fixed.readmit("ops", &malformed).await.unwrap(),
        Some(Readmission::StillSetAside)
    );
    assert_eq!(
        f.one("SELECT seen::bigint FROM {table}_rejects WHERE disposed_at IS NULL")
            .await,
        2
    );

    let elsewhere = digest_of(
        &f.inbox
            .intake("inbox://invoices/k", &order("orders.Order", "x", ID))
            .await
            .unwrap(),
    );
    assert!(matches!(
        f.inbox.readmit("ops", &elsewhere).await,
        Err(Error::Refused(_))
    ));
}

/// A flood readmitted in bulk, oldest first, after the fix.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_flood_is_readmitted_in_bulk_after_the_fix() {
    let f = fixture("flood").await;
    for (position, id) in [
        ("1", "0190a01a-7e5b-7d80-a000-00000000bb01"),
        ("2", "0190a01a-7e5b-7d80-a000-00000000bb02"),
        ("3", "0190a01a-7e5b-7d80-a000-00000000bb03"),
    ] {
        f.intake(&order("billing.Invoice", position, id)).await;
    }
    let fixed = inbox(&f.table, &f.audit, true);
    assert_eq!(
        fixed
            .readmit_all(
                "ops",
                "inbox://orders",
                Reason::Foreign,
                Duration::from_secs(3600)
            )
            .await
            .unwrap(),
        Readmitted {
            closed: 3,
            still_set_aside: 0
        }
    );
    assert_eq!(f.one("SELECT count(*) FROM {table}").await, 3);
}

/// A discard closes the row as a tombstone; a repeat of the same bytes
/// stays discarded, and is counted.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_discarded_row_stays_discarded_when_its_bytes_come_again() {
    let f = fixture("discard").await;
    let poison = order("orders.Order", "x", ID);
    let digest = digest_of(&f.intake(&poison).await);
    assert!(
        f.inbox
            .discard("ops", &digest, "a producer's test")
            .await
            .unwrap()
    );
    assert!(!f.inbox.discard("ops", &digest, "again").await.unwrap());
    f.intake(&poison).await;
    assert_eq!(
        f.one(
            "SELECT count(*) FROM {table}_rejects WHERE disposition = 'discarded' \
             AND disposed_because = 'a producer''s test' AND seen = 2 AND message IS NULL"
        )
        .await,
        1
    );
    assert!(f.inbox.set_aside_counts("ops").await.unwrap().is_empty());
}

/// Every row set aside has a bounded life (ADR-0036 R6): an open row past
/// its channel's window is closed as expired, its bytes gone, a tombstone,
/// and forgotten a window after; a row without a Tenant lives the host's
/// bound whatever its channel; a row with one, on a channel without a
/// window, is kept.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_row_set_aside_lives_its_window_then_a_tombstone_then_nothing() {
    let f = fixture("retention").await;
    let windowed = digest_of(&f.intake(&order("orders.Order", "x", ID)).await);
    let untenanted = digest_of(
        &f.inbox
            .intake(
                "inbox://invoices/k",
                &Message::new("p").with_header(STREAM_TYPE, "orders.Order"),
            )
            .await
            .unwrap(),
    );
    let kept = digest_of(
        &f.inbox
            .intake("inbox://invoices/k", &order("orders.Order", "x", ID))
            .await
            .unwrap(),
    );
    let retained = inbox(&f.table, &f.audit, false).with_retention(
        InboxRetention::keep_everything()
            .window("inbox://orders", Duration::from_secs(60))
            .untenanted(Duration::from_secs(60)),
    );
    let state = async |digest: &str| -> (String, bool) {
        let sql = format!(
            "SELECT coalesce(disposition, 'open'), message IS NOT NULL FROM {}_rejects \
             WHERE digest = '{digest}'",
            f.table
        );
        f.sessions
            .session(async |session| {
                Ok::<_, Error>(
                    session
                        .connection()
                        .query_opt(&sql, &[])
                        .await?
                        .map_or(("gone".to_owned(), false), |row| (row.get(0), row.get(1))),
                )
            })
            .await
            .unwrap()
    };

    retained.sweep().await.unwrap();
    assert_eq!(
        state(&windowed).await,
        ("open".to_owned(), true),
        "within its window"
    );

    f.one("WITH aged AS (UPDATE {table}_rejects SET first_seen_at = first_seen_at - interval '2 minutes' RETURNING 1) SELECT count(*) FROM aged")
        .await;
    retained.sweep().await.unwrap();
    assert_eq!(
        state(&windowed).await,
        ("expired".to_owned(), false),
        "a tombstone"
    );
    assert_eq!(
        state(&untenanted).await,
        ("expired".to_owned(), false),
        "the host's bound"
    );
    assert_eq!(
        state(&kept).await,
        ("open".to_owned(), true),
        "no window, a Tenant"
    );

    f.one("WITH aged AS (UPDATE {table}_rejects SET disposed_at = disposed_at - interval '2 minutes' WHERE disposed_at IS NOT NULL RETURNING 1) SELECT count(*) FROM aged")
        .await;
    retained.sweep().await.unwrap();
    assert_eq!(state(&windowed).await.0, "gone");
    assert_eq!(state(&untenanted).await.0, "gone");
    assert_eq!(state(&kept).await.0, "open");
}
