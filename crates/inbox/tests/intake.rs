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
use ascetic_ddd_inbox::{
    Admission, Bounds, ByStream, Error, Failure, InboxMessage, Outcome, PartitionKey, PgInbox,
    Reason,
};
use ascetic_ddd_session::pg::Identifier;
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::error::SqlState;
use ascetic_ddd_session::pg::tokio_postgres::{Client, Config, NoTls};
use ascetic_ddd_session::{PgAccess, PgSession, PgSessionPool, SessionPool};

const DEFAULT_URL: &str = "postgresql://devel:devel@localhost:5432/devel_karmabot_test";

fn config() -> Config {
    let url = std::env::var("ASCETIC_DDD_TEST_PG_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned());
    Config::from_str(&url).expect("a valid PostgreSQL URL")
}

fn pool() -> Pool {
    pool_of(config())
}

/// A pool whose statements give up on a lock after a fifth of a second, as
/// a host's `lock_timeout` has them do.
fn impatient_pool() -> Pool {
    let mut config = config();
    config.options("-c lock_timeout=200ms");
    pool_of(config)
}

fn pool_of(config: Config) -> Pool {
    let manager = Manager::from_config(
        config,
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
    fixture_on(name, pool()).await
}

/// The same, its inbox on the pool given.
async fn fixture_on(name: &str, on: Pool) -> Fixture {
    let table = format!("intake_{name}");
    let seen = Arc::new(Seen::default());
    let inbox = PgInbox::new(PgSessionPool::new(on))
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

/// A second inbox on the fixture's tables, on the pool given: no teardown,
/// so that its `migrate` finds the ledger current and only checks.
fn beside(f: &Fixture, on: Pool) -> PgInbox<PgSessionPool> {
    PgInbox::new(PgSessionPool::new(on))
        .with_table(
            Identifier::new(&f.table).unwrap(),
            Identifier::new(format!("{}_seq", f.table)).unwrap(),
        )
        .partitioned_by(ByStream)
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

    async fn count_text(&self, sql: &str) -> String {
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

/// A transaction of its own that takes `lock` and keeps it until rolled
/// back.
async fn holding(lock: &str) -> Client {
    let (client, connection) = config().connect(NoTls).await.unwrap();
    tokio::spawn(connection);
    client
        .batch_execute(&format!("BEGIN; {lock}"))
        .await
        .unwrap();
    client
}

/// The SQLSTATE of a refusal from the database, if it is one.
fn code_of(error: &Error) -> Option<&str> {
    match error {
        Error::Database(error) => error.code().map(SqlState::code),
        _ => None,
    }
}

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
        Message::new("p")
            .with_header(TENANT_ID, vec![b't', 0xff])
            .with_header(STREAM_TYPE, "orders.Order")
            .with_header(STREAM_ID, "o-1")
            .with_header(STREAM_POSITION, "1"),
        Message::new("p".repeat(1 << 20))
            .with_header(TENANT_ID, "t1")
            .with_header(STREAM_TYPE, "orders.Order")
            .with_header(STREAM_ID, "o-1")
            .with_header(STREAM_POSITION, "x"),
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
    assert!(
        f.count("SELECT max(octet_length(message))::bigint FROM {table}_rejects")
            .await
            > 1 << 20,
        "a megabyte's bytes kept whole"
    );
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

/// A lock held on the inbox's table past `lock_timeout` is the environment:
/// the message is held, an error to its source, and nothing is set aside.
/// Once the lock is gone the same message is stored.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_lock_on_the_inbox_holds_the_message_and_sets_nothing_aside() {
    let f = fixture_on("locked", impatient_pool()).await;
    let holder = holding(&format!("LOCK TABLE {} IN SHARE MODE", f.table)).await;
    let message = order("1", ID_1, "p");
    let held = f.inbox.intake("inbox://orders/k", &message).await;
    assert_eq!(
        held.as_ref().err().and_then(code_of),
        Some(SqlState::LOCK_NOT_AVAILABLE.code()),
        "{held:?}"
    );
    holder.batch_execute("ROLLBACK").await.unwrap();
    assert!(matches!(f.intake(&message).await, Admission::Stored(_)));
    assert_eq!(f.rows().await, 1);
    assert_eq!(f.count("SELECT count(*) FROM {table}_rejects").await, 0);
    assert!(f.seen.0.lock().unwrap().is_empty());
}

/// A lock held on the rows set aside holds a message that can never be
/// stored as well: an error to its source, nothing recorded and the
/// observer told nothing. Once the lock is gone it is set aside, once.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_lock_on_the_rows_set_aside_holds_a_poison_then_sets_it_aside_once() {
    let f = fixture_on("rejects_locked", impatient_pool()).await;
    let holder = holding(&format!("LOCK TABLE {}_rejects IN SHARE MODE", f.table)).await;
    let poison = order("x", ID_1, "p");
    let held = f.inbox.intake("inbox://orders/k", &poison).await;
    assert_eq!(
        held.as_ref().err().and_then(code_of),
        Some(SqlState::LOCK_NOT_AVAILABLE.code()),
        "{held:?}"
    );
    assert_eq!(f.count("SELECT count(*) FROM {table}_rejects").await, 0);
    assert!(
        f.seen.0.lock().unwrap().is_empty(),
        "told nothing while held"
    );
    holder.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(reason_of(&f.intake(&poison).await), Some(Reason::Malformed));
    assert_eq!(f.count("SELECT seen::bigint FROM {table}_rejects").await, 1);
    assert_eq!(f.seen.0.lock().unwrap().len(), 1, "told once");
}

/// A message whose identity another intake is storing at that moment — a
/// rebalance handing it to two members at once — waits for that intake's
/// transaction: committed, the message is a duplicate; rolled back, it is
/// stored. Never set aside, never an error, never two rows.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn an_intake_waits_for_another_storing_the_same_identity() {
    for commit in [true, false] {
        let f = fixture(if commit {
            "racing_commit"
        } else {
            "racing_rollback"
        })
        .await;
        let message = order("1", ID_1, "p");
        assert!(matches!(f.intake(&message).await, Admission::Stored(_)));
        // the row as an intake writes it, written again by another
        // transaction, in flight: the copy kept, the row gone, then the insert
        let (holder, connection) = config().connect(NoTls).await.unwrap();
        tokio::spawn(connection);
        // Two queries: statements of one query before a BEGIN share its
        // transaction, and the rollback would restore the row.
        holder
            .batch_execute(&format!(
                "CREATE TEMP TABLE held AS SELECT * FROM {t}; DELETE FROM {t}",
                t = f.table
            ))
            .await
            .unwrap();
        holder
            .batch_execute(&format!(
                "BEGIN; \
                 INSERT INTO {t} (tenant_id, stream_type, stream_id, stream_position, uri, payload, metadata) \
                 SELECT tenant_id, stream_type, stream_id, stream_position, uri, payload, metadata FROM held",
                t = f.table
            ))
            .await
            .unwrap();
        let racing = {
            let inbox = Arc::clone(&f.inbox);
            let message = message.clone();
            tokio::spawn(async move { inbox.intake("inbox://orders/k", &message).await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(
            !racing.is_finished(),
            "the intake waits for the transaction in flight"
        );
        holder
            .batch_execute(if commit { "COMMIT" } else { "ROLLBACK" })
            .await
            .unwrap();
        let admission = racing.await.unwrap().unwrap();
        if commit {
            assert_eq!(admission, Admission::Duplicate);
        } else {
            assert!(matches!(admission, Admission::Stored(_)), "{admission:?}");
        }
        assert_eq!(f.rows().await, 1);
        assert_eq!(f.count("SELECT count(*) FROM {table}_rejects").await, 0);
    }
}

/// Many intakes of one message at once, for many identities: each is stored
/// once, every other intake finds it a duplicate, and none is set aside or
/// fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn many_intakes_of_one_message_at_once_store_it_once() {
    let f = fixture("racing_many").await;
    let mut stored = 0;
    let mut duplicates = 0;
    for position in 1..=20 {
        let message = order(
            &position.to_string(),
            &format!("0190a01a-7e5b-7d80-a000-{position:012}"),
            "p",
        );
        let racing: Vec<_> = (0..8)
            .map(|_| {
                let (inbox, message) = (Arc::clone(&f.inbox), message.clone());
                tokio::spawn(async move { inbox.intake("inbox://orders/k", &message).await })
            })
            .collect();
        let mut stored_here = 0;
        for task in racing {
            match task.await.unwrap() {
                Ok(Admission::Stored(_)) => stored_here += 1,
                Ok(Admission::Duplicate) => duplicates += 1,
                other => panic!("position {position}: {other:?}"),
            }
        }
        assert_eq!(stored_here, 1, "position {position} stored once");
        stored += stored_here;
    }
    assert_eq!((stored, duplicates), (20, 140));
    assert_eq!(f.rows().await, 20);
    assert_eq!(f.count("SELECT count(*) FROM {table}_rejects").await, 0);
}

/// A message past the inbox's bounds is set aside before its headers are
/// read, and kept cut: the same bytes again are the same row, and a readmit
/// is refused, since the row is not the message (ADR-0037 decision 3).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_message_past_the_bounds_is_kept_cut_and_never_readmitted() {
    let table = "intake_bounds";
    let inbox = PgInbox::new(PgSessionPool::new(pool()))
        .with_table(
            Identifier::new(table).unwrap(),
            Identifier::new(format!("{table}_seq")).unwrap(),
        )
        .with_bounds(Bounds {
            payload: 1 << 10,
            ..Bounds::default()
        })
        .readmitting(|_| true);
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    let big = order("1", ID_1, &"p".repeat(1 << 16));

    let Admission::SetAside {
        digest,
        reason: Reason::Malformed,
        repeat: false,
    } = inbox.intake("inbox://orders/k", &big).await.unwrap()
    else {
        panic!("set aside as malformed");
    };
    assert!(matches!(
        inbox.intake("inbox://orders/k", &big).await.unwrap(),
        Admission::SetAside { repeat: true, .. }
    ));
    let kept = inbox
        .set_aside_message("ops", &digest)
        .await
        .unwrap()
        .expect("the row's bytes");
    assert!(kept.len() < 4 << 10, "kept cut: {} bytes", kept.len());
    assert!(matches!(
        inbox.readmit("ops", &digest).await,
        Err(Error::Refused(why)) if why.contains("cut")
    ));
}

/// Runs `ddl` on the fixture's tables, `{t}` naming the inbox's.
async fn altered(f: &Fixture, ddl: &str) {
    let ddl = ddl.replace("{t}", &f.table);
    f.sessions
        .session(async |session| {
            session.connection().batch_execute(&ddl).await?;
            Ok::<_, Error>(())
        })
        .await
        .unwrap();
}

/// What the library did not make and that would change what the intake's
/// statements can raise is refused at `migrate` (ADR-0037 decision 5), on
/// the inbox's table and on the rows set aside.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_host_object_on_the_inbox_s_tables_is_refused_at_migrate() {
    for (kind, ddl) in [
        ("indexes", "CREATE INDEX {t}_host_idx ON {t} (uri)"),
        (
            "indexes",
            "CREATE INDEX {t}_rejects_host_idx ON {t}_rejects (reason)",
        ),
        (
            "constraints",
            "ALTER TABLE {t} ADD CONSTRAINT {t}_host_check CHECK (stream_position > 0)",
        ),
        (
            "triggers",
            "CREATE OR REPLACE FUNCTION intake_host_noop() RETURNS trigger LANGUAGE plpgsql \
             AS $$ BEGIN RETURN NEW; END $$; \
             CREATE TRIGGER {t}_host_trg BEFORE INSERT ON {t}_rejects \
             FOR EACH ROW EXECUTE FUNCTION intake_host_noop()",
        ),
        (
            "rules",
            "CREATE RULE {t}_host_rule AS ON INSERT TO {t} DO ALSO NOTIFY intake_host",
        ),
        (
            "policies",
            "CREATE POLICY {t}_host_policy ON {t} USING (true)",
        ),
        ("row security", "ALTER TABLE {t} ENABLE ROW LEVEL SECURITY"),
        (
            "columns",
            "ALTER TABLE {t} ADD COLUMN host_note text NOT NULL DEFAULT 'x'",
        ),
        // A column the intake's insert leaves out, made NOT NULL: every
        // insert would raise 23502, which is not the message's own, and the
        // source would hold. 14 records it in `attnotnull`, 18 in
        // `pg_constraint` as well; both are refused alike.
        (
            "columns",
            "ALTER TABLE {t} ALTER COLUMN processed_position SET NOT NULL",
        ),
        (
            "columns",
            "ALTER TABLE {t}_rejects ALTER COLUMN tenant_id SET NOT NULL",
        ),
        // A default the library makes, changed: a row would be born tried.
        (
            "defaults",
            "ALTER TABLE {t} ALTER COLUMN attempts SET DEFAULT 1",
        ),
        // A column the library makes, gone: its statements name it.
        (
            "none of the columns",
            "ALTER TABLE {t} DROP COLUMN last_error",
        ),
        // The slot cut by another key at the same count: one stream's rows
        // would land in two slots. The frame of the expression is the
        // library's; only the server's rendering of the library's own
        // expression tells the key apart. Re-added, the column moves to the
        // end, and `__head_idx` goes with it.
        (
            "generated",
            "ALTER TABLE {t} DROP COLUMN slot CASCADE, ADD COLUMN slot smallint \
             GENERATED ALWAYS AS ((hashtext(uri) & 2147483647) % 1) STORED",
        ),
        // An identity on a column the insert names: every insert raises 428C9.
        (
            "identity",
            "ALTER TABLE {t} ALTER COLUMN stream_position ADD GENERATED ALWAYS AS IDENTITY",
        ),
        // A primary key made DEFERRABLE under its own name, on either table:
        // a conflict cannot arbitrate it (ADR-0036 R2).
        (
            "deferrable",
            "ALTER TABLE {t} DROP CONSTRAINT {t}_pk, ADD CONSTRAINT {t}_pk \
             PRIMARY KEY (tenant_id, stream_type, stream_id, stream_position) DEFERRABLE",
        ),
        (
            "deferrable",
            "ALTER TABLE {t}_rejects DROP CONSTRAINT {t}_rejects_pkey, \
             ADD CONSTRAINT {t}_rejects_pkey PRIMARY KEY (digest) DEFERRABLE",
        ),
    ] {
        let f = fixture("host_object").await;
        altered(&f, ddl).await;
        let refused = f.inbox.migrate().await.unwrap_err();
        assert!(refused.to_string().contains(kind), "{kind}: {refused}");
    }
    // From 18 a generated column may be VIRTUAL: the same expression,
    // computed at each read, is no stored slot the statements index. 14 has
    // no such column, so the row has nothing to stand for there.
    let f = fixture("host_object").await;
    if server_version_num(&f).await >= 180_000 {
        altered(
            &f,
            "ALTER TABLE {t} DROP COLUMN slot CASCADE, ADD COLUMN slot smallint \
             GENERATED ALWAYS AS ((hashtext(tenant_id || ':' || stream_type || ':' || \
             stream_id::text) & 2147483647) % 1) VIRTUAL",
        )
        .await;
        let refused = f.inbox.migrate().await.unwrap_err();
        assert!(refused.to_string().contains("generated"), "{refused}");
    }
}

/// A function the dispatch calls that is not as the library made it is
/// refused at `migrate` (ADR-0041 decision 6), by what differs: the server
/// would otherwise run another statement, refuse the walk at every dispatch,
/// plan it at every call, or find the call ambiguous. Another function named
/// after the table, under a name the dispatch does not call, is let be.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_dispatch_function_not_as_the_library_made_it_is_refused_at_migrate() {
    const WALK: &str = "{t}__walk(smallint, integer, double precision, text[])";
    let schema = "intake_function_schema";
    let sessions = PgSessionPool::new(pool());
    let setup = async |sql: String| {
        sessions
            .session(async |session| {
                session.connection().batch_execute(&sql).await?;
                Ok::<_, Error>(())
            })
            .await
            .unwrap()
    };
    // What a run stopped half-way leaves, which a teardown does not drop:
    // the functions this test makes beside the library's.
    setup(format!(
        "DROP FUNCTION IF EXISTS intake_dispatch_function__take(integer); \
         DROP FUNCTION IF EXISTS intake_dispatch_function__walk_v2(); \
         DROP FUNCTION IF EXISTS intake_dispatch_function__walk(text, text, text, text); \
         DROP FUNCTION IF EXISTS public.intake_dispatch_function_schema__take(); \
         DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_proc \
             WHERE proname = 'intake_dispatch_function__take' AND prokind = 'p') \
             THEN DROP PROCEDURE intake_dispatch_function__take(); END IF; END $$; \
         DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}"
    ))
    .await;
    for (what, ddl, undo) in [
        ("is missing", format!("DROP FUNCTION {WALK}"), ""),
        // A column of another type, all else the library's: the dispatch
        // would read an integer as a bigint.
        (
            "its signature",
            format!(
                "DO $$ DECLARE made text := pg_get_functiondef('{WALK}'::regprocedure); \
                 BEGIN DROP FUNCTION {WALK}; \
                 EXECUTE replace(made, 'head_position bigint', 'head_position integer'); END $$"
            ),
            "",
        ),
        // A procedure of the name: no result to read.
        (
            "its signature",
            "DROP FUNCTION {t}__take(); \
             CREATE PROCEDURE {t}__take() LANGUAGE plpgsql AS $$ BEGIN END $$"
                .to_owned(),
            "DROP PROCEDURE {t}__take()",
        ),
        (
            "its settings",
            format!("ALTER FUNCTION {WALK} RESET plan_cache_mode"),
            "",
        ),
        ("its volatility", format!("ALTER FUNCTION {WALK} STABLE"), ""),
        (
            "its owner's rights",
            "ALTER FUNCTION {t}__take() SECURITY DEFINER".to_owned(),
            "",
        ),
        (
            "no EXECUTE",
            "REVOKE EXECUTE ON FUNCTION {t}__take() FROM PUBLIC, CURRENT_USER".to_owned(),
            "",
        ),
        // Its body edited in place: the function runs another statement.
        (
            "its body",
            "DO $$ BEGIN EXECUTE replace(pg_get_functiondef('{t}__take()'::regprocedure), \
             'BEGIN', 'BEGIN PERFORM 1;'); END $$"
                .to_owned(),
            "",
        ),
        (
            "its language",
            "CREATE OR REPLACE FUNCTION {t}__take(OUT taken_slot smallint, OUT take_snapshot text, \
             OUT slot_count integer, OUT take_isolation text) LANGUAGE sql \
             AS 'SELECT NULL::smallint, NULL::text, 1, ''read committed''::text'"
                .to_owned(),
            "",
        ),
        // An overload a call without arguments could resolve to: 42725.
        (
            "another function of its name",
            "CREATE FUNCTION {t}__take(integer DEFAULT 0) RETURNS integer LANGUAGE sql \
             AS 'SELECT 1'"
                .to_owned(),
            "DROP FUNCTION {t}__take(integer)",
        ),
    ] {
        let f = fixture("dispatch_function").await;
        altered(&f, &ddl).await;
        let refused = f.inbox.migrate().await;
        if !undo.is_empty() {
            altered(&f, undo).await;
        }
        let refused = refused.unwrap_err();
        assert!(refused.to_string().contains(what), "{what}: {refused}");
    }
    // Its table's schema and not another: the table where the search path
    // puts it, the function moved out to `public`, which the call then
    // resolves to.
    let mut searched = config();
    searched.options(format!("-c search_path={schema},public"));
    let f = fixture_on("dispatch_function_schema", pool_of(searched)).await;
    let table = &f.table;
    setup(format!(
        "ALTER FUNCTION {schema}.{table}__take() SET SCHEMA public"
    ))
    .await;
    let refused = f.inbox.migrate().await;
    setup(format!(
        "DROP FUNCTION public.{table}__take(); DROP SCHEMA {schema} CASCADE"
    ))
    .await;
    let refused = refused.unwrap_err();
    assert!(refused.to_string().contains("its schema"), "{refused}");
    // An overload in a schema the search path puts first is one a call may
    // resolve to as well: the library's own objects in `public`, a second
    // inbox on the same table started over that path.
    let f = fixture("dispatch_function").await;
    altered(
        &f,
        &format!(
            "CREATE SCHEMA {schema}; \
             CREATE FUNCTION {schema}.{{t}}__take(integer DEFAULT 0) RETURNS integer \
             LANGUAGE sql AS 'SELECT 1'"
        ),
    )
    .await;
    let mut ahead = config();
    ahead.options(format!("-c search_path={schema},public"));
    let refused = beside(&f, pool_of(ahead)).migrate().await;
    altered(&f, &format!("DROP SCHEMA {schema} CASCADE")).await;
    let refused = refused.unwrap_err();
    assert!(
        refused.to_string().contains("another function of its name"),
        "{refused}"
    );
    // The check reads the catalogue's values, not the server's rendering of
    // them, which a session's `quote_all_identifiers` changes.
    let mut quoted = config();
    quoted.options("-c quote_all_identifiers=on");
    beside(&f, pool_of(quoted)).migrate().await.unwrap();
    // The dispatch calls the functions: without them it fails, 42883, where
    // statements sent as text would not notice.
    let ok = |_: &PgSession, _: &InboxMessage| std::future::ready(Ok::<(), Failure>(()));
    for gone in [WALK, "{t}__take()"] {
        let f = fixture("dispatch_function").await;
        f.intake(&order("1", ID_1, "{}")).await;
        altered(&f, &format!("DROP FUNCTION {gone}")).await;
        let failed = f.inbox.dispatch(ok).await.unwrap_err();
        assert_eq!(code_of(&failed), Some("42883"), "{gone}: {failed}");
    }
    // The walk's one row is required: a slot without its row returns none,
    // and the call fails (P0002) rather than return a row of nulls.
    let f = fixture("dispatch_function").await;
    let strict = f
        .sessions
        .session(async |session| {
            session
                .connection()
                .query_one(
                    &format!(
                        "SELECT * FROM {}__walk(999::smallint, 32, 1.0::float8, '{{}}'::text[])",
                        f.table
                    ),
                    &[],
                )
                .await?;
            Ok::<_, Error>(())
        })
        .await
        .unwrap_err();
    assert_eq!(code_of(&strict), Some("P0002"), "{strict}");
    // The call's arguments are cast to the walk's own types: a function of
    // the name taking text, made after the start, is not the one it reaches.
    let f = fixture("dispatch_function").await;
    f.intake(&order("1", ID_1, "{}")).await;
    altered(
        &f,
        "CREATE FUNCTION {t}__walk(text, text, text, text, OUT head_position bigint, \
         OUT head_place text, OUT walk_outcome text, OUT walk_snapshot text) LANGUAGE sql \
         AS 'SELECT NULL::bigint, NULL::text, ''nothing'', NULL::text'",
    )
    .await;
    let dispatched = f.inbox.dispatch(ok).await;
    altered(&f, "DROP FUNCTION {t}__walk(text, text, text, text)").await;
    assert_eq!(dispatched.unwrap(), Outcome::Processed);
    // A function of another name after the table is a newer release's, or
    // the host's: let be.
    let f = fixture("dispatch_function").await;
    altered(
        &f,
        "CREATE FUNCTION {t}__walk_v2() RETURNS integer LANGUAGE sql AS 'SELECT 1'",
    )
    .await;
    let started = f.inbox.migrate().await;
    altered(&f, "DROP FUNCTION {t}__walk_v2()").await;
    started.unwrap();
}

async fn server_version_num(f: &Fixture) -> i32 {
    f.sessions
        .session(async |session| {
            let row = session
                .connection()
                .query_one("SELECT current_setting('server_version_num')::int", &[])
                .await?;
            Ok::<_, Error>(row.get(0))
        })
        .await
        .unwrap()
}

/// A value of each type the library's columns have, other than NULL: `SET
/// DEFAULT NULL` stores no default, and would prove nothing.
fn a_value_of(sql_type: &str) -> &'static str {
    match sql_type {
        "integer" | "bigint" | "smallint" => "1",
        "text" => "'x'",
        t if t.starts_with("character varying") => "'x'",
        "jsonb" => "'{}'",
        "bytea" => "'\\x00'",
        "timestamp with time zone" => "statement_timestamp()",
        "uuid" => "'00000000-0000-4000-8000-000000000000'",
        other => panic!("a value of `{other}`: name one for the library's new type"),
    }
}

/// Every column the library makes is held at `migrate`, one by one: its
/// default dropped, or set where it makes none, and a NOT NULL set where it
/// makes the column nullable (ADR-0037 decision 5). The columns are read
/// from tables the library has just made, not from a list, so that a
/// column the comparison misses is a red case here. A generated column's
/// case is the host object's.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn every_column_the_inbox_makes_is_held_at_migrate() {
    let f = fixture("held_column").await;
    let rejects = format!("{}_rejects", f.table);
    let columns: Vec<(String, String, String, bool, bool, bool)> = f
        .sessions
        .session(async |session| {
            let rows = session
                .connection()
                .query(
                    "SELECT c.relname::text, a.attname::text, format_type(a.atttypid, a.atttypmod), \
                            d.adbin IS NOT NULL, a.attgenerated <> '', a.attnotnull \
                     FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid \
                     LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
                     WHERE a.attrelid IN ($1::text::regclass, $2::text::regclass) \
                       AND a.attnum > 0 AND NOT a.attisdropped",
                    &[&f.table, &rejects],
                )
                .await?;
            Ok::<_, Error>(
                rows.iter()
                    .map(|row| {
                        (
                            row.get(0),
                            row.get(1),
                            row.get(2),
                            row.get(3),
                            row.get(4),
                            row.get(5),
                        )
                    })
                    .collect(),
            )
        })
        .await
        .unwrap();
    let mut cases = Vec::new();
    for (relation, column, sql_type, defaulted, generated, not_null) in &columns {
        if *generated {
            continue;
        }
        let alter = format!("ALTER TABLE {relation} ALTER COLUMN {column}");
        let set = if *defaulted {
            format!("{alter} DROP DEFAULT")
        } else {
            format!("{alter} SET DEFAULT {}", a_value_of(sql_type))
        };
        cases.push((relation.clone(), column.clone(), set));
        if !not_null {
            cases.push((
                relation.clone(),
                column.clone(),
                format!("{alter} SET NOT NULL"),
            ));
        }
    }
    assert!(
        cases.len() > 40,
        "both tables' columns were read: {cases:?}"
    );
    for (relation, column, ddl) in cases {
        let f = fixture("held_column").await;
        altered(&f, &ddl).await;
        let refused = f.inbox.migrate().await.unwrap_err().to_string();
        assert!(
            refused.contains(&format!("`{relation}`")) && refused.contains(&format!("({column})")),
            "{ddl}: {refused}"
        );
    }
}

/// A start over a current schema takes no lock on the inbox's tables
/// (ADR-0029): it reads the catalogue, and renders both sides of its
/// comparison against its twins. Another session holds both tables in ACCESS
/// EXCLUSIVE mode — a migration of a newer release, an operator's `VACUUM
/// FULL` — and `migrate` still returns at once.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_current_inbox_starts_beside_its_tables_held_exclusively() {
    let f = fixture("held_exclusively").await;
    let (holder, connection) = config().connect(NoTls).await.expect("a connection");
    tokio::spawn(connection);
    holder
        .batch_execute(&format!(
            "BEGIN; LOCK TABLE {t} IN ACCESS EXCLUSIVE MODE; \
             LOCK TABLE {t}_rejects IN ACCESS EXCLUSIVE MODE;",
            t = f.table
        ))
        .await
        .expect("held");
    tokio::time::timeout(std::time::Duration::from_secs(5), f.inbox.migrate())
        .await
        .expect("a current schema takes no lock on the inbox's tables")
        .unwrap();
    holder.batch_execute("ROLLBACK").await.expect("released");
}

/// A sequence named otherwise than `{table}_seq`, as a host names its own
/// (the library's default is `inbox_received_position_seq`): the default the
/// library compares the table's with draws from the table's own sequence.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_table_whose_sequence_is_named_otherwise_passes_its_check() {
    let inbox = PgInbox::new(PgSessionPool::new(pool()))
        .with_table(
            Identifier::new("intake_own_numbers").unwrap(),
            Identifier::new("intake_numbers_drawn").unwrap(),
        )
        .partitioned_by(ByStream);
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    inbox.migrate().await.unwrap();
    inbox.teardown().await.unwrap();
}

/// What a newer release makes is the library's: an older one starts beside
/// it (ADR-0036 R12). Its objects carry the `{table}__` prefix, a column it
/// adds is nullable, without a default, and the ledger row of its migration
/// is one this release does not know.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_newer_release_s_objects_on_the_inbox_s_tables_are_the_library_s() {
    let f = fixture("newer_release").await;
    altered(
        &f,
        "CREATE OR REPLACE FUNCTION intake_newer_noop() RETURNS trigger LANGUAGE plpgsql \
         AS $$ BEGIN RETURN NEW; END $$; \
         CREATE TRIGGER {t}__newer_trg BEFORE INSERT ON {t} \
         FOR EACH ROW EXECUTE FUNCTION intake_newer_noop(); \
         CREATE UNIQUE INDEX {t}__newer_uniq ON {t} (received_position); \
         CREATE INDEX {t}_rejects__newer_idx ON {t}_rejects (last_seen_at); \
         ALTER TABLE {t} ADD COLUMN newer_note text NULL; \
         INSERT INTO ascetic_ddd_migrations (component, migration, checksum) \
         VALUES ('inbox:{t}', '29991231T000000Z_newer', 'a newer release''s');",
    )
    .await;
    let applied = f.inbox.migrate().await.unwrap();
    assert_eq!(applied.unknown, ["29991231T000000Z_newer"]);
    assert!(matches!(
        f.intake(&order("1", ID_1, "p")).await,
        Admission::Stored(_)
    ));
}

/// A host's partition key over the stream id, cast to a type the value
/// overflows: the only 22003 the intake's statements can raise once the
/// stream id is in its domain. It is the message's own, set aside as
/// unstorable, never a stall (ADR-0037 decision 5).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_number_a_host_s_key_overflows_is_set_aside_as_unstorable() {
    struct SmallKey;
    impl PartitionKey for SmallKey {
        fn sql_expression(&self) -> &str {
            "((stream_id->>'n')::int2)::text"
        }
    }
    let table = "intake_small_key";
    let inbox = PgInbox::new(PgSessionPool::new(pool()))
        .with_table(
            Identifier::new(table).unwrap(),
            Identifier::new(format!("{table}_seq")).unwrap(),
        )
        .partitioned_by(SmallKey);
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    let keyed = |n: &str| {
        Message::new("p")
            .with_header(TENANT_ID, "t1")
            .with_header(STREAM_TYPE, "orders.Order")
            .with_header(STREAM_ID, format!(r#"{{"n":"{n}"}}"#))
            .with_header(STREAM_POSITION, "1")
    };
    let overflowing = inbox.intake("inbox://orders/k", &keyed("40000")).await;
    assert!(
        matches!(
            overflowing,
            Ok(Admission::SetAside {
                reason: Reason::Unstorable,
                ..
            })
        ),
        "{overflowing:?}"
    );
    assert!(matches!(
        inbox.intake("inbox://orders/k", &keyed("5")).await,
        Ok(Admission::Stored(_))
    ));
}

/// A repeat past `seen`'s range saturates instead of failing the intake,
/// which would hold the channel (ADR-0037 decision 7).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_repeat_past_the_count_s_range_saturates() {
    let f = fixture("seen_saturates").await;
    let poison = order("not-a-position", ID_1, "p");
    let Admission::SetAside { digest, .. } = f.intake(&poison).await else {
        panic!("set aside");
    };
    altered(
        &f,
        &format!("UPDATE {{t}}_rejects SET seen = 2147483647 WHERE digest = '{digest}'"),
    )
    .await;
    assert!(matches!(
        f.intake(&poison).await,
        Admission::SetAside { repeat: true, .. }
    ));
}

/// The same bytes judged otherwise by another release refresh the open
/// row's reason, keeping its first sight: the gauge and the readmit by
/// reason act on today's verdict (ADR-0037 decision 7). Here one inbox
/// refuses the stream type as foreign, and another on the same table,
/// admitting it, finds its `message_id` held at another identity.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_repeat_judged_otherwise_refreshes_the_open_row_s_reason() {
    let f = fixture("verdict_refreshed").await;
    let admitting_all = PgInbox::new(PgSessionPool::new(pool())).with_table(
        Identifier::new(&f.table).unwrap(),
        Identifier::new(format!("{}_seq", f.table)).unwrap(),
    );
    let thing = |stream_id: &str| {
        Message::new("p")
            .with_header(TENANT_ID, "t1")
            .with_header(STREAM_TYPE, "other.Thing")
            .with_header(STREAM_ID, stream_id)
            .with_header(STREAM_POSITION, "1")
            .with_header("message_id", ID_1)
    };
    assert!(matches!(
        admitting_all
            .intake("inbox://orders/k", &thing("held"))
            .await
            .unwrap(),
        Admission::Stored(_)
    ));
    let late = thing("late");
    assert_eq!(reason_of(&f.intake(&late).await), Some(Reason::Foreign));
    let first_seen: String = f
        .count_text("SELECT min(first_seen_at)::text FROM {table}_rejects")
        .await;
    assert_eq!(
        reason_of(
            &admitting_all
                .intake("inbox://orders/k", &late)
                .await
                .unwrap()
        ),
        Some(Reason::IdentityTaken)
    );
    assert_eq!(f.set_aside("identity_taken").await, 1);
    assert_eq!(f.set_aside("foreign").await, 0);
    assert_eq!(
        f.count_text("SELECT min(first_seen_at)::text FROM {table}_rejects")
            .await,
        first_seen
    );
}
