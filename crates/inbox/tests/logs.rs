//! What the inbox writes to the `log` facade names a row by its channel and
//! its place, and an error by the library's words and the server's codes:
//! never a stream id, a Tenant, a `message_id`, a header's value or a
//! subscriber's text, which may be personal data or the very value refused
//! (ADR-0036 R9).
//!
//! The driver is another matter: `tokio_postgres` logs every statement with
//! its parameters at `Debug`, the message's values with them. A host keeps
//! the target `tokio_postgres` at `Info` or above wherever real data runs;
//! these tests hold that no line of the driver above `Debug` carries a
//! value, the bound that rule rests on. They need a live database:
//!
//! ```text
//! ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
//!     cargo test -p ascetic-ddd-inbox --test logs -- --ignored
//! ```

use std::str::FromStr;
use std::sync::{Mutex, OnceLock};

use ascetic_ddd_bus::Message;
use ascetic_ddd_bus::identity::{STREAM_ID, STREAM_POSITION, STREAM_TYPE, TENANT_ID};
use ascetic_ddd_inbox::{Admission, Error, Failure, InboxMessage, PgInbox, Retries};
use ascetic_ddd_session::pg::Identifier;
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};
use ascetic_ddd_session::{PgAccess, PgSession, PgSessionPool, SessionPool};

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

/// Every line logged in this binary, at every level: its level, its target
/// and its text.
struct Captured(Mutex<Vec<(log::Level, String, String)>>);

impl log::Log for Captured {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        self.0.lock().unwrap().push((
            record.level(),
            record.target().to_owned(),
            record.args().to_string(),
        ));
    }

    fn flush(&self) {}
}

fn captured() -> &'static Captured {
    static CAPTURED: OnceLock<&'static Captured> = OnceLock::new();
    CAPTURED.get_or_init(|| {
        let captured: &'static Captured = Box::leak(Box::new(Captured(Mutex::new(Vec::new()))));
        log::set_logger(captured).unwrap();
        log::set_max_level(log::LevelFilter::Trace);
        captured
    })
}

/// The lines logged so far that hold `needle`, of a target that starts
/// with `target`, at `level` or more severe.
fn lines_with(needle: &str, target: &str, level: log::Level) -> Vec<String> {
    captured()
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|(at, from, line)| {
            *at <= level && from.starts_with(target) && line.contains(needle)
        })
        .map(|(at, from, line)| format!("{at} {from}: {line}"))
        .collect()
}

/// The library's own lines that hold `needle`, at any level.
fn ours_with(needle: &str) -> Vec<String> {
    lines_with(needle, "ascetic_ddd", log::Level::Trace)
}

/// Values no line may carry, each in a header or the subscriber's text.
const TENANT: &str = "tenant-secret-0";
const STREAM: &str = "stream-secret-1";
const MESSAGE_ID: &str = "0190a01a-7e5b-7d80-a000-5ec2e7000002";
const TRACE: &str = "header-secret-3";
const SUBSCRIBER_TEXT: &str = "subscriber-secret-4";

fn secrets() -> [&'static str; 5] {
    [TENANT, STREAM, MESSAGE_ID, TRACE, SUBSCRIBER_TEXT]
}

fn message(position: &str) -> Message {
    Message::new("p")
        .with_header(TENANT_ID, TENANT)
        .with_header(STREAM_TYPE, "logs.Order")
        .with_header(STREAM_ID, STREAM)
        .with_header(STREAM_POSITION, position)
        .with_header("message_id", MESSAGE_ID)
        .with_header("traceparent", TRACE)
}

async fn inbox(table: &str) -> PgInbox<PgSessionPool> {
    let inbox = PgInbox::new(PgSessionPool::new(pool())).with_table(
        Identifier::new(table).unwrap(),
        Identifier::new(format!("{table}_seq")).unwrap(),
    );
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    inbox
}

/// A message set aside, an attempt that failed and a message parked are
/// logged by their channel and place alone.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_inbox_s_lines_carry_no_value_of_the_message() {
    captured();
    let inbox = inbox("logs_lines").await.with_retries(Retries::up_to(2));
    let channel = "inbox://logs-lines";
    let poison = inbox
        .intake(&format!("{channel}/k"), &message("not-a-position"))
        .await
        .unwrap();
    assert!(matches!(poison, Admission::SetAside { .. }), "{poison:?}");
    assert!(matches!(
        inbox
            .intake(&format!("{channel}/k"), &message("1"))
            .await
            .unwrap(),
        Admission::Stored(_)
    ));
    let failing = |_: &PgSession, _: &InboxMessage| {
        std::future::ready(Err::<(), Failure>(SUBSCRIBER_TEXT.into()))
    };
    inbox.dispatch(failing).await.unwrap();
    inbox.dispatch(failing).await.unwrap();

    let lines = ours_with(channel);
    assert!(
        lines.iter().any(|line| line.contains("set aside"))
            && lines.iter().any(|line| line.contains("attempt 1"))
            && lines.iter().any(|line| line.contains("parked after 2")),
        "the three lines, logged: {lines:#?}"
    );
    for secret in secrets() {
        assert!(
            ours_with(secret).is_empty(),
            "{secret} logged: {:#?}",
            ours_with(secret)
        );
        let driver = lines_with(secret, "tokio_postgres", log::Level::Info);
        assert!(
            driver.is_empty(),
            "the driver logs {secret} above Debug: {driver:#?}"
        );
    }
}

/// A refusal of the server is shown by its SQLSTATE and its constraint,
/// never by its DETAIL, which echoes the key's values.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_refusal_shows_its_code_and_constraint_and_no_value() {
    let sessions = PgSessionPool::new(pool());
    let refused = sessions
        .session(async |session| {
            let connection = session.connection();
            connection
                .batch_execute(
                    "DROP TABLE IF EXISTS logs_refusal; \
                     CREATE TABLE logs_refusal (k text CONSTRAINT logs_refusal_k UNIQUE)",
                )
                .await?;
            connection
                .execute("INSERT INTO logs_refusal VALUES ($1)", &[&TENANT])
                .await?;
            Ok::<_, Error>(
                connection
                    .execute("INSERT INTO logs_refusal VALUES ($1)", &[&TENANT])
                    .await
                    .unwrap_err(),
            )
        })
        .await
        .unwrap();
    let shown = Error::Database(refused).to_string();
    assert!(
        shown.contains("23505") && shown.contains("logs_refusal_k"),
        "{shown}"
    );
    assert!(!shown.contains(TENANT), "{shown}");
}
