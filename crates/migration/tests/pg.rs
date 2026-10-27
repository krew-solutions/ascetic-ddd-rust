//! The runner against a real PostgreSQL (ADR-0029):
//!
//! ```text
//! ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
//!     cargo test -p ascetic-ddd-migration -- --ignored
//! ```
//!
//! Each test names its own tables and component, and drops and forgets them.

use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ascetic_ddd_migration::{
    Component, LEDGER, Migration, MigrationError, Options, forget, migrate,
};
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{self, Client, NoTls};
use ascetic_ddd_session::pg::{Identifier, PgSessionPool};

const DEFAULT_URL: &str = "postgresql://devel:devel@localhost:5432/devel_karmabot_test";

fn url() -> String {
    std::env::var("ASCETIC_DDD_TEST_PG_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned())
}

fn sessions() -> PgSessionPool {
    let config = tokio_postgres::Config::from_str(&url()).expect("a valid PostgreSQL URL");
    let manager = Manager::from_config(
        config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    PgSessionPool::new(Pool::builder(manager).max_size(4).build().expect("a pool"))
}

/// Another session, as live traffic or another runner has.
async fn another() -> Client {
    let (client, connection) = tokio_postgres::connect(&url(), NoTls)
        .await
        .expect("a connection");
    tokio::spawn(connection);
    client
}

/// A name of this test's own.
fn unique(stem: &str) -> String {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|t| t.subsec_nanos())
        .unwrap_or(0);
    format!("{stem}_{:x}_{}", nanos, NEXT.fetch_add(1, Ordering::SeqCst))
}

fn table(name: &str) -> Identifier {
    Identifier::new(name).expect("an identifier")
}

/// A component over the tables `a` and `b`: its baseline creates them,
/// a step adds a column to `a`, another indexes it.
fn orders(name: &str, a: &str, b: &str) -> Component {
    Component::new(
        name,
        vec![
            Migration::new(
                "0001_baseline",
                format!("CREATE TABLE {a} (id INT PRIMARY KEY); CREATE TABLE {b} (id INT);"),
            ),
            Migration::new("0002_note", format!("ALTER TABLE {a} ADD COLUMN note TEXT"))
                .on(table(a)),
            Migration::new(
                "0003_note_index",
                format!("CREATE INDEX {a}_note ON {a} (note)"),
            )
            .on(table(a)),
        ],
    )
    .expect("a component")
}

async fn drop_all(name: &str, tables: &[&str]) {
    let client = another().await;
    for t in tables {
        client
            .batch_execute(&format!("DROP TABLE IF EXISTS {t}"))
            .await
            .expect("dropped");
    }
    forget(&sessions(), name).await.expect("forgotten");
}

async fn ledger_rows(name: &str) -> Vec<String> {
    let client = another().await;
    client
        .query(
            &format!("SELECT migration FROM {LEDGER} WHERE component = $1 ORDER BY migration"),
            &[&name],
        )
        .await
        .expect("the ledger")
        .iter()
        .map(|row| row.get(0))
        .collect()
}

fn quick() -> Options {
    Options {
        lock_timeout: Duration::from_millis(100),
        budget: Duration::from_secs(5),
        backoff: Duration::from_millis(20),
        backoff_ceiling: Duration::from_millis(200),
    }
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_component_is_migrated_once_and_then_only_read() {
    let (name, a, b) = (unique("orders"), unique("a"), unique("b"));
    let component = orders(&name, &a, &b);
    let first = migrate(&sessions(), &component, &quick())
        .await
        .expect("migrated");
    let applied: Vec<&str> = first.applied.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(applied, ["0001_baseline", "0002_note", "0003_note_index"]);
    let again = migrate(&sessions(), &component, &quick())
        .await
        .expect("read");
    assert!(
        again.applied.is_empty() && again.unknown.is_empty(),
        "{again:?}"
    );
    assert_eq!(
        ledger_rows(&name).await,
        ["0001_baseline", "0002_note", "0003_note_index"]
    );
    drop_all(&name, &[&a, &b]).await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_current_schema_starts_beside_its_tables_held_exclusively() {
    let (name, a, b) = (unique("orders"), unique("a"), unique("b"));
    let component = orders(&name, &a, &b);
    migrate(&sessions(), &component, &quick())
        .await
        .expect("migrated");
    let live = another().await;
    live.batch_execute(&format!(
        "BEGIN; LOCK TABLE {a} IN ACCESS EXCLUSIVE MODE; LOCK TABLE {b} IN ACCESS EXCLUSIVE MODE;"
    ))
    .await
    .expect("held");
    let started = Instant::now();
    let read = migrate(
        &sessions(),
        &component,
        &Options {
            budget: Duration::from_millis(300),
            ..quick()
        },
    )
    .await
    .expect("a current schema takes no table lock");
    assert!(read.applied.is_empty());
    assert!(
        started.elapsed() < Duration::from_millis(300),
        "{:?}",
        started.elapsed()
    );
    live.batch_execute("ROLLBACK").await.expect("released");
    drop_all(&name, &[&a, &b]).await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn two_runners_apply_each_migration_once() {
    let (name, a, b) = (unique("orders"), unique("a"), unique("b"));
    let component = orders(&name, &a, &b);
    let (first_pool, second_pool, options) = (sessions(), sessions(), quick());
    let (one, two) = tokio::join!(
        migrate(&first_pool, &component, &options),
        migrate(&second_pool, &component, &options)
    );
    let (one, two) = (
        one.expect("the first runner"),
        two.expect("the second runner"),
    );
    assert_eq!(one.applied.len() + two.applied.len(), 3, "{one:?} {two:?}");
    assert_eq!(ledger_rows(&name).await.len(), 3);
    drop_all(&name, &[&a, &b]).await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_held_target_is_waited_for_and_then_migrated() {
    let (name, a, b) = (unique("orders"), unique("a"), unique("b"));
    let full = orders(&name, &a, &b);
    let baseline = Component::new(&name, full.migrations()[..1].to_vec()).expect("a component");
    migrate(&sessions(), &baseline, &quick())
        .await
        .expect("the baseline");
    let live = another().await;
    live.batch_execute(&format!("BEGIN; LOCK TABLE {a} IN ACCESS EXCLUSIVE MODE;"))
        .await
        .expect("held");
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(600)).await;
        live.batch_execute("COMMIT").await.expect("released");
    });
    let done = migrate(&sessions(), &full, &quick())
        .await
        .expect("migrated once released");
    assert_eq!(done.applied.len(), 2, "{done:?}");
    release.await.expect("the holder");
    drop_all(&name, &[&a, &b]).await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn held_past_the_budget_the_start_is_blocked_naming_the_holder() {
    let (name, a, b) = (unique("orders"), unique("a"), unique("b"));
    let full = orders(&name, &a, &b);
    let baseline = Component::new(&name, full.migrations()[..1].to_vec()).expect("a component");
    migrate(&sessions(), &baseline, &quick())
        .await
        .expect("the baseline");
    let live = another().await;
    live.batch_execute(&format!(
        "SET application_name = 'the_holder'; BEGIN; LOCK TABLE {a} IN ACCESS EXCLUSIVE MODE;"
    ))
    .await
    .expect("held");
    let refused = migrate(
        &sessions(),
        &full,
        &Options {
            budget: Duration::from_millis(300),
            ..quick()
        },
    )
    .await
    .expect_err("blocked");
    match refused {
        MigrationError::Blocked {
            migration, holders, ..
        } => {
            assert_eq!(migration, "0002_note");
            assert!(
                holders.iter().any(|h| h.contains("the_holder")),
                "{holders:?}"
            );
        }
        other => panic!("{other}"),
    }
    live.batch_execute("ROLLBACK").await.expect("released");
    assert_eq!(ledger_rows(&name).await, ["0001_baseline"]);
    drop_all(&name, &[&a, &b]).await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_migration_applied_under_another_text_is_refused_and_an_unknown_one_tolerated() {
    let (name, a, b) = (unique("orders"), unique("a"), unique("b"));
    let component = orders(&name, &a, &b);
    migrate(&sessions(), &component, &quick())
        .await
        .expect("migrated");
    another()
        .await
        .execute(
            &format!(
                "INSERT INTO {LEDGER} (component, migration, checksum) VALUES ($1, 'zz_newer', 'x')"
            ),
            &[&name],
        )
        .await
        .expect("a newer binary's row");
    let read = migrate(&sessions(), &component, &quick())
        .await
        .expect("tolerated");
    assert_eq!(read.unknown, ["zz_newer"]);
    let mut edited = component.migrations().to_vec();
    edited[1] = Migration::new(
        "0002_note",
        format!("ALTER TABLE {a} ADD COLUMN note VARCHAR"),
    )
    .on(table(&a));
    let refused = migrate(
        &sessions(),
        &Component::new(&name, edited).expect("a component"),
        &quick(),
    )
    .await
    .expect_err("refused");
    assert!(
        matches!(&refused, MigrationError::Changed { migration, .. } if migration == "0002_note"),
        "{refused}"
    );
    drop_all(&name, &[&a, &b]).await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_step_that_locks_a_second_table_is_refused_and_leaves_nothing() {
    let (name, a, b) = (unique("orders"), unique("a"), unique("b"));
    let full = orders(&name, &a, &b);
    let both = Component::new(
        &name,
        vec![
            full.migrations()[0].clone(),
            Migration::new(
                "0002_both",
                format!("ALTER TABLE {a} ADD COLUMN x INT; ALTER TABLE {b} ADD COLUMN y INT;"),
            )
            .on(table(&a)),
        ],
    )
    .expect("a component");
    let refused = migrate(&sessions(), &both, &quick())
        .await
        .expect_err("refused");
    match refused {
        MigrationError::TwoTables {
            migration, tables, ..
        } => {
            assert_eq!(migration, "0002_both");
            assert_eq!(tables.as_slice(), std::slice::from_ref(&b));
        }
        other => panic!("{other}"),
    }
    let columns: i64 = another()
        .await
        .query_one(
            "SELECT count(*) FROM information_schema.columns WHERE table_name = $1 AND column_name = 'x'",
            &[&a],
        )
        .await
        .expect("the catalogue")
        .get(0);
    assert_eq!(columns, 0, "rolled back");
    assert_eq!(ledger_rows(&name).await, ["0001_baseline"]);
    // A column and an index on the one target are one step.
    let one = Component::new(
        &name,
        vec![
            full.migrations()[0].clone(),
            Migration::new(
                "0003_one",
                format!("ALTER TABLE {a} ADD COLUMN z INT; CREATE INDEX {a}_z ON {a} (z);"),
            )
            .on(table(&a)),
        ],
    )
    .expect("a component");
    migrate(&sessions(), &one, &quick())
        .await
        .expect("one table");
    drop_all(&name, &[&a, &b]).await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_live_reader_then_writer_commits_while_its_table_is_migrated() {
    let (name, a, b) = (unique("orders"), unique("a"), unique("b"));
    let full = orders(&name, &a, &b);
    let baseline = Component::new(&name, full.migrations()[..1].to_vec()).expect("a component");
    migrate(&sessions(), &baseline, &quick())
        .await
        .expect("the baseline");
    let step = Component::new(
        &name,
        vec![
            full.migrations()[0].clone(),
            Migration::new(
                "0002_index_then_column",
                format!("CREATE INDEX {a}_id ON {a} (id); ALTER TABLE {a} ADD COLUMN w INT;"),
            )
            .on(table(&a)),
        ],
    )
    .expect("a component");
    let live = another().await;
    let (live_a, live_b) = (a.clone(), b.clone());
    let reader_then_writer = tokio::spawn(async move {
        live.batch_execute(&format!("BEGIN; SELECT count(*) FROM {live_a};"))
            .await?;
        tokio::time::sleep(Duration::from_millis(600)).await;
        live.batch_execute(&format!(
            "INSERT INTO {live_b} VALUES (1); INSERT INTO {live_a} VALUES (1000); COMMIT;"
        ))
        .await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    migrate(
        &sessions(),
        &step,
        &Options {
            lock_timeout: Duration::from_secs(2),
            ..quick()
        },
    )
    .await
    .expect("migrated");
    reader_then_writer
        .await
        .expect("the live transaction")
        .expect("the live transaction commits, never a deadlock's victim");
    drop_all(&name, &[&a, &b]).await;
}
