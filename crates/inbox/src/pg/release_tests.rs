//! The release before ADR-0040 admits the table this one migrated: the
//! columns it adds — the causes and the hold's clock — are nullable,
//! without a default, so the previous release's own list of columns finds
//! none that binds an insert (ADR-0037 decision 5 and R12; ADR-0040
//! § Executors). Inside the crate, so that it runs the library's own check
//! over the migration's real statement, not a copy of either.

use std::str::FromStr;

use ascetic_ddd_session::pg::Identifier;
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};
use ascetic_ddd_session::{PgAccess, PgSessionPool, SessionPool};

use super::PgInbox;
use super::store::{INBOX_COLUMNS, refuse_what_the_library_did_not_make};
use crate::error::Error;
use crate::partition::ByStream;

const DEFAULT_URL: &str = "postgresql://devel:devel@localhost:5432/devel_karmabot_test";

/// What ADR-0040 added to the inbox table.
const ADDED: [&str; 3] = ["last_cause", "parked_cause", "unavailable_since"];

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
        .max_size(2)
        .build()
        .expect("the pool can be built")
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_release_before_the_causes_admits_the_table_they_were_added_to() {
    let table = "inbox_release_before";
    let inbox = PgInbox::new(PgSessionPool::new(pool()))
        .with_table(
            Identifier::new(table).unwrap(),
            Identifier::new("inbox_release_before_seq").unwrap(),
        )
        .partitioned_by(ByStream);
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    let before: Vec<&str> = INBOX_COLUMNS
        .iter()
        .copied()
        .filter(|column| !ADDED.contains(column))
        .collect();
    assert_eq!(before.len(), INBOX_COLUMNS.len() - ADDED.len());
    let sessions = PgSessionPool::new(pool());
    let pk = format!("{table}_pk");
    let admitted = sessions
        .session(async |session| {
            refuse_what_the_library_did_not_make(&session, table, &pk, &before).await
        })
        .await;
    assert!(admitted.is_ok(), "{admitted:?}");

    // the same list refuses a column that binds an insert: the check bites
    sessions
        .session(async |session| {
            Ok::<_, Error>(
                session
                    .connection()
                    .batch_execute(&format!(
                        "ALTER TABLE {table} ADD COLUMN binding text NOT NULL DEFAULT 'x'"
                    ))
                    .await?,
            )
        })
        .await
        .unwrap();
    let refused = sessions
        .session(async |session| {
            refuse_what_the_library_did_not_make(&session, table, &pk, &before).await
        })
        .await;
    assert!(matches!(refused, Err(Error::Malformed(_))), "{refused:?}");
    inbox.teardown().await.unwrap();
}
