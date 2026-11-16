//! The table this release migrated, judged against its twin without the
//! columns ADR-0040 added — the causes and the hold's clock — is admitted:
//! they are nullable, without a default, so they bind no insert of a
//! release that does not know them (ADR-0037 decision 5 and R12; ADR-0040
//! § Executors). The twin stands for the release before ADR-0040; it is not
//! that release's own check, which ADR-0041 decision 6 replaced while no
//! release is deployed. Inside the crate, so that it runs the library's own
//! check over the migration's real statement, not a copy of either.

use std::str::FromStr;

use ascetic_ddd_session::pg::Identifier;
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};
use ascetic_ddd_session::{PgAccess, PgSessionPool, Session, SessionPool};

use super::PgInbox;
use super::store::refuse_what_the_library_did_not_make;
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

/// `twin` without the columns ADR-0040 added: the twin the release before
/// it rendered, its last column's comma dropped.
fn before_the_causes(twin: &str) -> String {
    let kept: Vec<&str> = twin
        .lines()
        .filter(|line| {
            !ADDED
                .iter()
                .any(|column| line.trim_start().starts_with(column))
        })
        .collect();
    assert_eq!(
        kept.len(),
        twin.lines().count() - ADDED.len(),
        "the twin names each added column once"
    );
    kept.join("\n")
        .replace(",\n    ) ON COMMIT DROP;", "\n    ) ON COMMIT DROP;")
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
    let before = before_the_causes(&inbox.inbox_twin());
    let sessions = PgSessionPool::new(pool());
    let pk = format!("{table}_pk");
    // The twin lives for one transaction, as `migrate` makes it.
    let judged = async || {
        sessions
            .session(async |session| {
                session
                    .atomic(async |tx| {
                        tx.connection().batch_execute(&before).await?;
                        refuse_what_the_library_did_not_make(
                            &tx,
                            table,
                            &pk,
                            "pg_temp.ascetic_inbox_twin",
                        )
                        .await
                    })
                    .await
            })
            .await
    };
    let admitted = judged().await;
    assert!(admitted.is_ok(), "{admitted:?}");

    // the same twin refuses a column that binds an insert: the check bites
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
    let refused = judged().await;
    assert!(matches!(refused, Err(Error::Malformed(_))), "{refused:?}");
    inbox.teardown().await.unwrap();
}
