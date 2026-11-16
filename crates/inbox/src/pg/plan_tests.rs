//! The parked rows' count held to its plan (ADR-0036 decision 6): it goes
//! by the parked index, so its work is the parked rows' and not the
//! table's. Inside the crate, so that it explains the library's own
//! statement, not a copy that a change to it would leave behind.
//!
//! The planner reads the table's statistics, so the table is analysed
//! first, and the parked rows are few beside the rest: with many parked, a
//! sequential scan is the cheaper plan and the planner is right to take it
//! (2M rows on PostgreSQL 14: 10k parked go by the index in 5 ms; 1M by a
//! parallel scan of the table in 315 ms).

use std::str::FromStr;

use ascetic_ddd_session::PgSessionPool;
use ascetic_ddd_session::pg::Identifier;
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};
use serde_json::Value;

use super::PgInbox;
use super::store::parked_counts_sql;
use super::surface::unpark_batch;
use crate::partition::ByStream;

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
        .max_size(2)
        .build()
        .expect("the pool can be built")
}

/// Every node of a plan, depth first.
fn nodes(plan: &Value) -> Vec<&Value> {
    let children = plan
        .get("Plans")
        .and_then(Value::as_array)
        .map(|plans| plans.iter().flat_map(nodes).collect())
        .unwrap_or_default();
    [vec![plan], children].concat()
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_parked_rows_are_counted_by_their_index_and_read_no_other_row() {
    let table = "inbox_parked_plan";
    let inbox = PgInbox::new(PgSessionPool::new(pool()))
        .with_table(
            Identifier::new(table).unwrap(),
            Identifier::new("inbox_parked_plan_seq").unwrap(),
        )
        .partitioned_by(ByStream);
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    let client = pool().get().await.unwrap();
    // 4,000 rows processed on two channels, three keys each; then 20 parked
    // on both, of every ground.
    client
        .batch_execute(&format!(
            "INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, payload, \
                 metadata, processed_position) \
             SELECT 'tenant-1', 'orders.Order', to_jsonb('o-' || g), 1, \
                 'kafka://' || (ARRAY['orders', 'pay'])[1 + g % 2] || '/k' || (g % 3), 'x', '{{}}', g \
             FROM generate_series(1, 4000) g; \
             UPDATE {table} SET processed_position = NULL, parked_at = now(), \
                 unreadable_at = CASE WHEN received_position % 3 = 0 THEN now() END, \
                 waiting_for = CASE WHEN received_position % 3 = 1 THEN '{{\"stream_id\": \"x\"}}'::jsonb END \
             WHERE received_position <= 20;"
        ))
        .await
        .unwrap();
    // A statement of its own: a batch of several runs as one transaction,
    // which VACUUM refuses.
    client
        .batch_execute(&format!("VACUUM ANALYZE {table}"))
        .await
        .unwrap();
    let explained = client
        .query_one(
            &format!(
                "EXPLAIN (ANALYZE, FORMAT JSON) {}",
                parked_counts_sql(table)
            ),
            &[],
        )
        .await
        .unwrap()
        .get::<_, Value>(0);
    let plan = &explained[0]["Plan"];
    let nodes = nodes(plan);
    let kinds: Vec<&str> = nodes
        .iter()
        .filter_map(|node| node["Node Type"].as_str())
        .collect();
    assert!(!kinds.contains(&"Seq Scan"), "{kinds:?}");
    assert!(
        nodes
            .iter()
            .any(|node| node["Index Name"].as_str() == Some(&format!("{table}__parked_idx"))),
        "the parked index: {plan}"
    );
    let fetched: Vec<u64> = nodes
        .iter()
        .filter(|node| {
            matches!(
                node["Node Type"].as_str(),
                Some("Bitmap Heap Scan" | "Index Scan")
            )
        })
        .filter_map(|node| node["Actual Rows"].as_u64())
        .collect();
    assert_eq!(fetched, [20], "the rows read are the parked ones: {plan}");
    inbox.teardown().await.unwrap();
}

/// A batch of a bulk unpark walks the parked index in its channel's order
/// and probes each candidate's stream by the primary key: its work is the
/// channel's parked rows, not the table's (ADR-0040 decision 8). The scale
/// the host runs at is measured on its drill (ADR-0040 § Owed).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_bulk_unpark_walks_the_parked_index_and_reads_no_other_row() {
    let table = "inbox_unpark_plan";
    let inbox = PgInbox::new(PgSessionPool::new(pool()))
        .with_table(
            Identifier::new(table).unwrap(),
            Identifier::new("inbox_unpark_plan_seq").unwrap(),
        )
        .partitioned_by(ByStream);
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    let client = pool().get().await.unwrap();
    // 20,000 rows processed on two channels, fifty Tenants; then 200 of
    // `orders` parked on the failure ground, one in a hundred for a refused
    // context, the rest for a key gone, and 2,000 of `pay` beside them: a
    // walk past the channel's own would read those too.
    client
        .batch_execute(&format!(
            "INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, payload, \
                 metadata, processed_position) \
             SELECT 'tenant-' || (g % 50), 'orders.Order', to_jsonb('o-' || g), 1, \
                 'kafka://' || (ARRAY['orders', 'pay'])[1 + g % 2] || '/k' || (g % 3), 'x', '{{}}', g \
             FROM generate_series(1, 20000) g; \
             UPDATE {table} SET processed_position = NULL, parked_at = now(), \
                 parked_cause = CASE WHEN received_position % 100 = 0 THEN 'kms.refused' \
                                     ELSE 'kms.gone' END \
             WHERE (received_position % 2 = 0 AND received_position <= 400) \
                OR (received_position % 2 = 1 AND received_position <= 4000);"
        ))
        .await
        .unwrap();
    client
        .batch_execute(&format!("VACUUM ANALYZE {table}"))
        .await
        .unwrap();
    let as_of: String = client
        .query_one("SELECT statement_timestamp()::text", &[])
        .await
        .unwrap()
        .get(0);
    let explained = client
        .query_one(
            &format!("EXPLAIN (ANALYZE, FORMAT JSON) {}", unpark_batch(table)),
            &[
                &"kafka://orders",
                &i64::MIN,
                &as_of,
                &3600.0_f64,
                &Some("kms.refused"),
                &None::<&str>,
                &100_i64,
                &4_i32,
                &false,
            ],
        )
        .await
        .unwrap()
        .get::<_, Value>(0);
    let plan = &explained[0]["Plan"];
    let nodes = nodes(plan);
    let scans: Vec<(&str, &str)> = nodes
        .iter()
        .filter(|node| node["Relation Name"].as_str() == Some(table))
        .map(|node| {
            (
                node["Node Type"].as_str().unwrap_or("?"),
                node["Index Name"].as_str().unwrap_or("-"),
            )
        })
        .collect();
    assert!(
        scans.iter().all(|(kind, _)| *kind != "Seq Scan"),
        "{scans:?}: {plan}"
    );
    assert!(
        scans
            .iter()
            .any(|(_, index)| *index == format!("{table}__parked_idx")),
        "the parked index: {scans:?}"
    );
    let walked: u64 = nodes
        .iter()
        .filter(|node| node["Index Name"].as_str() == Some(&format!("{table}__parked_idx")))
        .filter_map(|node| node["Actual Rows"].as_u64())
        .sum();
    assert!(
        walked <= 200,
        "the rows walked are the channel's parked ones, not the table's: {walked}"
    );
    inbox.teardown().await.unwrap();
}
