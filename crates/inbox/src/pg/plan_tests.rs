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
use super::store::{held_counts_sql, parked_counts_sql, take_sql, walk_sql};
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
    // A number, not an integer: 18 prints "Actual Rows" with its fraction
    // (20.0), 14 without (20). Over one loop it is a whole count either way.
    let fetched: Vec<f64> = nodes
        .iter()
        .filter(|node| {
            matches!(
                node["Node Type"].as_str(),
                Some("Bitmap Heap Scan" | "Index Scan")
            )
        })
        .filter_map(|node| node["Actual Rows"].as_f64())
        .collect();
    assert_eq!(fetched, [20.0], "the rows read are the parked ones: {plan}");
    inbox.teardown().await.unwrap();
}

/// The heads held are counted by their index, and read no row but theirs:
/// 4,000 held of 100,000 on two channels, by both kinds. And the index
/// carries every column the count reads, so that the count can be answered
/// from it alone: with the scans that read the heap set off, the plan is an
/// index-only scan of it. Whether the planner takes that plan is its cost's,
/// by the share of the heap all visible, which open transactions elsewhere
/// hold down; what the index offers is the library's (ADR-0042 § 6 and
/// § Executors, R1). Red when the index lacks a column the count reads.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_held_heads_are_counted_by_their_index() {
    let table = "inbox_held_plan";
    let inbox = PgInbox::new(PgSessionPool::new(pool()))
        .with_table(
            Identifier::new(table).unwrap(),
            Identifier::new("inbox_held_plan_seq").unwrap(),
        )
        .partitioned_by(ByStream);
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    let client = pool().get().await.unwrap();
    client
        .batch_execute(&format!(
            "INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, payload, \
                 metadata, processed_position) \
             SELECT 'tenant-' || (g % 500), 'orders.Order', to_jsonb('o-' || g), 1, \
                 'kafka://' || (ARRAY['orders', 'pay'])[1 + g % 2] || '/k' || g, convert_to(repeat('x', 200), 'UTF8'), \
                 '{{}}', CASE WHEN g % 25 <> 0 THEN g END \
             FROM generate_series(1, 100000) g; \
             UPDATE {table} SET held_since = now(), next_attempt_at = now() + interval '1 minute', \
                 cause = CASE WHEN received_position % 50 = 0 THEN 'kms.unavailable' ELSE 'db.timeout' END, \
                 hold_kind = CASE WHEN received_position % 50 = 0 THEN 'unavailable' ELSE 'backoff' END, \
                 holds = CASE WHEN received_position % 50 = 0 THEN 1 END \
             WHERE processed_position IS NULL;"
        ))
        .await
        .unwrap();
    client
        .batch_execute(&format!("VACUUM ANALYZE {table}"))
        .await
        .unwrap();
    let explain = async |sql: &str| {
        client
            .query_one(&format!("EXPLAIN (ANALYZE, FORMAT JSON) {sql}"), &[])
            .await
            .unwrap()
            .get::<_, Value>(0)
    };
    let explained = explain(&held_counts_sql(table)).await;
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
            .any(|node| node["Index Name"].as_str() == Some(&format!("{table}__held_idx"))),
        "the held index: {plan}"
    );
    let fetched: Vec<f64> = nodes
        .iter()
        .filter(|node| {
            matches!(
                node["Node Type"].as_str(),
                Some("Bitmap Heap Scan" | "Index Scan" | "Index Only Scan")
            )
        })
        .filter_map(|node| node["Actual Rows"].as_f64())
        .collect();
    assert_eq!(fetched, [4000.0], "the rows read are the held ones: {plan}");
    // the heap's scans set off for this transaction alone
    client
        .batch_execute("BEGIN; SET LOCAL enable_seqscan = off; SET LOCAL enable_bitmapscan = off")
        .await
        .unwrap();
    let explained = explain(&held_counts_sql(table)).await;
    client.batch_execute("ROLLBACK").await.unwrap();
    let plan = &explained[0]["Plan"];
    assert!(
        self::nodes(plan).iter().any(|node| {
            node["Node Type"].as_str() == Some("Index Only Scan")
                && node["Index Name"].as_str() == Some(&format!("{table}__held_idx"))
        }),
        "the count answered from the held index alone: {plan}"
    );
    inbox.teardown().await.unwrap();
}

/// Holds a plan to the queue's indexes: every read of `table` goes by one
/// of `indexes` (or by a row's place), none scans the table or reads its
/// primary key, and no sort orders more than `sorted` rows — the lateral
/// candidates of a walk's step, or the slots' rows — never the queue's.
fn by_the_indexes(plan: &Value, table: &str, indexes: &[String], sorted: f64) {
    for node in nodes(plan) {
        let kind = node["Node Type"].as_str().unwrap_or_default();
        if node["Relation Name"].as_str() == Some(table) {
            assert!(
                matches!(
                    kind,
                    "Index Scan" | "Index Only Scan" | "Bitmap Heap Scan" | "Tid Scan"
                ),
                "the table is read by a {kind}: {node}"
            );
        }
        // the inbox table's own indexes: its key and the `{table}__` ones;
        // `{table}_slots_pkey` is the slots' and may be read
        if let Some(index) = node["Index Name"].as_str() {
            if index == format!("{table}_pk") || index.starts_with(&format!("{table}__")) {
                assert!(indexes.iter().any(|i| i == index), "{index}: {node}");
            }
        }
        if kind == "Sort" {
            let rows = node["Actual Rows"].as_f64().unwrap_or_default();
            assert!(rows <= sorted, "a sort of {rows} rows: {node}");
        }
    }
}

/// E8 (ADR-0041): the take and the walk go by the queue's indexes, the
/// take by `__head_idx`, the walk by `__tenant_head_idx`, at one slot and at
/// 64, over a Tenant with a long processed history beside a thousand
/// others, and a Tenant the last `ANALYZE` did not see. Red when the walk's
/// index is dropped: its steps then sort a slot's queue, or scan it.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_take_and_the_walk_go_by_the_queue_s_indexes() {
    for slots in [1u32, 64] {
        let table = format!("inbox_walk_plan_{slots}");
        let inbox = PgInbox::new(PgSessionPool::new(pool()))
            .with_table(
                Identifier::new(&table).unwrap(),
                Identifier::new(format!("{table}_seq")).unwrap(),
            )
            .with_slots(slots)
            .partitioned_by(ByStream);
        inbox.teardown().await.unwrap();
        inbox.migrate().await.unwrap();
        let client = pool().get().await.unwrap();
        // 100,000 processed rows of one Tenant, 200 of its rows queued, and
        // a thousand Tenants with two queued each.
        client
            .batch_execute(&format!(
                "INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, \
                     payload, metadata, processed_position) \
                 SELECT 'tbig', 'orders.Order', to_jsonb('h-' || g), 1, 'kafka://orders/h', 'x', \
                     '{{}}', g FROM generate_series(1, 100000) g; \
                 INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, \
                     payload, metadata) \
                 SELECT 'tbig', 'orders.Order', to_jsonb('q-' || g), 1, 'kafka://orders/q', 'x', \
                     '{{}}' FROM generate_series(1, 200) g; \
                 INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, \
                     payload, metadata) \
                 SELECT 't' || lpad((g / 2)::text, 4, '0'), 'orders.Order', to_jsonb('s-' || g), 1, \
                     'kafka://orders/s', 'x', '{{}}' FROM generate_series(0, 1999) g;"
            ))
            .await
            .unwrap();
        client
            .batch_execute(&format!("VACUUM ANALYZE {table}"))
            .await
            .unwrap();
        client
            .batch_execute(&format!(
                "INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, \
                     payload, metadata) \
                 SELECT 'tnew', 'orders.Order', to_jsonb('n-' || g), 1, 'kafka://orders/n', 'x', \
                     '{{}}' FROM generate_series(1, 50) g;"
            ))
            .await
            .unwrap();
        let explain = async |sql: &str,
                             params: &[&(
                                    dyn ascetic_ddd_session::pg::tokio_postgres::types::ToSql + Sync
                                )]| {
            client
                .query_one(&format!("EXPLAIN (ANALYZE, FORMAT JSON) {sql}"), params)
                .await
                .unwrap()
                .get::<_, Value>(0)
        };
        let take = explain(&take_sql(&table), &[]).await;
        by_the_indexes(
            &take[0]["Plan"],
            &table,
            &[format!("{table}__head_idx")],
            f64::from(slots),
        );
        // The walk over the new Tenant's slot, from a cursor just before it.
        let slot: i16 = client
            .query_one(
                &format!("SELECT slot FROM {table} WHERE tenant_id = 'tnew' LIMIT 1"),
                &[],
            )
            .await
            .unwrap()
            .get(0);
        client
            .batch_execute(&format!(
                "UPDATE {table}_slots SET last_tenant = 'tm' WHERE slot = {slot}"
            ))
            .await
            .unwrap();
        let none: Vec<String> = Vec::new();
        let walk = explain(&walk_sql(&table), &[&slot, &32i32, &1.0f64, &none]).await;
        by_the_indexes(
            &walk[0]["Plan"],
            &table,
            &[
                format!("{table}__tenant_head_idx"),
                format!("{table}__head_idx"),
            ],
            3.0,
        );
        assert!(
            nodes(&walk[0]["Plan"])
                .iter()
                .any(|node| node["Index Name"].as_str()
                    == Some(&format!("{table}__tenant_head_idx"))),
            "the walk goes by its index: {walk}"
        );
        // The plans the functions run: generic, the walk's forced so and the
        // take's without a parameter to make it otherwise. Prepared with the
        // walk function's own argument types, from the same cursor.
        client
            .batch_execute(&format!(
                "UPDATE {table}_slots SET last_tenant = 'tm', round_from = NULL, \
                     dormant_until = NULL, dormant_after = NULL WHERE slot = {slot}; \
                 SET plan_cache_mode = force_generic_plan; \
                 PREPARE generic_take AS {}; \
                 PREPARE generic_walk (smallint, integer, double precision, text[]) AS {};",
                take_sql(&table),
                walk_sql(&table)
            ))
            .await
            .unwrap();
        let explain_generic = async |name: &str| {
            client
                .query_one(
                    &format!("EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE {name}"),
                    &[],
                )
                .await
                .unwrap()
                .get::<_, Value>(0)
        };
        let take = explain_generic("generic_take").await;
        by_the_indexes(
            &take[0]["Plan"],
            &table,
            &[format!("{table}__head_idx")],
            f64::from(slots),
        );
        let walk = explain_generic(&format!("generic_walk ({slot}, 32, 1.0, '{{}}')")).await;
        by_the_indexes(
            &walk[0]["Plan"],
            &table,
            &[
                format!("{table}__tenant_head_idx"),
                format!("{table}__head_idx"),
            ],
            3.0,
        );
        assert!(
            nodes(&walk[0]["Plan"])
                .iter()
                .any(|node| node["Index Name"].as_str()
                    == Some(&format!("{table}__tenant_head_idx"))),
            "the generic walk goes by its index: {walk}"
        );
        assert!(
            nodes(&walk[0]["Plan"]).iter().any(|node| node["Index Cond"]
                .as_str()
                .is_some_and(|cond| cond.contains("$1"))),
            "a generic plan, the slot a parameter in it: {walk}"
        );
        client
            .batch_execute("DEALLOCATE ALL; RESET plan_cache_mode")
            .await
            .unwrap();
        inbox.teardown().await.unwrap();
    }
}

/// The take and the walk are planned once a connection (ADR-0041 decision
/// 6): the walk's function forces the generic plan, so that a session asking
/// for custom plans, as the specification crate's queries need (ADR-0027),
/// does not plan it at every call; the take's statement has no parameter.
/// Counted from the server's own report of each plan it makes
/// (`debug_print_plan`), on a connection of the test's: ten calls of each,
/// each call planned — the call is a statement sent as text — and each
/// function's statement once. Red when the walk's `SET plan_cache_mode` is
/// dropped, or the take's statement given a parameter: then planned at each
/// of its ten.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_take_and_the_walk_are_planned_once_a_connection() {
    use ascetic_ddd_session::pg::tokio_postgres::{self, AsyncMessage};
    let table = "inbox_walk_planned";
    let inbox = PgInbox::new(PgSessionPool::new(pool()))
        .with_table(
            Identifier::new(table).unwrap(),
            Identifier::new("inbox_walk_planned_seq").unwrap(),
        )
        .partitioned_by(ByStream);
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    let url = std::env::var("ASCETIC_DDD_TEST_PG_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned());
    let (client, mut connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let (notices, mut reported) = tokio::sync::mpsc::unbounded_channel();
    let driven = tokio::spawn(async move {
        while let Some(message) =
            std::future::poll_fn(|context| connection.poll_message(context)).await
        {
            match message {
                Ok(AsyncMessage::Notice(notice)) => {
                    let _ = notices.send(notice.message().to_owned());
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });
    // A row of one Tenant in the one slot: every walk takes it, and nothing
    // writes the inbox's table to invalidate a plan.
    client
        .batch_execute(&format!(
            "ALTER TABLE {table} SET (autovacuum_enabled = off); \
             ALTER TABLE {table}_slots SET (autovacuum_enabled = off); \
             INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, \
                 payload, metadata) \
             VALUES ('tenant-1', 'orders.Order', '\"o-1\"', 1, 'kafka://orders/o', 'x', '{{}}'); \
             SET plan_cache_mode = force_custom_plan; SET client_min_messages = log; \
             SET debug_print_plan = on;"
        ))
        .await
        .unwrap();
    const CALLS: usize = 10;
    for _ in 0..CALLS {
        let row = client
            .query_one(&format!("SELECT * FROM {table}__take()"), &[])
            .await
            .unwrap();
        assert_eq!(row.get::<_, Option<i16>>(0), Some(0));
        let row = client
            .query_one(
                &format!(
                    "SELECT * FROM {table}__walk($1::smallint, $2::integer, $3::double precision, \
                     $4::text[])"
                ),
                &[&0i16, &32i32, &1.0f64, &Vec::<String>::new()],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(5), "taken");
    }
    client
        .batch_execute("RESET debug_print_plan; RESET client_min_messages")
        .await
        .unwrap();
    drop(client);
    driven.await.unwrap();
    let mut plans = 0;
    while let Ok(notice) = reported.try_recv() {
        plans += usize::from(notice == "plan:");
    }
    assert_eq!(
        plans,
        2 * CALLS + 2,
        "each call planned, and the take's and the walk's statements once"
    );
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
                 cause = CASE WHEN received_position % 100 = 0 THEN 'kms.refused' \
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
