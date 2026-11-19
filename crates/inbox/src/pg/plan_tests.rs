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

use super::store::{
    EXPIRY_BATCH, expire_sql, held_counts_sql, parked_counts_sql, take_sql, waiting_counts_sql,
    walk_sql,
};
use super::surface::unpark_batch;
use super::verbs::{park_heads_batch, park_queue_batch};
use super::{Outcome, PgInbox};
use crate::message::InboxMessage;
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
                // a write is no read: the rows it writes are its child's
                matches!(
                    kind,
                    "Index Scan"
                        | "Index Only Scan"
                        | "Bitmap Heap Scan"
                        | "Tid Scan"
                        | "ModifyTable"
                ),
                "the table is read by a {kind}: {node}"
            );
        }
        // the inbox table's own indexes: its key and the `{table}__` ones;
        // `{table}_slots_pkey` is the slots' and may be read
        if let Some(index) = node["Index Name"].as_str()
            && (index == format!("{table}_pk") || index.starts_with(&format!("{table}__")))
        {
            assert!(indexes.iter().any(|i| i == index), "{index}: {node}");
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
        let nothing = serde_json::json!({});
        let walk = explain(
            &walk_sql(&table),
            &[&slot, &32i32, &1.0f64, &none, &nothing],
        )
        .await;
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
                 PREPARE generic_walk (smallint, integer, double precision, text[], jsonb) AS {};",
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
        let walk =
            explain_generic(&format!("generic_walk ({slot}, 32, 1.0, '{{}}', '{{}}')")).await;
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

/// V1 (ADR-0042 decision 8): a statement of `park_queue` reads the Tenant's
/// queue rows alone, probed slot by slot by `__tenant_head_idx`, at 64
/// slots beside a thousand other Tenants' rows and its own processed
/// history of 10,000: no scan of the table, no sort of more than the
/// Tenant's queue. Run in a transaction rolled back:
/// the statement parks what it reads. Red when the slots are not part of
/// the probe: the index is then read whole.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_queue_given_up_is_read_by_its_tenant_s_index() {
    let table = "inbox_park_queue_plan";
    let inbox = PgInbox::new(PgSessionPool::new(pool()))
        .with_table(
            Identifier::new(table).unwrap(),
            Identifier::new("inbox_park_queue_plan_seq").unwrap(),
        )
        .with_slots(64)
        .partitioned_by(ByStream);
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    let client = pool().get().await.unwrap();
    client
        .batch_execute(&format!(
            "INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, payload, \
                 metadata) \
             SELECT 't' || (g % 1000), 'orders.Order', to_jsonb('s-' || g), 1, 'kafka://orders/k', \
                 'x', '{{}}' FROM generate_series(1, 20000) g; \
             INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, payload, \
                 metadata) \
             SELECT 'tx', 'orders.Order', to_jsonb('x-' || g), 1, 'kafka://orders/k', 'x', '{{}}' \
             FROM generate_series(1, 50) g; \
             INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, payload, \
                 metadata, processed_position) \
             SELECT 'tx', 'orders.Order', to_jsonb('h-' || g), 1, 'kafka://orders/k', 'x', '{{}}', g \
             FROM generate_series(1, 10000) g"
        ))
        .await
        .unwrap();
    client
        .batch_execute(&format!("VACUUM ANALYZE {table}"))
        .await
        .unwrap();
    let slots: Vec<i16> = (0..64).collect();
    client.batch_execute("BEGIN").await.unwrap();
    let explained = client
        .query_one(
            &format!("EXPLAIN (ANALYZE, FORMAT JSON) {}", park_queue_batch(table)),
            &[
                &"kafka://orders",
                &"tx",
                &"operator.given_up",
                &100_i64,
                &slots,
            ],
        )
        .await
        .unwrap()
        .get::<_, Value>(0);
    client.batch_execute("ROLLBACK").await.unwrap();
    let plan = &explained[0]["Plan"];
    by_the_indexes(plan, table, &[format!("{table}__tenant_head_idx")], 50.0);
    let read: f64 = nodes(plan)
        .iter()
        .filter(|node| node["Index Name"].as_str() == Some(&format!("{table}__tenant_head_idx")))
        .filter_map(|node| node["Actual Rows"].as_f64())
        .sum();
    assert!(
        read > 0.0 && read <= 50.0,
        "the Tenant's rows alone: {read} {plan}"
    );
    inbox.teardown().await.unwrap();
}

/// V1 (ADR-0042 decision 8): a statement of `park_heads` reads a batch of
/// `__held_idx` in its order and sorts nothing, whatever the heads held
/// under its cause: 4,000 of them here, against a batch of 100. Run in a
/// transaction rolled back. Red when the batch is ordered otherwise than the
/// index: every held head of the cause is then read and sorted.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_heads_given_up_are_read_a_batch_at_a_time_by_their_index() {
    let table = "inbox_park_heads_plan";
    let inbox = PgInbox::new(PgSessionPool::new(pool()))
        .with_table(
            Identifier::new(table).unwrap(),
            Identifier::new("inbox_park_heads_plan_seq").unwrap(),
        )
        .partitioned_by(ByStream);
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    let client = pool().get().await.unwrap();
    client
        .batch_execute(&format!(
            "INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, payload, \
                 metadata, held_since, next_attempt_at, hold_kind, holds, cause) \
             SELECT 't' || g, 'orders.Order', to_jsonb('o-' || g), 1, 'kafka://orders/k', 'x', '{{}}', \
                 now() - g * interval '1 second', now() + interval '1 hour', 'unavailable', 1, \
                 'kms.unavailable' FROM generate_series(1, 4000) g; \
             INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, payload, \
                 metadata, processed_position) \
             SELECT 'h' || (g % 100), 'orders.Order', to_jsonb('h-' || g), 1, 'kafka://orders/k', \
                 'x', '{{}}', g FROM generate_series(1, 40000) g"
        ))
        .await
        .unwrap();
    client
        .batch_execute(&format!("VACUUM ANALYZE {table}"))
        .await
        .unwrap();
    client.batch_execute("BEGIN").await.unwrap();
    let explained = client
        .query_one(
            &format!("EXPLAIN (ANALYZE, FORMAT JSON) {}", park_heads_batch(table)),
            &[
                &"kafka://orders",
                &"kms.unavailable",
                &None::<&str>,
                &100_i64,
            ],
        )
        .await
        .unwrap()
        .get::<_, Value>(0);
    client.batch_execute("ROLLBACK").await.unwrap();
    let plan = &explained[0]["Plan"];
    by_the_indexes(plan, table, &[format!("{table}__held_idx")], 100.0);
    // the candidates' read and the locked rows' recheck, each a batch at most
    let read: Vec<f64> = nodes(plan)
        .iter()
        .filter(|node| node["Index Name"].as_str() == Some(&format!("{table}__held_idx")))
        .filter_map(|node| node["Actual Rows"].as_f64())
        .collect();
    assert!(
        !read.is_empty() && read.iter().all(|rows| *rows <= 100.0),
        "a batch of the index: {read:?} {plan}"
    );
    inbox.teardown().await.unwrap();
}

/// § 6 (ADR-0042 decision 6): the waiting rows are counted by
/// `__waiting_since_idx`, and no row but the waiting is read.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_waiting_rows_are_counted_by_their_index() {
    let table = "inbox_waiting_plan";
    let inbox = PgInbox::new(PgSessionPool::new(pool()))
        .with_table(
            Identifier::new(table).unwrap(),
            Identifier::new("inbox_waiting_plan_seq").unwrap(),
        )
        .partitioned_by(ByStream);
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    let client = pool().get().await.unwrap();
    client
        .batch_execute(&format!(
            "INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, payload, \
                 metadata, processed_position, waiting_for, waiting_since) \
             SELECT 'tenant-' || (g % 500), 'orders.Order', to_jsonb('o-' || g), 1, \
                 'kafka://' || (ARRAY['orders', 'pay'])[1 + g % 2] || '/k', 'x', '{{}}', \
                 CASE WHEN g % 50 <> 0 THEN g END, \
                 CASE WHEN g % 50 = 0 THEN '{{\"tenant_id\": \"never\"}}'::jsonb END, \
                 CASE WHEN g % 50 = 0 THEN now() END \
             FROM generate_series(1, 100000) g"
        ))
        .await
        .unwrap();
    client
        .batch_execute(&format!("VACUUM ANALYZE {table}"))
        .await
        .unwrap();
    let explained = client
        .query_one(
            &format!(
                "EXPLAIN (ANALYZE, FORMAT JSON) {}",
                waiting_counts_sql(table)
            ),
            &[],
        )
        .await
        .unwrap()
        .get::<_, Value>(0);
    let plan = &explained[0]["Plan"];
    let nodes = nodes(plan);
    assert!(
        nodes
            .iter()
            .all(|node| node["Node Type"].as_str() != Some("Seq Scan")),
        "{plan}"
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
    assert_eq!(fetched, [2000.0], "the waiting rows alone: {plan}");
    inbox.teardown().await.unwrap();
}

/// E1 (ADR-0042 decision 7): a statement of the expiry examines at most a
/// batch of the waiters, by `__waiting_since_idx` in its keyset's order,
/// and follows each one's chain by the primary key, a probe a hop: no scan
/// of the table, no sort of more than a batch, whatever the waiters behind
/// the batch and the rows the expiry parked before. Its generic plan, the
/// one its function runs, is held to the same. Red when the batch bounds
/// the rows parked rather than those examined: the index then reads every
/// waiter past `max_wait`.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_expiry_examines_a_batch_by_its_index() {
    let table = "inbox_expiry_plan";
    let inbox = PgInbox::new(PgSessionPool::new(pool()))
        .with_table(
            Identifier::new(table).unwrap(),
            Identifier::new("inbox_expiry_plan_seq").unwrap(),
        )
        .partitioned_by(ByStream);
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    let client = pool().get().await.unwrap();
    // 20,000 processed rows of history; 5,000 waiters past `max_wait`, each
    // on a chain of two hops to a queued row; 2,000 rows the expiry parked
    let identity = |stream: &str| {
        format!(
            "jsonb_build_object('tenant_id', 'tenant-1', 'stream_type', 'orders.Order', \
             'stream_id', to_jsonb({stream}), 'stream_position', 1)"
        )
    };
    client
        .batch_execute(&format!(
            "INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, payload, \
                 metadata, processed_position) \
             SELECT 'tenant-1', 'orders.Order', to_jsonb('h-' || g), 1, 'kafka://orders/h', 'x', \
                 '{{}}', g FROM generate_series(1, 20000) g; \
             INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, payload, \
                 metadata) \
             SELECT 'tenant-1', 'orders.Order', to_jsonb('q-' || g), 1, 'kafka://orders/q', 'x', \
                 '{{}}' FROM generate_series(1, 5000) g; \
             INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, payload, \
                 metadata, waiting_for, waiting_since) \
             SELECT 'tenant-1', 'orders.Order', to_jsonb('m-' || g), 1, 'kafka://orders/m', 'x', \
                 '{{}}', {mid}, now() - interval '3 hours' \
             FROM generate_series(1, 5000) g; \
             INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, payload, \
                 metadata, waiting_for, waiting_since) \
             SELECT 'tenant-1', 'orders.Order', to_jsonb('w-' || g), 1, 'kafka://orders/w', 'x', \
                 '{{}}', {head}, now() - interval '2 hours' + g * interval '1 millisecond' \
             FROM generate_series(1, 5000) g; \
             INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, payload, \
                 metadata, waiting_for, parked_at) \
             SELECT 'tenant-1', 'orders.Order', to_jsonb('e-' || g), 1, 'kafka://orders/e', 'x', \
                 '{{}}', {never}, now() FROM generate_series(1, 2000) g",
            mid = identity("'q-' || g"),
            head = identity("'m-' || g"),
            never = identity("'never'::text"),
        ))
        .await
        .unwrap();
    client
        .batch_execute(&format!("VACUUM ANALYZE {table}"))
        .await
        .unwrap();
    let indexes = [format!("{table}__waiting_since_idx"), format!("{table}_pk")];
    let held = |plan: &Value| {
        by_the_indexes(plan, table, &indexes, f64::from(EXPIRY_BATCH));
        let nodes = nodes(plan);
        let examined: Vec<f64> = nodes
            .iter()
            .filter(|node| {
                node["Index Name"].as_str() == Some(&format!("{table}__waiting_since_idx"))
            })
            .filter_map(|node| node["Actual Rows"].as_f64())
            .collect();
        assert!(!examined.is_empty(), "the waiting index: {plan}");
        assert!(
            examined.iter().all(|rows| *rows <= f64::from(EXPIRY_BATCH)),
            "a batch examined at most: {examined:?} {plan}"
        );
        // a hop is one probe of the key, its position among the key's
        // columns, not a walk of its stream's history
        let probes: Vec<&str> = nodes
            .iter()
            .filter(|node| node["Index Name"].as_str() == Some(&format!("{table}_pk")))
            .filter_map(|node| node["Index Cond"].as_str())
            .collect();
        assert!(!probes.is_empty(), "the hops probe the key: {plan}");
        assert!(
            probes.iter().all(|cond| cond.contains("stream_position")),
            "{probes:?}"
        );
        nodes.iter().any(|node| {
            node["Index Cond"]
                .as_str()
                .is_some_and(|cond| cond.contains("$3"))
        })
    };
    let explained = client
        .query_one(
            &format!("EXPLAIN (ANALYZE, FORMAT JSON) {}", expire_sql(table)),
            &[
                &3600.0f64,
                // a `LIMIT` sent as text is a bigint; the function's is its integer argument
                &i64::from(EXPIRY_BATCH),
                &None::<std::time::SystemTime>,
                &None::<i64>,
            ],
        )
        .await
        .unwrap()
        .get::<_, Value>(0);
    held(&explained[0]["Plan"]);
    client
        .batch_execute(&format!(
            "SET plan_cache_mode = force_generic_plan; \
             PREPARE generic_expire (double precision, integer, timestamptz, bigint) AS {};",
            expire_sql(table)
        ))
        .await
        .unwrap();
    let generic = client
        .query_one(
            "EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE generic_expire (3600, 100, NULL, NULL)",
            &[],
        )
        .await
        .unwrap()
        .get::<_, Value>(0);
    assert!(
        held(&generic[0]["Plan"]),
        "a generic plan, the key a parameter of the index's condition: {generic}"
    );
    client
        .batch_execute("DEALLOCATE ALL; RESET plan_cache_mode")
        .await
        .unwrap();
    inbox.teardown().await.unwrap();
}

/// The take, the walk and the expiry are planned once a connection
/// (ADR-0041 decision 6, ADR-0042 decision 7): the walk's and the expiry's
/// functions force the generic plan, so that a session asking for custom
/// plans, as the specification crate's queries need (ADR-0027), does not
/// plan them at every call; the take's statement has no parameter.
/// Counted from the server's own report of each plan it makes
/// (`debug_print_plan`), on a connection of the test's, call by call: a
/// call is planned — it is a statement sent as text — and a function's
/// statement within it is planned when the function's plan is not kept.
///
/// Once a connection means once a reset of its plan cache: another session
/// that creates a schema — the first temporary table of a backend's slot in
/// a database does — resets every kept plan of every session of the
/// database, which the suite's siblings do at their start. A canary, a
/// function whose statement has no parameter, counts the resets: each
/// plans the canary's statement again as it does the others'. So each of
/// the three is planned in at most one call more than the canary, the
/// canary first in the round. Under resets in more than half of the
/// rounds the count cannot tell a kept plan from one made at every call:
/// the ten rounds are run again, up to five times, the siblings' resets
/// coming in bursts at their start. Red when the walk's or the expiry's
/// `SET plan_cache_mode` is dropped, or the take's statement given a
/// parameter: then planned at each of its ten calls.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_take_the_walk_and_the_expiry_are_planned_once_a_connection() {
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
    // A notice is handed on before the connection reads on to the call's
    // result: what a call made is reported by the time the call returns.
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
             CREATE OR REPLACE FUNCTION {table}_canary() RETURNS bigint LANGUAGE plpgsql AS \
                 $$ BEGIN RETURN (SELECT count(*) FROM {table}_meta); END $$; \
             SET plan_cache_mode = force_custom_plan; SET client_min_messages = log; \
             SET debug_print_plan = on;"
        ))
        .await
        .unwrap();
    // The calls since the last look whose function's statement was planned:
    // two plans, the call's and the statement's.
    let mut planned = || {
        let mut plans = 0;
        while let Ok(notice) = reported.try_recv() {
            plans += usize::from(notice == "plan:");
        }
        assert!(plans == 1 || plans == 2, "a call made {plans} plans");
        usize::from(plans == 2)
    };
    const CALLS: usize = 10;
    let mut counts = Vec::new();
    for _ in 0..5 {
        let (mut canary, mut take, mut walk, mut expire) = (0, 0, 0, 0);
        for _ in 0..CALLS {
            client
                .query_one(&format!("SELECT {table}_canary()"), &[])
                .await
                .unwrap();
            canary += planned();
            let row = client
                .query_one(&format!("SELECT * FROM {table}__take()"), &[])
                .await
                .unwrap();
            assert_eq!(row.get::<_, Option<i16>>(0), Some(0));
            take += planned();
            let row = client
            .query_one(
                &format!(
                    "SELECT * FROM {table}__walk($1::smallint, $2::integer, $3::double precision, \
                     $4::text[], $5::jsonb)"
                ),
                &[&0i16, &32i32, &1.0f64, &Vec::<String>::new(), &serde_json::json!({})],
            )
            .await
            .unwrap();
            assert_eq!(row.get::<_, String>(5), "taken");
            walk += planned();
            let rows = client
                .query(
                    &format!(
                        "SELECT * FROM {table}__expire($1::double precision, $2::integer, \
                     $3::timestamptz, $4::bigint)"
                    ),
                    &[
                        &3600.0f64,
                        &100i32,
                        &None::<std::time::SystemTime>,
                        &None::<i64>,
                    ],
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 1, "no row parked, the key's row");
            expire += planned();
        }
        counts.push((canary, take, walk, expire));
        if canary <= CALLS / 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    client
        .batch_execute(&format!(
            "RESET debug_print_plan; RESET client_min_messages; DROP FUNCTION {table}_canary()"
        ))
        .await
        .unwrap();
    drop(client);
    driven.await.unwrap();
    let Some(&(canary, take, walk, expire)) =
        counts.iter().find(|(canary, ..)| *canary <= CALLS / 2)
    else {
        panic!(
            "the plan cache was reset in more than half of the rounds of every attempt: \
             the count cannot tell; the canary, the take, the walk and the expiry planned \
             in: {counts:?}"
        );
    };
    assert!(
        take <= canary + 1 && walk <= canary + 1 && expire <= canary + 1,
        "the take, the walk and the expiry planned in {take}, {walk} and {expire} calls, \
         the canary in {canary}"
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
                &"failure",
                // the window's end and the slots the batch holds: all of them
                &i64::MAX,
                &vec![0_i16],
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

/// What the session sent, statement by statement: the text of each, as the
/// session's observer is told it before it runs.
#[derive(Default)]
struct Sent(std::sync::Mutex<Vec<String>>);

impl ascetic_ddd_session::observer::SessionObserver for Sent {}

impl ascetic_ddd_session::pg::PgObserver for Sent {
    fn on_query_started(&self, event: &ascetic_ddd_session::pg::QueryStarted<'_>) {
        self.0.lock().unwrap().push(event.statement.to_owned());
    }
}

impl Sent {
    fn taken(&self) -> Vec<String> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

/// A happy dispatch's statements, at an absolute count (ADR-0042 B1),
/// counted by the session's observer, which every statement of the library
/// goes through. At one slot, ten: `BEGIN`; the take, which brings the
/// transaction's isolation with it (ADR-0042 decision 10); the walk; the
/// row read in a savepoint and its `RELEASE`; the subscriber's savepoint
/// and its `RELEASE`; the mark, which wakes the rows waiting for it in the
/// same statement; `COMMIT`. At more than one slot, as a host cuts its
/// table, eleven: the mark first locks its row's identity, which a waiter
/// in another slot locks too (ADR-0009). Statements, not round trips: the
/// driver prepares a statement sent as text, a round trip more. The count
/// is the dispatch's alone: a first dispatch, on an empty table, runs
/// before it, and nothing is paused or reported. Red when the take's
/// isolation is read by a statement of its own.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_happy_dispatch_sends_its_statements_and_no_more() {
    for (slots, statements) in HAPPY_STATEMENTS {
        happy_dispatch_sends(slots, statements).await;
    }
}

async fn happy_dispatch_sends(slots: u32, sends: usize) {
    let sent = std::sync::Arc::new(Sent::default());
    let table = format!("inbox_happy_statements_{slots}");
    let inbox = PgInbox::new(PgSessionPool::new(pool()).observed_by(std::sync::Arc::clone(&sent)))
        .with_table(
            Identifier::new(&table).unwrap(),
            Identifier::new(format!("{table}_seq")).unwrap(),
        )
        .with_slots(slots)
        .partitioned_by(ByStream);
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    let subscriber = |_: &_, _: &InboxMessage| std::future::ready(Ok(()));
    assert_eq!(inbox.dispatch(subscriber).await.unwrap(), Outcome::Nothing);
    crate::port::Inbox::publish(
        &inbox,
        &InboxMessage::new(
            "tenant-1",
            "orders.Order",
            serde_json::json!({ "id": "o-1" }),
            1,
            "kafka://orders/o-1",
            "x",
        )
        .with_metadata(serde_json::json!({
            "message_id": "00000000-0000-4000-8000-000000000001"
        })),
    )
    .await
    .unwrap();
    sent.taken();
    assert_eq!(
        inbox.dispatch(subscriber).await.unwrap(),
        Outcome::Processed
    );
    let statements = sent.taken();
    let heads: Vec<&str> = statements
        .iter()
        .map(|statement| statement.split_whitespace().next().unwrap_or_default())
        .collect();
    assert_eq!(statements.len(), sends, "at {slots} slots: {statements:#?}");
    assert_eq!(heads.first(), Some(&"BEGIN"), "{statements:#?}");
    assert_eq!(heads.last(), Some(&"COMMIT"), "{statements:#?}");
    inbox.teardown().await.unwrap();
}

/// The statements of a happy dispatch, as B1 counts them, at one slot and
/// at 64.
const HAPPY_STATEMENTS: [(u32, usize); 2] = [(1, 10), (64, 11)];

/// What finding a message costs in B1's shape, at absolute figures
/// (ADR-0042 B1): the shared buffers the take and the walk read, run as
/// their functions run them — generic plans, planned before — and the
/// walk's steps, at the host's 64 slots over a thousand Tenants' queues and
/// one Tenant's processed history: some 31 Tenants and 34 queue rows a
/// slot, the queue's indexes of two levels. At the host's 2,000 Tenants a
/// slot they have three, a look reads a buffer more, and the figures grow
/// so; the walk at that scale is M1's, measured by hand. The walk starts at a cursor before two Tenants
/// waiting for their back-off and a third due, so it steps three times and
/// takes the third's head; bounded at two, it passes the two. Each statement runs once before it is counted,
/// and each in a transaction rolled back, so that the count is the
/// execution's: not its planning, nor a first read of an index's metapage.
/// The table is analysed and never vacuumed: a probe of an index fetches
/// the heap, as a queue's new rows make it do. The figures, on 14 and 18
/// alike:
/// - the take, 198: its look at the whole queue, 3 — an index's root and
///   leaf, and the row's heap page — and 3 for each slot's newest queue row,
///   192; the slots' page, the meta's, and the slot's row locked;
/// - the walk, 24: the slot's row read and written, 4; three steps of two
///   looks each, 3 a look, 18; the head locked by its `ctid`, 2;
/// - the same walk bounded at two, which passes the two waiting and takes
///   nothing, 19: the slot's row, 4; two steps, 12; the slot's newest queue
///   row, where the round it begins ends, 3;
/// - the take of an idle inbox, its queue empty beside a processed history,
///   2: its one look at the whole queue, an empty index's page, and the
///   meta's; no slot's row and no slot's look.
///
/// The take's looks go by `__head_idx` alone, its index-only scans: a heap
/// page they fetch here, the visibility map empty, the host's all-visible
/// pages spare. A planner's choice moves the figures, and so does a heap
/// page or a slot's row read once more to find a message or to pass a
/// Tenant. Red when the walk reads its head's row again for the channel, or
/// its slot's row again for the round; when a take's look reads a column
/// outside its index; when an idle take looks at every slot.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_message_is_found_at_its_buffers_and_the_walk_at_its_steps() {
    let table = "inbox_found";
    let inbox = PgInbox::new(PgSessionPool::new(pool()))
        .with_table(
            Identifier::new(table).unwrap(),
            Identifier::new("inbox_found_seq").unwrap(),
        )
        .with_slots(64)
        .partitioned_by(ByStream);
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    let client = pool().get().await.unwrap();
    // 10,000 processed rows of one Tenant and 200 of its rows queued; a
    // thousand Tenants with two rows queued each, two in three of them
    // waiting an hour for their back-off.
    client
        .batch_execute(&format!(
            "ALTER TABLE {table} SET (autovacuum_enabled = off); \
             ALTER TABLE {table}_slots SET (autovacuum_enabled = off); \
             INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, \
                 payload, metadata, processed_position) \
             SELECT 'tbig', 'orders.Order', to_jsonb('h-' || g), 1, 'kafka://orders/h', 'x', \
                 '{{}}', g FROM generate_series(1, 10000) g; \
             INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, \
                 payload, metadata) \
             SELECT 'tbig', 'orders.Order', to_jsonb('q-' || g), 1, 'kafka://orders/q', 'x', \
                 '{{}}' FROM generate_series(1, 200) g; \
             INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, \
                 payload, metadata, attempts, next_attempt_at) \
             SELECT 't' || lpad((g / 2)::text, 4, '0'), 'orders.Order', to_jsonb('s-' || g), 1, \
                 'kafka://orders/s', 'x', '{{}}', \
                 CASE WHEN (g / 2) % 3 = 0 THEN 0 ELSE 1 END, \
                 CASE WHEN (g / 2) % 3 = 0 THEN NULL \
                      ELSE CURRENT_TIMESTAMP + interval '1 hour' END \
             FROM generate_series(0, 1999) g; \
             ANALYZE {table}; ANALYZE {table}_slots"
        ))
        .await
        .unwrap();
    // In slot 0, the Tenant just before two waiting and a third due.
    let (cursor, due): (String, String) = {
        let row = client
            .query_one(
                &format!(
                    "WITH heads AS ( \
                         SELECT DISTINCT ON (tenant_id) tenant_id, \
                                next_attempt_at IS NULL AS due \
                         FROM {table} WHERE slot = 0 AND processed_position IS NULL \
                         ORDER BY tenant_id, received_position), \
                     seq AS ( \
                         SELECT tenant_id, due, lag(tenant_id) OVER w AS before, \
                                lead(due) OVER w AS d1, lead(due, 2) OVER w AS d2, \
                                lead(tenant_id, 2) OVER w AS third \
                         FROM heads WINDOW w AS (ORDER BY tenant_id)) \
                     SELECT before::text, third::text FROM seq \
                     WHERE NOT due AND NOT d1 AND d2 AND before IS NOT NULL \
                     ORDER BY tenant_id LIMIT 1"
                ),
                &[],
            )
            .await
            .unwrap();
        (row.get(0), row.get(1))
    };
    client
        .batch_execute(&format!(
            "UPDATE {table}_slots SET last_tenant = '{cursor}' WHERE slot = 0; \
             SET plan_cache_mode = force_generic_plan; \
             PREPARE found_take AS {}; \
             PREPARE found_walk (smallint, integer, double precision, text[], jsonb) AS {};",
            take_sql(table),
            walk_sql(table)
        ))
        .await
        .unwrap();
    let walk = "EXECUTE found_walk (0, 32, 1.0, '{}', '{}')";
    client.batch_execute("BEGIN").await.unwrap();
    let taken = client.query_one(walk, &[]).await.unwrap();
    client.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        (taken.get::<_, Option<String>>(2), taken.get::<_, String>(5)),
        (Some(due.clone()), "taken".to_owned()),
        "the walk takes the third Tenant's head"
    );
    let explained = async |statement: &str| -> Value {
        let mut explained = Value::Null;
        for _ in 0..2 {
            client.batch_execute("BEGIN").await.unwrap();
            explained = client
                .query_one(
                    &format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {statement}"),
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            client.batch_execute("ROLLBACK").await.unwrap();
        }
        explained
    };
    let buffers = |explained: &Value| {
        let plan = &explained[0]["Plan"];
        plan["Shared Hit Blocks"].as_u64().unwrap_or_default()
            + plan["Shared Read Blocks"].as_u64().unwrap_or_default()
    };
    let steps = |explained: &Value| {
        (nodes(&explained[0]["Plan"])
            .iter()
            .find(|node| node["Node Type"].as_str() == Some("Recursive Union"))
            .and_then(|node| node["Actual Rows"].as_f64())
            .unwrap_or_default()
            // 18 prints a node's rows with two decimals, 14 as an integer
            .round() as u64)
            .saturating_sub(1)
    };
    // The same walk bounded at two: it passes the two waiting and takes
    // nothing, and decides the round the slot goes on with.
    let passing = "EXECUTE found_walk (0, 2, 1.0, '{}', '{}')";
    client.batch_execute("BEGIN").await.unwrap();
    let passed = client.query_one(passing, &[]).await.unwrap();
    client.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        passed.get::<_, String>(5),
        "passed",
        "the walk bounded at two"
    );
    let take = explained("EXECUTE found_take").await;
    let walked = explained(walk).await;
    let passed = explained(passing).await;
    assert_eq!(
        (steps(&walked), steps(&passed)),
        (3, 2),
        "the walks' steps:\n{walked}\n{passed}"
    );
    let looks: Vec<&str> = nodes(&take[0]["Plan"])
        .iter()
        .filter(|node| node["Index Name"].as_str() == Some(&format!("{table}__head_idx")))
        .map(|node| node["Node Type"].as_str().unwrap_or_default())
        .collect();
    assert!(
        !looks.is_empty() && looks.iter().all(|kind| *kind == "Index Only Scan"),
        "the take's looks by the index alone: {looks:?}\n{take}"
    );
    // An idle inbox: its queue empty, its history processed.
    let idle_table = "inbox_found_idle";
    let idle = PgInbox::new(PgSessionPool::new(pool()))
        .with_table(
            Identifier::new(idle_table).unwrap(),
            Identifier::new("inbox_found_idle_seq").unwrap(),
        )
        .with_slots(64)
        .partitioned_by(ByStream);
    idle.teardown().await.unwrap();
    idle.migrate().await.unwrap();
    client
        .batch_execute(&format!(
            "ALTER TABLE {idle_table} SET (autovacuum_enabled = off); \
             ALTER TABLE {idle_table}_slots SET (autovacuum_enabled = off); \
             INSERT INTO {idle_table} (tenant_id, stream_type, stream_id, stream_position, uri, \
                 payload, metadata, processed_position) \
             SELECT 't' || lpad((g % 1000)::text, 4, '0'), 'orders.Order', to_jsonb('h-' || g), \
                 1, 'kafka://orders/h', 'x', '{{}}', g FROM generate_series(1, 10000) g; \
             ANALYZE {idle_table}; ANALYZE {idle_table}_slots; \
             PREPARE idle_take AS {};",
            take_sql(idle_table)
        ))
        .await
        .unwrap();
    let idling = explained("EXECUTE idle_take").await;
    assert_eq!(
        (
            buffers(&take),
            buffers(&walked),
            buffers(&passed),
            buffers(&idling)
        ),
        FOUND,
        "the take, the walk, the walk that passes, the idle take:\n{take}\n{walked}\n{passed}\n{idling}"
    );
    client
        .batch_execute("DEALLOCATE ALL; RESET plan_cache_mode")
        .await
        .unwrap();
    idle.teardown().await.unwrap();
    inbox.teardown().await.unwrap();
}

/// The buffers the take, the walk, the walk that passes and the idle take
/// read in B1's shape.
const FOUND: (u64, u64, u64, u64) = (198, 24, 19, 2);
