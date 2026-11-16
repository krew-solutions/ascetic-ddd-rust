//! The measurements behind ADR-0041, M7 and M9: run by hand, on one machine
//! at a time, against a database of one's own; no gate runs them. `test-pg`
//! runs every ignored test of the library's `--lib`, so they return at once
//! unless `ASCETIC_DDD_MEASURE` is set. Their times compare only with each
//! other, on the machine they ran on (ADR-0041 § Measured):
//!
//! ```text
//! ASCETIC_DDD_MEASURE=1 ASCETIC_DDD_TEST_PG_URL=… cargo test -p ascetic-ddd-inbox --lib -- \
//!     --ignored --nocapture --test-threads=1 measure_tests
//! ```
//!
//! They explain and run the library's own statements, as `plan_tests.rs` does,
//! not copies a change would leave behind.

use std::str::FromStr;
use std::time::{Duration, Instant};

use ascetic_ddd_session::PgSessionPool;
use ascetic_ddd_session::pg::Identifier;
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};
use serde_json::Value;

use super::store::{take_sql, walk_sql};
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

/// Whether the measurements were asked for.
fn asked() -> bool {
    let asked = std::env::var_os("ASCETIC_DDD_MEASURE").is_some();
    if !asked {
        eprintln!("a measurement: set ASCETIC_DDD_MEASURE to run it");
    }
    asked
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

/// An explained statement's planning time.
fn planning(explained: &Value) -> f64 {
    explained[0]["Planning Time"].as_f64().unwrap_or_default()
}

/// An explained statement's execution time, its shared buffers hit and
/// read, and how many rows its recursive union made but the one at the
/// cursor, which names no head — a walk's steps.
fn cost(explained: &Value) -> (f64, u64, u64) {
    let plan = &explained[0]["Plan"];
    let buffers = plan["Shared Hit Blocks"].as_u64().unwrap_or_default()
        + plan["Shared Read Blocks"].as_u64().unwrap_or_default();
    let steps = (nodes(plan)
        .iter()
        .find(|node| node["Node Type"].as_str() == Some("Recursive Union"))
        .and_then(|node| node["Actual Rows"].as_f64())
        .unwrap_or_default() as u64)
        .saturating_sub(1);
    (
        explained[0]["Execution Time"].as_f64().unwrap_or_default(),
        buffers,
        steps,
    )
}

/// M7: the take and the walk on the scratch shape of ADR-0041 § Measured at
/// M7's 10,000 Tenants: 64 slots cut by stream, 60,000 rows processed at the
/// front, one Tenant with 150,000 rows queued, each a stream of its own,
/// and 10,000 Tenants with ten each — about 156 Tenants a slot. Measured
/// under ordinary load, every head due, and with nine heads in ten waiting
/// for their backoff, every slot's walk from the start, `K = 32`.
#[tokio::test]
#[ignore = "a measurement (ADR-0041 M7): run by hand"]
async fn m7_the_take_and_the_walk_at_ten_thousand_tenants() {
    if !asked() {
        return;
    }
    const SLOTS: u32 = 64;
    let table = "inbox_m7";
    let inbox = PgInbox::new(PgSessionPool::new(pool()))
        .with_table(
            Identifier::new(table).unwrap(),
            Identifier::new("inbox_m7_seq").unwrap(),
        )
        .with_slots(SLOTS)
        .partitioned_by(ByStream);
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    let client = pool().get().await.unwrap();
    client
        .batch_execute(&format!(
            "INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, payload, \
                 metadata, processed_position) \
             SELECT 'tbig', 'orders.Order', to_jsonb('h-' || g), 1, 'kafka://orders/h', 'x', '{{}}', g \
             FROM generate_series(1, 60000) g; \
             INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, payload, metadata) \
             SELECT 'tbig', 'orders.Order', to_jsonb('q-' || g), 1, 'kafka://orders/q', 'x', '{{}}' \
             FROM generate_series(1, 150000) g; \
             INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, payload, metadata) \
             SELECT 't' || lpad((g / 10)::text, 5, '0'), 'orders.Order', to_jsonb('s-' || g), 1, \
                 'kafka://orders/s', 'x', '{{}}' FROM generate_series(0, 99999) g;"
        ))
        .await
        .unwrap();
    client
        .batch_execute(&format!("VACUUM ANALYZE {table}"))
        .await
        .unwrap();
    let tenants: f64 = client
        .query_one(
            &format!(
                "SELECT avg(n)::float8 FROM (SELECT count(DISTINCT tenant_id) n FROM {table} \
                 WHERE processed_position IS NULL GROUP BY slot) s"
            ),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    println!("M7: {tenants:.0} Tenants a slot on average");
    let explain = async |sql: &str,
                         params: &[&(
                                dyn ascetic_ddd_session::pg::tokio_postgres::types::ToSql + Sync
                            )]| {
        client
            .query_one(
                &format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"),
                params,
            )
            .await
            .unwrap()
            .get::<_, Value>(0)
    };
    let reset = format!(
        "UPDATE {table}_slots SET last_tenant = NULL, round_from = NULL, dormant_until = NULL, \
         dormant_after = NULL"
    );
    for (case, setup) in [
        ("every head due", String::new()),
        (
            "nine heads in ten in backoff",
            format!(
                "UPDATE {table} SET next_attempt_at = now() + interval '1 hour', attempts = 1 \
                 WHERE (slot, tenant_id, received_position) IN ( \
                     SELECT slot, tenant_id, min(received_position) FROM {table} \
                     WHERE processed_position IS NULL GROUP BY slot, tenant_id) \
                   AND random() < 0.9; ANALYZE {table};"
            ),
        ),
    ] {
        client.batch_execute(&setup).await.unwrap();
        client.batch_execute(&reset).await.unwrap();
        let take = explain(&take_sql(table), &[]).await;
        let (take_ms, take_buffers, _) = cost(&take);
        let mut walks = Vec::new();
        let mut planned = Vec::new();
        for slot in 0..SLOTS as i16 {
            let walk = explain(&walk_sql(table), &[&slot, &32i32, &1.0f64]).await;
            planned.push(planning(&walk));
            walks.push(cost(&walk));
        }
        println!(
            "M7, {case}: planning the take {:.2} ms, a walk {:.2} ms on average",
            planning(&take),
            planned.iter().sum::<f64>() / planned.len() as f64
        );
        let due: i64 = client
            .query_one(
                &format!("SELECT count(*) FROM {table}_slots WHERE last_tenant IS NOT NULL AND round_from IS NULL AND dormant_until IS NULL"),
                &[],
            )
            .await
            .unwrap()
            .get(0);
        let n = walks.len() as f64;
        let mean = |f: &dyn Fn(&(f64, u64, u64)) -> f64| walks.iter().map(f).sum::<f64>() / n;
        println!(
            "M7, {case}: take {take_ms:.2} ms, {take_buffers} buffers; walk mean {:.2} ms, \
             {:.0} buffers, {:.1} steps ({} in all); {due} of {SLOTS} slots took a head",
            mean(&|w| w.0),
            mean(&|w| w.1 as f64),
            mean(&|w| w.2 as f64),
            walks.iter().map(|w| w.2).sum::<u64>(),
        );
    }
    // The walk's planning, paid at every call issued as text: the same
    // walk issued 50 times as text, then 50 times prepared once on the
    // connection, every head due again, timed at the client.
    client
        .batch_execute(&format!(
            "UPDATE {table} SET next_attempt_at = NULL, attempts = 0; {reset}"
        ))
        .await
        .unwrap();
    let sql = walk_sql(table);
    let started = Instant::now();
    for i in 0..50i16 {
        let slot = i % SLOTS as i16;
        client
            .query_one(&sql, &[&slot, &32i32, &1.0f64])
            .await
            .unwrap();
    }
    let as_text = started.elapsed();
    let prepared = client.prepare(&sql).await.unwrap();
    let started = Instant::now();
    for i in 0..50i16 {
        let slot = i % SLOTS as i16;
        client
            .query_one(&prepared, &[&slot, &32i32, &1.0f64])
            .await
            .unwrap();
    }
    let as_prepared = started.elapsed();
    println!(
        "M7, the walk issued: as text {:.2} ms a call, prepared once {:.2} ms",
        as_text.as_secs_f64() * 1000.0 / 50.0,
        as_prepared.as_secs_f64() * 1000.0 / 50.0
    );
    inbox.teardown().await.unwrap();
}

/// M9: the happy path's price of the decision's two partial indexes — the
/// walk's and the held rows' — as an A/B on one machine: messages stored and
/// processed one at a time, with the indexes and with both dropped,
/// alternated, three rounds each. ADR-0041 accepts at most 0.1 ms a message.
#[tokio::test]
#[ignore = "a measurement (ADR-0041 M9): run by hand"]
async fn m9_the_happy_path_with_and_without_the_new_indexes() {
    if !asked() {
        return;
    }
    const MESSAGES: usize = 500;
    let run = async |name: &str, without: bool| -> Duration {
        let table = format!("inbox_m9_{name}");
        let inbox = PgInbox::new(PgSessionPool::new(pool()))
            .with_table(
                Identifier::new(&table).unwrap(),
                Identifier::new(format!("{table}_seq")).unwrap(),
            )
            .with_slots(8)
            .partitioned_by(ByStream);
        inbox.teardown().await.unwrap();
        inbox.migrate().await.unwrap();
        if without {
            pool()
                .get()
                .await
                .unwrap()
                .batch_execute(&format!(
                    "DROP INDEX {table}__tenant_head_idx; DROP INDEX {table}__held_idx"
                ))
                .await
                .unwrap();
        }
        let messages: Vec<InboxMessage> = (0..MESSAGES)
            .map(|i| {
                InboxMessage::new(
                    format!("tenant-{}", i % 50),
                    "orders.Order",
                    serde_json::json!({ "id": format!("s-{i}") }),
                    1,
                    "kafka://orders/s",
                    "x",
                )
                .with_metadata(serde_json::json!({
                    "message_id": format!("00000000-0000-4000-8000-{i:012x}")
                }))
            })
            .collect();
        let started = Instant::now();
        for message in &messages {
            crate::port::Inbox::publish(&inbox, message).await.unwrap();
        }
        let stored = started.elapsed();
        let subscriber = |_: &_, _: &InboxMessage| std::future::ready(Ok(()));
        // Bounded, as the suite's drains are: a dispatch that never runs dry
        // fails the measurement instead of hanging it.
        let mut dispatches = 0;
        while inbox.dispatch(subscriber).await.unwrap() != Outcome::Nothing {
            dispatches += 1;
            assert!(dispatches <= 50 + 3 * MESSAGES, "the inbox never ran dry");
        }
        let elapsed = started.elapsed();
        println!(
            "M9 {name}: stored {:.3} ms a message, processed {:.3}",
            stored.as_secs_f64() * 1000.0 / MESSAGES as f64,
            (elapsed - stored).as_secs_f64() * 1000.0 / MESSAGES as f64
        );
        inbox.teardown().await.unwrap();
        elapsed
    };
    const ROUNDS: usize = 3;
    let (mut with, mut without) = (Vec::new(), Vec::new());
    for _ in 0..ROUNDS {
        with.push(run("with", false).await);
        without.push(run("without", true).await);
    }
    with.sort();
    without.sort();
    let per = |d: Duration| d.as_secs_f64() * 1000.0 / MESSAGES as f64;
    println!(
        "M9: {MESSAGES} messages stored and processed: with the indexes {:.3} ms a message \
         (median of {with:?}), without {:.3} (median of {without:?}): the indexes cost {:.3} ms",
        per(with[1]),
        per(without[1]),
        per(with[1]) - per(without[1]),
    );
}
