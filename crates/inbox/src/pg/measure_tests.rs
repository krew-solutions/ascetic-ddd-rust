//! The measurements behind ADR-0041, M7 and M9, and ADR-0042's: run by hand,
//! on one machine at a time, against a database of one's own; no gate runs
//! them. `test-pg` runs every ignored test of the library's `--lib`, so they
//! return at once unless `ASCETIC_DDD_MEASURE` is set. Their times compare
//! only with each other, on the machine they ran on (ADR-0041 § Measured,
//! ADR-0042 § Measured):
//!
//! ```text
//! ASCETIC_DDD_MEASURE=1 ASCETIC_DDD_TEST_PG_URL=… cargo test -p ascetic-ddd-inbox --lib -- \
//!     --ignored --nocapture --test-threads=1 measure_tests
//! ```
//!
//! They explain and run the library's own statements, as `plan_tests.rs` does,
//! not copies a change would leave behind; an A/B against the library as it
//! was keeps that version's statements as constants, named by its commit.

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
    // No channel paused: the walk of a process whose breaker is closed.
    let nothing_paused: Vec<String> = Vec::new();
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
            let walk = explain(&walk_sql(table), &[&slot, &32i32, &1.0f64, &nothing_paused]).await;
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
            .query_one(&sql, &[&slot, &32i32, &1.0f64, &nothing_paused])
            .await
            .unwrap();
    }
    let as_text = started.elapsed();
    let prepared = client.prepare(&sql).await.unwrap();
    let started = Instant::now();
    for i in 0..50i16 {
        let slot = i % SLOTS as i16;
        client
            .query_one(&prepared, &[&slot, &32i32, &1.0f64, &nothing_paused])
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

/// `VACUUM ANALYZE` of an inbox's table and its slots' table, each a
/// statement of its own: a vacuum runs outside a transaction block.
async fn vacuum(client: &ascetic_ddd_session::pg::deadpool_postgres::Object, table: &str) {
    for name in [table.to_owned(), format!("{table}_slots")] {
        client
            .batch_execute(&format!("VACUUM ANALYZE {name}"))
            .await
            .unwrap();
    }
}

/// The take as it was before ADR-0042 (the library at 47d6848): its
/// statement read no isolation. The A of M1's A/B, beside the take of today.
const TAKE_BEFORE: &str = r#"
    WITH taken AS (
        SELECT s.slot FROM {table}_slots s
        CROSS JOIN LATERAL (
            SELECT r.received_position AS newest FROM {table} r
            WHERE r.slot = s.slot AND processed_position IS NULL AND parked_at IS NULL AND waiting_for IS NULL
            ORDER BY r.received_position DESC LIMIT 1
        ) q
        WHERE (SELECT true FROM {table} q WHERE processed_position IS NULL AND parked_at IS NULL AND waiting_for IS NULL
               ORDER BY q.slot, q.received_position LIMIT 1) IS TRUE
          AND (s.round_from IS NOT NULL
               OR s.dormant_until IS NULL
               OR s.dormant_until <= CURRENT_TIMESTAMP
               OR q.newest > s.dormant_after)
        ORDER BY s.served_at
        LIMIT 1
        FOR UPDATE OF s SKIP LOCKED
    )
    SELECT t.slot, pg_current_snapshot()::text, (SELECT slots FROM {table}_meta)
    FROM (SELECT 1) AS one
    LEFT JOIN taken t ON true
"#;

/// The walk as it was before ADR-0042 (the library at 47d6848): no paused
/// channels, no channel read at each step, no Tenants met, no guest.
const WALK_BEFORE: &str = r#"
    WITH RECURSIVE here AS (
        SELECT coalesce(last_tenant, '') AS c, round_from, dormant_until, dormant_after
        FROM {table}_slots WHERE slot = $1
    ), walk (n, phase, tenant_id, place, due, retry_at) AS (
        SELECT 0, 0, c, NULL::tid, false, NULL::timestamptz FROM here
        UNION ALL
        SELECT w.n + 1, h.phase, h.tenant_id, h.place, h.due, h.retry_at
        FROM walk w CROSS JOIN here CROSS JOIN LATERAL (
            SELECT * FROM (
                (SELECT w.phase AS phase, after.* FROM (
                     SELECT r.tenant_id, r.ctid AS place,
                            (r.next_attempt_at IS NULL OR r.next_attempt_at <= CURRENT_TIMESTAMP) AS due,
                            r.next_attempt_at AS retry_at
                     FROM {table} r
                     WHERE r.slot = $1 AND processed_position IS NULL AND parked_at IS NULL
                       AND waiting_for IS NULL AND r.tenant_id > w.tenant_id
                     ORDER BY r.tenant_id, r.received_position LIMIT 1) after
                 WHERE w.phase = 0 OR after.tenant_id <= here.c)
                UNION ALL
                (SELECT 1, r.tenant_id, r.ctid AS place,
                        (r.next_attempt_at IS NULL OR r.next_attempt_at <= CURRENT_TIMESTAMP) AS due,
                        r.next_attempt_at AS retry_at
                 FROM {table} r
                 WHERE w.phase = 0 AND r.slot = $1 AND processed_position IS NULL AND parked_at IS NULL
                   AND waiting_for IS NULL AND r.tenant_id <= here.c
                 ORDER BY r.tenant_id, r.received_position LIMIT 1)
            ) next ORDER BY phase LIMIT 1
        ) h
        WHERE w.n < $2 AND NOT w.due
    ), last AS (
        SELECT * FROM walk WHERE n > 0 AND n = (SELECT max(n) FROM walk)
    ), head AS (
        SELECT r.received_position, r.ctid::text AS place, r.tenant_id
        FROM {table} r JOIN last ON r.ctid = last.place
        WHERE last.due AND processed_position IS NULL AND parked_at IS NULL AND waiting_for IS NULL
        FOR UPDATE OF r
    ), verdict AS (
        SELECT h.tenant_id IS NOT NULL AS taken, h.tenant_id AS taken_tenant,
               l.tenant_id AS u, l.phase AS u_phase,
               coalesce(l.due, false) AND h.tenant_id IS NULL AS vanished,
               seen.n < $2 AND NOT coalesce(l.due, false) AS exhausted,
               seen.earliest, here.c, here.round_from, here.dormant_until, here.dormant_after,
               here.round_from IS NULL AS starts
        FROM here
        CROSS JOIN (SELECT max(n) AS n, min(retry_at) FILTER (WHERE NOT due) AS earliest
                    FROM walk) seen
        LEFT JOIN last l ON true
        LEFT JOIN head h ON true
    ), decided AS (
        SELECT v.*,
               CASE WHEN v.starts THEN v.c ELSE v.round_from END AS r_from,
               v.exhausted OR (NOT v.starts AND
                   ROW(CASE WHEN v.round_from > v.c THEN 0 ELSE 1 END, v.round_from)
                       <= ROW(v.u_phase, v.u)) AS ends,
               CASE WHEN v.starts THEN v.earliest
                    ELSE least(v.dormant_until, v.earliest) END AS round_earliest,
               CASE WHEN v.starts
                    THEN (SELECT q.received_position FROM {table} q
                          WHERE q.slot = $1 AND processed_position IS NULL AND parked_at IS NULL
                            AND waiting_for IS NULL
                          ORDER BY q.received_position DESC LIMIT 1)
                    ELSE v.dormant_after END AS round_after
        FROM verdict v
    ), updated AS (
        UPDATE {table}_slots s SET
            served_at = CURRENT_TIMESTAMP,
            last_tenant = CASE WHEN d.taken THEN d.taken_tenant
                               WHEN d.u IS NOT NULL AND NOT d.vanished THEN d.u
                               ELSE s.last_tenant END,
            round_from = CASE WHEN d.taken OR d.u IS NULL OR d.vanished OR d.ends THEN NULL
                              ELSE d.r_from END,
            dormant_until = CASE WHEN d.taken OR d.u IS NULL OR d.vanished THEN NULL
                                 WHEN d.ends THEN least(d.round_earliest,
                                     CURRENT_TIMESTAMP + make_interval(secs => $3))
                                 ELSE d.round_earliest END,
            dormant_after = CASE WHEN d.taken OR d.u IS NULL OR d.vanished THEN NULL
                                 ELSE d.round_after END
        FROM decided d
        WHERE s.slot = $1
        RETURNING CASE WHEN d.taken THEN 'taken'
                       WHEN d.u IS NULL OR d.vanished OR d.ends THEN 'nothing'
                       ELSE 'passed' END AS outcome
    )
    SELECT h.received_position, h.place, u.outcome, pg_current_snapshot()::text
    FROM updated u LEFT JOIN head h ON true
"#;

/// M1 (ADR-0042 § Executors, R9: nothing new on the happy path above 0.1 ms
/// a message): the statements of a happy dispatch as they were before
/// ADR-0042 and as they are, an A/B on one machine and one table, timed by
/// the server in a procedure that commits each call, as a dispatch commits
/// its transaction, `synchronous_commit` off to keep the disk out of it.
/// Blocks of 5,000 calls, A and B alternated, A B B A, four rounds.
/// - The take, and the walk with nothing paused: the functions of
///   [`TAKE_BEFORE`] and [`WALK_BEFORE`] beside the baseline's own, over
///   the host's launch shape — 64 slots, 2,000 Tenants of a hundred
///   streams each, some 1,600 Tenants a slot, a processed history of
///   200,000 — every head due, then nine heads in ten in back-off.
/// - The mark: today's statement, which also ends a hold and checks the
///   decision's three CHECKs, against one that sets as many columns as the
///   mark of 47d6848 did, on a twin table without those CHECKs.
///
/// The dispatch no longer runs the expiry's statement (decision 7): the
/// statement of 47d6848 is timed too, the column it tested for a hold then
/// `held_since`, on a table with no waiter, the cost of a dispatch then
/// spared. The breaker's own work in the process is
/// `breaker::tests::m1_*`'s.
#[tokio::test]
#[ignore = "a measurement (ADR-0042 M1): run by hand"]
async fn m1_the_happy_path_s_statements_before_the_decision_and_after() {
    if !asked() {
        return;
    }
    const SLOTS: u32 = 64;
    const ROUNDS: usize = 4;
    // `ASCETIC_DDD_MEASURE=small` tries the measurement on a tenth of the
    // shape and of the calls, to see that it runs; its figures mean nothing.
    let small = std::env::var("ASCETIC_DDD_MEASURE").as_deref() == Ok("small");
    let (calls, tenants, history): (i32, i32, i32) = if small {
        (500, 200, 20_000)
    } else {
        (5_000, 2_000, 200_000)
    };
    let client = pool().get().await.unwrap();
    let mut tables = Vec::new();
    for table in ["inbox_m1", "inbox_m1_twin"] {
        let inbox = PgInbox::new(PgSessionPool::new(pool()))
            .with_table(
                Identifier::new(table).unwrap(),
                Identifier::new(format!("{table}_seq")).unwrap(),
            )
            .with_slots(SLOTS)
            .partitioned_by(ByStream);
        inbox.teardown().await.unwrap();
        inbox.migrate().await.unwrap();
        client
            .batch_execute(&format!(
                "INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, \
                     payload, metadata, processed_position) \
                 SELECT 't' || lpad((g % {tenants})::text, 4, '0'), 'orders.Order', \
                     to_jsonb('h-' || g), 1, 'kafka://orders/h', 'x', '{{}}', g \
                 FROM generate_series(1, {history}) g; \
                 INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, \
                     payload, metadata) \
                 SELECT 't' || lpad(t::text, 4, '0'), 'orders.Order', \
                     to_jsonb('s-' || t || '-' || k), 1, 'kafka://orders/s', 'x', '{{}}' \
                 FROM generate_series(0, {tenants} - 1) t, generate_series(1, 100) k"
            ))
            .await
            .unwrap();
        vacuum(&client, table).await;
        tables.push(inbox);
    }
    let table = "inbox_m1";
    let twin = "inbox_m1_twin";
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
    println!("M1: {tenants:.0} Tenants a slot on average");
    let body = |sql: &str, into: &str| {
        format!(
            "\n    #variable_conflict use_column\n    BEGIN\n{}\n    INTO STRICT {into};\n    END\n    ",
            sql.replace("{table}", table)
        )
    };
    client
        .batch_execute(&format!(
            "DROP FUNCTION IF EXISTS {table}__take_before(); \
             DROP FUNCTION IF EXISTS {table}__walk_before(smallint, integer, double precision); \
             CREATE FUNCTION {table}__take_before(OUT taken_slot smallint, OUT take_snapshot text, \
                 OUT slot_count integer) LANGUAGE plpgsql VOLATILE AS $fn${}$fn$; \
             CREATE FUNCTION {table}__walk_before(smallint, integer, double precision, \
                 OUT head_position bigint, OUT head_place text, OUT walk_outcome text, \
                 OUT walk_snapshot text) \
                 LANGUAGE plpgsql VOLATILE SET plan_cache_mode = force_generic_plan \
                 AS $fn${}$fn$; \
             ALTER TABLE {twin} DROP CONSTRAINT {twin}__hold_in_queue, \
                 DROP CONSTRAINT {twin}__cause_unprocessed, DROP CONSTRAINT {twin}__wait_is_open;",
            body(TAKE_BEFORE, "taken_slot, take_snapshot, slot_count"),
            body(
                WALK_BEFORE,
                "head_position, head_place, walk_outcome, walk_snapshot"
            ),
        ))
        .await
        .unwrap();
    // A block of calls, each committed; the marks take the queue's oldest
    // rows of their table, read before the clock starts.
    let mark_after = format!(
        "processed_at = statement_timestamp(), cause = NULL, {}",
        super::store::LEAVE_QUEUE
    );
    let mark_before = "processed_at = statement_timestamp(), held_since = NULL, cause = NULL, \
                       holds = NULL, hold_kind = NULL";
    let procedure = format!(
        r#"
        CREATE OR REPLACE PROCEDURE m1_block(kind text, n integer, INOUT micros double precision)
        LANGUAGE plpgsql AS $$
        DECLARE t0 timestamptz; r record; s smallint; places tid[]; target text;
        BEGIN
          IF kind LIKE 'mark%' THEN
            target := CASE kind WHEN 'mark_before' THEN '{twin}' ELSE '{table}' END;
            EXECUTE format('SELECT array_agg(ctid) FROM (SELECT ctid FROM %I WHERE processed_position IS NULL
                            AND parked_at IS NULL AND waiting_for IS NULL ORDER BY received_position LIMIT %s) q',
                           target, n) INTO places;
          END IF;
          t0 := clock_timestamp();
          FOR i IN 1..n LOOP
            s := (i % {SLOTS})::smallint;
            CASE kind
              WHEN 'take_before' THEN SELECT * INTO r FROM {table}__take_before();
              WHEN 'take_after' THEN SELECT * INTO r FROM {table}__take();
              WHEN 'walk_before' THEN SELECT * INTO r FROM {table}__walk_before(s, 32, 1.0);
              WHEN 'walk_after' THEN SELECT * INTO r FROM {table}__walk(s, 32, 1.0, '{{}}', '{{}}');
              WHEN 'expire_before' THEN
                EXECUTE 'UPDATE {table} SET parked_at = CURRENT_TIMESTAMP, '
                     || 'last_error = ''dependency '' || waiting_for::text || '' never arrived'', '
                     || 'waiting_since = NULL WHERE ctid IN (SELECT w.ctid FROM {table} w '
                     || 'WHERE w.waiting_for IS NOT NULL AND w.waiting_since < CURRENT_TIMESTAMP '
                     || '- make_interval(secs => $1::double precision) AND NOT EXISTS (SELECT 1 FROM {table} '
                     || 'WHERE tenant_id = w.waiting_for->>''tenant_id'' AND stream_type = w.waiting_for->>''stream_type'' '
                     || 'AND stream_id = w.waiting_for->''stream_id'' AND jsonb_build_object(''tenant_id'', tenant_id, '
                     || '''stream_type'', stream_type, ''stream_id'', stream_id, ''stream_position'', stream_position) '
                     || '= w.waiting_for AND held_since IS NOT NULL) LIMIT $2 FOR UPDATE OF w SKIP LOCKED) '
                     || 'RETURNING received_position'
                  USING 600.0::double precision, 100::bigint;
              WHEN 'mark_before' THEN
                EXECUTE 'WITH marked AS (UPDATE {twin} SET processed_position = nextval(''{twin}_seq''), '
                     || '{mark_before} WHERE ctid = $1 RETURNING processed_position, '
                     || 'jsonb_build_object(''tenant_id'', tenant_id, ''stream_type'', stream_type, '
                     || '''stream_id'', stream_id, ''stream_position'', stream_position) AS identity), '
                     || 'woken AS (UPDATE {twin} SET waiting_for = NULL, waiting_since = NULL, parked_at = NULL '
                     || 'WHERE waiting_for = (SELECT identity FROM marked) RETURNING received_position) '
                     || 'SELECT marked.processed_position, woken.* FROM marked LEFT JOIN woken ON true'
                  INTO r USING places[i];
              WHEN 'mark_after' THEN
                EXECUTE 'WITH marked AS (UPDATE {table} SET processed_position = nextval(''{table}_seq''), '
                     || '{mark_after} WHERE ctid = $1 RETURNING processed_position, '
                     || 'jsonb_build_object(''tenant_id'', tenant_id, ''stream_type'', stream_type, '
                     || '''stream_id'', stream_id, ''stream_position'', stream_position) AS identity), '
                     || 'woken AS (UPDATE {table} SET waiting_for = NULL, waiting_since = NULL, parked_at = NULL '
                     || 'WHERE waiting_for = (SELECT identity FROM marked) RETURNING received_position) '
                     || 'SELECT marked.processed_position, woken.* FROM marked LEFT JOIN woken ON true'
                  INTO r USING places[i];
            END CASE;
            COMMIT;
          END LOOP;
          micros := extract(epoch FROM clock_timestamp() - t0) * 1e6 / n;
        END $$;
        "#
    );
    client.batch_execute(&procedure).await.unwrap();
    client
        .batch_execute("SET synchronous_commit = off")
        .await
        .unwrap();
    // A procedure that commits is called by the simple protocol, outside a
    // transaction block.
    let block = async |kind: &str| -> f64 {
        let messages = client
            .simple_query(&format!("CALL m1_block('{kind}', {calls}, 0)"))
            .await
            .unwrap();
        messages
            .iter()
            .find_map(|message| match message {
                ascetic_ddd_session::pg::tokio_postgres::SimpleQueryMessage::Row(row) => {
                    row.get(0).map(|micros| micros.parse::<f64>().unwrap())
                }
                _ => None,
            })
            .unwrap()
    };
    let ab = async |before: &str, after: &str| {
        // one block of each to warm the caches and the plans
        block(before).await;
        block(after).await;
        let (mut a, mut b) = (Vec::new(), Vec::new());
        for round in 0..ROUNDS {
            if round % 2 == 0 {
                a.push(block(before).await);
                b.push(block(after).await);
            } else {
                b.push(block(after).await);
                a.push(block(before).await);
            }
        }
        let median = |v: &mut Vec<f64>| {
            v.sort_by(f64::total_cmp);
            (v[v.len() / 2 - 1] + v[v.len() / 2]) / 2.0
        };
        let (ma, mb) = (median(&mut a), median(&mut b));
        println!(
            "M1, {before} / {after}: {ma:.1} / {mb:.1} µs a call (medians of {ROUNDS} blocks of \
             {calls}; before {a:.1?}, after {b:.1?}): {:+.1} µs",
            mb - ma
        );
    };
    block("expire_before").await;
    let mut expiring = Vec::new();
    for _ in 0..ROUNDS {
        expiring.push(block("expire_before").await);
    }
    expiring.sort_by(f64::total_cmp);
    println!(
        "M1, the expiry's statement a dispatch ran before: {:.1} µs a call ({expiring:.1?}), spared",
        (expiring[ROUNDS / 2 - 1] + expiring[ROUNDS / 2]) / 2.0
    );
    ab("take_before", "take_after").await;
    ab("walk_before", "walk_after").await;
    client
        .batch_execute(&format!(
            "UPDATE {table} SET next_attempt_at = now() + interval '1 hour', attempts = 1 \
             WHERE processed_position IS NULL AND random() < 0.9; \
             UPDATE {table}_slots SET last_tenant = NULL, round_from = NULL, \
                 dormant_until = NULL, dormant_after = NULL"
        ))
        .await
        .unwrap();
    vacuum(&client, table).await;
    println!("M1: nine heads in ten in back-off");
    ab("walk_before", "walk_after").await;
    client
        .batch_execute(&format!(
            "UPDATE {table} SET next_attempt_at = NULL, attempts = 0 \
             WHERE processed_position IS NULL"
        ))
        .await
        .unwrap();
    vacuum(&client, table).await;
    ab("mark_before", "mark_after").await;
    client
        .batch_execute("DROP PROCEDURE m1_block; RESET synchronous_commit")
        .await
        .unwrap();
    client
        .batch_execute(&format!(
            "DROP FUNCTION {table}__take_before(); \
             DROP FUNCTION {table}__walk_before(smallint, integer, double precision)"
        ))
        .await
        .unwrap();
    for inbox in tables {
        inbox.teardown().await.unwrap();
    }
}

/// A pool of `size` connections, for a measurement that runs dispatchers
/// beside a verb.
fn pool_of(size: usize) -> Pool {
    let url = std::env::var("ASCETIC_DDD_TEST_PG_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned());
    let manager = Manager::from_config(
        Config::from_str(&url).expect("a valid PostgreSQL URL"),
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    Pool::builder(manager)
        .max_size(size)
        .build()
        .expect("the pool can be built")
}

/// The median and the largest of `samples`, in milliseconds.
fn spread(samples: &mut [Duration]) -> (f64, f64, f64) {
    samples.sort();
    let at = |q: f64| {
        samples[((samples.len() as f64 - 1.0) * q).round() as usize].as_secs_f64() * 1000.0
    };
    (at(0.5), at(0.99), at(1.0))
}

/// M7 (ADR-0042 § Executors): the operator's verbs at the host's scale, on
/// its launch shape in a full outage — 64 slots, 2,000 Tenants of a hundred
/// streams over seven channels, a Tenant with a backlog of 70,000 rows, a
/// processed history of 200,000, every head held under one cause, some
/// 100,000 held heads. Timed at the client, the verb's own statements:
/// - a page of the held heads, which sorts the held heads each page;
/// - `park_heads` of one channel, batch after batch;
/// - `park_queue` of the backlogged Tenant on one channel, whose batches
///   re-read its rows of the other channels in its slots;
/// - `unpark_all` of what `park_heads` parked;
/// - eight dispatchers' dispatches, without a verb and beside `park_heads`
///   of another channel: a walk that meets a head the verb's batch locks
///   waits for the batch.
#[tokio::test]
#[ignore = "a measurement (ADR-0042 M7): run by hand"]
async fn m7_the_operator_s_verbs_at_the_host_s_scale() {
    use super::{ParkedFor, ParkedUnder, ParkingHeads, ParkingQueue, Unparking};
    if !asked() {
        return;
    }
    let small = std::env::var("ASCETIC_DDD_MEASURE").as_deref() == Ok("small");
    let (tenants, backlog, history): (i32, i32, i32) = if small {
        (200, 7_000, 20_000)
    } else {
        (2_000, 70_000, 200_000)
    };
    let table = "inbox_m7v";
    let inbox = PgInbox::new(PgSessionPool::new(pool_of(12)))
        .with_table(
            Identifier::new(table).unwrap(),
            Identifier::new("inbox_m7v_seq").unwrap(),
        )
        .with_slots(64)
        .partitioned_by(ByStream);
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    let client = pool().get().await.unwrap();
    client
        .batch_execute(&format!(
            "INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, \
                 payload, metadata, processed_position) \
             SELECT 't' || lpad((g % {tenants})::text, 4, '0'), 'orders.Order', \
                 to_jsonb('h-' || g), 1, 'kafka://c' || (g % 7) || '/h', 'x', '{{}}', g \
             FROM generate_series(1, {history}) g; \
             INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, \
                 payload, metadata) \
             SELECT 't' || lpad(t::text, 4, '0'), 'orders.Order', \
                 to_jsonb('s-' || t || '-' || k), 1, 'kafka://c' || (k % 7) || '/s', 'x', '{{}}' \
             FROM generate_series(0, {tenants} - 1) t, generate_series(1, 100) k; \
             INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, \
                 payload, metadata) \
             SELECT 'tbig', 'orders.Order', to_jsonb('b-' || k), 1, \
                 'kafka://c' || (k % 7) || '/b', 'x', '{{}}' \
             FROM generate_series(1, {backlog}) k; \
             UPDATE {table} SET held_since = now(), next_attempt_at = now() + interval '1 hour', \
                 hold_kind = 'unavailable', holds = 1, cause = 'kms.unavailable' \
             WHERE (slot, tenant_id, received_position) IN ( \
                 SELECT slot, tenant_id, min(received_position) FROM {table} \
                 WHERE processed_position IS NULL GROUP BY slot, tenant_id)"
        ))
        .await
        .unwrap();
    vacuum(&client, table).await;
    let held: i64 = client
        .query_one(
            &format!("SELECT count(*) FROM {table} WHERE held_since IS NOT NULL"),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    println!("M7v: {held} heads held");
    // the held page, first and from the middle, five times each
    let middle: i64 = client
        .query_one(
            &format!(
                "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY received_position) \
                 FROM {table} WHERE held_since IS NOT NULL"
            ),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    for (name, after) in [
        ("the first page", None),
        ("a page from the middle", Some(middle)),
    ] {
        let mut times = Vec::new();
        for _ in 0..6 {
            let started = Instant::now();
            let page = inbox.held("ops", after, 100).await.unwrap();
            assert_eq!(page.len(), 100);
            times.push(started.elapsed());
        }
        let (median, _, most) = spread(&mut times[1..]);
        println!("M7v, the held heads, {name} of 100: {median:.1} ms, at most {most:.1}");
    }
    let cause = "kms.unavailable";
    let started = Instant::now();
    let given = inbox
        .park_heads(
            "ops",
            &ParkingHeads {
                channel: "kafka://c0",
                cause,
                tenant: None,
                limit: u32::MAX,
            },
        )
        .await
        .unwrap();
    println!(
        "M7v, park_heads of one channel, every head: {:.0} ms, {given:?}",
        started.elapsed().as_secs_f64() * 1000.0
    );
    let started = Instant::now();
    let given = inbox
        .park_queue(
            "ops",
            &ParkingQueue {
                channel: "kafka://c1",
                tenant: "tbig",
                cause: ascetic_ddd_bus::Cause::new("operator.given_up").unwrap(),
                limit: u32::MAX,
            },
        )
        .await
        .unwrap();
    println!(
        "M7v, park_queue of the backlogged Tenant on one channel: {:.0} ms, {given:?}",
        started.elapsed().as_secs_f64() * 1000.0
    );
    let started = Instant::now();
    let unparked = inbox
        .unpark_all(
            "ops",
            &Unparking {
                channel: "kafka://c0",
                ground: ParkedFor::Failure(ParkedUnder::Cause(cause)),
                tenant: None,
                within: Duration::from_secs(3600),
                tries: 1,
                limit: u32::MAX,
                overtaken: false,
            },
        )
        .await
        .unwrap();
    println!(
        "M7v, unpark_all of what park_heads parked: {:.0} ms, {unparked:?}",
        started.elapsed().as_secs_f64() * 1000.0
    );
    // Dispatchers on c0, re-armed and due, without a verb and beside
    // park_heads of the other channels, one after another, five seconds
    // each, after a second unrecorded that opens their connections and
    // plans their statements.
    let dispatching = async |verb: bool, seconds: u64| -> Vec<Duration> {
        let stop = std::sync::atomic::AtomicBool::new(false);
        let loops = (0..8).map(|_| async {
            let mut times = Vec::new();
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let started = Instant::now();
                inbox
                    .dispatch(|_: &_, _: &InboxMessage| std::future::ready(Ok(())))
                    .await
                    .unwrap();
                times.push(started.elapsed());
            }
            times
        });
        let clock = async {
            let started = Instant::now();
            if verb {
                for channel in [
                    "kafka://c2",
                    "kafka://c3",
                    "kafka://c4",
                    "kafka://c5",
                    "kafka://c6",
                ] {
                    if started.elapsed() >= Duration::from_secs(seconds) {
                        break;
                    }
                    let given = inbox
                        .park_heads(
                            "ops",
                            &ParkingHeads {
                                channel,
                                cause,
                                tenant: None,
                                limit: u32::MAX,
                            },
                        )
                        .await
                        .unwrap();
                    println!(
                        "M7v, beside the dispatchers, {channel} at {:.1} s: {given:?}",
                        started.elapsed().as_secs_f64()
                    );
                }
            }
            tokio::time::sleep(Duration::from_secs(seconds).saturating_sub(started.elapsed()))
                .await;
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
        };
        let (times, ()) = tokio::join!(futures::future::join_all(loops), clock);
        times.into_iter().flatten().collect()
    };
    dispatching(false, 1).await;
    for verb in [false, true] {
        let mut all = dispatching(verb, 5).await;
        let n = all.len();
        let (median, p99, most) = spread(&mut all);
        println!(
            "M7v, eight dispatchers{}: {n} dispatches, {median:.2} ms median, {p99:.2} at the \
             99th centile, {most:.2} at most",
            if verb {
                " beside park_heads of the other channels"
            } else {
                ""
            }
        );
    }

    inbox.teardown().await.unwrap();
}

/// The two-session run of ADR-0042 § Executors: `park_heads` beside a
/// dispatcher that holds the same heads again and processes some, each
/// change committed between a batch's reading of its candidates and its
/// lock. A head a batch read and another session changed meanwhile is
/// locked at its newest version, its selection tested again there: parked
/// if still held under the cause, passed by otherwise, and read again by
/// the next batch. So a verb asked until it reports nothing left parks every
/// head still held under the cause, and none processed; the passes counted
/// say how often the race was met.
#[tokio::test]
#[ignore = "a measurement (ADR-0042 M7): run by hand"]
async fn m7_park_heads_beside_a_dispatcher_changing_the_same_heads() {
    use super::ParkingHeads;
    if !asked() {
        return;
    }
    const HEADS: i32 = 4_000;
    let table = "inbox_m7r";
    let inbox = PgInbox::new(PgSessionPool::new(pool_of(4)))
        .with_table(
            Identifier::new(table).unwrap(),
            Identifier::new("inbox_m7r_seq").unwrap(),
        )
        .with_slots(8)
        .partitioned_by(ByStream);
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    let client = pool().get().await.unwrap();
    client
        .batch_execute(&format!(
            "INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, \
                 payload, metadata, held_since, next_attempt_at, hold_kind, holds, cause) \
             SELECT 't' || g, 'orders.Order', to_jsonb('s-' || g), 1, 'kafka://c0/s', 'x', '{{}}', \
                 now(), now() + interval '1 hour', 'unavailable', 1, 'kms.unavailable' \
             FROM generate_series(1, {HEADS}) g"
        ))
        .await
        .unwrap();
    vacuum(&client, table).await;
    let stop = std::sync::atomic::AtomicBool::new(false);
    // Four sessions that hold a head again or process one, at random,
    // a statement committed at a time.
    let changers = (0..4).map(|_| async {
        let session = pool().get().await.unwrap();
        let (mut held, mut processed) = (0u64, 0u64);
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
            let rows = session
                .execute(
                    &format!(
                        "WITH one AS (SELECT ctid FROM {table} WHERE held_since IS NOT NULL \
                             AND parked_at IS NULL AND processed_position IS NULL \
                             OFFSET floor(random() * 50) LIMIT 1 FOR UPDATE SKIP LOCKED) \
                         UPDATE {table} t SET holds = holds + 1, \
                             next_attempt_at = now() + interval '1 hour' \
                         FROM one WHERE t.ctid = one.ctid AND random() < 0.8"
                    ),
                    &[],
                )
                .await
                .unwrap();
            held += rows;
            let rows = session
                .execute(
                    &format!(
                        "WITH one AS (SELECT ctid FROM {table} WHERE held_since IS NOT NULL \
                             AND parked_at IS NULL AND processed_position IS NULL \
                             OFFSET floor(random() * 50) LIMIT 1 FOR UPDATE SKIP LOCKED) \
                         UPDATE {table} t SET processed_position = nextval('{table}_seq'), \
                             processed_at = now(), cause = NULL, {} \
                         FROM one WHERE t.ctid = one.ctid AND random() < 0.05",
                        super::store::LEAVE_QUEUE
                    ),
                    &[],
                )
                .await
                .unwrap();
            processed += rows;
        }
        (held, processed)
    });
    let verb = async {
        let (mut parked, mut passed, mut calls) = (0u64, 0u64, 0u64);
        loop {
            let given = inbox
                .park_heads(
                    "ops",
                    &ParkingHeads {
                        channel: "kafka://c0",
                        cause: "kms.unavailable",
                        tenant: None,
                        limit: 100,
                    },
                )
                .await
                .unwrap();
            calls += 1;
            parked += given.parked;
            passed += given.passed;
            if given.parked == 0 && given.passed == 0 && !given.more {
                break;
            }
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        (parked, passed, calls)
    };
    let (changed, (parked, passed, calls)) =
        tokio::join!(futures::future::join_all(changers), verb);
    let held_again: u64 = changed.iter().map(|c| c.0).sum();
    let processed: u64 = changed.iter().map(|c| c.1).sum();
    let count = async |sql: &str| -> i64 {
        client
            .query_one(&sql.replace("{table}", table), &[])
            .await
            .unwrap()
            .get(0)
    };
    let left = count(
        "SELECT count(*) FROM {table} WHERE held_since IS NOT NULL AND parked_at IS NULL \
         AND processed_position IS NULL",
    )
    .await;
    let both = count(
        "SELECT count(*) FROM {table} WHERE parked_at IS NOT NULL AND processed_position IS NOT NULL",
    )
    .await;
    let parked_rows = count("SELECT count(*) FROM {table} WHERE parked_at IS NOT NULL").await;
    let processed_rows =
        count("SELECT count(*) FROM {table} WHERE processed_position IS NOT NULL").await;
    println!(
        "M7r: {calls} calls parked {parked} and passed {passed} by, beside {held_again} heads \
         held again and {processed} processed; left held {left}, parked and processed {both}, \
         parked {parked_rows}, processed {processed_rows} of {HEADS}"
    );
    assert_eq!((left, both), (0, 0));
    assert_eq!(parked_rows + processed_rows, i64::from(HEADS));
    assert_eq!(u64::try_from(parked_rows).unwrap(), parked);
    inbox.teardown().await.unwrap();
}

/// What M3 watches: when the process paused a channel, and how many walks
/// of its guests found nothing due.
#[derive(Default)]
struct Guests {
    paused: std::sync::Mutex<Option<Instant>>,
    walks: std::sync::atomic::AtomicU64,
}

impl crate::observer::InboxObserver for Guests {
    fn on_breaker(&self, event: &crate::observer::BreakerChanged<'_>) {
        if matches!(event.effect, crate::breaker::Effect::Paused { .. }) {
            self.paused.lock().unwrap().get_or_insert_with(Instant::now);
        }
    }

    fn on_dispatched(&self, event: &crate::observer::Dispatched<'_>) {
        if matches!(event.outcome, Ok(Outcome::Paused)) {
            self.walks
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

/// M3 (ADR-0042 § Rule of Three, 1): a head of another channel waits for a
/// guest while one channel alone is paused — at most a circle of the
/// guests' walks over the paused heads, N / (L · K / poll), 33 s at the
/// host's 128,000 heads, three replicas of eight loops and a 200 ms poll.
/// One process of eight loops, a 200 ms poll, K = 32, 64 slots, 32,000
/// Tenants with a head on a channel whose every ask fails at the stage's
/// scope: once the process paused it, 20 rows arrive on another channel,
/// each a Tenant's head, after every paused head in their slots' order of
/// Tenants. Timed: when each of the 20 was processed after it arrived, and
/// the guests' walks a second meanwhile — the guest's sweep, its cost.
#[tokio::test]
#[ignore = "a measurement (ADR-0042 M3): run by hand"]
async fn m3_a_backlog_of_another_channel_beside_a_paused_one() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    if !asked() {
        return;
    }
    let small = std::env::var("ASCETIC_DDD_MEASURE").as_deref() == Ok("small");
    let paused_heads: i32 = if small { 3_200 } else { 32_000 };
    const ARRIVING: usize = 20;
    const LOOPS: u32 = 8;
    const POLL: Duration = Duration::from_millis(200);
    let table = "inbox_m3";
    let watch = Arc::new(Guests::default());
    let inbox = PgInbox::new(PgSessionPool::new(pool_of(12)))
        .with_table(
            Identifier::new(table).unwrap(),
            Identifier::new("inbox_m3_seq").unwrap(),
        )
        .with_slots(64)
        .partitioned_by(ByStream)
        .observed_by(Arc::clone(&watch));
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    let client = pool().get().await.unwrap();
    client
        .batch_execute(&format!(
            "INSERT INTO {table} (tenant_id, stream_type, stream_id, stream_position, uri, \
                 payload, metadata) \
             SELECT 'x' || lpad(g::text, 6, '0'), 'orders.Order', to_jsonb('x-' || g), 1, \
                 'kafka://x/s', 'x', '{{}}' FROM generate_series(1, {paused_heads}) g"
        ))
        .await
        .unwrap();
    vacuum(&client, table).await;
    let started = Instant::now();
    let arrived = std::sync::Mutex::new(None::<Instant>);
    let served = std::sync::Mutex::new(Vec::<Duration>::new());
    let count = AtomicUsize::new(0);
    let subscriber = |_: &_, message: &InboxMessage| {
        let verdict = if message.uri.starts_with("kafka://x") {
            Err(crate::Failure::from(ascetic_ddd_bus::Unavailable::new(
                ascetic_ddd_bus::Cause::new("kms.unavailable").unwrap(),
                ascetic_ddd_bus::Scope::Stage,
                None,
                "Vault is sealed",
            )))
        } else {
            if let Some(at) = *arrived.lock().unwrap() {
                served.lock().unwrap().push(at.elapsed());
            }
            count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        };
        std::future::ready(verdict)
    };
    let walks_at = std::sync::Mutex::new(0u64);
    let arrive = async {
        while watch.paused.lock().unwrap().is_none() {
            assert!(started.elapsed() < Duration::from_secs(120), "never paused");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // a few seconds of guests alone, then the arrivals
        tokio::time::sleep(Duration::from_secs(3)).await;
        for y in 0..ARRIVING {
            crate::port::Inbox::publish(
                &inbox,
                &InboxMessage::new(
                    format!("y{y:02}"),
                    "orders.Order",
                    serde_json::json!({ "id": format!("y-{y}") }),
                    1,
                    "kafka://y/s",
                    "y",
                )
                .with_metadata(serde_json::json!({
                    "message_id": format!("00000000-0000-4000-8000-{y:012x}")
                })),
            )
            .await
            .unwrap();
        }
        *walks_at.lock().unwrap() = watch.walks.load(Ordering::Relaxed);
        *arrived.lock().unwrap() = Some(Instant::now());
    };
    let shutdown = async {
        while count.load(Ordering::SeqCst) < ARRIVING
            && started.elapsed() < Duration::from_secs(600)
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    let loops = super::Loops {
        concurrency: LOOPS,
        poll_interval: POLL,
        max_error_wait: Duration::from_secs(1),
    };
    let (ran, ()) = tokio::join!(inbox.run(subscriber, loops, shutdown), arrive);
    ran.unwrap();
    let waited = arrived.lock().unwrap().unwrap().elapsed();
    let walks = watch.walks.load(Ordering::Relaxed) - *walks_at.lock().unwrap();
    let paused_after = watch.paused.lock().unwrap().unwrap() - started;
    let mut times = served.lock().unwrap().clone();
    times.sort();
    let seconds = |d: &Duration| d.as_secs_f64();
    let circle = f64::from(paused_heads) / (f64::from(LOOPS) * 32.0 / POLL.as_secs_f64());
    println!(
        "M3: paused after {:.1} s; the {ARRIVING} rows processed {:.1} s after they arrived at \
         the latest, {:.1} s at the median, the first after {:.1} s; a circle of the guests' walks \
         over {paused_heads} paused heads, N / (L · K / poll): {circle:.1} s; {walks} walks that \
         found nothing in {:.1} s, {:.1} a second",
        seconds(&paused_after),
        seconds(times.last().unwrap()),
        seconds(&times[times.len() / 2]),
        seconds(&times[0]),
        seconds(&waited),
        walks as f64 / seconds(&waited)
    );
    inbox.teardown().await.unwrap();
}
