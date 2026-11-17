//! E1 (ADR-0042 decision 7): the expiry parks a row that has waited
//! `max_wait` for a dependency that never arrived, is parked, or reaches no
//! queued row within 8 hops of waiting rows, and spares one whose
//! dependency is there and on its way — untried, held, behind a held head
//! or a long backlog — writing and locking nothing of it. A pass goes on
//! from the key it stopped at, a batch a statement, so that a waiter past
//! any number of spared ones is reached; it runs on a timer of `run`, off
//! the dispatch path. They need a live database:
//!
//! ```text
//! ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
//!     cargo test -p ascetic-ddd-inbox --test expiry -- --ignored
//! ```

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ascetic_ddd_inbox::observer::{Expired, InboxObserver};
use ascetic_ddd_inbox::{
    ByStream, CausalDependency, Error, Expiry, Failure, Inbox, InboxMessage, Loops, PgInbox,
};
use ascetic_ddd_session::pg::Identifier;
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::error::SqlState;
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};
use ascetic_ddd_session::{PgAccess, PgSession, PgSessionPool, Session, SessionPool};
use serde_json::json;

const DEFAULT_URL: &str = "postgresql://devel:devel@localhost:5432/devel_karmabot_test";

/// The batch a statement of the expiry examines, as the library has it.
const BATCH: usize = 100;

fn config() -> Config {
    let url = std::env::var("ASCETIC_DDD_TEST_PG_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned());
    Config::from_str(&url).expect("a valid PostgreSQL URL")
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

/// What the observer was told the expiry parked, by stream.
#[derive(Default)]
struct Told(Mutex<Vec<String>>);

impl InboxObserver for Told {
    fn on_expired(&self, event: &Expired<'_>) {
        self.0.lock().unwrap().extend(
            event
                .messages
                .iter()
                .map(|message| message.stream_id["id"].as_str().unwrap_or("?").to_owned()),
        );
    }
}

struct Fixture {
    inbox: PgInbox<PgSessionPool, Arc<Told>>,
    told: Arc<Told>,
    sessions: PgSessionPool,
    table: String,
}

/// A table of its own, one slot, `max_wait` an hour; its sessions as
/// `config` opens them.
async fn fixture_on(name: &str, config: Config, max_wait: Duration) -> Fixture {
    let table = format!("expiry_{name}");
    let told = Arc::new(Told::default());
    let inbox = PgInbox::new(PgSessionPool::new(pool_of(config.clone())))
        .with_table(
            Identifier::new(&table).unwrap(),
            Identifier::new(format!("{table}_seq")).unwrap(),
        )
        .partitioned_by(ByStream)
        .with_max_wait(max_wait)
        .observed_by(Arc::clone(&told));
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    Fixture {
        inbox,
        told,
        sessions: PgSessionPool::new(pool_of(config)),
        table,
    }
}

async fn fixture(name: &str) -> Fixture {
    fixture_on(name, config(), Duration::from_secs(3600)).await
}

/// The identity of `stream`'s first row, as the inbox waits for it.
fn identity(stream: &str) -> String {
    json!({
        "tenant_id": "tenant-1",
        "stream_type": "orders.Order",
        "stream_id": { "id": stream },
        "stream_position": 1,
    })
    .to_string()
}

/// The columns every row the tests write carries, `{stream}` its stream.
const ROW: &str = "'tenant-1', 'orders.Order', jsonb_build_object('id', {stream}), 1, \
                   'kafka://orders/' || {stream}, '\\x'::bytea, '{}'";
const COLUMNS: &str = "tenant_id, stream_type, stream_id, stream_position, uri, payload, metadata";

impl Fixture {
    /// Runs `sql`, `{table}` standing for the inbox's table.
    async fn execute(&self, sql: &str) {
        let sql = sql.replace("{table}", &self.table);
        self.sessions
            .session(async |session| {
                Ok::<_, Error>(session.connection().batch_execute(&sql).await?)
            })
            .await
            .unwrap();
    }

    async fn query(&self, sql: &str) -> Vec<Vec<Option<String>>> {
        let sql = sql.replace("{table}", &self.table);
        self.sessions
            .session(async |session| {
                let rows = session.connection().query(&sql, &[]).await?;
                Ok::<_, Error>(
                    rows.iter()
                        .map(|row| (0..row.len()).map(|at| row.get(at)).collect())
                        .collect(),
                )
            })
            .await
            .unwrap()
    }

    /// A row of `stream` in the queue.
    async fn queued(&self, stream: &str) {
        let row = ROW.replace("{stream}", &format!("'{stream}'"));
        self.execute(&format!("INSERT INTO {{table}} ({COLUMNS}) VALUES ({row})"))
            .await;
    }

    /// A row of `stream` in the queue, held for an `Unavailable`.
    async fn held(&self, stream: &str) {
        let row = ROW.replace("{stream}", &format!("'{stream}'"));
        self.execute(&format!(
            "INSERT INTO {{table}} ({COLUMNS}, held_since, next_attempt_at, holds, hold_kind, cause) \
             VALUES ({row}, now(), now() + interval '1 hour', 3, 'unavailable', 'kms.unavailable')"
        ))
        .await;
    }

    /// A row of `stream` parked for its own failure.
    async fn parked(&self, stream: &str) {
        let row = ROW.replace("{stream}", &format!("'{stream}'"));
        self.execute(&format!(
            "INSERT INTO {{table}} ({COLUMNS}, attempts, parked_at, cause) \
             VALUES ({row}, 3, now(), 'db.timeout')"
        ))
        .await;
    }

    /// A row of `stream` waiting for `on`, a `jsonb` text, since `ago`.
    async fn waiting(&self, stream: &str, on: &str, ago: &str) {
        let row = ROW.replace("{stream}", &format!("'{stream}'"));
        self.execute(&format!(
            "INSERT INTO {{table}} ({COLUMNS}, waiting_for, waiting_since) \
             VALUES ({row}, '{on}'::jsonb, now() - interval '{ago}')"
        ))
        .await;
    }

    /// Each row's version, by stream: its place, the transaction that wrote
    /// it, and the one that last locked or deleted it.
    async fn versions(&self) -> BTreeMap<String, String> {
        self.query(
            "SELECT stream_id->>'id', ctid::text || ' ' || xmin::text || ' ' || xmax::text \
             FROM {table}",
        )
        .await
        .into_iter()
        .map(|row| (row[0].clone().unwrap(), row[1].clone().unwrap()))
        .collect()
    }

    /// The streams parked, with their `last_error`, in order of arrival.
    async fn parked_rows(&self) -> Vec<(String, String)> {
        self.query(
            "SELECT stream_id->>'id', last_error FROM {table} \
             WHERE parked_at IS NOT NULL ORDER BY received_position",
        )
        .await
        .into_iter()
        .map(|row| (row[0].clone().unwrap(), row[1].clone().unwrap_or_default()))
        .collect()
    }

    async fn expire(&self) -> Expiry {
        self.inbox.expire().await.unwrap()
    }

    /// Whether a transaction that calls the expiry's statement from a
    /// pass's start, and nothing else, is given a transaction id.
    async fn expiry_takes_an_xid(&self) -> bool {
        let table = self.table.clone();
        self.sessions
            .session(async |session: PgSession| {
                session
                    .atomic(async |tx: PgSession| {
                        tx.connection()
                            .query(
                                &format!(
                                    "SELECT * FROM {table}__expire(3600::double precision, \
                                     100, NULL::timestamptz, NULL::bigint)"
                                ),
                                &[],
                            )
                            .await?;
                        let xid: Option<String> = tx
                            .connection()
                            .query_one("SELECT pg_current_xact_id_if_assigned()::text", &[])
                            .await?
                            .get(0);
                        Ok::<_, Error>(xid.is_some())
                    })
                    .await
            })
            .await
            .unwrap()
    }
}

/// Each parked row's stream and the reason its `last_error` names after the
/// dependency — the identities written as `…` — the whole text when it
/// names none.
fn reasons(parked: &[(String, String)]) -> Vec<(&str, String)> {
    parked
        .iter()
        .map(|(stream, error)| {
            let reason = match error.strip_prefix("dependency ") {
                Some(rest) => match rest.split_once(" waits on ") {
                    Some((_, on)) => format!(
                        "waits on …, which {}",
                        on.rsplit(", which ").next().unwrap_or_default()
                    ),
                    None => {
                        let ends = [
                            "never arrived",
                            "is parked",
                            "is processed",
                            "reaches no queued row within 8 hops",
                        ];
                        ends.into_iter()
                            .find(|end| rest.ends_with(&format!(" {end}")))
                            .unwrap_or(rest)
                            .to_owned()
                    }
                },
                None => error.clone(),
            };
            (stream.as_str(), reason)
        })
        .collect()
}

/// `reasons` as the tests write them.
fn stated<'a>(reasons: &[(&'a str, &str)]) -> Vec<(&'a str, String)> {
    reasons
        .iter()
        .map(|(stream, reason)| (*stream, (*reason).to_owned()))
        .collect()
}

/// The versions of the rows but those of `gone`.
fn only(versions: &BTreeMap<String, String>, gone: &[&str]) -> BTreeMap<String, String> {
    versions
        .iter()
        .filter(|(stream, _)| !gone.contains(&stream.as_str()))
        .map(|(stream, version)| (stream.clone(), version.clone()))
        .collect()
}

/// A waiter is spared while its dependency is queued — untried, held,
/// behind a held head — or waits on one so spared, and expires when its
/// dependency never arrived or is parked, or it waits through waiting rows
/// on one that never arrived or is parked, each with where its chain broke; a waiter not yet past `max_wait` is not examined.
/// A spared row is neither written nor locked: its version is the one it
/// had. Red when the spare drops the dependency's being queued, or a
/// parked dependency spares, or the spared rows are renewed by a write or
/// locked before the test.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_waiter_is_spared_while_its_dependency_is_on_its_way_and_parked_otherwise() {
    let f = fixture("spared").await;
    f.queued("q1").await;
    f.waiting("untried", &identity("q1"), "2 hours").await;
    f.held("h2").await;
    f.waiting("on_held", &identity("h2"), "2 hours").await;
    // b3 behind h3, a held head of its (slot, Tenant)
    f.held("h3").await;
    f.queued("b3").await;
    f.waiting("behind_held", &identity("b3"), "2 hours").await;
    f.queued("q4").await;
    f.waiting("w4", &identity("q4"), "2 hours").await;
    f.waiting("chain", &identity("w4"), "2 hours").await;
    f.waiting("missing", &identity("nowhere"), "2 hours").await;
    f.parked("p6").await;
    f.waiting("on_parked", &identity("p6"), "2 hours").await;
    // w7 waits for what never arrived, and has not waited long
    f.waiting("w7", &identity("nowhere-7"), "1 minute").await;
    f.waiting("chain_missing", &identity("w7"), "2 hours").await;
    // v9 waits for p9, parked, and has not waited long
    f.parked("p9").await;
    f.waiting("v9", &identity("p9"), "1 minute").await;
    f.waiting("chain_parked", &identity("v9"), "2 hours").await;
    f.waiting("young", &identity("nowhere-8"), "1 minute").await;
    let before = f.versions().await;

    assert_eq!(
        f.expire().await,
        Expiry {
            parked: 4,
            more: false
        }
    );
    assert_eq!(
        reasons(&f.parked_rows().await),
        stated(&[
            ("missing", "never arrived"),
            ("p6", ""),
            ("on_parked", "is parked"),
            ("chain_missing", "waits on …, which never arrived"),
            ("p9", ""),
            ("chain_parked", "waits on …, which is parked"),
        ])
    );
    let mut told = f.told.0.lock().unwrap().clone();
    told.sort();
    assert_eq!(
        told,
        ["chain_missing", "chain_parked", "missing", "on_parked"]
    );
    let expired = ["missing", "on_parked", "chain_missing", "chain_parked"];
    assert_eq!(
        only(&f.versions().await, &expired),
        only(&before, &expired),
        "a spared row, a young one, a dependency: neither written nor locked"
    );
    assert!(
        !f.expiry_takes_an_xid().await,
        "a pass over spared rows takes no transaction id"
    );
}

/// A chain of waiting rows spares its waiter when a queued row is within 8
/// hops, and does not at 9; a cycle reaches no queued row and expires every
/// waiter in it, the statement bounded by its hops, under a
/// `statement_timeout` a recursion without its bound would hit. Red at a
/// bound of 7 or of 16, and when the bound is dropped.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_chain_spares_within_eight_hops_and_a_cycle_expires() {
    let mut bounded = config();
    bounded.options("-c statement_timeout=5000");
    let f = fixture_on("hops", bounded, Duration::from_secs(3600)).await;
    // c0 → c1 → … → c7 → q8: c0 is 8 hops from a queued row
    f.queued("q8").await;
    for n in (0..8).rev() {
        let on = if n == 7 {
            identity("q8")
        } else {
            identity(&format!("c{}", n + 1))
        };
        f.waiting(&format!("c{n}"), &on, "2 hours").await;
    }
    // d0 → … → d8 → q9: d0 is 9 hops from it
    f.queued("q9").await;
    for n in (0..9).rev() {
        let on = if n == 8 {
            identity("q9")
        } else {
            identity(&format!("d{}", n + 1))
        };
        f.waiting(&format!("d{n}"), &on, "2 hours").await;
    }
    f.waiting("y0", &identity("y1"), "2 hours").await;
    f.waiting("y1", &identity("y0"), "2 hours").await;
    f.waiting("z", &identity("z"), "2 hours").await;

    assert_eq!(
        f.expire().await,
        Expiry {
            parked: 4,
            more: false
        }
    );
    assert_eq!(
        reasons(&f.parked_rows().await),
        stated(
            &["d0", "y0", "y1", "z"].map(|stream| (stream, "reaches no queued row within 8 hops"))
        )
    );
}

/// A pass reaches a waiter behind 20,000 spared ones, a batch a statement:
/// the statement that examines the last of them is the 201st, every one
/// before it examined a whole batch and parked nothing, and the spared rows
/// keep their versions; a statement over spared rows takes no transaction
/// id. Red when the key returned is the last row parked rather than the
/// last examined (the pass never moves), when the batch bounds the rows
/// parked rather than examined (one statement reads them all), when a
/// spared row is renewed by a write or locked before its test.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_pass_reaches_a_waiter_behind_twenty_thousand_spared_ones() {
    const SPARED: usize = 20_000;
    let f = fixture("spared_many").await;
    f.queued("q").await;
    let row = ROW.replace("{stream}", "('s' || n)");
    f.execute(&format!(
        "INSERT INTO {{table}} ({COLUMNS}, waiting_for, waiting_since) \
         SELECT {row}, '{}'::jsonb, now() - interval '2 hours' + n * interval '1 millisecond' \
         FROM generate_series(1, {SPARED}) n",
        identity("q")
    ))
    .await;
    f.waiting("last", &identity("nowhere"), "1 hour 59 minutes")
        .await;
    f.execute("VACUUM ANALYZE {table}").await;
    let before = f.versions().await;

    let started = Instant::now();
    let mut expiries = Vec::new();
    while expiries.len() < 2 * SPARED / BATCH {
        let expiry = f.expire().await;
        expiries.push(expiry);
        if expiry.parked > 0 {
            break;
        }
    }
    let took = started.elapsed();
    let statements = SPARED.div_ceil(BATCH) + 1;
    assert_eq!(expiries.len(), statements, "{expiries:?}");
    assert!(
        expiries[..statements - 1].iter().all(|expiry| *expiry
            == Expiry {
                parked: 0,
                more: true
            }),
        "{expiries:?}"
    );
    assert_eq!(
        expiries[statements - 1],
        Expiry {
            parked: 1,
            more: false
        }
    );
    assert_eq!(
        reasons(&f.parked_rows().await),
        stated(&[("last", "never arrived")])
    );
    assert_eq!(
        only(&f.versions().await, &["last"]),
        only(&before, &["last"]),
        "the spared rows neither written nor locked"
    );
    assert!(!f.expiry_takes_an_xid().await);
    eprintln!(
        "E1: a pass over {SPARED} spared waiters in {statements} statements took {took:?}, \
         {:?} a statement",
        took / statements as u32
    );
}

/// Waiters at one `waiting_since` are each examined once a pass, by their
/// order of arrival, whatever their rows' places: rows moved in the heap
/// by a write, a pass of three statements parks every one of them whose
/// dependency never arrived. Red when the pass is keyed by `waiting_since`
/// alone (a batch's ties past its end are skipped) or ordered by the rows'
/// places (a moved row lands behind the key).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn waiters_at_one_time_are_each_examined_once_a_pass() {
    const WAITERS: usize = 250;
    let f = fixture("ties").await;
    f.queued("q").await;
    let row = ROW.replace("{stream}", "('t' || n)");
    f.execute(&format!(
        "INSERT INTO {{table}} ({COLUMNS}, waiting_for, waiting_since) \
         SELECT {row}, CASE WHEN n % 10 = 0 THEN '{}'::jsonb ELSE '{}'::jsonb END, \
                date_trunc('second', now()) - interval '2 hours' \
         FROM generate_series(1, {WAITERS}) n; \
         UPDATE {{table}} SET metadata = '{{\"moved\": true}}' \
         WHERE waiting_since IS NOT NULL AND (stream_id->>'id' ~ '^t[0-9]*[05]$' \
                                             OR stream_id->>'id' ~ '^t1[0-9]$')",
        identity("nowhere"),
        identity("q")
    ))
    .await;
    let moved = f
        .query(
            "SELECT count(*)::text FROM {table} \
             WHERE ctid > (SELECT max(ctid) FROM {table} WHERE metadata = '{}')",
        )
        .await;
    assert_ne!(moved[0][0].as_deref(), Some("0"), "rows moved in the heap");

    let mut expiries = Vec::new();
    for _ in 0..WAITERS.div_ceil(BATCH) {
        expiries.push(f.expire().await);
    }
    assert_eq!(
        expiries
            .iter()
            .map(|expiry| expiry.more)
            .collect::<Vec<_>>(),
        [true, true, false]
    );
    assert_eq!(
        expiries.iter().map(|expiry| expiry.parked).sum::<usize>(),
        WAITERS / 10,
        "{expiries:?}"
    );
}

/// A `waiting_for` the library did not write — a position not a number, a
/// fraction, out of the column's range, null; an array, a string, an empty
/// object, a Tenant not a string — matches no row and expires as never
/// arrived, and aborts nothing; a position written `1.0` is the number 1,
/// which a dependency's mark compares equal, and is spared while its row is
/// queued. Red when the position is cast plainly.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_wait_written_otherwise_expires_and_aborts_nothing() {
    let f = fixture("malformed").await;
    f.queued("q").await;
    let of = |position: &str| {
        format!(
            r#"{{"tenant_id": "tenant-1", "stream_type": "orders.Order", "stream_id": {{"id": "q"}}, "stream_position": {position}}}"#
        )
    };
    let malformed = [
        ("text", of("\"x\"")),
        ("fraction", of("1.5")),
        ("huge", of("1e12")),
        ("null", of("null")),
        ("quoted", of("\"1\"")),
        ("array", "[1, 2]".to_owned()),
        ("string", "\"text\"".to_owned()),
        ("empty", "{}".to_owned()),
        ("numeric_tenant", r#"{"tenant_id": 5}"#.to_owned()),
    ];
    for (stream, on) in &malformed {
        f.waiting(stream, on, "2 hours").await;
    }
    f.waiting("decimal", &of("1.0"), "2 hours").await;

    assert_eq!(
        f.expire().await,
        Expiry {
            parked: malformed.len(),
            more: false
        }
    );
    let parked = f.parked_rows().await;
    assert!(
        reasons(&parked)
            .iter()
            .all(|(_, reason)| reason == "never arrived"),
        "{parked:?}"
    );
    assert_eq!(parked.len(), malformed.len());
    assert!(
        parked.iter().all(|(stream, _)| stream != "decimal"),
        "1.0 is the queued row's position"
    );
}

/// The rows the expiry parked keep the identity they waited for, and are
/// neither examined nor written again: a pass over 300 of them and one
/// waiter whose dependency never arrived examines that one alone.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_rows_the_expiry_parked_are_not_examined_again() {
    let f = fixture("expired_again").await;
    let row = ROW.replace("{stream}", "('e' || n)");
    f.execute(&format!(
        "INSERT INTO {{table}} ({COLUMNS}, waiting_for, parked_at, last_error) \
         SELECT {row}, '{}'::jsonb, now() - interval '1 day', 'dependency never arrived' \
         FROM generate_series(1, 300) n",
        identity("nowhere")
    ))
    .await;
    f.waiting("w", &identity("nowhere"), "2 hours").await;
    let before = f.versions().await;
    let examined = f
        .query(
            "SELECT expiry_examined::text FROM {table}__expire(3600::double precision, 100, \
             NULL::timestamptz, NULL::bigint)",
        )
        .await;
    assert_eq!(examined[0][0].as_deref(), Some("1"));
    assert_eq!(only(&f.versions().await, &["w"]), only(&before, &["w"]));
}

/// `run` expires on a timer of its own, and the rows it parks are told; a
/// dispatch alone parks nothing (ADR-0042 decision 7). Red when `run`
/// starts no expiry.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn run_expires_on_its_timer() {
    let f = fixture_on("timer", config(), Duration::from_millis(300)).await;
    let cause = InboxMessage::new(
        "tenant-1",
        "users.User",
        json!({ "id": "never" }),
        1,
        "kafka://users/never",
        "",
    );
    let effect = InboxMessage::new(
        "tenant-1",
        "orders.Order",
        json!({ "id": "o" }),
        1,
        "kafka://orders/o",
        "",
    )
    .depending_on(&[CausalDependency::on(&cause)]);
    f.inbox.publish(&effect).await.unwrap();
    let ok = |_: &PgSession, _: &InboxMessage| std::future::ready(Ok::<(), Failure>(()));
    f.inbox.dispatch(ok).await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    f.inbox.dispatch(ok).await.unwrap();
    assert!(f.parked_rows().await.is_empty(), "no dispatch expires");

    let loops = Loops {
        concurrency: 1,
        poll_interval: Duration::from_millis(50),
        max_error_wait: Duration::from_millis(200),
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    let watched = async {
        while f.parked_rows().await.is_empty() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    f.inbox.run(ok, loops, watched).await.unwrap();
    assert_eq!(
        reasons(&f.parked_rows().await),
        stated(&[("o", "never arrived")])
    );
    assert_eq!(*f.told.0.lock().unwrap(), ["o"]);
}

/// A defect of the expiry stops `run`, as one of a dispatch does
/// (ADR-0010): without its function, the call fails (42883), and so does
/// `run`, without waiting for a shutdown.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_defect_of_the_expiry_stops_run() {
    let f = fixture("defect").await;
    f.execute("DROP FUNCTION {table}__expire(double precision, integer, timestamptz, bigint)")
        .await;
    let code = |error: &Error| match error {
        Error::Database(error) => error.code().map(SqlState::code).map(str::to_owned),
        _ => None,
    };
    let failed = f.inbox.expire().await.unwrap_err();
    assert_eq!(code(&failed).as_deref(), Some("42883"), "{failed}");
    let ok = |_: &PgSession, _: &InboxMessage| std::future::ready(Ok::<(), Failure>(()));
    let stopped = tokio::time::timeout(
        Duration::from_secs(10),
        f.inbox
            .run(ok, Loops::default(), std::future::pending::<()>()),
    )
    .await
    .expect("run stops on the defect");
    assert_eq!(code(&stopped.unwrap_err()).as_deref(), Some("42883"));
}

/// A row with a time of wait is a waiter: the baseline refuses a statement
/// that parks it, processes it or ends its wait and keeps the time
/// (`__wait_is_open`), so that the expiry's index holds the waiters alone.
/// Red when the CHECK is dropped.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_statement_that_keeps_a_time_of_wait_on_no_waiter_is_refused() {
    let f = fixture("wait_open").await;
    f.waiting("w", &identity("nowhere"), "2 hours").await;
    for sql in [
        "UPDATE {table} SET parked_at = now()",
        "UPDATE {table} SET waiting_for = NULL",
        "UPDATE {table} SET waiting_for = NULL, processed_position = 1, processed_at = now()",
    ] {
        let sql = sql.replace("{table}", &f.table);
        let refused = f
            .sessions
            .session(async |session| {
                Ok::<_, Error>(session.connection().batch_execute(&sql).await?)
            })
            .await
            .unwrap_err();
        let code = match &refused {
            Error::Database(error) => error.code().map(SqlState::code),
            _ => None,
        };
        assert_eq!(code, Some("23514"), "{sql}: {refused}");
    }
}
