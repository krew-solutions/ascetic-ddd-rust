//! Integration tests for the PostgreSQL adapter.
//!
//! They need a live PostgreSQL, so they are ignored by default:
//!
//! ```text
//! ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
//!     cargo test -p ascetic-ddd-session --features pg -- --ignored
//! ```

#![cfg(feature = "pg")]

use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ascetic_ddd_session::observer::{
    Outcome, ScopeEnded, ScopeKind, ScopeStarted, SessionObserver,
};
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::NoTls;
use ascetic_ddd_session::pg::{PgObserver, QueryEnded};
use ascetic_ddd_session::{PgAccess, PgSession, PgSessionPool, Session, SessionError, SessionPool};
use futures::future::BoxFuture;

const DEFAULT_URL: &str = "postgresql://devel:devel@localhost:5432/devel_karmabot_test";

fn make_pool() -> Pool {
    make_pool_of(4)
}

fn make_pool_of(max_size: usize) -> Pool {
    let url = std::env::var("ASCETIC_DDD_TEST_PG_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned());
    let config = ascetic_ddd_session::pg::tokio_postgres::Config::from_str(&url)
        .expect("a valid PostgreSQL URL");
    let manager = Manager::from_config(
        config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    Pool::builder(manager)
        .max_size(max_size)
        .build()
        .expect("the pool can be built")
}

// ----------------------------- the domain -----------------------------

#[derive(Debug)]
enum AppError {
    Session(SessionError),
    Driver(String),
    Domain(&'static str),
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppError::Session(error) => write!(f, "session: {error}"),
            AppError::Driver(message) => write!(f, "driver: {message}"),
            AppError::Domain(message) => write!(f, "domain: {message}"),
        }
    }
}

impl From<SessionError> for AppError {
    fn from(error: SessionError) -> Self {
        AppError::Session(error)
    }
}

struct Order {
    id: i64,
}

/// Port: the session is a type parameter, so the domain never learns what it is.
trait OrderRepository<S: Session>: Sync {
    fn save<'a>(&'a self, session: &'a S, order: &'a Order) -> BoxFuture<'a, Result<(), AppError>>;
    fn count<'a>(&'a self, session: &'a S) -> BoxFuture<'a, Result<i64, AppError>>;
}

/// Application service: a transaction with a nested scope inside it.
async fn place_order<S, R>(repository: &R, session: &S, order: Order) -> Result<i64, AppError>
where
    S: Session,
    R: OrderRepository<S>,
{
    session
        .atomic(async |session| {
            repository.save(&session, &order).await?;
            session
                .atomic(async |session| {
                    repository.save(&session, &Order { id: order.id + 1 }).await
                })
                .await?;
            Ok(order.id)
        })
        .await
}

// -------------------------- the infrastructure --------------------------

struct PgOrderRepository {
    table: String,
}

// The repository names the capability, not the concrete session type.
impl<S: Session + PgAccess> OrderRepository<S> for PgOrderRepository {
    fn save<'a>(&'a self, session: &'a S, order: &'a Order) -> BoxFuture<'a, Result<(), AppError>> {
        Box::pin(async move {
            session
                .connection()
                .execute(
                    &format!("INSERT INTO {} (id) VALUES ($1)", self.table),
                    &[&order.id],
                )
                .await
                .map_err(|error| AppError::Driver(error.to_string()))?;
            Ok(())
        })
    }

    fn count<'a>(&'a self, session: &'a S) -> BoxFuture<'a, Result<i64, AppError>> {
        Box::pin(async move {
            let row = session
                .connection()
                .query_one(&format!("SELECT count(*) FROM {}", self.table), &[])
                .await
                .map_err(|error| AppError::Driver(error.to_string()))?;
            Ok(row.get(0))
        })
    }
}

// ------------------------------ fixture ------------------------------

/// Creates a table for the test and drops it afterwards.
async fn with_table<F>(name: &str, body: F)
where
    F: AsyncFnOnce(&PgSessionPool, &PgOrderRepository),
{
    let pool = PgSessionPool::new(make_pool());
    let repository = PgOrderRepository {
        table: name.to_owned(),
    };

    let ddl = async |sql: String| {
        pool.session(async |session: PgSession| {
            session
                .connection()
                .batch_execute(&sql)
                .await
                .map_err(|error| AppError::Driver(error.to_string()))
        })
        .await
        .expect("DDL succeeds")
    };

    ddl(format!(
        "DROP TABLE IF EXISTS {name}; CREATE TABLE {name} (id bigint primary key)"
    ))
    .await;

    body(&pool, &repository).await;

    ddl(format!("DROP TABLE IF EXISTS {name}")).await;
}

// ------------------------------- tests -------------------------------

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn nested_scope_commits_through_a_savepoint() {
    with_table("ascetic_pg_nested", async |pool, repository| {
        let id = pool
            .session(async |session| place_order(repository, &session, Order { id: 1 }).await)
            .await
            .unwrap();
        assert_eq!(id, 1);

        let count = pool
            .session(async |session| repository.count(&session).await)
            .await
            .unwrap();
        assert_eq!(count, 2, "both the outer and the nested insert are durable");
    })
    .await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn failing_nested_scope_leaves_the_outer_transaction_alive() {
    with_table("ascetic_pg_savepoint", async |pool, repository| {
        pool.session(async |session| {
            session
                .atomic(async |session| {
                    repository.save(&session, &Order { id: 1 }).await?;

                    let nested: Result<(), AppError> = session
                        .atomic(async |session| {
                            repository.save(&session, &Order { id: 2 }).await?;
                            Err(AppError::Domain("rejected"))
                        })
                        .await;
                    assert!(matches!(nested, Err(AppError::Domain(_))));

                    // The savepoint rolled back, but this transaction is alive.
                    repository.save(&session, &Order { id: 3 }).await?;
                    Ok::<_, AppError>(())
                })
                .await
        })
        .await
        .unwrap();

        let rows = pool
            .session(async |session| {
                let rows = session
                    .connection()
                    .query("SELECT id FROM ascetic_pg_savepoint ORDER BY id", &[])
                    .await
                    .map_err(|error| AppError::Driver(error.to_string()))?;
                Ok::<_, AppError>(
                    rows.iter()
                        .map(|row| row.get::<_, i64>(0))
                        .collect::<Vec<_>>(),
                )
            })
            .await
            .unwrap();

        assert_eq!(rows, [1, 3], "the row written inside the savepoint is gone");
    })
    .await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn failing_outer_scope_rolls_everything_back() {
    with_table("ascetic_pg_rollback", async |pool, repository| {
        let outcome: Result<(), AppError> = pool
            .session(async |session| {
                session
                    .atomic(async |session| {
                        repository.save(&session, &Order { id: 1 }).await?;
                        Err(AppError::Domain("rejected"))
                    })
                    .await
            })
            .await;

        assert!(matches!(outcome, Err(AppError::Domain("rejected"))));

        let count = pool
            .session(async |session| repository.count(&session).await)
            .await
            .unwrap();
        assert_eq!(count, 0);
    })
    .await;
}

/// `&self` lets independent statements inside one scope be pipelined.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn independent_statements_inside_one_scope_run_concurrently() {
    with_table("ascetic_pg_concurrent", async |pool, repository| {
        pool.session(async |session| {
            session
                .atomic(async |session| {
                    futures::try_join!(
                        repository.save(&session, &Order { id: 1 }),
                        repository.save(&session, &Order { id: 2 }),
                    )?;
                    Ok::<_, AppError>(())
                })
                .await
        })
        .await
        .unwrap();

        let count = pool
            .session(async |session| repository.count(&session).await)
            .await
            .unwrap();
        assert_eq!(count, 2);
    })
    .await;
}

#[derive(Default)]
struct Recording {
    statements: Mutex<Vec<String>>,
    scopes: Mutex<Vec<(usize, ScopeKind)>>,
    timed: AtomicUsize,
}

impl SessionObserver for Recording {
    fn on_scope_started(&self, event: &ScopeStarted) {
        self.scopes.lock().unwrap().push((event.depth, event.kind));
    }

    fn on_scope_ended(&self, _event: &ScopeEnded) {}
}

impl PgObserver for Recording {
    fn on_query_ended(&self, event: &QueryEnded<'_>) {
        self.statements
            .lock()
            .unwrap()
            .push(event.statement.to_owned());
        if !event.elapsed.is_zero() {
            self.timed.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn observer_sees_the_real_statements() {
    let recording = Arc::new(Recording::default());
    let pool = PgSessionPool::new(make_pool()).observed_by(Arc::clone(&recording));

    pool.session(async |session: PgSession| {
        session
            .atomic(async |session| {
                session
                    .connection()
                    .query_one("SELECT 1", &[])
                    .await
                    .map_err(|error| AppError::Driver(error.to_string()))?;
                session.atomic(async |_session| Ok::<_, AppError>(())).await
            })
            .await
    })
    .await
    .unwrap();

    assert_eq!(
        *recording.statements.lock().unwrap(),
        [
            "BEGIN",
            "SELECT 1",
            "SAVEPOINT sp1",
            "RELEASE SAVEPOINT sp1",
            "COMMIT",
        ],
    );
    assert_eq!(
        *recording.scopes.lock().unwrap(),
        [
            (0, ScopeKind::Session),
            (1, ScopeKind::Transaction),
            (2, ScopeKind::Savepoint),
        ],
    );
    assert!(
        recording.timed.load(Ordering::SeqCst) > 0,
        "statements are timed",
    );
}

/// Two scopes opened side by side on one session would share a savepoint stack:
/// releasing the older one silently destroys the newer. The compiler cannot
/// rule this out once the session is shared by `&`, so it is refused at run
/// time — with an error that names the cause.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn concurrent_scopes_on_one_session_are_refused() {
    with_table("ascetic_pg_guard", async |pool, repository| {
        pool.session(async |session| {
            let (first, second) = futures::join!(
                session.atomic(async |scope| { repository.save(&scope, &Order { id: 1 }).await }),
                session.atomic(async |scope| { repository.save(&scope, &Order { id: 2 }).await }),
            );

            // The one that got there first ran normally.
            assert!(first.is_ok(), "first scope: {first:?}");
            // The other was refused before it could touch the connection.
            assert!(
                matches!(
                    second,
                    Err(AppError::Session(SessionError::ScopeAlreadyOpen)),
                ),
                "second scope: {second:?}",
            );
            Ok::<_, AppError>(())
        })
        .await
        .unwrap();

        let count = pool
            .session(async |session| repository.count(&session).await)
            .await
            .unwrap();
        assert_eq!(count, 1, "only the scope that ran wrote anything");
    })
    .await;
}

/// A scope dropped mid-statement leaves the connection inside a transaction.
/// The connection is poisoned and taken out of the pool: the next session gets
/// a fresh one, outside any transaction.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_dropped_scope_discards_its_connection() {
    let pool = make_pool_of(1);
    let sessions = PgSessionPool::new(pool.clone());

    let cut_short = tokio::time::timeout(
        Duration::from_millis(200),
        sessions.session(async |session| {
            session
                .atomic(async |tx| {
                    tx.connection()
                        .batch_execute("SELECT pg_sleep(30)")
                        .await
                        .map_err(|error| AppError::Driver(error.to_string()))?;
                    Ok::<(), AppError>(())
                })
                .await
        }),
    )
    .await;
    assert!(cut_short.is_err(), "the scope was cut short by the timeout");
    assert_eq!(
        pool.status().size,
        0,
        "the poisoned connection left the pool"
    );

    let in_transaction: bool = sessions
        .session(async |session| {
            let row = session
                .connection()
                .query_one("SELECT pg_current_xact_id_if_assigned() IS NOT NULL", &[])
                .await
                .map_err(|error| AppError::Driver(error.to_string()))?;
            Ok::<bool, AppError>(row.get(0))
        })
        .await
        .unwrap();
    assert!(
        !in_transaction,
        "the next session starts on a clean connection"
    );
}

/// An inner scope dropped mid-way poisons the connection: the outer scope is
/// refused at commit and rolled back, and the connection is discarded with it.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn an_outer_scope_is_refused_after_an_inner_one_was_dropped() {
    let pool = make_pool_of(1);
    let sessions = PgSessionPool::new(pool.clone());

    let outcome = sessions
        .session(async |session| {
            session
                .atomic(async |tx| {
                    let inner = Box::pin(tx.atomic(async |_| {
                        std::future::pending::<()>().await;
                        Ok::<(), AppError>(())
                    }));
                    let _ = futures::future::select(inner, std::future::ready(())).await;
                    Ok::<(), AppError>(())
                })
                .await
        })
        .await;

    assert!(matches!(
        outcome,
        Err(AppError::Session(SessionError::Abandoned))
    ));
    assert_eq!(
        pool.status().size,
        0,
        "the poisoned connection left the pool"
    );
}

// ------------------- an absorbed failure (ADR-0028) -------------------

/// A table whose second unique index refuses a holder twice — the shape that
/// `ON CONFLICT (id) DO NOTHING` does not cover — and a child whose foreign
/// key is checked at commit.
async fn with_held_table<F>(name: &str, body: F)
where
    F: AsyncFnOnce(&PgSessionPool, &str),
{
    with_held_table_observed(name, Arc::new(()), body).await;
}

async fn with_held_table_observed<F>(name: &str, observer: Arc<dyn PgObserver>, body: F)
where
    F: AsyncFnOnce(&PgSessionPool, &str),
{
    let pool = PgSessionPool::new(make_pool()).observed_by(observer);
    let ddl = async |sql: String| {
        PgSessionPool::new(make_pool())
            .session(async |session: PgSession| {
                session
                    .connection()
                    .batch_execute(&sql)
                    .await
                    .map_err(|error| AppError::Driver(error.to_string()))
            })
            .await
            .expect("DDL succeeds")
    };
    ddl(format!(
        "DROP TABLE IF EXISTS {name}_child, {name}; \
         CREATE TABLE {name} (id bigint PRIMARY KEY, holder bigint NOT NULL); \
         CREATE UNIQUE INDEX {name}_once ON {name} (holder); \
         CREATE TABLE {name}_child (id bigint PRIMARY KEY, \
           parent bigint REFERENCES {name} DEFERRABLE INITIALLY DEFERRED)"
    ))
    .await;
    body(&pool, name).await;
    ddl(format!("DROP TABLE IF EXISTS {name}_child, {name}")).await;
}

/// Holds `holder` under `id`, as a store would: a refusal is the server's.
async fn hold<S: PgAccess>(session: &S, table: &str, id: i64, holder: i64) -> Result<(), AppError> {
    session
        .connection()
        .execute(
            &format!(
                "INSERT INTO {table} (id, holder) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING"
            ),
            &[&id, &holder],
        )
        .await
        .map(|_| ())
        .map_err(|error| AppError::Driver(error.to_string()))
}

async fn held(pool: &PgSessionPool, table: &str) -> Vec<i64> {
    pool.session(async |session: PgSession| {
        let rows = session
            .connection()
            .query(&format!("SELECT id FROM {table} ORDER BY id"), &[])
            .await
            .map_err(|error| AppError::Driver(error.to_string()))?;
        Ok::<_, AppError>(rows.iter().map(|row| row.get(0)).collect())
    })
    .await
    .unwrap()
}

/// A scope that catches a refused statement and carries on reported success
/// while the server, which aborts a transaction on its first failed
/// statement, rolled the whole transaction back at its `COMMIT`. It is
/// refused instead, naming the failure it absorbed.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_scope_that_absorbed_a_refused_statement_is_aborted_and_keeps_nothing() {
    with_held_table("ascetic_pg_absorbed", async |pool, table| {
        let outcome = pool
            .session(async |session: PgSession| {
                session
                    .atomic(async |tx| {
                        hold(&tx, table, 1, 7).await?;
                        // "Held already": the refusal absorbed, the scope going on.
                        let _ = hold(&tx, table, 2, 7).await;
                        Ok::<(), AppError>(())
                    })
                    .await
            })
            .await;
        match outcome {
            Err(AppError::Session(SessionError::Aborted { cause: Some(cause) })) => {
                assert_eq!(cause.sqlstate, "23505");
                assert_eq!(
                    cause.constraint.as_deref(),
                    Some("ascetic_pg_absorbed_once")
                );
            }
            other => panic!("the absorbed refusal is not reported: {other:?}"),
        }
        assert_eq!(
            held(pool, table).await,
            Vec::<i64>::new(),
            "nothing is durable"
        );
    })
    .await;
}

/// A nested scope that absorbed a refused statement is rolled back to its
/// savepoint and refused; the enclosing transaction stays alive, so its
/// caller may absorb that refusal and commit the rest.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_nested_scope_that_absorbed_a_refusal_is_aborted_and_the_outer_one_goes_on() {
    with_held_table("ascetic_pg_absorbed_nested", async |pool, table| {
        pool.session(async |session: PgSession| {
            session
                .atomic(async |tx| {
                    hold(&tx, table, 1, 1).await?;
                    let inner = tx
                        .atomic(async |tx| {
                            let _ = hold(&tx, table, 2, 1).await;
                            Ok::<(), AppError>(())
                        })
                        .await;
                    assert!(
                        matches!(
                            inner,
                            Err(AppError::Session(SessionError::Aborted { cause: Some(_) }))
                        ),
                        "the nested scope is refused: {inner:?}"
                    );
                    hold(&tx, table, 3, 3).await
                })
                .await
        })
        .await
        .expect("the outer transaction commits");
        assert_eq!(held(pool, table).await, [1, 3]);
    })
    .await;
}

/// A rollback that fails leaves the connection in a state nobody knows: here a
/// scope released its own savepoint, so the rollback to it finds none. The
/// connection is poisoned — the enclosing scope refused and rolled back, the
/// connection discarded — rather than returned to the pool mid-transaction.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_failed_rollback_poisons_the_connection() {
    with_held_table("ascetic_pg_failed_rollback", async |pool, table| {
        pool.session(async |session: PgSession| {
            let outer = session
                .atomic(async |tx| {
                    hold(&tx, table, 1, 1).await?;
                    let inner = tx
                        .atomic(async |tx| {
                            tx.connection()
                                .batch_execute("RELEASE SAVEPOINT sp1")
                                .await
                                .map_err(|error| AppError::Driver(error.to_string()))?;
                            Err::<(), AppError>(AppError::Domain("refused"))
                        })
                        .await;
                    assert!(inner.is_err());
                    Ok::<(), AppError>(())
                })
                .await;
            assert!(
                matches!(outer, Err(AppError::Session(SessionError::Abandoned))),
                "the enclosing scope is refused: {outer:?}"
            );
            assert!(session.is_abandoned(), "the connection is poisoned");
            Ok::<(), AppError>(())
        })
        .await
        .unwrap();
        assert_eq!(held(pool, table).await, Vec::<i64>::new());
    })
    .await;
}

/// What the observer sees of an absorbed failure: the close reported as the
/// `COMMIT` it is, failed; the rollback after it; the scope ended as failed.
#[derive(Default)]
struct Closing {
    statements: Mutex<Vec<(String, bool)>>,
    ended: Mutex<Vec<(usize, Outcome)>>,
}

impl SessionObserver for Closing {
    fn on_scope_ended(&self, event: &ScopeEnded) {
        self.ended
            .lock()
            .unwrap()
            .push((event.depth, event.outcome));
    }
}

impl PgObserver for Closing {
    fn on_query_ended(&self, event: &QueryEnded<'_>) {
        self.statements
            .lock()
            .unwrap()
            .push((event.statement.to_owned(), event.failed));
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_observer_sees_an_absorbed_failure_end_the_scope_as_failed() {
    let closing = Arc::new(Closing::default());
    let observer: Arc<dyn PgObserver> = Arc::clone(&closing) as Arc<dyn PgObserver>;
    with_held_table_observed("ascetic_pg_absorbed_seen", observer, async |pool, table| {
        let outcome = pool
            .session(async |session: PgSession| {
                session
                    .atomic(async |tx| {
                        hold(&tx, table, 1, 7).await?;
                        let _ = hold(&tx, table, 2, 7).await;
                        Ok::<(), AppError>(())
                    })
                    .await
            })
            .await;
        assert!(outcome.is_err());
    })
    .await;
    let statements = closing.statements.lock().unwrap().clone();
    let closes: Vec<(String, bool)> = statements
        .into_iter()
        .filter(|(statement, _)| !statement.starts_with("INSERT"))
        .collect();
    assert_eq!(
        closes,
        [
            ("BEGIN".to_owned(), false),
            ("COMMIT".to_owned(), true),
            ("ROLLBACK".to_owned(), false),
        ]
    );
    assert_eq!(
        *closing.ended.lock().unwrap(),
        [(1, Outcome::Failed), (0, Outcome::Failed)]
    );
}

/// The verdict is the server's, not the connection's memory: a failure the
/// scope recovered from itself — to a savepoint of its own — leaves the
/// transaction whole, and it commits.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_failure_the_scope_recovered_from_itself_does_not_abort_it() {
    with_held_table("ascetic_pg_recovered", async |pool, table| {
        pool.session(async |session: PgSession| {
            session
                .atomic(async |tx| {
                    hold(&tx, table, 1, 7).await?;
                    let statement = async |sql: &str| {
                        tx.connection()
                            .batch_execute(sql)
                            .await
                            .map_err(|error| AppError::Driver(error.to_string()))
                    };
                    statement("SAVEPOINT manual").await?;
                    let _ = hold(&tx, table, 2, 7).await;
                    statement("ROLLBACK TO SAVEPOINT manual").await?;
                    hold(&tx, table, 3, 3).await
                })
                .await
        })
        .await
        .expect("the transaction commits");
        assert_eq!(held(pool, table).await, [1, 3]);
    })
    .await;
}

/// A COMMIT the server refuses for a reason of its own — here a foreign key
/// checked at commit — stays a failed commit, not an absorbed failure; the
/// server has ended the transaction, so the connection goes back to the pool.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_commit_the_server_refuses_is_a_failed_commit_and_the_connection_stays() {
    with_held_table("ascetic_pg_refused_commit", async |pool, table| {
        pool.session(async |session: PgSession| {
            let outcome = session
                .atomic(async |tx| {
                    tx.connection()
                        .execute(
                            &format!("INSERT INTO {table}_child (id, parent) VALUES (1, 42)"),
                            &[],
                        )
                        .await
                        .map_err(|error| AppError::Driver(error.to_string()))?;
                    Ok::<(), AppError>(())
                })
                .await;
            match outcome {
                Err(AppError::Session(error @ SessionError::Commit { .. })) => {
                    assert!(!error.is_transient(), "a foreign key is a defect: {error}");
                }
                other => panic!("not a failed commit: {other:?}"),
            }
            assert!(!session.is_abandoned(), "the connection is healthy");
            session
                .atomic(async |tx| hold(&tx, table, 1, 1).await)
                .await
        })
        .await
        .expect("the connection takes the next transaction");
        assert_eq!(held(pool, table).await, [1]);
    })
    .await;
}

/// A statement that failed outside any transaction aborted nothing: the next
/// scope on the session begins clean and commits.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_failure_before_a_scope_does_not_abort_it() {
    with_held_table("ascetic_pg_before", async |pool, table| {
        pool.session(async |session: PgSession| {
            hold(&session, table, 1, 7).await?;
            let _ = hold(&session, table, 2, 7).await;
            session
                .atomic(async |tx| hold(&tx, table, 3, 3).await)
                .await
        })
        .await
        .expect("the scope commits");
        assert_eq!(held(pool, table).await, [1, 3]);
    })
    .await;
}
