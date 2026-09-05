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

use ascetic_ddd_session::observer::{
    QueryEnded, ScopeEnded, ScopeKind, ScopeStarted, SessionObserver,
};
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::NoTls;
use ascetic_ddd_session::{PgAccess, PgSession, PgSessionPool, Session, SessionError, SessionPool};
use futures::future::BoxFuture;

const DEFAULT_URL: &str = "postgresql://devel:devel@localhost:5432/devel_karmabot_test";

fn make_pool() -> Pool {
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
        .max_size(4)
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
            repository.save(session, &order).await?;
            session
                .atomic(async |session| repository.save(session, &Order { id: order.id + 1 }).await)
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
        pool.session(async |session: &PgSession| {
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
            .session(async |session| place_order(repository, session, Order { id: 1 }).await)
            .await
            .unwrap();
        assert_eq!(id, 1);

        let count = pool
            .session(async |session| repository.count(session).await)
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
                    repository.save(session, &Order { id: 1 }).await?;

                    let nested: Result<(), AppError> = session
                        .atomic(async |session| {
                            repository.save(session, &Order { id: 2 }).await?;
                            Err(AppError::Domain("rejected"))
                        })
                        .await;
                    assert!(matches!(nested, Err(AppError::Domain(_))));

                    // The savepoint rolled back, but this transaction is alive.
                    repository.save(session, &Order { id: 3 }).await?;
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
                        repository.save(session, &Order { id: 1 }).await?;
                        Err(AppError::Domain("rejected"))
                    })
                    .await
            })
            .await;

        assert!(matches!(outcome, Err(AppError::Domain("rejected"))));

        let count = pool
            .session(async |session| repository.count(session).await)
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
                        repository.save(session, &Order { id: 1 }),
                        repository.save(session, &Order { id: 2 }),
                    )?;
                    Ok::<_, AppError>(())
                })
                .await
        })
        .await
        .unwrap();

        let count = pool
            .session(async |session| repository.count(session).await)
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

    pool.session(async |session: &PgSession| {
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
