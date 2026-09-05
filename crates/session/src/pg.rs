//! PostgreSQL adapter, built on `tokio-postgres` and `deadpool-postgres`.
//!
//! Available under the `pg` feature.
//!
//! # Why the driver's transaction type is not used
//!
//! `tokio_postgres::Transaction` borrows the client mutably, which would force
//! `&mut self` through every signature and rule out both an immutable session
//! value and concurrent work inside one scope. The scope boundary is therefore
//! issued as plain statements — `BEGIN`, `SAVEPOINT spN`, `COMMIT`,
//! `RELEASE`/`ROLLBACK TO` — which is exactly what the driver would send
//! anyway, while queries go through `Client`, whose methods take `&self`.
//!
//! Savepoint names come from a counter shared by the whole session tree, not
//! from the nesting depth: two sibling scopes may be open at once, and depth
//! alone would give them the same name.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use deadpool_postgres::{Object, Pool};
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Row};

use crate::error::SessionError;
use crate::identity_map::IdentityMap;
use crate::isolation::IsolationLevel;
use crate::observer::{
    Outcome, QueryEnded, QueryStarted, ScopeEnded, ScopeKind, ScopeStarted, SessionObserver,
};
use crate::session::{ScopeFlag, Session, SessionPool};

// Re-exported so that a user of this crate does not have to match versions
// with the driver and the pool independently.
pub use deadpool_postgres;
pub use tokio_postgres;

/// Error reported by the driver.
pub type PgError = tokio_postgres::Error;

/// The infrastructure capability: "this session speaks to PostgreSQL".
///
/// A repository asks for the capability instead of naming a concrete session
/// type, so it works with any session that offers it. The domain never
/// mentions this trait, which is what keeps the connection out of its reach.
///
/// ```ignore
/// impl<S: Session + PgAccess> OrderRepository<S> for PgOrderRepository { … }
/// ```
pub trait PgAccess {
    /// The connection of the current scope.
    fn connection(&self) -> &PgConnection;
}

/// State shared by a session and every scope nested in it.
struct Shared {
    client: Object,
    observer: Arc<dyn SessionObserver>,
    savepoints: AtomicU64,
}

/// A connection that reports what it executes.
///
/// The counterpart of Python's `AsyncConnectionDecorator`, but narrowed to the
/// operations a repository needs rather than proxying the whole driver through
/// `__getattr__`.
#[derive(Clone)]
pub struct PgConnection {
    shared: Arc<Shared>,
}

impl PgConnection {
    /// The underlying client, for what this wrapper does not cover
    /// (prepared statements, `COPY`, pipelining).
    ///
    /// Statements issued through it are not reported to the observer.
    pub fn client(&self) -> &Client {
        &self.shared.client
    }

    /// Executes a statement, returning the number of rows affected.
    pub async fn execute(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, PgError> {
        self.observed(statement, self.client().execute(statement, params))
            .await
    }

    /// Executes a query, returning all rows.
    pub async fn query(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, PgError> {
        self.observed(statement, self.client().query(statement, params))
            .await
    }

    /// Executes a query expected to return at most one row.
    pub async fn query_opt(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, PgError> {
        self.observed(statement, self.client().query_opt(statement, params))
            .await
    }

    /// Executes a query expected to return exactly one row.
    pub async fn query_one(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, PgError> {
        self.observed(statement, self.client().query_one(statement, params))
            .await
    }

    /// Executes a sequence of simple statements.
    pub async fn batch_execute(&self, sql: &str) -> Result<(), PgError> {
        self.observed(sql, self.client().batch_execute(sql)).await
    }

    /// Times the call and reports it to the observer.
    async fn observed<T>(
        &self,
        statement: &str,
        call: impl Future<Output = Result<T, PgError>>,
    ) -> Result<T, PgError> {
        let observer = &self.shared.observer;
        observer.on_query_started(&QueryStarted { statement });

        let started = Instant::now();
        let outcome = call.await;

        observer.on_query_ended(&QueryEnded {
            statement,
            elapsed: started.elapsed(),
            failed: outcome.is_err(),
        });
        outcome
    }

    fn next_savepoint(&self) -> String {
        format!(
            "sp{}",
            self.shared.savepoints.fetch_add(1, Ordering::SeqCst) + 1
        )
    }
}

/// A session bound to one pooled connection.
#[derive(Clone)]
pub struct PgSession {
    connection: PgConnection,
    identity_map: Arc<IdentityMap>,
    isolation: IsolationLevel,
    depth: usize,
    /// Set while a scope opened on this session, or on a clone of it, is running.
    scope_open: ScopeFlag,
}

impl PgSession {
    /// The identity map of the current scope.
    ///
    /// Disabled outside a transaction, fresh for the outermost one, shared with
    /// every scope nested in it.
    pub fn identity_map(&self) -> &IdentityMap {
        &self.identity_map
    }

    /// Number of transaction scopes currently open around this session.
    pub fn depth(&self) -> usize {
        self.depth
    }

    fn child(&self) -> Self {
        PgSession {
            connection: self.connection.clone(),
            identity_map: if self.depth == 0 {
                Arc::new(IdentityMap::with_isolation(self.isolation))
            } else {
                Arc::clone(&self.identity_map)
            },
            isolation: self.isolation,
            depth: self.depth + 1,
            scope_open: ScopeFlag::new(),
        }
    }
}

impl PgAccess for PgSession {
    fn connection(&self) -> &PgConnection {
        &self.connection
    }
}

impl Session for PgSession {
    async fn atomic<T, E, F>(&self, scope: F) -> Result<T, E>
    where
        F: AsyncFnOnce(&Self) -> Result<T, E>,
        E: From<SessionError>,
    {
        // Claimed for the whole scope and released on the way out, whether the
        // scope returns, fails early or unwinds.
        let _guard = self.scope_open.acquire()?;

        let savepoint = (self.depth > 0).then(|| self.connection.next_savepoint());
        let kind = if savepoint.is_some() {
            ScopeKind::Savepoint
        } else {
            ScopeKind::Transaction
        };
        let depth = self.depth + 1;
        let observer = Arc::clone(&self.connection.shared.observer);

        let open = match &savepoint {
            None => "BEGIN".to_owned(),
            Some(name) => format!("SAVEPOINT {name}"),
        };
        self.connection
            .batch_execute(&open)
            .await
            .map_err(|error| SessionError::Begin(Box::new(error)))?;
        observer.on_scope_started(&ScopeStarted { depth, kind });

        let child = self.child();
        let outcome = scope(&child).await;
        let committed = outcome.is_ok();

        let close = match (&savepoint, committed) {
            (None, true) => "COMMIT".to_owned(),
            (None, false) => "ROLLBACK".to_owned(),
            (Some(name), true) => format!("RELEASE SAVEPOINT {name}"),
            (Some(name), false) => format!("ROLLBACK TO SAVEPOINT {name}"),
        };
        let closed = self.connection.batch_execute(&close).await;

        observer.on_scope_ended(&ScopeEnded {
            depth,
            kind,
            outcome: if committed {
                Outcome::Committed
            } else {
                Outcome::RolledBack
            },
        });

        // The identity map lives exactly as long as the outermost transaction.
        if self.depth == 0 {
            child.identity_map.clear();
        }

        match (outcome, closed) {
            // A failing commit is a failure of the whole scope: the caller
            // believes the work is durable, and it is not.
            (Ok(_), Err(error)) => Err(SessionError::Commit(Box::new(error)).into()),
            // A failing rollback does not replace the error that caused it;
            // the observer has already seen it as a failed statement.
            (outcome, _) => outcome,
        }
    }
}

/// Hands out sessions backed by a `deadpool` connection pool.
pub struct PgSessionPool {
    pool: Pool,
    observer: Arc<dyn SessionObserver>,
    isolation: IsolationLevel,
}

impl PgSessionPool {
    /// Creates a pool with no observer and the default isolation level.
    pub fn new(pool: Pool) -> Self {
        PgSessionPool {
            pool,
            observer: Arc::new(()),
            isolation: IsolationLevel::default(),
        }
    }

    /// Returns a pool that notifies `observer`.
    ///
    /// Wiring is a value, not a registration: this consumes the pool and
    /// returns a new one.
    pub fn observed_by(self, observer: impl SessionObserver + 'static) -> Self {
        PgSessionPool {
            observer: Arc::new(observer),
            ..self
        }
    }

    /// Returns a pool whose transactions use the given isolation level for
    /// their identity map.
    pub fn with_isolation(self, isolation: IsolationLevel) -> Self {
        PgSessionPool { isolation, ..self }
    }

    /// The underlying connection pool.
    pub fn pool(&self) -> &Pool {
        &self.pool
    }
}

impl SessionPool for PgSessionPool {
    type Session = PgSession;

    async fn session<T, E, F>(&self, scope: F) -> Result<T, E>
    where
        F: AsyncFnOnce(&Self::Session) -> Result<T, E>,
        E: From<SessionError>,
    {
        let client = self
            .pool
            .get()
            .await
            .map_err(|error| SessionError::Acquire(Box::new(error)))?;

        let session = PgSession {
            connection: PgConnection {
                shared: Arc::new(Shared {
                    client,
                    observer: Arc::clone(&self.observer),
                    savepoints: AtomicU64::new(0),
                }),
            },
            // Outside a transaction nothing may be cached.
            identity_map: Arc::new(IdentityMap::with_isolation(IsolationLevel::ReadUncommitted)),
            isolation: self.isolation,
            depth: 0,
            scope_open: ScopeFlag::new(),
        };

        self.observer.on_scope_started(&ScopeStarted {
            depth: 0,
            kind: ScopeKind::Session,
        });

        let outcome = scope(&session).await;

        self.observer.on_scope_ended(&ScopeEnded {
            depth: 0,
            kind: ScopeKind::Session,
            outcome: if outcome.is_ok() {
                Outcome::Committed
            } else {
                Outcome::RolledBack
            },
        });

        outcome
        // The connection returns to the pool as `session` is dropped here.
    }
}
