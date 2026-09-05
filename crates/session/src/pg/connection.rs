//! The connection a repository reaches through [`PgAccess`], and the statements
//! it reports to the observer.
//!
//! Narrowed to what a repository needs rather than proxying the whole driver:
//! the Python port wraps every attribute through `__getattr__`, which has no
//! counterpart here and no need for one.
//!
//! [`PgAccess`]: super::PgAccess

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use deadpool_postgres::Object;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Row};

use super::observer::{PgObserver, QueryEnded, QueryStarted};

/// Error reported by the driver.
pub type PgError = tokio_postgres::Error;

struct Shared {
    client: Object,
    observer: Arc<dyn PgObserver>,
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
    /// Wraps a pooled connection.
    pub(super) fn new(client: Object, observer: Arc<dyn PgObserver>) -> Self {
        PgConnection {
            shared: Arc::new(Shared {
                client,
                observer,
                savepoints: AtomicU64::new(0),
            }),
        }
    }

    /// The observer this connection reports to.
    pub(super) fn observer(&self) -> &Arc<dyn PgObserver> {
        &self.shared.observer
    }

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

    pub(super) fn next_savepoint(&self) -> String {
        format!(
            "sp{}",
            self.shared.savepoints.fetch_add(1, Ordering::SeqCst) + 1
        )
    }
}
