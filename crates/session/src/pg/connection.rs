//! The connection a repository reaches through [`PgAccess`], and the statements
//! it reports to the observer.
//!
//! Narrowed to what a repository needs rather than proxying the whole driver:
//! the Python port wraps every attribute through `__getattr__`, which has no
//! counterpart here and no need for one. Every statement goes through it — the
//! driver's client is not handed out — so the connection sees every failure
//! the server reports, and remembers the one that aborted the transaction, for
//! the session to name if the scope absorbed it (ADR-0028).
//!
//! [`PgAccess`]: super::PgAccess

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use deadpool_postgres::Object;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Row};

use super::observer::{PgObserver, QueryEnded, QueryStarted};
use crate::error::AbortCause;

/// Error reported by the driver.
pub type PgError = tokio_postgres::Error;

struct Shared {
    /// `None` only while `Drop` runs.
    client: Option<Object>,
    observer: Arc<dyn PgObserver>,
    savepoints: AtomicU64,
    /// Set when a scope's future was dropped before the scope closed, or a
    /// rollback failed: the connection may be inside a transaction nobody
    /// will finish.
    poisoned: AtomicBool,
    /// The failure the server reported since the current transaction began
    /// or a scope last closed: what aborted it, if anything did. It names the
    /// cause of an absorbed failure and decides nothing — the server's answer
    /// to the close does (ADR-0028).
    aborted_by: Mutex<Option<AbortCause>>,
}

impl Drop for Shared {
    fn drop(&mut self) {
        // A poisoned connection is taken out of the pool for good; the server
        // ends whatever transaction it was in when the socket closes. A
        // healthy one returns to the pool as usual.
        if self.poisoned.load(Ordering::SeqCst) {
            if let Some(client) = self.client.take() {
                drop(Object::take(client));
            }
        }
    }
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
                client: Some(client),
                observer,
                savepoints: AtomicU64::new(0),
                poisoned: AtomicBool::new(false),
                aborted_by: Mutex::new(None),
            }),
        }
    }

    /// The observer this connection reports to.
    pub(super) fn observer(&self) -> &Arc<dyn PgObserver> {
        &self.shared.observer
    }

    /// The underlying client, kept to this wrapper: a statement issued past
    /// it would be neither observed nor remembered (ADR-0028).
    fn client(&self) -> &Client {
        self.shared
            .client
            .as_ref()
            .expect("the client lives as long as the connection")
    }

    /// Whether a scope on this connection was dropped before it closed.
    pub fn is_poisoned(&self) -> bool {
        self.shared.poisoned.load(Ordering::SeqCst)
    }

    pub(super) fn poisoned(&self) -> &AtomicBool {
        &self.shared.poisoned
    }

    /// Takes the connection out of the pool for good when its last handle
    /// drops: its transaction state is no longer known.
    pub(super) fn poison(&self) {
        self.shared.poisoned.store(true, Ordering::SeqCst);
    }

    /// The failure that aborted the transaction, forgotten as it is taken.
    pub(super) fn take_abort_cause(&self) -> Option<AbortCause> {
        self.aborted_by().take()
    }

    /// Forgets what aborted a transaction that has since ended or been
    /// restored.
    pub(super) fn forget_abort_cause(&self) {
        *self.aborted_by() = None;
    }

    fn aborted_by(&self) -> std::sync::MutexGuard<'_, Option<AbortCause>> {
        // A panic while the lock was held cannot leave the value torn: it is
        // replaced whole.
        self.shared
            .aborted_by
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Remembers a failure the server reported. The first one aborts the
    /// transaction; what fails after it fails for that reason (`25P02`) and
    /// is kept only until the first one is seen, which two statements
    /// pipelined in one scope may report in either order.
    fn remember(&self, error: &PgError) {
        let Some(server) = error.as_db_error() else {
            return;
        };
        let consequence = *server.code() == SqlState::IN_FAILED_SQL_TRANSACTION;
        let mut aborted_by = self.aborted_by();
        let replace = match aborted_by.as_ref() {
            None => true,
            Some(kept) => {
                kept.sqlstate == SqlState::IN_FAILED_SQL_TRANSACTION.code() && !consequence
            }
        };
        if replace {
            *aborted_by = Some(AbortCause {
                sqlstate: server.code().code().to_owned(),
                message: server.message().to_owned(),
                constraint: server.constraint().map(str::to_owned),
            });
        }
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

    /// Executes `sql`, reported to the observer as `reported`: the close of a
    /// transaction, whose probe is a detail of the wire (ADR-0028).
    pub(super) async fn batch_execute_reported(
        &self,
        reported: &str,
        sql: &str,
    ) -> Result<(), PgError> {
        self.observed(reported, self.client().batch_execute(sql))
            .await
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
        if let Err(error) = &outcome {
            self.remember(error);
        }
        outcome
    }

    pub(super) fn next_savepoint(&self) -> String {
        format!(
            "sp{}",
            self.shared.savepoints.fetch_add(1, Ordering::SeqCst) + 1
        )
    }
}
