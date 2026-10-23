//! The session bound to one pooled connection, and the pool that hands them out.

use std::sync::Arc;

use deadpool_postgres::Pool;
use tokio_postgres::error::SqlState;

use crate::error::SessionError;
use crate::identity_map::IdentityMap;
use crate::isolation::IsolationLevel;
use crate::observer::{Outcome, ScopeEnded, ScopeKind, ScopeStarted, SessionObserver};
use crate::session::{Abandon, AsyncScope, ScopeFlag, Session, SessionPool};

use super::connection::PgConnection;
use super::observer::PgObserver;
use super::transient::transient;

/// A repository asks for the capability instead of naming a concrete session
/// type, so it works with any session that offers it. The domain never
/// mentions this trait, which is what keeps the connection out of its reach.
///
/// ```ignore
/// impl<S: Session + PgAccess> OrderRepository<S> for PgOrderRepository { /* … */ }
/// ```
pub trait PgAccess {
    /// The connection of the current scope.
    fn connection(&self) -> &PgConnection;
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

    /// Whether a scope on this session's connection was dropped before it
    /// closed; see [`SessionError::Abandoned`].
    pub fn is_abandoned(&self) -> bool {
        self.connection.is_poisoned()
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
        F: AsyncScope<Self, Result<T, E>, Fut: Send> + Send,
        T: Send,
        E: From<SessionError> + Send,
    {
        // Claimed for the whole scope and released on the way out, whether the
        // scope returns, fails early or unwinds.
        let _guard = self.scope_open.acquire()?;
        if self.connection.is_poisoned() {
            return Err(SessionError::Abandoned.into());
        }

        let savepoint = (self.depth > 0).then(|| self.connection.next_savepoint());
        let kind = if savepoint.is_some() {
            ScopeKind::Savepoint
        } else {
            ScopeKind::Transaction
        };
        let depth = self.depth + 1;
        let observer = Arc::clone(self.connection.observer());

        let open = match &savepoint {
            None => "BEGIN".to_owned(),
            Some(name) => format!("SAVEPOINT {name}"),
        };
        // Armed until the closing statement has run: a future dropped anywhere
        // in between leaves the transaction open, and the guard poisons the
        // connection.
        let mut abandon = Abandon::armed(self.connection.poisoned(), &*observer, depth, kind);
        if let Err(error) = self.connection.batch_execute(&open).await {
            abandon.disarm();
            return Err(SessionError::Begin {
                transient: transient(&error),
                error: Box::new(error),
            }
            .into());
        }
        if savepoint.is_none() {
            // A failure before the transaction began aborted nothing in it.
            self.connection.forget_abort_cause();
        }
        observer.on_scope_started(&ScopeStarted { depth, kind });
        abandon.started();

        let child = self.child();
        let identity_map = Arc::clone(&child.identity_map);
        let outcome = scope(child).await;
        // A nested scope dropped mid-way poisoned the connection: nothing done
        // since may be committed.
        let outcome = match outcome {
            Ok(_) if self.connection.is_poisoned() => Err(SessionError::Abandoned.into()),
            outcome => outcome,
        };
        let closed = if outcome.is_ok() {
            self.commit(savepoint.as_deref()).await
        } else {
            // A failing rollback does not replace the error that caused it;
            // the observer has already seen it as a failed statement.
            self.roll_back(savepoint.as_deref()).await;
            Ok(())
        };
        abandon.disarm();

        let succeeded = outcome.is_ok() && closed.is_ok();
        observer.on_scope_ended(&ScopeEnded {
            depth,
            kind,
            outcome: if succeeded {
                Outcome::Succeeded
            } else {
                Outcome::Failed
            },
        });

        // The identity map lives exactly as long as the outermost transaction.
        if self.depth == 0 {
            identity_map.clear();
        }

        match (outcome, closed) {
            // A scope that did not commit is a failure of the whole scope: the
            // caller believes the work is durable, and it is not.
            (Ok(_), Err(error)) => Err(error.into()),
            (outcome, _) => outcome,
        }
    }
}

impl PgSession {
    /// Closes a scope that succeeded: commits the transaction, or releases
    /// the savepoint of a nested scope (ADR-0028).
    ///
    /// The server alone knows whether the transaction is still whole: a
    /// statement that failed inside it aborted it, and the scope may have
    /// absorbed the failure. To a `COMMIT` in that state the server answers
    /// with a rollback and no error, and the driver keeps the answer's tag to
    /// itself; so the transaction is closed with `SELECT 1; COMMIT` in one
    /// message, whose probe the server refuses with `25P02` before it reaches
    /// the `COMMIT`. A `RELEASE SAVEPOINT` in that state is refused the same
    /// way. A refused close leaves nothing open behind it: the scope is rolled
    /// back, and the failure that aborted it, as the connection remembers it,
    /// is named in [`SessionError::Aborted`].
    async fn commit(&self, savepoint: Option<&str>) -> Result<(), SessionError> {
        let closed = match savepoint {
            None => {
                self.connection
                    .batch_execute_reported("COMMIT", "SELECT 1; COMMIT")
                    .await
            }
            Some(name) => {
                self.connection
                    .batch_execute(&format!("RELEASE SAVEPOINT {name}"))
                    .await
            }
        };
        let Err(error) = closed else {
            self.connection.forget_abort_cause();
            return Ok(());
        };
        let aborted = error.code() == Some(&SqlState::IN_FAILED_SQL_TRANSACTION);
        // Taken before the rollback, which forgets it.
        let cause = aborted
            .then(|| self.connection.take_abort_cause())
            .flatten();
        // Whatever refused the close, nothing of the scope is durable: a
        // refused probe left the transaction open and a refused release the
        // savepoint; after a refused COMMIT the server has ended the block,
        // and the ROLLBACK is a warning.
        self.roll_back(savepoint).await;
        Err(if aborted {
            SessionError::Aborted { cause }
        } else {
            SessionError::Commit {
                transient: transient(&error),
                error: Box::new(error),
            }
        })
    }

    /// Rolls a scope back: the transaction, or a nested scope to its
    /// savepoint. A rollback that fails leaves the connection in a state
    /// nobody knows — inside a transaction the pool would hand to its next
    /// user, whose `BEGIN` the server only warns about — so the connection is
    /// poisoned and discarded.
    async fn roll_back(&self, savepoint: Option<&str>) {
        let statement = match savepoint {
            None => "ROLLBACK".to_owned(),
            Some(name) => format!("ROLLBACK TO SAVEPOINT {name}"),
        };
        if self.connection.batch_execute(&statement).await.is_err() {
            self.connection.poison();
        }
        self.connection.forget_abort_cause();
    }
}

/// Hands out sessions backed by a `deadpool` connection pool.
pub struct PgSessionPool {
    pool: Pool,
    observer: Arc<dyn PgObserver>,
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
    pub fn observed_by(self, observer: impl PgObserver + 'static) -> Self {
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

    async fn acquire(&self) -> Result<PgSession, SessionError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|error| SessionError::Acquire(Box::new(error)))?;
        Ok(PgSession {
            connection: PgConnection::new(client, Arc::clone(&self.observer)),
            // Outside a transaction nothing may be cached.
            identity_map: Arc::new(IdentityMap::with_isolation(IsolationLevel::ReadUncommitted)),
            isolation: self.isolation,
            depth: 0,
            scope_open: ScopeFlag::new(),
        })
    }

    fn scope_opened(&self) {
        self.observer.on_scope_started(&ScopeStarted {
            depth: 0,
            kind: ScopeKind::Session,
        });
    }

    fn scope_closed(&self, succeeded: bool) {
        self.observer.on_scope_ended(&ScopeEnded {
            depth: 0,
            kind: ScopeKind::Session,
            outcome: if succeeded {
                Outcome::Succeeded
            } else {
                Outcome::Failed
            },
        });
    }
}
