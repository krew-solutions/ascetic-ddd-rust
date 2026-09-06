//! The session bound to one pooled connection, and the pool that hands them out.

use std::sync::Arc;

use deadpool_postgres::Pool;

use crate::error::SessionError;
use crate::identity_map::IdentityMap;
use crate::isolation::IsolationLevel;
use crate::observer::{Outcome, ScopeEnded, ScopeKind, ScopeStarted, SessionObserver};
use crate::session::{ScopeFlag, Session, SessionPool};

use super::connection::PgConnection;
use super::observer::PgObserver;

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
        let observer = Arc::clone(self.connection.observer());

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
                Outcome::Succeeded
            } else {
                Outcome::Failed
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
            connection: PgConnection::new(client, Arc::clone(&self.observer)),
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
                Outcome::Succeeded
            } else {
                Outcome::Failed
            },
        });

        outcome
        // The connection returns to the pool as `session` is dropped here.
    }
}
