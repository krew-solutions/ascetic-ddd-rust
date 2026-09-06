//! An in-memory session, for testing a domain without a database.
//!
//! The layering only pays off if the domain can actually be exercised without
//! infrastructure, so the tool for that ships with the crate. The session
//! records the scopes it opens, which is what a test asserts on:
//!
//! ```
//! use ascetic_ddd_session::testing::MemorySessionPool;
//! use ascetic_ddd_session::{Session, SessionError, SessionPool};
//!
//! let pool = MemorySessionPool::new();
//! let journal = pool.journal();
//!
//! futures::executor::block_on(pool.session(async |session| {
//!     session.atomic(async |session| {
//!         session.atomic(async |_session| Ok(())).await?;
//!         Ok::<_, SessionError>(())
//!     }).await
//! })).unwrap();
//!
//! assert_eq!(
//!     journal.entries(),
//!     ["BEGIN", "SAVEPOINT sp1", "RELEASE SAVEPOINT sp1", "COMMIT"],
//! );
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::SessionError;
use crate::identity_map::IdentityMap;
use crate::isolation::IsolationLevel;
use crate::observer::{Outcome, ScopeEnded, ScopeKind, ScopeStarted, SessionObserver};
use crate::session::{ScopeFlag, Session, SessionPool};

/// Everything the in-memory session has recorded.
#[derive(Debug, Default)]
pub struct Journal {
    entries: Mutex<Vec<String>>,
    savepoints: AtomicU64,
}

impl Journal {
    /// The statements recorded so far, in order.
    pub fn entries(&self) -> Vec<String> {
        self.lock().clone()
    }

    /// Forgets everything recorded so far.
    pub fn clear(&self) {
        self.lock().clear();
    }

    fn record(&self, statement: impl Into<String>) {
        self.lock().push(statement.into());
    }

    fn next_savepoint(&self) -> u64 {
        self.savepoints.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<String>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Hands out [`MemorySession`]s that share one journal.
pub struct MemorySessionPool {
    journal: Arc<Journal>,
    observer: Arc<dyn SessionObserver>,
    isolation: IsolationLevel,
}

impl MemorySessionPool {
    /// Creates a pool with no observer and the default isolation level.
    pub fn new() -> Self {
        MemorySessionPool {
            journal: Arc::new(Journal::default()),
            observer: Arc::new(()),
            isolation: IsolationLevel::default(),
        }
    }

    /// Returns a pool that notifies `observer`.
    ///
    /// Wiring is a value, not a registration: this consumes the pool and
    /// returns a new one.
    pub fn observed_by(self, observer: impl SessionObserver + 'static) -> Self {
        MemorySessionPool {
            observer: Arc::new(observer),
            ..self
        }
    }

    /// Returns a pool whose transactions use the given isolation level.
    pub fn with_isolation(self, isolation: IsolationLevel) -> Self {
        MemorySessionPool { isolation, ..self }
    }

    /// The journal shared by every session this pool hands out.
    pub fn journal(&self) -> Arc<Journal> {
        Arc::clone(&self.journal)
    }
}

impl Default for MemorySessionPool {
    fn default() -> Self {
        MemorySessionPool::new()
    }
}

impl SessionPool for MemorySessionPool {
    type Session = MemorySession;

    async fn session<T, E, F>(&self, scope: F) -> Result<T, E>
    where
        F: AsyncFnOnce(&Self::Session) -> Result<T, E>,
        E: From<SessionError>,
    {
        let session = MemorySession {
            journal: Arc::clone(&self.journal),
            observer: Arc::clone(&self.observer),
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
    }
}

/// A session that records scopes and statements instead of executing them.
#[derive(Clone)]
pub struct MemorySession {
    journal: Arc<Journal>,
    observer: Arc<dyn SessionObserver>,
    identity_map: Arc<IdentityMap>,
    isolation: IsolationLevel,
    depth: usize,
    /// Set while a scope opened on this session, or on a clone of it, is running.
    scope_open: ScopeFlag,
}

impl MemorySession {
    /// The journal this session records into.
    pub fn journal(&self) -> &Journal {
        &self.journal
    }

    /// The identity map of the current scope.
    pub fn identity_map(&self) -> &IdentityMap {
        &self.identity_map
    }

    /// A handle to the identity map of the current scope, so that a test can
    /// still inspect it after the scope has ended.
    pub fn identity_map_handle(&self) -> Arc<IdentityMap> {
        Arc::clone(&self.identity_map)
    }

    /// Number of transaction scopes currently open around this session.
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// Records a statement, as a repository would.
    ///
    /// The journal is what a test asserts on, so nothing is reported to the
    /// observer here: statements reach an observer through a real adapter, and
    /// that path is covered where it exists — see `tests/pg.rs`.
    pub fn record(&self, statement: &str) {
        self.journal.record(statement);
    }

    fn child(&self) -> Self {
        MemorySession {
            journal: Arc::clone(&self.journal),
            observer: Arc::clone(&self.observer),
            // The outermost transaction gets a fresh map; nested scopes share it.
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

impl Session for MemorySession {
    async fn atomic<T, E, F>(&self, scope: F) -> Result<T, E>
    where
        F: AsyncFnOnce(&Self) -> Result<T, E>,
        E: From<SessionError>,
    {
        let _guard = self.scope_open.acquire()?;

        let savepoint = (self.depth > 0).then(|| self.journal.next_savepoint());
        let kind = if savepoint.is_some() {
            ScopeKind::Savepoint
        } else {
            ScopeKind::Transaction
        };
        let depth = self.depth + 1;

        self.journal.record(match savepoint {
            None => "BEGIN".to_owned(),
            Some(number) => format!("SAVEPOINT sp{number}"),
        });
        self.observer
            .on_scope_started(&ScopeStarted { depth, kind });

        let child = self.child();
        let outcome = scope(&child).await;
        let committed = outcome.is_ok();

        self.journal.record(match (savepoint, committed) {
            (None, true) => "COMMIT".to_owned(),
            (None, false) => "ROLLBACK".to_owned(),
            (Some(number), true) => format!("RELEASE SAVEPOINT sp{number}"),
            (Some(number), false) => format!("ROLLBACK TO SAVEPOINT sp{number}"),
        });
        self.observer.on_scope_ended(&ScopeEnded {
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

        outcome
    }
}
