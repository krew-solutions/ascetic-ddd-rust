//! REST session: a scope over an HTTP client.
//!
//! There is no transaction behind it. A scope here bounds an identity map and
//! reports itself to the observer — the same shape as a database session, so a
//! use case can be written once against [`Session`] and run against either.
//! What it deliberately does *not* do is pretend that HTTP calls can be rolled
//! back; work that must be undone across services belongs in a saga.
//!
//! # No HTTP dependency
//!
//! Python ties its REST session to `aiohttp` and Go to `net/http`. Here the
//! client is a type parameter, so the crate depends on no HTTP library and any
//! of them fits:
//!
//! ```
//! use ascetic_ddd_session::rest::{HttpAccess, RestSessionPool};
//! use ascetic_ddd_session::{Session, SessionError, SessionPool};
//!
//! struct FakeClient;
//!
//! let sessions = RestSessionPool::new(FakeClient);
//!
//! futures::executor::block_on(sessions.session(async |session| {
//!     session.atomic(async |session| {
//!         let _client: &FakeClient = session.http();
//!         Ok::<_, SessionError>(())
//!     })
//!     .await
//! })).unwrap();
//! ```
//!
//! Requests are timed by wrapping the call, which replaces the transport hooks
//! of the other ports (`aiohttp.TraceConfig`, a custom `http.RoundTripper`):
//!
//! ```ignore
//! let response = session
//!     .request("GET", &url, client.get(&url).send())
//!     .await?;
//! ```

use std::sync::Arc;
use std::time::Instant;

use crate::error::SessionError;
use crate::identity_map::IdentityMap;
use crate::isolation::IsolationLevel;
use crate::observer::{
    Outcome, RequestEnded, RequestStarted, ScopeEnded, ScopeKind, ScopeStarted, SessionObserver,
};
use crate::session::{ScopeFlag, Session, SessionPool};

/// The infrastructure capability: "this session speaks HTTP".
///
/// A repository asks for the capability rather than naming a concrete session
/// type; the domain never mentions this trait.
pub trait HttpAccess {
    /// The HTTP client this session carries.
    type Client;

    /// The client of the current scope.
    fn http(&self) -> &Self::Client;

    /// Times an outbound call and reports it to the observer.
    ///
    /// The call is made by the caller, so any client works and nothing is
    /// hidden: this only wraps it. It replaces the transport hooks of the
    /// other ports (`aiohttp.TraceConfig`, a custom `http.RoundTripper`),
    /// which could not be expressed without fixing the HTTP library.
    fn request<T: Send, E: Send>(
        &self,
        method: &str,
        url: &str,
        call: impl Future<Output = Result<T, E>> + Send,
    ) -> impl Future<Output = Result<T, E>> + Send
    where
        Self: Sync;
}

/// A session over an HTTP client.
pub struct RestSession<C> {
    client: Arc<C>,
    observer: Arc<dyn SessionObserver>,
    identity_map: Arc<IdentityMap>,
    isolation: IsolationLevel,
    depth: usize,
    /// Set while a scope opened on this session, or on a clone of it, is running.
    scope_open: ScopeFlag,
}

// Not derived: that would demand `C: Clone`, and the client is behind an `Arc`.
impl<C> Clone for RestSession<C> {
    fn clone(&self) -> Self {
        RestSession {
            client: Arc::clone(&self.client),
            observer: Arc::clone(&self.observer),
            identity_map: Arc::clone(&self.identity_map),
            isolation: self.isolation,
            depth: self.depth,
            scope_open: self.scope_open.clone(),
        }
    }
}

impl<C> RestSession<C> {
    /// The identity map of the current scope.
    pub fn identity_map(&self) -> &IdentityMap {
        &self.identity_map
    }

    /// Number of scopes currently open around this session.
    pub fn depth(&self) -> usize {
        self.depth
    }

    fn child(&self) -> Self {
        RestSession {
            client: Arc::clone(&self.client),
            observer: Arc::clone(&self.observer),
            // The outermost scope gets a fresh map; nested scopes share it.
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

impl<C: Send + Sync> HttpAccess for RestSession<C> {
    type Client = C;

    fn http(&self) -> &C {
        &self.client
    }

    async fn request<T: Send, E: Send>(
        &self,
        method: &str,
        url: &str,
        call: impl Future<Output = Result<T, E>> + Send,
    ) -> Result<T, E> {
        self.observer
            .on_request_started(&RequestStarted { method, url });

        let started = Instant::now();
        let outcome = call.await;

        self.observer.on_request_ended(&RequestEnded {
            method,
            url,
            elapsed: started.elapsed(),
            failed: outcome.is_err(),
        });
        outcome
    }
}

impl<C: Send + Sync> Session for RestSession<C> {
    async fn atomic<T, E, F>(&self, scope: F) -> Result<T, E>
    where
        F: AsyncFnOnce(&Self) -> Result<T, E>,
        E: From<SessionError>,
    {
        let _guard = self.scope_open.acquire()?;

        let depth = self.depth + 1;
        self.observer.on_scope_started(&ScopeStarted {
            depth,
            kind: ScopeKind::Logical,
        });

        let child = self.child();
        let outcome = scope(&child).await;

        self.observer.on_scope_ended(&ScopeEnded {
            depth,
            kind: ScopeKind::Logical,
            outcome: if outcome.is_ok() {
                Outcome::Committed
            } else {
                Outcome::RolledBack
            },
        });

        // The identity map lives exactly as long as the outermost scope.
        if self.depth == 0 {
            child.identity_map.clear();
        }

        outcome
    }
}

/// Hands out sessions over one shared HTTP client.
///
/// Python creates a new `aiohttp.ClientSession` per scope, because that is how
/// `aiohttp` is built; a `reqwest::Client` or a `hyper` client is itself the
/// connection pool and is meant to be shared, so one client serves every
/// session here.
pub struct RestSessionPool<C> {
    client: Arc<C>,
    observer: Arc<dyn SessionObserver>,
    isolation: IsolationLevel,
}

impl<C> RestSessionPool<C> {
    /// Creates a pool over the given client.
    pub fn new(client: C) -> Self {
        RestSessionPool::from_shared(Arc::new(client))
    }

    /// Creates a pool over a client that is already shared.
    pub fn from_shared(client: Arc<C>) -> Self {
        RestSessionPool {
            client,
            observer: Arc::new(()),
            isolation: IsolationLevel::default(),
        }
    }

    /// Returns a pool that notifies `observer`.
    pub fn observed_by(self, observer: impl SessionObserver + 'static) -> Self {
        RestSessionPool {
            observer: Arc::new(observer),
            ..self
        }
    }

    /// Returns a pool whose scopes use the given isolation level for their
    /// identity map.
    pub fn with_isolation(self, isolation: IsolationLevel) -> Self {
        RestSessionPool { isolation, ..self }
    }

    /// The shared HTTP client.
    pub fn client(&self) -> &Arc<C> {
        &self.client
    }
}

impl<C: Send + Sync> SessionPool for RestSessionPool<C> {
    type Session = RestSession<C>;

    async fn session<T, E, F>(&self, scope: F) -> Result<T, E>
    where
        F: AsyncFnOnce(&Self::Session) -> Result<T, E>,
        E: From<SessionError>,
    {
        let session = RestSession {
            client: Arc::clone(&self.client),
            observer: Arc::clone(&self.observer),
            // Outside a scope nothing may be cached.
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
    }
}
