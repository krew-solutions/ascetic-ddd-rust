//! Unit of Work for a DDD application: session scopes, nested transactions
//! and an identity map.
//!
//! # Layering
//!
//! The domain sees exactly one operation — [`Session::atomic`]. It cannot
//! reach the connection, because it never learns the concrete session type:
//! repositories are parameterised by it, and infrastructure states the
//! capability it needs as a bound.
//!
//! ```ignore
//! // порт: сессия параметром, домен видит только `Session`
//! pub trait OrderRepository<S: Session>: Sync {
//!     fn save<'a>(&'a self, session: &'a S, order: &'a Order)
//!         -> BoxFuture<'a, Result<(), Error>>;
//! }
//!
//! // инфраструктура: способность вместо подтипа
//! pub trait PgAccess { fn connection(&self) -> &PgConnection; }
//!
//! impl<S: Session + PgAccess> OrderRepository<S> for PgOrderRepository { … }
//! ```
//!
//! Python needs `ISession` plus `IPgSession` plus a cast in
//! `extract_connection()`; here the cast is a trait bound the compiler checks.
//!
//! # Status
//!
//! Ported so far: the session traits, the identity map, the observer, the REST
//! and composite sessions, an in-memory session for testing and the PostgreSQL
//! adapter (feature `pg`).

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod composite;
pub mod error;
pub mod identity_key;
pub mod identity_map;
pub mod isolation;
pub mod observer;
#[cfg(feature = "pg")]
pub mod pg;
pub mod rest;
pub mod session;
pub mod testing;

pub use crate::composite::{CompositeSession, CompositeSessionPool};
pub use crate::error::{BoxError, SessionError};
pub use crate::identity_key::IdentityKey;
pub use crate::identity_map::{DEFAULT_CACHE_SIZE, IdentityMap, Lookup};
pub use crate::isolation::IsolationLevel;
pub use crate::observer::SessionObserver;
#[cfg(feature = "pg")]
pub use crate::pg::{PgAccess, PgConnection, PgSession, PgSessionPool};
pub use crate::rest::{HttpAccess, RestSession, RestSessionPool};
pub use crate::session::{Session, SessionPool};
