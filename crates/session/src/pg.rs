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
pub mod connection;
pub mod observer;
pub mod session;

// Re-exported so that a user of this crate does not have to match versions
// with the driver and the pool independently.
pub use deadpool_postgres;
pub use tokio_postgres;

pub use self::connection::{PgConnection, PgError};
pub use self::observer::{PgObserver, QueryEnded, QueryStarted};
pub use self::session::{PgAccess, PgSession, PgSessionPool};
