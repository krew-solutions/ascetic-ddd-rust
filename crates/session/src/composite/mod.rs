//! Several sessions acting as one.
//!
//! A use case that must write to a database *and* call a service opens both
//! scopes at once. A composite is itself a [`Session`][crate::session::Session],
//! so the use case is
//! written against one session as usual, and the scopes nest:
//!
//! ```text
//! first open, second open, … work …, second close, first close
//! ```
//!
//! Three shapes are kept side by side so that they can be compared in use:
//!
//! - [`pair`] — a named type of exactly two delegates, `CompositeSession<A, B>`;
//!   more than two nest, `CompositeSession<A, CompositeSession<B, C>>`, and are
//!   reached as `.second().first()`.
//! - [`tuple`][mod@tuple] — a tuple of two to eight sessions is itself a session, and the
//!   delegates stay flat: `sessions.0`, `sessions.1`, `sessions.2`.
//! - [`many`] — delegates of one type whose number is known only at run time.
//!
//! # What it is not
//!
//! It is not a distributed transaction. If the first delegate commits and the
//! second fails, the two diverge — no amount of composition can prevent that.
//! Work that must be undone across systems belongs in a saga; a composite only
//! spares a use case from threading several sessions by hand.
//!
//! # Capabilities
//!
//! Repositories ask for capabilities (`S: Session + PgAccess`), so a composite
//! must offer the capabilities of its delegates. This crate deliberately does
//! **not** provide those impls: an impl taking a capability from one delegate
//! and one taking it from another would overlap wherever both offer it, so a
//! blanket impl would have to fix a position — and would then take the first
//! delegate that fits, silently. That is exactly what Python's `__getattr__`
//! does, and getting the wrong database out of a composite of two is not a
//! failure worth inheriting.
//!
//! An application names the delegate itself, in a newtype it owns. Over a
//! tuple:
//!
//! ```ignore
//! pub struct AppSession((PgSession, RestSession<Client>));
//!
//! impl Session for AppSession {
//!     async fn atomic<T, E, F>(&self, scope: F) -> Result<T, E>
//!     where
//!         F: AsyncFnOnce(&Self) -> Result<T, E>,
//!         E: From<SessionError>,
//!     {
//!         self.0.atomic(async |inner| scope(&AppSession(inner.clone())).await).await
//!     }
//! }
//!
//! impl PgAccess for AppSession {
//!     fn connection(&self) -> &PgConnection {
//!         self.0.0.connection()
//!     }
//! }
//!
//! impl HttpAccess for AppSession {
//!     type Client = Client;
//!
//!     fn http(&self) -> &Client {
//!         self.0.1.http()
//!     }
//!     // …request() delegates the same way
//! }
//! ```
//!
//! Over a pair the newtype is `AppSession(CompositeSession<PgSession, RestSession<Client>>)`
//! and the delegates are reached as `self.0.first()` and `self.0.second()`;
//! everything else reads the same.
//!
//! The newtype is also what the orphan rule requires: neither the capability
//! nor the composite belongs to the application, so it cannot write the impl
//! directly. Naming the delegate is a line of code; picking it by search is a
//! bug waiting for the second database.

pub mod many;
pub mod pair;
pub mod tuple;
