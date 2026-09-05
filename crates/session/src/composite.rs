//! A session made of two others.
//!
//! A use case that must write to a database *and* call a service opens both
//! scopes at once. The composite is itself a [`Session`], so the use case is
//! written against one session as usual, and its scopes nest:
//!
//! ```text
//! first open, second open, … work …, second close, first close
//! ```
//!
//! More than two delegates nest: `CompositeSession<A, CompositeSession<B, C>>`.
//!
//! # What it is not
//!
//! It is not a distributed transaction. If the first delegate commits and the
//! second fails, the two diverge — no amount of composition can prevent that.
//! Work that must be undone across systems belongs in a saga; this type only
//! spares a use case from threading two sessions by hand.
//!
//! # Capabilities
//!
//! Repositories ask for capabilities (`S: Session + PgAccess`), so a composite
//! must offer the capabilities of its delegates. This crate deliberately does
//! **not** provide those impls: an impl taking a capability from the left
//! delegate and one taking it from the right would overlap wherever both offer
//! it, so a blanket impl would have to fix a direction — and would then take
//! the first delegate that fits, silently. That is exactly what Python's
//! `__getattr__` does, and getting the wrong database out of a composite of two
//! is not a failure worth inheriting.
//!
//! An application names the delegate itself, in a newtype it owns:
//!
//! ```ignore
//! pub struct AppSession(CompositeSession<PgSession, RestSession<Client>>);
//!
//! impl Session for AppSession {
//!     async fn atomic<T, E, F>(&self, scope: F) -> Result<T, E>
//!     where
//!         F: AsyncFnOnce(Self) -> Result<T, E>,
//!         E: From<SessionError>,
//!     {
//!         self.0.atomic(async |inner| scope(AppSession(inner)).await).await
//!     }
//! }
//!
//! impl PgAccess for AppSession {
//!     fn connection(&self) -> &PgConnection {
//!         self.0.first().connection()
//!     }
//! }
//!
//! impl HttpAccess for AppSession {
//!     type Client = Client;
//!
//!     fn http(&self) -> &Client {
//!         self.0.second().http()
//!     }
//!     // …request() delegates the same way
//! }
//! ```
//!
//! The newtype is also what the orphan rule requires: neither the capability
//! nor [`CompositeSession`] belongs to the application, so it cannot write the
//! impl for the pair directly. Naming the delegate is a line of code; picking
//! it by search is a bug waiting for the second database.

use crate::error::SessionError;
use crate::session::{Session, SessionPool};

/// Two sessions acting as one.
pub struct CompositeSession<A, B> {
    first: A,
    second: B,
}

impl<A, B> CompositeSession<A, B> {
    /// Combines two sessions.
    pub fn new(first: A, second: B) -> Self {
        CompositeSession { first, second }
    }

    /// The first delegate — the outer scope.
    pub fn first(&self) -> &A {
        &self.first
    }

    /// The second delegate — the inner scope.
    pub fn second(&self) -> &B {
        &self.second
    }

    /// Takes the delegates apart.
    pub fn into_parts(self) -> (A, B) {
        (self.first, self.second)
    }
}

impl<A: Session, B: Session> Session for CompositeSession<A, B> {
    /// Opens a scope on both delegates, innermost closing first.
    ///
    /// Each delegate refuses a second scope of its own, so the composite needs
    /// no guard: opening two composite scopes at once is stopped by whichever
    /// delegate is asked first.
    async fn atomic<T, E, F>(&self, scope: F) -> Result<T, E>
    where
        F: AsyncFnOnce(Self) -> Result<T, E>,
        E: From<SessionError>,
    {
        self.first
            .atomic(async |first| {
                self.second
                    .atomic(async |second| scope(CompositeSession::new(first, second)).await)
                    .await
            })
            .await
    }
}

/// Two pools acting as one.
pub struct CompositeSessionPool<A, B> {
    first: A,
    second: B,
}

impl<A, B> CompositeSessionPool<A, B> {
    /// Combines two pools.
    pub fn new(first: A, second: B) -> Self {
        CompositeSessionPool { first, second }
    }

    /// The first delegate.
    pub fn first(&self) -> &A {
        &self.first
    }

    /// The second delegate.
    pub fn second(&self) -> &B {
        &self.second
    }
}

impl<A: SessionPool, B: SessionPool> SessionPool for CompositeSessionPool<A, B> {
    type Session = CompositeSession<A::Session, B::Session>;

    async fn session<T, E, F>(&self, scope: F) -> Result<T, E>
    where
        F: AsyncFnOnce(Self::Session) -> Result<T, E>,
        E: From<SessionError>,
    {
        self.first
            .session(async |first| {
                self.second
                    .session(async |second| scope(CompositeSession::new(first, second)).await)
                    .await
            })
            .await
    }
}
