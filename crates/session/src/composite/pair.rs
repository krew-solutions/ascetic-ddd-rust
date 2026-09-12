//! A session made of exactly two others, as a named type.
//!
//! More than two delegates nest — `CompositeSession<A, CompositeSession<B, C>>`
//! — and are reached as `.second().first()`. The flat alternative is a
//! [`tuple`][super::tuple]; what a composite is and is not, and how an
//! application reaches a delegate's capabilities, is described in the
//! [parent module][super].

use crate::error::SessionError;
use crate::session::{AsyncScope, Session, SessionPool};

/// Two sessions acting as one.
#[derive(Clone)]
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
        F: AsyncScope<Self, Result<T, E>, Fut: Send> + Send,
        T: Send,
        E: From<SessionError> + Send,
    {
        let second = self.second.clone();
        self.first
            .atomic(async move |first: A| {
                second
                    .atomic(async move |second: B| {
                        // Boxed: each level nests one closure per delegate, and the compiler
                        // walks that nesting when it lays out the outer future. The box ends
                        // the walk here, so nesting depth stays within the default
                        // `recursion_limit`.
                        Box::pin(scope(CompositeSession::new(first, second))).await
                    })
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

    /// Delegates are acquired left to right; scopes open in that order and
    /// close in reverse.
    async fn acquire(&self) -> Result<Self::Session, SessionError> {
        Ok(CompositeSession::new(
            self.first.acquire().await?,
            self.second.acquire().await?,
        ))
    }

    fn scope_opened(&self) {
        self.first.scope_opened();
        self.second.scope_opened();
    }

    fn scope_closed(&self, succeeded: bool) {
        self.second.scope_closed(succeeded);
        self.first.scope_closed(succeeded);
    }
}
