//! Several sessions of the *same* type acting as one.
//!
//! A tuple composes delegates of different types, and its count is fixed when
//! the code is written. Shards of one database are the other case: the
//! delegates share a type and their number comes from configuration.
//!
//! ```
//! use ascetic_ddd_session::testing::MemorySessionPool;
//! use ascetic_ddd_session::{Many, Session, SessionError, SessionPool};
//!
//! let pools: Vec<_> = (0..3).map(|_| MemorySessionPool::new()).collect();
//! let journals: Vec<_> = pools.iter().map(|pool| pool.journal()).collect();
//!
//! futures::executor::block_on(Many::new(pools).session(async |shards| {
//!     shards.atomic(async |shards| {
//!         shards.get(1).unwrap().record("INSERT INTO orders (id) VALUES (1)");
//!         Ok::<_, SessionError>(())
//!     })
//!     .await
//! })).unwrap();
//!
//! assert_eq!(journals[1].entries(), ["BEGIN", "INSERT INTO orders (id) VALUES (1)", "COMMIT"]);
//! assert_eq!(journals[0].entries(), ["BEGIN", "COMMIT"]);
//! ```
//!
//! Delegates open in order and close in reverse, as in a tuple, and each opens
//! a scope of its own — a scope over three shards is three transactions, not
//! one. Nothing here makes them atomic together; that is what a saga is for.
//!
//! # `Send`
//!
//! A count known only at run time means the scopes nest a run-time number of
//! times, so the recursion is boxed. The box holds a `dyn Future + Send`: a
//! recursive future cannot show the compiler its own `Send`-ness, and the
//! promise the trait makes (ADR-0004) names it instead.
//!
//! The box also ends the compiler's walk over nested closures at each level, so
//! the layout of a `Many` scope does not grow with the number of delegates.

use std::future::Future;
use std::pin::Pin;

use crate::error::SessionError;
use crate::session::{AsyncScope, Session, SessionPool};

/// Several sessions — or pools — of the same type, acting as one.
#[derive(Clone, Debug, Default)]
pub struct Many<S>(Vec<S>);

impl<S> Many<S> {
    /// Collects delegates. An empty set is allowed: its scope opens nothing.
    pub fn new(delegates: impl IntoIterator<Item = S>) -> Self {
        Many(delegates.into_iter().collect())
    }

    /// The delegate at `index`, if there is one.
    pub fn get(&self, index: usize) -> Option<&S> {
        self.0.get(index)
    }

    /// Number of delegates.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// True if there are no delegates.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The delegates, in order.
    pub fn as_slice(&self) -> &[S] {
        &self.0
    }
}

impl<S> FromIterator<S> for Many<S> {
    fn from_iter<I: IntoIterator<Item = S>>(delegates: I) -> Self {
        Many::new(delegates)
    }
}

/// The opened sessions so far, plus one. A new vector rather than a push, so
/// that nothing in the recursion is mutated.
fn with<S>(opened: Vec<S>, next: S) -> Vec<S> {
    opened.into_iter().chain(std::iter::once(next)).collect()
}

/// Opens a scope on each delegate in turn, then hands the scope all of them.
///
/// Recursive, so boxed; the box holds a `dyn Future + Send`, because a
/// recursive future cannot show its own `Send`-ness and the promise the trait
/// makes names it instead — see the module documentation.
fn open_scopes<'a, S, T, E, F>(
    rest: &'a [S],
    opened: Vec<S>,
    scope: F,
) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>
where
    S: Session,
    T: Send + 'a,
    E: From<SessionError> + Send + 'a,
    F: AsyncScope<Many<S>, Result<T, E>, Fut: Send> + Send + 'a,
{
    match rest.split_first() {
        None => Box::pin(async move { scope(Many(opened)).await }),
        Some((head, tail)) => Box::pin(head.atomic(async move |opened_head: S| {
            open_scopes(tail, with(opened, opened_head), scope).await
        })),
    }
}

impl<S: Session> Session for Many<S> {
    async fn atomic<T, E, F>(&self, scope: F) -> Result<T, E>
    where
        F: AsyncScope<Self, Result<T, E>, Fut: Send> + Send,
        T: Send,
        E: From<SessionError> + Send,
    {
        open_scopes(&self.0, Vec::new(), scope).await
    }
}

impl<P: SessionPool> SessionPool for Many<P> {
    type Session = Many<P::Session>;

    /// Delegates are acquired in order; scopes open in that order and close
    /// in reverse.
    async fn acquire(&self) -> Result<Self::Session, SessionError> {
        let mut opened = Vec::with_capacity(self.0.len());
        for pool in &self.0 {
            opened.push(pool.acquire().await?);
        }
        Ok(Many(opened))
    }

    fn scope_opened(&self) {
        for pool in &self.0 {
            pool.scope_opened();
        }
    }

    fn scope_closed(&self, succeeded: bool) {
        for pool in self.0.iter().rev() {
            pool.scope_closed(succeeded);
        }
    }
}
