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
//! # The future is not `Send`
//!
//! A count known only at run time means the scopes nest a run-time number of
//! times, which means the future has to be boxed — and a boxed future is `Send`
//! only if what it holds is. What it holds includes the caller's scope closure
//! and its result, and [`Session::atomic`] requires neither to be `Send`. So
//! the box cannot promise it either:
//!
//! ```text
//! error[E0277]: `dyn Future<Output = …>` cannot be sent between threads safely
//! ```
//!
//! In practice: a `Many` scope runs on a current-thread runtime, or anywhere
//! the future is awaited rather than handed to `tokio::spawn` on a multi-thread
//! runtime. Every other session in this crate returns a `Send` future.
//!
//! Lifting this means requiring `Send` of the scope and its result in
//! [`Session`] itself, so that the box can promise it. That is a change to the
//! trait every session implements, and it has not been made.

use std::pin::Pin;

use crate::error::SessionError;
use crate::session::{Session, SessionPool};

/// A future that has to be boxed because the recursion depth is dynamic.
type Nested<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + 'a>>;

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

/// Opens a scope on the first delegate and continues with the rest inside it.
fn open_scopes<'a, S, T, E, F>(rest: &'a [S], opened: Vec<S>, scope: F) -> Nested<'a, T, E>
where
    S: Session + 'a,
    T: 'a,
    E: From<SessionError> + 'a,
    F: AsyncFnOnce(&Many<S>) -> Result<T, E> + 'a,
{
    Box::pin(async move {
        match rest.split_first() {
            None => scope(&Many(opened)).await,
            Some((head, tail)) => {
                head.atomic(async |opened_head: &S| {
                    open_scopes(tail, with(opened, opened_head.clone()), scope).await
                })
                .await
            }
        }
    })
}

/// Same, for a set of pools handing out a set of sessions.
fn open_sessions<'a, P, T, E, F>(
    rest: &'a [P],
    opened: Vec<P::Session>,
    scope: F,
) -> Nested<'a, T, E>
where
    P: SessionPool + 'a,
    T: 'a,
    E: From<SessionError> + 'a,
    F: AsyncFnOnce(&Many<P::Session>) -> Result<T, E> + 'a,
{
    Box::pin(async move {
        match rest.split_first() {
            None => scope(&Many(opened)).await,
            Some((head, tail)) => {
                head.session(async |opened_head: &P::Session| {
                    open_sessions(tail, with(opened, opened_head.clone()), scope).await
                })
                .await
            }
        }
    })
}

impl<S: Session> Session for Many<S> {
    /// Opens a scope on every delegate, in order, closing them in reverse.
    ///
    /// Each delegate refuses a second scope of its own, so this needs no guard.
    async fn atomic<T, E, F>(&self, scope: F) -> Result<T, E>
    where
        F: AsyncFnOnce(&Self) -> Result<T, E>,
        E: From<SessionError>,
    {
        open_scopes(&self.0, Vec::new(), scope).await
    }
}

impl<P: SessionPool> SessionPool for Many<P> {
    type Session = Many<P::Session>;

    async fn session<T, E, F>(&self, scope: F) -> Result<T, E>
    where
        F: AsyncFnOnce(&Self::Session) -> Result<T, E>,
        E: From<SessionError>,
    {
        open_sessions(&self.0, Vec::new(), scope).await
    }
}
