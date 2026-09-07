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
//! times, so the recursion is boxed. The box holds the concrete future, not a
//! `dyn Future`, so whether the whole is `Send` is still read off its contents
//! — as for every other session in this crate: a `Send` scope gives a `Send`
//! future, and a scope that is not `Send` is still accepted.
//!
//! The box also ends the compiler's walk over nested closures at each level, so
//! the layout of a `Many` scope does not grow with the number of delegates.

use crate::error::SessionError;
use crate::session::{Session, SessionPool};

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
///
/// The recursive call is boxed, as a recursive `async fn` must be; the box
/// holds the concrete future, so `Send` is inferred rather than promised.
async fn open_scopes<S, T, E, F>(rest: &[S], opened: Vec<S>, scope: F) -> Result<T, E>
where
    S: Session,
    E: From<SessionError>,
    F: AsyncFnOnce(&Many<S>) -> Result<T, E>,
{
    match rest.split_first() {
        None => scope(&Many(opened)).await,
        Some((head, tail)) => {
            head.atomic(async |opened_head: &S| {
                Box::pin(open_scopes(tail, with(opened, opened_head.clone()), scope)).await
            })
            .await
        }
    }
}

/// Same, for a set of pools handing out a set of sessions.
async fn open_sessions<P, T, E, F>(rest: &[P], opened: Vec<P::Session>, scope: F) -> Result<T, E>
where
    P: SessionPool,
    E: From<SessionError>,
    F: AsyncFnOnce(&Many<P::Session>) -> Result<T, E>,
{
    match rest.split_first() {
        None => scope(&Many(opened)).await,
        Some((head, tail)) => {
            head.session(async |opened_head: &P::Session| {
                Box::pin(open_sessions(
                    tail,
                    with(opened, opened_head.clone()),
                    scope,
                ))
                .await
            })
            .await
        }
    }
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
