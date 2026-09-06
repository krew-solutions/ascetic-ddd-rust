//! A tuple of sessions is itself a session.
//!
//! Delegates open left to right and close right to left; what a composite is
//! and is not, and how an application reaches a delegate's capabilities, is
//! described in the [parent module][super].
//!
//! ```
//! use ascetic_ddd_session::testing::MemorySessionPool;
//! use ascetic_ddd_session::{Session, SessionError, SessionPool};
//!
//! let first = MemorySessionPool::new();
//! let journal = first.journal();
//! let second = MemorySessionPool::new();
//!
//! futures::executor::block_on((first, second).session(async |sessions| {
//!     sessions.atomic(async |sessions| {
//!         sessions.0.record("INSERT INTO orders (id) VALUES (1)");
//!         Ok::<_, SessionError>(())
//!     })
//!     .await
//! })).unwrap();
//!
//! assert_eq!(
//!     journal.entries(),
//!     ["BEGIN", "INSERT INTO orders (id) VALUES (1)", "COMMIT"],
//! );
//! ```
//!
//! Tuples of two to eight sessions are supported, and delegates are reached by
//! position — `sessions.0`, `sessions.1` — which stays flat however many there
//! are. Nesting types instead (`(A, (B, C))`) would work as well, at the price
//! of `.1.0` chains growing with the count.
//!
//! The number of delegates is fixed at compile time. A count known only at run
//! time needs a collection, which in turn needs every delegate to be of the
//! same type — see [`many`][super::many].
//!
use crate::error::SessionError;
use crate::session::{Session, SessionPool};

/// Opens a scope on each delegate, left to right, and collects the sessions
/// they hand out into a tuple of the same shape.
macro_rules! open_scopes {
    // Every delegate is open: hand the collected sessions to the scope.
    ($self:ident, $scope:ident, [], [$($opened:ident),*]) => {{
        let sessions = ($($opened.clone(),)*);
        // Boxed: each level nests one closure per delegate, and the compiler
        // walks that nesting when it lays out the outer future. The box ends
        // the walk here, so nesting depth stays within the default
        // `recursion_limit`. The type is concrete, so `Send` is unaffected.
        Box::pin($scope(&sessions)).await
    }};
    // Open the next delegate and carry on inside its scope.
    ($self:ident, $scope:ident, [$index:tt $($rest:tt)*], [$($opened:ident),*]) => {
        $self.$index
            .atomic(async |next| open_scopes!($self, $scope, [$($rest)*], [$($opened,)* next]))
            .await
    };
}

/// Same, for a tuple of pools handing out a tuple of sessions.
macro_rules! open_sessions {
    ($self:ident, $scope:ident, [], [$($opened:ident),*]) => {{
        let sessions = ($($opened.clone(),)*);
        // Boxed for the same reason as in `open_scopes!`.
        Box::pin($scope(&sessions)).await
    }};
    ($self:ident, $scope:ident, [$index:tt $($rest:tt)*], [$($opened:ident),*]) => {
        $self.$index
            .session(async |next| open_sessions!($self, $scope, [$($rest)*], [$($opened,)* next]))
            .await
    };
}

macro_rules! impl_composite {
    ($($name:ident : $index:tt),+) => {
        /// Delegates are opened left to right and closed right to left.
        ///
        /// Each delegate refuses a second scope of its own, so the tuple needs
        /// no guard: opening two composite scopes at once is stopped by
        /// whichever delegate is asked first.
        impl<$($name: Session),+> Session for ($($name,)+) {
            async fn atomic<T, E, F>(&self, scope: F) -> Result<T, E>
            where
                F: AsyncFnOnce(&Self) -> Result<T, E>,
                E: From<SessionError>,
            {
                open_scopes!(self, scope, [$($index)+], [])
            }
        }

        /// A tuple of pools hands out a tuple of sessions.
        impl<$($name: SessionPool),+> SessionPool for ($($name,)+) {
            type Session = ($($name::Session,)+);

            async fn session<T, E, F>(&self, scope: F) -> Result<T, E>
            where
                F: AsyncFnOnce(&Self::Session) -> Result<T, E>,
                E: From<SessionError>,
            {
                open_sessions!(self, scope, [$($index)+], [])
            }
        }
    };
}

// Type parameters are named `S0..` rather than `A..` so that they cannot
// collide with the `E` and `F` of the method signatures at higher arities.
impl_composite!(S0: 0, S1: 1);
impl_composite!(S0: 0, S1: 1, S2: 2);
impl_composite!(S0: 0, S1: 1, S2: 2, S3: 3);
impl_composite!(S0: 0, S1: 1, S2: 2, S3: 3, S4: 4);
impl_composite!(S0: 0, S1: 1, S2: 2, S3: 3, S4: 4, S5: 5);
impl_composite!(S0: 0, S1: 1, S2: 2, S3: 3, S4: 4, S5: 5, S6: 6);
impl_composite!(S0: 0, S1: 1, S2: 2, S3: 3, S4: 4, S5: 5, S6: 6, S7: 7);
