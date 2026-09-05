//! Observing a session without taking part in it.
//!
//! Python and Go give every session six signals with `attach` / `detach` /
//! `observer_id` / `Disposable`. Across the whole Python code base there is one
//! real `attach` (guarded by a "subscribe once" flag), no `detach` at all, and
//! nothing subscribes to the session's own signals. The dynamic registry is
//! capability nobody uses, and its notion of observer identity - the address of
//! a function object - has no counterpart in Rust.
//!
//! So a signal is modelled as what it is: a function. A signal with several
//! subscribers is the composition of several functions, and composition is
//! spelled with values rather than with a mutable registry:
//!
//! ```
//! use ascetic_ddd_session::observer::{ScopeEnded, SessionObserver};
//!
//! struct Log;
//! struct Metrics;
//!
//! impl SessionObserver for Log {
//!     fn on_scope_ended(&self, event: &ScopeEnded) {
//!         println!("{:?} at depth {} {:?}", event.kind, event.depth, event.outcome);
//!     }
//! }
//!
//! impl SessionObserver for Metrics {
//!     fn on_scope_ended(&self, _event: &ScopeEnded) { /* … */ }
//! }
//!
//! let observer = (Log, Metrics);   // CompositeSignal, выраженный значением
//! ```
//!
//! This trait carries only what every session does: opening and closing scopes.
//! What a session does *besides* that is transport-specific and lives with the
//! transport — [`PgObserver`][crate::pg::PgObserver] adds the statements a
//! PostgreSQL session executes, [`RestObserver`][crate::rest::RestObserver] the
//! requests a REST session makes. Each extends this trait, so one session is
//! still watched by one observer.
//!
//! Observers are **synchronous and infallible** on purpose. They observe; they
//! do not participate. In the Go port a failing `Notify` aborts the surrounding
//! transaction, which makes logging a source of business failures. An observer
//! that must do asynchronous work sends the event to a channel and lets its own
//! task deal with it.

use std::sync::Arc;

/// What kind of boundary a scope opened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScopeKind {
    /// A connection was taken from the pool; no transaction yet.
    Session,
    /// The outermost transaction: `BEGIN`.
    Transaction,
    /// A nested transaction: `SAVEPOINT`.
    Savepoint,
    /// A scope with no transaction behind it, as in a REST session: it groups
    /// work and bounds an identity map, but nothing is committed.
    Logical,
}

/// How a scope ended.
///
/// For a [`ScopeKind::Logical`] scope the names mean simply "succeeded" and
/// "failed": there is nothing to commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The scope succeeded and was committed (or released).
    Committed,
    /// The scope failed and was rolled back.
    RolledBack,
}

/// A scope has been opened.
#[derive(Clone, Copy, Debug)]
pub struct ScopeStarted {
    /// Nesting depth: 0 for the session scope, 1 for the outermost transaction.
    pub depth: usize,
    /// What kind of boundary this is.
    pub kind: ScopeKind,
}

/// A scope has been closed.
#[derive(Clone, Copy, Debug)]
pub struct ScopeEnded {
    /// Nesting depth of the scope that ended.
    pub depth: usize,
    /// What kind of boundary this was.
    pub kind: ScopeKind,
    /// Whether it was committed or rolled back.
    pub outcome: Outcome,
}

/// Watches the session lifecycle.
///
/// Every method defaults to doing nothing, so an implementation states only
/// what it cares about.
pub trait SessionObserver: Send + Sync {
    /// A session, transaction or savepoint scope has been opened.
    fn on_scope_started(&self, _event: &ScopeStarted) {}

    /// A scope has been committed or rolled back.
    fn on_scope_ended(&self, _event: &ScopeEnded) {}
}

/// The neutral element: observing nothing.
impl SessionObserver for () {}

/// Composition of two observers, notified left to right.
impl<A: SessionObserver, B: SessionObserver> SessionObserver for (A, B) {
    fn on_scope_started(&self, event: &ScopeStarted) {
        self.0.on_scope_started(event);
        self.1.on_scope_started(event);
    }

    fn on_scope_ended(&self, event: &ScopeEnded) {
        self.0.on_scope_ended(event);
        self.1.on_scope_ended(event);
    }
}

/// Shared observers are observers.
impl<O: SessionObserver + ?Sized> SessionObserver for Arc<O> {
    fn on_scope_started(&self, event: &ScopeStarted) {
        (**self).on_scope_started(event);
    }

    fn on_scope_ended(&self, event: &ScopeEnded) {
        (**self).on_scope_ended(event);
    }
}

/// N-ary composition, notified in order.
impl<O: SessionObserver> SessionObserver for Vec<O> {
    fn on_scope_started(&self, event: &ScopeStarted) {
        self.iter().for_each(|o| o.on_scope_started(event));
    }

    fn on_scope_ended(&self, event: &ScopeEnded) {
        self.iter().for_each(|o| o.on_scope_ended(event));
    }
}
