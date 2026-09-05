//! What a PostgreSQL session reports.
//!
//! [`PgObserver`] extends [`SessionObserver`] rather than standing beside it:
//! one session both opens scopes and runs statements, so one observer watches
//! both. An observer that only cares about scopes implements `SessionObserver`
//! and adds an empty `impl PgObserver for … {}`.

use std::sync::Arc;
use std::time::Duration;

use crate::observer::SessionObserver;

/// A statement is about to be executed.
#[derive(Clone, Copy, Debug)]
pub struct QueryStarted<'a> {
    /// The statement text.
    pub statement: &'a str,
}

/// A statement has finished executing.
#[derive(Clone, Copy, Debug)]
pub struct QueryEnded<'a> {
    /// The statement text.
    pub statement: &'a str,
    /// How long it took.
    pub elapsed: Duration,
    /// Whether the driver reported an error.
    pub failed: bool,
}

/// Watches a PostgreSQL session: its scopes, through [`SessionObserver`], and
/// the statements it executes.
///
/// Every method defaults to doing nothing, so an implementation states only
/// what it cares about.
pub trait PgObserver: SessionObserver {
    /// A statement is about to run.
    fn on_query_started(&self, _event: &QueryStarted<'_>) {}

    /// A statement has finished.
    fn on_query_ended(&self, _event: &QueryEnded<'_>) {}
}

/// The neutral element: observing nothing.
impl PgObserver for () {}

/// Composition of two observers, notified left to right.
impl<A: PgObserver, B: PgObserver> PgObserver for (A, B) {
    fn on_query_started(&self, event: &QueryStarted<'_>) {
        self.0.on_query_started(event);
        self.1.on_query_started(event);
    }

    fn on_query_ended(&self, event: &QueryEnded<'_>) {
        self.0.on_query_ended(event);
        self.1.on_query_ended(event);
    }
}

/// Shared observers are observers.
impl<O: PgObserver + ?Sized> PgObserver for Arc<O> {
    fn on_query_started(&self, event: &QueryStarted<'_>) {
        (**self).on_query_started(event);
    }

    fn on_query_ended(&self, event: &QueryEnded<'_>) {
        (**self).on_query_ended(event);
    }
}

/// N-ary composition, notified in order.
impl<O: PgObserver> PgObserver for Vec<O> {
    fn on_query_started(&self, event: &QueryStarted<'_>) {
        self.iter().for_each(|o| o.on_query_started(event));
    }

    fn on_query_ended(&self, event: &QueryEnded<'_>) {
        self.iter().for_each(|o| o.on_query_ended(event));
    }
}
