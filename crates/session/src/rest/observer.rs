//! What a REST session reports.
//!
//! [`RestObserver`] extends [`SessionObserver`] rather than standing beside it:
//! one session both opens scopes and makes requests, so one observer watches
//! both.

use std::sync::Arc;
use std::time::Duration;

use crate::observer::SessionObserver;

/// An outbound request is about to be made.
#[derive(Clone, Copy, Debug)]
pub struct RequestStarted<'a> {
    /// HTTP method.
    pub method: &'a str,
    /// Target URL.
    pub url: &'a str,
}

/// An outbound request has finished.
///
/// Python carries a `RequestViewModel` with a pre-formatted metrics label; the
/// parts are given here instead, so that the observer decides how to name it.
#[derive(Clone, Copy, Debug)]
pub struct RequestEnded<'a> {
    /// HTTP method.
    pub method: &'a str,
    /// Target URL.
    pub url: &'a str,
    /// How long it took.
    pub elapsed: Duration,
    /// Whether the call returned an error.
    pub failed: bool,
}

/// Watches a REST session: its scopes, through [`SessionObserver`], and
/// the statements it executes.
///
/// Every method defaults to doing nothing, so an implementation states only
/// what it cares about.
pub trait RestObserver: SessionObserver {
    /// An outbound request is about to be made.
    fn on_request_started(&self, _event: &RequestStarted<'_>) {}

    /// An outbound request has finished.
    fn on_request_ended(&self, _event: &RequestEnded<'_>) {}
}

/// The neutral element: observing nothing.
impl RestObserver for () {}

/// Composition of two observers, notified left to right.
impl<A: RestObserver, B: RestObserver> RestObserver for (A, B) {
    fn on_request_started(&self, event: &RequestStarted<'_>) {
        self.0.on_request_started(event);
        self.1.on_request_started(event);
    }

    fn on_request_ended(&self, event: &RequestEnded<'_>) {
        self.0.on_request_ended(event);
        self.1.on_request_ended(event);
    }
}

/// Shared observers are observers.
impl<O: RestObserver + ?Sized> RestObserver for Arc<O> {
    fn on_request_started(&self, event: &RequestStarted<'_>) {
        (**self).on_request_started(event);
    }

    fn on_request_ended(&self, event: &RequestEnded<'_>) {
        (**self).on_request_ended(event);
    }
}

/// N-ary composition, notified in order.
impl<O: RestObserver> RestObserver for Vec<O> {
    fn on_request_started(&self, event: &RequestStarted<'_>) {
        self.iter().for_each(|o| o.on_request_started(event));
    }

    fn on_request_ended(&self, event: &RequestEnded<'_>) {
        self.iter().for_each(|o| o.on_request_ended(event));
    }
}
