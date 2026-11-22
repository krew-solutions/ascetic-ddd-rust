//! What carries an attempt (ADR-0047 decision 16): the REST carrier over
//! hyper's own connection (K2b-2), or a test's script.

use std::future::Future;

use ascetic_ddd_command_log::record::Header;

use crate::clock::Clock;
use crate::stamps::Attempt;

/// Carries attempts: each in a task of its own, spawned on its source's
/// runtime, reporting through its [`Attempt`].
pub trait Carrier<C: Clock>: Send + Sync + 'static {
    /// Refuses, before T0, a log it cannot carry: its reason.
    fn carries(&self, header: &Header) -> Result<(), String>;

    /// The task that carries one attempt.
    fn carry(&self, attempt: Attempt<C>) -> impl Future<Output = ()> + Send + 'static;
}
