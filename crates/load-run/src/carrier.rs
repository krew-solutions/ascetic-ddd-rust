//! What carries an attempt (ADR-0047 decision 16): the REST carrier over
//! hyper's own connection (K2b-2), or a test's script.

use std::future::Future;
use std::sync::Arc;

use ascetic_ddd_command_log::record::Header;
use ascetic_ddd_command_log::validate::Peaks;

use crate::clock::Clock;
use crate::stamps::Attempt;
use crate::stand::{Opening, Refused};

/// What a carrier needs of a source under the plan's bound (ADR-0047
/// decision 20): its own to tell — it alone maps attempts to sockets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Needs {
    /// No socket of its own on the source's runtime: a test's script.
    Nothing,
    /// Sockets on the source's runtime: `needed` at once under the plan's
    /// bound, at most `cap` held.
    Sockets {
        /// At once, under the plan's bound.
        needed: u64,
        /// Its cap on the source.
        cap: usize,
    },
}

/// A probe's failure (decision 20): the declared endpoint's index, and how —
/// a dotted part (`refused`, `unanswered`, `egress_local`, …), never the
/// host's text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unprobed {
    /// The endpoint's index among those its source declares.
    pub endpoint: usize,
    /// How.
    pub kind: String,
}

/// Carries attempts: each in a task of its own, spawned on its source's
/// runtime, reporting through its [`Attempt`].
pub trait Carrier<C: Clock>: Send + Sync + 'static {
    /// Refuses, before T0, a log it cannot carry: its reason.
    fn carries(&self, header: &Header) -> Result<(), String>;

    /// What it needs of `source` under the plan's `peaks` (decision 20); a
    /// source it does not know, refused — never none.
    fn needs(&self, peaks: &Peaks, source: u32) -> Result<Needs, String>;

    /// Its sockets of `source` probed before T0 (decision 20): one connect
    /// to each endpoint it declares for it, all at once, each within `bound`
    /// µs of `clock`, nothing sent — the failures; none, passed. Run on the
    /// source's runtime.
    fn probe(
        &self,
        source: u32,
        clock: Arc<C>,
        bound: u64,
    ) -> impl Future<Output = Vec<Unprobed>> + Send + 'static;

    /// The task that carries one attempt.
    fn carry(&self, attempt: Attempt<C>) -> impl Future<Output = ()> + Send + 'static;

    /// Its stand's gate `i` before T0 (decision 18; see
    /// [`Stand::gate`](crate::stand::Stand::gate)): a carrier with no stand
    /// has none.
    fn gate(
        &self,
        _i: usize,
        _opening: Opening<'_>,
    ) -> impl Future<Output = Option<Result<String, Refused>>> + Send {
        std::future::ready(None)
    }
}
