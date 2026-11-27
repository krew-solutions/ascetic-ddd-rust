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
    /// bound, at most `cap` held, each part's bytes as `costs` states them.
    Sockets {
        /// At once, under the plan's bound.
        needed: u64,
        /// Its cap on the source.
        cap: usize,
        /// What its parts hold (decision 22).
        costs: Costs,
    },
}

/// The bytes a carrier's parts hold at most (ADR-0047 decision 22), beside
/// the machine's cells and the shell's own share an attempt
/// ([`ATTEMPT`](crate::run::ATTEMPT)): each an upper bound over the part's
/// life, transients included, that its executor holds two-sided
/// (`tests/costs.rs`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Costs {
    /// A connection, pooled or in use: its buffers, its task, its place in
    /// its pool.
    pub connection: u64,
    /// An attempt beside its connection: its task, its frame, its request in
    /// flight.
    pub attempt: u64,
    /// An answer being read at the profile's cap: its body, and its head as
    /// it is read and copied.
    pub answer: u64,
}

/// Who states a carrier's costs (ADR-0047 decision 22): the library, for its
/// own carrier, its executors holding them; or a host, by its word.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatedBy {
    /// The library's own carrier.
    Library,
    /// A host's carrier.
    Host,
}

/// What a carrier holds a source beside its sockets (ADR-0047 decision 22,
/// K2b-7e-2b-2c-4): on the source's thread, whatever its sockets do, each an
/// upper bound over the part's life — charged summed, not their largest, since
/// a host's carrier may hold them at once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SourceCosts {
    /// Its scratch a source's thread: what its reads copy through.
    pub scratch: u64,
    /// An answer's head as it is copied, one at a time a source.
    pub head: u64,
    /// The most a source's answered tails hold at once: its answers read,
    /// parsed and judged.
    pub tail: u64,
    /// Who states them.
    pub stated_by: StatedBy,
}

impl SourceCosts {
    /// A carrier holding nothing on its source's thread — a test's script —
    /// by its own word.
    pub const NOTHING: SourceCosts = SourceCosts {
        scratch: 0,
        head: 0,
        tail: 0,
        stated_by: StatedBy::Host,
    };

    /// What they charge a source: their sum.
    #[must_use]
    pub const fn total(&self) -> u64 {
        self.scratch
            .saturating_add(self.head)
            .saturating_add(self.tail)
    }
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

    /// What it holds of `source` beside its sockets under the plan's `peaks`
    /// (decision 22): its own to tell, socketless or not; a source it does
    /// not know, refused — never none.
    fn source_costs(&self, peaks: &Peaks, source: u32) -> Result<SourceCosts, String>;

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

#[cfg(test)]
mod tests {
    use super::{SourceCosts, StatedBy};

    /// A carrier's source costs charged summed, not their largest, and
    /// saturating.
    #[test]
    fn source_costs_are_charged_summed() {
        let c = SourceCosts {
            scratch: 1,
            head: 20,
            tail: 300,
            stated_by: StatedBy::Host,
        };
        assert_eq!(c.total(), 321);
        assert_eq!(
            SourceCosts {
                tail: u64::MAX,
                ..c
            }
            .total(),
            u64::MAX
        );
    }
}
