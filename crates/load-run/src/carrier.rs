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
    /// A connection, pooled or in use: its buffers — the head buffer at the
    /// plan's largest request head (K2b-7e-2b-3a-2) —, its task, its place
    /// in its pool.
    pub connection: u64,
    /// A connection's own heap its egress states (TLS's buffers): the
    /// host's.
    pub io: u64,
    /// An answer being read at the profile's cap: its body, and its head as
    /// it is read and copied.
    pub answer: u64,
}

/// What a carrier holds an attempt beside its connection and the shell's
/// share (ADR-0047 decision 22, K2b-7e-2b-3a-2), process-wide: a part each,
/// and its request by the record's request bytes by class — coefficients the
/// shell multiplies by a shape's counts ([`price`](AttemptCosts::price)), so
/// the plan's own shapes price every attempt it holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AttemptCosts {
    /// Each attempt's own: its task, its small parts.
    pub each: u64,
    /// A target's literal byte.
    pub target: u64,
    /// A target's hole.
    pub target_hole: u64,
    /// A header's literal byte.
    pub header: u64,
    /// A header's hole.
    pub header_hole: u64,
    /// A query's or a header's field.
    pub field: u64,
    /// A body's byte.
    pub body: u64,
    /// A body's hole.
    pub body_hole: u64,
    /// A captured hole of the record's: the value the carrier copies.
    pub hole: u64,
    /// A byte of a captured hole's subject and slot.
    pub hole_bytes: u64,
    /// Each attempt's decorations as the carrier holds them, at the bound
    /// its host's decorator states (K2b-7e-2b-3b): the host's.
    pub decorations: u64,
    /// Who states the rest.
    pub stated_by: StatedBy,
}

impl AttemptCosts {
    /// A carrier holding nothing an attempt — a test's script — by its own
    /// word.
    pub const NOTHING: AttemptCosts = AttemptCosts {
        each: 0,
        target: 0,
        target_hole: 0,
        header: 0,
        header_hole: 0,
        field: 0,
        body: 0,
        body_hole: 0,
        hole: 0,
        hole_bytes: 0,
        decorations: 0,
        stated_by: StatedBy::Host,
    };

    /// The carrier's own of the attempts of records summed into `s`, by
    /// their count (ADR-0047 decision 22, K2b-7e-2b-3d-3a): each one's part,
    /// and every class's bytes and holes at their coefficients beside — its
    /// decorations apart, the decorator's.
    #[must_use]
    pub fn priced(&self, s: &ascetic_ddd_command_log::shape::Shape) -> (u64, u64) {
        let decorated = AttemptCosts {
            each: 0,
            decorations: 0,
            ..*self
        };
        (self.each, decorated.price(s))
    }

    /// What the attempts of records summed into `s` hold: each one's part,
    /// and every class's bytes and holes at their coefficients.
    #[must_use]
    pub fn price(&self, s: &ascetic_ddd_command_log::shape::Shape) -> u64 {
        let AttemptCosts {
            each,
            target,
            target_hole,
            header,
            header_hole,
            field,
            body,
            body_hole,
            hole,
            hole_bytes,
            decorations,
            stated_by: _,
        } = *self;
        [
            (each, s.records),
            (decorations, s.records),
            (target, s.target),
            (target_hole, s.target_holes),
            (header, s.headers),
            (header_hole, s.header_holes),
            (field, s.fields.elements),
            (body, s.body),
            (body_hole, s.body_holes),
            (hole, s.holes),
            (hole_bytes, s.hole_bytes),
        ]
        .into_iter()
        .fold(0u64, |sum, (c, n)| sum.saturating_add(c.saturating_mul(n)))
    }
}

/// A part whose bytes its owner does not state: which (ADR-0047 decision 22,
/// K2b-7e-2b-3) — admission refuses it, never charges it nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Unstated {
    /// The part.
    pub part: &'static str,
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

impl From<StatedBy> for ascetic_ddd_scheduler::journal::StatedBy {
    fn from(s: StatedBy) -> ascetic_ddd_scheduler::journal::StatedBy {
        match s {
            StatedBy::Library => ascetic_ddd_scheduler::journal::StatedBy::Library,
            StatedBy::Host => ascetic_ddd_scheduler::journal::StatedBy::Host,
        }
    }
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

    /// What it holds an attempt under the plan's `peaks` (decision 22,
    /// K2b-7e-2b-3a-2), process-wide: its coefficients by class; a part it
    /// cannot state, unstated.
    fn attempts(&self, peaks: &Peaks) -> Result<AttemptCosts, Unstated>;

    /// What its stand's gates' client holds (decision 22, K2b-7e-2b-3b): a
    /// carrier with no stand, none.
    fn gate_bytes(&self) -> crate::stand::GateBytes;

    /// The sockets it holds open now, by its own word (ADR-0047 decision 22,
    /// K2b-7e-2b-3e): the run's samples journal it beside the kernel's socket
    /// memory; none only where it says it holds none it counts. Required, so
    /// that a carrier states it or says why not.
    fn sockets(&self) -> Option<u64>;

    /// Its egress's total of ports, the knob a memory refusal's remedy names
    /// (ADR-0047 decision 22, K2b-7e-2b-3d-2); none for a carrier with no
    /// such knob.
    fn ports(&self) -> Option<usize> {
        None
    }

    /// Each of `sources` sources' cap at a total of `ports`, as its egress
    /// divides them — the map a remedy searches over; none for a carrier
    /// with no such knob.
    fn caps_at(&self, ports: usize, sources: u32) -> Option<Vec<usize>> {
        let _ = (ports, sources);
        None
    }

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
    use super::{AttemptCosts, SourceCosts, StatedBy};

    /// An attempt's price: each part times the records, each class's
    /// coefficient times its count — its decorations each attempt's.
    #[test]
    fn an_attempt_s_price_sums_its_parts() {
        let shape = ascetic_ddd_command_log::shape::Shape {
            records: 2,
            target: 10,
            holes: 1,
            ..Default::default()
        };
        let costs = AttemptCosts {
            each: 100,
            target: 6,
            hole: 1_000,
            decorations: 7,
            ..AttemptCosts::NOTHING
        };
        assert_eq!(costs.price(&shape), 2 * 100 + 2 * 7 + 6 * 10 + 1_000);
    }

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
