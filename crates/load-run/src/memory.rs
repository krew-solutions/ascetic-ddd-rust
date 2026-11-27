//! Admission against a stated memory (ADR-0047 decision 22, K2b-7e-2b-3): a
//! pure function of the parts admission fixed — each a count held by an
//! enforcer times a cost stated by its owner — and the memory the operator
//! states. The process passes three phases, each holding its own and the
//! fixed parts: the stages' judging before T0, the gates' probes, the run
//! with its pool of steps and attempts. What a run needs is the largest; the
//! pool's budget is what the run's fixed parts leave, its prediction priced
//! at the run's catch-up.

use crate::carrier::StatedBy;
use crate::stand::GateBytes;

/// The memory the operator states (decision 22): declared, never the host's
/// free memory guessed — the cgroup's with K2b-7e-2c.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Memory {
    bytes: u64,
}

impl Memory {
    /// `bytes`, declared.
    #[must_use]
    pub const fn declared(bytes: u64) -> Memory {
        Memory { bytes }
    }

    /// Its bytes.
    #[must_use]
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }
}

/// Where a part's bytes are held, a phase of the process's life.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// From the build to the end: in every phase.
    Fixed,
    /// The stages' judging, before T0.
    Stages,
    /// The gates' probes and clients, before T0.
    Gates,
    /// The run's own beside its pool.
    Run,
    /// The run's pool: steps read and not ended, attempts and their handles.
    Pool,
}

/// A part of the process (decision 22): every part admission prices, closed —
/// a part added does not compile until it is priced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Part {
    /// The reader, its channel of records ahead and its thread.
    Reader,
    /// The log's decompressor, outside the counted heap.
    Decoder,
    /// A source's runtime and its driver's events.
    Runtime,
    /// A carrier's own of a source: its scratch, a head copied, its tails.
    CarrierSource,
    /// The shell's keeping of an answer's captures, one a source.
    Keeping,
    /// The journal's channel, its writer and its output.
    Journal,
    /// A source's connections at its cap.
    Connections,
    /// Their egress's own heap.
    Io,
    /// The answers they read at the cap.
    Answers,
    /// Stage 2's judging at the cap.
    Judging,
    /// Stage 3's probes, a connection each at most the cap's.
    Probes,
    /// The gates' client while the stages open them.
    GateStages,
    /// The gates' client kept through the run.
    GateRun,
    /// The machine's cells.
    Cells,
    /// The machine's barrier keys.
    Barriers,
    /// The records of the steps late by a jitter: not ended, or held past
    /// their end by an attempt's handle.
    Records,
    /// The producers the steps are bound to.
    BoundTo,
    /// What the held keep once ended.
    Kept,
    /// One record's bindings while its step is taken.
    Binding,
    /// The shell's share of the attempts.
    ShellAttempts,
    /// The carrier's share of the attempts.
    CarrierAttempts,
}

impl Part {
    /// Every part.
    pub const ALL: [Part; 21] = [
        Part::Reader,
        Part::Decoder,
        Part::Runtime,
        Part::CarrierSource,
        Part::Keeping,
        Part::Journal,
        Part::Connections,
        Part::Io,
        Part::Answers,
        Part::Judging,
        Part::Probes,
        Part::GateStages,
        Part::GateRun,
        Part::Cells,
        Part::Barriers,
        Part::Records,
        Part::BoundTo,
        Part::Kept,
        Part::Binding,
        Part::ShellAttempts,
        Part::CarrierAttempts,
    ];
}

/// Who holds a part's count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Held {
    /// What is built, as built.
    Built,
    /// A channel's bound, by type.
    Channel,
    /// A source's cap on its sockets.
    Cap,
    /// One a source, or one a process.
    Once,
    /// The read gate (K2b-7e-3), once built; until then the plan's bound,
    /// nothing enforcing it.
    Gate {
        /// Whether it is built.
        built: bool,
    },
    /// The host's word.
    Host,
}

/// What a part's bytes are counted against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Basis {
    /// The counted heap the executors hold.
    Heap,
    /// Outside it: a library's own allocator.
    Outside,
}

/// A part's term: its bytes, where they are held and who states them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Term {
    /// The part.
    pub part: Part,
    /// Its source, if it is a source's.
    pub source: Option<u32>,
    /// Its phase.
    pub phase: Phase,
    /// Its bytes.
    pub bytes: u64,
    /// Who holds its count.
    pub held: Held,
    /// Who states its cost.
    pub stated: StatedBy,
    /// What it is counted against.
    pub basis: Basis,
}

/// A source's parts as admission fixed them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SourcePart {
    /// Its runtime's own and its events, as they will be built; none where
    /// no event's size is stated.
    pub runtime: Option<u64>,
    /// Its sockets' cap.
    pub cap: u64,
    /// A connection's bytes, its head buffer at the plan's largest head.
    pub connection: u64,
    /// A connection's own heap, as its egress states it.
    pub io: u64,
    /// An answer's at the cap.
    pub answer: u64,
    /// The carrier's own of the source.
    pub carrier: u64,
    /// Who states the carrier's.
    pub carrier_stated: StatedBy,
    /// The shell's keeping, one a source.
    pub keeping: u64,
}

/// What admission fixed of a run (decision 22): each part's bytes at its
/// count, the pool's priced at the plan's peaks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Planned {
    /// Each source's.
    pub sources: Vec<SourcePart>,
    /// The reader's, at its records ahead.
    pub reader: u64,
    /// The log's decompressor's, outside the counted heap.
    pub decoder: u64,
    /// The journal's, at its lines behind.
    pub journal: u64,
    /// Stage 2's judging at the cap.
    pub judging: u64,
    /// The stand's gates' client.
    pub gates: GateBytes,
    /// The machine's terms, its records apart.
    pub machine: ascetic_ddd_scheduler::fits::Terms,
    /// The records of the steps late by a jitter, in place of the machine's.
    pub records: u64,
    /// The shell's share of the attempts the plan holds at once.
    pub shell_attempts: u64,
    /// The carrier's share of them.
    pub carrier_attempts: u64,
    /// Who states the carrier's.
    pub carrier_stated: StatedBy,
    /// The run's catch-up, per mille: the pool's prediction at its rate.
    pub kappa: u64,
}

/// Why a run does not fit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failed {
    /// A phase needs more than the stated memory.
    Memory,
    /// The pool's prediction past what the run's fixed parts leave it (I2).
    Pool,
    /// A part's bytes past any count.
    Overflow(Part),
    /// A part no owner states.
    Unstated(Part),
}

/// A run that fits: its terms, what it needs and what is left.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fit {
    /// Every term.
    pub terms: Vec<Term>,
    /// What the process needs at its largest phase.
    pub need: u64,
    /// The stated memory less it.
    pub slack: u64,
    /// The pool's budget: the stated memory less the run's fixed parts.
    pub budget: u64,
    /// The pool's prediction at the run's catch-up.
    pub pool: u64,
    /// Whether every count is held: the pool's is not until the read gate.
    pub held: bool,
}

/// A run that does not fit: every term, what failed, the term that binds
/// and the least memory that would admit it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unfit {
    /// Every term priced.
    pub terms: Vec<Term>,
    /// What failed.
    pub failed: Failed,
    /// The largest term of the phase that binds, an index of `terms`.
    pub binding: Option<usize>,
    /// The least memory that admits it; none past any count.
    pub least: Option<u64>,
}

/// The pool's terms at the run's catch-up: `⌈bytes·1000/(1000 − κ)⌉`.
fn at_kappa(bytes: u64, kappa: u64) -> Option<u64> {
    let c = u128::from(1000u64.checked_sub(kappa).filter(|c| *c > 0)?);
    let scaled = u128::from(bytes).checked_mul(1000)?;
    let up = scaled.checked_add(c.checked_sub(1)?)?.checked_div(c)?;
    u64::try_from(up).ok()
}

/// The terms of `planned`, each part priced, its phase, holder and owner.
#[must_use]
pub fn terms(planned: &Planned) -> Vec<Term> {
    let library = StatedBy::Library;
    let host = StatedBy::Host;
    let term = |part, source, phase, bytes, held, stated, basis| Term {
        part,
        source,
        phase,
        bytes,
        held,
        stated,
        basis,
    };
    let mut out = vec![
        term(
            Part::Reader,
            None,
            Phase::Fixed,
            planned.reader,
            Held::Channel,
            library,
            Basis::Heap,
        ),
        term(
            Part::Decoder,
            None,
            Phase::Fixed,
            planned.decoder,
            Held::Built,
            library,
            Basis::Outside,
        ),
    ];
    for (s, p) in (0u32..).zip(&planned.sources) {
        let source = Some(s);
        let connections = p.cap.saturating_mul(p.connection);
        out.extend([
            term(
                Part::Runtime,
                source,
                Phase::Fixed,
                p.runtime.unwrap_or(u64::MAX),
                Held::Built,
                library,
                Basis::Heap,
            ),
            term(
                Part::CarrierSource,
                source,
                Phase::Run,
                p.carrier,
                Held::Once,
                p.carrier_stated,
                Basis::Heap,
            ),
            term(
                Part::Keeping,
                source,
                Phase::Run,
                p.keeping,
                Held::Once,
                library,
                Basis::Heap,
            ),
            term(
                Part::Connections,
                source,
                Phase::Run,
                connections,
                Held::Cap,
                library,
                Basis::Heap,
            ),
            term(
                Part::Io,
                source,
                Phase::Run,
                p.cap.saturating_mul(p.io),
                Held::Cap,
                host,
                Basis::Heap,
            ),
            term(
                Part::Answers,
                source,
                Phase::Run,
                p.cap.saturating_mul(p.answer),
                Held::Cap,
                library,
                Basis::Heap,
            ),
            // Stage 3's probes: a connect to each endpoint, each a socket
            // under the cap.
            term(
                Part::Probes,
                source,
                Phase::Gates,
                p.cap.saturating_mul(p.connection.saturating_add(p.io)),
                Held::Cap,
                library,
                Basis::Heap,
            ),
        ]);
    }
    let gate = Held::Gate { built: false };
    let m = planned.machine;
    out.extend([
        term(
            Part::Journal,
            None,
            Phase::Run,
            planned.journal,
            Held::Channel,
            library,
            Basis::Heap,
        ),
        term(
            Part::Judging,
            None,
            Phase::Stages,
            planned.judging,
            Held::Once,
            library,
            Basis::Heap,
        ),
        term(
            Part::GateStages,
            None,
            Phase::Gates,
            planned.gates.stages,
            Held::Host,
            host,
            Basis::Heap,
        ),
        term(
            Part::GateRun,
            None,
            Phase::Run,
            planned.gates.run,
            Held::Host,
            host,
            Basis::Heap,
        ),
        term(
            Part::Cells,
            None,
            Phase::Pool,
            m.cells,
            gate,
            library,
            Basis::Heap,
        ),
        term(
            Part::Barriers,
            None,
            Phase::Pool,
            m.barriers,
            Held::Once,
            library,
            Basis::Heap,
        ),
        term(
            Part::Records,
            None,
            Phase::Pool,
            planned.records,
            gate,
            library,
            Basis::Heap,
        ),
        term(
            Part::BoundTo,
            None,
            Phase::Pool,
            m.bound_to,
            gate,
            library,
            Basis::Heap,
        ),
        term(
            Part::Kept,
            None,
            Phase::Pool,
            m.keeping,
            gate,
            library,
            Basis::Heap,
        ),
        term(
            Part::Binding,
            None,
            Phase::Pool,
            m.binding,
            Held::Once,
            library,
            Basis::Heap,
        ),
        term(
            Part::ShellAttempts,
            None,
            Phase::Pool,
            planned.shell_attempts,
            gate,
            library,
            Basis::Heap,
        ),
        term(
            Part::CarrierAttempts,
            None,
            Phase::Pool,
            planned.carrier_attempts,
            gate,
            planned.carrier_stated,
            Basis::Heap,
        ),
    ]);
    out
}

/// The sum of `terms` in `phase`, checked; past any count, the first part
/// that passed it.
fn sum(terms: &[Term], phase: Phase) -> Result<u64, Part> {
    terms
        .iter()
        .filter(|t| t.phase == phase)
        .try_fold(0u64, |n, t| n.checked_add(t.bytes).ok_or(t.part))
}

/// What a run of `planned` needs, and whether it fits `memory` (decision 22,
/// K2b-7e-2b-3c): the largest of three phases — the fixed parts with the
/// stages' judging, with the gates', with the run's own and its pool at the
/// catch-up's rate —, the pool within what the run's fixed parts leave it
/// (I2). Every sum checked: a part past any count is refused, never fits.
pub fn fit(planned: &Planned, memory: Memory) -> Result<Fit, Unfit> {
    let terms = terms(planned);
    let unfit = |terms: Vec<Term>, failed| Unfit {
        terms,
        failed,
        binding: None,
        least: None,
    };
    if let Some(t) = terms
        .iter()
        .find(|t| t.part == Part::Runtime && t.bytes == u64::MAX)
    {
        return Err(unfit(terms.clone(), Failed::Unstated(t.part)));
    }
    // A count times its cost past any count saturates: refused, never a
    // term that a memory stated at its most would admit.
    if let Some(t) = terms.iter().find(|t| t.bytes == u64::MAX) {
        return Err(unfit(terms.clone(), Failed::Overflow(t.part)));
    }
    let sums = [
        Phase::Fixed,
        Phase::Stages,
        Phase::Gates,
        Phase::Run,
        Phase::Pool,
    ]
    .map(|p| sum(&terms, p));
    let [fixed, stages, gates, run, pool] = match sums {
        [Ok(a), Ok(b), Ok(c), Ok(d), Ok(e)] => [a, b, c, d, e],
        _ => {
            let part = sums.iter().find_map(|s| s.err()).unwrap_or(Part::Reader);
            return Err(unfit(terms, Failed::Overflow(part)));
        }
    };
    let Some(predicted) = at_kappa(pool, planned.kappa) else {
        return Err(unfit(terms, Failed::Overflow(Part::Cells)));
    };
    let phases = [
        (Phase::Stages, fixed.checked_add(stages)),
        (Phase::Gates, fixed.checked_add(gates)),
        (
            Phase::Run,
            fixed
                .checked_add(run)
                .and_then(|n| n.checked_add(predicted)),
        ),
    ];
    let mut need = 0u64;
    let mut bind = Phase::Run;
    for (phase, n) in phases {
        let Some(n) = n else {
            return Err(unfit(terms, Failed::Overflow(Part::Reader)));
        };
        if n > need {
            need = n;
            bind = phase;
        }
    }
    let Some(slack) = memory.bytes.checked_sub(need) else {
        let binding = binding(&terms, bind);
        let failed =
            if bind == Phase::Run && fixed.checked_add(run).is_some_and(|n| n <= memory.bytes) {
                Failed::Pool
            } else {
                Failed::Memory
            };
        return Err(Unfit {
            terms,
            failed,
            binding,
            least: Some(need),
        });
    };
    // The run's phase within `need`, within the stated bytes.
    let budget = memory.bytes.saturating_sub(fixed.saturating_add(run));
    let held = !terms
        .iter()
        .any(|t| matches!(t.held, Held::Gate { built: false }));
    Ok(Fit {
        terms,
        need,
        slack,
        budget,
        pool: predicted,
        held,
    })
}

/// The largest term of the phase `bind` that binds: its own, or the fixed
/// parts every phase holds.
fn binding(terms: &[Term], bind: Phase) -> Option<usize> {
    let own = |t: &Term| match bind {
        Phase::Run => matches!(t.phase, Phase::Run | Phase::Pool | Phase::Fixed),
        p => t.phase == p || t.phase == Phase::Fixed,
    };
    terms
        .iter()
        .enumerate()
        .filter(|(_, t)| own(t))
        .max_by_key(|(_, t)| t.bytes)
        .map(|(i, _)| i)
}
