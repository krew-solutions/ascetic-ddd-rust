//! Admission against a stated memory (ADR-0047 decision 22, K2b-7e-2b-3): a
//! pure function of the parts admission fixed — each a count held by an
//! enforcer times a cost stated by its owner — and the memory the operator
//! states. The process passes three phases, each holding its own and the
//! fixed parts: the stages' judging before T0, the gates' probes, the run
//! with its pool of steps and attempts. What a run needs is the largest; the
//! pool's budget is what the run's fixed parts leave, its prediction priced
//! at the run's catch-up.

use ascetic_ddd_scheduler::journal;

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
    /// The journal's output, as its host states it (K2b-7e-2b-3d-3a).
    JournalOut,
    /// A gate's probe's connection heap of its own, as its egress states it.
    ProbesIo,
    /// The steps not ended, beyond their cells as ended (K2b-7e-2b-3d-3a).
    Pending,
    /// The attempts' decorations, as their decorator states them.
    Decorations,
    /// Admission's evidence itself, its copies at once (K2b-7e-2b-3d-3b-4c).
    Evidence,
    /// The host's allocator probe, a read's bytes as its host states them
    /// (version 11, K2b-7e-2b-3e).
    Probe,
}

impl Part {
    /// Every part.
    pub const ALL: [Part; 27] = [
        Part::Reader,
        Part::Decoder,
        Part::Runtime,
        Part::CarrierSource,
        Part::Keeping,
        Part::Journal,
        Part::JournalOut,
        Part::Connections,
        Part::Io,
        Part::Answers,
        Part::Judging,
        Part::Probes,
        Part::ProbesIo,
        Part::GateStages,
        Part::GateRun,
        Part::Cells,
        Part::Pending,
        Part::Barriers,
        Part::Records,
        Part::BoundTo,
        Part::Kept,
        Part::Binding,
        Part::ShellAttempts,
        Part::CarrierAttempts,
        Part::Decorations,
        Part::Probe,
        Part::Evidence,
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
        /// Its proof of being built: none until the read gate mints one.
        built: Option<GateBuilt>,
    },
    /// The host's word.
    Host,
}

/// The read gate's proof of being built (decision 22, K2b-7e-2b-3d-3a): an
/// uninhabited witness until K2b-7e-3 gives the gate a type that mints it —
/// no part held by the gate is claimed held before, a test's build included.
///
/// ```
/// use ascetic_ddd_load_run::memory::Held;
/// let held = Held::Gate { built: None };
/// assert!(matches!(held, Held::Gate { built: None }));
/// ```
///
/// ```compile_fail,E0451
/// use ascetic_ddd_load_run::memory::{GateBuilt, Held};
/// let held = Held::Gate { built: Some(GateBuilt { witness: unreachable!() }) };
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GateBuilt {
    witness: std::convert::Infallible,
}

/// What a part's bytes are counted against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Basis {
    /// The counted heap the executors hold.
    Heap,
    /// Outside it: a library's own allocator.
    Outside,
}

/// The library's constants a part's cost is one of (decision 22,
/// K2b-7e-2b-3d-3a): taken by `fit` in `Planned` and journaled as the
/// evidence's, so a constant perturbed moves both.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Model {
    /// An ended step's cell.
    pub ended_cell: u64,
    /// A step not ended's cell.
    pub live_cell: u64,
    /// A barrier key beside its own bytes.
    pub barrier: u64,
    /// A record's `Arc`.
    pub record: u64,
    /// The log's decompressor.
    pub decoder: u64,
    /// The shell's share of an attempt.
    pub attempt: u64,
    /// A runtime's own beside its events.
    pub runtime: u64,
    /// A driver's event, where its size is stated.
    pub event: Option<u64>,
    /// A term of the evidence, as it is held (K2b-7e-2b-3d-3b-4c).
    pub term: u64,
    /// A row of the evidence: a source's sockets.
    pub row: u64,
    /// A part named in the evidence.
    pub part: u64,
}

impl Model {
    /// The library's, as built.
    pub const LIBRARY: Model = Model {
        ended_cell: ascetic_ddd_scheduler::fits::ENDED_CELL,
        live_cell: ascetic_ddd_scheduler::fits::LIVE_CELL,
        barrier: ascetic_ddd_scheduler::fits::BARRIER,
        record: ascetic_ddd_command_log::shape::ARC as u64,
        decoder: ascetic_ddd_command_log::live::DECODER,
        attempt: crate::run::ATTEMPT,
        runtime: crate::run::RUNTIME,
        event: crate::run::EVENT,
        term: size_of::<journal::MemoryTerm>() as u64,
        row: size_of::<journal::SourceFit>() as u64,
        part: size_of::<journal::Part>() as u64,
    };
}

/// A part's term: the things its enforcer bounds, what each holds and what
/// does not scale with them, its bytes, where they are held and who states
/// them (decision 22, K2b-7e-2b-3d-3a).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Term {
    /// The part.
    pub part: Part,
    /// Its source, if it is a source's.
    pub source: Option<u32>,
    /// Its phase.
    pub phase: Phase,
    /// The things its enforcer bounds: 0 for a part absent, 1 for one held
    /// once.
    pub count: u64,
    /// What each holds.
    pub cost: u64,
    /// What does not scale with them.
    pub rest: u64,
    /// `count · cost + rest`; `u64::MAX` past any count, refused.
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
    /// Its runtime's events a driver turn, as it will be built; none where a
    /// driver sizes none so many.
    pub events: Option<u64>,
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
    /// Who states the carrier's — its connections and answers alike.
    pub carrier_stated: StatedBy,
    /// The shell's keeping, one a source.
    pub keeping: u64,
}

/// What admission fixed of a run (decision 22): each part by its count, the
/// counts the pool's parts share held once, the costs that are constants
/// in its `Model`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Planned {
    /// Each source's.
    pub sources: Vec<SourcePart>,
    /// The reader's, at its records ahead; none without one.
    pub reader: Option<ascetic_ddd_scheduler::fits::Priced>,
    /// The journal's, at its lines behind.
    pub journal: ascetic_ddd_scheduler::fits::Priced,
    /// Its output's, as its host states it.
    pub journal_out: u64,
    /// Its probe's read, as its host states it; none without one.
    pub probe: u64,
    /// Stage 2's judging at the cap.
    pub judging: u64,
    /// The stand's gates' client.
    pub gates: GateBytes,
    /// The machine's counts.
    pub machine: ascetic_ddd_scheduler::fits::Counts,
    /// The carrier's cost an attempt late, and its parts beside.
    pub carrier_attempts: (u64, u64),
    /// The shell's parts of the attempts late beside its share each.
    pub shell_parts: u64,
    /// The decorations' cost an attempt, as their decorator states it.
    pub decorations: u64,
    /// Who states the carrier's attempts.
    pub carrier_stated: StatedBy,
    /// The constants.
    pub model: Model,
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
    /// The parts whose count no enforcer holds yet — the pool's, until the
    /// read gate is built.
    pub unheld: Vec<Part>,
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

/// The terms of `planned`, each part priced by its count, its phase, holder
/// and owner; a cost derived past any count is `u64::MAX`, refused.
#[must_use]
pub fn terms(planned: &Planned) -> Vec<Term> {
    use ascetic_ddd_scheduler::fits::Priced;
    let library = StatedBy::Library;
    let host = StatedBy::Host;
    let gate = Held::Gate { built: None };
    let term = |part, source, phase, priced: Option<Priced>, held, stated, basis| {
        let p = priced.unwrap_or(Priced {
            count: 1,
            cost: u64::MAX,
            rest: 0,
        });
        Term {
            part,
            source,
            phase,
            count: p.count,
            cost: p.cost,
            rest: p.rest,
            bytes: p.bytes().unwrap_or(u64::MAX),
            held,
            stated,
            basis,
        }
    };
    let priced = |count, cost, rest| Some(Priced { count, cost, rest });
    let once = |bytes| priced(1, bytes, 0);
    let m = planned.model;
    let c = planned.machine;
    let mut out = vec![
        term(
            Part::Reader,
            None,
            Phase::Fixed,
            Some(planned.reader.unwrap_or_default()),
            Held::Channel,
            library,
            Basis::Heap,
        ),
        term(
            Part::Decoder,
            None,
            Phase::Fixed,
            priced(u64::from(planned.reader.is_some()), m.decoder, 0),
            Held::Built,
            library,
            Basis::Outside,
        ),
    ];
    for (s, p) in (0u32..).zip(&planned.sources) {
        let source = Some(s);
        let runtime = p
            .events
            .zip(m.event)
            .and_then(|(e, cost)| priced(e, cost, m.runtime));
        out.extend([
            term(
                Part::Runtime,
                source,
                Phase::Fixed,
                runtime,
                Held::Built,
                library,
                Basis::Heap,
            ),
            term(
                Part::CarrierSource,
                source,
                Phase::Run,
                once(p.carrier),
                Held::Once,
                p.carrier_stated,
                Basis::Heap,
            ),
            term(
                Part::Keeping,
                source,
                Phase::Run,
                once(p.keeping),
                Held::Once,
                library,
                Basis::Heap,
            ),
            term(
                Part::Connections,
                source,
                Phase::Run,
                priced(p.cap, p.connection, 0),
                Held::Cap,
                p.carrier_stated,
                Basis::Heap,
            ),
            term(
                Part::Io,
                source,
                Phase::Run,
                priced(p.cap, p.io, 0),
                Held::Cap,
                host,
                Basis::Heap,
            ),
            term(
                Part::Answers,
                source,
                Phase::Run,
                priced(p.cap, p.answer, 0),
                Held::Cap,
                p.carrier_stated,
                Basis::Heap,
            ),
            term(
                Part::Probes,
                source,
                Phase::Gates,
                priced(p.cap, p.connection, 0),
                Held::Cap,
                p.carrier_stated,
                Basis::Heap,
            ),
            term(
                Part::ProbesIo,
                source,
                Phase::Gates,
                priced(p.cap, p.io, 0),
                Held::Cap,
                host,
                Basis::Heap,
            ),
        ]);
    }
    out.extend([
        term(
            Part::Journal,
            None,
            Phase::Run,
            Some(planned.journal),
            Held::Channel,
            library,
            Basis::Heap,
        ),
        term(
            Part::JournalOut,
            None,
            Phase::Run,
            once(planned.journal_out),
            Held::Once,
            host,
            Basis::Heap,
        ),
        term(
            Part::Judging,
            None,
            Phase::Stages,
            once(planned.judging),
            Held::Once,
            library,
            Basis::Heap,
        ),
        term(
            Part::GateStages,
            None,
            Phase::Gates,
            once(planned.gates.stages),
            Held::Host,
            host,
            Basis::Heap,
        ),
        term(
            Part::GateRun,
            None,
            Phase::Run,
            once(planned.gates.run),
            Held::Host,
            host,
            Basis::Heap,
        ),
        term(
            Part::Cells,
            None,
            Phase::Pool,
            priced(c.held, m.ended_cell, 0),
            gate,
            library,
            Basis::Heap,
        ),
        term(
            Part::Pending,
            None,
            Phase::Pool,
            m.live_cell
                .checked_sub(m.ended_cell)
                .and_then(|cost| priced(c.pending, cost, 0)),
            gate,
            library,
            Basis::Heap,
        ),
        term(
            Part::Barriers,
            None,
            Phase::Pool,
            priced(c.barriers, m.barrier, c.barrier_bytes),
            Held::Once,
            library,
            Basis::Heap,
        ),
        term(
            Part::Records,
            None,
            Phase::Pool,
            priced(c.late, m.record, c.late_parts),
            gate,
            library,
            Basis::Heap,
        ),
        term(
            Part::BoundTo,
            None,
            Phase::Pool,
            priced(1, 0, c.bound_to),
            gate,
            library,
            Basis::Heap,
        ),
        term(
            Part::Kept,
            None,
            Phase::Pool,
            priced(1, 0, c.keeping),
            gate,
            library,
            Basis::Heap,
        ),
        term(
            Part::Binding,
            None,
            Phase::Pool,
            priced(1, 0, c.binding),
            Held::Once,
            library,
            Basis::Heap,
        ),
        term(
            Part::ShellAttempts,
            None,
            Phase::Pool,
            priced(c.late, m.attempt, planned.shell_parts),
            gate,
            library,
            Basis::Heap,
        ),
        term(
            Part::CarrierAttempts,
            None,
            Phase::Pool,
            priced(
                c.late,
                planned.carrier_attempts.0,
                planned.carrier_attempts.1,
            ),
            gate,
            planned.carrier_stated,
            Basis::Heap,
        ),
        term(
            Part::Decorations,
            None,
            Phase::Pool,
            priced(c.late, planned.decorations, 0),
            gate,
            host,
            Basis::Heap,
        ),
        term(
            Part::Probe,
            None,
            Phase::Run,
            once(planned.probe),
            Held::Once,
            host,
            Basis::Heap,
        ),
    ]);
    // The evidence itself (K2b-7e-2b-3d-3b-4c): two copies at once —
    // admission's and the header's while it is written —, each every term
    // (this one with them), a row a source and the unheld parts.
    let unheld = out
        .iter()
        .filter(|t| matches!(t.held, Held::Gate { .. }))
        .count();
    let of = |n: usize| u64::try_from(n).ok();
    let copy = of(out.len().saturating_add(1))
        .and_then(|n| n.checked_mul(m.term))
        .zip(of(planned.sources.len()).and_then(|n| n.checked_mul(m.row)))
        .zip(of(unheld).and_then(|n| n.checked_mul(m.part)))
        .and_then(|((terms, rows), parts)| terms.checked_add(rows)?.checked_add(parts));
    out.push(term(
        Part::Evidence,
        None,
        Phase::Fixed,
        copy.and_then(|cost| priced(2, cost, 0)),
        Held::Built,
        library,
        Basis::Heap,
    ));
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
    // A runtime whose events, or an event's size, nothing states.
    if planned.model.event.is_none() || planned.sources.iter().any(|s| s.events.is_none()) {
        return Err(unfit(terms, Failed::Unstated(Part::Runtime)));
    }
    // A count times its cost past any count saturates, and so does a cost or
    // a rest derived past it, whatever its count (rule 1: a saturated cost
    // times 0 is no term a memory admits): refused, never a term that a
    // memory stated at its most would admit.
    if let Some(t) = terms
        .iter()
        .find(|t| [t.bytes, t.cost, t.rest].contains(&u64::MAX))
    {
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
    let unheld = unheld(&terms);
    Ok(Fit {
        terms,
        need,
        slack,
        budget,
        pool: predicted,
        unheld,
    })
}

/// The parts of `terms` held by a gate not built, each once, in their order.
#[must_use]
pub fn unheld(terms: &[Term]) -> Vec<Part> {
    let mut parts: Vec<Part> = Vec::new();
    for t in terms {
        if matches!(t.held, Held::Gate { built: None }) && !parts.contains(&t.part) {
            parts.push(t.part);
        }
    }
    parts
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

/// A remedy for a run that does not fit (decision 22, K2b-7e-2b-3c-2): each
/// its owner's knob, its number found by bisection over the same fit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Remedy {
    /// The operator's: at least this memory.
    Memory {
        /// Bytes.
        at_least: u64,
    },
    /// The carrier's egress: at most these ports, as it divides them.
    Ports {
        /// Ports.
        at_most: usize,
    },
    /// The host's reader: at most these records ahead.
    Ahead {
        /// Records.
        at_most: usize,
    },
    /// The host's journal: at most these lines behind.
    Behind {
        /// Lines.
        at_most: usize,
    },
    /// The plan's author: an answer cap at most this — a new dry run, the
    /// largest answer by operation owed.
    AnswerCap {
        /// Bytes.
        at_most: u64,
    },
    /// A slice that would lower the binding part: named, not a number.
    Slice {
        /// The part.
        part: Part,
        /// The slice.
        slice: &'static str,
    },
}

/// The knobs a run's parts follow: the egress's ports, the reader's records
/// ahead, the journal's lines behind, the plan's answer cap.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Knobs {
    /// The ports the egress divides among its sources.
    pub ports: usize,
    /// The reader's records ahead.
    pub ahead: usize,
    /// The journal's lines behind.
    pub behind: usize,
    /// The plan's answer cap.
    pub answer_cap: u64,
}

/// The remedies for a run of `planned(knobs)` short of `memory`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Remedies {
    /// The least memory at the knobs given.
    pub least: Option<u64>,
    /// The least memory with each source's cap the plan's need of it: the
    /// knobs at their floors' ports.
    pub least_at_needed: Option<u64>,
    /// Each knob's, where moving it alone admits the run.
    pub remedies: Vec<Remedy>,
}

/// The largest `v` in `[lo, hi]` with `fits(v)`, `fits` monotone — true up to
/// some value, false past it; none if `fits(lo)` is false.
fn largest(lo: u64, hi: u64, fits: impl Fn(u64) -> bool) -> Option<u64> {
    if lo > hi || !fits(lo) {
        return None;
    }
    if fits(hi) {
        return Some(hi);
    }
    // `fits(ok)`, not `fits(bad)`, `ok < bad`.
    let (mut ok, mut bad) = (lo, hi);
    while bad.saturating_sub(ok) > 1 {
        let mid = ok.saturating_add(bad.saturating_sub(ok) / 2);
        if fits(mid) {
            ok = mid;
        } else {
            bad = mid;
        }
    }
    Some(ok)
}

/// The slice that would lower `part`, if any.
fn slice(part: Part) -> Option<&'static str> {
    match part {
        Part::Cells
        | Part::Pending
        | Part::Records
        | Part::BoundTo
        | Part::Kept
        | Part::ShellAttempts
        | Part::CarrierAttempts
        | Part::Decorations => Some("K2b-7e-3, the read gate"),
        // The parse is the tape's since K2b-7e-4: no slice lowers it further;
        // its remedy is the plan's answer cap.
        Part::CarrierSource
        | Part::Judging
        | Part::JournalOut
        | Part::ProbesIo
        | Part::Reader
        | Part::Decoder
        | Part::Runtime
        | Part::Keeping
        | Part::Journal
        | Part::Connections
        | Part::Io
        | Part::Answers
        | Part::Probes
        | Part::GateStages
        | Part::GateRun
        | Part::Barriers
        | Part::Binding
        | Part::Evidence
        | Part::Probe => None,
    }
}

/// The remedies for a run of `planned(knobs)` short of `memory`, each knob
/// moved alone between its floor and its value (decision 22,
/// K2b-7e-2b-3c-2): its number the largest that admits the run, by bisection
/// over the same fit — monotone in every knob above its floor, the ports'
/// floor the least that gives each source the sockets the plan needs of it.
pub fn remedies(
    memory: Memory,
    knobs: Knobs,
    floors: Knobs,
    planned: impl Fn(&Knobs) -> Planned,
) -> Remedies {
    let at = |k: &Knobs| fit(&planned(k), memory);
    let need = |k: &Knobs| match fit(&planned(k), Memory::declared(u64::MAX)) {
        Ok(f) => Some(f.need),
        Err(u) => u.least,
    };
    let mut remedies = Vec::new();
    let least = need(&knobs);
    if let Some(at_least) = least {
        remedies.push(Remedy::Memory { at_least });
    }
    let usize_of = |v: u64| usize::try_from(v).unwrap_or(usize::MAX);
    let u64_of = |v: usize| u64::try_from(v).unwrap_or(u64::MAX);
    if let Some(p) = largest(u64_of(floors.ports), u64_of(knobs.ports), |v| {
        at(&Knobs {
            ports: usize_of(v),
            ..knobs
        })
        .is_ok()
    }) {
        remedies.push(Remedy::Ports {
            at_most: usize_of(p),
        });
    }
    if let Some(a) = largest(u64_of(floors.ahead), u64_of(knobs.ahead), |v| {
        at(&Knobs {
            ahead: usize_of(v),
            ..knobs
        })
        .is_ok()
    }) {
        remedies.push(Remedy::Ahead {
            at_most: usize_of(a),
        });
    }
    if let Some(b) = largest(u64_of(floors.behind), u64_of(knobs.behind), |v| {
        at(&Knobs {
            behind: usize_of(v),
            ..knobs
        })
        .is_ok()
    }) {
        remedies.push(Remedy::Behind {
            at_most: usize_of(b),
        });
    }
    if let Some(c) = largest(floors.answer_cap, knobs.answer_cap, |v| {
        at(&Knobs {
            answer_cap: v,
            ..knobs
        })
        .is_ok()
    }) {
        remedies.push(Remedy::AnswerCap { at_most: c });
    }
    if let Err(u) = at(&knobs)
        && let Some(b) = u.binding
        && let Some(part) = u.terms.get(b).map(|t| t.part)
        && let Some(slice) = slice(part)
    {
        remedies.push(Remedy::Slice { part, slice });
    }
    Remedies {
        least,
        least_at_needed: need(&Knobs {
            ports: floors.ports,
            ..knobs
        }),
        remedies,
    }
}

/// A run refused on its memory (decision 22, K2b-7e-2b-3d): the memory
/// stated, why the run does not fit and each remedy found for it. Its least
/// memory is predicted with the pool's count unheld until the read gate
/// (K2b-7e-3): not a safe number while the counted heap's factor against the
/// process's resident memory is unmeasured (K2b-7e-2b-3e).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refused {
    /// The memory stated.
    pub memory: Memory,
    /// Why it does not fit.
    pub unfit: Unfit,
    /// The remedies.
    pub remedies: Remedies,
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let failed = match self.unfit.failed {
            Failed::Memory => "a phase needs more".to_owned(),
            Failed::Pool => "the pool's prediction past its budget".to_owned(),
            Failed::Overflow(part) => format!("{part:?} past any count"),
            Failed::Unstated(part) => format!("{part:?} stated by no owner"),
        };
        write!(f, "the stated memory {} B: {failed}", self.memory.bytes())?;
        if let Some(t) = self.unfit.binding.and_then(|i| self.unfit.terms.get(i)) {
            match t.source {
                Some(s) => write!(f, ", binding {:?} of source {s} at {} B", t.part, t.bytes)?,
                None => write!(f, ", binding {:?} at {} B", t.part, t.bytes)?,
            }
        }
        if let Some(least) = self.unfit.least {
            write!(f, ", the least {least} B (predicted, the pool unheld)")?;
        }
        let remedies: Vec<String> = self
            .remedies
            .remedies
            .iter()
            .map(|r| match r {
                Remedy::Memory { at_least } => format!("memory at least {at_least} B"),
                Remedy::Ports { at_most } => format!("ports at most {at_most}"),
                Remedy::Ahead { at_most } => format!("records ahead at most {at_most}"),
                Remedy::Behind { at_most } => format!("lines behind at most {at_most}"),
                Remedy::AnswerCap { at_most } => format!("an answer cap at most {at_most} B"),
                Remedy::Slice { part, slice } => format!("{part:?} lowered by {slice}"),
            })
            .collect();
        if !remedies.is_empty() {
            write!(f, "; remedies: {}", remedies.join(", "))?;
        }
        Ok(())
    }
}

// The journal's evidence of the fit (version 10, K2b-7e-2b-3d-3b): each of
// the fit's own, by name, every variant matched — a part added fails to
// compile here until the journal names it.

impl From<Part> for journal::Part {
    fn from(p: Part) -> journal::Part {
        match p {
            Part::Reader => journal::Part::Reader,
            Part::Decoder => journal::Part::Decoder,
            Part::Runtime => journal::Part::Runtime,
            Part::CarrierSource => journal::Part::CarrierSource,
            Part::Keeping => journal::Part::Keeping,
            Part::Journal => journal::Part::Journal,
            Part::JournalOut => journal::Part::JournalOut,
            Part::Connections => journal::Part::Connections,
            Part::Io => journal::Part::Io,
            Part::Answers => journal::Part::Answers,
            Part::Judging => journal::Part::Judging,
            Part::Probes => journal::Part::Probes,
            Part::ProbesIo => journal::Part::ProbesIo,
            Part::GateStages => journal::Part::GateStages,
            Part::GateRun => journal::Part::GateRun,
            Part::Cells => journal::Part::Cells,
            Part::Pending => journal::Part::Pending,
            Part::Barriers => journal::Part::Barriers,
            Part::Records => journal::Part::Records,
            Part::BoundTo => journal::Part::BoundTo,
            Part::Kept => journal::Part::Kept,
            Part::Binding => journal::Part::Binding,
            Part::ShellAttempts => journal::Part::ShellAttempts,
            Part::CarrierAttempts => journal::Part::CarrierAttempts,
            Part::Decorations => journal::Part::Decorations,
            Part::Evidence => journal::Part::Evidence,
            Part::Probe => journal::Part::Probe,
        }
    }
}

impl From<Phase> for journal::Phase {
    fn from(p: Phase) -> journal::Phase {
        match p {
            Phase::Fixed => journal::Phase::Fixed,
            Phase::Stages => journal::Phase::Stages,
            Phase::Gates => journal::Phase::Gates,
            Phase::Run => journal::Phase::Run,
            Phase::Pool => journal::Phase::Pool,
        }
    }
}

impl From<Held> for journal::Held {
    fn from(h: Held) -> journal::Held {
        match h {
            Held::Built => journal::Held::Built,
            Held::Channel => journal::Held::Channel,
            Held::Cap => journal::Held::Cap,
            Held::Once => journal::Held::Once,
            Held::Gate { built } => journal::Held::Gate {
                built: built.is_some(),
            },
            Held::Host => journal::Held::Host,
        }
    }
}

impl From<Basis> for journal::Basis {
    fn from(b: Basis) -> journal::Basis {
        match b {
            Basis::Heap => journal::Basis::Heap,
            Basis::Outside => journal::Basis::Outside,
        }
    }
}

impl From<Model> for journal::Model {
    fn from(m: Model) -> journal::Model {
        let Model {
            ended_cell,
            live_cell,
            barrier,
            record,
            decoder,
            attempt,
            runtime,
            event,
            term,
            row,
            part,
        } = m;
        journal::Model {
            ended_cell,
            live_cell,
            barrier,
            record,
            decoder,
            attempt,
            runtime,
            event,
            term,
            row,
            part,
        }
    }
}

impl From<&Term> for journal::MemoryTerm {
    fn from(t: &Term) -> journal::MemoryTerm {
        let Term {
            part,
            source,
            phase,
            count,
            cost,
            rest,
            bytes,
            held,
            stated,
            basis,
        } = *t;
        journal::MemoryTerm {
            part: part.into(),
            source,
            phase: phase.into(),
            count,
            cost,
            rest,
            bytes,
            held: held.into(),
            stated: stated.into(),
            basis: basis.into(),
        }
    }
}
