//! The machine's loop (ADR-0047 decision 16): `sim`'s, with time read from the
//! clock and attempts carried by tasks.
//!
//! An instant at a time: the lines due by then read (by the machine's own
//! timer, `r.at − L`, not by its `Want`: a run whose next line is more than L
//! after its last end would otherwise wait for nothing), the wakes due taken,
//! the instant settled and drained from the frontier, and its inputs stepped
//! one per settle — an input caused at an instant may sort below one already
//! queued there, as `sim` takes it. Between instants it waits on the earliest
//! of its own work, the frontier's, the next record, and a push.

use std::collections::BTreeMap;
use std::fmt;
use std::future::poll_fn;
use std::marker::PhantomData;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use ascetic_ddd_command_log::LogError;
use ascetic_ddd_command_log::fills::Namespace;
use ascetic_ddd_command_log::record::{Action, Header, Probe, Record};
use ascetic_ddd_scheduler::journal::{self, FaultClass, JournalError, LogFault};
use ascetic_ddd_scheduler::machine::{Input, Line, Machine, Output};
use ascetic_ddd_scheduler::order::Ordered;
use tokio::runtime::Handle;

use crate::carrier::{Carrier, Needs};
use crate::clock::Clock;
use crate::live::Descriptors;
use crate::sink::JournalSink;
use crate::source::{LineSource, Plan};
use crate::stamps::{Attempt, Harness, Stamps};

/// Where attempts run: a runtime handle per source, in the order of the
/// profile's sources, and the I/O events each driver takes a turn (tokio's
/// own setting cannot be read back from a handle). Made by `Live`; in tests,
/// one paused runtime behind every handle, its events assumed.
#[derive(Clone, Debug)]
pub struct Runtimes {
    sources: Vec<Handle>,
    events: Vec<usize>,
    descriptors: Option<Descriptors>,
    /// Made by a test, not read.
    assumed: bool,
}

impl Runtimes {
    /// The runtimes built: `assumed` if their events are a test's choice,
    /// not a fit's — exempt by name from being the runtimes fitted.
    pub(crate) fn new(
        sources: Vec<Handle>,
        events: Vec<usize>,
        descriptors: Option<Descriptors>,
        assumed: bool,
    ) -> Runtimes {
        Runtimes {
            sources,
            events,
            descriptors,
            assumed,
        }
    }

    /// A test's runtimes: `sources`, each taking `events` a driver turn; the
    /// process's descriptors unbounded.
    #[cfg(feature = "testing")]
    pub fn assumed(sources: Vec<Handle>, events: usize) -> Runtimes {
        let n = sources.len();
        Runtimes {
            sources,
            events: vec![events; n],
            descriptors: Some(Descriptors::assumed(u64::MAX, u64::MAX, 0)),
            assumed: true,
        }
    }

    /// The same, the process's descriptors `descriptors` — none, unknown.
    #[cfg(feature = "testing")]
    pub fn with_descriptors(self, descriptors: Option<Descriptors>) -> Runtimes {
        Runtimes {
            descriptors,
            ..self
        }
    }

    /// The process's descriptors, read once the runtimes existed; none if
    /// they could not be.
    pub fn descriptors(&self) -> Option<Descriptors> {
        self.descriptors
    }

    /// By source.
    pub fn sources(&self) -> &[Handle] {
        &self.sources
    }

    /// The I/O events source `s`'s driver takes a turn.
    pub fn events(&self, s: u32) -> Option<usize> {
        usize::try_from(s)
            .ok()
            .and_then(|s| self.events.get(s).copied())
    }

    /// The bytes runtime `s` holds of its own (ADR-0047 decision 22,
    /// K2b-7e-2b-2c-1): [`RUNTIME`], and its driver's events at [`EVENT`]
    /// each, as built — charged for every runtime built, whatever the plan's
    /// sources. None for a test's runtimes, or where no event's size is
    /// stated.
    pub fn own(&self, s: u32) -> Option<u64> {
        if self.assumed {
            return None;
        }
        runtime_own(self.events(s)?)
    }
}

/// The bytes the shell holds of an answer's captures as it keeps them
/// (ADR-0047 decision 22, K2b-7e-2b-2c-3): one answer a source at a time,
/// whatever carries it — their slots, and each spec's subject and slot
/// copied; their values moved, not copied. At most `record`'s shape: the
/// plan's largest.
pub fn ending(record: &ascetic_ddd_command_log::shape::Shape) -> u64 {
    let slot =
        u64::try_from(size_of::<ascetic_ddd_scheduler::machine::Captured>()).unwrap_or(u64::MAX);
    record
        .captures
        .elements
        .saturating_mul(slot)
        .saturating_add(record.capture_bytes)
}

/// The bytes a source's runtime holds of its own, its events aside (ADR-0047
/// decision 22, K2b-7e-2b-2c-1): a `current_thread` runtime with I/O, its
/// thread and its stop, its places in `Live` — measured at build by
/// `tests/sources.rs` (2,547 B, alike at 1 and 9 sources; the margin a
/// thread's name of more digits). A look's and a socket's own on its thread
/// are the attempts' and the connections'.
pub const RUNTIME: u64 = 2_600;

/// The bytes a driver's event takes: libc's `epoll_event`, packed on x86_64
/// and on x86 with glibc, 12 B; 16 B on Linux elsewhere; none on another
/// system, where no executor states it (ADR-0047 decision 22, K2b-7e-2b-2c-1).
#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", all(target_arch = "x86", target_env = "gnu"))
))]
pub const EVENT: Option<u64> = Some(12);
/// See the x86 variant.
#[cfg(all(
    target_os = "linux",
    not(any(target_arch = "x86_64", all(target_arch = "x86", target_env = "gnu")))
))]
pub const EVENT: Option<u64> = Some(16);
/// See the x86 variant.
#[cfg(not(target_os = "linux"))]
pub const EVENT: Option<u64> = None;

/// Descriptors kept for what is opened after they are read (decision 20):
/// the journal's file, a gate's client, the host's own.
pub const DESCRIPTORS_RESERVE: u64 = 16;

/// The bytes an attempt holds at most beside the machine's cell and its
/// carrier's parts (ADR-0047 decision 22): its task's own, its stamps, its
/// inputs in flight — the shell's share, whatever carries it; held two-sided
/// by `tests/costs.rs` against a carrier that holds nothing of its own.
pub const ATTEMPT: u64 = 1_536;

/// A captured hole's value as an attempt holds it (ADR-0047 decision 22,
/// K2b-7e-2b-3a-3): the shell's copy the machine dispatches it with, at most
/// [`VALUE`](ascetic_ddd_scheduler::fits::VALUE) bytes, and its entry.
pub const CAPTURED: u64 = ascetic_ddd_scheduler::fits::VALUE
    .saturating_add(size_of::<ascetic_ddd_command_log::fills::Captured>() as u64);

/// The bytes the shell holds of the attempts of records summed into `shape`
/// (decision 22, K2b-7e-2b-3a-3), whatever carries them: its share
/// [`ATTEMPT`] each, and its copy of each captured hole's value — at most
/// [`CAPTURED`] with its subject's and slot's bytes. The carrier's beside it
/// (`Carrier::attempts`).
#[must_use]
pub fn attempt_share(shape: &ascetic_ddd_command_log::shape::Shape) -> u64 {
    ATTEMPT
        .saturating_mul(shape.records)
        .saturating_add(CAPTURED.saturating_mul(shape.holes))
        .saturating_add(shape.hole_bytes)
}

/// The I/O events a source's driver spends a turn on its own (decision 20):
/// its waker, and its signal receiver when tokio's `signal` feature is
/// compiled in — an upper bound.
pub const DRIVER_EVENTS: usize = 2;

/// Why a run is not admitted (decision 20): each number its own.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Inadmissible {
    /// The carrier's own reason.
    Carrier(String),
    /// A source's runtime not `current_thread`.
    Flavour {
        /// The source.
        source: usize,
    },
    /// Fewer runtimes than sources.
    Runtimes {
        /// Runtimes given.
        have: usize,
        /// The profile's sources.
        sources: u32,
    },
    /// More sockets needed at once under the plan's bound than the source's
    /// cap — the plan's bound over all its sources together.
    Sockets {
        /// The source.
        source: u32,
        /// Needed at once.
        needed: u64,
        /// Its cap.
        cap: usize,
    },
    /// The sources' caps with the descriptors open and the reserve past the
    /// process's soft limit — or the descriptors unknown.
    Descriptors {
        /// The caps summed, the open and the reserve.
        needed: u64,
        /// The process's, if read.
        descriptors: Option<Descriptors>,
    },
    /// A source's cap with its driver's own past the events a turn takes:
    /// a look's turn could miss a ready socket.
    Events {
        /// The source.
        source: u32,
        /// Its cap.
        cap: usize,
        /// The events a turn, if known.
        events: Option<usize>,
    },
    /// The lines run from a reader holding more records ahead than the one
    /// its plan was admitted with (decision 22): what admission charged is
    /// not what runs.
    Reader {
        /// The records ahead the plan's reader holds.
        admitted: usize,
        /// The source's.
        ahead: usize,
    },
    /// The journal's output larger inline than its thread is stated for
    /// (ADR-0047 decision 22, K2b-7e-2b-2b): its host boxes it.
    Output {
        /// Its bytes inline.
        size: usize,
        /// The most [`sink::THREAD`](crate::sink::THREAD) states.
        most: usize,
    },
    /// A run that does not fit the memory stated (decision 22, K2b-7e-2b-3d):
    /// its terms, what failed, the term that binds, the least memory, each
    /// remedy on its owner's knob.
    Memory(Box<crate::memory::Refused>),
    /// A part whose bytes no owner states (decision 22): refused, never left
    /// no room.
    Unstated {
        /// The part.
        part: &'static str,
    },
    /// A source's runtime as built not the one fitted (decision 22,
    /// K2b-7e-2b-3d): what admission priced is not what runs.
    Runtime {
        /// The source.
        source: u32,
        /// Its runtime's own as built.
        built: Option<u64>,
        /// As fitted.
        fitted: Option<u64>,
    },
    /// The memory stated past what the process's cgroup tree allows
    /// (decision 22, K2b-7e-2b-3d-2): the kernel would reclaim or kill first.
    Limit {
        /// The memory stated.
        stated: u64,
        /// The least limit over the tree.
        limit: u64,
        /// Which: `memory.max` or `memory.high`.
        file: &'static str,
        /// Its depth: 0 the process's own cgroup.
        depth: usize,
    },
    /// A run with a count no enforcer holds — its pool, until the read gate
    /// (K2b-7e-3) — outside a scope of its own with a finite `memory.max`
    /// (decision 22, K2b-7e-2b-3d-2): the ADR's own first host run inside a
    /// scope with `MemoryMax`, executed.
    Scope {
        /// The least `memory.max` over the tree and its depth, if any.
        max: Option<(u64, usize)>,
    },
    /// Every refusal at once (decision 22): raising one limit to be refused
    /// on the next is wasted work.
    All(Vec<Inadmissible>),
}

impl Inadmissible {
    /// `refusals` as one: itself if there is one, all of them if more — each
    /// a single refusal, as `fit` and `admit` push them.
    fn of(mut refusals: Vec<Inadmissible>) -> Option<Inadmissible> {
        match refusals.len() {
            0 => None,
            1 => refusals.pop(),
            _ => Some(Inadmissible::All(refusals)),
        }
    }

    /// Each refusal it holds.
    pub fn each(&self) -> &[Inadmissible] {
        match self {
            Inadmissible::All(all) => all,
            one => std::slice::from_ref(one),
        }
    }
}

impl fmt::Display for Inadmissible {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Inadmissible::Carrier(why) => write!(f, "{why}"),
            Inadmissible::Flavour { source } => {
                write!(f, "source {source}'s runtime is not current_thread")
            }
            Inadmissible::Runtimes { have, sources } => {
                write!(f, "{have} runtimes for {sources} sources")
            }
            Inadmissible::Sockets {
                source,
                needed,
                cap,
            } => write!(
                f,
                "source {source} may hold {needed} sockets at once under the plan's bound (its \
                 pending steps, over all sources), its cap {cap}"
            ),
            Inadmissible::Descriptors {
                needed,
                descriptors: Some(d),
            } => write!(
                f,
                "{needed} descriptors needed (the caps, {} open, {DESCRIPTORS_RESERVE} kept), \
                 the soft limit {} of a hard {}{}",
                d.open(),
                d.soft(),
                d.hard(),
                if d.raised() {
                    ", raised"
                } else {
                    " — `Descriptors::raise` lifts it"
                }
            ),
            Inadmissible::Descriptors {
                needed,
                descriptors: None,
            } => write!(f, "{needed} descriptors needed, the process's unknown"),
            Inadmissible::Events {
                source,
                cap,
                events,
            } => match events {
                Some(e) => write!(
                    f,
                    "source {source}'s cap {cap} and its driver's {DRIVER_EVENTS} past the {e} \
                     events a turn"
                ),
                None => write!(f, "source {source}'s events a turn unknown, its cap {cap}"),
            },
            Inadmissible::Reader { admitted, ahead } => write!(
                f,
                "the lines read {ahead} records ahead, their plan admitted with {admitted}"
            ),
            Inadmissible::Memory(refused) => write!(f, "{refused}"),
            Inadmissible::Unstated { part } => write!(f, "{part}: its bytes stated by no owner"),
            Inadmissible::Runtime {
                source,
                built,
                fitted,
            } => write!(
                f,
                "source {source}'s runtime holds {built:?} B as built, {fitted:?} B as fitted: \
                 build it by `Live::fitted`"
            ),
            Inadmissible::Limit {
                stated,
                limit,
                file,
                depth,
            } => write!(
                f,
                "the stated memory {stated} B past the {file} of {limit} B {depth} cgroups up"
            ),
            Inadmissible::Scope { max } => {
                let held = match max {
                    Some((bytes, depth)) => {
                        format!("its least memory.max {bytes} B {depth} cgroups up")
                    }
                    None => "no memory.max".to_owned(),
                };
                write!(
                    f,
                    "its pool's count held by no enforcer until the read gate, and {held}: run it \
                     inside a scope of its own — `systemd-run --user --scope -p MemoryMax=<bytes>`"
                )
            }
            Inadmissible::All(all) => {
                let each: Vec<String> = all.iter().map(ToString::to_string).collect();
                write!(f, "{}", each.join("; "))
            }
            Inadmissible::Output { size, most } => write!(
                f,
                "the journal's output holds {size} bytes inline, at most {most}: box it"
            ),
        }
    }
}

/// A run that left no journal to fold: refused before it began, or its
/// journal's own sink failed (a failed write may leave a line cut short, so
/// nothing may follow it).
#[derive(Debug)]
pub enum RunError {
    /// Refused before T0.
    Inadmissible(Inadmissible),
    /// A measurement before T0 failed: the harness's.
    Measure(String),
    /// T0's clock would not start.
    Clock(crate::clock::ClockError),
    /// The log, refused before T0.
    Log(LogError),
    /// The journal's sink.
    Journal(JournalError),
}

impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RunError::Inadmissible(why) => write!(f, "refused before T0: {why}"),
            RunError::Measure(why) => write!(f, "not measured before T0: {why}"),
            RunError::Clock(e) => write!(f, "{e}"),
            RunError::Log(e) => write!(f, "{e}"),
            RunError::Journal(e) => write!(f, "{e}"),
        }
    }
}

/// Why a run stopped short (decision 18): its class journaled, the whole —
/// a host's text included — told here, for the caller only.
#[derive(Debug)]
pub enum Stop {
    /// The harness.
    Harness(Harness),
    /// The log, or the machine on it.
    Log(LogError),
}

impl Stop {
    /// Its class in the journal.
    pub fn class(&self) -> FaultClass {
        match self {
            Stop::Harness(h) => h.class(),
            Stop::Log(e) => FaultClass::Log(log_fault(e)),
        }
    }
}

impl fmt::Display for Stop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Stop::Harness(h) => write!(f, "harness fault: {h}"),
            Stop::Log(e) => write!(f, "{e}"),
        }
    }
}

/// How the log failed the run, as the journal holds it.
fn log_fault(e: &LogError) -> LogFault {
    match e {
        LogError::Diverged { .. } => LogFault::Diverged,
        LogError::Truncated => LogFault::Truncated,
        LogError::Io(_) | LogError::Space { .. } | LogError::Hot { .. } => LogFault::Io,
        LogError::Encode(_)
        | LogError::Malformed { .. }
        | LogError::NotCanonical { .. }
        | LogError::Header { .. }
        | LogError::Format(_)
        | LogError::Trailer { .. }
        | LogError::AfterTrailer => LogFault::Malformed,
    }
}

/// How a run ended: through its log, or stopped by a fault — its journal
/// whole either way, and its verdict the fold's.
#[derive(Debug)]
pub enum RunEnd {
    /// Every line ended.
    Ended,
    /// Stopped: the journal's last line its fault.
    Stopped(Stop),
}

/// What halts the loop: a stop, journaled, or the sink, which cannot be.
enum Halt {
    Stop(Stop),
    Journal(JournalError),
}

impl From<Harness> for Halt {
    fn from(h: Harness) -> Halt {
        Halt::Stop(Stop::Harness(h))
    }
}

impl From<LogError> for Halt {
    fn from(e: LogError) -> Halt {
        Halt::Stop(Stop::Log(e))
    }
}

impl From<JournalError> for Halt {
    fn from(e: JournalError) -> Halt {
        Halt::Journal(e)
    }
}

/// Aborts the run's tasks when the run goes — ended, stopped, or dropped
/// (decision 18).
struct Aborts<C: Clock>(Arc<Stamps<C>>);

impl<C: Clock> Drop for Aborts<C> {
    fn drop(&mut self) {
        self.0.tasks().abort();
    }
}

/// Microseconds a stop waits for its aborted tasks to end: one stuck in its
/// judging never does.
const QUIESCE: u64 = 1_000_000;

impl std::error::Error for RunError {}

impl From<LogError> for RunError {
    fn from(e: LogError) -> RunError {
        RunError::Log(e)
    }
}

impl From<JournalError> for RunError {
    fn from(e: JournalError) -> RunError {
        RunError::Journal(e)
    }
}

/// A run's end: what its journal left, the machine's last counts, and how it
/// ended.
#[derive(Debug)]
pub struct Ran<F> {
    /// The journal's sink, finished.
    pub journal: F,
    /// Cells the machine still held.
    pub held: usize,
    /// Cells kept for their dependents.
    pub kept: usize,
    /// Ended, or stopped and why.
    pub end: RunEnd,
    /// The run's tasks still running: after a stop, those its wait did not
    /// see end — one stuck in its judging.
    pub running: usize,
    /// The closing reports journaled, by their code (decision 20): where
    /// attempts never sent stood — `rest.cap.full`, say — a code no carrier
    /// named as `other`; at most [`UNSENT`] codes (ADR-0047 decision 22,
    /// K2b-7e-2b-2b-3), a carrier minting more counted in `unsent_past`.
    pub unsent: BTreeMap<String, u64>,
    /// The closing reports whose codes came past the first [`UNSENT`]: a
    /// carrier's own vocabulary is a few; their codes are in the journal.
    pub unsent_past: u64,
}

/// The most codes [`Ran::unsent`] names: the library's carrier has eight, and
/// a carrier naming a code an attempt would grow it with the run.
pub const UNSENT: usize = 32;

/// Turns at one instant with work due and none done, past which the loop is
/// stuck: far more than a race at a microsecond makes, far fewer than a
/// test's patience.
const STALLS: u32 = 1_000;

/// The source an action is sent from.
fn source_of(record: &Record) -> u32 {
    match &record.action {
        Action::Rest(r) => r.source,
        Action::Observe(o) => match &o.probe {
            Probe::Rest(r) => r.source,
            Probe::Bus { .. } => 0,
        },
        Action::Bus(_) | Action::Host(_) => 0,
    }
}

/// The record ready now, if any; `Pending` if the source has none yet.
fn poll_now<S: LineSource>(source: &mut S) -> Poll<Result<Option<Arc<Record>>, LogError>> {
    source.poll_next(&mut Context::from_waker(Waker::noop()))
}

/// A run admitted before T0 (decision 18, the first stage): the plan, the
/// carrier that carries it and the runtimes it runs on, checked against each
/// other with nothing touched outside the process. Only [`admit`] makes one,
/// and only one runs.
pub struct Admitted<C, X> {
    header: Arc<Header>,
    /// What admission held it to (decision 20): the journal's evidence.
    admission: journal::Admission,
    /// Its log's digest and validator, from its plan.
    log: String,
    validator: String,
    peaks: ascetic_ddd_command_log::validate::Peaks,
    /// The reader its plan was read by; none for a test's.
    reader: Option<crate::source::Reader>,
    /// The journal its run writes (decision 22, K2b-7e-2b-2b): what its
    /// channel is built from.
    journal: crate::sink::Journal,
    /// The caps of the sources needing sockets, summed (decision 20).
    caps: Option<u64>,
    read_ahead: u64,
    carrier: Arc<X>,
    runtimes: Runtimes,
    /// What it was fitted to (decision 22): the journal's evidence.
    fit: crate::memory::Fit,
    /// The clock it is carried by: the run's.
    clock: PhantomData<fn() -> C>,
}

impl<C, X> Admitted<C, X> {
    /// What it was fitted to: every term, what it needs, its slack.
    pub fn fit(&self) -> &crate::memory::Fit {
        &self.fit
    }

    /// The log's header.
    pub fn header(&self) -> &Arc<Header> {
        &self.header
    }

    /// What a run of it must hold: the validator's peaks.
    pub fn peaks(&self) -> &ascetic_ddd_command_log::validate::Peaks {
        &self.peaks
    }

    /// Its log's digest, as its commitment holds it.
    pub fn log(&self) -> &str {
        &self.log
    }

    /// The validator that committed its log.
    pub fn validator(&self) -> &str {
        &self.validator
    }

    /// What admission held it to: each source's sockets, the descriptors.
    pub fn admission(&self) -> &journal::Admission {
        &self.admission
    }

    /// The caps of its sources needing sockets, summed; none if none does.
    pub fn caps(&self) -> Option<u64> {
        self.caps
    }

    /// The reader its plan was read by; none for a test's.
    pub fn reader(&self) -> Option<&crate::source::Reader> {
        self.reader.as_ref()
    }

    /// The journal its run writes.
    pub fn journal(&self) -> &crate::sink::Journal {
        &self.journal
    }

    /// Its read-ahead.
    pub fn read_ahead(&self) -> u64 {
        self.read_ahead
    }

    /// Its carrier.
    pub fn carrier(&self) -> &X {
        &self.carrier
    }

    /// Where it runs.
    pub fn runtimes(&self) -> &Runtimes {
        &self.runtimes
    }
}

/// The caps summed, with the descriptors open and the reserve, within the
/// soft limit (decision 20's I1); unknown descriptors refused.
pub(crate) fn descriptors_fit(
    caps: u64,
    descriptors: Option<Descriptors>,
) -> Result<(), Inadmissible> {
    let needed = caps.saturating_add(DESCRIPTORS_RESERVE);
    let fits = descriptors.is_some_and(|d| needed.saturating_add(d.open()) <= d.soft());
    if fits {
        return Ok(());
    }
    Err(Inadmissible::Descriptors {
        needed: needed.saturating_add(descriptors.map_or(0, |d| d.open())),
        descriptors,
    })
}

/// The descriptors a source's runtime holds of its own (decision 22,
/// K2b-7e-2b-3d): its driver's epoll, its waker's eventfd and one more of
/// tokio's I/O driver — measured on Linux, three a runtime
/// (`tests/descriptors.rs`), counted by the fit before they exist.
pub const RUNTIME_DESCRIPTORS: u64 = 3;

/// The run's catch-up, per mille (decision 21): what the header journals and
/// the pool's prediction is priced at — one function for both.
pub fn kappa(profile: &ascetic_ddd_command_log::record::Profile) -> u64 {
    u64::from(profile.reserve / 2)
}

/// The bytes a runtime holds of its own with `events` a driver turn
/// (decision 22, K2b-7e-2b-2c-1): [`RUNTIME`] and the events at [`EVENT`]
/// each; none where no event's size is stated.
fn runtime_own(events: usize) -> Option<u64> {
    let events = u64::try_from(events).ok()?;
    RUNTIME.checked_add(events.checked_mul(EVENT?)?)
}

/// A source as `fit` found it: its sockets' cap, what its carrier states of
/// its parts, the plan's need of its sockets.
#[derive(Clone, Copy, Debug)]
struct Source {
    cap: usize,
    needed: u64,
    costs: Option<crate::carrier::Costs>,
    own: crate::carrier::SourceCosts,
}

/// What admission fixes of `plan` carried by `carrier` at `knobs` (decision
/// 22, K2b-7e-2b-3d): each part's bytes at its count — its sources', its
/// reader's at the records ahead, its journal's at the lines behind, the
/// stages', the gates', the machine's and the pool's.
fn planned<C: Clock, X: Carrier<C>>(
    plan: &Plan,
    carrier: &X,
    journal: &crate::sink::Journal,
    sources: &[Source],
    knobs: &crate::memory::Knobs,
) -> Result<crate::memory::Planned, crate::carrier::Unstated> {
    let peaks = plan.peaks();
    let record = &peaks.record;
    let attempts = carrier.attempts(peaks)?;
    let reader = plan.reader().map(|r| crate::source::Reader {
        ahead: knobs.ahead,
        ..*r
    });
    let of = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
    // Each source's cap at the knob's total of ports, where the carrier has
    // one and the knob is moved: as its egress divides them — at its own
    // total, its needs' caps, which `fit` holds equal to that map.
    let count = u32::try_from(sources.len()).unwrap_or(u32::MAX);
    let caps = carrier
        .ports()
        .filter(|&own| own != knobs.ports)
        .and_then(|_| carrier.caps_at(knobs.ports, count));
    Ok(crate::memory::Planned {
        sources: (0usize..)
            .zip(sources)
            .map(|(i, s)| {
                let cap = match (&caps, s.costs) {
                    (Some(c), Some(_)) => c.get(i).copied().unwrap_or(s.cap),
                    _ => s.cap,
                };
                (cap, s)
            })
            .map(|(cap, s)| crate::memory::SourcePart {
                runtime: crate::live::events_for(&[cap])
                    .ok()
                    .and_then(|e| e.first().copied())
                    .and_then(runtime_own),
                cap: of(cap),
                connection: s.costs.map_or(0, |c| c.connection),
                io: s.costs.map_or(0, |c| c.io),
                answer: s.costs.map_or(0, |c| c.answer),
                carrier: s.own.total(),
                carrier_stated: s.own.stated_by,
                keeping: ending(record),
            })
            .collect(),
        reader: reader.map_or(0, |r| r.bytes(record)),
        decoder: if reader.is_some() {
            ascetic_ddd_command_log::live::DECODER
        } else {
            0
        },
        journal: crate::sink::Journal::new(knobs.behind, journal.out()).bytes(record),
        judging: crate::stages::judging_bound(plan.header()),
        gates: carrier.gate_bytes(),
        machine: ascetic_ddd_scheduler::fits::terms(peaks),
        records: ascetic_ddd_scheduler::fits::late_records(peaks),
        shell_attempts: attempt_share(&peaks.late_shape),
        carrier_attempts: attempts.price(&peaks.late_shape),
        carrier_stated: attempts.stated_by,
        kappa: kappa(&plan.header().profile),
    })
}

/// A run fitted to the memory stated (decision 22, K2b-7e-2b-3d): its plan,
/// its carrier, its journal and its read-ahead bound by value to what they
/// price — the only way to [`Live::fitted`](crate::live::Live::fitted) and to
/// [`admit`]; a host fitting one journal and running another cannot.
pub struct Fitted<C, X> {
    plan: Plan,
    read_ahead: u64,
    carrier: Arc<X>,
    journal: crate::sink::Journal,
    memory: crate::memory::Memory,
    planned: crate::memory::Planned,
    fit: crate::memory::Fit,
    sources: Vec<Source>,
    clock: PhantomData<fn() -> C>,
}

impl<C, X> Fitted<C, X> {
    /// The fit: every term, what the run needs, its slack.
    pub fn fit(&self) -> &crate::memory::Fit {
        &self.fit
    }

    /// The memory stated.
    pub fn memory(&self) -> crate::memory::Memory {
        self.memory
    }

    /// Its sources' caps, as the runtimes are to be built for them.
    pub fn caps(&self) -> Vec<usize> {
        self.sources.iter().map(|s| s.cap).collect()
    }

    /// The log's header.
    pub fn header(&self) -> &Arc<Header> {
        self.plan.header()
    }
}

/// Fits `plan`, read `read_ahead` µs ahead, its attempts carried by
/// `carrier`, its journal `journal`, to `memory` (decision 22, K2b-7e-2b-3d) —
/// pure, before anything is built, every refusal at once: a carrier that
/// cannot carry it or tell a source's needs, a source needing more sockets
/// under the plan's bound than its cap (decision 20's I2), a cap past what a
/// driver sizes, the descriptors its caps and runtimes would hold past the
/// soft limit as read now, a part no owner states, the memory, the memory
/// past what `limit` allows, a pool no enforcer holds outside a scope of its
/// own with a finite `memory.max`.
pub fn fit<C: Clock, X: Carrier<C>>(
    plan: Plan,
    read_ahead: u64,
    carrier: Arc<X>,
    journal: crate::sink::Journal,
    memory: crate::memory::Memory,
    limit: crate::live::Limit,
) -> Result<Fitted<C, X>, RunError> {
    let header = plan.header().clone();
    // The machine's own refusals: a read-ahead past the header's.
    drop(Machine::new(header.clone(), read_ahead)?);
    let mut refused = Vec::new();
    if let Err(why) = carrier.carries(&header) {
        refused.push(Inadmissible::Carrier(why));
    }
    let count = header.profile.sources.max(1);
    let mut sources = Vec::new();
    for source in 0..count {
        let needs = carrier.needs(plan.peaks(), source);
        let own = carrier.source_costs(plan.peaks(), source);
        let (needs, own) = match (needs, own) {
            (Ok(n), Ok(o)) => (n, o),
            (Err(why), _) | (_, Err(why)) => {
                refused.push(Inadmissible::Carrier(why));
                continue;
            }
        };
        let s = match needs {
            Needs::Nothing => Source {
                cap: 0,
                needed: 0,
                costs: None,
                own,
            },
            Needs::Sockets { needed, cap, costs } => {
                if u64::try_from(cap).is_ok_and(|c| needed > c) {
                    refused.push(Inadmissible::Sockets {
                        source,
                        needed,
                        cap,
                    });
                }
                if crate::live::events_for(&[cap]).is_err() {
                    refused.push(Inadmissible::Events {
                        source,
                        cap,
                        events: None,
                    });
                }
                Source {
                    cap,
                    needed,
                    costs: Some(costs),
                    own,
                }
            }
        };
        sources.push(s);
    }
    // The carrier's map at its own total is its needs' caps: what a remedy
    // prices at another total is what would be built at it.
    if let Some(ports) = carrier.ports()
        && let Some(map) = carrier.caps_at(ports, count)
    {
        for (source, s) in (0u32..).zip(&sources) {
            let mapped = map
                .get(usize::try_from(source).unwrap_or(usize::MAX))
                .copied();
            if s.costs.is_some() && mapped != Some(s.cap) {
                refused.push(Inadmissible::Carrier(format!(
                    "source {source}: a cap of {} as its needs state it, {mapped:?} as its egress \
                     divides its {ports} ports",
                    s.cap
                )));
            }
        }
    }
    // The descriptors as read now — after the host raised them —, the caps'
    // and each runtime's own, before they are built.
    let caps: u64 = sources
        .iter()
        .map(|s| u64::try_from(s.cap).unwrap_or(u64::MAX))
        .fold(0, u64::saturating_add);
    if caps > 0 {
        let runtimes = u64::from(count).saturating_mul(RUNTIME_DESCRIPTORS);
        if let Err(why) = descriptors_fit(caps.saturating_add(runtimes), Descriptors::read().ok()) {
            refused.push(why);
        }
    }
    if let Some((least, depth, file)) = limit.least()
        && memory.bytes() > least
    {
        refused.push(Inadmissible::Limit {
            stated: memory.bytes(),
            limit: least,
            file,
            depth,
        });
    }
    let knobs = crate::memory::Knobs {
        ports: carrier.ports().unwrap_or(0),
        ahead: plan.reader().map_or(0, |r| r.ahead),
        behind: journal.behind(),
        answer_cap: header.profile.answer_cap,
    };
    let fitted = if sources.len() == usize::try_from(count).unwrap_or(usize::MAX) {
        match planned(&plan, carrier.as_ref(), &journal, &sources, &knobs) {
            Err(unstated) => {
                refused.push(Inadmissible::Unstated {
                    part: unstated.part,
                });
                None
            }
            Ok(planned) => {
                // A count no enforcer holds — the pool's, until the read
                // gate — is held by the process's own cgroup, or refused.
                let unheld = crate::memory::terms(&planned)
                    .iter()
                    .any(|t| matches!(t.held, crate::memory::Held::Gate { built: false }));
                if unheld && limit.max().is_none_or(|(_, depth)| depth > 0) {
                    refused.push(Inadmissible::Scope { max: limit.max() });
                }
                fitted_or_refused(
                    crate::memory::fit(&planned, memory),
                    planned,
                    &mut refused,
                    |unfit, planned| {
                        // Each knob moved alone: the records ahead and the
                        // lines behind down to one, the carrier's ports down
                        // to the least that gives each source the sockets the
                        // plan needs of it; the plan's answer cap is its
                        // author's.
                        let floors = crate::memory::Knobs {
                            ports: least_ports(carrier.as_ref(), &sources, knobs.ports),
                            ahead: knobs.ahead.min(1),
                            behind: 1,
                            ..knobs
                        };
                        let at = |k: &crate::memory::Knobs| {
                            planned_of(&plan, carrier.as_ref(), &journal, &sources, k, planned)
                        };
                        let remedies = crate::memory::remedies(memory, knobs, floors, at);
                        Inadmissible::Memory(Box::new(crate::memory::Refused {
                            memory,
                            unfit,
                            remedies,
                        }))
                    },
                )
            }
        }
    } else {
        None
    };
    match (Inadmissible::of(refused), fitted) {
        (Some(why), _) => Err(RunError::Inadmissible(why)),
        (None, Some((planned, fit))) => Ok(Fitted {
            plan,
            read_ahead,
            carrier,
            journal,
            memory,
            planned,
            fit,
            sources,
            clock: PhantomData,
        }),
        (None, None) => Err(RunError::Inadmissible(Inadmissible::Unstated {
            part: "the plan's sources",
        })),
    }
}

/// The fit's outcome: `planned` and its fit, or none and its refusal — made
/// by `refuse` from the unfit — pushed beside the others.
fn fitted_or_refused(
    fit: Result<crate::memory::Fit, crate::memory::Unfit>,
    planned: crate::memory::Planned,
    refused: &mut Vec<Inadmissible>,
    refuse: impl FnOnce(crate::memory::Unfit, &crate::memory::Planned) -> Inadmissible,
) -> Option<(crate::memory::Planned, crate::memory::Fit)> {
    match fit {
        Ok(fit) => Some((planned, fit)),
        Err(unfit) => {
            refused.push(refuse(unfit, &planned));
            None
        }
    }
}

/// The least total of ports, at most `ports`, that gives each source with
/// sockets the sockets the plan needs of it, as `carrier`'s egress divides
/// them; `ports` itself where it has no such knob or none gives them.
fn least_ports<C: Clock, X: Carrier<C>>(carrier: &X, sources: &[Source], ports: usize) -> usize {
    let count = u32::try_from(sources.len()).unwrap_or(u32::MAX);
    let enough = |p: usize| {
        carrier.caps_at(p, count).is_some_and(|caps| {
            sources
                .iter()
                .zip(&caps)
                .all(|(s, &c)| s.costs.is_none() || u64::try_from(c).is_ok_and(|c| c >= s.needed))
        })
    };
    if carrier.ports().is_none() || !enough(ports) {
        return ports;
    }
    // `enough` is monotone in the total: the least it holds at.
    let (mut lo, mut hi) = (0usize, ports);
    while lo < hi {
        let mid = lo.saturating_add(hi.saturating_sub(lo) / 2);
        if enough(mid) {
            hi = mid;
        } else {
            lo = mid.saturating_add(1);
        }
    }
    hi
}

/// [`planned`] at `knobs`, or `base` where a part goes unstated — which no
/// knob changes.
fn planned_of<C: Clock, X: Carrier<C>>(
    plan: &Plan,
    carrier: &X,
    journal: &crate::sink::Journal,
    sources: &[Source],
    knobs: &crate::memory::Knobs,
    base: &crate::memory::Planned,
) -> crate::memory::Planned {
    planned(plan, carrier, journal, sources, knobs).unwrap_or_else(|_| base.clone())
}

/// Admits `fitted` on `runtimes` (decision 20; decision 22, K2b-7e-2b-3d) —
/// nothing else: what was fitted runs. Refused, every refusal at once: a
/// runtime not `current_thread`, fewer runtimes than sources, a source whose
/// cap with its driver's own exceeds the events a turn (decision 20's I1),
/// the descriptors past the soft limit as the runtimes left them, a runtime's
/// own not the one fitted — a test's assumed runtimes exempt by name.
///
/// Admission takes what `fit` made:
///
/// ```
/// use ascetic_ddd_load_run::carrier::Carrier;
/// use ascetic_ddd_load_run::clock::RealClock;
/// use ascetic_ddd_load_run::run::{Fitted, Runtimes, admit};
/// fn host<X: Carrier<RealClock>>(fitted: Fitted<RealClock, X>, runtimes: &Runtimes) {
///     let _ = admit(fitted, runtimes);
/// }
/// ```
///
/// and nothing else — a plan is no fit:
///
/// ```compile_fail
/// use ascetic_ddd_load_run::carrier::Carrier;
/// use ascetic_ddd_load_run::clock::RealClock;
/// use ascetic_ddd_load_run::run::{Fitted, Runtimes, admit};
/// use ascetic_ddd_load_run::source::Plan;
/// fn host<X: Carrier<RealClock>>(fitted: Plan, runtimes: &Runtimes) {
///     let _ = admit::<RealClock, X>(fitted, runtimes);
/// }
/// ```
pub fn admit<C: Clock, X: Carrier<C>>(
    fitted: Fitted<C, X>,
    runtimes: &Runtimes,
) -> Result<Admitted<C, X>, RunError> {
    let Fitted {
        plan,
        read_ahead,
        carrier,
        journal,
        memory: _,
        planned,
        fit,
        sources,
        clock: _,
    } = fitted;
    let header = plan.header().clone();
    let mut refused = Vec::new();
    // A source's driver turns only between its tasks on a `current_thread`
    // runtime: what an attempt's look verifies (decision 17).
    if let Some(source) = runtimes
        .sources
        .iter()
        .position(|h| h.runtime_flavor() != tokio::runtime::RuntimeFlavor::CurrentThread)
    {
        refused.push(Inadmissible::Flavour { source });
    }
    let count = header.profile.sources.max(1);
    if runtimes.sources.len() < usize::try_from(count).unwrap_or(usize::MAX) {
        refused.push(Inadmissible::Runtimes {
            have: runtimes.sources.len(),
            sources: header.profile.sources,
        });
    }
    let mut caps: Option<u64> = None;
    let mut rows = Vec::new();
    for (source, s) in (0u32..).zip(&sources) {
        if !runtimes.assumed {
            let fitted = planned
                .sources
                .get(usize::try_from(source).unwrap_or(usize::MAX));
            let fitted = fitted.and_then(|p| p.runtime);
            let built = runtimes.own(source);
            if built != fitted {
                refused.push(Inadmissible::Runtime {
                    source,
                    built,
                    fitted,
                });
            }
        }
        if s.costs.is_none() {
            continue;
        }
        // I1: a look's verified turn sees every ready socket.
        let events = runtimes.events(source);
        let fits = s
            .cap
            .checked_add(DRIVER_EVENTS)
            .zip(events)
            .is_some_and(|(want, have)| want <= have);
        if !fits {
            refused.push(Inadmissible::Events {
                source,
                cap: s.cap,
                events,
            });
        }
        let cap = u64::try_from(s.cap).unwrap_or(u64::MAX);
        caps = Some(caps.unwrap_or(0).saturating_add(cap));
        rows.push(journal::SourceFit {
            source,
            needed: s.needed,
            cap,
            events: events
                .and_then(|e| u64::try_from(e).ok())
                .unwrap_or(u64::MAX),
        });
    }
    // I1: every cap's sockets, with those open — the runtimes' own among
    // them now — and the reserve, within the soft limit.
    if let Some(caps) = caps
        && let Err(why) = descriptors_fit(caps, runtimes.descriptors())
    {
        refused.push(why);
    }
    if let Some(why) = Inadmissible::of(refused) {
        return Err(RunError::Inadmissible(why));
    }
    let descriptors = caps
        .and(runtimes.descriptors())
        .map(|d| journal::DescriptorsRead {
            soft: d.soft(),
            hard: d.hard(),
            open: d.open(),
            raised: d.raised(),
        });
    Ok(Admitted {
        admission: journal::Admission {
            sources: rows,
            descriptors,
            assumed: runtimes.assumed || plan.is_assumed(),
        },
        header,
        log: plan.log().to_owned(),
        validator: plan.validator().to_owned(),
        peaks: plan.peaks().clone(),
        reader: plan.reader().copied(),
        journal,
        caps,
        read_ahead,
        carrier,
        runtimes: runtimes.clone(),
        fit,
        clock: PhantomData,
    })
}

/// A test's admission: `plan` fitted to a memory no run exceeds, in a scope
/// of its own that holds it, then admitted on `runtimes` — [`fit`] and
/// [`admit`] in turn.
#[cfg(feature = "testing")]
pub fn admitted<C: Clock, X: Carrier<C>>(
    plan: Plan,
    read_ahead: u64,
    carrier: Arc<X>,
    runtimes: &Runtimes,
    journal: crate::sink::Journal,
) -> Result<Admitted<C, X>, RunError> {
    let memory = u64::MAX / 4;
    let fitted = fit(
        plan,
        read_ahead,
        carrier,
        journal,
        crate::memory::Memory::declared(memory),
        crate::live::Limit::assumed(Some((memory, 0)), None),
    )?;
    admit(fitted, runtimes)
}

/// A run ready at T0 (decision 20): admitted, measured and gated — its
/// clock started after the third stage, its journal opened from their
/// evidence, its namespace the one its gates certified. Only the stages
/// make one, and [`Ready::assumed`] a test's.
pub struct Ready<C, X, K> {
    admitted: Admitted<C, X>,
    clock: Arc<C>,
    sink: K,
    namespace: Namespace,
}

impl<C, X, K> Ready<C, X, K> {
    pub(crate) fn new(
        admitted: Admitted<C, X>,
        clock: Arc<C>,
        sink: K,
        namespace: Namespace,
    ) -> Ready<C, X, K> {
        Ready {
            admitted,
            clock,
            sink,
            namespace,
        }
    }

    /// A test's: `admitted` run by `clock` into `sink` under `namespace`,
    /// its stages assumed.
    #[cfg(feature = "testing")]
    pub fn assumed(
        admitted: Admitted<C, X>,
        clock: Arc<C>,
        sink: K,
        namespace: Namespace,
    ) -> Ready<C, X, K> {
        Ready::new(admitted, clock, sink, namespace)
    }

    /// Its journal's sink: a test's view of what the stages built.
    #[cfg(feature = "testing")]
    pub fn sink(&self) -> &K {
        &self.sink
    }
}

impl<X, K> Ready<crate::clock::RealClock, X, K> {
    /// T0's wall instant, microseconds since the Unix epoch, as its journal's
    /// header holds it: a host naming its output by its run needs no
    /// journal read back.
    pub fn t0(&self) -> i64 {
        self.clock.wall()
    }
}

/// Runs the log `ready` heads, read from `source` ahead of its clock — T0 its
/// start, after the stages (decisions 18, 20) — its journal's lines after
/// the header into its sink.
///
/// Only a [`Ready`] runs: a run admitted and not through its stages does
/// not.
///
/// ```compile_fail,E0308
/// use ascetic_ddd_command_log::record::Record;
/// use ascetic_ddd_load_run::carrier::Carrier;
/// use ascetic_ddd_load_run::clock::RealClock;
/// use ascetic_ddd_load_run::run::{Admitted, run};
/// use ascetic_ddd_load_run::source::Records;
/// async fn skipped<X: Carrier<RealClock>>(a: Admitted<RealClock, X>) {
///     let _ = run(a, Records::new(std::iter::empty::<Record>())).await;
/// }
/// ```
///
/// Nor is a [`Ready`] built but by the stages:
///
/// ```compile_fail,E0451
/// use ascetic_ddd_load_run::run::{Admitted, Ready};
/// fn forged<C, X, K>(admitted: Admitted<C, X>, clock: std::sync::Arc<C>, sink: K) {
///     let _ = Ready {
///         admitted,
///         clock,
///         sink,
///         namespace: ascetic_ddd_command_log::fills::Namespace(1),
///     };
/// }
/// ```
pub async fn run<C, S, K, X>(
    ready: Ready<C, X, K>,
    mut source: S,
) -> Result<Ran<K::Finished>, RunError>
where
    C: Clock,
    S: LineSource,
    K: JournalSink,
    X: Carrier<C>,
{
    let Ready {
        admitted,
        clock,
        mut sink,
        namespace,
    } = ready;
    let Admitted {
        header,
        read_ahead,
        carrier,
        runtimes,
        reader,
        ..
    } = admitted;
    // The lines read by no more records ahead than admission charged.
    if let (Some(reader), Some(ahead)) = (reader, source.ahead())
        && ahead > reader.ahead
    {
        return Err(RunError::Inadmissible(Inadmissible::Reader {
            admitted: reader.ahead,
            ahead,
        }));
    }
    let runtimes = &runtimes;
    // A hold that outlives it is a stuck carrier (decision 17).
    let route_timeout = header.profile.route_timeout;
    let mut machine = Machine::new(header, read_ahead)?;
    let stamps = Arc::new(Stamps::new(clock.clone(), namespace));
    let mut queue = Ordered::new();
    // The lines read and not ended, for their dispatches, and when each was
    // first dispatched.
    let mut live: BTreeMap<Line, (Arc<Record>, Option<u64>)> = BTreeMap::new();
    let mut line: Line = 2;
    // The next record and when it may be read: `at − L`, not before the line
    // above it.
    let mut pending: Option<(u64, Arc<Record>)> = None;
    let mut last = 0u64;
    let mut exhausted = false;
    let mut stalled: Option<(u64, u32)> = None;
    let mut unsent: BTreeMap<String, u64> = BTreeMap::new();
    let mut unsent_past = 0u64;
    let aborts = Aborts(stamps.clone());
    let halted: Result<(), Halt> = async {
    queue.push(0, Input::Wake);
    loop {
        if let Some(fault) = stamps.raised() {
            return Err(fault.into());
        }
        if pending.is_none() && !exhausted {
            match poll_now(&mut source) {
                Poll::Ready(Ok(Some(r))) => {
                    pending = Some((r.at.saturating_sub(read_ahead).max(last), r));
                }
                Poll::Ready(Ok(None)) => exhausted = true,
                Poll::Ready(Err(e)) => return Err(e.into()),
                Poll::Pending => {}
            }
        }
        // The instant, read under the frontier's lock with what it holds: an
        // answer that arrives after it is stamped no earlier.
        // What the frontier holds and who may still push to it, in one read:
        // read apart, an attempt on another thread could push its end and
        // go between them, and the run end with its answer unread. A fault
        // is read with them: an attempt raises its own before it leaves the
        // count (decision 18), so none alive is never read without it.
        let (now, work, floor, oldest, alive) = {
            let (g, now) = stamps.lock();
            let f = &g.frontier;
            let work = [f.next_pushed(), f.next_deadline()]
                .into_iter()
                .flatten()
                .min();
            (now, work, f.floor(), f.oldest_hold(), g.alive())
        };
        let (alive, oldest_deadline) = alive?;
        // An attempt outliving its deadline by the route timeout: a source
        // that cannot keep up (decision 19) — by age, not count: any number
        // of steps may share a deadline, each alive until its last look.
        if oldest_deadline.is_some_and(|d| now >= d.saturating_add(route_timeout)) {
            return Err(Harness::Saturated { alive }.into());
        }
        // A hold older than the route timeout: a carrier stuck in its
        // judging — a fault, never a hang (decision 17).
        let stuck = oldest.map(|(since, line, attempt)| {
            (since.saturating_add(route_timeout), line, attempt, since)
        });
        if let Some((at, line, attempt, since)) = stuck
            && now >= at
        {
            return Err(Harness::Held {
                line,
                attempt,
                since,
                at: now,
            }
            .into());
        }
        // An answer being judged holds a floor, no later than now: nothing at
        // or past it is taken until it ends — neither the frontier's inputs
        // nor the machine's own lines and wakes, stamped at the instant taken
        // — and then they are taken late, the judging's lateness shown.
        let held = floor.is_some();
        let bound = match floor {
            Some(f) => f.checked_sub(1),
            None => Some(now),
        };
        let reading = pending.as_ref().map(|(when, _)| (*when).max(machine.now()));
        // The machine's own pending wakes, not its outputs' copies: a step's
        // end cancels its backstop, and a run waits for no stale one.
        let own = [machine.next_wake(), reading]
            .into_iter()
            .flatten()
            .min()
            .filter(|_| !held);
        let next = [queue.first(), work, own].into_iter().flatten().min();
        let Some(t) = bound.filter(|b| next.is_some_and(|n| n <= *b)) else {
            if next.is_none() && exhausted && alive == 0 {
                break;
            }
            let reads = pending.is_none() && !exhausted;
            // Held, the floor's end is a push, and its stuck instant the only
            // one to sleep until. Not held, nothing due means the next is
            // later than now; a sleep to an instant passed would return at
            // once, and turn forever.
            // An attempt alive is aged at its deadline plus the route
            // timeout, the loop awake to see it (decision 19).
            let aged = oldest_deadline.map(|d| d.saturating_add(route_timeout));
            let sleep = match stuck {
                Some((at, ..)) => Some(at),
                None => next,
            };
            let sleep = [sleep, aged].into_iter().flatten().min();
            if sleep.is_some_and(|s| s <= now) {
                return Err(Harness::Stalled { at: now }.into());
            }
            tokio::select! {
                biased;
                () = stamps.notified() => {}
                r = poll_fn(|cx| source.poll_next(cx)), if reads => match r {
                    Ok(Some(r)) => pending = Some((r.at.saturating_sub(read_ahead).max(last), r)),
                    Ok(None) => exhausted = true,
                    Err(e) => return Err(e.into()),
                },
                () = clock.sleep_until(sleep.unwrap_or(0)), if sleep.is_some() => {}
            }
            continue;
        };
        let mut progressed = false;
        while let Some((when, r)) = pending.take() {
            if held || when.max(machine.now()) > t {
                pending = Some((when, r));
                break;
            }
            progressed = true;
            // The machine's own work is stamped at the instant taken: never
            // below it, as a floor less one would put it (decision 16).
            if t != now {
                return Err(Harness::Backdated { at: t, now }.into());
            }
            live.insert(line, (r.clone(), None));
            queue.push(t, Input::Line { line, record: r });
            line = line.saturating_add(1);
            last = when;
            match poll_now(&mut source) {
                Poll::Ready(Ok(Some(r))) => {
                    pending = Some((r.at.saturating_sub(read_ahead).max(last), r));
                }
                Poll::Ready(Ok(None)) => exhausted = true,
                Poll::Ready(Err(e)) => return Err(e.into()),
                Poll::Pending => {}
            }
        }
        if !held && machine.next_wake().is_some_and(|w| w <= t) {
            if t != now {
                return Err(Harness::Backdated { at: t, now }.into());
            }
            queue.push(t, Input::Wake);
            progressed = true;
        }
        progressed |= settle(t, &stamps, &mut queue).await;
        while let Some((at, input)) = queue.pop(t) {
            progressed = true;
            // Decision 16: only stamps are bound across instants, never a
            // stamp below the last taken.
            if at < machine.now() {
                return Err(Harness::Disordered {
                    at,
                    last: machine.now(),
                }
                .into());
            }
            let outputs = machine.step(at, input.clone())?;
            for l in journal::events(at, &input, &outputs) {
                if let journal::Line::Answer(a) = &l
                    && let Some(u) = &a.unsent
                {
                    let code = match &u.code {
                        journal::Code::Named(c) => c.clone(),
                        journal::Code::Other => "other".to_owned(),
                    };
                    if unsent.len() < UNSENT || unsent.contains_key(&code) {
                        let n = unsent.entry(code).or_insert(0u64);
                        *n = n.saturating_add(1);
                    } else {
                        unsent_past = unsent_past.saturating_add(1);
                    }
                }
                sink.line(l)?;
            }
            for output in outputs {
                match output {
                    Output::Want(_)
                    | Output::Bound { .. }
                    | Output::Released { .. }
                    | Output::Ignored { .. }
                    // Its wakes are read back from the machine itself.
                    | Output::WakeAt(_) => {}
                    Output::Dispatch {
                        line,
                        attempt,
                        deadline,
                        captured,
                        ..
                    } => {
                        let censor = stamps.lock().0.frontier.open(at, line, attempt, deadline);
                        if let Some((at, censor)) = censor {
                            queue.push(at, censor);
                        }
                        if let Some((record, first)) = live.get_mut(&line) {
                            let first = *first.get_or_insert(at);
                            let source = usize::try_from(source_of(record)).unwrap_or(usize::MAX);
                            let Some(handle) = runtimes.sources.get(source) else {
                                return Err(Harness::Carrier {
                                    line,
                                    attempt,
                                    cannot: "run.source",
                                    detail: format!("source {source} has no runtime"),
                                }
                                .into());
                            };
                            let a = Attempt::new(
                                stamps.clone(),
                                line,
                                attempt,
                                record.clone(),
                                captured,
                                (first, at),
                                deadline,
                            );
                            // Open loop: spawned, never queued (a queue hides
                            // lateness). Its first poll, on its source's
                            // runtime, is when it was handed (decision 17):
                            // stamped here, so no carrier can forget it.
                            let carrier = carrier.clone();
                            let path = crate::stamps::Feeds::Attempt(a.path());
                            stamps.spawn_on(handle, path, async move {
                                let mut a = a;
                                a.handed();
                                carrier.carry(a).await;
                            });
                        }
                    }
                    Output::Ended(e) => {
                        live.remove(&e.line);
                    }
                }
            }
            if let Some(fault) = stamps.raised() {
                return Err(fault.into());
            }
            settle(t, &stamps, &mut queue).await;
        }
        // Work due at an instant and none done: live, once, a race with an
        // answer arriving at that microsecond; again and again at one
        // instant, a loop that never ends.
        if progressed {
            stalled = None;
        } else {
            let n = match stalled {
                Some((at, n)) if at == t => n.saturating_add(1),
                _ => 1,
            };
            if n > STALLS {
                return Err(Harness::Stalled { at: t }.into());
            }
            stalled = Some((t, n));
        }
    }
    Ok(())
    }
    .await;
    let stop = match halted {
        Ok(()) => None,
        Err(Halt::Stop(stop)) => Some(stop),
        Err(Halt::Journal(e)) => return Err(e.into()),
    };
    if let Some(stop) = &stop {
        // Decision 18: no more dispatches; every task aborted and awaited,
        // bounded; then the fault — no earlier than any line — and the
        // trailer.
        stamps.tasks().abort();
        let until = clock.now().saturating_add(QUIESCE);
        while stamps.tasks().running() > 0 && clock.now() < until {
            let next = clock.now().saturating_add(1_000).min(until);
            clock.sleep_until(next).await;
        }
        let at = clock.now().max(machine.now());
        sink.line(journal::Line::Fault(journal::Fault {
            at,
            class: stop.class(),
        }))?;
    }
    let running = stamps.tasks().running();
    drop(aborts);
    Ok(Ran {
        journal: sink.finish()?,
        held: machine.held(),
        kept: machine.kept(),
        end: stop.map_or(RunEnd::Ended, RunEnd::Stopped),
        running,
        unsent,
        unsent_past,
    })
}

/// The instant `t` settled, then what the frontier releases through it into
/// the machine's buffer: whether anything came.
async fn settle<C: Clock>(t: u64, stamps: &Stamps<C>, queue: &mut Ordered) -> bool {
    stamps.clock().settled(t).await;
    let drained = stamps.lock().0.frontier.drain(t, true);
    let any = !drained.is_empty();
    for (at, input) in drained {
        queue.push(at, input);
    }
    any
}
