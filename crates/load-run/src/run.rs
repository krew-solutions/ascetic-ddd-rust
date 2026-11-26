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
    pub(crate) fn new(
        sources: Vec<Handle>,
        events: Vec<usize>,
        descriptors: Option<Descriptors>,
    ) -> Runtimes {
        Runtimes {
            sources,
            events,
            descriptors,
            assumed: false,
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
        let events = u64::try_from(self.events(s)?).ok()?;
        RUNTIME.checked_add(events.checked_mul(EVENT?)?)
    }
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
    /// The clock it is carried by: the run's.
    clock: PhantomData<fn() -> C>,
}

impl<C, X> Admitted<C, X> {
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

/// Admits `plan`, read `read_ahead` µs ahead, its attempts carried by
/// `carrier` on `runtimes`, its journal `journal` — or refuses it, before any
/// journal exists: a
/// carrier that cannot carry it, a runtime not `current_thread`, fewer
/// runtimes than sources, a read-ahead past the header's; a source whose cap
/// with its driver's own exceeds the events a turn (decision 20's I1), or
/// whose sockets needed under the plan's bound exceed its cap (I2).
pub fn admit<C: Clock, X: Carrier<C>>(
    plan: Plan,
    read_ahead: u64,
    carrier: Arc<X>,
    runtimes: &Runtimes,
    journal: crate::sink::Journal,
) -> Result<Admitted<C, X>, RunError> {
    let header = plan.header().clone();
    let refused = |i| RunError::Inadmissible(i);
    carrier
        .carries(&header)
        .map_err(|why| refused(Inadmissible::Carrier(why)))?;
    // A source's driver turns only between its tasks on a `current_thread`
    // runtime: what an attempt's look verifies (decision 17).
    if let Some(source) = runtimes
        .sources
        .iter()
        .position(|h| h.runtime_flavor() != tokio::runtime::RuntimeFlavor::CurrentThread)
    {
        return Err(refused(Inadmissible::Flavour { source }));
    }
    let sources = header.profile.sources.max(1);
    if runtimes.sources.len() < usize::try_from(sources).unwrap_or(usize::MAX) {
        return Err(refused(Inadmissible::Runtimes {
            have: runtimes.sources.len(),
            sources: header.profile.sources,
        }));
    }
    // The caps of the sources needing sockets, summed: the process's share.
    let mut caps: Option<u64> = None;
    let mut fitted = Vec::new();
    for source in 0..sources {
        let needs = carrier
            .needs(plan.peaks(), source)
            .map_err(|why| refused(Inadmissible::Carrier(why)))?;
        if let Needs::Sockets { needed, cap, .. } = needs {
            // I1: a look's verified turn sees every ready socket.
            let events = runtimes.events(source);
            let fits = cap
                .checked_add(DRIVER_EVENTS)
                .zip(events)
                .is_some_and(|(want, have)| want <= have);
            if !fits {
                return Err(refused(Inadmissible::Events {
                    source,
                    cap,
                    events,
                }));
            }
            // I2: the plan's bound within the cap.
            if u64::try_from(cap).is_ok_and(|c| needed > c) {
                return Err(refused(Inadmissible::Sockets {
                    source,
                    needed,
                    cap,
                }));
            }
            let cap = u64::try_from(cap).unwrap_or(u64::MAX);
            caps = Some(caps.unwrap_or(0).saturating_add(cap));
            fitted.push(journal::SourceFit {
                source,
                needed,
                cap,
                events: events
                    .and_then(|e| u64::try_from(e).ok())
                    .unwrap_or(u64::MAX),
            });
        }
    }
    // I1: every cap's sockets, with those open and the reserve, within the
    // soft limit — EMFILE never before a cap's own wait.
    if let Some(caps) = caps {
        let descriptors = runtimes.descriptors();
        descriptors_fit(caps, descriptors).map_err(refused)?;
    }
    // The machine's own refusals: a read-ahead past the header's.
    drop(Machine::new(header.clone(), read_ahead)?);
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
            sources: fitted,
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
        clock: PhantomData,
    })
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
