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
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use ascetic_ddd_command_log::LogError;
use ascetic_ddd_command_log::record::{Action, Header, Probe, Record};
use ascetic_ddd_scheduler::journal::{self, FaultClass, JournalError, LogFault};
use ascetic_ddd_scheduler::machine::{Input, Line, Machine, Output};
use ascetic_ddd_scheduler::order::Ordered;
use tokio::runtime::Handle;

use crate::carrier::Carrier;
use crate::clock::Clock;
use crate::sink::JournalSink;
use crate::source::LineSource;
use crate::stamps::{Attempt, Harness, Stamps};

/// Where attempts run: a runtime handle per source, in the order of the
/// profile's sources. Live, one `current_thread` runtime per source; in tests,
/// one paused runtime behind every handle.
#[derive(Clone, Debug)]
pub struct Runtimes {
    /// By source.
    pub sources: Vec<Handle>,
}

/// A run that left no journal to fold: refused before it began, or its
/// journal's own sink failed (a failed write may leave a line cut short, so
/// nothing may follow it).
#[derive(Debug)]
pub enum RunError {
    /// Refused before T0.
    Refused(String),
    /// The log, refused before T0.
    Log(LogError),
    /// The journal's sink.
    Journal(JournalError),
}

impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RunError::Refused(why) => write!(f, "refused before T0: {why}"),
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

/// Attempt handles alive past the steps not ended, beyond which a source is
/// saturated: its tasks outlive their steps (decision 18).
pub const SATURATION: usize = 1_024;

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
}

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

/// Runs the log `header` heads, read from `source` `read_ahead` µs ahead of
/// the clock, its attempts carried by `carrier` on `runtimes`, its journal's
/// lines after the header into `sink`. Refused before T0 if the carrier
/// cannot carry the log or a source has no runtime.
pub async fn run<C, S, K, X>(
    header: Arc<Header>,
    read_ahead: u64,
    clock: Arc<C>,
    mut source: S,
    mut sink: K,
    carrier: Arc<X>,
    runtimes: &Runtimes,
) -> Result<Ran<K::Finished>, RunError>
where
    C: Clock,
    S: LineSource,
    K: JournalSink,
    X: Carrier<C>,
{
    carrier.carries(&header).map_err(RunError::Refused)?;
    // A source's driver turns only between its tasks on a `current_thread`
    // runtime: what an attempt's look verifies (decision 17).
    if let Some(i) = runtimes
        .sources
        .iter()
        .position(|h| h.runtime_flavor() != tokio::runtime::RuntimeFlavor::CurrentThread)
    {
        return Err(RunError::Refused(format!(
            "source {i}'s runtime is not current_thread"
        )));
    }
    let sources = usize::try_from(header.profile.sources).unwrap_or(usize::MAX);
    if runtimes.sources.len() < sources.max(1) {
        return Err(RunError::Refused(format!(
            "{} runtimes for {} sources",
            runtimes.sources.len(),
            header.profile.sources
        )));
    }
    // A hold that outlives it is a stuck carrier (decision 17).
    let route_timeout = header.profile.route_timeout;
    let mut machine = Machine::new(header, read_ahead)?;
    let stamps = Arc::new(Stamps::new(clock.clone()));
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
        let alive = alive?;
        // Attempts outliving their steps: a source that cannot keep up.
        if alive > live.len().saturating_add(SATURATION) {
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
            let sleep = match stuck {
                Some((at, ..)) => Some(at),
                None => next,
            };
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
                            stamps.spawn_on(handle, async move {
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
