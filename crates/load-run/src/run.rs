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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::poll_fn;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use ascetic_ddd_command_log::LogError;
use ascetic_ddd_command_log::record::{Action, Header, Probe, Record};
use ascetic_ddd_scheduler::journal::{self, JournalError};
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

/// A run that did not end.
#[derive(Debug)]
pub enum RunError {
    /// Refused before T0: no journal.
    Refused(String),
    /// The log, or the machine on it.
    Log(LogError),
    /// The journal's sink.
    Journal(JournalError),
    /// The harness.
    Harness(Harness),
}

impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RunError::Refused(why) => write!(f, "refused before T0: {why}"),
            RunError::Log(e) => write!(f, "{e}"),
            RunError::Journal(e) => write!(f, "{e}"),
            RunError::Harness(h) => write!(f, "harness fault: {h}"),
        }
    }
}

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

impl From<Harness> for RunError {
    fn from(h: Harness) -> RunError {
        RunError::Harness(h)
    }
}

/// A run's end: what its journal left, and the machine's last counts.
#[derive(Debug)]
pub struct Ran<F> {
    /// The journal's sink, finished.
    pub journal: F,
    /// Cells the machine still held.
    pub held: usize,
    /// Cells kept for their dependents.
    pub kept: usize,
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
    let mut wakes: BTreeSet<u64> = BTreeSet::new();
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
        let (now, work, floor, oldest) = {
            let (g, now) = stamps.lock();
            let f = &g.frontier;
            let work = [f.next_pushed(), f.next_deadline()]
                .into_iter()
                .flatten()
                .min();
            (now, work, f.floor(), f.oldest_hold())
        };
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
        let own = [wakes.first().copied(), reading]
            .into_iter()
            .flatten()
            .min()
            .filter(|_| !held);
        let next = [queue.first(), work, own].into_iter().flatten().min();
        let Some(t) = bound.filter(|b| next.is_some_and(|n| n <= *b)) else {
            if next.is_none() && exhausted && stamps.alive() == 0 {
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
        while !held && wakes.first().is_some_and(|w| *w <= t) {
            wakes.pop_first();
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
                    | Output::Ignored { .. } => {}
                    Output::WakeAt(w) => {
                        wakes.insert(w.max(at));
                    }
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
                                return Err(RunError::Refused(format!(
                                    "line {line}: source {source} has no runtime"
                                )));
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
                            drop(handle.spawn(async move {
                                a.handed();
                                carrier.carry(a).await;
                            }));
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
    let held = machine.held();
    let kept = machine.kept();
    Ok(Ran {
        journal: sink.finish()?,
        held,
        kept,
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
