//! The frontier's lock and the attempts that push through it (ADR-0047
//! decision 16).
//!
//! Every input an attempt reports is a read of the clock and a push into the
//! [`Frontier`] under one lock: pushes arrive in the order of their stamps, and
//! the machine's drains, taken under the same lock, never release an input
//! below one still to come. A push below what was released, an attempt dropped
//! unended, a poisoned lock — each is a harness fault, raised here and taken by
//! the machine's loop, never the system's Unknown.

use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use ascetic_ddd_command_log::record::Record;
use ascetic_ddd_scheduler::frontier::{Below, Frontier, Hold};
use ascetic_ddd_scheduler::journal::{Code, FaultClass};
use ascetic_ddd_scheduler::machine::{Captured, Found, Judgement, Line, Seen};
use tokio::sync::Notify;
use tokio::task::AbortHandle;

use crate::clock::Clock;

/// A harness fault: the shell, not the system under test, failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Harness {
    /// An input stamped below what the frontier released.
    Below(Below),
    /// An attempt's handle dropped before it ended: a carrier that panicked
    /// or forgot it.
    Dropped {
        /// Its step's line.
        line: Line,
        /// Which attempt.
        attempt: u32,
    },
    /// A lock poisoned by a panic while held.
    Poisoned,
    /// The loop found work due at an instant and made no progress on it.
    Stalled {
        /// The instant.
        at: u64,
    },
    /// A carrier could not carry an attempt: a frame, a route or a
    /// predicate the plan's checks should have refused.
    Carrier {
        /// Its step's line.
        line: Line,
        /// Which attempt.
        attempt: u32,
        /// What it could not do: a dotted name the carrier chooses, the only
        /// part journaled (decision 18).
        cannot: &'static str,
        /// Why, for the caller only: it may hold a host's text — a route's
        /// reason, a decorator's error — never the journal's.
        detail: String,
    },
    /// Attempt handles alive past the steps not ended and a margin: a source
    /// that cannot keep up, its tasks outliving their steps (decision 18).
    Saturated {
        /// Attempt handles alive.
        alive: usize,
    },
    /// An answer judged for the route timeout and still not ended (ADR-0047
    /// decision 17): a carrier stuck, never an OS slice.
    Held {
        /// Its step's line.
        line: Line,
        /// Which attempt.
        attempt: u32,
        /// When it arrived.
        since: u64,
        /// When the loop gave up on it.
        at: u64,
    },
    /// The machine's own work — a line read, a wake — stamped below the
    /// instant it was taken: its lateness hidden.
    Backdated {
        /// Its stamp.
        at: u64,
        /// When it was taken.
        now: u64,
    },
    /// An input taken stamped below one taken before it: the machine's
    /// clock would go back.
    Disordered {
        /// Its stamp.
        at: u64,
        /// The last taken.
        last: u64,
    },
}

impl fmt::Display for Harness {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Harness::Below(b) => write!(f, "{b}"),
            Harness::Dropped { line, attempt } => {
                write!(f, "attempt {attempt} of line {line} dropped unended")
            }
            Harness::Poisoned => write!(f, "the frontier's lock poisoned"),
            Harness::Stalled { at } => write!(f, "no progress on what was due at {at} µs"),
            Harness::Carrier {
                line,
                attempt,
                cannot,
                detail,
            } => write!(f, "attempt {attempt} of line {line}: {cannot}: {detail}"),
            Harness::Saturated { alive } => {
                write!(f, "{alive} attempts alive, past the steps not ended")
            }
            Harness::Held {
                line,
                attempt,
                since,
                at,
            } => write!(
                f,
                "attempt {attempt} of line {line} judged since {since} µs, still at {at} µs"
            ),
            Harness::Backdated { at, now } => {
                write!(
                    f,
                    "the machine's own work stamped {at} µs, taken at {now} µs"
                )
            }
            Harness::Disordered { at, last } => {
                write!(f, "an input stamped {at} µs taken after one at {last} µs")
            }
        }
    }
}

impl std::error::Error for Harness {}

impl Harness {
    /// Its class in the journal (decision 18): numbers and dotted names, no
    /// host text.
    pub fn class(&self) -> FaultClass {
        match self {
            Harness::Below(b) => FaultClass::Below {
                stamp: b.at,
                released: b.released,
            },
            Harness::Dropped { line, attempt } => FaultClass::Dropped {
                line: *line,
                attempt: *attempt,
            },
            Harness::Poisoned => FaultClass::Poisoned,
            Harness::Stalled { .. } => FaultClass::Stalled,
            Harness::Carrier {
                line,
                attempt,
                cannot,
                ..
            } => FaultClass::Carrier {
                line: *line,
                attempt: *attempt,
                cannot: Code::from(&ascetic_ddd_scheduler::machine::Code::of(cannot)),
            },
            Harness::Saturated { alive } => FaultClass::Saturated {
                alive: u64::try_from(*alive).unwrap_or(u64::MAX),
            },
            Harness::Held {
                line,
                attempt,
                since,
                ..
            } => FaultClass::Held {
                line: *line,
                attempt: *attempt,
                since: *since,
            },
            Harness::Backdated { at, now } => FaultClass::Backdated {
                stamp: *at,
                now: *now,
            },
            Harness::Disordered { at, last } => FaultClass::Disordered {
                stamp: *at,
                last: *last,
            },
        }
    }
}

pub(crate) struct State {
    pub(crate) frontier: Frontier,
    /// The first fault raised: read only with the count, by
    /// [`State::alive`].
    fault: Option<Harness>,
    /// Attempt handles alive: an attempt pushes its end before its handle
    /// goes, so none alive, read with the frontier, is none to come.
    alive: usize,
}

impl State {
    /// Raises `fault`: the first one stands.
    pub(crate) fn raise(&mut self, fault: Harness) {
        self.fault.get_or_insert(fault);
    }

    /// The first fault raised.
    pub(crate) fn raised(&self) -> Option<&Harness> {
        self.fault.as_ref()
    }

    /// The fault raised, or else how many attempt handles are alive: the
    /// count is never read without the fault (decision 18).
    pub(crate) fn alive(&self) -> Result<usize, Harness> {
        match &self.fault {
            Some(f) => Err(f.clone()),
            None => Ok(self.alive),
        }
    }

    fn enter(&mut self) {
        self.alive = self.alive.saturating_add(1);
    }

    /// An attempt's handle gone, raising its fault if it went unended, in one
    /// critical section with the count: none alive is never read without it
    /// (decision 18).
    fn leave(&mut self, fault: Option<Harness>) {
        if let Some(f) = fault {
            self.raise(f);
        }
        self.alive = self.alive.saturating_sub(1);
    }
}

/// Every task the shell spawns for a run, so that a stop aborts them all, and
/// a run dropped with them (decision 18): reaped as the list grows.
#[derive(Default)]
pub(crate) struct Tasks {
    handles: Mutex<Vec<AbortHandle>>,
}

impl Tasks {
    fn keep(&self, h: AbortHandle) {
        let mut v = self.handles.lock().unwrap_or_else(PoisonError::into_inner);
        if v.len() == v.capacity() {
            v.retain(|h| !h.is_finished());
        }
        v.push(h);
    }

    /// Aborts every task still running.
    pub(crate) fn abort(&self) {
        let v = self.handles.lock().unwrap_or_else(PoisonError::into_inner);
        for h in v.iter() {
            h.abort();
        }
    }

    /// Tasks not finished.
    pub(crate) fn running(&self) -> usize {
        let v = self.handles.lock().unwrap_or_else(PoisonError::into_inner);
        v.iter().filter(|h| !h.is_finished()).count()
    }
}

/// The frontier under its lock, the clock it stamps by, and the machine's
/// signal.
pub(crate) struct Stamps<C> {
    clock: Arc<C>,
    state: Mutex<State>,
    notify: Notify,
    tasks: Tasks,
}

impl<C: Clock> Stamps<C> {
    pub(crate) fn new(clock: Arc<C>) -> Stamps<C> {
        Stamps {
            clock,
            state: Mutex::new(State {
                frontier: Frontier::new(),
                fault: None,
                alive: 0,
            }),
            notify: Notify::new(),
            tasks: Tasks::default(),
        }
    }

    pub(crate) fn clock(&self) -> &C {
        &self.clock
    }

    /// The state, the clock read under its lock. A poisoned lock raises its
    /// fault and still yields the state, for the machine to stop on.
    pub(crate) fn lock(&self) -> (MutexGuard<'_, State>, u64) {
        let g = match self.state.lock() {
            Ok(g) => g,
            Err(p) => {
                let mut g = p.into_inner();
                g.raise(Harness::Poisoned);
                g
            }
        };
        // The clock read under the lock: every push is in stamp order.
        let now = self.clock.now();
        (g, now)
    }

    /// Raises a fault (the first one stands) and wakes the machine.
    pub(crate) fn fault(&self, fault: Harness) {
        self.lock().0.raise(fault);
        self.notify.notify_one();
    }

    /// The fault raised, if any.
    pub(crate) fn raised(&self) -> Option<Harness> {
        self.lock().0.raised().cloned()
    }

    /// The run's tasks.
    pub(crate) fn tasks(&self) -> &Tasks {
        &self.tasks
    }

    /// Spawns `task` on `on`, kept for the stop to abort.
    pub(crate) fn spawn_on<F>(&self, on: &tokio::runtime::Handle, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.tasks.keep(on.spawn(task).abort_handle());
    }

    pub(crate) fn notify(&self) {
        self.notify.notify_one();
    }

    pub(crate) async fn notified(&self) {
        self.notify.notified().await;
    }

    fn push(&self, f: impl FnOnce(&mut Frontier, u64) -> Result<(), Below>) {
        let below = {
            let (mut g, now) = self.lock();
            f(&mut g.frontier, now).err()
        };
        if let Some(b) = below {
            self.fault(Harness::Below(b));
        }
        self.notify.notify_one();
    }
}

/// One attempt, as its carrier sees it: what to send and by when, and how
/// to report what it saw. Each report is stamped by the clock under the
/// frontier's lock. Dropped before it [ended](Attempt::ended) or was left
/// [unanswered](Attempt::unanswered), it raises a harness fault.
pub struct Attempt<C: Clock> {
    line: Line,
    attempt: u32,
    record: Arc<Record>,
    captured: Vec<Captured>,
    first: u64,
    dispatched: u64,
    deadline: u64,
    stamps: Arc<Stamps<C>>,
    hold: Option<Hold>,
    done: bool,
    /// The last instant it knew its answer had not come (decision 17): its
    /// hand-off, its request's hand-over, a look that found the answer
    /// incomplete.
    absent: u64,
}

impl<C: Clock> Attempt<C> {
    pub(crate) fn new(
        stamps: Arc<Stamps<C>>,
        line: Line,
        attempt: u32,
        record: Arc<Record>,
        captured: Vec<Captured>,
        (first, dispatched): (u64, u64),
        deadline: u64,
    ) -> Attempt<C> {
        stamps.lock().0.enter();
        Attempt {
            line,
            attempt,
            record,
            captured,
            first,
            dispatched,
            deadline,
            stamps,
            hold: None,
            done: false,
            absent: dispatched,
        }
    }

    /// Its step's line.
    pub fn line(&self) -> Line {
        self.line
    }

    /// Which attempt, from 1.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Its step.
    pub fn record(&self) -> &Record {
        &self.record
    }

    /// Its step, shared: for a task that reports while it reads it.
    pub fn shared_record(&self) -> Arc<Record> {
        self.record.clone()
    }

    /// The values its step's producers captured.
    pub fn captured(&self) -> &[Captured] {
        &self.captured
    }

    /// When the machine dispatched it.
    pub fn dispatched(&self) -> u64 {
        self.dispatched
    }

    /// When the machine dispatched its step's first attempt: a call's frame
    /// is built from it, so a resend's is the same bytes.
    pub fn first_dispatched(&self) -> u64 {
        self.first
    }

    /// When the frontier censors it, unanswered.
    pub fn deadline(&self) -> u64 {
        self.deadline
    }

    /// The clock its reports are stamped by.
    pub fn clock(&self) -> &C {
        self.stamps.clock()
    }

    /// Its bytes left, now: pushed if before its deadline, once.
    pub fn sent(&self) {
        let (line, attempt) = (self.line, self.attempt);
        self.stamps.push(|f, now| f.sent(now, line, attempt));
    }

    /// Its task's first instant, now: the hand-off's lateness from the
    /// dispatch is the harness's (ADR-0047 decision 17). The loop stamps it
    /// as the task starts.
    pub(crate) fn handed(&mut self) {
        let (line, attempt) = (self.line, self.attempt);
        let (mut g, now) = self.stamps.lock();
        g.frontier.handed(now, line, attempt);
        self.absent = now;
    }

    /// Its request handed to a ready connection, now: the connect's lateness
    /// from `handed` is the stand's. The last such instant stands.
    pub fn connected(&mut self) {
        let (line, attempt) = (self.line, self.attempt);
        let (mut g, now) = self.stamps.lock();
        g.frontier.connected(now, line, attempt);
        self.absent = now;
    }

    /// A look found its answer incomplete; `at`, the clock read before it,
    /// the last instant it is known to have been absent (decision 17).
    pub fn absent_before(&mut self, at: u64) {
        self.absent = self.absent.max(at);
    }

    /// What reports its bytes left, for a task that sees them leave
    /// (a connection's, not the attempt's).
    pub(crate) fn sent_hook(&self) -> impl FnOnce() + Send + 'static {
        let (stamps, line, attempt) = (self.stamps.clone(), self.line, self.attempt);
        move || stamps.push(|f, now| f.sent(now, line, attempt))
    }

    /// Its answer arrived, now: a floor held there until it is judged.
    pub fn arrived(&mut self) {
        if self.hold.is_some() {
            return;
        }
        let (line, attempt) = (self.line, self.attempt);
        let held = {
            let (mut g, now) = self.stamps.lock();
            g.frontier.arrived(now, line, attempt)
        };
        match held {
            Ok(h) => self.hold = Some(h),
            Err(b) => self.stamps.fault(Harness::Below(b)),
        }
        self.stamps.notify();
    }

    /// Its answer judged: pushed at the instant it arrived — now, if it was
    /// not reported to have arrived.
    pub fn ended(mut self, judgement: Judgement, mut seen: Seen) {
        self.arrived();
        if let Some(hold) = self.hold.take() {
            // A late answer carries where it was not (decision 17): nothing
            // but its hold's lateness decides it.
            if hold.late() {
                seen.found = Some(Found {
                    absent: self.absent,
                    deadline: self.deadline,
                });
            }
            self.stamps.push(|f, _| f.ended(hold, judgement, seen));
        }
        self.done = true;
    }

    /// Nothing more comes of it: no answer, the frontier censoring it at its
    /// deadline.
    pub fn unanswered(mut self) {
        self.done = true;
    }

    /// Its carrier could not carry it: a harness fault, `cannot` a dotted
    /// name the journal holds, `detail` the caller's only.
    pub fn fault(mut self, cannot: &'static str, detail: String) {
        let (line, attempt) = (self.line, self.attempt);
        self.hold = None;
        self.done = true;
        self.stamps.fault(Harness::Carrier {
            line,
            attempt,
            cannot,
            detail,
        });
    }

    /// Spawns a task of its carrier's — a connection's — on the runtime it
    /// runs on, kept with the run's for a stop to abort (decision 18).
    pub fn spawn<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.stamps
            .spawn_on(&tokio::runtime::Handle::current(), task);
    }
}

impl<C: Clock> Drop for Attempt<C> {
    fn drop(&mut self) {
        let dropped = (!self.done || self.hold.is_some()).then_some(Harness::Dropped {
            line: self.line,
            attempt: self.attempt,
        });
        self.stamps.lock().0.leave(dropped);
        self.stamps.notify();
    }
}

impl<C: Clock> fmt::Debug for Attempt<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Its captured values may be secrets: never printed.
        f.debug_struct("Attempt")
            .field("line", &self.line)
            .field("attempt", &self.attempt)
            .field("dispatched", &self.dispatched)
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}
