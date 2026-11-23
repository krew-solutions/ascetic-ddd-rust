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
use std::sync::{Arc, Mutex, MutexGuard};

use ascetic_ddd_command_log::record::Record;
use ascetic_ddd_scheduler::frontier::{Below, Frontier, Hold};
use ascetic_ddd_scheduler::machine::{Captured, Judgement, Line, Seen};
use tokio::sync::Notify;

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
        /// What, never a value.
        what: String,
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
                what,
            } => write!(f, "attempt {attempt} of line {line}: {what}"),
            Harness::Held {
                line,
                attempt,
                since,
                at,
            } => write!(
                f,
                "attempt {attempt} of line {line} judged since {since} µs, still at {at} µs"
            ),
            Harness::Disordered { at, last } => {
                write!(f, "an input stamped {at} µs taken after one at {last} µs")
            }
        }
    }
}

impl std::error::Error for Harness {}

pub(crate) struct State {
    pub(crate) frontier: Frontier,
    fault: Option<Harness>,
    /// Attempt handles alive.
    alive: usize,
}

/// The frontier under its lock, the clock it stamps by, and the machine's
/// signal.
pub(crate) struct Stamps<C> {
    clock: Arc<C>,
    state: Mutex<State>,
    notify: Notify,
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
                g.fault.get_or_insert(Harness::Poisoned);
                g
            }
        };
        // The clock read under the lock: every push is in stamp order.
        let now = self.clock.now();
        (g, now)
    }

    /// Raises a fault (the first one stands) and wakes the machine.
    pub(crate) fn fault(&self, fault: Harness) {
        self.lock().0.fault.get_or_insert(fault);
        self.notify.notify_one();
    }

    /// The fault raised, if any.
    pub(crate) fn raised(&self) -> Option<Harness> {
        self.lock().0.fault.clone()
    }

    /// Attempt handles alive.
    pub(crate) fn alive(&self) -> usize {
        self.lock().0.alive
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
        stamps.lock().0.alive += 1;
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
    pub(crate) fn handed(&self) {
        let (line, attempt) = (self.line, self.attempt);
        let (mut g, now) = self.stamps.lock();
        g.frontier.handed(now, line, attempt);
    }

    /// Its request handed to a ready connection, now: the connect's lateness
    /// from `handed` is the stand's. The last such instant stands.
    pub fn connected(&self) {
        let (line, attempt) = (self.line, self.attempt);
        let (mut g, now) = self.stamps.lock();
        g.frontier.connected(now, line, attempt);
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
    pub fn ended(mut self, judgement: Judgement, seen: Seen) {
        self.arrived();
        if let Some(hold) = self.hold.take() {
            self.stamps.push(|f, _| f.ended(hold, judgement, seen));
        }
        self.done = true;
    }

    /// Nothing more comes of it: no answer, the frontier censoring it at its
    /// deadline.
    pub fn unanswered(mut self) {
        self.done = true;
    }

    /// Its carrier could not carry it: a harness fault, `what` naming no
    /// value.
    pub fn fault(mut self, what: String) {
        let (line, attempt) = (self.line, self.attempt);
        self.hold = None;
        self.done = true;
        self.stamps.fault(Harness::Carrier {
            line,
            attempt,
            what,
        });
    }
}

impl<C: Clock> Drop for Attempt<C> {
    fn drop(&mut self) {
        {
            let (mut g, _) = self.stamps.lock();
            g.alive = g.alive.saturating_sub(1);
        }
        if !self.done || self.hold.is_some() {
            self.stamps.fault(Harness::Dropped {
                line: self.line,
                attempt: self.attempt,
            });
        }
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
