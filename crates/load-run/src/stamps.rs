//! The frontier's lock and the attempts that push through it (ADR-0047
//! decision 16).
//!
//! Every input an attempt reports is a read of the clock and a push into the
//! [`Frontier`] under one lock: pushes arrive in the order of their stamps, and
//! the machine's drains, taken under the same lock, never release an input
//! below one still to come. A push below what was released, an attempt dropped
//! unended, a poisoned lock — each is a harness fault, raised here and taken by
//! the machine's loop, never the system's Unknown.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use ascetic_ddd_command_log::fills::Namespace;
use ascetic_ddd_command_log::record::Record;
use ascetic_ddd_scheduler::frontier::{Below, Frontier, Hold, Left};
use ascetic_ddd_scheduler::journal::{Code, FaultClass};
use ascetic_ddd_scheduler::machine::{
    self, Captured, Found, Judgement, Line, Refusal, Seen, Unsent,
};
use tokio::sync::Notify;
use tokio::task::{AbortHandle, JoinHandle};

use crate::clock::Clock;
use crate::io::Probe;

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
    /// The alive handles' deadlines, counted: the oldest outliving its
    /// deadline by the route timeout is a source saturated (decision 19).
    deadlines: BTreeMap<u64, usize>,
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

    /// The fault raised, or else how many attempt handles are alive and the
    /// earliest deadline among them: the count is never read without the
    /// fault (decision 18).
    pub(crate) fn alive(&self) -> Result<(usize, Option<u64>), Harness> {
        match &self.fault {
            Some(f) => Err(f.clone()),
            None => Ok((self.alive, self.deadlines.keys().next().copied())),
        }
    }

    fn enter(&mut self, deadline: u64) {
        self.alive = self.alive.saturating_add(1);
        let n = self.deadlines.entry(deadline).or_insert(0);
        *n = n.saturating_add(1);
    }

    /// An attempt's handle gone, raising its fault if it went unended, in one
    /// critical section with the count: none alive is never read without it
    /// (decision 18).
    fn leave(&mut self, deadline: u64, fault: Option<Harness>) {
        if let Some(f) = fault {
            self.raise(f);
        }
        self.alive = self.alive.saturating_sub(1);
        if let Some(n) = self.deadlines.get_mut(&deadline) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                self.deadlines.remove(&deadline);
            }
        }
    }
}

/// Where a send path stands (decision 18): off; on its attempt's task, from
/// `handed`; on its connection's task, from `connected`.
const OFF: u8 = 0;
const ATTEMPT: u8 = 1;
const CONNECTION: u8 = 2;

/// A send path's lag (decision 18): the scheduling lag of its wakes, summed
/// over the polls of the task that carries it — each from the later of the
/// task's last pending and the path's start on that task, at most since its
/// thread's last park that blocked. Its windows are disjoint inside
/// `[handed, Sent]`.
#[derive(Debug, Default)]
pub(crate) struct Path {
    on: AtomicU8,
    start: AtomicU64,
    lag: AtomicU64,
    /// Its task's current poll (decision 19): when it began, the window's
    /// start, and the lag it counted — a timer due within the window adds
    /// what the bound did not count.
    poll_at: AtomicU64,
    poll_since: AtomicU64,
    poll_counted: AtomicU64,
    /// The frontier pushed its `Sent` (decision 19): a censor of it is the
    /// stand's to answer for, no report owed.
    sent: AtomicBool,
}

impl Path {
    /// Moved to `on` at `at`: where it was.
    fn move_to(&self, on: u8, at: u64) -> u8 {
        self.start.store(at, Ordering::SeqCst);
        self.on.swap(on, Ordering::SeqCst)
    }

    /// Its lag so far, in microseconds.
    pub(crate) fn lag(&self) -> u64 {
        self.lag.load(Ordering::SeqCst)
    }

    /// A poll at `p` of the task `on`, pending since `w`, its thread idle
    /// since `u`.
    fn polled(&self, on: u8, p: u64, w: u64, u: u64) {
        if self.on.load(Ordering::SeqCst) == on {
            let since = w.max(self.start.load(Ordering::SeqCst));
            let lag = p.saturating_sub(since).min(p.saturating_sub(u));
            self.poll_at.store(p, Ordering::SeqCst);
            self.poll_since.store(since, Ordering::SeqCst);
            self.poll_counted.store(lag, Ordering::SeqCst);
            self.lag.fetch_add(lag, Ordering::SeqCst);
        }
    }

    /// A timer its attempt's task registered, due at `due`, found ready or
    /// dropped in the task's poll (decision 19): the task was runnable from
    /// it at the latest, so the poll's lag is at least `p − due` within its
    /// window — exact where the bound by the last blocked park is not (a
    /// whole process stalled ends in a park that blocked).
    fn woken(&self, due: u64) {
        if self.on.load(Ordering::SeqCst) != ATTEMPT {
            return;
        }
        let p = self.poll_at.load(Ordering::SeqCst);
        let since = self
            .poll_since
            .load(Ordering::SeqCst)
            .max(self.start.load(Ordering::SeqCst));
        let timer = p.saturating_sub(due.max(since));
        let counted = self.poll_counted.load(Ordering::SeqCst);
        if timer > counted {
            self.lag
                .fetch_add(timer.saturating_sub(counted), Ordering::SeqCst);
            self.poll_counted.store(timer, Ordering::SeqCst);
        }
    }
}

/// A sleep of an attempt's task, its due noted on its path once it was
/// registered and is found ready or dropped (decision 19).
pub(crate) struct Due<'a, F> {
    sleep: Pin<Box<F>>,
    due: u64,
    path: &'a Path,
    pended: bool,
    done: bool,
}

impl<F: Future<Output = ()>> Future for Due<'_, F> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        match this.sleep.as_mut().poll(cx) {
            Poll::Ready(()) => {
                this.done = true;
                if this.pended {
                    this.path.woken(this.due);
                }
                Poll::Ready(())
            }
            Poll::Pending => {
                this.pended = true;
                Poll::Pending
            }
        }
    }
}

impl<F> Drop for Due<'_, F> {
    fn drop(&mut self) {
        if self.pended && !self.done {
            self.path.woken(self.due);
        }
    }
}

/// What a task's polls feed.
pub(crate) enum Feeds {
    /// Nothing: a task of its own.
    Nothing,
    /// Its attempt's path, while on the attempt's task.
    Attempt(Arc<Path>),
    /// The path of whichever request its connection carries.
    Connection(Arc<Probe>),
}

/// A task timed at each poll (decision 18).
struct Timed<C, F> {
    clock: Arc<C>,
    feeds: Feeds,
    /// Its last poll's end: pending since.
    pending: u64,
    task: Pin<Box<F>>,
}

impl<C: Clock, F: Future<Output = ()>> Future for Timed<C, F> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let p = this.clock.now();
        let u = this.clock.idle_since();
        match &this.feeds {
            Feeds::Nothing => {}
            Feeds::Attempt(path) => path.polled(ATTEMPT, p, this.pending, u),
            Feeds::Connection(probe) => {
                if let Some(path) = probe.path() {
                    path.polled(CONNECTION, p, this.pending, u);
                }
            }
        }
        let done = this.task.as_mut().poll(cx);
        this.pending = this.clock.now();
        done
    }
}

/// Runs `f` on `on`'s thread and gives its value: a measurement before T0
/// (decision 18), no run's task; none if its runtime failed it.
pub(crate) async fn on_source<T, F>(on: &tokio::runtime::Handle, f: F) -> Option<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    on.spawn(async move { f() }).await.ok()
}

/// Runs `task` on `on`'s runtime before T0 (decision 20): a probe, no run's
/// task; its handle, to await or abort.
pub(crate) fn on_source_task<T, F>(on: &tokio::runtime::Handle, task: F) -> JoinHandle<T>
where
    T: Send + 'static,
    F: Future<Output = T> + Send + 'static,
{
    on.spawn(task)
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
    /// The run's namespace, its gates' (decision 20).
    namespace: Namespace,
    state: Mutex<State>,
    notify: Notify,
    tasks: Tasks,
}

impl<C: Clock> Stamps<C> {
    pub(crate) fn new(clock: Arc<C>, namespace: Namespace) -> Stamps<C> {
        Stamps {
            clock,
            namespace,
            state: Mutex::new(State {
                frontier: Frontier::new(),
                fault: None,
                alive: 0,
                deadlines: BTreeMap::new(),
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

    /// Spawns `task` on `on`, timed at each poll for what it `feeds`, kept
    /// for the stop to abort: the one place the shell spawns a task.
    pub(crate) fn spawn_on<F>(
        &self,
        on: &tokio::runtime::Handle,
        feeds: Feeds,
        task: F,
    ) -> AbortHandle
    where
        C: Clock,
        F: Future<Output = ()> + Send + 'static,
    {
        let timed = Timed {
            clock: self.clock.clone(),
            feeds,
            pending: self.clock.now(),
            task: Box::pin(task),
        };
        let handle = on.spawn(timed).abort_handle();
        self.tasks.keep(handle.clone());
        handle
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
    /// Its send path's lag (decision 18).
    path: Arc<Path>,
    /// When it was ready to connect, decorated, and handed to a connection:
    /// a never-sent attempt's report (decision 19).
    ready: Option<u64>,
    connected_at: Option<u64>,
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
        stamps.lock().0.enter(deadline);
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
            path: Arc::new(Path::default()),
            ready: None,
            connected_at: None,
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

    /// The run's namespace: the one its gates certified (decision 20).
    pub fn namespace(&self) -> Namespace {
        self.stamps.namespace
    }

    /// Its bytes left, now: pushed if before its deadline, once.
    pub fn sent(&self) {
        sent(&self.stamps, &self.path, self.line, self.attempt);
    }

    /// Its task's first instant, now: the hand-off's lateness from the
    /// dispatch is the harness's (ADR-0047 decision 17). The loop stamps it
    /// as the task starts, and the REST carrier again once the attempt is
    /// decorated: its send path's lag counted from the last.
    pub(crate) fn handed(&mut self) {
        let (line, attempt) = (self.line, self.attempt);
        let (mut g, now) = self.stamps.lock();
        g.frontier.handed(now, line, attempt);
        let _ = self.path.move_to(ATTEMPT, now);
        self.absent = now;
    }

    /// Its request handed to a ready connection, now: the connect's lateness
    /// from `handed` is the stand's, less its thread's lag. The last such
    /// instant stands; the path's lag goes on on the connection's task.
    pub fn connected(&mut self) {
        let (line, attempt) = (self.line, self.attempt);
        let was = {
            let (mut g, now) = self.stamps.lock();
            g.frontier.connected(now, line, attempt);
            self.absent = now;
            self.connected_at = Some(now);
            self.path.move_to(CONNECTION, now)
        };
        // Only a path on its attempt is handed to a connection: one handed
        // back is moved back first, or its lag would go uncounted.
        if was != ATTEMPT {
            self.stamps.fault(Harness::Carrier {
                line,
                attempt,
                cannot: "path.connected",
                detail: "connected while not on its attempt".to_owned(),
            });
        }
    }

    /// Ready to connect, its frame built and decorated (decision 19): its
    /// hand-off, the path's start — the lag before it, the decoration's, the
    /// hand-off's gap already (decision 19 resets it).
    pub(crate) fn decorated(&mut self) {
        self.handed();
        self.path.lag.store(0, Ordering::SeqCst);
        self.ready = Some(self.absent);
    }

    /// Whether the frontier pushed its `Sent`.
    pub fn was_sent(&self) -> bool {
        self.path.sent.load(Ordering::SeqCst)
    }

    /// Its closing report, never sent (decision 19): its path as it stands,
    /// the peer's refusals, where it stood — `code` a dotted name.
    pub fn report(&self, code: &str, refused: u32, refused_by: Option<Refusal>) -> Unsent {
        Unsent {
            // No gate yet between them (K2b-9c builds it).
            decorated: self.ready,
            handed: self.ready,
            connected: self.connected_at,
            lag: self.path.lag(),
            refused,
            refused_by,
            code: machine::Code::of(code),
        }
    }

    /// Censored and never sent, its closing report pushed (decision 19):
    /// after its deadline — at or before it, a fault.
    pub fn left(mut self, unsent: Unsent) {
        let (line, attempt) = (self.line, self.attempt);
        self.done = true;
        let refused = {
            let (mut g, now) = self.stamps.lock();
            g.frontier.left(now, line, attempt, unsent).err()
        };
        match refused {
            None => {}
            Some(Left::Below(b)) => self.stamps.fault(Harness::Below(b)),
            Some(Left::Early { at, deadline }) => self.stamps.fault(Harness::Carrier {
                line,
                attempt,
                cannot: "path.early_report",
                detail: format!("reported at {at} µs, its deadline {deadline} µs"),
            }),
        }
        self.stamps.notify();
    }

    /// Its request handed back unsent: the path's lag goes on on its own
    /// task, until the next connection takes it.
    pub(crate) fn back(&mut self) {
        let now = self.stamps.clock().now();
        let _ = self.path.move_to(ATTEMPT, now);
    }

    /// Its send path, for the connection that carries its request.
    pub(crate) fn path(&self) -> Arc<Path> {
        self.path.clone()
    }

    /// A look found its answer incomplete; `at`, the clock read before it,
    /// the last instant it is known to have been absent (decision 17).
    pub fn absent_before(&mut self, at: u64) {
        self.absent = self.absent.max(at);
    }

    /// What reports its bytes left, for a task that sees them leave
    /// (a connection's, not the attempt's).
    pub(crate) fn sent_hook(&self) -> impl FnOnce() + Send + 'static {
        let (stamps, path) = (self.stamps.clone(), self.path.clone());
        let (line, attempt) = (self.line, self.attempt);
        move || sent(&stamps, &path, line, attempt)
    }

    /// Its answer arrived, now: a floor held there until it is judged.
    pub fn arrived(&mut self) {
        if self.hold.is_some() {
            return;
        }
        let (line, attempt) = (self.line, self.attempt);
        let held = {
            let (mut g, now) = self.stamps.lock();
            // An early reply's `Sent`, made at its end, carries the lag so
            // far.
            g.frontier.lag(line, attempt, self.path.lag());
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
                    handed: self.ready,
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

    /// Spawns a task of its carrier's on the runtime it runs on, kept with
    /// the run's for a stop to abort (decision 18).
    pub fn spawn<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.stamps
            .spawn_on(&tokio::runtime::Handle::current(), Feeds::Nothing, task);
    }

    /// Spawns a connection's task, its polls feeding the path of whichever
    /// request it carries (decision 18): its handle, for the attempt that
    /// resets it (decision 19).
    pub(crate) fn spawn_connection<F>(&self, probe: Arc<Probe>, task: F) -> AbortHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.stamps.spawn_on(
            &tokio::runtime::Handle::current(),
            Feeds::Connection(probe),
            task,
        )
    }

    /// A sleep of its task until `at`, its due noted on its send path
    /// (decision 19).
    pub(crate) fn sleep_until(&self, at: u64) -> impl Future<Output = ()> + Send + '_ {
        Due {
            sleep: Box::pin(self.clock().sleep_until(at)),
            due: at,
            path: &self.path,
            pended: false,
            done: false,
        }
    }

    /// Its send path's lag so far.
    pub(crate) fn lag(&self) -> u64 {
        self.path.lag()
    }

    /// It waited, from `from`, its path's lag then `before`, for a place at
    /// its source's cap (decision 19): the harness's, the whole wait on its
    /// path's lag — the polls inside it counted no more than the wait.
    pub(crate) fn waited(&self, from: u64, before: u64) {
        let wait = self.clock().now().saturating_sub(from);
        self.path
            .lag
            .fetch_max(before.saturating_add(wait), Ordering::SeqCst);
    }

    /// Whether its request's bytes may still leave: before its deadline
    /// (decision 19) — a connection holding one at it gives it no byte.
    pub(crate) fn sending(&self) -> Box<dyn Fn() -> bool + Send> {
        let (stamps, deadline) = (self.stamps.clone(), self.deadline);
        Box::new(move || stamps.clock().now() < deadline)
    }
}

/// Its bytes left, now: its path's lag with it, pushed if before its
/// deadline, once; the path off.
fn sent<C: Clock>(stamps: &Stamps<C>, path: &Path, line: Line, attempt: u32) {
    stamps.push(|f, now| {
        f.lag(line, attempt, path.lag());
        let _ = path.move_to(OFF, now);
        let pushed = f.sent(now, line, attempt)?;
        if pushed {
            path.sent.store(true, Ordering::SeqCst);
        }
        Ok(())
    });
}

impl<C: Clock> Drop for Attempt<C> {
    fn drop(&mut self) {
        let dropped = (!self.done || self.hold.is_some()).then_some(Harness::Dropped {
            line: self.line,
            attempt: self.attempt,
        });
        self.stamps.lock().0.leave(self.deadline, dropped);
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

#[cfg(test)]
mod tests {
    use super::{ATTEMPT, CONNECTION, OFF, Path};

    /// A poll's lag (decision 18): from the later of the task's last pending
    /// and the path's start on it, at most since its thread last blocked;
    /// none for a task the path is not on.
    #[test]
    fn a_poll_s_lag() {
        let path = Path::default();
        let _ = path.move_to(ATTEMPT, 100);
        // Pending since before the path's start: from the start.
        path.polled(ATTEMPT, 150, 0, 0);
        assert_eq!(path.lag(), 50);
        // The thread blocked since: from then.
        path.polled(ATTEMPT, 300, 200, 290);
        assert_eq!(path.lag(), 60);
        // A connection's poll while the path is on its attempt: nothing.
        path.polled(CONNECTION, 400, 300, 0);
        assert_eq!(path.lag(), 60);
        assert_eq!(path.move_to(CONNECTION, 400), ATTEMPT);
        path.polled(CONNECTION, 450, 0, 0);
        assert_eq!(path.lag(), 110);
        assert_eq!(path.move_to(OFF, 450), CONNECTION);
        path.polled(CONNECTION, 900, 0, 0);
        assert_eq!(path.lag(), 110);
    }

    /// A timer registered on the attempt's task, due within its poll's
    /// window, makes the poll's lag at least `p − due` (decision 19): beyond
    /// what the bound counted, once, never before the window; nothing on a
    /// connection's task, nor for a timer due after the poll began.
    #[test]
    fn a_timer_s_due_is_exact() {
        let path = Path::default();
        let _ = path.move_to(ATTEMPT, 100);
        // Pending since 50, the thread blocked until 990: the bound 10.
        path.polled(ATTEMPT, 1_000, 50, 990);
        assert_eq!(path.lag(), 10);
        // A timer due at 200: the task runnable from it — 800 in all.
        path.woken(200);
        assert_eq!(path.lag(), 800);
        // A later one, already counted; one due after the poll began.
        path.woken(500);
        path.woken(1_500);
        assert_eq!(path.lag(), 800);
        // Due before the path's start: from its start.
        let fresh = Path::default();
        let _ = fresh.move_to(ATTEMPT, 400);
        fresh.polled(ATTEMPT, 1_000, 0, 1_000);
        fresh.woken(0);
        assert_eq!(fresh.lag(), 600);
        // Re-handed in the poll, at 800: nothing before its new start.
        let handed = Path::default();
        let _ = handed.move_to(ATTEMPT, 0);
        handed.polled(ATTEMPT, 1_000, 0, 1_000);
        let _ = handed.move_to(ATTEMPT, 800);
        handed.woken(100);
        assert_eq!(handed.lag(), 200);
        // On its connection: the attempt's timers are none of its.
        assert_eq!(path.move_to(CONNECTION, 1_000), ATTEMPT);
        // Its poll's window from 1,500, the bound 10.
        path.polled(CONNECTION, 3_000, 1_500, 2_990);
        path.woken(1_000);
        assert_eq!(path.lag(), 810);
    }
}
