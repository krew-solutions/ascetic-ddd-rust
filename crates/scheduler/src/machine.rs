//! The dependency machine (ADR-0047 decisions 2–8): lines in as they are read,
//! sends and answers in as their handlers saw them, and out what to send, when
//! to wake, how far to read, and each step's end.
//!
//! A dependency is bound when its line is read, through the validator's own
//! window. A step is released at `intended = max(at, ready + think)` and not
//! before it is read; `ready` is when the last of its dependencies was met:
//! `Sent` at the dependency's first send, `Settled` at its end, `Expected` at
//! an expected end — or, for a dependency that ended otherwise, never: the
//! step is skipped. A step that is never sent meets `Sent` and `Settled` at its
//! end. A barrier waits for the members read before it, by prefix.
//!
//! A step's cell is freed once it ended, the window let it go, no reader of
//! its captured value nor observation of it as a cause is still to be
//! dispatched, and every prefix of its barrier key passed it
//! (verify/tla/Scheduler.tla): nothing else reads an ended step's cell. A
//! dependent folds a met instant in when it is told; a holder takes its key's
//! latest met instant from the barrier; a prefix of members expected stops
//! for good at a member that failed it; a reader takes its captured values
//! when first dispatched, and resends them.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use ascetic_ddd_command_log::LogError;
use ascetic_ddd_command_log::record::{
    Action, Dep, Header, Hole, Name, On, Probe, Record, Safety, Subject,
};
use ascetic_ddd_command_log::validate::holes;
use ascetic_ddd_command_log::window::{Window, digest};

/// A line of the log: the header's is 1, the first step's 2.
pub type Line = u64;

/// A value a step's answer gave for a slot of a subject (decision 5).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Captured {
    /// Whose.
    pub subject: Subject,
    /// Which.
    pub slot: Name,
    /// The value, as the answer gave it.
    pub value: Vec<u8>,
}

/// How an attempt ended, as its handler judged it (decision 6).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Judgement {
    /// An outcome `expect` lists — for an observation, `until` holds — with
    /// the values it captured.
    Expected(Vec<Captured>),
    /// An answer the contract defines that `expect` does not list — for an
    /// observation, not yet.
    Unexpected,
    /// A refusal a problem code the operation lists certifies as not run.
    NotTaken,
    /// No certain outcome by the deadline: no answer, a reset after the request
    /// was written, a gateway's own reply.
    Unknown,
}

/// What comes in, in the order of the instants their sources saw them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Input {
    /// A line read.
    Line {
        /// Its number.
        line: Line,
        /// The step.
        record: Arc<Record>,
    },
    /// An attempt's bytes left.
    Sent {
        /// The step's line.
        line: Line,
        /// The attempt, from 1.
        attempt: u32,
    },
    /// An attempt ended.
    Ended {
        /// The step's line.
        line: Line,
        /// The attempt, from 1.
        attempt: u32,
        /// How.
        judgement: Judgement,
    },
    /// Time passed.
    Wake,
}

impl Input {
    /// The rank of a kind among inputs of one instant and line: a line is read
    /// before its sends, a send before its answer.
    pub fn kind(&self) -> u8 {
        match self {
            Input::Line { .. } => 0,
            Input::Sent { .. } => 1,
            Input::Ended { .. } => 2,
            Input::Wake => 3,
        }
    }

    /// The line it is about; a wake is about none.
    pub fn line(&self) -> Line {
        match self {
            Input::Line { line, .. } | Input::Sent { line, .. } | Input::Ended { line, .. } => {
                *line
            }
            Input::Wake => 0,
        }
    }
}

/// Why a step was not sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Skip {
    /// A dependency awaited as expected that ended otherwise.
    Dependency(Line),
    /// A barrier's member awaited as expected that ended otherwise.
    Member(Line),
    /// A captured value its producer's answer did not give.
    MissingCapture(Line),
}

/// How a step ended (decision 6).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum End {
    /// As expected.
    Expected,
    /// With an answer `expect` does not list.
    Unexpected,
    /// With no certain outcome by its deadline.
    Unknown,
    /// Never sent.
    Skipped(Skip),
}

/// The dependency whose met instant made a step ready.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Critical {
    /// A step dependency's line.
    Step(Line),
    /// A barrier member's line.
    Member(Line),
}

/// A step's journal entry: what happened to it, and when.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// Its line.
    pub line: Line,
    /// How it ended.
    pub end: End,
    /// When it was read.
    pub read: u64,
    /// When its dependencies were met: 0, if it has none.
    pub ready: u64,
    /// `max(at, ready + think)`.
    pub intended: u64,
    /// `max(intended, read)`.
    pub released: u64,
    /// When its first attempt was dispatched.
    pub dispatched: Option<u64>,
    /// When its first attempt's bytes left.
    pub sent: Option<u64>,
    /// When it ended.
    pub ended: u64,
    /// Attempts dispatched.
    pub attempts: u32,
    /// The dependency that made it ready last.
    pub critical: Option<Critical>,
    /// The lines its step dependencies bound, in its `after`'s order.
    pub bound: Vec<Line>,
    /// Whether it ended only at the machine's backstop: a harness fault.
    pub fault: bool,
}

/// What the machine asks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Output {
    /// Send attempt `attempt` of `line` now, its captured holes filled from
    /// `captured`; its handler ends it by `deadline`.
    Dispatch {
        /// The step's line.
        line: Line,
        /// The attempt, from 1.
        attempt: u32,
        /// The values its captured holes take.
        captured: Vec<Captured>,
        /// When its handler gives up on it.
        deadline: u64,
    },
    /// Wake me at this instant.
    WakeAt(u64),
    /// Read the lines planned up to this instant.
    Want(u64),
    /// A step ended.
    Ended(Box<Entry>),
}

/// How long past a deadline the machine waits for a handler that does not
/// report: then the step ends unknown, and it is the harness's fault.
pub const GRACE: u64 = 1_000_000;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Phase {
    /// Waiting for dependencies.
    Waiting,
    /// Released, to be dispatched at its instant.
    Due,
    /// An attempt out; `outstanding` while it has not ended.
    Flight { attempt: u32, outstanding: bool },
    /// Ended.
    Done,
}

#[derive(Clone, Debug)]
struct Cell {
    /// Its step, until it ends: nothing reads an ended step's record.
    record: Option<Arc<Record>>,
    /// Its plan and its think.
    at: u64,
    think: u64,
    read: u64,
    waiting: u32,
    ready: u64,
    critical: Option<Critical>,
    bound: Vec<Line>,
    /// The producers of its captured holes, bound at read.
    producers: Vec<(Subject, Name, Line)>,
    /// An observation's cause's line; its own if it is its own.
    cause: Option<Line>,
    phase: Phase,
    intended: u64,
    released: u64,
    deadline: u64,
    dispatched: Option<u64>,
    sent: Option<u64>,
    end: Option<(End, u64)>,
    attempts: u32,
    captures: Vec<Captured>,
    /// Dependents bound to it, awaiting a strength not yet met.
    waiters: Vec<(Line, On)>,
    /// Its barrier key and ordinal, if it is a member of one.
    member: Option<(BarrierKey, usize)>,
    fault: bool,
    /// Whether the window still keeps it, for dependencies read later.
    in_window: bool,
    /// Readers of its captured values, and observations of it as their
    /// cause, still to be dispatched.
    refs: u32,
    /// Whether it still holds its producers and its cause.
    holding: bool,
    /// The values its captured holes took when its release was decided.
    captured: Vec<Captured>,
    /// The producer whose value was missing then: it is skipped for it when
    /// dispatched.
    missing: Option<Line>,
    /// Its one pending wake.
    wake: Option<u64>,
}

type BarrierKey = ascetic_ddd_command_log::plan::MemberKey;

#[derive(Clone, Debug, Default)]
struct Barrier {
    /// The members some prefix has not passed yet, from ordinal `base`.
    members: VecDeque<Line>,
    base: usize,
    /// The members read.
    count: usize,
    /// Members up to here met the strength, by `On` rank; the latest instant
    /// among them and the member that met it last.
    prefix: [usize; 3],
    latest: [(u64, Line); 3],
    /// The lowest ordinal of a member that ended otherwise than expected:
    /// the prefix of members expected stops there for good.
    failed: Option<usize>,
    /// The first such member in time, and its end: what a holder read later
    /// is skipped for, and when.
    first_failure: Option<(Line, u64)>,
    /// Holders waiting, by strength rank, by the members read before them.
    holders: [BTreeMap<usize, Vec<Line>>; 3],
}

fn rank(on: On) -> usize {
    match on {
        On::Sent => 0,
        On::Settled => 1,
        On::Expected => 2,
    }
}

/// The machine.
#[derive(Debug)]
pub struct Machine {
    header: Arc<Header>,
    read_ahead: u64,
    window: Window<Line>,
    cells: BTreeMap<Line, Cell>,
    barriers: BTreeMap<BarrierKey, Barrier>,
    now: u64,
    wakes: BTreeSet<(u64, Line)>,
    asked: Option<u64>,
    wanted: Option<u64>,
}

impl Machine {
    /// A machine for a log of this header, reading `read_ahead` microseconds
    /// ahead of its clock: no further than the header plans, the read-ahead
    /// the plan's own bound assumes (decision 7, K1e-2).
    pub fn new(header: Arc<Header>, read_ahead: u64) -> Result<Machine, LogError> {
        if read_ahead > header.read_ahead {
            return Err(LogError::Malformed {
                line: 1,
                reason: format!(
                    "a read-ahead of {read_ahead} µs past the header's {}: the plan's bound assumes it",
                    header.read_ahead
                ),
            });
        }
        Ok(Machine::unbounded(header, read_ahead))
    }

    /// [`Machine::new`] at any read-ahead: the deterministic stand's, which
    /// runs past the header's to show that bindings do not depend on it.
    pub(crate) fn unbounded(header: Arc<Header>, read_ahead: u64) -> Machine {
        let window = Window::new(header.window);
        Machine {
            header,
            read_ahead,
            window,
            cells: BTreeMap::new(),
            barriers: BTreeMap::new(),
            now: 0,
            wakes: BTreeSet::new(),
            asked: None,
            wanted: None,
        }
    }

    /// The machine's clock: the latest input's instant.
    pub fn now(&self) -> u64 {
        self.now
    }

    /// The steps it holds: read and not freed.
    pub fn held(&self) -> usize {
        self.cells.len()
    }

    /// The barrier keys it keeps: one a key a member was read of.
    pub fn barriers(&self) -> usize {
        self.barriers.len()
    }

    /// The steps the window keeps: within `W` behind the cursor, or retained.
    pub fn kept(&self) -> usize {
        self.window.len()
    }

    /// Takes one input stamped `at`, no earlier than the last one's.
    pub fn step(&mut self, at: u64, input: Input) -> Result<Vec<Output>, LogError> {
        let mut out = Vec::new();
        self.now = self.now.max(at);
        match input {
            Input::Line { line, record } => self.read(line, record, &mut out)?,
            Input::Sent { line, attempt } => self.sent(line, attempt, &mut out),
            Input::Ended {
                line,
                attempt,
                judgement,
            } => self.ended(line, attempt, judgement, &mut out),
            Input::Wake => {}
        }
        self.due(&mut out);
        let want = self.now.saturating_add(self.read_ahead);
        if self.wanted.is_none_or(|w| want > w) {
            self.wanted = Some(want);
            out.push(Output::Want(want));
        }
        if let Some(&(t, _)) = self.wakes.first()
            && self.asked != Some(t)
        {
            self.asked = Some(t);
            out.push(Output::WakeAt(t));
        }
        Ok(out)
    }

    fn read(
        &mut self,
        line: Line,
        record: Arc<Record>,
        out: &mut Vec<Output>,
    ) -> Result<(), LogError> {
        let mut gone = Vec::new();
        self.window.read(record.at, |l| gone.push(l));
        self.forget(gone);
        let mut cell = Cell {
            record: Some(record.clone()),
            at: record.at,
            think: record.think,
            read: self.now,
            waiting: 0,
            // With no dependency it is ready from the start: a late reader's
            // gap is the reader's, not its dependencies'.
            ready: 0,
            critical: None,
            bound: Vec::new(),
            producers: Vec::new(),
            cause: None,
            phase: Phase::Waiting,
            intended: 0,
            released: 0,
            deadline: 0,
            dispatched: None,
            sent: None,
            end: None,
            attempts: 0,
            captures: Vec::new(),
            waiters: Vec::new(),
            member: None,
            fault: false,
            in_window: false,
            refs: 0,
            holding: false,
            captured: Vec::new(),
            missing: None,
            wake: None,
        };
        // The ready instant is the latest met instant of its dependencies;
        // with none, 0.
        let mut ready: Option<(u64, Option<Critical>)> = None;
        let mut skip: Option<(Skip, u64)> = None;
        let mut pending: Vec<(Line, On)> = Vec::new();
        for d in &record.after {
            match d {
                Dep::Step { step, on } => {
                    let Some(&(_, dep)) = self.window.get(&digest(step)?) else {
                        // A committed log binds every dependency (decision 2).
                        return Err(LogError::Malformed {
                            line,
                            reason: format!(
                                "{step:?} is bound by no step: the log was not validated"
                            ),
                        });
                    };
                    cell.bound.push(dep);
                    match self.met(dep, *on) {
                        Met::Yes(t) => fold(&mut ready, t, Critical::Step(dep)),
                        Met::Never(t) => earliest(&mut skip, Skip::Dependency(dep), t),
                        Met::Not => pending.push((dep, *on)),
                    }
                }
                Dep::Each {
                    kind,
                    scope,
                    label,
                    n,
                    on,
                    ..
                } => {
                    // A key no member was read of: nothing to wait for, and
                    // no barrier kept for it.
                    let Some(b) =
                        self.barriers
                            .get_mut(&ascetic_ddd_command_log::plan::barrier_key(
                                kind, scope, label, *n,
                            ))
                    else {
                        continue;
                    };
                    let members = b.count;
                    let r = rank(*on);
                    match b.first_failure {
                        // Every member that failed was read before it.
                        Some((first, t)) if *on == On::Expected => {
                            earliest(&mut skip, Skip::Member(first), t);
                        }
                        _ if b.prefix[r] >= members => {
                            // Every member read met it: the latest of them, from
                            // the barrier, not their cells.
                            if members > 0 {
                                let (t, m) = b.latest[r];
                                fold(&mut ready, t, Critical::Member(m));
                            }
                        }
                        _ => {
                            b.holders[r].entry(members).or_default().push(line);
                            cell.waiting = cell.waiting.saturating_add(1);
                        }
                    }
                }
            }
        }
        for (dep, on) in pending {
            cell.waiting = cell.waiting.saturating_add(1);
            if let Some(d) = self.cells.get_mut(&dep) {
                d.waiters.push((line, on));
            }
        }
        if let Some((t, critical)) = ready {
            cell.ready = t;
            cell.critical = critical;
        }
        // The producers of its captured holes, and an observation's cause.
        for h in holes(&record) {
            if let Hole::Captured {
                subject,
                slot,
                from,
            } = h
                && let Some(&(_, producer)) = self.window.get(&digest(from)?)
            {
                cell.producers
                    .push((subject.clone(), slot.clone(), producer));
            }
        }
        if let Action::Observe(o) = &record.action {
            cell.cause = if o.cause == record.step {
                Some(line)
            } else {
                self.window.get(&digest(&o.cause)?).map(|e| e.1)
            };
        }
        // It is a member of its own barrier key, for the holders read later.
        let key = ascetic_ddd_command_log::plan::member_key(&record.step);
        let b = self.barriers.entry(key.clone()).or_default();
        cell.member = Some((key, b.count));
        b.members.push_back(line);
        b.count = b.count.saturating_add(1);
        let retained = self.header.retained.contains(&record.step.label);
        let id = digest(&record.step)?;
        let mut gone = Vec::new();
        self.window
            .keep(id, record.at, retained, line, |l| gone.push(l));
        cell.in_window = self.window.get(&id).is_some_and(|e| e.1 == line);
        // It holds its producers and its cause until it is first dispatched.
        let mut held: Vec<Line> = cell.producers.iter().map(|p| p.2).collect();
        held.extend(cell.cause.filter(|c| *c != line));
        for p in &held {
            if let Some(c) = self.cells.get_mut(p) {
                c.refs = c.refs.saturating_add(1);
            }
        }
        cell.holding = !held.is_empty();
        self.cells.insert(line, cell);
        self.forget(gone);
        match skip {
            Some((why, t)) => self.finish(line, End::Skipped(why), t.max(self.now), out),
            None => self.ready_if_met(line),
        }
        Ok(())
    }

    /// Whether `dep` has met `on`: at an instant, never, or not yet.
    fn met(&self, dep: Line, on: On) -> Met {
        let Some(c) = self.cells.get(&dep) else {
            return Met::Not;
        };
        match (on, &c.end) {
            (On::Sent, _) if c.sent.is_some() => Met::Yes(c.sent.unwrap_or(0)),
            (On::Sent | On::Settled, Some((_, t))) => Met::Yes(*t),
            (On::Expected, Some((End::Expected, t))) => Met::Yes(*t),
            (On::Expected, Some((_, t))) => Met::Never(*t),
            _ => Met::Not,
        }
    }

    /// A step whose dependencies are all met is released.
    fn ready_if_met(&mut self, line: Line) {
        let Some(c) = self.cells.get_mut(&line) else {
            return;
        };
        if c.waiting > 0 || c.phase != Phase::Waiting {
            return;
        }
        c.intended = ascetic_ddd_command_log::plan::intended(c.at, c.ready, c.think);
        c.released = c.intended.max(c.read);
        c.phase = Phase::Due;
        let t = c.released;
        self.decided(line);
        self.wake(line, t);
    }

    /// Its release decided: the values its captured holes take, from their
    /// producers' cells, and its deadline, from its cause's intended start —
    /// then it lets go of them, so that a wake taken late holds nothing.
    fn decided(&mut self, line: Line) {
        let Some(c) = self.cells.get(&line) else {
            return;
        };
        let mut captured = Vec::new();
        let mut missing = None;
        for (subject, slot, producer) in &c.producers {
            let value = self.cells.get(producer).and_then(|p| {
                p.captures
                    .iter()
                    .find(|v| &v.subject == subject && &v.slot == slot)
            });
            match value {
                Some(v) => captured.push(v.clone()),
                None => {
                    missing = Some(*producer);
                    break;
                }
            }
        }
        let deadline = self.deadline(line);
        if let Some(c) = self.cells.get_mut(&line) {
            c.captured = captured;
            c.missing = missing;
            c.deadline = deadline;
        }
        self.release(line);
    }

    /// The line's one pending wake, at `t`.
    fn wake(&mut self, line: Line, t: u64) {
        let Some(c) = self.cells.get_mut(&line) else {
            return;
        };
        if let Some(old) = c.wake.replace(t) {
            self.wakes.remove(&(old, line));
        }
        self.wakes.insert((t, line));
    }

    /// Dispatches what is due, polls on their grid, ends what passed its
    /// backstop.
    fn due(&mut self, out: &mut Vec<Output>) {
        while let Some(&(t, line)) = self.wakes.first() {
            if t > self.now {
                break;
            }
            self.wakes.pop_first();
            let Some(c) = self.cells.get_mut(&line) else {
                continue;
            };
            if c.wake != Some(t) {
                continue;
            }
            c.wake = None;
            match c.phase.clone() {
                Phase::Due => self.dispatch(line, 1, out),
                Phase::Flight {
                    attempt,
                    outstanding,
                } => {
                    let deadline = c.deadline;
                    if outstanding && t >= deadline.saturating_add(GRACE) {
                        if let Some(c) = self.cells.get_mut(&line) {
                            c.fault = true;
                        }
                        self.finish(line, End::Unknown, self.now, out);
                    } else if !outstanding && t >= deadline {
                        // An observation not yet satisfied by its deadline.
                        self.finish(line, End::Unknown, deadline, out);
                    } else if !outstanding {
                        self.dispatch(line, attempt.saturating_add(1), out);
                    }
                }
                Phase::Waiting | Phase::Done => {}
            }
        }
    }

    fn dispatch(&mut self, line: Line, attempt: u32, out: &mut Vec<Output>) {
        let now = self.now;
        let Some(c) = self.cells.get(&line) else {
            return;
        };
        // The values taken when its release was decided; a resend sends
        // them again.
        if let Some(producer) = c.missing {
            self.finish(line, End::Skipped(Skip::MissingCapture(producer)), now, out);
            return;
        }
        let captured = c.captured.clone();
        let deadline = c.deadline;
        // Released past its deadline — an observation whose cause's window
        // closed before it could start — it ends unknown, never sent.
        if attempt == 1 && now >= deadline {
            self.finish(line, End::Unknown, now, out);
            return;
        }
        let Some(c) = self.cells.get_mut(&line) else {
            return;
        };
        c.deadline = deadline;
        c.attempts = attempt;
        c.dispatched.get_or_insert(now);
        c.phase = Phase::Flight {
            attempt,
            outstanding: true,
        };
        self.wake(line, deadline.saturating_add(GRACE));
        out.push(Output::Dispatch {
            line,
            attempt,
            captured,
            deadline,
        });
    }

    /// A step's deadline (decision 7): a call's from its release, by its
    /// operation, channel or host call; an observation's from its cause's
    /// intended start.
    fn deadline(&self, line: Line) -> u64 {
        let Some(c) = self.cells.get(&line) else {
            return self.now;
        };
        let Some(record) = c.record.as_ref() else {
            return self.now;
        };
        let timeout = ascetic_ddd_command_log::plan::timeout(&self.header, &record.action);
        let start = match &record.action {
            Action::Observe(_) => c
                .cause
                .and_then(|l| self.cells.get(&l))
                .map_or(c.intended, |cause| cause.intended),
            _ => c.released,
        };
        start.saturating_add(timeout)
    }

    fn sent(&mut self, line: Line, attempt: u32, out: &mut Vec<Output>) {
        let now = self.now;
        let Some(c) = self.cells.get_mut(&line) else {
            return;
        };
        if c.sent.is_some() || c.end.is_some() || c.attempts != attempt {
            return;
        }
        c.sent = Some(now);
        self.notify(line, out);
    }

    fn ended(&mut self, line: Line, attempt: u32, judgement: Judgement, out: &mut Vec<Output>) {
        let now = self.now;
        let Some(c) = self.cells.get_mut(&line) else {
            return;
        };
        // A late answer, or one of an attempt no longer out, changes nothing.
        if c.end.is_some() || c.attempts != attempt || now > c.deadline {
            return;
        }
        // A poll's interval, if it is one: an observation probing by REST.
        let every = match c.record.as_ref().map(|r| &r.action) {
            Some(Action::Observe(o)) if matches!(o.probe, Probe::Rest(_)) => Some(o.every.max(1)),
            _ => None,
        };
        let poll = every.is_some();
        let safety = self.safety(line);
        let Some(c) = self.cells.get_mut(&line) else {
            return;
        };
        match judgement {
            Judgement::Expected(captures) => {
                c.captures = captures;
                self.finish(line, End::Expected, now, out);
            }
            Judgement::Unexpected | Judgement::NotTaken if poll => {
                // Not yet: the next poll on its grid, never two out at once.
                let every = every.unwrap_or(1);
                let since = now.saturating_sub(c.released);
                let k = since.div_ceil(every).max(u64::from(attempt));
                let next = c.released.saturating_add(k.saturating_mul(every));
                c.phase = Phase::Flight {
                    attempt,
                    outstanding: false,
                };
                let deadline = c.deadline;
                self.wake(line, next.min(deadline));
            }
            Judgement::NotTaken if safety != Safety::Never && now < c.deadline => {
                self.dispatch(line, attempt.saturating_add(1), out);
            }
            Judgement::Unknown if safety == Safety::Idempotent && !poll && now < c.deadline => {
                self.dispatch(line, attempt.saturating_add(1), out);
            }
            Judgement::Unexpected | Judgement::NotTaken => {
                self.finish(line, End::Unexpected, now, out);
            }
            Judgement::Unknown => self.finish(line, End::Unknown, now, out),
        }
    }

    fn safety(&self, line: Line) -> Safety {
        self.cells
            .get(&line)
            .and_then(|c| c.record.as_ref())
            .map_or(Safety::Never, |r| {
                ascetic_ddd_command_log::plan::safety(&self.header, &r.action)
            })
    }

    /// A step ends: journaled, and its dependents and barriers told.
    fn finish(&mut self, line: Line, end: End, at: u64, out: &mut Vec<Output>) {
        let Some(c) = self.cells.get_mut(&line) else {
            return;
        };
        if c.end.is_some() {
            return;
        }
        if c.phase == Phase::Waiting {
            // Never released: its instants are its end's.
            c.intended = at;
            c.released = at;
        }
        c.end = Some((end.clone(), at));
        c.phase = Phase::Done;
        if let Some(t) = c.wake.take() {
            self.wakes.remove(&(t, line));
        }
        out.push(Output::Ended(Box::new(Entry {
            line,
            end,
            read: c.read,
            ready: c.ready,
            intended: c.intended,
            released: c.released,
            dispatched: c.dispatched,
            sent: c.sent,
            ended: at,
            attempts: c.attempts,
            critical: c.critical,
            bound: c.bound.clone(),
            fault: c.fault,
        })));
        // It lets go of its producers before its barrier may free it.
        self.release(line);
        self.notify(line, out);
        // What only its run needed goes now (K1e-2): a cell still held after
        // its end — in the window, pinned, unpassed — keeps what is read of
        // an ended step, its end, sends, captures, intended start and key.
        if let Some(c) = self.cells.get_mut(&line) {
            c.record = None;
            c.bound = Vec::new();
            c.producers = Vec::new();
            c.captured = Vec::new();
        }
        self.free_if_done(line);
    }

    /// A step lets go of its producers and its cause: its captured values are
    /// taken, its deadline decided, or it ended unsent.
    fn release(&mut self, line: Line) {
        let held = match self.cells.get_mut(&line) {
            Some(c) if c.holding => {
                c.holding = false;
                let mut held: Vec<Line> = c.producers.iter().map(|p| p.2).collect();
                held.extend(c.cause.filter(|x| *x != line));
                held
            }
            _ => return,
        };
        for p in held {
            if let Some(c) = self.cells.get_mut(&p) {
                c.refs = c.refs.saturating_sub(1);
            }
            self.free_if_done(p);
        }
    }

    /// The lines the window let go: no dependency read later binds them.
    fn forget(&mut self, gone: Vec<Line>) {
        for l in gone {
            if let Some(c) = self.cells.get_mut(&l) {
                c.in_window = false;
            }
            self.free_if_done(l);
        }
    }

    /// Frees a step's cell once nothing can read it again.
    fn free_if_done(&mut self, line: Line) {
        let Some(c) = self.cells.get(&line) else {
            return;
        };
        let passed = c
            .member
            .as_ref()
            .is_none_or(|(key, ord)| self.barriers.get(key).is_none_or(|b| *ord < b.base));
        if c.end.is_some() && !c.holding && !c.in_window && c.refs == 0 && passed {
            self.cells.remove(&line);
        }
    }

    /// Tells a step's waiters and its barrier what it has now met.
    fn notify(&mut self, line: Line, out: &mut Vec<Output>) {
        let waiters = match self.cells.get_mut(&line) {
            Some(c) => std::mem::take(&mut c.waiters),
            None => return,
        };
        let mut still = Vec::new();
        for (dependent, on) in waiters {
            match self.met(line, on) {
                Met::Yes(t) => {
                    if let Some(d) = self.cells.get_mut(&dependent) {
                        d.waiting = d.waiting.saturating_sub(1);
                        if t >= d.ready || d.critical.is_none() {
                            d.ready = d.ready.max(t);
                            d.critical = Some(Critical::Step(line));
                        }
                    }
                    self.ready_if_met(dependent);
                }
                Met::Never(t) => {
                    self.finish(dependent, End::Skipped(Skip::Dependency(line)), t, out);
                }
                Met::Not => still.push((dependent, on)),
            }
        }
        if let Some(c) = self.cells.get_mut(&line) {
            c.waiters = still;
        }
        self.barrier(line, out);
    }

    /// Advances a member's barrier prefixes, and fires or skips its holders.
    fn barrier(&mut self, line: Line, out: &mut Vec<Output>) {
        let Some((key, ord)) = self.cells.get(&line).and_then(|c| c.member.clone()) else {
            return;
        };
        let Some(mut b) = self.barriers.remove(&key) else {
            return;
        };
        let mut fired: Vec<(Line, u64, Line)> = Vec::new();
        let mut skipped: Vec<(Line, Line, u64)> = Vec::new();
        // A member that ended otherwise than expected fails every holder
        // awaiting it expected at once, wherever the prefix stands: such a
        // holder can never be met (decision 6, `vectors/schedule.py`).
        if let Met::Never(t) = self.met(line, On::Expected)
            && b.failed.is_none_or(|f| ord < f)
        {
            b.failed = Some(ord);
            b.first_failure.get_or_insert((line, t));
            let r = rank(On::Expected);
            let failing = b.holders[r].split_off(&ord.saturating_add(1));
            for (_, holders) in failing {
                skipped.extend(holders.into_iter().map(|h| (h, line, t)));
            }
        }
        for on in [On::Sent, On::Settled, On::Expected] {
            let r = rank(on);
            // A prefix of members expected stops for good at the first that
            // failed: it is not swept to, nor its cell read again.
            let limit = match b.failed {
                Some(f) if on == On::Expected => f,
                _ => b.count,
            };
            while b.prefix[r] < limit {
                let at = b.prefix[r];
                let Some(&m) = b.members.get(at.saturating_sub(b.base)) else {
                    break;
                };
                match self.met(m, on) {
                    Met::Yes(t) => {
                        if (t, m) >= b.latest[r] {
                            b.latest[r] = (t, m);
                        }
                        b.prefix[r] = at.saturating_add(1);
                        // The holders waiting for exactly these members: the
                        // latest among them is the latest so far.
                        if let Some(holders) = b.holders[r].remove(&b.prefix[r]) {
                            let (t, last) = b.latest[r];
                            fired.extend(holders.into_iter().map(|h| (h, t, last)));
                        }
                    }
                    // A failure is recorded when its member ends, above.
                    Met::Never(_) | Met::Not => break,
                }
            }
        }
        // The members every prefix passed: nothing sweeps them again — for
        // expected, those before its prefix, and every one once it reached
        // the first that failed.
        let expected = match b.failed {
            Some(f) if b.prefix[rank(On::Expected)] >= f => usize::MAX,
            _ => b.prefix[rank(On::Expected)],
        };
        let passed_all = b.prefix[rank(On::Sent)]
            .min(b.prefix[rank(On::Settled)])
            .min(expected);
        let mut passed = Vec::new();
        while b.base < passed_all {
            let Some(m) = b.members.pop_front() else {
                break;
            };
            b.base = b.base.saturating_add(1);
            passed.push(m);
        }
        self.barriers.insert(key, b);
        for (holder, t, last) in fired {
            if let Some(d) = self.cells.get_mut(&holder) {
                d.waiting = d.waiting.saturating_sub(1);
                if t >= d.ready || d.critical.is_none() {
                    d.ready = d.ready.max(t);
                    d.critical = Some(Critical::Member(last));
                }
            }
            self.ready_if_met(holder);
        }
        for (holder, first, t) in skipped {
            self.finish(holder, End::Skipped(Skip::Member(first)), t, out);
        }
        for m in passed {
            self.free_if_done(m);
        }
    }
}

enum Met {
    Yes(u64),
    Never(u64),
    Not,
}

/// The skip that ended first: of dependencies already failed when a line is
/// read, the one whose end came earliest, the first in `after` on a tie.
fn earliest(skip: &mut Option<(Skip, u64)>, why: Skip, t: u64) {
    if skip.as_ref().is_none_or(|(_, at)| t < *at) {
        *skip = Some((why, t));
    }
}

fn fold(ready: &mut Option<(u64, Option<Critical>)>, t: u64, c: Critical) {
    match ready {
        Some((at, _)) if *at > t => {}
        _ => *ready = Some((t, Some(c))),
    }
}
