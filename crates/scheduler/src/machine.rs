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

use std::collections::{BTreeMap, BTreeSet};
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
    record: Arc<Record>,
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
}

type BarrierKey = (String, Vec<u8>, String, u32);

#[derive(Clone, Debug, Default)]
struct Barrier {
    members: Vec<Line>,
    /// Members up to here met the strength, by `On` rank; and the latest
    /// instant among them.
    prefix: [usize; 3],
    latest: [u64; 3],
    /// The first member awaited as expected that ended otherwise.
    failed: Option<usize>,
    /// Holders: their line, the members read before them, the strength.
    holders: Vec<(Line, usize, On)>,
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
    /// ahead of its clock.
    pub fn new(header: Arc<Header>, read_ahead: u64) -> Machine {
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

    /// The steps it holds: read and not ended, or ended and still bound.
    pub fn held(&self) -> usize {
        self.cells.len()
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
        self.window.read(record.at);
        let mut cell = Cell {
            record: record.clone(),
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
                        Met::Never(t) => {
                            skip.get_or_insert((Skip::Dependency(dep), t));
                        }
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
                    let key = (kind.clone(), scope.clone(), label.clone(), *n);
                    let b = self.barriers.entry(key.clone()).or_default();
                    let members = b.members.len();
                    let r = rank(*on);
                    if *on == On::Expected && b.failed.is_some_and(|f| f < members) {
                        let first = b
                            .failed
                            .and_then(|f| b.members.get(f))
                            .copied()
                            .unwrap_or(0);
                        let t = self
                            .cells
                            .get(&first)
                            .and_then(|c| c.end.as_ref())
                            .map_or(self.now, |e| e.1);
                        skip.get_or_insert((Skip::Member(first), t));
                    } else if b.prefix[r] >= members {
                        let members = b.members.clone();
                        let latest = members
                            .iter()
                            .filter_map(|m| match self.met(*m, *on) {
                                Met::Yes(t) => Some((t, *m)),
                                _ => None,
                            })
                            .max();
                        if let Some((t, m)) = latest {
                            fold(&mut ready, t, Critical::Member(m));
                        }
                    } else {
                        b.holders.push((line, members, *on));
                        cell.waiting = cell.waiting.saturating_add(1);
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
        let s = &record.step;
        let key = (
            s.subject.kind.clone(),
            s.subject.scope.clone(),
            s.label.clone(),
            s.n,
        );
        let b = self.barriers.entry(key.clone()).or_default();
        cell.member = Some((key, b.members.len()));
        b.members.push(line);
        let retained = self.header.retained.contains(&record.step.label);
        self.window
            .keep(digest(&record.step)?, record.at, retained, line);
        self.cells.insert(line, cell);
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
        c.intended = c.record.at.max(c.ready.saturating_add(c.record.think));
        c.released = c.intended.max(c.read);
        c.phase = Phase::Due;
        self.wakes.insert((c.released, line));
    }

    /// Dispatches what is due, polls on their grid, ends what passed its
    /// backstop.
    fn due(&mut self, out: &mut Vec<Output>) {
        while let Some(&(t, line)) = self.wakes.first() {
            if t > self.now {
                break;
            }
            self.wakes.pop_first();
            let Some(c) = self.cells.get(&line) else {
                continue;
            };
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
        // The captured values its holes take, from their producers' cells.
        let mut captured = Vec::new();
        for (subject, slot, producer) in &c.producers {
            let value = self.cells.get(producer).and_then(|p| {
                p.captures
                    .iter()
                    .find(|v| &v.subject == subject && &v.slot == slot)
            });
            match value {
                Some(v) => captured.push(v.clone()),
                None => {
                    let producer = *producer;
                    self.finish(line, End::Skipped(Skip::MissingCapture(producer)), now, out);
                    return;
                }
            }
        }
        let deadline = if attempt == 1 {
            self.deadline(line)
        } else {
            c.deadline
        };
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
        self.wakes.insert((deadline.saturating_add(GRACE), line));
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
        let h = &self.header;
        let timeout = |name: &str| {
            h.operations
                .iter()
                .find(|o| o.name == name)
                .map(|o| o.timeout)
        };
        match &c.record.action {
            Action::Rest(r) => c.released.saturating_add(timeout(&r.op).unwrap_or(0)),
            Action::Bus(b) => c.released.saturating_add(
                h.channels
                    .iter()
                    .find(|t| t.name == b.channel)
                    .map_or(0, |t| t.timeout),
            ),
            Action::Host(call) => c.released.saturating_add(
                h.calls
                    .iter()
                    .find(|t| t.name == call.op)
                    .map_or(0, |t| t.timeout),
            ),
            Action::Observe(o) => {
                let start = c
                    .cause
                    .and_then(|l| self.cells.get(&l))
                    .map_or(c.intended, |cause| cause.intended);
                start.saturating_add(o.timeout)
            }
        }
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
        let poll =
            matches!(&c.record.action, Action::Observe(o) if matches!(o.probe, Probe::Rest(_)));
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
                let every = match &c.record.action {
                    Action::Observe(o) => o.every.max(1),
                    _ => 1,
                };
                let since = now.saturating_sub(c.released);
                let k = since.div_ceil(every).max(u64::from(attempt));
                let next = c.released.saturating_add(k.saturating_mul(every));
                c.phase = Phase::Flight {
                    attempt,
                    outstanding: false,
                };
                let deadline = c.deadline;
                self.wakes.insert((next.min(deadline), line));
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
        let Some(c) = self.cells.get(&line) else {
            return Safety::Never;
        };
        match &c.record.action {
            Action::Rest(r) => self
                .header
                .operations
                .iter()
                .find(|o| o.name == r.op)
                .map_or(Safety::Never, |o| o.safety),
            Action::Host(call) => self
                .header
                .calls
                .iter()
                .find(|t| t.name == call.op)
                .map_or(Safety::Never, |t| t.safety),
            // A bus frame is resent byte for byte: the inbox keeps one.
            Action::Bus(_) => Safety::Idempotent,
            Action::Observe(_) => Safety::Never,
        }
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
        self.notify(line, out);
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
        let Some((key, _)) = self.cells.get(&line).and_then(|c| c.member.clone()) else {
            return;
        };
        let Some(mut b) = self.barriers.remove(&key) else {
            return;
        };
        for on in [On::Sent, On::Settled, On::Expected] {
            let r = rank(on);
            while let Some(&m) = b.members.get(b.prefix[r]) {
                match self.met(m, on) {
                    Met::Yes(t) => {
                        b.latest[r] = b.latest[r].max(t);
                        b.prefix[r] = b.prefix[r].saturating_add(1);
                    }
                    Met::Never(_) => {
                        b.failed = Some(b.failed.map_or(b.prefix[r], |f| f.min(b.prefix[r])));
                        break;
                    }
                    Met::Not => break,
                }
            }
        }
        let holders = std::mem::take(&mut b.holders);
        let mut fired = Vec::new();
        let mut skipped = Vec::new();
        for (holder, members, on) in holders {
            let r = rank(on);
            if on == On::Expected && b.failed.is_some_and(|f| f < members) {
                let first = b
                    .failed
                    .and_then(|f| b.members.get(f))
                    .copied()
                    .unwrap_or(0);
                skipped.push((holder, first));
            } else if b.prefix[r] >= members {
                // The latest among the holder's members, the first `members`
                // read; its critical member, the one that met it last.
                let (t, last) = b
                    .members
                    .iter()
                    .take(members)
                    .filter_map(|m| match self.met(*m, on) {
                        Met::Yes(t) => Some((t, *m)),
                        _ => None,
                    })
                    .max()
                    .unwrap_or((0, 0));
                fired.push((holder, t, last));
            } else {
                b.holders.push((holder, members, on));
            }
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
        for (holder, first) in skipped {
            let t = self
                .cells
                .get(&first)
                .and_then(|c| c.end.as_ref())
                .map_or(self.now, |e| e.1);
            self.finish(holder, End::Skipped(Skip::Member(first)), t, out);
        }
    }
}

enum Met {
    Yes(u64),
    Never(u64),
    Not,
}

fn fold(ready: &mut Option<(u64, Option<Critical>)>, t: u64, c: Critical) {
    match ready {
        Some((at, _)) if *at > t => {}
        _ => *ready = Some((t, Some(c))),
    }
}
