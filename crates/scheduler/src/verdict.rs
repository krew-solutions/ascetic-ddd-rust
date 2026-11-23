//! The verdict (ADR-0047 decision 14, as decided for K1e-3): a run's journal
//! folded into a class a line and a verdict, from the journal alone.
//!
//! Two passes. The first takes every line in the order the journal holds them
//! — time's — and keeps, beside the steps read and not ended, one byte a line:
//! how it ended, whether the harness's own lateness made its end, whether a
//! later reader lacked its captured value, whether its bytes left late. The
//! second takes the reads alone, which the journal holds in line order, and
//! decides each line's class from its dependencies' bytes, a dependency always
//! a line before it, with a few flags a barrier key: no sort, no second byte.
//!
//! Classes partition the lines by precedence: Consequent (skipped) before
//! Inconclusive (conditional) before Undecided before Root before Passed. The
//! verdict: void on broken evidence (unreadable, incoherent, another log, a
//! fault) above fail (a root) above void undecided; pass only when every line
//! passed.

use std::collections::BTreeMap;

use ascetic_ddd_command_log::record::{On, Safety};

use crate::journal::{
    Action, Answer, Dep, End, Ended, Fault, FaultClass, Found, Header, JournalError, Judged, Key,
    Line, Read, Skip,
};
use crate::machine::Line as Number;

/// What the fold decides, as a number: raised whenever the classes or the
/// verdict it gives a journal change, which the goldens' and the differential's
/// classes pin beside it.
pub const FOLD_REVISION: u32 = 6;

/// A line's class (decision 14), by precedence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Class {
    /// Ended as expected, nothing against it.
    Passed,
    /// A failure counted here: it ended otherwise than expected on no doubtful
    /// ground, or a later reader lacked a value it was to give.
    Root,
    /// Ended unknown because the harness's own lateness ate its deadline.
    Undecided,
    /// Its assertion stands on a step whose end is in doubt (decision 6).
    Inconclusive,
    /// Never sent: the failure it follows is counted at its root.
    Consequent,
}

impl Class {
    /// Its name in a report.
    pub fn name(self) -> &'static str {
        match self {
            Class::Passed => "passed",
            Class::Root => "root",
            Class::Undecided => "undecided",
            Class::Inconclusive => "inconclusive",
            Class::Consequent => "consequent",
        }
    }
}

/// Why a run is void.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Void {
    /// The journal could not be read.
    Unreadable(String),
    /// It journals another log, or not all of it.
    AnotherLog {
        /// What the journal names.
        journal: String,
        /// What the run was to run.
        log: String,
    },
    /// A harness fault: the run's stop, or else the first backstop.
    Fault {
        /// Its class.
        class: FaultClass,
    },
    /// The journal disagrees with itself.
    Incoherent {
        /// Where, 0 for the whole.
        line: Number,
        /// Why.
        why: &'static str,
    },
    /// No root, but a step undecided.
    Undecided,
}

/// A run's verdict.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Every line passed, on sound evidence.
    Pass,
    /// A root, on sound evidence.
    Fail,
    /// Neither can be said.
    Void(Void),
}

impl Verdict {
    /// Its name in a report: `pass`, `fail`, or `void:` and why.
    pub fn name(&self) -> &'static str {
        match self {
            Verdict::Pass => "pass",
            Verdict::Fail => "fail",
            Verdict::Void(Void::Unreadable(_)) => "void:unreadable",
            Verdict::Void(Void::AnotherLog { .. }) => "void:another_log",
            Verdict::Void(Void::Fault { .. }) => "void:fault",
            Verdict::Void(Void::Incoherent { .. }) => "void:incoherent",
            Verdict::Void(Void::Undecided) => "void:undecided",
        }
    }
}

/// The fold of a journal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Folded {
    /// [`FOLD_REVISION`].
    pub fold: u32,
    /// The verdict.
    pub verdict: Verdict,
    /// Each line's class, from line 2; none if the evidence broke before the
    /// second pass.
    pub classes: Vec<Class>,
}

/// The log a run was to run: its commitment's digest and step count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Log {
    /// The trailer's SHA-256.
    pub sha256: String,
    /// Its steps.
    pub records: u64,
}

// A line's byte.
const END: u8 = 0b111;
const EXPECTED: u8 = 1;
const UNEXPECTED: u8 = 2;
const UNKNOWN: u8 = 3;
const SKIPPED: u8 = 4;
/// Its end the harness's lateness made: a backstop, a call's last attempt
/// sent past J after its dispatch or never, a release past its deadline read
/// past J late.
const UNDECIDED: u8 = 1 << 3;
/// A later reader was skipped for a value it was to capture.
const PRODUCER: u8 = 1 << 4;
/// Its bytes left more than J after its intended start, or never.
const LATE: u8 = 1 << 5;
/// Its assertion stands on a step whose end is in doubt.
const CONDITIONAL: u8 = 1 << 6;

fn incoherent(line: Number, why: &'static str) -> Void {
    Void::Incoherent { line, why }
}

fn end_code(e: End) -> u8 {
    match e {
        End::Expected => EXPECTED,
        End::Unexpected => UNEXPECTED,
        End::Unknown => UNKNOWN,
        End::Skipped(_) => SKIPPED,
    }
}

/// A step read and not ended, in the first pass.
struct Live {
    read: u64,
    action: Action,
    /// Its action's safety: which resends its answers license.
    safety: Safety,
    /// Its step dependencies, by strength.
    deps: Vec<(On, Number)>,
    /// Intended, released, deadline.
    released: Option<(u64, u64, u64)>,
    attempts: u32,
    first_dispatch: Option<u64>,
    last_dispatch: u64,
    /// The last attempt's first send.
    last_sent: Option<u64>,
    /// The first send of the attempt then out (the machine's `Entry.sent`).
    sent: Option<u64>,
    /// The last attempt's answer the machine took.
    taken: Option<Judged>,
    /// That answer: its attempt, its instant, the wait it asked for.
    answered: Option<(u32, u64, Option<u64>)>,
    /// That answer was the frontier's censor at the deadline (decision 17).
    censored: bool,
    /// The plan's `at`.
    planned: u64,
    /// The last attempt's deadline.
    last_deadline: u64,
    /// The harness's lateness over its attempts (decision 17): each one's
    /// machine (`dispatch.at − due`) and hand-off (`handed − dispatch.at`).
    harness: u64,
    /// That lateness by its first send: the cause's, for its observation.
    harness_sent: Option<u64>,
    /// A late answer found for its censor (version 5): once a censor.
    found: bool,
    /// That answer read more than J after the shell last knew it absent,
    /// absent before the deadline: the receive side's lateness.
    found_late: bool,
}

/// A step ended by its last attempt's censor, a late answer not yet found
/// for it: what a `found` must agree with.
struct Censor {
    attempt: u32,
    deadline: u64,
    dispatched: u64,
}

struct First {
    jitter: u64,
    /// The profile's resend floor.
    floor: u64,
    bytes: Vec<u8>,
    live: BTreeMap<Number, Live>,
    next: Number,
    /// A backstop's end, its fault line still to come.
    owed: Option<Number>,
    fault: Option<Number>,
    /// The run's stop (decision 18): the journal's last line.
    stop: Option<FaultClass>,
    /// Lines taken, and the latest instant among them.
    taken: u64,
    latest: u64,
    /// The header holds no T0: only a gate's refusal may follow.
    unstarted: bool,
    /// Steps ended by a censor, their late answer not yet found.
    censors: BTreeMap<Number, Censor>,
}

impl First {
    fn index(line: Number) -> Option<usize> {
        line.checked_sub(2).and_then(|i| usize::try_from(i).ok())
    }

    fn byte(&self, line: Number) -> Option<u8> {
        First::index(line).and_then(|i| self.bytes.get(i).copied())
    }

    fn mark(&mut self, line: Number, bit: u8) {
        if let Some(b) = First::index(line).and_then(|i| self.bytes.get_mut(i)) {
            *b |= bit;
        }
    }

    fn read_before(line: Number, holder: Number) -> bool {
        line >= 2 && line < holder
    }

    fn live(&mut self, line: Number) -> Result<&mut Live, Void> {
        self.live
            .get_mut(&line)
            .ok_or_else(|| incoherent(line, "an event of a step not read, or ended"))
    }

    fn take(&mut self, l: &Line) -> Result<(), Void> {
        if self.stop.is_some() {
            return Err(incoherent(0, "a line after the run's stop"));
        }
        let at = match l {
            Line::Read(x) => x.at,
            Line::Released(x) => x.at,
            Line::Dispatch(x) => x.at,
            Line::Sent(x) => x.at,
            Line::Answer(x) => x.at,
            Line::End(x) => x.at,
            Line::Fault(x) => x.at,
            Line::Header(_) | Line::Trailer(_) => 0,
        };
        let taken = self.step(l);
        self.taken = self.taken.saturating_add(1);
        self.latest = self.latest.max(at);
        taken
    }

    fn step(&mut self, l: &Line) -> Result<(), Void> {
        if let Some(owed) = self.owed.take() {
            return match l {
                Line::Fault(f) if f.class == (FaultClass::Backstop { line: owed }) => {
                    self.fault.get_or_insert(owed);
                    Ok(())
                }
                _ => Err(incoherent(owed, "a backstop's end without its fault")),
            };
        }
        match l {
            Line::Header(_) | Line::Trailer(_) => {
                Err(incoherent(0, "a header or a trailer amid the journal"))
            }
            Line::Read(r) => {
                if r.line != self.next {
                    return Err(incoherent(r.line, "the reads are not one a line, in order"));
                }
                self.next = self.next.saturating_add(1);
                self.bytes.push(0);
                let deps = r
                    .after
                    .iter()
                    .filter_map(|d| match d {
                        Dep::Step { on, line } => Some((*on, *line)),
                        Dep::Each { .. } => None,
                    })
                    .collect();
                self.live.insert(
                    r.line,
                    Live {
                        read: r.at,
                        action: r.action,
                        safety: r.safety,
                        deps,
                        released: None,
                        attempts: 0,
                        first_dispatch: None,
                        last_dispatch: 0,
                        last_sent: None,
                        sent: None,
                        taken: None,
                        answered: None,
                        censored: false,
                        planned: r.planned,
                        last_deadline: 0,
                        harness: 0,
                        harness_sent: None,
                        found: false,
                        found_late: false,
                    },
                );
                Ok(())
            }
            Line::Released(x) => {
                let lv = self.live(x.line)?;
                if lv.released.is_some() || x.at < lv.read || x.released != x.intended.max(lv.read)
                {
                    return Err(incoherent(x.line, "a release against its read"));
                }
                lv.released = Some((x.intended, x.released, x.deadline));
                Ok(())
            }
            Line::Dispatch(d) => {
                let (jitter, floor) = (self.jitter, self.floor);
                let lv = self.live(d.line)?;
                let Some((_, released, deadline)) = lv.released else {
                    return Err(incoherent(d.line, "an attempt dispatched unreleased"));
                };
                if Some(d.attempt) != lv.attempts.checked_add(1) || d.at < released {
                    return Err(incoherent(d.line, "an attempt dispatched out of turn"));
                }
                // Decision 17: dispatched by a wake no later than it, the first
                // at its release.
                if d.due > d.at {
                    return Err(incoherent(d.line, "a dispatch before its wake"));
                }
                if d.attempt == 1 && d.due != released {
                    return Err(incoherent(d.line, "a first attempt's wake not its release"));
                }
                // A resend its predecessor's answer licensed (ADR-0047
                // decision 15): a poll's next after an answer not expected, no
                // sooner than its wait; a call's after a refusal its safety
                // resends, at the later of its wait and the floor, more than J
                // before its deadline.
                if d.attempt > 1 {
                    let Some((attempt, at, after)) = lv.answered else {
                        return Err(incoherent(d.line, "a resend no answer licensed"));
                    };
                    let licensed = match (lv.action, lv.taken) {
                        (
                            Action::Poll,
                            Some(Judged::Unexpected | Judged::NotTaken | Judged::Unknown),
                        ) => d.at >= at.saturating_add(after.unwrap_or(0)),
                        (Action::Poll, _) => false,
                        (_, Some(j)) => {
                            let by_safety = match j {
                                Judged::NotTaken => lv.safety != Safety::Never,
                                Judged::Unknown => lv.safety == Safety::Idempotent,
                                Judged::Expected | Judged::Unexpected => false,
                            };
                            by_safety
                                && d.at >= at.saturating_add(after.unwrap_or(0).max(floor))
                                && d.at.saturating_add(jitter) < deadline
                        }
                        (_, None) => false,
                    };
                    if attempt != lv.attempts || !licensed {
                        return Err(incoherent(d.line, "a resend its answer did not license"));
                    }
                    // Its wake: a poll's after its answer (before its deadline
                    // as its dispatch is); a call's at its answer and its wait.
                    let due = match lv.action {
                        Action::Poll => d.due >= at,
                        _ => d.due == at.saturating_add(after.unwrap_or(0).max(floor)),
                    };
                    if !due {
                        return Err(incoherent(d.line, "a resend against its wake"));
                    }
                }
                lv.attempts = d.attempt;
                lv.last_deadline = d.deadline;
                lv.harness = lv.harness.saturating_add(d.at.saturating_sub(d.due));
                lv.censored = false;
                lv.first_dispatch.get_or_insert(d.at);
                lv.last_dispatch = d.at;
                lv.last_sent = None;
                lv.taken = None;
                lv.answered = None;
                Ok(())
            }
            Line::Sent(t) => {
                let next = self.next;
                match self.live.get_mut(&t.line) {
                    Some(lv) => {
                        if t.attempt > lv.attempts || t.attempt == 0 {
                            return Err(incoherent(t.line, "a send of an attempt not dispatched"));
                        }
                        if t.handed > t.connected || t.connected > t.at {
                            return Err(incoherent(t.line, "a send against its path"));
                        }
                        // Its thread's lag lies inside its path (decision 18).
                        if t.lag > t.at.saturating_sub(t.handed) {
                            return Err(incoherent(t.line, "a send's lag past its path"));
                        }
                        if t.attempt == lv.attempts && lv.last_sent.is_none() {
                            if t.handed < lv.last_dispatch {
                                return Err(incoherent(
                                    t.line,
                                    "a send handed before its dispatch",
                                ));
                            }
                            // The hand-off, the harness's (decision 17), and
                            // its source thread's lag on the send path
                            // (decision 18).
                            lv.harness = lv
                                .harness
                                .saturating_add(t.handed.saturating_sub(lv.last_dispatch))
                                .saturating_add(t.lag);
                            lv.harness_sent.get_or_insert(lv.harness);
                        }
                        if t.attempt == lv.attempts {
                            lv.last_sent.get_or_insert(t.at);
                            lv.sent.get_or_insert(t.at);
                        }
                        Ok(())
                    }
                    None if First::read_before(t.line, next) => Ok(()),
                    None => Err(incoherent(t.line, "a send of a line not read")),
                }
            }
            Line::Answer(a) => {
                if let Some(f) = a.found {
                    return self.found(a, f);
                }
                let next = self.next;
                // The frontier's censor: unknown, no answer's status.
                if a.censored && (a.judged != Judged::Unknown || a.status.is_some()) {
                    return Err(incoherent(a.line, "a censor with an answer's content"));
                }
                match (self.live.get_mut(&a.line), a.ignored) {
                    (Some(lv), None) => {
                        if a.attempt != lv.attempts || lv.attempts == 0 {
                            return Err(incoherent(
                                a.line,
                                "an answer taken from an attempt not out",
                            ));
                        }
                        if a.censored && a.at != lv.last_deadline {
                            return Err(incoherent(a.line, "a censor off its deadline"));
                        }
                        lv.taken = Some(a.judged);
                        lv.answered = Some((a.attempt, a.at, a.after));
                        lv.censored = a.censored;
                        Ok(())
                    }
                    (Some(lv), Some(_)) => {
                        if a.attempt > lv.attempts {
                            return Err(incoherent(
                                a.line,
                                "an answer of an attempt not dispatched",
                            ));
                        }
                        Ok(())
                    }
                    (None, Some(_)) if First::read_before(a.line, next) => Ok(()),
                    (None, _) => Err(incoherent(
                        a.line,
                        "an answer taken of a step not read, or ended",
                    )),
                }
            }
            Line::End(e) => self.end(e),
            Line::Fault(f) => self.stopped(f),
        }
    }

    /// A run's stop (decision 18): a gate's refusal, the journal's only line,
    /// at 0, under a header with no T0; any other no earlier than the lines
    /// before it, the step it names read and its attempt dispatched.
    fn stopped(&mut self, f: &Fault) -> Result<(), Void> {
        match &f.class {
            FaultClass::Backstop { line } => {
                return Err(incoherent(*line, "a fault without its backstop's end"));
            }
            FaultClass::Gate { .. } => {
                if self.taken != 0 || f.at != 0 || !self.unstarted {
                    return Err(incoherent(
                        0,
                        "a gate's refusal not the journal's only line",
                    ));
                }
            }
            class => {
                if f.at < self.latest {
                    return Err(incoherent(0, "a stop before the line above it"));
                }
                if let Some((line, attempt)) = class.struck() {
                    if !First::read_before(line, self.next) {
                        return Err(incoherent(line, "a stop naming a line not read"));
                    }
                    if let (Some(a), Some(lv)) = (attempt, self.live.get(&line))
                        && (a == 0 || a > lv.attempts)
                    {
                        return Err(incoherent(line, "a stop naming an attempt not out"));
                    }
                }
            }
        }
        self.stop = Some(f.class.clone());
        Ok(())
    }

    /// A late answer and its bracket (decision 17, version 5): ignored, for
    /// its attempt's censor — the step's taken answer if it is live, its end
    /// if not — once; `absent` after its dispatch and before the answer, the
    /// deadline its attempt's. Undecided if it was absent before the deadline
    /// and found more than J after.
    fn found(&mut self, a: &Answer, f: Found) -> Result<(), Void> {
        let bad = |why| Err(incoherent(a.line, why));
        if a.ignored.is_none() {
            return bad("a found answer taken");
        }
        if f.absent >= a.at || f.deadline >= a.at {
            return bad("a found answer within its bracket");
        }
        let late = f.absent < f.deadline && a.at.saturating_sub(f.absent) > self.jitter;
        if let Some(lv) = self.live.get_mut(&a.line) {
            if !lv.censored
                || lv.found
                || a.attempt != lv.attempts
                || f.deadline != lv.last_deadline
                || f.absent < lv.last_dispatch
            {
                return bad("a found answer against its censor");
            }
            lv.found = true;
            lv.found_late = late;
            return Ok(());
        }
        let Some(c) = self.censors.remove(&a.line) else {
            return bad("a found answer without its censor");
        };
        if a.attempt != c.attempt || f.deadline != c.deadline || f.absent < c.dispatched {
            return bad("a found answer against its censor");
        }
        if late {
            self.mark(a.line, UNDECIDED);
        }
        Ok(())
    }

    fn end(&mut self, e: &Ended) -> Result<(), Void> {
        let Some(lv) = self.live.remove(&e.line) else {
            return Err(incoherent(
                e.line,
                "an end of a line not read, or ended twice",
            ));
        };
        let released = match lv.released {
            Some((i, r, _)) => e.intended == i && e.released == r,
            None => e.dispatched.is_none(),
        };
        if e.read != lv.read
            || e.attempts != lv.attempts
            || e.dispatched != lv.first_dispatch
            || e.sent != lv.sent
            || !released
        {
            return Err(incoherent(e.line, "an end against its events"));
        }
        if lv.released.is_some() && lv.planned > e.intended {
            return Err(incoherent(e.line, "an intended start before its plan"));
        }
        let answered = match e.end {
            End::Expected => lv.taken == Some(Judged::Expected) && !e.fault,
            End::Unexpected => {
                matches!(lv.taken, Some(Judged::Unexpected | Judged::NotTaken)) && !e.fault
            }
            End::Unknown => {
                e.fault || lv.attempts == 0 || !matches!(lv.taken, Some(Judged::Expected))
            }
            End::Skipped(_) => lv.attempts == 0 && !e.fault,
        };
        if !answered {
            return Err(incoherent(e.line, "an end against its answers"));
        }
        let failed = |b: Option<u8>| b.is_some_and(|b| !matches!(b & END, 0 | EXPECTED));
        match e.end {
            End::Skipped(Skip::Dependency(d)) => {
                if !lv.deps.contains(&(On::Expected, d)) || !failed(self.byte(d)) {
                    return Err(incoherent(
                        e.line,
                        "skipped for a dependency that did not fail it",
                    ));
                }
            }
            End::Skipped(Skip::Member(m)) => {
                if !First::read_before(m, e.line) || !failed(self.byte(m)) {
                    return Err(incoherent(
                        e.line,
                        "skipped for a member that did not fail it",
                    ));
                }
            }
            End::Skipped(Skip::MissingCapture(p)) => {
                if !First::read_before(p, e.line) {
                    return Err(incoherent(
                        e.line,
                        "skipped for a producer not read before it",
                    ));
                }
                self.mark(p, PRODUCER);
            }
            End::Expected | End::Unexpected | End::Unknown => {}
        }
        let j = self.jitter;
        self.wake(e, &lv)?;
        // Decision 17: the harness's lateness is the machine's and the
        // hand-off's over the attempts, and an observation's reader's; the
        // stand's connect and write are not the harness's.
        let reader = match lv.action {
            Action::Poll | Action::Observe => {
                lv.released.map_or(0, |(i, r, _)| r.saturating_sub(i))
            }
            _ => 0,
        };
        // An observation's cause sent late by the harness, or never.
        let late = e.sent.is_none()
            || lv
                .released
                .map_or(0, |(i, r, _)| r.saturating_sub(i))
                .saturating_add(lv.harness_sent.unwrap_or(0))
                > j;
        // Only an end the harness could have caused: the last attempt
        // censored at its deadline, or never sent — an answer the system gave
        // is its own, however late it was sent.
        let censor_late = e.end == End::Unknown
            && lv.attempts >= 1
            && lv.censored
            && (lv.last_sent.is_none() || lv.harness.saturating_add(reader) > j);
        let read_late = lv
            .released
            .is_some_and(|(i, r, d)| r >= d && i < d && r.saturating_sub(i) > j);
        // An end by a wake the harness took late (decision 17): due before
        // the deadline — a poll's, a call's fallback, a first dispatch's.
        let wake_late = matches!(e.end, End::Unknown | End::Unexpected)
            && !e.fault
            && e.wake
                .is_some_and(|w| lv.released.is_some_and(|(_, _, d)| w < d));
        let mut byte = end_code(e.end);
        if late {
            byte |= LATE;
        }
        if (e.end == End::Unknown && (e.fault || censor_late || read_late || lv.found_late))
            || wake_late
        {
            byte |= UNDECIDED;
        }
        // A censor's late answer may yet be found (version 5).
        if e.end == End::Unknown && lv.attempts >= 1 && lv.censored && !lv.found {
            self.censors.insert(
                e.line,
                Censor {
                    attempt: lv.attempts,
                    deadline: lv.last_deadline,
                    dispatched: lv.last_dispatch,
                },
            );
        }
        self.mark(e.line, byte);
        if e.fault {
            self.owed = Some(e.line);
        }
        Ok(())
    }

    /// An end's wake, against what it ended (decision 17): none for an input's
    /// end or a skip; with attempts, no later than the end — a poll's at most
    /// its deadline, there only for an end there, before it only for an end at
    /// or past it; a call's fallback its answer plus its wait, more than J
    /// before its deadline; with none, its release, the end at or past its
    /// deadline. A backstop's end is exempt.
    fn wake(&self, e: &Ended, lv: &Live) -> Result<(), Void> {
        let Some(w) = e.wake else {
            return Ok(());
        };
        if e.fault {
            return Ok(());
        }
        let bad = |why| Err(incoherent(e.line, why));
        if matches!(e.end, End::Expected | End::Skipped(_)) {
            return bad("a wake on an end an input or a skip made");
        }
        let Some((_, released, d)) = lv.released else {
            return bad("a wake on an end never released");
        };
        if w > e.ended {
            return bad("a wake after its end");
        }
        if lv.attempts == 0 {
            if w != released || e.ended < d {
                return bad("a first dispatch's wake against its release");
            }
            return Ok(());
        }
        match lv.action {
            Action::Poll => {
                if w > d || (w == d && e.ended != d) || (w < d && e.ended < d) {
                    return bad("a poll's wake against its deadline");
                }
            }
            _ => {
                let fallback = lv
                    .answered
                    .map(|(_, at, after)| at.saturating_add(after.unwrap_or(0).max(self.floor)));
                if fallback != Some(w) || w.saturating_add(self.jitter) >= d {
                    return bad("a fallback's wake against its licence");
                }
            }
        }
        Ok(())
    }

    fn finish(&self) -> Result<(), Void> {
        if let Some(owed) = self.owed {
            return Err(incoherent(owed, "a backstop's end without its fault"));
        }
        if self.unstarted && !matches!(self.stop, Some(FaultClass::Gate { .. })) {
            return Err(incoherent(0, "no T0, and no gate's refusal"));
        }
        // Stopped, the run's steps need not all have ended.
        if self.stop.is_some() {
            return Ok(());
        }
        match self.live.keys().next() {
            Some(line) => Err(incoherent(*line, "a step read and never ended")),
            None => Ok(()),
        }
    }
}

/// A barrier key's members read so far, in the second pass.
#[derive(Clone, Copy, Default)]
struct Members {
    count: u64,
    /// One ended unknown or skipped.
    doubtful: bool,
    /// One is conditional.
    conditional: bool,
    /// One ended otherwise than expected.
    failed: bool,
}

struct Second {
    bytes: Vec<u8>,
    keys: BTreeMap<Key, Members>,
    next: Number,
    classes: Vec<Class>,
}

impl Second {
    fn byte(&self, line: Number) -> u8 {
        First::index(line)
            .and_then(|i| self.bytes.get(i).copied())
            .unwrap_or(0)
    }

    fn read(&mut self, r: &Read) -> Result<(), Void> {
        if r.line != self.next {
            return Err(incoherent(r.line, "the second pass reads another journal"));
        }
        self.next = self.next.saturating_add(1);
        let me = self.byte(r.line);
        let end = me & END;
        let doubtful = |b: u8| matches!(b & END, UNKNOWN | SKIPPED);
        let mut conditional = false;
        let mut must_skip = false;
        for d in &r.after {
            match d {
                Dep::Step { on, line } => {
                    if *line < 2 || *line >= r.line {
                        return Err(incoherent(
                            r.line,
                            "a dependency not read before its holder",
                        ));
                    }
                    let b = self.byte(*line);
                    if (*on != On::Expected && doubtful(b)) || b & CONDITIONAL != 0 {
                        conditional = true;
                    }
                    if *on == On::Expected && b & END != EXPECTED {
                        must_skip = true;
                    }
                }
                Dep::Each { on, key, members } => {
                    let m = self.keys.get(key).copied().unwrap_or_default();
                    if m.count != *members {
                        return Err(incoherent(r.line, "a barrier's members miscounted"));
                    }
                    if (*on != On::Expected && m.doubtful) || m.conditional {
                        conditional = true;
                    }
                    if *on == On::Expected && m.failed {
                        must_skip = true;
                    }
                }
            }
        }
        // Decision 6 amended: an observation asserts its cause's effect.
        let mut late_cause = false;
        if r.action.observes() {
            let Some(cause) = r.cause else {
                return Err(incoherent(r.line, "an observation without its cause"));
            };
            if cause < 2 || cause > r.line {
                return Err(incoherent(
                    r.line,
                    "a cause not read before its observation",
                ));
            }
            let b = self.byte(cause);
            if cause != r.line && b & END != EXPECTED {
                conditional = true;
            }
            // A cause the harness sent late delays the effect itself; an
            // observation's own late start removes only polls whose effect
            // was absent anyway, and is the censor's to excuse (decision 17).
            late_cause = cause != r.line && b & LATE != 0;
        }
        if must_skip && end != SKIPPED {
            return Err(incoherent(
                r.line,
                "awaited as expected a failed step, and not skipped",
            ));
        }
        let undecided = (end == UNKNOWN && (me & UNDECIDED != 0 || late_cause))
            || (end == UNEXPECTED && me & UNDECIDED != 0);
        let class = if end == SKIPPED {
            Class::Consequent
        } else if conditional {
            Class::Inconclusive
        } else if undecided {
            Class::Undecided
        } else if end != EXPECTED || me & PRODUCER != 0 {
            Class::Root
        } else {
            Class::Passed
        };
        if conditional && let Some(b) = First::index(r.line).and_then(|i| self.bytes.get_mut(i)) {
            *b |= CONDITIONAL;
        }
        self.classes.push(class);
        let m = self.keys.entry(r.key.clone()).or_default();
        m.count = m.count.saturating_add(1);
        m.doubtful |= doubtful(me);
        m.conditional |= conditional;
        m.failed |= end != EXPECTED;
        Ok(())
    }
}

/// The fold of a run's journal: its header, its lines twice — the journal
/// read once for each pass — and the log the run was to run.
pub fn fold<A, B>(header: &Header, log: &Log, first: A, second: B) -> Folded
where
    A: IntoIterator<Item = Result<Line, JournalError>>,
    B: IntoIterator<Item = Result<Line, JournalError>>,
{
    let void = |v: Void, classes: Vec<Class>| Folded {
        fold: FOLD_REVISION,
        verdict: Verdict::Void(v),
        classes,
    };
    if header.log != log.sha256 {
        return void(
            Void::AnotherLog {
                journal: header.log.clone(),
                log: log.sha256.clone(),
            },
            Vec::new(),
        );
    }
    let mut one = First {
        jitter: header.jitter,
        floor: header.resend,
        bytes: Vec::new(),
        live: BTreeMap::new(),
        censors: BTreeMap::new(),
        next: 2,
        owed: None,
        fault: None,
        stop: None,
        taken: 0,
        latest: 0,
        unstarted: header.t0.is_none(),
    };
    for l in first {
        let step = l
            .map_err(|e| Void::Unreadable(e.to_string()))
            .and_then(|l| one.take(&l));
        if let Err(v) = step {
            return void(v, Vec::new());
        }
    }
    if let Err(v) = one.finish() {
        return void(v, Vec::new());
    }
    let read = u64::try_from(one.bytes.len()).unwrap_or(u64::MAX);
    // A stopped run read at most its log; its classes are none: the second
    // pass reads a whole run (decision 18).
    if let Some(class) = one.stop {
        if read > log.records {
            return void(
                Void::AnotherLog {
                    journal: format!("{read} lines read"),
                    log: format!("{} steps", log.records),
                },
                Vec::new(),
            );
        }
        return void(Void::Fault { class }, Vec::new());
    }
    if read != log.records {
        return void(
            Void::AnotherLog {
                journal: format!("{} lines read", one.bytes.len()),
                log: format!("{} steps", log.records),
            },
            Vec::new(),
        );
    }
    let fault = one.fault;
    let mut two = Second {
        bytes: one.bytes,
        keys: BTreeMap::new(),
        next: 2,
        classes: Vec::new(),
    };
    for l in second {
        let step = match l {
            Ok(Line::Read(r)) => two.read(&r),
            Ok(_) => Ok(()),
            Err(e) => Err(Void::Unreadable(e.to_string())),
        };
        if let Err(v) = step {
            return void(v, Vec::new());
        }
    }
    if two.next != one.next {
        return void(
            incoherent(0, "the second pass reads another journal"),
            Vec::new(),
        );
    }
    let classes = two.classes;
    let verdict = if let Some(line) = fault {
        Verdict::Void(Void::Fault {
            class: FaultClass::Backstop { line },
        })
    } else if classes.contains(&Class::Root) {
        Verdict::Fail
    } else if classes.contains(&Class::Undecided) {
        Verdict::Void(Void::Undecided)
    } else if classes.iter().all(|c| *c == Class::Passed) {
        Verdict::Pass
    } else {
        Verdict::Void(incoherent(0, "neither passed, failed nor undecided"))
    };
    Folded {
        fold: FOLD_REVISION,
        verdict,
        classes,
    }
}
