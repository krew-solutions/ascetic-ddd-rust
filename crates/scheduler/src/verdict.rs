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
    Action, Dep, End, Ended, Header, JournalError, Judged, Key, Line, Read, Skip,
};
use crate::machine::Line as Number;

/// What the fold decides, as a number: raised whenever the classes or the
/// verdict it gives a journal change, which the goldens' and the differential's
/// classes pin beside it.
pub const FOLD_REVISION: u32 = 3;

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
    /// A harness fault.
    Fault {
        /// The first step it struck.
        line: Number,
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
        if let Some(owed) = self.owed.take() {
            return match l {
                Line::Fault(f) if f.line == owed => {
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
                }
                lv.attempts = d.attempt;
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
                let next = self.next;
                match (self.live.get_mut(&a.line), a.ignored) {
                    (Some(lv), None) => {
                        if a.attempt != lv.attempts || lv.attempts == 0 {
                            return Err(incoherent(
                                a.line,
                                "an answer taken from an attempt not out",
                            ));
                        }
                        lv.taken = Some(a.judged);
                        lv.answered = Some((a.attempt, a.at, a.after));
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
            Line::Fault(f) => Err(incoherent(f.line, "a fault without its backstop's end")),
        }
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
        let late = e.sent.is_none_or(|s| s.saturating_sub(e.intended) > j);
        let call_late = lv.action != Action::Observe
            && lv.attempts >= 1
            && lv
                .last_sent
                .is_none_or(|s| s.saturating_sub(lv.last_dispatch) > j);
        let read_late = lv
            .released
            .is_some_and(|(i, r, d)| r >= d && i < d && r.saturating_sub(i) > j);
        // A wake the harness took too late (ADR-0047 decision 16), whatever
        // its lateness: an unknown ended past its deadline, released before it
        // — only a wake due before the deadline and taken past it ends a step
        // there; a resend its last answer licensed, due more than J before the
        // deadline, the step ended without it — the machine schedules no other
        // and, taken on time, it leaves.
        let ended_late = e.end == End::Unknown
            && lv.attempts >= 1
            && lv.released.is_some_and(|(_, r, d)| r < d && e.ended > d);
        let resend_late = lv.action != Action::Poll
            && match (lv.taken, lv.answered, lv.released) {
                (Some(taken), Some((_, at, after)), Some((_, _, d))) => {
                    let licensed = match taken {
                        Judged::NotTaken => lv.safety != Safety::Never,
                        Judged::Unknown => lv.safety == Safety::Idempotent,
                        Judged::Expected | Judged::Unexpected => false,
                    };
                    licensed
                        && at
                            .saturating_add(after.unwrap_or(0).max(self.floor))
                            .saturating_add(j)
                            < d
                }
                _ => false,
            };
        let mut byte = end_code(e.end);
        if late {
            byte |= LATE;
        }
        if (e.end == End::Unknown && (e.fault || call_late || read_late || ended_late))
            || (matches!(e.end, End::Unknown | End::Unexpected) && resend_late)
        {
            byte |= UNDECIDED;
        }
        self.mark(e.line, byte);
        if e.fault {
            self.owed = Some(e.line);
        }
        Ok(())
    }

    fn finish(&self) -> Result<(), Void> {
        if let Some(owed) = self.owed {
            return Err(incoherent(owed, "a backstop's end without its fault"));
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
            late_cause = b & LATE != 0;
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
        next: 2,
        owed: None,
        fault: None,
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
    if u64::try_from(one.bytes.len()).ok() != Some(log.records) {
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
        Verdict::Void(Void::Fault { line })
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
