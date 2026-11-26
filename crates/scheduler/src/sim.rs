//! A deterministic stand (ADR-0047 § Executors): the machine driven in
//! virtual time by a script that says, for each attempt, when its bytes leave,
//! when its answer comes and how it is judged. A handler that does not answer
//! by the deadline reports unknown at it, as the shell's will.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use ascetic_ddd_command_log::LogError;
use ascetic_ddd_command_log::record::{Action, Dep, Header, Hole, Probe, Record, Subject, Value};

use crate::frontier::Frontier;
use crate::journal;
use crate::machine::{
    Captured, Code, End, Entry, Input, Judgement, Line, Machine, Output, Refusal, Seen, Unsent,
};
use crate::order::Ordered;

/// What becomes of one attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Attempt {
    /// Microseconds from dispatch until its bytes leave.
    pub sent_after: u64,
    /// Microseconds from its bytes leaving until its answer, if it comes.
    pub answer_after: Option<u64>,
    /// How its answer is judged.
    pub judgement: Judgement,
    /// Microseconds its answer's `Retry-After` asks for (ADR-0047 decision
    /// 15).
    pub after: Option<u64>,
}

impl Attempt {
    /// Sent at once and answered after `latency`, as expected, capturing
    /// nothing.
    pub fn expected(latency: u64) -> Attempt {
        Attempt {
            sent_after: 0,
            answer_after: Some(latency),
            judgement: Judgement::Expected(vec![]),
            after: None,
        }
    }
}

/// What a run did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Run {
    /// Every step's entry, by line.
    pub entries: Vec<Entry>,
    /// Every dispatch: when, which line, which attempt, with what captured.
    pub dispatches: Vec<(u64, Line, u32, Vec<Captured>)>,
    /// The most steps the machine held at once.
    pub held: usize,
    /// Every input the machine took, with its stamp, in the order it took
    /// them; wakes left out (a trace of the run: verify/tla/TraceScheduler.tla).
    pub inputs: Vec<(u64, Input)>,
    /// The steps the machine held when the run ended, and those its window
    /// kept: every other cell freed, they are equal.
    pub left: usize,
    /// See `left`.
    pub kept: usize,
    /// How many steps ended each way, kept or not: expected, unexpected,
    /// unknown, skipped.
    pub ends: [u64; 4],
    /// Attempts dispatched past each step's first.
    pub resent: u64,
    /// The most steps read and not ended at once.
    pub pending: usize,
    /// The barrier keys the machine kept by the end.
    pub barriers: usize,
    /// The journal's lines between its header and its trailer, as
    /// [`journal::events`] writes them: only with the run's inputs kept.
    pub journal: Vec<journal::Line>,
}

/// Runs `records` (lines 2, 3, …) against the script, reading `read_ahead`
/// microseconds ahead of the clock.
pub fn run(
    header: Header,
    records: &[Record],
    read_ahead: u64,
    script: &dyn Fn(Line, u32, &Record) -> Attempt,
) -> Result<Run, LogError> {
    run_reading(header, records, read_ahead, &|_| 0, script)
}

/// The most inputs a run takes: past it a machine that loops is stopped.
pub const INPUTS: u64 = 1 << 22;

/// [`run`] over lines as an iterator yields them, keeping no entry, dispatch
/// or input: only the counts. What it holds is the machine's own and the
/// lines in flight — what a heap executor measures.
pub fn run_streaming(
    header: Header,
    records: &mut dyn Iterator<Item = Record>,
    read_ahead: u64,
    script: &dyn Fn(Line, u32, &Record) -> Attempt,
) -> Result<Run, LogError> {
    drive(
        header,
        &mut records.map(Arc::new),
        read_ahead,
        &|_| 0,
        &|_| 0,
        &mut Scripted { script },
        false,
    )
}

/// [`run`] with a reader that lags: a line is read `lag(line)` microseconds
/// after it may be, and never before the line above it.
pub fn run_reading(
    header: Header,
    records: &[Record],
    read_ahead: u64,
    lag: &dyn Fn(Line) -> u64,
    script: &dyn Fn(Line, u32, &Record) -> Attempt,
) -> Result<Run, LogError> {
    drive(
        header,
        &mut records.iter().cloned().map(Arc::new),
        read_ahead,
        lag,
        &|_| 0,
        &mut Scripted { script },
        true,
    )
}

/// [`run_reading`] with the machine's wakes taken late: a wake for `w` is
/// taken at `w + wake_lag(w)`, as a real timer's slack takes it (ADR-0047
/// decision 7's A3 broken on purpose, decision 16).
pub fn run_waking_late(
    header: Header,
    records: &[Record],
    read_ahead: u64,
    lag: &dyn Fn(Line) -> u64,
    wake_lag: &dyn Fn(u64) -> u64,
    script: &dyn Fn(Line, u32, &Record) -> Attempt,
) -> Result<Run, LogError> {
    drive(
        header,
        &mut records.iter().cloned().map(Arc::new),
        read_ahead,
        lag,
        wake_lag,
        &mut Scripted { script },
        true,
    )
}

/// What carries out the machine's dispatches: a script, or a model stand.
trait Handler {
    /// Attempt `attempt` of `record` dispatched at `at`, its captured holes
    /// filled from `captured`, to end by `deadline`: what becomes of it, on the
    /// timeline.
    #[allow(clippy::too_many_arguments)]
    fn dispatch(
        &mut self,
        at: u64,
        line: Line,
        attempt: u32,
        record: &Record,
        captured: &[Captured],
        deadline: u64,
        timeline: &mut Timeline,
    );
    /// The handler's own next instant, if it has one.
    fn next(&self) -> Option<u64> {
        None
    }
    /// What it does up to `t`, on the timeline.
    fn advance(&mut self, t: u64, timeline: &mut Timeline) {
        let _ = (t, timeline);
    }
}

/// What an attempt's handler sees, at an instant to come.
#[derive(Debug)]
enum Event {
    Sent {
        line: Line,
        attempt: u32,
    },
    Answer {
        line: Line,
        attempt: u32,
        judgement: Judgement,
        after: Option<u64>,
    },
    /// A never-sent attempt's closing report (ADR-0047 decision 19).
    Left {
        line: Line,
        attempt: u32,
        unsent: Unsent,
    },
}

/// The closing report `sim` draws for a never-sent attempt dispatched at
/// `dispatched` — and the shell's scripted carrier, so that both write the
/// same bytes: every side of the fold's rule over the lines, by their numbers
/// (ADR-0047 decision 19). No lag: `sim`'s time is virtual.
pub fn drawn_unsent(line: Line, attempt: u32, dispatched: u64) -> Unsent {
    let a = u64::from(attempt);
    let refused = u32::try_from(line.wrapping_add(a.wrapping_mul(2)) % 3).unwrap_or(0);
    let handed = (line.wrapping_add(a) % 4 != 0).then_some(dispatched);
    Unsent {
        decorated: handed,
        handed,
        connected: None,
        lag: 0,
        refused,
        refused_by: (refused > 0).then_some(match line % 4 {
            0 => Refusal::Refused,
            1 => Refusal::Reset,
            2 => Refusal::Aborted,
            _ => Refusal::Closed,
        }),
        code: Code::of("sim.unsent"),
    }
}

/// A never-sent attempt's report, a microsecond past its deadline: after
/// its censor, as the shell's.
fn left(timeline: &mut Timeline, line: Line, attempt: u32, dispatched: u64, deadline: u64) {
    timeline.add(
        deadline.saturating_add(1),
        Event::Left {
            line,
            attempt,
            unsent: drawn_unsent(line, attempt, dispatched),
        },
    );
}

/// The handlers' events to come, given to the frontier as their instants come
/// (ADR-0047 decision 16): what the shell's tasks stamp as they see them.
#[derive(Debug, Default)]
pub(crate) struct Timeline {
    events: BTreeMap<(u64, u64), Event>,
    seq: u64,
}

impl Timeline {
    fn add(&mut self, at: u64, e: Event) {
        self.seq = self.seq.saturating_add(1);
        self.events.insert((at, self.seq), e);
    }

    fn first(&self) -> Option<u64> {
        self.events.first_key_value().map(|(k, _)| k.0)
    }

    /// Everything seen up to `t`, given to the frontier in its order.
    fn apply(&mut self, t: u64, frontier: &mut Frontier) -> Result<(), LogError> {
        let below = |b: crate::frontier::Below| LogError::Malformed {
            line: 0,
            reason: b.to_string(),
        };
        while let Some(entry) = self.events.first_entry() {
            let at = entry.key().0;
            if at > t {
                break;
            }
            match entry.remove() {
                Event::Sent { line, attempt } => {
                    frontier.sent(at, line, attempt).map_err(below)?;
                }
                Event::Left {
                    line,
                    attempt,
                    unsent,
                } => frontier
                    .left(at, line, attempt, unsent)
                    .map_err(|e| LogError::Malformed {
                        line: 0,
                        reason: e.to_string(),
                    })?,
                Event::Answer {
                    line,
                    attempt,
                    judgement,
                    after,
                } => {
                    let hold = frontier.arrived(at, line, attempt).map_err(below)?;
                    let seen = Seen {
                        after,
                        ..Seen::default()
                    };
                    frontier.ended(hold, judgement, seen).map_err(below)?;
                }
            }
        }
        Ok(())
    }
}

/// The answer of an attempt answered at `answered`, asking for `after`: on
/// the timeline if it comes by the deadline. `sim` models no answer past it:
/// the frontier censors the attempt at its deadline.
fn answer(
    timeline: &mut Timeline,
    line: Line,
    attempt: u32,
    answered: Option<u64>,
    deadline: u64,
    judgement: Judgement,
    after: Option<u64>,
) {
    if let Some(t) = answered
        && t <= deadline
    {
        timeline.add(
            t,
            Event::Answer {
                line,
                attempt,
                judgement,
                after,
            },
        );
    }
}

struct Scripted<'a> {
    script: &'a dyn Fn(Line, u32, &Record) -> Attempt,
}

impl Handler for Scripted<'_> {
    fn dispatch(
        &mut self,
        at: u64,
        line: Line,
        attempt: u32,
        record: &Record,
        _: &[Captured],
        deadline: u64,
        timeline: &mut Timeline,
    ) {
        let a = (self.script)(line, attempt, record);
        let sent = at.saturating_add(a.sent_after);
        if sent < deadline {
            timeline.add(sent, Event::Sent { line, attempt });
        } else {
            left(timeline, line, attempt, at, deadline);
        }
        // An attempt whose bytes never left is answered by nobody.
        let answered = (sent < deadline)
            .then_some(a.answer_after)
            .flatten()
            .map(|d| sent.saturating_add(d));
        answer(
            timeline,
            line,
            attempt,
            answered,
            deadline,
            a.judgement,
            a.after,
        );
    }
}

/// The machine against `handler`, the lines as `records` yields them; with
/// `keep`, its entries, dispatches and inputs kept in the run.
fn drive(
    header: Header,
    records: &mut dyn Iterator<Item = Arc<Record>>,
    read_ahead: u64,
    lag: &dyn Fn(Line) -> u64,
    wake_lag: &dyn Fn(u64) -> u64,
    handler: &mut dyn Handler,
    keep: bool,
) -> Result<Run, LogError> {
    let mut machine = Machine::unbounded(Arc::new(header), read_ahead);
    let mut queue = Ordered::new();
    let mut timeline = Timeline::default();
    let mut frontier = Frontier::new();
    // What the handlers saw up to `t`, through the frontier, into the queue:
    // the instant settled, as nothing in `sim` is stamped later at it.
    let settle = |t: u64,
                  timeline: &mut Timeline,
                  frontier: &mut Frontier,
                  queue: &mut Ordered|
     -> Result<(), LogError> {
        timeline.apply(t, frontier)?;
        for (at, input) in frontier.drain(t, true) {
            queue.push(at, input);
        }
        Ok(())
    };
    let mut wakes: BTreeSet<u64> = BTreeSet::new();
    // The lines read and not ended, for their dispatches.
    let mut live: BTreeMap<Line, Arc<Record>> = BTreeMap::new();
    // The next line to read and when: once it may be, plus its lag, and not
    // before the line above it.
    let upcoming = |line: Line, last: u64, records: &mut dyn Iterator<Item = Arc<Record>>| {
        records.next().map(|r| {
            let may = r.at.saturating_sub(read_ahead).saturating_add(lag(line));
            (may.max(last), r)
        })
    };
    let mut line: Line = 2;
    let mut pending = upcoming(line, 0, records);
    let mut taken = 0u64;
    let mut run = Run {
        entries: Vec::new(),
        dispatches: Vec::new(),
        held: 0,
        inputs: Vec::new(),
        left: 0,
        kept: 0,
        ends: [0; 4],
        resent: 0,
        pending: 0,
        barriers: 0,
        journal: Vec::new(),
    };
    queue.push(0, Input::Wake);
    // Every turn takes an input, applies an event or moves time on; a run
    // turning more often than that loops (a frontier that never censors a
    // deadline it names, say), and is stopped rather than left to hang.
    let mut turns = 0u64;
    loop {
        turns = turns.saturating_add(1);
        if turns > INPUTS.saturating_mul(4) {
            return Err(LogError::Malformed {
                line: 0,
                reason: format!("the run turned {turns} times: it loops"),
            });
        }
        // The next instant: an input, a wake, the handler's own, or the next
        // line's reading.
        let reading = pending.as_ref().map(|(t, _)| (*t).max(machine.now()));
        let Some(t) = [
            queue.first(),
            wakes.first().copied(),
            handler.next(),
            timeline.first(),
            frontier.next_deadline(),
            reading,
        ]
        .into_iter()
        .flatten()
        .min() else {
            break;
        };
        handler.advance(t, &mut timeline);
        while let Some((when, r)) = pending.take() {
            if when.max(machine.now()) > t {
                pending = Some((when, r));
                break;
            }
            live.insert(line, r.clone());
            queue.push(t, Input::Line { line, record: r });
            line = line.saturating_add(1);
            pending = upcoming(line, when, records);
        }
        while wakes.first().is_some_and(|w| *w <= t) {
            wakes.pop_first();
            queue.push(t, Input::Wake);
        }
        settle(t, &mut timeline, &mut frontier, &mut queue)?;
        while let Some((at, input)) = queue.pop(t) {
            let taken_input = keep.then(|| input.clone());
            if let Some(input) = &taken_input
                && *input != Input::Wake
            {
                run.inputs.push((at, input.clone()));
            }
            taken = taken.saturating_add(1);
            if taken > INPUTS {
                return Err(LogError::Malformed {
                    line: 0,
                    reason: format!("the machine took more than {INPUTS} inputs: it loops"),
                });
            }
            let outputs = machine.step(at, input)?;
            if let Some(input) = &taken_input {
                run.journal.extend(journal::events(at, input, &outputs));
            }
            for output in outputs {
                match output {
                    Output::Want(_)
                    | Output::Bound { .. }
                    | Output::Released { .. }
                    | Output::Ignored { .. } => {}
                    Output::WakeAt(w) => {
                        // Taken late by `wake_lag`, as a real timer may.
                        wakes.insert(w.max(at).saturating_add(wake_lag(w)));
                    }
                    Output::Dispatch {
                        line,
                        attempt,
                        deadline,
                        captured,
                        ..
                    } => {
                        if let Some((at, censor)) = frontier.open(at, line, attempt, deadline) {
                            queue.push(at, censor);
                        }
                        if let Some(record) = live.get(&line) {
                            handler.dispatch(
                                at,
                                line,
                                attempt,
                                record,
                                &captured,
                                deadline,
                                &mut timeline,
                            );
                        }
                        if attempt > 1 {
                            run.resent = run.resent.saturating_add(1);
                        }
                        if keep {
                            run.dispatches.push((at, line, attempt, captured));
                        }
                    }
                    Output::Ended(e) => {
                        live.remove(&e.line);
                        let kind = match e.end {
                            End::Expected => 0,
                            End::Unexpected => 1,
                            End::Unknown => 2,
                            End::Skipped(_) => 3,
                        };
                        if let Some(n) = run.ends.get_mut(kind) {
                            *n = n.saturating_add(1);
                        }
                        if keep {
                            run.entries.push(*e);
                        }
                    }
                }
            }
            run.held = run.held.max(machine.held());
            run.pending = run.pending.max(live.len());
            settle(t, &mut timeline, &mut frontier, &mut queue)?;
        }
    }
    run.entries.sort_by_key(|e| e.line);
    run.left = machine.held();
    run.kept = machine.kept();
    run.barriers = machine.barriers();
    Ok(run)
}

/// A model stand from the host's contracts (ADR-0047 Rule of Three, 3): a
/// resource's ETag a version that each write of it raises, a request whose
/// `If-Match` names another version refused; a barrier's holder answered as
/// expected only if every member read before it was processed first;
/// requests processed in the order they arrive, which their delays choose. It
/// knows nothing of the decision's sentences: a clean log must end with every
/// step expected under every delay that fails nothing.
pub fn run_stand(
    header: Header,
    records: &[Record],
    read_ahead: u64,
    delays: &dyn Fn(Line, u32) -> (u64, u64, u64),
) -> Result<Run, LogError> {
    run_stand_failing(header, records, read_ahead, delays, None)
}

/// A failure the stand injects at a step's first attempt (ADR-0047 K1e-3d):
/// what a network or a server does, not what the decision says of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    /// The request is lost on its way: never processed, never answered.
    LostRequest,
    /// The request is processed, its answer lost.
    LostAnswer,
    /// The request is refused unprocessed, with an answer the contract
    /// defines and `expect` does not list.
    Refused,
}

/// [`run_stand`] with `failure` injected at a line's first attempt.
pub fn run_stand_failing(
    header: Header,
    records: &[Record],
    read_ahead: u64,
    delays: &dyn Fn(Line, u32) -> (u64, u64, u64),
    failure: Option<(Line, Failure)>,
) -> Result<Run, LogError> {
    let mut stand = Stand {
        header: header.clone(),
        delays,
        records: records.to_vec(),
        arriving: BTreeMap::new(),
        versions: BTreeMap::new(),
        processed: BTreeSet::new(),
        failure,
    };
    drive(
        header,
        &mut records.iter().cloned().map(Arc::new),
        read_ahead,
        &|_| 0,
        &|_| 0,
        &mut stand,
        true,
    )
}

/// A request on its way: what it carries, the delay of its answer, its
/// deadline, the failure it meets.
type Arriving = (Vec<Captured>, u64, u64, Option<Failure>);

struct Stand<'a> {
    header: Header,
    delays: &'a dyn Fn(Line, u32) -> (u64, u64, u64),
    records: Vec<Record>,
    /// Requests on their way: when they arrive, which, with what, and the
    /// delay of their answer and their deadline.
    arriving: BTreeMap<(u64, Line, u32), Arriving>,
    versions: BTreeMap<(Subject, String), u64>,
    /// The requests the stand has handled, refused or not: what a barrier's
    /// holder must follow (a refusal is the server's answer, not a request
    /// that never arrived).
    processed: BTreeSet<Line>,
    failure: Option<(Line, Failure)>,
}

impl Stand<'_> {
    fn record(&self, line: Line) -> Option<&Record> {
        self.records
            .get(usize::try_from(line.saturating_sub(2)).unwrap_or(usize::MAX))
    }

    /// What the stand makes of a request: its state changed, its answer.
    fn process(&mut self, line: Line, captured: &[Captured]) -> Judgement {
        let Some(r) = self.record(line).cloned() else {
            return Judgement::Unknown;
        };
        self.processed.insert(line);
        let rest = match &r.action {
            Action::Rest(rest) => Some(rest.clone()),
            Action::Observe(o) => match &o.probe {
                Probe::Rest(rest) => Some(rest.clone()),
                Probe::Bus { .. } => None,
            },
            _ => None,
        };
        if let Some(rest) = rest {
            // An If-Match names the version it was captured at.
            for f in &rest.headers {
                if let Value::Hole(Hole::Captured { subject, slot, .. }) = &f.value
                    && f.name.eq_ignore_ascii_case("If-Match")
                {
                    let now = self
                        .versions
                        .get(&(subject.clone(), slot.clone()))
                        .copied()
                        .unwrap_or(0);
                    let sent = captured
                        .iter()
                        .find(|c| &c.subject == subject && &c.slot == slot)
                        .map(|c| c.value.clone());
                    if sent != Some(now.to_string().into_bytes()) {
                        return Judgement::Unexpected;
                    }
                }
            }
            if matches!(r.action, Action::Rest(_))
                && let Some(op) = self.header.operations.iter().find(|o| o.name == rest.op)
            {
                for slot in &op.writes {
                    let v = self
                        .versions
                        .entry((r.step.subject.clone(), slot.clone()))
                        .or_insert(0);
                    *v = v.saturating_add(1);
                }
            }
        }
        // A barrier's holder counts the members processed before it.
        for d in &r.after {
            if let Dep::Each {
                kind,
                scope,
                label,
                n,
                ..
            } = d
            {
                let member = |e: &Record| {
                    &e.step.subject.kind == kind
                        && &e.step.subject.scope == scope
                        && &e.step.label == label
                        && e.step.n == *n
                };
                let index = usize::try_from(line.saturating_sub(2)).unwrap_or(0);
                let before: Vec<Line> = self
                    .records
                    .iter()
                    .take(index)
                    .enumerate()
                    .filter(|(_, e)| member(e))
                    .map(|(i, _)| (i as u64).saturating_add(2))
                    .collect();
                if before.iter().any(|m| !self.processed.contains(m)) {
                    return Judgement::Unexpected;
                }
            }
        }
        let captures = r
            .capture
            .iter()
            .map(|c| Captured {
                subject: c.subject.clone(),
                slot: c.slot.clone(),
                value: self
                    .versions
                    .get(&(c.subject.clone(), c.slot.clone()))
                    .copied()
                    .unwrap_or(0)
                    .to_string()
                    .into_bytes(),
            })
            .collect();
        Judgement::Expected(captures)
    }
}

impl Handler for Stand<'_> {
    fn dispatch(
        &mut self,
        at: u64,
        line: Line,
        attempt: u32,
        _: &Record,
        captured: &[Captured],
        deadline: u64,
        timeline: &mut Timeline,
    ) {
        let (sent_after, there, back) = (self.delays)(line, attempt);
        let sent = at.saturating_add(sent_after);
        if sent >= deadline {
            left(timeline, line, attempt, at, deadline);
            return;
        }
        timeline.add(sent, Event::Sent { line, attempt });
        let failure = self
            .failure
            .filter(|(l, _)| *l == line && attempt == 1)
            .map(|(_, f)| f);
        if failure == Some(Failure::LostRequest) {
            answer(
                timeline,
                line,
                attempt,
                None,
                deadline,
                Judgement::Unknown,
                None,
            );
            return;
        }
        self.arriving.insert(
            (sent.saturating_add(there), line, attempt),
            (captured.to_vec(), back, deadline, failure),
        );
    }

    fn next(&self) -> Option<u64> {
        self.arriving.first_key_value().map(|(k, _)| k.0)
    }

    fn advance(&mut self, t: u64, timeline: &mut Timeline) {
        while let Some((&(arrived, line, attempt), _)) = self.arriving.first_key_value() {
            if arrived > t {
                break;
            }
            let Some((_, (captured, back, deadline, failure))) = self.arriving.pop_first() else {
                break;
            };
            let (judgement, answered) = match failure {
                Some(Failure::Refused) => {
                    self.processed.insert(line);
                    (Judgement::Unexpected, true)
                }
                Some(Failure::LostAnswer) => (self.process(line, &captured), false),
                Some(Failure::LostRequest) | None => (self.process(line, &captured), true),
            };
            answer(
                timeline,
                line,
                attempt,
                answered.then(|| arrived.saturating_add(back)),
                deadline,
                if answered {
                    judgement
                } else {
                    Judgement::Unknown
                },
                None,
            );
        }
    }
}
