//! A deterministic stand (ADR-0047 § Executors): the machine driven in
//! virtual time by a script that says, for each attempt, when its bytes leave,
//! when its answer comes and how it is judged. A handler that does not answer
//! by the deadline reports unknown at it, as the shell's will.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use ascetic_ddd_command_log::LogError;
use ascetic_ddd_command_log::record::{Action, Dep, Header, Hole, Probe, Record, Subject, Value};

use crate::machine::{Captured, End, Entry, Input, Judgement, Line, Machine, Output};
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
}

impl Attempt {
    /// Sent at once and answered after `latency`, as expected, capturing
    /// nothing.
    pub fn expected(latency: u64) -> Attempt {
        Attempt {
            sent_after: 0,
            answer_after: Some(latency),
            judgement: Judgement::Expected(vec![]),
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
        &mut Scripted { script },
        true,
    )
}

/// What carries out the machine's dispatches: a script, or a model stand.
trait Handler {
    /// Attempt `attempt` of `record` dispatched at `at`, its captured holes
    /// filled from `captured`, to end by `deadline`: its inputs, queued.
    #[allow(clippy::too_many_arguments)]
    fn dispatch(
        &mut self,
        at: u64,
        line: Line,
        attempt: u32,
        record: &Record,
        captured: &[Captured],
        deadline: u64,
        queue: &mut Ordered,
    );
    /// The handler's own next instant, if it has one.
    fn next(&self) -> Option<u64> {
        None
    }
    /// What it does up to `t`, its inputs queued.
    fn advance(&mut self, t: u64, queue: &mut Ordered) {
        let _ = (t, queue);
    }
}

/// The ending of an attempt answered at `answered`, or unknown at its deadline.
fn answer(
    queue: &mut Ordered,
    line: Line,
    attempt: u32,
    answered: Option<u64>,
    deadline: u64,
    judgement: Judgement,
) {
    match answered {
        Some(t) if t <= deadline => queue.push(
            t,
            Input::Ended {
                line,
                attempt,
                judgement,
            },
        ),
        _ => queue.push(
            deadline,
            Input::Ended {
                line,
                attempt,
                judgement: Judgement::Unknown,
            },
        ),
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
        queue: &mut Ordered,
    ) {
        let a = (self.script)(line, attempt, record);
        let sent = at.saturating_add(a.sent_after);
        if sent < deadline {
            queue.push(sent, Input::Sent { line, attempt });
        }
        // An attempt whose bytes never left is answered by nobody.
        let answered = (sent < deadline)
            .then_some(a.answer_after)
            .flatten()
            .map(|d| sent.saturating_add(d));
        answer(queue, line, attempt, answered, deadline, a.judgement);
    }
}

/// The machine against `handler`, the lines as `records` yields them; with
/// `keep`, its entries, dispatches and inputs kept in the run.
fn drive(
    header: Header,
    records: &mut dyn Iterator<Item = Arc<Record>>,
    read_ahead: u64,
    lag: &dyn Fn(Line) -> u64,
    handler: &mut dyn Handler,
    keep: bool,
) -> Result<Run, LogError> {
    let mut machine = Machine::unbounded(Arc::new(header), read_ahead);
    let mut queue = Ordered::new();
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
    };
    queue.push(0, Input::Wake);
    loop {
        // The next instant: an input, a wake, the handler's own, or the next
        // line's reading.
        let reading = pending.as_ref().map(|(t, _)| (*t).max(machine.now()));
        let Some(t) = [
            queue.first(),
            wakes.first().copied(),
            handler.next(),
            reading,
        ]
        .into_iter()
        .flatten()
        .min() else {
            break;
        };
        handler.advance(t, &mut queue);
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
        while let Some((at, input)) = queue.pop(t) {
            if keep && input != Input::Wake {
                run.inputs.push((at, input.clone()));
            }
            taken = taken.saturating_add(1);
            if taken > INPUTS {
                return Err(LogError::Malformed {
                    line: 0,
                    reason: format!("the machine took more than {INPUTS} inputs: it loops"),
                });
            }
            for output in machine.step(at, input)? {
                match output {
                    Output::Want(_) => {}
                    Output::WakeAt(w) => {
                        wakes.insert(w.max(at));
                    }
                    Output::Dispatch {
                        line,
                        attempt,
                        deadline,
                        captured,
                    } => {
                        if let Some(record) = live.get(&line) {
                            handler.dispatch(
                                at, line, attempt, record, &captured, deadline, &mut queue,
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
    let mut stand = Stand {
        header: header.clone(),
        delays,
        records: records.to_vec(),
        arriving: BTreeMap::new(),
        versions: BTreeMap::new(),
        processed: BTreeSet::new(),
    };
    drive(
        header,
        &mut records.iter().cloned().map(Arc::new),
        read_ahead,
        &|_| 0,
        &mut stand,
        true,
    )
}

/// A request on its way: what it carries, the delay of its answer, its
/// deadline.
type Arriving = (Vec<Captured>, u64, u64);

struct Stand<'a> {
    header: Header,
    delays: &'a dyn Fn(Line, u32) -> (u64, u64, u64),
    records: Vec<Record>,
    /// Requests on their way: when they arrive, which, with what, and the
    /// delay of their answer and their deadline.
    arriving: BTreeMap<(u64, Line, u32), Arriving>,
    versions: BTreeMap<(Subject, String), u64>,
    processed: BTreeSet<Line>,
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
        queue: &mut Ordered,
    ) {
        let (sent_after, there, back) = (self.delays)(line, attempt);
        let sent = at.saturating_add(sent_after);
        if sent >= deadline {
            answer(queue, line, attempt, None, deadline, Judgement::Unknown);
            return;
        }
        queue.push(sent, Input::Sent { line, attempt });
        self.arriving.insert(
            (sent.saturating_add(there), line, attempt),
            (captured.to_vec(), back, deadline),
        );
    }

    fn next(&self) -> Option<u64> {
        self.arriving.first_key_value().map(|(k, _)| k.0)
    }

    fn advance(&mut self, t: u64, queue: &mut Ordered) {
        while let Some((&(arrived, line, attempt), _)) = self.arriving.first_key_value() {
            if arrived > t {
                break;
            }
            let Some((_, (captured, back, deadline))) = self.arriving.pop_first() else {
                break;
            };
            let judgement = self.process(line, &captured);
            answer(
                queue,
                line,
                attempt,
                Some(arrived.saturating_add(back)),
                deadline,
                judgement,
            );
        }
    }
}
