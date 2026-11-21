//! A deterministic stand (ADR-0047 § Executors): the machine driven in
//! virtual time by a script that says, for each attempt, when its bytes leave,
//! when its answer comes and how it is judged. A handler that does not answer
//! by the deadline reports unknown at it, as the shell's will.

use std::collections::BTreeSet;
use std::sync::Arc;

use ascetic_ddd_command_log::LogError;
use ascetic_ddd_command_log::record::{Header, Record};

use crate::machine::{Captured, Entry, Input, Judgement, Line, Machine, Output};
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

/// [`run`] with a reader that lags: a line is read `lag(line)` microseconds
/// after it may be, and never before the line above it.
pub fn run_reading(
    header: Header,
    records: &[Record],
    read_ahead: u64,
    lag: &dyn Fn(Line) -> u64,
    script: &dyn Fn(Line, u32, &Record) -> Attempt,
) -> Result<Run, LogError> {
    let records: Vec<Arc<Record>> = records.iter().cloned().map(Arc::new).collect();
    let mut machine = Machine::new(Arc::new(header), read_ahead);
    let mut queue = Ordered::new();
    let mut wakes: BTreeSet<u64> = BTreeSet::new();
    let mut next = 0usize;
    // When each line is read: once it may be, plus its lag, in order.
    let mut reads = Vec::with_capacity(records.len());
    for (i, r) in records.iter().enumerate() {
        let line = (i as u64).saturating_add(2);
        let may = r.at.saturating_sub(read_ahead).saturating_add(lag(line));
        let after = reads.last().copied().unwrap_or(0);
        reads.push(may.max(after));
    }
    let mut taken = 0u64;
    let mut run = Run {
        entries: Vec::new(),
        dispatches: Vec::new(),
        held: 0,
    };
    queue.push(0, Input::Wake);
    loop {
        // The next instant: an input, a wake, or the next line's reading.
        let reading = reads.get(next).map(|t| (*t).max(machine.now()));
        let Some(t) = [queue.first(), wakes.first().copied(), reading]
            .into_iter()
            .flatten()
            .min()
        else {
            break;
        };
        while let (Some(r), Some(when)) = (records.get(next), reads.get(next)) {
            if (*when).max(machine.now()) > t {
                break;
            }
            let line = (next as u64).saturating_add(2);
            queue.push(
                t,
                Input::Line {
                    line,
                    record: r.clone(),
                },
            );
            next = next.saturating_add(1);
        }
        while wakes.first().is_some_and(|w| *w <= t) {
            wakes.pop_first();
            queue.push(t, Input::Wake);
        }
        while let Some((at, input)) = queue.pop(t) {
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
                        run.dispatches.push((at, line, attempt, captured));
                        let index = usize::try_from(line.saturating_sub(2)).unwrap_or(usize::MAX);
                        let Some(record) = records.get(index) else {
                            continue;
                        };
                        let a = script(line, attempt, record);
                        let sent = at.saturating_add(a.sent_after);
                        if sent < deadline {
                            queue.push(sent, Input::Sent { line, attempt });
                        }
                        match a.answer_after.map(|d| sent.saturating_add(d)) {
                            Some(answered) if answered <= deadline => queue.push(
                                answered,
                                Input::Ended {
                                    line,
                                    attempt,
                                    judgement: a.judgement,
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
                    Output::Ended(e) => run.entries.push(*e),
                }
            }
            run.held = run.held.max(machine.held());
        }
    }
    run.entries.sort_by_key(|e| e.line);
    Ok(run)
}
