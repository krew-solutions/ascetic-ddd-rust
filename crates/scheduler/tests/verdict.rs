//! The verdict (ADR-0047 decision 14, K1e-3c-2): golden runs, each with its
//! classes and verdict written by hand; the fold against the verdict by
//! definition (`vectors/verdict.py`) on every golden and every twin; twins of
//! broken evidence, each journal resealed so that only the fold can refuse it.

// The command log's fixture reads its corpus's directory; the lint gate refuses
// files to the machine, not to its tests (ADR-0047 K1e-3).
#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

mod admission;

use std::io::{Cursor, Write as _};
use std::process::{Command, Stdio};
use std::sync::Arc;

use ascetic_ddd_command_log::record::{Dep, Header, On, Record, Safety, StepId};
use ascetic_ddd_scheduler::journal::{self, Ended, JOURNAL_VERSION, Line, Reader, Writer};
use ascetic_ddd_scheduler::machine::{Input, Judgement, MACHINE_REVISION, Machine, Output};
use ascetic_ddd_scheduler::sim::{Attempt, run_waking_late};
use ascetic_ddd_scheduler::verdict::{Class, FOLD_REVISION, Folded, Log, Verdict, Void, fold};
use fixture::*;

const J: u64 = 200;

fn header_j() -> Header {
    let mut h = header();
    h.profile.jitter = J;
    h
}

fn log_of(header: &Header, records: &[Record]) -> Log {
    let text = String::from_utf8(bytes(header.clone(), records)).unwrap();
    let trailer: serde_json::Value = serde_json::from_str(text.lines().last().unwrap()).unwrap();
    Log {
        sha256: trailer["trailer"]["sha256"].as_str().unwrap().to_owned(),
        records: records.len() as u64,
    }
}

fn journal_header(header: &Header, log: &Log) -> journal::Header {
    journal::Header {
        journal: JOURNAL_VERSION,
        machine: MACHINE_REVISION,
        source: "verdict".to_owned(),
        log: log.sha256.clone(),
        // A test's input, not the machine's: pinned journals do not move
        // with the validator's revision.
        validator: "a test's validator".to_owned(),
        read_ahead: header.read_ahead,
        jitter: header.profile.jitter,
        resend: header.profile.resend,
        catch_up: header.profile.reserve / 2,
        namespace: 1,
        t0: Some(1_763_640_000_000_000),
        gates: Vec::new(),
        judging: None,
        admission: None,
    }
}

fn write(header: &journal::Header, lines: &[Line]) -> Vec<u8> {
    let mut w = Writer::new(Vec::new(), header.clone()).unwrap();
    for l in lines {
        w.line(l).unwrap();
    }
    w.finish().unwrap().0
}

/// The fold of a journal's bytes, read once for each pass.
fn fold_bytes(bytes: &[u8], log: &Log) -> Folded {
    let open = || Reader::new(Cursor::new(bytes.to_vec())).unwrap();
    let header = open().header().clone();
    fold(&header, log, open(), open())
}

fn verdict_name(v: &Verdict) -> &'static str {
    v.name()
}

/// The case the verdict by definition reads.
fn case(name: &str, bytes: &[u8], log: &Log, folded: &Folded) -> serde_json::Value {
    let lines: Vec<serde_json::Value> = std::str::from_utf8(bytes)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    // The run's stop, by its class's name (decision 18): the definition must
    // name the same.
    let stop = match &folded.verdict {
        Verdict::Void(Void::Fault { class }) if class.stops() => {
            match serde_json::to_value(class).unwrap() {
                serde_json::Value::String(s) => Some(s),
                serde_json::Value::Object(o) => o.keys().next().cloned(),
                other => panic!("a class written as {other}"),
            }
        }
        _ => None,
    };
    // Admission's evidence refused (K2b-7e-2b-3d-3b-3): by its rule's code,
    // which the definition must name the same.
    let why = match &folded.verdict {
        Verdict::Void(Void::Incoherent { line: 0, why }) if why.starts_with("admission_") => {
            Some(*why)
        }
        _ => None,
    };
    serde_json::json!({
        "name": name,
        "log": {"sha256": log.sha256, "records": log.records},
        "journal": lines,
        "verdict": verdict_name(&folded.verdict),
        "classes": folded.classes.iter().map(|c| c.name()).collect::<Vec<_>>(),
        "stop": stop,
        "why": why,
    })
}

/// The verdict by definition over `cases`: what it reached.
fn by_definition(cases: &[serde_json::Value]) -> String {
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/vectors/verdict.py");
    let mut child = Command::new("python3")
        .args(["-I", script])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("python3, which the library's gate requires");
    {
        let mut stdin = child.stdin.take().unwrap();
        for c in cases {
            if writeln!(stdin, "{c}").is_err() {
                break;
            }
        }
    }
    let out = child.wait_with_output().unwrap();
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        out.status.success(),
        "{text}{}",
        String::from_utf8_lossy(&out.stderr)
    );
    text
}

fn answer(sent_after: u64, answer_after: Option<u64>, judgement: Judgement) -> Attempt {
    Attempt {
        after: None,
        sent_after,
        answer_after,
        judgement,
    }
}

fn expected() -> Attempt {
    answer(0, Some(100), Judgement::Expected(vec![]))
}

fn watched(g: u64, at: u64, cause: &StepId, timeout: u64) -> Record {
    observe(
        after(step(id("device", g, "watched", 0), at), cause, On::Sent),
        0,
        timeout,
    )
}

/// A golden run: its log, a script by line and attempt, a lag by line, the
/// lateness of the machine's wakes by the instant each was due.
struct Golden {
    name: &'static str,
    header: Header,
    records: Vec<Record>,
    script: Box<dyn Fn(u64, u32) -> Attempt>,
    lag: Box<dyn Fn(u64) -> u64>,
    wake_lag: Box<dyn Fn(u64) -> u64>,
    classes: Vec<Class>,
    verdict: &'static str,
}

fn goldens() -> Vec<Golden> {
    use Class::*;
    let mut out = Vec::new();
    let mut add =
        |name, header, records, script: Box<dyn Fn(u64, u32) -> Attempt>, classes, verdict| {
            out.push(Golden {
                name,
                header,
                records,
                script,
                lag: Box::new(|_| 0),
                wake_lag: Box::new(|_| 0),
                classes,
                verdict,
            })
        };
    // Every step as expected.
    add(
        "clean",
        header_j(),
        vec![call("read", 1, 0, 0, 0), call("read", 2, 0, 100, 0)],
        Box::new(|_, _| expected()),
        vec![Passed, Passed],
        "pass",
    );
    // A failure, and the steps it skipped, counted once.
    let a = call("update", 1, 0, 0, 0);
    let b = after(call("read", 1, 0, 100, 0), &a.step, On::Expected);
    let c = after(call("read", 2, 0, 200, 0), &b.step, On::Expected);
    add(
        "root_and_skips",
        header_j(),
        vec![a, b, c],
        Box::new(|line, _| {
            if line == 2 {
                answer(0, Some(50), Judgement::Unexpected)
            } else {
                expected()
            }
        }),
        vec![Root, Consequent, Consequent],
        "fail",
    );
    // Doubt three levels deep: sent on an unknown, expected on that, settled
    // on that.
    let a = call("update", 1, 0, 0, 0);
    let b = after(call("read", 1, 0, 100, 0), &a.step, On::Sent);
    let c = after(call("read", 2, 0, 200, 0), &b.step, On::Expected);
    let d = after(call("read", 3, 0, 300, 0), &c.step, On::Settled);
    add(
        "three_levels_of_doubt",
        header_j(),
        vec![a, b, c, d],
        Box::new(|line, _| {
            if line == 2 {
                answer(0, Some(50), Judgement::Unknown)
            } else {
                expected()
            }
        }),
        vec![Root, Inconclusive, Inconclusive, Inconclusive],
        "fail",
    );
    // A dependent awaiting only the send ends long before its dependency,
    // which never answers.
    let a = call("update", 1, 0, 0, 0);
    let b = after(call("read", 1, 0, 100, 0), &a.step, On::Sent);
    add(
        "dependent_ends_first",
        header_j(),
        vec![a, b],
        Box::new(|line, _| {
            if line == 2 {
                answer(0, None, Judgement::Unknown)
            } else {
                expected()
            }
        }),
        vec![Root, Inconclusive],
        "fail",
    );
    // An observation asserts its cause's effect: its cause refused, it is in
    // doubt, though it awaited only the send (decision 6 amended).
    let p = call("update", 1, 0, 0, 0);
    let o = watched(1, 100, &p.step, TIMEOUT);
    add(
        "failed_cause_observed",
        header_j(),
        vec![p, o],
        Box::new(|line, _| {
            if line == 2 {
                answer(0, Some(50), Judgement::Unexpected)
            } else {
                expected()
            }
        }),
        vec![Root, Inconclusive],
        "fail",
    );
    // A producer answered without the value a later reader sends: the
    // producer is the root.
    let p = captures_etag(call("update", 1, 0, 0, 0), 1);
    let mut r = after(call("read", 1, 0, 100, 0), &p.step, On::Expected);
    r.action = read_etag("read", 1, &p.step);
    add(
        "producer_without_its_value",
        header_j(),
        vec![p, r],
        Box::new(|_, _| expected()),
        vec![Root, Consequent],
        "fail",
    );
    // A retry whose bytes never left: the harness ate the deadline.
    let mut h = header_j();
    for o in &mut h.operations {
        o.safety = Safety::Idempotent;
    }
    add(
        "retry_never_sent",
        h,
        vec![call("read", 1, 0, 0, 0)],
        Box::new(|_, attempt| {
            if attempt == 1 {
                answer(0, Some(10), Judgement::Unknown)
            } else {
                answer(2 * TIMEOUT, Some(10), Judgement::Expected(vec![]))
            }
        }),
        vec![Undecided],
        "void:undecided",
    );
    // A certified refusal resent, after the floor, and then expected.
    let mut resent = header_j();
    for o in &mut resent.operations {
        o.safety = Safety::NotTaken;
        o.not_run = vec!["service.unavailable".into()];
    }
    add(
        "refusal_resent",
        resent,
        vec![call("read", 1, 0, 0, 0)],
        Box::new(|_, attempt| {
            if attempt == 1 {
                answer(0, Some(10), Judgement::NotTaken)
            } else {
                expected()
            }
        }),
        vec![Passed],
        "pass",
    );
    // A poll whose last poll never left: the harness ate the deadline, as for
    // a call (ADR-0047 K2a-5b; fold revision 1 counted it a root).
    add(
        "last_poll_never_sent",
        header_j(),
        vec![polling(1, 0, 0, 1_000, 10_000)],
        Box::new(|_, attempt| {
            if attempt == 1 {
                answer(0, Some(10), Judgement::Unexpected)
            } else {
                answer(20_000, Some(10), Judgement::Expected(vec![]))
            }
        }),
        vec![Undecided],
        "void:undecided",
    );
    // A barrier settled and a step sent: one member unknown makes the holder
    // doubtful.
    let x = call("update", 3, 0, 50, 0);
    let mut holder = after(step(id("device", 4, "report", 0), 200), &x.step, On::Sent);
    holder.after.insert(
        0,
        Dep::Each {
            kind: "device".into(),
            scope: subject("device", 1).scope,
            label: "read".into(),
            n: 0,
            on: On::Settled,
            count: None,
        },
    );
    add(
        "barrier_and_step",
        header_j(),
        vec![
            call("read", 1, 0, 0, 0),
            call("read", 2, 0, 10, 0),
            x,
            holder,
        ],
        Box::new(|line, _| {
            if line == 3 {
                answer(0, Some(50), Judgement::Unknown)
            } else {
                expected()
            }
        }),
        vec![Passed, Root, Passed, Inconclusive],
        "fail",
    );
    // An observation's cause sent 1,000 µs past its intended start, the
    // observation unknown (decision 17): late by the stand's send — its
    // connect and write — the system's, a root; late by the harness — its
    // release wake taken late — undecided.
    let c = call("update", 1, 0, 0, 0);
    let o = watched(1, 100, &c.step, 2_000);
    add(
        "late_cause_by_the_stand",
        header_j(),
        vec![c.clone(), o.clone()],
        Box::new(|line, _| {
            if line == 2 {
                answer(1_000, Some(100), Judgement::Expected(vec![]))
            } else {
                answer(0, None, Judgement::Unknown)
            }
        }),
        vec![Passed, Root],
        "fail",
    );
    // A call the stand was slow to take — its write 1,000 µs — then silent:
    // censored, the system's (decision 17), as long as its source's thread
    // did not keep the write waiting (decision 18: `lag`).
    add(
        "censored_written_slowly",
        header_j(),
        vec![call("read", 1, 0, 0, 0)],
        Box::new(|_, _| answer(1_000, None, Judgement::Unknown)),
        vec![Root],
        "fail",
    );
    // An observation read past its cause's window: released at its deadline
    // though intended before it.
    let c = call("update", 1, 0, 0, 0);
    let o = watched(1, 100, &c.step, 1_000);
    out.push(Golden {
        name: "read_past_the_deadline",
        header: header_j(),
        records: vec![c, o],
        script: Box::new(|_, _| expected()),
        lag: Box::new(|line| if line == 3 { 5_000 } else { 0 }),
        wake_lag: Box::new(|_| 0),
        classes: vec![Passed, Undecided],
        verdict: "void:undecided",
    });
    // Planned at 1,000 and read ahead: its release is a wake of its own,
    // taken 1,000 µs late (a step released at its read is dispatched in that
    // step, with no wake to take late).
    let lc = call("update", 1, 0, 1_000, 0);
    let lo = watched(1, 1_100, &lc.step, 2_000);
    out.push(Golden {
        name: "late_cause",
        header: header_j(),
        records: vec![lc, lo],
        script: Box::new(|line, _| {
            if line == 2 {
                answer(0, Some(100), Judgement::Expected(vec![]))
            } else {
                answer(0, None, Judgement::Unknown)
            }
        }),
        lag: Box::new(|_| 0),
        wake_lag: Box::new(|w| if w == 1_000 { 1_000 } else { 0 }),
        classes: vec![Passed, Undecided],
        verdict: "void:undecided",
    });
    // Decision 16: a poll sent on time, its next poll (due at 1,000) taken
    // 1 µs past the observation's deadline (2,500): whatever its lateness,
    // the harness ate the step's last poll.
    out.push(Golden {
        name: "poll_taken_late",
        header: header_j(),
        records: vec![polling(1, 0, 0, 1_000, 2_500)],
        script: Box::new(|_, _| answer(0, Some(10), Judgement::Unexpected)),
        lag: Box::new(|_| 0),
        wake_lag: Box::new(|w| if w == 1_000 { 1_501 } else { 0 }),
        classes: vec![Undecided],
        verdict: "void:undecided",
    });
    // Decision 17: a poll's next due at 2,499, its deadline 2,500, taken 1 µs
    // late — at its deadline: the harness ate it; untaken, the poll's
    // unsatisfied end at its deadline is the system's.
    for (name, every, lag, class, verdict) in [
        (
            "poll_wake_at_its_deadline",
            2_499,
            1,
            Undecided,
            "void:undecided",
        ),
        ("poll_unsatisfied_at_its_deadline", 1_000, 0, Root, "fail"),
    ] {
        out.push(Golden {
            name,
            header: header_j(),
            records: vec![polling(1, 0, 0, every, 2_500)],
            script: Box::new(|_, _| answer(0, Some(10), Judgement::Unexpected)),
            lag: Box::new(|_| 0),
            wake_lag: Box::new(move |w| if w == 2_499 { lag } else { 0 }),
            classes: vec![class],
            verdict,
        });
    }
    // Decision 17: a call planned at 1,000, read ahead, its deadline 1,300:
    // its release wake taken 500 µs late, at or past its deadline, it ends
    // never sent — the harness's.
    let mut short = header_j();
    short.operations.iter_mut().for_each(|o| o.timeout = 300);
    out.push(Golden {
        name: "first_dispatch_taken_past_its_deadline",
        header: short,
        records: vec![call("read", 1, 0, 1_000, 0)],
        script: Box::new(|_, _| expected()),
        lag: Box::new(|_| 0),
        wake_lag: Box::new(|w| if w == 1_000 { 500 } else { 0 }),
        classes: vec![Undecided],
        verdict: "void:undecided",
    });
    // Decision 17: dispatched late by the machine and censored at the
    // deadline — past J the harness's, at J the system's; an answer the
    // system gave (a 500) its own however late the harness sent it.
    let mut window = header_j();
    window.operations.iter_mut().for_each(|o| o.timeout = 2_000);
    for (name, op, lag, answered, class, verdict) in [
        (
            "dispatched_late_then_censored",
            "read",
            J + 1,
            None,
            Undecided,
            "void:undecided",
        ),
        (
            "dispatched_within_j_then_censored",
            "read",
            J,
            None,
            Root,
            "fail",
        ),
        (
            "answered_unknown_dispatched_late",
            "update",
            J + 1,
            Some(100),
            Root,
            "fail",
        ),
    ] {
        out.push(Golden {
            name,
            header: window.clone(),
            records: vec![call(op, 1, 0, 1_000, 0)],
            script: Box::new(move |_, _| answer(0, answered, Judgement::Unknown)),
            lag: Box::new(|_| 0),
            wake_lag: Box::new(move |w| if w == 1_000 { lag } else { 0 }),
            classes: vec![class],
            verdict,
        });
    }
    // Decision 17: the harness's lateness summed over attempts — a refusal's
    // first attempt and its resend each dispatched J/2 + 1 late, the resend
    // censored: J + 2, undecided; the first alone late, within J, a root.
    let mut certified = header_j();
    for o in &mut certified.operations {
        o.safety = Safety::NotTaken;
        o.not_run = vec!["service.unavailable".into()];
        o.timeout = 3_000;
    }
    let late = J / 2 + 1;
    for (name, both, class, verdict) in [
        (
            "harness_late_over_attempts",
            true,
            Undecided,
            "void:undecided",
        ),
        ("harness_late_on_one_attempt", false, Root, "fail"),
    ] {
        // Attempt 1 at 1,000 + late, refused 10 µs later; its resend due at
        // that answer plus the floor.
        let resend_due = 1_000 + late + 10 + RESEND;
        out.push(Golden {
            name,
            header: certified.clone(),
            records: vec![call("update", 1, 0, 1_000, 0)],
            script: Box::new(|_, attempt| {
                if attempt == 1 {
                    answer(0, Some(10), Judgement::NotTaken)
                } else {
                    answer(0, None, Judgement::Unknown)
                }
            }),
            lag: Box::new(|_| 0),
            wake_lag: Box::new(move |w| {
                if w == 1_000 || (both && w == resend_due) {
                    late
                } else {
                    0
                }
            }),
            classes: vec![class],
            verdict,
        });
    }
    // Decision 17: an observation read 500 µs past its intended start, its
    // window shortened by the reader: its last poll censored, undecided; its
    // last poll answered "not yet", the effect absent — a root.
    for (name, answered, class, verdict) in [
        (
            "observation_read_late_then_censored",
            None,
            Undecided,
            "void:undecided",
        ),
        ("observation_read_late_not_yet", Some(10), Root, "fail"),
    ] {
        out.push(Golden {
            name,
            header: header_j(),
            records: vec![polling(1, 0, 1_000, 400, 1_000)],
            script: Box::new(move |_, _| answer(0, answered, Judgement::Unexpected)),
            lag: Box::new(|line| if line == 2 { 1_500 } else { 0 }),
            wake_lag: Box::new(|_| 0),
            classes: vec![class],
            verdict,
        });
    }
    // Decision 17: a poll read past its deadline — released after it, never
    // sent, its wake its release: coherent, the reader's (`read_late`).
    out.push(Golden {
        name: "poll_released_past_its_deadline",
        header: header_j(),
        records: vec![polling(1, 0, 1_000, 400, 500)],
        script: Box::new(|_, _| answer(0, Some(10), Judgement::Unexpected)),
        lag: Box::new(|line| if line == 2 { 2_000 } else { 0 }),
        wake_lag: Box::new(|_| 0),
        classes: vec![Undecided],
        verdict: "void:undecided",
    });
    // Decision 17, version 5: censored on time — a call, and a poll's second
    // poll — the bases of the found answers' twins.
    out.push(Golden {
        name: "censored_on_time",
        header: window.clone(),
        records: vec![call("read", 1, 0, 1_000, 0)],
        script: Box::new(|_, _| answer(0, None, Judgement::Unknown)),
        lag: Box::new(|_| 0),
        wake_lag: Box::new(|_| 0),
        classes: vec![Root],
        verdict: "fail",
    });
    out.push(Golden {
        name: "poll_censored",
        header: header_j(),
        records: vec![polling(1, 0, 0, 1_000, 2_500)],
        script: Box::new(|_, attempt| {
            let after = (attempt == 1).then_some(10);
            answer(0, after, Judgement::Unexpected)
        }),
        lag: Box::new(|_| 0),
        wake_lag: Box::new(|_| 0),
        classes: vec![Root],
        verdict: "fail",
    });
    // Decision 16: a certified refusal at 10, its resend due at 110, J + 1
    // before the deadline (311): taken 2 µs late it would leave within J of
    // it, so it never leaves and the step ends unexpected — the harness's
    // lateness, not the step's. With the deadline at 310 the machine never
    // schedules the resend: the refusal is the step's own.
    for (name, deadline, lag, class, verdict) in [
        ("resend_taken_late", 311, 2, Undecided, "void:undecided"),
        ("resend_too_late_to_schedule", 310, 0, Root, "fail"),
    ] {
        let mut late = header_j();
        for o in &mut late.operations {
            o.safety = Safety::NotTaken;
            o.not_run = vec!["service.unavailable".into()];
            o.timeout = deadline;
        }
        out.push(Golden {
            name,
            header: late,
            records: vec![call("read", 1, 0, 0, 0)],
            script: Box::new(|_, _| answer(0, Some(10), Judgement::NotTaken)),
            lag: Box::new(|_| 0),
            wake_lag: Box::new(move |w| if w == 10 + RESEND { lag } else { 0 }),
            classes: vec![class],
            verdict,
        });
    }
    out
}

/// A handler that never reports: the backstop ends the step, a fault.
fn backstop() -> (Header, Vec<Record>, Vec<Line>) {
    let h = header_j();
    let r = call("read", 1, 0, 0, 0);
    let mut m = Machine::new(Arc::new(h.clone()), W).unwrap();
    let mut lines = Vec::new();
    let mut step = |m: &mut Machine, at: u64, input: Input| {
        let out = m.step(at, input.clone()).unwrap();
        lines.extend(journal::events(at, &input, &out));
        out
    };
    let out = step(
        &mut m,
        0,
        Input::Line {
            line: 2,
            record: Arc::new(r.clone()),
        },
    );
    let deadline = out
        .iter()
        .find_map(|o| match o {
            Output::Dispatch { deadline, .. } => Some(*deadline),
            _ => None,
        })
        .unwrap();
    step(
        &mut m,
        0,
        Input::Sent {
            line: 2,
            attempt: 1,
            decorated: 0,
            handed: 0,
            connected: 0,
            lag: 0,
        },
    );
    step(
        &mut m,
        deadline.saturating_add(ascetic_ddd_scheduler::machine::GRACE),
        Input::Wake,
    );
    (h, vec![r], lines)
}

/// A golden's journal, with the hand's classes and verdict.
struct Journal {
    name: String,
    bytes: Vec<u8>,
    log: Log,
    classes: Vec<Class>,
    verdict: &'static str,
}

/// Every golden's journal.
fn golden_journals() -> Vec<Journal> {
    let mut out = Vec::new();
    for g in goldens() {
        let script = &g.script;
        let ran = run_waking_late(
            g.header.clone(),
            &g.records,
            W,
            &*g.lag,
            &*g.wake_lag,
            &|line, attempt, _| script(line, attempt),
        )
        .unwrap();
        let log = log_of(&g.header, &g.records);
        let bytes = write(&journal_header(&g.header, &log), &ran.journal);
        out.push(Journal {
            name: g.name.to_owned(),
            bytes,
            log,
            classes: g.classes,
            verdict: g.verdict,
        });
    }
    let (h, records, lines) = backstop();
    let log = log_of(&h, &records);
    out.push(Journal {
        name: "backstop".to_owned(),
        bytes: write(&journal_header(&h, &log), &lines),
        log,
        classes: vec![Class::Undecided],
        verdict: "void:fault",
    });
    out
}

const GOLDEN_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/golden/runs");

/// Each golden run folds to the classes and verdict the hand wrote, and the
/// verdict by definition agrees; the journals are the files kept.
#[test]
#[expect(
    clippy::disallowed_methods,
    reason = "the golden runs are files (ADR-0047 K1e-3 refuses files to the machine, not its tests)"
)]
fn every_golden_run_folds_as_the_hand_wrote() {
    let mut cases = Vec::new();
    for Journal {
        name,
        bytes,
        log,
        classes,
        verdict,
    } in golden_journals()
    {
        let folded = fold_bytes(&bytes, &log);
        assert_eq!(folded.fold, FOLD_REVISION);
        assert_eq!(
            (verdict_name(&folded.verdict), &folded.classes),
            (verdict, &classes),
            "{name}: {:?}",
            folded.verdict
        );
        let kept = std::fs::read(format!("{GOLDEN_DIR}/{name}.jsonl"))
            .unwrap_or_else(|e| panic!("{name}: {e}: regenerate with the ignored `regenerate`"));
        assert_eq!(
            kept, bytes,
            "{name}: regenerate with the ignored `regenerate`"
        );
        cases.push(case(&name, &bytes, &log, &folded));
    }
    let reached = by_definition(&cases);
    eprintln!("{reached}");
    for counter in [
        "conditional_doubtful",
        "conditional_transitive",
        "conditional_amended",
        "conditional_and_expected",
        "conditional_ended_before_its_dependency",
        "undecided_censor",
        "undecided_read",
        "undecided_cause",
        "undecided_backstop",
        "undecided_wake",
        "producer_root",
        "consequent",
        "root",
        "passed",
        "pass",
        "fail",
        "void:undecided",
        "void:fault",
    ] {
        assert!(
            reached.contains(&format!("'{counter}'")),
            "{counter}: {reached}"
        );
    }
}

/// A journal edited and resealed, so that its reader accepts it: only the fold
/// can see that it disagrees with itself, or journals another log.
#[test]
fn broken_evidence_is_void() {
    let goldens = golden_journals();
    let by_name = |n: &str| goldens.iter().find(|g| g.name == n).unwrap();
    let lines_of = |bytes: &[u8]| -> (journal::Header, Vec<Line>) {
        let r = Reader::new(Cursor::new(bytes.to_vec())).unwrap();
        let h = r.header().clone();
        (h, r.collect::<Result<_, _>>().unwrap())
    };
    let position = |lines: &[Line], f: &dyn Fn(&Line) -> bool| lines.iter().position(f).unwrap();
    let is_end = |l: &Line| matches!(l, Line::End(_));
    type Edit = Box<dyn Fn(&mut journal::Header, &mut Vec<Line>, &mut Log)>;
    let resend = |l: &mut Vec<Line>, f: &dyn Fn(&mut journal::Dispatch)| {
        for x in l.iter_mut() {
            if let Line::Dispatch(d) = x
                && d.attempt == 2
            {
                f(d);
            }
        }
    };
    let edits: Vec<(&str, &str, Edit, &str)> = vec![
        // Decision 15: a resend its predecessor's answer did not license.
        (
            "a resend before its floor",
            "retry_never_sent",
            Box::new(move |_, l, _| resend(l, &|d| d.at = 50)),
            "void:incoherent",
        ),
        (
            "a resend too near its deadline",
            "retry_never_sent",
            Box::new(move |_, l, _| resend(l, &|d| d.at = d.deadline - J)),
            "void:incoherent",
        ),
        (
            "a refusal resent of a call never resent",
            "refusal_resent",
            Box::new(|_, l, _| {
                for x in l.iter_mut() {
                    if let Line::Read(r) = x {
                        r.safety = ascetic_ddd_command_log::record::Safety::Never;
                    }
                }
            }),
            "void:incoherent",
        ),
        (
            "a poll before the wait its answer asked for",
            "last_poll_never_sent",
            Box::new(|_, l, _| {
                for x in l.iter_mut() {
                    if let Line::Answer(a) = x
                        && a.attempt == 1
                    {
                        a.after = Some(5_000);
                    }
                }
            }),
            "void:incoherent",
        ),
        (
            "a resend of a call never resent",
            "retry_never_sent",
            Box::new(|_, l, _| {
                for x in l.iter_mut() {
                    if let Line::Read(r) = x {
                        r.safety = ascetic_ddd_command_log::record::Safety::Never;
                    }
                }
            }),
            "void:incoherent",
        ),
        (
            "an end dropped",
            "clean",
            Box::new(move |_, l, _| {
                let i = position(l, &is_end);
                l.remove(i);
            }),
            "void:incoherent",
        ),
        (
            "an end duplicated",
            "clean",
            Box::new(move |_, l, _| {
                let i = position(l, &is_end);
                let e = l[i].clone();
                l.insert(i + 1, e);
            }),
            "void:incoherent",
        ),
        (
            "two ends' lines swapped",
            "clean",
            Box::new(|_, l, _| {
                let ends: Vec<usize> = (0..l.len())
                    .filter(|i| matches!(l[*i], Line::End(_)))
                    .collect();
                let (Line::End(a), Line::End(b)) = (l[ends[0]].clone(), l[ends[1]].clone()) else {
                    unreachable!()
                };
                let mut a2 = a.clone();
                a2.line = b.line;
                let mut b2 = b.clone();
                b2.line = a.line;
                l[ends[0]] = Line::End(a2);
                l[ends[1]] = Line::End(b2);
            }),
            "void:incoherent",
        ),
        (
            "a read dropped",
            "clean",
            Box::new(|_, l, _| {
                let i = l.iter().rposition(|x| matches!(x, Line::Read(_))).unwrap();
                l.remove(i);
            }),
            "void:incoherent",
        ),
        (
            "a dispatch dropped",
            "clean",
            Box::new(|_, l, _| {
                let i = l
                    .iter()
                    .position(|x| matches!(x, Line::Dispatch(_)))
                    .unwrap();
                l.remove(i);
            }),
            "void:incoherent",
        ),
        (
            "an end's read moved",
            "clean",
            Box::new(move |_, l, _| {
                let i = position(l, &is_end);
                if let Line::End(e) = &mut l[i] {
                    e.read += 1;
                }
            }),
            "void:incoherent",
        ),
        (
            "a skipped step said expected",
            "root_and_skips",
            Box::new(|_, l, _| {
                for x in l.iter_mut() {
                    if let Line::End(e) = x
                        && e.line == 3
                    {
                        e.end = journal::End::Expected;
                    }
                }
            }),
            "void:incoherent",
        ),
        (
            "a barrier's members miscounted",
            "barrier_and_step",
            Box::new(|_, l, _| {
                for x in l.iter_mut() {
                    if let Line::Read(r) = x {
                        for d in &mut r.after {
                            if let journal::Dep::Each { members, .. } = d {
                                *members -= 1;
                            }
                        }
                    }
                }
            }),
            "void:incoherent",
        ),
        (
            "a backstop's fault dropped",
            "backstop",
            Box::new(|_, l, _| l.retain(|x| !matches!(x, Line::Fault(_)))),
            "void:incoherent",
        ),
        (
            "a fault without its backstop",
            "clean",
            Box::new(|_, l, _| {
                l.push(Line::Fault(journal::Fault {
                    at: 1_000_000,
                    class: journal::FaultClass::Backstop { line: 2 },
                }))
            }),
            "void:incoherent",
        ),
        (
            "another log",
            "clean",
            Box::new(|h, _, _| h.log = "0".repeat(64)),
            "void:another_log",
        ),
        (
            "a log of more steps",
            "clean",
            Box::new(|_, _, log| log.records += 1),
            "void:another_log",
        ),
        // Decision 17: the hand-off is the harness's — a dispatch J late,
        // its task first run a microsecond later, censored: J + 1, undecided.
        (
            "a hand-off J + 1 late, censored",
            "dispatched_within_j_then_censored",
            Box::new(|_, l, _| {
                for x in l.iter_mut() {
                    match x {
                        Line::Sent(t) => {
                            t.handed += 1;
                            t.connected += 1;
                            t.at += 1;
                        }
                        Line::End(e) => e.sent = e.sent.map(|s| s + 1),
                        _ => {}
                    }
                }
            }),
            "void:undecided",
        ),
    ];
    let mut cases = Vec::new();
    for (name, from, edit, expected) in edits {
        let g = by_name(from);
        let (mut h, mut lines) = lines_of(&g.bytes);
        let mut log = g.log.clone();
        edit(&mut h, &mut lines, &mut log);
        let bytes = write(&h, &lines);
        let folded = fold_bytes(&bytes, &log);
        assert_eq!(
            verdict_name(&folded.verdict),
            expected,
            "{name}: {:?}",
            folded.verdict
        );
        cases.push(case(name, &bytes, &log, &folded));
    }
    // Decision 17's coherence, a twin a rule: each edit breaks that rule
    // alone, and the fold names it.
    let at_attempt = |l: &mut Vec<Line>, attempt: u32, f: &dyn Fn(&mut Line)| {
        for x in l.iter_mut() {
            let hit = match x {
                Line::Dispatch(d) => d.attempt == attempt,
                Line::Sent(t) => t.attempt == attempt,
                Line::Answer(a) => a.attempt == attempt && a.ignored.is_none(),
                _ => false,
            };
            if hit {
                f(x);
            }
        }
    };
    let on_end = |l: &mut Vec<Line>, f: &dyn Fn(&mut Ended)| {
        for x in l.iter_mut() {
            if let Line::End(e) = x {
                f(e);
            }
        }
    };
    let rules: Vec<(&str, &str, Edit, &str)> = vec![
        (
            "a poll's resend dispatched before its wake",
            "poll_unsatisfied_at_its_deadline",
            Box::new(move |_, l, _| {
                at_attempt(l, 2, &|x| {
                    if let Line::Dispatch(d) = x {
                        d.due = d.at + 1;
                    }
                })
            }),
            "a dispatch before its wake",
        ),
        (
            "a first attempt's wake before its release",
            "dispatched_late_then_censored",
            Box::new(move |_, l, _| {
                at_attempt(l, 1, &|x| {
                    if let Line::Dispatch(d) = x {
                        d.due -= 1;
                    }
                })
            }),
            "a first attempt's wake not its release",
        ),
        (
            "a resend's wake off its answer's wait",
            "harness_late_over_attempts",
            Box::new(move |_, l, _| {
                at_attempt(l, 2, &|x| {
                    if let Line::Dispatch(d) = x {
                        d.due += 1;
                    }
                })
            }),
            "a resend against its wake",
        ),
        (
            "a poll's resend due before its answer",
            "poll_unsatisfied_at_its_deadline",
            Box::new(move |_, l, _| {
                at_attempt(l, 2, &|x| {
                    if let Line::Dispatch(d) = x {
                        d.due = 9;
                    }
                })
            }),
            "a resend against its wake",
        ),
        (
            "a send handed after it connected",
            "dispatched_late_then_censored",
            Box::new(move |_, l, _| {
                at_attempt(l, 1, &|x| {
                    if let Line::Sent(t) = x {
                        t.handed = t.connected + 1;
                        t.at = t.handed;
                    }
                })
            }),
            "a send against its path",
        ),
        (
            "a send connected after it left",
            "dispatched_late_then_censored",
            Box::new(move |_, l, _| {
                at_attempt(l, 1, &|x| {
                    if let Line::Sent(t) = x {
                        t.connected = t.at + 1;
                    }
                })
            }),
            "a send against its path",
        ),
        (
            "a send handed before its dispatch",
            "dispatched_late_then_censored",
            Box::new(move |_, l, _| {
                at_attempt(l, 1, &|x| {
                    if let Line::Sent(t) = x {
                        t.handed -= 1;
                        t.connected -= 1;
                    }
                })
            }),
            "a send handed before its dispatch",
        ),
        (
            "a censor with a status",
            "dispatched_late_then_censored",
            Box::new(move |_, l, _| {
                at_attempt(l, 1, &|x| {
                    if let Line::Answer(a) = x {
                        a.status = Some(500);
                    }
                })
            }),
            "a censor with an answer's content",
        ),
        (
            "a censor off its deadline",
            "dispatched_late_then_censored",
            Box::new(move |_, l, _| {
                at_attempt(l, 1, &|x| {
                    if let Line::Answer(a) = x {
                        a.at -= 1;
                    }
                })
            }),
            "a censor off its deadline",
        ),
        (
            "a wake on an expected end",
            "clean",
            Box::new(move |_, l, _| on_end(l, &|e| e.wake = Some(0))),
            "a wake on an end an input or a skip made",
        ),
        (
            "a wake after the end it made",
            "poll_wake_at_its_deadline",
            Box::new(move |_, l, _| on_end(l, &|e| e.wake = Some(e.ended + 1))),
            "a wake after its end",
        ),
        (
            "a first dispatch's wake off its release",
            "first_dispatch_taken_past_its_deadline",
            Box::new(move |_, l, _| on_end(l, &|e| e.wake = e.wake.map(|w| w + 1))),
            "a first dispatch's wake against its release",
        ),
        (
            "a first dispatch ended before its deadline",
            "first_dispatch_taken_past_its_deadline",
            Box::new(move |_, l, _| on_end(l, &|e| e.ended = 1_299)),
            "a first dispatch's wake against its release",
        ),
        (
            "a poll's wake past its deadline",
            "poll_taken_late",
            Box::new(move |_, l, _| on_end(l, &|e| e.wake = Some(2_501))),
            "a poll's wake against its deadline",
        ),
        (
            "a poll's wake at its deadline, its end past it",
            "poll_taken_late",
            Box::new(move |_, l, _| on_end(l, &|e| e.wake = Some(2_500))),
            "a poll's wake against its deadline",
        ),
        (
            "a poll's wake before its deadline, its end before it",
            "poll_wake_at_its_deadline",
            Box::new(move |_, l, _| on_end(l, &|e| e.ended = 2_499)),
            "a poll's wake against its deadline",
        ),
        (
            "a fallback's wake off its licence",
            "resend_taken_late",
            Box::new(move |_, l, _| on_end(l, &|e| e.wake = e.wake.map(|w| w + 1))),
            "a fallback's wake against its licence",
        ),
        (
            "a fallback's wake within J of its deadline",
            "resend_taken_late",
            Box::new(|h, _, _| h.jitter = 201),
            "a fallback's wake against its licence",
        ),
        (
            "an intended start before its plan",
            "clean",
            Box::new(move |_, l, _| {
                for x in l.iter_mut() {
                    if let Line::Read(r) = x
                        && r.line == 2
                    {
                        r.planned = 1;
                    }
                }
            }),
            "an intended start before its plan",
        ),
    ];
    for (name, from, edit, why) in rules {
        let g = by_name(from);
        let (mut h, mut lines) = lines_of(&g.bytes);
        let mut log = g.log.clone();
        edit(&mut h, &mut lines, &mut log);
        let bytes = write(&h, &lines);
        let folded = fold_bytes(&bytes, &log);
        match &folded.verdict {
            Verdict::Void(Void::Incoherent { why: w, .. }) => assert_eq!(*w, why, "{name}"),
            other => panic!("{name}: {other:?}"),
        }
        cases.push(case(name, &bytes, &log, &folded));
    }
    by_definition(&cases);
    // A pass that reads nothing it can decode is void, not short.
    let clean = by_name("clean");
    let log = &clean.log;
    let h = Reader::new(Cursor::new(clean.bytes.clone()))
        .unwrap()
        .header()
        .clone();
    let broken = || {
        vec![Err(journal::JournalError::Truncated)]
            .into_iter()
            .chain(std::iter::empty())
    };
    let folded = fold(&h, log, broken(), broken());
    assert_eq!(verdict_name(&folded.verdict), "void:unreadable");
}

/// Writes `golden/runs/*.jsonl` from the goldens as they are. Run only on
/// purpose.
#[test]
#[ignore = "regenerates golden/runs"]
#[expect(
    clippy::disallowed_methods,
    reason = "the golden runs are files (ADR-0047 K1e-3 refuses files to the machine, not its tests)"
)]
fn regenerate() {
    std::fs::create_dir_all(GOLDEN_DIR).unwrap();
    for g in golden_journals() {
        std::fs::write(format!("{GOLDEN_DIR}/{}.jsonl", g.name), &g.bytes).unwrap();
    }
}

/// Its step's end's place in a journal of one step.
fn end_at(l: &[Line]) -> usize {
    l.iter().position(|x| matches!(x, Line::End(_))).unwrap()
}

/// A late answer's bracket.
fn bracket(absent: u64, deadline: u64) -> Option<journal::Found> {
    Some(journal::Found {
        absent,
        deadline,
        handed: None,
    })
}

/// A late answer, found for its attempt's censor at `at` (version 5).
fn found_answer(
    line: u64,
    attempt: u32,
    at: u64,
    found: Option<journal::Found>,
    ignored: journal::Ignored,
) -> Line {
    Line::Answer(journal::Answer {
        at,
        line,
        attempt,
        judged: journal::Judged::Expected,
        captured: vec![],
        ignored: Some(ignored),
        status: Some(200),
        code: None,
        after: None,
        censored: false,
        found,
        unsent: None,
    })
}

/// Decision 17, version 5: a late answer's bracket. Censored at its deadline
/// 3,000, a call's late answer found blind for J + 1 is the receive side's —
/// undecided; for J, absent from the deadline on, or with no bracket, the
/// system's; a poll's found before its end, as the hold of a late answer
/// makes it, the same. And a twin a coherence rule.
#[test]
fn a_late_answer_found_is_the_receive_side_s() {
    use journal::Ignored;
    let goldens = golden_journals();
    let by_name = |n: &str| goldens.iter().find(|g| g.name == n).unwrap();
    let lines_of = |bytes: &[u8]| -> (journal::Header, Vec<Line>) {
        let r = Reader::new(Cursor::new(bytes.to_vec())).unwrap();
        let h = r.header().clone();
        (h, r.collect::<Result<_, _>>().unwrap())
    };
    // (name, base, edit, the verdict, the reason if incoherent)
    type Edit = Box<dyn Fn(&mut Vec<Line>)>;
    let after_end = |answer: Line| -> Edit {
        Box::new(move |l| {
            let i = end_at(l);
            l.insert(i + 1, answer.clone());
        })
    };

    let cases: Vec<(&str, &str, Edit, &str, Option<&str>)> = vec![
        (
            "blind J + 1",
            "censored_on_time",
            after_end(found_answer(
                2,
                1,
                2_999 + J + 1,
                bracket(2_999, 3_000),
                Ignored::Ended,
            )),
            "void:undecided",
            None,
        ),
        (
            "blind J",
            "censored_on_time",
            after_end(found_answer(
                2,
                1,
                2_999 + J,
                bracket(2_999, 3_000),
                Ignored::Ended,
            )),
            "fail",
            None,
        ),
        (
            "absent at the deadline",
            "censored_on_time",
            after_end(found_answer(
                2,
                1,
                3_500,
                bracket(3_000, 3_000),
                Ignored::Ended,
            )),
            "fail",
            None,
        ),
        (
            "a late answer without its bracket",
            "censored_on_time",
            after_end(found_answer(2, 1, 3_500, None, Ignored::Ended)),
            "fail",
            None,
        ),
        (
            "a poll's found before its end",
            "poll_censored",
            Box::new(|l| {
                let i = end_at(l);
                l.insert(
                    i,
                    found_answer(2, 2, 2_500 + J + 10, bracket(2_400, 2_500), Ignored::Late),
                );
            }),
            "void:undecided",
            None,
        ),
        // Coherence.
        (
            "a found answer taken",
            "censored_on_time",
            Box::new(|l| {
                let i = end_at(l);
                let mut a = found_answer(2, 1, 3_500, bracket(2_999, 3_000), Ignored::Ended);
                if let Line::Answer(x) = &mut a {
                    x.ignored = None;
                }
                l.insert(i + 1, a);
            }),
            "void:incoherent",
            Some("a found answer taken"),
        ),
        (
            "a found answer within its bracket",
            "censored_on_time",
            after_end(found_answer(
                2,
                1,
                2_999,
                bracket(2_999, 3_000),
                Ignored::Ended,
            )),
            "void:incoherent",
            Some("a found answer within its bracket"),
        ),
        (
            "a found answer without its censor",
            "clean",
            after_end(found_answer(2, 1, 3_500, bracket(10, 20), Ignored::Ended)),
            "void:incoherent",
            Some("a found answer without its censor"),
        ),
        (
            "a found answer off its deadline",
            "censored_on_time",
            after_end(found_answer(
                2,
                1,
                3_500,
                bracket(2_999, 2_999),
                Ignored::Ended,
            )),
            "void:incoherent",
            Some("a found answer against its censor"),
        ),
        (
            "a found answer absent before its dispatch",
            "censored_on_time",
            after_end(found_answer(
                2,
                1,
                3_500,
                bracket(999, 3_000),
                Ignored::Ended,
            )),
            "void:incoherent",
            Some("a found answer against its censor"),
        ),
        (
            "a found answer of another attempt",
            "censored_on_time",
            after_end(found_answer(
                2,
                2,
                3_500,
                bracket(2_999, 3_000),
                Ignored::Ended,
            )),
            "void:incoherent",
            Some("a found answer against its censor"),
        ),
        (
            "two found answers for one censor",
            "censored_on_time",
            Box::new(|l| {
                let i = end_at(l);
                l.insert(
                    i + 1,
                    found_answer(2, 1, 3_500, bracket(2_999, 3_000), Ignored::Ended),
                );
                l.insert(
                    i + 2,
                    found_answer(2, 1, 3_600, bracket(2_999, 3_000), Ignored::Ended),
                );
            }),
            "void:incoherent",
            Some("a found answer without its censor"),
        ),
        (
            "two found answers before a poll's end",
            "poll_censored",
            Box::new(|l| {
                let i = end_at(l);
                l.insert(
                    i,
                    found_answer(2, 2, 2_600, bracket(2_400, 2_500), Ignored::Late),
                );
                l.insert(
                    i + 1,
                    found_answer(2, 2, 2_700, bracket(2_400, 2_500), Ignored::Late),
                );
            }),
            "void:incoherent",
            Some("a found answer against its censor"),
        ),
        (
            "a poll's found before its censor",
            "poll_censored",
            Box::new(|l| {
                // Right after attempt 2's dispatch: its taken answer not yet
                // the censor.
                let i = l
                    .iter()
                    .position(|x| matches!(x, Line::Dispatch(d) if d.attempt == 2))
                    .unwrap();
                l.insert(
                    i + 1,
                    found_answer(2, 2, 2_600, bracket(2_400, 2_500), Ignored::Late),
                );
            }),
            "void:incoherent",
            Some("a found answer against its censor"),
        ),
    ];
    let mut defined = Vec::new();
    for (name, from, edit, verdict, why) in cases {
        let g = by_name(from);
        let (h, mut lines) = lines_of(&g.bytes);
        edit(&mut lines);
        let bytes = write(&h, &lines);
        let folded = fold_bytes(&bytes, &g.log);
        assert_eq!(
            verdict_name(&folded.verdict),
            verdict,
            "{name}: {:?}",
            folded.verdict
        );
        if let Some(why) = why {
            match &folded.verdict {
                Verdict::Void(Void::Incoherent { why: w, .. }) => assert_eq!(*w, why, "{name}"),
                other => panic!("{name}: {other:?}"),
            }
        }
        defined.push(case(name, &bytes, &g.log, &folded));
    }
    by_definition(&defined);
}

/// Decision 17's guarantees over a model of the shell's looks, the true
/// arrival the oracle. The source's runtime blocked over `[b0, b1)`; the
/// attempt runs at its pre-look (`D − J/2`), at its answer's arrival `k`, and
/// at its look (`D + 1`), each put off past the block; a look before `k`
/// finds the answer absent, any run at or past `k` reads it. P2: an answer
/// in the host by its deadline read more than J late is never a root. P1:
/// both looks late by less than J/2, a late answer is never undecided for
/// it. P3: an answer never found came after the look.
#[test]
fn the_receive_side_s_guarantees_hold_over_the_looks() {
    use journal::{Found, Ignored};
    let goldens = golden_journals();
    let base = goldens
        .iter()
        .find(|g| g.name == "censored_on_time")
        .unwrap();
    let r = Reader::new(Cursor::new(base.bytes.clone())).unwrap();
    let h = r.header().clone();
    let lines: Vec<Line> = r.collect::<Result<_, _>>().unwrap();
    let end = lines
        .iter()
        .position(|x| matches!(x, Line::End(_)))
        .unwrap();
    let (d, connected) = (3_000u64, 1_000u64);
    let (pre, look) = (d - J / 2, d + 1);
    let mut defined = Vec::new();
    let (mut p1, mut p2, mut p3) = (0, 0, 0);
    let root = ascetic_ddd_faker::Key::root(ascetic_ddd_faker::Seed(17)).child("looks");
    for n in 0..20_000u64 {
        let draw =
            |label: &str, lo: u64, hi: u64| lo + root.at(n).child(label).unit().raw() % (hi - lo);
        let k = draw("k", 2_500, 3_600);
        let b0 = draw("b0", 2_400, 3_600);
        let b1 = b0 + draw("len", 1, 1_200);
        let run = |t: u64| if t >= b0 && t < b1 { b1 } else { t };
        let (p, rk, u) = (run(pre), run(k), run(look));
        // The looks and the answer's own wake, in the order the task runs.
        let mut runs = [(p, true), (rk, false), (u, true)];
        runs.sort();
        let mut absent = connected;
        let mut read = None;
        for (t, is_look) in runs {
            if t >= k {
                read = Some(t);
                break;
            }
            if is_look {
                absent = absent.max(t);
            }
            if t >= u {
                break;
            }
        }
        // The look at `D + 1` gives up on what it did not find.
        let read = match read {
            Some(t) if t <= d => continue, // in time: answered, no censor
            other => other,
        };
        let mut edited = lines.clone();
        if let Some(t) = read {
            edited.insert(
                end + 1,
                found_answer(
                    2,
                    1,
                    t,
                    Some(Found {
                        absent,
                        deadline: d,
                        handed: None,
                    }),
                    Ignored::Ended,
                ),
            );
        }
        let bytes = write(&h, &edited);
        let folded = fold_bytes(&bytes, &base.log);
        let class = folded.classes[0];
        if k <= d && read.is_some_and(|t| t - k > J) {
            assert_ne!(
                class,
                Class::Root,
                "P2: k {k}, block [{b0}, {b1}), read {read:?}, absent {absent}"
            );
            p2 += 1;
        }
        if k > d && p - pre < J / 2 && u - look < J / 2 {
            assert_eq!(
                class,
                Class::Root,
                "P1: k {k}, block [{b0}, {b1}), read {read:?}, absent {absent}"
            );
            p1 += 1;
        }
        if read.is_none() {
            assert!(k > u, "P3: k {k}, the look at {u}");
            p3 += 1;
        }
        if n % 50 == 0 {
            defined.push(case(&format!("looks {n}"), &bytes, &base.log, &folded));
        }
    }
    assert!(
        p1 > 500 && p2 > 500 && p3 > 500,
        "P1 {p1}, P2 {p2}, P3 {p3}"
    );
    by_definition(&defined);
}

/// A journal's header and lines, as its reader gives them.
fn read_lines(bytes: &[u8]) -> (journal::Header, Vec<Line>) {
    let r = Reader::new(Cursor::new(bytes.to_vec())).unwrap();
    let h = r.header().clone();
    (h, r.collect::<Result<_, _>>().unwrap())
}

/// The latest instant among `lines`.
fn latest(lines: &[Line]) -> u64 {
    lines
        .iter()
        .map(|l| match l {
            Line::Read(x) => x.at,
            Line::Released(x) => x.at,
            Line::Dispatch(x) => x.at,
            Line::Sent(x) => x.at,
            Line::Answer(x) => x.at,
            Line::End(x) => x.at,
            Line::Fault(x) => x.at,
            Line::Header(_) | Line::Trailer(_) => 0,
        })
        .max()
        .unwrap_or(0)
}

fn stop_at(at: u64, class: journal::FaultClass) -> Line {
    Line::Fault(journal::Fault { at, class })
}

/// Decision 18: the source thread's lag on a send path is the harness's — a
/// censored call's, and an observation's cause's — past J undecided, at J
/// the system's; and it lies inside its path.
#[test]
fn a_send_s_lag_is_the_harness_s() {
    use Class::*;
    let goldens = golden_journals();
    let by_name = |n: &str| goldens.iter().find(|g| g.name == n).unwrap();
    // The send of `line`, its hand-off and lag set.
    let lagged = |from: &str, line: u64, handed: u64, lag: u64| {
        let g = by_name(from);
        let (h, mut lines) = read_lines(&g.bytes);
        for x in lines.iter_mut() {
            if let Line::Sent(t) = x
                && t.line == line
            {
                t.handed = handed;
                t.connected = t.connected.max(handed);
                t.lag = lag;
            }
        }
        (write(&h, &lines), g.log.clone())
    };
    let mut cases = Vec::new();
    let classed = [
        // A censored call: its lag alone, then with a hand-off.
        ("censored_written_slowly", 2, 0, J, vec![Root], "fail"),
        (
            "censored_written_slowly",
            2,
            0,
            J + 1,
            vec![Undecided],
            "void:undecided",
        ),
        (
            "censored_written_slowly",
            2,
            100,
            J - 100,
            vec![Root],
            "fail",
        ),
        (
            "censored_written_slowly",
            2,
            100,
            J - 99,
            vec![Undecided],
            "void:undecided",
        ),
        // An observation whose cause's send its thread held.
        (
            "late_cause_by_the_stand",
            2,
            0,
            J,
            vec![Passed, Root],
            "fail",
        ),
        (
            "late_cause_by_the_stand",
            2,
            0,
            J + 1,
            vec![Passed, Undecided],
            "void:undecided",
        ),
    ];
    for (from, line, handed, lag, classes, verdict) in classed {
        let (bytes, log) = lagged(from, line, handed, lag);
        let folded = fold_bytes(&bytes, &log);
        assert_eq!(
            (verdict_name(&folded.verdict), &folded.classes),
            (verdict, &classes),
            "{from}: hand-off {handed}, lag {lag}"
        );
        cases.push(case(
            &format!("{from} {handed} {lag}"),
            &bytes,
            &log,
            &folded,
        ));
    }
    // Its lag past its path: the send left 1,000 µs after it was handed.
    let (bytes, log) = lagged("censored_written_slowly", 2, 0, 1_001);
    let folded = fold_bytes(&bytes, &log);
    match &folded.verdict {
        Verdict::Void(Void::Incoherent { why, .. }) => {
            assert_eq!(*why, "a send's lag past its path")
        }
        other => panic!("{other:?}"),
    }
    cases.push(case("a lag past its path", &bytes, &log, &folded));
    let reached = by_definition(&cases);
    assert!(reached.contains("'undecided_censor'") && reached.contains("'undecided_cause'"));
}

/// Decision 18: a run's stop is its journal's last line — void, no classes,
/// whatever its steps had shown; a gate's refusal the journal's only line,
/// under a header with no T0; and each rule's breach incoherent.
#[test]
fn a_run_s_stop_is_its_journal_s_last_line() {
    use journal::FaultClass as F;
    let goldens = golden_journals();
    let by_name = |n: &str| goldens.iter().find(|g| g.name == n).unwrap();
    let clean = by_name("clean");
    let (h, lines) = read_lines(&clean.bytes);
    let first_end = lines
        .iter()
        .position(|l| matches!(l, Line::End(_)))
        .unwrap();
    let mut cases = Vec::new();
    let mut check = |name: &str, h: &journal::Header, lines: &[Line], log: &Log, want: &str| {
        let bytes = write(h, lines);
        let folded = fold_bytes(&bytes, log);
        match &folded.verdict {
            Verdict::Void(Void::Incoherent { why, .. }) => assert_eq!(*why, want, "{name}"),
            Verdict::Void(Void::Fault { class }) => {
                assert!(class.stops(), "{name}");
                assert_eq!("stopped", want, "{name}");
                assert!(folded.classes.is_empty(), "{name}");
            }
            other => panic!("{name}: {other:?}"),
        }
        cases.push(case(name, &bytes, log, &folded));
    };
    let at = latest(&lines);
    // Stopped mid-run, and after every step ended: void, not pass.
    let mut mid = lines[..=first_end].to_vec();
    mid.push(stop_at(latest(&mid), F::Stalled));
    check("stopped mid-run", &h, &mid, &clean.log, "stopped");
    let mut all = lines.clone();
    all.push(stop_at(at, F::Poisoned));
    check(
        "stopped once every step ended",
        &h,
        &all,
        &clean.log,
        "stopped",
    );
    // A stop after a backstop's fault, naming the struck step.
    let backstop = by_name("backstop");
    let (bh, mut bl) = read_lines(&backstop.bytes);
    bl.push(stop_at(
        latest(&bl),
        F::Dropped {
            line: 2,
            attempt: 1,
        },
    ));
    check(
        "a stop after a backstop",
        &bh,
        &bl,
        &backstop.log,
        "stopped",
    );
    // A gate's refusal: the header with no T0, the fault at 0, alone.
    let unstarted = journal::Header {
        t0: None,
        ..h.clone()
    };
    let gate = stop_at(
        0,
        F::Gate {
            name: journal::Code::Named("stand.namespace_unseen".to_owned()),
        },
    );
    check(
        "a gate refused",
        &unstarted,
        std::slice::from_ref(&gate),
        &clean.log,
        "stopped",
    );
    // The rules' breaches.
    let mut after = mid.clone();
    after.push(lines[first_end + 1].clone());
    check(
        "a line after the run's stop",
        &h,
        &after,
        &clean.log,
        "a line after the run's stop",
    );
    let mut late_gate = lines.clone();
    late_gate.push(stop_at(
        at,
        F::Gate {
            name: journal::Code::Other,
        },
    ));
    check(
        "a gate's refusal after a run's lines",
        &unstarted,
        &late_gate,
        &clean.log,
        "a gate's refusal not the journal's only line",
    );
    // At 0 after lines at 0: only its place breaks the rule.
    let mut at_zero: Vec<Line> = lines
        .iter()
        .take_while(|l| latest(std::slice::from_ref(l)) == 0)
        .cloned()
        .collect();
    assert!(!at_zero.is_empty());
    at_zero.push(stop_at(
        0,
        F::Gate {
            name: journal::Code::Other,
        },
    ));
    check(
        "a gate's refusal after lines at 0",
        &unstarted,
        &at_zero,
        &clean.log,
        "a gate's refusal not the journal's only line",
    );
    check(
        "a gate's refusal under a T0",
        &h,
        &[gate],
        &clean.log,
        "a gate's refusal not the journal's only line",
    );
    check(
        "a gate's refusal past 0",
        &unstarted,
        &[stop_at(
            1,
            F::Gate {
                name: journal::Code::Other,
            },
        )],
        &clean.log,
        "a gate's refusal not the journal's only line",
    );
    check(
        "no T0 and no gate's refusal",
        &unstarted,
        &lines,
        &clean.log,
        "no T0, and no gate's refusal",
    );
    let mut before = lines.clone();
    before.push(stop_at(at - 1, F::Stalled));
    check(
        "a stop before the line above it",
        &h,
        &before,
        &clean.log,
        "a stop before the line above it",
    );
    let mut unread = mid.clone();
    unread.pop();
    unread.push(stop_at(
        latest(&unread),
        F::Held {
            line: 9,
            attempt: 1,
            since: 0,
        },
    ));
    check(
        "a stop naming a line not read",
        &h,
        &unread,
        &clean.log,
        "a stop naming a line not read",
    );
    // Line 3 read and dispatched once, not ended, in the first end's prefix.
    let live = lines[..first_end]
        .iter()
        .rposition(|l| matches!(l, Line::Dispatch(d) if d.line == 3))
        .map(|i| lines[..=i].to_vec())
        .unwrap_or_else(|| lines[..first_end].to_vec());
    let mut out = live.clone();
    out.push(stop_at(
        latest(&out),
        F::Dropped {
            line: 3,
            attempt: 2,
        },
    ));
    check(
        "a stop naming an attempt not out",
        &h,
        &out,
        &clean.log,
        "a stop naming an attempt not out",
    );
    let mut owed = bl.clone();
    owed.pop();
    let fault = owed
        .iter()
        .rposition(|l| matches!(l, Line::Fault(_)))
        .unwrap();
    owed[fault] = stop_at(latest(&owed), F::Stalled);
    check(
        "a backstop's end, a stop for its fault",
        &bh,
        &owed,
        &backstop.log,
        "a backstop's end without its fault",
    );
    let reached = by_definition(&cases);
    for stop in ["stop_stalled", "stop_poisoned", "stop_dropped", "stop_gate"] {
        assert!(reached.contains(&format!("'{stop}'")), "{stop}: {reached}");
    }
}

/// Decision 18: every golden run cut after any of its lines — but between a
/// backstop's end and its fault — and stopped there is void, classed not at
/// all, never incoherent; the definition names the same stop.
#[test]
fn every_run_cut_and_stopped_is_void() {
    use journal::FaultClass as F;
    let mut cases = Vec::new();
    for g in golden_journals() {
        let (h, lines) = read_lines(&g.bytes);
        for cut in 0..=lines.len() {
            let prefix = &lines[..cut];
            if matches!(prefix.last(), Some(Line::End(e)) if e.fault) {
                continue;
            }
            let at = latest(prefix);
            // A step read and dispatched in the prefix, for a class that names
            // one.
            let named = prefix.iter().rev().find_map(|l| match l {
                Line::Dispatch(d) => Some((d.line, d.attempt)),
                _ => None,
            });
            let mut classes = vec![F::Stalled, F::Log(journal::LogFault::Diverged)];
            if let Some((line, attempt)) = named {
                classes.push(F::Held {
                    line,
                    attempt,
                    since: at,
                });
            }
            for class in classes {
                let mut cut_lines = prefix.to_vec();
                cut_lines.push(stop_at(at, class.clone()));
                let bytes = write(&h, &cut_lines);
                let folded = fold_bytes(&bytes, &g.log);
                assert_eq!(
                    folded.verdict,
                    Verdict::Void(Void::Fault {
                        class: class.clone()
                    }),
                    "{} cut at {cut}",
                    g.name
                );
                assert!(folded.classes.is_empty());
                cases.push(case(
                    &format!("{} cut at {cut}", g.name),
                    &bytes,
                    &g.log,
                    &folded,
                ));
            }
        }
    }
    let reached = by_definition(&cases);
    for stop in ["stop_stalled", "stop_log", "stop_held"] {
        assert!(reached.contains(&format!("'{stop}'")), "{stop}: {reached}");
    }
}

/// A never-sent censor's golden journal: a call whose bytes never left, its
/// closing report as `sim` draws it — ready, refused once, on time.
fn never_sent() -> (journal::Header, Vec<Line>, Log) {
    let h = header_j();
    let records = vec![call("read", 1, 0, 0, 0)];
    let ran = run_waking_late(h.clone(), &records, W, &|_| 0, &|_| 0, &|_, _, _| {
        answer(TIMEOUT, None, Judgement::Unknown)
    })
    .unwrap();
    let log = log_of(&h, &records);
    (journal_header(&h, &log), ran.journal, log)
}

/// The report of `lines`, edited.
fn reported(lines: &mut [Line], edit: &dyn Fn(&mut journal::Answer)) {
    for l in lines.iter_mut() {
        if let Line::Answer(a) = l
            && a.unsent.is_some()
        {
            edit(a);
        }
    }
}

/// Decision 19: a never-sent censor's end is its closing report's to decide
/// — a Root iff the attempt was ready, the peer refused it by its own act,
/// and the harness's lateness with the report's hand-off and lag is at most
/// J; undecided otherwise, or for an early reply found late; and the report's
/// own rules, each broken alone, incoherent.
#[test]
fn a_never_sent_censor_is_its_report_s_to_decide() {
    use Class::*;
    let (h, lines, log) = never_sent();
    let report = lines
        .iter()
        .find_map(|l| match l {
            Line::Answer(a) => a.unsent.clone().map(|u| (a.at, u)),
            _ => None,
        })
        .expect("sim's closing report");
    let dispatched = lines
        .iter()
        .find_map(|l| match l {
            Line::Dispatch(d) => Some(d.at),
            _ => None,
        })
        .unwrap();
    assert!(
        report.1.handed.is_some() && report.1.refused >= 1,
        "{report:?}"
    );
    let mut cases = Vec::new();
    let mut check =
        |name: &str, edit: &dyn Fn(&mut Vec<Line>), want: Result<(&str, Class), &str>| {
            let mut l = lines.clone();
            edit(&mut l);
            let bytes = write(&h, &l);
            let folded = fold_bytes(&bytes, &log);
            match (want, &folded.verdict) {
                (Ok((verdict, class)), _) => assert_eq!(
                    (verdict_name(&folded.verdict), folded.classes.as_slice()),
                    (verdict, [class].as_slice()),
                    "{name}"
                ),
                (Err(why), Verdict::Void(Void::Incoherent { why: w, .. })) => {
                    assert_eq!(*w, why, "{name}")
                }
                (Err(why), other) => panic!("{name}: wanted {why}, folded {other:?}"),
            }
            cases.push(case(name, &bytes, &log, &folded));
        };
    let edited = |e: &'static dyn Fn(&mut journal::Answer)| move |l: &mut Vec<Line>| reported(l, e);
    check("ready, refused, on time", &|_| {}, Ok(("fail", Root)));
    check(
        "never ready",
        &edited(&|a| a.unsent.as_mut().unwrap().handed = None),
        Ok(("void:undecided", Undecided)),
    );
    check(
        "never refused",
        &edited(&|a| {
            let u = a.unsent.as_mut().unwrap();
            u.refused = 0;
            u.refused_by = None;
        }),
        Ok(("void:undecided", Undecided)),
    );
    check(
        "its lag at J",
        &edited(&|a| a.unsent.as_mut().unwrap().lag = J),
        Ok(("fail", Root)),
    );
    check(
        "its lag past J",
        &edited(&|a| a.unsent.as_mut().unwrap().lag = J + 1),
        Ok(("void:undecided", Undecided)),
    );
    check(
        "a hand-off and its lag at J",
        &move |l: &mut Vec<Line>| {
            reported(l, &move |a| {
                let u = a.unsent.as_mut().unwrap();
                u.handed = Some(dispatched + 100);
                u.lag = J - 100;
            })
        },
        Ok(("fail", Root)),
    );
    check(
        "a hand-off and its lag past J",
        &move |l: &mut Vec<Line>| {
            reported(l, &move |a| {
                let u = a.unsent.as_mut().unwrap();
                u.handed = Some(dispatched + 100);
                u.lag = J - 99;
            })
        },
        Ok(("void:undecided", Undecided)),
    );
    check(
        "an early reply found late",
        &edited(&|a| {
            a.found = Some(journal::Found {
                absent: a.at - 1,
                deadline: a.at - 1,
                handed: None,
            });
            a.unsent = None;
        }),
        Ok(("void:undecided", Undecided)),
    );
    check(
        "an early reply found in its look's microsecond",
        &edited(&|a| {
            a.found = Some(journal::Found {
                absent: a.at,
                deadline: a.at - 1,
                handed: None,
            });
            a.unsent = None;
        }),
        Ok(("void:undecided", Undecided)),
    );
    check(
        "no report",
        &|l: &mut Vec<Line>| l.retain(|x| !matches!(x, Line::Answer(a) if a.unsent.is_some())),
        Err("a never-sent censor without its closing report"),
    );
    check(
        "two reports",
        &|l: &mut Vec<Line>| {
            let r = l
                .iter()
                .find(|x| matches!(x, Line::Answer(a) if a.unsent.is_some()))
                .cloned()
                .unwrap();
            l.push(r);
        },
        Err("a closing report without its never-sent censor"),
    );
    check(
        "a report at its deadline",
        &edited(&|a| a.at -= 1),
        Err("a closing report against its censor"),
    );
    check(
        "a refusal's kind with none counted",
        &edited(&|a| a.unsent.as_mut().unwrap().refused = 0),
        Err("a closing report against its path"),
    );
    check(
        "connected, never ready",
        &edited(&|a| {
            let u = a.unsent.as_mut().unwrap();
            u.connected = u.handed;
            u.handed = None;
        }),
        Err("a closing report against its path"),
    );
    check(
        "a lag past its path",
        &edited(&|a| a.unsent.as_mut().unwrap().lag = a.at + 1),
        Err("a closing report against its path"),
    );
    check(
        "a report with an answer's content",
        &edited(&|a| a.status = Some(200)),
        Err("a closing report with an answer's content"),
    );
    let reached = by_definition(&cases);
    for counter in [
        "unsent_root",
        "unsent_unready",
        "unsent_unrefused",
        "unsent_late",
        "unsent_found",
    ] {
        assert!(
            reached.contains(&format!("'{counter}'")),
            "{counter}: {reached}"
        );
    }
    // A report of a sent censor has none to close.
    let sent = golden_journals()
        .into_iter()
        .find(|g| g.name == "censored_written_slowly")
        .unwrap();
    let (sh, mut sl) = read_lines(&sent.bytes);
    let mut r = lines
        .iter()
        .find(|x| matches!(x, Line::Answer(a) if a.unsent.is_some()))
        .cloned()
        .unwrap();
    if let Line::Answer(a) = &mut r {
        a.at = latest(&sl) + 1;
    }
    sl.push(r);
    let folded = fold_bytes(&write(&sh, &sl), &sent.log);
    assert!(
        matches!(&folded.verdict, Verdict::Void(Void::Incoherent { why, .. }) if *why == "a closing report without its never-sent censor"),
        "{:?}",
        folded.verdict
    );
}

/// An admission journaled and folded: its case's name, the journal's bytes,
/// its log, the fold, whether it is consistent.
struct Admitted {
    name: String,
    bytes: Vec<u8>,
    log: Log,
    folded: Folded,
    consistent: bool,
}

/// Every consistent base and every tamper of the recheck's census, each
/// journaled as a header alone and folded (K2b-7e-2b-3d-3b-3).
fn admissions() -> Vec<Admitted> {
    let log = log_of(&header_j(), &[]);
    let journaled = |k: u32, a: journal::Admission| {
        let mut h = journal_header(&header_j(), &log);
        h.catch_up = k;
        h.admission = Some(a);
        write(&h, &[])
    };
    let mut out = Vec::new();
    for (name, b) in admission::bases() {
        let (k, a) = admission::consistent(&b);
        let bytes = journaled(k, a);
        out.push(Admitted {
            name: format!("admission: {name}"),
            folded: fold_bytes(&bytes, &log),
            bytes,
            log: log.clone(),
            consistent: true,
        });
    }
    for (name, b, tamper, _) in admission::census() {
        let (mut k, mut a) = admission::consistent(&b);
        tamper(&mut k, &mut a);
        let bytes = journaled(k, a);
        out.push(Admitted {
            name: format!("admission: {name}"),
            folded: fold_bytes(&bytes, &log),
            bytes,
            log: log.clone(),
            consistent: false,
        });
    }
    out
}

/// K2b-7e-2b-3d-3b-3b of ADR-0047 (decision 22): admission's evidence by
/// both folds — consistent, read on; each tamper of the census void by its
/// rule's code, the definition naming the same, every code reached by the
/// definition too.
#[test]
fn admission_s_evidence_folds_as_the_definition_does() {
    let census = admission::census();
    let mut cases = Vec::new();
    let mut tampers = census.iter();
    for a in admissions() {
        if a.consistent {
            assert_eq!(a.folded.verdict, Verdict::Pass, "{}", a.name);
        } else {
            let (_, _, _, why) = tampers.next().unwrap();
            assert_eq!(
                a.folded.verdict,
                Verdict::Void(Void::Incoherent { line: 0, why }),
                "{}",
                a.name
            );
        }
        cases.push(case(&a.name, &a.bytes, &a.log, &a.folded));
    }
    let reached = by_definition(&cases);
    for why in ascetic_ddd_scheduler::recheck::WHYS_10 {
        assert!(reached.contains(&format!("'{why}'")), "{why}: {reached}");
    }
}

/// An admission's pinned line (K2b-7e-2b-3d-3b-4a): a consistent one's keyed
/// by the journal's version alone — its rules frozen with it —, a tampered
/// one's by the version and the fold's revision; the header it judged by its
/// digest, the verdict and the code.
fn pin(a: &Admitted) -> String {
    use sha2::Digest as _;
    let header = a.bytes.split(|b| *b == b'\n').next().unwrap();
    let digest: String = sha2::Sha256::digest(header)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let why = match &a.folded.verdict {
        Verdict::Void(Void::Incoherent { line: 0, why }) => Some(*why),
        _ => None,
    };
    serde_json::json!({
        "journal": JOURNAL_VERSION,
        "fold": (!a.consistent).then_some(FOLD_REVISION),
        "case": a.name,
        "header": digest,
        "verdict": verdict_name(&a.folded.verdict),
        "why": why,
    })
    .to_string()
}

const ADMISSIONS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/golden/admissions.jsonl");

/// A pinned line's key: its version, its revision where it has one, its case.
fn key(line: &str) -> (String, String, String) {
    let v: serde_json::Value = serde_json::from_str(line).unwrap();
    (
        v["journal"].to_string(),
        v["fold"].to_string(),
        v["case"].to_string(),
    )
}

/// Whether `file` pins each of `lines`, one line a key; why not.
fn pinned(file: &str, lines: &[String]) -> Result<(), String> {
    let mut keys = std::collections::BTreeSet::new();
    for line in file.lines() {
        if !keys.insert(key(line)) {
            return Err(format!("a key pinned twice: {line}"));
        }
    }
    match lines.iter().find(|line| !file.lines().any(|l| l == *line)) {
        Some(line) => Err(format!(
            "not pinned: {line}; a result moved under its key moves the key (JOURNAL_VERSION, \
             FOLD_REVISION), then append with the ignored `regenerate_admissions`"
        )),
        None => Ok(()),
    }
}

/// K2b-7e-2b-3d-3b-4a: every admission's result is pinned, one line a key,
/// the file appended to and never edited (CI's `append-only` holds the
/// latter): a rule's change that moves a result cannot be re-pinned under
/// the same key — a consistent one's moves the journal's version, a
/// tampered one's the fold's revision. A key pinned twice is refused, so a
/// moved result appended under its old key is too.
#[test]
#[expect(
    clippy::disallowed_methods,
    reason = "the pins are a file (ADR-0047 K1e-3 refuses files to the machine, not its tests)"
)]
fn admissions_are_pinned_one_line_a_key() {
    let lines: Vec<String> = admissions().iter().map(pin).collect();
    let file = std::fs::read_to_string(ADMISSIONS).unwrap();
    assert_eq!(pinned(&file, &lines), Ok(()));
    // The rule itself: a moved result re-pinned beside its old line under the
    // same key, and one not pinned, each refused.
    let first = &lines[0];
    let moved = first.replace(r#""verdict":"pass""#, r#""verdict":"fail""#);
    assert_ne!(&moved, first);
    let twice = format!("{first}\n{moved}\n");
    assert!(pinned(&twice, std::slice::from_ref(&moved)).is_err());
    assert!(pinned(&format!("{first}\n"), std::slice::from_ref(&moved)).is_err());
    assert_eq!(
        pinned(&format!("{first}\n"), std::slice::from_ref(first)),
        Ok(())
    );
}

/// Appends the admissions' lines not pinned yet, refusing to change one
/// pinned under its key. Run only on purpose.
#[test]
#[ignore = "appends to golden/admissions.jsonl"]
#[expect(
    clippy::disallowed_methods,
    reason = "the pins are a file (ADR-0047 K1e-3 refuses files to the machine, not its tests)"
)]
fn regenerate_admissions() {
    let mut file = std::fs::read_to_string(ADMISSIONS).unwrap_or_default();
    for a in admissions() {
        let line = pin(&a);
        match file.lines().find(|l| key(l) == key(&line)) {
            Some(l) => assert_eq!(l, line, "a pinned line changed under its key"),
            None => {
                file.push_str(&line);
                file.push('\n');
            }
        }
    }
    std::fs::write(ADMISSIONS, file).unwrap();
}
