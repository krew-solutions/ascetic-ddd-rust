//! The verdict (ADR-0047 decision 14, K1e-3c-2): golden runs, each with its
//! classes and verdict written by hand; the fold against the verdict by
//! definition (`vectors/verdict.py`) on every golden and every twin; twins of
//! broken evidence, each journal resealed so that only the fold can refuse it.

// The command log's fixture reads its corpus's directory; the lint gate refuses
// files to the machine, not to its tests (ADR-0047 K1e-3).
#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use std::io::{Cursor, Write as _};
use std::process::{Command, Stdio};
use std::sync::Arc;

use ascetic_ddd_command_log::record::{Dep, Header, On, Record, Safety, StepId};
use ascetic_ddd_scheduler::journal::{self, JOURNAL_VERSION, Line, Reader, Writer};
use ascetic_ddd_scheduler::machine::{Input, Judgement, MACHINE_REVISION, Machine, Output};
use ascetic_ddd_scheduler::sim::{Attempt, run_waking_late};
use ascetic_ddd_scheduler::verdict::{Class, FOLD_REVISION, Folded, Log, Verdict, fold};
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
        validator: ascetic_ddd_command_log::live::validator(),
        read_ahead: header.read_ahead,
        jitter: header.profile.jitter,
        resend: header.profile.resend,
        namespace: 1,
        t0: 1_763_640_000_000_000,
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
    serde_json::json!({
        "name": name,
        "log": {"sha256": log.sha256, "records": log.records},
        "journal": lines,
        "verdict": verdict_name(&folded.verdict),
        "classes": folded.classes.iter().map(|c| c.name()).collect::<Vec<_>>(),
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
    // An observation's cause sent past J after its intended start, the
    // observation unknown: the harness's lateness, not the system's.
    let c = call("update", 1, 0, 0, 0);
    let o = watched(1, 100, &c.step, 2_000);
    add(
        "late_cause",
        header_j(),
        vec![c, o],
        Box::new(|line, _| {
            if line == 2 {
                answer(1_000, Some(100), Judgement::Expected(vec![]))
            } else {
                answer(0, None, Judgement::Unknown)
            }
        }),
        vec![Passed, Undecided],
        "void:undecided",
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
        "undecided_call",
        "undecided_read",
        "undecided_cause",
        "undecided_backstop",
        "undecided_ended_late",
        "undecided_resend",
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
                    class: journal::FaultClass::Backstop,
                    line: 2,
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
