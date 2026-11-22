//! K1d of ADR-0047: runs of the machine in `sim`, written as traces that
//! verify/tla/TraceScheduler.tla checks — the log, every input the machine
//! took with its stamp, in its order, and its journal.
//!
//! With `ASCETIC_DDD_TRACE_DIR` set, each run is written there (`make
//! traces`). Otherwise it must equal the trace the repository holds under
//! verify/tla/traces: a change of the machine's behaviour fails here until its
//! traces are recorded again and checked against the model.

// The command log's fixture reads its corpus's directory; the lint gate refuses
// files to the machine, not to its tests (ADR-0047 K1e-3).
#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use std::collections::BTreeMap;

use ascetic_ddd_command_log::record::{
    Action, Dep, Header, Hole, On, Record, Safety, StepId, Subject, Value,
};
use ascetic_ddd_command_log::validate::{NoHost, holes};
use ascetic_ddd_scheduler::machine::{Captured, End, Entry, Input, Judgement, Skip};
use ascetic_ddd_scheduler::sim::{Attempt, Run, run_reading};
use fixture::*;
use serde_json::{Value as Json, json};

/// The header: the fixture's, with the update resent when not taken.
fn trace_header() -> Header {
    let mut h = header();
    for op in &mut h.operations {
        if op.name == "update" {
            op.safety = Safety::NotTaken;
        }
    }
    h
}

fn member(n: u64, at: u64) -> Record {
    step(id("device", n, "a", 0), at)
}

/// Six steps: a writer whose ETag a reader sends; a call awaiting the writer
/// sent, another awaiting that one settled; a barrier over both; a member read
/// after the barrier, awaiting it and the reader expected.
fn log() -> Vec<Record> {
    let w = captures_etag(
        Record {
            action: rest("update", vec![Value::Literal("1".into())]),
            ..step(id("group", 1, "update", 0), 0)
        },
        1,
    );
    let a = after(member(1, 100), &w.step, On::Sent);
    let r = after(
        Record {
            action: read_etag("read", 1, &w.step),
            ..step(id("group", 1, "read", 0), 500)
        },
        &w.step,
        On::Expected,
    );
    let b = after(member(2, 600), &a.step, On::Settled);
    let holder = Record {
        after: vec![Dep::Each {
            kind: "device".into(),
            scope: vec![7],
            label: "a".into(),
            n: 0,
            on: On::Settled,
            count: None,
        }],
        ..step(id("group", 2, "report", 0), 1_000)
    };
    let last = after(
        after(member(3, 1_500), &holder.step, On::Expected),
        &r.step,
        On::Expected,
    );
    vec![w, a, r, b, holder, last]
}

fn captures(r: &Record, line: u64) -> Vec<Captured> {
    r.capture
        .iter()
        .map(|c| Captured {
            subject: c.subject.clone(),
            slot: c.slot.clone(),
            value: line.to_string().into_bytes(),
        })
        .collect()
}

/// An attempt: its bytes leave after `sent`, its answer `after` later.
fn answered(sent: u64, after: Option<u64>, judgement: Judgement) -> Attempt {
    Attempt {
        sent_after: sent,
        answer_after: after,
        judgement,
    }
}

fn key(s: &StepId) -> String {
    format!(
        "{}/{:?}/{}/{}",
        s.subject.kind, s.subject.scope, s.label, s.n
    )
}

fn subject_key(kind: &str, scope: &[u8], label: &str, n: u32) -> String {
    key(&StepId {
        subject: Subject {
            kind: kind.into(),
            scope: scope.to_vec(),
            index: 0,
        },
        label: label.into(),
        n,
    })
}

fn on(o: On) -> &'static str {
    match o {
        On::Sent => "sent",
        On::Settled => "settled",
        On::Expected => "expected",
    }
}

/// The log as the model reads it: lines from 2, dependencies by the line of
/// their step's id — the log's own, not the machine's binding.
fn log_json(h: &Header, records: &[Record], read_ahead: u64) -> Json {
    let mut lines_of: BTreeMap<String, u64> = BTreeMap::new();
    let mut lines = Vec::new();
    for (i, r) in records.iter().enumerate() {
        let line = i as u64 + 2;
        let line_of = |s: &StepId| {
            lines_of
                .get(&serde_json::to_string(s).unwrap())
                .copied()
                .unwrap()
        };
        let after: Vec<Json> = r
            .after
            .iter()
            .map(|d| match d {
                Dep::Step { step, on: o } => json!({"step": line_of(step), "on": on(*o)}),
                Dep::Each {
                    kind,
                    scope,
                    label,
                    n,
                    on: o,
                    ..
                } => json!({"each": subject_key(kind, scope, label, *n), "on": on(*o)}),
            })
            .collect();
        let producer = holes(r)
            .into_iter()
            .find_map(|h| match h {
                Hole::Captured { from, .. } => Some(line_of(from)),
                _ => None,
            })
            .unwrap_or(0);
        let (timeout, safety) = match &r.action {
            Action::Rest(rest) => {
                let op = h.operations.iter().find(|o| o.name == rest.op).unwrap();
                (op.timeout, op.safety)
            }
            Action::Host(call) => {
                let c = h.calls.iter().find(|c| c.name == call.op).unwrap();
                (c.timeout, c.safety)
            }
            other => panic!("the model has no {other:?}"),
        };
        lines.push(json!({
            "at": r.at,
            "think": r.think,
            "after": after,
            "key": key(&r.step),
            "producer": producer,
            "captures": !r.capture.is_empty(),
            "timeout": timeout,
            "safety": match safety {
                Safety::Never => "never",
                Safety::NotTaken => "not_taken",
                Safety::Idempotent => "idempotent",
            },
            "retained": h.retained.contains(&r.step.label),
        }));
        lines_of.insert(serde_json::to_string(&r.step).unwrap(), line);
    }
    json!({
        "observer": "scheduler",
        "event": "log",
        "w": h.window,
        "read_ahead": read_ahead,
        "lines": lines,
    })
}

fn input_json(at: u64, input: &Input, records: &[Record]) -> Json {
    match input {
        Input::Line { line, .. } => {
            json!({"observer": "scheduler", "event": "input", "input": "read", "at": at, "line": line})
        }
        Input::Sent { line, attempt } => json!({
            "observer": "scheduler", "event": "input", "input": "sent",
            "at": at, "line": line, "attempt": attempt,
        }),
        Input::Ended {
            line,
            attempt,
            judgement,
        } => {
            let r = &records[(*line - 2) as usize];
            let j = match judgement {
                Judgement::Expected(c) if !r.capture.is_empty() && c.is_empty() => "bare",
                Judgement::Expected(_) => "expected",
                Judgement::Unexpected => "unexpected",
                Judgement::NotTaken => "not_taken",
                Judgement::Unknown => "unknown",
            };
            json!({
                "observer": "scheduler", "event": "input", "input": "ended",
                "at": at, "line": line, "attempt": attempt, "judgement": j,
            })
        }
        Input::Wake => unreachable!("wakes are not recorded"),
    }
}

fn entry_json(e: &Entry) -> Json {
    let end = match &e.end {
        End::Expected => json!("expected"),
        End::Unexpected => json!("unexpected"),
        End::Unknown => json!("unknown"),
        End::Skipped(Skip::Dependency(l)) => json!(["skipped", "dependency", l]),
        End::Skipped(Skip::Member(l)) => json!(["skipped", "member", l]),
        End::Skipped(Skip::MissingCapture(l)) => json!(["skipped", "missing_capture", l]),
    };
    json!({
        "observer": "scheduler", "event": "journal", "line": e.line, "end": end,
        "read": e.read, "intended": e.intended, "released": e.released,
        "dispatched": e.dispatched, "sent": e.sent, "ended": e.ended,
        "attempts": e.attempts, "bound": e.bound,
    })
}

fn trace_text(h: &Header, records: &[Record], read_ahead: u64, ran: &Run) -> String {
    let mut out = String::new();
    let mut push = |j: Json| {
        out.push_str(&serde_json::to_string(&j).unwrap());
        out.push('\n');
    };
    push(log_json(h, records, read_ahead));
    for (at, input) in &ran.inputs {
        push(input_json(*at, input, records));
    }
    for e in &ran.entries {
        push(entry_json(e));
    }
    out
}

/// Runs the log under the script and writes or checks its trace.
#[allow(
    clippy::disallowed_methods,
    reason = "the test harness, not the machine, asks where `make traces` records"
)]
// A test writes and reads its traces; the lint gate is the machine's.
#[expect(
    clippy::disallowed_methods,
    reason = "the golden traces are files (ADR-0047 K1e-3 refuses files to the machine, not its tests)"
)]
fn trace(
    name: &str,
    read_ahead: u64,
    lag: &dyn Fn(u64) -> u64,
    script: &dyn Fn(u64, u32, &Record) -> Attempt,
) -> Run {
    let h = trace_header();
    let records = log();
    let report = run_with(h.clone(), &records, &NoHost, 1_000);
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    let ran = run_reading(h.clone(), &records, read_ahead, lag, script).unwrap();
    assert_eq!(ran.entries.len(), records.len(), "{name}: every line ends");
    assert_eq!(ran.left, ran.kept, "{name}: cells left behind");
    assert_eq!(
        ran.barriers as u64, report.peaks.barriers,
        "{name}: barriers"
    );
    let text = trace_text(&h, &records, read_ahead, &ran);
    let file = format!("scheduler-{name}.jsonl");
    match std::env::var_os("ASCETIC_DDD_TRACE_DIR") {
        Some(dir) => std::fs::write(std::path::Path::new(&dir).join(&file), &text).unwrap(),
        None => {
            let kept = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../verify/tla/traces")
                .join(&file);
            let held = std::fs::read_to_string(&kept).unwrap_or_else(|e| {
                panic!("{}: {e}; record it with `make traces`", kept.display())
            });
            assert!(
                held == text,
                "{file} no longer records this run: record the traces again with `make traces`, \
                 which checks them against verify/tla/Scheduler.tla"
            );
        }
    }
    ran
}

fn entry(ran: &Run, line: u64) -> &Entry {
    ran.entries.iter().find(|e| e.line == line).unwrap()
}

/// A not-taken update resent; an idempotent call resent after an unknown;
/// the rest answered as expected.
#[test]
fn retries() {
    let script = |line: u64, attempt: u32, r: &Record| match (line, attempt) {
        (2, 1) => answered(50, Some(200), Judgement::NotTaken),
        (3, 1) => answered(20, Some(100), Judgement::Unknown),
        _ => answered(
            20,
            Some(150 + 10 * line),
            Judgement::Expected(captures(r, line)),
        ),
    };
    let ran = trace("retries", 3_000, &|_| 0, &script);
    assert_eq!(entry(&ran, 2).attempts, 2);
    assert_eq!(entry(&ran, 3).attempts, 2);
    assert!(ran.entries.iter().all(|e| e.end == End::Expected));
}

/// The writer's answer gives no ETag: its reader is skipped for the missing
/// value, and the last step for the reader; a call ending unexpected leaves
/// its settled dependent and the barrier to run.
#[test]
fn skips() {
    let script = |line: u64, _: u32, r: &Record| match line {
        2 => answered(30, Some(100), Judgement::Expected(vec![])),
        3 => answered(10, Some(400), Judgement::Unexpected),
        _ => answered(10, Some(200), Judgement::Expected(captures(r, line))),
    };
    let ran = trace("skips", 3_000, &|_| 0, &script);
    assert_eq!(entry(&ran, 4).end, End::Skipped(Skip::MissingCapture(2)));
    assert_eq!(entry(&ran, 7).end, End::Skipped(Skip::Dependency(4)));
    assert_eq!(entry(&ran, 5).end, End::Expected);
    assert_eq!(entry(&ran, 6).end, End::Expected);
}

/// A call never answered ends unknown at its deadline, and its settled
/// dependent runs then; the writer refused, its reader is skipped.
#[test]
fn deadline() {
    let script = |line: u64, _: u32, r: &Record| match line {
        2 => answered(10, Some(300), Judgement::Unexpected),
        3 => answered(10, None, Judgement::Unknown),
        _ => answered(10, Some(200), Judgement::Expected(captures(r, line))),
    };
    let ran = trace("deadline", 3_000, &|_| 0, &script);
    let call = entry(&ran, 3);
    assert_eq!(call.end, End::Unknown);
    assert_eq!(call.ended, call.released + TIMEOUT);
    assert_eq!(entry(&ran, 4).end, End::Skipped(Skip::Dependency(2)));
    assert_eq!(entry(&ran, 5).released, call.ended);
}

/// Read with no read-ahead, two lines late: released when read.
#[test]
fn late_reader() {
    let script = |line: u64, _: u32, r: &Record| {
        answered(
            15,
            Some(100 + 25 * line),
            Judgement::Expected(captures(r, line)),
        )
    };
    let lag = |line: u64| if line == 4 || line == 6 { 2_000 } else { 0 };
    let ran = trace("late-reader", 0, &lag, &script);
    assert!(entry(&ran, 4).read > entry(&ran, 4).intended);
    assert_eq!(entry(&ran, 4).released, entry(&ran, 4).read);
    assert!(ran.entries.iter().all(|e| e.end == End::Expected));
}
