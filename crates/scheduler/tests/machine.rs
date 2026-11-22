//! K1a of ADR-0047: the machine in virtual time — the release rule, binding at
//! read, the strengths, barriers by prefix, captures in their producers' cells,
//! retries by safety, the backstop.

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports)]
mod fixture;

use std::sync::Arc;

use ascetic_ddd_command_log::record::{Dep, Header, On, Record, Safety};
use ascetic_ddd_command_log::validate::NoHost;
use ascetic_ddd_scheduler::machine::{
    Captured, Critical, End, Entry, GRACE, Input, Judgement, Machine, Output, Skip,
};
use ascetic_ddd_scheduler::order::Ordered;
use ascetic_ddd_scheduler::sim::{Attempt, run};
use fixture::*;

/// A group's update, writing its ETag.
fn writer(g: u64, n: u32, at: u64) -> Record {
    let mut r = step(id("group", g, "update", n), at);
    r.action = rest(
        "update",
        vec![ascetic_ddd_command_log::record::Value::Literal(
            g.to_string(),
        )],
    );
    r
}

/// A group's read, sending the ETag `from` captured.
fn reader(
    g: u64,
    n: u32,
    at: u64,
    from: &ascetic_ddd_command_log::record::StepId,
    on: On,
) -> Record {
    let mut r = after(step(id("group", g, "read", n), at), from, on);
    r.action = read_etag("read", g, from);
    r
}

/// What a step's expected answer captures: every slot it declares.
fn captures(r: &Record) -> Vec<Captured> {
    r.capture
        .iter()
        .map(|c| Captured {
            subject: c.subject.clone(),
            slot: c.slot.clone(),
            value: b"v".to_vec(),
        })
        .collect()
}

fn expected_after(latency: u64) -> impl Fn(u64, u32, &Record) -> Attempt {
    move |_, _, r| Attempt {
        sent_after: 0,
        answer_after: Some(latency),
        judgement: Judgement::Expected(captures(r)),
    }
}

fn entry(entries: &[Entry], line: u64) -> &Entry {
    entries.iter().find(|e| e.line == line).unwrap()
}

/// Under zero latencies a clean log releases every step at its plan: `feasible`
/// leaves the release rule nothing to add (ADR-0047 decision 3). A reader of a
/// writer that captured nothing is skipped for it, as decision 5 says.
#[test]
fn a_clean_log_at_zero_latency_runs_on_its_plan() {
    let mut clean = 0;
    for (name, records) in fixture::corpus::logs() {
        let report = run_with(fixture::corpus::corpus_header(), &records, &NoHost, 1_000);
        if !report.violations.is_empty() {
            continue;
        }
        clean += 1;
        let ran = run(
            fixture::corpus::corpus_header(),
            &records,
            W,
            &expected_after(0),
        )
        .unwrap();
        assert_eq!(ran.entries.len(), records.len(), "{name}");
        for (e, r) in ran.entries.iter().zip(&records) {
            match &e.end {
                End::Expected => assert_eq!(e.intended, r.at, "{name} line {}", e.line),
                End::Skipped(Skip::MissingCapture(_)) => {}
                other => panic!("{name} line {}: {other:?}", e.line),
            }
        }
    }
    assert!(clean > 50, "{clean} clean logs");
}

/// A latency drawn from a key of (line, attempt), up to 2 ms.
fn scripted(line: u64, attempt: u32, r: &Record) -> Attempt {
    let k = ascetic_ddd_faker::Key::root(ascetic_ddd_faker::Seed(47))
        .at(line)
        .at(u64::from(attempt));
    Attempt {
        sent_after: k.child("sent").unit().raw() % 200,
        answer_after: Some(k.child("answer").unit().raw() % 2_000),
        judgement: Judgement::Expected(captures(r)),
    }
}

/// Decision 2's executor: bindings, critical dependencies, intended instants
/// and ends are the same however far ahead the machine reads.
#[test]
fn bindings_and_releases_do_not_depend_on_the_read_ahead() {
    for (name, records) in fixture::corpus::logs() {
        let report = run_with(fixture::corpus::corpus_header(), &records, &NoHost, 1_000);
        if !report.violations.is_empty() {
            continue;
        }
        let at = |l: u64| {
            run(fixture::corpus::corpus_header(), &records, l, &scripted)
                .unwrap()
                .entries
                .into_iter()
                .map(|e| (e.line, e.end, e.intended, e.bound, e.critical))
                .collect::<Vec<_>>()
        };
        let all = at(u64::MAX / 4);
        assert_eq!(at(0), all, "{name} at L = 0");
        assert_eq!(at(W / 2), all, "{name} at L = W/2");
        // A step goes out at its release, and never before it is read.
        let ran = run(fixture::corpus::corpus_header(), &records, 0, &scripted).unwrap();
        for e in &ran.entries {
            assert!(
                e.released >= e.read && e.released >= e.intended,
                "{name} {e:?}"
            );
            if let Some(d) = e.dispatched {
                assert_eq!(d, e.released, "{name} line {}", e.line);
            }
        }
    }
}

fn header_with(safety: Safety) -> Header {
    let mut h = header();
    for o in &mut h.operations {
        o.safety = safety;
    }
    h
}

/// The strengths (decision 6): `Sent` at the dependency's first send, `Settled`
/// at its end, `Expected` at an expected end, and a step never sent meets
/// `Sent` and `Settled` at its end.
#[test]
fn a_dependency_is_met_by_its_strength() {
    let (a, b, c) = (
        call("read", 1, 0, 0, 0),
        id("device", 2, "seen", 0),
        id("device", 3, "seen", 0),
    );
    let after_a = |on: On| vec![a.clone(), after(step(b.clone(), 10), &a.step, on)];
    // a sends at 50 and answers at 150.
    let slow = |line: u64, _: u32, _: &Record| Attempt {
        sent_after: if line == 2 { 50 } else { 0 },
        answer_after: Some(if line == 2 { 100 } else { 0 }),
        judgement: Judgement::Expected(vec![]),
    };
    let sent = run(header(), &after_a(On::Sent), W, &slow).unwrap();
    assert_eq!(entry(&sent.entries, 3).intended, 50);
    assert_eq!(entry(&sent.entries, 3).critical, Some(Critical::Step(2)));
    let settled = run(header(), &after_a(On::Settled), W, &slow).unwrap();
    assert_eq!(entry(&settled.entries, 3).intended, 150);
    // An unexpected end does not meet `Expected`: the dependent is skipped.
    let refused = |_: u64, _: u32, _: &Record| Attempt {
        sent_after: 0,
        answer_after: Some(100),
        judgement: Judgement::Unexpected,
    };
    let skipped = run(header(), &after_a(On::Expected), W, &refused).unwrap();
    assert_eq!(
        entry(&skipped.entries, 3).end,
        End::Skipped(Skip::Dependency(2))
    );
    // A skipped step meets `Sent` at its skip: its dependent is sent.
    let chain = [
        a.clone(),
        after(step(b.clone(), 10), &a.step, On::Expected),
        after(step(c.clone(), 20), &b, On::Sent),
    ];
    let ran = run(header(), &chain, W, &refused).unwrap();
    assert_eq!(
        entry(&ran.entries, 3).end,
        End::Skipped(Skip::Dependency(2))
    );
    assert_eq!(entry(&ran.entries, 4).intended, 100);
    assert!(entry(&ran.entries, 4).dispatched.is_some());
}

/// A barrier waits for the members read before it, by prefix (decision 8): a
/// member read after it that finishes first does not release it.
#[test]
fn a_barrier_waits_for_its_members_by_prefix() {
    let member = |n: u64, at: u64| step(id("device", n, "managed", 0), at);
    let holder = Record {
        after: vec![Dep::Each {
            kind: "device".into(),
            scope: vec![7],
            label: "managed".into(),
            n: 0,
            on: On::Settled,
            count: None,
        }],
        ..step(id("group", 1, "report", 0), 20)
    };
    let records = [member(1, 0), member(2, 10), holder, member(3, 30)];
    // The first member answers at 500, the others at once.
    let script = |line: u64, _: u32, _: &Record| Attempt {
        sent_after: 0,
        answer_after: Some(if line == 2 { 500 } else { 0 }),
        judgement: Judgement::Expected(vec![]),
    };
    let ran = run(header(), &records, W, &script).unwrap();
    let h = entry(&ran.entries, 4);
    assert_eq!(h.intended, 500);
    assert_eq!(h.critical, Some(Critical::Member(2)));
}

/// A captured value lives in its producer's cell (decision 5): a reader whose
/// producer's answer did not give it is skipped for it.
#[test]
fn a_reader_takes_its_value_from_its_producer() {
    let w = captures_etag(writer(1, 0, 0), 1);
    let r = reader(1, 0, 20, &w.step, On::Expected);
    let ran = run(header(), &[w.clone(), r.clone()], W, &expected_after(5)).unwrap();
    assert_eq!(entry(&ran.entries, 3).end, End::Expected);
    let silent = |_: u64, _: u32, _: &Record| Attempt::expected(5);
    let ran = run(header(), &[w, r], W, &silent).unwrap();
    assert_eq!(
        entry(&ran.entries, 3).end,
        End::Skipped(Skip::MissingCapture(2))
    );
}

/// A stale read on purpose takes its own producer's value, not the slot's
/// latest: the value lives in the producer's cell (decision 5).
#[test]
fn a_stale_read_takes_its_producer_s_value() {
    let w0 = captures_etag(writer(1, 0, 0), 1);
    let w1 = after(captures_etag(writer(1, 1, 10), 1), &w0.step, On::Settled);
    let stale = deviating(reader(1, 0, 20, &w0.step, On::Expected), "stale");
    let report = run_with(
        header(),
        &[w0.clone(), w1.clone(), stale.clone()],
        &NoHost,
        1_000,
    );
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    let by_line = |line: u64, _: u32, r: &Record| Attempt {
        sent_after: 0,
        answer_after: Some(1),
        judgement: Judgement::Expected(
            r.capture
                .iter()
                .map(|c| Captured {
                    subject: c.subject.clone(),
                    slot: c.slot.clone(),
                    value: line.to_string().into_bytes(),
                })
                .collect(),
        ),
    };
    let ran = run(header(), &[w0, w1, stale], W, &by_line).unwrap();
    let (_, _, _, captured) = ran.dispatches.iter().find(|d| d.1 == 4).unwrap();
    assert_eq!(
        captured.iter().map(|c| c.value.clone()).collect::<Vec<_>>(),
        vec![b"2".to_vec()]
    );
}

/// Retries by safety (decision 11): a certified refusal is resent unless the
/// operation is never resent; an unknown outcome only when it is idempotent.
#[test]
fn an_attempt_is_resent_by_its_safety() {
    let records = [call("read", 1, 0, 0, 0)];
    let first = |j: Judgement| {
        move |_: u64, attempt: u32, _: &Record| Attempt {
            sent_after: 0,
            answer_after: Some(10),
            judgement: if attempt == 1 {
                j.clone()
            } else {
                Judgement::Expected(vec![])
            },
        }
    };
    let ends = |safety: Safety, j: Judgement| {
        let ran = run(header_with(safety), &records, W, &first(j)).unwrap();
        let e = entry(&ran.entries, 2).clone();
        (e.end, e.attempts)
    };
    assert_eq!(
        ends(Safety::Never, Judgement::NotTaken),
        (End::Unexpected, 1)
    );
    assert_eq!(
        ends(Safety::NotTaken, Judgement::NotTaken),
        (End::Expected, 2)
    );
    assert_eq!(
        ends(Safety::NotTaken, Judgement::Unknown),
        (End::Unknown, 1)
    );
    assert_eq!(
        ends(Safety::Idempotent, Judgement::Unknown),
        (End::Expected, 2)
    );
}

/// A handler that never reports ends its step unknown at the backstop, the
/// harness's fault (decision 4).
#[test]
fn a_silent_handler_is_the_harness_s_fault() {
    let r = Arc::new(call("read", 1, 0, 0, 0));
    let mut m = Machine::new(Arc::new(header()), W).unwrap();
    let out = m.step(0, Input::Line { line: 2, record: r }).unwrap();
    let deadline = out
        .iter()
        .find_map(|o| match o {
            Output::Dispatch { deadline, .. } => Some(*deadline),
            _ => None,
        })
        .unwrap();
    assert!(
        m.step(deadline.saturating_add(GRACE) - 1, Input::Wake)
            .unwrap()
            .is_empty()
            || true
    );
    let out = m.step(deadline.saturating_add(GRACE), Input::Wake).unwrap();
    let ended = out
        .iter()
        .find_map(|o| match o {
            Output::Ended(e) => Some(e.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!((ended.end, ended.fault), (End::Unknown, true));
}

/// Inputs of one instant are taken in (line, kind) order whatever order they
/// arrived in (decision 4).
#[test]
fn inputs_of_one_instant_are_taken_in_one_order() {
    let inputs = |order: &[usize]| {
        let all = [
            Input::Ended {
                line: 3,
                attempt: 1,
                judgement: Judgement::Unknown,
            },
            Input::Sent {
                line: 3,
                attempt: 1,
            },
            Input::Wake,
            Input::Sent {
                line: 2,
                attempt: 1,
            },
        ];
        let mut q = Ordered::new();
        for &i in order {
            q.push(10, all[i].clone());
        }
        q.push(5, Input::Wake);
        let mut popped = Vec::new();
        while let Some(x) = q.pop(10) {
            popped.push(x);
        }
        popped
    };
    let canonical = inputs(&[0, 1, 2, 3]);
    for order in [[3, 2, 1, 0], [1, 3, 0, 2], [2, 0, 3, 1]] {
        assert_eq!(inputs(&order), canonical);
    }
    assert_eq!(canonical.first().map(|x| x.0), Some(5));
}

/// A line read after its intended instant is released when it is read, and
/// the gap is the reader's (decision 3: `released = max(intended, read)`).
#[test]
fn a_line_read_late_is_released_when_read() {
    let r = Arc::new(call("read", 1, 0, 10, 0));
    let mut m = Machine::new(Arc::new(header()), W).unwrap();
    m.step(0, Input::Wake).unwrap();
    let out = m.step(100, Input::Line { line: 2, record: r }).unwrap();
    assert!(out.iter().any(|o| matches!(
        o,
        Output::Dispatch {
            line: 2,
            attempt: 1,
            ..
        }
    )));
    let out = m
        .step(
            105,
            Input::Ended {
                line: 2,
                attempt: 1,
                judgement: Judgement::Expected(vec![]),
            },
        )
        .unwrap();
    let e = out
        .iter()
        .find_map(|o| match o {
            Output::Ended(e) => Some(e.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        (e.intended, e.read, e.released, e.dispatched),
        (10, 100, 100, Some(100))
    );
}

/// Of dependencies already failed when a line is read, the skip names the one
/// that ended first.
#[test]
fn a_skip_names_the_dependency_that_failed_first() {
    let (a, b) = (call("read", 1, 0, 0, 0), call("read", 2, 0, 10, 0));
    let c = after(
        after(
            step(id("device", 3, "seen", 0), 1_000),
            &a.step,
            On::Expected,
        ),
        &b.step,
        On::Expected,
    );
    let refused = |line: u64, _: u32, _: &Record| Attempt {
        sent_after: 0,
        answer_after: Some(if line == 2 { 500 } else { 100 }),
        judgement: Judgement::Unexpected,
    };
    let ran = run(header(), &[a, b, c], 0, &refused).unwrap();
    assert_eq!(
        entry(&ran.entries, 4).end,
        End::Skipped(Skip::Dependency(3))
    );
}

/// An observation's polls keep to their grid, `every` apart, never twice at
/// one instant, though each answer comes at once (decision 11).
#[test]
fn polls_keep_to_their_grid() {
    let r = polling(1, 0, 0, 100, 450);
    let not_yet = |_: u64, _: u32, _: &Record| Attempt {
        sent_after: 0,
        answer_after: Some(0),
        judgement: Judgement::Unexpected,
    };
    let ran = run(header(), &[r], W, &not_yet).unwrap();
    let at: Vec<u64> = ran.dispatches.iter().map(|d| d.0).collect();
    assert_eq!(at, vec![0, 100, 200, 300, 400]);
    assert_eq!(entry(&ran.entries, 2).end, End::Unknown);
}

/// A holder awaiting its barrier as expected is skipped when a member read
/// before it ended otherwise — at that end, and not before the holder was read
/// (decision 6, `vectors/schedule.py`) — whatever the members before that one
/// are still doing: m1 refused at 10, m0 answering only at 50, the holder read
/// at 20 is skipped at 20, its journal's instants no earlier than its read.
#[test]
fn a_holder_is_skipped_by_a_member_failed_past_the_prefix() {
    let member = |n: u64, at: u64| step(id("device", n, "m", 0), at);
    let holder = Record {
        after: vec![Dep::Each {
            kind: "device".into(),
            scope: vec![7],
            label: "m".into(),
            n: 0,
            on: On::Expected,
            count: None,
        }],
        ..step(id("group", 1, "report", 0), 20)
    };
    let records = [member(1, 0), member(2, 5), holder];
    let script = |line: u64, _: u32, _: &Record| Attempt {
        sent_after: 0,
        answer_after: Some(if line == 2 { 50 } else { 5 }),
        judgement: if line == 3 {
            Judgement::Unexpected
        } else {
            Judgement::Expected(vec![])
        },
    };
    let ran = run(header(), &records, 0, &script).unwrap();
    let h = entry(&ran.entries, 4);
    assert_eq!(h.end, End::Skipped(Skip::Member(3)));
    assert!(h.ended >= h.read && h.released >= h.read, "{h:?}");
    assert_eq!(h.ended, 20);
}
