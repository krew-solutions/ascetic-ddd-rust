//! K1e-2 of ADR-0047, executor K1-8's stalls: the plan's own bound is what the
//! machine holds when every action ends at its deadline — tight, not only an
//! upper bound — and what it holds under any uniform stall below it; a stand
//! that never answers ends every step unknown within it; a reader late past a
//! step's plan breaks the assumption it rests on; `fits` refuses at and past
//! each boundary.

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports)]
mod fixture;

use ascetic_ddd_command_log::record::{Header, On, Record};
use ascetic_ddd_command_log::validate::{NoHost, Peaks};
use ascetic_ddd_scheduler::fits::{Budget, Refusal, fits, needed};
use ascetic_ddd_scheduler::machine::{End, Judgement};
use ascetic_ddd_scheduler::sim::{Attempt, Run, run};
use fixture::*;

/// T: every host call's timeout.
const T: u64 = 10_000;
/// The read-ahead L and the window W.
const L: u64 = 2_000;
const WINDOW: u64 = 4_000;

fn plan_header(timeout: u64) -> Header {
    let mut h = Header {
        window: WINDOW,
        read_ahead: L,
        ..header()
    };
    for c in &mut h.calls {
        c.timeout = timeout;
    }
    h
}

/// Chains of depth six over ten subjects, a step 3,107 µs after the one it
/// awaits — below T, so that worst lateness accumulates down a chain — awaited
/// settled and expected in turn; the subjects 1,301 µs apart, every instant
/// distinct (an end at a read's instant would be taken before it).
fn chains() -> Vec<Record> {
    let mut records = Vec::new();
    for i in 0..10u64 {
        for j in 0..6u32 {
            let at = i * 1_301 + u64::from(j) * 3_107;
            let mut r = step(id("device", i, "chain", j), at);
            if j > 0 {
                let on = if j % 2 == 0 {
                    On::Expected
                } else {
                    On::Settled
                };
                r = after(r, &id("device", i, "chain", j - 1), on);
            }
            records.push(r);
        }
    }
    records.sort_by(|a, b| (a.at, &a.step).cmp(&(b.at, &b.step)));
    records
}

/// Every attempt sent at once and answered expected `s` after — or never.
fn stalled(s: Option<u64>) -> impl Fn(u64, u32, &Record) -> Attempt {
    move |_, _, _| Attempt {
        sent_after: 0,
        answer_after: s,
        judgement: Judgement::Expected(vec![]),
    }
}

fn peaks(timeout: u64, records: &[Record]) -> Peaks {
    let report = run_with(plan_header(timeout), records, &NoHost, 1_000);
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    report.peaks
}

fn ran(s: Option<u64>, records: &[Record]) -> Run {
    run(plan_header(T), records, L, &stalled(s)).unwrap()
}

/// A stall of s below T holds what the bound derives with s for every
/// timeout, and no more than the plan's own; at T, the plan's own exactly.
#[test]
fn a_uniform_stall_holds_what_the_bound_derives() {
    let records = chains();
    let own = peaks(T, &records);
    for s in [T / 2, T - 1, T] {
        let derived = peaks(s, &records);
        let run = ran(Some(s), &records);
        assert!(
            run.entries
                .iter()
                .all(|e| e.end == End::Expected && !e.fault),
            "s = {s}"
        );
        assert_eq!(
            (run.held as u64, run.pending as u64),
            (derived.held, derived.pending),
            "s = {s}: held and pending against the bound with timeouts of s"
        );
        assert!(
            run.held as u64 <= own.held && run.pending as u64 <= own.pending,
            "s = {s}"
        );
        let last = run.entries.iter().map(|e| e.ended).max().unwrap();
        assert_eq!(last, derived.horizon, "s = {s}: the last end");
    }
    // The chains are what the bound is for: a count by rate alone, the steps
    // planned within one timeout, is less.
    let rate_alone = records
        .iter()
        .map(|r| {
            records
                .iter()
                .filter(|o| o.at <= r.at && o.at + T + L >= r.at)
                .count()
        })
        .max()
        .unwrap() as u64;
    eprintln!("the plan's bound: {own:?}; by rate alone {rate_alone}");
    assert!(
        own.held > rate_alone,
        "{} held, {rate_alone} by rate alone",
        own.held
    );
}

/// A stand that never answers: every step unknown at its deadline, each
/// settled dependent going on, the expected ones skipped; the machine holds no
/// more than the bound, and the last end is no later than its horizon.
#[test]
fn a_stand_that_never_answers_ends_within_the_bound() {
    let settled: Vec<Record> = (0..4u32)
        .map(|j| {
            let r = step(id("device", 0, "chain", j), u64::from(j) * 3_107);
            if j > 0 {
                after(r, &id("device", 0, "chain", j - 1), On::Settled)
            } else {
                r
            }
        })
        .collect();
    let own = peaks(T, &settled);
    let run = ran(None, &settled);
    assert!(
        run.entries
            .iter()
            .all(|e| e.end == End::Unknown && !e.fault)
    );
    assert!(run.held as u64 <= own.held);
    assert_eq!(
        run.entries.iter().map(|e| e.ended).max().unwrap(),
        own.horizon
    );
}

/// The bound assumes every line read by its plan (A2): a reader late past a
/// step's instant releases it later than the bound's worst.
#[test]
fn a_reader_late_past_the_plan_breaks_the_bound() {
    let records = vec![
        step(id("device", 0, "chain", 0), 0),
        step(id("device", 1, "chain", 0), 1_000),
    ];
    let report = run_bounded(plan_header(T), &records, &NoHost, 1_000);
    let end = report.1[0][2];
    let late = ascetic_ddd_scheduler::sim::run_reading(
        plan_header(T),
        &records,
        L,
        &|line| if line == 2 { 7_000 } else { 0 },
        &stalled(None),
    )
    .unwrap();
    assert!(
        late.entries[0].ended > end,
        "{} against {end}",
        late.entries[0].ended
    );
}

/// `fits`: at its budget a plan fits, a byte short it does not, and a
/// read-ahead past the header's is refused whatever the budget.
#[test]
fn fits_refuses_at_and_past_each_boundary() {
    let peaks = Peaks {
        held: 1_000,
        pending: 200,
        barriers: 30,
        ..Peaks::default()
    };
    let h = plan_header(T);
    let line = 300;
    let need = needed(&peaks, line);
    assert_eq!(fits(&peaks, &h, L, &Budget { bytes: need, line }), Ok(need));
    assert!(matches!(
        fits(&peaks, &h, L, &Budget { bytes: need - 1, line }),
        Err(Refusal::Bytes { needed, budget, .. }) if needed == need && budget == need - 1
    ));
    assert_eq!(
        fits(
            &peaks,
            &h,
            L + 1,
            &Budget {
                bytes: u64::MAX,
                line
            }
        ),
        Err(Refusal::ReadAhead {
            asked: L + 1,
            planned: L
        })
    );
    // Each part counts.
    for more in [
        Peaks {
            held: 1_001,
            ..peaks
        },
        Peaks {
            pending: 201,
            ..peaks
        },
        Peaks {
            barriers: 31,
            ..peaks
        },
    ] {
        assert!(needed(&more, line) > need, "{more:?}");
    }
}

/// A reader lets go of its producer when its release is decided, not when it
/// is dispatched (decision 5's value "taken at release"): a wake taken late
/// holds nothing. P answers at 100 µs; its reader R, planned at 900, is
/// decided at its read at 600; Y, read at 800, moves the window past P, which
/// is freed then — not at 900 — so at most two steps are held at once.
#[test]
fn a_reader_lets_go_of_its_producer_when_its_release_is_decided() {
    let mut h = Header {
        window: 1_000,
        read_ahead: 300,
        ..header()
    };
    for o in &mut h.operations {
        o.timeout = T;
    }
    for c in &mut h.calls {
        c.timeout = T;
    }
    let w = captures_etag(
        Record {
            action: rest(
                "update",
                vec![ascetic_ddd_command_log::record::Value::Literal("1".into())],
            ),
            ..step(id("group", 1, "update", 0), 0)
        },
        1,
    );
    let r = after(
        Record {
            action: read_etag("read", 1, &w.step),
            ..step(id("group", 1, "read", 0), 900)
        },
        &w.step,
        On::Expected,
    );
    let y = step(id("device", 9, "later", 0), 1_100);
    let records = vec![w, r, y];
    let report = run_with(h.clone(), &records, &NoHost, 1_000);
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    let script = |line: u64, _: u32, r: &Record| Attempt {
        sent_after: 0,
        answer_after: Some(if line == 2 { 100 } else { 1_000 }),
        judgement: Judgement::Expected(
            r.capture
                .iter()
                .map(|c| ascetic_ddd_scheduler::machine::Captured {
                    subject: c.subject.clone(),
                    slot: c.slot.clone(),
                    value: b"1".to_vec(),
                })
                .collect(),
        ),
    };
    let ran = run(h, &records, 300, &script).unwrap();
    assert!(ran.entries.iter().all(|e| e.end == End::Expected));
    assert_eq!(ran.held, 2, "the producer held past its reader's decision");
}

/// The TLA+ model's bound instance (`verify/tla/MCScheduler.tla`, `Bnd`) as a
/// command log: the validator's bound has the numbers the model pins in
/// `BndTables` — so the model's tables are the validator's, not a copy of a
/// sentence.
#[test]
fn the_model_s_bound_instance_has_the_validator_s_numbers() {
    use ascetic_ddd_command_log::record::{Action, Call, Safety, Value};
    let mut h = Header {
        window: 1,
        read_ahead: 1,
        ..header()
    };
    for o in &mut h.operations {
        o.timeout = 1;
    }
    h.calls = vec![
        Call {
            name: "t3".into(),
            timeout: 3,
            safety: Safety::Never,
        },
        Call {
            name: "t1".into(),
            timeout: 1,
            safety: Safety::Never,
        },
    ];
    let call = |r: Record, op: &str| {
        let mut r = r;
        if let Action::Host(c) = &mut r.action {
            c.op = op.into();
        }
        r
    };
    let w = captures_etag(
        Record {
            action: rest("update", vec![Value::Literal("1".into())]),
            ..step(id("group", 1, "update", 0), 0)
        },
        1,
    );
    let first = call(step(id("device", 0, "a", 0), 0), "t3");
    // The reader also awaits the long first line: its release decided as late
    // as 1's end, it pins 2 until then.
    let reader = after(
        after(
            Record {
                action: read_etag("read", 1, &w.step),
                ..step(id("group", 1, "read", 0), 1)
            },
            &w.step,
            On::Expected,
        ),
        &first.step,
        On::Settled,
    );
    let records = vec![
        first,
        w,
        call(step(id("device", 1, "a", 0), 1), "t1"),
        reader,
        call(step(id("device", 5, "x", 0), 4), "t1"),
        call(step(id("device", 6, "y", 0), 5), "t1"),
    ];
    let (report, bounds) = run_bounded(h, &records, &NoHost, 1_000);
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    // [line, R̂, Ê, held, pending], the header being line 1.
    assert_eq!(
        bounds,
        vec![
            [2, 0, 3, 1, 1],
            [3, 0, 1, 2, 2],
            [4, 1, 2, 3, 3],
            [5, 3, 4, 4, 4],
            [6, 4, 5, 5, 3],
            [7, 5, 6, 3, 3],
        ]
    );
    assert_eq!(
        (
            report.peaks.held,
            report.peaks.pending,
            report.peaks.horizon
        ),
        (5, 4, 6)
    );
}
