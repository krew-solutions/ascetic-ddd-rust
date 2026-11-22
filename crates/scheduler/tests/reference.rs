//! K1b of ADR-0047: the machine against the scheduler by definition
//! (`vectors/schedule.py`) on every clean corpus log, under scripts of mixed
//! latencies and judgements, at three read-aheads.

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports)]
mod fixture;

use std::collections::BTreeMap;

use ascetic_ddd_command_log::record::{Header, Record};
use ascetic_ddd_command_log::validate::NoHost;
use ascetic_ddd_faker::{Key, Seed};
use ascetic_ddd_scheduler::machine::{Captured, Critical, End, Entry, Judgement, Skip};
use ascetic_ddd_scheduler::sim::{Attempt, run_reading};
use fixture::*;
use serde_json::{Value as Json, json};

const ATTEMPTS: u32 = 16;

/// A script drawn from a key of (seed, line, attempt): mostly expected, some
/// refusals of every kind, some answers never coming.
fn script(seed: u64, log: &str) -> impl Fn(u64, u32, &Record) -> Attempt + use<> {
    // Each log draws its own: lines of one number fare differently across logs.
    let root = Key::root(Seed(seed)).child(log);
    move |line, attempt, r| {
        let k = root.at(line).at(u64::from(attempt));
        let draw = |label: &str, m: u64| k.child(label).unit().raw() % m;
        let judgement = match draw("judgement", 20) {
            0 => Judgement::Unexpected,
            1 => Judgement::NotTaken,
            2 => Judgement::Unknown,
            _ => Judgement::Expected(
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
        Attempt {
            sent_after: draw("sent", 300),
            answer_after: (draw("silent", 30) != 0).then(|| draw("answer", 3_000)),
            judgement,
        }
    }
}

/// What a run's handlers do, attempt by attempt.
type Script = dyn Fn(u64, u32, &Record) -> Attempt;

/// Every attempt sent at once and answered as expected exactly at its
/// deadline — an observation's past it, unknown there: the plan's own bound's
/// schedule (ADR-0047 K1e-2).
fn at_deadline(header: Header) -> impl Fn(u64, u32, &Record) -> Attempt {
    move |line, _, r| Attempt {
        sent_after: 0,
        answer_after: Some(ascetic_ddd_command_log::plan::timeout(&header, &r.action)),
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
    }
}

fn judgement_json(j: &Judgement) -> &'static str {
    match j {
        Judgement::Expected(_) => "expected",
        Judgement::Unexpected => "unexpected",
        Judgement::NotTaken => "not_taken",
        Judgement::Unknown => "unknown",
    }
}

fn end_json(e: &End) -> Json {
    match e {
        End::Expected => json!("expected"),
        End::Unexpected => json!("unexpected"),
        End::Unknown => json!("unknown"),
        End::Skipped(Skip::Dependency(l)) => json!(["skipped", ["dependency", l]]),
        End::Skipped(Skip::Member(l)) => json!(["skipped", ["member", l]]),
        End::Skipped(Skip::MissingCapture(l)) => json!(["skipped", ["missing_capture", l]]),
    }
}

fn entry_json(e: &Entry) -> Json {
    let critical = match e.critical {
        Some(Critical::Step(l)) => json!(["step", l]),
        Some(Critical::Member(l)) => json!(["member", l]),
        None => Json::Null,
    };
    // A skipped line's critical dependency is not compared: the definition
    // counts every dependency, the machine those met before the skip.
    let critical = if matches!(e.end, End::Skipped(_)) {
        Json::Null
    } else {
        critical
    };
    json!({
        "line": e.line,
        "end": end_json(&e.end),
        "read": e.read,
        "intended": e.intended,
        "released": e.released,
        "dispatched": e.dispatched,
        "sent": e.sent,
        "ended": e.ended,
        "attempts": e.attempts,
        "bound": e.bound,
        "critical": critical,
    })
}

#[test]
fn the_machine_is_the_scheduler_by_definition() {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let script_py = concat!(env!("CARGO_MANIFEST_DIR"), "/vectors/schedule.py");
    let mut child = Command::new("python3")
        .args(["-I", script_py])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("python3, which the library's gate requires");
    let mut runs = 0;
    // The plan's own bound against the machine (ADR-0047 K1e-2): the runs
    // whose every read lies within [at − L, at] (A2), those of them with a
    // reader lagging, and the lines that ended exactly at their worst end.
    let (mut within, mut lagged, mut attained) = (0u64, 0u64, 0u64);
    {
        let mut stdin = child.stdin.take().unwrap();
        let mut cases = Vec::new();
        for (name, records) in fixture::corpus::logs() {
            // The base logs also under timeouts on the window's scale, where
            // the bound's terms turn within a log.
            if name.matches('-').count() == 1 {
                cases.push((
                    format!("{name}/short"),
                    fixture::corpus::short_header(),
                    records.clone(),
                ));
            }
            cases.push((name, fixture::corpus::corpus_header(), records));
        }
        for (name, header, records) in cases {
            let (report, bounds) = run_bounded(header.clone(), &records, &NoHost, 1_000);
            if !report.violations.is_empty() {
                continue;
            }
            let peaks = report.peaks;
            let log = String::from_utf8(bytes(header.clone(), &records)).unwrap();
            let scripts: Vec<(u64, Box<Script>)> = vec![
                (1, Box::new(script(1, &name))),
                (2, Box::new(script(2, &name))),
                (3, Box::new(script(3, &name))),
                (0, Box::new(at_deadline(header.clone()))),
            ];
            for (seed, s) in scripts {
                let mut table = BTreeMap::new();
                for (i, r) in records.iter().enumerate() {
                    let line = i as u64 + 2;
                    for attempt in 1..=ATTEMPTS {
                        let a = s(line, attempt, r);
                        let captures: Vec<Json> = match &a.judgement {
                            Judgement::Expected(c) => c
                                .iter()
                                .map(|c| {
                                    json!({
                                        "subject": serde_json::to_value(&c.subject).unwrap(),
                                        "slot": c.slot,
                                    })
                                })
                                .collect(),
                            _ => vec![],
                        };
                        table.insert(
                            format!("{line}/{attempt}"),
                            json!({
                                "sent_after": a.sent_after,
                                "answer_after": a.answer_after,
                                "judgement": judgement_json(&a.judgement),
                                "captures": captures,
                            }),
                        );
                    }
                }
                // A reader that lags on some lines, by up to 2 ms.
                let lag = move |line: u64| {
                    let k = || Key::root(Seed(seed)).child("lag").at(line);
                    if k().unit().raw() % 4 == 0 {
                        k().child("by").unit().raw() % 2_000
                    } else {
                        0
                    }
                };
                for read_ahead in [0u64, W / 2, 1 << 40] {
                    let ran = run_reading(header.clone(), &records, read_ahead, &lag, &s).unwrap();
                    // No step released or ended before it was read.
                    for e in &ran.entries {
                        assert!(
                            e.released >= e.read && e.ended >= e.read,
                            "{name}/{seed}/{read_ahead} line {}: read at {}, released at {}, ended at {}",
                            e.line,
                            e.read,
                            e.released,
                            e.ended
                        );
                    }
                    // The machine keeps the barrier keys the validator counted.
                    assert_eq!(
                        ran.barriers as u64, peaks.barriers,
                        "{name}/{seed}/{read_ahead}: barriers"
                    );
                    let l = header.read_ahead;
                    let at = |line: u64| records[(line - 2) as usize].at;
                    let a2 = read_ahead <= l
                        && ran.entries.iter().all(|e| {
                            e.read.saturating_add(l) >= at(e.line) && e.read <= at(e.line)
                        });
                    if a2 {
                        within += 1;
                        if ran
                            .entries
                            .iter()
                            .any(|e| e.read.saturating_add(read_ahead) > at(e.line))
                        {
                            lagged += 1;
                        }
                        for e in &ran.entries {
                            let [_, intended, end, _, _] = bounds[(e.line - 2) as usize];
                            assert!(
                                e.released <= intended && e.ended <= end,
                                "{name}/{seed}/{read_ahead} line {}: released {} and ended {}, past the bound's {intended} and {end}",
                                e.line,
                                e.released,
                                e.ended
                            );
                            if e.ended == end {
                                attained += 1;
                            }
                        }
                        assert!(
                            ran.held as u64 <= peaks.held && ran.pending as u64 <= peaks.pending,
                            "{name}/{seed}/{read_ahead}: {} held and {} pending past the bound's {} and {}",
                            ran.held,
                            ran.pending,
                            peaks.held,
                            peaks.pending
                        );
                    }
                    assert!(
                        ran.entries.iter().all(|e| e.attempts <= ATTEMPTS),
                        "{name}: more attempts than the table holds"
                    );
                    // Every cell the window let go is freed by the end.
                    assert_eq!(
                        ran.left, ran.kept,
                        "{name}/{seed}/{read_ahead}: cells left behind"
                    );
                    let journal: Vec<Json> = ran.entries.iter().map(entry_json).collect();
                    let line = json!({
                        "name": format!("{name}/{seed}/{read_ahead}"),
                        "log": log,
                        "read_ahead": read_ahead,
                        "reads": ran
                            .entries
                            .iter()
                            .map(|e| (e.line.to_string(), json!(e.read)))
                            .collect::<serde_json::Map<_, _>>(),
                        "script": table,
                        "journal": journal,
                    });
                    // The reference stops at its first difference: its
                    // reason, not a broken pipe, is the failure.
                    if writeln!(stdin, "{line}").is_err() {
                        break;
                    }
                    runs += 1;
                }
            }
        }
    }
    let out = child.wait_with_output().unwrap();
    eprintln!("{}", String::from_utf8_lossy(&out.stdout));
    assert!(
        out.status.success(),
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(runs > 500, "{runs} runs");
    eprintln!(
        "the bound: {within} runs within A2, {lagged} of them lagging; {attained} lines ended at it"
    );
    assert!(
        within > 0 && lagged > 0 && attained > 0,
        "{within} {lagged} {attained}"
    );
}
