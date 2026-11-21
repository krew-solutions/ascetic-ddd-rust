//! K1b of ADR-0047: the machine against the scheduler by definition
//! (`vectors/schedule.py`) on every clean corpus log, under scripts of mixed
//! latencies and judgements, at three read-aheads.

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports)]
mod fixture;

use std::collections::BTreeMap;

use ascetic_ddd_command_log::record::Record;
use ascetic_ddd_command_log::validate::NoHost;
use ascetic_ddd_faker::{Key, Seed};
use ascetic_ddd_scheduler::machine::{Captured, Critical, End, Entry, Judgement, Skip};
use ascetic_ddd_scheduler::sim::{Attempt, run_reading};
use fixture::*;
use serde_json::{Value as Json, json};

const ATTEMPTS: u32 = 16;

/// A script drawn from a key of (seed, line, attempt): mostly expected, some
/// refusals of every kind, some answers never coming.
fn script(seed: u64) -> impl Fn(u64, u32, &Record) -> Attempt {
    move |line, attempt, r| {
        let k = Key::root(Seed(seed)).at(line).at(u64::from(attempt));
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
    {
        let mut stdin = child.stdin.take().unwrap();
        for (name, records) in fixture::corpus::logs() {
            let report = run_with(fixture::corpus::corpus_header(), &records, &NoHost, 1_000);
            if !report.violations.is_empty() {
                continue;
            }
            let log = String::from_utf8(bytes(fixture::corpus::corpus_header(), &records)).unwrap();
            for seed in [1u64, 2, 3] {
                let s = script(seed);
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
                    let ran = run_reading(
                        fixture::corpus::corpus_header(),
                        &records,
                        read_ahead,
                        &lag,
                        &s,
                    )
                    .unwrap();
                    assert!(
                        ran.entries.iter().all(|e| e.attempts <= ATTEMPTS),
                        "{name}: more attempts than the table holds"
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
}
