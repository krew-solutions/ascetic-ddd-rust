//! K1b of ADR-0047: the machine against the scheduler by definition
//! (`vectors/schedule.py`) on every clean corpus log, under scripts of mixed
//! latencies and judgements, at three read-aheads.

// The command log's fixture reads its corpus's directory; the lint gate refuses
// files to the machine, not to its tests (ADR-0047 K1e-3).
#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use std::collections::BTreeMap;

use ascetic_ddd_command_log::record::{Header, Record};
use ascetic_ddd_command_log::validate::NoHost;
use ascetic_ddd_faker::{Key, Seed};
use ascetic_ddd_scheduler::journal::{self, JOURNAL_VERSION, Judged};
use ascetic_ddd_scheduler::machine::{
    Captured, Critical, End, Entry, Input, Judgement, MACHINE_REVISION, Skip,
};
use ascetic_ddd_scheduler::sim::{Attempt, Run, run_reading};
use ascetic_ddd_scheduler::verdict::{FOLD_REVISION, Log, fold};
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
            // An expected answer without the values it was to capture: its
            // readers are skipped, and it is their root (K1e-3c-2).
            _ if !r.capture.is_empty() && draw("bare", 6) == 0 => Judgement::Expected(vec![]),
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

/// What the machine decided in the differential's runs, their journals'
/// digests by journal version and machine revision (ADR-0047 K1e-3): a machine
/// that decides otherwise raises its revision, a journal written otherwise its
/// version. A corpus or a script that changes changes the digest with
/// neither: re-pin it then, as revision 6's MEANINGS was (K1e-3c-2's bare
/// answers re-pinned (2, 1)).
const JOURNALS: &[(u32, u32, &str)] = &[
    (
        1,
        1,
        "5f3f0a6c485c77c6c3c7ca916afda5f0bac9ccd76cd1363400e7a908a58bab91",
    ),
    (
        2,
        1,
        "adc7ab97553a58621e21f77d3c2fcf03763208ea069bb9073133604e4c2aa00e",
    ),
];

/// What the fold decided of the differential's runs, the digest of their
/// verdicts and classes, by journal version, machine revision and fold
/// revision (ADR-0047 K1e-3c-2): a fold that decides otherwise raises its
/// revision.
const CLASSES: &[(u32, u32, u32, &str)] = &[(
    2,
    1,
    1,
    "2124597f7d52665afb12cf13b908b35e7ea8908ca31dfba5d3ee07e8bf13935a",
)];

/// The journal written, read back, and told against the run it journals
/// (ADR-0047 K1e-3b): every end and dispatch, every send and answer in the
/// order the machine took them, the bindings of every read, the releases.
fn journal_of(name: &str, header: journal::Header, ran: &Run, records: &[Record]) -> Vec<u8> {
    use journal::Line;
    let mut writer = journal::Writer::new(Vec::new(), header.clone()).unwrap();
    for l in &ran.journal {
        writer.line(l).unwrap();
    }
    let bytes = writer.finish().unwrap().0;
    let reader = journal::Reader::new(std::io::Cursor::new(bytes.clone())).unwrap();
    assert_eq!(reader.header(), &header, "{name}");
    let lines: Vec<Line> = reader.collect::<Result<_, _>>().unwrap();
    assert_eq!(lines, ran.journal, "{name}: read back");
    let mut reads = Vec::new();
    let mut released = BTreeMap::new();
    let mut ends = BTreeMap::new();
    let mut dispatches = Vec::new();
    let mut taken = Vec::new();
    let mut faults = Vec::new();
    for l in &lines {
        match l {
            Line::Read(r) => {
                reads.push(r.line);
                // Its action as the log names it; an observation's cause the
                // step its log names, read before it, or itself.
                let record = &records[(r.line - 2) as usize];
                use ascetic_ddd_command_log::record::Action as Act;
                let action = match &record.action {
                    Act::Rest(_) => journal::Action::Rest,
                    Act::Bus(_) => journal::Action::Bus,
                    Act::Observe(_) => journal::Action::Observe,
                    Act::Host(_) => journal::Action::Host,
                };
                assert_eq!(r.action, action, "{name} line {}", r.line);
                match (&record.action, r.cause) {
                    (ascetic_ddd_command_log::record::Action::Observe(o), Some(c)) => {
                        assert!(c <= r.line, "{name} line {}: a cause read later", r.line);
                        assert_eq!(
                            records[(c - 2) as usize].step,
                            o.cause,
                            "{name} line {}: cause",
                            r.line
                        );
                    }
                    (ascetic_ddd_command_log::record::Action::Observe(_), None) => {
                        panic!("{name} line {}: an observation without its cause", r.line)
                    }
                    (_, cause) => assert_eq!(cause, None, "{name} line {}", r.line),
                }
                let bound: Vec<u64> = r
                    .after
                    .iter()
                    .filter_map(|d| match d {
                        journal::Dep::Step { line, .. } => Some(*line),
                        journal::Dep::Each { .. } => None,
                    })
                    .collect();
                taken.push((r.at, r.line, 0, None));
                released.entry(r.line).or_insert((bound, None));
            }
            Line::Released(r) => {
                let slot = released.get_mut(&r.line).expect("released before read");
                assert!(slot.1.is_none(), "{name}: line {} released twice", r.line);
                slot.1 = Some((r.intended, r.released, r.at));
            }
            Line::Dispatch(d) => dispatches.push((d.at, d.line, d.attempt)),
            Line::Sent(t) => taken.push((t.at, t.line, t.attempt, None)),
            Line::Answer(a) => taken.push((a.at, a.line, a.attempt, Some(a.judged))),
            Line::End(e) => {
                assert!(
                    ends.insert(e.line, e.clone()).is_none(),
                    "{name}: line {} ended twice",
                    e.line
                );
            }
            Line::Fault(f) => faults.push(f.line),
            Line::Header(_) | Line::Trailer(_) => panic!("{name}: a header or trailer amid"),
        }
    }
    // Reads in line order: the fold's second pass depends on it.
    assert!(reads.windows(2).all(|w| w[0] < w[1]), "{name}: reads");
    assert_eq!(ends.len(), ran.entries.len(), "{name}: ends");
    for e in &ran.entries {
        let j = &ends[&e.line];
        assert!(
            j.at >= e.ended,
            "{name} line {}: decided before its end",
            e.line
        );
        assert_eq!(
            **j,
            journal::Ended {
                at: j.at,
                ..journal_entry(e)
            },
            "{name}"
        );
        let (bound, rel) = &released[&e.line];
        assert_eq!(bound, &e.bound, "{name} line {}: bound", e.line);
        match rel {
            Some((intended, r, _)) => {
                assert_eq!(
                    (*intended, *r),
                    (e.intended, e.released),
                    "{name} line {}",
                    e.line
                )
            }
            None => assert!(
                e.dispatched.is_none(),
                "{name} line {}: dispatched unreleased",
                e.line
            ),
        }
    }
    let ran_dispatches: Vec<_> = ran.dispatches.iter().map(|d| (d.0, d.1, d.2)).collect();
    assert_eq!(dispatches, ran_dispatches, "{name}: dispatches");
    let ran_taken: Vec<_> = ran
        .inputs
        .iter()
        .map(|(at, input)| match input {
            Input::Line { line, .. } => (*at, *line, 0, None),
            Input::Sent { line, attempt } => (*at, *line, *attempt, None),
            Input::Ended {
                line,
                attempt,
                judgement,
            } => (
                *at,
                *line,
                *attempt,
                Some(match judgement {
                    Judgement::Expected(_) => Judged::Expected,
                    Judgement::Unexpected => Judged::Unexpected,
                    Judgement::NotTaken => Judged::NotTaken,
                    Judgement::Unknown => Judged::Unknown,
                }),
            ),
            Input::Wake => unreachable!("a run keeps no wake"),
        })
        .collect();
    assert_eq!(taken, ran_taken, "{name}: inputs");
    let ran_faults: Vec<u64> = ran
        .entries
        .iter()
        .filter(|e| e.fault)
        .map(|e| e.line)
        .collect();
    let mut sorted = faults.clone();
    sorted.sort_unstable();
    assert_eq!(sorted, ran_faults, "{name}: faults");
    bytes
}

fn journal_entry(e: &Entry) -> journal::Ended {
    let skip = |s: &Skip| match s {
        Skip::Dependency(l) => journal::Skip::Dependency(*l),
        Skip::Member(l) => journal::Skip::Member(*l),
        Skip::MissingCapture(l) => journal::Skip::MissingCapture(*l),
    };
    journal::Ended {
        at: 0,
        line: e.line,
        end: match &e.end {
            End::Expected => journal::End::Expected,
            End::Unexpected => journal::End::Unexpected,
            End::Unknown => journal::End::Unknown,
            End::Skipped(s) => journal::End::Skipped(skip(s)),
        },
        read: e.read,
        ready: e.ready,
        intended: e.intended,
        released: e.released,
        dispatched: e.dispatched,
        sent: e.sent,
        ended: e.ended,
        attempts: e.attempts,
        critical: e.critical.map(|c| match c {
            Critical::Step(l) => journal::Critical::Step(l),
            Critical::Member(l) => journal::Critical::Member(l),
        }),
        bound: e.bound.clone(),
        fault: e.fault,
    }
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
    let mut journals = <sha2::Sha256 as sha2::Digest>::new();
    let mut classes = <sha2::Sha256 as sha2::Digest>::new();
    // The verdict by definition, beside the scheduler by definition.
    let verdict_py = concat!(env!("CARGO_MANIFEST_DIR"), "/vectors/verdict.py");
    let mut verdicts = Command::new("python3")
        .args(["-I", verdict_py])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("python3, which the library's gate requires");
    let mut verdicts_in = verdicts.stdin.take().unwrap();
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
            let identity = serde_json::from_str::<Json>(log.lines().last().unwrap()).unwrap()
                ["trailer"]["sha256"]
                .as_str()
                .unwrap()
                .to_owned();
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
                    let run_name = format!("{name}/{seed}/{read_ahead}");
                    let journal_header = journal::Header {
                        journal: JOURNAL_VERSION,
                        machine: MACHINE_REVISION,
                        source: "differential".to_owned(),
                        log: identity.clone(),
                        validator: ascetic_ddd_command_log::live::validator(),
                        read_ahead,
                        jitter: header.profile.jitter,
                    };
                    let journal_bytes =
                        journal_of(&run_name, journal_header.clone(), &ran, &records);
                    sha2::Digest::update(&mut journals, &journal_bytes);
                    let open = || {
                        journal::Reader::new(std::io::Cursor::new(journal_bytes.clone())).unwrap()
                    };
                    let log_id = Log {
                        sha256: identity.clone(),
                        records: records.len() as u64,
                    };
                    let folded = fold(&journal_header, &log_id, open(), open());
                    let names: Vec<&str> = folded.classes.iter().map(|c| c.name()).collect();
                    sha2::Digest::update(
                        &mut classes,
                        format!("{} {}\n", folded.verdict.name(), names.join(" ")),
                    );
                    let parsed: Vec<Json> = std::str::from_utf8(&journal_bytes)
                        .unwrap()
                        .lines()
                        .map(|l| serde_json::from_str(l).unwrap())
                        .collect();
                    let case = json!({
                        "name": run_name,
                        "log": {"sha256": log_id.sha256, "records": log_id.records},
                        "journal": parsed,
                        "verdict": folded.verdict.name(),
                        "classes": names,
                    });
                    // The definition stops at its first difference: its
                    // reason, not a broken pipe, is the failure.
                    let _ = writeln!(verdicts_in, "{case}");
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
    drop(verdicts_in);
    let verdicts_out = verdicts.wait_with_output().unwrap();
    let reached = String::from_utf8_lossy(&verdicts_out.stdout).into_owned();
    eprintln!("{reached}");
    assert!(
        verdicts_out.status.success(),
        "{reached}{}",
        String::from_utf8_lossy(&verdicts_out.stderr)
    );
    let out = child.wait_with_output().unwrap();
    eprintln!("{}", String::from_utf8_lossy(&out.stdout));
    assert!(
        out.status.success(),
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(runs > 500, "{runs} runs");
    let digest: String = sha2::Digest::finalize(journals)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    // What the differential's verdicts reach (the definition counts them).
    for counter in [
        "conditional_doubtful",
        "conditional_transitive",
        "conditional_and_expected",
        "conditional_ended_before_its_dependency",
        "undecided_call",
        "undecided_cause",
        "undecided_read",
        "producer_root",
        "root",
        "consequent",
        "inconclusive",
        "passed",
        "pass",
        "fail",
        "void:undecided",
    ] {
        assert!(
            reached.contains(&format!("'{counter}'")),
            "{counter}: {reached}"
        );
    }
    let classes: String = sha2::Digest::finalize(classes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(
        CLASSES.last().copied(),
        Some((
            JOURNAL_VERSION,
            MACHINE_REVISION,
            FOLD_REVISION,
            classes.as_str()
        )),
        "the verdicts changed: bump FOLD_REVISION if the fold decides otherwise, and append their digest"
    );
    let revisions: Vec<(u32, u32)> = JOURNALS.iter().map(|m| (m.0, m.1)).collect();
    assert!(revisions.windows(2).all(|w| w[0] < w[1]));
    assert_eq!(
        JOURNALS.last().copied(),
        Some((JOURNAL_VERSION, MACHINE_REVISION, digest.as_str())),
        "the journals changed: bump MACHINE_REVISION if the machine decides otherwise, \
         JOURNAL_VERSION if a journal is written otherwise, and append their digest"
    );
    eprintln!(
        "the bound: {within} runs within A2, {lagged} of them lagging; {attained} lines ended at it"
    );
    assert!(
        within > 0 && lagged > 0 && attained > 0,
        "{within} {lagged} {attained}"
    );
}
