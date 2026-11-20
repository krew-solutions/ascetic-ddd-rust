//! V1 of ADR-0046: for every rule, a minimal log that breaks it beside its
//! twin that keeps it.

mod fixture;

use ascetic_ddd_command_log::record::{
    Codec, Dep, Form, Hole, Of, On, Record, Rendering, StepId, Value,
};
use ascetic_ddd_command_log::validate::{Host, NoHost, Rule};
use fixture::*;

/// The twin keeps every rule; the other breaks exactly `rule`.
fn twins(rule: Rule, keeps: &[Record], breaks: &[Record]) {
    let kept = run(keeps);
    assert!(
        kept.violations.is_empty(),
        "{rule}: the twin breaks {:?}",
        kept.violations
    );
    let broken = run(breaks);
    assert_eq!(rules(&broken), vec![rule], "{:?}", broken.violations);
}

#[test]
fn order() {
    let (a, b) = (id("device", 1, "seen", 0), id("device", 2, "seen", 0));
    twins(
        Rule::Order,
        &[step(a.clone(), 10), step(b.clone(), 20)],
        &[step(b, 20), step(a, 10)],
    );
    // Equal times are ordered by the step id.
    let (a, b) = (id("device", 1, "seen", 0), id("device", 2, "seen", 0));
    twins(
        Rule::Order,
        &[step(a.clone(), 10), step(b.clone(), 10)],
        &[step(b, 10), step(a, 10)],
    );
}

#[test]
fn duplicate() {
    let a = id("device", 1, "seen", 0);
    twins(
        Rule::Duplicate,
        &[step(a.clone(), 10), step(id("device", 1, "seen", 1), 20)],
        &[step(a.clone(), 10), step(a, 20)],
    );
}

#[test]
fn edge() {
    let (a, b) = (id("device", 1, "seen", 0), id("device", 2, "seen", 0));
    let unknown = id("device", 9, "seen", 0);
    twins(
        Rule::Edge,
        &[
            step(a.clone(), 10),
            after(step(b.clone(), 20), &a, On::Expected),
        ],
        &[
            step(a.clone(), 10),
            after(step(b.clone(), 20), &unknown, On::Expected),
        ],
    );
    // Not before in planned time.
    twins(
        Rule::Edge,
        &[
            step(a.clone(), 10),
            after(step(b.clone(), 20), &a, On::Sent),
        ],
        &[
            step(a.clone(), 10),
            after(step(b.clone(), 10), &a, On::Sent),
        ],
    );
    // Older than W, unless retained.
    let created = id("group", 1, "created", 0);
    twins(
        Rule::Edge,
        &[
            step(created.clone(), 10),
            after(step(b.clone(), 10 + W + 1), &created, On::Expected),
        ],
        &[
            step(a.clone(), 10),
            after(step(b.clone(), 10 + W + 1), &a, On::Expected),
        ],
    );
    twins(
        Rule::Edge,
        &[
            step(a.clone(), 10),
            after(step(b.clone(), 10 + W), &a, On::Expected),
        ],
        &[
            step(a.clone(), 10),
            after(step(b, 10 + W + 1), &a, On::Expected),
        ],
    );
}

#[test]
fn barrier() {
    let members = [
        step(id("device", 1, "managed", 0), 10),
        step(id("device", 2, "managed", 0), 11),
    ];
    let holder = |count| {
        let mut r = step(id("tenant", 1, "report", 0), 20);
        r.after.push(Dep::Each {
            kind: "device".into(),
            scope: vec![7],
            label: "managed".into(),
            n: 0,
            on: On::Settled,
            count: Some(count),
        });
        r
    };
    let with = |count| {
        let mut v = members.to_vec();
        v.push(holder(count));
        v
    };
    twins(Rule::Barrier, &with(2), &with(3));
}

fn writer(g: u64, n: u32, at: u64) -> Record {
    let mut r = step(id("group", g, "update", n), at);
    r.action = rest("update", vec![Value::Literal(g.to_string())]);
    r
}

fn reader(g: u64, n: u32, at: u64, from: &StepId, on: On) -> Record {
    let mut r = after(step(id("group", g, "read", n), at), from, on);
    r.action = read_etag("read", g, from);
    r
}

#[test]
fn producer() {
    let w = writer(1, 0, 10);
    twins(
        Rule::Producer,
        &[w.clone(), reader(1, 0, 20, &w.step, On::Expected)],
        &[w.clone(), reader(1, 0, 20, &w.step, On::Sent)],
    );
}

#[test]
fn stale() {
    let w0 = writer(1, 0, 10);
    let w1 = after(writer(1, 1, 20), &w0.step, On::Expected);
    twins(
        Rule::Stale,
        &[
            w0.clone(),
            w1.clone(),
            reader(1, 0, 30, &w1.step, On::Expected),
        ],
        &[
            w0.clone(),
            w1.clone(),
            after(
                reader(1, 0, 30, &w0.step, On::Expected),
                &w1.step,
                On::Expected,
            ),
        ],
    );
    // A value no step of the slot wrote.
    let other = step(id("group", 1, "seen", 0), 5);
    twins(
        Rule::Stale,
        &[w0.clone(), reader(1, 0, 30, &w0.step, On::Expected)],
        &[other.clone(), reader(1, 0, 30, &other.step, On::Expected)],
    );
}

#[test]
fn writers() {
    let w0 = writer(1, 0, 10);
    let r0 = reader(1, 0, 20, &w0.step, On::Expected);
    let w1 = after(writer(1, 1, 30), &w0.step, On::Settled);
    twins(
        Rule::Writers,
        &[
            w0.clone(),
            r0.clone(),
            after(w1.clone(), &r0.step, On::Sent),
        ],
        &[w0.clone(), r0.clone(), w1.clone()],
    );
    // The previous writer's start is not its end.
    twins(
        Rule::Writers,
        &[w0.clone(), after(writer(1, 1, 30), &w0.step, On::Settled)],
        &[w0.clone(), after(writer(1, 1, 30), &w0.step, On::Sent)],
    );
    // A capture is a write, though its operation writes nothing.
    let c0 = captures_etag(step(id("group", 1, "listed", 0), 10), 1);
    let c1 = captures_etag(step(id("group", 1, "listed", 1), 20), 1);
    twins(
        Rule::Writers,
        &[c0.clone(), after(c1.clone(), &c0.step, On::Expected)],
        &[c0, c1],
    );
}

#[test]
fn stream() {
    let m = |n: u32, at: u64, position: u32| {
        message(step(id("device", 1, "fact", n), at), "telemetry", position)
    };
    let first = m(0, 10, 1);
    twins(
        Rule::Stream,
        &[first.clone(), after(m(1, 20, 5), &first.step, On::Expected)],
        &[first.clone(), after(m(1, 20, 1), &first.step, On::Expected)],
    );
    twins(
        Rule::Stream,
        &[first.clone(), after(m(1, 20, 2), &first.step, On::Expected)],
        &[first.clone(), after(m(1, 20, 2), &first.step, On::Settled)],
    );
    twins(Rule::Stream, &[m(0, 10, 1)], &[m(0, 10, 0)]);
    twins(
        Rule::Stream,
        &[m(0, 10, i32::MAX as u32)],
        &[m(0, 10, i32::MAX as u32 + 1)],
    );
}

#[test]
fn gap() {
    let m = |n: u32, at: u64, position: u32| {
        message(step(id("device", 1, "fact", n), at), "lifecycle", position)
    };
    let first = m(0, 10, 1);
    twins(
        Rule::Gap,
        &[first.clone(), after(m(1, 20, 2), &first.step, On::Expected)],
        &[first.clone(), after(m(1, 20, 3), &first.step, On::Expected)],
    );
    twins(Rule::Gap, &[m(0, 10, 1)], &[m(0, 10, 2)]);
}

#[test]
fn span() {
    let s = || step(id("device", 1, "stamped", 0), 10);
    let ok = with_payload(
        s(),
        Codec::Text,
        8,
        vec![(0, 4, Form::Raw), (4, 4, Form::Raw)],
    );
    twins(
        Rule::Span,
        std::slice::from_ref(&ok),
        &[with_payload(
            s(),
            Codec::Text,
            8,
            vec![(0, 4, Form::Raw), (3, 4, Form::Raw)],
        )],
    );
    twins(
        Rule::Span,
        std::slice::from_ref(&ok),
        &[with_payload(s(), Codec::Text, 8, vec![(5, 4, Form::Raw)])],
    );
    twins(
        Rule::Span,
        &[ok],
        &[with_payload(
            s(),
            Codec::Text,
            8,
            vec![(0, 4, Form::JsonToken)],
        )],
    );
}

#[test]
fn profile() {
    let rendered = |kind: &str| {
        let mut r = step(id("tenant", 1, "joined", 0), 10);
        r.action = rest(
            "read",
            vec![Value::Hole(Hole::Rendered {
                of: Of::Subject(subject(kind, 3)),
                role: "id".into(),
                rendering: Rendering::UuidText,
            })],
        );
        r
    };
    twins(Rule::Profile, &[rendered("device")], &[rendered("group")]);
}

#[test]
fn operation() {
    let call = |op: &str, path: Vec<Value>| {
        let mut r = step(id("group", 1, "seen", 0), 10);
        r.action = rest(op, path);
        r
    };
    let one = || vec![Value::Literal("1".into())];
    twins(
        Rule::Operation,
        &[call("read", one())],
        &[call("delete", one())],
    );
    twins(
        Rule::Operation,
        &[call("read", one())],
        &[call("read", vec![])],
    );
    twins(
        Rule::Operation,
        &[call("list", vec![])],
        &[call("list", one())],
    );
    let o = |every| observe(step(id("group", 1, "seen", 0), 10), every, 100);
    twins(Rule::Operation, &[o(10)], &[o(0)]);
}

#[test]
fn deviation() {
    let m = |n: u32, at: u64, position: u32| {
        message(step(id("device", 1, "fact", n), at), "lifecycle", position)
    };
    let first = m(0, 10, 1);
    let gap = after(m(1, 20, 3), &first.step, On::Expected);
    // A declared gap is exempt; a declared rule the step keeps, or no rule, is not.
    twins(
        Rule::Deviation,
        &[first.clone(), deviating(gap.clone(), "gap")],
        &[
            first.clone(),
            deviating(after(m(1, 20, 2), &first.step, On::Expected), "gap"),
        ],
    );
    let kept = after(m(1, 20, 2), &first.step, On::Expected);
    twins(
        Rule::Deviation,
        &[first.clone(), deviating(gap, "gap")],
        &[first.clone(), deviating(kept, "gaps")],
    );
    // A duplicate on purpose, decided at the end, is exempt too.
    let a = id("device", 1, "seen", 0);
    let report = run(&[step(a.clone(), 10), deviating(step(a, 20), "duplicate")]);
    assert!(report.violations.is_empty(), "{:?}", report.violations);
}

#[test]
fn a_header_that_contradicts_itself() {
    let mut h = header();
    h.profile.captured.push("device".into());
    let report = run_with(
        h,
        &[step(id("device", 1, "seen", 0), 10)],
        &fixture_host(),
        1_000,
    );
    assert_eq!(rules(&report), vec![Rule::Header]);
    assert!(
        run(&[step(id("device", 1, "seen", 0), 10)])
            .violations
            .is_empty()
    );
}

/// A host whose domain requires every `fact` to await the `managed` of its
/// subject as expected.
struct Lives;

impl Host for Lives {
    fn required(&self, record: &Record) -> Vec<(StepId, On)> {
        if record.step.label == "fact" {
            let mut managed = record.step.clone();
            managed.label = "managed".into();
            managed.n = 0;
            vec![(managed, On::Expected)]
        } else {
            vec![]
        }
    }
}

fn fixture_host() -> Lives {
    Lives
}

#[test]
fn required() {
    let managed = step(id("device", 1, "managed", 0), 10);
    let fact = step(id("device", 1, "fact", 0), 20);
    let keeps = [
        managed.clone(),
        after(fact.clone(), &managed.step, On::Expected),
    ];
    let breaks = [managed.clone(), after(fact, &managed.step, On::Sent)];
    assert!(
        run_with(header(), &keeps, &Lives, 1_000)
            .violations
            .is_empty()
    );
    assert_eq!(
        rules(&run_with(header(), &breaks, &Lives, 1_000)),
        vec![Rule::Required]
    );
}

/// The peaks a run must hold: completions within W, live captures, polls.
#[test]
fn peaks() {
    let w0 = writer(1, 0, 10);
    let records = [
        w0.clone(),
        step(id("group", 1, "created", 0), 11),
        observe(step(id("group", 2, "seen", 0), 12), 10, 95),
        step(id("device", 1, "seen", 0), 10 + W + 1),
    ];
    let report = run(&records);
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    assert_eq!(report.records, 4);
    assert_eq!(report.peaks.captures, 1);
    // Polls at 0, 10, …, 90: ⌊95 / 10⌋ + 1.
    assert_eq!(report.peaks.polls, 10);
    // At the observation: w0 and it in the window, `created` retained. At the
    // last step w0 is out of the window, and `created` stays.
    assert_eq!(report.peaks.completions, 3);
}

/// Uniqueness by an external sort gives what it gives in memory, line for line.
#[test]
fn uniqueness_spilled_is_uniqueness_in_memory() {
    let records: Vec<Record> = (0..40u64)
        .map(|i| step(id("device", i % 13, "seen", 0), i))
        .collect();
    let in_memory = run_with(header(), &records, &NoHost, 1_000);
    for budget in [1, 2, 7] {
        assert_eq!(
            run_with(header(), &records, &NoHost, budget),
            in_memory,
            "budget {budget}"
        );
    }
    assert_eq!(in_memory.violations.len(), 40 - 13);
    assert!(
        in_memory
            .violations
            .iter()
            .all(|v| v.rule == Rule::Duplicate)
    );
}

/// V2: the independent reference (`vectors/reference.py`, quadratic, from the
/// rules' definitions) reaches the validator's verdict on every log of the
/// corpus: small random logs and their one-edit mutants.
#[test]
fn the_validator_agrees_with_the_reference() {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/vectors/reference.py");
    let mut child = Command::new("python3")
        .args(["-I", script])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("python3, which the library's gate requires");
    {
        let mut stdin = child.stdin.take().unwrap();
        for line in fixture::corpus::lines() {
            writeln!(stdin, "{line}").unwrap();
        }
    }
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Holes are found wherever a record holds them: in a payload's spans, in an
/// observation's predicates.
#[test]
fn holes_inside_payloads_and_predicates_are_seen() {
    use ascetic_ddd_command_log::record::{Action, Payload, Predicate, Span, Test};
    let w = writer(1, 0, 10);
    let captured = Hole::Captured {
        subject: subject("group", 1),
        slot: "etag".into(),
        from: w.step.clone(),
    };
    let in_span = |on| {
        let mut r = after(step(id("group", 1, "posted", 0), 20), &w.step, on);
        r.action = Action::Host(ascetic_ddd_command_log::record::HostCall {
            op: "noop".into(),
            args: Payload {
                codec: Codec::Text,
                bytes: vec![b'x'; 8],
                holes: vec![Span {
                    offset: 0,
                    len: 8,
                    field: "etag".into(),
                    hole: captured.clone(),
                    form: Form::Raw,
                }],
            },
        });
        r
    };
    twins(
        Rule::Producer,
        &[w.clone(), in_span(On::Expected)],
        &[w.clone(), in_span(On::Sent)],
    );
    let in_predicate = |on| {
        let mut r = after(
            observe(step(id("group", 1, "watched", 0), 20), 10, 100),
            &w.step,
            on,
        );
        if let Action::Observe(o) = &mut r.action {
            o.until.push(Predicate {
                pointer: "/etag".into(),
                test: Test::Equals(Value::Hole(captured.clone())),
            });
        }
        r
    };
    twins(
        Rule::Producer,
        &[w.clone(), in_predicate(On::Expected)],
        &[w.clone(), in_predicate(On::Sent)],
    );
    // An id rendered inside a span is held to the profile too.
    let rendered = |kind: &str| {
        let mut r = with_payload(
            step(id("tenant", 1, "joined", 0), 10),
            Codec::Text,
            8,
            vec![(0, 8, Form::Raw)],
        );
        if let Action::Host(h) = &mut r.action {
            h.args.holes[0].hole = Hole::Rendered {
                of: Of::Subject(subject(kind, 3)),
                role: "id".into(),
                rendering: Rendering::UuidText,
            };
        }
        r
    };
    twins(Rule::Profile, &[rendered("device")], &[rendered("group")]);
}

/// A captured id is held to the profile: only a kind the stand captures.
#[test]
fn a_captured_id_is_of_a_kind_the_stand_captures() {
    use ascetic_ddd_command_log::record::{Capture, Source};
    let pair = |kind: &str| {
        let mut created = step(id(kind, 1, "made", 0), 10);
        created.capture.push(Capture {
            subject: subject(kind, 1),
            slot: "id".into(),
            source: Source::Location,
            secret: false,
        });
        let mut r = after(
            step(id("tenant", 1, "joined", 0), 20),
            &created.step,
            On::Expected,
        );
        r.action = rest(
            "read",
            vec![Value::Hole(Hole::Captured {
                subject: subject(kind, 1),
                slot: "id".into(),
                from: created.step.clone(),
            })],
        );
        vec![created, r]
    };
    twins(Rule::Profile, &pair("group"), &pair("device"));
}

/// A host that counts the spill directory's files at every step.
struct Watching {
    dir: std::path::PathBuf,
    most: std::cell::Cell<usize>,
}

impl Host for Watching {
    fn required(&self, _: &Record) -> Vec<(StepId, On)> {
        let files = std::fs::read_dir(&self.dir).unwrap().count();
        self.most.set(self.most.get().max(files));
        vec![]
    }
}

/// Under its budget the pass keeps ids in memory and writes no run; past it,
/// runs go to disk during the pass, and are gone after it.
#[test]
fn ids_spill_past_their_budget_only() {
    use ascetic_ddd_command_log::validate::{Limits, validate};
    let records: Vec<Record> = (0..20u64)
        .map(|i| step(id("device", i, "seen", 0), i))
        .collect();
    for (budget, spills) in [(100, false), (4, true)] {
        let dir = tempfile::tempdir().unwrap();
        let host = Watching {
            dir: dir.path().to_owned(),
            most: std::cell::Cell::new(0),
        };
        let limits = Limits {
            ids_in_memory: budget,
            spill: dir.path().to_owned(),
            kept: 10,
        };
        let reader =
            ascetic_ddd_command_log::Reader::new(std::io::Cursor::new(bytes(header(), &records)))
                .unwrap();
        let report = validate(reader, &host, &limits).unwrap();
        assert!(report.violations.is_empty());
        assert_eq!(
            host.most.get() > 0,
            spills,
            "budget {budget}: {} files at most",
            host.most.get()
        );
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}

/// The report keeps at most `kept` violations and counts them all.
#[test]
fn a_report_keeps_what_it_may_and_counts_the_rest() {
    let a = id("device", 1, "seen", 0);
    let records: Vec<Record> = (0..6u64).map(|i| step(a.clone(), i)).collect();
    let dir = tempfile::tempdir().unwrap();
    let limits = ascetic_ddd_command_log::validate::Limits {
        ids_in_memory: 100,
        spill: dir.path().to_owned(),
        kept: 2,
    };
    let reader =
        ascetic_ddd_command_log::Reader::new(std::io::Cursor::new(bytes(header(), &records)))
            .unwrap();
    let report = ascetic_ddd_command_log::validate::validate(reader, &NoHost, &limits).unwrap();
    assert_eq!((report.violations.len(), report.violated), (2, 5));
    assert_eq!(
        report.violations[0].to_string(),
        "line 3: duplicate: a step id an earlier line has"
    );
    assert_eq!(Rule::Writers.to_string(), "writers");
}

/// A file is validated as its bytes are.
#[test]
fn a_file_is_validated_as_its_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("run.jsonl");
    let a = id("device", 1, "seen", 0);
    let records = [step(a.clone(), 10), step(a, 20)];
    std::fs::write(&path, bytes(header(), &records)).unwrap();
    let spill = tempfile::tempdir().unwrap();
    let report = ascetic_ddd_command_log::validate::validate_file(
        &path,
        &NoHost,
        &ascetic_ddd_command_log::validate::Limits::new(spill.path()),
    )
    .unwrap();
    assert_eq!(rules(&report), vec![Rule::Duplicate]);
    assert_eq!(report.records, 2);
}
