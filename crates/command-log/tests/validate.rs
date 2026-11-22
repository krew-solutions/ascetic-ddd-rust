//! V1 of ADR-0046: for every rule, a minimal log that breaks it beside its
//! twin that keeps it.

mod fixture;

use ascetic_ddd_command_log::record::{
    Action, Codec, Dep, Form, Hole, Of, On, Outcome, Predicate, Probe, Profile, Record, Rendering,
    Safety, StepId, Test, Value,
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

/// A group's update: it writes the group's ETag and captures it.
fn writer(g: u64, n: u32, at: u64) -> Record {
    let r = step(id("group", g, "update", n), at);
    captures_etag(
        acting(r, rest("update", vec![Value::Literal(g.to_string())])),
        g,
    )
}

fn reader(g: u64, n: u32, at: u64, from: &StepId, on: On) -> Record {
    let r = step(id("group", g, "read", n), at);
    after(acting(r, read_etag("read", g, from)), from, on)
}

#[test]
fn producer() {
    let w = writer(1, 0, 10);
    twins(
        Rule::Producer,
        &[w.clone(), reader(1, 0, 20, &w.step, On::Expected)],
        &[w.clone(), reader(1, 0, 20, &w.step, On::Sent)],
    );
    // The slot's latest writer must capture the value its reader sends: one
    // that only writes it gives the reader nothing (ADR-0047 decision 5).
    let mut silent = writer(1, 0, 10);
    silent.capture.clear();
    twins(
        Rule::Producer,
        &[w.clone(), reader(1, 0, 20, &w.step, On::Expected)],
        &[silent.clone(), reader(1, 0, 20, &silent.step, On::Expected)],
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
            after(w1.clone(), &r0.step, On::Settled),
        ],
        &[w0.clone(), r0.clone(), w1.clone()],
    );
    // A reader's start is not its end: the server may see the next writer
    // first (ADR-0047 decision 5).
    twins(
        Rule::Writers,
        &[
            w0.clone(),
            r0.clone(),
            after(w1.clone(), &r0.step, On::Settled),
        ],
        &[
            w0.clone(),
            r0.clone(),
            after(w1.clone(), &r0.step, On::Sent),
        ],
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
        acting(
            step(id("tenant", 1, "joined", 0), 10),
            rest(
                "read",
                vec![Value::Hole(Hole::Rendered {
                    of: Of::Subject(subject(kind, 3)),
                    role: "id".into(),
                    rendering: Rendering::UuidText,
                })],
            ),
        )
    };
    twins(Rule::Profile, &[rendered("device")], &[rendered("group")]);
}

#[test]
fn operation() {
    let call =
        |op: &str, path: Vec<Value>| acting(step(id("group", 1, "seen", 0), 10), rest(op, path));
    let one = || vec![Value::Literal("1".into())];
    twins(
        Rule::Operation,
        &[call("read", one())],
        &[call("delete", one())],
    );
    // A path of another arity cannot be built (ADR-0047 decision 15).
    twins(Rule::Frame, &[call("read", one())], &[call("read", vec![])]);
    twins(Rule::Frame, &[call("list", vec![])], &[call("list", one())]);
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
    // The scheduler's premises are not deviable (ADR-0047 decision 13): a
    // duplicate on purpose is a duplicate, and its deviation unused.
    let a = id("device", 1, "seen", 0);
    let report = run(&[step(a.clone(), 10), deviating(step(a, 20), "duplicate")]);
    assert_eq!(rules(&report), vec![Rule::Duplicate, Rule::Deviation]);
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
    println!("{}", String::from_utf8_lossy(&out.stdout));
    // The corpus reaches decision 15's rules, or the agreement on them says
    // nothing (ADR-0047 K2a-4b).
    let mut reached = std::collections::BTreeSet::new();
    for line in fixture::corpus::lines() {
        let entry: serde_json::Value = serde_json::from_str(&line).unwrap();
        for v in entry["verdict"].as_array().unwrap() {
            reached.insert(v[1].as_str().unwrap().to_owned());
        }
    }
    for rule in [
        "frame",
        "predicate",
        "expect",
        "secret",
        "sent",
        "operation",
        "span",
    ] {
        assert!(
            reached.contains(rule),
            "the corpus reaches no {rule}: {reached:?}"
        );
    }
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
                codec: Codec::Json,
                bytes: b"\"xxxxxx\"".to_vec(),
                holes: vec![Span {
                    offset: 0,
                    len: 8,
                    field: "/etag".into(),
                    hole: captured.clone(),
                    form: Form::JsonToken,
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
        let r = after(
            step(id("tenant", 1, "joined", 0), 20),
            &created.step,
            On::Expected,
        );
        let r = acting(
            r,
            rest(
                "read",
                vec![Value::Hole(Hole::Captured {
                    subject: subject(kind, 1),
                    slot: "id".into(),
                    from: created.step.clone(),
                })],
            ),
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
        let files = runs(&self.dir);
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
            memory: budget * 24,
            spill: dir.path().to_owned(),
            kept: 10,
            fan_in: 0,
            memory_spill: true,
            progress: None,
            hot: 1 << 20,
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
        assert_eq!(leftover(dir.path()).len(), 0);
    }
}

/// The report keeps at most `kept` violations and counts them all.
#[test]
fn a_report_keeps_what_it_may_and_counts_the_rest() {
    let a = id("device", 1, "seen", 0);
    let records: Vec<Record> = (0..6u64).map(|i| step(a.clone(), i)).collect();
    let dir = tempfile::tempdir().unwrap();
    let limits = ascetic_ddd_command_log::validate::Limits {
        memory: 100 * 24,
        spill: dir.path().to_owned(),
        kept: 2,
        fan_in: 0,
        memory_spill: true,
        progress: None,
        hot: 1 << 20,
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
    let report =
        ascetic_ddd_command_log::validate::validate_file(&path, &NoHost, &limits_in(spill.path()))
            .unwrap();
    assert_eq!(rules(&report), vec![Rule::Duplicate]);
    assert_eq!(report.records, 2);
}

/// The pairs a report names, in its order.
fn pairs(report: &ascetic_ddd_command_log::validate::Report) -> Vec<(u64, Rule)> {
    report.violations.iter().map(|v| (v.line, v.rule)).collect()
}

/// Decision 16: W is measured behind the read cursor. A line planned ahead
/// takes the cursor with it; a later line awaiting a step more than W behind
/// the cursor breaks `edge` even where its own time is near — a run that has
/// read the line ahead has dropped the completion.
#[test]
fn w_is_behind_the_read_cursor() {
    let (s, x, y) = (
        id("device", 1, "seen", 0),
        id("device", 2, "seen", 0),
        id("device", 3, "seen", 0),
    );
    // Y is planned behind X, which breaks the order — and no deviation
    // excuses it (ADR-0047 decision 13) — but the edge is decided by the
    // cursor all the same.
    let y_after = |at| after(step(y.clone(), at), &s, On::Expected);
    // X within W of S: the cursor at X leaves S in reach.
    let near = run(&[step(s.clone(), 50), step(x.clone(), 50 + W), y_after(150)]);
    assert_eq!(pairs(&near), vec![(4, Rule::Order)]);
    // X past W of S: S is behind the cursor by more than W.
    let ahead = run(&[step(s.clone(), 50), step(x.clone(), 51 + W), y_after(150)]);
    assert_eq!(pairs(&ahead), vec![(4, Rule::Order), (4, Rule::Edge)]);
}

/// A step dead on arrival — more than W behind the cursor — answers for no
/// later edge, and an older occurrence of its id does not answer for it.
#[test]
fn a_step_dead_on_arrival_answers_for_nothing() {
    let (s, x, z) = (
        id("device", 1, "seen", 0),
        id("device", 2, "seen", 0),
        id("device", 3, "seen", 0),
    );
    let records = |again: u64| {
        vec![
            step(x.clone(), 20_000),
            step(s.clone(), 19_500),
            // S again: within W of the cursor, or more than W behind it.
            step(s.clone(), again),
            after(step(z.clone(), 20_100), &s, On::Expected),
        ]
    };
    let alive = run(&records(19_400));
    assert_eq!(
        pairs(&alive),
        vec![(3, Rule::Order), (4, Rule::Order), (4, Rule::Duplicate)]
    );
    let dead = run(&records(18_000));
    assert_eq!(
        pairs(&dead),
        vec![
            (3, Rule::Order),
            (4, Rule::Order),
            (4, Rule::Duplicate),
            (5, Rule::Edge)
        ]
    );
}

/// The window is evicted by planned time, not by line: steps planned behind a
/// line ahead leave it as the cursor passes them, though the line ahead stays.
#[test]
fn the_window_holds_only_what_is_within_w() {
    let mut records = vec![step(id("device", 0, "seen", 0), 100_000)];
    // Ten steps a little behind it, each within W on arrival; the first breaks
    // the order.
    for i in 1..=10u64 {
        records.push(step(id("device", i, "seen", 0), 99_000 + i * 10));
    }
    // The cursor moves on, the line ahead still within W of it: the ten fall
    // more than W behind it while twenty more arrive.
    for i in 1..=20u64 {
        records.push(step(id("device", 100 + i, "seen", 0), 100_500 + i));
    }
    let report = run(&records);
    // The first of the ten breaks the order, and no deviation excuses it.
    assert_eq!(pairs(&report), vec![(3, Rule::Order)]);
    // The line ahead and the twenty; the ten would be held behind it by an
    // eviction in line order.
    assert_eq!(report.peaks.completions, 21);
}

/// No deviation exempts `deviation`: one declared against it is unused.
#[test]
fn a_deviation_does_not_exempt_itself() {
    let a = id("device", 1, "seen", 0);
    let report = run(&[deviating(step(a.clone(), 10), "deviation")]);
    assert_eq!(pairs(&report), vec![(2, Rule::Deviation)]);
}

/// The report keeps the least violations by line, whenever each is found: a
/// duplicate decided at the end on line 3 before an edge found on line 4.
#[test]
fn the_report_keeps_the_first_lines_whenever_found() {
    let (a, u) = (id("device", 1, "seen", 0), id("device", 9, "seen", 0));
    let records = [
        step(a.clone(), 10),
        step(a.clone(), 20),
        after(step(id("device", 2, "seen", 0), 30), &u, On::Expected),
    ];
    let dir = tempfile::tempdir().unwrap();
    let limits = ascetic_ddd_command_log::validate::Limits {
        memory: 100 * 24,
        spill: dir.path().to_owned(),
        kept: 1,
        fan_in: 0,
        memory_spill: true,
        progress: None,
        hot: 1 << 20,
    };
    let reader =
        ascetic_ddd_command_log::Reader::new(std::io::Cursor::new(bytes(header(), &records)))
            .unwrap();
    let report = ascetic_ddd_command_log::validate::validate(reader, &NoHost, &limits).unwrap();
    assert_eq!(
        (pairs(&report), report.violated),
        (vec![(3, Rule::Duplicate)], 2)
    );
}

/// Live captures: a slot whose writer left W behind the cursor is not live.
#[test]
fn captures_are_counted_while_live() {
    let near = run(&[writer(1, 0, 10), writer(2, 0, 10 + W)]);
    assert_eq!(near.peaks.captures, 2);
    let apart = run(&[writer(1, 0, 10), writer(2, 0, 11 + W)]);
    assert_eq!(apart.peaks.captures, 1);
}

/// What a stream or a slot remembers is not forgotten with the window
/// (decision 16): a message more than W after its predecessor, not awaiting
/// it; a writer more than W after the last; a gapless stream begun again.
#[test]
fn streams_and_slots_outlive_the_window() {
    let m = |name: &str, n: u32, at: u64, position: u32| {
        message(step(id("device", 1, "fact", n), at), name, position)
    };
    let late = run(&[m("telemetry", 0, 10, 1), m("telemetry", 1, 11 + W, 2)]);
    assert_eq!(pairs(&late), vec![(3, Rule::Stream)]);
    let rewritten = run(&[writer(1, 0, 10), writer(1, 1, 11 + W)]);
    assert_eq!(pairs(&rewritten), vec![(3, Rule::Writers)]);
    let again = run(&[m("lifecycle", 0, 10, 1), m("lifecycle", 1, 11 + W, 1)]);
    assert_eq!(rules(&again), vec![Rule::Stream, Rule::Gap]);
}

/// What the rules mean, pinned beside their revision: the SHA-256 of the
/// corpus's logs, verdicts and peaks. A rule whose meaning changes changes it;
/// then `RULES_REVISION` is bumped and the new digest appended. A corpus that
/// changes alone is re-pinned under the same revision.
#[test]
fn the_rules_meaning_is_pinned_to_its_revision() {
    use sha2::{Digest, Sha256};
    const MEANINGS: &[(u32, &str)] = &[
        (
            3,
            "a0f4ebb2d85eb119d5b36d57c8efd5924b264d1236b7fbbbab118dc6f7389fdf",
        ),
        (
            4,
            "8718e8bf5d7499618d255566f4d6a38f9111b7eb90a7d580904c34dce7168969",
        ),
        (
            5,
            "e513cbb1258ca9684b36c132ccea304c67b00a45617e811f6c4729bc29a93b8a",
        ),
        (
            6,
            "a7b4728558e9465c6d197224f8d4d22a9d141956bb0581f8c43335a513f46aac",
        ),
        (
            7,
            "0ae498d98f8cf6e01c6913a4d382c12067abb6cee01289d7a311030226fcd67a",
        ),
        (
            8,
            "2c611cc2ae7e1d1659f54eab2de6e2135cdcc06ec965d28a3f90053afb6812eb",
        ),
    ];
    let mut digest = Sha256::new();
    for line in fixture::corpus::lines() {
        digest.update(line.as_bytes());
        digest.update(b"\n");
    }
    let hex: String = digest
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let revisions: Vec<u32> = MEANINGS.iter().map(|m| m.0).collect();
    assert!(revisions.windows(2).all(|w| w[0] < w[1]));
    assert_eq!(
        MEANINGS.last().copied(),
        Some((ascetic_ddd_command_log::live::RULES_REVISION, hex.as_str())),
        "a rule's meaning changed: bump RULES_REVISION and append its digest"
    );
}

/// Decision 16: a report is a function of the log alone — every corpus log
/// validated with every event its own run through a fan-in of two, and with a
/// few events a run through three, gives the very report it gives in memory.
#[test]
fn a_report_is_a_function_of_the_log_alone() {
    let at = |records: &[Record], memory: usize, fan_in: usize| {
        let dir = tempfile::tempdir().unwrap();
        let limits = ascetic_ddd_command_log::validate::Limits {
            memory,
            spill: dir.path().to_owned(),
            kept: 1_000,
            fan_in,
            memory_spill: true,
            progress: None,
            hot: 1 << 20,
        };
        let reader = ascetic_ddd_command_log::Reader::new(std::io::Cursor::new(bytes(
            fixture::corpus::corpus_header(),
            records,
        )))
        .unwrap();
        let report = ascetic_ddd_command_log::validate::validate(reader, &NoHost, &limits).unwrap();
        assert!(leftover(dir.path()).is_empty());
        report
    };
    for (name, records) in fixture::corpus::logs() {
        let whole = at(&records, 1 << 30, 0);
        for (memory, fan_in) in [(1, 2), (100, 3)] {
            assert_eq!(
                at(&records, memory, fan_in),
                whole,
                "{name} at {memory} bytes, a fan-in of {fan_in}"
            );
        }
    }
}

/// A long gapless stream through runs of a few hundred messages, merged two
/// and three at a time and all at once, is clean: no message is lost or
/// reordered on its way through the runs.
#[test]
fn a_long_stream_through_small_runs_is_clean() {
    let mut records = Vec::new();
    let mut before: Option<StepId> = None;
    for n in 0..10_000u32 {
        let mut r = message(
            step(id("device", 1, "fact", n), u64::from(n) * 10),
            "lifecycle",
            n + 1,
        );
        if let Some(b) = &before {
            r = after(r, b, On::Expected);
        }
        before = Some(r.step.clone());
        records.push(r);
    }
    for fan_in in [2, 3, 0] {
        let dir = tempfile::tempdir().unwrap();
        let limits = ascetic_ddd_command_log::validate::Limits {
            memory: 40_000,
            spill: dir.path().to_owned(),
            kept: 10,
            fan_in,
            memory_spill: true,
            progress: None,
            hot: 1 << 20,
        };
        let reader =
            ascetic_ddd_command_log::Reader::new(std::io::Cursor::new(bytes(header(), &records)))
                .unwrap();
        let report = ascetic_ddd_command_log::validate::validate(reader, &NoHost, &limits).unwrap();
        assert!(
            report.violations.is_empty(),
            "fan-in {fan_in}: {:?}",
            report.violations
        );
        // Positions rising by one through the runs: a message dropped or two
        // swapped would be a gap.
        let mut broken = records.clone();
        broken.remove(5_000);
        let reader =
            ascetic_ddd_command_log::Reader::new(std::io::Cursor::new(bytes(header(), &broken)))
                .unwrap();
        let report = ascetic_ddd_command_log::validate::validate(reader, &NoHost, &limits).unwrap();
        assert_eq!(rules(&report), vec![Rule::Edge, Rule::Stream, Rule::Gap]);
    }
}

/// A stream falling back is broken where it falls: the message whose position
/// is not above the one before it, in line order — not where a sort by
/// position would put the break.
#[test]
fn a_stream_falling_back_is_broken_where_it_falls() {
    let m = |n: u32, at: u64, position: u32| {
        message(step(id("device", 1, "fact", n), at), "telemetry", position)
    };
    let first = m(0, 10, 5);
    let report = run(&[first.clone(), after(m(1, 20, 2), &first.step, On::Expected)]);
    assert_eq!(pairs(&report), vec![(3, Rule::Stream)]);
}

/// Decision 16: a hot part past its limit fails the pass by name and line —
/// the completions kept, the captured slots, the barrier counts, the declared
/// deviations, the polls in flight — and at its limit it does not.
#[test]
fn a_hot_part_past_its_limit_fails_by_name() {
    use ascetic_ddd_command_log::LogError;
    use ascetic_ddd_command_log::record::{By, Header};
    let at = |header: Header, records: &[Record], hot: u64| {
        let dir = tempfile::tempdir().unwrap();
        let limits = ascetic_ddd_command_log::validate::Limits {
            hot,
            memory_spill: true,
            ..ascetic_ddd_command_log::validate::Limits::new(dir.path())
        };
        let reader =
            ascetic_ddd_command_log::Reader::new(std::io::Cursor::new(bytes(header, records)))
                .unwrap();
        let result = ascetic_ddd_command_log::validate::validate(reader, &NoHost, &limits);
        assert!(leftover(dir.path()).is_empty());
        result.map(|_| ())
    };
    // Steps within W of one another for the completions; more than W apart
    // for the rest, so that only their own part grows.
    let near = |i: u64| step(id("device", i, "seen", 0), i);
    let apart = |i: u64| step(id("device", i, "seen", 0), i * 2 * W);
    // Polls are held for an observation whose call a limit counts; a bucket
    // a value of the group in a call's path.
    let counted = limited(vec![limit("all", By::All, 1_000, 1, 1_000)], 2, 0, 0);
    let by_group = limited(
        vec![limit(
            "g",
            By::At(ascetic_ddd_command_log::record::Place::Path("group".into())),
            1_000,
            1,
            1_000,
        )],
        2,
        0,
        0,
    );
    let cases: Vec<(&str, Header, Vec<Record>)> = vec![
        ("completions", header(), (0..4).map(near).collect()),
        (
            "captures",
            header(),
            (0..4).map(|g| writer(g, 0, g * 2 * W)).collect(),
        ),
        (
            "barrier counts",
            header(),
            (0..4)
                .map(|i| step(id("device", 1, &format!("l{i}"), 0), i * 2 * W))
                .collect(),
        ),
        (
            "deviations",
            header(),
            (0..4).map(|i| deviating(apart(i), "gap")).collect(),
        ),
        (
            "buckets",
            by_group,
            (0..4).map(|g| call("read", g, 0, g * 2 * W, 0)).collect(),
        ),
        (
            "polls",
            counted,
            (0..4)
                .map(|g| polling(g, 0, g * 2 * W, 10, 10 * W))
                .collect(),
        ),
    ];
    // The bound's parts (ADR-0047 K1e-2), each with its own line: steps the
    // window let go held by an earlier member of their key, which a host call
    // that times out at once leaves no other reason to hold; and steps not
    // ended, two in the window and two let go.
    let mut quick = header();
    quick.calls[0].timeout = W / 2;
    let first = Record {
        action: rest("update", vec![Value::Literal("0".into())]),
        ..step(id("device", 0, "x", 0), 0)
    };
    let held_past: Vec<Record> = std::iter::once(first)
        .chain((1..5).map(|i| step(id("device", i, "x", 0), i * 2 * W)))
        .collect();
    let long = |i: u64, at: u64| step(id("device", i, "long", 0), at);
    let pending = vec![
        long(1, 0),
        long(2, 2 * W),
        long(3, 2 * W + 1),
        long(4, 4 * W + 2),
    ];
    let bound_cases: Vec<(&str, Header, Vec<Record>, u64)> = vec![
        ("held past the window", quick, held_past, 6),
        ("pending", header(), pending, 5),
    ];
    for (part, header, records, line) in bound_cases {
        match at(header.clone(), &records, 3) {
            Err(LogError::Hot {
                part: p,
                held,
                line: l,
            }) => {
                assert_eq!((p, held, l), (part, 4, line), "{part}")
            }
            other => panic!("{part}: {other:?}"),
        }
        assert!(
            !matches!(at(header, &records, 4), Err(LogError::Hot { .. })),
            "{part} at its limit"
        );
    }
    for (part, header, records) in cases {
        match at(header.clone(), &records, 3) {
            Err(LogError::Hot {
                part: p,
                held,
                line,
            }) => {
                assert_eq!((p, held, line), (part, 4, 5), "{part}")
            }
            other => panic!("{part}: {other:?}"),
        }
        assert!(
            !matches!(at(header, &records, 4), Err(LogError::Hot { .. })),
            "{part} at its limit"
        );
    }
}

/// ADR-0047 decision 3: a step is planned no earlier than its dependencies and
/// its think allow — a step dependency and a barrier's latest member alike.
#[test]
fn feasible() {
    let (a, b) = (id("device", 1, "seen", 0), id("device", 2, "seen", 0));
    let thinking = |at: u64, think: u64| Record {
        think,
        ..after(step(b.clone(), at), &a, On::Expected)
    };
    twins(
        Rule::Feasible,
        &[step(a.clone(), 10), thinking(60, 50)],
        &[step(a.clone(), 10), thinking(59, 50)],
    );
    let barrier = |at: u64, think: u64| Record {
        think,
        after: vec![Dep::Each {
            kind: "device".into(),
            scope: vec![7],
            label: "seen".into(),
            n: 0,
            on: On::Settled,
            count: None,
        }],
        ..step(id("device", 9, "report", 0), at)
    };
    let members = [step(a.clone(), 10), step(b.clone(), 30)];
    let with = |r: Record| members.iter().cloned().chain([r]).collect::<Vec<_>>();
    // The latest member, at 30, not the first.
    twins(
        Rule::Feasible,
        &with(barrier(50, 20)),
        &with(barrier(49, 20)),
    );
    // Think from the run's start, whatever a step awaits (K1e-2): a step with
    // no dependency, and a holder of a barrier no member was read before.
    let alone = |at: u64, think: u64| Record {
        think,
        ..step(a.clone(), at)
    };
    twins(Rule::Feasible, &[alone(20, 20)], &[alone(19, 20)]);
    twins(Rule::Feasible, &[barrier(20, 20)], &[barrier(19, 20)]);
}

/// A measure is from a step an earlier line has, however far back.
#[test]
fn measure() {
    let (a, b) = (id("device", 1, "seen", 0), id("device", 2, "seen", 0));
    let measured = |from: &StepId| Record {
        measure: vec![ascetic_ddd_command_log::record::Measure {
            metric: "m".into(),
            from: from.clone(),
        }],
        ..step(b.clone(), 10 * W)
    };
    // From a step more than W back: still a measure's start.
    twins(
        Rule::Measure,
        &[step(a.clone(), 10), measured(&a)],
        &[step(a.clone(), 10), measured(&b)],
    );
}

/// An observation awaits its cause, or is its own; one of the bus tells its
/// cause's fact from another's.
#[test]
fn cause_and_until() {
    use ascetic_ddd_command_log::record::{Of, Predicate, Test};
    let (a, b) = (
        id("device", 1, "managed", 0),
        id("device", 1, "reported", 0),
    );
    let watched = |cause: &StepId, awaited: bool| {
        let r = step(b.clone(), 20);
        let r = if awaited { after(r, &a, On::Sent) } else { r };
        let mut r = observe(r, 10, 100);
        if let Action::Observe(o) = &mut r.action {
            o.cause = cause.clone();
            o.until = vec![Predicate {
                pointer: "/cause".into(),
                test: Test::Equals(Value::Hole(Hole::Rendered {
                    of: Of::Step(cause.clone()),
                    role: "id".into(),
                    rendering: Rendering::UuidText,
                })),
            }];
        }
        r
    };
    twins(
        Rule::Cause,
        &[step(a.clone(), 10), watched(&a, true)],
        &[step(a.clone(), 10), watched(&a, false)],
    );
    // Its own cause needs no dependency, but its until must name it.
    let own_until = watched(&b, false);
    let mut own = own_until.clone();
    if let Action::Observe(o) = &mut own.action {
        o.until = vec![];
    }
    twins(Rule::Until, &[own_until], &[own]);
    // An id that recurs across a subject's lives does not tell its cause.
    let mut by_subject = watched(&a, true);
    if let Action::Observe(o) = &mut by_subject.action {
        o.until = vec![Predicate {
            pointer: "/device".into(),
            test: Test::Equals(Value::Hole(Hole::Rendered {
                of: Of::Subject(a.subject.clone()),
                role: "id".into(),
                rendering: Rendering::UuidText,
            })),
        }];
    }
    twins(
        Rule::Until,
        &[step(a.clone(), 10), watched(&a, true)],
        &[step(a.clone(), 10), by_subject],
    );
}

/// A step that awaits a dependency only sent carries no measure and no
/// expectation; an observation may.
#[test]
fn sent() {
    use ascetic_ddd_command_log::record::Outcome;
    let (a, b) = (id("device", 1, "seen", 0), id("device", 2, "seen", 0));
    let expecting = |on: On| Record {
        expect: vec![Outcome::Delivered],
        ..after(message(step(b.clone(), 20), "lifecycle", 1), &a, on)
    };
    twins(
        Rule::Sent,
        &[step(a.clone(), 10), expecting(On::Settled)],
        &[step(a.clone(), 10), expecting(On::Sent)],
    );
    let polled = observe(after(step(b.clone(), 20), &a, On::Sent), 10, 100);
    let report = run(&[
        step(a.clone(), 10),
        Record {
            expect: vec![Outcome::Observed],
            ..polled
        },
    ]);
    assert!(report.violations.is_empty(), "{:?}", report.violations);
}

/// ADR-0047 decision 15: the profile names a problem pointer, a correlation
/// header of its own, a resend floor, an answer cap and the stand's idle.
#[test]
fn the_profile_names_what_the_shell_reads_and_sends_by() {
    let seen = [step(id("device", 1, "seen", 0), 10)];
    let report = run_with(header(), &seen, &NoHost, 1_000);
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    type Edit = (&'static str, fn(&mut Profile));
    let edits: [Edit; 9] = [
        ("a pointer without its slash", |p| p.problem = "code".into()),
        ("a pointer's bad escape", |p| p.problem = "/c~2".into()),
        ("no correlation header", |p| p.correlation = String::new()),
        ("a correlation header not a token", |p| {
            p.correlation = "X Id".into()
        }),
        ("a reserved correlation header", |p| {
            p.correlation = "Content-Length".into()
        }),
        ("a reserved one in another case", |p| {
            p.correlation = "hOST".into()
        }),
        ("no resend floor", |p| p.resend = 0),
        ("no answer cap", |p| p.answer_cap = 0),
        ("no idle", |p| p.idle = 0),
    ];
    for (name, edit) in edits {
        let mut h = header();
        edit(&mut h.profile);
        let report = run_with(h, &seen, &NoHost, 1_000);
        assert_eq!(pairs(&report), vec![(1, Rule::Header)], "{name}");
    }
    let mut h = header();
    h.profile.problem = String::new();
    let report = run_with(h, &seen, &NoHost, 1_000);
    assert!(
        report.violations.is_empty(),
        "the document itself: {:?}",
        report.violations
    );
}

/// Deadlines: an operation's below the route timeout by more than the
/// jitter; channels and host calls in their tables, with a timeout.
#[test]
fn deadlines_and_tables() {
    let mut h = header();
    h.operations[0].timeout = ROUTE_TIMEOUT;
    let report = run_with(h, &[step(id("device", 1, "seen", 0), 10)], &NoHost, 1_000);
    assert_eq!(pairs(&report), vec![(1, Rule::Header)]);
    let mut h = header();
    h.operations[0].timeout = ROUTE_TIMEOUT - 1;
    let report = run_with(h, &[step(id("device", 1, "seen", 0), 10)], &NoHost, 1_000);
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    let mut h = header();
    h.channels[0].timeout = 0;
    let report = run_with(h, &[], &NoHost, 1_000);
    assert_eq!(pairs(&report), vec![(1, Rule::Header)]);
    let unknown = Record {
        action: Action::Host(ascetic_ddd_command_log::record::HostCall {
            op: "unlisted".into(),
            args: ascetic_ddd_command_log::record::Payload {
                codec: Codec::Json,
                bytes: b"{}".to_vec(),
                holes: vec![],
            },
        }),
        ..step(id("device", 1, "seen", 0), 10)
    };
    assert_eq!(pairs(&run(&[unknown])), vec![(2, Rule::Operation)]);
    let mut off = message(step(id("device", 1, "fact", 0), 10), "lifecycle", 1);
    if let Action::Bus(b) = &mut off.action {
        b.channel = "unlisted".into();
    }
    assert_eq!(pairs(&run(&[off])), vec![(2, Rule::Operation)]);
}

/// The scheduler's premises are not deviable (ADR-0047 decision 13): a line
/// out of order, an edge out of reach, a producer not awaited stay broken
/// under a deviation naming them, and the deviation is unused.
#[test]
fn the_premises_are_not_deviable() {
    let (a, b) = (id("device", 1, "seen", 0), id("device", 2, "seen", 0));
    let back = run(&[step(b.clone(), 20), deviating(step(a.clone(), 10), "order")]);
    assert_eq!(pairs(&back), vec![(3, Rule::Order), (3, Rule::Deviation)]);
    let far = run(&[
        step(a.clone(), 10),
        deviating(after(step(b.clone(), 20 + W), &a, On::Expected), "edge"),
    ]);
    assert_eq!(pairs(&far), vec![(3, Rule::Edge), (3, Rule::Deviation)]);
    let w = writer(1, 0, 10);
    let unawaited = run(&[
        w.clone(),
        deviating(reader(1, 0, 20, &w.step, On::Sent), "producer"),
    ]);
    assert_eq!(
        pairs(&unawaited),
        vec![(3, Rule::Producer), (3, Rule::Deviation)]
    );
}

// ADR-0047 decision 15, K2a-4b: what the shell sends and judges, checked
// before a run.

fn lit(s: &str) -> Value {
    Value::Literal(s.into())
}

fn one() -> Vec<Value> {
    vec![lit("1")]
}

/// A read of group 1 planning `headers` and these path values.
fn read_with(headers: Vec<(&str, Value)>, path: Vec<Value>) -> Record {
    let mut action = rest("read", path);
    if let Action::Rest(r) = &mut action {
        r.headers = headers
            .into_iter()
            .map(|(name, value)| ascetic_ddd_command_log::record::Field {
                name: name.into(),
                value,
            })
            .collect();
    }
    acting(step(id("group", 1, "seen", 0), 10), action)
}

fn rendered(role: &str, rendering: Rendering) -> Value {
    Value::Hole(Hole::Rendered {
        of: Of::Subject(subject("device", 1)),
        role: role.into(),
        rendering,
    })
}

/// A REST call's request can be built from its plan.
#[test]
fn frame() {
    let fine = || read_with(vec![("X-A", lit("1"))], one());
    for (name, broken) in [
        ("a host header", read_with(vec![("Host", lit("x"))], one())),
        (
            "the correlation header",
            read_with(vec![("x-correlation-id", lit("x"))], one()),
        ),
        (
            "a line break",
            read_with(vec![("X-A", lit("a\r\nb"))], one()),
        ),
        (
            "a name not a token",
            read_with(vec![("X A", lit("1"))], one()),
        ),
        ("a dot segment", read_with(vec![], vec![lit("..")])),
        ("an empty segment", read_with(vec![], vec![lit("")])),
        (
            "bytes in a header",
            read_with(
                vec![("X-A", rendered("serial", Rendering::UuidBytes))],
                one(),
            ),
        ),
        (
            "bytes in a path",
            read_with(vec![], vec![rendered("serial", Rendering::UuidBytes)]),
        ),
    ] {
        let kept = run(&[fine()]);
        assert!(kept.violations.is_empty(), "{name}: {:?}", kept.violations);
        assert_eq!(rules(&run(&[broken])), vec![Rule::Frame], "{name}");
    }
    // A poll's request too.
    let mut poll = polling(1, 0, 10, 10, 100);
    if let Action::Observe(o) = &mut poll.action
        && let Probe::Rest(r) = &mut o.probe
    {
        r.path.clear();
    }
    assert_eq!(rules(&run(&[poll])), vec![Rule::Frame]);
    // Frames are not deviable.
    let deviated = deviating(read_with(vec![("Host", lit("x"))], one()), "frame");
    assert!(rules(&run(&[deviated])).contains(&Rule::Frame));
}

/// Every predicate can be judged.
#[test]
fn predicate() {
    let until = |pointer: &str, test: Test| {
        let mut r = polling(1, 0, 10, 10, 100);
        if let Action::Observe(o) = &mut r.action {
            o.until.push(Predicate {
                pointer: pointer.into(),
                test,
            });
        }
        r
    };
    twins(
        Rule::Predicate,
        &[until("/a", Test::Exists)],
        &[until("a", Test::Exists)],
    );
    twins(
        Rule::Predicate,
        &[until("/a~0", Test::Exists)],
        &[until("/a~2", Test::Exists)],
    );
    twins(
        Rule::Predicate,
        &[until("/a", Test::Equals(lit("{\"n\":1.5}")))],
        &[until("/a", Test::Equals(lit("one")))],
    );
    twins(
        Rule::Predicate,
        &[until(
            "/a",
            Test::Contains(rendered("serial", Rendering::UuidText)),
        )],
        &[until(
            "/a",
            Test::Contains(rendered("serial", Rendering::Secret32)),
        )],
    );
    let body = |pointer: &str| Record {
        expect: vec![Outcome::Response {
            status: 200,
            problem: None,
            body: vec![Predicate {
                pointer: pointer.into(),
                test: Test::Exists,
            }],
        }],
        ..read_with(vec![], one())
    };
    twins(Rule::Predicate, &[body("")], &[body("x")]);
}

/// An expectation fits its action; a step asserting its answer has one.
#[test]
fn expect() {
    let read = || read_with(vec![], one());
    let response = |status| Outcome::Response {
        status,
        problem: None,
        body: vec![],
    };
    twins(
        Rule::Expect,
        &[read()],
        &[Record {
            expect: vec![],
            ..read()
        }],
    );
    twins(
        Rule::Expect,
        &[read()],
        &[Record {
            expect: vec![response(200), Outcome::Delivered],
            ..read()
        }],
    );
    let poll = || polling(1, 0, 10, 10, 100);
    twins(
        Rule::Expect,
        &[poll()],
        &[Record {
            expect: vec![],
            ..poll()
        }],
    );
    twins(
        Rule::Expect,
        &[poll()],
        &[Record {
            expect: vec![Outcome::Observed, response(409)],
            ..poll()
        }],
    );
    let fact = || message(step(id("device", 1, "fact", 0), 10), "lifecycle", 1);
    twins(
        Rule::Expect,
        &[fact()],
        &[Record {
            expect: vec![],
            ..fact()
        }],
    );
    let host = || step(id("device", 1, "seen", 0), 10);
    twins(
        Rule::Expect,
        &[host()],
        &[Record {
            expect: vec![response(200)],
            ..host()
        }],
    );
    // A call awaiting a step only sent expects nothing, and is clean.
    let a = step(id("device", 2, "seen", 0), 5);
    let sent = after(read_with(vec![], one()), &a.step, On::Sent);
    let report = run(&[a, sent]);
    assert!(report.violations.is_empty(), "{:?}", report.violations);
}

/// No secret in a request's target or a message's key.
#[test]
fn secret() {
    use ascetic_ddd_command_log::record::{Capture, Source};
    let w = {
        let mut w = writer(1, 0, 10);
        w.capture.push(Capture {
            subject: subject("group", 1),
            slot: "token".into(),
            source: Source::Header("X-Token".into()),
            secret: true,
        });
        w
    };
    let token = Value::Hole(Hole::Captured {
        subject: subject("group", 1),
        slot: "token".into(),
        from: w.step.clone(),
    });
    let reading = |in_path: bool| {
        let r = if in_path {
            read_with(vec![], vec![token.clone()])
        } else {
            read_with(vec![("Authorization", token.clone())], one())
        };
        after(
            Record {
                step: id("group", 1, "read", 0),
                at: 20,
                ..r
            },
            &w.step,
            On::Expected,
        )
    };
    twins(
        Rule::Secret,
        &[w.clone(), reading(false)],
        &[w.clone(), reading(true)],
    );
    // Key material in a query, which also cannot be text there.
    let mut q = read_with(vec![], one());
    if let Action::Rest(r) = &mut q.action {
        r.query.push(ascetic_ddd_command_log::record::Field {
            name: "k".into(),
            value: rendered("dpop", Rendering::Secret32),
        });
    }
    assert_eq!(rules(&run(&[q])), vec![Rule::Frame, Rule::Secret]);
    // In a message's key.
    let keyed = |secret: bool| {
        let mut m = message(step(id("device", 1, "fact", 0), 10), "lifecycle", 1);
        if let Action::Bus(b) = &mut m.action {
            b.key = if secret {
                rendered("dpop", Rendering::Secret32)
            } else {
                rendered("serial", Rendering::Hex { len: 4 })
            };
        }
        m
    };
    twins(Rule::Secret, &[keyed(false)], &[keyed(true)]);
}

/// A not-taken resend needs its codes; an idempotent call cannot send a value
/// its own first attempt rewrites; one producer a slot a record; no captured
/// value in a raw span.
#[test]
fn resends_producers_and_raw_spans() {
    let mut h = header();
    h.operations[0].safety = Safety::NotTaken;
    assert_eq!(
        pairs(&run_with(h.clone(), &[], &NoHost, 1_000)),
        vec![(1, Rule::Header)]
    );
    h.operations[0].not_run = vec!["service.unavailable".into()];
    assert!(run_with(h, &[], &NoHost, 1_000).violations.is_empty());
    // An update sending the ETag it rewrites, idempotent or not.
    let w0 = writer(1, 0, 10);
    let w1 = {
        let mut r = after(writer(1, 1, 30), &w0.step, On::Expected);
        if let (Action::Rest(rest), Action::Rest(read)) =
            (&mut r.action, read_etag("update", 1, &w0.step))
        {
            rest.headers = read.headers;
        }
        r
    };
    let plan = [w0.clone(), w1.clone()];
    assert!(
        run(&plan).violations.is_empty(),
        "{:?}",
        run(&plan).violations
    );
    let mut h = header();
    h.operations[0].safety = Safety::Idempotent;
    assert_eq!(
        pairs(&run_with(h, &plan, &NoHost, 1_000)),
        vec![(3, Rule::Operation)]
    );
    // A read of the ETag from two producers, the older one deviated stale.
    let from_both = |both: bool| {
        let mut r = reader(1, 0, 40, &w1.step, On::Expected);
        if both {
            r = after(r, &w0.step, On::Expected);
            if let (Action::Rest(rest), Action::Rest(old)) =
                (&mut r.action, read_etag("read", 1, &w0.step))
            {
                rest.headers.extend(old.headers.into_iter().map(|mut f| {
                    f.name = "X-Old".into();
                    f
                }));
            }
            r = deviating(r, "stale");
        }
        r
    };
    let kept = run(&[w0.clone(), w1.clone(), from_both(false)]);
    assert!(kept.violations.is_empty(), "{:?}", kept.violations);
    assert_eq!(
        rules(&run(&[w0.clone(), w1.clone(), from_both(true)])),
        vec![Rule::Producer]
    );
    // A captured value in a raw span, against one in a JSON token.
    let spanned = |raw: bool| {
        let mut r = after(
            step(id("group", 1, "posted", 0), 20),
            &w0.step,
            On::Expected,
        );
        r.action = Action::Host(ascetic_ddd_command_log::record::HostCall {
            op: "noop".into(),
            args: ascetic_ddd_command_log::record::Payload {
                codec: if raw { Codec::Text } else { Codec::Json },
                bytes: b"\"xxxxxx\"".to_vec(),
                holes: vec![ascetic_ddd_command_log::record::Span {
                    offset: 0,
                    len: 8,
                    field: "/etag".into(),
                    hole: Hole::Captured {
                        subject: subject("group", 1),
                        slot: "etag".into(),
                        from: w0.step.clone(),
                    },
                    form: if raw { Form::Raw } else { Form::JsonToken },
                }],
            },
        });
        r
    };
    twins(
        Rule::Span,
        &[w0.clone(), spanned(false)],
        &[w0.clone(), spanned(true)],
    );
}
