//! The journal's bytes (ADR-0047 decision 14, K1e-3b): one byte form, pinned
//! by its golden, which holds every line and variant; no shorter journal and
//! no other version mistaken for the whole.

// The command log's fixture reads its corpus's directory; the lint gate refuses
// files to the machine, not to its tests (ADR-0047 K1e-3).
#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use std::collections::BTreeSet;
use std::io::Cursor;

use ascetic_ddd_command_log::record::On;
use ascetic_ddd_scheduler::journal::{
    self, Critical, Dep, End, FaultClass, Header, Ignored, JOURNAL, JOURNAL_VERSION, JournalError,
    Judged, Line, Reader, Skip, Writer, sample,
};
use sha2::{Digest, Sha256};

const GOLDEN: &[u8] = include_bytes!("../golden/journal.jsonl");

fn write(header: Header, lines: &[Line]) -> Vec<u8> {
    let mut writer = Writer::new(Vec::new(), header).unwrap();
    for l in lines {
        writer.line(l).unwrap();
    }
    writer.finish().unwrap().0
}

fn golden_bytes() -> Vec<u8> {
    write(sample::header(), &sample::lines())
}

fn read_all(bytes: &[u8]) -> Result<(Header, Vec<Line>), JournalError> {
    let reader = Reader::new(Cursor::new(bytes.to_vec()))?;
    let header = reader.header().clone();
    Ok((header, reader.collect::<Result<_, _>>()?))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The writer gives the golden bytes, and `JOURNAL` is their digest — a changed
/// byte form changes the pin in the same edit.
#[test]
fn the_golden_journal_is_the_byte_form() {
    assert_eq!(
        golden_bytes(),
        GOLDEN,
        "regenerate with the ignored `regenerate` test, then set JOURNAL"
    );
    assert_eq!(hex(&Sha256::digest(GOLDEN)), JOURNAL);
}

/// Every line and variant reads back as itself.
#[test]
fn every_line_round_trips() {
    let (header, lines) = read_all(GOLDEN).unwrap();
    assert_eq!(header, sample::header());
    assert_eq!(lines, sample::lines());
}

// Each enum's variants, by an exhaustive match: a variant added fails to
// compile here until it is given a number, and the golden fails until it holds
// it.
fn line_kind(l: &Line) -> usize {
    match l {
        Line::Header(_) => 0,
        Line::Read(_) => 1,
        Line::Released(_) => 2,
        Line::Dispatch(_) => 3,
        Line::Sent(_) => 4,
        Line::Answer(_) => 5,
        Line::End(_) => 6,
        Line::Fault(_) => 7,
        Line::Trailer(_) => 8,
    }
}

fn dep_kind(d: &Dep) -> usize {
    match d {
        Dep::Step { .. } => 0,
        Dep::Each { .. } => 1,
    }
}

fn on_kind(on: On) -> usize {
    match on {
        On::Sent => 0,
        On::Expected => 1,
        On::Settled => 2,
    }
}

fn judged_kind(j: Judged) -> usize {
    match j {
        Judged::Expected => 0,
        Judged::Unexpected => 1,
        Judged::NotTaken => 2,
        Judged::Unknown => 3,
    }
}

fn ignored_kind(i: Option<Ignored>) -> usize {
    match i {
        None => 0,
        Some(Ignored::Ended) => 1,
        Some(Ignored::Stale) => 2,
        Some(Ignored::Late) => 3,
    }
}

fn end_kind(e: End) -> usize {
    match e {
        End::Expected => 0,
        End::Unexpected => 1,
        End::Unknown => 2,
        End::Skipped(Skip::Dependency(_)) => 3,
        End::Skipped(Skip::Member(_)) => 4,
        End::Skipped(Skip::MissingCapture(_)) => 5,
    }
}

fn critical_kind(c: Option<Critical>) -> usize {
    match c {
        None => 0,
        Some(Critical::Step(_)) => 1,
        Some(Critical::Member(_)) => 2,
    }
}

fn fault_kind(f: FaultClass) -> usize {
    match f {
        FaultClass::Backstop => 0,
    }
}

/// The golden holds every variant of every enum its lines carry, both sides
/// of every optional field, and a `u64` at its top in every kind of field.
#[test]
fn the_golden_holds_every_variant() {
    let (_, lines) = read_all(GOLDEN).unwrap();
    let mut seen: [BTreeSet<usize>; 10] = Default::default();
    // The header and the trailer are lines of every journal.
    seen[0].extend([0, 8]);
    let mut options = BTreeSet::new();
    for l in &lines {
        seen[0].insert(line_kind(l));
        match l {
            Line::Read(r) => {
                for d in &r.after {
                    seen[1].insert(dep_kind(d));
                    let (Dep::Step { on, .. } | Dep::Each { on, .. }) = d;
                    seen[2].insert(on_kind(*on));
                }
            }
            Line::Answer(a) => {
                seen[3].insert(judged_kind(a.judged));
                seen[4].insert(ignored_kind(a.ignored));
                options.insert(("captured", !a.captured.is_empty()));
            }
            Line::End(e) => {
                seen[5].insert(end_kind(e.end));
                seen[6].insert(critical_kind(e.critical));
                options.insert(("dispatched", e.dispatched.is_some()));
                options.insert(("sent", e.sent.is_some()));
                options.insert(("bound", !e.bound.is_empty()));
                options.insert(("fault", e.fault));
            }
            Line::Fault(f) => {
                seen[7].insert(fault_kind(f.class));
            }
            Line::Header(_) | Line::Released(_) | Line::Dispatch(_) | Line::Sent(_) => {}
            Line::Trailer(_) => unreachable!("the reader hands no trailer out"),
        }
    }
    let counts = [9, 2, 3, 4, 4, 6, 3, 1];
    for (i, n) in counts.iter().enumerate() {
        assert_eq!(seen[i], (0..*n).collect(), "enum {i}");
    }
    for name in ["captured", "dispatched", "sent", "bound", "fault"] {
        assert!(
            options.contains(&(name, true)) && options.contains(&(name, false)),
            "{name}"
        );
    }
    let text = std::str::from_utf8(GOLDEN).unwrap();
    for field in [
        "at",
        "line",
        "members",
        "deadline",
        "index",
        "read_ahead",
        "sent",
    ] {
        assert!(
            text.contains(&format!(r#""{field}":"18446744073709551615""#)),
            "{field}"
        );
    }
    assert!(text.contains(r#""bound":["2","18446744073709551615"]"#));
    assert!(text.contains(r#""missing_capture":"18446744073709551615""#));
    assert!(!text.contains(":18446744073709551615"));
}

fn golden_lines() -> Vec<String> {
    std::str::from_utf8(GOLDEN)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect()
}

fn joined(lines: &[String]) -> Vec<u8> {
    let mut bytes = lines.join("\n").into_bytes();
    bytes.push(b'\n');
    bytes
}

/// A run killed partway: every strict prefix of the golden's bytes is refused,
/// none read as a shorter run.
#[test]
fn a_truncated_journal_is_refused() {
    for cut in 0..GOLDEN.len() {
        let error = read_all(&GOLDEN[..cut]).unwrap_err();
        assert!(
            match error {
                JournalError::Header { line: 1 } => cut == 0,
                JournalError::Truncated => cut > 0,
                _ => false,
            },
            "cut at {cut}: {error:?}"
        );
    }
}

/// The trailer commits to every line before it: a line dropped, swapped or
/// changed, or a trailer that miscounts, is refused.
#[test]
fn the_trailer_is_checked() {
    let lines = golden_lines();
    let n = lines.len();
    let mut dropped = lines.clone();
    dropped.remove(3);
    let mut swapped = lines.clone();
    swapped.swap(3, 4);
    let mut changed = lines.clone();
    changed[3] = changed[3].replacen(r#""at":"1""#, r#""at":"2""#, 1);
    assert_ne!(changed, lines);
    for (name, bytes) in [
        ("dropped", joined(&dropped)),
        ("swapped", joined(&swapped)),
        ("changed", joined(&changed)),
    ] {
        assert!(
            matches!(read_all(&bytes), Err(JournalError::Trailer { .. })),
            "{name}"
        );
    }
    let mut miscounted = lines.clone();
    miscounted[n - 1] = miscounted[n - 1].replacen(r#""steps":"7""#, r#""steps":"6""#, 1);
    assert_ne!(miscounted, lines);
    assert!(matches!(
        read_all(&joined(&miscounted)),
        Err(JournalError::Trailer { .. })
    ));
}

/// The header is the first line and only it; nothing follows the trailer.
#[test]
fn the_header_and_the_trailer_bound_the_journal() {
    let lines = golden_lines();
    let mut second = lines.clone();
    second.insert(2, lines[0].clone());
    assert!(matches!(
        read_all(&joined(&second)),
        Err(JournalError::Header { line: 3 })
    ));
    let mut headless = lines.clone();
    headless.remove(0);
    assert!(matches!(
        read_all(&joined(&headless)),
        Err(JournalError::Header { line: 1 })
    ));
    let mut after = lines.clone();
    after.push(lines[1].clone());
    assert!(matches!(
        read_all(&joined(&after)),
        Err(JournalError::AfterTrailer)
    ));
    // A writer hands out no header or trailer of its own making.
    let mut writer = Writer::new(Vec::new(), sample::header()).unwrap();
    assert!(
        writer
            .line(&Line::Header(Box::new(sample::header())))
            .is_err()
    );
}

/// Another version is refused by the writer and the reader alike.
#[test]
fn another_version_is_refused() {
    let other = Header {
        journal: JOURNAL_VERSION + 1,
        ..sample::header()
    };
    assert!(matches!(
        Writer::new(Vec::new(), other),
        Err(JournalError::Version(v)) if v == JOURNAL_VERSION + 1
    ));
    let mut lines = golden_lines();
    lines[0] = lines[0].replacen(
        &format!(r#""journal":{JOURNAL_VERSION}"#),
        &format!(r#""journal":{}"#, JOURNAL_VERSION + 1),
        1,
    );
    assert!(matches!(
        read_all(&joined(&lines)),
        Err(JournalError::Version(_))
    ));
}

/// One byte form: a line that parses but is not in its canonical bytes, an
/// unknown field, a number where a decimal string belongs — each refused.
#[test]
fn a_line_not_in_its_canonical_bytes_is_refused() {
    let lines = golden_lines();
    let cases = [
        (
            lines[3].replacen(r#"{"released":{"#, r#"{ "released":{"#, 1),
            "space",
        ),
        (
            lines[3].replacen(r#""at":"1","#, r#""at":"1","extra":0,"#, 1),
            "unknown field",
        ),
        (lines[3].replacen(r#""at":"1""#, r#""at":1"#, 1), "a number"),
        (
            lines[3].replacen(r#""at":"1""#, r#""at":"01""#, 1),
            "a leading zero",
        ),
    ];
    for (line, name) in cases {
        assert_ne!(line, lines[3], "{name}");
        let mut changed = lines.clone();
        changed[3] = line;
        let error = read_all(&joined(&changed)).unwrap_err();
        assert!(
            matches!(
                error,
                JournalError::Frame(
                    ascetic_ddd_command_log::frame::FrameError::NotCanonical { line: 4 }
                        | ascetic_ddd_command_log::frame::FrameError::Malformed { line: 4, .. }
                )
            ),
            "{name}: {error:?}"
        );
    }
}

/// `journal::events` tells one step's story in order: read, released,
/// dispatched, sent, answered, ended.
#[test]
fn a_step_s_story_is_told_in_order() {
    use ascetic_ddd_scheduler::sim::{Attempt, run};
    let (name, records) = fixture::corpus::logs()
        .into_iter()
        .find(|(_, r)| r.iter().all(|r| r.after.is_empty()))
        .expect("a corpus log of independent steps");
    let first = vec![records[0].clone()];
    let ran = run(fixture::corpus::corpus_header(), &first, 0, &|_, _, _| {
        Attempt::expected(100)
    })
    .unwrap();
    let kinds: Vec<usize> = ran.journal.iter().map(line_kind).collect();
    assert_eq!(kinds, vec![1, 2, 3, 4, 5, 6], "{name}: {:?}", ran.journal);
    let _ = journal::JOURNAL_VERSION;
}

/// An answer the machine took nothing from is journaled with why: one to an
/// attempt a retry replaced, one past its deadline, one to a step that ended.
#[test]
fn an_answer_taken_nowhere_is_marked_with_why() {
    use std::sync::Arc;

    use ascetic_ddd_scheduler::machine::{GRACE, Input, Judgement, Machine, Output};
    let r = Arc::new(fixture::call("read", 1, 0, 0, 0));
    let mut header = fixture::header();
    for o in &mut header.operations {
        o.safety = ascetic_ddd_command_log::record::Safety::Idempotent;
    }
    let mut m = Machine::new(Arc::new(header), fixture::W).unwrap();
    let mut lines = Vec::new();
    let mut step = |m: &mut Machine, at: u64, input: Input| {
        let out = m.step(at, input.clone()).unwrap();
        lines.extend(journal::events(at, &input, &out));
        out
    };
    let dispatched = |out: &[Output], attempt: u32| {
        out.iter().find_map(|o| match o {
            Output::Dispatch {
                attempt: a,
                deadline,
                ..
            } if *a == attempt => Some(*deadline),
            _ => None,
        })
    };
    let wake = |out: &[Output]| {
        out.iter().find_map(|o| match o {
            Output::WakeAt(t) => Some(*t),
            _ => None,
        })
    };
    let ended = |line: u64, attempt: u32, judgement: Judgement| Input::Ended {
        line,
        attempt,
        judgement,
    };
    let out = step(&mut m, 0, Input::Line { line: 2, record: r });
    assert!(dispatched(&out, 1).is_some());
    // The first attempt unknown before its deadline: an idempotent call is
    // resent.
    let mut out = step(&mut m, 1, ended(2, 1, Judgement::Unknown));
    let mut at = 1;
    let second = loop {
        if let Some(d) = dispatched(&out, 2) {
            break d;
        }
        at = wake(&out).expect("a read resent after an unknown attempt");
        out = step(&mut m, at, Input::Wake);
    };
    step(&mut m, at, ended(2, 1, Judgement::Expected(vec![])));
    step(&mut m, second + 1, ended(2, 2, Judgement::Expected(vec![])));
    step(&mut m, second.saturating_add(GRACE), Input::Wake);
    step(
        &mut m,
        second.saturating_add(GRACE) + 1,
        ended(2, 2, Judgement::Unexpected),
    );
    let answers: Vec<(u32, Option<Ignored>)> = lines
        .iter()
        .filter_map(|l| match l {
            Line::Answer(a) => Some((a.attempt, a.ignored)),
            _ => None,
        })
        .collect();
    assert_eq!(
        answers,
        vec![
            (1, None),
            (1, Some(Ignored::Stale)),
            (2, Some(Ignored::Late)),
            (2, Some(Ignored::Ended)),
        ],
        "{lines:?}"
    );
    // The backstop ended it: a fault, journaled after its end.
    let tail: Vec<usize> = lines.iter().map(line_kind).rev().take(3).collect();
    assert_eq!(tail, vec![5, 7, 6], "{lines:?}");
}

/// Writes `golden/journal.jsonl` from the sample as it is. Run only on purpose,
/// then set `JOURNAL` to its digest.
#[test]
#[ignore = "regenerates golden/journal.jsonl"]
#[expect(
    clippy::disallowed_methods,
    reason = "the golden is a file (ADR-0047 K1e-3 refuses files to the machine, not its tests)"
)]
fn regenerate() {
    let bytes = golden_bytes();
    std::fs::write(
        concat!(env!("CARGO_MANIFEST_DIR"), "/golden/journal.jsonl"),
        &bytes,
    )
    .unwrap();
    eprintln!("JOURNAL = {}", hex(&Sha256::digest(&bytes)));
}
