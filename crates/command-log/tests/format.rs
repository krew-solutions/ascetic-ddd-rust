//! F1, F2 and F3 of ADR-0046: one byte form, pinned, and no shorter log
//! mistaken for the whole.

use std::io::Cursor;

use ascetic_ddd_command_log::record::{Line, Trailer};
use ascetic_ddd_command_log::{
    Compression, FORMAT, LogError, Reader, Writer, encode, sample, write_file,
};
use sha2::{Digest, Sha256};

const GOLDEN: &[u8] = include_bytes!("../golden/c1.jsonl");

fn golden_bytes() -> Vec<u8> {
    let mut writer = Writer::new(Vec::new(), sample::header()).unwrap();
    for r in sample::records() {
        writer.step(r).unwrap();
    }
    writer.finish().unwrap().0
}

fn read_all(bytes: &[u8]) -> Result<Vec<ascetic_ddd_command_log::record::Record>, LogError> {
    Reader::new(Cursor::new(bytes))?.collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// F2: the writer gives the golden bytes, and `FORMAT` is their digest — a
/// changed byte form changes the version in the same edit.
#[test]
fn the_golden_log_is_the_format() {
    assert_eq!(
        golden_bytes(),
        GOLDEN,
        "regenerate with the ignored `regenerate` test, then set FORMAT"
    );
    assert_eq!(hex(&Sha256::digest(GOLDEN)), FORMAT);
}

/// F1: every record and hole variant reads back as itself.
#[test]
fn every_variant_round_trips() {
    assert_eq!(read_all(GOLDEN).unwrap(), sample::records());
    let reader = Reader::new(Cursor::new(GOLDEN)).unwrap();
    assert_eq!(reader.header(), &sample::header());
}

/// F1: a u64 is never a JSON number — another reader would round it.
#[test]
fn large_integers_are_strings() {
    let text = std::str::from_utf8(GOLDEN).unwrap();
    assert!(text.contains(r#""index":"18446744073709551615""#));
    assert!(text.contains(r#""at":"18446744073709551615""#));
    assert!(text.contains(r#""offset":"-9223372036854775808""#));
    assert!(!text.contains(r#""at":18446744073709551615"#));
}

/// The golden log's lines, as text.
fn lines() -> Vec<String> {
    std::str::from_utf8(GOLDEN)
        .unwrap()
        .split_inclusive('\n')
        .map(str::to_owned)
        .collect()
}

/// A log with line `i` replaced, its trailer recomputed over the new bytes so
/// that only the line itself is wrong.
fn with_line(i: usize, replacement: &str) -> Vec<u8> {
    let mut lines = lines();
    lines[i] = replacement.to_owned();
    let last = lines.len() - 1;
    let body: String = lines[..last].concat();
    let trailer = Trailer {
        records: (last - 1) as u64,
        sha256: hex(&Sha256::digest(body.as_bytes())),
    };
    let mut bytes = body.into_bytes();
    bytes.extend(encode(&Line::Trailer(trailer)).unwrap());
    bytes
}

/// F1: a line in any other bytes than its canonical ones is refused, whatever
/// it decodes to.
#[test]
fn a_line_not_in_its_canonical_bytes_is_refused() {
    let step = lines()[1].clone();
    let variants = [
        step.replacen(":", ": ", 1),                     // whitespace
        step.replacen(r#""at":"0""#, r#""at":"00""#, 1), // a leading zero
        step.replacen(r#""at":"0""#, r#""at":0"#, 1),    // a number
        step.replacen(r#""kind":"tenant""#, r#""kind" : "tenant""#, 1), // spaces
        step.replacen("\n", "\r\n", 1),                  // a CR
        step.replacen(r#"{"step":{"step""#, r#"{"step":{"at":"0","step""#, 1), // a duplicate, reordered
    ];
    for (k, v) in variants.iter().enumerate() {
        assert_ne!(v, &step, "variant {k} changed nothing");
        let refused = read_all(&with_line(1, v));
        assert!(refused.is_err(), "variant {k} was accepted: {v}");
    }
    // Base64 in another alphabet, or unpadded.
    let bus = lines()
        .into_iter()
        .find(|l| l.contains("device.lifecycle"))
        .unwrap();
    let index = lines().iter().position(|l| l == &bus).unwrap();
    let scope = "+/+/Ag==";
    assert!(bus.contains(scope), "{bus}");
    for other in ["-_-_Ag==", "+/+/Ag"] {
        assert!(
            read_all(&with_line(index, &bus.replacen(scope, other, 1))).is_err(),
            "{other}"
        );
    }
}

/// F1: an unknown field is refused, not ignored: an older reader must not
/// drop a newer field's meaning.
#[test]
fn an_unknown_field_is_refused() {
    let step = lines()[1].clone();
    let added = step.replacen(r#""deviation":null"#, r#""deviation":null,"later":1"#, 1);
    assert_ne!(added, step);
    assert!(matches!(
        read_all(&with_line(1, &added)),
        Err(LogError::Malformed { .. })
    ));
}

/// F3: a log cut anywhere — at a line's end, inside a line — is refused, and so
/// is one that goes on after its trailer.
#[test]
fn a_truncated_log_is_refused() {
    for cut in 0..GOLDEN.len() {
        let prefix = &GOLDEN[..cut];
        let read = Reader::new(Cursor::new(prefix)).and_then(|r| r.collect::<Result<Vec<_>, _>>());
        assert!(read.is_err(), "a prefix of {cut} bytes was accepted");
    }
    let mut longer = GOLDEN.to_vec();
    longer.extend_from_slice(&lines()[1].clone().into_bytes());
    assert!(matches!(read_all(&longer), Err(LogError::AfterTrailer)));
    assert_eq!(
        read_all(&GOLDEN[..GOLDEN.len() - 1])
            .unwrap_err()
            .to_string(),
        "the log ends without its trailer"
    );
}

/// F3: the trailer is checked: a step dropped with its trailer kept, or a
/// trailer whose digest is off, is refused.
#[test]
fn the_trailer_is_checked() {
    let mut lines = lines();
    lines.remove(2);
    assert!(matches!(
        read_all(lines.concat().as_bytes()),
        Err(LogError::Trailer { .. })
    ));
    let mut lines = self::lines();
    let last = lines.len() - 1;
    lines[last] = lines[last]
        .replacen("\"sha256\":\"", "\"sha256\":\"0", 1)
        .replacen("0\"}", "\"}", 1);
    assert!(read_all(lines.concat().as_bytes()).is_err());
}

/// F3: a file is written whole or not at all.
#[test]
fn a_file_appears_only_when_complete() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("run.jsonl");
    let trailer = write_file(
        &path,
        sample::header(),
        sample::records(),
        Compression::None,
    )
    .unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), GOLDEN);
    assert_eq!(trailer.records, sample::records().len() as u64);
    assert!(!dir.path().join("run.jsonl.partial").exists());

    let mut wrong = sample::header();
    wrong.format += 1;
    let failed = dir.path().join("failed.jsonl");
    assert!(matches!(
        write_file(&failed, wrong, sample::records(), Compression::None),
        Err(LogError::Format(_))
    ));
    assert!(!failed.exists());
    assert!(!dir.path().join("failed.jsonl.partial").exists());
}

/// Writes `golden/c1.jsonl` from the sample as it is. Run only on purpose, then
/// set `FORMAT` to the digest it prints: a changed byte form is a new version.
#[test]
#[ignore = "regenerates golden/c1.jsonl"]
fn regenerate() {
    let bytes = golden_bytes();
    std::fs::write(
        concat!(env!("CARGO_MANIFEST_DIR"), "/golden/c1.jsonl"),
        &bytes,
    )
    .unwrap();
    println!("FORMAT = {}", hex(&Sha256::digest(&bytes)));
}
