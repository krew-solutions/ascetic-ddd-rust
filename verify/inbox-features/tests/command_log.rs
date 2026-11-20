//! The command log's golden log written and read in the host's build of
//! serde_json (ADR-0046, F4): the same bytes as the library's default build.
//! The build is the host's: `stream_ids.rs`'s `the_build_is_the_host_s` checks
//! it for this whole crate.

use std::io::Cursor;

use ascetic_ddd_command_log::{FORMAT, Reader, Writer, sample};

const GOLDEN: &[u8] = include_bytes!("../../../crates/command-log/golden/c1.jsonl");

#[test]
fn the_golden_log_is_the_same_bytes_in_the_host_s_build() {
    let mut writer = Writer::new(Vec::new(), sample::header()).unwrap();
    for r in sample::records() {
        writer.step(r).unwrap();
    }
    assert_eq!(writer.finish().unwrap().0, GOLDEN);
    let read: Vec<_> = Reader::new(Cursor::new(GOLDEN))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(read, sample::records());
    assert_eq!(FORMAT.len(), 64);
}
