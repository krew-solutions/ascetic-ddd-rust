//! The scheduler's golden journal written and read in the host's build of
//! serde_json (ADR-0047 K1e-3b): the same bytes as the library's default
//! build. The build is the host's: `stream_ids.rs`'s `the_build_is_the_host_s`
//! checks it for this whole crate.

use std::io::Cursor;

use ascetic_ddd_scheduler::journal::{JOURNAL, Reader, Writer, sample};

const GOLDEN: &[u8] = include_bytes!("../../../crates/scheduler/golden/journal.jsonl");

#[test]
fn the_golden_journal_is_the_same_bytes_in_the_host_s_build() {
    let mut writer = Writer::new(Vec::new(), sample::header()).unwrap();
    for l in sample::lines() {
        writer.line(&l).unwrap();
    }
    assert_eq!(writer.finish().unwrap().0, GOLDEN);
    let reader = Reader::new(Cursor::new(GOLDEN)).unwrap();
    assert_eq!(reader.header(), &sample::header());
    let read: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
    assert_eq!(read, sample::lines());
    assert_eq!(JOURNAL.len(), 64);
}
