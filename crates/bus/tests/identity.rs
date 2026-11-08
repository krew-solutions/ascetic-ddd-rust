//! A stream id read and written in the workspace's build (ADR-0037 decision
//! 1): the golden table, and the writer as the reader's inverse over every
//! value in the domain D (`golden/stream_ids.rs`). `verify/inbox-features`
//! runs the same in the host's build.

#[path = "golden/stream_ids.rs"]
mod golden;

#[test]
fn the_golden_table_reads_as_it_should() {
    golden::check();
}

#[test]
fn the_writer_writes_one_text_for_one_value() {
    golden::check_written();
}
