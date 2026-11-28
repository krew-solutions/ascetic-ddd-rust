//! Z1 of ADR-0046: the golden log written compressed by the crate, on stdout,
//! for the zstd CLI to decode and compare with the plain golden log:
//!
//! ```text
//! cargo run -q -p ascetic-ddd-command-log --example zstd_golden | zstd -dc | cmp - crates/command-log/golden/c1.jsonl
//! ```

use std::io::Write;

use ascetic_ddd_command_log::{Compression, sample, write_file};

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c1.jsonl.zst");
    write_file(
        &path,
        sample::header(),
        sample::records(),
        Compression::default(),
    )
    .unwrap();
    std::io::stdout()
        .write_all(&std::fs::read(&path).unwrap())
        .unwrap();
}
