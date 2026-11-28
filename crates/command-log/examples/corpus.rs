//! V2 of ADR-0046: the corpus `tests/fixture/corpus.rs` builds, as JSON lines on
//! stdout, for the independent reference to decide again:
//!
//! ```text
//! cargo run -q -p ascetic-ddd-command-log --example corpus | python3 -I crates/command-log/vectors/reference.py
//! ```

#[path = "../tests/fixture/mod.rs"]
mod fixture;

fn main() {
    for line in fixture::corpus::lines() {
        println!("{line}");
    }
}
