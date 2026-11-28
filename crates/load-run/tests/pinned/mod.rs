//! The headers admission journals, pinned (ADR-0047 decision 22,
//! K2b-7e-2b-3d-3b-4b): `golden/admitted/*.jsonl`, each a header alone and
//! its trailer, settled at every change to admission's section by its diff and
//! folded by both of the scheduler's folds. Shared by the tests that make them.

#![allow(dead_code)]

use ascetic_ddd_command_log::fills::Namespace;
use ascetic_ddd_load_run::carrier::Carrier;
use ascetic_ddd_load_run::clock::Clock;
use ascetic_ddd_load_run::live::Live;
use ascetic_ddd_load_run::run::{Fitted, admit};
use ascetic_ddd_load_run::stages::{Identity, journal_header};
use ascetic_ddd_scheduler::journal::{DescriptorsRead, Writer};

/// A fitted run admitted on the runtimes built for it and journaled as a
/// header alone, then its trailer: its identity fixed, its descriptors — the
/// process's as read — replaced by fixed ones where it read them, the rest
/// admission's own.
pub fn header<C: Clock, X: Carrier<C>>(fitted: Fitted<C, X>) -> Vec<u8> {
    let live = Live::fitted(&fitted).unwrap();
    let admitted = admit(fitted, live.runtimes()).unwrap();
    let mut header = journal_header(
        &admitted,
        Identity {
            source: "pinned".into(),
            log: "ab".repeat(32),
            validator: "a test's validator".into(),
            namespace: Namespace(1),
        },
        Some(1_763_640_000_000_000),
        vec![],
        None,
    );
    if let Some(d) = header
        .admission
        .as_mut()
        .and_then(|a| a.descriptors.as_mut())
    {
        *d = DescriptorsRead {
            soft: 1_024,
            hard: 524_288,
            open: 32,
            raised: false,
        };
    }
    Writer::new(Vec::new(), header).unwrap().finish().unwrap().0
}

fn path(name: &str) -> String {
    format!(
        "{}/golden/admitted/{name}.jsonl",
        env!("CARGO_MANIFEST_DIR")
    )
}

/// `bytes` are the pinned header `name`'s.
pub fn check(name: &str, bytes: &[u8]) {
    let kept = std::fs::read(path(name)).unwrap_or_else(|e| {
        panic!("{name}: {e}: regenerate with the ignored `regenerate_admitted`")
    });
    assert_eq!(
        String::from_utf8_lossy(&kept),
        String::from_utf8_lossy(bytes),
        "{name}: settle the change line by line, then regenerate with the ignored `regenerate_admitted`"
    );
}

/// Pins `bytes` as the header `name`'s. Run only on purpose, after settling.
pub fn write(name: &str, bytes: &[u8]) {
    std::fs::create_dir_all(format!("{}/golden/admitted", env!("CARGO_MANIFEST_DIR"))).unwrap();
    std::fs::write(path(name), bytes).unwrap();
}
