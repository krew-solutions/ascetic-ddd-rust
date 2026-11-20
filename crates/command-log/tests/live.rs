//! L1 of ADR-0046 (decision 15): a live run offers only what a dry run
//! validated — refused before it emits on a stale commitment, before it writes
//! a line on a different emission, stopped before the first chunk that differs.

mod fixture;

use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ascetic_ddd_command_log::engine::{Config, EngineError, Unit, generate};
use ascetic_ddd_command_log::live::{Commitment, dry, live, validator};
use ascetic_ddd_command_log::record::{Header, On, Record};
use ascetic_ddd_command_log::validate::NoHost;
use fixture::*;
use sha2::{Digest, Sha256};

/// Devices `0..count`, each managed and then telling a fact; counts its runs.
struct Devices {
    count: u64,
    runs: AtomicUsize,
}

impl Devices {
    fn new(count: u64) -> Devices {
        Devices {
            count,
            runs: AtomicUsize::new(0),
        }
    }
}

impl Unit for Devices {
    fn emit(
        &self,
        out: &mut dyn FnMut(Record) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        for d in 0..self.count {
            let managed = step(id("device", d, "managed", 0), d * 10);
            let fact = after(
                step(id("device", d, "seen", 0), d * 10 + 5),
                &managed.step,
                On::Expected,
            );
            out(managed)?;
            out(fact)?;
        }
        Ok(())
    }
}

fn config(spill: &Path) -> Config {
    Config {
        threads: 2,
        budget: 512,
        cell: 50,
        cells: 8,
        spill: spill.to_owned(),
        fan_in: 0,
        memory_spill: true,
    }
}

/// A writer into a buffer the test keeps, whatever happens to the writer.
#[derive(Clone, Default)]
struct Shared(Arc<Mutex<Vec<u8>>>);

impl Write for Shared {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Shared {
    fn bytes(&self) -> Vec<u8> {
        self.0.lock().unwrap().clone()
    }
}

const CHUNK: u64 = 4;

/// A dry run's commitment for `units`, and the log a persisted generation
/// writes.
fn committed(dir: &Path, units: &[&dyn Unit]) -> (Commitment, Vec<u8>) {
    let path = dir.join("plan.commitment");
    let report = dry(
        header(),
        units,
        &config(dir),
        &NoHost,
        &limits_in(dir),
        "build-1",
        CHUNK,
        &path,
    )
    .unwrap();
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    let log = generate(header(), units, &config(dir), Vec::new())
        .unwrap()
        .0;
    (Commitment::read(&path).unwrap(), log)
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The first `n` lines of `log`.
fn head(log: &[u8], n: usize) -> &[u8] {
    let mut seen = 0;
    for (i, b) in log.iter().enumerate() {
        if *b == b'\n' {
            seen += 1;
            if seen == n {
                return &log[..=i];
            }
        }
    }
    log
}

/// A live run delivers the very log a persisted generation writes; its prefix
/// digests are `head -n (1 + i·chunk) | sha256sum` of that log.
#[test]
fn a_live_run_is_the_validated_log() {
    let dir = tempfile::tempdir().unwrap();
    let plan = Devices::new(9);
    let units: [&dyn Unit; 1] = [&plan];
    let (commitment, log) = committed(dir.path(), &units);
    assert_eq!(commitment.records, 18);
    assert_eq!(commitment.prefixes.len(), 4);
    assert_eq!(commitment.header, sha256_hex(head(&log, 1)));
    for (i, p) in commitment.prefixes.iter().enumerate() {
        assert_eq!(
            p,
            &sha256_hex(head(&log, 1 + (i + 1) * CHUNK as usize)),
            "P{}",
            i + 1
        );
    }
    let out = Shared::default();
    let (_, trailer) = live(
        header(),
        &units,
        &config(dir.path()),
        &commitment,
        "build-1",
        out.clone(),
    )
    .unwrap();
    assert!(out.bytes() == log);
    assert_eq!(trailer.sha256, commitment.sha256);
    assert_eq!(
        leftover(dir.path()),
        vec![dir.path().join("plan.commitment")]
    );
}

/// A stale commitment is refused before a unit runs: another header, format,
/// validator or fingerprint.
#[test]
fn a_stale_commitment_is_refused_before_emitting() {
    let dir = tempfile::tempdir().unwrap();
    let plan = Devices::new(3);
    let units: [&dyn Unit; 1] = [&plan];
    let (commitment, _) = committed(dir.path(), &units);
    let runs = plan.runs.load(Ordering::SeqCst);
    let mut other: Header = header();
    other.revision = "another".into();
    let refusals = [
        (other, commitment.clone(), "build-1", "the header"),
        (
            header(),
            Commitment {
                format: "0".into(),
                ..commitment.clone()
            },
            "build-1",
            "the format",
        ),
        (
            header(),
            Commitment {
                validator: "0".into(),
                ..commitment.clone()
            },
            "build-1",
            "the validator",
        ),
        (header(), commitment.clone(), "build-2", "the fingerprint"),
    ];
    for (h, c, fingerprint, what) in refusals {
        let out = Shared::default();
        match live(h, &units, &config(dir.path()), &c, fingerprint, out.clone()) {
            Err(EngineError::Diverged { what: w }) => assert_eq!(w, what),
            other => panic!("{what}: {:?}", other.map(|_| ())),
        }
        assert!(out.bytes().is_empty(), "{what}");
    }
    assert_eq!(plan.runs.load(Ordering::SeqCst), runs, "a unit ran");
}

/// A plan that emits otherwise is refused before a line is written.
#[test]
fn another_emission_is_refused_before_a_line() {
    let dir = tempfile::tempdir().unwrap();
    let plan = Devices::new(5);
    let (commitment, _) = committed(dir.path(), &[&plan]);
    let changed = Devices::new(6);
    let out = Shared::default();
    match live(
        header(),
        &[&changed],
        &config(dir.path()),
        &commitment,
        "build-1",
        out.clone(),
    ) {
        Err(EngineError::Diverged { what }) => assert_eq!(what, "what was emitted"),
        other => panic!("{:?}", other.map(|_| ())),
    }
    assert!(out.bytes().is_empty());
}

/// A chunk whose digest differs stops the run before it: the chunks before it
/// are delivered, it and those after are not; a last chunk that differs is
/// held back too.
#[test]
fn a_differing_chunk_is_never_released() {
    let dir = tempfile::tempdir().unwrap();
    let plan = Devices::new(9);
    let units: [&dyn Unit; 1] = [&plan];
    let (commitment, log) = committed(dir.path(), &units);
    for i in 1..=commitment.prefixes.len() {
        let mut forged = commitment.clone();
        forged.prefixes[i - 1] = "0".repeat(64);
        let out = Shared::default();
        match live(
            header(),
            &units,
            &config(dir.path()),
            &forged,
            "build-1",
            out.clone(),
        ) {
            Err(EngineError::Diverged { what }) => assert_eq!(what, format!("chunk {i}")),
            other => panic!("chunk {i}: {:?}", other.map(|_| ())),
        }
        assert!(
            out.bytes() == head(&log, 1 + (i - 1) * CHUNK as usize),
            "chunk {i}"
        );
    }
    let forged = Commitment {
        sha256: "0".repeat(64),
        ..commitment.clone()
    };
    let out = Shared::default();
    match live(
        header(),
        &units,
        &config(dir.path()),
        &forged,
        "build-1",
        out.clone(),
    ) {
        Err(EngineError::Diverged { what }) => assert_eq!(what, "chunk 5, the last"),
        other => panic!("{:?}", other.map(|_| ())),
    }
    assert!(out.bytes() == head(&log, 1 + 4 * CHUNK as usize));
}

struct Refusing;

impl Unit for Refusing {
    fn emit(
        &self,
        _: &mut dyn FnMut(Record) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        Err(EngineError::Unit("no law for this scope".into()))
    }
}

/// A dry run commits only a clean report — a plan that breaks a rule leaves no
/// commitment, and its report is returned — and a generation that fails is
/// reported as itself, not as the cut stream the validator saw.
#[test]
fn a_dry_run_commits_only_what_is_clean() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("plan.commitment");
    let twice = step(id("device", 1, "seen", 0), 1);
    struct Dup(Record);
    impl Unit for Dup {
        fn emit(
            &self,
            out: &mut dyn FnMut(Record) -> Result<(), EngineError>,
        ) -> Result<(), EngineError> {
            out(self.0.clone())?;
            let mut later = self.0.clone();
            later.at += 1;
            out(later)
        }
    }
    let report = dry(
        header(),
        &[&Dup(twice)],
        &config(dir.path()),
        &NoHost,
        &limits_in(dir.path()),
        "b",
        CHUNK,
        &path,
    )
    .unwrap();
    assert!(!report.violations.is_empty());
    assert!(!path.exists());
    let plan = Devices::new(3);
    match dry(
        header(),
        &[&plan, &Refusing],
        &config(dir.path()),
        &NoHost,
        &limits_in(dir.path()),
        "b",
        CHUNK,
        &path,
    ) {
        Err(EngineError::Unit(reason)) => assert_eq!(reason, "no law for this scope"),
        other => panic!("{:?}", other.map(|_| ())),
    }
    assert!(!path.exists());
}

/// The validator's identity: its rules' revision and names. A rule added or
/// renamed changes it; a rule's meaning changed must bump the revision.
#[test]
fn the_validator_s_identity_is_its_rules() {
    assert_eq!(
        validator(),
        "2: header order duplicate edge barrier producer stale writers stream gap span profile operation required deviation rate key"
    );
}

struct One;

impl Unit for One {
    fn emit(
        &self,
        out: &mut dyn FnMut(Record) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        out(step(id("tenant", 1, "joined", 0), 7))
    }
}

/// The trailer is no step: 19 steps in chunks of 4 make 4 whole chunks, not 5.
#[test]
fn the_trailer_is_not_a_chunk_s_line() {
    let dir = tempfile::tempdir().unwrap();
    let plan = Devices::new(9);
    let units: [&dyn Unit; 2] = [&plan, &One];
    let (commitment, log) = committed(dir.path(), &units);
    assert_eq!(commitment.records, 19);
    assert_eq!(commitment.prefixes.len(), 4);
    assert_eq!(commitment.sha256, sha256_hex(head(&log, 20)));
}
