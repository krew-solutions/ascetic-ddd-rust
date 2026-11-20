//! E3, E5 and E6 of ADR-0046 (decision 15): a generation or a validation that
//! fails halfway says so and leaves nothing behind; a run cut short is never
//! read as fewer steps; a pass killed outright is swept by the next.

mod fixture;

use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use ascetic_ddd_command_log::engine::{Config, EngineError, Unit, generate};
use ascetic_ddd_command_log::record::{On, Record, StepId};
use ascetic_ddd_command_log::validate::{Host, Limits, NoHost, validate};
use ascetic_ddd_command_log::{LogError, Reader};
use fixture::*;

fn config(spill: &Path, threads: usize, budget: usize) -> Config {
    Config {
        threads,
        budget,
        cell: 1_000,
        cells: 4,
        spill: spill.to_owned(),
        fan_in: 0,
        memory_spill: true,
        progress: None,
        pipe_blocks: 64,
    }
}

/// The run files under a spill directory's passes.
fn run_files(spill: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = leftover(spill)
        .into_iter()
        .filter(|p| p.is_dir())
        .flat_map(|d| std::fs::read_dir(d).unwrap().map(|e| e.unwrap().path()))
        .filter(|p| p.extension().is_some_and(|x| x == "run"))
        .collect();
    out.sort();
    out
}

/// Three steps, each spilled as a run of its own (a budget of one byte).
struct Steps;

impl Unit for Steps {
    fn emit(
        &self,
        out: &mut dyn FnMut(Record) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        for i in 0..3 {
            out(step(id("device", i, "seen", 0), i * 10))?;
        }
        Ok(())
    }
}

/// Cuts the first run to `len` bytes while the generation goes on.
struct Cut {
    spill: PathBuf,
    len: Option<u64>,
}

impl Unit for Cut {
    fn emit(
        &self,
        _: &mut dyn FnMut(Record) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        if let (Some(len), Some(run)) = (self.len, run_files(&self.spill).first()) {
            std::fs::OpenOptions::new()
                .write(true)
                .open(run)
                .unwrap()
                .set_len(len)
                .unwrap();
        }
        Ok(())
    }
}

/// E3: a spill run cut at any byte — at a record's boundary too, or to nothing —
/// fails the generation, and nothing is left behind.
#[test]
fn a_cut_spill_fails_the_generation() {
    let dir = tempfile::tempdir().unwrap();
    // The size of a one-step run, from a pass that measures its first run.
    struct Measure<'a>(PathBuf, &'a std::sync::atomic::AtomicU64);
    impl Unit for Measure<'_> {
        fn emit(
            &self,
            _: &mut dyn FnMut(Record) -> Result<(), EngineError>,
        ) -> Result<(), EngineError> {
            let run = run_files(&self.0).first().cloned().unwrap();
            self.1
                .store(std::fs::metadata(run).unwrap().len(), Ordering::SeqCst);
            Ok(())
        }
    }
    let size = std::sync::atomic::AtomicU64::new(0);
    let measure = Measure(dir.path().to_owned(), &size);
    let both: [&dyn Unit; 2] = [&Steps, &measure];
    generate(header(), &both, &config(dir.path(), 1, 1), Vec::new()).unwrap();
    let whole = size.load(Ordering::SeqCst);
    assert!(whole > 8, "{whole}");
    for len in 0..whole {
        let cut = Cut {
            spill: dir.path().to_owned(),
            len: Some(len),
        };
        let units: [&dyn Unit; 2] = [&Steps, &cut];
        let result = generate(header(), &units, &config(dir.path(), 1, 1), Vec::new());
        assert!(
            matches!(
                result,
                Err(EngineError::Io {
                    phase: "merging",
                    ..
                })
            ),
            "cut to {len} of {whole}: {:?}",
            result.map(|_| ())
        );
        assert_eq!(leftover(dir.path()), Vec::<PathBuf>::new(), "cut to {len}");
    }
}

/// A host that cuts the validator's first id run at a given line.
struct CutIds {
    spill: PathBuf,
    at: StepId,
    len: u64,
}

impl Host for CutIds {
    fn required(&self, record: &Record) -> Vec<(StepId, On)> {
        if record.step == self.at
            && let Some(run) = run_files(&self.spill).first()
        {
            std::fs::OpenOptions::new()
                .write(true)
                .open(run)
                .unwrap()
                .set_len(self.len)
                .unwrap();
        }
        vec![]
    }
}

/// E3 for the validator: an id run cut at any byte fails the pass, never hides
/// a duplicate.
#[test]
fn a_cut_id_run_fails_the_validation() {
    let records: Vec<Record> = (0..4)
        .map(|i| step(id("device", i, "seen", 0), i))
        .collect();
    let bytes = bytes(header(), &records);
    for len in 0..24 {
        let dir = tempfile::tempdir().unwrap();
        let host = CutIds {
            spill: dir.path().to_owned(),
            at: records[3].step.clone(),
            len,
        };
        let limits = Limits {
            ids_in_memory: 1,
            spill: dir.path().to_owned(),
            kept: 10,
            fan_in: 0,
            memory_spill: true,
        };
        let result = validate(Reader::new(Cursor::new(&bytes)).unwrap(), &host, &limits);
        assert!(
            matches!(result, Err(LogError::Io(_))),
            "cut to {len}: {:?}",
            result.map(|_| ())
        );
        assert_eq!(leftover(dir.path()), Vec::<PathBuf>::new(), "cut to {len}");
    }
}

struct Panicking;

impl Unit for Panicking {
    fn emit(
        &self,
        out: &mut dyn FnMut(Record) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        out(step(id("device", 1, "seen", 0), 1))?;
        panic!("a defect in the host's plan");
    }
}

/// E5: a panicking unit fails the generation with its message and leaves
/// nothing; so does a validation whose input ends early.
#[test]
fn a_failure_leaves_nothing_behind() {
    let dir = tempfile::tempdir().unwrap();
    let units: [&dyn Unit; 2] = [&Steps, &Panicking];
    match generate(header(), &units, &config(dir.path(), 2, 1), Vec::new()) {
        Err(EngineError::Unit(reason)) => {
            assert!(reason.contains("a defect in the host's plan"), "{reason}")
        }
        other => panic!("{:?}", other.map(|_| ())),
    }
    assert_eq!(leftover(dir.path()), Vec::<PathBuf>::new());

    let records: Vec<Record> = (0..6)
        .map(|i| step(id("device", i, "seen", 0), i))
        .collect();
    let mut cut = bytes(header(), &records);
    cut.truncate(cut.len() - 20);
    let limits = Limits {
        ids_in_memory: 1,
        spill: dir.path().to_owned(),
        kept: 10,
        fan_in: 0,
        memory_spill: true,
    };
    let result = validate(Reader::new(Cursor::new(&cut)).unwrap(), &NoHost, &limits);
    assert!(result.is_err());
    assert_eq!(leftover(dir.path()), Vec::<PathBuf>::new());
}

/// E5: a pass killed outright leaves its directory, which the next pass sweeps;
/// a directory whose lock a live pass holds is left alone.
#[test]
fn a_killed_pass_is_swept_and_a_live_one_kept() {
    let dir = tempfile::tempdir().unwrap();
    let spill = dir.path();
    std::fs::create_dir(spill.join("pass-7")).unwrap();
    std::fs::write(spill.join("pass-7").join("t0-c0-0.run"), [0u8; 64]).unwrap();
    std::fs::write(spill.join("pass-7.lock"), []).unwrap();
    std::fs::create_dir(spill.join("pass-8")).unwrap();
    std::fs::write(spill.join("pass-9.lock"), []).unwrap();
    std::fs::create_dir(spill.join("pass-9")).unwrap();
    let live = std::fs::File::open(spill.join("pass-9.lock")).unwrap();
    live.lock().unwrap();
    let units: [&dyn Unit; 1] = [&Steps];
    generate(header(), &units, &config(spill, 1, 1), Vec::new()).unwrap();
    let mut left = leftover(spill);
    left.sort();
    assert_eq!(left, vec![spill.join("pass-9"), spill.join("pass-9.lock")]);
    drop(live);
}

/// Waits for the refusal, then counts itself started; emits nothing, so only
/// the check before each unit can stop it.
struct Counting<'a> {
    refused: &'a AtomicBool,
    started: &'a AtomicUsize,
}

impl Unit for Counting<'_> {
    fn emit(
        &self,
        _: &mut dyn FnMut(Record) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        while !self.refused.load(Ordering::SeqCst) {
            std::hint::spin_loop();
        }
        self.started.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Waits for the refusal, then emits a hundred thousand steps, counting those
/// the engine took: only the check at each record can stop it.
struct Long<'a> {
    refused: &'a AtomicBool,
    taken: &'a AtomicUsize,
}

impl Unit for Long<'_> {
    fn emit(
        &self,
        out: &mut dyn FnMut(Record) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        while !self.refused.load(Ordering::SeqCst) {
            std::hint::spin_loop();
        }
        for i in 0..100_000 {
            out(step(id("device", i, "late", 0), 1))?;
            self.taken.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

struct Refusing<'a>(&'a AtomicBool);

impl Unit for Refusing<'_> {
    fn emit(
        &self,
        _: &mut dyn FnMut(Record) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        self.0.store(true, Ordering::SeqCst);
        Err(EngineError::Unit("no law for this scope".into()))
    }
}

/// E6: after a unit refuses, the other threads take no further unit: of a
/// thousand units after it, a handful start, not all.
#[test]
fn a_refusal_stops_the_other_threads() {
    let dir = tempfile::tempdir().unwrap();
    let (refused, started) = (AtomicBool::new(false), AtomicUsize::new(0));
    let refusing = Refusing(&refused);
    let counting = Counting {
        refused: &refused,
        started: &started,
    };
    let mut units: Vec<&dyn Unit> = vec![&refusing];
    units.extend(std::iter::repeat_n(&counting as &dyn Unit, 1_000));
    let result = generate(
        header(),
        &units,
        &config(dir.path(), 2, usize::MAX),
        Vec::new(),
    );
    match result {
        Err(EngineError::Unit(reason)) => assert_eq!(reason, "no law for this scope"),
        other => panic!("{:?}", other.map(|_| ())),
    }
    let started = started.load(Ordering::SeqCst);
    assert!(started <= 10, "{started} units started after the refusal");

    // A unit already running stops at its next record.
    let (refused, taken) = (AtomicBool::new(false), AtomicUsize::new(0));
    let refusing = Refusing(&refused);
    let long = Long {
        refused: &refused,
        taken: &taken,
    };
    let units: [&dyn Unit; 2] = [&long, &refusing];
    assert!(
        generate(
            header(),
            &units,
            &config(dir.path(), 2, usize::MAX),
            Vec::new()
        )
        .is_err()
    );
    let taken = taken.load(Ordering::SeqCst);
    assert!(
        taken < 100_000,
        "a running unit went on to its end: {taken}"
    );
}
