//! E6 of ADR-0046 (decision 15), the rest: the space a merge needs counts the
//! log by where it goes and how it is written, and a dry run counts the
//! validator's id runs; the observer hears every unit and every line.

mod fixture;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use ascetic_ddd_command_log::Compression;
use ascetic_ddd_command_log::disk::Disk;
use ascetic_ddd_command_log::engine::{
    Config, EngineError, Observer, Progress, Unit, generate, generate_file_on,
};
use ascetic_ddd_command_log::live::dry_on;
use ascetic_ddd_command_log::record::Record;
use ascetic_ddd_command_log::validate::{Limits, NoHost};
use fixture::*;

const MARGIN: u64 = 64 << 20;

/// A disk whose filesystems are told by path: under `apart` is filesystem 2,
/// all else filesystem 1; free space by filesystem.
struct Fake {
    apart: PathBuf,
    free: [u64; 2],
}

impl Fake {
    fn which(&self, path: &Path) -> usize {
        usize::from(path.starts_with(&self.apart))
    }
}

impl Disk for Fake {
    fn free(&self, path: &Path) -> std::io::Result<u64> {
        Ok(self.free[self.which(path)])
    }
    fn device(&self, path: &Path) -> std::io::Result<u64> {
        Ok(self.which(path) as u64 + 1)
    }
    fn in_memory(&self, _: &Path) -> std::io::Result<bool> {
        Ok(false)
    }
    fn open_files(&self) -> std::io::Result<u64> {
        Ok(1_024)
    }
}

/// `count` steps spread over eight cells of 1,000 µs.
struct Spread {
    count: u64,
}

impl Unit for Spread {
    fn emit(
        &self,
        out: &mut dyn FnMut(Record) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        for i in 0..self.count {
            out(step(
                id("device", i, "seen", 0),
                (i % 8) * 1_000 + i % 1_000,
            ))?;
        }
        Ok(())
    }
}

fn config(spill: &Path) -> Config {
    Config {
        threads: 2,
        budget: 16 << 10,
        cell: 1_000,
        cells: 8,
        spill: spill.to_owned(),
        fan_in: 0,
        memory_spill: true,
        progress: None,
    }
}

/// The step lines' bytes of the log `units` make.
fn lines(units: &[&dyn Unit], spill: &Path) -> u64 {
    let log = generate(header(), units, &config(spill), Vec::new())
        .unwrap()
        .0;
    let all: Vec<&[u8]> = log.split_inclusive(|b| *b == b'\n').collect();
    all[1..all.len() - 1].iter().map(|l| l.len() as u64).sum()
}

fn needed(result: Result<impl std::fmt::Debug, EngineError>, at: &Path) -> u64 {
    match result {
        Err(EngineError::Space { path, needed, free }) => {
            assert_eq!(path, at);
            assert_eq!(free, 0);
            needed
        }
        other => panic!("{other:?}"),
    }
}

/// A plain log on a filesystem of its own needs exactly its step lines there,
/// and nothing is written when they do not fit.
#[test]
fn a_plain_log_elsewhere_needs_its_lines() {
    let dir = tempfile::tempdir().unwrap();
    let (spill, logs) = (dir.path().join("spill"), dir.path().join("logs"));
    std::fs::create_dir_all(&logs).unwrap();
    let units: [&dyn Unit; 1] = [&Spread { count: 2_000 }];
    let expected = lines(&units, &spill);
    let disk = Fake {
        apart: logs.clone(),
        free: [u64::MAX, 0],
    };
    let log = logs.join("plan.jsonl");
    let result = generate_file_on(
        &log,
        header(),
        &units,
        &config(&spill),
        Compression::None,
        &disk,
    );
    assert_eq!(needed(result, &logs), MARGIN + expected);
    assert_eq!(std::fs::read_dir(&logs).unwrap().count(), 0);
}

/// A log beside its spills counts its share and gives the runs back: a plain
/// log needs about its lines, a compressed one about its runs.
#[test]
fn a_log_beside_its_spills_counts_its_share() {
    let dir = tempfile::tempdir().unwrap();
    let spill = dir.path().join("spill");
    let units: [&dyn Unit; 1] = [&Spread { count: 2_000 }];
    let half = lines(&units, &spill) / 2;
    let disk = Fake {
        apart: dir.path().join("elsewhere"),
        free: [0, 0],
    };
    let log = dir.path().join("plan.jsonl");
    let plain = needed(
        generate_file_on(
            &log,
            header(),
            &units,
            &config(&spill),
            Compression::None,
            &disk,
        ),
        &spill,
    );
    let compressed = needed(
        generate_file_on(
            &log,
            header(),
            &units,
            &config(&spill),
            Compression::Zstd { level: 1 },
            &disk,
        ),
        &spill,
    );
    assert!(plain > MARGIN + half, "{plain}");
    assert!(compressed < MARGIN + half, "{compressed}");
}

/// Devices `0..count`, each managed and then seen.
struct Devices(u64);

impl Unit for Devices {
    fn emit(
        &self,
        out: &mut dyn FnMut(Record) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        for d in 0..self.0 {
            out(step(id("device", d, "managed", 0), d * 10))?;
            out(step(id("device", d, "seen", 0), d * 10 + 5))?;
        }
        Ok(())
    }
}

/// A dry run counts the validator's id runs, which grow while the merge runs:
/// 24 bytes a step and a cascade group more, refused short of one byte, let
/// through at it; none while the ids fit in memory.
#[test]
fn a_dry_run_counts_the_validator_s_runs() {
    let dir = tempfile::tempdir().unwrap();
    let (spill, ids) = (dir.path().join("spill"), dir.path().join("ids"));
    let path = dir.path().join("plan.commitment");
    let units: [&dyn Unit; 1] = [&Devices(9)];
    // 18 steps: 432 bytes of ids, a group of two runs of four ids 192 more.
    let limits = |held| Limits {
        ids_in_memory: held,
        spill: ids.clone(),
        kept: 100,
        fan_in: 2,
        memory_spill: true,
    };
    let run = |held, free| {
        let disk = Fake {
            apart: ids.clone(),
            free: [u64::MAX, free],
        };
        let config = Config {
            budget: 512,
            ..config(&spill)
        };
        dry_on(
            header(),
            &units,
            &config,
            &NoHost,
            &limits(held),
            "build-1",
            4,
            &path,
            &disk,
        )
    };
    match run(4, MARGIN + 623) {
        Err(EngineError::Space {
            path: p, needed, ..
        }) => {
            assert_eq!((p, needed), (ids.clone(), MARGIN + 624));
        }
        other => panic!("{:?}", other.map(|r| r.violations)),
    }
    assert!(!path.exists());
    let report = run(4, MARGIN + 624).unwrap();
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    assert!(path.exists());
    std::fs::remove_file(&path).unwrap();
    run(18, MARGIN).unwrap();
    assert!(path.exists());
    // On the spills' filesystem the two needs add up.
    let beside = |held| {
        let disk = Fake {
            apart: dir.path().join("elsewhere"),
            free: [0, 0],
        };
        let config = Config {
            budget: 512,
            ..config(&spill)
        };
        match dry_on(
            header(),
            &units,
            &config,
            &NoHost,
            &limits(held),
            "build-1",
            4,
            &path,
            &disk,
        ) {
            Err(EngineError::Space { needed, .. }) => needed,
            other => panic!("{:?}", other.map(|r| r.violations)),
        }
    };
    let engine_alone = beside(18);
    assert!(engine_alone > MARGIN);
    assert_eq!(beside(4), engine_alone + 624);
}

#[derive(Debug, Clone, PartialEq)]
enum Heard {
    Began(u64),
    Emitted(u64, u64, u64),
    Merging(u64),
    Merged(u64, u64),
}

#[derive(Default)]
struct Ear(Mutex<Vec<Heard>>);

impl Progress for Ear {
    fn began(&self, swept: u64) {
        self.0.lock().unwrap().push(Heard::Began(swept));
    }
    fn emitted(&self, units: u64, records: u64, spilled: u64) {
        self.0
            .lock()
            .unwrap()
            .push(Heard::Emitted(units, records, spilled));
    }
    fn merging(&self, cells: u64) {
        self.0.lock().unwrap().push(Heard::Merging(cells));
    }
    fn merged(&self, cells: u64, lines: u64) {
        self.0.lock().unwrap().push(Heard::Merged(cells, lines));
    }
}

/// Steps `from..from + count`, spread over eight cells.
struct Part {
    from: u64,
    count: u64,
}

impl Unit for Part {
    fn emit(
        &self,
        out: &mut dyn FnMut(Record) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        for i in self.from..self.from + self.count {
            out(step(
                id("device", i, "seen", 0),
                (i % 8) * 1_000 + i % 1_000,
            ))?;
        }
        Ok(())
    }
}

/// The observer hears the sweep of a pass killed before, every unit with the
/// steps and spilled bytes so far, the cells to merge before any is merged,
/// and every cell merged, in order, with the lines written so far.
#[test]
fn the_observer_hears_every_unit_and_line() {
    let dir = tempfile::tempdir().unwrap();
    let spill = dir.path().join("spill");
    std::fs::create_dir_all(spill.join("pass-7")).unwrap();
    std::fs::write(spill.join("pass-7").join("t0-c0-0.run"), [0u8; 1_000]).unwrap();
    let ear = Arc::new(Ear::default());
    let config = Config {
        progress: Some(Observer(ear.clone())),
        ..config(&spill)
    };
    let units: [&dyn Unit; 3] = [
        &Part {
            from: 0,
            count: 500,
        },
        &Part {
            from: 500,
            count: 300,
        },
        &Part {
            from: 800,
            count: 200,
        },
    ];
    generate(header(), &units, &config, Vec::new()).unwrap();
    let heard = ear.0.lock().unwrap().clone();
    assert_eq!(heard.first(), Some(&Heard::Began(1_000)));
    let emitted: Vec<_> = heard
        .iter()
        .filter_map(|h| match h {
            Heard::Emitted(u, r, s) => Some((*u, *r, *s)),
            _ => None,
        })
        .collect();
    let mut units_heard: Vec<u64> = emitted.iter().map(|e| e.0).collect();
    units_heard.sort_unstable();
    assert_eq!(units_heard, vec![1, 2, 3]);
    let last = emitted.iter().max_by_key(|e| e.0).unwrap();
    assert_eq!(last.1, 1_000);
    assert!(last.2 > 0, "{heard:?}");
    let at_merge = heard.iter().position(|h| *h == Heard::Merging(8)).unwrap();
    assert!(
        heard[..at_merge]
            .iter()
            .all(|h| !matches!(h, Heard::Merged(..)))
    );
    let merged: Vec<_> = heard
        .iter()
        .filter_map(|h| match h {
            Heard::Merged(c, l) => Some((*c, *l)),
            _ => None,
        })
        .collect();
    assert_eq!(
        merged.iter().map(|m| m.0).collect::<Vec<_>>(),
        (1..=8).collect::<Vec<_>>()
    );
    assert!(merged.windows(2).all(|w| w[0].1 < w[1].1));
    assert_eq!(merged.last().map(|m| m.1), Some(1_000));
}

/// One unit on one thread: what it spilled is heard with it, not a unit late.
#[test]
fn a_unit_s_spills_are_heard_with_it() {
    let dir = tempfile::tempdir().unwrap();
    let ear = Arc::new(Ear::default());
    let config = Config {
        threads: 1,
        progress: Some(Observer(ear.clone())),
        ..config(dir.path())
    };
    let units: [&dyn Unit; 1] = [&Part {
        from: 0,
        count: 1_000,
    }];
    generate(header(), &units, &config, Vec::new()).unwrap();
    let heard = ear.0.lock().unwrap().clone();
    let emitted: Vec<_> = heard
        .iter()
        .filter_map(|h| match h {
            Heard::Emitted(u, r, s) => Some((*u, *r, *s)),
            _ => None,
        })
        .collect();
    assert_eq!(emitted.len(), 1);
    assert_eq!((emitted[0].0, emitted[0].1), (1, 1_000));
    assert!(emitted[0].2 > 0, "{heard:?}");
}
