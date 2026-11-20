//! E1 of ADR-0046: the engine's bytes are the sorted multiset of its units'
//! records, whatever the threads, the budget, the cells and the units' order.

mod fixture;

use ascetic_ddd_command_log::Writer;
use ascetic_ddd_command_log::engine::{Config, EngineError, Unit, generate};
use ascetic_ddd_command_log::record::{On, Record};
use ascetic_ddd_command_log::validate::NoHost;
use ascetic_ddd_faker::{Key, Seed};
use fixture::*;

const HORIZON: u64 = 10_000_000;

/// Devices `first..first + count`: each managed at one of a thousand instants
/// drawn from its key,
/// then telling three facts, each awaiting the one before.
struct Devices {
    first: u64,
    count: u64,
}

impl Unit for Devices {
    fn emit(
        &self,
        out: &mut dyn FnMut(Record) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        for d in self.first..self.first + self.count {
            let key = Key::root(Seed(5)).child("device").at(d);
            // A thousand instants for 391 Devices: many share one, and the
            // step id orders them.
            let at = key.child("arrival").unit().raw() % 1_000 * (HORIZON / 1_000);
            let managed = step(id("device", d, "managed", 0), at);
            let mut before = managed.step.clone();
            out(managed)?;
            let mut t = at;
            for n in 0..3u32 {
                t += 1 + key.child("fact").at(u64::from(n)).unit().raw() % 500;
                let fact = after(step(id("device", d, "fact", n), t), &before, On::Expected);
                before = fact.step.clone();
                out(fact)?;
            }
        }
        Ok(())
    }
}

fn units() -> Vec<Devices> {
    (0..23)
        .map(|i| Devices {
            first: i * 17,
            count: 17,
        })
        .collect()
}

/// The oracle: every record, sorted in memory, written.
fn reference() -> Vec<u8> {
    let mut all = Vec::new();
    for u in units() {
        u.emit(&mut |r| {
            all.push(r);
            Ok(())
        })
        .unwrap();
    }
    all.sort_by(|a, b| (a.at, &a.step).cmp(&(b.at, &b.step)));
    let mut w = Writer::new(Vec::new(), header()).unwrap();
    for r in all {
        w.step(r).unwrap();
    }
    w.finish().unwrap().0
}

fn run(threads: usize, budget: usize, cells: u64, reversed: bool) -> Result<Vec<u8>, EngineError> {
    let dir = tempfile::tempdir().unwrap();
    let owned = units();
    let mut units: Vec<&dyn Unit> = owned.iter().map(|u| u as &dyn Unit).collect();
    if reversed {
        units.reverse();
    }
    let config = Config {
        threads,
        budget,
        cell: HORIZON / 32,
        cells,
        spill: dir.path().to_owned(),
    };
    let out = generate(header(), &units, &config, Vec::new()).map(|(bytes, _)| bytes);
    assert_eq!(
        std::fs::read_dir(dir.path()).unwrap().count(),
        0,
        "spills removed"
    );
    out
}

/// E1: one byte form across threads × budgets × cell caps × the units' order.
#[test]
fn the_bytes_are_the_records_sorted() {
    let expected = reference();
    for threads in [1, 2, 7] {
        for budget in [usize::MAX, 1, 2_000] {
            for cells in [1, 64] {
                for reversed in [false, true] {
                    let got = run(threads, budget, cells, reversed).unwrap();
                    assert!(
                        got == expected,
                        "threads {threads}, budget {budget}, cells {cells}, reversed {reversed}"
                    );
                }
            }
        }
    }
    let report = run_bytes(&expected);
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    assert_eq!(report.records, 23 * 17 * 4);
    let text = String::from_utf8(expected).unwrap();
    let ats: Vec<&str> = text
        .lines()
        .filter_map(|l| l.split("\"at\":\"").nth(1)?.split('"').next())
        .collect();
    let distinct: std::collections::BTreeSet<_> = ats.iter().collect();
    // Steps sharing an instant, so that the merge is decided by the step id.
    let ties = ats.len() - distinct.len();
    assert!(ties >= 50, "{ties} steps share an instant");
}

fn run_bytes(bytes: &[u8]) -> ascetic_ddd_command_log::validate::Report {
    let dir = tempfile::tempdir().unwrap();
    let reader = ascetic_ddd_command_log::Reader::new(std::io::Cursor::new(bytes)).unwrap();
    ascetic_ddd_command_log::validate::validate(
        reader,
        &NoHost,
        &ascetic_ddd_command_log::validate::Limits::new(dir.path()),
    )
    .unwrap()
}

struct Twice;

impl Unit for Twice {
    fn emit(
        &self,
        out: &mut dyn FnMut(Record) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        out(step(id("device", 1, "seen", 0), 10))?;
        out(step(id("device", 2, "seen", 0), 5))
    }
}

struct Refusing;

impl Unit for Refusing {
    fn emit(
        &self,
        out: &mut dyn FnMut(Record) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        out(step(id("device", 3, "seen", 0), 7))?;
        Err(EngineError::Unit("no law for this scope".into()))
    }
}

/// Two records with one key are refused, spilled or not; a unit's refusal
/// fails the generation; spills are removed either way.
#[test]
fn equal_keys_and_refusals_fail_the_generation() {
    for budget in [usize::MAX, 1] {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            threads: 2,
            budget,
            cell: 1,
            cells: 4,
            spill: dir.path().to_owned(),
        };
        let units: [&dyn Unit; 2] = [&Twice, &Twice];
        match generate(header(), &units, &config, Vec::new()) {
            Err(EngineError::Equal { at, step }) => {
                assert_eq!(at, 5);
                assert_eq!(*step, id("device", 2, "seen", 0));
            }
            other => panic!("budget {budget}: {:?}", other.map(|_| ())),
        }
        let units: [&dyn Unit; 2] = [&Twice, &Refusing];
        assert!(matches!(
            generate(header(), &units, &config, Vec::new()),
            Err(EngineError::Unit(_))
        ));
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}

/// A unit that, as it emits, counts the spill directory's files.
struct Watching {
    dir: std::path::PathBuf,
    most: std::sync::atomic::AtomicUsize,
}

impl Unit for Watching {
    fn emit(
        &self,
        out: &mut dyn FnMut(Record) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        for i in 0..50 {
            out(step(id("device", i, "seen", 0), i))?;
            let files = std::fs::read_dir(&self.dir).unwrap().count();
            self.most
                .fetch_max(files, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(())
    }
}

/// Under its budget a thread keeps its records in memory and writes no run;
/// past it, runs go to disk as it emits.
#[test]
fn a_thread_spills_past_its_budget_only() {
    for (budget, spills) in [(usize::MAX, false), (1, true)] {
        let dir = tempfile::tempdir().unwrap();
        let unit = Watching {
            dir: dir.path().to_owned(),
            most: std::sync::atomic::AtomicUsize::new(0),
        };
        let config = Config {
            threads: 1,
            budget,
            cell: 10,
            cells: 8,
            spill: dir.path().to_owned(),
        };
        generate(header(), &[&unit], &config, Vec::new()).unwrap();
        let most = unit.most.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(most > 0, spills, "budget {budget}: {most} files at most");
    }
}

/// The errors say what failed.
#[test]
fn errors_say_what_failed() {
    let e = EngineError::Equal {
        at: 5,
        step: Box::new(id("device", 2, "seen", 0)),
    };
    assert!(
        e.to_string().starts_with("two records at 5 of StepId"),
        "{e}"
    );
    assert_eq!(
        EngineError::Unit("no law".into()).to_string(),
        "a unit refused: no law"
    );
}
