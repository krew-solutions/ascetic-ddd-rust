//! Z1–Z4, E4–E6 of ADR-0046 (decision 15): a compressed log is the same log;
//! a merge stays within its fan-in and deletes as it goes; a merge that would
//! not fit, or a spill in memory, is refused before the first line.

mod fixture;

use std::io::Cursor;
use std::path::{Path, PathBuf};

use ascetic_ddd_command_log::disk::Disk;
use ascetic_ddd_command_log::engine::{Config, EngineError, Unit, generate, generate_on};
use ascetic_ddd_command_log::record::Record;
use ascetic_ddd_command_log::validate::{Limits, NoHost, validate_on};
use ascetic_ddd_command_log::{Compression, LogError, Reader, open, open_file, sample, write_file};
use fixture::*;

const GOLDEN: &[u8] = include_bytes!("../golden/c1.jsonl");
const GOLDEN_ZST: &[u8] = include_bytes!("../golden/c1.jsonl.zst");

fn records_of(input: &[u8]) -> Result<Vec<Record>, LogError> {
    open(Cursor::new(input.to_vec()))?.collect()
}

/// Z1: compressed and plain logs hold the same records and the same trailer;
/// the compressed file is a zstd stream.
#[test]
fn a_compressed_log_is_the_same_log() {
    let dir = tempfile::tempdir().unwrap();
    let (plain, zst) = (
        dir.path().join("plain.jsonl"),
        dir.path().join("log.jsonl.zst"),
    );
    let t1 = write_file(
        &plain,
        sample::header(),
        sample::records(),
        Compression::None,
    )
    .unwrap();
    let t2 = write_file(
        &zst,
        sample::header(),
        sample::records(),
        Compression::Zstd { level: 1 },
    )
    .unwrap();
    assert_eq!(t1, t2);
    assert_eq!(std::fs::read(&plain).unwrap(), GOLDEN);
    let compressed = std::fs::read(&zst).unwrap();
    assert!(compressed.starts_with(&ascetic_ddd_command_log::log::ZSTD_MAGIC));
    assert_eq!(zstd::decode_all(&compressed[..]).unwrap(), GOLDEN);
    let read: Vec<Record> = open_file(&zst).unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(read, sample::records());
}

/// Z2: a stream of two frames, made by the zstd CLI, reads as the golden log.
#[test]
fn concatenated_frames_read_as_one_log() {
    assert_eq!(zstd::decode_all(GOLDEN_ZST).unwrap(), GOLDEN);
    assert_eq!(records_of(GOLDEN_ZST).unwrap(), sample::records());
}

/// Z3: a log is told by its bytes, not its name; anything else is refused.
#[test]
fn a_log_is_told_by_its_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let (named_plain, named_zst) = (dir.path().join("log.jsonl"), dir.path().join("log.zst"));
    std::fs::write(&named_plain, GOLDEN_ZST).unwrap();
    std::fs::write(&named_zst, GOLDEN).unwrap();
    for path in [&named_plain, &named_zst] {
        let read: Vec<Record> = open_file(path).unwrap().collect::<Result<_, _>>().unwrap();
        assert_eq!(read, sample::records(), "{}", path.display());
    }
    assert!(matches!(
        records_of(b"not a log\n"),
        Err(LogError::Malformed { .. })
    ));
    assert!(matches!(records_of(b""), Err(LogError::Truncated)));
}

/// Z4: a compressed log cut at any byte is refused, never read as fewer steps;
/// a failed write leaves no file.
#[test]
fn a_cut_compressed_log_is_refused() {
    for cut in 0..GOLDEN_ZST.len() {
        assert!(records_of(&GOLDEN_ZST[..cut]).is_err(), "cut at {cut}");
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.jsonl.zst");
    let mut wrong = sample::header();
    wrong.format += 1;
    assert!(write_file(&path, wrong, sample::records(), Compression::default()).is_err());
    assert_eq!(leftover(dir.path()), Vec::<PathBuf>::new());
}

/// Steps over two cells, each spilled as runs of one step.
struct Steps {
    count: u64,
}

impl Unit for Steps {
    fn emit(
        &self,
        out: &mut dyn FnMut(Record) -> Result<(), ascetic_ddd_command_log::engine::EngineError>,
    ) -> Result<(), ascetic_ddd_command_log::engine::EngineError> {
        for i in 0..self.count {
            out(step(id("device", i, "seen", 0), (i % 2) * 1_000 + i))?;
        }
        Ok(())
    }
}

fn config(spill: &Path, fan_in: usize) -> Config {
    Config {
        threads: 1,
        budget: 1,
        cell: 1_000,
        cells: 2,
        spill: spill.to_owned(),
        fan_in,
        memory_spill: true,
        progress: None,
        pipe_blocks: 64,
    }
}

/// E4: the bytes are the same whatever the fan-in, the cascade's runs included;
/// a step twice, in runs of different groups, still fails.
#[test]
fn the_fan_in_changes_no_byte() {
    let dir = tempfile::tempdir().unwrap();
    let units: [&dyn Unit; 1] = [&Steps { count: 40 }];
    let reference = generate(header(), &units, &config(dir.path(), 1_000), Vec::new())
        .unwrap()
        .0;
    for fan_in in [2, 3, 0] {
        let bytes = generate(header(), &units, &config(dir.path(), fan_in), Vec::new())
            .unwrap()
            .0;
        assert!(bytes == reference, "fan-in {fan_in}");
    }
    assert_eq!(leftover(dir.path()), Vec::<PathBuf>::new());
    let twice: [&dyn Unit; 2] = [&Steps { count: 9 }, &Steps { count: 9 }];
    assert!(matches!(
        generate(header(), &twice, &config(dir.path(), 2), Vec::new()),
        Err(EngineError::Equal { .. })
    ));
}

/// Watches the spill directory's runs as the log is written.
struct Watch {
    spill: PathBuf,
    seen: Vec<usize>,
}

impl std::io::Write for Watch {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.seen.push(runs(&self.spill));
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// E5: a cell's runs are gone once it is merged: while the second cell is
/// written, the first's runs no longer count.
#[test]
fn runs_are_deleted_as_cells_merge() {
    let dir = tempfile::tempdir().unwrap();
    let units: [&dyn Unit; 1] = [&Steps { count: 20 }];
    let watch = Watch {
        spill: dir.path().to_owned(),
        seen: Vec::new(),
    };
    let (watch, _) = generate(header(), &units, &config(dir.path(), 1_000), watch).unwrap();
    let (first, last) = (
        watch.seen.first().copied().unwrap(),
        watch.seen.last().copied().unwrap(),
    );
    assert_eq!(first, 20, "{:?}", watch.seen);
    assert!(last <= 10, "the first cell's runs kept: {:?}", watch.seen);
}

/// Counts the writes into a log.
struct Counter(std::sync::Arc<std::sync::atomic::AtomicUsize>);

impl std::io::Write for Counter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A machine with a given free space, in memory or not.
struct Fake {
    free: u64,
    memory: bool,
}

impl Disk for Fake {
    fn free(&self, _: &Path) -> std::io::Result<u64> {
        Ok(self.free)
    }
    fn device(&self, _: &Path) -> std::io::Result<u64> {
        Ok(1)
    }
    fn in_memory(&self, _: &Path) -> std::io::Result<bool> {
        Ok(self.memory)
    }
    fn open_files(&self) -> std::io::Result<u64> {
        Ok(1_024)
    }
}

/// E6: short of one byte, the generation is refused before its first line and
/// names what it needs; a spill directory in memory is refused unless allowed,
/// for the validator too.
#[test]
fn a_merge_that_would_not_fit_is_refused_first() {
    let dir = tempfile::tempdir().unwrap();
    let units: [&dyn Unit; 1] = [&Steps { count: 20 }];
    let config = config(dir.path(), 0);
    let needed = match generate_on(
        header(),
        &units,
        &config,
        &Fake {
            free: 0,
            memory: false,
        },
        Vec::new(),
    ) {
        Err(EngineError::Space {
            needed, free: 0, ..
        }) => needed,
        other => panic!("{:?}", other.map(|_| ())),
    };
    // More than the margin alone: the cells' spilled bytes count.
    assert!(needed > 64 << 20, "{needed}");
    let short = Fake {
        free: needed - 1,
        memory: false,
    };
    let written = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    match generate_on(header(), &units, &config, &short, Counter(written.clone())) {
        Err(EngineError::Space { .. }) => {}
        other => panic!("{:?}", other.map(|_| ())),
    }
    assert_eq!(
        written.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "a line written before the refusal"
    );
    let enough = Fake {
        free: needed,
        memory: false,
    };
    assert!(generate_on(header(), &units, &config, &enough, Vec::new()).is_ok());

    let memory = Fake {
        free: u64::MAX,
        memory: true,
    };
    let refused = Config {
        memory_spill: false,
        ..config.clone()
    };
    assert!(matches!(
        generate_on(header(), &units, &refused, &memory, Vec::new()),
        Err(EngineError::InMemory { .. })
    ));
    assert!(generate_on(header(), &units, &config, &memory, Vec::new()).is_ok());
    let log = bytes(header(), &[step(id("device", 1, "seen", 0), 1)]);
    let limits = Limits {
        memory_spill: false,
        ..limits_in(dir.path())
    };
    assert!(
        validate_on(
            Reader::new(Cursor::new(&log)).unwrap(),
            &NoHost,
            &limits,
            &memory
        )
        .is_err()
    );
    assert!(
        validate_on(
            Reader::new(Cursor::new(&log)).unwrap(),
            &NoHost,
            &limits_in(dir.path()),
            &memory
        )
        .is_ok()
    );
}
