//! The engine (ADR-0046 decision 11): units of identity emit their records in
//! any order, on any number of threads; the engine buckets them by time cell,
//! spills sorted runs past a byte budget, and merges each cell by `(at, StepId)`
//! into the log.
//!
//! The output is the sorted multiset of the units' records, so neither the
//! thread count, nor the budget, nor the cells, nor the units' order changes a
//! byte. Two records with one `(at, StepId)` would let the spill order choose:
//! they are refused, a generation error (decision 7).

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::disk::{Disk, LocalDisk};
use crate::log::{Compression, FileOut, LogError, Partial, Writer, encode};
use crate::record::{Header, Line, Record, StepId, Trailer};
use crate::spill::{Counted, SpillDir};

/// A partition of identity the Plan fixes: a population family, a scope, an
/// index range. Its records' planned times are pure functions of identity and
/// the Plan.
pub trait Unit: Send + Sync {
    /// Emits the unit's records, in any order.
    fn emit(
        &self,
        out: &mut dyn FnMut(Record) -> Result<(), EngineError>,
    ) -> Result<(), EngineError>;
}

/// Why a generation failed.
#[derive(Debug)]
pub enum EngineError {
    /// Writing the log or a spill.
    Log(LogError),
    /// A unit refused, with its reason.
    Unit(String),
    /// Reading or writing a spill, with the phase and the path.
    Io {
        /// What the engine was doing.
        phase: &'static str,
        /// The file or directory.
        path: PathBuf,
        /// What failed.
        error: std::io::Error,
    },
    /// Not enough free space for the merge, found before its first line.
    Space {
        /// The filesystem's path.
        path: PathBuf,
        /// What the merge needs there, in bytes.
        needed: u64,
        /// What is free.
        free: u64,
    },
    /// A spill directory in memory, not allowed by the configuration.
    InMemory {
        /// The directory.
        path: PathBuf,
    },
    /// A live run that is not its commitment's: what differs.
    Diverged {
        /// The format, the validator, the fingerprint, the header, what was
        /// emitted, or a chunk.
        what: String,
    },
    /// Two records with one `(at, StepId)`.
    Equal {
        /// Their planned time.
        at: u64,
        /// Their step.
        step: Box<StepId>,
    },
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineError::Log(e) => write!(f, "{e}"),
            EngineError::Unit(reason) => write!(f, "a unit refused: {reason}"),
            EngineError::Io { phase, path, error } => {
                write!(f, "{phase} {}: {error}", path.display())
            }
            EngineError::Space { path, needed, free } => write!(
                f,
                "{}: the merge needs {needed} bytes, {free} are free",
                path.display()
            ),
            EngineError::InMemory { path } => write!(
                f,
                "{} is in memory: a spill there is refused unless allowed",
                path.display()
            ),
            EngineError::Diverged { what } => write!(f, "{what} differs from the commitment"),
            EngineError::Equal { at, step } => write!(f, "two records at {at} of {step:?}"),
        }
    }
}

impl std::error::Error for EngineError {}

impl From<LogError> for EngineError {
    fn from(e: LogError) -> EngineError {
        EngineError::Log(e)
    }
}

impl From<std::io::Error> for EngineError {
    fn from(e: std::io::Error) -> EngineError {
        EngineError::Log(LogError::Io(e))
    }
}

/// How a generation runs. None of it changes a byte of the log.
#[derive(Clone, Debug)]
pub struct Config {
    /// Threads emitting units and sorting their buckets; at least 1.
    pub threads: usize,
    /// Bytes of records a thread holds before it spills its buckets.
    pub budget: usize,
    /// The width of a time cell, in microseconds; at least 1.
    pub cell: u64,
    /// The most cells: a later record goes to the last one.
    pub cells: u64,
    /// Where spills go: a directory of the caller's, its files removed after.
    pub spill: PathBuf,
    /// The most runs a merge holds open; 0 derives it from the open-file limit
    /// ([`crate::disk::fan_in`]).
    pub fan_in: usize,
    /// Whether `spill` may be memory (tmpfs): refused unless allowed, since a
    /// spill the size of a log in memory is what oomd kills.
    pub memory_spill: bool,
}

/// The bytes a record sorts by: `at` big-endian, then the step id in an
/// encoding whose byte order is the step id's order — strings and bytes with
/// each 0 written as `0 255` and ended by `0 0`, integers big-endian.
fn order_key(at: u64, step: &StepId) -> Vec<u8> {
    fn bytes(v: &[u8], out: &mut Vec<u8>) {
        for &b in v {
            out.push(b);
            if b == 0 {
                out.push(255);
            }
        }
        out.extend_from_slice(&[0, 0]);
    }
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(&at.to_be_bytes());
    bytes(step.subject.kind.as_bytes(), &mut out);
    bytes(&step.subject.scope, &mut out);
    out.extend_from_slice(&step.subject.index.to_be_bytes());
    bytes(step.label.as_bytes(), &mut out);
    out.extend_from_slice(&step.n.to_be_bytes());
    out
}

/// A record ready to sort: its key, its canonical line, and what it was for an
/// error.
struct Entry {
    key: Vec<u8>,
    line: Vec<u8>,
}

/// An I/O error of `phase` at `path`.
fn at(phase: &'static str, path: &std::path::Path) -> impl FnOnce(std::io::Error) -> EngineError {
    let path = path.to_owned();
    move |error| EngineError::Io { phase, path, error }
}

/// One thread's buckets: a cell's records in memory, and the runs it spilled,
/// each with its record count.
struct Buckets {
    thread: usize,
    dir: PathBuf,
    held: BTreeMap<u64, Vec<Entry>>,
    bytes: usize,
    runs: BTreeMap<u64, Vec<(PathBuf, u64)>>,
    spilled: usize,
    /// The thread's share of [`Emitted`].
    emitted: Emitted,
}

/// What a generation emitted, known before its merge: the step count and an
/// order-independent digest of its step lines — the sum, modulo 2¹²⁸, of a
/// 128-bit SipHash of each line. Equal for two generations of one multiset of
/// lines whatever the threads; the merge writes that multiset in order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Emitted {
    /// Steps emitted.
    pub records: u64,
    /// The lines' order-independent digest.
    pub multiset: u128,
}

impl Emitted {
    fn add(&mut self, line: &[u8]) {
        use std::hash::Hasher;
        let mut h = siphasher::sip128::SipHasher13::new_with_keys(0, 0);
        h.write(line);
        let hash = siphasher::sip128::Hasher128::finish128(&h).as_u128();
        self.records = self.records.saturating_add(1);
        self.multiset = self.multiset.wrapping_add(hash);
    }

    fn merge(self, other: Emitted) -> Emitted {
        Emitted {
            records: self.records.saturating_add(other.records),
            multiset: self.multiset.wrapping_add(other.multiset),
        }
    }
}

impl Buckets {
    fn push(&mut self, cell: u64, entry: Entry, config: &Config) -> Result<(), EngineError> {
        self.bytes = self
            .bytes
            .saturating_add(entry.key.len())
            .saturating_add(entry.line.len());
        self.held.entry(cell).or_default().push(entry);
        if self.bytes > config.budget {
            self.spill()?;
        }
        Ok(())
    }

    /// Every cell's records, sorted, to a run of its own.
    fn spill(&mut self) -> Result<(), EngineError> {
        for (cell, mut entries) in std::mem::take(&mut self.held) {
            entries.sort_unstable_by(|a, b| a.key.cmp(&b.key));
            let path = self
                .dir
                .join(format!("t{}-c{cell}-{}.run", self.thread, self.spilled));
            self.spilled = self.spilled.saturating_add(1);
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(at("spilling to", &path))?;
            let count = entries.len() as u64;
            let mut out = RunOut::new(file, &path)?;
            for e in entries {
                out.write(&e)?;
            }
            out.finish()?;
            self.runs.entry(cell).or_default().push((path, count));
        }
        self.bytes = 0;
        Ok(())
    }
}

/// A run being written: a zstd stream at level 1 of its entries.
struct RunOut {
    out: BufWriter<zstd::Encoder<'static, File>>,
    path: PathBuf,
}

impl RunOut {
    fn new(file: File, path: &std::path::Path) -> Result<RunOut, EngineError> {
        let encoder = zstd::Encoder::new(file, 1).map_err(at("spilling to", path))?;
        Ok(RunOut {
            out: BufWriter::new(encoder),
            path: path.to_owned(),
        })
    }

    fn write(&mut self, e: &Entry) -> Result<(), EngineError> {
        write_entry(&mut self.out, e).map_err(|e| match e {
            EngineError::Log(LogError::Io(error)) => at("spilling to", &self.path)(error),
            other => other,
        })
    }

    /// Every byte written and the frame finished, its error kept.
    fn finish(self) -> Result<(), EngineError> {
        let path = self.path;
        let encoder = self
            .out
            .into_inner()
            .map_err(|e| at("spilling to", &path)(e.into_error()))?;
        encoder.finish().map_err(at("spilling to", &path))?;
        Ok(())
    }
}

/// A spilled run opened for a merge, held to its count.
fn open_run(
    path: &std::path::Path,
    count: u64,
) -> Result<Counted<Box<dyn Read + Send>>, EngineError> {
    let file = File::open(path).map_err(at("merging", path))?;
    let decoder = zstd::Decoder::new(file).map_err(at("merging", path))?;
    Ok(Counted {
        reader: Box::new(BufReader::new(decoder)),
        left: count,
        path: path.to_owned(),
    })
}

fn write_entry(out: &mut impl Write, e: &Entry) -> Result<(), EngineError> {
    let len =
        |n: usize| u32::try_from(n).map_err(|_| EngineError::Unit("a record past 4 GiB".into()));
    out.write_all(&len(e.key.len())?.to_le_bytes())?;
    out.write_all(&e.key)?;
    out.write_all(&len(e.line.len())?.to_le_bytes())?;
    out.write_all(&e.line)?;
    Ok(())
}

/// A run's next record, or `None` past its last: a run that ends early or goes
/// on past its count is an error, never fewer or more records.
fn read_entry<R: Read>(run: &mut Counted<R>) -> Result<Option<Entry>, EngineError> {
    let path = run.path.clone();
    if !run.due().map_err(at("merging", &path))? {
        return Ok(None);
    }
    run.left = run.left.saturating_sub(1);
    let mut len = [0u8; 4];
    run.exact(&mut len).map_err(at("merging", &path))?;
    let mut key = vec![0u8; u32::from_le_bytes(len) as usize];
    run.exact(&mut key).map_err(at("merging", &path))?;
    run.exact(&mut len).map_err(at("merging", &path))?;
    let mut line = vec![0u8; u32::from_le_bytes(len) as usize];
    run.exact(&mut line).map_err(at("merging", &path))?;
    Ok(Some(Entry { key, line }))
}

/// Where a cell's merge reads from: a spilled run, or what a thread still held.
enum Source {
    Run(Counted<Box<dyn Read + Send>>),
    Held(std::vec::IntoIter<Entry>),
}

impl Source {
    fn next(&mut self) -> Result<Option<Entry>, EngineError> {
        match self {
            Source::Run(r) => read_entry(r),
            Source::Held(it) => Ok(it.next()),
        }
    }
}

/// Generates a log: every unit's records, in `(at, StepId)` order, under
/// `header`, into `out`.
pub fn generate<W: Write>(
    header: Header,
    units: &[&dyn Unit],
    config: &Config,
    out: W,
) -> Result<(W, Trailer), EngineError> {
    generate_on(header, units, config, &LocalDisk, out)
}

/// [`generate`] on a given machine: its free space, filesystems and limits.
pub fn generate_on<W: Write>(
    header: Header,
    units: &[&dyn Unit],
    config: &Config,
    disk: &dyn Disk,
    out: W,
) -> Result<(W, Trailer), EngineError> {
    pass(header, units, config, disk, out, None, &|_| Ok(()))
}

/// Generates a log into the file `path`, whole or not at all: `path.partial`,
/// compressed as asked, synced and renamed once its trailer is written.
pub fn generate_file(
    path: &std::path::Path,
    header: Header,
    units: &[&dyn Unit],
    config: &Config,
    compression: Compression,
) -> Result<Trailer, EngineError> {
    let (partial, file) = Partial::create(path).map_err(at("creating", path))?;
    let out = FileOut::new(file, compression).map_err(at("creating", path))?;
    let (out, trailer) = pass(header, units, config, &LocalDisk, out, Some(path), &|_| {
        Ok(())
    })?;
    let file = out.finish().map_err(at("finishing", path))?;
    partial.commit(file).map_err(at("finishing", path))?;
    Ok(trailer)
}

/// [`generate`] with a check between the emit phase and the merge: `check`
/// sees what was emitted and may refuse before a line is written.
pub fn generate_checked<W: Write>(
    header: Header,
    units: &[&dyn Unit],
    config: &Config,
    out: W,
    check: &dyn Fn(Emitted) -> Result<(), EngineError>,
) -> Result<(W, Trailer), EngineError> {
    pass(header, units, config, &LocalDisk, out, None, check)
}

fn pass<W: Write>(
    header: Header,
    units: &[&dyn Unit],
    config: &Config,
    disk: &dyn Disk,
    out: W,
    log: Option<&std::path::Path>,
    check: &dyn Fn(Emitted) -> Result<(), EngineError>,
) -> Result<(W, Trailer), EngineError> {
    std::fs::create_dir_all(&config.spill)
        .map_err(at("claiming a spill directory in", &config.spill))?;
    if !config.memory_spill
        && disk
            .in_memory(&config.spill)
            .map_err(at("inspecting", &config.spill))?
    {
        return Err(EngineError::InMemory {
            path: config.spill.clone(),
        });
    }
    let fan_in = match config.fan_in {
        0 => crate::disk::fan_in(disk.open_files().map_err(at("inspecting", &config.spill))?),
        n => n.max(2),
    };
    let threads = config.threads.max(1);
    let cell = config.cell.max(1);
    let last = config.cells.max(1).saturating_sub(1);
    // The pass's own directory, removed on every exit below, unwinding too.
    let dir =
        SpillDir::new(&config.spill).map_err(at("claiming a spill directory in", &config.spill))?;
    let next = AtomicUsize::new(0);
    // Set by the first failure: the other threads take no further unit.
    let stop = AtomicBool::new(false);
    let done: Mutex<Vec<Buckets>> = Mutex::new(Vec::new());
    let failed: Mutex<Option<EngineError>> = Mutex::new(None);
    std::thread::scope(|scope| {
        for thread in 0..threads {
            let (next, stop, done, failed, dir) = (&next, &stop, &done, &failed, dir.path());
            scope.spawn(move || {
                let mut buckets = Buckets {
                    thread,
                    dir: dir.to_owned(),
                    held: BTreeMap::new(),
                    bytes: 0,
                    runs: BTreeMap::new(),
                    spilled: 0,
                    emitted: Emitted::default(),
                };
                let result = (|| -> Result<(), EngineError> {
                    while !stop.load(Ordering::Relaxed) {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let Some(unit) = units.get(i) else {
                            return Ok(());
                        };
                        let emitted =
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                unit.emit(&mut |record: Record| {
                                    if stop.load(Ordering::Relaxed) {
                                        return Err(EngineError::Unit(
                                            "stopped by another unit's failure".into(),
                                        ));
                                    }
                                    let key = order_key(record.at, &record.step);
                                    let at_cell =
                                        record.at.checked_div(cell).unwrap_or(0).min(last);
                                    let line = encode(&Line::Step(Box::new(record)))?;
                                    buckets.emitted.add(&line);
                                    buckets.push(at_cell, Entry { key, line }, config)
                                })
                            }));
                        match emitted {
                            Ok(result) => result?,
                            Err(panic) => {
                                let what = panic
                                    .downcast_ref::<&str>()
                                    .map(|s| (*s).to_owned())
                                    .or_else(|| panic.downcast_ref::<String>().cloned())
                                    .unwrap_or_default();
                                return Err(EngineError::Unit(format!(
                                    "unit {i} panicked: {what}"
                                )));
                            }
                        }
                    }
                    Ok(())
                })();
                if let Err(e) = result {
                    // The first failure is the one reported; then the others stop.
                    if let Ok(mut f) = failed.lock() {
                        f.get_or_insert(e);
                    }
                    stop.store(true, Ordering::Relaxed);
                }
                if let Ok(mut d) = done.lock() {
                    d.push(buckets);
                }
            });
        }
    });
    let mut all = done.into_inner().unwrap_or_default();
    let result = match failed.into_inner().ok().flatten() {
        Some(e) => Err(e),
        None => {
            let emitted = all
                .iter()
                .fold(Emitted::default(), |e, b| e.merge(b.emitted));
            check(emitted)
                .and_then(|()| room(&all, config, disk, log))
                .and_then(|()| merge(header, &mut all, threads, fan_in, dir.path(), out))
        }
    };
    drop(dir);
    result
}

/// Whether the merge has room, checked before its first line: on the spill
/// directory's filesystem, the largest cell once more for the cascade's
/// intermediate runs, and a margin; on the log's, if it is another, every
/// spilled byte (the log is about as large compressed). On the same filesystem
/// the log grows as each merged cell's runs are deleted.
fn room(
    all: &[Buckets],
    config: &Config,
    disk: &dyn Disk,
    log: Option<&std::path::Path>,
) -> Result<(), EngineError> {
    const MARGIN: u64 = 64 << 20;
    let mut cells: BTreeMap<u64, u64> = BTreeMap::new();
    for b in all {
        for (cell, runs) in &b.runs {
            for (path, _) in runs {
                let size = std::fs::metadata(path)
                    .map_err(at("measuring", path))?
                    .len();
                let c = cells.entry(*cell).or_insert(0);
                *c = c.saturating_add(size);
            }
        }
    }
    let largest = cells.values().copied().max().unwrap_or(0);
    let total = cells.values().copied().fold(0u64, u64::saturating_add);
    let mut needs: Vec<(PathBuf, u64)> =
        vec![(config.spill.clone(), largest.saturating_add(MARGIN))];
    if let Some(log) = log {
        let parent = log
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(std::path::Path::new("."));
        let same = disk.device(parent).map_err(at("inspecting", parent))?
            == disk
                .device(&config.spill)
                .map_err(at("inspecting", &config.spill))?;
        if !same {
            needs.push((parent.to_owned(), total.saturating_add(MARGIN)));
        }
    }
    for (path, needed) in needs {
        let free = disk.free(&path).map_err(at("inspecting", &path))?;
        if free < needed {
            return Err(EngineError::Space { path, needed, free });
        }
    }
    Ok(())
}

/// Each cell in turn: its held records sorted — on the threads, cell by cell —
/// and merged with its runs into the log; more runs than `fan_in` merged in a
/// cascade first; a cell's runs deleted as soon as it is merged.
fn merge<W: Write>(
    header: Header,
    all: &mut [Buckets],
    threads: usize,
    fan_in: usize,
    dir: &std::path::Path,
    out: W,
) -> Result<(W, Trailer), EngineError> {
    let mut writer = Writer::new(out, header)?;
    let mut cells: Vec<u64> = all
        .iter()
        .flat_map(|b| b.held.keys().chain(b.runs.keys()).copied())
        .collect();
    cells.sort_unstable();
    cells.dedup();
    for cell in cells {
        let mut held: Vec<Vec<Entry>> = all
            .iter_mut()
            .filter_map(|b| b.held.remove(&cell))
            .collect();
        std::thread::scope(|scope| {
            let size = held.len().div_ceil(threads).max(1);
            for chunk in held.chunks_mut(size) {
                scope.spawn(move || {
                    for entries in chunk {
                        entries.sort_unstable_by(|a, b| a.key.cmp(&b.key));
                    }
                });
            }
        });
        let runs: Vec<(PathBuf, u64)> = all
            .iter_mut()
            .filter_map(|b| b.runs.remove(&cell))
            .flatten()
            .collect();
        let runs = cascade(cell, runs, fan_in, dir)?;
        let mut sources: Vec<Source> = held
            .into_iter()
            .map(|v| Source::Held(v.into_iter()))
            .collect();
        for (path, count) in &runs {
            sources.push(Source::Run(open_run(path, *count)?));
        }
        kway(sources, |e| Ok(writer.line(&e.line)?))?;
        for (path, _) in &runs {
            let _ = std::fs::remove_file(path);
        }
    }
    Ok(writer.finish()?)
}

/// Merges groups of `fan_in` runs into one until no more than `fan_in` are
/// left; each group's runs are deleted as soon as it is merged. Equal keys fail
/// here as in the final merge.
fn cascade(
    cell: u64,
    mut runs: Vec<(PathBuf, u64)>,
    fan_in: usize,
    dir: &std::path::Path,
) -> Result<Vec<(PathBuf, u64)>, EngineError> {
    let mut level = 0u32;
    while runs.len() > fan_in {
        let mut next = Vec::with_capacity(runs.len().div_ceil(fan_in));
        for (g, group) in runs.chunks(fan_in).enumerate() {
            if let [single] = group {
                next.push(single.clone());
                continue;
            }
            let path = dir.join(format!("c{cell}-l{level}-g{g}.run"));
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(at("merging into", &path))?;
            let mut out = RunOut::new(file, &path)?;
            let mut count = 0u64;
            let sources = group
                .iter()
                .map(|(p, c)| open_run(p, *c).map(Source::Run))
                .collect::<Result<Vec<_>, _>>()?;
            kway(sources, |e| {
                count = count.saturating_add(1);
                out.write(&e)
            })?;
            out.finish()?;
            for (p, _) in group {
                let _ = std::fs::remove_file(p);
            }
            next.push((path, count));
        }
        runs = next;
        level = level.saturating_add(1);
    }
    Ok(runs)
}

/// A k-way merge of `sources` by key into `emit`; two equal keys fail.
fn kway(
    mut sources: Vec<Source>,
    mut emit: impl FnMut(Entry) -> Result<(), EngineError>,
) -> Result<(), EngineError> {
    let mut heap = BinaryHeap::new();
    let mut current: Vec<Option<Entry>> = Vec::with_capacity(sources.len());
    for (i, s) in sources.iter_mut().enumerate() {
        let e = s.next()?;
        if let Some(e) = &e {
            heap.push(Reverse((e.key.clone(), i)));
        }
        current.push(e);
    }
    let mut last: Option<Vec<u8>> = None;
    while let Some(Reverse((key, i))) = heap.pop() {
        let Some(entry) = current.get_mut(i).and_then(Option::take) else {
            continue;
        };
        if last.as_ref() == Some(&key) {
            return Err(equal(&entry));
        }
        last = Some(key);
        if let Some(s) = sources.get_mut(i) {
            let e = s.next()?;
            if let Some(e) = &e {
                heap.push(Reverse((e.key.clone(), i)));
            }
            if let Some(slot) = current.get_mut(i) {
                *slot = e;
            }
        }
        emit(entry)?;
    }
    Ok(())
}

/// The error for a record whose key another had: its time and step, read back
/// from its line.
fn equal(entry: &Entry) -> EngineError {
    match serde_json::from_slice::<Line>(entry.line.strip_suffix(b"\n").unwrap_or(&entry.line)) {
        Ok(Line::Step(r)) => EngineError::Equal {
            at: r.at,
            step: Box::new(r.step),
        },
        _ => EngineError::Unit("two records with one key".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Subject;
    use proptest::prelude::*;

    fn text() -> impl Strategy<Value = String> {
        // Short, over an alphabet with NUL, so that prefixes and zeros are common.
        proptest::collection::vec(prop_oneof![Just('\0'), Just('a'), Just('b')], 0..3)
            .prop_map(|v| v.into_iter().collect())
    }

    fn step_id() -> impl Strategy<Value = StepId> {
        (
            text(),
            proptest::collection::vec(0u8..3, 0..3),
            0u64..3,
            text(),
            0u32..3,
        )
            .prop_map(|(kind, scope, index, label, n)| StepId {
                subject: Subject { kind, scope, index },
                label,
                n,
            })
    }

    proptest! {
        /// The key's byte order is `(at, StepId)`'s order.
        #[test]
        fn the_key_orders_as_the_step_does(a in (0u64..3, step_id()), b in (0u64..3, step_id())) {
            let (ka, kb) = (order_key(a.0, &a.1), order_key(b.0, &b.1));
            prop_assert_eq!(ka.cmp(&kb), (a.0, &a.1).cmp(&(b.0, &b.1)));
        }
    }

    struct Failing;

    impl Read for Failing {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("disk"))
        }
    }

    fn run<R: Read>(reader: R, left: u64) -> Counted<R> {
        Counted {
            reader,
            left,
            path: "test.run".into(),
        }
    }

    /// A run's records are its count, no fewer and no more: an error reading
    /// it, a run that ends early — a cut record or a cut frame — and a run that
    /// goes on past its count all fail the generation.
    #[test]
    fn a_run_holds_exactly_its_count() {
        assert!(read_entry(&mut run(Failing, 1)).is_err());
        assert!(matches!(
            read_entry(&mut run(std::io::empty(), 0)),
            Ok(None)
        ));
        assert!(read_entry(&mut run(std::io::empty(), 1)).is_err());
        assert!(read_entry(&mut run(&[0u8, 0][..], 1)).is_err());
        assert!(read_entry(&mut run(&[0u8][..], 0)).is_err());
        let whole = [0u8, 0, 0, 0, 0, 0, 0, 0];
        assert!(matches!(read_entry(&mut run(&whole[..], 1)), Ok(Some(_))));
    }
}
