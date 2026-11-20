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
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::log::{LogError, Writer, encode};
use crate::record::{Header, Line, Record, StepId, Trailer};

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

/// One thread's buckets: a cell's records in memory, and the runs it spilled.
struct Buckets {
    thread: usize,
    held: BTreeMap<u64, Vec<Entry>>,
    bytes: usize,
    runs: BTreeMap<u64, Vec<PathBuf>>,
    spilled: usize,
}

impl Buckets {
    fn push(&mut self, cell: u64, entry: Entry, config: &Config) -> Result<(), EngineError> {
        self.bytes = self
            .bytes
            .saturating_add(entry.key.len())
            .saturating_add(entry.line.len());
        self.held.entry(cell).or_default().push(entry);
        if self.bytes > config.budget {
            self.spill(config)?;
        }
        Ok(())
    }

    /// Every cell's records, sorted, to a run of its own.
    fn spill(&mut self, config: &Config) -> Result<(), EngineError> {
        for (cell, mut entries) in std::mem::take(&mut self.held) {
            entries.sort_unstable_by(|a, b| a.key.cmp(&b.key));
            let path = config
                .spill
                .join(format!("t{}-c{cell}-{}.run", self.thread, self.spilled));
            self.spilled = self.spilled.saturating_add(1);
            let mut out = BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)?,
            );
            for e in entries {
                write_entry(&mut out, &e)?;
            }
            out.flush()?;
            self.runs.entry(cell).or_default().push(path);
        }
        self.bytes = 0;
        Ok(())
    }
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

fn read_entry(r: &mut impl Read) -> Result<Option<Entry>, EngineError> {
    let mut len = [0u8; 4];
    match r.read_exact(&mut len) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let mut key = vec![0u8; u32::from_le_bytes(len) as usize];
    r.read_exact(&mut key)?;
    r.read_exact(&mut len)?;
    let mut line = vec![0u8; u32::from_le_bytes(len) as usize];
    r.read_exact(&mut line)?;
    Ok(Some(Entry { key, line }))
}

/// Where a cell's merge reads from: a spilled run, or what a thread still held.
enum Source {
    Run(BufReader<File>),
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
    let threads = config.threads.max(1);
    let cell = config.cell.max(1);
    let last = config.cells.max(1).saturating_sub(1);
    let next = AtomicUsize::new(0);
    let done: Mutex<Vec<Buckets>> = Mutex::new(Vec::new());
    let failed: Mutex<Option<EngineError>> = Mutex::new(None);
    std::thread::scope(|scope| {
        for thread in 0..threads {
            let (next, done, failed) = (&next, &done, &failed);
            scope.spawn(move || {
                let mut buckets = Buckets {
                    thread,
                    held: BTreeMap::new(),
                    bytes: 0,
                    runs: BTreeMap::new(),
                    spilled: 0,
                };
                let result = (|| -> Result<(), EngineError> {
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let Some(unit) = units.get(i) else {
                            return Ok(());
                        };
                        unit.emit(&mut |record: Record| {
                            let key = order_key(record.at, &record.step);
                            let at_cell = record.at.checked_div(cell).unwrap_or(0).min(last);
                            let line = encode(&Line::Step(Box::new(record)))?;
                            buckets.push(at_cell, Entry { key, line }, config)
                        })?;
                    }
                })();
                if let Err(e) = result
                    && let Ok(mut f) = failed.lock()
                {
                    f.get_or_insert(e);
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
        None => merge(header, &mut all, threads, out),
    };
    for b in &all {
        for path in b.runs.values().flatten() {
            let _ = fs::remove_file(path);
        }
    }
    result
}

/// Each cell in turn: its held records sorted — on the threads, cell by cell —
/// and merged with its runs into the log.
fn merge<W: Write>(
    header: Header,
    all: &mut [Buckets],
    threads: usize,
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
        let mut sources: Vec<Source> = held
            .into_iter()
            .map(|v| Source::Held(v.into_iter()))
            .collect();
        for b in all.iter() {
            for path in b.runs.get(&cell).into_iter().flatten() {
                sources.push(Source::Run(BufReader::new(File::open(path)?)));
            }
        }
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
            writer.line(&entry.line)?;
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
        }
    }
    Ok(writer.finish()?)
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

    /// An error reading a run fails the generation: a run cut short would drop
    /// its steps from the log.
    #[test]
    fn a_failing_run_fails_the_generation() {
        assert!(read_entry(&mut Failing).is_err());
        assert!(matches!(read_entry(&mut std::io::empty()), Ok(None)));
    }
}
