//! The validator's cold parts (ADR-0046 decision 16): events of one type held
//! in memory up to what the pass gives them, sorted runs on disk past it, and
//! one merge at the end that hands every event back in order. Each write is
//! preceded by a look at the disk, so that a disk running out fails the pass
//! by name, not hours later in a write; an observer hears the bytes held.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::disk::Disk;
use crate::engine::Observer;
use crate::log::LogError;
use crate::spill::Counted;

/// Free space kept beside a run being written.
pub(crate) const RESERVE: u64 = 64 << 20;

/// A run read in blocks this large: a merge of many runs on a rotating disk
/// seeks once a block, not once a page.
pub(crate) const READ_BUFFER: usize = 1 << 20;

/// What a run holds, in the order the merge hands back.
pub(crate) trait Event: Ord + Sized {
    /// Its bytes in a run, and near enough in memory to budget by.
    fn size(&self) -> usize;
    /// Writes it to a run.
    fn write(&self, out: &mut dyn Write) -> io::Result<()>;
    /// Reads the next of a run, whose count `Counted` keeps.
    fn read<R: Read>(run: &mut Counted<R>) -> io::Result<Self>;
}

/// One type's events: a buffer, then runs.
pub(crate) struct Sorter<E> {
    name: &'static str,
    dir: PathBuf,
    buffer: Vec<E>,
    held: usize,
    runs: Vec<(PathBuf, u64, u64)>,
    fan_in: usize,
    /// Bytes of this sorter's runs on disk now.
    on_disk: u64,
    progress: Option<Observer>,
}

impl<E: Event> Sorter<E> {
    pub(crate) fn new(
        name: &'static str,
        dir: &Path,
        fan_in: usize,
        progress: Option<Observer>,
    ) -> Sorter<E> {
        Sorter {
            name,
            dir: dir.to_owned(),
            buffer: Vec::new(),
            held: 0,
            runs: Vec::new(),
            fan_in: fan_in.max(2),
            on_disk: 0,
            progress,
        }
    }

    /// Bytes held in memory.
    pub(crate) fn held(&self) -> usize {
        self.held
    }

    pub(crate) fn push(&mut self, e: E) {
        self.held = self.held.saturating_add(e.size());
        self.buffer.push(e);
    }

    /// The buffer, sorted, to a run of its own.
    pub(crate) fn spill(&mut self, disk: &dyn Disk) -> Result<(), LogError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.buffer.sort_unstable();
        let path = self
            .dir
            .join(format!("{}-{}.run", self.name, self.runs.len()));
        let count = self.buffer.len() as u64;
        let bytes = self.held as u64;
        room(disk, &self.dir, bytes)?;
        let mut out = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?,
        );
        for e in self.buffer.drain(..) {
            e.write(&mut out)?;
        }
        out.flush()?;
        drop(out);
        self.held = 0;
        self.runs.push((path, count, bytes));
        self.wrote(bytes);
        Ok(())
    }

    /// `bytes` more on disk, heard by the observer.
    fn wrote(&mut self, bytes: u64) {
        self.on_disk = self.on_disk.saturating_add(bytes);
        self.tell();
    }

    fn tell(&self) {
        if let Some(Observer(p)) = &self.progress {
            p.runs(self.name, self.on_disk);
        }
    }

    /// Every event, in order, to `each`; the runs deleted as they are merged.
    pub(crate) fn drain(
        mut self,
        disk: &dyn Disk,
        mut each: impl FnMut(E) -> Result<(), LogError>,
    ) -> Result<(), LogError> {
        if self.runs.is_empty() {
            self.buffer.sort_unstable();
            for e in std::mem::take(&mut self.buffer) {
                each(e)?;
            }
            return Ok(());
        }
        self.spill(disk)?;
        let mut level = 0u32;
        while self.runs.len() > self.fan_in {
            let runs = std::mem::take(&mut self.runs);
            let mut next = Vec::with_capacity(runs.len().div_ceil(self.fan_in));
            for (g, group) in runs.chunks(self.fan_in).enumerate() {
                if let [single] = group {
                    next.push(single.clone());
                    continue;
                }
                let path = self.dir.join(format!("{}-l{level}-g{g}.run", self.name));
                let bytes = group.iter().map(|r| r.2).fold(0u64, u64::saturating_add);
                room(disk, &self.dir, bytes)?;
                let mut out = BufWriter::new(
                    OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&path)?,
                );
                let mut count = 0u64;
                merge(group, |e: E| {
                    count = count.saturating_add(1);
                    e.write(&mut out).map_err(LogError::from)
                })?;
                out.flush()?;
                drop(out);
                self.wrote(bytes);
                for (p, _, _) in group {
                    let _ = fs::remove_file(p);
                }
                // Heard with the next write: a deletion never makes a peak.
                self.on_disk = self.on_disk.saturating_sub(bytes);
                next.push((path, count, bytes));
            }
            self.runs = next;
            level = level.saturating_add(1);
        }
        let result = merge(&self.runs, &mut each);
        for (p, _, _) in &self.runs {
            let _ = fs::remove_file(p);
        }
        self.on_disk = 0;
        self.tell();
        result
    }
}

/// Whether `bytes` and the reserve fit where `dir` is.
fn room(disk: &dyn Disk, dir: &Path, bytes: u64) -> Result<(), LogError> {
    let free = disk.free(dir)?;
    let needed = bytes.saturating_add(RESERVE);
    if free < needed {
        return Err(LogError::Space {
            path: dir.to_owned(),
            needed,
            free,
        });
    }
    Ok(())
}

/// A k-way merge of sorted runs into `each`, ties by run, so that runs
/// written in line order keep it.
fn merge<E: Event>(
    runs: &[(PathBuf, u64, u64)],
    mut each: impl FnMut(E) -> Result<(), LogError>,
) -> Result<(), LogError> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;
    let mut readers: Vec<Counted<BufReader<File>>> = runs
        .iter()
        .map(|(p, left, _)| {
            File::open(p).map(|f| Counted {
                reader: BufReader::with_capacity(READ_BUFFER, f),
                left: *left,
                path: p.clone(),
            })
        })
        .collect::<Result<_, _>>()?;
    let mut heap = BinaryHeap::new();
    for (i, r) in readers.iter_mut().enumerate() {
        if let Some(e) = next(r)? {
            heap.push(Reverse((e, i)));
        }
    }
    while let Some(Reverse((e, i))) = heap.pop() {
        each(e)?;
        if let Some(r) = readers.get_mut(i)
            && let Some(e) = next(r)?
        {
            heap.push(Reverse((e, i)));
        }
    }
    Ok(())
}

/// A run's next event, or `None` past its last: a run that ends early — an
/// event cut short — or goes on past its count is an error, never fewer
/// events.
pub(crate) fn next<E: Event, R: Read>(run: &mut Counted<R>) -> Result<Option<E>, LogError> {
    if !run.due()? {
        return Ok(None);
    }
    run.left = run.left.saturating_sub(1);
    Ok(Some(E::read(run)?))
}

/// The disk a pass of `bytes` of events needs, `held` of them a run at the
/// least and `fan_in` wide: the runs, and while a cascade merges a group, the
/// group again — taken whole, to be safe on the side of refusing.
pub(crate) fn need(bytes: u64, held: u64, fan_in: usize) -> u64 {
    let runs = bytes.div_ceil(held.max(1));
    if runs <= fan_in as u64 {
        bytes
    } else {
        bytes.saturating_mul(2)
    }
}

/// A step id's identity in the validator's runs and hot parts: a 128-bit
/// SipHash-1-3 of a canonical form of it — 2⁻⁶⁹ for a collision at 10⁹, for
/// logs not crafted to collide.
pub(crate) fn digest(bytes: &[u8]) -> [u8; 16] {
    use std::hash::Hasher;
    let mut h = siphasher::sip128::SipHasher13::new_with_keys(0, 0);
    h.write(bytes);
    siphasher::sip128::Hasher128::finish128(&h)
        .as_u128()
        .to_be_bytes()
}
