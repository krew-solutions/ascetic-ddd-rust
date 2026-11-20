//! A run with no log (ADR-0046 decision 15): a dry generation, validated, leaves
//! a commitment; a live generation of the same plan is released chunk by chunk,
//! each only once its prefix digest equals the commitment's. A persisted log
//! validated by [`commit_file`] leaves the same commitment, and its replay goes
//! through the same reader, [`Verified`].
//!
//! The digests are the log's own: `Pᵢ` is the SHA-256 of the header line and
//! the first `i · chunk` step lines — `head -n (1 + i·chunk) | sha256sum` of the
//! log a persisted generation would have written — and the last is the
//! trailer's. A live generation is refused before it emits if its header (`P₀`)
//! or the identities differ, before it releases a line if what it emitted (the
//! step count and the order-independent digest) differs, and stops before the
//! first chunk whose `Pᵢ` differs: it never offers load the dry run did not
//! validate.

use std::io::{self, BufRead, Read, Write};
use std::path::Path;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::disk::{Disk, LocalDisk};
use crate::engine::{Config, Emitted, EngineError, Need, Unit, generate_checked};
use crate::log::{FORMAT, LogError, Partial, Reader, encode};
use crate::record::{Header, Line, Trailer};
use crate::validate::{Host, Limits, Report, Rule, spill_need, validate_on};

/// Bumped when a rule's meaning changes, beside its name: a commitment from
/// another validator is stale (2: C5 added `rate` and `key`; 3: decision 16
/// measured W behind the read cursor and stopped `deviation` exempting itself).
pub const RULES_REVISION: u32 = 3;

/// The validator's identity: its revision and its rules' names.
pub fn validator() -> String {
    let names: Vec<&str> = Rule::ALL.iter().map(|r| r.name()).collect();
    format!("{RULES_REVISION}: {}", names.join(" "))
}

/// Step lines a chunk: about 30 MB of lines at M7's sizes.
pub const CHUNK: u64 = 1 << 16;

/// What a dry run validated, for a live run to prove itself against.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Commitment {
    /// The byte form's version (`FORMAT`).
    pub format: String,
    /// The validator's identity.
    pub validator: String,
    /// The host's fingerprint of its generator: its build, its plan revision.
    pub fingerprint: String,
    /// `P₀`: the SHA-256 of the header line.
    pub header: String,
    /// Step lines a chunk.
    pub chunk: u64,
    /// `Pᵢ` for each whole chunk, in order.
    pub prefixes: Vec<String>,
    /// What was emitted: the step count.
    pub records: u64,
    /// What was emitted: the order-independent digest, in hex.
    pub multiset: String,
    /// The trailer's digest: of the header and every step line.
    pub sha256: String,
}

impl Commitment {
    /// Writes the commitment to `path`, whole or not at all.
    pub fn write(&self, path: &Path) -> io::Result<()> {
        let (partial, mut file) = Partial::create(path)?;
        serde_json::to_writer(&mut file, self).map_err(io::Error::other)?;
        file.write_all(b"\n")?;
        partial.commit(file)
    }

    /// Reads a commitment.
    pub fn read(path: &Path) -> io::Result<Commitment> {
        serde_json::from_slice(&std::fs::read(path)?).map_err(io::Error::other)
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The digest of a log's lines as they pass, with its prefixes at chunk
/// boundaries.
struct Chain {
    digest: Sha256,
    steps: u64,
    chunk: u64,
}

impl Chain {
    fn new(chunk: u64) -> Chain {
        Chain {
            digest: Sha256::new(),
            steps: 0,
            chunk: chunk.max(1),
        }
    }

    fn now(&self) -> String {
        hex(&self.digest.clone().finalize())
    }

    /// Hashes a line; `Some(i)` when it completes chunk `i` (from 1).
    fn step(&mut self, line: &[u8]) -> Option<u64> {
        self.digest.update(line);
        self.steps = self.steps.saturating_add(1);
        self.steps
            .is_multiple_of(self.chunk)
            .then(|| self.steps.checked_div(self.chunk).unwrap_or(0))
    }
}

/// Splits what is written into whole lines.
#[derive(Default)]
struct Lines {
    pending: Vec<u8>,
}

impl Lines {
    fn push(
        &mut self,
        buf: &[u8],
        mut line: impl FnMut(&[u8]) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut rest = buf;
        while let Some(end) = rest.iter().position(|&b| b == b'\n') {
            let (head, tail) = rest.split_at(end.saturating_add(1));
            if self.pending.is_empty() {
                line(head)?;
            } else {
                self.pending.extend_from_slice(head);
                let whole = std::mem::take(&mut self.pending);
                line(&whole)?;
            }
            rest = tail;
        }
        self.pending.extend_from_slice(rest);
        Ok(())
    }
}

fn is_trailer(line: &[u8]) -> bool {
    line.starts_with(b"{\"trailer\":")
}

/// What a commitment needs of a log, gathered as its lines pass.
struct Tally {
    lines: Lines,
    chain: Chain,
    header: Option<String>,
    prefixes: Vec<String>,
    emitted: Emitted,
    sha256: Option<String>,
}

impl Tally {
    fn new(chunk: u64) -> Tally {
        Tally {
            lines: Lines::default(),
            chain: Chain::new(chunk),
            header: None,
            prefixes: Vec::new(),
            emitted: Emitted::default(),
            sha256: None,
        }
    }

    fn push(&mut self, buf: &[u8]) {
        let Tally {
            lines,
            chain,
            header,
            prefixes,
            emitted,
            sha256,
        } = self;
        let _ = lines.push(buf, |line| {
            if header.is_none() {
                chain.digest.update(line);
                *header = Some(chain.now());
            } else if is_trailer(line) {
                sha256.get_or_insert_with(|| chain.now());
            } else if sha256.is_none() {
                emitted.add(line);
                if chain.step(line).is_some() {
                    prefixes.push(chain.now());
                }
            }
            Ok(())
        });
    }

    fn commitment(self, fingerprint: &str, chunk: u64) -> Commitment {
        Commitment {
            format: FORMAT.to_owned(),
            validator: validator(),
            fingerprint: fingerprint.to_owned(),
            header: self.header.unwrap_or_default(),
            chunk: chunk.max(1),
            prefixes: self.prefixes,
            records: self.emitted.records,
            multiset: format!("{:032x}", self.emitted.multiset),
            sha256: self.sha256.unwrap_or_default(),
        }
    }
}

/// A writer that passes a dry generation's lines on and tallies them.
struct Prefixing<W> {
    inner: W,
    tally: Tally,
}

impl<W: Write> Write for Prefixing<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.tally.push(buf);
        self.inner.write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// A reader that tallies a persisted log's lines as they are read.
struct Tee<'a, R> {
    inner: R,
    tally: &'a mut Tally,
}

impl<R: Read> Read for Tee<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.tally.push(buf.get(..n).unwrap_or(&[]));
        Ok(n)
    }
}

/// What differs between a log and its commitment; carried in an
/// [`io::Error`] by [`Verified`] and told apart as [`LogError::Diverged`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diverged {
    /// The format, the validator, the header, or a chunk.
    pub what: String,
}

impl std::fmt::Display for Diverged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} differs from the commitment", self.what)
    }
}

impl std::error::Error for Diverged {}

/// A log's lines checked against a commitment as they pass: the header at
/// once, each chunk once its last line makes `Pᵢ`, the last lines with the
/// trailer's digest. Nothing is released before it is checked.
struct Verifier {
    chain: Chain,
    held: Vec<u8>,
    commitment: Commitment,
    started: bool,
}

impl Verifier {
    fn line(&mut self, line: &[u8], released: &mut Vec<u8>) -> Result<(), Diverged> {
        let diverged = |what: String| Diverged { what };
        if !self.started {
            self.started = true;
            self.chain.digest.update(line);
            if self.chain.now() != self.commitment.header {
                return Err(diverged("the header".to_owned()));
            }
            released.extend_from_slice(line);
            return Ok(());
        }
        if is_trailer(line) {
            if self.chain.steps != self.commitment.records
                || self.chain.now() != self.commitment.sha256
            {
                let last = self.chain.steps.div_ceil(self.chain.chunk);
                return Err(diverged(format!("chunk {last}, the last")));
            }
            released.append(&mut self.held);
            released.extend_from_slice(line);
            return Ok(());
        }
        self.held.extend_from_slice(line);
        if let Some(i) = self.chain.step(line) {
            let expected = usize::try_from(i.saturating_sub(1))
                .ok()
                .and_then(|k| self.commitment.prefixes.get(k));
            if expected != Some(&self.chain.now()) {
                return Err(diverged(format!("chunk {i}")));
            }
            released.append(&mut self.held);
        }
        Ok(())
    }
}

/// A log's bytes, released chunk by chunk only once each matches the
/// commitment: the one reader a live run and a persisted replay go through.
/// A cut last line ends it unreleased, so the log reads as truncated; a
/// divergence is an error carrying [`Diverged`]. Nothing after a divergence
/// can match again: each prefix digest chains every byte before it.
pub struct Verified<R> {
    inner: R,
    verifier: Verifier,
    line: Vec<u8>,
    out: Vec<u8>,
    at: usize,
}

impl<R: BufRead> Verified<R> {
    /// Refused at once if the commitment is of another format or validator.
    pub fn new(inner: R, commitment: &Commitment) -> Result<Verified<R>, LogError> {
        let diverged = |what: &str| LogError::Diverged {
            what: what.to_owned(),
        };
        if commitment.format != FORMAT {
            return Err(diverged("the format"));
        }
        if commitment.validator != validator() {
            return Err(diverged("the validator"));
        }
        Ok(Verified {
            inner,
            verifier: Verifier {
                chain: Chain::new(commitment.chunk),
                held: Vec::new(),
                commitment: commitment.clone(),
                started: false,
            },
            line: Vec::new(),
            out: Vec::new(),
            at: 0,
        })
    }
}

impl<R: BufRead> Read for Verified<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let available = self.fill_buf()?;
        let n = available.len().min(buf.len());
        if let (Some(dst), Some(src)) = (buf.get_mut(..n), available.get(..n)) {
            dst.copy_from_slice(src);
        }
        self.consume(n);
        Ok(n)
    }
}

impl<R: BufRead> BufRead for Verified<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        while self.at >= self.out.len() {
            self.out.clear();
            self.at = 0;
            self.line.clear();
            self.inner.read_until(b'\n', &mut self.line)?;
            if self.line.last() != Some(&b'\n') {
                // The end, or a line cut short: nothing more is released.
                return Ok(&[]);
            }
            if let Err(d) = self.verifier.line(&self.line, &mut self.out) {
                self.out.clear();
                return Err(io::Error::other(d));
            }
        }
        Ok(self.out.get(self.at..).unwrap_or(&[]))
    }

    fn consume(&mut self, amt: usize) {
        self.at = self.at.saturating_add(amt).min(self.out.len());
    }
}

/// A persisted log, plain or compressed, read through its commitment.
pub fn replay(
    path: &Path,
    commitment: &Commitment,
) -> Result<Reader<Verified<Box<dyn BufRead + Send>>>, LogError> {
    let input = crate::log::decoded(io::BufReader::new(std::fs::File::open(path)?))?;
    Reader::new(Verified::new(input, commitment)?)
}

/// A persisted log validated, and its commitment written to `commitment`
/// only when the report is clean: the same commitment a dry run of the plan
/// that wrote it leaves.
#[allow(clippy::too_many_arguments)]
pub fn commit_file(
    path: &Path,
    host: &impl Host,
    limits: &Limits,
    fingerprint: &str,
    chunk: u64,
    commitment: &Path,
    disk: &dyn Disk,
) -> Result<Report, LogError> {
    let mut tally = Tally::new(chunk);
    let input = crate::log::decoded(io::BufReader::new(std::fs::File::open(path)?))?;
    let report = {
        let tee = io::BufReader::new(Tee {
            inner: input,
            tally: &mut tally,
        });
        validate_on(Reader::new(tee)?, host, limits, disk)?
    };
    if report.violations.is_empty() && report.violated == 0 {
        tally.commitment(fingerprint, chunk).write(commitment)?;
    }
    Ok(report)
}

/// A bounded pipe between a generation and its consumer: `capacity` blocks of
/// up to 1 MiB in flight, the generation blocked past them.
pub(crate) fn pipe(capacity: usize) -> (PipeWriter, PipeReader) {
    let (tx, rx) = sync_channel(capacity.max(1));
    (
        PipeWriter {
            tx: Some(tx),
            block: Vec::new(),
        },
        PipeReader {
            rx,
            block: Vec::new(),
            at: 0,
        },
    )
}

const BLOCK: usize = 1 << 20;

pub(crate) struct PipeWriter {
    tx: Option<SyncSender<Vec<u8>>>,
    block: Vec<u8>,
}

impl PipeWriter {
    fn send(&mut self) -> io::Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        let block = std::mem::take(&mut self.block);
        match &self.tx {
            Some(tx) => tx
                .send(block)
                .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe)),
            None => Err(io::Error::from(io::ErrorKind::BrokenPipe)),
        }
    }

    /// Sends what is left and closes the pipe.
    pub(crate) fn close(mut self) -> io::Result<()> {
        self.send()?;
        self.tx = None;
        Ok(())
    }
}

impl Write for PipeWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.block.extend_from_slice(buf);
        if self.block.len() >= BLOCK {
            self.send()?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) struct PipeReader {
    rx: Receiver<Vec<u8>>,
    block: Vec<u8>,
    at: usize,
}

impl Read for PipeReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let available = self.fill_buf()?;
        let n = available.len().min(buf.len());
        if let (Some(dst), Some(src)) = (buf.get_mut(..n), available.get(..n)) {
            dst.copy_from_slice(src);
        }
        self.consume(n);
        Ok(n)
    }
}

impl BufRead for PipeReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        while self.at >= self.block.len() {
            match self.rx.recv() {
                Ok(block) => {
                    self.block = block;
                    self.at = 0;
                }
                Err(_) => return Ok(&[]),
            }
        }
        Ok(self.block.get(self.at..).unwrap_or(&[]))
    }

    fn consume(&mut self, amt: usize) {
        self.at = self.at.saturating_add(amt).min(self.block.len());
    }
}

/// A dry run: the plan generated into a bounded pipe, validated as it comes,
/// nothing persisted but spills. Writes the commitment to `commitment` only
/// when the report is clean, and returns the report either way. A failure of
/// the generation is reported before what the validator made of the cut
/// stream.
#[allow(clippy::too_many_arguments)]
pub fn dry(
    header: Header,
    units: &[&dyn Unit],
    config: &Config,
    host: &(impl Host + Sync),
    limits: &Limits,
    fingerprint: &str,
    chunk: u64,
    commitment: &Path,
) -> Result<Report, EngineError> {
    dry_on(
        header,
        units,
        config,
        host,
        limits,
        fingerprint,
        chunk,
        commitment,
        &LocalDisk,
    )
}

/// [`dry`] on a given disk. The space checked before the merge's first line
/// counts the validator's id runs, which grow while the merge runs.
#[allow(clippy::too_many_arguments)]
pub fn dry_on(
    header: Header,
    units: &[&dyn Unit],
    config: &Config,
    host: &(impl Host + Sync),
    limits: &Limits,
    fingerprint: &str,
    chunk: u64,
    commitment: &Path,
    disk: &dyn Disk,
) -> Result<Report, EngineError> {
    // The validator's directory exists before the space check measures it:
    // the validator itself starts only with the merge's first line.
    std::fs::create_dir_all(&limits.spill).map_err(crate::engine::at(
        "claiming a spill directory in",
        &limits.spill,
    ))?;
    let fan_in = match limits.fan_in {
        0 => crate::disk::fan_in(disk.open_files().map_err(LogError::Io)?),
        n => n.max(2),
    };
    let (tx, rx) = pipe(config.pipe_blocks);
    let out = Prefixing {
        inner: tx,
        tally: Tally::new(chunk),
    };
    let need = |e: Emitted| {
        Ok(vec![Need {
            path: limits.spill.clone(),
            bytes: spill_need(e.records, limits, fan_in),
        }])
    };
    let (generated, validated) = std::thread::scope(|scope| {
        let generator = scope.spawn(|| {
            // On a failure the pipe closes as `out` drops: the validator sees
            // the cut.
            let (out, _) = generate_checked(header, units, config, disk, out, &need)?;
            out.inner.close()?;
            Ok(out.tally)
        });
        let validated = Reader::new(rx).and_then(|reader| validate_on(reader, host, limits, disk));
        let generated = generator
            .join()
            .unwrap_or_else(|_| Err(EngineError::Unit("the generation panicked".into())));
        (generated, validated)
    });
    let tally = generated?;
    let report = validated.map_err(EngineError::Log)?;
    if report.violations.is_empty() && report.violated == 0 {
        tally
            .commitment(fingerprint, chunk)
            .write(commitment)
            .map_err(|e| EngineError::Log(LogError::Io(e)))?;
    }
    Ok(report)
}

/// A live run: the plan generated again into a bounded pipe and read through
/// [`Verified`] into `out`, each chunk once its prefix digest equals
/// `commitment`'s. Refused before it emits on a stale commitment, before it
/// writes a line on a different emission, and stopped before the first chunk
/// that differs. A stalled `out` stalls the generation: the pipe holds
/// [`Config::pipe_blocks`].
pub fn live<W: Write>(
    header: Header,
    units: &[&dyn Unit],
    config: &Config,
    commitment: &Commitment,
    fingerprint: &str,
    mut out: W,
) -> Result<(W, Trailer), EngineError> {
    let diverged = |what: &str| EngineError::Diverged {
        what: what.to_owned(),
    };
    if commitment.fingerprint != fingerprint {
        return Err(diverged("the fingerprint"));
    }
    let (tx, rx) = pipe(config.pipe_blocks);
    let verified = Verified::new(rx, commitment).map_err(|e| match e {
        LogError::Diverged { what } => EngineError::Diverged { what },
        e => EngineError::Log(e),
    })?;
    let p0 = hex(&Sha256::digest(encode(&Line::Header(Box::new(
        header.clone(),
    )))?));
    if p0 != commitment.header {
        return Err(diverged("the header"));
    }
    let expected = (commitment.records, commitment.multiset.clone());
    let check = |e: Emitted| {
        if (e.records, format!("{:032x}", e.multiset)) == expected {
            Ok(Vec::new())
        } else {
            Err(diverged("what was emitted"))
        }
    };
    let (generated, copied) = std::thread::scope(|scope| {
        let generator = scope.spawn(|| {
            let (tx, trailer) = generate_checked(header, units, config, &LocalDisk, tx, &check)?;
            tx.close()?;
            Ok(trailer)
        });
        // The reader moved in and dropped here, whatever the copy did: a
        // generation blocked on a full pipe is let go before it is joined.
        let copied = {
            let mut verified = verified;
            io::copy(&mut verified, &mut out)
        };
        let generated = generator
            .join()
            .unwrap_or_else(|_| Err(EngineError::Unit("the generation panicked".into())));
        (generated, copied)
    });
    // What the reader saw first: a divergence, or `out` failing — the
    // generation then fails for want of a reader. Else the generation's own.
    match (copied.map_err(LogError::from), generated) {
        (Err(LogError::Diverged { what }), _) => Err(EngineError::Diverged { what }),
        (Err(e), _) => Err(EngineError::Log(e)),
        (Ok(_), Err(e)) => Err(e),
        (Ok(_), Ok(trailer)) => Ok((out, trailer)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A stalled consumer keeps memory flat: past `capacity` blocks and the
    /// one being filled, the writer blocks.
    #[test]
    fn a_stalled_consumer_blocks_the_writer() {
        let (mut tx, rx) = pipe(4);
        let written = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let block = vec![b'x'; BLOCK];
                while tx.write_all(&block).is_ok() {
                    written.fetch_add(1, Ordering::SeqCst);
                }
            });
            std::thread::sleep(std::time::Duration::from_millis(300));
            let held = written.load(Ordering::SeqCst);
            assert!((4..=5).contains(&held), "{held} blocks in flight");
            drop(rx);
        });
    }
}
