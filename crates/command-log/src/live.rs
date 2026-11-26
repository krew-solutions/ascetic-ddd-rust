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
use std::mem::size_of;
use std::path::Path;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::disk::{Disk, LocalDisk};
use crate::engine::{Config, Emitted, EngineError, Need, Unit, generate_checked};
use crate::log::{FORMAT, LogError, Partial, Reader, encode};
use crate::record::{Header, Line, Trailer};
use crate::validate::{Host, Limits, Report, Rule, spill_need, validate_on};

/// Bumped when a rule's or a committed peak's meaning changes, beside its
/// name: a commitment from another validator is stale (2: C5 added `rate` and
/// `key`; 3: decision 16 measured W behind the read cursor and stopped
/// `deviation` exempting itself; 4: ADR-0047's FORMAT 3 — `feasible`,
/// `measure`, `cause`, `until`, `sent`, writers awaiting readers settled, the
/// scheduler's premises not deviable; 5: a producer captures the value its
/// reader sends; 6: ADR-0047 K1e-2 — the plan's own bound and its peaks
/// committed, two hot parts beside them, `feasible` for every step's think; 7:
/// ADR-0047 K2a-4a — the profile's problem pointer, correlation header,
/// resend floor, answer cap and idle checked; 8: K2a-4b — `frame`,
/// `predicate`, `expect`, `secret`, a not-taken resend's codes, an idempotent
/// call's own slot, one producer a slot a record, no captured value in a raw
/// span; 9: ADR-0047 K2b-7d — a call's source within the profile's; 10:
/// K2b-7e-2b — the peaks of the records' shapes, of the steps late by a
/// jitter, of what the held keep, of each source's sockets, the barrier keys'
/// bytes; the largest line and chunk committed; 11: K2b-7e-2b-2 — the
/// largest record's shape, each count at its most over one record; 12:
/// K2b-7e-2b-2b — a shape's barrier dependencies' key bytes, which 10
/// missed; 13: K2b-7e-2b-2c — its largest predicate literal and its
/// longest pointer).
pub const RULES_REVISION: u32 = 13;

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
    /// What a run of it must hold (ADR-0047 K1e-2): the validator's peaks,
    /// the plan's own bound among them, which the scheduler's shell refuses
    /// before T0 when its machine cannot.
    pub peaks: crate::validate::Peaks,
    /// Its header line, in bytes, `\n` included (ADR-0047 decision 22): the
    /// first line a reader reads, at most this long.
    pub header_line: u64,
    /// Its largest step line, in bytes, `\n` included (ADR-0047 decision 22):
    /// a reader holds no longer one.
    pub line: u64,
    /// Its largest chunk of step lines, in bytes: a verifier holds a chunk
    /// until its prefix is checked.
    pub chunk_bytes: u64,
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
    /// The header line; the largest step line; the largest chunk, and the
    /// one being tallied.
    header_line: u64,
    line: u64,
    chunk_bytes: u64,
    this_chunk: u64,
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
            header_line: 0,
            line: 0,
            chunk_bytes: 0,
            this_chunk: 0,
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
            header_line,
            line: largest,
            chunk_bytes,
            this_chunk,
        } = self;
        let _ = lines.push(buf, |line| {
            if header.is_none() {
                chain.digest.update(line);
                *header = Some(chain.now());
                *header_line = u64::try_from(line.len()).unwrap_or(u64::MAX);
            } else if is_trailer(line) {
                sha256.get_or_insert_with(|| chain.now());
            } else if sha256.is_none() {
                emitted.add(line);
                let n = u64::try_from(line.len()).unwrap_or(u64::MAX);
                *largest = (*largest).max(n);
                *this_chunk = this_chunk.saturating_add(n);
                *chunk_bytes = (*chunk_bytes).max(*this_chunk);
                if chain.step(line).is_some() {
                    prefixes.push(chain.now());
                    *this_chunk = 0;
                }
            }
            Ok(())
        });
    }

    fn commitment(
        self,
        fingerprint: &str,
        chunk: u64,
        peaks: crate::validate::Peaks,
    ) -> Commitment {
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
            peaks,
            header_line: self.header_line,
            line: self.line,
            chunk_bytes: self.chunk_bytes,
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
    /// The line being read: reserved at its limit, never grown — a line past
    /// it is refused before a byte more is held (ADR-0047 decision 22).
    line: Vec<u8>,
    out: Vec<u8>,
    at: usize,
    /// The header line's limit, then a step's or the trailer's.
    limits: Bounds,
    /// Lines read.
    number: u64,
}

/// A replay's line limits, from its commitment.
#[derive(Clone, Copy, Debug)]
struct Bounds {
    header: usize,
    step: usize,
    chunk: usize,
}

impl Bounds {
    fn of(c: &Commitment) -> io::Result<Bounds> {
        let size = |n: u64| {
            usize::try_from(n).map_err(|_| io::Error::other("a commitment's size past this host's"))
        };
        Ok(Bounds {
            header: size(c.header_line)?,
            step: size(c.line.max(trailer_line()))?,
            chunk: size(c.chunk_bytes.saturating_add(trailer_line()))?,
        })
    }
}

/// The longest trailer line: the most steps, a SHA-256 in hex.
pub fn trailer_line() -> u64 {
    let trailer = Line::Trailer(Trailer {
        records: u64::MAX,
        sha256: "0".repeat(64),
    });
    crate::log::encode(&trailer).map_or(u64::MAX, |b| u64::try_from(b.len()).unwrap_or(u64::MAX))
}

/// `capacity` bytes, or the allocator's refusal as an error — never an abort.
fn reserved(capacity: usize) -> io::Result<Vec<u8>> {
    let mut v = Vec::new();
    v.try_reserve_exact(capacity)
        .map_err(|e| io::Error::new(io::ErrorKind::OutOfMemory, e))?;
    Ok(v)
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
        let limits = Bounds::of(commitment)?;
        Ok(Verified {
            inner,
            verifier: Verifier {
                chain: Chain::new(commitment.chunk),
                held: reserved(limits.chunk)?,
                commitment: commitment.clone(),
                started: false,
            },
            line: reserved(limits.header)?,
            out: reserved(limits.header)?,
            at: 0,
            limits,
            number: 0,
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
            if self.number == 1 {
                // The header released: its buffers let go, a step's taken.
                self.line = reserved(self.limits.step)?;
                self.out = reserved(self.limits.chunk)?;
            }
            self.out.clear();
            self.at = 0;
            self.line.clear();
            let limit = if self.number == 0 {
                self.limits.header
            } else {
                self.limits.step
            };
            self.number = self.number.saturating_add(1);
            let read = (&mut self.inner)
                .take(u64::try_from(limit).unwrap_or(u64::MAX))
                .read_until(b'\n', &mut self.line)?;
            if self.line.last() != Some(&b'\n') {
                if read == limit && !self.inner.fill_buf()?.is_empty() {
                    return Err(io::Error::other(Diverged {
                        what: format!("line {}, past its {limit} bytes,", self.number),
                    }));
                }
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
    replay_priced(path, commitment).map(|(reader, _)| reader)
}

/// The reader of a replay: a log's lines verified, then decoded.
pub type Replay = Reader<Verified<Box<dyn BufRead + Send>>>;

/// [`replay`], and what its reader holds by construction.
pub fn replay_priced(path: &Path, commitment: &Commitment) -> Result<(Replay, Reading), LogError> {
    let file = io::BufReader::with_capacity(crate::log::BUFFER, std::fs::File::open(path)?);
    let (input, compressed) = crate::log::decoded_as(file)?;
    let reader = Reader::new(Verified::new(input, commitment)?)?;
    let reading = reading(commitment, reader.header(), compressed);
    Ok((reader, reading))
}

/// What a replay's reader holds by construction (ADR-0047 decision 22,
/// K2b-7e-2b-2), the records it decodes apart — the shell prices those by the
/// plan's largest record, as many as its channel holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reading {
    /// Its input's buffers, each [`crate::log::BUFFER`]: the file's, and a
    /// compressed log's decoded output; the boxed reader over them.
    pub buffers: u64,
    /// The verifier's, each reserved at its limit and never grown: a line
    /// and a chunk released, the header's while it is read, a step's after —
    /// and a chunk held.
    pub verifier: u64,
    /// The commitment the verifier keeps.
    pub commitment: u64,
    /// A line as the log's reader copies it out of the verifier and decodes
    /// it, the header's or a step's: its copy, and serde's scratch for an
    /// escaped string, at worst twice the line — three lines.
    pub line: u64,
    /// The header, decoded once and shared with the run.
    pub header: u64,
    /// libzstd's context at [`crate::log::WINDOW_LOG`], outside the counted
    /// heap; none for a plain log.
    pub decoder: u64,
}

impl Reading {
    /// The counted heap's terms summed — [`Reading::decoder`] apart.
    pub fn heap(&self) -> u64 {
        let Reading {
            buffers,
            verifier,
            commitment,
            line,
            header,
            decoder: _,
        } = *self;
        [buffers, verifier, commitment, line, header]
            .into_iter()
            .fold(0, u64::saturating_add)
    }
}

/// libzstd's decoding context at [`crate::log::WINDOW_LOG`]: its window, its
/// input block, its own state — as `ZSTD_sizeof_DCtx` reports it once a frame
/// of that window is decoded (`tests/zstd.rs` holds it two-sided).
pub const DECODER: u64 = 2_600_000;

/// What a replay of `c`'s log, of header `h`, holds by construction.
pub fn reading(c: &Commitment, h: &Header, compressed: bool) -> Reading {
    let buffer = crate::log::BUFFER as u64;
    let file = size_of::<io::BufReader<std::fs::File>>() as u64;
    let buffers = if compressed {
        let boxed =
            size_of::<io::BufReader<zstd::Decoder<'static, io::BufReader<std::fs::File>>>>();
        buffer.saturating_mul(2).saturating_add(boxed as u64)
    } else {
        buffer.saturating_add(file)
    };
    let trailer = trailer_line();
    let step = c.line.max(trailer);
    let chunk = c.chunk_bytes.saturating_add(trailer);
    let reading_header = c.header_line.saturating_mul(2).saturating_add(chunk);
    let reading_steps = step.saturating_add(chunk.saturating_mul(2));
    Reading {
        buffers,
        verifier: reading_header.max(reading_steps),
        commitment: commitment_heap(c),
        line: c.header_line.max(c.line).saturating_mul(3),
        header: crate::shape::header(h),
        decoder: if compressed { DECODER } else { 0 },
    }
}

/// The bytes a commitment's clone holds: its strings, its prefixes, its peaks'
/// sockets, each at its length.
fn commitment_heap(c: &Commitment) -> u64 {
    let Commitment {
        format,
        validator,
        fingerprint,
        header,
        chunk: _,
        prefixes,
        records: _,
        multiset,
        sha256,
        peaks,
        header_line: _,
        line: _,
        chunk_bytes: _,
    } = c;
    let crate::validate::Peaks {
        completions: _,
        captures: _,
        polls: _,
        held: _,
        pending: _,
        horizon: _,
        barriers: _,
        pending_shape: _,
        late: _,
        late_shape: _,
        kept: _,
        sockets,
        barrier_bytes: _,
        record: _,
    } = peaks;
    let len = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
    let strings = [format, validator, fingerprint, header, multiset, sha256]
        .into_iter()
        .chain(prefixes)
        .fold(0u64, |n, s| n.saturating_add(len(s.len())));
    strings
        .saturating_add(len(prefixes.len().saturating_mul(size_of::<String>())))
        .saturating_add(len(sockets.len().saturating_mul(size_of::<u64>())))
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
        tally
            .commitment(fingerprint, chunk, report.peaks.clone())
            .write(commitment)?;
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
    let fan_in = crate::validate::fan_in(limits, disk).map_err(LogError::Io)?;
    let (tx, rx) = pipe(config.pipe_blocks);
    let out = Prefixing {
        inner: tx,
        tally: Tally::new(chunk),
    };
    let need = |e: Emitted| {
        if e.window > limits.hot {
            return Err(EngineError::Log(LogError::Hot {
                part: "completions",
                held: e.window,
                line: 0,
            }));
        }
        Ok(vec![Need {
            path: limits.spill.clone(),
            bytes: spill_need(e.run_bytes, limits, fan_in),
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
            .commitment(fingerprint, chunk, report.peaks.clone())
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
