//! A run with no log (ADR-0046 decision 15): a dry generation, validated, leaves
//! a commitment; a live generation of the same plan is released chunk by chunk,
//! each only once its prefix digest equals the commitment's.
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
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::engine::{Config, Emitted, EngineError, Unit, generate_checked};
use crate::log::{FORMAT, LogError, Partial, Reader, encode};
use crate::record::{Header, Line, Trailer};
use crate::validate::{Host, Limits, Report, Rule, validate};

/// Bumped when a rule's meaning changes, beside its name: a commitment from
/// another validator is stale (C5 added `rate` and `key`).
pub const RULES_REVISION: u32 = 2;

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
    /// The trailer's digest: every step line's.
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

/// A writer that passes a dry generation's lines on and keeps its prefixes.
struct Prefixing<W> {
    inner: W,
    lines: Lines,
    chain: Chain,
    header: Option<String>,
    prefixes: Vec<String>,
}

impl<W: Write> Write for Prefixing<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let (chain, header, prefixes) = (&mut self.chain, &mut self.header, &mut self.prefixes);
        self.lines.push(buf, |line| {
            if header.is_none() {
                chain.digest.update(line);
                *header = Some(chain.now());
            } else if !is_trailer(line) && chain.step(line).is_some() {
                prefixes.push(chain.now());
            }
            Ok(())
        })?;
        self.inner.write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// A writer that releases a live generation's lines chunk by chunk, each only
/// once its prefix digest equals the commitment's.
struct Verified<W> {
    inner: W,
    lines: Lines,
    chain: Chain,
    held: Vec<u8>,
    commitment: Arc<Commitment>,
    started: bool,
    /// The first chunk that diverged, for the error.
    diverged: Arc<Mutex<Option<String>>>,
}

impl<W: Write> Verified<W> {
    fn refuse(diverged: &Mutex<Option<String>>, what: String) -> io::Error {
        if let Ok(mut d) = diverged.lock() {
            d.get_or_insert(what.clone());
        }
        io::Error::other(what)
    }
}

impl<W: Write> Write for Verified<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let Verified {
            inner,
            lines,
            chain,
            held,
            commitment,
            started,
            diverged,
        } = self;
        lines.push(buf, |line| {
            if !*started {
                *started = true;
                chain.digest.update(line);
                if chain.now() != commitment.header {
                    return Err(Self::refuse(diverged, "the header".to_owned()));
                }
                return inner.write_all(line);
            }
            if is_trailer(line) {
                if chain.steps != commitment.records || chain.now() != commitment.sha256 {
                    let last = chain.steps.div_ceil(chain.chunk);
                    return Err(Self::refuse(diverged, format!("chunk {last}, the last")));
                }
                inner.write_all(held)?;
                held.clear();
                return inner.write_all(line);
            }
            held.extend_from_slice(line);
            if let Some(i) = chain.step(line) {
                let expected = usize::try_from(i.saturating_sub(1))
                    .ok()
                    .and_then(|k| commitment.prefixes.get(k));
                if expected != Some(&chain.now()) {
                    return Err(Self::refuse(diverged, format!("chunk {i}")));
                }
                inner.write_all(held)?;
                held.clear();
            }
            Ok(())
        })?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
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
    let (tx, rx) = pipe(64);
    let emitted: Mutex<Option<Emitted>> = Mutex::new(None);
    let out = Prefixing {
        inner: tx,
        lines: Lines::default(),
        chain: Chain::new(chunk),
        header: None,
        prefixes: Vec::new(),
    };
    let (generated, validated) = std::thread::scope(|scope| {
        let generator = scope.spawn(|| {
            let record = |e: Emitted| {
                if let Ok(mut slot) = emitted.lock() {
                    *slot = Some(e);
                }
                Ok(())
            };
            let result = generate_checked(header, units, config, out, &record);
            match result {
                Ok((out, trailer)) => {
                    let Prefixing {
                        inner,
                        header,
                        prefixes,
                        ..
                    } = out;
                    inner.close()?;
                    Ok((header, prefixes, trailer))
                }
                // The pipe closes as `out` drops: the validator sees the cut.
                Err(e) => Err(e),
            }
        });
        let validated = Reader::new(rx).and_then(|reader| validate(reader, host, limits));
        let generated = generator
            .join()
            .unwrap_or_else(|_| Err(EngineError::Unit("the generation panicked".into())));
        (generated, validated)
    });
    let (p0, prefixes, trailer) = generated?;
    let report = validated.map_err(EngineError::Log)?;
    let emitted = emitted
        .into_inner()
        .ok()
        .flatten()
        .ok_or_else(|| EngineError::Unit("the generation reported nothing emitted".into()))?;
    if report.violations.is_empty() && report.violated == 0 {
        Commitment {
            format: FORMAT.to_owned(),
            validator: validator(),
            fingerprint: fingerprint.to_owned(),
            header: p0.unwrap_or_default(),
            chunk: chunk.max(1),
            prefixes,
            records: emitted.records,
            multiset: format!("{:032x}", emitted.multiset),
            sha256: trailer.sha256,
        }
        .write(commitment)
        .map_err(|e| EngineError::Log(LogError::Io(e)))?;
    }
    Ok(report)
}

/// A live run: the plan generated again and released into `out` chunk by
/// chunk, each once its prefix digest equals `commitment`'s. Refused before it
/// emits on a stale commitment, before it writes a line on a different
/// emission, and stopped before the first chunk that differs.
pub fn live<W: Write>(
    header: Header,
    units: &[&dyn Unit],
    config: &Config,
    commitment: &Commitment,
    fingerprint: &str,
    out: W,
) -> Result<(W, Trailer), EngineError> {
    let diverged = |what: &str| EngineError::Diverged {
        what: what.to_owned(),
    };
    if commitment.format != FORMAT {
        return Err(diverged("the format"));
    }
    if commitment.validator != validator() {
        return Err(diverged("the validator"));
    }
    if commitment.fingerprint != fingerprint {
        return Err(diverged("the fingerprint"));
    }
    let p0 = hex(&Sha256::digest(encode(&Line::Header(Box::new(
        header.clone(),
    )))?));
    if p0 != commitment.header {
        return Err(diverged("the header"));
    }
    let expected = (commitment.records, commitment.multiset.clone());
    let check = |e: Emitted| {
        if (e.records, format!("{:032x}", e.multiset)) == expected {
            Ok(())
        } else {
            Err(diverged("what was emitted"))
        }
    };
    let first = Arc::new(Mutex::new(None));
    let verified = Verified {
        inner: out,
        lines: Lines::default(),
        chain: Chain::new(commitment.chunk),
        held: Vec::new(),
        commitment: Arc::new(commitment.clone()),
        started: false,
        diverged: first.clone(),
    };
    match generate_checked(header, units, config, verified, &check) {
        Ok((verified, trailer)) => Ok((verified.inner, trailer)),
        Err(e) => match first.lock().ok().and_then(|d| d.clone()) {
            Some(what) => Err(EngineError::Diverged { what }),
            None => Err(e),
        },
    }
}
