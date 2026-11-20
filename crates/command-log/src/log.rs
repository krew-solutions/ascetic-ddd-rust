//! Writing and reading a log: canonical lines, a streaming SHA-256, a trailer
//! (ADR-0046 decision 6).
//!
//! A line is its record serialised by serde_json — fields in their declared
//! order, no whitespace — and `\n`. A reader re-encodes every line it reads and
//! refuses one whose bytes differ, so a log has exactly one byte form whoever
//! wrote it. The trailer carries the number of steps and the digest of every
//! line before it; a log without one is refused, never replayed as a shorter
//! run.

use std::fmt;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::record::{Header, Line, Record, Trailer};

/// The format a reader accepts, and a writer writes: exactly this one.
pub const FORMAT_VERSION: u32 = 1;

/// The byte form's identity: the SHA-256 of the golden log
/// (`golden/c1.jsonl`), which holds every record and hole variant. A change to
/// how any line is written changes the golden log, and so this, in the same
/// edit (ADR-0046 decision 6, F2).
pub const FORMAT: &str = "8ffd12b73eeb944e71cbdd16e332918bfab536ce5198175242505c143f387e4b";

/// The canonical bytes of a line, `\n` included.
pub fn encode(line: &Line) -> Result<Vec<u8>, LogError> {
    let mut bytes = serde_json::to_vec(line).map_err(|e| LogError::Encode(e.to_string()))?;
    bytes.push(b'\n');
    Ok(bytes)
}

/// Why a log was refused.
#[derive(Debug)]
pub enum LogError {
    /// Reading or writing failed.
    Io(io::Error),
    /// A line could not be serialised.
    Encode(String),
    /// A line is not a record of this format.
    Malformed {
        /// Its number, from 1.
        line: u64,
        /// Why.
        reason: String,
    },
    /// A line is a record, but not in its canonical bytes.
    NotCanonical {
        /// Its number, from 1.
        line: u64,
    },
    /// The header is not the first line, or another follows it.
    Header {
        /// Where.
        line: u64,
    },
    /// The header names another format.
    Format(u32),
    /// The log ends without a trailer.
    Truncated,
    /// The trailer's count or digest differs from what was read.
    Trailer {
        /// What the trailer says.
        expected: Trailer,
        /// What was read.
        read: Trailer,
    },
    /// A line follows the trailer.
    AfterTrailer,
}

impl fmt::Display for LogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogError::Io(e) => write!(f, "io: {e}"),
            LogError::Encode(e) => write!(f, "a line could not be serialised: {e}"),
            LogError::Malformed { line, reason } => write!(f, "line {line}: {reason}"),
            LogError::NotCanonical { line } => {
                write!(f, "line {line} is not in its canonical bytes")
            }
            LogError::Header { line } => write!(
                f,
                "line {line}: the header must be the first line, and only it"
            ),
            LogError::Format(v) => write!(f, "format {v}, not {FORMAT_VERSION}"),
            LogError::Truncated => f.write_str("the log ends without its trailer"),
            LogError::Trailer { expected, read } => write!(
                f,
                "the trailer says {} steps, {}; read {} steps, {}",
                expected.records, expected.sha256, read.records, read.sha256
            ),
            LogError::AfterTrailer => f.write_str("a line follows the trailer"),
        }
    }
}

impl std::error::Error for LogError {}

impl From<io::Error> for LogError {
    fn from(e: io::Error) -> LogError {
        LogError::Io(e)
    }
}

/// Writes a log to `out`: the header now, steps one by one, the trailer at
/// [`finish`](Writer::finish).
pub struct Writer<W: Write> {
    out: W,
    digest: Sha256,
    records: u64,
}

impl<W: Write> Writer<W> {
    /// Starts a log; refused if the header names another format.
    pub fn new(mut out: W, header: Header) -> Result<Writer<W>, LogError> {
        if header.format != FORMAT_VERSION {
            return Err(LogError::Format(header.format));
        }
        let mut digest = Sha256::new();
        let bytes = encode(&Line::Header(Box::new(header)))?;
        out.write_all(&bytes)?;
        digest.update(&bytes);
        Ok(Writer {
            out,
            digest,
            records: 0,
        })
    }

    /// Appends a step.
    pub fn step(&mut self, record: Record) -> Result<(), LogError> {
        let bytes = encode(&Line::Step(Box::new(record)))?;
        self.out.write_all(&bytes)?;
        self.digest.update(&bytes);
        self.records = self.records.saturating_add(1);
        Ok(())
    }

    /// Writes the trailer and hands back the output.
    pub fn finish(mut self) -> Result<(W, Trailer), LogError> {
        let trailer = Trailer {
            records: self.records,
            sha256: hex(&self.digest.finalize()),
        };
        self.out
            .write_all(&encode(&Line::Trailer(trailer.clone()))?)?;
        self.out.flush()?;
        Ok((self.out, trailer))
    }
}

/// Writes a log to `path` whole or not at all: into `path.partial`, synced,
/// renamed only once the trailer is written.
pub fn write_file(
    path: &Path,
    header: Header,
    records: impl IntoIterator<Item = Record>,
) -> Result<Trailer, LogError> {
    let partial = PathBuf::from(format!("{}.partial", path.display()));
    let result = (|| {
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&partial)?;
        let mut writer = Writer::new(io::BufWriter::new(file), header)?;
        for record in records {
            writer.step(record)?;
        }
        let (out, trailer) = writer.finish()?;
        let file = out.into_inner().map_err(|e| LogError::Io(e.into_error()))?;
        file.sync_all()?;
        Ok(trailer)
    })();
    match result {
        Ok(trailer) => {
            fs::rename(&partial, path)?;
            Ok(trailer)
        }
        Err(e) => {
            // Best effort: the failure to report is the first one.
            let _ = fs::remove_file(&partial);
            Err(e)
        }
    }
}

/// Reads a log: its header at once, its steps as an iterator, its trailer
/// checked at the end.
pub struct Reader<R: BufRead> {
    input: R,
    header: Header,
    digest: Sha256,
    records: u64,
    line: u64,
    ended: bool,
}

impl<R: BufRead> Reader<R> {
    /// Opens a log; refused unless its first line is a header of this format.
    pub fn new(mut input: R) -> Result<Reader<R>, LogError> {
        let mut digest = Sha256::new();
        let header = match read_line(&mut input, 1, &mut digest)? {
            Some(Line::Header(header)) => *header,
            _ => return Err(LogError::Header { line: 1 }),
        };
        if header.format != FORMAT_VERSION {
            return Err(LogError::Format(header.format));
        }
        Ok(Reader {
            input,
            header,
            digest,
            records: 0,
            line: 1,
            ended: false,
        })
    }

    /// The log's header.
    pub fn header(&self) -> &Header {
        &self.header
    }

    fn next_record(&mut self) -> Result<Option<Record>, LogError> {
        if self.ended {
            return Ok(None);
        }
        self.line = self.line.saturating_add(1);
        // The trailer is not part of its own digest: hash into a copy, kept only
        // when the line is a step.
        let mut digest = self.digest.clone();
        match read_line(&mut self.input, self.line, &mut digest)? {
            None => Err(LogError::Truncated),
            Some(Line::Header(_)) => Err(LogError::Header { line: self.line }),
            Some(Line::Step(record)) => {
                self.digest = digest;
                self.records = self.records.saturating_add(1);
                Ok(Some(*record))
            }
            Some(Line::Trailer(expected)) => {
                self.ended = true;
                let read = Trailer {
                    records: self.records,
                    sha256: hex(&self.digest.clone().finalize()),
                };
                if expected != read {
                    return Err(LogError::Trailer { expected, read });
                }
                let mut rest = Vec::new();
                self.input.read_until(b'\n', &mut rest)?;
                if rest.is_empty() {
                    Ok(None)
                } else {
                    Err(LogError::AfterTrailer)
                }
            }
        }
    }
}

impl<R: BufRead> Iterator for Reader<R> {
    type Item = Result<Record, LogError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_record() {
            Ok(Some(record)) => Some(Ok(record)),
            Ok(None) => None,
            Err(e) => {
                self.ended = true;
                Some(Err(e))
            }
        }
    }
}

/// Reads one line, checks it canonical, hashes it; `None` at the end of input.
fn read_line(
    input: &mut impl BufRead,
    number: u64,
    digest: &mut Sha256,
) -> Result<Option<Line>, LogError> {
    let mut bytes = Vec::new();
    if input.read_until(b'\n', &mut bytes)? == 0 {
        return Ok(None);
    }
    if bytes.last() != Some(&b'\n') {
        // A line cut short: the writer died mid-line.
        return Err(LogError::Truncated);
    }
    let body = bytes
        .get(..bytes.len().saturating_sub(1))
        .unwrap_or_default();
    let line: Line = serde_json::from_slice(body).map_err(|e| LogError::Malformed {
        line: number,
        reason: e.to_string(),
    })?;
    if encode(&line)? != bytes {
        return Err(LogError::NotCanonical { line: number });
    }
    digest.update(&bytes);
    Ok(Some(line))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
