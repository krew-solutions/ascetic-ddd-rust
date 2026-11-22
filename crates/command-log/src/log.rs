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

use crate::frame::{self, Digest, FrameError};
use crate::record::{Header, Line, Record, Trailer};

/// The format a reader accepts, and a writer writes: exactly this one.
pub const FORMAT_VERSION: u32 = 4;

/// The byte form's identity: the SHA-256 of the golden log
/// (`golden/c1.jsonl`), which holds every record and hole variant. A change to
/// how any line is written changes the golden log, and so this, in the same
/// edit (ADR-0046 decision 6, F2).
pub const FORMAT: &str = "f4611ec575e2c880b5c6ba8bfa33c6d1a4d124e6c0234619defce732188c0fa0";

/// The canonical bytes of a line, `\n` included.
pub fn encode(line: &Line) -> Result<Vec<u8>, LogError> {
    Ok(frame::canonical(line)?)
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
    /// A pass's runs would not fit the disk, beside a reserve: refused before
    /// the write.
    Space {
        /// The directory.
        path: std::path::PathBuf,
        /// What the write needs, the reserve included.
        needed: u64,
        /// What is free.
        free: u64,
    },
    /// A hot part of a pass holds more than its limit (decision 16): the plan
    /// asks more than this machine holds, or the log is broken where it is.
    Hot {
        /// The part.
        part: &'static str,
        /// What it holds, or would.
        held: u64,
        /// Where: the line, or 0 before the first.
        line: u64,
    },
    /// A log read through a commitment is not what was validated: what
    /// differs ([`crate::live`]).
    Diverged {
        /// The format, the validator, the header, or a chunk.
        what: String,
    },
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
            LogError::Space { path, needed, free } => {
                write!(f, "{}: {needed} bytes needed, {free} free", path.display())
            }
            LogError::Hot { part, held, line } => {
                write!(f, "line {line}: {held} {part} held, past the limit")
            }
            LogError::Diverged { what } => write!(f, "{what} differs from the commitment"),
        }
    }
}

impl std::error::Error for LogError {}

impl From<FrameError> for LogError {
    fn from(e: FrameError) -> LogError {
        match e {
            // Through `From<io::Error>`: a replay's divergence rides an I/O error.
            FrameError::Io(e) => LogError::from(e),
            FrameError::Encode(e) => LogError::Encode(e),
            FrameError::Malformed { line, reason } => LogError::Malformed { line, reason },
            FrameError::NotCanonical { line } => LogError::NotCanonical { line },
            FrameError::Truncated => LogError::Truncated,
        }
    }
}

impl From<io::Error> for LogError {
    fn from(e: io::Error) -> LogError {
        match e
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<crate::live::Diverged>())
        {
            Some(d) => LogError::Diverged {
                what: d.what.clone(),
            },
            None => LogError::Io(e),
        }
    }
}

/// Writes a log to `out`: the header now, steps one by one, the trailer at
/// [`finish`](Writer::finish).
pub struct Writer<W: Write> {
    out: W,
    digest: Digest,
    records: u64,
}

impl<W: Write> Writer<W> {
    /// Starts a log; refused if the header names another format.
    pub fn new(mut out: W, header: Header) -> Result<Writer<W>, LogError> {
        if header.format != FORMAT_VERSION {
            return Err(LogError::Format(header.format));
        }
        let mut digest = Digest::new();
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
        self.line(&bytes)
    }

    /// Appends a step already encoded by [`encode`]: the engine encodes a
    /// record once, before it is bucketed.
    pub(crate) fn line(&mut self, bytes: &[u8]) -> Result<(), LogError> {
        self.out.write_all(bytes)?;
        self.digest.update(bytes);
        self.records = self.records.saturating_add(1);
        Ok(())
    }

    /// Writes the trailer and hands back the output.
    pub fn finish(mut self) -> Result<(W, Trailer), LogError> {
        let trailer = Trailer {
            records: self.records,
            sha256: self.digest.hex(),
        };
        self.out
            .write_all(&encode(&Line::Trailer(trailer.clone()))?)?;
        self.out.flush()?;
        Ok((self.out, trailer))
    }
}

/// How a log is written to a file (ADR-0046 decision 15). Neither changes a
/// byte of its lines, its trailer or its digest: only the file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compression {
    /// The lines as they are.
    None,
    /// One zstd stream of the lines, at a level.
    Zstd {
        /// The level: 1, the default, is both smaller and faster than 3 on a
        /// log (M8).
        level: i32,
    },
}

impl Default for Compression {
    fn default() -> Compression {
        Compression::Zstd { level: 1 }
    }
}

/// zstd's frame magic, little-endian: a compressed log's first four bytes.
pub const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];

/// A file being written whole or not at all: `path.partial`, synced and renamed
/// by [`Partial::commit`], removed if dropped first.
pub(crate) struct Partial {
    partial: PathBuf,
    path: PathBuf,
    done: bool,
}

impl Partial {
    pub(crate) fn create(path: &Path) -> io::Result<(Partial, fs::File)> {
        let partial = PathBuf::from(format!("{}.partial", path.display()));
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&partial)?;
        Ok((
            Partial {
                partial,
                path: path.to_owned(),
                done: false,
            },
            file,
        ))
    }

    /// Syncs `file` and puts it at its path.
    pub(crate) fn commit(mut self, file: fs::File) -> io::Result<()> {
        file.sync_all()?;
        drop(file);
        fs::rename(&self.partial, &self.path)?;
        self.done = true;
        Ok(())
    }
}

impl Drop for Partial {
    fn drop(&mut self) {
        if !self.done {
            let _ = fs::remove_file(&self.partial);
        }
    }
}

/// The output a log file is written through: plain or a zstd stream.
pub(crate) enum FileOut {
    Plain(io::BufWriter<fs::File>),
    Zstd(io::BufWriter<zstd::Encoder<'static, fs::File>>),
}

impl FileOut {
    pub(crate) fn new(file: fs::File, compression: Compression) -> io::Result<FileOut> {
        Ok(match compression {
            Compression::None => FileOut::Plain(io::BufWriter::new(file)),
            Compression::Zstd { level } => {
                FileOut::Zstd(io::BufWriter::new(zstd::Encoder::new(file, level)?))
            }
        })
    }

    /// The file, every byte written: a zstd frame finished, its error kept.
    pub(crate) fn finish(self) -> io::Result<fs::File> {
        match self {
            FileOut::Plain(out) => out.into_inner().map_err(io::IntoInnerError::into_error),
            FileOut::Zstd(out) => out
                .into_inner()
                .map_err(io::IntoInnerError::into_error)?
                .finish(),
        }
    }
}

impl Write for FileOut {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            FileOut::Plain(out) => out.write(buf),
            FileOut::Zstd(out) => out.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            FileOut::Plain(out) => out.flush(),
            FileOut::Zstd(out) => out.flush(),
        }
    }
}

/// Writes a log to `path` whole or not at all: into `path.partial`, the zstd
/// frame finished, synced, renamed only once the trailer is written.
pub fn write_file(
    path: &Path,
    header: Header,
    records: impl IntoIterator<Item = Record>,
    compression: Compression,
) -> Result<Trailer, LogError> {
    let (partial, file) = Partial::create(path)?;
    let mut writer = Writer::new(FileOut::new(file, compression)?, header)?;
    for record in records {
        writer.step(record)?;
    }
    let (out, trailer) = writer.finish()?;
    partial.commit(out.finish()?)?;
    Ok(trailer)
}

/// A log's input, plain or compressed, told by its first bytes: a zstd
/// frame's magic, or a line's `{`. Anything else is refused.
pub fn open<R: BufRead + Send + 'static>(
    input: R,
) -> Result<Reader<Box<dyn BufRead + Send>>, LogError> {
    Reader::new(decoded(input)?)
}

/// A log's lines from its input, plain or compressed, before any is read.
pub fn decoded<R: BufRead + Send + 'static>(
    mut input: R,
) -> Result<Box<dyn BufRead + Send>, LogError> {
    let first = input.fill_buf()?;
    let boxed: Box<dyn BufRead + Send> = if first.starts_with(&ZSTD_MAGIC) {
        Box::new(io::BufReader::new(zstd::Decoder::with_buffer(input)?))
    } else if first.first() == Some(&b'{') {
        Box::new(input)
    } else if first.is_empty() {
        return Err(LogError::Truncated);
    } else {
        return Err(LogError::Malformed {
            line: 1,
            reason: "neither a log nor a zstd stream of one".to_owned(),
        });
    };
    Ok(boxed)
}

/// Opens the log in `path`, plain or compressed.
pub fn open_file(path: &Path) -> Result<Reader<Box<dyn BufRead + Send>>, LogError> {
    open(io::BufReader::new(fs::File::open(path)?))
}

/// Reads a log: its header at once, its steps as an iterator, its trailer
/// checked at the end.
pub struct Reader<R: BufRead> {
    input: R,
    header: Header,
    digest: Digest,
    records: u64,
    line: u64,
    ended: bool,
}

impl<R: BufRead> Reader<R> {
    /// Opens a log; refused unless its first line is a header of this format.
    pub fn new(mut input: R) -> Result<Reader<R>, LogError> {
        let mut digest = Digest::new();
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
                    sha256: self.digest.hex(),
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
    digest: &mut Digest,
) -> Result<Option<Line>, LogError> {
    let Some(bytes) = frame::next_line(input)? else {
        return Ok(None);
    };
    let line: Line = frame::decode(&bytes, number)?;
    digest.update(&bytes);
    Ok(Some(line))
}
