//! Streams of canonical JSON lines (ADR-0046 decision 6, ADR-0047 K1e-3): the
//! framing the command log and the scheduler's journal share. A line is its
//! value serialised by serde_json — fields in their declared order, no
//! whitespace — and `\n`; a reader re-encodes every line it reads and refuses
//! one whose bytes differ, so a stream has exactly one byte form whoever wrote
//! it; a running SHA-256 over the lines lets a trailer commit to every line
//! before it. What a stream's header, trailer and version are is its own.

use std::fmt;
use std::io::{self, BufRead, Write};

use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::{Digest as _, Sha256};

/// Why a line of a stream was refused.
#[derive(Debug)]
pub enum FrameError {
    /// Reading failed.
    Io(io::Error),
    /// A value could not be serialised.
    Encode(String),
    /// A line is not a value of the stream's vocabulary.
    Malformed {
        /// Its number, from 1.
        line: u64,
        /// Why.
        reason: String,
    },
    /// A line is a value, but not in its canonical bytes.
    NotCanonical {
        /// Its number, from 1.
        line: u64,
    },
    /// A line cut short: the writer died mid-line.
    Truncated,
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::Io(e) => write!(f, "{e}"),
            FrameError::Encode(e) => write!(f, "cannot encode a line: {e}"),
            FrameError::Malformed { line, reason } => write!(f, "line {line}: {reason}"),
            FrameError::NotCanonical { line } => {
                write!(f, "line {line}: not in its canonical bytes")
            }
            FrameError::Truncated => f.write_str("a line cut short"),
        }
    }
}

impl std::error::Error for FrameError {}

impl From<io::Error> for FrameError {
    fn from(e: io::Error) -> FrameError {
        FrameError::Io(e)
    }
}

/// The canonical bytes of a value as a line, `\n` included.
pub fn canonical<T: Serialize>(value: &T) -> Result<Vec<u8>, FrameError> {
    let mut bytes = serde_json::to_vec(value).map_err(|e| FrameError::Encode(e.to_string()))?;
    bytes.push(b'\n');
    Ok(bytes)
}

/// The value line `number` holds, refused unless `bytes` are its canonical
/// bytes, `\n` included.
pub fn decode<T: Serialize + DeserializeOwned>(bytes: &[u8], number: u64) -> Result<T, FrameError> {
    let Some(body) = bytes.strip_suffix(b"\n") else {
        return Err(FrameError::Truncated);
    };
    let value: T = serde_json::from_slice(body).map_err(|e| FrameError::Malformed {
        line: number,
        reason: e.to_string(),
    })?;
    if !is_canonical(&value, body)? {
        return Err(FrameError::NotCanonical { line: number });
    }
    Ok(value)
}

/// What line `number` holds of `T`, every other field ignored and its bytes
/// not checked canonical: a part read ahead of the line's own type — a
/// journal header's version before the schema that version names (ADR-0047
/// decision 22, K2b-7e-2b-3d-3b).
pub fn peek<T: DeserializeOwned>(bytes: &[u8], number: u64) -> Result<T, FrameError> {
    let Some(body) = bytes.strip_suffix(b"\n") else {
        return Err(FrameError::Truncated);
    };
    serde_json::from_slice(body).map_err(|e| FrameError::Malformed {
        line: number,
        reason: e.to_string(),
    })
}

/// Whether `value` serialises to exactly `body`: compared as it is written,
/// so a line is checked with no copy of it (ADR-0047 decision 22 — a reader
/// holds no buffer of a line's size but its own).
fn is_canonical<T: Serialize>(value: &T, body: &[u8]) -> Result<bool, FrameError> {
    let mut same = Same {
        rest: body,
        same: true,
    };
    serde_json::to_writer(&mut same, value).map_err(|e| FrameError::Encode(e.to_string()))?;
    Ok(same.same && same.rest.is_empty())
}

/// A writer comparing what it is given with the bytes left.
struct Same<'a> {
    rest: &'a [u8],
    same: bool,
}

impl io::Write for Same<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.same {
            match self.rest.strip_prefix(buf) {
                Some(rest) => self.rest = rest,
                None => self.same = false,
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A stream's lines written to `W` in their canonical bytes with no copy of
/// a line (ADR-0047 decision 22, K2b-7e-2b-2b): serialised through a buffer
/// of a fixed capacity, never grown, the digest taken as its bytes leave;
/// each line whole in `W`, and `W` flushed, once [`line`](Out::line)
/// returns.
pub struct Out<W: Write> {
    out: W,
    buffer: Vec<u8>,
    digest: Digest,
}

impl<W: Write> Out<W> {
    /// Lines to `out` through a buffer of `capacity` bytes, at least one.
    pub fn new(out: W, capacity: usize) -> Out<W> {
        Out {
            out,
            buffer: Vec::with_capacity(capacity.max(1)),
            digest: Digest::new(),
        }
    }

    /// `value` as a line. An error writing is [`FrameError::Io`], one
    /// encoding [`FrameError::Encode`]; either leaves the line cut.
    pub fn line<T: Serialize>(&mut self, value: &T) -> Result<(), FrameError> {
        serde_json::to_writer(Chunks(self), value).map_err(|e| {
            if e.is_io() {
                FrameError::Io(e.into())
            } else {
                FrameError::Encode(e.to_string())
            }
        })?;
        self.push(b"\n")?;
        self.drain()?;
        self.out.flush()?;
        Ok(())
    }

    /// The digest of the lines written.
    pub fn digest(&self) -> &Digest {
        &self.digest
    }

    /// The output.
    pub fn into_inner(self) -> W {
        self.out
    }

    /// `bytes` into the buffer, which is written out each time it is full.
    fn push(&mut self, mut bytes: &[u8]) -> io::Result<()> {
        while !bytes.is_empty() {
            let room = self.buffer.capacity().saturating_sub(self.buffer.len());
            if room == 0 {
                self.drain()?;
                continue;
            }
            let (now, later) = bytes.split_at(room.min(bytes.len()));
            self.buffer.extend_from_slice(now);
            bytes = later;
        }
        Ok(())
    }

    /// What the buffer holds, digested and written out.
    fn drain(&mut self) -> io::Result<()> {
        self.digest.update(&self.buffer);
        self.out.write_all(&self.buffer)?;
        self.buffer.clear();
        Ok(())
    }
}

/// What serde writes, pushed into an [`Out`]'s buffer.
struct Chunks<'a, W: Write>(&'a mut Out<W>);

impl<W: Write> io::Write for Chunks<'_, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.push(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// The next line of `input`, `\n` included; `None` at its end.
pub fn next_line(input: &mut impl BufRead) -> Result<Option<Vec<u8>>, FrameError> {
    let mut bytes = Vec::new();
    if input.read_until(b'\n', &mut bytes)? == 0 {
        return Ok(None);
    }
    if bytes.last() != Some(&b'\n') {
        return Err(FrameError::Truncated);
    }
    Ok(Some(bytes))
}

/// A running SHA-256 over a stream's lines.
#[derive(Clone, Debug, Default)]
pub struct Digest(Sha256);

impl Digest {
    /// A digest of nothing.
    pub fn new() -> Digest {
        Digest(Sha256::new())
    }

    /// A line taken in.
    pub fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    /// The digest of the lines so far, in lowercase hex.
    pub fn hex(&self) -> String {
        self.0
            .clone()
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }
}
