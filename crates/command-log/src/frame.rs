//! Streams of canonical JSON lines (ADR-0046 decision 6, ADR-0047 K1e-3): the
//! framing the command log and the scheduler's journal share. A line is its
//! value serialised by serde_json — fields in their declared order, no
//! whitespace — and `\n`; a reader re-encodes every line it reads and refuses
//! one whose bytes differ, so a stream has exactly one byte form whoever wrote
//! it; a running SHA-256 over the lines lets a trailer commit to every line
//! before it. What a stream's header, trailer and version are is its own.

use std::fmt;
use std::io::{self, BufRead};

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
    if canonical(&value)? != bytes {
        return Err(FrameError::NotCanonical { line: number });
    }
    Ok(value)
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
