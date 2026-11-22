//! What fills a hole at replay, and the splice (ADR-0046 decisions 3 and 4).
//!
//! A rendered value is a function of the run's namespace, what it is rendered
//! of and a role, drawn with faker's public key derivations:
//! `root(namespace) / "ascetic-ddd-command-log/rendered" / of / role`. A UUID is
//! that key's id under the namespace — the same id as text and as bytes; hex is
//! the first words of `key / hex`. Key material is not drawn from the
//! namespace (ADR-0047 decision 15): faker's derivations are public, so every
//! value drawn from the namespace is known to whoever knows it, and a run's
//! evidence names it. A `Secret32` is HMAC-SHA256 under the run's secret — 32
//! bytes the shell draws from its OS and holds in memory only — of what it is
//! rendered of and its role. A step's correlation id is drawn from the
//! namespace under a key of its own, `root(namespace) /
//! "ascetic-ddd-command-log/correlation" / step / attempt`, which no planned
//! role reaches. `vectors/render.py` computes them outside the crate (H2).

use std::fmt;

use ascetic_ddd_faker::{Encoder, Key, KeyPart, Namespace, Seed};
use sha2::{Digest, Sha256};

use crate::record::{Form, Of, Payload, Rendering, Span, StepId, Subject, TimeRendering};

/// A value to splice, before its form writes it. Its `Debug` shows only its
/// kind: it may be key material, or a value a step captured.
#[derive(Clone, PartialEq, Eq)]
pub enum Filled {
    /// A text: a JSON string token, or its UTF-8 bytes raw.
    Text(String),
    /// Bytes, raw only.
    Bytes(Vec<u8>),
    /// An integer: a JSON number token, or eight bytes little-endian raw.
    Integer(i64),
    /// One JSON value's text, as a response held it: as it is, either form.
    Json(String),
}

impl fmt::Debug for Filled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Filled::Text(s) => write!(f, "Text({} bytes)", s.len()),
            Filled::Bytes(b) => write!(f, "Bytes({} bytes)", b.len()),
            Filled::Integer(_) => f.write_str("Integer"),
            Filled::Json(s) => write!(f, "Json({} bytes)", s.len()),
        }
    }
}

/// The run's secret: 32 bytes the shell draws from its OS at the run's start
/// and holds in memory only, the key of every `Secret32` the run renders. It
/// is never serialised, and its `Debug` shows nothing of it.
#[derive(Clone)]
pub struct RunSecret([u8; 32]);

impl RunSecret {
    /// A secret of these bytes.
    pub fn new(bytes: [u8; 32]) -> RunSecret {
        RunSecret(bytes)
    }
}

impl fmt::Debug for RunSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RunSecret(..)")
    }
}

/// Why a payload could not be filled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FillError {
    /// The spans are out of order, overlap, or leave the bytes.
    Span,
    /// The value cannot be written in its span's form: bytes as a JSON token,
    /// or text that is not one JSON value.
    Form {
        /// The span's field.
        field: String,
    },
    /// A raw value is not its span's length.
    Length {
        /// The span's field.
        field: String,
        /// The span's length.
        expected: usize,
        /// The value's.
        got: usize,
    },
    /// A time outside the years 0000 to 9999, or past an `i64` of microseconds.
    Time,
}

impl fmt::Display for FillError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FillError::Span => f.write_str("spans out of order, overlapping or out of bounds"),
            FillError::Form { field } => {
                write!(f, "{field}: the value cannot be written in its form")
            }
            FillError::Length {
                field,
                expected,
                got,
            } => {
                write!(f, "{field}: {got} bytes for a span of {expected}")
            }
            FillError::Time => f.write_str("a time outside the years 0000 to 9999"),
        }
    }
}

impl std::error::Error for FillError {}

/// The canonical bytes of a subject: its kind, its scope's bytes, its index.
impl KeyPart for Subject {
    fn encode(&self, out: &mut Encoder<'_>) {
        out.str(&self.kind);
        out.bytes(&self.scope);
        out.u64(self.index);
    }
}

/// A step's: its subject, its label, its occurrence.
impl KeyPart for StepId {
    fn encode(&self, out: &mut Encoder<'_>) {
        self.subject.encode(out);
        out.str(&self.label);
        out.u32(self.n);
    }
}

/// A subject's or a step's, after the variant's number: the id of a subject
/// is never the id of one of its steps.
impl KeyPart for Of {
    fn encode(&self, out: &mut Encoder<'_>) {
        match self {
            Of::Subject(s) => {
                out.u8(0);
                s.encode(out);
            }
            Of::Step(s) => {
                out.u8(1);
                s.encode(out);
            }
        }
    }
}

fn key(namespace: Namespace, of: &Of, role: &str) -> Key {
    Key::root(Seed(namespace.0))
        .child("ascetic-ddd-command-log/rendered")
        .part(of)
        .child(role)
}

/// `len` bytes: the first words of `key / family`, eight at a time.
fn words(key: &Key, family: &str, len: usize) -> Vec<u8> {
    let base = key.child(family);
    (0u64..)
        .flat_map(|i| base.at(i).bits().to_le_bytes())
        .take(len)
        .collect()
}

/// `bytes`, after their length as eight bytes little-endian: a prefix-free field.
fn field(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    out.extend_from_slice(bytes);
}

/// What a `Secret32` is the HMAC of: the family, what it is rendered of, its
/// role, each field prefixed by its length.
fn secret_message(of: &Of, role: &str) -> Vec<u8> {
    let mut out = b"ascetic-ddd-command-log/secret32".to_vec();
    let subject = |out: &mut Vec<u8>, s: &Subject| {
        field(out, s.kind.as_bytes());
        field(out, &s.scope);
        out.extend_from_slice(&s.index.to_le_bytes());
    };
    match of {
        Of::Subject(s) => {
            out.push(0);
            subject(&mut out, s);
        }
        Of::Step(step) => {
            out.push(1);
            subject(&mut out, &step.subject);
            field(&mut out, step.label.as_bytes());
            out.extend_from_slice(&step.n.to_le_bytes());
        }
    }
    field(&mut out, role.as_bytes());
    out
}

/// HMAC-SHA256 (RFC 2104) under a 32-byte key.
fn hmac(key: &RunSecret, message: &[u8]) -> Vec<u8> {
    let pad = |b: u8| -> Vec<u8> {
        key.0
            .iter()
            .copied()
            .chain(std::iter::repeat_n(0, 32))
            .map(|k| k ^ b)
            .collect()
    };
    let inner = Sha256::new()
        .chain_update(pad(0x36))
        .chain_update(message)
        .finalize();
    Sha256::new()
        .chain_update(pad(0x5c))
        .chain_update(inner)
        .finalize()
        .to_vec()
}

/// The value of a rendered hole: public renderings from the namespace, key
/// material from the run's secret.
pub fn render(
    namespace: Namespace,
    secret: &RunSecret,
    of: &Of,
    role: &str,
    rendering: Rendering,
) -> Filled {
    if rendering == Rendering::Secret32 {
        return Filled::Bytes(hmac(secret, &secret_message(of, role)));
    }
    let key = key(namespace, of, role);
    match rendering {
        Rendering::UuidText => Filled::Text(key.uuid(&namespace).to_string()),
        Rendering::UuidBytes => Filled::Bytes(key.uuid(&namespace).as_bytes().to_vec()),
        Rendering::Hex { len } => Filled::Text(
            words(&key, "hex", usize::from(len))
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect(),
        ),
        Rendering::Secret32 => Filled::Bytes(hmac(secret, &secret_message(of, role))),
    }
}

/// Attempt `attempt`'s correlation id of `step`: 32 lower-case hex digits, the
/// first words of `root(namespace) / "ascetic-ddd-command-log/correlation" /
/// step / attempt / id` (ADR-0047 decision 15). A server's record of the
/// attempt joins to it; nothing planned renders under its key.
pub fn correlation(namespace: Namespace, step: &StepId, attempt: u32) -> String {
    let key = Key::root(Seed(namespace.0))
        .child("ascetic-ddd-command-log/correlation")
        .part(step)
        .at(u64::from(attempt));
    words(&key, "id", 16)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The value of a time hole: `base` plus `offset`, microseconds since the
/// Unix epoch, as RFC 3339 in UTC with six decimals or as epoch milliseconds
/// (rounded down).
pub fn render_time(base: i64, offset: i64, rendering: TimeRendering) -> Result<Filled, FillError> {
    let t = base.checked_add(offset).ok_or(FillError::Time)?;
    Ok(match rendering {
        TimeRendering::EpochMillis => Filled::Integer(t.div_euclid(1000)),
        TimeRendering::Rfc3339Micros => Filled::Text(rfc3339(t)?),
    })
}

/// `YYYY-MM-DDTHH:MM:SS.ffffffZ` from microseconds since the epoch: the
/// proleptic Gregorian calendar by Hinnant's `civil_from_days`, integers only.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "after the checked era step every operand is below 10⁶: a day of an era, a year of it, a second of a day"
)]
pub(crate) fn rfc3339(micros: i64) -> Result<String, FillError> {
    let (seconds, fraction) = (micros.div_euclid(1_000_000), micros.rem_euclid(1_000_000));
    let (days, second) = (seconds.div_euclid(86_400), seconds.rem_euclid(86_400));
    // Days from 0000-03-01, in eras of 400 years (146,097 days).
    let z = days.checked_add(719_468).ok_or(FillError::Time)?;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = era
        .checked_mul(400)
        .and_then(|y| y.checked_add(yoe))
        .and_then(|y| y.checked_add(i64::from(month <= 2)))
        .ok_or(FillError::Time)?;
    if !(0..=9999).contains(&year) {
        return Err(FillError::Time);
    }
    Ok(format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{fraction:06}Z",
        second / 3600,
        second / 60 % 60,
        second % 60
    ))
}

/// The bytes `filled` is written as in `span`'s form.
fn written(span: &Span, filled: Filled) -> Result<Vec<u8>, FillError> {
    let form = || FillError::Form {
        field: span.field.clone(),
    };
    let bytes = match (span.form, filled) {
        (Form::JsonToken, Filled::Text(s)) => serde_json::to_vec(&s).map_err(|_| form())?,
        (Form::JsonToken, Filled::Integer(i)) => i.to_string().into_bytes(),
        (Form::JsonToken, Filled::Json(text)) => {
            serde_json::from_str::<serde::de::IgnoredAny>(&text).map_err(|_| form())?;
            text.into_bytes()
        }
        (Form::JsonToken, Filled::Bytes(_)) => return Err(form()),
        (Form::Raw, Filled::Text(s) | Filled::Json(s)) => s.into_bytes(),
        (Form::Raw, Filled::Bytes(b)) => b,
        (Form::Raw, Filled::Integer(i)) => i.to_le_bytes().to_vec(),
    };
    let expected = span.len as usize;
    if span.form == Form::Raw && bytes.len() != expected {
        return Err(FillError::Length {
            field: span.field.clone(),
            expected,
            got: bytes.len(),
        });
    }
    Ok(bytes)
}

/// The payload's bytes with every span replaced by its value: the bytes
/// between spans as they are.
pub fn fill(
    payload: &Payload,
    mut value: impl FnMut(&Span) -> Result<Filled, FillError>,
) -> Result<Vec<u8>, FillError> {
    let mut out = Vec::with_capacity(payload.bytes.len());
    let mut at = 0usize;
    for span in &payload.holes {
        let start = span.offset as usize;
        let end = start
            .checked_add(span.len as usize)
            .ok_or(FillError::Span)?;
        out.extend_from_slice(payload.bytes.get(at..start).ok_or(FillError::Span)?);
        if end > payload.bytes.len() {
            return Err(FillError::Span);
        }
        out.extend(written(span, value(span)?)?);
        at = end;
    }
    out.extend_from_slice(payload.bytes.get(at..).ok_or(FillError::Span)?);
    Ok(out)
}
