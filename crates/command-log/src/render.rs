//! What fills a hole at replay, and the splice (ADR-0046 decisions 3 and 4).
//!
//! A rendered value is a function of the run's namespace, what it is rendered
//! of and a role, drawn with faker's public key derivations:
//! `root(namespace) / "ascetic-ddd-command-log/rendered" / of / role`. A UUID is
//! that key's id under the namespace — the same id as text and as bytes; hex and
//! key material are the first words of `key / family`, `family` being `hex` or
//! `secret32`, so a public value never shows a secret's bytes rendered under the
//! same role. `vectors/render.py` computes them outside the crate (H2).

use std::fmt;

use ascetic_ddd_faker::{Encoder, Key, KeyPart, Namespace, Seed};

use crate::record::{Form, Of, Payload, Rendering, Span, StepId, Subject, TimeRendering};

/// A value to splice, before its form writes it.
#[derive(Clone, Debug, PartialEq, Eq)]
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

/// The value of a rendered hole.
pub fn render(namespace: Namespace, of: &Of, role: &str, rendering: Rendering) -> Filled {
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
        Rendering::Secret32 => Filled::Bytes(words(&key, "secret32", 32)),
    }
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
