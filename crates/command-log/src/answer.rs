//! What an answer says against a record (ADR-0047 decision 15): its problem's
//! code, whether predicates hold, the values a step captures from it, the wait
//! it asks for. The judgement over these is the scheduler's `judge`.
//!
//! - The body is read only up to the cap the shell read it to; past it, the
//!   answer has no readable body: no code, no predicate holds, nothing taken.
//! - The code is the string at the profile's pointer, read only from a status
//!   of 400 or more whose one `Content-Type` is `application/problem+json`
//!   (compared without case, its parameters ignored) and whose body is an
//!   object.
//! - A predicate's value is JSON text, or a hole filled as the frame filled it.
//! - A value is taken only if every use can carry it — a number, or a string of
//!   visible ASCII with inner blanks, neither empty, `.` nor `..`, at most
//!   [`TAKEN`] bytes — and is then one JSON value's text: `Header(n)` from
//!   exactly one field line, `Json(p)` a string or a number there, `Location`
//!   its last non-empty path segment, its query and fragment dropped,
//!   percent-decoded strictly. Otherwise it is absent.
//! - `Retry-After` in delta-seconds, from exactly one field line; a date is
//!   none, as the judge has no clock.

#![deny(
    clippy::indexing_slicing,
    clippy::string_slice,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

use std::fmt;

use crate::fills::{Fills, HoleError};
use crate::json::{self, Json};
use crate::record::{Capture, Predicate, Source, Test, Value};
use crate::render::Filled;

/// The most bytes a taken value has.
pub const TAKEN: usize = 8 * 1024;

/// An answer, as the shell read it. Its `Debug` shows the status and sizes.
pub struct Reply<'a> {
    /// The status.
    pub status: u16,
    /// Every header field line, its name as sent.
    pub headers: &'a [(String, Vec<u8>)],
    /// The body, whole and identity-encoded, if it was within the cap; `None`
    /// past it.
    pub body: Option<&'a [u8]>,
}

impl fmt::Debug for Reply<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reply")
            .field("status", &self.status)
            .field("headers", &self.headers.len())
            .field("body", &self.body.map(<[u8]>::len))
            .finish()
    }
}

/// An answer read: its body parsed once.
pub struct Read<'a> {
    reply: &'a Reply<'a>,
    body: Option<Json>,
}

impl fmt::Debug for Read<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Read")
            .field("reply", self.reply)
            .field("body", &self.body)
            .finish()
    }
}

/// A predicate the plan wrote wrong: the validator refuses it before a run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PredicateError {
    /// Not an RFC 6901 pointer.
    Pointer,
    /// A literal that is not JSON text.
    Literal,
    /// A hole that cannot be filled.
    Hole(HoleError),
}

impl fmt::Display for PredicateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PredicateError::Pointer => f.write_str("a predicate's pointer is not RFC 6901's"),
            PredicateError::Literal => f.write_str("a predicate's literal is not JSON"),
            PredicateError::Hole(e) => write!(f, "a predicate's value: {e}"),
        }
    }
}

impl std::error::Error for PredicateError {}

/// The answer read: its body parsed, if it is JSON.
pub fn read<'a>(reply: &'a Reply<'a>) -> Read<'a> {
    let body = reply.body.and_then(|b| json::parse(b).ok());
    Read { reply, body }
}

/// The field lines named `name`, compared without case.
fn lines<'a>(reply: &'a Reply<'_>, name: &'a str) -> impl Iterator<Item = &'a [u8]> + 'a {
    reply
        .headers
        .iter()
        .filter(move |(n, _)| n.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_slice())
}

/// The one field line named `name`; none if there are none or several.
fn one<'a>(reply: &'a Reply<'_>, name: &'a str) -> Option<&'a [u8]> {
    let mut it = lines(reply, name);
    let first = it.next()?;
    it.next().is_none().then_some(first)
}

/// A string every use can carry: a path segment, a header, a query, a JSON
/// token.
fn carried(s: &str) -> bool {
    let b = s.as_bytes();
    let visible = |c: &u8| (0x21..=0x7e).contains(c);
    !b.is_empty()
        && b.len() <= TAKEN
        && s != "."
        && s != ".."
        && b.iter().all(|c| visible(c) || *c == b' ' || *c == b'\t')
        && b.first().is_some_and(visible)
        && b.last().is_some_and(visible)
}

/// A carried string as one JSON value's text.
fn quoted(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len().saturating_add(2));
    out.push(b'"');
    for b in s.bytes() {
        match b {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\t' => out.extend_from_slice(b"\\t"),
            _ => out.push(b),
        }
    }
    out.push(b'"');
    out
}

fn hex(b: u8) -> Option<u8> {
    char::from(b)
        .to_digit(16)
        .and_then(|d| u8::try_from(d).ok())
}

/// `%XX` decoded; a `%` not followed by two hex digits, or bytes not UTF-8,
/// is none.
fn decoded(s: &str) -> Option<String> {
    let mut out = Vec::with_capacity(s.len());
    let mut it = s.bytes();
    while let Some(b) = it.next() {
        if b == b'%' {
            let hi = hex(it.next()?)?;
            let lo = hex(it.next()?)?;
            out.push(hi.checked_mul(16)?.checked_add(lo)?);
        } else {
            out.push(b);
        }
    }
    String::from_utf8(out).ok()
}

/// The last non-empty path segment of a `Location`, its query and fragment
/// dropped, decoded.
fn segment(location: &str) -> Option<String> {
    let s = location.split('#').next()?.split('?').next()?;
    let path = match s.find("://") {
        Some(i) => {
            let after = s.get(i.saturating_add(3)..)?;
            after.find('/').and_then(|j| after.get(j..)).unwrap_or("")
        }
        None => s,
    };
    decoded(path.split('/').rfind(|p| !p.is_empty())?)
}

impl Read<'_> {
    /// The status.
    pub fn status(&self) -> u16 {
        self.reply.status
    }

    /// The problem's code, at `pointer`.
    pub fn code(&self, pointer: &str) -> Option<String> {
        if self.reply.status < 400 {
            return None;
        }
        let media = one(self.reply, "content-type")?;
        let media = std::str::from_utf8(media).ok()?;
        let essence = media.split(';').next()?.trim();
        if !essence.eq_ignore_ascii_case("application/problem+json") {
            return None;
        }
        let body @ Json::Object(_) = self.body.as_ref()? else {
            return None;
        };
        match body.at(pointer).ok()?? {
            Json::String(s) => Some(s.clone()),
            _ => None,
        }
    }

    /// Whether every predicate holds of the body; none holds of a body that
    /// is not JSON, or past the cap.
    pub fn holds(
        &self,
        predicates: &[Predicate],
        fills: &Fills<'_>,
    ) -> Result<bool, PredicateError> {
        let mut all = true;
        for p in predicates {
            let want = match &p.test {
                Test::Exists => None,
                Test::Equals(v) | Test::Contains(v) => Some(expected(v, fills)?),
            };
            json::tokens(&p.pointer).map_err(|_| PredicateError::Pointer)?;
            let found = match &self.body {
                Some(body) => body.at(&p.pointer).map_err(|_| PredicateError::Pointer)?,
                None => None,
            };
            let ok = match (&p.test, found, &want) {
                (_, None, _) => false,
                (Test::Exists, Some(_), _) => true,
                (Test::Equals(_), Some(v), Some(w)) => v == w,
                (Test::Contains(_), Some(Json::Array(items)), Some(w)) => items.contains(w),
                _ => false,
            };
            all = all && ok;
        }
        Ok(all)
    }

    /// The value a capture takes, as one JSON value's text; none if its source
    /// is absent or no use could carry it.
    pub fn take(&self, capture: &Capture) -> Option<Vec<u8>> {
        let text = |s: &str| carried(s).then(|| quoted(s));
        match &capture.source {
            Source::Header(name) => text(std::str::from_utf8(one(self.reply, name)?).ok()?),
            Source::Location => {
                let location = std::str::from_utf8(one(self.reply, "location")?).ok()?;
                text(&segment(location)?)
            }
            Source::Json(pointer) => match self.body.as_ref()?.at(pointer).ok()?? {
                Json::String(s) => text(s),
                Json::Number(n) => Some(n.text().as_bytes().to_vec()),
                _ => None,
            },
        }
    }

    /// The wait `Retry-After` asks for, in microseconds: delta-seconds only.
    pub fn retry_after(&self) -> Option<u64> {
        let v = one(self.reply, "retry-after")?;
        if v.is_empty() || !v.iter().all(u8::is_ascii_digit) {
            return None;
        }
        let seconds = v.iter().try_fold(0u64, |acc, d| {
            acc.checked_mul(10)
                .and_then(|a| a.checked_add(u64::from(d.saturating_sub(b'0'))))
        });
        Some(seconds.map_or(u64::MAX, |s| s.saturating_mul(1_000_000)))
    }
}

/// A predicate's value, as JSON.
fn expected(v: &Value, fills: &Fills<'_>) -> Result<Json, PredicateError> {
    let text = match v {
        Value::Literal(s) => return json::parse(s.as_bytes()).map_err(|_| PredicateError::Literal),
        Value::Hole(h) => match fills.filled(h).map_err(PredicateError::Hole)? {
            Filled::Text(s) => return Ok(Json::String(s)),
            Filled::Integer(i) => i.to_string(),
            Filled::Json(t) => t,
            Filled::Bytes(_) => return Err(PredicateError::Hole(HoleError::NotText)),
        },
    };
    json::parse(text.as_bytes()).map_err(|_| PredicateError::Literal)
}
