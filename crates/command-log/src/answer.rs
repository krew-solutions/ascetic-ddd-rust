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
    clippy::unreachable,
    clippy::cast_possible_truncation
)]

use std::fmt;

use crate::fills::{Fills, HoleError};
use crate::json::{self, Json, Kind, Node, Sorted, Tape};
use crate::record::{Capture, Predicate, Source, Test, Value};
use crate::render::Filled;
use crate::shape::Shape;

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

/// An answer read: its body read once into a [`Tape`], on its first reader —
/// a problem's code, a predicate, a capture (ADR-0047 decision 23, K2b-7e-4a,
/// K2b-7e-4b-2): a body no one reads is never read, its tape never built.
/// Neither `Send` nor `Sync` (ADR-0047 decision 22, K2b-7e-2b-2c-3): a
/// carrier's attempt is a `Send` future, so no carrier holds a body's tape
/// across an await — a source's thread holds one at a time, as its memory is
/// charged, not one an attempt waiting.
///
/// ```compile_fail
/// use ascetic_ddd_command_log::answer::{Reply, read};
/// fn sent<T: Send>(_: T) {}
/// let reply = Reply { status: 200, headers: &[], body: None };
/// sent(read(&reply));
/// ```
///
/// Read where it is made, it is what it was:
///
/// ```
/// use ascetic_ddd_command_log::answer::{Reply, read};
/// let reply = Reply { status: 200, headers: &[], body: None };
/// assert_eq!(read(&reply).status(), 200);
/// ```
pub struct Read<'a> {
    reply: &'a Reply<'a>,
    body: std::cell::OnceCell<Option<Tape<'a>>>,
    here: std::marker::PhantomData<*const ()>,
}

impl fmt::Debug for Read<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Read")
            .field("reply", self.reply)
            .field("body", &self.body.get())
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

/// The answer read: its body to be read, if it is JSON, when first asked.
pub fn read<'a>(reply: &'a Reply<'a>) -> Read<'a> {
    Read {
        reply,
        body: std::cell::OnceCell::new(),
        here: std::marker::PhantomData,
    }
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

/// A carried string as one JSON value's text, held at its exact length: each
/// byte it escapes two (ADR-0047 decision 22, K2b-7e-2b-2b-3 — a value is
/// priced by its buffer's capacity, at most `VALUE`).
fn quoted(s: &str) -> Vec<u8> {
    let escaped = s
        .bytes()
        .filter(|b| matches!(b, b'"' | b'\\' | b'\t'))
        .count();
    let mut out = Vec::with_capacity(s.len().saturating_add(escaped).saturating_add(2));
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
    // Sized before it is: each `%XX` one byte, and past what a capture takes
    // not decoded at all (ADR-0047 decision 22, K2b-7e-2b-2c-2).
    let escapes = s.bytes().filter(|&b| b == b'%').count();
    let len = s.len().saturating_sub(escapes.saturating_mul(2));
    if len > TAKEN {
        return None;
    }
    let mut out = Vec::with_capacity(len);
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

/// The most bytes a captured value holds while a predicate reads it, a value
/// [`is_taken`] alone admits: parsed, its text copied, parsed again — a
/// number of `TAKEN` digits at its worst (ADR-0047 decision 22,
/// K2b-7e-2b-2c-2).
pub const HOLE: u64 = 4 * TAKEN as u64 + 2 * json::NODE as u64;

/// The most bytes `take` holds while it reads a capture, beside the value it
/// keeps: a `Location`'s segment decoded, at most `TAKEN` bytes, sized
/// before it is (K2b-7e-2b-2c-2).
pub const DECODE: u64 = TAKEN as u64;

/// The most bytes reading an answer of at most `cap` body bytes holds beside
/// its body and the values it takes, for a record of at most `record`'s
/// shape whose problem pointer is `problem` bytes (ADR-0047 decision 22,
/// K2b-7e-2b-2c-2; decision 23, K2b-7e-4b-2). Saturating. Its tape's entries
/// throughout; and the larger of two phases, each with one value read at a
/// time — a predicate's want, a captured hole's value, a capture's segment
/// or string decoded:
///
/// - the tape's build, the names' check beside its entries and the want of
///   the predicate whose reading built it;
/// - after it, the problem's code decoded — kept by the judge to its end —,
///   one pointer's tokens, the value read.
pub fn bound(cap: u64, record: &Shape, problem: u64) -> u64 {
    let hole = if record.holes > 0 { HOLE } else { 0 };
    let decode = if record.captures.elements > 0 {
        DECODE
    } else {
        0
    };
    let value = json::bound(record.literal_bytes).max(hole).max(decode);
    let build = json::names_bound(cap);
    let after = cap.saturating_add(json::pointer_bound(record.pointer_bytes.max(problem)));
    entries(cap)
        .saturating_add(build.max(after))
        .saturating_add(value)
}

/// A tape's entries for `n` bytes: [`json::tape_bound`] less its names'
/// check.
fn entries(n: u64) -> u64 {
    json::tape_bound(n).saturating_sub(json::names_bound(n))
}

/// Whether `value` is one [`Read::take`] makes: a carried text quoted, or a
/// carried JSON number — the only values a capture holds (ADR-0047 decision
/// 22, K2b-7e-2b-2c-2). Checked with nothing allocated, so a value no take
/// made — an array a host's carrier hands the shell — is refused before
/// anything parses it.
pub fn is_taken(value: &[u8]) -> bool {
    match value
        .strip_prefix(b"\"")
        .and_then(|v| v.strip_suffix(b"\""))
    {
        Some(inner) => is_quoted(inner),
        None => std::str::from_utf8(value).is_ok_and(|s| carried(s) && is_number(s)),
    }
}

/// Whether `inner` is a carried text as `quoted` writes it, its quotes
/// aside.
fn is_quoted(inner: &[u8]) -> bool {
    let visible = |c: u8| (0x21..=0x7e).contains(&c);
    let (mut n, mut first, mut last) = (0usize, None, None);
    let mut bytes = inner.iter().copied();
    while let Some(b) = bytes.next() {
        let c = match b {
            b'\\' => match bytes.next() {
                Some(b'"') => b'"',
                Some(b'\\') => b'\\',
                Some(b't') => b'\t',
                _ => return false,
            },
            b'"' | b'\t' => return false,
            c if visible(c) || c == b' ' => c,
            _ => return false,
        };
        n = n.saturating_add(1);
        first.get_or_insert(c);
        last = Some(c);
    }
    let dots = inner == b"." || inner == b"..";
    (1..=TAKEN).contains(&n) && !dots && first.is_some_and(visible) && last.is_some_and(visible)
}

/// Whether `s` is a JSON number: `-`, an integer part with no leading zero,
/// a fraction, an exponent.
fn is_number(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = usize::from(b.first() == Some(&b'-'));
    let digits = |i: &mut usize| {
        let start = *i;
        while b.get(*i).is_some_and(u8::is_ascii_digit) {
            *i = i.saturating_add(1);
        }
        i.saturating_sub(start)
    };
    match b.get(i) {
        Some(b'0') => i = i.saturating_add(1),
        Some(c) if c.is_ascii_digit() => {
            digits(&mut i);
        }
        _ => return false,
    }
    if b.get(i) == Some(&b'.') {
        i = i.saturating_add(1);
        if digits(&mut i) == 0 {
            return false;
        }
    }
    if matches!(b.get(i), Some(b'e' | b'E')) {
        i = i.saturating_add(1);
        if matches!(b.get(i), Some(b'+' | b'-')) {
            i = i.saturating_add(1);
        }
        if digits(&mut i) == 0 {
            return false;
        }
    }
    i == b.len()
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

    /// Its body's value, if it is JSON: read on this first call, its tape
    /// kept for the next — every reader of the body asks here, none decides
    /// before.
    fn parsed(&self) -> Option<Node<'_>> {
        self.body
            .get_or_init(|| self.reply.body.and_then(|b| json::tape(b).ok()))
            .as_ref()
            .map(Tape::root)
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
        let body = self.parsed()?;
        if body.kind() != Kind::Object {
            return None;
        }
        body.at(pointer).ok()??.string()
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
                Test::Equals(v) | Test::Contains(v) => Some(Sorted::new(expected(v, fills)?)),
            };
            json::tokens(&p.pointer).map_err(|_| PredicateError::Pointer)?;
            let found = match self.parsed() {
                Some(body) => body.at(&p.pointer).map_err(|_| PredicateError::Pointer)?,
                None => None,
            };
            let ok = match (&p.test, found, &want) {
                (_, None, _) => false,
                (Test::Exists, Some(_), _) => true,
                (Test::Equals(_), Some(v), Some(w)) => v.equals(w),
                (Test::Contains(_), Some(v), Some(w)) => v.contains(w),
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
            Source::Json(pointer) => {
                let v = self.parsed()?.at(pointer).ok()??;
                match v.kind() {
                    // Decoded only within what a use carries (K2b-7e-4b-2).
                    Kind::String => text(&v.string_within(TAKEN)?),
                    // A number as written, within what a use carries: a value
                    // taken is at most `TAKEN` bytes, whatever its kind
                    // (ADR-0047 decision 22).
                    Kind::Number => {
                        let n = v.number()?;
                        carried(n).then(|| n.as_bytes().to_vec())
                    }
                    Kind::Null | Kind::Bool | Kind::Array | Kind::Object => None,
                }
            }
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
