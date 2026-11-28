//! The request a REST step sends (ADR-0047 decision 15): its frame, and the
//! static half of it the validator checks.
//!
//! - The path: the operation's template, its literals verbatim — held to RFC
//!   3986 `pchar` and `/`, no `%`, so that a custom method's `:verb` reaches
//!   its route, which a gateway matches on the undecoded path — and each
//!   `{name}` filled in order by the step's value, every byte outside
//!   unreserved percent-encoded in upper-case hex; a value empty, `.` or `..`
//!   is refused, as a gateway would route it as another path.
//! - The query: `name=value` in the step's order, both encoded alike, joined
//!   by `&`; no `?` when there are none.
//! - Planned headers: a token name and a value of visible ASCII with inner
//!   blanks only. Reserved, compared without case for planned names, the
//!   operation's decorated names and the correlation header alike: `Host`,
//!   `Content-Length`, `Transfer-Encoding`, `Connection`, `Content-Type`; a
//!   planned header may not take a decorated name or the correlation header's,
//!   which the shell sets on every attempt. `content-type` follows them, the
//!   operation's, when there is a body.
//! - The body: the payload's spans filled.
//!
//! A frame lives as long as its attempts are resends of one request: a call's
//! is built at its first dispatch and resent byte for byte, a poll's for that
//! poll. The target is relative to the stand's base, which the shell's `Stand`
//! port gives with the scheme and the authority.

use std::fmt;

use crate::fills::{Fills, HoleError};
use crate::record::{Action, Operation, Probe, Rest, Value};
use crate::render::{FillError, fill};

/// The header names a step never plans, compared without case.
pub const RESERVED: [&str; 5] = [
    "host",
    "content-length",
    "transfer-encoding",
    "connection",
    "content-type",
];

/// What a REST attempt sends, but for what the shell adds on each attempt:
/// the correlation id, the decorated headers, the scheme and the authority.
/// Its `Debug` shows the method, the target's length and the headers' names.
#[derive(Clone, PartialEq, Eq)]
pub struct Frame {
    /// The method.
    pub method: String,
    /// The path and the query, from the stand's base.
    pub target: String,
    /// The planned headers in the step's order, then `content-type` with a
    /// body.
    pub headers: Vec<(String, String)>,
    /// The body.
    pub body: Option<Vec<u8>>,
}

impl fmt::Debug for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names: Vec<&str> = self.headers.iter().map(|(n, _)| n.as_str()).collect();
        f.debug_struct("Frame")
            .field("method", &self.method)
            .field("target", &format_args!("{} bytes", self.target.len()))
            .field("headers", &names)
            .field(
                "body",
                &format_args!("{:?}", self.body.as_ref().map(Vec::len)),
            )
            .finish()
    }
}

/// Why a step's request cannot be built. It names a field, never a value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RequestError {
    /// The method is not a token.
    Method,
    /// The path template is not `/`-rooted literals and `{name}`s.
    Template,
    /// A literal of the template outside `pchar` and `/`.
    Literal,
    /// The step's path values do not fill the template's placeholders.
    Arity {
        /// The placeholders.
        placeholders: usize,
        /// The values.
        values: usize,
    },
    /// A path value empty, `.` or `..`.
    Segment {
        /// Its place, from 0.
        index: usize,
    },
    /// A header name that is not a token.
    HeaderName {
        /// The name.
        name: String,
    },
    /// A header value outside visible ASCII and inner blanks.
    HeaderValue {
        /// The header's name.
        name: String,
    },
    /// A planned header of a reserved, decorated or correlation name; or a
    /// decorated name, or the correlation header, reserved.
    Reserved {
        /// The name.
        name: String,
    },
    /// The operation's content type is not a header value.
    ContentType,
    /// A hole that cannot be filled.
    Hole {
        /// Where: `path[i]`, `query <name>`, `header <name>`, `body`.
        field: String,
        /// Why.
        error: HoleError,
    },
    /// The body's spans cannot be filled.
    Fill(FillError),
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestError::Method => f.write_str("the method is not a token"),
            RequestError::Template => f.write_str("the path template is malformed"),
            RequestError::Literal => {
                f.write_str("a literal of the path template is outside pchar and /")
            }
            RequestError::Arity {
                placeholders,
                values,
            } => write!(f, "{values} path values for {placeholders} placeholders"),
            RequestError::Segment { index } => {
                write!(f, "path value {index} is empty, `.` or `..`")
            }
            RequestError::HeaderName { name } => write!(f, "{name}: not a token"),
            RequestError::HeaderValue { name } => {
                write!(f, "{name}: a value outside visible ASCII and inner blanks")
            }
            RequestError::Reserved { name } => write!(f, "{name}: a reserved header"),
            RequestError::ContentType => f.write_str("the content type is not a header value"),
            RequestError::Hole { field, error } => write!(f, "{field}: {error}"),
            RequestError::Fill(e) => write!(f, "body: {e}"),
        }
    }
}

impl std::error::Error for RequestError {}

/// The REST call an action makes: its own, or an observation's poll.
pub fn rest_of(action: &Action) -> Option<&Rest> {
    match action {
        Action::Rest(r) => Some(r),
        Action::Observe(o) => match &o.probe {
            Probe::Rest(r) => Some(r),
            Probe::Bus { .. } => None,
        },
        Action::Bus(_) | Action::Host(_) => None,
    }
}

/// RFC 9110's `tchar`.
fn tchar(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

/// Whether `s` is an RFC 9110 token: a header's or a method's name.
pub fn token(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(tchar)
}

/// A field value: visible ASCII and blanks, no blank first or last.
fn field_value(s: &str) -> bool {
    let b = s.as_bytes();
    let visible = |c: &u8| (0x21..=0x7e).contains(c);
    b.iter().all(|c| visible(c) || *c == b' ' || *c == b'\t')
        && b.first().is_none_or(visible)
        && b.last().is_none_or(visible)
}

fn unreserved(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"-._~".contains(&b)
}

/// A literal's byte: `pchar` but `%` (unreserved, sub-delims, `:`, `@`), and `/`.
fn literal(b: u8) -> bool {
    unreserved(b) || b"!$&'()*+,;=:@/".contains(&b)
}

/// The length of `s` encoded: an unreserved byte as it is, any other as
/// `%XX`.
fn encoded(s: &str) -> usize {
    s.bytes().fold(0usize, |n, b| {
        n.saturating_add(if unreserved(b) { 1 } else { 3 })
    })
}

/// Every byte outside unreserved as `%XX`, upper-case, into `out`.
fn encode(out: &mut String, s: &str) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for b in s.bytes() {
        if unreserved(b) {
            out.push(char::from(b));
        } else {
            out.push('%');
            out.push(char::from(HEX[usize::from(b >> 4)]));
            out.push(char::from(HEX[usize::from(b & 0xF)]));
        }
    }
}

enum Piece<'a> {
    Literal(&'a str),
    Placeholder,
}

/// The template's literals and placeholders, in order.
fn template(path: &str) -> Result<Vec<Piece<'_>>, RequestError> {
    if !path.starts_with('/') {
        return Err(RequestError::Template);
    }
    let mut out = Vec::new();
    let mut rest = path;
    while !rest.is_empty() {
        match rest.find(['{', '}']) {
            None => {
                out.push(Piece::Literal(rest));
                rest = "";
            }
            Some(i) => {
                let (lit, tail) = rest.split_at(i);
                if !lit.is_empty() {
                    out.push(Piece::Literal(lit));
                }
                let name = tail.strip_prefix('{').ok_or(RequestError::Template)?;
                let end = name.find('}').ok_or(RequestError::Template)?;
                let (name, tail) = name.split_at(end);
                if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                {
                    return Err(RequestError::Template);
                }
                out.push(Piece::Placeholder);
                rest = tail.strip_prefix('}').ok_or(RequestError::Template)?;
            }
        }
    }
    if out
        .iter()
        .any(|p| matches!(p, Piece::Literal(l) if !l.bytes().all(literal)))
    {
        return Err(RequestError::Literal);
    }
    Ok(out)
}

fn reserved(name: &str, op: &Operation, correlation: &str) -> bool {
    RESERVED.iter().any(|r| r.eq_ignore_ascii_case(name))
        || op.decorated.iter().any(|d| d.eq_ignore_ascii_case(name))
        || correlation.eq_ignore_ascii_case(name)
}

/// What a request's frame needs that the step's values cannot change: the
/// method, the template and its arity, the decorated names and the
/// correlation header's, the planned headers' names and literal values, the
/// content type, the literal path values. The validator's half (decision 15).
pub fn check(op: &Operation, rest: &Rest, correlation: &str) -> Result<(), RequestError> {
    if !token(&op.method) {
        return Err(RequestError::Method);
    }
    let placeholders = template(&op.path)?
        .iter()
        .filter(|p| matches!(p, Piece::Placeholder))
        .count();
    if placeholders != rest.path.len() {
        return Err(RequestError::Arity {
            placeholders,
            values: rest.path.len(),
        });
    }
    for (index, v) in rest.path.iter().enumerate() {
        if let Value::Literal(s) = v
            && matches!(s.as_str(), "" | "." | "..")
        {
            return Err(RequestError::Segment { index });
        }
    }
    for name in op.decorated.iter().map(String::as_str).chain([correlation]) {
        if !token(name) {
            return Err(RequestError::HeaderName {
                name: name.to_owned(),
            });
        }
        if RESERVED.iter().any(|r| r.eq_ignore_ascii_case(name)) {
            return Err(RequestError::Reserved {
                name: name.to_owned(),
            });
        }
    }
    if op
        .decorated
        .iter()
        .any(|d| d.eq_ignore_ascii_case(correlation))
    {
        return Err(RequestError::Reserved {
            name: correlation.to_owned(),
        });
    }
    for h in &rest.headers {
        if !token(&h.name) {
            return Err(RequestError::HeaderName {
                name: h.name.clone(),
            });
        }
        if reserved(&h.name, op, correlation) {
            return Err(RequestError::Reserved {
                name: h.name.clone(),
            });
        }
        if let Value::Literal(s) = &h.value
            && !field_value(s)
        {
            return Err(RequestError::HeaderValue {
                name: h.name.clone(),
            });
        }
    }
    if rest.body.is_some() && (op.content_type.is_empty() || !field_value(&op.content_type)) {
        return Err(RequestError::ContentType);
    }
    Ok(())
}

/// The frame of `rest`, a call of `op`, its holes filled from `fills`;
/// `correlation` is the header the shell sets on every attempt.
pub fn frame(
    op: &Operation,
    rest: &Rest,
    fills: &Fills<'_>,
    correlation: &str,
) -> Result<Frame, RequestError> {
    check(op, rest, correlation)?;
    let hole = |field: String| move |error| RequestError::Hole { field, error };
    // The target sized at once (ADR-0047 decision 22, K2b-7e-2b-3a): its
    // values filled first, its length counted, then written — never grown by
    // doubling, so a carrier prices it by its encoded bytes.
    let pieces = template(&op.path)?;
    let mut values = rest.path.iter().enumerate();
    let mut path = Vec::with_capacity(rest.path.len());
    for piece in &pieces {
        if let Piece::Placeholder = piece {
            let (index, v) = values.next().ok_or(RequestError::Arity {
                placeholders: rest.path.len().saturating_add(1),
                values: rest.path.len(),
            })?;
            let v = fills.text(v).map_err(hole(format!("path[{index}]")))?;
            if matches!(v.as_str(), "" | "." | "..") {
                return Err(RequestError::Segment { index });
            }
            path.push(v);
        }
    }
    let mut query = Vec::with_capacity(rest.query.len());
    for q in &rest.query {
        query.push(
            fills
                .text(&q.value)
                .map_err(hole(format!("query {}", q.name)))?,
        );
    }
    let length = pieces
        .iter()
        .map(|p| match p {
            Piece::Literal(l) => l.len(),
            Piece::Placeholder => 0,
        })
        .chain(path.iter().map(|v| encoded(v)))
        .chain(rest.query.iter().zip(&query).map(|(q, v)| {
            encoded(&q.name)
                .saturating_add(encoded(v))
                .saturating_add(2)
        }))
        .fold(0usize, usize::saturating_add);
    let mut target = String::with_capacity(length);
    let mut path = path.iter();
    for piece in &pieces {
        match piece {
            Piece::Literal(l) => target.push_str(l),
            Piece::Placeholder => {
                if let Some(v) = path.next() {
                    encode(&mut target, v);
                }
            }
        }
    }
    for (i, (q, v)) in rest.query.iter().zip(&query).enumerate() {
        target.push(if i == 0 { '?' } else { '&' });
        encode(&mut target, &q.name);
        target.push('=');
        encode(&mut target, v);
    }
    let mut headers = Vec::with_capacity(rest.headers.len().saturating_add(1));
    for h in &rest.headers {
        let v = fills
            .text(&h.value)
            .map_err(hole(format!("header {}", h.name)))?;
        if !field_value(&v) {
            return Err(RequestError::HeaderValue {
                name: h.name.clone(),
            });
        }
        headers.push((h.name.clone(), v));
    }
    let body = match &rest.body {
        None => None,
        Some(payload) => {
            headers.push(("content-type".to_owned(), op.content_type.clone()));
            let values = payload
                .holes
                .iter()
                .map(|span| fills.filled(&span.hole))
                .collect::<Result<Vec<_>, _>>()
                .map_err(hole("body".to_owned()))?;
            let mut values = values.into_iter();
            Some(
                fill(payload, |_| values.next().ok_or(FillError::Span))
                    .map_err(RequestError::Fill)?,
            )
        }
    };
    Ok(Frame {
        method: op.method.clone(),
        target,
        headers,
        body,
    })
}
