//! An answer's JSON, read by the crate's own rules (ADR-0047 decision 15).
//!
//! What a stand answered is judged by its JSON: a problem's code, the
//! predicates of `expect` and `until`, the values a step captures. serde_json
//! reads numbers by categories its build's features choose — with
//! `arbitrary_precision`, which a host's workspace may turn on for every crate,
//! `1e400` reads where it was refused, `-0` is an integer where it was a float,
//! a 23-digit id keeps its text where it became a float — so a judgement left
//! to it would depend on who built the shell. This reader is the crate's:
//!
//! - RFC 8259 over UTF-8, strictly: no byte order mark, no `NaN`, no comment,
//!   no trailing comma, nothing after the value but whitespace;
//! - a document is unreadable if a string escapes half of a surrogate pair, if
//!   an object names a member twice, if it nests deeper than [`DEPTH`], or if a
//!   number's exponent has more than [`EXPONENT_DIGITS`] digits once its
//!   leading zeros are gone;
//! - a number keeps its text; two numbers are equal when their decimal values
//!   are (`1`, `1.0`, `10e-1`; `-0` and `0`); two values are equal member by
//!   member, an object's members in any order;
//! - a pointer is RFC 6901's: `""` the document, each token after a `/`, `~1`
//!   a `/` and `~0` a `~`; an array's element by its index without leading
//!   zeros, `-` never one of an array's elements — but an object's member named
//!   `-`.
//!
//! `vectors/json.py` reads the same documents by Python's `json` and
//! `decimal`, outside this implementation.

#![deny(
    clippy::indexing_slicing,
    clippy::string_slice,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

use std::collections::BTreeSet;
use std::fmt;

/// The deepest an answer nests: arrays and objects inside one another.
pub const DEPTH: usize = 128;

/// The most digits a number's exponent has, its leading zeros dropped.
pub const EXPONENT_DIGITS: usize = 18;

/// A JSON value. Its `Debug` shows only its shape: an answer may carry a
/// secret a step captures.
#[derive(Clone)]
pub enum Json {
    /// `null`.
    Null,
    /// `true` or `false`.
    Bool(bool),
    /// A number, by its text.
    Number(Number),
    /// A string, its escapes decoded.
    String(String),
    /// An array.
    Array(Vec<Json>),
    /// An object: its members in the order written, each name once.
    Object(Vec<(String, Json)>),
}

/// A number: its text, and its decimal value as `±digits · 10^exponent`, the
/// digits with no leading or trailing zero (none for zero, whatever its sign).
#[derive(Clone)]
pub struct Number {
    text: String,
    negative: bool,
    digits: String,
    exponent: i64,
}

impl Number {
    /// The number as the answer wrote it.
    pub fn text(&self) -> &str {
        &self.text
    }
}

impl PartialEq for Number {
    fn eq(&self, other: &Number) -> bool {
        (self.negative, &self.digits, self.exponent)
            == (other.negative, &other.digits, other.exponent)
    }
}

impl Eq for Number {}

impl PartialEq for Json {
    fn eq(&self, other: &Json) -> bool {
        match (self, other) {
            (Json::Null, Json::Null) => true,
            (Json::Bool(a), Json::Bool(b)) => a == b,
            (Json::Number(a), Json::Number(b)) => a == b,
            (Json::String(a), Json::String(b)) => a == b,
            (Json::Array(a), Json::Array(b)) => a == b,
            (Json::Object(a), Json::Object(b)) => {
                // Each name once (the reader refuses a second): by name.
                a.len() == b.len()
                    && sorted(a)
                        .into_iter()
                        .zip(sorted(b))
                        .all(|((k, v), (l, w))| k == l && v == w)
            }
            _ => false,
        }
    }
}

/// An object's members by name.
fn sorted(o: &[(String, Json)]) -> Vec<&(String, Json)> {
    let mut o: Vec<&(String, Json)> = o.iter().collect();
    o.sort_by(|x, y| x.0.cmp(&y.0));
    o
}

impl Eq for Json {}

impl fmt::Debug for Json {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Json::Null => f.write_str("null"),
            Json::Bool(_) => f.write_str("a bool"),
            Json::Number(_) => f.write_str("a number"),
            Json::String(s) => write!(f, "a string of {} bytes", s.len()),
            Json::Array(a) => write!(f, "an array of {}", a.len()),
            Json::Object(o) => write!(f, "an object of {}", o.len()),
        }
    }
}

impl fmt::Debug for Number {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a number")
    }
}

/// Why a document is unreadable. It names a place, never a value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JsonError {
    /// Not UTF-8.
    Utf8,
    /// Not JSON at this byte.
    Syntax {
        /// The byte's offset.
        at: usize,
    },
    /// Nested deeper than [`DEPTH`].
    Depth,
    /// An object names a member twice.
    Duplicate {
        /// The offset of the second.
        at: usize,
    },
    /// A string escapes half of a surrogate pair.
    Surrogate {
        /// The escape's offset.
        at: usize,
    },
    /// A number's exponent past [`EXPONENT_DIGITS`].
    Exponent {
        /// The number's offset.
        at: usize,
    },
}

impl fmt::Display for JsonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JsonError::Utf8 => f.write_str("not UTF-8"),
            JsonError::Syntax { at } => write!(f, "not JSON at byte {at}"),
            JsonError::Depth => write!(f, "nested deeper than {DEPTH}"),
            JsonError::Duplicate { at } => write!(f, "a member named twice, at byte {at}"),
            JsonError::Surrogate { at } => write!(f, "half of a surrogate pair, at byte {at}"),
            JsonError::Exponent { at } => {
                write!(f, "an exponent past {EXPONENT_DIGITS} digits, at byte {at}")
            }
        }
    }
}

impl std::error::Error for JsonError {}

/// A document read whole.
pub fn parse(bytes: &[u8]) -> Result<Json, JsonError> {
    let text = std::str::from_utf8(bytes).map_err(|_| JsonError::Utf8)?;
    let mut p = Parser { text, at: 0 };
    p.space();
    let value = p.value(0)?;
    p.space();
    if p.at != text.len() {
        return Err(JsonError::Syntax { at: p.at });
    }
    Ok(value)
}

struct Parser<'a> {
    text: &'a str,
    at: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Option<u8> {
        self.text.as_bytes().get(self.at).copied()
    }

    fn bump(&mut self) {
        self.at = self.at.saturating_add(1);
    }

    fn syntax(&self) -> JsonError {
        JsonError::Syntax { at: self.at }
    }

    fn eat(&mut self, b: u8) -> Result<(), JsonError> {
        if self.peek() == Some(b) {
            self.bump();
            Ok(())
        } else {
            Err(self.syntax())
        }
    }

    fn space(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.bump();
        }
    }

    fn word(&mut self, word: &str, value: Json) -> Result<Json, JsonError> {
        let end = self.at.saturating_add(word.len());
        if self.text.get(self.at..end) == Some(word) {
            self.at = end;
            Ok(value)
        } else {
            Err(self.syntax())
        }
    }

    fn value(&mut self, depth: usize) -> Result<Json, JsonError> {
        match self.peek() {
            Some(b'{') => self.object(depth),
            Some(b'[') => self.array(depth),
            Some(b'"') => self.string().map(Json::String),
            Some(b't') => self.word("true", Json::Bool(true)),
            Some(b'f') => self.word("false", Json::Bool(false)),
            Some(b'n') => self.word("null", Json::Null),
            Some(b'-' | b'0'..=b'9') => self.number().map(Json::Number),
            _ => Err(self.syntax()),
        }
    }

    fn deeper(depth: usize) -> Result<usize, JsonError> {
        let depth = depth.saturating_add(1);
        if depth > DEPTH {
            Err(JsonError::Depth)
        } else {
            Ok(depth)
        }
    }

    fn array(&mut self, depth: usize) -> Result<Json, JsonError> {
        let depth = Self::deeper(depth)?;
        self.eat(b'[')?;
        let mut items = Vec::new();
        self.space();
        if self.peek() == Some(b']') {
            self.bump();
            return Ok(Json::Array(items));
        }
        loop {
            self.space();
            items.push(self.value(depth)?);
            self.space();
            match self.peek() {
                Some(b',') => self.bump(),
                Some(b']') => {
                    self.bump();
                    return Ok(Json::Array(items));
                }
                _ => return Err(self.syntax()),
            }
        }
    }

    fn object(&mut self, depth: usize) -> Result<Json, JsonError> {
        let depth = Self::deeper(depth)?;
        self.eat(b'{')?;
        let mut members: Vec<(String, Json)> = Vec::new();
        let mut names: BTreeSet<String> = BTreeSet::new();
        self.space();
        if self.peek() == Some(b'}') {
            self.bump();
            return Ok(Json::Object(members));
        }
        loop {
            self.space();
            let at = self.at;
            if self.peek() != Some(b'"') {
                return Err(self.syntax());
            }
            let name = self.string()?;
            self.space();
            self.eat(b':')?;
            self.space();
            let value = self.value(depth)?;
            if !names.insert(name.clone()) {
                return Err(JsonError::Duplicate { at });
            }
            members.push((name, value));
            self.space();
            match self.peek() {
                Some(b',') => self.bump(),
                Some(b'}') => {
                    self.bump();
                    return Ok(Json::Object(members));
                }
                _ => return Err(self.syntax()),
            }
        }
    }

    /// Four hex digits after `\u`.
    fn hex4(&mut self) -> Result<u32, JsonError> {
        let end = self.at.saturating_add(4);
        let digits = self.text.get(self.at..end).ok_or_else(|| self.syntax())?;
        if !digits.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(self.syntax());
        }
        let v = u32::from_str_radix(digits, 16).map_err(|_| self.syntax())?;
        self.at = end;
        Ok(v)
    }

    fn string(&mut self) -> Result<String, JsonError> {
        self.eat(b'"')?;
        let mut out = String::new();
        loop {
            let start = self.at;
            while matches!(self.peek(), Some(b) if b != b'"' && b != b'\\' && b >= 0x20) {
                self.bump();
            }
            // Every stop is at an ASCII byte, so the run is whole characters.
            out.push_str(self.text.get(start..self.at).ok_or_else(|| self.syntax())?);
            match self.peek() {
                Some(b'"') => {
                    self.bump();
                    return Ok(out);
                }
                Some(b'\\') => {
                    let escape = self.at;
                    self.bump();
                    let c = match self.peek() {
                        Some(b'"') => '"',
                        Some(b'\\') => '\\',
                        Some(b'/') => '/',
                        Some(b'b') => '\u{8}',
                        Some(b'f') => '\u{c}',
                        Some(b'n') => '\n',
                        Some(b'r') => '\r',
                        Some(b't') => '\t',
                        Some(b'u') => {
                            self.bump();
                            let c = self.unicode(escape)?;
                            out.push(c);
                            continue;
                        }
                        _ => return Err(self.syntax()),
                    };
                    self.bump();
                    out.push(c);
                }
                // A control character unescaped, or the end.
                _ => return Err(self.syntax()),
            }
        }
    }

    /// The character a `\u` escape names, after its `u`: a pair of escapes for
    /// one outside the Basic Multilingual Plane.
    fn unicode(&mut self, escape: usize) -> Result<char, JsonError> {
        let half = JsonError::Surrogate { at: escape };
        let high = self.hex4()?;
        let code = match high {
            0xD800..=0xDBFF => {
                if self.text.get(self.at..self.at.saturating_add(2)) != Some("\\u") {
                    return Err(half);
                }
                self.at = self.at.saturating_add(2);
                let low = self.hex4()?;
                if !(0xDC00..=0xDFFF).contains(&low) {
                    return Err(half);
                }
                // Both halves in range: at most 0x10FFFF.
                high.checked_sub(0xD800)
                    .and_then(|h| h.checked_mul(0x400))
                    .and_then(|h| h.checked_add(low.checked_sub(0xDC00)?))
                    .and_then(|c| c.checked_add(0x10000))
                    .ok_or_else(|| half.clone())?
            }
            0xDC00..=0xDFFF => return Err(half),
            c => c,
        };
        char::from_u32(code).ok_or(half)
    }

    fn digits(&mut self) -> usize {
        let start = self.at;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.bump();
        }
        self.at.saturating_sub(start)
    }

    fn number(&mut self) -> Result<Number, JsonError> {
        let start = self.at;
        let negative = self.peek() == Some(b'-');
        if negative {
            self.bump();
        }
        let int_start = self.at;
        match self.peek() {
            Some(b'0') => self.bump(),
            Some(b'1'..=b'9') => {
                self.digits();
            }
            _ => return Err(self.syntax()),
        }
        let int = self.text.get(int_start..self.at).unwrap_or_default();
        let mut frac = "";
        if self.peek() == Some(b'.') {
            self.bump();
            let f = self.at;
            if self.digits() == 0 {
                return Err(self.syntax());
            }
            frac = self.text.get(f..self.at).unwrap_or_default();
        }
        let mut exponent: i64 = 0;
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.bump();
            let minus = match self.peek() {
                Some(b'-') => {
                    self.bump();
                    true
                }
                Some(b'+') => {
                    self.bump();
                    false
                }
                _ => false,
            };
            let e = self.at;
            if self.digits() == 0 {
                return Err(self.syntax());
            }
            let e = self
                .text
                .get(e..self.at)
                .unwrap_or_default()
                .trim_start_matches('0');
            if e.len() > EXPONENT_DIGITS {
                return Err(JsonError::Exponent { at: start });
            }
            let magnitude: i64 = if e.is_empty() {
                0
            } else {
                e.parse().map_err(|_| JsonError::Exponent { at: start })?
            };
            exponent = if minus {
                magnitude
                    .checked_neg()
                    .ok_or(JsonError::Exponent { at: start })?
            } else {
                magnitude
            };
        }
        let text = self.text.get(start..self.at).unwrap_or_default().to_owned();
        // The value: int.frac · 10^exponent = (int frac) · 10^(exponent − |frac|),
        // its digits stripped of leading zeros, then of trailing ones.
        let all = format!("{int}{frac}");
        let lead = all.trim_start_matches('0');
        let digits = lead.trim_end_matches('0');
        let trailing = lead.len().saturating_sub(digits.len());
        let overflow = JsonError::Exponent { at: start };
        let exponent = i64::try_from(frac.len())
            .ok()
            .and_then(|f| exponent.checked_sub(f))
            .and_then(|e| e.checked_add(i64::try_from(trailing).ok()?))
            .ok_or(overflow)?;
        let zero = digits.is_empty();
        Ok(Number {
            text,
            negative: negative && !zero,
            digits: digits.to_owned(),
            exponent: if zero { 0 } else { exponent },
        })
    }
}

/// A pointer that is not RFC 6901's: neither empty nor starting with `/`, or
/// a `~` not followed by `0` or `1`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Malformed;

impl fmt::Display for Malformed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("not an RFC 6901 pointer")
    }
}

impl std::error::Error for Malformed {}

/// A pointer's reference tokens, unescaped: none for the document itself.
pub fn tokens(pointer: &str) -> Result<Vec<String>, Malformed> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    let rest = pointer.strip_prefix('/').ok_or(Malformed)?;
    rest.split('/')
        .map(|t| {
            let mut out = String::with_capacity(t.len());
            let mut chars = t.chars();
            while let Some(c) = chars.next() {
                if c == '~' {
                    match chars.next() {
                        Some('0') => out.push('~'),
                        Some('1') => out.push('/'),
                        _ => return Err(Malformed),
                    }
                } else {
                    out.push(c);
                }
            }
            Ok(out)
        })
        .collect()
}

/// An array index token: `0`, or digits not starting with `0`.
fn index(token: &str) -> Option<usize> {
    let well_formed = token == "0"
        || (!token.is_empty()
            && !token.starts_with('0')
            && token.bytes().all(|b| b.is_ascii_digit()));
    if well_formed {
        token.parse().ok()
    } else {
        None
    }
}

impl Json {
    /// The value `pointer` names, if the document has one there.
    pub fn at(&self, pointer: &str) -> Result<Option<&Json>, Malformed> {
        let mut here = self;
        for token in tokens(pointer)? {
            let next = match here {
                Json::Object(members) => members.iter().find(|(k, _)| *k == token).map(|(_, v)| v),
                Json::Array(items) => index(&token).and_then(|i| items.get(i)),
                _ => None,
            };
            match next {
                Some(v) => here = v,
                None => return Ok(None),
            }
        }
        Ok(Some(here))
    }
}
