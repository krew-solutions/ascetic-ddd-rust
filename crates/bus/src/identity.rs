//! The identity a message carries in its headers (ADR-0003, ADR-0030): the
//! Tenant it is under, the stream it belongs to and its position there.
//!
//! The inbox stores a message under this identity and takes a second one
//! under it for a redelivery of the first, ignoring it; it orders a stream's
//! messages by their positions. So a producer owes one message per identity
//! on a channel, and the outbox refuses a transaction that writes two
//! (ADR-0030). The names are the headers', and the outbox's `metadata`
//! fields of the same names.
//!
//! **The stream id is JSON text** (ADR-0037 decision 1), read by
//! [`read_stream_id`] and written by [`write_stream_id`], its inverse. The
//! reader is the library's own scanner of RFC 8259, never serde_json's
//! parser: serde_json reads numbers, `-0` and its private keys differently
//! by feature, and a host's features are unified into the library's build,
//! so one header would be one identity in one build and another in the
//! next. The scanner builds the value from what it read, the same in every
//! build, and admits only what PostgreSQL's `jsonb` holds and compares
//! exactly — the domain D:
//!
//! - every number is an integer written without fraction or exponent, not
//!   `-0`, within `[i64::MIN, u64::MAX]`. `jsonb` compares numbers by value
//!   and prints them with their scale, so `1` and `1.0` would be one key and
//!   two slots; and past `numeric`'s scale PostgreSQL refuses a number;
//! - no string or key holds U+0000 or a lone surrogate: `jsonb` refuses the
//!   first, and a Rust string cannot hold the second;
//! - no key, once unescaped, begins with serde_json's private prefix, which
//!   its reader takes for a number or a raw value under its features;
//! - no object names a key twice, and nesting is bounded.
//!
//! A header's text that is not JSON by the grammar is a raw id, the string
//! of that text: a producer that stamps a UUID unquoted means nothing else.
//! JSON outside D is refused, never read as a string; so is a text that
//! begins as JSON — a quote, a bracket or a brace — and is not, such as a
//! string with a raw control character or an escape RFC 8259 does not
//! know: its producer meant JSON, and reading it as a raw id would give the
//! stream another identity than the one meant, in silence.

use std::fmt;

use serde_json::{Map, Number, Value};

/// The Tenant the message is under.
pub const TENANT_ID: &str = "tenant_id";
/// The stream's type.
pub const STREAM_TYPE: &str = "stream_type";
/// The stream's id, as JSON text: the inbox compares it as JSON
/// ([`read_stream_id`]).
pub const STREAM_ID: &str = "stream_id";
/// The message's position in its stream.
pub const STREAM_POSITION: &str = "stream_position";

/// The identity's headers, in the order the inbox's key names them.
pub const HEADERS: [&str; 4] = [TENANT_ID, STREAM_TYPE, STREAM_ID, STREAM_POSITION];

/// The deepest a stream id nests: the inbox wraps it in a dependency's
/// object, and its readers read `jsonb` back through serde_json, whose
/// limit is 128 nested values (ADR-0037).
pub const STREAM_ID_DEPTH: usize = 32;

/// The prefix of serde_json's private keys: an object whose first key is
/// `$serde_json::private::Number` is read as a number under
/// `arbitrary_precision`, and one of `…RawValue` as a raw value.
const PRIVATE_KEYS: &str = "$serde_json::private::";

/// Why a header's text is no identity, in the library's words: never the
/// text's, which may be personal data or the very value refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Refused(&'static str);

impl Refused {
    /// The reason.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for Refused {}

/// The stream id a `stream_id` header's text names: the JSON value it
/// writes, when it is JSON in the domain D; the string of the text, when it
/// is not JSON at all.
///
/// # Errors
/// [`Refused`] when the text is JSON outside D, begins as JSON and is not,
/// or is a raw id holding U+0000.
pub fn read_stream_id(text: &str) -> Result<Value, Refused> {
    match read_json(text, STREAM_ID_DEPTH)? {
        Some(id) => Ok(id),
        None if begins_as_json(text) => Err(Refused("the stream id begins as JSON and is not")),
        None if text.contains('\0') => Err(Refused("a raw stream id holds U+0000")),
        None => Ok(Value::String(text.to_owned())),
    }
}

/// Whether `text`, past its leading whitespace, opens a JSON string, array
/// or object: what no raw id does. A digit is no such sign, since a UUID
/// may begin with one.
fn begins_as_json(text: &str) -> bool {
    text.trim_start_matches([' ', '\t', '\n', '\r'])
        .starts_with(['"', '[', '{'])
}

/// The `stream_id` header's text for `id`, the inverse of
/// [`read_stream_id`]: a string whose text the reader would take for JSON,
/// or refuse as beginning as JSON, is written quoted; any other string as
/// it is, and every other value as JSON. So
/// `read_stream_id(&write_stream_id(v)) == Ok(v)` for every v in D; a value
/// outside D is written as JSON, which the reader refuses.
#[must_use]
pub fn write_stream_id(id: &Value) -> String {
    match id {
        Value::String(text) if !is_json(text) && !begins_as_json(text) => text.clone(),
        other => other.to_string(),
    }
}

/// The JSON value `text` writes, when it is JSON in the domain D nested at
/// most `max_depth` deep; `None` when the text is not JSON by RFC 8259's
/// grammar. The grammar is checked over the whole text before any value is
/// built, so a text that only begins as JSON is not JSON.
///
/// # Errors
/// [`Refused`] when the text is JSON outside D.
pub fn read_json(text: &str, max_depth: usize) -> Result<Option<Value>, Refused> {
    if !is_json(text) {
        return Ok(None);
    }
    let mut reader = Reader {
        bytes: text.as_bytes(),
        at: whitespace_from(text.as_bytes(), 0),
        max_depth,
    };
    reader.value(0).map(Some)
}

/// Whether `text` is one JSON value by RFC 8259's grammar, with whitespace
/// around it. Iterative, so that no depth of nesting overflows the stack.
fn is_json(text: &str) -> bool {
    let bytes = text.as_bytes();
    let mut at = whitespace_from(bytes, 0);
    let mut open: Vec<u8> = Vec::new();
    loop {
        // A value at `at`.
        match bytes.get(at) {
            Some(b'{') => {
                at = whitespace_from(bytes, at + 1);
                if bytes.get(at) == Some(&b'}') {
                    at += 1;
                } else {
                    open.push(b'{');
                    match member_from(bytes, at) {
                        Some(next) => at = next,
                        None => return false,
                    }
                    continue;
                }
            }
            Some(b'[') => {
                at = whitespace_from(bytes, at + 1);
                if bytes.get(at) == Some(&b']') {
                    at += 1;
                } else {
                    open.push(b'[');
                    continue;
                }
            }
            Some(b'"') => match string_end(bytes, at) {
                Some(next) => at = next,
                None => return false,
            },
            Some(b'-' | b'0'..=b'9') => match number_end(bytes, at) {
                Some((next, _)) => at = next,
                None => return false,
            },
            Some(b't') if bytes[at..].starts_with(b"true") => at += 4,
            Some(b'f') if bytes[at..].starts_with(b"false") => at += 5,
            Some(b'n') if bytes[at..].starts_with(b"null") => at += 4,
            _ => return false,
        }
        // After a value: close what it ends, or go on to the next.
        loop {
            at = whitespace_from(bytes, at);
            match (open.last(), bytes.get(at)) {
                (None, _) => return at == bytes.len(),
                (Some(b'['), Some(b']')) | (Some(b'{'), Some(b'}')) => {
                    open.pop();
                    at += 1;
                }
                (Some(b'['), Some(b',')) => {
                    at = whitespace_from(bytes, at + 1);
                    break;
                }
                (Some(b'{'), Some(b',')) => {
                    match member_from(bytes, whitespace_from(bytes, at + 1)) {
                        Some(next) => at = next,
                        None => return false,
                    }
                    break;
                }
                _ => return false,
            }
        }
    }
}

/// Past an object member's key and its colon, at its value; `None` when the
/// text is not that.
fn member_from(bytes: &[u8], at: usize) -> Option<usize> {
    if bytes.get(at) != Some(&b'"') {
        return None;
    }
    let at = whitespace_from(bytes, string_end(bytes, at)?);
    (bytes.get(at) == Some(&b':')).then(|| whitespace_from(bytes, at + 1))
}

fn whitespace_from(bytes: &[u8], mut at: usize) -> usize {
    while matches!(bytes.get(at), Some(b' ' | b'\t' | b'\n' | b'\r')) {
        at += 1;
    }
    at
}

/// Past the string that opens at `at`; `None` when it is not a JSON string.
/// The text is UTF-8 already, so only the escapes and the control
/// characters are the grammar's to check.
fn string_end(bytes: &[u8], mut at: usize) -> Option<usize> {
    at += 1;
    loop {
        match *bytes.get(at)? {
            b'"' => return Some(at + 1),
            b'\\' => match *bytes.get(at + 1)? {
                b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => at += 2,
                b'u' => {
                    let hex = bytes.get(at + 2..at + 6)?;
                    if !hex.iter().all(u8::is_ascii_hexdigit) {
                        return None;
                    }
                    at += 6;
                }
                _ => return None,
            },
            byte if byte < 0x20 => return None,
            _ => at += 1,
        }
    }
}

/// Past the number that begins at `at`, and whether it is an integer
/// written plainly; `None` when it is not a JSON number.
fn number_end(bytes: &[u8], mut at: usize) -> Option<(usize, bool)> {
    let digits = |at: usize| -> Option<usize> {
        let end = at
            + bytes[at..]
                .iter()
                .take_while(|b| b.is_ascii_digit())
                .count();
        (end > at).then_some(end)
    };
    if bytes.get(at) == Some(&b'-') {
        at += 1;
    }
    at = match bytes.get(at)? {
        b'0' => at + 1,
        b'1'..=b'9' => digits(at)?,
        _ => return None,
    };
    let mut plain = true;
    if bytes.get(at) == Some(&b'.') {
        at = digits(at + 1)?;
        plain = false;
    }
    if matches!(bytes.get(at), Some(b'e' | b'E')) {
        at += 1;
        if matches!(bytes.get(at), Some(b'+' | b'-')) {
            at += 1;
        }
        at = digits(at)?;
        plain = false;
    }
    Some((at, plain))
}

/// The builder of a value from text the grammar accepted, checking D as it
/// goes. Its recursion is bounded by `max_depth`, checked before it
/// descends. A byte where the grammar promised another is a refusal, never
/// a panic.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
    max_depth: usize,
}

/// What the builder found where the grammar promised otherwise: a defect of
/// the library, refused rather than panicking in the intake.
const UNEXPECTED: Refused = Refused("the JSON ends where it should not");

impl Reader<'_> {
    fn peek(&self) -> Result<u8, Refused> {
        self.bytes.get(self.at).copied().ok_or(UNEXPECTED)
    }

    fn take(&mut self) -> Result<u8, Refused> {
        let byte = self.peek()?;
        self.at += 1;
        Ok(byte)
    }

    fn whitespace(&mut self) {
        self.at = whitespace_from(self.bytes, self.at);
    }

    fn value(&mut self, depth: usize) -> Result<Value, Refused> {
        match self.peek()? {
            b'{' | b'[' if depth >= self.max_depth => Err(Refused("the JSON nests too deep")),
            b'{' => self.object(depth + 1),
            b'[' => self.array(depth + 1),
            b'"' => self.string().map(Value::String),
            b't' => self.literal(4, Value::Bool(true)),
            b'f' => self.literal(5, Value::Bool(false)),
            b'n' => self.literal(4, Value::Null),
            _ => self.number(),
        }
    }

    fn literal(&mut self, length: usize, value: Value) -> Result<Value, Refused> {
        self.at += length;
        Ok(value)
    }

    fn object(&mut self, depth: usize) -> Result<Value, Refused> {
        let mut fields = Map::new();
        self.take()?;
        self.whitespace();
        if self.peek()? == b'}' {
            self.take()?;
            return Ok(Value::Object(fields));
        }
        loop {
            let key = self.string()?;
            if key.starts_with(PRIVATE_KEYS) {
                return Err(Refused("an object names a key of serde_json's own"));
            }
            self.whitespace();
            self.take()?; // the colon
            self.whitespace();
            let value = self.value(depth)?;
            if fields.insert(key, value).is_some() {
                return Err(Refused("an object names a key twice"));
            }
            self.whitespace();
            if self.take()? == b'}' {
                return Ok(Value::Object(fields));
            }
            self.whitespace();
        }
    }

    fn array(&mut self, depth: usize) -> Result<Value, Refused> {
        let mut items = Vec::new();
        self.take()?;
        self.whitespace();
        if self.peek()? == b']' {
            self.take()?;
            return Ok(Value::Array(items));
        }
        loop {
            items.push(self.value(depth)?);
            self.whitespace();
            if self.take()? == b']' {
                return Ok(Value::Array(items));
            }
            self.whitespace();
        }
    }

    fn string(&mut self) -> Result<String, Refused> {
        let mut text = String::new();
        self.take()?;
        loop {
            match self.take()? {
                b'"' => return Ok(text),
                b'\\' => {
                    let c = match self.take()? {
                        b'"' => '"',
                        b'\\' => '\\',
                        b'/' => '/',
                        b'b' => '\u{8}',
                        b'f' => '\u{c}',
                        b'n' => '\n',
                        b'r' => '\r',
                        b't' => '\t',
                        _ => self.unicode_escape()?,
                    };
                    if c == '\0' {
                        return Err(Refused("a JSON string holds U+0000"));
                    }
                    text.push(c);
                }
                _ => {
                    // A run of plain characters, UTF-8 as the text is: it
                    // ends at an ASCII quote or backslash, a boundary.
                    let start = self.at - 1;
                    let run = self.bytes[start..]
                        .iter()
                        .take_while(|b| !matches!(b, b'"' | b'\\'))
                        .count();
                    self.at = start + run;
                    let run =
                        std::str::from_utf8(&self.bytes[start..self.at]).map_err(|_| UNEXPECTED)?;
                    text.push_str(run);
                }
            }
        }
    }

    /// The character of a `\u` escape whose four digits are next, a
    /// surrogate pair joined; a lone surrogate is refused.
    fn unicode_escape(&mut self) -> Result<char, Refused> {
        let lone = Refused("a JSON string holds a lone surrogate");
        let first = self.hex4()?;
        match first {
            0xD800..=0xDBFF => {
                if self.bytes.get(self.at..self.at + 2) != Some(b"\\u") {
                    return Err(lone);
                }
                self.at += 2;
                let second = self.hex4()?;
                if !(0xDC00..=0xDFFF).contains(&second) {
                    return Err(lone);
                }
                char::from_u32(0x10000 + ((first - 0xD800) << 10) + (second - 0xDC00)).ok_or(lone)
            }
            0xDC00..=0xDFFF => Err(lone),
            code => char::from_u32(code).ok_or(lone),
        }
    }

    fn hex4(&mut self) -> Result<u32, Refused> {
        let digits = self.bytes.get(self.at..self.at + 4).ok_or(UNEXPECTED)?;
        self.at += 4;
        digits.iter().try_fold(0, |code, digit| {
            char::from(*digit)
                .to_digit(16)
                .map(|value| code * 16 + value)
                .ok_or(UNEXPECTED)
        })
    }

    fn number(&mut self) -> Result<Value, Refused> {
        let start = self.at;
        let (end, plain) = number_end(self.bytes, start).ok_or(UNEXPECTED)?;
        self.at = end;
        if !plain {
            return Err(Refused(
                "a JSON number is not an integer written without fraction or exponent",
            ));
        }
        let token = std::str::from_utf8(&self.bytes[start..end]).map_err(|_| UNEXPECTED)?;
        if token == "-0" {
            return Err(Refused("a JSON number is -0"));
        }
        let number = if token.starts_with('-') {
            token.parse::<i64>().map(Number::from)
        } else {
            token.parse::<u64>().map(Number::from)
        };
        number
            .map(Value::Number)
            .map_err(|_| Refused("a JSON number is past 64 bits"))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::*;

    #[test]
    fn the_grammar_takes_json_and_only_json() {
        for json in [
            "1",
            "-1",
            "0",
            " 1 ",
            "\"x\"",
            "true",
            "null",
            "[]",
            "{}",
            "[1,[2,{}]]",
            "{\"a\":{\"b\":[\"\\u00e9\"]}}",
            "1.5",
            "1e5",
            "-0",
            "\"\\ud800\"",
        ] {
            assert!(is_json(json), "{json}");
        }
        for not in [
            "",
            "o-1",
            "01",
            "1.",
            ".5",
            "1e",
            "+1",
            "[1,]",
            "{\"a\"}",
            "{\"a\":1,}",
            "[1",
            "\"x",
            "\"\\x\"",
            "\"\\u12\"",
            "tru",
            "nul",
            "1 2",
            "\"a\u{1}\"",
            "[1]]",
            "{1:2}",
        ] {
            assert!(!is_json(not), "{not}");
        }
    }

    #[test]
    fn nesting_past_the_bound_is_refused_and_any_depth_is_still_json() {
        let deep = |n: usize| format!("{}{}", "[".repeat(n), "]".repeat(n));
        assert!(
            read_json(&deep(STREAM_ID_DEPTH), STREAM_ID_DEPTH)
                .unwrap()
                .is_some()
        );
        assert!(read_json(&deep(STREAM_ID_DEPTH + 1), STREAM_ID_DEPTH).is_err());
        assert!(is_json(&deep(100_000)), "the grammar recurses on nothing");
    }
}
