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
//! A document is read by one grammar two ways (ADR-0047 decision 23,
//! K2b-7e-4b): into a tree by [`parse`] — a predicate's literal, a hole's
//! value —, or into a [`Tape`] by [`tape`] — an answer's body —, its values
//! by their places and none of its text copied. Whether a document is
//! readable is the grammar's alone, for both; a member named twice is found
//! by one check, at its object's close. A document longer than [`LONGEST`]
//! bytes is unreadable: a place in it is a `u32`.
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

use std::fmt;

/// The deepest an answer nests: arrays and objects inside one another.
pub const DEPTH: usize = 128;

/// The most digits a number's exponent has, its leading zeros dropped.
pub const EXPONENT_DIGITS: usize = 18;

/// A value's bytes in a parse's tree: every value is one (ADR-0047 decision
/// 22, K2b-7e-2b-2c-2).
pub const NODE: usize = size_of::<Json>();

/// The longest document read: a place in it is a `u32`.
pub const LONGEST: u64 = u32::MAX as u64;

/// The most bytes [`parse`] holds at once for `n` bytes of input, whatever
/// they are — valid, truncated or not JSON: two nodes a byte and two more,
/// and the names' check. The densest input is an array of one element, which
/// holds four slots, std's first growth, on its two bytes; any other value
/// holds less a byte (K2b-7e-2b-2c-2). Saturating: a bound past the address
/// space is no bound.
pub const fn bound(n: u64) -> u64 {
    TWO_NODES
        .saturating_mul(n.saturating_add(1))
        .saturating_add(names_bound(n))
}

const TWO_NODES: u64 = 2 * NODE as u64;

/// The most bytes the check of a member named twice holds for `n` bytes:
/// an entry a member — its decoded name's hash and its place —, at most one
/// member each five bytes (`"":0,`) and one more, sized at once at the first
/// object (K2b-7e-4b-1); none for a document with no object.
pub const fn names_bound(n: u64) -> u64 {
    NAME.saturating_mul((n / 5).saturating_add(1))
}

const NAME: u64 = size_of::<(u32, u32)>() as u64;

/// A value's bytes in a [`Tape`]: every value is one, a member's name one
/// more.
pub const ENTRY: usize = size_of::<Entry>();

/// The most bytes [`tape`] holds at once for `n` bytes of input, whatever
/// they are: its entries, at most one each two bytes and one more, sized at
/// once; and the names' check (K2b-7e-4b-1).
pub const fn tape_bound(n: u64) -> u64 {
    (ENTRY as u64)
        .saturating_mul((n / 2).saturating_add(1))
        .saturating_add(names_bound(n))
}

/// The most bytes a pointer of `n` bytes' reference tokens hold at once
/// ([`tokens`]): three strings a byte — a slot each, grown one at a time, held
/// twice as it moves —, a byte, and std's first four slots.
pub const fn pointer_bound(n: u64) -> u64 {
    TOKEN.saturating_mul(n).saturating_add(FIRST_SLOTS)
}

const STRING: u64 = size_of::<String>() as u64;
const TOKEN: u64 = 3 * STRING + 1;
const FIRST_SLOTS: u64 = 4 * STRING;

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
    /// Longer than [`LONGEST`] bytes.
    Long,
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
            JsonError::Long => write!(f, "longer than {LONGEST} bytes"),
        }
    }
}

impl std::error::Error for JsonError {}

/// A document read whole, into a tree.
pub fn parse(bytes: &[u8]) -> Result<Json, JsonError> {
    read(bytes, Tree).map(|(_, value, _)| value)
}

/// A document read whole, into its values' places (K2b-7e-4b-1): its
/// entries sized at once from its length, nothing else held after.
pub fn tape(bytes: &[u8]) -> Result<Tape<'_>, JsonError> {
    long(bytes)?;
    let values = (bytes.len() / 2).saturating_add(1);
    let recorder = Recorder {
        entries: Vec::with_capacity(values),
    };
    let (text, (), recorder) = read(bytes, recorder)?;
    Ok(Tape {
        text,
        entries: recorder.entries,
    })
}

fn long(bytes: &[u8]) -> Result<(), JsonError> {
    if u64::try_from(bytes.len()).is_ok_and(|n| n <= LONGEST) {
        Ok(())
    } else {
        Err(JsonError::Long)
    }
}

/// The grammar, its values to `sink`.
fn read<S: Sink>(bytes: &[u8], sink: S) -> Result<(&str, S::Value, S), JsonError> {
    long(bytes)?;
    let text = std::str::from_utf8(bytes).map_err(|_| JsonError::Utf8)?;
    let mut p = Parser {
        text,
        at: 0,
        names: Vec::new(),
        sink,
    };
    p.space();
    let value = p.value(0)?;
    p.space();
    if p.at != text.len() {
        return Err(JsonError::Syntax { at: p.at });
    }
    Ok((text, value, p.sink))
}

/// `null`, `true` or `false`.
#[derive(Clone, Copy)]
enum Word {
    Null,
    True,
    False,
}

/// What the grammar's values become: each told at its start, a container's
/// members as they end, the container at its end.
trait Sink {
    type Value;
    type Items;
    type Members;
    type Name;
    /// A string's or a name's text, as it is decoded.
    type Text: Feed;
    fn word(&mut self, at: usize, word: Word) -> Self::Value;
    fn number(&mut self, at: usize, number: &Lexed<'_>) -> Self::Value;
    fn text(&mut self, at: usize) -> Self::Text;
    fn string(&mut self, text: Self::Text) -> Self::Value;
    fn name(&mut self, at: usize) -> Self::Text;
    fn named(&mut self, text: Self::Text) -> Self::Name;
    fn array(&mut self, at: usize) -> Self::Items;
    fn item(&mut self, items: &mut Self::Items, value: Self::Value);
    fn array_end(&mut self, items: Self::Items) -> Self::Value;
    fn object(&mut self, at: usize) -> Self::Members;
    fn member(&mut self, members: &mut Self::Members, name: Self::Name, value: Self::Value);
    fn object_end(&mut self, members: Self::Members) -> Self::Value;
}

/// The tree: today's [`Json`].
struct Tree;

impl Sink for Tree {
    type Value = Json;
    type Items = Vec<Json>;
    type Members = Vec<(String, Json)>;
    type Name = String;
    type Text = String;

    fn word(&mut self, _: usize, word: Word) -> Json {
        match word {
            Word::Null => Json::Null,
            Word::True => Json::Bool(true),
            Word::False => Json::Bool(false),
        }
    }

    fn number(&mut self, _: usize, number: &Lexed<'_>) -> Json {
        Json::Number(number.number())
    }

    fn text(&mut self, _: usize) -> String {
        String::new()
    }

    fn string(&mut self, text: String) -> Json {
        Json::String(text)
    }

    fn name(&mut self, _: usize) -> String {
        String::new()
    }

    fn named(&mut self, text: String) -> String {
        text
    }

    fn array(&mut self, _: usize) -> Vec<Json> {
        Vec::new()
    }

    fn item(&mut self, items: &mut Vec<Json>, value: Json) {
        items.push(value);
    }

    fn array_end(&mut self, items: Vec<Json>) -> Json {
        Json::Array(items)
    }

    fn object(&mut self, _: usize) -> Vec<(String, Json)> {
        Vec::new()
    }

    fn member(&mut self, members: &mut Vec<(String, Json)>, name: String, value: Json) {
        members.push((name, value));
    }

    fn object_end(&mut self, members: Vec<(String, Json)>) -> Json {
        Json::Object(members)
    }
}

/// The tape's entries, each pushed at its value's start.
struct Recorder {
    entries: Vec<Entry>,
}

impl Recorder {
    fn push(&mut self, at: usize) -> usize {
        let i = self.entries.len();
        self.entries.push(Entry {
            start: offset(at),
            next: offset(i.saturating_add(1)),
        });
        i
    }

    fn close(&mut self, i: usize) {
        let next = offset(self.entries.len());
        if let Some(e) = self.entries.get_mut(i) {
            e.next = next;
        }
    }
}

/// A place within [`LONGEST`], as the tape keeps it.
fn offset(n: usize) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}

/// A place the tape keeps, as an index.
fn place(n: u32) -> usize {
    usize::try_from(n).unwrap_or(usize::MAX)
}

impl Sink for Recorder {
    type Value = ();
    type Items = usize;
    type Members = usize;
    type Name = ();
    type Text = ();

    fn word(&mut self, at: usize, _: Word) {
        self.push(at);
    }

    fn number(&mut self, at: usize, _: &Lexed<'_>) {
        self.push(at);
    }

    fn text(&mut self, at: usize) {
        self.push(at);
    }

    fn string(&mut self, (): ()) {}

    fn name(&mut self, at: usize) {
        self.push(at);
    }

    fn named(&mut self, (): ()) {}

    fn array(&mut self, at: usize) -> usize {
        self.push(at)
    }

    fn item(&mut self, _: &mut usize, (): ()) {}

    fn array_end(&mut self, i: usize) {
        self.close(i);
    }

    fn object(&mut self, at: usize) -> usize {
        self.push(at)
    }

    fn member(&mut self, _: &mut usize, (): (), (): ()) {}

    fn object_end(&mut self, i: usize) {
        self.close(i);
    }
}

/// A piece of a string's decoded text: a run as written, or an escape's
/// character.
enum Piece<'a> {
    Run(&'a str),
    Char(char),
}

impl<'a> Piece<'a> {
    fn bytes(&self) -> PieceBytes<'a> {
        match self {
            Piece::Run(s) => PieceBytes::Run(s.bytes()),
            Piece::Char(c) => {
                let mut buf = [0; 4];
                let n = c.encode_utf8(&mut buf).len();
                PieceBytes::Char(buf, 0, n)
            }
        }
    }
}

/// A piece's UTF-8.
enum PieceBytes<'a> {
    Run(std::str::Bytes<'a>),
    Char([u8; 4], usize, usize),
}

impl Iterator for PieceBytes<'_> {
    type Item = u8;

    fn next(&mut self) -> Option<u8> {
        match self {
            PieceBytes::Run(b) => b.next(),
            PieceBytes::Char(buf, at, n) => {
                if at < n {
                    let b = buf.get(*at).copied();
                    *at = at.saturating_add(1);
                    b
                } else {
                    None
                }
            }
        }
    }
}

/// Where a string's decoded text goes.
trait Feed {
    fn feed(&mut self, piece: &Piece<'_>);
}

impl Feed for String {
    fn feed(&mut self, piece: &Piece<'_>) {
        match piece {
            Piece::Run(s) => self.push_str(s),
            Piece::Char(c) => self.push(*c),
        }
    }
}

impl Feed for () {
    fn feed(&mut self, _: &Piece<'_>) {}
}

/// FNV-1a over a name's decoded bytes.
struct Fnv(u32);

impl Fnv {
    const NEW: Fnv = Fnv(0x811c_9dc5);
}

impl Feed for Fnv {
    fn feed(&mut self, piece: &Piece<'_>) {
        for b in piece.bytes() {
            self.0 = (self.0 ^ u32::from(b)).wrapping_mul(0x0100_0193);
        }
    }
}

impl<A: Feed, B: Feed> Feed for (&mut A, &mut B) {
    fn feed(&mut self, piece: &Piece<'_>) {
        self.0.feed(piece);
        self.1.feed(piece);
    }
}

/// A string's text decoded piece by piece, from just past its opening quote;
/// `at` just past its closing quote once it is done.
struct Pieces<'a> {
    text: &'a str,
    at: usize,
    done: bool,
}

impl<'a> Iterator for Pieces<'a> {
    type Item = Result<Piece<'a>, JsonError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let bytes = self.text.as_bytes();
        let start = self.at;
        while matches!(bytes.get(self.at), Some(&b) if b != b'"' && b != b'\\' && b >= 0x20) {
            self.at = self.at.saturating_add(1);
        }
        if self.at > start {
            // Every stop is at an ASCII byte, so the run is whole characters.
            let run = self.text.get(start..self.at).map(Piece::Run);
            return Some(run.ok_or(JsonError::Syntax { at: start }));
        }
        let piece = match bytes.get(self.at) {
            Some(b'"') => {
                self.at = self.at.saturating_add(1);
                self.done = true;
                return None;
            }
            Some(b'\\') => self.escape().map(Piece::Char),
            // A control character unescaped, or the end.
            _ => Err(JsonError::Syntax { at: self.at }),
        };
        if piece.is_err() {
            self.done = true;
        }
        Some(piece)
    }
}

impl Pieces<'_> {
    fn escape(&mut self) -> Result<char, JsonError> {
        let escape = self.at;
        self.at = self.at.saturating_add(1);
        let c = match self.text.as_bytes().get(self.at) {
            Some(b'"') => '"',
            Some(b'\\') => '\\',
            Some(b'/') => '/',
            Some(b'b') => '\u{8}',
            Some(b'f') => '\u{c}',
            Some(b'n') => '\n',
            Some(b'r') => '\r',
            Some(b't') => '\t',
            Some(b'u') => {
                self.at = self.at.saturating_add(1);
                return self.unicode(escape);
            }
            _ => return Err(JsonError::Syntax { at: self.at }),
        };
        self.at = self.at.saturating_add(1);
        Ok(c)
    }

    /// Four hex digits after `\u`.
    fn hex4(&mut self) -> Result<u32, JsonError> {
        let syntax = JsonError::Syntax { at: self.at };
        let end = self.at.saturating_add(4);
        let digits = self.text.get(self.at..end).ok_or_else(|| syntax.clone())?;
        if !digits.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(syntax);
        }
        let v = u32::from_str_radix(digits, 16).map_err(|_| syntax)?;
        self.at = end;
        Ok(v)
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
}

/// A string's text decoded into `feed`, from just past its opening quote;
/// the place just past its closing quote.
fn decode(text: &str, from: usize, feed: &mut impl Feed) -> Result<usize, JsonError> {
    let mut pieces = Pieces {
        text,
        at: from,
        done: false,
    };
    for piece in pieces.by_ref() {
        feed.feed(&piece?);
    }
    Ok(pieces.at)
}

/// The decoded bytes of the string whose opening quote is at `quote`, in a
/// text the grammar read: an escape it refused ends them.
fn decoded(text: &str, quote: usize) -> impl Iterator<Item = u8> + '_ {
    Pieces {
        text,
        at: quote.saturating_add(1),
        done: false,
    }
    .map_while(Result::ok)
    .flat_map(|p| p.bytes())
}

/// A number lexed: its text, and its decimal value's parts over that text.
struct Lexed<'a> {
    text: &'a str,
    negative: bool,
    int: &'a str,
    frac: &'a str,
    /// The leading zeros of `int frac`.
    lead: usize,
    /// Its significant digits' count: `int frac` less its leading zeros, then
    /// its trailing ones.
    len: usize,
    /// The value's exponent over its significant digits.
    exponent: i64,
}

impl Lexed<'_> {
    fn digits(&self) -> impl Iterator<Item = u8> + '_ {
        self.int
            .bytes()
            .chain(self.frac.bytes())
            .skip(self.lead)
            .take(self.len)
    }

    fn number(&self) -> Number {
        let zero = self.len == 0;
        Number {
            text: self.text.to_owned(),
            negative: self.negative && !zero,
            digits: self.digits().map(char::from).collect(),
            exponent: if zero { 0 } else { self.exponent },
        }
    }
}

/// The number at `start`, and the place past it.
fn lex(text: &str, start: usize) -> Result<(Lexed<'_>, usize), JsonError> {
    let b = text.as_bytes();
    let digits = |at: &mut usize| {
        let from = *at;
        while matches!(b.get(*at), Some(b'0'..=b'9')) {
            *at = at.saturating_add(1);
        }
        at.saturating_sub(from)
    };
    let mut at = start;
    let negative = b.get(at) == Some(&b'-');
    if negative {
        at = at.saturating_add(1);
    }
    let int_start = at;
    match b.get(at) {
        Some(b'0') => at = at.saturating_add(1),
        Some(b'1'..=b'9') => {
            digits(&mut at);
        }
        _ => return Err(JsonError::Syntax { at }),
    }
    let int = text.get(int_start..at).unwrap_or_default();
    let mut frac = "";
    if b.get(at) == Some(&b'.') {
        at = at.saturating_add(1);
        let f = at;
        if digits(&mut at) == 0 {
            return Err(JsonError::Syntax { at });
        }
        frac = text.get(f..at).unwrap_or_default();
    }
    let mut exponent: i64 = 0;
    if matches!(b.get(at), Some(b'e' | b'E')) {
        at = at.saturating_add(1);
        let minus = match b.get(at) {
            Some(b'-') => {
                at = at.saturating_add(1);
                true
            }
            Some(b'+') => {
                at = at.saturating_add(1);
                false
            }
            _ => false,
        };
        let e = at;
        if digits(&mut at) == 0 {
            return Err(JsonError::Syntax { at });
        }
        let e = text.get(e..at).unwrap_or_default().trim_start_matches('0');
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
    // The value: int.frac · 10^exponent = (int frac) · 10^(exponent − |frac|),
    // its digits stripped of leading zeros, then of trailing ones.
    let all = int.len().saturating_add(frac.len());
    let lead = int
        .bytes()
        .chain(frac.bytes())
        .take_while(|&d| d == b'0')
        .count();
    let trailing = if lead == all {
        0
    } else {
        int.bytes()
            .chain(frac.bytes())
            .rev()
            .take_while(|&d| d == b'0')
            .count()
    };
    let overflow = JsonError::Exponent { at: start };
    let exponent = i64::try_from(frac.len())
        .ok()
        .and_then(|f| exponent.checked_sub(f))
        .and_then(|e| e.checked_add(i64::try_from(trailing).ok()?))
        .ok_or(overflow)?;
    let lexed = Lexed {
        text: text.get(start..at).unwrap_or_default(),
        negative,
        int,
        frac,
        lead,
        len: all.saturating_sub(lead).saturating_sub(trailing),
        exponent,
    };
    Ok((lexed, at))
}

struct Parser<'a, S> {
    text: &'a str,
    at: usize,
    /// The open objects' names, `(hash, place)`, each object's above its
    /// parent's: sized once, at the first object.
    names: Vec<(u32, u32)>,
    sink: S,
}

impl<S: Sink> Parser<'_, S> {
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

    fn word(&mut self, text: &str, word: Word) -> Result<S::Value, JsonError> {
        let at = self.at;
        let end = at.saturating_add(text.len());
        if self.text.get(at..end) == Some(text) {
            self.at = end;
            Ok(self.sink.word(at, word))
        } else {
            Err(self.syntax())
        }
    }

    fn value(&mut self, depth: usize) -> Result<S::Value, JsonError> {
        match self.peek() {
            Some(b'{') => self.object(depth),
            Some(b'[') => self.array(depth),
            Some(b'"') => self.string(),
            Some(b't') => self.word("true", Word::True),
            Some(b'f') => self.word("false", Word::False),
            Some(b'n') => self.word("null", Word::Null),
            Some(b'-' | b'0'..=b'9') => self.number(),
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

    fn array(&mut self, depth: usize) -> Result<S::Value, JsonError> {
        let depth = Self::deeper(depth)?;
        let open = self.at;
        self.eat(b'[')?;
        let mut items = self.sink.array(open);
        self.space();
        if self.peek() == Some(b']') {
            self.bump();
            return Ok(self.sink.array_end(items));
        }
        loop {
            self.space();
            let value = self.value(depth)?;
            self.sink.item(&mut items, value);
            self.space();
            match self.peek() {
                Some(b',') => self.bump(),
                Some(b']') => {
                    self.bump();
                    return Ok(self.sink.array_end(items));
                }
                _ => return Err(self.syntax()),
            }
        }
    }

    fn object(&mut self, depth: usize) -> Result<S::Value, JsonError> {
        let depth = Self::deeper(depth)?;
        let open = self.at;
        self.eat(b'{')?;
        if self.names.capacity() == 0 {
            // At most a member each five bytes of what is left, and one more.
            let left = self.text.len().saturating_sub(open);
            self.names.reserve_exact((left / 5).saturating_add(1));
        }
        let base = self.names.len();
        let mut members = self.sink.object(open);
        self.space();
        if self.peek() == Some(b'}') {
            self.bump();
            return Ok(self.sink.object_end(members));
        }
        loop {
            self.space();
            let at = self.at;
            if self.peek() != Some(b'"') {
                return Err(self.syntax());
            }
            let (name, hash) = self.name()?;
            self.space();
            self.eat(b':')?;
            self.space();
            let value = self.value(depth)?;
            self.names.push((hash, offset(at)));
            self.sink.member(&mut members, name, value);
            self.space();
            match self.peek() {
                Some(b',') => self.bump(),
                Some(b'}') => {
                    self.bump();
                    self.once(base)?;
                    self.names.truncate(base);
                    return Ok(self.sink.object_end(members));
                }
                _ => return Err(self.syntax()),
            }
        }
    }

    /// No name of the object whose names are those from `base` twice: sorted
    /// by hash, and only names of one hash compared — decoded, sorted by
    /// their bytes.
    fn once(&mut self, base: usize) -> Result<(), JsonError> {
        let text = self.text;
        let Some(mut rest) = self.names.get_mut(base..) else {
            return Ok(());
        };
        rest.sort_unstable();
        while let Some(&(hash, _)) = rest.first() {
            let run = rest.iter().take_while(|(h, _)| *h == hash).count();
            let Some((same, after)) = rest.split_at_mut_checked(run) else {
                return Ok(());
            };
            if same.len() > 1 {
                same.sort_unstable_by(|a, b| {
                    decoded(text, place(a.1)).cmp(decoded(text, place(b.1)))
                });
                for pair in same.windows(2) {
                    if let [a, b] = pair
                        && decoded(text, place(a.1)).eq(decoded(text, place(b.1)))
                    {
                        return Err(JsonError::Duplicate {
                            at: place(a.1.max(b.1)),
                        });
                    }
                }
            }
            rest = after;
        }
        Ok(())
    }

    fn string(&mut self) -> Result<S::Value, JsonError> {
        let at = self.at;
        self.eat(b'"')?;
        let mut text = self.sink.text(at);
        self.at = decode(self.text, self.at, &mut text)?;
        Ok(self.sink.string(text))
    }

    /// A member's name, and its decoded bytes' hash.
    fn name(&mut self) -> Result<(S::Name, u32), JsonError> {
        let at = self.at;
        self.eat(b'"')?;
        let mut text = self.sink.name(at);
        let mut hash = Fnv::NEW;
        self.at = decode(self.text, self.at, &mut (&mut text, &mut hash))?;
        Ok((self.sink.named(text), hash.0))
    }

    fn number(&mut self) -> Result<S::Value, JsonError> {
        let at = self.at;
        let (number, end) = lex(self.text, at)?;
        self.at = end;
        Ok(self.sink.number(at, &number))
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

/// A document read into its values' places (ADR-0047 decision 23,
/// K2b-7e-4b-1): an entry a value — a member's name one, just before its
/// value — in the order written, each with its start and the index past its
/// last descendant. Its `Debug` shows only its size: an answer may carry a
/// secret a step captures.
pub struct Tape<'a> {
    text: &'a str,
    entries: Vec<Entry>,
}

/// A value's place: its start, and the index past its last descendant.
#[derive(Clone, Copy)]
struct Entry {
    start: u32,
    next: u32,
}

impl fmt::Debug for Tape<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a tape of {} values", self.entries.len())
    }
}

impl Tape<'_> {
    /// The document's value.
    pub fn root(&self) -> Node<'_> {
        Node {
            text: self.text,
            entries: &self.entries,
            index: 0,
        }
    }
}

/// What a value is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// `null`.
    Null,
    /// `true` or `false`.
    Bool,
    /// A number.
    Number,
    /// A string.
    String,
    /// An array.
    Array,
    /// An object.
    Object,
}

/// A value a tape's is compared against: a tree, its objects' members
/// sorted by name, once (K2b-7e-4b-2). Its `Debug` shows only its shape.
pub struct Sorted(Json);

impl Sorted {
    /// `want`, its objects' members sorted in place.
    pub fn new(mut want: Json) -> Sorted {
        sort(&mut want);
        Sorted(want)
    }
}

impl fmt::Debug for Sorted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

fn sort(v: &mut Json) {
    match v {
        Json::Array(items) => items.iter_mut().for_each(sort),
        Json::Object(members) => {
            members.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            members.iter_mut().for_each(|(_, v)| sort(v));
        }
        Json::Null | Json::Bool(_) | Json::Number(_) | Json::String(_) => {}
    }
}

/// Whether a number lexed is `n` by value: `Number`'s equality over its
/// text, nothing allocated.
fn same(n: &Number, m: &Lexed<'_>) -> bool {
    let zero = m.len == 0;
    n.negative == (m.negative && !zero)
        && n.exponent == if zero { 0 } else { m.exponent }
        && n.digits.bytes().eq(m.digits())
}

/// A value in a [`Tape`]. Its `Debug` shows only its kind.
#[derive(Clone, Copy)]
pub struct Node<'t> {
    text: &'t str,
    entries: &'t [Entry],
    index: usize,
}

impl fmt::Debug for Node<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a node: {:?}", self.kind())
    }
}

impl<'t> Node<'t> {
    fn at_index(&self, index: usize) -> Node<'t> {
        Node { index, ..*self }
    }

    fn start(&self) -> usize {
        self.entries
            .get(self.index)
            .map_or(self.text.len(), |e| place(e.start))
    }

    /// The index past its last descendant.
    fn next(&self) -> usize {
        self.entries
            .get(self.index)
            .map_or(self.entries.len(), |e| place(e.next))
    }

    fn first(&self) -> Option<u8> {
        self.text.as_bytes().get(self.start()).copied()
    }

    /// What it is.
    pub fn kind(&self) -> Kind {
        match self.first() {
            Some(b'{') => Kind::Object,
            Some(b'[') => Kind::Array,
            Some(b'"') => Kind::String,
            Some(b't' | b'f') => Kind::Bool,
            Some(b'n') => Kind::Null,
            _ => Kind::Number,
        }
    }

    /// Its value, if it is `true` or `false`.
    pub fn bool(&self) -> Option<bool> {
        match self.first() {
            Some(b't') => Some(true),
            Some(b'f') => Some(false),
            _ => None,
        }
    }

    /// Its text as written, if it is a number.
    pub fn number(&self) -> Option<&'t str> {
        if self.kind() != Kind::Number {
            return None;
        }
        lex(self.text, self.start()).ok().map(|(n, _)| n.text)
    }

    /// Its text decoded, if it is a string: sized at once, its decoded
    /// length counted first (K2b-7e-4b-2).
    pub fn string(&self) -> Option<String> {
        self.string_within(usize::MAX)
    }

    /// Its text decoded, if it is a string of at most `most` bytes decoded;
    /// none past them, nothing held for them.
    pub fn string_within(&self, most: usize) -> Option<String> {
        if self.kind() != Kind::String {
            return None;
        }
        let len = decoded(self.text, self.start())
            .take(most.saturating_add(1))
            .count();
        if len > most {
            return None;
        }
        let mut s = String::with_capacity(len);
        decode(self.text, self.start().saturating_add(1), &mut s).ok()?;
        Some(s)
    }

    /// Whether it equals `want` as [`Json`]'s own equality has it: numbers
    /// by their decimal values, strings decoded, arrays in order, objects
    /// member by member in any order — each of its members found in
    /// `want`'s, sorted, by its decoded name (K2b-7e-4b-2).
    pub fn equals(&self, want: &Sorted) -> bool {
        self.equal(&want.0)
    }

    /// Whether it is an array holding an element equal to `want`.
    pub fn contains(&self, want: &Sorted) -> bool {
        self.kind() == Kind::Array && self.items().any(|x| x.equal(&want.0))
    }

    fn equal(&self, want: &Json) -> bool {
        match want {
            Json::Null => self.kind() == Kind::Null,
            Json::Bool(b) => self.bool() == Some(*b),
            Json::Number(n) => {
                self.kind() == Kind::Number
                    && lex(self.text, self.start()).is_ok_and(|(m, _)| same(n, &m))
            }
            Json::String(s) => self.is(s),
            Json::Array(a) => {
                self.kind() == Kind::Array
                    && self.items().count() == a.len()
                    && self.items().zip(a).all(|(x, w)| x.equal(w))
            }
            Json::Object(o) => {
                // Each name once on either side (the grammar refuses a
                // second): a member each found, and as many, is all of them.
                self.kind() == Kind::Object
                    && self.members().count() == o.len()
                    && self.members().all(|(k, v)| {
                        o.binary_search_by(|(name, _)| {
                            name.bytes().cmp(decoded(self.text, k.start()))
                        })
                        .ok()
                        .and_then(|i| o.get(i))
                        .is_some_and(|(_, w)| v.equal(w))
                    })
            }
        }
    }

    /// Whether it is a string whose text decoded is `s`.
    pub fn is(&self, s: &str) -> bool {
        self.kind() == Kind::String && decoded(self.text, self.start()).eq(s.bytes())
    }

    /// Its elements, if it is an array; none otherwise.
    pub fn items(&self) -> Items<'t> {
        let (at, end) = if self.kind() == Kind::Array {
            (self.index.saturating_add(1), self.next())
        } else {
            (0, 0)
        };
        Items {
            node: *self,
            at,
            end,
        }
    }

    /// Its members, each its name and its value, if it is an object; none
    /// otherwise.
    pub fn members(&self) -> Members<'t> {
        let (at, end) = if self.kind() == Kind::Object {
            (self.index.saturating_add(1), self.next())
        } else {
            (0, 0)
        };
        Members {
            node: *self,
            at,
            end,
        }
    }

    /// The value `pointer` names, if the document has one there: as
    /// [`Json::at`].
    pub fn at(&self, pointer: &str) -> Result<Option<Node<'t>>, Malformed> {
        let mut here = *self;
        for token in tokens(pointer)? {
            let next = match here.kind() {
                Kind::Object => here.members().find(|(k, _)| k.is(&token)).map(|(_, v)| v),
                Kind::Array => index(&token).and_then(|i| here.items().nth(i)),
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

/// An array's elements in a [`Tape`].
pub struct Items<'t> {
    node: Node<'t>,
    at: usize,
    end: usize,
}

impl<'t> Iterator for Items<'t> {
    type Item = Node<'t>;

    fn next(&mut self) -> Option<Node<'t>> {
        if self.at >= self.end {
            return None;
        }
        let item = self.node.at_index(self.at);
        self.at = item.next();
        Some(item)
    }
}

/// An object's members in a [`Tape`], each its name and its value.
pub struct Members<'t> {
    node: Node<'t>,
    at: usize,
    end: usize,
}

impl<'t> Iterator for Members<'t> {
    type Item = (Node<'t>, Node<'t>);

    fn next(&mut self) -> Option<(Node<'t>, Node<'t>)> {
        if self.at >= self.end {
            return None;
        }
        let name = self.node.at_index(self.at);
        let value = self.node.at_index(self.at.saturating_add(1));
        self.at = value.next();
        Some((name, value))
    }
}
