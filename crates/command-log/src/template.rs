//! Templates: a payload's bytes with the spans its holes fill (ADR-0046
//! decision 3).
//!
//! A host never writes a span. It asks the template for a sentinel per hole,
//! puts the sentinel where the value goes in its typed message — a DTO field, a
//! prost field — serialises the message as it always does, and hands the bytes
//! to [`Template::finish`], which finds each sentinel exactly once and records
//! its span and field. Sentinels never reach the log: every one is a span.
//!
//! ```
//! use ascetic_ddd_command_log::record::{Codec, Hole, Of, Rendering, Subject};
//! use ascetic_ddd_command_log::template::Template;
//!
//! let device = Subject { kind: "device".into(), scope: vec![1], index: 7 };
//! let mut template = Template::new(Codec::Json);
//! let id = template
//!     .text("/id", Hole::Rendered { of: Of::Subject(device), role: "id".into(), rendering: Rendering::UuidText })
//!     .unwrap();
//! // The host's DTO, serialised as it always is.
//! let body = format!(r#"{{"id":"{id}","name":"probe"}}"#).into_bytes();
//! let payload = template.finish(body).unwrap();
//! assert_eq!((payload.holes[0].offset, payload.holes[0].len), (6, 38));
//! ```

use std::fmt;

use crate::record::{Codec, Form, Hole, Payload, Rendering, Span, TimeRendering};

/// Why a template refused a hole or its bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TemplateError {
    /// A sentinel was found other than exactly once, as a whole token where it
    /// is one.
    Sentinel {
        /// The hole's field.
        field: String,
        /// How many times it was found.
        found: usize,
    },
    /// The hole cannot be written in this form: key material or a UUID's bytes
    /// as a JSON token, a text as a JSON number.
    Form {
        /// The hole's field.
        field: String,
    },
    /// A JSON token in a payload that is not JSON, or raw bytes in one that is.
    Codec {
        /// The hole's field.
        field: String,
    },
    /// A raw span whose length is not its rendering's.
    Length {
        /// The hole's field.
        field: String,
        /// The rendering's length.
        expected: usize,
    },
    /// Two spans overlap, or a payload is past 4 GiB.
    Layout,
}

impl fmt::Display for TemplateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TemplateError::Sentinel { field, found } => {
                write!(f, "{field}: its sentinel found {found} times, not once")
            }
            TemplateError::Form { field } => {
                write!(f, "{field}: the hole cannot be written in this form")
            }
            TemplateError::Codec { field } => {
                write!(f, "{field}: the form is not the payload's codec's")
            }
            TemplateError::Length { field, expected } => {
                write!(f, "{field}: a raw span of its rendering's {expected} bytes")
            }
            TemplateError::Layout => f.write_str("spans overlap, or a payload past 4 GiB"),
        }
    }
}

impl std::error::Error for TemplateError {}

/// How a sentinel is found: a JSON string with its quotes, a JSON number
/// between delimiters, or bytes as they are.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Token {
    String,
    Number,
    Raw,
}

struct Pending {
    sentinel: Vec<u8>,
    token: Token,
    field: String,
    hole: Hole,
}

/// A payload being built: the sentinels handed out, then the bytes.
pub struct Template {
    codec: Codec,
    pending: Vec<Pending>,
}

// A sentinel is a valid value of the field it stands in — a version 4 UUID, an
// RFC 3339 time in 2999, hex digits, ASCII bytes — so a host's typed message
// takes it, and it carries a tag a host's own content would have to spell: if
// it does, `finish` finds a sentinel twice and refuses instead of guessing.

/// The first group of a UUID sentinel, the first digits of a hex one.
const TAG_HEX: &str = "5e471e1a";
/// The first bytes of a raw one: ASCII, so a protobuf `string` takes it.
const TAG_RAW: [u8; 4] = [0x5e, 0x47, 0x1e, 0x1a];
/// The first digits of a numeric one.
const TAG_NUMBER: i64 = 5_471_000_000_000_000_000;
/// 2999-01-01T00:00:00Z, in microseconds: time sentinels count from it.
const TIME_BASE: i64 = 32_472_144_000_000_000;

/// The `n`th UUID sentinel.
fn uuid_text(n: u64) -> String {
    format!("{TAG_HEX}-0000-4000-8000-{:012x}", n & 0xffff_ffff_ffff)
}

/// The `n`th time sentinel: `n` seconds after the base, in whole seconds — as
/// a typed time serialises a whole second — or `n` microseconds after it, with
/// six decimals, for a raw span of 27 bytes.
fn time_text(n: u64, micros: bool) -> String {
    let n = i64::try_from(n).unwrap_or(0);
    let t = if micros {
        TIME_BASE.saturating_add(n)
    } else {
        TIME_BASE.saturating_add(n.saturating_mul(1_000_000))
    };
    let full = crate::render::rfc3339(t).unwrap_or_default();
    if micros {
        full
    } else {
        format!("{}Z", full.get(..19).unwrap_or_default())
    }
}

/// The `n`th hex sentinel of `len` digits: the tag cut to what the number leaves.
fn hex_text(n: u64, len: usize) -> Vec<u8> {
    let digits = format!("{n:x}");
    let room = len.saturating_sub(digits.len());
    let tag = TAG_HEX.bytes().cycle().take(room);
    tag.chain(digits.bytes()).take(len).collect()
}

/// The `n`th raw sentinel of `len` bytes: the tag, the number seven bits a byte,
/// then `~`: ASCII throughout.
fn raw_bytes(n: u64, len: usize) -> Vec<u8> {
    let groups = (0..9u32).map(|i| (n.checked_shr(i.saturating_mul(7)).unwrap_or(0) & 0x7f) as u8);
    TAG_RAW
        .into_iter()
        .chain(groups)
        .chain(std::iter::repeat(b'~'))
        .take(len)
        .collect()
}

impl Template {
    /// A template for a payload of `codec`.
    pub fn new(codec: Codec) -> Template {
        Template {
            codec,
            pending: Vec::new(),
        }
    }

    fn number(&self) -> u64 {
        self.pending.len() as u64
    }

    fn push(&mut self, sentinel: Vec<u8>, token: Token, field: &str, hole: Hole) {
        self.pending.push(Pending {
            sentinel,
            token,
            field: field.to_owned(),
            hole,
        });
    }

    /// A JSON string's sentinel for `hole`, at the JSON pointer `field`: a
    /// version 4 UUID, so a field typed `Uuid` takes it, or for a time an RFC
    /// 3339 time in whole seconds, so a field typed as a time takes it and
    /// writes it back as it was. The span is the whole token, its quotes
    /// included; the value spliced there is the hole's, in its own rendering.
    pub fn text(&mut self, field: &str, hole: Hole) -> Result<String, TemplateError> {
        let refuse = || TemplateError::Form {
            field: field.to_owned(),
        };
        if self.codec != Codec::Json {
            return Err(TemplateError::Codec {
                field: field.to_owned(),
            });
        }
        match &hole {
            Hole::Rendered { rendering, .. } => match rendering {
                Rendering::UuidText | Rendering::Hex { .. } => {}
                Rendering::UuidBytes | Rendering::Secret32 => return Err(refuse()),
            },
            Hole::Time { rendering, .. } => {
                if *rendering != TimeRendering::Rfc3339Micros {
                    return Err(refuse());
                }
            }
            Hole::Captured { .. } => {}
        }
        let n = self.number();
        let sentinel = match &hole {
            Hole::Time { .. } => time_text(n, false),
            Hole::Rendered { .. } | Hole::Captured { .. } => uuid_text(n),
        };
        self.push(
            format!("\"{sentinel}\"").into_bytes(),
            Token::String,
            field,
            hole,
        );
        Ok(sentinel)
    }

    /// A JSON number's sentinel for `hole`, at the JSON pointer `field`: a time
    /// in epoch milliseconds, or a captured value. The span is the whole token.
    pub fn integer(&mut self, field: &str, hole: Hole) -> Result<i64, TemplateError> {
        if self.codec != Codec::Json {
            return Err(TemplateError::Codec {
                field: field.to_owned(),
            });
        }
        match &hole {
            Hole::Time {
                rendering: TimeRendering::EpochMillis,
                ..
            }
            | Hole::Captured { .. } => {}
            _ => {
                return Err(TemplateError::Form {
                    field: field.to_owned(),
                });
            }
        }
        let n = i64::try_from(self.number()).unwrap_or(i64::MAX);
        let sentinel = TAG_NUMBER.saturating_add(n);
        self.push(
            sentinel.to_string().into_bytes(),
            Token::Number,
            field,
            hole,
        );
        Ok(sentinel)
    }

    /// A raw span's sentinel for `hole`: `len` bytes at the field path `field`
    /// of a protobuf message or a text, valid UTF-8 always, so a `string` field
    /// takes it. A rendering of a known length must have that length: 16 for a
    /// UUID's bytes, 36 for its text, 32 for key material, `2·len` for hex, 8 for
    /// a time in epoch milliseconds (fixed64), 27 for RFC 3339 with
    /// microseconds.
    pub fn raw(&mut self, field: &str, hole: Hole, len: usize) -> Result<Vec<u8>, TemplateError> {
        if self.codec == Codec::Json {
            return Err(TemplateError::Codec {
                field: field.to_owned(),
            });
        }
        let expected = match &hole {
            Hole::Rendered { rendering, .. } => Some(match rendering {
                Rendering::UuidText => 36,
                Rendering::UuidBytes => 16,
                Rendering::Hex { len } => usize::from(*len).saturating_mul(2),
                Rendering::Secret32 => 32,
            }),
            Hole::Time { rendering, .. } => Some(match rendering {
                TimeRendering::EpochMillis => 8,
                TimeRendering::Rfc3339Micros => 27,
            }),
            Hole::Captured { .. } => None,
        };
        if expected.is_some_and(|e| e != len) || len == 0 {
            return Err(TemplateError::Length {
                field: field.to_owned(),
                expected: expected.unwrap_or(1),
            });
        }
        let n = self.number();
        let sentinel = match &hole {
            Hole::Rendered {
                rendering: Rendering::UuidText,
                ..
            } => uuid_text(n).into_bytes(),
            Hole::Rendered {
                rendering: Rendering::Hex { .. },
                ..
            } => hex_text(n, len),
            Hole::Time {
                rendering: TimeRendering::Rfc3339Micros,
                ..
            } => time_text(n, true).into_bytes(),
            _ => raw_bytes(n, len),
        };
        self.push(sentinel.clone(), Token::Raw, field, hole);
        Ok(sentinel)
    }

    /// The payload: every sentinel found exactly once — a number only between
    /// JSON delimiters — and its span recorded; spans in order and apart.
    pub fn finish(self, bytes: Vec<u8>) -> Result<Payload, TemplateError> {
        let mut holes = Vec::with_capacity(self.pending.len());
        for p in self.pending {
            let found: Vec<usize> = occurrences(&bytes, &p.sentinel)
                .filter(|&at| p.token != Token::Number || delimited(&bytes, at, p.sentinel.len()))
                .collect();
            let [offset] = found[..] else {
                return Err(TemplateError::Sentinel {
                    field: p.field,
                    found: found.len(),
                });
            };
            holes.push(Span {
                offset: u32::try_from(offset).map_err(|_| TemplateError::Layout)?,
                len: u32::try_from(p.sentinel.len()).map_err(|_| TemplateError::Layout)?,
                field: p.field,
                hole: p.hole,
                form: match p.token {
                    Token::String | Token::Number => Form::JsonToken,
                    Token::Raw => Form::Raw,
                },
            });
        }
        holes.sort_by_key(|s| s.offset);
        let apart = holes
            .windows(2)
            .all(|w| matches!(w, [a, b] if a.offset.saturating_add(a.len) <= b.offset));
        if !apart || u32::try_from(bytes.len()).is_err() {
            return Err(TemplateError::Layout);
        }
        Ok(Payload {
            codec: self.codec,
            bytes,
            holes,
        })
    }
}

/// Every position `needle` occurs at in `haystack`.
fn occurrences<'a>(haystack: &'a [u8], needle: &'a [u8]) -> impl Iterator<Item = usize> + 'a {
    haystack
        .windows(needle.len().max(1))
        .enumerate()
        .filter(move |(_, w)| !needle.is_empty() && *w == needle)
        .map(|(at, _)| at)
}

/// Whether the token at `at..at + len` stands between JSON delimiters, so that
/// it is not the middle of a longer number or of a string.
fn delimited(bytes: &[u8], at: usize, len: usize) -> bool {
    let before = at.checked_sub(1).and_then(|i| bytes.get(i));
    let after = bytes.get(at.saturating_add(len));
    let opens = |b: Option<&u8>| {
        b.is_none_or(|b| matches!(b, b':' | b'[' | b',' | b' ' | b'\n' | b'\t' | b'\r'))
    };
    let closes = |b: Option<&u8>| {
        b.is_none_or(|b| matches!(b, b',' | b']' | b'}' | b' ' | b'\n' | b'\t' | b'\r'))
    };
    opens(before) && closes(after)
}
