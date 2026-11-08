//! What the intake does with a message it is handed (ADR-0036): it stores
//! it as an inbox row, finds it already stored, or sets it aside beside the
//! inbox. Nothing else, so that a message that can never be stored does not
//! hold its source, and only the environment does.
//!
//! This is the pure half. A message past its size bounds is read no further
//! (ADR-0037 decision 3). Its identity is read from its headers and checked
//! before any statement (R3): a header missing, not UTF-8 or named twice, a
//! position that is not an `i32`, a NUL, a stream id outside the bus's
//! domain (`identity::read_stream_id`, ADR-0037 decision 1), a value past
//! its column, a key past the index's row, a `message_id` that is not a
//! UUID. What fails is *malformed*. The checks are PostgreSQL's own limits,
//! stated here so that a host's `serde_json` features cannot turn a
//! storable message into a stall. PostgreSQL's refusal stays a backstop
//! (`unstorable`).
//!
//! Every header that is not a column is kept as its text, never structured
//! (ADR-0037 decision 2): nothing reads the structure, and a handler gets
//! the producer's bytes.
//!
//! A message set aside is kept as a frame of its bytes — the destination,
//! the key, the headers and the payload, each length-delimited — under a
//! digest of that frame, so that a repeat of the same bytes is the same row.

use std::collections::BTreeSet;

use ascetic_ddd_bus::identity::{self, STREAM_ID, STREAM_POSITION, STREAM_TYPE, TENANT_ID};
use ascetic_ddd_bus::{Message, uri};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::message::InboxMessage;

/// The header the outbox stamps with the channel a message was sent to.
pub(crate) const DESTINATION: &str = "destination";

/// The headers that are columns of the inbox: its identity (the bus's,
/// ADR-0030), and the channel the message was sent to.
pub(crate) const COLUMNS: [&str; 5] = [
    TENANT_ID,
    STREAM_TYPE,
    STREAM_ID,
    STREAM_POSITION,
    DESTINATION,
];

/// The widths of the inbox's columns, in characters (`store.rs`).
const TENANT_ID_WIDTH: usize = 128;
const STREAM_TYPE_WIDTH: usize = 128;
const URI_WIDTH: usize = 255;

/// The bytes the primary key's columns may take together: a btree's row is
/// refused past about 2,704 bytes (54000), and the key's entry carries a
/// header of its own.
const KEY_BYTES: usize = 2_600;

/// Why a message was set aside beside the inbox.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Reason {
    /// Its identity could not be read from its headers, or a value it
    /// carries could never be stored.
    Malformed,
    /// PostgreSQL refused a value the checks did not foresee: 22P02, 22001,
    /// 22021, 22P05, or 54000 on the inbox's own index.
    Unstorable,
    /// Its stream's owner is not a producer of the channel it arrived on
    /// (ADR-0036 R5): the host's admission refused it.
    Foreign,
    /// Its identity is held by a message with its own `message_id` and
    /// another payload: a re-emission, never a redelivery (R1).
    Differs,
    /// Its `message_id` is held at another identity of its Tenant and
    /// stream type (R5).
    IdentityTaken,
}

impl Reason {
    /// The reason as the table and the observer name it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Reason::Malformed => "malformed",
            Reason::Unstorable => "unstorable",
            Reason::Foreign => "foreign",
            Reason::Differs => "differs",
            Reason::IdentityTaken => "identity_taken",
        }
    }

    /// The reason the table names `text`, if it names one.
    #[must_use]
    pub fn parse(text: &str) -> Option<Reason> {
        [
            Reason::Malformed,
            Reason::Unstorable,
            Reason::Foreign,
            Reason::Differs,
            Reason::IdentityTaken,
        ]
        .into_iter()
        .find(|reason| reason.as_str() == text)
    }
}

/// A message's identity read from its headers, the checks passed, as the
/// row it becomes; or what refused it, in the library's own words.
pub(crate) fn row_of(destination: &str, message: &Message) -> Result<InboxMessage, &'static str> {
    let column = |name: &str| -> Result<&str, &'static str> {
        let value = message
            .header(name)
            .ok_or("an identity header is missing")?;
        text(value)
    };
    // `header` reads a name's first occurrence and the metadata would keep
    // its last: a dependency list checked here could differ from the one
    // dispatch reads.
    let mut names = BTreeSet::new();
    if !message
        .headers()
        .iter()
        .all(|(name, _)| names.insert(name.as_str()))
    {
        return Err("a header is named twice");
    }
    let tenant_id = column(TENANT_ID)?;
    let stream_type = column(STREAM_TYPE)?;
    let stream_id_text = column(STREAM_ID)?;
    if tenant_id.chars().count() > TENANT_ID_WIDTH {
        return Err("the tenant is longer than its column");
    }
    if stream_type.chars().count() > STREAM_TYPE_WIDTH {
        return Err("the stream type is longer than its column");
    }
    let stream_id = identity::read_stream_id(stream_id_text).map_err(identity::Refused::as_str)?;
    let stream_position = column(STREAM_POSITION)?
        .parse::<i32>()
        .map_err(|_| "the position is not an integer")?;
    let key_bytes = tenant_id.len() + stream_type.len() + stream_id.to_string().len() + 4;
    if key_bytes > KEY_BYTES {
        return Err("the identity is past the index's row size");
    }
    // The intake's name for the row: the channel this producer was built
    // for, with the message's key, or the channel's own (ADR-0018). A
    // `destination` header the producer stamped is its address, not ours.
    let key = match message.key() {
        Some(key) => Some(text(key)?),
        None => uri::key(destination),
    };
    let uri = match key {
        Some(key) => format!("{}/{key}", uri::without_key(destination)),
        None => destination.to_owned(),
    };
    if uri.chars().count() > URI_WIDTH {
        return Err("the destination is longer than its column");
    }
    if uri.contains('\0') {
        return Err("the destination holds a NUL");
    }
    let metadata = message
        .headers()
        .iter()
        .filter(|(name, _)| !COLUMNS.contains(&name.as_str()))
        .map(|(name, value)| {
            if name.contains('\0') {
                return Err("a header's name holds a NUL");
            }
            Ok((name.clone(), Value::String(text(value)?.to_owned())))
        })
        .collect::<Result<Map<_, _>, _>>()?;
    match metadata.get("message_id") {
        None => {}
        Some(Value::String(id)) if is_uuid(id) => {}
        Some(_) => return Err("the message_id is not a UUID"),
    }
    Ok(InboxMessage::new(
        tenant_id,
        stream_type,
        stream_id,
        stream_position,
        uri,
        message.payload(),
    )
    .with_metadata(Value::Object(metadata)))
}

/// A header's bytes as text: UTF-8, without a NUL, which no text column
/// takes (22021).
fn text(bytes: &[u8]) -> Result<&str, &'static str> {
    let text = std::str::from_utf8(bytes).map_err(|_| "a header or the key is not UTF-8")?;
    if text.contains('\0') {
        return Err("a header holds a NUL");
    }
    Ok(text)
}

/// Whether PostgreSQL's `uuid` takes `text`: 32 hexadecimal digits, of
/// either case, optionally between braces, with a hyphen allowed after any
/// group of four digits but not at either end nor twice in a row.
pub(crate) fn is_uuid(text: &str) -> bool {
    let inner = match (text.strip_prefix('{'), text.strip_suffix('}')) {
        (Some(_), Some(_)) if text.len() >= 2 => &text[1..text.len() - 1],
        (None, None) => text,
        _ => return false,
    };
    let mut digits = 0usize;
    let mut previous_hyphen = true;
    for c in inner.chars() {
        if c.is_ascii_hexdigit() {
            digits += 1;
            previous_hyphen = false;
        } else if c == '-' {
            if previous_hyphen || digits % 4 != 0 {
                return false;
            }
            previous_hyphen = true;
        } else {
            return false;
        }
    }
    digits == 32 && !previous_hyphen
}

/// The largest message the intake reads (ADR-0037 decision 3): a message
/// past one bound is set aside as malformed before any header is parsed or
/// copied, and kept cut: its sizes, a prefix of each part and a digest of
/// the whole, which a readmit refuses. The broker's adapter
/// caps what it fetches to the same sizes, so that a compressed record does
/// not unfold into what no consumer can hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bounds {
    /// The payload's bytes.
    pub payload: usize,
    /// One header's bytes, its name with its value.
    pub header: usize,
    /// How many headers.
    pub headers: usize,
    /// All the headers' bytes together.
    pub header_bytes: usize,
}

impl Default for Bounds {
    /// 8 MiB of payload; 64 headers of at most 256 KiB, 1 MiB in all: a
    /// list of 64 causal dependencies at the largest identity fits.
    fn default() -> Self {
        Self {
            payload: 8 << 20,
            header: 256 << 10,
            headers: 64,
            header_bytes: 1 << 20,
        }
    }
}

impl Bounds {
    /// Which bound `message` is past, if any.
    pub(crate) fn exceeded(&self, message: &Message) -> Option<&'static str> {
        let headers = message.headers();
        let size = |(name, value): &(String, Vec<u8>)| name.len() + value.len();
        if message.payload().len() > self.payload {
            Some("its payload is past the intake's bound")
        } else if headers.len() > self.headers {
            Some("it carries more headers than the intake's bound")
        } else if headers.iter().any(|header| size(header) > self.header) {
            Some("a header is past the intake's bound")
        } else if headers.iter().map(size).sum::<usize>() > self.header_bytes {
            Some("its headers together are past the intake's bound")
        } else {
            None
        }
    }
}

/// How a cut frame begins: a destination of `u32::MAX` bytes, which no
/// whole frame has, so that [`unframe`] reads none from it and a readmit is
/// refused.
const CUT: [u8; 4] = [0xFF; 4];

/// How many bytes of each part a cut frame keeps.
const CUT_PREFIX: usize = 1024;

/// Whether `bytes` are a cut frame.
pub(crate) fn is_cut(bytes: &[u8]) -> bool {
    bytes.starts_with(&CUT)
}

/// A message past its bounds as kept beside the inbox (ADR-0037 decision
/// 3): the mark, the digest of its whole frame — computed as the frame is
/// written, never held — the destination, the key's and every header's
/// size with a prefix of each, and the payload's size with its prefix. Two
/// messages past their bounds frame alike only when their whole frames
/// are alike; none reads back as a message.
pub(crate) fn cut_frame(destination: &str, message: &Message) -> Vec<u8> {
    let mut whole = Sha256::new();
    write_frame(destination, message, &mut |bytes| whole.update(bytes));
    let mut out = Vec::with_capacity(CUT_PREFIX * 4);
    out.extend(CUT);
    out.extend(whole.finalize());
    let mut put = |bytes: &[u8]| {
        out.extend(u32::try_from(bytes.len()).unwrap_or(u32::MAX).to_be_bytes());
        out.extend(&bytes[..bytes.len().min(CUT_PREFIX)]);
    };
    put(destination.as_bytes());
    put(message.key().unwrap_or_default());
    for (name, value) in message.headers().iter().take(CUT_PREFIX) {
        put(name.as_bytes());
        put(value);
    }
    put(message.payload());
    out
}

/// The message as kept beside the inbox: the destination, the key — a flag
/// and its bytes — the headers in order, and the payload, each
/// length-delimited, so that no two messages frame alike.
pub(crate) fn frame(destination: &str, message: &Message) -> Vec<u8> {
    let mut out = Vec::with_capacity(message.payload().len() + 256);
    write_frame(destination, message, &mut |bytes| out.extend(bytes));
    out
}

/// The frame's bytes, in order, to `sink`.
fn write_frame(destination: &str, message: &Message, sink: &mut impl FnMut(&[u8])) {
    fn put(sink: &mut impl FnMut(&[u8]), bytes: &[u8]) {
        sink(&u32::try_from(bytes.len()).unwrap_or(u32::MAX).to_be_bytes());
        sink(bytes);
    }
    put(sink, destination.as_bytes());
    match message.key() {
        Some(key) => {
            sink(&[1]);
            put(sink, key);
        }
        None => sink(&[0]),
    }
    let headers = message.headers();
    sink(
        &u32::try_from(headers.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    for (name, value) in headers {
        put(sink, name.as_bytes());
        put(sink, value);
    }
    put(sink, message.payload());
}

/// A frame read back: the destination and the message, as [`frame`] wrote
/// them, so that a readmit takes the very message set aside; none when the
/// bytes are not a whole frame.
pub(crate) fn unframe(bytes: &[u8]) -> Option<(String, Message)> {
    struct Reader<'a>(&'a [u8]);
    impl<'a> Reader<'a> {
        fn take(&mut self, n: usize) -> Option<&'a [u8]> {
            let (head, rest) = self.0.split_at_checked(n)?;
            self.0 = rest;
            Some(head)
        }
        fn length(&mut self) -> Option<usize> {
            let bytes = self.take(4)?;
            usize::try_from(u32::from_be_bytes(bytes.try_into().ok()?)).ok()
        }
        fn part(&mut self) -> Option<&'a [u8]> {
            let n = self.length()?;
            self.take(n)
        }
        fn text(&mut self) -> Option<String> {
            String::from_utf8(self.part()?.to_vec()).ok()
        }
    }
    let mut reader = Reader(bytes);
    let destination = reader.text()?;
    let key = match reader.take(1)? {
        [0] => None,
        [1] => Some(reader.part()?),
        _ => return None,
    };
    let count = reader.length()?;
    let mut headers = Vec::new();
    for _ in 0..count {
        let name = reader.text()?;
        headers.push((name, reader.part()?));
    }
    let payload = reader.part()?;
    if !reader.0.is_empty() {
        return None;
    }
    let message = headers
        .into_iter()
        .fold(Message::new(payload), |message, (name, value)| {
            message.with_header(name, value)
        });
    Some((
        destination,
        match key {
            Some(key) => message.with_key(key),
            None => message,
        },
    ))
}

/// A digest as the UUID text PostgreSQL's `uuid` takes: the first 16 bytes
/// of SHA-256 over `bytes`.
pub(crate) fn digest(bytes: &[u8]) -> String {
    let hash = Sha256::digest(bytes);
    let hex: String = hash[..16].iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// The digest of a row's identity, by which a read by identity finds the
/// message set aside (ADR-0036 R4): the identity's columns, framed.
pub(crate) fn identity_digest(row: &InboxMessage) -> String {
    let mut out = Vec::new();
    for part in [
        row.tenant_id.as_bytes(),
        row.stream_type.as_bytes(),
        row.stream_id.to_string().as_bytes(),
        &row.stream_position.to_be_bytes(),
    ] {
        out.extend(u32::try_from(part.len()).unwrap_or(u32::MAX).to_be_bytes());
        out.extend(part);
    }
    digest(&out)
}

/// A header's value as the text a derived column keeps, and an observer and
/// a log are handed as an identifier: UTF-8, without a NUL, CR and LF
/// escaped so that it cannot start a line of its own (ADR-0036 R9), at most
/// `width` characters; none when it is not UTF-8.
pub(crate) fn sanitized(bytes: Option<&[u8]>, width: usize) -> Option<String> {
    let text = std::str::from_utf8(bytes?).ok()?;
    let escaped = text.chars().filter(|c| *c != '\0').flat_map(|c| match c {
        '\r' => vec!['\\', 'r'],
        '\n' => vec!['\\', 'n'],
        c => vec![c],
    });
    Some(escaped.take(width).collect())
}

/// The widths of the derived columns of the rows set aside.
pub(crate) const CHANNEL_WIDTH: usize = URI_WIDTH;
pub(crate) const DERIVED_WIDTH: usize = TENANT_ID_WIDTH;

/// Whether a PostgreSQL error is the message's own value, refused by the
/// server — a backstop for what [`row_of`] did not foresee — rather than the
/// environment's: a SQLSTATE of the closed list, and 54000 only when it
/// names one of the inbox's own indexes, since 54000 is also the server's
/// transaction-id stop limit.
pub(crate) fn unstorable(
    code: &str,
    constraint: Option<&str>,
    table: &str,
) -> Option<&'static str> {
    match code {
        "22P02" => Some("22P02, invalid text representation"),
        "22001" => Some("22001, a value too long for its column"),
        "22021" => Some("22021, a character not in the encoding"),
        "22P05" => Some("22P05, an untranslatable character"),
        "54000"
            if constraint.is_some_and(|name| {
                name == format!("{table}_pk") || name.starts_with(&format!("{table}__"))
            }) =>
        {
            Some("54000, an index row past its size")
        }
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::*;

    /// A message of a full identity, each header as given, one replaced by
    /// `name` and `value` when named.
    fn with(name: &str, value: impl Into<Vec<u8>>) -> Message {
        let value = value.into();
        [
            (TENANT_ID, b"t1".to_vec()),
            (STREAM_TYPE, b"orders.Order".to_vec()),
            (STREAM_ID, b"o-1".to_vec()),
            (STREAM_POSITION, b"3".to_vec()),
            (
                "message_id",
                b"0190a01a-7e5b-7d80-a000-00000000d211".to_vec(),
            ),
            ("traceparent", b"00-1-2-01".to_vec()),
        ]
        .into_iter()
        .fold(Message::new("payload"), |message, (header, default)| {
            if header == name {
                message.with_header(header, value.clone())
            } else {
                message.with_header(header, default)
            }
        })
    }

    fn identified(stream_id: &str) -> Message {
        with(STREAM_ID, stream_id)
    }

    #[test]
    fn a_message_s_identity_is_read_from_its_headers() {
        let row = row_of("inbox://orders/k", &identified("o-1")).unwrap();
        assert_eq!(row.tenant_id, "t1");
        assert_eq!(row.stream_id, Value::String("o-1".to_owned()));
        assert_eq!(row.stream_position, 3);
        assert_eq!(row.uri, "inbox://orders/k");
        assert_eq!(
            row.message_id(),
            Some("0190a01a-7e5b-7d80-a000-00000000d211")
        );
    }

    #[test]
    fn what_could_never_be_stored_is_refused_before_any_statement() {
        let without_tenant = Message::new("p")
            .with_header(STREAM_TYPE, "s")
            .with_header(STREAM_ID, "o")
            .with_header(STREAM_POSITION, "1");
        let cases: Vec<(&str, Message)> = vec![
            ("no tenant", without_tenant),
            ("a position not an integer", with(STREAM_POSITION, "x")),
            ("a position past i32", with(STREAM_POSITION, "4294967296")),
            ("a NUL in a header", identified("o\0")),
            (
                "\\u0000 in the stream id",
                identified(r#"{"id":"a\u0000"}"#),
            ),
            ("a tenant too long", with(TENANT_ID, "t".repeat(129))),
            (
                "a key past the row",
                identified(&format!("\"{}\"", "k".repeat(3_000))),
            ),
            ("a message_id not a UUID", with("message_id", "not-a-uuid")),
            (
                "a header not UTF-8",
                with("traceparent", vec![0xff_u8, 0xfe]),
            ),
        ];
        for (case, message) in cases {
            assert!(
                row_of("inbox://orders", &message).is_err(),
                "{case} must be refused"
            );
        }
    }

    /// A stream id is read by the bus's identity function (ADR-0037
    /// decision 1), the same in every build of serde_json's features: JSON
    /// outside its domain is refused, a raw id is the string of its text.
    /// Before, `1e999999` was a string in the default build and a stall in
    /// a host's build with `arbitrary_precision`.
    #[test]
    fn a_stream_id_outside_the_domain_is_refused_in_every_build() {
        for outside in ["1e999999", "1e-16384", "1.5", "-0", "18446744073709551616"] {
            assert!(
                row_of("inbox://orders", &identified(outside)).is_err(),
                "{outside}"
            );
        }
        let row = row_of("inbox://orders", &identified("o-1")).unwrap();
        assert_eq!(row.stream_id, Value::String("o-1".to_owned()));
        let row = row_of("inbox://orders", &identified("12345")).unwrap();
        assert_eq!(row.stream_id, Value::from(12_345_u64));
    }

    /// Every header that is not a column is kept as its text (ADR-0037
    /// decision 2): `jsonb` would reorder an object's keys, normalise its
    /// numbers and drop a key named twice, and nothing reads the structure.
    #[test]
    fn a_header_is_kept_as_its_text() {
        let texts = [
            "3",
            "true",
            "[1]",
            "\"x\"",
            "{\"b\":2,\"aa\":1}",
            "{\"a\":1,\"a\":2}",
            "[1e-16384]",
            "[1.0, 1e5]",
        ];
        let message = texts
            .iter()
            .enumerate()
            .fold(identified("o"), |message, (at, text)| {
                message.with_header(format!("h{at}"), *text)
            });
        let row = row_of("inbox://orders", &message).unwrap();
        for (at, text) in texts.iter().enumerate() {
            assert_eq!(
                row.metadata.as_ref().and_then(|m| m.get(format!("h{at}"))),
                Some(&Value::String((*text).to_owned())),
                "{text}"
            );
        }
    }

    #[test]
    fn a_header_named_twice_is_refused() {
        let twice = identified("o")
            .with_header("causal_dependencies", "[]")
            .with_header("causal_dependencies", "[]");
        assert_eq!(
            row_of("inbox://orders", &twice).err(),
            Some("a header is named twice")
        );
        let column_twice = identified("o").with_header(STREAM_ID, "p");
        assert!(row_of("inbox://orders", &column_twice).is_err());
    }

    /// Each bound refuses the message past it, and no other.
    #[test]
    fn a_message_past_a_bound_is_named_by_it() {
        // The six headers of `identified` hold 126 bytes, the largest 46.
        let bounds = Bounds {
            payload: 8,
            header: 48,
            headers: 10,
            header_bytes: 200,
        };
        let within = identified("o");
        assert_eq!(bounds.exceeded(&within), None);
        let payload = Message::new(vec![0_u8; 9])
            .with_header(TENANT_ID, "t")
            .with_header(STREAM_TYPE, "orders.Order");
        assert!(
            bounds
                .exceeded(&payload)
                .is_some_and(|why| why.contains("payload"))
        );
        let header = within.clone().with_header("x", "y".repeat(48));
        assert!(
            bounds
                .exceeded(&header)
                .is_some_and(|why| why.contains("a header"))
        );
        let many = (0..5).fold(within.clone(), |message, at| {
            message.with_header(format!("x{at}"), "y")
        });
        assert!(
            bounds
                .exceeded(&many)
                .is_some_and(|why| why.contains("more headers"))
        );
        let together = (0..3).fold(within, |message, at| {
            message.with_header(format!("x{at}"), "y".repeat(30))
        });
        assert!(
            bounds
                .exceeded(&together)
                .is_some_and(|why| why.contains("together"))
        );
    }

    /// A message past its bounds is kept cut: never read back as a
    /// message, the same for the same bytes, and another for a message that
    /// differs past the prefix it keeps.
    #[test]
    fn a_cut_frame_is_no_message_and_tells_whole_messages_apart() {
        let big = |last: u8| {
            let mut payload = vec![b'a'; 3 * CUT_PREFIX];
            payload.push(last);
            identified("o").with_payload(payload)
        };
        let cut = cut_frame("inbox://orders/k", &big(b'x'));
        assert!(is_cut(&cut));
        assert!(unframe(&cut).is_none());
        assert!(cut.len() < 2 * CUT_PREFIX + 512, "{}", cut.len());
        assert_eq!(cut, cut_frame("inbox://orders/k", &big(b'x')));
        assert_ne!(cut, cut_frame("inbox://orders/k", &big(b'y')));
        assert!(!is_cut(&frame("inbox://orders/k", &big(b'x'))));
    }

    #[test]
    fn a_uuid_is_what_postgresql_takes() {
        for taken in [
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
            "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}",
            "a0eebc999c0b4ef8bb6d6bb9bd380a11",
            "a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11",
            "{a0eebc99-9c0b4ef8-bb6d6bb9-bd380a11}",
        ] {
            assert!(is_uuid(taken), "{taken}");
        }
        for refused in [
            "",
            "urn:uuid:a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1",
            "-a0eebc999c0b4ef8bb6d6bb9bd380a11",
            "a0eebc999c0b4ef8bb6d6bb9bd380a11-",
            "a0e-ebc999c0b4ef8bb6d6bb9bd380a11",
            "a0eebc99--9c0b-4ef8-bb6d-6bb9bd380a11",
            "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            "g0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
        ] {
            assert!(!is_uuid(refused), "{refused}");
        }
    }

    #[test]
    fn two_messages_frame_alike_only_when_their_bytes_are_alike() {
        let a = Message::new("ab").with_header("x", "c");
        let b = Message::new("b").with_header("x", "ac");
        assert_ne!(frame("inbox://c", &a), frame("inbox://c", &b));
        assert_ne!(
            frame("inbox://c", &a),
            frame("inbox://c", &a.clone().with_key("k"))
        );
        assert_eq!(
            digest(&frame("inbox://c", &a)),
            digest(&frame("inbox://c", &a.clone()))
        );
        assert_eq!(digest(b"x").len(), 36);
    }

    #[test]
    fn derived_text_is_bounded_without_a_nul_or_a_line_break() {
        assert_eq!(sanitized(Some(b"a\0b"), 8).as_deref(), Some("ab"));
        assert_eq!(
            sanitized(Some("ü".repeat(10).as_bytes()), 3).as_deref(),
            Some("üüü")
        );
        assert_eq!(sanitized(Some(b"a\r\nb"), 8).as_deref(), Some("a\\r\\nb"));
        assert_eq!(sanitized(Some(&[0xff]), 8), None);
        assert_eq!(sanitized(None, 8), None);
    }

    #[test]
    fn only_the_listed_codes_are_the_message_s_own() {
        assert!(unstorable("22P02", None, "inbox").is_some());
        assert!(unstorable("54000", Some("inbox_pk"), "inbox").is_some());
        assert!(unstorable("54000", Some("inbox__message_id_uniq"), "inbox").is_some());
        assert!(
            unstorable("54000", None, "inbox").is_none(),
            "the XID stop limit"
        );
        for environment in [
            "57014", "55P03", "25006", "42P01", "42501", "40001", "08006", "22003",
        ] {
            assert!(
                unstorable(environment, None, "inbox").is_none(),
                "{environment}"
            );
        }
    }

    /// Properties over arbitrary messages (ADR-0036 § Executors): the
    /// intake's reading never panics, and what it makes a row of is what
    /// PostgreSQL stores.
    mod properties {
        use proptest::prelude::*;
        use proptest::strategy::ValueTree;
        use proptest::test_runner::TestRunner;

        use super::*;

        /// Bytes no column takes, or text past the bounds, or text that
        /// JSON reads as a value `jsonb` refuses.
        fn hostile() -> impl Strategy<Value = Vec<u8>> {
            prop_oneof![
                proptest::collection::vec(any::<u8>(), 0..24),
                any::<String>().prop_map(String::into_bytes),
                "[a-z\\x00]{0,300}".prop_map(String::into_bytes),
                "[a-z\\r\\n]{0,40}".prop_map(String::into_bytes),
                Just(br#"["\u0000"]"#.to_vec()),
                Just(br#"{"a\u0000":1}"#.to_vec()),
                Just(b"[1e999]".to_vec()),
                Just(b"1e-16384".to_vec()),
                Just(b"[1e-16384]".to_vec()),
                Just(br#"{"$serde_json::private::Number":"5"}"#.to_vec()),
            ]
        }

        /// A header's value: as a producer writes it, mostly, else hostile.
        fn header(plausible: &'static str) -> impl Strategy<Value = Vec<u8>> {
            prop_oneof![
                6 => plausible.prop_map(String::into_bytes),
                1 => hostile(),
            ]
        }

        fn message() -> impl Strategy<Value = (String, Message)> {
            (
                prop_oneof![
                    6 => "inbox://[a-z]{1,8}(/[a-z0-9]{0,40})?",
                    1 => "inbox://[a-z]{1,8}/[a-z0-9\\x00]{0,260}",
                ],
                proptest::option::of(header("[a-z0-9\\-]{1,40}")),
                (
                    header("[a-z0-9]{1,130}"),
                    header("[a-z]{1,8}\\.[A-Z][a-z]{1,124}"),
                    header("[a-z0-9\\-]{1,64}|\\[[0-9]{1,6},\"[a-z]{1,8}\"\\]"),
                    header("-?[0-9]{1,10}"),
                    proptest::option::of(header(
                        "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
                    )),
                ),
                proptest::collection::vec(
                    (
                        prop_oneof![8 => "[a-z_]{1,12}", 1 => "[a-z_\\x00]{1,12}"],
                        header("[ -~]{0,40}"),
                    ),
                    0..3,
                ),
                proptest::collection::vec(any::<u8>(), 0..16),
            )
                .prop_map(
                    |(
                        destination,
                        key,
                        (tenant, stream_type, stream_id, position, id),
                        extra,
                        payload,
                    )| {
                        let mut message = Message::new(payload)
                            .with_header(TENANT_ID, tenant)
                            .with_header(STREAM_TYPE, stream_type)
                            .with_header(STREAM_ID, stream_id)
                            .with_header(STREAM_POSITION, position);
                        if let Some(key) = key {
                            message = message.with_key(key);
                        }
                        if let Some(id) = id {
                            message = message.with_header("message_id", id);
                        }
                        let message = extra.into_iter().fold(message, |message, (name, value)| {
                            message.with_header(name, value)
                        });
                        (destination, message)
                    },
                )
        }

        /// The oracle: `jsonb` takes no `\u0000` in a string or a key
        /// (22P05), and holds exactly only an integer within 64 bits
        /// (ADR-0037 decision 1).
        fn jsonb_takes(value: &Value) -> bool {
            match value {
                Value::String(text) => !text.contains('\0'),
                Value::Number(number) => number.is_i64() || number.is_u64(),
                Value::Array(items) => items.iter().all(jsonb_takes),
                Value::Object(fields) => fields
                    .iter()
                    .all(|(name, field)| !name.contains('\0') && jsonb_takes(field)),
                _ => true,
            }
        }

        proptest! {
            #[test]
            fn a_row_the_intake_makes_is_one_postgresql_stores((destination, message) in message()) {
                if let Ok(row) = row_of(&destination, &message) {
                    for (text, width) in [
                        (&row.tenant_id, TENANT_ID_WIDTH),
                        (&row.stream_type, STREAM_TYPE_WIDTH),
                        (&row.uri, URI_WIDTH),
                    ] {
                        prop_assert!(!text.contains('\0'), "{text:?}");
                        prop_assert!(text.chars().count() <= width, "{text:?}");
                    }
                    prop_assert!(jsonb_takes(&row.stream_id));
                    prop_assert!(row.metadata.as_ref().is_none_or(jsonb_takes));
                    prop_assert!(
                        row.metadata
                            .as_ref()
                            .and_then(Value::as_object)
                            .is_none_or(|fields| fields.values().all(Value::is_string)),
                        "a header is kept as its text"
                    );
                    prop_assert!(row.message_id().is_none_or(is_uuid));
                }
            }

            #[test]
            fn derived_text_has_no_nul_and_fits_its_column(
                bytes in proptest::option::of(hostile()),
                width in 0usize..300,
            ) {
                match sanitized(bytes.as_deref(), width) {
                    Some(text) => {
                        prop_assert!(!text.contains(['\0', '\r', '\n']));
                        prop_assert!(text.chars().count() <= width);
                    }
                    None => prop_assert!(
                        bytes.as_deref().is_none_or(|b| std::str::from_utf8(b).is_err())
                    ),
                }
            }

            #[test]
            fn a_digest_is_a_uuid_postgresql_takes(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
                prop_assert!(is_uuid(&digest(&bytes)));
            }

            #[test]
            fn a_frame_reads_back_as_the_message_it_was(
                destination in any::<String>(),
                key in proptest::option::of(proptest::collection::vec(any::<u8>(), 0..16)),
                headers in proptest::collection::vec((any::<String>(), hostile()), 0..4),
                payload in proptest::collection::vec(any::<u8>(), 0..64),
            ) {
                let message = headers
                    .into_iter()
                    .fold(Message::new(payload), |message, (name, value)| message.with_header(name, value));
                let message = match key {
                    Some(key) => message.with_key(key),
                    None => message,
                };
                prop_assert_eq!(unframe(&frame(&destination, &message)), Some((destination, message)));
            }

            #[test]
            fn bytes_that_are_no_frame_read_as_none_without_a_panic(
                bytes in proptest::collection::vec(any::<u8>(), 0..64),
            ) {
                let _ = unframe(&bytes);
            }
        }

        /// The message strategy reaches both sides: rows made and messages
        /// refused, each in a good share of the cases, so that the property
        /// over rows is not passed by refusing everything.
        #[test]
        fn the_messages_reach_rows_and_refusals_alike() {
            let mut runner = TestRunner::deterministic();
            let strategy = message();
            let cases = 1_000;
            let rows = (0..cases)
                .filter(|_| {
                    let (destination, message) = strategy.new_tree(&mut runner).unwrap().current();
                    row_of(&destination, &message).is_ok()
                })
                .count();
            assert!(
                (cases / 5..=cases * 4 / 5).contains(&rows),
                "{rows} rows of {cases}"
            );
        }
    }
}
