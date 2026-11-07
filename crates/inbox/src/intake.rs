//! What the intake does with a message it is handed (ADR-0036): it stores
//! it as an inbox row, finds it already stored, or sets it aside beside the
//! inbox. Nothing else, so that a message that can never be stored does not
//! hold its source, and only the environment does.
//!
//! This is the pure half. The message's identity is read from its headers
//! and checked before any statement (R3): a header missing or not UTF-8, a
//! position that is not an `i32`, a NUL, a number PostgreSQL could not hold,
//! a value past its column, a key past the index's row, a `message_id` that
//! is not a UUID. What fails is *malformed*. The checks are PostgreSQL's
//! own limits, stated here so that a host's `serde_json` features — which
//! decide what a header's JSON becomes — cannot turn a storable message into
//! a stall. PostgreSQL's refusal stays a backstop (`unstorable`).
//!
//! A message set aside is kept as a frame of its bytes — the destination,
//! the key, the headers and the payload, each length-delimited — under a
//! digest of that frame, so that a repeat of the same bytes is the same row.

use ascetic_ddd_bus::identity::{STREAM_ID, STREAM_POSITION, STREAM_TYPE, TENANT_ID};
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
    let tenant_id = column(TENANT_ID)?;
    let stream_type = column(STREAM_TYPE)?;
    let stream_id_text = column(STREAM_ID)?;
    if tenant_id.chars().count() > TENANT_ID_WIDTH {
        return Err("the tenant is longer than its column");
    }
    if stream_type.chars().count() > STREAM_TYPE_WIDTH {
        return Err("the stream type is longer than its column");
    }
    let stream_id = serde_json::from_str(stream_id_text)
        .unwrap_or_else(|_| Value::String(stream_id_text.to_owned()));
    storable(&stream_id)?;
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
            let value = structured(text(value)?);
            storable(&value)?;
            Ok((name.clone(), value))
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

/// A JSON value PostgreSQL can hold as `jsonb`: no `\u0000` in a string,
/// which `jsonb` refuses (22P05), and no number past what `numeric` takes
/// (22003). A number is held to a finite `f64`, a bound tighter than
/// `numeric`'s, since a number past it is no identity a producer means.
fn storable(value: &Value) -> Result<(), &'static str> {
    match value {
        Value::String(text) if text.contains('\0') => Err("a JSON string holds \\u0000"),
        Value::Number(number) if !number.as_f64().is_some_and(f64::is_finite) => {
            Err("a JSON number is past what PostgreSQL holds")
        }
        Value::Array(items) => items.iter().try_for_each(storable),
        Value::Object(fields) => fields.iter().try_for_each(|(name, field)| {
            if name.contains('\0') {
                Err("a JSON key holds \\u0000")
            } else {
                storable(field)
            }
        }),
        _ => Ok(()),
    }
}

/// A header's text as a metadata field: structured again when it is a JSON
/// array or object, text otherwise.
pub(crate) fn structured(text: &str) -> Value {
    match serde_json::from_str::<Value>(text) {
        Ok(value @ (Value::Array(_) | Value::Object(_))) => value,
        _ => Value::String(text.to_owned()),
    }
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

/// The message as kept beside the inbox: the destination, the key — a flag
/// and its bytes — the headers in order, and the payload, each
/// length-delimited, so that no two messages frame alike.
pub(crate) fn frame(destination: &str, message: &Message) -> Vec<u8> {
    fn put(out: &mut Vec<u8>, bytes: &[u8]) {
        out.extend(u32::try_from(bytes.len()).unwrap_or(u32::MAX).to_be_bytes());
        out.extend(bytes);
    }
    let mut out = Vec::with_capacity(message.payload().len() + 256);
    put(&mut out, destination.as_bytes());
    match message.key() {
        Some(key) => {
            out.push(1);
            put(&mut out, key);
        }
        None => out.push(0),
    }
    let headers = message.headers();
    out.extend(
        u32::try_from(headers.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    for (name, value) in headers {
        put(&mut out, name.as_bytes());
        put(&mut out, value);
    }
    put(&mut out, message.payload());
    out
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

/// A header's value as the text a derived column keeps: UTF-8, without a
/// NUL, at most `width` characters; none when it is not UTF-8.
pub(crate) fn sanitized(bytes: Option<&[u8]>, width: usize) -> Option<String> {
    let text = std::str::from_utf8(bytes?).ok()?;
    Some(text.chars().filter(|c| *c != '\0').take(width).collect())
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

    /// A number past `f64` is a JSON number only under `serde_json`'s
    /// `arbitrary_precision`, which a host may enable: there it is refused
    /// (the host's differential test holds that, ADR-0036 R3). Without the
    /// feature it does not parse as a number, and is kept as the string it
    /// is, which PostgreSQL stores.
    #[test]
    fn a_number_past_f64_without_the_feature_is_a_string() {
        let row = row_of("inbox://orders", &identified("1e999999")).unwrap();
        assert_eq!(row.stream_id, Value::String("1e999999".to_owned()));
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
    fn derived_text_is_bounded_without_a_nul() {
        assert_eq!(sanitized(Some(b"a\0b"), 8).as_deref(), Some("ab"));
        assert_eq!(
            sanitized(Some("ü".repeat(10).as_bytes()), 3).as_deref(),
            Some("üüü")
        );
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
                Just(br#"["\u0000"]"#.to_vec()),
                Just(br#"{"a\u0000":1}"#.to_vec()),
                Just(b"[1e999]".to_vec()),
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

        /// The oracle, apart from [`storable`]: `jsonb` takes no `\u0000`
        /// in a string or a key (22P05).
        fn jsonb_takes(value: &Value) -> bool {
            match value {
                Value::String(text) => !text.contains('\0'),
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
                        prop_assert!(!text.contains('\0'));
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
