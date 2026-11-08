//! The golden table of ADR-0037 decision 1: a `stream_id` header's text and
//! the identity it names, or its refusal. It is the same in every build of
//! serde_json's features, so it runs in the workspace's build and in
//! `verify/inbox-features`, which builds the library as the host does.
//!
//! Beside the table, a property: the writer is the reader's inverse over
//! every value in D, in both builds.
//!
//! Each row's expected identity is written as JSON in D, which serde_json
//! reads to the same value in every build: integers within 64 bits, strings,
//! and containers of them.

use ascetic_ddd_bus::identity::{STREAM_ID_DEPTH, read_stream_id, write_stream_id};
use proptest::prelude::*;
use serde_json::{Map, Value};

/// What a header's text must read as.
pub enum Expected {
    /// The identity, written as JSON in D.
    Id(&'static str),
    /// Refused: JSON outside D.
    Refused,
}

use Expected::{Id, Refused};

/// The rows: text, then what it reads as.
pub const ROWS: &[(&str, Expected)] = &[
    // Integers within 64 bits, as they are.
    ("12345", Id("12345")),
    ("0", Id("0")),
    (" 1", Id("1")),
    ("18446744073709551615", Id("18446744073709551615")),
    ("-9223372036854775808", Id("-9223372036854775808")),
    // Past 64 bits, or not an integer written plainly: refused in every
    // build. Without `arbitrary_precision`, `1e400` fails serde_json's parse
    // and fell back to a string; with it, it was kept as written.
    ("18446744073709551616", Refused),
    ("-9223372036854775809", Refused),
    ("1e5", Refused),
    ("1E5", Refused),
    ("1.0", Refused),
    ("0.5", Refused),
    ("1e-16384", Refused),
    ("1e131072", Refused),
    ("1e400", Refused),
    ("[1e400]", Refused),
    // `-0`: under `arbitrary_precision` serde_json reads it as the integer
    // 0, an alias of stream 0.
    ("-0", Refused),
    ("[-0]", Refused),
    ("{\"a\":-0}", Refused),
    // serde_json's private keys: under `arbitrary_precision` the first is
    // read as the number inside it, so the row carries an integer in D —
    // a check on the parsed value would admit it there. PostgreSQL sorts a
    // jsonb object's keys by length, so a token placed second comes back
    // first; an escaped one is the same key.
    ("{\"$serde_json::private::Number\":\"5\"}", Refused),
    ("{\"a\":1,\"$serde_json::private::Number\":\"5\"}", Refused),
    ("{\"\\u0024serde_json::private::Number\":\"5\"}", Refused),
    ("{\"$serde_json::private::RawValue\":\"5\"}", Refused),
    // Strings PostgreSQL or Rust cannot hold.
    ("\"\\u0000\"", Refused),
    ("{\"a\\u0000\":1}", Refused),
    ("\"\\ud800\"", Refused),
    ("\"\\udc00\"", Refused),
    ("\"\\ud83d\\ude00\"", Id("\"\u{1F600}\"")),
    ("o\u{0}1", Refused),
    // One key, one value.
    ("{\"a\":1,\"a\":2}", Refused),
    (
        "{\"b\":2,\"aa\":[1,\"x\",{\"k\":[true,null]}]}",
        Id("{\"aa\":[1,\"x\",{\"k\":[true,null]}],\"b\":2}"),
    ),
    // Raw ids: text that is not JSON is the string of that text.
    ("o-1", Id("\"o-1\"")),
    (
        "0a1b2c3d-0000-4000-8000-000000000000",
        Id("\"0a1b2c3d-0000-4000-8000-000000000000\""),
    ),
    ("01", Id("\"01\"")),
    // Text that begins as JSON and is not: its producer meant JSON, and is
    // refused rather than given another identity in silence.
    ("[1", Refused),
    ("{\"a\"", Refused),
    ("\"a\u{1}b\"", Refused),
    ("\"\\x41\"", Refused),
    ("\"\\u12\"", Refused),
    ("\"\\u12g4\"", Refused),
    (" \"unterminated", Refused),
    ("a b", Id("\"a b\"")),
    ("", Id("\"\"")),
    // Quoted ids: JSON strings.
    ("\"o-1\"", Id("\"o-1\"")),
    ("\"12345\"", Id("\"12345\"")),
    ("\"1e5\"", Id("\"1e5\"")),
    ("true", Id("true")),
    ("null", Id("null")),
];

/// Every row reads as it should; every identity is written back to text
/// that reads as the same identity; and nesting is bounded at
/// [`STREAM_ID_DEPTH`], however deep the text goes.
///
/// # Panics
/// On the first row that reads otherwise.
pub fn check() {
    for (text, expected) in ROWS {
        let read = read_stream_id(text);
        match expected {
            Id(json) => {
                let id: Value = serde_json::from_str(json).expect("a row's identity is JSON in D");
                assert_eq!(read, Ok(id.clone()), "{text:?}");
                assert_eq!(
                    read_stream_id(&write_stream_id(&id)),
                    Ok(id),
                    "{text:?} written back"
                );
            }
            Refused => assert!(read.is_err(), "{text:?} read as {read:?}"),
        }
    }
    let deep = |n: usize| format!("{}{}", "[".repeat(n), "]".repeat(n));
    assert!(read_stream_id(&deep(STREAM_ID_DEPTH)).is_ok());
    for depth in [STREAM_ID_DEPTH + 1, 129, 10_000] {
        assert!(read_stream_id(&deep(depth)).is_err(), "depth {depth}");
    }
}

/// Strings whose text a reader could take for JSON, beside arbitrary ones:
/// the writer must quote exactly these.
fn string() -> impl Strategy<Value = String> {
    prop_oneof![
        any::<String>().prop_map(|text| text.replace('\0', "")),
        prop::sample::select(vec![
            "12345",
            "1e5",
            "-0",
            "1e400",
            " 1",
            "true",
            "null",
            "[1]",
            "{\"a\":1}",
            "\"x\"",
            "{\"$serde_json::private::Number\":\"5\"}",
            "o-1",
            "",
            "[abc",
            "{x",
            "\"half",
            " [1",
        ])
        .prop_map(str::to_owned),
    ]
}

/// A value in D: integers within 64 bits, strings, booleans, null, and
/// containers of them, nested within the bound.
fn identity() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        any::<i64>().prop_map(Value::from),
        any::<u64>().prop_map(Value::from),
        any::<bool>().prop_map(Value::Bool),
        Just(Value::Null),
        string().prop_map(Value::String),
    ];
    leaf.prop_recursive(4, 32, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::btree_map(
                string().prop_filter("not serde_json's", |key| !key.starts_with("$serde_json::")),
                inner,
                0..4
            )
            .prop_map(|fields| Value::Object(fields.into_iter().collect::<Map<_, _>>())),
        ]
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]

    /// The writer is the reader's inverse on D.
    #[test]
    fn a_stream_id_written_reads_back_as_itself(id in identity()) {
        prop_assert_eq!(read_stream_id(&write_stream_id(&id)), Ok(id));
    }
}
