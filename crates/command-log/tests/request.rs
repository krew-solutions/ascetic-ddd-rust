//! A REST step's frame (ADR-0047 decision 15, K2a-2): the frames of
//! `golden/frames.jsonl` against `golden/frames.txt`, which
//! `vectors/request.py` writes outside the crate — Python's `urllib.parse`,
//! `json` and `hmac` and the decision's sentences — and twins of what the
//! golden cannot show.

use ascetic_ddd_command_log::fills::{Captured, Fills, HoleError};
use ascetic_ddd_command_log::record::{
    ClientRetry, Codec, Field, Form, Hole, Of, Operation, Payload, Rendering, Rest, Safety, Span,
    StepId, Subject, TimeBase, TimeRendering, Value,
};
use ascetic_ddd_command_log::render::{FillError, RunSecret};
use ascetic_ddd_command_log::request::{RequestError, check, frame};
use ascetic_ddd_faker::Namespace;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;

const CASES: &str = include_str!("../golden/frames.jsonl");
const FRAMES: &str = include_str!("../golden/frames.txt");
const CORRELATION: &str = "X-Correlation-Id";
const RUN_START: i64 = 1_763_640_000_123_456;
const SENT: i64 = RUN_START + 2_500_000;

fn device(index: u64) -> Subject {
    Subject {
        kind: "device".into(),
        scope: vec![1, 2],
        index,
    }
}

fn created() -> StepId {
    StepId {
        subject: device(7),
        label: "created".into(),
        n: 0,
    }
}

fn secret() -> RunSecret {
    RunSecret::new(std::array::from_fn(|i| i as u8))
}

fn op(method: &str, path: &str, content_type: &str, decorated: &[&str]) -> Operation {
    Operation {
        name: "op".into(),
        method: method.into(),
        path: path.into(),
        content_type: content_type.into(),
        decorated: decorated.iter().map(|d| (*d).to_owned()).collect(),
        writes: vec![],
        safety: Safety::Never,
        client: ClientRetry {
            attempts: 1,
            on: vec![],
            backoff: 0,
            retry_after: false,
        },
        not_run: vec![],
        timeout: 10_000_000,
        limits: vec![],
    }
}

fn rest(
    path: Vec<Value>,
    query: Vec<(&str, Value)>,
    headers: Vec<(&str, Value)>,
    body: Option<Payload>,
) -> Rest {
    let fields = |fs: Vec<(&str, Value)>| {
        fs.into_iter()
            .map(|(name, value)| Field {
                name: name.into(),
                value,
            })
            .collect()
    };
    Rest {
        op: "op".into(),
        caller: None,
        source: 0,
        path,
        query: fields(query),
        headers: fields(headers),
        body,
    }
}

fn lit(s: &str) -> Value {
    Value::Literal(s.into())
}

fn rendered(index: u64, role: &str, rendering: Rendering) -> Hole {
    Hole::Rendered {
        of: Of::Subject(device(index)),
        role: role.into(),
        rendering,
    }
}

fn captured(index: u64, slot: &str) -> Hole {
    Hole::Captured {
        subject: device(index),
        slot: slot.into(),
        from: created(),
    }
}

fn time(base: TimeBase, offset: i64, rendering: TimeRendering) -> Hole {
    Hole::Time {
        base,
        offset,
        rendering,
    }
}

enum P {
    Bytes(&'static [u8]),
    Span(Hole, Form, &'static [u8]),
}

/// A payload of literal bytes and spans, each span over its placeholder.
fn payload(codec: Codec, pieces: Vec<P>) -> Payload {
    let mut bytes = Vec::new();
    let mut holes = Vec::new();
    for p in pieces {
        match p {
            P::Bytes(b) => bytes.extend_from_slice(b),
            P::Span(hole, form, placeholder) => {
                holes.push(Span {
                    offset: bytes.len() as u32,
                    len: placeholder.len() as u32,
                    field: format!("/{}", holes.len()),
                    hole,
                    form,
                });
                bytes.extend_from_slice(placeholder);
            }
        }
    }
    Payload {
        codec,
        bytes,
        holes,
    }
}

/// The values the cases' captured holes take, as answers held them.
fn values() -> Vec<Captured> {
    let c = |index, slot: &str, value: &str| Captured {
        subject: device(index),
        slot: slot.into(),
        value: value.as_bytes().to_vec(),
    };
    vec![
        c(7, "etag", "\"\\\"v-1\\\"\""),
        c(7, "id", "\"dev:7/a b\""),
        c(7, "count", "12345678901234567890123"),
        c(7, "obj", "{\"a\":1}"),
        c(7, "bad", "nope"),
        c(8, "id", "\".\""),
        c(8, "crlf", "\"a\\r\\nb\""),
    ]
}

/// Every case: a name, the operation, the step's call.
fn cases() -> Vec<(&'static str, Operation, Rest)> {
    let get = |path: &str| op("GET", path, "", &[]);
    let json = |path: &str| op("POST", path, "application/json", &[]);
    let r = |path, query, headers| rest(path, query, headers, None);
    let h = Value::Hole;
    vec![
        (
            "a custom method",
            json("/devices/{device_id}:retire"),
            r(vec![lit("d1")], vec![], vec![]),
        ),
        (
            "a collection's verb",
            json("/policies:simulate"),
            rest(
                vec![],
                vec![],
                vec![],
                Some(payload(Codec::Json, vec![P::Bytes(b"{\"dry\":true}")])),
            ),
        ),
        (
            "values encoded, literals not",
            get("/tenants/{tenant}/devices/{device_id}:status"),
            r(vec![lit("a:b/c%d e~-._"), lit("é+@!")], vec![], vec![]),
        ),
        (
            "a query in order, encoded",
            get("/devices"),
            r(
                vec![],
                vec![
                    ("page size", lit("10&x=1")),
                    ("filter", lit("")),
                    ("filter", lit("a+b é")),
                ],
                vec![],
            ),
        ),
        (
            "rendered values",
            get("/devices/{device_id}"),
            r(
                vec![h(rendered(7, "id", Rendering::UuidText))],
                vec![(
                    "t",
                    h(time(TimeBase::RunStart, 0, TimeRendering::EpochMillis)),
                )],
                vec![("X-Hex", h(rendered(7, "tag", Rendering::Hex { len: 8 })))],
            ),
        ),
        (
            "times from the run's start and the send",
            get("/clock"),
            r(
                vec![],
                vec![],
                vec![
                    (
                        "X-At",
                        h(time(TimeBase::Sent, -1500, TimeRendering::Rfc3339Micros)),
                    ),
                    (
                        "X-Millis",
                        h(time(TimeBase::RunStart, 999, TimeRendering::EpochMillis)),
                    ),
                ],
            ),
        ),
        (
            "captured values in a path, a header and a body",
            op(
                "PUT",
                "/devices/{device_id}",
                "application/json",
                &["Authorization", "DPoP"],
            ),
            rest(
                vec![h(captured(7, "id"))],
                vec![("n", h(captured(7, "count")))],
                vec![("If-Match", h(captured(7, "etag")))],
                Some(payload(
                    Codec::Json,
                    vec![
                        P::Bytes(b"{\"etag\":"),
                        P::Span(captured(7, "etag"), Form::JsonToken, b"\"x\""),
                        P::Bytes(b",\"count\":"),
                        P::Span(captured(7, "count"), Form::JsonToken, b"0"),
                        P::Bytes(b",\"id\":"),
                        P::Span(captured(7, "id"), Form::JsonToken, b"\"y\""),
                        P::Bytes(b",\"at\":"),
                        P::Span(
                            time(TimeBase::Sent, 0, TimeRendering::Rfc3339Micros),
                            Form::JsonToken,
                            b"\"t\"",
                        ),
                        P::Bytes(b"}"),
                    ],
                )),
            ),
        ),
        (
            "key material in a raw body",
            op("POST", "/keys", "application/x-protobuf", &[]),
            rest(
                vec![],
                vec![],
                vec![],
                Some(payload(
                    Codec::Protobuf,
                    vec![
                        P::Bytes(b"\x0a\x20"),
                        P::Span(
                            rendered(7, "dpop", Rendering::Secret32),
                            Form::Raw,
                            &[b'k'; 32],
                        ),
                        P::Bytes(b"\x12\x10"),
                        P::Span(
                            rendered(7, "id", Rendering::UuidBytes),
                            Form::Raw,
                            &[b'u'; 16],
                        ),
                    ],
                )),
            ),
        ),
        (
            "arity",
            get("/devices/{device_id}"),
            r(vec![], vec![], vec![]),
        ),
        (
            "an empty segment",
            get("/devices/{device_id}"),
            r(vec![lit("")], vec![], vec![]),
        ),
        (
            "a captured dot segment",
            get("/devices/{device_id}"),
            r(vec![h(captured(8, "id"))], vec![], vec![]),
        ),
        (
            "a literal's percent",
            get("/a%20b"),
            r(vec![], vec![], vec![]),
        ),
        (
            "a literal's query",
            get("/a?b=1"),
            r(vec![], vec![], vec![]),
        ),
        (
            "a placeholder unclosed",
            get("/a/{id"),
            r(vec![lit("x")], vec![], vec![]),
        ),
        ("a brace alone", get("/a}/b"), r(vec![], vec![], vec![])),
        (
            "a template not rooted",
            get("a/b"),
            r(vec![], vec![], vec![]),
        ),
        (
            "a method not a token",
            op("GET POST", "/a", "", &[]),
            r(vec![], vec![], vec![]),
        ),
        (
            "a header name not a token",
            get("/a"),
            r(vec![], vec![], vec![("X Bad", lit("1"))]),
        ),
        (
            "a header value with a line break",
            get("/a"),
            r(vec![], vec![], vec![("X-A", lit("a\r\nb"))]),
        ),
        (
            "a header value's trailing blank",
            get("/a"),
            r(vec![], vec![], vec![("X-A", lit("a "))]),
        ),
        (
            "a header value outside ASCII",
            get("/a"),
            r(vec![], vec![], vec![("X-A", lit("é"))]),
        ),
        (
            "a host header",
            get("/a"),
            r(vec![], vec![], vec![("host", lit("other"))]),
        ),
        (
            "a Content-Length header",
            get("/a"),
            r(vec![], vec![], vec![("Content-Length", lit("0"))]),
        ),
        (
            "a decorated name planned",
            op("GET", "/a", "", &["Authorization"]),
            r(vec![], vec![], vec![("authorization", lit("x"))]),
        ),
        (
            "the correlation header planned",
            get("/a"),
            r(vec![], vec![], vec![("x-correlation-id", lit("x"))]),
        ),
        (
            "a decorated name reserved",
            op("GET", "/a", "", &["Content-Length"]),
            r(vec![], vec![], vec![]),
        ),
        (
            "the correlation header decorated",
            op("GET", "/a", "", &["X-Correlation-Id"]),
            r(vec![], vec![], vec![]),
        ),
        (
            "a capture missing",
            get("/a"),
            r(vec![], vec![], vec![("X-A", h(captured(7, "absent")))]),
        ),
        (
            "bytes in a plain value",
            get("/a"),
            r(
                vec![],
                vec![],
                vec![("X-A", h(rendered(7, "id", Rendering::UuidBytes)))],
            ),
        ),
        (
            "a captured object",
            get("/a"),
            r(vec![], vec![], vec![("X-A", h(captured(7, "obj")))]),
        ),
        (
            "a captured value not JSON",
            get("/a"),
            r(vec![], vec![], vec![("X-A", h(captured(7, "bad")))]),
        ),
        (
            "a captured line break into a header",
            get("/a"),
            r(vec![], vec![], vec![("X-A", h(captured(8, "crlf")))]),
        ),
        (
            "no content type for a body",
            get("/a"),
            rest(
                vec![],
                vec![],
                vec![],
                Some(payload(Codec::Json, vec![P::Bytes(b"{}")])),
            ),
        ),
        (
            "key material in a short raw span",
            op("POST", "/keys", "application/x-protobuf", &[]),
            rest(
                vec![],
                vec![],
                vec![],
                Some(payload(
                    Codec::Protobuf,
                    vec![P::Span(
                        rendered(7, "dpop", Rendering::Secret32),
                        Form::Raw,
                        &[b'k'; 16],
                    )],
                )),
            ),
        ),
        (
            "key material as a JSON token",
            json("/keys"),
            rest(
                vec![],
                vec![],
                vec![],
                Some(payload(
                    Codec::Json,
                    vec![P::Span(
                        rendered(7, "dpop", Rendering::Secret32),
                        Form::JsonToken,
                        b"\"k\"",
                    )],
                )),
            ),
        ),
    ]
}

fn case_lines() -> String {
    let secret: String = (0u8..32).map(|b| format!("{b:02x}")).collect();
    let captured: Vec<serde_json::Value> = values()
        .iter()
        .map(|c| {
            serde_json::json!({"subject": c.subject, "slot": c.slot, "value": STANDARD.encode(&c.value)})
        })
        .collect();
    cases()
        .into_iter()
        .map(|(name, op, rest)| {
            serde_json::json!({
                "name": name, "op": op, "rest": rest, "namespace": 1, "secret": secret,
                "run_start": RUN_START, "sent": SENT, "captured": captured, "correlation": CORRELATION,
            })
            .to_string()
                + "\n"
        })
        .collect()
}

fn class(e: &RequestError) -> String {
    let hole = |e: &HoleError| match e {
        HoleError::Missing { .. } => "missing",
        HoleError::NotText => "not_text",
        HoleError::NotScalar { .. } => "not_scalar",
        HoleError::NotJson { .. } => "not_json",
        HoleError::Time => "time",
    };
    let fill = |e: &FillError| match e {
        FillError::Span => "span",
        FillError::Form { .. } => "form",
        FillError::Length { .. } => "length",
        FillError::Time => "time",
    };
    match e {
        RequestError::Method => "method".into(),
        RequestError::Template => "template".into(),
        RequestError::Literal => "literal".into(),
        RequestError::Arity { .. } => "arity".into(),
        RequestError::Segment { .. } => "segment".into(),
        RequestError::HeaderName { .. } => "header_name".into(),
        RequestError::HeaderValue { .. } => "header_value".into(),
        RequestError::Reserved { .. } => "reserved".into(),
        RequestError::ContentType => "content_type".into(),
        RequestError::Hole { error, .. } => format!("hole {}", hole(error)),
        RequestError::Fill(e) => format!("fill {}", fill(e)),
    }
}

/// The golden text, as the crate builds it.
fn computed() -> String {
    let secret = secret();
    let captured = values();
    let fills = Fills {
        namespace: Namespace(1),
        secret: &secret,
        run_start: RUN_START,
        sent: SENT,
        captured: &captured,
    };
    let mut out = String::new();
    for (name, op, rest) in cases() {
        out += &format!("case {name}\n");
        match frame(&op, &rest, &fills, CORRELATION) {
            Err(e) => out += &format!("error {}\n", class(&e)),
            Ok(f) => {
                out += &format!("method {}\ntarget {}\n", f.method, f.target);
                for (n, v) in &f.headers {
                    out += &format!("header {n}: {v}\n");
                }
                match &f.body {
                    None => out += "body none\n",
                    Some(b) => {
                        out += &format!(
                            "body {}\n",
                            b.iter().map(|x| format!("{x:02x}")).collect::<String>()
                        )
                    }
                }
            }
        }
    }
    out
}

/// The cases the vectors read are these.
#[test]
fn the_golden_cases_are_the_test_s() {
    assert_eq!(
        case_lines(),
        CASES,
        "golden/frames.jsonl is stale: run `cargo test --test request -- --ignored`"
    );
}

#[test]
#[ignore = "regenerates golden/frames.jsonl"]
fn regenerate() {
    std::fs::write(
        concat!(env!("CARGO_MANIFEST_DIR"), "/golden/frames.jsonl"),
        case_lines(),
    )
    .unwrap();
}

/// Decision 15: every frame and every refusal as built outside the crate.
#[test]
fn frames_match_the_independent_vectors() {
    assert_eq!(computed(), FRAMES);
}

/// The static half refuses what the frame refuses for its plan, and passes
/// what only the values can fail.
#[test]
fn check_is_the_frame_s_static_half() {
    for (name, op, rest) in cases() {
        let secret = secret();
        let captured = values();
        let fills = Fills {
            namespace: Namespace(1),
            secret: &secret,
            run_start: RUN_START,
            sent: SENT,
            captured: &captured,
        };
        let built = frame(&op, &rest, &fills, CORRELATION);
        match check(&op, &rest, CORRELATION) {
            Err(e) => assert_eq!(built, Err(e), "{name}"),
            Ok(()) => assert!(
                !matches!(
                    built,
                    Err(RequestError::Method
                        | RequestError::Template
                        | RequestError::Literal
                        | RequestError::Arity { .. }
                        | RequestError::HeaderName { .. }
                        | RequestError::Reserved { .. }
                        | RequestError::ContentType)
                ),
                "{name}: {built:?}"
            ),
        }
    }
}

/// A frame is a function of its fills: the `Sent` instant is the frame's,
/// handed once; nothing in a frame names an attempt.
#[test]
fn a_frame_is_its_fills_function() {
    let (_, op, rest) = cases()
        .into_iter()
        .find(|c| c.0.starts_with("captured values"))
        .unwrap();
    let secret = secret();
    let captured = values();
    let fills = |sent| Fills {
        namespace: Namespace(1),
        secret: &secret,
        run_start: RUN_START,
        sent,
        captured: &captured,
    };
    let a = frame(&op, &rest, &fills(SENT), CORRELATION).unwrap();
    assert_eq!(a, frame(&op, &rest, &fills(SENT), CORRELATION).unwrap());
    assert_ne!(a, frame(&op, &rest, &fills(SENT + 1), CORRELATION).unwrap());
}

/// Decision 15: no `Debug` of a fill, a frame or a captured value shows a
/// secret, a key or a captured value.
#[test]
fn no_debug_shows_a_secret_or_a_captured_value() {
    let secret = secret();
    let captured = values();
    let fills = Fills {
        namespace: Namespace(1),
        secret: &secret,
        run_start: RUN_START,
        sent: SENT,
        captured: &captured,
    };
    let (_, op, rest) = cases()
        .into_iter()
        .find(|c| c.0 == "key material in a raw body")
        .unwrap();
    let raw = frame(&op, &rest, &fills, CORRELATION).unwrap();
    let key = &raw.body.as_ref().unwrap()[2..34];
    let (_, op, rest) = cases()
        .into_iter()
        .find(|c| c.0.starts_with("captured values"))
        .unwrap();
    let put = frame(&op, &rest, &fills, CORRELATION).unwrap();
    let shown = format!("{fills:?} {raw:?} {put:?} {captured:?} {secret:?}");
    let hex = |b: &[u8]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
    for planted in [
        hex(key),
        hex(&(0u8..32).collect::<Vec<_>>()),
        "v-1".into(),
        "dev:7".into(),
        "dev%3A7".into(),
        "12345678901234567890123".into(),
    ] {
        assert!(!shown.contains(&planted), "{planted} in {shown}");
    }
    assert!(!shown.contains(&String::from_utf8_lossy(key).into_owned()));
}

/// K2b-7e-2b-3a of ADR-0047 (decision 22): every case's frame is sized at
/// once — its target and its body each allocated to their length, never grown
/// by doubling — so a carrier prices a frame by its bytes: an encoded target,
/// a body filled past its spans.
#[test]
fn every_frame_is_sized_at_once() {
    let secret = secret();
    let held = values();
    let fills = Fills {
        namespace: Namespace(1),
        secret: &secret,
        run_start: RUN_START,
        sent: SENT,
        captured: &held,
    };
    // An encoded target past its values' length, a body filled past its
    // spans: what growing by doubling would over-allocate.
    let grown = (
        "grown",
        op("POST", "/d/{id}/{name}/x", "application/json", &[]),
        rest(
            vec![Value::Hole(captured(7, "id")), lit("a \"b\" c/d?e")],
            vec![
                ("q x", lit("é/?&=")),
                ("n", Value::Hole(captured(7, "count"))),
            ],
            vec![],
            Some(payload(
                Codec::Json,
                vec![
                    P::Bytes(b"{\"etag\":"),
                    P::Span(captured(7, "etag"), Form::JsonToken, b"0"),
                    P::Bytes(b",\"n\":"),
                    P::Span(captured(7, "count"), Form::JsonToken, b"0"),
                    P::Bytes(b"}"),
                ],
            )),
        ),
    );
    let mut framed = 0;
    for (name, op, rest) in cases().into_iter().chain([grown]) {
        let Ok(f) = frame(&op, &rest, &fills, CORRELATION) else {
            continue;
        };
        framed += 1;
        assert_eq!(f.target.capacity(), f.target.len(), "{name}'s target");
        if let Some(b) = &f.body {
            assert_eq!(b.capacity(), b.len(), "{name}'s body");
        }
        if name == "grown" {
            assert!(f.target.len() > 60, "{}", f.target);
            assert!(
                f.body.as_ref().is_some_and(|b| b.len() > 40),
                "{:?}",
                f.body
            );
        }
    }
    assert!(framed > 1, "{framed} cases framed");
}
