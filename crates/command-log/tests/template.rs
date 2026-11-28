//! H1 of ADR-0046: a template's spans, and the splice that fills them, give the
//! bytes the typed message serialises to with its real values — through a host's
//! decorator too.

use ascetic_ddd_command_log::record::{
    Codec, Form, Hole, Of, Payload, Rendering, StepId, Subject, TimeBase, TimeRendering,
};
use ascetic_ddd_command_log::render::{FillError, Filled, RunSecret, fill, render, render_time};
use ascetic_ddd_command_log::template::{Template, TemplateError};
use ascetic_ddd_faker::Namespace;
use proptest::prelude::*;
use serde::Serialize;

fn device(index: u64) -> Subject {
    Subject {
        kind: "device".into(),
        scope: vec![0xfb, 0xff],
        index,
    }
}

fn id_of(index: u64) -> Hole {
    Hole::Rendered {
        of: Of::Subject(device(index)),
        role: "id".into(),
        rendering: Rendering::UuidText,
    }
}

fn sent_at(offset: i64, rendering: TimeRendering) -> Hole {
    Hole::Time {
        base: TimeBase::Sent,
        offset,
        rendering,
    }
}

/// The host's DTO, as its OpenAPI generator would write it.
#[derive(Serialize)]
struct Membership {
    group: String,
    members: Vec<String>,
    note: String,
    at: String,
    millis: i64,
}

const NS: Namespace = Namespace(11);
const SENT: i64 = 1_763_640_000_123_456;

/// The value a span takes at replay, from its hole alone.
fn value(span: &ascetic_ddd_command_log::record::Span) -> Result<Filled, FillError> {
    match &span.hole {
        Hole::Rendered {
            of,
            role,
            rendering,
        } => Ok(render(NS, &RunSecret::new([7; 32]), of, role, *rendering)),
        Hole::Time {
            offset, rendering, ..
        } => render_time(SENT, *offset, *rendering),
        Hole::Captured { .. } => Ok(Filled::Text("captured".into())),
    }
}

fn text(f: Filled) -> String {
    match f {
        Filled::Text(s) => s,
        other => panic!("{other:?}"),
    }
}

fn integer(f: Filled) -> i64 {
    match f {
        Filled::Integer(i) => i,
        other => panic!("{other:?}"),
    }
}

fn json_payload(note: &str) -> Payload {
    let mut t = Template::new(Codec::Json);
    let dto = Membership {
        group: t.text("/group", id_of(1)).unwrap(),
        members: vec![
            t.text("/members/0", id_of(2)).unwrap(),
            t.text("/members/1", id_of(3)).unwrap(),
        ],
        note: note.to_owned(),
        at: t
            .text("/at", sent_at(-5, TimeRendering::Rfc3339Micros))
            .unwrap(),
        millis: t
            .integer("/millis", sent_at(1_000, TimeRendering::EpochMillis))
            .unwrap(),
    };
    t.finish(serde_json::to_vec(&dto).unwrap()).unwrap()
}

fn json_real(note: &str) -> Vec<u8> {
    let id = |i| text(value_of(id_of(i)));
    serde_json::to_vec(&Membership {
        group: id(1),
        members: vec![id(2), id(3)],
        note: note.to_owned(),
        at: text(render_time(SENT, -5, TimeRendering::Rfc3339Micros).unwrap()),
        millis: integer(render_time(SENT, 1_000, TimeRendering::EpochMillis).unwrap()),
    })
    .unwrap()
}

fn value_of(hole: Hole) -> Filled {
    match hole {
        Hole::Rendered {
            of,
            role,
            rendering,
        } => render(NS, &RunSecret::new([7; 32]), &of, &role, rendering),
        _ => unreachable!(),
    }
}

proptest! {
    /// H1, JSON: the splice is the message serialised with its real values,
    /// whatever else the message holds — quotes, escapes, non-ASCII, a value that
    /// looks like a number sentinel inside a string.
    #[test]
    fn a_json_splice_is_the_message_with_its_values(note in "[\"\\\\a-z0-9 é\u{1F600}]{0,12}") {
        let payload = json_payload(&note);
        prop_assert_eq!(payload.holes.len(), 5);
        prop_assert!(payload.holes.iter().all(|s| s.form == Form::JsonToken));
        prop_assert_eq!(fill(&payload, value).unwrap(), json_real(&note));
    }
}

/// H1: the bytes outside the spans are the template's, and filling every span
/// with its own bytes gives the template back: no span is off by one, no token
/// half replaced.
#[test]
fn the_bytes_outside_the_spans_are_the_template_s() {
    let payload = json_payload("5471000000000000004");
    let own = |span: &ascetic_ddd_command_log::record::Span| {
        let at = span.offset as usize;
        let bytes = &payload.bytes[at..at + span.len as usize];
        Ok(Filled::Json(String::from_utf8(bytes.to_vec()).unwrap()))
    };
    assert_eq!(fill(&payload, own).unwrap(), payload.bytes);
    for span in &payload.holes {
        let token = &payload.bytes[span.offset as usize..(span.offset + span.len) as usize];
        assert!(
            token.first() == Some(&b'"') && token.last() == Some(&b'"')
                || token.iter().all(u8::is_ascii_digit),
            "{span:?}: {}",
            String::from_utf8_lossy(token)
        );
    }
}

/// A minimal protobuf writer: enough for a message of bytes, strings, a
/// varint and a fixed64.
fn tag(field: u32, wire: u32, out: &mut Vec<u8>) {
    varint(u64::from(field << 3 | wire), out);
}

fn varint(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn delimited(field: u32, bytes: &[u8], out: &mut Vec<u8>) {
    tag(field, 2, out);
    varint(bytes.len() as u64, out);
    out.extend_from_slice(bytes);
}

/// The host's message: a device id's bytes (1), the group's id as text (2), the
/// envelope's `occurred_at` as a varint (4) that only a decorator can stamp, a
/// time as fixed64 (5).
fn message(device: &[u8], group: &str, occurred_at: u64, observed: i64) -> Vec<u8> {
    let mut out = Vec::new();
    delimited(1, device, &mut out);
    delimited(2, group.as_bytes(), &mut out);
    tag(4, 0, &mut out);
    varint(occurred_at, &mut out);
    tag(5, 1, &mut out);
    out.extend_from_slice(&observed.to_le_bytes());
    out
}

/// The host's decorator: field 4's varint written again, its length changed.
fn decorate(bytes: &[u8], occurred_at: u64) -> Vec<u8> {
    let field4 = bytes.iter().position(|&b| b == 4 << 3).unwrap();
    let end = field4 + 1 + bytes[field4 + 1..].iter().position(|&b| b < 0x80).unwrap() + 1;
    let mut out = bytes[..field4].to_vec();
    tag(4, 0, &mut out);
    varint(occurred_at, &mut out);
    out.extend_from_slice(&bytes[end..]);
    out
}

/// H1, protobuf: raw spans of fixed length, spliced, then a decorator that
/// moves every later byte, give the message the host would have encoded with
/// its real values.
#[test]
fn a_protobuf_splice_survives_the_decorator() {
    let mut t = Template::new(Codec::Protobuf);
    let device_hole = Hole::Rendered {
        of: Of::Subject(device(9)),
        role: "id".into(),
        rendering: Rendering::UuidBytes,
    };
    let device_bytes = t.raw("1", device_hole.clone(), 16).unwrap();
    let group = t.raw("2", id_of(1), 36).unwrap();
    let observed = t
        .raw("5", sent_at(0, TimeRendering::EpochMillis), 8)
        .unwrap();
    // Every sentinel is valid UTF-8, so a prost `String` field takes it.
    let template = message(
        &device_bytes,
        std::str::from_utf8(&group).unwrap(),
        0,
        i64::from_le_bytes(observed.try_into().unwrap()),
    );
    assert!(std::str::from_utf8(&device_bytes).is_ok());
    let payload = t.finish(template).unwrap();
    assert!(payload.holes.iter().all(|s| s.form == Form::Raw));
    let spliced = fill(&payload, value).unwrap();

    let device_id = match value_of(device_hole) {
        Filled::Bytes(b) => b,
        other => panic!("{other:?}"),
    };
    let real = message(
        &device_id,
        &text(value_of(id_of(1))),
        1_763_640_000_123,
        integer(render_time(SENT, 0, TimeRendering::EpochMillis).unwrap()),
    );
    assert_eq!(decorate(&spliced, 1_763_640_000_123), real);
}

/// A sentinel the host did not use, or used twice, is refused, and so is a hole
/// the form cannot carry.
#[test]
fn a_template_refuses_what_it_cannot_place() {
    let mut t = Template::new(Codec::Json);
    let a = t.text("/a", id_of(1)).unwrap();
    let _unused = t.text("/b", id_of(2)).unwrap();
    assert_eq!(
        t.finish(format!(r#"{{"a":"{a}"}}"#).into_bytes()),
        Err(TemplateError::Sentinel {
            field: "/b".into(),
            found: 0
        })
    );

    let mut t = Template::new(Codec::Json);
    let a = t.text("/a", id_of(1)).unwrap();
    assert_eq!(
        t.finish(format!(r#"["{a}","{a}"]"#).into_bytes()),
        Err(TemplateError::Sentinel {
            field: "/a".into(),
            found: 2
        })
    );

    // A number sentinel inside a string or a longer number is not the token.
    let mut t = Template::new(Codec::Json);
    let n = t
        .integer("/n", sent_at(0, TimeRendering::EpochMillis))
        .unwrap();
    let payload = t
        .finish(format!(r#"{{"s":"x{n}","m":{n}1,"n":{n}}}"#).into_bytes())
        .unwrap();
    assert_eq!(
        payload.holes[0].offset as usize,
        format!(r#"{{"s":"x{n}","m":{n}1,"n":"#).len()
    );

    let mut t = Template::new(Codec::Json);
    let bytes_hole = Hole::Rendered {
        of: Of::Subject(device(1)),
        role: "id".into(),
        rendering: Rendering::UuidBytes,
    };
    assert_eq!(
        t.text("/a", bytes_hole.clone()),
        Err(TemplateError::Form { field: "/a".into() })
    );
    assert_eq!(
        t.integer("/a", id_of(1)),
        Err(TemplateError::Form { field: "/a".into() })
    );
    assert_eq!(
        t.text("/a", sent_at(0, TimeRendering::EpochMillis)),
        Err(TemplateError::Form { field: "/a".into() })
    );
    assert_eq!(
        t.raw("1", id_of(1), 36),
        Err(TemplateError::Codec { field: "1".into() })
    );

    let mut t = Template::new(Codec::Protobuf);
    assert_eq!(
        t.text("/a", id_of(1)),
        Err(TemplateError::Codec { field: "/a".into() })
    );
    assert_eq!(
        t.raw("1", bytes_hole, 15),
        Err(TemplateError::Length {
            field: "1".into(),
            expected: 16
        })
    );
}

/// A splice refuses a value its span cannot take.
#[test]
fn a_splice_refuses_what_its_span_cannot_take() {
    let payload = json_payload("n");
    let bytes = |_: &_| Ok(Filled::Bytes(vec![1; 16]));
    assert!(matches!(fill(&payload, bytes), Err(FillError::Form { .. })));
    let not_json = |_: &_| Ok(Filled::Json("{".into()));
    assert!(matches!(
        fill(&payload, not_json),
        Err(FillError::Form { .. })
    ));

    let mut t = Template::new(Codec::Protobuf);
    let s = t.raw("1", id_of(1), 36).unwrap();
    let payload = t.finish(s).unwrap();
    let short = |_: &_| Ok(Filled::Text("too short".into()));
    assert_eq!(
        fill(&payload, short),
        Err(FillError::Length {
            field: "1".into(),
            expected: 36,
            got: 9
        })
    );
}

/// A step can be what a value is rendered of: a message id per send.
#[test]
fn a_step_renders_apart_from_its_subject() {
    let step = StepId {
        subject: device(1),
        label: "managed".into(),
        n: 0,
    };
    let of_step = render(
        NS,
        &RunSecret::new([7; 32]),
        &Of::Step(step),
        "id",
        Rendering::UuidText,
    );
    let of_subject = render(
        NS,
        &RunSecret::new([7; 32]),
        &Of::Subject(device(1)),
        "id",
        Rendering::UuidText,
    );
    assert_ne!(of_step, of_subject);
}

fn is_uuid_v4(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 36
        && b.iter().enumerate().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => *c == b'-',
            14 => *c == b'4',
            19 => matches!(c, b'8' | b'9' | b'a' | b'b'),
            _ => c.is_ascii_hexdigit() && !c.is_ascii_uppercase(),
        })
}

/// Every sentinel is a valid value of the field it stands in: a UUID where a
/// UUID goes, an RFC 3339 time in 2999 where a time goes, hex digits where hex
/// goes, ASCII where bytes go — for 300 holes of a message as for one. The
/// `n`th hole's time is `n` seconds (or microseconds) after the base.
#[test]
fn a_sentinel_is_a_value_of_its_field() {
    let mut t = Template::new(Codec::Json);
    assert!(is_uuid_v4(&t.text("/id", id_of(1)).unwrap()));
    let rfc = TimeRendering::Rfc3339Micros;
    assert_eq!(
        t.text("/at", sent_at(0, rfc)).unwrap(),
        "2999-01-01T00:00:01Z"
    );
    assert_eq!(
        t.text("/at", sent_at(0, rfc)).unwrap(),
        "2999-01-01T00:00:02Z"
    );

    let mut t = Template::new(Codec::Protobuf);
    let uuid = t.raw("1", id_of(1), 36).unwrap();
    assert!(is_uuid_v4(std::str::from_utf8(&uuid).unwrap()));
    let hex_hole = Hole::Rendered {
        of: Of::Subject(device(1)),
        role: "serial".into(),
        rendering: Rendering::Hex { len: 6 },
    };
    let hex = t.raw("2", hex_hole, 12).unwrap();
    assert!(
        hex.iter().all(u8::is_ascii_hexdigit) && hex.len() == 12,
        "{hex:?}"
    );
    assert_eq!(
        t.raw("3", sent_at(0, rfc), 27).unwrap(),
        b"2999-01-01T00:00:00.000002Z"
    );

    let captured = || Hole::Captured {
        subject: device(1),
        slot: "etag".into(),
        from: StepId {
            subject: device(1),
            label: "created".into(),
            n: 0,
        },
    };
    let mut t = Template::new(Codec::Protobuf);
    let sentinels: Vec<Vec<u8>> = (0..300)
        .map(|i| t.raw(&i.to_string(), captured(), 16).unwrap())
        .collect();
    assert!(
        sentinels.iter().all(|s| s.is_ascii()),
        "a raw sentinel is ASCII"
    );
    let distinct: std::collections::BTreeSet<_> = sentinels.iter().collect();
    assert_eq!(distinct.len(), 300);
}

/// Spans that overlap are refused, though each sentinel is found once.
#[test]
fn overlapping_spans_are_refused() {
    let captured = Hole::Captured {
        subject: device(1),
        slot: "etag".into(),
        from: StepId {
            subject: device(1),
            label: "created".into(),
            n: 0,
        },
    };
    let mut t = Template::new(Codec::Protobuf);
    let short = t.raw("1", captured.clone(), 2).unwrap();
    let long = t.raw("2", captured, 5).unwrap();
    assert!(long.starts_with(&short));
    assert_eq!(t.finish(long), Err(TemplateError::Layout));
}

/// The errors say what is wrong, and where.
#[test]
fn errors_name_their_field() {
    let refused = TemplateError::Sentinel {
        field: "/a".into(),
        found: 2,
    };
    assert_eq!(
        refused.to_string(),
        "/a: its sentinel found 2 times, not once"
    );
    let short = FillError::Length {
        field: "1".into(),
        expected: 36,
        got: 9,
    };
    assert_eq!(short.to_string(), "1: 9 bytes for a span of 36");
}
