//! H2 of ADR-0046: rendered values and times against `golden/render.txt`,
//! which `vectors/render.py` writes outside the crate with faker's independent
//! SipHash and Python's `datetime`.

use ascetic_ddd_command_log::record::{Of, Rendering, StepId, Subject, TimeRendering};
use ascetic_ddd_command_log::render::{FillError, Filled, render, render_time};
use ascetic_ddd_faker::Namespace;

const GOLDEN: &str = include_str!("../golden/render.txt");

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn device(index: u64) -> Subject {
    Subject {
        kind: "device".into(),
        scope: vec![1, 2],
        index,
    }
}

fn text(f: Filled) -> String {
    match f {
        Filled::Text(s) => s,
        other => panic!("{other:?}"),
    }
}

fn bytes(f: Filled) -> Vec<u8> {
    match f {
        Filled::Bytes(b) => b,
        other => panic!("{other:?}"),
    }
}

/// The golden text, as the crate computes it.
fn computed() -> String {
    let ofs = [
        ("device 7 of tenant [1,2]", Of::Subject(device(7))),
        ("device 8 of tenant [1,2]", Of::Subject(device(8))),
        (
            "step managed#0 of device 7",
            Of::Step(StepId {
                subject: device(7),
                label: "managed".into(),
                n: 0,
            }),
        ),
    ];
    let mut out = String::new();
    for ns in [1, 2] {
        let ns_ = Namespace(ns);
        for (name, of) in &ofs {
            for role in ["id", "message"] {
                let line = format!("ns {ns} | {name} | {role} |");
                let uuid = text(render(ns_, of, role, Rendering::UuidText));
                let raw = bytes(render(ns_, of, role, Rendering::UuidBytes));
                out += &format!("{line} uuid {uuid} | bytes {}\n", hex(&raw));
                let h = text(render(ns_, of, role, Rendering::Hex { len: 5 }));
                out += &format!("{line} hex 5 {h}\n");
                let secret = bytes(render(ns_, of, role, Rendering::Secret32));
                out += &format!("{line} secret32 {}\n", hex(&secret));
            }
        }
    }
    // The times are the vectors' own: the calendar's edges, chosen there.
    let times = GOLDEN.lines().filter_map(|l| {
        l.strip_prefix("time ")?
            .split(' ')
            .next()?
            .parse::<i64>()
            .ok()
    });
    for micros in times {
        let at = text(render_time(micros, 0, TimeRendering::Rfc3339Micros).unwrap());
        let millis = match render_time(0, micros, TimeRendering::EpochMillis).unwrap() {
            Filled::Integer(m) => m,
            other => panic!("{other:?}"),
        };
        out += &format!("time {micros} | rfc3339 {at} | millis {millis}\n");
    }
    out
}

/// H2: every rendering, two namespaces, two subjects and a step, two roles, and
/// times across the calendar's edges, as computed outside the crate.
#[test]
fn renderings_match_the_independent_vectors() {
    assert_eq!(computed(), GOLDEN);
}

/// H2: a UUID is one id, as text and as bytes; key material is not one block
/// repeated, nor the hex of the same role.
#[test]
fn renderings_agree_and_stay_apart() {
    let of = Of::Subject(device(7));
    let ns = Namespace(1);
    let uuid = text(render(ns, &of, "id", Rendering::UuidText));
    let raw = bytes(render(ns, &of, "id", Rendering::UuidBytes));
    assert_eq!(uuid.replace('-', ""), hex(&raw));
    let secret = bytes(render(ns, &of, "id", Rendering::Secret32));
    assert_ne!(secret[..16], secret[16..]);
    let h = text(render(ns, &of, "id", Rendering::Hex { len: 32 }));
    assert_ne!(h, hex(&secret));
}

/// Times past the calendar's years, or past an `i64`, are refused.
#[test]
fn a_time_outside_the_calendar_is_refused() {
    let rfc = TimeRendering::Rfc3339Micros;
    assert_eq!(
        render_time(253_402_300_800_000_000, 0, rfc),
        Err(FillError::Time)
    );
    assert_eq!(
        render_time(-62_167_219_200_000_001, 0, rfc),
        Err(FillError::Time)
    );
    assert_eq!(
        render_time(i64::MAX, 1, TimeRendering::EpochMillis),
        Err(FillError::Time)
    );
    assert_eq!(
        render_time(i64::MIN, 0, TimeRendering::EpochMillis),
        Ok(Filled::Integer(i64::MIN.div_euclid(1000)))
    );
}
