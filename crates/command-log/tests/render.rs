//! H2 of ADR-0046: rendered values and times against `golden/render.txt`,
//! which `vectors/render.py` writes outside the crate with faker's independent
//! SipHash, Python's `hmac` and its `datetime`; key material from the run's
//! secret, not the namespace, and correlation ids (ADR-0047 decision 15).

use ascetic_ddd_command_log::record::{Of, Rendering, StepId, Subject, TimeRendering};
use ascetic_ddd_command_log::render::{
    FillError, Filled, RunSecret, correlation, render, render_time,
};
use ascetic_ddd_faker::Namespace;

const GOLDEN: &str = include_str!("../golden/render.txt");

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The vectors' two secrets.
fn secret(name: &str) -> RunSecret {
    let first: u8 = if name == "A" { 0 } else { 32 };
    RunSecret::new(std::array::from_fn(|i| first + i as u8))
}

fn managed() -> StepId {
    StepId {
        subject: device(7),
        label: "managed".into(),
        n: 0,
    }
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
        ("step managed#0 of device 7", Of::Step(managed())),
    ];
    let mut out = String::new();
    let a = secret("A");
    for ns in [1, 2] {
        let ns_ = Namespace(ns);
        for (name, of) in &ofs {
            for role in ["id", "message"] {
                let line = format!("ns {ns} | {name} | {role} |");
                let uuid = text(render(ns_, &a, of, role, Rendering::UuidText));
                let raw = bytes(render(ns_, &a, of, role, Rendering::UuidBytes));
                out += &format!("{line} uuid {uuid} | bytes {}\n", hex(&raw));
                let h = text(render(ns_, &a, of, role, Rendering::Hex { len: 5 }));
                out += &format!("{line} hex 5 {h}\n");
            }
        }
    }
    for s in ["A", "B"] {
        for (name, of) in &ofs {
            for role in ["id", "message"] {
                let k = bytes(render(
                    Namespace(1),
                    &secret(s),
                    of,
                    role,
                    Rendering::Secret32,
                ));
                out += &format!("secret {s} | {name} | {role} | secret32 {}\n", hex(&k));
            }
        }
    }
    for ns in [1, 2] {
        for attempt in [1, 2] {
            let c = correlation(Namespace(ns), &managed(), attempt);
            out += &format!(
                "correlation ns {ns} | step managed#0 of device 7 | attempt {attempt} | {c}\n"
            );
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
    let a = secret("A");
    let uuid = text(render(ns, &a, &of, "id", Rendering::UuidText));
    let raw = bytes(render(ns, &a, &of, "id", Rendering::UuidBytes));
    assert_eq!(uuid.replace('-', ""), hex(&raw));
    let key = bytes(render(ns, &a, &of, "id", Rendering::Secret32));
    assert_ne!(key[..16], key[16..]);
    let h = text(render(ns, &a, &of, "id", Rendering::Hex { len: 32 }));
    assert_ne!(h, hex(&key));
}

/// Decision 15: key material changes with the run's secret and not with the
/// namespace, which a run's evidence names; public renderings the other way
/// round; and no `Debug` shows a secret or a key.
#[test]
fn key_material_is_the_run_secret_s_not_the_namespace_s() {
    let of = Of::Subject(device(7));
    let (a, b) = (secret("A"), secret("B"));
    let key = |ns, s: &RunSecret| bytes(render(Namespace(ns), s, &of, "dpop", Rendering::Secret32));
    assert_eq!(key(1, &a), key(2, &a));
    assert_ne!(key(1, &a), key(1, &b));
    let id = |ns, s: &RunSecret| text(render(Namespace(ns), s, &of, "id", Rendering::UuidText));
    assert_eq!(id(1, &a), id(1, &b));
    assert_ne!(id(1, &a), id(2, &a));
    let shown = format!(
        "{:?} {:?}",
        a,
        render(Namespace(1), &a, &of, "dpop", Rendering::Secret32)
    );
    for planted in [
        hex(&key(1, &a)),
        hex(&std::array::from_fn::<u8, 32, _>(|i| i as u8)),
    ] {
        assert!(!shown.contains(&planted[..8]), "{shown}");
    }
    assert_eq!(shown, "RunSecret(..) Bytes(32 bytes)");
}

/// Decision 15: a correlation id is one an attempt, and no planned role's
/// rendering of the step is it.
#[test]
fn a_correlation_id_is_its_attempt_s_and_no_role_s() {
    let ns = Namespace(1);
    let (one, two) = (
        correlation(ns, &managed(), 1),
        correlation(ns, &managed(), 2),
    );
    assert_ne!(one, two);
    assert_eq!(one.len(), 32);
    let a = secret("A");
    for role in ["correlation", "correlation/1", "id", "1"] {
        let h = text(render(
            ns,
            &a,
            &Of::Step(managed()),
            role,
            Rendering::Hex { len: 16 },
        ));
        assert_ne!(h, one, "{role}");
    }
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
