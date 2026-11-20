//! Small logs built by hand, for the validator's tests.
#![allow(dead_code)]

pub mod corpus;

use std::io::Cursor;

use ascetic_ddd_command_log::record::{
    Action, Bus, Capture, Codec, Dep, Deviation, Field, Form, Header, Hole, HostCall, Observe, On,
    Operation, Payload, Probe, Profile, Record, Release, Rendering, Rest, Retry, Source, Span,
    StepId, StreamPosition, Subject, Value,
};
use ascetic_ddd_command_log::validate::{Host, Limits, NoHost, Report, Rule, validate};
use ascetic_ddd_command_log::{Reader, Writer};

pub const W: u64 = 1_000;

pub fn subject(kind: &str, index: u64) -> Subject {
    Subject {
        kind: kind.into(),
        scope: vec![7],
        index,
    }
}

pub fn id(kind: &str, index: u64, label: &str, n: u32) -> StepId {
    StepId {
        subject: subject(kind, index),
        label: label.into(),
        n,
    }
}

pub fn header() -> Header {
    let op = |name: &str, path: &str, writes: &[&str]| Operation {
        name: name.into(),
        method: "POST".into(),
        path: path.into(),
        content_type: "application/json".into(),
        decorated: vec![],
        writes: writes.iter().map(|s| (*s).to_owned()).collect(),
        retry: Retry::Never,
        limits: vec![],
    };
    Header {
        format: ascetic_ddd_command_log::FORMAT_VERSION,
        algorithm: "test".into(),
        scenario: "fixture".into(),
        revision: "0".into(),
        profile: Profile {
            name: "policy".into(),
            rendered: vec!["device".into()],
            captured: vec!["group".into()],
            gapless: vec!["lifecycle".into()],
            sources: 1,
            jitter: 0,
            reserve: 0,
        },
        window: W,
        read_ahead: W,
        retained: vec!["created".into()],
        laws: vec![],
        operations: vec![
            op("update", "/groups/{group}", &["etag"]),
            op("read", "/groups/{group}", &[]),
            op("list", "/groups", &[]),
        ],
    }
}

/// A step that calls the host, depending on nothing.
pub fn step(id: StepId, at: u64) -> Record {
    Record {
        step: id,
        at,
        release: Release::At,
        after: vec![],
        action: Action::Host(HostCall {
            op: "noop".into(),
            args: Payload {
                codec: Codec::Text,
                bytes: vec![],
                holes: vec![],
            },
        }),
        expect: vec![],
        capture: vec![],
        measure: vec![],
        deviation: None,
    }
}

pub fn after(mut r: Record, step: &StepId, on: On) -> Record {
    r.after.push(Dep::Step {
        step: step.clone(),
        on,
    });
    r
}

pub fn rest(op: &str, path: Vec<Value>) -> Action {
    Action::Rest(Rest {
        op: op.into(),
        caller: None,
        source: 0,
        path,
        query: vec![],
        headers: vec![],
        body: None,
    })
}

/// A REST call of `op` on group `g` whose `If-Match` is the group's ETag as
/// `from` captured it.
pub fn read_etag(op: &str, g: u64, from: &StepId) -> Action {
    Action::Rest(Rest {
        op: op.into(),
        caller: None,
        source: 0,
        path: vec![Value::Literal(g.to_string())],
        query: vec![],
        headers: vec![Field {
            name: "If-Match".into(),
            value: Value::Hole(Hole::Captured {
                subject: subject("group", g),
                slot: "etag".into(),
                from: from.clone(),
            }),
        }],
        body: None,
    })
}

pub fn captures_etag(mut r: Record, g: u64) -> Record {
    r.capture.push(Capture {
        subject: subject("group", g),
        slot: "etag".into(),
        source: Source::Header("ETag".into()),
        secret: false,
    });
    r
}

pub fn message(r: Record, stream: &str, position: u32) -> Record {
    Record {
        action: Action::Bus(Bus {
            channel: "device.lifecycle".into(),
            key: Value::Literal("k".into()),
            headers: vec![],
            stream: Some(StreamPosition {
                name: stream.into(),
                position,
            }),
            payload: Payload {
                codec: Codec::Protobuf,
                bytes: vec![0; 4],
                holes: vec![],
            },
        }),
        ..r
    }
}

pub fn with_payload(r: Record, codec: Codec, bytes: usize, spans: Vec<(u32, u32, Form)>) -> Record {
    let holes = spans
        .into_iter()
        .map(|(offset, len, form)| Span {
            offset,
            len,
            field: "f".into(),
            hole: Hole::Rendered {
                of: ascetic_ddd_command_log::record::Of::Subject(subject("device", 1)),
                role: "serial".into(),
                rendering: Rendering::Hex { len: 2 },
            },
            form,
        })
        .collect();
    Record {
        action: Action::Host(HostCall {
            op: "noop".into(),
            args: Payload {
                codec,
                bytes: vec![b'x'; bytes],
                holes,
            },
        }),
        ..r
    }
}

pub fn observe(r: Record, every: u64, timeout: u64) -> Record {
    Record {
        action: Action::Observe(Observe {
            probe: Probe::Bus {
                channel: "c".into(),
                key: Value::Literal("k".into()),
                event: "e".into(),
            },
            until: vec![],
            every,
            timeout,
        }),
        ..r
    }
}

pub fn deviating(mut r: Record, rule: &str) -> Record {
    r.deviation = Some(Deviation {
        rule: rule.into(),
        reason: "on purpose".into(),
    });
    r
}

pub fn bytes(header: Header, records: &[Record]) -> Vec<u8> {
    let mut w = Writer::new(Vec::new(), header).unwrap();
    for r in records {
        w.step(r.clone()).unwrap();
    }
    w.finish().unwrap().0
}

pub fn run_with(
    header: Header,
    records: &[Record],
    host: &impl Host,
    ids_in_memory: usize,
) -> Report {
    let dir = tempfile::tempdir().unwrap();
    let limits = Limits {
        ids_in_memory,
        spill: dir.path().to_owned(),
        kept: 1_000,
    };
    let reader = Reader::new(Cursor::new(bytes(header, records))).unwrap();
    let report = validate(reader, host, &limits).unwrap();
    assert_eq!(
        std::fs::read_dir(dir.path()).unwrap().count(),
        0,
        "the runs are removed"
    );
    report
}

pub fn run(records: &[Record]) -> Report {
    run_with(header(), records, &NoHost, 1_000)
}

/// The rules a log breaks, each once.
pub fn rules(report: &Report) -> Vec<Rule> {
    let mut r: Vec<Rule> = report.violations.iter().map(|v| v.rule).collect();
    r.dedup();
    r.sort();
    r.dedup();
    r
}

/// A header whose operations on a group (`/groups/{group}`) carry `limits`,
/// with `sources`, `jitter` and `reserve` in the profile.
pub fn limited(
    limits: Vec<ascetic_ddd_command_log::record::Limit>,
    sources: u32,
    jitter: u64,
    reserve: u32,
) -> Header {
    let mut h = header();
    h.profile.sources = sources;
    h.profile.jitter = jitter;
    h.profile.reserve = reserve;
    for op in h
        .operations
        .iter_mut()
        .filter(|o| o.path.contains("{group}"))
    {
        op.limits = limits.clone();
    }
    h
}

pub fn limit(
    bucket: &str,
    by: ascetic_ddd_command_log::record::By,
    requests: u32,
    per_seconds: u32,
    burst: u32,
) -> ascetic_ddd_command_log::record::Limit {
    ascetic_ddd_command_log::record::Limit {
        key: bucket.into(),
        bucket: bucket.into(),
        by,
        requests,
        per_seconds,
        burst,
    }
}

/// A REST call of `op` on group `g` from `source`, its line's `n`th.
pub fn call(op: &str, g: u64, n: u32, at: u64, source: u32) -> Record {
    let mut r = step(id("device", g, op, n), at);
    r.action = Action::Rest(Rest {
        op: op.into(),
        caller: None,
        source,
        path: if op == "list" {
            vec![]
        } else {
            vec![Value::Literal(g.to_string())]
        },
        query: vec![],
        headers: vec![],
        body: None,
    });
    r
}

/// An observation polling `read` on group `g` every `every` to `timeout`.
pub fn polling(g: u64, n: u32, at: u64, every: u64, timeout: u64) -> Record {
    let mut r = step(id("device", g, "watched", n), at);
    let Action::Rest(rest) = call("read", g, 0, at, 0).action else {
        unreachable!()
    };
    r.action = Action::Observe(Observe {
        probe: Probe::Rest(rest),
        until: vec![],
        every,
        timeout,
    });
    r
}
