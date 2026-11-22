//! Small logs built by hand, for the validator's tests.
#![allow(dead_code)]

pub mod corpus;

use std::io::Cursor;

use ascetic_ddd_command_log::record::{
    Action, Bus, Call, Capture, Channel, ClientRetry, Codec, Dep, Deviation, Field, Form, Header,
    Hole, HostCall, Observe, On, Operation, Outcome, Payload, Probe, Profile, Record, Rendering,
    Rest, Safety, Source, Span, StepId, StreamPosition, Subject, Value,
};
use ascetic_ddd_command_log::validate::{Host, Limits, NoHost, Report, Rule, validate};
use ascetic_ddd_command_log::{Reader, Writer};

pub const W: u64 = 1_000;

/// An operation's, a channel's and a host call's timeout in the fixtures.
pub const TIMEOUT: u64 = 10_000_000;

/// The fixture stand's gateway timeout.
pub const ROUTE_TIMEOUT: u64 = 15_000_000;

/// The least a harness resend waits after the answer it follows.
pub const RESEND: u64 = 100;

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
        safety: Safety::Never,
        client: ClientRetry {
            attempts: 1,
            on: vec![],
            backoff: 0,
            retry_after: false,
        },
        not_run: vec![],
        timeout: TIMEOUT,
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
            route_timeout: ROUTE_TIMEOUT,
            problem: "/code".into(),
            correlation: "X-Correlation-Id".into(),
            resend: RESEND,
            answer_cap: 1 << 20,
            idle: 3_600_000_000,
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
        channels: vec![
            Channel {
                name: "c".into(),
                timeout: TIMEOUT,
            },
            Channel {
                name: "device.lifecycle".into(),
                timeout: TIMEOUT,
            },
        ],
        calls: vec![Call {
            name: "noop".into(),
            timeout: TIMEOUT,
            safety: Safety::Idempotent,
        }],
    }
}

/// A step that calls the host, depending on nothing.
pub fn step(id: StepId, at: u64) -> Record {
    Record {
        step: id,
        at,
        think: 0,
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

/// What a step of `action` expects, the plainest answer of its kind: a 200,
/// a delivery, its effect observed; a host call nothing, its port judging it
/// (ADR-0047 decision 15).
pub fn expected(action: &Action) -> Vec<Outcome> {
    match action {
        Action::Rest(_) => vec![Outcome::Response {
            status: 200,
            problem: None,
            body: vec![],
        }],
        Action::Bus(_) => vec![Outcome::Delivered],
        Action::Observe(_) => vec![Outcome::Observed],
        Action::Host(_) => vec![],
    }
}

/// `r` doing `action`, expecting what it plainly does.
pub fn acting(r: Record, action: Action) -> Record {
    Record {
        expect: expected(&action),
        action,
        ..r
    }
}

/// `r` awaiting `step`; awaiting it only sent, a call or a message expects
/// nothing (ADR-0047 decision 6), an observation still its effect.
pub fn after(mut r: Record, step: &StepId, on: On) -> Record {
    r.after.push(Dep::Step {
        step: step.clone(),
        on,
    });
    if on == On::Sent && !matches!(r.action, Action::Observe(_)) {
        r.expect.clear();
    }
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
        expect: vec![Outcome::Delivered],
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

/// The step an observation of `r` awaits: its first step dependency, or `r`
/// itself when it has none (a fixture's shorthand).
fn cause_of(r: &Record) -> StepId {
    r.after
        .iter()
        .find_map(|d| match d {
            Dep::Step { step, .. } => Some(step.clone()),
            Dep::Each { .. } => None,
        })
        .unwrap_or_else(|| r.step.clone())
}

/// An observation of the bus: the fact whose `/cause` is its cause's id.
pub fn observe(r: Record, every: u64, timeout: u64) -> Record {
    let cause = cause_of(&r);
    Record {
        expect: vec![Outcome::Observed],
        action: Action::Observe(Observe {
            cause: cause.clone(),
            probe: Probe::Bus {
                channel: "c".into(),
                key: Value::Literal("k".into()),
                event: "e".into(),
            },
            until: vec![ascetic_ddd_command_log::record::Predicate {
                pointer: "/cause".into(),
                test: ascetic_ddd_command_log::record::Test::Equals(Value::Hole(Hole::Rendered {
                    of: ascetic_ddd_command_log::record::Of::Step(cause),
                    role: "id".into(),
                    rendering: Rendering::UuidText,
                })),
            }],
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

/// What the plan's bound counts at each read (ADR-0047 K1e-2): the line, its
/// worst intended start and end, the steps held and those not ended.
#[derive(Default)]
pub struct Bounds(pub std::sync::Mutex<Vec<[u64; 5]>>);

impl ascetic_ddd_command_log::engine::Progress for Bounds {
    fn bound(&self, line: u64, intended: u64, end: u64, held: u64, pending: u64) {
        self.0
            .lock()
            .unwrap()
            .push([line, intended, end, held, pending]);
    }
}

/// [`run_with`], and what the bound counts at each read.
pub fn run_bounded(
    header: Header,
    records: &[Record],
    host: &impl Host,
    ids: usize,
) -> (Report, Vec<[u64; 5]>) {
    let dir = tempfile::tempdir().unwrap();
    let bounds = std::sync::Arc::new(Bounds::default());
    let limits = Limits {
        memory: ids * 25,
        spill: dir.path().to_owned(),
        kept: 1_000,
        fan_in: 0,
        memory_spill: true,
        progress: Some(ascetic_ddd_command_log::engine::Observer(bounds.clone())),
        hot: 1 << 20,
    };
    let reader = Reader::new(Cursor::new(bytes(header, records))).unwrap();
    let report = validate(reader, host, &limits).unwrap();
    let seen = std::mem::take(&mut *bounds.0.lock().unwrap());
    (report, seen)
}

pub fn run_with(header: Header, records: &[Record], host: &impl Host, ids: usize) -> Report {
    let dir = tempfile::tempdir().unwrap();
    // Memory for `ids` step ids of 25 bytes; stream messages share it.
    let limits = Limits {
        memory: ids * 25,
        spill: dir.path().to_owned(),
        kept: 1_000,
        fan_in: 0,
        memory_spill: true,
        progress: None,
        hot: 1 << 20,
    };
    let reader = Reader::new(Cursor::new(bytes(header, records))).unwrap();
    let report = validate(reader, host, &limits).unwrap();
    assert_eq!(leftover(dir.path()).len(), 0, "the runs are removed");
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
    r.expect = expected(&r.action);
    r
}

/// An observation polling `read` on group `g` every `every` to `timeout`.
pub fn polling(g: u64, n: u32, at: u64, every: u64, timeout: u64) -> Record {
    let mut r = step(id("device", g, "watched", n), at);
    let Action::Rest(rest) = call("read", g, 0, at, 0).action else {
        unreachable!()
    };
    r.action = Action::Observe(Observe {
        cause: cause_of(&r),
        probe: Probe::Rest(rest),
        until: vec![],
        every,
        timeout,
    });
    r.expect = vec![Outcome::Observed];
    r
}

/// What a pass left under a spill directory: every entry but the directory's
/// own sweep lock, which stays.
pub fn leftover(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.file_name().is_some_and(|n| n != ".sweep.lock"))
        .collect()
}

/// The runs under a spill directory, in every pass's directory.
pub fn runs(dir: &std::path::Path) -> usize {
    leftover(dir)
        .iter()
        .filter(|p| p.is_dir())
        .flat_map(|d| std::fs::read_dir(d).unwrap().map(|e| e.unwrap().path()))
        .filter(|p| p.extension().is_some_and(|x| x == "run"))
        .count()
}

/// Limits for a test: the defaults, a spill in memory allowed (the tests' temp
/// directories are on tmpfs).
pub fn limits_in(spill: &std::path::Path) -> Limits {
    Limits {
        memory_spill: true,
        ..Limits::new(spill)
    }
}
