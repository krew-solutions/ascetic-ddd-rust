//! K2b-7e-2b-1 of ADR-0047 (decision 22): a record's shape prices what it
//! holds decoded. Over records generated across every field, the bytes a
//! record decoded from its line holds in its `Arc` are exactly `shape::heap`,
//! and at most `shape::bound` of its shape. A binary of its own: its
//! allocator counts each thread's bytes.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

use ascetic_ddd_command_log::frame;
use ascetic_ddd_command_log::record::{
    Action, Bus, By, Call, Caller, Capture, Channel, ClientRetry, Codec, Dep, Deviation, Field,
    Form, Header, Hole, HostCall, Law, Limit, Measure, Observe, Of, On, Operation, Outcome,
    Payload, Place, Predicate, Probe, Profile, Record, Rendering, Rest, Safety, Source, Span,
    StepId, StreamPosition, Subject, Test, TimeBase, TimeRendering, Value,
};
use ascetic_ddd_command_log::shape::{bound, heap, shape};
use proptest::prelude::*;

/// The system allocator, counting the bytes each thread holds and their peak:
/// a thread's own, so the harness's threads — spawning tests, collecting
/// their output — are not counted in a test's (they were, under load: a
/// record's heap off by ±600 B); a block moved counted at its new size before
/// its old is let go.
struct Counting;

thread_local! {
    /// The bytes this thread allocated less those it let go.
    static NOW: Cell<isize> = const { Cell::new(0) };
    /// The most of [`NOW`] since [`reset_peak`].
    static PEAK: Cell<isize> = const { Cell::new(0) };
}

fn counted(by: isize) {
    // A thread past its locals' end counts nothing: no test measures there.
    let _ = NOW.try_with(|now| {
        let n = now.get().saturating_add(by);
        now.set(n);
        let _ = PEAK.try_with(|peak| peak.set(peak.get().max(n)));
    });
}

fn size(bytes: usize) -> isize {
    isize::try_from(bytes).unwrap_or(isize::MAX)
}

/// The bytes this thread holds.
fn now() -> isize {
    NOW.with(Cell::get)
}

/// This thread's peak, counted from what it holds now.
fn reset_peak() {
    PEAK.with(|peak| peak.set(now()));
}

/// What this thread held at most past `base`, since [`reset_peak`].
fn peak_past(base: isize) -> u64 {
    u64::try_from(PEAK.with(Cell::get) - base).unwrap()
}

/// What this thread holds past `base`.
fn held_past(base: isize) -> u64 {
    u64::try_from(now() - base).unwrap()
}

// SAFETY: every call goes to the system allocator unchanged; this only counts
// the sizes it was asked for.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract, passed on.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            counted(size(layout.size()));
        }
        p
    }

    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract, passed on.
        unsafe { System.dealloc(p, layout) };
        counted(-size(layout.size()));
    }

    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: the caller's contract, passed on.
        let q = unsafe { System.realloc(p, layout, size) };
        if !q.is_null() {
            counted(self::size(size));
            counted(-self::size(layout.size()));
        }
        q
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Strings of every kind serde_json writes: plain, escaped, multi-byte.
fn text() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(String::new()),
        "[a-z]{1,12}",
        "[a-z\"\\\\\\n\u{e9}\u{1f600}]{1,10}",
        "[a-z]{40,90}",
    ]
}

fn bytes() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        Just(Vec::new()),
        prop::collection::vec(any::<u8>(), 1..12),
        prop::collection::vec(any::<u8>(), 60..200),
    ]
}

/// Sequences across serde's growth: none, within its first four slots, past
/// them, just past a doubling.
fn many<T: std::fmt::Debug + Clone, S: Strategy<Value = T>>(
    s: impl Fn() -> S,
) -> impl Strategy<Value = Vec<T>> {
    prop_oneof![
        Just(Vec::new()),
        prop::collection::vec(s(), 1..=4),
        prop::collection::vec(s(), 5..=9),
        prop::collection::vec(s(), 16..=17),
    ]
}

fn subject() -> impl Strategy<Value = Subject> {
    (text(), bytes(), any::<u64>()).prop_map(|(kind, scope, index)| Subject { kind, scope, index })
}

fn step_id() -> impl Strategy<Value = StepId> {
    (subject(), text(), any::<u32>()).prop_map(|(subject, label, n)| StepId { subject, label, n })
}

fn on() -> impl Strategy<Value = On> {
    prop_oneof![Just(On::Sent), Just(On::Expected), Just(On::Settled)]
}

fn hole() -> impl Strategy<Value = Hole> {
    prop_oneof![
        (
            prop_oneof![
                subject().prop_map(Of::Subject),
                step_id().prop_map(Of::Step)
            ],
            text()
        )
            .prop_map(|(of, role)| Hole::Rendered {
                of,
                role,
                rendering: Rendering::UuidText,
            }),
        (subject(), text(), step_id()).prop_map(|(subject, slot, from)| Hole::Captured {
            subject,
            slot,
            from,
        }),
        any::<i64>().prop_map(|offset| Hole::Time {
            base: TimeBase::RunStart,
            offset,
            rendering: TimeRendering::Rfc3339Micros,
        }),
    ]
}

fn value() -> impl Strategy<Value = Value> {
    prop_oneof![
        text().prop_map(Value::Literal),
        hole().prop_map(Value::Hole)
    ]
}

fn field() -> impl Strategy<Value = Field> {
    (text(), value()).prop_map(|(name, value)| Field { name, value })
}

fn span() -> impl Strategy<Value = Span> {
    (any::<u32>(), any::<u32>(), text(), hole()).prop_map(|(offset, len, field, hole)| Span {
        offset,
        len,
        field,
        hole,
        form: Form::Raw,
    })
}

fn payload() -> impl Strategy<Value = Payload> {
    (bytes(), many(span)).prop_map(|(bytes, holes)| Payload {
        codec: Codec::Json,
        bytes,
        holes,
    })
}

fn rest() -> impl Strategy<Value = Rest> {
    let caller =
        (subject(), many(subject)).prop_map(|(principal, names)| Caller { principal, names });
    (
        text(),
        prop::option::of(caller),
        0u32..3,
        many(value),
        many(field),
        many(field),
        prop::option::of(payload()),
    )
        .prop_map(|(op, caller, source, path, query, headers, body)| Rest {
            op,
            caller,
            source,
            path,
            query,
            headers,
            body,
        })
}

fn predicate() -> impl Strategy<Value = Predicate> {
    let test = prop_oneof![
        Just(Test::Exists),
        value().prop_map(Test::Equals),
        value().prop_map(Test::Contains),
    ];
    (text(), test).prop_map(|(pointer, test)| Predicate { pointer, test })
}

fn action() -> impl Strategy<Value = Action> {
    let stream =
        (text(), any::<u32>()).prop_map(|(name, position)| StreamPosition { name, position });
    let bus = (
        text(),
        value(),
        many(field),
        prop::option::of(stream),
        payload(),
    )
        .prop_map(|(channel, key, headers, stream, payload)| Bus {
            channel,
            key,
            headers,
            stream,
            payload,
        });
    let probe = prop_oneof![
        rest().prop_map(Probe::Rest),
        (text(), value(), text()).prop_map(|(channel, key, event)| Probe::Bus {
            channel,
            key,
            event,
        }),
    ];
    let observe = (
        step_id(),
        probe,
        many(predicate),
        any::<u64>(),
        any::<u64>(),
    )
        .prop_map(|(cause, probe, until, every, timeout)| Observe {
            cause,
            probe,
            until,
            every,
            timeout,
        });
    prop_oneof![
        rest().prop_map(Action::Rest),
        bus.prop_map(Action::Bus),
        observe.prop_map(Action::Observe),
        (text(), payload()).prop_map(|(op, args)| Action::Host(HostCall { op, args })),
    ]
}

fn dep() -> impl Strategy<Value = Dep> {
    prop_oneof![
        (step_id(), on()).prop_map(|(step, on)| Dep::Step { step, on }),
        (
            text(),
            bytes(),
            text(),
            any::<u32>(),
            on(),
            prop::option::of(any::<u64>())
        )
            .prop_map(|(kind, scope, label, n, on, count)| Dep::Each {
                kind,
                scope,
                label,
                n,
                on,
                count,
            }),
    ]
}

fn outcome() -> impl Strategy<Value = Outcome> {
    prop_oneof![
        (any::<u16>(), prop::option::of(text()), many(predicate)).prop_map(
            |(status, problem, body)| Outcome::Response {
                status,
                problem,
                body,
            }
        ),
        Just(Outcome::Delivered),
        Just(Outcome::Observed),
    ]
}

fn capture() -> impl Strategy<Value = Capture> {
    let source = prop_oneof![
        text().prop_map(Source::Header),
        text().prop_map(Source::Json),
        Just(Source::Location),
    ];
    (subject(), text(), source, any::<bool>()).prop_map(|(subject, slot, source, secret)| Capture {
        subject,
        slot,
        source,
        secret,
    })
}

fn measure() -> impl Strategy<Value = Measure> {
    (text(), step_id()).prop_map(|(metric, from)| Measure { metric, from })
}

fn record() -> impl Strategy<Value = Record> {
    let deviation = (text(), text()).prop_map(|(rule, reason)| Deviation { rule, reason });
    (
        step_id(),
        any::<u64>(),
        any::<u64>(),
        many(dep),
        action(),
        many(outcome),
        many(capture),
        many(measure),
        prop::option::of(deviation),
    )
        .prop_map(
            |(step, at, think, after, action, expect, capture, measure, deviation)| Record {
                step,
                at,
                think,
                after,
                action,
                expect,
                capture,
                measure,
                deviation,
            },
        )
}

/// The bytes `r`, decoded from its line, holds in its own `Arc`.
fn decoded(r: &Record) -> usize {
    let line = frame::canonical(r).unwrap();
    let before = now();
    let held: Arc<Record> = Arc::new(frame::decode(&line, 2).unwrap());
    let after = held_past(before);
    assert_eq!(*held, *r);
    drop(held);
    usize::try_from(after).unwrap()
}

/// Decoded from its line, a record holds exactly what `shape::heap` says, and
/// at most what its shape's bound prices — at most three times it. On a thread of its own: a record's
/// strategy nests deep enough to pass a test thread's stack unoptimised.
#[test]
fn a_record_holds_what_its_shape_prices() {
    std::thread::Builder::new()
        .stack_size(256 << 20)
        .spawn(|| {
            let mut runner =
                proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(512));
            runner
                .run(&record(), |r| {
                    let held = decoded(&r) as u64;
                    prop_assert_eq!(held, heap(&r));
                    let priced = bound(&shape(&r));
                    prop_assert!(held <= priced, "{} > {}", held, priced);
                    // Not loose past serde's growth at its worst: four slots for
                    // one element bound at four, four for four at ten.
                    prop_assert!(priced <= 3 * held, "{} priced, {} held", priced, held);
                    Ok(())
                })
                .unwrap();
        })
        .unwrap()
        .join()
        .unwrap();
}

/// A record with no sequence is priced exactly: its `Arc`, its strings, its
/// byte strings at base64's estimate — a scope of one byte decoded into
/// three, a payload of four into six.
#[test]
fn a_record_of_no_sequence_is_priced_exactly() {
    let r = Record {
        step: StepId {
            subject: Subject {
                kind: "device".to_owned(),
                scope: vec![7],
                index: 1,
            },
            label: "call".to_owned(),
            n: 0,
        },
        at: 0,
        think: 0,
        after: vec![],
        action: Action::Host(HostCall {
            op: "noop".to_owned(),
            args: Payload {
                codec: Codec::Text,
                bytes: vec![1, 2, 3, 4],
                holes: vec![],
            },
        }),
        expect: vec![],
        capture: vec![],
        measure: vec![],
        deviation: None,
    };
    let held = decoded(&r) as u64;
    assert_eq!(held, heap(&r));
    assert_eq!(held, bound(&shape(&r)));
}

/// Decoding a line holds nothing but its record (K2b-7e-2b-2): its canonical
/// bytes checked as they are written, no copy of the line — the peak while a
/// record of no escaped string is decoded and put in its `Arc` is its heap.
#[test]
fn a_line_decodes_holding_nothing_but_its_record() {
    let mut r = Record {
        step: StepId {
            subject: Subject {
                kind: "device".to_owned(),
                scope: vec![7; 40],
                index: 1,
            },
            label: "call".repeat(200),
            n: 0,
        },
        at: 0,
        think: 0,
        after: vec![],
        action: Action::Host(HostCall {
            op: "noop".to_owned(),
            args: Payload {
                codec: Codec::Text,
                bytes: vec![1; 4_000],
                holes: vec![],
            },
        }),
        expect: vec![],
        capture: vec![],
        measure: vec![],
        deviation: None,
    };
    for _ in 0..2 {
        let line = frame::canonical(&r).unwrap();
        let before = now();
        reset_peak();
        let held: Arc<Record> = Arc::new(frame::decode(&line, 2).unwrap());
        let peak = peak_past(before);
        assert_eq!(peak, heap(&r), "a line of {} bytes", line.len());
        drop(held);
        r.after.push(Dep::Step {
            step: r.step.clone(),
            on: On::Sent,
        });
    }
}

fn names(n: usize, of: &str) -> Vec<String> {
    (0..n).map(|i| format!("{of}-{i}")).collect()
}

/// A header with every sequence at `n` elements, every kind of limit.
fn header_of(n: usize) -> Header {
    let places = [
        By::All,
        By::Source,
        By::At(Place::Path("group".to_owned())),
        By::At(Place::Query("q".to_owned())),
        By::At(Place::Header("X-Tenant".to_owned())),
        By::At(Place::Body("/a/b".to_owned())),
        By::At(Place::Credential("sub".to_owned())),
    ];
    let limit = |i: usize| Limit {
        key: format!("key-{i}"),
        bucket: format!("bucket-{i}"),
        by: places[i % places.len()].clone(),
        requests: 10,
        per_seconds: 1,
        burst: 2,
    };
    let operation = |i: usize| Operation {
        name: format!("operation-{i}"),
        method: "POST".to_owned(),
        path: format!("/groups/{i}/{{group}}"),
        content_type: "application/json".to_owned(),
        decorated: names(n, "decorated"),
        writes: names(n, "etag"),
        safety: Safety::Idempotent,
        client: ClientRetry {
            attempts: 3,
            on: (0..n).map(|k| 500 + k as u16).collect(),
            backoff: 100,
            retry_after: true,
        },
        not_run: names(n, "not-run"),
        timeout: 1_000,
        limits: (0..n).map(limit).collect(),
    };
    Header {
        format: ascetic_ddd_command_log::FORMAT_VERSION,
        algorithm: "test".to_owned(),
        scenario: "a scenario".to_owned(),
        revision: "1".to_owned(),
        profile: Profile {
            name: "policy".to_owned(),
            rendered: names(n, "rendered"),
            captured: names(n, "captured"),
            gapless: names(n, "gapless"),
            sources: 2,
            jitter: 100,
            reserve: 100,
            route_timeout: 30_000_000,
            problem: "/code".to_owned(),
            correlation: "X-Correlation-Id".to_owned(),
            resend: 1_000,
            answer_cap: 1 << 20,
            idle: 3_600_000_000,
        },
        window: 1_000,
        read_ahead: 1_000,
        retained: names(n, "retained"),
        laws: (0..n)
            .map(|i| Law {
                name: format!("law-{i}"),
                law: "a law".repeat(i + 1),
            })
            .collect(),
        operations: (0..n).map(operation).collect(),
        channels: (0..n)
            .map(|i| Channel {
                name: format!("channel-{i}"),
                timeout: 1_000,
            })
            .collect(),
        calls: (0..n)
            .map(|i| Call {
                name: format!("call-{i}"),
                timeout: 1_000,
                safety: Safety::Never,
            })
            .collect(),
    }
}

/// A log's header, decoded from its line and shared in its `Arc`, holds
/// exactly `shape::header` — every sequence empty, within serde's first four
/// slots, past them, just past a doubling; every kind of limit.
#[test]
fn a_header_holds_what_its_walk_counts() {
    for n in [0, 1, 4, 5, 9, 17] {
        let h = header_of(n);
        let line = frame::canonical(&h).unwrap();
        let before = now();
        let held: Arc<Header> = Arc::new(frame::decode(&line, 1).unwrap());
        let after = held_past(before);
        assert_eq!(*held, h);
        drop(held);
        assert_eq!(
            after,
            ascetic_ddd_command_log::shape::header(&h),
            "{n} a sequence"
        );
    }
}

/// K2b-7e-2b-2c-2: a record's largest predicate literal — its expectation's
/// or its observation's `until` — and its longest pointer, a predicate's or a
/// capture's, are its shape's.
#[test]
fn a_record_s_largest_literal_and_longest_pointer() {
    let subject = Subject {
        kind: "device".to_owned(),
        scope: vec![7],
        index: 1,
    };
    let mut r = Record {
        step: StepId {
            subject: subject.clone(),
            label: "read".to_owned(),
            n: 0,
        },
        at: 0,
        think: 0,
        after: vec![],
        action: Action::Host(HostCall {
            op: "read".to_owned(),
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
    };
    r.expect = vec![Outcome::Response {
        status: 200,
        problem: None,
        body: vec![
            Predicate {
                pointer: "/a".into(),
                test: Test::Equals(Value::Literal("\"xy\"".into())),
            },
            Predicate {
                pointer: "/b".into(),
                test: Test::Contains(Value::Literal("[1,2,3]".into())),
            },
        ],
    }];
    r.capture = vec![Capture {
        subject,
        slot: "id".into(),
        source: Source::Json("/a/long/pointer".into()),
        secret: false,
    }];
    let s = shape(&r);
    assert_eq!(s.literal_bytes, 7, "the largest literal, not their sum");
    assert_eq!(s.pointer_bytes, 15, "the capture's pointer, the longest");
}
