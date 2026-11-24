//! K2b-7e-2b-1 of ADR-0047 (decision 22): a record's shape prices what it
//! holds decoded. Over records generated across every field, the bytes a
//! record decoded from its line holds in its `Arc` are exactly `shape::heap`,
//! and at most `shape::bound` of its shape. A binary of its own: the
//! allocator counts the whole process.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex, PoisonError};

use ascetic_ddd_command_log::frame;
use ascetic_ddd_command_log::record::{
    Action, Bus, Caller, Capture, Codec, Dep, Deviation, Field, Form, Hole, HostCall, Measure,
    Observe, Of, On, Outcome, Payload, Predicate, Probe, Record, Rendering, Rest, Source, Span,
    StepId, StreamPosition, Subject, Test, TimeBase, TimeRendering, Value,
};
use ascetic_ddd_command_log::shape::{bound, heap, shape};
use proptest::prelude::*;

/// The system allocator, counting the bytes it holds; a block moved counted
/// at its new size before its old is let go.
struct Counting;

static NOW: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every call goes to the system allocator unchanged; this only counts
// the sizes it was asked for.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract, passed on.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            NOW.fetch_add(layout.size(), Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract, passed on.
        unsafe { System.dealloc(p, layout) };
        NOW.fetch_sub(layout.size(), Relaxed);
    }

    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: the caller's contract, passed on.
        let q = unsafe { System.realloc(p, layout, size) };
        if !q.is_null() {
            NOW.fetch_add(size, Relaxed);
            NOW.fetch_sub(layout.size(), Relaxed);
        }
        q
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// One test measuring at a time: the allocator counts the whole process.
fn alone() -> std::sync::MutexGuard<'static, ()> {
    static ONE: Mutex<()> = Mutex::new(());
    ONE.lock().unwrap_or_else(PoisonError::into_inner)
}

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
    let before = NOW.load(Relaxed);
    let held: Arc<Record> = Arc::new(frame::decode(&line, 2).unwrap());
    let after = NOW.load(Relaxed);
    assert_eq!(*held, *r);
    drop(held);
    after - before
}

/// Decoded from its line, a record holds exactly what `shape::heap` says, and
/// at most what its shape's bound prices — at most three times it. On a thread of its own: a record's
/// strategy nests deep enough to pass a test thread's stack unoptimised.
#[test]
fn a_record_holds_what_its_shape_prices() {
    let _alone = alone();
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
    let _alone = alone();
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
