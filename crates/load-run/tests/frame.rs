//! K2b-7e-2b-3a-2 of ADR-0047 (decision 22): what the REST carrier holds of
//! an attempt's request beside the shell's share — its copy of the captured
//! values, its frame, the request built of it before hyper takes it — by its
//! record's request bytes by class, within what `AttemptCosts` states. A
//! binary of its own: its allocator counts each thread's bytes, a block moved
//! counted new before old.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use ascetic_ddd_command_log::answer::TAKEN;
use ascetic_ddd_command_log::fills::{Captured, Fills, Namespace};
use ascetic_ddd_command_log::record::{
    Codec, Field, Form, Hole, Operation, Payload, Rest, Span, StepId, Subject, Value,
};
use ascetic_ddd_command_log::render::RunSecret;
use ascetic_ddd_command_log::request;
use ascetic_ddd_load_run::rest::built;

/// The system allocator, counting the bytes each thread holds and their peak.
struct Counting;

thread_local! {
    static NOW: Cell<isize> = const { Cell::new(0) };
    static PEAK: Cell<isize> = const { Cell::new(0) };
}

fn counted(by: isize) {
    let _ = NOW.try_with(|now| {
        let n = now.get().saturating_add(by);
        now.set(n);
        let _ = PEAK.try_with(|peak| peak.set(peak.get().max(n)));
    });
}

fn size(bytes: usize) -> isize {
    isize::try_from(bytes).unwrap_or(isize::MAX)
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

const CORRELATION: &str = "X-Correlation-Id";

fn subject() -> Subject {
    Subject {
        kind: "group".to_owned(),
        scope: vec![],
        index: 1,
    }
}

fn hole() -> Hole {
    Hole::Captured {
        subject: subject(),
        slot: "id".to_owned(),
        from: StepId {
            subject: subject(),
            label: "created".to_owned(),
            n: 0,
        },
    }
}

/// The captured value of `text`, as one JSON string's text.
fn captured(text: &str) -> Vec<Captured> {
    vec![Captured {
        subject: subject(),
        slot: "id".to_owned(),
        value: serde_json::to_vec(text).unwrap(),
    }]
}

fn read_op() -> Operation {
    fixture::header()
        .operations
        .into_iter()
        .find(|o| o.name == "read")
        .unwrap()
}

fn rest(path: Value, headers: Vec<Field>, body: Option<Payload>) -> Rest {
    Rest {
        op: "read".to_owned(),
        caller: None,
        source: 0,
        path: vec![path],
        query: vec![],
        headers,
        body,
    }
}

/// The most REST held on this thread for one attempt's request: its copy of
/// the captured values, its frame, the request built of it.
fn held(rest: &Rest, captured: &[Captured]) -> u64 {
    let secret = RunSecret::new([7; 32]);
    let op = read_op();
    let base = NOW.with(Cell::get);
    PEAK.with(|p| p.set(base));
    let copy = captured.to_vec();
    let fills = Fills {
        namespace: Namespace(1),
        secret: &secret,
        run_start: 0,
        sent: 0,
        captured: &copy,
    };
    let frame = request::frame(&op, rest, &fills, CORRELATION).unwrap();
    assert!(built(
        &frame,
        "stand.test",
        (CORRELATION, "0123456789abcdef"),
        &[]
    ));
    let peak = u64::try_from(PEAK.with(Cell::get) - base).unwrap();
    drop((frame, copy));
    peak
}

fn field(name: &str, value: Value) -> Field {
    Field {
        name: name.to_owned(),
        value,
    }
}

fn body(bytes: Vec<u8>, holes: Vec<Span>) -> Option<Payload> {
    Some(Payload {
        codec: Codec::Json,
        bytes,
        holes,
    })
}

/// A JSON string of `n` bytes, quotes and all.
fn string(n: usize) -> Vec<u8> {
    let mut b = vec![b'a'; n];
    b[0] = b'"';
    b[n - 1] = b'"';
    b
}

/// Each class at its worst — a target of reserved bytes, a hole's value of
/// quotes, fields just past a power of two —: what REST holds of its request
/// past a small one's within what `AttemptCosts` prices it, and not far
/// below; the whole within its price with each attempt's own part.
#[test]
fn each_class_holds_what_rest_prices_it() {
    let costs = attempts();
    let small_rest = rest(Value::Literal("1".into()), vec![], None);
    let small = held(&small_rest, &[]);
    let quotes = captured(&"\"".repeat(TAKEN));
    let rows: Vec<(&str, Rest, Vec<Captured>)> = vec![
        (
            "a target of 16,385 spaces",
            rest(Value::Literal(" ".repeat(16_385)), vec![], None),
            vec![],
        ),
        (
            "a target's hole of TAKEN quotes",
            rest(Value::Hole(hole()), vec![], None),
            quotes.clone(),
        ),
        (
            "a header of 65,537 bytes",
            rest(
                Value::Literal("1".into()),
                vec![field("x-pad", Value::Literal("a".repeat(65_537)))],
                None,
            ),
            vec![],
        ),
        (
            "a header's hole of TAKEN quotes",
            rest(
                Value::Literal("1".into()),
                vec![field("x-pad", Value::Hole(hole()))],
                None,
            ),
            quotes.clone(),
        ),
        (
            "1,025 fields",
            rest(
                Value::Literal("1".into()),
                (0..1_025)
                    .map(|i| field(&format!("x-{i:04}"), Value::Literal("a".into())))
                    .collect(),
                None,
            ),
            vec![],
        ),
        (
            "a body of 65,537 bytes",
            rest(
                Value::Literal("1".into()),
                vec![],
                body(string(65_537), vec![]),
            ),
            vec![],
        ),
        (
            "a body's hole of TAKEN quotes",
            rest(
                Value::Literal("1".into()),
                vec![],
                body(
                    b"[0]".to_vec(),
                    vec![Span {
                        offset: 1,
                        len: 1,
                        field: "/0".to_owned(),
                        hole: hole(),
                        form: Form::JsonToken,
                    }],
                ),
            ),
            quotes,
        ),
    ];
    let priced = |r: &Rest| {
        let shape = shape_of(r);
        let each = costs.each.saturating_mul(shape.records);
        (costs.price(&shape), costs.price(&shape) - each)
    };
    let (_, small_classes) = priced(&small_rest);
    for (name, r, c) in rows {
        let h = held(&r, &c);
        let (price, classes) = priced(&r);
        let past = h - small;
        let stated = classes - small_classes;
        eprintln!(
            "MEASURED {name}: {past} B past a small request's, {stated} B stated by class ({:.1} %); \
             {h} B in all, {price} B priced",
            100.0 * past as f64 / stated as f64
        );
        assert!(h <= price, "{name}: {h} B held, {price} B priced");
        assert!(
            past as f64 >= 0.95 * stated as f64,
            "{name}: {past} B, below 95 % of the {stated} B stated by class"
        );
    }
}

/// REST's coefficients, a plain plan's.
fn attempts() -> ascetic_ddd_load_run::carrier::AttemptCosts {
    use ascetic_ddd_load_run::carrier::{AttemptCosts, StatedBy};
    use ascetic_ddd_load_run::rest::{
        ATTEMPT, BODY, BODY_HOLE, FIELD, HEADER, HEADER_HOLE, HOLE, TARGET, TARGET_HOLE,
    };
    AttemptCosts {
        each: ATTEMPT,
        target: TARGET,
        target_hole: TARGET_HOLE,
        header: HEADER,
        header_hole: HEADER_HOLE,
        field: FIELD,
        body: BODY,
        body_hole: BODY_HOLE,
        hole: HOLE,
        hole_bytes: 1,
        decorations: 0,
        stated_by: StatedBy::Library,
    }
}

/// The shape of a read whose request is `r`.
fn shape_of(r: &Rest) -> ascetic_ddd_command_log::shape::Shape {
    let mut record = fixture::call("read", 1, 0, 0, 0);
    record.action = ascetic_ddd_command_log::record::Action::Rest(r.clone());
    ascetic_ddd_command_log::shape::shape(&record)
}

/// K2b-7e-2b-3b: an attempt's decorations — as a host's decorator gives them,
/// copied into the request — within what REST prices them at the bound the
/// decorator states, and not far below.
#[test]
fn decorations_hold_what_rest_prices_them() {
    use ascetic_ddd_load_run::rest::{FIELD, HEADER};
    let secret = RunSecret::new([7; 32]);
    let op = read_op();
    let r = rest(Value::Literal("1".into()), vec![], None);
    let fills = Fills {
        namespace: Namespace(1),
        secret: &secret,
        run_start: 0,
        sent: 0,
        captured: &[],
    };
    let frame = request::frame(&op, &r, &fills, CORRELATION).unwrap();
    let measure = |decorated: &[(String, String)]| {
        let base = NOW.with(Cell::get);
        PEAK.with(|p| p.set(base));
        let copy = decorated.to_vec();
        assert!(built(
            &frame,
            "stand.test",
            (CORRELATION, "0123456789abcdef"),
            &copy
        ));
        let peak = u64::try_from(PEAK.with(Cell::get) - base).unwrap();
        drop(copy);
        peak
    };
    let none = measure(&[]);
    let decorated: Vec<(String, String)> = (0..4)
        .map(|i| (format!("dpop-{i}"), "a".repeat(8_192)))
        .collect();
    let bytes: u64 = decorated
        .iter()
        .map(|(n, v)| (n.len() + v.len()) as u64)
        .sum();
    let held = measure(&decorated) - none;
    let stated = HEADER * bytes + FIELD * decorated.len() as u64;
    eprintln!(
        "MEASURED 4 decorations of {bytes} B: {held} B held, {stated} B stated ({:.1} %)",
        100.0 * held as f64 / stated as f64
    );
    assert!(held <= stated, "{held} > {stated}");
    assert!(
        held as f64 >= 0.95 * stated as f64,
        "{held} B, below 95 % of {stated}"
    );
}
