//! K2b-7e-2b-2c-2 of ADR-0047 (decision 22): judging an answer holds at most
//! `judge::bound` beside its body — its tree, a pointer's tokens, one value
//! read at a time, what it takes — each term the one that decides a row of
//! its own. A binary of its own: its allocator counts each thread's bytes, a
//! block moved counted at its new size before its old is let go.

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use ascetic_ddd_command_log::answer::{self, Reply, read};
use ascetic_ddd_command_log::fills::{Captured, Fills, Namespace};
use ascetic_ddd_command_log::json;
use ascetic_ddd_command_log::record::{
    Capture, Hole, Outcome, Predicate, Record, Source, Subject, Test, Value,
};
use ascetic_ddd_command_log::render::RunSecret;
use ascetic_ddd_command_log::shape;
use ascetic_ddd_scheduler::judge::{self, Contract, Transport};
use fixture::*;

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

/// The problem pointer the rows judge under.
const PROBLEM: &str = "/code";

/// What reading and judging `body` under `headers` for `record` holds at its
/// peak, `captured` the values its holes take; with its bound at `cap`.
fn judged(
    record: &Record,
    headers: &[(String, Vec<u8>)],
    body: Option<&[u8]>,
    captured: &[Captured],
    cap: u64,
) -> (u64, u64) {
    let secret = RunSecret::new([0; 32]);
    let fills = Fills {
        namespace: Namespace(1),
        secret: &secret,
        run_start: 0,
        sent: 0,
        captured,
    };
    let reply = Reply {
        status: 200,
        headers,
        body,
    };
    let contract = Contract {
        not_run: &[],
        problem: PROBLEM,
    };
    let base = NOW.with(Cell::get);
    PEAK.with(|p| p.set(base));
    let judged = judge::judge(record, contract, &fills, &Transport::Answered(read(&reply)));
    drop(judged.unwrap());
    let peak = u64::try_from(PEAK.with(Cell::get) - base).unwrap();
    let bound = judge::bound(cap, &shape::shape(record), PROBLEM.len() as u64);
    (peak, bound)
}

/// A read expecting a 200 whose body satisfies `predicates`.
fn reading(predicates: Vec<Predicate>) -> Record {
    let mut r = call("read", 1, 0, 0, 0);
    r.expect = vec![Outcome::Response {
        status: 200,
        problem: None,
        body: predicates,
    }];
    r
}

/// `k` chains as deep as an answer nests, in one array: the densest body.
fn forest(k: usize) -> String {
    let chain = format!(
        "{}1{}",
        "[".repeat(json::DEPTH - 1),
        "]".repeat(json::DEPTH - 1)
    );
    format!("[{}]", vec![chain; k].join(","))
}

/// The row a term decides: within the bound, and past it with the term gone.
fn decided(row: &str, (peak, bound): (u64, u64), term: u64) {
    eprintln!(
        "MEASURED {row}: {peak} B at the peak, {bound} B bound ({:.1} %), its term {term} B",
        100.0 * peak as f64 / bound as f64
    );
    assert!(peak <= bound, "{row}: {peak} > {bound}");
    assert!(
        peak > bound - term,
        "{row}: {peak} within {} B, its term not deciding",
        bound - term
    );
}

/// The body's tree and a literal of its size, both at the densest: each
/// half the row.
#[test]
fn a_literal_s_tree_beside_the_body_s() {
    let doc = forest(17);
    let r = reading(vec![Predicate {
        pointer: String::new(),
        test: Test::Equals(Value::Literal(doc.clone())),
    }]);
    let cap = doc.len() as u64;
    let row = judged(&r, &[], Some(doc.as_bytes()), &[], cap);
    decided("a body and its literal", row, json::bound(cap));
}

/// A header's value taken at `VALUE` under names of 8 KiB each: the value
/// and the names decide.
#[test]
fn a_value_taken_at_its_largest() {
    let mut r = reading(vec![]);
    let subject = Subject {
        kind: "k".repeat(8_192),
        scope: vec![7],
        index: 1,
    };
    r.capture = vec![Capture {
        subject,
        slot: "s".repeat(8_192),
        source: Source::Header("ETag".into()),
        secret: false,
    }];
    let headers = vec![("ETag".to_owned(), vec![b'"'; answer::TAKEN])];
    let row = judged(&r, &headers, None, &[], 0);
    decided("a value at VALUE", row, ascetic_ddd_scheduler::fits::VALUE);
    let s = shape::shape(&r);
    decided("its names", row, s.capture_bytes);
}

/// A predicate's hole taking a number of `TAKEN` digits: its value read,
/// copied and read again decides.
#[test]
fn a_hole_s_value_read() {
    let producer = call("create", 1, 0, 0, 0);
    let subject = producer.step.subject.clone();
    let r = reading(vec![Predicate {
        pointer: "/0".into(),
        test: Test::Equals(Value::Hole(Hole::Captured {
            subject: subject.clone(),
            slot: "n".into(),
            from: producer.step.clone(),
        })),
    }]);
    let captured = vec![Captured {
        subject,
        slot: "n".into(),
        value: vec![b'1'; answer::TAKEN],
    }];
    let body = b"[1]";
    let row = judged(&r, &[], Some(body), &captured, body.len() as u64);
    decided("a hole's value", row, answer::HOLE);
}

/// A `Location` of `%22` escapes, its segment decoded into `TAKEN` quotes,
/// then quoted: the decoded segment decides.
#[test]
fn a_location_s_segment_decoded() {
    let mut r = reading(vec![]);
    r.capture = vec![Capture {
        subject: r.step.subject.clone(),
        slot: "at".into(),
        source: Source::Location,
        secret: false,
    }];
    let location = format!("/devices/{}", "%22".repeat(answer::TAKEN));
    let headers = vec![("Location".to_owned(), location.into_bytes())];
    let row = judged(&r, &headers, None, &[], 0);
    decided("a decoded segment", row, answer::DECODE);
}

/// A pointer of 65 empty tokens: its tokens decide.
#[test]
fn a_pointer_s_tokens() {
    let pointer = "/".repeat(65);
    let r = reading(vec![Predicate {
        pointer: pointer.clone(),
        test: Test::Exists,
    }]);
    let body = b"{}";
    let row = judged(&r, &[], Some(body), &[], body.len() as u64);
    decided(
        "a pointer's tokens",
        row,
        json::pointer_bound(pointer.len() as u64),
    );
}

/// A value no take makes is never parsed: a host's array, with or without
/// whitespace before it, is not a scalar; a take's own are.
#[test]
fn only_a_value_a_take_makes_is_parsed() {
    for refused in [
        &b"[[[1]]]"[..],
        b" [1]",
        b"\t{\"a\":1}",
        b"\r\n[1]",
        b"\"\\u0000\"",
        b"\"a\nb\"",
        b"01",
        b"1.",
        b"\"\"",
        b"\".\"",
    ] {
        assert!(
            !answer::is_taken(refused),
            "{:?}",
            String::from_utf8_lossy(refused)
        );
    }
    // A take's quoted text: its escapes, its first and last byte visible.
    assert!(!answer::is_taken(b"\"a\\t\""), "a tab last");
    for taken in [
        &b"\"a b\""[..],
        b"\"\\\"\\\\\\tx\"",
        b"-1.5e+7",
        b"0",
        b"12",
    ] {
        assert!(
            answer::is_taken(taken),
            "{:?}",
            String::from_utf8_lossy(taken)
        );
    }
}

/// A host's array in a predicate's hole, whitespace before it: refused
/// before anything parses it, its tree never built.
#[test]
fn a_host_s_array_is_never_parsed() {
    let producer = call("create", 1, 0, 0, 0);
    let subject = producer.step.subject.clone();
    let r = reading(vec![Predicate {
        pointer: "/0".into(),
        test: Test::Equals(Value::Hole(Hole::Captured {
            subject: subject.clone(),
            slot: "n".into(),
            from: producer.step.clone(),
        })),
    }]);
    let chains = forest(64);
    let captured = vec![Captured {
        subject,
        slot: "n".into(),
        value: format!(" \r\n\t{chains}").into_bytes(),
    }];
    let secret = RunSecret::new([0; 32]);
    let fills = Fills {
        namespace: Namespace(1),
        secret: &secret,
        run_start: 0,
        sent: 0,
        captured: &captured,
    };
    let body = b"[1]";
    let reply = Reply {
        status: 200,
        headers: &[],
        body: Some(body),
    };
    let contract = Contract {
        not_run: &[],
        problem: PROBLEM,
    };
    let base = NOW.with(Cell::get);
    PEAK.with(|p| p.set(base));
    let judged = judge::judge(&r, contract, &fills, &Transport::Answered(read(&reply)));
    let peak = u64::try_from(PEAK.with(Cell::get) - base).unwrap();
    assert!(judged.is_err(), "a hole of an array judged");
    drop(judged);
    assert!(peak < 4_096, "{peak} B: the array parsed");
}
