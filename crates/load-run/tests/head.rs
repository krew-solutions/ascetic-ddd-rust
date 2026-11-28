//! K2b-7e-2b-2c-4 of ADR-0047 (decision 22): an answer's head copied holds
//! at most `rest::HEAD_COPY` — the heads hyper parses, at most `READ` bytes
//! in at most `FIELDS` fields, the copy's vector sized at once whether a name
//! repeats or not. A binary of its own: its allocator counts each thread's
//! bytes, a block moved counted new before old.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use ascetic_ddd_load_run::rest::{HEAD_COPY, READ, copied};
use http::header::{HeaderName, HeaderValue};

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

/// The most fields hyper parses in a head, its default — not the library's
/// `FIELDS`, which states it: `tests/rest.rs` holds a head of 101 refused.
const HYPER: usize = 100;

/// The status line hyper reads first, and the blank line ending the head.
const FRAMING: usize = "HTTP/1.1 200 X\r\n".len() + "\r\n".len();

/// A head's parts of `fields` fields filling `READ` bytes on the wire, each
/// `name: a`, the bulk in their names — one name for all when `repeated`.
fn parts(fields: usize, repeated: bool) -> http::response::Parts {
    // `: a` and `\r\n`: five bytes beside a name.
    let room = READ - FRAMING;
    let mut b = http::Response::builder().status(200);
    let mut left = room;
    for i in 0..fields {
        let share = if repeated {
            room / fields
        } else {
            left / (fields - i)
        };
        left -= share;
        let name = if repeated {
            format!("x-{}", "a".repeat(share - 7))
        } else {
            format!("x-{i:02}{}", "a".repeat(share - 9))
        };
        b = b.header(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_static("a"),
        );
    }
    let (parts, ()) = b.body(()).unwrap().into_parts();
    assert_eq!(parts.headers.len(), fields);
    let wire: usize = parts
        .headers
        .iter()
        .map(|(n, v)| n.as_str().len() + v.len() + 4)
        .sum::<usize>()
        + FRAMING;
    assert!(wire <= READ, "a head of {wire} B on the wire");
    parts
}

/// The most the copy of `parts` held on this thread, its result alive.
fn peak(parts: &http::response::Parts) -> u64 {
    let base = NOW.with(Cell::get);
    PEAK.with(|p| p.set(base));
    let copy = copied(parts);
    let peak = u64::try_from(PEAK.with(Cell::get) - base).unwrap();
    assert_eq!(copy.fields.len(), parts.headers.len());
    drop(copy);
    peak
}

/// The worst head, `READ` bytes in hyper's most fields: within `HEAD_COPY`,
/// and not far below it.
#[test]
fn the_worst_head_s_copy_holds_what_rest_states() {
    let fields = HYPER;
    for repeated in [false, true] {
        let p = parts(fields, repeated);
        let peak = peak(&p);
        eprintln!(
            "MEASURED a head of {fields} fields (one name: {repeated}): {peak} B of {HEAD_COPY} B \
             ({:.1} %)",
            100.0 * peak as f64 / HEAD_COPY as f64
        );
        assert!(peak <= HEAD_COPY, "{peak} > {HEAD_COPY}");
        assert!(
            peak as f64 >= 0.95 * HEAD_COPY as f64,
            "{peak} B, below 95 % of {HEAD_COPY}"
        );
    }
}

/// One name repeated just past a power of two: the copy's vector is not grown
/// by doubling, so it holds no more than at hyper's most fields.
#[test]
fn a_repeated_name_s_copy_is_sized_at_once() {
    for fields in [33, 65] {
        let p = parts(fields, true);
        let peak = peak(&p);
        eprintln!("MEASURED a head of {fields} fields of one name: {peak} B of {HEAD_COPY} B");
        assert!(peak <= HEAD_COPY, "at {fields}: {peak} > {HEAD_COPY}");
    }
}
