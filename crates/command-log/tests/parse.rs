//! K2b-7e-2b-2c-2 of ADR-0047 (decision 22): parsing `n` bytes holds at most
//! `json::bound(n)`, whatever they are, and at its densest input not far
//! below it; a pointer's tokens at most `json::pointer_bound`. A binary of
//! its own: its allocator counts each thread's bytes, a block moved counted
//! at its new size before its old is let go.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use ascetic_ddd_command_log::json::{self, NODE, bound, pointer_bound};
use proptest::prelude::*;

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

/// The most `f` held on this thread at once past what it held before.
fn peak_of<T>(f: impl FnOnce() -> T) -> u64 {
    let base = NOW.with(Cell::get);
    PEAK.with(|p| p.set(base));
    drop(f());
    u64::try_from(PEAK.with(Cell::get) - base).unwrap()
}

/// What parsing `bytes` holds at its peak.
fn parsed(bytes: &[u8]) -> u64 {
    peak_of(|| json::parse(bytes))
}

/// A chain of single-element arrays `depth` deep around a `1`.
fn chain(depth: usize) -> String {
    format!("{}1{}", "[".repeat(depth), "]".repeat(depth))
}

/// `k` chains as deep as an answer nests, in one array: the densest input.
fn forest(k: usize) -> String {
    let chains: Vec<String> = (0..k).map(|_| chain(json::DEPTH - 1)).collect();
    format!("[{}]", chains.join(","))
}

/// The densest input holds what its bound states, near enough: a forest of
/// chains just past a power of two — its outer array moved at its last
/// growth — at 99 % or more.
#[test]
fn the_densest_input_holds_near_its_bound() {
    assert_eq!(NODE, size_of::<json::Json>());
    for k in [17, 257] {
        let doc = forest(k);
        let n = doc.len() as u64;
        let held = parsed(doc.as_bytes());
        let stated = bound(n);
        eprintln!(
            "MEASURED {k} chains, {n} B: {held} B at the peak, {stated} B bound ({:.2} %)",
            100.0 * held as f64 / stated as f64
        );
        assert!(held <= stated, "{k} chains: {held} > {stated}");
        assert!(
            held as f64 >= 0.99 * stated as f64,
            "{k} chains: {held} of {stated}"
        );
    }
}

/// The bound's added slot: an input cut inside its densest value holds past
/// two nodes a byte — `[1` its four slots and its number on two bytes.
#[test]
fn a_cut_input_holds_within_its_bound() {
    let mut past = 0;
    for doc in ["[0".to_owned(), "[1".to_owned()]
        .into_iter()
        .chain((1..json::DEPTH).map(|k| format!("{}1{}", "[".repeat(k), "]".repeat(k - 1))))
    {
        let n = doc.len() as u64;
        let held = parsed(doc.as_bytes());
        assert!(held <= bound(n), "{doc:?}: {held} > {}", bound(n));
        past += usize::from(held > 2 * NODE as u64 * n);
    }
    assert!(
        past > 0,
        "no cut input past two nodes a byte: the added slot unexercised"
    );
}

/// Objects nested one in another, as deep as an answer nests: within the
/// bound.
#[test]
fn nested_objects_hold_within_their_bound() {
    let depth = json::DEPTH - 1;
    let doc = format!("{}1{}", "{\"\":".repeat(depth), "}".repeat(depth));
    let n = doc.len() as u64;
    let held = parsed(doc.as_bytes());
    eprintln!(
        "MEASURED nested objects, {n} B: {held} B, bound {}",
        bound(n)
    );
    assert!(held <= bound(n));
}

/// A JSON-like text: arrays, objects, strings with escapes, numbers, nested.
fn document() -> impl Strategy<Value = String> {
    let leaf = prop_oneof![
        Just("null".to_owned()),
        Just("true".to_owned()),
        "-?[0-9]{1,6}(\\.[0-9]{1,3})?([eE][+-]?[0-9]{1,2})?".prop_map(|s| s),
        "[a-z\\\\\"]{0,8}"
            .prop_map(|s| format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))),
    ];
    leaf.prop_recursive(8, 256, 8, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..8).prop_map(|v| format!("[{}]", v.join(","))),
            prop::collection::vec(("[a-z]{0,4}", inner), 0..6).prop_map(|v| {
                let members: Vec<String> =
                    v.into_iter().map(|(k, v)| format!("\"{k}\":{v}")).collect();
                format!("{{{}}}", members.join(","))
            }),
        ]
    })
}

proptest! {
    // No seed kept: a mutant's failure would make the property red
    // under every later run.
    #![proptest_config(ProptestConfig {
        failure_persistence: None,
        ..ProptestConfig::with_cases(512)
    })]

    /// Any bytes, and every prefix of a document — valid, cut or not JSON —
    /// hold within the bound of their length.
    #[test]
    fn any_input_holds_within_its_bound(bytes in prop::collection::vec(any::<u8>(), 0..256), doc in document()) {
        let held = parsed(&bytes);
        prop_assert!(held <= bound(bytes.len() as u64), "{} B: {}", bytes.len(), held);
        for end in 0..=doc.len() {
            let prefix = &doc.as_bytes()[..end];
            let held = parsed(prefix);
            prop_assert!(held <= bound(end as u64), "{:?}: {} > {}", String::from_utf8_lossy(prefix), held, bound(end as u64));
        }
    }
}

/// A bound past the address space saturates; nothing parsed holds nothing
/// but its slot.
#[test]
fn the_bound_saturates() {
    assert_eq!(bound(u64::MAX), u64::MAX);
    assert_eq!(bound(0), 2 * NODE as u64);
    assert_eq!(pointer_bound(u64::MAX), u64::MAX);
}

/// A pointer's tokens hold within their bound: 65 empty tokens — its vector
/// moved just past a power of two — at 95 % or more; tokens of a byte each.
#[test]
fn a_pointer_s_tokens_hold_within_their_bound() {
    let empty = "/".repeat(65);
    let held = peak_of(|| json::tokens(&empty));
    let stated = pointer_bound(empty.len() as u64);
    eprintln!("MEASURED 65 empty tokens: {held} B, bound {stated} B");
    assert!(
        held <= stated && held as f64 >= 0.95 * stated as f64,
        "{held} of {stated}"
    );
    for p in ["/a".repeat(65), "/~0~1".repeat(33), "/".to_owned()] {
        let held = peak_of(|| json::tokens(&p));
        assert!(held <= pointer_bound(p.len() as u64), "{p:?}: {held}");
    }
}

/// K2b-7e-4a of ADR-0047 (decision 23): a body no one reads is never parsed —
/// an answer read for its status, its problem's code below 400 and no
/// predicate, holds no tree —; its first reader parses it once, within its
/// bound; a second reader parses nothing more; a problem's code at 400 or
/// past it reads the body.
#[test]
fn a_body_is_parsed_on_its_first_reader_alone() {
    use ascetic_ddd_command_log::answer::{Reply, read};
    use ascetic_ddd_command_log::fills::{Fills, Namespace};
    use ascetic_ddd_command_log::record::{Predicate, Test};
    use ascetic_ddd_command_log::render::RunSecret;
    let doc = forest(65);
    let body = doc.as_bytes();
    let secret = RunSecret::new([0; 32]);
    let fills = Fills {
        namespace: Namespace(0),
        secret: &secret,
        run_start: 0,
        sent: 0,
        captured: &[],
    };
    let problem = [(
        "content-type".to_owned(),
        b"application/problem+json".to_vec(),
    )];
    let exists = [Predicate {
        pointer: "/0".to_owned(),
        test: Test::Exists,
    }];
    // Read for its status alone, and a code below 400, and no predicate.
    let ok = Reply {
        status: 200,
        headers: &problem,
        body: Some(body),
    };
    let r = read(&ok);
    let none = peak_of(|| (r.status(), r.code("/code"), r.holds(&[], &fills).unwrap()));
    assert!(none < 64, "{none} B held with nothing reading the body");
    // Its first reader: one tree, within its bound.
    let first = peak_of(|| r.holds(&exists, &fills).unwrap());
    let len = u64::try_from(body.len()).unwrap();
    assert!(first > len, "{first} B: no tree for its reader");
    assert!(
        first <= bound(len) + pointer_bound(2),
        "{first} B past its bound"
    );
    // A second reader: the tree kept, none built again.
    let again = peak_of(|| r.holds(&exists, &fills).unwrap());
    assert!(again < 1_024, "{again} B: the body parsed again");
    // A problem's code at 400 or past it: the body read.
    let refused = Reply {
        status: 404,
        headers: &problem,
        body: Some(br#"{"code":"route.not_found"}"#),
    };
    let r = read(&refused);
    assert_eq!(r.code("/code").as_deref(), Some("route.not_found"));
}
