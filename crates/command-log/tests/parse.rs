//! K2b-7e-2b-2c-2 of ADR-0047 (decision 22): parsing `n` bytes holds at most
//! `json::bound(n)`, whatever they are, and at its densest input not far
//! below it; a pointer's tokens at most `json::pointer_bound`. A binary of
//! its own: its allocator counts each thread's bytes, a block moved counted
//! at its new size before its old is let go.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use ascetic_ddd_command_log::json::{
    self, ENTRY, JsonError, LONGEST, NODE, bound, names_bound, pointer_bound, tape, tape_bound,
};
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

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract, passed on: zeroed by the system, its
        // pages untouched until read.
        let p = unsafe { System.alloc_zeroed(layout) };
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
/// growth — at 99 % or more of the tree's part; it has no object, so no
/// names' check (K2b-7e-4b-1).
#[test]
fn the_densest_input_holds_near_its_bound() {
    assert_eq!(NODE, size_of::<json::Json>());
    for k in [17, 257] {
        let doc = forest(k);
        let n = doc.len() as u64;
        let held = parsed(doc.as_bytes());
        let stated = bound(n);
        let tree = stated - names_bound(n);
        eprintln!(
            "MEASURED {k} chains, {n} B: {held} B at the peak, {tree} B the tree's bound ({:.2} %)",
            100.0 * held as f64 / tree as f64
        );
        assert!(held <= tree, "{k} chains: {held} > {tree}");
        assert!(
            held as f64 >= 0.99 * tree as f64,
            "{k} chains: {held} of {tree}"
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
    assert_eq!(bound(0), 2 * NODE as u64 + 8);
    assert_eq!(names_bound(u64::MAX), u64::MAX);
    assert_eq!(tape_bound(u64::MAX), u64::MAX);
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
    // Its first reader: one tape, its entries — the forest has no object —
    // and its pointer's tokens (K2b-7e-4b-2).
    let first = peak_of(|| r.holds(&exists, &fills).unwrap());
    let len = u64::try_from(body.len()).unwrap();
    let entries = ENTRY as u64 * (len / 2 + 1);
    assert!(first >= entries, "{first} B: no tape for its reader");
    assert!(
        first <= entries + pointer_bound(2),
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

/// What [`tape`] holds.
fn taped(bytes: &[u8]) -> u64 {
    peak_of(|| tape(bytes))
}

/// K2b-7e-4b-1 of ADR-0047 (decision 23): a tape holds its entries sized at
/// once from its length — an entry each two bytes and one more, which an
/// array of zeros of odd length fills, its last value at its last slot —,
/// and nothing past them for a document with no object.
#[test]
fn a_tape_holds_its_entries_sized_once() {
    for k in [1usize, 2, 1_000, 1_001] {
        let doc = format!("[{}]", vec!["0"; k].join(","));
        let n = doc.len() as u64;
        let held = taped(doc.as_bytes());
        let entries = ENTRY as u64 * (n / 2 + 1);
        assert_eq!(held, entries, "{k} zeros, {n} B");
        assert!(held <= tape_bound(n));
    }
    let chain = format!("{}1{}", "[".repeat(json::DEPTH), "]".repeat(json::DEPTH));
    let n = chain.len() as u64;
    assert_eq!(taped(chain.as_bytes()), ENTRY as u64 * (n / 2 + 1));
}

/// The names' check holds its stack sized once at the first object — a
/// member each five bytes of what is left and one more, which members named
/// `""` fill, a name twice found only at the close —, in a tape and in a
/// tree alike.
#[test]
fn the_names_check_is_sized_once() {
    for k in [1usize, 2, 999, 1_000] {
        let named_twice = format!("{{{}}}", vec!["\"\":0"; k].join(","));
        let distinct: Vec<String> = (0..k).map(|i| format!("\"{i}\":0")).collect();
        let distinct = format!("{{{}}}", distinct.join(","));
        for doc in [named_twice, distinct] {
            let n = doc.len() as u64;
            let held = taped(doc.as_bytes());
            let stated = ENTRY as u64 * (n / 2 + 1) + names_bound(n);
            assert_eq!(held, stated, "{k} members, {n} B");
            assert_eq!(stated, tape_bound(n));
            assert!(parsed(doc.as_bytes()) <= bound(n));
        }
    }
    // An object first, then the densest arrays: the stack and the tree at
    // once, within the bound.
    let chains: Vec<String> = (0..17).map(|_| chain(json::DEPTH - 1)).collect();
    let mixed = format!("[{{}},{}]", chains.join(","));
    let n = mixed.len() as u64;
    let held = parsed(mixed.as_bytes());
    assert!(held <= bound(n), "{held} > {}", bound(n));
    // The same document with an empty array for its object: the same tree,
    // no stack.
    let alone = parsed(format!("[[],{}]", chains.join(",")).as_bytes());
    let stack = 8 * ((n - 1) / 5 + 1);
    assert_eq!(held, alone + stack, "the stack, {stack} B, not held once");
}

/// A document longer than `LONGEST` is unreadable, and nothing is held for
/// it: neither its entries nor its text checked (K2b-7e-4b-1). Its bytes are
/// zeroed by the system and never touched; skipped where it refuses them.
#[test]
fn a_document_past_the_longest_is_unreadable_and_holds_nothing() {
    let n = usize::try_from(LONGEST).unwrap() + 1;
    let layout = Layout::array::<u8>(n).unwrap();
    // SAFETY: a nonzero size; freed below with the same layout.
    let p = unsafe { std::alloc::alloc_zeroed(layout) };
    if p.is_null() {
        eprintln!("SKIPPED: the system refused {n} zeroed bytes");
        return;
    }
    // SAFETY: `n` bytes allocated and zeroed at `p`, borrowed until freed.
    let bytes = unsafe { std::slice::from_raw_parts(p, n) };
    let mut read = None;
    let held = peak_of(|| {
        read = Some((tape(bytes).err(), json::parse(bytes).err()));
    });
    // SAFETY: allocated above with this layout, no borrow left.
    unsafe { std::alloc::dealloc(p, layout) };
    assert_eq!(read, Some((Some(JsonError::Long), Some(JsonError::Long))));
    assert!(held < 1_024, "{held} B held for a document too long");
}

/// What `f`'s result holds once it is made.
fn retained<T>(f: impl FnOnce() -> T) -> (T, u64) {
    let base = NOW.with(Cell::get);
    let t = f();
    (t, u64::try_from(NOW.with(Cell::get) - base).unwrap())
}

/// K2b-7e-4b-2 of ADR-0047 (decision 23): reading an answer holds within
/// `answer::bound`, and above its parts: a predicate's want alive while its
/// reading builds the tape and its names' check; a problem's code decoded
/// at once, its buffer its length.
#[test]
fn reading_an_answer_holds_within_its_bound() {
    use ascetic_ddd_command_log::answer::{self, Reply, read};
    use ascetic_ddd_command_log::fills::{Fills, Namespace};
    use ascetic_ddd_command_log::record::{Predicate, Test, Value};
    use ascetic_ddd_command_log::render::RunSecret;
    use ascetic_ddd_command_log::shape::Shape;
    let secret = RunSecret::new([0; 32]);
    let fills = Fills {
        namespace: Namespace(0),
        secret: &secret,
        run_start: 0,
        sent: 0,
        captured: &[],
    };
    let members: Vec<String> = (0..10_000).map(|i| format!("\"{i}\":{i}")).collect();
    let doc = format!("{{{}}}", members.join(","));
    let n = doc.len() as u64;
    let numbers: Vec<String> = (0..10_000).map(|i| i.to_string()).collect();
    let literal = format!("[{}]", numbers.join(","));
    let shape = Shape {
        literal_bytes: literal.len() as u64,
        pointer_bytes: 2,
        ..Shape::default()
    };
    let stated = answer::bound(n, &shape, 5);
    let (want, kept) = retained(|| json::parse(literal.as_bytes()).unwrap());
    drop(want);
    // A predicate's want, then the tape its reading builds.
    let reply = Reply {
        status: 200,
        headers: &[],
        body: Some(doc.as_bytes()),
    };
    let r = read(&reply);
    let equals = [Predicate {
        pointer: "/0".to_owned(),
        test: Test::Equals(Value::Literal(literal.clone())),
    }];
    let held = peak_of(|| r.holds(&equals, &fills).unwrap());
    let parts = ENTRY as u64 * (n / 2 + 1) + names_bound(n) + kept;
    eprintln!("MEASURED a want and a tape: {held} B, its parts {parts} B, bound {stated} B");
    assert!(held <= stated, "{held} > {stated}");
    assert!(
        held >= parts,
        "{held} < {parts}: the want or the names unseen"
    );
    // A problem's code as long as the body allows: its tape, then the code
    // sized at once.
    // Its text in many pieces, an escape every other character: a string
    // grown piece by piece would hold more than its length.
    let code = format!("{{\"code\":\"a.{}\"}}", "b\\u0062".repeat(35_001));
    let n = code.len() as u64;
    let problem = [(
        "content-type".to_owned(),
        b"application/problem+json".to_vec(),
    )];
    let reply = Reply {
        status: 404,
        headers: &problem,
        body: Some(code.as_bytes()),
    };
    let r = read(&reply);
    let mut read_code = None;
    let held = peak_of(|| read_code = r.code("/code"));
    let read_code = read_code.unwrap();
    assert_eq!(read_code.capacity(), read_code.len(), "the code grown");
    let stated = answer::bound(n, &Shape::default(), 5);
    assert!(held <= stated, "{held} > {stated}");
    assert!(held >= ENTRY as u64 * (n / 2 + 1) + names_bound(n));
}

/// Each of `answer::bound`'s terms seen where it binds (K2b-7e-4b-2): the
/// names' check beside the tape when the want is small; the code kept while
/// a long pointer's tokens are held, past the names' check; a long string a
/// capture refuses decoded no further than what a use carries.
#[test]
fn each_term_of_the_answer_s_bound_binds() {
    use ascetic_ddd_command_log::answer::{self, Reply, TAKEN, read};
    use ascetic_ddd_command_log::fills::{Fills, Namespace};
    use ascetic_ddd_command_log::record::{Capture, Predicate, Source, Subject, Test, Value};
    use ascetic_ddd_command_log::render::RunSecret;
    use ascetic_ddd_command_log::shape::Shape;
    let secret = RunSecret::new([0; 32]);
    let fills = Fills {
        namespace: Namespace(0),
        secret: &secret,
        run_start: 0,
        sent: 0,
        captured: &[],
    };
    let members: Vec<String> = (0..10_000).map(|i| format!("\"{i}\":{i}")).collect();
    let doc = format!("{{{}}}", members.join(","));
    let n = doc.len() as u64;
    let entries = ENTRY as u64 * (n / 2 + 1);
    // A small want: the names' check binds.
    let shape = Shape {
        literal_bytes: 1,
        pointer_bytes: 2,
        ..Shape::default()
    };
    let reply = Reply {
        status: 200,
        headers: &[],
        body: Some(doc.as_bytes()),
    };
    let r = read(&reply);
    let one = [Predicate {
        pointer: "/0".to_owned(),
        test: Test::Equals(Value::Literal("0".into())),
    }];
    let held = peak_of(|| r.holds(&one, &fills).unwrap());
    let stated = answer::bound(n, &shape, 5);
    assert!(held <= stated, "{held} > {stated}");
    assert!(held >= entries + names_bound(n), "{held}: the names unseen");
    assert!(
        held + n > stated,
        "{held} B against {stated}: the names' term loose"
    );
    // The code kept, then a pointer of empty tokens just past a power of
    // two, its tokens' densest: past the names' check, the bound near.
    let code = format!("{{\"code\":\"a.{}\"}}", "b".repeat(65_536));
    let n = code.len() as u64;
    let pointer = "/".repeat(1_025);
    let shape = Shape {
        pointer_bytes: pointer.len() as u64,
        ..Shape::default()
    };
    let problem = [(
        "content-type".to_owned(),
        b"application/problem+json".to_vec(),
    )];
    let reply = Reply {
        status: 404,
        headers: &problem,
        body: Some(code.as_bytes()),
    };
    let r = read(&reply);
    let far = [Predicate {
        pointer: pointer.clone(),
        test: Test::Exists,
    }];
    let held = peak_of(|| {
        let kept = r.code("/code");
        let found = r.holds(&far, &fills).unwrap();
        (kept, found)
    });
    let stated = answer::bound(n, &shape, 5);
    let entries = ENTRY as u64 * (n / 2 + 1);
    assert!(held <= stated, "{held} > {stated}");
    assert!(
        held > entries + names_bound(n),
        "{held}: the code and the tokens not held past the names' check"
    );
    assert!(
        held + n / 2 > stated,
        "{held} B against {stated}: the code's term loose"
    );
    // A string past what a use carries, refused with nothing decoded: its
    // first run exactly `TAKEN` bytes, escapes after it — a count that
    // stops at `TAKEN` would decode them all.
    let long = format!(
        "{{\"s\":\"{}{}\"}}",
        "A".repeat(TAKEN),
        "\\u0041".repeat(1 << 17)
    );
    let reply = Reply {
        status: 200,
        headers: &[],
        body: Some(long.as_bytes()),
    };
    let r = read(&reply);
    let capture = Capture {
        subject: Subject {
            kind: "device".into(),
            scope: vec![],
            index: 1,
        },
        slot: "s".into(),
        source: Source::Json("/s".into()),
        secret: false,
    };
    // The tape built by a first take, so that the second's peak is its own.
    assert_eq!(r.take(&capture), None);
    let held = peak_of(|| r.take(&capture));
    assert!(
        held <= pointer_bound(2) + TAKEN as u64,
        "{held} B: the string decoded past {TAKEN}"
    );
}
