//! K2b-7e-2b-2c-1 of ADR-0047 (decision 22): a source's runtime holds what
//! `Runtimes::own` states — `RUNTIME`, and its driver's events at `EVENT`
//! each, as built — its events exactly, its own within 95 % of the stated.
//! A binary of its own and one test: its allocator counts the whole process,
//! the sources' threads with it.
#![cfg(target_os = "linux")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering::SeqCst};

use ascetic_ddd_load_run::live::{Live, events_for};
use ascetic_ddd_load_run::run::{EVENT, RUNTIME, Runtimes};

/// The system allocator, counting the bytes the process holds.
struct Counting;

static NOW: AtomicIsize = AtomicIsize::new(0);

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
            NOW.fetch_add(size(layout.size()), SeqCst);
        }
        p
    }

    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract, passed on.
        unsafe { System.dealloc(p, layout) };
        NOW.fetch_sub(size(layout.size()), SeqCst);
    }

    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: the caller's contract, passed on.
        let q = unsafe { System.realloc(p, layout, size) };
        if !q.is_null() {
            NOW.fetch_add(self::size(size) - self::size(layout.size()), SeqCst);
        }
        q
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Rounds a wait takes at most: a heap that never stands still is refused,
/// not waited on.
const ROUNDS: usize = 20_000;

/// The bytes held once the heap stands still for a while.
fn still() -> isize {
    let mut calm = 0;
    for _ in 0..ROUNDS {
        let before = NOW.load(SeqCst);
        for _ in 0..200 {
            std::thread::yield_now();
        }
        if NOW.load(SeqCst) == before {
            calm += 1;
            if calm == 20 {
                return before;
            }
        } else {
            calm = 0;
        }
    }
    panic!("the heap never stood still");
}

/// What runtimes for `caps` hold, built and their threads parked, past what
/// was held before; with what their `Runtimes` states of each.
fn built(caps: &[usize]) -> (u64, Vec<Option<u64>>) {
    let base = still();
    let live = Live::for_caps(caps).unwrap();
    let held = u64::try_from(still() - base).unwrap();
    let own = (0..caps.len())
        .map(|s| live.runtimes().own(u32::try_from(s).unwrap()))
        .collect();
    drop(live);
    (held, own)
}

/// Runtimes for 1 and 9 sources of caps 0 and 4,096: their events exactly
/// `EVENT` each, as `Runtimes::own` states them; a source's own within what
/// it states at 95 % or more; one charged for each runtime built; a test's
/// runtimes and their events as built.
#[test]
fn a_source_s_runtime_holds_what_it_states() {
    let event = EVENT.expect("an event's size on Linux");
    // The process's own, once: tokio's and std's first use.
    let _ = built(&[0]);
    let (a, own_a) = built(&[0]);
    let (b, own_b) = built(&[0; 9]);
    let (c, _) = built(&[4_096]);
    let (d, own_d) = built(&[4_096; 9]);
    eprintln!("MEASURED runtimes: 1 at cap 0 {a} B, 9 {b} B; 1 at cap 4096 {c} B, 9 {d} B");
    // The events: exactly an event each, as built.
    assert_eq!(c - a, 4_096 * event, "one runtime's events");
    assert_eq!(d - b, 9 * 4_096 * event, "nine runtimes' events");
    // A source's own: the slope from one runtime to nine.
    let each = (b - a) / 8;
    let events = events_for(&[0]).unwrap()[0] as u64;
    let stated = RUNTIME + events * event;
    eprintln!(
        "MEASURED a source's runtime: {each} B, stated {stated} B ({:.1} %)",
        100.0 * each as f64 / stated as f64
    );
    assert!(each <= stated, "{each} B a source, {stated} B stated");
    assert!(
        each as f64 >= 0.95 * stated as f64,
        "{each} B a source, {stated} B stated"
    );
    // `own` states each runtime built.
    assert_eq!(own_a, vec![Some(stated)]);
    assert_eq!(own_b, vec![Some(stated); 9]);
    assert_eq!(own_d, vec![Some(RUNTIME + (4_096 + events) * event); 9]);
    // A test's runtimes are priced by no one; a runtime of no event is
    // built, and recorded, with one.
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let assumed = Runtimes::assumed(vec![rt.handle().clone()], 1_024);
    assert_eq!(assumed.own(0), None);
    let one = Live::start(1, 0).unwrap();
    assert_eq!(one.runtimes().events(0), Some(1));
}
