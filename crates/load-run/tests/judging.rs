//! K2b-7e-2b-2c-3 of ADR-0047 (decision 22): stage 2's judging holds at most
//! what `stages::judging_bound` states — a document at the cap sized at
//! once, the step it judges by, judging it. A binary of its own: its
//! allocator counts each thread's bytes, a block moved counted new before
//! old.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use ascetic_ddd_command_log::record::Header;
use ascetic_ddd_load_run::clock::RealClock;
use ascetic_ddd_load_run::stages::{SHAPES, document, judging, judging_bound};

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

/// A header whose answers are capped at `cap`.
fn capped(cap: u64) -> Header {
    let mut h = fixture::header();
    h.profile.answer_cap = cap;
    h
}

/// Each shape's document at the cap is sized at once: no larger than the cap
/// it was made for.
#[test]
fn a_document_is_sized_at_once() {
    for cap in [2usize, 4_096, 65_537] {
        for shape in SHAPES {
            let (body, _) = document(shape, cap);
            assert!(body.len() <= cap, "{shape:?} at {cap}: {} B", body.len());
            assert!(
                body.capacity() <= cap,
                "{shape:?} at {cap}: a buffer of {} B",
                body.capacity()
            );
        }
    }
}

/// Stage 2's judging at its peak is within its bound, at caps just past a
/// power of two.
#[test]
fn stage_2_s_judging_holds_within_its_bound() {
    let clock = Arc::new(RealClock::start(2_000).unwrap());
    for cap in [4_097u64, 65_537] {
        let header = capped(cap);
        let base = NOW.with(Cell::get);
        PEAK.with(|p| p.set(base));
        let _ = judging(&header, clock.as_ref());
        let peak = u64::try_from(PEAK.with(Cell::get) - base).unwrap();
        let stated = judging_bound(&header);
        eprintln!(
            "MEASURED stage 2 at a cap of {cap}: {peak} B at the peak, {stated} B bound ({:.1} %)",
            100.0 * peak as f64 / stated as f64
        );
        assert!(peak <= stated, "at {cap}: {peak} > {stated}");
        assert!(
            peak > cap,
            "at {cap}: {peak} B, the document's {cap} not seen"
        );
        // Where the cap's terms outweigh the record's, the bound is near what
        // is held: the tape and its names' check at the cap, not a tree
        // (ADR-0047 decision 23, K2b-7e-4b-2).
        if cap > 65_536 {
            assert!(
                peak as f64 >= 0.8 * stated as f64,
                "at {cap}: {peak} B of {stated}"
            );
        }
    }
}
