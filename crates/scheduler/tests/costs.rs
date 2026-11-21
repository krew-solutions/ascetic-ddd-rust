//! K1e-2 of ADR-0047: the costs `fits` counts a machine's parts by are upper
//! bounds of what the machine holds. A binary of its own: the allocator counts
//! the whole process, and a test beside it would move its peak.

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports)]
mod fixture;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use ascetic_ddd_command_log::record::{Header, Record};
use ascetic_ddd_command_log::validate::Peaks;
use ascetic_ddd_scheduler::fits;
use ascetic_ddd_scheduler::machine::Judgement;
use ascetic_ddd_scheduler::sim::{Attempt, Run, run_streaming};
use fixture::*;

/// The system allocator, counting the bytes it holds and their peak.
struct Counting;

static NOW: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn grew(by: usize) {
    let now = NOW.fetch_add(by, Relaxed).saturating_add(by);
    PEAK.fetch_max(now, Relaxed);
}

// SAFETY: every call goes to the system allocator unchanged; this only counts
// the sizes it was asked for.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract, passed on.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            grew(layout.size());
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
            if size >= layout.size() {
                grew(size - layout.size());
            } else {
                NOW.fetch_sub(layout.size() - size, Relaxed);
            }
        }
        q
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// One plan of `lines` steps a line every `every` µs, through `sim` with every
/// answer `answer` µs after its send, W `w` µs; each step its own label when
/// `keys`, so that every line is a barrier key of its own. The machine's peak
/// heap, the run, and the log's mean line bytes.
fn costs(lines: u64, every: u64, answer: u64, w: u64, keys: bool) -> (usize, Run, u64) {
    let header = Header {
        window: w,
        read_ahead: w.min(1_000),
        ..header()
    };
    let record = move |i: u64| {
        let label = if keys {
            format!("k{i}")
        } else {
            "call".to_owned()
        };
        step(id("device", i, &label, 0), i * every)
    };
    let bytes = bytes(header.clone(), &(0..lines).map(record).collect::<Vec<_>>());
    let line = (bytes.len() as u64) / lines.max(1);
    let script = move |_: u64, _: u32, _: &Record| Attempt {
        sent_after: 0,
        answer_after: Some(answer),
        judgement: Judgement::Expected(vec![]),
    };
    let before = NOW.load(Relaxed);
    PEAK.store(before, Relaxed);
    let ran = run_streaming(
        header.clone(),
        &mut (0..lines).map(record),
        header.read_ahead,
        &script,
    )
    .unwrap();
    (PEAK.load(Relaxed) - before, ran, line)
}

/// K1e-2: the costs `fits` counts are upper bounds of what the machine holds:
/// a plan of ended steps the window keeps, one of steps not ended, one of
/// barrier keys — each peak within what `fits::needed` says of its counts.
#[test]
fn the_machine_holds_no_more_than_fits_counts() {
    for (name, lines, every, answer, w, keys) in [
        ("ended in the window", 40_000, 50, 100, 1_000_000, false),
        ("not ended", 40_000, 50, 500_000, 1_000, false),
        ("barrier keys", 40_000, 50, 100, 1_000, true),
    ] {
        let (peak, ran, line) = costs(lines, every, answer, w, keys);
        let peaks = Peaks {
            held: ran.held as u64,
            pending: ran.pending as u64,
            barriers: ran.barriers as u64,
            ..Peaks::default()
        };
        let needed = fits::needed(&peaks, line);
        eprintln!(
            "{name}: peak {peak} B, held {}, pending {}, barriers {}, line {line} B; {} B a held step; fits counts {needed}",
            ran.held,
            ran.pending,
            ran.barriers,
            peak as u64 / (ran.held as u64).max(1)
        );
        assert!(
            peak as u64 <= needed,
            "{name}: {peak} B held, {needed} counted"
        );
    }
}
