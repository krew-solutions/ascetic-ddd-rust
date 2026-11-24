//! K1e-2 of ADR-0047: the costs `fits` counts a machine's parts by are upper
//! bounds of what the machine holds. A binary of its own: the allocator counts
//! the whole process, and a test beside it would move its peak.

// The command log's fixture reads its corpus's directory; the lint gate refuses
// files to the machine, not to its tests (ADR-0047 K1e-3).
#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
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

    /// The new size counted before the old is let go: a block moved holds
    /// both at once, and the system's realloc may move it.
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: the caller's contract, passed on.
        let q = unsafe { System.realloc(p, layout, size) };
        if !q.is_null() {
            grew(size);
            NOW.fetch_sub(layout.size(), Relaxed);
        }
        q
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// One plan of `lines` steps a line every `every` µs from `start` — all at
/// one instant when `every` is 0 —, through `sim` with every answer `answer` µs after its send, W `w` µs;
/// each step its own label when `keys`, so that every line is a barrier key
/// of its own; a REST read when `rest`, a host call otherwise. The machine's
/// peak heap, the run, and the log's mean line bytes.
fn costs(
    lines: u64,
    start: u64,
    every: u64,
    answer: u64,
    w: u64,
    keys: bool,
    rest: bool,
) -> (usize, Run, u64) {
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
        if rest {
            call("read", i, 0, start + i * every, 0)
        } else {
            step(id("device", i, &label, 0), start + i * every)
        }
    };
    let bytes = bytes(header.clone(), &(0..lines).map(record).collect::<Vec<_>>());
    let line = (bytes.len() as u64) / lines.max(1);
    let script = move |_: u64, _: u32, _: &Record| Attempt {
        after: None,
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
    let mut over = Vec::new();
    // Each row's counts just past a power of two where a container grows
    // with them — a block moved holds the old and the new at once — or
    // read ahead and released at one instant, a burst of outputs and inputs.
    for (name, lines, start, every, answer, w, keys, rest) in [
        (
            "ended in the window",
            40_000,
            0,
            50,
            100,
            1_000_000,
            false,
            false,
        ),
        (
            "ended in the window, 2^14 + 1",
            40_000,
            0,
            50,
            100,
            819_200,
            false,
            false,
        ),
        ("not ended", 40_000, 0, 50, 500_000, 1_000, false, false),
        ("barrier keys", 40_000, 0, 50, 100, 1_000, true, false),
        (
            "REST reads not ended, 2^13 + 1",
            40_000,
            0,
            50,
            409_600,
            1_000,
            false,
            true,
        ),
        (
            "host calls at one instant",
            32_769,
            10_000,
            0,
            500_000,
            1_000,
            false,
            false,
        ),
        (
            "REST reads at one instant",
            32_769,
            10_000,
            0,
            500_000,
            1_000,
            false,
            true,
        ),
        (
            "REST reads at one instant, at 0",
            32_769,
            0,
            0,
            500_000,
            1_000,
            false,
            true,
        ),
    ] {
        let (peak, ran, line) = costs(lines, start, every, answer, w, keys, rest);
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
        if peak as u64 > needed {
            over.push(format!("{name}: {peak} B held, {needed} counted"));
        }
    }
    assert!(over.is_empty(), "{over:#?}");
}
