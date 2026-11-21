//! Decision 16's bound, executed: the validator's peak heap does not grow with
//! the plan's subjects or steps at one planned rate — eight times the devices
//! over eight times the time hold what one time holds. A binary of its own:
//! the allocator counts the whole process.

mod fixture;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use ascetic_ddd_command_log::record::{On, Record};
use ascetic_ddd_command_log::validate::{Limits, NoHost, validate};
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

/// `devices` managed a millisecond apart, each then telling three facts two
/// milliseconds apart on a gapless stream, each awaiting the one before; W ten
/// milliseconds. The planned rate is the same at any count.
fn plan(devices: u64) -> Vec<u8> {
    let mut records: Vec<Record> = Vec::new();
    for d in 0..devices {
        let at = d * 1_000;
        let managed = step(id("device", d, "managed", 0), at);
        let mut before = managed.step.clone();
        records.push(managed);
        for n in 0..3u32 {
            let fact = after(
                message(
                    step(id("device", d, "fact", n), at + 2_000 * u64::from(n + 1)),
                    "lifecycle",
                    n + 1,
                ),
                &before,
                On::Expected,
            );
            before = fact.step.clone();
            records.push(fact);
        }
    }
    records.sort_by(|a, b| (a.at, &a.step).cmp(&(b.at, &b.step)));
    let header = ascetic_ddd_command_log::record::Header {
        window: 10_000,
        ..header()
    };
    bytes(header, &records)
}

/// The peak heap of a validation of `log` beyond what was held before it.
fn peak(log: &[u8], spill: &std::path::Path) -> usize {
    let limits = Limits {
        memory: 256 << 10,
        memory_spill: true,
        ..Limits::new(spill)
    };
    let reader = ascetic_ddd_command_log::Reader::new(std::io::Cursor::new(log)).unwrap();
    let before = NOW.load(Relaxed);
    PEAK.store(before, Relaxed);
    let report = validate(reader, &NoHost, &limits).unwrap();
    let most = PEAK.load(Relaxed).saturating_sub(before);
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    assert!(report.peaks.completions < 64, "{:?}", report.peaks);
    most
}

#[test]
fn the_validator_s_heap_does_not_grow_with_the_plan() {
    let dir = tempfile::tempdir().unwrap();
    let small = plan(2_000);
    let large = plan(16_000);
    let (one, eight) = (peak(&small, dir.path()), peak(&large, dir.path()));
    // Both past the memory, so both spill; eight times the plan, within a
    // quarter of the memory of the same peak.
    eprintln!("heap: {one} at 8·10³ steps, {eight} at 6.4·10⁴");
    assert!(one > 256 << 10, "{one}: nothing spilled");
    assert!(
        eight <= one + (256 << 10),
        "{one} bytes at 2·10⁴ steps, {eight} at 1.6·10⁵"
    );
}
