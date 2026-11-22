//! K1-8 of ADR-0047, its heap: the machine's state does not grow with the
//! plan's subjects or its length at one planned rate. A binary of its own: the
//! allocator counts the whole process.
//!
//! The plan: eight subjects live at a time, each through `cycles` cycles of an
//! update capturing its ETag, a read sending it, and a call awaiting the read
//! settled — the next update awaits the last one and its read settled — then
//! retired for the next eight. A share of updates answer without the ETag or
//! refused, reads and calls unknown: readers skipped for a missing value or a
//! failed producer, calls resent. The lines stream into `sim`, which keeps no
//! journal: what the allocator sees grow is the machine's.

// The command log's fixture reads its corpus's directory; the lint gate refuses
// files to the machine, not to its tests (ADR-0047 K1e-3).
#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use ascetic_ddd_command_log::record::{Header, On, Record, Value};
use ascetic_ddd_command_log::validate::NoHost;
use ascetic_ddd_faker::{Key, Seed};
use ascetic_ddd_scheduler::machine::{Captured, Judgement};
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

/// A line every 125 µs; W 20 ms, beyond the farthest dependency.
const EVERY: u64 = 125;
/// Subjects live at once.
const LIVE: u64 = 8;

fn plan_header() -> Header {
    Header {
        window: 20_000,
        ..header()
    }
}

/// `lines` steps: eight subjects at a time through `cycles` cycles, phase by
/// phase — the eight updates, the eight reads, the eight calls — then the
/// next eight subjects.
fn plan(cycles: u32, lines: u64) -> impl Iterator<Item = Record> {
    (0..lines).map(move |i| {
        let at = i * EVERY;
        let (round, within) = (i / (3 * LIVE), i % (3 * LIVE));
        let (phase, slot) = (within / LIVE, within % LIVE);
        let cycle = (round % u64::from(cycles)) as u32;
        let s = (round / u64::from(cycles)) * LIVE + slot;
        let update = id("group", s, "update", cycle);
        let read = id("group", s, "read", cycle);
        match phase {
            0 => {
                let mut r = captures_etag(
                    Record {
                        action: rest("update", vec![Value::Literal(s.to_string())]),
                        ..step(update, at)
                    },
                    s,
                );
                if cycle > 0 {
                    r = after(r, &id("group", s, "update", cycle - 1), On::Settled);
                    r = after(r, &id("group", s, "read", cycle - 1), On::Settled);
                }
                r
            }
            1 => after(
                Record {
                    action: read_etag("read", s, &update),
                    ..step(read, at)
                },
                &update,
                On::Expected,
            ),
            _ => after(step(id("group", s, "call", cycle), at), &read, On::Settled),
        }
    })
}

/// Updates: 8 % answered without the ETag, 7 % refused; reads 10 % unknown;
/// calls 5 % unknown, resent. Answers within 0.2–1.2 ms.
fn script(line: u64, attempt: u32, r: &Record) -> Attempt {
    let k = Key::root(Seed(7)).at(line).at(u64::from(attempt));
    let draw = |label: &str, m: u64| k.child(label).unit().raw() % m;
    let captures: Vec<Captured> = r
        .capture
        .iter()
        .map(|c| Captured {
            subject: c.subject.clone(),
            slot: c.slot.clone(),
            value: line.to_string().into_bytes(),
        })
        .collect();
    let roll = draw("judgement", 100);
    let judgement = match r.step.label.as_str() {
        "update" if roll < 8 => Judgement::Expected(vec![]),
        "update" if roll < 15 => Judgement::Unexpected,
        "read" if roll < 10 => Judgement::Unknown,
        "call" if roll < 5 => Judgement::Unknown,
        _ => Judgement::Expected(captures),
    };
    Attempt {
        sent_after: draw("sent", 100),
        answer_after: Some(200 + draw("answer", 1_000)),
        judgement,
    }
}

/// The run's peak heap above what was held before it, and the run.
fn measured(cycles: u32, lines: u64) -> (usize, Run) {
    // The plan repeats one shape: a prefix of thirty lifetimes or more is
    // what the validator checks.
    let all: Vec<Record> = plan(cycles, lines.min(6_000)).collect();
    let report = run_with(plan_header(), &all, &NoHost, 1_000_000);
    assert!(
        report.violations.is_empty(),
        "{:?}",
        &report.violations[..3.min(report.violations.len())]
    );
    drop(all);
    let before = NOW.load(Relaxed);
    PEAK.store(before, Relaxed);
    let ran = run_streaming(plan_header(), &mut plan(cycles, lines), 3_000, &script).unwrap();
    let peak = PEAK.load(Relaxed) - before;
    assert_eq!(ran.ends.iter().sum::<u64>(), lines, "every line ends");
    assert_eq!(
        ran.left, ran.kept,
        "{cycles} cycles a subject: cells left behind"
    );
    (peak, ran)
}

#[test]
fn the_machine_holds_what_the_rate_holds_not_the_subjects_nor_the_length() {
    let lines = 32_000;
    // 1,333 subjects of 8 cycles, against 10,666 of one.
    let (few, ran) = measured(8, lines);
    // The plan reaches what it is for: skips and resends.
    assert!(ran.ends[3] > 500, "{:?} skipped", ran.ends);
    assert!(ran.resent > 40, "{} resent", ran.resent);
    let (many, many_run) = measured(1, lines);
    eprintln!("peak heap: 1,333 subjects {few} B, 10,666 subjects {many} B");
    assert!(
        many <= few + few / 5,
        "10,666 subjects held {many} B at the peak, 1,333 held {few} B"
    );
    // Twice as long at the same rate.
    let (half, half_run) = measured(1, lines / 2);
    eprintln!(
        "over half the plan: {half} B and {} steps held at once; over all of it {many} B and {}",
        half_run.held, many_run.held
    );
    assert!(
        many <= half + half / 5,
        "{many} B over the plan, {half} B over half"
    );
    assert!(
        many_run.held <= half_run.held + half_run.held / 10,
        "{} steps held over the plan, {} over half",
        many_run.held,
        half_run.held
    );
}
