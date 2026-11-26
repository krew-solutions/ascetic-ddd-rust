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

use ascetic_ddd_command_log::fills::Captured;
use ascetic_ddd_command_log::record::{Codec, Dep, On};
use ascetic_ddd_command_log::record::{Header, Record};
use ascetic_ddd_command_log::shape::{self, Shape};
use ascetic_ddd_command_log::validate::{Kept, Peaks};
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

/// What a row's steps are.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// A host call, its label its own when `keys`, of `label` bytes at least.
    Host { keys: bool, label: usize },
    /// A REST read.
    Read,
    /// A REST update carrying a JSON body of `body` bytes.
    Update { body: usize },
    /// A REST read awaiting the `deps` steps before it sent.
    Chained { deps: u64 },
    /// A read capturing its group's ETag, the largest value a capture holds,
    /// answered at once: kept with its capture while the window keeps it.
    Writers,
    /// Pairs: a read capturing its group's ETag — the largest value a capture
    /// holds —, then, `answer` µs before its answer, a read sending it back.
    Captures,
    /// Pairs: a read, then a step awaiting every read before it settled and
    /// `deps` barriers no step is a member of, each keyed by a label of
    /// `label` bytes (K2b-7e-2b-2b): the keys the machine binds it by.
    Barriers { deps: u64, label: usize },
}

/// One plan of `lines` steps of `kind` a line every `every` µs from `start` —
/// all at one instant when `every` is 0 —, through `sim` with every answer
/// `answer` µs after its send, W `w` µs. The machine's peak heap, the run, and
/// what `fits` counts of the plan: the run's own counts, every step priced at
/// the largest shape among the plan's.
fn costs(lines: u64, start: u64, every: u64, answer: u64, w: u64, kind: Kind) -> (usize, Run, u64) {
    let header = Header {
        window: w,
        read_ahead: w.min(1_000),
        ..header()
    };
    let record = move |i: u64| -> Record {
        let at = start + i * every;
        match kind {
            Kind::Host { keys, label } => {
                let name = if keys {
                    format!("k{i}")
                } else {
                    "call".to_owned()
                };
                let pad = label.saturating_sub(name.len());
                step(
                    id("device", i, &format!("{name}{}", "x".repeat(pad)), 0),
                    at,
                )
            }
            Kind::Read => call("read", i, 0, at, 0),
            Kind::Update { body } => {
                let r = call("update", i, 0, at, 0);
                with_payload(r, Codec::Json, body, vec![])
            }
            Kind::Chained { deps } => (i.saturating_sub(deps)..i)
                .fold(call("read", i, 0, at, 0), |r, j| {
                    after(r, &id("device", j, "read", 0), On::Sent)
                }),
            Kind::Writers => captures_etag(call("read", i, 0, at, 0), i),
            Kind::Barriers { deps, label } => {
                if i.is_multiple_of(2) {
                    call("read", i, 0, at, 0)
                } else {
                    let mut r = step(id("device", i, "hold", 0), at);
                    r.after.push(Dep::Each {
                        kind: "device".into(),
                        scope: vec![7],
                        label: "read".into(),
                        n: 0,
                        on: On::Settled,
                        count: None,
                    });
                    for j in 0..deps {
                        r.after.push(Dep::Each {
                            kind: "nobody".into(),
                            scope: vec![7],
                            label: format!("{j}{}", "x".repeat(label)),
                            n: 0,
                            on: On::Settled,
                            count: None,
                        });
                    }
                    r
                }
            }
            Kind::Captures => {
                let g = i / 2;
                let writer = captures_etag(call("read", g, 0, at, 0), g);
                if i.is_multiple_of(2) {
                    writer
                } else {
                    let mut r = acting(
                        step(id("device", g, "update", 0), at),
                        read_etag("update", g, &writer.step),
                    );
                    r = after(r, &writer.step, On::Expected);
                    r
                }
            }
        }
    };
    let records: Vec<Record> = (0..lines).map(record).collect();
    let largest = records
        .iter()
        .map(shape::shape)
        .fold(Shape::default(), Shape::most);
    let keys: std::collections::BTreeSet<_> = records
        .iter()
        .map(|r| ascetic_ddd_command_log::plan::member_key(&r.step))
        .collect();
    let barrier_bytes = keys
        .iter()
        .map(|k| (k.0.len() + k.1.len() + k.2.len()) as u64)
        .sum();
    drop(records);
    // A writer's value: the largest a capture holds.
    let value = vec![b'a'; fits::VALUE as usize];
    let script = move |_: u64, _: u32, r: &Record| Attempt {
        after: None,
        sent_after: 0,
        answer_after: Some(if r.capture.is_empty() { answer } else { 10 }),
        judgement: Judgement::Expected(
            r.capture
                .iter()
                .map(|c| Captured {
                    subject: c.subject.clone(),
                    slot: c.slot.clone(),
                    value: value.clone(),
                })
                .collect(),
        ),
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
    let peak = PEAK.load(Relaxed) - before;
    let (held, pending) = (ran.held as u64, ran.pending as u64);
    let peaks = Peaks {
        held,
        pending,
        barriers: ran.barriers as u64,
        barrier_bytes,
        pending_shape: largest.times(pending),
        kept: Kept {
            keys: largest.key_bytes * held,
            // What the held keep once ended: a writer's capture, the writers
            // being those within W behind the cursor, one every two lines —
            // its readers' copies are their own, priced as their holes.
            captures: if kind == Kind::Captures {
                w / (2 * every) + 1
            } else {
                largest.captures.elements * held
            },
            capture_bytes: if kind == Kind::Captures {
                largest.capture_bytes * (w / (2 * every) + 1)
            } else {
                largest.capture_bytes * held
            },
        },
        record: largest,
        ..Peaks::default()
    };
    let shaped = fits::needed(&Peaks {
        held: 0,
        pending: 0,
        ..peaks.clone()
    });
    let residue = (peak as u64).saturating_sub(shaped);
    eprintln!(
        "MEASURED residue beside the shapes: {residue} B, {} a held step, {} a pending one",
        residue / held.max(1),
        residue / pending.max(1)
    );
    (peak, ran, fits::needed(&peaks))
}

/// K1e-2, K2b-7e-2b-1c: the costs `fits` counts are upper bounds of what the
/// machine holds, and not loose ones: each row's peak within what
/// `fits::needed` counts of its counts and shapes, and on the rows a constant
/// or a price is pinned on at least its floor of it.
#[test]
fn the_machine_holds_no_more_than_fits_counts() {
    let mut over = Vec::new();
    // Each row's counts just past a power of two where a container grows
    // with them — a block moved holds the old and the new at once — or
    // read ahead and released at one instant, a burst of outputs and inputs.
    let host = Kind::Host {
        keys: false,
        label: 0,
    };
    for (name, lines, start, every, answer, w, kind) in [
        ("ended in the window", 40_000, 0, 50, 100, 1_000_000, host),
        (
            "ended in the window, 2^14 + 1",
            40_000,
            0,
            50,
            100,
            819_200,
            host,
        ),
        ("not ended", 40_000, 0, 50, 500_000, 1_000, host),
        (
            "barrier keys",
            40_000,
            0,
            50,
            100,
            1_000,
            Kind::Host {
                keys: true,
                label: 0,
            },
        ),
        (
            "barrier keys of 1 KiB labels",
            16_385,
            0,
            50,
            100,
            1_000,
            Kind::Host {
                keys: true,
                label: 1_024,
            },
        ),
        (
            "REST reads not ended, 2^13 + 1",
            40_000,
            0,
            50,
            409_600,
            1_000,
            Kind::Read,
        ),
        (
            "host calls at one instant",
            32_769,
            10_000,
            0,
            500_000,
            1_000,
            host,
        ),
        (
            "REST reads at one instant",
            32_769,
            10_000,
            0,
            500_000,
            1_000,
            Kind::Read,
        ),
        (
            "REST reads at one instant, at 0",
            32_769,
            0,
            0,
            500_000,
            1_000,
            Kind::Read,
        ),
        (
            "REST updates of 4 KiB bodies at one instant",
            8_193,
            10_000,
            0,
            500_000,
            1_000,
            Kind::Update { body: 4_097 },
        ),
        (
            "REST reads, each awaiting the one before",
            8_193,
            0,
            50,
            500_000,
            1_000,
            Kind::Chained { deps: 1 },
        ),
        (
            "REST reads, each awaiting the 65 before it",
            4_097,
            0,
            10,
            500_000,
            1_000,
            Kind::Chained { deps: 65 },
        ),
        (
            "barriers of 1 KiB labels, memberless and with members",
            2_049,
            0,
            50,
            100,
            1_000,
            Kind::Barriers {
                deps: 64,
                label: 1_024,
            },
        ),
        (
            "captures kept in the window",
            8_193,
            0,
            100,
            100,
            1_000_000,
            Kind::Writers,
        ),
        (
            "captured values sent back",
            16_386,
            0,
            25,
            500_000,
            1_000,
            Kind::Captures,
        ),
    ] {
        let (peak, ran, needed) = costs(lines, start, every, answer, w, kind);
        // The rows each constant and price is pinned on: not loose on them.
        let floor = match name {
            "ended in the window" | "ended in the window, 2^14 + 1" => 0.7,
            "host calls at one instant" => 0.75,
            "barrier keys of 1 KiB labels"
            | "REST updates of 4 KiB bodies at one instant"
            | "captures kept in the window"
            | "captured values sent back" => 0.8,
            _ => 0.0,
        };
        if (peak as f64) < floor * needed as f64 {
            over.push(format!(
                "{name}: {peak} B held, below {floor} of the {needed} counted"
            ));
        }
        eprintln!(
            "MEASURED {name}: peak {peak} B, held {}, pending {}, barriers {}; fits counts {needed} ({:.0} %)",
            ran.held,
            ran.pending,
            ran.barriers,
            100.0 * peak as f64 / needed as f64
        );
        if peak as u64 > needed {
            over.push(format!("{name}: {peak} B held, {needed} counted"));
        }
    }
    assert!(over.is_empty(), "{over:#?}");
}
