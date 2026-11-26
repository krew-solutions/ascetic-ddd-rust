//! K2b-7e-2b-2b-2 of ADR-0047 (decision 22): a journal's writer, its channel
//! full and its thread stalled in a write, holds what its `Journal` states —
//! the channel's slots, `behind + 2` lines at their bound, the writer's
//! buffer, its thread and channel's own, its output's — exactly in its slope
//! and not loosely in its level. A binary of its own and one test: its
//! allocator counts the whole process, the journal's three threads with it.

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};

use ascetic_ddd_command_log::record::{Dep as RecordDep, On, Record, Safety};
use ascetic_ddd_command_log::shape::{self, Shape};
use ascetic_ddd_load_run::sink::{Journal, JournalSink, OUT, Written};
use ascetic_ddd_scheduler::journal::{
    self, Action, Dep, Header, Key, Line, Read, line_bound, line_heap,
};
use fixture::*;

/// The system allocator, counting the bytes the process holds and their peak.
struct Counting;

static NOW: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);

fn grew(by: isize) {
    let now = NOW.fetch_add(by, SeqCst).saturating_add(by);
    PEAK.fetch_max(now, SeqCst);
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
            grew(size(layout.size()));
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
            grew(self::size(size));
            NOW.fetch_sub(self::size(layout.size()), SeqCst);
        }
        q
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The heap the output states it holds.
const OUT_HEAP: usize = 4 << 10;

/// Rounds of yields a wait takes at most: a channel that never fills to its
/// count is refused, not waited on.
const ROUNDS: usize = 20_000;

/// Where a stalled output stands.
#[derive(Default)]
struct Stall {
    /// Writes pass until it is armed: the header's.
    armed: AtomicBool,
    /// A write waits on it, armed.
    released: AtomicBool,
    /// A write is waiting.
    waiting: AtomicBool,
}

/// An output of exactly `OUT` bytes inline, holding `OUT_HEAP` on the heap,
/// stalled in its first write once armed.
struct Stalled {
    stall: Arc<Stall>,
    heap: Vec<u8>,
    _pad: [u8; OUT - size_of::<Arc<Stall>>() - size_of::<Vec<u8>>()],
}

impl Write for Stalled {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.stall.armed.load(SeqCst) {
            self.stall.waiting.store(true, SeqCst);
            while !self.stall.released.load(SeqCst) {
                std::thread::yield_now();
            }
        }
        let _ = self.heap.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A record whose read line is its largest: a key of a long label and eight
/// barriers of long labels, no member.
fn record() -> Record {
    let mut r = call("read", 0, 0, 0, 0);
    r.step.label = "l".repeat(1_024);
    for j in 0..8 {
        r.after.push(RecordDep::Each {
            kind: "nobody".into(),
            scope: vec![7; 16],
            label: format!("{j}{}", "b".repeat(256)),
            n: 0,
            on: On::Settled,
            count: None,
        });
    }
    r
}

/// `r`'s read line, as the machine makes it: every string and sequence at its
/// exact length.
fn read_line(r: &Record) -> Line {
    let key = |kind: &str, scope: &[u8], label: &str, n: u32| Key {
        kind: kind.to_owned(),
        scope: scope.to_vec(),
        label: label.to_owned(),
        n,
    };
    let after: Vec<Dep> = r
        .after
        .iter()
        .map(|d| match d {
            RecordDep::Each {
                kind,
                scope,
                label,
                n,
                on,
                ..
            } => Dep::Each {
                on: *on,
                key: key(kind, scope, label, *n),
                members: 0,
            },
            RecordDep::Step { .. } => unreachable!("barriers only"),
        })
        .collect();
    Line::Read(Read {
        at: 0,
        line: 2,
        key: key(
            &r.step.subject.kind,
            &r.step.subject.scope,
            &r.step.label,
            r.step.n,
        ),
        action: Action::Rest,
        safety: Safety::Never,
        cause: None,
        after,
        planned: 0,
        refusal: false,
    })
}

/// A header holding nothing on the heap: what `start` drops is not the term's.
fn bare_header() -> Header {
    Header {
        source: String::new(),
        log: String::new(),
        validator: String::new(),
        gates: Vec::new(),
        judging: None,
        admission: None,
        ..journal::sample::header()
    }
}

/// What a journal `behind` lines behind holds, its channel full of `line`s and
/// its thread stalled in a write: the bytes held, the peak, past the base.
fn held(behind: usize, line: &Line) -> (u64, u64) {
    let stall = Arc::new(Stall::default());
    let slot: Arc<Mutex<Option<Written<Stalled>>>> = Arc::new(Mutex::new(None));
    let (go, stop) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    let (entered, completed) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let header = bare_header();
    // The feeder, started before the base: its spawn is not the journal's.
    let feeder = {
        let (slot, go, stop, entered, completed, line) = (
            slot.clone(),
            go.clone(),
            stop.clone(),
            entered.clone(),
            completed.clone(),
            line.clone(),
        );
        std::thread::Builder::new()
            .name("feeder".into())
            .spawn(move || {
                while !go.load(SeqCst) {
                    std::thread::yield_now();
                }
                let mut sink = slot.lock().unwrap().take().unwrap();
                while !stop.load(SeqCst) {
                    let next = line.clone();
                    entered.fetch_add(1, SeqCst);
                    sink.line(next).unwrap();
                    completed.fetch_add(1, SeqCst);
                }
                sink.finish().unwrap();
            })
            .unwrap()
    };
    let base = NOW.load(SeqCst);
    PEAK.store(base, SeqCst);
    let out = Stalled {
        stall: stall.clone(),
        heap: Vec::with_capacity(OUT_HEAP),
        _pad: [0; OUT - size_of::<Arc<Stall>>() - size_of::<Vec<u8>>()],
    };
    let written = Written::assumed(out, header, behind).unwrap();
    stall.armed.store(true, SeqCst);
    // The writer waits on its empty channel first: its waiting is its own.
    for _ in 0..ROUNDS {
        std::thread::yield_now();
    }
    *slot.lock().unwrap() = Some(written);
    go.store(true, SeqCst);
    let behind = behind.max(1);
    let mut still = None;
    for _ in 0..ROUNDS {
        let before = NOW.load(SeqCst);
        for _ in 0..200 {
            std::thread::yield_now();
        }
        let done = (entered.load(SeqCst), completed.load(SeqCst));
        if stall.waiting.load(SeqCst)
            && done == (behind + 2, behind + 1)
            && NOW.load(SeqCst) == before
        {
            still = Some(before);
            break;
        }
    }
    let now = still.unwrap_or_else(|| {
        panic!(
            "the channel never stood full at {behind}: {} sends begun, {} done",
            entered.load(SeqCst),
            completed.load(SeqCst)
        )
    });
    let peak = PEAK.load(SeqCst);
    stop.store(true, SeqCst);
    stall.released.store(true, SeqCst);
    feeder.join().unwrap();
    (
        u64::try_from(now - base).unwrap(),
        u64::try_from(peak - base).unwrap(),
    )
}

/// A journal's channel full, its thread stalled: within what `Journal`
/// states at 95 % or more, at 1, 4 and 64 lines and at 0 (clamped to 1); its
/// slope between 64 and 128 lines what it states — a slot and a line's bound
/// a line — to within the thread's own variation.
#[test]
fn a_journal_holds_what_it_states() {
    let r = record();
    let s: Shape = shape::shape(&r);
    let line = read_line(&r);
    let bound = line_bound(&s);
    assert_eq!(
        line_heap(&line),
        bound,
        "the read line at its record's bound"
    );
    let mut levels = Vec::new();
    for behind in [64, 128, 4, 1, 0] {
        let (held, peak) = held(behind, &line);
        let stated = Journal::new(behind, OUT_HEAP as u64).bytes(&s);
        eprintln!(
            "MEASURED a journal {behind} behind, lines of {bound} B: held {held}, peak {peak}, stated {stated} ({:.1} %)",
            100.0 * held as f64 / stated as f64
        );
        assert!(
            peak <= stated,
            "{behind} behind: {peak} B at its peak, {stated} stated"
        );
        assert!(
            held as f64 >= 0.95 * stated as f64,
            "{behind} behind: {held} B held, {stated} stated"
        );
        levels.push((behind, held));
    }
    let at = |b: usize| levels.iter().find(|(x, _)| *x == b).unwrap().1;
    let stated = |b: usize| Journal::new(b, OUT_HEAP as u64).bytes(&s);
    // Within what the thread's own may differ by between runs — whether its
    // writer waited on an empty channel, 72 B —, less than a slot's stamp
    // over 64 slots.
    let differ = |a: u64, b: u64| a.abs_diff(b);
    assert!(
        differ(at(128) - at(64), stated(128) - stated(64)) <= 128,
        "64 lines more: their slots and lines — {} B held, {} B stated",
        at(128) - at(64),
        stated(128) - stated(64)
    );
    assert!(differ(at(0), at(1)) <= 128, "no channel of no line");
}
