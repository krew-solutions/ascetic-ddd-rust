//! K2b-7e-2b-2 of ADR-0047 (decision 22): a replay's reader holds what its
//! `Reading` states, by construction — its input's buffers, the verifier's
//! line and chunks reserved at the commitment's sizes, the line it decodes,
//! the header once — and no line past its commitment's is held; libzstd's
//! context at the window is what `DECODER` states, and a frame asking a wider
//! window is refused. A binary of its own: its allocator counts each
//! thread's bytes.

mod fixture;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use ascetic_ddd_command_log::disk::LocalDisk;
use ascetic_ddd_command_log::live::{Commitment, DECODER, commit_file, replay, replay_priced};
use ascetic_ddd_command_log::log::WINDOW_LOG;
use ascetic_ddd_command_log::record::{
    Action, ClientRetry, Codec, Header, HostCall, Operation, Payload, Record, Safety,
};
use ascetic_ddd_command_log::shape::bound;
use ascetic_ddd_command_log::validate::NoHost;
use ascetic_ddd_command_log::{Compression, LogError, write_file};
use fixture::*;

/// The system allocator, counting the bytes each thread holds and their peak:
/// a thread's own, so the harness's threads — spawning tests, collecting
/// their output — are not counted in a test's (they were, under load: a
/// record's heap off by ±600 B); a block moved counted at its new size before
/// its old is let go.
struct Counting;

thread_local! {
    /// The bytes this thread allocated less those it let go.
    static NOW: Cell<isize> = const { Cell::new(0) };
    /// The most of [`NOW`] since [`reset_peak`].
    static PEAK: Cell<isize> = const { Cell::new(0) };
}

fn counted(by: isize) {
    // A thread past its locals' end counts nothing: no test measures there.
    let _ = NOW.try_with(|now| {
        let n = now.get().saturating_add(by);
        now.set(n);
        let _ = PEAK.try_with(|peak| peak.set(peak.get().max(n)));
    });
}

fn size(bytes: usize) -> isize {
    isize::try_from(bytes).unwrap_or(isize::MAX)
}

/// The bytes this thread holds.
fn now() -> isize {
    NOW.with(Cell::get)
}

/// This thread's peak, counted from what it holds now.
fn reset_peak() {
    PEAK.with(|peak| peak.set(now()));
}

/// What this thread held at most past `base`, since [`reset_peak`].
fn peak_past(base: isize) -> u64 {
    u64::try_from(PEAK.with(Cell::get) - base).unwrap()
}

/// What this thread holds past `base`.
fn held_past(base: isize) -> u64 {
    u64::try_from(now() - base).unwrap()
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

/// Step lines a chunk: four, so a chunk is a few lines and the line's own
/// terms show beside it.
const CHUNK_LINES: u64 = 4;

/// A header with `n` more operations: far longer than a chunk at two hundred,
/// its scenario then a long escaped string — serde's scratch takes it while
/// the header's buffers are held.
fn wide_header(n: usize) -> Header {
    let mut h = header();
    if n > 0 {
        h.scenario = "a\"".repeat(20_000);
    }
    for i in 0..n {
        h.operations.push(Operation {
            name: format!("operation-{i:04}"),
            method: "POST".into(),
            path: format!("/a/rather/long/path/to/resource/{i:04}/{{group}}"),
            content_type: "application/json".into(),
            decorated: vec![],
            writes: vec![],
            safety: Safety::Never,
            client: ClientRetry {
                attempts: 1,
                on: vec![],
                backoff: 0,
                retry_after: false,
            },
            not_run: vec![],
            timeout: TIMEOUT,
            limits: vec![],
        });
    }
    h
}

/// A host call whose label is long and escaped — serde's scratch holds it —,
/// its argument some kilobytes.
fn record(i: u64) -> Record {
    let label = "say \"hello\" ".repeat(150);
    let mut r = step(id("device", i, &label, 0), i * 1_000);
    r.action = Action::Host(HostCall {
        op: "noop".into(),
        args: Payload {
            codec: Codec::Text,
            bytes: vec![b'x'; 3_000],
            holes: vec![],
        },
    });
    r
}

fn persisted(dir: &Path, compression: Compression, wide: usize) -> (PathBuf, Commitment) {
    let log = dir.join(format!("plan-{compression:?}-{wide}.jsonl"));
    write_file(&log, wide_header(wide), (0..24).map(record), compression).unwrap();
    let path = dir.join(format!("plan-{compression:?}-{wide}.commitment"));
    let report = commit_file(
        &log,
        &NoHost,
        &limits_in(dir),
        "build",
        CHUNK_LINES,
        &path,
        &LocalDisk,
    )
    .unwrap();
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    (log, Commitment::read(&path).unwrap())
}

/// A replay's reader holds, at its peak, at most what its `Reading` states
/// with one record in decoding at the plan's largest — and not much less: its
/// buffers, the verifier's line and chunks, the commitment and the header are
/// exact; only the line's scratch is a bound.
#[test]
fn a_replay_holds_what_its_reading_states() {
    let dir = tempfile::tempdir().unwrap();
    for compression in [Compression::None, Compression::Zstd { level: 1 }] {
        let (log, commitment) = persisted(dir.path(), compression, 0);
        assert!(commitment.header_line < commitment.line);
        let base = now();
        reset_peak();
        let (reader, reading) = replay_priced(&log, &commitment).unwrap();
        let mut n = 0;
        for r in reader {
            r.unwrap();
            n += 1;
        }
        let peak = peak_past(base);
        assert_eq!(n, 24);
        let stated = reading.heap() + bound(&commitment.peaks.record);
        eprintln!("{compression:?}: peak {peak}, stated {stated}, {reading:?}");
        assert!(peak <= stated, "{compression:?}: {peak} > {stated}");
        assert!(
            peak * 100 >= stated * 85,
            "{compression:?}: {peak} below 85 % of {stated}"
        );
        assert_eq!(
            reading.decoder,
            if compression == Compression::None {
                0
            } else {
                DECODER
            }
        );
    }
}

/// A header far longer than a chunk: read at its peak within what the
/// reading states, its two buffers let go once it is — what the reader holds
/// past it is a step's reading alone.
#[test]
fn a_long_header_s_buffers_are_let_go() {
    let dir = tempfile::tempdir().unwrap();
    let (log, commitment) = persisted(dir.path(), Compression::None, 200);
    assert!(commitment.header_line > 2 * commitment.chunk_bytes);
    let base = now();
    reset_peak();
    let (mut reader, reading) = replay_priced(&log, &commitment).unwrap();
    let first = reader.next().unwrap().unwrap();
    let held = held_past(base);
    for r in reader.by_ref() {
        r.unwrap();
    }
    let peak = peak_past(base);
    let stated = reading.heap() + bound(&commitment.peaks.record);
    assert!(peak <= stated, "{peak} > {stated}");
    // Past the header: a step's verifier, the line's copy freed, the header
    // decoded, one record out.
    let trailer = ascetic_ddd_command_log::live::trailer_line();
    let chunk = commitment.chunk_bytes + trailer;
    let steps = reading.buffers
        + commitment.line.max(trailer)
        + 2 * chunk
        + reading.commitment
        + reading.header
        + bound(&commitment.peaks.record);
    eprintln!("past the header {held}, a step's reading {steps}, peak {peak} of {stated}");
    assert!(held <= steps, "{held} > {steps}");
    drop(first);
}

/// A line one byte past its commitment's limit is refused before a byte past
/// the limit is held — a step's and the header's —, a line at it read.
#[test]
fn a_line_past_its_commitment_s_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (log, commitment) = persisted(dir.path(), Compression::None, 200);
    let read = |c: &Commitment| -> Result<u64, LogError> {
        let mut n = 0;
        for r in replay(&log, c)? {
            r?;
            n += 1;
        }
        Ok(n)
    };
    assert_eq!(read(&commitment).unwrap(), 24);
    for (shorter, what) in [
        (
            Commitment {
                line: commitment.line - 1,
                ..commitment.clone()
            },
            "",
        ),
        (
            Commitment {
                header_line: commitment.header_line - 1,
                ..commitment.clone()
            },
            "line 1,",
        ),
    ] {
        let base = now();
        reset_peak();
        let refused = read(&shorter).unwrap_err();
        let peak = peak_past(base);
        assert!(
            matches!(&refused, LogError::Diverged { what: w }
                if w.starts_with(what) && w.contains(", past its ")),
            "{refused:?}"
        );
        // Nothing grown past the limits: each line read into its reservation
        // — the header's two at its limit beside what a step's reading holds.
        let reading = ascetic_ddd_command_log::live::reading(&shorter, &wide_header(200), false);
        let most = reading.heap() + bound(&shorter.peaks.record) + 4_096;
        assert!(peak <= most, "{what:?}: {peak} > {most}");
    }
}

/// libzstd's context, once a frame of the writer's window is decoded, is what
/// `DECODER` states — two-sided; a frame asking a window past it is refused
/// at its header, never allocated; the log's writer asks none past it.
#[test]
fn a_compressed_log_s_window_is_bounded() {
    use zstd::zstd_safe::{DCtx, DParameter, InBuffer, OutBuffer};
    // A stream as the log's writer makes one: its window the writer's.
    let mut encoder = zstd::Encoder::new(Vec::new(), 1).unwrap();
    encoder.window_log(WINDOW_LOG).unwrap();
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    for _ in 0..(1 << 20) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        encoder.write_all(&state.to_le_bytes()[..4]).unwrap();
    }
    let frame = encoder.finish().unwrap();
    let mut dctx = DCtx::create();
    dctx.set_parameter(DParameter::WindowLogMax(WINDOW_LOG))
        .unwrap();
    let mut out = vec![0u8; 1 << 16];
    let mut input = InBuffer::around(&frame);
    while input.pos() < frame.len() {
        let mut output = OutBuffer::around(&mut out[..]);
        dctx.decompress_stream(&mut output, &mut input).unwrap();
    }
    let size = dctx.sizeof() as u64;
    eprintln!("libzstd's context at window 2^{WINDOW_LOG}: {size} B");
    assert!(size <= DECODER, "{size} > {DECODER}");
    assert!(size * 100 >= DECODER * 98, "{size} below 98 % of {DECODER}");
    // A log compressed with a window past the reader's: refused.
    let dir = tempfile::tempdir().unwrap();
    let (plain, commitment) = persisted(dir.path(), Compression::None, 0);
    let mut wide = zstd::Encoder::new(Vec::new(), 1).unwrap();
    wide.window_log(WINDOW_LOG + 2).unwrap();
    wide.write_all(&std::fs::read(&plain).unwrap()).unwrap();
    let log = dir.path().join("wide.jsonl.zst");
    std::fs::write(&log, wide.finish().unwrap()).unwrap();
    assert!(replay(&log, &commitment).is_err());
    // The log's writer keeps its window at every level — level 19's own is
    // four times the reader's: its log replays.
    let (high, commitment) = persisted(dir.path(), Compression::Zstd { level: 19 }, 0);
    assert_eq!(replay(&high, &commitment).unwrap().count(), 24);
}
