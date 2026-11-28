//! K2b-7e-2b-3d-4a of ADR-0047 (decision 22): the whole-run measurement, a
//! row a process. A run made by the real path — its log committed, replayed
//! through its reader, fitted to the memory its scope's limit allows at its
//! need, admitted on the runtimes built for it, measured, gated, started on a
//! journal on disk and run against a stand of its own over loopback TCP —,
//! its heap counted process-wide at each stage's edge, glibc's own and the
//! process's resident set beside it. Run by `tests/whole/rows.py`, which
//! makes each row in scopes of its own and compares it with the terms its
//! journal's header carries: an ignored test, a measurement, no assertion
//! on the totals (ADR-0047, K2b-7e-2b-3d-4's decision).

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use ascetic_ddd_command_log::fills::Namespace;
use ascetic_ddd_command_log::live::{Commitment, commit_file};
use ascetic_ddd_command_log::record::{Codec, Payload, Record, Rest};
use ascetic_ddd_command_log::render::RunSecret;
use ascetic_ddd_command_log::validate::NoHost;
use ascetic_ddd_load_run::clock::RealClock;
use ascetic_ddd_load_run::live::{Limit, Live};
use ascetic_ddd_load_run::memory::Memory;
use ascetic_ddd_load_run::rest::{Config, RestCarrier};
use ascetic_ddd_load_run::run::{admit, fit, run};
use ascetic_ddd_load_run::sink::Journal;
use ascetic_ddd_load_run::source::Replayed;
use ascetic_ddd_load_run::stand::{Route, Stand};
use ascetic_ddd_load_run::tcp::TcpEgress;
use fixture::*;

/// The system allocator, counting the bytes the process holds, their peak
/// and the blocks alive; a block moved counted at both sizes at once.
struct Counting;

static NOW: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);

fn grew(by: usize) {
    let now = NOW.fetch_add(by, Relaxed) + by;
    PEAK.fetch_max(now, Relaxed);
    LIVE.fetch_add(1, Relaxed);
}

fn shrank(by: usize) {
    NOW.fetch_sub(by, Relaxed);
    LIVE.fetch_sub(1, Relaxed);
}

// SAFETY: every block comes from the system allocator and goes back to it
// with its layout; a moved block is a new one, copied, then the old let go.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract, passed on.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            grew(layout.size());
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract, passed on.
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            grew(layout.size());
        }
        p
    }

    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract, passed on.
        unsafe { System.dealloc(p, layout) };
        shrank(layout.size());
    }

    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: the caller's contract: `size` non-zero, the layout valid.
        let new = unsafe { Layout::from_size_align_unchecked(size, layout.align()) };
        // SAFETY: as above.
        let q = unsafe { self.alloc(new) };
        if !q.is_null() {
            // SAFETY: both blocks hold at least the smaller size; distinct.
            unsafe { std::ptr::copy_nonoverlapping(p, q, layout.size().min(size)) };
            // SAFETY: the old block, its layout.
            unsafe { self.dealloc(p, layout) };
        }
        q
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// glibc's own accounting (`mallinfo2(3)`).
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct Mallinfo2 {
    arena: usize,
    ordblks: usize,
    smblks: usize,
    hblks: usize,
    hblkhd: usize,
    usmblks: usize,
    fsmblks: usize,
    uordblks: usize,
    fordblks: usize,
    keepcost: usize,
}

unsafe extern "C" {
    fn mallinfo2() -> Mallinfo2;
    fn prctl(option: i32, ...) -> i32;
}

/// `PR_SET_THP_DISABLE`: no transparent huge page for the process.
const PR_SET_THP_DISABLE: i32 = 41;

/// The threads' stacks resident: each anonymous writable mapping right above
/// a guard of one page (`---p`, 4 KiB), its `Rss` summed, and their number;
/// the main thread's `[stack]` among them.
fn stacks() -> (u64, u64) {
    let Ok(smaps) = std::fs::read_to_string("/proc/self/smaps") else {
        return (0, 0);
    };
    let (mut bytes, mut count) = (0, 0);
    let (mut guarded, mut after_guard) = (false, false);
    for line in smaps.lines() {
        let mut fields = line.split_whitespace();
        let first = fields.next().unwrap_or("");
        if first.contains('-') && first.chars().next().is_some_and(|c| c.is_ascii_hexdigit()) {
            let perms = fields.next().unwrap_or("");
            let path = line.split_whitespace().nth(5);
            let size = first.split_once('-').and_then(|(a, b)| {
                Some(u64::from_str_radix(b, 16).ok()? - u64::from_str_radix(a, 16).ok()?)
            });
            guarded = path == Some("[stack]") || (after_guard && perms == "rw-p" && path.is_none());
            // A thread's guard is one page below its stack; glibc's arenas
            // leave larger reservations, which are no guard.
            after_guard = perms == "---p" && path.is_none() && size == Some(4_096);
        } else if first == "Rss:" && guarded {
            bytes += fields
                .next()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0)
                * 1024;
            count += 1;
            guarded = false;
        }
    }
    (bytes, count)
}

/// A `/proc/self/status` or `/proc/self/smaps_rollup` field, in bytes.
fn proc_kb(file: &str, field: &str) -> u64 {
    std::fs::read_to_string(file)
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with(field))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse::<u64>().ok())
        })
        .map_or(0, |kb| kb * 1024)
}

/// What one edge reads: the counted heap now and its peak since the last
/// edge (the peak then reset), the blocks alive, glibc's in-use, free and
/// mmapped bytes, the resident high-water, the anonymous resident and its
/// huge pages.
fn edge(name: &str) -> serde_json::Value {
    let now = NOW.load(Relaxed);
    let peak = PEAK.swap(now, Relaxed);
    // SAFETY: a plain call into glibc, no pointer passed.
    let m = unsafe { mallinfo2() };
    serde_json::json!({
        "edge": name,
        "now": now,
        "peak": peak,
        "live": LIVE.load(Relaxed),
        "uordblks": m.uordblks,
        "fordblks": m.fordblks,
        "hblkhd": m.hblkhd,
        "arena": m.arena,
        "hwm": proc_kb("/proc/self/status", "VmHWM:"),
        "rss_anon": proc_kb("/proc/self/status", "RssAnon:"),
        "anon_huge": proc_kb("/proc/self/smaps_rollup", "AnonHugePages:"),
        "stacks": stacks().0,
        "threads": stacks().1,
    })
}

struct OneStand;

impl Stand for OneStand {
    fn gate_bytes(&self) -> ascetic_ddd_load_run::stand::GateBytes {
        ascetic_ddd_load_run::stand::GateBytes::NONE
    }

    fn route(&self, _: &Record, _: &Rest) -> Result<Route, String> {
        Ok(Route {
            authority: "stand.test".to_owned(),
            endpoint: "gateway".to_owned(),
        })
    }

    fn endpoints(&self, _: &ascetic_ddd_command_log::record::Header, _: u32) -> Vec<String> {
        vec!["gateway".to_owned()]
    }

    fn gate(
        &self,
        _: usize,
        _: ascetic_ddd_load_run::stand::Opening<'_>,
    ) -> impl Future<Output = Option<Result<String, ascetic_ddd_load_run::stand::Refused>>> + Send
    {
        std::future::ready(None)
    }
}

fn var(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name}: set by tests/whole/rows.py"))
}

fn num(name: &str) -> u64 {
    var(name).parse().unwrap()
}

/// One row, its parameters from the environment: `WHOLE_PORT` the stand's,
/// `WHOLE_N` the reads at one instant, `WHOLE_BODY` each request's body
/// bytes, `WHOLE_CAP` the sockets' cap, `WHOLE_TIMEOUT` the operation's in
/// µs, `WHOLE_ZSTD` 1 for a compressed log, `WHOLE_DIR` a directory for the
/// log, its commitment and the journal. Prints one line, `WHOLE {json}`.
#[test]
#[ignore = "a measurement, a row a process: tests/whole/rows.py"]
fn row() {
    let n = num("WHOLE_N");
    let body = usize::try_from(num("WHOLE_BODY")).unwrap();
    let cap = usize::try_from(num("WHOLE_CAP")).unwrap();
    let dir = std::path::PathBuf::from(var("WHOLE_DIR"));
    let addr: SocketAddr = format!("127.0.0.1:{}", num("WHOLE_PORT")).parse().unwrap();
    let sources = u32::try_from(num("WHOLE_SOURCES")).unwrap();
    if var("WHOLE_THP") == "off" {
        // SAFETY: a plain call into the kernel, no pointer passed.
        assert_eq!(
            unsafe { prctl(PR_SET_THP_DISABLE, 1u64, 0u64, 0u64, 0u64) },
            0
        );
    }
    let _ = ascetic_ddd_load_run::live::Descriptors::raise().unwrap();
    // The plan: `n` reads at one instant 200 ms in, each with a body of
    // `body` bytes, then their trailer; committed as a host would.
    let mut header = header();
    header.channels.clear();
    header.calls.clear();
    header.profile.jitter = 200_000;
    header.profile.reserve = 100;
    header.profile.sources = sources;
    header
        .operations
        .iter_mut()
        .for_each(|o| o.timeout = num("WHOLE_TIMEOUT"));
    let records: Vec<Record> = (0..n)
        .map(|i| {
            let source = u32::try_from(i % u64::from(sources)).unwrap();
            let mut r = call("read", i + 1, 0, 200_000, source);
            if body > 0
                && let ascetic_ddd_command_log::record::Action::Rest(rest) = &mut r.action
            {
                let mut bytes = vec![b'a'; body];
                bytes[0] = b'"';
                bytes[body - 1] = b'"';
                rest.body = Some(Payload {
                    codec: Codec::Json,
                    bytes,
                    holes: vec![],
                });
            }
            r
        })
        .collect();
    let log = dir.join("plan.jsonl");
    let compression = if num("WHOLE_ZSTD") == 1 {
        ascetic_ddd_command_log::Compression::Zstd { level: 1 }
    } else {
        ascetic_ddd_command_log::Compression::None
    };
    ascetic_ddd_command_log::write_file(&log, header.clone(), records, compression).unwrap();
    let committed = dir.join("plan.commitment");
    let report = commit_file(
        &log,
        &NoHost,
        &limits_in(&dir),
        "build",
        64,
        &committed,
        &ascetic_ddd_command_log::disk::LocalDisk,
    )
    .unwrap();
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    let commitment = Commitment::read(&committed).unwrap();
    let mut edges = vec![edge("base")];
    // The real path, each stage's edge read.
    let (plan, replayed) = Replayed::open(&log, &commitment, 64).unwrap();
    edges.push(edge("opened"));
    let shared = plan.header().clone();
    let carrier = || {
        Arc::new(RestCarrier::new(
            shared.clone(),
            OneStand,
            TcpEgress::new(
                BTreeMap::from([("gateway".to_owned(), addr)]),
                vec![None; sources as usize],
            )
            .with_ports(cap),
            Config::new(RunSecret::new([7; 32])),
        ))
    };
    let limit = Limit::read().unwrap();
    let most = limit
        .max()
        .map(|(b, _)| b)
        .expect("a scope with a finite memory.max");
    let read_ahead = plan.header().read_ahead;
    let need = fit::<RealClock, _>(
        plan.clone(),
        read_ahead,
        carrier(),
        Journal::new(64, 0),
        Memory::declared(most),
        limit,
    )
    .unwrap()
    .fit()
    .need;
    let fitted = fit::<RealClock, _>(
        plan,
        read_ahead,
        carrier(),
        Journal::new(64, 0),
        Memory::declared(need),
        limit,
    )
    .unwrap();
    edges.push(edge("fitted"));
    let live = Live::fitted(&fitted).unwrap();
    let admitted = admit(fitted, live.runtimes()).unwrap();
    edges.push(edge("admitted"));
    let root = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let stopwatch = Arc::new(RealClock::start(2_000).unwrap());
    let measured = root.block_on(admitted.measure(stopwatch.clone())).unwrap();
    edges.push(edge("stage 2"));
    let gated = root
        .block_on(measured.gates(Namespace(1), stopwatch, 5_000_000))
        .unwrap_or_else(|b| panic!("barred: {:?}", b.refused()));
    edges.push(edge("stage 3"));
    let journal = dir.join("run.journal");
    let ready = gated
        .start("whole".to_owned(), || std::fs::File::create(&journal))
        .unwrap();
    edges.push(edge("started"));
    let ran = root.block_on(run(ready, replayed)).unwrap();
    edges.push(edge("ran"));
    drop(ran);
    drop(live);
    edges.push(edge("dropped"));
    let line = serde_json::json!({
        "need": need,
        "journal": journal.display().to_string(),
        "edges": edges,
    });
    println!("WHOLE {line}");
}
