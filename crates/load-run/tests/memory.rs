//! K2b-7e's measurement (ADR-0047 § Owed, "K2b's measurements"): the shell's
//! own bytes — an open attempt with its fresh connection, a connection pooled
//! idle, an answer's body while it is read and judged — by a counting
//! allocator, beside a stand of its own process. A binary of its own: the
//! allocator counts the whole process. Measured on purpose (`--ignored`).

#![cfg(target_os = "linux")]

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::process::{Child, Command};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};

use ascetic_ddd_command_log::fills::Namespace;
use ascetic_ddd_command_log::record::{Record, Rest};
use ascetic_ddd_command_log::render::RunSecret;
use ascetic_ddd_load_run::clock::{Clock, RealClock};
use ascetic_ddd_load_run::live::{Descriptors, Live};
use ascetic_ddd_load_run::rest::{Config, RestCarrier};
use ascetic_ddd_load_run::run::{Ready, admit, run};
use ascetic_ddd_load_run::sink::JournalSink;
use ascetic_ddd_load_run::source::{Plan, Records};
use ascetic_ddd_load_run::stand::{Route, Stand};
use ascetic_ddd_load_run::tcp::TcpEgress;
use ascetic_ddd_scheduler::journal::{self, JournalError};
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

struct OneStand;

impl Stand for OneStand {
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

/// The stand, a process of its own: answers each request `delay` seconds after
/// it, with a JSON string of `size` bytes in all.
const STAND: &str = r#"
import socket, sys, threading, time
port, delay, size = int(sys.argv[1]), float(sys.argv[2]), int(sys.argv[3])
body = b'"' + b"a" * max(size - 2, 0) + b'"'
head = b"HTTP/1.1 200 X\r\ncontent-type: application/json\r\ncontent-length: " + str(len(body)).encode() + b"\r\n\r\n"
def serve(c):
    buf = b""
    try:
        while True:
            d = c.recv(65536)
            if not d:
                break
            buf += d
            while b"\r\n\r\n" in buf:
                _, _, buf = buf.partition(b"\r\n\r\n")
                time.sleep(delay)
                c.sendall(head + body)
    except OSError:
        pass
    finally:
        c.close()
l = socket.socket()
l.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
l.bind(("127.0.0.1", port))
l.listen(8192)
while True:
    c, _ = l.accept()
    threading.Thread(target=serve, args=(c,), daemon=True).start()
"#;

/// Kills its child when dropped.
struct Killed(Child);

impl Drop for Killed {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A journal counted, not kept: its lines would weigh on what is measured.
#[derive(Default)]
struct Counted {
    sent: usize,
    answered: usize,
}

impl JournalSink for Counted {
    type Finished = (usize, usize);

    fn line(&mut self, line: journal::Line) -> Result<(), JournalError> {
        match line {
            journal::Line::Sent(_) => self.sent += 1,
            journal::Line::Answer(_) => self.answered += 1,
            _ => {}
        }
        Ok(())
    }

    fn finish(self) -> Result<(usize, usize), JournalError> {
        Ok((self.sent, self.answered))
    }
}

/// The bytes held, sampled: `(µs since the run's clock started, bytes)`.
type Samples = Vec<(u64, usize)>;

/// `n` reads at one instant, 200 ms in, each answered `delay` s later with
/// `size` bytes; a last read at 2.6 s keeps the run open while the
/// connections lie idle. The samples and the peak past the start; the
/// attempts sent and answered.
fn measured(n: u64, delay: f64, size: usize) -> (Samples, usize, usize, (usize, usize)) {
    let _ = Descriptors::raise().unwrap();
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let dir = tempfile::tempdir().unwrap();
    let script = dir.path().join("stand.py");
    std::fs::write(&script, STAND).unwrap();
    let _stand = Killed(
        Command::new("python3")
            .arg("-I")
            .arg(&script)
            .arg(port.to_string())
            .arg(delay.to_string())
            .arg(size.to_string())
            .spawn()
            .unwrap(),
    );
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let waiting = RealClock::start(2_000).unwrap();
    while std::net::TcpStream::connect(addr).is_err() {
        assert!(waiting.now() < 5_000_000, "the stand never listened");
        std::thread::yield_now();
    }
    let mut header = header();
    header.channels.clear();
    header.calls.clear();
    header.profile.jitter = 2_000;
    header
        .operations
        .iter_mut()
        .for_each(|o| o.timeout = 3_000_000);
    let mut records: Vec<Record> = (0..n).map(|i| call("read", i + 1, 0, 200_000, 0)).collect();
    records.push(call("read", n + 1, 0, 2_600_000, 0));
    let header = Arc::new(header);
    const CAP: usize = 4_500;
    let carrier = RestCarrier::new(
        header.clone(),
        OneStand,
        TcpEgress::new(BTreeMap::from([("gateway".to_owned(), addr)]), vec![None]).with_ports(CAP),
        Config::new(RunSecret::new([7; 32])),
    );
    let live = Live::for_caps(&[CAP]).unwrap();
    let admitted = admit(
        Plan::assumed(
            header.clone(),
            ascetic_ddd_command_log::validate::Peaks {
                pending: n + 16,
                ..Default::default()
            },
        ),
        W,
        Arc::new(carrier),
        live.runtimes(),
    )
    .unwrap();
    let root = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let clock = Arc::new(RealClock::start(header.profile.jitter).unwrap());
    let base = NOW.load(Relaxed);
    PEAK.store(base, Relaxed);
    let done = Arc::new(AtomicBool::new(false));
    let sampler = {
        let (done, clock) = (done.clone(), clock.clone());
        std::thread::spawn(move || {
            let mut samples: Samples = Vec::with_capacity(4_000);
            while !done.load(Relaxed) && samples.len() < samples.capacity() {
                samples.push((clock.now(), NOW.load(Relaxed)));
                #[expect(
                    clippy::disallowed_methods,
                    reason = "a sampler's own pace, outside the run's clock"
                )]
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            samples
        })
    };
    let counted = root
        .block_on(run(
            Ready::assumed(admitted, clock, Counted::default(), Namespace(1)),
            Records::new(records.into_iter()),
        ))
        .unwrap()
        .journal;
    done.store(true, Relaxed);
    let samples = sampler.join().unwrap();
    (samples, base, PEAK.load(Relaxed), counted)
}

/// The median bytes held over `[from, to)` µs of the run's clock.
fn plateau(samples: &Samples, from: u64, to: u64) -> usize {
    let mut v: Vec<usize> = samples
        .iter()
        .filter(|(t, _)| (from..to).contains(t))
        .map(|&(_, b)| b)
        .collect();
    assert!(!v.is_empty(), "no sample in [{from}, {to})");
    v.sort_unstable();
    v[v.len() / 2]
}

#[test]
#[ignore = "a measurement, run on purpose"]
fn the_shell_s_bytes() {
    let mut rows = Vec::new();
    for (n, size) in [(1_000, 2), (2_000, 2), (1_000, 65_536), (2_000, 65_536)] {
        let (samples, base, peak, (sent, answered)) = measured(n, 1.0, size);
        // Open: every attempt sent, none answered (0.5–1.0 s); idle: every
        // attempt answered, the connections pooled, the last read not yet due
        // (1.8–2.4 s).
        let open = plateau(&samples, 500_000, 1_000_000);
        let idle = plateau(&samples, 1_800_000, 2_400_000);
        eprintln!(
            "MEASURED memory: {n} reads, answers of {size} B: {sent} sent, {answered} answered; held before \
             {base} B, while open {open} B, while idle {idle} B, at the peak {peak} B"
        );
        rows.push((n, size, base, open, idle, peak));
    }
    for size in [2, 65_536] {
        let at = |n| *rows.iter().find(|r| r.0 == n && r.1 == size).unwrap();
        let (one, two) = (at(1_000), at(2_000));
        let per = |a: usize, b: usize| (b as f64 - a as f64) / 1_000.0;
        eprintln!(
            "MEASURED memory per attempt, answers of {size} B: open {:.0} B, idle {:.0} B, peak {:.0} B",
            per(one.3, two.3),
            per(one.4, two.4),
            per(one.5, two.5)
        );
    }
}
