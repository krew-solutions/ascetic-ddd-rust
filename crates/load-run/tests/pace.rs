//! Decision 21's measurement (ADR-0047 § Owed): how often a healthy run on
//! this box would have its hand-offs paced — the gate at the hand-off
//! replayed over the journal of a run with no stall, beside a stand of its
//! own process answering at once. Measured on purpose (`--ignored`).
#![cfg(target_os = "linux")]

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::process::{Child, Command};
use std::sync::Arc;

use ascetic_ddd_command_log::fills::Namespace;
use ascetic_ddd_command_log::record::{Record, Rest};
use ascetic_ddd_command_log::render::RunSecret;
use ascetic_ddd_load_run::clock::{Clock, RealClock};
use ascetic_ddd_load_run::live::{Descriptors, Live};
use ascetic_ddd_load_run::rest::{Config, RestCarrier};
use ascetic_ddd_load_run::run::{Ready, admit, run};
use ascetic_ddd_load_run::sink::Lines;
use ascetic_ddd_load_run::source::{Plan, Records};
use ascetic_ddd_load_run::stand::{Route, Stand};
use ascetic_ddd_load_run::tcp::TcpEgress;
use ascetic_ddd_scheduler::journal;
use fixture::*;

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

/// The stand, a process of its own: answers each request at once — every
/// `every`-th of all (0: none) with a JSON array of numbers of `size` bytes,
/// the judge's slowest shape (ADR-0047 § Measured, K2b-7d), the rest `{}`.
const STAND: &str = r#"
import socket, sys, threading
port = int(sys.argv[1])
every = int(sys.argv[2]) if len(sys.argv) > 2 else 0
size = int(sys.argv[3]) if len(sys.argv) > 3 else 0
small = b"HTTP/1.1 200 X\r\ncontent-length: 2\r\n\r\n{}"
items = b",".join(b"%d" % (i % 1000) for i in range(max(size // 4, 1)))
body = b"[" + items + b"]"
large = b"HTTP/1.1 200 X\r\ncontent-length: " + str(len(body)).encode() + b"\r\n\r\n" + body
lock = threading.Lock()
count = [0]
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
                with lock:
                    count[0] += 1
                    n = count[0]
                c.sendall(large if every and n % every == 0 else small)
    except OSError:
        pass
    finally:
        c.close()
l = socket.socket()
l.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
l.bind(("127.0.0.1", port))
l.listen(4096)
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

/// The gate replayed: attempts, those displaced past J, those it would
/// refuse, and the largest displacement.
fn paced(lines: &[journal::Line], j: u64) -> (usize, usize, usize, u64) {
    let mut c: BTreeMap<(u64, u32), u64> = BTreeMap::new();
    let mut a: BTreeMap<(u64, u32), u64> = BTreeMap::new();
    for l in lines {
        match l {
            journal::Line::Released(r) => {
                c.insert((r.line, 1), r.intended);
            }
            journal::Line::Dispatch(d) if d.attempt > 1 => {
                c.insert((d.line, d.attempt), d.due);
            }
            journal::Line::Sent(s) => {
                a.entry((s.line, s.attempt)).or_insert(s.handed);
            }
            journal::Line::Answer(x) => {
                if let Some(h) = x.unsent.as_ref().and_then(|u| u.handed) {
                    a.entry((x.line, x.attempt)).or_insert(h);
                }
            }
            _ => {}
        }
    }
    let mut displaced: Vec<(u64, u64)> = a
        .iter()
        .filter_map(|(k, &at)| c.get(k).map(|&due| (at, due)))
        .filter(|(at, due)| at.saturating_sub(*due) > j)
        .collect();
    displaced.sort_unstable();
    let largest = displaced
        .iter()
        .map(|(at, due)| at - due)
        .max()
        .unwrap_or(0);
    let (mut last, mut refused) = (0u64, 0usize);
    for (at, due) in &displaced {
        if due + j >= last {
            last = *at;
        } else {
            refused += 1;
        }
    }
    (a.len(), displaced.len(), refused, largest)
}

/// Round 3's shaper replayed (ADR-0047 § Measured, decision 21), as its
/// discriminator prescribed: the harness's outages — the union of `[due + J,
/// handed)` over attempts handed more than J late — taken from the journal;
/// attempts in dispatch order, each passing at the first instant at or past
/// its pace outside an outage (the plan compressed by at most `reserve` per
/// mille, its rounding down so that no reserve is lost; bunched at most J);
/// one whose pass falls at or past its deadline never handed. The attempts
/// never handed, the largest shift past the raw due, and the share handed
/// more than J past it, per mille.
fn shaped(lines: &[journal::Line], j: u64, reserve: u64, pairwise: bool) -> Shape {
    let mut intended: BTreeMap<u64, u64> = BTreeMap::new();
    let mut handed: BTreeMap<(u64, u32), u64> = BTreeMap::new();
    let mut order: Vec<(u64, u32, u64, u64)> = Vec::new();
    for l in lines {
        match l {
            journal::Line::Released(r) => {
                intended.insert(r.line, r.intended);
            }
            journal::Line::Dispatch(d) => order.push((d.line, d.attempt, d.due, d.deadline)),
            journal::Line::Sent(x) => {
                handed.entry((x.line, x.attempt)).or_insert(x.handed);
            }
            journal::Line::Answer(x) => {
                if let Some(h) = x.unsent.as_ref().and_then(|u| u.handed) {
                    handed.entry((x.line, x.attempt)).or_insert(h);
                }
            }
            _ => {}
        }
    }
    let raw = |line: u64, attempt: u32, due: u64| {
        if attempt == 1 {
            intended.get(&line).copied().unwrap_or(due)
        } else {
            due
        }
    };
    // The harness's outages, merged.
    let mut out: Vec<(u64, u64)> = order
        .iter()
        .filter_map(|&(line, attempt, due, _)| {
            let h = *handed.get(&(line, attempt))?;
            let r = raw(line, attempt, due);
            (h > r + j).then_some((r + j, h))
        })
        .collect();
    out.sort_unstable();
    let mut outages: Vec<(u64, u64)> = Vec::new();
    for (s, e) in out {
        match outages.last_mut() {
            Some(last) if s <= last.1 => last.1 = last.1.max(e),
            _ => outages.push((s, e)),
        }
    }
    let free = |t: u64| -> u64 {
        let i = outages.partition_point(|&(s, _)| s <= t);
        match i.checked_sub(1).map(|k| outages[k]) {
            Some((_, e)) if t < e => e,
            _ => t,
        }
    };
    // The solver's recursion: `(D, P)`, the dues clamped monotonic. The
    // pairwise form (its discriminator): for every two hand-offs
    // `1000·|G_k − G_i| ≥ c·(|raw_k − raw_i| − J)`, `c = 1000 − reserve` — the
    // allowance `(1 − κ)·J` exact, no clamp, no rounding — as two running
    // maxima `U = max(1000·G + c·raw)`, `V = max(1000·G − c·raw)`.
    let c = i128::from(1_000 - u32::try_from(reserve).unwrap());
    let ji = i128::from(j);
    let (mut d_last, mut p, mut g_last) = (0u64, 0u64, 0u64);
    let (mut u, mut v): (Option<i128>, Option<i128>) = (None, None);
    let mut d_max = 0u64;
    let mut shape = Shape::default();
    let mut passed = 0u64;
    for &(line, attempt, due, deadline) in &order {
        if !handed.contains_key(&(line, attempt)) {
            continue;
        }
        let r = raw(line, attempt, due);
        let a = if pairwise {
            let ri = i128::from(r);
            [
                v.map(|v| v + c * ri - c * ji),
                u.map(|u| u - c * ri - c * ji),
            ]
            .into_iter()
            .flatten()
            .max()
            .map_or(r, |m| {
                r.max(u64::try_from((m.max(0) + 999) / 1_000).unwrap())
            })
        } else {
            let d = r.max(d_last);
            d.max(p + (1_000 - reserve) * (d - d_last) / 1_000)
        };
        let g = free(a.max(g_last));
        if g >= deadline {
            shape.missed += 1;
            continue;
        }
        if r < d_max {
            shape.inverted += 1;
        }
        passed += 1;
        shape.largest = shape.largest.max(g - r);
        if g > r + j {
            shape.late += 1;
        }
        if pairwise {
            let (gi, ri) = (1_000 * i128::from(g), i128::from(r));
            u = Some(u.map_or(gi + c * ri, |u| u.max(gi + c * ri)));
            v = Some(v.map_or(gi - c * ri, |v| v.max(gi - c * ri)));
        } else {
            let d = r.max(d_last);
            p = a.max(g.saturating_sub(j));
            d_last = d;
        }
        d_max = d_max.max(r);
        g_last = g;
    }
    shape.late = (shape.late * 1_000).checked_div(passed).unwrap_or(0);
    shape
}

/// A shaper's replay: attempts never handed, the largest shift past the raw
/// due, the share handed more than J past it (per mille), and the attempts
/// handed behind a later due already handed.
#[derive(Default)]
struct Shape {
    missed: usize,
    largest: u64,
    late: u64,
    inverted: usize,
}

/// A healthy run of `steps` reads `gap` µs apart: the gate replayed.
/// The gate's replay — attempts, displaced, refused, the largest displacement
/// — and the shaper's per reserve: missed, the largest shift, the share late.
type Replayed = ((usize, usize, usize, u64), Vec<(u64, bool, Shape)>);

fn healthy(gap: u64, steps: u64) -> Replayed {
    healthy_with(gap, steps, 0, 0)
}

/// [`healthy`], every `every`-th answer `size` bytes of numbers.
fn healthy_with(gap: u64, steps: u64, every: u64, size: u64) -> Replayed {
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
            .arg(every.to_string())
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
        .for_each(|o| o.timeout = 500_000);
    let records: Vec<Record> = (0..steps)
        .map(|i| call("read", i + 1, 0, i * gap, 0))
        .collect();
    let header = Arc::new(header);
    const CAP: usize = 4_000;
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
                pending: 2_000,
                ..Default::default()
            },
        ),
        W,
        Arc::new(carrier),
        live.runtimes(),
    )
    .unwrap();
    let clock = Arc::new(RealClock::start(header.profile.jitter).unwrap());
    let root = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let lines = root
        .block_on(run(
            Ready::assumed(admitted, clock, Lines::new(), Namespace(1)),
            Records::new(records.into_iter()),
        ))
        .unwrap()
        .journal;
    let j = header.profile.jitter;
    let shapes = [10, 50, 100, 200]
        .into_iter()
        .flat_map(|r| [false, true].map(|pairwise| (r, pairwise)))
        .map(|(r, pairwise)| (r, pairwise, shaped(&lines, j, r, pairwise)))
        .collect();
    (paced(&lines, j), shapes)
}

#[test]
#[ignore = "decision 21's measurement, on purpose: the gate replayed over healthy runs"]
fn the_gate_replayed_over_healthy_runs() {
    for (gap, steps) in [(2_000, 15_000), (500, 20_000), (100, 50_000)] {
        for run in 0..3 {
            let ((attempts, displaced, refused, largest), shapes) = healthy(gap, steps);
            for (reserve, pairwise, s) in shapes {
                let form = if pairwise { "pairwise" } else { "recursion" };
                eprintln!(
                    "MEASURED shape: a step every {gap} µs, run {run}, {form}, reserve {reserve}‰: {} never \
                     handed, the largest shift {} µs, {}‰ handed past J, {} inverted",
                    s.missed, s.largest, s.late, s.inverted
                );
            }
            eprintln!(
                "MEASURED pace: a step every {gap} µs, run {run}: {attempts} attempts, {displaced} displaced \
                 past J, {refused} the gate would refuse, the largest displacement {largest} µs"
            );
        }
    }
}

/// The roles' judging-hold measurement (decision 21, round 3: Operability,
/// Performance, Distributed Systems): a healthy run whose every hundredth
/// answer is large — its read and judgement hold the loop — replayed through
/// the pairwise pace. Measured on purpose (`--ignored`).
#[test]
#[ignore = "a measurement, run on purpose"]
fn the_pace_replayed_with_large_answers() {
    for size in [16_384, 65_536, 262_144] {
        for run in 0..2 {
            let ((attempts, displaced, _, largest), shapes) =
                healthy_with(2_000, 15_000, 100, size);
            eprintln!(
                "MEASURED large: a step every 2000 µs, one answer in 100 of {size} B, run {run}: {attempts} \
                 attempts, {displaced} displaced past J, the largest displacement {largest} µs"
            );
            for (reserve, pairwise, s) in shapes {
                if pairwise {
                    eprintln!(
                        "MEASURED large shape: {size} B, run {run}, reserve {reserve}‰: {} never handed, the \
                         largest shift {} µs, {}‰ handed past J",
                        s.missed, s.largest, s.late
                    );
                }
            }
        }
    }
}
