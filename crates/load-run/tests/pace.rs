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

/// The stand, a process of its own: answers each request at once.
const STAND: &str = r#"
import socket, sys, threading
port = int(sys.argv[1])
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
                c.sendall(b"HTTP/1.1 200 X\r\ncontent-length: 2\r\n\r\n{}")
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
fn shaped(lines: &[journal::Line], j: u64, reserve: u64) -> (usize, u64, u64) {
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
    let (mut d_last, mut p, mut g_last) = (0u64, 0u64, 0u64);
    let (mut missed, mut largest, mut late, mut passed) = (0usize, 0u64, 0u64, 0u64);
    for &(line, attempt, due, deadline) in &order {
        if !handed.contains_key(&(line, attempt)) {
            continue;
        }
        let r = raw(line, attempt, due);
        let d = r.max(d_last);
        let a = d.max(p + (1_000 - reserve) * (d - d_last) / 1_000);
        let g = free(a.max(g_last));
        if g >= deadline {
            missed += 1;
            continue;
        }
        passed += 1;
        largest = largest.max(g - r);
        if g > r + j {
            late += 1;
        }
        p = a.max(g.saturating_sub(j));
        d_last = d;
        g_last = g;
    }
    (
        missed,
        largest,
        (late * 1_000).checked_div(passed).unwrap_or(0),
    )
}

/// A healthy run of `steps` reads `gap` µs apart: the gate replayed.
/// The gate's replay — attempts, displaced, refused, the largest displacement
/// — and the shaper's per reserve: missed, the largest shift, the share late.
type Replayed = ((usize, usize, usize, u64), Vec<(u64, usize, u64, u64)>);

fn healthy(gap: u64, steps: u64) -> Replayed {
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
    let shapes = [0, 10, 50, 100, 200]
        .into_iter()
        .map(|r| {
            let (missed, largest, late) = shaped(&lines, j, r);
            (r, missed, largest, late)
        })
        .collect();
    (paced(&lines, j), shapes)
}

#[test]
#[ignore = "decision 21's measurement, on purpose: the gate replayed over healthy runs"]
fn the_gate_replayed_over_healthy_runs() {
    for (gap, steps) in [(2_000, 15_000), (500, 20_000)] {
        for run in 0..3 {
            let ((attempts, displaced, refused, largest), shapes) = healthy(gap, steps);
            for (reserve, missed, shift, late) in shapes {
                eprintln!(
                    "MEASURED shape: a step every {gap} µs, run {run}, reserve {reserve}‰: {missed} never \
                     handed, the largest shift {shift} µs, {late}‰ handed past J"
                );
            }
            eprintln!(
                "MEASURED pace: a step every {gap} µs, run {run}: {attempts} attempts, {displaced} displaced \
                 past J, {refused} the gate would refuse, the largest displacement {largest} µs"
            );
        }
    }
}
