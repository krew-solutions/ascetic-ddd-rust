//! Decision 20's I1 on the real clock: a source's driver takes at most its
//! events a turn, ready sockets past them left for the next; a look's
//! verified turn sees every ready socket only when the cap and the driver's
//! own fit the events. `Live::for_caps` builds that; a runtime of fewer,
//! claiming more, misses an answer in the host — a false Root.
//!
//! The state is built causally, as in K2b-3b's twin: a spinner holds the
//! source's thread from before the answers to past the deadline; `K − 1`
//! answers come, then the driver's own wake (a task spawned from outside),
//! then the target's answer — all in the host by the deadline, `peek` says.
//! A run whose preconditions did not hold is retried, never counted.

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use ascetic_ddd_load_run::sink::Journal;
use std::collections::BTreeMap;
use std::future::Future;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use ascetic_ddd_command_log::fills::Namespace;
use ascetic_ddd_command_log::record::{Record, Rest};
use ascetic_ddd_command_log::render::{self, RunSecret};
use ascetic_ddd_load_run::clock::{Clock, RealClock};
use ascetic_ddd_load_run::live::Live;
use ascetic_ddd_load_run::rest::{Config, RestCarrier};
use ascetic_ddd_load_run::run::{Ready, Runtimes, admitted, run};
use ascetic_ddd_load_run::sink::Lines;
use ascetic_ddd_load_run::source::{Plan, Records};
use ascetic_ddd_load_run::stand::{Connect, Egress, Route, Stand};
use ascetic_ddd_scheduler::journal::Line;
use fixture::*;

/// The sockets a source holds: its cap.
const K: usize = 4;
const D: u64 = 70_000;

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

/// Loopback TCP, its cap `K`, a clone of each socket kept for `peek`.
struct Tcp {
    addr: SocketAddr,
    peers: Arc<Mutex<Vec<std::net::TcpStream>>>,
}

impl Egress for Tcp {
    type Io = tokio::net::TcpStream;

    fn io_bytes(&self) -> u64 {
        0
    }

    fn caps(&self, ports: usize, sources: u32) -> Vec<usize> {
        vec![ports; sources as usize]
    }

    fn ports(&self, _: u32) -> usize {
        K
    }

    fn admits(&self, _: u32) -> Result<(), ascetic_ddd_load_run::stand::Unfit> {
        Ok(())
    }

    fn reaches(&self, _: u32, _: &str) -> Result<(), ascetic_ddd_load_run::stand::Unfit> {
        Ok(())
    }

    fn connect(
        &self,
        _: u32,
        _: &Route,
    ) -> impl Future<Output = Result<tokio::net::TcpStream, Connect>> + Send {
        let (addr, peers) = (self.addr, self.peers.clone());
        async move {
            let connected = async {
                let s = tokio::net::TcpStream::connect(addr).await?.into_std()?;
                peers.lock().unwrap().push(s.try_clone()?);
                tokio::net::TcpStream::from_std(s)
            };
            connected.await.map_err(Connect::of_connect)
        }
    }
}

/// Spins its thread on `clock` until `until`.
fn spin(clock: &RealClock, until: u64) {
    while clock.now() < until {
        std::hint::spin_loop();
    }
}

/// How the source's runtime is made.
enum Source {
    /// `Live`'s, from the cap.
    Live,
    /// One taking `K` events a turn, claiming the cap and the driver's own.
    Short,
}

/// One run: `K` reads, the target the last; its answer read, or none if a
/// precondition failed.
fn once(how: &Source) -> Option<bool> {
    let records: Vec<Record> = (1..=K as u64)
        .map(|g| call("read", g, 0, 20_000, 0))
        .collect();
    let target = render::correlation(Namespace(1), &records[K - 1].step, 1);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let clock = Arc::new(RealClock::start(2_000).unwrap());
    // When the others were answered, the driver woken, the target answered.
    let marks: Arc<[AtomicU64; 3]> = Arc::new([
        AtomicU64::new(u64::MAX),
        AtomicU64::new(u64::MAX),
        AtomicU64::new(u64::MAX),
    ]);
    let (c, m, t) = (clock.clone(), marks.clone(), target.clone());
    let stand = std::thread::spawn(move || {
        let mut by_id: BTreeMap<String, std::net::TcpStream> = BTreeMap::new();
        for _ in 0..K {
            let Ok((mut s, _)) = listener.accept() else {
                return;
            };
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            while !buf.windows(4).any(|x| x == b"\r\n\r\n") {
                match s.read(&mut chunk) {
                    Ok(0) | Err(_) => return,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                }
            }
            let head = String::from_utf8_lossy(&buf).to_lowercase();
            let id = head
                .lines()
                .find_map(|l| l.strip_prefix("x-correlation-id:"))
                .map(|v| v.trim().to_owned())
                .unwrap_or_default();
            by_id.insert(id, s);
        }
        let answer = b"HTTP/1.1 200 X\r\ncontent-length: 2\r\n\r\n{}";
        spin(&c, D - 600);
        for (id, s) in &mut by_id {
            if *id != t {
                let _ = s.write_all(answer);
            }
        }
        m[0].store(c.now(), Ordering::SeqCst);
        spin(&c, D - 200);
        if let Some(s) = by_id.get_mut(&t) {
            let _ = s.write_all(answer);
        }
        m[2].store(c.now(), Ordering::SeqCst);
        // Held open a while: the client's reset, or the bound, ends them.
        let mut sink = [0u8; 64];
        for s in by_id.values_mut() {
            let _ = s.set_read_timeout(Some(std::time::Duration::from_millis(200)));
            let _ = s.read(&mut sink);
        }
    });
    // The source's runtime.
    let (live, handle, runtimes) = match how {
        Source::Live => {
            let live = Live::for_caps(&[K]).unwrap();
            let handle = live.runtimes().sources()[0].clone();
            let runtimes = live.runtimes().clone();
            (Some(live), handle, runtimes)
        }
        Source::Short => {
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .max_io_events_per_tick(K)
                    .build()
                    .unwrap();
                tx.send(rt.handle().clone()).unwrap();
                rt.block_on(std::future::pending::<()>());
            });
            let handle = rx.recv().unwrap();
            let runtimes = Runtimes::assumed(vec![handle.clone()], K + 2);
            (None, handle, runtimes)
        }
    };
    let mut header = header();
    header.channels.clear();
    header.calls.clear();
    // The look before the deadline at `D − 1,000`, before the spin: past it,
    // only the last look's one verified turn decides.
    header.profile.jitter = 2_000;
    header
        .operations
        .iter_mut()
        .for_each(|o| o.timeout = 50_000);
    let header = Arc::new(header);
    let peers = Arc::new(Mutex::new(Vec::new()));
    let carrier = RestCarrier::new(
        header.clone(),
        OneStand,
        Tcp {
            addr,
            peers: peers.clone(),
        },
        Config::new(RunSecret::new([7; 32])),
    );
    // The spinner from before the answers to past the deadline; the
    // driver's own wake between the others' answers and the target's; the
    // bytes in the host by the deadline.
    let spun = Arc::new(AtomicU64::new(u64::MAX));
    let peeked = Arc::new(AtomicU64::new(0));
    let (c, s, h, m, p, pk) = (
        clock.clone(),
        spun.clone(),
        handle.clone(),
        marks.clone(),
        peers.clone(),
        peeked.clone(),
    );
    let driver = std::thread::spawn(move || {
        spin(&c, D - 800);
        let (c2, s2) = (c.clone(), s.clone());
        drop(h.spawn(async move {
            s2.store(c2.now(), Ordering::SeqCst);
            spin(&c2, D + 6_000);
        }));
        spin(&c, D - 400);
        drop(h.spawn(async {}));
        m[1].store(c.now(), Ordering::SeqCst);
        spin(&c, D - 100);
        let ready = p
            .lock()
            .unwrap()
            .iter()
            .filter(|s| {
                let mut b = [0u8; 8];
                s.peek(&mut b).is_ok_and(|n| n > 0)
            })
            .count();
        pk.store(ready as u64, Ordering::SeqCst);
    });
    let machine = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let plan = Plan::assumed(
        header,
        ascetic_ddd_command_log::validate::Peaks {
            pending: K as u64,
            sockets: vec![K as u64],
            ..Default::default()
        },
    );
    let admitted = admitted(plan, W, Arc::new(carrier), &runtimes, Journal::new(64, 0)).unwrap();
    let journal = machine
        .block_on(run(
            Ready::assumed(admitted, clock.clone(), Lines::new(), Namespace(1)),
            Records::new(records.into_iter()),
        ))
        .unwrap()
        .journal;
    driver.join().unwrap();
    drop(live);
    let _ = stand.join();
    let (spun, others, woke, answered) = (
        spun.load(Ordering::SeqCst),
        marks[0].load(Ordering::SeqCst),
        marks[1].load(Ordering::SeqCst),
        marks[2].load(Ordering::SeqCst),
    );
    let held = spun < others && others < woke && woke < answered && answered < D;
    if !held || peeked.load(Ordering::SeqCst) != K as u64 {
        return None;
    }
    // The target's line, the last read: its answer found at the look past
    // the spinner, or missed.
    let line = 1 + K as u64;
    Some(
        journal
            .iter()
            .any(|l| matches!(l, Line::Answer(a) if a.line == line && a.found.is_some())),
    )
}

/// `Live`'s runtime reads every answer in the host at the look; a runtime
/// taking fewer events than the cap and its driver's own misses one in some
/// run.
#[test]
fn a_look_sees_every_ready_socket_only_within_the_events() {
    let mut live = 0;
    for _ in 0..8 {
        if let Some(read) = once(&Source::Live) {
            assert!(read, "Live's runtime missed an answer in the host");
            live += 1;
        }
    }
    assert!(
        live > 0,
        "no run held its preconditions: the box too loaded"
    );
    let missed = (0..8)
        .filter_map(|_| once(&Source::Short))
        .any(|read| !read);
    assert!(missed, "a runtime of K events never missed the K + 1st");
}
