//! K2b-3b on the real clock over loopback TCP (ADR-0047 decision 17): an
//! answer in the host before its deadline, the source's runtime busy across
//! it, is read at the attempt's look past the deadline — a turn of the driver
//! verified first — and journaled late with where it was not; without the
//! turn, the attempt, woken by the clock's own thread before any turn, would
//! see nothing and leave it unread.
//!
//! The state is built causally, not by timing: the source's runtime turns its
//! driver only when idle (`event_interval` at its maximum); a spinner holds it
//! from before the answer's bytes come to past the look; the bytes are seen in
//! the socket by `peek` before the deadline. A run whose preconditions did not
//! hold is retried, never counted.

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use std::future::{Future, poll_fn};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};

use ascetic_ddd_command_log::record::{Record, Rest};
use ascetic_ddd_command_log::render::RunSecret;
use ascetic_ddd_load_run::clock::{Clock, RealClock};
use ascetic_ddd_load_run::rest::{Config, RestCarrier};
use ascetic_ddd_load_run::run::{Runtimes, admit, run};
use ascetic_ddd_load_run::sink::Lines;
use ascetic_ddd_load_run::source::Records;
use ascetic_ddd_load_run::stand::{Connect, Egress, Route, Stand};
use ascetic_ddd_scheduler::journal::Line;
use fixture::*;

struct OneStand;

impl Stand for OneStand {
    fn route(&self, _: &Record, _: &Rest) -> Result<Route, String> {
        Ok(Route {
            authority: "stand.test".to_owned(),
            endpoint: "gateway".to_owned(),
        })
    }
    fn gate(
        &self,
        _: usize,
        _: ascetic_ddd_load_run::stand::Opening<'_>,
    ) -> impl Future<Output = Option<Result<String, ascetic_ddd_load_run::stand::Refused>>> + Send
    {
        std::future::ready(None)
    }

    fn endpoints(&self, _: &ascetic_ddd_command_log::record::Header, _: u32) -> Vec<String> {
        vec!["gateway".to_owned()]
    }
}

/// Loopback TCP, a clone of each connection's socket kept for `peek`.
struct Tcp {
    addr: SocketAddr,
    peer: Arc<Mutex<Option<std::net::TcpStream>>>,
}

impl Egress for Tcp {
    type Io = tokio::net::TcpStream;

    fn ports(&self, _: u32) -> usize {
        62
    }

    fn connect(
        &self,
        _: u32,
        _: &Route,
    ) -> impl Future<Output = Result<tokio::net::TcpStream, Connect>> + Send {
        let (addr, peer) = (self.addr, self.peer.clone());
        async move {
            let connected = async {
                let s = tokio::net::TcpStream::connect(addr).await?.into_std()?;
                *peer.lock().unwrap() = Some(s.try_clone()?);
                tokio::net::TcpStream::from_std(s)
            };
            connected.await.map_err(Connect::of_connect)
        }
    }

    fn admits(&self, _: u32) -> Result<(), ascetic_ddd_load_run::stand::Unfit> {
        Ok(())
    }

    fn reaches(&self, _: u32, _: &str) -> Result<(), ascetic_ddd_load_run::stand::Unfit> {
        Ok(())
    }
}

const D: u64 = 70_000;

/// The real clock, a sleep due at `prod` waking a sibling task as it ends —
/// from the attempt's own poll, so the sibling runs next on its runtime and
/// wakes the attempt again before the driver turns: a wake from anyone but
/// the runtime's defer list between an attempt's yield and the turn.
struct Prodding {
    inner: Arc<RealClock>,
    prod: u64,
    attempt: Arc<Mutex<Option<Waker>>>,
    sibling: Arc<Mutex<Option<Waker>>>,
}

impl Clock for Prodding {
    fn now(&self) -> u64 {
        self.inner.now()
    }

    fn wall(&self) -> i64 {
        self.inner.wall()
    }

    fn idle_since(&self) -> u64 {
        self.inner.idle_since()
    }

    async fn sleep_until(&self, at: u64) {
        self.inner.sleep_until(at).await;
        if at == self.prod {
            poll_fn(|cx| {
                *self.attempt.lock().unwrap() = Some(cx.waker().clone());
                if let Some(w) = self.sibling.lock().unwrap().take() {
                    w.wake();
                }
                Poll::Ready(())
            })
            .await;
        }
    }

    fn settled(&self, at: u64) -> impl Future<Output = ()> + Send + '_ {
        self.inner.settled(at)
    }
}

/// One run: the answer written at `D − 500`, the runtime spun from `D − 800`
/// to `D + 6,000`. Its journal, or none if a precondition failed.
fn attempt_once(prodded: bool) -> Option<Vec<Line>> {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let clock = Arc::new(RealClock::start(2_000).unwrap());
    let written = Arc::new(AtomicU64::new(u64::MAX));
    let (c, w) = (clock.clone(), written.clone());
    std::thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        while !buf.windows(4).any(|x| x == b"\r\n\r\n") {
            let n = s.read(&mut chunk).unwrap();
            if n == 0 {
                return;
            }
            buf.extend_from_slice(&chunk[..n]);
        }
        while c.now() < D - 500 {
            std::hint::spin_loop();
        }
        s.write_all(b"HTTP/1.1 200 X\r\ncontent-length: 2\r\n\r\n{}")
            .unwrap();
        w.store(c.now(), Ordering::SeqCst);
        let _ = s.read(&mut chunk);
    });
    // The source: a current_thread runtime whose driver turns only when it
    // is idle.
    let (tx, rx) = std::sync::mpsc::channel();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .event_interval(u32::MAX)
            .build()
            .unwrap();
        tx.send(rt.handle().clone()).unwrap();
        rt.block_on(async {
            let _ = stopped.await;
        });
    });
    let source = rx.recv().unwrap();
    // The sibling: once prodded, it wakes the attempt.
    let (attempt, sibling) = (
        Arc::new(Mutex::new(None::<Waker>)),
        Arc::new(Mutex::new(None::<Waker>)),
    );
    if prodded {
        let (a, s) = (attempt.clone(), sibling.clone());
        drop(source.spawn(poll_fn(move |cx| {
            if let Some(w) = a.lock().unwrap().take() {
                w.wake();
                return Poll::Ready(());
            }
            *s.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        })));
    }
    let peer = Arc::new(Mutex::new(None));
    let mut header = header();
    header.channels.clear();
    header.calls.clear();
    header.profile.jitter = 2_000;
    header
        .operations
        .iter_mut()
        .for_each(|o| o.timeout = 50_000);
    let header = Arc::new(header);
    let carrier = RestCarrier::new(
        header.clone(),
        OneStand,
        Tcp {
            addr,
            peer: peer.clone(),
        },
        Config::new(RunSecret::new([7; 32])),
    );
    // The spinner and the peek, from the test's own thread.
    let spun = Arc::new(AtomicU64::new(u64::MAX));
    let peeked = Arc::new(AtomicU64::new(0));
    let (c, s, h, p, pk) = (
        clock.clone(),
        spun.clone(),
        source.clone(),
        peer.clone(),
        peeked.clone(),
    );
    let driver = std::thread::spawn(move || {
        while c.now() < D - 800 {
            std::hint::spin_loop();
        }
        let (c2, s2) = (c.clone(), s.clone());
        drop(h.spawn(async move {
            s2.store(c2.now(), Ordering::SeqCst);
            while c2.now() < D + 6_000 {
                std::hint::spin_loop();
            }
        }));
        while c.now() < D - 100 {
            std::hint::spin_loop();
        }
        if let Some(sock) = p.lock().unwrap().as_ref() {
            let mut b = [0u8; 8];
            if let Ok(n) = sock.peek(&mut b) {
                pk.store(n as u64, Ordering::SeqCst);
            }
        }
    });
    let records = [call("read", 1, 0, 20_000, 0)];
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let shell = Arc::new(Prodding {
        inner: clock.clone(),
        prod: if prodded { D + 1 } else { u64::MAX },
        attempt,
        sibling,
    });
    let journal = rt
        .block_on(run(
            ascetic_ddd_load_run::run::Ready::assumed(
                admit(
                    ascetic_ddd_load_run::source::Plan::assumed(header, Default::default()),
                    W,
                    Arc::new(carrier),
                    &Runtimes::assumed(vec![source], 1_024),
                )
                .unwrap(),
                shell,
                Lines::new(),
                ascetic_ddd_command_log::fills::Namespace(1),
            ),
            Records::new(records.iter().cloned()),
        ))
        .unwrap()
        .journal;
    driver.join().unwrap();
    let _ = stop.send(());
    // The state the twin is about: the runtime busy before the bytes came,
    // the bytes in the socket before the deadline.
    let (spun, written, peeked) = (
        spun.load(Ordering::SeqCst),
        written.load(Ordering::SeqCst),
        peeked.load(Ordering::SeqCst),
    );
    (spun < written && written < D && peeked > 0).then_some(journal)
}

#[test]
fn an_answer_in_the_host_by_its_deadline_is_read_at_the_look() {
    look(false);
}

/// The same, the attempt woken again between its yield and the driver's
/// turn: only the turn's own flag tells the turn from that wake.
#[test]
fn a_wake_between_the_yield_and_the_turn_is_no_turn() {
    look(true);
}

fn look(prodded: bool) {
    for _ in 0..8 {
        let Some(journal) = attempt_once(prodded) else {
            continue;
        };
        let found = journal.iter().find_map(|l| match l {
            Line::Answer(a) => a.found.map(|f| (a.at, f)),
            _ => None,
        });
        let (at, f) =
            found.unwrap_or_else(|| panic!("the answer in the host was not read: {journal:?}"));
        assert_eq!(f.deadline, D);
        assert!(f.absent < D - 500, "absent at {}", f.absent);
        assert!(at >= D + 6_000, "read at {at}");
        return;
    }
    panic!("no run held its preconditions: the box too loaded for the twin");
}

/// Decision 16, the live composition: calls over the library's own TCP
/// egress, from a source bound to its address, on a source runtime of
/// `Live`'s, each answered.
#[test]
fn calls_over_tcp_from_a_live_source_are_answered() {
    use ascetic_ddd_load_run::live::Live;
    use ascetic_ddd_load_run::tcp::TcpEgress;
    use ascetic_ddd_scheduler::journal::End;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let peers = Arc::new(Mutex::new(Vec::new()));
    let p = peers.clone();
    std::thread::spawn(move || {
        for s in listener.incoming() {
            let Ok(mut s) = s else { return };
            p.lock().unwrap().push(s.peer_addr().unwrap().ip());
            std::thread::spawn(move || {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    while !buf.windows(4).any(|x| x == b"\r\n\r\n") {
                        match s.read(&mut chunk) {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let end = buf.windows(4).position(|x| x == b"\r\n\r\n").unwrap() + 4;
                    buf.drain(..end);
                    if s.write_all(b"HTTP/1.1 200 X\r\ncontent-length: 2\r\n\r\n{}")
                        .is_err()
                    {
                        return;
                    }
                }
            });
        }
    });
    let live = Live::start(1, 64).unwrap();
    let clock = Arc::new(RealClock::start(2_000).unwrap());
    let mut header = header();
    header.channels.clear();
    header.calls.clear();
    let header = Arc::new(header);
    let egress = TcpEgress::new(
        [("gateway".to_owned(), addr)].into_iter().collect(),
        // Its own address, one the system would not choose (Linux routes all
        // of 127.0.0.0/8 to the loopback).
        vec![Some(std::net::IpAddr::from([127, 0, 0, 2]))],
    )
    .with_ports(62);
    let carrier = RestCarrier::new(
        header.clone(),
        OneStand,
        egress,
        Config::new(RunSecret::new([7; 32])),
    );
    let records = [
        call("read", 1, 0, 0, 0),
        call("read", 2, 0, 500, 0),
        call("read", 3, 0, 1_000, 0),
    ];
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let started = clock.now();
    let journal = rt
        .block_on(run(
            ascetic_ddd_load_run::run::Ready::assumed(
                admit(
                    ascetic_ddd_load_run::source::Plan::assumed(header, Default::default()),
                    W,
                    Arc::new(carrier),
                    live.runtimes(),
                )
                .unwrap(),
                clock.clone(),
                Lines::new(),
                ascetic_ddd_command_log::fills::Namespace(1),
            ),
            Records::new(records.iter().cloned()),
        ))
        .unwrap()
        .journal;
    // Its last call ended within milliseconds, and the run with it — not at
    // a backstop's wake its end had cancelled, its deadline and grace on.
    let took = clock.now() - started;
    assert!(took < 2_000_000, "the run took {took} µs");
    let ends: Vec<End> = journal
        .iter()
        .filter_map(|l| match l {
            Line::End(e) => Some(e.end),
            _ => None,
        })
        .collect();
    assert_eq!(ends, vec![End::Expected; 3]);
    let peers = peers.lock().unwrap();
    assert!(!peers.is_empty());
    assert!(
        peers
            .iter()
            .all(|ip| *ip == std::net::IpAddr::from([127, 0, 0, 2])),
        "{peers:?}"
    );
}
