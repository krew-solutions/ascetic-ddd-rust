//! Decision 18 on the real clock: a send path's lag is the scheduling lag of
//! its wakes on its source's thread — counted from the thread's last park
//! that blocked, never its busy time while the path waited on the stand.
//!
//! The source runtime is `Live`'s, its park hooks seeing whether each park
//! blocked. A hog of 100 ms holds the thread; the path's wait ends inside it
//! (its lag about the hog's rest) or after an idle stretch past it (its lag
//! about nothing). Assertions hold at half the hog, which load only widens on
//! the right side; a run whose preconditions did not hold is retried.

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use ascetic_ddd_load_run::sink::Journal;
use std::future::Future;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use ascetic_ddd_command_log::record::{Header, Record, Rest};
use ascetic_ddd_command_log::render::RunSecret;
use ascetic_ddd_load_run::carrier::Carrier;
use ascetic_ddd_load_run::clock::{Clock, RealClock};
use ascetic_ddd_load_run::decorate::{Decoration, Decorator};
use ascetic_ddd_load_run::live::Live;
use ascetic_ddd_load_run::rest::{Config, RestCarrier};
use ascetic_ddd_load_run::run::{admit, run};
use ascetic_ddd_load_run::sink::Lines;
use ascetic_ddd_load_run::source::Records;
use ascetic_ddd_load_run::stamps::Attempt;
use ascetic_ddd_load_run::stand::{Connect, Egress, Route, Stand, StandConnect};
use ascetic_ddd_scheduler::journal::{self, Line};
use ascetic_ddd_scheduler::machine::{Judgement, Seen};
use fixture::*;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

/// The hog: how long it holds its source's thread, from 5,000 µs.
const HOG: u64 = 100_000;
const HOG_AT: u64 = 5_000;

/// Spins its thread on the clock until `until`.
fn spin(clock: &RealClock, until: u64) {
    while clock.now() < until {
        std::hint::spin_loop();
    }
}

/// The send of `line` in `journal`.
fn sent(journal: &[Line], line: u64) -> Option<journal::Sent> {
    journal.iter().find_map(|l| match l {
        Line::Sent(s) if s.line == line => Some(s.clone()),
        _ => None,
    })
}

/// When `line` was dispatched.
fn dispatched(journal: &[Line], line: u64) -> Option<u64> {
    journal.iter().find_map(|l| match l {
        Line::Dispatch(d) if d.line == line => Some(d.at),
        _ => None,
    })
}

/// Line 2 hogs its thread from 5,000 µs for 100 ms; line 3, handed at once,
/// waits on a socket a thread of its own writes to at `write_at`, then
/// reports itself connected and sent.
struct Waiting {
    clock: Arc<RealClock>,
    write_at: u64,
}

impl Carrier<RealClock> for Waiting {
    fn carries(&self, _: &Header) -> Result<(), String> {
        Ok(())
    }

    fn needs(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
        _: u32,
    ) -> Result<ascetic_ddd_load_run::carrier::Needs, String> {
        Ok(ascetic_ddd_load_run::carrier::Needs::Nothing)
    }

    fn source_costs(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
        _: u32,
    ) -> Result<ascetic_ddd_load_run::carrier::SourceCosts, String> {
        Ok(ascetic_ddd_load_run::carrier::SourceCosts::NOTHING)
    }

    fn probe(
        &self,
        _: u32,
        _: std::sync::Arc<RealClock>,
        _: u64,
    ) -> impl std::future::Future<Output = Vec<ascetic_ddd_load_run::carrier::Unprobed>> + Send + 'static
    {
        std::future::ready(Vec::new())
    }

    fn carry(&self, mut a: Attempt<RealClock>) -> impl Future<Output = ()> + Send + 'static {
        let (clock, write_at) = (self.clock.clone(), self.write_at);
        async move {
            if a.line() == 2 {
                a.clock().sleep_until(HOG_AT).await;
                spin(&clock, HOG_AT + HOG);
            } else {
                let (mut r, w) = tokio::net::UnixStream::pair().unwrap();
                let mut w = w.into_std().unwrap();
                w.set_nonblocking(false).unwrap();
                std::thread::spawn(move || {
                    spin(&clock, write_at);
                    let _ = w.write_all(b"x");
                });
                let mut byte = [0u8; 1];
                r.read_exact(&mut byte).await.unwrap();
                a.connected();
                a.sent();
            }
            a.ended(Judgement::Expected(vec![]), Seen::default());
        }
    }
}

/// The lag of line 3's send, its wait ending at `write_at`; `None` if line 3
/// was not handed before the hog.
fn waiting_lag(write_at: u64) -> Option<u64> {
    let live = Live::start(1, 64).unwrap();
    let clock = Arc::new(RealClock::start(2_000).unwrap());
    let mut h = header();
    h.operations.iter_mut().for_each(|o| o.timeout = 5_000_000);
    let records = [call("read", 1, 0, 0, 0), call("read", 2, 0, 0, 0)];
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let journal = rt
        .block_on(run(
            ascetic_ddd_load_run::run::Ready::assumed(
                admit(
                    ascetic_ddd_load_run::source::Plan::assumed(Arc::new(h), Default::default()),
                    W,
                    Arc::new(Waiting {
                        clock: clock.clone(),
                        write_at,
                    }),
                    live.runtimes(),
                    Journal::new(64, 0),
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
    let s = sent(&journal, 3)?;
    (s.handed < HOG_AT).then_some(s.lag)
}

/// A wait ending inside the hog: the thread busy, no park that blocked since
/// the hog began — its lag about the hog's rest (a lag taken from the last
/// park whatever it was would be about nothing: the park after the hog
/// returns at once with the byte). Ending after the hog and an idle stretch:
/// about nothing (the thread's busy time would count the hog).
#[test]
fn a_wait_s_lag_is_the_thread_s_since_it_last_blocked() {
    for (write_at, inside) in [(HOG_AT + 15_000, true), (HOG_AT + HOG + 60_000, false)] {
        let lag = (0..5)
            .find_map(|_| waiting_lag(write_at))
            .expect("line 3 handed before the hog, in some run");
        if inside {
            assert!(lag >= HOG / 2, "inside the hog: lag {lag} µs");
        } else {
            assert!(lag < HOG / 2, "after it: lag {lag} µs");
        }
    }
}

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

/// A connection whose writes past a request's head wait for a thread of the
/// test's to open them: the stand's receive window, closed on the body.
struct Gated {
    inner: tokio::net::TcpStream,
    open: Arc<AtomicBool>,
    waker: Arc<Mutex<Option<Waker>>>,
    headed: bool,
}

impl AsyncRead for Gated {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for Gated {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let head_end = buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4);
        let n = if self.open.load(Ordering::SeqCst) {
            buf.len()
        } else if !self.headed {
            self.headed = head_end.is_some();
            head_end.unwrap_or(buf.len())
        } else {
            *self.waker.lock().unwrap() = Some(cx.waker().clone());
            // Opened between the check and the waker kept: go on.
            if !self.open.load(Ordering::SeqCst) {
                return Poll::Pending;
            }
            buf.len()
        };
        Pin::new(&mut self.inner).poll_write(cx, &buf[..n])
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Loopback TCP, each connection gated.
struct GatedTcp {
    addr: SocketAddr,
    open: Arc<AtomicBool>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl Egress for GatedTcp {
    type Io = Gated;

    fn ports(&self, _: u32) -> usize {
        62
    }

    fn connect(&self, _: u32, _: &Route) -> impl Future<Output = Result<Gated, Connect>> + Send {
        let (addr, open, waker) = (self.addr, self.open.clone(), self.waker.clone());
        async move {
            let inner = tokio::net::TcpStream::connect(addr)
                .await
                .map_err(Connect::of_connect)?;
            inner.set_nodelay(true).map_err(Connect::of_connect)?;
            Ok(Gated {
                inner,
                open,
                waker,
                headed: false,
            })
        }
    }

    fn admits(&self, _: u32) -> Result<(), ascetic_ddd_load_run::stand::Unfit> {
        Ok(())
    }

    fn reaches(&self, _: u32, _: &str) -> Result<(), ascetic_ddd_load_run::stand::Unfit> {
        Ok(())
    }
}

/// Spins its thread 100 ms from 5,000 µs: the host's own work on the
/// source's thread, decorating the hog's call.
struct Hogging(Arc<RealClock>);

impl Decorator for Hogging {
    async fn decorate<'a>(&'a self, d: Decoration<'a>) -> Result<Vec<(String, String)>, String> {
        // Only the hog's call, planned at its instant, spins.
        if d.record.at == HOG_AT {
            spin(&self.0, HOG_AT + HOG);
        }
        Ok(vec![("dpop".to_owned(), "proof".to_owned())])
    }
}

/// Line 2's request handed to hyper at once, its write held past its head by
/// the gate until `open_at` — inside the hog of line 3's decoration, from
/// 5,000 µs. `early`: the stand answers at `open_at` from the head alone,
/// the gate never opening, so line 2's `Sent` is the frontier's, made at the
/// answer. Its send's lag; `None` if line 2 was not connected before the hog.
fn connection_lag(early: bool) -> Option<u64> {
    let open_at = HOG_AT + 15_000;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let clock = Arc::new(RealClock::start(2_000).unwrap());
    let stub_clock = clock.clone();
    std::thread::spawn(move || {
        for s in listener.incoming() {
            let Ok(mut s) = s else { return };
            let clock = stub_clock.clone();
            std::thread::spawn(move || {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                while !buf.windows(4).any(|x| x == b"\r\n\r\n") {
                    match s.read(&mut chunk) {
                        Ok(0) | Err(_) => return,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    }
                }
                if early {
                    spin(&clock, open_at);
                } else {
                    // The rest of the request, once the gate opened.
                    let _ = s.read(&mut chunk);
                }
                let _ = s.write_all(b"HTTP/1.1 200 X\r\ncontent-length: 2\r\n\r\n{}");
                let _ = s.read(&mut chunk);
            });
        }
    });
    let open = Arc::new(AtomicBool::new(false));
    let waker: Arc<Mutex<Option<Waker>>> = Arc::new(Mutex::new(None));
    if !early {
        let (open, waker, clock) = (open.clone(), waker.clone(), clock.clone());
        std::thread::spawn(move || {
            spin(&clock, open_at);
            open.store(true, Ordering::SeqCst);
            if let Some(w) = waker.lock().unwrap().take() {
                w.wake();
            }
        });
    }
    let live = Live::start(1, 64).unwrap();
    let mut h = header();
    h.channels.clear();
    h.calls.clear();
    for o in &mut h.operations {
        o.timeout = 5_000_000;
        if o.name == "update" {
            o.decorated = vec!["dpop".into()];
        }
    }
    let h = Arc::new(h);
    let carrier = RestCarrier::decorated(
        h.clone(),
        OneStand,
        GatedTcp { addr, open, waker },
        Hogging(clock.clone()),
        Config::new(RunSecret::new([7; 32])),
    );
    // Line 2's body is what the gate holds.
    let mut gated = call("update", 1, 0, 0, 0);
    if let ascetic_ddd_command_log::record::Action::Rest(rest) = &mut gated.action {
        rest.body = Some(ascetic_ddd_command_log::record::Payload {
            codec: ascetic_ddd_command_log::record::Codec::Json,
            bytes: b"{\"group\":1}".to_vec(),
            holes: vec![],
        });
    }
    let records = [gated, call("update", 2, 0, HOG_AT, 0)];
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let journal = rt
        .block_on(run(
            ascetic_ddd_load_run::run::Ready::assumed(
                admit(
                    ascetic_ddd_load_run::source::Plan::assumed(h, Default::default()),
                    W,
                    Arc::new(carrier),
                    live.runtimes(),
                    Journal::new(64, 0),
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
    let s = sent(&journal, 2)?;
    let hog = dispatched(&journal, 3)?;
    // Early, the `Sent` the frontier made at the answer.
    if early {
        let answered = journal.iter().find_map(|l| match l {
            Line::Answer(a) if a.line == 2 && a.ignored.is_none() => Some(a.at),
            _ => None,
        });
        assert_eq!(answered, Some(s.at));
    }
    (s.connected < hog).then_some(s.lag)
}

/// The connection's task carries the path from `connected`: its flush held
/// by the stand's window past the hog's start, run only after the hog — the
/// lag the hog's rest; and for an early answer, the `Sent` the frontier
/// makes at it carries the lag so far.
#[test]
fn a_connection_s_wait_carries_its_path_s_lag() {
    for early in [false, true] {
        let lag = (0..5)
            .find_map(|_| connection_lag(early))
            .expect("line 2 connected before the hog, in some run");
        assert!(lag >= HOG / 2, "early {early}: lag {lag} µs");
    }
}

/// A loopback stub answering every request `200 {}`, keeping its
/// connection: its address.
fn answering() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for s in listener.incoming() {
            let Ok(mut s) = s else { return };
            std::thread::spawn(move || {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    let Some(end) = buf.windows(4).position(|x| x == b"\r\n\r\n") else {
                        match s.read(&mut chunk) {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                        continue;
                    };
                    let head = String::from_utf8_lossy(&buf[..end]).to_lowercase();
                    let len: usize = head
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length:"))
                        .and_then(|v| v.trim().parse().ok())
                        .unwrap_or(0);
                    while buf.len() < end + 4 + len {
                        match s.read(&mut chunk) {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    buf.drain(..end + 4 + len);
                    if s.write_all(b"HTTP/1.1 200 X\r\ncontent-length: 2\r\n\r\n{}")
                        .is_err()
                    {
                        return;
                    }
                }
            });
        }
    });
    addr
}

/// The real clock, its timers due within `[from, to]` firing `late` µs after
/// their instant: a whole process stalled while they wait — the thread's
/// park blocked throughout, its end at the timer's.
struct LateTimers {
    inner: Arc<RealClock>,
    from: u64,
    to: u64,
    late: u64,
}

impl Clock for LateTimers {
    fn now(&self) -> u64 {
        self.inner.now()
    }

    fn wall(&self) -> i64 {
        self.inner.wall()
    }

    fn idle_since(&self) -> u64 {
        self.inner.idle_since()
    }

    fn sleep_until(&self, at: u64) -> impl Future<Output = ()> + Send + '_ {
        let fires = if (self.from..=self.to).contains(&at) {
            at + self.late
        } else {
            at
        };
        self.inner.sleep_until(fires)
    }

    fn settled(&self, at: u64) -> impl Future<Output = ()> + Send + '_ {
        self.inner.settled(at)
    }
}

/// Loopback TCP, its first connect refused by the stand.
struct RefusingFirst {
    addr: SocketAddr,
    refused: AtomicBool,
}

impl Egress for RefusingFirst {
    type Io = tokio::net::TcpStream;

    fn ports(&self, _: u32) -> usize {
        62
    }

    fn connect(
        &self,
        _: u32,
        _: &Route,
    ) -> impl Future<Output = Result<tokio::net::TcpStream, Connect>> + Send {
        let refuse = !self.refused.swap(true, Ordering::SeqCst);
        let addr = self.addr;
        async move {
            if refuse {
                return Err(Connect::Stand(StandConnect::Refused));
            }
            let s = tokio::net::TcpStream::connect(addr)
                .await
                .map_err(Connect::of_connect)?;
            s.set_nodelay(true).map_err(Connect::of_connect)?;
            Ok(s)
        }
    }

    fn admits(&self, _: u32) -> Result<(), ascetic_ddd_load_run::stand::Unfit> {
        Ok(())
    }

    fn reaches(&self, _: u32, _: &str) -> Result<(), ascetic_ddd_load_run::stand::Unfit> {
        Ok(())
    }
}

/// Decision 19 (F4): a refused connect, then the floor's timer firing
/// `LATE` µs after its instant, the thread parked throughout — its last
/// blocked park ending at the wake, the bound by it about nothing. The timer
/// the attempt registered tells: its send's lag at least the timer's
/// lateness. `None` if the connect after the floor was not that late.
fn late_floor_lag() -> Option<u64> {
    const LATE: u64 = 60_000;
    let addr = answering();
    let live = Live::start(1, 64).unwrap();
    let clock = Arc::new(LateTimers {
        inner: Arc::new(RealClock::start(2_000).unwrap()),
        // The floor's instant, a connect floor past the first connect: no
        // other timer of the run's is due there.
        from: 1_000,
        to: 1_000_000,
        late: LATE,
    });
    let mut h = header();
    h.channels.clear();
    h.calls.clear();
    h.operations.iter_mut().for_each(|o| o.timeout = 5_000_000);
    let h = Arc::new(h);
    let carrier = RestCarrier::new(
        h.clone(),
        OneStand,
        RefusingFirst {
            addr,
            refused: AtomicBool::new(false),
        },
        Config::new(RunSecret::new([7; 32])),
    );
    let records = [call("read", 1, 0, 0, 0)];
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let journal = rt
        .block_on(run(
            ascetic_ddd_load_run::run::Ready::assumed(
                admit(
                    ascetic_ddd_load_run::source::Plan::assumed(h, Default::default()),
                    W,
                    Arc::new(carrier),
                    live.runtimes(),
                    Journal::new(64, 0),
                )
                .unwrap(),
                clock,
                Lines::new(),
                ascetic_ddd_command_log::fills::Namespace(1),
            ),
            Records::new(records.iter().cloned()),
        ))
        .unwrap()
        .journal;
    let s = sent(&journal, 2)?;
    (s.connected >= s.handed + ascetic_ddd_load_run::rest::CONNECT_FLOOR + LATE).then_some(s.lag)
}

#[test]
fn a_timer_woken_late_is_its_path_s_lag() {
    let lag = (0..5)
        .find_map(|_| late_floor_lag())
        .expect("the floor's timer late, in some run");
    assert!(lag >= 50_000, "lag {lag} µs");
}

/// Its decoration awaits 5 ms, the thread held 50 ms by a sibling's: the
/// lag its wake took is the hand-off's, not its send's — the path's start
/// and lag reset once decorated (decision 19).
struct Awaiting(Arc<RealClock>);

impl Decorator for Awaiting {
    async fn decorate<'a>(&'a self, d: Decoration<'a>) -> Result<Vec<(String, String)>, String> {
        if d.record.at == 0 {
            let until = self.0.now() + 5_000;
            self.0.sleep_until(until).await;
        } else {
            spin(&self.0, HOG_AT + 50_000);
        }
        Ok(vec![("dpop".to_owned(), "proof".to_owned())])
    }
}

/// Line 2's send: its decoration's wake held by line 3's from 5,000 µs;
/// `None` if line 2's decoration did not end after the hog.
fn decorated_lag() -> Option<journal::Sent> {
    let addr = answering();
    let clock = Arc::new(RealClock::start(2_000).unwrap());
    let live = Live::start(1, 64).unwrap();
    let mut h = header();
    h.channels.clear();
    h.calls.clear();
    for o in &mut h.operations {
        o.timeout = 5_000_000;
        if o.name == "update" {
            o.decorated = vec!["dpop".into()];
        }
    }
    let h = Arc::new(h);
    let carrier = RestCarrier::decorated(
        h.clone(),
        OneStand,
        ascetic_ddd_load_run::tcp::TcpEgress::new(
            std::collections::BTreeMap::from([("gateway".to_owned(), addr)]),
            vec![None],
        )
        .with_ports(62),
        Awaiting(clock.clone()),
        Config::new(RunSecret::new([7; 32])),
    );
    let records = [call("update", 1, 0, 0, 0), call("update", 2, 0, HOG_AT, 0)];
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let journal = rt
        .block_on(run(
            ascetic_ddd_load_run::run::Ready::assumed(
                admit(
                    ascetic_ddd_load_run::source::Plan::assumed(h, Default::default()),
                    W,
                    Arc::new(carrier),
                    live.runtimes(),
                    Journal::new(64, 0),
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
    let s = sent(&journal, 2)?;
    (s.handed >= HOG_AT + 40_000).then_some(s)
}

#[test]
fn a_decoration_s_lag_is_no_send_s() {
    let s = (0..5)
        .find_map(|_| decorated_lag())
        .expect("line 2 decorated after the hog, in some run");
    assert!(s.lag < 20_000, "lag {} µs: {s:?}", s.lag);
}

/// Loopback TCP, its first connect pending until `at` on the clock, then
/// refused by the stand.
struct RefusedAt {
    addr: SocketAddr,
    clock: Arc<RealClock>,
    at: u64,
    refused: AtomicBool,
}

impl Egress for RefusedAt {
    type Io = tokio::net::TcpStream;

    fn ports(&self, _: u32) -> usize {
        62
    }

    fn connect(
        &self,
        _: u32,
        _: &Route,
    ) -> impl Future<Output = Result<tokio::net::TcpStream, Connect>> + Send {
        let refuse = !self.refused.swap(true, Ordering::SeqCst);
        let (addr, clock, at) = (self.addr, self.clock.clone(), self.at);
        async move {
            if refuse {
                clock.sleep_until(at).await;
                return Err(Connect::Stand(StandConnect::Refused));
            }
            let s = tokio::net::TcpStream::connect(addr)
                .await
                .map_err(Connect::of_connect)?;
            s.set_nodelay(true).map_err(Connect::of_connect)?;
            Ok(s)
        }
    }

    fn admits(&self, _: u32) -> Result<(), ascetic_ddd_load_run::stand::Unfit> {
        Ok(())
    }

    fn reaches(&self, _: u32, _: &str) -> Result<(), ascetic_ddd_load_run::stand::Unfit> {
        Ok(())
    }
}

/// The closing report of `line` in `journal`, and when.
fn report(journal: &[Line], line: u64) -> Option<(u64, journal::Unsent)> {
    journal.iter().find_map(|l| match l {
        Line::Answer(a) if a.line == line => a.unsent.clone().map(|u| (a.at, u)),
        _ => None,
    })
}

/// Decision 19: a connect pending over the deadline, its race's timer due at
/// `D + 1` held back — a whole process stalled across the deadline — and the
/// refusal then read with the thread's park ending at it: the timer the
/// attempt dropped, due, is its lag. `None` if the report came before the
/// refusal's instant.
fn stalled_report() -> Option<journal::Unsent> {
    const D: u64 = 30_000;
    const REFUSED_AT: u64 = 80_000;
    let addr = answering();
    let live = Live::start(1, 64).unwrap();
    let inner = Arc::new(RealClock::start(2_000).unwrap());
    let clock = Arc::new(LateTimers {
        inner: inner.clone(),
        // `D + 1` and the machine's censor at `D`, both after the refusal.
        from: D / 2,
        to: D + D / 2,
        late: 200_000,
    });
    let mut h = header();
    h.channels.clear();
    h.calls.clear();
    h.operations.iter_mut().for_each(|o| o.timeout = D);
    let h = Arc::new(h);
    let carrier = RestCarrier::new(
        h.clone(),
        OneStand,
        RefusedAt {
            addr,
            clock: inner,
            at: REFUSED_AT,
            refused: AtomicBool::new(false),
        },
        Config::new(RunSecret::new([7; 32])),
    );
    let records = [call("read", 1, 0, 0, 0)];
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let journal = rt
        .block_on(run(
            ascetic_ddd_load_run::run::Ready::assumed(
                admit(
                    ascetic_ddd_load_run::source::Plan::assumed(h, Default::default()),
                    W,
                    Arc::new(carrier),
                    live.runtimes(),
                    Journal::new(64, 0),
                )
                .unwrap(),
                clock,
                Lines::new(),
                ascetic_ddd_command_log::fills::Namespace(1),
            ),
            Records::new(records.iter().cloned()),
        ))
        .unwrap()
        .journal;
    let (at, u) = report(&journal, 2)?;
    (at >= REFUSED_AT && u.handed.is_some_and(|h| h < D / 2)).then_some(u)
}

#[test]
fn a_timer_dropped_due_is_its_path_s_lag() {
    let u = (0..5)
        .find_map(|_| stalled_report())
        .expect("the report after the refusal, in some run");
    assert_eq!(u.refused, 1);
    assert!(u.lag >= 40_000, "lag {} µs: {u:?}", u.lag);
}

/// Decision 19: a connect refused only after 80 ms leaves its floor's
/// instant passed — a timer never registered is no wake of the task, its
/// due no lag. Line 2's send; `None` if its connect after the refusal came
/// before it.
fn slow_refusal_sent() -> Option<journal::Sent> {
    const REFUSED_AT: u64 = 80_000;
    let addr = answering();
    let live = Live::start(1, 64).unwrap();
    let clock = Arc::new(RealClock::start(2_000).unwrap());
    let mut h = header();
    h.channels.clear();
    h.calls.clear();
    h.operations.iter_mut().for_each(|o| o.timeout = 5_000_000);
    let h = Arc::new(h);
    let carrier = RestCarrier::new(
        h.clone(),
        OneStand,
        RefusedAt {
            addr,
            clock: clock.clone(),
            at: REFUSED_AT,
            refused: AtomicBool::new(false),
        },
        Config::new(RunSecret::new([7; 32])),
    );
    let records = [call("read", 1, 0, 0, 0)];
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let journal = rt
        .block_on(run(
            ascetic_ddd_load_run::run::Ready::assumed(
                admit(
                    ascetic_ddd_load_run::source::Plan::assumed(h, Default::default()),
                    W,
                    Arc::new(carrier),
                    live.runtimes(),
                    Journal::new(64, 0),
                )
                .unwrap(),
                clock,
                Lines::new(),
                ascetic_ddd_command_log::fills::Namespace(1),
            ),
            Records::new(records.iter().cloned()),
        ))
        .unwrap()
        .journal;
    let s = sent(&journal, 2)?;
    (s.connected >= REFUSED_AT && s.handed < 20_000).then_some(s)
}

#[test]
fn a_timer_never_registered_is_no_lag() {
    let s = (0..5)
        .find_map(|_| slow_refusal_sent())
        .expect("line 2 connected after the refusal, in some run");
    assert!(s.lag < 20_000, "lag {} µs: {s:?}", s.lag);
}
