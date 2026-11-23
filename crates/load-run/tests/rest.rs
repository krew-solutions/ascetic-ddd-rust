//! K2b-2a (ADR-0047 decision 16): an attempt over hyper's own HTTP/1.1
//! connection, against a stub on an in-memory pipe, under the virtual clock —
//! `Sent` at the instant its last byte is accepted, never at the first nor at
//! the hand-off, with its body too; the answer at its last body frame, never
//! at its head; a connection its peer closed never reused, one kept alive
//! reused; an error once a byte left the system's Unknown at the error; no
//! answer censored at the deadline; an answer past the cap unread and its
//! connection closed.

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};

use ascetic_ddd_command_log::fills::Namespace;
use ascetic_ddd_command_log::record::{Action, Codec, Header, Payload, Record, Rest};
use ascetic_ddd_command_log::render::{self, RunSecret};
use ascetic_ddd_load_run::clock::{Clock, VirtualClock};
use ascetic_ddd_load_run::rest::{Config, RestCarrier};
use ascetic_ddd_load_run::run::{RunError, Runtimes, run};
use ascetic_ddd_load_run::sink::Lines;
use ascetic_ddd_load_run::source::Records;
use ascetic_ddd_load_run::stand::{Egress, Route, Stand};
use ascetic_ddd_scheduler::journal::{self, End, Judged, Line};
use fixture::*;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};

const NAMESPACE: Namespace = Namespace(1);

/// What the stub does with a request, found by its correlation id.
#[derive(Clone, Debug)]
enum Answer {
    /// The head `head` µs after the request's last byte came, the body
    /// `body` µs after that; `close` asks the client to close.
    Ok {
        head: u64,
        body: u64,
        status: u16,
        headers: Vec<(String, String)>,
        bytes: Vec<u8>,
        close: bool,
    },
    /// The connection closed `after` µs after the request came, unanswered.
    Reset { after: u64 },
    /// The head and three bytes of a body of ten `after` µs after the
    /// request came, then the connection closed.
    Cut { after: u64 },
    /// Never answered.
    Silent,
}

/// A request as it came: its correlation id, the instant its last byte did,
/// and its bytes.
#[derive(Clone, Debug)]
struct Came {
    id: String,
    at: u64,
    bytes: Vec<u8>,
}

struct Stub {
    clock: Arc<VirtualClock>,
    answers: BTreeMap<String, Answer>,
    connections: AtomicUsize,
    came: Mutex<Vec<Came>>,
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

fn field(head: &str, name: &str) -> Option<String> {
    head.lines().find_map(|l| {
        let (n, v) = l.split_once(':')?;
        n.trim()
            .eq_ignore_ascii_case(name)
            .then(|| v.trim().to_owned())
    })
}

impl Stub {
    async fn serve(self: Arc<Self>, mut io: DuplexStream) {
        self.connections.fetch_add(1, Ordering::SeqCst);
        let mut buf = Vec::new();
        loop {
            let head_end = loop {
                if let Some(i) = find(&buf, b"\r\n\r\n") {
                    break i + 4;
                }
                let mut chunk = [0u8; 4096];
                match io.read(&mut chunk).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                }
            };
            let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
            let len: usize = field(&head, "content-length")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            while buf.len() < head_end + len {
                let mut chunk = [0u8; 4096];
                match io.read(&mut chunk).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                }
            }
            let bytes: Vec<u8> = buf.drain(..head_end + len).collect();
            let at = self.clock.now();
            let id = field(&head, "x-correlation-id").unwrap_or_default();
            self.came.lock().unwrap().push(Came {
                id: id.clone(),
                at,
                bytes,
            });
            match self.answers.get(&id).cloned().unwrap_or(Answer::Silent) {
                Answer::Ok {
                    head,
                    body,
                    status,
                    headers,
                    bytes,
                    close,
                } => {
                    self.clock.sleep_until(at + head).await;
                    let connection = if close { "connection: close\r\n" } else { "" };
                    let fields: String = headers
                        .iter()
                        .map(|(n, v)| format!("{n}: {v}\r\n"))
                        .collect();
                    let text = format!(
                        "HTTP/1.1 {status} X\r\ncontent-length: {}\r\n{fields}{connection}\r\n",
                        bytes.len()
                    );
                    if io.write_all(text.as_bytes()).await.is_err() {
                        return;
                    }
                    self.clock.sleep_until(at + head + body).await;
                    if io.write_all(&bytes).await.is_err() {
                        return;
                    }
                    if close {
                        return;
                    }
                }
                Answer::Reset { after } => {
                    self.clock.sleep_until(at + after).await;
                    return;
                }
                Answer::Cut { after } => {
                    self.clock.sleep_until(at + after).await;
                    let _ = io
                        .write_all(b"HTTP/1.1 200 X\r\ncontent-length: 10\r\n\r\n{\"a")
                        .await;
                    return;
                }
                Answer::Silent => std::future::pending::<()>().await,
            }
        }
    }
}

/// A pipe's client end, its writes held back: the first until `first` µs
/// after it is asked to write, then at most `chunk` bytes at a time, `gap`
/// µs apart.
struct Throttled {
    inner: DuplexStream,
    clock: Arc<VirtualClock>,
    first: u64,
    chunk: usize,
    gap: u64,
    next: Option<u64>,
    sleep: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    /// Each request's write held this long after its first poll, by its
    /// correlation id: `sim`'s `sent_after`.
    holds: Option<Arc<BTreeMap<String, u64>>>,
    /// The request whose first write fails, and the run's count of them.
    fail: Option<usize>,
    requests: Arc<AtomicUsize>,
    broken: bool,
    /// From this instant its reads end, with no wake of their own: a close
    /// by the stand of an idle connection, seen when the client next polls.
    eof: Option<u64>,
}

impl AsyncRead for Throttled {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.eof.is_some_and(|at| self.clock.now() >= at) {
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for Throttled {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.broken {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        if find(buf, b"x-correlation-id").is_some() || find(buf, b"X-Correlation-Id").is_some() {
            let n = self.requests.fetch_add(1, Ordering::SeqCst) + 1;
            if self.fail == Some(n) {
                self.broken = true;
                return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
            }
        }
        let now = self.clock.now();
        let hold = self.holds.as_ref().map(|h| {
            let text = String::from_utf8_lossy(buf);
            field(&text, "x-correlation-id")
                .and_then(|id| h.get(&id).copied())
                .unwrap_or(0)
        });
        let first = hold.unwrap_or(self.first);
        let allowed = *self.next.get_or_insert(now + first);
        if now < allowed {
            if self.sleep.is_none() {
                let clock = self.clock.clone();
                self.sleep = Some(Box::pin(async move { clock.sleep_until(allowed).await }));
            }
            if let Some(s) = self.sleep.as_mut() {
                ready!(s.as_mut().poll(cx));
            }
        }
        self.sleep = None;
        let n = buf.len().min(self.chunk);
        let wrote = ready!(Pin::new(&mut self.inner).poll_write(cx, &buf[..n]))?;
        let gap = self.gap;
        // Held per request: the next one's hold starts at its own first poll.
        self.next = if self.holds.is_some() {
            None
        } else {
            Some(self.clock.now() + gap)
        };
        Poll::Ready(Ok(wrote))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[derive(Clone, Copy)]
struct Throttle {
    first: u64,
    chunk: usize,
    gap: u64,
}

struct Pipe {
    stub: Arc<Stub>,
    throttle: Throttle,
    holds: Option<Arc<BTreeMap<String, u64>>>,
    /// Microseconds a connect takes: the stand's accept.
    connect: u64,
    /// Connects refused before one is accepted.
    refuse: Arc<AtomicUsize>,
    /// The request, counted from 1 over the run, whose first write fails
    /// with no byte accepted: its connection gone under it.
    fail: Option<usize>,
    requests: Arc<AtomicUsize>,
    /// The first connection's reads end at this instant, unwoken.
    eof: Option<u64>,
    connects: Arc<AtomicUsize>,
}

impl Egress for Pipe {
    type Io = Throttled;

    fn connect(&self, _: u32, _: &Route) -> impl Future<Output = io::Result<Throttled>> + Send {
        let stub = self.stub.clone();
        let t = self.throttle;
        let holds = self.holds.clone();
        let connect = self.connect;
        let refused = self
            .refuse
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        let (fail, requests) = (self.fail, self.requests.clone());
        let eof = self
            .eof
            .filter(|_| self.connects.fetch_add(1, Ordering::SeqCst) == 0);
        async move {
            if refused {
                return Err(io::Error::from(io::ErrorKind::ConnectionRefused));
            }
            // Never, for `u64::MAX`: a stand dropping every SYN.
            if connect == u64::MAX {
                std::future::pending::<()>().await;
            }
            let until = stub.clock.now() + connect;
            stub.clock.sleep_until(until).await;
            let (client, server) = tokio::io::duplex(1 << 16);
            drop(tokio::spawn(stub.clone().serve(server)));
            Ok(Throttled {
                inner: client,
                clock: stub.clock.clone(),
                first: t.first,
                chunk: t.chunk,
                gap: t.gap,
                next: None,
                sleep: None,
                holds,
                fail,
                requests,
                broken: false,
                eof,
            })
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
}

fn rest_header() -> Header {
    let mut h = header();
    h.channels.clear();
    h.calls.clear();
    h
}

fn id(record: &Record, attempt: u32) -> String {
    render::correlation(NAMESPACE, &record.step, attempt)
}

/// A run of `records` against a stub answering each first attempt as
/// `answers` says, its writes held back as `throttle` says: the journal and
/// what the stub saw.
fn run_rest(
    header: Header,
    records: &[Record],
    answers: Vec<Answer>,
    throttle: Throttle,
) -> (Result<Vec<Line>, RunError>, Vec<Came>, usize) {
    run_rest_connecting(header, records, answers, throttle, 0)
}

/// [`run_rest`] with each connect taking `connect` µs.
fn run_rest_connecting(
    header: Header,
    records: &[Record],
    answers: Vec<Answer>,
    throttle: Throttle,
    connect: u64,
) -> (Result<Vec<Line>, RunError>, Vec<Came>, usize) {
    run_rest_with(
        header,
        records,
        answers,
        throttle,
        connect,
        0,
        None,
        None,
        &|_| {},
    )
}

/// [`run_rest_connecting`] with the first `refuse` connects refused, the
/// `fail`-th request's first write failing, and the carrier's config tuned.
#[allow(clippy::too_many_arguments)]
fn run_rest_with(
    header: Header,
    records: &[Record],
    answers: Vec<Answer>,
    throttle: Throttle,
    connect: u64,
    refuse: usize,
    fail: Option<usize>,
    eof: Option<u64>,
    tune: &dyn Fn(&mut Config),
) -> (Result<Vec<Line>, RunError>, Vec<Came>, usize) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    rt.block_on(async {
        let clock = Arc::new(VirtualClock::start(1_763_640_000_000_000));
        let stub = Arc::new(Stub {
            clock: clock.clone(),
            answers: records.iter().map(|r| id(r, 1)).zip(answers).collect(),
            connections: AtomicUsize::new(0),
            came: Mutex::new(Vec::new()),
        });
        let header = Arc::new(header);
        let carrier = RestCarrier::new(
            header.clone(),
            OneStand,
            Pipe {
                stub: stub.clone(),
                throttle,
                holds: None,
                connect,
                refuse: Arc::new(AtomicUsize::new(refuse)),
                fail,
                requests: Arc::new(AtomicUsize::new(0)),
                eof,
                connects: Arc::new(AtomicUsize::new(0)),
            },
            {
                let mut c = Config::new(NAMESPACE, RunSecret::new([7; 32]), clock.wall());
                tune(&mut c);
                c
            },
        );
        let runtimes = Runtimes {
            sources: vec![tokio::runtime::Handle::current()],
        };
        let ran = clock
            .within(run(
                header,
                W,
                clock.clone(),
                Records::new(records.iter().cloned()),
                Lines::new(),
                Arc::new(carrier),
                &runtimes,
            ))
            .await;
        let came = stub.came.lock().unwrap().clone();
        (
            ran.map(|r| r.journal),
            came,
            stub.connections.load(Ordering::SeqCst),
        )
    })
}

const AT_ONCE: Throttle = Throttle {
    first: 0,
    chunk: usize::MAX,
    gap: 0,
};

fn ok(head: u64, body: u64) -> Answer {
    Answer::Ok {
        head,
        body,
        status: 200,
        headers: vec![],
        bytes: b"{}".to_vec(),
        close: false,
    }
}

fn sent(journal: &[Line], line: u64) -> Vec<u64> {
    journal
        .iter()
        .filter_map(|l| match l {
            Line::Sent(s) if s.line == line => Some(s.at),
            _ => None,
        })
        .collect()
}

fn answer(journal: &[Line], line: u64) -> Option<&journal::Answer> {
    journal.iter().find_map(|l| match l {
        Line::Answer(a) if a.line == line => Some(a),
        _ => None,
    })
}

fn end(journal: &[Line], line: u64) -> Option<(End, u64)> {
    journal.iter().find_map(|l| match l {
        Line::End(e) if e.line == line => Some((e.end, e.at)),
        _ => None,
    })
}

#[test]
fn sent_is_the_last_byte_and_the_answer_its_last_body_frame() {
    let records = vec![call("read", 1, 0, 0, 0)];
    let throttle = Throttle {
        first: 30,
        chunk: 16,
        gap: 5,
    };
    let (journal, came, _) = run_rest(rest_header(), &records, vec![ok(100, 200)], throttle);
    let journal = journal.unwrap();
    let last = came[0].at;
    // Its first byte at 30, sixteen at a time five apart: its last well after.
    assert!(last > 30 + 5, "the last byte came at {last}");
    assert_eq!(sent(&journal, 2), vec![last]);
    let a = answer(&journal, 2).unwrap();
    assert_eq!(
        (a.at, a.status, a.judged),
        (last + 300, Some(200), Judged::Expected)
    );
    assert_eq!(end(&journal, 2), Some((End::Expected, last + 300)));
    // The frame as hyper wrote it: the route's host, the correlation id.
    let text = String::from_utf8_lossy(&came[0].bytes).into_owned();
    assert!(text.starts_with("POST /groups/1 HTTP/1.1\r\n"), "{text}");
    assert_eq!(field(&text, "host").as_deref(), Some("stand.test"));
    assert_eq!(field(&text, "x-correlation-id"), Some(id(&records[0], 1)));
}

#[test]
fn a_body_s_last_byte_is_sent() {
    let mut r = call("update", 1, 0, 0, 0);
    let body = br#"{"name":"a group of a long enough name to take several writes"}"#;
    if let Action::Rest(rest) = &mut r.action {
        rest.body = Some(Payload {
            codec: Codec::Json,
            bytes: body.to_vec(),
            holes: vec![],
        });
    }
    let records = vec![r];
    let throttle = Throttle {
        first: 10,
        chunk: 8,
        gap: 3,
    };
    let (journal, came, _) = run_rest(rest_header(), &records, vec![ok(0, 0)], throttle);
    let journal = journal.unwrap();
    assert!(came[0].bytes.ends_with(body));
    assert_eq!(sent(&journal, 2), vec![came[0].at]);
    assert_eq!(end(&journal, 2).map(|e| e.0), Some(End::Expected));
}

#[test]
fn a_connection_kept_alive_is_reused_and_one_closed_is_not() {
    let records = vec![call("read", 1, 0, 0, 0), call("read", 2, 0, 10_000, 0)];
    for (close, connections) in [(false, 1), (true, 2)] {
        let first = Answer::Ok {
            head: 10,
            body: 0,
            status: 200,
            headers: vec![],
            bytes: b"{}".to_vec(),
            close,
        };
        let (journal, came, opened) =
            run_rest(rest_header(), &records, vec![first, ok(10, 0)], AT_ONCE);
        let journal = journal.unwrap();
        assert_eq!(opened, connections, "close: {close}");
        let ids: Vec<String> = came.iter().map(|c| c.id.clone()).collect();
        assert_eq!(ids, vec![id(&records[0], 1), id(&records[1], 1)]);
        assert_eq!(end(&journal, 2).map(|e| e.0), Some(End::Expected));
        assert_eq!(end(&journal, 3).map(|e| e.0), Some(End::Expected));
    }
}

#[test]
fn a_connection_closed_once_a_byte_left_is_unknown_at_the_close() {
    let records = vec![call("read", 1, 0, 0, 0)];
    let (journal, came, _) = run_rest(
        rest_header(),
        &records,
        vec![Answer::Reset { after: 50 }],
        AT_ONCE,
    );
    let journal = journal.unwrap();
    let a = answer(&journal, 2).unwrap();
    assert_eq!(
        (a.at, a.status, a.judged),
        (came[0].at + 50, None, Judged::Unknown)
    );
    assert_eq!(end(&journal, 2).map(|e| e.0), Some(End::Unknown));
}

#[test]
fn no_answer_is_censored_at_the_deadline() {
    let mut header = rest_header();
    header.operations.iter_mut().for_each(|o| o.timeout = 5_000);
    let records = vec![call("read", 1, 0, 0, 0)];
    let (journal, came, _) = run_rest(header, &records, vec![Answer::Silent], AT_ONCE);
    let journal = journal.unwrap();
    assert_eq!(came.len(), 1);
    assert_eq!(sent(&journal, 2), vec![0]);
    assert_eq!(end(&journal, 2), Some((End::Unknown, 5_000)));
}

#[test]
fn an_answer_past_the_cap_is_unread_and_its_connection_closed() {
    let mut header = rest_header();
    header.profile.answer_cap = 64;
    let records = vec![call("read", 1, 0, 0, 0), call("read", 2, 0, 10_000, 0)];
    let big = Answer::Ok {
        head: 10,
        body: 20,
        status: 200,
        headers: vec![],
        bytes: vec![b' '; 200],
        close: false,
    };
    let (journal, came, opened) = run_rest(header, &records, vec![big, ok(10, 0)], AT_ONCE);
    let journal = journal.unwrap();
    let a = answer(&journal, 2).unwrap();
    // Arrived once the cap was passed: the body's frame, at head + body.
    assert_eq!((a.at, a.status), (came[0].at + 30, Some(200)));
    assert_eq!(opened, 2, "a connection with its answer unread is closed");
}

#[test]
fn an_answer_cut_short_is_unknown_at_the_cut() {
    let records = vec![call("read", 1, 0, 0, 0)];
    let (journal, came, _) = run_rest(
        rest_header(),
        &records,
        vec![Answer::Cut { after: 40 }],
        AT_ONCE,
    );
    let journal = journal.unwrap();
    let a = answer(&journal, 2).unwrap();
    assert_eq!(
        (a.at, a.status, a.judged),
        (came[0].at + 40, None, Judged::Unknown)
    );
    assert_eq!(end(&journal, 2).map(|e| e.0), Some(End::Unknown));
}

mod common;

use ascetic_ddd_command_log::record::{Outcome, Probe, Source};
use ascetic_ddd_command_log::request::rest_of;
use ascetic_ddd_command_log::validate::NoHost;
use ascetic_ddd_faker::{Key, Seed};
use ascetic_ddd_load_run::carrier::Carrier;
use ascetic_ddd_load_run::stamps::Attempt;
use ascetic_ddd_scheduler::machine::{Captured, Judgement};
use ascetic_ddd_scheduler::sim::{self, run_reading};
use common::{Script, Scripted};

const ATTEMPTS: u32 = 16;

fn over_rest(r: &Record) -> bool {
    matches!(&r.action, Action::Rest(_))
        || matches!(&r.action, Action::Observe(o) if matches!(o.probe, Probe::Rest(_)))
}

/// The differential's script, as an answer over HTTP can carry it: a refusal
/// certified only where the operation lists a code for it; `Retry-After` in
/// whole seconds, on a certified refusal; an unexpected answer only where one
/// can be; each captured value in the form its source yields it — a
/// header's and a `Location`'s quoted, a body's number bare.
fn rest_script(
    seed: u64,
    log: &str,
    header: Header,
) -> impl Fn(u64, u32, &Record) -> sim::Attempt + Send + Sync + use<> {
    let root = Key::root(Seed(seed)).child("rest").child(log);
    move |line, attempt, r| {
        let k = root.at(line).at(u64::from(attempt));
        let draw = |label: &str, m: u64| k.child(label).unit().raw() % m;
        let certifiable = rest_of(&r.action)
            .and_then(|rest| header.operations.iter().find(|o| o.name == rest.op))
            .is_some_and(|o| !o.not_run.is_empty());
        let value = |c: &ascetic_ddd_command_log::record::Capture| match c.source {
            Source::Json(_) => line.to_string().into_bytes(),
            Source::Header(_) | Source::Location => format!("\"{line}\"").into_bytes(),
        };
        // A call that expects nothing takes any answer below 500 as expected
        // (the judge's sent-only rule): only a poll's or an expectation's can
        // be unexpected.
        let refusable = !r.expect.is_empty() || matches!(r.action, Action::Observe(_));
        let judgement = match draw("judgement", 10) {
            0 | 1 if refusable && !(certifiable && draw("certified", 2) == 0) => {
                Judgement::Unexpected
            }
            0 | 1 if certifiable => Judgement::NotTaken,
            2 => Judgement::Unknown,
            _ if !r.capture.is_empty() && draw("bare", 6) == 0 => Judgement::Expected(vec![]),
            _ => Judgement::Expected(
                r.capture
                    .iter()
                    .map(|c| Captured {
                        subject: c.subject.clone(),
                        slot: c.slot.clone(),
                        value: value(c),
                    })
                    .collect(),
            ),
        };
        // The judge takes `Retry-After` from a certified refusal alone
        // (decision 15): only one carries a wait.
        let after = match (&judgement, draw("after", 3)) {
            (Judgement::NotTaken, n) if n > 0 => Some(n * 1_000_000),
            _ => None,
        };
        sim::Attempt {
            after,
            sent_after: draw("sent", 300),
            answer_after: (draw("silent", 30) != 0).then(|| draw("answer", 3_000)),
            judgement,
        }
    }
}

/// The answer whose judgement is `a`'s: the inverse of the judge, for the
/// stub. Its status and its problem's code, for the journal.
fn respond(
    header: &Header,
    r: &Record,
    line: u64,
    a: &sim::Attempt,
) -> (Answer, u16, Option<String>) {
    let mut headers = Vec::new();
    if let Some(after) = a.after {
        headers.push(("retry-after".to_owned(), (after / 1_000_000).to_string()));
    }
    let (status, bytes, code) = match &a.judgement {
        Judgement::Expected(captured) => {
            let status = r
                .expect
                .iter()
                .find_map(|o| match o {
                    Outcome::Response { status, .. } => Some(*status),
                    _ => None,
                })
                .unwrap_or(200);
            let mut body = serde_json::Map::new();
            for c in captured {
                let Some(capture) = r
                    .capture
                    .iter()
                    .find(|x| x.subject == c.subject && x.slot == c.slot)
                else {
                    continue;
                };
                match &capture.source {
                    Source::Header(name) => headers.push((name.clone(), line.to_string())),
                    Source::Location => {
                        headers.push(("location".to_owned(), format!("/things/{line}")))
                    }
                    Source::Json(pointer) => {
                        body.insert(
                            pointer.trim_start_matches('/').to_owned(),
                            serde_json::json!(line),
                        );
                    }
                }
            }
            (status, serde_json::to_vec(&body).unwrap(), None)
        }
        Judgement::Unexpected => (409, Vec::new(), None),
        Judgement::NotTaken => {
            let op = rest_of(&r.action)
                .and_then(|rest| header.operations.iter().find(|o| o.name == rest.op))
                .unwrap();
            let code = op.not_run[0].clone();
            headers.push((
                "content-type".to_owned(),
                "application/problem+json".to_owned(),
            ));
            (
                503,
                serde_json::to_vec(&serde_json::json!({ "code": code })).unwrap(),
                Some(code),
            )
        }
        Judgement::Unknown => (500, Vec::new(), None),
    };
    let answer = match a.answer_after {
        Some(after) => Answer::Ok {
            head: after,
            body: 0,
            status,
            headers,
            bytes,
            close: false,
        },
        None => Answer::Silent,
    };
    (answer, status, code)
}

/// REST attempts over hyper to the stub, every other kind as `sim`'s script.
struct Mixed {
    rest: RestCarrier<OneStand, Pipe>,
    scripted: Scripted,
}

impl Carrier<VirtualClock> for Mixed {
    fn carries(&self, _: &Header) -> Result<(), String> {
        Ok(())
    }

    fn carry(&self, a: Attempt<VirtualClock>) -> impl Future<Output = ()> + Send + 'static {
        let f: Pin<Box<dyn Future<Output = ()> + Send>> = if over_rest(a.record()) {
            Box::pin(Carrier::<VirtualClock>::carry(&self.rest, a))
        } else {
            Box::pin(self.scripted.carry(a))
        };
        f
    }
}

/// A journal with what `sim` cannot know of an answer — its status, its
/// problem's code — left out.
fn projected(journal: &[Line]) -> Vec<Line> {
    journal
        .iter()
        .cloned()
        .map(|l| match l {
            Line::Answer(mut a) => {
                a.status = None;
                a.code = None;
                Line::Answer(a)
            }
            other => other,
        })
        .collect()
}

#[test]
fn over_rest_the_shell_journals_what_sim_journals() {
    let mut runs = 0;
    let mut kinds: BTreeMap<&str, u64> = BTreeMap::new();
    for (name, records) in fixture::corpus::logs() {
        let header = fixture::corpus::corpus_header();
        let (report, _) = run_bounded(header.clone(), &records, &NoHost, 1_000);
        if !report.violations.is_empty() || !records.iter().any(over_rest) {
            continue;
        }
        for seed in 1..=2u64 {
            let script: Arc<Script> = Arc::new(rest_script(seed, &name, header.clone()));
            let ran = run_reading(header.clone(), &records, W, &|_| 0, &*script).unwrap();
            // The stub's answers, the pipe's holds and the journal's status
            // and code, by correlation id and by (line, attempt).
            let mut answers = BTreeMap::new();
            let mut holds = BTreeMap::new();
            let mut sent: BTreeMap<(u64, u32), (u16, Option<String>)> = BTreeMap::new();
            for (i, r) in records.iter().enumerate() {
                let line = i as u64 + 2;
                if !over_rest(r) {
                    continue;
                }
                for attempt in 1..=ATTEMPTS {
                    let a = script(line, attempt, r);
                    let (answer, status, code) = respond(&header, r, line, &a);
                    answers.insert(id(r, attempt), answer);
                    holds.insert(id(r, attempt), a.sent_after);
                    sent.insert((line, attempt), (status, code));
                }
            }
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();
            let shelled = rt.block_on(async {
                let clock = Arc::new(VirtualClock::start(1_763_640_000_000_000));
                let stub = Arc::new(Stub {
                    clock: clock.clone(),
                    answers,
                    connections: AtomicUsize::new(0),
                    came: Mutex::new(Vec::new()),
                });
                let header = Arc::new(header.clone());
                let carrier = Mixed {
                    rest: RestCarrier::new(
                        header.clone(),
                        OneStand,
                        Pipe {
                            stub,
                            throttle: AT_ONCE,
                            holds: Some(Arc::new(holds)),
                            connect: 0,
                            refuse: Arc::new(AtomicUsize::new(0)),
                            fail: None,
                            requests: Arc::new(AtomicUsize::new(0)),
                            eof: None,
                            connects: Arc::new(AtomicUsize::new(0)),
                        },
                        Config::new(NAMESPACE, RunSecret::new([7; 32]), clock.wall()),
                    ),
                    scripted: Scripted::new(script.clone()),
                };
                let runtimes = Runtimes {
                    sources: vec![
                        tokio::runtime::Handle::current();
                        header.profile.sources.max(1) as usize
                    ],
                };
                clock
                    .within(run(
                        header,
                        W,
                        clock.clone(),
                        Records::new(records.iter().cloned()),
                        Lines::new(),
                        Arc::new(carrier),
                        &runtimes,
                    ))
                    .await
                    .unwrap_or_else(|e| panic!("{name}/{seed}: {e}"))
                    .journal
            });
            let (theirs, ours) = (projected(&ran.journal), projected(&shelled));
            if let Some(i) =
                (0..theirs.len().max(ours.len())).find(|&i| theirs.get(i) != ours.get(i))
            {
                panic!(
                    "{name}/{seed}: line {i} differs: sim {:?}, the shell {:?}",
                    theirs.get(i),
                    ours.get(i)
                );
            }
            // What `sim` cannot know, as the stub sent it — for an answer that
            // came by its attempt's deadline; a censor carries none.
            let dispatched: BTreeMap<(u64, u32), (u64, u64)> = shelled
                .iter()
                .filter_map(|l| match l {
                    Line::Dispatch(d) => Some(((d.line, d.attempt), (d.at, d.deadline))),
                    _ => None,
                })
                .collect();
            for l in &shelled {
                let Line::Answer(a) = l else { continue };
                let Some((status, code)) = sent.get(&(a.line, a.attempt)) else {
                    continue;
                };
                let (at, deadline) = dispatched[&(a.line, a.attempt)];
                let r = &records[(a.line - 2) as usize];
                let s = script(a.line, a.attempt, r);
                let sent_at = at + s.sent_after;
                let came = s
                    .answer_after
                    .map(|d| sent_at + d)
                    .filter(|t| sent_at < deadline && *t <= deadline);
                let (status, code) = match came {
                    Some(_) => (Some(*status), code.clone()),
                    None => (None, None),
                };
                assert_eq!(
                    a.status, status,
                    "{name}/{seed} line {} attempt {}",
                    a.line, a.attempt
                );
                let journaled = a.code.as_ref().map(|c| serde_json::to_value(c).unwrap());
                assert_eq!(
                    journaled,
                    code.as_ref().map(|c| serde_json::json!({ "named": c })),
                    "{name}/{seed}"
                );
                *kinds
                    .entry(match a.judged {
                        Judged::Expected => "expected",
                        Judged::Unexpected => "unexpected",
                        Judged::NotTaken => "not taken",
                        Judged::Unknown => "unknown",
                    })
                    .or_default() += 1;
                if a.attempt > 1 {
                    *kinds.entry("resent").or_default() += 1;
                }
                if !a.captured.is_empty() {
                    *kinds.entry("captured").or_default() += 1;
                }
                if a.after.is_some() {
                    *kinds.entry("retry-after").or_default() += 1;
                }
            }
            runs += 1;
        }
    }
    eprintln!("{runs} runs over REST, answers {kinds:?}");
    for kind in [
        "expected",
        "unexpected",
        "not taken",
        "unknown",
        "resent",
        "captured",
        "retry-after",
    ] {
        assert!(
            kinds.get(kind).copied().unwrap_or(0) > 0,
            "{kind} never reached: {kinds:?}"
        );
    }
}

/// An answer that came exactly at its deadline is taken, as `sim` takes it:
/// the attempt leaves only past the deadline.
#[test]
fn an_answer_at_its_deadline_is_taken() {
    let mut header = rest_header();
    header.operations.iter_mut().for_each(|o| o.timeout = 5_000);
    let records = vec![call("read", 1, 0, 0, 0)];
    let (journal, _, _) = run_rest(header, &records, vec![ok(5_000, 0)], AT_ONCE);
    let journal = journal.unwrap();
    assert_eq!(end(&journal, 2), Some((End::Expected, 5_000)));
}

/// Decision 17: the stand's connect and write, journaled where they end — a
/// connect of 70 µs from the attempt's hand-off to its request handed to a
/// ready connection; a write held 30 µs from there to its last byte.
#[test]
fn the_connect_and_the_write_are_journaled_apart() {
    let records = vec![call("read", 1, 0, 0, 0)];
    let throttle = Throttle {
        first: 30,
        chunk: usize::MAX,
        gap: 0,
    };
    let (journal, _, _) =
        run_rest_connecting(rest_header(), &records, vec![ok(10, 0)], throttle, 70);
    let journal = journal.unwrap();
    let s = journal
        .iter()
        .find_map(|l| match l {
            Line::Sent(s) if s.line == 2 => Some(s.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!((s.handed, s.connected, s.at), (0, 70, 100));
}

/// The shell's clock with chosen sleeps put off: a sleep due at `at` ends
/// `lags[at]` later — the source's runtime busy at that instant. The stub
/// keeps the true clock.
struct Lagging {
    inner: Arc<VirtualClock>,
    lags: BTreeMap<u64, u64>,
}

impl Clock for Lagging {
    fn now(&self) -> u64 {
        self.inner.now()
    }

    fn wall(&self) -> i64 {
        self.inner.wall()
    }

    fn sleep_until(&self, at: u64) -> impl Future<Output = ()> + Send + '_ {
        self.inner
            .sleep_until(at + self.lags.get(&at).copied().unwrap_or(0))
    }

    fn settled(&self, at: u64) -> impl Future<Output = ()> + Send + '_ {
        self.inner.settled(at)
    }
}

/// A call at 0, its deadline 10,000, J 2,000: the looks due at 9,000 and
/// 10,001, `lags` putting them off, the stub answering at `answer`.
fn looked(lags: &[(u64, u64)], answer: Answer) -> Vec<Line> {
    let mut header = rest_header();
    header.profile.jitter = 2_000;
    header
        .operations
        .iter_mut()
        .for_each(|o| o.timeout = 10_000);
    let records = [call("read", 1, 0, 0, 0)];
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    rt.block_on(async {
        let inner = Arc::new(VirtualClock::start(1_763_640_000_000_000));
        let clock = Arc::new(Lagging {
            inner: inner.clone(),
            lags: lags.iter().copied().collect(),
        });
        let stub = Arc::new(Stub {
            clock: inner.clone(),
            answers: [(id(&records[0], 1), answer)].into_iter().collect(),
            connections: AtomicUsize::new(0),
            came: Mutex::new(Vec::new()),
        });
        let header = Arc::new(header);
        let carrier = RestCarrier::new(
            header.clone(),
            OneStand,
            Pipe {
                stub,
                throttle: AT_ONCE,
                holds: None,
                connect: 0,
                refuse: Arc::new(AtomicUsize::new(0)),
                fail: None,
                requests: Arc::new(AtomicUsize::new(0)),
                eof: None,
                connects: Arc::new(AtomicUsize::new(0)),
            },
            Config::new(NAMESPACE, RunSecret::new([7; 32]), inner.wall()),
        );
        let runtimes = Runtimes {
            sources: vec![tokio::runtime::Handle::current()],
        };
        inner
            .within(run(
                header,
                W,
                clock,
                Records::new(records.iter().cloned()),
                Lines::new(),
                Arc::new(carrier),
                &runtimes,
            ))
            .await
            .unwrap()
            .journal
    })
}

fn found(journal: &[Line]) -> Option<(u64, journal::Found)> {
    journal.iter().find_map(|l| match l {
        Line::Answer(a) => a.found.map(|f| (a.at, f)),
        _ => None,
    })
}

/// Decision 17, version 5, its bookkeeping under the virtual clock: a late
/// answer carries where it was not — the early look's instant, or the
/// deadline if that look itself came late; none when the look past the
/// deadline found nothing, the attempt then left. (The virtual runtime is never
/// busy: a late look leaves the task waiting, and it reads the answer as it
/// comes; a runtime that cannot read is the real-clock twin's.)
#[test]
fn a_late_answer_carries_where_it_was_not() {
    let at = |t: u64| ok(t, 0);
    let bracket = |absent, deadline| journal::Found { absent, deadline };
    // The look past the deadline put off 5 ms: the answer, in at 13,000, is
    // read as it comes, absent since the early look.
    assert_eq!(
        found(&looked(&[(10_001, 5_000)], at(13_000))),
        Some((13_000, bracket(9_000, 10_000)))
    );
    // On time: the look at 10,001 finds nothing, and the attempt leaves.
    assert_eq!(found(&looked(&[], at(13_000))), None);
    // Put off 500 µs, the answer in at 10,200: found blind for 1,200.
    assert_eq!(
        found(&looked(&[(10_001, 500)], at(10_200))),
        Some((10_200, bracket(9_000, 10_000)))
    );
    // The early look itself at the deadline: absent from there.
    assert_eq!(
        found(&looked(&[(9_000, 1_000), (10_001, 5_000)], at(13_000))),
        Some((13_000, bracket(10_000, 10_000)))
    );
    // In time, no bracket.
    assert_eq!(found(&looked(&[(10_001, 5_000)], at(9_500))), None);
}

use ascetic_ddd_command_log::record::{Field, Hole, Safety, TimeBase, TimeRendering, Value};
use ascetic_ddd_load_run::decorate::{Decoration, Decorator};

/// The host's decorator in a test: a fresh proof an attempt, minted in
/// `takes` µs of the clock; or the header it is told to give.
struct Proofs {
    clock: Arc<VirtualClock>,
    takes: u64,
    minted: AtomicUsize,
    give: Option<(String, String)>,
}

impl Decorator for Proofs {
    async fn decorate<'a>(&'a self, _: Decoration<'a>) -> Result<Vec<(String, String)>, String> {
        // Never, for `u64::MAX`: a host's minting over a dead IdP.
        if self.takes == u64::MAX {
            std::future::pending::<()>().await;
        }
        let until = self.clock.now() + self.takes;
        self.clock.sleep_until(until).await;
        let n = self.minted.fetch_add(1, Ordering::SeqCst) + 1;
        // Told to fail: its error, the host's own text.
        if let Some((name, why)) = &self.give
            && name == "!fail"
        {
            return Err(why.clone());
        }
        Ok(vec![self.give.clone().unwrap_or((
            "dpop".to_owned(),
            format!("SECRET-proof-{n}"),
        ))])
    }
}

/// Operations that decorate `dpop`, a refusal certified, a resend's floor.
fn decorating() -> Header {
    let mut h = rest_header();
    for o in &mut h.operations {
        o.decorated = vec!["dpop".into()];
        o.safety = Safety::NotTaken;
        o.not_run = vec!["service.unavailable".into()];
    }
    h
}

/// `r` with a header its send's wall time fills.
fn stamped(mut r: Record) -> Record {
    let field = Field {
        name: "x-sent".into(),
        value: Value::Hole(Hole::Time {
            base: TimeBase::Sent,
            offset: 0,
            rendering: TimeRendering::EpochMillis,
        }),
    };
    match &mut r.action {
        Action::Rest(rest) => rest.headers.push(field),
        Action::Observe(o) => {
            if let Probe::Rest(rest) = &mut o.probe {
                rest.headers.push(field);
            }
        }
        _ => {}
    }
    r
}

fn refused_503() -> Answer {
    Answer::Ok {
        head: 10,
        body: 0,
        status: 503,
        // A second's wait: the resend's own instant renders a millisecond
        // apart, so only the first attempt's keeps its bytes.
        headers: vec![
            ("content-type".into(), "application/problem+json".into()),
            ("retry-after".into(), "1".into()),
        ],
        bytes: br#"{"code":"service.unavailable"}"#.to_vec(),
        close: false,
    }
}

/// A run decorated by `proofs`; every attempt's answer by its id.
fn run_decorated(
    header: Header,
    records: &[Record],
    answers: Vec<(String, Answer)>,
    takes: u64,
    give: Option<(String, String)>,
) -> (Result<Vec<Line>, RunError>, Vec<Came>, usize) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    rt.block_on(async {
        let clock = Arc::new(VirtualClock::start(1_763_640_000_000_000));
        let stub = Arc::new(Stub {
            clock: clock.clone(),
            answers: answers.into_iter().collect(),
            connections: AtomicUsize::new(0),
            came: Mutex::new(Vec::new()),
        });
        let proofs = Proofs {
            clock: clock.clone(),
            takes,
            minted: AtomicUsize::new(0),
            give,
        };
        let header = Arc::new(header);
        let carrier = RestCarrier::decorated(
            header.clone(),
            OneStand,
            Pipe {
                stub: stub.clone(),
                throttle: AT_ONCE,
                holds: None,
                connect: 0,
                refuse: Arc::new(AtomicUsize::new(0)),
                fail: None,
                requests: Arc::new(AtomicUsize::new(0)),
                eof: None,
                connects: Arc::new(AtomicUsize::new(0)),
            },
            proofs,
            Config::new(NAMESPACE, RunSecret::new([7; 32]), clock.wall()),
        );
        let runtimes = Runtimes {
            sources: vec![tokio::runtime::Handle::current()],
        };
        let ran = clock
            .within(run(
                header,
                W,
                clock.clone(),
                Records::new(records.iter().cloned()),
                Lines::new(),
                Arc::new(carrier),
                &runtimes,
            ))
            .await;
        let came = stub.came.lock().unwrap().clone();
        (ran.map(|r| r.journal), came, 0)
    })
}

/// A request's lines but those an attempt makes its own.
fn planned_lines(bytes: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(bytes)
        .split("\r\n")
        .filter(|l| {
            let l = l.to_ascii_lowercase();
            !l.starts_with("dpop:") && !l.starts_with("x-correlation-id:")
        })
        .map(str::to_owned)
        .collect()
}

/// K2-5 (ADR-0047 decision 16): decorated once an attempt — a fresh proof
/// each — and every other byte of a resend its first attempt's, the time its
/// first was sent among them; a correlation id an attempt.
#[test]
fn a_resend_is_its_first_attempt_s_bytes_but_its_own_proof() {
    let records = vec![stamped(call("update", 1, 0, 0, 0))];
    let answers = vec![
        (id(&records[0], 1), refused_503()),
        (id(&records[0], 2), ok(10, 0)),
    ];
    let (journal, came, _) = run_decorated(decorating(), &records, answers, 0, None);
    assert_eq!(end(&journal.unwrap(), 2).map(|e| e.0), Some(End::Expected));
    assert_eq!(came.len(), 2);
    let text = |c: &Came| String::from_utf8_lossy(&c.bytes).into_owned();
    assert_eq!(
        field(&text(&came[0]), "dpop").as_deref(),
        Some("SECRET-proof-1")
    );
    assert_eq!(
        field(&text(&came[1]), "dpop").as_deref(),
        Some("SECRET-proof-2")
    );
    assert_ne!(came[0].id, came[1].id);
    assert!(field(&text(&came[0]), "x-sent").is_some());
    assert_eq!(planned_lines(&came[0].bytes), planned_lines(&came[1].bytes));
}

/// K2-5: each poll's frame its own — the time it was sent its own dispatch's.
#[test]
fn each_poll_s_frame_is_its_own() {
    let records = vec![stamped(polling(1, 0, 0, 5_000, 20_000))];
    let not_yet = Answer::Ok {
        head: 10,
        body: 0,
        status: 409,
        headers: vec![],
        bytes: vec![],
        close: false,
    };
    let answers = vec![
        (id(&records[0], 1), not_yet),
        (id(&records[0], 2), ok(10, 0)),
    ];
    let (journal, came, _) = run_decorated(decorating(), &records, answers, 0, None);
    journal.unwrap();
    assert_eq!(came.len(), 2);
    let sent = |c: &Came| field(&String::from_utf8_lossy(&c.bytes), "x-sent").unwrap();
    assert_ne!(sent(&came[0]), sent(&came[1]));
}

/// The decorator's time is the harness's (decision 17): the attempt handed,
/// ready to connect, only once it is decorated.
#[test]
fn the_decorator_s_time_is_the_harness_s() {
    let records = vec![call("read", 1, 0, 0, 0)];
    let (journal, _, _) = run_decorated(
        decorating(),
        &records,
        vec![(id(&records[0], 1), ok(10, 0))],
        100,
        None,
    );
    let s = journal
        .unwrap()
        .iter()
        .find_map(|l| match l {
            Line::Sent(s) => Some(s.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!((s.handed, s.connected, s.at), (100, 100, 100));
}

/// A decorated header its operation does not declare, a value no header
/// takes, a decorating operation without a decorator: each refused.
#[test]
fn what_a_decorator_may_give_is_the_operation_s() {
    let records = vec![call("read", 1, 0, 0, 0)];
    let answers = || vec![(id(&records[0], 1), ok(10, 0))];
    // The run stops, its fault the journal's last line, named, no value in it
    // (decision 18).
    for (give, cannot) in [
        (("authorization", "x"), "rest.undeclared_header"),
        (("dpop", "a\nb"), "rest.bad_header"),
    ] {
        let give = Some((give.0.to_owned(), give.1.to_owned()));
        let (journal, _, _) = run_decorated(decorating(), &records, answers(), 0, give);
        match journal.unwrap().last() {
            Some(Line::Fault(journal::Fault {
                class:
                    journal::FaultClass::Carrier {
                        line: 2,
                        attempt: 1,
                        cannot: journal::Code::Named(c),
                    },
                ..
            })) => assert_eq!(c, cannot),
            other => panic!("{other:?}"),
        }
    }
    let (journal, _, _) = run_rest(decorating(), &records, vec![ok(10, 0)], AT_ONCE);
    assert!(
        matches!(journal, Err(RunError::Refused(ref why)) if why.contains("decorates")),
        "{journal:?}"
    );
}

/// K2-5: no secret in the journal — a decorator's proof, the run's secret —
/// raw, nor in base64, base64url or hex.
#[test]
fn no_secret_reaches_the_journal() {
    let records = vec![stamped(call("update", 1, 0, 0, 0))];
    let answers = vec![
        (id(&records[0], 1), refused_503()),
        (id(&records[0], 2), ok(10, 0)),
    ];
    let (journal, _, _) = run_decorated(decorating(), &records, answers, 0, None);
    let text: String = journal
        .unwrap()
        .iter()
        .map(|l| serde_json::to_string(l).unwrap())
        .collect();
    let hex = |b: &[u8]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
    let b64 = |b: &[u8], url: bool| {
        const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::new();
        for chunk in b.chunks(3) {
            let n =
                chunk.iter().fold(0u32, |n, x| (n << 8) | u32::from(*x)) << (8 * (3 - chunk.len()));
            for i in 0..=chunk.len() {
                let c = A[((n >> (18 - 6 * i)) & 63) as usize];
                out.push(match (url, c) {
                    (true, b'+') => '-',
                    (true, b'/') => '_',
                    (_, c) => c as char,
                });
            }
        }
        out
    };
    for secret in [
        b"SECRET-proof-1".as_slice(),
        b"SECRET-proof-2".as_slice(),
        &[7u8; 32],
    ] {
        for form in [
            String::from_utf8_lossy(secret).into_owned(),
            hex(secret),
            b64(secret, false),
            b64(secret, true),
        ] {
            assert!(!text.contains(&form), "{form} in the journal");
        }
    }
    assert!(text.contains("\"line\""));
}

use ascetic_ddd_command_log::record::On;

/// An answer of `status`, its problem's code `code`, its wait a second.
fn refusal(status: u16, code: &str) -> Answer {
    Answer::Ok {
        head: 10,
        body: 0,
        status,
        headers: vec![
            ("content-type".into(), "application/problem+json".into()),
            ("retry-after".into(), "1".into()),
        ],
        bytes: format!(r#"{{"code":"{code}"}}"#).into_bytes(),
        close: false,
    }
}

/// The operations' safety and the codes that certify a refusal of them.
fn certified(safety: Safety, not_run: &[&str]) -> Header {
    let mut h = decorating();
    for o in &mut h.operations {
        o.safety = safety;
        o.not_run = not_run.iter().map(|c| (*c).to_owned()).collect();
    }
    h
}

/// The journal of `lines` folded against `records`' log, under `header`.
fn folded(
    header: &Header,
    records: &[Record],
    lines: &[Line],
) -> ascetic_ddd_scheduler::verdict::Folded {
    use ascetic_ddd_scheduler::journal::{JOURNAL_VERSION, Reader, Writer};
    use ascetic_ddd_scheduler::machine::MACHINE_REVISION;
    use ascetic_ddd_scheduler::verdict::{Log, fold};
    let log = String::from_utf8(bytes(header.clone(), records)).unwrap();
    let sha256 =
        serde_json::from_str::<serde_json::Value>(log.lines().last().unwrap()).unwrap()["trailer"]
            ["sha256"]
            .as_str()
            .unwrap()
            .to_owned();
    let h = journal::Header {
        journal: JOURNAL_VERSION,
        machine: MACHINE_REVISION,
        source: "load-run rest".to_owned(),
        log: sha256.clone(),
        validator: ascetic_ddd_command_log::live::validator(),
        read_ahead: W,
        jitter: header.profile.jitter,
        resend: header.profile.resend,
        namespace: 1,
        t0: Some(0),
        gates: Vec::new(),
        judging: None,
    };
    let mut w = Writer::new(Vec::new(), h.clone()).unwrap();
    for l in lines {
        w.line(l).unwrap();
    }
    let bytes = w.finish().unwrap().0;
    let open = || Reader::new(std::io::Cursor::new(bytes.clone())).unwrap();
    fold(
        &h,
        &Log {
            sha256,
            records: records.len() as u64,
        },
        open(),
        open(),
    )
}

/// K2-4 (ADR-0047 decisions 11, 15): a refusal resent only when its code
/// certifies it not run — a 503 with any other code, its `Retry-After`
/// notwithstanding, is the system's unknown; `service.session_failed`, where
/// the operation lists it, resent after its wait.
#[test]
fn only_a_certified_refusal_is_resent() {
    let records = vec![call("update", 1, 0, 0, 0)];
    let header = certified(Safety::NotTaken, &["service.session_failed"]);
    let (journal, came, _) = run_decorated(
        header.clone(),
        &records,
        vec![(id(&records[0], 1), refusal(503, "service.unavailable"))],
        0,
        None,
    );
    let journal = journal.unwrap();
    assert_eq!(came.len(), 1, "an uncertified 503 resent");
    let a = answer(&journal, 2).unwrap();
    assert_eq!((a.status, a.judged), (Some(503), Judged::Unknown));
    let (journal, came, _) = run_decorated(
        header,
        &records,
        vec![
            (id(&records[0], 1), refusal(503, "service.session_failed")),
            (id(&records[0], 2), ok(10, 0)),
        ],
        0,
        None,
    );
    assert_eq!(came.len(), 2);
    assert_eq!(end(&journal.unwrap(), 2).map(|e| e.0), Some(End::Expected));
}

/// K2-4: an Envoy-shaped 504 — no problem document, a gateway's text — is the
/// system's unknown, and the step awaiting it settled stands on doubt.
#[test]
fn a_gateway_s_504_leaves_its_settled_dependent_conditional() {
    let a = call("update", 1, 0, 0, 0);
    let b = after(call("read", 2, 0, 10, 0), &a.step, On::Settled);
    let records = vec![a, b];
    let header = certified(Safety::NotTaken, &["service.unavailable"]);
    let envoy = Answer::Ok {
        head: 10,
        body: 0,
        status: 504,
        headers: vec![("content-type".into(), "text/plain".into())],
        bytes: b"upstream request timeout".to_vec(),
        close: false,
    };
    let (journal, came, _) = run_decorated(
        header.clone(),
        &records,
        vec![(id(&records[0], 1), envoy), (id(&records[1], 1), ok(10, 0))],
        0,
        None,
    );
    let journal = journal.unwrap();
    assert_eq!(came.len(), 2);
    let f = folded(&header, &records, &journal);
    use ascetic_ddd_scheduler::verdict::Class;
    assert_eq!(f.classes, vec![Class::Root, Class::Inconclusive]);
}

/// K2-4: a call that is never resent, its bytes out and no answer by its
/// deadline, is not resent: censored once, unknown.
#[test]
fn a_call_never_resent_is_not_resent_on_its_timeout() {
    let mut header = certified(Safety::Never, &[]);
    header.operations.iter_mut().for_each(|o| o.timeout = 5_000);
    let records = vec![call("update", 1, 0, 0, 0)];
    let (journal, came, _) = run_decorated(
        header,
        &records,
        vec![(id(&records[0], 1), Answer::Silent)],
        0,
        None,
    );
    let journal = journal.unwrap();
    assert_eq!(came.len(), 1);
    assert_eq!(end(&journal, 2), Some((End::Unknown, 5_000)));
}

/// Decision 16: connects a floor apart, the first at once — two refused, the
/// third made at two floors; refused until the deadline, never connected,
/// the attempt censored there, never sent.
#[test]
fn refused_connects_are_tried_a_floor_apart() {
    use ascetic_ddd_load_run::rest::CONNECT_FLOOR;
    let records = vec![call("read", 1, 0, 0, 0)];
    let (journal, _, _) = run_rest_with(
        rest_header(),
        &records,
        vec![ok(10, 0)],
        AT_ONCE,
        0,
        2,
        None,
        None,
        &|_| {},
    );
    let journal = journal.unwrap();
    let s = journal
        .iter()
        .find_map(|l| match l {
            Line::Sent(s) => Some(s.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!((s.handed, s.connected), (0, 2 * CONNECT_FLOOR));
    assert_eq!(end(&journal, 2).map(|e| e.0), Some(End::Expected));
    let mut header = rest_header();
    header
        .operations
        .iter_mut()
        .for_each(|o| o.timeout = 25_000);
    let (journal, _, _) = run_rest_with(
        header,
        &records,
        vec![ok(10, 0)],
        AT_ONCE,
        0,
        100,
        None,
        None,
        &|_| {},
    );
    let journal = journal.unwrap();
    assert!(sent(&journal, 2).is_empty());
    assert_eq!(end(&journal, 2), Some((End::Unknown, 25_000)));
}

/// Decision 16: a connection idle less than the profile's idle less the
/// margin reused; one idle longer retired unused; a profile whose idle lies
/// within the margin refused before T0.
#[test]
fn an_idle_connection_is_retired_before_the_gateway_s_idle() {
    let tune = |c: &mut Config| c.idle_margin = 2_000;
    let mut header = rest_header();
    header.profile.idle = 10_000;
    for (later, connections) in [(5_000, 1), (20_000, 2)] {
        let records = vec![call("read", 1, 0, 0, 0), call("read", 2, 0, later, 0)];
        let (journal, _, opened) = run_rest_with(
            header.clone(),
            &records,
            vec![ok(10, 0), ok(10, 0)],
            AT_ONCE,
            0,
            0,
            None,
            None,
            &tune,
        );
        journal.unwrap();
        assert_eq!(opened, connections, "a second call {later} µs on");
    }
    header.profile.idle = 2_000;
    let records = vec![call("read", 1, 0, 0, 0)];
    let (journal, _, _) = run_rest_with(
        header,
        &records,
        vec![ok(10, 0)],
        AT_ONCE,
        0,
        0,
        None,
        None,
        &tune,
    );
    assert!(
        matches!(journal, Err(RunError::Refused(ref why)) if why.contains("idle")),
        "{journal:?}"
    );
}

/// Decision 16: a request whose connection is gone under it before a byte
/// of it is accepted is no attempt's end — the next connection takes it, and
/// the stand sees it once.
#[test]
fn a_request_no_byte_of_which_left_goes_on_the_next_connection() {
    let records = vec![call("read", 1, 0, 0, 0), call("read", 2, 0, 1_000, 0)];
    let (journal, came, opened) = run_rest_with(
        rest_header(),
        &records,
        vec![ok(10, 0), ok(10, 0)],
        AT_ONCE,
        0,
        0,
        Some(2),
        None,
        &|_| {},
    );
    let journal = journal.unwrap();
    assert_eq!(opened, 2);
    let ids: Vec<String> = came.iter().map(|c| c.id.clone()).collect();
    assert_eq!(ids, vec![id(&records[0], 1), id(&records[1], 1)]);
    assert_eq!(end(&journal, 2).map(|e| e.0), Some(End::Expected));
    assert_eq!(end(&journal, 3).map(|e| e.0), Some(End::Expected));
}

/// Decision 18 (K2b-7a): a pooled connection the stand closed while it sat
/// idle, seen only once the next request wakes it — hyper flushes its empty
/// buffer, then hands the request back. No byte of it left, so no `Sent`:
/// the request goes on the next connection and is sent when it leaves there.
#[test]
fn a_connection_closed_while_idle_sends_nothing() {
    let records = vec![call("read", 1, 0, 0, 0), call("read", 2, 0, 10_000, 0)];
    let (journal, came, opened) = run_rest_with(
        rest_header(),
        &records,
        vec![ok(10, 0), ok(10, 0)],
        AT_ONCE,
        100,
        0,
        None,
        Some(5_000),
        &|_| {},
    );
    let journal = journal.unwrap();
    assert_eq!(opened, 2);
    let came: Vec<u64> = came
        .iter()
        .filter(|c| c.id == id(&records[1], 1))
        .map(|c| c.at)
        .collect();
    assert_eq!(came.len(), 1);
    assert_eq!(sent(&journal, 3), came);
    assert_eq!(end(&journal, 3).map(|e| e.0), Some(End::Expected));
}

/// Decision 18 (K2b-7a): every await of an attempt races its deadline — a
/// connect or a decorator that never answers leaves the attempt unsent at its
/// deadline, censored there, and the run ends with it.
#[test]
fn a_connect_or_a_decorator_that_never_answers_ends_at_the_deadline() {
    let records = vec![call("read", 1, 0, 0, 0)];
    let connecting =
        run_rest_connecting(rest_header(), &records, vec![ok(10, 0)], AT_ONCE, u64::MAX);
    let decorating = run_decorated(
        decorating(),
        &records,
        vec![(id(&records[0], 1), ok(10, 0))],
        u64::MAX,
        None,
    );
    for (what, (journal, came, _)) in [("connect", connecting), ("decorator", decorating)] {
        let journal = journal.unwrap();
        assert!(came.is_empty(), "{what}");
        assert!(sent(&journal, 2).is_empty(), "{what}");
        assert_eq!(end(&journal, 2).map(|e| e.0), Some(End::Unknown), "{what}");
    }
}

/// Decision 18: a decorator's error is the host's text — it may carry a proof
/// — and stays out of the journal; the stop journals only its name.
#[test]
fn a_carrier_s_detail_stays_out_of_the_journal() {
    let records = vec![call("read", 1, 0, 0, 0)];
    let answers = vec![(id(&records[0], 1), ok(10, 0))];
    let give = Some(("!fail".to_owned(), "secret.proof_abc".to_owned()));
    let (journal, _, _) = run_decorated(decorating(), &records, answers, 0, give);
    let journal = journal.unwrap();
    let text: String = journal
        .iter()
        .map(|l| serde_json::to_string(l).unwrap())
        .collect();
    assert!(!text.contains("secret"), "{text}");
    assert!(matches!(
        journal.last(),
        Some(Line::Fault(journal::Fault {
            class: journal::FaultClass::Carrier {
                cannot: journal::Code::Named(c),
                ..
            },
            ..
        })) if c == "rest.decorator"
    ));
}

/// The log's records, then its error in place of its end.
struct Failing {
    records: std::vec::IntoIter<Record>,
    error: Option<ascetic_ddd_command_log::LogError>,
}

impl ascetic_ddd_load_run::source::LineSource for Failing {
    fn poll_next(
        &mut self,
        _: &mut Context<'_>,
    ) -> Poll<Result<Option<Arc<Record>>, ascetic_ddd_command_log::LogError>> {
        Poll::Ready(match self.records.next() {
            Some(r) => Ok(Some(Arc::new(r))),
            None => match self.error.take() {
                Some(e) => Err(e),
                None => Ok(None),
            },
        })
    }
}

/// Decision 18: a stop aborts the run's attempts and their connections'
/// tasks — three connecting until 5,000 µs, or handed to hyper and written
/// at 5,000, when the log fails at 2,000 — so no request of theirs reaches
/// the stand, then or after the run returned; none is left running.
#[test]
fn a_stop_leaves_no_request_behind() {
    let held = Throttle {
        first: 5_000,
        chunk: usize::MAX,
        gap: 0,
    };
    for (connect, throttle) in [(5_000, AT_ONCE), (0, held)] {
        stop_leaves_no_request(connect, throttle);
    }
}

fn stop_leaves_no_request(connect: u64, throttle: Throttle) {
    use ascetic_ddd_load_run::run::{RunEnd, Stop};
    let records = vec![
        call("read", 1, 0, 0, 0),
        call("read", 2, 0, 0, 0),
        call("read", 3, 0, 0, 0),
        call("read", 4, 0, 3_000, 0),
    ];
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let (ran, at_return, after) = rt.block_on(async {
        let clock = Arc::new(VirtualClock::start(1_763_640_000_000_000));
        let stub = Arc::new(Stub {
            clock: clock.clone(),
            answers: records
                .iter()
                .map(|r| id(r, 1))
                .zip(vec![ok(10, 0); 4])
                .collect(),
            connections: AtomicUsize::new(0),
            came: Mutex::new(Vec::new()),
        });
        let header = Arc::new(rest_header());
        let carrier = RestCarrier::new(
            header.clone(),
            OneStand,
            Pipe {
                stub: stub.clone(),
                throttle,
                holds: None,
                connect,
                refuse: Arc::new(AtomicUsize::new(0)),
                fail: None,
                requests: Arc::new(AtomicUsize::new(0)),
                eof: None,
                connects: Arc::new(AtomicUsize::new(0)),
            },
            Config::new(NAMESPACE, RunSecret::new([7; 32]), clock.wall()),
        );
        let runtimes = Runtimes {
            sources: vec![tokio::runtime::Handle::current()],
        };
        let source = Failing {
            records: records.clone().into_iter(),
            error: Some(ascetic_ddd_command_log::LogError::Truncated),
        };
        let ran = clock
            .within(run(
                header,
                W,
                clock.clone(),
                source,
                Lines::new(),
                Arc::new(carrier),
                &runtimes,
            ))
            .await
            .unwrap();
        let at_return = stub.came.lock().unwrap().len();
        clock.sleep_until(clock.now() + 1_000_000).await;
        let after = stub.came.lock().unwrap().len();
        (ran, at_return, after)
    });
    assert_eq!((at_return, after), (0, 0), "connect {connect}");
    assert_eq!(ran.running, 0);
    assert!(
        matches!(ran.end, RunEnd::Stopped(Stop::Log(_))),
        "{:?}",
        ran.end
    );
    assert!(matches!(
        ran.journal.last(),
        Some(Line::Fault(f)) if f.at >= 3_000 - W
    ));
}

/// Decision 18: a run's future dropped — its caller gone — takes its
/// attempts with it: none of the three connecting reaches the stand after.
#[test]
fn a_run_dropped_takes_its_attempts_with_it() {
    let records = vec![
        call("read", 1, 0, 0, 0),
        call("read", 2, 0, 0, 0),
        call("read", 3, 0, 0, 0),
    ];
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let came = rt.block_on(async {
        let clock = Arc::new(VirtualClock::start(1_763_640_000_000_000));
        let stub = Arc::new(Stub {
            clock: clock.clone(),
            answers: records
                .iter()
                .map(|r| id(r, 1))
                .zip(vec![ok(10, 0); 3])
                .collect(),
            connections: AtomicUsize::new(0),
            came: Mutex::new(Vec::new()),
        });
        let header = Arc::new(rest_header());
        let carrier = RestCarrier::new(
            header.clone(),
            OneStand,
            Pipe {
                stub: stub.clone(),
                throttle: AT_ONCE,
                holds: None,
                connect: 5_000,
                refuse: Arc::new(AtomicUsize::new(0)),
                fail: None,
                requests: Arc::new(AtomicUsize::new(0)),
                eof: None,
                connects: Arc::new(AtomicUsize::new(0)),
            },
            Config::new(NAMESPACE, RunSecret::new([7; 32]), clock.wall()),
        );
        let runtimes = Runtimes {
            sources: vec![tokio::runtime::Handle::current()],
        };
        let running = run(
            header,
            W,
            clock.clone(),
            Records::new(records.clone().into_iter()),
            Lines::new(),
            Arc::new(carrier),
            &runtimes,
        );
        tokio::select! {
            biased;
            _ = running => panic!("the run ended before its connects"),
            () = clock.sleep_until(2_000) => {}
        }
        clock.sleep_until(clock.now() + 1_000_000).await;
        stub.came.lock().unwrap().len()
    });
    assert_eq!(came, 0);
}
