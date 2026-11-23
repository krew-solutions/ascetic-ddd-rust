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
                    bytes,
                    close,
                } => {
                    self.clock.sleep_until(at + head).await;
                    let connection = if close { "connection: close\r\n" } else { "" };
                    let text = format!(
                        "HTTP/1.1 {status} X\r\ncontent-length: {}\r\n{connection}\r\n",
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
}

impl AsyncRead for Throttled {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for Throttled {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let now = self.clock.now();
        let first = self.first;
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
        self.next = Some(self.clock.now() + gap);
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
}

impl Egress for Pipe {
    type Io = Throttled;

    fn connect(&self, _: u32, _: &Route) -> impl Future<Output = io::Result<Throttled>> + Send {
        let stub = self.stub.clone();
        let t = self.throttle;
        async move {
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
            },
            Config {
                namespace: NAMESPACE,
                secret: RunSecret::new([7; 32]),
                run_start: clock.wall(),
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
