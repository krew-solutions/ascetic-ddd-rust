//! K2b-7e-2 of ADR-0047 (decision 22): the costs the shell and the REST
//! carrier state — an idle connection's, an open attempt's, an answer's
//! while it is read — held two-sided by a counting allocator, under the
//! virtual clock.
//!
//! What is counted is the harness's: the stand's futures run tagged, and
//! every block they allocate is theirs wherever it is let go; a connection's
//! bytes cross a pipe of fixed rings the stand allocates — a socket's buffers
//! are the kernel's, never the harness's heap. The machine's own bytes are
//! subtracted by `sim` running the same plan, sampled at the same states;
//! the run's constants by differencing two sizes of it. A binary of its own:
//! the allocator counts the whole process.

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

mod common;

use ascetic_ddd_load_run::sink::Journal;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};

use ascetic_ddd_command_log::answer::TAKEN;
use ascetic_ddd_command_log::fills::{Captured, Namespace};
use ascetic_ddd_command_log::record::{Header, Record, Rest};
use ascetic_ddd_command_log::render::RunSecret;
use ascetic_ddd_command_log::validate::Peaks;
use ascetic_ddd_load_run::carrier::{Carrier, Needs};
use ascetic_ddd_load_run::clock::{Clock, VirtualClock};
use ascetic_ddd_load_run::rest::{Config, READ, RestCarrier};
use ascetic_ddd_load_run::run::{ATTEMPT, Ready, Runtimes, admit, run};
use ascetic_ddd_load_run::sink::JournalSink;
use ascetic_ddd_load_run::source::{Plan, Records};
use ascetic_ddd_load_run::stand::{Connect, Egress, Route, Stand, Unfit};
use ascetic_ddd_scheduler::fits::VALUE;
use ascetic_ddd_scheduler::journal::{self, JournalError};
use ascetic_ddd_scheduler::machine::Judgement;
use ascetic_ddd_scheduler::sim;
use fixture::*;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

// The allocator: every block tagged with whose it is.

const HARNESS: usize = 0;
const STAND: usize = 1;

thread_local! {
    /// Whose the blocks allocated now are: the stand's while its futures run.
    static WHOSE: Cell<usize> = const { Cell::new(HARNESS) };
}

/// The bytes each holds, by [`HARNESS`] and [`STAND`].
static HELD: [AtomicUsize; 2] = [AtomicUsize::new(0), AtomicUsize::new(0)];
/// The harness's peak.
static PEAK: AtomicUsize = AtomicUsize::new(0);

/// The prefix a block carries before the caller's bytes: its owner, at the
/// alignment asked.
fn prefix(layout: Layout) -> usize {
    layout.align().max(16)
}

/// A block of `layout` for `whose`, its owner written in its prefix.
///
/// # Safety
///
/// `layout`'s size is not zero (`GlobalAlloc`'s contract).
unsafe fn place(layout: Layout, whose: usize, zeroed: bool) -> *mut u8 {
    let off = prefix(layout);
    let Some(outer) = layout
        .size()
        .checked_add(off)
        .and_then(|size| Layout::from_size_align(size, off).ok())
    else {
        return std::ptr::null_mut();
    };
    // SAFETY: `outer` is larger than the caller's non-zero layout.
    let base = unsafe {
        if zeroed {
            System.alloc_zeroed(outer)
        } else {
            System.alloc(outer)
        }
    };
    if base.is_null() {
        return base;
    }
    // SAFETY: `base` is aligned to at least 16 and holds `off` ≥ 16 bytes
    // before the caller's.
    unsafe { base.cast::<usize>().write(whose) };
    let now = HELD[whose]
        .fetch_add(layout.size(), Relaxed)
        .saturating_add(layout.size());
    if whose == HARNESS {
        PEAK.fetch_max(now, Relaxed);
    }
    // SAFETY: within `outer`, aligned to `off`, a multiple of the caller's.
    unsafe { base.add(off) }
}

/// The owner a block's prefix holds.
///
/// # Safety
///
/// `p` was returned by [`place`] for `layout`.
unsafe fn owner(p: *mut u8, layout: Layout) -> usize {
    // SAFETY: the prefix `place` wrote, `prefix(layout)` bytes before.
    unsafe { p.sub(prefix(layout)).cast::<usize>().read() }
}

struct Tagged;

// SAFETY: every block comes from the system allocator, `prefix` bytes larger,
// and goes back to it with the layout it was allocated with.
unsafe impl GlobalAlloc for Tagged {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let whose = WHOSE.try_with(Cell::get).unwrap_or(HARNESS);
        // SAFETY: the caller's contract, passed on.
        unsafe { place(layout, whose, false) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let whose = WHOSE.try_with(Cell::get).unwrap_or(HARNESS);
        // SAFETY: the caller's contract, passed on.
        unsafe { place(layout, whose, true) }
    }

    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        let off = prefix(layout);
        // SAFETY: `p` came from `place` for `layout`.
        let whose = unsafe { owner(p, layout) };
        HELD[whose].fetch_sub(layout.size(), Relaxed);
        // SAFETY: the block `place` allocated, with its layout.
        unsafe {
            System.dealloc(
                p.sub(off),
                Layout::from_size_align_unchecked(layout.size() + off, off),
            );
        }
    }

    /// A new block, the bytes copied, then the old one let go: a block moved
    /// counts both at once, as the system's realloc may hold them (the
    /// owner kept).
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: `p` came from `place` for `layout`.
        let whose = unsafe { owner(p, layout) };
        // SAFETY: the caller's contract: `size` non-zero, the layout valid.
        let new = unsafe { Layout::from_size_align_unchecked(size, layout.align()) };
        // SAFETY: as above.
        let q = unsafe { place(new, whose, false) };
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
static ALLOCATOR: Tagged = Tagged;

/// Runs `f` as the stand: what it allocates is the stand's.
fn standing<R>(f: impl FnOnce() -> R) -> R {
    let was = WHOSE.replace(STAND);
    let r = f();
    WHOSE.set(was);
    r
}

/// A future polled as the stand's.
struct Standing<F>(Pin<Box<F>>);

impl<F: Future> Future for Standing<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        standing(|| self.0.as_mut().poll(cx))
    }
}

/// Spawns `f` as the stand's, its task too.
fn stand_spawn(f: impl Future<Output = ()> + Send + 'static) {
    standing(|| drop(tokio::spawn(Standing(Box::pin(f)))));
}

// The pipe: two rings of fixed capacity, the stand's.

/// One way of a pipe.
struct Ring {
    bytes: Box<[u8]>,
    start: usize,
    len: usize,
    closed: bool,
    reader: Option<Waker>,
    writer: Option<Waker>,
}

type Way = Arc<Mutex<Ring>>;

fn way(capacity: usize) -> Way {
    Arc::new(Mutex::new(Ring {
        bytes: vec![0; capacity].into_boxed_slice(),
        start: 0,
        len: 0,
        closed: false,
        reader: None,
        writer: None,
    }))
}

fn lock(way: &Way) -> std::sync::MutexGuard<'_, Ring> {
    way.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A pipe's end: reads one way, writes the other; dropped, both closed.
struct End {
    rx: Way,
    tx: Way,
}

/// A pipe's two ends, each way of `capacity` bytes, allocated by the stand.
fn pipe(capacity: usize) -> (End, End) {
    standing(|| {
        let (a, b) = (way(capacity), way(capacity));
        (
            End {
                rx: a.clone(),
                tx: b.clone(),
            },
            End { rx: b, tx: a },
        )
    })
}

impl AsyncRead for End {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut r = lock(&self.rx);
        if r.len == 0 {
            if r.closed {
                return Poll::Ready(Ok(()));
            }
            r.reader = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let capacity = r.bytes.len();
        let n = r.len.min(buf.remaining());
        let first = n.min(capacity - r.start);
        let start = r.start;
        buf.put_slice(&r.bytes[start..start + first]);
        buf.put_slice(&r.bytes[..n - first]);
        r.start = (start + n) % capacity;
        r.len -= n;
        if let Some(w) = r.writer.take() {
            w.wake();
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for End {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut r = lock(&self.tx);
        if r.closed {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        let capacity = r.bytes.len();
        let free = capacity - r.len;
        if free == 0 {
            r.writer = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let n = free.min(data.len());
        let at = (r.start + r.len) % capacity;
        let first = n.min(capacity - at);
        r.bytes[at..at + first].copy_from_slice(&data[..first]);
        r.bytes[..n - first].copy_from_slice(&data[first..n]);
        r.len += n;
        if let Some(w) = r.reader.take() {
            w.wake();
        }
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut r = lock(&self.tx);
        r.closed = true;
        if let Some(w) = r.reader.take() {
            w.wake();
        }
        Poll::Ready(Ok(()))
    }
}

impl Drop for End {
    fn drop(&mut self) {
        for way in [&self.rx, &self.tx] {
            let mut r = lock(way);
            r.closed = true;
            let wakers = [r.reader.take(), r.writer.take()];
            drop(r);
            wakers.into_iter().flatten().for_each(Waker::wake);
        }
    }
}

// The stand.

/// How the stand answers: each part's bytes `after` µs after the request
/// came — the plan's reads, those that come before [`CUT`]; the probes, at
/// once.
struct Answers {
    reads: Vec<(u64, Vec<u8>)>,
    probes: Vec<(u64, Vec<u8>)>,
}

/// The instant before which the plan's reads come.
const CUT: u64 = 100_000;

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

fn content_length(head: &[u8]) -> usize {
    String::from_utf8_lossy(head)
        .lines()
        .find_map(|l| {
            let (n, v) = l.split_once(':')?;
            n.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| v.trim().parse().ok())
                .flatten()
        })
        .unwrap_or(0)
}

async fn serve(mut io: End, answers: Arc<Answers>, clock: Arc<VirtualClock>) {
    let mut buf = Vec::new();
    loop {
        let end = loop {
            if let Some(i) = find(&buf, b"\r\n\r\n") {
                break i + 4;
            }
            let mut chunk = [0u8; 4096];
            match io.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        };
        let len = content_length(&buf[..end]);
        while buf.len() < end + len {
            let mut chunk = [0u8; 4096];
            match io.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        }
        buf.drain(..end + len);
        let at = clock.now();
        let parts = if at < CUT {
            &answers.reads
        } else {
            &answers.probes
        };
        for (after, bytes) in parts {
            clock.sleep_until(at + after).await;
            if io.write_all(bytes).await.is_err() {
                return;
            }
        }
    }
}

/// A row: answers of `body` bytes, a JSON string — the profile's cap —,
/// the first `first` of them with the head and the rest [`REST`] µs later;
/// the head of `head` bytes in `fields` fields, or as small as they let it,
/// its bulk in the names when `names`; chunked, of no length, when
/// `chunked`.
#[derive(Clone, Copy, Debug)]
struct Row {
    body: usize,
    first: usize,
    head: usize,
    fields: usize,
    names: bool,
    /// One name for every field past its own two, its bulk in it.
    repeated: bool,
    chunked: bool,
    /// The reads' requests by class (K2b-7e-2b-3a-2).
    req: Req,
    /// Each read's `If-Match` a writer's captured value of `VALUE` bytes,
    /// the writer answered at once before it (K2b-7e-2b-3a-3).
    captured: bool,
}

/// A read's request beyond its group: a path value of `target` spaces (each
/// encoded `%20`), a header of `header` bytes, a body of `body` bytes.
#[derive(Clone, Copy, Debug, Default)]
struct Req {
    target: usize,
    header: usize,
    body: usize,
}

/// The row's answer, its parts' instants after the request.
fn answer(row: Row, at: u64) -> Vec<(u64, Vec<u8>)> {
    let Row {
        body,
        first,
        head,
        fields,
        names,
        repeated,
        chunked,
        req: _,
        captured: _,
    } = row;
    let framing = if chunked {
        "transfer-encoding: chunked".to_owned()
    } else {
        format!("content-length: {body}")
    };
    let own = format!("HTTP/1.1 200 X\r\ncontent-type: application/json\r\n{framing}\r\n");
    let mut text = own.clone();
    // The fields past its own two, `x-NN: aaa…` or `x-NNaaa…: a`, filling
    // the head to `head` bytes.
    let extra = fields.saturating_sub(2);
    let room = head.saturating_sub(own.len() + 2);
    // One name repeated: its share the same each field, `x-` and its bulk.
    let each = (room / extra.max(1)).saturating_sub(8).max(1);
    for i in 0..extra {
        let left = room.saturating_sub(text.len() - own.len());
        let share = left / (extra - i);
        // `x-NN`, `: `, `\r\n`: eight bytes beside the bulk.
        let bulk = share.saturating_sub(8).max(1);
        if repeated {
            text.push_str(&format!("x-aa{}: a\r\n", "a".repeat(each - 1)));
        } else if names {
            text.push_str(&format!("x-{i:02}{}: a\r\n", "a".repeat(bulk - 1)));
        } else {
            text.push_str(&format!("x-{i:02}: {}\r\n", "a".repeat(bulk)));
        }
    }
    text.push_str("\r\n");
    let mut data = vec![b'a'; body];
    data[0] = b'"';
    data[body - 1] = b'"';
    let (one, two) = data.split_at(first.min(body));
    let frame = |part: &[u8], last: bool| -> Vec<u8> {
        if !chunked {
            return part.to_vec();
        }
        let mut b = Vec::new();
        if !part.is_empty() {
            b.extend_from_slice(format!("{:x}\r\n", part.len()).as_bytes());
            b.extend_from_slice(part);
            b.extend_from_slice(b"\r\n");
        }
        if last {
            b.extend_from_slice(b"0\r\n\r\n");
        }
        b
    };
    let mut start = text.into_bytes();
    start.extend(frame(one, two.is_empty()));
    if two.is_empty() {
        return vec![(at, start)];
    }
    vec![(at, start), (at + REST, frame(two, true))]
}

/// The row's head, in bytes.
fn head_of(row: Row) -> usize {
    let bytes = &answer(row, 0)[0].1;
    find(bytes, b"\r\n\r\n").unwrap() + 4
}

struct Pipes {
    clock: Arc<VirtualClock>,
    answers: Arc<Answers>,
    /// Each way's capacity: a whole answer fits, as a socket's buffers hold
    /// it.
    ring: usize,
}

impl Egress for Pipes {
    type Io = End;

    fn ports(&self, _: u32) -> usize {
        100_000
    }

    fn admits(&self, _: u32) -> Result<(), Unfit> {
        Ok(())
    }

    fn reaches(&self, _: u32, _: &str) -> Result<(), Unfit> {
        Ok(())
    }

    fn connect(&self, _: u32, _: &Route) -> impl Future<Output = Result<End, Connect>> + Send {
        let (clock, answers, ring) = (self.clock.clone(), self.answers.clone(), self.ring);
        async move {
            let (client, server) = pipe(ring);
            stand_spawn(serve(server, answers, clock));
            Ok(client)
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

    fn endpoints(&self, _: &Header, _: u32) -> Vec<String> {
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

/// A journal not kept: its lines would weigh on what is measured.
struct Unkept;

impl JournalSink for Unkept {
    type Finished = ();

    fn line(&mut self, _: journal::Line) -> Result<(), JournalError> {
        Ok(())
    }

    fn finish(self) -> Result<(), JournalError> {
        Ok(())
    }
}

// The plan and its runs.

/// The plan's reads, at once.
const AT: u64 = 10_000;
/// Their answers' heads, after their requests.
const ANSWERED: u64 = 1_000_000;
/// A body's rest, after its head.
const REST: u64 = 10_000;
/// The states sampled: just past the dispatch; every read open, its request
/// sent; just past the answers' heads; a body's first part read; just past
/// the rest; every connection idle in its pool.
const DISPATCHED: u64 = AT + 1;
const OPEN: u64 = 600_000;
const HEADS: u64 = AT + ANSWERED + 1;
const MID: u64 = AT + ANSWERED + REST / 2;
const DONE: u64 = AT + ANSWERED + 2 * REST;
const IDLE: u64 = 1_600_000;
const SAMPLED: [u64; 6] = [DISPATCHED, OPEN, HEADS, MID, DONE, IDLE];
/// The probes' instants: `sim` samples the machine at their dispatch — the
/// states of `OPEN` and `IDLE`; the last keeps the run open.
const PROBES: [u64; 3] = [500_000, 1_500_000, 2_600_000];

fn plan(n: u64, row: Row) -> (Header, Vec<Record>) {
    let mut h = header();
    h.channels.clear();
    h.calls.clear();
    h.profile.answer_cap = row.body as u64;
    let mut records: Vec<Record> = Vec::new();
    if row.captured {
        // The writers first, at the reads' instant and answered at once, each
        // capturing its group's ETag; then the reads that send it, released
        // at its answer.
        records.extend((1..=n).map(|g| captures_etag(call("update", g, 0, AT, 0), g)));
    }
    records.extend((1..=n).map(|g| {
        let r = requested(call("read", g, 0, AT, 0), row.req);
        if !row.captured {
            return r;
        }
        let writer = id("device", g, "update", 0);
        let mut r = after(r, &writer, ascetic_ddd_command_log::record::On::Expected);
        r.action = read_etag("read", g, &writer);
        r
    }));
    records.extend(
        PROBES
            .iter()
            .zip(1..)
            .map(|(&at, g)| call("read", n + g, 0, at, 0)),
    );
    (h, records)
}

/// `r`'s request grown by `req`'s classes.
fn requested(mut r: Record, req: Req) -> Record {
    use ascetic_ddd_command_log::record::{Action, Codec, Field, Payload, Value};
    let Action::Rest(rest) = &mut r.action else {
        return r;
    };
    if req.target > 0 {
        rest.path = vec![Value::Literal(" ".repeat(req.target))];
    }
    if req.header > 0 {
        rest.headers.push(Field {
            name: "x-pad".into(),
            value: Value::Literal("a".repeat(req.header)),
        });
    }
    if req.body > 1 {
        let mut bytes = vec![b'a'; req.body];
        bytes[0] = b'"';
        bytes[req.body - 1] = b'"';
        rest.body = Some(Payload {
            codec: Codec::Json,
            bytes,
            holes: vec![],
        });
    }
    r
}

/// The script `sim` and the scripted carrier run: the reads answered after
/// [`ANSWERED`], the probes at once; `seen` called at each probe's dispatch.
fn script(n: u64, seen: impl Fn() + Send + Sync + 'static) -> Arc<common::Script> {
    Arc::new(move |line: u64, _: u32, r: &Record| {
        // The writers, answered at once with their capture; the reads, after
        // [`ANSWERED`]; then the probes.
        if let Some(c) = r.capture.first() {
            let mut value = Vec::with_capacity(VALUE as usize);
            value.push(b'"');
            value.extend(std::iter::repeat_n(*b"\\\"", TAKEN).flatten());
            value.push(b'"');
            assert_eq!(value.len() as u64, VALUE);
            return sim::Attempt {
                sent_after: 0,
                answer_after: Some(10),
                judgement: Judgement::Expected(vec![Captured {
                    subject: c.subject.clone(),
                    slot: c.slot.clone(),
                    value,
                }]),
                after: None,
            };
        }
        let read = r.at < PROBES[0];
        let _ = (line, n);
        if !read {
            seen();
        }
        sim::Attempt {
            sent_after: 0,
            answer_after: Some(if read { ANSWERED } else { 10 }),
            judgement: Judgement::Expected(vec![]),
            after: None,
        }
    })
}

/// The machine's bytes past its start, `sim` running the plan: held at the
/// first probe's dispatch, its peak until then, held at the second's.
struct Machine {
    open: usize,
    peak: usize,
    idle: usize,
}

fn machine(n: u64, row: Row) -> Machine {
    let (h, records) = plan(n, row);
    let samples = Arc::new(Mutex::new(Vec::with_capacity(8)));
    let base = HELD[HARNESS].load(Relaxed);
    PEAK.store(base, Relaxed);
    let seen = {
        let samples = samples.clone();
        move || {
            let now = HELD[HARNESS].load(Relaxed);
            let peak = PEAK.swap(now, Relaxed);
            let mut v = samples.lock().unwrap_or_else(PoisonError::into_inner);
            v.push(now.saturating_sub(base));
            v.push(peak.saturating_sub(base));
        }
    };
    let script = script(n, seen);
    sim::run_streaming(
        h.clone(),
        &mut records.iter().cloned(),
        h.read_ahead,
        &|l, a, r| script(l, a, r),
    )
    .unwrap();
    let v = samples
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    Machine {
        open: v[0],
        peak: v[1],
        idle: v[2],
    }
}

/// What a run of the shell held past its start at each of [`SAMPLED`], and
/// its peak since the instant before.
type Held = Vec<(usize, usize)>;

/// The plan run by the shell, its attempts carried by REST over the pipes, or
/// by the scripted carrier when `scripted`.
fn shell(n: u64, row: Row, scripted: bool) -> Held {
    let (h, records) = plan(n, row);
    let header = Arc::new(h);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let base = HELD[HARNESS].load(Relaxed);
    PEAK.store(base, Relaxed);
    rt.block_on(async move {
        let clock = Arc::new(VirtualClock::start(1_763_640_000_000_000));
        let runtimes = Runtimes::assumed(vec![tokio::runtime::Handle::current()], 200_000);
        let samples = Arc::new(Mutex::new(Vec::with_capacity(SAMPLED.len())));
        {
            let (samples, clock) = (samples.clone(), clock.clone());
            stand_spawn(async move {
                for t in SAMPLED {
                    clock.sleep_until(t).await;
                    let now = HELD[HARNESS].load(Relaxed);
                    let peak = PEAK.swap(now, Relaxed);
                    samples
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .push((now.saturating_sub(base), peak.saturating_sub(base)));
                }
            });
        }
        let plan = Plan::assumed(
            header.clone(),
            Peaks {
                pending: n + 4,
                sockets: vec![n + 4],
                ..Default::default()
            },
        );
        if scripted {
            let carrier = common::Scripted::new(script(n, || {}));
            go(plan, carrier, &runtimes, clock, records).await;
        } else {
            let answers = Arc::new(Answers {
                reads: answer(row, ANSWERED),
                probes: answer(SMALL, 10),
            });
            let carrier = RestCarrier::new(
                header.clone(),
                OneStand,
                Pipes {
                    clock: clock.clone(),
                    answers,
                    ring: row.head
                        + row.body
                        + 3 * (row.req.target + row.req.header + row.req.body)
                        + 8_192,
                },
                Config::new(RunSecret::new([7; 32])),
            );
            go(plan, carrier, &runtimes, clock, records).await;
        }
        samples
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    })
}

async fn go<X: Carrier<VirtualClock>>(
    plan: Plan,
    carrier: X,
    runtimes: &Runtimes,
    clock: Arc<VirtualClock>,
    records: Vec<Record>,
) {
    let admitted = admit(plan, W, Arc::new(carrier), runtimes, Journal::new(64, 0)).unwrap();
    clock
        .within(run(
            Ready::assumed(admitted, clock.clone(), Unkept, Namespace(1)),
            Records::new(records.into_iter()),
        ))
        .await
        .unwrap();
}

/// A read's share between plans of `a` and `b` reads: `(y − x) / (b − a)`.
fn per(x: usize, y: usize, a: u64, b: u64) -> u64 {
    let d = y.saturating_sub(x) as u64;
    d / (b - a)
}

/// What a read holds, beside the machine at the same state, between plans of
/// `a` and `b` reads — each just past a power of two, where a container
/// growing with them holds the most a read.
#[derive(Debug)]
struct Costs {
    /// The shell's own share: the scripted carrier's, held or at the burst
    /// of its dispatch.
    shell: u64,
    /// A connection idle in its pool.
    idle: u64,
    /// An attempt beside its connection and the shell's share, held or at
    /// its dispatch.
    attempt: u64,
    /// An answer, the most it held as it was read beside what its attempt
    /// held open: at its head, its body's first part read, at its end.
    answer: u64,
    /// Held with its body's first part read, beside what it held open.
    mid: u64,
}

/// The shell's own share a read, beside the machine at the same state: a
/// carrier that holds nothing of its own, held at its open or at its
/// dispatch's burst.
fn shell_share(row: Row, (a, b): (u64, u64)) -> u64 {
    let _ = (machine(1, row), shell(1, row, true));
    let (ma, mb) = (machine(a, row), machine(b, row));
    let (sa, sb) = (shell(a, row, true), shell(b, row, true));
    let p = |x: usize, y: usize| per(x, y, a, b);
    (p(sa[1].0, sb[1].0).saturating_sub(p(ma.open, mb.open)))
        .max(p(sa[0].1, sb[0].1).saturating_sub(p(ma.peak, mb.peak)))
}

fn costs(row: Row, (a, b): (u64, u64)) -> Costs {
    // Warmed: what a process allocates once, at its first run of each.
    let _ = (machine(1, row), shell(1, row, true), shell(1, row, false));
    let (ma, mb) = (machine(a, row), machine(b, row));
    let (ra, rb) = (shell(a, row, false), shell(b, row, false));
    let p = |x: usize, y: usize| per(x, y, a, b);
    let (m_open, m_peak, m_idle) = (
        p(ma.open, mb.open),
        p(ma.peak, mb.peak),
        p(ma.idle, mb.idle),
    );
    let held = |h: &Held, i: usize| h[i].0;
    let peak = |h: &Held, i: usize| h[i].1;
    // By `SAMPLED`'s index: 0 dispatched, 1 open, 2 heads, 3 mid, 4 done, 5 idle.
    let shell = shell_share(row, (a, b));
    let idle = p(held(&ra, 5), held(&rb, 5)).saturating_sub(m_idle);
    let open = p(held(&ra, 1), held(&rb, 1));
    let attempt = (open.saturating_sub(m_open))
        .max(p(peak(&ra, 0), peak(&rb, 0)).saturating_sub(m_peak))
        .saturating_sub(shell + idle);
    let answer = [2, 3, 4]
        .into_iter()
        .map(|i| p(peak(&ra, i), peak(&rb, i)).max(p(held(&ra, i), held(&rb, i))))
        .max()
        .unwrap_or(0)
        .saturating_sub(open);
    let mid = p(held(&ra, 3), held(&rb, 3)).saturating_sub(open);
    Costs {
        shell,
        idle,
        attempt,
        answer,
        mid,
    }
}

/// `f` of the REST carrier of `header`.
fn with_carrier<T>(header: Header, f: impl FnOnce(&RestCarrier<OneStand, Pipes>) -> T) -> T {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let _in = rt.enter();
    let carrier = RestCarrier::new(
        Arc::new(header),
        OneStand,
        Pipes {
            clock: Arc::new(VirtualClock::start(0)),
            answers: Arc::new(Answers {
                reads: vec![],
                probes: vec![],
            }),
            ring: 0,
        },
        Config::new(RunSecret::new([7; 32])),
    );
    f(&carrier)
}

/// The peaks of `row`'s plan as REST prices them: its largest record, a
/// read's.
fn row_peaks(row: Row) -> (Header, Peaks, ascetic_ddd_command_log::shape::Shape) {
    let (h, records) = plan(1, row);
    let read = ascetic_ddd_command_log::shape::shape(&records[0]);
    let peaks = Peaks {
        record: read,
        ..Peaks::default()
    };
    (h, peaks, read)
}

/// The REST carrier's stated costs for `row`'s profile.
fn stated(row: Row) -> ascetic_ddd_load_run::carrier::Costs {
    let (h, peaks, _) = row_peaks(row);
    with_carrier(h, |carrier| {
        match Carrier::<VirtualClock>::needs(carrier, &peaks, 0) {
            Ok(Needs::Sockets { costs, .. }) => costs,
            other => panic!("{other:?}"),
        }
    })
}

/// What the REST carrier states an attempt of `row`'s reads holds.
fn stated_attempt(row: Row) -> u64 {
    let (h, peaks, read) = row_peaks(row);
    with_carrier(h, |carrier| {
        Carrier::<VirtualClock>::attempts(carrier, &peaks)
            .unwrap()
            .price(&read)
    })
}

/// `measured` within `[floor · stated, stated]`: an upper bound, and not a
/// loose one.
fn within(name: &str, measured: u64, stated: u64, floor: f64) {
    eprintln!(
        "MEASURED {name}: {measured} B of {stated} B stated ({:.0} %)",
        100.0 * measured as f64 / stated as f64
    );
    assert!(
        measured <= stated,
        "{name}: {measured} B, {stated} B stated"
    );
    assert!(
        measured as f64 >= floor * stated as f64,
        "{name}: {measured} B, below {floor} of the {stated} B stated"
    );
}

/// Small answers, a small head.
const SMALL: Row = Row {
    body: 2,
    first: 2,
    head: 0,
    fields: 2,
    names: false,
    repeated: false,
    chunked: false,
    req: Req {
        target: 0,
        header: 0,
        body: 0,
    },
    captured: false,
};

/// A body of 2^16 + 1 bytes at the cap, its first 20,000 with the head.
const LARGE: Row = Row {
    body: 65_537,
    first: 20_000,
    ..SMALL
};

/// The worst head hyper parses: [`READ`] bytes in a hundred fields, its bulk
/// in their names.
const WORST: Row = Row {
    head: READ,
    fields: 100,
    names: true,
    ..SMALL
};

/// The worst head's bytes in a hundred fields of one name: a name hyper
/// keeps once, its copy each field's own (K2b-7e-2b-2c-4).
const REPEATED: Row = Row {
    repeated: true,
    ..WORST
};

/// One test measuring at a time: the allocator counts the whole process,
/// and the harness runs a binary's tests at once.
fn alone() -> std::sync::MutexGuard<'static, ()> {
    static ONE: Mutex<()> = Mutex::new(());
    ONE.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Where the plans' sizes fall: just past powers of two.
const SIZES: (u64, u64) = (257, 513);

/// The shell's own share an attempt: beside the machine at the same state,
/// a carrier that holds nothing of its own, its attempts dispatched together.
#[test]
fn the_shell_holds_its_stated_share_an_attempt() {
    let _alone = alone();
    let c = costs(SMALL, (4_097, 8_193));
    within("the shell's share an attempt", c.shell, ATTEMPT, 0.8);
}

/// A head of one name repeated: within what REST charges an answer's head.
#[test]
fn a_head_of_one_name_repeated_holds_what_rest_charges() {
    let _alone = alone();
    assert!(head_of(REPEATED) <= READ, "{} B", head_of(REPEATED));
    assert!(head_of(REPEATED) > READ - 256, "{} B", head_of(REPEATED));
    let c = costs(REPEATED, SIZES);
    within(
        "a head of one name repeated",
        c.answer,
        stated(REPEATED).answer,
        0.5,
    );
}

/// K2b-7e-2b-3a-2: a decorating plan's attempts are unstated until its
/// decorator states its decorations (K2b-7e-2b-3b); a plain one's stated by
/// the library.
#[test]
fn rest_leaves_a_decorated_attempt_unstated() {
    use ascetic_ddd_load_run::carrier::{StatedBy, Unstated};
    let (mut h, peaks, _) = row_peaks(SMALL);
    let plain = with_carrier(h.clone(), |c| Carrier::<VirtualClock>::attempts(c, &peaks));
    assert_eq!(plain.map(|a| a.stated_by), Ok(StatedBy::Library));
    h.operations[0].decorated = vec!["authorization".into()];
    let decorated = with_carrier(h, |c| Carrier::<VirtualClock>::attempts(c, &peaks));
    assert_eq!(decorated, Err(Unstated { part: "decorator" }));
}

/// What the REST carrier holds a source beside its sockets, as its needs
/// tell it: its read scratch, a head copied, one tail — the parse at the
/// cap of the plan's largest record, judged; a source it does not know,
/// refused.
#[test]
fn rest_states_its_source_costs_from_the_plan() {
    use ascetic_ddd_load_run::carrier::{SourceCosts, StatedBy};
    use ascetic_ddd_load_run::rest::{HEAD_COPY, SCRATCH};
    let (h, records) = plan(1, LARGE);
    let cap = h.profile.answer_cap;
    let problem = u64::try_from(h.profile.problem.len()).unwrap();
    let mut record = ascetic_ddd_command_log::shape::shape(&records[0]);
    record.captures.elements = 3;
    record.captures.sequences = 1;
    record.capture_bytes = 4_096;
    let peaks = Peaks {
        record,
        ..Peaks::default()
    };
    let (costs, unknown) = with_carrier(h, |carrier| {
        (
            Carrier::<VirtualClock>::source_costs(carrier, &peaks, 0),
            Carrier::<VirtualClock>::source_costs(carrier, &peaks, 1),
        )
    });
    assert_eq!(
        costs,
        Ok(SourceCosts {
            scratch: SCRATCH,
            head: HEAD_COPY,
            tail: ascetic_ddd_scheduler::judge::bound(cap, &peaks.record, problem),
            stated_by: StatedBy::Library,
        })
    );
    assert!(
        costs.unwrap().tail > ascetic_ddd_scheduler::judge::bound(cap, &Default::default(), 0),
        "the plan's record and the problem charged"
    );
    assert!(unknown.is_err(), "{unknown:?}");
}

/// K2b-7e-2b-3a-3: the shell's share of an attempt whose hole a writer's
/// captured value of `VALUE` bytes fills — its own and its copy of the value
/// — as `run::attempt_share` prices it.
#[test]
fn the_shell_holds_its_stated_share_of_a_captured_hole() {
    let _alone = alone();
    let row = Row {
        captured: true,
        ..SMALL
    };
    let share = shell_share(row, (257, 513));
    let (_, records) = plan(1, row);
    let read = ascetic_ddd_command_log::shape::shape(&records[1]);
    assert_eq!(read.holes, 1, "the read's hole");
    within(
        "the shell's share of an attempt with a captured hole",
        share,
        ascetic_ddd_load_run::run::attempt_share(&read),
        0.9,
    );
}

/// A connection idle in its pool, an attempt open beside it: as the REST
/// carrier states them.
#[test]
fn a_connection_and_an_attempt_hold_what_rest_states() {
    let _alone = alone();
    let c = costs(SMALL, SIZES);
    let s = stated(SMALL);
    within("a connection idle", c.idle, s.connection, 0.8);
    within(
        "an attempt beside its connection",
        c.attempt,
        stated_attempt(SMALL),
        0.8,
    );
    within("a small answer", c.answer, s.answer, 0.0);
}

/// K2b-7e-2b-3a-2: a connection idle after writing a head just past 16 KiB —
/// a target of spaces, each encoded `%20` — holds hyper's head buffer, never
/// shrunk, within what REST charges a connection at the plan's largest head;
/// its attempts, a target and a header and a body at once, within what REST
/// states an attempt holds.
#[test]
fn a_large_head_s_buffer_and_its_attempt_hold_what_rest_states() {
    let _alone = alone();
    let small = costs(SMALL, SIZES);
    let row = Row {
        req: Req {
            target: 5_434,
            header: 0,
            body: 0,
        },
        ..SMALL
    };
    let c = costs(row, SIZES);
    // hyper's buffer grows to between the head and twice it — exactly to a
    // head past twice its capacity, doubled past one within it: the bound is
    // twice, reached only by the second.
    within(
        "a connection after a head of 16 KiB",
        c.idle,
        stated(row).connection,
        0.8,
    );
    assert!(
        c.idle > small.idle + 16_384,
        "the head buffer not seen: {} B against {} B",
        c.idle,
        small.idle
    );
    let row = Row {
        req: Req {
            target: 4_097,
            header: 4_097,
            body: 16_385,
        },
        ..SMALL
    };
    let c = costs(row, SIZES);
    // One-sided where a request's copies are written before the next read
    // runs, as here: `tests/frame.rs` holds each class's coefficient two-sided.
    within(
        "an attempt with a target, a header and a body",
        c.attempt,
        stated_attempt(row),
        0.5,
    );
}

/// An answer's body sized at once, never grown: its length's or, of no
/// length, the cap's — held whole while its first part is read, nothing of
/// its head pinned beside it; at most what REST charges an answer.
#[test]
fn a_body_is_allocated_whole_and_charged_at_the_cap() {
    let _alone = alone();
    for row in [
        LARGE,
        Row {
            chunked: true,
            ..LARGE
        },
    ] {
        let c = costs(row, SIZES);
        let s = stated(row);
        let body = row.body as u64;
        eprintln!("MEASURED {row:?}: {c:?}");
        assert!(
            c.mid.abs_diff(body) <= 1_024,
            "{row:?}: {} B held with its first part read, its body {body} B",
            c.mid
        );
        within(&format!("{row:?}'s answer"), c.answer, s.answer, 0.0);
    }
}

/// The worst head: within what REST charges an answer's head, and not far
/// below it; with a large body after it, within the charge too.
#[test]
fn the_worst_head_holds_what_rest_charges() {
    let _alone = alone();
    let c = costs(WORST, SIZES);
    let s = stated(WORST);
    within("the worst head's answer", c.answer, s.answer, 0.85);
    let row = Row {
        body: LARGE.body,
        first: LARGE.first,
        ..WORST
    };
    assert_eq!(head_of(row), READ);
    let c = costs(row, SIZES);
    within(
        "the worst head and a large body",
        c.answer,
        stated(row).answer,
        0.0,
    );
}

/// K2b-7e-2b-2: a replay's reader holds what its `Reader` states — its
/// reading, the records its channel holds ahead and the two beside it (the
/// one its thread waits to send, the one the run took), each at the plan's
/// largest, the channel's blocks, its thread — with the channel full and its
/// thread waiting: an upper bound, not a loose one.
#[test]
fn a_replay_s_reader_holds_what_it_states() {
    use ascetic_ddd_command_log::disk::LocalDisk;
    use ascetic_ddd_command_log::live::{Commitment, commit_file};
    use ascetic_ddd_command_log::record::{Action, Codec, HostCall, Payload};
    use ascetic_ddd_command_log::validate::NoHost;
    use ascetic_ddd_load_run::source::{LineSource, Replayed};
    let _alone = alone();
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("plan.jsonl");
    let ahead = 64;
    let records = (0..(ahead as u64 + 8)).map(|i| {
        let mut r = step(id("device", i, "call", 0), i * 1_000);
        r.action = Action::Host(HostCall {
            op: "noop".into(),
            args: Payload {
                codec: Codec::Text,
                bytes: vec![b'x'; 12_000],
                holes: vec![],
            },
        });
        r
    });
    ascetic_ddd_command_log::write_file(
        &log,
        header(),
        records,
        ascetic_ddd_command_log::Compression::None,
    )
    .unwrap();
    let path = dir.path().join("plan.commitment");
    let report = commit_file(
        &log,
        &NoHost,
        &limits_in(dir.path()),
        "build",
        16,
        &path,
        &LocalDisk,
    )
    .unwrap();
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    let commitment = Commitment::read(&path).unwrap();
    let base = HELD[HARNESS].load(Relaxed);
    PEAK.store(base, Relaxed);
    let (plan, mut replayed) = Replayed::open(&log, &commitment, ahead).unwrap();
    // The run takes one; the channel fills again; its thread decodes the
    // next and waits to send it — the heap still once it does.
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let taken = loop {
        if let Poll::Ready(r) = replayed.poll_next(&mut cx) {
            break r.unwrap().unwrap();
        }
        std::thread::yield_now();
    };
    // Bounded, in rounds (the shell's one clock is `Clock`): a channel that
    // never holds exactly `ahead` — wider than its reader's — is refused, not
    // waited on.
    let still = || {
        for _ in 0..10_000 {
            let before = HELD[HARNESS].load(Relaxed);
            for _ in 0..20_000 {
                std::thread::yield_now();
            }
            if replayed.queued() == ahead && HELD[HARNESS].load(Relaxed) == before {
                return;
            }
        }
        panic!(
            "the channel holds {} records, its reader {ahead}",
            replayed.queued()
        );
    };
    still();
    let peak = (PEAK.load(Relaxed) - base) as u64;
    let reader = *plan.reader().unwrap();
    assert_eq!(reader.ahead, ahead);
    // The plan's own beside it: its digest, its validator, its peaks' sockets.
    let own = (plan.log().len() + plan.validator().len() + 8 * plan.peaks().sockets.len()) as u64;
    // No string of the log escaped: serde's scratch, the line's bound past
    // its copy, is never taken — so the rest is resolved to a record or two.
    assert!(!std::fs::read(&log).unwrap().contains(&b'\\'));
    let scratch = 2 * commitment.line;
    let stated = reader.bytes(&plan.peaks().record) + own - scratch;
    within("a replay's reader, its channel full", peak, stated, 0.95);
    drop(taken);
}
