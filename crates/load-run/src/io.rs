//! A connection's bytes, counted (ADR-0047 decision 16), and its socket's
//! place under its source's cap (decision 19).
//!
//! hyper encodes a request's head and its body — one frame, ready at once —
//! into its buffer, and flushes the socket only once that buffer is empty
//! (hyper 1.11.1, `proto/h1/io.rs`, `proto/h1/dispatch.rs`); the attempt arms
//! the probe and hands the request over in one poll, on the connection's own
//! runtime, so no flush comes between. The first flush of the socket that
//! succeeds after the request is armed is therefore the instant its last byte
//! left: `Sent` (the REST twins hold hyper to it: `Sent` is the instant the
//! stub saw the last byte, writes held back and cut short) — once a byte of
//! it was accepted: hyper flushes an empty buffer too, on a connection the
//! stand closed while it sat idle, before it hands the request back. The first byte
//! accepted is the commit point: before it, a request handed back is no
//! attempt's end and goes on another connection; after it, an error is the
//! system's Unknown. Reads fill hyper's buffer by copy, never by an
//! uninitialised slice (the crate forbids `unsafe`).

use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, ready};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::Notify;
use tokio::sync::futures::Notified;

/// A source's sockets under its cap (decision 19): each counted from before
/// its creation to its `Io`'s drop — a reset connection's socket until the
/// runtime drops its aborted task.
pub(crate) struct Sockets {
    cap: usize,
    open: AtomicUsize,
    /// Of them, their connection gone, their `Io` not yet dropped: freed
    /// soon.
    dying: AtomicUsize,
    /// A place freed or a connection idled: one waiter woken, first come
    /// first.
    freed: Notify,
    /// Since when its waiters have got no connection (decision 19); none
    /// waiting since the last that did, `u64::MAX`.
    starved: AtomicU64,
}

/// Where a socket's place stands.
const HELD: u8 = 0;
const DYING: u8 = 1;
const GONE: u8 = 2;

impl Sockets {
    /// At most `cap` sockets at once.
    pub(crate) fn new(cap: usize) -> Arc<Sockets> {
        Arc::new(Sockets {
            cap,
            open: AtomicUsize::new(0),
            dying: AtomicUsize::new(0),
            freed: Notify::new(),
            starved: AtomicU64::new(u64::MAX),
        })
    }

    /// A connection idled in its source's pool: a waiter woken to take it
    /// (decision 19).
    pub(crate) fn idled(&self) {
        self.freed.notify_one();
    }

    /// A waiter at the cap, now: since when the source's waiters have got
    /// none — its own wait the first, if none was starving.
    pub(crate) fn starving(&self, now: u64) -> u64 {
        match self
            .starved
            .compare_exchange(u64::MAX, now, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => now,
            Err(since) => since,
        }
    }

    /// An attempt got a connection: the source's waiters starve no more.
    pub(crate) fn served(&self) {
        self.starved.store(u64::MAX, Ordering::SeqCst);
    }

    /// A place for a new socket, if under the cap.
    pub(crate) fn take(self: &Arc<Self>) -> Option<Slot> {
        self.open
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                (n < self.cap).then_some(n.saturating_add(1))
            })
            .ok()?;
        Some(Slot {
            sockets: self.clone(),
            state: Arc::new(AtomicU8::new(HELD)),
        })
    }

    /// Sockets open.
    pub(crate) fn open(&self) -> usize {
        self.open.load(Ordering::SeqCst)
    }

    /// Of them, dying.
    pub(crate) fn dying(&self) -> usize {
        self.dying.load(Ordering::SeqCst)
    }

    /// The cap.
    pub(crate) fn cap(&self) -> usize {
        self.cap
    }

    /// Ready once a place is freed or a connection idled: enabled before the
    /// count is read, it misses none after.
    pub(crate) fn freed(&self) -> Notified<'_> {
        self.freed.notified()
    }
}

/// A socket's place, held by its `Io`: freed when dropped — the `Io`'s drop,
/// or its connect's failure.
pub(crate) struct Slot {
    sockets: Arc<Sockets>,
    state: Arc<AtomicU8>,
}

impl Slot {
    /// Its connection's side: dropped while the socket is open, the socket
    /// is dying.
    pub(crate) fn held(&self) -> Held {
        Held {
            sockets: self.sockets.clone(),
            state: self.state.clone(),
        }
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        if self.state.swap(GONE, Ordering::SeqCst) == DYING {
            let _ = self
                .sockets
                .dying
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                    Some(n.saturating_sub(1))
                });
        }
        let _ = self
            .sockets
            .open
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                Some(n.saturating_sub(1))
            });
        self.sockets.freed.notify_one();
    }
}

/// A connection's hold on its socket's place.
pub(crate) struct Held {
    sockets: Arc<Sockets>,
    state: Arc<AtomicU8>,
}

impl Drop for Held {
    fn drop(&mut self) {
        // Counted first: a slot dropped between the two sees it dying, or
        // gone before it, and the count comes back.
        self.sockets.dying.fetch_add(1, Ordering::SeqCst);
        if self
            .state
            .compare_exchange(HELD, DYING, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            let _ = self
                .sockets
                .dying
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                    Some(n.saturating_sub(1))
                });
        }
    }
}

/// What reports a request's last byte left.
pub(crate) type Hook = Box<dyn FnOnce() + Send>;

/// Whether an armed request's bytes may still leave.
pub(crate) type Sending = Box<dyn Fn() -> bool + Send>;

#[derive(Default)]
struct Armed {
    /// Bytes of the armed request accepted.
    written: u64,
    hook: Option<Hook>,
    /// Until its last byte left: whether another may (decision 19).
    sending: Option<Sending>,
    /// Its send path, fed by the connection's task (decision 18).
    path: Option<Arc<crate::stamps::Path>>,
}

/// One connection's request in flight, as its bytes go.
#[derive(Default)]
pub(crate) struct Probe {
    armed: Mutex<Armed>,
}

impl Probe {
    fn lock(&self) -> MutexGuard<'_, Armed> {
        self.armed.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A request about to be handed to hyper: none of its bytes accepted
    /// yet, `hook` called once its last is.
    pub(crate) fn arm(&self, hook: Hook, path: Arc<crate::stamps::Path>, sending: Sending) {
        *self.lock() = Armed {
            written: 0,
            hook: Some(hook),
            sending: Some(sending),
            path: Some(path),
        };
    }

    /// No byte of the armed request may leave (decision 19): its attempt's
    /// deadline reached before its last one did.
    fn refused(&self) -> bool {
        let a = self.lock();
        a.hook.is_some() && a.sending.as_ref().is_some_and(|may| !may())
    }

    /// The send path of the request it carries, if any.
    pub(crate) fn path(&self) -> Option<Arc<crate::stamps::Path>> {
        self.lock().path.clone()
    }

    /// Bytes of the armed request accepted.
    pub(crate) fn written(&self) -> u64 {
        self.lock().written
    }

    fn wrote(&self, n: usize) {
        let mut a = self.lock();
        a.written = a
            .written
            .saturating_add(u64::try_from(n).unwrap_or(u64::MAX));
    }

    fn flushed(&self) {
        // hyper flushes an empty buffer too — a connection the stand closed
        // while it sat idle, before it hands the request back (decision 18):
        // only a flush after a byte of the armed request was accepted is its
        // last byte's.
        let hook = {
            let mut a = self.lock();
            if a.written == 0 { None } else { a.hook.take() }
        };
        if let Some(hook) = hook {
            hook();
        }
    }
}

/// A connection's bytes for hyper, counted by its probe; its socket's place
/// freed when it drops.
pub(crate) struct Io<T> {
    inner: T,
    probe: Arc<Probe>,
    _slot: Slot,
}

/// The most a read copies at once.
pub(crate) const SCRATCH: usize = 16 * 1024;

std::thread_local! {
    /// The read's scratch, one a thread, not one a connection (ADR-0047
    /// decision 22): it is used only inside one synchronous read.
    static READ: std::cell::RefCell<Box<[u8]>> =
        std::cell::RefCell::new(vec![0; SCRATCH].into_boxed_slice());
}

impl<T> Io<T> {
    pub(crate) fn new(inner: T, probe: Arc<Probe>, slot: Slot) -> Io<T> {
        Io {
            inner,
            probe,
            _slot: slot,
        }
    }
}

impl<T: AsyncRead + Unpin> hyper::rt::Read for Io<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        READ.with_borrow_mut(|scratch| {
            let room = buf.remaining().min(scratch.len());
            let Some(scratch) = scratch.get_mut(..room) else {
                return Poll::Ready(Ok(()));
            };
            let mut read = ReadBuf::new(scratch);
            ready!(Pin::new(&mut this.inner).poll_read(cx, &mut read))?;
            buf.put_slice(read.filled());
            Poll::Ready(Ok(()))
        })
    }
}

impl<T: AsyncWrite + Unpin> hyper::rt::Write for Io<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        // Held, unwoken, until its attempt resets the connection at its last
        // look (decision 19): a request's byte never leaves past its
        // deadline.
        if this.probe.refused() {
            return Poll::Pending;
        }
        let n = ready!(Pin::new(&mut this.inner).poll_write(cx, buf))?;
        this.probe.wrote(n);
        Poll::Ready(Ok(n))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.probe.refused() {
            return Poll::Pending;
        }
        let n = ready!(Pin::new(&mut this.inner).poll_write_vectored(cx, bufs))?;
        this.probe.wrote(n);
        Poll::Ready(Ok(n))
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(Pin::new(&mut this.inner).poll_flush(cx))?;
        this.probe.flushed();
        Poll::Ready(Ok(()))
    }

    /// Not forwarded (decision 19): hyper's graceful close would send a FIN
    /// first, and the peer's FIN after it leaves a TIME_WAIT on the source's
    /// port that the reset of the socket's drop, at zero linger, cannot undo.
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Arc;

    use tokio::io::AsyncReadExt;

    use super::{Io, Probe};

    /// hyper's graceful close reaches no socket (decision 19): its peer
    /// reads no end — the socket's drop, at zero linger, its only close.
    #[test]
    fn a_shutdown_is_not_forwarded() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let (client, mut peer) = tokio::io::duplex(64);
            let slot = super::Sockets::new(1).take().unwrap();
            let mut io = Io::new(client, Arc::new(Probe::default()), slot);
            std::future::poll_fn(|cx| hyper::rt::Write::poll_shutdown(Pin::new(&mut io), cx))
                .await
                .unwrap();
            let mut buf = [0u8; 1];
            tokio::select! {
                biased;
                r = peer.read(&mut buf) => panic!("the peer read {r:?}"),
                () = std::future::ready(()) => {}
            }
            // Dropped, it closes.
            drop(io);
            assert_eq!(peer.read(&mut buf).await.unwrap(), 0);
        });
    }

    /// An armed request's bytes refused once its attempt may send no more
    /// (decision 19) — plain and vectored writes alike, held unwoken; once
    /// its last byte left, nothing of the connection's is refused.
    #[test]
    fn an_armed_request_s_bytes_are_refused_past_its_deadline() {
        use std::io::IoSlice;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::task::{Context, Poll, Waker};

        use hyper::rt::Write;

        let (client, _peer) = tokio::io::duplex(64);
        let probe = Arc::new(Probe::default());
        let slot = super::Sockets::new(1).take().unwrap();
        let mut io = Io::new(client, probe.clone(), slot);
        let may = Arc::new(AtomicBool::new(true));
        let m = may.clone();
        probe.arm(
            Box::new(|| {}),
            Arc::new(crate::stamps::Path::default()),
            Box::new(move || m.load(Ordering::SeqCst)),
        );
        let mut cx = Context::from_waker(Waker::noop());
        let mut io = Pin::new(&mut io);
        assert!(matches!(
            io.as_mut().poll_write(&mut cx, b"ab"),
            Poll::Ready(Ok(2))
        ));
        may.store(false, Ordering::SeqCst);
        assert!(io.as_mut().poll_write(&mut cx, b"cd").is_pending());
        let bufs = [IoSlice::new(b"cd")];
        assert!(io.as_mut().poll_write_vectored(&mut cx, &bufs).is_pending());
        assert_eq!(probe.written(), 2);
        // Its last byte left (flushed after one was accepted): not refused.
        may.store(true, Ordering::SeqCst);
        assert!(matches!(
            io.as_mut().poll_flush(&mut cx),
            Poll::Ready(Ok(()))
        ));
        may.store(false, Ordering::SeqCst);
        assert!(matches!(
            io.as_mut().poll_write(&mut cx, b"ef"),
            Poll::Ready(Ok(2))
        ));
        assert!(matches!(
            io.as_mut().poll_write_vectored(&mut cx, &bufs),
            Poll::Ready(Ok(2))
        ));
    }

    /// A source's sockets (decision 19): no more than its cap at once; a
    /// connection gone before its socket, the socket dying until it drops;
    /// a waiter enabled before the count woken when a place is freed.
    #[test]
    fn a_source_s_sockets_under_its_cap() {
        use super::Sockets;
        let sockets = Sockets::new(2);
        let a = sockets.take().unwrap();
        let b = sockets.take().unwrap();
        assert!(sockets.take().is_none());
        assert_eq!((sockets.open(), sockets.dying(), sockets.cap()), (2, 0, 2));
        // Its connection dropped, its socket not yet: dying.
        drop(a.held());
        assert_eq!(sockets.dying(), 1);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut freed = std::pin::pin!(sockets.freed());
            freed.as_mut().enable();
            drop(a);
            freed.await;
        });
        assert_eq!((sockets.open(), sockets.dying()), (1, 0));
        // Its socket gone first: never dying.
        let held = b.held();
        drop(b);
        drop(held);
        assert_eq!((sockets.open(), sockets.dying()), (0, 0));
        assert!(sockets.take().is_some());
    }
}
