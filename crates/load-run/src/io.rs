//! A connection's bytes, counted (ADR-0047 decision 16).
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
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, ready};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

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

/// A connection's bytes for hyper, counted by its probe.
pub(crate) struct Io<T> {
    inner: T,
    probe: Arc<Probe>,
    scratch: Vec<u8>,
}

/// The most a read copies at once.
const SCRATCH: usize = 16 * 1024;

impl<T> Io<T> {
    pub(crate) fn new(inner: T, probe: Arc<Probe>) -> Io<T> {
        Io {
            inner,
            probe,
            scratch: vec![0; SCRATCH],
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
        let room = buf.remaining().min(this.scratch.len());
        let Some(scratch) = this.scratch.get_mut(..room) else {
            return Poll::Ready(Ok(()));
        };
        let mut read = ReadBuf::new(scratch);
        ready!(Pin::new(&mut this.inner).poll_read(cx, &mut read))?;
        buf.put_slice(read.filled());
        Poll::Ready(Ok(()))
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
            let mut io = Io::new(client, Arc::new(Probe::default()));
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
        let mut io = Io::new(client, probe.clone());
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
}
