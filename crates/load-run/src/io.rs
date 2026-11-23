//! A connection's bytes, counted (ADR-0047 decision 16).
//!
//! hyper encodes a request's head and its body — one frame, ready at once —
//! into its buffer, and flushes the socket only once that buffer is empty
//! (hyper 1.11.1, `proto/h1/io.rs`, `proto/h1/dispatch.rs`); the attempt arms
//! the probe and hands the request over in one poll, on the connection's own
//! runtime, so no flush comes between. The first flush of the socket that
//! succeeds after the request is armed is therefore the instant its last byte
//! left: `Sent` (the REST twins hold hyper to it: `Sent` is the instant the
//! stub saw the last byte, writes held back and cut short). The first byte
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

#[derive(Default)]
struct Armed {
    /// Bytes of the armed request accepted.
    written: u64,
    hook: Option<Hook>,
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
    pub(crate) fn arm(&self, hook: Hook) {
        *self.lock() = Armed {
            written: 0,
            hook: Some(hook),
        };
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
        let hook = self.lock().hook.take();
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

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
