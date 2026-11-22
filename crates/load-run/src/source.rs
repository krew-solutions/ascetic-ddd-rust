//! The log's lines, as the machine reads them (ADR-0047 decision 16): a port,
//! with an adapter in the machine's task for tests and one on a thread of its
//! own for a run, whose reads block on the file.

use std::path::Path;
use std::sync::Arc;
use std::task::{Context, Poll};

use ascetic_ddd_command_log::LogError;
use ascetic_ddd_command_log::live::{self, Commitment};
use ascetic_ddd_command_log::record::{Header, Record};
use tokio::sync::mpsc;

/// The log's records in order, then its verified end. Polled by the machine's
/// loop; a record not ready is waited for while everything else goes on.
pub trait LineSource: Send {
    /// The next record; `None` at the log's verified end.
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Arc<Record>>, LogError>>;
}

/// Records already in memory, each ready at once: a test's, in the
/// machine's task.
pub struct Records<I> {
    records: I,
}

impl<I> Records<I> {
    /// Over `records`.
    pub fn new(records: I) -> Records<I> {
        Records { records }
    }
}

impl<I: Iterator<Item = Record> + Send> LineSource for Records<I> {
    fn poll_next(&mut self, _: &mut Context<'_>) -> Poll<Result<Option<Arc<Record>>, LogError>> {
        Poll::Ready(Ok(self.records.next().map(Arc::new)))
    }
}

/// A persisted log read through its commitment on a thread of its own, at most
/// `ahead` records before the machine.
pub struct Replayed {
    rx: mpsc::Receiver<Result<Record, LogError>>,
    done: bool,
}

impl Replayed {
    /// Opens `path` through `commitment` — refused at once if the commitment
    /// is of another format or validator — and starts its reading thread.
    pub fn open(
        path: &Path,
        commitment: &Commitment,
        ahead: usize,
    ) -> Result<(Header, Replayed), LogError> {
        let reader = live::replay(path, commitment)?;
        let header = reader.header().clone();
        let (tx, rx) = mpsc::channel(ahead.max(1));
        std::thread::Builder::new()
            .name("load-run-lines".to_owned())
            .spawn(move || {
                for record in reader {
                    let failed = record.is_err();
                    // The machine gone, nothing more is read.
                    if tx.blocking_send(record).is_err() || failed {
                        return;
                    }
                }
            })?;
        Ok((header, Replayed { rx, done: false }))
    }
}

impl LineSource for Replayed {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Arc<Record>>, LogError>> {
        if self.done {
            return Poll::Ready(Ok(None));
        }
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(Ok(r))) => Poll::Ready(Ok(Some(Arc::new(r)))),
            Poll::Ready(Some(Err(e))) => {
                self.done = true;
                Poll::Ready(Err(e))
            }
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(Ok(None))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
