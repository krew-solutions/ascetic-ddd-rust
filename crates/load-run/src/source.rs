//! The log's lines, as the machine reads them (ADR-0047 decision 16): a port,
//! with an adapter in the machine's task for tests and one on a thread of its
//! own for a run, whose reads block on the file.

use std::path::Path;
use std::sync::Arc;
use std::task::{Context, Poll};

use std::mem::size_of;

use ascetic_ddd_command_log::LogError;
use ascetic_ddd_command_log::live::{self, Commitment, Reading};
use ascetic_ddd_command_log::record::{Header, Record};
use ascetic_ddd_command_log::shape::{Shape, bound};
use ascetic_ddd_command_log::validate::Peaks;
use tokio::sync::mpsc;

/// The plan a run admits (ADR-0047 decision 20): its log's header and the
/// validator's peaks, from one commitment — never a header beside another's
/// peaks.
#[derive(Clone, Debug)]
pub struct Plan {
    header: Arc<Header>,
    peaks: Peaks,
    /// The log's trailer's SHA-256, as its commitment holds it.
    log: String,
    /// The validator that committed it.
    validator: String,
    /// Made by a test, not read from a commitment.
    assumed: bool,
    /// The reader that read it, as it was built: none for a test's.
    reader: Option<Reader>,
}

/// A replay's reader as [`Replayed::open`] built it (ADR-0047 decision 22,
/// K2b-7e-2b-2): what it holds by construction, and how many records its
/// channel holds ahead of the machine — the count admission charges, taken
/// from what built the channel, never stated beside it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reader {
    /// Records the channel holds.
    pub ahead: usize,
    /// Its input, its verifier, the line it decodes, the header.
    pub reading: Reading,
}

/// A channel's block of tokio's: `BLOCK_CAP` messages and four words.
const BLOCK: usize = 32;

/// The blocks a channel of `ahead` holds at most: those its messages span
/// across a block's edge, one read and not yet handed back, and the three a
/// receiver hands back for reuse past the tail (tokio's `reclaim_block` walks
/// at most three).
fn blocks(ahead: usize) -> usize {
    ahead.div_ceil(BLOCK).saturating_add(5)
}

/// What the reading thread sends: a record in the `Arc` the machine holds it
/// in, so a channel's slot is a pointer's, not a record's.
type Message = Result<Arc<Record>, LogError>;

/// The channel's own state and the reading thread's: its handle, its packet,
/// its name, its closure boxed with the log's reader in it.
pub const THREAD: u64 = 1_024;

impl Reader {
    /// What it holds at most with the plan's largest record `record`: its
    /// reading; the records its channel holds, the one its thread decodes or
    /// waits to send, the one the run took and has not read yet — each in its
    /// `Arc` at the largest record; the channel's blocks; its thread.
    pub fn bytes(&self, record: &Shape) -> u64 {
        self.priced(record).bytes().unwrap_or(u64::MAX)
    }

    /// The same by its count (ADR-0047 decision 22, K2b-7e-2b-3d-3a): its
    /// records ahead and the two beside them, each at the largest record's
    /// bound; its reading, its channel's blocks and its thread beside.
    pub fn priced(&self, record: &Shape) -> ascetic_ddd_scheduler::fits::Priced {
        let records = u64::try_from(self.ahead.saturating_add(2)).unwrap_or(u64::MAX);
        let block = BLOCK
            .saturating_mul(size_of::<Message>())
            .saturating_add(4 * size_of::<usize>());
        let channel = blocks(self.ahead).saturating_mul(block);
        let thread = THREAD.saturating_add(size_of::<live::Replay>() as u64);
        ascetic_ddd_scheduler::fits::Priced {
            count: records,
            cost: bound(record),
            rest: [
                self.reading.heap(),
                u64::try_from(channel).unwrap_or(u64::MAX),
                thread,
            ]
            .into_iter()
            .fold(0, u64::saturating_add),
        }
    }
}

impl Plan {
    /// Its header.
    pub fn header(&self) -> &Arc<Header> {
        &self.header
    }

    /// What a run of it must hold.
    pub fn peaks(&self) -> &Peaks {
        &self.peaks
    }

    /// Its log's digest, as its commitment holds it.
    pub fn log(&self) -> &str {
        &self.log
    }

    /// The validator that committed it.
    pub fn validator(&self) -> &str {
        &self.validator
    }

    /// Whether a test made it.
    pub fn is_assumed(&self) -> bool {
        self.assumed
    }

    /// The reader that read it; none for a test's.
    pub fn reader(&self) -> Option<&Reader> {
        self.reader.as_ref()
    }

    /// A test's plan: `header`, its peaks assumed, its log a test's.
    #[cfg(feature = "testing")]
    pub fn assumed(header: Arc<Header>, peaks: Peaks) -> Plan {
        Plan {
            header,
            peaks,
            log: "ab".repeat(32),
            validator: live::validator(),
            assumed: true,
            reader: None,
        }
    }
}

/// The log's records in order, then its verified end. Polled by the machine's
/// loop; a record not ready is waited for while everything else goes on.
pub trait LineSource: Send {
    /// The next record; `None` at the log's verified end.
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Arc<Record>>, LogError>>;

    /// The records it holds ahead of the machine, as it was built; none if it
    /// holds them all (a test's, in memory).
    fn ahead(&self) -> Option<usize> {
        None
    }
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
    rx: mpsc::Receiver<Message>,
    done: bool,
    ahead: usize,
}

impl Replayed {
    /// Opens `path` through `commitment` — refused at once if the commitment
    /// is of another format or validator — and starts its reading thread:
    /// the plan, its header and the commitment's peaks, with it.
    pub fn open(
        path: &Path,
        commitment: &Commitment,
        ahead: usize,
    ) -> Result<(Plan, Replayed), LogError> {
        let (reader, reading) = live::replay_priced(path, commitment)?;
        let ahead = ahead.max(1);
        let plan = Plan {
            header: reader.shared_header(),
            peaks: commitment.peaks.clone(),
            log: commitment.sha256.clone(),
            validator: commitment.validator.clone(),
            assumed: false,
            reader: Some(Reader { ahead, reading }),
        };
        let (tx, rx) = mpsc::channel(ahead);
        std::thread::Builder::new()
            .name("load-run-lines".to_owned())
            .spawn(move || {
                for record in reader {
                    let record = record.map(Arc::new);
                    let failed = record.is_err();
                    // The machine gone, nothing more is read.
                    if tx.blocking_send(record).is_err() || failed {
                        return;
                    }
                }
            })?;
        Ok((
            plan,
            Replayed {
                rx,
                done: false,
                ahead,
            },
        ))
    }
}

impl Replayed {
    /// The records its channel holds now: a test's, to wait for it full.
    #[cfg(feature = "testing")]
    pub fn queued(&self) -> usize {
        self.rx.len()
    }
}

impl LineSource for Replayed {
    fn ahead(&self) -> Option<usize> {
        Some(self.ahead)
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Arc<Record>>, LogError>> {
        if self.done {
            return Poll::Ready(Ok(None));
        }
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(Ok(r))) => Poll::Ready(Ok(Some(r))),
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
