//! Where the journal goes (ADR-0047 decision 16): lines in the machine's task
//! for tests, and for a run a thread of its own writing to the host's output,
//! whose writes block on the file. A run's channel is built only from what
//! admission holds (decision 22, K2b-7e-2b-2b): the host gives the output,
//! the library the rest.

use std::io::Write;
use std::sync::mpsc;
use std::thread::JoinHandle;

use ascetic_ddd_command_log::frame::FrameError;
use ascetic_ddd_command_log::shape::Shape;
use ascetic_ddd_scheduler::journal::{self, Header, JournalError, Line, Trailer, Writer};

fn io(e: std::io::Error) -> JournalError {
    JournalError::Frame(FrameError::Io(e))
}

/// The journal's lines after its header, then its trailer.
pub trait JournalSink: Send {
    /// What a finished journal leaves.
    type Finished;

    /// One line.
    fn line(&mut self, line: Line) -> Result<(), JournalError>;

    /// The trailer written: what the journal left.
    fn finish(self) -> Result<Self::Finished, JournalError>;
}

/// The bytes a journal's output may hold inline: the writer's thread holds
/// it twice — in the closure it runs and in what it hands back —, so
/// [`THREAD`] states them at this size; a larger output is refused when a
/// run starts, and boxed by its host.
pub const OUT: usize = 256;

/// The bytes the journal's thread and channel hold of their own, an output of
/// [`OUT`] bytes inline: the thread's handle and name, the closure it runs and
/// the result it hands back, the channel's counter and its waiters — pinned
/// by `tests/channel.rs` (at most 1,727 B measured, its channel full and its
/// writer stalled, at 1 to 128 lines alike; 72 B less when the writer never
/// waited on an empty channel).
pub const THREAD: u64 = 2_048;

/// The journal a run is admitted with (ADR-0047 decision 22, K2b-7e-2b-2b):
/// the lines its channel holds behind the machine, at least one, and what its
/// output holds on the heap, as its host states it. Admission holds it, and a
/// run's journal is built from it alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Journal {
    behind: usize,
    out: u64,
}

impl Journal {
    /// `behind` lines behind the machine, at least one; an output holding
    /// `out` bytes.
    pub fn new(behind: usize, out: u64) -> Journal {
        Journal {
            behind: behind.max(1),
            out,
        }
    }

    /// The lines its channel holds.
    pub fn behind(&self) -> usize {
        self.behind
    }

    /// What its output holds, as its host states it.
    pub fn out(&self) -> u64 {
        self.out
    }

    /// The bytes it holds at most, its lines about records of at most
    /// `record`'s shape — the plan's largest: the channel's slots, allocated
    /// with it; `behind + 2` lines at their bound — the channel's, the one
    /// the machine waits to send, the one the thread writes —; the writer's
    /// buffer; the thread and the channel's own; the output's.
    pub fn bytes(&self, record: &Shape) -> u64 {
        self.priced(record)
            .bytes()
            .unwrap_or(u64::MAX)
            .saturating_add(self.out)
    }

    /// The library's of the same by its count, the output's apart — its
    /// host's (ADR-0047 decision 22, K2b-7e-2b-3d-3a): `behind + 2` lines at
    /// their bound; the channel's slots, its writer's buffer and its thread
    /// beside.
    pub fn priced(&self, record: &Shape) -> ascetic_ddd_scheduler::fits::Priced {
        let of = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
        let behind = of(self.behind);
        let slot = of(size_of::<Message>()).saturating_add(of(size_of::<usize>()));
        ascetic_ddd_scheduler::fits::Priced {
            count: behind.saturating_add(2),
            cost: journal::line_bound(record),
            rest: behind
                .saturating_mul(slot)
                .saturating_add(of(journal::BUFFER))
                .saturating_add(THREAD),
        }
    }
}

/// Lines kept in memory: a test's, in the machine's task.
#[cfg(feature = "testing")]
#[derive(Debug, Default)]
pub struct Lines {
    lines: Vec<Line>,
}

#[cfg(feature = "testing")]
impl Lines {
    /// None yet.
    pub fn new() -> Lines {
        Lines::default()
    }
}

#[cfg(feature = "testing")]
impl JournalSink for Lines {
    type Finished = Vec<Line>;

    fn line(&mut self, line: Line) -> Result<(), JournalError> {
        self.lines.push(line);
        Ok(())
    }

    fn finish(self) -> Result<Vec<Line>, JournalError> {
        Ok(self.lines)
    }
}

/// What the machine hands its journal's thread.
#[expect(
    clippy::large_enum_variant,
    reason = "every message but a run's last is a line: boxing would allocate each to shrink the one `Finish`"
)]
enum Message {
    Line(Line),
    /// The journal whole: its trailer, and only then (decision 18).
    Finish,
}

/// The journal written to `W` on a thread of its own, at most its admitted
/// `behind` lines after the machine: a full channel blocks the machine, whose
/// lateness then shows. Its trailer is written only when it is finished:
/// dropped unfinished — a run that errs or is dropped — it has none, and reads
/// as truncated. Only the stages build a run's, from what admission holds.
pub struct Written<W: Write + Send + 'static> {
    tx: mpsc::SyncSender<Message>,
    thread: Option<JoinHandle<Ended<W>>>,
    #[cfg_attr(not(feature = "testing"), allow(dead_code))]
    behind: usize,
}

/// What the journal's thread leaves: the journal finished, none if it was
/// dropped unfinished, or its writer's error.
type Ended<W> = Result<Option<(W, Trailer)>, JournalError>;

impl<W: Write + Send + 'static> Written<W> {
    /// The header written to `out` at once, then its writing thread started,
    /// its channel `journal`'s.
    pub(crate) fn start(
        out: W,
        header: Header,
        journal: Journal,
    ) -> Result<Written<W>, JournalError> {
        let mut writer = Writer::new(out, header)?;
        let (tx, rx) = mpsc::sync_channel::<Message>(journal.behind());
        let thread = std::thread::Builder::new()
            .name("load-run-journal".to_owned())
            .spawn(move || {
                for w in rx {
                    match w {
                        Message::Line(line) => writer.line(&line)?,
                        Message::Finish => return writer.finish().map(Some),
                    }
                }
                // Dropped unfinished: no trailer.
                Ok(None)
            })
            .map_err(io)?;
        Ok(Written {
            tx,
            thread: Some(thread),
            behind: journal.behind(),
        })
    }

    /// A test's journal, `behind` lines behind, its header's admission marked
    /// assumed: no admission sized it.
    #[cfg(feature = "testing")]
    pub fn assumed(out: W, mut header: Header, behind: usize) -> Result<Written<W>, JournalError> {
        if let Some(a) = header.admission.as_mut() {
            a.assumed = true;
        }
        Written::start(out, header, Journal::new(behind, 0))
    }

    /// The lines its channel holds: a test's view of what admission sized.
    #[cfg(feature = "testing")]
    pub fn behind(&self) -> usize {
        self.behind
    }

    /// The thread's end: its error, or the journal.
    fn join(&mut self) -> Ended<W> {
        match self.thread.take().map(JoinHandle::join) {
            Some(Ok(r)) => r,
            Some(Err(_)) => Err(io(std::io::Error::other(
                "the journal's writing thread panicked",
            ))),
            None => Err(io(std::io::Error::other("the journal's writer is gone"))),
        }
    }
}

impl<W: Write + Send + 'static> JournalSink for Written<W> {
    type Finished = (W, Trailer);

    fn line(&mut self, line: Line) -> Result<(), JournalError> {
        // A writer gone failed on a line before: its error, now, so the run
        // stops within `behind` lines of it rather than at its end.
        if self.tx.send(Message::Line(line)).is_err() {
            return Err(match self.join() {
                Err(e) => e,
                Ok(_) => io(std::io::Error::other("the journal's writer stopped")),
            });
        }
        Ok(())
    }

    fn finish(mut self) -> Result<(W, Trailer), JournalError> {
        // A writer already gone has no trailer to write.
        let _ = self.tx.send(Message::Finish);
        match self.join()? {
            Some(done) => Ok(done),
            None => Err(io(std::io::Error::other(
                "the journal's writer ended unfinished",
            ))),
        }
    }
}

/// A journal written in the caller's thread, line by line: a gate's refusal,
/// whose one line needs no thread.
impl<W: Write + Send> JournalSink for Writer<W> {
    type Finished = (W, Trailer);

    fn line(&mut self, line: Line) -> Result<(), JournalError> {
        Writer::line(self, &line)
    }

    fn finish(self) -> Result<(W, Trailer), JournalError> {
        Writer::finish(self)
    }
}
