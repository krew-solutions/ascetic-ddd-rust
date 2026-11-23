//! Where the journal goes (ADR-0047 decision 16): a port, with an adapter in
//! the machine's task for tests and one on a thread of its own for a run,
//! whose writes block on the file.

use std::io::Write;
use std::sync::mpsc;
use std::thread::JoinHandle;

use ascetic_ddd_command_log::frame::FrameError;
use ascetic_ddd_scheduler::journal::{Header, JournalError, Line, Trailer, Writer};

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

/// Lines kept in memory: a test's, in the machine's task.
#[derive(Debug, Default)]
pub struct Lines {
    lines: Vec<Line>,
}

impl Lines {
    /// None yet.
    pub fn new() -> Lines {
        Lines::default()
    }
}

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
enum Message {
    Line(Line),
    /// The journal whole: its trailer, and only then (decision 18).
    Finish,
}

/// The journal written to `W` on a thread of its own, at most `behind` lines
/// after the machine: a full channel blocks the machine, whose lateness then
/// shows. Its trailer is written only when it is finished: dropped
/// unfinished — a run that errs or is dropped — it has none, and reads as
/// truncated.
pub struct Written<W: Write + Send + 'static> {
    tx: mpsc::SyncSender<Message>,
    thread: Option<JoinHandle<Ended<W>>>,
}

/// What the journal's thread leaves: the journal finished, none if it was
/// dropped unfinished, or its writer's error.
type Ended<W> = Result<Option<(W, Trailer)>, JournalError>;

impl<W: Write + Send + 'static> Written<W> {
    /// The header written to `out` at once, then its writing thread started.
    pub fn start(out: W, header: Header, behind: usize) -> Result<Written<W>, JournalError> {
        let mut writer = Writer::new(out, header)?;
        let (tx, rx) = mpsc::sync_channel::<Message>(behind.max(1));
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
        })
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
