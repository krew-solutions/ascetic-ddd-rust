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

/// The journal written to `W` on a thread of its own, at most `behind` lines
/// after the machine: a full channel blocks the machine, whose lateness then
/// shows.
pub struct Written<W: Write + Send + 'static> {
    tx: mpsc::SyncSender<Line>,
    thread: JoinHandle<Result<(W, Trailer), JournalError>>,
}

impl<W: Write + Send + 'static> Written<W> {
    /// The header written to `out` at once, then its writing thread started.
    pub fn start(out: W, header: Header, behind: usize) -> Result<Written<W>, JournalError> {
        let mut writer = Writer::new(out, header)?;
        let (tx, rx) = mpsc::sync_channel::<Line>(behind.max(1));
        let thread = std::thread::Builder::new()
            .name("load-run-journal".to_owned())
            .spawn(move || {
                for line in rx {
                    writer.line(&line)?;
                }
                writer.finish()
            })
            .map_err(io)?;
        Ok(Written { tx, thread })
    }
}

impl<W: Write + Send + 'static> JournalSink for Written<W> {
    type Finished = (W, Trailer);

    fn line(&mut self, line: Line) -> Result<(), JournalError> {
        // A writer gone failed on its last line: its error is the finish's.
        let _ = self.tx.send(line);
        Ok(())
    }

    fn finish(self) -> Result<(W, Trailer), JournalError> {
        drop(self.tx);
        match self.thread.join() {
            Ok(r) => r,
            Err(_) => Err(io(std::io::Error::other(
                "the journal's writing thread panicked",
            ))),
        }
    }
}
