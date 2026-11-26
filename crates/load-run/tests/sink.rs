//! The journal's own thread (ADR-0047 decision 18): its trailer written only
//! when the journal is finished, and a writer that failed heard within the
//! lines the machine may run ahead of it.

use std::io::{self, Cursor, Write};
use std::sync::{Arc, Mutex, mpsc};

use ascetic_ddd_load_run::sink::{JournalSink, Written};
use ascetic_ddd_scheduler::journal::{JournalError, Reader, sample};

/// A writer the test reads once the journal's thread let it go, failing
/// every write after its first `fail` lines.
struct Shared {
    buf: Arc<Mutex<Vec<u8>>>,
    gone: mpsc::Sender<()>,
    lines: usize,
    fail: Option<usize>,
}

impl Write for Shared {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        if self.fail.is_some_and(|f| self.lines >= f) {
            return Err(io::Error::other("the disk is full"));
        }
        self.lines += b.iter().filter(|c| **c == b'\n').count();
        self.buf.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        let _ = self.gone.send(());
    }
}

fn shared(fail: Option<usize>) -> (Shared, Arc<Mutex<Vec<u8>>>, mpsc::Receiver<()>) {
    let buf = Arc::new(Mutex::new(Vec::new()));
    let (gone, rx) = mpsc::channel();
    let w = Shared {
        buf: buf.clone(),
        gone,
        lines: 0,
        fail,
    };
    (w, buf, rx)
}

/// A journal dropped unfinished — a run that erred, or was dropped — has no
/// trailer, and reads as truncated; finished, it reads whole.
#[test]
fn a_journal_dropped_unfinished_has_no_trailer() {
    let lines = sample::lines();
    for finished in [false, true] {
        let (out, buf, gone) = shared(None);
        let mut w = Written::assumed(out, sample::header(), 4).unwrap();
        for l in lines.iter().take(5) {
            w.line(l.clone()).unwrap();
        }
        if finished {
            let (out, trailer) = w.finish().unwrap();
            assert_eq!(trailer.events, 5);
            drop(out);
        } else {
            drop(w);
        }
        // The thread let the writer go: everything it will write is written.
        gone.recv().unwrap();
        let bytes = buf.lock().unwrap().clone();
        let read: Result<Vec<_>, _> = Reader::new(Cursor::new(bytes)).unwrap().collect();
        if finished {
            assert_eq!(read.unwrap().len(), 5);
        } else {
            assert!(matches!(read, Err(JournalError::Truncated)), "{read:?}");
        }
    }
}

/// A writer failing at its journal's line k is heard by the machine's line
/// k + `behind` + 1 at the latest — the run stops near it, not at its end —
/// and its error is the writer's.
#[test]
fn a_failing_writer_is_heard_within_its_lag() {
    let behind = 4;
    let lines = sample::lines();
    // The header is the writer's first line; it fails on its third.
    let (out, _, _gone) = shared(Some(3));
    let mut w = Written::assumed(out, sample::header(), behind).unwrap();
    let (heard, error) = lines
        .iter()
        .cycle()
        .take(100)
        .enumerate()
        .find_map(|(i, l)| w.line(l.clone()).err().map(|e| (i, e)))
        .expect("the failure heard");
    // Lines 1 and 2 of the machine's are written; its line 3 fails.
    assert!(heard <= 2 + behind + 1, "heard at line {heard}");
    assert!(error.to_string().contains("the disk is full"), "{error}");
}

/// A test's journal is no admission's (ADR-0047 K2b-7e-2b-2b): its header's
/// admission says it was assumed, whatever it said before.
#[test]
fn an_assumed_journal_says_so() {
    let mut header = sample::header();
    if let Some(a) = header.admission.as_mut() {
        a.assumed = false;
    }
    assert!(header.admission.is_some());
    let (out, buf, gone) = shared(None);
    let w = Written::assumed(out, header, 4).unwrap();
    drop(w.finish().unwrap());
    gone.recv().unwrap();
    let bytes = buf.lock().unwrap().clone();
    let read = Reader::new(Cursor::new(bytes)).unwrap();
    assert!(read.header().admission.as_ref().is_some_and(|a| a.assumed));
}
