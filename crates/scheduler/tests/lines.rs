//! K2b-7e-2b-2b-1 of ADR-0047 (decision 22): a journal's line holds what its
//! walk counts and what its record's shape prices, and the journal's writer
//! holds its buffer alone. A binary of its own: its allocator counts each
//! thread's bytes.

// The command log's fixture reads its corpus's directory; the lint gate refuses
// files to the machine, not to its tests (ADR-0047 K1e-3).
#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::io::{self, Cursor, Write};
use std::rc::Rc;
use std::sync::Arc;

use ascetic_ddd_command_log::fills::Captured;
use ascetic_ddd_command_log::frame::{self, FrameError};
use ascetic_ddd_command_log::record::{Dep as RecordDep, On, Record};
use ascetic_ddd_command_log::shape::{self, Shape};
use ascetic_ddd_command_log::validate::{NoHost, Peaks};
use ascetic_ddd_scheduler::fits;
use ascetic_ddd_scheduler::journal::{
    self, BUFFER, Dep, Ended, Header, JournalError, Key, Line, Reader, Slot, Writer, line_bound,
    line_heap, sample,
};
use ascetic_ddd_scheduler::machine::{
    self, CODE, Code, Input, Judgement, Machine, Output, Seen, Unsent,
};
use ascetic_ddd_scheduler::sim::{self, Attempt};
use fixture::*;

/// The system allocator, counting each thread's own: the bytes it holds and
/// the bytes it was given, so the harness's threads are never a test's.
struct Counting;

thread_local! {
    /// The bytes this thread allocated less those it let go.
    static NOW: Cell<isize> = const { Cell::new(0) };
    /// The bytes this thread was given, never less.
    static TOOK: Cell<usize> = const { Cell::new(0) };
}

fn counted(by: isize, took: usize) {
    // A thread past its locals' end counts nothing: no test measures there.
    let _ = NOW.try_with(|n| n.set(n.get().saturating_add(by)));
    let _ = TOOK.try_with(|t| t.set(t.get().saturating_add(took)));
}

fn size(bytes: usize) -> isize {
    isize::try_from(bytes).unwrap_or(isize::MAX)
}

/// The bytes this thread holds.
fn now() -> isize {
    NOW.with(Cell::get)
}

/// The bytes this thread was given.
fn took() -> usize {
    TOOK.with(Cell::get)
}

// SAFETY: every call goes to the system allocator unchanged; this only counts
// the sizes it was asked for.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract, passed on.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            counted(size(layout.size()), layout.size());
        }
        p
    }

    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract, passed on.
        unsafe { System.dealloc(p, layout) };
        counted(-size(layout.size()), 0);
    }

    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: the caller's contract, passed on.
        let q = unsafe { System.realloc(p, layout, size) };
        if !q.is_null() {
            counted(self::size(size) - self::size(layout.size()), size);
        }
        q
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn bytes(n: usize) -> u64 {
    n as u64
}

/// A dotted code of exactly `CODE` bytes, as `Code::of` names it.
fn widest_code() -> Code {
    let code = format!("{}.{}", "a".repeat(31), "b".repeat(CODE - 32));
    assert_eq!(code.len(), CODE);
    let c = Code::of(&code);
    assert!(c.name().is_some_and(|n| n.len() == CODE));
    c
}

/// Every capture spec of `r` taken, each with `value`.
fn captured(r: &Record, value: &[u8]) -> Vec<Captured> {
    r.capture
        .iter()
        .map(|c| Captured {
            subject: c.subject.clone(),
            slot: c.slot.clone(),
            value: value.to_vec(),
        })
        .collect()
}

/// The lines of one machine step, each measured as it is made: the bytes
/// made for it are its walk's, exactly.
fn made(at: u64, input: &Input, outputs: &[Output]) -> Vec<Line> {
    let mut lines = Vec::new();
    let mut made = journal::events(at, input, outputs);
    loop {
        let before = now();
        let Some(line) = made.next() else { break };
        let held = u64::try_from(now() - before).unwrap();
        assert_eq!(held, line_heap(&line), "{line:?}");
        lines.push(line);
    }
    lines
}

/// Each row's price, at its worst, is what its line holds and what the walk
/// counts: a read of `k` step dependencies and `k` barriers no step is a
/// member of, its answer capturing every spec under codes of `CODE` bytes, its
/// end the lines its step dependencies bound — at 0, 1, 4, 5, 9 and 17.
#[test]
fn each_row_holds_its_price_at_its_worst() {
    for k in [0u64, 1, 4, 5, 9, 17] {
        let mut m = Machine::new(Arc::new(header()), W).unwrap();
        let producers: Vec<Record> = (0..k).map(|j| call("read", j, 0, 0, 0)).collect();
        let mut holder = captures_etag(call("read", 1_000, 0, 0, 0), 1_000);
        for p in &producers {
            holder.after.push(RecordDep::Step {
                step: p.step.clone(),
                on: On::Sent,
            });
        }
        for j in 0..k {
            holder.after.push(RecordDep::Each {
                kind: "nobody".into(),
                scope: vec![7; j as usize],
                label: format!("barrier-{j}-{}", "x".repeat(j as usize * 13)),
                n: 0,
                on: On::Settled,
                count: None,
            });
        }
        let s = shape::shape(&holder);
        let bound = line_bound(&s);
        let mut lines = Vec::new();
        let holder_line = k + 2;
        let mut binding = None;
        let mut step = |m: &mut Machine, at: u64, input: Input, lines: &mut Vec<Line>| {
            let out = m.step(at, input.clone()).unwrap();
            lines.extend(made(at, &input, &out));
            for o in &out {
                if let Output::Bound { line, after, .. } = o
                    && *line == holder_line
                {
                    // What the machine binds the holder by, walked.
                    let keys: usize = after
                        .iter()
                        .map(|b| match b {
                            machine::Binding::Step { .. } => 0,
                            machine::Binding::Each { key, .. } => {
                                key.0.capacity() + key.1.capacity() + key.2.capacity()
                            }
                        })
                        .sum();
                    binding = Some(bytes(
                        after.capacity() * size_of::<machine::Binding>() + keys,
                    ));
                }
            }
        };
        for (i, r) in producers.iter().chain([&holder]).enumerate() {
            let input = Input::Line {
                line: i as u64 + 2,
                record: Arc::new(r.clone()),
            };
            step(&mut m, 0, input, &mut lines);
        }
        for line in 2..holder_line {
            step(
                &mut m,
                1,
                Input::Sent {
                    line,
                    attempt: 1,
                    decorated: 1,
                    handed: 1,
                    connected: 1,
                    lag: 0,
                },
                &mut lines,
            );
        }
        step(&mut m, 2, Input::Wake, &mut lines);
        step(
            &mut m,
            3,
            Input::Sent {
                line: holder_line,
                attempt: 1,
                decorated: 3,
                handed: 3,
                connected: 3,
                lag: 0,
            },
            &mut lines,
        );
        let seen = Seen {
            code: Some(widest_code()),
            unsent: Some(Unsent {
                code: widest_code(),
                ..sim::drawn_unsent(holder_line, 1, 0)
            }),
            ..Seen::default()
        };
        step(
            &mut m,
            4,
            Input::Ended {
                line: holder_line,
                attempt: 1,
                judgement: Judgement::Expected(captured(&holder, b"\"v\"")),
                seen,
            },
            &mut lines,
        );
        let of_holder = |l: &&Line| match l {
            Line::Read(r) => r.line == holder_line,
            Line::Answer(a) => a.line == holder_line,
            Line::End(e) => e.line == holder_line,
            _ => false,
        };
        let rows: Vec<&Line> = lines.iter().filter(of_holder).collect();
        let read = rows
            .iter()
            .find(|l| matches!(l, Line::Read(_)))
            .expect("the holder read");
        let answer = rows
            .iter()
            .find(|l| matches!(l, Line::Answer(_)))
            .expect("the holder answered");
        let end = rows
            .iter()
            .find(|l| matches!(l, Line::End(_)))
            .expect("the holder ended");
        let deps = s.deps.elements;
        assert_eq!(deps, 2 * k);
        assert_eq!(
            line_heap(read),
            s.key_bytes + bytes(size_of::<Dep>()) * deps + s.each_bytes,
            "{k}: the read's row"
        );
        assert_eq!(
            line_heap(answer),
            bytes(size_of::<Slot>()) * s.captures.elements + s.capture_bytes + 2 * bytes(CODE),
            "{k}: the answer's row"
        );
        assert_eq!(
            line_heap(end),
            bytes(size_of::<Ended>()) + 8 * k,
            "{k}: the end's row, its step dependencies bound"
        );
        for l in &lines {
            assert!(line_heap(l) <= bound, "{k}: {l:?} past {bound}");
        }
        assert_eq!(
            bound,
            [line_heap(read), line_heap(answer), line_heap(end) + 8 * k]
                .into_iter()
                .max()
                .unwrap()
                .max(bytes(CODE)),
            "{k}: the bound is its rows' largest"
        );
        if let Line::Read(r) = read {
            assert_eq!(r.after.capacity(), r.after.len(), "{k}: built exact");
        }
        // The machine's own copy of the bindings, priced by `fits` from the
        // plan's largest record alone: exactly.
        assert_eq!(
            binding,
            Some(fits::needed(&Peaks {
                record: s,
                ..Peaks::default()
            })),
            "{k}: the bindings `fits` prices"
        );
    }
}

/// Over the corpus's logs, every answer capturing every spec: every line is
/// within the bound of the log's largest record, each read holding exactly
/// its key, its dependencies and its barriers' keys, each end its step
/// dependencies' lines, each answer its slots and codes — at their exact
/// capacities. Each row reached.
#[test]
fn every_corpus_line_is_within_its_record_s_bound() {
    let mut reached = [0usize; 3];
    for (name, records) in fixture::corpus::logs() {
        // The corpus's own: its logs that break a rule are no plan.
        let report = run_with(fixture::corpus::corpus_header(), &records, &NoHost, 1_000);
        if !report.violations.is_empty() {
            continue;
        }
        let largest = records
            .iter()
            .map(shape::shape)
            .fold(Shape::default(), Shape::most);
        let bound = line_bound(&largest);
        let script = |_: u64, _: u32, r: &Record| Attempt {
            judgement: Judgement::Expected(captured(r, b"\"v\"")),
            ..Attempt::expected(100)
        };
        let ran = sim::run(fixture::corpus::corpus_header(), &records, 0, &script).unwrap();
        let record = |line: u64| &records[usize::try_from(line - 2).unwrap()];
        for l in &ran.journal {
            let held = line_heap(l);
            assert!(held <= bound, "{name}: {l:?} holds {held}, past {bound}");
            match l {
                Line::Read(r) => {
                    let s = shape::shape(record(r.line));
                    assert_eq!(
                        held,
                        s.key_bytes + bytes(size_of::<Dep>()) * s.deps.elements + s.each_bytes,
                        "{name}: line {}",
                        r.line
                    );
                    reached[0] +=
                        usize::from(r.after.iter().any(|d| matches!(d, Dep::Each { .. })));
                }
                Line::End(e) => {
                    let steps = record(e.line)
                        .after
                        .iter()
                        .filter(|d| matches!(d, RecordDep::Step { .. }))
                        .count();
                    assert_eq!(
                        held,
                        bytes(size_of::<Ended>()) + 8 * steps as u64,
                        "{name}: line {}",
                        e.line
                    );
                    reached[1] += usize::from(!e.bound.is_empty());
                }
                Line::Answer(a) => {
                    let slots: usize = a
                        .captured
                        .iter()
                        .map(|s| s.subject.kind.len() + s.subject.scope.len() + s.slot.len())
                        .sum();
                    let codes = [a.code.as_ref(), a.unsent.as_ref().map(|u| &u.code)]
                        .into_iter()
                        .flatten()
                        .map(|c| match c {
                            journal::Code::Named(n) => n.len(),
                            journal::Code::Other => 0,
                        })
                        .sum::<usize>();
                    assert_eq!(
                        held,
                        bytes(size_of::<Slot>() * a.captured.len() + slots + codes),
                        "{name}: line {}",
                        a.line
                    );
                    assert!(a.captured.len() <= record(a.line).capture.len());
                    reached[2] += usize::from(!a.captured.is_empty());
                }
                _ => {}
            }
        }
    }
    assert!(
        reached.iter().all(|&n| n > 0),
        "reads with a barrier, ends with a bound line, answers with a capture: {reached:?}"
    );
}

/// What a writer writes to, shared so a test reads it as it is written; its
/// buffer reserved, so it is given nothing while a line is written; failing
/// once `fail` is set.
#[derive(Clone)]
struct Out {
    bytes: Rc<RefCell<Vec<u8>>>,
    flushes: Rc<Cell<usize>>,
    fail: Rc<Cell<bool>>,
}

impl Out {
    fn new() -> Out {
        Out {
            bytes: Rc::new(RefCell::new(Vec::with_capacity(4 << 20))),
            flushes: Rc::new(Cell::new(0)),
            fail: Rc::new(Cell::new(false)),
        }
    }
}

impl Write for Out {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.fail.get() {
            return Err(io::Error::other("the disk is gone"));
        }
        self.bytes.borrow_mut().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flushes.set(self.flushes.get() + 1);
        Ok(())
    }
}

/// A read line whose key's label is `label`.
fn labelled(label: String) -> Line {
    let Some(Line::Read(mut r)) = sample::lines()
        .into_iter()
        .find(|l| matches!(l, Line::Read(_)))
    else {
        unreachable!("the sample reads a line")
    };
    r.key = Key {
        label,
        ..r.key.clone()
    };
    Line::Read(r)
}

/// A read line whose canonical bytes are `len` long, at least its shortest.
fn read_of(len: usize) -> Line {
    let shortest = frame::canonical(&labelled(String::new())).unwrap().len();
    labelled("x".repeat(len.saturating_sub(shortest)))
}

/// The writer holds its buffer alone: given it and the header's box at its
/// start, nothing while a line is written — the shortest, about the buffer,
/// ten buffers of escapes, an end with its bound lines —; each line whole in
/// its output and the output flushed when it returns; its bytes the
/// canonical lines'; a header past the buffer read back with its trailer.
#[test]
fn the_writer_holds_its_buffer_alone() {
    for source in [String::from("build"), "s".repeat(3 * BUFFER)] {
        let header = Header {
            source,
            ..sample::header()
        };
        let (out, opened) = (Out::new(), header.clone());
        let given = took();
        let mut w = Writer::new(out.clone(), opened).unwrap();
        assert_eq!(
            took() - given,
            BUFFER + size_of::<Header>(),
            "its buffer and the header's box"
        );
        let mut expected = frame::canonical(&Line::Header(Box::new(header.clone()))).unwrap();
        assert_eq!(*out.bytes.borrow(), expected, "the header whole");
        let shortest = frame::canonical(&labelled(String::new())).unwrap().len();
        let sizes = [shortest, BUFFER - 1, BUFFER, BUFFER + 1, 3 * BUFFER];
        let mut lines: Vec<Line> = sizes.into_iter().map(read_of).collect();
        for (l, n) in lines.iter().zip(sizes) {
            assert_eq!(
                frame::canonical(l).unwrap().len(),
                n,
                "a line about the buffer"
            );
        }
        // Ten buffers of escapes: a control character written in six bytes.
        lines.push(labelled("\u{1}".repeat(10 * BUFFER / 6)));
        lines.extend(
            sample::lines()
                .into_iter()
                .filter(|l| matches!(l, Line::End(e) if !e.bound.is_empty())),
        );
        assert!(
            lines.iter().any(|l| matches!(l, Line::End(_))),
            "an end with its bound"
        );
        for (n, l) in lines.iter().enumerate() {
            let flushes = out.flushes.get();
            let given = took();
            w.line(l).unwrap();
            assert_eq!(took() - given, 0, "line {n} given nothing");
            expected.extend(frame::canonical(l).unwrap());
            assert_eq!(*out.bytes.borrow(), expected, "line {n} whole once written");
            assert_eq!(out.flushes.get(), flushes + 1, "line {n} flushed");
        }
        let (_, trailer) = w.finish().unwrap();
        expected.extend(frame::canonical(&Line::Trailer(trailer)).unwrap());
        let written = out.bytes.borrow().clone();
        assert_eq!(written, expected);
        let mut reader = Reader::new(Cursor::new(written)).unwrap();
        assert_eq!(*reader.header(), header);
        let read: Vec<Line> = reader.by_ref().map(Result::unwrap).collect();
        assert_eq!(read, lines, "read back, its trailer verified");
    }
}

/// An output that fails is the writer's I/O error, not an encoding's.
#[test]
fn a_failing_output_is_an_io_error() {
    let out = Out::new();
    let mut w = Writer::new(out.clone(), sample::header()).unwrap();
    out.fail.set(true);
    let failed = w.line(&read_of(3 * BUFFER));
    assert!(
        matches!(failed, Err(JournalError::Frame(FrameError::Io(_)))),
        "{failed:?}"
    );
    let out = Out::new();
    out.fail.set(true);
    let failed = Writer::new(out, sample::header());
    assert!(
        matches!(failed, Err(JournalError::Frame(FrameError::Io(_)))),
        "{:?}",
        failed.err()
    );
}

/// The two dependencies' sizes, which a price must not confuse.
#[test]
fn the_record_s_and_the_journal_s_dependencies() {
    eprintln!(
        "MEASURED size_of record::Dep {}, journal::Dep {}, machine::Binding {}, Slot {}, Ended {}",
        size_of::<RecordDep>(),
        size_of::<Dep>(),
        size_of::<machine::Binding>(),
        size_of::<Slot>(),
        size_of::<Ended>()
    );
}
