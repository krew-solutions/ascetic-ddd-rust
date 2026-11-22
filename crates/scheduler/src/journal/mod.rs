//! The journal (ADR-0047 decision 14, as decided for K1e-3): a run's event
//! stream, canonical JSON lines through `command-log`'s framing — a header, the
//! machine's inputs and what it decided, a trailer of counts and the SHA-256 of
//! every line before it, so that a run killed partway reads as truncated.
//!
//! Self-contained: a `read` carries each dependency's strength beside what it
//! bound, so that the verdict needs the log only to check its identity. No
//! captured value is journaled, only the slots an answer captured. A pure
//! [`events`] turns one machine step into its events, so that `sim` and the
//! shell write the same bytes. Version 3 (ADR-0047 decision 15): the header
//! names the run's namespace, T0 and the resend floor; a read, its
//! operation's safety and whether an observation polls; an answer, its status,
//! its code and the wait it asked for — what the fold needs to tell a licensed
//! resend from another. Any change of meaning is another version.

use std::fmt;
use std::io::{BufRead, Write};

use ascetic_ddd_command_log::frame::{self, Digest, FrameError};
use ascetic_ddd_command_log::record::{self, Name, On, Safety, Subject};
use ascetic_ddd_command_log::wire::{b64, i64s, u64s, u64s_option, u64s_vec};
use serde::{Deserialize, Serialize};

use crate::machine::{self, Binding, Input, Judgement, Line as Number, Output};

pub mod sample;

/// The version a reader accepts and a writer writes: exactly this one.
pub const JOURNAL_VERSION: u32 = 3;

/// The byte form's identity: the SHA-256 of the golden journal
/// (`golden/journal.jsonl`, [`sample`]), which holds every line and variant. A
/// change to how any line is written changes the golden journal, and so this,
/// in the same edit.
pub const JOURNAL: &str = "fd8f6d0802d9914a00c08fade092f5a5c46f4cd2db7e32ae09da7c6ae6790c31";

/// A journal's line.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Line {
    /// The first line.
    Header(Box<Header>),
    /// A line of the log read.
    Read(Read),
    /// A step's release decided.
    Released(Released),
    /// An attempt dispatched.
    Dispatch(Dispatch),
    /// An attempt's bytes left.
    Sent(Sent),
    /// An attempt answered, or given up at its deadline.
    Answer(Answer),
    /// A step ended.
    End(Box<Ended>),
    /// A harness fault.
    Fault(Fault),
    /// The last line.
    Trailer(Trailer),
}

/// What anchors a run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Header {
    /// [`JOURNAL_VERSION`].
    pub journal: u32,
    /// [`machine::MACHINE_REVISION`].
    pub machine: u32,
    /// The writer's own revision: its build, as it names it.
    pub source: String,
    /// The log's identity: its trailer's SHA-256, as its commitment holds it.
    pub log: String,
    /// The validator that committed the log (`command-log::live::validator`).
    pub validator: String,
    /// The run's read-ahead, at most the header's.
    #[serde(with = "u64s")]
    pub read_ahead: u64,
    /// The profile's jitter J.
    #[serde(with = "u64s")]
    pub jitter: u64,
    /// The profile's resend floor (ADR-0047 decision 15).
    #[serde(with = "u64s")]
    pub resend: u64,
    /// The run's namespace: public, every secret drawn from the run's own
    /// secret instead (decision 15), so that the run's rendered values and
    /// correlation ids can be told again.
    #[serde(with = "u64s")]
    pub namespace: u64,
    /// T0's wall instant, microseconds since the Unix epoch: the base of the
    /// run's times.
    #[serde(with = "i64s")]
    pub t0: i64,
}

/// A barrier key: the kind, scope, label and occurrence its members share.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Key {
    /// The members' kind.
    pub kind: Name,
    /// Their scope.
    #[serde(with = "b64")]
    pub scope: Vec<u8>,
    /// Their step.
    pub label: Name,
    /// Its occurrence.
    pub n: u32,
}

/// A dependency as the machine bound it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Dep {
    /// A step dependency: its strength, the line it bound.
    Step {
        /// Its strength.
        on: On,
        /// The line it bound.
        #[serde(with = "u64s")]
        line: Number,
    },
    /// A barrier: its strength, its key, its members read before the holder.
    Each {
        /// Its strength.
        on: On,
        /// Its key.
        key: Key,
        /// Its members read before the holder.
        #[serde(with = "u64s")]
        members: u64,
    },
}

/// What a step does, as its line names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    /// A REST call.
    Rest,
    /// A message published on the bus.
    Bus,
    /// An effect awaited on the bus.
    Observe,
    /// An effect awaited by polling a REST call (ADR-0047 decision 15): each
    /// poll an attempt of its own.
    Poll,
    /// A call of the stand's own.
    Host,
}

impl From<&record::Action> for Action {
    fn from(a: &record::Action) -> Action {
        match a {
            record::Action::Rest(_) => Action::Rest,
            record::Action::Bus(_) => Action::Bus,
            record::Action::Observe(o) => match o.probe {
                record::Probe::Rest(_) => Action::Poll,
                record::Probe::Bus { .. } => Action::Observe,
            },
            record::Action::Host(_) => Action::Host,
        }
    }
}

impl Action {
    /// Whether it awaits an effect: an observation, by the bus or polled.
    pub fn observes(self) -> bool {
        matches!(self, Action::Observe | Action::Poll)
    }
}

/// A line of the log read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Read {
    /// When.
    #[serde(with = "u64s")]
    pub at: u64,
    /// Its number.
    #[serde(with = "u64s")]
    pub line: Number,
    /// The barrier key it is a member of.
    pub key: Key,
    /// What it does.
    pub action: Action,
    /// Its action's safety: what a harness resend of it may be (ADR-0047
    /// decision 11, 15).
    pub safety: Safety,
    /// An observation's cause's line — its own if it is its own; none for
    /// another action (version 2: the fold reads the journal alone).
    #[serde(with = "u64s_option")]
    pub cause: Option<Number>,
    /// Its dependencies, in its `after`'s order.
    pub after: Vec<Dep>,
}

/// A step's release decided.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Released {
    /// When it was decided.
    #[serde(with = "u64s")]
    pub at: u64,
    /// Its line.
    #[serde(with = "u64s")]
    pub line: Number,
    /// `max(at, ready + think)`.
    #[serde(with = "u64s")]
    pub intended: u64,
    /// `max(intended, read)`.
    #[serde(with = "u64s")]
    pub released: u64,
    /// When its handler gives up on it.
    #[serde(with = "u64s")]
    pub deadline: u64,
}

/// An attempt dispatched.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Dispatch {
    /// When.
    #[serde(with = "u64s")]
    pub at: u64,
    /// Its step's line.
    #[serde(with = "u64s")]
    pub line: Number,
    /// The attempt, from 1.
    pub attempt: u32,
    /// When its handler gives up on it.
    #[serde(with = "u64s")]
    pub deadline: u64,
}

/// An attempt's bytes left.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sent {
    /// When.
    #[serde(with = "u64s")]
    pub at: u64,
    /// Its step's line.
    #[serde(with = "u64s")]
    pub line: Number,
    /// The attempt.
    pub attempt: u32,
}

/// How an attempt ended, as its handler judged it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Judged {
    /// An outcome `expect` lists.
    Expected,
    /// One it does not.
    Unexpected,
    /// A refusal certified as not run.
    NotTaken,
    /// No certain outcome.
    Unknown,
}

/// A slot an answer captured a value of: whose, which; never the value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Slot {
    /// Whose.
    pub subject: Subject,
    /// Which.
    pub slot: Name,
}

/// Why an answer changed nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Ignored {
    /// Its step had ended.
    Ended,
    /// It answers an attempt no longer out.
    Stale,
    /// It came past its deadline.
    Late,
}

/// An attempt answered.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Answer {
    /// When, as its handler stamped it.
    #[serde(with = "u64s")]
    pub at: u64,
    /// Its step's line.
    #[serde(with = "u64s")]
    pub line: Number,
    /// The attempt.
    pub attempt: u32,
    /// How it was judged.
    pub judged: Judged,
    /// The slots it captured.
    pub captured: Vec<Slot>,
    /// Whether the machine took nothing from it, and why.
    pub ignored: Option<Ignored>,
    /// Its status, if an answer came (ADR-0047 decision 15).
    pub status: Option<u16>,
    /// Its problem's code, as the journal may carry it.
    pub code: Option<Code>,
    /// Microseconds its `Retry-After` asked for.
    #[serde(with = "u64s_option")]
    pub after: Option<u64>,
}

/// A problem's code: a short dotted name as the server wrote it, or `other`
/// (ADR-0047 decision 15): no other server text enters the journal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Code {
    /// A dotted name.
    Named(Name),
    /// Any other text.
    Other,
}

impl From<&machine::Code> for Code {
    fn from(c: &machine::Code) -> Code {
        match c {
            machine::Code::Named(n) => Code::Named(n.clone()),
            machine::Code::Other => Code::Other,
        }
    }
}

/// Why a step was not sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Skip {
    /// A dependency awaited as expected that ended otherwise.
    Dependency(#[serde(with = "u64s")] Number),
    /// A barrier's member awaited as expected that ended otherwise.
    Member(#[serde(with = "u64s")] Number),
    /// A captured value its producer's answer did not give.
    MissingCapture(#[serde(with = "u64s")] Number),
}

/// How a step ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum End {
    /// As expected.
    Expected,
    /// With an answer `expect` does not list.
    Unexpected,
    /// With no certain outcome by its deadline.
    Unknown,
    /// Never sent.
    Skipped(Skip),
}

/// The dependency that made a step ready last.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Critical {
    /// A step dependency's line.
    Step(#[serde(with = "u64s")] Number),
    /// A barrier member's line.
    Member(#[serde(with = "u64s")] Number),
}

/// A step ended: the machine's entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ended {
    /// When the machine decided it.
    #[serde(with = "u64s")]
    pub at: u64,
    /// Its line.
    #[serde(with = "u64s")]
    pub line: Number,
    /// How.
    pub end: End,
    /// When it was read.
    #[serde(with = "u64s")]
    pub read: u64,
    /// When its dependencies were met.
    #[serde(with = "u64s")]
    pub ready: u64,
    /// `max(at, ready + think)`, or its end's instant if never released.
    #[serde(with = "u64s")]
    pub intended: u64,
    /// `max(intended, read)`, likewise.
    #[serde(with = "u64s")]
    pub released: u64,
    /// Its first attempt's dispatch.
    #[serde(with = "u64s_option")]
    pub dispatched: Option<u64>,
    /// When an attempt's bytes first left: the attempt then out's.
    #[serde(with = "u64s_option")]
    pub sent: Option<u64>,
    /// When it ended.
    #[serde(with = "u64s")]
    pub ended: u64,
    /// Attempts dispatched.
    pub attempts: u32,
    /// The dependency that made it ready last.
    pub critical: Option<Critical>,
    /// The lines its step dependencies bound.
    #[serde(with = "u64s_vec")]
    pub bound: Vec<Number>,
    /// Whether it ended only at the machine's backstop.
    pub fault: bool,
}

/// A harness fault: the run is void (decision 14).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultClass {
    /// A handler that never reported: the machine's backstop ended its step.
    Backstop,
}

/// A harness fault.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fault {
    /// When.
    #[serde(with = "u64s")]
    pub at: u64,
    /// Which.
    pub class: FaultClass,
    /// The step it struck.
    #[serde(with = "u64s")]
    pub line: Number,
}

/// The last line: the lines and steps before it, and their digest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Trailer {
    /// Lines between the header and the trailer.
    #[serde(with = "u64s")]
    pub events: u64,
    /// Of them, steps ended.
    #[serde(with = "u64s")]
    pub steps: u64,
    /// The SHA-256 of every line before the trailer, the header included.
    pub sha256: String,
}

fn key_of(step: &ascetic_ddd_command_log::record::StepId) -> Key {
    let (kind, scope, label, n) = ascetic_ddd_command_log::plan::member_key(step);
    Key {
        kind,
        scope,
        label,
        n,
    }
}

impl From<&machine::Skip> for Skip {
    fn from(s: &machine::Skip) -> Skip {
        match s {
            machine::Skip::Dependency(l) => Skip::Dependency(*l),
            machine::Skip::Member(l) => Skip::Member(*l),
            machine::Skip::MissingCapture(l) => Skip::MissingCapture(*l),
        }
    }
}

impl From<&machine::End> for End {
    fn from(e: &machine::End) -> End {
        match e {
            machine::End::Expected => End::Expected,
            machine::End::Unexpected => End::Unexpected,
            machine::End::Unknown => End::Unknown,
            machine::End::Skipped(s) => End::Skipped(s.into()),
        }
    }
}

impl From<machine::Ignored> for Ignored {
    fn from(i: machine::Ignored) -> Ignored {
        match i {
            machine::Ignored::Ended => Ignored::Ended,
            machine::Ignored::Stale => Ignored::Stale,
            machine::Ignored::Late => Ignored::Late,
        }
    }
}

fn ended(at: u64, e: &machine::Entry) -> Ended {
    Ended {
        at,
        line: e.line,
        end: (&e.end).into(),
        read: e.read,
        ready: e.ready,
        intended: e.intended,
        released: e.released,
        dispatched: e.dispatched,
        sent: e.sent,
        ended: e.ended,
        attempts: e.attempts,
        critical: e.critical.map(|c| match c {
            machine::Critical::Step(l) => Critical::Step(l),
            machine::Critical::Member(l) => Critical::Member(l),
        }),
        bound: e.bound.clone(),
        fault: e.fault,
    }
}

/// The journal's lines of one machine step: the input it took at `at`, then
/// what it decided, in order. A line's bindings and an answer's being ignored
/// are told with their input; wakes, wants and wake-ups are not journaled.
pub fn events(at: u64, input: &Input, outputs: &[Output]) -> Vec<Line> {
    let mut lines = Vec::new();
    match input {
        Input::Line { line, record } => {
            let bound = outputs.iter().find_map(|o| match o {
                Output::Bound {
                    line: l,
                    after,
                    cause,
                    safety,
                } if l == line => Some((after, *cause, *safety)),
                _ => None,
            });
            let cause = bound.and_then(|b| b.1);
            let safety = bound.map_or(Safety::Never, |b| b.2);
            let after = bound
                .map(|b| b.0)
                .into_iter()
                .flatten()
                .map(|b| match b {
                    Binding::Step { on, line } => Dep::Step {
                        on: *on,
                        line: *line,
                    },
                    Binding::Each { on, key, members } => Dep::Each {
                        on: *on,
                        key: Key {
                            kind: key.0.clone(),
                            scope: key.1.clone(),
                            label: key.2.clone(),
                            n: key.3,
                        },
                        members: *members,
                    },
                })
                .collect();
            lines.push(Line::Read(Read {
                at,
                line: *line,
                key: key_of(&record.step),
                action: (&record.action).into(),
                safety,
                cause,
                after,
            }));
        }
        Input::Sent { line, attempt } => lines.push(Line::Sent(Sent {
            at,
            line: *line,
            attempt: *attempt,
        })),
        Input::Ended {
            line,
            attempt,
            judgement,
            seen,
        } => {
            let ignored = outputs.iter().find_map(|o| match o {
                Output::Ignored {
                    line: l,
                    attempt: a,
                    why,
                } if l == line && a == attempt => Some((*why).into()),
                _ => None,
            });
            let (judged, captured) = match judgement {
                Judgement::Expected(c) => (
                    Judged::Expected,
                    c.iter()
                        .map(|c| Slot {
                            subject: c.subject.clone(),
                            slot: c.slot.clone(),
                        })
                        .collect(),
                ),
                Judgement::Unexpected => (Judged::Unexpected, Vec::new()),
                Judgement::NotTaken => (Judged::NotTaken, Vec::new()),
                Judgement::Unknown => (Judged::Unknown, Vec::new()),
            };
            lines.push(Line::Answer(Answer {
                at,
                line: *line,
                attempt: *attempt,
                judged,
                captured,
                ignored,
                status: seen.status,
                code: seen.code.as_ref().map(Code::from),
                after: seen.after,
            }));
        }
        Input::Wake => {}
    }
    for o in outputs {
        match o {
            Output::Released {
                line,
                intended,
                released,
                deadline,
            } => lines.push(Line::Released(Released {
                at,
                line: *line,
                intended: *intended,
                released: *released,
                deadline: *deadline,
            })),
            Output::Dispatch {
                line,
                attempt,
                deadline,
                ..
            } => lines.push(Line::Dispatch(Dispatch {
                at,
                line: *line,
                attempt: *attempt,
                deadline: *deadline,
            })),
            Output::Ended(e) => {
                lines.push(Line::End(Box::new(ended(at, e))));
                if e.fault {
                    lines.push(Line::Fault(Fault {
                        at,
                        class: FaultClass::Backstop,
                        line: e.line,
                    }));
                }
            }
            Output::Bound { .. } | Output::Ignored { .. } | Output::WakeAt(_) | Output::Want(_) => {
            }
        }
    }
    lines
}

/// Why a journal was refused.
#[derive(Debug)]
pub enum JournalError {
    /// Its framing: I/O, a malformed or non-canonical line, a line cut short.
    Frame(FrameError),
    /// The header is not the first line, or another follows it.
    Header {
        /// Where.
        line: u64,
    },
    /// The header names another journal version.
    Version(u32),
    /// It ends without its trailer.
    Truncated,
    /// The trailer disagrees with the lines before it.
    Trailer {
        /// What it says.
        expected: Trailer,
        /// What was read.
        read: Trailer,
    },
    /// A line follows the trailer.
    AfterTrailer,
}

impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JournalError::Frame(e) => write!(f, "journal: {e}"),
            JournalError::Header { line } => write!(
                f,
                "journal line {line}: the header must be the first line, and only it"
            ),
            JournalError::Version(v) => write!(f, "journal version {v}, not {JOURNAL_VERSION}"),
            JournalError::Truncated => f.write_str("the journal ends without its trailer"),
            JournalError::Trailer { expected, read } => write!(
                f,
                "the journal's trailer says {} lines, {} steps, {}; read {}, {}, {}",
                expected.events,
                expected.steps,
                expected.sha256,
                read.events,
                read.steps,
                read.sha256
            ),
            JournalError::AfterTrailer => f.write_str("a line follows the journal's trailer"),
        }
    }
}

impl std::error::Error for JournalError {}

impl From<FrameError> for JournalError {
    fn from(e: FrameError) -> JournalError {
        match e {
            FrameError::Truncated => JournalError::Truncated,
            e => JournalError::Frame(e),
        }
    }
}

/// Writes a journal: its header, its lines, then its trailer at
/// [`finish`](Writer::finish).
pub struct Writer<W: Write> {
    out: W,
    digest: Digest,
    events: u64,
    steps: u64,
}

impl<W: Write> Writer<W> {
    /// Starts a journal; refused if the header names another version.
    pub fn new(mut out: W, header: Header) -> Result<Writer<W>, JournalError> {
        if header.journal != JOURNAL_VERSION {
            return Err(JournalError::Version(header.journal));
        }
        let bytes = frame::canonical(&Line::Header(Box::new(header)))?;
        out.write_all(&bytes).map_err(FrameError::Io)?;
        let mut digest = Digest::new();
        digest.update(&bytes);
        Ok(Writer {
            out,
            digest,
            events: 0,
            steps: 0,
        })
    }

    /// Appends a line; a header or a trailer is refused.
    pub fn line(&mut self, line: &Line) -> Result<(), JournalError> {
        match line {
            Line::Header(_) | Line::Trailer(_) => {
                return Err(JournalError::Header {
                    line: self.events.saturating_add(2),
                });
            }
            Line::End(_) => self.steps = self.steps.saturating_add(1),
            _ => {}
        }
        let bytes = frame::canonical(line)?;
        self.out.write_all(&bytes).map_err(FrameError::Io)?;
        self.digest.update(&bytes);
        self.events = self.events.saturating_add(1);
        Ok(())
    }

    /// Writes the trailer and hands back the output.
    pub fn finish(mut self) -> Result<(W, Trailer), JournalError> {
        let trailer = Trailer {
            events: self.events,
            steps: self.steps,
            sha256: self.digest.hex(),
        };
        let bytes = frame::canonical(&Line::Trailer(trailer.clone()))?;
        self.out.write_all(&bytes).map_err(FrameError::Io)?;
        self.out.flush().map_err(FrameError::Io)?;
        Ok((self.out, trailer))
    }
}

/// Reads a journal: its header at once, then its lines, its trailer checked.
pub struct Reader<R: BufRead> {
    input: R,
    header: Header,
    digest: Digest,
    events: u64,
    steps: u64,
    line: u64,
    ended: bool,
}

impl<R: BufRead> Reader<R> {
    /// Opens a journal; refused unless its first line is a header of this
    /// version.
    pub fn new(mut input: R) -> Result<Reader<R>, JournalError> {
        let Some(bytes) = frame::next_line(&mut input)? else {
            return Err(JournalError::Header { line: 1 });
        };
        let header = match frame::decode::<Line>(&bytes, 1)? {
            Line::Header(h) => *h,
            _ => return Err(JournalError::Header { line: 1 }),
        };
        if header.journal != JOURNAL_VERSION {
            return Err(JournalError::Version(header.journal));
        }
        let mut digest = Digest::new();
        digest.update(&bytes);
        Ok(Reader {
            input,
            header,
            digest,
            events: 0,
            steps: 0,
            line: 1,
            ended: false,
        })
    }

    /// The journal's header.
    pub fn header(&self) -> &Header {
        &self.header
    }

    fn next_line(&mut self) -> Result<Option<Line>, JournalError> {
        if self.ended {
            return Ok(None);
        }
        self.line = self.line.saturating_add(1);
        let Some(bytes) = frame::next_line(&mut self.input)? else {
            return Err(JournalError::Truncated);
        };
        match frame::decode::<Line>(&bytes, self.line)? {
            Line::Header(_) => Err(JournalError::Header { line: self.line }),
            Line::Trailer(expected) => {
                self.ended = true;
                let read = Trailer {
                    events: self.events,
                    steps: self.steps,
                    sha256: self.digest.hex(),
                };
                if expected != read {
                    return Err(JournalError::Trailer { expected, read });
                }
                if frame::next_line(&mut self.input)?.is_some() {
                    return Err(JournalError::AfterTrailer);
                }
                Ok(None)
            }
            line => {
                if matches!(line, Line::End(_)) {
                    self.steps = self.steps.saturating_add(1);
                }
                self.events = self.events.saturating_add(1);
                self.digest.update(&bytes);
                Ok(Some(line))
            }
        }
    }
}

impl<R: BufRead> Iterator for Reader<R> {
    type Item = Result<Line, JournalError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_line() {
            Ok(Some(line)) => Some(Ok(line)),
            Ok(None) => None,
            Err(e) => {
                self.ended = true;
                Some(Err(e))
            }
        }
    }
}
