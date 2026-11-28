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
use ascetic_ddd_command_log::wire::{b64, i64s_option, u64s, u64s_option, u64s_vec};
use serde::{Deserialize, Serialize};

use crate::machine::{self, Binding, Input, Judgement, Line as Number, Output};

pub mod sample;

/// The version a reader accepts and a writer writes: exactly this one.
pub const JOURNAL_VERSION: u32 = 11;

/// The byte form's identity: the SHA-256 of the golden journal
/// (`golden/journal.jsonl`, [`sample`]), which holds every line and variant. A
/// change to how any line is written changes the golden journal, and so this,
/// in the same edit.
pub const JOURNAL: &str = "625abefb014879ddabf9572a8be8d9e9452a6ebe8c1524f5c8691f9deb06739e";

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
    /// The run's memory, sampled by the journal's writer (version 11,
    /// decision 22, K2b-7e-2b-3e): evidence no fold reads.
    Sample(Box<Sample>),
    /// The last line.
    Trailer(Trailer),
}

/// Why a sample was taken.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SampleWhy {
    /// Right after the header: the stages' residue.
    Baseline,
    /// Its instant came.
    Due,
    /// The run finished, before the trailer.
    Finish,
}

/// The run's memory at one instant (version 11): the process's, its
/// cgroup's, the host's pressure, the shell's live counts and the
/// allocator's, each group read or not and why.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sample {
    /// Why it was taken.
    pub why: SampleWhy,
    /// The run's clock when it was read, microseconds.
    #[serde(with = "u64s")]
    pub at: u64,
    /// The instant it was due: `at` for a baseline's and a finish's.
    #[serde(with = "u64s")]
    pub due: u64,
    /// The due instants passed unsampled since the last sample.
    #[serde(with = "u64s")]
    pub skipped: u64,
    /// The process's own, from `/proc/self/status`.
    pub process: Reading<Process>,
    /// Its cgroup's, from its own group's files.
    pub cgroup: Reading<Cgroup>,
    /// The host's memory pressure, `/proc/pressure/memory`.
    pub host: Reading<Pressure>,
    /// The shell's live counts, as its loop last stored them.
    pub live: Live,
    /// The allocator's, from a host-given probe.
    pub allocator: Allocator,
}

/// A group read, or why not.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reading<T> {
    /// Read.
    Read(T),
    /// Not read.
    Unread(Unread),
}

/// Why a group was not read: its anchor's file absent, not permitted, no
/// cgroup v2 here, or its read failed otherwise.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Unread {
    /// Its file is not there.
    Absent,
    /// Not permitted.
    Permission,
    /// No unified cgroup hierarchy.
    NoCgroupV2,
    /// Read, but not as expected.
    Failed,
}

/// The process's memory, bytes: its anchor the resident set; each other
/// field none where the kernel gives none.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Process {
    /// `VmRSS`.
    #[serde(with = "u64s")]
    pub rss: u64,
    /// `RssAnon`.
    #[serde(with = "u64s_option")]
    pub rss_anon: Option<u64>,
    /// `RssFile`.
    #[serde(with = "u64s_option")]
    pub rss_file: Option<u64>,
    /// `RssShmem`.
    #[serde(with = "u64s_option")]
    pub rss_shmem: Option<u64>,
    /// `VmSwap`.
    #[serde(with = "u64s_option")]
    pub swap: Option<u64>,
    /// `VmHWM`: the resident set's high-water.
    #[serde(with = "u64s_option")]
    pub hwm: Option<u64>,
    /// `Threads`.
    #[serde(with = "u64s_option")]
    pub threads: Option<u64>,
    /// Microseconds its read took.
    #[serde(with = "u64s")]
    pub took: u64,
}

/// The process's cgroup's memory, bytes: its anchor `memory.current`; each
/// other field none where the kernel gives none; the events and pressure
/// of the group whose limit binds beside its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cgroup {
    /// The processes in it.
    #[serde(with = "u64s_option")]
    pub procs: Option<u64>,
    /// `memory.current`.
    #[serde(with = "u64s")]
    pub current: u64,
    /// `memory.peak`.
    #[serde(with = "u64s_option")]
    pub peak: Option<u64>,
    /// `memory.stat`'s `anon`.
    #[serde(with = "u64s_option")]
    pub anon: Option<u64>,
    /// `anon_thp`.
    #[serde(with = "u64s_option")]
    pub anon_thp: Option<u64>,
    /// `file`: the page cache, the journal's own among it.
    #[serde(with = "u64s_option")]
    pub file: Option<u64>,
    /// `sock`: the kernel's socket buffers.
    #[serde(with = "u64s_option")]
    pub sock: Option<u64>,
    /// `kernel_stack`.
    #[serde(with = "u64s_option")]
    pub kernel_stack: Option<u64>,
    /// `pagetables`.
    #[serde(with = "u64s_option")]
    pub pagetables: Option<u64>,
    /// `slab`.
    #[serde(with = "u64s_option")]
    pub slab: Option<u64>,
    /// `memory.swap.current`.
    #[serde(with = "u64s_option")]
    pub swap: Option<u64>,
    /// Its own `memory.events`.
    pub events: Option<Events>,
    /// Its own `memory.pressure`.
    pub pressure: Option<Pressure>,
    /// The depth above it of the group whose `memory.max` binds, and that
    /// group's events and pressure: an ancestor's limit counts there, not
    /// here.
    pub binding: Option<Binds>,
    /// Microseconds its reads took.
    #[serde(with = "u64s")]
    pub took: u64,
}

/// A cgroup's `memory.events`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Events {
    /// `high`.
    #[serde(with = "u64s")]
    pub high: u64,
    /// `max`.
    #[serde(with = "u64s")]
    pub max: u64,
    /// `oom`.
    #[serde(with = "u64s")]
    pub oom: u64,
    /// `oom_kill`.
    #[serde(with = "u64s")]
    pub oom_kill: u64,
}

/// A pressure file's memory stall: `some` and `full`, each its ten-second
/// average in hundredths of a percent and its total in microseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pressure {
    /// `some avg10`, hundredths of a percent.
    pub some_avg10: u32,
    /// `some total`, microseconds.
    #[serde(with = "u64s")]
    pub some_total: u64,
    /// `full avg10`, hundredths of a percent.
    pub full_avg10: u32,
    /// `full total`, microseconds.
    #[serde(with = "u64s")]
    pub full_total: u64,
}

/// The group whose limit binds, above the process's own.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binds {
    /// Its depth: 0 for the process's own.
    pub depth: u32,
    /// Its `memory.events`.
    pub events: Option<Events>,
    /// Its `memory.pressure`.
    pub pressure: Option<Pressure>,
}

/// The shell's live counts, as its loop stored them at its last turn: one
/// instant's, never two turns' mixed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Live {
    /// The run's clock at that turn: stale and past `wakes`, a stuck loop.
    #[serde(with = "u64s")]
    pub turn: u64,
    /// The instant the loop sleeps until, if it sleeps.
    #[serde(with = "u64s_option")]
    pub wakes: Option<u64>,
    /// The lines read and not ended.
    #[serde(with = "u64s")]
    pub pending: u64,
    /// The attempts' handles past their deadline — not admission's `late`,
    /// which counts the steps late by a jitter, ended or not.
    #[serde(with = "u64s")]
    pub past_deadline: u64,
    /// The attempts alive.
    #[serde(with = "u64s")]
    pub alive: u64,
    /// The carrier's sockets open, by its own word; none where it gives
    /// none.
    #[serde(with = "u64s_option")]
    pub sockets: Option<u64>,
    /// The journal's lines sent while its channel was full.
    #[serde(with = "u64s")]
    pub full: u64,
    /// Microseconds the machine waited on a full channel.
    #[serde(with = "u64s")]
    pub waited: u64,
}

/// The allocator's own figures, from a host-given probe.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Allocator {
    /// Whether a probe is stated, and how its read went.
    pub state: AllocatorState,
    /// The allocator, a dotted name the probe gives (`glibc.2.39`, say).
    pub name: Option<Code>,
    /// Its bytes in use.
    #[serde(with = "u64s_option")]
    pub in_use: Option<u64>,
    /// Its bytes free and kept.
    #[serde(with = "u64s_option")]
    pub free: Option<u64>,
    /// Its bytes mapped apart.
    #[serde(with = "u64s_option")]
    pub mapped: Option<u64>,
    /// Its free chunks: what its walk is as long as.
    #[serde(with = "u64s_option")]
    pub chunks: Option<u64>,
    /// Microseconds the probe took.
    #[serde(with = "u64s")]
    pub took: u64,
}

/// Whether the allocator was read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocatorState {
    /// No probe stated: not this run's to read.
    Unstated,
    /// A probe stated, its read failed (it panicked or gave none).
    Failed,
    /// A probe stated, not read at this sample: only a baseline's and a
    /// finish's are, until measurements at a launch's states move it.
    Skipped,
    /// Read.
    Read,
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
    /// The run's catch-up κ, parts per thousand (version 9, ADR-0047 decision
    /// 21): a late harness compresses its plan by at most this, at most the
    /// profile's reserve.
    pub catch_up: u32,
    /// The run's namespace: public, every secret drawn from the run's own
    /// secret instead (decision 15), so that the run's rendered values and
    /// correlation ids can be told again.
    #[serde(with = "u64s")]
    pub namespace: u64,
    /// T0's wall instant, microseconds since the Unix epoch: the base of the
    /// run's times; none when a gate refused the run before its clock started
    /// (version 6, ADR-0047 decision 18).
    #[serde(with = "i64s_option")]
    pub t0: Option<i64>,
    /// The stand's gates the run passed before T0, in order (version 6).
    pub gates: Vec<Gate>,
    /// The judging time measured before T0 (version 6): evidence, not a
    /// refusal, until a plan claims J's precision.
    pub judging: Option<Judging>,
    /// What admission held the run to (version 8, ADR-0047 decision 20):
    /// evidence of the sockets each source needed against its cap and its
    /// driver's events, and of the process's descriptors.
    pub admission: Option<Admission>,
}

/// Admission's evidence (version 8, decision 20; version 10, decision 22's
/// memory, K2b-7e-2b-3d-3b).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Admission {
    /// Each source whose carrier needs sockets.
    pub sources: Vec<SourceFit>,
    /// The process's descriptors, read once its runtimes existed; none if
    /// no source needed sockets.
    pub descriptors: Option<DescriptorsRead>,
    /// What of it a test assumed rather than read, each apart (version 10).
    pub assumed: Assumed,
    /// The memory it was fitted to (version 10).
    pub memory: MemoryFit,
}

/// What of admission's evidence a test assumed, each apart (version 10): no
/// assumption folded into another.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Assumed {
    /// The plan: a test's peaks, no commitment's.
    pub plan: bool,
    /// The runtimes: a test's, not built for the caps fitted.
    pub runtimes: bool,
    /// The journal: a test's, no admission sized its lines behind.
    pub journal: bool,
}

impl Assumed {
    /// Whether any of it was.
    #[must_use]
    pub fn any(&self) -> bool {
        self.plan || self.runtimes || self.journal
    }
}

/// The memory a run was fitted to (version 10, decision 22): what was stated,
/// the process's limit, the library's constants, every term, and what the fit
/// made of them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryFit {
    /// The memory stated, in bytes.
    #[serde(with = "u64s")]
    pub stated: u64,
    /// Where it was stated.
    pub origin: MemoryOrigin,
    /// The process's limit as admission read it.
    pub limit: LimitRead,
    /// The library's constants a cost is one of.
    pub model: Model,
    /// Every term.
    pub terms: Vec<MemoryTerm>,
    /// The parts whose count no enforcer holds yet.
    pub unheld: Vec<Part>,
    /// The pool's budget: the stated memory less the fixed and the run's parts.
    #[serde(with = "u64s")]
    pub budget: u64,
    /// The pool's prediction at the run's catch-up, the header's `catch_up`.
    #[serde(with = "u64s")]
    pub pool: u64,
    /// What the process needs at its largest phase.
    #[serde(with = "u64s")]
    pub need: u64,
    /// The stated memory less the need.
    #[serde(with = "u64s")]
    pub slack: u64,
}

/// Where the memory stated came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryOrigin {
    /// Declared by the host.
    Declared,
}

/// The process's cgroup limits as admission read them: the least of each,
/// with its depth above the process's own group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitRead {
    /// The least `memory.max`; none where none is finite.
    pub max: Option<Bound>,
    /// The least `memory.high`; none where none is finite.
    pub high: Option<Bound>,
    /// Where it came from.
    pub origin: LimitOrigin,
}

/// A limit's bytes and its depth.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bound {
    /// Its bytes.
    #[serde(with = "u64s")]
    pub bytes: u64,
    /// Its depth above the process's own group: 0 for that group.
    pub depth: u32,
}

/// Where a limit came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LimitOrigin {
    /// Read from the process's own cgroups.
    Read,
    /// Read from a test's tree.
    Tree,
    /// A test's assumption.
    Assumed,
}

/// The library's constants a part's cost is one of, as the fit took them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Model {
    /// An ended step's cell.
    #[serde(with = "u64s")]
    pub ended_cell: u64,
    /// A step not ended's cell.
    #[serde(with = "u64s")]
    pub live_cell: u64,
    /// A barrier key beside its own bytes.
    #[serde(with = "u64s")]
    pub barrier: u64,
    /// A record's `Arc`.
    #[serde(with = "u64s")]
    pub record: u64,
    /// The log's decompressor.
    #[serde(with = "u64s")]
    pub decoder: u64,
    /// The shell's share of an attempt.
    #[serde(with = "u64s")]
    pub attempt: u64,
    /// A runtime's own beside its events.
    #[serde(with = "u64s")]
    pub runtime: u64,
    /// A driver's event, where its size is stated.
    #[serde(with = "u64s_option")]
    pub event: Option<u64>,
    /// A term of the evidence, as it is held (K2b-7e-2b-3d-3b-4c).
    #[serde(with = "u64s")]
    pub term: u64,
    /// A row of the evidence.
    #[serde(with = "u64s")]
    pub row: u64,
    /// A part named in the evidence.
    #[serde(with = "u64s")]
    pub part: u64,
}

/// A part's term: `count · cost + rest`, its phase, who holds its count, who
/// states its cost and what it is counted against.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryTerm {
    /// The part.
    pub part: Part,
    /// Its source, if it is a source's.
    pub source: Option<u32>,
    /// Its phase.
    pub phase: Phase,
    /// The things its enforcer bounds.
    #[serde(with = "u64s")]
    pub count: u64,
    /// What each holds.
    #[serde(with = "u64s")]
    pub cost: u64,
    /// What does not scale with them.
    #[serde(with = "u64s")]
    pub rest: u64,
    /// `count · cost + rest`.
    #[serde(with = "u64s")]
    pub bytes: u64,
    /// Who holds its count.
    pub held: Held,
    /// Who states its cost.
    pub stated: StatedBy,
    /// What it is counted against.
    pub basis: Basis,
}

/// A part of what a run holds (decision 22): the fit's own, by name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Part {
    /// The reader's records ahead.
    Reader,
    /// The log's decompressor.
    Decoder,
    /// A source's runtime.
    Runtime,
    /// The carrier's own of a source.
    CarrierSource,
    /// The shell's keeping, one a source.
    Keeping,
    /// The journal's lines behind.
    Journal,
    /// The journal's output.
    JournalOut,
    /// A source's connections.
    Connections,
    /// A source's connections' own heap.
    Io,
    /// A source's answers.
    Answers,
    /// Stage 2's judging.
    Judging,
    /// A gate's probes' connections.
    Probes,
    /// A gate's probes' own heap.
    ProbesIo,
    /// The gates' client before the run.
    GateStages,
    /// The gates' client during it.
    GateRun,
    /// The cells of the steps held, as ended.
    Cells,
    /// The steps not ended, beyond their cells as ended.
    Pending,
    /// The barrier keys.
    Barriers,
    /// The late steps' records.
    Records,
    /// The producers the records not ended are bound to.
    BoundTo,
    /// What the held keep once ended.
    Kept,
    /// One record's bindings.
    Binding,
    /// The shell's attempts.
    ShellAttempts,
    /// The carrier's attempts.
    CarrierAttempts,
    /// The attempts' decorations.
    Decorations,
    /// Admission's evidence itself, its copies at once (K2b-7e-2b-3d-3b-4c).
    Evidence,
    /// The host's allocator probe, a read's bytes as the host states them
    /// (version 11, K2b-7e-2b-3e-2c).
    Probe,
}

/// A part's phase: when its bytes are held.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// Throughout.
    Fixed,
    /// The stages before T0.
    Stages,
    /// The gates' probes.
    Gates,
    /// The run.
    Run,
    /// The run's pool, at its catch-up.
    Pool,
}

/// Who holds a part's count.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Held {
    /// What is built holds it.
    Built,
    /// A bounded channel.
    Channel,
    /// A source's cap.
    Cap,
    /// It is held once.
    Once,
    /// A gate: the read gate, built or not.
    Gate {
        /// Whether it is built.
        built: bool,
    },
    /// The host.
    Host,
}

/// Who states a part's cost.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatedBy {
    /// The library.
    Library,
    /// The host.
    Host,
}

/// What a part is counted against.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Basis {
    /// The heap.
    Heap,
    /// Outside the heap.
    Outside,
}

/// A source's sockets as admission held them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceFit {
    /// The source.
    pub source: u32,
    /// Needed at once under the plan's bound.
    #[serde(with = "u64s")]
    pub needed: u64,
    /// Its cap.
    #[serde(with = "u64s")]
    pub cap: u64,
    /// Its driver's events a turn.
    #[serde(with = "u64s")]
    pub events: u64,
}

/// The process's descriptors as admission read them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DescriptorsRead {
    /// Its soft limit.
    #[serde(with = "u64s")]
    pub soft: u64,
    /// Its hard limit.
    #[serde(with = "u64s")]
    pub hard: u64,
    /// Those open.
    #[serde(with = "u64s")]
    pub open: u64,
    /// Whether the soft limit was raised to the hard one.
    pub raised: bool,
}

/// A stand's gate passed before T0 (decision 18).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Gate {
    /// Its name, a dotted name the host chooses.
    pub name: Code,
    /// Microseconds it took, waits included.
    #[serde(with = "u64s")]
    pub took: u64,
}

/// The judging time at the profile's cap, measured before T0 (decision 18):
/// a document of each shape filling the cap, read, judged and dropped on a
/// source's runtime; the worst shape's median.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Judging {
    /// The cap, in bytes.
    #[serde(with = "u64s")]
    pub cap: u64,
    /// The worst shape's median, in microseconds.
    #[serde(with = "u64s")]
    pub took: u64,
    /// That shape.
    pub shape: Shape,
}

/// A JSON document's shape, by what judging it costs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Shape {
    /// One object of scalar members.
    Flat,
    /// An object of small objects.
    Objects,
    /// An array of numbers.
    Numbers,
    /// An array of strings of escapes.
    Strings,
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
    /// When the plan has it (version 4): the dependencies' lateness is its
    /// intended start's from it (ADR-0047 decision 17).
    #[serde(with = "u64s")]
    pub planned: u64,
    /// Whether its plan expects it refused at a rate limit — a deviation
    /// from the validator's rate rule (version 9, ADR-0047 decision 21): a
    /// bucket refills over a shift, so it may be admitted.
    pub refusal: bool,
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
    /// When the wake that dispatched it was due (version 4): the machine's
    /// lateness is `at` from it (ADR-0047 decision 17).
    #[serde(with = "u64s")]
    pub due: u64,
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
    /// When it was ready to pass the hand-off — decorated, if its carrier
    /// decorates (version 9, ADR-0047 decision 21).
    #[serde(with = "u64s")]
    pub decorated: u64,
    /// When it was handed off (version 4): the hand-off's lateness from the
    /// dispatch, the harness's (ADR-0047 decision 17).
    #[serde(with = "u64s")]
    pub handed: u64,
    /// When its request was handed to a ready connection: the connect's
    /// lateness from `handed`, the stand's; the write's to `at`, the stand's —
    /// less `lag`.
    #[serde(with = "u64s")]
    pub connected: u64,
    /// Microseconds its source's thread kept its send path's wakes waiting
    /// between `handed` and `at` (version 6): the harness's, not the stand's
    /// (ADR-0047 decision 18).
    #[serde(with = "u64s")]
    pub lag: u64,
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
    /// The frontier's censor at the deadline, not an answer (version 4):
    /// the harness's lateness may excuse only it (ADR-0047 decision 17).
    pub censored: bool,
    /// A late answer's bracket (version 5): it arrived after `absent`, its
    /// attempt censored at `deadline` (ADR-0047 decision 17).
    pub found: Option<Found>,
    /// A never-sent attempt's closing report (version 7, ADR-0047 decision
    /// 19): not an answer — its path to its censor.
    pub unsent: Option<Unsent>,
}

/// A never-sent attempt's path to its censor (version 7).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Unsent {
    /// When it was ready to pass the hand-off — decorated, if its carrier
    /// decorates (version 9); none if it never was.
    #[serde(with = "u64s_option")]
    pub decorated: Option<u64>,
    /// When it was handed off, ready to connect; none if it never was.
    #[serde(with = "u64s_option")]
    pub handed: Option<u64>,
    /// When its request was handed to a connection, if ever.
    #[serde(with = "u64s_option")]
    pub connected: Option<u64>,
    /// Its source thread's lag on its path, in microseconds.
    #[serde(with = "u64s")]
    pub lag: u64,
    /// Connects the peer refused by its own act.
    pub refused: u32,
    /// The last of them, if any.
    pub refused_by: Option<Refusal>,
    /// Where it stood: a dotted name its carrier chooses.
    pub code: Code,
}

/// How the peer refused a connect (version 7).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Refusal {
    /// No listener.
    Refused,
    /// Reset while connecting.
    Reset,
    /// Aborted while connecting.
    Aborted,
    /// A fresh connection closed before a byte of it was sent.
    Closed,
}

impl From<&machine::Unsent> for Unsent {
    fn from(u: &machine::Unsent) -> Unsent {
        Unsent {
            decorated: u.decorated,
            handed: u.handed,
            connected: u.connected,
            lag: u.lag,
            refused: u.refused,
            refused_by: u.refused_by.map(|r| match r {
                machine::Refusal::Refused => Refusal::Refused,
                machine::Refusal::Reset => Refusal::Reset,
                machine::Refusal::Aborted => Refusal::Aborted,
                machine::Refusal::Closed => Refusal::Closed,
            }),
            code: Code::from(&u.code),
        }
    }
}

/// Where a late answer was not, and its attempt's deadline (version 5).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Found {
    /// The last instant the shell knew it had not come.
    #[serde(with = "u64s")]
    pub absent: u64,
    /// Its attempt's deadline.
    #[serde(with = "u64s")]
    pub deadline: u64,
    /// When its attempt was handed off, if it was (version 9, ADR-0047
    /// decision 21): an early reply's attempt has no `Sent`, yet its bytes
    /// reached the system.
    #[serde(with = "u64s_option")]
    pub handed: Option<u64>,
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
        match c.name() {
            Some(n) => Code::Named(n.to_owned()),
            None => Code::Other,
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
    /// When the wake whose taking ended it was due (version 4); none for an
    /// end an input made, or a skip (ADR-0047 decision 17).
    #[serde(with = "u64s_option")]
    pub wake: Option<u64>,
}

/// A harness fault: the run is void (decisions 14, 18). A step's — the
/// backstop's — after which the run goes on; or a run's, which stops it: the
/// journal's last line before its trailer. No host text: names are dotted
/// names, the rest numbers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultClass {
    /// A handler that never reported: the machine's backstop ended its step.
    Backstop {
        /// The step.
        #[serde(with = "u64s")]
        line: Number,
    },
    /// An input stamped below what the frontier released.
    Below {
        /// Its stamp.
        #[serde(with = "u64s")]
        stamp: u64,
        /// What was released.
        #[serde(with = "u64s")]
        released: u64,
    },
    /// An input taken stamped below one taken before it.
    Disordered {
        /// Its stamp.
        #[serde(with = "u64s")]
        stamp: u64,
        /// The last taken.
        #[serde(with = "u64s")]
        last: u64,
    },
    /// The machine's own work stamped below the instant it was taken.
    Backdated {
        /// Its stamp.
        #[serde(with = "u64s")]
        stamp: u64,
        /// When it was taken.
        #[serde(with = "u64s")]
        now: u64,
    },
    /// An attempt's handle dropped before it ended.
    Dropped {
        /// Its step.
        #[serde(with = "u64s")]
        line: Number,
        /// The attempt.
        attempt: u32,
    },
    /// A carrier could not carry an attempt.
    Carrier {
        /// Its step.
        #[serde(with = "u64s")]
        line: Number,
        /// The attempt.
        attempt: u32,
        /// What it could not do, a dotted name the carrier chooses.
        cannot: Code,
    },
    /// An answer judged for the route timeout and still not ended.
    Held {
        /// Its step.
        #[serde(with = "u64s")]
        line: Number,
        /// The attempt.
        attempt: u32,
        /// When it arrived.
        #[serde(with = "u64s")]
        since: u64,
    },
    /// Work due at an instant and no progress on it.
    Stalled,
    /// A lock poisoned by a panic.
    Poisoned,
    /// Attempt tasks alive past the steps not ended and a margin: a source
    /// that cannot keep up.
    Saturated {
        /// Attempt tasks alive.
        #[serde(with = "u64s")]
        alive: u64,
    },
    /// The clock failed.
    Clock,
    /// The log, as the run read it.
    Log(LogFault),
    /// A stand's gate refused the run before T0: the journal's only line.
    Gate {
        /// Its name, a dotted name the host chooses.
        name: Code,
    },
}

/// How the log failed the run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFault {
    /// Its bytes against its commitment.
    Diverged,
    /// It ended before its records.
    Truncated,
    /// A record it could not read.
    Malformed,
    /// Its file.
    Io,
}

impl FaultClass {
    /// Whether it stops the run: every class but the backstop's.
    pub fn stops(&self) -> bool {
        !matches!(self, FaultClass::Backstop { .. })
    }

    /// The step it names, and the attempt, if any.
    pub fn struck(&self) -> Option<(Number, Option<u32>)> {
        match self {
            FaultClass::Backstop { line } => Some((*line, None)),
            FaultClass::Dropped { line, attempt }
            | FaultClass::Carrier { line, attempt, .. }
            | FaultClass::Held { line, attempt, .. } => Some((*line, Some(*attempt))),
            FaultClass::Below { .. }
            | FaultClass::Disordered { .. }
            | FaultClass::Backdated { .. }
            | FaultClass::Stalled
            | FaultClass::Poisoned
            | FaultClass::Saturated { .. }
            | FaultClass::Clock
            | FaultClass::Log(_)
            | FaultClass::Gate { .. } => None,
        }
    }
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
        wake: e.wake,
    }
}

/// The journal's lines of one machine step: the input it took at `at`, then
/// what it decided, in order. A line's bindings and an answer's being ignored
/// are told with their input; wakes, wants and wake-ups are not journaled.
///
/// Made one at a time, as they are taken (ADR-0047 decision 22, K2b-7e-2b-2b):
/// a run holds the line it sends, never every line of a burst at once.
pub fn events<'a>(
    at: u64,
    input: &'a Input,
    outputs: &'a [Output],
) -> impl Iterator<Item = Line> + 'a {
    std::iter::once_with(move || input_line(at, input, outputs))
        .flatten()
        .chain(outputs.iter().flat_map(move |o| output_lines(at, o)))
}

/// The line of the input a step took, if it is journaled.
fn input_line(at: u64, input: &Input, outputs: &[Output]) -> Option<Line> {
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
            // From a slice, at its exact length: a line's price counts its
            // dependencies, not a collection's growth.
            let after = bound
                .map_or(&[][..], |b| b.0.as_slice())
                .iter()
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
            Some(Line::Read(Read {
                at,
                line: *line,
                key: key_of(&record.step),
                action: (&record.action).into(),
                safety,
                cause,
                after,
                planned: record.at,
                refusal: record.deviation.as_ref().is_some_and(|d| d.rule == "rate"),
            }))
        }
        Input::Sent {
            line,
            attempt,
            decorated,
            handed,
            connected,
            lag,
        } => Some(Line::Sent(Sent {
            at,
            line: *line,
            attempt: *attempt,
            decorated: *decorated,
            handed: *handed,
            connected: *connected,
            lag: *lag,
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
            Some(Line::Answer(Answer {
                at,
                line: *line,
                attempt: *attempt,
                judged,
                captured,
                ignored,
                status: seen.status,
                code: seen.code.as_ref().map(Code::from),
                after: seen.after,
                censored: seen.censored,
                found: seen.found.map(|f| Found {
                    absent: f.absent,
                    deadline: f.deadline,
                    handed: f.handed,
                }),
                unsent: seen.unsent.as_ref().map(Unsent::from),
            }))
        }
        Input::Wake => None,
    }
}

/// The lines one output is journaled as: none, one, or a step's end and the
/// backstop's fault after it.
fn output_lines(at: u64, o: &Output) -> impl Iterator<Item = Line> {
    let (line, fault) = match o {
        Output::Released {
            line,
            intended,
            released,
            deadline,
        } => (
            Some(Line::Released(Released {
                at,
                line: *line,
                intended: *intended,
                released: *released,
                deadline: *deadline,
            })),
            None,
        ),
        Output::Dispatch {
            line,
            attempt,
            deadline,
            due,
            ..
        } => (
            Some(Line::Dispatch(Dispatch {
                at,
                line: *line,
                attempt: *attempt,
                deadline: *deadline,
                due: *due,
            })),
            None,
        ),
        Output::Ended(e) => (
            Some(Line::End(Box::new(ended(at, e)))),
            e.fault.then_some(Line::Fault(Fault {
                at,
                class: FaultClass::Backstop { line: e.line },
            })),
        ),
        Output::Bound { .. } | Output::Ignored { .. } | Output::WakeAt(_) | Output::Want(_) => {
            (None, None)
        }
    };
    line.into_iter().chain(fault)
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

/// The bytes a journal's writer buffers: a constant, however long a line
/// (ADR-0047 decision 22, K2b-7e-2b-2b), as the log's reader's `BUFFER`.
pub const BUFFER: usize = 8 << 10;

/// Writes a journal: its header, its lines, then its trailer at
/// [`finish`](Writer::finish) — each line streamed through [`BUFFER`] bytes,
/// whole in its output and the output flushed once written, no copy of it.
pub struct Writer<W: Write> {
    out: frame::Out<W>,
    events: u64,
    steps: u64,
}

impl<W: Write> Writer<W> {
    /// Starts a journal; refused if the header names another version.
    pub fn new(out: W, header: Header) -> Result<Writer<W>, JournalError> {
        if header.journal != JOURNAL_VERSION {
            return Err(JournalError::Version(header.journal));
        }
        let mut out = frame::Out::new(out, BUFFER);
        out.line(&Line::Header(Box::new(header)))?;
        Ok(Writer {
            out,
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
        self.out.line(line)?;
        self.events = self.events.saturating_add(1);
        Ok(())
    }

    /// Writes the trailer and hands back the output.
    pub fn finish(mut self) -> Result<(W, Trailer), JournalError> {
        let trailer = Trailer {
            events: self.events,
            steps: self.steps,
            sha256: self.out.digest().hex(),
        };
        self.out.line(&Line::Trailer(trailer.clone()))?;
        Ok((self.out.into_inner(), trailer))
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

/// A header's version alone, every other field ignored: what a reader takes
/// of line 1 before the schema, which is its version's (K2b-7e-2b-3d-3b).
#[derive(Deserialize)]
struct Versioned {
    header: Version,
}

#[derive(Deserialize)]
struct Version {
    journal: u32,
}

impl<R: BufRead> Reader<R> {
    /// Opens a journal; refused unless its first line is a header of this
    /// version — another version refused by its number, before its schema,
    /// which is that version's.
    pub fn new(mut input: R) -> Result<Reader<R>, JournalError> {
        let Some(bytes) = frame::next_line(&mut input)? else {
            return Err(JournalError::Header { line: 1 });
        };
        if let Ok(Versioned {
            header: Version { journal },
        }) = frame::peek::<Versioned>(&bytes, 1)
            && journal != JOURNAL_VERSION
        {
            return Err(JournalError::Version(journal));
        }
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

/// The most bytes a line of the journal's channel holds on the heap, any
/// line a run's machine makes about a record of at most `record`'s shape —
/// the plan's largest record (`Peaks::record`) — or about the harness (ADR-0047
/// decision 22, K2b-7e-2b-2b): the largest of a read's key and dependencies,
/// an answer's captured slots and its two codes, an end's box and the lines
/// its step dependencies bound, a fault's code. A header and a trailer never
/// enter the channel ([`Writer::line`] refuses them).
pub fn line_bound(record: &ascetic_ddd_command_log::shape::Shape) -> u64 {
    let bytes = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
    let code = bytes(machine::CODE);
    let read = record
        .key_bytes
        .saturating_add(bytes(size_of::<Dep>()).saturating_mul(record.deps.elements))
        .saturating_add(record.each_bytes);
    let answer = bytes(size_of::<Slot>())
        .saturating_mul(record.captures.elements)
        .saturating_add(record.capture_bytes)
        .saturating_add(code.saturating_mul(2));
    let end = bytes(size_of::<Ended>())
        .saturating_add(bytes(size_of::<Number>()).saturating_mul(record.deps.elements));
    read.max(answer).max(end).max(code)
}

/// The bytes `line` holds on the heap, exactly: its boxes, its strings and
/// sequences at their capacities. Every type taken apart whole, so a field
/// added is counted or does not compile.
pub fn line_heap(line: &Line) -> u64 {
    let mut n = Heap(0);
    match line {
        Line::Header(h) => {
            n.size::<Header>();
            n.header(h);
        }
        Line::Read(r) => n.read(r),
        Line::Released(Released {
            at: _,
            line: _,
            intended: _,
            released: _,
            deadline: _,
        })
        | Line::Dispatch(Dispatch {
            at: _,
            line: _,
            attempt: _,
            deadline: _,
            due: _,
        })
        | Line::Sent(Sent {
            at: _,
            line: _,
            attempt: _,
            decorated: _,
            handed: _,
            connected: _,
            lag: _,
        }) => {}
        Line::Answer(a) => n.answer(a),
        Line::End(e) => {
            n.size::<Ended>();
            n.ended(e);
        }
        Line::Fault(Fault { at: _, class }) => n.fault(class),
        Line::Sample(sample) => {
            n.size::<Sample>();
            if let Some(name) = &sample.allocator.name {
                n.code(name);
            }
        }
        Line::Trailer(Trailer {
            events: _,
            steps: _,
            sha256,
        }) => n.owned(sha256),
    }
    n.0
}

/// A heap's bytes, walked.
struct Heap(u64);

impl Heap {
    fn add(&mut self, bytes: usize) {
        self.0 = self
            .0
            .saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX));
    }

    fn size<T>(&mut self) {
        self.add(size_of::<T>());
    }

    fn vec<T>(&mut self, v: &Vec<T>) {
        self.add(v.capacity().saturating_mul(size_of::<T>()));
    }

    fn code(&mut self, c: &Code) {
        match c {
            Code::Named(name) => self.owned(name),
            Code::Other => {}
        }
    }

    fn owned(&mut self, s: &String) {
        self.add(s.capacity());
    }

    fn subject(&mut self, s: &Subject) {
        let Subject {
            kind,
            scope,
            index: _,
        } = s;
        self.owned(kind);
        self.vec(scope);
    }

    fn key(&mut self, k: &Key) {
        let Key {
            kind,
            scope,
            label,
            n: _,
        } = k;
        self.owned(kind);
        self.vec(scope);
        self.owned(label);
    }

    fn header(&mut self, h: &Header) {
        let Header {
            journal: _,
            machine: _,
            source,
            log,
            validator,
            read_ahead: _,
            jitter: _,
            resend: _,
            catch_up: _,
            namespace: _,
            t0: _,
            gates,
            judging,
            admission,
        } = h;
        self.owned(source);
        self.owned(log);
        self.owned(validator);
        self.vec(gates);
        for Gate { name, took: _ } in gates {
            self.code(name);
        }
        if let Some(Judging {
            cap: _,
            took: _,
            shape: _,
        }) = judging
        {}
        if let Some(Admission {
            sources,
            descriptors: _,
            assumed: _,
            memory:
                MemoryFit {
                    stated: _,
                    origin: _,
                    limit: _,
                    model: _,
                    terms,
                    unheld,
                    budget: _,
                    pool: _,
                    need: _,
                    slack: _,
                },
        }) = admission
        {
            self.vec(sources);
            self.vec(terms);
            self.vec(unheld);
        }
    }

    fn read(&mut self, r: &Read) {
        let Read {
            at: _,
            line: _,
            key,
            action: _,
            safety: _,
            cause: _,
            after,
            planned: _,
            refusal: _,
        } = r;
        self.key(key);
        self.vec(after);
        for d in after {
            match d {
                Dep::Step { on: _, line: _ } => {}
                Dep::Each {
                    on: _,
                    key,
                    members: _,
                } => self.key(key),
            }
        }
    }

    fn answer(&mut self, a: &Answer) {
        let Answer {
            at: _,
            line: _,
            attempt: _,
            judged: _,
            captured,
            ignored: _,
            status: _,
            code,
            after: _,
            censored: _,
            found: _,
            unsent,
        } = a;
        self.vec(captured);
        for Slot { subject, slot } in captured {
            self.subject(subject);
            self.owned(slot);
        }
        if let Some(c) = code {
            self.code(c);
        }
        if let Some(Unsent {
            decorated: _,
            handed: _,
            connected: _,
            lag: _,
            refused: _,
            refused_by: _,
            code,
        }) = unsent
        {
            self.code(code);
        }
    }

    fn ended(&mut self, e: &Ended) {
        let Ended {
            at: _,
            line: _,
            end: _,
            read: _,
            ready: _,
            intended: _,
            released: _,
            dispatched: _,
            sent: _,
            ended: _,
            attempts: _,
            critical: _,
            bound,
            fault: _,
            wake: _,
        } = e;
        self.vec(bound);
    }

    fn fault(&mut self, class: &FaultClass) {
        match class {
            FaultClass::Carrier {
                line: _,
                attempt: _,
                cannot,
            } => self.code(cannot),
            FaultClass::Gate { name } => self.code(name),
            FaultClass::Backstop { .. }
            | FaultClass::Below { .. }
            | FaultClass::Disordered { .. }
            | FaultClass::Backdated { .. }
            | FaultClass::Dropped { .. }
            | FaultClass::Held { .. }
            | FaultClass::Stalled
            | FaultClass::Poisoned
            | FaultClass::Saturated { .. }
            | FaultClass::Clock
            | FaultClass::Log(_) => {}
        }
    }
}
