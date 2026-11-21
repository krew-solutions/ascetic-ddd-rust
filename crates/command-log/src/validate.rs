//! The validator (ADR-0046 decisions 9 and 16): one streaming pass over a log
//! on one clock, the read cursor — the running maximum of `at` — behind which
//! the window is measured and evicted exactly, and a budget of step ids past
//! which uniqueness is decided by an external sort. Streams, slots, barrier
//! counts, buckets and declared deviations are held whole, unbounded until
//! decision 16's later slices.
//!
//! Framing — the header, canonical lines, the trailer — is the [`Reader`]'s,
//! and fails the pass. Every other rule is collected as a [`Violation`], so one
//! pass reports them all. `vectors/reference.py` decides the same rules from
//! their definitions, quadratically, and is compared with this on a corpus.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fmt;
use std::fs;
use std::io::{BufRead, Read, Write};
use std::path::{Path, PathBuf};

use crate::disk::{Disk, LocalDisk};
use crate::log::{LogError, Reader};
use crate::rate::{self, Curve, Lower};
use crate::record::{
    Action, Bus, By, Codec, Dep, Field, Form, Header, Hole, Limit, Observe, Of, On, Outcome,
    Payload, Place, Predicate, Probe, Record, Rest, Span, StepId, Subject, Test, Value,
};
use crate::runs::{Event, Sorter};
use crate::spill::{Counted, SpillDir};

/// A rule of decisions 5, 7 and 9.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Rule {
    /// The header contradicts itself: a kind both rendered and captured, an
    /// operation named twice.
    Header,
    /// `(at, StepId)` not strictly above the line before.
    Order,
    /// A step id a second time.
    Duplicate,
    /// A dependency on no step planned earlier within the window `W`, or of a
    /// retained label.
    Edge,
    /// A barrier's `count` other than the steps it names planned before it.
    Barrier,
    /// A captured value whose producer is not awaited as expected.
    Producer,
    /// A captured value read from a step that is not the slot's latest writer.
    Stale,
    /// A slot's writer that does not await the previous writer's end and each
    /// of its readers.
    Writers,
    /// A stream position out of `[1, 2³¹ − 1]`, not above the one before, or
    /// whose predecessor is not awaited as expected.
    Stream,
    /// A gap on a stream the profile declares gapless.
    Gap,
    /// A span outside its payload, overlapping another, in a form its codec
    /// cannot carry, or that the host finds is not its field.
    Span,
    /// An id rendered for a kind the profile does not render, or captured for a
    /// kind it does not capture.
    Profile,
    /// An operation not in the table, a path of another arity, an observation
    /// that polls every 0.
    Operation,
    /// A dependency the host's domain requires, not awaited.
    Required,
    /// A deviation that names no rule, or breaks none.
    Deviation,
    /// An arrival a bucket's curve refuses (decision 14).
    Rate,
    /// A limit's value a call does not give; no deviation exempts it.
    Key,
    /// A step planned before its dependencies and its think time allow: a
    /// release is never earlier than planned (ADR-0047 decision 3).
    Feasible,
    /// A measure from no step planned on an earlier line.
    Measure,
    /// An observation whose cause it does not await, nor is.
    Cause,
    /// An observation of the bus whose `until` does not tell its cause's fact
    /// from another's: no `Equals` on a value of the cause (ADR-0047 § 12).
    Until,
    /// A step that awaits a dependency only sent and carries a measure or an
    /// expectation: sent is the harness's order, not the server's (ADR-0047
    /// decision 6).
    Sent,
}

/// The rules no deviation exempts: the scheduler's premises, a key, a
/// deviation itself (ADR-0047 decision 13).
const NON_DEVIABLE: [Rule; 6] = [
    Rule::Order,
    Rule::Duplicate,
    Rule::Edge,
    Rule::Producer,
    Rule::Key,
    Rule::Deviation,
];

impl Rule {
    /// Every rule, in order.
    pub const ALL: [Rule; 22] = [
        Rule::Header,
        Rule::Order,
        Rule::Duplicate,
        Rule::Edge,
        Rule::Barrier,
        Rule::Producer,
        Rule::Stale,
        Rule::Writers,
        Rule::Stream,
        Rule::Gap,
        Rule::Span,
        Rule::Profile,
        Rule::Operation,
        Rule::Required,
        Rule::Deviation,
        Rule::Rate,
        Rule::Key,
        Rule::Feasible,
        Rule::Measure,
        Rule::Cause,
        Rule::Until,
        Rule::Sent,
    ];

    /// The rule's name, as a deviation names it.
    pub fn name(self) -> &'static str {
        match self {
            Rule::Header => "header",
            Rule::Order => "order",
            Rule::Duplicate => "duplicate",
            Rule::Edge => "edge",
            Rule::Barrier => "barrier",
            Rule::Producer => "producer",
            Rule::Stale => "stale",
            Rule::Writers => "writers",
            Rule::Stream => "stream",
            Rule::Gap => "gap",
            Rule::Span => "span",
            Rule::Profile => "profile",
            Rule::Operation => "operation",
            Rule::Required => "required",
            Rule::Deviation => "deviation",
            Rule::Rate => "rate",
            Rule::Key => "key",
            Rule::Feasible => "feasible",
            Rule::Measure => "measure",
            Rule::Cause => "cause",
            Rule::Until => "until",
            Rule::Sent => "sent",
        }
    }

    /// The rule a name names.
    pub fn named(name: &str) -> Option<Rule> {
        Rule::ALL.into_iter().find(|r| r.name() == name)
    }
}

impl fmt::Display for Rule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A rule a line breaks.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Violation {
    /// The line, the header's being 1.
    pub line: u64,
    /// The rule.
    pub rule: Rule,
    /// What breaks it.
    pub detail: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}: {}: {}", self.line, self.rule, self.detail)
    }
}

/// What a run must hold, as the log plans it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Peaks {
    /// The most completions kept at once for later dependencies: the steps
    /// whose latest occurrence is within `W` behind the cursor, and the
    /// retained.
    pub completions: u64,
    /// The most captured slots live at once: those whose latest writer is
    /// within `W` behind the cursor, or retained.
    pub captures: u64,
    /// The polls the observations imply, summed over the log (not at once):
    /// each one's timeout over its interval, and one.
    pub polls: u64,
}

/// What a pass found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Report {
    /// The steps read.
    pub records: u64,
    /// The least [`Limits::kept`] violations by (line, rule, detail), in that
    /// order.
    pub violations: Vec<Violation>,
    /// How many there were, kept or not.
    pub violated: u64,
    /// The peaks.
    pub peaks: Peaks,
}

/// What only the host knows.
pub trait Host {
    /// Whether `span` holds exactly the bytes of the field it names in
    /// `payload`: the host decodes its own message.
    fn span(&self, payload: &Payload, span: &Span) -> Result<(), String> {
        let _ = (payload, span);
        Ok(())
    }

    /// The steps `record` must await, and how, that only the host's domain
    /// knows: the step that opened a Device's life before a fact of it, a
    /// message's declared causal dependency.
    fn required(&self, record: &Record) -> Vec<(StepId, On)> {
        let _ = record;
        Vec::new()
    }
}

/// A host that knows nothing more.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoHost;

impl Host for NoHost {}

/// The memory a pass may use.
#[derive(Clone, Debug)]
pub struct Limits {
    /// Bytes of events — step ids, stream messages — held in memory before the
    /// larger buffer goes to disk as a sorted run (decision 16). At the end
    /// the same memory reads the runs back, a block a run.
    pub memory: usize,
    /// Where runs go: a directory of the caller's, its files removed after.
    pub spill: PathBuf,
    /// Violations kept in the report.
    pub kept: usize,
    /// The most runs a merge holds open; 0 derives it from the open-file
    /// limit ([`crate::disk::fan_in`]). Never more than `memory` reads a block
    /// of at a time.
    pub fan_in: usize,
    /// Whether `spill` may be memory (tmpfs): refused unless allowed.
    pub memory_spill: bool,
    /// Who hears the pass's runs, if anyone.
    pub progress: Option<crate::engine::Observer>,
    /// The most entries a hot part may hold — the completions kept, captured
    /// slots, barrier counts, buckets, declared deviations, polls in flight —
    /// past which the pass fails by name (decision 16).
    pub hot: u64,
}

impl Limits {
    /// 256 MiB of events in memory, spills under `spill`, a merge as wide as
    /// the open-file limit and the memory allow, no spill in memory, hot parts
    /// of at most 2²⁵ entries each — some 2.7 GB of completions at about 80
    /// bytes an entry.
    pub fn new(spill: impl Into<PathBuf>) -> Limits {
        Limits {
            memory: 256 << 20,
            spill: spill.into(),
            kept: 10_000,
            fan_in: 0,
            memory_spill: false,
            progress: None,
            hot: 32 << 20,
        }
    }
}

/// The disk a validation needs for its runs, given the bytes of events its
/// records make ([`run_bytes`]), `fan_in` wide: none while they fit in memory.
pub(crate) fn spill_need(bytes: u64, limits: &Limits, fan_in: usize) -> u64 {
    let memory = limits.memory.max(1) as u64;
    if bytes <= memory {
        return 0;
    }
    // A run holds at least half the memory: the larger buffer goes.
    crate::runs::need(bytes, memory.div_ceil(2), fan_in)
}

/// The most runs a pass's merge holds open: as `limits` says or the open-file
/// limit allows, and a block a run within the memory.
pub(crate) fn fan_in(limits: &Limits, disk: &dyn Disk) -> std::io::Result<usize> {
    let wide = match limits.fan_in {
        0 => crate::disk::fan_in(disk.open_files()?),
        n => n.max(2),
    };
    Ok(wide.min((limits.memory / crate::runs::READ_BUFFER).max(2)))
}

/// The bytes of events a record makes in the validator's runs: its id, and a
/// stream message's event.
pub(crate) fn run_bytes(r: &Record) -> u64 {
    let id = (IdEvent::SIZE as u64).saturating_mul((r.measure.len() as u64).saturating_add(1));
    match stream_of(r) {
        Some(_) => id.saturating_add(StreamEvent::size_of(predecessors(r).count()) as u64),
        None => id,
    }
}

/// The stream a message is on, if it is one.
fn stream_of(r: &Record) -> Option<&crate::record::StreamPosition> {
    match &r.action {
        Action::Bus(Bus {
            stream: Some(s), ..
        }) => Some(s),
        _ => None,
    }
}

/// The steps a record awaits as expected on its own subject: the only ones
/// that may be its stream's message before it.
fn predecessors(r: &Record) -> impl Iterator<Item = &StepId> {
    r.after.iter().filter_map(move |d| match d {
        Dep::Step { step, on } if *on == On::Expected && step.subject == r.step.subject => {
            Some(step)
        }
        _ => None,
    })
}

/// A step id's digest in the validator's runs.
fn id_digest(step: &StepId) -> Result<[u8; 16], LogError> {
    let bytes = serde_json::to_vec(step).map_err(|e| LogError::Encode(e.to_string()))?;
    Ok(crate::runs::digest(&bytes))
}

/// A step id seen, for uniqueness, or a measure's `from` asked for: its
/// digest and line. Sorted by digest and line, a query follows the ids of
/// its line, so that only an earlier line's id answers it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct IdEvent {
    digest: [u8; 16],
    line: u64,
    query: bool,
}

impl IdEvent {
    const SIZE: usize = 25;
}

impl Event for IdEvent {
    fn size(&self) -> usize {
        IdEvent::SIZE
    }

    fn write(&self, out: &mut dyn Write) -> std::io::Result<()> {
        out.write_all(&self.digest)?;
        out.write_all(&self.line.to_le_bytes())?;
        out.write_all(&[u8::from(self.query)])
    }

    fn read<R: Read>(run: &mut Counted<R>) -> std::io::Result<IdEvent> {
        let mut buf = [0u8; IdEvent::SIZE];
        run.exact(&mut buf)?;
        let (d, rest) = buf.split_at(16);
        let (l, q) = rest.split_at(8);
        let mut digest = [0u8; 16];
        digest.copy_from_slice(d);
        let mut line = [0u8; 8];
        line.copy_from_slice(l);
        Ok(IdEvent {
            digest,
            line: u64::from_le_bytes(line),
            query: q == [1],
        })
    }
}

/// A stream's message, for the stream rules decided at the end: replayed per
/// stream in line order, as the reference defines them.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct StreamEvent {
    /// The digest of (subject, stream).
    key: [u8; 16],
    line: u64,
    position: u32,
    gapless: bool,
    step: [u8; 16],
    /// The digests of the steps it awaits as expected on its subject.
    deps: Vec<[u8; 16]>,
}

impl StreamEvent {
    const FIXED: usize = 16 + 8 + 4 + 1 + 16 + 4;

    fn size_of(deps: usize) -> usize {
        StreamEvent::FIXED.saturating_add(deps.saturating_mul(16))
    }
}

impl Event for StreamEvent {
    fn size(&self) -> usize {
        StreamEvent::size_of(self.deps.len())
    }

    fn write(&self, out: &mut dyn Write) -> std::io::Result<()> {
        out.write_all(&self.key)?;
        out.write_all(&self.line.to_le_bytes())?;
        out.write_all(&self.position.to_le_bytes())?;
        out.write_all(&[u8::from(self.gapless)])?;
        out.write_all(&self.step)?;
        let n = u32::try_from(self.deps.len()).map_err(std::io::Error::other)?;
        out.write_all(&n.to_le_bytes())?;
        for d in &self.deps {
            out.write_all(d)?;
        }
        Ok(())
    }

    fn read<R: Read>(run: &mut Counted<R>) -> std::io::Result<StreamEvent> {
        let mut buf = [0u8; StreamEvent::FIXED];
        run.exact(&mut buf)?;
        let take = |at: usize, n: usize| buf.get(at..at.saturating_add(n)).unwrap_or(&[]);
        let digest = |at: usize| {
            let mut d = [0u8; 16];
            d.copy_from_slice(take(at, 16));
            d
        };
        let mut line = [0u8; 8];
        line.copy_from_slice(take(16, 8));
        let mut position = [0u8; 4];
        position.copy_from_slice(take(24, 4));
        let mut n = [0u8; 4];
        n.copy_from_slice(take(45, 4));
        let mut deps = Vec::new();
        for _ in 0..u32::from_le_bytes(n) {
            let mut d = [0u8; 16];
            run.exact(&mut d)?;
            deps.push(d);
        }
        Ok(StreamEvent {
            key: digest(0),
            line: u64::from_le_bytes(line),
            position: u32::from_le_bytes(position),
            gapless: take(28, 1) == [1],
            step: digest(29),
            deps,
        })
    }
}

/// How strongly a step is awaited: an `Expected` wait is also a settled one,
/// and either is also a start.
fn strength(on: On) -> u8 {
    match on {
        On::Sent => 0,
        On::Settled => 1,
        On::Expected => 2,
    }
}

/// Whether `record` awaits `step` at least as `on`.
fn awaits(record: &Record, step: &StepId, on: On) -> bool {
    record.after.iter().any(|d| match d {
        Dep::Step { step: s, on: o } => s == step && strength(*o) >= strength(on),
        Dep::Each { .. } => false,
    })
}

/// Every hole a record holds, in its values and its payloads' spans.
pub(crate) fn holes(record: &Record) -> Vec<&Hole> {
    fn value<'a>(v: &'a Value, out: &mut Vec<&'a Hole>) {
        if let Value::Hole(h) = v {
            out.push(h);
        }
    }
    fn fields<'a>(fs: &'a [Field], out: &mut Vec<&'a Hole>) {
        for f in fs {
            value(&f.value, out);
        }
    }
    fn payload<'a>(p: &'a Payload, out: &mut Vec<&'a Hole>) {
        out.extend(p.holes.iter().map(|s| &s.hole));
    }
    fn rest<'a>(r: &'a Rest, out: &mut Vec<&'a Hole>) {
        r.path.iter().for_each(|v| value(v, out));
        fields(&r.query, out);
        fields(&r.headers, out);
        if let Some(b) = &r.body {
            payload(b, out);
        }
    }
    fn predicates<'a>(ps: &'a [Predicate], out: &mut Vec<&'a Hole>) {
        for p in ps {
            match &p.test {
                Test::Exists => {}
                Test::Equals(v) | Test::Contains(v) => value(v, out),
            }
        }
    }
    let mut out = Vec::new();
    match &record.action {
        Action::Rest(r) => rest(r, &mut out),
        Action::Bus(Bus {
            key,
            headers,
            payload: p,
            ..
        }) => {
            value(key, &mut out);
            fields(headers, &mut out);
            payload(p, &mut out);
        }
        Action::Observe(o) => {
            match &o.probe {
                Probe::Rest(r) => rest(r, &mut out),
                Probe::Bus { key, .. } => value(key, &mut out),
            }
            predicates(&o.until, &mut out);
        }
        Action::Host(h) => payload(&h.args, &mut out),
    }
    for e in &record.expect {
        if let Outcome::Response { body, .. } = e {
            predicates(body, &mut out);
        }
    }
    out
}

/// Every payload a record carries.
fn payloads(record: &Record) -> Vec<&Payload> {
    match &record.action {
        Action::Rest(r) => r.body.iter().collect(),
        Action::Bus(b) => vec![&b.payload],
        Action::Observe(o) => match &o.probe {
            Probe::Rest(r) => r.body.iter().collect(),
            Probe::Bus { .. } => Vec::new(),
        },
        Action::Host(h) => vec![&h.args],
    }
}

/// The REST calls a record makes: its action's, its probe's.
fn calls(record: &Record) -> Vec<&Rest> {
    match &record.action {
        Action::Rest(r) => vec![r],
        Action::Observe(o) => match &o.probe {
            Probe::Rest(r) => vec![r],
            Probe::Bus { .. } => Vec::new(),
        },
        Action::Bus(_) | Action::Host(_) => Vec::new(),
    }
}

/// A slot's latest writer and who has read from it since.
struct Slot {
    writer: StepId,
    readers: BTreeSet<StepId>,
}

/// A barrier's key: the kind, scope, label and occurrence its members share.
type BarrierKey = (String, Vec<u8>, String, u32);

struct Pass<'h, H: Host> {
    host: &'h H,
    disk: &'h dyn Disk,
    /// [`Limits::memory`].
    memory: usize,
    header: Header,
    kept: usize,
    report: Report,
    previous: Option<(u64, StepId)>,
    /// The steps within W behind the cursor, by their ids' digests.
    window: BTreeMap<[u8; 16], u64>,
    /// The window's steps by planned time: evicted exactly, whatever the
    /// lines' order.
    order: BTreeSet<(u64, [u8; 16])>,
    /// The retained steps, by their ids' digests.
    retained: BTreeMap<[u8; 16], u64>,
    /// [`Limits::hot`].
    hot: u64,
    /// A barrier key's members planned so far, and the latest `at` of them.
    barriers: BTreeMap<BarrierKey, (u64, u64)>,
    slots: BTreeMap<(Subject, String), Slot>,
    /// Live captures: slots whose latest writer is within W of the cursor
    /// (its `at`) or retained (`None`).
    live: BTreeMap<(Subject, String), Option<u64>>,
    /// The live captures by their writer's planned time, for eviction.
    live_order: BTreeSet<(u64, (Subject, String))>,
    /// The least violations found, at most `kept`: a max-heap.
    least: BinaryHeap<Violation>,
    /// Stream messages, decided at the end by stream.
    streams: Sorter<StreamEvent>,
    /// Step ids, for uniqueness, decided at the end.
    ids: Sorter<IdEvent>,
    /// Lines that declared a deviation, its rule, and whether it was used.
    deviations: BTreeMap<u64, (Rule, bool)>,
    /// The read cursor: the running maximum of `at` up to the line in hand
    /// (decision 16). W is measured behind it; a line's calls arrive at it.
    cursor: u64,
    /// The header's refused limits, by operation and index: never evaluated.
    refused: BTreeSet<(String, usize)>,
    /// Each bucket's curve and lower model, by its name and value.
    buckets: BTreeMap<(String, Vec<u8>), (Curve, Lower)>,
    /// Observations' polls still to arrive, in (time, line) order.
    polls: BinaryHeap<Reverse<Polling>>,
}

/// An observation's polls still to arrive, ordered by their next poll's (time,
/// line).
#[derive(Clone, Debug)]
struct Polling {
    next: u64,
    line: u64,
    k: u64,
    last: u64,
    every: u64,
    forced: bool,
    counts: Vec<Count>,
}

impl PartialEq for Polling {
    fn eq(&self, other: &Polling) -> bool {
        (self.next, self.line, self.k) == (other.next, other.line, other.k)
    }
}

impl Eq for Polling {}

impl PartialOrd for Polling {
    fn partial_cmp(&self, other: &Polling) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Polling {
    fn cmp(&self, other: &Polling) -> std::cmp::Ordering {
        (self.next, self.line, self.k).cmp(&(other.next, other.line, other.k))
    }
}

/// A bucket a call counts against: its name and value, and its limit.
#[derive(Clone, Debug)]
struct Count {
    bucket: String,
    value: Vec<u8>,
    limit: Limit,
}

/// The most polls an observation may imply.
const MAX_POLLS: u64 = 1 << 20;

impl<H: Host> Pass<'_, H> {
    /// A violation of `rule` at `line`, unless the line declared it. The
    /// report keeps the least `kept` by (line, rule, detail), whenever each is
    /// found; a detail is written only if it could be kept.
    fn violate(&mut self, line: u64, rule: Rule, detail: impl FnOnce() -> String) {
        // No deviation exempts the scheduler's premises — order, uniqueness,
        // edges, producers — nor a key — an arrival left out of its bucket
        // would be an under-count — nor itself: one declared against them is
        // unused, and says so.
        if let Some((declared, used)) = self.deviations.get_mut(&line)
            && *declared == rule
            && !NON_DEVIABLE.contains(&rule)
        {
            *used = true;
            return;
        }
        self.report.violated = self.report.violated.saturating_add(1);
        if self.kept == 0 {
            return;
        }
        if self.least.len() < self.kept {
            self.least.push(Violation {
                line,
                rule,
                detail: detail(),
            });
            return;
        }
        let Some(most) = self.least.peek() else {
            return;
        };
        if (line, rule) > (most.line, most.rule) {
            return;
        }
        let v = Violation {
            line,
            rule,
            detail: detail(),
        };
        if &v < most {
            self.least.pop();
            self.least.push(v);
        }
    }

    fn header(&mut self) {
        let profile = &self.header.profile;
        let both: Vec<String> = profile
            .rendered
            .iter()
            .filter(|k| profile.captured.contains(k))
            .cloned()
            .collect();
        let mut names = BTreeSet::new();
        let twice: Vec<String> = self
            .header
            .operations
            .iter()
            .filter(|o| !names.insert(o.name.clone()))
            .map(|o| o.name.clone())
            .collect();
        for k in both {
            self.violate(1, Rule::Header, || {
                format!("kind {k:?} both rendered and captured")
            });
        }
        for o in twice {
            self.violate(1, Rule::Header, || format!("operation {o:?} twice"));
        }
        // Every action has a deadline; an operation's lies below the stand's
        // route timeout by more than the jitter, so that a slow call cannot
        // end either censored or as the gateway's own answer by microseconds.
        let (route, jitter) = (
            self.header.profile.route_timeout,
            self.header.profile.jitter,
        );
        let mut found = Vec::new();
        for o in &self.header.operations {
            if o.timeout == 0 || o.timeout.saturating_add(jitter) >= route {
                found.push(format!(
                    "operation {:?}: a timeout of {} µs, not within (0, {route} − {jitter})",
                    o.name, o.timeout
                ));
            }
        }
        let mut channels = BTreeSet::new();
        for c in &self.header.channels {
            if !channels.insert(c.name.clone()) {
                found.push(format!("channel {:?} twice", c.name));
            }
            if c.timeout == 0 {
                found.push(format!("channel {:?}: no timeout", c.name));
            }
        }
        let mut calls = BTreeSet::new();
        for c in &self.header.calls {
            if !calls.insert(c.name.clone()) {
                found.push(format!("host call {:?} twice", c.name));
            }
            if c.timeout == 0 {
                found.push(format!("host call {:?}: no timeout", c.name));
            }
        }
        for detail in found {
            self.violate(1, Rule::Header, || detail);
        }
        let (found, refused) = self.limits();
        self.refused = refused;
        for detail in found {
            self.violate(1, Rule::Header, || detail);
        }
    }

    /// What is wrong with the header's limits — a zero parameter, a `Path` that
    /// names no parameter, one bucket of two shapes, no source, a reserve of all
    /// — and the limits refused, which are never evaluated: by operation and
    /// index, every limit of a bucket of two shapes among them.
    fn limits(&self) -> (Vec<String>, BTreeSet<(String, usize)>) {
        let mut found = Vec::new();
        let mut refused = BTreeSet::new();
        let mut misshapen = BTreeSet::new();
        let profile = &self.header.profile;
        if profile.sources == 0 {
            found.push("no source".to_owned());
        }
        if profile.reserve >= 1000 {
            found.push(format!("a reserve of {} per thousand", profile.reserve));
        }
        let mut shapes: BTreeMap<&str, &Limit> = BTreeMap::new();
        let shape = |l: &Limit| (l.by.clone(), l.requests, l.per_seconds, l.burst);
        for op in &self.header.operations {
            for (i, l) in op.limits.iter().enumerate() {
                if !rate::valid(l) {
                    found.push(format!("{} {}: a zero parameter", op.name, l.key));
                    refused.insert((op.name.clone(), i));
                }
                if let By::At(Place::Path(name)) = &l.by
                    && !rate::parameters(&op.path).contains(name)
                {
                    found.push(format!(
                        "{} {}: {{{name}}} is no parameter of {}",
                        op.name, l.key, op.path
                    ));
                    refused.insert((op.name.clone(), i));
                }
                match shapes.get(l.bucket.as_str()) {
                    Some(first) if shape(first) != shape(l) => {
                        found.push(format!("bucket {:?} of two shapes", l.bucket));
                        misshapen.insert(l.bucket.clone());
                    }
                    Some(_) => {}
                    None => {
                        shapes.insert(&l.bucket, l);
                    }
                }
            }
        }
        for op in &self.header.operations {
            for (i, l) in op.limits.iter().enumerate() {
                if misshapen.contains(&l.bucket) {
                    refused.insert((op.name.clone(), i));
                }
            }
        }
        (found, refused)
    }

    /// The buckets a call counts against; a limit it gives no value for is a
    /// `key` violation, and counts nowhere.
    fn counts(&mut self, line: u64, rest: &Rest) -> Vec<Count> {
        let Some(op) = self.header.operations.iter().find(|o| o.name == rest.op) else {
            return Vec::new();
        };
        let sources = self.header.profile.sources;
        let mut out = Vec::new();
        let mut missing = Vec::new();
        let refused = &self.refused;
        let limits = op
            .limits
            .iter()
            .enumerate()
            .filter(|(i, _)| !refused.contains(&(op.name.clone(), *i)))
            .map(|(_, l)| l);
        for l in limits {
            match rate::value(l, op, rest, sources) {
                Ok(value) => out.push(Count {
                    bucket: l.bucket.clone(),
                    value,
                    limit: l.clone(),
                }),
                Err(reason) => missing.push(format!("{} {}: {reason}", op.name, l.key)),
            }
        }
        for detail in missing {
            self.violate(line, Rule::Key, || detail);
        }
        out
    }

    /// An arrival at `t` from `line` in every bucket it counts against. A
    /// deliberate refusal spends, and is used only if the lower model refuses it.
    fn arrive(&mut self, t: u64, line: u64, forced: bool, counts: &[Count]) {
        let (jitter, reserve) = (self.header.profile.jitter, self.header.profile.reserve);
        for c in counts {
            let key = (c.bucket.clone(), c.value.clone());
            if !self.buckets.contains_key(&key) {
                let (Some(curve), Some(lower)) = (
                    Curve::new(&c.limit, jitter, reserve),
                    Lower::new(&c.limit, jitter),
                ) else {
                    continue;
                };
                self.buckets.insert(key.clone(), (curve, lower));
            }
            let Some((curve, lower)) = self.buckets.get_mut(&key) else {
                continue;
            };
            let admitted = curve.arrive(t, forced);
            let certain = !lower.arrive(t);
            if forced {
                if certain && let Some((Rule::Rate, used)) = self.deviations.get_mut(&line) {
                    *used = true;
                }
            } else if !admitted {
                let detail = format!("{} {} at {t}: past its curve", c.bucket, c.limit.key);
                self.violate(line, Rule::Rate, || detail);
            }
        }
    }

    /// The polls due by `t`, in (time, line) order.
    fn drain(&mut self, t: u64) {
        while let Some(Reverse(p)) = self.polls.peek()
            && p.next <= t
        {
            let Some(Reverse(mut p)) = self.polls.pop() else {
                break;
            };
            let counts = std::mem::take(&mut p.counts);
            self.arrive(p.next, p.line, p.forced, &counts);
            p.counts = counts;
            if p.k < p.last {
                p.k = p.k.saturating_add(1);
                p.next = p.next.saturating_add(p.every);
                self.polls.push(Reverse(p));
            }
        }
    }

    /// The line's calls: a REST action at the cursor, an observation's polls
    /// from it, every `every` to its timeout.
    fn offered(&mut self, line: u64, r: &Record) {
        let t = self.cursor;
        self.drain(t);
        let forced = matches!(self.deviations.get(&line), Some((Rule::Rate, _)));
        match &r.action {
            Action::Rest(rest) => {
                let counts = self.counts(line, rest);
                self.arrive(t, line, forced, &counts);
            }
            Action::Observe(o) => {
                if let Probe::Rest(rest) = &o.probe {
                    let counts = self.counts(line, rest);
                    self.arrive(t, line, forced, &counts);
                    let polls = o.timeout.checked_div(o.every).filter(|n| *n < MAX_POLLS);
                    if let Some(last) = polls
                        && last > 0
                        && !counts.is_empty()
                    {
                        self.polls.push(Reverse(Polling {
                            next: t.saturating_add(o.every),
                            line,
                            k: 1,
                            last,
                            every: o.every,
                            forced,
                            counts,
                        }));
                    }
                }
            }
            Action::Bus(_) | Action::Host(_) => {}
        }
    }

    fn record(&mut self, line: u64, r: &Record) -> Result<(), LogError> {
        self.report.records = self.report.records.saturating_add(1);
        self.cursor = self.cursor.max(r.at);
        if let Some(d) = &r.deviation {
            match Rule::named(&d.rule) {
                Some(rule) => {
                    self.deviations.insert(line, (rule, false));
                }
                None => self.violate(line, Rule::Deviation, || {
                    format!("{:?} names no rule", d.rule)
                }),
            }
        }
        self.order(line, r);
        let digest = id_digest(&r.step)?;
        self.ids.push(IdEvent {
            digest,
            line,
            query: false,
        });
        for m in &r.measure {
            self.ids.push(IdEvent {
                digest: id_digest(&m.from)?,
                line,
                query: true,
            });
        }
        self.hold()?;
        self.evict();
        self.edges(line, r)?;
        self.captures(line, r);
        self.stream(line, r)?;
        self.observation(line, r);
        self.sent_only(line, r);
        self.spans(line, r);
        self.profile(line, r);
        self.operations(line, r);
        for (step, on) in self.host.required(r) {
            if !awaits(r, &step, on) {
                self.violate(line, Rule::Required, || {
                    format!("{step:?} not awaited as {on:?}")
                });
            }
        }
        self.offered(line, r);
        self.keep(r, digest);
        self.within(line)
    }

    /// Every hot part within [`Limits::hot`] entries: past it the pass fails,
    /// naming the part, rather than grow until the machine kills it.
    fn within(&self, line: u64) -> Result<(), LogError> {
        let parts = [
            (
                "completions",
                self.window.len().saturating_add(self.retained.len()),
            ),
            ("captures", self.slots.len()),
            ("barrier counts", self.barriers.len()),
            ("buckets", self.buckets.len()),
            ("deviations", self.deviations.len()),
            ("polls", self.polls.len()),
        ];
        match parts.into_iter().find(|(_, n)| *n as u64 > self.hot) {
            Some((part, n)) => Err(LogError::Hot {
                part,
                held: n as u64,
                line,
            }),
            None => Ok(()),
        }
    }

    fn order(&mut self, line: u64, r: &Record) {
        let key = (r.at, r.step.clone());
        if let Some(previous) = &self.previous
            && &key <= previous
        {
            self.violate(line, Rule::Order, || {
                format!("({}, {:?}) not above the line before", r.at, r.step)
            });
        }
        self.previous = Some(key);
    }

    fn edges(&mut self, line: u64, r: &Record) -> Result<(), LogError> {
        for d in &r.after {
            match d {
                Dep::Step { step, .. } => {
                    // Eviction behind the cursor is exact (decision 16): a step
                    // in the window is within W of it.
                    let at_l = r.at;
                    let digest = id_digest(step)?;
                    let found = self
                        .retained
                        .get(&digest)
                        .or_else(|| self.window.get(&digest))
                        .copied();
                    let think = r.think;
                    match found {
                        Some(at) if at >= at_l => self.violate(line, Rule::Edge, || {
                            format!("{step:?} planned at {at}, not before {at_l}")
                        }),
                        Some(at) if at.saturating_add(think) > at_l => {
                            self.violate(line, Rule::Feasible, || {
                                format!("{step:?} planned at {at}, and {think} µs of think, after {at_l}")
                            });
                        }
                        Some(_) => {}
                        None => self.violate(line, Rule::Edge, || {
                            format!("{step:?} is no step planned within W behind the cursor, nor retained")
                        }),
                    }
                }
                Dep::Each {
                    kind,
                    scope,
                    label,
                    n,
                    count,
                    ..
                } => {
                    let (planned, latest) = self
                        .barriers
                        .get(&(kind.clone(), scope.clone(), label.clone(), *n))
                        .copied()
                        .unwrap_or((0, 0));
                    if let Some(c) = count
                        && *c != planned
                    {
                        self.violate(line, Rule::Barrier, || {
                            format!("{kind} {label}#{n}: {c} counted, {planned} planned before")
                        });
                    }
                    let (think, at_l) = (r.think, r.at);
                    if planned > 0 && latest.saturating_add(think) > at_l {
                        self.violate(line, Rule::Feasible, || {
                            format!("{kind} {label}#{n}: its latest planned at {latest}, and {think} µs of think, after {at_l}")
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn captures(&mut self, line: u64, r: &Record) {
        // Reads first: a step may read a slot's value and write the slot.
        let reads: BTreeSet<(Subject, String, StepId)> = holes(r)
            .into_iter()
            .filter_map(|h| match h {
                Hole::Captured {
                    subject,
                    slot,
                    from,
                } => Some((subject.clone(), slot.clone(), from.clone())),
                _ => None,
            })
            .collect();
        for (subject, slot, from) in reads {
            if !awaits(r, &from, On::Expected) {
                self.violate(line, Rule::Producer, || {
                    format!("{slot} of {subject:?}: {from:?} not awaited as expected")
                });
            }
            match self.slots.get_mut(&(subject.clone(), slot.clone())) {
                Some(s) if s.writer == from => {
                    s.readers.insert(r.step.clone());
                }
                _ => self.violate(line, Rule::Stale, || {
                    format!("{slot} of {subject:?}: {from:?} is not its latest writer")
                }),
            }
        }
        let mut writes: BTreeSet<(Subject, String)> = r
            .capture
            .iter()
            .map(|c| (c.subject.clone(), c.slot.clone()))
            .collect();
        if let Action::Rest(rest) = &r.action
            && let Some(op) = self.header.operations.iter().find(|o| o.name == rest.op)
        {
            writes.extend(
                op.writes
                    .iter()
                    .map(|s| (r.step.subject.clone(), s.clone())),
            );
        }
        for key in writes {
            if let Some(previous) = self.slots.get(&key) {
                let mut missing = Vec::new();
                if !awaits(r, &previous.writer, On::Settled) {
                    missing.push(format!("the writer {:?} settled", previous.writer));
                }
                for reader in &previous.readers {
                    if reader != &r.step && !awaits(r, reader, On::Settled) {
                        missing.push(format!("the reader {reader:?} settled"));
                    }
                }
                if !missing.is_empty() {
                    let detail = format!(
                        "{} of {:?}: awaits not {}",
                        key.1,
                        key.0,
                        missing.join(", ")
                    );
                    self.violate(line, Rule::Writers, || detail);
                }
            }
            // Live while its writer may still be awaited: within W of the
            // cursor, or retained.
            if self.header.retained.contains(&r.step.label) {
                self.live.insert(key.clone(), None);
            } else if self.cursor.saturating_sub(r.at) <= self.header.window {
                self.live_order.insert((r.at, key.clone()));
                self.live.insert(key.clone(), Some(r.at));
            } else {
                self.live.remove(&key);
            }
            self.slots.insert(
                key,
                Slot {
                    writer: r.step.clone(),
                    readers: BTreeSet::new(),
                },
            );
        }
        self.report.peaks.captures = self.report.peaks.captures.max(self.live.len() as u64);
    }

    /// A message's position checked now; its stream's order, its predecessor
    /// and its gaps at the end, by stream ([`Pass::streams_end`]).
    fn stream(&mut self, line: u64, r: &Record) -> Result<(), LogError> {
        let Some(s) = stream_of(r) else {
            return Ok(());
        };
        if !(1..=i32::MAX as u32).contains(&s.position) {
            self.violate(line, Rule::Stream, || {
                format!("{} at {}: out of [1, 2³¹ − 1]", s.name, s.position)
            });
        }
        let key = serde_json::to_vec(&(&r.step.subject, &s.name))
            .map_err(|e| LogError::Encode(e.to_string()))?;
        let deps = predecessors(r).map(id_digest).collect::<Result<_, _>>()?;
        self.streams.push(StreamEvent {
            key: crate::runs::digest(&key),
            line,
            position: s.position,
            gapless: self.header.profile.gapless.contains(&s.name),
            step: id_digest(&r.step)?,
            deps,
        });
        self.hold()
    }

    /// An observation awaits its cause, or is its own (a watch begun on its
    /// own, its deadline from its own start); one of the bus tells its
    /// cause's fact from another's by an `Equals` on a value of the cause.
    fn observation(&mut self, line: u64, r: &Record) {
        let Action::Observe(o) = &r.action else {
            return;
        };
        let cause = &o.cause;
        if cause != &r.step
            && !r
                .after
                .iter()
                .any(|d| matches!(d, Dep::Step { step, .. } if step == cause))
        {
            self.violate(line, Rule::Cause, || {
                format!("the cause {cause:?} is not awaited")
            });
        }
        if let Probe::Bus { .. } = o.probe {
            let of_cause = |v: &Value| match v {
                Value::Hole(Hole::Rendered {
                    of: Of::Step(step), ..
                }) => step == cause,
                Value::Hole(Hole::Captured { from, .. }) => from == cause,
                _ => false,
            };
            let tells = o
                .until
                .iter()
                .any(|p| matches!(&p.test, Test::Equals(v) if of_cause(v)));
            if !tells {
                self.violate(line, Rule::Until, || {
                    format!("no predicate equals a value of the cause {cause:?}")
                });
            }
        }
    }

    /// A step that awaits a dependency only sent carries no measure and no
    /// expectation, unless it is an observation.
    fn sent_only(&mut self, line: u64, r: &Record) {
        if matches!(r.action, Action::Observe(_)) {
            return;
        }
        let sent = r.after.iter().any(|d| match d {
            Dep::Step { on, .. } | Dep::Each { on, .. } => *on == On::Sent,
        });
        if sent && !(r.measure.is_empty() && r.expect.is_empty()) {
            self.violate(line, Rule::Sent, || {
                "a step awaiting a dependency only sent carries a measure or an expectation"
                    .to_owned()
            });
        }
    }

    /// The larger buffer to disk while both hold more than the memory.
    fn hold(&mut self) -> Result<(), LogError> {
        if self.ids.held().saturating_add(self.streams.held()) <= self.memory {
            return Ok(());
        }
        if self.ids.held() >= self.streams.held() {
            self.ids.spill(self.disk)
        } else {
            self.streams.spill(self.disk)
        }
    }

    /// Each stream's messages in line order: positions rising, each awaiting
    /// the one before as expected, a gapless stream from 1 without a gap.
    fn streams_end(&mut self) -> Result<(), LogError> {
        let streams = std::mem::replace(
            &mut self.streams,
            Sorter::new("streams", Path::new(""), 2, None),
        );
        let disk = self.disk;
        let mut before: Option<StreamEvent> = None;
        streams.drain(disk, |e| {
            match &before {
                Some(p) if p.key == e.key => {
                    if e.position <= p.position {
                        self.violate(e.line, Rule::Stream, || {
                            format!(
                                "at {}: not above {}, on line {}",
                                e.position, p.position, p.line
                            )
                        });
                    }
                    if !e.deps.contains(&p.step) {
                        self.violate(e.line, Rule::Stream, || {
                            format!(
                                "the message before, on line {}, not awaited as expected",
                                p.line
                            )
                        });
                    }
                    if e.gapless && e.position != p.position.saturating_add(1) {
                        self.violate(e.line, Rule::Gap, || {
                            format!("at {} after {}, on line {}", e.position, p.position, p.line)
                        });
                    }
                }
                _ => {
                    if e.gapless && e.position != 1 {
                        self.violate(e.line, Rule::Gap, || format!("begins at {}", e.position));
                    }
                }
            }
            before = Some(e);
            Ok(())
        })
    }

    fn spans(&mut self, line: u64, r: &Record) {
        let mut found = Vec::new();
        for p in payloads(r) {
            let mut end = 0u64;
            for span in &p.holes {
                let (start, stop) = (
                    u64::from(span.offset),
                    u64::from(span.offset).saturating_add(u64::from(span.len)),
                );
                if start < end {
                    found.push(format!("{}: overlaps the span before", span.field));
                }
                if stop > p.bytes.len() as u64 {
                    found.push(format!(
                        "{}: past the payload's {} bytes",
                        span.field,
                        p.bytes.len()
                    ));
                }
                let fits = match span.form {
                    Form::JsonToken => p.codec == Codec::Json,
                    Form::Raw => p.codec != Codec::Json,
                };
                if !fits {
                    found.push(format!(
                        "{}: a {:?} span in a {:?} payload",
                        span.field, span.form, p.codec
                    ));
                }
                if let Err(e) = self.host.span(p, span) {
                    found.push(format!("{}: {e}", span.field));
                }
                end = end.max(stop);
            }
        }
        for detail in found {
            self.violate(line, Rule::Span, || detail);
        }
    }

    fn profile(&mut self, line: u64, r: &Record) {
        let profile = &self.header.profile;
        let mut found = Vec::new();
        for h in holes(r) {
            match h {
                Hole::Rendered {
                    of: Of::Subject(s),
                    role,
                    ..
                } if role == "id" && !profile.rendered.contains(&s.kind) => {
                    found.push(format!(
                        "an id rendered for {:?}, which the stand does not render",
                        s.kind
                    ));
                }
                Hole::Captured { subject, slot, .. }
                    if slot == "id" && !profile.captured.contains(&subject.kind) =>
                {
                    found.push(format!(
                        "an id captured for {:?}, which the stand does not capture",
                        subject.kind
                    ));
                }
                _ => {}
            }
        }
        for detail in found {
            self.violate(line, Rule::Profile, || detail);
        }
    }

    fn operations(&mut self, line: u64, r: &Record) {
        let mut found = Vec::new();
        for rest in calls(r) {
            match self.header.operations.iter().find(|o| o.name == rest.op) {
                None => found.push(format!("{:?} is not in the operations table", rest.op)),
                Some(op) => {
                    let arity = op.path.matches('{').count();
                    if arity != rest.path.len() {
                        found.push(format!(
                            "{}: {} path values for {arity} parameters",
                            op.name,
                            rest.path.len()
                        ));
                    }
                }
            }
        }
        let channel = match &r.action {
            Action::Bus(b) => Some(&b.channel),
            Action::Observe(Observe {
                probe: Probe::Bus { channel, .. },
                ..
            }) => Some(channel),
            _ => None,
        };
        if let Some(c) = channel
            && !self.header.channels.iter().any(|t| &t.name == c)
        {
            found.push(format!("channel {c:?} is not in the channels table"));
        }
        if let Action::Host(h) = &r.action
            && !self.header.calls.iter().any(|t| t.name == h.op)
        {
            found.push(format!("host call {:?} is not in the calls table", h.op));
        }
        if let Action::Observe(o) = &r.action {
            // Polls at 0, every, … up to the timeout: ⌊timeout / every⌋ + 1.
            match o.timeout.checked_div(o.every) {
                Some(n) if n < MAX_POLLS => {
                    self.report.peaks.polls =
                        self.report.peaks.polls.saturating_add(n.saturating_add(1));
                }
                Some(n) => {
                    found.push(format!(
                        "an observation implying {} polls",
                        n.saturating_add(1)
                    ));
                    self.report.peaks.polls = self.report.peaks.polls.saturating_add(1);
                }
                None => {
                    found.push("an observation polling every 0".to_owned());
                    self.report.peaks.polls = self.report.peaks.polls.saturating_add(1);
                }
            }
        }
        for detail in found {
            self.violate(line, Rule::Operation, || detail);
        }
    }

    /// The steps planned more than `W` behind the cursor dropped, and the
    /// captures their writers made: no later step may depend on them, the
    /// cursor never going back. A step id kept again later stays.
    fn evict(&mut self) {
        let (w, c) = (self.header.window, self.cursor);
        while let Some((planned, _)) = self.order.first() {
            if c.saturating_sub(*planned) <= w {
                break;
            }
            if let Some((planned, step)) = self.order.pop_first()
                && self.window.get(&step) == Some(&planned)
            {
                self.window.remove(&step);
            }
        }
        while let Some((written, _)) = self.live_order.first() {
            if c.saturating_sub(*written) <= w {
                break;
            }
            if let Some((written, key)) = self.live_order.pop_first()
                && self.live.get(&key) == Some(&Some(written))
            {
                self.live.remove(&key);
            }
        }
    }

    /// The step, kept for later dependencies by its id's digest; the barrier
    /// counts.
    fn keep(&mut self, r: &Record, digest: [u8; 16]) {
        if self.header.retained.contains(&r.step.label) {
            self.retained.insert(digest, r.at);
        } else if self.cursor.saturating_sub(r.at) > self.header.window {
            // Dead on arrival: no later edge may accept it, and an older
            // occurrence must not answer for it.
            self.window.remove(&digest);
        } else {
            self.window.insert(digest, r.at);
            self.order.insert((r.at, digest));
        }
        let held = (self.window.len() as u64).saturating_add(self.retained.len() as u64);
        self.report.peaks.completions = self.report.peaks.completions.max(held);
        let s = &r.step;
        let held = self
            .barriers
            .entry((
                s.subject.kind.clone(),
                s.subject.scope.clone(),
                s.label.clone(),
                s.n,
            ))
            .or_insert((0, 0));
        *held = (held.0.saturating_add(1), held.1.max(r.at));
    }

    fn finish(mut self) -> Result<Report, LogError> {
        // The polls still due arrive before deviations are judged.
        self.drain(u64::MAX);
        // The hot parts go before the runs are read back: their memory reads.
        self.window = BTreeMap::new();
        self.order = BTreeSet::new();
        self.retained = BTreeMap::new();
        self.barriers = BTreeMap::new();
        self.slots = BTreeMap::new();
        self.live = BTreeMap::new();
        self.live_order = BTreeSet::new();
        self.buckets = BTreeMap::new();
        self.streams_end()?;
        let ids = std::mem::replace(&mut self.ids, Sorter::new("ids", Path::new(""), 2, None));
        let disk = self.disk;
        // The digest in hand, and the first line its id was seen on.
        let mut seen: Option<([u8; 16], Option<u64>)> = None;
        ids.drain(disk, |e| {
            let first = match seen {
                Some((d, first)) if d == e.digest => first,
                _ => None,
            };
            if e.query {
                if !first.is_some_and(|l| l < e.line) {
                    self.violate(e.line, Rule::Measure, || {
                        "a measure from no step planned on an earlier line".to_owned()
                    });
                }
                seen = Some((e.digest, first));
            } else {
                if first.is_some() {
                    self.violate(e.line, Rule::Duplicate, || {
                        "a step id an earlier line has".to_owned()
                    });
                }
                seen = Some((e.digest, first.or(Some(e.line))));
            }
            Ok(())
        })?;
        let unused: Vec<(u64, Rule)> = self
            .deviations
            .iter()
            .filter(|(_, (_, used))| !used)
            .map(|(line, (rule, _))| (*line, *rule))
            .collect();
        for (line, rule) in unused {
            self.violate(line, Rule::Deviation, || {
                format!("declared against {rule}, which the line keeps")
            });
        }
        self.report.violations = std::mem::take(&mut self.least).into_sorted_vec();
        Ok(self.report)
    }
}

/// Validates a log: framing by the reader, every other rule collected.
pub fn validate<R: BufRead>(
    reader: Reader<R>,
    host: &impl Host,
    limits: &Limits,
) -> Result<Report, LogError> {
    validate_on(reader, host, limits, &LocalDisk)
}

/// [`validate`] on a given machine: its filesystems and open-file limit.
pub fn validate_on<R: BufRead>(
    reader: Reader<R>,
    host: &impl Host,
    limits: &Limits,
    disk: &dyn Disk,
) -> Result<Report, LogError> {
    fs::create_dir_all(&limits.spill)?;
    if !limits.memory_spill && disk.in_memory(&limits.spill)? {
        return Err(LogError::Io(std::io::Error::other(format!(
            "{} is in memory: a spill there is refused unless allowed",
            limits.spill.display()
        ))));
    }
    let fan_in = fan_in(limits, disk)?;
    // The pass's own directory for its runs, removed on every exit below.
    let dir = SpillDir::new(&limits.spill)?;
    let mut pass = Pass {
        host,
        disk,
        memory: limits.memory.max(1),
        hot: limits.hot,
        header: reader.header().clone(),
        kept: limits.kept,
        report: Report::default(),
        previous: None,
        window: BTreeMap::new(),
        order: BTreeSet::new(),
        retained: BTreeMap::new(),
        barriers: BTreeMap::new(),
        slots: BTreeMap::new(),
        live: BTreeMap::new(),
        live_order: BTreeSet::new(),
        least: BinaryHeap::new(),
        streams: Sorter::new("streams", dir.path(), fan_in, limits.progress.clone()),
        ids: Sorter::new("ids", dir.path(), fan_in, limits.progress.clone()),
        deviations: BTreeMap::new(),
        cursor: 0,
        refused: BTreeSet::new(),
        buckets: BTreeMap::new(),
        polls: BinaryHeap::new(),
    };
    pass.header();
    let mut line = 1u64;
    for record in reader {
        line = line.saturating_add(1);
        pass.record(line, &record?)?;
    }
    let report = pass.finish();
    drop(dir);
    report
}

/// Validates the log in `path`.
pub fn validate_file(path: &Path, host: &impl Host, limits: &Limits) -> Result<Report, LogError> {
    validate(crate::log::open_file(path)?, host, limits)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A reader that fails: an error reading a run is an error of the pass, not
    /// the run's end — a run cut short would hide its duplicates.
    struct Failing;

    impl Read for Failing {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("disk"))
        }
    }

    fn run<R: Read>(reader: R, left: u64) -> Counted<R> {
        Counted {
            reader,
            left,
            path: "ids.run".into(),
        }
    }

    /// A run holds exactly its count: an error reading it, a run cut short
    /// — at an event's boundary too, or inside a stream event's dependencies —
    /// or one going on past its count fail the pass, never hide a violation.
    #[test]
    fn a_run_holds_exactly_its_count() {
        use crate::runs::next;
        fn id<R: Read>(r: &mut Counted<R>) -> Result<Option<IdEvent>, LogError> {
            next(r)
        }
        assert!(id(&mut run(Failing, 1)).is_err());
        assert_eq!(id(&mut run(std::io::empty(), 0)).ok(), Some(None));
        assert!(id(&mut run(std::io::empty(), 1)).is_err());
        assert!(id(&mut run(&[0u8; 24][..], 1)).is_err());
        assert!(id(&mut run(&[0u8; 1][..], 0)).is_err());
        assert!(matches!(id(&mut run(&[0u8; 25][..], 1)), Ok(Some(_))));
        let event = StreamEvent {
            key: [1; 16],
            line: 7,
            position: 3,
            gapless: true,
            step: [2; 16],
            deps: vec![[3; 16], [4; 16]],
        };
        let mut bytes = Vec::new();
        event.write(&mut bytes).unwrap();
        assert_eq!(bytes.len(), event.size());
        let stream = |b: &[u8], left| next::<StreamEvent, _>(&mut run(b, left));
        assert_eq!(stream(&bytes, 1).ok(), Some(Some(event.clone())));
        for cut in 0..bytes.len() {
            assert!(stream(&bytes[..cut], 1).is_err(), "cut at {cut}");
        }
    }
}
