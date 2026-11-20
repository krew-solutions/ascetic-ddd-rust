//! The validator (ADR-0046 decision 9): one streaming pass over a log, in
//! memory bounded by the window, the live captures and streams, and a budget of
//! step ids past which uniqueness is decided by an external sort.
//!
//! Framing — the header, canonical lines, the trailer — is the [`Reader`]'s,
//! and fails the pass. Every other rule is collected as a [`Violation`], so one
//! pass reports them all. `vectors/reference.py` decides the same rules from
//! their definitions, quadratically, and is compared with this on a corpus.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::log::{LogError, Reader};
use crate::rate::{self, Curve, Lower};
use crate::record::{
    Action, Bus, By, Codec, Dep, Field, Form, Header, Hole, Limit, Of, On, Outcome, Payload, Place,
    Predicate, Probe, Record, Rest, Span, StepId, Subject, Test, Value,
};

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
}

impl Rule {
    /// Every rule, in order.
    pub const ALL: [Rule; 17] = [
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

/// The most a run must hold at once, as the log plans it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Peaks {
    /// Steps whose completion must be kept for a later dependency: those within
    /// `W` of the latest, and the retained.
    pub completions: u64,
    /// Captured slots live at once.
    pub captures: u64,
    /// Polls the observations imply, at most: each one's timeout over its
    /// interval, summed.
    pub polls: u64,
}

/// What a pass found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Report {
    /// The steps read.
    pub records: u64,
    /// The violations, in line order, at most [`Limits::kept`] of them.
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
    /// Step ids held in memory before a sorted run goes to disk; at least 1.
    pub ids_in_memory: usize,
    /// Where runs go: a directory of the caller's, its files removed after.
    pub spill: PathBuf,
    /// Violations kept in the report.
    pub kept: usize,
}

impl Limits {
    /// Ten million ids in memory (about 240 MB), spills under `spill`.
    pub fn new(spill: impl Into<PathBuf>) -> Limits {
        Limits {
            ids_in_memory: 10_000_000,
            spill: spill.into(),
            kept: 10_000,
        }
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

/// The step ids seen, for uniqueness: in memory up to a budget, in sorted runs
/// on disk past it, merged at the end. Each entry is a 16-byte digest of the
/// id and its line; equal digests are the same id but with probability 2⁻¹²⁸
/// per pair.
struct Ids {
    budget: usize,
    dir: PathBuf,
    buffer: Vec<([u8; 16], u64)>,
    runs: Vec<PathBuf>,
}

const ENTRY: usize = 24;

impl Ids {
    fn push(&mut self, step: &StepId, line: u64) -> Result<(), LogError> {
        let bytes = serde_json::to_vec(step).map_err(|e| LogError::Encode(e.to_string()))?;
        let digest = Sha256::digest(&bytes);
        let mut d = [0u8; 16];
        d.copy_from_slice(digest.get(..16).unwrap_or(&[0; 16]));
        self.buffer.push((d, line));
        if self.buffer.len() >= self.budget {
            self.spill()?;
        }
        Ok(())
    }

    fn spill(&mut self) -> Result<(), LogError> {
        self.buffer.sort_unstable();
        let path = self.dir.join(format!("ids-{}.run", self.runs.len()));
        let mut out = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?,
        );
        for (d, line) in self.buffer.drain(..) {
            out.write_all(&d)?;
            out.write_all(&line.to_le_bytes())?;
        }
        out.flush()?;
        self.runs.push(path);
        Ok(())
    }

    /// The lines whose id an earlier line has.
    fn duplicates(mut self) -> Result<Vec<u64>, LogError> {
        let mut found = Vec::new();
        if self.runs.is_empty() {
            self.buffer.sort_unstable();
            for pair in self.buffer.windows(2) {
                if let [(a, _), (b, line)] = pair
                    && a == b
                {
                    found.push(*line);
                }
            }
            return Ok(found);
        }
        if !self.buffer.is_empty() {
            self.spill()?;
        }
        let result = merge(&self.runs, &mut found);
        for run in &self.runs {
            let _ = fs::remove_file(run);
        }
        result.map(|()| found)
    }
}

fn read_entry(r: &mut impl Read) -> Result<Option<([u8; 16], u64)>, LogError> {
    let mut buf = [0u8; ENTRY];
    match r.read_exact(&mut buf) {
        Ok(()) => {
            let mut d = [0u8; 16];
            d.copy_from_slice(buf.get(..16).unwrap_or(&[0; 16]));
            let mut l = [0u8; 8];
            l.copy_from_slice(buf.get(16..).unwrap_or(&[0; 8]));
            Ok(Some((d, u64::from_le_bytes(l))))
        }
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// A k-way merge of sorted runs: a line whose digest equals the one before it
/// in the merged order is a duplicate.
fn merge(runs: &[PathBuf], found: &mut Vec<u64>) -> Result<(), LogError> {
    let mut readers: Vec<BufReader<File>> = runs
        .iter()
        .map(|p| File::open(p).map(BufReader::new))
        .collect::<Result<_, _>>()?;
    let mut heap = BinaryHeap::new();
    for (i, r) in readers.iter_mut().enumerate() {
        if let Some(e) = read_entry(r)? {
            heap.push(Reverse((e, i)));
        }
    }
    let mut last: Option<[u8; 16]> = None;
    while let Some(Reverse(((d, line), i))) = heap.pop() {
        if last == Some(d) {
            found.push(line);
        }
        last = Some(d);
        if let Some(r) = readers.get_mut(i)
            && let Some(e) = read_entry(r)?
        {
            heap.push(Reverse((e, i)));
        }
    }
    Ok(())
}

struct Pass<'h, H: Host> {
    host: &'h H,
    header: Header,
    kept: usize,
    report: Report,
    previous: Option<(u64, StepId)>,
    window: BTreeMap<StepId, u64>,
    order: VecDeque<(u64, StepId)>,
    retained: BTreeMap<StepId, u64>,
    barriers: BTreeMap<(String, Vec<u8>, String, u32), u64>,
    slots: BTreeMap<(Subject, String), Slot>,
    streams: BTreeMap<(Subject, String), (u32, StepId)>,
    ids: Ids,
    /// Lines that declared a deviation, its rule, and whether it was used.
    deviations: BTreeMap<u64, (Rule, bool)>,
    /// The running maximum of `at`: when a line's calls arrive.
    clamp: u64,
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
    /// A violation of `rule` at `line`, unless the line declared it.
    fn violate(&mut self, line: u64, rule: Rule, detail: String) {
        // No deviation exempts a key: an arrival left out of its bucket would be
        // an under-count.
        if let Some((declared, used)) = self.deviations.get_mut(&line)
            && *declared == rule
            && rule != Rule::Key
        {
            *used = true;
            return;
        }
        self.report.violated = self.report.violated.saturating_add(1);
        if self.report.violations.len() < self.kept {
            self.report
                .violations
                .push(Violation { line, rule, detail });
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
            self.violate(
                1,
                Rule::Header,
                format!("kind {k:?} both rendered and captured"),
            );
        }
        for o in twice {
            self.violate(1, Rule::Header, format!("operation {o:?} twice"));
        }
        let (found, refused) = self.limits();
        self.refused = refused;
        for detail in found {
            self.violate(1, Rule::Header, detail);
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
            self.violate(line, Rule::Key, detail);
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
                self.violate(line, Rule::Rate, detail);
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

    /// The line's calls: a REST action at the clamped time, an observation's
    /// polls from it, every `every` to its timeout.
    fn offered(&mut self, line: u64, r: &Record) {
        self.clamp = self.clamp.max(r.at);
        let t = self.clamp;
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
        if let Some(d) = &r.deviation {
            match Rule::named(&d.rule) {
                Some(rule) => {
                    self.deviations.insert(line, (rule, false));
                }
                None => self.violate(line, Rule::Deviation, format!("{:?} names no rule", d.rule)),
            }
        }
        self.order(line, r);
        self.ids.push(&r.step, line)?;
        self.evict(r.at);
        self.edges(line, r);
        self.captures(line, r);
        self.stream(line, r);
        self.spans(line, r);
        self.profile(line, r);
        self.operations(line, r);
        for (step, on) in self.host.required(r) {
            if !awaits(r, &step, on) {
                self.violate(
                    line,
                    Rule::Required,
                    format!("{step:?} not awaited as {on:?}"),
                );
            }
        }
        self.offered(line, r);
        self.keep(r);
        Ok(())
    }

    fn order(&mut self, line: u64, r: &Record) {
        let key = (r.at, r.step.clone());
        if let Some(previous) = &self.previous
            && &key <= previous
        {
            self.violate(
                line,
                Rule::Order,
                format!("({}, {:?}) not above the line before", r.at, r.step),
            );
        }
        self.previous = Some(key);
    }

    fn edges(&mut self, line: u64, r: &Record) {
        for d in &r.after {
            match d {
                Dep::Step { step, .. } => {
                    // The distance is checked here, not left to eviction: a line out
                    // of order keeps an old step in the window.
                    let w = self.header.window;
                    let found = match (self.retained.get(step), self.window.get(step)) {
                        (Some(at), _) => Some((*at, true)),
                        (None, Some(at)) => Some((*at, false)),
                        (None, None) => None,
                    };
                    let problem = match found {
                        Some((at, _)) if at >= r.at => {
                            Some(format!("{step:?} planned at {at}, not before {}", r.at))
                        }
                        Some((at, false)) if r.at.saturating_sub(at) > w => Some(format!(
                            "{step:?} planned at {at}, more than W = {w} before {}",
                            r.at
                        )),
                        Some(_) => None,
                        None => Some(format!(
                            "{step:?} is no step planned within the window, nor retained"
                        )),
                    };
                    if let Some(detail) = problem {
                        self.violate(line, Rule::Edge, detail);
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
                    let planned = self
                        .barriers
                        .get(&(kind.clone(), scope.clone(), label.clone(), *n))
                        .copied()
                        .unwrap_or(0);
                    if let Some(c) = count
                        && *c != planned
                    {
                        self.violate(
                            line,
                            Rule::Barrier,
                            format!("{kind} {label}#{n}: {c} counted, {planned} planned before"),
                        );
                    }
                }
            }
        }
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
                self.violate(
                    line,
                    Rule::Producer,
                    format!("{slot} of {subject:?}: {from:?} not awaited as expected"),
                );
            }
            match self.slots.get_mut(&(subject.clone(), slot.clone())) {
                Some(s) if s.writer == from => {
                    s.readers.insert(r.step.clone());
                }
                _ => self.violate(
                    line,
                    Rule::Stale,
                    format!("{slot} of {subject:?}: {from:?} is not its latest writer"),
                ),
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
                    if reader != &r.step && !awaits(r, reader, On::Sent) {
                        missing.push(format!("the reader {reader:?}"));
                    }
                }
                if !missing.is_empty() {
                    let detail = format!(
                        "{} of {:?}: awaits not {}",
                        key.1,
                        key.0,
                        missing.join(", ")
                    );
                    self.violate(line, Rule::Writers, detail);
                }
            }
            self.slots.insert(
                key,
                Slot {
                    writer: r.step.clone(),
                    readers: BTreeSet::new(),
                },
            );
        }
        self.report.peaks.captures = self.report.peaks.captures.max(self.slots.len() as u64);
    }

    fn stream(&mut self, line: u64, r: &Record) {
        let Action::Bus(Bus {
            stream: Some(s), ..
        }) = &r.action
        else {
            return;
        };
        if !(1..=i32::MAX as u32).contains(&s.position) {
            self.violate(
                line,
                Rule::Stream,
                format!("{} at {}: out of [1, 2³¹ − 1]", s.name, s.position),
            );
        }
        let gapless = self.header.profile.gapless.contains(&s.name);
        let key = (r.step.subject.clone(), s.name.clone());
        match self.streams.get(&key).cloned() {
            Some((position, step)) => {
                if s.position <= position {
                    self.violate(
                        line,
                        Rule::Stream,
                        format!("{} at {}: not above {position}", s.name, s.position),
                    );
                }
                if !awaits(r, &step, On::Expected) {
                    self.violate(
                        line,
                        Rule::Stream,
                        format!(
                            "{}: the message before, {step:?}, not awaited as expected",
                            s.name
                        ),
                    );
                }
                if gapless && s.position != position.saturating_add(1) {
                    self.violate(
                        line,
                        Rule::Gap,
                        format!("{} at {} after {position}", s.name, s.position),
                    );
                }
            }
            None => {
                if gapless && s.position != 1 {
                    self.violate(
                        line,
                        Rule::Gap,
                        format!("{} begins at {}", s.name, s.position),
                    );
                }
            }
        }
        self.streams.insert(key, (s.position, r.step.clone()));
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
            self.violate(line, Rule::Span, detail);
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
            self.violate(line, Rule::Profile, detail);
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
            self.violate(line, Rule::Operation, detail);
        }
    }

    /// The steps planned more than `W` before `at` dropped: no later step may
    /// depend on them. A step id kept again later stays.
    fn evict(&mut self, at: u64) {
        let w = self.header.window;
        while let Some((planned, _)) = self.order.front() {
            if at.saturating_sub(*planned) <= w {
                break;
            }
            if let Some((planned, step)) = self.order.pop_front()
                && self.window.get(&step) == Some(&planned)
            {
                self.window.remove(&step);
            }
        }
    }

    /// The step, kept for later dependencies; the barrier counts.
    fn keep(&mut self, r: &Record) {
        if self.header.retained.contains(&r.step.label) {
            self.retained.insert(r.step.clone(), r.at);
        } else {
            self.window.insert(r.step.clone(), r.at);
            self.order.push_back((r.at, r.step.clone()));
        }
        let held = (self.window.len() as u64).saturating_add(self.retained.len() as u64);
        self.report.peaks.completions = self.report.peaks.completions.max(held);
        let s = &r.step;
        let count = self
            .barriers
            .entry((
                s.subject.kind.clone(),
                s.subject.scope.clone(),
                s.label.clone(),
                s.n,
            ))
            .or_insert(0);
        *count = count.saturating_add(1);
    }

    fn finish(mut self) -> Result<Report, LogError> {
        // The polls still due arrive before deviations are judged.
        self.drain(u64::MAX);
        let ids = std::mem::replace(
            &mut self.ids,
            Ids {
                budget: 1,
                dir: PathBuf::new(),
                buffer: Vec::new(),
                runs: Vec::new(),
            },
        );
        for line in ids.duplicates()? {
            self.violate(
                line,
                Rule::Duplicate,
                "a step id an earlier line has".to_owned(),
            );
        }
        let unused: Vec<(u64, Rule)> = self
            .deviations
            .iter()
            .filter(|(_, (_, used))| !used)
            .map(|(line, (rule, _))| (*line, *rule))
            .collect();
        for (line, rule) in unused {
            self.violate(
                line,
                Rule::Deviation,
                format!("declared against {rule}, which the line keeps"),
            );
        }
        self.report.violations.sort();
        Ok(self.report)
    }
}

/// Validates a log: framing by the reader, every other rule collected.
pub fn validate<R: BufRead>(
    reader: Reader<R>,
    host: &impl Host,
    limits: &Limits,
) -> Result<Report, LogError> {
    let mut pass = Pass {
        host,
        header: reader.header().clone(),
        kept: limits.kept,
        report: Report::default(),
        previous: None,
        window: BTreeMap::new(),
        order: VecDeque::new(),
        retained: BTreeMap::new(),
        barriers: BTreeMap::new(),
        slots: BTreeMap::new(),
        streams: BTreeMap::new(),
        ids: Ids {
            budget: limits.ids_in_memory.max(1),
            dir: limits.spill.clone(),
            buffer: Vec::new(),
            runs: Vec::new(),
        },
        deviations: BTreeMap::new(),
        clamp: 0,
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
    pass.finish()
}

/// Validates the log in `path`.
pub fn validate_file(path: &Path, host: &impl Host, limits: &Limits) -> Result<Report, LogError> {
    let reader = Reader::new(BufReader::new(File::open(path)?))?;
    validate(reader, host, limits)
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

    #[test]
    fn a_failing_run_fails_the_pass() {
        assert!(read_entry(&mut Failing).is_err());
        assert_eq!(read_entry(&mut std::io::empty()).ok(), Some(None));
    }
}
