//! A record's shape (ADR-0047 decision 22, K2b-7e-2b): the counts its bytes are
//! priced by, intrinsic to the plan — its strings' bytes, its sequences'
//! elements by type, its captured holes, its barrier key, its request's
//! literal bytes and holes —, summed over the records a run holds at once and
//! committed by the validator at their peaks. Whoever holds a record's bytes
//! prices its shape at the run's own build: the decoded record here
//! ([`bound`]), the machine its cell, the carrier its frame — so a price
//! re-pinned needs no new dry run, and a committed shape changes its meaning
//! only with the record's types.
//!
//! Every type is taken apart whole, no field left to `..`: a field added to a
//! record does not compile until its shape counts it.

use std::mem::size_of;

use serde::{Deserialize, Serialize};

use crate::record::{
    Action, Bus, By, Call, Caller, Capture, Channel, ClientRetry, Dep, Deviation, Field, Header,
    Hole, HostCall, Law, Limit, Measure, Observe, Of, Operation, Outcome, Payload, Place,
    Predicate, Probe, Profile, Record, Rest, Source, Span, StepId, StreamPosition, Subject, Test,
    Value,
};
use crate::wire::u64s;

/// The sequences of one element type: their elements, and how many of the
/// sequences are not empty.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Seqs {
    /// Elements.
    #[serde(with = "u64s")]
    pub elements: u64,
    /// Sequences of at least one element.
    #[serde(with = "u64s")]
    pub sequences: u64,
}

impl Seqs {
    fn one(len: usize) -> Seqs {
        let len = u64::try_from(len).unwrap_or(u64::MAX);
        Seqs {
            elements: len,
            sequences: u64::from(len > 0),
        }
    }

    fn plus(self, o: Seqs) -> Seqs {
        Seqs {
            elements: self.elements.saturating_add(o.elements),
            sequences: self.sequences.saturating_add(o.sequences),
        }
    }

    fn most(self, o: Seqs) -> Seqs {
        Seqs {
            elements: self.elements.max(o.elements),
            sequences: self.sequences.max(o.sequences),
        }
    }

    /// The bytes they hold at most decoded, elements of `size` bytes: a
    /// sequence grown one element at a time holds at most twice its elements
    /// and two more (four slots at first, then doubling).
    fn bound(self, size: usize) -> u64 {
        let slots = self
            .elements
            .saturating_mul(2)
            .saturating_add(self.sequences.saturating_mul(2));
        slots.saturating_mul(u64::try_from(size).unwrap_or(u64::MAX))
    }
}

/// A record's shape, or a sum of shapes (see the [module documentation](self)).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Shape {
    /// Records: one a record's.
    #[serde(with = "u64s")]
    pub records: u64,
    /// The bytes of its strings and byte strings, decoded.
    #[serde(with = "u64s")]
    pub bytes: u64,
    /// Its byte strings not empty (base64 in a line): each decoded into at
    /// most two bytes past its length.
    #[serde(with = "u64s")]
    pub byte_strings: u64,
    /// Its dependencies.
    pub deps: Seqs,
    /// Its path values.
    pub values: Seqs,
    /// Its query, header and bus header fields.
    pub fields: Seqs,
    /// Its expected outcomes.
    pub outcomes: Seqs,
    /// Its predicates: an outcome's body, an observation's until.
    pub predicates: Seqs,
    /// Its capture specs: the slots it produces.
    pub captures: Seqs,
    /// Their subjects' kinds and scopes and their slots, in bytes: what a
    /// value it captures is kept under.
    #[serde(with = "u64s")]
    pub capture_bytes: u64,
    /// Its measures.
    pub measures: Seqs,
    /// Its payloads' holes.
    pub spans: Seqs,
    /// Its caller's names.
    pub subjects: Seqs,
    /// Its captured holes: the producers the machine binds it to.
    #[serde(with = "u64s")]
    pub holes: u64,
    /// Their subjects' kinds and scopes and their slots, in bytes: what the
    /// machine copies to bind them.
    #[serde(with = "u64s")]
    pub hole_bytes: u64,
    /// Its step's barrier key — its subject's kind and scope and its label —
    /// in bytes.
    #[serde(with = "u64s")]
    pub key_bytes: u64,
    /// The literal bytes of its request — a call's or a probe's path, query
    /// and headers, its body — before its holes are filled.
    #[serde(with = "u64s")]
    pub request: u64,
    /// The holes its request fills.
    #[serde(with = "u64s")]
    pub request_holes: u64,
}

impl Shape {
    /// The sum of two shapes.
    pub fn plus(self, o: Shape) -> Shape {
        Shape {
            records: self.records.saturating_add(o.records),
            bytes: self.bytes.saturating_add(o.bytes),
            byte_strings: self.byte_strings.saturating_add(o.byte_strings),
            deps: self.deps.plus(o.deps),
            values: self.values.plus(o.values),
            fields: self.fields.plus(o.fields),
            outcomes: self.outcomes.plus(o.outcomes),
            predicates: self.predicates.plus(o.predicates),
            captures: self.captures.plus(o.captures),
            capture_bytes: self.capture_bytes.saturating_add(o.capture_bytes),
            measures: self.measures.plus(o.measures),
            spans: self.spans.plus(o.spans),
            subjects: self.subjects.plus(o.subjects),
            holes: self.holes.saturating_add(o.holes),
            hole_bytes: self.hole_bytes.saturating_add(o.hole_bytes),
            key_bytes: self.key_bytes.saturating_add(o.key_bytes),
            request: self.request.saturating_add(o.request),
            request_holes: self.request_holes.saturating_add(o.request_holes),
        }
    }

    /// The shape of `n` records of this shape.
    pub fn times(self, n: u64) -> Shape {
        let seqs = |a: Seqs| Seqs {
            elements: a.elements.saturating_mul(n),
            sequences: a.sequences.saturating_mul(n),
        };
        Shape {
            records: self.records.saturating_mul(n),
            bytes: self.bytes.saturating_mul(n),
            byte_strings: self.byte_strings.saturating_mul(n),
            deps: seqs(self.deps),
            values: seqs(self.values),
            fields: seqs(self.fields),
            outcomes: seqs(self.outcomes),
            predicates: seqs(self.predicates),
            captures: seqs(self.captures),
            capture_bytes: self.capture_bytes.saturating_mul(n),
            measures: seqs(self.measures),
            spans: seqs(self.spans),
            subjects: seqs(self.subjects),
            holes: self.holes.saturating_mul(n),
            hole_bytes: self.hole_bytes.saturating_mul(n),
            key_bytes: self.key_bytes.saturating_mul(n),
            request: self.request.saturating_mul(n),
            request_holes: self.request_holes.saturating_mul(n),
        }
    }

    /// The sum less a shape it holds: the validator's running sums, each
    /// count let go as it was added.
    pub fn less(self, o: Shape) -> Shape {
        let seqs = |a: Seqs, b: Seqs| Seqs {
            elements: a.elements.saturating_sub(b.elements),
            sequences: a.sequences.saturating_sub(b.sequences),
        };
        Shape {
            records: self.records.saturating_sub(o.records),
            bytes: self.bytes.saturating_sub(o.bytes),
            byte_strings: self.byte_strings.saturating_sub(o.byte_strings),
            deps: seqs(self.deps, o.deps),
            values: seqs(self.values, o.values),
            fields: seqs(self.fields, o.fields),
            outcomes: seqs(self.outcomes, o.outcomes),
            predicates: seqs(self.predicates, o.predicates),
            captures: seqs(self.captures, o.captures),
            capture_bytes: self.capture_bytes.saturating_sub(o.capture_bytes),
            measures: seqs(self.measures, o.measures),
            spans: seqs(self.spans, o.spans),
            subjects: seqs(self.subjects, o.subjects),
            holes: self.holes.saturating_sub(o.holes),
            hole_bytes: self.hole_bytes.saturating_sub(o.hole_bytes),
            key_bytes: self.key_bytes.saturating_sub(o.key_bytes),
            request: self.request.saturating_sub(o.request),
            request_holes: self.request_holes.saturating_sub(o.request_holes),
        }
    }

    /// Its counts in their declared order, each sequence's elements then its
    /// sequences: the form the corpus carries to the reference.
    pub fn counts(&self) -> Vec<u64> {
        let Shape {
            records,
            bytes,
            byte_strings,
            deps,
            values,
            fields,
            outcomes,
            predicates,
            captures,
            capture_bytes,
            measures,
            spans,
            subjects,
            holes,
            hole_bytes,
            key_bytes,
            request,
            request_holes,
        } = *self;
        let mut v = vec![records, bytes, byte_strings];
        for s in [deps, values, fields, outcomes, predicates, captures] {
            v.extend([s.elements, s.sequences]);
        }
        v.push(capture_bytes);
        for s in [measures, spans, subjects] {
            v.extend([s.elements, s.sequences]);
        }
        v.extend([holes, hole_bytes, key_bytes, request, request_holes]);
        v
    }

    /// Each count the larger of two shapes': what a peak of sums commits —
    /// a price linear in the counts is at most its value at these.
    pub fn most(self, o: Shape) -> Shape {
        Shape {
            records: self.records.max(o.records),
            bytes: self.bytes.max(o.bytes),
            byte_strings: self.byte_strings.max(o.byte_strings),
            deps: self.deps.most(o.deps),
            values: self.values.most(o.values),
            fields: self.fields.most(o.fields),
            outcomes: self.outcomes.most(o.outcomes),
            predicates: self.predicates.most(o.predicates),
            captures: self.captures.most(o.captures),
            capture_bytes: self.capture_bytes.max(o.capture_bytes),
            measures: self.measures.most(o.measures),
            spans: self.spans.most(o.spans),
            subjects: self.subjects.most(o.subjects),
            holes: self.holes.max(o.holes),
            hole_bytes: self.hole_bytes.max(o.hole_bytes),
            key_bytes: self.key_bytes.max(o.key_bytes),
            request: self.request.max(o.request),
            request_holes: self.request_holes.max(o.request_holes),
        }
    }
}

/// The bytes the records of shape `s` hold at most decoded, each in its own
/// `Arc` as the machine holds it: their strings, their byte strings, their
/// sequences grown one element at a time.
pub fn bound(s: &Shape) -> u64 {
    s.records
        .saturating_mul(as_u64(ARC))
        .saturating_add(parts(s))
}

/// What [`bound`] prices but the records' own `Arc`s: the bytes their fields
/// hold at most decoded, the records wherever they are — in a channel's slot,
/// on a thread's stack.
pub fn parts(s: &Shape) -> u64 {
    let parts = [
        s.deps.bound(size_of::<Dep>()),
        s.values.bound(size_of::<Value>()),
        s.fields.bound(size_of::<Field>()),
        s.outcomes.bound(size_of::<Outcome>()),
        s.predicates.bound(size_of::<Predicate>()),
        s.captures.bound(size_of::<Capture>()),
        s.measures.bound(size_of::<Measure>()),
        s.spans.bound(size_of::<Span>()),
        s.subjects.bound(size_of::<Subject>()),
    ];
    parts.into_iter().fold(
        s.bytes.saturating_add(s.byte_strings.saturating_mul(2)),
        u64::saturating_add,
    )
}

/// An `Arc` of a record: its two counts and the record.
const ARC: usize = 2 * size_of::<usize>() + size_of::<Record>();

fn as_u64(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

/// The shape of `r`.
pub fn shape(r: &Record) -> Shape {
    walked(r).0
}

/// The bytes `r` holds decoded from its line, in its own `Arc`, at this build:
/// its strings at their length, its byte strings as base64 decodes them, its
/// sequences as serde grows them — what [`bound`] bounds, exactly.
pub fn heap(r: &Record) -> u64 {
    walked(r).1
}

fn walked(r: &Record) -> (Shape, u64) {
    let mut w = Walk {
        shape: Shape {
            records: 1,
            ..Shape::default()
        },
        heap: as_u64(ARC),
        request: false,
    };
    w.record(r);
    (w.shape, w.heap)
}

/// A walk over a record: its shape and its exact heap as it goes.
struct Walk {
    shape: Shape,
    heap: u64,
    /// Whether what is walked is a request's: its literal bytes and holes
    /// counted as the frame's.
    request: bool,
}

/// The slots serde's sequence of `len` elements holds decoded: none for none,
/// four at first, then doubling.
fn slots(len: usize) -> usize {
    match len {
        0 => 0,
        n => n.next_power_of_two().max(4),
    }
}

impl Walk {
    fn grow(&mut self, by: u64) {
        self.heap = self.heap.saturating_add(by);
    }

    fn string(&mut self, s: &str) {
        let n = as_u64(s.len());
        self.shape.bytes = self.shape.bytes.saturating_add(n);
        self.grow(n);
    }

    /// A string a request carries as it is: the frame's too.
    fn literal(&mut self, s: &str) {
        if self.request {
            self.shape.request = self.shape.request.saturating_add(as_u64(s.len()));
        }
        self.string(s);
    }

    fn byte_string(&mut self, b: &[u8]) {
        let n = b.len();
        self.shape.bytes = self.shape.bytes.saturating_add(as_u64(n));
        if n > 0 {
            self.shape.byte_strings = self.shape.byte_strings.saturating_add(1);
        }
        // Base64 decodes into its estimate, three bytes a four-byte group.
        self.grow(as_u64(n.div_ceil(3).saturating_mul(3)));
    }

    /// A sequence of `len` elements of type `T`, counted into `seqs`.
    fn seq<T>(&mut self, len: usize, seqs: fn(&mut Shape) -> &mut Seqs) {
        let s = seqs(&mut self.shape);
        *s = s.plus(Seqs::one(len));
        self.grow(as_u64(slots(len).saturating_mul(size_of::<T>())));
    }

    fn record(&mut self, r: &Record) {
        let Record {
            step,
            at: _,
            think: _,
            after,
            action,
            expect,
            capture,
            measure,
            deviation,
        } = r;
        self.step(step);
        self.shape.key_bytes = as_u64(
            step.subject
                .kind
                .len()
                .saturating_add(step.subject.scope.len())
                .saturating_add(step.label.len()),
        );
        self.seq::<Dep>(after.len(), |s| &mut s.deps);
        after.iter().for_each(|d| self.dep(d));
        self.action(action);
        self.seq::<Outcome>(expect.len(), |s| &mut s.outcomes);
        expect.iter().for_each(|o| self.outcome(o));
        self.seq::<Capture>(capture.len(), |s| &mut s.captures);
        capture.iter().for_each(|c| self.capture(c));
        self.seq::<Measure>(measure.len(), |s| &mut s.measures);
        measure.iter().for_each(|m| self.measure(m));
        if let Some(d) = deviation {
            let Deviation { rule, reason } = d;
            self.string(rule);
            self.string(reason);
        }
    }

    fn subject(&mut self, s: &Subject) {
        let Subject {
            kind,
            scope,
            index: _,
        } = s;
        self.string(kind);
        self.byte_string(scope);
    }

    fn step(&mut self, s: &StepId) {
        let StepId {
            subject,
            label,
            n: _,
        } = s;
        self.subject(subject);
        self.string(label);
    }

    fn dep(&mut self, d: &Dep) {
        match d {
            Dep::Step { step, on: _ } => self.step(step),
            Dep::Each {
                kind,
                scope,
                label,
                n: _,
                on: _,
                count: _,
            } => {
                self.string(kind);
                self.byte_string(scope);
                self.string(label);
            }
        }
    }

    fn action(&mut self, a: &Action) {
        match a {
            Action::Rest(rest) => self.rest(rest),
            Action::Bus(bus) => self.bus(bus),
            Action::Observe(o) => self.observe(o),
            Action::Host(h) => {
                let HostCall { op, args } = h;
                self.string(op);
                self.payload(args);
            }
        }
    }

    fn rest(&mut self, r: &Rest) {
        let Rest {
            op,
            caller,
            source: _,
            path,
            query,
            headers,
            body,
        } = r;
        self.string(op);
        if let Some(c) = caller {
            let Caller { principal, names } = c;
            self.subject(principal);
            self.seq::<Subject>(names.len(), |s| &mut s.subjects);
            names.iter().for_each(|n| self.subject(n));
        }
        let was = std::mem::replace(&mut self.request, true);
        self.seq::<Value>(path.len(), |s| &mut s.values);
        path.iter().for_each(|v| self.value(v));
        self.fields(query);
        self.fields(headers);
        if let Some(p) = body {
            self.payload(p);
        }
        self.request = was;
    }

    fn fields(&mut self, fields: &[Field]) {
        self.seq::<Field>(fields.len(), |s| &mut s.fields);
        for f in fields {
            let Field { name, value } = f;
            self.literal(name);
            self.value(value);
        }
    }

    fn bus(&mut self, b: &Bus) {
        let Bus {
            channel,
            key,
            headers,
            stream,
            payload,
        } = b;
        self.string(channel);
        self.value(key);
        self.fields(headers);
        if let Some(s) = stream {
            let StreamPosition { name, position: _ } = s;
            self.string(name);
        }
        self.payload(payload);
    }

    fn observe(&mut self, o: &Observe) {
        let Observe {
            cause,
            probe,
            until,
            every: _,
            timeout: _,
        } = o;
        self.step(cause);
        match probe {
            Probe::Rest(rest) => self.rest(rest),
            Probe::Bus {
                channel,
                key,
                event,
            } => {
                self.string(channel);
                self.value(key);
                self.string(event);
            }
        }
        self.predicates(until);
    }

    fn payload(&mut self, p: &Payload) {
        let Payload {
            codec: _,
            bytes,
            holes,
        } = p;
        if self.request {
            self.shape.request = self.shape.request.saturating_add(as_u64(bytes.len()));
        }
        self.byte_string(bytes);
        self.seq::<Span>(holes.len(), |s| &mut s.spans);
        for span in holes {
            let Span {
                offset: _,
                len: _,
                field,
                hole,
                form: _,
            } = span;
            self.string(field);
            self.hole(hole);
        }
    }

    fn value(&mut self, v: &Value) {
        match v {
            Value::Literal(s) => self.literal(s),
            Value::Hole(h) => self.hole(h),
        }
    }

    fn hole(&mut self, h: &Hole) {
        if self.request {
            self.shape.request_holes = self.shape.request_holes.saturating_add(1);
        }
        match h {
            Hole::Rendered {
                of,
                role,
                rendering: _,
            } => {
                match of {
                    Of::Subject(s) => self.subject(s),
                    Of::Step(s) => self.step(s),
                }
                self.string(role);
            }
            Hole::Captured {
                subject,
                slot,
                from,
            } => {
                self.shape.holes = self.shape.holes.saturating_add(1);
                let bytes = subject
                    .kind
                    .len()
                    .saturating_add(subject.scope.len())
                    .saturating_add(slot.len());
                self.shape.hole_bytes = self.shape.hole_bytes.saturating_add(as_u64(bytes));
                self.subject(subject);
                self.string(slot);
                self.step(from);
            }
            Hole::Time {
                base: _,
                offset: _,
                rendering: _,
            } => {}
        }
    }

    fn outcome(&mut self, o: &Outcome) {
        match o {
            Outcome::Response {
                status: _,
                problem,
                body,
            } => {
                if let Some(p) = problem {
                    self.string(p);
                }
                self.predicates(body);
            }
            Outcome::Delivered | Outcome::Observed => {}
        }
    }

    fn predicates(&mut self, ps: &[Predicate]) {
        self.seq::<Predicate>(ps.len(), |s| &mut s.predicates);
        for p in ps {
            let Predicate { pointer, test } = p;
            self.string(pointer);
            match test {
                Test::Exists => {}
                Test::Equals(v) | Test::Contains(v) => self.value(v),
            }
        }
    }

    fn capture(&mut self, c: &Capture) {
        let Capture {
            subject,
            slot,
            source,
            secret: _,
        } = c;
        let bytes = subject
            .kind
            .len()
            .saturating_add(subject.scope.len())
            .saturating_add(slot.len());
        self.shape.capture_bytes = self.shape.capture_bytes.saturating_add(as_u64(bytes));
        self.subject(subject);
        self.string(slot);
        match source {
            Source::Header(s) | Source::Json(s) => self.string(s),
            Source::Location => {}
        }
    }

    fn measure(&mut self, m: &Measure) {
        let Measure { metric, from } = m;
        self.string(metric);
        self.step(from);
    }
}

/// The bytes a log's header holds decoded from its line, in its own `Arc` as a
/// run shares it (ADR-0047 decision 22, K2b-7e-2b-2): its strings at their
/// length, its sequences as serde grows them — exactly, at this build. Every
/// type taken apart whole, as a record's is.
pub fn header(h: &Header) -> u64 {
    let mut n = Bytes(as_u64(size_of::<[usize; 2]>()).saturating_add(as_u64(size_of::<Header>())));
    let Header {
        format: _,
        algorithm,
        scenario,
        revision,
        profile,
        window: _,
        read_ahead: _,
        retained,
        laws,
        operations,
        channels,
        calls,
    } = h;
    n.strings([algorithm, scenario, revision]);
    n.profile(profile);
    n.names(retained);
    n.seq::<Law>(laws.len());
    for Law { name, law } in laws {
        n.strings([name, law]);
    }
    n.seq::<Operation>(operations.len());
    for o in operations {
        n.operation(o);
    }
    n.seq::<Channel>(channels.len());
    for Channel { name, timeout: _ } in channels {
        n.strings([name]);
    }
    n.seq::<Call>(calls.len());
    for Call {
        name,
        timeout: _,
        safety: _,
    } in calls
    {
        n.strings([name]);
    }
    n.0
}

/// A header's bytes as they are counted.
struct Bytes(u64);

impl Bytes {
    fn strings<'a>(&mut self, ss: impl IntoIterator<Item = &'a String>) {
        for s in ss {
            self.0 = self.0.saturating_add(as_u64(s.len()));
        }
    }

    fn seq<T>(&mut self, len: usize) {
        self.0 = self
            .0
            .saturating_add(as_u64(slots(len).saturating_mul(size_of::<T>())));
    }

    fn names(&mut self, names: &[String]) {
        self.seq::<String>(names.len());
        self.strings(names);
    }

    fn profile(&mut self, p: &Profile) {
        let Profile {
            name,
            rendered,
            captured,
            gapless,
            sources: _,
            jitter: _,
            reserve: _,
            route_timeout: _,
            problem,
            correlation,
            resend: _,
            answer_cap: _,
            idle: _,
        } = p;
        self.strings([name, problem, correlation]);
        self.names(rendered);
        self.names(captured);
        self.names(gapless);
    }

    fn operation(&mut self, o: &Operation) {
        let Operation {
            name,
            method,
            path,
            content_type,
            decorated,
            writes,
            safety: _,
            client,
            not_run,
            timeout: _,
            limits,
        } = o;
        self.strings([name, method, path, content_type]);
        self.names(decorated);
        self.names(writes);
        let ClientRetry {
            attempts: _,
            on,
            backoff: _,
            retry_after: _,
        } = client;
        self.seq::<u16>(on.len());
        self.names(not_run);
        self.seq::<Limit>(limits.len());
        for Limit {
            key,
            bucket,
            by,
            requests: _,
            per_seconds: _,
            burst: _,
        } in limits
        {
            self.strings([key, bucket]);
            match by {
                By::All | By::Source => {}
                By::At(
                    Place::Path(s)
                    | Place::Query(s)
                    | Place::Header(s)
                    | Place::Body(s)
                    | Place::Credential(s),
                ) => self.strings([s]),
            }
        }
    }
}
