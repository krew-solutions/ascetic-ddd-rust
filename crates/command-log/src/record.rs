//! The lines of a log (ADR-0046 decisions 2, 3 and 6).
//!
//! Every type here is a wire type: serialised in its fields' order, with
//! unknown fields refused. Integers that can pass 2⁵³ are strings, bytes are
//! standard padded base64, each in its one canonical text; there is no floating
//! point. The normative schema is `schema/line.schema.json`.

use serde::{Deserialize, Serialize};

use crate::wire::{b64, u64s};

/// A name: a kind, a label, a slot, an operation, a channel, a metric.
pub type Name = String;

/// One line of a log: the header first, the steps, the trailer last.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Line {
    /// What the log is for.
    Header(Box<Header>),
    /// A planned step.
    Step(Box<Record>),
    /// How many steps there were, and their digest.
    Trailer(Trailer),
}

/// What a log is for, and what replaying it needs (decision 6).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Header {
    /// The format's version: a reader refuses any other.
    pub format: u32,
    /// faker's `ALGORITHM`, as provenance.
    pub algorithm: String,
    /// The scenario that generated the log, as provenance.
    pub scenario: String,
    /// The scenario's revision, as provenance.
    pub revision: String,
    /// The stand the log is for (decision 10).
    pub profile: Profile,
    /// How far behind the read cursor a completion is kept, in planned
    /// microseconds (decision 7).
    #[serde(with = "u64s")]
    pub window: u64,
    /// How far ahead the scheduler reads, in planned microseconds.
    #[serde(with = "u64s")]
    pub read_ahead: u64,
    /// Labels whose completions are kept for the whole run.
    pub retained: Vec<Name>,
    /// The laws the scenario drew from, by name, in their canonical form.
    pub laws: Vec<Law>,
    /// The operations the steps call.
    pub operations: Vec<Operation>,
}

/// A law of the scenario, by its canonical `Display`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Law {
    /// What it draws.
    pub name: Name,
    /// The law, as faker displays it.
    pub law: String,
}

/// The stand a log runs on (decision 10).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    /// The stand's name.
    pub name: String,
    /// Kinds whose ids the log renders.
    pub rendered: Vec<Name>,
    /// Kinds whose ids the system assigns and the log captures.
    pub captured: Vec<Name>,
    /// Streams a producer keeps without gaps.
    pub gapless: Vec<Name>,
}

/// An operation a step calls, from the host's contracts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Operation {
    /// Its name, as steps refer to it.
    pub name: Name,
    /// The HTTP method, for a REST operation.
    pub method: String,
    /// The path template, `{name}` per parameter.
    pub path: String,
    /// The body's content type.
    pub content_type: String,
    /// Headers a decorator supplies at replay (DPoP, authorisation).
    pub decorated: Vec<String>,
    /// The slots of its subject it writes (an ETag), captured or not.
    pub writes: Vec<Name>,
    /// Whether, and when, it may be sent again.
    pub retry: Retry,
}

/// When an operation may be sent again (decision 12).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Retry {
    /// Never: an unknown outcome fails the step.
    Never,
    /// Only when the request was certainly not taken (429, 503 with
    /// `Retry-After`).
    NotTaken,
    /// Always: sending it again changes nothing (a PUT of a whole value).
    Idempotent,
}

/// The log's end: how many steps it holds and their digest (decision 6).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Trailer {
    /// Steps, not lines.
    #[serde(with = "u64s")]
    pub records: u64,
    /// SHA-256 of every line before the trailer, `\n` included, in hex.
    pub sha256: String,
}

/// faker's `Ref`, its scope as the scope's canonical bytes.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subject {
    /// The population's kind.
    pub kind: Name,
    /// The scope's `KeyPart` encoding.
    #[serde(with = "b64")]
    pub scope: Vec<u8>,
    /// The member's index in its scope.
    #[serde(with = "u64s")]
    pub index: u64,
}

/// The `n`th step `label` of `subject`: from identity alone.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepId {
    /// Whose step it is.
    pub subject: Subject,
    /// Which step.
    pub label: Name,
    /// Which occurrence of it.
    pub n: u32,
}

/// A planned step (decision 2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    /// Its identity.
    pub step: StepId,
    /// Its planned time, in microseconds from the run's start.
    #[serde(with = "u64s")]
    pub at: u64,
    /// When it may start.
    pub release: Release,
    /// What must happen first.
    pub after: Vec<Dep>,
    /// What it does.
    pub action: Action,
    /// The outcomes that count as expected; any other fails the step.
    pub expect: Vec<Outcome>,
    /// Values its outcome provides to later steps.
    pub capture: Vec<Capture>,
    /// Latencies it closes.
    pub measure: Vec<Measure>,
    /// A rule it breaks on purpose.
    pub deviation: Option<Deviation>,
}

/// When a step may start (decision 7).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Release {
    /// Not before its planned time, and not before its dependencies.
    At,
    /// After its dependencies, and `think` microseconds more.
    Ready {
        /// The pause after readiness.
        #[serde(with = "u64s")]
        think: u64,
    },
}

/// A dependency.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Dep {
    /// One step.
    Step {
        /// The step.
        step: StepId,
        /// What of it is awaited.
        on: On,
    },
    /// Every step `label`, occurrence `n`, of the kind's members in the scope,
    /// planned before the holder: a barrier.
    Each {
        /// The members' kind.
        kind: Name,
        /// Their scope.
        #[serde(with = "b64")]
        scope: Vec<u8>,
        /// Their step.
        label: Name,
        /// Its occurrence.
        n: u32,
        /// What of each is awaited.
        on: On,
        /// How many there are: an assertion the validator checks.
        #[serde(with = "crate::wire::u64s_option")]
        count: Option<u64>,
    },
}

/// What of a dependency is awaited.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum On {
    /// That it has started.
    Sent,
    /// That it ended as expected; otherwise the holder is skipped.
    Expected,
    /// That it ended, however.
    Settled,
}

/// What a step does.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    /// A REST call.
    Rest(Rest),
    /// A message published on the bus.
    Bus(Bus),
    /// An effect awaited.
    Observe(Observe),
    /// A call of the stand's own (onboarding a Tenant).
    Host(HostCall),
}

/// A REST call.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rest {
    /// The operation, from the header's table.
    pub op: Name,
    /// The principal who calls, when not the stand's default.
    pub caller: Option<Subject>,
    /// The path's parameters, in order.
    pub path: Vec<Value>,
    /// Query parameters.
    pub query: Vec<Field>,
    /// Headers.
    pub headers: Vec<Field>,
    /// The body.
    pub body: Option<Payload>,
}

/// A named value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Field {
    /// The name.
    pub name: String,
    /// The value.
    pub value: Value,
}

/// A message published on the bus.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bus {
    /// The channel.
    pub channel: Name,
    /// The partition key.
    pub key: Value,
    /// Headers.
    pub headers: Vec<Field>,
    /// The message's place on its subject's stream.
    pub stream: Option<StreamPosition>,
    /// The payload, in plaintext: a decorator seals it at replay.
    pub payload: Payload,
}

/// A message's place on the stream `name` of the step's subject.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamPosition {
    /// The stream.
    pub name: Name,
    /// The position, from 1, rising across the subject's lives.
    pub position: u32,
}

/// An effect awaited: probed until a predicate holds, or until the timeout.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observe {
    /// How it is looked for.
    pub probe: Probe,
    /// What must hold.
    pub until: Vec<Predicate>,
    /// Microseconds between probes, for a polled probe.
    #[serde(with = "u64s")]
    pub every: u64,
    /// Microseconds from the cause's intended start.
    #[serde(with = "u64s")]
    pub timeout: u64,
}

/// How an effect is looked for.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Probe {
    /// A REST call, polled.
    Rest(Rest),
    /// An event on the bus, matched by key and type.
    Bus {
        /// The channel.
        channel: Name,
        /// The partition key.
        key: Value,
        /// The event's type.
        event: Name,
    },
}

/// A call of the stand's own.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostCall {
    /// The call.
    pub op: Name,
    /// Its arguments.
    pub args: Payload,
}

/// An outcome that counts as expected.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Outcome {
    /// A response: its status, a problem's type when refused, predicates on its
    /// body.
    Response {
        /// The status.
        status: u16,
        /// The problem's type, for a refusal.
        problem: Option<Name>,
        /// What must hold of the body.
        body: Vec<Predicate>,
    },
    /// A message accepted by the broker.
    Delivered,
    /// An effect observed.
    Observed,
}

/// Something that must hold of a JSON document: one type for outcomes and
/// observations (decision 2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Predicate {
    /// Where, as an RFC 6901 pointer.
    pub pointer: String,
    /// What.
    pub test: Test,
}

/// A test of a JSON value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Test {
    /// Present.
    Exists,
    /// Equal to the value, as JSON text.
    Equals(Value),
    /// An array containing the value, as JSON text.
    Contains(Value),
}

/// A value an outcome provides to later steps (decision 5).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capture {
    /// Whose value it is.
    pub subject: Subject,
    /// Which of its values.
    pub slot: Name,
    /// Where in the outcome.
    pub source: Source,
    /// Kept in memory only, never in the run's evidence.
    pub secret: bool,
}

/// Where a captured value comes from.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Source {
    /// A response header.
    Header(String),
    /// A JSON pointer into the response body, or into an observed event.
    Json(String),
    /// The `Location` header.
    Location,
}

/// A latency a step closes: from `from`'s intended start to this step's end.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Measure {
    /// The metric.
    pub metric: Name,
    /// The step it is measured from.
    pub from: StepId,
}

/// A rule broken on purpose, and why.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deviation {
    /// The validator's rule it is exempt from.
    pub rule: Name,
    /// What it tests.
    pub reason: String,
}

/// A value: written in the log, or a hole filled at replay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Value {
    /// Text, as it is sent.
    Literal(String),
    /// Filled at replay.
    Hole(Hole),
}

/// Bytes with the spans holes fill (decision 3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Payload {
    /// What the bytes are.
    pub codec: Codec,
    /// The bytes as serialised, each span holding a placeholder the hole's
    /// value replaces at replay.
    #[serde(with = "b64")]
    pub bytes: Vec<u8>,
    /// The spans, in order, disjoint.
    pub holes: Vec<Span>,
}

/// What a payload's bytes are.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Codec {
    /// A JSON document.
    Json,
    /// A protobuf message.
    Protobuf,
    /// Text.
    Text,
}

/// A span of a payload a hole fills.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Span {
    /// Its first byte.
    pub offset: u32,
    /// Its length.
    pub len: u32,
    /// The field it is, as a path: a JSON pointer, or protobuf field numbers.
    pub field: String,
    /// What fills it.
    pub hole: Hole,
    /// How the value is written into it.
    pub form: Form,
}

/// How a hole's value is written into its span.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Form {
    /// A whole JSON token, quoted as JSON quotes it.
    JsonToken,
    /// Raw bytes of exactly the span's length.
    Raw,
}

/// A value the generation cannot know (decision 3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Hole {
    /// A function of the run's namespace, a subject or a step, and a role.
    Rendered {
        /// What it is rendered of.
        of: Of,
        /// Which of its values.
        role: Name,
        /// As what.
        #[serde(rename = "as")]
        rendering: Rendering,
    },
    /// A value an earlier step's outcome provided.
    Captured {
        /// Whose.
        subject: Subject,
        /// Which.
        slot: Name,
        /// The step that provides it.
        from: StepId,
    },
    /// A time, relative to the run or to the send.
    Time {
        /// From when.
        base: TimeBase,
        /// Microseconds after it.
        #[serde(with = "crate::wire::i64s")]
        offset: i64,
        /// As what.
        #[serde(rename = "as")]
        rendering: TimeRendering,
    },
}

/// What a rendered value is rendered of.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Of {
    /// A member.
    Subject(Subject),
    /// A step: a message id.
    Step(StepId),
}

/// How a rendered value is written.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Rendering {
    /// A version 4 UUID, as text.
    UuidText,
    /// A version 4 UUID, as its 16 bytes.
    UuidBytes,
    /// `len` bytes, in lower-case hex.
    Hex {
        /// The bytes.
        len: u8,
    },
    /// 32 bytes of key material.
    Secret32,
}

/// The base of a time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeBase {
    /// The run's start.
    RunStart,
    /// The moment the step is sent.
    Sent,
}

/// How a time is written.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeRendering {
    /// RFC 3339 with microseconds, in UTC.
    Rfc3339Micros,
    /// Milliseconds since the epoch, as decimal text.
    EpochMillis,
}
