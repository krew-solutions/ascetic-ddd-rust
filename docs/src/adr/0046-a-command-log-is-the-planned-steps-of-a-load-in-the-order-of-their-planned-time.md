# ADR-0046: A command log is the planned steps of a load, in the order of their planned time

## Status

Proposed (2026-11-20); slices C1 to C5 built. Stage 2 of three toward the host's fitness functions (ADR-0045 § Status): the Command Log —
what a generation writes and a load run replays — its model, byte form, validator and generation engine. Stage 3,
the scheduler that replays it, is a later ADR; § 12 states what it inherits.

To be built in slices, each a commit of its own:
- **C1:** the crate, the record model, the canonical JSON Lines form with its header and trailer, the digest, the
  golden logs and `FORMAT`, an independent writer and reader.
- **C2:** templates and holes: the builder a host serialises its typed messages through, the rendering of holes at
  replay.
- **C3:** the validator, and an independent reference checker.
- **C4:** the engine: units, time-cell buckets, spills, the canonical merge; the time-major acceptance test; M7.
- **C5:** offered-load rules: planned rates against declared limits, the polls an observation implies.
- **C6** (decision 15): a log at 10⁹ steps — a generation that fails halfway cleans up and says why; compressed logs and
  spills; a bounded merge; a live run with no log, checked chunk by chunk against a validated dry run.
- **C7–C9** (decision 16): the validator within a memory bound — C7 the rules and the report on one clock, C8 streams
  to sorted runs, C9 the window by digest under an early refusal; each followed by its measurement (M10).

Amends ADR-0045 decisions 7 and 8 (§ 4, § 8).

Decided under the library's gate: a blind solver, the main position and a discriminator, then three roles on the
synthesis (Distributed Systems; Verifiability; Software Design with Evolution). § Alternatives Considered keeps
what each refuted.

## Context

**The owner's requirements** (ADR-0045 § Context): the Command Log is the source of every request of a load run,
end-to-end REST tests included; simpler beats faithful; generation must not fall over on billions of records and
runs on many threads, because it is regenerated after every change of the system's public interface.

**The scale the host's quality scenarios imply** (the discriminator, counted from the QAS):

| Source | Load | Records a run |
|---|---|---|
| QAS-SCALE-0001 | 10⁵ enrollments in 4 h, again at three times the backlog | ~10⁶, ~3·10⁶ (about ten calls a session, assumed) |
| The launch-shape drills | 3 Tenants × 10⁶ Devices, 3–5 records a Device | 1–1.5·10⁷ |
| QAS-SCALE-0002 bands | up to 10⁶ Devices reporting | ≤ 8·10⁶ (a 15-minute cadence over 2 h, assumed) |
| QAS-PERF-0007 | 5·10⁴ events/s for 2 h and a tenfold burst, 4 KB each | 3.6·10⁸ + 1.5·10⁸ — no telemetry service exists yet |

So nothing may fall over at 10⁹ — memory bounded, time linear, an external sort — but constant factors and the
format are chosen for 10⁷; the telemetry scale gets its own answer when its service exists (interned payloads, or
stream records the scheduler expands).

**What the host's contracts impose** (verified against them by the discriminator and the roles):
- Many ids are assigned by the server: device groups (`policy-v1.yaml`, `DeviceGroup.device_group_id`), policies,
  drafts, enrollments, and on a full stand the Device itself (`device-enrollment-v1.yaml`, assigned at
  authentication). They are captured from responses, not rendered.
- A repeated enrollment key is answered with its earlier outcome, the same `device_id`: key material must be fresh
  each run.
- A draft's ETag is a KMS MAC (`kms_key_directory.rs` in the host): it cannot be predicted, only captured.
- Device-group membership is a full-list merge-patch without `If-Match`.
- Bus payloads are protobuf, sealed per Tenant with a key reused for a while (ADR-0071 in the host); `occurred_at`
  and `observed_at` are varints.
- A Device's facts position rises strictly across its lives (ADR-0086 in the host); the lifecycle stream is gapless
  by the producer's contract; the inbox waits on declared dependencies, not on gaps (ADR-0006).
- No contract has an `Idempotency-Key`; enrollment operations are limited per client address (120 a minute).

## Decision

**Principle:** the log is the multiset of steps a pure Plan emits, sorted by planned time. Every value the
generation cannot know is a typed hole beside the bytes it fills, never inside them; every order the replay must
keep is an edge, never a coincidence of planned times.

### 1. The log is a stream; the file is its persisted form

A generation's output is one canonical stream of lines. A file is the stream persisted — validated before a run,
kept as the evidence of the offered load. A channel into an in-process executor is the same stream; the run records
its digest. The log's identity is that digest (§ 6), nothing else.

### 2. A record is a step

```rust
pub struct Subject { pub kind: Name, pub scope: ScopeBytes, pub index: u64 }   // faker's Ref, scope as KeyPart bytes
pub struct StepId { pub subject: Subject, pub label: Name, pub n: u32 }       // from identity alone
pub struct Record {
    pub step: StepId,
    pub at: u64,                    // planned, microseconds from the run's start
    pub release: Release,           // At (not before `at`) | Ready { think } (after its dependencies)
    pub after: Vec<Dep>,
    pub action: Action,
    pub expect: Vec<Outcome>,       // any of them is "as expected"; a refusal can be expected
    pub capture: Vec<Capture>,
    pub measure: Vec<Measure>,
    pub deviation: Option<Deviation>, // a rule broken on purpose, with the outcome it must produce
}
pub enum Dep { Step { step: StepId, on: On }, Each { kind: Name, scope: ScopeBytes, label: Name, n: u32, on: On, count: Option<u64> } }
pub enum On { Sent, Expected, Settled }
pub enum Action { Rest(Rest), Bus(Bus), Observe(Observe), Host(HostCall) }
```

- **`Subject`** is faker's `Ref` with its scope carried as the scope's canonical `KeyPart` bytes, not a `u64`: a
  hashed scope would collide at 10⁹ subjects about 3 % of the time (Software Design).
- **`StepId`** is derived from identity: (subject, label, occurrence). Adding a Tenant changes no other step's id.
- **`Dep::Each`** is a barrier: every step of (kind, scope, label, n) planned before the holder. One record does
  not list the 10⁶ edges of a wave. `count` is an assertion the validator checks for equality, not data the
  scheduler trusts. `On::Sent` lets a step start once another has started (the cohort drill's probe of a partition
  held by a publication in progress).
- **`Action::Rest { op, caller, path, query, headers, body }`** and **`Action::Bus { channel, key, headers, stream,
  payload }`**: every field that may hold a run-time value is a `Value` (a literal or a hole). `stream:
  Some { name, position }` numbers the message on (subject, stream). There is no gRPC action until the host has a
  gRPC server.
- **`Action::Observe { probe, until, every, timeout }`** and **`Outcome`** share one predicate type: a JSON pointer, a
  comparison, a `Value`. That is what makes end-to-end assertions expressible.
- **`Capture { subject, slot, source, secret }`**: `secret` values never reach the run's evidence.
- **`Deviation`**: a deliberate stale `If-Match`, a duplicate on a stream, a gap — exempt from the rule it breaks
  and required to produce its declared outcome.

### 3. Holes, beside the bytes

Every payload — a JSON body, a protobuf bus message — is **bytes with spans**. A span names the field it fills and
the hole that fills it:

```rust
pub struct Payload { pub codec: Codec, pub bytes: Vec<u8>, pub holes: Vec<Span> }
pub struct Span { pub offset: u32, pub len: u32, pub field: FieldPath, pub hole: Hole, pub form: Form }
pub enum Hole {
    Rendered { of: Of, role: Name, as_: Rendering },    // Of::Subject(Subject) | Of::Step(StepId)
    Captured { subject: Subject, slot: Name, from: StepId },
    Time { base: TimeBase, offset: i64, as_: TimeRendering },
}
```

- **Rendered** values are a function of (the run's namespace, the subject or step, a role): an id a client chooses,
  a per-run-unique attribute, a message id, key material. The function is the crate's: faker's public derivations
  (SipHash-2-4-128) from a root keyed by the namespace, over the canonical encoding of (of, role), pinned by golden
  vectors computed outside the crate. The subject → id map is therefore a
  function by construction, and the validator can read which kind a hole renders (§ 9).
- **Captured** values come from a response (header, JSON pointer, `Location`) or an observed fact: server-assigned
  ids, ETags, server secrets, values a fact carries.
- **Time** values: an offset from the run's start or from the send.

A host never writes a span. It serialises its typed message — a DTO generated from its OpenAPI, a prost message —
through the builder (C2), which hands out sentinels for the holes, finds them in the serialised bytes, and records
the spans with their field paths. Sentinels never reach the log. A JSON span replaces a whole token; a protobuf span
is fixed-length raw bytes. A varint field (`occurred_at`, `observed_at`) cannot be spliced; the host's decorator
stamps it at replay.

- **A sentinel is a valid value of its field**, or the host's typed message cannot hold it: a version 4 UUID for a
  field typed `Uuid`; for a time, an RFC 3339 time in 2999 in whole seconds, which a typed time writes back as it
  read it; hex digits for hex; ASCII for bytes, so a protobuf `string` takes them. Each carries a tag a host's own
  content would have to spell, and the builder refuses a sentinel found other than once — a number only between
  JSON delimiters — rather than guess. (Found in building C2: raw sentinels of arbitrary bytes, and UUID-shaped
  sentinels for times, could not be put into the fields they stood in.)
- **A form takes what it can write.** A JSON token takes text, an integer (a time in epoch milliseconds) or a
  captured JSON value; a UUID's bytes and key material go raw, at their fixed length, and the builder refuses them
  as JSON tokens.
- **Renderings by family.** A UUID is the rendering key's id under the namespace: one id as text and as bytes. Hex
  and key material are the first words of `key / hex` and `key / secret32`, so a public value rendered under a role
  never shows the bytes of a secret rendered under the same role.
  *Amended by ADR-0047 decision 15 (K2a-2):* key material is not drawn from the namespace. faker's derivations are
  public, so every value drawn from the namespace is known to whoever knows it, and a run's journal names it: a
  `Secret32` is HMAC-SHA256 under the run's secret — 32 bytes the scheduler's shell draws from its OS and holds in
  memory only — of what it is rendered of and its role, each field after its length.

### 4. Ids are rendered from the subject, in this crate (amends ADR-0045 decision 7)

ADR-0045 decision 7 said the log carries the `Ref` or its key, rendered by `Key::uuid` under the namespace. Neither
can be written from outside faker: `Key` has no public byte form (by design: a key is drawn from once). The solver
proposed a faker addition, `IdKey`; Software Design and Verifiability refuted it:
- each unit would derive the id at its own call site, and one differing label gives ids no Device has — groups of
  phantom members, empty cohorts, a passing run — that no rule could see, since the hole carried no subject;
- a golden equation `key.uuid(ns) == key.identity().uuid(ns)` would expose the raw key, and with it the value drawn
  from it;
- `secret` and the other renderings would sit outside faker's golden tables.

So the log carries the subject, and the crate renders from it. faker is unchanged.

### 5. Captures and their order

- A capture writes (subject, slot); a read names its producer, which must be in the reader's `after` with
  `On::Expected`.
- All writers of a (subject, slot) are totally ordered by direct edges, and each reader of writer *i* is in writer
  *i + 1*'s `after`. These are static rules: they hold for every schedule the edges allow, not for the planned one
  alone (Verifiability: a reader ordered before the next writer only by `at` reads a newer value under real
  latency).
- The operations table states which operations write which slots, from the contracts, so a writer that captures
  nothing is still ordered.
- At replay a missing capture skips the reader and is reported as missing, apart from a race.

### 6. Byte form, digest, trailer

- **JSON Lines, canonical.** One record a line, a header line first, a trailer line last. Keys in a fixed order (the
  structs' field order), no whitespace, `\n` endings. Integers that can pass 2⁵³ — indices, offsets of 64 bits —
  are strings. Payload bytes and scope bytes are base64 (standard alphabet, padded). No floating point in a record.
  A normative JSON Schema describes a line.
- **`FORMAT`** is the digest of checked-in golden logs covering every record and hole variant and the hard cases
  (base64 with `+`, `/`, `=`; integers at 2⁶⁴ − 1; non-ASCII text in a payload), regenerated only on purpose, like
  `ALGORITHM`. An independent writer and reader in Python reproduce the golden bytes and their digests. A reader
  refuses a line whose canonical re-encoding differs from its bytes, and refuses unknown fields; the format version
  must equal its own.
- **The digest** is a streaming SHA-256 over the raw bytes of every line but the trailer: `sha256sum` checks it,
  which SipHash would not allow. The trailer carries the record count and the digest; a reader refuses a log without
  it, so a truncated log is never replayed as a shorter, passing run.
- **The header**: the format version; faker's `ALGORITHM` and the scenario's name and revision, as provenance; the
  stand profile (§ 10); the window W and the read-ahead L (§ 7); the retained labels; the laws' `Display`; the
  operations table (method, path, content type, the headers decorators supply, the slots each writes, its retry
  class, its declared limits — generated by the host from its contracts).
- **No compression, no chunks, no shards** in this decision: at 10⁷ records a log is a few GB, regenerated in
  minutes. Each comes with the measurement that needs it (M7).

### 7. Order and release

- The log is ordered by `(at, StepId)`; every edge points backward, `at(dep) < at(step)`; equal keys are a
  generation error.
- **Release** (the scheduler's, stated here because the generator plans for it): an `At` step at `max(at, ready)`,
  a `Ready` step at `ready + think`; a `Ready` step's `at` is a lower bound, never a promise.
- **W**, declared in the header, is how far behind the read cursor, in planned time, a completion must be kept;
  **retained labels** are kept longer (a group created at the start, its Devices joining hours later). **L** is how
  far ahead the scheduler reads. A dependency older than W that is not retained is a validation error.

### 8. Identity and staleness (amends ADR-0045 decision 8)

A log is identified by its digest. Before a run it is regenerated, or reused only when a build-computed digest of
the generator's sources, its lock, the contracts it read and its arguments equals the one it was made with.
`ALGORITHM` and the scenario's revision are provenance: a reader does not refuse on them. ADR-0045 decision 8's "a
reader refuses a mismatch" and its `testing::fingerprint` (never built) give way to this: a fingerprint over the
Plan misses a change in emission code, and one over the output is the digest itself (Software Design).

### 9. The validator

One streaming pass, on the read cursor's clock (decision 16; what it holds and for how long, there):
- **framing**: the header, canonical lines, the trailer's count and digest, the format version;
- **order**: `(at, StepId)` strictly increasing; StepIds unique (in a set up to a budget, by external sort past it);
- **edges**: every `Dep::Step` refers to an earlier step within W or a retained label; a barrier's `count`, when
  given, equals the matching steps planned before it;
- **captures**: § 5's rules;
- **streams**: on each (subject, stream), positions rise strictly — across the subject's lives — and lie in
  [1, 2³¹ − 1]; each message has its predecessor on the stream in `after` with `On::Expected`; a stream the profile
  declares gapless has no gap; a fact depends on the `managed` that opened its life; a message's declared causal
  dependency is in its `after`;
- **holes**: every span inside its payload, spans disjoint, the field it names decodes to exactly that span (a host
  hook decodes); rendered roles per kind as the profile declares; a hole of a kind the profile declares captured is
  refused;
- **operations**: known, path arity, the slots it writes;
- **peaks it reports**: live captures, completions within W, the polls observations imply.

Its oracle is not itself: an independent reference checker (Python, O(n²) by definition) is compared with it on
small random logs and on their one-edit mutants, and every rule has a minimal violating log beside a passing twin.

What the rules mean where the list above leaves it open (settled in building C3, each decided by both the validator
and the reference):
- **Awaiting has a strength**, `Sent` < `Settled` < `Expected`: a stronger wait meets a weaker requirement.
- **An edge** is to the latest earlier step of that id, planned strictly before the holder, within W behind the read
  cursor or of a retained label (revised by decision 16, which first read "within W of it": a run that has read a
  line ahead has dropped what lies more than W behind it). The window is evicted exactly by planned time, so a step
  in it is within reach; a step more than W behind the cursor on arrival is not kept and takes an older occurrence of
  its id with it.
- **Captures (§ 5).** A writer of a slot is a step that captures it, or a REST call whose operation writes it on the
  step's subject. A reader awaits its producer as expected, and the producer is the slot's latest earlier writer.
  A writer awaits the previous writer as at least settled — its value must be stored before the next — and each
  reader of it as at least sent, which is when the reader takes the value.
- **Ids and the profile.** A rendered `id` only of a kind the profile renders; a captured `id` only of one it
  captures.
- **A deviation** exempts its line from the rule it names; one naming no rule, or a rule its line keeps, is itself a
  violation.
- **Uniqueness** compares 16-byte digests of the step ids with their lines, sorted in runs past a budget and merged
  at the end: two equal digests are one id but with probability 2⁻¹²⁸ a pair.
- **The host** (`validate::Host`) decodes its spans and names the dependencies only its domain knows — the
  `managed` that opened a Device's life before a fact of it, a message's declared causal dependency — and the
  validator checks that they are awaited.
- **Framing** fails the pass; every other rule is collected, so one pass reports every violation.

### 10. The stand profile

The header names the stand a log is for: which kinds' ids are rendered and which captured, which streams are
gapless, the emulated producers' sealing configuration, the egress model (§ 14), the client-side jitter and the
rate reserve (§ 14). (Replica counts are not in it: every limit is a floor per gateway instance and per replica, and a
VIP does not balance per request — § 14.) On the
policy stand a Device's id is rendered, so groups can name Devices before they arrive (ADR-0045 decision 6); on a
full stand it is captured at enrollment, and the cohort scenario needs groups filled after enrollment — blocked on
the device-group contract (ADR-0045 § Owed). A host chooses rendered or captured in one function per kind, never at
a call site.

### 11. The engine

- A **unit** is a partition of identity the Plan fixes (a population family, a scope, an index range). It returns
  its records — any order — each with a planned `at` that is a pure function of identity and the Plan: a unit
  computes its dependencies' planned times, never looks them up.
- Records go to **time-cell buckets**, spilled to disk past a byte budget; the number of cells is capped. Cells are
  sorted by `(at, StepId)` and concatenated into the `Sink`.
- The thread count decides who sorts which bucket, never a byte.
- Per-cell enumeration (lag classes, rank-stratified phases) may feed the same merge later, when a measured volume
  needs it; it changes no byte of the format.
- **Folds** — aggregates written by another population's members — are not built: the device-group contract is
  asked for delta membership or `If-Match` first (ADR-0045 § Owed).
- **Two seams**: a unit's returned records (host → engine, unordered) and the `Sink` (engine → destination, the
  canonical stream). ADR-0045 decision 11 stands; it named the destination.

Settled in building C4:
- **The order key** is `at` big-endian, then the step id in an encoding whose byte order is the step id's order —
  strings and bytes with each 0 written as `0 255` and ended by `0 0`, integers big-endian. Sorting and merging
  compare bytes; no line is parsed again.
- **A thread's buckets** spill every cell they hold, each as a sorted run, when the thread holds more than its
  budget; a record past the last cell goes to the last one. Each cell is merged in turn from the runs and what the
  threads still hold, which the threads sort first.
- **Two records with one `(at, StepId)` fail the generation** (decision 7): they would let the spill order choose the
  bytes. A unit's refusal fails it too, and the spills are removed either way.

### 12. What stage 3 inherits

The scheduler:
- builds a bus step's frame once — `message_id` rendered from the step, headers, stamped times, payload sealed with
  the emulated producer's key reuse (not a fresh key a message: about 10³ times the consumer's KMS calls) — and
  resends that frame byte for byte on every attempt; a reseal under the same id is `Differs` at the inbox;
- retries by the operation's retry class: what the real client retries is load and measured; a harness retry only
  when "not taken" is certain; an unknown outcome on a non-idempotent operation fails the step;
- releases by § 7, reports lateness split into dependency-caused and its own, holds a token bucket per limit key;
- arms fact subscriptions from the run's start; a fact's deadline runs from its cause's intended start;
- counts failures in percentiles as censored at their timeout, never drops them; carries a correlation id per step
  so the host's inbox times join for the per-hop breakdown QAS-SCALE-0001 requires;
- runs only on a stand of its log's profile, after a quiescence gate, with a namespace the stand has not seen; A/B
  pairs ordered ABBA or from a snapshot;
- offers load by § 14: shapes every send against the log's buckets, keyed by actual bytes, and reports the lateness
  shaping causes as its own cause; voids a run in which a step a `Measure` spans was shaped, in which a decorator sent
  a request to a limited operation that is not a planned step, or whose egress collapsed; never retries a poll nor
  sends one while the previous is outstanding; treats an outcome listed in `expect` as final, never retried; sends
  from one connection pool per source index; waits, in quiescence, at least the longest `burst · per_seconds /
  requests` of the log's limits since its sources and operations were last used.

### 13. Crates

- **Library: `crates/command-log`** — the record model, holes and their rendering, the canonical form, the digest,
  the validator and its pure dependency machine, the engine. One crate until stage 3's scheduler, which shares the
  dependency machine; the engine's spill and thread code stays generation-only. It carries faker's lint gate:
  `at` is computed there.
- **Host:** a tool package of its own, the owner's to place (STYLE.md's `test` layer is dev-dependencies only):
  scenarios, the operations table generated from its contracts by an `openapi-dto` generator, end-to-end fixtures
  written in Rust against its typed DTOs, stand profiles, and, for stage 3, decorators (auth and step-up, DPoP,
  sealing, envelope stamping).

### 14. Offered load (C5)

A load run must fail on the system and never on the generator's own excess: a planned load above a declared rate
limit is refused before the run, and the scheduler never sends what the limit refuses.

- **Limits in the header.** `Operation.limits: Vec<Limit { key, bucket, by, requests, per_seconds, burst }>`, the
  values the stand applies (a configured quota's, else the declared). `key` is the host's word, for reports only;
  `by: By { All, Source, At(Place) }` with `Place { Path(name), Query(name), Header(name), Body(field), Credential(kind)
  }` gives the semantics, so the library holds no host vocabulary. `bucket` names the counter: limits with one name
  share one counter per value, as the host's counters live — one `provisioning-channel` counter serves sixteen SCIM
  operations, so a bucket is not per operation. One bucket name with two shapes (`by` included), a zero parameter or a
  `Path` that names no parameter of its template is a `header` violation, and a refused limit is not evaluated.
- **Egress and callers in the records.** The profile gains `sources` (distinct source addresses; an IPv6 source a /64
  of its own), `jitter` (how much the client side — network, timer, send loop — can delay a send; not the service tier,
  whose queueing before its counter is the system's) and `reserve` (parts per thousand of every rate left unplanned:
  room for retries after a stall, and for clock-rate offset). `Rest` gains `source` (an index below `sources`; the
  scenario decides it — a store's Devices behind one NAT share one) and its `caller` becomes `Caller { principal,
  names }`, the subjects its credential names (its Tenant, its channel).
- **A key's value** is derived where the server reads it: `All` one value; `Source` the source index; `At(place)` the
  `Value` there reduced to its subject (a rendered or captured value of a subject is that subject, of a step that
  step, a literal its text); `Credential(kind)` the caller's one name of that kind. A `Time` hole, an absent place, a
  body field with no span, no caller, zero or two names of the kind, or a source past `sources` is a `key` violation,
  which no deviation exempts: an exempt arrival left out of its bucket would be an under-count.
- **Arrivals.** A REST action arrives at `c` — the running maximum of `at` up to its line, `at` itself in an ordered log
  — and an observation with a REST probe polls at `c + k · every`, `k = 0 ..= ⌊timeout / every⌋`: to the timeout, since
  the slow run is the one that matters, so polls are bounded above, never matched. `every = 0` or more than 2²⁰ polls is
  an `operation` violation, its first poll alone counted. Arrivals are ordered by (time, line, poll) and a violation is
  reported on the arrival's line.
- **The bucket** is the arrival curve of a token bucket full at t = 0, at rate `R · (1 − reserve)` per `P`, for
  arrivals each delayed by up to `J`: any window of planned width at most `J` holds at most `B` arrivals, and a wider
  one of width `w` at most `B + R′ · (w − J) / P` — exact for delays in `[0, J]`, and with `J = 0` the plain token bucket.
  Integers throughout (`R′ = R · (1000 − reserve)`, `P′ = 1000 · per_seconds · 10⁶`). A refused arrival spends nothing, as
  the host's GCRA does. A `rate` violation is an arrival the curve refuses.
- **Deliberate refusals.** A `rate`-deviated arrival spends in every bucket it counts in, refused or not: the host's
  counters do not all spend nothing on a refusal (the enrollment open spends the token's budget before its repeat
  check), and a model that spends no less than the system admits no more. It is used only if a lower model — burst
  `B + ⌈R · J / P⌉`, refusals spending nothing — refuses it too, so that a planned 429 the system would admit is reported
  as a deviation that breaks nothing.
- **What the validator does not prove.** Dependency latency moves sends: a reader released when its producer answers
  is sent later than planned, and readers of unequally late producers bunch. The validator proves the planned schedule
  under client-side jitter; the scheduler absorbs the rest by shaping (§ 12), and a run whose measures it shaped is void,
  not failed. The interval model that would prove it — each arrival an interval from its dependencies' declared bounds —
  is § Alternatives', with the measurement that would bring it in.
- **One module for both**, `command_log::rate`: arrivals, key values, the curve. The validator and the scheduler call
  it; `vectors/reference.py` decides `rate` and `key` from the definitions, window by window.

### 15. 10⁹ steps: compression, a live run, failure halfway (C6)

The owner's direction (§ Owed, 2026-11-20): the persisted log is compressed with zstd, its digest over the canonical
lines; as an alternative, a generation streams straight into the run with no log. Measured first (M8): zstd level 1
makes the 4.50 GB log of M7 162 MB, level 3 176 MB; SHA-256 runs at 79 MB/s here.

- **One crate, no option.** `zstd` (libzstd bundled by zstd-sys through `cc`; the workspace builds C already), no
  feature of ours and no `zstdmt`: a generation at 10⁹ spills on either path, and libzstd's streaming output does not
  depend on its workers (since 1.4.7), SHA-256 being the serial bound. ruzstd encodes only its fastest level.
- **The persisted log** is one zstd stream of the canonical lines, level 1 by default (an option of the writer);
  `FORMAT`, the trailer and its SHA-256 over the uncompressed lines unchanged, the header silent on compression — one
  plan, one digest. The reader tells a compressed log by its magic, decodes concatenated frames, refuses anything else.
  `Encoder::finish` is called and its error kept, before `sync_all` and the rename.
- **Spills** compressed at level 1, one encoder a thread. A run's end is a clean end of its file, never a short read: a
  run's record count is checked at merge, in the engine and in the validator's id runs alike (a cut zstd frame and a
  cut record both read as `UnexpectedEof`, which both merges took for the end — a run cut at a record boundary dropped
  its steps and the trailer still matched).
- **Bounded merges.** A cell's runs, and the validator's id runs, merge in a cascade of a bounded fan-in, derived from
  the soft open-file limit (a quarter of it, between 2 and 256); equal keys across groups still fail. The library does
  not raise a process-wide limit behind its caller's back: a host that wants a wider merge raises it. A cell's runs are
  deleted as soon as it is merged, a cascade group's as soon as the group is.
- **Failure halfway.** Each pass owns a fresh subdirectory of the spill directory, locked; a guard removes it on every
  exit, unwinding included, and a pass sweeps unlocked siblings left by a kill and reports what it freed. A unit's
  refusal or panic stops the other threads before their next unit. After the emit phase, the free space of each
  filesystem is checked against what the merge will need — the largest cell times the cascade's depth, the validator's
  24 bytes a step, the log if it is elsewhere — and a shortfall fails before the first line, naming both. A spill
  directory on tmpfs is refused unless the caller allows it. Errors carry their phase and path; a progress observer
  reports the phase, units, records, spill bytes, cells merged and lines written.
- **The live run.** A dry pass (the engine into a bounded pipe, the reader, the validator) writes a commitment only when
  its report is clean: `FORMAT`, the validator's identity, the host's fingerprint, the trailer, an order-independent
  digest of the step lines, and prefix digests `Pᵢ` — the one SHA-256 over the header and the first `i · 2¹⁶` step
  lines, `zstd -dc | head | sha256sum`'s to check. The live pass refuses before emitting if `P₀` (the header) or the
  identities differ, before releasing anything if the step count or the multiset digest differ, and stops before
  releasing chunk `i` if `Pᵢ` differs. A persisted replay goes through the same verified reader. The dry pass is once a
  plan revision — the bytes do not depend on the namespace.
- **Considered, not assessed:** `structured-zstd` (pure Rust, 0.4, claiming every level), found while building; libzstd
  is the reference implementation a host's operators already run, and a young encoder would be a second thing to
  trust.
- **Not now:** passes over planned time (`Unit::emit(window)`) — their trigger is a compressed spill that does not fit,
  and a window invites a unit to prune and lose a step; a `Sink` beside `W: Write` — a pipe and the reader serve; a
  hashing thread — M9 decides.

### 16. The validator's memory: a hot part by the read cursor, a cold part in sorted runs (M9)

M9 measured the validator at 1.97 GB for 10⁷ steps, growing through the pass: § 9's "in memory bounded by the window"
holds for the window alone. Per rule the validator keeps a map from a key to that key's history, and every such map
is one structure: a **hot part** in memory, evicted by one clock, and a **cold part** in sorted runs on disk, decided
by a per-key merge at the end. A lookup is decided by the hot part where it is authoritative and deferred to the merge
otherwise. The report is a function of the log alone, never of a budget. Uniqueness (§ 9, `Ids`) is already this
structure with a budgeted hot part; the rest follows it.

- **One clock: the read cursor.** `c` is the running maximum of `at` up to the line, updated before its rules. An
  edge from a line to step `D` holds iff `D`'s latest earlier occurrence `E` exists, `E.at` is before the line's `at`,
  and `E` is retained or `c − E.at ≤ W`; the window evicts by `c − W`, exact because `c` only grows. This revises
  § 9's "within W of it" to decision 7's "behind the read cursor" and decision 14's arrival at the cursor: a run that
  has read a line at 10 000 has dropped a completion at 50, so a later line at 150 awaiting it cannot be honoured. On
  a log in order nothing changes — every generated log is in order (§ 11) — and the reference takes the same rule.
- **Nothing leaves memory by being dropped.** Forgetting a stream or a slot whose latest step left the window keeps
  clean logs clean and passes violating ones: a stream's message more than W after its predecessor, which it does not
  await, would read as the stream's beginning (refuted in review with three logs, now twins). Whatever leaves the hot
  part is written out.
- **Streams first, as runs with an empty hot part.** One event a stream message — its (subject, stream) digest, line,
  position, step digest, its `Expected` dependencies' digests, the line's deviation — to sorted runs; each key
  replayed in line order at the end, which is the reference's definition. A hot table with snapshots at eviction is
  the same structure with a non-empty hot part; held back until a measurement shows the stream merge above about a
  tenth of the pass.
- **Identities as 16-byte digests** (SipHash-1-3-128, the engine's) in the hot parts and the runs. In a lookup a
  collision is fail-open (a dependency on a step never planned passes), in uniqueness fail-closed; about 2⁻⁶⁹ at 10⁹
  for logs not crafted to collide. The validator is not a security boundary. Details name lines, not ids from maps.
- **The window by digest**, under the cursor; a budget with edges deferred to the id merge only if a measurement shows
  the window, at its measured bytes an entry, past what the laptop leaves beside the generation.
- **The report:** `kept` keeps the least violations by (line, rule, detail), whatever decides them when; no deviation
  exempts `deviation`; `Peaks.captures` counts live captures (§ 9's meaning), and `Peaks.polls` is documented as the
  sum it is. `RULES_REVISION` goes to 3.
- **The room check counts each run type** before the first line, in both directions (the id runs' cascade group is
  asked for only when there is a cascade, and for every level of it), and `validate_file` checks it too.
- **Named unbounded until measured on a host scenario:** slots and their readers, barrier counts (a `Dep::Each`
  counts the whole history), buckets, declared deviations, retained steps (multi-life subjects whose messages lie more
  than W apart need their predecessor retained), a line's length and a detail's. § 9 names them; their runs or caps
  wait for a host scenario at 10⁸ with captures, per-key limits and barriers.

- **The bound is the plan's own:** the window holds about W times the peak planned rate, which the run must hold
  too (decision 7, `Peaks.completions`). A plan past what the validator's machine holds is refused early and by name —
  in a dry run from the steps a cell counted while emitting, on both paths by a cap on hot entries that fails the pass
  naming the structure and its count — not killed by oomd hours in. A step dead on arrival (`c − at > W`, not
  retained) is not kept and removes its id from the window: no later edge may accept it, and an out-of-order line
  cannot pin the window behind it.
- **Streams' events carry what their rules read:** the stream's name, and only the `Expected` dependencies of the same
  subject, the only ones that can be its predecessor. Run buffers are budgeted in bytes under one budget shared by the
  run types; a run is read through an explicit buffer counted in memory; hot parts are dropped before the end merges.
- **Room:** a dry run's need is exact, from what the emit phase counted per record; a persisted validation, whose
  counts are known only at its trailer, checks before each run it writes against a reserve. Each is executed against
  the bytes the runs actually occupy, not against the formula.

**C7** (first): the cursor in the validator and the reference; a step dead on arrival not kept; no deviation exempting
`deviation`; `kept` the least by (line, rule, detail), ranked before a detail is formatted; live captures (slots whose
latest writer is within W of the cursor or retained, counted as the window evicts); the reference computing the peaks
from their definitions; `RULES_REVISION` 3, with the corpus's verdicts digested beside it. The corpus gains a line
moved forward past 2W and deviations of every rule's name and an unknown one, and counts the logs whose reference
verdict differs between the two clocks — above zero, or V2 does not execute the cursor. **C8:** streams to sorted
runs, the run machinery generalised from `Ids`, duplicates fed straight into the report, room per run type; V2 at run
budgets of 1 and 2 and a fan-in of 2, a long clean stream through small runs, every run type cut at every byte; then
M10 at 10⁸. **C9:** the window, order and retained by digest, the early refusal; a counting allocator asserting the
peak heap flat across eight times the subjects at one planned rate; then M10: a dense 10⁸ on both paths (10⁹ was
dropped for this laptop by the owner on 2026-11-21: it would measure only this machine's throughput, some 14 hours
persisted and 17 more without a log; the stand's scale is measured on the stand). **A window budget with edges deferred to the id merge** waits for a host plan whose W times rate
the run's machine holds and the validator's does not.

M10 records, beside wall time by phase and peak RSS: the planned rate and W, `Peaks.completions` and the retained
count, the hot tables' capacities and resident memory at each resize, bytes an entry, stream events and their bytes,
same-subject dependencies an event, time writing, sorting and merging runs, the spill disk's kind and read-ahead,
available memory, swap and pressure — under `MemorySwapMax=0`, so that a blown bound is a kill and not a thrash.

## Consequences

- A load is a value: a digest names exactly what was offered, and two runs of one log offered the same commands.
- A host writes typed code; spans, pointers and offsets are the builder's.
- The log carries everything the scheduler needs to keep order without trusting planned times, and nothing the
  scheduler must look up in the generator.
- End-to-end tests are generated logs too: a renamed DTO field fails to compile instead of sending a field the
  service ignores.

## Rule of Three

1. **Phantom ids.** Two call sites render one Device's id differently, or a full stand assigns it while the log
   renders it: groups of members that do not exist, empty cohorts, a passing run. Decided by rendering from the
   subject (§ 4) and the profile (§ 10); checked by the rendered-roles rule and, at replay, a capture never read
   before its producer ran.
2. **An order that holds only on schedule.** A reader, a stream message or a barrier member ordered only by `at`
   breaks under real latency: stale `If-Match`, a parked lifecycle change, a barrier firing early. Decided by edges
   (§ 5, § 9); checked by the static rules and by the reference checker's mutants.
3. **A log the build changed.** A body passed through `serde_json::Value` is reordered under the host's
   `preserve_order`, or renumbered without `arbitrary_precision`; the wire differs from the log. Decided by bytes
   with spans (§ 3) and a canonical form (§ 6); checked by `FORMAT`, the independent writer, and the crate in the
   host-features gate.
4. **A truncated log.** A writer killed at a line boundary leaves a valid prefix. Decided by the trailer; checked by
   truncation at every line.
5. **A retry that is a new message.** A resealed bus message, or a fresh id, reads as a producer defect at the
   inbox. Decided by § 12's frame built once; checked in stage 3.
6. **Load the generator offers that the stand refuses.** 2.8 enrollments a second from one address against 120 a
   minute: 429s fail the QAS for the generator's sake. Decided by limits and the egress model in the header; checked
   by C5's token buckets over planned times.

## Alternatives Considered

- **For decision 16, streams and slots evicted with the window** (the main position): memory by W, not subjects; it
  passes a stream's message more than W after its predecessor that does not await it, a writer more than W after the
  last, a gapless stream restarted at 1 — each now a V1 twin. **Every structure decided by runs, the window included**
  (a window budget of 0): no window memory for 31 GB more disk at 10⁹; waits for its trigger. **A hot stream table
  with snapshots at eviction**: exact, a twentieth of the stream runs on long streams, but every stream's first message
  misses it; waits for a measurement of the whole stream path above a tenth of the pass. **Interned names** instead of
  digests: exact and invertible, but the table grows with the names a log uses. **Partitioning by subject and
  re-reading the log**: 3.7 h a read at 10⁹.
- **For C5, arrivals as intervals** (the Distributed Systems role): each arrival `[lo, hi]` from its dependencies'
  declared bounds, the curve checked against every placement. It would prove conformance under latency, but needs a
  bound for every dependency in the header, and, counting an observation's polls as independent intervals, refuses
  plans whose readers float by a 15 s timeout though their polls are rigidly spaced. Not chosen while shaping at the
  scheduler absorbs latency; the measurement that would bring it in is the shaped share of real runs.
- **For C5, key values stated per call** (the main position): a copy the host writes beside the bytes it sends can
  drift from them; derived from the place the server reads, it cannot. **An address list in the profile**: two IPv6
  addresses of one /64 are one bucket at the gateway, and the list binds the digest to a stand's addresses; a count
  and an index per call are the list's special case without either. **Limits folded with replica counts**: a VIP does
  not balance per request, so a plan folded at two replicas lands on one. **The burst reduced by `⌈R · J / P⌉` for every
  window** (the blind solver's): it refuses `readEnrollmentNotice` (burst 400, 200 a second) at any `J ≥ 2 s` even at
  one request a second; the two-regime curve is exact.

- **Protobuf records in zstd chunks with BLAKE3** (the solver). JSON Lines is enough at 10⁷ and needs no new
  dependency; the escaping objection fell to bytes with spans; the chunks' consumers (shards, resume, parallel
  validation) are deferred.
- **A log ordered by time cell, read a cell at a time** (the main position). A burst fills one cell beyond any
  window: QAS-PERF-0007's tenfold burst is ≈ 9 GB in a minute's cell.
- **Units of time cells enumerating their steps** (the main position). It needs an inverse for every family of steps,
  loses or doubles a step silently at a cell boundary when one is wrong, and its lag classes made a Device's lag
  depend on its arrival cell — breaking ADR-0045 decision 6's common random numbers. Kept as a later producer.
- **Captures by step with consumer counts** (the main position). Counts are unknown for picks by a law; the table by
  (subject, slot) is O(subjects).
- **`IdKey` in faker** (the solver, the discriminator). § 4.
- **JSON bodies with holes by pointer, parsed at replay** (the discriminator). A parse through `serde_json::Value`
  makes the wire depend on the host's serde features; spans over bytes do not.
- **A `predicted aggregate_version`** (the main position). The contracts carry none; the ETag is a MAC.
- **Contract digests over whole files in the header, a reader refusing a stale log** (the synthesis). Twenty-three
  commits to `policy-v1.yaml` in a month, most unrelated: false staleness daily; and compared against the
  scheduler's checkout, not the deployed stand: false freshness. § 8.
- **Hand-written JSON Lines fixtures** (the synthesis). Not regenerable; a renamed field passes a schema without
  `additionalProperties: false`. End-to-end fixtures are Rust against the typed DTOs.
- **The validator as the scheduler run dry, alone.** One schedule, and one implementation to be wrong twice:
  static rules over edges, and an independent checker.

## Executors

| Id | Slice | Asserts | Mutation it turns red |
|---|---|---|---|
| F1 | C1 | every record and hole variant round-trips; a line whose re-encoding differs is refused; unknown fields refused; u64 at 2⁶⁴ − 1 written as a string (a property on every u64 field) | a u64 written as a number; a field dropped |
| F2 | C1 | `FORMAT` is the golden log's SHA-256; `make vectors` checks the golden log outside the crate (`vectors/format.py`: the normative schema, the canonical key order, re-encoding, the trailer), the standard library only | key order from a map; another base64 alphabet; whitespace |
| F3 | C1 | a log truncated at every line and mid-line is refused; the atomic rename does not happen after a failed write | the trailer optional |
| F4 | C1 | the crate in the host-features build gives the golden bytes | a body through `Value` under `preserve_order` |
| H1 | C2 | the builder's spans: bytes outside the holes equal the template's; splicing real values gives the bytes the typed message serialises to with them, end to end through a decorator | an offset off by one; a token half replaced |
| H2 | C2 | rendering against golden vectors computed outside the crate (Python): each role and rendering, two namespaces, two subjects; secret halves differ | rendering without the namespace; a role ignored |
| V1 | C3 | one minimal violating log per rule beside its passing twin | a rule disabled |
| V2 | C3 | the validator agrees with the reference checker (`vectors/reference.py`) on 120 random small logs and their one-edit mutants (an edge dropped, an `at` moved, a StepId duplicated, a position renumbered, a barrier count ±1, a capture's producer swapped, an unknown operation, spans, a profile's id, a deviation), every (line, rule) pair, with uniqueness spilled to disk; a test of the crate | a rule's condition weakened |
| E1 | C4 | bytes identical across threads {1, 2, 7} × bucket budgets {unbounded, one record, a few} × cell caps {1, many}, units reversed | spills merged by `at` alone; spills in thread order; a last buffer unflushed |
| E2 | C4 | the time-major acceptance: Tenants over a day, Devices over hours, reports, a publication's facts; validates; per-chunk dispersion ≈ 1 for an open total, the exact total for a fixed one, `apportion` as the negative control | arrivals by `apportion`; time in a key |
| R1 | C5 | minimal twins (`tests/rate.rs`): with `R = 2` a second and `B = 3`, three at 0 admitted and a fourth refused, the fourth at `T − 1 µs` refused and at `T` admitted, four at 0 then an hour idle refused (an average far under the rate), three at 0 then one every `1.2 T` admitted; `R = 3` a second, `B = 1` at 333,333 µs refused and 333,334 admitted; `R = 3`, `B = 2` at 0, 0, 333,334, 666,667 admitted; two operations in one bucket each under and together over; two values in one bucket each under and together over, admitted; one subject through a rendered and a captured value together over; polls alone over, beside `every` doubled; 5 s / 2 s polls at burst 2 refused and at burst 3 admitted, 4 s / 2 s at burst 2 refused; 2²⁰ and 2²⁰ + 1 polls; the drain of polls before an action at the same time, at the log's end, and before the deviation check; `J`: `B` arrivals within `J` admitted and `B + 1` refused, the `readEnrollmentNotice` shape (`B = 400`, 200 a second, `J = 3 s`) at one a second admitted; the reserve: 99 % of `R` admitted without and refused at 5 %; a source past `sources`, each unevaluable place, a `key` deviation still `key`; a deliberate 429 the system would admit reported unused (both the forced spend and `J` cases); a refused limit not evaluated; the clamp for an action and for an observation | the average instead of the bucket; `<` for `≤`; T rounded either way; polls counted as `timeout/every`, `div_ceil` or a strict bound; a bucket per operation or per name instead of per value; the clamp dropped; a deviated arrival that does not spend; use decided on the admission model; `key` deviable; the burst reduced for windows under `J` |
| R2 | C5 | the reference decides `rate` and `key` window by window from the definitions, its deviated arrivals spending; the corpus carries limits of every `By` and `Place`, sources, callers, observations with REST probes, rates whose interval is comparable to the gaps, jitter whose `R · J / P` is not an integer; mutants squeeze an `at`, a source past `sources`, `every` halved, a key made a `Time` hole, a caller's names dropped, a deviation naming `rate` or `key`; measured: arrivals whose verdict differs from a plain count, by mutant kind | the reference sharing a mistake with the validator |
| R3 | C5 | a property: the streaming curve equals the window definition on random arrivals in one bucket with small parameters, ties and deviations included | the streaming algorithm diverging from the definition |
| V3 | C7 | twins: a line planned past W ahead puts a step out of reach of a later line near it (`edge`), within W it does not; a step dead on arrival answers for no edge and an older occurrence of its id does not answer for it; the window holds only what is within W behind the cursor (21, not the 31 an eviction in line order holds); a deviation against `deviation` is unused; with `kept = 1` a duplicate on line 3 decided at the end is kept before an edge found on line 4; captures counted while live; a stream's late message, a slot's late writer and a gapless stream begun again still break their rules | a step dead on arrival kept; an older occurrence answering for a dead one; the window evicted in line order; captures counted ever written; live captures not evicted; a deviation exempting itself; `kept` by discovery |
| V4 | C7 | the reference decides on the cursor and computes the peaks from their definitions; the corpus moves a line past 2W ahead and declares deviations of every rule's name and one naming none; the reference fails unless some log's verdict differs between the cursor and the line's own time (47 of 1,642); the corpus's logs, verdicts and peaks are digested beside `RULES_REVISION` | the reference on the line's own time; its captures ever written; any Rust mutation above that V2 sees |
| V5 | C8 | every corpus log validated with each event its own run through a fan-in of 2, and a few events a run through 3, gives its whole in-memory report, details and peaks included; a gapless stream of 10⁴ messages through runs of a few hundred is clean at fan-ins 2, 3 and derived, and one message dropped breaks it; a stream falling back is broken where it falls; a stream run cut at every byte, and an event cut inside its dependencies, fail the pass; the third run one byte short of its look stops the pass before writing it; a dry run's asked space covers the runs' peak, heard after every write and checked against the files, across memories and fan-ins | stream events sorted without their line; a cascade group dropped; a run's end unchecked; no look before a write; the cascade's second copy not asked; runs asked for though they fit; a settled wait taken as expected; gaps, predecessors or duplicates unchecked at the end; a write not counted or the end not told |
| V6 | C9 | a hot part past its limit fails the pass naming it, its count and line — completions, captures, barrier counts, buckets, deviations, polls — and at its limit does not; a dry run whose cells hold more steps within W than the limit is refused before its first line, also across a cell's edge, and steps past the last cell are left to the in-pass limit; the validator's peak heap at 6.4·10⁴ steps is within 64 KiB of its peak at 8·10³ at one planned rate (`tests/heap.rs`, a counting allocator) | the window never evicted; the fan-in not held to the memory; no hot limit, or any part left out of it; no refusal before a line; a span a cell short; the last cell bounding though steps pass it; steps past it not counted |
| Z1 | C6 | compressed and plain logs give the same records and trailer; `make vectors` decodes a crate-written log with the zstd CLI and `cmp`s it with the plain one | a digest over compressed bytes; compression in the header |
| Z2 | C6 | a two-frame `golden/c1.jsonl.zst` made by the CLI decodes to the golden log | single-frame decoding |
| Z3 | C6 | a compressed log named `.jsonl` and a plain one named `.zst` both read; anything else refused | detection by extension |
| Z4 | C6 | a compressed log cut at every byte refused; a failing last write leaves no file | `finish`'s error dropped; a rename after a failure |
| E3 | C6 | a spill run cut at every byte, record boundaries included, fails the generation; the same for the validator's id runs | a short read taken for a run's end |
| E4 | C6 | bytes identical across fan-in {2, 3, unbounded} × the E1 matrix, a duplicate split across groups refused; the FD test: 10⁶ steps under a soft limit of 64 files | a group dropped; an equal key across groups passing; an unbounded merge |
| E5 | C6 | a disk watcher sees no run of a cell after it merges; the spill directory is empty after a refusal, an equal key, a panic, a truncated input and a write that fails partway | deletion at the end; removal on success only; no guard for unwinding |
| E6 | C6 | after a refusal at most `threads` units start; a free-space port short by one byte refuses before the first line; a spill directory on tmpfs refused without the option; the observer reaches every unit and every line | the stop flag unchecked; the validator's term dropped from the space check; the tmpfs check removed; a counter never advanced |
| L1 | C6 | a stale header emits no unit; a differing unit releases no chunk; chunk `i` differing delivers chunks `0..i` and an error naming `i`; `Pᵢ` equals `head | sha256sum`; a stalled consumer keeps memory flat | `P₀` checked late; the multiset check skipped; release before compare; an off-by-one boundary; an unbounded pipe |
| MUT | each | a `cargo mutants` run on the crate; survivors killed or argued equivalent in § Measured | — |

## Measured

**C1** (2026-11-20, this laptop):
- **The byte form.** The golden log `crates/command-log/golden/c1.jsonl` (7 steps, every record and hole variant, base64
  with `+`, `/` and `=`, `u64::MAX` and `i64::MIN`, non-ASCII text, quotes, backslashes and newlines in a payload);
  `FORMAT` is its SHA-256, `8ffd12b7…87e4b`. `crates/command-log/vectors/format.py`, written against the normative
  schema with the standard library only, validates it, finds every object's keys in the schema's order, re-encodes
  every line to the same bytes and recomputes the trailer; it refuses keys reordered, a u64 as a number, whitespace,
  an unknown field, a leading zero, unpadded base64, a step dropped, a missing trailer. `make vectors` runs it in
  `make all`; `verify/inbox-features` writes and reads the golden log under the host's serde_json features (F4).
- **The mutations**, each applied alone, `cargo test -p ascetic-ddd-command-log --no-fail-fast`; every one red:

| Mutation | Red |
|---|---|
| a u64 written as a JSON number | F1, F2, F3 |
| the reader's canonical re-encoding check dropped | F1 (whitespace) |
| the trailer hashed into its own digest | F1, F3 |
| a line after the trailer accepted | F3 |
| a failed file renamed into place anyway | F3 |
| unknown fields accepted on a record | F1 |
| base64 in the URL-safe alphabet | F1, F2, F3 |
| a step not hashed by the writer | F2, F3 |

- **The mutation run** (cargo-mutants 27.1.0, `cargo mutants -p ascetic-ddd-command-log`): 56 mutants, 40 caught, 16
  unviable after one round. The first run missed five: the field checks of a u64's and an i64's canonical text, which
  the reader's re-encoding check covers twice but a host decoding one line with `serde_json` does not, and
  `LogError`'s text. Each now has a test of its own.

**C2** (2026-11-20, this laptop):
- **H1.** A JSON DTO serialised through the template — three ids, a time as RFC 3339, a time in epoch milliseconds,
  and a free text drawn by a property over quotes, backslashes, non-ASCII and a look-alike of a number sentinel — and
  spliced, is byte for byte the DTO serialised with its real values. A protobuf message with a UUID's bytes, a UUID's
  text in a `string` field and a fixed64 time, spliced and then passed through a decorator that rewrites a varint
  before them, is the message encoded with its real values. Filling every span with its own bytes gives the template
  back.
- **H2.** `golden/render.txt` (512 lines) is written by `vectors/render.py` from faker's independent SipHash and
  Python's `datetime`: three renderings at two namespaces, two subjects and a step, two roles; 476 times at the
  calendar's edges — the epoch, every year's turn and end of February in every hundredth year, 0000-01-01 to
  9999-12-31. `make vectors` checks the script against the file, the crate checks itself against it.
- **What building it found.** The first sentinels could not stand in their fields: raw ones held bytes that are not
  UTF-8, so a protobuf `string` would not take them, and times got UUID-shaped text, which a typed time would refuse.
  Each sentinel is now a valid value of its field (decision 3). Hex and key material drawn from the same words would
  have let a public value show a secret's first bytes under a reused role: they are drawn by family (decision 3).
- **The mutation run** (cargo-mutants 27.1.0, `cargo mutants -p ascetic-ddd-command-log`): 180 mutants, 160 caught,
  20 unviable, after one round. The first run missed 14: the century corrections of the calendar (`+ doe/36524`,
  `− doe/146096`), which act only within days of 1 March in a century year, where the first vectors had no date;
  the raw sentinels of a UUID's text, of hex and of a time, which fell back to bytes unseen; a seven-bit mask, which
  only a 129th hole shows; spans overlapping; the errors' texts. A second run over the survivors' descriptions: 30
  mutants, 30 caught.

**C3** (2026-11-20, this laptop):
- **V1.** Every rule has a minimal violating log beside a passing twin (`tests/validate.rs`), boundaries included: an
  edge at exactly W and at W + 1, a retained dependency past W, equal times ordered by the step id, a stream at
  2³¹ − 1 and 2³¹, a capture that is a write though its operation writes nothing, a duplicate on purpose decided
  only at the end. Uniqueness spilled with budgets 1, 2 and 7 gives what it gives in memory, line for line.
- **V2.** 1,066 logs, 120 random and 946 mutants, each validated with three ids in memory so that every one spills:
  the reference reaches every verdict, (line, rule) pair for pair. Violations by rule: barrier 85, deviation 120,
  duplicate 120, edge 213, gap 85, operation 103, order 98, producer 18, profile 70, span 102, stale 20, stream 57,
  writers 10 (header and required, which the corpus cannot reach, by V1).
- **What V2 found.** The first validator relied on eviction for W: a line moved back in time stayed in the window, and
  a later edge to it more than W away passed. The reference refused it. The distance is now checked per edge. (On
  review: evicting a step id removed the entry a later duplicate had put back; eviction now removes only its own.)
- **The mutation run** (cargo-mutants 27.1.0, `--file crates/command-log/src/validate.rs`): 126 mutants, 94 caught, 4
  timed out (a merge that never reads its runs' end), 13 unviable, 15 missed. Killed by tests that were missing (13):
  holes inside a payload's spans and an observation's predicates, which no rule's twin held; a captured `id` against
  the profile; the spill — a budget not reached writing no run, and one passed writing runs during the pass, watched
  through a host hook, since the verdicts are the same either way; an error reading a run failing the pass instead
  of ending the run; the cap on kept violations; `validate_file`; the texts. Equivalent (2): `Host`'s default
  methods, whose bodies are the mutants. A second run over the survivors' descriptions: 18 mutants, 16 caught, the
  two defaults missed.

**C4** (2026-11-20, this laptop):
- **E1.** One byte form across threads {1, 2, 7} × budgets {unbounded, one byte — every record spills — and 2,000
  bytes} × cell caps {1, 64} × the units forward and reversed: 72 generations of 1,564 steps, 70 of them sharing an
  instant, each equal to the steps sorted in memory and written; the log validates. Mutations, each applied alone,
  red: the key by `at` alone (the ties fail as equal keys); runs read in their order instead of merged; the last
  held buffer dropped; a cell not monotone in time. Equal keys fail the generation spilled or not, and a unit's
  refusal fails it; no spill is left.
- **What E1 found.** Its first plan put no two steps at one instant, and a key by `at` alone passed: it now has a
  thousand instants for 391 Devices. The same plan first planned a fact before the fact it awaits, and the
  validator refused the log.
- **E2.** A day: Tenants arriving, each with a Poisson(3) number of Devices managed over the next six hours, three
  facts each an hour apart on the gapless `lifecycle` stream, a publication three hours after the Tenant and the
  facts of applying it; a unit per Tenant, three threads, 1 MiB budgets, an hour a cell; every log validates. An open
  total (a Poisson(1,500) number of Tenants, 1,515 drawn): by quarter-hour, χ² against the mean p = 0.56. A fixed
  total of 1,200: exact, p = 0.67. The negative control, so many a quarter-hour by `apportion`: p = 1.0, refused.
  Common random numbers: a day half as long moves every arrival and changes no Device's attributes; the negative
  control, attributes from a key of the arrival time, changes them.
- **M7** (`tests/scale.rs`, release, `/usr/bin/time -v`, a disk sampler each second): 2.5·10⁶ Devices, each managed
  once and telling three facts, on four threads with 64 MiB budgets and 64 cells a day.

| Steps | Log | A step | Wall | CPU | Peak RSS | Peak disk |
|---|---|---|---|---|---|---|
| 10⁷ | 4.50 GB | managed 282 B, fact 506 B | 4 min 55 s | 139 s user, 15 s system (51 % of 4) | 381 MB | 9.41 GB |

  The run is bound by the disk: it writes the log and a spill of every record (9.4 GB, `time`'s outputs). Peak disk
  is 2.09 times the log; memory is the threads' budgets and does not grow with the steps. Extrapolated, 10⁹ steps
  are a 450 GB log, about 940 GB at the peak and some eight hours: they do not fit this laptop (369 GB free), and
  10⁹ was not run. This is the measurement § Consequences named: it brings compression or chunks back (§ Owed).
- **The mutation run** (cargo-mutants 27.1.0, `--file crates/command-log/src/engine.rs`): 27 mutants, 11 caught, 8
  unviable, 8 missed. Killed by tests that were missing (7): the key's encoding of strings and bytes and its escape of
  0 — E1's ties differ by index only — now a property over step ids with zeros and prefixes in their strings and
  scopes; a thread that never spills, or spills below its budget — the bytes are the same either way — watched by a
  unit that counts the spill directory as it emits; an error reading a run failing the generation; the errors'
  texts. Equivalent (1): spilling at the budget rather than past it, one record's memory. A second run over the
  survivors' descriptions: 8 mutants, 7 caught.

**C5** (2026-11-20, this laptop):
- **The byte form.** Format 2: `Operation.limits`, `Profile.sources`, `jitter` and `reserve`, `Rest.source`, `Caller`;
  the golden log carries a limit of every `By` and `Place`, a source above 0, a jitter and a reserve; `FORMAT`
  `ea30b014…f5ce8`; `vectors/format.py` checks it against the schema, now of format 2.
- **R1.** The curve's boundaries as unit twins (`src/rate.rs`) — the token bucket at no jitter (a burst of three, the
  fourth at `T − 1 µs` and at `T`, an hour idle, a rate under its interval), the interval that is no whole microsecond
  both ways, `B` and `B + 1` within `J`, `readEnrollmentNotice` at one a second under `J = 3 s`, 99 % of the rate with
  and without the reserve, the lower model's certain and uncertain refusals — and twelve validator twins
  (`tests/rate.rs`): one counter across operations, one per value, one subject through a rendered and a captured
  value, sources, every unevaluable place, `key` not deviable, polls to the timeout (5 s / 2 s at bursts 2 and 3,
  4 s / 2 s, `every` doubled), 2²⁰ and 2²⁰ + 1 polls, polls drained before a later line at their time and at the end,
  deliberate refusals used only when certain and spending, the clamp for an action and an observation, jitter's
  short windows, refused limits not evaluated.
- **R3.** The streaming curve equals the window definition on random arrivals in one bucket — ties, forced spends,
  `J ∈ {0, 1, 250,000, 600,000}`, reserves of 0, 250 and 500 per thousand.
- **R2.** 1,522 logs, 120 random and 1,402 mutants, under limits whose intervals are comparable to the gaps (a source's
  250 µs) and jitter whose `R · J / P` is no integer: the reference reaches every verdict. `rate` in 535 pairs, `key`
  in 324. By kind: 23 of 120 random logs break `rate`; `every` halved breaks all 65; a squeezed `at` 19 of 86; a source
  past the profile's, a key made a time and a caller's names dropped give `key` in every one.
- **What building found.** A limit the header refused was still evaluated: a `Path` naming no parameter gave `key`
  violations beside the `header` one. Refused limits — every limit of a bucket of two shapes among them — are now never
  evaluated.
- **The mutation run** (cargo-mutants 27.1.0, `rate.rs` whole and `validate.rs`'s offered-load functions, tests `--lib
  --test rate --test validate`): 129 mutants, 108 caught, 11 unviable, 10 missed. Killed by twins that were missing
  (6): a query and a body place that find their own field among others; a zero `requests` and a zero `burst`, not only
  a zero period; the polls' cap with a limit present, so that polls past it would arrive; an observation whose timeout
  is shorter than its interval polling once. Equivalent (4): `Polling`'s equality, which the heap never calls (it
  orders by `Ord`); an arrival exactly `J` before aging one call later, where a window of width `J` has the bound `B`
  in either regime. A second run over the survivors' descriptions: 12 mutants, 8 caught.

**C6, first part — failure halfway** (2026-11-21, this laptop):
- **M8** (the zstd CLI 1.5.7 on M7's 4.50 GB log, read from disk): `-1` 65 s → 162 MB; `-3` 90 s → 176 MB; `-6` 87–107 s
  → 155 MB; `-9` 83–105 s → 152 MB; decompression 4.7 s; `zstd -d | sha256sum` 57 s (SHA-256 79 MB/s); `sha256sum` 69 s.
  Every compression ran at the disk's read speed; the synthetic log is far more repetitive than a real one.
- **Built:** a pass's own spill directory (`pass-N` under the caller's, claimed under a lock, removed on every exit,
  unwinding included; a killed pass's directory swept by the next, a live one's kept); every run counted, a run cut
  short or longer than written an error with its path; the engine's units stopped after the first failure, a panic
  caught and reported with its message; spill errors carrying their phase and path.
- **Executors** (`tests/spill.rs`, the readers' unit tests): a spill cut at every byte of a run, record boundaries and
  nothing included, fails the generation and leaves nothing; an id run cut at every byte fails the validation; a panic,
  and a validation of a log cut short, leave nothing; a killed pass is swept, a locked one kept; after a refusal a
  handful of a thousand units start, and a running unit stops before its hundred thousandth record. Mutations, each
  alone, red: a short read taken for a run's end, in the engine and in the validator; the stop flag unchecked before a
  unit, and at a record; the pass directory kept on exit; orphans never swept. A write failing partway is covered by
  the directory's guard; its own executor (a child under `RLIMIT_FSIZE`) comes with the free-space check.

**C6, second part — compression and bounded merges** (2026-11-21, this laptop):
- **Built:** `Compression { None, Zstd { level } }` for a log file (level 1 by default), `write_file` and
  `engine::generate_file` writing whole or not at all, the zstd frame finished and its error kept; `open` and
  `open_file` telling a compressed log by its magic, decoding concatenated frames, refusing anything else; spills as
  zstd streams at level 1, read back held to their counts; a cascade of bounded fan-in for the engine's runs and the
  validator's id runs, each group's runs deleted once merged, a cell's once it is; a `Disk` port (free space, a
  filesystem's identity, memory filesystems, the open-file limit) and its `rustix` implementation; the room check after
  the emit phase, before the first line; a spill directory on tmpfs refused unless allowed.
- **Executors** (`tests/zstd.rs`, `tests/fd.rs`, `make vectors`): compressed and plain logs give the same records and
  trailer, and the zstd CLI decodes the crate's compressed golden log to the plain one; a two-frame golden made by the
  CLI reads as the golden log; a log is told by its bytes, garbage refused; a compressed log cut at every byte refused;
  the bytes equal across fan-in 2, 3 and derived, a step twice across groups refused; the first cell's runs gone while
  the second is written; short of one byte the generation is refused before a line is written, and the space needed
  counts the spills; a spill in memory refused for the engine and the validator; three thousand runs merged, and three
  thousand id runs, under a soft limit of 64 open files. Mutations, each alone, red: the log's zstd frame not finished;
  a compressed log read as plain; single-frame decoding; the cascade dropping its last group; equal keys passing; a
  cell's runs kept to the end; the room check without the spills; a spill in memory let through; the engine's fan-in
  unbounded; the validator's fan-in unbounded.

**C6, third part — the live run** (2026-11-21, this laptop):
- **Built:** `engine::generate_checked`, a check between the emit phase and the merge, seeing `Emitted` — the step count
  and the sum modulo 2¹²⁸ of a 128-bit SipHash of each line, computed on the emit threads; `live::dry`, the plan
  generated into a bounded pipe and validated as it comes, writing a `Commitment` (`FORMAT`, the validator's identity,
  the host's fingerprint, `P₀`, the chunk size, the prefixes, the emitted count and digest, the trailer's digest) only
  for a clean report, and reporting a failed generation as itself; `live::live`, refusing before it emits on another
  format, validator, fingerprint or header, before it writes a line on another emission, and releasing each chunk only
  once its prefix digest matches, the last held back if the trailer's does not. The validator's identity is its rules'
  revision (`RULES_REVISION`, 2 since C5) and names.
- **Executors** (`tests/live.rs`): a live run delivers the very log a persisted generation writes; `P₀` and every `Pᵢ`
  equal the SHA-256 of the log's first `1 + i · chunk` lines; a stale header, format, validator or fingerprint refused
  with no unit run and nothing written; another emission refused with nothing written; a forged `Pᵢ` delivers exactly
  the chunks before `i`, a forged trailer digest every whole chunk and not the rest; a dirty report leaves no
  commitment; a failing generation reported as its own refusal; 19 steps in chunks of four make four prefixes, the
  trailer counting for none; the validator's identity pinned. Mutations, each alone, red: a chunk released before its
  comparison; comparison at the end only; an off-by-one boundary; `P₀` checked late; the emitted check skipped; the
  validator's error reported before the generation's; a commitment for a dirty report; the trailer hashed into the
  chain.
- **Owed of decision 15:** the verified reader for a persisted replay (stage 3 consumes it); an executor for a stalled
  consumer (the pipe is bounded: 64 blocks of up to 1 MiB) and for a write failing partway (`RLIMIT_FSIZE`); M9. All
  but M9 built in the fifth part.

**C6, fourth part — the observer and the room check revisited** (2026-11-21, this laptop):
- **Defect found in the second part's room check:** a log written on the spills' filesystem was not counted — the check
  asked for the largest cell and the margin, and a plain log, several times the compressed spills, would have filled
  the disk it had just been let onto. Found while adding the validator's term, not by an executor; the second part's
  E6 used one fake filesystem and a compressed log, where the omission costs about one cell.
- **Built:** the room check as a sum per filesystem with the margin once. Cell by cell, in merge order, a merge holds at
  most the cell's runs again (the cascade deletes each group's inputs once merged, so its depth multiplies nothing — the
  decision's "times the cascade's depth" was conservative past need) and then writes the cell's share of the log,
  deleting the cell's runs after: a log beside the spills needs the peak of what it has gained less what it gave back,
  plus that cell; a log elsewhere needs every share there and the largest cell beside the spills. A plain log's share is
  its lines' bytes; a compressed one's is estimated at the spills' measured ratio, conservative since the runs carry
  keys and lengths (M9 measures the log's ratio against the spills'). `engine::generate_checked`'s check names further
  needs; `live::dry_on` names the validator's: 24 bytes a step and one cascade group more, none while the ids fit in
  memory. A `Progress` observer in `Config` hears the sweep's bytes, every unit with the steps and spilled bytes so
  far, the cells to merge, and every cell merged with the lines written so far. `generate_file_on` and `dry_on` take
  the disk port, as `generate_on` and `validate_on` do.
- **Executors** (`tests/room.rs`): a plain log on a filesystem of its own needs exactly its step lines and the margin
  there, nothing written; beside its spills a plain log needs more than half its lines and a compressed one less; a dry
  run refused one byte short of the validator's need and let through at it, no need while the ids fit, the engine's
  and the validator's needs adding up on one filesystem; the observer hears the swept bytes, every unit, the cells
  before any merge and every cell in order with the lines so far, and one unit's spills with it. Mutations, each
  alone, red: the steps', spilled bytes' and lines' counters each never advanced (the spilled one lagging a unit, until
  the one-unit executor); the sweep not reported; the log's share ignored beside the spills; a compressed log's share
  taken as its lines; a plain log elsewhere ignored; needs on one filesystem not added; the validator's need dropped;
  its cascade group dropped.

**C6, fifth part — one verified reader for a live run and a persisted replay** (2026-11-21, this laptop):
- **Reworked from the third part:** the live run checked its chunks in a writer wrapped around `out`, while the decision
  names one verified reader for the live run and a persisted replay alike. The check is now `live::Verified<R:
  BufRead>`: the header at once, each chunk released once its last line makes `Pᵢ`, the last lines with the trailer's
  digest, a cut last line never released so the log reads as truncated. The live run generates into the bounded pipe
  and copies through it; a divergence is `LogError::Diverged` through the reader and `EngineError::Diverged` from the
  run, and outranks the broken pipe it leaves the generation. No state keeps the reader failed after a divergence:
  each prefix digest chains every byte before it, so nothing after can match.
- **Built:** `live::replay` (a persisted log, plain or compressed, read through its commitment); `live::commit_file`
  (a persisted log validated, and the commitment written only for a clean report — the very commitment a dry run of its
  plan leaves); `log::decoded` (a log's lines before any is read, plain or compressed); `Config::pipe_blocks` (blocks
  of up to 1 MiB in flight between a generation and its reader).
- **Executors** (`tests/live.rs`, `tests/fsize.rs`, the unit test of the pipe): a persisted log, plain and compressed,
  commits what its dry run commits; a dirty one commits nothing; a replay reads every record a plain read does; a byte
  changed in any step releases exactly the chunks before its own and names it, the last chunk named with the trailer,
  a changed header opening nothing, another validator refused; a log cut at every byte releases only whole checked
  chunks and reads as truncated; a divergence while the generation is blocked on a one-block pipe is what the run
  reports; a stalled consumer blocks the writer past the pipe's blocks; a log written past `RLIMIT_FSIZE` (SIGXFSZ
  ignored) fails with EFBIG and leaves no log, partial log or spill. Mutations, each alone, red: a cut line fed to the
  verifier; a chunk released before its comparison; the last chunk released without the trailer's check; the header
  released unchecked; the replay's validator unchecked; the trailer tallied as a step; a dirty persisted log
  committed; the generation's error before the reader's (green until the one-block executor); an unbounded pipe; a
  divergence read as an I/O error; a partial log kept.

- **M9** (`tests/scale.rs` module `m9`, release, inside `systemd-run --user --scope -p MemoryMax=6G` under
  `/usr/bin/time -v`, beside a sampler of resident memory, open files, disk used and memory pressure each second; this
  laptop: i3-4005U, two cores and four threads, no SHA extensions, 11 GiB, one filesystem). M7's scenario — devices
  managed once and telling three facts an hour apart on a gapless stream — with a header window of two hours; four
  threads, 64 MiB budgets, 64 cells a day, the log and the spills at zstd level 1.

| Path | Steps | Emit | Merge | Done | Read | Validated | Peak RSS | Open files | Peak disk |
|---|---|---|---|---|---|---|---|---|---|
| persisted | 10⁷ | 28 s | 74 s | 103 s, a 162.5 MB log | 132 s, 75.7·10³ steps/s | 305 s, 32.8·10³ steps/s | 1.97 GB | 77 | 0.29 GB |
| dry | 10⁷ | 28 s | 308 s, paced by the validator | 336 s | — | inside | 2.23 GB | 77 | 0.29 GB |
| live | 10⁷ | 28 s | 66 s, into a sink | 93 s | — | — | as the dry run | 77 | 0.29 GB |
| persisted | 10⁸ | 289 s | 1,186 s | 1,475 s, a 1.62 GB log | 1,333 s, 75.0·10³ steps/s | not run | 644 MB | 262 | 2.94 GB |

  - The compressed log is 27.7 times smaller than M7's plain 4.50 GB at 10⁷ (a synthetic scenario, far more repetitive
    than a host's); emission is linear (28 s → 289 s); the merge per line slows from 135·10³ to 84·10³ lines/s, about
    700 runs a cell at 10⁸ passing the fan-in of 256 through one cascade level.
  - SHA-256 in process: `sha2` 95 MB/s soft, 110 MB/s with `asm` (+16 %): not taken. At 10⁷ the digest is about 47 s of
    the merge's 74 s (4.5 GB at 95 MB/s), so the hashing thread decision 15 left to M9 is the larger lever; owed.
  - The soft open-file limit here is 524,288; the fan-in clamps to 256.
  - **The validator does not fit at 10⁸.** Its resident memory grows through the pass, linearly as sampled, to 1.97 GB
    at 10⁷: state kept for every stream ever seen, never evicted, and the window, which grows with the planned rate
    times W. Ten times that is past this laptop, so validation at 10⁸ (persisted, dry and live) was not run; decision 16
    is owed under the gate.
  - The first attempt found two defects, both fixed before the numbers above. The harness kept the fixture's window of
    1 ms, so the validator rightly refused facts awaiting steps an hour back (`edge`). The dry run measured the
    validator's spill directory before the validator, which starts only with the merge's first line, had created it
    (ENOENT): the dry run now creates it first (executor `a_dry_run_claims_the_validator_s_directory_first`, mutation —
    the directory not created first — red).

**C7 — the rules and the report on one clock** (decision 16, 2026-11-21):
- **Built:** the read cursor (the running maximum of `at`, updated first) is the one clock: the window is a map and a
  set ordered by planned time, evicted exactly behind the cursor, and a line's calls arrive at it; a step dead on
  arrival is not kept and removes its id; the edge rule's own distance check is gone, eviction being exact; a
  deviation never exempts `deviation`; the report keeps the least `kept` violations by (line, rule, detail), a detail
  written only when it could be kept; `Peaks.captures` counts the slots whose latest writer is within W behind the
  cursor or retained, as the window evicts; `Peaks` and the report documented as they are; `RULES_REVISION` 3. The
  reference measures W behind the cursor, computes the three peaks from their definitions, compares them, and counts
  the logs only the cursor decides. The corpus gains `jump` and every rule's deviation: 1,642 logs agree, 47 only on
  the cursor's clock.
- **Executors:** V3, V4. Mutations, each alone, red: as in V3 and V4. One is equivalent: the window evicted by the
  line's own time instead of the cursor — the cursor rises only on a line whose time it is, and eviction runs on
  every line, so both evict the same steps; the cursor lives on in the edge rule, through a step dead on arrival.

**C8 — streams to sorted runs** (decision 16, 2026-11-21):
- **Built:** `runs::Sorter<E: Event>`, the run machinery generalised from the id store: a buffer, sorted runs of
  variable-length events, a cascade of a bounded fan-in, a k-way merge handing every event back in order, each run
  read through a 1 MiB buffer, each write preceded by a look at the disk for its bytes and a 64 MiB reserve
  (`LogError::Space`, by name, before the write). Step ids and stream messages are its two kinds, both digested with
  SipHash-1-3-128 over their canonical JSON: an id event 24 bytes, a stream event 49 and 16 a dependency — its
  (subject, stream) key, line, position, the stream's gaplessness, its step and the steps it awaits as expected on
  its own subject. Streams are decided at the end, replayed per stream in line order; duplicates go straight into the
  report. `Limits::memory` (bytes, 256 MiB by default) replaces the id count and is shared: past it the larger buffer
  goes; a merge's fan-in is held to a 1 MiB block a run within it; the hot parts are dropped before the end merges.
  The dry run's need is the bytes of events its records make, counted while emitting (`Emitted::run_bytes`): none
  while they fit, the runs once, twice when they cascade. The engine's `Progress` hears a validation's runs
  (`runs(kind, on_disk)`), at each write and when they are gone. The look after a write, first written beside the
  look before, was dropped: it can fail only where the next look would, and served only to observe — the observer
  does that.
- **Executors:** V5; the unit test of the run reader covers both kinds. Mutations, each alone, red: as in V5. A
  deletion inside a cascade is heard with the next write, not at once: a deletion never makes a peak.
- **Owed of C8, to C9's counting allocator:** that the hot parts dropped before the end merges and the fan-in held to
  the memory bound the heap (C9 executes the second).

**M10 after C8** (2026-11-21, M9's harness and laptop, 10⁸ steps of M9's scenario, a day of plan, W two hours, under
`MemoryMax=6G` and `MemorySwapMax=0`; the spill disk rotating, an HGST HTS545050A7E680 with 8 MiB read-ahead; 8.3 GB
available at the start, the process never swapping):

| Path | Generation | Validation | Peak RSS | Validator's runs at most | Peak disk |
|---|---|---|---|---|---|
| persisted | 1,709 s, a 1.62 GB log | 3,487 s, 28.7·10³ steps/s | 5.45 GB | streams 4.38 GB, ids 2.25 GB | 8.76 GB |
| dry | 286 s emitting, then paced by the validator | 4,298 s in all | 5.89 GB | as persisted | 7.13 GB |
| live | 1,613 s, into a sink | — | as the dry run | — | — |

  - Validation now runs at 10⁸ (M9 could not); the plan's own peak, `Peaks.completions`, is 8.34·10⁶. The streams'
    end merge took about 6 minutes, the ids' under one; the stream path is about a tenth of the validation, the hot
    table's trigger at its edge.
  - The resident memory is the window keyed by full step ids, some 550 bytes an entry: the dry run peaked 110 MB
    short of the cap. C9 keys it by digests.

**C9 — the window by digest, an early refusal** (decision 16, 2026-11-21):
- **Built:** the window, its order and the retained keyed by the ids' 16-byte digests (computed once a line, and
  once a dependency); `Limits::hot` (2²⁵ entries by default) bounding each hot part — completions, captured slots,
  barrier counts, buckets, declared deviations, polls in flight — the pass failing past it with `LogError::Hot`,
  naming the part, its count and the line; the engine counting steps a cell while emitting and `Emitted::window`, the
  most any ⌈W / cell⌉ + 1 consecutive cells hold, so that a dry run whose plan keeps more completions than the limit is
  refused before its first line — the last cell, gathering steps planned past its end, bounding nothing.
- **Executors:** V6. Mutations, each alone, red: as in V6. The counting allocator executes C8's owed items too: the
  fan-in held to the memory (its mutation red); the hot parts dropped before the end merges remain unexecuted — the
  scenario's hot parts are too small to show — and are argued: the end merges read through `memory`, the hot parts
  are what the pass needed before them.

**M10 after C9** (2026-11-21, as M10 after C8):

| Path | Generation | Validation | Peak RSS | Before C9 |
|---|---|---|---|---|
| persisted | 1,684 s | 2,480 s, 40.3·10³ steps/s | 1.77 GB | 1 h 27 min, 5.45 GB |
| dry, then live | 291 s emitting, 3,191 s in all, then a live run of 1,215 s | inside the dry run | 2.19 GB | 1 h 39 min, 5.89 GB |

  - In all, 1 h 09 min persisted and 1 h 13 min without a log. The window's 8.34·10⁶ completions take at most about
    150 bytes an entry — the resident memory less the events' 256 MiB and the end merges' read buffers — a third of
    what full step ids took; without the ids' clones validation runs 40 % faster.
  - The decision's bound holds on this laptop for M9's scenario at 10⁸: a window of the plan's own size beside a
    generation, under the 6 GiB cap with room. A window budget (S4) stays on its trigger.

Owed:
- **Stage 3's dependency machine against the edge rule** (decision 16): whether the scheduler binds a dependency when
  it reads a line or when it releases it, with read-ahead, decides what the cursor must be; both oracles derive the
  rule from one sentence of decision 7, so their agreement proves nothing about it.
- **One SHA-256 a pass** (M9, decision 16's review): `commit_file` hashes a log twice on the validator's thread (the
  tally and the reader), the dry run twice on the generator's (the writer and the tally) — about 4.7 µs a step each
  here, 1.3 h at 10⁹ — beside the hashing thread M9 left owed.
- **M9's rest** (C6): the host scenario's bytes a step and its ratio at levels 1 and 3 — the host has no scenario yet.

## Owed

- **C5's host side, the owner's:** declare where a counter is shared (the `provisioning-channel` scope is in no
  contract, only in `scim/cap.rs`), so the generator's bucket names come from a contract; a live check that the gateway
  refills continuously (spend the burst, send at 90 % of the rate for one `per_seconds`, expect no refusal); two SCIM
  operations on one channel refused at 101 together; a measurement of whether an `X-Request-Id` survives the compose
  gateway in `direct` mode, which decides the egress join's key.

- **The device-group contract** (ADR-0045 § Owed): delta membership or `If-Match`. Until then folds are not built and
  the full-stand cohort scenario cannot be expressed.
- **The scheduler** (stage 3): § 12, its own ADR, its own gate; a TLA+ model of the dependency machine is its
  candidate executor, the library's `verify/tla` pattern.
- **Telemetry scale:** interned payloads or stream records, when the telemetry service exists.

## References

ADR-0002 (the outbox carries bytes), ADR-0006 (a message ahead of its dependency waits), ADR-0038 (one text at the
outbox, one value at the inbox), ADR-0045 (the faker); in the host: ADR-0071 (sealed payloads), ADR-0086 (a Device's
condition and its clocks), QAS-SCALE-0001, QAS-SCALE-0002, QAS-PERF-0007.

## Change Log

- 2026-11-20: Proposed.
  - **The blind solver**: records with typed holes, strict (at, StepId) order, protobuf in zstd chunks, units of
    identity placed by an external sort, captures by subject, two seams, an `IdKey` in faker. **The main position**:
    JSON Lines, cell-granular order, units of time cells with lag classes, captures by step with counts.
  - **The discriminator** verified the solver's reading of the contracts, refuted the main position's cell order,
    lag classes, `$id` without a key, version oracle and missing barrier, and the solver's "a group's sequence is
    cross-population"; counted the scale the QAS imply; synthesised JSON Lines, strict order, the solver's engine
    first.
  - **The roles** added: Distributed Systems — frames built once and resent byte for byte, sealing with the
    producer's key reuse, stream predecessors as edges, static capture rules, barrier semantics, retry classes, the
    egress model, fact deadlines, censored failures, quiescence and namespaces for A/B; Verifiability — subjects in
    rendered holes, a pinned byte form with `FORMAT` and an independent writer, a trailer, the spill matrix, the
    arrival acceptance, an independent reference checker, rate limits by token bucket; Software Design and
    Evolution — rendering from the subject instead of `IdKey`, end-to-end fixtures in Rust, the log's identity as its
    digest with regeneration, scopes as bytes, `On::Sent`, one predicate type, deviations, a host tool package.
- 2026-11-20: Slice C1 built (§ Measured).
- 2026-11-20: Slice C2 built (§ Measured). Decision 3 refined from what building it found: a sentinel is a valid value
  of the field it stands in; a form takes only what it can write; hex and key material are drawn by family, apart
  from each other.
- 2026-11-20: Slice C3 built (§ Measured). Decision 9's open meanings settled — awaiting by strength, edges to the
  latest earlier step within W checked per edge, the order of a slot's writers and readers, ids against the profile,
  deviations as exemptions, uniqueness by digests with an external sort, the host's hooks — and decided alike by the
  validator and the independent reference.
- 2026-11-20: Slice C4 built (§ Measured). Decision 11's open details settled: the byte order key, a thread's
  buckets spilled whole past its budget, equal keys and refusals failing the generation. M7 measured at 10⁷; 10⁹ does
  not fit the laptop, and the choice it forces is owed.
- 2026-11-20: Decision 14 (offered load, C5) under the gate: a blind solver (limits in the header with their place,
  egress as a count and an index, a caller's names, a burst reduced by jitter, a shaping scheduler), the main position
  (limits folded with quotas and replicas, key values stated per call, an address list); the discriminator refuted a
  refusal spending nothing everywhere, buckets per operation (SCIM shares one) and the solver's "can only over-count",
  and synthesised; two roles — Distributed Systems (arrivals float with their dependencies; the exact jitter curve;
  retries of polls and of deliberate refusals; a reserve; decorators; the egress join's key) and Verifiability (the use
  of a deliberate refusal decided on a lower model; executors for every boundary; the corpus reaching time). § 10's
  replica counts dropped; § 12 gains the scheduler's obligations.
- 2026-11-20: Slice C5 built (§ Measured), by decision 14.
- 2026-11-21: Decision 15 (C6) under the gate: the main position (compressed log and spills behind a feature, a `Sink`,
  dry and live digests compared at the end), a blind solver (compressed log and spills, no feature, passes over time
  windows, a bounded fan-in, chunk commitments, hashing on its own thread); the discriminator confirmed the crates' facts
  (ruzstd's encoder, libzstd's worker-independent output, the CI runner's zstd), found the short-read defect in the
  engine's spill reader, refuted `zstdmt`, level 3 and time windows for now, and synthesised prefix digests of the one
  SHA-256 over a pipe and the existing reader; Operability added cleanup on every exit, an early stop, the commitment's
  early checks and identities, the free-space check with the validator's runs, tmpfs, the validator's bounded merge and
  its own short-read defect, progress and errors with their phase.
- 2026-11-21: Slice C6, first part (failure halfway), built (§ Measured).
- 2026-11-21: Slice C6, second part (compression, bounded merges, room), built (§ Measured).
- 2026-11-21: Slice C6, third part (the live run), built (§ Measured).
- 2026-11-21: Slice C6, fourth part (the observer; the room check counting a log beside its spills, which the second
  part missed, and the validator's runs), built (§ Measured).
- 2026-11-21: Slice C6, fifth part (one verified reader for a live run and a persisted replay; the third part's writer
  check reworked into it), built (§ Measured).
- 2026-11-21: M9 measured at 10⁷ on both paths and at 10⁸ persisted without validation (§ Measured); the dry run's
  missing validator directory fixed; decision 16 (the validator's memory) owed.
- 2026-11-21: Decision 16 (the validator's memory), under the gate: a blind solver found every growing term
  (streams, the window, ids, and those empty in M9: slots, barriers, buckets, deviations, duplicates, a line's length)
  and three verdict-level defects (a deviation naming `deviation` exempting itself; a line planned ahead evicting
  steps later lines reach; `kept` by discovery, not line); the main position — evict streams and slots with the
  window — was refuted by the discriminator with three logs it would pass; the synthesis read both as one structure, a
  hot part by the read cursor and a cold part in sorted runs; Verifiability added the executors the corpus could not
  give (a line moved forward, every rule's deviation, the verdict digest, room against the disk); Performance the
  plan-own bound, the early refusal, the dead-on-arrival step and M10's record.
- 2026-11-21: Slice C7 built (§ Measured); § 9's edge revised to the read cursor, `RULES_REVISION` 3.
- 2026-11-21: Slice C8 built (§ Measured): streams decided by sorted runs, `Limits::memory` in bytes.
- 2026-11-21: M10 measured after C8 at 10⁸ on both paths (§ Measured); slice C9 built: the window by digest, the
  hot parts' limit and the dry run's refusal before a line.
- 2026-11-21: M10 measured after C9 at 10⁸ on both paths (§ Measured): decision 16 closed but for S4, on its trigger.
  The owner dropped 10⁹ for this laptop — it would measure only its throughput; 10⁸ stays its scale measurement and
  the stand's scale is measured on the stand. The § Measured owed list cleared of what C6 and M9 delivered.
- 2026-11-21: ADR-0047 (the scheduler) proposes amendments to §§ 7, 9, 12 and 13 — one release rule with `feasible`,
  writers awaiting readers `Settled`, the scheduler's premises non-deviable, the window (not a machine) shared — to
  be made in its slice K0a with FORMAT 3.
- 2026-11-21: FORMAT 3 (ADR-0047 K0a, first part): `think` replaces `Release` (§ 2, § 7), `Observe.cause`, an operation's
  `safety`, `client`, `not_run` and `timeout` in place of `retry`, the profile's `route_timeout`, the header's channels
  and calls. The rules are unchanged until K0a's second part.
- 2026-11-21: ADR-0047 K0a, second part: § 9 revised — writers await readers `Settled`; `order`, `duplicate`, `edge` and
  `producer` join `key` and `deviation` as rules no deviation exempts; new rules `feasible`, `measure`, `cause`,
  `until`, `sent`; the header's deadlines and the channel and call tables (`RULES_REVISION` 4).
- 2026-11-21: ADR-0047 K1c: § 9's producer rule also refuses a reader whose producer, the slot's latest writer,
  captures no value of it — the plan could never pass (`RULES_REVISION` 5).
- 2026-11-22: ADR-0047 K1e-2: the validator also holds the plan's own bound — 24 B more a window entry and 8 B a step
  held past the window or not ended, which grows with the plan's chains times their timeouts, not with W — two hot
  parts by name; V6's plan runs timeouts on its window's scale, its bound equal across its plans. Decision 16's refusal
  counts W × rate against the validating process: the scheduler's refusal of a plan its machine cannot hold is
  ADR-0047's `fits`, not it. `feasible` also holds every step's think from the run's start (`RULES_REVISION` 6).
- 2026-11-22: ADR-0047 K1e-3b: the log's framing — a value's canonical line, its decoding, the next line, the running
  digest — is `frame`, public, for the scheduler's journal as well; the log's reader and writer stand on it, its bytes
  and `FORMAT` unchanged. `wire` is public, with a list of `u64`s.
- 2026-11-22: ADR-0047 K2a-2 amends decision 3: `Secret32` from the run's secret by HMAC-SHA256, not from the
  namespace, which a run's journal names; `render` takes the secret beside the namespace; `golden/render.txt`'s key
  material is the vectors' two secrets', and it gains correlation ids. The log's bytes and `FORMAT` are unchanged.
