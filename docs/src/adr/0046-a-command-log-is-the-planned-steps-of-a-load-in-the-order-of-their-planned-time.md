# ADR-0046: A command log is the planned steps of a load, in the order of their planned time

## Status

Proposed (2026-11-20); slices C1 to C3 built. Stage 2 of three toward the host's fitness functions (ADR-0045 § Status): the Command Log —
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

One streaming pass, in memory bounded by the window:
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
- **An edge** is to the latest earlier step of that id, planned strictly before the holder, within W of it or of a
  retained label. The distance is checked edge by edge, not left to the window's eviction, which an out-of-order
  line defeats.
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
gapless, the emulated producers' sealing configuration, the egress model, the replica counts limits are per. On the
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
  pairs ordered ABBA or from a snapshot.

### 13. Crates

- **Library: `crates/command-log`** — the record model, holes and their rendering, the canonical form, the digest,
  the validator and its pure dependency machine, the engine. One crate until stage 3's scheduler, which shares the
  dependency machine; the engine's spill and thread code stays generation-only. It carries faker's lint gate:
  `at` is computed there.
- **Host:** a tool package of its own, the owner's to place (STYLE.md's `test` layer is dev-dependencies only):
  scenarios, the operations table generated from its contracts by an `openapi-dto` generator, end-to-end fixtures
  written in Rust against its typed DTOs, stand profiles, and, for stage 3, decorators (auth and step-up, DPoP,
  sealing, envelope stamping).

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
| R1 | C5 | a token bucket per limit key over planned times: a burst at t = 0 accepted, burst + 1 refused, an average under the rate with a peak over the burst refused; an unevaluable key refused | the average instead of the bucket |
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

Owed:
- **M7:** bytes a record by kind, generation time and peak RSS at 10⁷ and 10⁹ records on the laptop, peak disk with
  spills; the measurement that would bring compression or chunks back.
- SHA-256 throughput on the laptop (no SHA-NI) for 3 GB.

## Owed

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
