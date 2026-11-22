# ADR-0047: A planned step is bound when it is read and released no earlier than its plan

## Status

Proposed (2026-11-21); slices K0a, K1 (K1a to K1e) and K2a-1 to K2a-4 built (§ Measured), K2a-5 next. Stage 3 of three toward the host's
fitness functions (ADR-0045 § Status): the scheduler that runs a command log (ADR-0046) against a stand — what is pure
and what is the shell, how steps are bound, released, sent, judged and recorded, what voids a run.

To be built in slices, each a commit of its own:
- **K0a** (`crates/command-log`, FORMAT 3, `RULES_REVISION` 4): the command log's changes this decision needs
  (decision 13).
- **K1** (`crates/scheduler`, pure), in five parts: **K1a** the machine and `sim`; **K1b** the reference scheduler by
  definition, the differential and the corpus's coverage counters; **K1c** the stateful server model; **K1d** the TLA+
  model and trace validation of `sim`'s journals; **K1e** the verdict fold, the journal's schema and golden journals,
  the plan's own bound with peaks and a peak rate in the commitment, and the heap executors — in three parts: **K1e-1**
  the heap, **K1e-2** the plan's bound, peaks and peak rate, **K1e-3** the verdict fold, schema and goldens (itself in
  four: **K1e-3a** a defect of the machine's barriers the round found, **K1e-3b** the journal's bytes, **K1e-3c** the
  fold and its reader — the cause amendment with them, since the fold carries every class rule at once — **K1e-3d**
  the stand with failures injected). (The plan's bound, the
  commitment's peaks and the reference's bound lines were first listed in K0a; they moved where their consumer is.
  K1-4's `PendingByRate` moved to K1e with the bound it mutates.)
- **K2a** (pure, decision 15): a REST attempt's frame and its judgement, in five parts: **K2a-1** an answer's JSON
  (`command-log::json`); **K2a-2** the fills and the request, `Secret32` from a run secret; **K2a-3** reading an
  answer, and `scheduler::judge`; **K2a-4** FORMAT 4 and the validator's rules, in two: **K2a-4a** the model,
  **K2a-4b** the rules over steps; **K2a-5** the machine, the journal
  and the fold: polls re-polled, resends spaced and licensed.
- **K2b** (`crates/load-run`, the shell): REST, the clock, the reader, the journal writer, the gates.
- **The first host run:** REST only, on the compose stand, `sources = 1` — a correctness run.
- **K3** facts and the bus; **K4** shaping and the emulated client's retries; **K5** the report.

Amends ADR-0046 §§ 7, 9, 12, 13 (in K0a) and ADR-0045 § Context (the `Session` claim).

Decided under the library's gate: a blind solver, the main position and a discriminator, then two roles on the
synthesis (Distributed Systems; Verifiability). § Alternatives Considered keeps what each refuted.

## Context

ADR-0046 § 12 lists what the scheduler inherits: frames built once and resent byte for byte, retries by the
operation's class, release by § 7, lateness split by cause, a token bucket per limit key, fact subscriptions armed
from the start, failures censored at their timeout, correlation ids, stand and namespace gates, A/B order, shaping
by § 14, voiding conditions. § 7 states the release rule, W and L; § 16 and § Owed left open whether the scheduler
binds a dependency when it reads a line or when it releases it.

The host's runs this serves are of 10⁶ to 1.5·10⁷ steps (ADR-0046 § Context: QAS-SCALE-0001 about 10⁶, the
launch-shape drills 1–1.5·10⁷, QAS-SCALE-0002 at most 8·10⁶); telemetry at QAS-PERF-0007's rate is its own question
(ADR-0046 § Owed). Over hours that is about 10³ steps a second on average; bursts are what bind.

What review found in the existing code, each a premise of a decision below:
- `release` and `think` are read by nothing — not the validator, not the reference, no test (grep). § 7 releases a
  `Ready` step at `ready + think` even before its `at`: the same log and latencies then give a schedule decided by
  the read-ahead L, and an arrival before `at` is outside decision 14's curve, which models delays only.
- The writers rule awaits a slot's readers `Sent` (validate.rs:1028-1031): that orders the client's store, not what
  the server sees — a conditional reader on one connection races the next writer on another (the host's
  `requestReconciliationRelease`, `If-Match` on the evidence ETag, 412 `evidence_stale`). A per-(subject, slot) map
  also cannot run § 2's deliberate stale `If-Match`.
- `Observe.timeout` runs "from the cause's intended start" (record.rs:384-386), but no field names the cause.
- `Measure.from` is never checked (`edges` reads only `after`).
- `Operation.retry` mixes what the harness may resend with what the real client retries; operations carry no
  timeout; host calls are not in the operations table (validate.rs:506-515).
- `read_ahead` has no rule.
- Every rule but `key` and `deviation` is deviable, the scheduler's premises (`order`, `duplicate`, `edge`,
  `producer`) included: a committed log can make a missing dependency reachable.
- The validator has no dependency machine to share (ADR-0046 § 13 says it does): it has a window and static rules.
  What is shared is the window, the binding rule, and `rate`.
- `HttpAccess::request` times its caller's whole future with `std::time::Instant`, pool wait included, and
  `RequestEnded` carries no step, attempt, status or the instant the bytes left (session/src/rest): `Session` cannot
  serve the handlers (ADR-0045 § Context says it does).
- hyper-util's legacy client resends a request whose reused connection closed before it started
  (`retry_canceled_requests: true` by default): an attempt the journal would not see.
- `bus::Producer::publish` runs every stage on every call (bus/src/lib.rs:277-278): a resend through it reseals,
  fresh ciphertext under one `message_id`, which the inbox reads as `Differs`.
- After a refused COMMIT the library's session says "nothing of the scope is durable" (session/src/pg/session.rs:
  208-211) and classes a closed connection as transient; a COMMIT whose answer was lost on a closed connection may
  have committed. A 503 the host answers on such a failure can follow an effect that took place.

## Decision

### 1. A pure machine and a shell, two crates; the boundary executes purity

- **`crates/scheduler`** — no IO and no clock, under `command-log`'s lint gate (`forbid` cannot be allowed past, so a
  shell cannot share the crate) and a dependency allowlist (the gate names std paths only; a third-party clock would
  pass it): `Machine::step(Input) -> Vec<Output>`; `judge(record, op, response) -> Judgement` (the `expect` match, the
  captures, the attempt's class); frame filling from rendered, captured and time holes; the journal's types; `sim`, a
  deterministic stand; the verdict fold.
- **`crates/load-run`** — the shell: tokio; a `Clock` port; the reader over `live::replay` / `live::live`; REST on
  `hyper::client::conn` with its own pool per source index (no hidden retries: every attempt is the machine's); the
  journal writer; later the bus and the fact reader. Ports the host implements: `Decorator`, `Egress`, `HostCalls`,
  `Facts`, `Stand`.
- **`command-log`** exposes its window and `rate` as public modules.
- The host's tool package (ADR-0046 § 13) holds the stand binding, decorators (tokens per principal, step-up, DPoP,
  envelope stamping, sealing per emulated producer), host calls, the fact decoder and the stand's hooks.

### 2. A dependency is bound when its line is read

When a line is read, each dependency resolves through the validator's own window, evicted by the read cursor — the
running maximum of `at` over the lines read — before the line binds, exactly as the validator does. The scheduler's
bindings are then the validator's, a function of the log's prefix: independent of the read-ahead, of latencies, of
the order completions arrive.

Binding at release would keep every completion since the oldest unreleased line — the rate times the lag — where
binding at read keeps the blocked lines and W; binding at generation is impossible, units never knowing line numbers.

### 3. One release rule; nothing before its plan

Every step is released at `intended = max(at, ready + think)` — `At` is `think = 0` — and not before it is read:
`released = max(intended, read)`. A rule `feasible`, `at ≥ max(dep.at) + think` over step dependencies and a barrier's
members' running maximum `at`, makes `intended = at` under zero latencies, so that a release is never earlier than
planned and every lateness is the run's own (decision 14's premise).

### 4. Inputs are ordered by their stamps

Every input carries the instant its source saw it — a connection's task the bytes leaving and the answer arriving,
the fact reader a fact, the reader a line. The machine processes inputs in (stamp, line, kind) order up to a
watermark, the minimum over sources of each one's last stamp. A call's task decides its own deadline from its own
stamps; the machine's timer at a deadline plus a grace is only a backstop, and reaching it is a harness fault. So a
response stamped before its deadline is never censored for arriving after the tick, and two inputs with one stamp give
one journal whatever order tokio delivered them.

### 5. Captured values live in their producer's cell

A hole's producer is bound at read (its `from`), its value taken at release; a value missing at release skips the
reader as a missing capture, counted at the producer as a failure. Equal to a per-slot map on every line but a
declared stale read, it runs the stale `If-Match` and a writer that captures nothing. Writers await readers `Settled`.

### 6. Strengths, final states and provenance

Final states: **Expected**, **Unexpected** (an answer the contract defines that `expect` does not list), **Unknown**
(no certain outcome by the deadline — no answer, a reset after the request was written, a gateway's local reply, a
5xx not certified as not run), **Skipped** (a dependency's `Expected` not met, or a missing capture).
- `Sent` is met at the first attempt's sent instant, or at finality if the step was never sent; `Settled` at the
  answer, or at finality; `Expected` only by Expected.
- A dependent of a dependency awaited `Sent` or `Settled` that ended Unknown or Skipped is **conditional**,
  transitively. The verdict counts a failure at its root only, and refuses "pass" when an assertion is conditional.
- A step that awaits a dependency only `Sent` carries no measure and no `expect` beyond any answer, unless it is an
  observation whose `until` checks the dependency's effect: `Sent` is the harness's order, never the server's.

### 7. Deadlines, and the plan's own bound

Every action has a deadline: an operation's `timeout` (required, below the stand's route timeout by more than J, the
route timeout in the profile, so that one slow call cannot end either censored or as the gateway's 504 by
microseconds); a channel's (the bus's `message.timeout.ms` at or below it); an observation's, from its `cause`'s
intended start — an observation may be its own cause, a watch begun on its own, its deadline from its own start. A
step released at or past its deadline — an observation whose cause's window closed while a late reader or slow
dependencies held it — ends unknown at its release, never sent; the journal's read instant tells whose the delay was. The plan's own bound — every action ending at its deadline, the worst release along each chain — is
computed by the validator, and a plan past what the machine holds is refused before T0, as decision 16's dry run is.
Lines blocked behind censored roots are the system's: the run ends failed and names them, never void.

The bound, as built (K1e-2, decided under the gate: a blind solver, the main position, a discriminator, then three
roles — Verifiability, Performance, Software Design): the worst schedule the deadlines allow, under A1 every attempt
ends by its deadline (past it only the backstop, a harness fault), A2 every line is read within `[at − L, at]`, `L` the
header's `read_ahead` (its first reader), A3 a wake is taken at its instant. Per line in log order: `R̂` its latest
release, `max(at, ready̅ + think)`, `ready̅` the latest `Ê` of what it awaits — a strength is met, or its dependent
skipped, by the dependency's end; `Ê` its latest end, `R̂` plus its timeout (an observation's from its cause's `R̂`),
or `R̂` if that is past it; `Ĥ` the latest instant a machine may hold it — `Ê`, the latest `Ê` of its key's members up
to it, the `R̂` of a step it pins. Every hold is monotone in every end (checked case by case: skips, `Sent` met by the
end, observations released past their deadline, causes skipped while waiting, failed keys passing sooner), so every
end at its deadline maximises each. The validator folds it into the committed `Peaks` at each read, `τ` the cursor less
`L`: `held` the steps the window keeps and those it let go with `Ĥ ≥ τ`; `pending` those with `Ê ≥ τ`; `horizon` the
latest `Ê`; `barriers` the keys. A plan past what a machine holds is refused before T0 by a pure `scheduler::fits`
against one byte budget, `held`, `pending` and `barriers` each at a per-part cost the heap executor pins — not by
decision 16's refusal, which counts W × rate against the validating process and cannot see chains (ADR-0046 decision
16). At run time the shell compares the machine with its budget, not with the prediction: A2's late side is lateness
decision 9 judges, and a real timer breaks A3 by its slack. The rules the bound and the machine share — a step's key,
its intended start, its action's timeout and safety, the steps it pins — are one module, `command-log::plan`.

### 8. Barriers by prefix

A barrier's members are its key's lines read before the holder; the machine keeps prefixes of members that met
`Sent` and `Settled` and the first failed ordinal — never a counter, which fires early when a member read later
finishes first.

### 9. Lateness is a chain; a measure is an interval

`at ≤ intended ≤ read-bound release ≤ granted ≤ dispatched ≤ sent`, each gap one cause (the plan's dependencies, the
reader, the limiter or shaping, the pool and connect, the write); on the receive side the runtime's lag (a sampler
per worker) and, for facts, the consumer's lag. Own lateness beyond J — the profile's jitter — makes each measure an
interval [measured − own lateness beyond J, measured]: failed if its lower end exceeds the budget, passed if its upper
end meets it, void only if the budget lies inside. A poll's observation is bracketed by the last failing poll's send
and the passing poll's answer. The critical dependency is journaled, so that attribution follows it afterwards.

### 10. Frames, attempts and correlation

- A REST frame's body and planned headers are built once; decorators (DPoP, tokens) run on every attempt; the
  correlation id is rendered from (step, attempt) in a decorated header, so that a server record joins to one attempt.
- A bus frame is sealed once, while it is built, and published through a producer with no stages, byte for byte on
  every attempt; a resent frame's first hop is an interval.

### 11. Retries: what the harness may resend is certified by the contract

`Operation.retry` splits into `safety` — what the harness may resend: `Never`, `NotTaken`, `Idempotent` — and
`client`, what the emulated client retries (load, measured). **Not taken** is certified by the problem codes the
contract says mean "not run" (the host's `request.rate_limited` and `access.projection_behind`;
`service.session_failed` only where it means that no session opened — decision 15, § Owed), never by a status: a 503 with `Retry-After` may follow a COMMIT that took effect. Everything else not answered is Unknown; an
Unknown non-idempotent step is never resent. A poll is never retried, never two in flight. A connection is kept past
a censored deadline until the gateway's own timeout, so that the late answer is journaled and the port is not left
in TIME_WAIT; abandoned only past the route timeout and a grace, a stand defect, counted. A plan needing more ports
than its sources' ranges is refused before T0.

### 12. Facts

Read by an assigned reader — explicit partitions of every observed channel at end offsets read before T0, no group,
no commits; a gate checks every partition is positioned. Deduplicated by `message_id` before matching. A fact of the
run's Tenants and observed types is held until the longest observation timeout and matched when an observation arms,
so that matching does not depend on how far the reader is ahead; a fact the harness cannot decode is a harness fault,
journaled and skipped. An observation's `until` holds an `Equals` on a value of its cause itself (rendered from it,
or captured from its outcome) or a stream position bounded by it: an id that recurs across a Device's lives would
match the wrong cause.

### 13. The command log, FORMAT 3

`feasible`; `Release` folded into `think`; writers await readers `Settled`; `order`, `duplicate`, `edge`, `producer`
non-deviable; `Measure.from` checked through the id runs (a measure may span more than W); `Operation.timeout`
required and capped; the route timeout in the profile; a channel table with deadlines; host calls in the operations
table; the retry split and the not-run problem codes; `Observe.cause`, an edge awaited at least `Sent`; the
discriminating-`until` rule; the `Sent`-only rule of decision 6; the plan's own bound in the validator and its peaks
in the commitment (the peak rate deferred to M11, which names its width and its consumer); the window as a public
module; `reference.py` emitting each edge's bound line and
deciding the new rules.

### 14. The journal and the verdict

*As decided for K1e-3* (2026-11-22, under the gate: a blind solver and a discriminator on Fable, then three roles —
Verifiability, Software Design, Evolution — on Opus; the paragraphs after this one are the decision as first written):
- **The journal's bytes.** The framing — a canonical line in declared field order, re-encode and compare, a streaming
  digest, a trailer of counts, truncation detected, its own error naming its format — is `command-log`'s, generic and
  public; the vocabulary is the machine's, `scheduler::journal`, `serde` its one new dependency, and the scheduler's
  lint gate refuses `std::fs`, `std::net` and `command-log`'s file functions. Version 1: a `header` (the journal's
  version, the machine's revision, the writer's source revision, the log's commitment digest and `validator()`, the
  run's read-ahead, the profile's J); `read` (the line's barrier key, and each dependency's strength with the line it
  bound or the barrier key with its members' count — the journal is self-contained, the log needed only for its
  identity); `release` (intended, released, the deadline, at the decision); `dispatch`; `sent`; `answer` (the
  judgement's kind and the captured slots' names, never values; an answer the machine ignored marked so, and why);
  `end` (at the instant decided, the entry); `fault` (a closed class — the backstop in version 1, each later kind
  with its producer); a `trailer`. A pure `journal::events` writes the same bytes in `sim` and the shell. A golden
  holding every variant, built by exhaustive matches, its digest pinned (`JOURNAL`), the machine's revision pinned
  beside the digest of the differential's journals, every differential journal decoded back and compared with the
  run, the goldens also read in the host's serde build. Journals before K5 are disposable; from the first host run on
  a reader accepts the versions it lists (§ Owed).
- **The fold.** From the journal alone, in two passes — ends and per-attempt facts in any order, then the reads, which
  the journal holds in line order — one byte a line beside a few flags a barrier key: no sort. Classes, a partition by
  precedence: Consequent (skipped) before Inconclusive (conditional) before Undecided before Root before Passed. A step
  is conditional when it awaits sent or settled a step that ended unknown or skipped, or awaits a conditional one,
  and — amending decision 6 — when it is an observation whose cause did not end expected, whatever the strength: an
  observation asserts its cause's effect. Undecided: an unknown whose deadline the harness ate past J — a call whose
  last attempt left more than J after its dispatch, or never; a step released at or past its deadline though
  intended before it, read more than J late; an observation whose cause left more than J after its intended start —
  decision 9's interval applied to the deadline, the timeout the plan's own budget. The verdict in two levels: void on
  broken evidence (unreadable, incoherent, another log, a fault) above fail (a root) above void undecided (an
  undecided step); pass defined positively — every line expected, the evidence sound, nothing undecided. Under zero
  tolerance pass means every line expected, so provenance changes no run's verdict but its roots and classes, which
  the executors judge. The fold's revision comes with its verdict, pinned beside the digest of the classes it gives
  the goldens and the differential.
- **Deferred:** measures and decision 9's intervals over them, with K1-9's interval clause, to K5 — no corpus log
  carries a measure and no plan a budget, so no executor would reach them (trigger: the first host plan with a
  measure and a QAS number); gates, and the K2–K4 fault kinds, with their producers; late answers' meaning (K2; version
  1 marks them ignored); a list of readable journal versions (the first host run).

An event stream: canonical JSON lines, zstd, a trailer of counts and SHA-256, so that a killed run reads as
truncated; a line per step is the report's fold of it. The header anchors the run: the log's identity, the
machine's revision, the namespace, the stand and its profile, the gates' evidence, T0's monotonic and wall instants,
and clock offsets to each stand clock bracketed before T0 and after the end — a gate refuses T0 on a skew past a
fraction of the narrowest window the log declares. Every poll's sent and answered instants; a digest of each
attempt's sent bytes. The verdict fold (pass, fail, void; roots; conditionals) is in K1, beside a normative schema,
golden journals and an independent reader; time weighting, critical paths and per-hop joins are K5's.

A run is void on: a refused gate (profile, a namespace the stand has seen, egress collapse, clock skew, positioned
partitions, the gateway's buckets refilled since their last use); a harness fault (`EADDRNOTAVAIL`, an undecodable
fact, a backstop deadline); an unplanned 429; a divergence from the commitment; a truncated journal; a measure whose
budget lies inside its interval.

### 15. A REST attempt: its frame and its judgement

*Decided for K2a* (2026-11-22, under the gate: a blind solver and a discriminator on Fable, then three roles —
Distributed Systems, Verifiability, Security — on Opus; it cuts decision 1's pure half, amends decisions 10, 11 and 14
and ADR-0046 decision 3; § Alternatives Considered keeps what each refuted):
- **Placement.** What a record means against a request and an answer is `command-log`'s, beside `plan` — one copy
  for the validator, which checks its static half, and the scheduler: `json` (an answer's JSON), `fills` (every hole
  resolved), `request` (the frame, and `check`), `answer` (the code, predicates, captures, `Retry-After`).
  `scheduler::judge` is a decision list over what `answer` read, with no JSON: the scheduler's dependency allowlist
  is unchanged. The shell keeps scheme and authority (a `Stand` port: the host serves each Tenant on its own
  authority), decorators, hyper and the stamps.
- **An answer's JSON is read by the crate's own reader**, not by serde_json: strict RFC 8259 over UTF-8 — no byte
  order mark, no `NaN`; a document unreadable if it escapes half of a surrogate pair, names a member twice, nests
  deeper than 128 or writes an exponent of more than 18 digits — its numbers kept as their text. serde_json's number
  categories follow its build's features, and the host's workspace turns on `arbitrary_precision` (host
  `crates/http/Cargo.toml`, unified into every crate it builds): `1e400` reads in one build and is refused in the
  other, `-0` is an integer or a float, a 23-digit id keeps its text or becomes a float. A judgement left to it would
  depend on who built the shell. Two numbers are equal when their decimal values are; values member by member,
  objects in any order; pointers are RFC 6901's.
- **The frame.** The path: `op.path`'s literals verbatim — the validator holds them to RFC 3986 `pchar` and `/`, no
  `%` — each `{name}` filled in order by its value with every byte outside unreserved percent-encoded. A custom method
  (`/devices/{device_id}:retire`, 101 of the host's paths) then reaches its route, which the gateway matches as
  `[^/:]+:retire` on the undecoded path; encoded, `%3A` sends it to the 405 of `/devices/{device_id}`. The query:
  `name=value` in record order, encoded alike. Planned headers: a token name, a value of visible ASCII with inner
  blanks only; reserved — compared without case, for planned names, `op.decorated` and the correlation header alike
  — Host, Content-Length, Transfer-Encoding, Connection, Content-Type, every decorated name and the correlation
  header; `content-type` iff a body. A frame lives as long as its attempts are resends of one request: a call's is
  built at its first dispatch and resent byte for byte; each poll builds its own. `TimeBase::Sent` is the frame's
  first dispatch stamp on the run's wall clock, never the write instant.
- **Per attempt**, in the shell: the correlation id — `root(namespace) / "ascetic-ddd-command-log/correlation" / step
  / attempt`, 32 hex digits, a key domain no planned role reaches — under the profile's header; decorators, which see
  the final bytes and the attempt's deadline and return headers only of `op.decorated`'s names (any other is a
  harness fault). The frame's digest covers the frame with every secret replaced by its hole, and the decorated
  names, never their values.
- **Secrets.** `Secret32` is drawn from a run secret — 32 bytes from the OS, held in memory only — not from the
  namespace: faker's derivation is public, so every rendering is a function of the namespace, and a journal naming
  the namespace named every secret. The namespace is then public, and the journal carries it. A secret — a
  `Secret32` hole, or a captured hole of a secret capture — stands only in a body or a header, never in the target,
  which every proxy logs. Every type holding the run secret, a fill, a frame or a captured value redacts its `Debug`.
- **Captures**, from an expected answer only: `Header(n)` exactly one field line; `Json(p)` a string or a number;
  `Location` its last non-empty path segment, query and fragment stripped, percent-decoded strictly. A value is taken
  only if every use can carry it — a number, or a string of visible ASCII with inner blanks, neither empty, `.` nor
  `..`, at most 8 KiB; otherwise it is absent, named beside the judgement, which stays Expected, and the machine skips
  its readers for a missing capture, the producer the root (decision 5): a frame never fails on a server's value. A
  captured hole in a raw span is refused statically.
- **The judgement**, first match: (1) a reset or a cut answer after a byte of the request left, or no answer by the
  deadline — Unknown; (2) the code: the string at the profile's pointer, read only from a status ≥ 400 whose media
  type is `application/problem+json` and whose body, within the profile's cap, is an object; (3) an `expect`
  response whose status equals, whose problem equals the code strictly — `None` is "no readable code", so the
  gateway's 404 `route.not_found` never passes for a service's 404 — and whose body predicates hold — Expected; (4) a
  poll: a 2xx where every `until` holds — Expected; (5) the code in `not_run` — NotTaken, its `Retry-After` beside it;
  (6) a status ≥ 500 or below 200 — Unknown, the service's or the gateway's own; (7) an empty `expect`, a sent-only
  step's — Expected; (8) else Unexpected, for a poll "not yet". A request no byte of which left is no attempt's end:
  the shell keeps it in its pool-and-connect gap until a byte leaves or the deadline passes (hyper hands back a
  request it never started). The judge sees no clock, attempt or safety; it returns the status and a bounded code
  beside the judgement, and the journal carries them.
- **Resends**, the machine's: a poll's Unknown takes the next point of its grid, a poll censored only at its
  observation's deadline — the gateway's own timeout frees a hung one — so that never two polls of one observation
  are out; a harness resend leaves at `now + max(Retry-After, the profile's floor)` and only if that is before the
  deadline by more than J, else the step ends as it stands (NotTaken as Unexpected, Unknown as Unknown).
  `Idempotent` means a repeat answers the first attempt's outcome, as the contract certifies; a step carrying a
  captured value of a slot its own operation writes is refused `Idempotent` (its first attempt invalidates its own
  `If-Match`). The journal's `read` carries the operation's safety and its header the floor and the namespace, so
  that the fold refuses a resend its predecessor did not license.
- **The model**, FORMAT 4: the profile's problem pointer, correlation header, resend floor, answer cap, and the
  stand's downstream idle — the shell retires a pooled connection before it, and a first write refused with no byte
  accepted reconnects within the attempt (the race the host measured on its own hop as `503 UC`). Each rule above
  with a static half is the validator's.

## Consequences

- A load run is judged against the plan the validator checked, with every lateness the run's and every failure
  counted at its root; a run fails on the system, never on the harness, or is void.
- The scheduler's core can be proved and differentially tested without a stand; the shell is thin.
- FORMAT 3 refuses plans FORMAT 2 accepted (a `Ready` step before its plan, writers awaiting readers `Sent`,
  deviated premises, an observation with no cause): there is no deployed client, and the corpus and goldens are
  regenerated.
- The emulated principal needs the stand's permissions (Vault datakeys, broker principals per channel): the host's
  work.

## Rule of Three

1. **The plan's bound refuses host plans** — a barrier holder with `think > 0` cannot know its members' latest `at`
   (a unit never looks it up). Caught by K0a's twin and by validating the first host plan; fallback: barriers take
   `think = 0`.
2. **Provenance hides a real failure** — an assertion censored as conditional, a drill passing vacuously. Caught by
   the verdict refusing "pass" on a conditional assertion; mutation: a conditional step counted as a pass.
3. **The independent oracles share the decision's sentences** — the machine, `schedule.py` and the TLA+ model written
   from one text pass a rule wrong in meaning. Caught by the stateful server model (decision-independent: ETags per
   version, server ids per attempt, an inbox parking a message ahead of its predecessor, an order of processing the
   script chooses) and its property: every clean log ends all Expected under every latency script without failures.
4. **The laptop measures itself** — own lateness on critical paths voids or inflates. Caught by M12 with the stand
   running; measures go to the 12-core box.
5. **Ports run out under open loop** — caught by the port bound refused before T0 and a stub that answers only at
   the gateway's timeout, at rate, for twice TIME_WAIT, from a non-loopback address.

## Alternatives Considered

- **Binding at release** (any eviction): keeps rate × lag; under a deviated duplicate it can bind a line planned after
  the holder and deadlock. Excluded by decision 2; executed by the invariance across L and TLA+'s `BindAtRelease`.
- **§ 7's rule as written** (a `Ready` step at `ready + think`): a schedule decided by L; arrivals before plan.
- **Captured values per (subject, slot)**: cannot run the stale read; O(subjects × slots).
- **A plain counter for barriers**: fires early.
- **The machine deciding deadlines by arrival**: censors in-time responses; journals depend on scheduling.
- **Not taken by status** (429, 503 with `Retry-After`): doubles a committed effect.
- **One crate**: loses the executor of purity.
- **`Session` for the handlers**: times pool waits with `std::time::Instant`, knows no step or attempt.
- **A voiding cap chosen by hand**: voids the stalls QAS-SCALE-0001's open-loop probe exists to catch.
- **The `ahead` rule** (a bus-probe observation within L of its cause): the chunked reader's read-ahead swings to 0;
  held facts make matching independent of it.
- **serde_json for answers** (K2a): its number categories follow its build's features; a judgement would depend on
  who built the shell.
- **Path literals encoded with the values** (K2a's synthesis, refuted by all three roles): every custom method
  misrouted to the 405 of its resource.
- **The namespace as the secrets' root** (K2a, Security): a journal naming it names every secret.
- **A request never written judged NotTaken** (the main position): a `Never` step ends Unexpected, a root, for a
  keep-alive race; **a fifth judgement for it**: no machine arm needs one.
- **`problem: None` as a wildcard** (the main position): the gateway's 404 `route.not_found` passes for a service's.
- **A poll's own deadline** (the discriminator): a censored poll's connection is kept, so several polls of one
  observation are out at once; **a poll's Unknown as "not yet" in the judge** (the main position): decision 9's
  bracket then starts from a poll that carried no information.
- **Resends at once** (the machine as built): thousands within a timeout against a fast 500 or a `Retry-After`.
- **One SHA-256 a pass and a hashing thread**, **a partitioned machine for telemetry**, **ABBA and snapshots**,
  **TLA+ trace validation of the shell**: later, each on its own trigger (§ Owed).

## Executors

| Id | Slice | Asserts | Mutation it turns red |
|---|---|---|---|
| K0-1 | K0a | a twin per new rule: `feasible` at exactly `dep.at + think` and a barrier's running maximum; writers awaiting a reader `Sent`; a deviated premise; `Measure.from` beyond W passing through the id runs; an observation with no cause; an `until` discriminating by a recurring id refused and by the cause's own value clean; a `Sent`-only step with a measure; a timeout at the route timeout; under a zero-latency simulation `intended = at` for every line | `>` for `≥` in `feasible`; `think` or the barrier's maximum ignored; writers back to `Sent`; a premise exempted; the measure checked through the window |
| K0-2 | K0a | V2/V4: the corpus gains `think > 0`, causes, timeouts, deviations of the premises, a reader weakened to `Sent`; the reference decides them by definition | any rule differing from the reference |
| K1-1 | K1 | bindings and releases invariant across L ∈ {0, a chunk well below the log, ∞} on the corpus × latency scripts — the executor of decision 2; bindings equal the reference's bound lines | binding at release; the window evicted by the clock |
| K1-2 | K1 | `vectors/schedule.py`, a scheduler by definition (no window, cells or prefixes; captures through a versioned per-slot map; barriers as the set of earlier members; met instants from the event history), compared per line; the comparison may differ only on declared stale reads | a per-key barrier counter; the value from the slot's latest; release without `max`; the deadline from the step's own start |
| K1-3 | K1 | the stateful server model: every clean log ends all Expected under every failure-free latency script, the order of processing chosen by the script | writers awaiting readers `Sent` (a 412 under reordering); a wrong cell spliced into a request (the attempt's sent-bytes digest) |
| K1-4 | K1 | `verify/tla/Scheduler.tla`: `ReleaseSafe`, every read line ends, `NoUseAfterFree`, `BarrierSafe`, `BoundAsChecked`, `ConditionalExact`, no response stamped by its deadline censored; mutant configs `BindAtRelease`, `FreeOnEvict`, `CounterBarrier`, `ReadyWithoutAt`, `NoProvenance`, `ProvenanceOneLevel`, `DeadlineByArrival`, `NoSkipPropagation` (and `PendingByRate`, built in K1e-2c beside the plan's bound), each with a pinned twin and state count (`must_fail_with_twin`), on an instance of at least four lines with W below L or a latency above W | each config's own |
| K1-5 | K1 | sim journals checked against `Scheduler.tla` (the `trace2tla.py` pattern) | the machine diverging from the model |
| K1-6 | K1 | corpus coverage counted and failing on zero: lines where the two bindings differ, cells taken after their producer's eviction, barrier members finishing before earlier ones, conditional dependents, responses after their deadline, same-instant inputs, `think > 0` | — |
| K1-7 | K1 | the journal invariant under any permutation of same-instant inputs | ties broken by arrival |
| K1-8 | K1 | a counting allocator: the heap flat across eight times the subjects at one planned rate, with a share of producers Unknown or Unexpected and captures missing; a stall of s ∈ {T/2, T − 1 µs} over chains of depth ≥ 2 not void, its peak in flight as derived; a stalled stand on a `Settled` chain spaced below its deadline ending failed within the plan's bound | a cell freed on eviction; a refcount leak on a skip path; a bound chosen by rate alone |
| K1-9 | K1 | the verdict fold against golden journals and an independent Python reader: one twin per void, the interval verdicts (a stall that fails by more than own lateness fails; a straddling one is void), a skipped publication inconclusive, transitive provenance | a conditional step counted as a pass; one-level provenance |
| K1-10 | K1 | the dependency allowlist of `crates/scheduler` | a clock crate added |
| K2a-1 | K2a | the reader against `vectors/json.py` (Python's `json` under hooks, and `decimal`) on 6,000 drawn documents, a third of them mutated, every pointer into them, and 6,000 pairs of values; a twin per rule | a member named twice read; the depth off by one; numbers equal by text; `-0` signed; the exponent's digits; an index with a leading zero; a lone surrogate; objects compared in order; trailing bytes; a raw control character; `~0` and `~1` swapped; trailing zeros kept; a leading zero; the exponent's sign |
| K2a-2 | K2a | `vectors/request.py` writing `golden/frames.txt` — the sample, a `:verb` path, values with `:`, `/`, `%`, a space and non-ASCII — diffed in `make vectors`; a frame a function of its fills, `Sent` among them; a captured ETag into a body quoted, a JSON id into a path unquoted; `Secret32` changing with the run secret, not with the namespace; `{:?}` of every fill, frame and secret holding none | literals encoded; `+` for a space; lower-case hex; `Sent` from the run's start; a captured value's form ignored; `Secret32` from the namespace; a derived `Debug` |
| K2a-3 | K2a | `vectors/judge.py` over a sample of raw answers — statuses at each class's edges, media types with parameters and in other cases, a code not a string, two `ETag` lines, a header not UTF-8, a `Location` with a query, a fragment, a trailing slash, `%FF` and `%zz`, `Retry-After` in seconds, as a date, negative and huge; an `expect` listing a `not_run` code, one listing 503, `problem: None` against a coded refusal, a 2xx problem document with a `not_run` code; `answer` total over drawn statuses, headers and bodies | rules 3 and 5, or 3 and 6, swapped; a wildcard `None`; the code read from any body; `>` for `≥`; the first of two header lines taken |
| K2a-4 | K2a | a validator twin and a corpus mutant per new rule; the corpus with expectations that fit its actions, a resend floor, and every safety with `not_run` codes | each rule's condition weakened |
| K2a-5 | K2a | the machine and `schedule.py`: a poll's Unknown on its grid, one poll out; resends at `max(after, floor)`, none within J of the deadline, scripts drawing `retry_after`, coverage counted (`resend_by_retry_after`, `resend_by_floor`, `resend_past_deadline`); the fold refusing an unlicensed resend; TLA+ `ResendPastDeadline` | a poll's Unknown ending it; a resend at `now`; the floor ignored; an Unknown `Never` step resent |
| K2-1 | K2 | the shell against an in-memory transport (hyper over `tokio::io::duplex`) under paused time reproduces `sim`'s journal | `std::time::Instant` in a handler |
| K2-2 | K2 | each cause delayed alone — the reader, the token, the pool, the connect, the write, the dependency — moves exactly one gap of the chain | a gap merged |
| K2-3 | K2 | on the real clock, multi-thread: chain order, no release before its stamped met instants, the stub's requests per correlation id equal the journal's attempts — a keep-alive connection closed as it is reused included; a request processed then reset before the answer is Unknown; a response stamped 1 µs before its deadline delivered after the tick is Expected | hyper's hidden retry; Unknown read as not taken; deadlines by arrival |
| K2-4 | K2 | a 503 with `Retry-After` and an uncertified code not resent; `service.session_failed` resent; an Envoy-shaped 504 Unknown with its `Settled` dependent conditional; a non-idempotent call timing out after writing not resent | not taken by status |
| K2-5 | K2 | decorated headers differ across attempts (a fresh DPoP `jti`), the rest byte-identical, a `Sent` time hole included, and each poll's frame its own; a retried step joins one server record; no secret in the decompressed journal, raw, base64, base64url or hex | a decorator run once; one correlation id a step |
| K2-6 | K2 | void twins: a refused gate each, an unplanned 429, a harness fault, a divergence, the rate monitor's refusal (its key equal to `rate`'s on every corpus call); the gateway's buckets waited for since their last use | the monitor off; the bucket wait skipped |
| K2-7 | K2 | a stub answering only at the gateway's timeout, at rate, for twice TIME_WAIT, from a non-loopback address: no `EADDRNOTAVAIL`, the run failed; a stub never answering: abandoned connections within the bound | connections dropped at the deadline |
| H-1 | first host run | the gateway refills continuously (spend the burst, send at 90 % of the rate for one `per_seconds`, no refusal); whether `X-Request-Id` survives the gateway in `direct` mode; the journal's attempts per route and status reconciled with Envoy's access log | — |
| K3-1 | K3 | two attempts of one bus step read back byte-identical; a lost ack resolved by a resend leaving one inbox row; a fact between the end-offset read and T0 observed; an undecodable fact on one partition not delaying another; a duplicate counted once; a reader stalled 2 s and a zero-latency fact matched | resealing on resend; a group consumer |

## Measured

**K0a, first part — FORMAT 3's model** (2026-11-21): `Record.think` in place of `Release` (released at
`max(at, ready + think)`); `Observe.cause`; `Profile.route_timeout`; `Operation.safety` (the former `Retry`, renamed
`Safety`), `client` (`ClientRetry`: attempts, statuses, backoff, `Retry-After`), `not_run` (the problem codes that
certify a refusal as not run) and `timeout`; `Header.channels` (`Channel`: name, timeout) and `Header.calls` (`Call`:
name, timeout, safety). `FORMAT_VERSION` 3; the normative schema, the golden log, `FORMAT` (`34d0604f…149d`) and the
two-frame compressed golden regenerated; `vectors/format.py` checks it. The rules are unchanged: the corpus's 1,642
verdicts and peaks are those of FORMAT 2, the reference agrees, and the corpus digest is re-pinned under
`RULES_REVISION` 3. The rules come in K0a's second part.

**K0a, second part — FORMAT 3's rules** (2026-11-21, `RULES_REVISION` 4): new rules `feasible` (a step dependency's
bound step plus think, and a barrier's latest member plus think, at or before the line's `at`), `measure` (a measure's
`from` names a step of an earlier line, decided through the id runs: an id event carries a query flag and sorts after
its line's ids, so that only an earlier line answers), `cause` (an observation awaits its cause or is its own),
`until` (an observation of the bus has an `Equals` on a value rendered of its cause step or captured from it; the
alternative of a stream position bounded by the cause waits for a host need), `sent` (a line that is not an
observation and awaits a dependency as sent carries no measure or expectation); writers await readers `Settled`;
`order`, `duplicate`, `edge`, `producer`, `key` and `deviation` are not deviable; the header's deadlines (an
operation's timeout in (0, route timeout − jitter), channels and calls named once with a timeout) and the tables
(a bus message's or a bus observation's channel, a host call's name). The reference decides each by definition; the
corpus's base logs keep their verdicts (writers await readers `Settled`, a group's watch is caused by its latest writer
within reach), and gains `order-deviated`, `think`, `weaken-reader`, `measure-dangling`, `cause-dropped` and
`sent-measure`: 1,966 logs agree, 51 only on the cursor's clock.
- **Executors:** twins for each new rule and for the premises' non-deviability; the out-of-order rate twin without
  its deviation; the corpus differential; the corpus digest pinned under revision 4. Mutations, each alone, red:
  `feasible` at its boundary; think ignored; a barrier's latest member ignored; writers back to `Sent`; `order`
  deviable, in the validator and in the reference; a measure answered by its own line; no `cause`, `until` or `sent`
  rule; a timeout at the route timeout; channels or host calls unchecked; the reference's writers on `Sent`; the
  reference's `feasible` gone. `order` deviable was green until the twin of the premises and the `order-deviated`
  mutant: no log the corpus made broke the order and deviated it.

**K1a — the machine** (2026-11-21): `command-log` gains `window` — the binding rule as a public module, `Window<V>`
evicted by the read cursor, dead on arrival not kept, and `digest` — and the validator binds through it (its corpus
digest unchanged); `validate::holes` is public. `crates/scheduler`: `machine::Machine`, stepped by stamped inputs (a
line read, an attempt's bytes left, an attempt ended with its judgement, a wake) and answering with dispatches, wakes,
how far to read, and each step's journal entry (its end, read, ready, intended, released, dispatched, sent, ended,
attempts, critical dependency, bound lines, fault); `order::Ordered`, inputs by (stamp, line, kind) up to a
watermark; `sim::run`, the machine in virtual time against a script. A step with no dependency is ready from the start
(`ready = 0`), so that a late reader's gap is the reader's and `intended` stays its plan — a defect of the first draft,
found by the late-reader twin before the commit. Bus observations are dispatched like calls until K3.
- **Executors** (`tests/machine.rs`, `tests/allowlist.rs`): under zero latency every clean corpus log runs on its plan
  (`intended = at`, readers of a writer that captured nothing skipped for it); bindings, critical dependencies,
  intended instants and ends equal across L ∈ {0, W/2, ∞} on the clean corpus under keyed latencies, every step sent at
  its release and never before it is read; the strengths; a barrier by prefix; a reader's value from its producer, a
  stale read on purpose taking its own producer's; retries by safety; the backstop a fault; inputs of one instant in
  one order; a line read late released when read; the crate's dependencies the command log alone. Mutations, each
  alone, red: release by `ready + think` alone, or by `at` alone; released before read (green until the late-reader
  twin); `Sent` met only at the end; a skip propagating through `Sent`; a barrier's prefix past an unmet member; the
  value from the slot's latest; a refusal resent though never; an unknown resent though not idempotent; no fault at the
  backstop; ties by arrival.

**K1b — the scheduler by definition** (2026-11-21): `crates/scheduler/vectors/schedule.py` decides every line again
from the definitions, in log order, quadratically, with no window, cells or prefixes — bindings by the latest earlier
occurrence within W behind the cursor, met instants from the dependencies' decided entries, a barrier as the set of
earlier members, the deadline, the attempts, the poll grid — and compares the machine's journal line by line (its end,
read, intended, released, dispatched, sent, ended, attempts, bound lines, and the critical dependency where one alone
attains the latest instant, a skipped line's excepted). `sim::run_reading` adds a reader that lags; its read instants
go to the definition as the environment's. Two findings, both fixed in the machine and the definition: a skip names
the dependency that failed first, not the first in `after`; a step released past its deadline ends unknown unsent
(decision 7). The corpus gains feasible think times on its base logs, their verdicts unchanged.
- **Executors** (`tests/reference.rs`): every clean corpus log × three scripts of mixed latencies and judgements ×
  read-aheads {0, W/2, ∞}, with a reader lagging on a quarter of the lines: 2,043 runs agree. The definition counts
  what the runs reach and fails on any zero: bindings that a binding at release would change (517), captures taken
  after their producer left the window (45), barrier members ending out of read order (30), conditional dependents
  (213), answers after their deadline (1,222), instants shared by dispatches (1,493), think times (780), reads after
  the intended instant (5,559), releases past the deadline (63). Twins: a skip naming the dependency that failed first;
  polls on their grid though answered at once; `sim` stops a machine that loops. Mutations, each alone, red: the
  machine skipping at the first failed, not the earliest (green until its twin); its polls off their grid (green until
  its twin); its observation's deadline from its own start; its think ignored; the definition's `Sent` met at the end;
  its release without the read (green until the reader lagged); its barrier by its last member.

**K1c — the model stand** (2026-11-21): `sim::run_stand`, a stand built from the host's contracts rather than this
decision's sentences — a resource's ETag a version each write of it raises, a request whose `If-Match` names another
version refused, a barrier's holder answered as expected only if every member read before it was processed first,
requests processed in the order their delays make them arrive. `sim`'s loop takes a handler: the script, or the stand.
Building it found a hole in the validator: a reader whose producer, the slot's latest writer, only writes the slot and
captures nothing has no value to send, and the plan can never pass — `producer` now refuses it (`RULES_REVISION` 5,
the reference alike), and the corpus's base logs read an ETag only after a writer that captured it.
- **Executors** (`tests/stand.rs`): every clean corpus log × eight delay scripts, requests overtaking one another,
  round trips inside the corpus's shortest deadline: every step ends expected. Mutations, each alone, red — the stand
  knowing nothing of them: the validator's writers back to awaiting readers `Sent` (a 412 under reordering, on a
  `weaken-reader` log); the machine's barrier prefix past an unmet member (the holder before its members); the
  validator's producer capture unchecked (a reader skipped for a missing value, on an `uncaptured-writer` log — green
  until that corpus mutant). A twin in `command-log`: a writer that captures nothing is no producer.

**K1d — the TLA+ model and trace validation** (2026-11-22): `verify/tla/Scheduler.tla`, the machine as a protocol —
reads no earlier than `at − L`, bindings through a window evicted by the read cursor, barriers by prefix with each
key's latest met instant, captured values in the producer's cell, release at `max(at, ready + think)` and not before
the read, deadlines from the release, resends by safety, stamped inputs each taken with everything it sets off before
the next — and the rule the heap is to follow, which K1a's machine does not implement (it keeps every cell): a cell is
freed once its step ended, the window let it go, no reader of its captured value is still to be dispatched, and every
prefix of its key passed it, freed as early as it may be. The model's first run broke `NoUseAfterFree` under that rule
as first written: the machine re-sweeps a key's expected prefix from the member that failed it on every later member's
end, reading that member's cell. Two requirements on K1e follow, both in the model: a prefix of members expected stops
for good at a failure and is not swept again; a holder takes its key's latest met instant (and the member that met it
last) from the barrier, not by walking its members' cells, as `Machine::read` and `Machine::barrier` do now.
- **Executors** (`verify/tla/check.sh scheduler`): two instances of four lines, `W = 1` below `L = 2` — the window and
  a captured value (556,782 distinct states), a barrier and provenance (105,278) — each holding `ReleaseSafe`,
  `BarrierSafe`, `NoUseAfterFree`, `BoundAsChecked`, `NoCensored`, `EveryReadLineEnds`, `ConditionalExact`, with the
  handlers' answers and timeouts cut to what each needs (with every answer, each passed two million states in five
  minutes and was stopped). Mutant configurations, each breaking its property beside a pinned twin: `BindAtRelease`,
  `EvictByClock` (a reader one instant late; twin 1,665,389), `FreeOnEvict`, `ReadyWithoutAt`, `CounterBarrier`,
  `NoSkipPropagation`, `NoProvenance`, `ProvenanceOneLevel` (a step conditional through a conditional one),
  `DeadlineByArrival` (an answer stamped at the deadline after the machine's own timer); and the two requirements on
  K1e, each the K1a machine's own behaviour as a mutant: `ResweepFailed` and `WalkMembers`, both reading a failed
  member's freed cell. The first run's finding had no executor until the barrier's instance let its failed member go;
  the instance was changed so that it does.
- **Trace validation** (K1-5; `TraceScheduler.tla`, `crates/scheduler/tests/trace.rs`): `sim` records every input the
  machine took with its stamp; four runs of one six-step log — resends, skips, a deadline, a late reader — fit the
  model, its journal equal to the machine's line by line. The machine is deterministic, so the test also holds each run
  to its committed trace: a change of behaviour fails `cargo test` until `make traces` records them again. Two forged
  runs do not fit (a step sent before the one it awaits settled; a holder released before its last member settled).
  Machine mutations, each alone, recorded again and checked: release not after the read, a barrier one member short,
  a not-taken answer not resent — each leaves a trace unfit.
- `sim` answers no attempt whose bytes never left (`schedule.py` alike): a send at the deadline with a zero answer
  delay used to end by its judgement, sent never.

**K1e-1 — the heap** (2026-11-22): the machine frees a cell by the rule `Scheduler.tla` states — its step ended, the
window let it go (`Window::read` now returns what it evicts), no reader of its captured values nor observation of it
as a cause is still to be dispatched, every prefix of its barrier key passed it. A reader copies its captured values
at its first dispatch and resends them (decision 10's frame built once), and its deadline is decided then, so it lets
go of its producers and its cause at that point, or when it ends unsent. A barrier keeps per key the members no
prefix has passed yet, each prefix's latest met instant and the member that met it, and its holders by the count of
members they wait for, fired as the sweep passes that count; a prefix of members expected stops for good at a failure.
Each line has one pending wake, dropped when it ends. Found by the heap executor, fixed before the commit: a step
freed by its own barrier's sweep before it let go of its producer, which then never freed — a step holding producers
is not freed, and it lets go before its barrier is told.
- **Executors**: every run of the differential, the stand and the traces ends with the cells the window keeps and
  no other (`Run::left == Run::kept`). `tests/heap.rs`, a counting allocator over `sim::run_streaming` (the lines from
  an iterator, no journal kept): 32,000 lines at 125 µs, eight subjects live at a time, a share of producers refused
  or answering without the value, reads and calls unknown — 1,333 subjects of eight cycles peak at 444,413 B above the
  plan, 10,666 subjects of one at 402,343 B; half the plan peaks at the same 402,343 B; 161 steps held at once in
  both. 24 s in a debug build. Mutations, each alone, red: a cell freed as the window lets it go (the differential,
  the stand, a trace); a skipped reader keeping its producer (cells left behind; the heap); the window's evictions not
  told; a holder read late walking its members' cells (the read-ahead invariance, the differential); a prefix's
  members never let go; a step's wake left behind when it ends (the heap over twice the length: 1.44 MB against
  0.92 MB, the steps held unchanged). The stalls of K1-8 and the bound chosen by rate are K1e-2's.

**K1e-2a — the bound in the validator** (2026-11-22, the command log's half; the scheduler's — `fits`, the machine's
shared rules, its executors and the TLA+ model — is K1e-2b): `command-log::plan` holds the rules the validator and the
machine share. The validator keeps each window entry's worst intended start, end and hold (`Window` hands what it
evicts or displaces to a callback, and gains a crate-private `get_mut`), each barrier key's latest worst end beside its
count, a min-heap of holds the window let go and one of ends; `Peaks` gains `held`, `pending`, `horizon`, `barriers`;
the commitment carries the report's peaks; both heaps are hot parts by name. `feasible` holds every step's think from
the run's start (a step with no dependency, or awaiting only a barrier no member was read before, with `think > at`
passed and was released at its think). `RULES_REVISION` 6.
- **Executors**: `reference.py` decides the bound by definition, quadratically, and is compared at every read — each
  line's `R̂`, `Ê`, and the steps held and not ended at its read — on the corpus, whose base logs also run under a header
  with timeouts on W's scale (with seconds, every hold outlasts a log): 2,209 logs agree, and the corpus reaches reads
  at which a step is held only by a pin (12), only by its key's prefix (4), only by the read-ahead (1,014), and exactly
  at the threshold (2), each failing on zero. Mutations, each alone, red: the chain ignored; a barrier's members
  ignored; think ignored; the read-ahead ignored; pins dropped; the prefix hold dropped; strict for non-strict, for
  held and for pending; an observation's deadline from its own start; the let-go steps not counted; the think rule
  only beside dependencies. Twins: think past the plan with no dependency and with an empty barrier; the two new hot
  parts, each failing by name one line past its limit; the commitment's peaks equal to the report's. ADR-0046's V6 now
  runs timeouts on its window's scale and holds `held` and `pending` equal across its two plans: with seconds, its
  longer plan holds 57,996 steps against 8,000 — the bound grows with a plan shorter than its chains' horizon — and its
  heaps stayed under the end merges' peak, invisible to it.
- **Measured** (this laptop, release, one run at a time, A/B against f636577; `tests/scale.rs` `k1e2_bound`, a plan of
  eight live subjects through update–read–call cycles a line every 125 µs, W 20 ms, timeouts 10 s, its every hold
  outlasting the window; and M9's persisted path at 10⁷): the stress plan at 2·10⁶ validates at 38.6·10³ steps/s
  against 39.3·10³ (−1.8 %) and peaks at 154.8 MB against 134.6 MB — its two heaps of 1,279,904 steps; M9 at 10⁷
  validates at 43,972 steps/s against 43,013 (noise) and peaks at 1.163 GB against 1.120 GB — the larger window entry,
  its heaps empty (`held` equals `completions`, 834,884). A first run of the new side, a browser open beside it, gave
  38.7·10³ and 44,151 steps/s.
- **Found**: the stress plan's bound is nearly the whole plan — 1,279,904 of 2·10⁶ steps, the horizon 580 s — since a
  subject's chain spaced 3 ms below a 10 s deadline accumulates 20 s of worst lateness a cycle. A plan whose subjects
  chain below their deadlines is held whole by a machine under a full stall, and the bound says so; whether host plans
  do is read off the first host plan's bound (Rule of Three 1, § Owed's barrier and fraction triggers).

**K1e-2b — the bound in the scheduler** (2026-11-22): the machine takes the rules it shares with the validator from
`command-log::plan` (a step's key, its intended start, its action's timeout and safety, its captured values and
cause). A reader takes its captured values and decides its deadline when its release is decided, and lets go of its
producers and cause then — so a wake taken late holds nothing (decision 5's value "taken at release"); a cell drops
its record, bound lines, producers and values when its step ends; a holder of a key no member was read of keeps no
barrier (the validator counts none); `Machine::new` refuses a read-ahead past the header's, the bound's assumption,
`sim` building an unbounded one. `scheduler::fits` refuses a plan against one byte budget — `held · ENDED_CELL +
pending · (LIVE_CELL − ENDED_CELL + line) + barriers · BARRIER` — or a read-ahead past the header's, naming the part.
- **Executors**: `tests/costs.rs` measures, on the machine streaming three plans, 896 B an ended step held, 1,389 B a
  step not ended beside its 270-byte line, 556 B a barrier key, and holds each plan's peak within what `fits` counts
  of its own counts (`ENDED_CELL` 1,152, `LIVE_CELL` 1,792, `BARRIER` 768, some 30 % above); a cell keeping its record
  at the end is red (1,542 B an ended step). The differential holds the machine to the validator's bound on every run
  whose reads all lie within `[at − L, at]` — 822 of 3,780, 348 of them with a reader lagging, base logs also under
  timeouts on W's scale, an at-deadline script beside the three: no step released past its `R̂` or ended past its
  `Ê`, 1,290 ending exactly at it, no more held or pending than the bound; and the machine's barrier keys equal the
  validator's on every run (and every trace). `tests/bound.rs`, K1-8's stalls on chains of depth six spaced below T on
  distinct instants (45 of 60 steps held, 14 in the window, 37 by rate alone): a uniform stall of s ∈ {T/2, T − 1 µs,
  T} holds and leaves pending exactly what the validator derives with every timeout s, its last end the derived
  horizon; a stand that never answers ends every step of a settled chain unknown within the bound, its last end the
  horizon; a reader late past a step's plan ends it past its `Ê` (A2); a reader decided before its release lets its
  producer go at the decision (at most two held, not three); `fits` at, and a byte past, its budget. Mutations, each
  alone, red: the bound by rate alone (the chain ignored); the window counted twice; the machine freeing no prefix;
  pins kept past the decision; a holder of an empty key making a barrier; `fits` without the pending.

**K1e-2c — the bound in the model** (2026-11-22): `Scheduler.tla` follows the machine's pins — a reader takes its
value and lets go of its producer when its release is decided, a value missing then skipping it at its dispatch — and
computes the plan's own bound from its constants as the validator does: `EndsWithinBound` (no release or end past
it), `HeldWithinBound` and `PendingWithinBound` (with nothing due, no more held, nor read and not ended, than the bound
counts at the latest read), in every configuration with `Lag = 0` (the late reader's breaks A2 by design). A third
instance, `Bnd`, six lines with `W = 1` and `L = 1`, is the bound's own: at one read a producer is held only by its
pin and a member only by its key, two lines only by the read-ahead, and at another the bound holds three of six; its
tables are pinned (`BndTables`) and `tests/bound.rs` builds the same plan as a log and asserts the validator's
per-line bound has the same numbers. Found while tuning it: since the machine lets go of a pin at the decision, the
bound's pin term, the reader's latest release, is an upper bound of that instant, loose by the reader's think and
plan; kept, not narrowed, its meaning unchanged.
- **Executors** (`check.sh scheduler`): `Bnd` holds everything, 468,858 distinct states (answers cut to expected);
  the bound by rate alone (`PendingByRate`), without its pins, without its key's prefix, without the read-ahead, each
  breaks `HeldWithinBound` beside its twin; the earlier instances re-pinned (the window's 556,786, its late reader's
  1,665,393, the barrier's 105,278), every earlier mutant breaking what it broke; the four traces fit and the two
  forged ones do not.

**K1e-3a — a holder failed past the prefix** (2026-11-22, found by the K1e-3 round's Software Design role): a holder
awaiting its barrier as expected was failed only when the members' prefix reached the failed one, and stamped with
that member's end — read at 20, it ended at 10 when its earlier member answered at 50 (`Entry` against its own
`released = max(intended, read)`). Now a member that ends otherwise than expected fails at its end every holder
awaiting it expected, wherever the prefix stands: one read later is skipped for the first failure in time, at its end
or its read, whichever is later — as `schedule.py` defines it, which now names the first failure in time, as a step
dependency's skip does (K1b). The barrier keeps the lowest failed ordinal, where its expected prefix stops, apart from
the first failure in time; the model alike (`fail` and `ffail`), the skip reason the member that failed it — its
barrier instance re-pinned at 115,196 distinct states, every mutant breaking what it broke, the traces fitting.
- **Executors**: a twin (read at 20, skipped at 20); the differential reaches it (a counter, 9 holders), over corpus
  reports half of which now await their members expected and six reports of three or four reads, the differential's
  scripts drawn per log as well (lines of one number fared alike in every log); every differential run asserts no
  step released or ended before its read; 3,924 runs agree. The corpus changed, its rules not: revision 6's digest
  re-pinned.

**K1e-3b — the journal's bytes** (2026-11-22): decision 14's journal as decided for K1e-3, version 1.
- **The framing** is `command-log::frame` — a value's canonical line, its decoding refused unless re-encoded to the
  same bytes, the next line of a stream, a running digest, `FrameError` — public, and the log's reader and writer
  stand on it (the golden log's bytes and `FORMAT` unchanged). `command-log::wire` is public too, with a list of
  `u64`s, so that the journal writes its numbers as the log does: every `u64` a decimal string.
- **The vocabulary** is `scheduler::journal`: the lines, a `Writer` and a `Reader` that refuse another version, a
  header out of place, a truncated journal, a trailer that disagrees, a line after it, a non-canonical line, an
  unknown field; and the pure `journal::events`, which `sim` writes with the run (`Run.journal`). The machine says
  what the journal needs and `Entry` does not hold: a read's bindings (`Output::Bound`, each dependency's strength
  beside the line it bound or the barrier's key and its members' count), a release decided (`Output::Released`,
  with its deadline), an answer it took nothing from and why (`Output::Ignored` — stale, late, or its step ended,
  its cell freed or not). `MACHINE_REVISION` 1. J is in the journal's header though the log's profile holds it:
  the fold reads the journal alone. `serde` is the crate's one new dependency; `clippy.toml` refuses `std::fs`,
  `std::net` and the command log's file functions to the machine.
- **Executors**: the golden journal (`golden/journal.jsonl`, 19 lines, `JOURNAL`), holding every line and variant
  by exhaustive matches, both sides of every optional field and a `u64` at its top in every kind of field — also
  written and read in the host's serde build (`verify/inbox-features`); twins: every strict prefix of its bytes
  refused, a line dropped, swapped or changed and a miscounted trailer refused, the header first and only, nothing
  after the trailer, another version, a line not canonical four ways, a step's story in order, an answer stale,
  late and after its end marked so; the differential's 3,924 journals written, read back and told against their
  runs — every end, dispatch, send and answer, every read's bound lines, every release — their digest pinned
  beside `MACHINE_REVISION` (`JOURNALS`). Fourteen mutants, each killed: a release, an ignored answer's mark or a
  fault line dropped from `events`; the trailer unchecked; the end of input taken for the end; the header left out
  of the digest; a release's deadline, a barrier's members or a step's binding journaled wrong (the first two only
  the pinned digest sees); `std::fs`, a socket type, the log's file reader and the commitment's file read in the
  machine; a variant dropped from the golden with the golden and its pin regenerated alike. A vacuous assertion in the silent
  handler's twin (`… || true`) now asserts that nothing ends before the grace.

**K1e-3c-1 — journal version 2, its schema and its independent reader** (2026-11-22). The fold reads the journal
alone (decision 14), and two of its rules need what version 1 did not say: an observation's Undecided runs from its
cause's lateness, not its own polls', and the cause amendment asks whether its cause ended expected — so a `read`
names its action (`rest`, `bus`, `observe`, `host`) and an observation's cause's line, its own if it is its own
(`Output::Bound` carries the cause the machine bound). `JOURNAL_VERSION` 2; the machine decides as it did,
`MACHINE_REVISION` stays 1, and the differential's pins are by journal version and machine revision: (1, 1) kept,
(2, 1) appended. The normative schema (`schema/journal.schema.json`) and `vectors/journal.py` — the command log's
validator of the schema's subset, given the journal's schema as its root — check the golden's every line, its keys'
order, its canonical bytes, the header's place and the trailer, in `make vectors`.
- **Executors**: the golden holds the four actions and a cause both ways; the differential asserts every read's
  action is its record's and an observation's cause the step its log names, read before it or itself; fourteen
  golden copies, each resealed so that only the changed line is wrong — keys out of order, an unknown field, a number
  for a `u64`, a field missing, an action, end or skip unknown, a cause not a `u64`, version 1, a log identity not a
  digest, a miscounted trailer, a line after it, the header second, a space — each refused by `journal.py`; three
  mutants of the machine and `events` — a cause lost, a cause taken for the observation's own, a bus step journaled
  as a REST call — each killed by the differential's own check (the action's first check went through `From` itself,
  so that only the pinned digest saw the third: the test now names each action by its own match).

**K1e-3c-2 — the fold** (2026-11-22): `scheduler::verdict`, decision 14's fold as decided, `FOLD_REVISION` 1.
- **Two passes, one byte a line.** The first takes the journal in its order, keeping the steps read and not ended
  and, a line, its end, whether the harness's lateness made it (`UNDECIDED`: a backstop; a call's last attempt sent
  past J after its dispatch, or never; a release at or past its deadline though intended before it, read past J late),
  whether a later reader lacked its value (`PRODUCER`), whether its bytes left past J after its intended start or never
  (`LATE`) — and tells every end against its step's events: its read, release, attempts, sends and the answer the
  machine took, a skip's reason, a backstop's fault line after it. The second takes the reads, in line order, every
  dependency a line before its holder: conditional from its dependencies' bytes (`CONDITIONAL`, transitively) and a
  barrier key's flags (members counted, one doubtful, one conditional, one failed), then the class by precedence. An
  observation's Undecided is its cause's `LATE` — its own polls' lateness is not the measure, its deadline runs from
  its cause — a call's its last attempt's. A backstop's step is Undecided, its run void on the fault.
- **The log is the commitment's digest and step count**: a journal that reads fewer lines than its log holds is not
  that log's run, though every line of it is sound.
- **The verdict by definition** (`vectors/verdict.py`): the decision's sentences, each its own function, in memory —
  conditional recursive, members as decision 8 says them, no byte and no second pass — and the evidence told by
  definition. It stops at the first disagreement, and counts what it reached.
- **Executors**: eleven golden runs (`golden/runs/*.jsonl`, under the normative schema in `make vectors`), each with
  its classes and verdict written by hand — a clean pass; a root and the skips it causes; doubt three levels deep;
  a dependent ended before its unknown dependency; a refused cause observed (the amendment alone); a producer without
  its value; a retry never sent; a barrier settled beside a step sent; a late cause; a read past its deadline; a
  backstop — every class and verdict but `unreadable`, every rule of Undecided, every way to be conditional, reached;
  twelve twins of broken evidence, each resealed so that only the fold can refuse it — an end dropped, duplicated or
  given another's line, a read or a dispatch dropped, an end's read moved, a skipped step said expected, a barrier's
  members miscounted, a backstop's fault dropped, a fault without its backstop, another log, a log of more steps —
  and an unreadable line; the definition agrees on every one. The differential folds its 3,924 journals and the
  definition agrees with each; its scripts now answer a capturing step without its values one time in six, so that a
  producer root is reached there too (12); their verdicts' digest is pinned by journal version, machine revision and
  fold revision (`CLASSES`), the journals' (2, 1) re-pinned for the scripts. Seventeen mutants of the fold, each killed
  — fifteen by the goldens and twins, the barrier's provenance cut to one level and J taken inclusive only by the
  differential.

**K1e-3d — failures injected on the stand** (2026-11-22): the one oracle not written from decision 6. `run_stand`
gains `run_stand_failing`, which injects one failure at a line's first attempt — a request lost on its way (never
processed, never answered), an answer lost (processed), a refusal (unprocessed, answered unexpected) — what a network
or a server does, not what the decision says of it. Every clean corpus log passes on the stand; then, a failure at
each of its lines in turn, every other step whose end the failure changed must not be a root, the failed step itself
a root or undecided, and the run must not pass: 4,281 runs, 142 steps changed by a lost request, 115 by a lost
answer, 115 by a refusal, none counted a root.
- **What it found**: 27 barrier holders made second roots by a refused member — awaiting their members `Settled`,
  answered unexpected by the stand because a refused member was not "processed". The holders carry no expectation
  (each also awaits a step `Sent`, and the validator's `Sent` rule leaves them none), so the host would judge any
  answer of theirs expected: the stand's rule is a probe of order — a holder must follow its members — and a refusal
  is the server's handling of a member, not a member that never arrived. The stand now counts a refused request
  handled; a holder that overtakes one still fails. Decision 6 is unchanged; what it leaves open is in § Owed.
- **Executors**: three mutants, each turning the oracle red with second roots — the refusal not handled by the stand,
  an unknown dependency not doubtful, a failure's dependents not inconclusive.

**K2a-1 — an answer's JSON** (2026-11-22): `command-log::json`, the crate's own reader of what a stand answers
(decision 15): strict RFC 8259 over UTF-8, unreadable on half of a surrogate pair, a member named twice, nesting past
128 or an exponent past 18 digits; numbers kept as their text, equal by their decimal values; values member by member,
objects in any order; RFC 6901 pointers, `-` an object's member and never an array's element. Its `Debug` shows a
value's shape, never its content. It denies indexing, slicing, `unwrap`, `expect`, `panic` and `unreachable`, beside
the crate's arithmetic gate.
- **Executors**: a twin per rule; and `vectors/json.py` — Python's `json` with hooks refusing a member named twice and
  `NaN`, its own depth, surrogate and exponent rules, `decimal` for numbers, its own RFC 6901 — on 6,000 documents
  drawn by the test (numbers in tricky forms, escapes and surrogates, names repeated one object in eight, nesting
  around 128, a third of them mutated by a byte), every pointer into each and beside it, and 6,000 pairs of values —
  one value written two ways (numbers rewritten, letters escaped, members reordered) and values drawn apart: 11,499
  cases agree, every rule reached (88 duplicates, 63 too deep, 454 surrogates, 176 exponents, 118 not UTF-8, 1,407
  syntax; 2,524 pairs equal). Fourteen mutants, each killed: a member named twice read, the depth off by one, numbers
  equal by text, `-0` signed, the exponent's digits by one, an index with a leading zero, a lone surrogate replaced,
  objects compared in order, trailing bytes, a raw control character, `~0` and `~1` swapped, trailing zeros kept, a
  leading zero, the exponent's sign dropped.

**K2a-2 — the fills and the request** (2026-11-22): `fills` resolves every hole of a step — public renderings from the
namespace, key material from the run's secret, times from the run's start or the frame's first send, captured values
as one JSON value's text (a string's content in a plain value, re-quoted in a JSON token; a number as written) —
and `request` builds a REST step's frame and checks its static half (decision 15). `Captured` moves into the command
log, the machine re-exporting it. Amends ADR-0046 decision 3: `Secret32` is HMAC-SHA256 under the run's secret
(`render::RunSecret`, 32 bytes the shell draws, never serialised) of what it is rendered of and its role, no longer a
function of the namespace; a correlation id is drawn from the namespace under its own key, `render::correlation`. No
`Debug` of a fill, a frame, a captured value, a rendered value or the secret shows its bytes. The frame's digest
waits for the slice that journals it.
- **Executors**: `vectors/request.py` — `urllib.parse.quote`, `json.dumps`, `hmac`, the decision's sentences — builds
  `golden/frames.txt` from the 35 cases of `golden/frames.jsonl` (a custom method, a collection's verb, values
  encoded beside literals, a query in order, rendered values, times from the run's start and the send, captured
  values in a path, a query, a header and a body, key material in a raw body; 27 refusals, one defect each), diffed in
  `make vectors`, and the crate builds the same text; `render.py` computes the key material under two secrets and the
  correlation ids, and `golden/render.txt` holds them; twins: `check` refuses what the frame refuses for its plan, a
  frame is a function of its fills, key material follows the secret and not the namespace, a correlation id is its
  attempt's and no planned role's, no `Debug` shows a planted secret, key or captured value. Seventeen mutants, each
  killed: literals encoded, `+` for a space, lower-case hex, reserved names by case (alive until a `Content-Length`
  case was added), a captured `.` segment let through, no content type, the query's `?`, a filled header unchecked, a
  decorated name not reserved, `%` in a literal, `Sent` from the run's start, a captured string as its JSON text,
  `Secret32` from the namespace, a derived `Debug`, the correlation id under the rendered key, the HMAC's pads swapped,
  the role left out of the secret.

**K2a-3 — reading an answer, and the judge** (2026-11-22): `command-log::answer` reads what an answer says against
a record — the problem's code (the profile's pointer, a status of 400 or more, one `Content-Type` whose essence is
`application/problem+json` without case, an object body), predicates over the body (none holds past the cap or of a
body not JSON), the values a step takes (only those every use can carry, as one JSON value's text; `Location` its
last path segment, strictly decoded), `Retry-After` in delta-seconds; `scheduler::judge` is decision 15's list over
it, with no JSON, returning the judgement beside its rule, the status, the code as the journal may carry it (a short
dotted name, or `Other`), the wait and the slots missing. Both deny indexing, slicing, `unwrap`, `expect`, `panic`
and `unreachable`.
- **Executors**: `vectors/judge.py` — the rules in Python, JSON by `vectors/json.py` — judges the 54 cases of
  `golden/answers.jsonl` into `golden/judgements.txt`, diffed in `make vectors`, and the crates give the same text:
  statuses at each class's edges (199, 300, 399, 499, 500, 999), media types with parameters, in another case and
  another type, two of them, a code not a string or in no object, a code past the journal's bound and one not a
  dotted name, two `ETag` lines, a header not UTF-8, `Location` as a path, a URL with a query and a fragment, a
  fragment alone, `%zz`, `%FF`, a dot segment; `Retry-After` in seconds, as a date, negative, huge, twice; an
  `expect` listing a `not_run` code and one listing 503, `problem: None` against a coded refusal and against the
  gateway's 404, a 2xx problem with a `not_run` code; predicates that hold and fail, over a body past the cap and one
  not JSON, a captured value in one; polls that hold, not yet, 304, 404, 503, a certified 503, one without an
  expectation; a sent-only step's 409, 504 and not-run code; a reset; no answer. The judge is total over 4,096 drawn
  answers — statuses 100 to 999, header lines of every name the rules read, bodies of random bytes and of hostile
  JSON (`1e400`, `-0`, nesting) — never failing on a record whose predicates the validator passes, every capture
  taken or named missing; no `Debug` of a judgement shows a taken value. Twenty-one mutants, each killed: `not_run`
  before `expect`, 5xx before `expect`, a wildcard `None`, 1xx certain, 500 certain, a poll's empty expectation
  passed, a poll's 3xx passed, codes unbounded, no `Retry-After`, the code read from any body, the code from 401
  up, the media type by case, the media type by prefix, the first of two lines taken, a `Location`'s fragment kept,
  a bad escape kept, a dot segment carried, a number not taken, `Contains` as `Equals`, `Retry-After` none on
  overflow, a body past the cap read — three alive until their cases were added (a poll without an expectation, a
  poll's 304, a fragment without a query).

**K2a-4a — FORMAT 4's model** (2026-11-22): the profile names what the shell reads and sends by — `problem` (the
pointer to a problem's code; the host's `/code`), `correlation` (the header of each attempt's correlation id),
`resend` (a harness resend's floor), `answer_cap` (the most of a body the shell reads) and `idle` (the stand's
downstream idle) — and the header rule checks them: the pointer RFC 6901's, the correlation header a token outside
the reserved names in any case, the three numbers above zero; `vectors/reference.py` decides the same.
`FORMAT_VERSION` 4, `RULES_REVISION` 7. The record's documentation says what decision 15 made of it: `problem` is a
code, an empty `expect` a sent-only step's or a host call's, `Location` the last segment, `Equals` value equality,
`TimeBase::Sent` the frame's first dispatch. The sample no longer plans an `X-Request-Id` per step (the correlation
id is the shell's, one an attempt): its profile names `X-Request-Id` as the correlation header, and the rendered
header it planned is `X-Trace`.
- **Executors**: a twin per field (a pointer without its slash or with a bad escape, a correlation header missing,
  not a token, reserved in either case, no floor, no cap, no idle; an empty pointer, the document itself, passes);
  the golden log, its schema and `FORMAT` regenerated; the rules' meaning pinned at revision 7; the corpus's journals
  re-pinned under (2, 1), and the golden runs written again, their log and validator being the new ones — every
  class and verdict as before.

**K2a-4b — the rules over steps** (2026-11-22): what the shell sends and judges is checked before a run, under four
rules no deviation exempts — `frame` (a REST call's request can be built: `request::check`, and no UUID bytes or key
material in a plain value; the arity check moves here from `operation`), `predicate` (an RFC 6901 pointer, a literal
of JSON text, no bytes), `expect` (a REST call expects responses, a message its delivery, an observation its effect, a
host call nothing; only a step awaiting a dependency only sent expects nothing), `secret` (no key material and no
value captured as secret in a request's target or a message's key) — and four rules extended: `header` (a
`not_taken` operation names its codes), `operation` (an idempotent call sends no value of a slot its own first
attempt rewrites), `producer` (one producer a slot a record), `span` (no captured value in a raw span).
`RULES_REVISION` 8; `vectors/reference.py` decides each by definition, the frame's static half by
`vectors/request.py`'s and literals by `vectors/json.py`. The fixture's builders expect what their action plainly
does, and awaiting a step only sent clears a call's or a message's expectation; the corpus's logs expect so too.
- **Executors**: a twin per rule and per case (a reserved, a correlation and a lower-case header, a line break, a
  dot and an empty segment, bytes in a header and a path, a poll's frame, a deviated frame; pointers without their
  slash and with a bad escape, a literal not JSON, key material in a predicate, a body's predicate; an expectation
  missing or misfit on a call, a poll, a message and a host call, a sent-only call clean; a captured secret in a path
  against one in a header, key material in a query and in a message's key; a not-taken operation without codes; an
  idempotent update sending its own ETag; a read from two producers; a captured raw span). V2: six mutant kinds join
  the corpus — a reserved header, a dot segment, key material in a path, a predicate written wrong, an expectation
  dropped, one misfit — and the validator and the reference agree on its 2,841 logs, which reach `frame` 324 times,
  `expect` 249, `predicate` 65, `secret` 51; the test now fails if the corpus reaches none of them. Sixteen clean
  logs fewer: the mutants that drop a step's only `Sent` edge now assert its answer without an expectation, and are
  refused; the scheduler's differential runs 3,732 journals, its digests re-pinned under (2, 1) and (2, 1, 1), the
  golden runs written again. Sixteen mutants, each killed: `frame` unchecked, bytes in a plain value, a pointer
  and a literal unchecked, bytes in a predicate, a misfit expected, no expectation let through, a sent-only call
  refused, a captured secret, key material in a target, a message's key unchecked, a not-taken resend without codes,
  an idempotent call's own slot, two producers, a captured raw span, `frame` deviable — eight by the twins and the
  reference both, the other eight by the twins alone (the corpus plans no secret capture, no message keyed by a
  secret, no idempotent update and no second producer).

Nothing else yet. **M11** (K1): the machine's events a second at bursts in `sim` — a partitioned machine is needed when a
burst's dispatch lateness passes J at the host's largest planned burst. **M12** (K2): own lateness and the receive
side's lag against offered rate on this laptop with the stand running — past J, the laptop gives correctness runs
only and measures move to the 12-core box.

## Owed

- **The session's COMMIT in doubt** (the library's `crates/session`, the owner's review): a COMMIT refused on a
  closed connection may have committed; `SessionError::Commit` should say "in doubt", not "nothing is durable".
- **The host's answer to a session failure** (its services, their owners): an outcome in doubt must not say "send it
  again" (`Retry-After` on every 503); the not-run problem codes listed in the operations table.
- **Facts carrying a value of their cause** (the host, ADR-0071's envelope): without one the discriminating-`until`
  rule refuses the plans that need them.
- **One SHA-256 a pass and a hashing thread** (ADR-0046 § Owed): K3 or later.
- **Statement time against commit time** in the per-hop joins (QAS-SCALE-0001 defines hops from commit to commit).
- **ABBA, snapshots, TLA+ trace validation of the shell, a partitioned machine**: each on its trigger.
- **The bound's deferred parts** (K1e-2): the bytes of the steps not ended (the reader exposing a line's length;
  trigger: a host plan's lines far from their mean); the peak planned rate and a burst (M11, with its width); the
  release rate when a stall clears (M12); a bound parameterised by the reader's lag (K2's reader measured late past
  J); ports (K2-7).
- **Barrier keys** (K1e-2): every step is a member of its key `(kind, scope, label, n)`, with no subject, held until
  every earlier member of the key ended, and the machine keeps a barrier a key for the run. (B) a barrier keeping each
  unpassed member's met instants, so a cell frees whatever its key's prefixes — trigger: `held − completions`
  material on the first host plan; (C) barrier labels declared in the header, FORMAT 4 — trigger: `barriers` times
  the bytes a key past a budget on a host plan.
- **A dependent asserting a refused dependency's effect** (K1e-3d): a step awaiting a dependency `Settled` may carry
  an expectation, and one over that dependency's effect — a report counting its members — fails when the dependency
  is refused, a second root under decision 6, which makes a `Settled` dependent conditional only on an unknown or
  skipped end. No executor reaches it: the stand models no content but `If-Match` and a barrier's order, and the
  corpus's `Settled` holders carry no expectation. Trigger: the first host plan with a step awaiting `Settled` that
  carries an expectation — then, under the gate, the validator requiring `Expected` of an expectation over a
  dependency's effect against the fold making a `Settled` dependent of any end but expected conditional (which hides
  a dependent's own failure, never flips a verdict), with a stand that models the report's content to execute it.
- **Resends paced against the gateway's buckets** (K2a, Distributed Systems): a wave of Unknown `Idempotent` steps on
  one edge bucket resends at one `now + floor`, past its burst, and the 429s void the run. Trigger: K4's shaping,
  which every resend then goes through; until then a void, never a false pass.
- **A stand refusing connections** (K2a, Verifiability): every call ends unsent and Unknown, which the fold classes
  undecided — a void where the system failed. Trigger: K2-7, with the refused connects journaled, so that the fold
  tells them from the harness's lateness.
- **The host's `service.session_failed`** (the host's owner): tenancy, content and device answer it for a refused
  COMMIT as well — `503`, and `500` without `Retry-After` for a scope that absorbed a failure — so it certifies
  nothing there; the host's operations table must not list it in `not_run` until the code is split (no session
  opened, commit in doubt, aborted).
- **The host's frames against its routes** (the host's tool package): every operation's frame, built from example
  values, matches its own `template_regex` and no other route.
- **H-1's measurements, added by K2a**: whether the correlation header survives the gateway (`use_remote_address`, no
  `preserve_external_request_id`) and lies outside the prefix it strips; whether Envoy rewrites a UUID-shaped
  `x-request-id` under tracing; the largest answer of the plan's operations, for the cap; `Content-Type` parameters
  on answers; Unknown attempts with no line in Envoy's access log, which must be none.
- **A cell's residue** (K1e-2): an ended cell keeps its key's strings and its map node, some 0.6–0.9 kB; a compact
  ended cell and keys interned per barrier — trigger: capacity binding on a host plan.

## References

ADR-0045 (the faker), ADR-0046 (the command log); in the host: ADR-0071 (sealed payloads), ADR-0086 (a Device's
lives), ADR-0088 (DPoP), QAS-SCALE-0001, QAS-SCALE-0002, QAS-PERF-0007, `tools/gateway_routes.py`.

## Change Log

- 2026-11-21: Proposed under the gate: a blind solver (binding at read; the release rule and `feasible`; per-producer
  captures; the strength table; barriers by prefix; ten changes to the command log), the main position (refuted on
  frames built once with their decorators, shaping by the lower model, barrier counters, lateness formulas negative
  under early completions, the store bounded by W and L, facts arriving before their key), a discriminator (the
  crate boundary as purity's executor, provenance, bounds derived from deadlines, one chain of instants), and two
  roles: Distributed Systems (not taken by problem code, stamped inputs and a watermark, paused time and sockets, the
  `read` link and interval verdicts, the `Sent`-only rule, the bus's deadline and sealing, an assigned fact reader,
  held facts in place of the `ahead` rule, the discriminating `until`, correlation per attempt, clock anchors, the
  bucket wait, transitive provenance) and Verifiability (the stateful server model, a scheduler by definition, corpus
  coverage, ties, the dependency allowlist, TLA+ properties and mutants that close, trace validation in K1, the
  failure-path heap, bunching, the verdict fold in K1, the fault matrix, hidden retries, void twins, the gateway's
  refill and the request id at the first host run).
- 2026-11-21: K0a's first part built (§ Measured): FORMAT 3's model.
- 2026-11-21: K0a's second part built (§ Measured): FORMAT 3's rules, `RULES_REVISION` 4; an observation may be its
  own cause; `until` applies to observations of the bus.
- 2026-11-21: K1 cut into K1a–K1e; K1a built (§ Measured): the window as a module, the machine, the ordering buffer and
  `sim`.
- 2026-11-21: K1b built (§ Measured); decision 7 completed: a step released past its deadline ends unknown, unsent.
- 2026-11-21: K1c built (§ Measured): the model stand; `producer` refuses a reader whose producer captures nothing.
- 2026-11-22: K1d built (§ Measured): the TLA+ model, its mutants and trace validation; the heap's free rule stated for
  K1e, with the two requirements the model found; `PendingByRate` moved to K1e.
- 2026-11-22: K1e cut into three parts; K1e-1 built (§ Measured): the machine frees its cells by the model's rule.
- 2026-11-22: K1e-2 decided under the gate (solver, discriminator, three roles); decision 7 gains the bound as built;
  decision 13's peak rate deferred to M11; K1e-2a built (§ Measured).
- 2026-11-22: K1e-2b built (§ Measured): the machine on the shared rules, `fits`, the bound's executors in `sim`.
- 2026-11-22: K1e-2c built (§ Measured): the bound in the model, its own instance tied to the validator, `PendingByRate`.
- 2026-11-22: K1e-3 decided under the gate (decision 14's journal and verdict, decision 6's cause amendment, measures to
  K5); K1e-3a built (§ Measured): a holder failed by a member past its prefix.
- 2026-11-22: K1e-3b built (§ Measured): the journal's bytes, `command-log::frame` and `wire` public, the machine's
  bindings, releases and ignored answers told, `MACHINE_REVISION` 1.
- 2026-11-22: K1e-3c-1 built (§ Measured): journal version 2 — a read's action and an observation's cause — its
  normative schema and `vectors/journal.py`.
- 2026-11-22: K1e-3c-2 built (§ Measured): the fold, its verdict by definition, golden runs and twins; the cause
  amendment moved from K1e-3d into it.
- 2026-11-22: K1e-3d built (§ Measured): failures injected on the stand, a failure counted at its root only; the stand
  counts a refused request handled; a `Settled` dependent asserting a refused dependency's effect owed.
- 2026-11-22: § Status names what is built: K0a and K1; K2 is next.
- 2026-11-22: K2 cut into K2a (pure) and K2b (the shell); decision 15 decided under the gate (a solver and a
  discriminator on Fable, three roles on Opus); decision 11's examples are the host's codes; K2a-1 built (§ Measured):
  an answer's JSON, read by the crate's own reader.
- 2026-11-22: K2a-2 built (§ Measured): the fills and the request; `Secret32` from the run's secret (ADR-0046
  decision 3 amended); K2-5 gains the frame's byte identity across attempts, from K2a-2's row.
- 2026-11-22: K2a-3 built (§ Measured): reading an answer, and the judge.
- 2026-11-22: K2a-4 cut into K2a-4a and K2a-4b; K2a-4a built (§ Measured): FORMAT 4's model.
- 2026-11-22: K2a-4b built (§ Measured): the rules over steps — `frame`, `predicate`, `expect`, `secret`, and four
  extended; `RULES_REVISION` 8.
