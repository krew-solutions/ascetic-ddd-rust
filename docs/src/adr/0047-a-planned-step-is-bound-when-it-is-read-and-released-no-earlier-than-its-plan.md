# ADR-0047: A planned step is bound when it is read and released no earlier than its plan

## Status

Proposed (2026-11-21); slices K0a, K1 (K1a to K1e), K2a, K2b-0a, K2b-0b, K2b-1, K2b-2 (K2b-2a, K2b-2b) K2b-3 (K2b-3a, K2b-3b), K2b-4, K2b-5, K2b-6 (K2b-6a, K2b-6b), K2b-7a, K2b-7b, K2b-7c (K2b-7c-1, K2b-7c-2), K2b-7d, K2b-8a, K2b-8b (K2b-8b-1, K2b-8b-2), K2b-8c (K2b-8c-1 to K2b-8c-7b) and K2b-8d (K2b-8d-1, K2b-8d-2) built (§ Measured), decision 21 decided — the harness shapes at the hand-off (§ Decision 21), K2b-9 to build it, then K2b-7e and the first host run, whose memory bound counts the connections K2b-8 decides. Stage 3 of three toward the host's
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
  and the fold, in two: **K2a-5a** the machine and its model — polls re-polled, resends spaced; **K2a-5b** the
  journal's version 3 and the fold licensing resends.
- **K2b** (the shell, decision 16), in parts: **K2b-0a** the frontier in the scheduler, `sim` driven through it;
  **K2b-0b** the machine re-checking a wake at the instant taken, the fold seeing a resend taken late; **K2b-1** the
  `load-run` crate — the clock, the frontier's lock, the machine loop, the line source and the journal sink, a scripted
  carrier; **K2b-2** REST over hyper, in two: **K2b-2a** the transport — the counting `Io`, the connection, the pool, the
  attempt, the `Stand` and `Egress` ports; **K2b-2b** the inverse-judge stub, `sim::Attempt.seen`, the shell's journal
  equal to `sim`'s over REST; **K2b-3** the harness's lateness attributed (decision 17), in two: **K2b-3a** journal
  version 4 — the machine's and the hand-off's lateness, the censor marked, the stuck hold; **K2b-3b** journal
  version 5 — the receive side;
  **K2b-4** frames, correlation, decorators; **K2b-5** resends end to end; **K2b-6** connections on the real clock, in two: **K2b-6a** the connection
  mechanisms and the live composition, **K2b-6b** the real clock's evidence (K2-3, the lateness drill, the timed wait);
  **K2b-7** the gates before T0 and the shell's memory (decision 18), in five: **K2b-7a** the defects the gate found
  in the built shell; **K2b-7b** journal version 6 — a run's stop, the source thread's lag, the stages' evidence —
  in the pure fold; **K2b-7c** the shell's stop and its lag, in two: **K2b-7c-1** the stop, **K2b-7c-2** the source
  thread's lag, its park detector measured first; **K2b-7d** the three stages before T0; **K2b-7e** the
  shell's memory; **K2b-8** a censored attempt's connection, its closing report and the ports (decision 19), in
  four: **K2b-8a** journal version 7 in the pure fold; **K2b-8b** the shell's censor, report and connect errors, in two — **K2b-8b-1** a connect's failure classed, zero
  linger, no shutdown, saturation by age; **K2b-8b-2** the reset at the censor, no byte or hand-over at the deadline,
  an unused connection pooled, a timer's exact lag;
  **K2b-8c** ports, the cap, admission's bound, the connect probe (decision 20), in seven — **K2b-8c-1** the
  egress's ports and the cap over each socket's life; **K2b-8c-2** a wait at the cap; **K2b-8c-3** the plan, the
  carrier's needs and the events; **K2b-8c-4** the descriptors; **K2b-8c-5** endpoints and sources; **K2b-8c-6** the
  probe; **K2b-8c-7** the chain and the evidence, in two — **K2b-8c-7a** the chain, **K2b-8c-7b** admission's
  evidence in the journal; **K2b-8d** the real clock's twins, in two — **K2b-8d-1** a driver's events, `TIME_WAIT`,
  exhausted ports measured; **K2b-8d-2** a hog over the deadline while connecting and writing, a whole process
  stalled; **K2b-9** the harness shaping at the hand-off (decision 21), in four — **K2b-9a** the machine's poll grid at
  the plan's instant; **K2b-9b** the pure pace, `sim` paced, journal version 9, the fold's revision 8; **K2b-9c** the
  shell's gate, its type, I2 and κ at admission; **K2b-9d** the twins.
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

### 16. The shell: inputs ordered by a frontier, HTTP/1.1 attempts, the host's ports

*Decided for K2b* (2026-11-23, under the gate: a blind solver and a discriminator on Fable, then three roles —
Distributed Systems, Verifiability, Performance — on Opus; it amends decision 4 and refines decisions 1, 9, 10, 11;
§ Alternatives Considered keeps what each refuted):
- **Structure.** Two crates hold (`scheduler`'s root forbids what a shell needs). `crates/load-run`: tokio, hyper's
  `client::conn::http1`, http, http-body, bytes, getrandom, the scheduler, the command log — a dependency allowlist;
  no hyper-util (its pool is keyed by authority, not by source; it shows no pool, connect or write instant; a dropped
  response cannot be kept past a deadline), no TLS (the compose stand's listener is plaintext; `Egress::Io` leaves it to
  the host), HTTP/1.1 only (the gateway negotiates h2 with a real client: a declared difference with its trigger). One
  machine task. Ports the host implements: `Clock`, `Stand` (a route per step; the gates before T0), `Egress` (connect
  from a source; its sources and ports; zero linger on an abandoned or capped connection), `Decorator` (once per
  attempt, seeing the step, the attempt, the caller, the operation, the route, the final frame, the correlation id, the
  deadline and wall time; only `op.decorated` names back). The line source and the journal sink are ports too, with
  in-task adapters for tests and thread adapters live: a std thread or `spawn_blocking` under paused time races the
  stamps. `HostCalls` and `Facts` wait for K3: a log with channels or host calls is refused. The run secret is a
  parameter (`os_secret()` from the OS, called by the composition root). `Runtimes { machine, sources }`: live, one
  `current_thread` runtime per source (a lag sampler per worker), the machine on its own; in tests one paused runtime.
  Dispatch spawns an attempt on its source's runtime, no queue (open loop: a queue hides lateness).
- **The watermark is constructed** (amending decision 4, whose wording — the minimum of each source's last stamp —
  stalls on a source awaiting a long answer; `order.rs`'s "the least instant any source may still report" stands). A
  pure `scheduler::frontier::Frontier` holds the open attempts by deadline, the floors of answers being judged and the
  inputs pushed: `open` at dispatch (a deadline at or below what was released censors at once, into the machine's
  buffer); `sent` pushes `Sent` iff before the deadline; `arrived` — past the deadline the censor at the deadline first —
  holds a floor while the answer is read and judged; `ended` pushes the answer at its arrival, and `Sent` before it if
  none was (an early local reply); `drain(now, settled)` releases through the least of `now − 1` (or `now`, settled)
  and the least floor less one, censoring at its deadline every open attempt with no arrival within it. The shell holds
  it under one mutex, every stamp a clock read and a push under it; inputs are taken in (stamp, line, kind) order
  among those released, a stamp below the last taken a harness fault — an input caused at an instant may sort below one
  already taken there, as `sim` takes it (the frontier's property found the key-order rule the roles proposed wrong). The frontier decides a deadline; an attempt's timer only leaves the pool gap.
  `sim` drives the same frontier. The loop reads the machine's own pending wakes, never its outputs' copies — a step's
  end cancels its backstop, and a run waits for no stale one (K2b-6a: a live run idled to its last call's deadline and
  grace); and it ends on what the frontier holds and who may yet push to it read in one lock — read apart, an attempt
  on another thread could push its end and go between them (K2b-6a: one run in fifteen lost an answer). While an answer is judged the machine takes nothing at or past its floor — neither
  the frontier's inputs nor its own lines and wakes, which are stamped at the instant taken; they are taken once it
  ends, late, the judging's lateness shown (K2b-1: taking them meanwhile put an input below the machine's clock, which
  `Machine::step` absorbs silently; the loop refuses one, a harness fault).
- **The machine re-checks a wake at the instant it is taken**: a resend leaves only if `now + J` is before the
  deadline, a poll only if `now` is; else the step ends as it stands, then — a real timer breaks A3 by its slack, and a
  resend taken late is otherwise one the fold refuses. The fold makes such an end undecided whatever the lateness (the
  first wording, "more than J after the resend was due", contradicted its own executor, K2b-0b): a resend its last
  answer licensed, due more than J before the deadline, that the step ended without — the machine schedules no other,
  and taken on time it leaves; an unknown ended past its deadline, released before it — only a wake due before the
  deadline and taken past it ends a step there. A poll's wake taken exactly at its deadline ends the step there, as an
  unsatisfied observation ends; journal version 3 cannot tell the two, so the fold counts it a root — a false failure,
  never a false pass — until version 4 journals a wake's due instant (§ Owed).
- **An attempt**: the frame from its fills — a call's at its first dispatch, so a resend's is the same bytes, a poll's
  at its own (K2b-4: rebuilt, not cached — a cache would save only its building, worth having if building shows in the
  hand-off) — (originally: from a cache, a call's built once and resent byte for byte, a poll's its own); the
  correlation and decorated headers; `try_send_request`, never once `now` reached the deadline. Two write instants,
  by the shell's own `Io` counting accepted bytes: the first byte is the commit point (no reconnect after it);
  `Sent` is the first flush of the socket once the request is handed over — hyper encodes the head and the body, one
  frame ready at once, before it flushes, and flushes only with its buffer empty; the attempt arms and hands over in one
  poll, so no flush comes between (K2b-2a: the body's end and a byte written, first guards of it, are dead there, and the
  twins hold hyper to it — amended by decision 18: hyper flushes an empty buffer on a connection the peer closed while
  it sat idle, so a byte of the armed request accepted is the guard after all). A request handed back unstarted, or with no byte accepted, is no end:
  the next connection, connects a floor apart, until a byte leaves or the frontier censors. The answer arrives at its
  last body frame (or at the cap passed: unreadable, the connection closed); it is read — lazily, JSON only when a
  predicate, a capture or a problem needs it, is owed (decision 18: `answer::read` parses every body within the cap) —
  and judged under the hold. Past a censored deadline a connection is kept
  only if `Sent` was pushed before it, until `sent + route_timeout + grace`, its late answer journaled ignored; an
  attempt with a byte out and no `Sent` is shut at the censor (else the request completes after its step ended). Pool
  per (source, endpoint), capped by the source's ports (`open + closes within TIME_WAIT`), retired at checkout before
  `idle − margin`; a connection the peer closes younger than that is a profile fault. One drop guard per attempt and per
  connection task: an attempt dropped unended, a panic, hyper's user or dispatch-gone error, a poisoned lock — each a
  harness fault, never the system's Unknown.
- **Lateness the fold must see** (superseded by decision 17: no `attempt` line, no source's lag, no hold's age in the
  fold) (journal version 4, an `attempt` line): when the machine's output reached the
  attempt (`handed`), decorated, checked out, connected, the first write, connect tries and refusals, the hold's age,
  the source's lag around a censor, abandoned. A censored Unknown is undecided when the source's lag or the hold
  delayed the answer past J (the receive side, as `call_late` is the send side). A hold is a fault only at a stuck
  scale (the route timeout), never at J (an OS slice exceeds J).
- **Before T0** (decision 18 orders these in three stages and makes the judging time evidence): the commitment;
  `fits`, with the shell's own memory — kept connections (`Peaks.kept`: REST steps
  within `R̂ + route_timeout + grace` of the cursor) at their measured cost, answers at the cap; the sources against
  `Egress`; ports and the descriptor limit against `kept`; the idle margin; the time of `answer::read` + `judge` on an
  object-dense document of the cap against J/4; `Stand::gates`. A plan refusal writes no journal; a gate refusal
  writes the header, one fault, the trailer.
- **Clock**: `now` (µs), `wall`, `sleep_until`, `settled(at)`. Live: a timed wait — the clock's own timer thread,
  parked to each sleeper's microsecond, wakes it; a push wakes the machine (no spin: a spin takes a core for the run;
  tokio's timer rounds up to its millisecond); `settled` returns once `now > at`, spinning at most the timer's slack,
  50 µs — in the loop, the microsecond it awaits. Virtual: an own µs counter and sleepers, tokio's documented
  auto-advance as the idle signal — no scaling of µs to ms, no tokio pin; idle with nothing to wake, it panics in the
  test's own future (`within`: a panic in a spawned task is tokio's to swallow). A lint refuses `std::time` and `tokio::time` outside `clock`. Every `select!` is `biased`.

### 17. The harness's lateness, attributed: whose each gap is, and what it may excuse

*Decided for K2b-3* (2026-11-23, under the gate: a blind solver and a discriminator on Fable, then two roles —
Distributed Systems, Verifiability — on Opus; it amends decisions 9, 14 and 16; § Alternatives Considered keeps what
each refuted):
- **Whose each gap is.** The harness's: the machine (`dispatch.at − due` — a wake taken late, a hold), the hand-off
  to the source's runtime (`handed − dispatch.at`) and, for an observation, the reader (`released − intended`: its
  window starts at its cause's intended instant, a call's at its release). The stand's: the connect (`connected −
  handed`, its accept backlog) and the write (`sent − connected`, its receive window) — less the source thread's lag
  in them, the harness's (decision 18). Version 3 charged the harness
  with the whole gap from dispatch to `Sent`.
- **What the harness's lateness may excuse: only an end it could have caused.** A step ended Unknown with its last
  attempt censored at its deadline, or never sent, is undecided when the harness's lateness summed over its attempts —
  and, for an observation, its reader's — exceeds J. An answer the system gave — a 500, a reset, a refusal — is its
  own, however late the harness sent it (version 3's `call_late` voided it). The frontier marks its censor, so the fold
  reads it rather than guessing it from a status and an instant.
- **The machine's own wake.** An Unknown or Unexpected end by a wake taken late is undecided: with attempts, a wake due
  before the deadline (a poll's, a call's fallback) — the poll's wake taken exactly at its deadline included, which
  version 3 could not tell from an unsatisfied observation's end; with none, a first dispatch due before its deadline
  and taken at or past it, which no version-3 rule reached. Version 3's slack of J for a fallback is dead — a fallback
  is scheduled only when its wake is more than J before the deadline — and is now a coherence rule.
- **Journal version 4**: `read.planned` (the plan's `at`), `dispatch.due` (the instant the wake that dispatched it was
  due), `sent.handed` and `sent.connected` (the attempt ready to connect — its task run, and for REST its frame built
  and decorated, a decorator being the host's own work, K2b-4 — then the request handed to a ready connection,
  the last if it reconnected; both the dispatch's instant in `sim`, `connected` at least `handed` in the shell),
  `answer.censored`, `end.wake` (the due instant of the wake whose taking ended the step, none through the cascade of
  skips). No new line kind: `journal::events`, pure, writes them from the machine's inputs and outputs, the same bytes
  in `sim` and the shell (decision 14). Coherence: `planned ≤ intended`; a first attempt's `due` its release, a call's
  resend's its answer plus its wait, a poll's after its last answer and before its deadline; `due ≤ dispatch.at ≤
  handed ≤ connected ≤ sent.at`; a censored answer Unknown, without status, at its attempt's deadline; with attempts, a
  wake no later than its end, a fallback's its answer plus its wait and more than J before the deadline, a poll's at
  most its deadline, and at the deadline only for an end there; with none, the wake the release, the end at or past
  the deadline; a fault's end exempt.
- **A hold that never ends** is a harness fault at the oldest floor plus the route timeout — a scale far above any
  legitimate judging (decision 16's J/4 gate), not a claim that every deadline has passed (an observation's timeout is
  unbounded): the loop, which takes nothing while held, sleeps until then.
- **The receive side (K2b-3b, journal version 5).** An answer in the host by its deadline but read after it, the
  source's runtime busy, is censored and counted the system's; today's attempt even gives up at `D + 1` on a wake of
  the clock's own thread that can come before tokio's driver turned (tokio 1.53.1,
  `runtime/scheduler/current_thread/mod.rs`): a false Root, live. Version 5: `answer.found {absent, deadline}` on an
  attempt whose own hold was late; `absent` advanced only by a look that found the answer incomplete — a pre-look at
  `D − J/2` and one at `D + 1`, each a turn-verified round (a yield polled under a flag waker only tokio's defer list
  fires); undecided when `absent < deadline` and `at − absent > J`. Exact for an answer in the host by its deadline
  read more than J late (never Root); a genuinely late answer is void, not fail, only if a look was late by more than
  `J/2` less the look's own duration. Sources on `current_thread` runtimes whose events per tick exceed their
  descriptors, and an `Egress::Io` that relays through no task, refused or stated before T0.

### 18. A run's stop, the source's thread attributed, and three stages before T0

*Decided for K2b-7* (2026-11-23, under the gate: a blind solver and a discriminator on Fable, then three roles —
Distributed Systems, Verifiability, Operability — on Opus; it amends decisions 14, 16 and 17; § Alternatives
Considered keeps what each refuted):
- **The source's thread is the harness's.** Judging runs synchronously on its source's `current_thread` runtime, where
  the connections' tasks run too and their flush stamps `Sent`; decision 17 gave `connected − handed` and `sent −
  connected` to the stand. So a sibling held back by another answer's judging, sent late but inside its deadline,
  then censored, is a Root — a false failure, live in version 5 (the discriminator's scenario). Neither the thread's
  busy time over the send path measures the harness's part (it charges the stand's own waits: at 5 % utilisation a
  connect retransmitted after 1 s collects some 50 ms, a stand's failure undecided) nor a task's wake-to-poll queue
  wait (an I/O event that comes during a sibling's poll is seen only at the next driver turn: no wait, a false Root).
  The harness's part is the **scheduling lag** of the send path's wakes: at each poll p of the attempt's task on
  `(handed, connected]` and of its connection's task on `(connected, Sent]`, `min(p − w, p − u)` — w the task's last
  pending, no earlier than its path's start; u the end of the thread's last park that blocked (a park's driver turn
  delivers every event ready before it; tokio 1.53.1 also parks with a zero timeout when work is pending, so a park
  that returned at once may deliver an event ready since the thread's busy stretch began — whether one blocked is told
  by the thread's voluntary context switches across it, measured exact, never by its length; K2b-7c-2's `nix`, Linux
  only, joins the shell's allowlist for it, and elsewhere no park counts as blocked). The windows are disjoint
  inside `[handed, Sent]`. Journal version 6's `sent.lag` is their sum; the fold adds it to the harness's lateness
  (and its `harness_sent`); coherence `lag ≤ sent.at − handed`. What it cannot see — the OS waking an idle thread, a
  driver turn's own dispatch — is J's, by J's definition (the client side's send delay). Parks are seen through
  tokio's `on_thread_park`/`on_thread_unpark`, so `Runtimes` are built by `Live`, and by a virtual constructor for
  tests (`lag` 0, as in `sim`); every task the shell spawns on a source runtime is spawned by the module that times it.
- **A run's fault stops it, in its journal.** One `fault` line `{ at, class }`, its class an enum whose variants carry
  their fields: `backstop { line }` (the run goes on, as before); the shell's `below`, `disordered`, `backdated`,
  `dropped`, `carrier { line, attempt, cannot }`, `held`, `stalled`, `poisoned`, `saturated` (attempt tasks past the
  steps not ended plus a margin), `clock`; `log { diverged | truncated | malformed | io }`; `gate { name }`. A
  carrier's `cannot` and a gate's name are dotted names in `Code::of`'s grammar; no host text is journaled — the
  host's reason goes to the caller in a structured error. A stop: no more dispatches; every attempt task and
  connection task aborted (a registry whose drop aborts, so an error or a dropped run future stops them too), a
  connection with a byte out and no `Sent` closed at zero linger (hyper flushes a request it already took after the
  cancel is seen); their ends awaited, bounded (a thread stuck judging never quiesces); then the fault — at no earlier
  than any line —, the trailer; `run` returns `Ran { .., end: Ended | Stopped }`. A stopped journal: lines read at most
  the log's, steps unended allowed, the fault the last line before the trailer; `void:fault`; no classes (the second
  pass marks a line after its end, and classes an unended step Root); a dispatched unended attempt may have reached the
  stand. `Written` writes its trailer only when finished, never when dropped (version 5's writer thread wrote one when
  the channel closed: a faulted run's journal read complete, and a fault after the last end folded pass or fail).
- **Before T0, three stages, by one predicate: a journal exists iff something outside the process was touched.**
  (1) *Admit*, pure: the commitment, the read-ahead, `carries`, the runtimes, `fits` with the shell's memory, the
  sources against `Egress`, a call's source within the profile's — now the validator's rule (it held only under a
  source-keyed rate limit). (2) *Measure*, on a stopwatch, not the run's clock: the judging time at the profile's cap
  — four shapes filling it (a flat object, an object of small objects, an array of numbers, an array of escaped
  strings), a capture at the last member, read, judged and dropped on a source runtime, the median of five after a
  warm-up, the worst shape. (3) *The stand's gates*, `Stand::gates`, bounded. Refused at (1) or (2): no journal, a
  structured refusal. At (3): the header (`t0` null), `fault { at: 0, class: gate }`, the trailer. The journal is
  opened after (3); T0's clock is started after it, and the run's wall start bound from that clock (a carrier built
  earlier stamped its frames' times stages 2 and 3 early). The header carries the evidence: the gates passed and their
  time, the judging time with its cap and its shape.
- **The judging time is evidence, not a refusal, until a plan claims J's precision** (decision 16's "against J/4",
  refused by Operability). With the lag attributed a long judgement makes no false Root, and an Expected end is never
  undecided; what consumes J's precision is a measure with a budget — K5's. Measured: J/4 at J = 2 ms admits a cap of
  about 3 KiB on this laptop (the worst shape an array of numbers, about 165 ns a byte), and misses the sample's 1 MiB
  by 380×; no parser closes that (§ Alternatives Considered). The refusal is K5's, for a plan carrying a measure; a
  preflight reports each shape's time, the largest cap within J/4 at a J and the smallest J at a cap.
- **Defects the gate found in the built shell** (K2b-7a): the decorator's and the connect's awaits raced no deadline
  (`TcpEgress` connects without one) — a decorator over a dead IdP or a stand dropping SYNs kept an attempt alive past
  its censor and the run from ending; an empty flush on a pooled connection the peer closed while idle pushed a false
  `Sent`; an attempt dropped at the run's end could decrement the live count before raising its fault, and the run end
  clean.

### 19. A censored attempt's connection reset; a never-sent censor's closing report; ports

*Decided for K2b-8* (2026-11-23, under the gate: a blind solver and a discriminator on Fable, then three roles —
Distributed Systems, Verifiability, Operability — on Opus; it amends decisions 11, 16, 17 and 18; § Alternatives
Considered keeps what each refuted):
- **A censored attempt's connection is reset at its last look, as the emulated client's would be** (decision 11's keep
  reversed, by two roles independently). A kept connection holds the gateway's upstream connection and the service's
  handler to the route timeout (15 s on the host's routes), where a client timing out at its deadline disconnects and
  the gateway cancels upstream: under a stall, requests a real client abandoned crowd out later steps — Roots the
  harness made, a void run turned fail. Keeping bought no verdict (the machine ignores a late answer; the fold's `late`
  is false past `D + 1`) and, under zero linger, no port: only a late answer's evidence, foregone — a keep within a
  budget below the gateway's limits owed, on the trigger "a measure needs late answers' latency". Reset means the
  connection's task aborted and its socket closed at zero linger; no byte of an armed request leaves once its clock
  reached the deadline (the shell's `Io` refuses it), and no request is handed to a connection at or past the deadline
  (decision 16's sentence, which had no code). A connection to which no request was handed goes back to the pool.
- **A never-sent censor owes a closing report** — the attempt's, pushed at `D + 1` or later, after a verified driver
  turn, riding `Seen` on an `Ended` the machine ignores (as `found` does: no new machine input, the same bytes in
  `sim`); the frontier refuses a report at or before its attempt's deadline (a harness fault). The censor cannot carry
  it: the censor is made on the machine's thread at the deadline, and a hog covering it is counted on the source's
  thread only after (a false Root otherwise — the discriminator's scenarios A and B). Journal version 7: `unsent {
  handed?, connected?, lag, refused, refused_by?, code }` — `handed` only once the attempt was ready (decorated),
  `refused` the peer's explicit refusals (refused, reset, a fresh connection closed before a byte), `refused_by` the
  last one's kind, `code` the carrier's dotted name. The fold holds a never-sent censor's end pending; its report
  decides: a Root iff `handed` is present, `refused ≥ 1` — the peer's own act, positive evidence: a connect left
  pending by the harness host's own network (conntrack, a local drop, the LAN) is elimination — and the harness's
  lateness over the step's attempts and the report's lag is at most J; otherwise undecided — a `found` for it (an
  early reply read late) undecided too. In a run that ended, every never-sent censor has exactly one report. A stand
  dropping SYNs on overload is then undecided — void, never a false fail; stand-side evidence (its listen overflows,
  the gateway's connection counts) bracketed before T0 and after the end, owed on the trigger "a host run voided by
  never-sent connect censors".
- **The source thread's lag, completed** (decision 18): a poll woken by the clock's own timer has an exact lag, `p −
  due`, added beyond the poll's bound (a whole process stalled — a container's CPU throttle, a pause — ends in a park
  that blocked, its bound ~0); the carrier's `handed` after decoration resets the path's lag (the decoration's delay is
  the hand-off's gap; a hand-back keeps it).
- **Connect errors are classed by the `Egress`**, which alone knows which step failed: `Connect::Stand(kind)` —
  refused, reset, aborted, timed out, unreachable — retried a floor apart and counted; `Connect::Harness { cannot,
  detail }` — a non-local address, exhausted ports (`EADDRINUSE` at a bound socket's bind, `EADDRNOTAVAIL` at an
  unbound one's connect), descriptors, an unknown endpoint, anything unknown — a carrier fault, the run stopped.
  `TcpEgress` sets zero linger at connect on every socket, and the shell forwards no shutdown (hyper's graceful close
  would send a FIN first, and a peer's FIN in FIN_WAIT2 makes a TIME_WAIT a later reset cannot undo).
- **Ports** (K2b-8c): `Egress::ports(source)`; the cap per source over each socket's life, from its creation to its
  `Io`'s drop; at the cap an idle connection evicted, else `egress.cap`; `Live` built from the cap (`events ≥ cap + 1`,
  exact: one event a descriptor a wait); admission's bound `pending` (one attempt out a step not ended, nothing kept)
  plus a reserve against the ports, the events and the descriptors' soft limit — the library offers raising it to the
  hard one; an unknown endpoint and a non-local source refused at admission (nothing outside touched); one TCP
  connect per (source, endpoint) a stage-3 gate of its own, never an HTTP request.
- **Saturation by age, not count** (a K2b-7c-1 defect): more than 1,024 steps sharing a deadline left their attempts
  alive past their censors, awaiting their look a microsecond later — a false `saturated`. A source is saturated when
  an attempt outlives its deadline by the route timeout.

### 20. Admission's bound kept apart from its soundness; a wait at the cap is the harness's

*Decided for K2b-8c-2 onward* (2026-11-24, under the gate: a blind solver on Opus, a discriminator on Fable, then
three roles — Operability, Verifiability, Software Design — on Opus; it amends decision 19's cap and decision 18's
stage 1; § Alternatives Considered keeps what each refuted):
- **A wait at the cap is the harness's, and a full cap waits** (amending decision 19's `egress.cap`). The wait lies
  between `handed` and `connected`, which decision 17 gives the stand; its whole span goes on the path's lag (a
  window's lag at least its length), so an attempt placed late is no Root — two roles found the false Root
  independently, and in the built dying and evict waits too. A connection idled (checked in, or put back unused)
  wakes one waiter, which tries its endpoint's pool first; a freed place wakes one. A never-placed attempt leaves at
  `D + 1`, its report `rest.cap.full` or `rest.cap.dying`. `egress.cap` stops the run only when a source's waiters
  have got no connection for the route timeout — every socket in flight frees within its step's timeout, so only a
  leak or a cap of nothing does it. `Peaks.pending` bounds the sockets only while the harness's lateness stays under
  the read-ahead (the discriminator): past it, waits, never a stop.
- **Two invariants, kept apart.** I1, soundness whatever the run does: `cap(s) + driver events ≤ events(s)` (tokio
  registers a waker, and a signal receiver when its `signal` feature is unified in — 2, an upper bound) and `Σ cap +
  open + reserve ≤ soft`. I2, the prediction under the plan's bound: `needed(s) ≤ cap(s)`. Admission checks both and
  mutates nothing; the library offers each derivation as a constructor — `Live` from the caps, the soft limit raised
  to the hard, a cap from the descriptors' share taken as its minimum with the ports.
- **Owners** (Software Design): I2 is the carrier's — it alone maps attempts to sockets (one an HTTP/1.1 attempt; an
  h2 carrier would multiplex) — stated by a method with no default, an unknown source an error, not none; the events
  are the runtimes' (`Runtimes` private, events per source, made by `Live`); the descriptors the process's, read after
  the runtimes exist and re-read as stage 3's last gate.
- **Endpoints per source**, `Stand::endpoints(&Header, source)` — pairs, not a product (a shard in maintenance, a v6
  source and a v6 gateway); a route outside its source's set a harness fault at the attempt. A stand routing by
  operation is the case whose sets are the image of its operations. The egress admits a source and a pair apart:
  the index, the family, distinct addresses (the unbound's routed address too, by a UDP connect and its local name —
  no packet), a throwaway local bind.
- **The probe** at stage 3: every (source, endpoint) at once, each its own bound, on its source's runtime, aborted
  when raced out; journaled as gates its numbers name — source, endpoint's index, kind — never the host's text.
- **The chain** (decision 18's owed typestate): `Admitted → Measured → Gated → Ready`, `run` taking only `Ready`; the
  library opening the journal from the evidence and building T0's clock after stage 3; the run's header and
  namespace reaching the carrier from the chain, not from its own construction (a reused carrier certified one
  namespace and sent another: 409s, false Roots); `testing`'s constructors held to their feature by the lint.
- **Evidence**: admission's numbers (per source needed, cap and its origin, events; soft, hard, open; assumed or not)
  in the journal's header; the refusal named `Inadmissible` (`Refusal` is the peer's, decision 19), its numbers
  typed, a host carrier's own error its own.
- **Cut**: K2b-8c-2 the wait at the cap; K2b-8c-3 the plan, the carrier's needs, I2, `Runtimes` and I1's events (an
  epoll twin on the real clock: `events = k` misses a ready socket, `k + 2` reads it); K2b-8c-4 the descriptors (a
  test binary of their own); K2b-8c-5 endpoints and sources; K2b-8c-6 the probe; K2b-8c-7 the chain and the
  evidence.

### 21. The harness shapes at the hand-off: a late harness delays its plan, never compresses it

*Decided for K2b-9 onward* (2026-11-24, under the gate in three rounds: blind solvers and discriminators on Fable,
four roles on round 3's synthesis — Distributed Systems, Verifiability, Operability, Performance — on Opus; it
amends decision 17; § Alternatives Considered keeps what each round refuted):
- **The problem.** A harness stalled resumes and releases the stalled stretch's steps at once (K2b-8d-2); the stand
  meets a burst the plan never had, and its resets under it — answers the system gave, its own however late sent
  (decision 17) — read Fail. Measured since (§ Measured): every judgement of a large answer holds the machine in the
  same way, so the burst is no rarity of stalls. The fold cannot tell after the fact which answers a burst caused (no
  capacity model but the declared limits, no bounded memory of the system, no counterfactual run): the harness must
  not make it.
- **The pace.** For every two hand-offs i, k: `1000·|G_k − G_i| ≥ (1000 − κ)·(|raw_k − raw_i| − J)`, exact integers —
  in hand-off order two running maxima `U = max(1000·G + c·raw)`, `V = max(1000·G − c·raw)`, `c = 1000 − κ`; the
  earliest k may pass `max(raw_k, ⌈(max(V + c·raw_k, U − c·raw_k) − c·J) / 1000⌉)`. The attempts handed in any window
  of width u then have raw dues spanning at most `(u + (1 − κ)·J)/(1 − κ)`: a declared bucket meets at most
  `B + R·u/P`, exactly the curve the validator certified the plan against (`rate.rs`: arrivals each delayed by up to
  J, at `R·(1 − reserve)`), given `κ ≤ reserve`. For capacity no limit declares, the same reading is the profile's
  premise: the stand tolerates its plan compressed by `1/(1 − reserve)`.
- **Raw dues.** A first attempt: its `Released.intended` — never the plan's instant, which would make the system's
  latency a shift on every later step (coordinated omission). A poll: its grid point anchored at the plan's instant,
  `read.planned + k·every`, the grid the validator certified, the points before `intended` skipped — anchored at
  `intended` or `released`, observations caused or read together share one grid for life and the pace lets their
  polls pass bunched at every interval. A resend: its `Dispatch.due` — uncertified (the validator counts none): equal
  raws pass together.
- **κ, the run's catch-up**: a header field, `κ ≤ profile.reserve` (compressing less than certified stays inside the
  certificate), by default half the reserve — the other half the resends' room until K4 paces resends against the
  buckets (a 503 resent into a bucket the catch-up filled meets a 429 and a `Retry-After` past the catch-up's end).
  A run on the real clock with κ = 0 is refused at stage 1 — a shift never drains; `sim` and the virtual clock
  allow it.
- **The gate**, the shell's, after decoration, under the stamps lock, one for the run:
  - **in a band ordered by raw due**: an attempt passes when the pace allows and its raw is within J of the least
    raw among the attempts dispatched, neither handed nor gone, ties by line and attempt — dispatch order would charge
    the zigzag of raws (late-read lines interleaved with dependents) instead of their span;
  - **self-stamped**: each attempt stamps its own `handed` when its own thread passes — a hand-off stamped for it
    by another thread would let a busy source's backlog leave as a burst below the gate; a waiter sleeps to its
    predicted earliest and is woken early only when the band's front advances;
  - **holding, not killing**: an attempt not yet arrived (decorating, its thread busy) holds the front until its
    own deadline — a kill would void healthy runs, whose attempts the replays measured displaced past J;
  - **released at the decision**, not in a drop; waiters race their own deadline; one waker each, registered under
    the lock that checks the front; a head that cannot pass before its deadline leaves at once, `pace.late`;
  - **every carrier through it**, by type: no connect or send without a pass; `sim` paces by the same pure
    `scheduler::pace`, its scripted hand-offs drawn from it;
  - **admission's I2 at the compressed rate**: `⌈needed·1000/(1000 − κ)⌉ ≤ cap(s)`.
- **The fold** (revision 8):
  - coherence: the pace over hand-offs in hand-off order, final by reports — a hand-off is final when every attempt
    dispatched at or before its instant has reported (`Sent`, a closing report, an answer found at the look, which
    now carries its `handed`: its bytes reached the system);
  - a step its plan expects refused at a rate limit, admitted while handed more than J past its raw: undecided — the
    pace bounds compression, not spreading, and a bucket refills over a shift;
  - the report: excursions (maximal spans of hand-offs more than J late: start, end, peak shift), gate deaths by
    code, Roots handed more than J late or as resends, the share past J.
- **Decision 17, amended**: an answer is the system's however late it was sent, because the load offered was the
  certified one — except resends and `Retry-After` polls (K4), dependents of answers stamped together after a
  receive-side stall, and bunching below the gate (§ Owed). The harness's lateness is a shift later steps inherit; a
  censor more than J late is undecided, as today, while it lasts.
- **The journal** (version 9): the header's κ; `decorated` beside `handed` on `Sent` and `Unsent`; an answer found
  with its `handed`; the read line marking a step expected refused at a rate limit. **The machine** (revision 5): the
  poll grid at `read.planned`.
- **Not built with it — decision 14's unplanned 429**: judged by status alone, it lets the system choose void over
  Fail (its services' own 429s; a bucket tighter than declared); the rule needs decision 14's rate monitor —
  undecided only where the journal shows the harness could have caused it within the bucket's horizon. Until then a
  harness-made 429 (a resend wave, coincident dependents, bunching below the gate) reads a Root, which the report
  marks.
- **Cut**: K2b-9a the machine's poll grid; K2b-9b the pure pace, `sim` paced, journal 9, fold 8; K2b-9c the shell's
  gate, its type, I2 and κ at admission; K2b-9d the twins. Each settled, not re-pinned: one commit per cause, the
  previous fold's rules over the new journals equal to the new fold's classes, every changed run explained.

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
- **The watermark by a delivery slack** (K2b's main position): no slack fits between a tokio timer's millisecond and
  J, and every reaction moves by it; **an attempt's timer censoring its deadline**: an answer stamped by its deadline
  but taken after the tick is lost, and a backdated censor is an ignored answer and a backstop fault.
- **tokio's paused clock as the virtual one** (the main position): it ticks in milliseconds; **microseconds as tokio
  milliseconds over its park hook** (the solver): it rests on internals and pins the host's tokio down.
- **An `attempt` line, a per-source lag sampler, the hold's age** (K2b-3's main position, refuted by the solver and
  the discriminator): a shell-only line breaks the bytes `sim` and the shell share and leaves the receive rule no `sim`
  executor; a sampler voids two attempts on one stalled source alike, one answer in the socket and one never sent; the
  hold's age delays only the machine's own work, which `dispatch.due` and `end.wake` show. **A timer at the deadline**
  as the receive measure (the main position's correction): a stall ending a microsecond before D reads the answer just
  past it on time — a false Root — and a stall across D with no answer at all voids it — gameable. **The harness's
  lateness excusing any unknown** (the synthesis, refuted by Verifiability and Distributed Systems): a 500, a reset, a
  partly written body reset 3 s later and its fabricated `Sent`, each voided; **the last attempt's lateness alone**: a
  first attempt late past J that pushed its resend late is lost; **the connect and the write as the harness's**: a stand
  slow to accept voids its own timeouts. **Rules keyed by the step, not its attempts** (the synthesis, refuted by
  Distributed Systems): a poll released past its deadline and a poll's backstop made incoherent. **An observation's own
  late release as its cause's lateness** (version 3, found by a golden of K2b-3a): a "not yet" at the end is
  conclusive however late polling began.
- **A fault for a hold older than J** (the synthesis, refuted by Performance): an OS slice exceeds J; **a spin before
  each timer**: a core for the run, and no push seen while spinning.
- **K2b-8** (decision 19): **a connection kept past its censor to the route timeout** (decision 11; the synthesis,
  refuted by Distributed Systems and Operability): the gateway's slots held where a real client disconnects — Roots
  the harness made; **the censor carrying a never-sent attempt's path** (the main position, refuted by the solver and
  the discriminator): the lag is the source thread's, counted after a hog covering the deadline; **a never-sent Root
  by elimination** (`waited: stand` with no refusal; the synthesis, refuted by Operability): the harness host's own
  network reads as the stand's; **connect errors classed by kind in the carrier** (Operability): a bind's
  `EADDRNOTAVAIL` and a connect's mean different remedies; **zero linger only on abandoned connections** (the main
  position): the stop's abort, a panic, a dropped run would each need a code path; **saturation by a count** (K2b-7c-1,
  refuted by Verifiability): a deadline shared by more than the margin.
- **K2b-7** (decision 18): **a long judgement can only make a run undecidable** (the main position, refuted by the
  discriminator): a sibling's flush waits on the same thread and was charged to the stand; **the thread's busy time
  over the send path** as the harness's part (the synthesis, refuted by Distributed Systems and Verifiability): the
  stand's own waits charged, scaled by utilisation; **a task's queue wait**: an I/O event seen only at the next driver
  turn; **J/4 a refusal before T0** (decision 16; Operability): no false verdict to prevent once the lag is attributed,
  and every cap above some 3 KiB refused at J = 2 ms; **a parser fast enough for 1 MiB in J/4**: some 2 GB/s and an
  allocation per value at 5 ns — a different reader, not a faster one; **an optional `line` on a fault** (the main
  position): a backstop without its line representable; **classes on a stopped journal** (the main position): the
  second pass is unsound on a prefix; **aborting the attempts' tasks alone** (the synthesis): hyper's detached
  connection task flushes what it took; **the stand's text in a fault** (the main position's `what`): a decorator's
  error can carry a proof.
- **K2b-8c** (decision 20): **a full cap stopping the run** (K2b-8c-1): the harness's lateness past the read-ahead —
  a burst's looks on a busy source — turns into `void:fault` what a wait keeps; **the cap lowered at admission** to
  fit the events and descriptors (the solver): an admission that mutates, the egress's setting overridden behind its
  back — kept as constructors the root calls; **`Peaks.pending` plus a constant reserve against the events** (the
  main position): the look's soundness is the cap's, not the prediction's; **the endpoints a function of the
  operation** (the solver): no per-source gateway, no shard per tenant — kept as a special case; **a declared list
  crossed with every source** (the main position): false refusals of pairs no step uses; **the probe as the
  carrier's first gates** (the main position): awaited on the root's runtime, which has no I/O driver, and counted
  against the stand's 64 gates; **waking every waiter on a free** (K2b-8c-1's `notify_waiters`): O(waiters) of
  `place` a free on the source's thread (Operability).
- **Decision 21, rounds 1 and 2** (2026-11-24, the gate on the burst the harness makes catching up — § Owed): **a
  displacement window** (the main position: a step released more than J late voids the Roots around it) — refuted by
  the discriminator: a lone late step's 500 voided is version 3's `call_late` again, and a release's lateness is the
  reader's alone (a step read ahead and woken late shows none), so its verdict hung on L; **an overrun of the plan's
  densest J-window** (the blind solver's count, synthesised: dispatches in a J-bucket past the prefix maximum of the
  plan's own, the steps whose lives meet it excusable) — refuted by three roles: its reference (`intended`) absorbs
  the run's own lateness through dependencies, so a catch-up's dependents count as planned; its trigger has no slack
  at a planned peak and none in a sparse plan, and never sees a plateau under the peak; its horizon follows step
  lifetimes, too long by a 60 s observation and too short for a backlog the members leave; its population missed polls,
  resends and hand-offs; its coherence check was a theorem; **pacing at the hand-off** (round 2's blind solver: at
  most one overdue attempt in flight at once — decision 17's lone late step — the rest shed, the fold verifying it) —
  refuted by measurement (§ Measured, decision 21): on this laptop it would have voided two of three healthy runs at
  500 steps a second and every one at 2,000.
- **Decision 21, round 3** (2026-11-24): its blind solver's **recursion** — `d = max(raw, D)` — refuted by its
  discriminator: late-read lines behind a drained wake take its due and pass at one instant, the burst certified; its
  rounding up cancels the reserve at small gaps; its allowance J leaves `κRJ/P` over the curve. Refuted by the roles:
  **FIFO by dispatch sequence** (it charges the zigzag of raws; after a hold resends and polls dispatch first, the
  followers' shift some 2.8 times the raw order's); **each waiter passing when its own pace allows** (a straggler's
  earliest outruns now for ever once an on-time stream passes — every displaced attempt dies, round 2's failure);
  **hand-offs stamped on behalf of waiters** by the deciding thread (a busy source's backlog leaves as a burst below
  the gate); **killing an attempt whose decoration misses its turn by J** (a healthy run's displaced attempts die —
  void); **the whole reserve for the catch-up** (the resends' room gone); **the poll grid at `intended`** (shared
  grids for observations caused together); **the unplanned 429 by status** (the system choosing void over Fail);
  **the plan's instant as every raw due** (the system's latency a shift on every step).
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
| K2b-0a | K2b | the frontier's properties under drawn interleavings with monotone clocks and ties — drains nondecreasing, no input released below a released bound, an arrival by its deadline keeping its judgement, an attempt with none censored once at its deadline before anything later, `Sent` only before the deadline, nothing at or past a live floor, nothing at T0 unsettled; twins: an answer at D, at D + 1 (the censor, then the late answer), a send at D, a deadline inside a released batch, an early local reply; `sim` through it with every digest unchanged | censor at `<`; `Sent` at `<=`; a floor ignored; `through = now` unsettled; an arrival past D uncensored; a double censor; an answer stamped at its end |
| K2b-0b | K2b | `sim` with wakes taken late over the corpus, deadlines a few answers long: the fold never incoherent, the definition agreeing, both new rules reached; the machine's twin — a resend due at `D − J − 1` taken 1 or 2 µs late ends as it stood, a poll's next taken past the deadline ends unknown then; golden runs — a poll taken 1 µs past its deadline undecided, a resend due at `D − J − 1` taken 2 µs late undecided, one due at `D − J` never scheduled and a root; on the differential, with no wake late, neither rule fires | the wake's re-check removed, or without J; a late poll ended at its deadline; either rule dropped, at its edge, or with J; a late refusal a root; the definition's rules dropped |
| K2b-1 | K2b | under the virtual clock and a scripted carrier, the shell journals what `sim` journals, line for line, on every clean corpus log under the differential's scripts at three read-aheads; a line after an idle gap read at `at − L`; an answer judged for 50 µs holding the machine's own work, the line due meanwhile read after it at the instant taken; judging drawn up to 400 µs on the corpus, no input taken below the last; the clock's probe — a 137 µs sleep woken at 137, an instant settled only after a chain of spawned yields, idle with nothing to wake said; a dropped and a panicking carrier faulted; a carrier refusing the log and a source with no runtime refused before T0; on the real clock across four threads, no push below the frontier, none taken out of order, no stall, every line ended, the fold coherent — and with every read on a carrier's thread slowed by 200 µs; the allowlist, the clock's confinement, every `select!` biased | the settle skipped; drained unsettled; the read-ahead ignored; own work not held by a floor, or lines or wakes alone; the bound at `now` while held; a stamp read outside the lock; a dropped attempt no fault; sleepers woken before settlers; settled at the instant; a real sleep woken early; a `select!` unbiased |
| K2b-2a | K2b | over hyper's connection to a stub on an in-memory pipe, under the virtual clock: `Sent` the instant the stub saw the last byte, writes held back and sixteen bytes at a time, and with a body eight at a time; the answer at its last body frame, not its head; a connection kept alive reused, one closed by `connection: close` not; a close once a byte left, and an answer cut short mid-body, each Unknown at the close; no answer censored at the deadline; an answer past the cap arrived at the cap, its connection closed; the frame as written — the route's `host`, the correlation id | `Sent` at the hand-off, or at the first write; the arrival at the head; no connection pooled; an answer past the cap read; a close after a byte unanswered, or retried; an answer cut short unanswered |
| K2b-2b | K2b | over REST, the shell journals what `sim` journals on every clean corpus log with a REST step, under two seeds of a script an answer can carry (a refusal certified only where its operation lists a code, `Retry-After` in whole seconds on it alone, an unexpected answer only where one can be, each captured value in its source's form), its REST attempts over hyper to a stub inverting the judge and every other action scripted — status and code, which `sim` cannot know, projected out and checked against what the stub sent; expected, unexpected, certified, unknown, resent, captured and waited answers each reached; an answer at its deadline taken | the attempt leaving at the deadline; no code certifying a refusal; what an answer showed dropped; one correlation id a step; no captured value filled |
| K2b-3a | K2b | goldens, each class by hand, folded by Rust and `verdict.py`: a poll's wake taken at its deadline undecided, its unsatisfied end there a root; a first dispatch taken past its deadline undecided; a dispatch J + 1 late then censored undecided, J late a root, and a 500 answered on a dispatch J + 1 late a root; the harness's lateness summed over a refusal and its resend undecided, on one attempt a root; an observation read late then censored undecided, its "not yet" a root; a poll released past its deadline coherent and undecided; a cause late by the harness's release undecided, by the stand's send a root; a twin a coherence rule, each naming it; the corpus and late-wake differentials coherent with the definition agreeing; a send's path from a frontier twin; the shell's hold past the route timeout a fault at exactly its instant, a microsecond short none, a carrier stuck on its thread caught on the real clock; a late hand-off journaled; the connect and the write journaled apart | a censor not required; the hand-off or the machine not the harness's; the last attempt alone; no reader for an observation; the lateness at J; a wake at its deadline late; no wake rule; a cause late by the stand; an observation its own late cause; the definition's three; a censor unmarked; an early reply's path the dispatch's; `handed` not `connected`; a first dispatch's end without its wake; a dispatch's due its dispatch; `handed` at the spawn; `connected` never reported; no stuck timer; the newest hold |
| K2b-3b | K2b | on the real clock over loopback TCP, the source's runtime turning its driver only idle and held by a spinner from before the answer's bytes came to past the look, the bytes seen in the socket by `peek` before the deadline: the answer read at the look past the deadline and journaled late with where it was not — a run whose preconditions failed retried, not counted; under the virtual clock the bracket's bookkeeping — absent from the early look, from the deadline when that look came there, none when the look found nothing, none in time; a source not on a `current_thread` runtime refused; edited goldens — blind J + 1 undecided, blind J, absent at the deadline and no bracket roots, a poll's found before its end — and a twin a coherence rule; the guarantees over a model of the looks with the true arrival the oracle, 20,000 draws, the definition agreeing | the look without its turn; no early look; absent never advanced; a bracket on every answer; any runtime a source; found late at J, or whatever its absence; a live found without its censor; two founds a censor; the definition's two |
| K2b-4 | K2b | K2-5's part over hyper to the stub: decorated once an attempt — a fresh proof each, a correlation id each — every other byte of a resend its first attempt's, the instant it was first sent among them (the resend a second later); each poll's frame its own instant; a decorator's minting time the harness's — `handed` after it; a decorated name the operation does not declare, a value no header takes, a decorating operation and no decorator, each refused; no decorator's proof nor the run's secret in the journal, raw, base64, base64url or hex | a call's frame its own dispatch's; a poll's its first's; an undeclared name given; a value not checked; `handed` before its decoration; a decorating operation without a decorator; the decoration not sent |
| K2b-5 | K2b | K2-4 over hyper to the stub: a 503 whose code its operation does not list, its `Retry-After` notwithstanding, the system's unknown and not resent; `service.session_failed`, listed, resent after its wait and then expected; an Envoy-shaped 504 — a gateway's text, no problem document — unknown, the step awaiting it settled inconclusive in the fold; a call never resent, censored once at its deadline | not taken by status; an unknown resent whatever its safety |
| K2b-6a | K2b | connects refused twice then made at two floors, and refused to the deadline, never sent; a connection idle under the profile's idle less the margin reused, one past it retired, a profile whose idle lies within the margin refused; a request whose connection is gone before a byte of it left taken by the next, the stand seeing it once; calls over the library's TCP egress from a source bound to 127.0.0.2, on a `Live` runtime, each answered, the run ending within two seconds of its last call; the real-clock stress on `Live`'s runtimes | no connect floor; the first connect floored; no idle retirement; an idle within the margin accepted; a request with no byte out judged broken; a source's address not bound |
| K2b-6b | K2b | on the real clock over TCP from a `Live` source: forty calls, a third refused certified and resent, every attempt the journal holds one request the stand saw and none other; a request the stand read and closed on unknown, not resent; an answer in 500 µs before its deadline judged on a thread held 5 ms past it expected; the lateness drill, its state placed past T0's reading — a burst read across a held floor, sends below it, a wake and a line due under it — every line ended, the machine's own work stamped when taken (a stamp below `now` a harness fault); the timed wait measured | lines not held by a floor (each sweep); wakes not held by a floor (two sweeps of three) |
| K2b-7a | K2b | a connect and a decorator that never answer: the attempt unsent, censored at its deadline, the run ended; a pooled connection the stand closed while it sat idle, seen once the next request wakes it: no `Sent` until a byte of the request left — on the next connection, at the instant the stub saw it | the decorator's await not raced; the connect not raced; an empty flush taken for `Sent` |
| K2b-7b | K2b | journal version 6, folded by Rust and `verdict.py` alike, the definition naming the same stop: a send's lag the harness's — a censored call's at J a root, at J + 1 undecided, with a hand-off h at J − h and J − h + 1; an observation's cause's likewise; a lag past its path incoherent; a run's stop the last line — stopped mid-run, once every step ended, after a backstop's fault: void, no classes; a gate's refusal the only line, at 0, under a header with no T0; each breach incoherent by its own rule; every golden run cut after any line (but inside a backstop's pair) and stopped: void, never incoherent; uncut, version 6's classes version 5's (the differential's digest unchanged); every fault class and both sides of each option in the golden, every header variant read back | the lag not added, added after the first send's, past its path accepted; a line after a stop accepted; the second pass after one; a stopped run's reads all its log's; unended steps refused after a stop; a gate after lines, past 0, under a T0; no T0 without a gate; a stop before its lines, naming a line not read, an attempt not out; the backstop a stop; the definition's: a line after the stop, the lag, the reads, the stop unnamed |
| K2b-7c-1 | K2b | a run's fault stops it in its journal — the lines taken, the fault, the trailer — `run` returning the journal and why: a dropped attempt, a stuck hold (virtual and real clock), a carrier's refusal by its dotted name, a log failing mid-run (truncated, diverged), a lock poisoned by a panic under it, a saturated source, a record from a source with no runtime; every harness fault and log error mapped to its class; a decorator's error — the host's text — out of the journal; no request of an aborted attempt reaches the stand, connecting or mid-write, then or after the run returned, none left running; a run's future dropped takes its attempts with it; a journal dropped unfinished has no trailer; a failing writer heard within its lag, its own error | the stop's fault unwritten; the trailer on drop; a dead writer unheard; the stop's abort, the guard's, the wait skipped; saturation unchecked; a class mapped wrong; connection tasks, attempt tasks unkept; a carrier's name dropped |
| K2b-7c-2 | K2b | on the real clock, `Live`'s runtimes seeing whether each park blocked: a wait ending inside a 100 ms hog of the source's thread — the lag at least half the hog; ending after it and an idle stretch — under half; a connection's flush held by the stand's window past the hog's start — its lag the hog's rest, and an early answer's `Sent`, the frontier's at it, carrying it; a poll's lag from the later of its task's last pending and the path's start, at most since its thread blocked; a path connected off its attempt a fault; every task spawned at the one place that times it; on the virtual clock none — every differential unchanged | the thread's busy time; any park ending the idleness; the lag from the task's last pending; the connection's, the attempt's polls unfed; the lag not pushed with its send, at an answer; a request handed back not moved back; connected off its attempt accepted; the park hooks uninstalled; the virtual clock lagging |
| K2b-7d | K2b | admission refuses before any journal — a carrier that cannot carry the log, a runtime not `current_thread`, fewer runtimes than sources, a read-ahead past the header's — and `run` takes only what admission made; the judging time the worst shape's median of five after a warm-up (a scripted stopwatch: not its mean, its maximum, its warm-up, its first shape); each shape's document within its cap, JSON, its last member at its pointer; the stand's gates in turn, each passed named and timed, the first refusal stopping them, one unanswered within its bound refused, the REST carrier's its stand's; a gate's refusal journaled alone under a header with no T0 — void by its fault, its reason nowhere in it; a call from a source past the profile's breaks `operation`, the Python reference agreeing; a frame's time by the clock's wall instant | the source rule dropped (Rust, Python); admission skipping the carrier, the flavour, the count, the read-ahead; the mean, the maximum for the median; the warm-up counted; the first shape kept; a gate unbounded; a refusal passed over; a gate untimed; a refused gate unnamed; the carrier's gates not its stand's; frames stamped by a wall of 0 |
| K2b-8a | K2b | journal version 7, folded by Rust and `verdict.py` alike: a never-sent censor's end decided by its closing report — ready, refused by the peer, on time a Root; never ready, never refused, its lag or a hand-off with it past J undecided, at J a Root; an early reply found late undecided, in its look's microsecond too; no report, two, one at its deadline, a refusal's kind with none counted, connected never ready, a lag past its path, a report with an answer's content — each incoherent, a sent censor's report too; the frontier refusing a report at its deadline and censoring first when the drain has not reached it; `sim` drawing every side of the rule (its counters reached in the differential); the REST carrier's report — the peer's refusals counted, ready only once decorated, where it stood | the report deciding nothing; a Root never ready, unrefused; the report's lag, its hand-off not added; totality dropped; a report at its deadline accepted (fold, frontier); an early reply found late decided by the stand; the bracket's end open; never sent undecided at its end; each path rule dropped; the censor not pushed first; the definition's report, totality, refusal; the shell reporting before its deadline, reporting a sent censor, never marking ready, not noting refusals |
| K2b-8b-1 | K2b | `Connect::of_connect` classing each kind — refused, reset, aborted, timed out, unreachable the stand's; exhausted ports and anything unknown the harness's; `TcpEgress` naming an unknown endpoint, a source address not this host's (TEST-NET-1), a closed port refused; a dropped connection read by its peer as a reset, not an end; the carrier stopping the run on the harness's, its name journaled, and counting only the peer's own refusals of the stand's — a reset and an abort each one, a timeout and an unreachable none; `Io` forwarding no shutdown, the peer reading no end until the drop; a source saturated when an attempt outlives its deadline by the route timeout, stopped there (the stamp within the quiesce's step), five thousand sharing a deadline no saturation | a refusal classed timed out; exhausted ports the connect's; an unknown error the stand's; a foreign source the bind's; no zero linger; the harness's connect retried; a reset uncounted; a timeout counted; a shutdown forwarded; aged past the limit, by the deadline alone, the newest deadline; no wake at the age; a deadline never left |
| K2b-8b-2 | K2b | a request's write held by the stand to its deadline refused by the shell — the stand reading none of it, the attempt never sent, its report `rest.writing`, its connection reset (the stand's end closed); `Io` refusing an armed request's plain and vectored writes once its attempt may send no more, and nothing once its last byte left; a connection ready only at the deadline handed nothing, back to the pool idle since its connect, the next step's; on the real clock: the floor's timer woken late with the thread parked throughout — its lateness the send's lag; a connect pending across the deadline, its race's timer due and dropped — the report's lag; a timer never registered no lag; a decoration's wait held by a sibling no send's lag; the path's timer rule — beyond the bound once, from the window's start, nothing on a connection's task or for a timer due after the poll | a write, a vectored write not refused; refused once sent; bytes at the deadline; handed at the deadline; the unused dropped; a fresh one idle since 0; a completed timer, a dropped one unnoted; an unregistered timer noted; a timer on any task; a re-hand unseen; the bound counted twice; the floor, the connect race on the raw clock; the decoration's lag kept (K2b-8a's survivor) |
| K2b-8c-1 | K2b | a source's sockets under its cap — a connection gone before its socket dying until the socket drops, a waiter enabled before the count woken when a place is freed; the place at the cap — a dying socket awaited and the idle kept, none dying the idle evicted, none idle full; through the carrier at a cap of one: a read's idle connection evicted, reset, for a list to another endpoint; two reads at once, the second the harness's `egress.cap`; at a cap of two, idle on two endpoints and a call to a third, the one idle longest evicted and the other kept for the next list; `TcpEgress`'s ports — a bound source's its own, the unbound sharing the rest, the host's range read | no cap; a slot never freed; a freed place untold (a hang); a connection gone, its socket not dying; evicted though one dies; never evicted; the latest evicted; full, left unsent; the unbound unshared |
| K2b-8c-2 | K2b | at a cap of one, two reads at once: the second woken when the first's connection idles and sent on it, its wait on its send's lag; the first's connection closed at its answer, the second placed late and answered past its deadline — censored, folded undecided, not a Root; a cap of nothing: the read leaving `rest.cap.full`, its wait on its report's lag, one waiting the route timeout later the harness's `egress.cap`; waiters served at the start and the route timeout later — by the pool, and by a freed place — no stop | the wait off the lag, off a report's lag; no wake at a checkin; never served at a place, from the pool; no stuck cap; stuck past the route timeout; the codes swapped; starving from each wait |
| K2b-8c-3 | K2b | admission with the validator's own peaks of five reads at once: at a cap of the bound and events of the cap and the driver's two, admitted — every answer by its deadline, none waiting, five connections; a cap one short refused naming the source, the need and the cap; events one short refused naming the cap and the events; a cap past any events refused; a plan of two sources over a carrier of one refused naming source 1; `Live::for_caps` recording each cap and the driver's own, none past its sources, a cap no driver can take refused; a replayed log's plan its header and its commitment's peaks | no events check; events strictly past; no driver's own, one driver event; no bound check; the bound at the cap refused; needs nothing; an unknown source passed; the first source alone; `Live` without the driver's; a replay's peaks dropped |
| K2b-8c-4 | K2b | admission with assumed descriptors: the cap, ten open and the reserve at the soft limit admitted, one below refused naming the need; two sources' caps summed; the process's unknown refused; in a test binary of its own, the process's: read as `/proc/self/fd` lists them, `Live`'s read once its runtimes exist, raised to the hard limit and marked; a soft limit one below admission's bound refused, at it a run filling its source's cap of twenty — every request out, none answered — with no `EMFILE` | no reserve; the open not counted; unknown passed; at the soft refused; the last cap alone; read before the runtimes; a raise unmarked; the listing's own counted |
| K2b-8c-5 | K2b | `TcpEgress` admitting a bound source of its own and reaching its endpoint; refusing more sources than listed and one past the list, two sources bound to one address, an address not the host's, an endpoint with no address, a v4 source to a v6 endpoint, an unbound source routed from a bound one's address — the stand's listener accepting nothing; admission refusing an endpoint the stand declares and the egress does not reach, and a source not the host's, admitting it reached; a route outside its source's declared endpoints the harness's `rest.endpoint` | sources past the list; one address twice; no local bind; an unknown endpoint reached; any family; a shared route passed; the egress not admitted; no endpoint reached; a route outside passed |
| K2b-8c-6 | K2b | two sources bound to their own addresses probed from a root runtime with no I/O driver: one gate `egress.probe`, the stand accepting two connections, each read as reset with no byte; a closed port refused as `egress.probe.s0.e0.refused`; a connect never answered refused as `…unanswered` within its bound, its future dropped; the harness's failure named by its own, `…egress_ports`; in the descriptors' own binary, stage 3's re-read within at admission's bound and refused once the reserve and one more were opened since; the lint counting every method spawn of a task, its receiver whatever | probed on the root; failures ignored; the harness's name kept dotted; a stream kept; no re-read |
| K2b-8c-7a | K2b | on the real clock, the chain from admission to the run: the journal's header holding the gates that ran — `egress.probe`, `egress.descriptors` — the judging, the namespace the gates certified, T0 no earlier than the third stage's end; the stand reading the run's frame under that namespace; a probe's refusal journaled by the library — no T0, no gate passed, its fault the only line; `run` refusing an `Admitted` and a `Ready` built by hand (`compile_fail` doctests, their error codes pinned); every assumption's constructor held to `testing` (the lint, five counted); a carrier built from a header not the plan's refused | no probe in the chain; no re-read in the chain; no T0 in the header; another namespace run; the frames' namespace the config's; a header of its own carried; a plan assumed in a host's build |
| K2b-8c-7b | K2b | journal version 8 — the golden holding admission's evidence, every field at its bounds, the schema its `admission`, `source_fit`, `descriptors_read`; the differential's journals re-pinned (8, 4), their classes version 7's; the chain's header holding the source's need, cap and events, the descriptors read, the plan assumed; a barred run's too; a replayed plan on `Live` not assumed, on a test's runtimes assumed; five thousand closing reports counted by their code in `Ran` | no evidence journaled; no source's fit; no descriptors read; the plan's assumption alone; the runtimes' alone; reports uncounted |
| K2b-8d-1 | K2b | on the real clock, a source of four sockets: `Live::for_caps`' runtime reading every answer in the host at the look, three answers, the driver's own wake and the target's ready across a spin past the deadline; a runtime taking four events a turn, claiming six, missing the fifth in some run — a false Root I1 forbids; no socket the shell closes in `TIME_WAIT` on its source's address — closed by the stand's `connection: close`, pooled and closed at the run's end, censored and reset; measured on purpose (`ports.rs`, ignored: two minutes), a bound source's ports out at its bind and an unbound one's at its connect, each `egress.ports` | no zero linger; `Live` building fewer events than it records |
| K2b-8d-2 | K2b | a whole process stopped 1.5 s by `SIGSTOP` mid-run beside a stand of its own process answering at once and refusing connects 40 ms in every 100, two thousand reads 2 ms apart, each on a connection of its own: no censor — sent, or never sent — a Root, in every run (some five hundred never-sent reports a run, every one undecided); the stand's own resets and the system's answers in the machine's catch-up burst counted, not asserted (decision 21 owed) | — (the timer rule's mutants survive it: § Measured) |
| K2b-9a | K2b | the machine's poll grid at the plan's instant: an observation read late polled on its planned phase, two caused together on their own grids, nothing dispatched before `intended`; the differential with `schedule.py` moved alike, every changed run one whose observation was read or caused late | the grid at `intended`; at `released` |
| K2b-9b | K2b | `scheduler::pace` against brute force over every pair (κ ∈ {0, 1, 100, 500, 999}, J ∈ {0, 1, 250, 2000}, ties and inversions); a hand-off late by at most `(1 − κ)·J` past its earliest moving neither maximum; `sim` paced and `verdict.py` recomputing its gate exactly and checking every pair on every differential run; a golden at κ > 0, J > 0 coherent under the pace and void under the allowance J; a refusal admitted under a shift undecided; excursions recomputed by `verdict.py` | the allowance J; the clamp `max(raw, D)`; rounding up; finality by time; the found answer's `handed` ignored |
| K2b-9c | K2b | shell == sim with a reader lag, gate-delayed hand-offs and `pace.late` more than none; a decoration timing out ahead of a second attempt that passes; two sources' inverted raws passed in raw order; hand-offs within the band in raw order as a journal property; κ = 0 refused on the real clock; I2 at the compressed rate | the band dropped; dispatch order; the head kept past its decision; a carrier sending ungated |
| K2b-9d | K2b | the stall twin beside a stand that resets past the certified bucket, its random closures gone: no Root gated, Roots ungated (the control); the gate recomputed from the journal predicting each hand-off and `pace.late`; the healthy and large-answer replays rerun with the gate built, optimised | the gate off; FIFO by dispatch |
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

**K2a-5a — polls re-polled, resends spaced** (2026-11-22): the machine, `MACHINE_REVISION` 2 (decision 15). A poll's
unknown answer takes its grid's next point as a not-yet one does, no sooner than its `Retry-After`: one gateway reply
no longer ends an observation, and never two polls of one are out. A harness resend — a certified refusal of an
operation that may be resent, an unknown answer of an idempotent one — leaves at the later of its `Retry-After` and
the profile's floor after the answer, and only if that is before the deadline by more than J; else the step ends as it
stood, a refusal unexpected, an unknown unknown. `Input::Ended` carries what came back beside the judgement (`Seen`:
the status, the code as the journal may carry it, the wait), which the judge gives; the journal's version 2 does not
write it yet (K2a-5b). `vectors/schedule.py` decides the same by definition. `verify/tla/Scheduler.tla`: a resend
falls due at `now + Max({w, Floor})`, none out until then, kept before the deadline by more than `Jitter`
(`ResendInTime`); trace validation carries each answer's wait, the floor and J.
- **Executors**: a machine test of the wait — the floor, a shorter and a longer `Retry-After`, J one microsecond
  either side of its edge, a wait past any deadline, a poll's grid against its `Retry-After`. The differential's
  scripts draw a `Retry-After` none, below the floor, above it and past any deadline; the corpus's update is resent
  when a code certifies it not run, its read when unknown; `schedule.py` counts, failing on zero: 3,732 runs agree,
  198 resends by `Retry-After`, 519 by the floor, 3,909 refused near their deadline, 15 polls re-polled. Journals
  pinned under (2, 2), classes under (2, 2, 1); the golden journal and runs written again. TLC: `SchedulerResendPast
  Deadline` drops the guard and violates `ResendInTime`, its twin holding; a guarded resend reached (an invariant
  that none is ever due falls in 43 states); the window's state count 556,786 to 276,405 and the late reader's
  1,665,393 to 821,976, a resend no longer instant; the scheduler's traces recorded again fit the model, the forged
  ones still refused. Nine mutants of the machine, each killed by the differential alone, three also by the traces
  and all nine by the machine test: a poll's unknown ending it, a poll's `Retry-After` ignored, a resend at once,
  `Retry-After` ignored, the floor ignored, J ignored, a resend past the deadline, a refusal past it ended unknown,
  an unknown resent unless the operation is never resent.

**K2a-5b — journal version 3, resends licensed** (2026-11-22): the journal's header names the resend floor, the
run's namespace (public, every secret drawn from the run's own) and T0's wall instant; a read, its action's safety,
and an observation that polls is a `poll`; an answer, its status, its code as the journal may carry it (a short
dotted name, or `other`) and the wait it asked for. The fold, `FOLD_REVISION` 2: an attempt past the first is
licensed by the answer it follows, or the journal is incoherent — a poll's next after an answer not expected, no
sooner than its wait; a call's resend after a certified refusal of an operation that may be resent or an unknown
answer of an idempotent one, at the later of its wait and the floor, more than J before its deadline. A poll's last
poll that never left is the harness's lateness, as a call's is — undecided, where revision 1 counted a root (the
Distributed Systems role's M3) — and a poll is an observation for its cause's rules. `vectors/verdict.py` decides the
same; the schema and `vectors/journal.py` read version 3.
- **Executors**: the golden journal holds every new variant — a poll, each safety, a code named and other, a status
  and a wait on both sides, a namespace at the top, a T0 before the epoch; a golden run of a poll whose last poll
  never left, its class written by hand (undecided); three twins of broken evidence, each void and the definition
  agreeing — a resend before its floor, one too near its deadline, an unknown resent and a certified refusal resent
  of a call never resent (the latter on a golden run of its own, a refusal resent and then expected), a poll before
  the wait its answer asked for. The differential checks each read's action and safety by its own lookup in the log's
  tables, and each answer's status, code and wait against what the machine took; its 3,732 verdicts agree with the
  definition; a test journals an answer's status, code and wait as the machine took them, `sim` giving none. Journals
  pinned under (3, 2), classes under (3, 2, 2). Eleven mutants, each killed: the license dropped, its floor, its J,
  its safety, a poll's wait; a poll's lateness not the harness's; a poll no observation; a poll journaled an
  observation; no safety, no wait, no code journaled — three alive until their executors were added (the refusal's
  golden run, the poll's twin, the journaling test).

**K2b-0a — the frontier** (2026-11-23): `scheduler::frontier`, pure, no clock (decision 16): attempts opened with
their deadlines, indexed by them; a send pushed only before its deadline; an answer held at a floor while judged,
censoring its attempt at the deadline first if it came past it, and pushed with `Sent` before it if none was; a drain
releasing through the least of `now − 1` (`now` settled) and the oldest floor less one, after censoring every attempt
with no answer whose deadline lies within it; an attempt opened within what was released censored at once; a push below
what was released refused. `sim` drives it: its handlers put what their attempts see on a timeline, applied to the
frontier as each instant comes, every instant settled, and model no answer past its deadline — the frontier censors
there.
- **What it found**: the rule the roles proposed — an input taken below the last taken key a fault — is wrong: an
  input caused at a settled instant sorts below one already taken there, as `sim` takes it (a `Sent` of line 9 caused
  by line 10's end, both at one instant). Only stamps are bound across drains; decision 16 says so.
- **Executors**: the property over 4,000 drawn interleavings of dispatches, sends, answers, judgements and drains on a
  clock with ties — stamps never back across drains and in key order within one, nothing at or past a live floor, an
  answer by its deadline and before its release kept, one past it or after it censored once at the deadline and then
  taken late, an attempt with none censored once, a send only before its deadline or at an early reply — its seed of
  the same-instant case kept; twins: an answer at its deadline held past it, one past it after its censor, a send at
  the deadline, a deadline already released, an early reply, nothing at T0 unsettled, a push below refused. `sim`
  through it leaves every digest as it was: the differential's journals and classes, the stand, the traces, the golden
  runs, the heap. Nine mutants, each killed: a censor at `<` (it first hung `sim` at a deadline it never censored —
  `sim` now stops a run turning more than four times its input cap), `Sent` at `<=`, a floor ignored, the unsettled
  bound at `now`, an arrival past its deadline uncensored, a censored attempt kept open, an early reply unsent, no censor
  at once, a push below accepted.

**K2b-0b — a wake taken late** (2026-11-23): the machine re-checks a resend or a poll at the instant its wake is taken
(`MACHINE_REVISION` 3); the fold makes undecided what a late wake ended (`FOLD_REVISION` 3); `sim::run_waking_late`
takes a wake due at `w` at `w + lag(w)`.
- **What it found**: decision 16's first rule, undecided "more than J after the resend was due", contradicted its
  executor's own case — a resend due at `D − J − 1` taken 2 µs late ends within J of being due, a root by that rule,
  though the harness alone kept it from leaving. Undecided whatever the lateness is exact: the machine schedules a
  resend only if due more than J before the deadline, and taken on time it leaves; a step ends past its deadline only
  by a wake taken past it. The poll's rule first survived its mutant: on the corpus every poll ended late by a wake had
  also sent its first poll more than J after its intended instant, so the cause rule covered it; the golden run of a
  poll sent on time and taken 1 µs past its deadline separates them.
- **Executors**: 434 runs of the corpus with deadlines of 2,000 µs, answers within 300 µs and a quarter each refused
  and unknown, under wake lags drawn from {0, 1, J, J + 1, 3J + 7, 1,000, 1,700}: never incoherent, the definition
  agreeing, the resend rule firing 64 times and the poll's 27; on the differential, with no wake late, neither fires;
  the machine's twin; golden runs `poll_taken_late`, `resend_taken_late`, `resend_too_late_to_schedule`. Journals
  pinned under (3, 3); classes under (3, 3, 3) equal to (3, 2, 2)'s, as no wake is late there. Thirteen mutants, each
  killed: the re-check of a resend removed, or without J; that of a poll removed; a late poll ended at its deadline;
  the poll's rule dropped, at the deadline, or past `D + J`; the resend's rule dropped, at `<=`, or without J; a late
  refusal a root; the definition's two rules dropped.

**K2b-1 — the shell's loop** (2026-11-23): the `load-run` crate — its dependencies the command log, the scheduler and
tokio, an allowlist; the clocks its lint refuses read by the `clock` module alone. `RealClock`: the monotonic clock
from T0, its wall instant bracketed within J; its sleeps woken by a timer thread of its own, parked to each sleeper's
microsecond. `VirtualClock` (feature `testing`): a counter of its own and its sleepers, moved only on tokio's paused
auto-advance, which tokio takes only when its scheduler parks with no task ready — a yield or a maintenance poll parks
with a zero limit and moves nothing (tokio 1.53.1, `runtime/time/mod.rs`). `stamps`: the frontier under one lock,
every report of an attempt a clock read and a push under it; an attempt's handle a drop guard. `run`: `sim`'s loop
with time read from the clock; the ports `LineSource`, `JournalSink`, `Carrier`, each with an in-task adapter and a
thread adapter (the log through its commitment; the journal through its writer). `Frontier::next_pushed`, so that the
loop waits rather than turns while an answer is judged.
- **What it found**: the loop first took its own lines and wakes while an answer was judged; once released, the
  answer's stamp lay below the machine's clock, and `Machine::step` absorbs that silently (`now.max(at)`). Only
  judging that takes time shows it, which neither `sim` nor the first carrier had: the twin and the corpus run with
  judging drawn reveal it, and the loop now refuses an input below the machine's clock, a harness fault. A stamp read
  outside the frontier's lock survived the stress until every read on a carrier's thread was slowed by 200 µs: read
  under the lock that only slows the run, read before it a stamp goes stale. A mutant made the loop sleep to an instant
  passed, and the run hung rather than failed: such a sleep is a stall now.
- **Executors**: 3,732 runs, 137,157 journal lines equal to `sim`'s; the twins and the probe of the executor row; on
  the real clock, eight seeds of 400 calls over four runtime threads and three of 200 with the slowed reads, each
  coherent to the fold. Thirteen mutants: eleven killed — the settle skipped, drained unsettled, the read-ahead
  ignored, own work not held by a floor, the bound at `now` while held, a stamp read outside the lock, a dropped
  attempt no fault, sleepers woken before settlers, settled at the instant, a real sleep woken early, a `select!`
  unbiased; two survive, equivalent under the virtual clock — lines alone, or wakes alone, not held by a floor: there
  the machine is never late, so no floor stands below work due; live, a busy machine would stamp its late work at the
  floor less one, understating its lateness without breaking the order. Their executor is a lateness drill on the real
  clock, K2b-6's (§ Owed).

**K2b-2a — REST over hyper's connection** (2026-11-23): `stand` (the host's `Stand` — a route per step, its `host` and
the endpoint its connections are pooled by — and `Egress`, a connection from a source); `io`, the connection's bytes
for hyper by copy (`ReadBufCursor::put_slice`; the crate forbids `unsafe`), counted, `Sent` at the first flush after
the hand-off; `rest`, the carrier: the frame from its fills (a call's at its first dispatch, so a resend's is the same
bytes; a poll's at its own), the route, the correlation id, a connection from the source's pool (idle by endpoint, the
latest first, one its peer closed dropped) or a new one whose task runs on the source's runtime, the request handed
over; the answer read to its last frame or to the cap, judged under the floor. hyper 1.11.1 (`client`, `http1`),
`http`, `http-body`, `bytes` join the allowlist; the command log's `fills` re-exports the faker's `Namespace`, so the
shell names no faker. A log whose header names channels or host calls is refused before T0.
- **What it found**: the guards decision 16 put on `Sent` — the body's end, a byte written — never change it. hyper
  encodes the head and a one-frame body before its first flush, and flushes the socket only with its buffer empty
  (`proto/h1/io.rs:271-303`, `proto/h1/dispatch.rs`), and the attempt arms the probe and hands the request over in one
  poll on the connection's own runtime: the first successful flush after the hand-off is the last byte. Their mutants
  survived; the guards are gone, and the twins, which compare `Sent` with the instant the stub saw the last byte under
  held-back writes, hold hyper to the order. The first Unknown twin broke the connection before the answer's head and
  so left the branch of a body cut short untested: its mutant survived until the cut-short twin.
- **Executors**: seven twins (the executor row). Eight mutants, each killed: `Sent` at the hand-off, or at the first
  write; the arrival at the head; no connection pooled; an answer past the cap read; a close once a byte left
  unanswered, or retried as if none had; an answer cut short unanswered. Not executed here: a request handed back
  unsent, or with no byte accepted, taken by the next connection — only a reuse racing the peer's close makes one, K2-3's
  twin in K2b-6 (§ Owed).

**K2b-2b — the shell's journal over REST** (2026-11-23): the REST differential: 390 runs, two seeds of every clean
corpus log with a REST step, equal line for line to `sim`'s; answers expected 551, unexpected 104, certified 42, unknown
239; 110 resent, 91 captured, 29 waited.
- **What it found**: no corpus log is REST alone — every one onboards its Tenant by host calls, and most carry bus
  steps — so the differential composes carriers by action kind, REST over hyper and every other action scripted, as
  the shell will in K3. The judge takes `Retry-After` from a certified refusal alone (decision 15), and a call that
  expects nothing takes any answer below 500 as expected: a script of `sim`'s draws what an answer cannot carry, so the
  differential's draws only what one can. The attempt gave up at its deadline, and an answer arriving at that instant
  races its timer: it now leaves only past it, the frontier censoring at the deadline what had no answer. `sim` is
  left as it is — the plan's `sim::Attempt.seen` became a projection: status and code out of both journals, checked
  against the stub's.
- **Not reached**: a request whose last byte leaves exactly at its deadline answered in the same microsecond — `sim`
  gives it no answer, the frontier takes it as an early reply; no run met it, and no wire answers within the
  microsecond its request ended.
- **Executors**: the executor row. Five mutants, each killed: the attempt leaving at the deadline; no code certifying
  a refusal; what an answer showed dropped; one correlation id a step; no captured value filled.

**K2b-3a — the harness's lateness, attributed** (2026-11-23): journal version 4, `MACHINE_REVISION` 4,
`FOLD_REVISION` 4 (decision 17). The machine's `Output::Dispatch` carries its wake's due instant and `Entry` the wake
that ended it, an explicit parameter of `finish`; the frontier's open entry the attempt's path, on whichever `Sent` it
pushes, and its censor a mark; `journal::events` writes them on the lines they belong to; the fold reads the harness's
part and the censor; `verdict.py` says the same in its own sentences. The shell stamps `handed` around every carrier
and `connected` at the REST hand-over; a hold past the route timeout is `Harness::Held`.
- **What it found**: version 3's own rules voided the system's answers — `call_late` fired on every unknown, a 500
  answered on a late send included (Verifiability; Distributed Systems found a partly written body reset seconds later,
  its fabricated `Sent` stamped at the reset, voided the same way): the harness's lateness may excuse only a censor or
  a send that never left. A golden of a cause made late by its send — `sent_after` past J, the harness's in version 3 —
  is the stand's under decision 17, and became a root; the cause made late by its release wake is the harness's, and
  undecided: both are goldens now. A self-caused observation counted its own late release as its cause's lateness and
  voided a conclusive "not yet". A step released at its read is dispatched in that step, with no wake to take late, so
  every late-dispatch golden plans its step ahead of its read (the Verifiability role's note). Rules keyed by the step
  made a poll released past its deadline, and a poll's backstop, incoherent (Distributed Systems): keyed by its
  attempts now, a fault's end exempt. A poll's due before its deadline is implied by its dispatch: that condition went.
  The hand-off's place in the harness's sum survived its mutant until an edited golden moved one hand-off: `sim` and
  the virtual clock hand every attempt off at its dispatch.
- **Executors**: the executor row's goldens and twins; the corpus differential (3,732 runs) and the late-wake one (434)
  coherent under every new rule, the definition agreeing — classes under (4, 4, 4) `913215b9…`, journals under (4, 4)
  `1a26d118…`, the golden journal `0110468d…`. Twenty-two mutants, each killed (the executor row's list).

**K2b-3b — the receive side** (2026-11-23): journal version 5, `FOLD_REVISION` 5, `MACHINE_REVISION` 4 kept (the
machine passes the bracket through, deciding nothing by it). `Seen.found` and `answer.found {absent, deadline}`; the
attempt keeps `absent` — its hand-off, its request's hand-over, the clock read before a look that found the answer
incomplete — and its end carries the bracket whenever its hold was late; `turn`, a yield polled under a flag waker that
only tokio's defer list fires; the REST attempt's looks at `D − J/2` and `D + 1`, in its head's wait and its body's, a
body's look repeating its turn while frames come; sources refused before T0 off a `current_thread` runtime; the
`Egress::Io` contract on the port. The fold keeps, rather than a bit, each censored step's attempt, deadline and
dispatch until its late answer is found — what a `found` must agree with — and `verdict.py` says the same.
- **What it found**: the virtual runtime is never busy — an attempt whose look is put off reads its answer as it comes
  — so a lagging clock executes the bracket's bookkeeping and nothing more; the turn's own executor is the real-clock
  TCP twin, and the flag's needed a wake of the attempt between its yield and the driver's turn, which no state made
  until a test clock, at the look's timer, woke a sibling queued behind the yielding attempt. A live found's once-check
  survived its mutant: the twin of two founds had both after the end.
- **Executors**: the executor row; journals under (5, 4) `3f95c013…`, classes under (5, 4, 5) equal to (4, 4, 4)'s — no
  differential reaches a late answer — and the golden journal `4ffb293d…`. Twelve mutants, each killed (the row's).

**K2b-4 — frames, correlation, decorators** (2026-11-23): `decorate`, the host's `Decorator` port — once an attempt,
seeing the step, the attempt, the caller, the operation, the route, the final frame, the correlation id, the deadline
and the wall time; its headers only the operation's `decorated` names, each a header's value, or a harness fault; an
`Undecorated` default that declares it decorates nothing, so an operation that decorates is refused before T0 without
one. `handed` is stamped again once the attempt is decorated: the decorator's time is the harness's.
- **What it found**: the resend twin first came 110 µs after its first attempt, inside the millisecond the `Sent` hole
  renders, and a call's frame rebuilt at its own dispatch passed it; a second's `Retry-After` separates them. A frame
  cache is not needed for a resend's bytes — its fills hold its first dispatch — and is left out.
- **Executors**: the executor row. Seven mutants, each killed (the row's); "one correlation id a step" stays killed by
  the REST differential (K2b-2b).

**K2b-5 — resends end to end** (2026-11-23): no new mechanism — K2a's judge and machine, wired by K2b-2 — executed
over REST by K2-4's shapes of the gateway's answers. A censored call is never resent by construction: its censor is
at its deadline, where no resend fits more than J before it, so the twin of a call never resent holds whatever its
safety, and the safety's own executors are the certified and uncertified refusals. Two mutants, each killed.

**K2b-6a — connections and the live composition** (2026-11-23): `Config`'s connect floor (10 ms by default, the first
connect at once) and idle margin (1 s); the pool retiring an idle connection before the profile's idle less the
margin; `tcp::TcpEgress`, endpoints by name, a source bound to its own address, `TCP_NODELAY`; `live::Live`, a
`current_thread` runtime per source on a thread of its own, its driver taking a given number of events a turn;
`Machine::next_wake`.
- **What it found**: two defects of the loop since K2b-1, hidden by the virtual clock. The loop kept its own copies of
  the machine's wakes; a step's end cancels its backstop in the machine, not the copy, so a live run waited to its
  last call's deadline and grace — eleven seconds for three calls of three milliseconds. And it read what the
  frontier holds and how many attempts live under two locks: on the real clock an attempt on its source's thread
  pushed its end and went between them, and the run ended Ok with that answer unread — measured 2 of 30 runs before,
  none of 60 after. The first is executed by the run's duration on the TCP twin; the second by that twin's repetition,
  never deterministically — a twin that holds an attempt between its push and its going is owed.
- **Executors**: the executor row. Six mutants, each killed. Not executed: a driver turn taking fewer events than the
  sockets ready (more than 1,024 on one source) — K2b-8, with the ports' cap.

**K2b-6b — the real clock's evidence** (2026-11-23): K2-3's twins over TCP on `Live`; the loop's own work checked at
the instant taken (`Harness::Backdated`); the lateness drill; the timed wait measured.
- **The timed wait** (this laptop, load 0.7, 20,000 sleeps of 0–2 ms on the real clock's own timer thread, twice):
  late by p50 217 and 221 µs, p99 381 and 374, p99.9 1,845 and 504, the worst 4,233 and 8,529 — p99 well within J
  (2 ms), the median five times a tokio timer's millisecond, the worst an OS slice; four times the timer's slack, the
  rest the hand-off from the timer thread to the machine's runtime. Its lateness is the harness's and journaled
  (decision 17); a hybrid — parked to a few hundred µs before, then spun — is owed, on its trigger: the first host
  run's dispatch lateness against J.
- **What it found**: the drill's first constructions never reached their state, and a probe in the loop showed why —
  its instants lay near T0, and reading the burst's own lines took the machine past all of them; placed after T0's
  reading, with the burst's reading itself the busy window, the state is reached each run. The two mutants K2b-1 left
  alive are killed by it: lines alone not held in each of three sweeps, wakes alone in two — a real clock's state,
  reached by timing, the run-time check making it observable.

**K2b-7a — the defects K2b-7's gate found** (2026-11-23):
- **The judging time, measured for the gate** (this laptop, release; the ignored family test
  `the_judge_s_time_at_the_cap`): read, judged and dropped, a capture at the last member, the median of five after a
  warm-up, then the parse alone —

  | cap | flat object | small objects | array of numbers | escaped strings |
  |---|---|---|---|---|
  | 4 KiB | 524 µs | 530 | 673 | 68 |
  | 16 KiB | 1,611 | 1,231 | 2,481 | 275 |
  | 64 KiB | 6,123 | 4,597 | 9,948 | 1,080 |
  | 256 KiB | 24,157 | 19,285 | 40,236 | 4,425 |
  | 1 MiB | 120,125 | 73,538 | 189,678 | 17,473 |

  The parse is nearly all of it; an array of numbers is the worst shape at 150–180 ns a byte, some 330 ns a number
  (`Number` allocates its text, a formatted copy and its digits); not linear in the size. A debug build is about four
  times slower (309 ms for the small objects at 1 MiB); without the duplicate check the small objects' parse halves
  (28 ms at 1 MiB) — the numbers' does not move (an array has no members). Decision 18 makes it evidence.
- **What the twins found.** The idle-close twin journaled `Sent` at 10,000 µs, the empty flush's instant, where the
  stub saw the request at 10,100 on the second connection — red before the guard, green after. Without the race, the
  never-answering connect and decorator left the virtual clock idle with the run unfinished (`within`'s panic).
- **Owed by it**: an attempt that drops unended now raises its fault and leaves the count in one critical section, and
  the loop reads the fault in its one snapshot — a structural fix with no executor: on one thread nothing interleaves
  there, and the clock is read under the lock, so a gate in the clock deadlocks rather than reaching the window. A
  model of `Stamps` (loom) — an attempt's drop against the loop's snapshot, every interleaving, both mutants (two
  critical sections; the snapshot's fault unread) failing it — is K2b-7c's, which rebuilds that code for the stop.

**K2b-7b — journal version 6 in the pure fold** (2026-11-23): `sent.lag`, the fault classes with their fields, the
header's evidence (`t0` optional, the gates passed, the judging time) — written by `sim` as 0, none and empty until
K2b-7c and K2b-7d produce them; the fold's stop; `verdict.py` written from decision 18.
- The differential's classes under (6, 4, 6) are version 5's digest exactly: no clean run stops or lags, and none
  folds otherwise. Its journals' digest moved with the bytes.
- The sweep left one mutant alive — a gate's refusal accepted after a run's lines — because the twin put the gate at
  the lines' latest instant, which the at-0 rule refused alone; a twin of lines all at 0 then a gate at 0 isolates it.

**K2b-7c-1 — the shell's stop** (2026-11-23): `run` returns `Ran { end: Ended | Stopped(Stop), running }` with the
journal finished; `RunError` keeps what leaves no journal to fold. Every task the shell spawns — an attempt's, a
connection's, through `Attempt::spawn` — is kept, reaped as the list grows, aborted by a stop and by a guard when the
run goes; a stop waits for them at most 1 s. A carrier's fault is a dotted name and a detail for the caller only.
- **K2b-7a's owed model, by construction instead**: the count of attempt handles is private to the frontier's state,
  read only through `alive()`, which gives the fault first, and left only through `leave(fault)`, one critical
  section — the two mutants K2b-7a left alive (the fault raised after leaving the count; the count read without the
  fault) no longer compile. A model checker would prove what the types now state.
- **What the twins found**: the log twin's first assertion — line 3 read before the stop — was wrong, not the code:
  the loop reads the next record as it takes a line, so the log failing on the read after line 3 stops the run with
  line 3 queued, never taken, at its read instant. A stop's instant is read after its wait for the aborted tasks, so
  with tasks in flight it is later than the fault's cause.
- **Still K2b-8's**: zero linger on an aborted connection — bytes the kernel accepted before the abort may still reach
  the stand; the stopped journal says a dispatched, unended attempt may have.

**K2b-7c-2 — the source thread's lag** (2026-11-23): every task on a source runtime is timed at each poll, its send
path's lag journaled on `sent`; `Live` installs the park hooks.
- **The park detector, measured first** (a probe outside the repository, this laptop, tokio 1.53.1): of 6,000 parks
  that blocked — a message every 500 µs from another thread — every one switched its thread out of its own accord; of
  120,006 that returned at once — a byte in the socket before the park, nothing runnable — none did, idle and beside
  eight CPU hogs. Beside the hogs 0.1–0.15 % of those returned at once lasted 1 ms or more, to 11 ms, preempted: a
  length threshold would take them for idleness and under-count — the false Root's direction. And a park after
  deferred tasks is tokio's `park_yield`, which runs no hook: a yield's driver turn never ends an idleness (an
  over-count, never an under-count).
- **What the twins found**: the connection twin's first gate held all but a request's first 16 bytes, so the stub
  never saw a head and never answered — the early case read as a censor; gated at the head's end, the request given a
  body, it reaches both cases. The hog's decorator spun for every attempt at first: the carrier decorates every one.
- **Hand-back** is executed by construction: a path is handed to a connection only from its attempt — `connected`
  otherwise a carrier fault — so a request handed back and not moved back fails the idle-close twins.

**K2b-7d — the stages before T0** (2026-11-23): `admit` makes the `Admitted` run `run` takes — a refusal there
leaves no journal, as no sink exists; `stages::measure` times the judging at the cap on a source's runtime by a
stopwatch; `stages::gates` asks the stand's gates in turn (`Stand::gate`, one at a time, so that each is timed and
bounded); `stages::journal_header` builds the header with the stages' evidence; `stages::refused` journals a gate's
refusal. The carrier's `Config` has no wall instant: a frame's time is the clock's, read at the attempt.
- **The order of the stages is the composition root's**: `run` cannot begin unadmitted, but nothing in the types
  forces the measurement and the gates before it; the journal shows what ran — no judging, no gates — so a skipped
  stage is a reader's to see. A typestate through the journal's opening is owed with the composition root, the first
  host run's.
- **The validator's rules, revision 9**: a call from a source past the profile's breaks `operation` (it held only
  under a source-keyed rate limit); the corpus's meanings re-pinned, the Python reference taught the rule.
- **What the sweep found**: no twin read a frame's time against the clock's wall — the one twin reading the header
  only checked it was there; pinned to the wall's milliseconds now. And the differential's journals moved with the
  validator's revision, their headers naming the live validator — a test's input, not the machine's: they name a
  test's validator now, and the pin moved once.

**K2b-8a — a never-sent censor's closing report in the pure fold** (2026-11-23): `Seen.unsent`, `Frontier::left`,
`Frontier::sent` telling whether it pushed (its dead `close` deleted); `sim` reporting a never-sent attempt at `D + 1`
by a pure draw `sim::drawn_unsent` the shell's scripted carrier shares; the fold holding a never-sent censor pending in
its censors' map and deciding it by its report; the TLA trace skipping a report (the model takes no input of an ended
step); the REST carrier's basic report — every exit with no answer through one `leave`, no earlier than `D + 1`.
- The differential's classes under (7, 4, 7) are version 6's digest, yet it reaches every branch of the rule — five
  Roots among them: the never-sent censors it makes are undecided by another rule as well (a step read past its
  deadline is never sent). So the rule's own executor is the hand-written goldens, not the differential.
- One mutant lives: the decoration's lag kept past `decorated` (decision 19's reset) — the virtual clock's lag is 0;
  its executor is K2b-8b's real-clock twin, a decorator awaiting on a thread a sibling holds.

**K2b-8b-1 — a connect's failure classed; every socket reset; saturation by age** (2026-11-23): `Egress::connect` gives
`Result<Io, Connect>`; `TcpEgress` sets zero linger on every socket before its bind and classes each step's failure —
the socket's (descriptors by `EMFILE`/`ENFILE`, which have no kind of their own), the bind's (a source address not
this host's `egress.local`, a bound socket's ports `egress.ports`), the connect's (`Connect::of_connect`); hyper's
handshake failing the harness's (`rest.handshake`: it does no I/O). `SATURATION` is gone: the stamps count the alive
handles' deadlines, and the loop wakes at the oldest's age limit.
- The saturation twin found the loop never looking: an idle loop sleeps until its next work, and an attempt aging is
  none — under the count rule too, which was read only on a wake. The age limit is now one of its instants.
- Port exhaustion's errors are the kernel's documented ones, not measured here: a bound socket's at its bind
  (`EADDRINUSE`), an unbound one's at its connect (`EADDRNOTAVAIL`). K2b-8c's ports twin measures them.
- The sweep killed all 14 mutants; the newest deadline taken for the oldest lived until the twin aged a later step
  beside the first ones.

**K2b-8b-2 — the censor's reset; no byte or hand-over at the deadline; a timer's exact lag** (2026-11-23): a connection's
task's handle kept with it, aborted at the attempt's last look — its socket dropped, at zero linger a reset; the
probe armed with whether its attempt may still send, `Io` holding an armed request's writes pending and unwoken once
it may not (until that abort); a connection ready at the deadline put back idle since when it was, a fresh one since
its connect; the attempt's own sleeps (`Attempt::sleep_until`) noting their due on the path when registered and found
ready or dropped, the path keeping its task's poll — its instant, its window's start, the lag counted.
- A report follows a verified turn by construction: every exit that reports either never handed a request (decorating,
  connecting, the floor, pooling) or follows the writing loop's look, `Io` refusing what the turn could still write.
- Two mutants are equivalent under hyper 1.11.1: the reset dropped at the writing look or at the reading one. hyper
  closes a connection once both its halves are dropped, a write pending or not — the twin's stand sees the close
  either way. The abort stays, so that the reset does not rest on that.
- The sweep killed 18 of 20 mutants, the two equivalent ones aside; the path's twin first let two live — its cases
  had the timer and the bound agree — and was sharpened.
- Observed once in four runs of the shell's suite on this laptop, never alone (six runs) nor in the next three:
  `an_answer_in_the_host_by_its_deadline_is_read_at_the_look` (K2b-6b) panicked "the answer in the host was not
  read". Its request leaves long before its deadline and its look reads through a verified turn; nothing of K2b-8b-2
  is on its path. Owed: on a second sighting, the run's journal kept and the twin's turn diagnosed.

**K2b-8c-1 — a source's sockets under its cap** (2026-11-24): `Egress::ports(source)`; `TcpEgress` holding Linux's
default range (`tcp::PORTS`, the host's own read by `tcp::local_ports`), a bound source its own, the unbound sharing
the rest. The REST carrier keeps per source a `Sockets` count: a place taken before each connect — before the socket
exists — and held by its `Io`, freed at the `Io`'s drop or the connect's failure; the connection's own hold dropped
unpooled marks the socket dying, freed once the runtime drops its aborted task. At the cap the attempt awaits a dying
socket, else evicts — resets — the connection idle longest of its source, to any endpoint, else stops the run with the
carrier fault `egress.cap`; each wait races its deadline (`rest.cap`).
- The cap's fault is a carrier fault, its numbers in the detail, not the journal: open equals the cap when it fires,
  and the cap is the egress's setting — admission's inputs (the ports, the events, the descriptors' limit) belong in
  the journal's header with K2b-8c-2's bound, if a report needs them (Operability's minor finding, revised).
- The sweep killed all 9 mutants. The latest evicted lived until a twin had idle connections on two endpoints and a
  call to a third: the pool keeps each endpoint's oldest first, so the choice shows only across endpoints.

**K2b-8c-2 — a wait at the cap** (2026-11-24, decision 20): `place` tells a dying socket's place (or an evicted
one's) from a cap full of sockets in flight; at either the attempt awaits one wake — a place freed or a connection
idled, `Notify::notify_one`, first come first — and tries its endpoint's pool again; `Attempt::waited` puts the
whole wait on the path's lag (`fetch_max` of its lag before and the wait). `Sockets` keeps since when its waiters
have got no connection; any attempt served resets it; one waiting the route timeout after it stops the run.
- Two mutants are equivalent here: `notify_one` for `notify_waiters` (each woken waiter checks again — a cost, not an
  outcome) and the wait's `fetch_max` for an add (the virtual clock's polls count no lag inside the wait; on the real
  clock the add would count the wait twice).
- The sweep killed all 9 mutants; never served at a place lived until a case had every connection close at its
  answer — served from the pool, the waiters reset the starvation as well.

**K2b-8c-3 — the plan, the carrier's needs, the events** (2026-11-24, decision 20): `source::Plan` — a header and its
commitment's peaks, made by `Replayed::open` (and `Plan::assumed` under `testing`); `Carrier::needs(peaks, source)`,
no default — `Needs::Nothing` or `Needs::Sockets { needed, cap }`, the REST carrier's its pending steps and its
egress's cap, a source not in its own header refused; `Runtimes` private, its events by source, made by `Live`
(`Live::for_caps`: each cap and `run::DRIVER_EVENTS`) or `Runtimes::assumed`; `admit(plan, …)` checking, for each
source a carrier needs sockets of, I1 — the cap and the driver's own within the events, checked arithmetic — then
I2 — the need within the cap; `RunError::Inadmissible(Inadmissible)`, each number its own; stage 2's failures
`RunError::Measure`. The twins' egresses hold caps their runtimes' events take (62 beside 64, 1,000 beside 1,024).
- I1's executor here is arithmetic over the recorded events: tokio's own setting cannot be read back. That `Live`
  builds the runtime it records is K2b-8d's epoll twin (Verifiability).
- The sweep killed all 11 mutants.

**K2b-8c-4 — the descriptors** (2026-11-24, decision 20): `live::Descriptors { soft, hard, open, raised }` —
`read()` (Linux: `getrlimit` and `/proc/self/fd`, the listing's own less; unknown elsewhere), `raise()` (the soft
limit to the hard one, an infinite hard one to the kernel's `nr_open`), `assumed()` under `testing`; `Live` reads them
once its runtimes exist and `Runtimes` carries them, so admission reads them itself; admission refuses when the caps
of the sources needing sockets, the descriptors open and `run::DESCRIPTORS_RESERVE` (16: what is opened after the
read — the journal's file, a gate's client, the host's own) pass the soft limit, or when the descriptors are unknown.
- The reserve is a constant, its use the executor: a twin at the bound less one is refused only with it counted.
  What is opened after the read is stage 3's re-read (K2b-8c-6).
- The sweep killed all 8 mutants; read before the runtimes lives against a twin whose bound comes from `Live`'s own
  count — the twin counts `/proc/self/fd` itself, as Verifiability required.

**K2b-8c-5 — endpoints and sources at stage 1** (2026-11-24, decision 20): `Stand::endpoints(header, source)`, no
default — pairs, not a product; `Egress::admits(sources)` and `Egress::reaches(source, endpoint)`, no default,
`stand::Unfit { cannot, detail }`; `TcpEgress` — every source listed (`egress.source`), the bound on distinct
addresses (`egress.distinct`), each the host's by a throwaway UDP bind (`egress.local`), an address for the endpoint
(`egress.endpoint`), of the bound source's family (`egress.family`), an unbound source's route — its local name after
a UDP connect, which sends nothing — no bound source's address (`egress.shared`); the REST carrier keeping each
source's declared set, checking all of it in `carries` and every route against it at its attempt (`rest.endpoint`).
- `carries` checks the carrier's own declared sets: the plan's header reaching the carrier from the chain is
  K2b-8c-7's.
- The sweep killed all 9 mutants; the egress not admitted lived until admission's twin carried a foreign source.

**K2b-8c-6 — the probe; the descriptors read again** (2026-11-24, decision 20): `Carrier::probe(source, clock, bound)`,
no default, and `carrier::Unprobed { endpoint, kind }`; the REST carrier's — one connect to each endpoint the source
declares, all at once (a join of its own: no futures crate), each raced against the bound, a stream got dropped (a
reset at zero linger), nothing sent; `stages::probe` — each source's probe spawned on its own runtime
(`stamps::on_source_task`, before T0), a runtime that never runs it bounded at twice the bound and its task aborted;
passed, one gate `egress.probe`; refused, named by the first failure — `egress.probe.s<source>.e<index>.<kind>`, the
kind a stand's (`refused`, `reset`, …), `unanswered`, or the harness's own name with its dots as underscores — every
failure in the caller's reason; `stages::descriptors` — the process's descriptors read again against admission's
caps, one gate `egress.descriptors`. The spawn lint now counts any method spawn of a task (a thread's builder,
given a closure, aside).
- The inner bound and the outer one: a connect never answered is refused by either — the outer is the backstop of a
  runtime that never runs the probe; a mutant dropping the inner bound is caught only by timing, which the twins do
  not assert.
- The sweep killed all 5 mutants; a stream kept first hung the twin — the stand's reads now time out, and it fails.

**K2b-8c-7a — the chain** (2026-11-24, decision 20): `Admitted::measure → Measured`, `Measured::gates(namespace,
stopwatch, bound) → Gated` or a boxed `Barred` — the probe, the stand's gates with the namespace in their opening,
the descriptors read again; `Barred::journal` writes the refusal (no T0, the gates passed before it, the judging);
`Gated<RealClock, _>::start(source, open)` starts T0's clock itself, after the third stage, and opens the journal
from the stages' evidence and the plan's identity (`Plan` now carries its log's digest and validator); `run(ready,
source)` takes only a `Ready` (`Ready::assumed` under `testing`). The run's namespace is the attempt's
(`Attempt::namespace`, from the chain): `rest::Config` holds the secret alone; the REST carrier refuses a header
not the plan's own allocation (`ptr::eq`). The free stage functions stay: they make evidence, not a run.
- The twin of a plan over a carrier of fewer sources now meets the header's binding first; the carrier's own
  "no source" refusal stays as a guard no admission reaches.
- The sweep killed all 7 mutants.

**K2b-8c-7b — admission's evidence; reports counted** (2026-11-24, decision 20): journal version 8 — the header's
`admission: Option<Admission { sources: [SourceFit { source, needed, cap, events }], descriptors:
Option<DescriptorsRead { soft, hard, open, raised }>, assumed }>`; `Admitted` keeps it, the chain journals it, the
fold reads none of it (FOLD_REVISION 7, MACHINE_REVISION 4 kept); `assumed` is a test's plan or runtimes; `Ran.unsent`
counts the closing reports journaled by their code — `rest.cap.full`, say — the owed trigger of a read-ahead floor now
observed.
- The sweep killed all 6 mutants.

**K2b-8d-1 — a driver's events; `TIME_WAIT`; exhausted ports** (2026-11-24, decisions 19, 20): `tests/events.rs`,
`tests/ports.rs` and a `TIME_WAIT` twin in `tests/tcp.rs`.
- **The events twin** (Verifiability's strongest executor of I1): a turn of tokio 1.53.1's driver is one
  `epoll_wait` of at most its events, the ready list first in first out; four sockets on a source, a spinner on its
  thread from `D − 800` to `D + 6,000`, three answers at `D − 600`, a task spawned from outside at `D − 400` (the
  driver's own wake), the target's answer at `D − 200`. On a runtime of four events claiming six, the last look's one
  verified turn takes three sockets and the wake, and the target's answer, in the host before its deadline, is read
  absent — in some of eight runs; on `Live::for_caps`' runtime, in every run whose preconditions held, read. The first
  build of the twin never missed: with the fixture's jitter of 0 the look before the deadline also fell past the
  spin, and its own verified turn delivered the target — the look then needs `D − J/2` before the spin (J = 2 ms).
- **No `TIME_WAIT`**: after a run whose three connections were closed by `connection: close`, at the run's end, and
  reset at a censor, `/proc/net/tcp` holds no socket in `TIME_WAIT` on the source's own address.
- **Exhausted ports, measured on purpose** (2026-11-24, this laptop, its range 32768–60999): a source bound to
  127.0.0.7 took 28,232 binds of the 28,232, then `EADDRINUSE`; unbound, 28,228 connects to one endpoint, then
  `EADDRNOTAVAIL`; `TcpEgress` named each `egress.ports`. The kernel searches its range for each bind and connect,
  so near exhaustion the run is quadratic — 126 s for both, run in turn (a bound source's ports sit in the bind table
  an unbound connect's search reads: run together, the unbound ran out early). Not in the gate: the classing's
  executor is the pure twin.
- **The receive twin's second sighting**: `a_wake_between_the_yield_and_the_turn_is_no_turn` (K2b-6b, the prodded
  variant) failed once under the whole suite, passing alone four times and in the next whole suite; its message,
  which carries the journal, was not kept — the next sighting's is.
- **The third sighting, and its cause** (2026-11-24): `an_answer_in_the_host_by_its_deadline_is_read_at_the_look`
  failed under the whole suite at `f.deadline == D` — 75,062 against 70,000. A call's deadline runs from its release
  (`machine.rs:959`); the suite's load had the reader slip past the twin's read-ahead (1 ms) and release the step
  5 ms late, while the twin's driver writes and peeks at instants taken from the constant `D`: a run outside the
  twin's preconditions, which the twin checked for every other state it needs but not this one. Both twins share
  `look()` and this assertion — the earlier sightings, whose messages were not kept, are most likely the same. The
  twin now takes a run only if its dispatch's deadline is `D`, else tries another of its eight. Measured beside four
  spinning processes, thirty runs each: unfixed, 2 failures at that assertion (71,670 and 72,832) and 22 refusals "the
  box too loaded for the twin"; fixed, none at the assertion, 27 refusals — under that load the twin says so rather
  than read a moved deadline as a defect. A test-only fix.
- The sweep killed both mutants.

**K2b-8d-2 — a whole process stalled** (2026-11-24, § Owed's stall twin): `tests/stall.rs`, a test binary of its own —
a child stops it with `SIGSTOP` 1 s into the run for 1.5 s; the stand, a Python process of its own, answers every
request at once and closes its listener 40 ms in every 100; two thousand reads 2 ms apart, deadlines 500 ms, each on
a connection of its own (retired at once); the journal folded, each Root classed by its path.
- **No censor a Root**, in some fifteen runs: a sent censor's or a never-sent report's — some five hundred reports
  a run (`rest.connecting` most, `rest.floor`, `rest.pooling`, `rest.writing`), every one undecided. The wake-latency
  blind spot (§ Owed) made no false Root here.
- **Roots of two other kinds.** (1) The stand's own: closing its listener resets what its backlog held — 3 to 7 a run
  in some runs, steps released on time: true, the stand's. (2) **The burst the harness makes catching up**: the
  machine, stopped too, reads and releases the stalled stretch's steps at once on resuming — some 1.27 s past their
  plan — and the stand, meeting some six hundred connects at once, resets some; 10 and 12 Roots in two runs, each an
  answer the system gave to a step released far past J, which decision 17 makes the system's own however late it was
  sent. The verdict reads **Fail** on a load the plan never asked of the stand: owed — decision 21, under the gate,
  before the first host run.
- The timer rule's mutants (a completed timer's due unnoted, a dropped one's) survive this twin — its censors are
  undecided by paths besides — and are killed by `lag.rs`'s twins; why no attempt refused just before the stop, its
  floor across it, makes a Root under them is not established here.
- A hog over the deadline while connecting or writing on a thread others hold is the source thread's lag (K2b-7c-2's
  twins, the path's lag counted from the thread's last blocked park); the stall twin is its whole-process case.

**Decision 21's replays on an optimised build** (2026-11-24, this laptop; `tests/pace.rs`, ignored,
`cargo test --release`, one test at a time). **The replays below this paragraph ran on the test profile, opt-level 0**
— the library sets no profile — and so did their conclusion "this box cannot offer 2,000 a second": the optimised
harness offers it. The pairwise pace at κ ∈ {10, 50, 200}‰, the harness's outages from the journal:
- 15,000 reads 2 ms apart, three runs: 30, 10 and 0 hand-offs displaced past J (the largest 8.2 ms); none missed at any
  κ; past J at most 8 per mille at 10‰, 2 at 50‰.
- 20,000 reads 500 µs apart: 0, 18 and 3 displaced; none missed; past J at most 37 per mille at 10‰, 7 at 50‰.
- 50,000 reads 100 µs apart (10,000 a second, the stand a Python process on the same box, a thread per connection,
  each read on a fresh connection): 1,626, 3,100 and 9,681 displaced (up to 81 ms); at 50‰ none, none and 3,870
  missed, 61–100 % past J; at 200‰ none, none and 1,754 — this box's limit with its stand beside it.
- One answer in a hundred large (an array of numbers, the judge's slowest shape), 500 reads a second, two runs each:
  16 KiB — about 120 displaced, none missed at any κ, at most 8 per mille past J; 64 KiB — some 845 displaced (up to
  20 ms), 144–150 missed at 10‰, none at 50‰ but two thirds past J (the shift up to 19 ms), 18 % at 200‰; 256 KiB —
  2,800–3,300 displaced (up to 76 ms), 1,589–2,158 missed at 50‰, none at 200‰ but 83–99 % past J.
- So: a judgement holds the whole machine through the frontier's floor (decision 16) for its read — 9.9 ms at 64 KiB
  of numbers, 150–180 ns a byte (K2b-7a, release) — and every hold past `(1 − κ)·J` is a shift. Answers of a few KiB
  cost nothing; one in a hundred at 64 KiB leaves a healthy run whole but every censor of a failing one undecided for
  most of it; at 256 KiB, void. Today's fold reads all of these Pass on a healthy stand; on a failing one each
  catch-up after a hold is K2b-8d-2's burst. The judge's speed (decision 18's owed `Number` and duplicate check) is
  decision 21's precondition wherever answers are that large: trigger, H-1's largest answers.

**Decision 21's pacing, replayed over healthy runs** (2026-11-24, this laptop, opt-level 0; `tests/pace.rs`, ignored):
a stand of its own process answering at once, no stall, J = 2 ms, deadlines 500 ms, each read on a fresh connection;
the journal replayed through round 2's gate — an attempt displaced past J (its hand-off more than J after its due
instant) admitted only if no admitted displaced attempt's hand-off falls after its overdue instant.
- A read every 2 ms, 5,000 a run: 0, 22 and 28 displaced; the gate would refuse 0, 13 and 5 — two runs of three
  void; the largest displacement 11.4 ms.
- Every 500 µs, 20,000 a run: 954 to 3,617 displaced; 524 to 2,729 refused; the largest 51 ms.
- Every 100 µs, 50,000 a run: nearly all displaced, the largest 2.9 s — the laptop's harness cannot offer 10,000 a
  second at all; today's fold passes such a run on a healthy stand, its load never offered.
- So J = 2 ms is far tighter than this box's own scheduling holds, and any rule that sheds or voids on a displacement
  past J alone voids healthy runs; the plans' J and the box's jitter are a question for the host's reference machine
  (M12) as much as for decision 21.

**Decision 21, round 3's shaper replayed** (2026-11-24, this laptop, opt-level 0; `tests/pace.rs`, ignored): round 3's blind solver
— the harness a shaper at the hand-off, never compressing the plan by more than the profile's reserve ρ nor bunching
past J, the fold verifying it — replayed over the same healthy journals as its discriminator prescribed: the
harness's outages (the union of `[due + J, handed)` over attempts handed more than J late) taken from the journal, each
attempt passing at the first instant at or past its pace outside them, the reserve rounded down so none is lost.
- 15,000 reads 2 ms apart (30 s), three runs: none missed its deadline at any ρ; handed more than J past its due —
  at ρ = 0, 0, 900 and 870 per mille (the shift a running sum, up to 74 ms); at ρ = 50‰, 0, 5 and 48 per mille;
  at ρ = 200‰, 0, 1 and 11.
- 20,000 reads 500 µs apart (10 s), three runs: at ρ = 50‰, 0, 768 and 0 missed, 420, 998 and 44 per mille handed late;
  at ρ = 200‰, 0, 477 and 0 missed — this box cannot offer 2,000 a second reliably.
- So the shaper keeps healthy runs healthy only while ρ exceeds the box's own outage fraction (its inflow of lateness)
  — on this laptop at 500 a second ρ = 50‰ holds; at ρ = 0 (the fixture's) every run drifts. A first replay that took
  the larger of the pace and the real hand-off, not their sum, under-reported this (its discriminator).
- **The pairwise form against the recursion**, side by side over the same journals (a third set of runs): for every
  two hand-offs `1000·|G_k − G_i| ≥ (1000 − ρ)·(|raw_k − raw_i| − J)` — no clamp, no rounding, the allowance
  `(1 − ρ)·J` exact — against round 3's recursion. **No healthy run held an inversion** (an attempt handed behind a
  later due already handed): the read-ahead covers the reader, so the two forms differ only by the tighter allowance —
  within 1 per mille of hand-offs past J in every run. At 2 ms: none missed at any ρ ∈ {10, 50, 100, 200}‰, handed past
  J 0, 163 and 0 per mille at 10‰, 0, 34 and 0 at 50‰. At 500 µs: 424–516 missed in two runs of three at 10‰, 0–4 at
  50‰ and above, the shift up to 0.5 s.
- **This box's noise**: the same replay's runs differ several-fold between sets (at 500 µs and 50‰: 0, 768 and 0
  missed in one set, 2,191, 3,287 and 0 in a second, 4, 0 and 0 in a third) — availability's numbers belong to the
  machine that runs the load, measured there; this laptop's settle only that 500 steps a second hold at ρ ≥ 10‰.

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
- **K2b's measurements** (the roles): a burst of a bucket's size at one instant through the scripted carrier, one
  source and spread — past J at the plan's largest burst, the machine partitioned or the source sharded before the
  first host run; the stamp lock's wait and the holds' durations, p99.99 and maximum, at the launch's sources on the
  reference machine; `answer::read` + `judge` at the cap on an object-dense document; bytes per kept connection and
  open attempt (a counting allocator); a timed wait against a spin, the stand's p99 A/B on one machine; the probe of
  the virtual clock on the newest tokio (a library job) — each before the first host run.
- **The receive side's remainder** (K2b-3b): one driver turn sees at most tokio's events per tick (1024 by default)
  — the live composition (K2b-6) builds each source runtime with more than its descriptors; a look's own duration (a
  cap-sized answer through hyper's want and its 16 KiB reads) counts against P1's band, measured on the reference
  machine before the rule is trusted at scale; no differential reaches an answer within a microsecond past its
  deadline, where `sim` gives none and the shell finds it — a differential with deadlines among the answers' draws is
  owed with `sim` taking such an answer late; the `Egress::Io` contract (no relaying task) is stated on the port, its
  twin owed with the first host's TLS or proxy egress.
- **A never-sent attempt's path** (K2b-3a): with no `Sent` it carries no `handed` or `connected`; its class is undecided
  already, but attribution is lost, and refused connects are not told from the harness's lateness — K2b-6, with refused
  connects journaled.
- **The gaps' reader** (K2b-3a): the journal holds every instant of decision 9's chain, and the fold reads the
  harness's part; a reader of the six gaps per attempt, and K2-2's executor (each cause delayed alone moves its gap),
  are K5's report.
- **A late-found answer that was a refusal** (K2b-3b): voided with the censor it follows, never a false pass; a
  refinement reading its judgement is owed.
- **The shell's faults in its journal** (K2b-1; decided by decision 18, built by K2b-7b and K2b-7c): a harness fault
  ends the run with an error — and, as built, the writer's thread then writes a trailer when the channel closes (the
  gate found the first wording, "a journal without a trailer, read as truncated", false). A journal writer that fails
  is heard at the finish, not at its line.
- **What decision 18 leaves** (K2b-7's gate): the J/4 refusal, K5's, for a plan carrying a measure; a source's judging
  utilisation (its peak answer rate times the judging time), with M11's peak rate; the lazy read, the duplicate check
  without a clone per member and `Number`'s three allocations, and judging off the source's thread — trigger: H-1's
  largest judged answer at the host's J past J/4 on the reference machine; the journal's disk before T0 (the command
  log's `disk` precedent; ~1.18 kB and 7.4 lines a step on the goldens) and a stalled disk blocking the loop's send —
  trigger: a host run past a correctness run's size; a host's own code for a carrier's `cannot` — trigger: a host
  decorator with several kinds of failure; a driver turn's own dispatch at the launch's descriptors, measured on the
  reference machine (it is J's in the lag).
- **What K2b-2a leaves to later slices**: a request handed back unsent taken by the next connection, executed by K2-3's
  close-on-reuse twin, and a refused connect retried a floor apart (today the frontier censors it, unsent), K2b-6's; idle
  retirement, K2b-6's; an answer after a censored deadline kept and journaled ignored, and the ports' cap, K2b-8's; a
  poll's frame its own and a call's resent byte for byte, executed by K2-5's frames with a `Sent` hole, K2b-4's.
- **A wake delivered late after a park that blocked** (found building K2b-8b, 2026-11-23): decision 18 counts a poll's
  lag from its thread's last park that blocked, taking every event ready before the park's end as delivered at it. A
  thread woken from a blocking park runs only when the OS schedules it: under a CPU quota's throttle (a container's
  `cpu.max`), a pause or `SIGSTOP`, the park ends at the resume, and an event ready long before reads as no lag — a
  send path's lag under-counted, a stand charged with the harness's stall. A stall covering a never-sent attempt's
  deadline is still undecided (K2b-8b-2 counts the due of the timer its race registered at `D + 1`); one ending before
  the deadline is not, nor a sent attempt's. Owed: K2b-8d's stall twin (`kill -STOP` for 1.5 s beside a listener
  refusing 5 ms in 100) counting Roots on sent censors as well as never-sent ones; on it red, or on a host whose
  harness runs under a CPU quota, a stall witness — a thread of its own whose gaps are charged to the paths whose
  windows they cover — or a stage-1 refusal of the quota, decided under the gate.
- **Decision 20's owed** (K2b-8c): **the peaks bound to the log** (Verifiability) — the commitment's fields are
  public and nothing digests `peaks`; trigger: a host writing its commitment other than from the validator's run;
  **a per-source peak** (`Peaks.sockets`, a commitment format change), the refusal naming the peak's line and instant
  (Operability) — trigger: a plan with sources > 1 refused on the total while its share fits; **a process-wide
  descriptors' ledger** (Software Design) — trigger: a host running runs concurrently in one process; **a gateway's
  per-address connection limit** (Operability) — refusals at it charged to the stand where real clients, spread over
  addresses, meet none; trigger: a host gateway with such a limit; **exhausted ports measured** (`EADDRINUSE` at a
  bound socket's bind, `EADDRNOTAVAIL` at an unbound one's connect; Verifiability) — K2b-8d, in a namespace with a
  ten-port range; **a read-ahead floor against the looks' slack** — trigger: a run with `rest.cap.*` reports;
  **the counts of reports by code** in `Ran` or the fold — with K2b-8c-7's evidence.
- **Decision 21's owed** (2026-11-24, its roles): **a hung decorator holds the band's front** to its deadline (up to
  the route timeout) — a bound on a head not arrived, trigger: H-1's decorations measured past J; the Decorator
  contract stated now (shared credentials refreshed ahead, off the attempt's path; headers valid to the attempt's
  deadline); **decision 14's rate monitor and the unplanned 429** (§ Decision 21); **dependents of answers stamped
  together** after a receive-side stall — their `intended` coincide, the pace lets them pass together: the stall
  witness; **bunching below the gate** (connect floors, cap waiters) — witnessed by `connected` and the lag, judged by
  nothing: trigger, a host run's Roots with `connected − handed` bunched; **the judge's speed** where answers are large
  (§ Measured, decision 21's replays): trigger, H-1's largest answers past 16 KiB; **the stand co-located** with the
  harness, stopped with it (its own backlog answering paced attempts) — the stall twin with the stand stopped too, on
  this laptop; **the header's J and κ checked against the commitment** — trigger: a host writing its journal header;
  **admission refusing the rate** when no κ keeps the box's own shift draining — on the load machine; **a stop when
  nothing can pass before the longest timeout**, ranking below Fail, and a progress port; **the gate's capacity** —
  each hand-off a wake of the next while predictions fail: measured at the launch's rate on the load machine.
- **A lag sampler per source** (decision 9, K2b-1): for measures' intervals, not the fold's rules — K5's report, with
  the machine's own runtime set apart from the sources' (`Live` runs the sources; the composition root runs the
  machine).
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
- 2026-11-22: K2a-5 cut into K2a-5a and K2a-5b; K2a-5a built (§ Measured): polls re-polled, resends spaced,
  `MACHINE_REVISION` 2, the model's resend.
- 2026-11-22: K2a-5b built (§ Measured): journal version 3, the fold licensing resends, `FOLD_REVISION` 2. K2a is
  complete; K2b, the shell, next.
- 2026-11-23: K2b decided under the gate (decision 16; a solver and a discriminator on Fable, three roles on Opus) and
  cut into K2b-0a to K2b-8; decision 4 amended: the watermark constructed by a frontier.
- 2026-11-23: K2b-0a built (§ Measured): the frontier, `sim` driven through it; decision 16's rule on the order of
  inputs taken corrected by its property: stamps, not keys.
- 2026-11-23: K2b-0b built (§ Measured): the machine re-checks a wake at the instant taken, `MACHINE_REVISION` 3; the
  fold makes undecided what a late wake ended, `FOLD_REVISION` 3. Decision 16's rule corrected: undecided whatever the
  lateness, not past J — its executor's own case, a resend taken 2 µs late, contradicted it. A poll's wake taken
  exactly at its deadline owed to journal version 4.
- 2026-11-23: K2b-1 built (§ Measured): the `load-run` crate — the clock, the frontier's lock, the machine's loop,
  the ports. Decision 16 refined: the floor of an answer being judged holds the machine's own work too, an input below
  the machine's clock a harness fault; the live clock's timed wait is its own timer thread's; the virtual clock's
  idle panic is raised in the test's future.
- 2026-11-23: K2b-2 cut into K2b-2a and K2b-2b; K2b-2a built (§ Measured): REST over hyper's own connection. Decision
  16 refined: `Sent` is the first flush after the hand-off, the guards of the body's end and a byte written dropped as
  dead, the twins holding hyper's order.
- 2026-11-23: K2b-2b built (§ Measured): the shell's journal equal to `sim`'s over REST; an attempt leaves only past
  its deadline. `sim::Attempt.seen` replaced by a projection checked against the stub.
- 2026-11-23: K2b-3 decided under the gate (decision 17: a solver and a discriminator on Fable, two roles on Opus),
  amending decisions 9, 14 and 16; cut into K2b-3a and K2b-3b; K2b-3a built (§ Measured): journal version 4.
- 2026-11-23: K2b-3b built (§ Measured): journal version 5, the receive side. Decision 17 refined: the fold keeps a
  censored step's attempt until its late answer is found, rather than a bit; the looks in a head's wait and a body's.
- 2026-11-23: K2b-4 built (§ Measured): the decorator port, frames rebuilt from their fills; decision 16 refined (no
  frame cache) and decision 17 (`handed` once the attempt is decorated).
- 2026-11-23: K2b-5 built (§ Measured): resends end to end over REST.
- 2026-11-23: K2b-6 cut into K2b-6a and K2b-6b; K2b-6a built (§ Measured): connections and the live composition.
  Decision 16 refined: the loop reads the machine's own wakes, and ends on one read of the frontier and the attempts.
- 2026-11-23: K2b-6b built (§ Measured): K2-3 over TCP, the lateness drill, the timed wait measured.
- 2026-11-23: decision 18 added under the gate (K2b-7: a run's stop, the source's thread attributed, three stages
  before T0); decision 16's J/4 refusal moved to K5, its lazy read marked owed, its dead guard on a written byte live;
  decision 17's stand gaps less the source thread's lag; § Owed's sentence on the trailer corrected. K2b-7 cut in five.
- 2026-11-23: K2b-7a built (§ Measured): the attempt's every await races its deadline, `Sent` only after a byte of
  the armed request, a dropped attempt's fault raised before it leaves the count; the judging time measured.
- 2026-11-23: K2b-7b built (§ Measured): journal version 6 — a send's lag, a run's stop, a gate's refusal, the
  stages' evidence — in the pure fold and its definition; FOLD_REVISION 6, MACHINE_REVISION 4 kept.
- 2026-11-23: K2b-7c cut into K2b-7c-1 and K2b-7c-2; K2b-7c-1 built (§ Measured): the shell's stop in its journal,
  the run's tasks kept and aborted, the journal's trailer only when finished.
- 2026-11-23: K2b-7c-2 built (§ Measured): the source thread's lag on a send path, its park detector measured; `nix`
  joins the shell's allowlist (Linux).
- 2026-11-23: K2b-7d built (§ Measured): admission, the judging measured, the stand's gates in turn, a gate's refusal
  journaled; the validator's rules at revision 9 (a call's source within the profile's).
- 2026-11-23: decision 19 added under the gate (K2b-8): a censored attempt's connection reset, decision 11's keep
  reversed; a never-sent censor's closing report and its Root on the peer's refusal; the source thread's lag
  completed (timer lateness exact, a re-hand resetting it); connect errors classed by the egress; ports. K2b-8 cut in
  four, before K2b-7e.
- 2026-11-23: K2b-8a built (§ Measured): journal version 7 — a never-sent censor's closing report — in the fold, its
  definition, `sim` and the REST carrier's basic report; FOLD_REVISION 7, MACHINE_REVISION 4 kept.
- 2026-11-23: K2b-8b-1 built (§ Measured): a connect's failure classed by the egress, zero linger on every socket, no
  shutdown forwarded, a source saturated by age; `run::SATURATION` removed. Owed added: a wake delivered late after a
  park that blocked.
- 2026-11-23: K2b-8b-2 built (§ Measured): a censored attempt's connection reset at its last look, no byte of a
  request past its deadline, none handed at it and an unused connection pooled, a registered timer's due its poll's
  lag. K2b-8b built.
- 2026-11-24: K2b-8c-1 built (§ Measured): the egress's ports, a source's sockets capped over each socket's life, the
  idle evicted at the cap, `egress.cap` past it. K2b-8c cut in two. Revised: the cap's fault journals no numbers
  (Operability's minor finding) — open equals the cap when it fires, and the cap is the egress's setting.
- 2026-11-24: decision 20 added under the gate (K2b-8c): a wait at the cap the harness's and a full cap waiting
  (amending decision 19's `egress.cap`), admission's soundness and prediction kept apart, their owners, endpoints per
  source, the probe, the stages' chain, admission's evidence; K2b-8c cut in seven.
- 2026-11-24: K2b-8c-2 built (§ Measured): a wait at the cap on the path's lag, a connection idled waking a waiter,
  a full cap waiting, `egress.cap` for a stuck cap alone.
- 2026-11-24: K2b-8c-3 built (§ Measured): the plan from its commitment, the carrier's needs, `Runtimes` with its
  events made by `Live`, admission's I1 and I2, `Inadmissible`.
- 2026-11-24: K2b-8c-4 built (§ Measured): the process's descriptors read by `Live`, raised by the root, checked at
  admission against the caps summed.
- 2026-11-24: K2b-8c-5 built (§ Measured): the stand's endpoints per source, the egress's admission and reach with no
  packet, a route outside its source's set a harness fault.
- 2026-11-24: K2b-8c-6 built (§ Measured): the probe at stage 3, every declared pair at once on its source's runtime;
  the descriptors read again as stage 3's last gate; the spawn lint widened.
- 2026-11-24: K2b-8c-7a built (§ Measured): the stages a chain — `run` takes only what they make, T0's clock the
  library's, the journal opened from their evidence, the namespace and the header bound from the chain. K2b-8c-7
  cut in two.
- 2026-11-24: K2b-8c-7b built (§ Measured): journal version 8, admission's evidence in its header; the closing reports
  counted by code. K2b-8c built.
- 2026-11-24: K2b-8d-1 built (§ Measured): a driver's events against the cap on the real clock, no `TIME_WAIT` left
  by the shell, exhausted ports measured. K2b-8d cut in two.
- 2026-11-24: K2b-8d-2 built (§ Measured): a whole process stalled — no censor a Root; the burst the harness makes
  catching up found to make Roots of the system's answers (decision 17) — owed, decision 21. K2b-8d built.
- 2026-11-24: decision 21 under the gate, two rounds: a displacement window, an overrun of the plan's densest window,
  pacing at the hand-off — each refuted (§ Alternatives Considered), the last by measurement (§ Measured); a third
  round owed from what they established (§ Owed).
- 2026-11-24: decision 21, round 3: a shaper at the hand-off, sound in kind; its discriminator's defects and its
  replay over healthy runs (§ Measured) recorded; its roles next.
- 2026-11-24: decision 21's pairwise form replayed beside round 3's recursion (§ Measured): no inversion in a healthy
  run, the forms within 1 per mille; the laptop's noise between sets recorded. The owner, the same day: the 12-core box
  is busy and a remote load machine is coming; the launch-scale measurements wait for it, and the laptop settles
  counts, A/B on one box and the first host run as a correctness run.
- 2026-11-24: the receive twin's flake found — a release late past the read-ahead moved the deadline the twin's driver
  assumes; the twin now requires its step's deadline before judging a run (§ Measured K2b-8d-2, its third sighting).
- 2026-11-24: **decision 21 decided** after round 3's four roles (§ Decision 21; § Alternatives Considered round 3):
  the pace, its raw dues, κ, the band gate, the fold's revision 8, decision 17 amended; K2b-9 its cut. The replays
  rerun optimised (§ Measured): the earlier ones were on opt-level 0, and their "this box cannot offer 2,000 a second"
  withdrawn; judgements of large answers found holding the machine — the judge's speed owed with H-1's trigger.
