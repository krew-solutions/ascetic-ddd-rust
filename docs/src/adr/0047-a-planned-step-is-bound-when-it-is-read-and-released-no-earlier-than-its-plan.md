# ADR-0047: A planned step is bound when it is read and released no earlier than its plan

## Status

Proposed (2026-11-21); slices K0a, K1 (K1a to K1e), K2a, K2b-0a, K2b-0b, K2b-1, K2b-2 (K2b-2a, K2b-2b) K2b-3 (K2b-3a, K2b-3b), K2b-4, K2b-5, K2b-6 (K2b-6a, K2b-6b), K2b-7a, K2b-7b, K2b-7c (K2b-7c-1, K2b-7c-2), K2b-7d, K2b-8a, K2b-8b (K2b-8b-1, K2b-8b-2), K2b-8c (K2b-8c-1 to K2b-8c-7b) and K2b-8d (K2b-8d-1, K2b-8d-2) built (§ Measured), decision 21 decided — the harness shapes at the hand-off (§ Decision 21); K2b-9a, K2b-9b-1 and K2b-9b-2 built (§ Measured); its raw-due rule refuted in two rounds, a dead end whose next question is no catch-up (§ Owed); then K2b-9b-3 to 9d; decision 22 decided (the shell's memory), K2b-7e-1, K2b-7e-2a, K2b-7e-2b-1 (K2b-7e-2b-1a to K2b-7e-2b-1c), K2b-7e-2b-2a, K2b-7e-2b-2b (K2b-7e-2b-2b-1 to K2b-7e-2b-2b-3), K2b-7e-2b-2c-1 to K2b-7e-2b-2c-4 built, K2b-7e-2b-3 decided and cut in five, K2b-7e-2b-3a-1 to K2b-7e-2b-3a-3, K2b-7e-2b-3b, K2b-7e-2b-3c-1 and K2b-7e-2b-3c-2 built, K2b-7e-2b-3d cut in four, K2b-7e-2b-3d-1 and K2b-7e-2b-3d-2 built, K2b-7e-2b-3d-3 decided and cut in two, K2b-7e-2b-3d-3a in two, K2b-7e-2b-3d-3a-1 and K2b-7e-2b-3d-3a-2 built, K2b-7e-2b-3d-3b cut in four, K2b-7e-2b-3d-3b-1 and K2b-7e-2b-3d-3b-2 built, K2b-7e-2b-3d-3b-3 cut in two, K2b-7e-2b-3d-3b-3a and K2b-7e-2b-3d-3b-3b built, K2b-7e-2b-3d-3b-4 cut in three, K2b-7e-2b-3d-3b-4a to K2b-7e-2b-3d-3b-4c built, K2b-7e-2b-3d-3b done, K2b-7e-2b-3d-4 decided and cut in three, K2b-7e-2b-3d-4a built and measured — P4 refuted as stated, its rows' guard twice "no test" —, K2b-7e-2b-3d-4b built and measured — P6 no test at idle, P10's first reader void —, K2b-7e-2b-3d-4c next; decision 23 decided, K2b-7e-4a built, K2b-7e-4b decided and cut in three, K2b-7e-4b-1 and K2b-7e-4b-2 built, K2b-7e-4b-3 measured — four refutations, one remedy each —, K2b-7e-4b-4 and K2b-7e-4b-5 built and measured, K2b-7e-4 done (decision 23's steps 1 and 2), decision 23's step 3b on the owner's confirmation; then the first host run, whose memory bound counts the connections K2b-8 decides. Stage 3 of three toward the host's
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
  shell's memory (decision 22), in five — **K2b-7e-1** the constants by construction; **K2b-7e-2** admission, in three
  — **K2b-7e-2a** the costs stated and held, `fits` re-pinned, **K2b-7e-2b** a stated memory, in three — **K2b-7e-2b-1**
  the plan's shapes and their prices (**K2b-7e-2b-1a** a record's shape and its price; **K2b-7e-2b-1b** the
  validator's peaks of shapes in the commitment; **K2b-7e-2b-1c** `fits` and the frame priced by them), **K2b-7e-2b-2** the reader's, the journal's and the sources' terms by
  construction, in three (**K2b-7e-2b-2a** the reader; **K2b-7e-2b-2b** the journal, in three — **K2b-7e-2b-2b-1** the lines and the
  writer, **K2b-7e-2b-2b-2** the channel built from admission, **K2b-7e-2b-2b-3** the carrier's boundary;
  **K2b-7e-2b-2c** the sources, in four — **K2b-7e-2b-2c-1** the runtime and its events, **K2b-7e-2b-2c-2** the
  parse's bound and its roles, **K2b-7e-2b-2c-3** the tail held, **K2b-7e-2b-2c-4** a carrier's source costs),
  **K2b-7e-2b-3** admission and its evidence, in five — **K2b-7e-2b-3a** the prices, in three (**K2b-7e-2b-3a-1** a
  request's bytes by class and its frame sized at once, **K2b-7e-2b-3a-2** the carrier's attempt by class and hyper's
  head buffer, **K2b-7e-2b-3a-3** the shell's attempt, `needs` by source, the pool's records), **K2b-7e-2b-3b** the ports'
  statements, **K2b-7e-2b-3c** the pure fit, in two (**K2b-7e-2b-3c-1** the fit and its properties, **K2b-7e-2b-3c-2**
  the remedies, `Egress::caps`, `fits::fits` retired), **K2b-7e-2b-3d** the wiring and version 10, in four
  (**K2b-7e-2b-3d-1** `fit`, `Fitted`, `Live::fitted` and `admit(fitted, &runtimes)`, the descriptors read ahead, every
  refusal at once; **K2b-7e-2b-3d-2** `Limit::read` and the scope rule; **K2b-7e-2b-3d-3** journal version 10, in
  two — **K2b-7e-2b-3d-3a**, no journal change, in two (**K2b-7e-2b-3d-3a-1** κ by one call and refused by name,
  the limit's origin, `GateBuilt`, `unheld`; **K2b-7e-2b-3d-3a-2** the terms by count, cost and rest, the parts, one
  source by type, the `Model`, the provenance's labels, the owners' counts); **K2b-7e-2b-3d-3b** version 10, in four (**K2b-7e-2b-3d-3b-1** a header's version read before its
  schema, by the reader and by `verdict.py`; **K2b-7e-2b-3d-3b-2** `admission.memory`, its mapping, the pure
  `admission(…)`, the settling; **K2b-7e-2b-3d-3b-3** the recheck, in two — **K2b-7e-2b-3d-3b-3a** the versioned recheck in the
  fold, the part table as data, the self-check, the census in Rust; **K2b-7e-2b-3d-3b-3b** `verdict.py`'s twin, the
  census through both, the two-sided property —; **K2b-7e-2b-3d-3b-4** in three — **K2b-7e-2b-3d-3b-4a** the pins split and
  the CI step, **K2b-7e-2b-3d-3b-4b** load-run's pinned headers, **K2b-7e-2b-3d-3b-4c** the evidence priced —) —; **K2b-7e-2b-3d-4** the
  whole-run measurement, in three — **K2b-7e-2b-3d-4a** the harness and its first rows (the control, the answers
  held, the attempts and the late records, the decoder) with their predictions, **K2b-7e-2b-3d-4b** the sources, the
  kernel's sockets and the process's factors, **K2b-7e-2b-3d-4c** the server's matrix and the stall's curve —), **K2b-7e-2b-3e** the evidence — the `memory` line with its producer among it
  after a kill —, **K2b-7e-2c** the cgroup's memory, after the read gate;
  **K2b-7e-3** the read gate; **K2b-7e-4** the parse, in two (decision 23) — **K2b-7e-4a** a body parsed on its first
  reader and a late answer holding nothing, **K2b-7e-4b** the tape (decision 23's step 2), in three — **K2b-7e-4b-1**
  the grammar over two sinks, the names' check, the tape and its differential against the tree; **K2b-7e-4b-2** the
  readers on the tape, the frozen tree readers, the bound, rules revision 15; **K2b-7e-4b-3** M-a and the floor's
  measurement with bodies read, their predictions written first —; **K2b-7e-5** a waiting budget, if a real plan needs it; **K2b-8** a censored attempt's connection, its closing report and the ports (decision 19), in
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
  the plan's instant; **K2b-9b** in parts — **K2b-9b-1** journal version 9's bytes, **K2b-9b-2** the pure pace and
  its band, **K2b-9b-3** the raw dues in the machine (decision 21's raw-due rule, refuted while building, under the
  gate), **K2b-9b-4** `sim` paced, **K2b-9b-5** the fold's next revision (8 is K2b-7e-2b-3d-3's); **K2b-9c** the
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
- **Raw dues** (this bullet refuted while building K2b-9b, its amendment under the gate: § Owed, "decision 21's raw
  dues"). A first attempt: its `Released.intended` — never the plan's instant, which would make the system's
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
- **The fold** (its next revision; 8 taken by K2b-7e-2b-3d-3):
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

### 22. The shell's memory: constants by construction, a stated memory, the answers charged before they wait

*Decided for K2b-7e* (2026-11-24, under the gate: the main position, a blind solver and a discriminator on Fable,
two roles — Operability, Verifiability — on Opus; it builds decisions 16's and 18's "`fits` with the shell's memory",
which `admit` never called; § Alternatives Considered keeps what each refuted):
- **Constants by construction, not measurements that drift** (K2b-7e-1): one hyper HTTP/1.1 builder for every
  connection — an exact read size (`read_buf_exact_size`, 16 KiB: the read buffer never grows to a body's or a head's
  size; exclusive with `max_buf_size`, which unsets it) and vectored writes (`writev`: the body queued, never copied
  into the head's buffer); a head past the read size, or past hyper's hundred headers, a fault that stops the run
  naming the operation — never `Broken`, the system's Unknown (a declared head limit owed); the counting `Io`'s
  scratch one a thread, not one a connection (it is used only inside one synchronous read); an answer's body sized
  from its Content-Length, at most the cap, a length past the cap unreadable at its head, a body of no length reserved
  at the cap (no doubling — it held twice the cap, K2b-7e-2a —, no allocation a forged length could abort); the
  response's headers copied and its parts dropped before the body (a header slices the read buffer and pinned it); a
  finished attempt's task cell let go at once. The carrier states each cost (`carrier::Costs`: a connection, an
  attempt beside it, an answer at the cap with its head at its worst), the shell its own share an attempt
  (`run::ATTEMPT`), a source's its own (K2b-7e-2b) — and the executors hold them two-sided.
- **A memory the operator states** (K2b-7e-2): `Memory { bytes }` — declared (the cgroup's as its source with
  K2b-7e-2c), never the host's free memory guessed; I1, soundness, I2, prediction, a refusal naming every part and the
  least memory that would fit, every term and its origin in the header. **Amended for K2b-7e-2b** (2026-11-24, under
  the gate: the main position, a blind solver and a discriminator on Fable, three roles — Software Design,
  Operability, Verifiability — on Opus; § Alternatives Considered keeps what each refuted):
  - **I1 is a sum of terms, each a count held by a named enforcer times a cost stated by its part's owner** and held
    by an executor — two-sided where the library holds it, journaled `host-stated` where a host's part states it; a
    count with two enforcers takes the lesser; a term whose count or cost has no enforcer yet is charged at its
    measured worst and journaled `measured`, or refused naming it — never claimed held, never left no room. Checked
    arithmetic; the slack never a saturating subtraction.
  - **One pool, not two shares**: the machine's cells and the attempts are taken by one act, a read, and freed by one
    event, a step's end — a gate on attempts alive bounds nothing (lines read at `at − L` dispatch later with no
    further read, as do dependents, resends and polls; an answered attempt's handle is dropped before its end is
    stepped). The pool's count is the steps read and not ended plus the handles past their deadline (a censored
    attempt's handle lives to its `D + 1` look, holding its record, frame and captures).
  - **The plan commits shapes, the scheduler prices them**: the commitment carries, at their peaks over the pending
    and the held sets, plan-intrinsic sums — a record's string bytes by role, its holes, its dependencies, its frame's
    template bytes — with pending by source (one `source_of` in `command-log::plan`, socket steps only), the largest
    line and chunk, and pending with every end a jitter later; one pure function the scheduler owns prices a shape
    (cells, keys, bound producers, a producer's waiters, captures), the carrier a record's frame (its template, its
    holes at the capture bound, its decorator's stated bound) — `Σ cᵢ·max Sᵢ ≥ max Σ cᵢ·Sᵢ`, so a re-pin needs no new
    dry run and `RULES_REVISION` moves only with a shape's meaning. A heap model's output committed would freeze its
    revision (`LIVE_CELL` moved 1,792 → 3,776 in K2b-7e-2a).
  - **An attempt priced by its record**: its frame, the request's copy of the body, its captures' copies — a constant
    `a` charged a 64 KiB bulk import a tenth of what it holds.
  - **Captures held by the enforcer that exists**: a value is taken only within `TAKEN` (8 KiB, `answer::take`); a JSON
    number, today unbounded, bounded likewise; each copy a reader holds in flight priced.
  - **The reader and the journal by construction**: `ahead` and `behind` carried by type from what built their
    channels (admission saw neither); the verifier's chunk held twice at the committed largest chunk; the line limit
    enforced before its allocation, its four buffers stated; zstd's window bounded (`window_log_max`) and charged —
    libzstd's malloc is outside the counted heap. **Amended for K2b-7e-2b-2b** (2026-11-26, under the gate: the main
    position, a blind solver and a discriminator on Fable, three roles — Software Design, Operability,
    Verifiability — on Opus; § Alternatives Considered keeps what each refuted): **the journal's channel is built
    only from what admission holds** — its header exists only after the stages, so its count flows from admission
    to its builder, not from its builder to admission as the reader's does: a `sink::Journal { behind, out }`
    passed to `admit`, `Written::start` the library's alone, the host giving only `W`, opened before T0 (2b-2b-2);
    **a line priced by the journal's owner** — `journal::line_bound` of the plan's largest record: a read's key,
    dependencies and barriers' keys, an answer's slots and two codes of at most `CODE`, an end's box and its step
    dependencies' lines, a fault's code, each row exact, `journal::line_heap` the walk; **a record's barriers' keys
    a role of its shape** (`each_bytes`, revision 12: a barrier may have no member, so no step's key bounds its
    key), the machine's copy of them priced by `fits` from the plan's largest record; **a step's lines made one at
    a time** (`journal::events` an iterator); **the writer streaming** through a fixed `journal::BUFFER`, each line
    whole in its output and the output flushed once written (2b-2b-1); **the carrier's boundary** — what a carrier
    hands the shell held where it enters: captures matching distinct specs and kept in the spec's own names, values
    within `VALUE` kept at their length, codes `Code::of`'s alone by type (2b-2b-3).
  - **A source's own**: a constant (its runtime, its thread's scratch, one answered tail, one head in transition) and
    its driver's events times an event's size (`Runtimes` owns the events, linear in the cap); the parse at its
    measured worst until K2b-7e-4. The answered tail — arrived, read, judged, checked in — a function with no await,
    so "one answer a socket, one tail a source" holds by the compiler, not by position. **Amended for K2b-7e-2b-2c**
    (2026-11-27, under the gate: the main position, a blind solver and a discriminator on Fable, three roles —
    Performance, Software Design, Verifiability — on Opus; § Alternatives Considered keeps what each refuted): **the
    parse is bounded by the answer cap**, `json::bound(n) = 2·size_of::<Json>()·(n + 1)` — its densest input, chains of
    single-element arrays, holds about 128 B a body byte, a shape stage 2 never times —, enforced by the cap and lowered
    by K2b-7e-4, not "at its measured worst"; its cost to an owner is `128·cap·sources` (8 MiB a source at a 64 KiB
    cap, 128 MiB at 1 MiB, enough to refuse a plan that fits — the refusal names it, its remedies the cap and
    K2b-7e-4). **The shell's own**: `RUNTIME` and `EVENT` by target, `Runtimes::own` over the runtimes built (2b-2c-1);
    `kept`'s rebuild once a source, whatever carries it (2b-2c-3). **A carrier's source costs** beside its needs — its
    scratch, a head copied in transition, the most its tails hold at once — summed, not their largest (a host's
    carrier may parse a head and a body in one section), stated by the library or by its host (2b-2c-4). **One tail a
    source** held by a sync `answered` and an `answer::Read` neither `Send` nor `Sync` (2b-2c-3). **A capture's
    value** one predicate of `take`'s contract, at `kept` and before `fills` parses it; **the plan's largest predicate
    literal and pointer** roles of its shape (revision 13; 2b-2c-2).
  - **I2 stays decision 21's where it was decided**: `⌈needed(s)·1000/(1000 − κ)⌉ ≤ cap(s)`, `needed(s)` the carrier's
    from the plan's pending by source; the pool's prediction at the same rate — κ = 0 when decision 21's question
    settles on no catch-up, never dropped silently.
  - **The refusal**: every refusal at once, not the first (raising the descriptors to be refused on memory next is
    wasted work); I1 or I2 failed, the largest term, the least memory `M*` (`M*` admits, `M* − 1` does not), the
    largest uniform cap that fits; each remedy from its part's owner — the carrier's knob in its own argument (the
    REST egress's `with_ports` is a total it divides), the plan author's (`answer_cap` is the plan's: a new dry run,
    and the largest body by operation owed), the operator's memory; a cap derived from the stated memory offered, as
    decision 20 offers its derivations.
  - **The stated memory checked against the process's limits** at stage 1: the least `memory.max` over its cgroup's
    ancestors, a stated memory past it refused; `max` journaled with its remedy (a scope with `MemoryMax`, which also
    confines oomd's kill to the harness).
  - **The evidence**: the header's `admission.memory` (journal version 10) — the stated bytes and origin, `basis:
    counted-heap`, each term's count, cost and `enforced-by` (cap, channel, gate not built, measured, host-stated), the
    constants as values, the slack — and, in the same version, a periodic `memory` line (the process's resident and
    high-water bytes, the cgroup's current and events, the live counts of sockets, attempts, cells, captures), so a run
    the OOM killer took still shows what grew.
  - **No launch-scale run before the read gate**: until K2b-7e-3 the attempts' term is predicted, not held; the first
    host run (`sources = 1`, a correctness run) inside a scope with `MemoryMax`.
  - **The answers charged statically first**: each socket holds at most one answer (HTTP/1.1), so their charge is the
    sockets' cap times the answer cap's — never a wait; a budget that waits only when a real plan is refused by it;
    with the read gate, the lesser of the cap and the pool's handles.
  - **Amended for K2b-7e-2b-3** (2026-11-27, under the gate: the main position, a blind solver and a discriminator on
    Fable, three roles — Software Design, Verifiability, Operability — on Opus; § Alternatives Considered keeps what
    each refuted). **One pure `memory::fit` binds everything it prices** — the plan, the carrier, the journal, the
    read-ahead, the stated memory, the process's limit, by value — into a `Fitted`, the only way to `Live` and to
    `admit(fitted, &runtimes)`, which checks the flavour, the descriptors and that what was built is what was priced
    (a host fitting one journal and running another was this ADR's bug class twice). **Three phases, the largest
    sum**: `need = max(F + B, F + Gt, F + R + P)` — fixed (the reader, the decoder outside the counted heap, the
    runtimes as they will be built), stage 2's judging, stage 3's probes and a gate's client, the run's (each source's
    costs and keeping, the journal, the sockets and their answers) and the pool at the one κ the header's catch-up
    uses; admission decided by the slack's checked subtraction itself; the binding term the largest in the phase that
    binds. **The pool's count `late`** (it holds pending: `pending + late` charged them twice) **and its records at
    `late_shape`**, replacing the pending records' term — the records of steps ended while a handle holds them were
    priced by nobody. **Each part's bytes stated by its owner, apart**: the shell's attempt share and its copy of the
    captures (`run`'s); the carrier's by byte class (a target percent-encoded and copied into the URI, headers, a body
    and its clone — one coefficient over the summed request was exact only for bodies) with the body's render sized at
    once and hyper's head buffer, which never shrinks, charged to the connection at the plan's largest head; a host's
    `Egress` its connection's own heap (TLS), its `Decorator` a bound held where its headers enter, its gate's client —
    each required, unstated refused, never left no room. **The term set complete by construction**: a closed `Part`,
    a sensitivity property (a cost δ raises the least memory by count·δ in its phase), every library part naming its
    two-sided row, the model a value `fit` takes and the header journals. **The refusal** collected, each remedy on
    its owner's knob through the map it states (`Egress::caps`, not a uniform cap `with_ports` would split), the least
    memory also at the needed caps, and stated "predicted, the pool unheld" until the read gate. **The limit a process
    fact** (`Limit::read`, the least `memory.max` and `memory.high` over the ancestors), and the ADR's own "the first
    host run inside a scope with `MemoryMax`" executed: a run whose pool nothing holds refused unless its own cgroup
    has a finite limit. **Version 10**: `admission.memory` required, the fold and `verdict.py` rechecking its
    arithmetic and refusing tampered headers; the `memory` line's form fixed with it (anon, file and sock apart, heap
    in use and free, the limit as read), sent through a one-slot mailbox the writer drains first, and read from a
    journal the kernel cut. `fits::fits` and `Budget` retired, their loop carried to `fit`.
  - **Amended for K2b-7e-2b-3d-3** (2026-11-28, under the gate: the main position, a blind solver and a discriminator on
    Fable, two roles — Verifiability, Evolution — on Opus; § Alternatives Considered keeps what each refuted). **The
    fold rechecks admission's aggregation and its cross-references, never its prices**: it has the journal, not the
    plan's peaks, and several costs are owners' statements. A term is `count · cost + rest`, checked, its count the
    enforcer's (0 for an absent part, 1 for a part held once), its cost refused on overflow whatever its count; the
    parts gain `Pending`, `JournalOut`, `Decorations` and the probes' io, each labelled by who states it (Connections
    and Answers by the carrier's own word, not the library's); `Planned` holds each shared count once and a closed
    `Model` of the constants the fold binds where a cost is one verbatim; κ journaled once, as `catch_up`, from one
    call, a reserve of 2000 or more refused by name; the limit's and the runtimes' origins journaled apart, the
    runtime-events check exempt on the runtimes' own; `GateBuilt` uninhabited until the read gate mints it. **Version
    10** carries `admission.memory` alone — the `memory` line enters with its producer, K2b-7e-2b-3e, as decision 14
    says each line kind does —; `Reader::new` reads the version first; the recheck is versioned, `recheck(&Header)`
    dispatching on it, version 10's rules and its part table — a data file the Rust fold, `verdict.py` and load-run's
    `terms()` are held equal to — frozen when 11 lands; a pure `admission(fit, rows, limit)` the only producer of
    `Admitted`'s admission, calling the recheck; the fold voids an incoherent header; the tamper census as twins with a
    closed `why`, `verdict.py` counting each rule reached; consistent headers' results pinned by version, refusals by
    (version, fold revision), append-only, a CI step failing on a modified line whose key did not move; a few headers
    made by load-run pinned and settled at every change; the fold's revision the next free one, 8 — decision 21's
    becomes the next after. Executors two-sided: each owner's count tested, the shadow exact for budget, pool and
    unheld with a κ that does not divide, a property editing one field of a consistent evidence and refusing it unless
    the field is on a pinned free list.
  - **Decided for K2b-7e-2b-3d-4** (2026-11-28, under the gate: the main position, a blind solver and a discriminator on
    Fable; § Alternatives Considered keeps what each refuted). **The whole-run measurement is a set of differenced rows,
    not a total**: one process-wide counter (a moved block counted at both sizes, a live count) in an ignored test binary
    of its own, run by hand one row a process, zeroed before the plan is opened — its reader fills its channel before
    the fit —, its peak taken at the edges the stages' calls give; each phase's peak compared with the sum of its
    heap-basis terms read back from the header it journaled, never priced again; what the counter cannot see read
    apart (`mallinfo2`'s in-use less the counter: zstd's C side allocates through libc). Each row reaches the state it
    prices — the guard: its realised counts from its journal, below 0.9 of the priced a row that tests nothing —,
    states its prediction in this record before it runs, and has a mutant of the constant it prices (`CONNECTION`
    halved, `HEAD` dropped, `ATTEMPT` cut by a third) that must turn it red in a scratch build. The rows: a control;
    answers held — the stand holding every body's last byte until all are out, then releasing them one a source, so
    that each source judges while the rest are held —, at two counts for the per-answer slope; attempts never answered
    at two counts and two body sizes, for the per-attempt slope and the late records' coefficient
    (K2b-7e-2b-3a-3's owed row); sources one and four; the decoder with and without zstd; the kernel's sockets with the
    reads withheld. The process is reported beside them as factors, not verdicts — resident high-water, anonymous huge
    pages, the threads' stacks, glibc's arenas; THP off by `prctl` and on by glibc's tunable, each confirmed or the row
    void; resident can read below counted (bodies are reserved at the cap). The run sits in a scope of its own without
    swap, the stand in another; no row lets the cgroup judge the need until the sockets are measured, since the
    kernel's queued bytes would make it undiscriminating. **No verdict on κ**: nothing paces the run, so a stalled row
    measures the backlog against the stop's length, below the deadline plus the route's timeout — evidence for
    decision 21's direction either way.
- **The read gate** (K2b-7e-3): no line read while the attempts alive reach their cap or the machine's live cost its
  share — a late read, the harness's; starved past the longest timeout, a fault and a stop.
- **The parse bounded** (K2b-7e-4): the body's state journaled (whole, cut at the cap, unparsed and why) — an answer
  the harness could not parse is the harness's, never `Unexpected`; the allowance structural (nodes, string bytes,
  depth), its twin in `json.py`.
- **Cut**: K2b-7e-1 the constants; K2b-7e-2 admission — K2b-7e-2a the costs stated and held (and `fits` re-pinned, its
  executor's margin found an under-count); K2b-7e-2b in three: **2b-1** the plan's shapes and their prices (every
  field K2b-7e commits, in one revision: the shapes' peaks, pending by source and with ends a jitter later, the
  largest line and chunk; the pricing function and its executor — equality with a decoded record's heap under a
  generator of records, each field's term zeroed red; the scheduler's executor two-sided, with dependencies), **2b-2**
  the reader's, the journal's and the sources' terms by construction (`ahead` and `behind` by type, the line limit
  before its allocation, zstd's window, events by `Runtimes`, the answered tail with no await), **2b-3** admission
  (`memory::admit` over the stated parts, every refusal at once, the cgroup's check, the header and the `memory`
  line, journal version 10); K2b-7e-3 the read gate; K2b-7e-4 the parse; K2b-7e-2c the cgroup's memory as the
  stated number's source — after the gate and an RSS measurement (deriving it from `memory.max` first would make the
  kernel the judge of an I1 nothing enforces, and that counts heap, not the process); K2b-7e-5 a waiting budget, if a
  real plan needs it.

### 23. An answer is two facts: the system's arrival, the harness's judgement

*Decided* (2026-11-27, under the gate: the main position, a blind solver and a discriminator on Fable, three roles —
Distributed Systems, Performance, Verifiability — on Opus; after a measurement on the load server, § Measured "the
judging floor"; § Alternatives Considered keeps what each refuted). Steps 1 and 2 build decision 22's K2b-7e-4; step 3b
waits for the owner's confirmation:
- **The problem.** While any source's answer is judged, the loop takes nothing — the judging floor, opened at the
  answer's arrival, lifted by its judgement's push. It is the whole of decision 21's shift on the load server: at
  256 KiB and five large answers a second about 450 ms in 30 s, at twenty a second the 500 ms cap with thousands never
  handed; 64 KiB nothing at any rate; the hand-off never late — the floor masks the source's thread, which judges
  synchronously. The floor exists only so that a push backdated to the arrival is not `Below`: one input, the answer
  at A, carries two facts known at two instants — the bytes were here at A, the judgement is X at T.
- **The structure**: each consumer reads the instant that is its own. **The pace reads T**: a dependent's ready, the
  step's next action, a hand-off's raw. **The excusal and the licence read A**: the in-time check, a poll's grid point
  and a resend's due and licence (dispatched at `max(due, T)`, the difference the harness's dilation), an observation's
  cause lateness (the dilation added along `critical` into the reader's), `found`. A pace that read A everywhere shifts
  the run at κ = 0 (a dependent handed at T past its raw at A moves V); one that read T everywhere skips a poll's grid
  point, loses a resend's licence, and leaves an observation's cause lateness unexcused — three false Fails.
- **Step 1 (K2b-7e-4a)**: a body parsed on its first reader — the problem code of a status at or past 400, a predicate,
  an `until`, a capture —, memoised, never decided before (a static laziness passes a gateway's 404 problem against an
  expectation of none). A late answer opening no floor moved to step 3b while building: without its floor, a late
  answer's push at its arrival is below what the frontier released, and pushed at its judgement it changes the
  journal's instant — the two inputs' change, not a journal-neutral one. Executors: a counting allocator (no tree for a body nobody reads,
  one within `json::bound` for one reader), a lazy-against-eager differential over stratified answers comparing the
  whole judgement, a shell test of a late answer's judging not holding an independent step; the floor measurement
  rerun, refutable (the shift at most the 64 KiB row's), and its large answers as polls with `until` (what laziness
  cannot do for host-shaped plans).
- **Step 2 (K2b-7e-4b)**, decided in its own round (2026-11-27: a blind solver and a discriminator, Performance and
  Verifiability; § Alternatives Considered): **a tape**. The tree's cost is allocation — per number three strings, per
  member a name cloned into a set: 4.2 ms at 64 KiB of numbers and 306 ms at 4 MiB on the laptop, two nodes a byte. The
  round refuted this step's first sketch twice: a subtree budgeted by its literal's nodes does not bound its bytes
  (`Equals("1")` against `1.000…0` of 4 MiB is one node, megabytes, and equal), and a pointer set registered for one
  pass makes the judgement depend on what was registered. The rules:
  - **One grammar, two sinks.** The parser generic over a private sink — the tree (`parse`, its acceptance and values
    unchanged; literals, fills and the validator use it) and the tape —, its string decoder generic over what it feeds
    (a string, a name's hash, a comparison, nothing). Validity is one implementation for both.
  - **A name twice, in the grammar.** Per open object, `(FNV-1a of the decoded name, start)` on one stack allocated at
    the first `{`, sorted by hash at the object's close, names compared decoded only inside a run of equal hashes (the
    run sorted decoded: n log n at worst). `json::bound` gains the stack; a scalar — a hole's value — allocates none.
  - **The tape.** One entry a value, its start and its next sibling's index (`u32`, `u32`), sized once from the body's
    length (at most `n/2 + 1` values). A body past `u32::MAX` bytes reads as unreadable, and the validator refuses an
    `answer_cap` past `u32::MAX` (rules revision 15): the one exception to "every judgement today's", reached only by
    a host's carrier past its cap.
  - **Validity, not error-exactness.** The judge reads `parse(b).ok()`; which of two errors is named first may differ.
  - **The readers read the tape; their signatures, order and errors are today's, the judge unchanged.** `at` is
    today's walk; `code` a string at an object root, sized at once; `take` decoded only to `TAKEN + 1`; `Equals` a
    walk against the want, its objects sorted in place once and searched; numbers by `same` over the text; `Contains`
    each element.
  - **The bound:** the tape; at its build the names' stack with a predicate's want alive (it is parsed before the
    first reader builds); after it one reader's holding, the code alive. About 6 × cap against 128 × cap.
  - **Not changed now:** the literal's per-answer parse (§ Owed, its row in M-a), stage 2's judged record and
    `Judging.shape`.
  - **Executors:** the tape against the tree by value — numbers by their text, strings decoded, containers
    recursively — for every pointer, over generated and mutated documents with escaped and non-ASCII names, a colliding
    pair and the densest documents; `vectors/json.py` on the tape's validity; twins (`{"m9tcqis_":0,"i7giss29":0}`
    readable, a pair whose FNV-1a collides, and unreadable with its first name again; `{"a":1,"\u0061":2}`
    unreadable); the readers against a frozen copy of today's tree readers — lazy against eager over stratified
    answers, step 1's differential, not built at K2b-7e-4a —, the copy pinned by digest; `same` and the walk against
    the tree's equality over pairs both ways and NUMBERS × NUMBERS; hand rows; a counting allocator two-sided over
    documents and records; the `u32` rows.
  - **M-a** in the scheduler's tests: read, judge and drop timed together, 64 KiB to 4 MiB, shapes and readers as the
    roles named; its predictions and refutations written into § Measured before it runs.
- **Step 3a**: `sim`'s scripted judging delay under today's floor (the machine unchanged, its zero-delay digests equal)
  — a baseline for step 3b to settle against, not to re-pin.
- **Step 3b**: `Arrived` at A (carrying an early reply's `Sent`, cancelling the backstop), `Judged` at T (carrying A),
  a late answer sending no `Arrived` and holding nothing;
  the floors, the drain's cap and `held` gone; the poll grid and the resend from A; a skip at the later of its cause's
  end and its own read; the dilation as `ready` less the latest dependency's arrival; `Held` from the oldest arrival;
  coherence `sent ≤ arrived ≤ at`, a taken answer arrived by its deadline, a skip's provenance recomputed from the
  journal; journal version and fold revision bumped. Its independent executor: the counterfactual pair — every corpus
  log and script run at a judging delay and at none, a step rooted at the delay and not without it late by at most J
  between the two.
- **Step 4**: the judge off the source's thread, only if measured necessary — the send side and the receive side (the
  stand's write instant against the arrival) at 4 and 16 sources, judgements of J, 2J and 4J, the replayed shift at
  most 1 ‰ of the run —; then admission states the judges' load at the plan's peak large-answer rate per source, a
  live saturation stop, and a connection checked in only after its judgement.
- **Not decided here**: decision 21's κ; whether holds within J sum when they abut (a stand of Poisson arrivals, the
  holds journaled).

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
- **Decision 22, K2b-7e-2b's admission** (2026-11-24): the main position's **two shares** — an attempts' cap `A`
  derived as the largest that fits, the machine's share `fits(peaks)` (one resource taken by one read: a gate on
  attempts alive bounds nothing; the discriminator) and its **largest line** as a record's bytes (a line's bytes are
  not its record's heap); the synthesis's earlier **`A ≥ 2·⌈pending·1000/(1000 − κ)⌉`** (its factor unexplained, its
  count the wrong one); the blind solver's **slots = alive + waiting** (an answered attempt's handle is dropped before
  its end is stepped: a wave of 503s lets reads past it; the discriminator) and its **captures refused** when the plan
  has any (every host plan captures an id); the discriminator's **`weight(&Record)` in `command-log`** (it cannot see
  the machine's clones, and a model's output committed freezes its revision; Software Design), **a constant per
  attempt** (an attempt holds its frame, the request's copy and its captures'; Software Design, Verifiability),
  **captures journaled unenforced** (an enforcer exists, `TAKEN`; Operability) and **no κ in I2** (it would amend
  decision 21's decided I2 silently; Software Design, Verifiability); **the cgroup as the number's source before the
  read gate** (the discriminator); **the first refusal only, one binding term, remedies from another owner**
  (Operability, Software Design, Verifiability).
- **Decision 22, K2b-7e-2b-3d-3's journal** (2026-11-28): the main position's **bytes = count · cost for every term**
  (nine parts are sums or polynomials; the discriminator), **an open map for the `Model`** (the schema checker's key
  order refuses it; the discriminator), **admission required whenever T0 exists** (it would void every golden run and
  the reference corpus; the discriminator), **tampered twins in the byte-form golden** (it is never folded; the
  discriminator); the solver's **a cross-check `BoundTo = Pending`** (two variables, fixtures differ: a false refusal;
  the discriminator) and **a sample consistent and extreme** (unneeded; the discriminator); the synthesis's
  **`recheck(catch_up, &Admission)`, unversioned** (a later reader cannot judge a first-host-run journal by its rules;
  Evolution), **the `memory` line in version 10** (no producer and no reader for two of its groups; Evolution), **κ
  journaled twice** (Evolution), **the limit's origin ORed into `assumed`** (the runtime-events check then refuses a
  test's runtimes or is exempt everywhere; Verifiability), **the fold's revision 8 taken silently** (decision 21's
  promise; Evolution), **pins keyed by the journal's version alone** (a fold's rule over unchanged bytes forces the
  wrong bump; Evolution), **executors run on admission's own output** (consistent by construction: no rule-deleting
  mutant dies; Verifiability), **a `compile_fail` doctest without its code** (any error passes it; Verifiability).
- **Decision 22, K2b-7e-2b-3d-4's measurement** (2026-11-28): the main position's **totals and a stated band on them**
  (a floor on the total shares the sentence it checks, ADR K2b-7e-2b-3's synthesis; slack in one term absorbs another's
  under-price — 2.6 MB of the decoder alone; the discriminator), **the pool's peak against its prediction at κ** (the
  pool is unheld by declaration and nothing paces: the result is fixed by arithmetic; the solver, the discriminator),
  **`tests/tcp.rs`'s stand and `tests/stall.rs`'s late clock** (neither is what the position names; the
  discriminator), **THP on and off by `prctl` alone** (under `madvise` the two are one row; the discriminator), **the
  cgroup's current less the resident set as the sockets' memory, at most their defaults** (autotuning grows them; the
  current carries page cache, stacks and slab; the discriminator), **swap left on** (the high-water under-reads at the
  limit; the discriminator); the solver's **the cgroup as judge of the need** (the kernel's queued bytes make it come
  true either way; the discriminator), **two body sizes alone for the attempts** (they give the record's coefficient,
  not the per-attempt constant; the discriminator), **`tests/memory.rs`'s counter** (its `realloc` counts the
  difference only, the defect that once hid a 30 % under-count; the discriminator).
- **Decision 23's step 2, the tape** (2026-11-27, K2b-7e-4b's round): **a pointer set registered for one pass**
  (the main position, the solver: about 1.6 × cap — invisible to the fit, a source's answers charged at sockets × cap;
  the judgement then depends on the registration, which needs a fallback pass or a narrowed API; the discriminator),
  **comparators listening inside the pass** (every want alive at once, raised before `code` has the fills; the
  discriminator), **a resumable scan fed while the body is read** (it moves the work off the floor, not off the thread;
  it holds a scan across awaits; step 3b removes the floors; the discriminator), **a subtree budgeted by its literal's
  nodes** (unbounded bytes; both), **or by its normalised size** (an allocation an element under `Contains`; the
  discriminator), **names in an open-addressed index** (a rehash in one piece, quadratic under collisions; the
  solver's own failure), **names sorted at the close by their decoded text** (the body read in every comparison; the
  discriminator), **names checked per sink** (the riskiest rule twice; Verifiability), **judging off the thread first**
  (the floor stands whichever thread judges; the discriminator), **an object's `Equals` by a linear find** (quadratic:
  minutes at 4 MiB; Performance), **a frozen judge** (a copy of `judge.rs` that drifts; equal readers give an equal
  judgement; Verifiability).
- **Decision 23, the judging floor** (2026-11-27): the main position's **answer pushed at its judged instant** (an
  in-time answer read late at the machine's own check: a false Unknown; the solver, the discriminator) and **its
  admission check, judging at the cap within J** (admission states, it does not certify: decision 21's second round);
  the solver's **end dated at the arrival throughout** (a dependent handed at T past its raw moves the pace's V: the run
  shifts at κ = 0; the discriminator) and **its parking and reader's floor** (the fold never asks which failure was the
  earliest); the synthesis's **the harness's instant for every next action** (a poll's grid point skipped, a resend's
  licence lost, an observation's cause lateness unexcused — three false Fails; Distributed Systems, Verifiability),
  **a laziness decided from the record** (a gateway's 404 problem passes an expectation of none; Distributed Systems,
  Verifiability), **an oracle at zero judging delay** (it reaches none of the non-commuting sites; Verifiability), **the
  delay added to `sim` with the two inputs** (a re-pin, not a settling; Verifiability), **judge threads joined at a
  stop** (a stuck judgement hangs it; Verifiability), **M-c counted at one source** (the pace is global: sixteen sources
  shift as one does; Performance). Laziness alone was not the remedy: a host plan's large answers are polls whose
  `until` reads them (Performance).
- **Decision 22, K2b-7e-2b-3's admission** (2026-11-27): the main position's **pool counted `pending + late`**
  (`late` holds pending; the discriminator), **the reader after the stages** (it is spawned and filled before them),
  **a constant attempt price** (the ADR priced it by its record), **no I2 term for memory**, **one refusal**, **no
  limit read**, **`basis: counted-heap` for every term** (zstd's context is outside it), **`Live::from_admission`**
  (circular: `Admitted` needs the built runtimes); the solver's **pool on top of the phases' maximum** (the pool is
  the run's), **a two-cause refusal before the build** (the descriptors are read after it; a pre-read is a lower
  bound), **remedies by search over the carrier's header** (no knob there), **a hand-bumped `MODEL_REVISION`**; the
  synthesis's **`admit` taking the plan, carrier and journal again** (one fitted, another run; Software Design),
  **`late_shape` added beside the pending records** (charged twice; Software Design, Verifiability), **one
  `stated_by` for a carrier's attempt with a host's decorator inside**, **the egress's TLS inside the library's
  connection** (Software Design), **one coefficient over the summed request** (exact for bodies only: encoding,
  the render's doubling, hyper's head buffer; Verifiability), **the binding term across phases** (Software Design),
  **κ on part of the pool**, **a forgeable `Limit`**, **an unstated gate client unheld and charged nothing**
  (Software Design), **a uniform cap as the remedy** (`with_ports` splits it; Operability), **`answer_cap` without a
  number** (computable once `HEAD` is apart; Operability), **the scope rule left to the owner** (the ADR decided it;
  Operability, Verifiability), **`unheld` surfaced only by a finished run's report** and **samples through the main
  channel** (a killed run's journal is read by nothing, a blocked machine starves the samples; Operability), **a
  costs row building its own handles** and **a whole-run floor on the total** (each shares the sentence it checks;
  Verifiability).
- **Decision 22, K2b-7e-2b-2c's sources** (2026-11-27): the main position's **parse at its measured worst over stage
  2's shapes** (chains, the densest input, are no shape; it under-charged 2.7–3.9×; the discriminator), its **head in
  transition omitted**, its **`SOURCE` holding the scratch** (a carrier's, allocated at a thread's first read — an
  executor at build cannot see it) and its **runtime measured idle as its lazy part** (a look's own is the
  attempt's); the synthesis's **`scratch + max(head, tail)`** (REST's premise, not a type; Software Design), **`kept`'s
  rebuild in `judge`'s price** (the shell's, whatever carries it; Software Design), **a first-byte scalar check**
  (whitespace bypasses it; Software Design, Verifiability), **the source costs inside `Needs::Sockets`** (a socketless
  carrier that judges has none; Software Design), **charged over the runtimes built** (an idle runtime judges nothing;
  Performance), **`EVENT` 16 B elsewhere** (kqueue's and IOCP's are larger; Software Design), **`Live::start` gated as
  the remedy for allocating before admission** (`for_caps`, the production path, does the same; owed to 2b-3);
  **a validator refusing a literal past the cap** in place of a role (it doubles every source's tail); **one judging
  thread for the process** (one tail in all, but bodies would queue unbounded; the discriminator).
- **Decision 22, K2b-7e-2b-2b's journal** (2026-11-26): the main position's **reader's mirror** — `JournalSink::behind()`
  and a check in `run` (late: it fires after the stages touched the stand, the journal's header written and left
  with no trailer; self-declared: a host's sink states its own count; the discriminator) —, its **`Ended.bound` by
  the captured holes** (it is the step dependencies' lines, `machine.rs:655`), its **`after` taken as exact** (through
  a flatten its capacity is four for one to three dependencies) and its **`BufWriter` in `Written`** (it holds
  finished lines across a kill; the discriminator); **a brand token** handed to the host (a second admission makes a
  second); **`Admitted<C, X, J>`** owning a sink factory (a type parameter through four types for one call site);
  **`Journal` in `Runtimes`** (where a run runs, not what it writes); **`Shape.bytes` as the barriers' keys' bound**
  (loose by a record's body: 64 MiB for a 64 KiB bulk import at `behind` 1,024; the blind solver); **a const assert
  on `W`'s size** (a `compile_fail` doctest on stable checks no error code, and a post-monomorphisation const is not
  evaluated by a doctest that only compiles; Verifiability); **the boundary checking lengths and equality** (the
  price is a buffer's capacity: a value of 10 bytes in 64 KiB, an equal string with spare capacity; Software
  Design, Operability) and **in order** (the machine looks a capture up by its subject and slot, and two specs of
  one slot are legal; Software Design, Operability); **overflowing codes folded into `other`** (`Code::Other`'s key;
  Operability, Software Design). **A channel of encoded bytes bounded in bytes** — its term a constant and one line —
  is set aside, not refuted: its encoding on the machine's thread, against decision 16's reason for the thread, is
  unmeasured (§ Owed).
- **Decision 22** (2026-11-24): the main position's **exact read size with a bounded write buffer** (hyper's setters
  unset each other); **reading straight into hyper's buffer** (only `unsafe` advances it: `forbid(unsafe_code)`); **a
  body budget reserved as bytes are read** (two chunked answers each holding part deadlock to their deadlines);
  **the attempts open bounded by the plan** (the prediction relabelled soundness: a stall's backlog, decision 21's
  gate); **TCP's flow control holding the rest at the stand** (the receive buffer autotunes to tens of MiB, charged to
  the process); **"1.6×" as the parse's factor** (the per-attempt peak, the connection inside it; the worst shape,
  singleton nesting, some 100–130× counted); the solver's **socket buffers fixed** (Operability: the kernel doubles
  and clamps them, a slower reader than real clients at a high bandwidth-delay product, a load the gateway feels)
  and **its budget charged by the plan's pending to every source** (Operability: four sources, four times what a run
  can hold; the default egress's 28,232 ports some 37 GB at a 1 MiB cap).
- **Decision 21's raw dues** (2026-11-24, refuted while building and in their own round): **`intended`** (a dependency's
  shift counted twice, a covered dependent compressed); **intended less the least shift** of the dependencies (right
  only at κ = 0: under a catch-up a drained shift returns to the run); **plan-time twins** τ with waits dilated (a
  covered dependent made tight by the harness compressed onto the open plan — a false Fail; the trilemma above,
  § Measured). The common premise of all three: that a late harness should catch up (κ > 0) — catching up across
  causal chains and the open plan cannot be certified.
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
| K2b-7e-1 | K2b | a head of 16,384 bytes parsed, of 16,385 or of a hundred and one headers the run stopped naming `rest.head`; a forged length of 2^62 unread at its head; over a transport not vectored the first write the head alone, the body queued after it; after a burst of tasks ended and one aborted, no handle kept; one builder in the shell, no free handshake, no `max_buf_size` (the lint) | the exact read size dropped; writes flattened; a head too large left `Broken`; a length past the cap read; the body sized from any length |
| K2b-7e-2a | K2b | `tests/costs.rs` (a binary of its own: an allocator tagging each block with its owner, a moved block counted at both sizes, a pipe of rings the stand allocates, `sim` subtracting the machine at the same states, two sizes just past powers of two): the shell's share an attempt, a connection idle, an attempt beside it, each within `[80 %, 100 %]` of what is stated; a body held whole at its length, or at the cap with no length, within a KiB of it while its first part is read; the worst head within `[85 %, 100 %]` of `HEAD`, every answer within its charge; the scheduler's `tests/costs.rs` holding a burst of 32,769 host calls and of REST reads at one instant, a REST read not ended and an ended step just past powers of two within what `fits` counts | the exact read size dropped; a declared body not sized; a body of no length grown by doubling; the head's parts kept through the body; an answer charged its body only; `fits`' live cell as it was; a finished task's handle kept (its unit twin) |
| K2b-7e-2b-1a | K2b | `command-log/tests/shape.rs` (a counting allocator): over 512 records generated across every field, the bytes a record decoded from its line holds in its `Arc` equal `shape::heap`, at most `shape::bound` of its shape and at least a third of it; a record of no sequence priced exactly; every record type taken apart whole (a new field does not compile uncounted) | a predicate's pointer not counted; serde's first four slots forgotten; a sequence bound at its elements once; byte strings' padding left out; base64's estimate taken as the length; the `Arc` left out; a capture's source not counted |
| K2b-7e-2b-1b | K2b | `vectors/reference.py` folding each line's shape over the pending, late and held sets by definition and agreeing with the validator's byte peaks on every corpus log, deriving a shape's key, captures and source itself, refusing a corpus that reaches no late step, socket, held capture or captured hole; the commitment's largest line and chunk against a log of 19 steps in chunks of four; the validator's identity at revision 10 | a step past its end not kept late; the late kept past the jitter; what the window drops kept still; what the held let go kept still; a source's sockets never let go; a pending shape never let go; the barrier keys' bytes not counted; the largest line not taken; a chunk's bytes carried past its prefix |
| K2b-7e-2b-1c | K2b | the scheduler's `tests/costs.rs`: thirteen rows — ended in the window, not ended, bursts of host calls and REST reads at one instant, REST updates of 4 KiB bodies, barrier keys of 1 KiB labels, writers keeping captures, captured values sent back, a step awaiting the 65 before it — each within what `fits` counts of its counts and shapes, the rows a constant or a price is pinned on at least 70–80 % of it; `fits`' boundary pair and every count of a shape and of what the held keep raising its need; the judge's golden: a number of 8,192 digits taken, of 8,193 not, its twin in `vectors/judge.py` | the records' shapes, a captured value, what the held keep, the barrier keys' bytes not priced; a live cell as before the shapes, doubled; an ended cell a quarter lighter; a captured number taken past `TAKEN`; (survives: a dependency's binding not priced — owed) |
| K2b-7e-2b-2c-3 | K2b | `command-log`'s doctests: a `Read` sent between threads does not compile, one read where it is made does; `load-run`'s `stamps.rs`: keeping an answer's captures holds exactly what `run::ending` states of its record; `tests/shell.rs`: a value no take makes — an array behind whitespace, an escape no take writes — a carrier fault, the run stopped, its answer never journaled; `tests/judging.rs` (a counting allocator by thread): each shape's document at the cap no larger than the cap, stage 2's judging within `stages::judging_bound` at caps of 4,097 and 65,537 | a read sent between threads; a document grown; the keeping's slots, its names unpriced; a value no take makes kept (survives: the stage's document unpriced — its executor one-sided) |
| K2b-7e-2b-3c-2 | K2b | `load-run`'s `tests/fit.rs`: over a run whose parts follow its knobs, each knob's remedy the largest of it that admits the run and one past it refused, the memory's at the least, the least at the needed caps the floors' ports'; no remedy below a knob's floor, none where the floor itself does not fit; the binding pool's slice named; `tcp.rs`' own: a total's caps as `with_ports` divides it; `scheduler/tests/bound.rs`: the need rising with each count, a read-ahead past the header's the machine's refusal | the bisection a step past; a floor that does not fit searched; the ports searched from none; the least at the needed caps the given; the read gate's slice unnamed; a bound source sharing |
| K2b-7e-4a | K2b | `command-log`'s `tests/parse.rs` (a counting allocator by thread): an answer read for its status, its problem's code below 400 and no predicate holding no tree; its first reader one tree within `json::bound`; a second none; a problem's code at 404 read from its body; `scheduler`'s `tests/judge.rs` against the independent vectors, a gateway's 404 problem against an expectation of none among them | parsed always; parsed never; a problem's code read only once parsed |
| K2b-7e-4b-1 | K2b | `command-log`'s `tests/json.rs`: every twin read by the tape as by the tree, by value; names one by their decoded bytes — a pair of one FNV-1a hash two, a third equal to the first a duplicate past the other, escapes naming what they decode to, each object's names apart; the densest documents alike; `the_reader_agrees_with_python_s_json_and_decimal` with the tape's validity and every pointer's value against the tree's, names escaped, not ASCII and of one hash, each rule reached ten times or more; `tests/parse.rs` (a counting allocator by thread): a tape's entries sized once, exactly, zeros of odd length filling them; the names' stack sized once at the first object, exactly, beside the tree; a document past `LONGEST` unreadable holding nothing | a name twice unchecked; one hash taken for one name; a run compared by adjacency alone; names compared raw; a child's names left on its parent's; one level deeper; the tape one entry short; the names' stack a sixth; a container's end unpatched; an element one further; a member's value skipped by one; the tape's length unchecked |
| K2b-7e-4b-2 | K2b | `command-log`'s `tests/readers.rs`: a frozen copy of the eager readers and the tree's parser (`2679bb9`), pinned by digest; the readers on the tape against it over 4,000 generated answers read in two orders — every code, every predicate's verdict or error alone and together, every capture —, each stratum reached ten times or more (equal by value written apart, unequal, contained, not, a pointer, a literal and a hole wrong, an unreadable body read, a code, a code at a root not an object, a string of `TAKEN` bytes taken, one past it refused, a number taken); hand rows; an object's equality of 20,000 members within two seconds unoptimised; `tests/json.rs`: the tape's equality against the tree's both ways over the generator's pairs and every pair of numbers, `Contains` beside them; `tests/parse.rs` (a counting allocator): a want alive while its reading builds the tape and its names' check, exactly; the code sized at once; each term of `answer::bound` binding — the names' check under a small want, the code kept beside a pointer's densest tokens —; a second capture of a long string holding no more than `TAKEN`; `load-run`'s stage 2 at 80 % of its bound or more past 64 KiB; `tests/validate.rs`: a cap past `LONGEST` refused, `LONGEST` admitted, a capture's malformed pointer refused (the corpus's `capture-pointer`, `log-0/capped`), rules revision 15 pinned, `vectors/reference.py` agreeing | trailing zeros kept in `same`; a zero's sign kept; an object's count ignored; a member found by a walk; contains all; an array's length ignored; a string grown; the want unsorted; the code off any root; a capture decoded whole; the names' term dropped; the code's term dropped; a cap past the tape admitted; a capture's pointer unchecked |
| K2b-7e-4b-4 | K2b | `command-log`'s unit test of the comparisons piece by piece against the decoded strings' order — a prefix the lesser, an escape by what it decodes to, a run split by an escape —; the differentials of K2b-7e-4b-1 and K2b-7e-4b-2 unchanged; `tests/parse.rs`'s long string a first run of `TAKEN` bytes and escapes after it, refused with nothing decoded; `tests/validate.rs`: a literal of `TAKEN` bytes admitted, one past it refused, the corpus's bad predicate with a literal past it, rules revision 16 pinned, `vectors/reference.py` agreeing | a prefix taken for its whole; the same, both decoded; a chunk taken one further; a length counted short; a run past a control byte; digits never at the end; a literal past `TAKEN` admitted; a literal of `TAKEN` refused |
| K2b-7e-4b-5 | K2b | `command-log`'s unit test of the comparisons, its escapes restored — `\u0061b`, `a\u0062c`, `\u00e9`, a surrogate pair, an escape after a plain prefix —, each comparison over the text and piece by piece against the decoded strings' order | a plain string's end taken for equal; a plain mismatch reversed; an escape compared raw; the shorter of two plain names the greater; two names compared raw past an escape |
| K2b-7e-2b-3d-1 | K2b | `load-run`'s `tests/fitted.rs` (a counting allocator by thread): the runtimes `Live::fitted` builds admitted, another fit's refused naming its source; two sources past their caps and the memory refused at once, none nested; a fit at a cap of `i32::MAX − 2` refused on its runtime's term with nothing built; the least memory named admits and a byte less does not, the journal's lines behind a remedy; `admit`'s doctests — a `Fitted` compiles, a `Plan` does not; `tests/descriptors.rs`: a runtime's own three descriptors, the fit refusing one below the bound before anything is built and admitting at it; `tests/rest.rs`: a cap past a driver refused by the fit, each source past its cap named | the fitted runtimes unchecked; the first refusal alone; a runtime's descriptors uncounted; the memory ignored; the runtimes built for no cap; the journal's behind frozen; a cap past a driver unrefused; I1 unchecked |
| K2b-7e-2b-3d-2 | K2b | `load-run`'s `tests/fitted.rs`: the cgroup tree walked over a temporary tree — every level `max` no limit, the first finite two up, a lesser one nearer, the leaf's own, a greater root changing nothing, `memory.high` apart and the lesser of both —; a memory at its limit admitted, a byte past it refused naming the file and depth; the scope rule — no limit and one a cgroup up refused, naming `MemoryMax`, the process's own admitted —; the carrier's ports a remedy within its floor, the least memory at the needed sockets the floor's, refitted at the remedy admitted and one past it refused; `tests/lint.rs`: `read` alone makes a `Limit` outside a test's, `Live::for_caps` a test's | the greatest limit kept; the walk stopped at the leaf; `memory.high` ignored; the limit unchecked; a memory at its limit refused; a scope one up taken for its own; the scope rule off; the ports floor zero; the knob's caps unused |
| K2b-7e-2b-3d-3a-1 | K2b | `load-run`'s `tests/fitted.rs`: κ the same in the fit and the header's `catch_up` at 1 and 999, a reserve of 2,000 refused by name; a limit's origin read, a test's tree and a test's assumption; the parts no enforcer holds, the gate's, each once in their order; `tests/fit.rs`'s property over `unheld`; `GateBuilt`'s doctests — `None` compiles, a forged one is `E0451` | the token's field public; `unheld` empty; a κ of 1,000 admitted; a tree's limit taken for an assumption |
| K2b-7e-2b-3d-3a-2 | K2b | `load-run`'s `tests/fit.rs`: a property over drawn counts, costs, rests, model constants, readers and staters — the need, the pool at κ and the budget the shadow's exactly, each term `count · cost + rest` with its phase and its stater the decision's, the unheld parts the pool's but the keys' and the bindings' —; every count raised moves the need as the shadow does; κ = 1 rounds a pool of 1,000 up to 1,002; a saturated cost at a count of 0, a pending cell below the ended one and an unstated event refused; `tests/fitted.rs`: each owner's count on a fitted plan (a source's cap, its `cap + 2` events, the journal's `behind + 2` lines, the cells held, the steps pending, the keys, the late steps — not the pending), the costs the library's, the journal's output a host's term, a host carrier's sockets its own word, a carrier's attempts its price by their count; `tests/stages.rs`: a replayed reader's `ahead + 2` records and its decoder; `scheduler`'s `tests/bound.rs`: the counts make the terms, with a jitter and without; `command-log`'s corpus: the late shape's records the late steps | the cells at the library's constant, not the model's; the pending at the live cell; the connections the library's word; the probes' io at the connection's; the decorations the carrier's word; a decoder without a reader; the journal's output in nothing; a saturated cost admitted at a count of 0; an unstated event admitted; the model not the library's; the late count the pending's; `bound_to` of the late shape; the decorations counted twice; the reader's records ahead alone; the journal's lines behind alone |
| K2b-7e-2b-3d-3b-1 | K2b | `scheduler`'s `tests/journal.rs`: version 8's header (no `catch_up`) refused as `Version(8)`, a later one with a field this reader lacks as its version, the same header at this version malformed; `verdict.py` refusing a journal of a version it does not read | the version read with the schema; the version read strictly (`deny_unknown_fields`); the twin reading another version |
| K2b-7e-2b-3d-3b-2 | K2b | `scheduler`'s golden header holding the memory's whole vocabulary — every part once in order, every phase, holder, stater and basis, a term of no source and of one, a limit present and absent —, the origins named; the golden journal and the 31 golden runs settled against version 9 line by line, the reference corpus's 4,932 journals run by run, its classes `(10, 5, 7)` equal to `(9, 5, 7)`'s; `load-run`'s `tests/fitted.rs`: admission's evidence its fit journaled term by term, a header carrying it read back as itself, each name journaled as itself; the assumptions apart in `tests/stages.rs`, `tests/tcp.rs` and `tests/sink.rs` | the need journaled the slack; the stated the need; a gate journaled built; an assumed limit journaled a tree's; the max the high; no part unheld; the plan's assumption the runtimes'; the journal's assumption the plan's; the io journaled the probes'; another model journaled |
| K2b-7e-2b-3d-3b-3a | K2b | `scheduler`'s `tests/recheck.rs`: the table its data file's; consistent evidence passing whichever phase binds, a source with no row before one with, a test's runtimes with another's events; 60 tampers, each refused by its rule's code, every one of the 16 codes reached; another version's rules refused; a header disagreeing with itself folded void at line 0, consistent read on; `load-run`'s `tests/fit.rs`: the evidence of any fitted run passing the recheck, a memory a byte below the need refused by the slack; `tests/fitted.rs`: admission refusing its own evidence off by a byte or with its unheld parts lost; every admitted run of load-run's tests passing the self-check; the reference corpus's classes `(10, 5, 8)` equal to `(10, 5, 7)`'s | each of the 19 refusal sites deleted; the phase, the basis, the holder, the stater unchecked; the probe's cost and heap unbound; no runtime exempt; the runtime's events unchecked; the pool at the floor; the slack saturating; the fold not rechecking; admission not checking itself; revision 7 |
| K2b-7e-2b-3d-3b-3b | K2b | `scheduler`'s `tests/verdict.rs`: consistent evidence and each of the census's 60 tampers folded by both folds — the fold voiding by the rule's code, `verdict.py` naming the same, each of the 16 codes reached by the definition; `tests/recheck.rs`: every single edit of a consistent evidence (a term's count, cost or rest one more, its stater the other; everything downstream made again), on runtimes built and assumed, refused but those on the pinned free list | each of the definition's 20 refusal sites deleted; the definition not rechecking; its code dropped; its pool at the floor; and, against the single-edit property alone, a probe's cost, the decoder's count and the late steps' followers each unbound |
| K2b-7e-2b-3d-3b-4a | K2b | `scheduler`'s `tests/verdict.rs`: every admission's result pinned in `golden/admissions.jsonl`, one line a key — a consistent one's the journal's version, a tampered one's the version and the fold's revision —, a key pinned twice and a result not pinned each refused; `tests/append_only.rs`: in a scratch repository, a line appended and a file new since the base pass, a line edited, removed, moved or its end removed fails | a key pinned twice admitted; a result not pinned admitted; the check passing any edit; a consistent result keyed by the fold; revision 9; a base's reader moved |
| K2b-7e-2b-3d-3b-4b | K2b | `load-run`'s `tests/fitted.rs` and `tests/stages.rs`: four headers admission journals pinned in `golden/admitted/` — no reader on a host's carrier over two sources, nine sources, one REST source on the library's carrier, a replayed plan's reader (its records ahead and its decoder counted) —, each made again equal byte for byte, its pool priced at a catch-up of 10; `scheduler`'s `tests/verdict.rs`: each folded by both folds, passing, its result pinned in `golden/admissions.jsonl`; `make vectors`: each against the normative schema | the plan's assumption dropped; the shell's attempt a byte more; the decoder's count journaled one always |
| K2b-7e-2b-3d-3b-4c | K2b | `load-run`'s `tests/fitted.rs`: the heap of one copy of admission's evidence, as the counting allocator sees a clone of it, the evidence's cost exactly at one source and at nine, two copies counted, admission's own copy's vectors at their lengths; `tests/fit.rs`'s shadow with the evidence's two copies over drawn sizes; `scheduler`'s census with six tampers of the evidence, the single-edit property's free list without it, the golden and load-run's four headers settled against their last — each differing by the evidence's term, its need by twice its cost | one copy priced; no row priced; the rows grown by doubling; the evidence unchecked; the definition's evidence unchecked; the evidence's rows at the rows journaled |
| K2b-7e-2b-3d-4a | K2b | `load-run`'s `tests/whole.rs` and `tests/whole/rows.py`, by hand, a row a process in scopes of its own, against e618d4b: each window within its phase's heap-basis sum; L1's per-answer slope within its journaled term; L1c's resident below its counted; zstd's bytes outside the counter within the decoder's term; the guard's realised counts | `HEAD` dropped from the answer's cost: P2 refuted (1,094,004 B an answer against 1,048,576); `ATTEMPT` cut by a third not run — P4 cannot tell it as stated |
| K2b-7e-2b-3d-4b | K2b | `tests/whole/rows.py` by hand against 9f81d0d and its two harness fixes: a source's own at idle; the kernel's sockets beside the scope's anonymous memory; THP off and on, confirmed by anonymous huge pages; one arena against the default over four sources; the threads' stacks resident over the answers held and the sockets' cap | the stacks' first reader counted glibc's arena heaps (200 MB at 250 answers) — fixed to a guard of one page |
| K2b-7e-2b-3c-1 | K2b | `load-run`'s `tests/fit.rs`, against a shadow of the decision's sentence in u128 over arbitrary parts, catch-ups 0 to 999: the need the largest of the stages', the gates' and the run's phases with the fixed parts, the pool at the catch-up's rate; at the need admitted with no slack, a byte less refused naming it; every part (`Part::ALL`, matched without `_`) moving the need as the shadow does; the binding term the binding phase's; stage 2 binding a socketless run; a part saturated, a sum past any count, a runtime of no stated events refused; the pool refused as the pool's | the phases summed; the pool outside the largest phase; the reader the run's; the catch-up dropped; a saturated part admitted; a sum saturating; the pool's failure the memory's; the binding term any phase's; held with no gate |
| K2b-7e-2b-3b | K2b | `load-run`'s `tests/costs.rs`: a connection whose egress's `Io` holds 32 KiB of its own within `CONNECTION` and the egress's statement, above 90 %; REST pricing an attempt's decorations at the bound its decorator states, none undecorated, unstated where its decorator cannot state; a stand's gates' client relayed; `tests/frame.rs`: four decorations of 8 KiB copied into the request within their price, above 95 %; `tests/rest.rs`: a decoration past its decorator's bound a `rest.decorator` fault, the run stopped; `carrier.rs`' own: an attempt's price its parts' sum, its decorations each attempt's | the egress's heap unstated; a decoration past its bound passed; the bound's bytes unchecked; the gates' client not the stand's; decorations at one copy; decorations unpriced |
| K2b-7e-2b-3a-3 | K2b | `load-run`'s `tests/rest.rs`: two sources whose steps at once total twice the cap, each within it, admitted, one past it on each refused naming source 0; `tests/costs.rs`: the shell's share of an attempt whose header a writer's captured value of `VALUE` bytes fills, within `run::attempt_share` and above 90 %; `command-log`'s `tests/validate.rs`: over the corpus at jitters 0 and 250, each count of the late shape at least the pending shape's | needs the plan's whole pending; the shell's copy of a hole unpriced; the copy without its value; a read's late steps not summed; the records left out of the machine's sum |
| K2b-7e-2b-3a-2 | K2b | `load-run`'s `tests/frame.rs` (a counting allocator by thread): REST's copy of the captured, its frame and the request built of it, each class at its worst — a target of 16,385 spaces, a target's, a header's and a body's hole of `TAKEN` quotes, a header and a body of 65,537 bytes, 1,025 fields — within `AttemptCosts::price` and above 95 % by class; `tests/costs.rs`: a connection after a head just past 16 KiB within `CONNECTION` and hyper's head buffer at the plan's largest head, and above 80 %, an attempt with a target, a header and a body within its price (one-sided: its request's copies written before the next read), a decorating plan's attempts unstated; `command-log`'s own: a JSON string's escaped length `serde_json`'s, every byte below 0x80 and any text | each coefficient down and up a step; a hole's copy without its value; the body unpriced; a JSON token grown by doubling; a control byte escaped in two; the head buffer at the head; the head's target unencoded; the connection without its head buffer; a decorating plan stated |
| K2b-7e-2b-3a-1 | K2b | `command-log`'s `tests/shape.rs`: a request's target (path values, the query's names and values), headers (names and values) and body, each its bytes and holes apart; `tests/request.rs`: every golden case's frame and a row whose target encodes past its values and whose body fills past its spans, each sized at once (capacity its length); the reference deriving the six counts from each record over a corpus that now queries a call, the validator's identity and revision 14's digest | headers counted as the target's; a body's holes as the target's; the target's holes uncounted; the headers walked as the target; the twin without the headers' names, without the query's (it survived a corpus with no query; red once a corpus mutant added one); an encoded length by its bytes; the query's separators uncounted; the body grown from its template |
| K2b-7e-2b-2c-4 | K2b | `load-run`'s `tests/head.rs` (a counting allocator by thread): a head's copy at hyper's hundred fields filling `READ`, distinct names and one name, within `rest::HEAD_COPY` and above 95 % of it; one name repeated at 33 and 65 fields within it; `tests/costs.rs`: a head of one name repeated within what REST charges an answer, and REST's source costs from the plan — the scratch, the head's copy, the tail at the cap of the plan's largest record and its problem, stated by the library, a source it does not know refused; `carrier.rs`' own: the costs charged summed | the copy grown by doubling; the fields' bound short (red once the executor took hyper's limit, not the constant it checks), loose; the head, the scratch uncharged; the tail of no record, at no cap, without the problem; a host's word for the library's; an unknown source stated; the largest, not the sum |
| K2b-7e-2b-2c-2 | K2b | `command-log/tests/parse.rs` (a counting allocator by thread, a block moved counted new before old): the densest input — a forest of chains just past a power of two — at 99.7–99.8 % of `json::bound`, cut inputs within it and some past two nodes a byte, nested objects within it, any bytes and every prefix of generated documents within it, the bound saturating; a pointer's tokens at 95 % of `json::pointer_bound`; `scheduler/tests/tail.rs`: judging within `judge::bound`, each term the one that decides a row of its own — a literal the body's size, a value at `VALUE` under names of 8 KiB, a hole of `TAKEN` digits, a `Location` decoded into `TAKEN` quotes, a pointer of 65 tokens —, a host's array in a hole never parsed, `is_taken` refusing what no take makes; the validator's identity at revision 13, `vectors/reference.py` deriving the largest literal and the longest pointer, `tests/shape.rs` a capture's pointer the longest and the largest literal not their sum | the bound's added slot dropped; the bound a depth looser; one node a byte; objects pre-sized; a token's slot held once; a literal not walked; a capture's pointer not walked; the literals summed at their most; a hole's value, a segment's decoding, a pointer's tokens unpriced; the body and a value read at their largest; a segment sized by its escapes; a take's value checked by its first byte; a structure parsed; a value taken, its names unpriced |
| K2b-7e-2b-2c-1 | K2b | `load-run/tests/sources.rs` (a binary of one test, its allocator the process's): runtimes for 1 and 9 sources at caps 0 and 4,096 — their events exactly `EVENT` each, a source's own within `RUNTIME` and its events at 98 %, `Runtimes::own` each runtime built, a test's runtimes priced by no one, a runtime of no event built and recorded with one; `tests/lint.rs`: `Live::start` a test's, `EVENT` three items under their targets | an event of 16 B; an event's size unconditional; the runtime halved; the events uncharged; a test's runtimes priced; the events recorded as asked; unchecked runtimes a host's |
| K2b-7e-2b-2b-3 | K2b | `load-run/tests/shell.rs`: a carrier's capture no spec of its step names, one past the specs of its subject and slot, a value's buffer past `VALUE` — each a carrier fault named, the run stopped, its answer never journaled; captures within the specs taken in any order, two of one subject and slot where two specs name them, a buffer of exactly `VALUE`; a carrier minting a code an attempt: `UNSENT` codes named, the rest counted, every report counted; `stamps.rs`'s own test: a capture kept in its spec's names at their exact length; `scheduler/tests/judge.rs`: a value taken at its worst escape `VALUE` bytes, its buffer no larger; a `compile_fail` doctest: no code made but by `Code::of` | the boundary bypassed; the check after the push; captures matched in order; a spec taken twice; a buffer of exactly `VALUE` refused; a capture kept in its carrier's names; a value taken grown; the unsent codes uncapped; the codes past the cap dropped; a code of any shape made |
| K2b-7e-2b-2b-2 | K2b | `load-run/tests/channel.rs` (a binary of one test, its allocator the process's: the journal's three threads): a journal's channel full and its writer stalled in a write, its output of exactly `OUT` bytes inline holding a stated heap, a feeder's sends begun `behind + 2` and done `behind + 1`, within its `Journal`'s bytes at 98.8–99.9 % at 1, 4, 64 and 128 lines and at 0, clamped; its slope from 64 lines to 128 its stated one to within the thread's own 72 B; `tests/memory.rs`: a burst's first line comes with the run holding, from 64 dependents to 128, the machine's outputs' growth alone; `tests/tcp.rs`: through admission and the stages, the journal's channel admission's (37 lines), its output opened before T0, an output of `OUT` bytes taken and of `OUT + 1` refused before it is opened; `tests/sink.rs`: a test's journal marked assumed; `tests/lint.rs`: the journal kept whole a test's | the channel twice the admitted; one line beside the channel; the slots, a slot's stamp, the lines, the writer's buffer, the thread, the output uncharged; no line behind clamped; an assumed journal unmarked; the journal kept whole a host's; a burst's lines made at once; the stages ignoring the admitted journal; an output past `OUT` taken; the output opened after the clock |
| K2b-7e-2b-2b-1 | K2b | `scheduler/tests/lines.rs` (a counting allocator by thread): over the corpus's clean logs, every answer capturing every spec, each line within its log's largest record's `journal::line_bound`, each read, end and answer exactly its row at its exact capacities, each row reached; a read of 0, 1, 4, 5, 9 and 17 step and barrier dependencies, its answer under two codes of 64 B, its end: the bytes made for each line, as it is taken, its walk's, each row its price, the bound their largest, the machine's bindings exactly `fits`' price of the record; the writer given its buffer and the header's box at its start and nothing a line — the shortest, about the buffer, ten buffers of escapes, an end's bound lines —, each line whole and flushed once written, its bytes the canonical lines', a header past the buffer read back with its trailer, a failing output an I/O error; `tests/costs.rs`: barriers of 1 KiB labels, memberless and with members, within `fits`; the validator's identity at revision 12, `vectors/reference.py` deriving the barriers' keys' bytes | a read's dependencies, its barriers' keys, an answer's second code, an end's bound lines unpriced; the bound a read's alone; a barrier's key, an answer's slots, an unsent code, an end's box not walked; a read's dependencies collected through a flatten; a step's lines made at once; the machine's bindings unpriced; a record's barriers' keys not walked, not summed, not at their most; a line, the header written through a copy; a bound line's numbers through a string; the buffer grown, not written out; a full buffer not digested; no flush, no write at a line's end; a write's error an encoding's |
| K2b-7e-2b-2a | K2b | `command-log/tests/reading.rs` (a counting allocator): a replay's peak within its `Reading` and one record at the largest, plain and compressed, at 89–90 %; past a header longer than two chunks, its text escaped, a step's reading alone; a line one byte past its commitment's — a step's, the header's — refused, nothing grown past the limits; libzstd's context at the window two-sided, a frame of a wider window refused, a level-19 log replayed; `tests/shape.rs`: a header's heap exactly `shape::header`, every sequence at 0, 1, 4, 5, 9 and 17 elements, every kind of limit; a line decoded holding at its peak its record's heap alone; `load-run/tests/costs.rs`: a replay's reader, its channel full, within its `Reader` at 99 %; `tests/stages.rs`: a run whose source holds more records ahead than its plan's reader refused; the validator's identity at revision 11, `vectors/reference.py` agreeing on the largest record | a line read past its limit; the verifier's buffers grown; the header's buffers kept; the canonical check, base64's writing, a text's reading through a copy; the reader's window unbounded, the writer's its level's; the largest record not committed; the header's limits not walked; the line's term, the header's reading, libzstd's context dropped; the run's reader unchecked; the two beside the channel, the channel's blocks uncharged; the channel twice the reader's |
| K2b-9a | K2b | the machine's poll grid at the plan's instant: an observation read at 250 polled at 300, 400 …, read on a point at 600 polled there once, two read together at 620 on their own phases; the differential with `schedule.py` moved alike, settled run by run against machine 4 (16 of 3,944 changed, each at a poll of an observation released late) | the grid at `released`; the release's point not held (polled twice at one instant) |
| K2b-9b-1 | K2b | journal version 9 — the golden holding the run's catch-up at 999, a refusal read, `decorated` on a `Sent`, both sides of a never-sent report's `decorated` and a found answer's `handed`; the schema and `journal.py` alike; a rate deviation read as a refusal, another rule's and none not; the differential's journals and the golden runs settled against version 8, the new fields removed, every line the same | a rate deviation unread; any rule read as a refusal |
| K2b-9b-2 | K2b | `scheduler::pace`: each attempt handed at the later of its earliest and the instant it came, every pair keeping the rule by brute force, a microsecond sooner some pair breaking (κ ∈ {0, 1, 100, 500, 999}, J ∈ {0, 1, 250, 2000}); every hand-off within `(1 − κ)·J` of its raw never paced; the band — a dispatched batch in raw order, the rest paced to the instant computed by hand; an attempt not arrived holding the front; a head whose pace falls past its deadline leaving; a straggler within the band passing; an attempt past its deadline beyond the band leaving, so the next turn never names an instant gone | the allowance J; κ ignored; rounding down; the mirror term dropped; the band unbounded; arrival ignored; never late — each red |
| K2b-9b-4, -5 | K2b | `scheduler::pace` against brute force over every pair (κ ∈ {0, 1, 100, 500, 999}, J ∈ {0, 1, 250, 2000}, ties and inversions); a hand-off late by at most `(1 − κ)·J` past its earliest moving neither maximum; `sim` paced and `verdict.py` recomputing its gate exactly and checking every pair on every differential run; a golden at κ > 0, J > 0 coherent under the pace and void under the allowance J; a refusal admitted under a shift undecided; excursions recomputed by `verdict.py` | the allowance J; the clamp `max(raw, D)`; rounding up; finality by time; the found answer's `handed` ignored |
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
  of its own counts (`ENDED_CELL` 1,152, `LIVE_CELL` 1,792, `BARRIER` 768, some 30 % above — re-pinned by K2b-7e-2a
  to 1,216, 3,776 and 768, § Measured: that margin was its allocator counting a block moved as grown in place, and no
  row released a burst at one instant or held a REST read; then by K2b-7e-2b-1c, its records priced apart by the
  plan's shapes, `LIVE_CELL` 2,944); a cell keeping its record
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
- **A censor Root, once** (2026-11-24, K2b-9b-1's gate): one run of the twin read a censor Root (`censors 1`), against
  its assertion; the fold read the journal it always reads (K2b-9b-1 adds fields no rule uses), so a rare outcome of
  the run, not of the commit. Fifty-two runs since, none. The twin's tally counts as the stall's any Root whose step
  has a censored or never-sent attempt — but its stand refuses connects 40 ms in every 100, and a never-sent attempt
  on time whose connects the closed listener refused is a Root by decision 19, the stand's like its backlog's resets;
  which it was is not known: the twin now prints such a Root's whole path, kept for the next sighting. The same fifty
  runs: 402 Roots, 119 of steps released late (the burst), 283 of steps released on time — far more than the stand's
  closures (3–7 a run before): the burst's on-time neighbours, decision 21's case; K2b-9d's stand drops the closures.
- **The censor Root's second sighting, and its cause** (2026-11-27, K2b-7e-4b-4's gate): the printed path — a read
  released on plan, handed 0.75 ms past its due, its connect refused by the stand's closed listener (`refused_by:
  refused`, `rest.connecting`), censored at its deadline, its report arriving after the stop — is a Root by decision
  19, the stand's like its backlog's resets: the twin's tally, counting as the stall's any Root with a censored or
  never-sent attempt, was too broad. It now counts apart an attempt handed within J of its due whose connect the stand
  refused; six runs after, none such and no censor Root. A test-only fix.
- The timer rule's mutants (a completed timer's due unnoted, a dropped one's) survive this twin — its censors are
  undecided by paths besides — and are killed by `lag.rs`'s twins; why no attempt refused just before the stop, its
  floor across it, makes a Root under them is not established here.
- A hog over the deadline while connecting or writing on a thread others hold is the source thread's lag (K2b-7c-2's
  twins, the path's lag counted from the thread's last blocked park); the stall twin is its whole-process case.

**K2b-7e-2b-3d-4b, measured** (2026-11-28, the laptop, against 9f81d0d, `--release`). Two harness defects found
by the rows and fixed before the rows that read them ran again: the four-source rows' 128 ports are the egress's total,
32 a source against 63 needed, refused by admission — set to 256; the stacks' reader counted every anonymous mapping
after any `---p`, glibc's arena heaps among them (200 MB "of stacks" at 250 answers) — a thread's guard is one page.
- **P6 no test**: a source's own at idle, 4,837 B counted against 5.9 MB its terms price at either cap — the carrier's
  source costs (the answered tails, judged) are held only while a source judges, the driver's events allocated at its
  first turn: the idle row reaches nothing it prices. Owed to a row where every source judges at once.
- **P7 held**: 250 answers of 1 MiB sent at once left at most 28.5 MB in the kernel's sockets (`sock`), beside 267.7 MB
  of anonymous memory and a scope's peak of 277.5 MB — memory admission does not price, a tenth of the bodies here, on
  loopback.
- **P8 held**: THP off, no anonymous huge page at any edge; on (glibc's `hugetlb=1`), 16 MiB of them; the resident
  high-water's rise 267.2 MB of 274.0 MB counted and 267.2 of 274.1, either way.
- **P9 held**: answers over four sources, the high-water 279.7 MB with one arena against 266.0 MB by default (5.2 %),
  273.9 MB counted in both; glibc kept 122.7 MB and 159.3 MB free in its arenas after the run.
- **P10 held** with the fixed reader: 3–5 KB resident a thread's stack, 16–20 KB over 4–8 threads, whatever the answers
  held or the cap.
- **A reading, P4 restated**: 4a's 48,936 B an attempt in flight, less the 39.8 kB a pooled TCP connection measured
  (`tests/memory.rs`), is 9,136 B against the 15,002 B an attempt its needs' slope prices.

**K2b-7e-2b-3d-4b, the harness and its predictions** (2026-11-28, written before its rows run): the harness takes a
number of sources (the reads spread over them, an egress a source), turns transparent huge pages off by
`prctl(PR_SET_THP_DISABLE)`, reads the threads' stacks resident from `smaps` (each anonymous writable mapping after a
guard page, and `[stack]`); the rows' runner names each row's scope and samples its `memory.stat` and `memory.peak`
from outside every 20 ms. One idle-source row, run to make it work, read 5.8 MB of stacks over 8 threads — more than a
thread's touched stack, so the heuristic may count other guarded mappings (glibc's arenas): P10 is stated relative. The
late records' row moves to K2b-7e-2b-3d-4c: a stopped process is what makes an attempt's look come after its step's
end. The predictions, for L4 (one read a source, one and four sources, caps 100 and 1,000), L6 (250 answers of 1 MiB
sent at once), L1 with THP off and on, and L1's answers over four sources with glibc's arenas by default and one:
- **P6**: a source's own bytes at idle — four sources less one, over three, at `started` — within its journaled
  runtime, carrier-source and keeping terms, at either cap.
- **P7**: L6's kernel socket memory, the scope's `sock` at most, unpriced by admission: above zero and below twice the
  bodies in flight (250 MiB).
- **P8**: THP off, no anonymous huge page at any edge; on (`GLIBC_TUNABLES=glibc.malloc.hugetlb=1`), some; either
  way the resident high-water's rise within 1.05 of the counted window plus 2 MiB a thread.
- **P9**: over four sources, the high-water with one arena (`MALLOC_ARENA_MAX=1`) within 10 % of the default's.
- **P10**: the stacks resident a thread do not grow with the answers held or the sockets' cap.

**K2b-7e-2b-3d-4a, measured** (2026-11-28, the laptop, against e618d4b, `--release`, each row in a scope of its own
without swap; `tests/whole/rows.py --compare` reads every bound from the rows' own journals). The rows' operation timeout
was set to 10 s for the held answers before they ran — the validator refused 20 s past the route's 15 s less J.
- **P1 held**: no window above its phase's heap-basis sum in any row. Stage 2's window is tight in every row — 98.0 %
  at the control, 99.6 % at L1, 98.5 % at L2's 64 KiB bodies; stage 3's at most 42 % (L2 at 64 KiB: its probes' heap is
  the bodies' connections), 2 % elsewhere; the run's 89 % at 250 answers held of 256, 0.6 % at the control.
- **P2 held**: 1,094,170 B an answer (L1 at 250 less at 125, over 125), between the body's 1,048,576 and the answers'
  journaled cost 1,126,400 — 97 % of it; its guard, 125 and 250 in flight at once. Its mutant, `HEAD` dropped from the
  answer's cost in a scratch build, refuted it: 1,094,004 B against 1,048,576.
- **P3 held**: L1c's chunked answers counted 273.8 MB (each reserved at the cap) while the resident high-water rose
  5.8 MB, 2.1 %; L1's held bodies, touched, rose 267.2 MB of 274.0 MB counted (97.5 %). Resident and counted part
  either way: guidance on a stated memory cannot be a ratio of the two alone.
- **P4 refuted as stated**: L2's attempts in flight cost 48,936 B each (400 less 200, over 200) against the 15,002 B
  its two needs' slope prices. The prediction left out that each attempt in flight opens its connection, which
  admission prices at the cap (40,960 B a socket, the same at 200 and 400): with it, 55,962 B priced, above the 48,936
  measured. The `ATTEMPT` mutant cannot be told by P4 so stated and was not run; the per-attempt row needs the
  connection's share measured apart (K2b-7e-2b-3d-4b's sources and sockets).
- **The guard, twice "no test"**: no row ended a step while its attempt lived past it (late 0 in every L2 row — the
  never-answering stand's attempts are given up at their deadlines, the end after the answer), so the late records'
  coefficient (K2b-7e-2b-3a-3's owed row) is still unmeasured; L2 at 64 KiB bodies had 256 of its 400 calls in flight
  at once, below 0.9 of the priced, its per-body-byte slope no test.
- **P5 held**: with the decoder alive, zstd's bytes outside the counter (`mallinfo2`'s in-use less the counted, the zstd
  log's less the plain's) were 2,586,528 B, 99.5 % of the decoder's 2,600,000.
- **Beside them**: after a run that held 274 MB, glibc kept 265.6 MB free in its arenas (`fordblks`), not returned —
  the freed bytes the process holds past a phase (K2b-7e-2b-3d-4b's question of phases summed).

**K2b-7e-2b-3d-4a, the harness and its predictions** (2026-11-28, K2b-7e-2b-3d-4's decision; written before its rows
run, as M-a's were): `crates/load-run/tests/whole.rs`, an ignored test, a row a process — its log committed,
`Replayed::open`, `fit` at the need under `Limit::read()`, `Live::fitted`, `admit`, the stages, a journal on disk, `run` —
its heap counted process-wide (a moved block at both sizes, the blocks alive), its peak taken at each edge, `mallinfo2`,
`VmHWM`, `RssAnon` and `AnonHugePages` beside it; `tests/whole/rows.py` makes each row in a scope of its own without
swap and an explicit `memory.max`, its stand (holding every body's last byte until all are out, chunked, never
answering, or at once) in another, and reads back the row's journal: the heap-basis terms by phase and the realised
counts (in flight at once, ended unanswered at once). The control row, run once to make the harness work: stage 2's
window counted 6,944,382 B of the 7,089,154 B its phase prices (98 %), stage 3's and the run's near nothing. The
predictions, for the rows L1 (answers held, 125 and 250 of a cap of 256, 1 MiB each, plus 250 chunked), L2 (400 and
200 calls never answered, bodies of 1 KiB and 64 KiB, a cap of 512) and L5 (the control on a zstd log):
- **P1**: every row's windows within their phases' heap-basis sums — stage 2's within F + S, stage 3's within F + G,
  the run's within F + R + P.
- **P2**: L1's per-answer slope, the run's window at 250 less at 125 over 125, between 1,048,576 B (the body) and
  1,126,400 B (the answer cap and `HEAD`); its guard, 250 in flight at once.
- **P3**: L1c (chunked, bodies reserved at the cap, pages untouched): its counted per-answer slope near 1 MiB, its
  anonymous resident's rise below 5 % of its counted rise.
- **P4**: L2's per-attempt slope, 400 less 200 over 200, within the priced slope its two journals' needs give; its
  per-body-byte slope, 64 KiB less 1 KiB at 400, at most 3 B a body byte an attempt; its guard, every call in flight
  and ended unanswered at once.
- **P5**: L5's `mallinfo2` in-use less the counted, zstd's less the plain log's, above zero and within `DECODER`'s
  2,600,000 B — what the counter cannot see, and what the decoder's term prices.
- **Mutants**, each in a scratch build: `HEAD` dropped from the answer's cost must break P2's upper bound; `ATTEMPT`
  cut by a third must break P4.

**K2b-7e-2b-3d-3b-4c — the evidence priced** (2026-11-28, K2b-7e-2b-3d-3's decision, rule 11): `Admitted` keeps one
copy of admission's evidence, the journal's, and no longer the fit it was mapped from. The evidence is a part of its
own, `Evidence` (version 10's table's 26th row: the run's, fixed, the heap's, built, the library's): two copies at once —
admission's, and the header's while it is written —, each every term (this one with them), a row a source (the rows'
vector made at the sources' count) and the unheld parts, at sizes the `Model` journals (`term`, `row`, `part`), so that
both folds bind it (`admission_evidence`: its count 2, its cost those sizes times the journal's own counts, its rest 0).
Measured with the counting allocator: one copy's heap is its cost exactly, at one source (1,288 bytes) and at nine
(4,616). The table changed after K2b-7e-2b-3d-3b-4a pinned version 10's results, and before they reached `main`:
`golden/admissions.jsonl` was made again from nothing — the append-only rule holds against `main`, where the file is
not yet; once it is, such a change moves the version. The golden journal and load-run's four headers were settled
against their last: each differs by the evidence's term and the model's three sizes alone, its need by twice the
evidence's cost. The gate's run found the fit's property drawing a reader of no records ahead, which the journal
cannot tell from no reader and which the decoder's rule refuses: no reader holds fewer than the two beside its records
ahead (`Reader::priced`), so the generator now draws a reader's count from 2 — a test-only fix, three runs of 4,096 cases
green after it. Mutants: six, each red.

**K2b-7e-2b-3d-3b-4b — admission's headers pinned** (2026-11-28, K2b-7e-2b-3d-3's decision, Evolution E6): four
headers load-run's admission journals, each a header alone and its trailer, are pinned in `crates/load-run/golden/admitted/`
— no reader on a host's carrier over two sources, nine sources, one REST source on the library's carrier, and a replayed
plan's reader —, admitted on the runtimes built for their fits, their pools priced at a catch-up of 10; the identity is
fixed and the descriptors, the process's as read, replaced by fixed ones, the rest admission's own. Each is made again
and compared byte for byte (`tests/pinned/`), validated against the normative schema by `make vectors`, and folded by
both of the scheduler's folds, its result pinned beside the census's. A change to admission's section shows in their
diff, which is settled line by line before `regenerate_admitted` pins it again. Mutants: three, each red.

**K2b-7e-2b-3d-3b-4a — a rule's change moves its key** (2026-11-28, K2b-7e-2b-3d-3's decision, Evolution E3 and
Verifiability V9): the admissions both folds read — the six consistent bases and the census's 60 tampers — are pinned
in `golden/admissions.jsonl`, a line each: its case, the digest of the header it judged, its verdict and code, keyed by
the journal's version for a consistent one, whose rules are frozen with it, and by the version and the fold's revision
for a tampered one. The test refuses a key pinned twice and a result not pinned; CI's `append-only` (`make
append-only BASE=…`, a job of its own against a pull request's target or the commit a push followed,
`vectors/append_only.py`) refuses the file at its base not a byte prefix of the file now. So a rule's change that
moves a result can neither be re-pinned in place nor appended under the same key: a consistent result's move bumps
`JOURNAL_VERSION`, a tampered one's `FOLD_REVISION` — V9's process mutant (an expectation edited, the row re-pinned)
now fails. The ignored `regenerate_admissions` appends what is not pinned and refuses to change what is. Mutants: six,
each red.

**K2b-7e-2b-3d-3b-3b — the definition rechecks it too** (2026-11-28, K2b-7e-2b-3d-3's decision, Verifiability V2 and
V7): `verdict.py` rechecks a version-10 header's admission from the decision's sentences, its table read from
`schema/parts-10.json`, its rules in the fold's order, each refusal a `REACHED` counter by its code; a case carries the
fold's code where an admission is refused, the definition's must be the same. The census moved to a module both tests
share (`tests/admission/`) and folds through both twins. The two-sided property is exhaustive rather than drawn: every
single edit of a consistent evidence — 4 kinds over each of its 33 terms, on runtimes built and assumed — is refused
unless it is on a free list pinned in the test, the edits the journal alone cannot tell from another run's: a count no
other count or row binds, a cost no constant or other cost binds, every rest but a runtime's own, a carrier's part's
stater. The list was written from the rules before the run and held; a binding lost shows as an edit admitted. Mutants:
26, each red.

**K2b-7e-2b-3d-3b-3a — the fold rechecks admission** (2026-11-28, K2b-7e-2b-3d-3's decision, rule 7 as amended by
Evolution E1 and Verifiability V6, V7, V12): `recheck::recheck(&Header)` dispatches on the header's version —
`recheck::admission(version, catch_up, &Admission)`, version 10's rules its own (`admission_10`), another version's
refused as `admission_version` — and checks, in this order, each a closed code: κ (`catch_up`) below 1000; the term set
(each global part once, each source's part once a source, the sources the runtimes'); each term's phase, basis, holder
and stater by version 10's table (`PARTS_10`, `schema/parts-10.json`, asserted equal); no gate built; each term's
`count · cost + rest`, in `u128`; the library's constants where a cost is one verbatim (the cells', the pending's, a
key's, a record's, the shell's attempt's, the decoder's, a runtime's event and own); the decoder's count one exactly
when the reader's is not none; the counts terms share (a source's sockets its connections', the late steps' records)
and a probe's costs its connection's and its heap's; the rows (one a source at most, its sockets its cap, its runtime's
events its own but where a test assumed the runtimes, a source with no row holding no socket); the pool at κ rounded
up, the need, the slack and the budget; the unheld parts, each once in the terms' order; the scope (an unheld pool and
a finite `memory.max` at the process's own group); the limit. `FOLD_REVISION` 8: the fold voids a header that breaks
one, after the log's identity (`Void::Incoherent { line: 0, why }`); `admission: None` passes; the corpus's classes did
not move. `run::admission(…)` rechecks its own evidence by the version it journals before anything outside the process
is touched, a failure `Inadmissible::Evidence { why }` — and every admitted run of load-run's tests passes it, the table
and the rules written apart from `terms()`. The census found a hole the decision's tamper list left: a term beside a
full set — a part twice, or at a source past the runtimes', of no bytes — passes a count of the set; the loop's own
refusal kills it, two tampers now hold it. `verdict.py` reads no admission header yet — none reaches it, the golden
runs and the corpus journal none — until K2b-7e-2b-3d-3b-3b. Mutants: 32, each red once the census held the two.

**K2b-7e-2b-3d-3b-2 — version 10: admission's memory** (2026-11-28, K2b-7e-2b-3d-3's decision, rule 6 as amended):
`JOURNAL_VERSION` 10. An admission carries `memory: MemoryFit`, not optional where an admission is: the memory stated and
its origin, the process's limit as read (`LimitRead` — the least `max` and `high`, each its bytes and depth, and its
origin: read, a test's tree, a test's assumption), the library's constants (`Model`), every term (`MemoryTerm`: part,
source, phase, count, cost, rest, bytes, holder, stater, basis), the unheld parts, the budget, the pool, the need and the
slack; every enum closed and snake case, the normative schema (`schema/journal.schema.json`) the same, its
keys in their order. κ is not in it: the header's `catch_up` is its one copy. `assumed`, one flag
ORing a test's plan and a test's runtimes, became `Assumed { plan, runtimes, journal }`, each apart: version 10's
recheck exempts the runtime-events rule on the runtimes' assumption alone. `run::admission(…)`, pure, is the only maker
of an `Admitted`'s evidence; load-run's names map into the journal's by exhaustive matches, each journaled as its own
name. Settled: the golden journal's header with `memory` removed, `assumed` back to version 9's flag and the version
restored is version 9's line, and every other line is the same but the trailer; so are the 31 golden runs, and the
reference corpus's 4,932 journals (both of its tests), run by run, against a build of K2b-7e-2b-3d-3b-1; the corpus's
classes did not move. The fold reads none of it yet (K2b-7e-2b-3d-3b-3). Mutants: ten, each red.

**K2b-7e-2b-3d-3b-1 — a header's version before its schema** (2026-11-28, K2b-7e-2b-3d-3's decision, Evolution
E1): `Reader::new` reads line 1's version alone first — `frame::peek`, every other field ignored, its bytes not
checked canonical — and refuses another as `Version(n)`; the line is then decoded whole by this version's schema. Before,
a header whose schema had moved — version 8's, without `catch_up` — was refused as a malformed line, and a version-10
reader would have refused every version-9 journal so. `verdict.py` names the version it reads and refuses another's
journal rather than fold it by this version's rules. Mutants: three, each red.

**K2b-7e-2b-3d-3a-2 — the terms by count, cost and rest** (2026-11-28, K2b-7e-2b-3d-3's decision, its rules 1 to 4):
a term is `count · cost + rest`, checked, its count the enforcer's — a source's cap, a runtime's events, the reader's
records ahead and the two beside, the journal's lines behind and the two beside, the cells held, the steps pending, the
barrier keys, the late steps —, 0 for a part absent, 1 for one held once; a cost or a rest derived past any count is
refused whatever its count. The parts gain `Pending` (the cells' second product apart), `JournalOut` (the journal's
output, the host's), `ProbesIo` (a probe's connection heap, the host's) and `Decorations` (the decorator's), 25 in all;
Connections, Answers and Probes carry the carrier's own stater (the library's word unconditionally before, a host's
carrier journaled as the library's). `Planned` holds each shared count once (`fits::Counts`) and a closed `Model` of
the library's constants, `Model::LIBRARY`, which `run::planned` alone reads; each owner states its split
(`Reader::priced`, `Journal::priced`, `AttemptCosts::priced`, `run::attempt_parts`), its old byte function the split's
sum; `shape::ARC` is public. The pool's records are priced by `peaks.late`, the count the read gate will hold, in place
of the late shape's records: equal on every validated plan, held over the corpus. A runtime is unstated by its cause —
its events or an event's size —, no longer by a saturated term. Mutants: fifteen, each red.

**K2b-7e-2b-3d-3a-1 — κ by one call, the limit's origin, the gate's token** (2026-11-28, K2b-7e-2b-3d-3's decision):
`run::kappa` is the one call the fit prices the pool at and `stages::journal_header` journals as `catch_up`; a reserve
whose κ is 1,000 per mille or past it is refused by name (`Inadmissible::Reserve`), not as the cells' overflow. A
`Limit` carries its origin — read, a test's tree, a test's assumption (`LimitOrigin`) — for version 10 to journal apart
from the runtimes'; `Fitted` and `Admitted` keep the limit and the memory. `Held::Gate` holds `Option<GateBuilt>`, an
uninhabited token (`Infallible`, private) the read gate will mint; `Fit.held` became `unheld`, the gate's parts. The
header's own `reserve / 2` against `kappa` is an equivalent mutant while the formula is one. Mutants: four, each red.

**K2b-7e-2b-3d-2 — the process's limit and the scope rule** (2026-11-28, decision 22's K2b-7e-2b-3 rule 2 and rule 9):
`live::Limit` is a process fact — the least `memory.max` and `memory.high` over the cgroup v2 tree from the process's
own cgroup up to the root, each with its depth, `read` its one constructor beside a test's (`read_from` a tree,
`assumed`). `fit` takes it: a memory stated past its least is refused (`Inadmissible::Limit`, the file and depth); a
run with a count no enforcer holds — the pool's, held by a gate not built until the read gate — is refused unless the
process's own cgroup has a finite `memory.max` (`Inadmissible::Scope`, naming `systemd-run --user --scope -p
MemoryMax=…`): the ADR's own "the first host run inside a scope with `MemoryMax`", executed; a host runs inside one
from now. The carrier's knob joins the remedies: `Carrier::ports` and `Carrier::caps_at`, the REST carrier stating them
from its egress (`Egress::total`, required), the floor the least total that gives each source the sockets the plan
needs of it. Mutants: nine, each red. **A defect found after, by K2b-7e-2b-3d-3's blind solver**: `planned` took each
source's cap from the carrier's map at its own total while the runtimes and the admission's rows took its needs' — two
statements nothing tied by type, a host's egress free to make them differ, and then the fit priced caps no build
makes. Fixed (2026-11-28): the fit refuses a carrier whose map at its own total is not its needs' caps, and prices the
map only at a moved total; its row an askew carrier, its mutant red.

**K2b-7e-2b-3d-1 — what was fitted runs** (2026-11-28, decision 22's K2b-7e-2b-3 rule 1 and rule 9): `run::fit(plan,
read_ahead, carrier, journal, memory)` is pure and builds nothing; it prices every part from what it binds — the
carrier's needs and costs by source, the reader at its records ahead, the journal at its lines behind, stage 2's
judging, the gates', the machine's and the pool's — and returns a `Fitted` or every refusal at once
(`Inadmissible::All`): a carrier that cannot carry or tell a source, a source past its cap, a cap past what a driver
sizes, the descriptors its caps and runtimes would hold past the soft limit as read now, a part no owner states, the
memory (`Inadmissible::Memory`, the fit's `Unfit` with its remedies). `Live::fitted(&Fitted)` is the only way a host
builds runtimes (`Live::for_caps` a test's now); `admit(fitted, &runtimes)` takes nothing else and refuses a runtime
whose own is not the one fitted (`Inadmissible::Runtime`) beside the flavour, the count, I1 and the descriptors as the
runtimes left them — a test's runtimes (`Runtimes::assumed`, `Live::start`) exempt by name; `run::admitted` is a test's
`fit` then `admit`. **Measured against the decision**: a runtime holds three descriptors of its own on Linux, not the
two the decision named (an epoll and an eventfd) — the fit counts `RUNTIME_DESCRIPTORS = 3`, held by its row. The
remedies offered are the memory and the library's knobs (the records ahead, the lines behind); the carrier's (its ports
through `Egress::caps`) and the plan's answer cap are not yet searched (§ Owed). The judging's slice remedy named
K2b-7e-4, built: retired. Mutants: eight, each red. The compiling twin of `admit`'s `compile_fail` doctest found it
failing for a type it misnamed, not for the plan it was written against.

**K2b-7e-4b-5 — a plain name compared over its text** (2026-11-28, M-a's rerun after K2b-7e-4b-4: R4, R5 and R7
still refuted, by smaller margins — four lookups 2.3 to 2.6 times `Exists` on members, `Contains` 1.6 to 1.8 times on
strings, the names of one hash 4.8 to 6.2 times the members' grammar —; R6 refuted on the densest shapes, § Owed): a
string is compared over its text byte by byte while its bytes are plain — they are their own decoding, and names
differ from one another mostly in their first bytes, so the walk stops early —, decoded piece by piece from the start
at its first escape; two names alike. Searching a string's plain end first and comparing its text at once measured
slower on the laptop — the names of one hash twice as slow: it reads the whole name to learn what its first bytes
decide. **K2b-7e-4b-4's unit test had lost its escapes**: each `\uXXXX` in it was decoded to its character between
the author's tool and the file, so it compared plain strings while its record here named escapes; restored and
checked byte by byte in the file, its mutants rerun — the equivalent one stays equivalent. Mutants: five, each red. On
the laptop, loaded, four lookups on members 1.3 to 1.5 times `Exists`, the names of one hash at the members' grammar,
long names 2.1 to 2.3 times; the server's rerun decides.

**K2b-7e-4b-4 — names compared piece by piece, runs searched at once, a literal bounded** (2026-11-27, decision 23's
step 2, M-a's R4, R5, R7 and R8): a decoded string is compared a piece at a time — a run's bytes at once, an escape's
character's — against a want's name or string (`at`, `Equals`, `Contains`) and against another name within a run of
one hash; the decoder's run and the lexer's digits are searched over the slice at once, not byte by byte under a
checked index. A predicate's literal is at most `TAKEN` bytes (rules revision 16; `vectors/reference.py` the same),
which bounds its tree's parse every judgement at 159 µs on the server. Mutants: nine; one equivalent — an escape's
character taken whole, since both sides' chunks end on characters' boundaries and so a take within one never comes —,
one first survived — a length counted short, on a string whose first run is `TAKEN` bytes — and its row was
strengthened. On the laptop, loaded, four lookups on members fell from 2.5 to 1.9 times `Exists`; the server's rerun
of R4, R5 and R7 decides.

**K2b-7e-4b-2 — the readers on the tape, the frozen readers, the bound, rules revision 15** (2026-11-27, decision
23's step 2): `answer::Read` keeps a `Tape`, built on its first reader as K2b-7e-4a's tree was; `code`, `holds` and
`take` keep their signatures, their order and their errors, and the judge is unchanged. A want is a `Sorted` tree —
its objects' members sorted in place —, and `Node::equals` walks the tape against it: numbers by `same` over their
text, strings decoded, arrays in order, objects by member count and a binary search of each decoded name. A string is
decoded sized at once, and a capture's only to `TAKEN`. `answer::bound` is the tape's entries and the larger of two
phases — the build, its names' check beside the want of the predicate whose reading built it; after it, the code kept,
one pointer's tokens — and one value read: about 6 × cap against 128 × cap. Stage 2's judging holds 94 % of its bound
at a 64 KiB cap. The validator refuses an answer cap past `LONGEST` and a capture's pointer that is not RFC 6901's —
the latter a silent missing value before (rules revision 15; `vectors/reference.py` the same). Decision 23's step 1
differential, lazy against eager over stratified answers, is built here: the readers on the tape against a frozen copy
of the eager ones, pinned by digest. Measured: a want of 10,000 numbers and a tape of 10,000 members hold exactly their
parts, 1.83 MB, under a bound of 7.0 MB — the literal's term states the tree's densest. Mutants: fourteen, each red;
two first survived — a grown string on a code of one piece, a capture decoded whole under the names' slack — and the
rows were strengthened. The scheduler's reference re-pinned (`JOURNALS`, `CLASSES` at 9, 5[, 7]): the corpus's
capture-pointer mutants, refused and never run, number its barrier logs anew — without them both digests are the last
pins', measured.

**K2b-7e-4b-1 — one grammar, two sinks, the names' check, the tape** (2026-11-27, decision 23's step 2): `json.rs`'s
parser is generic over a private sink — the tree, which `parse` returns as before, and the tape's recorder —, its
strings decoded by one iterator of pieces feeding a string, a name's hash or nothing, its numbers lexed once with
their value's parts over their text (the tree's `Number` built from them with two allocations, not three). A member
named twice is the grammar's check for both: `(FNV-1a of the decoded name, place)` on one stack sized at the first
object (`names_bound`), sorted at the object's close, names compared decoded only inside a run of one hash; the
tree's `BTreeSet` of cloned names is gone. `json::tape` reads a body into entries (`ENTRY`, 8 B: a start and the
index past the last descendant) sized once (`tape_bound`), viewed by `Node` — its kind, a number's text, a string
decoded, whether it is a given string, its items, its members, `at` as `Json::at`. A document past `LONGEST`
(`u32::MAX`) bytes is unreadable by both, before its text is checked. `json::bound` gains the names' stack. Not yet
read by the judge (K2b-7e-4b-2). Measured: a tape of zeros of odd length holds exactly its `n/2 + 1` entries; the
names' stack exactly `n/5 + 1` entries beside the tree; a 4 GiB document holds under 1 KiB. Mutants: twelve, each
red (§ the table). Two not run: the length check dropped from the grammar itself (a 4 GiB text read whole by the
laptop), and the hash made constant — correct and slow, an equivalent mutant only M-a's colliding row sees.

**K2b-7e-4a — a body parsed on its first reader** (2026-11-27, decision 23): `answer::Read` parses its body when a
reader first asks — a problem's code at 400 or past it, a predicate, a JSON capture — and keeps the tree for the next;
a body no one reads is never parsed. Nothing decides it before: a laziness decided from the record would pass a
gateway's 404 problem against an expectation of none, which `tests/judge.rs` already holds. The measured reads (§
Measured, the judging floor) assert their status alone: their 256 KiB tree is no longer built. Measured: an answer of
65 chains held under 64 B read for its status, its code and no predicate; its first reader's tree within its bound; a
second reader nothing. Mutants: parsed always, parsed never, a problem's code read only once parsed — each red.

**K2b-7e-2b-3c-2 — the remedies, the egress's caps, the old fit retired** (2026-11-27, decision 22 as amended for
K2b-7e-2b-3): `memory::remedies(memory, knobs, floors, planned)` — each knob its owner's (the egress's ports as it
divides them, the reader's records ahead, the journal's lines behind, the plan's answer cap, the operator's memory),
its number the largest that admits the run, found by bisection over the same fit between the knob's floor and its
value (the ports' floor the least that gives each source the sockets the plan needs of it); the least memory at the
knobs and at the needed caps; a slice named where it would lower the binding part (the read gate for the pool's, the
parse for the tails and the judging). `Egress::caps(ports, sources)` states how an egress divides a total — the knob a
host turns, not a uniform cap `with_ports` would split. `fits::fits`, `Budget` and `Refusal` retired: nothing called
them; the need's rise with each count stays (`fits::needed`), and a read-ahead past the header's is the machine's own
refusal. Mutants: see § Executors, six, each red.

**K2b-7e-2b-3c-1 — the pure fit** (2026-11-27, decision 22 as amended for K2b-7e-2b-3): `memory::fit(planned, memory)`,
a pure function of what admission fixed (`Planned`: each source's runtime, cap, connection, egress's heap, answer,
carrier's and keeping; the reader, the decoder, the journal, stage 2's judging, the gates' client; the machine's terms,
the late records in place of its records, the shell's and the carrier's attempts; the catch-up) and the stated memory
(`Memory::declared`). Its terms are closed (`Part`, 21 parts, each its phase, the holder of its count, its owner, its
basis); a run needs the largest of the fixed parts with the stages' judging, with the gates' probes and client, with the
run's own and its pool at the catch-up's rate; the pool's budget is what the run's fixed parts leave. Every sum checked,
a part saturated refused — a memory stated at its most never admits one —, a runtime of no stated events unstated. A
refusal names what failed (the memory, the pool's I2, an overflow, an unstated part), the least memory and the binding
term, the largest of the phase that binds. A fit says whether every count is held: the pool's is not until the read
gate. Not yet called: K2b-7e-2b-3d wires it into admission. Mutants: see § Executors, nine, each red.

**K2b-7e-2b-3b — the host ports' statements** (2026-11-27, decision 22 as amended for K2b-7e-2b-3): each host port
states its own, a required method, no default. `Egress::io_bytes` — the heap its `Io` holds a connection of its own
(TLS's buffers; a plain socket's none, `TcpEgress`' 0) —, which REST's needs carry apart as `Costs.io`, the host's.
`Decorator::bound(shape)` — the most it decorates an attempt of a record of that shape with, its headers and their
bytes, a function of the record (a proof's size grows with its target), or unstated —; REST prices an attempt's
decorations at the bound for the plan's largest record (`AttemptCosts.decorations`, `HEADER·bytes + FIELD·headers`,
its copies into the request) and holds them where they enter: decorated headers past the bound for the attempt's own
record are a `rest.decorator` fault, after their names and values are checked. `Stand::gate_bytes` — what its gates'
client holds during the stages and through the run —, relayed by `Carrier::gate_bytes`. `Egress::caps`, the remedies'
map, comes with its consumer, K2b-7e-2b-3c. Measured (`tests/costs.rs`, `tests/frame.rs`):

| | measured | stated |
|---|---|---|
| a connection whose `Io` holds 32 KiB of its own | 70,000 B | 73,728 B (95 %) |
| four decorations of 8 KiB, copied into the request | 65,776 B | 66,768 B (98.5 %) |

Mutants: see § Executors, each red.

**K2b-7e-2b-3a-3 — the shell's attempt, sockets by source, the machine's terms** (2026-11-27, decision 22 as amended
for K2b-7e-2b-3): the shell states what it holds of the attempts of records summed into a shape, whatever carries them
— `run::attempt_share`: `ATTEMPT` each, and its copy of each captured hole's value the machine dispatches it with
(`run::CAPTURED`, at most `VALUE` and its entry, with its names' bytes). REST's needs are its own source's steps taking
sockets (`peaks.sockets[s]`, none for a source with none), not the plan's whole pending for every source — which refused
plans that fit. `fits::terms` gives the machine's terms apart, its records among them; `fits::late_records` prices the
records of the steps late by a jitter, which admission's pool takes in their place (K2b-7e-2b-3c) — each count of the
late shape at least the pending's, held over the corpus. Measured (`tests/costs.rs`): the shell's share of an attempt
with a captured hole of `VALUE` bytes, 16,748 B of 18,036 B stated (93 %). Owed to K2b-7e-2b-3d's whole-run
measurement: the records a late handle holds past its step's end against `late_records` — a row here would build its
own handles, or sample one instant at which the machine's end and the attempt's look are unordered.
Mutants: see § Executors, each red.

**K2b-7e-2b-3a-2 — the carrier's attempt by class, hyper's head buffer** (2026-11-27, decision 22 as amended for
K2b-7e-2b-3): a carrier states what it holds an attempt, process-wide — `Carrier::attempts(peaks) -> Result<AttemptCosts,
Unstated>`, coefficients the shell multiplies by a shape's counts (`AttemptCosts::price`): each attempt's own part, a
target's literal byte and hole, a header's, a field, a body's byte and hole, a captured hole's copied value and its names'
bytes. `Costs.attempt` left `Needs`: one fact, one statement. REST's: each attempt `ATTEMPT`; a target's byte 6 (encoded
at most `%XX` in its frame, and the URI's copy), its hole `6·TAKEN`; a header's byte 2 (the frame's and the header
map's), its hole `2·TAKEN`; a field 296 (its slot and its map entry, grown by doubling: 292 measured past 1,024 fields);
a body's byte 2 (the frame's, its copy in the request), its hole `2·VALUE`; a captured hole `VALUE` and its entry (REST
copies the attempt's captured). A decorating plan's attempts are unstated until its decorator states its decorations
(K2b-7e-2b-3b). A JSON token's written value is sized at once — `serde_json::to_vec` grew it by doubling, a hole of
quotes holding 73,389 B where 49,317 B are its own. The connection carries hyper's head buffer, 8 KiB at first, grown to
the largest head it writes and never shrunk: `head_buffer(head_bound(record, header))`, twice the head past 8 KiB, the
head bounded by the plan's largest record (its target encoded, its holes at `TAKEN`, its headers, each field's
separators, the operations' method, path and content type, the correlation, `HEAD_FIXED` 512 for the rest). Measured
(`tests/frame.rs`, `tests/costs.rs`):

| | measured | stated |
|---|---|---|
| a target of 16,385 spaces, past a small request's | 98,308 B | 98,304 B by class (100 %) |
| a target's hole of `TAKEN` quotes | 65,647 B | 65,643 B (100 %) |
| a header of 65,537 bytes | 131,132 B | 131,380 B (99.8 %) |
| a header's hole of `TAKEN` quotes | 32,939 B | 33,187 B (99.3 %) |
| 1,025 fields | 312,967 B | 317,750 B (98.5 %) |
| a body of 65,537 bytes | 131,118 B | 131,074 B by class, its content type within each attempt's part |
| a body's hole of `TAKEN` quotes | 49,317 B | 49,275 B by class, likewise |
| a connection after a head just past 16 KiB | 56,281 B | 66,498 B (85 %: hyper's buffer grows to between the head and twice it) |
| an attempt in a run, a target, a header and a body | 46,493 B | 70,972 B (66 %: its request's copies written before the next read runs) |

Found with it: a target longer than the URI's 65,534 bytes encoded cannot be built — a `rest.request` fault at its
dispatch, which the validator does not refuse (§ Owed). Mutants: each coefficient down and up a step, a captured
hole's copy without its value, the body unpriced, a JSON token grown by doubling, a control byte escaped in two, the head
buffer at the head, the head's target unencoded, the connection without its head buffer, a decorating plan stated —
seventeen, each red.

**K2b-7e-2b-3a-1 — a request's bytes by class, its frame sized at once** (2026-11-27, decision 22 as amended for
K2b-7e-2b-3): a record's shape commits its request's bytes and holes by the class a carrier renders them by — its
target (path values, the query's names and values: encoded), its headers, its body — where one role summed them
(`RULES_REVISION` 14; the reference derives all six from each record). A frame's target and body are sized at once:
the target's values filled first and its encoded length counted (an unreserved byte as it is, any other `%XX`), the
body's values written first and its length counted — where the target grew by doubling through each encoded value
and the body by doubling once its values outran their spans, keeping the capacity in the frame. So the carrier's price
of a frame is its bytes by class, which K2b-7e-2b-3a-2 states and holds two-sided. The corpus gained a mutant that
queries a call, so the reference derives a query's names too; the differential's digests (journal 9, machine 5,
fold 7) were re-pinned with it — the corpus changed, no machine or fold, each other log's draws its own key's.
Mutants: see § Executors.

**K2b-7e-2b-2c-4 — a carrier's source costs** (2026-11-27, decision 22 as amended for K2b-7e-2b-2c): a carrier states
what it holds a source beside its sockets — `carrier::SourceCosts { scratch, head, tail, stated_by }`, by
`Carrier::source_costs(peaks, source)`, socketless or not, a source it does not know refused, its charge their sum.
The REST carrier's: its read scratch (`rest::SCRATCH`, the I/O's one a thread), a head as it is copied
(`rest::HEAD_COPY`), one answered tail — `judge::bound` at the profile's cap of the plan's largest record and its
problem —, stated by the library. The head's copy is a plain function (`rest::head`), its vector of fields sized at once
by the head's count of fields: the cut measured the copy growing by doubling at 65 fields of one name, just past a
power of two; sized at once there is no doubling to measure, and the bound is the fields' bytes and their slots —
`READ + FIELDS·size_of::<(String, Vec<u8>)>()`, 21,184 B, `FIELDS` hyper's hundred left unset (setting it moves its
parse of every head to the heap; 101 refused, `tests/rest.rs`). Measured (`load-run/tests/head.rs`, a counting
allocator by thread; `tests/costs.rs`):

| | measured | bound |
|---|---|---|
| a head's copy, 100 fields filling `READ`, distinct names | 20,766 B | 21,184 B (98.0 %) |
| the same, one name | 20,700 B | 21,184 B (97.7 %) |
| 65 fields of one name | 19,175 B | 21,184 B (past it once the copy grows by doubling, the mutant below) |
| an answer with the worst head, distinct names, per read | 69,718 B | 77,826 B (90 %) |
| the same head of one name, per read | 46,846 B | 77,826 B (60 %: hyper keeps a name once) |

Not yet charged: K2b-7e-2b-3's admission sums `SourceCosts::total` over the plan's sources.
Mutants, each red: the copy grown by doubling; the fields' bound short — it survived at first, its executor building
`FIELDS` fields, the constant it checks; red once it took hyper's own limit —, loose; the head, the scratch uncharged;
the tail of no record, at no cap, without the problem; a host's word for the library's; an unknown source stated; the
largest, not the sum.

**K2b-7e-2b-2c-3 — the tail held** (2026-11-27, decision 22 as amended for K2b-7e-2b-2c): the REST carrier's
answered tail — arrived, read, judged — is a plain function (`rest::answered`), owning its head and body, so nothing it
holds waits on anything; its body's tree is let go before the attempt ends, its buffers when it returns, the instant its
connection may idle from. `answer::Read` is neither `Send` nor `Sync`: an attempt is a `Send` future, so no carrier holds
a tree across an await — a source's thread holds one at a time, as its memory is charged. A head and body read past a
look are let go before the attempt's closing report waits. The shell keeps an answer's captures only if each value is
one `answer::is_taken` admits (`shell.capture.not_taken`), and states what keeping them holds — their slots and each
spec's names, the values moved — as `run::ending` of the plan's largest record, once a source, whatever carries it.
Stage 2's documents are sized at once to the cap, and `stages::judging_bound` states what it holds: its document, the
step it judges by and its medians, judging it at the longest pointer a shape takes. Measured (`load-run/tests/judging.rs`,
a counting allocator by thread):

| | measured | bound |
|---|---|---|
| stage 2 at a cap of 4,097 | 202,020 B | 556,064 B (36 %) |
| stage 2 at a cap of 65,537 | 3,227,941 B | 8,481,898 B (38 %) |

One-sided: no shape stage 2 judges is the parse's densest, so its bound — the densest's — is not approached until chains
are a shape it judges (owed with its time, § Owed).
Mutants, each red but one: a read sent between threads (the doctest compiles); a document grown; the keeping's
slots unpriced; its names unpriced; a value no take makes kept. **The stage's document unpriced survives**:
its executor is one-sided while stage 2's shapes are not the densest — owed with chains as a shape. Held by
the order of the code, not by an executor: the tree let go before the attempt ends and a head and body before
a closing report waits — each a peak's order within one source's own, which the terms sum regardless.

**K2b-7e-2b-2c-2 — the parse's bound and its roles** (2026-11-27, decision 22 as amended for K2b-7e-2b-2c):
parsing `n` bytes holds at most `json::bound(n) = 2·size_of::<Json>()·(n + 1)` — a node 64 B; its densest input, an
array of one element, four slots (std's first growth) on two bytes; the added slot for an input cut inside it (`[1`,
258 B on two bytes) —, and a pointer's tokens at most `json::pointer_bound(n)`, three strings and a byte a byte and
std's first four slots. A record's shape commits two roles more (`RULES_REVISION` 13): its largest predicate literal
(`literal_bytes`, a judge parses one at a time) and its longest pointer, a predicate's or a capture's
(`pointer_bytes`). `answer::bound(cap, record, problem)` is the body's tree, one pointer's tokens, and one value read at
a time — a literal's tree, a captured hole's value (`HOLE`, a number of `TAKEN` digits parsed, copied, parsed again),
a `Location`'s segment decoded (`DECODE`, sized before it is decoded and refused past `TAKEN`) —, the predicates
read before the captures; `judge::bound` adds what it takes: each value at most `VALUE`, its names, its slot in what
is taken and in what is missed. `answer::is_taken` is `take`'s contract — a carried text quoted, or a carried JSON
number — checked with nothing allocated, for the shell's boundary (K2b-7e-2b-2c-3); `fills` refuses a structure
before it parses one, whitespace before it or not, its refusal's class the parse's (a host's array of `VALUE` bytes
was a tree of megabytes before `not_scalar`). Measured (`command-log/tests/parse.rs`, `scheduler/tests/tail.rs`):

| | measured | bound |
|---|---|---|
| 17 chains as deep as an answer nests, 4,353 B | 555,810 B | 557,312 B (99.73 %) |
| 257 chains, 65,793 B | 8,405,250 B | 8,421,632 B (99.81 %) |
| objects nested 127 deep, 636 B | 44,986 B | 81,536 B |
| 65 empty pointer tokens | 4,608 B | 4,841 B (95 %) |
| judging a body of 17 chains and a literal of its size | 1,110,596 B | 1,115,453 B (99.6 %) |
| a header's value at `VALUE` under names of 8 KiB, no body | 33,187 B | 42,288 B (78 %; the value or the names gone, past it) |
| a hole of 8,192 digits | 33,026 B | 34,237 B (96.5 %) |
| a `Location` of 8,192 `%22` | 24,578 B | 25,912 B (94.9 %) |
| a pointer of 65 tokens | 4,608 B | 5,721 B (80.5 %) |
| a host's array of 64 chains in a hole, whitespace before it | under 4 KiB: never parsed | |

The bound of a hole's value holds of a value a take makes; a host's carrier is held to that at the shell's boundary by
K2b-7e-2b-2c-3.
Mutants, each red: the bound's added slot dropped; the bound a depth looser; one node a byte; objects pre-sized;
a token's slot held once; a literal not walked; a capture's pointer not walked (it survived the corpus, whose
captured pointers are never a record's longest: red by `tests/shape.rs`' own row); the literals summed at their
most; a hole's value, a segment's decoding, a pointer's tokens unpriced; the body and a value read at their
largest; a segment sized by its escapes; a take's value checked by its first byte; a structure parsed; a value
taken, its names unpriced. A slot of what is taken or missed is no row's deciding term: held by construction,
as `fits` prices its own.

**K2b-7e-2b-2c-1 — the runtime and its events** (2026-11-27, decision 22 as amended for K2b-7e-2b-2c): a source's
runtime holds `RUNTIME` of its own — a `current_thread` runtime with I/O, its thread and its stop, its places in
`Live` — and its driver's events at `EVENT` each, as built: `Runtimes::own(s)` states them for each runtime built,
whatever the plan's sources, none for a test's. `EVENT` is libc's `epoll_event`, stated by target: 12 B packed on
x86_64 and on x86 with glibc, 16 B elsewhere on Linux, none on another system — where nothing is stated, admission
refuses. A runtime is built, and recorded, with one event at least (it was recorded as asked). `live::events_for(caps)`
prices a source's events before anything is built; `Live::start`, of events no cap checked, is a test's. Measured
(`load-run/tests/sources.rs`):

| | measured | stated |
|---|---|---|
| a runtime's events, 1 and 9 runtimes at cap 4,096 against cap 0 | 49,152 and 442,368 B — 12 B an event, exactly | `EVENT` |
| a source's own, from 1 runtime to 9 (one alone the same: no constant beside them) | 2,571 B (2,547 B and two events) | 2,624 B (98 %) |

Twenty runs idle and twenty beside four busy loops: none outside its bounds.
Mutants, each red: an event of 16 B; an event's size unconditional (by the lint: the CI's arch sees one);
the runtime halved; the events uncharged; a test's runtimes priced; the events recorded as asked; unchecked
runtimes a host's.

**K2b-7e-2b-2b-3 — the carrier's boundary** (2026-11-27, decision 22 as amended for K2b-7e-2b-2b): what a carrier
hands the shell is held where it enters, before the frontier takes it. A code is made by `Code::of` alone — the
machine's `Code` a name of at most `CODE` bytes at its exact length, or none, its payload private —, so a closing
report's code and an answer's are held by type, whatever a host's carrier passes. `Attempt::ended` keeps an answer's
captures as the shell prices them: each matched to a distinct spec of its step's by its subject and slot — in any
order, as the machine looks them up; two of one subject and slot where two specs name them —, rebuilt in that spec's
names, its value's buffer at most `VALUE`; a capture no spec names, one past its specs, a buffer past `VALUE` is a
carrier fault (`shell.capture.unspecified`, `.repeated`, `.too_large`; its detail positions and sizes, never a value
or a carrier's text), the run stopped and the answer never journaled. The library's own values are held at their
length (`answer::quoted` sized by its escapes): at their worst, `TAKEN` quotes, `VALUE` bytes exactly, their buffer no
larger. `Ran::unsent` names at most `UNSENT` (32) codes, the library's carrier eight; a carrier minting more is
counted in `unsent_past`, never under `other`, which is `Code::Other`'s.

Mutants, each red: the boundary bypassed; the check after the push (the answer taken before the fault); captures
matched in order; a spec taken twice; a buffer of exactly `VALUE` refused; a capture kept in its carrier's names
(by `stamps.rs`'s own test: their capacities); a value taken grown (its buffer past its length); the
unsent codes uncapped; the codes past the cap dropped, not counted; a code of any shape made (the doctest
compiles). Under the check after the push the run ended, its fault unseen: `run` reads a raised fault at an
iteration's start and after a step, and ends once nothing is due and no attempt is alive, so a fault raised after
its attempt's answer was taken can be lost. No attempt reaches it — `Attempt::ended` consumes the attempt, and
every other fault leaves its step alive until its deadline —, so it is noted, not owed.

**K2b-7e-2b-2b-2 — the journal's channel built from admission** (2026-11-26, decision 22 as amended for
K2b-7e-2b-2b): `sink::Journal { behind, out }` — the lines its channel holds, at least one, and what its output
holds as its host states it — is passed to `admit` and held by `Admitted`; a run's journal is built by the stages
alone (`Written::start` the library's, `Written::assumed` a test's, its header's admission marked assumed): the host
gives `Gated::start` only an output, opened before the run's clock starts, and an output larger inline than `OUT`
(256 B) is refused before it is opened — the writer's thread holds it twice, in the closure it runs and in what it
hands back. `Journal::bytes` states it: the channel's slots (std's array, a line and a stamp each); `behind + 2`
lines at the plan's largest record's `line_bound` — the channel's, the one the machine waits to send, the one the
thread writes —; the writer's `BUFFER`; `THREAD`, the thread and the channel's own; the output's. A gate's refusal is
written in the caller's thread, through `stages::refused` as before. The journal that keeps every line, `Lines`, is a
test's. `Ready::t0` gives the host T0's wall instant, which its `open` no longer sees. Measured
(`load-run/tests/channel.rs`, its feeder's sends begun `behind + 2`, done `behind + 1`; `tests/memory.rs`):

| | measured | stated |
|---|---|---|
| a journal 64 lines behind, its lines 4,031 B, its output 4 KiB on the heap | 294,397 B | 294,718 B (99.9 %) |
| 128 lines behind | 566,717 B | 567,038 B (99.9 %) |
| 4 lines behind | 39,097 B | 39,418 B (99.2 %) |
| 1 line behind, and 0 clamped to 1 | 26,332 B | 26,653 B (98.8 %) |
| the thread and the channel's own (`THREAD`) | 1,727 B; 72 B less when the writer never waited on an empty channel | 2,048 B |
| a burst's first line, from 64 dependents to 128 | 241 B a dependent (an output 56 B, a line 216 B) | under 440 B — four outputs and a line; made at once, two lines more |

Twenty runs of the journal's executor idle and twenty beside four busy loops: none outside its bounds.
Mutants, each red: the channel twice the admitted (its sends never stand at `behind + 1`, refused in bounded
rounds); one line beside the channel; the slots, a slot's stamp, the lines, the writer's buffer, the thread,
the output uncharged; no line behind clamped; an assumed journal unmarked; the journal kept whole a host's; a
burst's lines made at once (by the run); the stages ignoring the admitted journal; an output past `OUT`
taken; the output opened after the clock (an open of 2 ms past T0).

**K2b-7e-2b-2b-1 — the journal's lines and its writer** (2026-11-26, decision 22 as amended for K2b-7e-2b-2b): a
step's lines are made one at a time — `journal::events` an iterator, its input's line made when it is taken, `run`
and `sim` taking them as they come —, so a run holds the line it sends, never a burst's lines at once; a read's
dependencies built from a slice at their exact length (collected through a flatten, one to three dependencies held
four slots). A line is priced by the journal's owner: `journal::line_heap`, the walk, every type taken apart whole,
and `journal::line_bound` of the plan's largest record, the largest of its rows — a read's key, its dependencies
(`size_of::<journal::Dep>()` each) and its barriers' keys; an answer's captured slots (`size_of::<Slot>()` each,
their subjects' and slots' bytes) and two codes of at most `CODE`; an end's box and its step dependencies' lines; a
fault's code. A record's barrier dependencies' keys are a role of its shape, `each_bytes` (`RULES_REVISION` 12, a
field 2b-1 missed): a barrier may have no member, so no step's key bounds its key. `fits` prices the machine's copy
of them — the bindings of the line it reads, one record's at a time — from the plan's largest record. The writer
streams through a fixed `journal::BUFFER` of 8 KiB (`frame::Out`: a stream's lines in their canonical bytes, the
digest taken as the bytes leave), each line whole in its output and the output flushed once written, the header
and the trailer the same way; an output's failure stays an I/O error; a bound line's numbers are written as they
are formatted (`wire::u64s_vec`). The golden journal's bytes are unchanged. Measured (`scheduler/tests/lines.rs`, a
counting allocator by thread; `tests/costs.rs`):

| | measured |
|---|---|
| a line made, over the corpus's clean logs and a read of 0–17 step and barrier dependencies | the bytes made for it, as it is taken, its walk's, exactly; within its record's bound |
| a read, an answer capturing every spec under two codes of 64 B, an end, at 0, 1, 4, 5, 9 and 17 | each its row's price, exactly; the bound their largest |
| the machine's bindings of a read line | `fits`' price from its record alone, exactly |
| `Writer::new` | its buffer and the header's box: 8,192 + 248 B |
| a line written — the shortest (177 B), 8,191, 8,192, 8,193 and 24,576 B, ten buffers of escapes, an end's bound lines | 0 B given; whole and flushed once written; a header of 24 kB read back with its trailer |
| `size_of` of `record::Dep`, `journal::Dep`, `machine::Binding`, `Slot`, `Ended` | 96, 96, 96, 80, 168 B |
| a plan of 2,049 steps, every other awaiting every read before it and 64 memberless barriers of 1 KiB labels | 1,081,801 B of the 2,199,828 `fits` counts (49 %) |

Mutants, each red: a read's dependencies unpriced; its barriers' keys unpriced; one code an answer; an end's
bound lines unpriced; the bound a read's alone; a barrier's key, an answer's slots, an unsent code, an end's box
not walked; a read's dependencies collected through a flatten; a step's lines made at once; the machine's
bindings unpriced; a record's barriers' keys not walked, not summed, not at their most (by the reference and the
rules' pinned meaning); a line written through a copy; a bound line's numbers written through a string; the
buffer grown, not written out; a full buffer not digested (a line past the buffer's trailer refused); no flush
at a line's end; no write at a line's end; a write's error an encoding's; the header written through a copy.
The machine's bindings cannot be seen by its peak — the records they bind, priced at their bound, dominate them
(in the barriers' row about 72 kB — 65 bindings of 96 B and 64 keys of 1 kB — of the 2.2 MB counted) — so their price is held exactly on the object, not by a cost
row; `size_of::<record::Dep>()` in `journal::Dep`'s place is an equivalent mutant at this build (both 96 B).

**K2b-7e-2b-2a — the reader by construction** (2026-11-25, decision 22 as amended): a replay's reader holds what
`live::Reading` states, each term by construction —
- **its input**: two buffers of `log::BUFFER` (8 KiB), the file's and a compressed log's decoded output's, and the
  boxed reader over them;
- **the verifier's**, each reserved at the commitment's size and never grown: a line and a chunk released at the
  header line's while it is read (`Commitment.header_line`, committed now), then a line at the largest step line's (or
  the longest trailer's) and a chunk released at the largest chunk's and a trailer's; a chunk held. A line past its
  limit is refused (`Diverged`: "line N, past its L bytes") before a byte past the limit is held;
- **the commitment** the verifier keeps; **the line** the log's reader copies out and decodes, three lines at worst —
  its copy, and serde's scratch for an escaped string; **the header**, decoded once and shared with the run
  (`Reader::shared_header`; the run had a clone of it), priced exactly by `shape::header`.

A line decodes holding nothing but its value: its canonical bytes compared as the value is serialised, no re-encoded
copy; the wire's texts — decimal numbers, base64 — read where the deserializer lends them, base64 checked canonical a
768-byte group at a time on the stack and written as it is encoded (each held a copy, base64 two: 10 kB beside a
5.5 kB record of a 4 KB payload). zstd's window is bounded: the writer's `window_log` 21 at every level (level 3's own,
four times level 1's), the reader's `window_log_max` the same — a frame asking more is refused at its header, never
allocated; libzstd's context at that window, outside the counted heap, is `DECODER` (2.6 MB). The plan commits **the
largest record's shape** (each count at its most over one record; the validator's revision 11): a record the reader
holds ahead of the machine, none of the pending yet, is priced by it. The source's channel carries records in the `Arc`
the machine holds them in — a channel's slot a pointer's, not a record's — and `source::Reader { ahead, reading }`,
built by `Replayed::open` with its channel, states what the reader holds at most: its reading; `ahead + 2` records at
the largest — the channel's, the one its thread decodes or waits to send, the one the run took; the channel's blocks
(tokio's, 32 messages each: those its messages span across a block's edge, one read and not yet handed back, three
handed back for reuse past the tail — `reclaim_block` walks at most three); its thread. The plan carries it to
admission, and `run` refuses a source holding more records ahead than its plan's reader (`Inadmissible::Reader`).
Measured (counting allocators: `command-log/tests/reading.rs`, `tests/shape.rs`; `load-run/tests/costs.rs`):

| | measured | stated |
|---|---|---|
| a replay's peak, a plain log of 6.4 kB escaped lines in chunks of four | 82,914 B | 93,608 B (89 %) |
| the same compressed (libzstd's context apart) | 91,186 B | 101,880 B (90 %) |
| a header longer than two chunks, its scenario 40 kB of escaped text: its peak | 601,596 B | 753,328 B (80 %) |
| past that header: a step's reading | 191,977 B | 192,611 B (99.7 %) |
| libzstd's context at 2^21 (`ZSTD_sizeof_DCtx`) | 2,586,408 B | 2,600,000 B (99.5 %) |
| a replay's reader, its channel of 64 full, 12 kB records (no escaped string: the scratch apart) | 1,408,056 B | 1,416,871 B (99 %) |
| decoding a 6.4 kB line of no escaped string, at its peak | its record's heap exactly | |

The 11 % slack of a replay's peak is in part serde's scratch, stated at a whole line (`3 · max(header, line)`) while
the fixture's escaped label is a third of one (≈ 4.4 of 10.7 kB); the rest is not attributed. Mutants, each red: a line read past its limit; the verifier's buffers grown, not
reserved; the header's buffers kept; the canonical check by a copy; base64 written through a copy; a text read through
a copy; the reader's window unbounded; the writer's window its level's; the largest record not committed; the header's
limits not walked; the line's term dropped; the header's reading dropped (red once the wide header carries an escaped
scenario, its scratch past a step's chunks — it survived a header of plain text); libzstd's context not charged; the
run's reader unchecked; the two beside the channel uncharged; the channel's blocks uncharged; the channel twice the
reader's (red once the test's wait for a full channel is bounded — unbounded, the mutant spun it 27 h).

**K2b-7e-2b-1c — `fits` priced by the plan's shapes; a captured number bounded** (2026-11-24, decision 22 as
amended): `fits::needed` takes the peaks alone (`Budget.line`, a mean, gone): the cells — `ENDED_CELL`, `LIVE_CELL`
without their records, `BARRIER` — and, priced by the committed shapes at the run's own sizes, the records of the
steps not ended as they hold decoded (`shape::bound`), the producers their captured holes are bound to and the values
those take (a subject and a slot copied each, a value at most `VALUE` = `2·TAKEN + 2` bytes), a dependency's binding
and its producer's waiter, what the held keep once ended (their keys, their captures), the barrier keys' bytes. A
captured JSON number is taken only within what a use carries, as a string is — `TAKEN`, 8 KiB —, so `VALUE` bounds
every value (its twin in `vectors/judge.py`; the golden's case of a number of 8,192 digits taken and one of 8,193 not).
The scheduler's executor (`tests/costs.rs`) prices each row's steps at the largest shape among its records and holds
thirteen rows, each within what `fits` counts and, on the rows a constant or a price is pinned on, at least its floor
of it:

| row | measured of counted |
|---|---|
| ended in the window (and at 2^14 + 1) — `ENDED_CELL` | 75 % |
| host calls at one instant, 32,769 — `LIVE_CELL` | 81 % |
| REST updates of 4 KiB bodies at one instant — the record's bytes | 89 % |
| barrier keys of 1 KiB labels — the keys' bytes | 88 % |
| captures kept in the window — what the held keep | 97 % |
| captured values sent back — the values a hole takes | 85 % |
| REST reads, each awaiting the 65 before it | 69 % |
| the rest: not ended, REST reads (bursts, at 0, not ended), chained, barrier keys | 41–73 % |

Beside the shapes, a step's cell held at most 914 B ended and 2,256 B not ended (a burst of host calls), so
`LIVE_CELL` 3,776 → 2,944 (its record now priced apart) and `ENDED_CELL` kept (1,216). Mutants, each red but one: the
records' shapes not priced; a captured value not priced; what the held keep not priced; the barrier keys' bytes not
priced; a live cell as before the shapes; an ended cell a quarter lighter; a live cell doubled (by its floor); a
captured number taken past `TAKEN` (by the judge's twin). **A dependency's binding not priced survives** every row:
`LIVE_CELL`'s margin over a dependent's own cell exceeds the bindings of 65 dependencies — owed, a row where they pass
it.

**K2b-7e-2b-1b — the plan's byte peaks committed** (2026-11-24, decision 22 as amended): the validator, as it reads,
sums the shapes of the steps not ended (the plan's bound's pending set), of those not ended were every end a jitter
later (the handles a run holds past their deadline until their looks), what the held keep once ended (their barrier
keys' bytes, their capture specs and those specs' subjects' and slots' bytes — an ended cell's record is let go), and
by a call's or a probe's source the steps taking its sockets (`plan::socket`: a bus step and a host call take none);
`Peaks` commits each count at its peak (`pending_shape`, `late`, `late_shape`, `kept`, `sockets`) and the barrier keys'
bytes, one a key (`barrier_bytes`: a machine keeps each key for the run), and the commitment
its largest step line and its largest chunk of step lines, a last one short of its size included (`line`,
`chunk_bytes`); `RULES_REVISION` 10 — a commitment of revision 9 is stale. A shape carries its captures' bytes beside
their count. Its executors: the reference (`vectors/reference.py`) folds each line's shape, as the validator emits it,
over the same sets by definition — quadratic — and agrees on all 2,841 logs of the corpus, having derived itself what
of a shape it can from the record (its key's bytes, its captures and their bytes, its source); it refuses a corpus that
reaches no step late past pending (37 logs reach one), no source's socket (2,673), no capture held (847), no captured
hole (304); the commitment's line and chunk against a log of 19 steps in chunks of four. Mutants, each red: a step past
its end not kept late; the late kept past the jitter; what the window drops kept still; what the held let go kept
still; a source's sockets never let go; a pending shape never let go; the barrier keys' bytes not counted; the
largest line not taken; a chunk's bytes carried past its prefix.

**K2b-7e-2b-1a — a record's shape and its price** (2026-11-24, decision 22 as amended): `command-log::shape` — a
record's shape, the plan-intrinsic counts its bytes are priced by (its strings' and byte strings' bytes, its
sequences' elements and non-empty sequences by element type, its captured holes and their subjects' and slots' bytes,
its barrier key's bytes, its request's literal bytes and holes), summed and taken at their peaks as the validator will
commit them (K2b-7e-2b-1b); `shape::heap`, the bytes a record decoded from its line holds in its own `Arc` at this
build, exactly — its strings at their length, a byte string at base64's estimate (`3·⌈n/3⌉`), a sequence as serde
grows it (four slots at first, then doubling) times its element's size; `shape::bound`, the price of a shape — the
sequences at twice their elements and two a sequence, the byte strings at two bytes past their length — at the run's
own sizes, so a price re-pinned needs no new dry run. Every type is taken apart whole: a field added to a record does
not compile until its shape counts it. Its executor, `command-log/tests/shape.rs` (a binary of its own, a counting
allocator): over 512 records generated across every field — strings plain, escaped and multi-byte, byte strings
empty to 200 bytes, sequences of none, of up to four, of five to nine, of 16 or 17 — what each holds decoded equals
`heap` to the byte, is at most `bound`, and at least a third of it; a record of no sequence priced exactly. Mutants,
each red: a predicate's pointer not counted; serde's first four slots forgotten; a sequence bound at its elements once;
byte strings' padding left out (by the exact twin); base64's estimate taken as the length; the `Arc` left out; a
capture's source not counted.

**K2b-7e-2a — the shell's costs stated and held; `fits` re-pinned** (2026-11-24, decision 22): the carrier states
what its parts hold (`carrier::Costs` in `Needs::Sockets`: a connection, an attempt beside it, an answer at the
profile's cap), the shell its own share an attempt (`run::ATTEMPT`); `tests/costs.rs`, a binary of its own, holds each
two-sided — at most what is stated, at least 80 % of it (85 % for the worst head):
- **The instrument.** An allocator tagging every block with its owner in a prefix: the stand's futures run tagged,
  and their blocks are theirs wherever they are let go; a block moved counted at its old and its new size at once
  (V2: the system's realloc may hold both). A pipe of two fixed rings the stand allocates — a socket's buffers are the
  kernel's, never the harness's heap. The virtual clock, sampled at instants where nothing else is due: the bytes
  held and the peak since the instant before. The machine's own bytes subtracted by `sim` running the same plan,
  sampled at its probes' dispatch; the run's constants by two sizes of the plan, each just past a power of two
  (257 and 513; the shell's share 4,097 and 8,193), where a container growing with them holds the most a read. The
  counts are the same unoptimised and optimised.
- **Measured**, a read's share beside the machine:

| part | measured | stated |
|---|---|---|
| the shell's own share an attempt, held or at the burst of its dispatch | 1,316 B | `run::ATTEMPT` 1,536 B |
| a connection idle in its pool: its read block, its write buffer, its task, its place | 37,136 B | `rest::CONNECTION` 40,960 B |
| an attempt beside its connection: its task, its frame, its request in flight | 4,619 B | `rest::ATTEMPT` 5,120 B |
| an answer's head at its worst: 16,384 bytes in a hundred fields, its bulk in their names | 69,718 B | `rest::HEAD` 77,824 B |
| an answer of 2 bytes, its head small | 17,133 B | the cap and `HEAD` |
| an answer of 65,537 bytes at the cap, its length declared / chunked | 81,720 / 65,341 B | 143,361 B |

- **Found, and taken into the constants.** (1) **A read block an answer pins**: hyper reads again before the attempt
  can copy the head, and the head's slice keeps the first 16 KiB block, so every answer holds a second block until
  its attempt runs — the "read block" Verifiability put in an answer's charge. (2) **A head's three copies**: that
  block, hyper's map of it (it copies a custom name, slices a value), the judge's copy; a head at hyper's limit costs
  up to 69.7 kB, its bulk in long names the worst (54.3 kB in values, 49.9 kB in three fields). (3) **A body of no
  length grew by doubling** — up to twice the cap, three times while a block moved —, beyond what an answer is
  charged: now reserved at the cap, never grown (its pages untouched stay out of RSS); the chunked row holds it.
  (4) **`fits` under-counted the machine**: a REST read not ended 2,181 B a step against 2,157 counted, and a burst
  read ahead and released at one instant — its outputs and inputs at once, 840 B a read — 2,916 B a host call and
  3,144 B a REST read against some 2,050 and 2,140; its stated 30 % margin was its allocator counting a block moved as
  grown in place. Re-pinned some 30 % above the worst row: `LIVE_CELL` 1,792 → 3,776, `ENDED_CELL` 1,152 → 1,216
  (927 B measured), `BARRIER` kept (569 B); the scheduler's executor gains those rows, each count just past a power of
  two, and a moved block counted at both sizes.
- **Mutants**, each alone, each red: the exact read size dropped (a connection idle below 80 % of its stated cost, a
  body's first part held off its length); a declared body not sized, and a body of no length grown by doubling (the
  first part held off the body's size); the head's parts kept through the body (the same, and the worst head past
  `HEAD`); an answer charged its body only (every answer past its charge); `fits`' live cell as it was (the
  scheduler's executor); a finished task's handle kept — below the bands' resolution, red by its white-box twin
  (`a_task_ended_is_let_go_at_once`).

**K2b-7e-1 — the shell's constants by construction** (2026-11-24, decision 22): one HTTP/1.1 builder
(`rest::builder`: reads exact at `READ` = 16 KiB, writes vectored) for every connection, the pool test's too; a head
past it, or past hyper's hundred headers, a fault `rest.head` naming the operation (a client build of hyper has no
`is_parse_too_large` — server-gated —, so its description, "message head is too large", is the tell the twin pins);
the counting `Io`'s scratch one a thread; the body sized from its Content-Length, at most the cap, a length past the
cap unread at its head; the head copied and the response's parts dropped before the body; a task lets go of its own
handle as its future ends or is dropped (a registry keyed per task, a key ended before its handle kept remembered as
gone). Measured again (`tests/memory.rs`, optimised): a pooled idle connection 39.8 kB after 2 B answers and after
64 KiB alike (46.9 and 71.5 kB before — the drift gone), an open attempt with its connection 47.1 kB (51.5), the
peak while 64 KiB answers are read 50.7 kB (105). The twin of an answer past the cap moved: its length declared past
the cap, it is now unread at its head (10 µs after the request, not 30). Its executors (§ Executors), five mutants
each red — the exact read size dropped, writes flattened, a head too large left `Broken`, a length past the cap read,
the body sized from any length (the last two abort the process on a forged 2^62). The two-sided bands of `CONNECTION`
and `ATTEMPT` go with K2b-7e-2, where admission takes them.

**K2b-7e's measurement — the shell's own bytes** (2026-11-24, this laptop, optimised; `tests/memory.rs`, a binary of
its own, ignored: a counting allocator over the whole process, a stand of its own process, the journal counted, not
kept): 1,000 and 2,000 reads at one instant, each answered 1 s later, a last read keeping the run open while the
connections lie idle; the difference per attempt —

| answers of | an open attempt, its fresh connection with it | a connection pooled idle | at the peak, the answers read |
|---|---|---|---|
| 2 B | 51.5 kB | 46.9 kB | 51.5 kB |
| 64 KiB | 51.5 kB | 71.5 kB | 105 kB |

- The connection is nearly all of it: some 47 kB idle, the attempt 4.5 kB more. Its parts, read from the code: the
  counting `Io`'s scratch, 16 KiB, every read copied through it (`io.rs`, `SCRATCH`); hyper's read buffer, 8 KiB at
  first, grown by its adaptive strategy — capped at 32 KiB by those 16 KiB reads (corrected by decision 22's
  discriminator: not "to fit a body"), a pooled connection 24 kB heavier after a 64 KiB answer; hyper's write buffer.
- The per-attempt peak is some 1.6 times the body — the connection and its growth inside it, not a parse factor.
- So at 10,000 attempts at once some half a gigabyte, and an answer cap (1 MiB in the fixture) times the attempts in
  flight, gigabytes: K2b-7e's bound needs a connection's cost fixed (hyper's exact read size, the scratch
  reconsidered) and the answers being read bounded, not only counted — its design next, under the gate.

**The judging floor's share of the shift** (2026-11-27; the load server, 16 cores, commit `f4811fa`, optimised;
`load-run/tests/pace.rs`, `the_floor_s_share_of_the_shift`): 1,000 reads a second for 30 s, every n-th answer large, the
shift replayed with no catch-up, each attempt's lateness split into the machine's (dispatched past its due) and the
hand-off's (handed past its dispatch):

| large answers | sources | shift at κ = 0 | the machine past J | the hand-off past J |
|---|---|---|---|---|
| 64 KiB, 5, 20 or 83 a second | 1, 4, 16 | 0.3–3.7 ms | 1–7 times | never |
| 256 KiB, 5 a second | 1, 4, 16 | 453, 455, 160 ms | ~607 times, 1.4 s summed | never |
| 256 KiB, 20 a second | 1, 4, 16 | ~505 ms, the cap; 5,101–21,585 never handed | ~2,420 times, 5.7 s | never |
| 256 KiB, 83 a second | 1, 4, 16 | ~504 ms, the cap; 13,396–18,275 never handed | ~10,040 times, 23 s | 6, 3, 1 times |

The shift is the machine's — the judging floor —, the number of sources dilutes it not, and the hand-off is masked by
it: judging runs on the source's thread, and a held machine dispatches nothing to hand. A hold at 256 KiB is about 6 ms
on this box (about 24 ns a byte for the tree; the laptop's about 153). The reads here assert their status alone: all of
it is a tree nobody reads. Decided upon it: decision 23.
Rerun after K2b-7e-4a (2026-11-27; the tree of `dabcc9b`, `FLOOR_READS_BODY` choosing the reads): **M-0**, the reads
asserting their status alone — at 256 KiB and 83 large answers a second 2.9–3.4 ms in 30 s, the machine past J 3–4
times, the 64 KiB rows' level (the same rows were at the 500 ms cap); the floor of a body nobody reads is gone, as
predicted. **M-0′**, every read a predicate on its body — at 256 KiB unchanged (380–507 ms; at 20 and 83 a second the
cap, 5,654–23,786 never handed), at 64 KiB some 3 ms: what laziness cannot spare, which K2b-7e-4b and decision 23's
later steps are for. A residue of about 3 ms, three or four late wakes, stands in nearly every row at either size and
rate — not the floor's; unattributed.

**M-a and M-b, predicted before their run** (2026-11-27, K2b-7e-4b-3; the predictions the Performance role's, written
in K2b-7e-4b's round before any build). M-a is `scheduler`'s `tests/judging_time.rs`: an answer read, judged and
dropped, timed together, 200 runs a row (thirty past 20 ms), twice; twelve shapes — 5-digit numbers, zeros, strings,
strings not ASCII, whitespace, an array of records, members in order and shuffled, 2^15 names of one FNV-1a hash, long
names, chains 127 deep, an object 127 deep of wide levels —; 64 KiB to 4 MiB; readers none, `Exists ""`, a capture of
the last value, `Equals` of it, `Contains` of an absent string, 1, 4 and 16 lookups; the grammar alone (`json::check`)
and the tree beside them; a want of 1, 8 and 64 KiB; variants cold (64 MiB written between runs, outside the clock),
busy (spinning threads, a null judgement its control) and under `GLIBC_TUNABLES=glibc.malloc.mmap_threshold=131072`.
M-b is the floor's measurement with every body read (`FLOOR_READS_BODY`) at 64 KiB to 4 MiB (`FLOOR_SIZES`).
Predicted on the load server, idle, J = 2 ms: `Exists ""` at p99 0.7–2 ns a byte on strings, whitespace and text not
ASCII; 2–4 on numbers; 2.5–3 on members; 5–7 on zeros and chains; S* — the largest size within J/2 — 256 KiB on the
typical shapes and 64 KiB on the densest, 4 MiB on none; the laptop a size lower. M-b: 256 KiB at the 64 KiB rows'
level, 1 MiB adding up, 4 MiB at or past M-0′'s 256 KiB. Refuted, each with its remedy (§ Owed, decision 23's step
2), by:
- **R1**: `Exists` past J/2 at p99 at 256 KiB on numbers, strings or members, idle;
- **R2**: M-b's 256 KiB rows past the 64 KiB rows' shift, or an attempt never handed;
- **R3**: the tape past the grammar alone by 25 % at 1 MiB cold on zeros or chains;
- **R4**: four lookups past twice `Exists` at S*;
- **R5**: `Contains` of an absent string past `Exists` by half of it at S*;
- **R6**: p99 up 20 % under the low mmap threshold at 1 MiB;
- **R7**: the names of one hash past four times the members' grammar at the same size — the tree shares the names'
  check now, so it is no reference;
- **R8**: a want of 64 KiB past `Exists` on 64 KiB of numbers.

A smoke run at 64 KiB on the laptop (4 cores at 1.6 GHz under a load of 2.4 to 3.7) preceded this text and already
shows R4 and R7 there: four lookups on members 3.9 ms at p99 against `Exists` 1.5 ms; the names of one hash 10.6 ms
against the members' 1.5 ms. The server decides.

**M-a and M-b, measured** (2026-11-27; the load server, 16 cores, idle, the tree of `92446ec`; p99 the larger of two
passes). `Exists ""` at p99, µs, against the grammar
alone and the tree:

| shape | 64 KiB | 256 KiB | 1 MiB | 4 MiB | ns a byte | the grammar at 1 MiB | the tree at 1 MiB | S* |
|---|---|---|---|---|---|---|---|---|
| numbers | 178 | 688 | 2,736 | 11,059 | 2.6 | 2,676 | 18,424 | 256 KiB |
| zeros | 429 | 1,520 | 6,361 | 23,803 | 5.7–6.5 | 5,508 | 49,207 | 64 KiB |
| strings | 132 | 501 | 3,064 | 9,486 | 1.9–2.9 | 1,350 | 6,005 | 512 KiB |
| not ASCII | 178 | 679 | 2,703 | 11,022 | 2.6 | 2,045 | 6,954 | 256 KiB |
| whitespace | 133 | 517 | 2,020 | 8,040 | 1.9–3.3 | 1,986 | 12,322 | 256 KiB |
| records | 193 | 864 | 2,917 | 11,579 | 2.8–3.3 | 2,448 | 11,300 | 256 KiB |
| members | 215 | 875 | 4,800 | 17,411 | 3.3–4.6 | 3,529 | 14,092 | 256 KiB |
| shuffled | 213 | 882 | 3,732 | 15,238 | 3.3–3.6 | 3,551 | 12,658 | 256 KiB |
| one hash | 1,462 | 7,926 | 38,938 | 202,136 | 22–49 | 37,860 | 40,162 | none |
| long names | 136 | 520 | 1,981 | 7,636 | 1.8–2.1 | 1,923 | 2,126 | 512 KiB |
| chains | 205 | 805 | 3,700 | 21,121 | 3.1–5.0 | 2,478 | 72,051 | 256 KiB |
| deep | 278 | 1,131 | 4,683 | 22,846 | 4.2–5.0 | 4,392 | 18,867 | 64 KiB |

The tape is 4 to 20 times faster than the tree (chains: 3.7 ms against 72 ms at 1 MiB) and within a few per cent of
the grammar alone on most shapes. With every other core spinning (the null judgement's p99 0.1 µs), S* falls a size —
64 KiB on most shapes, 256 KiB on strings and long names. The verdicts:
- **The predictions** hold for S* — 256 KiB on the typical shapes, 64 KiB on the densest (zeros, deep), 4 MiB on
  none — and for numbers and zeros a byte; strings and members run a little past their predicted cost a byte, chains
  under it.
- **R1, not refuted**: at 256 KiB, 688 µs on numbers, 501 on strings, 875 on members, all within J/2.
- **R2, not refuted**: M-b at 256 KiB shifts 2.8–3.2 ms in 30 s, the 64 KiB rows' 2.9–3.4, none never handed; at
  1 MiB the machine is past J 300 to 5,000 times and up to 85 % of the attempts are handed past J — 1 MiB adds up,
  as predicted. The 4 MiB rows are void: the profile's cap is 1 MiB, so those bodies were never read.
- **R3, refuted, its remedy declined on the measurement**: at 1 MiB cold the tape is past the grammar alone by 35 %
  on zeros and 120 % on chains at p99 (8 % and 29 % at p50); warm, by 15 % and 49 %. Its remedies cannot move S*:
  the grammar alone is past J/2 at the next size on both shapes (zeros 1,380 µs at 256 KiB, chains 1,244 at 512 KiB),
  and a chain's every value is a container, which no smaller entry spares.
- **R4, refuted**: four lookups 3.8 times `Exists` on members at 256 KiB (3,298 µs against 875), 2.9 times on the
  shuffled and the long names — about 46 ns a member a lookup: the name compared byte by byte through the decoder's
  iterators.
- **R5, refuted**: `Contains` of an absent string 2.0 times `Exists` on strings at 256 KiB, 1.9 times on text not
  ASCII; on numbers, zeros and chains within the threshold. The same cause: each element compared byte by byte.
- **R6, not decided by this run**: under the low mmap threshold at 1 MiB p99 moved from −25 % (strings) to +23 %
  (zeros) — the spread between two runs of one row.
- **R7, refuted**: the names of one hash are 7 to 13 times the members' grammar at the same size: each run of one
  hash sorted by names compared byte by byte.
- **R8, refuted**: a want of 64 KiB costs 1.2 ms (8 KiB 159 µs, 1 KiB 25 µs) against `Exists` on 64 KiB of numbers'
  178 µs — the tree's parse of the literal, every judgement.

**M-a after the remedies** (2026-11-28; the load server, idle; the trees of `7a1bdaf`, after K2b-7e-4b-4, and of
`a0b90a5`, after K2b-7e-4b-5; at 256 KiB, S* of the typical shapes; first → after 4b-4 → after 4b-5):

| refutation | shape | ratio |
|---|---|---|
| R4, four lookups to `Exists`, at most 2 | members | 3.77 → 2.56 → 1.44 |
| | shuffled members | 2.92 → 2.34 → 1.79 |
| | long names | 2.88 → 2.33 → 2.02 (1.96 at 64 KiB, 2.40 at 1 MiB) |
| | deep | 1.98 → 1.53 → 1.22 |
| R5, `Contains` of an absent string to `Exists`, at most 1.5 | strings | 2.03 → 1.78 → 1.27 |
| | text not ASCII | 1.89 → 1.58 → 1.20 |
| | zeros, at their S* of 64 KiB | 1.16 → 1.44 → 1.66 |
| R7, the names of one hash to the members' grammar, at most 4 | — | 9.8 → 5.7 → 2.3 (1.3 at 64 KiB, 2.8 at 1 MiB) |

- **R4, not refuted** on members, shuffled members, the deep object, records and numbers; **on long names at its
  threshold** — 2.0 to 2.4: a 1 KiB prefix every name shares, walked by each lookup member by member. Its remedy, each
  object's hash run kept and searched, is owed with its trigger: a plan's pointers into objects of long names sharing a
  prefix, past S*.
- **R5, not refuted** on strings and text not ASCII; on zeros at 64 KiB the ratio moved 1.16, 1.44, 1.66 across the
  three runs — the walk a number's kind read an element, against a build of some 13 ns an element —, and its whole,
  712 µs, stays within J/2.
- **R7, not refuted**: the names of one hash reach S* = 64 KiB, none before.
- S* otherwise unchanged: 256 KiB on the typical shapes, 64 KiB on zeros and the deep object.

**Decision 21 at κ = 0, measured on the load server and on the laptop** (2026-11-26/27; commit `effcfad`, optimised,
one test at a time): the laptop's replay with large answers (`tests/pace.rs`, `the_pace_replayed_with_large_answers`,
500 reads a second for 30 s, one answer in 100 large) put S past 500 ms at κ = 0 at 64 KiB — the roles' prediction that
S sums judging's holds on the source's thread. The load server (16 cores, 62 GB) does not: judging 16 and 64 KiB holds
no hand-off past J, S 0; at 256 KiB 447–449 hand-offs held past J (the largest 6.3–8.9 ms), S 300–304 ms at κ = 0 —
10 ms a second —, 4.3–9.3 ms at 10 ‰, the largest hold at 50 ‰. S sums where judging an answer at the cap outlasts J,
on the box that judges it: a box's judging time, not its answers' size, is the summing's condition. The timed wait
(`tests/realclock.rs`, 20,000 sleeps, twice each):

| | p50 | p99 | p99.9 | worst |
|---|---|---|---|---|
| the load server, idle | 69 µs | 77–82 µs | 174–245 µs | 418–442 µs |
| the load server, every core busy beside it | 64–65 µs | 1,768–1,893 µs | 1,984–2,117 µs | 3,137–3,699 µs |
| the laptop (K2b-6b) | 217–221 µs | 374–381 µs | 504–1,845 µs | 4,233–8,529 µs |

— on a box with cores to spare a wake never outlasts J; with every core busy one in a thousand does, by up to 1.7 ms.
No catch-up replayed on the server (`the_shift_without_catch_up`, five minutes of 500 reads, twice): 0 and 1 hand-offs
past J, S 0 and 0.9 ms. So S sums wherever a hold outlasts J — the box's judging and its timer's wakes the terms
measured, sessions' chains (the roles' third) not yet. That admission could keep these terms at zero (a judging at the
cap within J, cores to spare) was a reading of these numbers, not a decision, and its round refuted it (§ Owed,
"decision 21 at κ = 0"): the cap is not the plan's answers, and the term is a sum of holds, not one hold.

**Decision 21's raw dues, a second round, and no catch-up replayed** (2026-11-24):
- **Plan-time twins refuted by their roles.** A blind solver (Fable, given the question) derived each instant's
  plan-time twin `τ = min(t, r + S(t − g))`, `S(w) = ⌊1000·w/c⌋`, waits dilated, joins by a second maximum — the
  discriminator's `v` reached from the drain rate. Refuted by Distributed Systems with numbers: a dependent the
  certificate covers (slack on time) made tight by the harness — its cause handed 50 ms late, 1 s of think — gets its
  shifted intended start as raw and passes with an independent step planned 50 ms after it: an unplanned 429 a Root,
  a false Fail. A trilemma, not a slip: a certified raw makes the band hold the chain and its shift grow per link
  (50 → 95 → 144 → 193 ms); a neutral raw compresses; holding the plan until a dependency answers is coordinated
  omission. At κ = 0 none of it happens. Verifiability: the twin and the machine would share the sentences, the
  proposed properties measure each attempt against its own raw (blind to the defect), every golden runs at κ = 0
  (blind to S), rounding per segment stops the drain at small κ; its counterfactual pair (one script on time and
  late, raws bounded per attempt) is an executor any rule keeps.
- **No catch-up, replayed** (`tests/pace.rs`, `the_shift_without_catch_up`, optimised, five minutes of 500 reads a
  second, twice): 33 and 17 hand-offs displaced past J (the largest 5.1 and 11.9 ms); paced at κ = 0, none missed, the
  largest shift 3.3 and 10.8 ms — but with deadlines from the release, as today, the second run's lasting 10 ms puts
  963 per mille of its hand-offs past J and every censor of it undecided. So the shift with no catch-up is small —
  the run would end some milliseconds later — and it is the deadlines' anchor, not the shift, that would cost the
  verdict.

**K2b-9b-2 — the pure pace and its band** (2026-11-24, decision 21): `scheduler::pace` — `Pace`, the two maxima in
exact integers (κ at most 999); `Gate`, the band (attempts dispatched, arrived, gone; turns at an instant — passes in
raw order within J of the front, a head whose earliest falls at or past its deadline late, any attempt past its
deadline late, its report owed; the next turn's instant). Its executors (§ Executors) and seven mutants, each red.
Not yet used: `sim` paced on decision 21's raw dues was built and run, and refuted that rule (below); it waits for
the rule's amendment.

**`sim` paced on decision 21's raw dues — built, run, not committed** (2026-11-24): what the run of the differential
found, each fixed in the saved slice or carried to the amendment —
- **A loop**: an attempt past its deadline beyond the band stayed in it, and the next turn named an instant gone —
  `sim` turned for ever (`the run turned 16,777,217 times`); now every attempt past its deadline leaves, a test holds
  it.
- **A report owed**: an attempt held at the gate to its deadline left without one — the fold read a never-sent censor
  without its closing report, incoherent; every removal but a pass is now `pace.late`, its report after the censor.
- **The heap grew with the run**: each line's intended start and each attempt's hand-off kept for good — the
  allocator twins (`costs.rs`, `heap.rs`) red; pruned at the first dispatch and the end, kept only with the inputs.
- **The reference could not follow**: `schedule.py` decides line by line and cannot model a pace that couples every
  line; it takes the gate's hand-offs as the environment's, as it takes the reads, and the pace is checked on its own.
- **A rule side lost**: under the pace every never-sent attempt of the corpus followed a late read whose shift the
  run kept, so decision 19's never-sent Root was reached by no run; a script of held writes with the reader on time
  reaches it again. And `sim`'s drawn report made a quarter of its never-sent attempts never ready, whatever the gate
  did; it now carries the gate's hand-off — never ready is the gate's own `pace.late`.
- **Settled against machine 5**: of 3,732 runs, 2,063 unchanged and 1,669 changed, each with a hand-off the gate
  delayed; 604 Roots turned undecided, 341 Passed undecided — and 221 Passed turned Root: 213 of the script answering
  exactly at the deadline, its zero margin crossed by a residual shift within J (a censor within J is the system's,
  decision 17), the rest a censor within J or an observation whose polls the shift left fewer (owed: § Owed).
- **The raw-due rule refuted** — the golden `late_cause`: a cause planned at 1,000 handed at 2,000; its observation,
  planned at 1,100, `on: sent` — its intended start, 2,000, is its raw due, and it carries the cause's shift already:
  the pace pairs the two and holds the observation to 2,800 where about 2,000 would do; along a chain each level adds
  the root's shift again, less J. Its discriminator (Fable) went further: the rule is unsound against the certificate
  — a shifted dependent's raw overstated lets an independent planned 800 µs from it pass 100 µs after it, where J
  allows 600; a resend's due carries the same shift; the first amendment proposed (subtracting the least shift of the
  dependencies) is right only at κ = 0 — under a catch-up it understates the raw, and a drained shift returns to the
  whole run. What a chain must carry is the pace's own coordinate `v = 1000·G − c·raw` of the event a met instant
  descends from (an answer: its attempt's hand-off; a censor or a wake's end: the release; a skip: its upstream's
  end), not the shift: `raw = clamp(⌈(1000·met − v)/c⌉ + think, at, intended)` — at κ = 0 the amendment, with no
  shift the current rule. Exact in the machine (a `v` beside each cell's `sent` and end, a second running maximum
  beside `ready` and each barrier's latest; the raw journaled), inexact in the shell.

**K2b-9b-1 — journal version 9's bytes** (2026-11-24, decision 21): the header's `catch_up` (half the profile's
reserve until K2b-9c gives the run its own κ), `decorated` beside `handed` on `Sent` and a never-sent report (equal
until a gate stands between them), a found answer's `handed` (an early reply's attempt has no `Sent`, yet its bytes
reached the system — DS5's hole), the read line's `refusal` (a deviation from the validator's rate rule). The fold reads
none of them yet: its classes' digest is version 8's. Settled run by run: the differential's 3,732 runs and the 31
golden runs, their new fields removed, line for line version 8's; `decorated` equal to `handed` in all 22,935 sends
and 37 never-sent reports; `catch_up` 50 throughout (the fixture's reserve 100). No run of the corpus deviates from
the rate rule: `refusal` is reached by the golden journal and a test of its own. The writer thread's message keeps
its line unboxed (`large_enum_variant` expected, its reason stated): boxing would allocate every line to shrink the
run's one `Finish`.

**K2b-9a — a poll's grid at its line's planned instant** (2026-11-24, machine revision 5; decision 21):
- **Settled, not re-pinned.** The differential writes each run's journal under `SCHEDULER_JOURNALS_TO` when it is set
  (a test's evidence, written on request); machine 4's journals against machine 5's, the header's revision and the
  trailer's whole-journal digest set aside: of 3,944 runs, 3,928 unchanged below the header; 16 changed, each first at
  a poll of an observation released after its planned instant — the poll that moved, or the event it moved past, or
  (the late-wakes differential drawing its lateness per wake in order) the first dispatch that differs. Folded before
  and after by `verdict.py`, five of the sixteen changed class: one `void:undecided` turned pass, four had a Root
  appear or go, each as the scripted answers met polls at other instants; the fold itself did not change.
- **The goldens**: one changed below its header — `observation_read_late_not_yet`, its second poll from 1,900
  (the release plus `every`) to 1,800, its plan's point.
- **A defect found building it**: anchored at the plan's instant alone, an observation released exactly on a grid
  point was polled twice there — its first attempt and the next at one instant, one past what the validator
  certified; machine and `schedule.py` alike, so the differential stayed green, and no run of its corpus releases on a
  point. The first attempt now holds its release's point: `k ≥ ⌊(released − at)/every⌋ + attempt`, on time the old
  `k ≥ attempt`. Two machine tests: an observation read at 250 polled at 300, 400 … (not 350, 450 …) and read at 600
  polled there once, then 700; two read together at 620 keep their own phases (700, 800 … and 650, 750 …). Each red
  under the release's anchor and under the point not held.
- **The REST twin's projection**: a poll moved within 44 µs of its deadline was never sent, and `sim`'s drawn report
  (`drawn_unsent`, refused twice) is not the REST carrier's (connected, its write held: `rest.writing`, refused by
  none) — no REST attempt of that corpus had been dispatched that near its deadline. The twin compares that a report
  exists, at its instant; its evidence is the carrier's, which the never-sent twins hold.

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

- **The dropped timer's lag twin under load** (`load-run`'s `tests/lag.rs`, `a_timer_dropped_due_is_its_path_s_lag`):
  seen once, 2026-11-28, in `make all` at a load near 3 — a lag of 39,631 µs against its floor of 40,000, the report
  otherwise as the twin expects; six runs alone, green. Trigger: a second sighting — then its preconditions measured
  (the source thread's last park against the refusal's instant), as the receive twin's were (K2b-8d).
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
- **K2b-7e-2b-1c's survivor** (2026-11-24): a dependency's binding left unpriced survives the scheduler's executor —
  `LIVE_CELL`'s margin over a dependent's cell exceeds the bindings of 65 dependencies; owed, a row where they pass it
  (a step awaiting a thousand), or a white-box twin of the bindings' count.
- **K2b-7e-2b's measurements owed** (2026-11-24, its roles), each before what it settles: **the catch-up's backlog** —
  `sim` with a stall past L at κ = half the reserve, the peak of steps read and not ended against pending and against
  `pending·1000/(1000 − κ)` (before decision 21's I2 is amended); **counted bytes against the process** — the counted
  peak beside the resident high-water, the cgroup's current and its socket memory, cap sockets each receiving 1 MiB
  answers while their thread judges, one and four sources, transparent huge pages off and on (before guidance on a
  stated memory, and before K2b-7e-2c); **the copies a capture holds in flight** — a costs row with a captured hole (five
  a reader counted from the code); **a connection over TCP** — tokio's registration a socket, which the pipe does not
  show (a loopback control row for `CONNECTION`); **a source's own bytes** — `Live` under the allocator, differenced over
  sources and over events.
- **Decision 22's owed** (2026-11-24, its roles): **a declared head limit** (`head_cap`, the trailer's largest head;
  hyper's header count too); **the socket buffers** (sized from the emulated client or the bandwidth-delay product,
  read back, admission against the host's `tcp_mem` less the stand's share; the sysctls and sockstat journaled);
  **RSS beside counted bytes** (glibc's chunks: `malloc_usable_size` in the executors, RSS with transparent huge pages
  off; per-thread arenas — a budget a source, or the allocator's tunables); **the plan's peak line bytes** (the mean
  under-counts a bulk phase: the validator commits the pending set's bytes); **the plan's pending per source**;
  **a host's `Io`'s own heap** (TLS) stated by its `Egress`; **hyper pinned** (`=1.11.1`) or a stage-2 behavioural
  self-check; **a streaming judge** (a body never held whole) — trigger: a waiting budget voiding a host run, or
  judging holds past J; **captured values' bytes** (K2b-7e-2a: `fits` counts no capture — `Peaks.captures` is
  unused, and a value captured from a body may reach the cap) — trigger: a host plan capturing from bodies; **a head
  limit below `READ`** would shrink `HEAD` (69.7 kB at hyper's limit, 17 kB at a small head) — with the declared head
  limit above; **the memory's evidence** (memory.pressure, events, peak bracketed; gate closures as lines; a
  periodic sample, so a run the OOM killer took still shows its memory).
- **Decision 21's raw dues — a dead end, its next question** (2026-11-24; § Measured, § Alternatives Considered): three
  rules refuted, each fixing one constraint and breaking another; their common premise, a catch-up (κ > 0). The next
  round's question, before `sim` and the shell pace: **no catch-up — a late harness delays its plan, never compresses
  it** — the plan's clock pausing with the harness (κ = 0: a raw due the exact plan-time counterfactual, nothing ever
  compressed) and **each attempt's deadline from its hand-off** (it keeps its whole window: its censor stays the
  system's after a hiccup) — amending decision 7's anchor and decision 21's κ; measured viable on this box (the shift
  with no catch-up some milliseconds over five minutes); what it costs: the run ends later by the harness's lost time,
  and the cells a held shift keeps (better the machine's own clock paused than attempts held at the gate). Then
  K2b-9b-3 to 9b-5 by whatever it decides. The owner's call before it is built.
  With it: **an observation's last "not yet" under a shift** — the harness's lateness left its polls fewer; whether
  its final answer stays the system's (decision 17 says so) is the fold's question, owed to K2b-9b-5.
- **The journal's level, once past its statement** (2026-11-27, K2b-7e-2b-3c-2's gate): `tests/channel.rs`'
  `a_journal_holds_what_it_states` measured 295,225 B at its peak against 294,718 stated, 64 lines behind (100.2 %),
  once, under `make all`. It was not reproduced in twenty runs: alone, with the workspace's features, with `make`'s
  environment, with four cores busy, each at 294,397 B (99.9 %). So some interleaving of the machine, the writer and
  the stalled output holds about 0.8 KiB more than the usual one, and the statement does not cover it. Owed: find
  what it is — the channel's waker entries or a thread's lazy state are the candidates —, state it, and give the
  executor a schedule that reaches it.
- **A target past the URI's limit** (2026-11-27, found by K2b-7e-2b-3a-2): `http::Uri` holds at most 65,534 bytes,
  so a request whose target encodes past it is a `rest.request` fault at its dispatch, which stops the run. The
  validator's frame rule should refuse it before T0 — a literal path value or query past the limit encoded, a hole's
  past it at `TAKEN` — with a row at the limit and one byte past it.
- **Decision 21 at κ = 0 — two rounds, not yet a decision** (2026-11-25 and 2026-11-27). The owner chose the direction
  on 2026-11-25: no catch-up, each attempt's deadline from its hand-off.
  - **The first round** (the main position, a blind solver and a discriminator on Fable; Distributed Systems,
    Operability and Verifiability on Opus) synthesised ten rules: twins by an attempt's own shift, dispatch at the
    real floor with the shift at the gate alone, a window a step from its hand-off, an observation's from its
    cause's, a run's shift bound Σ stopping `void:shift` below Fail. Its rule that S is the largest hold was refuted
    by its roles (S a sum: judging on the source's thread, the gate's own wakes, tight sessions doubled by the pace's
    inversion term) and by the laptop's replay with large answers (§ Measured). Its other findings: a stop that cuts a
    censor's last look makes a false Fail; an observation whose cause was resent, anchored at its first attempt, a
    false Fail; failing observations void with no shift; rules on absolute lateness void every run once S passes 2J;
    the shift undiagnosable; decorations made a lead early.
  - **The second round** (a blind solver and a discriminator on Fable; Distributed Systems, Operability,
    Performance and Verifiability on Opus) re-proved κ = 0 for
    plans with dependents, the hand-off as the anchor and S a running sum of each hand-off's lateness past J (V at
    κ = 0). It found the two positions' call deadlines one identity, `G₁ + T + Σ(G_i − due_i)⁺`, and refuted admission
    as a certifier: a judging time at the cap refuses healthy runs (the cap is not the plan's answers) and admits
    saturating ones (a sum of holds, each within J); an idle stage 2 measures the idle box. Its synthesis under
    refutation: a plan clock σ with the front the least plan due not handed (the band, the machine's untaken wakes,
    the reader's next line), the inversion term a coherence check of the fold; dilation bounded per declared window
    (δ, H), its stop ranked below Fail and carrying the last look's evidence; stretch rules above J; a receive-side
    stall's dependents undecided by the stamped lag; admission stating, not certifying; a shift ledger written at
    the pass. Open: a dependent's plan due (its parent's shift exact, or the clock's at its end), decided by a
    measurement in `sim` (two sources, 200 sessions of 50 zero-slack links, a 200 µs decorator, a 20 ms stand); owed
    too, on the load server: the built gate at 1, 4 and 16 sources at 256 KiB, the timed wait with 1, 4 and 16 cores
    busy, a 3 h idle-stand run's worst gap.
  - **Its roles refuted the synthesis's operability at launch**, the first cause upstream of decision 21:
    - **The judging floor holds the one machine** (decision 16): while any source's answer is judged, the machine
      takes no wake and no line (`run.rs`' floor, `stamps.rs`' push). σ's inflow is the run's large answers a second
      times their hold past J, whatever thread judges: the server's 256 KiB row costs about 2 ms of σ a large answer,
      so a host plan of 10³ steps a second, 1 % at 256 KiB, dilates about 20 ‰ on any number of sources. Under a
      catch-up the same holds were compressed back, a catch-up refuted for dependents; so the floor, not κ, is the
      question: how an answer is judged without holding the plan's other steps (a floor holding only the answer's
      dependents, the answer stamped after its judging at a declared latency cost, or a faster judge, K2b-7e-4).
    - **A frozen front is invisible**: σ booked only at a pass never rises while a hung decoration holds the front, so
      the dilation stop never fires, the band fills at the plan's rate, and another stop (the read gate's
      `void:fault`, ranked above Fail) discards answered Roots. The stop and the machine need the prospective
      `σ̂ = max(σ, t − F − J)`, the machine no dispatch past `F + J`; the front also the frontier's watermark (stamped,
      unstepped answers); a decorator synchronous over a credential its host refreshes off the path.
    - **The deadline's credit** starts at the second attempt (from the first, a first attempt held at the gate is
      given its hold: a false Pass), its dues the real ones, the machine's licence on the credited window.
    - **The fold must not trust the machine**: plan dues recomputed from the plan's definition (`schedule.py` on a
      perfect harness), the journal carrying each read's timeout, think, interval and `within`.
    - **A dependent's plan due** is its parent's shift exact: the clock's at its end bunches dependents of a flight
      spanning a stall with independents planned there — the counterfactual pair already decides it.
    - **No receive-side lag exists** to mark a stall's bunched answers, and a whole-process stall reads no lag;
      collateral 429s on independent steps stay Roots. Owed: the stall witness, and the bucket's horizon marked
      after a bunched release.
    - **The stop**: sliding windows, the fold's own dilation check, `Σ ≥ δ·horizon`, the stopped journal classed by
      its prefix's answered Roots, a ledger line at every pass that survives the stop, strict ties; a poll's
      `Retry-After` in its plan due; an observation whose cause's first attempt was Unknown decided by both bounds.
    - **The void's odds grow with the run**: the maximum over windows makes the usable budget about 0.85 δ (an
      estimate), and a rare stall's rate past δ·H needs some 86 h of launch-shaped runs to bound. Each of M4–M7 was
      re-specified with a threshold that can refute it.
  - Next, the owner's call: a round on the judging floor, after a measurement on the load server of the floor's own
    share of σ at 1, 4 and 16 sources and 5 to 80 large answers a second. K2b-9b-3 onward waits for both. Both done
    (2026-11-27): the floor is the whole shift measured (§ Measured), decision 23 decided; K2b-9b-3 onward waits for
    its steps and the owner's confirmation of decision 21.
- **For the owner, from K2b-7e-2b-3d-3's round** (2026-11-28): this ADR's lines on the journal's versions disagree for
  journals written between the first host run and K5 — "journals before K5 are disposable" against "from the first host
  run on a reader accepts the versions it lists". Version 10 is the section the first host run carries; the versioned
  recheck makes either answer buildable, but this ADR must say which.
- **K2b-7e-2b-3d-1's owed** (2026-11-28): the remedies searched over the carrier's knob — its ports through
  `Egress::caps`, a method the `Carrier` states — and over the plan's answer cap, whose costs the carrier states for the
  cap it was built with: each needs the carrier to price a knob it was not built at. The ports' done in K2b-7e-2b-3d-2;
  the answer cap's waits on a carrier pricing a cap it was not built with (`Costs.answer` and its head apart, the
  decision's O5), before the first host run's dry runs.
- **Decision 23's step 2, the tape — owed** (2026-11-27, its roles):
  - **A literal is parsed by the tree on every judgement**, and the validator bounds no literal: a 40 KiB `Equals` is
    about 1 ms on the server whatever the body. M-a's want row at 1, 8 and 64 KiB decides; a literal's parse past the
    body's build at the same size gets a remedy — parsed once a record, or bounded by the validator.
  - **A capture's JSON pointer is never checked by the validator**: a malformed one is a silent missing value. Done
    in rules revision 15 (K2b-7e-4b-2).
  - **Contingencies, each with its threshold in M-a**: the tape against a null sink at 1 MiB with cold caches past
    25 % (smaller entries, or registration reconsidered); four lookups costing more than the build (each object's hash
    run kept and searched); a `Contains` walk past half the build (`same`'s fast paths); p99 up 20 % under a low mmap
    threshold (a tape reused by the source, the read dropped after its end); colliding names past the tree's time at
    the same size (a keyed hash). Measured (§ Measured, M-a and M-b): R3 refuted and its remedies declined — neither
    moves S*; R4, R5 and R7 refuted by one cause, a decoded name compared byte by byte through iterators, so the first
    remedy is a comparison piece by piece — a run's bytes at once — for `at`, `Equals`, `Contains` and a run of one
    hash, before the remedies named (each object's hash run kept, a keyed hash), which a rerun of their rows decides
    (K2b-7e-4b-4); R8 refuted, its remedy a literal bounded at `TAKEN` by the validator (rules revision 16,
    K2b-7e-4b-4) — 159 µs at 8 KiB —, a literal parsed once a record left to a plan that needs more; R6 undecided, its
    rows owed twice more. Rerun (2026-11-28): R6 refuted on the densest shapes — under the low mmap threshold p99 up
    29 % and 58 % on zeros, 41 % on members, 40 % on shuffled members, none on numbers, chains, deep or long names —:
    a fresh tape's pages faulted in each judgement. Its remedy, a tape reused by the source, waits on its trigger: the
    host's allocator measured at H-1 (its mmap threshold, its purge of large frees); under glibc's defaults the
    effect is nil. After K2b-7e-4b-5 (2026-11-28, § Measured, M-a after the remedies): R4 not refuted but on long
    names, at its threshold — each object's hash run kept is owed to a plan that looks up long shared-prefix names past
    S* —; R5 and R7 not refuted. K2b-7e-4b is done.
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
- **Decision 22's sources, owed** (2026-11-27, K2b-7e-2b-2c's roles): **a look's burst** — a row of attempts sharing
  a deadline, a source thread's deferred wakers; **pool slots** — a row of several endpoints; **tokio's
  `ScheduledIo`** on a real socket; **`machine.step`'s input clone** of captures against `LIVE_CELL`; **a CI variant
  with tokio's `signal`** (a registration a runtime); **`Live` built from admission's fitted caps** by type, as the
  journal's channel is (K2b-7e-2b-3: `for_caps` allocates before admission, a cap of `i32::MAX` reserving 25 GB);
  **stage 2's time over chains**, measured before its vocabulary changes; **a host dry run's answers replayed**
  through `read` and `judge`, their tree a byte by operation — K2b-7e-4's priority (Performance); **`stamps::Tasks`'
  one mutex** measured on the 16-core box (Performance).
- **Decision 22's journal, owed** (2026-11-26, K2b-7e-2b-2b's roles): **the journal's backpressure** (Operability) —
  five waits with no bound: a line's send on a full channel (`sink.rs:121`), the trailer's (`sink.rs:132`), the join
  of a writer stuck in a write (`sink.rs:105, 133`), the header written on the caller's thread after T0
  (`sink.rs:82`), the stop's fault line (`run.rs:994`); an output that stalls for good hangs the run past every stop
  — a host's `ctrl_c` starved on the run's runtime, the stall checks suspended — until it is killed, its journal
  with no trailer (`void:unreadable`), and after K2b-7e-2b-2b-2 a host cannot supply its own sink to escape it; a
  timeout must end the run, never buffer elsewhere (or the channel stops being the count's enforcer); with it, a
  full channel counted and timed on the loop's clock, into 2b-3's `memory` line, so a slow journal disk is told from
  a starved machine; a slice before the first launch-scale run, its executor an output whose write never returns and
  a run that returns within its bound, its measurement an output of 2 ms a write at 1,000 lines a second and
  `behind` 64. **The reader's check is late and self-declared** (Software Design): `LineSource::ahead` defaults to
  none, and it fires after T0, a journal of a header alone — the journal's inversion is its remedy. **A failing
  `open` leaves the stand touched** (decision 18; with `open` given no header it can move before the gates). **A
  compressing output cannot be kill-durable a line**; a host stating one says so. **A channel of encoded bytes**: one
  timing of `to_writer` and the digest a line on the machine's thread settles whether it is cheaper. **The threads'
  stacks** in the RSS measurement before K2b-7e-2c. **2b-3's `memory` line** must not depend on the channel draining.

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
  the pace, its raw dues, κ, the band gate, the fold's next revision, decision 17 amended; K2b-9 its cut. The replays
  rerun optimised (§ Measured): the earlier ones were on opt-level 0, and their "this box cannot offer 2,000 a second"
  withdrawn; judgements of large answers found holding the machine — the judge's speed owed with H-1's trigger.
- 2026-11-24: K2b-9a built (§ Measured): machine revision 5, a poll's grid at its line's planned instant, its
  release's point held; settled run by run against machine 4; the REST twin's never-sent evidence projected.
- 2026-11-24: K2b-9b-1 built (§ Measured): journal version 9's bytes — catch-up, `decorated`, a found answer's
  `handed`, a refusal read — settled against version 8. The stall twin's first censor Root recorded (§ Measured
  K2b-8d-2), its path now printed.
- 2026-11-24: K2b-9b-2 built (§ Measured): the pure pace and its band. `sim` paced on decision 21's raw dues built
  and run, not committed: its findings recorded, and the raw-due rule refuted — a dependent's intended start carries
  its dependency's shift, counted again, unsound against the certificate; its amendment under the gate (§ Owed).
- 2026-11-24: decision 21's raw dues, a second round: plan-time twins (a blind solver and the discriminator
  converging) refuted by Distributed Systems and Verifiability — a covered dependent compressed, a trilemma; no catch-up
  replayed over five minutes (§ Measured): small, the deadlines' anchor what costs; the next question recorded (§ Owed).
- 2026-11-24: K2b-7e's measurement (§ Measured): an open attempt with its connection some 51.5 kB, a pooled connection
  47 kB (71.5 kB after 64 KiB answers), an answer read 1.6 times its body.
- 2026-11-24: **decision 22 decided** (§ Decision 22; § Alternatives Considered; § Owed): the shell's memory —
  constants by construction, a stated memory, the answers charged statically before any wait; K2b-7e cut in five.
  K2b-7e's measurement corrected: hyper's read buffer did not grow to fit a body — the counting `Io`'s 16 KiB reads
  capped it at 32 KiB, the growth measured; the "1.6×" is the per-attempt peak, not a parse factor.
- 2026-11-24: K2b-7e-1 built (§ Measured): the shell's constants by construction — one builder, a head too large a
  fault, the scratch one a thread, the body sized from its length, the head copied first, a task's handle let go at
  its end; a pooled connection 39.8 kB however large the answers it read.
- 2026-11-24: K2b-7e-2a built (§ Measured): the costs the carrier and the shell state, held two-sided by a tagging
  allocator under the virtual clock; an answer's charge its body at the cap and its head at its worst (a read block
  pinned, hyper's map, the judge's copy: 69.7 kB); a body of no length reserved at the cap, no longer doubled to twice
  it (decision 22 amended); `fits` found under-counting — a burst released at one instant, a REST read, a moved block
  counted once — and re-pinned (`LIVE_CELL` 3,776, `ENDED_CELL` 1,216; decision 7's executor). K2b-7e-2 cut in three.
- 2026-11-24: **decision 22's admission amended for K2b-7e-2b** (§ Decision 22; § Alternatives Considered; § Owed):
  I1 a sum of enforced counts times owner-stated costs, the unenforced charged at their measured worst; one pool, its
  count the steps read and not ended and the handles past their deadline; the plan commits shapes, the scheduler and
  the carrier price them; an attempt priced by its record; captures held by `TAKEN`; the reader, the journal and the
  sources by construction; decision 21's I2 kept; every refusal at once, each remedy its owner's; the stated memory
  checked against the cgroup; a periodic `memory` line; no launch-scale run before the read gate. K2b-7e-2b cut in
  three, K2b-7e-2c moved after the read gate.
- 2026-11-24: K2b-7e-2b-1a built (§ Measured): a record's shape and its price — exact against what a decoded record
  holds over records generated across every field; K2b-7e-2b-1 cut in three.
- 2026-11-24: K2b-7e-2b-1b built (§ Measured): the plan's byte peaks committed — the shapes pending and late by a
  jitter, what the held keep, each source's sockets, the barrier keys' bytes, the largest line and chunk; the
  validator's revision 10.
- 2026-11-24: K2b-7e-2b-1c built (§ Measured): `fits` priced by the plan's shapes, its cells without their records
  (`LIVE_CELL` 2,944), `Budget.line` gone; a captured JSON number bounded by `TAKEN` (decision 15's take amended).
- 2026-11-25: K2b-7e-2b-2a built (§ Measured): the reader by construction — the line limits enforced before their
  allocation (the header line committed), the verifier's buffers reserved at the commitment's sizes, a line decoded
  with no copy of it or of its texts, zstd's window bounded and libzstd's context stated, the header shared, the
  largest record's shape committed (the validator's revision 11), records sent in their `Arc`, the reader's `ahead`
  carried by the plan and held at the run; K2b-7e-2b-2 cut in three. The counting allocators of `command-log`'s
  `tests/shape.rs` and `tests/reading.rs` count each thread's bytes, not the process's: the gate, under load, saw a
  record's heap off by 533 B — the harness's threads allocating and letting go in a test's window, a lock between the
  tests no guard against them (four busy loops beside: 8 runs of `shape.rs` in 20 red counting the process, 0 of 20
  counting the thread, the same bytes measured idle).
- 2026-11-26: K2b-7e-2b-2b decided under the gate (the main position, a blind solver and a discriminator on Fable,
  three roles on Opus; § Alternatives Considered) and cut in three; decision 22's journal amended: its channel built
  only from what admission holds. K2b-7e-2b-2b-1 built (§ Measured): a step's lines made one at a time, a line priced
  by the journal's owner, a record's barriers' keys a role of its shape (the validator's revision 12), the machine's
  bindings priced by `fits`, the writer streaming through a fixed buffer; the journal's backpressure owed.
- 2026-11-26: K2b-7e-2b-2b-2 built (§ Measured): the journal's channel built from admission — `sink::Journal` passed
  to `admit`, `Written::start` the stages' alone, the host's output opened before T0 and refused past `OUT` inline,
  `Journal::bytes` stated and held, `Lines` a test's, `Ready::t0` the host's.
- 2026-11-27: K2b-7e-2b-2b-3 built (§ Measured): the carrier's boundary — a code made by `Code::of` alone, an answer's
  captures matched to distinct specs and kept in their names, a value's buffer at most `VALUE`, the library's values at
  their length, `Ran::unsent` capped at `UNSENT` codes; K2b-7e-2b-2b built.
- 2026-11-27: K2b-7e-2b-2c decided under the gate (the main position, a blind solver and a discriminator on Fable,
  three roles on Opus; § Alternatives Considered) and cut in four; decision 22's source bullet amended: the parse
  bounded by the answer cap, not at its measured worst. K2b-7e-2b-2c-1 built (§ Measured): `RUNTIME` and `EVENT`,
  `Runtimes::own`, a runtime's events as built, `live::events_for`, `Live::start` a test's.
- 2026-11-27: K2b-7e-2b-2c-2 built (§ Measured): `json::bound` and `json::pointer_bound`, the shape's largest literal and
  longest pointer (the validator's revision 13), `answer::bound` and `judge::bound`, `answer::is_taken`, a `Location`'s
  segment sized before it is decoded, `fills` refusing a structure before it parses one.
- 2026-11-27: K2b-7e-2b-2c-3 built (§ Measured): the answered tail a plain function, `answer::Read` neither `Send` nor
  `Sync`, a value no take makes refused at the shell's boundary, `run::ending` stated, stage 2's documents sized at once
  and `stages::judging_bound` stated.
- 2026-11-27: decision 21 at κ = 0 measured on the load server (§ Measured): S does not sum at 64 KiB there, it does at
  256 KiB; a wake outlasts J only with every core busy — the laptop's summing at 64 KiB was its judging's, not the
  rule's. Admission keeping S's terms at zero, a reading of these numbers put to the owner before a round, was refuted
  by its round (§ Owed, "decision 21 at κ = 0"), which records the 2026-11-25 round too.
- 2026-11-27: K2b-7e-2b-2c-4 built (§ Measured): `carrier::SourceCosts` and `Carrier::source_costs`, the REST
  carrier's scratch, head copy and tail; the head's copy sized at once, not measured growing by doubling as cut.
- 2026-11-27: decision 21 at κ = 0's second round's roles recorded (§ Owed): the judging floor holding the one machine
  is the cause upstream of the choice of κ; the synthesis is not yet a decision.
- 2026-11-27: K2b-7e-2b-3 decided under the gate (§ Alternatives Considered) and cut in five; decision 22 amended:
  one pure fit binding what admission prices, three phases, the pool at `late` and `late_shape`, each part's bytes by
  its owner and byte class, the term set complete by construction, the scope rule executed, version 10 with the
  `memory` line's form.
- 2026-11-27: K2b-7e-2b-3a-1 built (§ Measured): a request's bytes and holes by class (the validator's revision 14),
  a frame's target and body sized at once.
- 2026-11-27: K2b-7e-2b-3a-2 built (§ Measured): `Carrier::attempts` and `AttemptCosts` by class, `Costs.attempt` out of
  `Needs`, REST's coefficients measured two-sided, a JSON token sized at once, hyper's head buffer in the connection.
- 2026-11-27: K2b-7e-2b-3a-3 built (§ Measured): `run::attempt_share`, REST's needs by source, `fits::terms` and
  `fits::late_records`; the late records' whole-run row owed to K2b-7e-2b-3d.
- 2026-11-27: K2b-7e-2b-3b built (§ Measured): `Egress::io_bytes`, `Decorator::bound` held at entry, `Stand::gate_bytes`
  relayed; `Egress::caps` moved to K2b-7e-2b-3c, its consumer.
- 2026-11-27: K2b-7e-2b-3c cut in two; K2b-7e-2b-3c-1 built (§ Measured): `memory::fit`, its terms and its refusal,
  held against a shadow of the decision's sentence.
- 2026-11-27: K2b-7e-2b-3c-2 built (§ Measured): `memory::remedies` by bisection, `Egress::caps`, `fits::fits` retired.
- 2026-11-27: the judging floor measured on the load server (§ Measured) and decision 23 decided under the gate: an
  answer's arrival and its judgement two inputs, the pace at the judgement, the excusal and the licence at the
  arrival; laziness and a streaming judge first (K2b-7e-4), the two inputs after the owner's confirmation.
- 2026-11-27: K2b-7e-4a built (§ Measured): a body parsed on its first reader; a late answer's holding moved to
  decision 23's step 3b, not journal-neutral.
- 2026-11-27: K2b-7e-2b-3b's executor `a_connection_holds_its_egress_s_stated_io` given a kilobyte's tolerance on its
  difference from a small connection (a test-only fix): it asserted the difference exact at 32,768 B, and a small
  connection's own idle varies by some tens of bytes — 96 B of margin in its own runs, short under `make all`. Its
  two-sided `within` is unchanged.
- 2026-11-27: the floor's measurement rerun after K2b-7e-4a (§ Measured: M-0, M-0′) and K2b-7e-4b decided in its own
  round (§ Decision 23, step 2): a tape; the step's first sketch refuted; cut in three. Recorded with it: K2b-7e-4a's
  stratified differential was not built — its executors were the counting allocator and the judge's vectors —, and is
  K2b-7e-4b-2's.
- 2026-11-27: K2b-7e-4b-1 built (§ Measured): one grammar over a tree and a tape, the names' check in it, the tape
  and its views; `json::bound` restated with the names' stack, `the_densest_input_holds_near_its_bound` held against
  the tree's part.
- 2026-11-27: K2b-7e-4b-2 built (§ Measured): the readers on the tape, `answer::bound` by phase, rules revision 15
  (an answer cap past `LONGEST`, a capture's malformed pointer); decision 23's step 1 differential built against frozen
  eager readers.
- 2026-11-27: K2b-7e-4b-3's measurements predicted before their run (§ Measured, M-a and M-b): the harness
  (`scheduler`'s `tests/judging_time.rs`, `FLOOR_SIZES`), the predictions and eight refutations; `json::check`, the
  grammar alone, the validator's check of a literal.
- 2026-11-27: M-a and M-b measured on the load server (§ Measured): S* as predicted; R3, R4, R5, R7 and R8 refuted,
  R1 and R2 not, R6 undecided; K2b-7e-4b-4 cut for the remedies (§ Owed).
- 2026-11-27: K2b-7e-4b-4 built (§ Measured): decoded strings compared piece by piece, runs and digits searched at
  once, a literal bounded at `TAKEN` (rules revision 16).
- 2026-11-28: M-a rerun after K2b-7e-4b-4 on the load server (R4, R5, R7 still refuted by smaller margins; R6
  refuted on the densest shapes) and K2b-7e-4b-5 built (§ Measured): a plain string compared over its text; K2b-7e-4b-4's
  unit test's escapes restored — its record named escapes it no longer held.
- 2026-11-28: K2b-7e-2b-3d-4b measured (§ Measured): P7, P8, P9 held, P10 with the stacks' reader fixed; P6 no test
  at idle; P4 restated as a reading.
- 2026-11-28: K2b-7e-2b-3d-4b's harness built and its predictions written (§ Measured), before its rows; the late
  records' row moved to K2b-7e-2b-3d-4c.
- 2026-11-28: K2b-7e-2b-3d-4a measured (§ Measured): P1, P2, P3, P5 held, P2's mutant red; P4 refuted as stated
  (a connection an attempt, priced at the cap); the late records not reached, L2 at 64 KiB no test.
- 2026-11-28: K2b-7e-2b-3d-4a's harness built and its predictions written (§ Measured), before its rows.
- 2026-11-28: K2b-7e-2b-3d-4 decided under the gate (§ Decision 22, decided for K2b-7e-2b-3d-4) and cut in three: the
  whole-run measurement as differenced rows, each guarded, predicted and mutated; no verdict on κ.
- 2026-11-28: K2b-7e-2b-3d-3b-4c built (§ Measured): the evidence a part of its own, one copy kept; K2b-7e-2b-3d-3b
  done.
- 2026-11-28: K2b-7e-2b-3d-3b-4b built (§ Measured): four of admission's headers pinned, folded by both folds.
- 2026-11-28: K2b-7e-2b-3d-3b-4 cut in three; K2b-7e-2b-3d-3b-4a built (§ Measured): the admissions' results pinned
  one line a key, CI's append-only check.
- 2026-11-28: K2b-7e-2b-3d-3b-3b built (§ Measured): `verdict.py`'s recheck, the census through both twins, the
  single-edit property and its free list.
- 2026-11-28: K2b-7e-2b-3d-3b-3 cut in two; K2b-7e-2b-3d-3b-3a built (§ Measured): the fold rechecks admission's
  evidence, revision 8; admission checks its own; the part table as data.
- 2026-11-28: K2b-7e-2b-3d-3b-2 built (§ Measured): journal version 10, admission's memory, the assumptions apart,
  settled against version 9; the lag twin's first sighting under load (§ Owed).
- 2026-11-28: K2b-7e-2b-3d-3b cut in four; K2b-7e-2b-3d-3b-1 built (§ Measured): a header's version read before its
  schema.
- 2026-11-28: K2b-7e-2b-3d-3a-2 built (§ Measured): the terms by count, cost and rest, four parts more, the
  carrier's stater on its sockets, the shared counts and the `Model`, the pool's records by the late count.
- 2026-11-28: K2b-7e-2b-3d-3a cut in two; K2b-7e-2b-3d-3a-1 built (§ Measured): κ by one call and refused by name,
  the limit's origin, `GateBuilt`, `unheld`.
- 2026-11-28: K2b-7e-2b-3d-3 decided under the gate (§ Decision 22, amended for K2b-7e-2b-3d-3) and cut in two; the
  `memory` line moved to K2b-7e-2b-3e with its producer; the fold's revision 8 taken, decision 21's the next after; the
  owner asked which journals a reader must accept from the first host run on.
- 2026-11-28: K2b-7e-2b-3d-2's defect fixed (§ Measured): the carrier's map held to its needs' caps at its own total.
- 2026-11-28: K2b-7e-2b-3d-2 built (§ Measured): `Limit::read`, the limit's refusal, the scope rule, the carrier's
  ports a remedy.
- 2026-11-28: K2b-7e-2b-3d cut in four; K2b-7e-2b-3d-1 built (§ Measured): `fit`, `Fitted`, `Live::fitted`,
  `admit(fitted, &runtimes)`, every refusal at once; a runtime's own descriptors measured at three, not two.
- 2026-11-28: M-a rerun after K2b-7e-4b-5 (§ Measured): R4 not refuted but on long names at its threshold, R5 and
  R7 not refuted; K2b-7e-4b done.
- 2026-11-27: the stall twin's tally (K2b-8d-2) counts apart an on-time attempt the stand refused — the censor Root's
  second sighting, its path printed, a Root by decision 19 (a test-only fix).
