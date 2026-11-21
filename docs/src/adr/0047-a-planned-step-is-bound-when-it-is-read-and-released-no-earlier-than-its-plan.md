# ADR-0047: A planned step is bound when it is read and released no earlier than its plan

## Status

Proposed (2026-11-21). Stage 3 of three toward the host's fitness functions (ADR-0045 § Status): the scheduler that
runs a command log (ADR-0046) against a stand — what is pure and what is the shell, how steps are bound, released,
sent, judged and recorded, what voids a run. Nothing is built yet.

To be built in slices, each a commit of its own:
- **K0a** (`crates/command-log`, FORMAT 3, `RULES_REVISION` 4): the command log's changes this decision needs
  (decision 13).
- **K1** (`crates/scheduler`, pure): the machine, the reference scheduler, the stateful server model, the TLA+ model,
  the verdict fold and the journal's schema.
- **K2** (`crates/load-run`, the shell): REST, the clock, the reader, the journal writer, the gates.
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
intended start — an observation may be its own cause, a watch begun on its own, its deadline from its own start. The plan's own bound — every action ending at its deadline, the worst release along each chain — is
computed by the validator, and a plan past what the machine holds is refused before T0, as decision 16's dry run is.
Lines blocked behind censored roots are the system's: the run ends failed and names them, never void.

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
contract says mean "not run" (`service.session_failed`, `request.rate_limited`, `projection.behind`), never by a
status: a 503 with `Retry-After` may follow a COMMIT that took effect. Everything else not answered is Unknown; an
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
discriminating-`until` rule; the `Sent`-only rule of decision 6; the plan's own bound in the validator and peaks with
a peak rate in the commitment; the window as a public module; `reference.py` emitting each edge's bound line and
deciding the new rules.

### 14. The journal and the verdict

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
| K1-4 | K1 | `verify/tla/Scheduler.tla`: `ReleaseSafe`, every read line ends, `NoUseAfterFree`, `BarrierSafe`, `BoundAsChecked`, `ConditionalExact`, no response stamped by its deadline censored; mutant configs `BindAtRelease`, `FreeOnEvict`, `CounterBarrier`, `ReadyWithoutAt`, `NoProvenance`, `ProvenanceOneLevel`, `DeadlineByArrival`, `NoSkipPropagation`, `PendingByRate`, each with a pinned twin and state count (`must_fail_with_twin`), on an instance of at least four lines with W below L or a latency above W | each config's own |
| K1-5 | K1 | sim journals checked against `Scheduler.tla` (the `trace2tla.py` pattern) | the machine diverging from the model |
| K1-6 | K1 | corpus coverage counted and failing on zero: lines where the two bindings differ, cells taken after their producer's eviction, barrier members finishing before earlier ones, conditional dependents, responses after their deadline, same-instant inputs, `think > 0` | — |
| K1-7 | K1 | the journal invariant under any permutation of same-instant inputs | ties broken by arrival |
| K1-8 | K1 | a counting allocator: the heap flat across eight times the subjects at one planned rate, with a share of producers Unknown or Unexpected and captures missing; a stall of s ∈ {T/2, T − 1 µs} over chains of depth ≥ 2 not void, its peak in flight as derived; a stalled stand on a `Settled` chain spaced below its deadline ending failed within the plan's bound | a cell freed on eviction; a refcount leak on a skip path; a bound chosen by rate alone |
| K1-9 | K1 | the verdict fold against golden journals and an independent Python reader: one twin per void, the interval verdicts (a stall that fails by more than own lateness fails; a straddling one is void), a skipped publication inconclusive, transitive provenance | a conditional step counted as a pass; one-level provenance |
| K1-10 | K1 | the dependency allowlist of `crates/scheduler` | a clock crate added |
| K2-1 | K2 | the shell against an in-memory transport (hyper over `tokio::io::duplex`) under paused time reproduces `sim`'s journal | `std::time::Instant` in a handler |
| K2-2 | K2 | each cause delayed alone — the reader, the token, the pool, the connect, the write, the dependency — moves exactly one gap of the chain | a gap merged |
| K2-3 | K2 | on the real clock, multi-thread: chain order, no release before its stamped met instants, the stub's requests per correlation id equal the journal's attempts — a keep-alive connection closed as it is reused included; a request processed then reset before the answer is Unknown; a response stamped 1 µs before its deadline delivered after the tick is Expected | hyper's hidden retry; Unknown read as not taken; deadlines by arrival |
| K2-4 | K2 | a 503 with `Retry-After` and an uncertified code not resent; `service.session_failed` resent; an Envoy-shaped 504 Unknown with its `Settled` dependent conditional; a non-idempotent call timing out after writing not resent | not taken by status |
| K2-5 | K2 | decorated headers differ across attempts (a fresh DPoP `jti`), the rest byte-identical; a retried step joins one server record; no secret in the decompressed journal, raw, base64, base64url or hex | a decorator run once; one correlation id a step |
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
