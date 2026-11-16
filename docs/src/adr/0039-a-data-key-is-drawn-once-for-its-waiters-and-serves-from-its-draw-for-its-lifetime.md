# ADR-0039: A data key is drawn once for its waiters and serves from its draw for its lifetime

## Status

Proposed (2026-11-15). Slices S1a (the `kms` crate), S1b (`dek`'s pure
core) and S1c (`dek`'s shell) are built; S1d, the consumer's adoption, and
the later slices are owed, each with its executor (§ Owed).

Decided under the library's gate:
- a blind solver and a discriminator;
- the synthesis refuted by four roles: distributed systems, security,
  operability and verifiability;
- the synthesis after the roles checked by a second discriminator, whose
  findings are folded in below.

The consumer's need is its ADR-0091 decision 8 (architecture-docs).

## Context

**What the stage does today.** `dek`'s `EnvelopeStage` seals a payload
under a data key per (channel, Tenant), drawn from the KMS and reused for
1,000 messages or 60 s (`Reuse`).
- The draw is the caller's: the stage checks its slot, releases its mutex,
  awaits `generate_dek` in the caller's future, then installs
  (`stage.rs:163-213`).
  - N concurrent misses of one (channel, Tenant) make N draws; the last
    install wins.
  - A caller cancelled mid-draw cancels the draw.
  - The stage has no bound of its own: the consumer's HTTP client's, 10 s.
- A failed draw installs nothing: no backoff, no memory; the next seal
  draws again.
- A key's age is stamped at its landing (`std::time::Instant`), so the
  shredding window ADR-0017 and the consumer's ADR-0071 decision 9 state
  as "the lifetime" is the lifetime plus the draw's latency.
- A key never leaves memory: the serving map is never evicted, and an idle
  Tenant's last key outlives its lifetime; `Cached` evicts only on insert.

**Why it matters to the consumer.** The consumer seals inside its database
transactions, and in one of them while it holds a hot row lock. A draw is
a KMS call made while a pooled connection, and the row, are held. A Vault
that stalls at a key's end holds them for the HTTP client's 10 s on every
replica; the waiters hold the rest of the pool (ADR-0091's Context).

**How the KMS's refusals were read.**
- `kms::Error` is `#[non_exhaustive]`, and `dek`'s `verdict` matches it
  with a `_` arm: a variant added later is silently of the moment.
- A direct HTTP 404 on a KEK operation was `KekNotFound`
  (`vault/mod.rs:234` before this ADR). Transit answers a missing key on
  those paths 400, so a 404 there is a route: a missing or misnamed
  mount. Read as `KekNotFound` — Permanent at the stage, parked at once —
  a mount's name read as every Tenant shredded.
- A 403 whose read answered 404 was `KekNotFound` too, but Vault checks
  the policy before it looks a key up (measured below): a policy's drift
  on a context read as a Tenant shredded.
- A 400 whose explaining read failed became `Malformed` or `Decrypt`, the
  request's own fault, so a read that failed for a moment parked the
  message.
- `VaultRequest` and `VaultResponse` derived `Debug`, which shows the token
  and a key's plaintext.

**Measured** on Vault 1.21.4 (single node), as a service's AppRole and as
root, 2026-11-15:

| Request | Answer | Read of `keys/{name}` |
|---|---|---|
| datakey or decrypt, missing key | 400 "encryption key not found" | 404, `errors` empty |
| datakey, missing mount, service token | 403 | 403 |
| datakey, missing mount, root | 404 "no handler for route …" | 404, `errors` "no handler…" |
| datakey, context not admitted, missing key | 403 | 404, `errors` empty |
| datakey, context not admitted, existing key | 403 | 200 |
| datakey or read, unknown token | 403 "permission denied; invalid token" | 403 |

A missing key's read answers 404 with no words, a missing route's with
"no handler": the body tells them apart without matching a text.

## Decision

**The principle: a key serves only until its draw's start plus its
lifetime; a miss waits only on evidence that a draw will land in time;
what a stage does is counted and told.**

1. **A key's life counts from its draw's start S**: its hard end is S +
   lifetime − jitter (jitter ≤ lifetime/10). Vault answers 200 after a
   DELETE only to a request begun before it, so S precedes the deletion
   and the window is exactly the lifetime; no classification below can
   widen it (to be confirmed by a DELETE race on the stand, § Owed).
2. **A key leaves memory at its hard end**, wiped: each slot has one
   deadline at most — when it next changes by time, its key's end or the
   end of its failure's memory — kept in an ordered map, so the deadlines
   are never more than the slots; and a tick per stage, spawned lazily at
   the first flight, holding the stage weakly and sleeping until the
   earliest deadline, so an idle stage lets go of its keys. A slot is
   evicted only when it holds no key, no flight, no waiters and no failure
   still remembered — and then at once, by the transition that left it so
   (a seal, a waiter leaving, a landing) or by the sweep its deadline
   brings: no slot is kept that no sweep will reach. **A failure is
   remembered a back-off cap past its back-off's end**: until then it
   decides the next draw — a probe (decision 7), a doubled step (decision
   8's back-off) — and with it the slot. A waiter leaves once, whether
   woken, at its deadline or dropped with its caller. What the shell
   remembers of a tenant (why its last draw failed) is a note in its slot
   and goes with it; a flight's landing signal lives with the flight. The
   tick ends with its stage, or its runtime, and the next draw starts
   another. `Cached` drops an expired entry as it finds it and on
   `purge(now)`; the caller of `purge` is the opening side's (§ Owed 5).
   The transport's text of an answer is wiped; the JSON read from it is
   out of this library's reach until the caller drops it.
3. **A slot per (channel, Tenant)** holds the key in service, at most one
   flight, and evidence: trusted, late, or failed until an instant.
   - A landing signals its generation only. Woken waiters re-enter `seal`
     under the stage's mutex and take a use there, so every use is
     counted; waiters beyond a key's uses start the next flight: N misses
     cost ceil(N / messages) draws.
   - A seal's deadline is absolute: its start plus `wait`, however often
     it joins, or waits for a place.
   - A stage that runs as many flights as it may lets a seal wait for a
     place, freed by a landing, until its deadline; then it is refused
     `FlightsFull`.
   - A key per message (`messages` = 1) shares nothing: each seal draws on
     its own task, bounded by `draw`, as before this decision; a key gone
     is `Permanent`, any other failure of the moment.
4. **Generations** are one monotonic counter per stage, never reused; a
   landing is addressed by (Tenant, generation), and a flight captures its
   own stage: its KMS instance and derivation context.
5. **A flight** is started by a transition under the stage's std mutex
   that returns `Start{generation}`; the guard and the cap place are built
   there; the mutex is released; then `Handle::try_current()` and
   `tokio::spawn`, nothing awaited in between. No runtime: the seal is
   refused `Unspawned` and the guard frees the slot without the mutex. The
   library bounds the whole flight with `timeout(draw)` (a draw may be two
   requests, the operation and its explaining read); runtime shutdown
   lands `Canceled`, an outcome of its own. tokio is used directly: it is
   already in `dek[bus]`'s graph through `ascetic-ddd-bus`, and the inbox
   spawns the same way (`inbox/src/channel.rs:161`).
6. **A waiter that gives up** carries its generation and the time, and
   its give-up is evidence that the flight is late only when that flight
   is still in flight and has flown a `wait` at least — a waiter that
   joined it with little of its own wait left proves nothing; on its
   deadline it re-runs `seal` once under the mutex before refusing; it
   registers for the landing under the mutex.
7. **Evidence.** A miss joins a flight and waits only when its slot is
   trusted; a late or failed slot refuses at once. A slot whose backoff
   ended is trusted for its first draw when a slot of the same stage
   landed a draw in time (≤ `wait`) after this slot's failure: Vault
   demonstrably answers. No breaker spans Tenants: one Tenant's 429 or 5xx
   never stops another's draws.
8. **The class table** is `kms`'s (`Error::class()`), total over every
   variant and the fields that tell them apart:
   - **Gone**: `KekNotFound` at no version (a 400 whose read answered 404
     with no words), `SigningKeyNotFound`, `MacKeyNotFound`;
   - **Refused**: `KekNotFound` at a version (a retired one, the
     message's), `Decrypt`, `WrongKeyVersion`, `NoKeyOfVersion`,
     `InvalidSignature`, `UnsupportedAlgorithm`, `Malformed`, `Conflict`,
     `Forbidden`, `Unsupported`;
   - **Unavailable**: transport, Vault 412, 429 and 5xx, a transient
     session or database error;
   - **Moment**: any other Vault status, a non-transient session or
     database error, a migration's, the operating system's randomness.

   The adapter reads Vault so that the table holds: a direct 404 is
   `Vault{404}`; a 403 is `Forbidden` only when the key is read as there,
   otherwise the 403 as it came; a 400 whose read failed is the 400 as it
   came; a read's 404 with words is a route, not a key not there.

   **At sealing only Gone is Permanent**: every other class backs off as
   of the moment and is counted with its cause — a policy's drift is
   mended by a deploy, not by parking every Tenant's messages. A slot's
   memory of Gone tells every seal within it Permanent, none of them
   drawing.
   - **The premise.** On Vault Community a standby node forwards to the
     leader and never answers a key it holds as missing. Measured
     2026-11-16 on a three-node Raft cluster: 200 uses of a key made a
     moment before, through a standby; and about 20 000 requests through
     a standby while the leader was killed. The latter met elections
     (`500`) and redirects (`307`), and no false `404`.
   - Enterprise's performance standbys answer from their own state and
     reopen this: a false Gone would park a Tenant's messages.
   - The interim rule, Gone told to one seal and the rest refused as
     Unavailable, is withdrawn.
   - A Vault answer that cannot be read (a `200` without the field asked
     for, or not base64, a signature that is not JWS) is
     `Vault { status: 200 }`, of the moment, never the request's fault.

   At opening, the per-message refusals stay Permanent (ADR-0017; the
   consumer's ADR-0071 Rule of Three 1–2).
9. **Errors.** `dek::Unavailable{reason, retry_after, last}` is transient;
   its reason is `#[non_exhaustive]` (`Waited`, `BackingOff`, `Late`,
   `FlightsFull`, `Unspawned`); its `Display` names the last flight's
   outcome and Vault's status. Gone keeps `Permanent(kms::Error)`.
10. **Told.** A `SealingObserver`, called outside the mutex, one per
    channel (`Sealing::over_paced` takes a maker of them): flights with
    their outcome (`Landed`, `Gone`, `Refused`, `Unavailable`, `Moment`,
    `Abandoned`, `Canceled`), error, elapsed time and consecutive failures;
    served; unserved with its reason; retired — every key out of service,
    at its end, its uses spent, replaced or its tenant's gone.
    `Sealing::held()` gives each channel's keys, slots and flights. `dek`
    logs a slot's transitions through `log` — failing (warn), drawn again
    (info) — one line per stage per ten seconds at most, the next line
    counting the transitions kept quiet.
11. **Jitter and time.** Jitter is an input of the pure core, drawn from a
    per-`Sealing` seed (getrandom), injectable. Time is
    `tokio::time::Instant`; `crates/dek/clippy.toml` disallows the standard
    library's clock; the core's arithmetic saturates and denies panics.
12. **Parameters** are a `Pacing`, `#[non_exhaustive]` with a builder per
    part and `Pacing::fitted(reuse)` fitting any `Reuse`; they are validated
    by `Sealing::over_paced` and `EnvelopeStage::paced`, which return
    `Result<_, InvalidParameters>`, while `Sealing::over` and
    `EnvelopeStage::reusing` keep their signatures and take the fitted
    pacing. The rules: lifetime and the backoff cap ≤ a century (`LONGEST`,
    so that an instant plus any of them is an instant; `fitted` clamps a
    longer reuse to it); messages ≥ 2·burst, or both one; 0 < wait <
    draw < lifetime/2; renewal lead in [draw, lead_max] with jitter +
    lead_max + 2·draw ≤ lifetime; backoff base ≥ 2·wait (so that half a
    step, the least back-off, is never zero), cap ≥ base; 0 < gone ≤
    the backoff cap; flights ≥ 1; lifetime + cache + draw ≤ the ceiling
    the consumer passes. `burst` is the most seals one caller's
    transaction makes on one (channel, Tenant). Defaults are hypotheses
    until a staging measurement: wait 250 ms, draw 5 s, backoff 1 s to
    30 s, gone 10 s, 16 flights.
13. **Renewal ahead.** The successor is drawn at a jittered lead before
    the hard end, or when the uses left fall to max(burst, a share of the
    messages), never past the hard end. Without it a Vault slower than
    `wait` but answering turns every key's end into refusals.
14. **The flight cap is partitioned**: probes — a failed slot's first draw
    — take at most a quarter; renewals take a place only while half are
    free; misses of trusted or new slots take any free place.

## Consequences

- A Vault that stalls holds a caller for `wait` at most, once per slot,
  and then nothing: later seals are refused at once with `retry_after`.
- One draw serves its waiters; a slow Vault converges instead of being
  hammered.
- A key serves exactly its lifetime from its draw, and leaves memory then.
- A mount's name, a policy's drift or a read that failed never reads as a
  Tenant shredded, and never parks every Tenant's messages.
- `Sealing::over_paced` can refuse to build; a consumer's composition
  root refuses to start on an invalid configuration. `Sealing::over`
  keeps its signature: a consumer that names no pacing gets the fitted
  one.
- A stall longer than the inbox's attempts still parks inbound messages:
  the inbox's own change (§ Owed).

## Rule of Three

1. **A slow but healthy Vault above `wait`.** Without renewal ahead every
   key's end would be a refusal; decision 13 ships with the stage, and
   `wait` comes from a staging measurement.
2. **One transaction sealing more than a key's uses** (the consumer's
   policy publication: thousands of sends on one channel in one
   transaction). Without `burst`, a slow Vault refuses it at every key it
   crosses, forever; decisions 12 and 13 bound it.
3. **A false Gone** from a lagging standby node: measured absent on
   Vault Community (decision 8), whose premise an Enterprise deployment
   reopens.
4. **A dead flight wedging a slot**: the guard lands `Canceled` on a
   panic or a shutdown; no runtime is refused at once.
5. **Keys lingering in memory**: decision 2's tick and eviction rule.
6. **One Tenant's hanging probes starving another's draws**: decision 14.

## Alternatives Considered

- **Bounding at the caller only (a shorter HTTP client).** The draw is
  awaited in the caller's future, so a caller's bound cancels it: under a
  Vault slower than the bound no key ever lands.
- **A decorator around the KMS port.** A flight in it would carry one
  caller's session into every joiner's call, or ignore the session the
  port is handed.
- **A breaker across Tenants.** One Tenant's 429 would stop every
  Tenant's draws.
- **Serving an expired key through an outage.** Widens the shredding
  window without a bound; a longer lifetime within the consumer's ceiling
  is a parameter, not a mechanism.
- **A `Runtime` port.** tokio is already in the graph; a port adds a seam
  without a second implementation.

## Executors

S1a (built):
- `error::tests`: the class of every variant, and of Vault's statuses.
- `vault_mapping`: a direct 404 on datakey, encrypt, decrypt, rewrap and
  a key's making is `Vault{404}`, class Moment, nothing read; a 400 whose
  read fails, or finds no route, is the 400, class Moment; a 403 whose
  read answers 404 is the 403, class Moment; a missing managed key is
  Gone.
- `transport::tests`: `Debug` names neither the token nor a body.
- `cache::tests`: an expired key goes, wiped, when found or purged.
- Live (`make test-vault`): a missing key is Gone, a missing mount a
  route of the moment.
- Mutations, each killed: a route's 404 read as a key not there; a 403
  and a read's 404 read as a key not there; a 400 whose read failed read
  as the request's fault; a 429 out of Unavailable.

S1b (built), `dek::supply`, the pure core — `Core::seal`, `landed`,
`gave_up`, `left`, `sweep`, `next_deadline`, time and randomness passed in:
- unit tests: concurrent misses cost one draw and wait on it; a landed key
  serves its waiters no more than its uses; a give-up marks its flight
  late and a late flight refuses at once; a give-up after its flight
  landed is inert; a failed draw backs off 0.5, 1, 2, 4, 8, 15, 15 s with
  the jitter at its least; the first draw after a back-off waits when the
  stage has landed in time since; a shredded tenant's key retires at once
  and it is not asked again for `gone`; a key serves from its draw's start
  for its lifetime; a key leaves memory at its end on an idle stage, and
  its slot with it; a slot backing off is not evicted; a landing reaches
  its own flight and a stale one changes nothing; hanging probes and
  renewals never take every place; each invalid parameter is refused by
  its rule; a key landing past its end leaves no slot behind; a note goes
  with its slot;
- a property over interleavings of three tenants (seals, time, landings
  current and stale, give-ups, sweeps): at most one flight per tenant and
  the cap per stage, no hit past a key's draw plus its lifetime or its
  uses, no key drawn before its tenant's was gone, a stale landing
  changing nothing, `retry_after` within the back-off's cap, no slot kept
  that no sweep will reach (added with S1c: it found a seal refused for a
  full stage keeping an empty slot for good); and a test on a fixed seed
  that the histories reach every reason, kind and outcome;
- mutations, each killed: a key served past its end; past its uses; a
  stale landing applied; a renewal started beside a flight; a late flight
  joined; the key kept after its tenant's was gone; no doubling; no cap
  for misses; a key's life from its landing; with S1c, a seal, a waiter
  leaving or a landing keeping an empty slot; a note keeping its slot.

S1c (built), `dek`'s shell (`draws.rs`): `EnvelopeStage` draws through it
whatever its constructor; `Pacing` (fitted to any `Reuse`, each part set
by a builder) and `Sealing::over_paced` with a ceiling and a maker of
observers per channel; `Sealing::over` keeps its signature and gets the
fitted pacing; `Sealing::held()`; the opening end's verdict by the KMS's
class; `dek::Unavailable`, `SealingObserver`, `Held`;
`crates/dek/clippy.toml` refuses the standard clock, and `supply.rs`
denies what panics (`expect`, `unwrap`, `panic`, indexing, unchecked
arithmetic). Tests on tokio's paused clock against a KMS whose every draw
a test releases (`tests/draws.rs`):
- concurrent misses cost one draw and every one is served;
- a waiter refuses at its bound — exactly the wait — and a late draw
  refuses at once; the draw lands for the next;
- a seal waits its bound once however often it joins, and a flight joined
  with little of a wait left is not late;
- a dropped caller does not cancel the draw, and leaves nothing behind;
- a key gone is told to the first seal to ask, though its draw outlived
  the wait, and to no other within `gone`;
- a sealing-side refusal backs off and is never for good, its cause told;
- a KMS out of reach is asked less and less however often a tenant seals:
  at most eight draws in a minute of a seal every 100 ms (75 before the
  failure was remembered past its back-off);
- one tenant's failing draws do not stop another's;
- a seal on a full stage waits for a place within its bound;
- a flight past its bound is abandoned, its future dropped, its place
  given back; a flight that dies (a panic) frees its slot;
- a draw outside a runtime is refused `Unspawned`, neither deadlocks nor
  wedges its slot, and a runtime draws again; a hit is served though its
  successor's draw cannot start;
- a key leaves memory at its end though no one asks; a tick ended with
  its runtime starts again in the next; a stage dropped ends its tick;
- no seal waits while its successor is drawn; a burst under a KMS slower
  than the wait never waits, its key renewed by its uses alone;
- a key per message is drawn for each seal at once;
- a stage configured again while a draw flies starts draws of its own;
- a fitted pacing fits any reuse, `Duration::MAX` too, and serves;
- a tenant's last failure is forgotten with its slot;
- an observer is told every flight with its failures in a row, every
  seal served and unserved, every key retired, replaced ones too; a
  sealing tells each channel's observer of its own stage;
- a sealing whose keys would outlive its ceiling is refused;
- an outage of fifty tenants at once is logged one line, the next line
  counting the 49 kept quiet;
- on four threads, 64 tasks over 8 tenants against a KMS failing one draw
  in five: every seal keeps its bound, the cap holds, and the stage
  drains to nothing.
Core tests added: a give-up with little of a wait flown is no evidence; a
landing slower than the wait is no evidence that the KMS answers; a
renewal takes a place only while half are free; a failed slot is kept a
cap past its back-off and then goes; a key landing past its end leaves no
slot behind; a note goes with its slot; the doubling test sweeps at every
back-off's end. The property holds that a slot has one deadline, when it
next changes by time, and that every key out of service is told once and
never served again.
Mutations, each killed (22 in the last round): a failure forgotten at its
back-off's end; Gone never told; any give-up taken as evidence; a slow
landing counted in time; a renewal taking any free place; a replaced key
not retired; no renewal by uses; an idle slot kept; the back-off's 2·wait
rule dropped; the deadline set again per join; no tick end on a stage's
drop; per-message seals single-flight; a full stage refusing at once;
served, failures or a landing's retired keys not told; every transition
logged; a dropped waiter never leaving; the tick's flag never cleared; a
hit refused when its successor cannot start; no ceiling; the observer
dropped. Earlier: a waiter giving up leaves its slot trusted; a dead
flight never lands; no bound on a draw; no tick; no century's clamp.

## Owed

1. **S1b, `dek`'s pure core** — built 2026-11-15 (§ Executors).
2. **S1c, `dek`'s shell** — built 2026-11-15 (§ Executors). Still owed of
   its list: the jitter's spread over its interval (a mutation drawing no
   jitter survives); a landing reaching only its own slot across two
   stages. Its original list: N misses cost one draw; a
   landed key serves its waiters no more than its uses; a waiter refuses
   at its bound and the draw lands for the next; a dropped caller does not
   cancel the draw; a late draw refuses at once; the first draw after a
   backoff waits when the stage has landed since; a failing KMS is called
   once per backoff step (jitter pinned); the jitter spreads; one Tenant's
   failures do not stop another's draws; a full stage refuses without a
   call; a sealing-side refusal backs off; a shredded Tenant's key retires
   at once; a key's age counts from its draw; a key leaves memory on an
   idle stage; a landing reaches only its own slot; a flight that dies
   frees its slot; a flight past `draw` is abandoned; no runtime neither
   deadlocks nor wedges; no seal waits while its successor is drawn;
   invalid parameters are refused; `burst` seals back to back under a slow
   KMS are never refused after warm-up.
3. **S1d, the consumer** (architecture-docs ADR-0091 § Owed S4).
4. **The inbox's `Unavailable` marker**: a retry that spends no attempt,
   for stalls longer than the inbox's attempts; its own ADR, revising
   ADR-0005 and ADR-0010: ADR-0040 (Proposed 2026-11-16), whose S2 gives
   the stage's verdict its scope.
5. **The opening side**: one unwrap per (channel, Tenant, wrapped key), a
   memory of Gone per (channel, Tenant), age from the unwrap's start, a
   cap; the unwrap cache moved out of `Cached`; ADR-0017 decision 2
   revised.
6. **Measurements**:
   - Measured 2026-11-16 and closed:
     - a datakey racing a key's deletion: every `200` draw began before the
       deletion's answer, so decision 1's S holds;
     - a standby node's answers: decision 8's interim rule is dropped.
   - Owed to staging:
     - Vault's datakey latency under load, to set `wait` and `draw`;
     - a cold start of 500 and 2 000 tenants on one channel, counting the
       seals refused `FlightsFull` after their wait for a place, to set
       `flights`.
7. **`Cached::purge`'s caller**: the opening side's tick (slice 4), told
   the cache's next expiry and woken by the first entry of an empty cache;
   until then an unwrapped key outlives its TTL on a quiet channel until
   the next unwrap.

## References

ADR-0002 (serialize and encrypt before the outbox), ADR-0010, ADR-0016,
ADR-0017 (amended: decision 8 above replaces its point 5's reading of a
`403`), the consumer's ADR-0071 and ADR-0091.

## Change Log

- 2026-11-15 — Proposed; S1a built.
- 2026-11-15 — S1b built: `dek`'s pure core.
- 2026-11-15 — S1c built: `dek`'s shell. Its review found five defects
  in S1b's core and S1c's first draft, each fixed with its test: an empty
  slot kept for good (a seal refused for a full stage, a waiter dropped
  with its caller); the shell's per-tenant maps never shrinking, scanned
  whole under the lock on every tick; the tick never restarted after its
  runtime; a stage configured again while a draw flew panicking; a hit
  refused when its successor could not start. Decision 2 says how a slot
  goes; decision 12 how parameters are validated, with `LONGEST`.
- 2026-11-15 — S1c reviewed again by two roles (concurrency; resources
  and verifiability), each told the task and not our reading. Fixed, each
  with a test that failed first: a failure forgotten at its back-off's end
  (no doubling, no probe: 75 draws a minute instead of eight); Gone lost
  when its draw outlived the wait (now told to the first seal to ask); a
  give-up with no wait left marking its flight late; a key per message
  made single-flight (now drawn per seal); a full stage refusing at once
  (now a wait for a place); a stage dropped leaving its tick; a deadline
  entry per draw (now one per slot); keys replaced or spent untold;
  decision 10's failures, served and rate-limited log unbuilt; a first
  back-off step of a nanosecond; the burst test passing without renewal
  by uses. Decisions 2, 3, 6, 8, 10 and 12 say what changed;
  `Cached::purge`'s caller moved to § Owed 7.
- 2026-11-16: Owed 4 names its ADR, ADR-0040 (Proposed).
- 2026-11-16: A key whose renewal failed outlived its end on a quiet
  stage. The core's property found it (regression `ef90e39b…`).
  - A failed renewal leaves the key in service with the failure's
    memory, and that memory may end before the key does.
  - The slot's next change was the earlier of the two, dropped when it
    was past, so the key's end went with it. No sweep reached the key,
    and it stayed in memory until the Tenant's next seal.
  - The fix: each change is weighed only while it is ahead.
  - The property now also reads each change off the slot itself, not
    through the function it checks.
  - `a_key_whose_renewal_failed_still_leaves_memory_at_its_end` and the
    property both kill the defect.
- 2026-11-16: Decision 8's interim rule dropped (slice 4's S4e): on Vault
  Community a standby never answers a key it holds as missing (measured on
  a three-node Raft cluster), so every seal within a Gone's memory is told
  Permanent. A Vault answer that cannot be read is `Vault { status: 200 }`,
  of the moment (S4a), and the plaintext's text in an answer is wiped once
  read. Owed 6's race and standby measurements closed.
