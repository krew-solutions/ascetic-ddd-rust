# ADR-0047: A stage's dependency is suspected at its first refusal

## Status

Proposed (2026-11-20). Not implemented.

**Deferred (2026-11-20) until the measurements in § Open measurements decide it.** It changes ADR-0042's breaker, so before it is accepted it goes through the library's gate: a blind solver, a discriminator, and the roles.

**Origin.** It came from a dialogue between the owner and the consumer's session about the consumer's ADR-0091 § Owed 4 item 1, whose relay of a handler's `Unavailable` was refuted the same day:
- **The owner** asked whether processing rows whose keys are still cached, with the KMS down, pays for the machinery that tells an outage from one Tenant's trouble.
- **The consumer's session** answered that the gain is bounded and small, and that the cost sits in the breadth rule.

This ADR records that proposal and what would decide it. It changes no code.

## Context

**What the opening end's cache carries during an outage.**
- **The cache lasts minutes.** The opening end keeps an unwrapped key for `cache`, 5 minutes by default (`SealingParameters::default`, `crates/dek/src/sealing.rs:70`). A producer's data key serves 1,000 messages or 60 s (`Reuse`, `crates/dek/src/stage.rs:100-101`). In a full KMS outage the producers stop sealing within a minute, so the consumer's cache serves only the recent backlog, and for 5 minutes at most.
- **Cached rows do not flow during a confirmed outage.** Since slice 12 the dependency's breaker pauses a channel once a check confirms the outage, and "a row processed closes nothing" (ADR-0042 § 1). Cached keys carry rows only before the suspicion trips, and for the Tenants other than one in trouble, whom the memo holds apart (§ 2).
- **The outages a cache hides entirely are seconds long.** The consumer measured a Vault leader change on a three-node raft cluster (2026-11-16): the 307, 500 and transport answers lasted seconds.

**What the breadth rule costs** (ADR-0042 § 1):
- F and S over W = 30 s, held in a bounded set of 1,024;
- the dependency's `served` sampled at every dispatch, one sample a second kept;
- a second arm, F ≥ 32 and F ≥ 0.25 · (F + S), which exists because Tenants with cached keys still flow while many fail.

The thresholds are judgment (ADR-0042 Rule of Three 4), to be decided by M5 at scale. They sit in the core, its property tests and `InboxOutage.tla`.

**What the rule does for the dependency's breaker.** It decides when to spend a check. The check already decides whether to pause. A suspicion leaves the channel asked, and a check served refutes it (§ 1, Suspect).

## Decision (proposed)

1. **Suspect at the first refusal.** The dependency's breaker suspects at the first `Stage`-scoped refusal of its cause, in place of the breadth rule. A suspicion is a check, paced as today:
   - one check at a time a channel;
   - the step starts at 1 s and doubles at each refutation up to the tick's cap;
   - the step starts over after W without a suspicion.

   F, S and the `served` series leave the dependency's breaker.
2. **The check tells an outage from one Tenant's trouble.** It asks a control that is not the refusing Tenant's key. Today that is the last key the dependency opened, asked together with the head's key (Rule of Three 6).
   - Served: the refusal is the Tenant's own, and it goes to the Tenant's memo as positive evidence.
   - Unserved: the channel is paused, as today.
3. **The rows' breaker stays for channels with no registered dependency.** Since ADR-0044 the subscription registers one, and a host's start can refuse an unweighed destination, as the consumer's does.
4. **Unchanged:** the suspicion leaves the channel asked; a confirmed outage pauses it; the memo, the pause, the check, the stall report and the page.

## Consequences (if accepted)

- The breadth state leaves the core, its tests and the TLA model, and no threshold remains a judgment.
- Every new episode of refusal costs one check, bounded by the step. Today the first single Tenant's trouble costs none.
- A suspicion comes up to a window sooner. Today F must reach 2 Tenants and a share.

## Rule of Three (against the proposal)

1. **One Tenant's trouble can pause every Tenant.**
   - **When.** No control key exists yet: a process just started has opened no key. The check then asks the refusing Tenant's key alone, and its refusal reads as an outage.
   - **Today.** F ≥ 2 keeps one Tenant from opening the breaker.
   - **Owed.** The proposal needs a control before its first check: a sentinel's key, as decision 9 gives the sealing end, or no pause without one. The solver round must decide which.
2. **More checks against a struggling KMS.** In a brownout of 5 % at 100 asks a second, about 150 distinct Tenants refuse within W (§ 1). One check in flight a channel, and the step, bound the count, but every process and channel keeps its own step. Measure it (M-c).
3. **The last key opened is a Tenant's, and that Tenant's trouble can be the control's.** A customer-managed key revoked makes the control unserved while the KMS serves the others. ADR-0042 Rule of Three 6 bounds this today by asking the control and the head's key together. Under the proposal every suspicion leans on it, so the bound must be re-shown.

## Alternatives Considered

- **Keep the breadth rule (today).** This is what stands while the ADR is deferred: the consumer's recommendation is to measure before changing the core.
- **Stop serving from the cache during an outage.** Refused. A process would need a signal "the KMS is down" and an invalidation of every cache: more machinery, less availability, and no gain over the pause, which already stops cached rows once the outage is confirmed.

## Open measurements

They decide whether this ADR is reopened, in the drill of ADR-0042 § Owed 2 at the consumer's launch shape:
- **M-a.** The rows served from the opening cache between an outage's start and its pause, and the time they save.
- **M-b.** Under the proposal, how often and for how long one Tenant's trouble pauses a channel.
- **M-c.** Checks per process and minute in a brownout, under the proposal and today.

**Reopen** when M-a shows seconds saved, not minutes, and M-b is bounded. Then the proposal goes to the library's gate.

## References

ADR-0039, ADR-0040, ADR-0042 (§ 1, § 2, Rule of Three 4, 6 and 7, § Owed 2), ADR-0044. The consumer's ADR-0091 § Owed 4 item 1.

## Change Log

- 2026-11-20 — Proposed and deferred the same day, pending the measurements.
