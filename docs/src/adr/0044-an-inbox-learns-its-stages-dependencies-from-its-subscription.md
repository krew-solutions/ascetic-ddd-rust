# ADR-0044: An inbox learns its stages' dependencies from its subscription

## Status

Proposed and built (2026-11-19), in one slice:
- `crates/bus`: `Dependencies`, `Dependent`, `InboundStage`, `TransactionalConsumer::through` and `subscribe`,
  `TransactionalWireConsumer::subscribe`, `Error::Composition`;
- `crates/inbox`: the registration at subscribe, the inbox's one live subscription, `depending_on` for the raw path,
  `dependent_channels`;
- `crates/dek`: the envelope stage's one dependency, `ByDestination`'s declaration, `ByDestination::over`,
  `Unroutable`.

Revises ADR-0042: decision 1's "a channel whose dependency the composition root registered", § 12 item 7, the
Rule of Three's item 5 and § Owed 2's composition test and start-time check. The registration is no longer the
host's duty.

Decided under the library's gate: a blind solver, the main position, a discriminator, and four roles (Software
Design, Distributed Systems, Verifiability, Evolution) on the synthesis. § Alternatives Considered keeps what each
refuted.

## Context

ADR-0042 decision 1 gives a channel two breakers. The dependency's weighs a stage's refusals against what the
stage's dependency — the KMS of the envelope stage's opening end — served by a call, and closes only on a check of
it served. The rows' closes on a row processed. A stage that answers from a memory makes a row processed no evidence
of what it depends on: with the KMS down, a row whose key the opening end holds is processed, and the rows' breaker
closes the pause. M10 counted 309–537 such false resumes a run before slice 12.

Until this decision the inbox learnt a channel's dependency only from its composition root:
`PgInbox::depending_on(&opening)` beside `inbox.consumer(decode).through(opening)` (ADR-0042 § 12 item 7). Two
actions of the host had to be paired, and nothing paired them:
- the stages lived in the bus's `TransactionalConsumer` (`crates/bus/src/lib.rs`), the dependencies in the inbox;
- the bus handed the inbox an opaque handler, `TransactionalWireConsumer::subscribe(handler)`, its stages closed
  inside it.

The library's composition test (W1) checked the right composition; nothing caught a wrong one. Its one consumer, a
host, registered in three of its six services and not in the other three: the defect M10 measured was live there.

The round found two more facts on the way:
- **One inbox could be subscribed any number of times**, each subscription running over every row, unfiltered
  (`crates/inbox/src/channel.rs`). A second subscription without the opening stage, with a decoder that takes any
  bytes — W1's own takes them — handed its handler ciphertext and marked the row processed.
- **The envelope stage made a new dependency object at every call** (`EnvelopeStage::dependency`); only `Sealing`
  hid it, by calling it once per channel.

## Decision

**Principle:** a declaration is demanded exactly where something reads it, and kept where what it governs is kept.
The one reader of a stage's dependencies is the inbox's consumer: `TransactionalConsumer::subscribe` is the only
code that sees both the stages and the wire consumer. What it governs, a channel's gate in the breaker, belongs to
the inbox and outlives a subscription.

1. **A stage an inbox goes in through states its dependencies** (`crates/bus`).
   - `Dependent::dependencies(&self) -> Dependencies`, with no default body: a stage with none says so,
     `Dependencies::none()`, and a wrapper forwards or says none. The same objects on every call.
   - `Dependencies` is opaque: one dependency a destination, each destination without its key. Two different
     dependencies for one destination are a `Composition::Conflict` wherever they meet — in one declaration, in a
     merge, against what an inbox weighs already; the same object twice is one. Different means another object:
     a dependency is told by identity (`Arc::ptr_eq`).
   - `InboundStage: Stage + Dependent`, with a blanket impl. `TransactionalConsumer::through` takes an
     `Arc<dyn InboundStage>`: a plain `Stage` does not compile there.
   - `Stage`, `Producer::through`, `TransactionalProducer::through`, `Consumer::through` are unchanged: no producer
     and no plain consumer has anything that weighs a dependency.
2. **The subscription carries them** (`crates/bus`).
   - `TransactionalWireConsumer::subscribe(&self, handler, dependencies)`.
   - The typed `subscribe(handler)` merges its stages' declarations first; a conflict is
     `Error::Composition(Composition::Conflict)`, and the wire consumer is not asked.
   - `bus::Error` is `#[non_exhaustive]`, and its new variant `Composition` carries a `#[non_exhaustive]` enum: a
     composition error is told apart from a transport's failure of the moment, and a later refusal of the kind
     breaks no host's match.
3. **The inbox registers at subscribe, and one subscription is live at a time** (`crates/inbox`).
   - In this order, so that a refusal keeps nothing of the call: the runtime; the inbox's one live subscription,
     else `Composition::AlreadySubscribed`; every declared channel checked against the store, else
     `Composition::Conflict`; all of them committed — each gate registered in the breaker, the store's map swapped
     whole; only then the task.
   - The live subscription is held by the task, as a value it owns: let go when the task ends — returned, unwound by
     a panic, or dropped with its runtime — and before its cancel learns it ended. Not by the handle: the docs call
     dropping it usual, and the loops run on.
   - The store, beside the breaker, is never emptied: a channel's gate has no inverse, and a pause opened under one
     subscription is closed by its dependency's checks under the next.
   - A reader clones the store's `Arc` under its read lock and lets the lock go at once; the write takes the
     store's lock, then the breaker's.
   - `PgInbox::depending_on(self, &Dependencies) -> Result<Self, Composition>` stays, for the raw `run`/`dispatch`
     path, which has no stages; the same rule.
   - `PgInbox::dependent_channels()`: the channels weighed by a dependency, over the inbox's life — what a host
     checks its sealed channels against.
4. **The DEK's stages say what they can** (`crates/dek`).
   - `EnvelopeStage` makes its dependency with its opener, and `dependency()` clones it.
   - `ByDestination` declares each destination's: the KMS of the channel it opens under.
     `Sealing::by_destination` refuses, `Unroutable::Ambiguous`, two destinations that differ by their key only and
     map to two channels; `UnsealedChannel` is now `Unroutable::Unsealed`.
   - `ByDestination::over(stage, destinations)`: one channel's stage for every destination, its one dependency
     declared for each.
   - `EnvelopeStage` and `Sealing::of` state none: they do not know the destinations an inbox receives them under.
     An inbox opens through a `ByDestination`, as `crates/dek/src/sealing.rs` already said.
5. **The toolchain is the latest stable Rust, 1.100.0, and `rust-version` says so** (the owner's decision of
   2026-11-19). The declared 1.85 was false: the workspace did not build on 1.88 or 1.89 (§ Measured). With
   `rust-version` the version CI builds on, the declaration has its executor in every push. The lints 1.100 adds
   are fixed in the same change.

## Consequences

- **A host cannot subscribe an inbox through a stage without registering what it declares**, and cannot register
  one stage's dependencies while going in through another's: there is no second action to forget.
- **Every sealed channel of a host that subscribes through `ByDestination` is weighed by its KMS at the pin**, the
  three services that never registered included. That is the behaviour ADR-0042 decided; for those services it is
  new (§ For the consumer).
- **A second subscription of an inbox is refused while one is live**, and accepted once its task has ended. A cancel
  from inside a handler returns at once (`Subscription::cancel`), so a subscription made right after it, in that
  handler, is refused.
- **What is not covered**, named so that the freeze is not read as covering it:
  - `inbox.consumer(decode).subscribe(handler)` with no stage at all compiles: a sealed channel read without being
    opened. A later, additive check would need the inbox told which channels are sealed.
  - The single subscription is one `PgInbox`'s: a second `PgInbox` over the same table, another process, or the raw
    `run`/`dispatch` path each have their own. A decoder that takes any bytes there still marks ciphertext
    processed.
  - A host's own stage that wraps a `ByDestination` and states `none()` compiles, and the channel falls back to the
    rows' breaker. Its executor is the host's: a grep for `impl Dependent` at the pin, and its composition test.
- **The drills change meaning**: M5/M6 and M8 went in through `by_destination` without `depending_on`, so their
  recorded runs measured the rows' breaker alone; they now register. M10's unregistered arm wraps the stage to
  declare none (`Undeclared` in `crates/dek/tests/drill.rs`), the one composition the library keeps a host from
  making by mistake, made there on purpose. Each arm asserts `dependent_channels()` after it subscribes.

## Rule of Three

1. **A conflict that is none.** A stage rebuilt over the same opener, or a `ByDestination::over` built twice, would
   declare a different object for the same KMS, and a re-subscription would be refused. Decided by making the
   dependency one object for the opener's life, and checked (S6).
2. **A refused subscription that keeps something.** A guard taken and never released, or channels registered
   before a conflict, would leave an inbox that can never be subscribed again, or a channel weighed by a dependency
   nobody opens through. Decided by the order of decision 3 and checked: after a conflict, outside a runtime, and
   under a live subscription, `dependent_channels()` is unchanged and a correct subscription is then accepted (S3).
3. **A guard that outlives or underlives its task.** Released at the cancel's call, two runs would overlap;
   released by a statement a panic skips, the inbox could not be subscribed again. Decided by a guard the task owns,
   and checked: a cancel pending on a handler in hand, a panicking handler, a dropped handle, a cancel from inside a
   handler, fifty cancels and subscriptions in a row on a multi-threaded runtime (S3b).
4. **The registration not weighed at all.** The store filled but the gate not registered, or registered for one
   destination only, would leave a sealed channel to the rows' breaker. Checked by W1 without `depending_on`, its
   warm rows on both destinations and every resume by a check served (S4).

## Alternatives Considered

- **A required `Dependent` on every `Stage`, with an "every destination" form for the envelope stage** (the main
  position). Refuted by the discriminator: without "every", the envelope stage would have to state none, a lie
  that compiles; with it, a bare `EnvelopeStage` on an inbox of several channels sends another channel's wrapped
  key to its own KMS — on the PostgreSQL KMS that row is refused and parked for good (`kms/src/pg.rs`,
  `dek/src/opens.rs`), on Vault it may be retried for ever. And producers and plain consumers would be made to state
  what nothing reads.
- **The dependencies kept by the subscription, carried into `run`** (the main position). Refuted by a failure
  scenario: a pause opened under one subscription stays open after its cancel; a next subscription declaring
  nothing for that channel answers every check `Unasked`, and with a head queued `Unasked` never closes the gate
  (`crates/inbox/src/breaker.rs`). And `run` and `dispatch` are public, driven without a stage by the slice-12
  executors and 192 test call sites.
- **Keep `depending_on` as the host's duty, and have `subscribe` verify it** (the solver's fallback). Keeps a duty
  whose only purpose is to be checked.
- **A public chain stage in the bus that forwards its stages' dependencies** (the main position). Not now: inbound,
  the consumer already is the chain, and merges; the host's own `Chained` serves its producers only.
- **The port's new argument as a struct, to absorb later additions** (the solver's "would break again"). Refuted
  by the discriminator: a field an adapter can ignore without a compile error is this round's defect moved one
  field later. A later fact rides on `Dependency`, or inside the opaque `Dependencies`.
- **Returning the dependencies `#[must_use]`** and **a probe port on the stage**: refuted in ADR-0042, not proposed
  again.

## Executors

| Id | Where | Asserts | Mutation it turns red |
|---|---|---|---|
| S1 | `crates/bus/tests/stage.rs` | a consumer through two declaring stages and one that declares none hands the wire consumer exactly their merge, keys aside | the merge not handed; only the first stage's; only the last's |
| S2 | `crates/bus/src/dependency.rs` | a destination is declared without its key; one object twice is one; two objects for one destination, key or no key, are a conflict, in one declaration and in a merge | last wins; the key kept |
| S2b | `crates/bus/tests/stage.rs` | two stages declaring two objects for one destination: `subscribe` is `Composition::Conflict`, the wire consumer not asked; one object twice subscribes | the conflict swallowed, the wire consumer asked with none |
| S3 | `crates/inbox/tests/subscription.rs` | a subscription registers every channel its stages declare, and the registration outlives its cancel; a refused one — a conflict, outside a runtime — writes nothing and holds nothing; the same object the inbox was built with is no conflict | the argument ignored; registered as checked; registered before the runtime |
| S3b | `crates/inbox/tests/subscription.rs` | a second subscription refused, and registering nothing, while the first's task runs — a cancel pending on a handler in hand included — and accepted after; a panicked task lets go; a dropped handle does not; a subscription after a cancel from inside a handler is refused | the guard gone; released as the task starts; released by a statement after the loop; not released on a refusal |
| S4 | `crates/dek/tests/composition.rs` (W1) | no `depending_on`: `dependent_channels()` empty before the subscription and both destinations after; warm rows on both destinations; no resume while the KMS is down; every resume by a check served | the inbox ignores the argument; `ByDestination` declares none; one destination registered only |
| S5a | `crates/bus/tests/stage.rs` | `TransactionalConsumer::through` coerces to `fn(_, Arc<dyn InboundStage>) -> _` | `through` takes any `Stage` (a compile error) |
| S5b | `crates/dek/tests/opens.rs` | `EnvelopeStage` is not `Dependent`, by an ambiguity the call cannot resolve if it were | `impl Dependent for EnvelopeStage` (a compile error) |
| S5c | `crates/bus/src/dependency.rs`, a `compile_fail` doctest and its compiling twin | `impl Dependent for X {}` does not compile; with the body it does | a default body for `dependencies` |
| S6 | `crates/dek/tests/opens.rs` | the stage's dependency one object across calls, two `ByDestination::over` and two `by_destination`; the dependency the opener's that opens, built opened then with the sentinel as a sealing builds it; each destination's dependency; two destinations by key only under two channels refused | a dependency made per call; `opened` or `with_sentinel` keeping the old opener's dependency; the map keeping the first |
| S7 | `crates/inbox/src/pg/check_tests.rs`, `crates/inbox/tests/outage.rs` | the raw path's registration by `depending_on(&Dependencies)`, and `dependent_channels()` over it | `depending_on` dropped |
| S8 | `crates/dek/tests/drill.rs`, hand-run | each arm's `dependent_channels()` after it subscribes | — |

## Measured

On this box, 2026-11-19, PostgreSQL 18 for the live tests.

**The defect, before the change.** W1 at 033cb68 with its `depending_on` line deleted is red where it should be:
with the KMS down, `inbox://orders` resumed twice by a row processed (`Resumed { by: Processed }`), the flapping
M10 counted. So W1 tells a registered composition from an unregistered one, and S4 rests on it.

**The mutations.** Twenty-two, each applied alone and its executors run (`cargo test`, the live ones with
`--ignored`); every one red:

| Executor | Mutations, each red |
|---|---|
| S1 | the merge not handed; only the first stage's; only the last stage's |
| S2 | last wins; the key kept |
| S2b | the conflict swallowed |
| S3 | the argument ignored (S3 and W1 red); registered as checked; registered before the runtime |
| S3b | the guard gone; released as the task starts; released by a statement after the loop; not released on a refusal |
| S4 | one destination registered (W1 and S3 red); `ByDestination` declares none (W1 red) |
| S5b | `impl Dependent for EnvelopeStage`: `E0283` at the ambiguity in `crates/dek/tests/opens.rs` |
| S5c | a default body: the `compile_fail` doctest compiles, and fails |
| S6 | a dependency per call; `opened` keeping the old opener's (five checks of the opening end red); `with_sentinel` keeping it (red only by the test added for it: no other executor built a stage in the order a sealing does); the map keeping the first |
| S7 | `depending_on` registering nothing |

**The toolchain.** The workspace declared `rust-version = "1.85"`. It did not build on 1.88 or 1.89:
`ascetic-ddd-kms` failed with `E0308`, "one type is more general than the other", on the boxed future of a Vault
call (`crates/kms/src/vault/mod.rs`); on 1.93 it built. The owner chose the latest stable, 1.100.0, over measuring
the floor between. Its lints, fixed in this change: fourteen `if`s collapsed into let chains, one
`is_multiple_of`, `fetch_update` renamed `try_update`, and the manifests' `homepage`, which repeated `repository`.

## For the consumer

0. **The toolchain.** The library builds with Rust 1.100.0 and declares `rust-version = "1.100"`. The host
   declares 1.89, which was already false against the library: it does not build on 1.89 (§ Measured). The host
   moves its toolchain and its declaration with the pin.

The host pins the library by git revision. At the pin that carries this decision:

1. **Subscriptions.** `inbox.consumer(decode).through(Arc::new(opening)).subscribe(handler)` with `opening` from
   `sealing.by_destination(intake)` registers the dependencies. Delete every `PgInbox::depending_on(&opening)`; the
   method now takes `&Dependencies`, for the raw `run`/`dispatch` path only.
2. **`InboxOpening`** in the host's shared kernel paired `by_destination`, `depending_on` and the subscription.
   The pairing is the library's now: the type can go, or shrink to building the `ByDestination`.
   `subscribe_intake` takes `&InboxOpening` today; it takes the stage, or the `Sealing` and the intake, instead.
3. **The three services that never registered** (application, content, telemetry): their sealed channels start
   pausing by their KMS at this pin, and resume only on a check of it served — the behaviour the others have since
   their `InboxOpening`. Say so to their owners: with the KMS down their channels stall instead of flapping.
4. **Errors.** `bus::Error` is `#[non_exhaustive]`: the host's `verdict_of` (`crates/bus/src/verdict.rs`) needs a
   fallback arm; `Error::Composition(_)` is a configuration bug, never transient. `Sealing::by_destination` returns
   `Unroutable`; `UnsealedChannel` is its `Unsealed` variant.
5. **Stages.** No host stage goes in on an inbox but `ByDestination`, so no host stage needs `impl Dependent`:
   `Chained` and `DepthBound` serve producers. A host test that goes in on an inbox through a stage of its own
   implements `Dependent`.
6. **One subscription an inbox.** A host test that subscribes an inbox the service's factory also subscribes now
   gets `Composition::AlreadySubscribed`: check `services/policy/tests/it/event_streams.rs` and
   `services/policy/tests/process_management.rs` at the pin. A `start()` that starts a relay before it subscribes
   and fails on the subscription leaves the relay running (a dropped handle does not cancel).
7. **The composition test** (ADR-0042 § 12 item 7) narrows to: after the subscription, `dependent_channels()`
   covers every sealed destination of the intake.
8. **Rollout.** Replicas on the old pin keep the rows' breaker beside new replicas paused by the dependency until
   all have moved: no wrong data, but whether the stall alert ("every replica stalled") holds in the mixed fleet is
   not established. Rolling back the pin undoes only this decision's behaviour; it keeps no durable state.

## Owed

- **`Breaking::trial_wait`** is "the stage's own wait", kept equal to the opening's wait by hand (ADR-0042 decision
  1). It can ride on `Dependency` as a required method, which no host implements, so the change would cost the host
  nothing. Owed with ADR-0039 § Owed 6's retuning.
- **A sealed channel read with no stage** (§ Consequences): the inbox told which channels are sealed, checked at
  subscribe against the declared `Dependencies`. Additive.

## References

ADR-0018 (one subscriber an inbox), ADR-0039 (the DEK's opening end), ADR-0042 (decision 1, § 12 item 7, § Owed 2).

## Change Log

- 2026-11-19: Proposed and built.
  - The blind solver placed the declaration on an inbound stage and the store in the inbox; the main position put
    it on every stage and in the subscription. The discriminator refuted the main position's two placements by
    facts, and kept its conflict rule, its explicit "none" and its evolution by `Dependency`.
  - The roles added the dependency's one object (Software Design, Distributed Systems, Evolution, Verifiability),
    the keyless rule owned by `Dependencies` (Software Design, Distributed Systems), the order inside `subscribe`
    and the guard owned by the task (Distributed Systems), `Error::Composition` and `#[non_exhaustive]` (Software
    Design, Evolution), and the executors that turn the surviving mutations red (Verifiability).
