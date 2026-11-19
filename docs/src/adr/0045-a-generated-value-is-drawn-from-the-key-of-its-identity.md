# ADR-0045: A generated value is drawn from the key of its identity

## Status

Proposed (2026-11-20); slices S1 and S2 built. Stage 1 of three toward the host's fitness functions:
1. **This decision:** the faker — a port of ascetic-ddd-python's `faker/domain/fp` and what it needs.
2. **A later ADR:** the scenario file, its writer and its validator.
3. **A later ADR:** the scheduler that runs a scenario file against a stand.

To be built in four slices, each a commit of its own:
- **S1:** the crate, `Key`, `Unit`, `KeyPart`, `Namespace`, the PRF (SipHash-2-4-128, by M1), and the crate's lint
  gates.
- **S2:** pinned math, the laws (`Binomial` among them), `apportion`, `Permutation`, and the statistical helpers.
- **S3:** `Gen` and its combinators, with the key table and the key audit.
- **S4:** `Population` with `ScopeView`, `pick` and `pick_below`, `Ref` and `Ranges`; the golden tables and the
  fingerprint; the drill-shaped acceptance test, on one thread and on several; the Python regressions; a mutation
  run.

Decided under the library's gate: a blind solver, the main position and a discriminator, then three roles on the
synthesis (Software Design with Evolution, Verifiability, Performance). § Alternatives Considered keeps what each
refuted.

## Context

**The host's drills.** The host's load drills cannot serve as fitness functions, for two reasons.

- **Their data follows no law of the real fleet.**
  - Branches are of equal size (`services/policy/tests/it/cohort_drill.rs`, `Shape::branch`).
  - One fixed device context is used for every Device (`record_command`).
  - `bus_drill.rs` sets `os` to `30 + i % 5` and `oem` to `"samsung"`.
- **Their identities are ambient.**
  - `Uuid::new_v4` gives event and message ids in `bus_drill.rs`.
  - `fresh_tenant()` in the host's testkit gives each run's Tenant.

So two runs never offer the same load, and an experiment cannot vary one law while holding the rest.

**The Python faker the port starts from** has the same root:
- every choice comes from the process-global `random`;
- the FP factories pass state through a dynamically typed criteria dict;
- the distributors are mutable objects shared by every caller.

Reading it for this decision found these defects. The discriminator confirmed each against the code; paths are in
the ascetic-ddd-python repository, under `ascetic_ddd/faker/domain`.

| # | Defect | Where |
|---|---|---|
| 1 | `mean` never reaches the in-memory store, which always uses 50 | `distributors/m2o/weighted_distributor.py` stores `_mean` and calls `store.next_with_strategy` without it; `write_distributor.py` reads its own `_mean = 50`; `distributors/m2o/factory.py` builds the store without one |
| 2 | The retry after a stale value always creates a new one | `write_distributor.py`, `_relocate_stale_value` then `self.next`, which always raises `Cursor` |
| 3 | The realised reference law is not the stated one, and the tests' tolerances (×3, ±200 %) cannot fail on it | `weighted_distributor.py` (its own comment); `tests/distributors/m2o/test_weighted_distributor.py` |
| 4 | `RangeDistributorAdapter` stores at `len(values)` instead of the drawn slot, recurses without bound, and keeps its lock when `inner.create` raises; it has no tests | `distributors/o2m/range_distributor_adapter.py` |
| 5 | `DistributionDistributor` has no upper bound on heavy tails | `distributors/o2m/distribution_distributor.py` |
| 6 | `WeightedRangeDistributor` accepts negative weights, and returns a zero-weight first item when `r == 0.0` | `distributors/o2m/weighted_range_distributor.py` |
| 7 | Generators (see below) | `generators/generators.py`, `fp/factories/value_factory.py` |
| 8 | `NullableDistributor` ignores the specification: a pinned non-null value can come back `None` | `distributors/m2o/nullable_distributor.py` |
| 9 | The docstring percentages are wrong: skew 2 gives 45 %, not 60 %, and skew 3 gives 46 %, not 70 % | `distributors/m2o/skew_distributor.py`, `infrastructure/distributors/m2o/pg_skew_distributor.py` |
| 10 | The statistical tests are unseeded, have 40–300 % tolerances, and need PostgreSQL | `tests/distributors/m2o/*` |

The generator defects (row 7):
- `RangeGenerator` repeats values.
- `ValueFactory` passes position −1, which `RangeGenerator` asserts against, and which turns `SequenceGenerator` into
  the constant `lower − delta`.
- `CountableGenerator` embeds the pid.
- `HypothesisStrategyGenerator` uses `.example()`.
- `CallableGenerator` guesses its parameters from the arity, so a two-parameter generator receives the query as its
  position.
- `IterableGenerator` recurses for ever on an empty source.

**The owner's tentative placement of the writer** — "a Session replacing RestSession" — was put to the round as an
unproven premise. Decision 11 answers it.

**The owner's requirements, stated after the round (2026-11-20).**
- The scenario file is a Command Log: the source of every request of a load run, end-to-end REST tests included.
  The Python faker predates that idea, and it is not to be copied: the simpler design wins.
- Above all, generation must not fall over on millions and billions of records.
- Generation must run on many threads: the Command Log is regenerated after every change of the SUT's public
  interface.

They revised decisions 2, 6 and 10 the same day (§ Change Log).

## Decision

**Principle:** a generated value is a pure function of the key of its identity. Nothing in this stage's primitives
is shared or mutable, so any thread can compute any value, in any order, in memory that does not grow with the
members. Nothing the system answers enters it. State that a scenario needs — a fold owned by one scope — is stage
2's, and it is built on these functions, not inside them.

### 1. The crate

One new crate, `crates/faker` (`ascetic-ddd-faker`):
- a functional core: no IO, no async, no clock, no ambient randomness;
- `#![forbid(unsafe_code)]`;
- modules `key`, `math`, `law`, `gen`, `population`, `permutation`, and `testing` (statistical helpers, the key
  audit and the golden check — for its own tests and for the host's).

It departs from the README's rule of one module per Python module. The Python modules transliterate a design this
decision replaces, and the § Context table maps every ported construct.

### 2. The key

A `Key` is a path:

```
seed → kind → scope → index → label
```

- `Key::root(Seed)` starts the path.
- `child(&self, label: &str)` derives a key under a domain tag for labels, the label preceded by its length in
  LEB128: one byte under 128, no limit, nothing to refuse.
- `at(&self, index: u64)` derives a key under a domain tag for indices, with the index in little-endian bytes.
- `part(&self, &impl KeyPart)` derives a key for a typed scope.
- `KeyPart` is a canonical, injective byte encoding:
  - fixed-width little-endian integers;
  - length-prefixed strings;
  - tagged tuples and options, so `("ab", "c") ≠ ("a", "bc")` and `None ≠ Some("None")`.

  `Hash`, `Debug` and `type_name` are never used: none of them is stable.
- Internal derivations ("present", "count", the extra uniforms of a sampler) go under a tag of their own, so a host
  label with the same text derives a different key.

**The key is affine.** `Key` is `Clone`, not `Copy`, and the draws consume it: `unit(self)`, `bits(self)`,
`uuid(self, &Namespace)`. Drawing from a key and then passing it on is a use-after-move. A sampler that needs
several uniforms takes them as `at(i)` under the internal tag; there is no stream object.

**`Unit`** is `(k + 0.5) / 2^52` for a 52-bit `k`, so it lies strictly inside (0, 1): its extremes are 2⁻⁵³ and
1 − 2⁻⁵³. The 53-bit form rounds to 1.0 at `k = 2^53 − 1`; a heavy-tailed quantile there is infinite.

**The PRF is SipHash-2-4-128** (`siphasher` 1.0, already in the lock):
- keyed by the parent key, as two little-endian `u64`;
- fed explicit bytes only, never through `Hash::hash`;
- its 128-bit output is the child key.

It needs no secrecy here, only a fixed specification, outputs indistinguishable from independent ones, and no
collisions in practice: at 10¹⁰ derivations the chance of one is about 10⁻¹⁹. SHA-256 gives the same properties at
7–14 times the cost on a CPU without SHA instructions (M1). That cost is two and a half hours of one core for 10⁹
members, where SipHash needs a quarter of an hour. The choice is fixed before the golden tables are, because
changing it re-rolls every value.

**Run namespace.** Ids rendered for the wire are `H(namespace ‖ key)`. `Namespace` is not `Seed`. Two runs of
one Plan on one stand then use fresh ids and the same attributes: common random numbers hold across an A/B pair,
and the second run does not silently measure the inbox's duplicate path.

### 3. Pinned math

- Quantile functions go through `math`, which uses `libm` pinned exactly (`=0.2.16`, the version the host's lock
  already holds), without its `arch` feature. On x86_64 that feature replaces only `sqrt` and `fma`
  (`src/math/arch/x86.rs` of the crate, read), both correctly rounded, so a host whose lock enables it changes no
  value. What `libm` lacks is written in `math` from `+ − × ÷`: the regularized incomplete gamma and beta functions
  (series and Lentz's continued fractions, their common factors in Loader's form, decision 4), and the inverse
  normal — a rational start (Abramowitz and Stegun 26.2.23) refined by three Halley steps, on `Φ` from `erfc` in
  the tail and on `½ erf(x/√2) − (p − ½)` in the centre, where `Φ(x) − p` would be a difference of two numbers
  near ½. They reach `erf`'s precision with nothing but six well-known constants to get wrong.
- `f64::powf`, `ln`, `exp` and their relatives are never called directly: per the standard library's documentation
  their precision is unspecified, so a result may differ across platforms and Rust releases.
- Discrete choices compare the 52-bit integer `k` against integer thresholds fixed at construction. Construction uses
  only `+ − × ÷`, which are correctly rounded.
- The crate root carries `#![forbid(clippy::disallowed_methods, clippy::disallowed_types)]` (an inner `allow` is
  then E0453) and `#![deny(clippy::arithmetic_side_effects)]` (debug and release agree). `math` calls `libm`, which is
  not on the list, so it needs no exemption. The crate's `clippy.toml` lists:
  - float methods: `f64::{powf, powi, ln, log2, log10, exp, exp2, exp_m1, ln_1p, cbrt, hypot, sin, cos, tan}` and
    their `num_traits::Float` paths;
  - randomness and identity sources: `uuid::Uuid::{new_v4, now_v7}`, `rand::rng`, `OsRng`, `from_os_rng`,
    `getrandom`, `RandomState::new`;
  - process and environment: `std::process::id`, `std::thread::current`, `Instant::now`, `SystemTime::now`,
    `std::env::var`;
  - types: `HashMap` and `HashSet`.

### 4. A law is a value

```rust
pub trait Law: Display {
    type Value;
    fn quantile(&self, u: Unit) -> Self::Value;
    fn cdf(&self, x: &Self::Value) -> f64;
    fn cdf_below(&self, x: &Self::Value) -> f64; // P(X < x): what truncation needs at its lower bound
}
pub trait IndexLaw: Display {                    // over the members of a population of n
    fn index(&self, u: Unit, n: u64) -> u64;
    fn cdf(&self, i: u64, n: u64) -> f64;
}
```

A quantile reads the unit as `p` or as `1 − p` (`Unit::value`, `Unit::complement`), whichever its formula is
written in; both are exact, a unit being an odd multiple of 2⁻⁵³.

- Each law is validated at construction: negative, NaN or empty weights and out-of-range parameters are refused.
- Each has a canonical `Display`, for the scenario file's header.
- The laws are also gathered in two enums, discrete and continuous, so that the golden tables' coverage is an
  exhaustive `match`.

**Ported from Python, adapted:**
- the power-law skew index (`Skew`, an `IndexLaw`);
- weighted partitions (`Partitioned`, an `IndexLaw`): the part by weight and the member uniformly, both from one
  unit — the part by `Categorical`'s thresholds, the member by the unit's position inside the part's interval.
  Python bent each part towards its end to make up for a pool that grew while it was drawn from (defect 3); over a
  fixed size there is nothing to make up for, and the bend goes;
- categorical over integer thresholds, looked up by `partition_point`: O(log B), never Python's O(B) per draw;
- uniform integers, refused over more than 2⁵² of them: one unit cannot tell more apart (64 random bits are
  `Key::bits`);
- uniform, exponential, Pareto, log-normal and Weibull.

**The o2m mixtures** (a Poisson whose mean is itself drawn) take two uniforms, so they are not a `Law` but a
composition of two under two derived keys: slice S3's, with the combinators.

**Truncation is by the inverse CDF on [F(a), F(b)]**: one uniform, O(1). It is mandatory for a heavy tail.

**Poisson** (λ ≤ 10⁹) and **Binomial** (n ≤ 2⁵³) are inverted from one uniform by a walk:
- it starts at the mode, with the mass from `lgamma` and the distribution function from the incomplete gamma or
  beta function;
- below it, it steps down on `F` against `p`; above, it steps up on `S = 1 − F` against `1 − p`, so neither tail
  rounds to `1 − F`;
- a running sum keeps its absolute precision, about 10⁻¹⁶ of where it started, so each time it falls a million-fold
  it is computed again from the incomplete function, whose precision is relative. Without this, Poisson(30) at
  `1 − p = 5·10⁻¹⁶` gave 114 for the law's 84 (S2's own test, before the fix);
- a draw costs O(σ) steps and, in the far tails only, a few evaluations of the incomplete function;
- the naive walk from `e^(−λ)` would never end above λ ≈ 745, and the host's branch sizes reach λ = 10⁴.

Every mass, and the common factors of the incomplete gamma and beta functions, is computed in Loader's form:
Stirling's formula with its large terms cancelled exactly, leaving `x(ln(1 + t) − t)` and Stirling's remainder.
Written as `x ln λ − λ − ln Γ(x + 1)`, the terms are of the order of `x ln x` and their rounding is a relative
10⁻¹⁰ at 10⁵ and 10⁻⁶ at 10⁹ (S2's L1 measured the first, `gamma_p(10⁵ + 1, 10⁵)` off by 1.4·10⁻¹⁰); in Loader's
form the error grows as `ε√x`.

**New:**
- `Empirical::from_histogram`: the legacy system's own counts, the one law that answers "reflect real data";
- `Binomial`, inverted from one uniform by a walk from the mode like the Poisson: the splits of a multinomial, for
  a fixed total spread over chunks with its variance kept;
- `apportion(total, weights, key) -> Vec<u64>` by keyed systematic rounding, computed in `u128` (a total of 2·10¹⁰
  times a threshold of 2⁵² overflows `u64`):
  - `cᵢ = ⌊Qᵢ + u⌋ − ⌊Qᵢ₋₁ + u⌋`;
  - the sum is exactly `total`, each count is within 1 of `total·pᵢ`;
  - no item is favoured across scopes;
  - this is what a QAS's fixed totals need;
- `Permutation::new(n, key)`: a keyed Feistel bijection over [0, n) with cycle-walking, O(1) per element and no
  memory. It gives samples without replacement and an arrival order independent of identity.

**A count that sizes a list carries a cap.** A law drawn for the length of a list — a group's members, a
command's ids — must be truncated, and `replicate` refuses a count above its law's cap: a heavy tail must not decide
how much memory one value takes.

**Deferred:** gamma (no closed-form quantile and no consumer), and `estimate_skew` (`Empirical` is the measured
path).

### 5. Generation is a pure arrow from a key

```rust
pub trait Gen: Send + Sync {
    type Output;
    fn generate(&self, key: Key) -> Result<Self::Output, Refusal>;
}
```

No session, no criteria, no world: `&self` and a key, so one `Gen` serves every thread at once. A `Gen` cannot
create a member of a population, only name one, so it cannot re-enter one: self-referencing creation is refused by
construction.

| Combinator | Key passed on |
|---|---|
| `constant` | key unused |
| `draw(law)` | consumes the key |
| `from_fn(\|key\| …)` | the host derives `key.child("field")` per field of a struct literal — the main idiom |
| `map`, `try_map` (smart constructors, ROP) | same key |
| `and_then(label, f)` | the continuation under `label` |
| `optional(p, inner)` | the coin under an internal tag, `inner` under another |
| `replicate(count: impl Gen<Output = u64>, item)` | the count under an internal tag, item `i` under `at(i)`; a count above the count law's cap is refused |

`apportion` is offered as a `Gen` of counts.

Python's mapping:
- `ValueFactory` → `constant`, `draw`, `from_fn`;
- `StructureFactory` → `from_fn` with a struct literal;
- `ModeledFactory` → `map`, `try_map`;
- `Pipe` → `and_then`;
- `ReplicatedFactory` → `replicate`;
- `NullableDistributor` → `optional`.

The labelled tuple zip is not offered: with a positional `map` after it, two same-typed fields swap silently.

### 6. Populations hold identities, not values

```rust
pub struct Kind(&'static str);
pub struct Ref<K> { pub kind: Kind, pub scope: K, pub index: u64 }
```

A population owns three things: its `Kind`, its scope encoding and the `Gen` that creates its members. The creating
factory is bound to the population, never to a reference's call site, so a parent is the same whoever reaches it.

A member's key is `root.child(kind).part(&scope).at(index)`. The scope is in the path, so member `i` of two Tenants
is not one value. A Plan refuses two populations of one `Kind`. **Time never enters the path:** a member's identity
and attributes do not change when an arrival rate does, so an experiment on arrivals keeps common random numbers.

**`Population<K, G>`** declares its size per scope: a count, or a `Gen` of the count from the scope's key.
`scope(&self, &scope) -> ScopeView` evaluates the scope once — its size, its key, its law — so a unit of 10⁶ picks
does not redraw a Poisson size 10⁶ times. Everything it answers is a pure function with random access:
- `size()`.
- `pick(key, &law) -> Ref<K>` (many-to-one): index `j` drawn by the law over `0..size`.
- `pick_below(key, &law, upto) -> Option<Ref<K>>`: over the first `upto` positions of a declared order (the
  identity, or a `Permutation`); `None` when `upto` is 0. "A parent that exists at `t`" is
  `pick_below(.., A(t))` with `A` the parent's arrival count, and nesting "below the member's own index" is its
  special case.
- `value(&Ref<K>) -> Result<T, Refusal>` regenerates the member from its key; a refusal names the `Ref`.
  Populations are virtual: they hold no member.

**What a reference requires of order is the reference's, not the population's.** A step that needs its referent
created first — a Device managed into a group that must exist — is an edge, which stage 2 places and its validator
checks. A step that only names an identity — a group created with the ids of Devices that arrive hours later, the
host's import order (`cohort_drill.rs`: groups with their members before the wave) — requires nothing. Python's
"create when vacant" (`DistributedFactory`) needed a record of who had been reached; here a vacancy cannot arise,
so its wrong slot (defect 4) has nowhere to happen, and over a fixed size the realised law is the stated one
(defect 3).

**`Ranges`** (one-to-many, for few parents: groups, waves, time chunks): a prefix-sum table over the parents of
one scope, built from their child counts (drawn by a law, or by `apportion` for an exact total), 8 bytes a parent.
- The children of parent `p` are the indices `start(p)..end(p)` of the child population.
- `parent_of(child)` is a binary search, O(log parents).
- A group's members are a range, listed in O(1) memory, mapped through a `Permutation` when identity order must
  not follow the group.

Children of many parents (a Device's reports, its apps) take no table: they are scoped by the parent's `Ref`, so
`parent_of` is in the key path. Memory grows with the parents of each `Ranges`, and with nothing else.

**Python's growing pool, as a pure function.** Its "a new value with probability `1/mean`" is a prefix pick over
parents that grow with the children: before child `n`, `A(n) = 1 + ⌊n / mean + u⌋` parents exist (`u` from the
scope's key; parent 0 before child 0), parent `p ≥ 1` is emitted just before child `⌈(p − u) · mean⌉`, and child
`n` picks below `A(n)`. An open
stream keeps persistent parents, with no record and no total known in advance.

**The sequencer** is the member's index in its scope, `Ref::index`. The scope is typed, so `None` and `"None"`
cannot collide (Python's `str(scope)` let them).

**Selection by state** ("a Policy published before `t`") is not a primitive. It is a prefix pick over the order the
state is reached in, or a rejection against a pure predicate of the member's own schedule (cost 1 / the fraction
that qualifies), or stage 2's lag classes. A member's state never changes its `Ref`. Python's relocation is O(n) a
move: infeasible at 3M Devices moved four times.

**Nested groups** use a top-down tree (a depth law and a fan-out law, acyclic and bounded by construction), or
`pick_below` the member's own position. The tree is not built in this stage: the host's drills nest one level
(`all` over the branches). See § Owed.

### 7. What the generator never holds

The generator never sees a response. A value known only at run time is stage 2's: a typed hole, the producer table,
the captures with their use counts. Stage 1 guarantees what stage 2 needs:
- a stable identity per member, its `Ref`;
- `Key::uuid(&Namespace)`, the rendering of an id — **called at replay, not at generation**: the Command Log carries
  the `Ref` (or its key) in an id slot, and the executor renders it under the run's namespace for about 85 ns. One
  log then serves every run of an A/B pair, and a regeneration is owed only to a change of the SUT's interface;
- no state derived from anything outside the Plan.

### 8. Versions

There are two versions, never one.

- **The library's algorithm fingerprint** is the digest of the checked-in golden tables, in layers:
  - key vectors, computed independently of the implementation;
  - `k → Unit`;
  - each law's quantile at fixed `u`, boundaries and branch points included;
  - each combinator;
  - a reference Plan.

  Changing a value changes the table, so it changes the fingerprint: the bump and the change are one edit, and no
  discipline is asked for. A failing table names its first differing row per layer.
- **The host's scenario fingerprint** is computed by `testing::fingerprint` over the host's own generation. The
  header (stage 2) carries both, and a reader refuses a mismatch.

`testing::check_golden()` lets the host run the library's tables in its own build, in release. That build resolves
`libm` through the host's lock, which the library's CI never sees.

### 9. Not ported

- `faker/domain/query` and `faker/domain/specification`.
  - The FP factories use only `$eq` on foreign keys and `None` (`test_fp_infinite.py`, the `require` helpers).
  - `MergeConflict` and `$rel` belong to `aop` and `providers`.
  - If a predicate is ever needed beyond typed scopes and state selections, it is `crates/specification`'s `Expr`,
    not a second predicate tree.
- All of `faker/infrastructure`:
  - the PostgreSQL distributors draw with SQL `RANDOM()`;
  - the repositories become stage 2's emission, or the host's handlers;
  - the dump is the scenario file;
  - the session classes are capability bounds here.
- The generators that are not reproducible or are defective (§ Context, row 7).

### 10. Indexable, so stage 2 decomposes as it needs

Every primitive of this stage is a pure function of the Plan and a `Ref` or a key, with random access: `value`,
`pick`, `pick_below`, `Ranges::parent_of`, `Permutation` and its inverse, every law. So any decomposition stage 2
chooses — by scope, by positions of a declared order, by time cells across scopes — computes the same values on
any thread, in any order. The thread count changes the time, never a value (Par1).

The earlier claim that this stage defines the units of work, and that state means state shared by threads, was
wrong (§ Change Log, third entry). A Command Log ordered by arrival time needs units across scopes and
populations; and an aggregate written by members of another population — a group's member list under Devices
moving, a group's chain of writers — is a fold: state owned by one scope and folded in that scope's declared order,
scopes in parallel, the thread count still changing no byte. Both are stage 2's, under its own gate. What stage 2
must settle there, as found by this round:
- the unit: a time cell first, then scope and population inside it, then position;
- a two-way arrival map, `Ref ↔ (chunk, rank)`, by `Permutation`'s inverse and `parent_of` over per-scope chunk
  counts, and every member's emission position a pure function of its `Ref`, so that edges have known lengths;
- scope-local folds for cross-population writers;
- per-aggregate counters (a report's stream position, a group's sequence) from Device-centric periodic streams
  (a keyed phase and period: `F = ⌊(t − phase) / period⌋ + 1`) and lag classes, never from many-to-one picks;
- arrival counts per chunk by a law (Poisson for an open total, a multinomial by binomial splitting for a fixed
  one), not by `apportion`, which would take the variance out of the offered load;
- ids rendered at replay (decision 7); a writer bounded in bytes, not units; files cut at unit boundaries with
  per-unit digests (a Merkle fingerprint); step ids derived from identity; refusals collected per unit and failed
  once.

### 11. The writer is not a Session

This is recorded here because it shapes stage 1; stage 2 decides the writer itself.

Nothing in `Session` could do the writer's work:
- `Session` exposes only `atomic` (`crates/session/src/session.rs`).
- `HttpAccess::request` only times a future its caller built (`crates/session/src/rest/session.rs`).
- `RestObserver` sees a method and a URL (`crates/session/src/rest/observer.rs`).
- The drills call workflows in process and the bus's producer directly.
- In the host's `PgRestSession`, the REST half is the service's own outbound client to the KMS
  (`crates/pg/src/session.rs` in the host). Replacing it would record KMS calls, not the scenario.

What holds in the owner's intuition is the seam: one place where "what happens to a generated aggregate" is
swapped. That seam is a `Sink` the generation writes to: a file, a vector in tests, or a channel into an in-process
executor. `Session`, `HttpAccess` and `RestObserver` serve stage 3's handlers. The scheduler counts latency from
the scheduled start; the observer counts service time.

## Consequences

- Python's FP factories become a pure `Gen` algebra over populations that hold no state. A host writes its
  scenario as ordinary Rust over them: loops, struct literals, its own command types.
- The library's value is in `Key`, the laws, `apportion`, `Permutation` and the populations: what one line of host
  code cannot do right.
- Reproducibility is a property of the API, not of care:
  - no draw without a key;
  - no ambient source in the crate;
  - no shared mutable state that two tasks could interleave.

  It covers the offered load only. Measured latencies need tolerance bands from A/A runs on one machine.
- A renamed field label changes that field's values; a renamed `Kind` changes its population's. Both change the
  host's scenario fingerprint, which is the point.
- Time grows linearly with the members and divides by the threads; this stage's memory grows only with the parents
  of each `Ranges`. The derivations cost about 17 core-minutes for 10⁹ members on the host's laptop (M1, at about
  12 a member), and a `Permutation` adds about 9 a placed member (four Feistel rounds, about 2.2 cycle-walks):
  about 13 core-minutes more for an arrival order over 10⁹. Serialization and the disk are stage 2's cost.
- "An existing value, if one has been made yet" is a prefix pick, a pure function: the Python faker's growing pools
  keep their meaning and lose their order-dependence.

## Rule of Three

1. **A combinator reuses its key.** `optional` draws its coin from the key it then hands to `inner`: present values
   then fall in the bottom `p` of the law, while the presence fraction stays right. Decided by the affine key and
   the internal tags, and checked by the key audit and by conditional-law tests (C1, C2).
2. **Exact in the law, wrong at its edges.** A clamp instead of a truncation, `<` for `<=` at a threshold, or a typo
   in the inverse normal's far tail has a mass of 10⁻⁴ to 2⁻⁵². No sample at CI size sees it. Decided by
   deterministic quantile tests: boundary `k`, a stratified grid, independent reference values (L1–L4).
3. **Cross-machine drift, reblessed by reflex.** A direct `powf`, or a `libm` patch arriving through the host's
   lock, flips a `floor` and breaks a golden table that someone then regenerates. Decided by the forbidden lints,
   the exact pin and `check_golden` in the host's build. The fingerprint makes a reblessed table a version change
   that a reader refuses.
4. **Identity lost or duplicated.**
   - A population without its scope in the path makes three Tenants copies of one.
   - Two populations of one `Kind` correlate.
   - A parent keyed by its first consumer differs by who reached it first.

   Decided by decision 6, and checked by Pop1 and Pop2.
6. **The thread count leaks into the output.** A shared counter, or the order in which threads finish, decides a
   value. Two runs of one Plan on 2 and 12 threads then differ, and the regenerated Command Log no longer matches
   its fingerprint. Decided by decision 10: no primitive has state. Checked by Par1, over two decompositions.
7. **Memory that grows with the members after all.** A `Vec` of picks, a `Ranges` over 10⁸ Devices (800 MB), a
   group's member list materialised, or a list sized by an uncapped tail holds O(n). Decided by virtual
   populations, `Ranges` for few parents, children scoped by their parent's `Ref`, and capped count laws; checked
   by M6 and Cap1.
8. **A stage-1 primitive that forces a stage-2 order.** A definition here — of units, of time as a scope — would
   rule out the Command Log the host needs: Tenants arriving over a day, their Devices over hours, a log ordered by
   arrival. The first revision of decision 10 did exactly that (§ Change Log, third entry). Decided by keeping
   every primitive indexable and leaving decomposition and order to stage 2's gate.
5. **Parameters invented, not measured.** The mechanism is right and the load is still not the fleet's. The
   library cannot detect this: it needs histograms from the legacy system, checked into the host, and a host gate
   that every law of a Plan names its measured source (§ For the consumer).

## Alternatives Considered

- **A seeded ChaCha8 stream per named distributor** (the main position). Refuted by the discriminator.
  - Adding a distributor leaves the others' draws unchanged, but changing one entity's child count, or adding a
    consumer, shifts every later draw.
  - An experiment that varies one law reshuffles everything downstream.
  - No random access, so no parallel generation.

  The stream survives as the depth-one case of the key path, and not even as an object: extra uniforms are `at(i)`.
- **Async factories over a `Session`, so that one graph also runs live against REST** (the main position). Refuted:
  - generation does no IO once the PostgreSQL distributors go;
  - a session is a handle shared by its clones (`Session: Clone + Send + Sync + 'static`), so anything generation
    kept behind it would need a lock;
  - a live run waits on a network round trip per request — 15 million of them at launch scale, and the answers
    would make the output depend on the stand.

  Live seeding is "generate, then execute" through a `Sink`.
- **The writer as a `Session`**, the owner's assumption, or as a `ScenarioSession` with an `emit` capability (the
  main position). Decision 11 refutes the first. The second is a `Sink` under another name, carried by a handle
  that has nothing else to do.
- **Porting `query/*`, or using `crates/specification`'s `Expr` as the criteria now.**
  - The FP factories need neither.
  - `Expr` and `Value` derive no `Hash`/`Eq` (`crates/specification/src/domain/ast.rs`, `value.rs`), so they cannot
    key Python's per-specification index.
- **Populations that store their values** (the solver's `reference -> T`). Refuted by Performance: QAS-SCALE-0002's
  synthetic fleets of 1M Devices with inventories of about 4 KB take 8–12 GB as Rust values, more than either of the
  host's machines can spare. It also returns a value where stage 2 needs an identity.
- **A labelled tuple zip** (the solver). Refuted by Software Design: it swaps same-typed fields silently.
- **Stateful populations**: a record of members reached (`Closed::reach`), a growing pool with probability `1/mean`
  (`Growing`), a sequencer (the synthesis before the owner's requirements). Each recorded the order of calls in the
  population itself, which threads would share. Replaced by pure equivalents: a vacancy cannot arise; the growing
  pool is a prefix pick; the sequence is the index. **Not refuted:** state owned by one scope and folded in its
  declared order. It is not shared, it changes no byte with the thread count, and an aggregate written by members of
  another population needs it (decision 10). It is stage 2's.
- **SHA-256 as the PRF.** Its properties are SipHash's, at 7–14 times the cost without SHA instructions (M1).
- **Poisson by Knuth's loop, or by the normal approximation above 30** (Python). Knuth's loop consumes a variable
  number of uniforms; the approximation is not the law.
- **Hamilton's largest remainders for `apportion`.** Exact, but it favours the same items in every scope:
  +20 % on item 0 over 10⁴ Tenants of three equal branches.
- **Sampling from `rand_distr`, `proptest` or `quickcheck`.** They consume one stream in order and cannot be
  addressed by a key. `statrs` is used only as an independent oracle in tests.

## Executors

| Id | Slice | Asserts | Mutation it turns red |
|---|---|---|---|
| K1 | S1 | key vectors computed outside the implementation: `vectors/key_vectors.py`, SipHash-2-4-128 from its specification, checked first against the reference implementation's published vector | another tag; the length prefix dropped; `child` and `at` sharing a tag; an index's byte order |
| K2 | S1 | `KeyPart` injectivity, by property and by vectors: `("ab","c")` vs `("a","bc")`, `(1,23)` vs `(12,3)`, `None` vs `Some("None")`; an internal label vs the same host label | a separator instead of a length; untagged options |
| K3 | S1 | a `compile_fail` doctest: a key drawn from and then passed on; its twin, which derives first and draws last, compiles. Stable rustdoc does not check the error code (§ Measured), so the twin is what keeps the first from passing for another reason, such as a rename | `Key: Copy` |
| K4 | S1 | `Unit` at `k ∈ {0, 1, 2^52−2, 2^52−1}` lies strictly inside (0, 1) | the 53-bit form |
| R3 | S1 | the forbidden lints; measured once that clippy 1.100 resolves `f64::powf` in `disallowed-methods` and that `arithmetic_side_effects` fires | an inner `allow`; a direct `powf` |
| L1 | S2 | quantiles against mpmath at 50 digits (`vectors/law_vectors.py`, the command in its docstring), at both extreme units, the tails and the middle: continuous within 10⁻¹³ relative, discrete exactly, the incomplete gamma and beta functions within 10⁻¹² up to `a = 10⁵` | a coefficient typo; Halley's steps dropped; the walk without re-anchoring; Stirling's remainder dropped; `ln(1 + t) − t` from `ln(1 + t)`; the gamma factor from `lgamma` |
| L2 | S2 | discrete: `cdf(q(u)) ≥ u > cdf(q(u) − 1)`; continuous: `abs(cdf(q(u)) − u) ≤ c·ulp` | `q(u) + 1`; a cdf that returns 1.0 |
| L3 | S2 | a stratified grid `uᵢ = (i + ½)/N`, KS distance ≤ 1/N + c·ulp; no α, no seed | a clamp instead of a truncation |
| L4 | S2 | each law over a parameter grid with extremes (Poisson λ ∈ {0, 10⁻³, 1, 30, 700, 745, 800, 10⁴, 3·10⁴, 10⁶, 10⁹}; Binomial up to n = 2⁴⁰; skew from ¼ to 40 over n up to 2⁶⁴ − 1), at boundary `k`; a walk ends in ≤ 20σ + 20 steps and ≤ 3 exact evaluations of a tail (M4) | the naive walk from `e^(−λ)`; the index clamp removed; a running mass wrong by a factor (which the re-anchoring would hide as cost, not as a value) |
| L5 | S2 | monotonicity in each parameter | an exponent inverted consistently in `quantile` and `cdf` |
| L6 | S2 | `Display` golden strings; distinct laws, distinct strings | a parameter rounded or a cap omitted |
| L7 | S2 | mixture moments against the analytic variance; truncated laws against their own analytic mean | the untruncated mean as the oracle |
| L8 | S2 | categorical thresholds exact at boundaries ±1; a zero weight never drawn; bad weights refused (and counts summing to 2⁷⁶ or more); a draw over 10⁶ items in ≤ 21 comparisons, counted by a probe the search calls (M5) | `<` for `<=`; a linear scan |
| A1 | S2 | `apportion` against an exact u128 oracle; expectation exact over a `u` grid; no item favoured across scopes | remainders to the first items |
| P1 | S2 | `Permutation` is a bijection with an inverse for `n ∈ {0, 1, 2, 3, 4, 5, 7, 8, 63, 64, 65, 1000, 4099, 2¹⁶, 10⁵}`, and at `n = 2⁶⁴ − 1` both ways; over 20,000 keys the first value is uniform (χ², with the identity as the negative control) | cycle-walking dropped |
| T1 | S2 | the statistical helpers against reference values (χ² = 3.841 at df 1, p = 0.05; the KS critical value at n = 100); they refuse NaN statistics and zero expected counts; every statistical test carries a negative control, a mutant law that must be rejected; α from a family-wise budget | a helper returning p = 1 |
| C1 | S3 | per combinator, the keys it draws and passes on are pairwise distinct (the key audit, test-only and scoped to one member's tree: over a whole generation it would hold every derivation) | a combinator passing its own key on |
| C2 | S3 | conditional laws: `optional(p, draw(U))`'s present values pass KS against `U`; `replicate`'s items are independent of the count | the coin drawn from the inner key |
| M2 | S3 | derivations per member of the acceptance test's Device ≤ 15, and per member placed by a `Permutation` ≤ 12, counted by the audit | an accidental quadratic derivation; a Feistel round function heavier than one derivation |
| Cap1 | S3 | `replicate` refuses a count above its law's cap; a count law that sizes a list without a cap is refused at construction | an uncapped tail |
| Pop1 | S4 | `value(ref) == gen.generate(ref.key())` for every member; a pick lies in `0..size`; picks by two consumers of one member give one value | the consumer's key passed on; `size` off by one |
| Pop2 | S4 | two populations of one `Kind` refused; member `i` of two scopes are different values, and independent by χ² | the scope omitted from the path |
| Rng1 | S4 | `Ranges`: the children of the parents tile `0..total` without gap or overlap; `parent_of(c)` is the `p` with `c` in `start(p)..end(p)`, at every boundary; with `apportion` the total is exact | an off-by-one at `end(p)`; a linear search |
| Par1 | S4 | the acceptance test's values are identical on 1, 2 and 7 threads, and under two decompositions: by scope in index order, and by positions of a `Permutation` across scopes | a shared counter; a value depending on finish order |
| PP1 | S4 | `pick_below(.., upto)` lies in the first `upto` positions of its order, is `None` at 0, and its law over a fixed `upto` is the stated one; the growing pool: before child `n` exactly `A(n)` parents exist, parent `p` precedes the first child that can pick it, and uses per parent average `mean` (defect 1's regression) | `upto` off by one; the pool's parents emitted after their first pick |
| R1 | S4 | same Plan twice; members generated in permuted order; adding a field leaves the others unchanged | a counter-derived key; a `HashMap` |
| R2 | S4 | layered golden tables; the fingerprint is their digest; a failure names its layer and first differing row; coverage by an exhaustive `match` over the law enums | a law missing from the tables |
| R4 | S4 | `testing::check_golden()` passes in the host's build, in release | a `libm` patch arriving through the host's lock |
| D1 | S4 | a drill-shaped acceptance test, written in the library: Tenants; branches apportioned to an exact total and nested under `all`; Devices by a skewed law; Policies on drawn groups; contexts by categorical laws; a sample of 5,000 without replacement; groups created with the ids of Devices not yet placed. It compiles, the totals are exact, every foreign key resolves inside its scope. The time-major Command Log over it is stage 2's first acceptance test | — (the API's expressiveness: Software Design's acceptance gate) |
| PY | S4 | the Python regressions carried: the analytic skew values, uniformity at skew 1, monotonicity in skew; weighted-range bounds and validation; the null fraction within a binomial bound; uses per member at a mean of 10 (defect 1, now ⌈total / mean⌉ members); scope filtering | — |
| MUT | S4 | one `cargo mutants -p ascetic-ddd-faker` run; every survivor either killed by a new test or argued equivalent, in § Measured | — |

Not carried from Python: tests that encode a defect (`mean = 50` in the weighted distributor's tests; the triple
retry that hides defect 2), tests that cannot fail (per-partition weight within ×3; rank-sorted sums decreasing),
and ±15–20 % means on heavy tails.

## Measured

**M1, the PRF** (2026-11-20, the host's laptop: Intel Core i3-4005U at 1.7 GHz, no SHA-NI; `sha2` 0.10.9,
`siphasher` 1.0.3; release build; 5·10⁶ chained derivations of `key ‖ tag ‖ len ‖ label`, run twice):

| Label | SHA-256 | SipHash-2-4-128 | Ratio |
|---|---|---|---|
| 2 bytes | 714 ns | 73 ns | 9.8× |
| 8 bytes | 716 ns | 82 ns | 8.8× |
| 14–15 bytes | 714 ns | 94 ns | 7.6× |
| 22 bytes | 1,397 ns | 102 ns | 13.7× |

At 22 bytes SHA-256 takes two compressions: 32 + 2 + 22 bytes no longer fit one block with its padding. The
12-core box was not measured: it was busy, and the laptop bounds the cost. Chained derivations measure latency;
independent ones overlap, so these figures are upper bounds.

**S1** (2026-11-20, this laptop, Rust 1.100.0):
- **The lints (R3).** `f64::powf` in `Unit::value` is refused by `disallowed-methods`; an `#[allow]` of it on the
  function is E0453 under the crate's `forbid`. `arithmetic_side_effects` fires on a `usize` addition, and not on
  float arithmetic or a shift by a constant. Every path in `clippy.toml` resolves: clippy reports none unknown.
- **The error code of `compile_fail` is not checked on stable.** With `E0382` changed to `E0499` the doctest stays
  green. K3's twin carries the guard.
- **The key vectors (K1)** come from `crates/faker/vectors/key_vectors.py`, SipHash-2-4-128 written from its
  specification, which first reproduces the reference implementation's published vector (key `00..0f`, empty
  message, `a3817f04…c7550293`).
- **The mutations**, each applied alone, gates run (`cargo test`, or `cargo clippy` for the first two); every one red
  but the error-code change above:

| Mutation | Red |
|---|---|
| `f64::powf` in `Unit::value` | clippy: a disallowed method |
| that `powf` under `#[allow(clippy::disallowed_methods)]` | E0453 |
| `Key: Clone + Copy` | K3 |
| the label's length dropped | K1 |
| the label's tag equal to the index's | K1 |
| a 53-bit unit | K1, K4, the unit's high bits |
| the namespace not written into a rendered id | K1, ids under a namespace |
| the index big-endian | K1 |
| `None` written as nothing | K2's property |
| `Some` written without its tag | K2's vectors and property |
| a string without its length | K1, K2's vectors and property |
| a sequence without its length | K2's vectors and property |
| LEB128 without its continuation bit | K1, LEB128 |
| `compile_fail,E0382` as `E0499` | — (green: not checked on stable) |

**S2** (2026-11-20, this laptop):
- **What L1 found before it passed.** The incomplete gamma at `a = 10⁵` was off by 1.4·10⁻¹⁰ (its common factor
  from `lgamma`); the inverse normal at `p = ½ + 2⁻⁵³` by 0.7 % relative (`Φ(x) − p` near the centre). Both fixed
  (decisions 3 and 4). The walk's far upper tail, before re-anchoring, gave Poisson(30) 114 for the law's 84 at
  `1 − p = 5·10⁻¹⁶`.
- **What the mutations found.** A running mass wrong by a factor (the binomial's odds inverted) stayed green: the
  re-anchoring corrected the value, one exact evaluation a step. M4 now counts exact evaluations, at most three a
  draw. A partitioned member drawn from the same unit as its part stayed green: the test summed parts only. It now
  checks the members of a part alike.
- **The mutations**, each applied alone, `cargo test -p ascetic-ddd-faker`; every one red:

| Mutation | Red |
|---|---|
| truncation as a clamp | L2, L3 |
| categorical `<=` as `<` | L8 |
| the skew index without its clamp | L4 |
| a partitioned member from the part's own unit | the partitioned law |
| `apportion` without its unit | A1, twice |
| the permutation without cycle-walking | P1 |
| uniform integers by modulo | L2, uniform integers |
| `complement` as `value` | L1, L2, L3 |
| the inverse normal without Halley's steps | L1, L2, L3 |
| the inverse normal without its central form | L1 |
| the walk without re-anchoring | L1 |
| the binomial's odds inverted upward | M4 (exact evaluations) |
| Stirling's remainder dropped | L1, L2 |
| `ln(1 + t) − t` from `ln(1 + t)` | L1 |
| the gamma factor from `lgamma` | L1 |
| the chi-square p-value from the lower tail | P1's χ² |
| the categorical searched linearly | L8 (comparisons) |

To be filled per slice:
- **M6, scale:** 10⁹ members of the acceptance test's Devices, values computed and discarded, on 1 thread and on
  every thread of the machine: time, and peak RSS independent of the member count (compared against 10⁷). Hand-run,
  not on CI. It measures this stage's primitives only; the writer's bytes, window and disk are stage 2's (M7 there).

## For the consumer

The host pins the library by git revision. At the pin that carries slice S4:

1. **Write the drills' data as a Plan over this crate.** The cohort and bus drills' shapes become laws: branch sizes,
   device contexts, policy counts. `fresh_tenant()` and `Uuid::new_v4` give way to rendered ids under a
   `Namespace`.
2. **Bring measured laws.** Histograms of the legacy fleet (devices per group, OS and OEM mix, policies per
   Tenant), checked into the host, become `Empirical` laws. A host gate fails a Plan whose law names no measured
   source (Rule of Three, item 5). The data is the owner's to obtain.
3. **Call `testing::check_golden()` in a host test run in release.** The host's lock resolves `libm`.
4. Nothing of the host's sessions changes. The scenario file and its writer are stage 2.

## Owed

- **The Command Log's engine** (decision 10's list): stage 2's ADR, under the full gate — blind solver,
  discriminator, roles. Its first acceptance test is the time-major Plan: Tenants over a day, Devices over hours, a
  log ordered by arrival, on 1 and 7 threads.
- **To the host's BC-POLICY:** a Device moving between groups is a full-list merge-patch of each group
  (`DeviceGroupPatch.members` in `policy-v1.yaml`, with no `If-Match`). At the launch shape's moves the patches
  alone are terabytes, and two patches of one group run side by side lose an update. A delta-membership command,
  or `If-Match` on groups, is the owner's to decide; this library only records it.
- **A tree law for nested groups** (decision 6): depth and fan-out laws, acyclic and bounded by the host's
  `MAX_NESTING_DEPTH`. Owed when a scenario nests deeper than one level.
- **Gamma, `estimate_skew`:** when a consumer needs them.
- **A second architecture in CI** (macOS or arm), running the golden tables. Only if a scenario must regenerate
  bit for bit off x86_64 Linux. A musl target would not discriminate, because `libm` is musl's port.

## References

ADR-0004 (the scope receives its session by value), ADR-0011 and ADR-0012 (the specification crate),
`ascetic_ddd/faker` in the ascetic-ddd-python repository.

## Change Log

- 2026-11-20: Proposed.
  - **The blind solver** derived every draw from an identity key, removed the session and the criteria, and put
    the writer behind a `Sink`. **The main position** kept a stream per distributor, async factories over a
    session, and a session as the writer.
  - **The discriminator** refuted the main position on all three. It confirmed the ten Python defects the solver
    listed, with two corrections, and added an eleventh: `SequenceGenerator` through `ValueFactory`. It found
    `Unit` reaching 1.0 in the 53-bit form, and the scope missing from the key path.
  - **The roles** added:
    - Software Design: identities as `Ref`s returned by populations that own their creating `Gen`; the key table
      and the affine key; the run namespace; the canonical `KeyPart`; two versions; the identity scope that never
      changes; the drill-shaped acceptance gate.
    - Performance: virtual populations; Poisson from the mode; the PRF decided by measurement; `Permutation`;
      `partition_point`; truncation by inverse CDF; the narrowed parallel claim.
    - Verifiability: deterministic quantile tests at boundaries and on a grid; independent reference values; the
      fingerprint as the version; `check_golden` in the host's build; the complete lint list under `forbid`;
      `apportion`'s keyed rounding; negative controls; error codes on `compile_fail`; a mutation run.
- 2026-11-20: Revised by the owner's requirements (§ Context): a Command Log of millions to billions of records,
  regenerated on many threads; simpler beats faithful.
  - Decision 6: populations hold no state. `reach`, `Growing` and the sequencer go; members are made before they
    are named; `Ranges` gives one-to-many in 8 bytes a parent.
  - Decision 10: parallel by construction, in units fixed by the Plan; the thread count changes no byte.
  - Decision 2: the PRF is SipHash-2-4-128, by M1 on the laptop.
  - Executors: Pop3 goes with the state it checked; Rng1, Par1 and M6 come.
- 2026-11-20: Narrowed by a role round on the revision (Performance at scale, Distributed Systems).
  - **The premise refuted:** "state means state shared by threads". State owned by one scope and folded in its
    declared order is not shared and changes no byte; an aggregate written by another population's members (a
    group's list under moves) needs it. Decision 10 no longer claims units; it states what makes any decomposition
    possible, and lists what stage 2 must settle.
  - **Definitions that ruled out the host's log, removed:** units of one population in one scope; a soak cut into
    time chunks as scopes (time entered identity and broke common random numbers); "a member no later than any step
    that names it" for every reference (the host's import names Devices before they arrive).
  - **Added:** `pick_below`, the pure form of the growing pool and of "a parent that exists at `t`"; `ScopeView`;
    `Binomial`; `apportion` in `u128`; capped list counts; children of many parents scoped by the parent's `Ref`;
    ids rendered at replay, so one log serves every run.
  - **Moved to stage 2:** time-major units, the arrival map, folds, periodic streams and lag classes, arrival counts
    by law, the byte-bounded writer, unit-aligned files and their Merkle fingerprint, identity-derived step ids,
    refusals collected per unit.
  - **For the host:** the device-group contract (§ Owed).
