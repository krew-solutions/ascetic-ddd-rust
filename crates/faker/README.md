# ascetic-ddd-faker

Generated data for load tests. A port of ascetic-ddd-python's `faker/domain/fp`,
adapted to Rust rather than transliterated (ADR-0045).

**Status:** stage 1 of ADR-0045 built — the key, the laws, the generators, the
populations. Stage 2 (the Command Log and its writer) and stage 3 (the
scheduler) are their own ADRs.

## Principle

A generated value is a pure function of the key of its identity. Nothing in this
crate's primitives is shared or mutable, so any thread can compute any value,
in any order, and the thread count changes the time, never a byte.

## The key

A key is a path: seed → kind → scope → index → label. The operating system of
Device 12345 of Tenant 2 is drawn from

```
# use ascetic_ddd_faker::{Key, Seed};
let tenant = 2u64;
let os = Key::root(Seed(42))
    .child("device")
    .part(&tenant)
    .at(12345)
    .child("os");
let u = os.unit();   // strictly inside (0, 1)
# assert!(u.value() > 0.0 && u.value() < 1.0);
```

and from nothing else. Consequences:

- **Adding a field, an entity or a law leaves every other value unchanged.**
  Two runs that differ in one law differ only where that law reaches, which is
  what an experiment needs (common random numbers).
- **Any member can be computed alone**, so generation splits across threads
  without sharing anything.

Each step derives a new key with SipHash-2-4-128, keyed by the parent key, over
a domain tag and the step's canonical bytes (`KeyPart`).

**A key is drawn from once.** `Key` is neither `Copy` nor `Clone`, and the draws
— `unit`, `bits`, `uuid` — take it by value. Drawing a coin from a key and then
handing the same key to the value behind the coin does not compile, so the two
cannot be correlated by accident.

**Ids are rendered under a `Namespace`**, kept apart from the `Seed`. Two runs
of one Plan on one stand get fresh ids and the same attributes.

## Laws

A law turns one unit into one value by its quantile function, so one draw is
one key, whatever the value:

```
# use ascetic_ddd_faker::law::{Law, Poisson, Skew, IndexLaw};
# use ascetic_ddd_faker::{Key, Seed};
let branch = Key::root(Seed(42)).child("branch").at(17);
let devices = Poisson::new(10_000.0).unwrap().quantile(branch.child("devices").unit());
let group = Skew::new(2.0).unwrap().index(branch.child("group").unit(), 100);
# assert!(group < 100 && devices > 9_000);
```

- **Values:** uniform, exponential, Pareto, log-normal, Weibull, and any of
  them truncated — rescaled into the interval, not clamped onto its bounds.
- **Counts:** uniform integers, categorical (integer thresholds: no floating
  point in the choice), empirical histograms, Poisson and binomial (walked from
  the mode, exact in both tails).
- **Indices of a population:** uniform, a power-law skew, weighted parts.
- **`apportion`** splits a fixed total by weights exactly, with no item
  favoured across scopes; **`Permutation`** orders `0..n` by a key, both ways,
  with no table.

Every transcendental function goes through `math`, over `libm` pinned
exactly: the standard library leaves the precision of `f64::ln` and its
relatives unspecified, and a generated value must not depend on the platform.

## Generators

A `Gen` is a pure arrow from a key to a value: `&self` and a key, so one
generator serves every thread at once. A struct is a function of its key, a
label per field:

```
use ascetic_ddd_faker::generator::{Gen, GenExt, draw, from_fn, replicate};
use ascetic_ddd_faker::law::{Categorical, Poisson, UniformInt};
use ascetic_ddd_faker::{Key, Seed};

struct Device { os: u64, oem: Option<u64>, apps: Vec<u64> }

let os = draw(Categorical::from_weights(&[0.6, 0.3, 0.1]).unwrap());
let oem = draw(Categorical::from_counts(&[5, 3, 2]).unwrap()).optional(0.8).unwrap();
let apps = replicate(draw(Poisson::new(20.0).unwrap()), draw(UniformInt::new(0, 999).unwrap()), 200);
let device = from_fn(move |key: Key| {
    Ok(Device {
        os: os.generate(key.child("os"))?,
        oem: oem.generate(key.child("oem"))?,
        apps: apps.generate(key.child("apps"))?,
    })
});
let d = device.generate(Key::root(Seed(42)).child("device").part(&2u64).at(12345)).unwrap();
# assert!(d.os < 3 && d.apps.len() <= 200 && d.oem.map_or(true, |o| o < 3));
```

Each combinator states the keys it draws from and the keys it passes on, and
the two never meet: wrapping a generator in `optional` or `map` leaves its
values unchanged. The `audit` feature lets a test check it of its own
generators: `testing::audit` reports a member's derivations, draws, and draws
from a key already drawn from.

## Populations

A population holds identities, not values: a member is a `Ref` — kind, scope,
index — and its value is regenerated from its key wherever it is needed. A
population of 10⁹ holds nothing, and the parent a reference names is the same
whoever reaches it.

```
use ascetic_ddd_faker::generator::{constant, draw};
use ascetic_ddd_faker::law::{Categorical, Skew};
use ascetic_ddd_faker::{Key, Plan, Seed};

let mut plan = Plan::new(Seed(42));
let os = Categorical::from_weights(&[0.6, 0.3, 0.1]).unwrap();
let devices = plan.population::<u64, _, _>("device", constant(1_000_000u64), draw(os)).unwrap();
let groups = plan.population::<u64, _, _>("group", constant(100u64), constant(())).unwrap();

let tenant = 2;
let (devices, groups) = (devices.scope(tenant).unwrap(), groups.scope(tenant).unwrap());
let os_of_7 = devices.value(7).unwrap();
let group_of_7 = groups.pick(devices.key_of(7).child("group"), &Skew::new(2.0).unwrap()).unwrap();
# assert!(os_of_7 < 3 && group_of_7.index < 100 && group_of_7.scope == tenant);
```

- `pick_below` picks among the first `upto` of an order: "a parent that exists
  by now", and Python's growing pool as a pure function (`Pool`).
- `Ranges` (`Table`, or an `Apportionment` with no table) is one-to-many: a
  parent's children are a range, its members known before any of them exists.
- `permutation()` is a scope's own order, independent of identity: arrivals,
  samples without replacement.

Everything is a pure function with random access, so a Plan's output can be cut
into units and computed on any number of threads with the same bytes.

## Versions

`ALGORITHM` is the digest of the crate's golden tables (`golden/tables.txt`):
what this version generates, row by row. A changed value changes the tables and
the version in one edit. A host calls `testing::check_golden()` in its own
build, in release, where its lock resolves the pinned dependencies.
