# ascetic-ddd-faker

Generated data for load tests. A port of ascetic-ddd-python's `faker/domain/fp`,
adapted to Rust rather than transliterated (ADR-0045).

**Status: in progress.** Done: the key (slice S1). To come: the laws, the
generators, the populations.

## Principle

A generated value is a pure function of the key of its identity. Nothing in
generation is shared or mutable, so any thread can compute any part of the
output, and the thread count changes the time, never a byte.

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
