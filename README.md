# ascetic-ddd-rust

DDD Toolkit and Seedwork for Rust — the Rust port of
[ascetic-ddd-python](https://github.com/krew-solutions/ascetic-ddd-python)
and [ascetic-ddd-go](https://github.com/krew-solutions/ascetic-ddd-go).

## Repository layout

A Cargo workspace with **one crate per Python package**:

```
ascetic-ddd-rust/
├── Cargo.toml            # [workspace] members = ["crates/*"], shared metadata
└── crates/
    └── saga/             # ascetic-ddd-saga  <- ascetic_ddd/saga
        ├── src/          # library modules, one per Python module
        ├── examples/     # runnable examples (cargo run --example ...)
        └── tests/        # integration tests, one per Python test module
```

### Why one crate per package rather than one crate for everything

Python ships `ascetic_ddd` as a single distribution because its subpackages
cost nothing until imported, and their third-party requirements are resolved
lazily at runtime. Neither holds in Rust:

* **A crate is the unit of compilation and of dependency resolution.** One crate
  for everything would force every user of `saga` to build (and audit, and
  license-check) the dependencies of `kms`, `faker` and the Postgres-backed
  packages. Cargo features could hide that, but they turn into a combinatorial
  test matrix and are easy to get wrong.
* **A crate is the unit of versioning and publication.** Separate crates can
  reach 1.0 at their own pace; a monolith forces the least stable package to set
  the version of everything else.
* **A crate boundary cannot be cyclic.** That is a liability in Python (where
  cycles between subpackages are possible and occasionally accidental) and an
  asset here: the dependency direction between packages is checked by the
  compiler, which is exactly what DDD layering wants.

The workspace keeps the ergonomics that a single distribution provides:
one `cargo test` / `cargo clippy` for the whole repository, a shared
`Cargo.lock`, and shared metadata in `[workspace.package]`.

If a facade is wanted later — a single `ascetic-ddd` crate re-exporting the
others behind features, so that `ascetic_ddd::saga` keeps working as one import
— it can be added as one more member without moving any code.

## Crates

| Crate | Python package | Description |
| --- | --- | --- |
| [`ascetic-ddd-bus`](crates/bus) | `trading.bus` (OCaml) | Scheme-dispatched message bus with in-memory and Kafka adapters |
| [`ascetic-ddd-dek`](crates/dek) | `seedwork/infrastructure/repository/dek_store` | Data-encryption keys per resource, versioned, wrapped by the tenant's KEK; the ciphers a codec seals with |
| [`ascetic-ddd-event-store`](crates/event-store) | `seedwork/infrastructure/repository/event_store` | Event store on PostgreSQL: streams appended with an expected position, payloads sealed with the stream's DEK |
| [`ascetic-ddd-inbox`](crates/inbox) | `ascetic_ddd/inbox` | Transactional Inbox on PostgreSQL |
| [`ascetic-ddd-kms`](crates/kms) | `ascetic_ddd/kms` | Key management for a tenant's keys: a KEK per tenant, versioned and rotated, and signatures and MACs under the tenant's keys, in PostgreSQL under a master key or in Vault Transit |
| [`ascetic-ddd-outbox`](crates/outbox) | `ascetic_ddd/outbox` | Transactional Outbox on PostgreSQL |
| [`ascetic-ddd-rop`](crates/rop) | `trading.rop` (OCaml) | Railway-oriented programming: `Result` with accumulating, never-empty errors |
| [`ascetic-ddd-saga`](crates/saga) | `ascetic_ddd/saga` | Saga pattern (routing slip) for distributed transactions |
| [`ascetic-ddd-specification`](crates/specification) | `ascetic_ddd/specification`, Go `cmd/specgen` | Specification pattern: a predicate as a tree, built by hand, by typed terms, from a JSONPath template or from a Rust function; evaluated in memory or compiled to PostgreSQL |
| [`ascetic-ddd-specification-macros`](crates/specification-macros) | `specification/domain/lambda_filter`, Go `cmd/specgen` | `#[specification]`: a predicate function and its tree from one source |
| [`ascetic-ddd-session`](crates/session) | `ascetic_ddd/session` | Unit of Work: session scopes, savepoints, identity map, REST and composite sessions, PostgreSQL adapter *(in progress)* |
| [`ascetic-ddd-trace`](crates/trace) | — | Recorders of what the outbox and the inbox report, for trace validation against the TLA+ models |

## Architecture decisions

- [ADR-0001](docs/src/adr/0001-session-is-a-cloneable-handle.md) — a session is a
  cloneable handle *(the borrow is superseded by ADR-0004)*.
- [ADR-0002](docs/src/adr/0002-serialize-and-encrypt-before-the-outbox.md) — where a message is
  serialized and encrypted relative to the outbox.
- [ADR-0003](docs/src/adr/0003-the-outbox-and-the-inbox-as-channels-of-the-bus.md) — the
  outbox and the inbox as channels of the bus, joined by bridges.
- [ADR-0004](docs/src/adr/0004-the-scope-receives-its-session-by-value.md) — the scope
  receives its session by value, and the session promises `Send`.
- [ADR-0005](docs/src/adr/0005-a-failed-message-holds-its-partition-and-is-parked-after-its-attempts.md) — a
  failed message holds its partition, and is parked after its attempts.
- [ADR-0006](docs/src/adr/0006-a-message-ahead-of-its-dependency-waits-out-of-the-queue.md) — a
  message ahead of its dependency waits out of the queue, and is woken by the
  dependency's mark *(the wait's transaction is amended by ADR-0009)*.
- [ADR-0007](docs/src/adr/0007-a-dispatcher-owns-a-slot-by-locking-its-position.md) — a
  dispatcher owns a slot by locking its position, and has no identity.
- [ADR-0008](docs/src/adr/0008-the-inbox-takes-a-slot-by-locking-its-row.md) — the
  inbox takes a slot by locking its row, and its dispatcher has no identity
  *(the head's read is amended by ADR-0009)*.
- [ADR-0009](docs/src/adr/0009-a-wait-is-a-transaction-of-its-own-under-a-lock-on-the-dependency.md) — a
  wait is a transaction of its own, serialized with the dependency's mark by a
  lock on its identity.
- [ADR-0010](docs/src/adr/0010-which-failures-stop-the-loops-which-wait-and-which-park-at-once.md) — which
  failures stop the loops, which wait, and which park at once.
- [ADR-0011](docs/src/adr/0011-what-a-specification-must-do.md) — what a
  specification must do.
- [ADR-0012](docs/src/adr/0012-a-specification-is-a-tree-over-its-values-read-by-functions.md) — a
  specification is a tree over its values, read by functions.
- [ADR-0013](docs/src/adr/0013-a-null-is-tested-not-compared-where-null-is-a-value.md) — a
  null is tested, not compared, where null is a value.
- [ADR-0014](docs/src/adr/0014-a-name-is-the-storages-quoted-as-it-is-and-mapped-in-one-place.md) — a
  name is the storage's, quoted as it is, and mapped in one place.
- [ADR-0015](docs/src/adr/0015-a-string-constant-is-read-as-the-kind-of-the-member-beside-it.md) — a
  string constant is read as the kind of the member beside it.
- [ADR-0016](docs/src/adr/0016-a-tenants-keys-are-one-per-use.md) — a tenant's
  keys are one per use: the KEK, a signing key per algorithm, a MAC key per
  purpose.
- [ADR-0017](docs/src/adr/0017-the-bus-key-is-derived-per-channel-on-a-mount-of-its-own.md) —
  the bus's key is one per tenant on a mount of its own, derived per channel;
  the adapter makes no key, custody is a port beside, and a refusal is read
  once.
- [ADR-0018](docs/src/adr/0018-the-intake-names-the-row-and-the-stage-is-chosen-by-it.md) —
  the intake names the row, and at an inbox that receives several channels
  the stage is chosen by the row; one subscriber, no filter by address, and
  why.
- [ADR-0019](docs/src/adr/0019-a-signature-at-a-version-held-elsewhere-makes-no-key-and-the-public-keys-are-a-port.md) —
  a signature whose version something outside the KMS already holds is made
  at that version and makes no key; the public keys of the versions that
  verify are a port of their own, derived where the adapter holds the
  private halves *(Proposed)*.
- [ADR-0020](docs/src/adr/0020-custody-of-a-tenants-keys-is-a-port-per-kind-and-nothing-that-uses-a-key-makes-one.md) —
  custody of a tenant's keys is a port per kind, beside its use; under
  managed keys nothing that uses a key makes one; a rotation never makes the
  first version, and a retirement keeps the latest and never lowers the
  floor *(Proposed)*.

## Specifications

TLA+ models of the outbox protocol, checked with TLC, live in
[`verify/tla`](verify/tla/README.md): what is proved, on which instance, and
what is left to tests.

## Development

```bash
cargo test           # unit, integration and documentation tests
cargo clippy --all-targets
cargo fmt --all
cargo doc --open
```

## License

MIT — see [LICENSE](LICENSE).
