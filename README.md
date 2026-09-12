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
| [`ascetic-ddd-inbox`](crates/inbox) | `ascetic_ddd/inbox` | Transactional Inbox on PostgreSQL |
| [`ascetic-ddd-outbox`](crates/outbox) | `ascetic_ddd/outbox` | Transactional Outbox on PostgreSQL |
| [`ascetic-ddd-rop`](crates/rop) | `trading.rop` (OCaml) | Railway-oriented programming: `Result` with accumulating, never-empty errors |
| [`ascetic-ddd-saga`](crates/saga) | `ascetic_ddd/saga` | Saga pattern (routing slip) for distributed transactions |
| [`ascetic-ddd-session`](crates/session) | `ascetic_ddd/session` | Unit of Work: session scopes, savepoints, identity map, REST and composite sessions, PostgreSQL adapter *(in progress)* |

## Architecture decisions

- [ADR-0001](docs/src/adr/0001-session-is-a-cloneable-handle.md) — a session is a
  cloneable handle *(the borrow is superseded by ADR-0004)*.
- [ADR-0002](docs/src/adr/0002-serialize-and-encrypt-before-the-outbox.md) — where a message is
  serialized and encrypted relative to the outbox.
- [ADR-0003](docs/src/adr/0003-the-outbox-and-the-inbox-as-channels-of-the-bus.md) — the
  outbox and the inbox as channels of the bus, joined by bridges.
- [ADR-0004](docs/src/adr/0004-the-scope-receives-its-session-by-value.md) — the scope
  receives its session by value, and the session promises `Send`.

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
