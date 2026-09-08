# ascetic-ddd-rop

Railway-oriented programming: `Result` with accumulating, never-empty errors.
A port of `trading.rop` (OCaml), itself a thin layer over the standard
`result` after Scott Wlaschin's *Railway oriented programming*.

```rust
use ascetic_ddd_rop::{Rop, all, fail, succeed};

// Independent fields: every failure is reported.
let order = all!(symbol(""), side("hold"), quantity(10));
assert_eq!(order.unwrap_err().into_vec(), [Invalid::Symbol, Invalid::Side]);

// Dependent steps: `?` stops at the first failure.
fn place(s: &str, d: &str, q: i64) -> Rop<Order, Invalid> {
    let (symbol, side, quantity) = all!(symbol(s), side(d), quantity(q))?;
    succeed(Order { symbol, side, quantity })
}
```

## Design

`Rop<T, E>` is an alias of `Result<T, Errors<E>>`, so `?`, `map`, `and_then`
and every other `Result` method apply unchanged. `Errors<E>` is a list of
errors that cannot be empty: it is built from one error, grows by errors, and
merges with another `Errors`. The OCaml source keeps the same invariant in a
comment; here it is the type.

Wlaschin's toolkit maps as follows. `bind` and `>>=` are `and_then` and `?`;
`map` is `map`; `>=>` is `compose`; `either`, `switch`, `tee`, `plus` and
`&&&` (`and_also`) keep their names; `doubleMap` and OCaml's `apply` are
methods of `RopExt`. OCaml's `let+ … and+ …` — several independent results,
all errors reported — is the `all!` macro, for two to eight results.

Two things do not cross over. `tryCatch` has no counterpart: Rust has no
exceptions, and a panic is not an error. And a separate `Success | Failure`
type, as in Wlaschin's F#, was rejected for the same reason the OCaml source
rejected it: it would forfeit `?` and the standard library around `Result`.

The crate has no dependencies and no `std`, so it is fit for a domain crate.

## Testing

```bash
cargo test -p ascetic-ddd-rop
```
