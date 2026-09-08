//! Railway-oriented programming: `Result` with accumulating, never-empty errors.
//!
//! Scott Wlaschin's two-track pattern, as a thin layer over [`core::result::Result`].
//! The success track is Rust's own `Ok`; the failure track carries [`Errors`],
//! a list that is never empty, so that independent validations report every
//! problem at once rather than the first one.
//!
//! ```
//! use ascetic_ddd_rop::{Errors, Rop, all, fail, succeed};
//!
//! #[derive(Debug, PartialEq)]
//! enum Invalid { Symbol, Side, Quantity }
//!
//! fn symbol(s: &str) -> Rop<String, Invalid> {
//!     if s.is_empty() { fail(Invalid::Symbol) } else { succeed(s.to_uppercase()) }
//! }
//! fn side(s: &str) -> Rop<bool, Invalid> {
//!     match s { "buy" => succeed(true), "sell" => succeed(false), _ => fail(Invalid::Side) }
//! }
//! fn quantity(q: i64) -> Rop<u32, Invalid> {
//!     u32::try_from(q).ok().filter(|q| *q > 0).ok_or_else(|| Errors::new(Invalid::Quantity))
//! }
//!
//! // Independent fields: every failure is reported.
//! let order = all!(symbol(""), side("hold"), quantity(10));
//! assert_eq!(order.unwrap_err().into_vec(), [Invalid::Symbol, Invalid::Side]);
//!
//! // Dependent steps: `?` stops at the first failure.
//! fn place(s: &str, d: &str, q: i64) -> Rop<(String, bool, u32), Invalid> {
//!     let (symbol, side, quantity) = all!(symbol(s), side(d), quantity(q))?;
//!     succeed((symbol, side, quantity))
//! }
//! assert!(place("sber", "buy", 10).is_ok());
//! ```
//!
//! # What maps to what
//!
//! | Wlaschin | OCaml `Rop` | here |
//! |---|---|---|
//! | `Success` / `Failure` | `Ok` / `Error` | `Ok` / `Err` — [`Rop`] is an alias of `Result` |
//! | `succeed`, `fail` | same | [`succeed`], [`fail`] |
//! | `bind`, `>>=` | `bind`, `let*` | `Result::and_then`, `?` |
//! | `map` | `map`, `let+` | `Result::map` |
//! | `>=>` | `>=>` | [`compose`] |
//! | `either` | same | [`either`] |
//! | `switch`, `tee` | same | [`switch`], [`tee`] |
//! | `doubleMap` | `double_map` | [`RopExt::double_map`] |
//! | `plus`, `&&&` | `plus`, `&&&` | [`plus`], [`and_also`] |
//! | — | `both`, `and+` | [`both`], [`all!`] |
//! | — | `apply`, `<*>` | [`RopExt::apply`] |
//! | — | `of_result` | `?`, or [`Errors::from`] |
//! | `tryCatch` | `try_catch` | none: Rust has no exceptions, and a panic is not an error |
//!
//! # Deviations from the OCaml source
//!
//! * The failure list is a type, [`Errors`], not a `list` with a comment
//!   saying it is never empty. Merging two failures cannot produce an empty
//!   one, and neither can anything else.
//! * OCaml's `let*` and `let+`/`and+` have no counterpart in Rust's syntax.
//!   `?` covers the monadic case exactly; the applicative case — several
//!   independent results, all errors reported — is [`all!`], which takes up to
//!   eight results and yields a tuple.
//! * Operators are not definable in Rust, so the infix aliases are gone;
//!   adapters that take functions are free functions, operations on a
//!   two-track value are methods of [`RopExt`].
//!
//! The crate has no dependencies and no `std`: it is fit for a domain crate.

#![no_std]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::all)]

extern crate alloc;

mod adapters;
mod errors;
mod ext;

pub use crate::adapters::{and_also, both, compose, either, fail, plus, succeed, switch, tee};
pub use crate::errors::Errors;
pub use crate::ext::RopExt;

/// A two-track value: `Ok` on the success track, [`Errors`] on the failure
/// track. An alias, so `?`, `map`, `and_then` and every other `Result` method
/// apply unchanged.
pub type Rop<T, E> = Result<T, Errors<E>>;

/// Combines independent results, reporting every failure.
///
/// Takes two to eight results of the same error type and yields one result of
/// a tuple: `Ok` with every value if all succeeded, otherwise `Err` with the
/// errors of every branch that failed, in order. This is OCaml's `let+ … and+ …`
/// and the applicative half of the pattern; `?` is the monadic half.
///
/// ```
/// use ascetic_ddd_rop::{Rop, all, fail, succeed};
///
/// let a: Rop<i32, &str> = succeed(1);
/// let b: Rop<i32, &str> = fail("b is bad");
/// let c: Rop<i32, &str> = fail("c is bad");
///
/// assert_eq!(all!(a, b, c).unwrap_err().into_vec(), ["b is bad", "c is bad"]);
/// ```
#[macro_export]
macro_rules! all {
    ($a:expr, $b:expr $(,)?) => {
        $crate::both($a, $b)
    };
    ($a:expr, $b:expr, $c:expr $(,)?) => {
        $crate::both($crate::both($a, $b), $c).map(|((a, b), c)| (a, b, c))
    };
    ($a:expr, $b:expr, $c:expr, $d:expr $(,)?) => {
        $crate::both($crate::both($crate::both($a, $b), $c), $d)
            .map(|(((a, b), c), d)| (a, b, c, d))
    };
    ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr $(,)?) => {
        $crate::both($crate::both($crate::both($crate::both($a, $b), $c), $d), $e)
            .map(|((((a, b), c), d), e)| (a, b, c, d, e))
    };
    ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr $(,)?) => {
        $crate::both(
            $crate::both($crate::both($crate::both($crate::both($a, $b), $c), $d), $e),
            $f,
        )
        .map(|(((((a, b), c), d), e), f)| (a, b, c, d, e, f))
    };
    ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr $(,)?) => {
        $crate::both(
            $crate::both(
                $crate::both($crate::both($crate::both($crate::both($a, $b), $c), $d), $e),
                $f,
            ),
            $g,
        )
        .map(|((((((a, b), c), d), e), f), g)| (a, b, c, d, e, f, g))
    };
    ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr, $h:expr $(,)?) => {
        $crate::both(
            $crate::both(
                $crate::both(
                    $crate::both($crate::both($crate::both($crate::both($a, $b), $c), $d), $e),
                    $f,
                ),
                $g,
            ),
            $h,
        )
        .map(|(((((((a, b), c), d), e), f), g), h)| (a, b, c, d, e, f, g, h))
    };
}
