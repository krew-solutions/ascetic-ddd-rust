//! Wlaschin's toolkit: the functions that build and join switches.
//!
//! A *switch* is a function from a one-track input to a two-track output,
//! `Fn(X) -> Rop<T, E>`. The adapters here turn plain functions into switches
//! and join switches together. Operations on a two-track *value* are methods
//! of [`RopExt`][crate::RopExt] instead.

use crate::Rop;
use crate::errors::Errors;

/// A value on the success track. Wlaschin's `succeed`.
pub fn succeed<T, E>(value: T) -> Rop<T, E> {
    Ok(value)
}

/// A single error on the failure track. Wlaschin's `fail`.
pub fn fail<T, E>(error: E) -> Rop<T, E> {
    Err(Errors::new(error))
}

/// Runs `success` on the success track or `failure` on the failure track.
/// Wlaschin's `either`; every other adapter can be written with it.
pub fn either<T, E, R>(
    success: impl FnOnce(T) -> R,
    failure: impl FnOnce(Errors<E>) -> R,
    input: Rop<T, E>,
) -> R {
    match input {
        Ok(value) => success(value),
        Err(errors) => failure(errors),
    }
}

/// Turns a plain function into a switch that always succeeds. Wlaschin's
/// `switch`.
pub fn switch<X, T, E>(f: impl Fn(X) -> T) -> impl Fn(X) -> Rop<T, E> {
    move |x| Ok(f(x))
}

/// Turns a dead-end function — log, notify — into a pass-through: the input
/// flows on unchanged after `f` has seen it. Wlaschin's `tee`.
pub fn tee<X>(f: impl Fn(&X)) -> impl Fn(X) -> X {
    move |x| {
        f(&x);
        x
    }
}

/// Chains two switches into one: the output of `first` feeds `second`, and a
/// failure in either is the failure of the whole. Wlaschin's `>=>`.
pub fn compose<A, B, C, E>(
    first: impl Fn(A) -> Rop<B, E>,
    second: impl Fn(B) -> Rop<C, E>,
) -> impl Fn(A) -> Rop<C, E> {
    move |a| first(a).and_then(&second)
}

/// Pairs two results, reporting the errors of both if both failed.
/// OCaml's `both`, the plumbing behind `and+` and [`all!`][crate::all].
pub fn both<A, B, E>(a: Rop<A, E>, b: Rop<B, E>) -> Rop<(A, B), E> {
    match (a, b) {
        (Ok(a), Ok(b)) => Ok((a, b)),
        (Err(errors), Ok(_)) | (Ok(_), Err(errors)) => Err(errors),
        (Err(first), Err(second)) => Err(first.merge(second)),
    }
}

/// Runs two switches on the same input, in parallel: successes are joined by
/// `add_success`, failures by `add_failure`. Wlaschin's `plus`.
pub fn plus<X, T, E>(
    add_success: impl Fn(T, T) -> T,
    add_failure: impl Fn(Errors<E>, Errors<E>) -> Errors<E>,
    first: impl Fn(&X) -> Rop<T, E>,
    second: impl Fn(&X) -> Rop<T, E>,
) -> impl Fn(&X) -> Rop<T, E> {
    move |x| match (first(x), second(x)) {
        (Ok(a), Ok(b)) => Ok(add_success(a, b)),
        (Err(errors), Ok(_)) | (Ok(_), Err(errors)) => Err(errors),
        (Err(a), Err(b)) => Err(add_failure(a, b)),
    }
}

/// [`plus`] for validators: both must pass, the first one's value is kept,
/// and the errors of both are reported. Wlaschin's `&&&`.
pub fn and_also<X, T, E>(
    first: impl Fn(&X) -> Rop<T, E>,
    second: impl Fn(&X) -> Rop<T, E>,
) -> impl Fn(&X) -> Rop<T, E> {
    plus(|value, _| value, Errors::merge, first, second)
}
