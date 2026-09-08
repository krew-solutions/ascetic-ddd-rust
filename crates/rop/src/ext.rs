//! Operations on a two-track value, as methods.

use crate::Rop;
use crate::adapters::both;

/// What a [`Rop`] can do beyond what `Result` already does.
pub trait RopExt<T, E>: Sized {
    /// Pairs this result with another, reporting the errors of both if both
    /// failed. Method form of [`both`].
    fn both<U>(self, other: Rop<U, E>) -> Rop<(T, U), E>;

    /// Transforms the success value with `success` and every error with
    /// `failure`. Wlaschin's `doubleMap`, OCaml's `double_map`.
    fn double_map<U, F>(
        self,
        success: impl FnOnce(T) -> U,
        failure: impl FnMut(E) -> F,
    ) -> Rop<U, F>;

    /// Applies a wrapped function to a wrapped argument, reporting the errors
    /// of both if both failed. OCaml's `apply` / `<*>`.
    fn apply<A, B>(self, argument: Rop<A, E>) -> Rop<B, E>
    where
        T: FnOnce(A) -> B;
}

impl<T, E> RopExt<T, E> for Rop<T, E> {
    fn both<U>(self, other: Rop<U, E>) -> Rop<(T, U), E> {
        both(self, other)
    }

    fn double_map<U, F>(
        self,
        success: impl FnOnce(T) -> U,
        failure: impl FnMut(E) -> F,
    ) -> Rop<U, F> {
        self.map(success).map_err(|errors| errors.map(failure))
    }

    fn apply<A, B>(self, argument: Rop<A, E>) -> Rop<B, E>
    where
        T: FnOnce(A) -> B,
    {
        both(self, argument).map(|(f, a)| f(a))
    }
}
