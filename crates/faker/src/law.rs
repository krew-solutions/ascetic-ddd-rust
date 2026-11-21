//! Laws: probability distributions as values (ADR-0045 decision 4).
//!
//! A law turns one [`Unit`] into one value by its quantile function, the
//! inverse of its distribution function. One uniform a draw, always: the number
//! of draws never depends on the value drawn, so a key can name a draw.
//!
//! Each law is validated where it is built, and has a canonical `Display`, the
//! form a scenario's header records.
//!
//! - **Values:** [`Uniform`], [`Exponential`], [`Pareto`], [`LogNormal`],
//!   [`Weibull`], and any of them [`Truncated`].
//! - **Counts:** [`UniformInt`], [`Categorical`], [`Empirical`], [`Poisson`],
//!   [`Binomial`].
//! - **Indices of a population of `n`** ([`IndexLaw`]): [`UniformIndex`],
//!   [`Skew`], [`Partitioned`].

use std::fmt;

use crate::key::Unit;

mod continuous;
mod discrete;
mod index;
mod truncated;

#[cfg(test)]
mod reference;
#[cfg(test)]
mod tests;

pub use self::continuous::{Exponential, LogNormal, Pareto, Uniform, Weibull};
pub use self::discrete::{
    BINOMIAL_MAX_TRIALS, Binomial, Categorical, Empirical, POISSON_MAX_MEAN, Poisson, UniformInt,
};
pub use self::index::{IndexLaw, Partitioned, Skew, UniformIndex};
pub use self::truncated::Truncated;

/// A probability distribution, drawn from by its quantile function.
pub trait Law: fmt::Display {
    /// What a draw gives.
    type Value;

    /// The value at probability `u`: the least `x` with `cdf(x) ≥ u`.
    fn quantile(&self, u: Unit) -> Self::Value;

    /// The probability of a value at most `x`.
    fn cdf(&self, x: &Self::Value) -> f64;

    /// The probability of a value below `x`: `cdf(x)` for a continuous law,
    /// `cdf(x − 1)` for a discrete one. What [`Truncated`] needs at its lower
    /// bound.
    fn cdf_below(&self, x: &Self::Value) -> f64;
}

/// A law refused where it is built.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidLaw {
    /// The law.
    pub law: &'static str,
    /// What is wrong with its parameters.
    pub reason: &'static str,
}

impl fmt::Display for InvalidLaw {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.law, self.reason)
    }
}

impl std::error::Error for InvalidLaw {}

pub(crate) fn invalid<T>(law: &'static str, reason: &'static str) -> Result<T, InvalidLaw> {
    Err(InvalidLaw { law, reason })
}

/// 2⁵², the number of distinct units: the scale of every integer threshold.
pub(crate) const UNITS: u64 = 1 << Unit::BITS;

/// 2⁵² as a float, exact.
pub(crate) const UNITS_F: f64 = UNITS as f64;

/// `⌊k · n / 2⁵²⌋` for a 52-bit `k`, exact: the product of two `u64` fits a
/// `u128`.
pub(crate) fn scale(k: u64, n: u64) -> u64 {
    (u128::from(k).wrapping_mul(u128::from(n)) >> Unit::BITS) as u64
}

/// A probability as an integer threshold over the 2⁵² units: `⌈p · 2⁵²⌉`,
/// clamped to `[0, 2⁵²]`. Exact: the scale is a power of two.
pub(crate) fn threshold(p: f64) -> u64 {
    let t = (p * UNITS_F).ceil();
    if t <= 0.0 {
        0
    } else if t >= UNITS_F {
        UNITS
    } else {
        t as u64
    }
}
