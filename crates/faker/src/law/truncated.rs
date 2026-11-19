//! A law conditioned on an interval.

use std::fmt;

use super::{InvalidLaw, Law, UNITS_F, invalid, scale, threshold};
use crate::key::Unit;

/// `law` conditioned on `[low, high]`: drawn by its own quantile over the units
/// between `F(low⁻)` and `F(high)`, so a draw still takes one unit and costs
/// what the law's does.
///
/// Not a clamp: a clamp would pile the tail beyond `high` onto `high` as an atom
/// (for a Pareto of shape 1.16 capped at 1000 times its scale, a mass of
/// 3·10⁻⁴). Rescaled, the mass beyond is spread over the interval as the law
/// itself spreads it.
#[derive(Clone, Debug, PartialEq)]
pub struct Truncated<L: Law> {
    law: L,
    low: L::Value,
    high: L::Value,
    /// The units of the interval: `⌈F(low⁻) · 2⁵²⌉ .. ⌊F(high) · 2⁵²⌋`.
    from: u64,
    to: u64,
}

impl<L> Truncated<L>
where
    L: Law,
    L::Value: PartialOrd + Clone,
{
    /// Refused unless `low ≤ high` and the interval has mass: at least one of
    /// the 2⁵² units maps into it.
    pub fn new(law: L, low: L::Value, high: L::Value) -> Result<Truncated<L>, InvalidLaw> {
        if low > high {
            return invalid("truncated", "low must not be above high");
        }
        let from = threshold(law.cdf_below(&low));
        let to = (law.cdf(&high) * UNITS_F).floor() as u64;
        if to <= from {
            return invalid("truncated", "the interval holds no probability");
        }
        Ok(Truncated {
            law,
            low,
            high,
            from,
            to,
        })
    }

    fn clamp(&self, x: L::Value) -> L::Value {
        // The units map into [F(low⁻), F(high)], whose quantiles lie in
        // [low, high] but for the last bit of a rounded `F` at either end.
        if x < self.low {
            self.low.clone()
        } else if x > self.high {
            self.high.clone()
        } else {
            x
        }
    }
}

impl<L> Law for Truncated<L>
where
    L: Law,
    L::Value: PartialOrd + Clone + fmt::Display,
{
    type Value = L::Value;

    fn quantile(&self, u: Unit) -> L::Value {
        // from < to, checked in `new`.
        let k = self
            .from
            .saturating_add(scale(u.raw(), self.to.saturating_sub(self.from)));
        let inner = Unit::from_raw(k).unwrap_or(u);
        self.clamp(self.law.quantile(inner))
    }

    fn cdf(&self, x: &L::Value) -> f64 {
        let (a, b) = (self.from as f64 / UNITS_F, self.to as f64 / UNITS_F);
        ((self.law.cdf(x) - a) / (b - a)).clamp(0.0, 1.0)
    }

    fn cdf_below(&self, x: &L::Value) -> f64 {
        let (a, b) = (self.from as f64 / UNITS_F, self.to as f64 / UNITS_F);
        ((self.law.cdf_below(x) - a) / (b - a)).clamp(0.0, 1.0)
    }
}

impl<L> fmt::Display for Truncated<L>
where
    L: Law,
    L::Value: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "truncated({}, {}, {})", self.law, self.low, self.high)
    }
}
