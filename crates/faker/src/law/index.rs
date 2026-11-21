//! Laws over the indices of a population of `n`: which member a reference
//! picks (Python's m2o distributors).
//!
//! The size is given at the draw, because a population's size is per scope.
//! Over a fixed size the realised law is the stated one exactly: Python's
//! distributors drew over a pool that grew while it was drawn from, and their
//! weighted one bent each partition towards its end to make up for it
//! (ADR-0045 § Context, defect 3). Nothing grows here, so nothing is bent.

use std::fmt;

use super::{Categorical, InvalidLaw, invalid, scale};
use crate::key::Unit;
use crate::math;

/// A law over `0..n` for any `n ≥ 1`.
pub trait IndexLaw: fmt::Display {
    /// The index at probability `u`, in `0..n`. `n` is at least 1.
    fn index(&self, u: Unit, n: u64) -> u64;

    /// The probability of an index at most `i`.
    fn cdf(&self, i: u64, n: u64) -> f64;
}

/// Every index alike.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UniformIndex;

impl IndexLaw for UniformIndex {
    fn index(&self, u: Unit, n: u64) -> u64 {
        scale(u.raw(), n)
    }

    fn cdf(&self, i: u64, n: u64) -> f64 {
        if i >= n {
            1.0
        } else {
            (i as f64 + 1.0) / n as f64
        }
    }
}

impl fmt::Display for UniformIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("uniform_index")
    }
}

/// A power law towards the first indices: `⌊n · u^s⌋`, so that
/// `P(index < q·n) = q^(1/s)`.
///
/// `s = 1` is uniform; at `s = 2` the first 20 % of the members take 45 % of
/// the picks, at `s = 3` the first 10 % take 46 % (Python's docstring said 60 %
/// and 70 %: defect 9).
#[derive(Clone, Debug, PartialEq)]
pub struct Skew {
    skew: f64,
}

impl Skew {
    /// Refused unless the skew is finite and positive.
    pub fn new(skew: f64) -> Result<Skew, InvalidLaw> {
        if skew.is_finite() && skew > 0.0 {
            Ok(Skew { skew })
        } else {
            invalid("skew", "the skew must be finite and positive")
        }
    }
}

impl IndexLaw for Skew {
    fn index(&self, u: Unit, n: u64) -> u64 {
        // At s ≤ ½ and u near 1, u^s rounds to 1.0 and the product to n.
        let i = (n as f64 * math::pow(u.value(), self.skew)).floor() as u64;
        i.min(n.saturating_sub(1))
    }

    fn cdf(&self, i: u64, n: u64) -> f64 {
        if i >= n {
            1.0
        } else {
            math::pow((i as f64 + 1.0) / n as f64, 1.0 / self.skew).min(1.0)
        }
    }
}

impl fmt::Display for Skew {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "skew({})", self.skew)
    }
}

/// The population cut into `m` equal parts, part `j` picked with weight `wⱼ`
/// and a member of it uniformly: Python's weighted m2o distributor, without the
/// bend.
///
/// One unit does both: the part by [`Categorical`]'s thresholds, the member by
/// the unit's position inside the part's interval.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Partitioned {
    parts: Categorical,
}

impl Partitioned {
    /// From the parts' weights, as [`Categorical::from_weights`].
    pub fn new(weights: &[f64]) -> Result<Partitioned, InvalidLaw> {
        Categorical::from_weights(weights)
            .map(|parts| Partitioned { parts })
            .map_err(|e| InvalidLaw {
                law: "partitioned",
                reason: e.reason,
            })
    }

    /// The members of part `j` of `n`: `⌊j·n/m⌋ .. ⌊(j+1)·n/m⌋`, at least one.
    /// With fewer members than parts, an empty part keeps the member at its
    /// start, as Python's did.
    fn bounds(&self, part: usize, n: u64) -> (u64, u64) {
        let m = self.parts.len() as u128;
        let edge = |j: usize| -> u64 {
            (u128::from(n).wrapping_mul(j as u128))
                .checked_div(m)
                .map_or(0, |e| e as u64)
        };
        let start = edge(part).min(n.saturating_sub(1));
        let end = edge(part.saturating_add(1))
            .max(start.saturating_add(1))
            .min(n);
        (start, end)
    }
}

impl IndexLaw for Partitioned {
    fn index(&self, u: Unit, n: u64) -> u64 {
        let (part, inner) = self.parts.split(u);
        let (start, end) = self.bounds(part, n);
        start.saturating_add(scale(inner.raw(), end.saturating_sub(start)))
    }

    fn cdf(&self, i: u64, n: u64) -> f64 {
        if i >= n {
            return 1.0;
        }
        (0..self.parts.len())
            .map(|part| {
                let weight = self
                    .parts
                    .threshold(part.saturating_add(1))
                    .saturating_sub(self.parts.threshold(part)) as f64
                    / super::UNITS_F;
                // A part is never empty (`bounds`); its share up to `i` is 0
                // before it, 1 past it.
                let (start, end) = self.bounds(part, n);
                let covered = (i.saturating_add(1)).min(end).saturating_sub(start) as f64
                    / end.saturating_sub(start) as f64;
                weight * covered
            })
            .sum::<f64>()
            .min(1.0)
    }
}

impl fmt::Display for Partitioned {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "partitioned({})", self.parts)
    }
}
