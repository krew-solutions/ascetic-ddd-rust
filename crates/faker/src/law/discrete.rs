//! Discrete laws: a count from one unit.
//!
//! [`Categorical`] and [`UniformInt`] compare the unit's integer `k` with
//! integer thresholds: their choice involves no floating point at all.
//! [`Poisson`] and [`Binomial`] walk their probability mass function outward
//! from the mode, from one unit, in O(σ) steps.

use std::fmt;

use super::{InvalidLaw, Law, UNITS, UNITS_F, invalid};
use crate::key::Unit;
use crate::math;

/// Uniform over the integers `low..=high`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UniformInt {
    low: u64,
    high: u64,
}

impl UniformInt {
    /// Refused unless `low ≤ high` and the range holds at most 2⁵² integers:
    /// one unit cannot tell more apart. For 64 random bits, draw
    /// [`Key::bits`](crate::Key::bits).
    pub fn new(low: u64, high: u64) -> Result<UniformInt, InvalidLaw> {
        if low > high {
            invalid("uniform_int", "low must not be above high")
        } else if high.saturating_sub(low) >= UNITS {
            invalid(
                "uniform_int",
                "more than 2^52 integers: draw Key::bits instead",
            )
        } else {
            Ok(UniformInt { low, high })
        }
    }

    fn width(&self) -> u128 {
        // low ≤ high, checked in `new`.
        u128::from(self.high.saturating_sub(self.low)).wrapping_add(1)
    }
}

impl Law for UniformInt {
    type Value = u64;

    fn quantile(&self, u: Unit) -> u64 {
        // ⌊k · width / 2⁵²⌋ < width, exact in u128: width ≤ 2⁵².
        let offset = (u128::from(u.raw()).wrapping_mul(self.width()) >> Unit::BITS) as u64;
        self.low.saturating_add(offset)
    }

    fn cdf(&self, x: &u64) -> f64 {
        if *x < self.low {
            0.0
        } else if *x >= self.high {
            1.0
        } else {
            (u128::from(x.saturating_sub(self.low)).wrapping_add(1)) as f64 / self.width() as f64
        }
    }

    fn cdf_below(&self, x: &u64) -> f64 {
        match x.checked_sub(1) {
            Some(below) if *x > self.low => self.cdf(&below),
            _ => 0.0,
        }
    }
}

impl fmt::Display for UniformInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "uniform_int({}, {})", self.low, self.high)
    }
}

/// A choice among `m` items by weight: item `i` with probability `wᵢ / Σw`.
///
/// Built into integer thresholds `0 = T₀ ≤ T₁ ≤ … ≤ Tₘ = 2⁵²`; item `i` is
/// drawn exactly when `Tᵢ ≤ k < Tᵢ₊₁`. A zero weight is an empty interval and is
/// never drawn. A draw is a binary search: O(log m).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Categorical {
    /// `T₁ … Tₘ`; `T₀ = 0` is implicit.
    thresholds: Vec<u64>,
}

impl Categorical {
    /// From weights: finite, not negative, not all zero.
    pub fn from_weights(weights: &[f64]) -> Result<Categorical, InvalidLaw> {
        if weights.is_empty() {
            return invalid("categorical", "no weights");
        }
        if weights.iter().any(|w| !w.is_finite() || *w < 0.0) {
            return invalid("categorical", "a weight is negative or not finite");
        }
        let total: f64 = weights.iter().sum();
        if !(total.is_finite() && total > 0.0) {
            return invalid("categorical", "the weights sum to zero or overflow");
        }
        let mut cumulative = 0.0;
        let mut thresholds: Vec<u64> = weights
            .iter()
            .map(|w| {
                cumulative += w;
                ((cumulative / total) * UNITS_F).floor().min(UNITS_F) as u64
            })
            .collect();
        if let Some(last) = thresholds.last_mut() {
            *last = UNITS;
        }
        Ok(Categorical { thresholds })
    }

    /// From counts, exactly: `Tᵢ = ⌊(c₀ + … + cᵢ₋₁) · 2⁵² / Σc⌋` in integers.
    pub fn from_counts(counts: &[u64]) -> Result<Categorical, InvalidLaw> {
        if counts.is_empty() {
            return invalid("categorical", "no counts");
        }
        let total: u128 = counts.iter().map(|c| u128::from(*c)).sum();
        if total == 0 {
            return invalid("categorical", "the counts sum to zero");
        }
        // Each threshold is a cumulative count times 2⁵², which must fit a u128.
        if total >= 1 << (u128::BITS - Unit::BITS) {
            return invalid("categorical", "the counts sum to 2^76 or more");
        }
        let mut cumulative: u128 = 0;
        let thresholds = counts
            .iter()
            .map(|c| {
                cumulative = cumulative.wrapping_add(u128::from(*c));
                // cumulative < 2⁷⁶, checked above, so times 2⁵² it fits a u128.
                (cumulative << Unit::BITS)
                    .checked_div(total)
                    .map_or(0, |t| t as u64)
            })
            .collect();
        Ok(Categorical { thresholds })
    }

    /// The number of items.
    pub fn len(&self) -> usize {
        self.thresholds.len()
    }

    /// Whether there are no items: never, a categorical is refused without.
    pub fn is_empty(&self) -> bool {
        self.thresholds.is_empty()
    }

    /// The item whose interval holds `k`, by binary search: O(log m). Each
    /// comparison calls `probe`, which only a test counts (L8).
    pub(crate) fn locate(&self, k: u64, mut probe: impl FnMut()) -> usize {
        self.thresholds.partition_point(|t| {
            probe();
            *t <= k
        })
    }

    /// `Tᵢ`, for `i` in `0..=m`.
    pub(crate) fn threshold(&self, i: usize) -> u64 {
        match i.checked_sub(1) {
            None => 0,
            Some(j) => self.thresholds.get(j).copied().unwrap_or(UNITS),
        }
    }

    /// The item drawn, and the unit's position inside the item's interval,
    /// rescaled to a unit of its own: a second uniform from the same draw,
    /// independent of the first.
    pub(crate) fn split(&self, u: Unit) -> (usize, Unit) {
        let k = u.raw();
        let item = self.locate(k, || ());
        let low = self.threshold(item);
        let high = self.threshold(item.saturating_add(1));
        // low ≤ k < high, so the width is positive and the rescaled k is below 2⁵².
        let width = u128::from(high.saturating_sub(low));
        let inner = (u128::from(k.saturating_sub(low)) << Unit::BITS)
            .checked_div(width)
            .map_or(0, |i| i as u64);
        (item, Unit::from_raw(inner).unwrap_or(u))
    }
}

impl Law for Categorical {
    type Value = u64;

    fn quantile(&self, u: Unit) -> u64 {
        self.locate(u.raw(), || ()) as u64
    }

    fn cdf(&self, x: &u64) -> f64 {
        let i = usize::try_from(*x).unwrap_or(usize::MAX).saturating_add(1);
        self.threshold(i) as f64 / UNITS_F
    }

    fn cdf_below(&self, x: &u64) -> f64 {
        let i = usize::try_from(*x).unwrap_or(usize::MAX);
        self.threshold(i) as f64 / UNITS_F
    }
}

impl fmt::Display for Categorical {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("categorical[")?;
        for (i, t) in self.thresholds.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{t}")?;
        }
        f.write_str("]")
    }
}

/// A histogram measured on real data: value `vᵢ` with probability
/// `cᵢ / Σc` (ADR-0045: the one law that answers "reflect real data").
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Empirical {
    values: Vec<u64>,
    weights: Categorical,
}

impl Empirical {
    /// From `(value, count)` pairs, values strictly increasing.
    pub fn from_histogram(buckets: &[(u64, u64)]) -> Result<Empirical, InvalidLaw> {
        if buckets.windows(2).any(|w| w[0].0 >= w[1].0) {
            return invalid("empirical", "values must be strictly increasing");
        }
        let counts: Vec<u64> = buckets.iter().map(|(_, c)| *c).collect();
        let weights = Categorical::from_counts(&counts).map_err(|e| InvalidLaw {
            law: "empirical",
            reason: e.reason,
        })?;
        Ok(Empirical {
            values: buckets.iter().map(|(v, _)| *v).collect(),
            weights,
        })
    }
}

impl Law for Empirical {
    type Value = u64;

    fn quantile(&self, u: Unit) -> u64 {
        let i = self.weights.quantile(u);
        usize::try_from(i)
            .ok()
            .and_then(|i| self.values.get(i))
            .copied()
            .unwrap_or(u64::MAX)
    }

    fn cdf(&self, x: &u64) -> f64 {
        let at_most = self.values.partition_point(|v| v <= x);
        self.weights.threshold(at_most) as f64 / UNITS_F
    }

    fn cdf_below(&self, x: &u64) -> f64 {
        let below = self.values.partition_point(|v| v < x);
        self.weights.threshold(below) as f64 / UNITS_F
    }
}

impl fmt::Display for Empirical {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("empirical[")?;
        for (i, v) in self.values.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{v}:{}", self.weights.threshold(i.saturating_add(1)))?;
        }
        f.write_str("]")
    }
}

/// The largest mean a [`Poisson`] takes: its walk costs O(√λ) steps, about
/// 2.5·10⁵ at most here, and above it a count is better drawn by
/// [`Binomial`] or summed from smaller ones.
pub const POISSON_MAX_MEAN: f64 = 1e9;

/// The largest number of trials a [`Binomial`] takes: integers up to 2⁵³ are
/// exact in an `f64`.
pub const BINOMIAL_MAX_TRIALS: u64 = 1 << 53;

/// A discrete law walked from its mode: its mass, exact tails, and the ratio
/// of neighbouring masses.
trait Mass {
    /// The most likely value.
    fn mode(&self) -> u64;
    /// The highest value the law takes.
    fn top(&self) -> u64;
    /// `P(X = x)`, computed directly.
    fn mass(&self, x: u64) -> f64;
    /// `P(X ≤ x)`, to relative precision however small.
    fn at_most(&self, x: u64) -> f64;
    /// `P(X > x)`, to relative precision however small.
    fn above(&self, x: u64) -> f64;
    /// `P(X = x − 1)` from `P(X = x)`.
    fn down(&self, x: u64, mass: f64) -> f64;
    /// `P(X = x + 1)` from `P(X = x)`.
    fn up(&self, x: u64, mass: f64) -> f64;
}

/// How far a running tail may fall below its last exact value before it is
/// computed again: past it, the running sum's absolute rounding, about 10⁻¹⁶ of
/// where it started, would be more than 10⁻¹⁰ of what is left.
const REANCHOR: f64 = 1e-6;

/// What a walk cost: the steps it took and how many times it computed a tail
/// exactly (M4).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Cost {
    pub(crate) steps: u64,
    pub(crate) exact: u64,
}

/// The least `x` with `P(X ≤ x) ≥ p`, and what it cost.
///
/// Below the mode's distribution function it steps down on `F` against `p`;
/// above it, it steps up on the upper tail `S = 1 − F` against `1 − p`, both
/// exact from the unit, so neither tail rounds to `1 − F`. A running sum loses
/// relative precision as it shrinks — `S` at the mode is about ½ and its last
/// bit 10⁻¹⁶, so a tail of 5·10⁻¹⁶ would be noise — so each time it falls a
/// million-fold it is computed again from the incomplete gamma or beta
/// function, whose precision is relative. O(σ) steps, and a few exact
/// evaluations in the far tails only.
fn walk(law: &impl Mass, u: Unit) -> (u64, Cost) {
    let p = u.value();
    let mut cost = Cost::default();
    let mut x = law.mode();
    let mut mass = law.mass(x);
    let mut at_most = law.at_most(x);
    if p <= at_most {
        let mut anchor = at_most;
        while x > 0 {
            let below = at_most - mass;
            if below < p || below == at_most {
                break;
            }
            mass = law.down(x, mass);
            x = x.saturating_sub(1); // x > 0 in the loop
            at_most = below;
            if at_most < anchor * REANCHOR {
                at_most = law.at_most(x);
                mass = law.mass(x);
                anchor = at_most;
                cost.exact = cost.exact.saturating_add(1);
            }
            cost.steps = cost.steps.saturating_add(1);
        }
    } else {
        let tail = u.complement();
        let mut above = law.above(x);
        let mut anchor = above;
        while above > tail && x < law.top() {
            mass = law.up(x, mass);
            x = x.saturating_add(1); // x < top in the loop
            let next = above - mass;
            if next == above {
                break;
            }
            above = next;
            if above < anchor * REANCHOR {
                above = law.above(x);
                mass = law.mass(x);
                anchor = above;
                cost.exact = cost.exact.saturating_add(1);
            }
            cost.steps = cost.steps.saturating_add(1);
        }
    }
    (x, cost)
}

/// Poisson with mean `λ`: the number of events of a process of rate `λ` in a
/// unit of time.
#[derive(Clone, Debug, PartialEq)]
pub struct Poisson {
    mean: f64,
}

impl Poisson {
    /// Refused unless `0 ≤ λ ≤` [`POISSON_MAX_MEAN`].
    pub fn new(mean: f64) -> Result<Poisson, InvalidLaw> {
        if mean.is_finite() && (0.0..=POISSON_MAX_MEAN).contains(&mean) {
            Ok(Poisson { mean })
        } else {
            invalid(
                "poisson",
                "the mean must be finite, not negative, at most 1e9",
            )
        }
    }

    /// The quantile and what its walk cost (M4).
    pub(crate) fn walk_from(&self, u: Unit) -> (u64, Cost) {
        if self.mean == 0.0 {
            (0, Cost::default())
        } else {
            walk(self, u)
        }
    }
}

impl Mass for Poisson {
    fn mode(&self) -> u64 {
        self.mean.floor() as u64
    }

    fn top(&self) -> u64 {
        u64::MAX
    }

    fn mass(&self, x: u64) -> f64 {
        math::exp(math::ln_poisson_mass(x as f64, self.mean))
    }

    fn at_most(&self, x: u64) -> f64 {
        math::gamma_q(x as f64 + 1.0, self.mean)
    }

    fn above(&self, x: u64) -> f64 {
        math::gamma_p(x as f64 + 1.0, self.mean)
    }

    fn down(&self, x: u64, mass: f64) -> f64 {
        mass * x as f64 / self.mean
    }

    fn up(&self, x: u64, mass: f64) -> f64 {
        mass * self.mean / (x as f64 + 1.0)
    }
}

impl Law for Poisson {
    type Value = u64;

    fn quantile(&self, u: Unit) -> u64 {
        self.walk_from(u).0
    }

    fn cdf(&self, x: &u64) -> f64 {
        if self.mean == 0.0 {
            1.0
        } else {
            self.at_most(*x)
        }
    }

    fn cdf_below(&self, x: &u64) -> f64 {
        x.checked_sub(1).map_or(0.0, |below| self.cdf(&below))
    }
}

impl fmt::Display for Poisson {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "poisson(mean={})", self.mean)
    }
}

/// Binomial: the number of successes in `n` trials of probability `p`. The
/// split of a fixed total between two parts, kept random: a multinomial is a
/// chain of them.
#[derive(Clone, Debug, PartialEq)]
pub struct Binomial {
    trials: u64,
    p: f64,
}

impl Binomial {
    /// Refused unless `n ≤` [`BINOMIAL_MAX_TRIALS`] and `0 ≤ p ≤ 1`.
    pub fn new(trials: u64, p: f64) -> Result<Binomial, InvalidLaw> {
        if trials > BINOMIAL_MAX_TRIALS {
            return invalid("binomial", "at most 2^53 trials");
        }
        if !(p.is_finite() && (0.0..=1.0).contains(&p)) {
            return invalid("binomial", "p must lie in [0, 1]");
        }
        Ok(Binomial { trials, p })
    }

    fn degenerate(&self) -> Option<u64> {
        if self.trials == 0 || self.p == 0.0 {
            Some(0)
        } else if self.p == 1.0 {
            Some(self.trials)
        } else {
            None
        }
    }

    /// The quantile and what its walk cost.
    pub(crate) fn walk_from(&self, u: Unit) -> (u64, Cost) {
        match self.degenerate() {
            Some(x) => (x, Cost::default()),
            None => walk(self, u),
        }
    }

    fn odds(&self) -> f64 {
        self.p / (1.0 - self.p)
    }
}

impl Mass for Binomial {
    fn mode(&self) -> u64 {
        let n = self.trials as f64;
        ((n + 1.0) * self.p).floor().min(n) as u64
    }

    fn top(&self) -> u64 {
        self.trials
    }

    fn mass(&self, x: u64) -> f64 {
        math::exp(math::ln_binomial_mass(x as f64, self.trials as f64, self.p))
    }

    fn at_most(&self, x: u64) -> f64 {
        if x >= self.trials {
            1.0
        } else {
            // P(X ≤ k) = I_{1−p}(n − k, k + 1).
            let (n, k) = (self.trials as f64, x as f64);
            math::beta_i(n - k, k + 1.0, 1.0 - self.p)
        }
    }

    fn above(&self, x: u64) -> f64 {
        if x >= self.trials {
            0.0
        } else {
            // P(X > k) = I_p(k + 1, n − k).
            let (n, k) = (self.trials as f64, x as f64);
            math::beta_i(k + 1.0, n - k, self.p)
        }
    }

    fn down(&self, x: u64, mass: f64) -> f64 {
        let (n, k) = (self.trials as f64, x as f64);
        mass * k / ((n - k + 1.0) * self.odds())
    }

    fn up(&self, x: u64, mass: f64) -> f64 {
        let (n, k) = (self.trials as f64, x as f64);
        mass * (n - k) / (k + 1.0) * self.odds()
    }
}

impl Law for Binomial {
    type Value = u64;

    fn quantile(&self, u: Unit) -> u64 {
        self.walk_from(u).0
    }

    fn cdf(&self, x: &u64) -> f64 {
        match self.degenerate() {
            Some(d) => {
                if *x >= d {
                    1.0
                } else {
                    0.0
                }
            }
            None => self.at_most(*x),
        }
    }

    fn cdf_below(&self, x: &u64) -> f64 {
        x.checked_sub(1).map_or(0.0, |below| self.cdf(&below))
    }
}

impl fmt::Display for Binomial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "binomial(trials={}, p={})", self.trials, self.p)
    }
}
