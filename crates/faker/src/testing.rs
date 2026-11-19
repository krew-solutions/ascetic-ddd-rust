//! Helpers for the crate's tests and for a host's.

use std::fmt;

use crate::key::{Encoder, KeyPart, Unit};
use crate::math;

/// The canonical bytes a scope is written as: for a host's test that its own
/// [`KeyPart`] is injective, as the crate's are.
///
/// ```
/// use ascetic_ddd_faker::testing::encoding;
///
/// assert_ne!(encoding(&("ab", "c")), encoding(&("a", "bc")));
/// assert_ne!(encoding(&None::<&str>), encoding(&Some("None")));
/// ```
pub fn encoding<P: KeyPart + ?Sized>(part: &P) -> Vec<u8> {
    let mut bytes = Vec::new();
    part.encode(&mut Encoder::new(&mut bytes));
    bytes
}

/// What [`audit`] saw.
#[cfg(feature = "audit")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Audit {
    /// Keys derived: `child`, `at`, `part`, and the crate's own.
    pub derivations: u64,
    /// Draws: `unit`, `bits`, `uuid`.
    pub draws: u64,
    /// Draws from a key already drawn from in the same run: values that move
    /// together. Zero for a sound generator (ADR-0045, C1).
    pub repeated: u64,
}

/// Runs `f` and reports the keys it derived and drew on this thread (feature
/// `audit`). Scoped to one run — one member's tree — because a whole
/// generation's draws would not fit in memory.
///
/// ```
/// # #[cfg(feature = "audit")] {
/// use ascetic_ddd_faker::testing::audit;
/// use ascetic_ddd_faker::{Key, Seed};
///
/// let (_, twice) = audit(|| {
///     let k = Key::root(Seed(1));
///     (k.child("os").unit(), k.child("os").unit()) // one key drawn twice
/// });
/// assert_eq!(twice.repeated, 1);
/// # }
/// ```
#[cfg(feature = "audit")]
pub fn audit<R>(f: impl FnOnce() -> R) -> (R, Audit) {
    let (result, ledger) = crate::key::audit::record(f);
    let mut drawn = ledger.drawn;
    let draws = drawn.len() as u64;
    drawn.sort_unstable();
    drawn.dedup();
    let audit = Audit {
        derivations: ledger.derivations,
        draws,
        repeated: draws.saturating_sub(drawn.len() as u64),
    };
    (result, audit)
}

/// `n` units spread evenly over (0, 1): the `i`th at `(i + ½) / n`.
///
/// A law's quantiles at these points are its `n`-quantiles. Their empirical
/// distribution is within `1/(2n)` of the law's own, with no seed and no
/// chance, so a test over them has no α and cannot be flaky (ADR-0045, L3).
pub fn stratified(n: u64) -> impl Iterator<Item = Unit> {
    let units = 1u128 << Unit::BITS;
    let n128 = u128::from(n.max(1));
    (0..n).filter_map(move |i| {
        let k = (u128::from(i).wrapping_mul(2).wrapping_add(1))
            .wrapping_mul(units)
            .checked_div(n128.wrapping_mul(2))
            .unwrap_or(0);
        Unit::from_raw(k as u64)
    })
}

/// The Kolmogorov–Smirnov distance between `n` stratified points and a law:
/// `cdfs[i]` is the law's distribution function at the quantile of the `i`th
/// point of [`stratified`]`(n)`.
pub fn ks_distance(cdfs: &[f64]) -> f64 {
    let n = cdfs.len() as f64;
    cdfs.iter()
        .enumerate()
        .map(|(i, f)| {
            let (lo, hi) = (i as f64 / n, (i as f64 + 1.0) / n);
            (f - lo).abs().max((hi - f).abs())
        })
        .fold(0.0, f64::max)
}

/// The two-sided Kolmogorov–Smirnov critical distance for `n` independent
/// draws at level `alpha`, by the asymptotic law: `√(−ln(α/2) / 2) / √n`.
/// Slightly conservative below n = 100.
pub fn ks_critical(n: u64, alpha: f64) -> f64 {
    (-math::ln(alpha / 2.0) / 2.0).sqrt() / (n as f64).sqrt()
}

/// A chi-square test of observed counts against expected ones.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ChiSquare {
    /// `Σ (oᵢ − eᵢ)² / eᵢ`.
    pub statistic: f64,
    /// The number of cells less one.
    pub degrees: u64,
    /// The probability of a statistic at least this large if the counts follow
    /// the expectation: `Q(degrees/2, statistic/2)`.
    pub p_value: f64,
}

/// Why a chi-square test was refused: it would have answered anything.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChiSquareRefused {
    /// The cells' lengths differ, or there are fewer than two.
    Shape,
    /// An expected count is zero, negative or not finite: its term would be
    /// infinite or NaN, and `NaN > critical` is false — a test that passes.
    Expected,
}

impl fmt::Display for ChiSquareRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ChiSquareRefused::Shape => {
                "observed and expected differ in length, or fewer than two cells"
            }
            ChiSquareRefused::Expected => "an expected count is not positive and finite",
        })
    }
}

impl std::error::Error for ChiSquareRefused {}

/// Tests `observed` against `expected`.
pub fn chi_square(observed: &[u64], expected: &[f64]) -> Result<ChiSquare, ChiSquareRefused> {
    if observed.len() != expected.len() || observed.len() < 2 {
        return Err(ChiSquareRefused::Shape);
    }
    if expected.iter().any(|e| !(e.is_finite() && *e > 0.0)) {
        return Err(ChiSquareRefused::Expected);
    }
    let statistic: f64 = observed
        .iter()
        .zip(expected)
        .map(|(o, e)| {
            let d = *o as f64 - e;
            d * d / e
        })
        .sum();
    let degrees = (observed.len() as u64).saturating_sub(1);
    Ok(ChiSquare {
        statistic,
        degrees,
        p_value: math::gamma_q(degrees as f64 / 2.0, statistic / 2.0),
    })
}
