//! Continuous laws: a value on the real line from one unit.
//!
//! Each quantile reads the unit as `p` or as `1 − p` ([`Unit::value`],
//! [`Unit::complement`]), whichever its formula is written in. Both are exact:
//! a unit is an odd multiple of 2⁻⁵³, so `1 − p` loses nothing.

use std::fmt;

use super::{InvalidLaw, Law, invalid};
use crate::key::Unit;
use crate::math;

fn positive(law: &'static str, name: &'static str, v: f64) -> Result<f64, InvalidLaw> {
    if v.is_finite() && v > 0.0 {
        Ok(v)
    } else {
        invalid(law, name)
    }
}

fn finite(law: &'static str, name: &'static str, v: f64) -> Result<f64, InvalidLaw> {
    if v.is_finite() {
        Ok(v)
    } else {
        invalid(law, name)
    }
}

/// Uniform over `[low, high)`.
#[derive(Clone, Debug, PartialEq)]
pub struct Uniform {
    low: f64,
    high: f64,
}

impl Uniform {
    /// Refused unless both bounds are finite and `low < high`.
    pub fn new(low: f64, high: f64) -> Result<Uniform, InvalidLaw> {
        let low = finite("uniform", "low must be finite", low)?;
        let high = finite("uniform", "high must be finite", high)?;
        if low < high {
            Ok(Uniform { low, high })
        } else {
            invalid("uniform", "low must be below high")
        }
    }
}

impl Law for Uniform {
    type Value = f64;

    fn quantile(&self, u: Unit) -> f64 {
        self.low + (self.high - self.low) * u.value()
    }

    fn cdf(&self, x: &f64) -> f64 {
        ((x - self.low) / (self.high - self.low)).clamp(0.0, 1.0)
    }

    fn cdf_below(&self, x: &f64) -> f64 {
        self.cdf(x)
    }
}

impl fmt::Display for Uniform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "uniform({}, {})", self.low, self.high)
    }
}

/// Exponential with rate `λ` (mean `1/λ`): the time between arrivals of a
/// Poisson process.
#[derive(Clone, Debug, PartialEq)]
pub struct Exponential {
    rate: f64,
}

impl Exponential {
    /// Refused unless the rate is finite and positive.
    pub fn new(rate: f64) -> Result<Exponential, InvalidLaw> {
        Ok(Exponential {
            rate: positive("exponential", "rate must be finite and positive", rate)?,
        })
    }
}

impl Law for Exponential {
    type Value = f64;

    fn quantile(&self, u: Unit) -> f64 {
        -math::ln(u.complement()) / self.rate
    }

    fn cdf(&self, x: &f64) -> f64 {
        if *x <= 0.0 {
            0.0
        } else {
            -math::exp_m1(-self.rate * x)
        }
    }

    fn cdf_below(&self, x: &f64) -> f64 {
        self.cdf(x)
    }
}

impl fmt::Display for Exponential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "exponential(rate={})", self.rate)
    }
}

/// Pareto with scale `xₘ` and shape `α`: `P(X > x) = (xₘ / x)^α` for `x ≥ xₘ`.
///
/// Heavy-tailed: its mean is infinite for `α ≤ 1`. A count drawn from it
/// must be [`Truncated`](super::Truncated).
#[derive(Clone, Debug, PartialEq)]
pub struct Pareto {
    scale: f64,
    shape: f64,
}

impl Pareto {
    /// Refused unless both parameters are finite and positive.
    pub fn new(scale: f64, shape: f64) -> Result<Pareto, InvalidLaw> {
        Ok(Pareto {
            scale: positive("pareto", "scale must be finite and positive", scale)?,
            shape: positive("pareto", "shape must be finite and positive", shape)?,
        })
    }
}

impl Law for Pareto {
    type Value = f64;

    fn quantile(&self, u: Unit) -> f64 {
        self.scale * math::pow(u.complement(), -1.0 / self.shape)
    }

    fn cdf(&self, x: &f64) -> f64 {
        if *x <= self.scale {
            0.0
        } else {
            -math::exp_m1(self.shape * math::ln(self.scale / x))
        }
    }

    fn cdf_below(&self, x: &f64) -> f64 {
        self.cdf(x)
    }
}

impl fmt::Display for Pareto {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pareto(scale={}, shape={})", self.scale, self.shape)
    }
}

/// Log-normal: `ln X` is normal with mean `μ` and standard deviation `σ`.
#[derive(Clone, Debug, PartialEq)]
pub struct LogNormal {
    mu: f64,
    sigma: f64,
}

impl LogNormal {
    /// Refused unless `μ` is finite and `σ` finite and positive.
    pub fn new(mu: f64, sigma: f64) -> Result<LogNormal, InvalidLaw> {
        Ok(LogNormal {
            mu: finite("lognormal", "mu must be finite", mu)?,
            sigma: positive("lognormal", "sigma must be finite and positive", sigma)?,
        })
    }
}

impl Law for LogNormal {
    type Value = f64;

    fn quantile(&self, u: Unit) -> f64 {
        math::exp(self.mu + self.sigma * math::normal_quantile(u.value(), u.complement()))
    }

    fn cdf(&self, x: &f64) -> f64 {
        if *x <= 0.0 {
            0.0
        } else {
            math::normal_cdf((math::ln(*x) - self.mu) / self.sigma)
        }
    }

    fn cdf_below(&self, x: &f64) -> f64 {
        self.cdf(x)
    }
}

impl fmt::Display for LogNormal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "lognormal(mu={}, sigma={})", self.mu, self.sigma)
    }
}

/// Weibull with scale `λ` and shape `k`: `P(X > x) = exp(−(x/λ)^k)`.
#[derive(Clone, Debug, PartialEq)]
pub struct Weibull {
    scale: f64,
    shape: f64,
}

impl Weibull {
    /// Refused unless both parameters are finite and positive.
    pub fn new(scale: f64, shape: f64) -> Result<Weibull, InvalidLaw> {
        Ok(Weibull {
            scale: positive("weibull", "scale must be finite and positive", scale)?,
            shape: positive("weibull", "shape must be finite and positive", shape)?,
        })
    }
}

impl Law for Weibull {
    type Value = f64;

    fn quantile(&self, u: Unit) -> f64 {
        self.scale * math::pow(-math::ln(u.complement()), 1.0 / self.shape)
    }

    fn cdf(&self, x: &f64) -> f64 {
        if *x <= 0.0 {
            0.0
        } else {
            -math::exp_m1(-math::pow(x / self.scale, self.shape))
        }
    }

    fn cdf_below(&self, x: &f64) -> f64 {
        self.cdf(x)
    }
}

impl fmt::Display for Weibull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "weibull(scale={}, shape={})", self.scale, self.shape)
    }
}
