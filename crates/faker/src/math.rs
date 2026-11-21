//! Pinned math: the transcendental functions every law goes through
//! (ADR-0045 decision 3).
//!
//! The standard library leaves the precision of `f64::ln`, `exp`, `powf` and
//! their relatives unspecified, so their results may differ across platforms
//! and Rust releases, and a generated value with them. Here they come from
//! `libm`, pinned exactly: one pure-Rust implementation everywhere. On x86_64
//! its `arch` feature replaces only `sqrt` and `fma`, which IEEE 754 rounds
//! correctly either way, so a host enabling it changes no value.
//!
//! What `libm` does not have is written here from `+ − × ÷`, which are
//! correctly rounded: the regularized incomplete gamma and beta functions and
//! the inverse of the normal distribution.

/// The natural logarithm.
pub fn ln(x: f64) -> f64 {
    libm::log(x)
}

/// `ln(1 + x)`, exact near 0.
pub fn ln_1p(x: f64) -> f64 {
    libm::log1p(x)
}

/// `eˣ`.
pub fn exp(x: f64) -> f64 {
    libm::exp(x)
}

/// `eˣ − 1`, exact near 0.
pub fn exp_m1(x: f64) -> f64 {
    libm::expm1(x)
}

/// `xʸ`.
pub fn pow(x: f64, y: f64) -> f64 {
    libm::pow(x, y)
}

/// `ln |Γ(x)|`.
pub fn ln_gamma(x: f64) -> f64 {
    libm::lgamma_r(x).0
}

/// The error function, exact near 0.
pub fn erf(x: f64) -> f64 {
    libm::erf(x)
}

/// The complementary error function, `1 − erf(x)`, exact in the far tail.
pub fn erfc(x: f64) -> f64 {
    libm::erfc(x)
}

/// `ln(1 + t) − t`, to absolute precision `ε·|t|`: the difference that
/// Stirling's form of a mass leaves once its large terms cancel exactly.
fn ln_1p_minus(t: f64) -> f64 {
    ln_1p(t) - t
}

/// `ln Γ(z) − ((z − ½) ln z − z + ½ ln 2π)`: what Stirling's formula leaves out.
///
/// For `z ≥ 10`, Stirling's series `Σ B₂ₖ / (2k(2k − 1) z²ᵏ⁻¹)` to its seventh
/// term, `z⁻¹³`. The series is asymptotic and alternating, so what it leaves out
/// is less than its first omitted term, `3617/122400 · z⁻¹⁵`: 3·10⁻¹⁷ at
/// `z = 10`, a tenth of the last bit of a log mass, so a further term would
/// change nothing a test could see. (Four terms left 8·10⁻¹³ there, which the
/// masses' reference values found.) Below 10, by difference: `ln Γ(z)` is below
/// 13 there, so the difference is good to a few 10⁻¹⁵.
fn stirling_error(z: f64) -> f64 {
    if z >= 10.0 {
        let r = 1.0 / z;
        let r2 = r * r;
        r * (1.0 / 12.0
            - r2 * (1.0 / 360.0
                - r2 * (1.0 / 1260.0
                    - r2 * (1.0 / 1680.0
                        - r2 * (1.0 / 1188.0 - r2 * (691.0 / 360360.0 - r2 * (1.0 / 156.0)))))))
    } else {
        ln_gamma(z) - ((z - 0.5) * ln(z) - z + 0.5 * ln(2.0 * std::f64::consts::PI))
    }
}

/// `ln P(X = x)` for a Poisson of mean `λ`, for real `x ≥ 0`:
/// `x ln λ − λ − ln Γ(x + 1)`, written so that its large terms — of the order of
/// `x ln x` — cancel exactly instead of in rounding (Loader's form): with
/// `t = (λ − x)/x`, it is `x(ln(1 + t) − t) − ½ ln(2πx) − stirling_error(x)`. Its
/// error grows as `ε√x`, not as `ε x ln x`.
pub fn ln_poisson_mass(x: f64, lambda: f64) -> f64 {
    if x == 0.0 {
        return -lambda;
    }
    let t = (lambda - x) / x;
    x * ln_1p_minus(t) - 0.5 * ln(2.0 * std::f64::consts::PI * x) - stirling_error(x)
}

/// `ln P(X = k)` for a binomial of `n` trials of probability `p`, for real
/// `0 ≤ k ≤ n`: `ln C(n, k) + k ln p + (n − k) ln(1 − p)` in Loader's form. With
/// `r = n − k`, `t₁ = np/k − 1` and `t₂ = n(1 − p)/r − 1`, which satisfy
/// `k t₁ + r t₂ = 0`, it is
/// `k(ln(1 + t₁) − t₁) + r(ln(1 + t₂) − t₂) + ½ ln(n / (2π k r))`
/// `+ stirling_error(n) − stirling_error(k) − stirling_error(r)`.
pub fn ln_binomial_mass(k: f64, n: f64, p: f64) -> f64 {
    if k == 0.0 {
        return n * ln_1p(-p);
    }
    if k == n {
        return n * ln(p);
    }
    let r = n - k;
    let t1 = n * p / k - 1.0;
    let t2 = n * (1.0 - p) / r - 1.0;
    k * ln_1p_minus(t1)
        + r * ln_1p_minus(t2)
        + 0.5 * ln(n / (2.0 * std::f64::consts::PI * k * r))
        + stirling_error(n)
        - stirling_error(k)
        - stirling_error(r)
}

/// The standard normal distribution function, `Φ(x)`, exact in the lower tail.
pub fn normal_cdf(x: f64) -> f64 {
    0.5 * erfc(-x * std::f64::consts::FRAC_1_SQRT_2)
}

/// The inverse of `Φ`: the `x` with `Φ(x) = p`, for `p` given together with
/// `1 − p` (`q`), so that neither tail loses its digits to a subtraction.
///
/// A rational start (Abramowitz and Stegun 26.2.23, absolute error below
/// 4.5·10⁻⁴) refined by Halley's method on `Φ` computed from `erfc`, whose
/// convergence is cubic: three steps reach the precision of `erfc`.
pub fn normal_quantile(p: f64, q: f64) -> f64 {
    if p > q {
        return -normal_quantile(q, p);
    }
    // The lower half: p ≤ ½.
    let t = (-2.0 * ln(p)).sqrt();
    let start = t
        - (2.515517 + t * (0.802853 + t * 0.010328))
            / (1.0 + t * (1.432788 + t * (0.189269 + t * 0.001308)));
    let mut x = -start;
    // Near the centre `Φ(x) − p` would be a difference of two numbers near ½;
    // there it is `½ erf(x/√2) − (p − ½)`, both small and `p − ½` exact.
    let central = p > 0.25;
    for _ in 0..3 {
        let e = if central {
            0.5 * erf(x * std::f64::consts::FRAC_1_SQRT_2) - (p - 0.5)
        } else {
            normal_cdf(x) - p
        };
        let u = e * (2.0 * std::f64::consts::PI).sqrt() * exp(x * x / 2.0);
        x -= u / (1.0 + x * u / 2.0);
    }
    x
}

/// The relative precision a series or a continued fraction is run to.
const EPSILON: f64 = f64::EPSILON;

/// The smallest positive normal number, kept away from 0 in Lentz's method.
const TINY: f64 = f64::MIN_POSITIVE / EPSILON;

/// Enough terms for any argument this crate passes: the series and the
/// continued fraction converge in O(√a) terms near `x ≈ a`.
const MAX_TERMS: u32 = 1_000_000;

/// The regularized lower incomplete gamma function, `P(a, x)`, for `a > 0`,
/// `x ≥ 0`.
pub fn gamma_p(a: f64, x: f64) -> f64 {
    if x <= 0.0 {
        0.0
    } else if x < a + 1.0 {
        gamma_series(a, x)
    } else {
        1.0 - gamma_fraction(a, x)
    }
}

/// The regularized upper incomplete gamma function, `Q(a, x) = 1 − P(a, x)`,
/// computed directly where it is the small one.
pub fn gamma_q(a: f64, x: f64) -> f64 {
    if x <= 0.0 {
        1.0
    } else if x < a + 1.0 {
        1.0 - gamma_series(a, x)
    } else {
        gamma_fraction(a, x)
    }
}

/// `exp(−x + a ln x − ln Γ(a))`, the common factor of both forms: `a` times the
/// Poisson mass of `a` at mean `x`, in its cancelling form.
fn gamma_front(a: f64, x: f64) -> f64 {
    exp(ln(a) + ln_poisson_mass(a, x))
}

/// `P(a, x)` by its series, for `x < a + 1`.
fn gamma_series(a: f64, x: f64) -> f64 {
    let mut term = 1.0 / a;
    let mut sum = term;
    let mut ap = a;
    for _ in 0..MAX_TERMS {
        ap += 1.0;
        term *= x / ap;
        sum += term;
        if term.abs() < sum.abs() * EPSILON {
            break;
        }
    }
    sum * gamma_front(a, x)
}

/// `Q(a, x)` by its continued fraction (modified Lentz), for `x ≥ a + 1`.
fn gamma_fraction(a: f64, x: f64) -> f64 {
    let mut b = x + 1.0 - a;
    let mut c = 1.0 / TINY;
    let mut d = 1.0 / b;
    let mut h = d;
    let mut i = 0.0;
    for _ in 0..MAX_TERMS {
        i += 1.0;
        let an = -i * (i - a);
        b += 2.0;
        d = an * d + b;
        if d.abs() < TINY {
            d = TINY;
        }
        c = b + an / c;
        if c.abs() < TINY {
            c = TINY;
        }
        d = 1.0 / d;
        let delta = d * c;
        h *= delta;
        if (delta - 1.0).abs() < EPSILON {
            break;
        }
    }
    h * gamma_front(a, x)
}

/// The regularized incomplete beta function, `I_x(a, b)`, for `a, b > 0` and
/// `0 ≤ x ≤ 1`.
pub fn beta_i(a: f64, b: f64, x: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    if x >= 1.0 {
        return 1.0;
    }
    // Γ(a + b) / (Γ(a) Γ(b)) · xᵃ (1 − x)ᵇ: `ab / (a + b)` times the binomial mass
    // of `a` in `a + b` trials of probability `x`, in its cancelling form.
    let front = exp(ln(a) + ln(b) - ln(a + b) + ln_binomial_mass(a, a + b, x));
    // The continued fraction converges fast below the mean; above it, by
    // symmetry, `I_x(a, b) = 1 − I_{1−x}(b, a)`.
    if x < (a + 1.0) / (a + b + 2.0) {
        front * beta_fraction(a, b, x) / a
    } else {
        1.0 - front * beta_fraction(b, a, 1.0 - x) / b
    }
}

/// The continued fraction of `I_x(a, b)` (modified Lentz).
fn beta_fraction(a: f64, b: f64, x: f64) -> f64 {
    let qab = a + b;
    let qap = a + 1.0;
    let qam = a - 1.0;
    let mut c = 1.0;
    let mut d = 1.0 - qab * x / qap;
    if d.abs() < TINY {
        d = TINY;
    }
    d = 1.0 / d;
    let mut h = d;
    let mut m = 0.0;
    for _ in 0..MAX_TERMS {
        m += 1.0;
        let m2 = 2.0 * m;
        let even = m * (b - m) * x / ((qam + m2) * (a + m2));
        d = 1.0 + even * d;
        if d.abs() < TINY {
            d = TINY;
        }
        c = 1.0 + even / c;
        if c.abs() < TINY {
            c = TINY;
        }
        d = 1.0 / d;
        h *= d * c;
        let odd = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2));
        d = 1.0 + odd * d;
        if d.abs() < TINY {
            d = TINY;
        }
        c = 1.0 + odd / c;
        if c.abs() < TINY {
            c = TINY;
        }
        d = 1.0 / d;
        let delta = d * c;
        h *= delta;
        if (delta - 1.0).abs() < EPSILON {
            break;
        }
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Within two units in the last place where every argument of Stirling's
    /// error is in its series and below 50 — so a term of the series above the
    /// last bit is seen — and within 10⁻¹⁴ elsewhere: the difference form below
    /// 10 is good to a few 10⁻¹⁵, and beyond 50 the error grows as `ε√x`.
    fn close(got: f64, expected: f64, near: bool) -> bool {
        if near {
            got.signum() == expected.signum() && got.to_bits().abs_diff(expected.to_bits()) <= 2
        } else {
            (got - expected).abs() <= 1e-14 * expected.abs().max(1.0)
        }
    }

    /// The masses in Loader's form against mpmath at 50 digits, the edges (x
    /// or k at 0, k at n) and Stirling's series at its boundary (x = 10)
    /// included.
    #[test]
    fn the_log_masses_match_the_reference() {
        let poisson: &[(f64, f64, f64)] = &[
            (0.0, 0.5, -0.5),
            (0.0, 30.0, -30.0),
            (1.0, 2.5, -1.583709268125845),
            (3.0, 2.5, -1.5428872736055899),
            (9.0, 9.5, -2.040201292623013),
            (10.0, 10.0, -2.0785616431350586),
            (12.0, 11.0, -2.2124712220814398),
            (15.0, 15.0, -2.2785183673077407),
            (20.0, 21.0, -2.445167706285025),
            (40.0, 38.0, -2.8171933257019646),
            (30.0, 30.0, -2.622314898965503),
            (1000.0, 990.0, -4.423235359527738),
            (100000.0, 100007.0, -6.675647087590387),
        ];
        for &(x, lambda, expected) in poisson {
            let got = ln_poisson_mass(x, lambda);
            assert!(
                close(got, expected, (10.0..50.0).contains(&x)),
                "poisson ({x}, {lambda}): {got} vs {expected}"
            );
        }
        let binomial: &[(f64, f64, f64, f64)] = &[
            (0.0, 100.0, 0.3, -35.66749439387324),
            (100.0, 100.0, 0.3, -120.3972804325936),
            (1.0, 10.0, 0.5, -4.628886712605407),
            (10.0, 20.0, 0.5, -1.7361522965964518),
            (30.0, 100.0, 0.3, -2.444334564532973),
            (11.0, 25.0, 0.4, -1.9206805889716227),
            (50.0, 120.0, 0.4, -2.6767527935989044),
            (4990.0, 10000.0, 0.5, -4.850984552095466),
            (10000.0, 1000000.0, 0.01, -5.519091885441095),
        ];
        for &(k, n, p, expected) in binomial {
            let got = ln_binomial_mass(k, n, p);
            assert!(
                close(got, expected, k >= 10.0 && n - k >= 10.0 && n < 150.0),
                "binomial ({k}, {n}, {p}): {got} vs {expected}"
            );
        }
    }
}
