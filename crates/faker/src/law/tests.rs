//! L2–L8 and M4 of ADR-0045: the laws against themselves, at their edges and on
//! a grid. L1, the laws against an independent implementation, is in
//! `reference.rs` beside this file.

use super::*;
use crate::key::Unit;
use crate::testing::{ks_distance, stratified};

/// The units every law is tried at: both extremes and their neighbours, the
/// middle, and a stratified grid.
fn units(grid: u64) -> Vec<Unit> {
    let edges = [
        0,
        1,
        2,
        Unit::MAX_RAW / 2,
        Unit::MAX_RAW - 2,
        Unit::MAX_RAW - 1,
        Unit::MAX_RAW,
    ];
    edges
        .into_iter()
        .filter_map(Unit::from_raw)
        .chain(stratified(grid))
        .collect()
}

fn continuous_laws() -> Vec<(String, Box<dyn Law<Value = f64>>)> {
    let laws: Vec<Box<dyn Law<Value = f64>>> = vec![
        Box::new(Uniform::new(-3.0, 7.5).unwrap()),
        Box::new(Exponential::new(2.5).unwrap()),
        Box::new(Exponential::new(1e-6).unwrap()),
        Box::new(Pareto::new(1.0, 1.16).unwrap()),
        Box::new(Pareto::new(3.0, 2.5).unwrap()),
        Box::new(LogNormal::new(0.0, 1.0).unwrap()),
        Box::new(LogNormal::new(2.0, 0.5).unwrap()),
        Box::new(Weibull::new(1.0, 1.5).unwrap()),
        Box::new(Weibull::new(2.0, 0.5).unwrap()),
        Box::new(Truncated::new(Pareto::new(1.0, 1.16).unwrap(), 1.0, 1000.0).unwrap()),
        Box::new(Truncated::new(LogNormal::new(0.0, 1.0).unwrap(), 0.5, 2.0).unwrap()),
    ];
    laws.into_iter().map(|l| (l.to_string(), l)).collect()
}

/// L2, continuous: the quantile inverts the law's own distribution function, to
/// a precision relative to the smaller tail.
#[test]
fn a_continuous_quantile_inverts_its_distribution_function() {
    for (name, law) in continuous_laws() {
        for u in units(2_000) {
            let (p, q) = (u.value(), u.complement());
            let f = law.cdf(&law.quantile(u));
            let tolerance = 1e-12 * p.min(q) + 4.0 * f64::EPSILON;
            assert!(
                (f - p).abs() <= tolerance,
                "{name} at k = {}: cdf(quantile) = {f:e}, p = {p:e}",
                u.raw()
            );
        }
    }
}

fn discrete_laws() -> Vec<(Box<dyn Law<Value = u64>>, f64)> {
    // (law, tolerance in probability: 0 where the law compares integers)
    vec![
        (Box::new(UniformInt::new(3, 17).unwrap()), 0.0),
        (Box::new(UniformInt::new(0, (1 << 52) - 1).unwrap()), 0.0),
        (
            Box::new(Categorical::from_weights(&[0.7, 0.2, 0.0, 0.07, 0.03]).unwrap()),
            0.0,
        ),
        (
            Box::new(Categorical::from_counts(&[5, 0, 1, 1, 3]).unwrap()),
            0.0,
        ),
        (
            Box::new(Empirical::from_histogram(&[(1, 10), (2, 0), (5, 3), (40, 1)]).unwrap()),
            0.0,
        ),
        (Box::new(Poisson::new(0.5).unwrap()), 1e-12),
        (Box::new(Poisson::new(30.0).unwrap()), 1e-12),
        (Box::new(Poisson::new(700.0).unwrap()), 1e-11),
        (Box::new(Poisson::new(745.5).unwrap()), 1e-11),
        (Box::new(Poisson::new(10_000.0).unwrap()), 1e-10),
        (Box::new(Poisson::new(30_000.0).unwrap()), 1e-10),
        (Box::new(Binomial::new(100, 0.3).unwrap()), 1e-12),
        (Box::new(Binomial::new(10_000, 0.5).unwrap()), 1e-10),
        (Box::new(Binomial::new(1_000_000, 0.01).unwrap()), 1e-9),
        (
            Box::new(Truncated::new(Poisson::new(50.0).unwrap(), 40, 70).unwrap()),
            1e-11,
        ),
    ]
}

/// L2, discrete: the quantile is the least value whose distribution function
/// reaches `p` — on both sides, so neither `q + 1` nor `q − 1` passes.
#[test]
fn a_discrete_quantile_is_the_generalised_inverse() {
    for (law, tolerance) in discrete_laws() {
        for u in units(2_000) {
            let p = u.value();
            let x = law.quantile(u);
            assert!(
                law.cdf(&x) >= p - tolerance,
                "{law} at k = {}: cdf({x}) = {} < p = {p}",
                u.raw(),
                law.cdf(&x)
            );
            assert!(
                law.cdf_below(&x) < p + tolerance,
                "{law} at k = {}: cdf({x} - 1) = {} >= p = {p}",
                u.raw(),
                law.cdf_below(&x)
            );
        }
    }
}

/// L3: over a stratified grid of N, a correct quantile's empirical law is
/// within 1/(2N) of the law; a clamp in place of a truncation piles the tail
/// beyond onto the bound as an atom (3·10⁻⁴ for this Pareto), six times that.
#[test]
fn the_grid_follows_the_law() {
    const N: u64 = 10_000;
    for (name, law) in continuous_laws() {
        let cdfs: Vec<f64> = stratified(N).map(|u| law.cdf(&law.quantile(u))).collect();
        let d = ks_distance(&cdfs);
        assert!(d <= 0.5 / N as f64 + 1e-9, "{name}: KS distance {d}");
    }
    // The negative control: the clamp the truncation replaced.
    let pareto = Pareto::new(1.0, 1.16).unwrap();
    let truncated = Truncated::new(pareto.clone(), 1.0, 1000.0).unwrap();
    let clamped: Vec<f64> = stratified(N)
        .map(|u| truncated.cdf(&pareto.quantile(u).min(1000.0)))
        .collect();
    assert!(ks_distance(&clamped) > 3.0 * (0.5 / N as f64));
}

/// L4 and M4: Poisson over its range of means, at the extreme units, ends in
/// O(√λ) steps — the walk from `e^(−λ)` would never end above λ ≈ 745 — and its
/// value lies where the law puts it.
#[test]
fn poisson_ends_at_every_mean() {
    for mean in [
        0.0, 1e-3, 1.0, 30.0, 700.0, 745.0, 800.0, 1e4, 3e4, 1e6, 1e9,
    ] {
        let law = Poisson::new(mean).unwrap();
        let bound = 20.0 * mean.sqrt() + 20.0;
        for u in units(16) {
            let (x, cost) = law.walk_from(u);
            assert!(
                cost.steps as f64 <= bound,
                "poisson({mean}) at k = {}: {cost:?}",
                u.raw()
            );
            // A tail of 10⁻¹⁶ is three million-fold falls from ½: at most three
            // exact evaluations, however wrong a running mass might be.
            assert!(
                cost.exact <= 3,
                "poisson({mean}) at k = {}: {cost:?}",
                u.raw()
            );
            // A tail of 10⁻¹⁶ lies within 15σ + 40 of the mean at every λ here:
            // 14 at λ = ½, about 85 at λ = 30.
            assert!(
                (x as f64 - mean).abs() <= 15.0 * mean.sqrt() + 40.0,
                "poisson({mean}) at k = {}: {x}",
                u.raw()
            );
        }
    }
}

#[test]
fn binomial_ends_at_every_size() {
    for (n, p) in [
        (1, 0.5),
        (100, 0.3),
        (10_000, 0.999),
        (1_000_000_000, 0.5),
        (1 << 40, 1e-9),
    ] {
        let law = Binomial::new(n, p).unwrap();
        let sigma = (n as f64 * p * (1.0 - p)).sqrt();
        for u in units(16) {
            let (x, cost) = law.walk_from(u);
            assert!(
                cost.steps as f64 <= 20.0 * sigma + 20.0,
                "binomial({n}, {p}): {cost:?}"
            );
            assert!(cost.exact <= 3, "binomial({n}, {p}): {cost:?}");
            assert!(x <= n);
            assert!((x as f64 - n as f64 * p).abs() <= 15.0 * sigma + 40.0);
        }
    }
    assert_eq!(
        Binomial::new(0, 0.5)
            .unwrap()
            .quantile(Unit::from_raw(7).unwrap()),
        0
    );
    assert_eq!(
        Binomial::new(9, 0.0)
            .unwrap()
            .quantile(Unit::from_raw(Unit::MAX_RAW).unwrap()),
        0
    );
    assert_eq!(
        Binomial::new(9, 1.0)
            .unwrap()
            .quantile(Unit::from_raw(0).unwrap()),
        9
    );
}

/// L4: at `s ≤ ½` and the largest unit, `u^s` rounds to 1.0; the index stays
/// inside the population.
#[test]
fn a_skewed_index_stays_inside() {
    for skew in [0.25, 0.5, 1.0, 3.0, 40.0] {
        let law = Skew::new(skew).unwrap();
        for n in [1, 2, 10, 1_000_000_000, u64::MAX] {
            for u in units(64) {
                assert!(law.index(u, n) < n, "skew({skew}) over {n}");
            }
            let lowest: Vec<u64> = (0..4)
                .map(|k| law.index(Unit::from_raw(k).unwrap(), n))
                .collect();
            assert!(
                lowest.windows(2).all(|w| w[0] <= w[1]),
                "skew({skew}) over {n}"
            );
            if n <= 10 {
                assert_eq!(lowest[0], 0);
            }
        }
    }
    let quarter = Skew::new(0.25).unwrap();
    assert_eq!(quarter.index(Unit::from_raw(Unit::MAX_RAW).unwrap(), 10), 9);
}

/// L5: each law moves the right way as its parameter does — which a consistent
/// inversion in both `quantile` and `cdf` would not.
#[test]
fn laws_move_with_their_parameters() {
    let u = Unit::from_raw(Unit::MAX_RAW / 3).unwrap();
    // A larger skew puts more picks on the first members.
    let skews: Vec<u64> = [1.0, 1.5, 2.0, 3.0]
        .into_iter()
        .map(|s| Skew::new(s).unwrap().index(u, 1_000))
        .collect();
    assert!(skews.windows(2).all(|w| w[0] > w[1]), "{skews:?}");
    let i = 99;
    let cdfs: Vec<f64> = [1.0, 2.0, 3.0]
        .into_iter()
        .map(|s| Skew::new(s).unwrap().cdf(i, 1_000))
        .collect();
    assert!(cdfs.windows(2).all(|w| w[0] < w[1]), "{cdfs:?}");
    // Python's table (test_skew_distributor.py): skew 2 at 0.1 is 0.316.
    assert!((Skew::new(2.0).unwrap().cdf(99, 1_000) - 0.1f64.sqrt()).abs() < 1e-12);
    // A higher rate, a shorter wait; a larger shape, a lighter tail; a larger
    // mean, a larger count.
    let wait = |r: f64| Exponential::new(r).unwrap().quantile(u);
    assert!(wait(1.0) > wait(2.0));
    let tail = |a: f64| Pareto::new(1.0, a).unwrap().quantile(u);
    assert!(tail(1.1) > tail(2.0));
    let count = |m: f64| Poisson::new(m).unwrap().quantile(u);
    assert!(count(10.0) < count(20.0));
    let spread = |s: f64| {
        LogNormal::new(0.0, s)
            .unwrap()
            .quantile(Unit::from_raw(Unit::MAX_RAW / 10 * 9).unwrap())
    };
    assert!(spread(0.5) < spread(1.0));
}

/// L6: the canonical forms a scenario's header records.
#[test]
fn laws_display_canonically() {
    let golden: Vec<(String, &str)> = vec![
        (
            Uniform::new(0.0, 1.5).unwrap().to_string(),
            "uniform(0, 1.5)",
        ),
        (
            Exponential::new(2.5).unwrap().to_string(),
            "exponential(rate=2.5)",
        ),
        (
            Pareto::new(1.0, 1.16).unwrap().to_string(),
            "pareto(scale=1, shape=1.16)",
        ),
        (
            LogNormal::new(-1.0, 0.5).unwrap().to_string(),
            "lognormal(mu=-1, sigma=0.5)",
        ),
        (
            Weibull::new(2.0, 0.5).unwrap().to_string(),
            "weibull(scale=2, shape=0.5)",
        ),
        (
            Truncated::new(Pareto::new(1.0, 2.0).unwrap(), 1.0, 1000.0)
                .unwrap()
                .to_string(),
            "truncated(pareto(scale=1, shape=2), 1, 1000)",
        ),
        (
            UniformInt::new(3, 17).unwrap().to_string(),
            "uniform_int(3, 17)",
        ),
        (
            Categorical::from_counts(&[1, 1, 2]).unwrap().to_string(),
            "categorical[1125899906842624,2251799813685248,4503599627370496]",
        ),
        (
            Empirical::from_histogram(&[(1, 1), (5, 3)])
                .unwrap()
                .to_string(),
            "empirical[1:1125899906842624,5:4503599627370496]",
        ),
        (Poisson::new(30.0).unwrap().to_string(), "poisson(mean=30)"),
        (
            Binomial::new(100, 0.3).unwrap().to_string(),
            "binomial(trials=100, p=0.3)",
        ),
        (UniformIndex.to_string(), "uniform_index"),
        (Skew::new(2.0).unwrap().to_string(), "skew(2)"),
        (
            Partitioned::new(&[3.0, 1.0]).unwrap().to_string(),
            "partitioned(categorical[3377699720527872,4503599627370496])",
        ),
    ];
    for (shown, expected) in golden {
        assert_eq!(shown, expected);
    }
    // Distinct laws, distinct forms: a rounded parameter or a dropped bound
    // would make two laws one header.
    assert_ne!(
        Pareto::new(1.0, 1.16).unwrap().to_string(),
        Pareto::new(1.0, 1.1600000001).unwrap().to_string()
    );
    assert_ne!(
        Truncated::new(Poisson::new(5.0).unwrap(), 0, 10)
            .unwrap()
            .to_string(),
        Truncated::new(Poisson::new(5.0).unwrap(), 0, 11)
            .unwrap()
            .to_string()
    );
}

/// L8: a categorical's thresholds at their boundaries; a zero weight is never
/// drawn; bad weights are refused.
#[test]
fn a_categorical_draws_exactly_by_its_thresholds() {
    let law = Categorical::from_weights(&[1.0, 0.0, 3.0]).unwrap();
    let t = UNITS / 4;
    let at = |k: u64| law.quantile(Unit::from_raw(k).unwrap());
    assert_eq!(at(0), 0);
    assert_eq!(at(t - 1), 0);
    assert_eq!(at(t), 2, "the zero weight's empty interval is skipped");
    assert_eq!(at(Unit::MAX_RAW), 2);
    assert!(stratified(10_000).all(|u| law.quantile(u) != 1));
    assert_eq!(law.cdf(&0), 0.25);
    assert_eq!(law.cdf(&1), 0.25);
    assert_eq!(law.cdf_below(&2), 0.25);
    assert_eq!(law.cdf(&2), 1.0);

    for bad in [
        &[][..],
        &[f64::NAN],
        &[-1.0, 2.0],
        &[0.0, 0.0],
        &[f64::INFINITY],
        &[f64::MAX, f64::MAX],
    ] {
        assert!(Categorical::from_weights(bad).is_err(), "{bad:?}");
    }
    assert!(Categorical::from_counts(&[]).is_err());
    assert!(Categorical::from_counts(&[0, 0]).is_err());
    assert!(
        Categorical::from_counts(&[u64::MAX; 4097]).is_err(),
        "a sum of 2^76 or more"
    );
    assert!(Empirical::from_histogram(&[(5, 1), (5, 1)]).is_err());
    assert!(Empirical::from_histogram(&[(5, 1), (4, 1)]).is_err());
}

#[test]
fn counts_give_exact_thresholds() {
    let law = Categorical::from_counts(&[1, 1, 2]).unwrap();
    let thresholds: Vec<u64> = (0..=3).map(|i| law.threshold(i)).collect();
    assert_eq!(thresholds, [0, UNITS / 4, UNITS / 2, UNITS]);
}

/// A categorical's split hands the inner position on as a unit of its own,
/// uniform inside the item: what lets one unit pick a part and a member of it.
#[test]
fn a_split_leaves_a_uniform_unit() {
    let law = Categorical::from_weights(&[0.7, 0.2, 0.1]).unwrap();
    for item in 0..3 {
        let inner: Vec<f64> = stratified(30_000)
            .map(|u| law.split(u))
            .filter(|(i, _)| *i == item)
            .map(|(_, v)| v.value())
            .collect();
        let n = inner.len() as f64;
        let d = inner
            .iter()
            .enumerate()
            .map(|(i, v)| {
                (v - i as f64 / n)
                    .abs()
                    .max(((i as f64 + 1.0) / n - v).abs())
            })
            .fold(0.0, f64::max);
        assert!(d <= 1.0 / n + 1e-9, "item {item}: {d}");
    }
}

/// The partitioned law gives each part its weight, members of a part alike;
/// with fewer members than parts, every index is still inside.
#[test]
fn a_partitioned_index_follows_its_weights() {
    let law = Partitioned::new(&[0.7, 0.2, 0.07, 0.03]).unwrap();
    assert!((law.cdf(24, 100) - 0.7).abs() < 1e-12);
    assert!((law.cdf(49, 100) - 0.9).abs() < 1e-12);
    assert!((law.cdf(12, 100) - 0.7 * 13.0 / 25.0).abs() < 1e-12);
    let mut counts = [0u64; 100];
    for u in stratified(100_000) {
        counts[law.index(u, 100) as usize] += 1;
    }
    let part = |r: std::ops::Range<usize>| counts[r].iter().sum::<u64>();
    assert!(part(0..25).abs_diff(70_000) <= 25);
    assert!(part(75..100).abs_diff(3_000) <= 25);
    // Members of a part alike: the member comes from the unit's position inside
    // the part's interval, not from the unit again (which would crowd part 0's
    // picks onto its first 70 %).
    assert!(
        counts[..25].iter().all(|c| c.abs_diff(2_800) <= 2),
        "{:?}",
        &counts[..25]
    );
    assert!(
        counts[75..].iter().all(|c| c.abs_diff(120) <= 2),
        "{:?}",
        &counts[75..]
    );
    for u in units(256) {
        assert!(law.index(u, 2) < 2);
        assert!(law.index(u, 1) == 0);
    }
}

#[test]
fn uniform_integers_reach_both_ends() {
    let law = UniformInt::new(7, 7 + (1 << 52) - 1).unwrap();
    assert_eq!(law.quantile(Unit::from_raw(0).unwrap()), 7);
    assert_eq!(
        law.quantile(Unit::from_raw(Unit::MAX_RAW).unwrap()),
        7 + Unit::MAX_RAW
    );
    assert!(UniformInt::new(0, 1 << 52).is_err(), "2^52 + 1 integers");
    assert!(UniformInt::new(0, u64::MAX).is_err());
    let small = UniformInt::new(3, 5).unwrap();
    assert_eq!(small.quantile(Unit::from_raw(0).unwrap()), 3);
    assert_eq!(small.quantile(Unit::from_raw(Unit::MAX_RAW).unwrap()), 5);
    assert!(UniformInt::new(5, 3).is_err());
}

#[test]
fn bad_parameters_are_refused() {
    assert!(Uniform::new(1.0, 1.0).is_err());
    assert!(Uniform::new(f64::NAN, 1.0).is_err());
    assert!(Exponential::new(0.0).is_err());
    assert!(Exponential::new(f64::INFINITY).is_err());
    assert!(Pareto::new(1.0, -1.0).is_err());
    assert!(LogNormal::new(0.0, 0.0).is_err());
    assert!(Weibull::new(f64::NAN, 1.0).is_err());
    assert!(Poisson::new(-1.0).is_err());
    assert!(Poisson::new(2e9).is_err());
    assert!(Binomial::new(10, 1.5).is_err());
    assert!(Binomial::new(BINOMIAL_MAX_TRIALS + 1, 0.5).is_err());
    assert!(Skew::new(0.0).is_err());
    assert!(Partitioned::new(&[]).is_err());
    assert!(Truncated::new(Pareto::new(1.0, 2.0).unwrap(), 5.0, 4.0).is_err());
    assert!(
        Truncated::new(Exponential::new(1.0).unwrap(), 800.0, 900.0).is_err(),
        "an interval beyond the last unit holds no probability"
    );
}

/// L8: a draw from a million items is a binary search, at most ⌈log₂ m⌉ + 1
/// comparisons, never Python's scan of every weight.
#[test]
fn a_draw_is_a_binary_search() {
    let m = 1_000_000;
    let law = Categorical::from_counts(&vec![1; m]).unwrap();
    for u in units(64) {
        let mut comparisons = 0u32;
        let item = law.locate(u.raw(), || comparisons += 1);
        assert_eq!(item as u64, law.quantile(u));
        assert!(comparisons <= 21, "{comparisons} comparisons");
    }
}
