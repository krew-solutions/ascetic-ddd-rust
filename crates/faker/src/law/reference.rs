//! L1 of ADR-0045: the laws and the special functions against values computed
//! outside the crate, by mpmath at 50 digits (`vectors/law_vectors.py`, whose
//! docstring holds the command). Regenerated tables replace these as a whole.

use super::*;
use crate::key::Unit;
use crate::math;

/// `(law, k, quantile at (k + ½) / 2⁵²)`, the quantile the double nearest the exact one.
const CONTINUOUS: &[(&str, u64, f64)] = &[
    ("exponential(rate=2.5)", 0, 4.4408920985006264e-17),
    ("exponential(rate=2.5)", 1, 1.332267629550188e-16),
    ("exponential(rate=2.5)", 4503599627, 4.000002000116356e-07),
    (
        "exponential(rate=2.5)",
        4503599627370,
        0.00040020013343341376,
    ),
    (
        "exponential(rate=2.5)",
        450359962737049,
        0.042144206263130514,
    ),
    (
        "exponential(rate=2.5)",
        2251799813685248,
        0.2772588722239782,
    ),
    (
        "exponential(rate=2.5)",
        4053239664633447,
        0.9210340371976192,
    ),
    (
        "exponential(rate=2.5)",
        4499096027743126,
        2.7631021115929433,
    ),
    ("exponential(rate=2.5)", 4503595123770868, 5.526204223174207),
    (
        "exponential(rate=2.5)",
        4503599627370494,
        14.255275312403597,
    ),
    ("exponential(rate=2.5)", 4503599627370495, 14.69472022787084),
    ("pareto(scale=1, shape=1.16)", 0, 1.0),
    ("pareto(scale=1, shape=1.16)", 1, 1.0000000000000002),
    (
        "pareto(scale=1, shape=1.16)",
        4503599627,
        1.0000008620697682,
    ),
    (
        "pareto(scale=1, shape=1.16)",
        4503599627370,
        1.0008628723479047,
    ),
    (
        "pareto(scale=1, shape=1.16)",
        450359962737049,
        1.0950806688520889,
    ),
    (
        "pareto(scale=1, shape=1.16)",
        2251799813685248,
        1.8176431200757235,
    ),
    (
        "pareto(scale=1, shape=1.16)",
        4053239664633447,
        7.2789538439831665,
    ),
    (
        "pareto(scale=1, shape=1.16)",
        4499096027743126,
        385.6620421164207,
    ),
    (
        "pareto(scale=1, shape=1.16)",
        4503595123770868,
        148735.21072566407,
    ),
    (
        "pareto(scale=1, shape=1.16)",
        4503599627370494,
        22011278660794.59,
    ),
    (
        "pareto(scale=1, shape=1.16)",
        4503599627370495,
        56748802089643.98,
    ),
    ("pareto(scale=3, shape=2.5)", 0, 3.0),
    ("pareto(scale=3, shape=2.5)", 1, 3.0000000000000004),
    ("pareto(scale=3, shape=2.5)", 4503599627, 3.00000120000084),
    (
        "pareto(scale=3, shape=2.5)",
        4503599627370,
        3.001200840672572,
    ),
    (
        "pareto(scale=3, shape=2.5)",
        450359962737049,
        3.129134644531898,
    ),
    (
        "pareto(scale=3, shape=2.5)",
        2251799813685248,
        3.958523732318683,
    ),
    (
        "pareto(scale=3, shape=2.5)",
        4053239664633447,
        7.535659294528748,
    ),
    (
        "pareto(scale=3, shape=2.5)",
        4499096027743126,
        47.54679577383761,
    ),
    (
        "pareto(scale=3, shape=2.5)",
        4503595123770868,
        753.5659294442063,
    ),
    (
        "pareto(scale=3, shape=2.5)",
        4503599627370494,
        4657025.981974672,
    ),
    (
        "pareto(scale=3, shape=2.5)",
        4503599627370495,
        7226985.157736226,
    ),
    ("lognormal(mu=0, sigma=1)", 0, 0.0002720468805377755),
    ("lognormal(mu=0, sigma=1)", 1, 0.0003107347187607569),
    ("lognormal(mu=0, sigma=1)", 4503599627, 0.008622119793521591),
    (
        "lognormal(mu=0, sigma=1)",
        4503599627370,
        0.04549138524765352,
    ),
    (
        "lognormal(mu=0, sigma=1)",
        450359962737049,
        0.27760624185200977,
    ),
    (
        "lognormal(mu=0, sigma=1)",
        2251799813685248,
        1.0000000000000002,
    ),
    (
        "lognormal(mu=0, sigma=1)",
        4053239664633447,
        3.6022244792791627,
    ),
    (
        "lognormal(mu=0, sigma=1)",
        4499096027743126,
        21.98218397958428,
    ),
    (
        "lognormal(mu=0, sigma=1)",
        4503595123770868,
        115.9807592503378,
    ),
    (
        "lognormal(mu=0, sigma=1)",
        4503599627370494,
        3218.1791722151497,
    ),
    (
        "lognormal(mu=0, sigma=1)",
        4503599627370495,
        3675.8370396426703,
    ),
    ("lognormal(mu=2, sigma=0.5)", 0, 0.12187393650674593),
    ("lognormal(mu=2, sigma=0.5)", 1, 0.13025183605387944),
    ("lognormal(mu=2, sigma=0.5)", 4503599627, 0.6861135402325438),
    (
        "lognormal(mu=2, sigma=0.5)",
        4503599627370,
        1.5759903162668707,
    ),
    (
        "lognormal(mu=2, sigma=0.5)",
        450359962737049,
        3.893171874291367,
    ),
    (
        "lognormal(mu=2, sigma=0.5)",
        2251799813685248,
        7.389056098930651,
    ),
    (
        "lognormal(mu=2, sigma=0.5)",
        4053239664633447,
        14.024079027613492,
    ),
    (
        "lognormal(mu=2, sigma=0.5)",
        4499096027743126,
        34.64370908222043,
    ),
    (
        "lognormal(mu=2, sigma=0.5)",
        4503595123770868,
        79.57596932810762,
    ),
    (
        "lognormal(mu=2, sigma=0.5)",
        4503599627370494,
        419.1737459313771,
    ),
    (
        "lognormal(mu=2, sigma=0.5)",
        4503599627370495,
        447.9887299785557,
    ),
    ("weibull(scale=1, shape=1.5)", 0, 2.3099725541661604e-11),
    ("weibull(scale=1, shape=1.5)", 1, 4.804936541614919e-11),
    (
        "weibull(scale=1, shape=1.5)",
        4503599627,
        0.00010000003333526982,
    ),
    (
        "weibull(scale=1, shape=1.5)",
        4503599627370,
        0.010003335279136846,
    ),
    (
        "weibull(scale=1, shape=1.5)",
        450359962737049,
        0.22307552563691704,
    ),
    (
        "weibull(scale=1, shape=1.5)",
        2251799813685248,
        0.7832197687746515,
    ),
    (
        "weibull(scale=1, shape=1.5)",
        4053239664633447,
        1.7437215135964128,
    ),
    (
        "weibull(scale=1, shape=1.5)",
        4499096027743126,
        3.627086912339554,
    ),
    (
        "weibull(scale=1, shape=1.5)",
        4503595123770868,
        5.757641580219785,
    ),
    (
        "weibull(scale=1, shape=1.5)",
        4503599627370494,
        10.829550029526455,
    ),
    (
        "weibull(scale=1, shape=1.5)",
        4503599627370495,
        11.05098239621667,
    ),
    ("weibull(scale=2, shape=0.5)", 0, 2.4651903288156624e-32),
    ("weibull(scale=2, shape=0.5)", 1, 2.2186712959340966e-31),
    (
        "weibull(scale=2, shape=0.5)",
        4503599627,
        2.000002000116856e-12,
    ),
    (
        "weibull(scale=2, shape=0.5)",
        4503599627370,
        2.0020018350015272e-06,
    ),
    (
        "weibull(scale=2, shape=0.5)",
        450359962737049,
        0.022201676519366113,
    ),
    (
        "weibull(scale=2, shape=0.5)",
        2251799813685248,
        0.9609060278364034,
    ),
    (
        "weibull(scale=2, shape=0.5)",
        4053239664633447,
        10.603796220956818,
    ),
    (
        "weibull(scale=2, shape=0.5)",
        4499096027743126,
        95.43416598861728,
    ),
    (
        "weibull(scale=2, shape=0.5)",
        4503595123770868,
        381.73666395285557,
    ),
    (
        "weibull(scale=2, shape=0.5)",
        4503599627370494,
        2540.160927905293,
    ),
    (
        "weibull(scale=2, shape=0.5)",
        4503599627370495,
        2699.1850321924558,
    ),
    ("normal", 0, -8.209536151601387),
    ("normal", 1, -8.076571004130125),
    ("normal", 4503599627, -4.753424308817087),
    ("normal", 4503599627370, -3.090232306167813),
    ("normal", 450359962737049, -1.2815515655446006),
    ("normal", 2251799813685248, 2.782916424671767e-16),
    ("normal", 4053239664633447, 1.281551565544602),
    ("normal", 4499096027743126, 3.0902323061678794),
    ("normal", 4503595123770868, 4.753424308817087),
    ("normal", 4503599627370494, 8.076571004130125),
    ("normal", 4503599627370495, 8.209536151601387),
];

/// `(law, k, the least x with P(X ≤ x) ≥ (k + ½) / 2⁵²)`.
const DISCRETE: &[(&str, u64, u64)] = &[
    ("poisson(mean=0.5)", 0, 0),
    ("poisson(mean=0.5)", 1, 0),
    ("poisson(mean=0.5)", 4503599627, 0),
    ("poisson(mean=0.5)", 4503599627370, 0),
    ("poisson(mean=0.5)", 450359962737049, 0),
    ("poisson(mean=0.5)", 2251799813685248, 0),
    ("poisson(mean=0.5)", 4053239664633447, 1),
    ("poisson(mean=0.5)", 4499096027743126, 4),
    ("poisson(mean=0.5)", 4503595123770868, 7),
    ("poisson(mean=0.5)", 4503599627370494, 14),
    ("poisson(mean=0.5)", 4503599627370495, 14),
    ("poisson(mean=30)", 0, 0),
    ("poisson(mean=30)", 1, 0),
    ("poisson(mean=30)", 4503599627, 8),
    ("poisson(mean=30)", 4503599627370, 15),
    ("poisson(mean=30)", 450359962737049, 23),
    ("poisson(mean=30)", 2251799813685248, 30),
    ("poisson(mean=30)", 4053239664633447, 37),
    ("poisson(mean=30)", 4499096027743126, 48),
    ("poisson(mean=30)", 4503595123770868, 59),
    ("poisson(mean=30)", 4503599627370494, 84),
    ("poisson(mean=30)", 4503599627370495, 85),
    ("poisson(mean=1000)", 0, 752),
    ("poisson(mean=1000)", 1, 756),
    ("poisson(mean=1000)", 4503599627, 853),
    ("poisson(mean=1000)", 4503599627370, 904),
    ("poisson(mean=1000)", 450359962737049, 960),
    ("poisson(mean=1000)", 2251799813685248, 1000),
    ("poisson(mean=1000)", 4053239664633447, 1041),
    ("poisson(mean=1000)", 4499096027743126, 1099),
    ("poisson(mean=1000)", 4503595123770868, 1154),
    ("poisson(mean=1000)", 4503599627370494, 1266),
    ("poisson(mean=1000)", 4503599627370495, 1270),
    ("poisson(mean=100000)", 0, 97415),
    ("poisson(mean=100000)", 1, 97457),
    ("poisson(mean=100000)", 4503599627, 98500),
    ("poisson(mean=100000)", 4503599627370, 99024),
    ("poisson(mean=100000)", 450359962737049, 99595),
    ("poisson(mean=100000)", 2251799813685248, 100000),
    ("poisson(mean=100000)", 4053239664633447, 100405),
    ("poisson(mean=100000)", 4499096027743126, 100979),
    ("poisson(mean=100000)", 4503595123770868, 101507),
    ("poisson(mean=100000)", 4503599627370494, 102565),
    ("poisson(mean=100000)", 4503599627370495, 102607),
    ("binomial(trials=100, p=0.3)", 0, 0),
    ("binomial(trials=100, p=0.3)", 1, 1),
    ("binomial(trials=100, p=0.3)", 4503599627, 10),
    ("binomial(trials=100, p=0.3)", 4503599627370, 17),
    ("binomial(trials=100, p=0.3)", 450359962737049, 24),
    ("binomial(trials=100, p=0.3)", 2251799813685248, 30),
    ("binomial(trials=100, p=0.3)", 4053239664633447, 36),
    ("binomial(trials=100, p=0.3)", 4499096027743126, 45),
    ("binomial(trials=100, p=0.3)", 4503595123770868, 53),
    ("binomial(trials=100, p=0.3)", 4503599627370494, 69),
    ("binomial(trials=100, p=0.3)", 4503599627370495, 70),
    ("binomial(trials=10000, p=0.5)", 0, 4590),
    ("binomial(trials=10000, p=0.5)", 1, 4596),
    ("binomial(trials=10000, p=0.5)", 4503599627, 4762),
    ("binomial(trials=10000, p=0.5)", 4503599627370, 4845),
    ("binomial(trials=10000, p=0.5)", 450359962737049, 4936),
    ("binomial(trials=10000, p=0.5)", 2251799813685248, 5000),
    ("binomial(trials=10000, p=0.5)", 4053239664633447, 5064),
    ("binomial(trials=10000, p=0.5)", 4499096027743126, 5155),
    ("binomial(trials=10000, p=0.5)", 4503595123770868, 5238),
    ("binomial(trials=10000, p=0.5)", 4503599627370494, 5404),
    ("binomial(trials=10000, p=0.5)", 4503599627370495, 5410),
    ("binomial(trials=1000000, p=0.01)", 0, 9194),
    ("binomial(trials=1000000, p=0.01)", 1, 9207),
    ("binomial(trials=1000000, p=0.01)", 4503599627, 9531),
    ("binomial(trials=1000000, p=0.01)", 4503599627370, 9694),
    ("binomial(trials=1000000, p=0.01)", 450359962737049, 9873),
    ("binomial(trials=1000000, p=0.01)", 2251799813685248, 10000),
    ("binomial(trials=1000000, p=0.01)", 4053239664633447, 10128),
    ("binomial(trials=1000000, p=0.01)", 4499096027743126, 10309),
    ("binomial(trials=1000000, p=0.01)", 4503595123770868, 10476),
    ("binomial(trials=1000000, p=0.01)", 4503599627370494, 10814),
    ("binomial(trials=1000000, p=0.01)", 4503599627370495, 10828),
];

/// `(function, a, b, x, value)`.
const SPECIAL: &[(&str, f64, f64, f64, f64)] = &[
    ("gamma_p", 0.5, 0.0, 0.1, 0.34527915398142295),
    ("gamma_q", 0.5, 0.0, 0.1, 0.654720846018577),
    ("gamma_p", 1.0, 0.0, 1.0, 0.6321205588285577),
    ("gamma_q", 1.0, 0.0, 1.0, 0.36787944117144233),
    ("gamma_p", 5.0, 0.0, 2.0, 0.05265301734371116),
    ("gamma_q", 5.0, 0.0, 2.0, 0.9473469826562888),
    ("gamma_p", 30.0, 0.0, 31.0, 0.5953478209656109),
    ("gamma_q", 30.0, 0.0, 31.0, 0.4046521790343891),
    ("gamma_p", 1000.0, 0.0, 990.0, 0.3795213785379639),
    ("gamma_q", 1000.0, 0.0, 990.0, 0.620478621462036),
    ("gamma_p", 100001.0, 0.0, 100000.0, 0.49915895690065987),
    ("gamma_q", 100001.0, 0.0, 100000.0, 0.5008410430993401),
    ("beta_i", 1.0, 1.0, 0.3, 0.3),
    ("beta_i", 2.0, 5.0, 0.2, 0.34464),
    ("beta_i", 70.0, 31.0, 0.7, 0.5491236007687904),
    ("beta_i", 5000.0, 5001.0, 0.5, 0.5039893230696911),
];

fn unit(k: u64) -> Unit {
    Unit::from_raw(k).expect("a 52-bit k")
}

fn continuous(name: &str) -> Box<dyn Fn(Unit) -> f64> {
    let law: Box<dyn Law<Value = f64>> = match name {
        "exponential(rate=2.5)" => Box::new(Exponential::new(2.5).unwrap()),
        "pareto(scale=1, shape=1.16)" => Box::new(Pareto::new(1.0, 1.16).unwrap()),
        "pareto(scale=3, shape=2.5)" => Box::new(Pareto::new(3.0, 2.5).unwrap()),
        "lognormal(mu=0, sigma=1)" => Box::new(LogNormal::new(0.0, 1.0).unwrap()),
        "lognormal(mu=2, sigma=0.5)" => Box::new(LogNormal::new(2.0, 0.5).unwrap()),
        "weibull(scale=1, shape=1.5)" => Box::new(Weibull::new(1.0, 1.5).unwrap()),
        "weibull(scale=2, shape=0.5)" => Box::new(Weibull::new(2.0, 0.5).unwrap()),
        "normal" => return Box::new(|u: Unit| math::normal_quantile(u.value(), u.complement())),
        other => panic!("no law {other}"),
    };
    assert_eq!(law.to_string(), name);
    Box::new(move |u| law.quantile(u))
}

fn discrete(name: &str) -> Box<dyn Law<Value = u64>> {
    let law: Box<dyn Law<Value = u64>> = match name {
        "poisson(mean=0.5)" => Box::new(Poisson::new(0.5).unwrap()),
        "poisson(mean=30)" => Box::new(Poisson::new(30.0).unwrap()),
        "poisson(mean=1000)" => Box::new(Poisson::new(1000.0).unwrap()),
        "poisson(mean=100000)" => Box::new(Poisson::new(100_000.0).unwrap()),
        "binomial(trials=100, p=0.3)" => Box::new(Binomial::new(100, 0.3).unwrap()),
        "binomial(trials=10000, p=0.5)" => Box::new(Binomial::new(10_000, 0.5).unwrap()),
        "binomial(trials=1000000, p=0.01)" => Box::new(Binomial::new(1_000_000, 0.01).unwrap()),
        other => panic!("no law {other}"),
    };
    assert_eq!(law.to_string(), name);
    law
}

/// The relative error of each continuous quantile, both tails and the middle,
/// within 10⁻¹³: a few ulps of `libm` composed two or three deep, and the
/// inverse normal's Halley steps.
#[test]
fn continuous_quantiles_match_the_reference() {
    let mut worst = 0.0f64;
    for (name, k, expected) in CONTINUOUS {
        let got = continuous(name)(unit(*k));
        let error = ((got - expected) / expected).abs();
        worst = worst.max(error);
        assert!(
            error <= 1e-13,
            "{name} at k = {k}: {got:e} vs {expected:e} ({error:e})"
        );
    }
    assert!(worst > 0.0, "the comparison ran");
}

/// Every discrete quantile exactly, both far tails included: Poisson(30) at
/// `1 − p = 5·10⁻¹⁶` is 84, which a walk without its exact re-anchoring missed.
#[test]
fn discrete_quantiles_match_the_reference() {
    for (name, k, expected) in DISCRETE {
        assert_eq!(
            discrete(name).quantile(unit(*k)),
            *expected,
            "{name} at k = {k}"
        );
    }
}

/// The incomplete gamma and beta functions within 10⁻¹² relative, at the
/// arguments the walks start from (`a ≈ x`, up to 10⁵).
#[test]
fn special_functions_match_the_reference() {
    for (name, a, b, x, expected) in SPECIAL {
        let got = match *name {
            "gamma_p" => math::gamma_p(*a, *x),
            "gamma_q" => math::gamma_q(*a, *x),
            "beta_i" => math::beta_i(*a, *b, *x),
            other => panic!("no function {other}"),
        };
        let error = ((got - expected) / expected).abs();
        assert!(
            error <= 1e-12,
            "{name}({a}, {b}, {x}) = {got:e} vs {expected:e} ({error:e})"
        );
    }
}
