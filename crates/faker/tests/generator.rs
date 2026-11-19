//! C2, Cap1 and L7 of ADR-0045, and the combinators' behaviour. C1 and M2,
//! which need the key audit, are in `audit.rs`.

use ascetic_ddd_faker::generator::{
    Gen, GenExt, Refusal, apportioned, constant, draw, from_fn, replicate, skewed_count,
    weighted_count,
};
use ascetic_ddd_faker::law::{Categorical, Poisson, Uniform, UniformInt};
use ascetic_ddd_faker::testing::ks_critical;
use ascetic_ddd_faker::{Key, Seed};

fn keys(label: &str, n: u64) -> impl Iterator<Item = Key> {
    let root = Key::root(Seed(23)).child(label);
    (0..n).map(move |i| root.at(i))
}

/// The KS distance of a sample from the uniform law on [0, 1).
fn ks_uniform(mut sample: Vec<f64>) -> f64 {
    sample.sort_by(f64::total_cmp);
    let n = sample.len() as f64;
    sample
        .iter()
        .enumerate()
        .map(|(i, v)| {
            (v - i as f64 / n)
                .abs()
                .max(((i as f64 + 1.0) / n - v).abs())
        })
        .fold(0.0, f64::max)
}

#[test]
fn a_generator_is_a_function_of_its_key() {
    let g = draw(Poisson::new(30.0).unwrap()).map(|n| n * 2);
    let key = || Key::root(Seed(1)).child("x").at(4);
    assert_eq!(g.generate(key()).unwrap(), g.generate(key()).unwrap());
    assert_eq!(constant("a").generate(key()).unwrap(), "a");
    let boxed: Box<dyn Gen<Output = u64>> = Box::new(g);
    assert_eq!(boxed.generate(key()).unwrap() % 2, 0);
}

/// C2: `optional`'s present values follow the inner law alone — its coin is
/// drawn from a key the inner generator never sees. The negative control draws
/// the coin from the value's own key: present values crowd the bottom half.
#[test]
fn an_optional_value_is_the_inner_law_s() {
    const N: u64 = 40_000;
    let inner = draw(Uniform::new(0.0, 1.0).unwrap());
    let optional = inner.optional(0.5).unwrap();
    let present: Vec<f64> = keys("optional", N)
        .filter_map(|k| optional.generate(k).unwrap())
        .collect();
    let n = present.len() as u64;
    assert!(n.abs_diff(N / 2) <= 5 * 100, "{n} present of {N}"); // σ = 100
    let alpha = 1e-4;
    assert!(ks_uniform(present) <= ks_critical(n, alpha));

    let correlated = from_fn(|k: Key| {
        let u = k.child("v").unit().value();
        Ok((u < 0.5).then_some(u))
    });
    let present: Vec<f64> = keys("optional", N)
        .filter_map(|k| correlated.generate(k).unwrap())
        .collect();
    let n = present.len() as u64;
    assert!(ks_uniform(present) > ks_critical(n, alpha));
}

/// Wrapping a generator in `optional` or `map` leaves its values unchanged.
#[test]
fn wrapping_changes_no_value() {
    let inner = draw(Poisson::new(12.0).unwrap());
    let optional = draw(Poisson::new(12.0).unwrap()).optional(1.0).unwrap();
    let mapped = draw(Poisson::new(12.0).unwrap()).map(|n| n + 1);
    for k in keys("wrap", 100) {
        let alone = inner.generate(k.child("v")).unwrap();
        assert_eq!(optional.generate(k.child("v")).unwrap(), Some(alone));
        assert_eq!(mapped.generate(k.child("v")).unwrap(), alone + 1);
    }
    let never = draw(Poisson::new(12.0).unwrap()).optional(0.0).unwrap();
    assert!(keys("wrap", 100).all(|k| never.generate(k).unwrap().is_none()));
    assert!(draw(Poisson::new(1.0).unwrap()).optional(1.5).is_err());
}

/// C2: `replicate`'s items do not depend on the count: item `i` is the same
/// whatever count was drawn beside it, so seven items begin with the three.
#[test]
fn replicated_items_do_not_depend_on_the_count() {
    let item = || draw(UniformInt::new(0, 1_000_000).unwrap());
    let few = replicate(constant(3u64), item(), 100);
    let many = replicate(constant(7u64), item(), 100);
    for k in keys("replicate", 50) {
        let (a, b) = (
            few.generate(k.child("v")).unwrap(),
            many.generate(k.child("v")).unwrap(),
        );
        assert_eq!((a.len(), b.len()), (3, 7));
        assert_eq!(a[..], b[..3]);
    }
}

/// Cap1: a count above its cap is refused, not allocated.
#[test]
fn a_count_above_its_cap_is_refused() {
    let lists = replicate(draw(Poisson::new(100.0).unwrap()), constant(()), 50);
    let refusals = keys("cap", 200)
        .filter_map(|k| lists.generate(k).err())
        .collect::<Vec<Refusal>>();
    assert!(refusals.len() > 190, "{} refused", refusals.len());
    assert!(
        refusals[0].reason.contains("above its cap of 50"),
        "{}",
        refusals[0]
    );
    let under = replicate(draw(Poisson::new(10.0).unwrap()), constant(()), 1_000);
    assert!(keys("cap", 200).all(|k| under.generate(k).is_ok()));
}

/// A refusal names where it happened: the labels of the `and_then`s it passed.
#[test]
fn a_refusal_names_its_path() {
    let ten = || {
        draw(UniformInt::new(0, 9).unwrap()).try_map(|n| {
            if n < 10 {
                Err(format!("{n} is not ten"))
            } else {
                Ok(n)
            }
        })
    };
    let g = constant(1u8)
        .and_then("device", move |_| ten())
        .and_then("context", |_| constant(0u8));
    let refusal = g.generate(Key::root(Seed(2))).unwrap_err();
    assert_eq!(refusal.path, vec!["device"]);
    assert!(refusal.reason.ends_with("is not ten"), "{refusal}");
    assert!(refusal.to_string().starts_with("device: "), "{refusal}");
}

/// `and_then` hands its continuation the first value, under its own label.
#[test]
fn and_then_chooses_by_the_first_value() {
    let g = draw(UniformInt::new(1, 5).unwrap()).and_then("items", |n: &u64| {
        replicate(constant(*n), constant('x'), 10)
    });
    for k in keys("pipe", 100) {
        let items = g.generate(k).unwrap();
        assert!((1..=5).contains(&items.len()));
    }
}

/// L7: the skewed count's mean and variance are the mixture's —
/// `mean + mean² ((s + 1)² / (2s + 1) − 1)`, 2050 at a mean of 50 and `s = 2`.
/// The negative control, a plain Poisson of the same mean, has a variance of 50
/// and fails the variance check.
#[test]
fn a_skewed_count_has_the_mixture_s_moments() {
    const N: u64 = 200_000;
    let moments = |g: &dyn Gen<Output = u64>| {
        let draws: Vec<f64> = keys("skewed", N)
            .map(|k| g.generate(k).unwrap() as f64)
            .collect();
        let mean = draws.iter().sum::<f64>() / N as f64;
        let var = draws.iter().map(|x| (x - mean) * (x - mean)).sum::<f64>() / (N - 1) as f64;
        (mean, var)
    };
    let (mean, var) = moments(&skewed_count(50.0, 2.0).unwrap());
    assert!((mean - 50.0).abs() < 0.5, "{mean}");
    assert!((var - 2050.0).abs() < 0.05 * 2050.0, "{var}");
    let (_, plain) = moments(&draw(Poisson::new(50.0).unwrap()));
    assert!((plain - 2050.0).abs() > 0.05 * 2050.0);
    assert!(skewed_count(50.0, 0.5).is_err());
    assert!(
        skewed_count(1e9, 2.0).is_err(),
        "an owner's mean past the Poisson's limit"
    );
}

/// L7: the weighted count's parts have means `mean · m · wⱼ / Σw` — 140, 40, 14
/// and 6 here — so the mixture's mean is 50 and its variance 50 + 2858.
#[test]
fn a_weighted_count_has_the_mixture_s_moments() {
    const N: u64 = 200_000;
    let g = weighted_count(50.0, &[0.7, 0.2, 0.07, 0.03]).unwrap();
    let draws: Vec<f64> = keys("weighted", N)
        .map(|k| g.generate(k).unwrap() as f64)
        .collect();
    let mean = draws.iter().sum::<f64>() / N as f64;
    let var = draws.iter().map(|x| (x - mean) * (x - mean)).sum::<f64>() / (N - 1) as f64;
    assert!((mean - 50.0).abs() < 0.5, "{mean}");
    assert!((var - 2908.0).abs() < 0.05 * 2908.0, "{var}");
    assert!(weighted_count(50.0, &[]).is_err());
}

#[test]
fn apportioned_splits_at_the_key_s_unit() {
    let g = apportioned(1_000, Categorical::from_counts(&[1, 1, 2]).unwrap());
    let split = g.generate(Key::root(Seed(4)).child("tenant")).unwrap();
    assert_eq!(split.counts().sum::<u64>(), 1_000);
    assert_eq!(split.count(2), 500);
}
