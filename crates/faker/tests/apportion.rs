//! A1 of ADR-0045: a fixed total split by weights.

use ascetic_ddd_faker::law::Categorical;
use ascetic_ddd_faker::testing::stratified;
use ascetic_ddd_faker::{Key, Seed, Unit, apportion};
use proptest::prelude::*;

/// The share of the items before `i`, `total · Tᵢ / 2⁵²`, as an exact fraction
/// over 2⁵²: the oracle, written apart from the implementation.
fn share(total: u64, weights: &Categorical, i: usize) -> u128 {
    // Tᵢ is the categorical's cdf below item i, an exact multiple of 2⁻⁵².
    let t = if i == 0 {
        0
    } else {
        use ascetic_ddd_faker::Law;
        (weights.cdf(&(i as u64 - 1)) * (1u64 << 52) as f64) as u128
    };
    u128::from(total) * t
}

proptest! {
    /// The counts sum to the total, each within 1 of its exact share.
    #[test]
    fn counts_are_exact_in_sum_and_close_each(
        total in 0u64..=u64::MAX / 2,
        weights in proptest::collection::vec(0u64..1000, 1..20),
        k in 0u64..=Unit::MAX_RAW,
    ) {
        prop_assume!(weights.iter().any(|w| *w > 0));
        let law = Categorical::from_counts(&weights).unwrap();
        let split = apportion(total, law.clone(), Unit::from_raw(k).unwrap());
        let counts: Vec<u64> = split.counts().collect();
        prop_assert_eq!(counts.iter().map(|c| u128::from(*c)).sum::<u128>(), u128::from(total));
        prop_assert_eq!(split.start(0), 0);
        prop_assert_eq!(split.start(weights.len()), total);
        for (i, c) in counts.iter().enumerate() {
            let exact = share(total, &law, i + 1) - share(total, &law, i); // × 2⁵²
            let scaled = u128::from(*c) << 52;
            prop_assert!(scaled.abs_diff(exact) < 1u128 << 52, "item {}: {} vs {}", i, c, exact as f64 / (1u64 << 52) as f64);
        }
    }
}

/// Averaged over the unit, each item gets its share exactly: over a stratified
/// grid of M units, within 1/M of it.
#[test]
fn on_average_each_item_gets_its_share() {
    const M: u64 = 4_096;
    let law = Categorical::from_counts(&[3, 1, 1, 2]).unwrap();
    let total = 10;
    let mut sums = [0u64; 4];
    for u in stratified(M) {
        let split = apportion(total, law.clone(), u);
        assert_eq!(
            (split.total(), split.len(), split.is_empty()),
            (total, 4, false)
        );
        for (i, c) in split.counts().enumerate() {
            sums[i] += c;
        }
    }
    for (i, share) in [30.0 / 7.0, 10.0 / 7.0, 10.0 / 7.0, 20.0 / 7.0]
        .into_iter()
        .enumerate()
    {
        let mean = sums[i] as f64 / M as f64;
        assert!(
            (mean - share).abs() <= 1.0 / M as f64,
            "item {i}: {mean} vs {share}"
        );
    }
}

/// Across scopes, each with its own unit, no item is favoured: 10 split over
/// three equal branches in 10⁴ Tenants gives the first branch 10⁴ · 10/3, not
/// the 4 · 10⁴ that largest remainders (the first items take the extra one)
/// would give it. Each count is 3 or 4, so the sum's standard deviation is 47.
#[test]
fn no_item_is_favoured_across_scopes() {
    let law = Categorical::from_counts(&[1, 1, 1]).unwrap();
    let root = Key::root(Seed(11)).child("tenant");
    let first: u64 = (0..10_000u64)
        .map(|t| apportion(10, law.clone(), root.at(t).unit()).count(0))
        .sum();
    assert!(first.abs_diff(33_333) <= 5 * 47, "{first}");
}

#[test]
fn starts_tile_the_total() {
    let law = Categorical::from_weights(&[0.5, 0.0, 0.25, 0.25]).unwrap();
    let split = apportion(1_000_003, law, Unit::from_raw(Unit::MAX_RAW / 3).unwrap());
    let starts: Vec<u64> = (0..=4).map(|i| split.start(i)).collect();
    assert!(starts.windows(2).all(|w| w[0] <= w[1]), "{starts:?}");
    assert_eq!(split.count(1), 0, "a zero weight gets nothing");
    assert_eq!(starts[4], 1_000_003);
}
