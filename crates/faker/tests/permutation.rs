//! P1 of ADR-0045: a keyed permutation and its inverse.

use std::collections::BTreeSet;

use ascetic_ddd_faker::testing::chi_square;
use ascetic_ddd_faker::{Key, Permutation, Seed};

fn permutation(n: u64, seed: u64) -> Permutation {
    Permutation::new(n, Key::root(Seed(seed)).child("arrival"))
}

/// Every size from the degenerate ones to past a power of two: a bijection of
/// `0..n`, inverted by `position_of`, nothing past the end.
#[test]
fn it_is_a_bijection_with_an_inverse() {
    for n in [
        0,
        1,
        2,
        3,
        4,
        5,
        7,
        8,
        63,
        64,
        65,
        1_000,
        4_099,
        1 << 16,
        100_000,
    ] {
        let p = permutation(n, 1);
        assert_eq!(p.len(), n);
        let mut seen = BTreeSet::new();
        for position in 0..n {
            let value = p.at(position).expect("inside");
            assert!(value < n, "n = {n}: {position} -> {value}");
            assert!(seen.insert(value), "n = {n}: {value} twice");
            assert_eq!(p.position_of(value), Some(position), "n = {n}");
        }
        assert_eq!(p.at(n), None);
        assert_eq!(p.position_of(n), None);
    }
}

/// The largest domain: a 64-bit population still permutes, both ways.
#[test]
fn it_reaches_the_largest_sizes() {
    assert!(permutation(0, 2).is_empty());
    assert!(!permutation(1, 2).is_empty());
    let p = permutation(u64::MAX, 2);
    for position in [0, 1, u64::MAX / 2, u64::MAX - 1] {
        let value = p.at(position).unwrap();
        assert_eq!(p.position_of(value), Some(position));
    }
}

/// Over many keys, the first position lands uniformly: a permutation of 1,000
/// drawn from 20,000 keys, its first value counted in 20 bins.
/// The negative control: the identity, which is a bijection too.
#[test]
fn different_keys_draw_different_orders() {
    let bins = |first: &dyn Fn(u64) -> u64| {
        let mut counts = [0u64; 20];
        for seed in 0..20_000 {
            counts[(first(seed) / 50) as usize] += 1;
        }
        chi_square(&counts, &[1_000.0; 20]).unwrap().p_value
    };
    let alpha = 1e-4;
    assert!(bins(&|seed| permutation(1_000, seed).at(0).unwrap()) > alpha);
    assert!(
        bins(&|_| 0) < alpha,
        "the identity's first value is always 0"
    );
    // Not the identity either: few fixed points (about one in a random one).
    let p = permutation(1_000, 3);
    let fixed = (0..1_000).filter(|i| p.at(*i) == Some(*i)).count();
    assert!(fixed < 10, "{fixed} fixed points");
}
