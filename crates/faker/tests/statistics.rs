//! T1 of ADR-0045: the statistical helpers against known values, and the
//! key's units against the uniform law, with a negative control.

use ascetic_ddd_faker::testing::{
    ChiSquareRefused, chi_square, ks_critical, ks_distance, stratified,
};
use ascetic_ddd_faker::{Key, Seed, Unit};

#[test]
fn chi_square_matches_its_tables() {
    // 60 against 50 and 40 against 50: χ² = 4 on one degree, p = erfc(√2).
    let t = chi_square(&[60, 40], &[50.0, 50.0]).unwrap();
    assert_eq!(t.statistic, 4.0);
    assert_eq!(t.degrees, 1);
    assert!(
        (t.p_value - 0.045_500_263_896_358_4).abs() < 1e-12,
        "{}",
        t.p_value
    );
    // χ² = 2 on one degree: p = erfc(1).
    let t = chi_square(&[0, 2], &[1.0, 1.0]).unwrap();
    assert!((t.p_value - 0.157_299_207_050_285_1).abs() < 1e-12);
    let t = chi_square(&[50, 50, 50, 50], &[50.0, 50.0, 50.0, 50.0]).unwrap();
    assert_eq!((t.statistic, t.p_value), (0.0, 1.0));
}

#[test]
fn chi_square_refuses_what_would_pass_anything() {
    assert_eq!(
        chi_square(&[1, 2], &[1.0, 0.0]),
        Err(ChiSquareRefused::Expected)
    );
    assert_eq!(
        chi_square(&[1, 2], &[1.0, f64::NAN]),
        Err(ChiSquareRefused::Expected)
    );
    assert_eq!(chi_square(&[1, 2], &[1.0]), Err(ChiSquareRefused::Shape));
    assert_eq!(chi_square(&[1], &[1.0]), Err(ChiSquareRefused::Shape));
    assert_eq!(
        ChiSquareRefused::Expected.to_string(),
        "an expected count is not positive and finite"
    );
}

/// The KS distance is the largest gap between the points' empirical function,
/// a step of 1/n at each, and the law's, on either side of each step.
#[test]
fn the_ks_distance_is_the_largest_gap_either_side_of_a_step() {
    assert_eq!(ks_distance(&[0.25, 0.75]), 0.25);
    assert_eq!(ks_distance(&[0.0, 0.0]), 1.0);
    assert_eq!(ks_distance(&[1.0, 1.0]), 1.0);
}

#[test]
fn the_ks_critical_distance_is_kolmogorov_s() {
    assert!((ks_critical(100, 0.05) - 0.135_810_5).abs() < 1e-6);
    assert!((ks_critical(10_000, 0.001) - 0.019_494_9).abs() < 1e-6);
}

#[test]
fn stratified_units_are_the_midpoints() {
    let raws: Vec<u64> = stratified(4).map(Unit::raw).collect();
    let quarter = 1u64 << 50;
    assert_eq!(
        raws,
        [
            quarter / 2,
            quarter * 3 / 2,
            quarter * 5 / 2,
            quarter * 7 / 2
        ]
    );
}

/// The units keys give are uniform: 2¹⁶ keys in 64 bins. The negative control,
/// units from 51 bits instead of 52 — half the interval — must fail it.
#[test]
fn a_key_s_unit_is_uniform() {
    let root = Key::root(Seed(5)).child("units");
    let bin = |raw: u64| (raw >> 46) as usize;
    let mut counts = [0u64; 64];
    let mut halved = [0u64; 64];
    for i in 0..(1u64 << 16) {
        let raw = root.at(i).unit().raw();
        counts[bin(raw)] += 1;
        halved[bin(raw >> 1)] += 1;
    }
    let expected = [(1u64 << 16) as f64 / 64.0; 64];
    let alpha = 1e-4;
    assert!(chi_square(&counts, &expected).unwrap().p_value > alpha);
    assert!(chi_square(&halved, &expected).unwrap().p_value < alpha);
}
