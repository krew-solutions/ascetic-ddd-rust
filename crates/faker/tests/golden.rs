//! R4 of ADR-0045: the golden tables through the public helper a host calls in
//! its own build, in release, where its lock resolves `libm` and `siphasher`.

#[test]
fn the_golden_tables_hold_here() {
    if let Err(mismatch) = ascetic_ddd_faker::testing::check_golden() {
        panic!("{mismatch}");
    }
    assert_eq!(ascetic_ddd_faker::ALGORITHM.len(), 32);
}
