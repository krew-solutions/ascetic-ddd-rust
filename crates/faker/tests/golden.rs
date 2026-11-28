//! R4 of ADR-0045: the golden tables through the public helper a host calls in
//! its own build, in release, where its lock resolves `libm` and `siphasher`.

#[test]
fn the_golden_tables_hold_here() {
    match ascetic_ddd_faker::testing::check_golden() {
        Ok(rows) => assert_eq!(rows, include_str!("../golden/tables.txt").lines().count()),
        Err(mismatch) => panic!("{mismatch}"),
    }
    assert_eq!(ascetic_ddd_faker::ALGORITHM.len(), 32);
}
