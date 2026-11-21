//! K1c of ADR-0047: the machine against a model stand built from the host's
//! contracts, not from the decision — every clean corpus log ends with every
//! step expected under every delay that fails nothing, requests processed in
//! the order their delays make them arrive.

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports)]
mod fixture;

use ascetic_ddd_command_log::validate::NoHost;
use ascetic_ddd_faker::{Key, Seed};
use ascetic_ddd_scheduler::machine::End;
use ascetic_ddd_scheduler::sim::run_stand;
use fixture::*;

/// Delays drawn from a key of (seed, line, attempt): until the bytes leave,
/// on the way there, on the way back — enough that requests overtake one
/// another, short enough that no round trip passes the corpus's shortest
/// deadline (an observation's 1.5 ms), so that nothing fails by delay.
fn delays(seed: u64) -> impl Fn(u64, u32) -> (u64, u64, u64) {
    move |line, attempt| {
        let k = || Key::root(Seed(seed)).at(line).at(u64::from(attempt));
        (
            k().child("sent").unit().raw() % 50,
            k().child("there").unit().raw() % 300,
            k().child("back").unit().raw() % 300,
        )
    }
}

#[test]
fn a_clean_log_ends_all_expected_on_the_model_stand() {
    let mut runs = 0;
    for (name, records) in fixture::corpus::logs() {
        let report = run_with(fixture::corpus::corpus_header(), &records, &NoHost, 1_000);
        if !report.violations.is_empty() {
            continue;
        }
        for seed in 1..=8u64 {
            let ran =
                run_stand(fixture::corpus::corpus_header(), &records, W, &delays(seed)).unwrap();
            for e in &ran.entries {
                assert_eq!(e.end, End::Expected, "{name}, seed {seed}, line {}", e.line);
            }
            assert_eq!(ran.left, ran.kept, "{name}, seed {seed}: cells left behind");
            runs += 1;
        }
    }
    assert!(runs > 400, "{runs} runs");
}
