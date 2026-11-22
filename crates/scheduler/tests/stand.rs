//! K1c of ADR-0047: the machine against a model stand built from the host's
//! contracts, not from the decision — every clean corpus log ends with every
//! step expected under every delay that fails nothing, requests processed in
//! the order their delays make them arrive. K1e-3d: with one failure injected,
//! it is counted at its root only.

// The command log's fixture reads its corpus's directory; the lint gate refuses
// files to the machine, not to its tests (ADR-0047 K1e-3).
#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
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

/// What the fold makes of a run on the stand.
fn folded(
    header: &ascetic_ddd_command_log::record::Header,
    records: &[ascetic_ddd_command_log::record::Record],
    ran: &ascetic_ddd_scheduler::sim::Run,
) -> ascetic_ddd_scheduler::verdict::Folded {
    use ascetic_ddd_scheduler::journal;
    let text = String::from_utf8(bytes(header.clone(), records)).unwrap();
    let trailer: serde_json::Value = serde_json::from_str(text.lines().last().unwrap()).unwrap();
    let log = ascetic_ddd_scheduler::verdict::Log {
        sha256: trailer["trailer"]["sha256"].as_str().unwrap().to_owned(),
        records: records.len() as u64,
    };
    let h = journal::Header {
        journal: journal::JOURNAL_VERSION,
        machine: ascetic_ddd_scheduler::machine::MACHINE_REVISION,
        source: "stand".to_owned(),
        log: log.sha256.clone(),
        validator: ascetic_ddd_command_log::live::validator(),
        read_ahead: W,
        jitter: header.profile.jitter,
        resend: header.profile.resend,
        namespace: 1,
        t0: 1_763_640_000_000_000,
    };
    let lines = || ran.journal.iter().cloned().map(Ok);
    ascetic_ddd_scheduler::verdict::fold(&h, &log, lines(), lines())
}

/// K1e-3d of ADR-0047, the one oracle not written from decision 6: a clean
/// corpus log passes on the stand; with one failure injected — a request lost,
/// an answer lost, a refusal — at any line's first attempt, every other step
/// whose end it changed is never a root. A failure is counted at its root only.
#[test]
fn a_failure_is_counted_at_its_root_only() {
    use std::collections::BTreeMap;

    use ascetic_ddd_scheduler::sim::{Failure, run_stand_failing};
    use ascetic_ddd_scheduler::verdict::{Class, Verdict};
    let header = fixture::corpus::corpus_header();
    let mut runs = 0u64;
    let mut changed = BTreeMap::<&str, u64>::new();
    let mut findings = Vec::new();
    for (name, records) in fixture::corpus::logs() {
        let report = run_with(header.clone(), &records, &NoHost, 1_000);
        if !report.violations.is_empty() {
            continue;
        }
        let clean = run_stand(header.clone(), &records, W, &delays(1)).unwrap();
        assert_eq!(
            folded(&header, &records, &clean).verdict,
            Verdict::Pass,
            "{name}"
        );
        for k in 2..records.len() as u64 + 2 {
            for (kind, failure) in [
                ("lost request", Failure::LostRequest),
                ("lost answer", Failure::LostAnswer),
                ("refused", Failure::Refused),
            ] {
                let ran =
                    run_stand_failing(header.clone(), &records, W, &delays(1), Some((k, failure)))
                        .unwrap();
                let f = folded(&header, &records, &ran);
                runs += 1;
                assert!(
                    !matches!(
                        f.verdict,
                        Verdict::Void(ascetic_ddd_scheduler::verdict::Void::Incoherent { .. })
                    ),
                    "{name} line {k} {kind}: {:?}",
                    f.verdict
                );
                let mut differs = false;
                for (e, c) in ran.entries.iter().zip(&clean.entries) {
                    if e.end == c.end {
                        continue;
                    }
                    differs = true;
                    let class = f.classes[(e.line - 2) as usize];
                    if e.line == k {
                        assert!(
                            matches!(class, Class::Root | Class::Undecided),
                            "{name} line {k} {kind}: the failed step is {class:?}"
                        );
                    } else {
                        *changed.entry(kind).or_default() += 1;
                        if class == Class::Root {
                            findings.push(format!(
                                "{name}: {kind} at line {k} made line {} {:?} (clean {:?}), counted a root",
                                e.line, e.end, c.end
                            ));
                        }
                    }
                }
                if differs {
                    assert_ne!(f.verdict, Verdict::Pass, "{name} line {k} {kind}");
                }
            }
        }
    }
    eprintln!("{runs} runs; steps a failure changed besides its own: {changed:?}");
    assert!(
        findings.is_empty(),
        "{} second roots:\n{}",
        findings.len(),
        findings.join("\n")
    );
    assert!(runs > 1_000, "{runs} runs");
    for kind in ["lost request", "lost answer", "refused"] {
        assert!(
            changed.get(kind).copied().unwrap_or(0) > 0,
            "{kind}: {changed:?}"
        );
    }
}
