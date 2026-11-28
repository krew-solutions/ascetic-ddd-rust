//! C1 and M2 of ADR-0045: the keys each combinator draws and passes on, and
//! what a generated member costs in derivations. Needs the `audit` feature,
//! which `make test` enables with `--all-features`.

#![cfg(feature = "audit")]

use ascetic_ddd_faker::generator::{Gen, GenExt, constant, draw, from_fn, replicate, skewed_count};
use ascetic_ddd_faker::law::{Categorical, Poisson, UniformInt};
use ascetic_ddd_faker::testing::{Audit, audit};
use ascetic_ddd_faker::{Key, Permutation, Seed};

fn key() -> Key {
    Key::root(Seed(31)).child("member").at(9)
}

fn audited<G: Gen>(g: &G) -> Audit {
    audit(|| g.generate(key()).map(|_| ())).1
}

/// C1: no combinator draws twice from one key, and each draws what its table
/// says.
#[test]
fn every_combinator_draws_from_distinct_keys() {
    let law = || Poisson::new(5.0).unwrap();
    let cases: Vec<(&str, Audit, u64)> = vec![
        ("draw", audited(&draw(law())), 1),
        ("map", audited(&draw(law()).map(|n| n + 1)), 1),
        (
            "optional, present",
            audited(&draw(law()).optional(1.0).unwrap()),
            2,
        ),
        (
            "optional, absent",
            audited(&draw(law()).optional(0.0).unwrap()),
            1,
        ),
        (
            "replicate",
            audited(&replicate(constant(3u64), draw(law()), 10)),
            3,
        ),
        (
            "replicate, drawn count",
            audited(&replicate(
                draw(UniformInt::new(4, 4).unwrap()),
                draw(law()),
                10,
            )),
            5,
        ),
        (
            "and_then",
            audited(&draw(law()).and_then("next", |_| draw(law()))),
            2,
        ),
        (
            "skewed count",
            audited(&skewed_count(20.0, 2.0).unwrap()),
            2,
        ),
    ];
    for (name, a, draws) in cases {
        assert_eq!(a.repeated, 0, "{name}: {a:?}");
        assert_eq!(a.draws, draws, "{name}: {a:?}");
    }
}

/// C1's negative controls: a generator that draws twice from one key, and two
/// `and_then`s nested under one label, are what the audit reports.
#[test]
fn the_audit_sees_a_key_drawn_twice() {
    let twice = from_fn(|k: Key| Ok((k.child("os").unit(), k.child("os").unit())));
    assert_eq!(audited(&twice).repeated, 1);
    let law = || Poisson::new(5.0).unwrap();
    let nested = draw(law())
        .and_then("next", move |_| draw(law()))
        .and_then("next", move |_| draw(law()));
    assert_eq!(audited(&nested).repeated, 1);
}

/// M2: a Device as the host's drills shape one costs at most 15 derivations.
#[test]
fn a_device_costs_few_derivations() {
    let os = Categorical::from_weights(&[0.5, 0.3, 0.2]).unwrap();
    let platform = Categorical::from_weights(&[0.7, 0.3]).unwrap();
    let oem = draw(Categorical::from_weights(&[0.4, 0.4, 0.2]).unwrap())
        .optional(0.8)
        .unwrap();
    let mode = Categorical::from_weights(&[0.9, 0.1]).unwrap();
    let facets = replicate(
        draw(UniformInt::new(0, 2).unwrap()),
        draw(UniformInt::new(0, 99).unwrap()),
        2,
    );
    let device = from_fn(move |k: Key| {
        Ok((
            draw(os.clone()).generate(k.child("os"))?,
            draw(platform.clone()).generate(k.child("platform"))?,
            oem.generate(k.child("oem"))?,
            draw(mode.clone()).generate(k.child("management_mode"))?,
            facets.generate(k.child("enrollment"))?,
        ))
    });
    let root = Key::root(Seed(31)).child("device").part(&2u64);
    for i in 0..100 {
        let (d, a) = audit(|| device.generate(root.at(i)));
        let (_, _, oem, _, facets) = d.unwrap();
        assert_eq!(a.repeated, 0);
        // `at`, five fields, the coin, the count: 8; and an `at` a facet.
        assert_eq!(a.derivations, 8 + facets.len() as u64, "device {i}: {a:?}");
        assert!(a.derivations <= 15);
        // os, platform, the coin, the mode and the count; the OEM when present; a
        // draw a facet.
        let expected = 5 + u64::from(oem.is_some()) + facets.len() as u64;
        assert_eq!(a.draws, expected, "device {i}: {a:?}");
    }
}

/// M2: a member placed by a permutation costs four derivations a Feistel pass,
/// and a pass per cycle-walk: about 4.2 on average where the domain barely
/// exceeds `n`, at most about 16 where it is four times `n`.
#[test]
fn a_placement_costs_four_derivations_a_pass() {
    let mean = |n: u64| {
        let p = Permutation::new(n, Key::root(Seed(3)).child("arrival"));
        let positions = 10_000.min(n);
        let (_, a) = audit(|| {
            (0..positions).for_each(|i| {
                p.at(i);
            })
        });
        a.derivations as f64 / positions as f64
    };
    let close = mean(1_000_000); // domain 2²⁰, 1.05 n
    assert!((4.0..=5.0).contains(&close), "{close}");
    let far = mean((1 << 20) + 1); // domain 2²², 4 n
    assert!(far <= 17.0, "{far}");
}
