//! M6 of ADR-0045, hand-run in release under `/usr/bin/time -v`: members
//! computed and discarded on every thread of the machine, so that time and peak
//! memory can be compared between 10⁷ and 10⁹ members.
//!
//! ```text
//! /usr/bin/time -v cargo test --release -p ascetic-ddd-faker --test scale -- --ignored ten_million
//! /usr/bin/time -v cargo test --release -p ascetic-ddd-faker --test scale -- --ignored a_billion
//! ```

use std::sync::atomic::{AtomicU64, Ordering};

use ascetic_ddd_faker::generator::{Gen, GenExt, constant, draw, from_fn, replicate};
use ascetic_ddd_faker::law::{Categorical, Skew, UniformInt};
use ascetic_ddd_faker::population::Plan;
use ascetic_ddd_faker::{Key, Seed};

const CHUNK: u64 = 100_000;

fn run(members: u64) -> u64 {
    const TENANTS: u64 = 10;
    let mut plan = Plan::new(Seed(1));
    let os = draw(Categorical::from_weights(&[0.55, 0.3, 0.1, 0.05]).unwrap());
    let oem = draw(Categorical::from_counts(&[40, 35, 20, 5]).unwrap())
        .optional(0.9)
        .unwrap();
    let facets = replicate(
        draw(UniformInt::new(0, 3).unwrap()),
        draw(UniformInt::new(0, 63).unwrap()),
        3,
    );
    let device = from_fn(move |k: Key| {
        Ok((
            os.generate(k.child("os"))?,
            oem.generate(k.child("oem"))?,
            facets.generate(k.child("facets"))?,
        ))
    });
    let per_tenant = members / TENANTS;
    let devices = plan
        .population::<u64, _, _>("device", constant(per_tenant), device)
        .unwrap();
    let branches = plan
        .population::<u64, _, _>("branch", constant(1_000u64), constant(()))
        .unwrap();
    let skew = Skew::new(2.0).unwrap();
    let chunks = members.div_ceil(CHUNK);
    let next = AtomicU64::new(0);
    let checksum = AtomicU64::new(0);
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    std::thread::scope(|s| {
        for _ in 0..threads {
            s.spawn(|| {
                let mut sum = 0u64;
                loop {
                    let c = next.fetch_add(1, Ordering::Relaxed);
                    if c >= chunks {
                        break;
                    }
                    let (tenant, start) = ((c * CHUNK) / per_tenant, (c * CHUNK) % per_tenant);
                    let view = devices.scope(tenant).unwrap();
                    let groups = branches.scope(tenant).unwrap();
                    for i in start..(start + CHUNK).min(per_tenant) {
                        let (os, oem, facets) = view.value(i).unwrap();
                        let branch = groups.pick(view.key_of(i).child("branch"), &skew).unwrap();
                        sum = sum
                            .wrapping_add(os)
                            .wrapping_add(oem.unwrap_or(7))
                            .wrapping_add(facets.len() as u64)
                            .wrapping_add(branch.index);
                    }
                }
                checksum.fetch_add(sum, Ordering::Relaxed);
            });
        }
    });
    checksum.load(Ordering::Relaxed)
}

#[test]
#[ignore = "M6, hand-run in release: 10^7 members"]
fn ten_million() {
    assert_ne!(run(10_000_000), 0);
}

#[test]
#[ignore = "M6, hand-run in release: 10^9 members"]
fn a_billion() {
    assert_ne!(run(1_000_000_000), 0);
}
