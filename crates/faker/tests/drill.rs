//! D1 and Par1 of ADR-0045: the host's cohort drill, written against the
//! crate — Tenants; branches nested under `all`, their Devices apportioned to an
//! exact total by a skewed law; Devices with contexts by categorical laws;
//! Policies on drawn branches; a sample re-managed without replacement — and
//! generated on one thread and on several, in units fixed by the Plan.

use std::sync::atomic::{AtomicUsize, Ordering};

use ascetic_ddd_faker::generator::{Gen, GenExt, constant, draw, from_fn, replicate};
use ascetic_ddd_faker::law::{Categorical, Skew, UniformInt};
use ascetic_ddd_faker::population::{Plan, Population, Ranges};
use ascetic_ddd_faker::testing::encoding;
use ascetic_ddd_faker::{Apportionment, Key, Namespace, Seed, apportion};

const TENANTS: u64 = 3;
const BRANCHES: u64 = 100;
const DEVICES: u64 = 10_000;
const POLICIES: u64 = 10;
const SAMPLE: u64 = 500;
const UNIT: u64 = 1_000;

/// A Device as `RecordDeviceContextCommand` carries one.
#[derive(Clone, Debug, PartialEq)]
struct Device {
    os: u64,
    platform: u64,
    oem: Option<u64>,
    management_mode: u64,
    facets: Vec<u64>,
}

/// The drill's Plan: its populations and the branch weights.
struct Drill {
    devices:
        Population<u64, ascetic_ddd_faker::generator::Constant<u64>, Box<dyn Gen<Output = Device>>>,
    branches:
        Population<u64, ascetic_ddd_faker::generator::Constant<u64>, Box<dyn Gen<Output = ()>>>,
    policies:
        Population<u64, ascetic_ddd_faker::generator::Constant<u64>, Box<dyn Gen<Output = u64>>>,
    sizes: Key,
}

fn drill() -> Drill {
    let mut plan = Plan::new(Seed(2026));
    let device: Box<dyn Gen<Output = Device>> = {
        let os = draw(Categorical::from_weights(&[0.55, 0.3, 0.1, 0.05]).unwrap());
        let platform = draw(Categorical::from_weights(&[0.8, 0.2]).unwrap());
        let oem = draw(Categorical::from_counts(&[40, 35, 20, 5]).unwrap())
            .optional(0.9)
            .unwrap();
        let mode = draw(Categorical::from_weights(&[0.95, 0.05]).unwrap());
        let facets = replicate(
            draw(UniformInt::new(0, 3).unwrap()),
            draw(UniformInt::new(0, 63).unwrap()),
            3,
        );
        Box::new(from_fn(move |k: Key| {
            Ok(Device {
                os: os.generate(k.child("os"))?,
                platform: platform.generate(k.child("platform"))?,
                oem: oem.generate(k.child("oem"))?,
                management_mode: mode.generate(k.child("management_mode"))?,
                facets: facets.generate(k.child("facets"))?,
            })
        }))
    };
    let devices = plan
        .population::<u64, _, _>("device", constant(DEVICES), device)
        .unwrap();
    let branches = plan
        .population::<u64, _, _>(
            "branch",
            constant(BRANCHES),
            Box::new(constant(())) as Box<dyn Gen<Output = ()>>,
        )
        .unwrap();
    let priority: Box<dyn Gen<Output = u64>> = Box::new(draw(UniformInt::new(1, 100).unwrap()));
    let policies = plan
        .population::<u64, _, _>("policy", constant(POLICIES), priority)
        .unwrap();
    Drill {
        devices,
        branches,
        policies,
        sizes: Key::root(Seed(2026)).child("branch sizes"),
    }
}

impl Drill {
    /// A Tenant's branch sizes: a power law over its branches, apportioned to
    /// exactly `DEVICES`.
    fn branch_sizes(&self, tenant: u64) -> Apportionment {
        let weights: Vec<f64> = (0..BRANCHES)
            .map(|b| 1.0 / ascetic_ddd_faker::math::pow((b + 1) as f64, 1.1))
            .collect();
        apportion(
            DEVICES,
            Categorical::from_weights(&weights).unwrap(),
            self.sizes.part(&tenant).unit(),
        )
    }
}

/// One line of the scenario, as the encoding a digest would take.
fn device_line(tenant: u64, index: u64, branch: u64, d: &Device) -> Vec<u8> {
    encoding(&(
        ("device", tenant, index, branch),
        (
            d.os,
            d.platform,
            d.oem,
            (d.management_mode, d.facets.clone()),
        ),
    ))
}

/// A unit of work, fixed by the Plan: `UNIT` Devices of one Tenant.
fn unit(drill: &Drill, tenant: u64, chunk: u64) -> Vec<Vec<u8>> {
    let view = drill.devices.scope(tenant).unwrap();
    let sizes = drill.branch_sizes(tenant);
    (chunk * UNIT..((chunk + 1) * UNIT).min(view.size()))
        .map(|i| {
            let branch = sizes.parent_of(i).unwrap();
            device_line(tenant, i, branch, &view.value(i).unwrap())
        })
        .collect()
}

fn units() -> Vec<(u64, u64)> {
    (0..TENANTS)
        .flat_map(|t| (0..DEVICES.div_ceil(UNIT)).map(move |c| (t, c)))
        .collect()
}

/// The whole log on `threads` threads: units taken in any order, written in
/// the Plan's.
fn generate(threads: usize, reversed: bool) -> Vec<Vec<u8>> {
    let drill = drill();
    let mut units = units();
    let count = units.len();
    let order: Vec<usize> = if reversed {
        (0..count).rev().collect()
    } else {
        (0..count).collect()
    };
    let next = AtomicUsize::new(0);
    let mut out: Vec<Option<Vec<Vec<u8>>>> = vec![None; count];
    std::thread::scope(|s| {
        let workers: Vec<_> = (0..threads)
            .map(|_| {
                s.spawn(|| {
                    let mut done = Vec::new();
                    loop {
                        let n = next.fetch_add(1, Ordering::Relaxed);
                        let Some(&u) = order.get(n) else { break };
                        let (tenant, chunk) = units[u];
                        done.push((u, unit(&drill, tenant, chunk)));
                    }
                    done
                })
            })
            .collect();
        for w in workers {
            for (u, lines) in w.join().unwrap() {
                out[u] = Some(lines);
            }
        }
    });
    units.clear();
    out.into_iter().flat_map(|lines| lines.unwrap()).collect()
}

/// Par1: one Plan, one log, whatever the threads and the order units are
/// computed in.
#[test]
fn the_thread_count_changes_no_byte() {
    let one = generate(1, false);
    assert_eq!(one.len() as u64, TENANTS * DEVICES);
    assert_eq!(generate(2, false), one);
    assert_eq!(generate(7, false), one);
    assert_eq!(generate(1, true), one);
}

/// D1: the drill's shape, exact where the drill is exact.
#[test]
fn the_cohort_drill_is_expressible() {
    let drill = drill();
    for tenant in 0..TENANTS {
        // Branches apportioned to exactly the Tenant's Devices, skewed.
        let sizes = drill.branch_sizes(tenant);
        assert_eq!(Ranges::total(&sizes), DEVICES);
        assert!(sizes.count(0) > 10 * sizes.count(BRANCHES as usize - 1));
        // `all` nests every branch; a branch's members are a range, known
        // before any Device arrives: the host's import order.
        let branches = drill.branches.scope(tenant).unwrap();
        let all: Vec<u64> = (0..branches.size()).collect();
        assert_eq!(all.len() as u64, BRANCHES);
        let devices = drill.devices.scope(tenant).unwrap();
        let ns = Namespace(1);
        let members: Vec<String> = sizes
            .children(17)
            .map(|i| devices.key_of(i).uuid(&ns).to_string())
            .collect();
        assert_eq!(members.len() as u64, sizes.count(17));
        // Policies target branches drawn by a skew; every target is in scope.
        let policies = drill.policies.scope(tenant).unwrap();
        let skew = Skew::new(2.0).unwrap();
        for p in 0..policies.size() {
            let target = branches
                .pick(policies.key_of(p).child("target"), &skew)
                .unwrap();
            assert_eq!(target.scope, tenant);
            assert!(target.index < BRANCHES);
            assert!((1..=100).contains(&policies.value(p).unwrap()));
        }
        // A sample re-managed without replacement: the first positions of the
        // Tenant's own order.
        let order = devices.permutation();
        let mut sample: Vec<u64> = (0..SAMPLE).map(|i| order.at(i).unwrap()).collect();
        sample.sort_unstable();
        sample.dedup();
        assert_eq!(sample.len() as u64, SAMPLE);
        // Contexts by their laws: the OS mix within a few σ of its weights.
        let first_os = (0..DEVICES)
            .filter(|i| devices.value(*i).unwrap().os == 0)
            .count();
        assert!(
            (first_os as f64 - 0.55 * DEVICES as f64).abs()
                < 5.0 * (DEVICES as f64 * 0.55 * 0.45).sqrt()
        );
    }
    // Two Tenants are not copies of each other.
    let (a, b) = (
        drill.devices.scope(0).unwrap(),
        drill.devices.scope(1).unwrap(),
    );
    assert!((0..100).any(|i| a.value(i).unwrap() != b.value(i).unwrap()));
}
