//! Pop1, Pop2, PP1 and Rng1 of ADR-0045: populations hold identities, picks
//! are pure, one-to-many is ranges.

use ascetic_ddd_faker::generator::{Gen, constant, draw, from_fn};
use ascetic_ddd_faker::law::{Categorical, Poisson, Skew, UniformIndex};
use ascetic_ddd_faker::population::{DuplicateKind, Plan, Pool, Ranges, Table};
use ascetic_ddd_faker::testing::{chi_square, stratified};
use ascetic_ddd_faker::{Key, Seed, Unit, apportion};

fn os() -> Categorical {
    Categorical::from_weights(&[0.6, 0.3, 0.1]).unwrap()
}

/// Pop1: a member is its key's value — `root.child(kind).part(scope).at(i)` —
/// whoever names it; a pick lies inside the scope.
#[test]
fn a_member_is_its_key_s_value() {
    let mut plan = Plan::new(Seed(5));
    let devices = plan
        .population::<u64, _, _>("device", constant(1_000u64), draw(os()))
        .unwrap();
    let view = devices.scope(2).unwrap();
    assert_eq!(view.size(), 1_000);
    for i in [0, 1, 500, 999] {
        let by_path = draw(os())
            .generate(Key::root(Seed(5)).child("device").part(&2u64).at(i))
            .unwrap();
        assert_eq!(view.value(i).unwrap(), by_path);
        assert_eq!(devices.value(&view.reference(i)).unwrap(), by_path);
    }
    // Two consumers that pick one member see one value.
    let skew = Skew::new(2.0).unwrap();
    for c in 0..1_000u64 {
        let r = view
            .pick(Key::root(Seed(5)).child("consumer").at(c), &skew)
            .unwrap();
        assert!(r.index < 1_000);
        assert_eq!(r.scope, 2);
        assert_eq!(devices.value(&r).unwrap(), view.value(r.index).unwrap());
    }
    // Uniform picks reach the last member and never pass it: a pick over
    // `size + 1` would land on 1,000 about ten times in 10⁴.
    let mut last = 0;
    for c in 0..10_000u64 {
        let r = view
            .pick(Key::root(Seed(5)).child("uniform").at(c), &UniformIndex)
            .unwrap();
        assert!(r.index < 1_000);
        last = last.max(r.index);
    }
    assert_eq!(last, 999);
    assert!(view.value(1_000).is_err(), "past the size");
}

#[test]
fn a_size_is_drawn_per_scope() {
    let mut plan = Plan::new(Seed(5));
    let groups = plan
        .population::<u64, _, _>("group", draw(Poisson::new(30.0).unwrap()), constant(()))
        .unwrap();
    let sizes: Vec<u64> = (0..50).map(|t| groups.scope(t).unwrap().size()).collect();
    assert!(sizes.iter().any(|s| *s != sizes[0]), "{sizes:?}");
    assert_eq!(
        groups.scope(7).unwrap().size(),
        groups.scope(7).unwrap().size()
    );
    let empty = plan
        .population::<u64, _, _>("nothing", constant(0u64), constant(()))
        .unwrap();
    let view = empty.scope(1).unwrap();
    assert_eq!(view.pick(Key::root(Seed(1)), &UniformIndex), None);
}

/// Pop2: a Plan has each kind once; the scope is in the path, so member `i` of
/// two Tenants are independent — the negative control, a path without the
/// scope, makes them one.
#[test]
fn scopes_are_independent_and_kinds_unique() {
    let mut plan = Plan::new(Seed(9));
    let devices = plan
        .population::<u64, _, _>("device", constant(30_000u64), draw(os()))
        .unwrap();
    assert!(matches!(
        plan.population::<u64, _, _>("device", constant(1u64), constant(())),
        Err(DuplicateKind("device"))
    ));

    let contingency = |pairs: &dyn Fn(u64) -> (u64, u64)| {
        let mut table = [[0u64; 3]; 3];
        for i in 0..30_000 {
            let (a, b) = pairs(i);
            table[a as usize][b as usize] += 1;
        }
        let rows: Vec<f64> = table.iter().map(|r| r.iter().sum::<u64>() as f64).collect();
        let cols: Vec<f64> = (0..3)
            .map(|j| table.iter().map(|r| r[j]).sum::<u64>() as f64)
            .collect();
        let observed: Vec<u64> = table.iter().flatten().copied().collect();
        let expected: Vec<f64> = (0..9)
            .map(|c| rows[c / 3] * cols[c % 3] / 30_000.0)
            .collect();
        // (3 − 1)(3 − 1) = 4 degrees of freedom, not 8: the margins are estimated.
        let t = chi_square(&observed, &expected).unwrap();
        ascetic_ddd_faker::math::gamma_q(2.0, t.statistic / 2.0)
    };
    let (t0, t1) = (devices.scope(0).unwrap(), devices.scope(1).unwrap());
    let alpha = 1e-4;
    assert!(contingency(&|i| (t0.value(i).unwrap(), t1.value(i).unwrap())) > alpha);
    let unscoped = |i: u64| {
        draw(os())
            .generate(Key::root(Seed(9)).child("device").at(i))
            .unwrap()
    };
    assert!(contingency(&|i| (unscoped(i), unscoped(i))) < alpha);
}

/// PP1: a pick below `upto` lies in the first `upto` positions of its order,
/// is `None` at 0, and follows its law over them.
#[test]
fn a_pick_below_stays_below() {
    let mut plan = Plan::new(Seed(3));
    let parents = plan
        .population::<(), _, _>("parent", constant(1_000u64), constant(()))
        .unwrap();
    let view = parents.scope(()).unwrap();
    let order = view.permutation();
    let key = |i: u64| Key::root(Seed(3)).child("child").at(i);
    assert_eq!(view.pick_below(key(0), &UniformIndex, 0, None), None);
    let mut counts = [0u64; 10];
    for i in 0..10_000 {
        let r = view.pick_below(key(i), &UniformIndex, 10, None).unwrap();
        counts[r.index as usize] += 1;
        let p = view
            .pick_below(key(i), &UniformIndex, 10, Some(&order))
            .unwrap();
        assert!((0..10).any(|position| order.at(position) == Some(p.index)));
    }
    assert!(chi_square(&counts, &[1_000.0; 10]).unwrap().p_value > 1e-4);
    // `upto` past the size is the size.
    assert!(
        view.pick_below(key(1), &UniformIndex, 5_000, None)
            .unwrap()
            .index
            < 1_000
    );
}

/// PP1: the growing pool. Before child `n` exactly `A(n)` parents exist; a
/// parent arrives before the first child that can pick it; a parent is used
/// `mean` times on average — at a mean of 10, Python's in-memory pool used 50
/// whatever it was given (defect 1).
#[test]
fn the_pool_grows_with_its_children() {
    for mean in [1.0, 2.5, 10.0, 37.0] {
        let pool = Pool::new(mean, Key::root(Seed(4)).child("pool")).unwrap();
        for p in 0..500u64 {
            let n = pool.arrives_before(p);
            assert!(
                pool.parents_before(n) > p,
                "mean {mean}: parent {p} not there at {n}"
            );
            if n > 0 {
                assert!(
                    pool.parents_before(n - 1) <= p,
                    "mean {mean}: parent {p} before {n}"
                );
            }
        }
    }
    let pool = Pool::new(10.0, Key::root(Seed(4)).child("pool")).unwrap();
    let mut plan = Plan::new(Seed(4));
    let parents = plan
        .population::<(), _, _>("parent", constant(u64::MAX), constant(()))
        .unwrap();
    let view = parents.scope(()).unwrap();
    let children = 100_000u64;
    for n in 0..children {
        let upto = pool.parents_before(n);
        let r = view
            .pick_below(
                Key::root(Seed(4)).child("c").at(n),
                &UniformIndex,
                upto,
                None,
            )
            .unwrap();
        assert!(r.index < upto);
    }
    let mean = children as f64 / pool.parents_before(children) as f64;
    assert!((mean - 10.0).abs() < 0.01, "{mean}");
    assert!(Pool::new(0.5, Key::root(Seed(1))).is_err());
}

/// Rng1: ranges tile the children without gap or overlap, and `parent_of` finds
/// a child's parent at every boundary, empty parents skipped.
#[test]
fn ranges_tile_their_children() {
    let table = Table::from_counts([3, 0, 0, 5, 1, 0]).unwrap();
    assert_eq!(table.parents(), 6);
    assert_eq!(table.total(), 9);
    let mut next = 0;
    for p in 0..6 {
        let range = table.children(p);
        assert_eq!(range.start, next);
        next = range.end;
        for c in range {
            assert_eq!(table.parent_of(c), Some(p), "child {c}");
        }
    }
    assert_eq!(table.parent_of(9), None);
    // Past the last parent: nothing, at the end.
    assert_eq!(table.children(6), 9..9);
    assert_eq!(table.start(100), 9);
    assert!(Table::from_counts([u64::MAX, 1]).is_err());

    let weights = Categorical::from_weights(&[0.5, 0.0, 0.3, 0.2]).unwrap();
    let split = apportion(10_007, weights, Unit::from_raw(Unit::MAX_RAW / 7).unwrap());
    assert_eq!(Ranges::total(&split), 10_007);
    for c in [0, 1, 5_000, 5_003, 5_004, 10_006] {
        let p = split.parent_of(c).unwrap();
        assert!(split.children(p).contains(&c), "child {c} in {p}");
        assert_ne!(p, 1, "the empty part has no child");
    }
    for u in stratified(64) {
        let split = apportion(1_000, Categorical::from_counts(&[1, 1, 1]).unwrap(), u);
        assert_eq!(Ranges::total(&split), 1_000);
    }
}

/// A member named with another kind is refused, not answered.
#[test]
fn a_reference_of_another_kind_is_refused() {
    let mut plan = Plan::new(Seed(1));
    let a = plan
        .population::<u64, _, _>("a", constant(5u64), from_fn(|k: Key| Ok(k.bits())))
        .unwrap();
    let b = plan
        .population::<u64, _, _>("b", constant(5u64), from_fn(|k: Key| Ok(k.bits())))
        .unwrap();
    let r = a.scope(0).unwrap().reference(1);
    assert!(b.value(&r).is_err());
    assert_ne!(a.value(&r).unwrap(), b.scope(0).unwrap().value(1).unwrap());
}
