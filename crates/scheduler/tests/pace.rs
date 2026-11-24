//! Decision 21 of ADR-0047: the pace at the hand-off, against its own rule by
//! brute force, and the band the shell and `sim` pass attempts through.

use ascetic_ddd_scheduler::pace::{Gate, Pace, Turn};
use proptest::prelude::*;

/// The rule, pair by pair: `1000·|G_k − G_i| ≥ (1000 − κ)·(|raw_k − raw_i| − J)`.
fn kept(handed: &[(u64, u64)], jitter: u64, catch_up: u32) -> Result<(), (usize, usize)> {
    let c = 1_000_i128 - i128::from(catch_up);
    let j = i128::from(jitter);
    for (k, &(gk, rk)) in handed.iter().enumerate() {
        for (i, &(gi, ri)) in handed.iter().enumerate().take(k) {
            let dg = (i128::from(gk) - i128::from(gi)).abs();
            let dr = (i128::from(rk) - i128::from(ri)).abs();
            if 1_000 * dg < c * (dr - j) {
                return Err((i, k));
            }
        }
    }
    Ok(())
}

fn catch_ups() -> impl Strategy<Value = u32> {
    prop_oneof![Just(0), Just(1), Just(100), Just(500), Just(999)]
}

fn jitters() -> impl Strategy<Value = u64> {
    prop_oneof![Just(0), Just(1), Just(250), Just(2_000)]
}

proptest! {
    /// Each attempt handed at the later of its earliest and the instant it
    /// came, in the order it came: every pair keeps the rule, and its
    /// earliest is the least instant that does — a microsecond sooner, a pair
    /// breaks it.
    #[test]
    fn the_earliest_is_the_least_instant_the_rule_allows(
        catch_up in catch_ups(),
        jitter in jitters(),
        steps in prop::collection::vec((0u64..5_000, 0u64..3_000), 1..40),
    ) {
        let mut pace = Pace::new(jitter, catch_up);
        let mut handed: Vec<(u64, u64)> = Vec::new();
        let mut now = 0u64;
        for (raw, wait) in steps {
            now += wait;
            let earliest = pace.earliest(raw);
            prop_assert!(earliest >= raw);
            let g = earliest.max(now).max(handed.last().map_or(0, |h| h.0));
            // A microsecond before its earliest (and no sooner than the
            // last hand-off, nor than its raw due): some pair breaks.
            if earliest > raw && earliest > handed.last().map_or(0, |h| h.0) {
                let mut sooner = handed.clone();
                sooner.push((earliest - 1, raw));
                prop_assert!(kept(&sooner, jitter, catch_up).is_err(), "{sooner:?}");
            }
            prop_assert!(pace.keeps(raw, g));
            pace.hand(raw, g);
            handed.push((g, raw));
            prop_assert_eq!(kept(&handed, jitter, catch_up), Ok(()));
            now = g;
        }
    }

    /// A harness late by at most `(1 − κ)·J` on every hand-off is never paced:
    /// timer error within the allowance moves nothing (decision 21).
    #[test]
    fn a_hand_off_within_the_allowance_is_never_paced(
        catch_up in catch_ups(),
        jitter in jitters(),
        steps in prop::collection::vec((0u64..3_000, any::<u16>()), 1..40),
    ) {
        let allowance = (1_000 - u64::from(catch_up)) * jitter / 1_000;
        let mut raw = 0u64;
        let mut stream: Vec<(u64, u64)> = steps
            .into_iter()
            .map(|(gap, late)| {
                raw += gap;
                let late = if allowance == 0 { 0 } else { u64::from(late) % (allowance + 1) };
                (raw + late, raw)
            })
            .collect();
        // Taken in the order they were handed off.
        stream.sort();
        let mut pace = Pace::new(jitter, catch_up);
        for &(g, raw) in &stream {
            prop_assert!(pace.keeps(raw, g), "paced at {g}, raw {raw}: {stream:?}");
            pace.hand(raw, g);
        }
    }
}

/// The gate passes a dispatched batch in raw order and within the band: two
/// read late at one instant, their raws 0 and 5,000 apart, the second paced.
#[test]
fn the_band_passes_in_raw_order_and_paces_the_rest() {
    let mut gate = Gate::new(2_000, 50);
    for (line, raw) in [(3, 5_000), (2, 0)] {
        gate.dispatched(line, 1, raw, 1_000_000);
        gate.arrived(line, 1);
    }
    // At 6,000 the first passes; the second's earliest is its raw, 5,000,
    // plus what the rule asks after a hand-off at 6,000 of raw 0:
    // ⌈(6,000,000 + 950·5,000 − 950·2,000) / 1000⌉ = 8,850.
    assert_eq!(
        gate.turns(6_000),
        vec![Turn::Pass {
            line: 2,
            attempt: 1
        }]
    );
    assert_eq!(gate.next(), Some(8_850));
    assert_eq!(gate.turns(8_849), vec![]);
    assert_eq!(
        gate.turns(8_850),
        vec![Turn::Pass {
            line: 3,
            attempt: 1
        }]
    );
    assert!(gate.is_empty());
}

/// An attempt not yet arrived holds the band's front: one beyond it by more
/// than J waits; arrived, the front passes, then the rest.
#[test]
fn an_attempt_not_arrived_holds_the_front() {
    let mut gate = Gate::new(2_000, 50);
    gate.dispatched(2, 1, 0, 1_000_000);
    gate.dispatched(3, 1, 3_000, 1_000_000);
    gate.arrived(3, 1);
    assert_eq!(gate.turns(3_000), vec![]);
    // Within J of the front, it would pass.
    gate.dispatched(4, 1, 1_500, 1_000_000);
    gate.arrived(4, 1);
    assert_eq!(
        gate.turns(3_000),
        vec![Turn::Pass {
            line: 4,
            attempt: 1
        }]
    );
    gate.arrived(2, 1);
    let turns = gate.turns(3_000);
    assert_eq!(
        turns.first(),
        Some(&Turn::Pass {
            line: 2,
            attempt: 1
        })
    );
}

/// One whose pace falls at or past its deadline leaves at once, never
/// handed, and holds nothing; one past its deadline is gone.
#[test]
fn a_head_that_cannot_pass_by_its_deadline_leaves() {
    let mut gate = Gate::new(0, 0);
    gate.dispatched(2, 1, 10_000, 1_000_000);
    gate.arrived(2, 1);
    assert_eq!(
        gate.turns(20_000),
        vec![Turn::Pass {
            line: 2,
            attempt: 1
        }]
    );
    // Raw 0 after a hand-off at 20,000 of raw 10,000: earliest 30,000, past
    // its deadline at 25,000.
    gate.dispatched(3, 1, 0, 25_000);
    gate.arrived(3, 1);
    gate.dispatched(4, 1, 1, 1_000_000);
    gate.arrived(4, 1);
    let turns = gate.turns(20_000);
    assert_eq!(
        turns.first(),
        Some(&Turn::Late {
            line: 3,
            attempt: 1
        })
    );
    // Its deadline come while it waited: never handed, its report owed.
    let mut gate = Gate::new(0, 0);
    gate.dispatched(2, 1, 0, 100);
    assert_eq!(
        gate.turns(100),
        vec![Turn::Late {
            line: 2,
            attempt: 1
        }]
    );
    assert!(gate.is_empty());
}

/// A straggler behind an on-time stream within the band passes within its
/// pace: passes inside the band never push its earliest past now.
#[test]
fn a_straggler_within_the_band_is_not_starved() {
    let mut gate = Gate::new(2_000, 50);
    gate.dispatched(2, 1, 1_000, 1_000_000);
    for (line, raw) in [(3u64, 1_500u64), (4, 2_500), (5, 2_900)] {
        gate.dispatched(line, 1, raw, 1_000_000);
        gate.arrived(line, 1);
    }
    let first = gate.turns(3_000);
    assert_eq!(
        first,
        vec![
            Turn::Pass {
                line: 3,
                attempt: 1
            },
            Turn::Pass {
                line: 4,
                attempt: 1
            },
            Turn::Pass {
                line: 5,
                attempt: 1
            }
        ]
    );
    gate.arrived(2, 1);
    assert_eq!(
        gate.turns(3_000),
        vec![Turn::Pass {
            line: 2,
            attempt: 1
        }]
    );
}

/// One beyond the band past its deadline is gone too: the next turn never
/// names an instant already gone (a `sim` that would loop on it).
#[test]
fn the_next_turn_is_never_an_instant_gone() {
    let mut gate = Gate::new(0, 0);
    gate.dispatched(2, 1, 0, 1_000_000);
    gate.dispatched(3, 1, 5_000, 100);
    gate.arrived(3, 1);
    assert_eq!(
        gate.turns(200),
        vec![Turn::Late {
            line: 3,
            attempt: 1
        }]
    );
    assert!(gate.next().is_none_or(|n| n > 200), "{:?}", gate.next());
}
