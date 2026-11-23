//! The frontier (ADR-0047 decision 16, K2b-0a): its properties under drawn
//! interleavings of dispatches, sends, answers, judgements and drains on a
//! clock that never goes back, ties included; and a twin for each edge.

use std::collections::BTreeMap;

use ascetic_ddd_scheduler::frontier::{Frontier, Hold};
use ascetic_ddd_scheduler::machine::{Input, Judgement, Seen};
use proptest::prelude::*;

fn unknown_at(out: &[(u64, Input)], line: u64, attempt: u32) -> Vec<u64> {
    out.iter()
        .filter(|(_, i)| {
            matches!(i, Input::Ended { line: l, attempt: a, judgement: Judgement::Unknown, .. }
                if *l == line && *a == attempt)
        })
        .map(|(t, _)| *t)
        .collect()
}

fn ended(out: &[(u64, Input)], line: u64, attempt: u32) -> Vec<(u64, Judgement)> {
    out.iter()
        .filter_map(|(t, i)| match i {
            Input::Ended {
                line: l,
                attempt: a,
                judgement,
                ..
            } if *l == line && *a == attempt => Some((*t, judgement.clone())),
            _ => None,
        })
        .collect()
}

fn sent(out: &[(u64, Input)], line: u64) -> Vec<u64> {
    out.iter()
        .filter(|(_, i)| matches!(i, Input::Sent { line: l, .. } if *l == line))
        .map(|(t, _)| *t)
        .collect()
}

fn expected() -> Judgement {
    Judgement::Expected(vec![])
}

#[test]
fn an_answer_at_its_deadline_is_taken() {
    let mut f = Frontier::new();
    assert_eq!(f.open(0, 2, 1, 100), None);
    f.sent(10, 2, 1).unwrap();
    let h = f.arrived(100, 2, 1).unwrap();
    // While it is judged, nothing at or past it is released, and it is not
    // censored however long the judging takes.
    assert!(f.drain(500, false).iter().all(|(t, _)| *t < 100));
    f.ended(h, expected(), Seen::default()).unwrap();
    let out = f.drain(501, false);
    assert_eq!(ended(&out, 2, 1), vec![(100, expected())]);
}

#[test]
fn an_answer_past_its_deadline_follows_its_censor() {
    let mut f = Frontier::new();
    f.open(0, 2, 1, 100);
    f.sent(10, 2, 1).unwrap();
    let h = f.arrived(101, 2, 1).unwrap();
    assert!(h.late());
    f.ended(h, expected(), Seen::default()).unwrap();
    let out = f.drain(200, false);
    assert_eq!(
        ended(&out, 2, 1),
        vec![(100, Judgement::Unknown), (101, expected())]
    );
}

#[test]
fn a_send_at_its_deadline_is_not_pushed() {
    let mut f = Frontier::new();
    f.open(0, 2, 1, 100);
    f.sent(100, 2, 1).unwrap();
    let out = f.drain(200, false);
    assert!(sent(&out, 2).is_empty());
    assert_eq!(unknown_at(&out, 2, 1), vec![100]);
}

#[test]
fn a_deadline_already_released_is_censored_at_once() {
    let mut f = Frontier::new();
    f.drain(300, false);
    // Released through 299: an attempt whose deadline lies within it.
    let censor = f.open(0, 2, 1, 250).expect("censored at once");
    assert_eq!(censor.0, 250);
    assert!(matches!(
        censor.1,
        Input::Ended {
            judgement: Judgement::Unknown,
            ..
        }
    ));
    assert_eq!(f.open_attempts(), 0);
    // One just past it is open.
    assert_eq!(f.open(0, 3, 1, 300), None);
}

#[test]
fn an_early_reply_is_sent_first() {
    let mut f = Frontier::new();
    f.open(0, 2, 1, 100);
    // The gateway answered before the request's last byte left.
    let h = f.arrived(40, 2, 1).unwrap();
    f.ended(h, Judgement::Unexpected, Seen::default()).unwrap();
    f.sent(50, 2, 1).unwrap();
    let out = f.drain(60, false);
    assert_eq!(sent(&out, 2), vec![40]);
    assert_eq!(ended(&out, 2, 1), vec![(40, Judgement::Unexpected)]);
    let kinds: Vec<u8> = out.iter().map(|(_, i)| i.kind()).collect();
    assert_eq!(kinds, vec![1, 2]);
}

#[test]
fn nothing_is_released_unsettled_at_t0() {
    let mut f = Frontier::new();
    f.open(0, 2, 1, 0);
    assert!(f.drain(0, false).is_empty());
    assert_eq!(f.released(), None);
    // Settled, the instant itself.
    assert_eq!(unknown_at(&f.drain(0, true), 2, 1), vec![0]);
}

#[test]
fn a_push_below_what_was_released_is_refused() {
    let mut f = Frontier::new();
    f.open(0, 2, 1, 1_000);
    f.drain(500, false);
    assert!(f.sent(400, 2, 1).is_err());
    assert!(f.sent(499, 2, 1).is_ok());
}

/// What a drawn step does.
#[derive(Clone, Debug)]
enum Op {
    /// The clock moves on, perhaps not at all.
    Tick(u64),
    /// An attempt dispatched, its deadline this far ahead.
    Open(u64),
    /// An attempt's bytes left.
    Sent(usize),
    /// An answer arrived.
    Arrive(usize),
    /// An answer judged.
    End(usize),
    /// A drain, settled or not.
    Drain(bool),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (0u64..4).prop_map(Op::Tick),
        2 => (0u64..12).prop_map(Op::Open),
        2 => any::<prop::sample::Index>().prop_map(|i| Op::Sent(i.index(64))),
        2 => any::<prop::sample::Index>().prop_map(|i| Op::Arrive(i.index(64))),
        2 => any::<prop::sample::Index>().prop_map(|i| Op::End(i.index(64))),
        2 => any::<bool>().prop_map(Op::Drain),
    ]
}

#[derive(Debug)]
struct Attempt {
    line: u64,
    deadline: u64,
    sent: Option<u64>,
    arrived: Option<u64>,
    /// Its answer arrived by its deadline, before any drain released it.
    in_time: bool,
    censored_at_once: bool,
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 4_000, ..ProptestConfig::default() })]

    /// Decision 16: drained in stamp order, never below what was released;
    /// an answer by its deadline kept; one with none censored once at its
    /// deadline; a send only before it; nothing at or past a live floor.
    #[test]
    fn the_frontier_releases_in_order_and_censors_only_what_came_late(ops in prop::collection::vec(op(), 1..120)) {
        let mut f = Frontier::new();
        let mut now = 0u64;
        let mut attempts: Vec<Attempt> = Vec::new();
        let mut holds: BTreeMap<usize, Hold> = BTreeMap::new();
        let mut out: Vec<(u64, Input)> = Vec::new();
        let mut last = (0u64, 0u64, 0u8);
        for o in ops {
            match o {
                Op::Tick(d) => now += d,
                Op::Open(ahead) => {
                    let line = attempts.len() as u64 + 2;
                    let deadline = now + ahead;
                    let censor = f.open(now, line, 1, deadline);
                    attempts.push(Attempt { line, deadline, sent: None, arrived: None, in_time: false, censored_at_once: censor.is_some() });
                    if let Some(c) = censor {
                        prop_assert_eq!(c.0, deadline);
                        out.push(c);
                    }
                }
                Op::Sent(i) if i < attempts.len() => {
                    let a = &mut attempts[i];
                    prop_assert!(f.sent(now, a.line, 1).is_ok());
                    a.sent.get_or_insert(now);
                }
                Op::Arrive(i) if i < attempts.len() && attempts[i].arrived.is_none() => {
                    let released = f.released();
                    let a = &mut attempts[i];
                    a.in_time = now <= a.deadline
                        && !a.censored_at_once
                        && released.is_none_or(|r| r < a.deadline);
                    let h = f.arrived(now, a.line, 1);
                    prop_assert!(h.is_ok());
                    a.arrived = Some(now);
                    holds.insert(i, h.unwrap());
                }
                Op::End(i) => {
                    if let Some(h) = holds.remove(&i) {
                        prop_assert!(f.ended(h, Judgement::Unexpected, Seen::default()).is_ok());
                    }
                }
                Op::Drain(settled) => {
                    let floor = f.floor();
                    let drained = f.drain(now, settled);
                    let mut within = (0u64, 0u64, 0u8);
                    for (t, input) in &drained {
                        // Nothing at or past a live floor, nothing past now.
                        prop_assert!(floor.is_none_or(|fl| *t < fl), "{t} at or past the floor {floor:?}");
                        prop_assert!(*t <= now);
                        // Stamps never go back across drains; within one,
                        // (stamp, line, kind) order. An input caused at an
                        // instant settled may sort below one already taken
                        // there, as in `sim`: only its stamp is bound.
                        let key = (*t, input.line(), input.kind());
                        prop_assert!(*t >= last.0, "{key:?} after {last:?}");
                        prop_assert!(key >= within || *t > within.0, "{key:?} within a drain after {within:?}");
                        within = key;
                        last = key;
                    }
                    out.extend(drained);
                }
                _ => {}
            }
        }
        // Everything judged, then all of it released.
        for (_, h) in std::mem::take(&mut holds) {
            prop_assert!(f.ended(h, Judgement::Unexpected, Seen::default()).is_ok());
        }
        now += 1_000;
        out.extend(f.drain(now, true));
        for a in &attempts {
            let ends = ended(&out, a.line, 1);
            let unknowns = unknown_at(&out, a.line, 1);
            match a.arrived {
                Some(t) if a.in_time => {
                    // Taken, never censored.
                    prop_assert_eq!(ends, vec![(t, Judgement::Unexpected)]);
                }
                Some(t) => {
                    // Censored at the deadline first, then the late answer.
                    prop_assert_eq!(unknowns, vec![a.deadline]);
                    prop_assert_eq!(ends.last().cloned(), Some((t, Judgement::Unexpected)));
                }
                None => prop_assert_eq!(unknowns, vec![a.deadline]),
            }
            // A send only before the deadline, and before or at the answer.
            for s in sent(&out, a.line) {
                prop_assert!(s < a.deadline || a.arrived.is_some_and(|t| s == t), "sent {s}, deadline {}", a.deadline);
            }
            if let (Some(s), None) = (a.sent, a.arrived) {
                prop_assert_eq!(sent(&out, a.line).len(), usize::from(s < a.deadline && !a.censored_at_once));
            }
        }
    }
}

/// Decision 17: an attempt's path — its task's first poll, its request handed
/// to a ready connection — rides on whichever `Sent` is pushed: its own, or
/// the one an early reply makes; `handed` also its `connected` until reported.
#[test]
fn a_send_carries_the_path_its_attempt_reported() {
    let path = |out: &[(u64, Input)]| {
        out.iter()
            .find_map(|(_, i)| match i {
                Input::Sent {
                    handed, connected, ..
                } => Some((*handed, *connected)),
                _ => None,
            })
            .unwrap()
    };
    let mut f = Frontier::new();
    f.open(5, 2, 1, 1_000);
    f.handed(7, 2, 1);
    f.sent(30, 2, 1).unwrap();
    assert_eq!(path(&f.drain(40, false)), (7, 7));
    // An early reply: the `Sent` the frontier makes carries the path too.
    let mut f = Frontier::new();
    f.open(5, 2, 1, 1_000);
    f.handed(7, 2, 1);
    f.connected(9, 2, 1);
    let h = f.arrived(20, 2, 1).unwrap();
    f.ended(h, Judgement::Unexpected, Seen::default()).unwrap();
    assert_eq!(path(&f.drain(30, false)), (7, 9));
    // Never reported: the dispatch's instant.
    let mut f = Frontier::new();
    f.open(5, 2, 1, 1_000);
    f.sent(30, 2, 1).unwrap();
    assert_eq!(path(&f.drain(40, false)), (5, 5));
    // A censor is marked as one.
    let mut f = Frontier::new();
    f.open(5, 2, 1, 50);
    let censored = f
        .drain(100, false)
        .iter()
        .any(|(_, i)| matches!(i, Input::Ended { seen, .. } if seen.censored));
    assert!(censored);
}

/// Decision 19: a never-sent attempt's closing report — refused at or before
/// its deadline, where the machine would take it for the attempt's answer;
/// past it, after its censor: the censor pushed first if the drain has not
/// reached the deadline, and none twice.
#[test]
fn a_closing_report_follows_its_censor() {
    use ascetic_ddd_scheduler::frontier::Left;
    use ascetic_ddd_scheduler::machine::{Code, Unsent};
    let unsent = || Unsent {
        handed: Some(5),
        connected: None,
        lag: 0,
        refused: 0,
        refused_by: None,
        code: Code::Named("rest.connecting".to_owned()),
    };
    let censors = |out: &[(u64, Input)]| -> Vec<u64> {
        out.iter()
            .filter_map(|(at, i)| match i {
                Input::Ended { seen, .. } if seen.censored => Some(*at),
                _ => None,
            })
            .collect()
    };
    let report = |out: &[(u64, Input)]| -> Vec<u64> {
        out.iter()
            .filter_map(|(at, i)| match i {
                Input::Ended { seen, .. } if seen.unsent.is_some() => Some(*at),
                _ => None,
            })
            .collect()
    };
    // At its deadline: refused.
    let mut f = Frontier::new();
    f.open(0, 2, 1, 1_000);
    assert_eq!(
        f.left(1_000, 2, 1, unsent()),
        Err(Left::Early {
            at: 1_000,
            deadline: 1_000
        })
    );
    // Past it, the drain not there: the censor first, then the report.
    let mut f = Frontier::new();
    f.open(0, 2, 1, 1_000);
    f.left(1_001, 2, 1, unsent()).unwrap();
    let out = f.drain(2_000, true);
    assert_eq!(censors(&out), vec![1_000]);
    assert_eq!(report(&out), vec![1_001]);
    // Past a censor already drained: the report alone.
    let mut f = Frontier::new();
    f.open(0, 2, 1, 1_000);
    let censored = f.drain(1_000, true);
    assert_eq!(censors(&censored), vec![1_000]);
    f.left(1_001, 2, 1, unsent()).unwrap();
    let out = f.drain(2_000, true);
    assert!(censors(&out).is_empty());
    assert_eq!(report(&out), vec![1_001]);
}
