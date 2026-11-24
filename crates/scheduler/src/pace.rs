//! The harness's pace at the hand-off (ADR-0047 decision 21): a late harness
//! delays its plan; it never compresses it.
//!
//! For every two hand-offs `i`, `k`:
//!
//! ```text
//! 1000·|G_k − G_i| ≥ (1000 − κ)·(|raw_k − raw_i| − J)
//! ```
//!
//! `G` an attempt's hand-off, `raw` its raw due — a first attempt's intended
//! start, a poll's point on its plan's grid, a resend's due —, `J` the
//! profile's jitter, `κ` the run's catch-up in parts per thousand. The attempts
//! handed in any window of width `u` then have raw dues spanning at most
//! `(u + (1 − κ)·J) / (1 − κ)`: a bucket the validator certified the plan
//! against (arrivals each delayed by up to `J`, at its rate less the reserve,
//! `κ` at most the reserve) meets no more than it admits.
//!
//! [`Pace`] keeps the rule as two running maxima over the hand-offs so far —
//! `U = max(1000·G + c·raw)`, `V = max(1000·G − c·raw)`, `c = 1000 − κ` — in
//! exact integers; [`Gate`] is the band the shell and `sim` pass attempts
//! through: ordered by raw due, an attempt passing when its pace allows and
//! its raw due is within `J` of the least among the attempts dispatched and
//! neither handed nor gone; one not yet arrived holds the front until its
//! deadline; one whose pace falls at or past its deadline leaves, never handed.
//! The maxima only grow, so an attempt's earliest instant never falls: one that
//! cannot pass before its deadline never will.

use std::collections::BTreeMap;

use crate::machine::Line;

/// The pace's state: the two maxima of the hand-offs so far.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pace {
    /// `1000 − κ`.
    c: i128,
    /// The profile's jitter, µs.
    j: i128,
    /// `max(1000·G + c·raw)`; none before the first hand-off.
    u: Option<i128>,
    /// `max(1000·G − c·raw)`.
    v: Option<i128>,
}

impl Pace {
    /// The pace of a run with jitter `jitter` µs and catch-up `catch_up` parts
    /// per thousand, at most 999 (admission refuses a reserve of a thousand).
    pub fn new(jitter: u64, catch_up: u32) -> Pace {
        Pace {
            c: 1_000_i128.saturating_sub(i128::from(catch_up.min(999))),
            j: i128::from(jitter),
            u: None,
            v: None,
        }
    }

    /// The earliest instant an attempt of raw due `raw` may be handed off:
    /// never before its raw due, nor before the pace allows.
    pub fn earliest(&self, raw: u64) -> u64 {
        let r = i128::from(raw);
        let cr = self.c.saturating_mul(r);
        let cj = self.c.saturating_mul(self.j);
        let bound = [
            self.v.map(|v| v.saturating_add(cr)),
            self.u.map(|u| u.saturating_sub(cr)),
        ]
        .into_iter()
        .flatten()
        .max()
        .map(|m| m.saturating_sub(cj));
        match bound {
            // ⌈m / 1000⌉ for m > 0; nothing below 0.
            Some(m) if m > 0 => {
                let at = m.saturating_add(999).checked_div(1_000).unwrap_or(0);
                raw.max(u64::try_from(at).unwrap_or(u64::MAX))
            }
            _ => raw,
        }
    }

    /// Whether a hand-off at `at` of raw due `raw` keeps the pace with every
    /// hand-off so far, each at or before `at`.
    pub fn keeps(&self, raw: u64, at: u64) -> bool {
        at >= self.earliest(raw)
    }

    /// A hand-off at `at` of raw due `raw`, taken into the maxima.
    pub fn hand(&mut self, raw: u64, at: u64) {
        let g = i128::from(at).saturating_mul(1_000);
        let cr = self.c.saturating_mul(i128::from(raw));
        let up = g.saturating_add(cr);
        let down = g.saturating_sub(cr);
        self.u = Some(self.u.map_or(up, |u| u.max(up)));
        self.v = Some(self.v.map_or(down, |v| v.max(down)));
    }
}

/// An attempt in the gate's band.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Waiter {
    deadline: u64,
    /// Ready to pass: decorated, if its carrier decorates.
    arrived: bool,
}

/// What became of an attempt at the gate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Turn {
    /// Handed off now.
    Pass {
        /// Its step's line.
        line: Line,
        /// The attempt.
        attempt: u32,
    },
    /// Its pace falls at or past its deadline, or its deadline came while it
    /// waited: never handed (`pace.late`), its report owed.
    Late {
        /// Its step's line.
        line: Line,
        /// The attempt.
        attempt: u32,
    },
}

/// The band the attempts pass through (see the [module documentation](self)).
#[derive(Clone, Debug)]
pub struct Gate {
    pace: Pace,
    j: u64,
    /// The attempts dispatched and neither handed nor gone, by raw due.
    band: BTreeMap<(u64, Line, u32), Waiter>,
    /// Each one's raw due.
    raws: BTreeMap<(Line, u32), u64>,
}

impl Gate {
    /// An empty gate for a run of jitter `jitter` and catch-up `catch_up`.
    pub fn new(jitter: u64, catch_up: u32) -> Gate {
        Gate {
            pace: Pace::new(jitter, catch_up),
            j: jitter,
            band: BTreeMap::new(),
            raws: BTreeMap::new(),
        }
    }

    /// The pace so far.
    pub fn pace(&self) -> &Pace {
        &self.pace
    }

    /// An attempt dispatched, of raw due `raw` and deadline `deadline`: in
    /// the band, holding its front until it arrives or its deadline passes.
    pub fn dispatched(&mut self, line: Line, attempt: u32, raw: u64, deadline: u64) {
        self.raws.insert((line, attempt), raw);
        self.band.insert(
            (raw, line, attempt),
            Waiter {
                deadline,
                arrived: false,
            },
        );
    }

    /// The attempt ready to pass.
    pub fn arrived(&mut self, line: Line, attempt: u32) {
        if let Some(raw) = self.raws.get(&(line, attempt))
            && let Some(w) = self.band.get_mut(&(*raw, line, attempt))
        {
            w.arrived = true;
        }
    }

    /// The attempt gone from the band otherwise: censored, faulted, dropped.
    pub fn gone(&mut self, line: Line, attempt: u32) {
        if let Some(raw) = self.raws.remove(&(line, attempt)) {
            self.band.remove(&(raw, line, attempt));
        }
    }

    /// Every turn at `now`: the attempts past their deadlines, then in the
    /// band's order those handed off now and those that never can be.
    pub fn turns(&mut self, now: u64) -> Vec<Turn> {
        // Past their deadlines, wherever they stand: never handed, each its
        // report owed — and no instant gone left for [`Gate::next`].
        let mut turns = Vec::new();
        let raws = &mut self.raws;
        self.band.retain(|&(_, line, attempt), w| {
            let keep = w.deadline > now;
            if !keep {
                raws.remove(&(line, attempt));
                turns.push(Turn::Late { line, attempt });
            }
            keep
        });
        // Each removal moves the front: start over from it.
        while let Some(((raw, line, attempt), turn)) = self.turn(now) {
            if let Turn::Pass { .. } = turn {
                self.pace.hand(raw, now);
            }
            turns.push(turn);
            self.band.remove(&(raw, line, attempt));
            self.raws.remove(&(line, attempt));
        }
        turns
    }

    /// The first attempt within the band that leaves it at `now`, and how:
    /// handed off, or late.
    fn turn(&self, now: u64) -> Option<((u64, Line, u32), Turn)> {
        let (&(front, _, _), _) = self.band.first_key_value()?;
        let edge = front.saturating_add(self.j);
        self.band
            .range(..=(edge, Line::MAX, u32::MAX))
            .find_map(|(&(raw, line, attempt), w)| {
                let earliest = self.pace.earliest(raw);
                let turn = if earliest >= w.deadline {
                    Turn::Late { line, attempt }
                } else if w.arrived && earliest <= now {
                    Turn::Pass { line, attempt }
                } else {
                    return None;
                };
                Some(((raw, line, attempt), turn))
            })
    }

    /// The next instant a turn may come: an arrived attempt's earliest within
    /// the band, or a deadline.
    pub fn next(&self) -> Option<u64> {
        let front = self.band.first_key_value().map(|(k, _)| k.0)?;
        let edge = front.saturating_add(self.j);
        let passes = self
            .band
            .range(..=(edge, Line::MAX, u32::MAX))
            .filter(|(_, w)| w.arrived)
            .map(|(k, _)| self.pace.earliest(k.0));
        let deadlines = self.band.values().map(|w| w.deadline);
        passes.chain(deadlines).min()
    }

    /// Whether no attempt waits.
    pub fn is_empty(&self) -> bool {
        self.band.is_empty()
    }
}
