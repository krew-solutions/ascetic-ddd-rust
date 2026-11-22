//! The watermark, constructed (ADR-0047 decision 16, amending decision 4).
//!
//! The machine takes inputs in the order of the instants their sources saw
//! them, up to the least instant any source may still report (`order`). A
//! source awaiting an answer for seconds reports nothing meanwhile, so that
//! instant is not each source's last stamp: it is built here, from what every
//! attempt still out may yet report.
//!
//! - An attempt is opened at its dispatch with its deadline. Its bytes leaving
//!   are pushed as `Sent` only before the deadline.
//! - An answer arriving holds a floor at its instant while it is read and
//!   judged, and is then pushed at that instant — with `Sent` before it if none
//!   was (a gateway's reply before the request's last byte left). An answer
//!   past its deadline first censors the attempt there; it is then pushed at
//!   its own instant, and the machine ignores it as late.
//! - A drain releases what was pushed through the least of `now − 1` (`now`
//!   when the caller knows nothing more can be stamped at `now`) and the least
//!   floor less one; first, every attempt open with no answer and a deadline
//!   within that bound is censored — `Ended(Unknown)` at its deadline. So an
//!   answer that arrived by its deadline is never censored, however late its
//!   task ran, and no input is released before one stamped earlier.
//! - An attempt opened with its deadline already released is censored at once.
//! - A push stamped below what was released is refused: the shell's harness
//!   fault.
//!
//! The frontier knows no clock: the shell stamps every input under one lock
//! around it; `sim` drives it in virtual time.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use crate::machine::{Input, Judgement, Line, Seen};
use crate::order::Ordered;

/// An answer being read and judged: its attempt, the instant it arrived, and
/// whether its attempt was censored first.
#[derive(Debug, PartialEq, Eq)]
pub struct Hold {
    line: Line,
    attempt: u32,
    at: u64,
    late: bool,
}

impl Hold {
    /// When the answer arrived: the instant it is pushed at.
    pub fn at(&self) -> u64 {
        self.at
    }

    /// Whether it came past its attempt's deadline.
    pub fn late(&self) -> bool {
        self.late
    }
}

/// A push stamped below what the frontier released: the order the machine
/// took its inputs in would not be the order of their stamps.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Below {
    /// The push's stamp.
    pub at: u64,
    /// What was released.
    pub released: u64,
}

impl fmt::Display for Below {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "an input stamped {} after {} was released",
            self.at, self.released
        )
    }
}

impl std::error::Error for Below {}

#[derive(Debug)]
struct Open {
    deadline: u64,
    sent: bool,
    /// Its answer arrived in time: no longer censorable.
    arrived: bool,
}

/// See the [module documentation](self).
#[derive(Debug, Default)]
pub struct Frontier {
    pushed: Ordered,
    /// Attempts out and not censored, until their answer is pushed.
    open: BTreeMap<(Line, u32), Open>,
    /// The deadlines of those with no answer yet.
    deadlines: BTreeSet<(u64, Line, u32)>,
    /// The instants of the answers being judged.
    floors: BTreeSet<(u64, Line, u32)>,
    released: Option<u64>,
}

fn unknown(line: Line, attempt: u32) -> Input {
    Input::Ended {
        line,
        attempt,
        judgement: Judgement::Unknown,
        seen: Seen::default(),
    }
}

impl Frontier {
    /// Nothing out, nothing released.
    pub fn new() -> Frontier {
        Frontier::default()
    }

    /// What was released so far, if anything.
    pub fn released(&self) -> Option<u64> {
        self.released
    }

    /// The earliest deadline of an attempt with no answer.
    pub fn next_deadline(&self) -> Option<u64> {
        self.deadlines.first().map(|d| d.0)
    }

    /// The stamp of the earliest input pushed and not yet released.
    pub fn next_pushed(&self) -> Option<u64> {
        self.pushed.first()
    }

    /// The instant of the oldest answer being judged.
    pub fn floor(&self) -> Option<u64> {
        self.floors.first().map(|f| f.0)
    }

    /// Attempts out and not censored.
    pub fn open_attempts(&self) -> usize {
        self.open.len()
    }

    fn push(&mut self, at: u64, input: Input) -> Result<(), Below> {
        if let Some(released) = self.released
            && at < released
        {
            return Err(Below { at, released });
        }
        self.pushed.push(at, input);
        Ok(())
    }

    /// Attempt `attempt` of `line` dispatched, to end by `deadline`. If its
    /// deadline was already released, it is censored at once: the censor is
    /// returned, for the caller's own buffer, and nothing of it is kept.
    pub fn open(&mut self, line: Line, attempt: u32, deadline: u64) -> Option<(u64, Input)> {
        if self.released.is_some_and(|r| deadline <= r) {
            return Some((deadline, unknown(line, attempt)));
        }
        self.open.insert(
            (line, attempt),
            Open {
                deadline,
                sent: false,
                arrived: false,
            },
        );
        self.deadlines.insert((deadline, line, attempt));
        None
    }

    /// Its bytes left at `now`: pushed if before its deadline, once.
    pub fn sent(&mut self, now: u64, line: Line, attempt: u32) -> Result<(), Below> {
        let Some(o) = self.open.get_mut(&(line, attempt)) else {
            return Ok(());
        };
        if o.sent || o.arrived || now >= o.deadline {
            return Ok(());
        }
        o.sent = true;
        self.push(now, Input::Sent { line, attempt })
    }

    /// Its answer arrived at `now`: held until it is judged. Past its deadline
    /// the attempt is censored there first, the answer then late.
    pub fn arrived(&mut self, now: u64, line: Line, attempt: u32) -> Result<Hold, Below> {
        let late = match self.open.get_mut(&(line, attempt)) {
            Some(o) if now <= o.deadline && !o.arrived => {
                o.arrived = true;
                self.deadlines.remove(&(o.deadline, line, attempt));
                false
            }
            Some(o) if !o.arrived => {
                let deadline = o.deadline;
                self.deadlines.remove(&(deadline, line, attempt));
                self.open.remove(&(line, attempt));
                self.push(deadline, unknown(line, attempt))?;
                true
            }
            // A second answer, or one of an attempt censored and forgotten.
            _ => true,
        };
        self.floors.insert((now, line, attempt));
        Ok(Hold {
            line,
            attempt,
            at: now,
            late,
        })
    }

    /// Its answer judged: pushed at the instant it arrived, `Sent` before it
    /// if none was.
    pub fn ended(&mut self, hold: Hold, judgement: Judgement, seen: Seen) -> Result<(), Below> {
        let Hold {
            line,
            attempt,
            at,
            late,
        } = hold;
        self.floors.remove(&(at, line, attempt));
        if !late
            && let Some(o) = self.open.remove(&(line, attempt))
            && !o.sent
        {
            self.push(at, Input::Sent { line, attempt })?;
        }
        self.push(
            at,
            Input::Ended {
                line,
                attempt,
                judgement,
                seen,
            },
        )
    }

    /// Forgets an attempt nothing more will come of: its connection abandoned
    /// after its censor.
    pub fn close(&mut self, line: Line, attempt: u32) {
        if let Some(o) = self.open.remove(&(line, attempt)) {
            self.deadlines.remove(&(o.deadline, line, attempt));
        }
    }

    /// What may be taken: everything pushed through the least of `now − 1` —
    /// `now` when `settled` — and the oldest floor less one, after censoring
    /// every attempt with no answer whose deadline lies within it.
    pub fn drain(&mut self, now: u64, settled: bool) -> Vec<(u64, Input)> {
        let bound = if settled {
            Some(now)
        } else {
            now.checked_sub(1)
        };
        let through = match (bound, self.floor()) {
            (Some(b), Some(f)) => f.checked_sub(1).map(|f| b.min(f)),
            (b, None) => b,
            (None, Some(_)) => None,
        };
        let Some(through) = through else {
            return Vec::new();
        };
        while let Some(&(deadline, line, attempt)) = self.deadlines.first() {
            if deadline > through {
                break;
            }
            self.deadlines.pop_first();
            self.open.remove(&(line, attempt));
            // Within what is about to be released: never below it.
            self.pushed.push(deadline, unknown(line, attempt));
        }
        let mut out = Vec::new();
        while let Some(x) = self.pushed.pop(through) {
            out.push(x);
        }
        self.released = Some(self.released.map_or(through, |r| r.max(through)));
        out
    }
}
