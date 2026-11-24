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

use crate::machine::{Input, Judgement, Line, Seen, Unsent};
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

/// Why a closing report was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Left {
    /// Stamped below what was released.
    Below(Below),
    /// At or before its attempt's deadline: it would be taken for its answer.
    Early {
        /// Its stamp.
        at: u64,
        /// The attempt's deadline.
        deadline: u64,
    },
}

impl fmt::Display for Left {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Left::Below(b) => write!(f, "{b}"),
            Left::Early { at, deadline } => {
                write!(f, "a closing report at {at} µs, its deadline {deadline} µs")
            }
        }
    }
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
    /// When it was ready to pass the hand-off (decorated, if its carrier
    /// decorates), when it was handed off, and when its request was handed
    /// to a ready connection: carried by whichever `Sent` is pushed (ADR-0047
    /// decisions 17, 21).
    decorated: u64,
    handed: u64,
    connected: u64,
    /// Its source thread's scheduling lag on its send path (decision 18),
    /// carried likewise.
    lag: u64,
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

/// The frontier's censor at a deadline: marked as such, not an answer.
fn unknown(line: Line, attempt: u32) -> Input {
    Input::Ended {
        line,
        attempt,
        judgement: Judgement::Unknown,
        seen: Seen {
            censored: true,
            ..Seen::default()
        },
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

    /// The oldest answer being judged: its instant, line and attempt.
    pub fn oldest_hold(&self) -> Option<(u64, Line, u32)> {
        self.floors.first().copied()
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

    /// Attempt `attempt` of `line` dispatched at `at`, to end by `deadline`:
    /// handed to its task and connected then, until reported later. If its
    /// deadline was already released, it is censored at once: the censor is
    /// returned, for the caller's own buffer, and nothing of it is kept.
    pub fn open(
        &mut self,
        at: u64,
        line: Line,
        attempt: u32,
        deadline: u64,
    ) -> Option<(u64, Input)> {
        if self.released.is_some_and(|r| deadline <= r) {
            return Some((deadline, unknown(line, attempt)));
        }
        self.open.insert(
            (line, attempt),
            Open {
                deadline,
                sent: false,
                arrived: false,
                decorated: at,
                handed: at,
                connected: at,
                lag: 0,
            },
        );
        self.deadlines.insert((deadline, line, attempt));
        None
    }

    /// Ready to pass the hand-off and handed off at `now` — no gate yet
    /// stands between them (ADR-0047 decision 21's K2b-9c builds it):
    /// connected then too, until reported.
    pub fn handed(&mut self, now: u64, line: Line, attempt: u32) {
        if let Some(o) = self.open.get_mut(&(line, attempt)) {
            o.decorated = now;
            o.handed = now;
            o.connected = now;
        }
    }

    /// Its request handed to a ready connection at `now`: the last such
    /// instant stands, a reconnect's.
    pub fn connected(&mut self, now: u64, line: Line, attempt: u32) {
        if let Some(o) = self.open.get_mut(&(line, attempt)) {
            o.connected = now;
        }
    }

    /// The scheduling lag its source's thread kept its send path waiting,
    /// in microseconds (decision 18): carried by whichever `Sent` is pushed.
    pub fn lag(&mut self, line: Line, attempt: u32, lag: u64) {
        if let Some(o) = self.open.get_mut(&(line, attempt)) {
            o.lag = lag;
        }
    }

    /// Its bytes left at `now`: pushed if before its deadline, once —
    /// whether it was.
    pub fn sent(&mut self, now: u64, line: Line, attempt: u32) -> Result<bool, Below> {
        let Some(o) = self.open.get_mut(&(line, attempt)) else {
            return Ok(false);
        };
        if o.sent || o.arrived || now >= o.deadline {
            return Ok(false);
        }
        o.sent = true;
        let (decorated, handed, connected, lag) = (o.decorated, o.handed, o.connected, o.lag);
        self.push(
            now,
            Input::Sent {
                line,
                attempt,
                decorated,
                handed,
                connected,
                lag,
            },
        )
        .map(|()| true)
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
            // An early reply's: its path as reported, at the reply.
            let sent = Input::Sent {
                line,
                attempt,
                decorated: o.decorated,
                handed: o.handed,
                connected: o.connected,
                lag: o.lag,
            };
            self.push(at, sent)?;
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

    /// A never-sent attempt's closing report at `now` (ADR-0047 decision 19):
    /// after its censor — censored first if the drain has not reached its
    /// deadline — never at or before the deadline, where it would be taken
    /// for the attempt's answer.
    pub fn left(&mut self, now: u64, line: Line, attempt: u32, unsent: Unsent) -> Result<(), Left> {
        if let Some(o) = self.open.get(&(line, attempt)) {
            let deadline = o.deadline;
            if now <= deadline {
                return Err(Left::Early { at: now, deadline });
            }
            self.deadlines.remove(&(deadline, line, attempt));
            self.open.remove(&(line, attempt));
            self.push(deadline, unknown(line, attempt))
                .map_err(Left::Below)?;
        }
        self.push(
            now,
            Input::Ended {
                line,
                attempt,
                judgement: Judgement::Unknown,
                seen: Seen {
                    unsent: Some(unsent),
                    ..Seen::default()
                },
            },
        )
        .map_err(Left::Below)
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
