//! The pacing a stage's keys are drawn and unwrapped by, pure, shared by
//! the sealing end's core (`supply`) and the opening end's (`unwraps`): the
//! evidence of whether a flight lands in time, a failure's back-off, the
//! places of the flights a stage runs at once, the scope of a refusal
//! (ADR-0039, ADR-0040 decision 3). Nothing here reads a clock or draws a
//! key.
//!
//! It does not panic: its arithmetic saturates or is checked (ADR-0039
//! decision 11).

#![deny(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::unreachable,
    clippy::todo,
    clippy::unimplemented
)]

use std::time::{Duration, Instant};

/// The number of a flight, one counter per stage, never reused.
pub type Generation = u64;

/// The longest a key's lifetime or a back-off's cap may be: a century,
/// so that an instant plus any duration of a supply is an instant on every
/// platform.
pub const LONGEST: Duration = Duration::from_secs(100 * 365 * 86_400);

/// A fraction in [0, 1) of a random word.
pub(crate) fn fraction(random: u64) -> f64 {
    #[allow(clippy::cast_precision_loss, reason = "53 bits, exactly")]
    let fraction = (random >> 11) as f64 / (1u64 << 53) as f64;
    fraction
}

/// `span` scaled by a random fraction.
pub(crate) fn part(span: Duration, random: u64) -> Duration {
    span.mul_f64(fraction(random))
}

/// What a flight is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// A seal found no key.
    Miss,
    /// The key in service nears its end.
    Renew,
    /// The first draw of a slot whose back-off ended.
    Probe,
}

/// What became of a flight.
#[derive(Debug)]
pub enum Outcome<K> {
    /// A key, drawn by a draw started at `started`.
    Drawn {
        /// The key.
        key: K,
        /// When its draw started: its life counts from here.
        started: Instant,
    },
    /// The tenant's key is not there.
    Gone,
    /// The KMS refused, for a reason a deploy mends.
    Refused,
    /// The KMS could not answer now.
    Unavailable,
    /// Something the stage cannot attribute.
    Moment,
    /// The draw passed its bound and was given up.
    Abandoned,
    /// The draw's task ended without an answer: a panic, a shutdown.
    Canceled,
}

/// Why a seal was not served.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    /// It waited its bound for a draw that had not landed.
    Waited,
    /// The slot's last draw failed; it backs off.
    BackingOff,
    /// A draw is in flight that has not landed in time before.
    Late,
    /// The stage runs as many flights as it may.
    FlightsFull,
    /// No runtime would run the draw.
    Unspawned,
}

/// Whose seals a refusal stops, as far as the stage can tell (ADR-0040
/// decision 3): every tenant's, when the KMS serves no one, or this
/// tenant's alone, when a draw of another tenant landed in time since this
/// one's trouble began.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Scope {
    /// The stage cannot serve now: no draw has landed in time since this
    /// tenant's trouble began, or the stage itself is full or has no
    /// runtime.
    Stage,
    /// Another tenant's draw landed in time since: this tenant's own.
    Tenant,
}

/// The scope a refusal on evidence has: the tenant's own when a draw landed
/// in time after `since`, its trouble's start; the stage's otherwise.
pub(crate) fn scope_by(since: Option<Instant>, landed_in_time: Option<Instant>) -> Scope {
    match (since, landed_in_time) {
        (Some(since), Some(landed)) if landed > since => Scope::Tenant,
        _ => Scope::Stage,
    }
}

/// A flight the shell is to start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Start {
    /// Its number.
    pub generation: Generation,
    /// What it is for.
    pub kind: Kind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Failure {
    Gone,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Evidence {
    Trusted,
    Late,
    Failed {
        failure: Failure,
        until: Instant,
        failures: u32,
        at: Instant,
    },
}

/// A flight in flight: its number, what it is for, when it started.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Flying {
    pub(crate) generation: Generation,
    pub(crate) kind: Kind,
    pub(crate) started: Instant,
}

/// `at` plus `span`. Every span of a supply is at most [`LONGEST`], which
/// no monotonic clock's instant overflows; were one to, `at` itself.
pub(crate) fn later(at: Instant, span: Duration) -> Instant {
    at.checked_add(span).unwrap_or(at)
}

/// The places of a stage's flights, by kind.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Held {
    pub(crate) misses: u32,
    pub(crate) renewals: u32,
    pub(crate) probes: u32,
}

impl Held {
    pub(crate) const fn total(self) -> u32 {
        self.misses
            .saturating_add(self.renewals)
            .saturating_add(self.probes)
    }

    pub(crate) const fn of(&mut self, kind: Kind) -> &mut u32 {
        match kind {
            Kind::Miss => &mut self.misses,
            Kind::Renew => &mut self.renewals,
            Kind::Probe => &mut self.probes,
        }
    }
}

/// A failure's memory after `failures` before it, at `now` (ADR-0039
/// decision 6): a key gone is remembered for `gone`; anything else backs
/// off half its doubled step and a random part of the other half, at most
/// the cap.
pub(crate) fn failed(
    gone: Option<Duration>,
    backoff: (Duration, Duration),
    failures: u32,
    now: Instant,
    random: u64,
) -> Evidence {
    match gone {
        Some(gone) => Evidence::Failed {
            failure: Failure::Gone,
            until: later(now, gone),
            failures: failures.saturating_add(1),
            at: now,
        },
        None => {
            let step = backoff
                .0
                .saturating_mul(1u32.checked_shl(failures.min(16)).unwrap_or(u32::MAX))
                .min(backoff.1);
            let half = step.checked_div(2).unwrap_or(Duration::ZERO);
            Evidence::Failed {
                failure: Failure::Other,
                until: later(now, half.saturating_add(part(half, random))),
                failures: failures.saturating_add(1),
                at: now,
            }
        }
    }
}
