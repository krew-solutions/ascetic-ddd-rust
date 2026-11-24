//! Whether a machine holds a plan (ADR-0047 decision 7, K1e-2): the plan's
//! own bound, which the validator committed, against one budget of bytes — a
//! refusal before T0, naming the part, never a machine killed hours in.
//!
//! A step a machine holds weighs [`ENDED_CELL`] once it ended, [`LIVE_CELL`]
//! while it has not — its cell, its wake, its inputs in flight, its share of
//! a burst's outputs, its record aside —; a barrier key [`BARRIER`] and its
//! bytes. Its record, and what the machine copies of it, are priced by the
//! plan's committed shapes (decision 22, K2b-7e-2b): the records of the steps
//! not ended as they hold decoded ([`shape::bound`]); the producers their
//! captured holes are bound to, a subject and a slot copied each; the values
//! those holes take, at most [`VALUE`] bytes each; a dependency's binding and
//! its producer's waiter; what the held keep once ended — their keys, their
//! captures. The costs are upper bounds `tests/costs.rs` measures and pins:
//! some 30 % above the worst it measured, each count just past a power of two
//! where a container grows with it, a block moved counted at its old and new
//! sizes at once.

use std::mem::size_of;

use ascetic_ddd_command_log::answer::TAKEN;
use ascetic_ddd_command_log::fills::Captured;
use ascetic_ddd_command_log::record::{Header, Name, On, Subject};
use ascetic_ddd_command_log::shape::{self, Shape};
use ascetic_ddd_command_log::validate::{Kept, Peaks};

use crate::machine::Line;

/// The bytes an ended step's cell holds at most, its share of the window and
/// of the cells' map included, its keys and captures aside.
pub const ENDED_CELL: u64 = 1_216;
/// The bytes a step not ended holds at most beside its record: its cell, its
/// wake, its inputs in flight, its share of the outputs of a burst released
/// at one instant.
pub const LIVE_CELL: u64 = 2_944;
/// The bytes a barrier key holds at most beside its own bytes: its summary
/// and its map node.
pub const BARRIER: u64 = 768;
/// The bytes a captured value holds at most: a carried text of at most
/// `TAKEN` bytes, quoted, each byte escaped at worst (`answer::take`).
pub const VALUE: u64 = 2 * TAKEN as u64 + 2;

/// What a machine may hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budget {
    /// Its memory for the run, in bytes.
    pub bytes: u64,
}

/// Why a plan does not fit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// A read-ahead past the header's: the bound assumes no more.
    ReadAhead {
        /// The run's.
        asked: u64,
        /// The header's.
        planned: u64,
    },
    /// More bytes than the budget.
    Bytes {
        /// What the plan needs.
        needed: u64,
        /// What the machine has.
        budget: u64,
        /// The steps held, those not ended, and the barrier keys, by which
        /// the need was counted.
        held: u64,
        /// See `held`.
        pending: u64,
        /// See `held`.
        barriers: u64,
    },
}

fn bytes_of(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

/// Slots of `size` bytes for `n` elements pushed one at a time into
/// sequences, at most `sequences` of them not empty: twice the elements and
/// two a sequence.
fn slots(n: u64, sequences: u64, size: usize) -> u64 {
    n.saturating_mul(2)
        .saturating_add(sequences.saturating_mul(2))
        .saturating_mul(bytes_of(size))
}

/// What the machine copies of the records of shape `s` while their steps are
/// not ended: the producers their captured holes are bound to and the values
/// those take, each a subject and a slot copied; a dependency's binding and
/// its producer's waiter.
fn bound_to(s: &Shape) -> u64 {
    let holes = s.holes;
    let producers = slots(holes, holes, size_of::<(Subject, Name, Line)>());
    let values =
        slots(holes, holes, size_of::<Captured>()).saturating_add(holes.saturating_mul(VALUE));
    let deps = s.deps.elements;
    let bindings = slots(deps, s.deps.sequences, size_of::<Line>()).saturating_add(slots(
        deps,
        deps,
        size_of::<(Line, On)>(),
    ));
    producers
        .saturating_add(values)
        .saturating_add(s.hole_bytes.saturating_mul(2))
        .saturating_add(bindings)
}

/// What the held keep once ended: their keys' bytes, their captures — each a
/// value, a subject and a slot.
fn keeping(k: &Kept) -> u64 {
    k.keys
        .saturating_add(slots(k.captures, k.captures, size_of::<Captured>()))
        .saturating_add(k.captures.saturating_mul(VALUE))
        .saturating_add(k.capture_bytes)
}

/// The bytes a plan of these peaks needs.
pub fn needed(peaks: &Peaks) -> u64 {
    let live = LIVE_CELL.saturating_sub(ENDED_CELL);
    peaks
        .held
        .saturating_mul(ENDED_CELL)
        .saturating_add(peaks.pending.saturating_mul(live))
        .saturating_add(peaks.barriers.saturating_mul(BARRIER))
        .saturating_add(peaks.barrier_bytes)
        .saturating_add(shape::bound(&peaks.pending_shape))
        .saturating_add(bound_to(&peaks.pending_shape))
        .saturating_add(keeping(&peaks.kept))
}

/// Whether a machine of `budget`, reading `read_ahead` ahead, holds the plan
/// of `header` whose commitment carries `peaks`: the bytes it needs, or why
/// not.
pub fn fits(
    peaks: &Peaks,
    header: &Header,
    read_ahead: u64,
    budget: &Budget,
) -> Result<u64, Refusal> {
    if read_ahead > header.read_ahead {
        return Err(Refusal::ReadAhead {
            asked: read_ahead,
            planned: header.read_ahead,
        });
    }
    let needed = needed(peaks);
    if needed > budget.bytes {
        return Err(Refusal::Bytes {
            needed,
            budget: budget.bytes,
            held: peaks.held,
            pending: peaks.pending,
            barriers: peaks.barriers,
        });
    }
    Ok(needed)
}
