//! Whether a machine holds a plan (ADR-0047 decision 7, K1e-2): the plan's
//! own bound, which the validator committed, against one budget of bytes — a
//! refusal before T0, naming the part, never a machine killed hours in.
//!
//! A step a machine holds weighs [`ENDED_CELL`] once it ended (its record
//! dropped), [`LIVE_CELL`] and its line's bytes while it has not; a barrier
//! key [`BARRIER`]. The steps not ended are among those held, so a plan needs
//! `held · ENDED_CELL + pending · (LIVE_CELL − ENDED_CELL + line) + barriers ·
//! BARRIER`. The costs are upper bounds `tests/costs.rs` measures and pins:
//! some 30 % above what it measured on the fixture's records (2026-11-22: 896
//! B an ended step, 1,389 B a step not ended beside its 270-byte line, 556 B a
//! key).

use ascetic_ddd_command_log::record::Header;
use ascetic_ddd_command_log::validate::Peaks;

/// The bytes an ended step's cell holds at most, its share of the window and
/// of the cells' map included.
pub const ENDED_CELL: u64 = 1_152;
/// The bytes a step not ended holds at most beside its line's: its cell with
/// its record's structure, its wake, its inputs in flight.
pub const LIVE_CELL: u64 = 1_792;
/// The bytes a barrier key holds at most: its summary and its map node.
pub const BARRIER: u64 = 768;

/// What a machine may hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budget {
    /// Its memory for the run, in bytes.
    pub bytes: u64,
    /// The mean bytes of the log's step lines: a step not ended holds its
    /// record.
    pub line: u64,
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

/// The bytes a plan of these peaks needs, its lines of `line` bytes on mean.
pub fn needed(peaks: &Peaks, line: u64) -> u64 {
    let live = LIVE_CELL.saturating_sub(ENDED_CELL).saturating_add(line);
    peaks
        .held
        .saturating_mul(ENDED_CELL)
        .saturating_add(peaks.pending.saturating_mul(live))
        .saturating_add(peaks.barriers.saturating_mul(BARRIER))
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
    let needed = needed(peaks, budget.line);
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
