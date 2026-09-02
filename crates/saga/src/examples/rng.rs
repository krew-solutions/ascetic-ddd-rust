//! Deterministic pseudo-random reservation identifiers.
//!
//! The counterpart of the `_rnd = random.Random(seed)` class attribute of the
//! Python examples: one seeded generator per activity type, shared by every
//! instance of that type.

use std::sync::atomic::{AtomicU64, Ordering};

/// A seeded xorshift generator producing reservation identifiers.
pub(super) struct SeededRng(AtomicU64);

impl SeededRng {
    /// Creates a generator with the given (non-zero) seed.
    pub(super) const fn new(seed: u64) -> Self {
        SeededRng(AtomicU64::new(seed))
    }

    /// Returns the next reservation identifier, in `0..=99_999`.
    pub(super) fn next_reservation_id(&self) -> i64 {
        let previous = self
            .0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |state| {
                Some(Self::step(state))
            })
            .expect("the update never fails");
        (Self::step(previous) % 100_000) as i64
    }

    /// One round of xorshift64.
    const fn step(mut state: u64) -> u64 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    }
}
