//! A keyed permutation of `0..n` (ADR-0045 decision 4).

use crate::key::Key;

/// The rounds of the Feistel network: four make a pseudorandom permutation of
/// its domain (Luby–Rackoff).
const ROUNDS: usize = 4;

/// A bijection of `0..n` drawn from a key, with its inverse, each O(1) a
/// position and with no table: samples without replacement (the first `s`
/// positions), an arrival order independent of identity.
///
/// A balanced Feistel network over the smallest domain of an even number of
/// bits that holds `n`, at most `4n`, cycle-walked back into `0..n`: from a
/// position below `n`, the network's cycle returns below `n`, about twice
/// on average.
#[derive(Debug)]
pub struct Permutation {
    n: u64,
    half: u32,
    rounds: [Key; ROUNDS],
}

impl Permutation {
    /// The permutation of `0..n` drawn from `key`.
    pub fn new(n: u64, key: Key) -> Permutation {
        let bits = u64::BITS.saturating_sub(n.saturating_sub(1).leading_zeros());
        Permutation {
            n,
            half: bits.div_ceil(2),
            rounds: std::array::from_fn(|r| key.internal("permutation").at(r as u64)),
        }
    }

    /// The size.
    pub fn len(&self) -> u64 {
        self.n
    }

    /// Whether it permutes nothing.
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// The value at `position`, or `None` past the end.
    pub fn at(&self, position: u64) -> Option<u64> {
        (position < self.n).then(|| self.walk(position, |x| self.forward(x)))
    }

    /// The position of `value`, or `None` past the end: the inverse of
    /// [`at`](Self::at).
    pub fn position_of(&self, value: u64) -> Option<u64> {
        (value < self.n).then(|| self.walk(value, |x| self.backward(x)))
    }

    fn walk(&self, start: u64, step: impl Fn(u64) -> u64) -> u64 {
        if self.n <= 1 {
            return start;
        }
        let mut x = step(start);
        while x >= self.n {
            x = step(x);
        }
        x
    }

    fn mask(&self) -> u64 {
        (1u64 << self.half).wrapping_sub(1)
    }

    fn round(&self, r: usize, half: u64) -> u64 {
        self.rounds[r].at(half).bits() & self.mask()
    }

    fn forward(&self, x: u64) -> u64 {
        let (mut left, mut right) = (x >> self.half, x & self.mask());
        for r in 0..ROUNDS {
            (left, right) = (right, left ^ self.round(r, right));
        }
        (left << self.half) | right
    }

    fn backward(&self, x: u64) -> u64 {
        let (mut left, mut right) = (x >> self.half, x & self.mask());
        for r in (0..ROUNDS).rev() {
            (left, right) = (right ^ self.round(r, left), left);
        }
        (left << self.half) | right
    }
}
