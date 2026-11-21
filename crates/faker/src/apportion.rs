//! A fixed total split by weights, exactly (ADR-0045 decision 4).

use crate::key::Unit;
use crate::law::Categorical;

/// `total` split among the items of `weights` by keyed systematic rounding.
///
/// With `Qᵢ = total · Tᵢ / 2⁵²` the exact share of the items before `i` and `u`
/// the unit, the items before `i` get `Sᵢ = ⌊Qᵢ + u⌋` and item `i` gets
/// `Sᵢ₊₁ − Sᵢ`. So:
/// - the counts sum to `total` exactly;
/// - each count is within 1 of `total · pᵢ`;
/// - averaged over `u`, each count is `total · pᵢ` exactly, so across scopes —
///   each with its own `u` — no item is favoured. Largest remainders would give
///   the same items the extra one in every scope.
///
/// Computed in `u128`: `2 · total · Tᵢ` is at most 2¹¹⁷.
///
/// `start(i)` is O(1) and needs no table, so the children of parent `i` of a
/// population apportioned over its parents are `start(i)..start(i + 1)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Apportionment {
    total: u64,
    weights: Categorical,
    k: u64,
}

/// Splits `total` among the items of `weights`, at the unit `u`.
pub fn apportion(total: u64, weights: Categorical, u: Unit) -> Apportionment {
    Apportionment {
        total,
        weights,
        k: u.raw(),
    }
}

impl Apportionment {
    /// The number of items.
    pub fn len(&self) -> usize {
        self.weights.len()
    }

    /// Whether there are no items: never.
    pub fn is_empty(&self) -> bool {
        self.weights.is_empty()
    }

    /// The total split.
    pub fn total(&self) -> u64 {
        self.total
    }

    /// `Sᵢ`: what the items before `i` get, for `i` in `0..=len`.
    pub fn start(&self, i: usize) -> u64 {
        // ⌊(total · Tᵢ + k + ½) / 2⁵²⌋ = ⌊(2·total·Tᵢ + 2k + 1) / 2⁵³⌋, exact.
        let t = u128::from(self.weights.threshold(i));
        let numerator = u128::from(self.total)
            .wrapping_mul(t)
            .wrapping_mul(2)
            .wrapping_add(u128::from(self.k).wrapping_mul(2))
            .wrapping_add(1);
        (numerator >> (Unit::BITS + 1)) as u64
    }

    /// What item `i` gets.
    pub fn count(&self, i: usize) -> u64 {
        self.start(i.saturating_add(1))
            .saturating_sub(self.start(i))
    }

    /// Every item's count, in order.
    pub fn counts(&self) -> impl Iterator<Item = u64> + '_ {
        (0..self.len()).map(|i| self.count(i))
    }
}
