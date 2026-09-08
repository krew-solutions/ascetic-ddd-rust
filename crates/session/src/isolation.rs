//! Isolation levels of the identity map.
//!
//! The level decides what the map is allowed to remember, mirroring the
//! guarantees of the surrounding database transaction: caching a row that the
//! transaction is not allowed to see twice would turn the map into a source of
//! stale reads.

/// How much the identity map is allowed to cache.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IsolationLevel {
    /// The map is disabled: every lookup misses.
    ReadUncommitted,
    /// The map is disabled: every lookup misses.
    ReadCommitted,
    /// Prevents repeated queries for existing entities only.
    RepeatableRead,
    /// Prevents repeated queries for both existing and absent entities.
    #[default]
    Serializable,
}
