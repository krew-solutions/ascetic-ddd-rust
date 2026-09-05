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

impl IsolationLevel {
    /// True if the map remembers entities it has been given.
    pub(crate) fn caches_present(self) -> bool {
        matches!(
            self,
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable
        )
    }

    /// True if the map remembers that an entity does *not* exist.
    ///
    /// Only a serializable transaction may do this: at a weaker level another
    /// transaction may insert the row in the meantime, so a remembered absence
    /// would become a phantom.
    pub(crate) fn caches_absent(self) -> bool {
        matches!(self, IsolationLevel::Serializable)
    }
}
