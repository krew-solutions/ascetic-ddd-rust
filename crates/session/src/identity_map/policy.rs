//! What a transaction at each isolation level may remember without lying.
//!
//! | level | an entity | an absence |
//! |---|---|---|
//! | read uncommitted, read committed | no | no |
//! | repeatable read | yes | no |
//! | serializable | yes | yes |
//!
//! Below repeatable read a row may change between two reads, so nothing may
//! be served from memory. At repeatable read a row that was seen stays as it
//! was, but a row that was *not* seen may be inserted by another transaction
//! in the meantime, so a remembered absence would become a phantom. Only a
//! serializable transaction rules that out too.
//!
//! The same predicate gates writes and reads, so an answer never depends on
//! the level a fact happened to be remembered under.

use super::slot::Kind;
use crate::isolation::IsolationLevel;

pub(super) fn admits(level: IsolationLevel, kind: Kind) -> bool {
    match (level, kind) {
        (IsolationLevel::ReadUncommitted | IsolationLevel::ReadCommitted, _) => false,
        (IsolationLevel::RepeatableRead, Kind::Entity) => true,
        (IsolationLevel::RepeatableRead, Kind::Absence) => false,
        (IsolationLevel::Serializable, _) => true,
    }
}
