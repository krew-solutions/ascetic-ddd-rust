//! What the map knows about one key, and how that knowledge changes.
//!
//! Knowledge has two states. While a key is in the recency window the map
//! itself keeps the fact alive: the slot is *anchored*. Once the window lets
//! it go, an entity may live on because the domain still holds it — the slot
//! is *released* and holds it weakly — whereas an absence has no such
//! afterlife and is simply forgotten.
//!
//! Each transition is a function from a slot to the next slot, or to nothing.
//! The state machine lives here; the map only applies it.

use std::any::Any;
use std::sync::{Arc, Weak};

use super::Lookup;

pub(super) type AnyArc = Arc<dyn Any + Send + Sync>;
pub(super) type AnyWeak = Weak<dyn Any + Send + Sync>;
pub(super) type AnyLookup = Lookup<dyn Any + Send + Sync>;

/// The kind of fact a level may or may not admit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    Entity,
    Absence,
}

/// A fact the map remembers about a key.
pub(super) enum Fact {
    Entity(AnyArc),
    Absence,
}

impl Fact {
    pub(super) fn kind(&self) -> Kind {
        match self {
            Fact::Entity(_) => Kind::Entity,
            Fact::Absence => Kind::Absence,
        }
    }

    fn recall(&self) -> AnyLookup {
        match self {
            Fact::Entity(entity) => Lookup::Found(Arc::clone(entity)),
            Fact::Absence => Lookup::Absent,
        }
    }
}

/// What the map knows about a key.
pub(super) enum Slot {
    /// In the window, kept alive by the map; `since` is its place in the
    /// recency order.
    Anchored { since: u64, fact: Fact },
    /// Out of the window: alive only while the domain holds the entity.
    Released(AnyWeak),
}

impl Slot {
    /// The slot's place in the recency order, if it is in the window.
    pub(super) fn since(&self) -> Option<u64> {
        match self {
            Slot::Anchored { since, .. } => Some(*since),
            Slot::Released(_) => None,
        }
    }

    /// True while the slot can still answer.
    pub(super) fn is_alive(&self) -> bool {
        match self {
            Slot::Anchored { .. } => true,
            Slot::Released(weak) => weak.strong_count() > 0,
        }
    }

    /// Leaves the window. An entity the domain still holds is kept weakly;
    /// an entity nobody holds, or an absence, is forgotten.
    pub(super) fn release(self) -> Option<Slot> {
        match self {
            Slot::Anchored {
                fact: Fact::Entity(entity),
                ..
            } => {
                let weak = Arc::downgrade(&entity);
                drop(entity);
                (weak.strong_count() > 0).then_some(Slot::Released(weak))
            }
            Slot::Anchored {
                fact: Fact::Absence,
                ..
            } => None,
            released @ Slot::Released(_) => Some(released),
        }
    }

    /// Answers for the key and moves it to the front of the window: what has
    /// just been used is recently used. A released entity the domain has let
    /// go of is found to be gone, and the slot with it.
    pub(super) fn recall(self, now: u64) -> (Option<Slot>, AnyLookup) {
        match self {
            Slot::Anchored { fact, .. } => {
                let answer = fact.recall();
                (Some(Slot::Anchored { since: now, fact }), answer)
            }
            Slot::Released(weak) => match weak.upgrade() {
                Some(entity) => (
                    Some(Slot::Anchored {
                        since: now,
                        fact: Fact::Entity(Arc::clone(&entity)),
                    }),
                    Lookup::Found(entity),
                ),
                None => (None, Lookup::Unknown),
            },
        }
    }
}
