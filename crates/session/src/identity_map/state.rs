//! The map's memory: a slot per key, and a window over the anchored ones.
//!
//! One invariant: a key is in the window exactly when its slot is anchored.
//! The window says which keys it lets go of; `release` turns their slots into
//! released ones, or forgets them.
//!
//! A released slot dies when the domain drops the entity, and nothing tells
//! the map — Rust's `Weak` has no callback, unlike Python's weak dictionary.
//! Dead slots are swept once every `len` operations: a sweep costs a pass over
//! the slots, so that is a constant per operation, and a dead slot outlives
//! its entity by at most as many operations as there are slots.

use std::collections::HashMap;

use super::Lookup;
use super::key::{DynKey, KeyBox};
use super::slot::{AnyLookup, Fact, Slot};
use super::window::Window;

pub(super) struct State {
    slots: HashMap<KeyBox, Slot>,
    window: Window,
    /// Operations since the last sweep of dead slots.
    since_sweep: usize,
}

impl State {
    pub(super) fn new(window: Window) -> Self {
        State {
            slots: HashMap::new(),
            window,
            since_sweep: 0,
        }
    }

    /// Remembers a fact as just used, replacing what the key had.
    pub(super) fn remember(&mut self, key: KeyBox, fact: Fact) {
        self.slots.insert(key.clone(), Slot::Anchored(fact));
        let gone = self.window.admit(key);
        self.release(gone);
        self.sweep_when_due();
    }

    /// Answers for the key, and records the use.
    pub(super) fn recall(&mut self, key: &(dyn DynKey + 'static)) -> AnyLookup {
        let Some((key, slot)) = self.slots.remove_entry(key) else {
            return Lookup::Unknown;
        };
        let (next, answer) = slot.recall();
        match next {
            Some(slot) => {
                self.slots.insert(key.clone(), slot);
                let gone = self.window.admit(key);
                self.release(gone);
            }
            None => self.window.remove(key.as_dyn()),
        }
        self.sweep_when_due();
        answer
    }

    pub(super) fn forget(&mut self, key: &(dyn DynKey + 'static)) {
        self.slots.remove(key);
        self.window.remove(key);
    }

    pub(super) fn forget_all(&mut self) {
        self.slots.clear();
        self.window.clear();
    }

    /// Number of keys that can still be answered for. Entities the domain has
    /// let go of since their release are forgotten on the way.
    pub(super) fn len(&mut self) -> usize {
        self.sweep();
        self.slots.len()
    }

    pub(super) fn resize(&mut self, size: usize) {
        let gone = self.window.resize(size);
        self.release(gone);
    }

    /// Forgets the released slots whose entity has died.
    fn sweep(&mut self) {
        self.slots.retain(|_, slot| slot.is_alive());
        self.since_sweep = 0;
    }

    /// Sweeps once every `len` operations, so that sweeping costs a constant
    /// per operation and a dead slot does not outlive its entity for long.
    fn sweep_when_due(&mut self) {
        self.since_sweep += 1;
        if self.since_sweep > self.slots.len() {
            self.sweep();
        }
    }

    /// The window let these keys go: their slots leave the anchored state.
    fn release(&mut self, gone: Vec<KeyBox>) {
        for key in gone {
            if let Some(next) = self.slots.remove(&key).and_then(Slot::release) {
                self.slots.insert(key, next);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::State;
    use crate::identity_map::IdentityKey;
    use crate::identity_map::key::KeyBox;
    use crate::identity_map::recency::Recency;
    use crate::identity_map::slot::{AnyArc, Fact};
    use crate::identity_map::window::Window;

    #[derive(Clone, PartialEq, Eq, Hash)]
    struct Key(u32);

    impl IdentityKey for Key {
        type Entity = u32;
    }

    fn entity(id: u32) -> AnyArc {
        Arc::new(id)
    }

    /// A stream that holds entities past their eviction and then drops them
    /// must not leave a dead slot per entity behind.
    #[test]
    fn dead_released_slots_are_swept_as_the_map_is_used() {
        let mut state = State::new(Window::Recency(Recency::new(1)));

        let held: Vec<AnyArc> = (0..100)
            .map(|id| {
                let entity = entity(id);
                state.remember(KeyBox::new(Key(id)), Fact::Entity(Arc::clone(&entity)));
                entity
            })
            .collect();
        assert_eq!(
            state.slots.len(),
            100,
            "one anchored, ninety-nine released and alive"
        );

        drop(held);
        for id in 100..300 {
            state.remember(KeyBox::new(Key(id)), Fact::Entity(entity(id)));
        }

        assert!(state.slots.len() <= 2, "{} slots left", state.slots.len());
    }
}
