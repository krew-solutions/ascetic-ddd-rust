//! The map's memory: a slot per key, and a window over the anchored ones.
//!
//! One invariant: a key is in the window exactly when its slot is anchored.
//! The window says which keys it lets go of; `release` turns their slots into
//! released ones, or forgets them.

use std::collections::HashMap;

use super::Lookup;
use super::key::{DynKey, KeyBox};
use super::slot::{AnyLookup, Fact, Slot};
use super::window::Window;

pub(super) struct State {
    slots: HashMap<KeyBox, Slot>,
    window: Window,
}

impl State {
    pub(super) fn new(window: Window) -> Self {
        State {
            slots: HashMap::new(),
            window,
        }
    }

    /// Remembers a fact as just used, replacing what the key had.
    pub(super) fn remember(&mut self, key: KeyBox, fact: Fact) {
        self.slots.insert(key.clone(), Slot::Anchored(fact));
        let gone = self.window.admit(key);
        self.release(gone);
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
        self.slots.retain(|_, slot| slot.is_alive());
        self.slots.len()
    }

    pub(super) fn resize(&mut self, size: usize) {
        let gone = self.window.resize(size);
        self.release(gone);
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
