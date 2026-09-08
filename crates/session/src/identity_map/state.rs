//! The map's memory: a slot per key, and the recency window over the anchored
//! ones.
//!
//! One invariant, kept in one place (`place`): a slot is anchored exactly when
//! `recency` holds its tick, so the window is `recency` itself and its size is
//! `recency.len()`.

use std::collections::{BTreeMap, HashMap};

use super::Lookup;
use super::key::{DynKey, KeyBox};
use super::slot::{AnyLookup, Fact, Slot};

pub(super) struct State {
    size: usize,
    clock: u64,
    slots: HashMap<KeyBox, Slot>,
    /// Anchored keys by the tick of their last use, oldest first.
    recency: BTreeMap<u64, KeyBox>,
}

impl State {
    pub(super) fn new(size: usize) -> Self {
        State {
            size,
            clock: 0,
            slots: HashMap::new(),
            recency: BTreeMap::new(),
        }
    }

    /// Remembers a fact as the most recently used, replacing what the key had.
    pub(super) fn remember(&mut self, key: KeyBox, fact: Fact) {
        let previous = self.slots.remove(&key).and_then(|slot| slot.since());
        let since = self.tick();
        self.place(key, previous, Some(Slot::Anchored { since, fact }));
        self.evict();
    }

    /// Answers for the key, and records the use.
    pub(super) fn recall(&mut self, key: &(dyn DynKey + 'static)) -> AnyLookup {
        let Some((key, slot)) = self.slots.remove_entry(key) else {
            return Lookup::Unknown;
        };
        let previous = slot.since();
        let now = self.tick();
        let (next, answer) = slot.recall(now);
        self.place(key, previous, next);
        self.evict();
        answer
    }

    pub(super) fn forget(&mut self, key: &(dyn DynKey + 'static)) {
        if let Some(tick) = self.slots.remove(key).and_then(|slot| slot.since()) {
            self.recency.remove(&tick);
        }
    }

    pub(super) fn forget_all(&mut self) {
        self.slots.clear();
        self.recency.clear();
    }

    /// Number of keys that can still be answered for. Entities the domain has
    /// let go of since their release are forgotten on the way.
    pub(super) fn len(&mut self) -> usize {
        self.slots.retain(|_, slot| slot.is_alive());
        self.slots.len()
    }

    pub(super) fn resize(&mut self, size: usize) {
        self.size = size;
        self.evict();
    }

    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    /// Installs the next state of a key, keeping `recency` in step with it.
    fn place(&mut self, key: KeyBox, previous: Option<u64>, next: Option<Slot>) {
        if let Some(tick) = previous {
            self.recency.remove(&tick);
        }
        if let Some(slot) = next {
            if let Some(since) = slot.since() {
                self.recency.insert(since, key.clone());
            }
            self.slots.insert(key, slot);
        }
    }

    /// Releases the least recently used keys until the window fits.
    fn evict(&mut self) {
        while self.recency.len() > self.size {
            let Some((_, key)) = self.recency.pop_first() else {
                break;
            };
            if let Some(next) = self.slots.remove(&key).and_then(Slot::release) {
                self.slots.insert(key, next);
            }
        }
    }
}
