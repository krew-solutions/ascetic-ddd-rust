//! The window as an exact order of last use.
//!
//! Every key in the window carries the tick of its last use; the oldest tick
//! goes first. A `BTreeMap` keeps the order, so every operation is O(log n)
//! with n bounded by the window, and there is nothing to compact.

use std::collections::{BTreeMap, HashMap};

use super::key::{DynKey, KeyBox};

pub(super) struct Recency {
    size: usize,
    clock: u64,
    /// The tick of each key's last use.
    ticks: HashMap<KeyBox, u64>,
    /// Keys by tick, oldest first.
    order: BTreeMap<u64, KeyBox>,
}

impl Recency {
    pub(super) fn new(size: usize) -> Self {
        Recency {
            size,
            clock: 0,
            ticks: HashMap::new(),
            order: BTreeMap::new(),
        }
    }

    pub(super) fn admit(&mut self, key: KeyBox) -> Vec<KeyBox> {
        self.clock += 1;
        let now = self.clock;
        if let Some(previous) = self.ticks.insert(key.clone(), now) {
            self.order.remove(&previous);
        }
        self.order.insert(now, key);
        self.evict()
    }

    pub(super) fn remove(&mut self, key: &(dyn DynKey + 'static)) {
        if let Some(tick) = self.ticks.remove(key) {
            self.order.remove(&tick);
        }
    }

    pub(super) fn clear(&mut self) {
        self.ticks.clear();
        self.order.clear();
    }

    pub(super) fn resize(&mut self, size: usize) -> Vec<KeyBox> {
        self.size = size;
        self.evict()
    }

    /// Lets the oldest keys go until the window fits.
    fn evict(&mut self) -> Vec<KeyBox> {
        let mut gone = Vec::new();
        while self.order.len() > self.size {
            let Some((_, key)) = self.order.pop_first() else {
                break;
            };
            self.ticks.remove(&key);
            gone.push(key);
        }
        gone
    }
}
