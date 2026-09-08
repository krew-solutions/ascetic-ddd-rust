//! The window as two generations, after Storm's `GenerationalCache`.
//!
//! No order is kept. A key enters the young generation; when the young
//! generation is full, the old one is let go of as a whole, the young one
//! becomes old, and a fresh young one starts. A key therefore survives between
//! `size` and `2 * size` admissions, and a key that keeps being used survives
//! for as long as it is used. Every operation is constant time; letting a
//! generation go is one operation for all of it.

use std::collections::HashSet;

use super::key::{DynKey, KeyBox};

pub(super) struct Generations {
    size: usize,
    young: HashSet<KeyBox>,
    old: HashSet<KeyBox>,
}

impl Generations {
    pub(super) fn new(size: usize) -> Self {
        Generations {
            size,
            young: HashSet::new(),
            old: HashSet::new(),
        }
    }

    pub(super) fn admit(&mut self, key: KeyBox) -> Vec<KeyBox> {
        if self.size == 0 {
            return vec![key];
        }
        if self.young.contains(&key) {
            return Vec::new();
        }
        let mut gone = if self.young.len() >= self.size {
            self.bump()
        } else {
            Vec::new()
        };
        // The key may have been in the generation just let go of; it is being
        // admitted, so it did not go.
        gone.retain(|other| other != &key);
        self.young.insert(key);
        gone
    }

    /// The young generation grows old; the old one is let go of, except for
    /// what the young one also held.
    fn bump(&mut self) -> Vec<KeyBox> {
        let gone = std::mem::take(&mut self.old);
        self.old = std::mem::take(&mut self.young);
        gone.into_iter()
            .filter(|key| !self.old.contains(key))
            .collect()
    }

    pub(super) fn remove(&mut self, key: &(dyn DynKey + 'static)) {
        self.young.remove(key);
        self.old.remove(key);
    }

    pub(super) fn clear(&mut self) {
        self.young.clear();
        self.old.clear();
    }

    /// Keeps up to `size` keys, the young generation first, as the new young
    /// generation; the rest go.
    pub(super) fn resize(&mut self, size: usize) -> Vec<KeyBox> {
        self.size = size;
        let kept: HashSet<KeyBox> = self
            .young
            .iter()
            .chain(self.old.iter().filter(|key| !self.young.contains(*key)))
            .take(size)
            .cloned()
            .collect();
        let gone: HashSet<KeyBox> = self
            .young
            .drain()
            .chain(self.old.drain())
            .filter(|key| !kept.contains(key))
            .collect();
        self.young = kept;
        gone.into_iter().collect()
    }
}
