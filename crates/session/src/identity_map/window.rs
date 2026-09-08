//! The window: which keys the map keeps alive on its own, and which it lets go.
//!
//! Two kinds, as in Storm's `cache.py`:
//!
//! * [`Recency`] keeps an exact order of last use and lets the oldest key go
//!   first — Storm's `Cache`, Python's `CacheLru`.
//! * [`Generations`] keeps no order at all: keys enter a young generation, and
//!   when it is full the old generation is let go of as a whole — Storm's
//!   `GenerationalCache`. Cheaper, and imprecise by design: a key survives
//!   between `size` and `2 * size` admissions.
//!
//! The window knows keys only. What letting a key go means for its entity is
//! decided by the slot, in `state`.

use super::generations::Generations;
use super::key::{DynKey, KeyBox};
use super::recency::Recency;

pub(super) enum Window {
    Recency(Recency),
    Generations(Generations),
}

impl Window {
    /// Marks the key as just used, admitting it if it was not in the window.
    /// Returns the keys that lost their place as a result.
    pub(super) fn admit(&mut self, key: KeyBox) -> Vec<KeyBox> {
        match self {
            Window::Recency(window) => window.admit(key),
            Window::Generations(window) => window.admit(key),
        }
    }

    pub(super) fn remove(&mut self, key: &(dyn DynKey + 'static)) {
        match self {
            Window::Recency(window) => window.remove(key),
            Window::Generations(window) => window.remove(key),
        }
    }

    pub(super) fn clear(&mut self) {
        match self {
            Window::Recency(window) => window.clear(),
            Window::Generations(window) => window.clear(),
        }
    }

    /// Changes the size; returns the keys that no longer fit.
    pub(super) fn resize(&mut self, size: usize) -> Vec<KeyBox> {
        match self {
            Window::Recency(window) => window.resize(size),
            Window::Generations(window) => window.resize(size),
        }
    }
}
