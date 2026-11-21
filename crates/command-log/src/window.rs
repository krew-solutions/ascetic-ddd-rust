//! The binding rule (ADR-0046 decision 16, ADR-0047 decision 2): which earlier
//! step a dependency names, decided when its line is read. The validator checks
//! a log by it and the scheduler binds by it, so that the scheduler binds exactly
//! as the validator checked.
//!
//! A step is kept by its id's digest while it is planned within `W` behind the
//! read cursor — the running maximum of `at` over the lines read — or for the
//! whole run when its label is retained. Eviction is by planned time, exact: a
//! step evicted can never be reached again, the cursor never going back. A step
//! more than `W` behind the cursor on arrival is not kept, and takes an older
//! occurrence of its id with it: no later edge may accept it, and the older one
//! must not answer for it.

use std::collections::{BTreeMap, BTreeSet};

use crate::log::LogError;
use crate::record::StepId;

/// A step id's identity in the binding rule and the validator's runs: a 128-bit
/// SipHash-1-3 of its canonical JSON — 2⁻⁶⁹ for a collision at 10⁹, for logs
/// not crafted to collide.
pub fn digest(step: &StepId) -> Result<[u8; 16], LogError> {
    let bytes = serde_json::to_vec(step).map_err(|e| LogError::Encode(e.to_string()))?;
    Ok(crate::runs::digest(&bytes))
}

/// The steps a later line may bind: each with its planned time and a value of
/// the caller's (the scheduler's cell; nothing for the validator).
#[derive(Clone, Debug)]
pub struct Window<V> {
    w: u64,
    cursor: u64,
    kept: BTreeMap<[u8; 16], (u64, V)>,
    order: BTreeSet<(u64, [u8; 16])>,
    retained: BTreeMap<[u8; 16], (u64, V)>,
}

impl<V> Window<V> {
    /// An empty window `w` microseconds wide.
    pub fn new(w: u64) -> Window<V> {
        Window {
            w,
            cursor: 0,
            kept: BTreeMap::new(),
            order: BTreeSet::new(),
            retained: BTreeMap::new(),
        }
    }

    /// The read cursor.
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// A line read at `at`: the cursor moves, and what it leaves more than `W`
    /// behind goes, each value it held handed to `gone`.
    pub fn read(&mut self, at: u64, mut gone: impl FnMut(V)) {
        self.cursor = self.cursor.max(at);
        let (w, c) = (self.w, self.cursor);
        while let Some((planned, _)) = self.order.first() {
            if c.saturating_sub(*planned) <= w {
                break;
            }
            if let Some((planned, id)) = self.order.pop_first()
                && self.kept.get(&id).map(|e| e.0) == Some(planned)
                && let Some((_, value)) = self.kept.remove(&id)
            {
                gone(value);
            }
        }
    }

    /// The step a dependency on `id` binds: a retained one, or one within `W`
    /// behind the cursor; its planned time and value.
    pub fn get(&self, id: &[u8; 16]) -> Option<&(u64, V)> {
        self.retained.get(id).or_else(|| self.kept.get(id))
    }

    /// The step just read, planned at `at`, kept for later dependencies: for
    /// the whole run when `retained`, else while within `W` behind the cursor —
    /// and not at all when already past it. What it is no longer kept — an
    /// earlier step of its id it displaces, or itself, dead on arrival — goes
    /// to `gone`.
    pub fn keep(
        &mut self,
        id: [u8; 16],
        at: u64,
        retained: bool,
        value: V,
        mut gone: impl FnMut(V),
    ) {
        let displaced = if retained {
            self.retained.insert(id, (at, value))
        } else if self.cursor.saturating_sub(at) > self.w {
            gone(value);
            self.kept.remove(&id)
        } else {
            self.order.insert((at, id));
            self.kept.insert(id, (at, value))
        };
        if let Some((_, old)) = displaced {
            gone(old);
        }
    }

    /// The value kept for `id`, to change: a retained one, or one within `W`.
    pub(crate) fn get_mut(&mut self, id: &[u8; 16]) -> Option<&mut V> {
        match self.retained.get_mut(id) {
            Some(e) => Some(&mut e.1),
            None => self.kept.get_mut(id).map(|e| &mut e.1),
        }
    }

    /// The steps kept: within `W` and retained.
    pub fn len(&self) -> usize {
        self.kept.len().saturating_add(self.retained.len())
    }

    /// Whether nothing is kept.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
