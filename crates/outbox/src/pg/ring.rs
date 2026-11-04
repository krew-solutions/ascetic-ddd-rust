//! The ring the outbox's table is cut into (ADR-0033): which generation a
//! transaction's rows land in, and which generations a statement must name
//! so that the planner reads, and locks, those alone.

use std::collections::BTreeSet;

use crate::message::Snapshot;

/// The outbox's table as a ring of `generations` partitions, each taking in
/// turn the rows of `window` consecutive transaction ids, emptied whole once
/// every group has passed it (ADR-0033 decision 3). Fixed for the life of
/// the table, as the slots are: a process configured otherwise is refused
/// at start.
///
/// `generations · window`, with the margin, stays below the server's
/// `autovacuum_freeze_max_age`, so that no partition needs a freezing vacuum
/// before it is emptied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ring {
    /// How many partitions: `K`.
    pub generations: u32,
    /// How many transaction ids one window spans: `R`.
    pub window: u64,
}

impl Default for Ring {
    /// Eight generations of 2²³ ids: about 84 minutes a window at 1 654 ids a
    /// second, about eleven hours a lap.
    fn default() -> Self {
        Ring {
            generations: 8,
            window: 1 << 23,
        }
    }
}

impl Ring {
    /// The ring, its numbers brought into range: two generations at least —
    /// one would hold every lap and never be emptied — and a window of one
    /// id at least.
    pub fn new(generations: u32, window: u64) -> Self {
        Ring {
            generations: generations.clamp(2, i16::MAX as u32),
            window: window.max(1),
        }
    }

    /// The generation the rows of transaction `xid` land in.
    pub fn generation_of(&self, xid: u64) -> u32 {
        ((xid / self.window) % u64::from(self.generations)) as u32
    }

    /// The generations the rows new in `epoch` since `delivered` can lie in:
    /// those of the ids from `delivered`'s `xmax` up to `epoch`'s, and those
    /// of `delivered`'s in-progress ids that `epoch` sees. Neither `xmin`
    /// enters: it is the server's oldest transaction, of any database, and
    /// would name the generation an emptying waits for (ADR-0033 decision 3).
    pub fn generations_new(&self, delivered: &Snapshot, epoch: &Snapshot) -> BTreeSet<u32> {
        let mut names = self.generations_between(delivered.xmax, epoch.xmax);
        names.extend(
            delivered
                .xip
                .iter()
                .filter(|xid| epoch.sees(**xid))
                .map(|xid| self.generation_of(*xid)),
        );
        names
    }

    /// The generations of the ids in `[from, to)`: every one, once they span
    /// as many windows.
    fn generations_between(&self, from: u64, to: u64) -> BTreeSet<u32> {
        if from >= to {
            return BTreeSet::new();
        }
        let (first, last) = (from / self.window, (to - 1) / self.window);
        if last - first + 1 >= u64::from(self.generations) {
            return (0..self.generations).collect();
        }
        (first..=last)
            .map(|window| (window % u64::from(self.generations)) as u32)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(xmin: u64, xmax: u64, xip: &[u64]) -> Snapshot {
        Snapshot {
            xmin,
            xmax,
            xip: xip.to_vec(),
        }
    }

    #[test]
    fn a_transaction_s_rows_land_in_its_window_s_generation() {
        let ring = Ring::new(3, 4);
        assert_eq!(
            (0..14)
                .map(|xid| ring.generation_of(xid))
                .collect::<Vec<_>>(),
            [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 0, 0]
        );
    }

    /// The names are exactly the windows the new ids lie in: a generation
    /// that holds none of them is not named, whatever lies below `xmin`.
    #[test]
    fn an_epoch_names_the_windows_of_its_new_ids_alone() {
        let ring = Ring::new(4, 10);
        // ids 25..=41: windows 2, 3 and 4, generations 2, 3 and 0
        let names = ring.generations_new(&snapshot(3, 25, &[]), &snapshot(30, 42, &[]));
        assert_eq!(names.into_iter().collect::<Vec<_>>(), [0, 2, 3]);
        // nothing new: nothing named
        assert!(
            ring.generations_new(&snapshot(25, 25, &[]), &snapshot(25, 25, &[]))
                .is_empty()
        );
    }

    /// An in-progress id of `delivered` is named once the epoch sees it, and
    /// not while it still runs: a transaction left open, in this database or
    /// another, adds no generation to a fetch.
    #[test]
    fn an_in_progress_id_is_named_once_it_ended() {
        let ring = Ring::new(4, 10);
        let delivered = snapshot(5, 25, &[5, 12]);
        let names = ring.generations_new(&delivered, &snapshot(5, 25, &[5]));
        assert_eq!(names.into_iter().collect::<Vec<_>>(), [1]);
        let names = ring.generations_new(&delivered, &snapshot(5, 25, &[5, 12]));
        assert!(names.is_empty());
    }

    /// A range spanning as many windows as there are generations names them
    /// all; one window fewer leaves the one generation it misses out.
    #[test]
    fn a_range_of_a_lap_names_every_generation() {
        let ring = Ring::new(4, 10);
        let all = ring.generations_new(&snapshot(0, 10, &[]), &snapshot(0, 50, &[]));
        assert_eq!(all.len(), 4);
        let three = ring.generations_new(&snapshot(0, 10, &[]), &snapshot(0, 40, &[]));
        assert_eq!(three.into_iter().collect::<Vec<_>>(), [1, 2, 3]);
    }

    #[test]
    fn a_ring_has_two_generations_and_one_id_a_window_at_least() {
        assert_eq!(Ring::new(1, 0), Ring::new(2, 1));
    }
}
