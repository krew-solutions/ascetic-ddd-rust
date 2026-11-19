//! Populations: identities, not values (ADR-0045 decision 6).
//!
//! A population owns its kind, its scope encoding and the generator that
//! creates its members. A member is named by a [`Ref`] — kind, scope, index —
//! and its value is a pure function of the key `root.child(kind).part(scope).at(index)`:
//! regenerated wherever it is needed, never stored. So the parent a reference
//! names is the same whoever reaches it, and a population of 10⁹ holds nothing.
//!
//! Everything here is a pure function with random access: any thread computes
//! any member, any pick, any range, in any order.

use std::collections::BTreeSet;
use std::fmt;

use crate::apportion::Apportionment;
use crate::generator::{Gen, Refusal};
use crate::key::{Key, KeyPart, Seed};
use crate::law::IndexLaw;
use crate::permutation::Permutation;

/// A member of a population: its kind, its scope, its index in the scope.
///
/// The scope is the member's owner — a Tenant — and never changes: a member's
/// state, a selection by state, a move between groups never change its `Ref`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ref<K> {
    /// The population's kind.
    pub kind: &'static str,
    /// The owner.
    pub scope: K,
    /// The index in the scope: the member's sequence number too.
    pub index: u64,
}

/// The kinds of one Plan, each once: two populations of one kind would draw
/// the same keys and correlate.
#[derive(Debug)]
pub struct Plan {
    seed: Seed,
    root: Key,
    kinds: BTreeSet<&'static str>,
}

/// A second population of a kind the Plan already has.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DuplicateKind(pub &'static str);

impl fmt::Display for DuplicateKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the plan already has a population of kind {:?}", self.0)
    }
}

impl std::error::Error for DuplicateKind {}

impl From<DuplicateKind> for Refusal {
    fn from(e: DuplicateKind) -> Refusal {
        Refusal::new(e)
    }
}

impl Plan {
    /// A Plan of `seed`.
    pub fn new(seed: Seed) -> Plan {
        Plan {
            seed,
            root: Key::root(seed),
            kinds: BTreeSet::new(),
        }
    }

    /// Its seed.
    pub fn seed(&self) -> Seed {
        self.seed
    }

    /// The population of `kind`: `size` gives each scope's member count from
    /// the scope's key, `members` each member from its own.
    pub fn population<K, S, G>(
        &mut self,
        kind: &'static str,
        size: S,
        members: G,
    ) -> Result<Population<K, S, G>, DuplicateKind>
    where
        K: KeyPart,
        S: Gen<Output = u64>,
        G: Gen,
    {
        if !self.kinds.insert(kind) {
            return Err(DuplicateKind(kind));
        }
        Ok(Population {
            kind,
            key: self.root.child(kind),
            size,
            members,
            scope: std::marker::PhantomData,
        })
    }
}

/// A population: its members' count per scope and their generator.
pub struct Population<K, S, G> {
    kind: &'static str,
    key: Key,
    size: S,
    members: G,
    scope: std::marker::PhantomData<fn(&K)>,
}

impl<K, S, G> Population<K, S, G>
where
    K: KeyPart + Clone,
    S: Gen<Output = u64>,
    G: Gen,
{
    /// Its kind.
    pub fn kind(&self) -> &'static str {
        self.kind
    }

    /// One scope of it, evaluated once: its size drawn, its key derived. A unit
    /// of a million picks takes one view.
    pub fn scope(&self, scope: K) -> Result<ScopeView<'_, K, S, G>, Refusal> {
        let key = self.key.part(&scope);
        let size = self.size.generate(key.internal("size"))?;
        Ok(ScopeView {
            population: self,
            scope,
            key,
            size,
        })
    }

    /// The member `r` names, regenerated from its key; refused if `r` is of
    /// another kind or past its scope's size.
    pub fn value(&self, r: &Ref<K>) -> Result<G::Output, Refusal> {
        if r.kind != self.kind {
            return Err(Refusal::new(format!(
                "a {} is not a member of the {} population",
                r.kind, self.kind
            )));
        }
        self.scope(r.scope.clone())?.value(r.index)
    }
}

/// One scope of a population: its size and key, evaluated once.
pub struct ScopeView<'p, K, S, G> {
    population: &'p Population<K, S, G>,
    scope: K,
    key: Key,
    size: u64,
}

impl<K, S, G> ScopeView<'_, K, S, G>
where
    K: KeyPart + Clone,
    S: Gen<Output = u64>,
    G: Gen,
{
    /// How many members the scope has.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// The member at `index`, named.
    pub fn reference(&self, index: u64) -> Ref<K> {
        Ref {
            kind: self.population.kind,
            scope: self.scope.clone(),
            index,
        }
    }

    /// The key of the member at `index`.
    pub fn key_of(&self, index: u64) -> Key {
        self.key.at(index)
    }

    /// The member at `index`, regenerated from its key; refused past the size.
    pub fn value(&self, index: u64) -> Result<G::Output, Refusal> {
        if index >= self.size {
            return Err(Refusal::new(format!(
                "{} {} is past its scope's {} members",
                self.population.kind, index, self.size
            )));
        }
        self.population.members.generate(self.key_of(index))
    }

    /// A member picked by `law` from `key` (many-to-one); `None` in an empty
    /// scope.
    pub fn pick(&self, key: Key, law: &impl IndexLaw) -> Option<Ref<K>> {
        (self.size > 0).then(|| self.reference(law.index(key.unit(), self.size)))
    }

    /// A member picked by `law` among the first `upto` of `order` — the
    /// identity, or a permutation of the scope — and `None` while `upto` is 0.
    /// "A parent that exists at `t`" is a pick below the parents arrived by
    /// `t`; Python's growing pool is a pick below [`Pool::parents_before`].
    pub fn pick_below(
        &self,
        key: Key,
        law: &impl IndexLaw,
        upto: u64,
        order: Option<&Permutation>,
    ) -> Option<Ref<K>> {
        let upto = upto.min(self.size);
        if upto == 0 {
            return None;
        }
        let position = law.index(key.unit(), upto);
        let index = match order {
            Some(permutation) => permutation.at(position)?,
            None => position,
        };
        Some(self.reference(index))
    }

    /// The scope's own order of its members — an arrival order independent of
    /// identity — drawn from its key.
    pub fn permutation(&self) -> Permutation {
        Permutation::new(self.size, self.key.internal("order"))
    }
}

/// Python's growing pool as a pure function: parents that grow with their
/// children, `mean` children a parent on average, with no record of who was
/// reached and no total known in advance.
///
/// Before child `n`, `A(n) = 1 + ⌊n / mean + u⌋` parents exist; child `n` picks
/// below `A(n)`; parent `p ≥ 1` arrives just before the first child that can
/// pick it, child `⌈(p − u) · mean⌉`. Parent 0 arrives before child 0.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Pool {
    mean: f64,
    u: f64,
}

impl Pool {
    /// A pool of `mean` children a parent, its phase drawn from `key`.
    pub fn new(mean: f64, key: Key) -> Result<Pool, Refusal> {
        if mean.is_finite() && mean >= 1.0 {
            Ok(Pool {
                mean,
                u: key.unit().value(),
            })
        } else {
            Err(Refusal::new("a pool's mean must be finite and at least 1"))
        }
    }

    /// `A(n)`: the parents that exist before child `n`.
    pub fn parents_before(&self, child: u64) -> u64 {
        ((child as f64 / self.mean + self.u).floor() as u64).saturating_add(1)
    }

    /// The child before which parent `p` arrives.
    pub fn arrives_before(&self, parent: u64) -> u64 {
        if parent == 0 {
            0
        } else {
            ((parent as f64 - self.u) * self.mean).ceil().max(0.0) as u64
        }
    }
}

/// One-to-many: the children of each parent of a scope, as ranges of the child
/// population's indices.
pub trait Ranges {
    /// The number of parents.
    fn parents(&self) -> u64;

    /// The children before parent `p`'s, for `p` in `0..=parents()`.
    fn start(&self, parent: u64) -> u64;

    /// The children of all parents.
    fn total(&self) -> u64 {
        self.start(self.parents())
    }

    /// Parent `p`'s children.
    fn children(&self, parent: u64) -> std::ops::Range<u64> {
        self.start(parent)..self.start(parent.saturating_add(1))
    }

    /// The parent of `child`, by binary search: O(log parents) starts.
    fn parent_of(&self, child: u64) -> Option<u64> {
        if child >= self.total() {
            return None;
        }
        // Invariant: start(low) ≤ child < start(high). It ends at high = low + 1,
        // so `low`'s range holds `child`: an empty parent's never can.
        let (mut low, mut high) = (0u64, self.parents());
        while high.saturating_sub(low) > 1 {
            let mid = low.saturating_add(high.saturating_sub(low) / 2);
            if self.start(mid) <= child {
                low = mid;
            } else {
                high = mid;
            }
        }
        Some(low)
    }
}

/// Ranges from each parent's count, as a prefix-sum table: 8 bytes a parent.
/// For few parents — groups, waves, time chunks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Table {
    starts: Vec<u64>,
}

impl Table {
    /// From the counts in parent order; refused if they overflow `u64`.
    pub fn from_counts(counts: impl IntoIterator<Item = u64>) -> Result<Table, Refusal> {
        let mut starts = vec![0u64];
        let mut total = 0u64;
        for c in counts {
            total = total
                .checked_add(c)
                .ok_or_else(|| Refusal::new("the counts overflow u64"))?;
            starts.push(total);
        }
        Ok(Table { starts })
    }
}

impl Ranges for Table {
    fn parents(&self) -> u64 {
        self.starts.len().saturating_sub(1) as u64
    }

    fn start(&self, parent: u64) -> u64 {
        usize::try_from(parent)
            .ok()
            .and_then(|p| self.starts.get(p))
            .or(self.starts.last())
            .copied()
            .unwrap_or(0)
    }
}

/// An apportionment's ranges need no table: each start is O(1).
impl Ranges for Apportionment {
    fn parents(&self) -> u64 {
        self.len() as u64
    }

    fn start(&self, parent: u64) -> u64 {
        Apportionment::start(
            self,
            usize::try_from(parent)
                .unwrap_or(usize::MAX)
                .min(self.len()),
        )
    }
}
