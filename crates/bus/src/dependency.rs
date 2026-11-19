//! What a stage depends on, as the inbox's breaker asks it (seedwork ADR-0042
//! decision 1): the one witness of its own outage.
//!
//! A stage that answers from a memory — the envelope stage's opening end keeps
//! the keys it unwrapped — makes a message processed no evidence of what it
//! depends on: a row opened from the memory with the KMS down proves nothing
//! of the KMS. Such a stage declares its dependency by destination
//! ([`Dependent`]); the inbox then weighs the stage's own refusals against
//! the keys the dependency served by a call, and asks it directly, beside its
//! dispatch, while a channel is suspect or paused.
//!
//! The declaration travels with the subscription (seedwork ADR-0044): a
//! transactional consumer goes in only through an
//! [`InboundStage`](crate::InboundStage), which declares, and hands the
//! inbox its stages' [`Dependencies`] with the handler. No composition root
//! pairs a stage with its dependencies by hand.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;

use crate::error::{Cause, Composition};
use crate::message::Message;
use crate::uri;

/// What a stage depends on: counted when it serves by a call, and asked by
/// the inbox past the stage's memory and pacing.
pub trait Dependency: Send + Sync {
    /// The rows it served by a call so far — never from the stage's memory,
    /// never a [`Dependency::check`]'s. Monotone.
    fn served(&self) -> u64;

    /// The longest a check may take: the dependency cuts its own check there.
    fn bound(&self) -> Duration;

    /// Whether a refusal at the stage's scope under `cause` is this
    /// dependency's.
    fn answers_for(&self, cause: Cause) -> bool;

    /// Asks the dependency now, past the stage's memory and pacing: about
    /// `head`'s key, given the head's headers only, and about what the stage
    /// last served by a call. [`Check::Served`] as soon as one is served;
    /// [`Check::Unserved`] when none is and one was refused at the stage's
    /// scope or ran past the bound; [`Check::Unasked`] when nothing was asked,
    /// or every answer was about the thing asked for itself.
    fn check<'a>(&'a self, head: Option<&'a Message>) -> BoxFuture<'a, Check>;
}

/// What a [`Dependency::check`] came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Check {
    /// It served by a call: evidence that it serves.
    Served,
    /// It did not, under this cause: evidence that it does not.
    Unserved(Cause),
    /// Nothing was asked, or nothing learnt of it: no evidence either way.
    Unasked,
}

/// A stage that states what it depends on, by the destination whose rows it
/// serves: [`Dependencies::none`] for a stage that answers from no memory.
///
/// There is no default: a stage that wraps another forwards the inner
/// stage's dependencies or says it has none, and either is written down
/// (seedwork ADR-0044). A declaration left out does not compile:
///
/// ```compile_fail
/// use ascetic_ddd_bus::{Dependencies, Dependent};
/// struct Plain;
/// impl Dependent for Plain {}
/// ```
///
/// while the same stage saying it has none does:
///
/// ```
/// use ascetic_ddd_bus::{Dependencies, Dependent};
/// struct Plain;
/// impl Dependent for Plain { fn dependencies(&self) -> Dependencies { Dependencies::none() } }
/// ```
pub trait Dependent {
    /// Its dependencies by destination. The same objects on every call: an
    /// inbox tells a dependency it weighs already from a different one by
    /// identity, so a stage that made a new one per call could not be
    /// subscribed again.
    fn dependencies(&self) -> Dependencies;
}

/// The dependencies a stage declares, by destination, each destination
/// without its key: one dependency a destination (seedwork ADR-0044).
///
/// Two different dependencies for one destination are a [`Composition`]
/// conflict, wherever they meet — in one declaration, in a merge of a
/// consumer's stages, or against what an inbox already weighs; the same
/// dependency twice is one. Different means another object: a dependency is
/// told by identity, not by what it says.
#[derive(Clone, Default)]
pub struct Dependencies(BTreeMap<String, Arc<dyn Dependency>>);

impl Dependencies {
    /// No dependency: a stage that answers from no memory.
    #[must_use]
    pub fn none() -> Self {
        Dependencies::default()
    }

    /// The dependencies of `entries`, each destination's key left aside.
    ///
    /// # Errors
    /// [`Composition::Conflict`] when two entries name one destination, key
    /// or no key, with different dependencies.
    pub fn by_destination<D: AsRef<str>>(
        entries: impl IntoIterator<Item = (D, Arc<dyn Dependency>)>,
    ) -> Result<Self, Composition> {
        entries
            .into_iter()
            .try_fold(Dependencies::none(), |all, (destination, dependency)| {
                all.with(destination.as_ref(), dependency)
            })
    }

    /// These and `other`'s.
    ///
    /// # Errors
    /// [`Composition::Conflict`] when both name one destination with
    /// different dependencies.
    pub fn merge(self, other: Dependencies) -> Result<Self, Composition> {
        other
            .0
            .into_iter()
            .try_fold(self, |all, (destination, dependency)| {
                all.with(&destination, dependency)
            })
    }

    fn with(
        mut self,
        destination: &str,
        dependency: Arc<dyn Dependency>,
    ) -> Result<Self, Composition> {
        let destination = uri::without_key(destination);
        match self.0.get(destination) {
            Some(held) if Arc::ptr_eq(held, &dependency) => Ok(self),
            Some(_) => Err(Composition::Conflict {
                destination: destination.to_owned(),
            }),
            None => {
                self.0.insert(destination.to_owned(), dependency);
                Ok(self)
            }
        }
    }

    /// Each destination, without its key, with its dependency, in the
    /// destinations' order. A destination comes once.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Arc<dyn Dependency>)> {
        self.0
            .iter()
            .map(|(destination, dependency)| (destination.as_str(), dependency))
    }

    /// The destinations, without their keys, in order.
    pub fn destinations(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }

    /// Whether there is none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for Dependencies {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Dependencies")
            .field(&self.0.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Kms;

    impl Dependency for Kms {
        fn served(&self) -> u64 {
            0
        }
        fn bound(&self) -> Duration {
            Duration::from_secs(1)
        }
        fn answers_for(&self, _: Cause) -> bool {
            true
        }
        fn check<'a>(&'a self, _: Option<&'a Message>) -> BoxFuture<'a, Check> {
            Box::pin(async { Check::Unasked })
        }
    }

    fn kms() -> Arc<dyn Dependency> {
        Arc::new(Kms)
    }

    fn conflict(destination: &str) -> Composition {
        Composition::Conflict {
            destination: destination.to_owned(),
        }
    }

    #[test]
    fn a_destination_is_declared_without_its_key() {
        let a = kms();
        let declared =
            Dependencies::by_destination([("inbox://orders/k1", Arc::clone(&a))]).unwrap();
        assert_eq!(
            declared.destinations().collect::<Vec<_>>(),
            ["inbox://orders"]
        );
        assert!(Arc::ptr_eq(declared.iter().next().unwrap().1, &a));
    }

    #[test]
    fn one_dependency_twice_for_a_destination_is_one() {
        let a = kms();
        let declared = Dependencies::by_destination([
            ("inbox://orders/k1", Arc::clone(&a)),
            ("inbox://orders", Arc::clone(&a)),
        ])
        .unwrap()
        .merge(Dependencies::by_destination([("inbox://orders/k2", a)]).unwrap())
        .unwrap();
        assert_eq!(declared.iter().count(), 1);
    }

    #[test]
    fn two_dependencies_for_a_destination_are_a_conflict_key_or_no_key() {
        let (a, b) = (kms(), kms());
        assert_eq!(
            Dependencies::by_destination([
                ("inbox://orders/k1", Arc::clone(&a)),
                ("inbox://orders/k2", Arc::clone(&b)),
            ])
            .unwrap_err(),
            conflict("inbox://orders")
        );
        let first = Dependencies::by_destination([("inbox://orders/k1", a)]).unwrap();
        let second = Dependencies::by_destination([("inbox://orders", b)]).unwrap();
        assert_eq!(first.merge(second).unwrap_err(), conflict("inbox://orders"));
    }

    #[test]
    fn a_merge_keeps_both_sides_destinations() {
        let (a, b) = (kms(), kms());
        let merged = Dependencies::by_destination([("inbox://b", Arc::clone(&b))])
            .unwrap()
            .merge(Dependencies::by_destination([("inbox://a", Arc::clone(&a))]).unwrap())
            .unwrap();
        let mut each = merged.iter();
        let (first, second) = (each.next().unwrap(), each.next().unwrap());
        assert_eq!((first.0, second.0), ("inbox://a", "inbox://b"));
        assert!(Arc::ptr_eq(first.1, &a) && Arc::ptr_eq(second.1, &b));
        assert!(each.next().is_none());
        assert!(Dependencies::none().is_empty());
    }
}
