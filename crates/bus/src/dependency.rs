//! What a stage depends on, as the inbox's breaker asks it (seedwork ADR-0042
//! decision 1): the one witness of its own outage.
//!
//! A stage that answers from a memory — the envelope stage's opening end keeps
//! the keys it unwrapped — makes a message processed no evidence of what it
//! depends on: a row opened from the memory with the KMS down proves nothing
//! of the KMS. Such a stage declares its dependency, and a composition root
//! hands it to the inbox by channel ([`Dependent`]); the inbox then weighs the
//! stage's own refusals against the keys the dependency served by a call, and
//! asks it directly, beside its dispatch, while a channel is suspect or paused.

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;

use crate::error::Cause;
use crate::message::Message;

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

/// A stage whose dependencies a composition root hands to an inbox: each
/// with the destination whose rows it serves, its key left aside.
pub trait Dependent {
    /// Its dependencies by destination.
    fn dependencies(&self) -> Vec<(String, Arc<dyn Dependency>)>;
}
