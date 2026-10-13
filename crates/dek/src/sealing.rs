//! The sealing of a bus's channels: one envelope stage per channel, each
//! over a KMS instance that serves the channel as its derivation context,
//! behind a cache of unwrapped keys of its own (ADR-0017 decision 2).
//!
//! A producer takes the stage of the channel it writes on its way out, a
//! consumer the stage of the channel it reads on its way in; a data key
//! drawn for a channel unwraps there and nowhere else, and a policy at the
//! KMS may pin the context a caller passes. The stages are built up front,
//! from the channels a service's contracts name — the same text at both
//! ends — so that a channel nobody declared is not sealed under a context
//! nobody admits: [`Sealing::of`] answers `None` for it, and a composition
//! root refuses to start rather than seal under a typo.
//!
//! The KMS behind a stage is the caller's to name: Vault Transit on the
//! bus's mount under the service's own identity as the service runs, the
//! PostgreSQL adapter in a component test — both serving the channel as the
//! context, both making no key, which is custody's. The stage's parameters
//! are the deployment's: how many messages and how long one data key serves
//! at the sealing end, how long an unwrapped key is kept at the opening end.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ascetic_ddd_bus::Stage;
use ascetic_ddd_kms::{Cached, KeyManagementService};
use ascetic_ddd_session::SessionPool;

use crate::stage::{EnvelopeStage, Reuse};

/// The stage's parameters: deployment configuration, bounded from the
/// security side — the window after an off-boarding in which a key already
/// drawn or unwrapped still serves is the reuse's lifetime plus the cache's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SealingParameters {
    /// How many messages and how long one data key serves at the sealing
    /// end before a fresh one is drawn.
    pub reuse: Reuse,
    /// How long an unwrapped data key is kept at the opening end.
    pub cache: Duration,
}

impl Default for SealingParameters {
    /// A thousand messages or a minute, five minutes: the initial norm.
    fn default() -> Self {
        SealingParameters {
            reuse: Reuse::default(),
            cache: Duration::from_secs(5 * 60),
        }
    }
}

/// The stages of a bus's channels, one per channel, built up front.
#[derive(Clone)]
pub struct Sealing {
    stages: Arc<HashMap<String, Arc<dyn Stage>>>,
}

impl std::fmt::Debug for Sealing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sealing")
            .field("channels", &self.stages.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl Sealing {
    /// The sealing of `channels` over sessions of `sessions`, with
    /// `parameters`, each channel's stage over the KMS `kms` makes for it —
    /// an instance serving the channel as its derivation context — behind a
    /// cache of its own. A channel named twice gets one stage.
    pub fn over<P, K>(
        sessions: P,
        channels: impl IntoIterator<Item = impl Into<String>>,
        parameters: SealingParameters,
        kms: impl Fn(&str) -> K,
    ) -> Self
    where
        P: SessionPool + Clone + Send + Sync + 'static,
        P::Session: Sync,
        K: KeyManagementService<P::Session> + 'static,
    {
        let mut stages: HashMap<String, Arc<dyn Stage>> = HashMap::new();
        for channel in channels {
            let channel = channel.into();
            if stages.contains_key(&channel) {
                continue;
            }
            let cached = Cached::new(kms(&channel)).with_ttl(parameters.cache);
            let stage = EnvelopeStage::new(sessions.clone(), cached).reusing(parameters.reuse);
            stages.insert(channel, Arc::new(stage));
        }
        Sealing {
            stages: Arc::new(stages),
        }
    }

    /// The stage of `channel`, or `None` for a channel the sealing was not
    /// built for.
    #[must_use]
    pub fn of(&self, channel: &str) -> Option<Arc<dyn Stage>> {
        self.stages.get(channel).cloned()
    }

    /// The channels the sealing was built for.
    pub fn channels(&self) -> impl Iterator<Item = &str> {
        self.stages.keys().map(String::as_str)
    }
}
