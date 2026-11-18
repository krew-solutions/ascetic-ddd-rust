//! The sealing of a bus's channels: one envelope stage per channel, each
//! over a KMS instance that serves the channel as its derivation context,
//! its opening end keeping the keys it unwraps (ADR-0017 decision 2,
//! ADR-0039 slice 4).
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
//!
//! An inbox that receives several channels has one subscriber for every
//! row, and `through` takes one stage: [`Sealing::by_destination`] gives
//! that stage — [`ByDestination`], which reads the row's `destination`,
//! the intake's name for the channel it received on, and delegates to the
//! stage of the channel the row was sealed under (ADR-0018).

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use ascetic_ddd_bus::{BoxError, Dependency, Dependent, Message, Permanent, Stage, uri};
use ascetic_ddd_kms::KeyManagementService;
use ascetic_ddd_session::SessionPool;
use futures::future::BoxFuture;

use crate::draws::{Held, SealingObserver, causes};
use crate::stage::{EnvelopeStage, Pacing, Reuse};
use crate::unwraps::Opening;

/// The entries an opening end holds by default.
const KEYS: usize = 16_384;
use crate::supply::InvalidParameters;

/// The header an inbox row goes out with: the channel it was received on.
const DESTINATION: &str = "destination";

/// The stage's parameters: deployment configuration, bounded from the
/// security side — the window after an off-boarding in which a key already
/// drawn or unwrapped still serves is the reuse's lifetime plus the
/// opening's keep.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SealingParameters {
    /// How many messages and how long one data key serves at the sealing
    /// end before a fresh one is drawn.
    pub reuse: Reuse,
    /// How long an unwrapped data key is kept at the opening end: the
    /// fitted opening's `keep` ([`Sealing::over`]).
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
    holding: Arc<HashMap<String, Arc<dyn Holding>>>,
    /// Each channel's opening end's dependency, the KMS (seedwork ADR-0042
    /// decision 1).
    dependencies: Arc<HashMap<String, Arc<dyn Dependency>>>,
}

/// A stage seen for what it holds.
trait Holding: Send + Sync {
    fn held(&self) -> Held;
}

impl<P, K> Holding for EnvelopeStage<P, K>
where
    P: SessionPool + Send + Sync + 'static,
    P::Session: Sync,
    K: KeyManagementService<P::Session> + 'static,
{
    fn held(&self) -> Held {
        EnvelopeStage::held(self)
    }
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
    /// an instance serving the channel as its derivation context — opening
    /// as [`Opening::fitted`] fits `parameters.cache`. A channel named twice
    /// gets one stage.
    ///
    /// Its data keys are drawn as [`Pacing::fitted`] fits the reuse
    /// (ADR-0039); [`Sealing::over_paced`] says otherwise.
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
        Self::over_paced(
            sessions,
            channels,
            (
                parameters,
                Pacing::fitted(parameters.reuse),
                Opening::fitted(parameters.cache, KEYS),
            ),
            None,
            None,
            |_| Arc::new(()),
            kms,
        )
        .expect("a fitted pacing fits its reuse")
    }

    /// As [`Sealing::over`], each stage's data keys drawn as `pacing` says,
    /// and opened as `opening` says (ADR-0039 slice 4),
    /// what becomes of them told to the observer `observer` makes for its
    /// channel. With a `ceiling`, the
    /// window after an off-boarding in which a key already drawn or
    /// unwrapped still serves — the reuse's lifetime, the opening's keep, and
    /// a draw's — is held under it (ADR-0039 decision 12; the consumer's
    /// ADR-0071 decision 11 names the ceiling). With a `sentinel`, each
    /// stage draws for the host's own account when it refuses on no
    /// evidence of whether its KMS serves ([`EnvelopeStage::with_sentinel`]).
    ///
    /// # Errors
    /// [`InvalidParameters`] naming the rule broken: a composition root
    /// refuses to start.
    pub fn over_paced<P, K>(
        sessions: P,
        channels: impl IntoIterator<Item = impl Into<String>>,
        (parameters, pacing, opening): (SealingParameters, Pacing, Opening),
        ceiling: Option<Duration>,
        sentinel: Option<&str>,
        observer: impl Fn(&str) -> Arc<dyn SealingObserver>,
        kms: impl Fn(&str) -> K,
    ) -> Result<Self, InvalidParameters>
    where
        P: SessionPool + Clone + Send + Sync + 'static,
        P::Session: Sync,
        K: KeyManagementService<P::Session> + 'static,
    {
        pacing.supply(parameters.reuse)?;
        let opening = opening.validated()?;
        // What an unwrapped key serves for is the opening's `keep`: the
        // window a key outlives its tenant's erasure is the draw's lifetime,
        // that keep, and a draw.
        if let Some(ceiling) = ceiling
            && parameters
                .reuse
                .lifetime
                .saturating_add(opening.keep)
                .saturating_add(pacing.draw)
                > ceiling
        {
            return Err(InvalidParameters("lifetime + keep + draw ≤ the ceiling"));
        }
        let mut stages: HashMap<String, Arc<dyn Stage>> = HashMap::new();
        let mut holding: HashMap<String, Arc<dyn Holding>> = HashMap::new();
        let mut dependencies: HashMap<String, Arc<dyn Dependency>> = HashMap::new();
        for channel in channels {
            let channel = channel.into();
            if stages.contains_key(&channel) {
                continue;
            }
            let stage = EnvelopeStage::new(sessions.clone(), kms(&channel))
                .paced(parameters.reuse, pacing)?
                .opened(opening)?
                .observed_by(observer(&channel));
            let stage = Arc::new(match sentinel {
                Some(sentinel) => stage.with_sentinel(sentinel),
                None => stage,
            });
            holding.insert(channel.clone(), Arc::clone(&stage) as Arc<dyn Holding>);
            dependencies.insert(channel.clone(), stage.dependency());
            stages.insert(channel, stage);
        }
        Ok(Sealing {
            stages: Arc::new(stages),
            holding: Arc::new(holding),
            dependencies: Arc::new(dependencies),
        })
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

    /// What each channel's stage holds now: gauges for an operator.
    pub fn held(&self) -> impl Iterator<Item = (&str, Held)> {
        self.holding
            .iter()
            .map(|(channel, stage)| (channel.as_str(), stage.held()))
    }

    /// The stage of an inbox that receives several channels: for each
    /// `(destination, channel)` of `map`, a row whose `destination` is the
    /// first, key or no key, opens with the stage of the second — the
    /// channel it was sealed under at the producer's end. Built up front,
    /// like the sealing itself. Its [`Dependent::dependencies`], by the same
    /// destinations, are what the inbox's breaker is to be given (seedwork
    /// ADR-0042 decision 1).
    ///
    /// # Errors
    /// [`UnsealedChannel`] for a channel the sealing was not built for: a
    /// composition root refuses to start rather than route a destination to
    /// nothing.
    pub fn by_destination<D, C>(
        &self,
        map: impl IntoIterator<Item = (D, C)>,
    ) -> Result<ByDestination, UnsealedChannel>
    where
        D: Into<String>,
        C: Into<String>,
    {
        let mut stages: HashMap<String, Arc<dyn Stage>> = HashMap::new();
        let mut dependencies: HashMap<String, Arc<dyn Dependency>> = HashMap::new();
        for (destination, channel) in map {
            let (destination, channel) = (destination.into(), channel.into());
            let unsealed = || UnsealedChannel {
                destination: destination.clone(),
                channel: channel.clone(),
            };
            let stage = self.of(&channel).ok_or_else(unsealed)?;
            let dependency = self
                .dependencies
                .get(&channel)
                .cloned()
                .ok_or_else(unsealed)?;
            let destination = uri::without_key(&destination).to_owned();
            dependencies.insert(destination.clone(), dependency);
            stages.insert(destination, stage);
        }
        Ok(ByDestination {
            stages,
            dependencies,
        })
    }
}

/// A channel the sealing was not built for, named by a destination.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnsealedChannel {
    /// The destination that named the channel.
    pub destination: String,
    /// The channel.
    pub channel: String,
}

impl fmt::Display for UnsealedChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the sealing was not built for the channel `{}`, which the destination `{}` names",
            self.channel, self.destination
        )
    }
}

impl std::error::Error for UnsealedChannel {}

/// The stage of an inbox that receives several channels: one of the
/// sealing's stages per destination, chosen per message by its
/// `destination` header, the key left aside. A message without the header,
/// or with a destination the stage was not built for, is refused for good
/// without a call to any KMS: it is not opened under a guessed context.
pub struct ByDestination {
    stages: HashMap<String, Arc<dyn Stage>>,
    dependencies: HashMap<String, Arc<dyn Dependency>>,
}

impl fmt::Debug for ByDestination {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ByDestination")
            .field("destinations", &self.stages.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl ByDestination {
    /// The destinations the stage was built for, keys left aside.
    pub fn destinations(&self) -> impl Iterator<Item = &str> {
        self.stages.keys().map(String::as_str)
    }

    fn stage_of(&self, message: &Message) -> Result<Arc<dyn Stage>, BoxError> {
        let destination = message
            .header(DESTINATION)
            .ok_or_else(|| refused(format!("the message has no `{DESTINATION}` header")))?;
        let destination = std::str::from_utf8(destination)
            .map_err(|_| refused(format!("the `{DESTINATION}` header is not text")))?;
        self.stages
            .get(uri::without_key(destination))
            .cloned()
            .ok_or_else(|| refused(format!("no stage for the destination `{destination}`")))
    }
}

fn refused(what: String) -> BoxError {
    Permanent::because(causes::MALFORMED, what).into()
}

impl Dependent for ByDestination {
    /// Each destination's dependency: the KMS of the channel it opens under.
    fn dependencies(&self) -> Vec<(String, Arc<dyn Dependency>)> {
        let mut dependencies: Vec<(String, Arc<dyn Dependency>)> = self
            .dependencies
            .iter()
            .map(|(destination, dependency)| (destination.clone(), Arc::clone(dependency)))
            .collect();
        dependencies.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        dependencies
    }
}

impl Stage for ByDestination {
    fn outbound(&self, message: Message) -> BoxFuture<'_, Result<Message, BoxError>> {
        Box::pin(async move {
            let stage = self.stage_of(&message)?;
            stage.outbound(message).await
        })
    }

    fn inbound(&self, message: Message) -> BoxFuture<'_, Result<Message, BoxError>> {
        Box::pin(async move {
            let stage = self.stage_of(&message)?;
            stage.inbound(message).await
        })
    }
}
