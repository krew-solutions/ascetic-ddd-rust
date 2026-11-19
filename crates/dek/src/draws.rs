//! The shell of a stage's data keys (ADR-0039): what the pure core
//! ([`crate::supply`]) answers, carried out — a seal waits on a flight with
//! no lock held and no session, a flight is a task of its own that no
//! waiter's cancellation cancels, a key leaves memory at its end though no
//! one asks, and what happens is told to an observer and to the log.
//!
//! The core is held under one std mutex per stage, never across an await;
//! a waiter subscribes to its flight's landing, or to the stage's freed
//! places, while it is held, so that no landing is missed. A flight is
//! spawned once the mutex is released: on a runtime that is closing, tokio
//! drops the flight's future inside `spawn`, and its guard lands it
//! `Canceled` without the mutex held.
//!
//! A stage that seals each message under a key of its own shares no draw:
//! each seal draws, bounded by `draw`, as it always did.

use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::BuildHasher as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, Instant};

use ascetic_ddd_bus::{self as bus, BoxError, Cause, Permanent, cause};
use ascetic_ddd_kms::{Class, Error as KmsError, Key, KeyManagementService};
use ascetic_ddd_session::SessionPool;
use tokio::sync::{Notify, watch};

use crate::supply::{Answer, Core, Generation, Kind, Outcome, Reason, Scope, Start, Supply};

/// A data key in service: the key, and the key wrapped by the tenant's KEK.
#[derive(Clone)]
pub(crate) struct Served {
    pub(crate) dek: Key,
    pub(crate) wrapped: Arc<[u8]>,
}

impl fmt::Debug for Served {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Served")
    }
}

/// The causes the stage names its verdicts under (ADR-0040 decision 2).
pub mod causes {
    use ascetic_ddd_kms::{Class, Error as KmsError};

    use super::{Cause, cause};

    /// The tenant's key is not there: shredded, or never made.
    pub const GONE: Cause = cause!("kms.gone");
    /// The KMS refuses the operation for the context, the key there: a
    /// policy's verdict, which a deploy mends.
    pub const FORBIDDEN: Cause = cause!("kms.forbidden");
    /// The version the wrapped key names is retired, or was never made.
    pub const KEY_VERSION: Cause = cause!("kms.key_version");
    /// A ciphertext or a signature that does not open under the key.
    pub const CIPHERTEXT: Cause = cause!("kms.ciphertext");
    /// Bytes the KMS was handed, or answered, that are not what they should
    /// be.
    pub const KMS_MALFORMED: Cause = cause!("kms.malformed");
    /// An algorithm or an operation the KMS does not serve.
    pub const UNSUPPORTED: Cause = cause!("kms.unsupported");
    /// A key that exists made otherwise than asked: custody's.
    pub const CONFLICT: Cause = cause!("kms.conflict");
    /// The KMS serves no key now.
    pub const UNAVAILABLE: Cause = cause!("kms.unavailable");
    /// The KMS refused the caller's own credentials: a token expired or
    /// revoked.
    pub const UNAUTHORIZED: Cause = cause!("kms.unauthorized");
    /// What the KMS did not explain, nor the stage attribute.
    pub const UNEXPLAINED: Cause = cause!("kms.unexplained");
    /// The message cannot be sealed or opened for its own values.
    pub const MALFORMED: Cause = cause!("dek.malformed");

    /// The cause of a KMS's failure, by its remedy (seedwork ADR-0042
    /// decision 9): what an operator mends, never the error's text.
    #[must_use]
    pub fn of(error: &KmsError) -> Cause {
        match error {
            KmsError::KekNotFound {
                key_version: Some(_),
                ..
            }
            | KmsError::WrongKeyVersion { .. }
            | KmsError::NoKeyOfVersion(_) => KEY_VERSION,
            KmsError::Decrypt | KmsError::InvalidSignature => CIPHERTEXT,
            KmsError::Malformed(_) => KMS_MALFORMED,
            KmsError::UnsupportedAlgorithm(_) | KmsError::Unsupported(_) => UNSUPPORTED,
            KmsError::Conflict { .. } => CONFLICT,
            KmsError::Forbidden { .. } => FORBIDDEN,
            other if other.unauthorized() => UNAUTHORIZED,
            other => match other.class() {
                Class::Gone => GONE,
                Class::Unavailable => UNAVAILABLE,
                Class::Refused | Class::Moment => UNEXPLAINED,
            },
        }
    }
}

/// What the stage's last draw for a tenant came to, when it failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Last {
    /// Its landing.
    pub landed: Landed,
    /// Its cause, by remedy ([`causes::of`]): a landing with no error, the
    /// KMS not answering, is [`causes::UNAVAILABLE`].
    pub cause: Cause,
    /// Its error's words, for the log; never a verdict's ground.
    pub text: String,
}

/// Why a seal was not served now: of the moment, to be asked again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unavailable {
    /// Why.
    pub reason: Reason,
    /// When asking again may be served, when the stage knows.
    pub retry_after: Option<Duration>,
    /// Whose seals the refusal stops, as far as the stage can tell
    /// (ADR-0040 decision 3).
    pub scope: Scope,
    /// What the stage's last draw for the tenant came to, if it failed.
    pub last: Option<Last>,
}

impl Unavailable {
    /// The bus's verdict on the seal not served (ADR-0040 decisions 2 and
    /// 3): for good when the tenant's last draw found the context refused —
    /// a deploy, not a retry, mends that; otherwise unavailable over the
    /// refusal's scope, asked again past its `retry_after`. A key gone is no
    /// refusal of this kind: within its memory every seal is told
    /// [`Permanent`] under `kms.gone` (ADR-0039 decision 8), and past it a
    /// probe finds out. A host returns the verdict from a handler whose
    /// message the inbox is to hold, spend or park as it says.
    #[must_use]
    pub fn verdict(self) -> BoxError {
        let last = self.last.as_ref().map(|last| (last.landed, last.cause));
        match last {
            Some((Landed::Refused, cause)) => Permanent::because(cause, self).into(),
            _ => {
                let scope = match self.scope {
                    Scope::Stage => bus::Scope::Stage,
                    Scope::Tenant => bus::Scope::Tenant,
                };
                let retry_after = self.retry_after;
                bus::Unavailable::new(unavailable_cause(last), scope, retry_after, self).into()
            }
        }
    }
}

/// The cause an `Unavailable` names: the last landing's when it failed for
/// want of an answer or a credential, else the KMS's unavailability.
pub(crate) fn unavailable_cause(last: Option<(Landed, Cause)>) -> Cause {
    match last {
        Some((Landed::Unavailable | Landed::Moment, cause)) => cause,
        _ => causes::UNAVAILABLE,
    }
}

impl fmt::Display for Unavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = match self.reason {
            Reason::Waited => "the draw did not land within the wait",
            Reason::BackingOff => "the last draw failed, and the stage backs off",
            Reason::Late => "a draw is in flight that did not land in time",
            Reason::FlightsFull => "the stage runs as many draws as it may",
            Reason::Unspawned => "no runtime would run the draw",
        };
        write!(f, "no data key now: {reason}")?;
        if let Some(after) = self.retry_after {
            write!(f, "; again after {}ms", after.as_millis())?;
        }
        if let Some(last) = &self.last {
            write!(f, "; the last draw: {}", last.text)?;
        }
        Ok(())
    }
}

impl std::error::Error for Unavailable {}

/// What became of a flight, as an observer is told it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Landed {
    /// A key was drawn.
    Drawn,
    /// The tenant's key is not there.
    Gone,
    /// The KMS refused, for a reason a deploy mends.
    Refused,
    /// The KMS could not answer now.
    Unavailable,
    /// Something the stage could not attribute.
    Moment,
    /// The draw passed its bound.
    Abandoned,
    /// The draw's task ended without an answer.
    Canceled,
}

/// A flight that landed, as an observer is told it.
#[derive(Debug)]
#[non_exhaustive]
pub struct Flight<'a> {
    /// The tenant.
    pub tenant: &'a str,
    /// What it was for.
    pub kind: Kind,
    /// What became of it.
    pub landed: Landed,
    /// The KMS's error, when it failed with one.
    pub error: Option<&'a KmsError>,
    /// How long it took.
    pub elapsed: Duration,
    /// How many of the tenant's draws have failed in a row, this one too.
    pub failures: u32,
}

/// What a stage tells of its keys: called after the transition, outside
/// the stage's lock; every method does nothing unless implemented. A stage
/// that seals each message under a key of its own holds no key: its draws
/// are told as flights and its seals as served, and nothing is retired.
pub trait SealingObserver: Send + Sync {
    /// A flight landed.
    fn on_flight(&self, _flight: &Flight<'_>) {}
    /// A seal was served a key.
    fn on_served(&self, _tenant: &str) {}
    /// A seal was not served.
    fn on_unserved(&self, _tenant: &str, _reason: Reason) {}
    /// Keys went out of service, wiped: at their end, their uses spent,
    /// replaced by a successor, or their tenant's gone.
    fn on_retired(&self, _count: usize) {}
}

/// What a stage holds now.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Held {
    /// The keys in service.
    pub keys: usize,
    /// The tenants it keeps anything for.
    pub slots: usize,
    /// The draws in flight.
    pub flights: usize,
}

/// How often a stage logs its slots' transitions, at most: one line, the
/// transitions kept quiet since counted into the next (decision 10).
const TOLD_EVERY: Duration = Duration::from_secs(10);

/// A stage's log of its slots' transitions, rate-limited.
#[derive(Debug, Default)]
struct Told {
    last: Option<Instant>,
    quiet: u32,
}

impl Told {
    /// Whether a transition at `now` is logged: with the count kept quiet
    /// before it, or `None` when it is kept quiet itself.
    fn admit(&mut self, now: Instant) -> Option<u32> {
        if self
            .last
            .is_some_and(|last| now.saturating_duration_since(last) < TOLD_EVERY)
        {
            self.quiet = self.quiet.saturating_add(1);
            return None;
        }
        self.last = Some(now);
        Some(std::mem::take(&mut self.quiet))
    }
}

impl SealingObserver for () {}

/// The stage's random words: drawn from a seed of the process, or fixed.
#[derive(Debug)]
enum Seed {
    Process(RandomState),
    Fixed(u64),
}

#[derive(Debug)]
struct State {
    /// The core, noting why a tenant's last draw failed.
    core: Core<String, Served, Last>,
    /// Each flight's landing, told to its waiters: made when it starts,
    /// dropped when it lands.
    flights: HashMap<Generation, watch::Sender<()>>,
    /// Every landing, told to the seals waiting for a place.
    places: watch::Sender<()>,
    told: Told,
}

/// Where a stage's keys come from: the sessions and the KMS, shared by
/// every draws built over them — a stage configured again while a flight
/// of its draws is out builds new draws over the same parts.
pub(crate) struct Parts<P, K> {
    pub(crate) sessions: P,
    pub(crate) kms: K,
}

/// A stage's data keys: the core, the KMS it draws from, and how it tells.
pub(crate) struct Draws<P, K> {
    pub(crate) parts: Arc<Parts<P, K>>,
    supply: Supply,
    state: Mutex<State>,
    seed: Seed,
    counter: AtomicU64,
    observer: Arc<dyn SealingObserver>,
    tick: Arc<Notify>,
    ticking: Arc<AtomicBool>,
    /// The tenant drawn for evidence, carried to draws made anew.
    sentinel: Option<String>,
}

/// The tick running: cleared when its task ends.
struct Ticking(Arc<AtomicBool>);

impl Drop for Ticking {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl<P, K> fmt::Debug for Draws<P, K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Draws")
            .field("supply", &self.supply)
            .finish_non_exhaustive()
    }
}

/// The time, as tokio keeps it: a paused clock moves it.
fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}

/// What a stage's core is told a KMS error came to.
fn outcome_of(error: &KmsError) -> (Outcome<Served>, Landed) {
    match error.class() {
        Class::Gone => (Outcome::Gone, Landed::Gone),
        Class::Refused => (Outcome::Refused, Landed::Refused),
        Class::Unavailable => (Outcome::Unavailable, Landed::Unavailable),
        Class::Moment => (Outcome::Moment, Landed::Moment),
    }
}

impl<P, K> Draws<P, K> {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// What a seal came to, the stage's lock released.
enum Step<'a, P, K> {
    Hit(Served),
    Gone,
    Refused {
        reason: Reason,
        retry_after: Option<Duration>,
        scope: Scope,
    },
    /// The stage runs as many flights as it may: wait for a place.
    Full(watch::Receiver<()>),
    Join(Joined<'a, P, K>),
}

/// A waiter joined to a flight. It leaves the core once: woken, at its
/// deadline, or dropped with its caller — a caller's cancellation is no
/// evidence that the flight is late, and a waiter that never left would
/// keep its tenant's slot for good.
struct Joined<'a, P, K> {
    draws: &'a Draws<P, K>,
    tenant: &'a str,
    generation: Generation,
    landing: watch::Receiver<()>,
    present: bool,
}

impl<P, K> Joined<'_, P, K> {
    /// Leaves, under the stage's lock its caller holds.
    fn leave(&mut self, state: &mut State) {
        if std::mem::take(&mut self.present) {
            state.core.left(&self.tenant.to_owned(), now());
        }
    }
}

/// Draws gone wake their tick, so that it ends with them.
impl<P, K> Drop for Draws<P, K> {
    fn drop(&mut self) {
        self.tick.notify_one();
    }
}

impl<P, K> Drop for Joined<'_, P, K> {
    fn drop(&mut self) {
        if self.present {
            // Never under the stage's lock: `key` leaves explicitly there.
            let draws = self.draws;
            self.leave(&mut draws.lock());
        }
    }
}

impl<P, K> Draws<P, K>
where
    P: SessionPool + Send + Sync + 'static,
    P::Session: Sync,
    K: KeyManagementService<P::Session> + 'static,
{
    pub(crate) fn new(sessions: P, kms: K, supply: Supply) -> Self {
        Self::over(Arc::new(Parts { sessions, kms }), supply)
    }

    fn over(parts: Arc<Parts<P, K>>, supply: Supply) -> Self {
        Draws {
            parts,
            supply,
            state: Mutex::new(State {
                core: Core::new(supply),
                flights: HashMap::new(),
                places: watch::channel(()).0,
                told: Told::default(),
            }),
            seed: Seed::Process(RandomState::new()),
            counter: AtomicU64::new(0),
            observer: Arc::new(()),
            tick: Arc::new(Notify::new()),
            ticking: Arc::new(AtomicBool::new(false)),
            sentinel: None,
        }
    }

    /// New draws over the same parts, paced by `supply`, seeded and
    /// observed as these are: they hold no key and fly nothing, whatever
    /// these hold or fly.
    pub(crate) fn anew(&self, supply: Supply) -> Self {
        let mut draws = Self::over(Arc::clone(&self.parts), supply);
        if let Some(sentinel) = &self.sentinel {
            draws = draws.with_sentinel(sentinel.clone());
        }
        draws.seed = match &self.seed {
            Seed::Process(state) => Seed::Process(state.clone()),
            Seed::Fixed(seed) => Seed::Fixed(*seed),
        };
        draws.observer = Arc::clone(&self.observer);
        draws
    }

    /// The supply these draws are paced by.
    pub(crate) fn supply(&self) -> Supply {
        self.supply
    }

    /// The same draws, drawing for `sentinel`'s evidence when the stage has
    /// none (ADR-0040 decision 3).
    pub(crate) fn with_sentinel(mut self, sentinel: String) -> Self {
        let state = self.state.get_mut().unwrap_or_else(PoisonError::into_inner);
        state.core = Core::new(self.supply).with_sentinel(sentinel.clone());
        self.sentinel = Some(sentinel);
        self
    }

    pub(crate) fn with_seed(mut self, seed: u64) -> Self {
        self.seed = Seed::Fixed(seed);
        self
    }

    pub(crate) fn with_observer(mut self, observer: Arc<dyn SealingObserver>) -> Self {
        self.observer = observer;
        self
    }

    /// The next random word.
    fn random(&self) -> u64 {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        match &self.seed {
            Seed::Process(state) => state.hash_one(n),
            Seed::Fixed(seed) => {
                // SplitMix64 over the seed and the count.
                let mut z = seed.wrapping_add(n.wrapping_mul(0x9E37_79B9_7F4A_7C15));
                z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                z ^ (z >> 31)
            }
        }
    }

    /// The data key `tenant`'s next message is sealed under.
    ///
    /// # Errors
    /// [`Unavailable`] when none is served now; [`Permanent`] when the
    /// tenant's key is not there, told to the first seal that asks after
    /// the draw that found it so.
    pub(crate) async fn key(self: &Arc<Self>, tenant: &str) -> Result<Served, BoxError> {
        if self.supply.per_message() {
            return self.drawn_now(tenant).await;
        }
        let deadline = now() + self.supply.wait();
        let mut gave_up = false;
        loop {
            let (step, start, retired) = {
                let mut state = self.lock();
                let (answer, start) = state.core.seal(&tenant.to_owned(), now(), deadline);
                if let Some(start) = start {
                    state.flights.insert(start.generation, watch::channel(()).0);
                }
                let step = match answer {
                    Answer::Hit(served) => Step::Hit(served),
                    Answer::Gone => Step::Gone,
                    Answer::Refused {
                        reason: Reason::FlightsFull,
                        ..
                    } if !gave_up => Step::Full(state.places.subscribe()),
                    Answer::Refused {
                        reason,
                        retry_after,
                        scope,
                    } => Step::Refused {
                        reason,
                        retry_after,
                        scope,
                    },
                    Answer::Join(generation, _) => Step::Join(Joined {
                        draws: self,
                        tenant,
                        generation,
                        // A flight the core flies is one the shell started;
                        // were it not, a closed channel wakes the waiter at
                        // once, to seal again.
                        landing: state
                            .flights
                            .get(&generation)
                            .map_or_else(|| watch::channel(()).1, watch::Sender::subscribe),
                        present: true,
                    }),
                };
                (step, start, state.core.take_retired())
            };
            self.retire(retired);
            if let Some(start) = start {
                // A hit is served whether or not its successor's draw could
                // start; a miss that cannot draw is not served.
                if !self.launch(tenant, start) && !matches!(step, Step::Hit(_)) {
                    drop(step);
                    return Err(self.unavailable(
                        tenant,
                        Reason::Unspawned,
                        None,
                        Some(Scope::Stage),
                    ));
                }
            }
            let mut joined = match step {
                Step::Hit(served) => {
                    self.observer.on_served(tenant);
                    return Ok(served);
                }
                Step::Gone => return Err(gone(tenant)),
                Step::Refused {
                    reason,
                    retry_after,
                    scope,
                } => {
                    let reason = if gave_up && reason == Reason::Late {
                        Reason::Waited
                    } else {
                        reason
                    };
                    return Err(self.unavailable(tenant, reason, retry_after, Some(scope)));
                }
                Step::Full(mut places) => {
                    // A place freed by a landing before the deadline: asked
                    // again; none, asked once more, and refused if full.
                    gave_up = !matches!(
                        tokio::time::timeout_at(
                            tokio::time::Instant::from_std(deadline),
                            places.changed(),
                        )
                        .await,
                        Ok(Ok(()))
                    );
                    continue;
                }
                Step::Join(joined) => joined,
            };
            if gave_up {
                drop(joined);
                return Err(self.unavailable(tenant, Reason::Waited, None, None));
            }
            // Woken only by its flight's landing; at the deadline, or by a
            // channel closed unlanded, the waiter gives up — so a seal asks
            // once more at most, and never spins.
            let landed = matches!(
                tokio::time::timeout_at(
                    tokio::time::Instant::from_std(deadline),
                    joined.landing.changed(),
                )
                .await,
                Ok(Ok(()))
            );
            let retired = {
                let mut state = self.lock();
                if !landed {
                    state
                        .core
                        .gave_up(&tenant.to_owned(), joined.generation, now());
                }
                joined.leave(&mut state);
                state.core.take_retired()
            };
            self.retire(retired);
            // Asked once more, for a key that landed meanwhile — or told the
            // key is gone, the first to ask since.
            gave_up = !landed;
        }
    }

    /// A key drawn for one message, on the caller's own task: nothing is
    /// shared, so nothing is single-flight; bounded by `draw`.
    async fn drawn_now(self: &Arc<Self>, tenant: &str) -> Result<Served, BoxError> {
        let started = now();
        let drawn = tokio::time::timeout(
            self.supply.draw(),
            self.parts
                .sessions
                .session(async |session| self.parts.kms.generate_dek(&session, tenant).await),
        )
        .await;
        let elapsed = now().saturating_duration_since(started);
        let tell = |landed, error: Option<&KmsError>| {
            self.observer.on_flight(&Flight {
                tenant,
                kind: Kind::Miss,
                landed,
                error,
                elapsed,
                failures: 0,
            });
        };
        match drawn {
            Ok(Ok((dek, wrapped))) => {
                tell(Landed::Drawn, None);
                self.observer.on_served(tenant);
                Ok(Served {
                    dek,
                    wrapped: wrapped.into(),
                })
            }
            Ok(Err(error)) => {
                let (_, landed) = outcome_of(&error);
                tell(landed, Some(&error));
                // At the sealing end only a key gone is for good.
                Err(if landed == Landed::Gone {
                    Permanent::because(causes::GONE, error).into()
                } else {
                    error.into()
                })
            }
            Err(_) => {
                tell(Landed::Abandoned, None);
                Err(self.unavailable(tenant, Reason::Waited, None, None))
            }
        }
    }

    /// Keys gone out of service, wiped as they drop here, outside the lock;
    /// told.
    fn retire(&self, retired: Vec<Served>) {
        let count = retired.len();
        drop(retired);
        if count > 0 {
            self.observer.on_retired(count);
        }
    }

    /// A seal not served, told, over `scope` or the scope the core gives
    /// its reason. A refusal of the stage's scope on evidence asks the core
    /// for a draw of the sentinel's, so that a quiet stage learns whether
    /// its KMS serves (ADR-0040 decision 3).
    fn unavailable(
        self: &Arc<Self>,
        tenant: &str,
        reason: Reason,
        retry_after: Option<Duration>,
        scope: Option<Scope>,
    ) -> BoxError {
        self.observer.on_unserved(tenant, reason);
        let (scope, last, probe) = {
            let mut state = self.lock();
            let key = tenant.to_owned();
            let scope = scope.unwrap_or_else(|| state.core.scope(&key, reason, now()));
            let last = state.core.noted(&key).cloned();
            let on_evidence = matches!(reason, Reason::Waited | Reason::BackingOff | Reason::Late);
            let probe = (scope == Scope::Stage && on_evidence)
                .then(|| state.core.probe(now()))
                .flatten();
            if let Some((_, start)) = &probe {
                state.flights.insert(start.generation, watch::channel(()).0);
            }
            (scope, last, probe)
        };
        if let Some((sentinel, start)) = probe {
            self.launch(&sentinel, start);
        }
        Box::new(Unavailable {
            reason,
            retry_after,
            scope,
            last,
        })
    }

    /// Starts flight `start` for `tenant` on the current runtime, after the
    /// lock was released; `false` when no runtime would run it, the flight
    /// landed `Canceled`.
    fn launch(self: &Arc<Self>, tenant: &str, start: Start) -> bool {
        let weak: Weak<dyn Landing> = Arc::downgrade(self) as Weak<Self>;
        let guard = Guard {
            draws: weak,
            tenant: tenant.to_owned(),
            generation: start.generation,
            kind: start.kind,
            started: now(),
            landed: false,
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            drop(guard);
            return false;
        };
        self.ensure_tick(&runtime);
        let draws = Arc::clone(self);
        runtime.spawn(async move {
            let mut guard = guard;
            let tenant = guard.tenant.clone();
            let drawn = tokio::time::timeout(
                draws.supply.draw(),
                draws
                    .parts
                    .sessions
                    .session(async |session| draws.parts.kms.generate_dek(&session, &tenant).await),
            )
            .await;
            match drawn {
                Ok(Ok((dek, wrapped))) => guard.land(
                    Outcome::Drawn {
                        key: Served {
                            dek,
                            wrapped: wrapped.into(),
                        },
                        started: guard.started,
                    },
                    Landed::Drawn,
                    None,
                ),
                Ok(Err(error)) => {
                    let (outcome, landed) = outcome_of(&error);
                    guard.land(outcome, landed, Some(&error));
                }
                Err(_) => guard.land(Outcome::Abandoned, Landed::Abandoned, None),
            }
        });
        true
    }

    /// Flight `generation` of `tenant` landed with `outcome`: the core told
    /// under the lock, the flight's waiters and the seals waiting for a
    /// place woken, the tick re-armed; the observer and the log told after.
    fn land_flight(
        &self,
        flight: (&str, Generation, Kind, Instant),
        outcome: Outcome<Served>,
        landed: Landed,
        error: Option<&KmsError>,
    ) {
        let (tenant, generation, kind, started) = flight;
        let random = self.random();
        let at = now();
        let (failing_before, failures, retired, told) = {
            let mut state = self.lock();
            let landing = state.flights.remove(&generation);
            let key = tenant.to_owned();
            let failing_before = state.core.noted(&key).is_some();
            state.core.landed(&key, generation, outcome, at, random);
            if let Some(landing) = landing {
                // The flight's one landing: why it failed noted in its slot,
                // to go with it; its waiters woken, the sender dropped.
                let cause = match (landed, error) {
                    (Landed::Drawn, _) => None,
                    (landed, Some(error)) => Some(Last {
                        landed,
                        cause: causes::of(error),
                        text: error.to_string(),
                    }),
                    (landed, None) => Some(Last {
                        landed,
                        cause: causes::UNAVAILABLE,
                        text: format!("{landed:?}"),
                    }),
                };
                state.core.note(&key, cause);
                landing.send_replace(());
            }
            state.places.send_replace(());
            state.core.sweep(at);
            let failures = state.core.failures(&key);
            // A slot's transition — failing, or drawn again — is logged at
            // most once per `TOLD_EVERY` per stage.
            let transition = (landed == Landed::Drawn) == failing_before;
            let told = transition.then(|| state.told.admit(at)).flatten();
            (failing_before, failures, state.core.take_retired(), told)
        };
        self.tick.notify_one();
        self.retire(retired);
        self.observer.on_flight(&Flight {
            tenant,
            kind,
            landed,
            error,
            elapsed: at.saturating_duration_since(started),
            failures,
        });
        let quiet = match told {
            Some(quiet) if quiet > 0 => {
                format!("; {quiet} more of the stage's transitions not logged since")
            }
            _ => String::new(),
        };
        match (landed, failing_before, told) {
            // No Tenant and no error's text in a line: a key's name and a
            // path carry the Tenant (seedwork ADR-0042 decision 9).
            (Landed::Drawn, true, Some(_)) => {
                log::info!("a tenant's data keys drawn again{quiet}");
            }
            (Landed::Drawn, _, _) => {}
            (_, false, Some(_)) => log::warn!(
                "a tenant's data key was not drawn: {landed:?}{}{quiet}",
                error
                    .map(|e| format!(" ({})", causes::of(e)))
                    .unwrap_or_default()
            ),
            (_, _, _) => {
                log::debug!("a tenant's data key not drawn: {landed:?}, {failures} in a row")
            }
        }
    }

    /// The tick that retires keys at their end, started at the first
    /// flight: it holds the stage weakly, and ends with it.
    fn ensure_tick(self: &Arc<Self>, runtime: &tokio::runtime::Handle) {
        if self.ticking.swap(true, Ordering::AcqRel) {
            return;
        }
        let (draws, tick) = (Arc::downgrade(self), Arc::clone(&self.tick));
        let ticking = Ticking(Arc::clone(&self.ticking));
        runtime.spawn(async move {
            // Its end, however it comes — the stage gone, the runtime shut
            // down — lets the next draw start another.
            let _ticking = ticking;
            loop {
                let next = {
                    let Some(draws) = draws.upgrade() else { return };
                    draws.lock().core.next_deadline()
                };
                match next {
                    Some(at) => {
                        tokio::select! {
                            () = tokio::time::sleep_until(tokio::time::Instant::from_std(at)) => {}
                            () = tick.notified() => {}
                        }
                    }
                    None => tick.notified().await,
                }
                let Some(draws) = draws.upgrade() else { return };
                let retired = {
                    let mut state = draws.lock();
                    state.core.sweep(now());
                    state.core.take_retired()
                };
                draws.retire(retired);
            }
        });
    }

    /// What the stage holds now.
    pub(crate) fn held(&self) -> Held {
        let state = self.lock();
        Held {
            keys: state.core.keys_held(),
            slots: state.core.slots(),
            flights: state.core.flights(),
        }
    }
}

/// The tenant's key is not there, for good.
fn gone(tenant: &str) -> BoxError {
    Permanent::because(
        causes::GONE,
        KmsError::KekNotFound {
            tenant_id: tenant.to_owned(),
            key_version: None,
        },
    )
    .into()
}

/// What lands a flight: the stage, seen without its parameters, so that a
/// guard's drop can land it.
trait Landing: Send + Sync {
    fn land(
        &self,
        flight: (&str, Generation, Kind, Instant),
        outcome: Outcome<Served>,
        landed: Landed,
        error: Option<&KmsError>,
    );
}

impl<P, K> Landing for Draws<P, K>
where
    P: SessionPool + Send + Sync + 'static,
    P::Session: Sync,
    K: KeyManagementService<P::Session> + 'static,
{
    fn land(
        &self,
        flight: (&str, Generation, Kind, Instant),
        outcome: Outcome<Served>,
        landed: Landed,
        error: Option<&KmsError>,
    ) {
        self.land_flight(flight, outcome, landed, error);
    }
}

/// A flight's landing, made once: by its task, or by its drop — a panic,
/// a runtime's end, a runtime that never ran it.
struct Guard {
    draws: Weak<dyn Landing>,
    tenant: String,
    generation: Generation,
    kind: Kind,
    started: Instant,
    landed: bool,
}

impl Guard {
    fn land(&mut self, outcome: Outcome<Served>, landed: Landed, error: Option<&KmsError>) {
        self.landed = true;
        if let Some(draws) = self.draws.upgrade() {
            draws.land(
                (&self.tenant, self.generation, self.kind, self.started),
                outcome,
                landed,
                error,
            );
        }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if !self.landed {
            // Never answered: canceled, landed without the stage's lock,
            // which no one holds where a guard is dropped (decision 5).
            self.land(Outcome::Canceled, Landed::Canceled, None);
        }
    }
}
