//! The opening end's shell (ADR-0039 slice 4): it carries out what the
//! opening core (`unwraps`) decides. It reads tokio's clock, spawns the
//! unwraps, wakes the waiters, wipes the keys at their end and answers the
//! bus's verdicts. An open joins its wrapped key's unwrap or starts one on
//! a spawned task, and waits at most the opening's own wait; an unwrap runs
//! on when its caller is dropped and is given up past its bound.
//!
//! A key gone is told only once the stage has made sure it is the
//! tenant's and not the environment's: with a sentinel, the stage draws
//! the sentinel's key once per `gone`, and a sentinel gone too — a mount
//! wiped, a disaster-recovery copy without its keys — makes the Gone the
//! stage's outage, never a Tenant's shredding (the discriminator's runtime
//! sentinel).

use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, Instant};

use ascetic_ddd_bus::{self as bus, BoxError, Permanent};
use ascetic_ddd_kms::{Class, Error as KmsError, Key, KeyManagementService};
use ascetic_ddd_session::SessionPool;
use tokio::sync::{Notify, watch};

use crate::draws::{Landed, Last, Parts, Unavailable, causes};
use crate::pace::{Generation, Outcome, Reason, Scope, Start};
use crate::unwraps::{Opened, Opening, Unwraps};

/// A wrapped key, as the core keys its entries.
type Wrapped = Arc<[u8]>;

struct State {
    core: Unwraps<String, Wrapped, Key, Last>,
    flights: HashMap<Generation, watch::Sender<()>>,
    /// The sentinel's key, as last drawn: when, and whether it was there.
    sentinel: Option<(Instant, Option<bool>)>,
}

/// What an opening end holds now.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OpeningHeld {
    /// Keys unwrapped and held.
    pub keys: usize,
    /// Entries held: keys and memories of refusals.
    pub entries: usize,
    /// Unwraps in flight.
    pub flights: usize,
    /// Entries evicted to make room, in all.
    pub evicted: u64,
    /// Tenants whose key is remembered gone.
    pub gone: usize,
}

/// A stage's opening end.
pub(crate) struct Opener<P, K> {
    parts: Arc<Parts<P, K>>,
    opening: Opening,
    state: Mutex<State>,
    sentinel: Option<String>,
    seed: RandomState,
    counter: AtomicU64,
    tick: Arc<Notify>,
    ticking: Arc<AtomicBool>,
}

/// Its tick's flag, cleared however the tick ends.
struct Ticking(Arc<AtomicBool>);

impl Drop for Ticking {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}

impl<P, K> Drop for Opener<P, K> {
    fn drop(&mut self) {
        self.tick.notify_one();
    }
}

/// A waiter joined to an unwrap. It leaves the core once: woken, at its
/// deadline, or dropped with its caller.
struct Joined<'a, P, K> {
    opens: &'a Opener<P, K>,
    tenant: &'a str,
    wrapped: Wrapped,
    generation: Generation,
    landing: watch::Receiver<()>,
    present: bool,
}

impl<P, K> Joined<'_, P, K> {
    fn leave(&mut self, state: &mut State) {
        if std::mem::take(&mut self.present) {
            state
                .core
                .left(&self.tenant.to_owned(), &self.wrapped, now());
        }
    }
}

impl<P, K> Drop for Joined<'_, P, K> {
    fn drop(&mut self) {
        if self.present {
            let opens = self.opens;
            self.leave(&mut opens.lock());
        }
    }
}

impl<P, K> Opener<P, K> {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The opening it unwraps by.
    pub(crate) const fn opening(&self) -> &Opening {
        &self.opening
    }
}

impl<P, K> Opener<P, K>
where
    P: SessionPool + Send + Sync + 'static,
    P::Session: Sync,
    K: KeyManagementService<P::Session> + 'static,
{
    pub(crate) fn over(
        parts: Arc<Parts<P, K>>,
        opening: Opening,
        sentinel: Option<String>,
    ) -> Self {
        Opener {
            parts,
            opening,
            state: Mutex::new(State {
                core: Unwraps::new(opening),
                flights: HashMap::new(),
                sentinel: None,
            }),
            sentinel,
            seed: RandomState::new(),
            counter: AtomicU64::new(0),
            tick: Arc::new(Notify::new()),
            ticking: Arc::new(AtomicBool::new(false)),
        }
    }

    fn random(&self) -> u64 {
        self.seed
            .hash_one(self.counter.fetch_add(1, Ordering::Relaxed))
    }

    /// The key `wrapped` unwraps to for `tenant`.
    ///
    /// # Errors
    /// The bus's verdict: [`Permanent`] under `kms.gone` for a tenant whose
    /// key is gone, under `kms.refused` for a wrapped key the KMS refused;
    /// [`bus::Unavailable`] under `kms.unavailable` over its scope when no
    /// key is opened now.
    pub(crate) async fn key(
        self: &Arc<Self>,
        tenant: &str,
        wrapped: &[u8],
    ) -> Result<Key, BoxError> {
        let wrapped: Wrapped = Arc::from(wrapped);
        let deadline = now() + self.opening.wait;
        let mut gave_up = false;
        loop {
            let (answer, start, retired, landing) = {
                let mut state = self.lock();
                let (answer, start) =
                    state
                        .core
                        .open(&tenant.to_owned(), &wrapped, now(), deadline);
                if let Some(start) = start {
                    state.flights.insert(start.generation, watch::channel(()).0);
                }
                let landing = match answer {
                    Opened::Join(generation, _) => Some(
                        state
                            .flights
                            .get(&generation)
                            .map_or_else(|| watch::channel(()).1, watch::Sender::subscribe),
                    ),
                    _ => None,
                };
                (answer, start, state.core.take_retired(), landing)
            };
            drop(retired);
            if let Some(start) = start
                && !self.launch(tenant, &wrapped, start)
            {
                if matches!(answer, Opened::Join(..)) {
                    self.lock().core.left(&tenant.to_owned(), &wrapped, now());
                }
                return Err(self.unavailable(
                    tenant,
                    &wrapped,
                    Reason::Unspawned,
                    None,
                    Some(Scope::Stage),
                ));
            }
            let generation = match answer {
                Opened::Hit(key) => return Ok(key),
                Opened::Gone => return Err(gone(tenant)),
                Opened::Rejected => return Err(rejected(tenant)),
                Opened::Refused {
                    reason,
                    retry_after,
                    scope,
                } => {
                    let reason = if gave_up && reason == Reason::Late {
                        Reason::Waited
                    } else {
                        reason
                    };
                    return Err(self.unavailable(
                        tenant,
                        &wrapped,
                        reason,
                        retry_after,
                        Some(scope),
                    ));
                }
                Opened::Join(generation, _) => generation,
            };
            let mut joined = Joined {
                opens: self,
                tenant,
                wrapped: Arc::clone(&wrapped),
                generation,
                landing: landing.unwrap_or_else(|| watch::channel(()).1),
                present: true,
            };
            if gave_up {
                drop(joined);
                return Err(self.unavailable(tenant, &wrapped, Reason::Waited, None, None));
            }
            let landed = matches!(
                tokio::time::timeout_at(
                    tokio::time::Instant::from_std(deadline),
                    joined.landing.changed(),
                )
                .await,
                Ok(Ok(()))
            );
            {
                let mut state = self.lock();
                if !landed {
                    state
                        .core
                        .gave_up(&tenant.to_owned(), &wrapped, joined.generation, now());
                }
                joined.leave(&mut state);
            }
            gave_up = !landed;
        }
    }

    /// An open refused, as the bus's verdict: unavailable over `scope`, or
    /// the scope the core gives its reason.
    fn unavailable(
        &self,
        tenant: &str,
        wrapped: &Wrapped,
        reason: Reason,
        retry_after: Option<Duration>,
        scope: Option<Scope>,
    ) -> BoxError {
        let (scope, last) = {
            let state = self.lock();
            let key = tenant.to_owned();
            (
                scope.unwrap_or_else(|| state.core.scope(&key, wrapped, reason)),
                state.core.noted(&key).cloned(),
            )
        };
        let bus_scope = match scope {
            Scope::Stage => bus::Scope::Stage,
            Scope::Tenant => bus::Scope::Tenant,
        };
        bus::Unavailable::new(
            causes::UNAVAILABLE,
            bus_scope,
            retry_after,
            Unavailable {
                reason,
                retry_after,
                scope,
                last,
            },
        )
        .into()
    }

    /// Starts unwrap `start` of `tenant`'s `wrapped` on the current runtime;
    /// `false` when no runtime would run it, the unwrap landed `Canceled`.
    fn launch(self: &Arc<Self>, tenant: &str, wrapped: &Wrapped, start: Start) -> bool {
        let guard = Guard {
            opens: Arc::downgrade(self) as Weak<dyn Landing>,
            tenant: tenant.to_owned(),
            wrapped: Arc::clone(wrapped),
            generation: start.generation,
            started: now(),
            landed: false,
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            drop(guard);
            return false;
        };
        self.ensure_tick(&runtime);
        let opens = Arc::clone(self);
        runtime.spawn(async move {
            let mut guard = guard;
            let (tenant, wrapped) = (guard.tenant.clone(), Arc::clone(&guard.wrapped));
            let unwrapped = tokio::time::timeout(
                opens.opening.draw,
                opens.parts.sessions.session(async |session| {
                    opens
                        .parts
                        .kms
                        .decrypt_dek(&session, &tenant, &wrapped)
                        .await
                }),
            )
            .await;
            match unwrapped {
                Ok(Ok(key)) => guard.land(
                    Outcome::Drawn {
                        key,
                        started: guard.started,
                    },
                    Landed::Drawn,
                    None,
                ),
                Ok(Err(error)) => {
                    let (outcome, landed) = match error.class() {
                        Class::Gone => {
                            if opens.sentinel_present().await == Some(true) {
                                (Outcome::Gone, Landed::Gone)
                            } else {
                                // The sentinel gone too, or not to be read:
                                // the environment's fault, not the tenant's.
                                (Outcome::Unavailable, Landed::Unavailable)
                            }
                        }
                        Class::Refused => (Outcome::Refused, Landed::Refused),
                        Class::Unavailable => (Outcome::Unavailable, Landed::Unavailable),
                        Class::Moment => (Outcome::Moment, Landed::Moment),
                    };
                    guard.land(outcome, landed, Some(&error));
                }
                Err(_) => guard.land(Outcome::Abandoned, Landed::Abandoned, None),
            }
        });
        true
    }

    /// Whether the sentinel's key is there, drawn at most once per `gone`:
    /// `None` when it could not be told, and with no sentinel `Some(true)`,
    /// every Gone the tenant's.
    async fn sentinel_present(&self) -> Option<bool> {
        let Some(sentinel) = &self.sentinel else {
            return Some(true);
        };
        let at = now();
        if let Some((drawn, present)) = self.lock().sentinel
            && at < drawn + self.opening.gone
        {
            return present;
        }
        let drawn = tokio::time::timeout(
            self.opening.draw,
            self.parts
                .sessions
                .session(async |session| self.parts.kms.generate_dek(&session, sentinel).await),
        )
        .await;
        let present = match drawn {
            Ok(Ok(_)) => Some(true),
            Ok(Err(error)) if error.class() == Class::Gone => Some(false),
            Ok(Err(_)) | Err(_) => None,
        };
        if present == Some(false) {
            log::error!(
                "the opening's sentinel key is not there: a key gone is read as the KMS's outage, \
                 not a tenant's shredding"
            );
        }
        self.lock().sentinel = Some((at, present));
        present
    }

    /// Unwrap `generation` landed: the core told under the lock, its
    /// waiters woken, the tick re-armed, the keys retired wiped outside the
    /// lock.
    fn land_flight(
        &self,
        flight: (&str, &Wrapped, Generation),
        outcome: Outcome<Key>,
        landed: Landed,
        error: Option<&KmsError>,
    ) {
        let (tenant, wrapped, generation) = flight;
        let random = self.random();
        let at = now();
        let retired = {
            let mut state = self.lock();
            let landing = state.flights.remove(&generation);
            let key = tenant.to_owned();
            state
                .core
                .landed(&key, wrapped, generation, outcome, at, random);
            if landing.is_some() && landed != Landed::Drawn {
                let last = Last {
                    landed,
                    text: error.map_or_else(|| format!("{landed:?}"), ToString::to_string),
                };
                state.core.note(&key, Some(last));
            }
            if let Some(landing) = landing {
                landing.send_replace(());
            }
            state.core.sweep(at);
            state.core.take_retired()
        };
        self.tick.notify_one();
        drop(retired);
        if landed != Landed::Drawn {
            log::debug!("a data key of {tenant} not unwrapped: {landed:?}");
        }
    }

    /// The tick that wipes keys at their end, started at the first unwrap:
    /// it holds the stage weakly, and ends with it.
    fn ensure_tick(self: &Arc<Self>, runtime: &tokio::runtime::Handle) {
        if self.ticking.swap(true, Ordering::AcqRel) {
            return;
        }
        let (opens, tick) = (Arc::downgrade(self), Arc::clone(&self.tick));
        let ticking = Ticking(Arc::clone(&self.ticking));
        runtime.spawn(async move {
            let _ticking = ticking;
            loop {
                let next = {
                    let Some(opens) = opens.upgrade() else { return };
                    opens.lock().core.next_deadline()
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
                let Some(opens) = opens.upgrade() else { return };
                let retired = {
                    let mut state = opens.lock();
                    state.core.sweep(now());
                    state.core.take_retired()
                };
                drop(retired);
            }
        });
    }

    /// What the opening end holds now.
    pub(crate) fn held(&self) -> OpeningHeld {
        let state = self.lock();
        OpeningHeld {
            keys: state.core.keys_held(),
            entries: state.core.entries(),
            flights: state.core.flights(),
            evicted: state.core.evicted(),
            gone: state.core.gone(now()),
        }
    }
}

/// A tenant's key gone, for good.
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

/// A wrapped key the KMS refused, within its memory.
fn rejected(tenant: &str) -> BoxError {
    Permanent::because(
        causes::REFUSED,
        format!("the KMS refused to unwrap this key of {tenant}, a while ago"),
    )
    .into()
}

/// What lands an unwrap: the stage, seen without its parameters, so that a
/// guard's drop can land it.
trait Landing: Send + Sync {
    fn land(
        &self,
        flight: (&str, &Wrapped, Generation),
        outcome: Outcome<Key>,
        landed: Landed,
        error: Option<&KmsError>,
    );
}

impl<P, K> Landing for Opener<P, K>
where
    P: SessionPool + Send + Sync + 'static,
    P::Session: Sync,
    K: KeyManagementService<P::Session> + 'static,
{
    fn land(
        &self,
        flight: (&str, &Wrapped, Generation),
        outcome: Outcome<Key>,
        landed: Landed,
        error: Option<&KmsError>,
    ) {
        self.land_flight(flight, outcome, landed, error);
    }
}

/// An unwrap's landing, made once: by its task, or by its drop.
struct Guard {
    opens: Weak<dyn Landing>,
    tenant: String,
    wrapped: Wrapped,
    generation: Generation,
    started: Instant,
    landed: bool,
}

impl Guard {
    fn land(&mut self, outcome: Outcome<Key>, landed: Landed, error: Option<&KmsError>) {
        self.landed = true;
        if let Some(opens) = self.opens.upgrade() {
            opens.land(
                (&self.tenant, &self.wrapped, self.generation),
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
            self.land(Outcome::Canceled, Landed::Canceled, None);
        }
    }
}
