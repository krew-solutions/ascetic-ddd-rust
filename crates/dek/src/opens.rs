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

use std::collections::hash_map::RandomState;
use std::collections::{HashMap, VecDeque};
use std::hash::BuildHasher as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, Instant};

use ascetic_ddd_bus::{self as bus, BoxError, Cause, Permanent};
use ascetic_ddd_kms::{Class, Error as KmsError, Key, KeyManagementService};
use ascetic_ddd_session::SessionPool;
use tokio::sync::{Notify, watch};

use crate::draws::{Landed, Last, Parts, Unavailable, causes, unavailable_cause};
use crate::pace::{Generation, Outcome, Reason, Scope, Start};
use crate::unwraps::{Opened, Opening, Unwraps};

/// A wrapped key, as the core keys its entries.
type Wrapped = Arc<[u8]>;

struct State {
    core: Unwraps<String, Wrapped, Key, Last>,
    flights: HashMap<Generation, watch::Sender<()>>,
    /// The sentinel's key, as last read: when, and whether it was there.
    sentinel: Option<(Instant, Option<bool>)>,
    /// The cause each wrapped key the KMS refused was refused under, so that
    /// its memory tells the same cause (seedwork ADR-0042 decision 9): the
    /// core keeps the refusal, by wrapped key, not why. At most
    /// [`REFUSALS`], the oldest out.
    refusals: VecDeque<(Wrapped, Cause)>,
    /// The last key the KMS opened by a call — a flight's, or a check's of a
    /// head — when: what a check asks besides its head (seedwork ADR-0042
    /// decision 1). Aged from that opening, never by being asked again.
    control: Option<Known>,
    /// The last key refused at the stage's scope: what a check asks without a
    /// head.
    refused: Option<Known>,
}

/// A Tenant's wrapped key the opening end knows, and since when.
#[derive(Clone)]
struct Known {
    tenant: String,
    wrapped: Wrapped,
    at: Instant,
}

impl Known {
    fn is(&self, tenant: &str, wrapped: &Wrapped) -> bool {
        self.tenant == tenant && self.wrapped == *wrapped
    }
}

/// One unwrap by a call, judged as a flight's landing is.
enum Judged {
    /// The key.
    Opened(Key),
    /// How it landed instead, and why.
    Failed(Outcome<Key>, Landed, Option<KmsError>),
}

/// The most refused wrapped keys whose cause an opening end keeps.
const REFUSALS: usize = 1_024;

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
    /// Keys the KMS opened for flights: the dependency's `served`.
    served: AtomicU64,
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
                refusals: VecDeque::new(),
                control: None,
                refused: None,
            }),
            sentinel,
            seed: RandomState::new(),
            counter: AtomicU64::new(0),
            tick: Arc::new(Notify::new()),
            ticking: Arc::new(AtomicBool::new(false)),
            served: AtomicU64::new(0),
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
            let opened = now();
            let (answer, start, retired, landing) = {
                let mut state = self.lock();
                let (answer, start) =
                    state
                        .core
                        .open(&tenant.to_owned(), &wrapped, opened, deadline);
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
            let (generation, until) = match answer {
                Opened::Hit(key) => return Ok(key),
                Opened::Gone => return Err(gone(tenant)),
                Opened::Rejected => {
                    let cause = self
                        .lock()
                        .refusals
                        .iter()
                        .find(|(refused, _)| *refused == wrapped)
                        .map(|(_, cause)| *cause);
                    return Err(rejected(cause));
                }
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
                Opened::Join(generation, until) => (generation, until),
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
                    tokio::time::Instant::from_std(until),
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
            let mut state = self.lock();
            let key = tenant.to_owned();
            let scope = scope.unwrap_or_else(|| state.core.scope(&key, wrapped, reason, now()));
            if scope == Scope::Stage {
                state.refused = Some(Known {
                    tenant: key.clone(),
                    wrapped: Arc::clone(wrapped),
                    at: now(),
                });
            }
            (scope, state.core.noted(&key).cloned())
        };
        let bus_scope = match scope {
            Scope::Stage => bus::Scope::Stage,
            Scope::Tenant => bus::Scope::Tenant,
        };
        bus::Unavailable::new(
            unavailable_cause(last.as_ref().map(|last| (last.landed, last.cause))),
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
            match opens
                .judged(&tenant, &wrapped, &AtomicBool::new(false))
                .await
            {
                Judged::Opened(key) => guard.land(
                    Outcome::Drawn {
                        key,
                        started: guard.started,
                    },
                    Landed::Drawn,
                    None,
                ),
                Judged::Failed(outcome, landed, error) => {
                    guard.land(outcome, landed, error.as_ref())
                }
            }
        });
        true
    }

    /// One unwrap of `tenant`'s `wrapped` by a call, cut at `draw`, judged as
    /// a flight's landing: a key gone is the Tenant's only while the sentinel
    /// says the KMS keeps its keys. `asked` is set once a session was had and
    /// the call made. A flight and a check go through it alike, so that a
    /// check judges as the rows' opening would (seedwork ADR-0042 decision 1).
    async fn judged(&self, tenant: &str, wrapped: &Wrapped, asked: &AtomicBool) -> Judged {
        let unwrapped = tokio::time::timeout(
            self.opening.draw,
            self.parts.sessions.session(async |session| {
                asked.store(true, Ordering::Release);
                self.parts.kms.decrypt_dek(&session, tenant, wrapped).await
            }),
        )
        .await;
        match unwrapped {
            Ok(Ok(key)) => Judged::Opened(key),
            Ok(Err(error)) => {
                let (outcome, landed) = match error.class() {
                    Class::Gone => {
                        if self.sentinel_present().await == Some(true) {
                            (Outcome::Gone, Landed::Gone)
                        } else {
                            // The sentinel gone too, or not to be read: the
                            // environment's fault, not the tenant's.
                            (Outcome::Unavailable, Landed::Unavailable)
                        }
                    }
                    Class::Refused => (Outcome::Refused, Landed::Refused),
                    Class::Unavailable => (Outcome::Unavailable, Landed::Unavailable),
                    Class::Moment => (Outcome::Moment, Landed::Moment),
                };
                Judged::Failed(outcome, landed, Some(error))
            }
            Err(_) => Judged::Failed(Outcome::Abandoned, Landed::Abandoned, None),
        }
    }

    /// Asks the KMS now, past the opening's memory and pacing (seedwork
    /// ADR-0042 decision 1): `head`'s key, or without one the last key
    /// refused at the stage's scope, and the control — the last key the KMS
    /// opened by a call, younger than `keep` — concurrently, each judged as a
    /// flight's landing and cut at `draw`. Served as soon as one is opened, a
    /// head's then the control; unserved under the control's cause, else the
    /// other's, when one was refused or ran past the bound; unasked when
    /// nothing was asked, no session was had, or every answer was about the
    /// key itself. Its keys are dropped, wiped, never kept; it marks no
    /// landing in the core, and logs and keeps no error's text.
    pub(crate) async fn check(&self, head: Option<(String, Wrapped)>) -> bus::Check {
        let at = now();
        let (control, other) = {
            let mut state = self.lock();
            if state
                .control
                .as_ref()
                .is_some_and(|control| at >= control.at + self.opening.keep)
            {
                state.control = None;
            }
            let other = head
                .map(|(tenant, wrapped)| Known {
                    tenant,
                    wrapped,
                    at,
                })
                .or_else(|| state.refused.clone());
            (state.control.clone(), other)
        };
        let other = other.filter(|other| {
            control
                .as_ref()
                .is_none_or(|control| !control.is(&other.tenant, &other.wrapped))
        });
        let mut asks: futures::stream::FuturesUnordered<_> = control
            .into_iter()
            .map(|known| (true, known))
            .chain(other.into_iter().map(|known| (false, known)))
            .map(|(is_control, known)| async move {
                let asked = AtomicBool::new(false);
                let judged = tokio::time::timeout(
                    self.opening.draw,
                    self.judged(&known.tenant, &known.wrapped, &asked),
                )
                .await;
                (is_control, known, asked.load(Ordering::Acquire), judged)
            })
            .collect();
        let (mut control_cause, mut other_cause) = (None, None);
        while let Some((is_control, known, asked, judged)) =
            futures::StreamExt::next(&mut asks).await
        {
            let cause = match judged {
                Ok(Judged::Opened(key)) => {
                    drop(key);
                    let mut state = self.lock();
                    if !is_control {
                        if state
                            .refused
                            .as_ref()
                            .is_some_and(|refused| refused.is(&known.tenant, &known.wrapped))
                        {
                            state.refused = None;
                        }
                        state.control = Some(Known { at: now(), ..known });
                    }
                    return bus::Check::Served;
                }
                Ok(Judged::Failed(_, Landed::Gone | Landed::Refused, _)) => {
                    // about the key itself: no evidence of the KMS, and the
                    // key no longer worth asking
                    let mut guard = self.lock();
                    let state = &mut *guard;
                    for slot in [&mut state.control, &mut state.refused] {
                        if slot
                            .as_ref()
                            .is_some_and(|held| held.is(&known.tenant, &known.wrapped))
                        {
                            *slot = None;
                        }
                    }
                    None
                }
                Ok(Judged::Failed(_, _, error)) if asked => {
                    Some(error.as_ref().map_or(causes::UNAVAILABLE, causes::of))
                }
                Err(_) if asked => Some(causes::UNAVAILABLE),
                // no session within the bound: nothing was asked
                Ok(Judged::Failed(..)) | Err(_) => None,
            };
            if is_control {
                control_cause = control_cause.or(cause);
            } else {
                other_cause = other_cause.or(cause);
            }
        }
        control_cause
            .or(other_cause)
            .map_or(bus::Check::Unasked, bus::Check::Unserved)
    }

    /// The keys the KMS opened for flights so far.
    pub(crate) fn served(&self) -> u64 {
        self.served.load(Ordering::Relaxed)
    }

    /// Whether the sentinel's key is there, read at most once per `gone`:
    /// `None` when it could not be told, and with no sentinel `Some(true)`,
    /// every Gone the tenant's. A read, not a draw: a service may unwrap on
    /// the channels it only receives and not draw there (seedwork ADR-0042
    /// decision 9).
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
        let read = tokio::time::timeout(
            self.opening.draw,
            self.parts
                .sessions
                .session(async |session| self.parts.kms.key_exists(&session, sentinel).await),
        )
        .await;
        let present = match read {
            Ok(Ok(present)) => Some(present),
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
            if let (Landed::Refused, Some(error)) = (landed, error) {
                state.refusals.retain(|(refused, _)| refused != wrapped);
                state
                    .refusals
                    .push_back((Arc::clone(wrapped), causes::of(error)));
                while state.refusals.len() > REFUSALS {
                    state.refusals.pop_front();
                }
            }
            state
                .core
                .landed(&key, wrapped, generation, outcome, at, random);
            match landed {
                Landed::Drawn => {
                    self.served.fetch_add(1, Ordering::Relaxed);
                    if state
                        .refused
                        .as_ref()
                        .is_some_and(|refused| refused.is(tenant, wrapped))
                    {
                        state.refused = None;
                    }
                    state.control = Some(Known {
                        tenant: key.clone(),
                        wrapped: Arc::clone(wrapped),
                        at,
                    });
                }
                // the Tenant's keys wiped: none of them a check's to ask
                Landed::Gone => {
                    let state = &mut *state;
                    for slot in [&mut state.control, &mut state.refused] {
                        if slot.as_ref().is_some_and(|held| held.tenant == tenant) {
                            *slot = None;
                        }
                    }
                }
                _ => {}
            }
            if landing.is_some() && landed != Landed::Drawn {
                let last = Last {
                    landed,
                    cause: error.map_or(causes::UNAVAILABLE, causes::of),
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
            log::debug!("a tenant's data key not unwrapped: {landed:?}");
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

/// A wrapped key the KMS refused, within its memory, under the refusal's
/// cause as it was kept; its text names no tenant.
fn rejected(cause: Option<Cause>) -> BoxError {
    Permanent::because(
        cause.unwrap_or(causes::CIPHERTEXT),
        "the KMS refused to unwrap this key, a while ago",
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
