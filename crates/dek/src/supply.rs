//! The pure core of a stage's data keys (ADR-0039): which key a seal gets,
//! when a draw starts, how long a miss waits, and what a landing changes —
//! as transitions over a state, with the time and the randomness passed in.
//! Nothing here reads a clock, awaits, spawns or draws a key: the shell
//! does, and asks this core what follows.
//!
//! A slot per tenant holds the key in service, at most one flight, and the
//! evidence of whether a draw will land in time. A key serves from its
//! draw's start for its lifetime, less a jitter, and for so many messages;
//! a miss joins a flight and waits only on evidence that it will land in
//! time; a failed draw backs off, its slot refusing at once meanwhile. The
//! flights a stage runs at once are capped, the cap partitioned so that no
//! tenant's probes, nor the renewals, take every place.
//!
//! The core does not panic: its arithmetic saturates or is checked, and
//! the lints below refuse what would panic (ADR-0039 decision 11).

#![deny(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::unreachable,
    clippy::todo,
    clippy::unimplemented
)]

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::time::{Duration, Instant};

use crate::pace::{Evidence, Failure, Flying, Held, failed, later, part, scope_by, trouble_since};
pub use crate::pace::{Generation, Kind, LONGEST, Outcome, Reason, Scope, Start};

/// Why a supply's parameters were refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidParameters(pub &'static str);

impl std::fmt::Display for InvalidParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid sealing parameters: {}", self.0)
    }
}

impl std::error::Error for InvalidParameters {}

/// How a stage's keys are drawn, served and renewed, validated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Supply {
    lifetime: Duration,
    messages: u32,
    burst: u32,
    wait: Duration,
    draw: Duration,
    lead: (Duration, Duration),
    jitter: Duration,
    backoff: (Duration, Duration),
    gone: Duration,
    flights: u32,
}

/// The parameters a [`Supply`] is built from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SupplyParameters {
    /// How long a key serves from its draw's start, at most.
    pub lifetime: Duration,
    /// How many messages a key seals, at most.
    pub messages: u32,
    /// The most messages one caller's transaction seals for one tenant.
    pub burst: u32,
    /// How long a miss waits for a draw that will land in time.
    pub wait: Duration,
    /// How long a draw may take before it is abandoned.
    pub draw: Duration,
    /// How long before a key's end its successor is drawn: from, to.
    pub lead: (Duration, Duration),
    /// How much earlier than its lifetime a key may end, drawn per key.
    pub jitter: Duration,
    /// A failed draw's back-off: its first step, its cap.
    pub backoff: (Duration, Duration),
    /// How long a tenant whose key is gone is not asked again.
    pub gone: Duration,
    /// How many flights a stage runs at once.
    pub flights: u32,
}

impl Supply {
    /// The supply, its parameters checked against each other.
    ///
    /// # Errors
    /// [`InvalidParameters`] naming the first rule broken.
    pub fn new(p: SupplyParameters) -> Result<Self, InvalidParameters> {
        let rule = |holds: bool, rule| {
            if holds {
                Ok(())
            } else {
                Err(InvalidParameters(rule))
            }
        };
        rule(p.lifetime <= LONGEST, "lifetime ≤ a century")?;
        rule(p.burst >= 1, "burst ≥ 1")?;
        rule(
            u64::from(p.messages) >= 2 * u64::from(p.burst) || (p.messages == 1 && p.burst == 1),
            "messages ≥ 2 · burst",
        )?;
        rule(!p.wait.is_zero(), "wait > 0")?;
        rule(p.wait < p.draw, "wait < draw")?;
        let part_of = |span: Duration, n: u32| span.checked_div(n).unwrap_or(Duration::ZERO);
        rule(p.draw < part_of(p.lifetime, 2), "draw < lifetime / 2")?;
        rule(p.lead.0 >= p.draw, "the lead from ≥ draw")?;
        rule(p.lead.1 >= p.lead.0, "the lead to ≥ the lead from")?;
        rule(
            p.jitter <= part_of(p.lifetime, 10),
            "jitter ≤ lifetime / 10",
        )?;
        rule(
            p.jitter
                .saturating_add(p.lead.1)
                .saturating_add(p.draw.saturating_mul(2))
                <= p.lifetime,
            "jitter + the lead to + 2 · draw ≤ lifetime",
        )?;
        rule(
            p.backoff.0 >= p.wait.saturating_mul(2),
            "the back-off's first step ≥ 2 · wait",
        )?;
        rule(
            p.backoff.1 >= p.backoff.0,
            "the back-off's cap ≥ its first step",
        )?;
        rule(p.backoff.1 <= LONGEST, "the back-off's cap ≤ a century")?;
        rule(!p.gone.is_zero(), "gone > 0")?;
        rule(p.gone <= p.backoff.1, "gone ≤ the back-off's cap")?;
        rule(p.flights >= 1, "flights ≥ 1")?;
        Ok(Supply {
            lifetime: p.lifetime,
            messages: p.messages,
            burst: p.burst,
            wait: p.wait,
            draw: p.draw,
            lead: p.lead,
            jitter: p.jitter,
            backoff: p.backoff,
            gone: p.gone,
            flights: p.flights,
        })
    }

    /// How long a seal waits, at most.
    #[must_use]
    pub const fn wait(&self) -> Duration {
        self.wait
    }

    /// How long a draw may take.
    #[must_use]
    pub const fn draw(&self) -> Duration {
        self.draw
    }

    /// How long a key serves, at most.
    #[must_use]
    pub const fn lifetime(&self) -> Duration {
        self.lifetime
    }

    /// Whether a key seals one message: then no draw is shared, and the
    /// shell draws for each seal on its own.
    #[must_use]
    pub const fn per_message(&self) -> bool {
        self.messages == 1
    }

    /// The uses left at which a key's successor is drawn: the larger of a
    /// caller's burst and a tenth of the key's messages.
    fn renew_uses(&self) -> u32 {
        self.burst.max(self.messages / 10)
    }

    /// The probes a stage runs at once, at most: a quarter of its flights.
    fn probes(&self) -> u32 {
        self.flights.div_ceil(4)
    }
}

/// When a slot's trouble began: its failure; otherwise its flight's start,
/// or a wait before `now` if that is earlier or there is no flight.
fn slot_trouble_since<K, N>(slot: &Slot<K, N>, now: Instant, wait: Duration) -> Option<Instant> {
    match slot.evidence {
        Evidence::Failed { at, .. } => Some(at),
        Evidence::Trusted | Evidence::Late { .. } => {
            trouble_since(slot.flight.map(|flying| flying.started), now, wait)
        }
    }
}

/// What a seal is answered.
#[derive(Debug, PartialEq, Eq)]
pub enum Answer<K> {
    /// The key in service.
    Hit(K),
    /// Wait for flight `generation` until `deadline`, then ask again.
    Join(Generation, Instant),
    /// Not served now.
    Refused {
        /// Why.
        reason: Reason,
        /// When asking again may be served.
        retry_after: Option<Duration>,
        /// Whose seals the refusal stops, by the stage's evidence.
        scope: Scope,
    },
    /// The tenant's key is not there: answered to the flight's own caller.
    Gone,
}

#[derive(Debug)]
struct Serving<K> {
    key: K,
    ends: Instant,
    renew_at: Instant,
    used: u32,
}

#[derive(Debug)]
struct Slot<K, N> {
    key: Option<Serving<K>>,
    flight: Option<Flying>,
    evidence: Evidence,
    waiters: u32,
    /// What the shell remembers of the tenant while the slot is kept: it
    /// has no effect of its own, and goes with the slot.
    note: Option<N>,
    /// The slot's one deadline in the core's map: when a sweep is to come
    /// to it next.
    due: Option<(Instant, Generation)>,
}

impl<K, N> Slot<K, N> {
    const fn new() -> Self {
        Slot {
            key: None,
            flight: None,
            evidence: Evidence::Trusted,
            waiters: 0,
            note: None,
            due: None,
        }
    }

    /// When the slot changes next by time alone, after `now`: its key's
    /// end, or the end of its failure's memory — the back-off, and one cap
    /// past it in which the failure still decides the next draw (a probe, a
    /// doubled step). Each is weighed only while it is ahead: a memory that
    /// ended before a key that serves on — a renewal failed, and its back-off
    /// over — is no change, and must not hide the key's end.
    fn next(&self, memory: Duration, now: Instant) -> Option<Instant> {
        let ends = self.key.as_ref().map(|serving| serving.ends);
        let forgets = match self.evidence {
            Evidence::Failed { until, .. } => Some(later(until, memory)),
            Evidence::Trusted | Evidence::Late { .. } => None,
        };
        [ends, forgets]
            .into_iter()
            .flatten()
            .filter(|at| *at > now)
            .min()
    }

    /// Whether the slot holds nothing with a future effect as of `now`: no
    /// key, no flight, no waiter, no failure still remembered.
    fn idle(&self, now: Instant, memory: Duration) -> bool {
        self.key.is_none()
            && self.flight.is_none()
            && self.waiters == 0
            && match self.evidence {
                Evidence::Failed { until, .. } => later(until, memory) <= now,
                Evidence::Trusted | Evidence::Late { .. } => true,
            }
    }
}

/// The core of one stage's keys, a note of the shell's kept per tenant
/// while its slot is.
#[derive(Debug)]
pub struct Core<T, K, N = ()> {
    supply: Supply,
    slots: HashMap<T, Slot<K, N>>,
    in_flight: HashMap<Generation, (T, Kind)>,
    next: Generation,
    held: Held,
    /// Each slot's one deadline, by when it falls.
    deadlines: BTreeMap<(Instant, Generation), T>,
    /// The keys gone out of service since the shell last took them.
    retired: Vec<K>,
    landed_in_time: Option<Instant>,
    /// The tenant drawn for evidence when the stage has none: the host's own
    /// account, under the same KMS and policy (ADR-0040 decision 3).
    sentinel: Option<T>,
    /// When its last draw for evidence started.
    probed: Option<Instant>,
}

impl<T: Clone + Eq + Hash, K: Clone, N> Core<T, K, N> {
    /// A core with no slot.
    #[must_use]
    pub fn new(supply: Supply) -> Self {
        Core {
            supply,
            slots: HashMap::new(),
            in_flight: HashMap::new(),
            next: 1,
            held: Held::default(),
            deadlines: BTreeMap::new(),
            retired: Vec::new(),
            landed_in_time: None,
            sentinel: None,
            probed: None,
        }
    }

    /// The same, drawing for `sentinel` when the stage has no evidence of
    /// whether its KMS serves (ADR-0040 decision 3).
    #[must_use]
    pub fn with_sentinel(mut self, sentinel: T) -> Self {
        self.sentinel = Some(sentinel);
        self
    }

    /// The scope of a refusal of `tenant` for `reason` as of now: the
    /// stage's for a stage that is full or has no runtime; otherwise as the
    /// evidence says.
    #[must_use]
    pub fn scope(&self, tenant: &T, reason: Reason, now: Instant) -> Scope {
        match reason {
            Reason::FlightsFull | Reason::Unspawned => Scope::Stage,
            Reason::Waited | Reason::BackingOff | Reason::Late => scope_by(
                self.slots
                    .get(tenant)
                    .and_then(|slot| slot_trouble_since(slot, now, self.supply.wait)),
                self.landed_in_time,
            ),
        }
    }

    /// A draw for the sentinel's evidence at `now`, if one is due: at most
    /// one in flight, none within `wait` of the last, none while the
    /// sentinel's own failure backs off, and only on a place a probe may
    /// take. The shell starts it after a refusal of the stage's scope, so
    /// that a quiet stage — its other tenants served from their keys —
    /// learns whether its KMS serves before the inbox weighs a hold.
    pub fn probe(&mut self, now: Instant) -> Option<(T, Start)> {
        let sentinel = self.sentinel.clone()?;
        if self
            .probed
            .is_some_and(|at| now < later(at, self.supply.wait))
        {
            return None;
        }
        if self.slots.get(&sentinel).is_some_and(|slot| {
            slot.flight.is_some()
                || matches!(slot.evidence, Evidence::Failed { until, .. } if now < until)
        }) {
            return None;
        }
        if !self.place(Kind::Probe) {
            return None;
        }
        self.slots.entry(sentinel.clone()).or_insert_with(Slot::new);
        // the sentinel's draw is no caller's: nobody waits on it
        let start = self.start(&sentinel, Kind::Probe, now, false);
        self.probed = Some(now);
        self.settle(&sentinel, now);
        Some((sentinel, start))
    }

    /// The supply.
    #[must_use]
    pub const fn supply(&self) -> &Supply {
        &self.supply
    }

    fn generation(&mut self) -> Generation {
        let generation = self.next;
        self.next = self.next.saturating_add(1);
        generation
    }

    /// How long a failure is remembered past its back-off: a cap.
    const fn memory(&self) -> Duration {
        self.supply.backoff.1
    }

    /// Whether a place of `kind` is free.
    fn place(&self, kind: Kind) -> bool {
        let (held, cap) = (self.held, self.supply.flights);
        match kind {
            Kind::Miss => held.total() < cap,
            Kind::Probe => held.total() < cap && held.probes < self.supply.probes(),
            Kind::Renew => cap.saturating_sub(held.total()) >= cap.div_ceil(2),
        }
    }

    fn start(&mut self, tenant: &T, kind: Kind, now: Instant, trusted: bool) -> Start {
        let generation = self.generation();
        let held = self.held.of(kind);
        *held = held.saturating_add(1);
        self.in_flight.insert(generation, (tenant.clone(), kind));
        if let Some(slot) = self.slots.get_mut(tenant) {
            slot.flight = Some(Flying {
                generation,
                kind,
                started: now,
                trusted,
            });
        }
        Start { generation, kind }
    }

    /// After a transition of `tenant`'s slot at `now`: its key retires if
    /// it has ended; the slot goes when it holds nothing with a future
    /// effect, and is otherwise due for a sweep when it next changes by
    /// time, after `now` — so no slot is kept that no sweep will reach, a
    /// slot has one deadline at most, and a sweep never meets the same
    /// slot twice. A slot kept by its flight or its waiters alone needs no
    /// deadline: their landing and their leaving settle it.
    fn settle(&mut self, tenant: &T, now: Instant) {
        let memory = self.memory();
        let Some(slot) = self.slots.get_mut(tenant) else {
            return;
        };
        if slot.key.as_ref().is_some_and(|serving| serving.ends <= now)
            && let Some(serving) = slot.key.take()
        {
            self.retired.push(serving.key);
        }
        let next = slot.next(memory, now);
        let (idle, due) = (slot.idle(now, memory), slot.due);
        if idle {
            if let Some(due) = due {
                self.deadlines.remove(&due);
            }
            self.slots.remove(tenant);
            return;
        }
        if next == due.map(|(at, _)| at) {
            return;
        }
        if let Some(due) = due {
            self.deadlines.remove(&due);
        }
        let due = next.map(|at| (at, self.generation()));
        if let Some(due) = due {
            self.deadlines.insert(due, tenant.clone());
        }
        if let Some(slot) = self.slots.get_mut(tenant) {
            slot.due = due;
        }
    }

    /// What a seal for `tenant` at `now` is answered, its wait ending at
    /// `deadline`, and the flight to start, if any. A slot the seal leaves
    /// holding nothing — refused for a full stage, say — goes at once.
    pub fn seal(
        &mut self,
        tenant: &T,
        now: Instant,
        deadline: Instant,
    ) -> (Answer<K>, Option<Start>) {
        let answered = self.answer(tenant, now, deadline);
        self.settle(tenant, now);
        answered
    }

    fn answer(
        &mut self,
        tenant: &T,
        now: Instant,
        deadline: Instant,
    ) -> (Answer<K>, Option<Start>) {
        let supply = self.supply;
        let slot = self.slots.entry(tenant.clone()).or_insert_with(Slot::new);
        // The key in service, while it serves.
        if let Some(serving) = slot.key.as_mut() {
            if now < serving.ends && serving.used < supply.messages {
                serving.used = serving.used.saturating_add(1);
                let key = serving.key.clone();
                let due = now >= serving.renew_at
                    || supply.messages.saturating_sub(serving.used) <= supply.renew_uses();
                let quiet = slot.flight.is_none()
                    && !matches!(slot.evidence, Evidence::Failed { until, .. } if now < until);
                // a renewal of a trusted slot is joined at the key's end
                let trusted = slot.evidence == Evidence::Trusted;
                let start = (due && quiet && self.place(Kind::Renew))
                    .then(|| self.start(tenant, Kind::Renew, now, trusted));
                return (Answer::Hit(key), start);
            }
            // Ended, or its uses spent: it serves no more.
            if let Some(serving) = slot.key.take() {
                self.retired.push(serving.key);
            }
        }
        let landed_in_time = self.landed_in_time;
        let Some(slot) = self.slots.get_mut(tenant) else {
            return refused(Reason::Late, None, Scope::Stage);
        };
        let scope = scope_by(
            slot_trouble_since(slot, now, self.supply.wait),
            landed_in_time,
        );
        // A key gone: told to every seal that asks until its memory ends,
        // none of them drawing (ADR-0039 decision 8). A standby of Vault
        // Community forwards to the leader and never answers a key it holds
        // as missing (measured 2026-11-16), so a Gone is the key's.
        if let Evidence::Failed {
            failure: Failure::Gone,
            until,
            ..
        } = slot.evidence
            && now < until
        {
            return (Answer::Gone, None);
        }
        // A failed draw's back-off.
        if let Evidence::Failed { until, .. } = slot.evidence
            && now < until
        {
            return refused(
                Reason::BackingOff,
                Some(until.saturating_duration_since(now)),
                scope,
            );
        }
        // A flight in flight: joined only when it is waited on.
        if let Some(flying) = slot.flight {
            return if flying.trusted {
                slot.waiters = slot.waiters.saturating_add(1);
                (Answer::Join(flying.generation, deadline), None)
            } else {
                refused(Reason::Late, None, scope)
            };
        }
        // No flight: start one, and wait for it only on evidence.
        let (kind, trusted) = match slot.evidence {
            Evidence::Trusted => (Kind::Miss, true),
            Evidence::Late { .. } => (Kind::Miss, false),
            Evidence::Failed { at, .. } => (
                Kind::Probe,
                landed_in_time.is_some_and(|landed| landed > at),
            ),
        };
        if !self.place(kind) {
            return refused(Reason::FlightsFull, None, Scope::Stage);
        }
        let start = self.start(tenant, kind, now, trusted);
        if trusted {
            // A trusted probe is waited on, and the slot's failure stands:
            // only its own draw landed in time ends it, so that the back-off
            // doubles while its draws keep failing. The flight carries the
            // trust: its joiners wait on it too.
            if let Some(slot) = self.slots.get_mut(tenant) {
                slot.waiters = slot.waiters.saturating_add(1);
            }
            (Answer::Join(start.generation, deadline), Some(start))
        } else {
            (
                Answer::Refused {
                    reason: Reason::Late,
                    retry_after: None,
                    scope,
                },
                Some(start),
            )
        }
    }

    /// A waiter of `tenant` stops waiting at `now`: woken, at its deadline,
    /// or dropped with its caller. A slot it leaves holding nothing goes.
    pub fn left(&mut self, tenant: &T, now: Instant) {
        if let Some(slot) = self.slots.get_mut(tenant) {
            slot.waiters = slot.waiters.saturating_sub(1);
        }
        self.settle(tenant, now);
    }

    /// Notes `note` for `tenant`, kept while its slot is; nothing when the
    /// stage keeps no slot for it.
    pub fn note(&mut self, tenant: &T, note: Option<N>) {
        if let Some(slot) = self.slots.get_mut(tenant) {
            slot.note = note;
        }
    }

    /// The note kept for `tenant`.
    #[must_use]
    pub fn noted(&self, tenant: &T) -> Option<&N> {
        self.slots.get(tenant).and_then(|slot| slot.note.as_ref())
    }

    /// How many draws of `tenant` have failed in a row.
    #[must_use]
    pub fn failures(&self, tenant: &T) -> u32 {
        match self.slots.get(tenant).map(|slot| slot.evidence) {
            Some(Evidence::Failed { failures, .. }) => failures,
            _ => 0,
        }
    }

    /// A waiter of flight `generation` gave up at `now`, its deadline: the
    /// slot is late when that flight is still in flight and has flown a
    /// wait at least — a waiter that joined it with little of its own wait
    /// left is no evidence that it is late.
    pub fn gave_up(&mut self, tenant: &T, generation: Generation, now: Instant) {
        let wait = self.supply.wait;
        if let Some(slot) = self.slots.get_mut(tenant)
            && let Some(flying) = slot.flight.as_mut()
            && flying.generation == generation
            && now.saturating_duration_since(flying.started) >= wait
        {
            flying.trusted = false;
            if slot.evidence == Evidence::Trusted {
                slot.evidence = Evidence::Late { at: now };
            }
        }
    }

    /// Flight `generation` of `tenant` landed at `now` with `outcome`;
    /// `random` draws its jitters. A landing of a flight no longer in
    /// flight changes nothing.
    pub fn landed(
        &mut self,
        tenant: &T,
        generation: Generation,
        outcome: Outcome<K>,
        now: Instant,
        random: u64,
    ) {
        let Some((flying_for, kind)) = self.in_flight.remove(&generation) else {
            return;
        };
        let held = self.held.of(kind);
        *held = held.saturating_sub(1);
        if &flying_for != tenant {
            return;
        }
        let supply = self.supply;
        let Some(slot) = self.slots.get_mut(tenant) else {
            return;
        };
        if slot
            .flight
            .is_some_and(|flying| flying.generation == generation)
        {
            slot.flight = None;
        }
        let failures = match slot.evidence {
            Evidence::Failed { failures, .. } => failures,
            _ => 0,
        };
        let mut retired = None;
        match outcome {
            Outcome::Drawn { key, started } => {
                let ends = started
                    .checked_add(supply.lifetime)
                    .and_then(|end| end.checked_sub(part(supply.jitter, random)));
                let lead = supply.lead.0.saturating_add(part(
                    supply.lead.1.saturating_sub(supply.lead.0),
                    random.rotate_left(32),
                ));
                match ends {
                    Some(ends) if now < ends => {
                        let serving = Serving {
                            key,
                            ends,
                            renew_at: ends.checked_sub(lead).unwrap_or(started),
                            used: 0,
                        };
                        retired = slot.key.replace(serving).map(|old| old.key);
                    }
                    _ => retired = Some(key),
                }
                // a landing past the wait is no evidence that the slot's
                // draws land in time: it stays late
                if now.saturating_duration_since(started) <= supply.wait {
                    slot.evidence = Evidence::Trusted;
                    self.landed_in_time = Some(now);
                } else {
                    slot.evidence = Evidence::Late { at: now };
                }
            }
            Outcome::Gone => {
                retired = slot.key.take().map(|old| old.key);
                slot.evidence = failed(Some(supply.gone), supply.backoff, failures, now, random);
            }
            Outcome::Refused
            | Outcome::Unavailable
            | Outcome::Moment
            | Outcome::Abandoned
            | Outcome::Canceled => {
                slot.evidence = failed(None, supply.backoff, failures, now, random);
            }
        }
        self.retired.extend(retired);
        self.settle(tenant, now);
    }

    /// Flight `generation` could not be started: no runtime would run it.
    pub fn unspawned(&mut self, tenant: &T, generation: Generation, now: Instant, random: u64) {
        self.landed(tenant, generation, Outcome::Canceled, now, random);
    }

    /// Retires, as of `now`, every key past its end and evicts every slot
    /// that holds nothing with a future effect, its note with it, each one
    /// its deadline brings the sweep to.
    pub fn sweep(&mut self, now: Instant) {
        while let Some(entry) = self.deadlines.first_entry() {
            if entry.key().0 > now {
                break;
            }
            let tenant = entry.remove();
            let Some(slot) = self.slots.get_mut(&tenant) else {
                continue;
            };
            slot.due = None;
            self.settle(&tenant, now);
        }
    }

    /// The keys gone out of service since last taken — at their end, their
    /// uses spent, replaced by a successor, or their tenant's gone — to be
    /// wiped as they drop, outside the shell's lock.
    pub fn take_retired(&mut self) -> Vec<K> {
        std::mem::take(&mut self.retired)
    }

    /// When the next slot is due for a sweep: when the shell is to sweep
    /// next.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Instant> {
        self.deadlines.first_key_value().map(|((at, _), _)| *at)
    }

    /// How many keys the stage holds now.
    #[must_use]
    pub fn keys_held(&self) -> usize {
        self.slots
            .values()
            .filter(|slot| slot.key.is_some())
            .count()
    }

    /// How many slots the stage keeps.
    #[must_use]
    pub fn slots(&self) -> usize {
        self.slots.len()
    }

    /// How many flights the stage runs.
    #[must_use]
    pub fn flights(&self) -> usize {
        self.in_flight.len()
    }
}

/// A seal refused for `reason`, over `scope`.
const fn refused<K>(
    reason: Reason,
    retry_after: Option<Duration>,
    scope: Scope,
) -> (Answer<K>, Option<Start>) {
    (
        Answer::Refused {
            reason,
            retry_after,
            scope,
        },
        None,
    )
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_methods,
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    reason = "tests: a pure core's origin of time is any instant"
)]
mod tests {
    use super::*;

    const S: Duration = Duration::from_secs(1);

    fn parameters() -> SupplyParameters {
        SupplyParameters {
            lifetime: 60 * S,
            messages: 100,
            burst: 4,
            wait: Duration::from_millis(250),
            draw: 5 * S,
            lead: (5 * S, 10 * S),
            jitter: 6 * S,
            backoff: (S, 30 * S),
            gone: 10 * S,
            flights: 16,
        }
    }

    fn core_with(p: SupplyParameters) -> Core<&'static str, u32> {
        Core::new(Supply::new(p).unwrap())
    }

    fn core() -> Core<&'static str, u32> {
        core_with(parameters())
    }

    fn deadline(at: Instant) -> Instant {
        at + Duration::from_millis(250)
    }

    /// A key landed for `tenant` at `at`, its draw started then: its
    /// flight's number.
    fn drawn(
        core: &mut Core<&'static str, u32>,
        tenant: &'static str,
        at: Instant,
        key: u32,
    ) -> Generation {
        let (answer, start) = core.seal(&tenant, at, deadline(at));
        let start = start.expect("a flight");
        assert_eq!(answer, Answer::Join(start.generation, deadline(at)));
        core.left(&tenant, at);
        core.landed(
            &tenant,
            start.generation,
            Outcome::Drawn { key, started: at },
            at,
            0,
        );
        start.generation
    }

    #[test]
    fn concurrent_misses_cost_one_draw_and_wait_on_it() {
        let (mut core, t0) = (core(), Instant::now());
        let (first, start) = core.seal(&"a", t0, deadline(t0));
        let start = start.unwrap();
        assert_eq!(start.kind, Kind::Miss);
        assert_eq!(first, Answer::Join(start.generation, deadline(t0)));
        for _ in 0..4 {
            let (answer, again) = core.seal(&"a", t0, deadline(t0));
            assert_eq!(answer, Answer::Join(start.generation, deadline(t0)));
            assert_eq!(again, None, "one draw for every miss");
        }
        assert_eq!(core.flights(), 1);
    }

    #[test]
    fn a_landed_key_serves_its_waiters_no_more_than_its_uses() {
        let mut p = parameters();
        (p.messages, p.burst) = (2, 1);
        let (mut core, t0) = (core_with(p), Instant::now());
        drawn(&mut core, "a", t0, 7);
        let answers: Vec<_> = (0..5)
            .map(|_| core.seal(&"a", t0, deadline(t0)).0)
            .collect();
        let hits = answers.iter().filter(|a| **a == Answer::Hit(7)).count();
        assert_eq!(hits, 2, "a key seals its messages and no more: {answers:?}");
        assert!(
            answers[2..].iter().all(|a| matches!(a, Answer::Join(..))),
            "the rest wait for the next key: {answers:?}"
        );
    }

    #[test]
    fn a_give_up_marks_its_flight_late_and_a_late_flight_refuses_at_once() {
        let (mut core, t0) = (core(), Instant::now());
        let (_, start) = core.seal(&"a", t0, deadline(t0));
        let generation = start.unwrap().generation;
        let gave_up = deadline(t0);
        core.gave_up(&"a", generation, gave_up);
        core.left(&"a", gave_up);
        let (answer, start) = core.seal(&"a", gave_up, deadline(gave_up));
        assert_eq!(
            answer,
            Answer::Refused {
                reason: Reason::Late,
                retry_after: None,
                scope: Scope::Stage
            }
        );
        assert_eq!(start, None);
        core.landed(
            &"a",
            generation,
            Outcome::Drawn {
                key: 1,
                started: t0,
            },
            t0 + 2 * S,
            0,
        );
        assert_eq!(
            core.seal(&"a", t0 + 2 * S, t0).0,
            Answer::Hit(1),
            "trusted again"
        );
    }

    #[test]
    fn a_give_up_with_little_of_a_wait_flown_is_no_evidence() {
        let (mut core, t0) = (core(), Instant::now());
        let (_, start) = core.seal(&"a", t0, deadline(t0));
        let generation = start.unwrap().generation;
        // A waiter whose own wait was nearly spent when it joined.
        let soon = t0 + Duration::from_millis(10);
        core.gave_up(&"a", generation, soon);
        assert!(
            matches!(core.seal(&"a", soon, deadline(soon)).0, Answer::Join(..)),
            "the flight is still trusted"
        );
    }

    #[test]
    fn a_give_up_after_its_flight_landed_is_inert() {
        let (mut core, t0) = (core(), Instant::now());
        let generation = drawn(&mut core, "a", t0, 1);
        core.gave_up(&"a", generation, t0 + S);
        assert_eq!(core.seal(&"a", t0, deadline(t0)).0, Answer::Hit(1));
        // The key spent, a new miss waits: the slot is trusted.
        let mut p = parameters();
        (p.messages, p.burst) = (2, 1);
        let mut core = core_with(p);
        let generation = drawn(&mut core, "a", t0, 1);
        core.gave_up(&"a", generation, t0 + S);
        core.seal(&"a", t0, deadline(t0));
        core.seal(&"a", t0, deadline(t0));
        assert!(matches!(
            core.seal(&"a", t0, deadline(t0)).0,
            Answer::Join(..)
        ));
    }

    #[test]
    fn a_failed_draw_backs_off_doubling_to_its_cap() {
        let (mut core, t0) = (core(), Instant::now());
        let mut now = t0;
        let mut waited = Vec::new();
        for _ in 0..7 {
            let (_, start) = core.seal(&"a", now, deadline(now));
            let generation = start.expect("a probe or a miss").generation;
            core.landed(&"a", generation, Outcome::Unavailable, now, 0);
            core.left(&"a", now);
            let (answer, start) = core.seal(&"a", now, deadline(now));
            assert_eq!(start, None, "no call while it backs off");
            let Answer::Refused {
                reason: Reason::BackingOff,
                retry_after: Some(after),
                ..
            } = answer
            else {
                panic!("{answer:?}");
            };
            waited.push(after);
            now += after;
            // The tick comes at the back-off's end: the failure is still
            // remembered, its next draw a probe, its step doubled.
            core.sweep(now);
            assert_eq!(core.slots(), 1, "the failure is remembered");
        }
        // The jitter drawn at its least: half of each step.
        let halves: Vec<_> = [1u32, 2, 4, 8, 16, 30, 30]
            .iter()
            .map(|s| S * *s / 2)
            .collect();
        assert_eq!(waited, halves);
    }

    #[test]
    fn the_first_draw_after_a_back_off_waits_when_the_stage_has_landed_in_time_since() {
        let (mut core, t0) = (core(), Instant::now());
        let (_, start) = core.seal(&"a", t0, deadline(t0));
        core.landed(&"a", start.unwrap().generation, Outcome::Unavailable, t0, 0);
        let back = t0 + S;
        // No landing since: the probe runs on its own, the seal refused.
        let (answer, start) = core.seal(&"a", back, deadline(back));
        assert_eq!(start.unwrap().kind, Kind::Probe);
        assert!(matches!(
            answer,
            Answer::Refused {
                reason: Reason::Late,
                ..
            }
        ));
        core.landed(
            &"a",
            start.unwrap().generation,
            Outcome::Unavailable,
            back,
            0,
        );
        // Another tenant lands in time: Vault answers.
        let at = back + Duration::from_millis(100);
        drawn(&mut core, "b", at, 9);
        let after = back + 2 * S;
        let (answer, start) = core.seal(&"a", after, deadline(after));
        let start = start.unwrap();
        assert_eq!(start.kind, Kind::Probe);
        assert_eq!(
            answer,
            Answer::Join(start.generation, deadline(after)),
            "it waits"
        );
    }

    /// A slot whose draws keep failing while other Tenants' land keeps its
    /// failure through a trusted probe: the probe is waited on, its failure
    /// counted on top, the back-off doubled (seedwork ADR-0042 § Owed 2, the
    /// partial hang). Red when the trusted probe sets the slot trusted.
    #[test]
    fn a_trusted_probe_that_fails_doubles_the_slot_s_back_off() {
        let (mut core, t0) = (core(), Instant::now());
        let (_, start) = core.seal(&"a", t0, deadline(t0));
        core.landed(&"a", start.unwrap().generation, Outcome::Unavailable, t0, 0);
        assert_eq!(core.failures(&"a"), 1);
        // another Tenant lands in time after the failure: the probe is trusted
        drawn(&mut core, "b", t0 + Duration::from_millis(100), 9);
        let back = t0 + S;
        let (answer, start) = core.seal(&"a", back, deadline(back));
        let start = start.unwrap();
        assert_eq!(start.kind, Kind::Probe);
        assert_eq!(answer, Answer::Join(start.generation, deadline(back)));
        assert_eq!(
            core.failures(&"a"),
            1,
            "the trusted probe keeps the failure"
        );
        core.gave_up(&"a", start.generation, deadline(back));
        core.left(&"a", deadline(back));
        let failed = back + 5 * S;
        core.landed(&"a", start.generation, Outcome::Unavailable, failed, 0);
        assert_eq!(core.failures(&"a"), 2, "and counts its own");
        // the step doubled, 2 s: half of it at the least jitter
        let early = failed + Duration::from_millis(750);
        assert!(matches!(
            core.seal(&"a", early, deadline(early)).0,
            Answer::Refused {
                reason: Reason::BackingOff,
                ..
            }
        ));
    }

    /// A slot's draw that lands past the wait is no evidence that its draws
    /// land in time: the key it brought serves, and once it is spent the
    /// slot's miss is refused at once, its draw run on, not waited on. Red
    /// when any landing trusts the slot.
    #[test]
    fn a_slot_whose_draw_lands_past_the_wait_stays_late() {
        let (mut core, t0) = (core(), Instant::now());
        let (_, start) = core.seal(&"a", t0, deadline(t0));
        let start = start.unwrap();
        core.gave_up(&"a", start.generation, deadline(t0));
        core.left(&"a", deadline(t0));
        core.landed(
            &"a",
            start.generation,
            Outcome::Drawn {
                key: 1,
                started: t0,
            },
            t0 + S,
            0,
        );
        let at = t0 + 2 * S;
        let mut answer = core.seal(&"a", at, deadline(at)).0;
        assert_eq!(answer, Answer::Hit(1), "the late key serves");
        // another Tenant's draw lands in time: the refusal is the slot's own
        drawn(&mut core, "b", at, 9);
        while answer == Answer::Hit(1) {
            answer = core.seal(&"a", at, deadline(at)).0;
        }
        assert!(
            matches!(
                answer,
                Answer::Refused {
                    reason: Reason::Late,
                    scope: Scope::Tenant,
                    ..
                }
            ),
            "{answer:?}"
        );
    }

    /// The joiners of a trusted probe wait on it, as on any trusted flight:
    /// a command sealing beside it right after an outage's return is not
    /// refused. Red when a join reads the slot's evidence instead of the
    /// flight's trust.
    #[test]
    fn a_trusted_probe_is_joined_at_the_sealing_end() {
        let (mut core, t0) = (core(), Instant::now());
        let (_, start) = core.seal(&"a", t0, deadline(t0));
        core.landed(&"a", start.unwrap().generation, Outcome::Unavailable, t0, 0);
        drawn(&mut core, "b", t0 + Duration::from_millis(100), 9);
        let back = t0 + S;
        let (first, start) = core.seal(&"a", back, deadline(back));
        assert!(matches!(first, Answer::Join(..)) && start.is_some());
        let (second, again) = core.seal(&"a", back, deadline(back));
        assert!(matches!(second, Answer::Join(..)), "{second:?}");
        assert_eq!(again, None, "one flight");
    }

    #[test]
    fn a_landing_slower_than_the_wait_is_no_evidence_that_the_kms_answers() {
        let (mut core, t0) = (core(), Instant::now());
        let (_, a) = core.seal(&"a", t0, deadline(t0));
        let (_, b) = core.seal(&"b", t0, deadline(t0));
        core.landed(&"a", a.unwrap().generation, Outcome::Unavailable, t0, 0);
        // Another tenant's draw lands, but slower than a seal waits.
        core.landed(
            &"b",
            b.unwrap().generation,
            Outcome::Drawn {
                key: 9,
                started: t0,
            },
            t0 + S,
            0,
        );
        let after = t0 + 2 * S;
        let (answer, start) = core.seal(&"a", after, deadline(after));
        assert_eq!(start.unwrap().kind, Kind::Probe);
        assert_eq!(
            answer,
            Answer::Refused {
                reason: Reason::Late,
                retry_after: None,
                scope: Scope::Stage
            },
            "the probe runs on its own; the seal does not wait on it: a landing slower than the \
             wait is no evidence that the KMS serves another tenant"
        );
    }

    fn scope_of(answer: &Answer<u32>) -> Scope {
        match answer {
            Answer::Refused { scope, .. } => *scope,
            other => panic!("{other:?}"),
        }
    }

    /// A refusal is the tenant's when another tenant's draw landed in time
    /// since this tenant's trouble began, and the stage's otherwise; a full
    /// stage refuses over the stage (ADR-0040 decision 3).
    #[test]
    fn a_refusal_is_the_tenant_s_when_another_landed_in_time_since_its_trouble_began() {
        let (mut core, t0) = (core(), Instant::now());
        let (_, a) = core.seal(&"a", t0, deadline(t0));
        let (_, b) = core.seal(&"b", t0, deadline(t0));
        core.left(&"a", t0);
        core.left(&"b", t0);
        core.landed(&"a", a.unwrap().generation, Outcome::Unavailable, t0, 0);
        let before = t0 + Duration::from_millis(50);
        let (refused, _) = core.seal(&"a", before, deadline(before));
        assert_eq!(scope_of(&refused), Scope::Stage, "no landing since");
        core.landed(
            &"b",
            b.unwrap().generation,
            Outcome::Drawn {
                key: 2,
                started: t0,
            },
            t0 + Duration::from_millis(100),
            0,
        );
        let after = t0 + Duration::from_millis(200);
        let (refused, _) = core.seal(&"a", after, deadline(after));
        assert!(matches!(
            refused,
            Answer::Refused {
                reason: Reason::BackingOff,
                ..
            }
        ));
        assert_eq!(scope_of(&refused), Scope::Tenant, "b landed in time since");
        assert_eq!(core.scope(&"a", Reason::BackingOff, after), Scope::Tenant);
        assert_eq!(core.scope(&"a", Reason::FlightsFull, after), Scope::Stage);
        assert_eq!(core.scope(&"a", Reason::Unspawned, after), Scope::Stage);

        // A landing before the tenant's trouble began is no evidence of it.
        let mut core = self::core();
        let (_, b) = core.seal(&"b", t0, deadline(t0));
        core.left(&"b", t0);
        core.landed(
            &"b",
            b.unwrap().generation,
            Outcome::Drawn {
                key: 2,
                started: t0,
            },
            t0 + Duration::from_millis(100),
            0,
        );
        let later = t0 + S;
        let (_, a) = core.seal(&"a", later, deadline(later));
        core.left(&"a", later);
        core.landed(&"a", a.unwrap().generation, Outcome::Unavailable, later, 0);
        let asked = later + Duration::from_millis(50);
        let (refused, _) = core.seal(&"a", asked, deadline(asked));
        assert_eq!(
            scope_of(&refused),
            Scope::Stage,
            "b landed before a's trouble began"
        );

        let mut p = parameters();
        p.flights = 1;
        let mut core = core_with(p);
        core.seal(&"a", t0, deadline(t0));
        let (full, _) = core.seal(&"b", t0, deadline(t0));
        assert!(matches!(
            full,
            Answer::Refused {
                reason: Reason::FlightsFull,
                scope: Scope::Stage,
                ..
            }
        ));
    }

    /// The sentinel is drawn for evidence at most one at a time, never
    /// within `wait` of the last draw, never while its own failure backs
    /// off; a core without one draws none.
    #[test]
    fn a_sentinel_is_drawn_for_evidence_at_most_once_per_wait() {
        let t0 = Instant::now();
        assert_eq!(core().probe(t0), None, "no sentinel, no probe");
        let mut core = core().with_sentinel("s");
        let (sentinel, start) = core.probe(t0).expect("a probe");
        assert_eq!((sentinel, start.kind), ("s", Kind::Probe));
        assert_eq!(core.probe(t0), None, "one in flight");
        let landed = t0 + Duration::from_millis(100);
        core.landed(
            &"s",
            start.generation,
            Outcome::Drawn {
                key: 1,
                started: t0,
            },
            landed,
            0,
        );
        assert_eq!(
            core.probe(t0 + Duration::from_millis(200)),
            None,
            "within the wait of the last"
        );
        let again = t0 + Duration::from_millis(300);
        let (_, start) = core.probe(again).expect("past the wait");
        core.landed(&"s", start.generation, Outcome::Unavailable, again, 0);
        let later = again + Duration::from_millis(300);
        assert_eq!(core.probe(later), None, "the sentinel's failure backs off");
        assert!(core.probe(again + 30 * S).is_some(), "and is drawn past it");
    }

    #[test]
    fn a_renewal_takes_a_place_only_while_half_are_free() {
        let mut p = parameters();
        p.flights = 4;
        let mut core = core_with(p);
        let t0 = Instant::now();
        drawn(&mut core, "a", t0, 1);
        for tenant in ["b", "c", "d"] {
            core.seal(&tenant, t0, deadline(t0));
        }
        // Three of four places held: a renewal due waits for its place.
        let at = t0 + 55 * S;
        assert_eq!(core.seal(&"a", at, deadline(at)), (Answer::Hit(1), None));
        // With two held, half are free: it is drawn.
        let mut core = core_with(p);
        drawn(&mut core, "a", t0, 1);
        for tenant in ["b", "c"] {
            core.seal(&tenant, t0, deadline(t0));
        }
        let (hit, start) = core.seal(&"a", at, deadline(at));
        assert_eq!(hit, Answer::Hit(1));
        assert_eq!(start.unwrap().kind, Kind::Renew);
    }

    #[test]
    fn a_shredded_tenant_s_key_retires_at_once_and_it_is_not_asked_again_for_a_while() {
        let (mut core, t0) = (core(), Instant::now());
        let (_, start) = core.seal(&"a", t0, deadline(t0));
        let generation = start.unwrap().generation;
        core.landed(&"a", generation, Outcome::Gone, t0, 0);
        assert_eq!(
            core.seal(&"a", t0, deadline(t0)),
            (Answer::Gone, None),
            "a seal after the landing is told"
        );
        assert_eq!(
            core.seal(&"a", t0 + S, deadline(t0 + S)),
            (Answer::Gone, None),
            "and so is every other within gone, none of them drawing"
        );
        let past = t0 + 10 * S;
        let (_, start) = core.seal(&"a", past, deadline(past));
        assert!(start.is_some(), "asked again past gone");
        // A key in service retires at once on Gone.
        let mut core = self::core();
        drawn(&mut core, "a", t0, 1);
        let at = t0 + 55 * S;
        let (hit, renewal) = core.seal(&"a", at, deadline(at));
        assert_eq!(hit, Answer::Hit(1));
        let renewal = renewal.expect("its successor drawn ahead");
        assert_eq!(renewal.kind, Kind::Renew);
        core.landed(&"a", renewal.generation, Outcome::Gone, at, 0);
        assert_eq!(core.keys_held(), 0, "the key retires with its tenant's");
    }

    #[test]
    fn a_key_serves_from_its_draw_s_start_for_its_lifetime() {
        let (mut core, t0) = (core(), Instant::now());
        let (_, start) = core.seal(&"a", t0, deadline(t0));
        // The draw took four seconds; the key's life counts from its start.
        core.landed(
            &"a",
            start.unwrap().generation,
            Outcome::Drawn {
                key: 1,
                started: t0,
            },
            t0 + 4 * S,
            0,
        );
        let just = t0 + 60 * S - Duration::from_millis(1);
        assert_eq!(core.seal(&"a", just, deadline(just)).0, Answer::Hit(1));
        let end = t0 + 60 * S;
        assert!(
            !matches!(core.seal(&"a", end, deadline(end)).0, Answer::Hit(_)),
            "never past its draw plus its lifetime"
        );
    }

    #[test]
    fn a_key_leaves_memory_at_its_end_on_an_idle_stage() {
        let (mut core, t0) = (core(), Instant::now());
        drawn(&mut core, "a", t0, 1);
        assert_eq!(core.next_deadline(), Some(t0 + 60 * S));
        core.sweep(t0 + 59 * S);
        assert!(core.take_retired().is_empty());
        core.sweep(t0 + 60 * S);
        assert_eq!(core.take_retired(), vec![1]);
        assert_eq!(core.keys_held(), 0);
        assert_eq!(core.slots(), 0, "the idle slot goes too");
    }

    /// A renewal that failed, its back-off and its memory over before the
    /// key in service ends: the key still leaves memory at its end on a
    /// quiet stage. Its end was hidden behind the memory's, already past.
    #[test]
    fn a_key_whose_renewal_failed_still_leaves_memory_at_its_end() {
        let mut p = parameters();
        (p.backoff, p.gone) = ((S, 2 * S), 2 * S);
        let (mut core, t0) = (core_with(p), Instant::now());
        drawn(&mut core, "a", t0, 1);
        let at = t0 + 55 * S;
        let (hit, renewal) = core.seal(&"a", at, deadline(at));
        assert_eq!(hit, Answer::Hit(1));
        let renewal = renewal.expect("its successor drawn ahead");
        core.landed(&"a", renewal.generation, Outcome::Unavailable, at, 0);
        // the failure is forgotten at 57.5 s, the key ends at 60 s
        core.sweep(t0 + 59 * S);
        assert!(core.take_retired().is_empty());
        assert_eq!(
            core.next_deadline(),
            Some(t0 + 60 * S),
            "the key's end is still due"
        );
        core.sweep(t0 + 60 * S);
        assert_eq!(core.take_retired(), vec![1]);
        assert_eq!(core.keys_held(), 0);
    }

    #[test]
    fn a_key_landing_past_its_end_leaves_no_slot_behind() {
        let (mut core, t0) = (core(), Instant::now());
        let (_, start) = core.seal(&"a", t0, deadline(t0));
        core.left(&"a", t0);
        core.landed(
            &"a",
            start.unwrap().generation,
            Outcome::Drawn {
                key: 1,
                started: t0,
            },
            t0 + 61 * S,
            0,
        );
        assert_eq!(core.keys_held(), 0, "an ended key is never served");
        assert_eq!(core.slots(), 0, "and no slot is kept that no sweep reaches");
    }

    #[test]
    fn a_note_goes_with_its_slot() {
        let mut core: Core<&'static str, u32, &'static str> =
            Core::new(Supply::new(parameters()).unwrap());
        let t0 = Instant::now();
        let (_, start) = core.seal(&"a", t0, deadline(t0));
        core.left(&"a", t0);
        core.landed(&"a", start.unwrap().generation, Outcome::Moment, t0, 0);
        core.note(&"a", Some("down"));
        assert_eq!(core.noted(&"a"), Some(&"down"));
        core.sweep(t0 + 60 * S);
        assert_eq!((core.slots(), core.noted(&"a")), (0, None));
        core.note(&"a", Some("unheld"));
        assert_eq!(
            core.noted(&"a"),
            None,
            "a tenant without a slot is noted nothing"
        );
    }

    #[test]
    fn a_failed_slot_is_kept_a_cap_past_its_back_off_and_then_goes() {
        let (mut core, t0) = (core(), Instant::now());
        let (_, start) = core.seal(&"a", t0, deadline(t0));
        core.landed(&"a", start.unwrap().generation, Outcome::Gone, t0, 0);
        core.left(&"a", t0);
        assert_eq!(core.next_deadline(), Some(t0 + 40 * S), "gone, then a cap");
        core.sweep(t0 + 10 * S);
        assert_eq!(core.slots(), 1, "its back-off over, its failure decides");
        core.sweep(t0 + 40 * S);
        assert_eq!(core.slots(), 0, "and then it goes");
        assert_eq!(core.next_deadline(), None);
    }

    #[test]
    fn a_landing_reaches_its_own_flight_and_a_stale_one_changes_nothing() {
        let (mut core, t0) = (core(), Instant::now());
        let (_, a) = core.seal(&"a", t0, deadline(t0));
        let (_, b) = core.seal(&"b", t0, deadline(t0));
        let (a, b) = (a.unwrap().generation, b.unwrap().generation);
        // Addressed to another tenant: nothing reaches b.
        core.landed(
            &"b",
            a,
            Outcome::Drawn {
                key: 1,
                started: t0,
            },
            t0,
            0,
        );
        assert!(matches!(core.seal(&"b", t0, deadline(t0)).0, Answer::Join(g, _) if g == b));
        core.landed(
            &"b",
            b,
            Outcome::Drawn {
                key: 2,
                started: t0,
            },
            t0,
            0,
        );
        // Again: a duplicate changes nothing.
        core.landed(
            &"b",
            b,
            Outcome::Drawn {
                key: 3,
                started: t0,
            },
            t0,
            0,
        );
        assert_eq!(core.seal(&"b", t0, deadline(t0)).0, Answer::Hit(2));
        assert_eq!(core.flights(), 0, "every place given back once");
    }

    #[test]
    fn hanging_probes_and_renewals_never_take_every_place() {
        let mut p = parameters();
        p.flights = 4;
        let (mut core, t0) = (core_with(p), Instant::now());
        // Two tenants fail, and their probes hang.
        for tenant in ["e", "f"] {
            let (_, start) = core.seal(&tenant, t0, deadline(t0));
            core.landed(
                &tenant,
                start.unwrap().generation,
                Outcome::Unavailable,
                t0,
                0,
            );
        }
        let back = t0 + 2 * S;
        assert_eq!(
            core.seal(&"e", back, deadline(back)).1.unwrap().kind,
            Kind::Probe
        );
        assert!(
            matches!(
                core.seal(&"f", back, deadline(back)).0,
                Answer::Refused {
                    reason: Reason::FlightsFull,
                    ..
                }
            ),
            "a quarter of the places for probes"
        );
        // A healthy tenant still draws.
        let (answer, start) = core.seal(&"h", back, deadline(back));
        assert_eq!(start.unwrap().kind, Kind::Miss);
        assert!(matches!(answer, Answer::Join(..)));
    }

    #[test]
    fn invalid_parameters_are_refused() {
        type Change = Box<dyn Fn(&mut SupplyParameters)>;
        let cases: Vec<(&str, Change)> = vec![
            (
                "lifetime ≤ a century",
                Box::new(|p| p.lifetime = LONGEST + S),
            ),
            ("messages ≥ 2 · burst", Box::new(|p| p.burst = 51)),
            ("burst ≥ 1", Box::new(|p| p.burst = 0)),
            ("wait > 0", Box::new(|p| p.wait = Duration::ZERO)),
            ("wait < draw", Box::new(|p| p.wait = p.draw)),
            ("draw < lifetime / 2", Box::new(|p| p.draw = p.lifetime / 2)),
            ("the lead from ≥ draw", Box::new(|p| p.lead.0 = p.draw - S)),
            (
                "the lead to ≥ the lead from",
                Box::new(|p| p.lead.1 = p.lead.0 - S),
            ),
            (
                "jitter ≤ lifetime / 10",
                Box::new(|p| p.jitter = p.lifetime / 10 + S),
            ),
            (
                "jitter + the lead to + 2 · draw ≤ lifetime",
                Box::new(|p| p.lead.1 = 50 * S),
            ),
            (
                "the back-off's first step ≥ 2 · wait",
                Box::new(|p| p.backoff.0 = p.wait * 2 - Duration::from_nanos(1)),
            ),
            (
                "the back-off's cap ≥ its first step",
                Box::new(|p| p.backoff.1 = p.backoff.0 / 2),
            ),
            (
                "the back-off's cap ≤ a century",
                Box::new(|p| p.backoff.1 = LONGEST + S),
            ),
            ("gone > 0", Box::new(|p| p.gone = Duration::ZERO)),
            (
                "gone ≤ the back-off's cap",
                Box::new(|p| p.gone = p.backoff.1 + S),
            ),
            ("flights ≥ 1", Box::new(|p| p.flights = 0)),
        ];
        assert!(Supply::new(parameters()).is_ok());
        for (rule, change) in cases {
            let mut p = parameters();
            change(&mut p);
            assert_eq!(Supply::new(p), Err(InvalidParameters(rule)), "{rule}");
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_methods,
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    reason = "tests: a pure core's origin of time is any instant"
)]
mod property {
    use std::collections::{HashMap, HashSet};

    use proptest::prelude::*;
    use proptest::strategy::ValueTree as _;
    use proptest::test_runner::{Config, TestRunner};

    use super::*;

    const S: Duration = Duration::from_secs(1);
    const TENANTS: [&str; 3] = ["a", "b", "c"];

    /// A key that says when its draw started, so that a hit can be held to
    /// its end.
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Drawn {
        id: u32,
        started: Instant,
    }

    #[derive(Clone, Debug)]
    enum Step {
        Seal(usize),
        Advance(u64),
        Land(usize, u8, bool),
        GaveUp(usize),
        Left(usize),
        Sweep,
        /// A draw for the sentinel's evidence, if one is due.
        Probe,
    }

    fn step() -> impl Strategy<Value = Step> {
        prop_oneof![
            4 => (0..3usize).prop_map(Step::Seal),
            3 => (0..8_000u64).prop_map(Step::Advance),
            3 => (0..3usize, 0..7u8, any::<bool>()).prop_map(|(t, o, stale)| Step::Land(t, o, stale)),
            1 => (0..3usize).prop_map(Step::GaveUp),
            2 => (0..3usize).prop_map(Step::Left),
            1 => Just(Step::Sweep),
            1 => Just(Step::Probe),
        ]
    }

    fn supply() -> Supply {
        Supply::new(SupplyParameters {
            lifetime: 20 * S,
            messages: 6,
            burst: 2,
            wait: S / 2,
            draw: 3 * S,
            lead: (3 * S, 5 * S),
            jitter: 2 * S,
            backoff: (S, 8 * S),
            gone: 3 * S,
            flights: 2,
        })
        .unwrap()
    }

    /// What a history exercised.
    #[derive(Default, Debug)]
    struct Activity {
        hits: u32,
        joins: u32,
        refused: HashMap<&'static str, u32>,
        starts: HashMap<&'static str, u32>,
        landed: HashMap<&'static str, u32>,
        stale: u32,
        retired: u32,
        expired_by_uses: u32,
        tenant_scoped: u32,
        probes: u32,
        /// What the core answered and held after each step, its times as
        /// offsets from the history's start: the behaviour's fingerprint.
        trace: Vec<String>,
    }

    /// An answer as the fingerprint writes it: its instants as offsets from
    /// `t0`, so that two runs of one history write the same.
    fn described(answer: &Answer<Drawn>, t0: Instant) -> String {
        match answer {
            Answer::Hit(key) => format!("hit {} {:?}", key.id, key.started - t0),
            Answer::Join(generation, deadline) => {
                format!("join {generation} {:?}", *deadline - t0)
            }
            Answer::Refused {
                reason,
                retry_after,
                scope,
            } => format!("refused {reason:?} {retry_after:?} {scope:?}"),
            Answer::Gone => "gone".to_owned(),
        }
    }

    /// Runs `steps`, holding the invariants after each; what they exercised.
    fn run(steps: &[Step], random: u64) -> Result<Activity, TestCaseError> {
        let supply = supply();
        // `c` is the sentinel, and seals as any tenant does too.
        let mut core: Core<&'static str, Drawn> = Core::new(supply).with_sentinel("c");
        let t0 = Instant::now();
        let mut probed: Option<Instant> = None;
        let mut now = t0;
        let mut flying: HashMap<&'static str, (Generation, Instant)> = HashMap::new();
        let mut landed_generations: Vec<(&'static str, Generation)> = Vec::new();
        let mut uses: HashMap<u32, u32> = HashMap::new();
        let mut retired: HashSet<u32> = HashSet::new();
        let mut gone_at: HashMap<&'static str, Instant> = HashMap::new();
        let mut next_id = 0;
        let mut activity = Activity::default();
        for (n, step) in steps.iter().enumerate() {
            let random = random.rotate_left(u32::try_from(n % 64).unwrap());
            match *step {
                Step::Seal(t) => {
                    let tenant = TENANTS[t];
                    let (answer, start) = core.seal(&tenant, now, now + supply.wait());
                    activity.trace.push(format!(
                        "seal {tenant}: {} {start:?}",
                        described(&answer, t0)
                    ));
                    if let Some(start) = start {
                        prop_assert!(!flying.contains_key(tenant), "two flights for {tenant}");
                        flying.insert(tenant, (start.generation, now));
                        *activity
                            .starts
                            .entry(match start.kind {
                                Kind::Miss => "miss",
                                Kind::Renew => "renew",
                                Kind::Probe => "probe",
                            })
                            .or_default() += 1;
                    }
                    match answer {
                        Answer::Hit(key) => {
                            activity.hits += 1;
                            prop_assert!(!retired.contains(&key.id), "a retired key served");
                            prop_assert!(
                                now < key.started + supply.lifetime(),
                                "a key served past its draw plus its lifetime"
                            );
                            let used = uses.entry(key.id).or_default();
                            *used += 1;
                            prop_assert!(*used <= 6, "a key served past its uses");
                            if *used == 6 {
                                activity.expired_by_uses += 1;
                            }
                            if let Some(gone) = gone_at.get(tenant) {
                                prop_assert!(
                                    key.started >= *gone,
                                    "a key drawn before its tenant's was gone"
                                );
                            }
                        }
                        Answer::Join(..) => activity.joins += 1,
                        Answer::Refused {
                            reason,
                            retry_after,
                            scope,
                        } => {
                            if scope == Scope::Tenant {
                                activity.tenant_scoped += 1;
                            }
                            if let Some(after) = retry_after {
                                prop_assert!(!after.is_zero() && after <= 8 * S, "{after:?}");
                            }
                            *activity
                                .refused
                                .entry(match reason {
                                    Reason::Waited => "waited",
                                    Reason::BackingOff => "backing off",
                                    Reason::Late => "late",
                                    Reason::FlightsFull => "full",
                                    Reason::Unspawned => "unspawned",
                                })
                                .or_default() += 1;
                        }
                        Answer::Gone => {}
                    }
                }
                Step::Advance(ms) => now += Duration::from_millis(ms),
                Step::Land(t, outcome, stale) => {
                    let tenant = TENANTS[t];
                    if stale {
                        let Some(&(owner, generation)) = landed_generations.last() else {
                            continue;
                        };
                        let before = format!("{core:?}");
                        core.landed(
                            &owner,
                            generation,
                            Outcome::Drawn {
                                key: Drawn {
                                    id: u32::MAX,
                                    started: now,
                                },
                                started: now,
                            },
                            now,
                            random,
                        );
                        prop_assert_eq!(
                            before,
                            format!("{core:?}"),
                            "a stale landing changed the state"
                        );
                        activity.stale += 1;
                        continue;
                    }
                    let Some((generation, started)) = flying.remove(tenant) else {
                        continue;
                    };
                    let abandoned = now.saturating_duration_since(started) > supply.draw();
                    let (name, outcome) = if abandoned {
                        ("abandoned", Outcome::Abandoned)
                    } else {
                        match outcome {
                            0..=2 => {
                                next_id += 1;
                                (
                                    "drawn",
                                    Outcome::Drawn {
                                        key: Drawn {
                                            id: next_id,
                                            started,
                                        },
                                        started,
                                    },
                                )
                            }
                            3 => {
                                gone_at.insert(tenant, now);
                                ("gone", Outcome::Gone)
                            }
                            4 => ("refused", Outcome::Refused),
                            5 => ("unavailable", Outcome::Unavailable),
                            _ => ("canceled", Outcome::Canceled),
                        }
                    };
                    core.landed(&tenant, generation, outcome, now, random);
                    landed_generations.push((tenant, generation));
                    *activity.landed.entry(name).or_default() += 1;
                }
                Step::GaveUp(t) => {
                    let tenant = TENANTS[t];
                    if let Some(&(generation, _)) = flying.get(tenant) {
                        core.gave_up(&tenant, generation, now);
                    }
                }
                Step::Left(t) => core.left(&TENANTS[t], now),
                Step::Sweep => core.sweep(now),
                Step::Probe => {
                    let probe = core.probe(now);
                    activity.trace.push(format!("probe {probe:?}"));
                    if let Some((tenant, start)) = probe {
                        prop_assert_eq!(tenant, "c", "only the sentinel is probed");
                        prop_assert_eq!(start.kind, Kind::Probe);
                        prop_assert!(!flying.contains_key(tenant), "two flights for {tenant}");
                        prop_assert!(
                            probed.is_none_or(|at| now >= at + supply.wait()),
                            "a probe within the wait of the last"
                        );
                        probed = Some(now);
                        flying.insert(tenant, (start.generation, now));
                        activity.probes += 1;
                    }
                }
            }
            let next = core.next_deadline().map(|at| at - t0);
            activity.trace.push(format!(
                "{n} {step:?} next={next:?} flights={} keys={} slots={}",
                core.flights(),
                core.keys_held(),
                core.slots()
            ));
            // Every key gone out of service is told, once, and never served
            // again.
            for key in core.take_retired() {
                activity.trace.push(format!("retired {}", key.id));
                prop_assert!(retired.insert(key.id), "a key retired twice");
                activity.retired += 1;
            }
            prop_assert!(core.flights() <= 2, "more flights than the cap");
            prop_assert_eq!(core.flights(), flying.len());
            // No slot is kept that no sweep will reach, and a slot has one
            // deadline at most: when it next changes by time.
            let memory = supply.backoff.1;
            for (tenant, slot) in &core.slots {
                prop_assert!(
                    !slot.idle(now, memory) || slot.due.is_some_and(|(at, _)| at <= now),
                    "the slot of {} holds nothing and no sweep is due to it",
                    tenant
                );
                // Read off the slot itself, not through `next`: every change
                // ahead — its key's end, its failure's forgetting — is
                // reached by a sweep no later than it, unless a landing or a
                // leaving will settle the slot first.
                if slot.flight.is_none() && slot.waiters == 0 {
                    let reached = |change: Instant| {
                        change <= now || slot.due.is_some_and(|(at, _)| at <= change)
                    };
                    if let Some(serving) = &slot.key {
                        prop_assert!(
                            reached(serving.ends),
                            "the end of {}'s key is reached by no sweep",
                            tenant
                        );
                    }
                    if let Evidence::Failed { until, .. } = slot.evidence {
                        prop_assert!(
                            reached(later(until, memory)),
                            "the forgetting of {}'s failure is reached by no sweep",
                            tenant
                        );
                    }
                }
                // A slot whose deadline is past is owed to a sweep, which
                // settles it; any other is due when it next changes.
                if slot.flight.is_none()
                    && slot.waiters == 0
                    && slot.due.is_none_or(|(at, _)| at > now)
                {
                    prop_assert_eq!(
                        slot.due.map(|(at, _)| at),
                        slot.next(memory, now),
                        "the slot of {} is due when it does not change",
                        tenant
                    );
                }
                if let Some(due) = slot.due {
                    prop_assert_eq!(core.deadlines.get(&due), Some(tenant));
                }
                prop_assert!(
                    slot.due.is_some() || slot.flight.is_some() || slot.waiters > 0,
                    "the slot of {} is kept, and no sweep will reach it",
                    tenant
                );
            }
            prop_assert_eq!(
                core.deadlines.len(),
                core.slots
                    .values()
                    .filter(|slot| slot.due.is_some())
                    .count(),
                "a deadline of no slot"
            );
        }
        Ok(activity)
    }

    proptest! {
        #![proptest_config(Config::with_cases(512))]

        #[test]
        fn no_history_breaks_the_core_s_invariants(
            steps in proptest::collection::vec(step(), 1..120),
            random in any::<u64>(),
        ) {
            run(&steps, random)?;
        }
    }

    /// The histories exercise every path the invariants speak of, on a
    /// runner whose seed is fixed.
    /// The fingerprint of 300 histories, FNV-1a over their traces.
    fn fingerprint() -> u64 {
        let mut runner = TestRunner::deterministic();
        let strategy = (proptest::collection::vec(step(), 40..120), any::<u64>());
        let mut digest: u64 = 0xcbf2_9ce4_8422_2325;
        for _ in 0..300 {
            let (steps, random) = strategy.new_tree(&mut runner).unwrap().current();
            for line in run(&steps, random).unwrap().trace {
                for byte in line.bytes().chain(std::iter::once(b'\n')) {
                    digest ^= u64::from(byte);
                    digest = digest.wrapping_mul(0x0000_0100_0000_01b3);
                }
            }
        }
        digest
    }

    /// The core's behaviour over 300 histories, pinned: what it answers,
    /// starts, retires and holds after each step. A change that is to keep
    /// the behaviour — moving the pacing and the flights into modules the
    /// opening end shares (ADR-0039 slice 4, S4b) — keeps the digest; one
    /// that changes the behaviour pins the new digest with its reason.
    ///
    /// Re-pinned for seedwork ADR-0043: a trusted probe keeps the slot's
    /// failure; a flight carries its trust for its joiners; a draw landing
    /// past the wait leaves the slot late; a refusal's scope reads the
    /// stage's landings over the last wait. The changes are held by their
    /// own tests, not by this digest, which accepts whatever it pins.
    #[test]
    fn the_core_s_behaviour_is_pinned() {
        assert_eq!(fingerprint(), PINNED);
    }

    const PINNED: u64 = 8_812_330_262_278_217_155;

    #[test]
    fn the_histories_exercise_every_path() {
        let mut runner = TestRunner::deterministic();
        let strategy = (proptest::collection::vec(step(), 40..120), any::<u64>());
        let mut total = Activity::default();
        for _ in 0..300 {
            let (steps, random) = strategy.new_tree(&mut runner).unwrap().current();
            let activity = run(&steps, random).unwrap();
            total.hits += activity.hits;
            total.joins += activity.joins;
            total.stale += activity.stale;
            total.retired += activity.retired;
            total.expired_by_uses += activity.expired_by_uses;
            total.tenant_scoped += activity.tenant_scoped;
            total.probes += activity.probes;
            for (k, v) in activity.refused {
                *total.refused.entry(k).or_default() += v;
            }
            for (k, v) in activity.starts {
                *total.starts.entry(k).or_default() += v;
            }
            for (k, v) in activity.landed {
                *total.landed.entry(k).or_default() += v;
            }
        }
        assert!(
            total.tenant_scoped > 0,
            "never a refusal of the tenant's scope: {total:?}"
        );
        assert!(total.probes > 0, "never a sentinel's probe: {total:?}");
        let refused: HashSet<_> = total.refused.keys().copied().collect();
        for reason in ["backing off", "late", "full"] {
            assert!(
                refused.contains(reason),
                "never refused {reason}: {total:?}"
            );
        }
        for kind in ["miss", "renew", "probe"] {
            assert!(
                total.starts.get(kind).copied().unwrap_or(0) > 0,
                "never started a {kind}: {total:?}"
            );
        }
        for outcome in [
            "drawn",
            "gone",
            "refused",
            "unavailable",
            "canceled",
            "abandoned",
        ] {
            assert!(
                total.landed.get(outcome).copied().unwrap_or(0) > 0,
                "never landed {outcome}: {total:?}"
            );
        }
        assert!(
            total.hits > 0
                && total.joins > 0
                && total.stale > 0
                && total.retired > 0
                && total.expired_by_uses > 0,
            "{total:?}"
        );
    }
}
