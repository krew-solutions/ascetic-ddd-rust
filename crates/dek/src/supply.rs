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

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::hash::Hash;
use std::time::{Duration, Instant};

/// The number of a flight, one counter per stage, never reused.
pub type Generation = u64;

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
        rule(p.burst >= 1, "burst ≥ 1")?;
        rule(
            u64::from(p.messages) >= 2 * u64::from(p.burst),
            "messages ≥ 2 · burst",
        )?;
        rule(!p.wait.is_zero(), "wait > 0")?;
        rule(p.wait < p.draw, "wait < draw")?;
        rule(p.draw < p.lifetime / 2, "draw < lifetime / 2")?;
        rule(p.lead.0 >= p.draw, "the lead from ≥ draw")?;
        rule(p.lead.1 >= p.lead.0, "the lead to ≥ the lead from")?;
        rule(p.jitter <= p.lifetime / 10, "jitter ≤ lifetime / 10")?;
        rule(
            p.jitter
                .saturating_add(p.lead.1)
                .saturating_add(p.draw.saturating_mul(2))
                <= p.lifetime,
            "jitter + the lead to + 2 · draw ≤ lifetime",
        )?;
        rule(!p.backoff.0.is_zero(), "the back-off's first step > 0")?;
        rule(
            p.backoff.1 >= p.backoff.0,
            "the back-off's cap ≥ its first step",
        )?;
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

/// A fraction in [0, 1) of a random word.
fn fraction(random: u64) -> f64 {
    #[allow(clippy::cast_precision_loss, reason = "53 bits, exactly")]
    let fraction = (random >> 11) as f64 / (1u64 << 53) as f64;
    fraction
}

/// `span` scaled by a random fraction.
fn part(span: Duration, random: u64) -> Duration {
    span.mul_f64(fraction(random))
}

/// What a flight is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// A seal found no key.
    Miss,
    /// The key in service nears its end.
    Renew,
    /// The first draw of a slot whose back-off ended.
    Probe,
}

/// What became of a flight.
#[derive(Debug)]
pub enum Outcome<K> {
    /// A key, drawn by a draw started at `started`.
    Drawn {
        /// The key.
        key: K,
        /// When its draw started: its life counts from here.
        started: Instant,
    },
    /// The tenant's key is not there.
    Gone,
    /// The KMS refused, for a reason a deploy mends.
    Refused,
    /// The KMS could not answer now.
    Unavailable,
    /// Something the stage cannot attribute.
    Moment,
    /// The draw passed its bound and was given up.
    Abandoned,
    /// The draw's task ended without an answer: a panic, a shutdown.
    Canceled,
}

/// Why a seal was not served.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    /// It waited its bound for a draw that had not landed.
    Waited,
    /// The slot's last draw failed; it backs off.
    BackingOff,
    /// A draw is in flight that has not landed in time before.
    Late,
    /// The stage runs as many flights as it may.
    FlightsFull,
    /// No runtime would run the draw.
    Unspawned,
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
    },
    /// The tenant's key is not there: answered to the flight's own caller.
    Gone,
}

/// A flight the shell is to start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Start {
    /// Its number.
    pub generation: Generation,
    /// What it is for.
    pub kind: Kind,
}

#[derive(Debug)]
struct Serving<K> {
    key: K,
    ends: Instant,
    renew_at: Instant,
    used: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Failure {
    Gone,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Evidence {
    Trusted,
    Late,
    Failed {
        failure: Failure,
        until: Instant,
        failures: u32,
        at: Instant,
    },
}

#[derive(Debug)]
struct Slot<K> {
    key: Option<Serving<K>>,
    flight: Option<(Generation, Kind)>,
    evidence: Evidence,
    waiters: u32,
    /// The caller whose seal started the current flight joined it: told
    /// Gone when the flight lands so.
    starter: Option<Generation>,
}

impl<K> Slot<K> {
    const fn new() -> Self {
        Slot {
            key: None,
            flight: None,
            evidence: Evidence::Trusted,
            waiters: 0,
            starter: None,
        }
    }
}

/// The places of a stage's flights, by kind.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Held {
    misses: u32,
    renewals: u32,
    probes: u32,
}

impl Held {
    const fn total(self) -> u32 {
        self.misses + self.renewals + self.probes
    }
}

/// The core of one stage's keys.
#[derive(Debug)]
pub struct Core<T, K> {
    supply: Supply,
    slots: HashMap<T, Slot<K>>,
    in_flight: HashMap<Generation, (T, Kind)>,
    next: Generation,
    held: Held,
    deadlines: BinaryHeap<Reverse<(Instant, Generation)>>,
    deadline_of: HashMap<Generation, T>,
    landed_in_time: Option<Instant>,
}

impl<T: Clone + Eq + Hash, K: Clone> Core<T, K> {
    /// A core with no slot.
    #[must_use]
    pub fn new(supply: Supply) -> Self {
        Core {
            supply,
            slots: HashMap::new(),
            in_flight: HashMap::new(),
            next: 1,
            held: Held::default(),
            deadlines: BinaryHeap::new(),
            deadline_of: HashMap::new(),
            landed_in_time: None,
        }
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

    fn deadline(&mut self, at: Instant, tenant: &T) {
        let key = self.generation();
        self.deadlines.push(Reverse((at, key)));
        self.deadline_of.insert(key, tenant.clone());
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

    fn start(&mut self, tenant: &T, kind: Kind) -> Start {
        let generation = self.generation();
        match kind {
            Kind::Miss => self.held.misses += 1,
            Kind::Renew => self.held.renewals += 1,
            Kind::Probe => self.held.probes += 1,
        }
        self.in_flight.insert(generation, (tenant.clone(), kind));
        if let Some(slot) = self.slots.get_mut(tenant) {
            slot.flight = Some((generation, kind));
        }
        Start { generation, kind }
    }

    /// What a seal for `tenant` at `now` is answered, its wait ending at
    /// `deadline`, and the flight to start, if any.
    pub fn seal(
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
                serving.used += 1;
                let key = serving.key.clone();
                let due = now >= serving.renew_at
                    || supply.messages - serving.used <= supply.renew_uses();
                let quiet = slot.flight.is_none()
                    && !matches!(slot.evidence, Evidence::Failed { until, .. } if now < until);
                let start = (due && quiet && self.place(Kind::Renew))
                    .then(|| self.start(tenant, Kind::Renew));
                return (Answer::Hit(key), start);
            }
            // Ended, or its uses spent: it serves no more.
            slot.key = None;
        }
        let slot = self.slots.get_mut(tenant).expect("the slot");
        // A failed draw's back-off.
        if let Evidence::Failed { until, .. } = slot.evidence
            && now < until
        {
            return (
                Answer::Refused {
                    reason: Reason::BackingOff,
                    retry_after: Some(until - now),
                },
                None,
            );
        }
        // A flight in flight: joined only when trusted.
        if let Some((generation, _)) = slot.flight {
            return if slot.evidence == Evidence::Trusted {
                slot.waiters += 1;
                (Answer::Join(generation, deadline), None)
            } else {
                (
                    Answer::Refused {
                        reason: Reason::Late,
                        retry_after: None,
                    },
                    None,
                )
            };
        }
        // No flight: start one, and wait for it only on evidence.
        let (kind, trusted) = match slot.evidence {
            Evidence::Trusted => (Kind::Miss, true),
            Evidence::Late => (Kind::Miss, false),
            Evidence::Failed { at, .. } => (
                Kind::Probe,
                self.landed_in_time.is_some_and(|landed| landed > at),
            ),
        };
        if !self.place(kind) {
            return (
                Answer::Refused {
                    reason: Reason::FlightsFull,
                    retry_after: None,
                },
                None,
            );
        }
        let start = self.start(tenant, kind);
        let slot = self.slots.get_mut(tenant).expect("the slot");
        if trusted {
            slot.evidence = Evidence::Trusted;
            slot.waiters += 1;
            slot.starter = Some(start.generation);
            (Answer::Join(start.generation, deadline), Some(start))
        } else {
            (
                Answer::Refused {
                    reason: Reason::Late,
                    retry_after: None,
                },
                Some(start),
            )
        }
    }

    /// A waiter of `tenant` stops waiting: woken, or at its deadline.
    pub fn left(&mut self, tenant: &T) {
        if let Some(slot) = self.slots.get_mut(tenant) {
            slot.waiters = slot.waiters.saturating_sub(1);
        }
    }

    /// A waiter of flight `generation` gave up at its deadline: the slot
    /// is late, unless that flight is no longer in flight.
    pub fn gave_up(&mut self, tenant: &T, generation: Generation) {
        if let Some(slot) = self.slots.get_mut(tenant)
            && slot.flight.is_some_and(|(flying, _)| flying == generation)
            && slot.evidence == Evidence::Trusted
        {
            slot.evidence = Evidence::Late;
        }
    }

    /// Whether flight `generation`'s starter is to be told the key is gone:
    /// asked once, by the starter, after the flight landed so.
    pub fn starter_told_gone(&mut self, tenant: &T, generation: Generation) -> bool {
        self.slots.get_mut(tenant).is_some_and(|slot| {
            let told = slot.starter == Some(generation)
                && matches!(
                    slot.evidence,
                    Evidence::Failed {
                        failure: Failure::Gone,
                        ..
                    }
                );
            if told {
                slot.starter = None;
            }
            told
        })
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
        match kind {
            Kind::Miss => self.held.misses -= 1,
            Kind::Renew => self.held.renewals -= 1,
            Kind::Probe => self.held.probes -= 1,
        }
        if &flying_for != tenant {
            return;
        }
        let supply = self.supply;
        let Some(slot) = self.slots.get_mut(tenant) else {
            return;
        };
        if slot.flight.is_some_and(|(flying, _)| flying == generation) {
            slot.flight = None;
        }
        let failures = match slot.evidence {
            Evidence::Failed { failures, .. } => failures,
            _ => 0,
        };
        let mut deadline = None;
        match outcome {
            Outcome::Drawn { key, started } => {
                let ends = (started + supply.lifetime).checked_sub(part(supply.jitter, random));
                let lead =
                    supply.lead.0 + part(supply.lead.1 - supply.lead.0, random.rotate_left(32));
                if let Some(ends) = ends
                    && now < ends
                {
                    slot.key = Some(Serving {
                        key,
                        ends,
                        renew_at: ends.checked_sub(lead).unwrap_or(started),
                        used: 0,
                    });
                    deadline = Some(ends);
                }
                slot.evidence = Evidence::Trusted;
                if now.saturating_duration_since(started) <= supply.wait {
                    self.landed_in_time = Some(now);
                }
            }
            Outcome::Gone => {
                slot.key = None;
                slot.evidence = Evidence::Failed {
                    failure: Failure::Gone,
                    until: now + supply.gone,
                    failures: failures.saturating_add(1),
                    at: now,
                };
                deadline = Some(now + supply.gone);
            }
            Outcome::Refused
            | Outcome::Unavailable
            | Outcome::Moment
            | Outcome::Abandoned
            | Outcome::Canceled => {
                let step = supply
                    .backoff
                    .0
                    .saturating_mul(1u32 << failures.min(16))
                    .min(supply.backoff.1);
                let backoff = step / 2 + part(step / 2, random);
                slot.evidence = Evidence::Failed {
                    failure: Failure::Other,
                    until: now + backoff,
                    failures: failures.saturating_add(1),
                    at: now,
                };
                deadline = Some(now + backoff);
            }
        }
        if let Some(at) = deadline {
            self.deadline(at, tenant);
        }
    }

    /// Flight `generation` could not be started: no runtime would run it.
    pub fn unspawned(&mut self, tenant: &T, generation: Generation, now: Instant, random: u64) {
        self.landed(tenant, generation, Outcome::Canceled, now, random);
    }

    /// Retires, as of `now`, every key past its end and evicts every slot
    /// that holds nothing with a future effect: the keys retired are
    /// returned, to be wiped as they drop.
    pub fn sweep(&mut self, now: Instant) -> Vec<K> {
        let mut retired = Vec::new();
        while let Some(Reverse((at, key))) = self.deadlines.peek().copied() {
            if at > now {
                break;
            }
            self.deadlines.pop();
            let Some(tenant) = self.deadline_of.remove(&key) else {
                continue;
            };
            let Some(slot) = self.slots.get_mut(&tenant) else {
                continue;
            };
            if slot.key.as_ref().is_some_and(|serving| serving.ends <= now)
                && let Some(serving) = slot.key.take()
            {
                retired.push(serving.key);
            }
            let idle = slot.key.is_none()
                && slot.flight.is_none()
                && slot.waiters == 0
                && match slot.evidence {
                    Evidence::Failed { until, .. } => until <= now,
                    Evidence::Trusted | Evidence::Late => true,
                };
            if idle {
                self.slots.remove(&tenant);
            }
        }
        retired
    }

    /// When the next key ends or the next back-off is over: when the shell
    /// is to sweep next.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Instant> {
        self.deadlines.peek().map(|Reverse((at, _))| *at)
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

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "tests"
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
        core.left(&tenant);
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
        core.gave_up(&"a", generation);
        core.left(&"a");
        let (answer, start) = core.seal(&"a", t0, deadline(t0));
        assert_eq!(
            answer,
            Answer::Refused {
                reason: Reason::Late,
                retry_after: None
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
    fn a_give_up_after_its_flight_landed_is_inert() {
        let (mut core, t0) = (core(), Instant::now());
        let generation = drawn(&mut core, "a", t0, 1);
        core.gave_up(&"a", generation);
        assert_eq!(core.seal(&"a", t0, deadline(t0)).0, Answer::Hit(1));
        // The key spent, a new miss waits: the slot is trusted.
        let mut p = parameters();
        (p.messages, p.burst) = (2, 1);
        let mut core = core_with(p);
        let generation = drawn(&mut core, "a", t0, 1);
        core.gave_up(&"a", generation);
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
            let (answer, start) = core.seal(&"a", now, deadline(now));
            assert_eq!(start, None, "no call while it backs off");
            let Answer::Refused {
                reason: Reason::BackingOff,
                retry_after: Some(after),
            } = answer
            else {
                panic!("{answer:?}");
            };
            waited.push(after);
            now += after;
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

    #[test]
    fn a_shredded_tenant_s_key_retires_at_once_and_it_is_not_asked_again_for_a_while() {
        let (mut core, t0) = (core(), Instant::now());
        let (_, start) = core.seal(&"a", t0, deadline(t0));
        let generation = start.unwrap().generation;
        core.landed(&"a", generation, Outcome::Gone, t0, 0);
        assert!(
            core.starter_told_gone(&"a", generation),
            "its own caller is told"
        );
        assert!(!core.starter_told_gone(&"a", generation), "once");
        let (answer, start) = core.seal(&"a", t0 + S, deadline(t0 + S));
        assert_eq!(start, None, "no call within gone");
        assert!(matches!(
            answer,
            Answer::Refused {
                reason: Reason::BackingOff,
                ..
            }
        ));
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
        assert!(core.sweep(t0 + 59 * S).is_empty());
        assert_eq!(core.sweep(t0 + 60 * S), vec![1]);
        assert_eq!(core.keys_held(), 0);
        assert_eq!(core.slots(), 0, "the idle slot goes too");
    }

    #[test]
    fn a_slot_backing_off_is_not_evicted_before_its_back_off_ends() {
        let (mut core, t0) = (core(), Instant::now());
        let (_, start) = core.seal(&"a", t0, deadline(t0));
        core.landed(&"a", start.unwrap().generation, Outcome::Gone, t0, 0);
        core.left(&"a");
        core.sweep(t0 + S);
        assert_eq!(core.slots(), 1, "its memory of gone stays");
        core.sweep(t0 + 10 * S);
        assert_eq!(core.slots(), 0);
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
                "the back-off's first step > 0",
                Box::new(|p| p.backoff.0 = Duration::ZERO),
            ),
            (
                "the back-off's cap ≥ its first step",
                Box::new(|p| p.backoff.1 = p.backoff.0 / 2),
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
    reason = "tests"
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
    }

    fn step() -> impl Strategy<Value = Step> {
        prop_oneof![
            4 => (0..3usize).prop_map(Step::Seal),
            3 => (0..8_000u64).prop_map(Step::Advance),
            3 => (0..3usize, 0..7u8, any::<bool>()).prop_map(|(t, o, stale)| Step::Land(t, o, stale)),
            1 => (0..3usize).prop_map(Step::GaveUp),
            2 => (0..3usize).prop_map(Step::Left),
            1 => Just(Step::Sweep),
        ]
    }

    fn supply() -> Supply {
        Supply::new(SupplyParameters {
            lifetime: 20 * S,
            messages: 6,
            burst: 2,
            wait: S,
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
    }

    /// Runs `steps`, holding the invariants after each; what they exercised.
    fn run(steps: &[Step], random: u64) -> Result<Activity, TestCaseError> {
        let supply = supply();
        let mut core: Core<&'static str, Drawn> = Core::new(supply);
        let t0 = Instant::now();
        let mut now = t0;
        let mut flying: HashMap<&'static str, (Generation, Instant)> = HashMap::new();
        let mut landed_generations: Vec<(&'static str, Generation)> = Vec::new();
        let mut uses: HashMap<u32, u32> = HashMap::new();
        let mut gone_at: HashMap<&'static str, Instant> = HashMap::new();
        let mut next_id = 0;
        let mut activity = Activity::default();
        for (n, step) in steps.iter().enumerate() {
            let random = random.rotate_left(u32::try_from(n % 64).unwrap());
            match *step {
                Step::Seal(t) => {
                    let tenant = TENANTS[t];
                    let (answer, start) = core.seal(&tenant, now, now + supply.wait());
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
                        } => {
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
                        core.gave_up(&tenant, generation);
                    }
                }
                Step::Left(t) => core.left(&TENANTS[t]),
                Step::Sweep => {
                    let retired = core.sweep(now);
                    activity.retired += u32::try_from(retired.len()).unwrap();
                    for key in retired {
                        prop_assert!(
                            key.started + supply.lifetime() - 2 * S <= now,
                            "retired before its end"
                        );
                    }
                }
            }
            prop_assert!(core.flights() <= 2, "more flights than the cap");
            prop_assert_eq!(core.flights(), flying.len());
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
