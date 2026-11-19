//! The pure core of a stage's opening end (ADR-0039, slice 4): which key a
//! message opens with, when an unwrap starts, how long an open waits, and
//! what a landing changes — as transitions over a state, with the time and
//! the randomness passed in. Nothing here reads a clock, awaits, spawns or
//! unwraps a key: the shell does, and asks this core what follows.
//!
//! An entry per (tenant, wrapped key) holds the key it unwrapped, from its
//! unwrap's start for `keep`, or the memory that the KMS refused it, at most
//! one flight, and its waiters. A tenant holds the evidence of whether an
//! unwrap of its keys lands in time, and the memory of a key gone. A key
//! gone answers every open of the tenant within its memory, before any key
//! held, and wipes the keys held; an unwrap that lands after it is wiped
//! too. The entries are capped: a new one evicts the idle entry that ends
//! first, and an eviction is counted.
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

use crate::pace::{
    Evidence, Failure, Flying, Generation, Held, Kind, LONGEST, Outcome, Reason, Scope, Start,
    failed, later, scope_by,
};
use crate::supply::InvalidParameters;

/// How a stage's opening end unwraps and keeps its keys (ADR-0039 slice 4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Opening {
    /// How long an unwrapped key serves, from its unwrap's start.
    pub keep: Duration,
    /// The entries held at most, keys and memories of refusals alike.
    pub keys: usize,
    /// How long an open waits for its unwrap to land.
    pub wait: Duration,
    /// How long an unwrap may take before it is given up.
    pub draw: Duration,
    /// A failed unwrap's back-off: its first step, and its cap.
    pub backoff: (Duration, Duration),
    /// How long a key gone, or a wrapped key the KMS refused, is
    /// remembered.
    pub gone: Duration,
    /// The unwraps a stage runs at once, at most.
    pub flights: u32,
}

impl Opening {
    /// An opening keeping each key `keep`, holding `keys` entries, waiting
    /// near an unwrap's bound: an open refused costs the inbox an attempt and
    /// a slot held for a while, and no unwrap ahead hides its latency, so
    /// the open waits as long as an unwrap usually takes, not a seal's
    /// quarter of a second.
    #[must_use]
    pub const fn fitted(keep: Duration, keys: usize) -> Self {
        Opening {
            keep,
            keys,
            wait: Duration::from_secs(2),
            draw: Duration::from_secs(5),
            backoff: (Duration::from_secs(4), Duration::from_secs(30)),
            gone: Duration::from_secs(10),
            flights: 16,
        }
    }

    /// The same, unwraps waited for `wait`.
    #[must_use]
    pub const fn with_wait(mut self, wait: Duration) -> Self {
        self.wait = wait;
        self
    }

    /// The same, an unwrap given up past `draw`.
    #[must_use]
    pub const fn with_draw(mut self, draw: Duration) -> Self {
        self.draw = draw;
        self
    }

    /// The same, a failed unwrap backing off from `first` to `cap`.
    #[must_use]
    pub const fn with_backoff(mut self, first: Duration, cap: Duration) -> Self {
        self.backoff = (first, cap);
        self
    }

    /// The same, a key gone or a refused wrapped key remembered `gone`.
    #[must_use]
    pub const fn with_gone(mut self, gone: Duration) -> Self {
        self.gone = gone;
        self
    }

    /// The same, at most `flights` unwraps at once.
    #[must_use]
    pub const fn with_flights(mut self, flights: u32) -> Self {
        self.flights = flights;
        self
    }

    /// The opening, its rules held.
    ///
    /// # Errors
    /// [`InvalidParameters`] naming the rule broken.
    pub fn validated(self) -> Result<Self, InvalidParameters> {
        let rule = |holds: bool, rule: &'static str| {
            if holds {
                Ok(())
            } else {
                Err(InvalidParameters(rule))
            }
        };
        rule(!self.keep.is_zero(), "keep > 0")?;
        rule(self.keep <= LONGEST, "keep ≤ a century")?;
        rule(self.keys >= 1, "keys ≥ 1")?;
        rule(!self.wait.is_zero(), "the opening's wait > 0")?;
        rule(
            self.draw >= self.wait,
            "the unwrap's bound ≥ the opening's wait",
        )?;
        rule(
            self.backoff.0 >= self.wait.saturating_mul(2),
            "the opening's back-off ≥ 2 · its wait",
        )?;
        rule(
            self.backoff.1 >= self.backoff.0,
            "the opening's back-off cap ≥ its first step",
        )?;
        rule(
            self.backoff.1 <= LONGEST,
            "the opening's back-off cap ≤ a century",
        )?;
        rule(!self.gone.is_zero(), "the opening's gone > 0")?;
        rule(
            self.draw <= self.gone,
            "the unwrap's bound ≤ the opening's gone",
        )?;
        rule(
            self.gone <= self.backoff.1,
            "the opening's gone ≤ its back-off cap",
        )?;
        rule(self.flights >= 1, "the opening's flights ≥ 1")?;
        Ok(self)
    }

    /// The probes a stage runs at once, at most: a quarter of its flights.
    const fn probes(&self) -> u32 {
        self.flights.div_ceil(4)
    }
}

/// What an open is answered.
#[derive(Debug, PartialEq, Eq)]
pub enum Opened<K> {
    /// The key the wrapped key unwraps to.
    Hit(K),
    /// Wait for flight `generation` until `deadline`, then ask again.
    Join(Generation, Instant),
    /// The tenant's key is not there.
    Gone,
    /// The KMS refused this wrapped key, within its memory.
    Rejected,
    /// Not opened now.
    Refused {
        /// Why.
        reason: Reason,
        /// When asking again may be served.
        retry_after: Option<Duration>,
        /// Whose opens the refusal stops.
        scope: Scope,
    },
}

#[derive(Debug)]
enum State<K> {
    Empty,
    Opened { key: K, ends: Instant },
    Rejected { until: Instant },
}

#[derive(Debug)]
struct Entry<K> {
    state: State<K>,
    flight: Option<Flying>,
    waiters: u32,
    due: Option<(Instant, Generation)>,
}

impl<K> Entry<K> {
    const fn new() -> Self {
        Entry {
            state: State::Empty,
            flight: None,
            waiters: 0,
            due: None,
        }
    }

    /// Whether it serves nothing and nobody waits on it: it may go.
    const fn idle(&self) -> bool {
        self.flight.is_none() && self.waiters == 0
    }

    /// When it changes next by time alone, after `now`.
    fn next(&self, now: Instant) -> Option<Instant> {
        match self.state {
            State::Opened { ends, .. } => Some(ends),
            State::Rejected { until } => Some(until),
            State::Empty => None,
        }
        .filter(|at| *at > now)
    }

    /// When it stops serving: its end, or none.
    const fn ends(&self) -> Option<Instant> {
        match self.state {
            State::Opened { ends, .. } => Some(ends),
            State::Rejected { until } => Some(until),
            State::Empty => None,
        }
    }
}

#[derive(Debug)]
struct Tenant<N> {
    evidence: Evidence,
    note: Option<N>,
    due: Option<(Instant, Generation)>,
}

/// What a deadline is for.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Due<T, W> {
    Tenant(T),
    Entry(T, W),
}

/// The core of one stage's opening end, a note of the shell's kept per
/// tenant while its evidence is.
#[derive(Debug)]
pub struct Unwraps<T, W, K, N = ()> {
    opening: Opening,
    tenants: HashMap<T, Tenant<N>>,
    entries: HashMap<(T, W), Entry<K>>,
    in_flight: HashMap<Generation, (T, W, Kind)>,
    next: Generation,
    held: Held,
    deadlines: BTreeMap<(Instant, Generation), Due<T, W>>,
    retired: Vec<K>,
    landed_in_time: Option<Instant>,
    evicted: u64,
}

impl<T: Clone + Eq + Hash, W: Clone + Eq + Hash, K, N> Unwraps<T, W, K, N> {
    /// A core holding nothing.
    #[must_use]
    pub fn new(opening: Opening) -> Self {
        Unwraps {
            opening,
            tenants: HashMap::new(),
            entries: HashMap::new(),
            in_flight: HashMap::new(),
            next: 1,
            held: Held::default(),
            deadlines: BTreeMap::new(),
            retired: Vec::new(),
            landed_in_time: None,
            evicted: 0,
        }
    }

    /// The opening.
    #[must_use]
    pub const fn opening(&self) -> &Opening {
        &self.opening
    }

    fn generation(&mut self) -> Generation {
        let generation = self.next;
        self.next = self.next.saturating_add(1);
        generation
    }

    /// How long a failure is remembered past its back-off: a cap.
    const fn memory(&self) -> Duration {
        self.opening.backoff.1
    }

    fn place(&self, kind: Kind) -> bool {
        let (held, cap) = (self.held, self.opening.flights);
        match kind {
            Kind::Miss | Kind::Renew => held.total() < cap,
            Kind::Probe => held.total() < cap && held.probes < self.opening.probes(),
        }
    }

    fn evidence(&self, tenant: &T) -> Evidence {
        self.tenants
            .get(tenant)
            .map_or(Evidence::Trusted, |held| held.evidence)
    }

    /// The scope of a refusal of `tenant`'s open for `reason`.
    fn scope_of(&self, tenant: &T, entry: Option<&Entry<K>>, reason: Reason) -> Scope {
        match reason {
            Reason::FlightsFull | Reason::Unspawned => Scope::Stage,
            Reason::Waited | Reason::BackingOff | Reason::Late => {
                let since = match self.evidence(tenant) {
                    Evidence::Failed { at, .. } => Some(at),
                    Evidence::Trusted | Evidence::Late => {
                        entry.and_then(|entry| entry.flight.map(|flying| flying.started))
                    }
                };
                scope_by(since, self.landed_in_time)
            }
        }
    }

    /// The scope of a refusal of `tenant`'s open of `wrapped` for `reason`,
    /// as of now: for the shell's own refusals.
    #[must_use]
    pub fn scope(&self, tenant: &T, wrapped: &W, reason: Reason) -> Scope {
        let entry = self.entries.get(&(tenant.clone(), wrapped.clone()));
        self.scope_of(tenant, entry, reason)
    }

    /// What an open of `wrapped` for `tenant` at `now` is answered, its wait
    /// ending at `deadline`, and the unwrap to start, if any.
    pub fn open(
        &mut self,
        tenant: &T,
        wrapped: &W,
        now: Instant,
        deadline: Instant,
    ) -> (Opened<K>, Option<Start>)
    where
        K: Clone,
    {
        let answered = self.answer(tenant, wrapped, now, deadline);
        self.settle_entry(tenant, wrapped, now);
        self.settle_tenant(tenant, now);
        answered
    }

    fn answer(
        &mut self,
        tenant: &T,
        wrapped: &W,
        now: Instant,
        deadline: Instant,
    ) -> (Opened<K>, Option<Start>)
    where
        K: Clone,
    {
        let evidence = self.evidence(tenant);
        // A key gone answers every open of its tenant within its memory,
        // before any key held: the keys were wiped when it landed.
        if let Evidence::Failed {
            failure: Failure::Gone,
            until,
            ..
        } = evidence
            && now < until
        {
            return (Opened::Gone, None);
        }
        let pair = (tenant.clone(), wrapped.clone());
        if let Some(entry) = self.entries.get_mut(&pair) {
            match entry.state {
                State::Opened { ref key, ends } if now < ends => {
                    return (Opened::Hit(key.clone()), None);
                }
                State::Rejected { until } if now < until => return (Opened::Rejected, None),
                State::Opened { .. } | State::Rejected { .. } | State::Empty => {}
            }
        }
        if let Evidence::Failed { until, .. } = evidence
            && now < until
        {
            let scope = self.scope_of(tenant, self.entries.get(&pair), Reason::BackingOff);
            return refused(
                Reason::BackingOff,
                Some(until.saturating_duration_since(now)),
                scope,
            );
        }
        if let Some(flying) = self.entries.get(&pair).and_then(|entry| entry.flight) {
            if evidence == Evidence::Trusted {
                if let Some(entry) = self.entries.get_mut(&pair) {
                    entry.waiters = entry.waiters.saturating_add(1);
                }
                return (Opened::Join(flying.generation, deadline), None);
            }
            let scope = self.scope_of(tenant, self.entries.get(&pair), Reason::Late);
            return refused(Reason::Late, None, scope);
        }
        let (kind, trusted) = match evidence {
            Evidence::Trusted => (Kind::Miss, true),
            Evidence::Late => (Kind::Miss, false),
            Evidence::Failed { at, .. } => (
                Kind::Probe,
                self.landed_in_time.is_some_and(|landed| landed > at),
            ),
        };
        if !self.place(kind) {
            return refused(Reason::FlightsFull, None, Scope::Stage);
        }
        if !self.entries.contains_key(&pair) && !self.room(now) {
            return refused(Reason::FlightsFull, None, Scope::Stage);
        }
        let scope = self.scope_of(tenant, self.entries.get(&pair), Reason::Late);
        let start = self.start(&pair, kind, now);
        if trusted {
            if let Some(held) = self.tenants.get_mut(tenant) {
                held.evidence = Evidence::Trusted;
            }
            if let Some(entry) = self.entries.get_mut(&pair) {
                entry.waiters = entry.waiters.saturating_add(1);
            }
            (Opened::Join(start.generation, deadline), Some(start))
        } else {
            (
                Opened::Refused {
                    reason: Reason::Late,
                    retry_after: None,
                    scope,
                },
                Some(start),
            )
        }
    }

    /// Room for one entry more: none needed under the cap; otherwise the idle
    /// entry that ends first is evicted, its key retired and the eviction
    /// counted. No room when every entry has a flight or a waiter.
    fn room(&mut self, now: Instant) -> bool {
        if self.entries.len() < self.opening.keys {
            return true;
        }
        let victim = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.idle())
            .min_by_key(|(_, entry)| entry.ends().unwrap_or(now))
            .map(|(pair, _)| pair.clone());
        let Some(pair) = victim else {
            return false;
        };
        if let Some(entry) = self.entries.remove(&pair) {
            if let Some(due) = entry.due {
                self.deadlines.remove(&due);
            }
            if let State::Opened { key, .. } = entry.state {
                self.retired.push(key);
            }
            self.evicted = self.evicted.saturating_add(1);
        }
        true
    }

    fn start(&mut self, pair: &(T, W), kind: Kind, now: Instant) -> Start {
        let generation = self.generation();
        let held = self.held.of(kind);
        *held = held.saturating_add(1);
        self.in_flight
            .insert(generation, (pair.0.clone(), pair.1.clone(), kind));
        let entry = self.entries.entry(pair.clone()).or_insert_with(Entry::new);
        entry.flight = Some(Flying {
            generation,
            kind,
            started: now,
        });
        Start { generation, kind }
    }

    /// A waiter of `tenant`'s `wrapped` stops waiting at `now`.
    pub fn left(&mut self, tenant: &T, wrapped: &W, now: Instant) {
        if let Some(entry) = self.entries.get_mut(&(tenant.clone(), wrapped.clone())) {
            entry.waiters = entry.waiters.saturating_sub(1);
        }
        self.settle_entry(tenant, wrapped, now);
    }

    /// A waiter of flight `generation` gave up at `now`, the flight still
    /// out: the tenant's unwraps are late once the flight has flown a whole
    /// wait, and no evidence before.
    pub fn gave_up(&mut self, tenant: &T, wrapped: &W, generation: Generation, now: Instant) {
        let wait = self.opening.wait;
        let flown = self
            .entries
            .get(&(tenant.clone(), wrapped.clone()))
            .and_then(|entry| entry.flight)
            .filter(|flying| flying.generation == generation)
            .is_some_and(|flying| now.saturating_duration_since(flying.started) >= wait);
        if flown {
            // Only a trusted tenant turns late: a failure, a key gone above
            // all, is not forgotten for a slow unwrap.
            match self.tenants.get_mut(tenant) {
                Some(held) if held.evidence == Evidence::Trusted => {
                    held.evidence = Evidence::Late;
                }
                Some(_) => {}
                None => {
                    self.tenants.insert(
                        tenant.clone(),
                        Tenant {
                            evidence: Evidence::Late,
                            note: None,
                            due: None,
                        },
                    );
                }
            }
        }
        self.settle_tenant(tenant, now);
    }

    /// Notes `note` for `tenant`, kept while its evidence is.
    pub fn note(&mut self, tenant: &T, note: Option<N>) {
        if let Some(held) = self.tenants.get_mut(tenant) {
            held.note = note;
        }
    }

    /// The note kept for `tenant`.
    #[must_use]
    pub fn noted(&self, tenant: &T) -> Option<&N> {
        self.tenants.get(tenant).and_then(|held| held.note.as_ref())
    }

    /// How many unwraps of `tenant` have failed in a row.
    #[must_use]
    pub fn failures(&self, tenant: &T) -> u32 {
        match self.evidence(tenant) {
            Evidence::Failed { failures, .. } => failures,
            Evidence::Trusted | Evidence::Late => 0,
        }
    }

    /// Flight `generation` of `tenant`'s `wrapped` landed at `now` with
    /// `outcome`. A landing of a flight the entry no longer flies changes
    /// nothing but its place.
    pub fn landed(
        &mut self,
        tenant: &T,
        wrapped: &W,
        generation: Generation,
        outcome: Outcome<K>,
        now: Instant,
        random: u64,
    ) {
        let Some((_, _, kind)) = self.in_flight.remove(&generation) else {
            if let Outcome::Drawn { key, .. } = outcome {
                self.retired.push(key);
            }
            return;
        };
        let held = self.held.of(kind);
        *held = held.saturating_sub(1);
        let pair = (tenant.clone(), wrapped.clone());
        if let Some(entry) = self.entries.get_mut(&pair)
            && entry
                .flight
                .is_some_and(|flying| flying.generation == generation)
        {
            entry.flight = None;
        }
        let failures = self.failures(tenant);
        let evidence = self.evidence(tenant);
        let gone_now = matches!(
            evidence,
            Evidence::Failed { failure: Failure::Gone, until, .. } if now < until
        );
        match outcome {
            Outcome::Drawn { key, started } => {
                let ends = later(started, self.opening.keep);
                // A key gone wipes what lands within its memory, and any key
                // whose unwrap began before it: its key-encryption key is
                // shredded. An unwrap lasts at most `draw` ≤ `gone`, so one
                // begun before a Gone lands within its memory.
                let before_gone = matches!(
                    evidence,
                    Evidence::Failed { failure: Failure::Gone, at, .. } if started < at
                );
                if gone_now || before_gone || ends <= now {
                    self.retired.push(key);
                } else if let Some(entry) = self.entries.get_mut(&pair) {
                    if let State::Opened { key: old, .. } =
                        std::mem::replace(&mut entry.state, State::Opened { key, ends })
                    {
                        self.retired.push(old);
                    }
                } else {
                    self.retired.push(key);
                }
                if !gone_now {
                    self.trust(tenant);
                }
                if now.saturating_duration_since(started) <= self.opening.wait {
                    self.landed_in_time = Some(now);
                }
            }
            Outcome::Gone => {
                self.fail(
                    tenant,
                    failed(
                        Some(self.opening.gone),
                        self.opening.backoff,
                        failures,
                        now,
                        random,
                    ),
                );
                self.wipe(tenant, now);
            }
            Outcome::Refused => {
                if let Some(entry) = self.entries.get_mut(&pair) {
                    let until = later(now, self.opening.gone);
                    if let State::Opened { key, .. } =
                        std::mem::replace(&mut entry.state, State::Rejected { until })
                    {
                        self.retired.push(key);
                    }
                }
                if !gone_now {
                    self.trust(tenant);
                }
            }
            Outcome::Unavailable | Outcome::Moment | Outcome::Abandoned | Outcome::Canceled => {
                if !gone_now {
                    self.fail(
                        tenant,
                        failed(None, self.opening.backoff, failures, now, random),
                    );
                }
            }
        }
        self.settle_entry(tenant, wrapped, now);
        self.settle_tenant(tenant, now);
    }

    fn trust(&mut self, tenant: &T) {
        if let Some(held) = self.tenants.get_mut(tenant) {
            held.evidence = Evidence::Trusted;
        }
    }

    fn fail(&mut self, tenant: &T, evidence: Evidence) {
        self.tenants
            .entry(tenant.clone())
            .or_insert(Tenant {
                evidence: Evidence::Trusted,
                note: None,
                due: None,
            })
            .evidence = evidence;
    }

    /// Every key of `tenant` held retired, its entries emptied: a key gone.
    fn wipe(&mut self, tenant: &T, now: Instant) {
        let pairs: Vec<(T, W)> = self
            .entries
            .keys()
            .filter(|(owner, _)| owner == tenant)
            .cloned()
            .collect();
        for pair in pairs {
            if let Some(entry) = self.entries.get_mut(&pair)
                && let State::Opened { key, .. } = std::mem::replace(&mut entry.state, State::Empty)
            {
                self.retired.push(key);
            }
            self.settle_entry(&pair.0, &pair.1, now);
        }
    }

    /// After a transition of an entry at `now`: a key ended retires, a
    /// refusal remembered past its memory is forgotten, an entry holding
    /// nothing goes, and the rest are due when they next change.
    fn settle_entry(&mut self, tenant: &T, wrapped: &W, now: Instant) {
        let pair = (tenant.clone(), wrapped.clone());
        let Some(entry) = self.entries.get_mut(&pair) else {
            return;
        };
        match entry.state {
            State::Opened { ends, .. } if ends <= now => {
                if let State::Opened { key, .. } = std::mem::replace(&mut entry.state, State::Empty)
                {
                    self.retired.push(key);
                }
            }
            State::Rejected { until } if until <= now => entry.state = State::Empty,
            State::Opened { .. } | State::Rejected { .. } | State::Empty => {}
        }
        let next = entry.next(now);
        let gone = matches!(entry.state, State::Empty) && entry.idle();
        let due = entry.due;
        if gone {
            if let Some(due) = due {
                self.deadlines.remove(&due);
            }
            self.entries.remove(&pair);
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
            self.deadlines
                .insert(due, Due::Entry(tenant.clone(), wrapped.clone()));
        }
        if let Some(entry) = self.entries.get_mut(&pair) {
            entry.due = due;
        }
    }

    /// After a transition of a tenant's evidence at `now`: trusted and
    /// noting nothing, it goes; a failure is due when its memory ends.
    fn settle_tenant(&mut self, tenant: &T, now: Instant) {
        let memory = self.memory();
        let Some(held) = self.tenants.get(tenant) else {
            return;
        };
        let forgets = match held.evidence {
            Evidence::Failed { until, .. } => Some(later(until, memory)),
            Evidence::Trusted | Evidence::Late => None,
        };
        let idle = match forgets {
            Some(at) => at <= now,
            None => held.evidence == Evidence::Trusted,
        };
        let due = held.due;
        if idle {
            if let Some(due) = due {
                self.deadlines.remove(&due);
            }
            self.tenants.remove(tenant);
            return;
        }
        let next = forgets.filter(|at| *at > now);
        if next == due.map(|(at, _)| at) {
            return;
        }
        if let Some(due) = due {
            self.deadlines.remove(&due);
        }
        let due = next.map(|at| (at, self.generation()));
        if let Some(due) = due {
            self.deadlines.insert(due, Due::Tenant(tenant.clone()));
        }
        if let Some(held) = self.tenants.get_mut(tenant) {
            held.due = due;
        }
    }

    /// Everything due by `now` settled: keys past their end retired,
    /// memories past their end forgotten.
    pub fn sweep(&mut self, now: Instant) {
        loop {
            let Some((&(at, generation), _)) = self.deadlines.iter().next() else {
                return;
            };
            if at > now {
                return;
            }
            let Some(due) = self.deadlines.remove(&(at, generation)) else {
                return;
            };
            match due {
                Due::Entry(tenant, wrapped) => {
                    if let Some(entry) = self.entries.get_mut(&(tenant.clone(), wrapped.clone())) {
                        entry.due = None;
                    }
                    self.settle_entry(&tenant, &wrapped, now);
                }
                Due::Tenant(tenant) => {
                    if let Some(held) = self.tenants.get_mut(&tenant) {
                        held.due = None;
                    }
                    self.settle_tenant(&tenant, now);
                }
            }
        }
    }

    /// The keys gone out of service since the last call, for the shell to
    /// wipe.
    pub fn take_retired(&mut self) -> Vec<K> {
        std::mem::take(&mut self.retired)
    }

    /// When the next sweep is due.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Instant> {
        self.deadlines.keys().next().map(|(at, _)| *at)
    }

    /// The keys held.
    #[must_use]
    pub fn keys_held(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| matches!(entry.state, State::Opened { .. }))
            .count()
    }

    /// The entries held.
    #[must_use]
    pub fn entries(&self) -> usize {
        self.entries.len()
    }

    /// The tenants whose evidence is held.
    #[must_use]
    pub fn tenants(&self) -> usize {
        self.tenants.len()
    }

    /// The tenants whose key is remembered gone at `now`.
    #[must_use]
    pub fn gone(&self, now: Instant) -> usize {
        self.tenants
            .values()
            .filter(|held| {
                matches!(held.evidence, Evidence::Failed { failure: Failure::Gone, until, .. } if now < until)
            })
            .count()
    }

    /// How many unwraps the stage runs.
    #[must_use]
    pub fn flights(&self) -> usize {
        self.in_flight.len()
    }

    /// How many entries were evicted to make room, in all.
    #[must_use]
    pub const fn evicted(&self) -> u64 {
        self.evicted
    }
}

/// An open refused for `reason`, over `scope`.
const fn refused<K>(
    reason: Reason,
    retry_after: Option<Duration>,
    scope: Scope,
) -> (Opened<K>, Option<Start>) {
    (
        Opened::Refused {
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
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::disallowed_methods,
    reason = "tests: a pure core's origin of time is any instant"
)]
mod tests {
    use super::*;

    const S: Duration = Duration::from_secs(1);

    fn opening() -> Opening {
        Opening::fitted(60 * S, 16)
    }

    fn core_with(opening: Opening) -> Unwraps<&'static str, &'static str, u32> {
        Unwraps::new(opening.validated().unwrap())
    }

    fn core() -> Unwraps<&'static str, &'static str, u32> {
        core_with(opening())
    }

    fn deadline(at: Instant) -> Instant {
        at + 2 * S
    }

    /// `wrapped` of `tenant` unwrapped to `key` at `at`, its unwrap started
    /// then.
    fn unwrapped(
        core: &mut Unwraps<&'static str, &'static str, u32>,
        tenant: &'static str,
        wrapped: &'static str,
        at: Instant,
        key: u32,
    ) {
        let (answer, start) = core.open(&tenant, &wrapped, at, deadline(at));
        let start = start.expect("an unwrap");
        assert_eq!(answer, Opened::Join(start.generation, deadline(at)));
        core.left(&tenant, &wrapped, at);
        core.landed(
            &tenant,
            &wrapped,
            start.generation,
            Outcome::Drawn { key, started: at },
            at,
            0,
        );
    }

    #[test]
    fn concurrent_opens_of_one_wrapped_key_cost_one_unwrap_and_wait_on_it() {
        let (mut core, t0) = (core(), Instant::now());
        let (first, start) = core.open(&"a", &"w", t0, deadline(t0));
        let start = start.unwrap();
        assert_eq!(first, Opened::Join(start.generation, deadline(t0)));
        for _ in 0..4 {
            let (answer, again) = core.open(&"a", &"w", t0, deadline(t0));
            assert_eq!(answer, Opened::Join(start.generation, deadline(t0)));
            assert_eq!(again, None, "one unwrap for every open");
        }
        let (_, other) = core.open(&"a", &"v", t0, deadline(t0));
        assert!(other.is_some(), "another wrapped key unwraps on its own");
        assert_eq!(core.flights(), 2);
    }

    #[test]
    fn a_key_serves_from_its_unwrap_s_start_for_keep_and_leaves_memory_at_its_end() {
        let (mut core, t0) = (core(), Instant::now());
        let (_, start) = core.open(&"a", &"w", t0, deadline(t0));
        core.left(&"a", &"w", t0);
        // The unwrap took four seconds; the key's life counts from its start.
        core.landed(
            &"a",
            &"w",
            start.unwrap().generation,
            Outcome::Drawn {
                key: 1,
                started: t0,
            },
            t0 + 4 * S,
            0,
        );
        let just = t0 + 60 * S - Duration::from_millis(1);
        assert_eq!(
            core.open(&"a", &"w", just, deadline(just)).0,
            Opened::Hit(1)
        );
        assert_eq!(core.next_deadline(), Some(t0 + 60 * S));
        core.sweep(t0 + 60 * S);
        assert_eq!(core.take_retired(), vec![1], "retired at its end, idle");
        assert_eq!((core.keys_held(), core.entries()), (0, 0));
    }

    /// A key gone answers every open of its tenant within its memory,
    /// before any key held; it wipes the keys held, and an unwrap that lands
    /// after it is wiped, never served.
    #[test]
    fn a_key_gone_answers_every_open_before_any_key_held_and_wipes_what_lands_after() {
        let (mut core, t0) = (core(), Instant::now());
        unwrapped(&mut core, "a", "w", t0, 1);
        let (_, late) = core.open(&"a", &"v", t0, deadline(t0));
        let late = late.unwrap();
        let (_, gone) = core.open(&"a", &"u", t0, deadline(t0));
        core.landed(
            &"a",
            &"u",
            gone.unwrap().generation,
            Outcome::Gone,
            t0 + S,
            0,
        );
        assert_eq!(core.take_retired(), vec![1], "the key held, wiped");
        for wrapped in ["w", "v", "u"] {
            let (answer, start) = core.open(&"a", &wrapped, t0 + S, deadline(t0 + S));
            assert_eq!((answer, start), (Opened::Gone, None), "{wrapped}");
        }
        core.landed(
            &"a",
            &"v",
            late.generation,
            Outcome::Drawn {
                key: 2,
                started: t0,
            },
            t0 + 2 * S,
            0,
        );
        assert_eq!(core.take_retired(), vec![2], "landed after the Gone: wiped");
        assert_eq!(core.keys_held(), 0);
        assert_eq!(core.gone(t0 + 2 * S), 1);
        let (other, _) = core.open(&"b", &"w", t0 + S, deadline(t0 + S));
        assert!(matches!(other, Opened::Join(..)), "another tenant unwraps");
    }

    /// A wrapped key the KMS refused is remembered for `gone`, answered
    /// without an unwrap; the tenant's other keys still unwrap, and past the
    /// memory it is asked again.
    #[test]
    fn a_refused_wrapped_key_is_remembered_and_the_tenant_s_others_still_unwrap() {
        let (mut core, t0) = (core(), Instant::now());
        let (_, start) = core.open(&"a", &"w", t0, deadline(t0));
        core.left(&"a", &"w", t0);
        core.landed(
            &"a",
            &"w",
            start.unwrap().generation,
            Outcome::Refused,
            t0,
            0,
        );
        assert_eq!(
            core.open(&"a", &"w", t0 + S, deadline(t0 + S)),
            (Opened::Rejected, None)
        );
        let (other, start) = core.open(&"a", &"v", t0 + S, deadline(t0 + S));
        assert!(matches!(other, Opened::Join(..)) && start.is_some());
        let past = t0 + 10 * S;
        let (again, start) = core.open(&"a", &"w", past, deadline(past));
        assert!(
            matches!(again, Opened::Join(..)) && start.is_some(),
            "{again:?}"
        );
    }

    /// A failed unwrap backs its tenant off, over the stage until another
    /// tenant's unwrap lands in time since, then over the tenant.
    #[test]
    fn a_failed_unwrap_backs_its_tenant_off_and_the_scope_follows_the_evidence() {
        let (mut core, t0) = (core(), Instant::now());
        let (_, a) = core.open(&"a", &"w", t0, deadline(t0));
        let (_, b) = core.open(&"b", &"w", t0, deadline(t0));
        core.left(&"a", &"w", t0);
        core.left(&"b", &"w", t0);
        core.landed(
            &"a",
            &"w",
            a.unwrap().generation,
            Outcome::Unavailable,
            t0,
            0,
        );
        let soon = t0 + Duration::from_millis(100);
        let (refused, start) = core.open(&"a", &"v", soon, deadline(soon));
        assert_eq!(start, None);
        assert!(matches!(
            refused,
            Opened::Refused {
                reason: Reason::BackingOff,
                retry_after: Some(_),
                scope: Scope::Stage
            }
        ));
        core.landed(
            &"b",
            &"w",
            b.unwrap().generation,
            Outcome::Drawn {
                key: 2,
                started: t0,
            },
            t0 + Duration::from_millis(500),
            0,
        );
        let later = t0 + Duration::from_millis(600);
        let (refused, _) = core.open(&"a", &"v", later, deadline(later));
        assert!(
            matches!(
                refused,
                Opened::Refused {
                    reason: Reason::BackingOff,
                    scope: Scope::Tenant,
                    ..
                }
            ),
            "{refused:?}"
        );
    }

    /// The entries are capped: a new one evicts the idle entry that ends
    /// first, counted; an entry a flight or a waiter holds is never evicted,
    /// and with every entry held an open is refused over the stage.
    #[test]
    fn a_new_entry_evicts_the_idle_one_that_ends_first_and_never_a_busy_one() {
        let mut opening = opening();
        opening.keys = 2;
        let (mut core, t0) = (core_with(opening), Instant::now());
        unwrapped(&mut core, "a", "w", t0, 1);
        unwrapped(&mut core, "a", "v", t0 + S, 2);
        let (_, start) = core.open(&"b", &"w", t0 + S, deadline(t0 + S));
        assert!(start.is_some());
        assert_eq!(core.take_retired(), vec![1], "the one ending first");
        assert_eq!((core.entries(), core.evicted()), (2, 1));
        // b's entry has a flight and a waiter; a's other one is idle.
        let (_, start) = core.open(&"c", &"w", t0 + S, deadline(t0 + S));
        assert!(start.is_some());
        assert_eq!(core.take_retired(), vec![2], "the idle one, not b's");
        let (full, start) = core.open(&"d", &"w", t0 + S, deadline(t0 + S));
        assert_eq!(start, None);
        assert!(matches!(
            full,
            Opened::Refused {
                reason: Reason::FlightsFull,
                scope: Scope::Stage,
                ..
            }
        ));
    }

    #[test]
    fn a_give_up_after_a_whole_wait_marks_the_tenant_late() {
        let (mut core, t0) = (core(), Instant::now());
        let (_, start) = core.open(&"a", &"w", t0, deadline(t0));
        let generation = start.unwrap().generation;
        core.gave_up(&"a", &"w", generation, t0 + S);
        core.left(&"a", &"w", t0 + S);
        assert!(
            matches!(
                core.open(&"a", &"w", t0 + S, deadline(t0 + S)).0,
                Opened::Join(..)
            ),
            "half a wait is no evidence"
        );
        core.left(&"a", &"w", t0 + S);
        core.gave_up(&"a", &"w", generation, deadline(t0));
        core.left(&"a", &"w", deadline(t0));
        let (late, _) = core.open(&"a", &"w", deadline(t0), deadline(deadline(t0)));
        assert!(matches!(
            late,
            Opened::Refused {
                reason: Reason::Late,
                ..
            }
        ));
    }

    /// The core's own contract, whatever bounds the shell keeps: a key
    /// whose unwrap began before its tenant's key was gone is never served,
    /// however late it lands.
    #[test]
    fn a_key_whose_unwrap_began_before_a_gone_is_never_served() {
        let (mut core, t0) = (core(), Instant::now());
        let (_, early) = core.open(&"a", &"w", t0, deadline(t0));
        let early = early.unwrap();
        core.left(&"a", &"w", t0);
        let (_, gone) = core.open(&"a", &"v", t0 + S, deadline(t0 + S));
        core.left(&"a", &"v", t0 + S);
        core.landed(
            &"a",
            &"v",
            gone.unwrap().generation,
            Outcome::Gone,
            t0 + S,
            0,
        );
        // Past the memory of the Gone, the early unwrap lands.
        let after = t0 + 12 * S;
        core.landed(
            &"a",
            &"w",
            early.generation,
            Outcome::Drawn {
                key: 3,
                started: t0,
            },
            after,
            0,
        );
        assert_eq!(core.take_retired(), vec![3]);
        assert_eq!(core.keys_held(), 0);
    }

    /// A slow unwrap given up does not make a tenant whose key is gone
    /// merely late: its opens are still told Gone.
    #[test]
    fn a_give_up_does_not_forget_a_key_gone() {
        let (mut core, t0) = (core(), Instant::now());
        let (_, slow) = core.open(&"a", &"w", t0, deadline(t0));
        let slow = slow.unwrap();
        let (_, gone) = core.open(&"a", &"v", t0, deadline(t0));
        core.landed(
            &"a",
            &"v",
            gone.unwrap().generation,
            Outcome::Gone,
            t0 + S,
            0,
        );
        core.gave_up(&"a", &"w", slow.generation, t0 + 3 * S);
        let at = t0 + 3 * S;
        assert_eq!(
            core.open(&"a", &"u", at, deadline(at)),
            (Opened::Gone, None)
        );
    }

    /// An open at a key's end is not served it, swept or not.
    #[test]
    fn an_open_at_a_key_s_end_is_not_served_it_unswept() {
        let (mut core, t0) = (core(), Instant::now());
        unwrapped(&mut core, "a", "w", t0, 1);
        let end = t0 + 60 * S;
        let (answer, start) = core.open(&"a", &"w", end, deadline(end));
        assert!(matches!(answer, Opened::Join(..)), "{answer:?}");
        assert!(start.is_some(), "unwrapped anew");
        assert_eq!(core.take_retired(), vec![1]);
    }

    /// A wrapped key refused shows the KMS answers the tenant: a tenant
    /// that backed off for another failure is trusted again.
    #[test]
    fn a_refusal_shows_the_kms_answers_the_tenant() {
        let (mut core, t0) = (core(), Instant::now());
        let (_, w) = core.open(&"a", &"w", t0, deadline(t0));
        let (_, v) = core.open(&"a", &"v", t0, deadline(t0));
        core.left(&"a", &"w", t0);
        core.left(&"a", &"v", t0);
        core.landed(
            &"a",
            &"w",
            w.unwrap().generation,
            Outcome::Unavailable,
            t0,
            0,
        );
        core.landed(&"a", &"v", v.unwrap().generation, Outcome::Refused, t0, 0);
        let at = t0 + Duration::from_millis(100);
        let (answer, start) = core.open(&"a", &"u", at, deadline(at));
        assert!(
            matches!(answer, Opened::Join(..)) && start.is_some(),
            "trusted again: {answer:?}"
        );
    }

    #[test]
    fn a_stale_landing_changes_nothing_but_its_key_retired() {
        let (mut core, t0) = (core(), Instant::now());
        unwrapped(&mut core, "a", "w", t0, 1);
        let before = format!("{core:?}");
        core.landed(
            &"a",
            &"w",
            999,
            Outcome::Drawn {
                key: 7,
                started: t0,
            },
            t0,
            0,
        );
        assert_eq!(core.take_retired(), vec![7]);
        assert_eq!(before, format!("{core:?}"));
    }

    #[test]
    fn invalid_openings_are_refused() {
        type Change = fn(&mut Opening);
        let cases: [(&str, Change); 10] = [
            ("keep > 0", |o| o.keep = Duration::ZERO),
            ("keys ≥ 1", |o| o.keys = 0),
            ("the opening's wait > 0", |o| o.wait = Duration::ZERO),
            ("the unwrap's bound ≥ the opening's wait", |o| {
                o.draw = o.wait / 2
            }),
            ("the opening's back-off ≥ 2 · its wait", |o| {
                o.backoff.0 = o.wait
            }),
            ("the opening's back-off cap ≥ its first step", |o| {
                o.backoff.1 = o.backoff.0 / 2;
            }),
            ("the opening's gone > 0", |o| o.gone = Duration::ZERO),
            ("the unwrap's bound ≤ the opening's gone", |o| {
                o.draw = o.gone + S
            }),
            ("the opening's gone ≤ its back-off cap", |o| {
                o.gone = o.backoff.1 + S
            }),
            ("the opening's flights ≥ 1", |o| o.flights = 0),
        ];
        assert!(opening().validated().is_ok());
        for (rule, change) in cases {
            let mut o = opening();
            change(&mut o);
            assert_eq!(o.validated(), Err(InvalidParameters(rule)), "{rule}");
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::disallowed_methods,
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
    const WRAPPED: [&str; 3] = ["x", "y", "z"];

    /// A key that says when its unwrap started.
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Unwrapped {
        id: u32,
        started: Instant,
    }

    #[derive(Clone, Debug)]
    enum Step {
        Open(usize, usize),
        Advance(u64),
        /// A landing: of its flight, or (`stale`) of one it no longer flies.
        Land(usize, usize, u8, bool),
        GaveUp(usize, usize),
        Left(usize, usize),
        Sweep,
    }

    fn step() -> impl Strategy<Value = Step> {
        prop_oneof![
            4 => (0..3usize, 0..3usize).prop_map(|(t, w)| Step::Open(t, w)),
            2 => (0..3_000u64).prop_map(Step::Advance),
            6 => (0..3usize, 0..3usize, 0..7u8, prop::bool::weighted(0.1))
                .prop_map(|(t, w, o, stale)| Step::Land(t, w, o, stale)),
            1 => (0..3usize, 0..3usize).prop_map(|(t, w)| Step::GaveUp(t, w)),
            4 => (0..3usize, 0..3usize).prop_map(|(t, w)| Step::Left(t, w)),
            1 => Just(Step::Sweep),
        ]
    }

    /// Fewer entries than pairs, more than flights.
    fn opening() -> Opening {
        Opening {
            keep: 20 * S,
            keys: 4,
            wait: S / 2,
            draw: 3 * S,
            backoff: (S, 8 * S),
            gone: 3 * S,
            flights: 2,
        }
        .validated()
        .unwrap()
    }

    #[derive(Default, Debug)]
    struct Activity {
        hits: u32,
        joins: u32,
        gone: u32,
        rejected: u32,
        refused: HashMap<&'static str, u32>,
        evicted: u64,
        wiped_after_gone: u32,
        gone_retiring: u32,
    }

    fn run(steps: &[Step], random: u64) -> Result<Activity, TestCaseError> {
        let opening = opening();
        let mut core: Unwraps<&'static str, &'static str, Unwrapped> = Unwraps::new(opening);
        let t0 = Instant::now();
        let mut now = t0;
        let mut flying: HashMap<(&'static str, &'static str), (Generation, Instant)> =
            HashMap::new();
        let mut stale: Vec<((&'static str, &'static str), Generation)> = Vec::new();
        let mut gone_at: HashMap<&'static str, Instant> = HashMap::new();
        let mut refused_at: HashMap<(&'static str, &'static str), Instant> = HashMap::new();
        let mut retired: HashSet<u32> = HashSet::new();
        let mut next_id = 0;
        let mut activity = Activity::default();
        for (n, step) in steps.iter().enumerate() {
            let random = random.rotate_left(u32::try_from(n % 64).unwrap());
            match *step {
                Step::Open(t, w) => {
                    let pair = (TENANTS[t], WRAPPED[w]);
                    let (answer, start) = core.open(&pair.0, &pair.1, now, now + opening.wait);
                    let within_gone = gone_at
                        .get(pair.0)
                        .is_some_and(|at| now < *at + opening.gone);
                    if within_gone {
                        prop_assert_eq!(&answer, &Opened::Gone, "an open within gone");
                        prop_assert!(start.is_none(), "an unwrap within gone");
                    }
                    if let Some(start) = start {
                        prop_assert!(!flying.contains_key(&pair), "two flights for {pair:?}");
                        flying.insert(pair, (start.generation, now));
                    }
                    match answer {
                        Opened::Hit(key) => {
                            activity.hits += 1;
                            prop_assert!(!retired.contains(&key.id), "a retired key served");
                            prop_assert!(
                                now < key.started + opening.keep,
                                "a key served past its unwrap plus keep"
                            );
                            prop_assert!(
                                gone_at.get(pair.0).is_none_or(|at| key.started >= *at),
                                "a key unwrapped before its tenant's was gone, served"
                            );
                        }
                        Opened::Join(..) => activity.joins += 1,
                        Opened::Gone => {
                            activity.gone += 1;
                            prop_assert!(within_gone, "Gone answered outside its memory");
                        }
                        Opened::Rejected => {
                            activity.rejected += 1;
                            prop_assert!(
                                refused_at
                                    .get(&pair)
                                    .is_some_and(|at| now < *at + opening.gone),
                                "Rejected answered outside its memory"
                            );
                        }
                        Opened::Refused { reason, .. } => {
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
                    }
                }
                Step::Advance(ms) => now += Duration::from_millis(ms),
                Step::Land(t, w, outcome, is_stale) => {
                    let pair = (TENANTS[t], WRAPPED[w]);
                    if is_stale {
                        let Some(&(owner, generation)) = stale.last() else {
                            continue;
                        };
                        next_id += 1;
                        core.landed(
                            &owner.0,
                            &owner.1,
                            generation,
                            Outcome::Drawn {
                                key: Unwrapped {
                                    id: next_id,
                                    started: now,
                                },
                                started: now,
                            },
                            now,
                            random,
                        );
                        let wiped = core.take_retired();
                        prop_assert!(
                            wiped.iter().any(|key| key.id == next_id),
                            "a stale landing's key not retired"
                        );
                        for key in wiped {
                            retired.insert(key.id);
                        }
                        continue;
                    }
                    let Some((generation, started)) = flying.remove(&pair) else {
                        continue;
                    };
                    stale.push((pair, generation));
                    let held_before = core.keys_held();
                    // The shell gives an unwrap up past its bound.
                    let outcome = if now > started + opening.draw {
                        6
                    } else {
                        outcome
                    };
                    let outcome = match outcome {
                        0..=2 => {
                            next_id += 1;
                            Outcome::Drawn {
                                key: Unwrapped {
                                    id: next_id,
                                    started,
                                },
                                started,
                            }
                        }
                        3 => Outcome::Gone,
                        4 => Outcome::Refused,
                        5 => Outcome::Unavailable,
                        _ => Outcome::Abandoned,
                    };
                    let within_gone = gone_at
                        .get(pair.0)
                        .is_some_and(|at| now < *at + opening.gone);
                    match &outcome {
                        // Every Gone is fresh evidence: its memory runs from
                        // the latest.
                        Outcome::Gone => {
                            gone_at.insert(pair.0, now);
                        }
                        Outcome::Refused => {
                            refused_at.insert(pair, now);
                        }
                        _ => {}
                    }
                    let drawn_id = match &outcome {
                        Outcome::Drawn { key, .. } => Some(key.id),
                        _ => None,
                    };
                    let is_gone = matches!(outcome, Outcome::Gone);
                    core.landed(&pair.0, &pair.1, generation, outcome, now, random);
                    let wiped: Vec<u32> = core.take_retired().iter().map(|key| key.id).collect();
                    if within_gone && let Some(id) = drawn_id {
                        prop_assert!(wiped.contains(&id), "a key landing within gone, kept");
                        activity.wiped_after_gone += 1;
                    }
                    if is_gone && held_before >= 2 && wiped.len() >= 2 {
                        activity.gone_retiring += 1;
                    }
                    for id in wiped {
                        prop_assert!(retired.insert(id), "a key retired twice");
                    }
                }
                Step::GaveUp(t, w) => {
                    let pair = (TENANTS[t], WRAPPED[w]);
                    if let Some(&(generation, _)) = flying.get(&pair) {
                        core.gave_up(&pair.0, &pair.1, generation, now);
                    }
                }
                Step::Left(t, w) => core.left(&TENANTS[t], &WRAPPED[w], now),
                Step::Sweep => {
                    core.sweep(now);
                    prop_assert!(core.next_deadline().is_none_or(|at| at > now));
                }
            }
            for key in core.take_retired() {
                prop_assert!(retired.insert(key.id), "a key retired twice");
            }
            prop_assert!(core.flights() <= 2, "more unwraps than the cap");
            prop_assert_eq!(core.flights(), flying.len(), "a flight lost");
            prop_assert!(core.entries() <= 4, "more entries than the cap");
            for pair in flying.keys() {
                prop_assert!(
                    core.entries
                        .get(pair)
                        .and_then(|entry| entry.flight)
                        .is_some(),
                    "a flight whose entry was evicted"
                );
            }
        }
        activity.evicted = core.evicted();
        Ok(activity)
    }

    proptest! {
        #![proptest_config(Config::with_cases(512))]

        #[test]
        fn no_history_breaks_the_opening_core_s_invariants(
            steps in proptest::collection::vec(step(), 1..120),
            random in any::<u64>(),
        ) {
            run(&steps, random)?;
        }
    }

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
            total.gone += activity.gone;
            total.rejected += activity.rejected;
            total.evicted += activity.evicted;
            total.wiped_after_gone += activity.wiped_after_gone;
            total.gone_retiring += activity.gone_retiring;
            for (k, v) in activity.refused {
                *total.refused.entry(k).or_default() += v;
            }
        }
        for (what, count) in [
            ("hits", u64::from(total.hits)),
            ("joins", u64::from(total.joins)),
            ("gone", u64::from(total.gone)),
            ("rejected", u64::from(total.rejected)),
            ("evictions", total.evicted),
            (
                "landings wiped after gone",
                u64::from(total.wiped_after_gone),
            ),
        ] {
            assert!(count > 0, "never {what}: {total:?}");
        }
        for reason in ["backing off", "late", "full"] {
            assert!(
                total.refused.contains_key(reason),
                "never refused {reason}: {total:?}"
            );
        }
    }
}
