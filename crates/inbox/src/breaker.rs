//! The breaker and a Tenant's memo (ADR-0042 decisions 1 and 2): the pure core
//! a process keeps of the verdicts its subscribers returned, so that a cause is
//! recorded once, at the extent its evidence proves.
//!
//! - **A Tenant's memo**: a (channel, Tenant) whose latest verdict was
//!   `Unavailable` is asked once in flight and no sooner than its back-off; its
//!   other heads are held without asking. A healthy Tenant never enters it.
//! - **The breaker**, per channel, a circuit breaker: closed while the
//!   channel's failures are narrow; open (the channel **paused**) once
//!   `Stage`-scoped `Unavailable` verdicts are broad across Tenants; then a
//!   **trial** a tick asks one head (a **witness**), and its verdict closes the
//!   channel or keeps it open. A channel open for two failed ticks is
//!   **stalled**, which the process reports.
//!
//! Nothing here reads a clock or does I/O: the time is an argument, and the
//! core returns what the shell is to do ([`Effect`], [`Before`], [`Told`]).

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use ascetic_ddd_bus::{Cause, Scope};

/// The breaker's and the memo's settings (ADR-0042 decisions 1 and 2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Breaking {
    /// The window over which the verdicts of Tenants are weighed.
    pub window: Duration,
    /// The fewest distinct Tenants failing at `Stage` scope that open a
    /// channel: one Tenant never does.
    pub fewest: usize,
    /// The share of the Tenants weighed, failing at `Stage` scope, that opens
    /// a channel: numerator, denominator.
    pub share: (usize, usize),
    /// So many distinct Tenants failing at `Stage` scope open a channel at a
    /// smaller share, `broad_share`: a broad outage while Tenants with cached
    /// keys still flow. The smaller share keeps a brownout's scattered
    /// failures from opening it.
    pub broad: usize,
    /// The share the `broad` count needs.
    pub broad_share: (usize, usize),
    /// An open channel's tick: its first step and its cap.
    pub tick: (Duration, Duration),
    /// A Tenant's back-off in the memo: its first step and its cap.
    pub hold: (Duration, Duration),
    /// The most (channel, Tenant)s a channel's memo keeps.
    pub memo: usize,
    /// The most Tenants a channel weighs as processed; beyond, "at least".
    pub healthy: usize,
    /// The most witnesses a channel keeps.
    pub witnesses: usize,
    /// So many inconclusive trials in a row, on distinct witnesses, close a
    /// channel.
    pub inconclusive: u32,
    /// How long a trial's subscriber may run.
    pub trial_wait: Duration,
    /// How long an ask may stay in flight before its mark is void.
    pub flight: Duration,
}

impl Default for Breaking {
    /// 30 s weighed; two Tenants at least and three quarters, or 32 and a
    /// quarter; a
    /// tick from 1 s to 15 s; a Tenant's back-off from 1 s to 300 s; 1,024
    /// Tenants in the memo and weighed healthy; 64 witnesses; three
    /// inconclusive trials; a trial of 2 s; a flight of 30 s.
    fn default() -> Self {
        Breaking {
            window: Duration::from_secs(30),
            fewest: 2,
            share: (3, 4),
            broad: 32,
            broad_share: (1, 4),
            tick: (Duration::from_secs(1), Duration::from_secs(15)),
            hold: (Duration::from_secs(1), Duration::from_secs(300)),
            memo: 1_024,
            healthy: 1_024,
            witnesses: 64,
            inconclusive: 3,
            trial_wait: Duration::from_secs(2),
            flight: Duration::from_secs(30),
        }
    }
}

impl Breaking {
    /// The same, weighing the verdicts of `window`.
    #[must_use]
    pub const fn with_window(self, window: Duration) -> Self {
        Breaking { window, ..self }
    }

    /// The same, an open channel ticking from `first` to `cap`.
    #[must_use]
    pub const fn with_tick(self, first: Duration, cap: Duration) -> Self {
        Breaking {
            tick: (first, cap),
            ..self
        }
    }

    /// The same, a Tenant's back-off from `first` to `cap`.
    #[must_use]
    pub const fn with_hold(self, first: Duration, cap: Duration) -> Self {
        Breaking {
            hold: (first, cap),
            ..self
        }
    }

    /// The same, a trial's subscriber running at most `wait`.
    #[must_use]
    pub const fn with_trial_wait(self, wait: Duration) -> Self {
        Breaking {
            trial_wait: wait,
            ..self
        }
    }

    /// The same, a channel's memo keeping at most `memo` Tenants.
    #[must_use]
    pub const fn with_memo(self, memo: usize) -> Self {
        Breaking { memo, ..self }
    }

    /// How long a stall row stays valid after its refresh: twice the tick's
    /// cap and a trial.
    #[must_use]
    pub fn stall_ttl(&self) -> Duration {
        self.tick.1 * 2 + self.trial_wait
    }
}

/// A subscriber's verdict on a row, as the breaker weighs it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// The row was processed.
    Processed,
    /// The subscriber returned `Unavailable`.
    Unavailable {
        /// Its cause.
        cause: Cause,
        /// Whose messages it says it stops.
        scope: Scope,
        /// When the teller says to ask again.
        retry_after: Option<Duration>,
    },
    /// The row's own failure: `Transient` or `Permanent`.
    Own,
}

/// What a trial came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Trial {
    /// The subscriber returned this verdict on the witness's head.
    Told(Verdict),
    /// The witness's head was no longer a head of the channel: nothing was
    /// asked.
    Gone,
    /// Nothing was asked, and nothing was learnt of the witness: its slot was
    /// taken by a dispatch, or the trial's transaction failed before a verdict
    /// was told. The trial counts nothing; the witness is tried again at the
    /// tick's first step, and a hastened trial's haste is not spent.
    Busy,
    /// The subscriber ran past the trial's wait and was rolled back.
    TimedOut,
}

/// An ask the core let through: hand it back with its verdict
/// ([`Breaker::told`]) or, if it ended without one, as abandoned
/// ([`Breaker::abandoned`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Flight {
    id: u64,
    channel: String,
    tenant: String,
    slot: i16,
    row_holds: u32,
    marked: bool,
}

/// What to do with a head before calling the subscriber.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Before {
    /// Ask it.
    Ask(Flight),
    /// Do not ask it: hold it this long from the database's now, under the
    /// cause that holds it, without counting the hold on the row.
    Hold {
        /// How long.
        wait: Duration,
        /// The cause that holds it: the memo's, or the pause's.
        cause: Cause,
    },
    /// Do not ask it, and write nothing on it: its Tenant is held by this
    /// process's memo at the stage's scope, one process's evidence, which
    /// the walk keeps to the process ([`Breaker::excluded`]); met here only
    /// when the memo took the Tenant after the walk began.
    Pass,
}

/// What a verdict asks of the row, and what the shell is to do.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Told {
    /// For an `Unavailable`: how long from the database's now the row is held.
    pub hold: Option<Duration>,
    /// What else follows.
    pub effects: Vec<Effect>,
}

/// What the shell is to do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    /// The channel is paused: the walk no longer asks its heads.
    Paused {
        /// The channel.
        channel: String,
        /// The pause's cause.
        cause: Cause,
    },
    /// Ask the witness's head, under the trial's wait; answer with
    /// [`Breaker::tried`] before `deadline`.
    Trial {
        /// The trial's id.
        id: u64,
        /// The channel.
        channel: String,
        /// The witness's slot.
        slot: i16,
        /// The witness's Tenant.
        tenant: String,
        /// When an unanswered trial is a failed tick.
        deadline: Instant,
    },
    /// The channel is stalled: write or refresh its stall row, valid for `ttl`.
    Stalled {
        /// The channel.
        channel: String,
        /// The pause's cause.
        cause: Cause,
        /// When the pause began.
        since: Instant,
        /// How long the row stays valid from the database's now.
        ttl: Duration,
        /// Whether to say so in the log: at the stall's first tick, then
        /// every [`REMINDER`].
        remind: bool,
    },
    /// The channel is asked again.
    Resumed {
        /// The channel.
        channel: String,
        /// The pause's cause.
        cause: Cause,
        /// Whether it had stalled.
        stalled: bool,
    },
}

/// How often a stall is said again in the log while it lasts.
pub const REMINDER: Duration = Duration::from_secs(300);

/// The breaker and the memo of one process.
#[derive(Debug)]
pub struct Breaker {
    settings: Breaking,
    channels: HashMap<String, Channel>,
    next: u64,
    /// The walk's excluded (channel, Tenant)s as last built, and until when
    /// they hold by time alone; `None` once the memo or a channel's state
    /// changed. On the happy path nothing changes, and the object stays
    /// built: `{}`.
    keys: Option<Keys>,
}

/// The Tenants a process's memo keeps from its walk on one channel under one
/// cause ([`Breaker::excluded_counts`]): how many, never which.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Excluded {
    /// The channel.
    pub channel: String,
    /// The cause of the entries' latest verdicts.
    pub cause: Cause,
    /// How many Tenants.
    pub tenants: usize,
    /// When the oldest one's run of `Unavailable` verdicts began.
    pub since: Instant,
}

/// The walk's excluded (channel, Tenant)s ([`Breaker::excluded`]).
#[derive(Debug)]
struct Keys {
    /// A `jsonb` object of `"<channel>/<tenant>"` keys.
    json: std::sync::Arc<str>,
    /// The earliest instant an excluded entry's back-off and flight both end,
    /// when it leaves the object without any verdict.
    until: Option<Instant>,
}

/// Writes the (channel, Tenant) key the walk tests, as a JSON string: a
/// channel is `scheme://authority`, with no third slash, so the first one
/// after it begins the Tenant.
fn push_key(out: &mut String, channel: &str, tenant: &str) {
    out.push('"');
    push_escaped(out, channel);
    out.push('/');
    push_escaped(out, tenant);
    out.push('"');
}

/// `s` escaped for a JSON string, without its quotes.
fn push_escaped(out: &mut String, s: &str) {
    if !s.bytes().any(|b| b == b'"' || b == b'\\' || b < 0x20) {
        out.push_str(s);
        return;
    }
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if u32::from(c) < 0x20 => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
}

#[derive(Debug, Default)]
struct Channel {
    memo: HashMap<String, Entry>,
    /// Tenants whose latest verdict was a `Stage`-scoped `Unavailable`: when,
    /// and its cause.
    failing: HashMap<String, (Instant, Cause)>,
    /// Tenants whose latest verdict was processed, and when.
    healthy: HashMap<String, Instant>,
    /// Until when the healthy set was full: S reads "at least" until then.
    saturated: Option<Instant>,
    witnesses: VecDeque<(i16, String)>,
    state: State,
    /// Failed ticks.
    failed: u32,
    /// When the channel last closed.
    closed_at: Option<Instant>,
}

#[derive(Debug)]
struct Entry {
    slot: i16,
    cause: Cause,
    unavailable: u32,
    until: Instant,
    /// The step that set `until`: the entry is protected a step past it.
    step: Duration,
    flight: Option<(u64, Instant)>,
    /// The scope of its latest verdict: at the stage's, the walk keeps the
    /// Tenant to this process; at the Tenant's, the rows carry its hold.
    scope: Scope,
    /// When the Tenant's run of `Unavailable` verdicts began.
    since: Instant,
}

impl Entry {
    /// Whether the walk excludes the Tenant now: held at the stage's scope,
    /// its back-off running or an ask of it in flight.
    fn excludes(&self, now: Instant) -> bool {
        self.scope == Scope::Stage
            && (self.until > now || self.flight.is_some_and(|(_, deadline)| deadline > now))
    }

    /// When, unchanged, it stops excluding.
    fn excludes_until(&self) -> Instant {
        self.flight
            .map_or(self.until, |(_, deadline)| self.until.max(deadline))
    }
}

/// A channel's state. An open channel's record is boxed: a process holds a
/// state for every channel it saw, and few are open at a time.
#[derive(Debug, Default)]
enum State {
    #[default]
    Closed,
    Open(Box<Open>),
}

#[derive(Debug)]
struct Open {
    cause: Cause,
    since: Instant,
    next_tick: Instant,
    told_after: Option<Duration>,
    trial: Option<Asked>,
    /// Trials whose deadline counted them: a processed answer from one still
    /// closes the channel.
    expired: VecDeque<u64>,
    last_trial: Option<Instant>,
    inconclusive: Vec<String>,
    /// When the stall was last said in the log.
    reminded: Option<Instant>,
    /// When the stall was last reported: a stalled channel is reported at
    /// least once a tick's cap, whether or not a call's one trial is its.
    refreshed: Option<Instant>,
    /// Whether the next tick was brought forward by a head met.
    hastened: bool,
    /// Whether the latest trial issued was a hastened one: a head met hastens
    /// no other until a scheduled trial has been issued, so that a stream of
    /// heads met adds at most one trial a tick.
    haste_spent: bool,
}

#[derive(Debug)]
struct Asked {
    id: u64,
    deadline: Instant,
    slot: i16,
    tenant: String,
    /// Whether a head met brought it forward.
    hastened: bool,
}

impl Breaker {
    /// A breaker with `settings`, every channel closed.
    #[must_use]
    pub fn new(settings: Breaking) -> Self {
        Breaker {
            settings,
            channels: HashMap::new(),
            next: 0,
            keys: None,
        }
    }

    /// The settings.
    #[must_use]
    pub fn settings(&self) -> &Breaking {
        &self.settings
    }

    /// The channels this process does not ask, sorted: the walk's argument.
    #[must_use]
    pub fn paused(&self) -> Vec<String> {
        let mut paused: Vec<String> = self
            .channels
            .iter()
            .filter(|(_, channel)| matches!(channel.state, State::Open(_)))
            .map(|(name, _)| name.clone())
            .collect();
        paused.sort_unstable();
        paused
    }

    /// Whether `channel` is paused.
    #[must_use]
    pub fn is_paused(&self, channel: &str) -> bool {
        self.channels
            .get(channel)
            .is_some_and(|channel| matches!(channel.state, State::Open(_)))
    }

    /// The (channel, Tenant)s this process's memo holds at the stage's
    /// scope on a channel it has not paused — an entry whose back-off runs,
    /// or whose ask is in flight — as the walk's argument beside the paused
    /// channels: a `jsonb` object of `"<channel>/<tenant>"` keys. Such a head
    /// is not due for this process, which walks as a guest while it holds any
    /// (ADR-0042 decision 5): one process's evidence of the stage stays in the
    /// process, and the other processes take the Tenant's heads. A paused
    /// channel's heads are not due already. Built once while nothing changes
    /// and no excluded entry's back-off or flight ends.
    pub fn excluded(&mut self, now: Instant) -> std::sync::Arc<str> {
        if let Some(keys) = &self.keys {
            if keys.until.is_none_or(|until| now < until) {
                return std::sync::Arc::clone(&keys.json);
            }
        }
        // in the memo's order: a `jsonb` object orders its own keys
        let mut until: Option<Instant> = None;
        let mut json = String::from("{");
        for (name, channel) in &self.channels {
            if matches!(channel.state, State::Open(_)) {
                continue;
            }
            for (tenant, entry) in &channel.memo {
                if entry.excludes(now) {
                    if json.len() > 1 {
                        json.push(',');
                    }
                    push_key(&mut json, name, tenant);
                    json.push_str(":1");
                    let ends = entry.excludes_until();
                    until = Some(until.map_or(ends, |u| u.min(ends)));
                }
            }
        }
        json.push('}');
        let json: std::sync::Arc<str> = json.into();
        self.keys = Some(Keys {
            json: std::sync::Arc::clone(&json),
            until,
        });
        json
    }

    /// What this process's memo holds at the stage's scope and the walk so
    /// keeps from it, by channel and cause — how many Tenants, and since when
    /// the oldest's run began — never a Tenant (ADR-0042 decision 6): what a
    /// host reports of a process cut off from a stage alone, which writes
    /// nothing on the rows it leaves.
    #[must_use]
    pub fn excluded_counts(&self, now: Instant) -> Vec<Excluded> {
        let mut counts: HashMap<(&str, Cause), (usize, Instant)> = HashMap::new();
        for (name, channel) in &self.channels {
            if matches!(channel.state, State::Open(_)) {
                continue;
            }
            for entry in channel.memo.values().filter(|e| e.excludes(now)) {
                let count = counts
                    .entry((name.as_str(), entry.cause))
                    .or_insert((0, entry.since));
                count.0 += 1;
                count.1 = count.1.min(entry.since);
            }
        }
        let mut out: Vec<Excluded> = counts
            .into_iter()
            .map(|((channel, cause), (tenants, since))| Excluded {
                channel: channel.to_owned(),
                cause,
                tenants,
                since,
            })
            .collect();
        out.sort_unstable_by(|a, b| (&a.channel, a.cause).cmp(&(&b.channel, b.cause)));
        out
    }

    fn id(&mut self) -> u64 {
        self.next += 1;
        self.next
    }

    /// Before the subscriber is called on `tenant`'s head of `channel` at
    /// `slot`, whose row counts `row_holds` `Unavailable` verdicts: ask it, or
    /// hold it without asking. A Tenant the memo does not hold is always
    /// asked, and a healthy Tenant is never serialised.
    pub fn before(
        &mut self,
        now: Instant,
        channel: &str,
        tenant: &str,
        slot: i16,
        row_holds: u32,
    ) -> Before {
        let settings = self.settings;
        let id = self.id();
        let entry = self.channels.get_mut(channel).and_then(|c| {
            if matches!(c.state, State::Open(_)) {
                // A head of a paused channel is asked only by a trial, and
                // met here only when the pause came after the walk began:
                // passed, the pause one process's evidence, written on no row.
                return Some(Err(()));
            }
            c.memo.get_mut(tenant).map(Ok)
        });
        let marked = match entry {
            Some(Err(())) => return Before::Pass,
            Some(Ok(entry)) => {
                let cause = entry.cause;
                if entry.excludes(now) {
                    // the walk began before the memo took the Tenant, or the
                    // keys it was given missed it: rebuilt, the next walk
                    // leaves it
                    self.keys = None;
                    return Before::Pass;
                }
                if let Some((_, deadline)) = entry.flight.filter(|(_, deadline)| *deadline > now) {
                    let until = entry.until.max(deadline).max(now + settings.hold.0);
                    return Before::Hold {
                        wait: until - now,
                        cause,
                    };
                }
                if entry.until > now {
                    return Before::Hold {
                        wait: entry.until - now,
                        cause,
                    };
                }
                entry.flight = Some((id, now + settings.flight));
                if entry.scope == Scope::Stage {
                    self.keys = None;
                }
                true
            }
            None => false,
        };
        Before::Ask(Flight {
            id,
            channel: channel.to_owned(),
            tenant: tenant.to_owned(),
            slot,
            row_holds,
            marked,
        })
    }

    /// An ask that ended without a verdict: rolled back, cancelled, or its
    /// connection lost. Its mark is released.
    pub fn abandoned(&mut self, flight: &Flight) {
        if let Some(entry) = self
            .channels
            .get_mut(&flight.channel)
            .and_then(|c| c.memo.get_mut(&flight.tenant))
        {
            if entry.flight.is_some_and(|(id, _)| id == flight.id) {
                entry.flight = None;
                if entry.scope == Scope::Stage {
                    self.keys = None;
                }
            }
        }
    }

    /// The subscriber's verdict on an ask [`Breaker::before`] let through.
    pub fn told(&mut self, now: Instant, flight: Flight, verdict: Verdict) -> Told {
        self.abandoned(&flight);
        let settings = self.settings;
        let channel = self.channels.entry(flight.channel.clone()).or_default();
        // the walk's excluded keys change with any entry made, renewed or
        // dropped, and with a channel paused
        let changes = channel.memo.contains_key(&flight.tenant)
            || matches!(verdict, Verdict::Unavailable { .. });
        let hold = channel.weigh(
            &settings,
            now,
            &flight.tenant,
            flight.slot,
            flight.row_holds,
            verdict,
        );
        let mut effects = Vec::new();
        if matches!(
            verdict,
            Verdict::Unavailable {
                scope: Scope::Stage,
                ..
            }
        ) && matches!(channel.state, State::Closed)
        {
            if let Some(cause) = channel.broad(&settings, now) {
                channel.open(&settings, now, cause, None);
                effects.push(Effect::Paused {
                    channel: flight.channel.clone(),
                    cause,
                });
                // Opened again within the window of a stall: still stalled,
                // and reported now, not at the first tick, which its failed
                // ticks put up to a cap away — the gap a flapping stage's
                // report must not lapse in (ADR-0042 decision 6).
                effects.extend(channel.stall(&settings, &flight.channel, now));
            }
        }
        if changes {
            self.keys = None;
        }
        Told { hold, effects }
    }

    /// Time has passed: the failed ticks of unanswered trials, the stalls to
    /// refresh, and at most one trial — a dispatch runs one, and a trial
    /// issued and not run would expire as a failed tick that asked nothing.
    /// The channels due take their turns, the one due longest first; a
    /// channel whose tick finds no witness waits its next tick and leaves the
    /// call's trial to another.
    pub fn tick(&mut self, now: Instant) -> Vec<Effect> {
        let mut names: Vec<String> = self.channels.keys().cloned().collect();
        names.sort_unstable();
        let mut effects = Vec::new();
        for name in &names {
            effects.extend(self.expire(now, name));
        }
        let mut due: Vec<(Instant, &String)> = names
            .iter()
            .filter_map(|name| match &self.channels.get(name)?.state {
                State::Open(open) if open.trial.is_none() && open.next_tick <= now => {
                    Some((open.next_tick, name))
                }
                _ => None,
            })
            .collect();
        due.sort_unstable();
        for (_, name) in due {
            let (trial, more) = self.try_witness(now, name);
            effects.extend(more);
            if trial {
                break;
            }
        }
        effects
    }

    /// A channel's trial unanswered by its deadline is a failed tick; and a
    /// stalled channel not reported for a tick's cap is reported now.
    fn expire(&mut self, now: Instant, name: &str) -> Vec<Effect> {
        let settings = self.settings;
        let Some(channel) = self.channels.get_mut(name) else {
            return Vec::new();
        };
        let State::Open(open) = &mut channel.state else {
            return Vec::new();
        };
        let mut ticked = open
            .refreshed
            .is_some_and(|at| now.saturating_duration_since(at) >= settings.tick.1);
        if let Some(asked) = open.trial.take_if(|asked| asked.deadline <= now) {
            open.expired.push_back(asked.id);
            if open.expired.len() > 8 {
                open.expired.pop_front();
            }
            channel.failed += 1;
            channel.schedule(&settings, now);
            ticked = true;
        }
        if ticked {
            channel.stall(&settings, name, now).into_iter().collect()
        } else {
            Vec::new()
        }
    }

    /// A channel's tick, due: its trial, if it has a witness, and its next
    /// tick; whether a trial was issued.
    fn try_witness(&mut self, now: Instant, name: &str) -> (bool, Vec<Effect>) {
        let settings = self.settings;
        let id = self.next + 1;
        let Some(channel) = self.channels.get_mut(name) else {
            return (false, Vec::new());
        };
        let mut effects = Vec::new();
        let witness = channel.witness(now);
        let trial = witness.is_some();
        if !trial && !channel.awaits_witness() {
            // No witness, and none to wait for: every head it knew was tried
            // inconclusively. Nothing proves the stage down, and the channel
            // closes; a head still refused at the stage's scope opens it
            // again, its failing Tenants kept across the close.
            if let Some((cause, stalled)) = channel.close(now) {
                effects.push(Effect::Resumed {
                    channel: name.to_owned(),
                    cause,
                    stalled,
                });
                // its stage-scoped entries are the walk's again: one renewed
                // by an ask begun before the pause and told after its trial
                // keeps its Tenant
                self.keys = None;
            }
            return (false, effects);
        }
        if let Some((slot, tenant)) = witness {
            let deadline = now + settings.trial_wait + settings.tick.0;
            if let State::Open(open) = &mut channel.state {
                open.trial = Some(Asked {
                    id,
                    deadline,
                    slot,
                    tenant: tenant.clone(),
                    hastened: open.hastened,
                });
                open.haste_spent = open.hastened;
                open.hastened = false;
                open.last_trial = Some(now);
            }
            self.next = id;
            effects.push(Effect::Trial {
                id,
                channel: name.to_owned(),
                slot,
                tenant,
                deadline,
            });
        }
        // The next tick, whatever this one asked: a verdict reschedules.
        channel.schedule(&settings, now);
        effects.extend(channel.stall(&settings, name, now));
        (trial, effects)
    }

    /// A guest walk met these heads of `channel` while it was paused, not
    /// asked: they are witnesses, and one whose Tenant the memo does not hold
    /// is tried at the next tick, now — at most once a tick's first step, and
    /// not after a hastened trial that left the channel open until a
    /// scheduled one has been issued, so that heads met add at most one trial
    /// a tick. The next [`Breaker::tick`] asks it, so that no trial is issued
    /// that the shell is not about to run.
    pub fn met(&mut self, now: Instant, channel: &str, heads: &[(i16, String)]) {
        let settings = self.settings;
        let Some(c) = self.channels.get_mut(channel) else {
            return;
        };
        if !matches!(c.state, State::Open(_)) {
            return;
        }
        for (slot, tenant) in heads {
            if !c.witnesses.iter().any(|(_, t)| t == tenant) {
                c.witnesses.push_back((*slot, tenant.clone()));
                while c.witnesses.len() > settings.witnesses {
                    c.witnesses.pop_front();
                }
            }
        }
        let fresh = heads.iter().any(|(_, t)| !c.memo.contains_key(t));
        let State::Open(open) = &mut c.state else {
            return;
        };
        let rested = open
            .last_trial
            .is_none_or(|at| now.saturating_duration_since(at) >= settings.tick.0);
        if fresh && open.trial.is_none() && rested && !open.haste_spent {
            open.next_tick = now;
            open.hastened = true;
        }
    }

    /// What the trial `id` came to: the row's hold, for an `Unavailable`, and
    /// what follows. An answer after the trial's deadline changes nothing but
    /// a close.
    pub fn tried(&mut self, now: Instant, id: u64, trial: Trial) -> Told {
        // a trial may drop or make an entry, or close its channel
        self.keys = None;
        let settings = self.settings;
        let found = self
            .channels
            .iter()
            .find_map(|(name, channel)| match &channel.state {
                State::Open(open) if open.trial.as_ref().is_some_and(|a| a.id == id) => {
                    Some((name.clone(), false))
                }
                State::Open(open) if open.expired.contains(&id) => Some((name.clone(), true)),
                _ => None,
            });
        let Some((name, late)) = found else {
            return Told::default();
        };
        let channel = self.channels.get_mut(&name).expect("found");
        let mut told = Told::default();
        let asked = match &mut channel.state {
            State::Open(open) if !late => open.trial.take(),
            _ => None,
        };
        let late = late || asked.as_ref().is_some_and(|a| a.deadline <= now);
        if late {
            if let Some(asked) = asked {
                // Its deadline passed unseen by a tick: count it now.
                if let State::Open(open) = &mut channel.state {
                    open.expired.push_back(asked.id);
                }
                channel.failed += 1;
                channel.schedule(&settings, now);
            }
            if trial == Trial::Told(Verdict::Processed) {
                if let Some((cause, stalled)) = channel.close(now) {
                    told.effects.push(Effect::Resumed {
                        channel: name.clone(),
                        cause,
                        stalled,
                    });
                }
            }
            return told;
        }
        let asked = asked.expect("a trial in flight");
        if trial != Trial::Busy {
            channel.witnesses.retain(|(_, t)| *t != asked.tenant);
        }
        match trial {
            Trial::Busy => {
                // Nothing asked, nothing learnt: the witness again at the
                // first step, a met one first in line, and a hastened trial's
                // haste not spent.
                if let State::Open(open) = &mut channel.state {
                    open.next_tick = now + settings.tick.0;
                    if asked.hastened {
                        open.haste_spent = false;
                    }
                }
                if !channel.memo.contains_key(&asked.tenant)
                    && !channel.witnesses.iter().any(|(_, t)| *t == asked.tenant)
                {
                    channel.witnesses.push_front((asked.slot, asked.tenant));
                }
            }
            Trial::Told(Verdict::Processed) => {
                channel.memo.remove(&asked.tenant);
                if let Some((cause, stalled)) = channel.close(now) {
                    told.effects.push(Effect::Resumed {
                        channel: name.clone(),
                        cause,
                        stalled,
                    });
                }
            }
            Trial::Told(Verdict::Unavailable {
                scope: Scope::Stage,
                retry_after,
                ..
            }) => {
                if let State::Open(open) = &mut channel.state {
                    open.inconclusive.clear();
                    open.told_after = retry_after;
                }
                channel.failed += 1;
                channel.schedule(&settings, now);
                // The witness's row waits for the next tick only.
                if let State::Open(open) = &channel.state {
                    told.hold = Some(open.next_tick.saturating_duration_since(now));
                }
                told.effects.extend(channel.stall(&settings, &name, now));
            }
            Trial::Told(
                verdict @ Verdict::Unavailable {
                    scope: Scope::Tenant,
                    ..
                },
            ) => {
                told.hold = channel.weigh(&settings, now, &asked.tenant, asked.slot, 0, verdict);
            }
            Trial::Told(Verdict::Own) | Trial::Gone | Trial::TimedOut => {
                if trial == Trial::Told(Verdict::Own) {
                    // The row's own: its Tenant's latest verdict is no longer
                    // an `Unavailable`.
                    channel.memo.remove(&asked.tenant);
                }
                let closes = if let State::Open(open) = &mut channel.state {
                    if !open.inconclusive.contains(&asked.tenant) {
                        open.inconclusive.push(asked.tenant.clone());
                    }
                    open.inconclusive.len() >= settings.inconclusive as usize
                } else {
                    false
                };
                if closes {
                    if let Some((cause, stalled)) = channel.close(now) {
                        told.effects.push(Effect::Resumed {
                            channel: name.clone(),
                            cause,
                            stalled,
                        });
                    }
                }
            }
        }
        told
    }
}

impl Channel {
    /// Weighs a verdict on `tenant`: the memo and the breadth. Returns the
    /// row's hold for an `Unavailable`.
    fn weigh(
        &mut self,
        settings: &Breaking,
        now: Instant,
        tenant: &str,
        slot: i16,
        row_holds: u32,
        verdict: Verdict,
    ) -> Option<Duration> {
        match verdict {
            Verdict::Processed => {
                self.memo.remove(tenant);
                self.failing.remove(tenant);
                self.processed(settings, now, tenant);
                None
            }
            Verdict::Own => {
                self.memo.remove(tenant);
                self.failing.remove(tenant);
                self.healthy.remove(tenant);
                None
            }
            Verdict::Unavailable {
                cause,
                scope,
                retry_after,
            } => {
                let previous = self.memo.get(tenant).map_or(0, |e| e.unavailable);
                // a run of refusals: one after a back-off that ended more than a
                // window ago begins another
                let since = self
                    .memo
                    .get(tenant)
                    .filter(|e| now.saturating_duration_since(e.until) < settings.window)
                    .map_or(now, |e| e.since);
                let unavailable = previous.saturating_add(1);
                let exponent = unavailable.max(row_holds.saturating_add(1)).min(30);
                let step = settings
                    .hold
                    .0
                    .saturating_mul(1u32 << exponent.min(31))
                    .max(retry_after.unwrap_or(Duration::ZERO))
                    .min(settings.hold.1);
                let until = now + step;
                self.remember(
                    settings,
                    now,
                    tenant,
                    Entry {
                        slot,
                        cause,
                        unavailable,
                        until,
                        step,
                        flight: None,
                        scope,
                        since,
                    },
                );
                match scope {
                    Scope::Stage => {
                        self.healthy.remove(tenant);
                        self.failing.insert(tenant.to_owned(), (now, cause));
                        if self.failing.len() > settings.healthy {
                            let horizon = now.checked_sub(settings.window);
                            self.failing
                                .retain(|_, (at, _)| horizon.is_none_or(|h| *at >= h));
                        }
                        // A process cut off alone delays another's head by a
                        // tick at most: the longer clock stays here.
                        Some(step.min(settings.tick.1))
                    }
                    Scope::Tenant => {
                        self.healthy.remove(tenant);
                        self.failing.remove(tenant);
                        Some(step)
                    }
                }
            }
        }
    }

    fn processed(&mut self, settings: &Breaking, now: Instant, tenant: &str) {
        if self.healthy.len() >= settings.healthy && !self.healthy.contains_key(tenant) {
            let horizon = now.checked_sub(settings.window);
            self.healthy
                .retain(|_, at| horizon.is_none_or(|h| *at >= h));
            if self.healthy.len() >= settings.healthy {
                self.saturated = Some(now + settings.window);
                return;
            }
        }
        self.healthy.insert(tenant.to_owned(), now);
    }

    /// Keeps `entry` for `tenant`, evicting an unprotected entry when the memo
    /// is full; a Tenant that finds the memo full of protected entries is not
    /// kept, and its row's own count paces it.
    fn remember(&mut self, settings: &Breaking, now: Instant, tenant: &str, entry: Entry) {
        if !self.memo.contains_key(tenant) && self.memo.len() >= settings.memo {
            let evictable = self
                .memo
                .iter()
                .filter(|(_, e)| e.flight.is_none() && e.until + e.step <= now)
                .min_by_key(|(_, e)| e.until)
                .map(|(t, _)| t.clone());
            match evictable {
                Some(t) => {
                    self.memo.remove(&t);
                }
                None => return,
            }
        }
        self.memo.insert(tenant.to_owned(), entry);
    }

    /// The cause of a broad `Stage` outage within the window, if the verdicts
    /// are broad enough to open the channel.
    fn broad(&mut self, settings: &Breaking, now: Instant) -> Option<Cause> {
        let horizon = now.checked_sub(settings.window);
        let within = |at: &Instant| horizon.is_none_or(|h| *at >= h);
        let failing: Vec<Cause> = self
            .failing
            .values()
            .filter(|(at, _)| within(at))
            .map(|(_, cause)| *cause)
            .collect();
        let f = failing.len();
        let saturated = self.saturated.is_some_and(|until| until > now);
        let s = if saturated {
            settings.healthy
        } else {
            self.healthy.values().filter(|at| within(at)).count()
        };
        let at_least = |(num, den): (usize, usize)| f * den >= num * (f + s);
        let broad = f >= settings.fewest
            && (at_least(settings.share)
                || (f >= settings.broad && at_least(settings.broad_share)));
        if !broad {
            return None;
        }
        let mut tally: HashMap<Cause, usize> = HashMap::new();
        for cause in failing {
            *tally.entry(cause).or_default() += 1;
        }
        tally
            .into_iter()
            .max_by(|(a, n), (b, m)| n.cmp(m).then_with(|| b.cmp(a)))
            .map(|(cause, _)| cause)
    }

    fn open(
        &mut self,
        settings: &Breaking,
        now: Instant,
        cause: Cause,
        told_after: Option<Duration>,
    ) {
        if self
            .closed_at
            .is_none_or(|at| now.saturating_duration_since(at) >= settings.window)
        {
            self.failed = 0;
        }
        self.state = State::Open(Box::new(Open {
            cause,
            since: now,
            next_tick: now,
            told_after,
            trial: None,
            expired: VecDeque::new(),
            last_trial: None,
            inconclusive: Vec::new(),
            reminded: None,
            refreshed: None,
            hastened: false,
            haste_spent: false,
        }));
        self.schedule(settings, now);
    }

    /// The next tick after a failed one: the tick's first step doubled by the
    /// failed ticks, capped, never sooner than a `retry_after` told.
    fn schedule(&mut self, settings: &Breaking, now: Instant) {
        let failed = self.failed;
        if let State::Open(open) = &mut self.state {
            let step = settings
                .tick
                .0
                .saturating_mul(1u32 << failed.min(16))
                .max(
                    open.told_after
                        .unwrap_or(Duration::ZERO)
                        .min(settings.tick.1),
                )
                .min(settings.tick.1);
            open.next_tick = now + step;
        }
    }

    fn close(&mut self, now: Instant) -> Option<(Cause, bool)> {
        let State::Open(open) = std::mem::take(&mut self.state) else {
            return None;
        };
        let stalled = self.failed >= 2;
        self.closed_at = Some(now);
        self.witnesses.clear();
        Some((open.cause, stalled))
    }

    /// The witness of the next trial: a head met whose Tenant the memo does
    /// not hold, else a memoised Tenant's whose back-off has run out.
    ///
    /// A witness whose trial was inconclusive is not tried again while the
    /// channel stays open: the next inconclusive one must be another's.
    fn witness(&mut self, now: Instant) -> Option<(i16, String)> {
        let tried: &[String] = match &self.state {
            State::Open(open) => &open.inconclusive,
            State::Closed => &[],
        };
        if let Some(index) = self
            .witnesses
            .iter()
            .position(|(_, t)| !self.memo.contains_key(t) && !tried.contains(t))
        {
            return self.witnesses.remove(index);
        }
        self.memo
            .iter()
            .filter(|(t, e)| e.until <= now && e.flight.is_none() && !tried.contains(t))
            .min_by_key(|(_, e)| e.until)
            .map(|(t, e)| (e.slot, t.clone()))
    }

    /// Whether a witness may still come without a guest meeting one: a head
    /// met, or a memoised Tenant, not yet tried inconclusively — a memoised
    /// one whether its back-off has run out or not.
    fn awaits_witness(&self) -> bool {
        let tried: &[String] = match &self.state {
            State::Open(open) => &open.inconclusive,
            State::Closed => &[],
        };
        self.witnesses.iter().any(|(_, t)| !tried.contains(t))
            || self.memo.keys().any(|t| !tried.contains(t))
    }

    fn stall(&mut self, settings: &Breaking, name: &str, now: Instant) -> Option<Effect> {
        let failed = self.failed;
        let State::Open(open) = &mut self.state else {
            return None;
        };
        if failed < 2 {
            return None;
        }
        let remind = open
            .reminded
            .is_none_or(|at| now.saturating_duration_since(at) >= REMINDER);
        if remind {
            open.reminded = Some(now);
        }
        open.refreshed = Some(now);
        Some(Effect::Stalled {
            channel: name.to_owned(),
            cause: open.cause,
            since: open.since,
            ttl: settings.stall_ttl(),
            remind,
        })
    }
}

#[cfg(test)]
mod tests;
