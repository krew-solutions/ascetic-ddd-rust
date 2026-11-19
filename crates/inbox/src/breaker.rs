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
//! - **The dependency's breaker**, per channel whose stage declares what it
//!   depends on ([`Breaker::register`]): a pause on the dependency's causes
//!   opens on breadth confirmed by a **check** of the dependency, and ends only
//!   on a check served — a row processed is no evidence of a dependency the
//!   stage may answer for from memory; S counts what it served by a call.
//!
//! Nothing here reads a clock or does I/O: the time is an argument, and the
//! core returns what the shell is to do ([`Effect`], [`Before`], [`Told`]).

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use ascetic_ddd_bus::{Cause, Check, Scope};

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
        /// Whose evidence a `Stage`-scoped one is.
        source: Source,
    },
    /// The row's own failure: `Transient` or `Permanent`.
    Own,
}

/// Whose evidence a `Stage`-scoped `Unavailable` is (ADR-0042 decision 1).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Source {
    /// The rows': a handler's own, or a stage's of a cause no dependency
    /// registered for the channel answers for. A row processed is its
    /// evidence of health.
    #[default]
    Rows,
    /// The channel's registered dependency's: told by a stage, of a cause the
    /// dependency answers for. Only a check served is its evidence of health.
    Dependency,
}

/// What a trial came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Trial {
    /// The subscriber returned this verdict on the witness's head.
    Told(Verdict),
    /// The witness's head was no longer a head of the channel — its row
    /// finished, by another process as like as not, or the Tenant's head in
    /// the slot another channel's: nothing was asked, and nothing learnt of
    /// the stage. The trial counts nothing; the witness is tried for this
    /// pause, its memo entry kept and its back-off renewed (what it remembered
    /// is stale, and its exponent still paces the Tenant), and the next
    /// witness is asked at the tick's first step.
    Gone,
    /// The witness's head could not be asked: its row unreadable, or waiting
    /// on a dependency not yet processed. Inconclusive.
    Unreadable,
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
    /// Check the channel's dependency now, of `head` if given; answer with
    /// [`Breaker::checked`] before `deadline`.
    Check {
        /// The check's id.
        id: u64,
        /// The channel.
        channel: String,
        /// The head to ask of: its slot and its Tenant.
        head: Option<(i16, String)>,
        /// When an unanswered check is a failed tick.
        deadline: Instant,
    },
    /// A suspicion of the channel's dependency refuted by a check served: its
    /// failing Tenants forgotten, the channel asked on.
    Refuted {
        /// The channel.
        channel: String,
        /// The suspicion's cause.
        cause: Cause,
    },
    /// The channel is asked again.
    Resumed {
        /// The channel.
        channel: String,
        /// The pause's cause.
        cause: Cause,
        /// Whether it had stalled.
        stalled: bool,
        /// What closed it.
        by: Resumption,
    },
}

/// What closed a paused channel (ADR-0042 decision 1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resumption {
    /// A trial's witness was processed.
    Processed,
    /// Three trials in a row on distinct witnesses were inconclusive.
    Inconclusive,
    /// No witness was left, and none to wait for.
    NoWitness,
    /// A check of the channel's dependency was served.
    Served,
    /// The dependency's pause had nothing left to ask of, nor a head met, for
    /// a window: nothing it held.
    NothingToHold,
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
    /// Set by a close that no processed trial made, cleared by a row of the
    /// channel processed.
    probation: Option<Probation>,
    /// The breaker of the dependency registered for the channel, if any.
    dependency: Option<Box<Gate>>,
}

impl Channel {
    /// Whether the channel is paused: by its rows' breaker or its
    /// dependency's.
    fn is_paused(&self) -> bool {
        matches!(self.state, State::Open(_))
            || self
                .dependency
                .as_ref()
                .is_some_and(|gate| matches!(gate.state, GateState::Open(_)))
    }
}

/// The breaker of a channel's dependency (ADR-0042 decision 1).
#[derive(Debug)]
struct Gate {
    /// The longest a check of it takes.
    bound: Duration,
    /// Tenants whose latest verdict was the dependency's `Stage`-scoped
    /// `Unavailable`: when, and its cause.
    failing: HashMap<String, (Instant, Cause)>,
    /// What the dependency served, sampled at most once a second, within the
    /// window.
    served: VecDeque<(Instant, u64)>,
    state: GateState,
    /// Failed ticks.
    failed: u32,
    /// When it last closed.
    closed_at: Option<Instant>,
    /// Heads guests met while it was open.
    witnesses: VecDeque<(i16, String)>,
    /// The suspicion's step: the least time between two checks of a
    /// suspicion refuted.
    step: Duration,
    /// When the breadth rule last tripped.
    tripped_at: Option<Instant>,
    /// When a check last refuted a suspicion.
    refuted_at: Option<Instant>,
}

#[derive(Debug)]
enum GateState {
    /// Asked; suspect while a check is due or in flight.
    Closed {
        due: Option<Instant>,
        check: Option<Checking>,
    },
    Open(Box<GateOpen>),
}

#[derive(Debug)]
struct Checking {
    id: u64,
    deadline: Instant,
    /// What the dependency had served when the check was issued.
    served: u64,
    /// Whether a head was given.
    head: bool,
    /// Whether a head met brought it forward.
    hastened: bool,
}

#[derive(Debug)]
struct GateOpen {
    cause: Cause,
    since: Instant,
    next_tick: Instant,
    check: Option<Checking>,
    /// Checks whose deadline counted them: a served answer from one still
    /// closes the channel.
    expired: VecDeque<u64>,
    last_check: Option<Instant>,
    /// Heads asked of this pause.
    tried: Vec<String>,
    reminded: Option<Instant>,
    refreshed: Option<Instant>,
    hastened: bool,
    haste_spent: bool,
    /// Since when its checks had nothing to ask of and no head was met.
    unasked_since: Option<Instant>,
}

impl Gate {
    fn new(bound: Duration, settings: &Breaking) -> Self {
        Gate {
            bound,
            failing: HashMap::new(),
            served: VecDeque::new(),
            state: GateState::Closed {
                due: None,
                check: None,
            },
            failed: 0,
            closed_at: None,
            witnesses: VecDeque::new(),
            step: settings.tick.0,
            tripped_at: None,
            refuted_at: None,
        }
    }

    /// The newest value served.
    fn served_now(&self) -> u64 {
        self.served.back().map_or(0, |(_, value)| *value)
    }

    fn sample(&mut self, settings: &Breaking, now: Instant, value: u64) {
        match self.served.back_mut() {
            Some((at, last)) if now.saturating_duration_since(*at) < Duration::from_secs(1) => {
                *last = (*last).max(value);
            }
            _ => self.served.push_back((now, value)),
        }
        let horizon = now.checked_sub(settings.window);
        while self.served.len() > 1
            && self
                .served
                .front()
                .is_some_and(|(at, _)| horizon.is_some_and(|h| *at < h))
        {
            self.served.pop_front();
        }
    }

    /// S: what it served since the oldest sample within the window.
    fn s(&self, settings: &Breaking, now: Instant) -> usize {
        let horizon = now.checked_sub(settings.window);
        let oldest = self
            .served
            .iter()
            .find(|(at, _)| horizon.is_none_or(|h| *at >= h))
            .map_or(self.served_now(), |(_, value)| *value);
        usize::try_from(self.served_now().saturating_sub(oldest)).unwrap_or(usize::MAX)
    }

    /// The cause of a broad outage of the dependency within the window, if
    /// its refusals are broad enough against what it served.
    fn broad(&self, settings: &Breaking, now: Instant) -> Option<Cause> {
        let horizon = now.checked_sub(settings.window);
        let failing: Vec<Cause> = self
            .failing
            .values()
            .filter(|(at, _)| horizon.is_none_or(|h| *at >= h))
            .map(|(_, cause)| *cause)
            .collect();
        breadth(settings, &failing, self.s(settings, now))
    }

    /// The modal cause of its failing Tenants within the window, broad or
    /// not.
    fn modal(&self, settings: &Breaking, now: Instant) -> Cause {
        let horizon = now.checked_sub(settings.window);
        let failing: Vec<Cause> = self
            .failing
            .values()
            .filter(|(at, _)| horizon.is_none_or(|h| *at >= h))
            .map(|(_, cause)| *cause)
            .collect();
        modal_of(&failing).unwrap_or(Cause::UNEXPLAINED)
    }

    /// The next check of an open gate after a failed one.
    fn schedule(&mut self, settings: &Breaking, now: Instant) {
        let step = settings
            .tick
            .0
            .saturating_mul(1u32 << self.failed.min(16))
            .min(settings.tick.1);
        if let GateState::Open(open) = &mut self.state {
            open.next_tick = now + step;
        }
    }

    fn stall_ttl(&self, settings: &Breaking) -> Duration {
        settings.tick.1 * 2 + self.bound
    }

    fn stall(&mut self, settings: &Breaking, name: &str, now: Instant) -> Option<Effect> {
        let (failed, ttl) = (self.failed, self.stall_ttl(settings));
        let GateState::Open(open) = &mut self.state else {
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
            ttl,
            remind,
        })
    }

    /// Opens it under `cause`: a failed tick, the ones of a close shorter than
    /// the window kept; stalled at once at two.
    fn open(&mut self, settings: &Breaking, name: &str, now: Instant, cause: Cause) -> Vec<Effect> {
        if self
            .closed_at
            .is_none_or(|at| now.saturating_duration_since(at) >= settings.window)
        {
            self.failed = 0;
        }
        self.failed = self.failed.saturating_add(1);
        self.witnesses.clear();
        self.state = GateState::Open(Box::new(GateOpen {
            cause,
            since: now,
            next_tick: now,
            check: None,
            expired: VecDeque::new(),
            last_check: None,
            tried: Vec::new(),
            reminded: None,
            refreshed: None,
            hastened: false,
            haste_spent: false,
            unasked_since: None,
        }));
        self.schedule(settings, now);
        let mut effects = vec![Effect::Paused {
            channel: name.to_owned(),
            cause,
        }];
        effects.extend(self.stall(settings, name, now));
        effects
    }

    fn close(&mut self, name: &str, now: Instant, by: Resumption) -> Option<Effect> {
        let GateState::Open(open) = std::mem::replace(
            &mut self.state,
            GateState::Closed {
                due: None,
                check: None,
            },
        ) else {
            return None;
        };
        self.failing.clear();
        self.witnesses.clear();
        self.closed_at = Some(now);
        Some(Effect::Resumed {
            channel: name.to_owned(),
            cause: open.cause,
            stalled: self.failed >= 2,
            by,
        })
    }

    /// The breadth rule tripped: a check is due at the suspicion's step,
    /// pending though no further verdict comes.
    fn trip(&mut self, settings: &Breaking, now: Instant) {
        if self
            .tripped_at
            .is_none_or(|at| now.saturating_duration_since(at) >= settings.window)
        {
            self.step = settings.tick.0;
        }
        self.tripped_at = Some(now);
        let earliest = self.refuted_at.map_or(now, |at| (at + self.step).max(now));
        if let GateState::Closed { due, check } = &mut self.state
            && check.is_none()
            && due.is_none()
        {
            *due = Some(earliest);
        }
    }

    /// The head of the next check: the first met of the heads not asked this
    /// round whose Tenant the memo does not hold, else of those it holds;
    /// once every head met was asked, a round begins anew, so that a head met
    /// is never left unasked for good; with no head met, none.
    fn head(&mut self, memo: &HashMap<String, Entry>) -> Option<(i16, String)> {
        let Gate {
            witnesses, state, ..
        } = self;
        let GateState::Open(open) = state else {
            return None;
        };
        let pick = |tried: &[String]| {
            witnesses
                .iter()
                .position(|(_, t)| !tried.contains(t) && !memo.contains_key(t))
                .or_else(|| witnesses.iter().position(|(_, t)| !tried.contains(t)))
        };
        let index = match pick(&open.tried) {
            Some(index) => index,
            None if !witnesses.is_empty() => {
                open.tried.clear();
                pick(&open.tried)?
            }
            None => return None,
        };
        witnesses.remove(index)
    }

    /// Whether a refusal of the dependency's is left within the window: a
    /// suspicion without one lapses.
    fn suspected(&self, settings: &Breaking, now: Instant) -> bool {
        let horizon = now.checked_sub(settings.window);
        self.failing
            .values()
            .any(|(at, _)| horizon.is_none_or(|h| *at >= h))
    }
}

/// Whether `failing`, the causes of the distinct Tenants failing within the
/// window, against `s` weighed healthy, are broad enough: their modal cause.
fn breadth(settings: &Breaking, failing: &[Cause], s: usize) -> Option<Cause> {
    let f = failing.len();
    let at_least = |(num, den): (usize, usize)| f * den >= num * (f + s);
    let broad = f >= settings.fewest
        && (at_least(settings.share) || (f >= settings.broad && at_least(settings.broad_share)));
    if !broad {
        return None;
    }
    modal_of(failing)
}

/// The most frequent of `causes`, ties by the cause.
fn modal_of(causes: &[Cause]) -> Option<Cause> {
    let mut tally: HashMap<Cause, usize> = HashMap::new();
    for cause in causes {
        *tally.entry(*cause).or_default() += 1;
    }
    tally
        .into_iter()
        .max_by(|(a, n), (b, m)| n.cmp(m).then_with(|| b.cmp(a)))
        .map(|(cause, _)| cause)
}

/// A channel closed without a processed trial — three inconclusive, or no
/// witness left — is on probation until a row of it is processed: a pause
/// while on probation counts a failed tick at its opening, whatever the time
/// since the close, and a stalled channel on probation keeps its report
/// though closed. A handler that waits on a hanging dependency past the
/// trial's wait closes its pauses inconclusively, and its next refusal comes
/// a client's timeout later, perhaps past the window: probation, not the
/// window, tells its pauses from a slow handler's, which never pause again.
#[derive(Clone, Copy, Debug)]
struct Probation {
    /// When the first pause of the run began: the stall's `since`.
    since: Instant,
    /// Its cause.
    cause: Cause,
    /// When the stall was last reported while closed.
    refreshed: Option<Instant>,
    /// When the stall was last said in the log while closed.
    reminded: Option<Instant>,
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
    /// Witnesses whose heads were gone this pause: tried, not counted.
    tried: Vec<String>,
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
            .filter(|(_, channel)| channel.is_paused())
            .map(|(name, _)| name.clone())
            .collect();
        paused.sort_unstable();
        paused
    }

    /// Whether `channel` is paused.
    #[must_use]
    pub fn is_paused(&self, channel: &str) -> bool {
        self.channels.get(channel).is_some_and(Channel::is_paused)
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
        if let Some(keys) = &self.keys
            && keys.until.is_none_or(|until| now < until)
        {
            return std::sync::Arc::clone(&keys.json);
        }
        // in the memo's order: a `jsonb` object orders its own keys
        let mut until: Option<Instant> = None;
        let mut json = String::from("{");
        for (name, channel) in &self.channels {
            if channel.is_paused() {
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
            if channel.is_paused() {
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
            if c.is_paused() {
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
            && entry.flight.is_some_and(|(id, _)| id == flight.id)
        {
            entry.flight = None;
            if entry.scope == Scope::Stage {
                self.keys = None;
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
        if let Verdict::Unavailable {
            scope: Scope::Stage,
            source: Source::Dependency,
            ..
        } = verdict
            && let Some(gate) = channel.dependency.as_mut()
        {
            if matches!(gate.state, GateState::Closed { .. })
                && gate.broad(&settings, now).is_some()
            {
                gate.trip(&settings, now);
            }
        } else if matches!(
            verdict,
            Verdict::Unavailable {
                scope: Scope::Stage,
                ..
            }
        ) && matches!(channel.state, State::Closed)
            && let Some(cause) = channel.broad(&settings, now)
        {
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
        if changes {
            self.keys = None;
        }
        Told { hold, effects }
    }

    /// Time has passed: the failed ticks of unanswered trials and checks, the
    /// stalls to refresh, the suspicions lapsed, at most one check of a
    /// dependency — it runs beside the dispatch — and at most one trial — a
    /// dispatch runs one, and a trial issued and not run would expire as a
    /// failed tick that asked nothing.
    /// The channels due take their turns, the one due longest first; a
    /// channel whose tick finds no witness waits its next tick and leaves the
    /// call's trial to another.
    pub fn tick(&mut self, now: Instant) -> Vec<Effect> {
        let mut names: Vec<String> = self.channels.keys().cloned().collect();
        names.sort_unstable();
        let mut effects = Vec::new();
        for name in &names {
            effects.extend(self.expire(now, name));
            effects.extend(self.expire_check(now, name));
        }
        // (when due, whether the dependency's, the channel)
        let mut due: Vec<(Instant, bool, &String)> = Vec::new();
        for name in &names {
            let Some(channel) = self.channels.get(name) else {
                continue;
            };
            if let State::Open(open) = &channel.state
                && open.trial.is_none()
                && open.next_tick <= now
            {
                due.push((open.next_tick, false, name));
            }
            match channel.dependency.as_deref().map(|gate| &gate.state) {
                Some(GateState::Closed {
                    due: Some(at),
                    check: None,
                }) if *at <= now => due.push((*at, true, name)),
                Some(GateState::Open(open)) if open.check.is_none() && open.next_tick <= now => {
                    due.push((open.next_tick, true, name));
                }
                _ => {}
            }
        }
        due.sort_unstable();
        // a check runs beside the dispatch, a trial in it: a call issues at
        // most one of each
        let (mut checked, mut trialled) = (false, false);
        for (_, dependency, name) in due {
            if dependency {
                if !checked {
                    let more = self.try_check(now, name);
                    checked = more.iter().any(|e| matches!(e, Effect::Check { .. }));
                    effects.extend(more);
                }
                continue;
            }
            if !trialled {
                let (trial, more) = self.try_witness(now, name);
                effects.extend(more);
                trialled = trial;
            }
        }
        effects
    }

    /// Registers the dependency of `channel`'s stage, whose check takes at
    /// most `bound` (ADR-0042 decision 1): the channel's `Stage`-scoped
    /// refusals of its causes are then its breaker's, which only a check
    /// served closes.
    pub fn register(&mut self, channel: &str, bound: Duration) {
        let settings = self.settings;
        let entry = self.channels.entry(channel.to_owned()).or_default();
        match entry.dependency.as_mut() {
            Some(gate) => gate.bound = bound,
            None => entry.dependency = Some(Box::new(Gate::new(bound, &settings))),
        }
    }

    /// What `channel`'s dependency has served so far, sampled.
    pub fn served(&mut self, now: Instant, channel: &str, served: u64) {
        let settings = self.settings;
        if let Some(gate) = self
            .channels
            .get_mut(channel)
            .and_then(|c| c.dependency.as_mut())
        {
            gate.sample(&settings, now, served);
        }
    }

    /// A dependency's check unanswered by its deadline: a suspicion
    /// confirmed, or a failed tick of a pause; and a stalled pause not
    /// reported for a tick's cap reported now.
    fn expire_check(&mut self, now: Instant, name: &str) -> Vec<Effect> {
        let settings = self.settings;
        let Some(channel) = self.channels.get_mut(name) else {
            return Vec::new();
        };
        let Some(gate) = channel.dependency.as_mut() else {
            return Vec::new();
        };
        let mut effects = Vec::new();
        let suspected = gate.suspected(&settings, now);
        match &mut gate.state {
            GateState::Closed { due, check } => {
                if check.as_ref().is_some_and(|c| c.deadline <= now) {
                    let expired = check.take().map(|c| c.id);
                    *due = None;
                    // a suspicion with no refusal left within the window
                    // lapses rather than pausing on nothing
                    if suspected {
                        let cause = gate.modal(&settings, now);
                        effects.extend(gate.open(&settings, name, now, cause));
                        // its answer, however late, still closes the pause
                        if let (GateState::Open(open), Some(id)) = (&mut gate.state, expired) {
                            open.expired.push_back(id);
                        }
                        self.keys = None;
                    }
                } else if check.is_none() && due.is_some() && !suspected {
                    *due = None;
                }
            }
            GateState::Open(open) => {
                let mut ticked = open
                    .refreshed
                    .is_some_and(|at| now.saturating_duration_since(at) >= settings.tick.1);
                if let Some(asked) = open.check.take_if(|c| c.deadline <= now) {
                    open.expired.push_back(asked.id);
                    if open.expired.len() > 8 {
                        open.expired.pop_front();
                    }
                    gate.failed += 1;
                    gate.schedule(&settings, now);
                    ticked = true;
                }
                if ticked {
                    effects.extend(gate.stall(&settings, name, now));
                }
            }
        }
        effects
    }

    /// A dependency's check, due: issued, its next tick scheduled.
    fn try_check(&mut self, now: Instant, name: &str) -> Vec<Effect> {
        let settings = self.settings;
        let id = self.next + 1;
        let Some(channel) = self.channels.get_mut(name) else {
            return Vec::new();
        };
        let memo = &channel.memo;
        let Some(gate) = channel.dependency.as_mut() else {
            return Vec::new();
        };
        let served = gate.served_now();
        let deadline = now + gate.bound + settings.tick.0;
        let head = gate.head(memo);
        match &mut gate.state {
            GateState::Closed { due, check } => {
                *due = None;
                *check = Some(Checking {
                    id,
                    deadline,
                    served,
                    head: false,
                    hastened: false,
                });
            }
            GateState::Open(open) => {
                if let Some((_, tenant)) = &head {
                    open.tried.push(tenant.clone());
                }
                open.check = Some(Checking {
                    id,
                    deadline,
                    served,
                    head: head.is_some(),
                    hastened: open.hastened,
                });
                open.haste_spent = open.hastened;
                open.hastened = false;
                open.last_check = Some(now);
            }
        }
        self.next = id;
        let mut effects = vec![Effect::Check {
            id,
            channel: name.to_owned(),
            head: if matches!(gate.state, GateState::Open(_)) {
                head
            } else {
                None
            },
            deadline,
        }];
        gate.schedule(&settings, now);
        effects.extend(gate.stall(&settings, name, now));
        effects
    }

    /// What the dependency's check `id` came to.
    pub fn checked(&mut self, now: Instant, id: u64, check: Check) -> Vec<Effect> {
        self.keys = None;
        let settings = self.settings;
        let found = self.channels.iter().find_map(|(name, channel)| {
            let gate = channel.dependency.as_deref()?;
            match &gate.state {
                GateState::Closed {
                    check: Some(asked), ..
                } if asked.id == id => Some(name.clone()),
                GateState::Open(open)
                    if open.check.as_ref().is_some_and(|a| a.id == id)
                        || open.expired.contains(&id) =>
                {
                    Some(name.clone())
                }
                _ => None,
            }
        });
        let Some(name) = found else {
            return Vec::new();
        };
        let Some(gate) = self
            .channels
            .get_mut(&name)
            .and_then(|c| c.dependency.as_mut())
        else {
            return Vec::new();
        };
        let served_now = gate.served_now();
        let mut effects = Vec::new();
        match &mut gate.state {
            GateState::Closed { due, check: asked } => {
                let Some(asked) = asked.take() else {
                    return effects;
                };
                // what the dependency served for the rows meanwhile refutes
                // the suspicion as a check served does
                let refuted = check == Check::Served || served_now > asked.served;
                match check {
                    _ if refuted => {
                        *due = None;
                        let cause = gate.modal(&settings, now);
                        gate.failing.clear();
                        gate.refuted_at = Some(now);
                        gate.step = (gate.step * 2).min(settings.tick.1);
                        effects.push(Effect::Refuted {
                            channel: name.clone(),
                            cause,
                        });
                    }
                    Check::Unserved(cause) => {
                        *due = None;
                        effects.extend(gate.open(&settings, &name, now, cause));
                    }
                    Check::Unasked | Check::Served => {
                        gate.step = (gate.step * 2).min(settings.tick.1);
                        *due = Some(now + gate.step);
                    }
                }
            }
            GateState::Open(open) => {
                let late = open.expired.contains(&id);
                let asked = if late { None } else { open.check.take() };
                if check == Check::Served {
                    effects.extend(gate.close(&name, now, Resumption::Served));
                    return effects;
                }
                let Some(asked) = asked else {
                    return effects;
                };
                match check {
                    Check::Unserved(cause) => {
                        open.cause = cause;
                        open.unasked_since = None;
                        gate.failed += 1;
                        gate.schedule(&settings, now);
                        effects.extend(gate.stall(&settings, &name, now));
                    }
                    Check::Unasked => {
                        if asked.hastened {
                            open.haste_spent = false;
                        }
                        let nothing = !asked.head && gate.witnesses.is_empty();
                        let since = if nothing {
                            *open.unasked_since.get_or_insert(now)
                        } else {
                            open.unasked_since = None;
                            now
                        };
                        if nothing && now.saturating_duration_since(since) >= settings.window {
                            effects.extend(gate.close(&name, now, Resumption::NothingToHold));
                        }
                    }
                    Check::Served => {}
                }
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
            return channel
                .stall_on_probation(&settings, name, now)
                .into_iter()
                .collect();
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
            if let Some((cause, stalled)) = channel.close(now, Resumption::NoWitness) {
                effects.push(Effect::Resumed {
                    channel: name.to_owned(),
                    cause,
                    stalled,
                    by: Resumption::NoWitness,
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
        if let Some(gate) = c.dependency.as_mut()
            && let GateState::Open(open) = &mut gate.state
        {
            open.unasked_since = None;
            for (slot, tenant) in heads {
                if !gate.witnesses.iter().any(|(_, t)| t == tenant) {
                    gate.witnesses.push_back((*slot, tenant.clone()));
                    while gate.witnesses.len() > settings.witnesses {
                        gate.witnesses.pop_front();
                    }
                }
            }
            let fresh = heads.iter().any(|(_, t)| !open.tried.contains(t));
            let rested = open
                .last_check
                .is_none_or(|at| now.saturating_duration_since(at) >= settings.tick.0);
            if fresh && open.check.is_none() && rested && !open.haste_spent {
                open.next_tick = now;
                open.hastened = true;
            }
        }
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
            if trial == Trial::Told(Verdict::Processed)
                && let Some((cause, stalled)) = channel.close(now, Resumption::Processed)
            {
                told.effects.push(Effect::Resumed {
                    channel: name.clone(),
                    cause,
                    stalled,
                    by: Resumption::Processed,
                });
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
                if let Some((cause, stalled)) = channel.close(now, Resumption::Processed) {
                    told.effects.push(Effect::Resumed {
                        channel: name.clone(),
                        cause,
                        stalled,
                        by: Resumption::Processed,
                    });
                }
            }
            Trial::Told(
                verdict @ Verdict::Unavailable {
                    scope: Scope::Stage,
                    source: Source::Dependency,
                    ..
                },
            ) if channel.dependency.is_some() => {
                // The stage refused before the handler ran: nothing learnt
                // of the rows' own dependency, and a refusal of the stage's,
                // its breaker's as any other
                told.hold = channel.weigh(&settings, now, &asked.tenant, asked.slot, 0, verdict);
                if let Some(gate) = channel.dependency.as_mut()
                    && matches!(gate.state, GateState::Closed { .. })
                    && gate.broad(&settings, now).is_some()
                {
                    gate.trip(&settings, now);
                }
                told.effects
                    .extend(channel.inconclusive(&settings, now, &name, &asked.tenant));
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
            Trial::Gone => {
                // Nothing asked, and what the memo kept of the Tenant is
                // stale: its back-off renewed, not grown; the witness tried
                // for this pause; the next one at the first step.
                if let Some(entry) = channel.memo.get_mut(&asked.tenant) {
                    entry.until = now + entry.step;
                }
                if let State::Open(open) = &mut channel.state {
                    if !open.tried.contains(&asked.tenant) {
                        open.tried.push(asked.tenant.clone());
                    }
                    open.next_tick = now + settings.tick.0;
                    if asked.hastened {
                        open.haste_spent = false;
                    }
                }
            }
            Trial::Told(Verdict::Own) | Trial::Unreadable | Trial::TimedOut => {
                if trial == Trial::Told(Verdict::Own) {
                    // The row's own: its Tenant's latest verdict is no longer
                    // an `Unavailable`.
                    channel.memo.remove(&asked.tenant);
                }
                told.effects
                    .extend(channel.inconclusive(&settings, now, &name, &asked.tenant));
            }
        }
        told
    }
}

impl Channel {
    /// A trial that learnt nothing of the rows: its witness counted
    /// inconclusive, three distinct closing the channel.
    fn inconclusive(
        &mut self,
        settings: &Breaking,
        now: Instant,
        name: &str,
        tenant: &str,
    ) -> Option<Effect> {
        let State::Open(open) = &mut self.state else {
            return None;
        };
        if !open.inconclusive.iter().any(|t| t == tenant) {
            open.inconclusive.push(tenant.to_owned());
        }
        if open.inconclusive.len() < settings.inconclusive as usize {
            return None;
        }
        let (cause, stalled) = self.close(now, Resumption::Inconclusive)?;
        Some(Effect::Resumed {
            channel: name.to_owned(),
            cause,
            stalled,
            by: Resumption::Inconclusive,
        })
    }

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
                if let Some(gate) = self.dependency.as_mut() {
                    gate.failing.remove(tenant);
                }
                self.probation = None;
                self.processed(settings, now, tenant);
                None
            }
            Verdict::Own => {
                self.memo.remove(tenant);
                self.failing.remove(tenant);
                if let Some(gate) = self.dependency.as_mut() {
                    gate.failing.remove(tenant);
                }
                self.healthy.remove(tenant);
                None
            }
            Verdict::Unavailable {
                cause,
                scope,
                retry_after,
                source,
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
                        let failing = match (source, self.dependency.as_mut()) {
                            (Source::Dependency, Some(gate)) => {
                                self.failing.remove(tenant);
                                &mut gate.failing
                            }
                            _ => {
                                if let Some(gate) = self.dependency.as_mut() {
                                    gate.failing.remove(tenant);
                                }
                                &mut self.failing
                            }
                        };
                        failing.insert(tenant.to_owned(), (now, cause));
                        if failing.len() > settings.healthy {
                            let horizon = now.checked_sub(settings.window);
                            failing.retain(|_, (at, _)| horizon.is_none_or(|h| *at >= h));
                        }
                        // A process cut off alone delays another's head by a
                        // tick at most: the longer clock stays here.
                        Some(step.min(settings.tick.1))
                    }
                    Scope::Tenant => {
                        self.healthy.remove(tenant);
                        self.failing.remove(tenant);
                        if let Some(gate) = self.dependency.as_mut() {
                            gate.failing.remove(tenant);
                        }
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
        let saturated = self.saturated.is_some_and(|until| until > now);
        let s = if saturated {
            settings.healthy
        } else {
            self.healthy.values().filter(|at| within(at)).count()
        };
        breadth(settings, &failing, s)
    }

    fn open(
        &mut self,
        settings: &Breaking,
        now: Instant,
        cause: Cause,
        told_after: Option<Duration>,
    ) {
        let since = match self.probation {
            // a pause on probation counts a failed tick, whatever the time
            // since the close, and keeps the run's beginning
            Some(probation) => {
                self.failed = self.failed.saturating_add(1);
                probation.since
            }
            None => {
                if self
                    .closed_at
                    .is_none_or(|at| now.saturating_duration_since(at) >= settings.window)
                {
                    self.failed = 0;
                }
                now
            }
        };
        self.state = State::Open(Box::new(Open {
            cause,
            since,
            next_tick: now,
            told_after,
            trial: None,
            expired: VecDeque::new(),
            last_trial: None,
            inconclusive: Vec::new(),
            tried: Vec::new(),
            reminded: self.probation.and_then(|probation| probation.reminded),
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

    fn close(&mut self, now: Instant, by: Resumption) -> Option<(Cause, bool)> {
        let State::Open(open) = std::mem::take(&mut self.state) else {
            return None;
        };
        let stalled = self.failed >= 2;
        self.closed_at = Some(now);
        self.witnesses.clear();
        self.probation = match by {
            Resumption::Processed | Resumption::Served | Resumption::NothingToHold => None,
            Resumption::Inconclusive | Resumption::NoWitness => {
                let mut probation = self.probation.unwrap_or(Probation {
                    since: open.since,
                    cause: open.cause,
                    refreshed: None,
                    reminded: None,
                });
                // the report and the log carry on from the pause's
                probation.refreshed = probation.refreshed.max(open.refreshed);
                probation.reminded = probation.reminded.max(open.reminded);
                Some(probation)
            }
        };
        Some((open.cause, stalled))
    }

    /// The witness of the next trial: a head met whose Tenant the memo does
    /// not hold, else a memoised Tenant's whose back-off has run out.
    ///
    /// A witness whose trial was inconclusive is not tried again while the
    /// channel stays open: the next inconclusive one must be another's.
    fn witness(&mut self, now: Instant) -> Option<(i16, String)> {
        let (inconclusive, gone): (&[String], &[String]) = match &self.state {
            State::Open(open) => (&open.inconclusive, &open.tried),
            State::Closed => (&[], &[]),
        };
        let tried = |t: &String| inconclusive.contains(t) || gone.contains(t);
        if let Some(index) = self
            .witnesses
            .iter()
            .position(|(_, t)| !self.memo.contains_key(t) && !tried(t))
        {
            return self.witnesses.remove(index);
        }
        self.memo
            .iter()
            .filter(|(t, e)| e.until <= now && e.flight.is_none() && !tried(t))
            // the earliest due first, ties by the Tenant: the same every run
            .min_by(|(t, e), (u, f)| e.until.cmp(&f.until).then_with(|| t.cmp(u)))
            .map(|(t, e)| (e.slot, t.clone()))
    }

    /// Whether a witness may still come without a guest meeting one: a head
    /// met, or a memoised Tenant, not yet tried inconclusively — a memoised
    /// one whether its back-off has run out or not.
    fn awaits_witness(&self) -> bool {
        let (inconclusive, gone): (&[String], &[String]) = match &self.state {
            State::Open(open) => (&open.inconclusive, &open.tried),
            State::Closed => (&[], &[]),
        };
        let tried = |t: &String| inconclusive.contains(t) || gone.contains(t);
        self.witnesses.iter().any(|(_, t)| !tried(t)) || self.memo.keys().any(|t| !tried(t))
    }

    /// A channel closed on probation and stalled keeps its report, refreshed
    /// once a tick's cap, until a row of it is processed.
    fn stall_on_probation(
        &mut self,
        settings: &Breaking,
        name: &str,
        now: Instant,
    ) -> Option<Effect> {
        if self.failed < 2 {
            return None;
        }
        let probation = self.probation.as_mut()?;
        if probation
            .refreshed
            .is_some_and(|at| now.saturating_duration_since(at) < settings.tick.1)
        {
            return None;
        }
        let remind = probation
            .reminded
            .is_none_or(|at| now.saturating_duration_since(at) >= REMINDER);
        if remind {
            probation.reminded = Some(now);
        }
        probation.refreshed = Some(now);
        Some(Effect::Stalled {
            channel: name.to_owned(),
            cause: probation.cause,
            since: probation.since,
            ttl: settings.stall_ttl(),
            remind,
        })
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
