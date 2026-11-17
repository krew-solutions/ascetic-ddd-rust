//! The scenario catalogue of ADR-0042 (C1–C13), on the pure core: each test
//! names the mutation that turns it red.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use ascetic_ddd_bus::{Cause, Scope};
use proptest::prelude::*;

use super::{Before, Breaker, Breaking, Effect, Told, Trial, Verdict};

const CHANNEL: &str = "kafka://device.lifecycle";

fn cause(name: &'static str) -> Cause {
    Cause::new(name).expect("a valid cause")
}

fn stage() -> Verdict {
    Verdict::Unavailable {
        cause: cause("kms.unavailable"),
        scope: Scope::Stage,
        retry_after: None,
    }
}

fn tenant_scope(retry_after: Option<Duration>) -> Verdict {
    Verdict::Unavailable {
        cause: cause("kms.unavailable"),
        scope: Scope::Tenant,
        retry_after,
    }
}

/// A seeded xorshift: the histories are random and the same every run.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// A process's breaker, the rows' told counts and due times kept as the table
/// would: a row held is not walked to before its time.
struct Sim {
    breaker: Breaker,
    now: Instant,
    holds: HashMap<(String, i16), u32>,
    due: HashMap<(String, i16), Instant>,
}

impl Sim {
    fn new(settings: Breaking) -> Self {
        Sim {
            breaker: Breaker::new(settings),
            now: Instant::now(),
            holds: HashMap::new(),
            due: HashMap::new(),
        }
    }

    fn advance(&mut self, by: Duration) {
        self.now += by;
    }

    /// One ask of `tenant`'s head at `slot`, if the row is due: its verdict
    /// if the core let it through, `None` if it was held or not due.
    fn ask(&mut self, tenant: &str, slot: i16, verdict: Verdict) -> Option<Told> {
        let key = (tenant.to_owned(), slot);
        if self.due.get(&key).is_some_and(|due| *due > self.now) {
            return None;
        }
        let row_holds = self.holds.get(&key).copied().unwrap_or(0);
        match self
            .breaker
            .before(self.now, CHANNEL, tenant, slot, row_holds)
        {
            Before::Ask(flight) => {
                match verdict {
                    Verdict::Unavailable { .. } => *self.holds.entry(key.clone()).or_default() += 1,
                    Verdict::Processed | Verdict::Own => {
                        self.holds.remove(&key);
                    }
                }
                let told = self.breaker.told(self.now, flight, verdict);
                match told.hold {
                    Some(hold) => {
                        self.due.insert(key, self.now + hold);
                    }
                    None => {
                        self.due.remove(&key);
                    }
                }
                Some(told)
            }
            Before::Hold { wait, .. } => {
                assert!(wait > Duration::ZERO, "a hold is always ahead of now");
                self.due.insert(key, self.now + wait);
                None
            }
        }
    }

    fn tick(&mut self) -> Vec<Effect> {
        self.breaker.tick(self.now)
    }

    /// Opens the channel: two Tenants failing at `Stage` scope, the fewest.
    fn open(&mut self) {
        for tenant in ["a", "b"] {
            self.ask(tenant, 0, stage());
        }
        assert!(
            self.breaker.is_paused(CHANNEL),
            "two of two Tenants failing"
        );
    }

    /// Ticks until a trial is asked, for at most a minute.
    fn next_trial(&mut self) -> (u64, String) {
        for _ in 0..600 {
            for effect in self.tick() {
                if let Effect::Trial { id, tenant, .. } = effect {
                    return (id, tenant);
                }
            }
            self.advance(Duration::from_millis(100));
        }
        panic!("no trial within a minute");
    }
}

fn paused(effects: &[Effect]) -> bool {
    effects.iter().any(|e| matches!(e, Effect::Paused { .. }))
}

fn resumed(effects: &[Effect]) -> bool {
    effects.iter().any(|e| matches!(e, Effect::Resumed { .. }))
}

/// C1: a full outage begins on a warm window — every Tenant processed for 30 s
/// — and a quarter or a half of the Tenants keep processing on cached keys. The
/// channel pauses once max(32, ⌈S/3⌉) distinct Tenants failed at `Stage` scope,
/// S at most the healthy set's bound, within a bound in asks; then one trial a
/// tick. Red without the broad arm: the share alone never reaches three
/// quarters while half the Tenants process.
#[test]
fn c1_a_full_outage_pauses_within_a_bound_while_cached_keys_flow() {
    for cached in [25u64, 50] {
        let mut sim = Sim::new(Breaking::default());
        let mut rng = Rng(7 + cached);
        for _ in 0..3_000 {
            sim.advance(Duration::from_millis(10));
            let t = rng.below(2_000);
            sim.ask(&format!("t{t}"), (t % 64) as i16, Verdict::Processed);
        }
        // A third of the healthy set's bound fail before it pauses, and no
        // more: F ≥ (F + S) / 4 with S at most 1,024.
        let threshold = 1_024usize.div_ceil(3) + 1;
        let mut failed = std::collections::HashSet::new();
        let mut asks = 0;
        loop {
            let t = rng.below(2_000);
            let verdict = if t % 100 < cached {
                Verdict::Processed
            } else {
                stage()
            };
            sim.advance(Duration::from_millis(10));
            if let Some(told) = sim.ask(&format!("t{t}"), (t % 64) as i16, verdict) {
                asks += 1;
                if verdict != Verdict::Processed {
                    failed.insert(t);
                }
                if paused(&told.effects) {
                    break;
                }
            }
            assert!(
                asks <= 4_000,
                "{cached} % cached: still closed after {asks} asks"
            );
        }
        assert!(
            failed.len() <= threshold,
            "{cached} % cached: {} Tenants failed before the pause, over {threshold}",
            failed.len()
        );
        // Paused: ten minutes of trials, each told the stage is down.
        let mut trials = 0;
        for _ in 0..6_000 {
            sim.advance(Duration::from_millis(100));
            for effect in sim.tick() {
                if let Effect::Trial { id, .. } = effect {
                    trials += 1;
                    sim.breaker.tried(sim.now, id, Trial::Told(stage()));
                }
            }
        }
        assert!(
            (30..=45).contains(&trials),
            "{cached} % cached: {trials} trials in ten minutes, a tick doubling to 15 s"
        );
    }
}

/// C2: a brownout of 1 % or 5 % fast failures, each read `Stage` (no other
/// key landed in the instant since its trouble began), at 100 asks a second
/// over 2,000 Tenants, never pauses. Red with the broad arm and no share of
/// its own: some 150 distinct Tenants fail within the window.
#[test]
fn c2_a_brownout_never_pauses() {
    for failing in [1u64, 5] {
        let mut sim = Sim::new(Breaking::default());
        let mut rng = Rng(11 + failing);
        for _ in 0..30_000 {
            sim.advance(Duration::from_millis(10));
            let t = rng.below(2_000);
            let verdict = if rng.below(100) < failing {
                stage()
            } else {
                Verdict::Processed
            };
            if let Some(told) = sim.ask(&format!("t{t}"), (t % 64) as i16, verdict) {
                assert!(
                    !paused(&told.effects),
                    "{failing} % brownout paused the channel"
                );
            }
        }
    }
}

/// C3: one Tenant throttled with `retry_after` 60 s on a quiet channel, its 64
/// heads met by eight loops every 100 ms for ten minutes, never pauses the
/// channel; it is asked about once a minute; another Tenant arriving at five
/// minutes is asked at once. Red with one Tenant enough to open (`fewest` 1).
#[test]
fn c3_one_tenant_throttled_on_a_quiet_channel_never_pauses_it() {
    let mut sim = Sim::new(Breaking::default());
    let throttled = Verdict::Unavailable {
        cause: cause("kms.unavailable"),
        scope: Scope::Stage,
        retry_after: Some(Duration::from_secs(60)),
    };
    let mut asks = 0;
    for step in 0..6_000u32 {
        sim.advance(Duration::from_millis(100));
        for slot in 0..8i16 {
            let head = ((step * 8 + slot as u32) % 64) as i16;
            if let Some(told) = sim.ask("x", head, throttled) {
                asks += 1;
                assert!(!paused(&told.effects), "one Tenant paused the channel");
            }
        }
        if step == 3_000 {
            assert!(
                matches!(
                    sim.breaker.before(sim.now, CHANNEL, "y", 3, 0),
                    Before::Ask(_)
                ),
                "another Tenant is asked at once"
            );
        }
    }
    assert!(
        asks <= 600 / 60 + 2,
        "the throttled Tenant asked {asks} times in ten minutes"
    );
}

/// C4: 70 % of four times the memo's capacity of Tenants failing at `Tenant`
/// scope for 30 minutes: no pause, a healthy Tenant never held, and each head
/// asked no more than log2(cap/floor) + t/cap + c times, past the memo's
/// capacity too. Red with the exponent from the memo only (a Tenant the full
/// memo does not keep is asked at every floor).
#[test]
fn c4_a_partial_failure_at_tenant_scope_paces_each_tenant_and_pauses_nothing() {
    let settings = Breaking {
        memo: 16,
        ..Breaking::default()
    };
    let mut sim = Sim::new(settings);
    let mut rng = Rng(23);
    let tenants = 64u64;
    let mut asks: HashMap<(u64, i16), u32> = HashMap::new();
    for _ in 0..180_000 {
        sim.advance(Duration::from_millis(10));
        let t = rng.below(tenants);
        let slot = rng.below(8) as i16;
        let failing = t % 10 < 7;
        let verdict = if failing {
            tenant_scope(None)
        } else {
            Verdict::Processed
        };
        match sim.ask(&format!("t{t}"), slot, verdict) {
            Some(told) => {
                assert!(
                    !paused(&told.effects),
                    "a Tenant-scoped failure paused the channel"
                );
                if failing {
                    *asks.entry((t, slot)).or_default() += 1;
                }
            }
            None => assert!(failing, "healthy Tenant t{t} held or not due"),
        }
    }
    let bound = (300f64).log2().ceil() as u32 + 1_800 / 300 + 3;
    let most = asks.values().copied().max().unwrap_or(0);
    assert!(
        most <= bound,
        "a failing head asked {most} times, over {bound}"
    );
}

/// C5: one ask in flight a (channel, Tenant): while a failing Tenant's ask
/// hangs, its other heads are held, not asked; a healthy Tenant is never
/// serialised; an abandoned ask releases its mark, and so does its deadline.
/// Red without the in-flight mark.
#[test]
fn c5_a_hanging_tenant_is_asked_once_at_a_time() {
    let mut sim = Sim::new(Breaking::default());
    sim.ask("x", 0, tenant_scope(None));
    sim.advance(Duration::from_secs(3));
    let Before::Ask(flight) = sim.breaker.before(sim.now, CHANNEL, "x", 1, 0) else {
        panic!("its back-off has run out: asked");
    };
    for slot in 2..64 {
        assert!(
            matches!(
                sim.breaker.before(sim.now, CHANNEL, "x", slot, 0),
                Before::Hold { .. }
            ),
            "head {slot} asked while another is in flight"
        );
    }
    sim.breaker.abandoned(&flight);
    assert!(matches!(
        sim.breaker.before(sim.now, CHANNEL, "x", 2, 0),
        Before::Ask(_)
    ));
    // A mark past its deadline is void.
    sim.advance(Duration::from_secs(31));
    assert!(matches!(
        sim.breaker.before(sim.now, CHANNEL, "x", 3, 0),
        Before::Ask(_)
    ));
    // A healthy Tenant: two asks at once.
    let first = sim.breaker.before(sim.now, CHANNEL, "y", 0, 0);
    let second = sim.breaker.before(sim.now, CHANNEL, "y", 1, 0);
    assert!(matches!((first, second), (Before::Ask(_), Before::Ask(_))));
}

/// C6: recovered, every row now fails on its own: three inconclusive trials on
/// distinct witnesses close the channel — two memoised Tenants and a third
/// whose head a guest met. Red when only a processed trial closes.
#[test]
fn c6_three_inconclusive_trials_close_the_channel() {
    let mut sim = Sim::new(Breaking::default());
    sim.open();
    sim.advance(Duration::from_secs(5));
    let (first, a) = sim.next_trial();
    let told = sim.breaker.tried(sim.now, first, Trial::Told(Verdict::Own));
    assert!(!resumed(&told.effects));
    sim.advance(Duration::from_secs(5));
    let (second, b) = sim.next_trial();
    assert_ne!(a, b, "a witness tried twice");
    let told = sim.breaker.tried(sim.now, second, Trial::Gone);
    assert!(!resumed(&told.effects));
    sim.advance(Duration::from_secs(5));
    sim.breaker.met(sim.now, CHANNEL, &[(9, "c".to_owned())]);
    let effects = sim.tick();
    let third = effects
        .iter()
        .find_map(|e| match e {
            Effect::Trial { id, tenant, .. } if tenant == "c" => Some(*id),
            _ => None,
        })
        .expect("the head met is tried");
    let told = sim.breaker.tried(sim.now, third, Trial::TimedOut);
    assert!(
        resumed(&told.effects),
        "three inconclusive trials did not close it"
    );
    assert!(!sim.breaker.is_paused(CHANNEL));
}

/// C7: paused, the heads of both its memoised Tenants given up by a person: a
/// trial of each finds its head gone, two inconclusive verdicts of the three
/// that close a channel; the next tick finds no witness and none to wait for,
/// and closes it — a process's guests cannot see a whole circle of a slot,
/// their cursor shared — and no stall is refreshed after. Red when a tick
/// without a witness only waits for the next.
#[test]
fn c7_no_witness_left_closes_the_channel() {
    let mut sim = Sim::new(Breaking::default());
    sim.open();
    let mut effects = Vec::new();
    for _ in 0..2 {
        sim.advance(Duration::from_secs(5));
        let (id, _) = sim.next_trial();
        effects.extend(sim.breaker.tried(sim.now, id, Trial::Gone).effects);
    }
    assert!(!resumed(&effects), "two inconclusive trials closed it");
    sim.advance(Duration::from_secs(5));
    effects.extend(sim.tick());
    assert!(resumed(&effects), "no witness left: {effects:?}");
    for _ in 0..100 {
        sim.advance(Duration::from_secs(1));
        assert!(
            !sim.tick()
                .iter()
                .any(|e| matches!(e, Effect::Stalled { .. })),
            "a closed channel's stall refreshed"
        );
    }
}

/// C8: a `Stage`-scoped hold on a row is at most the tick's cap, whatever the
/// row has counted, so a process cut off alone delays another's head by a
/// tick at most; a `Tenant`-scoped one runs to the Tenant's back-off. Red with
/// the memo's clock written to the row for a `Stage` scope.
#[test]
fn c8_a_stage_hold_on_the_row_is_at_most_a_tick() {
    let mut breaker = Breaker::new(Breaking::default());
    let now = Instant::now();
    let Before::Ask(flight) = breaker.before(now, CHANNEL, "x", 0, 20) else {
        panic!("asked");
    };
    let told = breaker.told(now, flight, stage());
    assert_eq!(told.hold, Some(Duration::from_secs(15)));
    let Before::Ask(flight) = breaker.before(now, CHANNEL, "y", 0, 20) else {
        panic!("asked");
    };
    let told = breaker.told(now, flight, tenant_scope(None));
    assert_eq!(told.hold, Some(Duration::from_secs(300)));
}

/// C9: a trial answered after its deadline counted it changes nothing but a
/// close: a late `Unavailable` adds no failed tick (no stall from it), a late
/// processed answer still closes. Red when a stale answer is counted.
#[test]
fn c9_a_late_trial_answer_changes_nothing_but_a_close() {
    let mut sim = Sim::new(Breaking::default());
    sim.open();
    sim.advance(Duration::from_secs(5));
    let (first, _) = sim.next_trial();
    sim.advance(Duration::from_secs(10));
    sim.tick();
    let late = sim.breaker.tried(sim.now, first, Trial::Told(stage()));
    assert_eq!(
        late,
        Told::default(),
        "a late Unavailable changed something"
    );
    sim.advance(Duration::from_secs(20));
    let (second, _) = sim.next_trial();
    sim.advance(Duration::from_secs(10));
    sim.tick();
    let late = sim
        .breaker
        .tried(sim.now, second, Trial::Told(Verdict::Processed));
    assert!(
        resumed(&late.effects),
        "a late processed answer did not close"
    );
}

/// C10: a sparse channel: a guest meets the one head of a Tenant the memo does
/// not hold, and it is tried at once — at the next call's tick, ahead of the
/// tick the failed trials scheduled — the channel closed on its success. Red
/// when the trial waits for the scheduled tick or a memoised witness.
#[test]
fn c10_a_witness_met_by_a_guest_is_tried_at_once() {
    let mut sim = Sim::new(Breaking::default());
    sim.open();
    // a first trial, of a memoised witness, fails: the next tick is two
    // seconds on
    let (first, _) = sim.next_trial();
    sim.breaker.tried(sim.now, first, Trial::Told(stage()));
    sim.advance(Duration::from_millis(1100));
    assert!(
        !sim.tick().iter().any(|e| matches!(e, Effect::Trial { .. })),
        "nothing tried before the tick scheduled"
    );
    // the meeting issues no trial itself: the next tick does, now
    sim.breaker
        .met(sim.now, CHANNEL, &[(17, "lonely".to_owned())]);
    let effects = sim.tick();
    let Some((id, tenant)) = effects.iter().find_map(|e| match e {
        Effect::Trial { id, tenant, .. } => Some((*id, tenant.clone())),
        _ => None,
    }) else {
        panic!("no trial of the head met: {effects:?}");
    };
    assert_eq!(tenant, "lonely");
    let told = sim
        .breaker
        .tried(sim.now, id, Trial::Told(Verdict::Processed));
    assert!(resumed(&told.effects));
}

/// C11: mixed causes: the pause's cause is the modal one among the `Stage`
/// verdicts that opened it, and the stall keeps it whatever the trials say.
/// Two Tenants processed first, so that several causes gather before the
/// share is reached. Red with the least frequent cause.
#[test]
fn c11_a_pause_keeps_its_modal_cause() {
    let mut sim = Sim::new(Breaking::default());
    let unauthorized = Verdict::Unavailable {
        cause: cause("kms.unauthorized"),
        scope: Scope::Stage,
        retry_after: None,
    };
    sim.ask("p", 0, Verdict::Processed);
    sim.ask("q", 0, Verdict::Processed);
    sim.ask("a", 0, stage());
    sim.ask("b", 0, unauthorized);
    for tenant in ["c", "d", "e", "f"] {
        sim.ask(tenant, 0, stage());
    }
    assert!(sim.breaker.is_paused(CHANNEL), "six of eight failing");
    let mut causes = Vec::new();
    for _ in 0..600 {
        sim.advance(Duration::from_millis(500));
        for effect in sim.tick() {
            match effect {
                Effect::Trial { id, .. } => {
                    sim.breaker.tried(sim.now, id, Trial::Told(unauthorized));
                }
                Effect::Stalled { cause, .. } => causes.push(cause),
                _ => {}
            }
        }
    }
    assert!(!causes.is_empty(), "never stalled");
    assert!(
        causes
            .iter()
            .all(|c| *c == super::Cause::new("kms.unavailable").unwrap())
    );
}

/// C12: a trial never answered is a failed tick at its deadline, and the next
/// tick asks again. Red when an unanswered trial stays in flight.
#[test]
fn c12_an_unanswered_trial_is_asked_again() {
    let mut sim = Sim::new(Breaking::default());
    sim.open();
    sim.advance(Duration::from_secs(5));
    let (first, _) = sim.next_trial();
    let (second, _) = sim.next_trial();
    assert_ne!(
        first, second,
        "the unanswered trial was not followed by another"
    );
}

/// C14: a channel opened again within the window after a stall is still
/// stalled, and says so at its opening, not at its first tick, which the
/// failed ticks it kept put up to the cap away: a stage that comes back for
/// less than the stall's ttl keeps its report valid across the gap (ADR-0042
/// decision 6). Red when a reopened stall waits for its first tick.
#[test]
fn c14_a_stall_opened_again_within_the_window_is_reported_at_its_opening() {
    let mut sim = Sim::new(Breaking::default());
    sim.open();
    let mut stalled = false;
    for _ in 0..4 {
        sim.advance(Duration::from_secs(1));
        let (id, _) = sim.next_trial();
        let told = sim.breaker.tried(sim.now, id, Trial::Told(stage()));
        stalled |= told
            .effects
            .iter()
            .any(|e| matches!(e, Effect::Stalled { .. }));
    }
    assert!(stalled, "failed trials stall the channel");
    sim.advance(Duration::from_secs(1));
    let (id, _) = sim.next_trial();
    let told = sim
        .breaker
        .tried(sim.now, id, Trial::Told(Verdict::Processed));
    assert!(resumed(&told.effects), "a processed trial resumes it");
    // back for 20 s, within the 30 s window, and down again
    sim.advance(Duration::from_secs(20));
    let mut effects = Vec::new();
    for tenant in ["a", "b", "c"] {
        if let Some(told) = sim.ask(tenant, 0, stage()) {
            effects.extend(told.effects);
        }
    }
    assert!(paused(&effects), "opened again: {effects:?}");
    assert!(
        effects.iter().any(|e| matches!(e, Effect::Stalled { .. })),
        "the stall said at the opening: {effects:?}"
    );
}

/// Opens `channel` in `breaker`: two Tenants failing at `Stage` scope.
fn open_channel(breaker: &mut Breaker, now: Instant, channel: &str) {
    for tenant in ["a", "b"] {
        let Before::Ask(flight) = breaker.before(now, channel, tenant, 0, 0) else {
            panic!("a closed channel asks a Tenant without a memo");
        };
        breaker.told(now, flight, stage());
    }
    assert!(breaker.is_paused(channel), "{channel}: two of two failing");
}

/// C15: guests meet a fresh Tenant's head every 200 ms through ten minutes of
/// a stage down (ADR-0042 decision 1, its trials' haste): a head met hastens
/// a trial at most once a tick, so the trials are at most twice the scheduled
/// ones (C1's 30–45), not one a second. Red when a head met hastens a trial
/// whenever a second has passed since the last.
#[test]
fn c15_heads_met_add_at_most_one_trial_a_tick() {
    let mut sim = Sim::new(Breaking::default());
    sim.open();
    let mut trials = 0;
    for step in 0..3_000u32 {
        sim.advance(Duration::from_millis(200));
        let fresh = format!("guest{step}");
        sim.breaker
            .met(sim.now, CHANNEL, &[((step % 64) as i16, fresh)]);
        for effect in sim.tick() {
            if let Effect::Trial { id, .. } = effect {
                trials += 1;
                sim.breaker.tried(sim.now, id, Trial::Told(stage()));
            }
        }
    }
    assert!(
        (30..=90).contains(&trials),
        "{trials} trials in ten minutes of heads met every 200 ms"
    );
}

/// C16: one trial a tick call, whatever the channels due (a dispatch runs
/// one; another issued would expire unasked as a failed tick), the one due
/// longest first; and under one caller, twenty channels stalled each report
/// their stall within its ttl, though each waits its turn for a trial of two
/// seconds. Red with a trial for every channel due, or with a stall reported
/// only at its channel's own trial.
#[test]
fn c16_one_trial_a_tick_call_and_every_stall_refreshed() {
    let settings = Breaking::default();
    let mut breaker = Breaker::new(settings);
    let start = Instant::now();
    let names: Vec<String> = (0..20).map(|i| format!("kafka://c{i:02}")).collect();
    for name in &names {
        open_channel(&mut breaker, start, name);
    }
    let mut now = start + Duration::from_secs(5);
    let first: Vec<u64> = breaker
        .tick(now)
        .into_iter()
        .filter_map(|e| match e {
            Effect::Trial { id, .. } => Some(id),
            _ => None,
        })
        .collect();
    assert_eq!(first.len(), 1, "one trial a call, of twenty channels due");
    breaker.tried(now, first[0], Trial::Told(stage()));
    let second = breaker.tick(now);
    assert_eq!(
        second
            .iter()
            .filter(|e| matches!(e, Effect::Trial { .. }))
            .count(),
        1,
        "the next call asks the next channel's"
    );
    for effect in second {
        if let Effect::Trial { id, .. } = effect {
            breaker.tried(now, id, Trial::Told(stage()));
        }
    }
    // one caller, each trial two seconds, for ten minutes
    let mut last: HashMap<String, Instant> = HashMap::new();
    let ttl = settings.stall_ttl();
    for _ in 0..300 {
        now += Duration::from_secs(2);
        let effects = breaker.tick(now);
        for effect in &effects {
            if let Effect::Stalled { channel, .. } = effect {
                if let Some(at) = last.insert(channel.clone(), now) {
                    assert!(
                        now - at <= ttl,
                        "{channel}'s stall unreported for {:?}, past its ttl {ttl:?}",
                        now - at
                    );
                }
            }
        }
        for effect in effects {
            if let Effect::Trial { id, .. } = effect {
                breaker.tried(now, id, Trial::Told(stage()));
            }
        }
    }
    assert_eq!(last.len(), names.len(), "every channel stalled");
}

/// C17: a trial whose slot a dispatch held, or whose transaction failed,
/// asked nothing: it is no inconclusive verdict and no failed tick, and its
/// witness is tried again at the tick's first step. Red when a busy slot
/// counts as a head gone — three busy slots on distinct witnesses would close
/// a channel whose stage is down.
#[test]
fn c17_a_busy_slot_asks_nothing_and_counts_nothing() {
    let mut sim = Sim::new(Breaking::default());
    sim.open();
    sim.advance(Duration::from_secs(5));
    // one failed tick: the schedule's next step is two seconds, the busy
    // witness's one
    let (id, _) = sim.next_trial();
    sim.breaker.tried(sim.now, id, Trial::Told(stage()));
    let met: Vec<(i16, String)> = ["p", "q", "r", "s"]
        .iter()
        .enumerate()
        .map(|(i, t)| (i as i16, (*t).to_owned()))
        .collect();
    sim.breaker.met(sim.now, CHANNEL, &met);
    let (id, first) = sim.next_trial();
    sim.breaker.tried(sim.now, id, Trial::Busy);
    for _ in 0..8 {
        let before = sim.now;
        let (id, tenant) = sim.next_trial();
        assert_eq!(tenant, first, "the busy witness tried again, first in line");
        assert!(
            sim.now - before <= Duration::from_millis(1_100),
            "tried again {:?} later",
            sim.now - before
        );
        let told = sim.breaker.tried(sim.now, id, Trial::Busy);
        assert!(!resumed(&told.effects), "busy slots closed the channel");
    }
    assert!(sim.breaker.is_paused(CHANNEL), "still paused");
    // busy trials counted no failed tick: one failed tick so far, none
    // stalled the channel
    for _ in 0..50 {
        sim.advance(Duration::from_millis(100));
        for effect in sim.tick() {
            assert!(
                !matches!(effect, Effect::Stalled { .. }),
                "a busy slot counted a failed tick"
            );
            if let Effect::Trial { id, .. } = effect {
                sim.breaker.tried(sim.now, id, Trial::Busy);
            }
        }
    }
}

proptest! {
    /// C13: a memoised Tenant is held until its back-off has run out, however
    /// many other Tenants fail meanwhile and fill the memo: an entry in flight
    /// or not run out is never evicted. Red with a plain LRU.
    #[test]
    fn c13_the_memo_never_evicts_a_protected_entry(
        steps in proptest::collection::vec((0u64..200, 0u8..16, 0u8..3), 1..400)
    ) {
        let settings = Breaking { memo: 4, ..Breaking::default() };
        let mut breaker = Breaker::new(settings);
        let mut now = Instant::now();
        let mut held_until: HashMap<String, Instant> = HashMap::new();
        for (millis, tenant, kind) in steps {
            now += Duration::from_millis(millis);
            let tenant = format!("t{tenant}");
            let answer = breaker.before(now, CHANNEL, &tenant, 0, 0);
            if let Some(until) = held_until.get(&tenant) {
                if now < *until {
                    prop_assert!(matches!(answer, Before::Hold { .. }), "{tenant} evicted before {until:?}");
                    continue;
                }
            }
            match answer {
                Before::Hold { wait, .. } => {
                    held_until.insert(tenant, now + wait);
                }
                Before::Ask(flight) => {
                    let verdict = match kind {
                        0 => Verdict::Processed,
                        1 => Verdict::Own,
                        _ => tenant_scope(None),
                    };
                    breaker.told(now, flight, verdict);
                    held_until.remove(&tenant);
                }
            }
        }
    }
}

/// M1 (ADR-0042 § Executors, R9): the breaker's work in the process for a
/// happy message, as a dispatch does it — a tick and the paused channels,
/// then before and told, each under the inbox's mutex — over seven channels
/// and 2,000 Tenants, a million messages. A measurement, run by hand on an
/// optimised build:
///
/// ```text
/// ASCETIC_DDD_MEASURE=1 cargo test --release -p ascetic-ddd-inbox --lib -- \
///     --ignored --nocapture m1_the_breaker
/// ```
#[test]
#[ignore = "a measurement (ADR-0042 M1): run by hand"]
fn m1_the_breaker_s_work_for_a_happy_message() {
    if std::env::var_os("ASCETIC_DDD_MEASURE").is_none() {
        return;
    }
    const MESSAGES: usize = 1_000_000;
    let channels: Vec<String> = (0..7).map(|c| format!("kafka://channel-{c}")).collect();
    let tenants: Vec<String> = (0..2_000).map(|t| format!("t{t:04}")).collect();
    let breaker = std::sync::Mutex::new(Breaker::new(Breaking::default()));
    let lock = || {
        breaker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    };
    let mut now = Instant::now();
    let started = Instant::now();
    let mut asked = 0usize;
    for i in 0..MESSAGES {
        now += Duration::from_micros(500);
        let paused = {
            let mut breaker = lock();
            let effects = breaker.tick(now);
            std::hint::black_box(&effects);
            breaker.paused()
        };
        std::hint::black_box(&paused);
        let channel = &channels[i % channels.len()];
        let tenant = &tenants[(i / channels.len()) % tenants.len()];
        let before = lock().before(now, channel, tenant, (i % 64) as i16, 0);
        if let Before::Ask(flight) = before {
            asked += 1;
            let told = lock().told(now, flight, Verdict::Processed);
            std::hint::black_box(&told);
        }
    }
    let elapsed = started.elapsed();
    assert_eq!(asked, MESSAGES, "a healthy message is always asked");
    println!(
        "M1, the breaker: {:.3} µs a happy message ({MESSAGES} messages in {elapsed:?})",
        elapsed.as_secs_f64() * 1e6 / MESSAGES as f64
    );
}
