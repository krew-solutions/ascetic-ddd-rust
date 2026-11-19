//! The scenario catalogue of ADR-0042 (C1–C13), on the pure core: each test
//! names the mutation that turns it red.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use ascetic_ddd_bus::{Cause, Scope};
use proptest::prelude::*;

use ascetic_ddd_bus::Check;

use super::{Before, Breaker, Breaking, Effect, Flight, Source, Told, Trial, Verdict};

const CHANNEL: &str = "kafka://device.lifecycle";

fn cause(name: &'static str) -> Cause {
    Cause::new(name).expect("a valid cause")
}

fn stage() -> Verdict {
    Verdict::Unavailable {
        cause: cause("kms.unavailable"),
        scope: Scope::Stage,
        retry_after: None,
        source: Source::Rows,
    }
}

fn tenant_scope(retry_after: Option<Duration>) -> Verdict {
    Verdict::Unavailable {
        cause: cause("kms.unavailable"),
        scope: Scope::Tenant,
        retry_after,
        source: Source::Rows,
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
            // kept from the walk to this process: nothing written, the row
            // due for every process as it was
            Before::Pass => None,
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
        source: Source::Rows,
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

/// C6: recovered, every row now fails on its own or cannot be asked: three
/// inconclusive trials on distinct witnesses close the channel — two memoised
/// Tenants, the second's row unreadable, and a third whose head a guest met,
/// timed out. Red when only a processed trial closes.
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
    let told = sim.breaker.tried(sim.now, second, Trial::Unreadable);
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
/// trial of each finds its head gone, which counts nothing and tries the
/// witness for the pause; the next tick finds no witness and none to wait for,
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
    assert!(!resumed(&effects), "two heads gone closed it");
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
        source: Source::Rows,
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
            if let Effect::Stalled { channel, .. } = effect
                && let Some(at) = last.insert(channel.clone(), now)
            {
                assert!(
                    now - at <= ttl,
                    "{channel}'s stall unreported for {:?}, past its ttl {ttl:?}",
                    now - at
                );
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

/// The walk's excluded keys recomputed from the memo, as the cache must read:
/// the set of `"<channel>/<tenant>"` keys.
fn recomputed(breaker: &Breaker, now: Instant) -> std::collections::BTreeSet<String> {
    let mut keys = std::collections::BTreeSet::new();
    for (name, channel) in &breaker.channels {
        if matches!(channel.state, super::State::Open(_)) {
            continue;
        }
        for (tenant, entry) in &channel.memo {
            if entry.excludes(now) {
                keys.insert(format!("{name}/{tenant}"));
            }
        }
    }
    keys
}

/// The keys of the walk's object, as the server reads it.
fn keys_of(json: &str) -> std::collections::BTreeSet<String> {
    let value: serde_json::Value = serde_json::from_str(json).expect("a JSON object");
    value
        .as_object()
        .expect("an object")
        .iter()
        .map(|(key, one)| {
            assert_eq!(one, &serde_json::json!(1));
            key.clone()
        })
        .collect()
}

/// C19: a head of a Tenant the memo holds at the stage's scope, met while it
/// holds it, is passed and nothing written — one process's evidence stays in
/// the process; at the Tenant's scope it is held to its back-off, written on
/// the row for every process. Red when a stage-scoped entry holds the head.
#[test]
fn c19_a_stage_scoped_entry_passes_its_heads_a_tenant_scoped_one_holds_them() {
    let mut breaker = Breaker::new(Breaking::default());
    let now = Instant::now();
    for (tenant, verdict) in [("s", stage()), ("t", tenant_scope(None))] {
        let Before::Ask(flight) = breaker.before(now, CHANNEL, tenant, 0, 0) else {
            panic!("asked");
        };
        breaker.told(now, flight, verdict);
    }
    assert_eq!(breaker.before(now, CHANNEL, "s", 1, 0), Before::Pass);
    assert!(matches!(
        breaker.before(now, CHANNEL, "t", 1, 0),
        Before::Hold { .. }
    ));
    let json = breaker.excluded(now);
    assert_eq!(
        keys_of(&json),
        [format!("{CHANNEL}/s")].into_iter().collect()
    );
    // keys built without s, by a change missed: its head met is passed, and
    // the object rebuilt by it
    breaker.keys = Some(super::Keys {
        json: "{}".into(),
        until: None,
    });
    assert_eq!(breaker.before(now, CHANNEL, "s", 2, 0), Before::Pass);
    assert_eq!(
        keys_of(&breaker.excluded(now)),
        [format!("{CHANNEL}/s")].into_iter().collect()
    );
    // a head of a channel paused after the walk began: passed, not held
    let mut paused = Breaker::new(Breaking::default());
    paused_with(&mut paused, now);
    assert_eq!(paused.before(now, CHANNEL, "z", 0, 0), Before::Pass);
}

/// C21: a Tenant asked twice at once, in two slots, before it had an entry;
/// one ask refused at the stage's scope with another Tenant's pauses the
/// channel, the other still in flight; both Tenants' trials find their heads
/// gone, renewing their entries' back-off; the late ask is refused after,
/// renewing its Tenant's entry again while the channel is paused; the next
/// tick finds no witness and none to wait for, and closes the channel — whose
/// renewed entries the walk must now leave out. Red when a tick's close does
/// not rebuild the walk's keys: the Tenants' heads then met and passed, loop
/// after loop, for their back-off.
#[test]
fn c21_a_close_after_a_late_refusal_rebuilds_the_walk_s_keys() {
    let mut breaker = Breaker::new(Breaking::default());
    let mut now = Instant::now();
    let ask = |breaker: &mut Breaker, now: Instant, tenant: &str, slot: i16| {
        let Before::Ask(flight) = breaker.before(now, CHANNEL, tenant, slot, 0) else {
            panic!("{tenant} asked");
        };
        flight
    };
    let early = ask(&mut breaker, now, "a", 0);
    let late = ask(&mut breaker, now, "a", 1);
    let other = ask(&mut breaker, now, "b", 2);
    breaker.told(now, early, stage());
    breaker.told(now, other, stage());
    assert!(breaker.is_paused(CHANNEL), "two Tenants failing pause it");
    for _ in 0..2 {
        now += Duration::from_secs(3);
        let trial = breaker
            .tick(now)
            .into_iter()
            .find_map(|e| match e {
                Effect::Trial { id, .. } => Some(id),
                _ => None,
            })
            .expect("a trial of a memoised witness");
        breaker.tried(now, trial, Trial::Gone);
    }
    assert!(breaker.is_paused(CHANNEL), "two heads gone close nothing");
    breaker.told(now, late, stage());
    assert_eq!(
        &*breaker.excluded(now),
        "{}",
        "a paused channel's Tenants are not keys"
    );
    now += Duration::from_secs(1);
    let effects = breaker.tick(now);
    assert!(resumed(&effects), "no witness left: {effects:?}");
    assert_eq!(
        keys_of(&breaker.excluded(now)),
        [format!("{CHANNEL}/a"), format!("{CHANNEL}/b")]
            .into_iter()
            .collect(),
        "the renewed entries keep their Tenants from the walk"
    );
}

/// C31: paused, a memoised witness's head gone — its row finished by another
/// process — counts nothing: two inconclusive trials after it do not close the
/// channel; the next witness is asked at the tick's first step, not the failed
/// ticks' doubled one; and the entry's back-off is renewed, so that its Tenant
/// stays out of this process's walk once the channel closes. Red when a head
/// gone counts as inconclusive, waits the scheduled tick, or leaves the entry's
/// back-off as it was.
#[test]
fn c31_a_head_gone_counts_nothing_and_renews_its_entry() {
    // a Tenant's first step 20 s: the renewed back-off outlives the pause
    let settings = Breaking::default().with_hold(Duration::from_secs(10), Duration::from_secs(300));
    let mut breaker = Breaker::new(settings);
    let mut now = Instant::now();
    let flights: Vec<Flight> = ["a", "b", "c"]
        .into_iter()
        .enumerate()
        .map(
            |(slot, tenant)| match breaker.before(now, CHANNEL, tenant, slot as i16, 0) {
                Before::Ask(flight) => flight,
                other => panic!("{tenant} asked: {other:?}"),
            },
        )
        .collect();
    for flight in flights {
        breaker.told(now, flight, stage());
    }
    assert!(breaker.is_paused(CHANNEL), "three Tenants failing pause it");
    let trial = |breaker: &mut Breaker, now: Instant| {
        breaker.tick(now).into_iter().find_map(|e| match e {
            Effect::Trial { id, tenant, .. } => Some((id, tenant)),
            _ => None,
        })
    };
    now += Duration::from_secs(20);
    let (id, a) = trial(&mut breaker, now).expect("a memoised witness, due");
    assert_eq!(a, "a", "the earliest due, ties by the Tenant");
    breaker.tried(now, id, Trial::Told(stage()));
    now += Duration::from_secs(2);
    let (id, b) = trial(&mut breaker, now).expect("the next at the doubled step");
    breaker.tried(now, id, Trial::Gone);
    now += Duration::from_secs(1);
    let (id, third) = trial(&mut breaker, now).expect("after a head gone, the first step");
    assert_ne!(third, b, "a witness whose head was gone tried again");
    let told = breaker.tried(now, id, Trial::Told(Verdict::Own));
    assert!(!resumed(&told.effects));
    now += Duration::from_secs(1);
    breaker.met(now, CHANNEL, &[(9, "m".to_owned())]);
    let (id, m) = trial(&mut breaker, now).expect("the head met, hastened");
    assert_eq!(m, "m");
    let told = breaker.tried(now, id, Trial::Told(Verdict::Own));
    assert!(
        !resumed(&told.effects),
        "a head gone counted as an inconclusive trial"
    );
    let mut closed = false;
    for _ in 0..30 {
        now += Duration::from_secs(1);
        if let Some((id, tenant)) = trial(&mut breaker, now) {
            assert!(
                ![b.as_str(), third.as_str(), "m"].contains(&tenant.as_str()),
                "the one witness left untried, not {tenant}"
            );
            closed = resumed(
                &breaker
                    .tried(now, id, Trial::Told(Verdict::Processed))
                    .effects,
            );
            break;
        }
    }
    assert!(closed, "a processed trial closes it");
    assert_eq!(
        keys_of(&breaker.excluded(now)),
        [format!("{CHANNEL}/{b}")].into_iter().collect(),
        "the gone witness's entry, renewed, keeps its Tenant from the walk"
    );
}

/// Ticks a paused channel through three inconclusive trials — two memoised
/// witnesses' and `met`'s, a head a guest met — until it closes.
fn inconclusive_close(sim: &mut Sim, met: &str) {
    for _ in 0..2 {
        let (id, _) = sim.next_trial();
        let told = sim.breaker.tried(sim.now, id, Trial::TimedOut);
        assert!(!resumed(&told.effects));
    }
    sim.breaker.met(sim.now, CHANNEL, &[(9, met.to_owned())]);
    let (id, tenant) = sim.next_trial();
    assert_eq!(tenant, met, "the head met is tried");
    let told = sim.breaker.tried(sim.now, id, Trial::TimedOut);
    assert!(resumed(&told.effects), "three inconclusive trials close it");
}

/// C32: a handler that waits on a hanging dependency past the trial's wait:
/// its pauses close by three trials timed out, and its next refusals come a
/// client's timeout later, past the window. On probation, each pause counts a
/// failed tick at its opening: the third is stalled, and its report is kept
/// though closed — until a row of the channel is processed, which ends the
/// probation. Red when the failed ticks are reset by the window's horizon
/// rather than kept by probation, when a stall on probation is not refreshed
/// while closed, or when a processed row does not end it.
#[test]
fn c32_a_handler_s_hanging_dependency_stalls_on_probation() {
    let mut sim = Sim::new(Breaking::default());
    sim.open();
    inconclusive_close(&mut sim, "c");
    let window = Breaking::default().window;
    let mut stalled_at_opening = Vec::new();
    for (pause, tenants) in [["d", "e"], ["g", "h"]].iter().enumerate() {
        sim.advance(window + Duration::from_millis(1));
        let mut effects = Vec::new();
        for tenant in tenants {
            effects.extend(sim.ask(tenant, 0, stage()).expect("asked").effects);
        }
        assert!(
            paused(&effects),
            "pause {} on probation: {effects:?}",
            pause + 2
        );
        stalled_at_opening.push(effects.iter().any(|e| matches!(e, Effect::Stalled { .. })));
        inconclusive_close(&mut sim, &format!("m{pause}"));
    }
    assert_eq!(
        stalled_at_opening,
        [false, true],
        "the third pause on probation is stalled at its opening"
    );
    let mut refreshed = 0;
    for _ in 0..20 {
        sim.advance(Duration::from_secs(1));
        refreshed += sim
            .tick()
            .iter()
            .filter(|e| matches!(e, Effect::Stalled { .. }))
            .count();
    }
    assert!(
        refreshed > 0,
        "a stall on probation not refreshed while closed"
    );
    sim.ask("z", 3, Verdict::Processed).expect("asked");
    for _ in 0..60 {
        sim.advance(Duration::from_secs(1));
        assert!(
            !sim.tick()
                .iter()
                .any(|e| matches!(e, Effect::Stalled { .. })),
            "a processed row did not end the probation"
        );
    }
}

/// `breaker`'s channel paused: two Tenants failing at the stage's scope.
fn paused_with(breaker: &mut Breaker, now: Instant) {
    for tenant in ["a", "b"] {
        let Before::Ask(flight) = breaker.before(now, CHANNEL, tenant, 0, 0) else {
            panic!("asked");
        };
        breaker.told(now, flight, stage());
    }
    assert!(breaker.is_paused(CHANNEL));
}

/// C20: what the memo keeps from the walk, counted by channel and cause with
/// the oldest run's start, never a Tenant; nothing once the back-off ran out.
/// Red when a Tenant-scoped entry is counted, or a run's start is its latest
/// verdict's.
#[test]
fn c20_the_excluded_are_counted_without_their_tenants() {
    let mut breaker = Breaker::new(Breaking::default());
    let start = Instant::now();
    // a warm window: two Tenants failing among ten processed pause nothing
    for healthy in 0..10 {
        let Before::Ask(flight) = breaker.before(start, CHANNEL, &format!("h{healthy}"), 0, 0)
        else {
            panic!("asked");
        };
        breaker.told(start, flight, Verdict::Processed);
    }
    for (at, tenant, verdict) in [
        (0, "a", stage()),
        (2, "b", stage()),
        (2, "t", tenant_scope(None)),
        (3, "a", stage()),
    ] {
        let now = start + Duration::from_secs(at);
        match breaker.before(now, CHANNEL, tenant, 0, 0) {
            Before::Ask(flight) => {
                breaker.told(now, flight, verdict);
            }
            // a's back-off ran out at 2 s: a second refusal renews it
            other => panic!("{tenant} at {at} s: {other:?}"),
        }
    }
    let counts = breaker.excluded_counts(start + Duration::from_secs(3));
    assert_eq!(counts.len(), 1, "{counts:?}");
    assert_eq!(counts[0].channel, CHANNEL);
    assert_eq!(counts[0].tenants, 2, "a and b, not the Tenant-scoped t");
    assert_eq!(counts[0].since, start, "a's run began at its first refusal");
    assert!(
        breaker
            .excluded_counts(start + Duration::from_secs(600))
            .is_empty(),
        "nothing left once the back-offs ran out"
    );
    // a's back-off ran out at 7 s; refused again ten minutes on, its run
    // begins anew
    let later = start + Duration::from_secs(600);
    let Before::Ask(flight) = breaker.before(later, CHANNEL, "a", 0, 0) else {
        panic!("asked");
    };
    breaker.told(later, flight, stage());
    let counts = breaker.excluded_counts(later);
    assert_eq!(
        counts[0].since, later,
        "a run after a quiet window begins anew"
    );
}

// The dependency's breaker (ADR-0042 decision 1): a channel whose stage
// declares what it depends on.

/// A refusal of the dependency's: a stage's, of its cause.
fn dependency_down() -> Verdict {
    Verdict::Unavailable {
        cause: cause("kms.unavailable"),
        scope: Scope::Stage,
        retry_after: None,
        source: Source::Dependency,
    }
}

/// A process's breaker with the dependency of `CHANNEL` registered, a check
/// of it bound at 5 s.
struct Dep {
    breaker: Breaker,
    now: Instant,
    /// Tenants refused so far: each trip refuses fresh ones, a refused one
    /// kept from the process's walk for its back-off.
    refused: u32,
}

impl Dep {
    fn new() -> Self {
        Dep::with(Breaking::default())
    }

    fn with(settings: Breaking) -> Self {
        let mut breaker = Breaker::new(settings);
        breaker.register(CHANNEL, Duration::from_secs(5));
        Dep {
            breaker,
            now: Instant::now(),
            refused: 0,
        }
    }

    fn advance(&mut self, by: Duration) {
        self.now += by;
    }

    fn flight(&mut self, tenant: &str) -> Flight {
        match self.breaker.before(self.now, CHANNEL, tenant, 0, 0) {
            Before::Ask(flight) => flight,
            other => panic!("{tenant} asked: {other:?}"),
        }
    }

    fn ask(&mut self, tenant: &str, verdict: Verdict) -> Vec<Effect> {
        let flight = self.flight(tenant);
        self.breaker.told(self.now, flight, verdict).effects
    }

    fn served(&mut self, value: u64) {
        self.breaker.served(self.now, CHANNEL, value);
    }

    fn tick(&mut self) -> Vec<Effect> {
        self.breaker.tick(self.now)
    }

    /// The check a tick issues now, if any: its id and head.
    fn check(&mut self) -> Option<(u64, Option<(i16, String)>)> {
        self.tick().into_iter().find_map(|e| match e {
            Effect::Check { id, head, .. } => Some((id, head)),
            _ => None,
        })
    }

    /// Ticks until a check is issued, for at most a minute.
    fn next_check(&mut self) -> (u64, Option<(i16, String)>) {
        for _ in 0..600 {
            if let Some(check) = self.check() {
                return check;
            }
            self.advance(Duration::from_millis(100));
        }
        panic!("no check within a minute");
    }

    /// Trips the breadth rule: two Tenants refused by the dependency, nothing
    /// served.
    fn trip(&mut self) {
        for _ in 0..2 {
            self.refused += 1;
            let tenant = format!("f{}", self.refused);
            let effects = self.ask(&tenant, dependency_down());
            assert!(!paused(&effects), "a trip pauses nothing: {effects:?}");
        }
    }

    /// Paused: a trip confirmed by a check unserved.
    fn pause(&mut self) {
        self.trip();
        let (id, _) = self.next_check();
        let effects = self
            .breaker
            .checked(self.now, id, Check::Unserved(cause("kms.unavailable")));
        assert!(paused(&effects), "{effects:?}");
    }
}

fn refuted(effects: &[Effect]) -> bool {
    effects.iter().any(|e| matches!(e, Effect::Refuted { .. }))
}

fn stalled(effects: &[Effect]) -> bool {
    effects.iter().any(|e| matches!(e, Effect::Stalled { .. }))
}

/// C33: the dependency's refusals broad enough make the channel suspect, not
/// paused: a check is issued, and the channel still asked; served, the
/// suspicion is refuted under the refusals' modal cause, its failing Tenants
/// forgotten — one refusal after cannot trip — and two fresh ones right after
/// bring no check within the suspicion's step, doubled. Red when a trip
/// pauses the channel, a refutation keeps the failing Tenants or names
/// another cause, or the step is not kept.
#[test]
fn c33_a_trip_is_a_suspicion_a_check_confirms_or_refutes() {
    let refute = |dep: &mut Dep| {
        dep.trip();
        let effects = dep.ask(
            "o",
            Verdict::Unavailable {
                cause: cause("kms.unauthorized"),
                scope: Scope::Stage,
                retry_after: None,
                source: Source::Dependency,
            },
        );
        assert!(!paused(&effects));
        assert!(!dep.breaker.is_paused(CHANNEL), "a trip pauses nothing");
        let (id, head) = dep.check().expect("a check at once");
        assert_eq!(
            head, None,
            "a suspect channel's check asks what the stage remembers"
        );
        let effects = dep.breaker.checked(dep.now, id, Check::Served);
        assert!(!paused(&effects), "{effects:?}");
        let refuted_under: Vec<Cause> = effects
            .iter()
            .filter_map(|e| match e {
                Effect::Refuted { cause, .. } => Some(*cause),
                _ => None,
            })
            .collect();
        assert_eq!(
            refuted_under,
            [cause("kms.unavailable")],
            "refuted under the modal cause"
        );
    };
    let mut dep = Dep::new();
    refute(&mut dep);
    dep.advance(Duration::from_millis(500));
    dep.ask("c", dependency_down());
    dep.advance(Duration::from_secs(5));
    assert!(
        dep.check().is_none(),
        "one refusal checked: the refuted Tenants kept"
    );
    let mut dep = Dep::new();
    refute(&mut dep);
    dep.advance(Duration::from_millis(500));
    dep.trip();
    for _ in 0..14 {
        assert!(dep.check().is_none(), "within the suspicion's step");
        dep.advance(Duration::from_millis(100));
    }
    dep.advance(Duration::from_millis(100));
    assert!(dep.check().is_some(), "at the step's end, doubled to 2 s");
    assert!(!dep.breaker.is_paused(CHANNEL));
}

/// C34: a check unserved confirms the suspicion: paused, a failed tick; the
/// next check unserved stalls it, its report valid twice the tick's cap and
/// the check's bound. Red when the opening counts no failed tick, or the
/// report's ttl is the rows'.
#[test]
fn c34_an_unserved_check_pauses_and_the_next_stalls() {
    let mut dep = Dep::new();
    dep.pause();
    assert!(dep.breaker.is_paused(CHANNEL));
    let (id, _) = dep.next_check();
    let effects = dep
        .breaker
        .checked(dep.now, id, Check::Unserved(cause("kms.unavailable")));
    let ttl = effects.iter().find_map(|e| match e {
        Effect::Stalled { ttl, .. } => Some(*ttl),
        _ => None,
    });
    assert_eq!(
        ttl,
        Some(Duration::from_secs(35)),
        "the second failed tick stalls, valid 2 × 15 s + 5 s: {effects:?}"
    );
}

/// C35: paused by the dependency, a thousand rows processed — asks begun
/// before the pause, their keys the stage's memory's — close nothing. Red when
/// a row processed closes the dependency's pause.
#[test]
fn c35_rows_processed_from_the_stage_s_memory_close_no_dependency_pause() {
    let mut dep = Dep::new();
    let flights: Vec<Flight> = (0..1_000).map(|n| dep.flight(&format!("c{n}"))).collect();
    dep.pause();
    for flight in flights {
        let effects = dep
            .breaker
            .told(dep.now, flight, Verdict::Processed)
            .effects;
        assert!(!resumed(&effects));
    }
    assert!(
        dep.breaker.is_paused(CHANNEL),
        "a cached row closed the pause"
    );
}

/// C36: paused and stalled, a hundred checks of a head answered unasked hold
/// the pause, and its report is refreshed as each is issued. Red when an
/// unasked check closes it, or a pause that holds reports nothing.
#[test]
fn c36_an_unasked_check_holds_the_pause() {
    let mut dep = Dep::new();
    dep.pause();
    let (id, _) = dep.next_check();
    dep.breaker
        .checked(dep.now, id, Check::Unserved(cause("kms.unavailable")));
    for n in 0..100 {
        dep.breaker.met(dep.now, CHANNEL, &[(1, format!("h{n}"))]);
        let mut issued = None;
        let mut reported = false;
        for _ in 0..600 {
            for effect in dep.tick() {
                match effect {
                    Effect::Check { id, head, .. } => issued = Some((id, head)),
                    Effect::Stalled { .. } => reported = true,
                    _ => {}
                }
            }
            if issued.is_some() {
                break;
            }
            dep.advance(Duration::from_millis(100));
        }
        let (id, head) = issued.expect("a check");
        assert!(head.is_some(), "the head met");
        assert!(reported, "the report refreshed with check {n}");
        let effects = dep.breaker.checked(dep.now, id, Check::Unasked);
        assert!(!resumed(&effects), "an unasked check closed it");
    }
    assert!(dep.breaker.is_paused(CHANNEL));
}

/// C37: paused, its checks with nothing to ask — no head met — for a window:
/// nothing held, closed; a head met meanwhile keeps it. Red when it never
/// closes, or closes with a head met.
#[test]
fn c37_a_pause_with_nothing_to_ask_for_a_window_closes() {
    let mut dep = Dep::new();
    dep.pause();
    let mut by = None;
    for _ in 0..200 {
        let (id, head) = dep.next_check();
        assert_eq!(head, None);
        for effect in dep.breaker.checked(dep.now, id, Check::Unasked) {
            if let Effect::Resumed { by: b, .. } = effect {
                by = Some(b);
            }
        }
        if by.is_some() {
            break;
        }
    }
    assert_eq!(by, Some(super::Resumption::NothingToHold));
    let mut dep = Dep::new();
    dep.pause();
    for n in 0..60 {
        let (id, _) = dep.next_check();
        dep.breaker.met(dep.now, CHANNEL, &[(1, format!("h{n}"))]);
        let effects = dep.breaker.checked(dep.now, id, Check::Unasked);
        assert!(!resumed(&effects), "closed though heads were met");
    }
}

/// C38: a trip within the suspicion's step after a refutation is kept
/// pending: its check comes at the step's end though no further verdict
/// comes. Red when the pending trip is dropped.
#[test]
fn c38_a_trip_within_the_step_is_checked_at_its_end() {
    let mut dep = Dep::new();
    dep.trip();
    let (id, _) = dep.check().expect("a check");
    dep.breaker.checked(dep.now, id, Check::Served);
    dep.advance(Duration::from_millis(100));
    dep.trip();
    assert!(dep.check().is_none(), "within the step");
    for _ in 0..30 {
        dep.advance(Duration::from_millis(100));
        if dep.check().is_some() {
            return;
        }
    }
    panic!("the pending trip was never checked");
}

/// C39: S in what the dependency served by a call: on a quiet channel with
/// one sample, S is none and two refusals trip; with the dependency serving
/// thirty keys within the window, eight refusals do not (the arms' shares);
/// rows processed count nothing for it. Red when S comes from the rows
/// processed, or is ignored.
#[test]
fn c39_s_is_what_the_dependency_served_by_a_call() {
    let mut dep = Dep::new();
    dep.served(100);
    dep.trip();
    assert!(dep.check().is_some(), "one sample: S is none");
    let mut dep = Dep::new();
    for n in 0..30 {
        dep.served(100 + n);
        dep.advance(Duration::from_millis(500));
    }
    for n in 0..8 {
        dep.ask(&format!("g{n}"), dependency_down());
    }
    assert!(
        dep.check().is_none(),
        "eight refusals against thirty served"
    );
    let mut dep = Dep::new();
    dep.served(100);
    for n in 0..100 {
        dep.ask(&format!("p{n}"), Verdict::Processed);
    }
    dep.trip();
    assert!(
        dep.check().is_some(),
        "rows processed are no S of the dependency"
    );
}

/// C40: paused, its checks ask the heads met, each once a round, the first met
/// first, a Tenant the memo does not hold before one it does; once every head
/// met was asked a round begins anew: four checks ask m0, m1, then f1 (which
/// the memo holds), then m0 again, though m0 is met again before each. Red
/// when the newest is taken, the memo's Tenant before a fresh one, a head
/// twice a round, or none once each was asked.
#[test]
fn c40_checks_ask_the_heads_met_each_once_the_first_first() {
    let mut dep = Dep::new();
    dep.pause();
    let met = ["f1", "m0", "m1"];
    dep.breaker
        .met(dep.now, CHANNEL, &met.map(|t| (1, t.to_owned())));
    let mut asked = Vec::new();
    for _ in 0..4 {
        dep.breaker.met(dep.now, CHANNEL, &[(1, "m0".to_owned())]);
        let (id, head) = dep.next_check();
        asked.push(head.map(|(_, tenant)| tenant));
        dep.breaker
            .checked(dep.now, id, Check::Unserved(cause("kms.unavailable")));
    }
    assert_eq!(
        asked,
        ["m0", "m1", "f1", "m0"].map(|t| Some(t.to_owned())),
        "the first met first, the memo's last, each once a round"
    );
}

/// C41: the stall's cause follows the latest check unserved: a credential
/// refused after an outage reports under its own cause. Red when the cause is
/// fixed at the opening.
#[test]
fn c41_the_stall_s_cause_follows_the_latest_unserved_check() {
    let mut dep = Dep::new();
    dep.pause();
    let (id, _) = dep.next_check();
    let effects = dep
        .breaker
        .checked(dep.now, id, Check::Unserved(cause("kms.unauthorized")));
    let cause_of = effects.iter().find_map(|e| match e {
        Effect::Stalled { cause, .. } => Some(cause.as_str()),
        _ => None,
    });
    assert_eq!(cause_of, Some("kms.unauthorized"));
}

/// C42: a suspicion's check unserved while the dependency served the rows by a
/// call meanwhile is refuted, not confirmed: the shard's healthy keys landed.
/// Red when it opens regardless.
#[test]
fn c42_an_unserved_check_with_keys_served_meanwhile_refutes() {
    let mut dep = Dep::new();
    dep.served(10);
    dep.advance(Duration::from_secs(31));
    dep.served(10);
    dep.trip();
    let (id, _) = dep.check().expect("a check");
    dep.advance(Duration::from_secs(2));
    dep.served(11);
    let effects = dep
        .breaker
        .checked(dep.now, id, Check::Unserved(cause("kms.unavailable")));
    assert!(refuted(&effects) && !paused(&effects), "{effects:?}");
}

/// C43: a check unanswered by its deadline — its issue, the check's bound and
/// a tick's first step, 6 s — at a suspicion pauses the channel, and its
/// answer served after still closes the pause; paused, it is a failed tick
/// that stalls, an answer unserved after counts nothing, and one served
/// after closes; one check at a time. Red when an expired check counts
/// nothing, the deadline is another, a late answer of the suspicion's is
/// dropped, a stale one counts, or two are issued at once.
#[test]
fn c43_an_unanswered_check_is_a_failed_tick() {
    let mut dep = Dep::new();
    dep.trip();
    let (first, _) = dep.check().expect("a check");
    assert!(dep.check().is_none(), "one check at a time");
    dep.advance(Duration::from_millis(5_999));
    assert!(!paused(&dep.tick()), "before its deadline");
    assert!(dep.check().is_none(), "one check at a time");
    dep.advance(Duration::from_millis(1));
    assert!(
        paused(&dep.tick()),
        "the suspicion's check unanswered pauses it"
    );
    assert!(
        resumed(&dep.breaker.checked(dep.now, first, Check::Served)),
        "its late answer served closes the pause"
    );
    assert!(
        dep.breaker
            .checked(dep.now, first, Check::Served)
            .is_empty(),
        "a stale answer"
    );
    // a window on, the failed ticks forgotten
    dep.advance(Duration::from_secs(31));
    dep.pause();
    let (second, _) = dep.next_check();
    dep.advance(Duration::from_secs(6));
    assert!(
        stalled(&dep.tick()),
        "the pause's check unanswered stalls it"
    );
    assert!(
        dep.breaker
            .checked(dep.now, second, Check::Unserved(cause("kms.unavailable")))
            .is_empty(),
        "an unserved answer after counts nothing"
    );
    assert!(
        resumed(&dep.breaker.checked(dep.now, second, Check::Served)),
        "a late served closes"
    );
}

/// C44: the suspicion's step doubles at each refutation and starts over after
/// a window without a trip. Red when it never starts over, or never grows.
#[test]
fn c44_the_suspicion_s_step_doubles_and_starts_over_after_a_quiet_window() {
    let mut dep = Dep::new();
    let mut gaps = Vec::new();
    for _ in 0..4 {
        dep.trip();
        let issued = dep.now;
        let (id, _) = dep.next_check();
        gaps.push(dep.now - issued);
        dep.breaker.checked(dep.now, id, Check::Served);
    }
    assert!(
        gaps.windows(2).all(|w| w[1] >= w[0]) && gaps[3] > gaps[1],
        "{gaps:?}"
    );
    dep.advance(Duration::from_secs(31));
    dep.trip();
    let (id, _) = dep
        .check()
        .expect("a trip after a quiet window checks at once");
    dep.breaker.checked(dep.now, id, Check::Served);
    dep.trip();
    let issued = dep.now;
    dep.next_check();
    assert!(
        dep.now - issued <= gaps[1],
        "the step started over: {:?} against {gaps:?}",
        dep.now - issued
    );
}

/// C45: one channel, two breakers: the dependency's paused, a handler's own
/// `Stage` refusals pause the rows' too; the dependency's check served closes
/// only its own, the rows' trial processed only the rows'. Red when one
/// source's evidence closes the other's pause.
#[test]
fn c45_the_dependency_s_and_the_rows_breakers_close_by_their_own_evidence() {
    let handler = Verdict::Unavailable {
        cause: cause("crm.unavailable"),
        scope: Scope::Stage,
        retry_after: None,
        source: Source::Rows,
    };
    // the rows' pause, from asks begun before the dependency's
    let mut both = Dep::new();
    let early: Vec<Flight> = ["x", "y"].iter().map(|t| both.flight(t)).collect();
    both.pause();
    let mut opened = false;
    for flight in early {
        opened |= paused(&both.breaker.told(both.now, flight, handler).effects);
    }
    assert!(opened, "the rows' breaker opens on the handler's refusals");
    let mut dependency_check = None;
    let mut row_trial = None;
    for _ in 0..600 {
        for effect in both.tick() {
            match effect {
                Effect::Check { id, .. } if dependency_check.is_none() => {
                    dependency_check = Some(id)
                }
                Effect::Trial { id, .. } if row_trial.is_none() => row_trial = Some(id),
                _ => {}
            }
        }
        if dependency_check.is_some() && row_trial.is_some() {
            break;
        }
        both.advance(Duration::from_millis(100));
    }
    let check = dependency_check.expect("the dependency's check");
    let trial = row_trial.expect("the rows' trial");
    assert!(resumed(&both.breaker.checked(
        both.now,
        check,
        Check::Served
    )));
    assert!(
        both.breaker.is_paused(CHANNEL),
        "the rows' pause closed by the dependency's check"
    );
    assert!(resumed(
        &both
            .breaker
            .tried(both.now, trial, Trial::Told(Verdict::Processed))
            .effects
    ));
    assert!(!both.breaker.is_paused(CHANNEL));
}

/// C47: the two breakers count apart: a handler's one `Stage` refusal beside
/// the dependency's two is one Tenant failing for the rows, which pauses
/// nothing. Red when the dependency's refusals count for the rows too.
#[test]
fn c47_the_dependency_s_refusals_count_nothing_for_the_rows() {
    let mut dep = Dep::new();
    dep.trip();
    let handler = Verdict::Unavailable {
        cause: cause("crm.unavailable"),
        scope: Scope::Stage,
        retry_after: None,
        source: Source::Rows,
    };
    let effects = dep.ask("x", handler);
    assert!(!paused(&effects), "{effects:?}");
    assert!(!dep.breaker.is_paused(CHANNEL));
}

/// C48: paused through a long outage, every head met asked once, the
/// dependency's own keys gone — its checks without a head unasked — and the
/// heads met again as the guests walk on: the next check asks them anew,
/// and the first served closes. Red when a head asked once is not asked
/// again in the pause, which then never closes.
#[test]
fn c48_the_heads_met_are_asked_round_after_round() {
    let mut dep = Dep::new();
    dep.pause();
    let met = ["m0", "m1"].map(|t| (1, t.to_owned()));
    dep.breaker.met(dep.now, CHANNEL, &met);
    for expected in ["m0", "m1"] {
        let (id, head) = dep.next_check();
        assert_eq!(head.map(|(_, t)| t), Some(expected.to_owned()));
        dep.breaker
            .checked(dep.now, id, Check::Unserved(cause("kms.unavailable")));
    }
    let (id, head) = dep.next_check();
    assert_eq!(head, None, "no head left this round, none met");
    dep.breaker.checked(dep.now, id, Check::Unasked);
    dep.breaker.met(dep.now, CHANNEL, &met);
    let (id, head) = dep.next_check();
    assert_eq!(head.map(|(_, t)| t), Some("m0".to_owned()), "a new round");
    assert!(resumed(&dep.breaker.checked(dep.now, id, Check::Served)));
}

/// C49: the rows' breaker open on a handler's own refusals, its trials
/// refused by the stage's dependency: those refusals are the dependency's
/// breaker's — two Tenants trip it, and a check follows — and tell the rows
/// nothing: three close the rows' pause as inconclusive, no failed tick
/// counted, no stall. Red when a trial's refusal by the dependency counts as
/// the rows' failed tick and is never weighed by the dependency's breaker.
#[test]
fn c49_a_trial_refused_by_the_dependency_is_the_dependency_s() {
    let handler = Verdict::Unavailable {
        cause: cause("crm.unavailable"),
        scope: Scope::Stage,
        retry_after: None,
        source: Source::Rows,
    };
    let mut dep = Dep::new();
    for tenant in ["x", "y"] {
        dep.ask(tenant, handler);
    }
    assert!(dep.breaker.is_paused(CHANNEL), "the rows' pause");
    dep.breaker.met(
        dep.now,
        CHANNEL,
        &["w1", "w2", "w3"].map(|t| (1, t.to_owned())),
    );
    let mut checks = 0;
    let mut effects = Vec::new();
    for _ in 0..3 {
        let mut trial = None;
        for _ in 0..600 {
            for effect in dep.tick() {
                match effect {
                    Effect::Trial { id, .. } => trial = Some(id),
                    Effect::Check { .. } => checks += 1,
                    other => effects.push(other),
                }
            }
            if trial.is_some() {
                break;
            }
            dep.advance(Duration::from_millis(100));
        }
        let told = dep.breaker.tried(
            dep.now,
            trial.expect("a trial"),
            Trial::Told(dependency_down()),
        );
        effects.extend(told.effects);
    }
    for _ in 0..30 {
        checks += dep
            .tick()
            .iter()
            .filter(|e| matches!(e, Effect::Check { .. }))
            .count();
        dep.advance(Duration::from_millis(100));
    }
    assert!(checks > 0, "the dependency's breaker tripped");
    assert!(
        !stalled(&effects),
        "no failed tick of the rows': {effects:?}"
    );
    assert!(
        effects.iter().any(|e| matches!(
            e,
            Effect::Resumed {
                by: super::Resumption::Inconclusive,
                ..
            }
        )),
        "the rows' pause closed as inconclusive: {effects:?}"
    );
}

/// C50: a suspicion whose checks are unasked is checked again at a step that
/// doubles — 2 s, 4 s, 8 s — and lapses once no refusal of the dependency's
/// is left within the window: no check after, no pause; and one whose check
/// is in flight when the last refusal leaves the window lapses at its
/// deadline rather than pausing on nothing. Red when an unasked check leaves
/// the step as it was, or a suspicion never lapses — checked for ever, or
/// paused under no cause.
#[test]
fn c50_a_suspicion_with_no_refusal_left_lapses() {
    let mut dep = Dep::new();
    dep.trip();
    let began = dep.now;
    let mut at = Vec::new();
    for _ in 0..20 {
        let mut issued = None;
        for _ in 0..600 {
            if let Some(check) = dep.check() {
                issued = Some(check);
                break;
            }
            dep.advance(Duration::from_millis(100));
        }
        let Some((id, _)) = issued else { break };
        at.push(dep.now.duration_since(began).as_secs());
        let effects = dep.breaker.checked(dep.now, id, Check::Unasked);
        assert!(!paused(&effects));
    }
    assert_eq!(at, [0, 2, 6, 14, 29], "the step doubling, then lapsed");
    assert!(!dep.breaker.is_paused(CHANNEL));
    // a check in flight when the last refusal leaves the window
    let mut dep = Dep::new();
    dep.trip();
    dep.advance(Duration::from_secs(25));
    let (id, _) = dep.check().expect("a check");
    dep.breaker.checked(dep.now, id, Check::Unasked);
    let (_, _) = dep.next_check();
    dep.advance(Duration::from_secs(6));
    assert!(!paused(&dep.tick()), "lapsed, not paused on nothing");
    assert!(!dep.breaker.is_paused(CHANNEL));
}

/// C51: one tick call issues a dependency's check and another channel's
/// trial both, the check due first: the check runs beside the dispatch and
/// takes nothing from the call's trial. Red when the call ends at its check.
#[test]
fn c51_a_check_takes_nothing_from_the_call_s_trial() {
    const OTHER: &str = "kafka://payments";
    let mut dep = Dep::new();
    dep.trip();
    dep.advance(Duration::from_millis(500));
    for tenant in ["x", "y"] {
        let Before::Ask(flight) = dep.breaker.before(dep.now, OTHER, tenant, 0, 0) else {
            panic!("asked");
        };
        dep.breaker.told(dep.now, flight, stage());
    }
    assert!(dep.breaker.is_paused(OTHER));
    dep.breaker.met(dep.now, OTHER, &[(1, "w".to_owned())]);
    dep.advance(Duration::from_secs(2));
    let effects = dep.tick();
    assert!(
        effects.iter().any(|e| matches!(e, Effect::Check { .. }))
            && effects.iter().any(|e| matches!(e, Effect::Trial { .. })),
        "{effects:?}"
    );
}

/// C52: the dependency's failed ticks are kept across a close shorter than the
/// window — opened again, it stalls at its opening — and forgotten after a
/// window closed. Red when they are never kept, or never forgotten.
#[test]
fn c52_failed_ticks_kept_across_a_short_close() {
    let mut dep = Dep::new();
    dep.pause();
    let (id, _) = dep.next_check();
    assert!(resumed(&dep.breaker.checked(dep.now, id, Check::Served)));
    dep.advance(Duration::from_secs(10));
    dep.trip();
    let (id, _) = dep.next_check();
    let effects = dep
        .breaker
        .checked(dep.now, id, Check::Unserved(cause("kms.unavailable")));
    assert!(
        paused(&effects) && stalled(&effects),
        "opened within the window: stalled at once: {effects:?}"
    );
    let (id, _) = dep.next_check();
    assert!(resumed(&dep.breaker.checked(dep.now, id, Check::Served)));
    dep.advance(Duration::from_secs(31));
    dep.trip();
    let (id, _) = dep.next_check();
    let effects = dep
        .breaker
        .checked(dep.now, id, Check::Unserved(cause("kms.unavailable")));
    assert!(
        paused(&effects) && !stalled(&effects),
        "opened a window after: one failed tick: {effects:?}"
    );
}

proptest! {
    /// C46: over random histories of the dependency's refusals, rows
    /// processed and of their own, what it served, heads met, steps of time
    /// and checks answered every way or left unanswered: a pause of the
    /// dependency's comes only of a check — unserved, or unanswered by its
    /// deadline — never of a verdict; a close only of a check — served, or
    /// with nothing to hold — never of a verdict or a tick alone; a check
    /// served of the pause's own, in time or late, closes it; and a check is
    /// issued only once the one before was answered or its deadline passed.
    /// Red when a verdict pauses or closes the dependency's channel, a tick
    /// closes it, a served check of its own leaves it paused, or two checks
    /// are in flight at once.
    #[test]
    fn c46_the_dependency_s_pauses_and_closes_come_of_checks(
        seed in any::<u64>(),
        steps in 100usize..400,
    ) {
        let mut rng = Rng(seed | 1);
        let mut dep = Dep::new();
        let mut served = 0u64;
        let mut issued: Vec<u64> = Vec::new();
        // in flight: issued, neither answered nor past its deadline, nor of a
        // pause closed since
        let mut flying: Vec<(u64, Instant)> = Vec::new();
        // the checks of the pause: the one that opened it and those since
        let mut own: Vec<u64> = Vec::new();
        let mut last_closed: Option<u64> = None;
        for step in 0..steps {
            match rng.below(8) {
                0 | 1 => {
                    let tenant = format!("t{}", rng.below(12));
                    if let Before::Ask(flight) = dep.breaker.before(dep.now, CHANNEL, &tenant, 0, 0) {
                        let verdict = match rng.below(3) {
                            0 => dependency_down(),
                            1 => Verdict::Processed,
                            _ => Verdict::Own,
                        };
                        let effects = dep.breaker.told(dep.now, flight, verdict).effects;
                        prop_assert!(!paused(&effects) && !resumed(&effects), "a verdict at {step}: {effects:?}");
                    }
                }
                2 => {
                    served += rng.below(3);
                    dep.served(served);
                }
                3 => dep.breaker.met(dep.now, CHANNEL, &[(1, format!("h{}", rng.below(6)))]),
                4 => dep.advance(Duration::from_millis(rng.below(4_000))),
                5 if !issued.is_empty() => {
                    let id = issued.remove(rng.below(issued.len() as u64) as usize);
                    let check = match rng.below(3) {
                        0 => Check::Served,
                        1 => Check::Unserved(cause("kms.unavailable")),
                        _ => Check::Unasked,
                    };
                    let was_paused = dep.breaker.is_paused(CHANNEL);
                    flying.retain(|(f, _)| *f != id);
                    let effects = dep.breaker.checked(dep.now, id, check);
                    if paused(&effects) {
                        prop_assert!(matches!(check, Check::Unserved(_)), "{check:?} paused it");
                        own = vec![id];
                    }
                    if resumed(&effects) {
                        prop_assert!(check != Check::Unserved(cause("kms.unavailable")), "an unserved check closed it");
                        own.clear();
                        // a check of the pause still in flight is stale now:
                        // its answer finds no check, and a suspicion after
                        // asks its own
                        flying.clear();
                    }
                    let recent = own.iter().rev().take(8).any(|o| *o == id);
                    if was_paused && check == Check::Served && recent {
                        prop_assert!(resumed(&effects), "check {id} of its pause served, still paused: {effects:?}");
                    }
                }
                _ => {
                    let now = dep.now;
                    flying.retain(|(_, deadline)| *deadline > now);
                    let was_paused = dep.breaker.is_paused(CHANNEL);
                    let effects = dep.tick();
                    prop_assert!(!resumed(&effects), "a tick closed it: {effects:?}");
                    if paused(&effects) {
                        // the suspicion's check expired
                        own = last_closed.into_iter().collect();
                    }
                    for effect in &effects {
                        if let Effect::Check { id, deadline, .. } = effect {
                            prop_assert!(flying.is_empty(), "check {id} issued beside {flying:?}");
                            issued.push(*id);
                            flying.push((*id, *deadline));
                            if was_paused || paused(&effects) {
                                own.push(*id);
                            } else {
                                last_closed = Some(*id);
                            }
                        }
                    }
                }
            }
        }
    }
}

proptest! {
    /// C18: the walk's excluded keys, kept built while nothing changes, are
    /// after every step what the memo says now — over random histories of
    /// asks of both scopes, rollbacks, ticks with their trials of every
    /// outcome, heads met, asks held across steps and told after their channel
    /// paused, was tried and closed, and steps of time alone, two channels;
    /// and while an ask is in flight, as the process's other loops walk. Red
    /// when the object outlives an entry's back-off, a flight marked or
    /// abandoned, a verdict, a trial or a tick's close, unrebuilt: a Tenant
    /// kept from the walk for good, or its heads met again and again.
    #[test]
    fn c18_the_excluded_keys_are_what_the_memo_says_now(
        steps in proptest::collection::vec((0u8..9, 0u8..5, 0u8..2, 0u16..4_000), 1..300)
    ) {
        let mut breaker = Breaker::new(Breaking::default().with_window(Duration::from_secs(30)));
        let mut now = Instant::now();
        let channels = ["kafka://one", "kafka://two"];
        // asks held across steps: told later, after the channel may have
        // paused, been tried and closed
        let mut flying: Vec<Flight> = Vec::new();
        for (op, tenant, channel, millis) in steps {
            let tenant = format!("t{tenant}");
            let channel = channels[channel as usize];
            match op {
                // an ask and its verdict, by the Tenant's own lot
                0 | 1 => {
                    if let Before::Ask(flight) = breaker.before(now, channel, &tenant, 0, 0) {
                        // while the ask is in flight, as the process's other
                        // loops walk
                        let cached = keys_of(&breaker.excluded(now));
                        prop_assert_eq!(cached, recomputed(&breaker, now));
                        let verdict = match (millis % 5, op) {
                            (0, _) => Verdict::Processed,
                            (1, _) => Verdict::Own,
                            (2, _) => tenant_scope(None),
                            _ => stage(),
                        };
                        breaker.told(now, flight, verdict);
                    }
                }
                // an ask rolled back
                2 => {
                    if let Before::Ask(flight) = breaker.before(now, channel, &tenant, 0, 0) {
                        let cached = keys_of(&breaker.excluded(now));
                        prop_assert_eq!(cached, recomputed(&breaker, now));
                        breaker.abandoned(&flight);
                    }
                }
                // a tick, its trial answered by lot
                3 => {
                    for effect in breaker.tick(now) {
                        if let Effect::Trial { id, .. } = effect {
                            let trial = match millis % 7 {
                                0 => Trial::Told(Verdict::Processed),
                                1 => Trial::Busy,
                                2 => Trial::Gone,
                                3 => Trial::TimedOut,
                                4 => Trial::Told(Verdict::Own),
                                5 => Trial::Told(tenant_scope(None)),
                                _ => Trial::Told(stage()),
                            };
                            breaker.tried(now, id, trial);
                        }
                    }
                }
                // a head met by a guest
                4 => breaker.met(now, channel, &[(0, tenant.clone())]),
                // an ask begun and held, or a held one told
                6 => {
                    if let Before::Ask(flight) = breaker.before(now, channel, &tenant, 1, 0) {
                        flying.push(flight);
                    }
                }
                7 if !flying.is_empty() => {
                    let flight = flying.remove(usize::from(millis) % flying.len());
                    let verdict = match millis % 4 {
                        0 => Verdict::Processed,
                        1 => tenant_scope(None),
                        2 => Verdict::Own,
                        _ => stage(),
                    };
                    breaker.told(now, flight, verdict);
                }
                // time alone
                _ => {}
            }
            now += Duration::from_millis(u64::from(millis));
            let cached = keys_of(&breaker.excluded(now));
            prop_assert_eq!(cached, recomputed(&breaker, now));
        }
    }

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
            if let Some(until) = held_until.get(&tenant)
                && now < *until
            {
                prop_assert!(matches!(answer, Before::Hold { .. }), "{tenant} evicted before {until:?}");
                continue;
            }
            match answer {
                Before::Hold { wait, .. } => {
                    held_until.insert(tenant, now + wait);
                }
                Before::Pass => prop_assert!(false, "{tenant} passed at the Tenant's scope"),
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
        let (paused, excluded) = {
            let mut breaker = lock();
            let effects = breaker.tick(now);
            std::hint::black_box(&effects);
            (breaker.paused(), breaker.excluded(now))
        };
        std::hint::black_box((&paused, &excluded));
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

/// M9 (ADR-0042 § Executors, R9): the walk's excluded keys at the memo's
/// bound — seven channels, 1,024 Tenants each held at the stage's scope among
/// 1,024 processed, none paused — as a dispatch reads them under the inbox's
/// mutex: kept built, and rebuilt after a verdict on a memoised Tenant. A
/// measurement, run by hand on an optimised build:
///
/// ```text
/// ASCETIC_DDD_MEASURE=1 cargo test --release -p ascetic-ddd-inbox --lib -- \
///     --ignored --nocapture m9_the_walk_s_keys
/// ```
#[test]
#[ignore = "a measurement (ADR-0042 M9): run by hand"]
fn m9_the_walk_s_keys_at_the_memo_s_bound() {
    if std::env::var_os("ASCETIC_DDD_MEASURE").is_none() {
        return;
    }
    const CALLS: u32 = 20_000;
    let settings =
        Breaking::default().with_hold(Duration::from_secs(300), Duration::from_secs(300));
    let mut breaker = Breaker::new(settings);
    let start = Instant::now();
    let channels: Vec<String> = (0..7).map(|c| format!("kafka://channel-{c}")).collect();
    // 290 s: each channel 20 Tenants processed a second, and a new one refused
    // at the stage's scope every 283 ms, 1,024 in all, their back-offs of
    // 300 s all running at the end — fewer than a quarter of the window's
    // Tenants failing, so that nothing pauses
    let mut now = start;
    let mut refused = 0usize;
    for step in 0..2_900u32 {
        now += Duration::from_millis(100);
        for channel in &channels {
            for h in 0..2 {
                let healthy = format!("healthy-{:04}", (step * 2 + h) % 1_024);
                if let Before::Ask(flight) = breaker.before(now, channel, &healthy, 0, 0) {
                    breaker.told(now, flight, Verdict::Processed);
                }
            }
        }
        if refused < 1_024 && step * 1_024 / 2_900 >= refused as u32 {
            for channel in &channels {
                let tenant = format!("{refused:04}-a-tenant-identifier-of-36-chars");
                if let Before::Ask(flight) = breaker.before(now, channel, &tenant, 0, 0) {
                    breaker.told(now, flight, stage());
                }
            }
            refused += 1;
        }
    }
    assert!(
        breaker.paused().is_empty(),
        "nothing paused: {:?}",
        breaker.paused()
    );
    let keys = breaker.excluded(now);
    let tenants = keys.matches(":1").count();
    let built = Instant::now();
    for _ in 0..CALLS {
        std::hint::black_box(breaker.excluded(now));
    }
    let kept = built.elapsed();
    let rebuilt = Instant::now();
    for _ in 0..CALLS / 20 {
        breaker.keys = None;
        std::hint::black_box(breaker.excluded(now));
    }
    let rebuilding = rebuilt.elapsed();
    println!(
        "M9, the walk's keys: {tenants} excluded, {} bytes; {:.3} µs a dispatch kept built, {:.1} µs \
         rebuilt",
        keys.len(),
        kept.as_secs_f64() * 1e6 / f64::from(CALLS),
        rebuilding.as_secs_f64() * 1e6 / f64::from(CALLS / 20)
    );
}
