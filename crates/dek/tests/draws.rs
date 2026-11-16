//! How a stage draws its data keys (ADR-0039): once for its waiters, on a
//! flight no waiter's cancellation cancels, each seal waiting no longer
//! than its bound, a failing KMS asked once per back-off step, a key gone
//! told to the one who drew, a key leaving memory at its end — on tokio's
//! paused clock, against a KMS whose every draw a test releases.

#![cfg(feature = "bus")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "tests"
)]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ascetic_ddd_bus::{BoxError, Message, Permanent, Stage};
use ascetic_ddd_dek::Scope;
use ascetic_ddd_dek::stage::{DEK, MESSAGE_ID, TENANT_ID};
use ascetic_ddd_dek::supply::Reason;
use ascetic_ddd_dek::{EnvelopeStage, Landed, Pacing, Reuse, Unavailable};
use ascetic_ddd_kms::{Error, Key, KeyManagementService};
use ascetic_ddd_session::testing::{MemorySession, MemorySessionPool};
use tokio::sync::Notify;
use tokio::time::{Instant, advance, sleep};

/// What a draw answers when released.
#[derive(Clone, Debug)]
enum Answer {
    Key,
    Gone,
    Forbidden,
    Unreachable,
    Panic,
    /// Never: the draw hangs until it is dropped.
    Never,
}

/// A KMS whose draws wait for a script: each draw takes the next answer,
/// after `latency`; it counts its draws and the draws dropped unanswered.
struct Gated {
    script: Mutex<VecDeque<Answer>>,
    latency: Mutex<Duration>,
    release: Notify,
    gated: Mutex<bool>,
    draws: AtomicU32,
    dropped: Arc<AtomicU32>,
}

impl Gated {
    fn new(script: impl IntoIterator<Item = Answer>) -> Arc<Self> {
        Arc::new(Gated {
            script: Mutex::new(script.into_iter().collect()),
            latency: Mutex::new(Duration::from_millis(10)),
            release: Notify::new(),
            gated: Mutex::new(false),
            draws: AtomicU32::new(0),
            dropped: Arc::new(AtomicU32::new(0)),
        })
    }

    fn draws(&self) -> u32 {
        self.draws.load(Ordering::SeqCst)
    }

    fn dropped(&self) -> u32 {
        self.dropped.load(Ordering::SeqCst)
    }

    fn hold(&self) {
        *self.gated.lock().unwrap() = true;
    }

    fn open(&self) {
        *self.gated.lock().unwrap() = false;
        self.release.notify_waiters();
    }

    fn slow(&self, latency: Duration) {
        *self.latency.lock().unwrap() = latency;
    }
}

/// Counts a draw dropped before it answered.
struct Unanswered(Arc<AtomicU32>, bool);

impl Drop for Unanswered {
    fn drop(&mut self) {
        if !self.1 {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// The gated KMS as a stage holds it.
#[derive(Clone)]
struct Kms(Arc<Gated>);

impl std::ops::Deref for Kms {
    type Target = Gated;
    fn deref(&self) -> &Gated {
        &self.0
    }
}

impl KeyManagementService<MemorySession> for Kms {
    async fn encrypt_dek(&self, _: &MemorySession, _: &str, dek: &Key) -> Result<Vec<u8>, Error> {
        Ok(dek.as_bytes().to_vec())
    }

    async fn decrypt_dek(&self, _: &MemorySession, _: &str, wrapped: &[u8]) -> Result<Key, Error> {
        Ok(Key::new(wrapped.to_vec()))
    }

    async fn generate_dek(
        &self,
        _: &MemorySession,
        tenant_id: &str,
    ) -> Result<(Key, Vec<u8>), Error> {
        let n = self.draws.fetch_add(1, Ordering::SeqCst) + 1;
        let mut unanswered = Unanswered(Arc::clone(&self.dropped), false);
        loop {
            let released = self.release.notified();
            if !*self.gated.lock().unwrap() {
                break;
            }
            released.await;
        }
        let latency = *self.latency.lock().unwrap();
        sleep(latency).await;
        let answer = self
            .script
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Answer::Key);
        let result = match answer {
            Answer::Key => {
                let mut key = vec![0u8; 32];
                key[..4].copy_from_slice(&n.to_be_bytes());
                Ok((Key::new(key.clone()), key))
            }
            Answer::Gone => Err(Error::KekNotFound {
                tenant_id: tenant_id.to_owned(),
                key_version: None,
            }),
            Answer::Forbidden => Err(Error::Forbidden {
                tenant_id: tenant_id.to_owned(),
                operation: "datakey",
            }),
            Answer::Unreachable => Err(Error::Session(ascetic_ddd_session::SessionError::Acquire(
                "down".into(),
            ))),
            Answer::Panic => panic!("a draw that dies"),
            Answer::Never => std::future::pending().await,
        };
        unanswered.1 = true;
        result
    }

    async fn rewrap_dek(
        &self,
        _: &MemorySession,
        _: &str,
        wrapped: &[u8],
    ) -> Result<Vec<u8>, Error> {
        Ok(wrapped.to_vec())
    }
}

type Tested = Arc<EnvelopeStage<MemorySessionPool, Kms>>;

fn stage(kms: &Arc<Gated>, reuse: Reuse, pacing: impl FnOnce(Pacing) -> Pacing) -> Tested {
    Arc::new(
        EnvelopeStage::new(MemorySessionPool::new(), Kms(Arc::clone(kms)))
            .paced(reuse, pacing(Pacing::fitted(reuse)))
            .unwrap()
            .seeded(7),
    )
}

fn reuse() -> Reuse {
    Reuse {
        messages: 1_000,
        lifetime: Duration::from_secs(60),
    }
}

fn message(tenant: &str, n: u32) -> Message {
    Message::new(b"payload".to_vec())
        .with_header(TENANT_ID, tenant)
        .with_header(MESSAGE_ID, format!("m-{n}"))
}

/// The number of the draw a sealed message's key came from.
fn drawn_by(sealed: &Message) -> u32 {
    use base64::Engine as _;
    let wrapped = base64::engine::general_purpose::STANDARD
        .decode(sealed.header(DEK).unwrap())
        .unwrap();
    u32::from_be_bytes(wrapped[..4].try_into().unwrap())
}

fn unavailable(result: &Result<Message, BoxError>) -> Unavailable {
    result
        .as_ref()
        .err()
        .and_then(|e| e.downcast_ref::<Unavailable>())
        .cloned()
        .unwrap_or_else(|| panic!("unavailable: {result:?}"))
}

fn permanent(result: &Result<Message, BoxError>) -> bool {
    result.as_ref().err().is_some_and(|e| e.is::<Permanent>())
}

async fn seal(stage: &Tested, tenant: &str, n: u32) -> Result<Message, BoxError> {
    stage.outbound(message(tenant, n)).await
}

#[tokio::test(start_paused = true)]
async fn concurrent_misses_cost_one_draw_and_every_one_is_served() {
    let kms = Gated::new([]);
    kms.hold();
    let stage = stage(&kms, reuse(), |p| {
        p.with_wait(Duration::from_secs(1))
            .with_backoff(Duration::from_secs(2), Duration::from_secs(30))
    });
    let seals: Vec<_> = (0..20)
        .map(|n| {
            let stage = Arc::clone(&stage);
            tokio::spawn(async move { seal(&stage, "a", n).await })
        })
        .collect();
    sleep(Duration::from_millis(100)).await;
    assert_eq!(kms.draws(), 1, "one draw for every miss");
    kms.open();
    for one in seals {
        let sealed = one.await.unwrap().unwrap();
        assert_eq!(drawn_by(&sealed), 1);
    }
    assert_eq!(kms.draws(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_waiter_refuses_at_its_bound_and_the_draw_lands_for_the_next() {
    let kms = Gated::new([]);
    kms.slow(Duration::from_secs(2));
    let stage = stage(&kms, reuse(), |p| p);
    let started = Instant::now();
    let first = seal(&stage, "a", 1).await;
    assert_eq!(unavailable(&first).reason, Reason::Waited);
    assert_eq!(
        started.elapsed(),
        Duration::from_millis(250),
        "exactly its bound"
    );
    let late = seal(&stage, "a", 2).await;
    assert_eq!(
        unavailable(&late).reason,
        Reason::Late,
        "a late draw refuses at once"
    );
    advance(Duration::from_secs(2)).await;
    sleep(Duration::from_millis(1)).await;
    let next = seal(&stage, "a", 3).await.unwrap();
    assert_eq!(drawn_by(&next), 1, "the draw landed for the next");
    assert_eq!(kms.draws(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_dropped_caller_does_not_cancel_the_draw() {
    let kms = Gated::new([]);
    kms.hold();
    let stage = stage(&kms, reuse(), |p| p);
    let caller = {
        let stage = Arc::clone(&stage);
        tokio::spawn(async move { seal(&stage, "a", 1).await })
    };
    sleep(Duration::from_millis(10)).await;
    caller.abort();
    let _ = caller.await;
    kms.open();
    sleep(Duration::from_millis(50)).await;
    assert_eq!(kms.dropped(), 0, "the draw ran on");
    let next = seal(&stage, "a", 2).await.unwrap();
    assert_eq!(drawn_by(&next), 1);
    assert_eq!(kms.draws(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_shredded_tenant_is_told_by_every_seal_and_not_asked_again_for_a_while() {
    let kms = Gated::new([Answer::Gone]);
    let stage = stage(&kms, reuse(), |p| p);
    let drew = seal(&stage, "a", 1).await;
    assert!(
        permanent(&drew),
        "the one who drew is told the key is gone: {drew:?}"
    );
    for n in 2..5 {
        let after = seal(&stage, "a", n).await;
        assert_eq!(
            after
                .as_ref()
                .err()
                .and_then(|error| error.downcast_ref::<Permanent>())
                .map(Permanent::cause),
            Some(ascetic_ddd_dek::causes::GONE),
            "every seal within gone is told, under kms.gone: {after:?}"
        );
    }
    assert_eq!(kms.draws(), 1, "no call within gone");
    advance(Duration::from_secs(10)).await;
    let again = seal(&stage, "a", 3).await;
    assert!(
        again.is_ok() || unavailable(&again).reason == Reason::Late,
        "{again:?}"
    );
    // The flight runs once the test yields.
    sleep(Duration::from_millis(1)).await;
    assert_eq!(kms.draws(), 2, "asked again once gone is over");
}

#[tokio::test(start_paused = true)]
async fn a_sealing_side_refusal_backs_off_and_is_never_for_good() {
    let kms = Gated::new([Answer::Forbidden]);
    let stage = stage(&kms, reuse(), |p| p);
    let refused = seal(&stage, "a", 1).await;
    assert!(
        !permanent(&refused),
        "a policy's drift is mended by a deploy"
    );
    let refused = unavailable(&refused);
    assert!(
        refused
            .last
            .is_some_and(|last| last.landed == Landed::Refused && last.text.contains("refused")),
        "the cause told"
    );
    let backing = seal(&stage, "a", 2).await;
    let backing = unavailable(&backing);
    assert_eq!(backing.reason, Reason::BackingOff);
    assert!(
        backing
            .retry_after
            .is_some_and(|after| after > Duration::ZERO)
    );
    assert_eq!(kms.draws(), 1);
    assert_eq!(
        backing
            .verdict()
            .downcast_ref::<Permanent>()
            .map(Permanent::cause),
        Some(ascetic_ddd_dek::causes::REFUSED),
        "the bus's verdict: for good, under kms.refused, for a deploy to mend"
    );
}

/// A stage that refuses a tenant on no evidence of whether its KMS serves
/// draws for its sentinel, and once that lands in time, the tenant's
/// refusal is its own: the inbox spends an attempt rather than holding the
/// slot for an outage that is not there (ADR-0040 decision 3). Without a
/// sentinel the refusal stays the stage's.
#[tokio::test(start_paused = true)]
async fn a_sentinel_tells_a_tenant_s_trouble_from_the_kms_s() {
    for sentinel in [true, false] {
        let kms = Gated::new([Answer::Unreachable, Answer::Key]);
        let built = EnvelopeStage::new(MemorySessionPool::new(), Kms(Arc::clone(&kms)))
            .paced(reuse(), Pacing::fitted(reuse()))
            .unwrap()
            .seeded(7);
        let stage: Tested = Arc::new(if sentinel {
            built.with_sentinel("s")
        } else {
            built
        });
        let first = unavailable(&seal(&stage, "a", 1).await);
        assert_eq!(first.scope, Scope::Stage, "no evidence yet");
        sleep(Duration::from_millis(50)).await;
        let second = unavailable(&seal(&stage, "a", 2).await);
        assert_eq!(second.reason, Reason::BackingOff);
        if sentinel {
            assert_eq!(kms.draws(), 2, "a's draw, and the sentinel's");
            assert_eq!(second.scope, Scope::Tenant, "the sentinel landed in time");
            let verdict = second.verdict();
            let told = verdict
                .downcast_ref::<ascetic_ddd_bus::Unavailable>()
                .expect("unavailable");
            assert_eq!(
                (told.scope(), told.cause()),
                (
                    ascetic_ddd_bus::Scope::Tenant,
                    ascetic_ddd_dek::causes::UNAVAILABLE
                )
            );
        } else {
            assert_eq!(kms.draws(), 1, "no sentinel, no draw for evidence");
            assert_eq!(second.scope, Scope::Stage);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn one_tenant_s_failing_draws_do_not_stop_another_s() {
    let kms = Gated::new([Answer::Unreachable, Answer::Key]);
    let stage = stage(&kms, reuse(), |p| p);
    // a's draw fails within its wait: a is told its stage backs off.
    assert_eq!(
        unavailable(&seal(&stage, "a", 1).await).reason,
        Reason::BackingOff
    );
    let b = seal(&stage, "b", 2).await;
    assert!(b.is_ok(), "{b:?}");
}

#[tokio::test(start_paused = true)]
async fn a_flight_past_its_draw_bound_is_abandoned_and_its_place_given_back() {
    let kms = Gated::new([Answer::Never]);
    let stage = stage(&kms, reuse(), |p| p);
    assert_eq!(
        unavailable(&seal(&stage, "a", 1).await).reason,
        Reason::Waited
    );
    advance(Duration::from_secs(5)).await;
    sleep(Duration::from_millis(1)).await;
    assert_eq!(kms.dropped(), 1, "the draw's future dropped");
    assert_eq!(stage.held().flights, 0, "its place given back");
}

#[tokio::test(start_paused = true)]
async fn a_flight_that_dies_frees_its_slot() {
    let kms = Gated::new([Answer::Panic, Answer::Key]);
    let stage = stage(&kms, reuse(), |p| p);
    let died = seal(&stage, "a", 1).await;
    assert!(died.is_err());
    assert_eq!(stage.held().flights, 0, "the dead flight landed, canceled");
    advance(Duration::from_secs(2)).await;
    let mut served = None;
    for n in 2..6 {
        if let Ok(sealed) = seal(&stage, "a", n).await {
            served = Some(sealed);
            break;
        }
        advance(Duration::from_secs(1)).await;
    }
    assert!(served.is_some(), "the slot drew again");
}

#[test]
fn a_draw_outside_a_runtime_neither_deadlocks_nor_wedges_its_slot() {
    let kms = Gated::new([]);
    let stage = Arc::new(
        EnvelopeStage::new(MemorySessionPool::new(), Kms(Arc::clone(&kms))).reusing(reuse()),
    );
    let outside = {
        let stage = Arc::clone(&stage);
        std::thread::spawn(move || futures::executor::block_on(stage.outbound(message("a", 1))))
    };
    let refused = outside.join().expect("no deadlock, no panic");
    let refused = refused
        .err()
        .and_then(|e| e.downcast::<Unavailable>().ok())
        .expect("unavailable");
    assert_eq!(refused.reason, Reason::Unspawned);
    assert_eq!(stage.held().flights, 0, "the slot is free");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::advance(Duration::from_secs(2)).await;
        let mut sealed = None;
        for n in 2..6 {
            if let Ok(message) = stage.outbound(message("a", n)).await {
                sealed = Some(message);
                break;
            }
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        assert!(sealed.is_some(), "a runtime draws again");
    });
}

#[tokio::test(start_paused = true)]
async fn a_key_leaves_memory_at_its_end_though_no_one_asks() {
    let kms = Gated::new([]);
    let stage = stage(&kms, reuse(), |p| p.with_jitter(Duration::ZERO));
    seal(&stage, "a", 1).await.unwrap();
    assert_eq!(stage.held().keys, 1);
    advance(Duration::from_secs(59)).await;
    sleep(Duration::from_millis(1)).await;
    assert_eq!(stage.held().keys, 1);
    advance(Duration::from_secs(2)).await;
    sleep(Duration::from_millis(1)).await;
    assert_eq!(stage.held().keys, 0, "the tick retired it");
}

#[tokio::test(start_paused = true)]
async fn no_seal_waits_while_its_successor_is_drawn() {
    let kms = Gated::new([]);
    let stage = stage(&kms, reuse(), |p| p.with_jitter(Duration::ZERO));
    seal(&stage, "a", 1).await.unwrap();
    kms.slow(Duration::from_secs(3));
    // Within the lead of its end: the successor is drawn, and seals hit.
    advance(Duration::from_secs(52)).await;
    for n in 2..20 {
        let started = Instant::now();
        let sealed = seal(&stage, "a", n).await.unwrap();
        assert_eq!(started.elapsed(), Duration::ZERO, "no wait");
        assert!(drawn_by(&sealed) <= 2);
        sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(kms.draws(), 2, "one successor");
}

#[tokio::test(start_paused = true)]
async fn a_burst_under_a_slow_kms_is_never_refused_once_warm() {
    let kms = Gated::new([]);
    // A key's life far past the test: only its uses renew it.
    let reuse = Reuse {
        messages: 100,
        lifetime: Duration::from_secs(600),
    };
    let stage = stage(&kms, reuse, |p| p.with_burst(20));
    seal(&stage, "a", 0).await.unwrap();
    // Slower than a seal waits: a miss would be refused.
    kms.slow(Duration::from_secs(1));
    let mut n = 1;
    for _ in 0..20 {
        for _ in 0..20 {
            let at = Instant::now();
            let sealed = seal(&stage, "a", n).await;
            assert!(sealed.is_ok(), "seal {n}: {sealed:?}");
            assert_eq!(at.elapsed(), Duration::ZERO, "seal {n} waited");
            n += 1;
        }
        sleep(Duration::from_secs(2)).await;
    }
    assert!(kms.draws() >= 4, "its uses renewed it: {}", kms.draws());
}

#[tokio::test(start_paused = true)]
async fn a_stage_configured_again_while_its_draw_flies_starts_its_own() {
    let kms = Gated::new([]);
    kms.hold();
    let used = EnvelopeStage::new(MemorySessionPool::new(), Kms(Arc::clone(&kms)))
        .paced(reuse(), Pacing::fitted(reuse()))
        .unwrap();
    let refused = used.outbound(message("a", 1)).await;
    assert_eq!(unavailable(&refused).reason, Reason::Waited);
    assert_eq!(used.held().flights, 1, "its draw still flies");
    let stage: Tested = Arc::new(used.seeded(7));
    assert_eq!(
        stage.held().flights,
        0,
        "the stage configured again draws anew"
    );
    kms.open();
    let sealed = seal(&stage, "a", 2).await.unwrap();
    assert_eq!(
        drawn_by(&sealed),
        2,
        "its own draw, not the one before it was configured"
    );
    assert_eq!(kms.draws(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_fitted_pacing_fits_any_reuse_and_serves() {
    let durations = [
        Duration::ZERO,
        Duration::from_nanos(1),
        Duration::from_secs(1),
        Duration::from_secs(60),
        Duration::from_secs(100 * 365 * 86_400),
        Duration::MAX,
    ];
    for messages in [0, 1, 2, 3, 10, u32::MAX] {
        for lifetime in durations {
            let reuse = Reuse { messages, lifetime };
            let kms = Gated::new([]);
            let stage: Tested = Arc::new(
                EnvelopeStage::new(MemorySessionPool::new(), Kms(Arc::clone(&kms))).reusing(reuse),
            );
            seal(&stage, "a", 1)
                .await
                .unwrap_or_else(|e| panic!("{reuse:?}: {e}"));
            advance(Duration::from_secs(3_600)).await;
            seal(&stage, "a", 2)
                .await
                .unwrap_or_else(|e| panic!("{reuse:?}, an hour on: {e}"));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_caller_dropped_while_it_waits_leaves_nothing_behind() {
    let kms = Gated::new([]);
    kms.hold();
    let stage = stage(&kms, reuse(), |p| {
        p.with_wait(Duration::from_secs(1))
            .with_backoff(Duration::from_secs(2), Duration::from_secs(30))
    });
    let caller = {
        let stage = Arc::clone(&stage);
        tokio::spawn(async move { seal(&stage, "a", 1).await })
    };
    sleep(Duration::from_millis(10)).await;
    caller.abort();
    let _ = caller.await;
    kms.open();
    sleep(Duration::from_millis(50)).await;
    assert_eq!(
        (stage.held().keys, stage.held().slots),
        (1, 1),
        "the draw landed"
    );
    advance(reuse().lifetime).await;
    sleep(Duration::from_millis(1)).await;
    assert_eq!(
        (stage.held().keys, stage.held().slots),
        (0, 0),
        "the key ended, and its tenant is not kept for the waiter who left"
    );
}

#[test]
fn a_tick_ended_with_its_runtime_starts_again_in_the_next() {
    let kms = Gated::new([]);
    let stage: Tested = Arc::new(
        EnvelopeStage::new(MemorySessionPool::new(), Kms(Arc::clone(&kms))).reusing(reuse()),
    );
    for round in 1..=2 {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap();
        runtime.block_on(async {
            seal(&stage, "a", round).await.unwrap();
            assert_eq!(stage.held().keys, 1);
            advance(reuse().lifetime).await;
            sleep(Duration::from_millis(1)).await;
            assert_eq!(
                stage.held().keys,
                0,
                "round {round}: the key left memory at its end"
            );
        });
    }
    assert_eq!(kms.draws(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_tenant_s_last_failure_is_forgotten_with_its_slot() {
    let kms = Gated::new([Answer::Unreachable]);
    let stage = stage(&kms, reuse(), |p| p);
    let failed = unavailable(&seal(&stage, "a", 1).await);
    assert_eq!(failed.reason, Reason::BackingOff);
    assert!(failed.last.is_some(), "its cause told");
    advance(Duration::from_secs(2)).await;
    sleep(Duration::from_millis(1)).await;
    assert_eq!(
        stage.held().slots,
        1,
        "its back-off over, its failure still decides the next draw"
    );
    advance(Duration::from_secs(30)).await;
    sleep(Duration::from_millis(1)).await;
    assert_eq!(
        stage.held().slots,
        0,
        "a cap past its back-off, nothing is kept"
    );
    kms.hold();
    let waited = unavailable(&seal(&stage, "a", 2).await);
    assert_eq!(waited.reason, Reason::Waited);
    assert_eq!(
        waited.last, None,
        "a failure of an episode gone is not told as the cause"
    );
}

#[test]
fn a_hit_is_served_though_its_successor_s_draw_cannot_start() {
    let kms = Gated::new([]);
    let reuse = Reuse {
        messages: 4,
        lifetime: Duration::from_secs(60),
    };
    let stage: Tested = Arc::new(
        EnvelopeStage::new(MemorySessionPool::new(), Kms(Arc::clone(&kms))).reusing(reuse),
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let first = runtime.block_on(seal(&stage, "a", 1)).unwrap();
    drop(runtime);
    assert_eq!(drawn_by(&first), 1);
    let outside = {
        let stage = Arc::clone(&stage);
        std::thread::spawn(move || {
            (2..=3)
                .map(|n| futures::executor::block_on(stage.outbound(message("a", n))))
                .collect::<Vec<_>>()
        })
    };
    for sealed in outside.join().expect("no deadlock, no panic") {
        let sealed = sealed.expect("a hit is served");
        assert_eq!(drawn_by(&sealed), 1);
    }
    assert_eq!(stage.held().flights, 0, "the successor's place given back");
    assert_eq!(kms.draws(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_kms_out_of_reach_is_asked_less_and_less_however_often_a_tenant_seals() {
    let kms = Gated::new(std::iter::repeat_n(Answer::Unreachable, 1_000));
    let stage = stage(&kms, reuse(), |p| p);
    let started = Instant::now();
    let mut n = 0;
    while started.elapsed() < Duration::from_secs(60) {
        n += 1;
        assert!(seal(&stage, "a", n).await.is_err());
        sleep(Duration::from_millis(100)).await;
    }
    // A back-off doubling from a second to thirty, at its least jitter:
    // draws at 0, 0.5, 1.5, 3.5, 7.5, 15.5, 30.5 and 45.5 s.
    assert!(
        kms.draws() <= 8,
        "{} draws in a minute of {n} seals",
        kms.draws()
    );
}

#[tokio::test(start_paused = true)]
async fn a_key_gone_is_told_to_every_seal_though_its_draw_outlived_the_wait() {
    let kms = Gated::new([Answer::Gone]);
    kms.slow(Duration::from_secs(1));
    let stage = stage(&kms, reuse(), |p| p);
    let drew = seal(&stage, "a", 1).await;
    assert_eq!(
        unavailable(&drew).reason,
        Reason::Waited,
        "its draw outlived the wait"
    );
    advance(Duration::from_secs(1)).await;
    sleep(Duration::from_millis(1)).await;
    assert!(
        permanent(&seal(&stage, "a", 2).await),
        "the first to ask is told"
    );
    assert!(permanent(&seal(&stage, "a", 3).await), "and so is the next");
    assert_eq!(kms.draws(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_seal_waits_its_bound_once_however_often_it_joins() {
    let kms = Gated::new([]);
    kms.slow(Duration::from_millis(200));
    // Two messages a key: the first landing serves two of five waiters.
    let reuse = Reuse {
        messages: 2,
        lifetime: Duration::from_secs(60),
    };
    let stage = stage(&kms, reuse, |p| p.with_burst(1));
    let started = Instant::now();
    let seals: Vec<_> = (0..5)
        .map(|n| {
            let stage = Arc::clone(&stage);
            tokio::spawn(async move {
                let result = seal(&stage, "a", n).await;
                (result, started.elapsed())
            })
        })
        .collect();
    let mut served = 0;
    for one in seals {
        let (result, elapsed) = one.await.unwrap();
        assert!(elapsed <= Duration::from_millis(250), "waited {elapsed:?}");
        served += usize::from(result.is_ok());
    }
    assert_eq!(served, 2);
    // The next flight, joined with little of a wait left, is not late:
    // a seal now joins it and is served when it lands.
    let next = seal(&stage, "a", 9).await;
    assert!(next.is_ok(), "not refused as late: {next:?}");
}

#[tokio::test(start_paused = true)]
async fn a_stage_dropped_ends_its_tick() {
    let metrics = tokio::runtime::Handle::current().metrics();
    let before = metrics.num_alive_tasks();
    let kms = Gated::new([]);
    let stage = stage(&kms, reuse(), |p| p);
    seal(&stage, "a", 1).await.unwrap();
    assert!(metrics.num_alive_tasks() > before, "its tick runs");
    drop(stage);
    sleep(Duration::from_millis(1)).await;
    assert_eq!(metrics.num_alive_tasks(), before, "and ends with it");
}

#[tokio::test(start_paused = true)]
async fn a_key_per_message_is_drawn_for_each_seal_at_once() {
    let kms = Gated::new([]);
    kms.slow(Duration::from_millis(100));
    let stage: Tested = Arc::new(EnvelopeStage::new(
        MemorySessionPool::new(),
        Kms(Arc::clone(&kms)),
    ));
    let started = Instant::now();
    let seals: Vec<_> = (0..10)
        .map(|n| {
            let stage = Arc::clone(&stage);
            tokio::spawn(async move { seal(&stage, "a", n).await })
        })
        .collect();
    let mut drawn = std::collections::HashSet::new();
    for one in seals {
        drawn.insert(drawn_by(&one.await.unwrap().unwrap()));
    }
    assert_eq!(drawn.len(), 10, "a key of its own each");
    assert_eq!(started.elapsed(), Duration::from_millis(100), "all at once");
    assert_eq!(stage.held(), ascetic_ddd_dek::Held::default(), "none held");
}

#[tokio::test(start_paused = true)]
async fn a_seal_on_a_full_stage_waits_for_a_place_within_its_bound() {
    let kms = Gated::new([]);
    kms.slow(Duration::from_millis(100));
    let stage = stage(&kms, reuse(), |p| p.with_flights(1));
    let first = {
        let stage = Arc::clone(&stage);
        tokio::spawn(async move { seal(&stage, "a", 1).await })
    };
    sleep(Duration::from_millis(10)).await;
    let started = Instant::now();
    let second = seal(&stage, "b", 2).await;
    assert!(second.is_ok(), "a place freed, its draw landed: {second:?}");
    assert_eq!(started.elapsed(), Duration::from_millis(190));
    assert!(first.await.unwrap().is_ok());
}

/// A channel's observer, as a sealing made it.
type Observed = (String, Arc<Recorded>);

/// What a stage told, in order.
#[derive(Default)]
struct Recorded {
    flights: Mutex<Vec<(ascetic_ddd_dek::Landed, u32)>>,
    served: AtomicU32,
    unserved: Mutex<Vec<Reason>>,
    retired: AtomicU32,
}

impl ascetic_ddd_dek::SealingObserver for Recorded {
    fn on_flight(&self, flight: &ascetic_ddd_dek::Flight<'_>) {
        self.flights
            .lock()
            .unwrap()
            .push((flight.landed, flight.failures));
    }
    fn on_served(&self, _tenant: &str) {
        self.served.fetch_add(1, Ordering::SeqCst);
    }
    fn on_unserved(&self, _tenant: &str, reason: Reason) {
        self.unserved.lock().unwrap().push(reason);
    }
    fn on_retired(&self, count: usize) {
        self.retired
            .fetch_add(u32::try_from(count).unwrap(), Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn an_observer_is_told_every_flight_seal_and_retired_key() {
    use ascetic_ddd_dek::Landed;
    let kms = Gated::new([Answer::Unreachable, Answer::Unreachable]);
    let recorded = Arc::new(Recorded::default());
    let reuse = Reuse {
        messages: 4,
        lifetime: Duration::from_secs(60),
    };
    let stage: Tested = Arc::new(
        EnvelopeStage::new(MemorySessionPool::new(), Kms(Arc::clone(&kms)))
            .paced(reuse, Pacing::fitted(reuse))
            .unwrap()
            .seeded(7)
            .observed_by(Arc::clone(&recorded) as Arc<dyn ascetic_ddd_dek::SealingObserver>),
    );
    for n in 0..200 {
        if recorded.served.load(Ordering::SeqCst) >= 6 {
            break;
        }
        let _ = seal(&stage, "a", n).await;
        sleep(Duration::from_millis(100)).await;
    }
    assert!(
        recorded.served.load(Ordering::SeqCst) >= 6,
        "seals served, told"
    );
    let flights = recorded.flights.lock().unwrap().clone();
    assert_eq!(
        flights[..3],
        [
            (Landed::Unavailable, 1),
            (Landed::Unavailable, 2),
            (Landed::Drawn, 0)
        ],
        "each flight with the failures in a row"
    );
    assert!(
        recorded
            .unserved
            .lock()
            .unwrap()
            .contains(&Reason::BackingOff)
    );
    assert!(
        recorded.retired.load(Ordering::SeqCst) >= 1,
        "a key replaced by its successor is retired"
    );
}

#[test]
fn a_sealing_whose_keys_would_outlive_its_ceiling_is_refused() {
    let kms = Gated::new([]);
    let parameters = ascetic_ddd_dek::SealingParameters::default();
    let pacing = Pacing::fitted(parameters.reuse);
    let window = parameters.reuse.lifetime + parameters.cache + pacing.draw;
    let build = |ceiling| {
        ascetic_ddd_dek::Sealing::over_paced(
            Arc::new(MemorySessionPool::new()),
            ["c"],
            (
                parameters,
                pacing,
                ascetic_ddd_dek::Opening::fitted(parameters.cache, 16),
            ),
            Some(ceiling),
            None,
            |_| Arc::new(()),
            |_| Kms(Arc::clone(&kms)),
        )
    };
    assert_eq!(
        build(window - Duration::from_nanos(1)).unwrap_err(),
        ascetic_ddd_dek::InvalidParameters("lifetime + keep + draw ≤ the ceiling")
    );
    let sealing = build(window).unwrap();
    assert_eq!(
        sealing.held().collect::<Vec<_>>(),
        vec![("c", ascetic_ddd_dek::Held::default())]
    );
}

/// The log lines a test binary wrote: one logger for the process.
static LOGGED: Mutex<Vec<(log::Level, String)>> = Mutex::new(Vec::new());

struct Logged;

impl log::Log for Logged {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }
    fn log(&self, record: &log::Record<'_>) {
        LOGGED
            .lock()
            .unwrap()
            .push((record.level(), record.args().to_string()));
    }
    fn flush(&self) {}
}

#[tokio::test(start_paused = true)]
async fn an_outage_of_every_tenant_is_logged_a_line_at_a_time() {
    static LOGGER: Logged = Logged;
    let _ = log::set_logger(&LOGGER);
    log::set_max_level(log::LevelFilter::Trace);
    let kms = Gated::new(std::iter::repeat_n(Answer::Unreachable, 1_000));
    let stage = stage(&kms, reuse(), |p| p);
    for n in 0..50 {
        let _ = seal(&stage, &format!("told-{n}"), n).await;
    }
    let warned = LOGGED
        .lock()
        .unwrap()
        .iter()
        .filter(|(level, line)| *level == log::Level::Warn && line.contains("told-"))
        .count();
    assert_eq!(warned, 1, "fifty tenants failing at once: one line");
    advance(Duration::from_secs(11)).await;
    let _ = seal(&stage, "told-late", 99).await;
    sleep(Duration::from_millis(1)).await;
    let last = LOGGED
        .lock()
        .unwrap()
        .iter()
        .rev()
        .find(|(level, line)| *level == log::Level::Warn && line.contains("told-"))
        .map(|(_, line)| line.clone())
        .unwrap();
    assert!(
        last.contains("49 more"),
        "the next line counts the quiet: {last}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_many_threads_every_seal_keeps_its_bound_and_the_stage_drains() {
    // Every fifth draw fails; the rest take 10 ms.
    let script = (0..10_000).map(|n| {
        if n % 5 == 4 {
            Answer::Unreachable
        } else {
            Answer::Key
        }
    });
    let kms = Gated::new(script);
    let reuse = Reuse {
        messages: 20,
        lifetime: Duration::from_secs(2),
    };
    let stage = stage(&kms, reuse, |p| {
        p.with_backoff(Duration::from_millis(500), Duration::from_secs(1))
            .with_gone(Duration::from_secs(1))
            .with_flights(4)
    });
    let wait = Pacing::fitted(reuse).wait;
    let tasks: Vec<_> = (0..64u32)
        .map(|n| {
            let stage = Arc::clone(&stage);
            tokio::spawn(async move {
                let tenant = format!("t{}", n % 8);
                let until = Instant::now() + Duration::from_millis(1_500);
                let mut longest = Duration::ZERO;
                let mut served = 0u32;
                while Instant::now() < until {
                    let at = Instant::now();
                    served += u32::from(seal(&stage, &tenant, n).await.is_ok());
                    longest = longest.max(at.elapsed());
                    assert!(stage.held().flights <= 4, "past the cap");
                    sleep(Duration::from_millis(5)).await;
                }
                (longest, served)
            })
        })
        .collect();
    let mut served = 0;
    for task in tasks {
        let (longest, count) = task.await.unwrap();
        assert!(
            longest <= wait + Duration::from_millis(100),
            "a seal waited {longest:?}, its bound {wait:?}"
        );
        served += count;
    }
    assert!(served > 0);
    // A key's life, a cap's memory, and a margin: nothing is held.
    sleep(Duration::from_millis(3_500)).await;
    assert_eq!(stage.held(), ascetic_ddd_dek::Held::default());
}

#[tokio::test(start_paused = true)]
async fn a_sealing_tells_each_channel_s_observer_of_its_own_stage() {
    let kms = Gated::new([]);
    let parameters = ascetic_ddd_dek::SealingParameters::default();
    let observers: Arc<Mutex<Vec<Observed>>> = Arc::default();
    let sealing = ascetic_ddd_dek::Sealing::over_paced(
        Arc::new(MemorySessionPool::new()),
        ["c", "d"],
        (
            parameters,
            Pacing::fitted(parameters.reuse),
            ascetic_ddd_dek::Opening::fitted(parameters.cache, 16),
        ),
        None,
        None,
        |channel| {
            let recorded = Arc::new(Recorded::default());
            observers
                .lock()
                .unwrap()
                .push((channel.to_owned(), Arc::clone(&recorded)));
            recorded as Arc<dyn ascetic_ddd_dek::SealingObserver>
        },
        |_| Kms(Arc::clone(&kms)),
    )
    .unwrap();
    sealing
        .of("c")
        .unwrap()
        .outbound(message("a", 1))
        .await
        .unwrap();
    let served: Vec<(String, u32)> = observers
        .lock()
        .unwrap()
        .iter()
        .map(|(channel, recorded)| (channel.clone(), recorded.served.load(Ordering::SeqCst)))
        .collect();
    let mut served = served;
    served.sort();
    assert_eq!(served, vec![("c".to_owned(), 1), ("d".to_owned(), 0)]);
    let held: std::collections::HashMap<_, _> = sealing.held().collect();
    assert_eq!(held["c"].keys, 1);
    assert_eq!(held["d"].keys, 0);
}
