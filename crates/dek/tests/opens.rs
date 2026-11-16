//! The opening end (ADR-0039 slice 4), through the stage, on a paused
//! clock: each wrapped key unwrapped once for its waiters, an open waiting
//! no longer than the opening's own wait, a key gone told before any key
//! held unless the sentinel says the KMS lost its keys, a refused wrapped
//! key remembered, a key wiped at its end, and the bus's verdicts.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "tests"
)]

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ascetic_ddd_bus::{BoxError, Message, Permanent, Scope as BusScope, Stage, Unavailable};
use ascetic_ddd_dek::stage::{MESSAGE_ID, TENANT_ID};
use ascetic_ddd_dek::{EnvelopeStage, Opening, causes};
use ascetic_ddd_kms::{Error, Key, KeyManagementService};
use ascetic_ddd_session::testing::{MemorySession, MemorySessionPool};
use tokio::time::{advance, sleep};

const SENTINEL: &str = "sentinel";

/// What an unwrap answers.
#[derive(Clone, Copy, Debug)]
enum Unwrap {
    Key,
    Gone,
    Forbidden,
    Unreachable,
}

/// A KMS whose draws make a key whose wrapped form is itself, and whose
/// unwraps follow a script, after a latency, counted by wrapped key; the
/// sentinel's key there or not.
struct Scripted {
    unwraps: Mutex<VecDeque<Unwrap>>,
    latency: Mutex<Duration>,
    counted: Mutex<HashMap<Vec<u8>, u32>>,
    sentinel_there: AtomicBool,
    sentinel_draws: AtomicU32,
    drawn: AtomicU32,
}

impl Scripted {
    fn new(script: impl IntoIterator<Item = Unwrap>) -> Arc<Self> {
        Arc::new(Scripted {
            unwraps: Mutex::new(script.into_iter().collect()),
            latency: Mutex::new(Duration::from_millis(10)),
            counted: Mutex::new(HashMap::new()),
            sentinel_there: AtomicBool::new(true),
            sentinel_draws: AtomicU32::new(0),
            drawn: AtomicU32::new(0),
        })
    }

    fn unwraps(&self) -> u32 {
        self.counted.lock().unwrap().values().sum()
    }
}

#[derive(Clone)]
struct Kms(Arc<Scripted>);

impl KeyManagementService<MemorySession> for Kms {
    async fn encrypt_dek(&self, _: &MemorySession, _: &str, dek: &Key) -> Result<Vec<u8>, Error> {
        Ok(dek.as_bytes().to_vec())
    }

    async fn decrypt_dek(
        &self,
        _: &MemorySession,
        tenant_id: &str,
        wrapped: &[u8],
    ) -> Result<Key, Error> {
        *self
            .0
            .counted
            .lock()
            .unwrap()
            .entry(wrapped.to_vec())
            .or_default() += 1;
        let latency = *self.0.latency.lock().unwrap();
        sleep(latency).await;
        let answer = self
            .0
            .unwraps
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Unwrap::Key);
        match answer {
            Unwrap::Key => Ok(Key::new(wrapped.to_vec())),
            Unwrap::Gone => Err(Error::KekNotFound {
                tenant_id: tenant_id.to_owned(),
                key_version: None,
            }),
            Unwrap::Forbidden => Err(Error::Forbidden {
                tenant_id: tenant_id.to_owned(),
                operation: "decrypt",
            }),
            Unwrap::Unreachable => Err(Error::Session(ascetic_ddd_session::SessionError::Acquire(
                "down".into(),
            ))),
        }
    }

    async fn generate_dek(
        &self,
        _: &MemorySession,
        tenant_id: &str,
    ) -> Result<(Key, Vec<u8>), Error> {
        if tenant_id == SENTINEL {
            self.0.sentinel_draws.fetch_add(1, Ordering::SeqCst);
            if !self.0.sentinel_there.load(Ordering::SeqCst) {
                return Err(Error::KekNotFound {
                    tenant_id: tenant_id.to_owned(),
                    key_version: None,
                });
            }
        }
        let n = self.0.drawn.fetch_add(1, Ordering::SeqCst) + 1;
        let mut key = vec![0u8; 32];
        key[..4].copy_from_slice(&n.to_be_bytes());
        Ok((Key::new(key.clone()), key))
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

/// A stage keeping each key a minute, with the sentinel, waiting `wait`
/// for an unwrap.
fn stage(kms: &Arc<Scripted>, wait: Duration) -> Tested {
    let opening = Opening::fitted(Duration::from_secs(60), 64).with_wait(wait);
    Arc::new(
        EnvelopeStage::new(MemorySessionPool::new(), Kms(Arc::clone(kms)))
            .with_sentinel(SENTINEL)
            .opened(opening)
            .unwrap(),
    )
}

/// A message of `tenant`, sealed by `stage`, each `n` under a key of its
/// own.
async fn sealed(stage: &Tested, tenant: &str, n: u32) -> Message {
    stage
        .outbound(
            Message::new(format!("payload {n}").into_bytes())
                .with_header(TENANT_ID, tenant)
                .with_header(MESSAGE_ID, format!("m-{n}")),
        )
        .await
        .unwrap()
}

fn cause_of_permanent(result: &Result<Message, BoxError>) -> Option<&'static str> {
    result
        .as_ref()
        .err()
        .and_then(|error| error.downcast_ref::<Permanent>())
        .map(|permanent| permanent.cause().as_str())
}

fn unavailable(result: &Result<Message, BoxError>) -> &Unavailable {
    result
        .as_ref()
        .err()
        .and_then(|error| error.downcast_ref::<Unavailable>())
        .unwrap_or_else(|| panic!("unavailable: {result:?}"))
}

#[tokio::test(start_paused = true)]
async fn concurrent_opens_of_one_message_s_key_cost_one_unwrap() {
    let kms = Scripted::new([]);
    let stage = stage(&kms, Duration::from_secs(2));
    let message = sealed(&stage, "a", 1).await;
    let opens: Vec<_> = (0..12)
        .map(|_| {
            let (stage, message) = (Arc::clone(&stage), message.clone());
            tokio::spawn(async move { stage.inbound(message).await })
        })
        .collect();
    for open in opens {
        assert_eq!(open.await.unwrap().unwrap().payload(), b"payload 1");
    }
    assert_eq!(kms.unwraps(), 1, "one unwrap for every open");
    assert_eq!(stage.opening_held().keys, 1);
}

/// An unwrap slower than the opening's wait: the open is refused over the
/// stage, of the moment; the unwrap lands on its own, and the next open is
/// served without another.
#[tokio::test(start_paused = true)]
async fn an_unwrap_slower_than_the_wait_lands_for_the_next_open() {
    let kms = Scripted::new([]);
    *kms.latency.lock().unwrap() = Duration::from_secs(3);
    let stage = stage(&kms, Duration::from_secs(2));
    let message = sealed(&stage, "a", 1).await;
    let refused = stage.inbound(message.clone()).await;
    let told = unavailable(&refused);
    assert_eq!(
        (told.cause(), told.scope()),
        (causes::UNAVAILABLE, BusScope::Stage)
    );
    sleep(Duration::from_secs(2)).await;
    assert_eq!(
        stage.inbound(message).await.unwrap().payload(),
        b"payload 1"
    );
    assert_eq!(kms.unwraps(), 1);
}

/// A key gone is told, for good under `kms.gone`, to every open of the
/// tenant within its memory, one unwrap in all; while the sentinel's key is
/// there. With the sentinel's key gone too, it is the KMS's outage: of the
/// moment over the stage, never a tenant shredded.
#[tokio::test(start_paused = true)]
async fn a_key_gone_is_told_for_good_unless_the_sentinel_is_gone_too() {
    let kms = Scripted::new([Unwrap::Gone, Unwrap::Gone]);
    let stage = stage(&kms, Duration::from_secs(2));
    let first = sealed(&stage, "a", 1).await;
    let second = sealed(&stage, "a", 2).await;
    let other_tenant = sealed(&stage, "b", 3).await;
    assert_eq!(
        cause_of_permanent(&stage.inbound(first).await),
        Some("kms.gone")
    );
    assert_eq!(
        cause_of_permanent(&stage.inbound(second).await),
        Some("kms.gone"),
        "another key of the tenant, told without an unwrap"
    );
    assert_eq!(kms.unwraps(), 1);
    assert_eq!(
        cause_of_permanent(&stage.inbound(other_tenant).await),
        Some("kms.gone"),
        "another tenant shredded"
    );
    assert_eq!(
        kms.sentinel_draws.load(Ordering::SeqCst),
        1,
        "the sentinel drawn once per gone, for both"
    );
    assert_eq!(stage.opening_held().gone, 2);

    let kms = Scripted::new([Unwrap::Gone]);
    kms.sentinel_there.store(false, Ordering::SeqCst);
    let lost = self::stage(&kms, Duration::from_secs(2));
    let message = sealed(&lost, "a", 1).await;
    let refused = lost.inbound(message).await;
    assert_eq!(cause_of_permanent(&refused), None, "never shredded");
    assert_eq!(unavailable(&refused).scope(), BusScope::Stage);
}

/// A wrapped key the KMS refused is told for good under `kms.refused` and
/// remembered: asked again within its memory, no unwrap; the tenant's other
/// keys still unwrap.
#[tokio::test(start_paused = true)]
async fn a_refused_wrapped_key_is_remembered_and_the_tenant_s_others_open() {
    let kms = Scripted::new([Unwrap::Forbidden]);
    let stage = stage(&kms, Duration::from_secs(2));
    let refused = sealed(&stage, "a", 1).await;
    let other = sealed(&stage, "a", 2).await;
    for _ in 0..3 {
        assert_eq!(
            cause_of_permanent(&stage.inbound(refused.clone()).await),
            Some("kms.refused")
        );
    }
    assert_eq!(stage.inbound(other).await.unwrap().payload(), b"payload 2");
    assert_eq!(kms.unwraps(), 2, "the refused one unwrapped once");
}

/// A KMS out of reach backs the tenant off: the next open is refused at
/// once, saying when to ask again, over the stage.
#[tokio::test(start_paused = true)]
async fn a_kms_out_of_reach_backs_the_tenant_off_and_says_when_to_ask_again() {
    let kms = Scripted::new([Unwrap::Unreachable]);
    let stage = stage(&kms, Duration::from_secs(2));
    let message = sealed(&stage, "a", 1).await;
    let first = stage.inbound(message.clone()).await;
    assert_eq!(unavailable(&first).scope(), BusScope::Stage);
    let again = stage.inbound(message).await;
    let told = unavailable(&again);
    assert!(
        told.retry_after()
            .is_some_and(|after| after > Duration::ZERO)
    );
    assert_eq!(kms.unwraps(), 1, "no unwrap while it backs off");
}

/// A key leaves memory at its end on a quiet stage: the tick wipes it.
#[tokio::test(start_paused = true)]
async fn a_key_leaves_memory_at_its_end_on_a_quiet_stage() {
    let kms = Scripted::new([]);
    let stage = stage(&kms, Duration::from_secs(2));
    let message = sealed(&stage, "a", 1).await;
    stage.inbound(message).await.unwrap();
    assert_eq!(stage.opening_held().keys, 1);
    advance(Duration::from_secs(61)).await;
    sleep(Duration::from_millis(1)).await;
    assert_eq!(stage.opening_held().keys, 0, "wiped at its end");
}

/// A caller dropped while it waits leaves nothing behind: the unwrap runs
/// on and lands, and the next open is served from it.
#[tokio::test(start_paused = true)]
async fn a_caller_dropped_while_it_waits_leaves_the_unwrap_running() {
    let kms = Scripted::new([]);
    *kms.latency.lock().unwrap() = Duration::from_secs(1);
    let stage = stage(&kms, Duration::from_secs(2));
    let message = sealed(&stage, "a", 1).await;
    let waiting = {
        let (stage, message) = (Arc::clone(&stage), message.clone());
        tokio::spawn(async move { stage.inbound(message).await })
    };
    sleep(Duration::from_millis(100)).await;
    waiting.abort();
    sleep(Duration::from_secs(2)).await;
    assert_eq!(
        stage.inbound(message).await.unwrap().payload(),
        b"payload 1"
    );
    assert_eq!(kms.unwraps(), 1);
}

/// An unwrap that never answers is given up past its bound: its tenant backs
/// off, and the next open is refused at once, saying when to ask again.
#[tokio::test(start_paused = true)]
async fn an_unwrap_that_never_answers_is_given_up_past_its_bound() {
    let kms = Scripted::new([]);
    *kms.latency.lock().unwrap() = Duration::from_secs(3600);
    let stage = stage(&kms, Duration::from_secs(2));
    let message = sealed(&stage, "a", 1).await;
    let waited = stage.inbound(message.clone()).await;
    assert_eq!(
        unavailable(&waited).retry_after(),
        None,
        "it waited its wait"
    );
    // The opening's bound on an unwrap, five seconds, passes.
    sleep(Duration::from_secs(4)).await;
    let backing = stage.inbound(message).await;
    assert!(
        unavailable(&backing)
            .retry_after()
            .is_some_and(|after| after > Duration::ZERO),
        "given up, it backs off: {backing:?}"
    );
}
