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

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ascetic_ddd_bus::{
    BoxError, Check, Dependencies, Dependency, Dependent, Message, Permanent, Scope as BusScope,
    Stage, Unavailable,
};
use ascetic_ddd_dek::stage::{MESSAGE_ID, TENANT_ID};
use ascetic_ddd_dek::{
    ByDestination, EnvelopeStage, Opening, Sealing, SealingParameters, Unroutable, causes,
};
use ascetic_ddd_kms::{Error, Key, KeyManagementService};
use ascetic_ddd_session::testing::{MemorySession, MemorySessionPool};
use ascetic_ddd_session::{SessionError, SessionPool};
use tokio::time::{advance, sleep};

const SENTINEL: &str = "sentinel";

/// What an unwrap answers.
#[derive(Clone, Copy, Debug)]
enum Unwrap {
    Key,
    Gone,
    Forbidden,
    Unreachable,
    /// A failure the KMS did not explain nor the stage attribute.
    Unexplained,
}

/// A KMS whose draws make a key whose wrapped form is itself, and whose
/// unwraps follow a script, after a latency, counted by wrapped key; the
/// sentinel's key there or not.
struct Scripted {
    unwraps: Mutex<VecDeque<Unwrap>>,
    latency: Mutex<Duration>,
    counted: Mutex<HashMap<Vec<u8>, u32>>,
    sentinel_there: AtomicBool,
    /// The sentinel's read fails: whether it is there cannot be told.
    sentinel_unreadable: AtomicBool,
    sentinel_reads: AtomicU32,
    drawn: AtomicU32,
    /// Wrapped keys whose unwrap never answers.
    hung: Mutex<HashSet<Vec<u8>>>,
}

impl Scripted {
    fn new(script: impl IntoIterator<Item = Unwrap>) -> Arc<Self> {
        Arc::new(Scripted {
            unwraps: Mutex::new(script.into_iter().collect()),
            latency: Mutex::new(Duration::from_millis(10)),
            counted: Mutex::new(HashMap::new()),
            sentinel_there: AtomicBool::new(true),
            sentinel_unreadable: AtomicBool::new(false),
            sentinel_reads: AtomicU32::new(0),
            drawn: AtomicU32::new(0),
            hung: Mutex::new(HashSet::new()),
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

    /// The sentinel's key read: there or not.
    async fn key_exists(&self, _: &MemorySession, tenant_id: &str) -> Result<bool, Error> {
        if tenant_id == SENTINEL {
            self.0.sentinel_reads.fetch_add(1, Ordering::SeqCst);
            if self.0.sentinel_unreadable.load(Ordering::SeqCst) {
                return Err(Error::Session(SessionError::Acquire("down".into())));
            }
            return Ok(self.0.sentinel_there.load(Ordering::SeqCst));
        }
        Ok(true)
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
        if self.0.hung.lock().unwrap().contains(wrapped) {
            sleep(Duration::from_secs(3_600)).await;
        }
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
            Unwrap::Unexplained => {
                Err(Error::Session(ascetic_ddd_session::SessionError::Abandoned))
            }
        }
    }

    async fn generate_dek(
        &self,
        _: &MemorySession,
        tenant_id: &str,
    ) -> Result<(Key, Vec<u8>), Error> {
        if tenant_id == SENTINEL {
            // The opening end's service only unwraps on the channel: its
            // policy refuses a draw (the host's ADR-0071 decision 4).
            return Err(Error::Forbidden {
                tenant_id: tenant_id.to_owned(),
                operation: "datakey",
            });
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
        kms.sentinel_reads.load(Ordering::SeqCst),
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

/// A wrapped key the KMS refused is told for good under the cause of its
/// remedy, `kms.forbidden` here, and remembered: asked again within its
/// memory, no unwrap, and still under that cause; the tenant's other keys
/// still unwrap.
#[tokio::test(start_paused = true)]
async fn a_refused_wrapped_key_is_remembered_and_the_tenant_s_others_open() {
    let kms = Scripted::new([Unwrap::Forbidden]);
    let stage = stage(&kms, Duration::from_secs(2));
    let refused = sealed(&stage, "a", 1).await;
    let other = sealed(&stage, "a", 2).await;
    for _ in 0..3 {
        assert_eq!(
            cause_of_permanent(&stage.inbound(refused.clone()).await),
            Some("kms.forbidden")
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

/// The open refused after a failure the KMS did not explain names that, by
/// the landing's own cause, not the KMS's unavailability: the operator's
/// remedy differs (seedwork ADR-0042 decision 9).
#[tokio::test(start_paused = true)]
async fn an_unexplained_failure_is_told_under_its_own_cause() {
    let kms = Scripted::new([Unwrap::Unexplained]);
    let stage = stage(&kms, Duration::from_secs(2));
    let message = sealed(&stage, "a", 1).await;
    let _ = stage.inbound(message.clone()).await;
    let again = stage.inbound(message).await;
    assert_eq!(unavailable(&again).cause().as_str(), "kms.unexplained");
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

// What the opening end depends on, as the inbox's breaker asks it (seedwork
// ADR-0042 decision 1).

/// The headers of `message` alone: what the inbox hands a check of its head.
fn head_of(message: &Message) -> Message {
    message
        .headers()
        .iter()
        .fold(Message::new(Vec::new()), |head, (name, value)| {
            head.with_header(name.clone(), value.clone())
        })
}

/// A check of `stage`'s dependency, of `head` if given.
async fn check(stage: &Tested, head: Option<&Message>) -> Check {
    stage.dependency().check(head.map(head_of).as_ref()).await
}

/// A check judges a head's key as a cold open of its message does: opened is
/// served; refused, or gone with the sentinel there, is about the key and
/// asked nothing of the KMS; unavailable at the stage's scope — the KMS out of
/// reach, a failure unexplained, a key gone with the sentinel gone too or not
/// to be read, an answer past `draw` — is unserved under the same cause. An
/// answer later than the opening's wait and within `draw` is the one designed
/// difference: an open is refused, as the rows' wait says, and a check, bound
/// by `draw`, is served. Red when a check judges otherwise than a flight's
/// landing — the sentinel skipped, a refusal read as the KMS's, a late answer
/// read as down.
#[tokio::test(start_paused = true)]
async fn a_check_judges_a_head_s_key_as_a_cold_open_does() {
    struct Case {
        name: &'static str,
        unwrap: Unwrap,
        latency: Duration,
        sentinel: Option<bool>,
    }
    let ms = Duration::from_millis;
    let cases = [
        Case {
            name: "opened",
            unwrap: Unwrap::Key,
            latency: ms(10),
            sentinel: Some(true),
        },
        Case {
            name: "refused",
            unwrap: Unwrap::Forbidden,
            latency: ms(10),
            sentinel: Some(true),
        },
        Case {
            name: "gone, the sentinel there",
            unwrap: Unwrap::Gone,
            latency: ms(10),
            sentinel: Some(true),
        },
        Case {
            name: "gone, the sentinel gone",
            unwrap: Unwrap::Gone,
            latency: ms(10),
            sentinel: Some(false),
        },
        Case {
            name: "gone, the sentinel unread",
            unwrap: Unwrap::Gone,
            latency: ms(10),
            sentinel: None,
        },
        Case {
            name: "out of reach",
            unwrap: Unwrap::Unreachable,
            latency: ms(10),
            sentinel: Some(true),
        },
        Case {
            name: "unexplained",
            unwrap: Unwrap::Unexplained,
            latency: ms(10),
            sentinel: Some(true),
        },
        Case {
            name: "past the bound",
            unwrap: Unwrap::Key,
            latency: ms(6_000),
            sentinel: Some(true),
        },
    ];
    for case in cases {
        let script = || {
            let kms = Scripted::new([case.unwrap]);
            *kms.latency.lock().unwrap() = case.latency;
            kms.sentinel_there
                .store(case.sentinel == Some(true), Ordering::SeqCst);
            kms.sentinel_unreadable
                .store(case.sentinel.is_none(), Ordering::SeqCst);
            kms
        };
        let (cold, checked) = (
            stage(&script(), Duration::from_secs(2)),
            stage(&script(), Duration::from_secs(2)),
        );
        let message = sealed(&cold, "a", 1).await;
        let opened = cold.inbound(message.clone()).await;
        let expected = match &opened {
            Ok(_) => Check::Served,
            Err(error) if error.is::<Permanent>() => Check::Unasked,
            Err(error) => {
                let unavailable = error.downcast_ref::<Unavailable>().expect("an Unavailable");
                assert_eq!(unavailable.scope(), BusScope::Stage, "{}", case.name);
                Check::Unserved(unavailable.cause())
            }
        };
        assert_eq!(
            check(&checked, Some(&message)).await,
            expected,
            "{}",
            case.name
        );
    }
    // later than the wait, within `draw`
    let late = || {
        let kms = Scripted::new([Unwrap::Key]);
        *kms.latency.lock().unwrap() = Duration::from_secs(3);
        kms
    };
    let (cold, checked) = (
        stage(&late(), Duration::from_secs(2)),
        stage(&late(), Duration::from_secs(2)),
    );
    let message = sealed(&cold, "a", 1).await;
    assert_eq!(
        unavailable(&cold.inbound(message.clone()).await).scope(),
        BusScope::Stage
    );
    assert_eq!(check(&checked, Some(&message)).await, Check::Served);
}

/// The control: the last key the KMS opened by a call. A fresh opening end has
/// none, and a check with no head asks nothing; a flight's key opened is it;
/// it ages from that opening, asked again or not, and goes past `keep`; a
/// head's key a check opened becomes it. Red when the control is aged from its
/// last check, when it never ages, or when a check's head does not become it.
#[tokio::test(start_paused = true)]
async fn the_control_is_the_last_key_opened_by_a_call_aged_from_its_opening() {
    let kms = Scripted::new([]);
    let stage = stage(&kms, Duration::from_secs(2));
    assert_eq!(check(&stage, None).await, Check::Unasked, "no control yet");
    let message = sealed(&stage, "a", 1).await;
    stage.inbound(message).await.unwrap();
    assert_eq!(check(&stage, None).await, Check::Served, "the flight's key");
    advance(Duration::from_secs(50)).await;
    assert_eq!(check(&stage, None).await, Check::Served, "asked again");
    advance(Duration::from_secs(11)).await;
    assert_eq!(
        check(&stage, None).await,
        Check::Unasked,
        "past `keep` from its opening, its asking notwithstanding"
    );
    let other = sealed(&stage, "b", 2).await;
    assert_eq!(check(&stage, Some(&other)).await, Check::Served);
    assert_eq!(
        check(&stage, None).await,
        Check::Served,
        "the head's key, opened"
    );
}

/// A Tenant's key gone takes the control with it; and a key refused at the
/// stage's scope is what a check without a head asks, once opened no longer.
/// Red when a gone Tenant's key stays the control, or the refused key is not
/// asked.
#[tokio::test(start_paused = true)]
async fn the_control_goes_with_its_tenant_and_a_refused_key_is_asked_without_a_head() {
    let kms = Scripted::new([Unwrap::Key, Unwrap::Gone]);
    let stage = stage(&kms, Duration::from_secs(2));
    let (first, second) = (sealed(&stage, "a", 1).await, sealed(&stage, "a", 2).await);
    stage.inbound(first).await.unwrap();
    assert_eq!(
        cause_of_permanent(&stage.inbound(second).await),
        Some("kms.gone")
    );
    assert_eq!(
        check(&stage, None).await,
        Check::Unasked,
        "the control went with a"
    );
    let kms = Scripted::new([Unwrap::Unreachable]);
    let stage = self::stage(&kms, Duration::from_secs(2));
    let refused = sealed(&stage, "b", 1).await;
    assert_eq!(
        unavailable(&stage.inbound(refused).await).scope(),
        BusScope::Stage
    );
    let asked = kms.unwraps();
    assert_eq!(
        check(&stage, None).await,
        Check::Served,
        "the refused key, asked"
    );
    assert_eq!(kms.unwraps(), asked + 1);
    assert_eq!(check(&stage, None).await, Check::Served, "now the control");
}

/// `served` counts the keys the KMS opened for the rows by a call: a flight's,
/// not a key held, not a check's. Red when a hit or a check counts.
#[tokio::test(start_paused = true)]
async fn served_counts_the_keys_opened_for_the_rows_by_a_call() {
    let kms = Scripted::new([]);
    let stage = stage(&kms, Duration::from_secs(2));
    let dependency = stage.dependency();
    let message = sealed(&stage, "a", 1).await;
    stage.inbound(message.clone()).await.unwrap();
    stage.inbound(message.clone()).await.unwrap();
    assert_eq!(dependency.served(), 1, "one flight, one key held");
    assert_eq!(check(&stage, Some(&message)).await, Check::Served);
    assert_eq!(dependency.served(), 1, "a check is no row");
    assert_eq!(
        dependency.bound(),
        Duration::from_secs(5),
        "the opening's draw"
    );
    assert!(dependency.answers_for(causes::UNAVAILABLE));
    assert!(!dependency.answers_for(ascetic_ddd_bus::cause!("crm.unavailable")));
}

/// A check served hands no landing to a failing Tenant: its next open, in its
/// back-off, still reads the stage's scope, as nothing landed for the rows.
/// Red when a check's key lands in the opening's core.
#[tokio::test(start_paused = true)]
async fn a_check_hands_a_failing_tenant_no_landing() {
    let kms = Scripted::new([Unwrap::Unreachable]);
    let stage = stage(&kms, Duration::from_secs(2));
    let failing = sealed(&stage, "a", 1).await;
    assert_eq!(
        unavailable(&stage.inbound(failing.clone()).await).scope(),
        BusScope::Stage
    );
    let healthy = sealed(&stage, "b", 2).await;
    assert_eq!(check(&stage, Some(&healthy)).await, Check::Served);
    assert_eq!(
        unavailable(&stage.inbound(failing).await).scope(),
        BusScope::Stage,
        "a landing handed over by the check"
    );
}

/// A check asks its keys at once: one that never answers does not hold the
/// other's answer, whichever it is. Red when they are asked one after the
/// other.
#[tokio::test(start_paused = true)]
async fn a_check_is_served_by_whichever_key_opens_first() {
    for control_hangs in [true, false] {
        let kms = Scripted::new([]);
        let stage = stage(&kms, Duration::from_secs(2));
        let control = sealed(&stage, "a", 1).await;
        stage.inbound(control.clone()).await.unwrap();
        let head = sealed(&stage, "b", 2).await;
        // the scripted KMS wraps the nth key drawn as itself, n in its first
        // bytes: the control's the first, the head's the second
        let mut wrapped = vec![0u8; 32];
        wrapped[..4].copy_from_slice(&(if control_hangs { 1u32 } else { 2 }).to_be_bytes());
        kms.hung.lock().unwrap().insert(wrapped);
        let _ = &control;
        let began = tokio::time::Instant::now();
        assert_eq!(check(&stage, Some(&head)).await, Check::Served);
        assert!(
            began.elapsed() < Duration::from_secs(1),
            "the control hanging: {control_hangs}; served after {:?}",
            began.elapsed()
        );
    }
}

/// A pool that hands no session while it is starved.
struct Starvable(MemorySessionPool, Arc<AtomicBool>);

impl SessionPool for Starvable {
    type Session = MemorySession;

    async fn acquire(&self) -> Result<MemorySession, SessionError> {
        if self.1.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        self.0.acquire().await
    }
}

/// A check that had no session within its bound asked nothing: no evidence,
/// though the KMS is down. Red when a session never had reads as the KMS
/// unserved.
#[tokio::test(start_paused = true)]
async fn a_check_without_a_session_asks_nothing() {
    let kms = Scripted::new([]);
    let starved = Arc::new(AtomicBool::new(false));
    let stage = Arc::new(
        EnvelopeStage::new(
            Starvable(MemorySessionPool::new(), Arc::clone(&starved)),
            Kms(Arc::clone(&kms)),
        )
        .with_sentinel(SENTINEL)
        .opened(Opening::fitted(Duration::from_secs(60), 64))
        .unwrap(),
    );
    let message = stage
        .outbound(
            Message::new(b"payload".to_vec())
                .with_header(TENANT_ID, "a")
                .with_header(MESSAGE_ID, "m-1"),
        )
        .await
        .unwrap();
    stage.inbound(message).await.unwrap();
    kms.unwraps.lock().unwrap().extend([Unwrap::Unreachable; 4]);
    starved.store(true, Ordering::SeqCst);
    assert_eq!(stage.dependency().check(None).await, Check::Unasked);
    starved.store(false, Ordering::SeqCst);
    assert_eq!(
        stage.dependency().check(None).await,
        Check::Unserved(causes::UNAVAILABLE),
        "with a session, the KMS down is unserved"
    );
}

/// The log lines a test binary wrote, by the thread that wrote each.
static LOGGED: Mutex<Vec<(std::thread::ThreadId, String)>> = Mutex::new(Vec::new());

struct Logged;

impl log::Log for Logged {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }
    fn log(&self, record: &log::Record<'_>) {
        LOGGED
            .lock()
            .unwrap()
            .push((std::thread::current().id(), record.args().to_string()));
    }
    fn flush(&self) {}
}

/// A check's outcomes name no Tenant in a log line: the KMS's errors name
/// one, and a check keeps and logs none of their text (R11). Red when a check
/// logs an error it met.
#[tokio::test(start_paused = true)]
async fn a_check_names_no_tenant_in_the_log() {
    static LOGGER: Logged = Logged;
    let _ = log::set_logger(&LOGGER);
    log::set_max_level(log::LevelFilter::Trace);
    const TENANT: &str = "tenant-whose-name-is-private";
    for unwrap in [
        Unwrap::Gone,
        Unwrap::Forbidden,
        Unwrap::Unreachable,
        Unwrap::Unexplained,
    ] {
        for sentinel in [true, false] {
            let kms = Scripted::new([unwrap]);
            kms.sentinel_there.store(sentinel, Ordering::SeqCst);
            let stage = stage(&kms, Duration::from_secs(2));
            let message = sealed(&stage, TENANT, 1).await;
            let _ = check(&stage, Some(&message)).await;
        }
    }
    let this = std::thread::current().id();
    let named: Vec<String> = LOGGED
        .lock()
        .unwrap()
        .iter()
        .filter(|(thread, line)| *thread == this && line.contains(TENANT))
        .map(|(_, line)| line.clone())
        .collect();
    assert!(named.is_empty(), "{named:?}");
}

/// The dependencies an inbox is given, by destination, its key left aside:
/// destinations named apart from their channels, two sharing one channel's
/// stage sharing its dependency. Red when they are keyed by the channel, or
/// with the destination's key.
#[test]
fn the_dependencies_are_by_destination_without_its_key() {
    let kms = Scripted::new([]);
    let sealing = Sealing::over(
        Arc::new(MemorySessionPool::new()),
        ["chan-a", "chan-b"],
        SealingParameters::default(),
        |_: &str| Kms(Arc::clone(&kms)),
    );
    let opening = sealing
        .by_destination([
            ("inbox://in-a/the-key", "chan-a"),
            ("inbox://in-a2", "chan-a"),
            ("inbox://in-b", "chan-b"),
        ])
        .unwrap();
    let dependencies = opening.dependencies();
    let each: Vec<(&str, &Arc<dyn Dependency>)> = dependencies.iter().collect();
    let names: Vec<&str> = each.iter().map(|(d, _)| *d).collect();
    assert_eq!(names, ["inbox://in-a", "inbox://in-a2", "inbox://in-b"]);
    assert!(
        Arc::ptr_eq(each[0].1, each[1].1),
        "one channel, one dependency"
    );
    assert!(!Arc::ptr_eq(each[0].1, each[2].1));
}

/// A stage's dependency is one object for its opener's life, whichever way
/// it is reached — the stage itself, a sealing's map built twice, a stage
/// over it built twice — so that a subscription through any of them is no
/// conflict with another, and a subscription made again is none (seedwork
/// ADR-0044, S6). Red when the dependency is made anew at each call.
#[test]
fn a_stage_s_dependency_is_one_object_however_it_is_reached() {
    let kms = Scripted::new([]);
    let one = stage(&kms, Duration::from_secs(2));
    assert!(Arc::ptr_eq(&one.dependency(), &one.dependency()));
    let over = |destinations: [&str; 2]| ByDestination::over(Arc::clone(&one), destinations);
    let (first, second) = (
        over(["inbox://in-a/k1", "inbox://in-b"]),
        over(["inbox://in-a", "inbox://in-c"]),
    );
    let merged = first
        .dependencies()
        .merge(second.dependencies())
        .expect("one dependency, however reached");
    assert_eq!(
        merged.destinations().collect::<Vec<_>>(),
        ["inbox://in-a", "inbox://in-b", "inbox://in-c"]
    );
    assert!(
        merged
            .iter()
            .all(|(_, dependency)| Arc::ptr_eq(dependency, &one.dependency()))
    );

    let sealing = Sealing::over(
        Arc::new(MemorySessionPool::new()),
        ["chan-a"],
        SealingParameters::default(),
        |_: &str| Kms(Arc::clone(&kms)),
    );
    let map = || {
        sealing
            .by_destination([("inbox://in-a", "chan-a")])
            .unwrap()
    };
    map()
        .dependencies()
        .merge(map().dependencies())
        .expect("the sealing's map built twice declares one dependency");
}

/// The dependency is the opener's that opens, whichever builder made the
/// opener last: a sealing builds its stages opened, then with the sentinel
/// (seedwork ADR-0044). Red when a builder that makes a new opener keeps the
/// old one's dependency: its `served` would stay at none.
#[tokio::test(start_paused = true)]
async fn the_dependency_is_the_opener_s_that_opens() {
    let kms = Scripted::new([]);
    let stage: Tested = Arc::new(
        EnvelopeStage::new(MemorySessionPool::new(), Kms(Arc::clone(&kms)))
            .opened(Opening::fitted(Duration::from_secs(60), 64))
            .unwrap()
            .with_sentinel(SENTINEL),
    );
    let message = sealed(&stage, "a", 1).await;
    stage.inbound(message).await.unwrap();
    assert_eq!(stage.dependency().served(), 1);
}

/// Two destinations that differ by their key only, mapped to two channels,
/// are refused at construction: a row of either would open under one of
/// them; mapped to one channel they are one destination (seedwork ADR-0044,
/// S6). Red when the map keeps the last.
#[test]
fn two_destinations_differing_by_their_key_under_two_channels_are_refused() {
    let kms = Scripted::new([]);
    let sealing = Sealing::over(
        Arc::new(MemorySessionPool::new()),
        ["chan-a", "chan-b"],
        SealingParameters::default(),
        |_: &str| Kms(Arc::clone(&kms)),
    );
    let refused = sealing
        .by_destination([("inbox://in-a/k1", "chan-a"), ("inbox://in-a/k2", "chan-b")])
        .expect_err("refused");
    assert_eq!(
        refused,
        Unroutable::Ambiguous {
            destination: "inbox://in-a".to_owned()
        }
    );
    let one = sealing
        .by_destination([("inbox://in-a/k1", "chan-a"), ("inbox://in-a/k2", "chan-a")])
        .unwrap();
    assert_eq!(one.destinations().collect::<Vec<_>>(), ["inbox://in-a"]);
}

/// Whether a type states its dependencies, told by an ambiguity: a type that
/// is `Dependent` has two impls below, and the call in the test cannot pick.
trait Declaring<Which> {
    fn told() {}
}

impl<T: ?Sized> Declaring<()> for T {}

struct Declared;

impl<T: ?Sized + Dependent> Declaring<Declared> for T {}

/// The envelope stage of one channel states no dependency: it does not know
/// the destinations an inbox receives it under, and so cannot go in on an
/// inbox (seedwork ADR-0044, S5b). Red at compile time when it is given one.
#[test]
fn an_envelope_stage_states_no_dependency_of_its_own() {
    <EnvelopeStage<MemorySessionPool, Kms> as Declaring<_>>::told();
    // its twin, which does state them, is ambiguous and is not written here:
    // `<ByDestination as Declaring<_>>::told()` does not compile
    let _ = Dependencies::none();
}
