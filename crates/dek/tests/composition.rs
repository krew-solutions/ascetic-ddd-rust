//! The envelope stage and the inbox composed as a host composes them (seedwork
//! ADR-0042 decision 1, ADR-0044): the opening stage by destination, the
//! consumer through it — and nothing else: the subscription registers the
//! stage's dependencies. With the KMS down and keys of some Tenants still
//! held, a channel the stage's refusals paused stays paused — its rows served
//! from the stage's memory are no evidence of the KMS — and resumes once a
//! check finds the KMS opening a key again. It needs a live database:
//!
//! ```text
//! ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
//!     cargo test -p ascetic-ddd-dek --features bus --test composition -- --ignored
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "tests"
)]

use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ascetic_ddd_bus::identity::{STREAM_ID, STREAM_POSITION, STREAM_TYPE, TENANT_ID};
use ascetic_ddd_bus::{Adapter, Message};
use ascetic_ddd_dek::stage::MESSAGE_ID;
use ascetic_ddd_dek::{Sealing, SealingParameters};
use ascetic_ddd_inbox::breaker::{Effect, Resumption};
use ascetic_ddd_inbox::observer::{BreakerChanged, InboxObserver};
use ascetic_ddd_inbox::{Breaking, ByStream, Loops, PgInbox};
use ascetic_ddd_kms::{Error, Key, KeyManagementService};
use ascetic_ddd_session::pg::Identifier;
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};
use ascetic_ddd_session::testing::{MemorySession, MemorySessionPool};
use ascetic_ddd_session::{PgSessionPool, SessionError};

const DEFAULT_URL: &str = "postgresql://devel:devel@localhost:5432/devel_karmabot_test";

/// The channel the producers seal under, and the inbox's destinations that open
/// under it: named apart, one with a key, two sharing the channel's stage.
const SEALED: &str = "orders.sealed";
const DESTINATIONS: [&str; 2] = ["inbox://orders/the-key", "inbox://orders-b"];

fn pool(size: usize) -> Pool {
    let url = std::env::var("ASCETIC_DDD_TEST_PG_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned());
    let manager = Manager::from_config(
        Config::from_str(&url).expect("a valid PostgreSQL URL"),
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    Pool::builder(manager).max_size(size).build().unwrap()
}

/// A KMS that wraps a key as itself and refuses every call while down.
#[derive(Clone)]
struct Kms(Arc<AtomicBool>, Arc<AtomicU64>);

impl KeyManagementService<MemorySession> for Kms {
    async fn encrypt_dek(&self, _: &MemorySession, _: &str, dek: &Key) -> Result<Vec<u8>, Error> {
        Ok(dek.as_bytes().to_vec())
    }

    async fn key_exists(&self, _: &MemorySession, _: &str) -> Result<bool, Error> {
        Ok(true)
    }

    async fn decrypt_dek(&self, _: &MemorySession, _: &str, wrapped: &[u8]) -> Result<Key, Error> {
        if self.0.load(Ordering::SeqCst) {
            return Err(Error::Session(SessionError::Acquire("down".into())));
        }
        Ok(Key::new(wrapped.to_vec()))
    }

    async fn generate_dek(&self, _: &MemorySession, _: &str) -> Result<(Key, Vec<u8>), Error> {
        let n = self.1.fetch_add(1, Ordering::SeqCst) + 1;
        let mut key = vec![0u8; 32];
        key[..8].copy_from_slice(&n.to_be_bytes());
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

/// The breaker's effects, as the inbox told them.
#[derive(Default)]
struct Effects(Mutex<Vec<Effect>>);

impl InboxObserver for Effects {
    fn on_breaker(&self, event: &BreakerChanged<'_>) {
        self.0.lock().unwrap().push(event.effect.clone());
    }
}

impl Effects {
    fn count(&self, of: impl Fn(&Effect) -> bool) -> usize {
        self.0.lock().unwrap().iter().filter(|e| of(e)).count()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_channel_paused_by_its_stage_s_kms_stays_paused_on_the_keys_held() {
    const TABLE: &str = "dek_composition_inbox";
    let down = Arc::new(AtomicBool::new(false));
    let sealing = Sealing::over(
        Arc::new(MemorySessionPool::new()),
        [SEALED],
        SealingParameters::default(),
        {
            let (down, drawn) = (Arc::clone(&down), Arc::new(AtomicU64::new(0)));
            move |_: &str| Kms(Arc::clone(&down), Arc::clone(&drawn))
        },
    );
    let opening = sealing
        .by_destination(DESTINATIONS.map(|destination| (destination, SEALED)))
        .unwrap();
    let effects = Arc::new(Effects::default());
    let inbox = Arc::new(
        PgInbox::new(PgSessionPool::new(pool(8)))
            .with_table(
                Identifier::new(TABLE).unwrap(),
                Identifier::new(format!("{TABLE}_seq")).unwrap(),
            )
            .with_slots(4)
            .partitioned_by(ByStream)
            .with_loops(Loops {
                concurrency: 4,
                poll_interval: Duration::from_millis(50),
                max_error_wait: Duration::from_secs(1),
            })
            .with_breaking(
                Breaking::default()
                    .with_hold(Duration::from_millis(40), Duration::from_millis(200))
                    .with_tick(Duration::from_millis(50), Duration::from_millis(200)),
            )
            .observed_by(Arc::clone(&effects)),
    );
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    assert!(inbox.dependent_channels().is_empty());
    let processed = Arc::new(AtomicU64::new(0));
    let subscription = {
        let processed = Arc::clone(&processed);
        inbox
            .consumer(|message: &Message| Ok(message.payload().to_vec()))
            .through(Arc::new(opening))
            .subscribe(move |_tx, _payload: Vec<u8>| {
                let processed = Arc::clone(&processed);
                async move {
                    processed.fetch_add(1, Ordering::SeqCst);
                }
            })
            .unwrap()
    };
    // both destinations, keys aside, registered by the subscription
    assert_eq!(
        inbox.dependent_channels(),
        ["inbox://orders", "inbox://orders-b"]
    );
    let stage = sealing.of(SEALED).unwrap();
    let intake = inbox.channel();
    let made = AtomicU64::new(0);
    let seal = async |tenant: &str| {
        let n = made.fetch_add(1, Ordering::SeqCst) + 1;
        stage
            .outbound(
                Message::new(format!("m{n}").into_bytes())
                    .with_header(TENANT_ID, tenant)
                    .with_header(STREAM_TYPE, "orders.Order")
                    .with_header(STREAM_ID, format!("{tenant}-{n}"))
                    .with_header(STREAM_POSITION, "1")
                    .with_header(MESSAGE_ID, format!("00000000-0000-4000-8000-{n:012x}")),
            )
            .await
            .unwrap()
    };
    let publish = async |destination: &str, message: Message| {
        intake
            .producer(destination)
            .unwrap()
            .publish(message)
            .await
            .unwrap();
    };
    // warm: the keys of w0..w2 opened, held by the stage
    for tenant in ["w0", "w1", "w2"] {
        let message = seal(tenant).await;
        publish(DESTINATIONS[0], message).await;
    }
    let began = tokio::time::Instant::now();
    while processed.load(Ordering::SeqCst) < 3 {
        assert!(
            began.elapsed() < Duration::from_secs(20),
            "the warm rows not served"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // sealed before the outage, opened after: new Tenants' keys, and more of
    // the warm ones', on both destinations
    let mut fresh = Vec::new();
    for n in 0..12 {
        for destination in DESTINATIONS {
            fresh.push((destination, seal(&format!("n{n}")).await));
        }
    }
    // the warm Tenants' keys held, on both destinations
    let mut warm = Vec::new();
    for destination in DESTINATIONS {
        for tenant in ["w0", "w1", "w2"] {
            warm.push((destination, seal(tenant).await));
        }
    }
    down.store(true, Ordering::SeqCst);
    for (destination, message) in fresh {
        publish(destination, message).await;
    }
    let paused = |e: &Effect| matches!(e, Effect::Paused { .. });
    let began = tokio::time::Instant::now();
    while effects.count(paused) < 2 {
        assert!(
            began.elapsed() < Duration::from_secs(30),
            "both destinations not paused: {:?}",
            effects.0.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for (destination, message) in warm {
        publish(destination, message).await;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    let resumed = |e: &Effect| matches!(e, Effect::Resumed { .. });
    assert_eq!(
        effects.count(resumed),
        0,
        "resumed with the KMS down: {:?}",
        effects.0.lock().unwrap()
    );
    down.store(false, Ordering::SeqCst);
    let began = tokio::time::Instant::now();
    while effects.count(resumed) < 2 || processed.load(Ordering::SeqCst) < 3 + 24 + 6 {
        assert!(
            began.elapsed() < Duration::from_secs(30),
            "not resumed with the KMS back: {} processed, {:?}",
            processed.load(Ordering::SeqCst),
            effects.0.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // each pause ended by a check of the KMS, none by a row the stage's
    // memory served
    let by_a_row =
        |e: &Effect| matches!(e, Effect::Resumed { by, .. } if *by != Resumption::Served);
    assert_eq!(
        effects.count(by_a_row),
        0,
        "resumed by other than a check: {:?}",
        effects.0.lock().unwrap()
    );
    subscription.cancel().await;
    inbox.teardown().await.unwrap();
}
