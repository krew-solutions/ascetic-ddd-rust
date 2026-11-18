//! M5 and M6 of the inbox's ADR-0042: an outage of the KMS, drilled for an
//! hour at a scale below the host's, through the path a host's service
//! runs — producers sealing each channel's messages with the channel's
//! stage and storing them in the inbox, three consuming processes of eight
//! loops each opening them through the sealing's stage by destination, a
//! breaker each, a handler of 50 ms — over a KMS whose answers follow a
//! script: up, then down for a third of the hour, then up again under a
//! quota of calls a second below what draining the backlog asks. A launch
//! Tenant's 100,000 messages arrive in the two minutes before the outage,
//! faster than the consumers drain them, so that the outage meets a
//! backlog whose keys the consumers hold: a producer cannot seal while
//! the KMS is down, and without a backlog nothing would be asked. A measurement, run by hand on one
//! machine, against a database of one's own:
//!
//! ```text
//! ASCETIC_DDD_MEASURE=1 ASCETIC_DDD_TEST_PG_URL=… cargo test -p ascetic-ddd-dek \
//!     --test drill -- --ignored --nocapture
//! ```
//!
//! `ASCETIC_DDD_MEASURE=small` runs it in six minutes instead of sixty, to
//! see that it runs; its figures mean nothing. Every ten seconds a line of
//! counts; at the end what the drill decides: nothing parked and no attempt
//! spent for the outage; the KMS asked a bounded number of times while it
//! was down; the backlog drained under the quota, and how long it took;
//! and (M6) the share of the messages stored during the outage whose key a
//! consumer had unwrapped within the opening's keep — what the cache could
//! have served, had the channel not been paused.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "a measurement"
)]

use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ascetic_ddd_bus::identity::{STREAM_ID, STREAM_POSITION, STREAM_TYPE, TENANT_ID};
use ascetic_ddd_bus::{Adapter, Message, Stage};
use ascetic_ddd_dek::stage::MESSAGE_ID;
use ascetic_ddd_dek::{Sealing, SealingParameters};
use ascetic_ddd_inbox::breaker::Effect;
use ascetic_ddd_inbox::observer::{BreakerChanged, Dispatched, Failed, InboxObserver, Withheld};
use ascetic_ddd_inbox::{Loops, Outcome, PgInbox};
use ascetic_ddd_kms::{Error, Key, KeyManagementService};
use ascetic_ddd_session::pg::Identifier;
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};
use ascetic_ddd_session::testing::{MemorySession, MemorySessionPool};
use ascetic_ddd_session::{PgSessionPool, SessionError};
use tokio::time::Instant;

const DEFAULT_URL: &str = "postgresql://devel:devel@localhost:5432/devel_karmabot_test";
const CHANNELS: usize = 7;
const TENANTS: usize = 500;
const STREAMS: usize = 3;
const PROCESSES: usize = 3;
const LOOPS: u32 = 8;
const POLL: Duration = Duration::from_millis(200);
/// Messages a second, sealed and stored, of Tenants at random.
const RATE: u64 = 20;
/// The launch Tenant's messages, in the two minutes before the outage, by
/// four producers.
const LAUNCH: u64 = 100_000;
const LAUNCHERS: u64 = 4;
/// What the handler does in the transaction, besides the inbox.
const HANDLER: Duration = Duration::from_millis(50);
/// The KMS's quota, calls a second: above the steady draws and unwraps,
/// some 40, below what the drain asks.
const QUOTA: f64 = 60.0;
const TABLE: &str = "dek_drill_inbox";

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

fn channel(c: usize) -> String {
    format!("inbox://c{c}")
}

/// Standard base64, as the stage writes a wrapped key in the `dek` header.
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// The KMS's script and what it was asked.
struct Script {
    started: Instant,
    down: (Duration, Duration),
    bucket: Mutex<(f64, Instant)>,
    draws: AtomicU64,
    unwraps: AtomicU64,
    refused_down: AtomicU64,
    unwraps_down: AtomicU64,
    throttled: AtomicU64,
    /// When each process last unwrapped each wrapped key, by the key's
    /// base64 (M6).
    unwrapped: Mutex<HashMap<String, [Option<Instant>; PROCESSES]>>,
    /// A consuming process cut off from the KMS alone, for the whole run
    /// (M8): its unwraps refused as an unreachable KMS's are.
    cut: Option<usize>,
    /// The one Tenant whose unwraps the process cut off is refused, if not
    /// every one (M8's `lone` shape).
    cut_tenant: Option<String>,
    /// Calls a second the KMS answers: [`QUOTA`] for M5, none for M8.
    quota: f64,
    /// While down, a call hangs until the client's timeout, [`HUNG`], and is
    /// then refused (M10), rather than refused at once.
    hang: bool,
    /// The percentage of Tenants whose unwraps alone the outage refuses, the
    /// KMS answering every other call (M10's partial shape), if not every
    /// call.
    partial: Option<u64>,
    /// While down, every call answered after this long rather than refused
    /// (M10's slow shape: a brownout, not an outage).
    slow: Option<Duration>,
}

/// Whether `tenant` is among the `percent` of Tenants a partial outage
/// refuses: by a hash of its id, the same in every process.
fn broken(tenant: &str, percent: u64) -> bool {
    tenant.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    }) % 100
        < percent
}

/// How long a call to a KMS that hangs waits before its client gives up (M10).
const HUNG: Duration = Duration::from_secs(30);

impl Script {
    fn new(started: Instant, down: (Duration, Duration), cut: Option<usize>) -> Self {
        Script {
            started,
            down,
            bucket: Mutex::new((QUOTA, Instant::now())),
            draws: AtomicU64::new(0),
            unwraps: AtomicU64::new(0),
            refused_down: AtomicU64::new(0),
            unwraps_down: AtomicU64::new(0),
            throttled: AtomicU64::new(0),
            unwrapped: Mutex::new(HashMap::new()),
            cut,
            cut_tenant: None,
            quota: QUOTA,
            hang: false,
            partial: None,
            slow: None,
        }
    }

    fn down(&self) -> bool {
        let at = self.started.elapsed();
        at >= self.down.0 && at < self.down.1
    }

    /// Whether the outage refuses a call: every call, or under a partial
    /// outage only an unwrap of a broken Tenant's key.
    fn down_for(&self, tenant: Option<&str>) -> bool {
        self.slow.is_none()
            && self.down()
            && match (self.partial, tenant) {
                (None, _) => true,
                (Some(_), None) => false,
                (Some(percent), Some(tenant)) => broken(tenant, percent),
            }
    }

    /// A call of a KMS that hangs while down: it waits for the client's
    /// timeout, or the outage's end if sooner, and is then refused.
    async fn hung(&self, tenant: Option<&str>) -> Result<(), Error> {
        if let Some(slow) = self.slow.filter(|_| self.down()) {
            tokio::time::sleep(slow).await;
        }
        if self.hang && self.down_for(tenant) {
            let left = self.down.1.saturating_sub(self.started.elapsed());
            tokio::time::sleep(left.min(HUNG)).await;
            self.refused_down.fetch_add(1, Ordering::Relaxed);
            return Err(Error::Session(SessionError::Acquire("timed out".into())));
        }
        self.call_for(tenant)
    }

    /// A call answered, or refused as an unreachable or rate-limited KMS
    /// is: of the class `Unavailable`.
    fn call(&self) -> Result<(), Error> {
        self.call_for(None)
    }

    fn call_for(&self, tenant: Option<&str>) -> Result<(), Error> {
        if self.down_for(tenant) {
            self.refused_down.fetch_add(1, Ordering::Relaxed);
            return Err(Error::Session(SessionError::Acquire("down".into())));
        }
        let mut bucket = self.bucket.lock().unwrap();
        let now = Instant::now();
        bucket.0 =
            (bucket.0 + now.duration_since(bucket.1).as_secs_f64() * self.quota).min(self.quota);
        bucket.1 = now;
        if bucket.0 < 1.0 {
            self.throttled.fetch_add(1, Ordering::Relaxed);
            return Err(Error::Session(SessionError::Acquire("quota".into())));
        }
        bucket.0 -= 1.0;
        Ok(())
    }
}

/// The KMS as one process sees it: the producers' as `PROCESSES`.
#[derive(Clone)]
struct Kms(Arc<Script>, usize);

impl KeyManagementService<MemorySession> for Kms {
    async fn encrypt_dek(&self, _: &MemorySession, _: &str, dek: &Key) -> Result<Vec<u8>, Error> {
        Ok(dek.as_bytes().to_vec())
    }

    async fn key_exists(&self, _: &MemorySession, _: &str) -> Result<bool, Error> {
        self.0.call()?;
        Ok(true)
    }

    async fn decrypt_dek(
        &self,
        _: &MemorySession,
        tenant: &str,
        wrapped: &[u8],
    ) -> Result<Key, Error> {
        if self.0.cut == Some(self.1) && self.0.cut_tenant.as_deref().is_none_or(|t| t == tenant) {
            self.0.refused_down.fetch_add(1, Ordering::Relaxed);
            return Err(Error::Session(SessionError::Acquire("cut off".into())));
        }
        if self.0.down_for(Some(tenant)) {
            self.0.unwraps_down.fetch_add(1, Ordering::Relaxed);
        }
        self.0.hung(Some(tenant)).await?;
        self.0.unwraps.fetch_add(1, Ordering::Relaxed);
        if self.1 < PROCESSES {
            self.0
                .unwrapped
                .lock()
                .unwrap()
                .entry(base64(wrapped))
                .or_default()[self.1] = Some(Instant::now());
        }
        Ok(Key::new(wrapped.to_vec()))
    }

    async fn generate_dek(&self, _: &MemorySession, _: &str) -> Result<(Key, Vec<u8>), Error> {
        self.0.hung(None).await?;
        let n = self.0.draws.fetch_add(1, Ordering::Relaxed) + 1;
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

/// What a consuming process's inbox told.
#[derive(Default)]
struct Told {
    process: usize,
    started: Option<Instant>,
    processed: AtomicU64,
    held: AtomicU64,
    spent: AtomicU64,
    parked: AtomicU64,
    paused: AtomicU64,
    resumed: AtomicU64,
    trials: AtomicU64,
    guests: AtomicU64,
    /// Heads held without their subscriber called (M8).
    withheld: AtomicU64,
    /// Resumes by what closed the channel (M10).
    by_processed: AtomicU64,
    by_inconclusive: AtomicU64,
    by_no_witness: AtomicU64,
    by_served: AtomicU64,
    by_nothing: AtomicU64,
    /// The dependency's checks issued and its suspicions refuted (M10).
    checks: AtomicU64,
    refuted: AtomicU64,
    /// The outage's window, seconds into the run, and the resumes within it.
    down: (f64, f64),
    resumed_down: AtomicU64,
    /// When the last resume came, seconds into the run.
    last_resume: Mutex<Option<f64>>,
    /// Stall effects, and when the first came, seconds into the run (M10).
    stalled: AtomicU64,
    first_stall: Mutex<Option<f64>>,
    lines: Mutex<Vec<String>>,
}

impl InboxObserver for Told {
    fn on_withheld(&self, _: &Withheld<'_>) {
        self.withheld.fetch_add(1, Ordering::Relaxed);
    }

    fn on_failed(&self, event: &Failed<'_>) {
        if event.parked {
            self.parked.fetch_add(1, Ordering::Relaxed);
        } else if event.held {
            self.held.fetch_add(1, Ordering::Relaxed);
        } else {
            self.spent.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn on_breaker(&self, event: &BreakerChanged<'_>) {
        let at = self.started.map_or(0.0, |s| s.elapsed().as_secs_f64());
        match event.effect {
            Effect::Paused { .. } => {
                self.paused.fetch_add(1, Ordering::Relaxed);
                self.lines.lock().unwrap().push(format!(
                    "{at:7.1} s process {}: paused {:?}",
                    self.process, event.effect
                ));
            }
            Effect::Resumed { by, .. } => {
                self.resumed.fetch_add(1, Ordering::Relaxed);
                if at >= self.down.0 && at < self.down.1 {
                    self.resumed_down.fetch_add(1, Ordering::Relaxed);
                }
                *self.last_resume.lock().unwrap() = Some(at);
                match by {
                    ascetic_ddd_inbox::breaker::Resumption::Processed => &self.by_processed,
                    ascetic_ddd_inbox::breaker::Resumption::Inconclusive => &self.by_inconclusive,
                    ascetic_ddd_inbox::breaker::Resumption::NoWitness => &self.by_no_witness,
                    ascetic_ddd_inbox::breaker::Resumption::Served => &self.by_served,
                    ascetic_ddd_inbox::breaker::Resumption::NothingToHold => &self.by_nothing,
                }
                .fetch_add(1, Ordering::Relaxed);
                self.lines.lock().unwrap().push(format!(
                    "{at:7.1} s process {}: resumed {:?}",
                    self.process, event.effect
                ));
            }
            Effect::Trial { .. } => {
                self.trials.fetch_add(1, Ordering::Relaxed);
            }
            Effect::Check { .. } => {
                self.checks.fetch_add(1, Ordering::Relaxed);
            }
            Effect::Refuted { .. } => {
                self.refuted.fetch_add(1, Ordering::Relaxed);
            }
            Effect::Stalled { .. } => {
                self.stalled.fetch_add(1, Ordering::Relaxed);
                self.first_stall.lock().unwrap().get_or_insert(at);
            }
        }
    }

    fn on_dispatched(&self, event: &Dispatched<'_>) {
        match event.outcome {
            Ok(Outcome::Processed) => {
                self.processed.fetch_add(1, Ordering::Relaxed);
            }
            Ok(Outcome::Paused) => {
                self.guests.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }
}

fn sum(told: &[Arc<Told>], of: impl Fn(&Told) -> &AtomicU64) -> u64 {
    told.iter().map(|t| of(t).load(Ordering::Relaxed)).sum()
}

/// Milliseconds since the drill's first look at the clock: a message
/// carries when it was made, its handler reads how long it waited.
fn millis_now() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    EPOCH.get_or_init(Instant::now).elapsed().as_millis() as u64
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "a measurement (the inbox's ADR-0042 M5, M6): run by hand"]
async fn an_outage_of_the_kms_drilled_for_an_hour() {
    let Ok(asked) = std::env::var("ASCETIC_DDD_MEASURE") else {
        eprintln!("a measurement: set ASCETIC_DDD_MEASURE to run it");
        return;
    };
    let minute = if asked == "small" {
        Duration::from_secs(6)
    } else {
        Duration::from_secs(60)
    };
    let (steady, outage, recovery) = (minute * 10, minute * 20, minute * 30);
    // the small run's launch a tenth, its window a tenth too
    let launch = if asked == "small" {
        LAUNCH / 10
    } else {
        LAUNCH
    };
    let started = Instant::now();
    let script = Arc::new(Script::new(started, (steady, steady + outage), None));
    let channels: Vec<String> = (0..CHANNELS).map(channel).collect();
    let parameters = SealingParameters::default();
    let sealing = |process: usize| {
        Sealing::over(
            Arc::new(MemorySessionPool::new()),
            channels.clone(),
            parameters,
            |_: &str| Kms(Arc::clone(&script), process),
        )
    };
    let producing: Sealing = sealing(PROCESSES);
    // The table, made by the first process's inbox.
    let told: Vec<Arc<Told>> = (0..PROCESSES)
        .map(|process| {
            Arc::new(Told {
                process,
                started: Some(started),
                ..Told::default()
            })
        })
        .collect();
    let inboxes: Vec<Arc<PgInbox<PgSessionPool, Arc<Told>>>> = told
        .iter()
        .map(|told| {
            Arc::new(
                PgInbox::new(PgSessionPool::new(pool(LOOPS as usize + 4)))
                    .with_table(
                        Identifier::new(TABLE).unwrap(),
                        Identifier::new(format!("{TABLE}_seq")).unwrap(),
                    )
                    .with_slots(64)
                    .partitioned_by(ascetic_ddd_inbox::ByStream)
                    .with_loops(Loops {
                        concurrency: LOOPS,
                        poll_interval: POLL,
                        max_error_wait: Duration::from_secs(1),
                    })
                    .observed_by(Arc::clone(told)),
            )
        })
        .collect();
    inboxes[0].teardown().await.unwrap();
    inboxes[0].migrate().await.unwrap();
    // The consumers: each process's own sealing, its own cache of keys.
    let latencies: Arc<Mutex<Vec<(Instant, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let mut subscriptions = Vec::new();
    for (process, inbox) in inboxes.iter().enumerate() {
        let opening = sealing(process)
            .by_destination(channels.iter().map(|c| (c.clone(), c.clone())))
            .unwrap();
        let latencies = Arc::clone(&latencies);
        let subscription = inbox
            .consumer(|message: &Message| {
                Ok(std::str::from_utf8(message.payload())?.parse::<u64>()?)
            })
            .through(Arc::new(opening) as Arc<dyn Stage>)
            .subscribe(move |_tx, sent: u64| {
                let latencies = Arc::clone(&latencies);
                async move {
                    tokio::time::sleep(HANDLER).await;
                    latencies
                        .lock()
                        .unwrap()
                        .push((Instant::now(), millis_now().saturating_sub(sent)));
                }
            })
            .unwrap();
        subscriptions.push(subscription);
    }
    // The producer: one queue in order, as an outbox is, sealing its head
    // and storing it; a head that cannot be sealed waits a second.
    let intake = inboxes[0].channel();
    let stored = Arc::new(AtomicU64::new(0));
    let pending = Arc::new(AtomicU64::new(0));
    let end = steady + outage + recovery;
    let producer = {
        let (stored, pending) = (Arc::clone(&stored), Arc::clone(&pending));
        let producing = producing.clone();
        async move {
            let mut queue: VecDeque<(usize, Message)> = VecDeque::new();
            let mut positions: HashMap<(usize, usize, usize), u64> = HashMap::new();
            let mut made = 0u64;
            let mut seed = 0x9E37_79B9_7F4A_7C15_u64;
            let mut next = move |n: usize| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                (seed % n as u64) as usize
            };
            let mut tick = tokio::time::interval(Duration::from_millis(1000 / RATE));
            while started.elapsed() < end {
                tick.tick().await;
                let (t, c, s) = (next(TENANTS), next(CHANNELS), next(STREAMS));
                let position = positions.entry((t, c, s)).or_default();
                *position += 1;
                made += 1;
                let message = Message::new(millis_now().to_string().into_bytes())
                    .with_header(TENANT_ID, format!("t{t:03}"))
                    .with_header(STREAM_TYPE, "orders.Order")
                    .with_header(STREAM_ID, format!("c{c}-s{s}"))
                    .with_header(STREAM_POSITION, position.to_string())
                    .with_header(MESSAGE_ID, format!("00000000-0000-4000-8000-{made:012x}"));
                queue.push_back((c, message));
                while let Some((c, message)) = queue.front() {
                    let stage = producing.of(&channel(*c)).unwrap();
                    let Ok(sealed) = stage.outbound(message.clone()).await else {
                        break;
                    };
                    intake
                        .producer(&format!("{}/{}", channel(*c), "k"))
                        .unwrap()
                        .publish(sealed)
                        .await
                        .unwrap();
                    stored.fetch_add(1, Ordering::Relaxed);
                    queue.pop_front();
                }
                pending.store(queue.len() as u64, Ordering::Relaxed);
            }
        }
    };
    // The launch: four producers of the launch Tenant's messages, each its
    // own streams, in the two minutes before the outage.
    let launchers = (0..LAUNCHERS).map(|l| {
        let (stored, producing) = (Arc::clone(&stored), producing.clone());
        let intake = inboxes[0].channel();
        async move {
            tokio::time::sleep(steady.saturating_sub(minute * 2)).await;
            let window = minute * 2;
            let each = launch / LAUNCHERS;
            let begun = Instant::now();
            for n in 0..each {
                let due = window.mul_f64(n as f64 / each as f64);
                if let Some(rest) = due.checked_sub(begun.elapsed()) {
                    tokio::time::sleep(rest).await;
                }
                let c = (n % CHANNELS as u64) as usize;
                let stream = (n / CHANNELS as u64) % 50;
                let position = n / (CHANNELS as u64 * 50) + 1;
                let message = Message::new(millis_now().to_string().into_bytes())
                    .with_header(TENANT_ID, "tlaunch")
                    .with_header(STREAM_TYPE, "orders.Order")
                    .with_header(STREAM_ID, format!("l{l}-c{c}-s{stream}"))
                    .with_header(STREAM_POSITION, position.to_string())
                    .with_header(MESSAGE_ID, format!("00000000-0000-4000-9{l:03x}-{n:012x}"));
                let stage = producing.of(&channel(c)).unwrap();
                let Ok(sealed) = stage.outbound(message).await else {
                    // the KMS went down: the launch's rest is not sealed
                    break;
                };
                intake
                    .producer(&format!("{}/{}", channel(c), "k"))
                    .unwrap()
                    .publish(sealed)
                    .await
                    .unwrap();
                stored.fetch_add(1, Ordering::Relaxed);
            }
        }
    });
    let launching = futures::future::join_all(launchers);
    // M6: at the outage's start and one, three and five minutes into it,
    // the share of the queue whose key a consumer holds — any of them, the
    // most the cache could serve; every one of them, the least — had its
    // channel not been paused; and of it the rows the order lets through:
    // a (slot, Tenant)'s queue up to its first row whose key no consumer
    // holds, which holds the rest behind it across channels.
    let cached = async {
        let client = pool(1).get().await.unwrap();
        for after in [0u32, 1, 3, 5] {
            let at = steady + minute * after;
            if let Some(rest) = at.checked_sub(started.elapsed()) {
                tokio::time::sleep(rest).await;
            }
            let rows = client
                .query(
                    &format!(
                        "SELECT metadata->>'dek', slot, tenant_id FROM {TABLE} \
                         WHERE processed_position IS NULL AND parked_at IS NULL \
                         ORDER BY slot, tenant_id, received_position"
                    ),
                    &[],
                )
                .await
                .unwrap();
            let now = Instant::now();
            let unwrapped = script.unwrapped.lock().unwrap();
            let kept = |when: &Option<Instant>| {
                when.is_some_and(|u| now.duration_since(u) <= parameters.cache)
            };
            let (mut any, mut every) = (0usize, 0usize);
            let (mut open_any, mut open_every) = (0usize, 0usize);
            let mut queue: Option<(i16, String)> = None;
            let (mut blocked_any, mut blocked_every) = (false, false);
            for row in &rows {
                let at: (i16, String) = (row.get(1), row.get(2));
                if queue.as_ref() != Some(&at) {
                    queue = Some(at);
                    (blocked_any, blocked_every) = (false, false);
                }
                let key: Option<String> = row.get(0);
                let by = key.and_then(|k| unwrapped.get(&k));
                let (a, e) = by.map_or((false, false), |by| {
                    (by.iter().any(kept), by.iter().all(kept))
                });
                any += usize::from(a);
                every += usize::from(e);
                blocked_any |= !a;
                blocked_every |= !e;
                open_any += usize::from(!blocked_any);
                open_every += usize::from(!blocked_every);
            }
            let share = |n: usize| n as f64 / rows.len().max(1) as f64;
            println!(
                "M6, {after} min into the outage: {} rows queued; their key held by a consumer \
                 for {any} ({:.3}), by every consumer for {every} ({:.3}); of them before the \
                 first row of their (slot, Tenant) whose key is held by none, {open_any} ({:.3}), \
                 by not every one, {open_every} ({:.3})",
                rows.len(),
                share(any),
                share(every),
                share(open_any),
                share(open_every)
            );
        }
    };
    // Every ten seconds, a line of counts.
    let client = pool(1).get().await.unwrap();
    let sampler = async {
        let mut tick = tokio::time::interval(minute / 6);
        let mut last_processed = 0;
        println!(
            "{:>7} {:>7} {:>6} {:>8} {:>7} {:>6} {:>6} {:>6} {:>7} {:>7} {:>7} {:>9} {:>6} {:>6} {:>6} {:>8}",
            "s",
            "stored",
            "wait",
            "done",
            "queue",
            "held",
            "parked",
            "spent",
            "draws",
            "unwraps",
            "down",
            "throttled",
            "paused",
            "resume",
            "trials",
            "p99 ms"
        );
        while started.elapsed() < end + minute * 5 {
            tick.tick().await;
            let row = client
                .query_one(
                    &format!(
                        "SELECT count(*) FILTER (WHERE processed_position IS NULL AND parked_at IS NULL), \
                                count(*) FILTER (WHERE parked_at IS NOT NULL), \
                                count(*) FILTER (WHERE processed_position IS NULL AND attempts > 0), \
                                count(*) FILTER (WHERE held_since IS NOT NULL) \
                         FROM {TABLE}"
                    ),
                    &[],
                )
                .await
                .unwrap();
            let (queued, parked, spent, held): (i64, i64, i64, i64) =
                (row.get(0), row.get(1), row.get(2), row.get(3));
            let processed = sum(&told, |t| &t.processed);
            let mut recent: Vec<u64> = {
                let mut all = latencies.lock().unwrap();
                let window: Vec<u64> = all.iter().map(|(_, l)| *l).collect();
                all.clear();
                window
            };
            recent.sort_unstable();
            let p99 = recent
                .get(((recent.len() as f64 - 1.0) * 0.99).round() as usize)
                .copied()
                .unwrap_or_default();
            println!(
                "{:>7.0} {:>7} {:>6} {:>8} {:>7} {:>6} {:>6} {:>6} {:>7} {:>7} {:>7} {:>9} {:>6} {:>6} {:>6} {:>8}",
                started.elapsed().as_secs_f64(),
                stored.load(Ordering::Relaxed),
                pending.load(Ordering::Relaxed),
                processed,
                queued,
                held,
                parked,
                spent,
                script.draws.load(Ordering::Relaxed),
                script.unwraps.load(Ordering::Relaxed),
                script.refused_down.load(Ordering::Relaxed),
                script.throttled.load(Ordering::Relaxed),
                sum(&told, |t| &t.paused),
                sum(&told, |t| &t.resumed),
                sum(&told, |t| &t.trials),
                p99
            );
            last_processed = processed;
            if started.elapsed() > end && queued == 0 && pending.load(Ordering::Relaxed) == 0 {
                break;
            }
        }
        last_processed
    };
    let ((), _, (), processed) = tokio::join!(producer, launching, cached, sampler);
    for subscription in subscriptions {
        subscription.cancel().await;
    }
    for t in &told {
        for line in t.lines.lock().unwrap().iter() {
            println!("{line}");
        }
    }
    println!(
        "M5: {} stored, {processed} processed; held {}, spent {}, parked {}; the KMS asked {} \
         unwraps while down, refused {} calls while down and {} for the quota; {} draws, {} \
         unwraps; {} pauses, {} resumes, {} trials, {} guest walks",
        stored.load(Ordering::Relaxed),
        sum(&told, |t| &t.held),
        sum(&told, |t| &t.spent),
        sum(&told, |t| &t.parked),
        script.unwraps_down.load(Ordering::Relaxed),
        script.refused_down.load(Ordering::Relaxed),
        script.throttled.load(Ordering::Relaxed),
        script.draws.load(Ordering::Relaxed),
        script.unwraps.load(Ordering::Relaxed),
        sum(&told, |t| &t.paused),
        sum(&told, |t| &t.resumed),
        sum(&told, |t| &t.trials),
        sum(&told, |t| &t.guests),
    );
    inboxes[0].teardown().await.unwrap();
}

/// M8 of the inbox's ADR-0042 (§ Owed, 1): one consuming process of three
/// cut off from the KMS alone for the whole run, and a launch Tenant's
/// messages alone on their channel, so that the process cut off never pauses
/// that channel — one Tenant fails it — and its memo holds the Tenant. What
/// the healthy processes' service of the launch costs: the seconds from its
/// first message to its channel's queue empty, the launch's latency, the
/// heads the process cut off held without asking, and the longest hold on
/// the launch's queue. Steady traffic on the other six channels. Run as M5
/// is, by hand, its table its own; `small` a tenth of the launch.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "a measurement (the inbox's ADR-0042 M8): run by hand"]
async fn a_replica_cut_off_alone_drilled() {
    const TABLE8: &str = "dek_drill_cut_inbox";
    let Ok(asked) = std::env::var("ASCETIC_DDD_MEASURE") else {
        eprintln!("a measurement: set ASCETIC_DDD_MEASURE to run it");
        return;
    };
    let small = asked == "small";
    let minute = if small {
        Duration::from_secs(6)
    } else {
        Duration::from_secs(60)
    };
    let launch: u64 = if small { 2_000 } else { 20_000 };
    let cap = minute * 20;
    let cut = PROCESSES - 1;
    // ASCETIC_DDD_M8_CUT=none: the same run with no process cut off, the
    // figures it is compared with
    let cut_off = std::env::var("ASCETIC_DDD_M8_CUT").map_or(true, |v| v != "none");
    // ASCETIC_DDD_M8_SHAPE=lone: the process cut off is refused the launch
    // Tenant's keys alone, so that it pauses no channel, and a crowd of 200
    // Tenants with 50 messages each, on channel 1, shares the launch's slots
    let lone = std::env::var("ASCETIC_DDD_M8_SHAPE").is_ok_and(|v| v == "lone");
    let started = Instant::now();
    // never down; one process cut off
    let script = Arc::new(Script {
        cut_tenant: lone.then(|| "tlaunch".to_owned()),
        // no quota: M8 measures a process cut off, not a KMS's rate
        quota: 1e9,
        ..Script::new(started, (cap * 10, cap * 10), cut_off.then_some(cut))
    });
    let channels: Vec<String> = (0..CHANNELS).map(channel).collect();
    let parameters = SealingParameters::default();
    let sealing = |process: usize| {
        Sealing::over(
            Arc::new(MemorySessionPool::new()),
            channels.clone(),
            parameters,
            |_: &str| Kms(Arc::clone(&script), process),
        )
    };
    let producing: Sealing = sealing(PROCESSES);
    let told: Vec<Arc<Told>> = (0..PROCESSES)
        .map(|process| {
            Arc::new(Told {
                process,
                started: Some(started),
                ..Told::default()
            })
        })
        .collect();
    let inboxes: Vec<Arc<PgInbox<PgSessionPool, Arc<Told>>>> = told
        .iter()
        .map(|told| {
            Arc::new(
                PgInbox::new(PgSessionPool::new(pool(LOOPS as usize + 4)))
                    .with_table(
                        Identifier::new(TABLE8).unwrap(),
                        Identifier::new(format!("{TABLE8}_seq")).unwrap(),
                    )
                    .with_slots(64)
                    .partitioned_by(ascetic_ddd_inbox::ByStream)
                    .with_loops(Loops {
                        concurrency: LOOPS,
                        poll_interval: POLL,
                        max_error_wait: Duration::from_secs(1),
                    })
                    .observed_by(Arc::clone(told)),
            )
        })
        .collect();
    inboxes[0].teardown().await.unwrap();
    inboxes[0].migrate().await.unwrap();
    // The launch's latencies, in milliseconds, as each was processed; the
    // crowd's beside them.
    let latencies: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let crowd_latencies: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let mut subscriptions = Vec::new();
    for (process, inbox) in inboxes.iter().enumerate() {
        let opening = sealing(process)
            .by_destination(channels.iter().map(|c| (c.clone(), c.clone())))
            .unwrap();
        let latencies = Arc::clone(&latencies);
        let crowd_latencies = Arc::clone(&crowd_latencies);
        let subscription = inbox
            .consumer(|message: &Message| {
                let tenant = message.header(TENANT_ID).unwrap_or_default();
                let kind = if tenant == b"tlaunch" {
                    1u8
                } else if tenant.starts_with(b"crowd") {
                    2
                } else {
                    0
                };
                Ok((
                    kind,
                    std::str::from_utf8(message.payload())?.parse::<u64>()?,
                ))
            })
            .through(Arc::new(opening) as Arc<dyn Stage>)
            .subscribe(move |_tx, (kind, sent): (u8, u64)| {
                let (latencies, crowd_latencies) =
                    (Arc::clone(&latencies), Arc::clone(&crowd_latencies));
                async move {
                    tokio::time::sleep(HANDLER).await;
                    let waited = millis_now().saturating_sub(sent);
                    match kind {
                        1 => latencies.lock().unwrap().push(waited),
                        2 => crowd_latencies.lock().unwrap().push(waited),
                        _ => {}
                    }
                }
            })
            .unwrap();
        subscriptions.push(subscription);
    }
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // Steady traffic on the six other channels, until the drill ends.
    let producer = {
        let (producing, done) = (producing.clone(), Arc::clone(&done));
        let intake = inboxes[0].channel();
        async move {
            let mut positions: HashMap<(usize, usize, usize), u64> = HashMap::new();
            let mut made = 0u64;
            let mut seed = 0x9E37_79B9_7F4A_7C15_u64;
            let mut next = move |n: usize| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                (seed % n as u64) as usize
            };
            let mut tick = tokio::time::interval(Duration::from_millis(1000 / RATE));
            while !done.load(Ordering::Relaxed) {
                tick.tick().await;
                let (t, c, s) = (next(TENANTS), 1 + next(CHANNELS - 1), next(STREAMS));
                let position = positions.entry((t, c, s)).or_default();
                *position += 1;
                made += 1;
                let message = Message::new(millis_now().to_string().into_bytes())
                    .with_header(TENANT_ID, format!("t{t:03}"))
                    .with_header(STREAM_TYPE, "orders.Order")
                    .with_header(STREAM_ID, format!("c{c}-s{s}"))
                    .with_header(STREAM_POSITION, position.to_string())
                    .with_header(MESSAGE_ID, format!("00000000-0000-4000-8000-{made:012x}"));
                let sealed = producing
                    .of(&channel(c))
                    .unwrap()
                    .outbound(message)
                    .await
                    .unwrap();
                intake
                    .producer(&format!("{}/{}", channel(c), "k"))
                    .unwrap()
                    .publish(sealed)
                    .await
                    .unwrap();
            }
        }
    };
    // The launch: a minute in, four producers, its messages within a minute,
    // all on channel 0.
    let launched = Arc::new(AtomicU64::new(0));
    let launch_began: Arc<OnceLock<Instant>> = Arc::new(OnceLock::new());
    let launchers = (0..LAUNCHERS).map(|l| {
        let (launched, producing) = (Arc::clone(&launched), producing.clone());
        let launch_began = Arc::clone(&launch_began);
        let intake = inboxes[0].channel();
        async move {
            tokio::time::sleep(minute).await;
            let begun = *launch_began.get_or_init(Instant::now);
            let each = launch / LAUNCHERS;
            for n in 0..each {
                let due = minute.mul_f64(n as f64 / each as f64);
                if let Some(rest) = due.checked_sub(begun.elapsed()) {
                    tokio::time::sleep(rest).await;
                }
                let stream = n % 50;
                let position = n / 50 + 1;
                let message = Message::new(millis_now().to_string().into_bytes())
                    .with_header(TENANT_ID, "tlaunch")
                    .with_header(STREAM_TYPE, "orders.Order")
                    .with_header(STREAM_ID, format!("l{l}-s{stream}"))
                    .with_header(STREAM_POSITION, position.to_string())
                    .with_header(MESSAGE_ID, format!("00000000-0000-4000-9{l:03x}-{n:012x}"));
                let sealed = producing
                    .of(&channel(0))
                    .unwrap()
                    .outbound(message)
                    .await
                    .unwrap();
                intake
                    .producer(&format!("{}/{}", channel(0), "k"))
                    .unwrap()
                    .publish(sealed)
                    .await
                    .unwrap();
                launched.fetch_add(1, Ordering::Relaxed);
            }
        }
    });
    let launching = futures::future::join_all(launchers);
    let crowd = {
        let producing = producing.clone();
        let intake = inboxes[0].channel();
        async move {
            if !lone {
                return;
            }
            tokio::time::sleep(minute).await;
            for round in 0..50u64 {
                for tenant in 0..200u64 {
                    let message = Message::new(millis_now().to_string().into_bytes())
                        .with_header(TENANT_ID, format!("crowd{tenant:03}"))
                        .with_header(STREAM_TYPE, "orders.Order")
                        .with_header(STREAM_ID, format!("crowd{tenant:03}"))
                        .with_header(STREAM_POSITION, (round + 1).to_string())
                        .with_header(
                            MESSAGE_ID,
                            format!("00000000-0000-4000-a000-{:012x}", round * 1000 + tenant),
                        );
                    // on channel 1: its keys land on another stage than the
                    // launch's, so that the launch's refusals stay the
                    // stage's, its stage seeing no other key land
                    let sealed = producing
                        .of(&channel(1))
                        .unwrap()
                        .outbound(message)
                        .await
                        .unwrap();
                    intake
                        .producer(&format!("{}/{}", channel(1), "k"))
                        .unwrap()
                        .publish(sealed)
                        .await
                        .unwrap();
                }
                tokio::time::sleep(minute / 50).await;
            }
        }
    };
    // Every ten seconds, the launch's channel: its queue, its held rows, the
    // longest hold on it; each process's rows processed; the heads the
    // process cut off held unasked; the launch's p99 in the window.
    let client = pool(1).get().await.unwrap();
    let sampler = async {
        let mut tick = tokio::time::interval(minute / 6);
        let mut drained = None;
        let mut seen = 0usize;
        println!(
            "{:>6} {:>8} {:>7} {:>6} {:>7} {:>8} {:>8} {:>8} {:>9} {:>7} {:>8}",
            "s",
            "launched",
            "queue",
            "held",
            "hold s",
            "done p0",
            "done p1",
            "done p2",
            "withheld",
            "told",
            "p99 ms"
        );
        while started.elapsed() < cap {
            tick.tick().await;
            let row = client
                .query_one(
                    &format!(
                        "SELECT count(*) FILTER (WHERE processed_position IS NULL AND parked_at IS NULL), \
                                count(*) FILTER (WHERE held_since IS NOT NULL), \
                                coalesce(extract(epoch FROM max(next_attempt_at) \
                                    FILTER (WHERE processed_position IS NULL) - now()), 0)::float8 \
                         FROM {TABLE8} WHERE split_part(uri, '/', 3) = 'c0'"
                    ),
                    &[],
                )
                .await
                .unwrap();
            let (queued, held, hold): (i64, i64, f64) = (row.get(0), row.get(1), row.get(2));
            let window: Vec<u64> = {
                let all = latencies.lock().unwrap();
                let window = all[seen..].to_vec();
                seen = all.len();
                window
            };
            let mut window = window;
            window.sort_unstable();
            let p99 = window
                .get(((window.len() as f64 - 1.0) * 0.99).round() as usize)
                .copied()
                .unwrap_or_default();
            println!(
                "{:>6.0} {:>8} {:>7} {:>6} {:>7.1} {:>8} {:>8} {:>8} {:>9} {:>7} {:>8}",
                started.elapsed().as_secs_f64(),
                launched.load(Ordering::Relaxed),
                queued,
                held,
                hold.max(0.0),
                told[0].processed.load(Ordering::Relaxed),
                told[1].processed.load(Ordering::Relaxed),
                told[cut].processed.load(Ordering::Relaxed),
                told[cut].withheld.load(Ordering::Relaxed),
                told[cut].held.load(Ordering::Relaxed),
                p99
            );
            if launched.load(Ordering::Relaxed) == launch && queued == 0 {
                drained = launch_began.get().map(|b| b.elapsed());
                break;
            }
        }
        done.store(true, Ordering::Relaxed);
        drained
    };
    let ((), _, (), drained) = tokio::join!(producer, launching, crowd, sampler);
    for subscription in subscriptions {
        subscription.cancel().await;
    }
    let mut all = latencies.lock().unwrap().clone();
    all.sort_unstable();
    let at = |q: f64| {
        all.get(((all.len() as f64 - 1.0) * q).round() as usize)
            .copied()
            .unwrap_or_default()
    };
    let mut crowded = crowd_latencies.lock().unwrap().clone();
    crowded.sort_unstable();
    if lone {
        let at = |q: f64| {
            crowded
                .get(((crowded.len() as f64 - 1.0) * q).round() as usize)
                .copied()
                .unwrap_or_default()
        };
        println!(
            "M8, the crowd: {} processed; latency p50 {} ms, p99 {} ms, max {} ms",
            crowded.len(),
            at(0.5),
            at(0.99),
            crowded.last().copied().unwrap_or_default()
        );
    }
    println!(
        "M8: the launch of {launch} drained in {}; its latency p50 {} ms, p99 {} ms, max {} ms over {} \
         processed; the process cut off held {} heads unasked and was told {} holds, its walks a \
         guest's {} times, {} pauses; processed by process 0, 1, 2: {}, {}, {}; the KMS refused {} \
         unwraps of the process cut off",
        drained.map_or("— (not within the cap)".to_owned(), |d| format!(
            "{:.1} s",
            d.as_secs_f64()
        )),
        at(0.5),
        at(0.99),
        all.last().copied().unwrap_or_default(),
        all.len(),
        told[cut].withheld.load(Ordering::Relaxed),
        told[cut].held.load(Ordering::Relaxed),
        told[cut].guests.load(Ordering::Relaxed),
        told[cut].paused.load(Ordering::Relaxed),
        told[0].processed.load(Ordering::Relaxed),
        told[1].processed.load(Ordering::Relaxed),
        told[cut].processed.load(Ordering::Relaxed),
        script.refused_down.load(Ordering::Relaxed),
    );
    inboxes[0].teardown().await.unwrap();
}

/// M10 of the inbox's ADR-0042 (decision 1, § Measured): the KMS hanging
/// rather than refusing — every call while it is down waits for its client's
/// timeout, 30 s, and is then refused — for ten minutes, with a launch's
/// backlog queued before it and steady traffic; or refusing at once, a share
/// of Tenants only, at a larger scale, or slow and answering (the knobs
/// below). The opening's dependencies registered with each process's inbox,
/// as a host composes them, or not, for the reference. What the processes
/// report: when each first stalls, its stall row the page reads; how often
/// each pauses and resumes, and by what; its trials and checks. The pass
/// lines are asserted at the end. Run as M5 is, by hand, its table its own;
/// `small` a tenth.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "a measurement (the inbox's ADR-0042 M10): run by hand"]
async fn a_kms_hanging_drilled() {
    const TABLE10: &str = "dek_drill_hang_inbox";
    let Ok(asked) = std::env::var("ASCETIC_DDD_MEASURE") else {
        eprintln!("a measurement: set ASCETIC_DDD_MEASURE to run it");
        return;
    };
    let small = asked == "small";
    let minute = if small {
        Duration::from_secs(6)
    } else {
        Duration::from_secs(60)
    };
    let launch: u64 = if small { 2_000 } else { 20_000 };
    let (steady, outage, recovery) = (minute * 2, minute * 10, minute * 3);
    let end = steady + outage + recovery;
    // ASCETIC_DDD_M10_TRIAL_WAIT=<s>: the trial's wait, the default otherwise
    let breaking = std::env::var("ASCETIC_DDD_M10_TRIAL_WAIT")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map_or(ascetic_ddd_inbox::Breaking::default(), |s| {
            ascetic_ddd_inbox::Breaking::default().with_trial_wait(Duration::from_secs(s))
        });
    println!("M10: the trial's wait {:?}", breaking.trial_wait);
    let started = Instant::now();
    // ASCETIC_DDD_M10_HANG=0: the same run, the KMS refusing at once while down
    let hang = std::env::var("ASCETIC_DDD_M10_HANG").map_or(true, |v| v != "0");
    println!(
        "M10: the KMS {} while down",
        if std::env::var("ASCETIC_DDD_M10_SLOW").is_ok() {
            "answers slowly"
        } else if hang {
            "hangs"
        } else {
            "refuses at once"
        }
    );
    // ASCETIC_DDD_M10_PARTIAL=<percent>: only that share of Tenants' unwraps
    // refused (or hung) while down, every other call answered
    let partial = std::env::var("ASCETIC_DDD_M10_PARTIAL")
        .ok()
        .and_then(|v| v.parse::<u64>().ok());
    // ASCETIC_DDD_M10_SLOW=<ms>: while down, every call answered after that
    // long rather than refused or hung (a brownout)
    let slow = std::env::var("ASCETIC_DDD_M10_SLOW")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis);
    // ASCETIC_DDD_M10_TENANTS / _RATE: the steady traffic's Tenants and
    // messages a second, M10's 500 and 20 otherwise
    let tenants: usize = std::env::var("ASCETIC_DDD_M10_TENANTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(TENANTS);
    let rate: u64 = std::env::var("ASCETIC_DDD_M10_RATE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(RATE);
    println!(
        "M10: {tenants} Tenants at {rate} a second; {}",
        match (slow, partial) {
            (Some(slow), _) => format!("every call answered after {slow:?} while down"),
            (None, Some(p)) => format!("{p} % of Tenants' unwraps refused while down"),
            (None, None) => "every call refused while down".to_owned(),
        }
    );
    // the full outage: every call refused or hung
    let full = slow.is_none() && partial.is_none();
    let script = Arc::new(Script {
        hang,
        quota: 1e9,
        partial,
        slow,
        ..Script::new(started, (steady, steady + outage), None)
    });
    // The age at handling of the messages of Tenants the outage spares, made
    // while down: how long a pause held what the stage could have opened.
    let spared: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let window = (
        millis_now() + steady.as_millis() as u64,
        millis_now() + (steady + outage).as_millis() as u64,
    );
    let channels: Vec<String> = (0..CHANNELS).map(channel).collect();
    let parameters = SealingParameters::default();
    let sealing = |process: usize| {
        Sealing::over(
            Arc::new(MemorySessionPool::new()),
            channels.clone(),
            parameters,
            |_: &str| Kms(Arc::clone(&script), process),
        )
    };
    let producing: Sealing = sealing(PROCESSES);
    let told: Vec<Arc<Told>> = (0..PROCESSES)
        .map(|process| {
            Arc::new(Told {
                process,
                started: Some(started),
                down: (steady.as_secs_f64(), (steady + outage).as_secs_f64()),
                ..Told::default()
            })
        })
        .collect();
    // the opening ends by destination, their dependencies handed to each
    // process's inbox, as a host composes them (ADR-0042 decision 1);
    // ASCETIC_DDD_M10_UNREGISTERED=1: not handed, the stage's refusals the
    // rows' breaker's (the reference a registered run is read against)
    let registered = std::env::var("ASCETIC_DDD_M10_UNREGISTERED").map_or(true, |v| v != "1");
    if !registered {
        println!("M10: the dependencies not registered");
    }
    let openings: Vec<_> = (0..PROCESSES)
        .map(|process| {
            sealing(process)
                .by_destination(channels.iter().map(|c| (c.clone(), c.clone())))
                .unwrap()
        })
        .collect();
    let inboxes: Vec<Arc<PgInbox<PgSessionPool, Arc<Told>>>> = told
        .iter()
        .zip(&openings)
        .map(|(told, opening)| {
            let inbox = PgInbox::new(PgSessionPool::new(pool(LOOPS as usize + 4)))
                .with_table(
                    Identifier::new(TABLE10).unwrap(),
                    Identifier::new(format!("{TABLE10}_seq")).unwrap(),
                )
                .with_slots(64)
                .partitioned_by(ascetic_ddd_inbox::ByStream)
                .with_loops(Loops {
                    concurrency: LOOPS,
                    poll_interval: POLL,
                    max_error_wait: Duration::from_secs(1),
                })
                .with_breaking(breaking);
            let inbox = if registered {
                inbox.depending_on(opening)
            } else {
                inbox
            };
            Arc::new(inbox.observed_by(Arc::clone(told)))
        })
        .collect();
    inboxes[0].teardown().await.unwrap();
    inboxes[0].migrate().await.unwrap();
    let mut subscriptions = Vec::new();
    for (opening, inbox) in openings.into_iter().zip(&inboxes) {
        let spared = Arc::clone(&spared);
        let subscription = inbox
            .consumer(move |message: &Message| {
                let made = std::str::from_utf8(message.payload())?.parse::<u64>()?;
                let tenant = std::str::from_utf8(message.header(TENANT_ID).unwrap_or_default())?;
                Ok((made, partial.is_none_or(|p| !broken(tenant, p))))
            })
            .through(Arc::new(opening) as Arc<dyn Stage>)
            .subscribe(move |_tx, (made, spared_tenant): (u64, bool)| {
                let spared = Arc::clone(&spared);
                async move {
                    tokio::time::sleep(HANDLER).await;
                    if spared_tenant && made >= window.0 && made < window.1 {
                        spared
                            .lock()
                            .unwrap()
                            .push(millis_now().saturating_sub(made));
                    }
                }
            })
            .unwrap();
        subscriptions.push(subscription);
    }
    // Steady traffic, sealed while the KMS answers: a seal that hangs waits.
    let producer = {
        let producing = producing.clone();
        let intake = inboxes[0].channel();
        async move {
            let mut positions: HashMap<(usize, usize, usize), u64> = HashMap::new();
            let mut made = 0u64;
            let mut seed = 0x9E37_79B9_7F4A_7C15_u64;
            let mut next = move |n: usize| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                (seed % n as u64) as usize
            };
            let mut tick = tokio::time::interval(Duration::from_micros(1_000_000 / rate));
            while started.elapsed() < end {
                tick.tick().await;
                let (t, c, s) = (next(tenants), next(CHANNELS), next(STREAMS));
                let position = positions.entry((t, c, s)).or_default();
                *position += 1;
                made += 1;
                let message = Message::new(millis_now().to_string().into_bytes())
                    .with_header(TENANT_ID, format!("t{t:03}"))
                    .with_header(STREAM_TYPE, "orders.Order")
                    .with_header(STREAM_ID, format!("c{c}-s{s}"))
                    .with_header(STREAM_POSITION, position.to_string())
                    .with_header(MESSAGE_ID, format!("00000000-0000-4000-8000-{made:012x}"));
                let Ok(sealed) = producing.of(&channel(c)).unwrap().outbound(message).await else {
                    // the KMS down: this message is not made
                    *position -= 1;
                    continue;
                };
                intake
                    .producer(&format!("{}/{}", channel(c), "k"))
                    .unwrap()
                    .publish(sealed)
                    .await
                    .unwrap();
            }
        }
    };
    // The launch, in the minute before the outage, so that it meets a backlog.
    let launchers = (0..LAUNCHERS).map(|l| {
        let producing = producing.clone();
        let intake = inboxes[0].channel();
        async move {
            tokio::time::sleep(steady.saturating_sub(minute)).await;
            let each = launch / LAUNCHERS;
            let begun = Instant::now();
            for n in 0..each {
                let due = minute.mul_f64(n as f64 / each as f64);
                if let Some(rest) = due.checked_sub(begun.elapsed()) {
                    tokio::time::sleep(rest).await;
                }
                let c = (n % CHANNELS as u64) as usize;
                let stream = (n / CHANNELS as u64) % 50;
                let position = n / (CHANNELS as u64 * 50) + 1;
                let message = Message::new(millis_now().to_string().into_bytes())
                    .with_header(TENANT_ID, format!("tl{}", n % 40))
                    .with_header(STREAM_TYPE, "orders.Order")
                    .with_header(STREAM_ID, format!("l{l}-c{c}-s{stream}"))
                    .with_header(STREAM_POSITION, position.to_string())
                    .with_header(MESSAGE_ID, format!("00000000-0000-4000-9{l:03x}-{n:012x}"));
                let Ok(sealed) = producing.of(&channel(c)).unwrap().outbound(message).await else {
                    break;
                };
                intake
                    .producer(&format!("{}/{}", channel(c), "k"))
                    .unwrap()
                    .publish(sealed)
                    .await
                    .unwrap();
            }
        }
    });
    let launching = futures::future::join_all(launchers);
    let client = pool(1).get().await.unwrap();
    // samples in a full outage, from the first stall to the return, at which
    // no stall row was valid: the page would have lapsed
    let lapsed = AtomicU64::new(0);
    let sampler = async {
        let mut tick = tokio::time::interval(minute / 6);
        println!(
            "{:>6} {:>7} {:>8} {:>6} {:>6} {:>6} {:>6} {:>8}",
            "s", "queue", "done", "paused", "resume", "trials", "stalls", "refused"
        );
        while started.elapsed() < end {
            tick.tick().await;
            let queued: i64 = client
                .query_one(
                    &format!(
                        "SELECT count(*) FROM {TABLE10} \
                         WHERE processed_position IS NULL AND parked_at IS NULL"
                    ),
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            let at = started.elapsed().as_secs_f64();
            let stalled = told
                .iter()
                .filter_map(|t| *t.first_stall.lock().unwrap())
                .fold(f64::INFINITY, f64::min);
            if full && at > stalled && at < (steady + outage).as_secs_f64() {
                let valid: i64 = client
                    .query_one(
                        &format!("SELECT count(*) FROM {TABLE10}_stalls WHERE valid_until > now()"),
                        &[],
                    )
                    .await
                    .unwrap()
                    .get(0);
                if valid == 0 {
                    lapsed.fetch_add(1, Ordering::Relaxed);
                    println!("{at:6.0} no stall row valid");
                }
            }
            println!(
                "{:>6.0} {:>7} {:>8} {:>6} {:>6} {:>6} {:>6} {:>8}",
                started.elapsed().as_secs_f64(),
                queued,
                sum(&told, |t| &t.processed),
                sum(&told, |t| &t.paused),
                sum(&told, |t| &t.resumed),
                sum(&told, |t| &t.trials),
                sum(&told, |t| &t.stalled),
                script.refused_down.load(Ordering::Relaxed)
            );
        }
    };
    tokio::join!(producer, launching, sampler);
    for subscription in subscriptions {
        subscription.cancel().await;
    }
    for t in &told {
        let first = *t.first_stall.lock().unwrap();
        println!(
            "M10, process {}: first stalled {} into the outage; {} stall effects, {} pauses, {} \
             resumes ({} by a trial processed, {} by three inconclusive, {} with no witness, {} by \
             a check served, {} with nothing to hold; {} while down, the last {}), {} trials, {} \
             checks, {} refuted, {} parked, {} spent",
            t.process,
            first.map_or("never".to_owned(), |at| format!(
                "{:.1} s",
                at - steady.as_secs_f64()
            )),
            t.stalled.load(Ordering::Relaxed),
            t.paused.load(Ordering::Relaxed),
            t.resumed.load(Ordering::Relaxed),
            t.by_processed.load(Ordering::Relaxed),
            t.by_inconclusive.load(Ordering::Relaxed),
            t.by_no_witness.load(Ordering::Relaxed),
            t.by_served.load(Ordering::Relaxed),
            t.by_nothing.load(Ordering::Relaxed),
            t.resumed_down.load(Ordering::Relaxed),
            t.last_resume
                .lock()
                .unwrap()
                .map_or("none".to_owned(), |at| format!(
                    "{:.1} s after the return",
                    at - (steady + outage).as_secs_f64()
                )),
            t.trials.load(Ordering::Relaxed),
            t.checks.load(Ordering::Relaxed),
            t.refuted.load(Ordering::Relaxed),
            t.parked.load(Ordering::Relaxed),
            t.spent.load(Ordering::Relaxed),
        );
        for line in t.lines.lock().unwrap().iter() {
            println!("{line}");
        }
    }
    println!(
        "M10: the KMS hung {} calls while down, {} unwraps begun while down",
        script.refused_down.load(Ordering::Relaxed),
        script.unwraps_down.load(Ordering::Relaxed)
    );
    let mut ages = std::mem::take(&mut *spared.lock().unwrap());
    ages.sort_unstable();
    let at = |q: f64| {
        ages.get(((ages.len() as f64 - 1.0) * q) as usize)
            .copied()
            .unwrap_or(0)
    };
    println!(
        "M10: spared Tenants' messages made while down: {} handled, age p50 {} ms, p99 {} ms, max {} ms",
        ages.len(),
        at(0.5),
        at(0.99),
        ages.last().copied().unwrap_or(0)
    );
    // The pass lines (ADR-0042 decision 1, M10): no attempt spent and nothing
    // parked, read from the table; and in a full outage a stall row valid at
    // every sample from the first stall to the return, the first stall within
    // a minute, no channel resumed while the KMS was down and every one within
    // half a minute of its return.
    let spent: i64 = client
        .query_one(
            &format!("SELECT count(*) FROM {TABLE10} WHERE attempts > 0 OR parked_at IS NOT NULL"),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(spent, 0, "rows spent or parked");
    // and no stall row still valid at the end, three minutes after the return
    let valid: i64 = client
        .query_one(
            &format!("SELECT count(*) FROM {TABLE10}_stalls WHERE valid_until > now()"),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(valid, 0, "a stall row valid after the return");
    if !registered {
        inboxes[0].teardown().await.unwrap();
        return;
    }
    // Under a brownout nothing pauses (ADR-0042 decision 1: a check opens
    // the control within the stage's own draw).
    if slow.is_some() {
        assert_eq!(sum(&told, |t| &t.paused), 0, "paused under a brownout");
    }
    // Under a partial outage the spared Tenants wait no longer than twice
    // the control run's p99 on the reference machine, 4.2 s, if it refuses;
    // if it hangs, no longer than a quarter above the round's baseline,
    // 297 s (§ Owed: a partial hang eats the loops, the DEK's).
    if let (false, None, Some(_)) = (small, slow, partial) {
        let bound = if hang { 371_000 } else { 8_500 };
        assert!(at(0.99) <= bound, "the spared waited p99 {} ms", at(0.99));
    }
    if full {
        assert_eq!(
            lapsed.load(Ordering::Relaxed),
            0,
            "the page lapsed while down"
        );
        for t in &told {
            // the first stall: W, the suspicion's step, a check's bound, a
            // tick and a check's bound from the last landing (≈ 43 s), and a
            // margin
            let first = t.first_stall.lock().unwrap().unwrap_or(f64::INFINITY);
            assert!(
                first - steady.as_secs_f64() <= 60.0,
                "process {} first stalled {first:.1} s into the run",
                t.process
            );
            assert_eq!(
                t.resumed_down.load(Ordering::Relaxed),
                0,
                "process {} resumed with the KMS down",
                t.process
            );
            let last = t.last_resume.lock().unwrap().unwrap_or(f64::INFINITY);
            assert!(
                t.paused.load(Ordering::Relaxed) == 0
                    || last - (steady + outage).as_secs_f64() <= 30.0,
                "process {} resumed {last:.1} s into the run",
                t.process
            );
        }
    }
    inboxes[0].teardown().await.unwrap();
}
