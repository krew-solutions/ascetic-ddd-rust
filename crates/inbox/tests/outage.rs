//! A message that cannot be handled now stops exactly the messages that
//! share its cause (ADR-0040, ADR-0042): an `Unavailable` holds its row and
//! spends no attempt, whatever its scope; a Tenant's is remembered, its other
//! heads held without asking; a stage's, broad across Tenants, pauses the
//! channel in the process, a trial a tick asking a witness; and a cause lives
//! as long as the state it explains. They need a live database:
//!
//! ```text
//! ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
//!     cargo test -p ascetic-ddd-inbox --test outage -- --ignored
//! ```

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use ascetic_ddd_bus::{Cause, Permanent, Scope, Transient, Unavailable, cause};
use ascetic_ddd_inbox::breaker::Effect;
use ascetic_ddd_inbox::observer::{
    BreakerChanged, Failed, InboxObserver, Operated, Operation, Withheld,
};
use ascetic_ddd_inbox::{
    Breaking, ByStream, CausalDependency, Error, Failure, GivenUp, Hold, Inbox, InboxMessage,
    InboxRetention, Outcome, ParkedFor, ParkedUnder, ParkingHeads, ParkingQueue, PgInbox, Retries,
    Rotation, UnparkedAll, Unparking, WaitingCount,
};
use ascetic_ddd_session::pg::Identifier;
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};
use ascetic_ddd_session::{PgAccess, PgSession, PgSessionPool, Session, SessionPool};
use serde_json::json;

const DEFAULT_URL: &str = "postgresql://devel:devel@localhost:5432/devel_karmabot_test";

const KMS_UNAVAILABLE: Cause = cause!("kms.unavailable");
const KMS_THROTTLED: Cause = cause!("kms.throttled");
const KMS_GONE: Cause = cause!("kms.gone");
const DB_TIMEOUT: Cause = cause!("db.timeout");

fn url() -> String {
    std::env::var("ASCETIC_DDD_TEST_PG_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned())
}

fn pool_of(config: Config) -> Pool {
    let manager = Manager::from_config(
        config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("the pool can be built")
}

fn pool() -> Pool {
    pool_of(Config::from_str(&url()).expect("a valid PostgreSQL URL"))
}

/// Breaking fast enough for a test that waits on the wall's clock: a
/// Tenant's back-off of 40 ms to 200 ms, a tick of 50 ms to 200 ms.
fn quick() -> Breaking {
    Breaking::default()
        .with_hold(Duration::from_millis(40), Duration::from_millis(200))
        .with_tick(Duration::from_millis(50), Duration::from_millis(200))
}

/// What the observer was told of each failure.
#[derive(Clone, Debug, PartialEq)]
struct Told {
    cause: String,
    attempts: u32,
    parked: bool,
    held: bool,
    retry_after: Duration,
}

/// What a verb's audit named: its operation, its Tenant and its cause.
type Named = (Operation, Option<String>, Option<String>);

#[derive(Default)]
struct Failures(
    Mutex<Vec<Told>>,
    Mutex<Vec<(String, Operation, u64)>>,
    Mutex<Vec<Effect>>,
    AtomicU32,
    Mutex<Vec<Named>>,
);

impl InboxObserver for Failures {
    fn on_operated(&self, event: &Operated<'_>) {
        self.1
            .lock()
            .unwrap()
            .push((event.principal.to_owned(), event.operation, event.rows));
        self.4.lock().unwrap().push((
            event.operation,
            event.tenant.map(str::to_owned),
            event.cause.map(str::to_owned),
        ));
    }

    fn on_failed(&self, event: &Failed<'_>) {
        self.0.lock().unwrap().push(Told {
            cause: event.cause.as_str().to_owned(),
            attempts: event.attempts,
            parked: event.parked,
            held: event.held,
            retry_after: event.retry_after,
        });
    }

    fn on_breaker(&self, event: &BreakerChanged<'_>) {
        self.2.lock().unwrap().push(event.effect.clone());
    }

    fn on_withheld(&self, _: &Withheld<'_>) {
        self.3.fetch_add(1, Ordering::SeqCst);
    }
}

/// The lines the inbox logs at `info` or above, by the thread that wrote
/// them: a test on the current-thread runtime reads its own.
struct Lines(Mutex<Vec<(std::thread::ThreadId, String)>>);

impl log::Log for Lines {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.target().starts_with("ascetic_ddd_inbox") && metadata.level() <= log::Level::Info
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            self.0
                .lock()
                .unwrap()
                .push((std::thread::current().id(), record.args().to_string()));
        }
    }

    fn flush(&self) {}
}

/// The lines this thread's inbox logged so far.
fn my_lines() -> Vec<String> {
    static LINES: OnceLock<&'static Lines> = OnceLock::new();
    let lines = LINES.get_or_init(|| {
        let lines: &'static Lines = Box::leak(Box::new(Lines(Mutex::new(Vec::new()))));
        log::set_logger(lines).expect("the one logger of this binary");
        log::set_max_level(log::LevelFilter::Debug);
        lines
    });
    let me = std::thread::current().id();
    lines
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|(thread, _)| *thread == me)
        .map(|(_, line)| line.clone())
        .collect()
}

struct Fixture {
    inbox: PgInbox<PgSessionPool, Arc<Failures>>,
    told: Arc<Failures>,
    sessions: PgSessionPool,
    table: String,
}

/// A table of its own, one slot, `retries` as given.
async fn fixture(name: &str, retries: Retries) -> Fixture {
    fixture_cut(name, retries, 1, quick()).await
}

/// The same, a row waiting for its dependency at most `max_wait`.
async fn fixture_waiting(name: &str, retries: Retries, max_wait: Duration) -> Fixture {
    let f = fixture(name, retries).await;
    Fixture {
        inbox: f.inbox.with_max_wait(max_wait),
        ..f
    }
}

/// A table of its own, `slots` slots, `retries` and `breaking` as given.
async fn fixture_cut(name: &str, retries: Retries, slots: u32, breaking: Breaking) -> Fixture {
    let table = format!("outage_{name}");
    let told = Arc::new(Failures::default());
    let inbox = PgInbox::new(PgSessionPool::new(pool()))
        .with_table(
            Identifier::new(&table).unwrap(),
            Identifier::new(format!("{table}_seq")).unwrap(),
        )
        .with_slots(slots)
        .partitioned_by(ByStream)
        .with_retries(retries)
        .with_max_wait(Duration::from_secs(600))
        .with_breaking(breaking)
        .observed_by(Arc::clone(&told));
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    Fixture {
        inbox,
        told,
        sessions: PgSessionPool::new(pool()),
        table,
    }
}

impl Fixture {
    /// Another process over the same table: a breaker of its own.
    fn beside(&self, breaking: Breaking) -> PgInbox<PgSessionPool, ()> {
        PgInbox::new(PgSessionPool::new(pool()))
            .with_table(
                Identifier::new(&self.table).unwrap(),
                Identifier::new(format!("{}_seq", self.table)).unwrap(),
            )
            .with_slots(self.slots())
            .partitioned_by(ByStream)
            .with_breaking(breaking)
    }

    fn slots(&self) -> u32 {
        // the fixtures of this file cut one slot unless they say otherwise
        1
    }

    async fn publish(&self, messages: &[InboxMessage]) {
        for message in messages {
            self.inbox.publish(message).await.unwrap();
        }
    }

    async fn dispatch(&self, verdict: impl Fn() -> Result<(), Failure> + Send + Sync) -> Outcome {
        self.dispatch_on(|_| verdict()).await
    }

    /// The same, the verdict by the message: the walk serves a slot's
    /// Tenants in turn (ADR-0041 decision 2), so a test of two Tenants says
    /// what each message gets, not what the nth dispatch gets.
    async fn dispatch_on(
        &self,
        verdict: impl Fn(&InboxMessage) -> Result<(), Failure> + Send + Sync,
    ) -> Outcome {
        self.inbox
            .dispatch(|_: &PgSession, message: &InboxMessage| std::future::ready(verdict(message)))
            .await
            .unwrap()
    }

    /// Runs `sql`, `{table}` standing for the inbox's table.
    async fn execute(&self, sql: &str) {
        let sql = sql.replace("{table}", &self.table);
        self.sessions
            .session(async |session| {
                Ok::<_, Error>(session.connection().batch_execute(&sql).await?)
            })
            .await
            .unwrap();
    }

    /// The one row of stream `stream`: its attempts, its cause, its holds,
    /// parked or processed, and how far its next attempt is.
    async fn row(&self, stream: &str) -> Row {
        let sql = format!(
            "SELECT attempts, cause, holds, hold_kind, held_since IS NOT NULL, \
                    parked_at IS NOT NULL, processed_position IS NOT NULL, \
                    EXTRACT(EPOCH FROM next_attempt_at - clock_timestamp())::float8 \
             FROM {} WHERE stream_id->>'id' = $1",
            self.table
        );
        self.sessions
            .session(async |session| {
                let row = session.connection().query_one(&sql, &[&stream]).await?;
                Ok::<_, Error>(Row {
                    attempts: row.get(0),
                    cause: row.get(1),
                    holds: row.get(2),
                    kind: row.get(3),
                    held: row.get(4),
                    parked: row.get(5),
                    processed: row.get(6),
                    next_in: row.get(7),
                })
            })
            .await
            .unwrap()
    }

    /// Makes every row due now, and wakes the slot: a round that found no
    /// head due sleeps until the earliest back-off it saw (ADR-0041 decision
    /// 2), and the rows' clock moved by hand moves the slot's too.
    async fn due(&self) {
        self.execute(
            "UPDATE {table} SET next_attempt_at = now() WHERE next_attempt_at IS NOT NULL",
        )
        .await;
        self.wake().await;
    }

    /// Wakes the slot from the sleep a round that found no head due began:
    /// it sleeps at most `max_dormancy`, a second, and the tests do not wait
    /// it out (ADR-0041 decision 2).
    async fn wake(&self) {
        self.execute("UPDATE {table}_slots SET dormant_until = now()")
            .await;
    }

    fn told(&self) -> Vec<Told> {
        self.told.0.lock().unwrap().clone()
    }

    fn operated(&self) -> Vec<(String, Operation, u64)> {
        self.told.1.lock().unwrap().clone()
    }

    fn effects(&self) -> Vec<Effect> {
        self.told.2.lock().unwrap().clone()
    }

    fn withheld(&self) -> u32 {
        self.told.3.load(Ordering::SeqCst)
    }

    fn trials(&self) -> usize {
        self.effects()
            .iter()
            .filter(|effect| matches!(effect, Effect::Trial { .. }))
            .count()
    }

    fn paused(&self) -> bool {
        self.effects()
            .iter()
            .any(|effect| matches!(effect, Effect::Paused { .. }))
    }

    /// Each row's version, by its place in the inbox: what a statement
    /// rewrote since shows as another.
    async fn versions(&self) -> HashMap<i64, String> {
        let sql = format!("SELECT received_position, xmin::text FROM {}", self.table);
        self.sessions
            .session(async |session| {
                Ok::<_, Error>(
                    session
                        .connection()
                        .query(&sql, &[])
                        .await?
                        .iter()
                        .map(|row| (row.get(0), row.get(1)))
                        .collect(),
                )
            })
            .await
            .unwrap()
    }

    /// One text `sql` reads, `{table}` standing for the inbox's table.
    async fn text(&self, sql: &str) -> String {
        let sql = sql.replace("{table}", &self.table);
        self.sessions
            .session(async |session| {
                Ok::<_, Error>(session.connection().query_one(&sql, &[]).await?.get(0))
            })
            .await
            .unwrap()
    }

    /// One number `sql` reads, `{table}` standing for the inbox's table.
    async fn count(&self, sql: &str) -> i64 {
        let sql = sql.replace("{table}", &self.table);
        self.sessions
            .session(async |session| {
                Ok::<_, Error>(session.connection().query_one(&sql, &[]).await?.get(0))
            })
            .await
            .unwrap()
    }

    async fn unpark_all(&self, unparking: &Unparking<'_>) -> UnparkedAll {
        self.inbox.unpark_all("ops", unparking).await.unwrap()
    }

    async fn received_position(&self, stream: &str) -> i64 {
        let sql = format!(
            "SELECT received_position FROM {} WHERE stream_id->>'id' = $1",
            self.table
        );
        self.sessions
            .session(async |session| {
                Ok::<_, Error>(
                    session
                        .connection()
                        .query_one(&sql, &[&stream])
                        .await?
                        .get(0),
                )
            })
            .await
            .unwrap()
    }
}

#[derive(Debug, PartialEq)]
struct Row {
    attempts: i32,
    cause: Option<String>,
    holds: Option<i32>,
    kind: Option<String>,
    held: bool,
    parked: bool,
    processed: bool,
    next_in: Option<f64>,
}

fn message(stream: &str, position: i32) -> InboxMessage {
    message_of("tenant-1", stream, position)
}

fn message_of(tenant: &str, stream: &str, position: i32) -> InboxMessage {
    message_on("orders", tenant, stream, position)
}

/// A message of `topic`'s channel, `kafka://<topic>`.
fn message_on(topic: &str, tenant: &str, stream: &str, position: i32) -> InboxMessage {
    static EVENT: AtomicU64 = AtomicU64::new(1);
    let event = EVENT.fetch_add(1, Ordering::Relaxed);
    InboxMessage::new(
        tenant,
        "orders.Order",
        json!({ "id": stream }),
        position,
        format!("kafka://{topic}/{stream}"),
        format!("{stream}@{position}"),
    )
    .with_metadata(json!({ "message_id": format!("00000000-0000-4000-8000-{event:012x}") }))
}

fn stage_down(retry_after: Option<Duration>) -> Result<(), Failure> {
    Err(Unavailable::new(
        KMS_UNAVAILABLE,
        Scope::Stage,
        retry_after,
        "Vault is sealed",
    )
    .into())
}

fn tenant_down(retry_after: Option<Duration>) -> Result<(), Failure> {
    Err(Unavailable::new(KMS_THROTTLED, Scope::Tenant, retry_after, "429").into())
}

fn ok() -> Result<(), Failure> {
    Ok(())
}

fn held(attempts: u32) -> Outcome {
    Outcome::Failed {
        attempts,
        parked: false,
        held: true,
    }
}

fn spent(attempts: u32, parked: bool) -> Outcome {
    Outcome::Failed {
        attempts,
        parked,
        held: false,
    }
}

/// An `Unavailable` holds its row and spends no attempt, at either scope,
/// however often it is told: twenty times, nothing spent, nothing parked,
/// the row's count of holds the verdicts', its kind `unavailable` (ADR-0042
/// decisions 2 and 3). Red when it is routed to the spend.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn an_unavailable_spends_no_attempt_and_never_parks_at_either_scope() {
    for (scope, verdict) in [
        (
            "stage",
            stage_down as fn(Option<Duration>) -> Result<(), Failure>,
        ),
        ("tenant", tenant_down),
    ] {
        let f = fixture(&format!("never_spends_{scope}"), Retries::up_to(3)).await;
        f.publish(&[message("a", 1)]).await;
        for n in 1..=20 {
            tokio::time::sleep(Duration::from_millis(220)).await;
            f.wake().await;
            assert_eq!(
                f.dispatch(|| verdict(None)).await,
                held(0),
                "{scope}: ask {n}"
            );
        }
        let row = f.row("a").await;
        assert_eq!(
            (
                row.attempts,
                row.parked,
                row.holds,
                row.kind.as_deref(),
                row.held
            ),
            (0, false, Some(20), Some("unavailable"), true),
            "{scope}"
        );
        assert!(f.told().iter().all(|told| told.held && !told.parked));
        tokio::time::sleep(Duration::from_millis(220)).await;
        f.wake().await;
        assert_eq!(f.dispatch(ok).await, Outcome::Processed, "{scope}");
        let row = f.row("a").await;
        assert_eq!(
            (row.processed, row.cause, row.holds, row.kind, row.held),
            (true, None, None, None, false),
            "{scope}: the mark leaves nothing held"
        );
    }
}

/// A Tenant's `Unavailable` is remembered: its other heads are held without
/// their subscriber called while its back-off runs, under its cause, the
/// row's count of holds untouched; a healthy Tenant beside it is asked.
/// Red without the memo.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_tenant_s_heads_are_held_unasked_while_its_unavailable_is_remembered() {
    let f = fixture_cut(
        "memo",
        Retries::up_to(3),
        4,
        Breaking::default().with_hold(Duration::from_secs(60), Duration::from_secs(60)),
    )
    .await;
    for stream in ["a", "b", "c", "d", "e", "f", "g", "h"] {
        f.publish(&[message_of("throttled", stream, 1)]).await;
    }
    f.publish(&[message_of("healthy", "z", 1)]).await;
    let calls = AtomicU32::new(0);
    let verdict = |message: &InboxMessage| {
        calls.fetch_add(1, Ordering::SeqCst);
        if message.tenant_id == "throttled" {
            tenant_down(None)
        } else {
            Ok(())
        }
    };
    let mut outcomes = Vec::new();
    for _ in 0..12 {
        outcomes.push(f.dispatch_on(verdict).await);
    }
    let throttled_asked = f.told().iter().filter(|told| told.held).count();
    assert_eq!(
        throttled_asked, 1,
        "the throttled Tenant asked once: {outcomes:?}"
    );
    assert!(
        outcomes.contains(&Outcome::Processed),
        "the healthy Tenant asked: {outcomes:?}"
    );
    assert!(
        outcomes.contains(&Outcome::Held),
        "a head held unasked: {outcomes:?}"
    );
    let unasked = f
        .count(
            "SELECT count(*) FROM {table} WHERE tenant_id = 'throttled' AND hold_kind = 'unavailable' \
               AND holds IS NULL AND cause = 'kms.throttled' AND next_attempt_at > now()",
        )
        .await;
    assert!(
        unasked >= 1,
        "a head held unasked under the cause, its count untouched"
    );
}

/// A `Stage` hold on a row runs at most the tick's cap, whatever the row has
/// counted, so that a process cut off alone delays another's head a tick at
/// most; a `Tenant` hold runs to the Tenant's back-off. Red with the memo's
/// clock written to the row for a `Stage` scope.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_stage_hold_on_a_row_runs_a_tick_at_most_and_a_tenant_s_its_back_off() {
    let breaking = Breaking::default()
        .with_hold(Duration::from_secs(1), Duration::from_secs(300))
        .with_tick(Duration::from_secs(1), Duration::from_secs(15));
    let f = fixture_cut("tick_hold", Retries::up_to(3), 1, breaking).await;
    f.publish(&[message("a", 1), message_of("tenant-2", "b", 1)])
        .await;
    f.execute("UPDATE {table} SET holds = 20").await;
    f.dispatch_on(|message| {
        if message.tenant_id == "tenant-1" {
            stage_down(None)
        } else {
            tenant_down(None)
        }
    })
    .await;
    f.wake().await;
    f.dispatch_on(|message| {
        if message.tenant_id == "tenant-1" {
            stage_down(None)
        } else {
            tenant_down(None)
        }
    })
    .await;
    let stage = f.row("a").await.next_in.expect("held");
    let tenant = f.row("b").await.next_in.expect("held");
    assert!(stage <= 15.0 && stage > 0.0, "a stage's hold: {stage} s");
    assert!(
        tenant > 200.0,
        "a Tenant's: {tenant} s, its back-off at the cap"
    );
}

/// A channel broadly unavailable at `Stage` scope is paused in this process:
/// its heads are walked as a guest's, no subscriber called, the slot's
/// cursor, round and sleep left as they were and the guest's own cursor
/// moved; another process over the table, its own breaker closed, takes the
/// head at once. Then a trial at the next tick asks a witness, and the
/// stage answering resumes the channel. Red when a guest writes the round or
/// the cursor, or without the pause.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_channel_broadly_unavailable_is_paused_walked_as_a_guest_and_resumed_by_a_trial() {
    let breaking = quick().with_tick(Duration::from_secs(2), Duration::from_secs(2));
    let f = fixture_cut("paused", Retries::up_to(3), 1, breaking).await;
    // A walk of two Tenants of three: the guest's cursor moves past them.
    let f = Fixture {
        inbox: f.inbox.with_rotation(ascetic_ddd_inbox::Rotation {
            walk: 2,
            max_dormancy: Duration::from_secs(1),
        }),
        ..f
    };
    for tenant in ["t1", "t2", "t3"] {
        f.publish(&[message_of(tenant, &format!("{tenant}-a"), 1)])
            .await;
    }
    for _ in 0..3 {
        f.dispatch(|| stage_down(None)).await;
        f.wake().await;
    }
    assert!(
        f.effects()
            .iter()
            .any(|e| matches!(e, Effect::Paused { channel, .. } if channel == "kafka://orders")),
        "paused: {:?}",
        f.effects()
    );
    let state = async || {
        let sql = format!(
            "SELECT coalesce(last_tenant, ''), coalesce(round_from, ''), \
                    coalesce(dormant_until::text, ''), coalesce(dormant_after::text, ''), \
                    coalesce(guest_tenant, '') \
             FROM {}_slots",
            f.table
        );
        f.sessions
            .session(async |session| {
                let row = session.connection().query_one(&sql, &[]).await?;
                Ok::<_, Error>((0..5).map(|i| row.get::<_, String>(i)).collect::<Vec<_>>())
            })
            .await
            .unwrap()
    };
    f.due().await;
    let calls = AtomicU32::new(0);
    let counting = |_: &InboxMessage| {
        calls.fetch_add(1, Ordering::SeqCst);
        stage_down(None)
    };
    // The guest met a head of a Tenant not yet asked: a witness, tried at
    // once rather than at the tick (ADR-0042 decision 1).
    assert_eq!(f.dispatch_on(counting).await, held(0), "the witness tried");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    f.wake().await;
    let before = state().await;
    assert_eq!(f.dispatch_on(counting).await, Outcome::Paused);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "no subscriber called by the guest"
    );
    let after = state().await;
    assert_eq!(
        before[..4],
        after[..4],
        "the cursor, the round and the sleep left"
    );
    assert_ne!(before[4], after[4], "the guest's own cursor moved");
    // Another process, its breaker closed, asks a head at once.
    let beside = f.beside(Breaking::default());
    let taken = beside
        .dispatch(|_: &PgSession, _: &InboxMessage| std::future::ready(Ok::<(), Failure>(())))
        .await
        .unwrap();
    assert_eq!(taken, Outcome::Processed, "the other process asks it");
    // The stage answers: the next tick's trial resumes the channel.
    let mut resumed = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        f.wake().await;
        f.dispatch(ok).await;
        if f.effects()
            .iter()
            .any(|e| matches!(e, Effect::Resumed { .. }))
        {
            resumed = true;
            break;
        }
    }
    assert!(resumed, "a trial resumed the channel: {:?}", f.effects());
}

/// A dispatch under another isolation than READ COMMITTED is refused, for
/// its transaction: its locks and its waits hold under READ COMMITTED only
/// (ADR-0042 decision 10). Red when the level is checked at start only.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_dispatch_under_repeatable_read_is_refused() {
    let f = fixture("isolation", Retries::up_to(3)).await;
    f.publish(&[message("a", 1)]).await;
    let mut config = Config::from_str(&url()).unwrap();
    config.options("-c default_transaction_isolation=repeatable\\ read");
    let other = PgInbox::new(PgSessionPool::new(pool_of(config)))
        .with_table(
            Identifier::new(&f.table).unwrap(),
            Identifier::new(format!("{}_seq", f.table)).unwrap(),
        )
        .with_slots(1)
        .partitioned_by(ByStream);
    let refused = other
        .dispatch(|_: &PgSession, _: &InboxMessage| std::future::ready(Ok::<(), Failure>(())))
        .await
        .unwrap_err();
    assert!(refused.to_string().contains("isolation"), "{refused}");
    assert!(!f.row("a").await.processed);
}

/// A cause lives as long as the state it explains and every exit clears it:
/// a park by attempts keeps its cause, the unpark clears it, a permanent
/// park keeps its own, the resolve clears it, the mark clears a hold's.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_cause_is_kept_while_its_state_lasts_and_cleared_at_every_exit() {
    let f = fixture("exits", Retries::up_to(1)).await;
    f.publish(&[message("a", 1)]).await;

    assert_eq!(
        f.dispatch(|| Err(Transient::because(DB_TIMEOUT, "a moment").into()))
            .await,
        spent(1, true)
    );
    let exhausted = f.row("a").await;
    assert_eq!(
        (
            exhausted.cause.as_deref(),
            exhausted.held,
            exhausted.next_in
        ),
        (Some("db.timeout"), false, None),
        "parked under its cause, holding nothing"
    );
    let at = f.received_position("a").await;
    assert!(
        f.sessions
            .session(async |session| f.inbox.unpark(&session, at).await)
            .await
            .unwrap()
    );
    let unparked = f.row("a").await;
    assert_eq!(
        (unparked.cause, unparked.held),
        (None, false),
        "the unpark clears"
    );

    assert_eq!(
        f.dispatch(|| Err(Permanent::because(KMS_GONE, "no key").into()))
            .await,
        spent(1, true)
    );
    assert_eq!(f.row("a").await.cause.as_deref(), Some("kms.gone"));
    assert!(
        f.sessions
            .session(async |session| f.inbox.resolve(&session, at).await)
            .await
            .unwrap()
    );
    let resolved = f.row("a").await;
    assert_eq!(
        (resolved.processed, resolved.cause),
        (true, None),
        "the resolve clears"
    );

    f.publish(&[message("b", 1)]).await;
    assert_eq!(f.dispatch(|| stage_down(None)).await, held(0));
    tokio::time::sleep(Duration::from_millis(220)).await;
    f.due().await;
    assert_eq!(f.dispatch(ok).await, Outcome::Processed);
    let marked = f.row("b").await;
    assert_eq!(
        (marked.cause, marked.held, marked.holds, marked.kind),
        (None, false, None, None),
        "the mark clears"
    );
    let told: Vec<String> = f.told().into_iter().map(|told| told.cause).collect();
    assert_eq!(told, ["db.timeout", "kms.gone", "kms.unavailable"]);
}

/// A row waiting for a dependency that arrived and is held for an
/// `Unavailable` does not expire: what holds the dependency holds what
/// waits for it (ADR-0042 decision 7). Once the dependency is processed,
/// the waiter goes on.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_row_waiting_for_a_held_dependency_does_not_expire() {
    let f = fixture_waiting("spared", Retries::up_to(3), Duration::from_secs(600)).await;
    let cause = message("a", 1);
    let effect = message("b", 1).depending_on(&[CausalDependency::on(&cause)]);
    f.publish(std::slice::from_ref(&effect)).await;
    assert_eq!(f.dispatch(ok).await, Outcome::SetAside, "b@1 waits for a@1");
    f.publish(std::slice::from_ref(&cause)).await;
    assert_eq!(f.dispatch(|| stage_down(None)).await, held(0));

    f.execute(
        "UPDATE {table} SET waiting_since = now() - interval '20 minutes' \
         WHERE waiting_since IS NOT NULL",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(220)).await;
    f.due().await;
    assert_eq!(f.dispatch(|| stage_down(None)).await, held(0));
    assert_eq!(f.inbox.expire().await.unwrap().parked, 0);
    assert!(!f.row("b").await.parked, "spared while a@1 holds");
    tokio::time::sleep(Duration::from_millis(220)).await;
    f.due().await;
    assert_eq!(f.dispatch(ok).await, Outcome::Processed, "a@1");
    assert!(!f.row("b").await.parked, "woken, not expired");
}

/// The holds are counted by kind and cause: a head waiting its own back-off
/// and the heads held for an `Unavailable` are told apart — the one asked,
/// and its Tenant's other head, held unasked, which counted no verdict of
/// its own (ADR-0042 decision 6). Red when the kind is read from the row's
/// count of holds rather than its own.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_holds_are_counted_by_kind_and_cause() {
    let f = fixture_cut(
        "kinds",
        Retries::up_to(5).with_backoff(|_| Duration::from_secs(60)),
        2,
        Breaking::default().with_hold(Duration::from_secs(60), Duration::from_secs(60)),
    )
    .await;
    f.publish(&[message("a", 1)]).await;
    for stream in ["b1", "b2", "b3", "b4", "b5", "b6"] {
        f.publish(&[message_of("tenant-2", stream, 1)]).await;
    }
    assert_eq!(
        f.count("SELECT count(DISTINCT slot) FROM {table} WHERE tenant_id = 'tenant-2'")
            .await,
        2,
        "tenant-2 has a head in each slot"
    );
    for _ in 0..6 {
        f.dispatch_on(|message| {
            if message.tenant_id == "tenant-1" {
                Err(Transient::because(DB_TIMEOUT, "a moment").into())
            } else {
                tenant_down(None)
            }
        })
        .await;
        f.wake().await;
    }
    assert_eq!(f.withheld(), 1, "tenant-2's second head held unasked");
    let counts = f.inbox.held_counts(Duration::from_secs(5)).await.unwrap();
    let seen: Vec<_> = counts
        .iter()
        .map(|c| (c.hold, c.cause.as_deref(), c.tenants, c.heads))
        .collect();
    assert_eq!(
        seen,
        [
            (Hold::Backoff, Some("db.timeout"), 1, 1),
            (Hold::Unavailable, Some("kms.throttled"), 1, 2),
        ]
    );
}

/// R1: a row's holds are written as decided. Its count of holds stops at
/// the column's bound rather than overflow; a `Transient` after holds
/// forgets them, the row waiting its own back-off, still queued; and a hold
/// runs from the verdict's write, not from the transaction's start, which a
/// slow subscriber left behind. Red with no bound, with a spend that keeps
/// the holds or drops the back-off, or with the transaction's clock.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_row_s_holds_are_bounded_forgotten_by_a_spend_and_timed_from_the_verdict() {
    let breaking = quick().with_hold(Duration::from_millis(40), Duration::from_secs(5));
    let f = fixture_cut(
        "rows",
        Retries::up_to(5).with_backoff(|_| Duration::from_secs(60)),
        1,
        breaking,
    )
    .await;
    f.publish(&[message("a", 1)]).await;
    f.execute(
        "UPDATE {table} SET holds = 2147483647, held_since = now(), hold_kind = 'unavailable', \
             cause = 'kms.throttled', next_attempt_at = now()",
    )
    .await;
    assert_eq!(f.dispatch(|| tenant_down(None)).await, held(0));
    assert_eq!(
        f.row("a").await.holds,
        Some(2147483647),
        "bounded, not overflowed"
    );

    // another Tenant, held once and its memo run out, then spent: tenant-1
    // is held for the memo's cap now, its count at the bound
    f.publish(&[message_of("tenant-3", "c", 1)]).await;
    let held_then_spent = |message: &InboxMessage| match message.tenant_id.as_str() {
        "tenant-3" => tenant_down(None),
        _ => Ok(()),
    };
    assert_eq!(f.dispatch_on(held_then_spent).await, held(0));
    assert_eq!(f.row("c").await.holds, Some(1));
    tokio::time::sleep(Duration::from_millis(200)).await;
    // c@1 due, a@1 still held by its Tenant's memo
    f.execute("UPDATE {table} SET next_attempt_at = now() WHERE stream_id->>'id' = 'c'")
        .await;
    f.wake().await;
    assert_eq!(
        f.dispatch_on(|message| match message.tenant_id.as_str() {
            "tenant-3" => Err(Transient::because(DB_TIMEOUT, "a moment").into()),
            _ => Ok(()),
        })
        .await,
        spent(1, false)
    );
    let spent = f.row("c").await;
    assert_eq!(
        (spent.holds, spent.kind.as_deref(), spent.held, spent.parked),
        (None, Some("backoff"), true, false),
        "the holds forgotten, the row queued"
    );
    let back_off = spent.next_in.expect("its back-off");
    assert!(back_off > 50.0, "its own back-off, a minute: {back_off} s");

    f.publish(&[message_of("tenant-2", "b", 1)]).await;
    let slow = |_: &PgSession, message: &InboxMessage| {
        let tenant = message.tenant_id.clone();
        async move {
            if tenant == "tenant-2" {
                tokio::time::sleep(Duration::from_millis(1500)).await;
                tenant_down(Some(Duration::from_secs(1)))
            } else {
                Ok(())
            }
        }
    };
    let outcome = f.inbox.dispatch(slow).await.unwrap();
    assert_eq!(outcome, held(0));
    let next = f.row("b").await.next_in.expect("held");
    assert!(
        next > 0.5,
        "a second from the verdict, not from the transaction's start: {next} s"
    );
}

/// I1: a trial and a resolve under REPEATABLE READ are refused for their
/// transaction, as a dispatch is (ADR-0042 decision 10). The level is read
/// for each transaction: a session whose level changed after the process
/// began is refused all the same. Red when a trial or a resolve skips the
/// check, or when the level is read once.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_trial_and_a_resolve_under_repeatable_read_are_refused() {
    let f = fixture("isolated", Retries::up_to(3)).await;
    let one = pool_of(Config::from_str(&url()).unwrap());
    one.resize(1);
    let inbox = PgInbox::new(PgSessionPool::new(one.clone()))
        .with_table(
            Identifier::new(&f.table).unwrap(),
            Identifier::new(format!("{}_seq", f.table)).unwrap(),
        )
        .with_slots(1)
        .partitioned_by(ByStream)
        .with_breaking(quick())
        .observed_by(Arc::clone(&f.told));
    for tenant in ["t1", "t2", "t3"] {
        f.publish(&[message_of(tenant, &format!("{tenant}-a"), 1)])
            .await;
    }
    let down = |_: &PgSession, _: &InboxMessage| std::future::ready(stage_down(None));
    for _ in 0..3 {
        inbox.dispatch(down).await.unwrap();
        f.wake().await;
    }
    assert!(f.paused(), "{:?}", f.effects());
    one.get()
        .await
        .unwrap()
        .batch_execute("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    let trials = f.trials();
    let asked = AtomicU32::new(0);
    let refused = inbox
        .dispatch(|_: &PgSession, _: &InboxMessage| {
            asked.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok::<(), Failure>(()))
        })
        .await
        .unwrap_err();
    assert_eq!(f.trials(), trials + 1, "the call was a trial");
    assert!(refused.to_string().contains("isolation"), "{refused}");
    assert_eq!(asked.load(Ordering::SeqCst), 0, "no witness asked");

    f.publish(&[message("p", 1)]).await;
    f.execute(
        "UPDATE {table} SET parked_at = now(), cause = 'kms.gone', held_since = NULL, \
                   holds = NULL, hold_kind = NULL, next_attempt_at = NULL \
               WHERE stream_id->>'id' = 'p'",
    )
    .await;
    let at = f.received_position("p").await;
    let refused = f
        .sessions
        .session(async |session| {
            session
                .atomic(async |tx| {
                    tx.connection()
                        .batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                        .await?;
                    f.inbox.resolve(&tx, at).await
                })
                .await
        })
        .await
        .unwrap_err();
    assert!(refused.to_string().contains("isolation"), "{refused}");
    assert!(f.row("p").await.parked, "the resolve refused");
    assert!(
        f.sessions
            .session(async |session| f.inbox.resolve(&session, at).await)
            .await
            .unwrap(),
        "under READ COMMITTED it resolves"
    );
}

/// C8: a trial whose witness's subscriber hangs past `trial_wait` is cut
/// off there: the call returns within the wait, its transaction rolled
/// back and the witness's row as it was, and the trial is counted timed out,
/// so a slot is never held by a hanging trial longer than its wait. Red
/// without the trial's timeout.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_hanging_trial_is_cut_off_at_its_wait() {
    let wait = Duration::from_millis(300);
    let f = fixture_cut(
        "trial_wait",
        Retries::up_to(3),
        1,
        quick().with_trial_wait(wait),
    )
    .await;
    for tenant in ["t1", "t2", "t3"] {
        f.publish(&[message_of(tenant, &format!("{tenant}-a"), 1)])
            .await;
    }
    while !f.paused() {
        f.dispatch(|| stage_down(None)).await;
        f.wake().await;
    }
    let hanging = |_: &PgSession, _: &InboxMessage| async {
        tokio::time::sleep(Duration::from_secs(5)).await;
        Ok::<(), Failure>(())
    };
    let (trials, versions) = (f.trials(), f.versions().await);
    let mut calls = 0;
    loop {
        tokio::time::sleep(Duration::from_millis(60)).await;
        f.wake().await;
        let started = std::time::Instant::now();
        let outcome = f.inbox.dispatch(hanging).await.unwrap();
        if f.trials() > trials {
            let took = started.elapsed();
            assert!(
                took < wait + Duration::from_millis(700),
                "the trial took {took:?}"
            );
            assert_eq!(outcome, Outcome::Nothing);
            break;
        }
        calls += 1;
        assert!(calls < 50, "no trial");
    }
    assert_eq!(f.versions().await, versions, "the witness's row as it was");
    assert!(
        f.paused()
            && !f
                .effects()
                .iter()
                .any(|e| matches!(e, Effect::Resumed { .. }))
    );
}

/// C10: a paused channel whose one head is among many Tenants' heads of
/// another channel is tried by its witness, not found by a walk that
/// serves the others: once the stage answers, the head is processed within
/// two ticks. The slot's oldest rows are the other channel's, so a trial
/// that took the slot's head would try them. Red when the trial looks past
/// its witness.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_sparse_paused_channel_s_head_is_tried_by_its_witness() {
    let tick = Duration::from_secs(1);
    let f = fixture_cut(
        "sparse",
        Retries::up_to(3),
        1,
        quick().with_tick(tick, tick),
    )
    .await;
    let f = Fixture {
        inbox: f.inbox.with_rotation(Rotation {
            walk: 4,
            max_dormancy: Duration::from_secs(1),
        }),
        ..f
    };
    for tenant in 0..40 {
        for position in 1..=20 {
            f.publish(&[message_on(
                "payments",
                &format!("p{tenant:02}"),
                &format!("p{tenant:02}"),
                position,
            )])
            .await;
        }
    }
    for tenant in ["t1", "t2", "t3"] {
        f.publish(&[message_of(tenant, &format!("{tenant}-a"), 1)])
            .await;
    }
    let orders_down = |message: &InboxMessage| {
        if message.uri.starts_with("kafka://orders/") {
            stage_down(None)
        } else {
            Ok(())
        }
    };
    let mut calls = 0;
    while !f.paused() {
        f.dispatch_on(orders_down).await;
        f.wake().await;
        calls += 1;
        assert!(calls < 200, "never paused");
    }
    // t2's and t3's heads processed elsewhere: t1's is the channel's last
    f.execute(
        "UPDATE {table} SET processed_position = nextval('{table}_seq'), processed_at = now(), \
             cause = NULL, held_since = NULL, holds = NULL, hold_kind = NULL, next_attempt_at = NULL \
         WHERE tenant_id IN ('t2', 't3')",
    )
    .await;
    let recovered = std::time::Instant::now();
    while !f.row("t1-a").await.processed {
        f.dispatch(ok).await;
        f.wake().await;
        assert!(
            recovered.elapsed() <= 2 * tick + Duration::from_millis(500),
            "t1's head not processed within two ticks: {:?}",
            f.effects()
        );
    }
}

/// C5: one ask in flight a (channel, Tenant) in a process. A throttled
/// Tenant's subscriber hangs; four dispatchers share the process's breaker
/// and eight slots, the Tenant a head in each, twenty healthy Tenants
/// arriving while it hangs: the Tenant is asked by one at a time, each of its other heads
/// held once a hang rather than met again and again, and a healthy Tenant
/// asked while it hangs. Red without the in-flight mark, or with the hold of a head
/// met in flight cut to the Tenant's back-off.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_hanging_tenant_is_asked_by_one_dispatcher_at_a_time() {
    let f = fixture_cut("flight", Retries::up_to(3), 8, quick()).await;
    for stream in 0..32 {
        f.publish(&[message_of("x", &format!("x{stream:02}"), 1)])
            .await;
    }
    let heads = f
        .count("SELECT count(DISTINCT slot) FROM {table} WHERE tenant_id = 'x'")
        .await;
    assert_eq!(heads, 8, "x has a head in each slot");
    let hang = AtomicBool::new(false);
    let (in_flight, most, asked) = (AtomicU32::new(0), AtomicU32::new(0), AtomicU32::new(0));
    let beside = AtomicU32::new(0);
    let subscriber = |_: &PgSession, message: &InboxMessage| {
        let x = message.tenant_id == "x";
        let hang = hang.load(Ordering::SeqCst);
        let (in_flight, most, asked, beside) = (&in_flight, &most, &asked, &beside);
        async move {
            if !x {
                // a healthy Tenant asked while an ask of x hangs
                if in_flight.load(Ordering::SeqCst) > 0 {
                    beside.fetch_add(1, Ordering::SeqCst);
                }
                return Ok(());
            }
            asked.fetch_add(1, Ordering::SeqCst);
            let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            most.fetch_max(now, Ordering::SeqCst);
            if hang {
                tokio::time::sleep(Duration::from_millis(1500)).await;
            }
            in_flight.fetch_sub(1, Ordering::SeqCst);
            tenant_down(None)
        }
    };
    // x's first `Unavailable`, quick: the memo knows it from then on
    while asked.load(Ordering::SeqCst) == 0 {
        f.inbox.dispatch(subscriber).await.unwrap();
    }
    hang.store(true, Ordering::SeqCst);
    let withheld = f.withheld();
    let until = std::time::Instant::now() + Duration::from_secs(4);
    let dispatcher = async || {
        while std::time::Instant::now() < until {
            f.inbox.dispatch(subscriber).await.unwrap();
        }
    };
    // one waker: the test's wake updates every slot, and two at once lock
    // them in each other's order
    let waker = async {
        while std::time::Instant::now() < until {
            tokio::time::sleep(Duration::from_millis(50)).await;
            f.wake().await;
        }
    };
    // twenty healthy Tenants arrive once an ask of x hangs: what the other
    // dispatchers do meanwhile
    let arrivals = async {
        while in_flight.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        for tenant in 0..20 {
            f.publish(&[message_of(
                &format!("h{tenant:02}"),
                &format!("h{tenant:02}"),
                1,
            )])
            .await;
        }
    };
    futures::future::join3(
        futures::future::join_all((0..4).map(|_| dispatcher())),
        waker,
        arrivals,
    )
    .await;
    let asks = asked.load(Ordering::SeqCst) - 1;
    assert_eq!(most.load(Ordering::SeqCst), 1, "one ask of x at a time");
    assert!(asks >= 1, "x asked");
    let held = f.withheld() - withheld;
    assert!(
        held <= 7 * asks,
        "{held} holds of x's other heads over {asks} hanging asks"
    );
    // the healthy go on while x hangs, never behind it: told by what ran
    // during a hang, not by a deadline, which a loaded machine misses
    assert!(
        beside.load(Ordering::SeqCst) >= 1,
        "no healthy Tenant asked while x hung"
    );
}

/// G1: a paused channel's work in a process does not grow with its
/// Tenants. At one slot, a walk of four and a memo of sixteen, at 8 and at
/// 64 Tenants each with one head and the stage down: the channel pauses
/// after the fewest Tenants the breaker weighs are asked, and from then on
/// every call of the subscriber is a trial's, every row written a trial's
/// witness, a guest's walk four Tenants a call at most, and the log says
/// the pause and the stall alone, at 64 as at 8. Red when the walk takes a
/// paused head, holds each head it meets, walks without its bound, or logs
/// each hold.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_paused_channel_s_work_does_not_grow_with_its_tenants() {
    let mut logged = Vec::new();
    for tenants in [8usize, 64] {
        let f = fixture_cut(
            &format!("flat_{tenants}"),
            Retries::up_to(3),
            1,
            quick().with_memo(16),
        )
        .await;
        let f = Fixture {
            inbox: f.inbox.with_rotation(Rotation {
                walk: 4,
                max_dormancy: Duration::from_secs(1),
            }),
            ..f
        };
        let names: Vec<String> = (0..tenants).map(|t| format!("t{t:02}")).collect();
        for name in &names {
            f.publish(&[message_of(name, &format!("{name}-a"), 1)])
                .await;
        }
        let lines = my_lines().len();
        let calls = AtomicU32::new(0);
        let down = |_: &InboxMessage| {
            calls.fetch_add(1, Ordering::SeqCst);
            stage_down(None)
        };
        while !f.paused() {
            f.dispatch_on(down).await;
            f.wake().await;
            assert!(calls.load(Ordering::SeqCst) <= 8, "{tenants}: never paused");
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "{tenants}: the fewest asked"
        );
        let (versions, trials, asked) =
            (f.versions().await, f.trials(), calls.load(Ordering::SeqCst));
        let index = |cursor: &str| names.iter().position(|name| name == cursor);
        let mut cursor = f
            .text("SELECT coalesce(guest_tenant, last_tenant, '') FROM {table}_slots")
            .await;
        let mut walks = 0;
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            f.wake().await;
            let tried = f.trials();
            let outcome = f.dispatch_on(down).await;
            let next = f
                .text("SELECT coalesce(guest_tenant, last_tenant, '') FROM {table}_slots")
                .await;
            // a call that was not a trial walked: four Tenants exactly, none
            // due among more than four — never the whole circle back to
            // where it began
            if f.trials() == tried {
                assert_eq!(outcome, Outcome::Paused, "{tenants}");
                let (from, to) = (index(&cursor).unwrap(), index(&next).unwrap());
                let step = (to + tenants - from) % tenants;
                assert_eq!(step, 4, "{tenants}: a guest's walk of {step} Tenants");
                walks += 1;
            }
            cursor = next;
        }
        assert!(walks >= 10, "{tenants}: {walks} walks of 20 calls");
        let trials = f.trials() - trials;
        let asked = calls.load(Ordering::SeqCst) - asked;
        assert!(trials >= 1, "{tenants}: a trial a tick");
        assert!(
            asked as usize <= trials,
            "{tenants}: {asked} calls, {trials} trials"
        );
        let after = f.versions().await;
        let written = after
            .iter()
            .filter(|(position, version)| versions.get(position) != Some(version))
            .count();
        assert!(
            written <= asked as usize,
            "{tenants}: {written} rows written, {asked} witnesses asked"
        );
        assert_eq!(f.withheld(), 0, "{tenants}: no head held unasked");
        let said: Vec<String> = my_lines()[lines..].to_vec();
        assert!(
            said.iter()
                .all(|line| line.contains("paused in this process") || line.contains("stalled")),
            "{tenants}: {said:?}"
        );
        logged.push(said.len());
    }
    assert_eq!(logged[0], logged[1], "as many lines at 64 Tenants as at 8");
}

/// P1: a guest serves what is due beyond the paused heads, and in turn. A
/// channel paused at ten Tenants, of 128 characters each, and a walk of
/// four: a head of another channel past them is taken within four calls;
/// three Tenants backlogged on that channel are served in turn; a guest's
/// take clears the slot's round, its empty walk leaves it; and no index of
/// the slots' table reads what a guest writes, so that its write stays HOT.
/// Red when the guest walks from the slot's cursor, keeps its own on a
/// take, keeps the round, or its cursor is indexed.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_guest_serves_what_is_due_beyond_the_paused_heads_in_turn() {
    let f = fixture_cut(
        "guest",
        Retries::up_to(3),
        1,
        quick().with_tick(Duration::from_secs(30), Duration::from_secs(30)),
    )
    .await;
    let f = Fixture {
        inbox: f.inbox.with_rotation(Rotation {
            walk: 4,
            max_dormancy: Duration::from_secs(1),
        }),
        ..f
    };
    let wide = |name: &str| format!("{name:a>128}");
    for tenant in 0..10 {
        let name = wide(&format!("t{tenant:02}"));
        f.publish(&[message_of(&name, &format!("t{tenant:02}"), 1)])
            .await;
    }
    while !f.paused() {
        f.dispatch(|| stage_down(None)).await;
        f.wake().await;
    }
    // The stage stays down for the paused channel: its trial fails and it
    // stays paused; the other channel answers.
    let verdict = |message: &InboxMessage| {
        if message.uri.starts_with("kafka://orders/") {
            stage_down(None)
        } else {
            Ok(())
        }
    };
    // a head of another channel, its Tenant past the ten
    f.publish(&[message_on("payments", &wide("u0"), "u0", 1)])
        .await;
    let mut calls = 0;
    while !f.row("u0").await.processed {
        f.dispatch_on(verdict).await;
        f.wake().await;
        calls += 1;
        assert!(calls <= 4, "the guest never reached it");
    }
    // three Tenants backlogged on it, served in turn
    for tenant in ["u1", "u2", "u3"] {
        for position in 1..=2 {
            f.publish(&[message_on(
                "payments",
                &wide(tenant),
                &format!("{tenant}-{position}"),
                position,
            )])
            .await;
        }
    }
    let served = Mutex::new(Vec::new());
    let mut calls = 0;
    while served.lock().unwrap().len() < 6 {
        f.dispatch_on(|message| {
            if message.uri.starts_with("kafka://payments/") {
                served
                    .lock()
                    .unwrap()
                    .push(message.tenant_id.trim_start_matches('a').to_owned());
            }
            verdict(message)
        })
        .await;
        f.wake().await;
        calls += 1;
        assert!(calls <= 12, "{:?}", served.lock().unwrap());
    }
    let served = served.into_inner().unwrap();
    assert!(
        served.windows(2).all(|pair| pair[0] != pair[1]),
        "served in turn: {served:?}"
    );
    // the round: a guest's take clears it; its empty walk leaves it
    f.publish(&[message_on("payments", &wide("u4"), "u4", 1)])
        .await;
    f.execute("UPDATE {table}_slots SET round_from = 'z'").await;
    assert_eq!(f.dispatch_on(verdict).await, Outcome::Processed);
    assert_eq!(
        f.text("SELECT coalesce(round_from, '') FROM {table}_slots")
            .await,
        "",
        "a guest's take clears the round"
    );
    f.execute("UPDATE {table}_slots SET round_from = 'z'").await;
    assert_eq!(f.dispatch_on(verdict).await, Outcome::Paused);
    assert_eq!(
        f.text("SELECT coalesce(round_from, '') FROM {table}_slots")
            .await,
        "z",
        "a guest's empty walk leaves it"
    );
    assert_eq!(
        f.count(
            "SELECT count(*) FROM pg_indexes WHERE tablename = '{table}_slots' \
               AND (indexdef LIKE '%guest_tenant%' OR indexdef LIKE '%served_at%')"
        )
        .await,
        0,
        "what a guest writes is in no index"
    );
}

/// X1: no history of the inbox's operations leaves a hold on a row out of
/// the queue, nor a cause on a processed row: seeded random histories of
/// publishes, some depending on another stream, dispatches of every
/// verdict, unparks, resolves, bulk unparks and expiries; every operation
/// must succeed — the baseline's CHECKs refuse a statement that forgets
/// (23514) — and after each, a row's hold fields are set together, and its
/// count of holds only on an `Unavailable` hold. Red when an exit drops
/// the fragment.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn no_history_leaves_a_hold_on_a_row_out_of_the_queue() {
    for seed in [
        0x9E37_79B9_7F4A_7C15_u64,
        0xD1B5_4A32_D192_ED03,
        0x2545_F491_4F6C_DD1D,
    ] {
        let f = fixture_waiting(
            &format!("history_{:x}", seed & 0xffff),
            Retries::up_to(3),
            Duration::from_millis(300),
        )
        .await;
        let mut state = seed;
        let mut next = move |n: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % n
        };
        let mut positions: HashMap<String, i32> = HashMap::new();
        let mut last: Option<InboxMessage> = None;
        let mut met = std::collections::BTreeSet::new();
        for step in 0..200 {
            let op = next(10);
            match op {
                0..=2 => {
                    let tenant = format!("t{}", next(3));
                    let stream = format!("{tenant}-{}", next(3));
                    let position = positions.entry(stream.clone()).or_insert(0);
                    *position += 1;
                    let mut message = message_of(&tenant, &stream, *position);
                    if let Some(cause) = last.as_ref().filter(|_| next(4) == 0) {
                        if cause.stream_id != message.stream_id {
                            message = message.depending_on(&[CausalDependency::on(cause)]);
                        }
                    } else if step % 5 == 1 {
                        // a dependency that never arrives, the expiry's;
                        // by the step, so that the seed's draws stay its own
                        let hole = message_of(&tenant, "hole", 1);
                        message = message.depending_on(&[CausalDependency::on(&hole)]);
                    }
                    f.publish(std::slice::from_ref(&message)).await;
                    last = Some(message);
                }
                3..=6 => {
                    let verdict = next(5);
                    let outcome = f
                        .dispatch(move || match verdict {
                            0 => Ok(()),
                            1 => Err(Transient::because(DB_TIMEOUT, "a moment").into()),
                            2 => Err(Permanent::because(KMS_GONE, "no key").into()),
                            3 => stage_down(None),
                            _ => tenant_down(None),
                        })
                        .await;
                    met.insert(match outcome {
                        Outcome::Failed { held: true, .. } => "held",
                        Outcome::Failed { parked: true, .. } => "parked",
                        Outcome::Failed { .. } => "spent",
                        Outcome::Processed => "processed",
                        Outcome::SetAside => "set aside",
                        Outcome::Held => "held unasked",
                        _ => "other",
                    });
                }
                7 => f.due().await,
                8 => {
                    let parked = f
                        .count("SELECT coalesce(min(received_position), 0) FROM {table} WHERE parked_at IS NOT NULL")
                        .await;
                    if parked > 0 {
                        let resolve = next(2) == 0;
                        f.sessions
                            .session(async |session| {
                                if resolve {
                                    f.inbox.resolve(&session, parked).await
                                } else {
                                    f.inbox.unpark(&session, parked).await
                                }
                            })
                            .await
                            .unwrap_or_else(|error| panic!("{seed:x} step {step}: {error}"));
                    }
                }
                _ => {
                    if next(2) == 0 {
                        f.unpark_all(&Unparking {
                            ground: ParkedFor::Failure(ParkedUnder::Cause(if next(2) == 0 {
                                "kms.gone"
                            } else {
                                "db.timeout"
                            })),
                            tries: 1,
                            ..refused_within_an_hour()
                        })
                        .await;
                    } else {
                        f.execute(
                            "UPDATE {table} SET waiting_since = now() - interval '1 hour' \
                                   WHERE waiting_since IS NOT NULL",
                        )
                        .await;
                        loop {
                            let expiry =
                                f.inbox.expire().await.unwrap_or_else(|error| {
                                    panic!("{seed:x} step {step}: {error}")
                                });
                            if expiry.parked > 0 {
                                met.insert("expired");
                            }
                            if !expiry.more {
                                break;
                            }
                        }
                    }
                }
            }
            let torn = f
                .count(
                    "SELECT count(*) FROM {table} \
                     WHERE (held_since IS NULL) <> (hold_kind IS NULL) \
                        OR (hold_kind IS NULL) <> (next_attempt_at IS NULL) \
                        OR (holds IS NOT NULL AND hold_kind IS DISTINCT FROM 'unavailable') \
                        OR (cause IS NOT NULL AND processed_position IS NOT NULL)",
                )
                .await;
            assert_eq!(torn, 0, "{seed:x}: a row torn at step {step} (op {op})");
        }
        for kind in [
            "held",
            "parked",
            "spent",
            "processed",
            "set aside",
            "expired",
        ] {
            assert!(
                met.contains(kind),
                "{seed:x}: a history without a row {kind}: {met:?}"
            );
        }
    }
}

const REFUSED: Cause = cause!("kms.refused");

/// The re-arm of a refused context's rows, of a channel, parked within the
/// last hour: a canary of one failure, every Tenant.
fn refused_within_an_hour<'a>() -> Unparking<'a> {
    Unparking {
        channel: "kafka://orders",
        ground: ParkedFor::Failure(ParkedUnder::Cause("kms.refused")),
        tenant: None,
        within: Duration::from_secs(3600),
        tries: 1,
        limit: 1000,
        overtaken: false,
    }
}

/// The operator re-arms the parked rows of a cause — of a Tenant, when
/// named; never another cause's, nor a shredded Tenant's — and those parked
/// before causes were recorded only when asked for them. A row re-armed with
/// a canary of `tries` failures parks again at its `tries`th, however many
/// attempts it had spent: red when the re-arm keeps a higher count.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn an_operator_re_arms_the_rows_parked_for_a_cause_and_a_canary_parks_again() {
    let f = fixture("unparkall", Retries::up_to(5)).await;
    f.publish(&[
        message("a", 1),
        message("b", 1),
        message("c", 1),
        message_of("tenant-2", "d", 1),
        message("x", 1),
        message("u", 1),
    ])
    .await;
    // b@1's key is gone, every other's refused; the walk serves tenant-1
    // and tenant-2 in turn, so each verdict is said by its stream
    let refused = |message: &InboxMessage| {
        let cause = if message.stream_id == json!({ "id": "b" }) {
            KMS_GONE
        } else {
            REFUSED
        };
        Err(Permanent::because(cause, "refused").into())
    };
    for _ in 0..6 {
        assert_eq!(f.dispatch_on(refused).await, spent(1, true), "each parks");
    }
    for stream in ["a", "b", "c", "d", "x", "u"] {
        assert!(f.row(stream).await.parked, "{stream}@1 parked");
    }
    // c@1 as parked before causes were recorded, at its last attempt; x@1
    // as parked for a dependency that never arrived, u@1 for its own values:
    // other grounds, which a bulk unpark never re-arms
    f.execute(
        "UPDATE {table} SET cause = NULL WHERE stream_id->>'id' IN ('c', 'x', 'u'); \
         UPDATE {table} SET attempts = 5 WHERE stream_id->>'id' = 'c'; \
         UPDATE {table} SET waiting_for = '{\"tenant_id\": \"tenant-1\"}' WHERE stream_id->>'id' = 'x'; \
         UPDATE {table} SET unreadable_at = now() WHERE stream_id->>'id' = 'u'",
    )
    .await;

    let tenant_2 = f
        .unpark_all(&Unparking {
            tenant: Some("tenant-2"),
            ..refused_within_an_hour()
        })
        .await;
    assert_eq!(tenant_2.unparked, 1, "d@1 only");
    assert!(!f.row("d").await.parked);
    let all = f.unpark_all(&refused_within_an_hour()).await;
    assert_eq!(
        all.unparked, 1,
        "a@1; never b@1, whose Tenant's key is gone"
    );
    assert!(!all.as_of.is_empty());
    assert!(f.row("b").await.parked && f.row("c").await.parked);
    let unrecorded = f
        .unpark_all(&Unparking {
            ground: ParkedFor::Failure(ParkedUnder::Unrecorded),
            tries: 2,
            ..refused_within_an_hour()
        })
        .await;
    assert_eq!(
        unrecorded.unparked, 1,
        "c@1, asked for by name; never x@1 nor u@1, of other grounds"
    );
    let a = f.row("a").await;
    assert_eq!(
        (a.attempts, a.cause.as_deref()),
        (4, None),
        "a canary of one: one failure left of five"
    );

    // a@1 and c@1 fail, d@1 goes on: the walk serves tenant-1 and tenant-2
    // in turn, a@1 before c@1 within tenant-1, so each verdict is said by
    // the stream and the outcomes are counted, not ordered
    let fails_but_d = |message: &InboxMessage| {
        if message.stream_id == json!({ "id": "d" }) {
            Ok(())
        } else {
            Err(Transient::because(DB_TIMEOUT, "still").into())
        }
    };
    let mut outcomes = Vec::new();
    for _ in 0..4 {
        outcomes.push(format!("{:?}", f.dispatch_on(fails_but_d).await));
        f.due().await;
    }
    outcomes.sort();
    let mut expected = [
        format!("{:?}", spent(5, true)),
        format!("{:?}", spent(4, false)),
        format!("{:?}", spent(5, true)),
        format!("{:?}", Outcome::Processed),
    ];
    expected.sort();
    assert_eq!(
        outcomes, expected,
        "a@1 parks at its first failure, c@1 at its second, d@1 goes on"
    );
    let (a, c) = (f.row("a").await, f.row("c").await);
    assert_eq!(
        (a.parked, a.attempts, c.parked, c.attempts),
        (true, 5, true, 5)
    );
    assert!(f.row("d").await.processed, "d@1, re-armed, goes on");
    assert_eq!(
        f.operated()
            .iter()
            .filter(|(principal, operation, _)| principal == "ops"
                && *operation == Operation::UnparkedAll)
            .count(),
        3,
        "each call told with its principal"
    );
}

/// The window of a bulk unpark ends at the call and reaches back `within`:
/// a row parked earlier, or stamped after the call, is left; a row overtaken
/// by its stream stays parked, counted, unless asked for; and a window
/// longer than the channel keeps its processed rows is refused, unless the
/// overtaken are asked for too.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_bulk_unpark_keeps_to_its_window_and_leaves_an_overtaken_row_unless_asked() {
    let f = fixture("unparkwindow", Retries::up_to(5)).await;
    f.publish(&[
        message("old", 1),
        message("new", 1),
        message("e", 1),
        message("e", 2),
        message("late", 1),
    ])
    .await;
    let refuse = || Err(Permanent::because(REFUSED, "refused").into());
    assert_eq!(f.dispatch(refuse).await, spent(1, true));
    assert_eq!(f.dispatch(refuse).await, spent(1, true));
    assert_eq!(f.dispatch(refuse).await, spent(1, true), "e@1 parks");
    assert_eq!(f.dispatch(ok).await, Outcome::Processed, "e@2 overtakes it");
    assert_eq!(f.dispatch(refuse).await, spent(1, true));
    f.execute(
        "UPDATE {table} SET parked_at = now() - interval '2 hours' WHERE stream_id->>'id' = 'old'",
    )
    .await;
    f.execute(
        "UPDATE {table} SET parked_at = now() + interval '1 hour' WHERE stream_id->>'id' = 'late'",
    )
    .await;

    let first = f.unpark_all(&refused_within_an_hour()).await;
    assert_eq!(
        (first.unparked, first.overtaken),
        (1, 1),
        "new@1 re-armed; e@1 overtaken, left; old@1 and late@1 outside the window"
    );
    assert!(f.row("old").await.parked && f.row("late").await.parked);
    let overtaken = f
        .unpark_all(&Unparking {
            overtaken: true,
            ..refused_within_an_hour()
        })
        .await;
    assert_eq!(overtaken.unparked, 1, "e@1, asked for");

    let kept = PgInbox::new(PgSessionPool::new(pool()))
        .with_table(
            Identifier::new(&f.table).unwrap(),
            Identifier::new(format!("{}_seq", f.table)).unwrap(),
        )
        .with_retention(
            InboxRetention::keep_everything().window("kafka://orders", Duration::from_secs(1800)),
        );
    assert!(matches!(
        kept.unpark_all("ops", &refused_within_an_hour()).await,
        Err(Error::Refused(_))
    ));
    // asked to re-arm the overtaken too, it needs no overtaker and takes a
    // window past the retention: a park older than it has a way back in bulk
    let past = kept
        .unpark_all(
            "ops",
            &Unparking {
                overtaken: true,
                within: Duration::from_secs(3 * 3600),
                ..refused_within_an_hour()
            },
        )
        .await
        .unwrap();
    assert_eq!(past.unparked, 1, "old@1, parked two hours ago");
    assert!(!f.row("old").await.parked && f.row("late").await.parked);
}

/// A bulk unpark goes batch after batch to its limit, and no further.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_bulk_unpark_goes_batch_after_batch_to_its_limit() {
    let f = fixture("unparkbatches", Retries::up_to(5)).await;
    let messages: Vec<InboxMessage> = (0..250).map(|i| message(&format!("s{i}"), 1)).collect();
    f.publish(&messages).await;
    f.execute("UPDATE {table} SET parked_at = now(), cause = 'kms.refused', attempts = 1")
        .await;
    let limited = f
        .unpark_all(&Unparking {
            limit: 120,
            ..refused_within_an_hour()
        })
        .await;
    assert_eq!(limited.unparked, 120, "to its limit");
    let rest = f.unpark_all(&refused_within_an_hour()).await;
    assert_eq!(rest.unparked, 130, "past one batch");
    assert_eq!(
        f.count("SELECT count(*) FROM {table} WHERE parked_at IS NOT NULL")
            .await,
        0
    );
}

/// A plain error is of the moment for no named cause, and a bus verdict
/// wrapped in another error is read by its top only.
#[test]
fn a_verdict_is_read_off_the_error_s_top() {
    let plain = Failure::from("down");
    assert_eq!(plain.cause(), Cause::UNEXPLAINED);
    assert!(!plain.is_permanent() && !plain.holds());
    let stage: Failure = Unavailable::new(KMS_UNAVAILABLE, Scope::Stage, None, "x").into();
    assert!(stage.holds());
    let tenant: Failure = Unavailable::new(KMS_THROTTLED, Scope::Tenant, None, "x").into();
    assert!(!tenant.holds() && !tenant.is_permanent());
    let permanent: Failure = Permanent::because(KMS_GONE, "x").into();
    assert_eq!(
        (permanent.cause(), permanent.is_permanent()),
        (KMS_GONE, true)
    );
    #[derive(Debug)]
    struct Wrapped(Permanent);
    impl std::fmt::Display for Wrapped {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "wrapped")
        }
    }
    impl std::error::Error for Wrapped {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }
    let wrapped = Failure::from(Wrapped(Permanent::because(KMS_GONE, "x")));
    assert!(!wrapped.is_permanent(), "never read off its sources");
}

const GIVEN_UP: Cause = cause!("operator.given_up");

/// A slot that sleeps an hour once its heads are all held: what a verb
/// wakes, the tests not waiting it out.
fn asleep_for_an_hour(f: Fixture) -> Fixture {
    Fixture {
        inbox: f.inbox.with_rotation(Rotation {
            walk: 32,
            max_dormancy: Duration::from_secs(3600),
        }),
        ..f
    }
}

/// Holds the heads of `streams` (their first rows) for `cause` as a verdict
/// would: an `Unavailable` counted twice, or a back-off for `db.timeout`.
async fn hold(f: &Fixture, cause: &str, streams: &[&str]) {
    let kind = if cause == "db.timeout" {
        "backoff"
    } else {
        "unavailable"
    };
    let holds = if kind == "backoff" { "NULL" } else { "2" };
    let streams = streams
        .iter()
        .map(|stream| format!("'{stream}'"))
        .collect::<Vec<_>>()
        .join(", ");
    f.execute(&format!(
        "UPDATE {{table}} SET held_since = now(), next_attempt_at = now() + interval '1 hour', \
             hold_kind = '{kind}', holds = {holds}, cause = '{cause}' \
         WHERE stream_id->>'id' IN ({streams}) AND stream_position = 1"
    ))
    .await;
}

/// The streams the dispatches processed, in their order.
#[derive(Default)]
struct Seen(Mutex<Vec<String>>);

impl Seen {
    fn ok(&self, message: &InboxMessage) -> Result<(), Failure> {
        self.0
            .lock()
            .unwrap()
            .push(message.stream_id["id"].as_str().unwrap_or("?").to_owned());
        Ok(())
    }

    fn all(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
}

/// V1: the held heads are paged by their place, oldest first, each with its
/// hold, its cause, its count of holds and its identity; a read told to the
/// observer as the principal's (ADR-0042 decision 8).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_held_heads_are_paged_with_their_hold_and_identity() {
    let f = fixture("held_page", Retries::up_to(3)).await;
    f.publish(&[
        message_of("t1", "a", 1),
        message_of("t2", "b", 1),
        message_of("t3", "c", 1),
        message_of("t3", "c", 2),
    ])
    .await;
    hold(&f, "kms.unavailable", &["a", "c"]).await;
    hold(&f, "db.timeout", &["b"]).await;
    let first = f.inbox.held("ops", None, 2).await.unwrap();
    let rest = f
        .inbox
        .held("ops", Some(first[1].received_position), 2)
        .await
        .unwrap();
    let page: Vec<_> = first
        .iter()
        .chain(&rest)
        .map(|head| {
            (
                head.identity
                    .as_ref()
                    .map(|identity| identity.stream_id["id"].as_str().unwrap().to_owned()),
                head.hold,
                head.cause.clone(),
                head.holds,
                head.until.is_some(),
            )
        })
        .collect();
    let some = |text: &str| Some(text.to_owned());
    assert_eq!(
        page,
        [
            (
                some("a"),
                Hold::Unavailable,
                some("kms.unavailable"),
                Some(2),
                true
            ),
            (some("b"), Hold::Backoff, some("db.timeout"), None, true),
            (
                some("c"),
                Hold::Unavailable,
                some("kms.unavailable"),
                Some(2),
                true
            ),
        ]
    );
    assert_eq!(
        f.operated(),
        [
            ("ops".to_owned(), Operation::PagedHeld, 2),
            ("ops".to_owned(), Operation::PagedHeld, 1)
        ]
    );
}

/// V1: the heads held under a cause are given up, a batch at a time up to
/// the limit, of one Tenant when named; a head held under another cause
/// stays; the rows behind them go on at once, their slot woken from a sleep
/// of an hour; and they are re-armed by their cause (ADR-0042 decision 8).
/// Red when the verb parks another cause's heads, or wakes no slot.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_heads_held_under_a_cause_are_given_up_and_the_rows_behind_them_go_on() {
    let f = asleep_for_an_hour(fixture("park_heads", Retries::up_to(3)).await);
    f.publish(&[
        message_of("t1", "a", 1),
        message_of("t1", "x", 1),
        message_of("t2", "b", 1),
        message_of("t3", "c", 1),
    ])
    .await;
    hold(&f, "kms.unavailable", &["a", "b"]).await;
    hold(&f, "db.timeout", &["c"]).await;
    let seen = Seen::default();
    assert_eq!(f.dispatch_on(|m| seen.ok(m)).await, Outcome::Nothing);
    assert_eq!(
        f.text("SELECT (dormant_until > now() + interval '30 minutes')::text FROM {table}_slots")
            .await,
        "true",
        "the slot sleeps"
    );
    let heads = |limit, tenant| ParkingHeads {
        channel: "kafka://orders",
        cause: "kms.unavailable",
        tenant,
        limit,
    };
    let given = f.inbox.park_heads("ops", &heads(1, None)).await.unwrap();
    assert_eq!(
        given,
        GivenUp {
            parked: 1,
            passed: 0,
            more: true
        }
    );
    let given = f.inbox.park_heads("ops", &heads(100, None)).await.unwrap();
    assert_eq!(
        given,
        GivenUp {
            parked: 1,
            passed: 0,
            more: false
        }
    );
    assert!(f.row("a").await.parked && f.row("b").await.parked);
    assert_eq!(f.row("a").await.cause.as_deref(), Some("kms.unavailable"));
    let c = f.row("c").await;
    assert!(c.held && !c.parked, "another cause's head stays held");
    assert_eq!(
        f.dispatch_on(|m| seen.ok(m)).await,
        Outcome::Processed,
        "the row behind a@1 at once, its slot woken"
    );
    assert_eq!(seen.all(), ["x"]);
    let tenant_of = |tenant| ParkingHeads {
        cause: "db.timeout",
        ..heads(100, Some(tenant))
    };
    assert_eq!(
        f.inbox
            .park_heads("ops", &tenant_of("t9"))
            .await
            .unwrap()
            .parked,
        0
    );
    assert_eq!(
        f.inbox
            .park_heads("ops", &tenant_of("t3"))
            .await
            .unwrap()
            .parked,
        1
    );
    let unparked = f
        .unpark_all(&Unparking {
            ground: ParkedFor::Failure(ParkedUnder::Cause("kms.unavailable")),
            tries: 1,
            ..refused_within_an_hour()
        })
        .await;
    assert_eq!((unparked.unparked, unparked.more), (2, false));
    let operations: Vec<_> = f
        .operated()
        .into_iter()
        .filter(|(_, operation, _)| *operation == Operation::ParkedHeads)
        .map(|(principal, _, rows)| (principal, rows))
        .collect();
    assert_eq!(
        operations,
        [1, 1, 0, 1].map(|rows| ("ops".to_owned(), rows))
    );
    let named: Vec<_> = f.told.4.lock().unwrap().clone();
    assert!(
        named.contains(&(
            Operation::ParkedHeads,
            Some("t3".to_owned()),
            Some("db.timeout".to_owned())
        )),
        "the audit names the Tenant and the cause: {named:?}"
    );
}

/// V1: a (channel, Tenant)'s queue given up while a dispatcher serves it
/// ends a prefix processed and a suffix parked: every row one or the other,
/// each processed row older than every parked one, so that the re-arm
/// overtakes nothing (ADR-0042 decision 8). A dispatch between two batches
/// takes the Tenant's head, as a dispatcher does between two statements of
/// one call. Red when the verb parks the oldest first: the dispatch then
/// takes the row behind the first batch.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_queue_given_up_while_it_is_served_ends_a_prefix_processed_and_a_suffix_parked() {
    let f = fixture("park_served", Retries::up_to(3)).await;
    for n in 1..=300 {
        f.publish(&[message_of("tx", "s", n)]).await;
    }
    let queue = |limit| ParkingQueue {
        channel: "kafka://orders",
        tenant: "tx",
        cause: GIVEN_UP,
        limit,
    };
    assert_eq!(
        f.inbox.park_queue("ops", &queue(100)).await.unwrap().parked,
        100
    );
    assert_eq!(
        f.dispatch(ok).await,
        Outcome::Processed,
        "between two batches"
    );
    f.inbox.park_queue("ops", &queue(1000)).await.unwrap();
    let positions = |state: &str| {
        let sql = format!(
            "SELECT coalesce(array_agg(stream_position ORDER BY stream_position), '{{}}') \
             FROM {} WHERE {state}",
            f.table
        );
        let sessions = &f.sessions;
        async move {
            sessions
                .session(async |session| {
                    Ok::<Vec<i32>, Error>(session.connection().query_one(&sql, &[]).await?.get(0))
                })
                .await
                .unwrap()
        }
    };
    let processed = positions("processed_position IS NOT NULL").await;
    let parked = positions("parked_at IS NOT NULL").await;
    assert_eq!(
        processed.len() + parked.len(),
        300,
        "every row one or the other"
    );
    assert!(!parked.is_empty(), "the verb parked");
    if let (Some(last), Some(first)) = (processed.last(), parked.first()) {
        assert!(
            last < first,
            "a processed row behind a parked one: {last} after {first}"
        );
    }
}

/// V1: a head a dispatcher holds is passed by and counted, and the verb
/// waits for nothing — not for that head's row, not for the slot its holder
/// holds: the rest of the Tenant's queue is given up while the holder's
/// subscriber runs (ADR-0009 decision 4 as ADR-0042 decision 8 amends it).
/// Red when a verb locks its rows or its slots otherwise than `SKIP LOCKED`.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_head_a_dispatcher_holds_is_passed_by_and_the_verb_waits_for_nothing() {
    let f = fixture("passed_by", Retries::up_to(3)).await;
    f.publish(&[
        message_of("tx", "x1", 1),
        message_of("tx", "x2", 1),
        message_of("tx", "x3", 1),
    ])
    .await;
    let (entered, release) = (tokio::sync::Notify::new(), tokio::sync::Notify::new());
    let holding = f.inbox.dispatch(|_: &PgSession, _: &InboxMessage| async {
        entered.notify_one();
        release.notified().await;
        Ok::<(), Failure>(())
    });
    let verb = async {
        entered.notified().await;
        let given = tokio::time::timeout(
            Duration::from_secs(5),
            f.inbox.park_queue(
                "ops",
                &ParkingQueue {
                    channel: "kafka://orders",
                    tenant: "tx",
                    cause: GIVEN_UP,
                    limit: 100,
                },
            ),
        )
        .await
        .expect("the verb waits for nothing")
        .unwrap();
        release.notify_one();
        given
    };
    let (outcome, given) = tokio::join!(holding, verb);
    assert_eq!(outcome.unwrap(), Outcome::Processed, "x1, by its holder");
    assert_eq!(
        given,
        GivenUp {
            parked: 2,
            passed: 1,
            more: false
        }
    );
    assert!(f.row("x2").await.parked && f.row("x3").await.parked);
}

/// V1: a walk that waits on a verb's lock of its head reads the head, once
/// the verb commits, as vanished, and the next dispatch takes the Tenant's
/// next row (ADR-0009 decision 4 as ADR-0042 decision 8 amends it). The
/// verb's statement is a transaction of the test's, held open until the
/// walk waits on it.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_walk_that_waits_on_a_verb_s_lock_reads_its_head_vanished() {
    let f = fixture("vanished", Retries::up_to(3)).await;
    f.publish(&[message_of("tx", "x1", 1), message_of("tx", "x2", 1)])
        .await;
    let verb = pool().get().await.unwrap();
    verb.batch_execute(&format!(
        "BEGIN; UPDATE {} SET parked_at = now(), cause = 'operator.given_up' \
         WHERE stream_id->>'id' = 'x1'",
        f.table
    ))
    .await
    .unwrap();
    let seen = Seen::default();
    let walk = f.dispatch_on(|m| seen.ok(m));
    let commit = async {
        let started = std::time::Instant::now();
        loop {
            let waiting: i64 = verb
                .query_one(
                    "SELECT count(*) FROM pg_stat_activity \
                     WHERE wait_event_type = 'Lock' AND datname = current_database()",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            if waiting > 0 {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "the walk never waited"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        verb.batch_execute("COMMIT").await.unwrap();
    };
    let (outcome, ()) = tokio::join!(walk, commit);
    assert_eq!(outcome, Outcome::Nothing, "the head vanished");
    assert!(seen.all().is_empty());
    assert_eq!(f.dispatch_on(|m| seen.ok(m)).await, Outcome::Processed);
    assert_eq!(seen.all(), ["x2"]);
}

/// V1: a (channel, Tenant)'s queue is given up whole — held or not, the
/// limit taking the newest first, so that what is parked is the end of each
/// stream — another Tenant's and another channel's rows untouched; re-armed
/// by its cause, it is processed in its order (ADR-0042 decision 8). Red
/// when the verb takes the oldest first.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_queue_given_up_whole_is_re_armed_in_its_order() {
    let f = fixture("park_queue", Retries::up_to(3)).await;
    for n in 1..=5 {
        f.publish(&[message_of("tx", &format!("x{n}"), 1)]).await;
    }
    f.publish(&[message_of("ty", "y1", 1), message_on("pay", "tx", "p1", 1)])
        .await;
    hold(&f, "kms.unavailable", &["x1"]).await;
    let queue = |limit| ParkingQueue {
        channel: "kafka://orders",
        tenant: "tx",
        cause: GIVEN_UP,
        limit,
    };
    assert_eq!(
        f.inbox.park_queue("ops", &queue(2)).await.unwrap(),
        GivenUp {
            parked: 2,
            passed: 0,
            more: true
        }
    );
    assert!(f.row("x5").await.parked && f.row("x4").await.parked);
    assert!(!f.row("x3").await.parked, "the newest first");
    assert_eq!(
        f.inbox.park_queue("ops", &queue(100)).await.unwrap().parked,
        3
    );
    assert!(!f.row("y1").await.parked && !f.row("p1").await.parked);
    assert_eq!(
        f.row("x1").await.cause.as_deref(),
        Some("operator.given_up")
    );
    assert!(!f.row("x1").await.held, "no hold on a parked row");
    // across slots, the limit takes the Tenant's oldest of every slot's
    let cut = fixture_cut("park_queue_cut", Retries::up_to(3), 4, quick()).await;
    for n in 1..=8 {
        cut.publish(&[message_of("tx", &format!("x{n}"), 1)]).await;
    }
    assert_eq!(
        cut.inbox.park_queue("ops", &queue(3)).await.unwrap().parked,
        3
    );
    let mut parked = Vec::new();
    for n in 1..=8 {
        if cut.row(&format!("x{n}")).await.parked {
            parked.push(n);
        }
    }
    assert_eq!(parked, [6, 7, 8], "the newest first, whatever their slots");

    let unparked = f
        .unpark_all(&Unparking {
            ground: ParkedFor::Failure(ParkedUnder::Cause("operator.given_up")),
            tries: 1,
            ..refused_within_an_hour()
        })
        .await;
    assert_eq!(unparked.unparked, 5);
    let seen = Seen::default();
    while f.dispatch_on(|m| seen.ok(m)).await != Outcome::Nothing {}
    let xs: Vec<String> = seen
        .all()
        .into_iter()
        .filter(|stream| stream.starts_with('x'))
        .collect();
    assert_eq!(xs, ["x1", "x2", "x3", "x4", "x5"]);
}

/// V1: a canary of more tries than `max_attempts` re-arms a row to none
/// spent, never fewer: it parks again at `max_attempts` failures (ADR-0042
/// decision 8). Red when the attempts set may fall below none.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_canary_of_more_tries_than_attempts_parks_again_at_the_attempts() {
    let f = fixture("canary_above", Retries::up_to(5)).await;
    f.publish(&[message("r", 1)]).await;
    f.execute(
        "UPDATE {table} SET parked_at = now(), attempts = 5, cause = 'db.timeout' \
         WHERE stream_id->>'id' = 'r'",
    )
    .await;
    let unparked = f
        .unpark_all(&Unparking {
            ground: ParkedFor::Failure(ParkedUnder::Cause("db.timeout")),
            tries: 10,
            ..refused_within_an_hour()
        })
        .await;
    assert_eq!(unparked.unparked, 1);
    assert_eq!(f.row("r").await.attempts, 0);
    let mut failures = 0;
    while !f.row("r").await.parked {
        assert!(failures < 20, "never parked again");
        f.dispatch(|| Err(Transient::because(DB_TIMEOUT, "a moment").into()))
            .await;
        f.due().await;
        failures += 1;
    }
    assert_eq!(failures, 5);
}

/// V1: a bulk unpark re-arms the rows of the ground it names, each by its
/// remedy: a failure's tried again; an expired one looks for its dependency
/// again and waits for it anew; one parked at once is read again — and no
/// other ground's (ADR-0042 decision 8). Red when the ground is not
/// selected.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_bulk_unpark_re_arms_the_ground_it_names_by_its_remedy() {
    let f = fixture_waiting("grounds", Retries::up_to(3), Duration::from_secs(60)).await;
    let never = message("never", 1);
    f.publish(&[message("e", 1).depending_on(&[CausalDependency::on(&never)])])
        .await;
    assert_eq!(f.dispatch(ok).await, Outcome::SetAside);
    f.execute(
        "UPDATE {table} SET waiting_since = now() - interval '1 hour' \
         WHERE waiting_since IS NOT NULL",
    )
    .await;
    assert_eq!(f.inbox.expire().await.unwrap().parked, 1);
    f.publish(&[message("u", 1), message("g", 1)]).await;
    f.execute(
        "UPDATE {table} SET parked_at = now(), unreadable_at = now() \
             WHERE stream_id->>'id' = 'u'; \
         UPDATE {table} SET parked_at = now(), attempts = 3, cause = 'db.timeout' \
             WHERE stream_id->>'id' = 'g'",
    )
    .await;
    let by = |ground| Unparking {
        ground,
        ..refused_within_an_hour()
    };
    let failure = ParkedFor::Failure(ParkedUnder::Cause("db.timeout"));
    assert_eq!(f.unpark_all(&by(failure)).await.unparked, 1);
    assert!(f.row("e").await.parked && f.row("u").await.parked);
    assert!(!f.row("g").await.parked);
    assert_eq!(f.unpark_all(&by(ParkedFor::Expired)).await.unparked, 1);
    assert!(f.row("u").await.parked);
    assert_eq!(f.unpark_all(&by(ParkedFor::AtOnce)).await.unparked, 1);
    let seen = Seen::default();
    let mut outcomes = Vec::new();
    loop {
        let outcome = f.dispatch_on(|m| seen.ok(m)).await;
        if outcome == Outcome::Nothing {
            break;
        }
        outcomes.push(outcome);
    }
    let mut processed = seen.all();
    processed.sort();
    assert_eq!(
        processed,
        ["g", "u"],
        "the failure tried, the unreadable read"
    );
    assert!(outcomes.contains(&Outcome::SetAside), "e waits again");
    assert_eq!(
        f.count("SELECT count(*) FROM {table} WHERE stream_id->>'id' = 'e' AND waiting_since IS NOT NULL")
            .await,
        1
    );
}

/// § 6: the rows waiting for a dependency are counted by channel, with the
/// oldest wait; a row the expiry parked waits no more and is not counted
/// (ADR-0042 decision 6).
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn the_rows_waiting_are_counted_by_channel() {
    let f = fixture("waiting_counts", Retries::up_to(3)).await;
    let never = message("never", 1);
    for (topic, stream) in [
        ("orders", "w1"),
        ("orders", "w2"),
        ("pay", "w3"),
        ("pay", "w4"),
    ] {
        f.publish(&[
            message_on(topic, "tenant-1", stream, 1).depending_on(&[CausalDependency::on(&never)])
        ])
        .await;
        assert_eq!(f.dispatch(ok).await, Outcome::SetAside);
    }
    f.execute(
        "UPDATE {table} SET parked_at = now(), waiting_since = NULL \
         WHERE stream_id->>'id' = 'w4'",
    )
    .await;
    let counts: Vec<(String, u64)> = f
        .inbox
        .waiting_counts(Duration::from_secs(5))
        .await
        .unwrap()
        .into_iter()
        .map(
            |WaitingCount {
                 channel,
                 waiting,
                 oldest,
             }| {
                assert!(!oldest.is_empty());
                (channel, waiting)
            },
        )
        .collect();
    assert_eq!(
        counts,
        [
            ("kafka://orders".to_owned(), 2),
            ("kafka://pay".to_owned(), 1)
        ]
    );
}

/// § 6, C7: a channel this process finds stalled is reported — one row a
/// (channel, cause, process), the process's own label, valid ahead of the
/// database's clock and refreshed while the stall lasts; resumed, its row
/// is refreshed no more and lapses within its ttl; paused again while the
/// row holds, it counts another opening and keeps its start (ADR-0042
/// decision 6). Red when a refresh resets the start, or an opening is not
/// counted, or a resumed channel's row is refreshed.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_stall_is_reported_while_it_lasts_and_lapses_after() {
    let f = fixture("stalls", Retries::up_to(3)).await;
    for tenant in ["t1", "t2", "t3"] {
        f.publish(&[message_of(tenant, &format!("{tenant}-a"), 1)])
            .await;
    }
    // stalled the nth time: paused, two ticks' trials failed, and a stall
    // told after that pause, which the next dispatch reports
    let stall_down = async |nth: usize| {
        let started = std::time::Instant::now();
        loop {
            f.dispatch(|| stage_down(None)).await;
            f.due().await;
            let effects = f.effects();
            let paused = effects
                .iter()
                .enumerate()
                .filter(|(_, effect)| matches!(effect, Effect::Paused { .. }))
                .nth(nth - 1)
                .map(|(at, _)| at);
            if let Some(at) = paused {
                if effects[at..]
                    .iter()
                    .any(|effect| matches!(effect, Effect::Stalled { .. }))
                {
                    f.dispatch(|| stage_down(None)).await;
                    return f.inbox.stalls(Duration::from_secs(5)).await.unwrap();
                }
            }
            assert!(started.elapsed() < Duration::from_secs(20), "never stalled");
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    };
    let stalls = stall_down(1).await;
    assert_eq!(stalls.len(), 1, "{stalls:?}");
    let first = stalls[0].clone();
    assert_eq!(
        (first.channel.as_str(), first.cause.as_str(), first.openings),
        ("kafka://orders", "kms.unavailable", 1)
    );
    assert_eq!(first.reporter, f.inbox.reporter());
    // refreshed while it lasts, its start kept
    tokio::time::sleep(Duration::from_millis(300)).await;
    f.dispatch(|| stage_down(None)).await;
    let refreshed = f.inbox.stalls(Duration::from_secs(5)).await.unwrap();
    assert_eq!(refreshed[0].since, first.since);
    assert!(refreshed[0].valid_until >= first.valid_until);
    // resumed: a trial's processed row
    let resumed = std::time::Instant::now();
    loop {
        f.dispatch(ok).await;
        f.due().await;
        if f.effects()
            .iter()
            .any(|effect| matches!(effect, Effect::Resumed { .. }))
        {
            break;
        }
        assert!(resumed.elapsed() < Duration::from_secs(20), "never resumed");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    let held = f.inbox.stalls(Duration::from_secs(5)).await.unwrap();
    assert!(held[0].valid, "a resumed channel's row holds until its ttl");
    for tenant in ["t4", "t5", "t6"] {
        f.publish(&[message_of(tenant, &format!("{tenant}-a"), 1)])
            .await;
    }
    let again = stall_down(2).await;
    assert_eq!(again.len(), 1, "{again:?}");
    assert_eq!(again[0].since, first.since, "one stall across the gap");
    assert_eq!(again[0].openings, 2, "and two openings");
    // resumed, its row lapses within its ttl, refreshed by no dispatch
    loop {
        f.dispatch(ok).await;
        f.due().await;
        if f.effects()
            .iter()
            .filter(|effect| matches!(effect, Effect::Resumed { .. }))
            .count()
            == 2
        {
            break;
        }
        assert!(
            resumed.elapsed() < Duration::from_secs(40),
            "never resumed again"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    tokio::time::sleep(quick().stall_ttl() + Duration::from_millis(200)).await;
    f.dispatch(ok).await;
    let lapsed = f.inbox.stalls(Duration::from_secs(5)).await.unwrap();
    assert!(!lapsed[0].valid, "lapsed: {lapsed:?}");
}

/// § 6: the stall report is for people, never a gate — a report that cannot
/// be written, its table gone, leaves the dispatches to go on, the channel
/// paused and tried as ever (ADR-0042 decision 6). Red when a report's
/// failure fails its dispatch.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_report_that_cannot_be_written_fails_no_dispatch() {
    let f = fixture("report_fails", Retries::up_to(3)).await;
    for tenant in ["t1", "t2", "t3"] {
        f.publish(&[message_of(tenant, &format!("{tenant}-a"), 1)])
            .await;
    }
    f.execute("DROP TABLE {table}_stalls").await;
    let started = std::time::Instant::now();
    while !f
        .effects()
        .iter()
        .any(|effect| matches!(effect, Effect::Stalled { .. }))
    {
        assert!(started.elapsed() < Duration::from_secs(20), "never stalled");
        f.inbox
            .dispatch(|_: &PgSession, _: &InboxMessage| std::future::ready(stage_down(None)))
            .await
            .expect("a dispatch the report cannot fail");
        f.due().await;
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    for _ in 0..3 {
        f.inbox
            .dispatch(|_: &PgSession, _: &InboxMessage| std::future::ready(stage_down(None)))
            .await
            .expect("a dispatch the report cannot fail");
        f.due().await;
    }
}
