//! A message that cannot be handled now stops exactly the messages that
//! share its cause (ADR-0040): a stage's outage holds its head without
//! spending an attempt, weighed against the channel's evidence past its
//! budget; a Tenant's spends one, never sooner than its back-off; and a
//! cause lives as long as the state it explains. They need a live database:
//!
//! ```text
//! ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
//!     cargo test -p ascetic-ddd-inbox --test outage -- --ignored
//! ```

use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ascetic_ddd_bus::{Cause, Permanent, Scope, Transient, Unavailable, cause};
use ascetic_ddd_inbox::observer::{Failed, InboxObserver, Operated, Operation};
use ascetic_ddd_inbox::{
    ByStream, CausalDependency, Error, Failure, Inbox, InboxMessage, InboxRetention, Outage,
    Outcome, ParkedUnder, PgInbox, Retries, UnparkedAll, Unparking,
};
use ascetic_ddd_session::pg::Identifier;
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};
use ascetic_ddd_session::{PgAccess, PgSession, PgSessionPool, SessionPool};
use serde_json::json;

const DEFAULT_URL: &str = "postgresql://devel:devel@localhost:5432/devel_karmabot_test";

const KMS_UNAVAILABLE: Cause = cause!("kms.unavailable");
const KMS_THROTTLED: Cause = cause!("kms.throttled");
const KMS_GONE: Cause = cause!("kms.gone");
const DB_TIMEOUT: Cause = cause!("db.timeout");

fn pool() -> Pool {
    let url = std::env::var("ASCETIC_DDD_TEST_PG_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned());
    let manager = Manager::from_config(
        Config::from_str(&url).expect("a valid PostgreSQL URL"),
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

/// What the observer was told of each failure.
#[derive(Clone, Debug, PartialEq)]
struct Told {
    cause: String,
    attempts: u32,
    parked: bool,
    held: bool,
    past_budget: bool,
    retry_after: Duration,
}

#[derive(Default)]
struct Failures(Mutex<Vec<Told>>, Mutex<Vec<(String, Operation, u64)>>);

impl InboxObserver for Failures {
    fn on_operated(&self, event: &Operated<'_>) {
        self.1
            .lock()
            .unwrap()
            .push((event.principal.to_owned(), event.operation, event.rows));
    }

    fn on_failed(&self, event: &Failed<'_>) {
        self.0.lock().unwrap().push(Told {
            cause: event.cause.as_str().to_owned(),
            attempts: event.attempts,
            parked: event.parked,
            held: event.held,
            past_budget: event.past_budget,
            retry_after: event.retry_after,
        });
    }
}

struct Fixture {
    inbox: PgInbox<PgSessionPool, Arc<Failures>>,
    told: Arc<Failures>,
    sessions: PgSessionPool,
    table: String,
}

/// A table of its own, one slot, `retries` as given.
async fn fixture(name: &str, retries: Retries) -> Fixture {
    fixture_waiting(name, retries, Duration::from_secs(600)).await
}

/// The same, a row waiting for its dependency at most `max_wait`.
async fn fixture_waiting(name: &str, retries: Retries, max_wait: Duration) -> Fixture {
    let table = format!("outage_{name}");
    let told = Arc::new(Failures::default());
    let inbox = PgInbox::new(PgSessionPool::new(pool()))
        .with_table(
            Identifier::new(&table).unwrap(),
            Identifier::new(format!("{table}_seq")).unwrap(),
        )
        .with_slots(1)
        .partitioned_by(ByStream)
        .with_retries(retries)
        .with_max_wait(max_wait)
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

    /// The one row of stream `stream`: its attempts, its causes, whether it
    /// holds, parked or processed.
    async fn row(&self, stream: &str) -> Row {
        let sql = format!(
            "SELECT attempts, last_cause, parked_cause, unavailable_since IS NOT NULL, \
                    parked_at IS NOT NULL, processed_position IS NOT NULL, \
                    unavailable_since::text \
             FROM {} WHERE stream_id->>'id' = $1",
            self.table
        );
        self.sessions
            .session(async |session| {
                let row = session.connection().query_one(&sql, &[&stream]).await?;
                Ok::<_, Error>(Row {
                    attempts: row.get(0),
                    last_cause: row.get(1),
                    parked_cause: row.get(2),
                    holding: row.get(3),
                    parked: row.get(4),
                    processed: row.get(5),
                    since: row.get(6),
                })
            })
            .await
            .unwrap()
    }

    /// Makes every row due now, and wakes the slot: a round that found no
    /// head due sleeps until the earliest back-off it saw (ADR-0041 decision
    /// 2), and the rows' clock moved by hand moves the slot's too.
    async fn due(&self) {
        self.execute("UPDATE {table} SET next_attempt_at = now()")
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

    async fn park_held(&self, channel: &str, cause: &str, tenant: Option<&str>) -> u64 {
        self.inbox
            .park_held("ops", channel, cause, tenant)
            .await
            .unwrap()
    }

    fn last_told(&self) -> Told {
        self.told().last().cloned().expect("a failure told")
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
    last_cause: Option<String>,
    parked_cause: Option<String>,
    holding: bool,
    parked: bool,
    processed: bool,
    since: Option<String>,
}

fn message(stream: &str, position: i32) -> InboxMessage {
    message_of("tenant-1", stream, position)
}

fn message_of(tenant: &str, stream: &str, position: i32) -> InboxMessage {
    static EVENT: AtomicU64 = AtomicU64::new(1);
    let event = EVENT.fetch_add(1, Ordering::Relaxed);
    InboxMessage::new(
        tenant,
        "orders.Order",
        json!({ "id": stream }),
        position,
        format!("kafka://orders/{stream}"),
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

/// A stage's outage holds the head's slot and spends no attempt: the row
/// behind waits, the hold's clock starts once and is kept across holds, and
/// the message handled at last clears what the hold wrote.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_stage_outage_holds_its_slot_and_spends_no_attempt() {
    let f = fixture("holds", Retries::up_to(2)).await;
    f.publish(&[message("a", 1), message("b", 1)]).await;

    assert_eq!(
        f.dispatch(|| stage_down(Some(Duration::from_secs(5))))
            .await,
        held(0)
    );
    assert_eq!(
        f.dispatch(ok).await,
        Outcome::Nothing,
        "b@1 waits behind a@1"
    );
    let first = f.row("a").await;
    assert_eq!(
        (first.attempts, first.last_cause.as_deref(), first.holding),
        (0, Some("kms.unavailable"), true)
    );

    for _ in 0..3 {
        f.due().await;
        assert_eq!(
            f.dispatch(|| stage_down(None)).await,
            held(0),
            "never parks"
        );
    }
    let again = f.row("a").await;
    assert_eq!(again.attempts, 0, "no attempt spent");
    assert_eq!(again.since, first.since, "the hold's clock started once");

    f.due().await;
    assert_eq!(f.dispatch(ok).await, Outcome::Processed);
    let handled = f.row("a").await;
    assert_eq!(
        (handled.last_cause, handled.parked_cause, handled.holding),
        (None, None, false),
        "the mark clears what the failures wrote"
    );
    assert_eq!(f.dispatch(ok).await, Outcome::Processed, "b@1 goes on");
}

/// A Tenant's unavailability spends an attempt, never sooner than the
/// back-off, later when its teller says so; the last one parks it under its
/// cause.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_tenant_s_unavailability_spends_an_attempt_never_sooner_than_its_back_off() {
    let f = fixture(
        "tenant",
        Retries::up_to(3).with_backoff(Retries::exponential(
            Duration::from_secs(10),
            Duration::from_secs(300),
        )),
    )
    .await;
    f.publish(&[message("a", 1)]).await;

    assert_eq!(
        f.dispatch(|| tenant_down(Some(Duration::from_secs(3))))
            .await,
        spent(1, false)
    );
    assert_eq!(
        f.last_told().retry_after,
        Duration::from_secs(10),
        "the back-off, not the teller's sooner while"
    );
    f.due().await;
    assert_eq!(
        f.dispatch(|| tenant_down(Some(Duration::from_secs(45))))
            .await,
        spent(2, false)
    );
    assert_eq!(
        f.last_told().retry_after,
        Duration::from_secs(45),
        "the teller's later while, not the back-off's 20 s"
    );
    f.due().await;
    assert_eq!(f.dispatch(|| tenant_down(None)).await, spent(3, true));
    let parked = f.row("a").await;
    assert_eq!(
        (parked.parked_cause.as_deref(), parked.holding),
        (Some("kms.throttled"), false)
    );
}

/// A held message is asked again at the teller's while, else after as long
/// as it has held, never sooner than the first back-off nor later than the
/// cap.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_hold_is_asked_again_between_its_floor_and_its_cap() {
    let f = fixture(
        "asked",
        Retries::unlimited()
            .with_backoff(Retries::exponential(
                Duration::from_secs(2),
                Duration::from_secs(300),
            ))
            .with_outage(Outage {
                budget: Duration::from_secs(3600),
                cap: Duration::from_secs(60),
            }),
    )
    .await;
    f.publish(&[message("a", 1)]).await;

    f.dispatch(|| stage_down(Some(Duration::from_millis(200))))
        .await;
    assert_eq!(
        f.last_told().retry_after,
        Duration::from_secs(2),
        "the floor"
    );
    f.due().await;
    f.dispatch(|| stage_down(Some(Duration::from_secs(10_000))))
        .await;
    assert_eq!(
        f.last_told().retry_after,
        Duration::from_secs(60),
        "the cap"
    );
    f.due().await;
    f.dispatch(|| stage_down(Some(Duration::from_secs(7))))
        .await;
    assert_eq!(
        f.last_told().retry_after,
        Duration::from_secs(7),
        "the teller's"
    );
    f.execute("UPDATE {table} SET unavailable_since = unavailable_since - interval '30 seconds', next_attempt_at = now()")
        .await;
    f.dispatch(|| stage_down(None)).await;
    let waited = f.last_told().retry_after;
    assert!(
        waited >= Duration::from_secs(30) && waited < Duration::from_secs(32),
        "as long as it has held: {waited:?}"
    );
}

/// Past its budget a hold is weighed against the channel: while nothing of
/// the channel was processed within the cap it holds on, told past its
/// budget; once a row was, the outage is refuted, and every Stage verdict
/// after spends an attempt until the row parks under its cause.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_hold_past_its_budget_holds_on_unrefuted_and_spends_once_the_channel_refutes_it() {
    let f = fixture(
        "budget",
        Retries::up_to(2).with_outage(Outage {
            budget: Duration::from_secs(3600),
            cap: Duration::from_secs(60),
        }),
    )
    .await;
    f.publish(&[message("z", 1)]).await;
    assert_eq!(f.dispatch(ok).await, Outcome::Processed);
    f.execute("UPDATE {table} SET processed_at = now() - interval '2 hours' WHERE processed_position IS NOT NULL")
        .await;
    f.publish(&[message("a", 1)]).await;

    assert_eq!(f.dispatch(|| stage_down(None)).await, held(0));
    assert!(!f.last_told().past_budget);
    f.execute("UPDATE {table} SET unavailable_since = unavailable_since - interval '2 hours', next_attempt_at = now() WHERE processed_position IS NULL")
        .await;
    assert_eq!(f.dispatch(|| stage_down(None)).await, held(0), "unrefuted");
    assert!(f.last_told().past_budget, "told past its budget");

    f.execute("UPDATE {table} SET processed_at = now() WHERE processed_position IS NOT NULL")
        .await;
    f.due().await;
    assert_eq!(
        f.dispatch(|| stage_down(None)).await,
        spent(1, false),
        "refuted"
    );
    assert!(f.row("a").await.holding, "a refuted hold keeps its clock");
    f.due().await;
    assert_eq!(
        f.dispatch(|| stage_down(None)).await,
        spent(2, true),
        "weighed again at once"
    );
    let parked = f.row("a").await;
    assert_eq!(
        (parked.parked_cause.as_deref(), parked.holding),
        (Some("kms.unavailable"), false),
        "a parked row holds nothing"
    );
}

/// Two outages, each shorter than the budget, with a failure of another
/// kind between them: the second's clock starts afresh, and nothing parks.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn two_outages_each_under_the_budget_never_park() {
    let f = fixture(
        "twice",
        Retries::up_to(3).with_outage(Outage {
            budget: Duration::from_secs(3600),
            cap: Duration::from_secs(60),
        }),
    )
    .await;
    f.publish(&[message("a", 1)]).await;

    assert_eq!(f.dispatch(|| stage_down(None)).await, held(0));
    f.execute("UPDATE {table} SET unavailable_since = unavailable_since - interval '50 minutes', next_attempt_at = now()")
        .await;
    assert_eq!(
        f.dispatch(|| Err(Transient::because(DB_TIMEOUT, "a moment").into()))
            .await,
        spent(1, false)
    );
    let between = f.row("a").await;
    assert_eq!(
        (between.holding, between.last_cause.as_deref()),
        (false, Some("db.timeout")),
        "another verdict ends the hold"
    );
    f.due().await;
    assert_eq!(f.dispatch(|| stage_down(None)).await, held(1));
    assert!(
        !f.last_told().past_budget,
        "the second outage's clock is its own"
    );
}

/// A cause lives as long as the state it explains: an exhausted failure of
/// the moment parks under its own cause; an unpark, a resolve and a mark
/// each clear the causes and the hold.
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
            exhausted.last_cause.as_deref(),
            exhausted.parked_cause.as_deref()
        ),
        (Some("db.timeout"), Some("db.timeout"))
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
        (unparked.last_cause, unparked.parked_cause, unparked.holding),
        (None, None, false),
        "the unpark clears"
    );

    assert_eq!(
        f.dispatch(|| Err(Permanent::because(KMS_GONE, "no key").into()))
            .await,
        spent(1, true)
    );
    assert_eq!(f.row("a").await.parked_cause.as_deref(), Some("kms.gone"));
    assert!(
        f.sessions
            .session(async |session| f.inbox.resolve(&session, at).await)
            .await
            .unwrap()
    );
    let resolved = f.row("a").await;
    assert_eq!(
        (
            resolved.processed,
            resolved.last_cause,
            resolved.parked_cause
        ),
        (true, None, None),
        "the resolve clears"
    );

    f.publish(&[message("b", 1)]).await;
    assert_eq!(f.dispatch(|| stage_down(None)).await, held(0));
    f.due().await;
    assert_eq!(f.dispatch(ok).await, Outcome::Processed);
    let marked = f.row("b").await;
    assert_eq!(
        (marked.last_cause, marked.holding),
        (None, false),
        "the mark clears"
    );
    let told: Vec<String> = f.told().into_iter().map(|told| told.cause).collect();
    assert_eq!(told, ["db.timeout", "kms.gone", "kms.unavailable"]);
}

/// A row waiting for a dependency that arrived and holds for its stage's
/// outage does not expire: the outage that holds the dependency
/// holds what waits for it. Once the dependency is parked, it expires as
/// any other.
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
        "UPDATE {table} SET waiting_since = now() - interval '20 minutes', next_attempt_at = now()",
    )
    .await;
    assert_eq!(f.dispatch(|| stage_down(None)).await, held(0));
    assert!(!f.row("b").await.parked, "spared while a@1 holds");

    assert_eq!(
        f.park_held("kafka://orders", "kms.unavailable", None).await,
        1
    );
    f.dispatch(ok).await;
    assert!(f.row("b").await.parked, "expired once a@1 holds no more");
}

/// A slot serves its Tenants in turn (ADR-0041 decision 2), so a stage's
/// outage holds each Tenant's head on its own: another Tenant of the slot
/// goes on meanwhile, and the heads held wait, each with the rows behind
/// it. The operator parks every head held for the cause — of the Tenant
/// named, when one is — and each Tenant's next row is its head then. The
/// extent a stage's hold takes under rotation is ADR-0040's revision's to
/// decide (ADR-0040 § Owed); this is what it is until then.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn under_rotation_a_stage_outage_holds_each_tenant_s_head_and_an_operator_parks_them_all() {
    let f = fixture("parkheads", Retries::up_to(3)).await;
    // tenant-2 and tenant-4 have a row processed ahead of their heads, as
    // every Tenant has until the sweep: a head is the first row in the queue
    f.publish(&[
        message_of("tenant-2", "b0", 1),
        message_of("tenant-4", "d0", 1),
    ])
    .await;
    assert_eq!(f.dispatch(ok).await, Outcome::Processed);
    assert_eq!(f.dispatch(ok).await, Outcome::Processed);
    f.publish(&[
        message_of("tenant-1", "a", 1),
        message_of("tenant-2", "b", 1),
        message_of("tenant-3", "c", 1),
        message_of("tenant-4", "d", 1),
        message_of("tenant-1", "a2", 1),
        message_of("tenant-2", "b2", 1),
    ])
    .await;
    // the stage serves tenant-3's message and is down for every other's
    let down_but_for_c = |message: &InboxMessage| {
        if message.tenant_id == "tenant-3" {
            Ok(())
        } else {
            stage_down(None)
        }
    };
    let mut outcomes = Vec::new();
    for _ in 0..4 {
        outcomes.push(format!("{:?}", f.dispatch_on(down_but_for_c).await));
    }
    outcomes.sort();
    let mut expected = [
        format!("{:?}", held(0)),
        format!("{:?}", held(0)),
        format!("{:?}", held(0)),
        format!("{:?}", Outcome::Processed),
    ];
    expected.sort();
    assert_eq!(outcomes, expected, "tenant-3 goes on beside the holds");
    for stream in ["a", "b", "d"] {
        let head = f.row(stream).await;
        assert_eq!((head.holding, head.attempts), (true, 0), "{stream}@1 holds");
    }
    for stream in ["a2", "b2"] {
        let behind = f.row(stream).await;
        assert_eq!(
            (behind.holding, behind.attempts, behind.processed),
            (false, 0, false),
            "{stream}@1 waits behind its Tenant's head"
        );
    }
    assert_eq!(
        f.dispatch(ok).await,
        Outcome::Nothing,
        "every head left holds"
    );

    assert_eq!(
        f.park_held("kafka://orders", "kms.unavailable", Some("tenant-2"))
            .await,
        1,
        "tenant-2's head, not the slot's oldest"
    );
    assert!(f.row("b").await.parked);
    // d@1 as a hold refuted past its budget leaves it: an attempt spent on
    // its own account, its outage's clock kept; a park ends both
    f.execute("UPDATE {table} SET held_since = now() WHERE stream_id->>'id' = 'd'")
        .await;
    assert_eq!(
        f.park_held("kafka://orders", "kms.unavailable", None).await,
        2,
        "every Tenant's head held for the cause, in one call"
    );
    assert!(
        f.inbox
            .held_counts(Duration::from_secs(5))
            .await
            .unwrap()
            .is_empty(),
        "a parked row holds nothing"
    );
    for stream in ["a", "d"] {
        let parked = f.row(stream).await;
        assert_eq!(
            (
                parked.parked,
                parked.parked_cause.as_deref(),
                parked.holding
            ),
            (true, Some("kms.unavailable"), false),
            "{stream}@1"
        );
    }
    f.wake().await;
    assert_eq!(f.dispatch(ok).await, Outcome::Processed);
    assert_eq!(f.dispatch(ok).await, Outcome::Processed);
    assert!(
        f.row("a2").await.processed && f.row("b2").await.processed,
        "each Tenant's next row is its head, and goes on"
    );
}

/// A head that failed on its own and then holds for its stage's outage is
/// held by the outage: `held_counts` counts the holds of a head's own failure
/// (ADR-0041 decision 5), which an outage's are not among (ADR-0040 § Owed
/// 7), so it is not counted as `backoff`, aged by a failure the outage
/// superseded. Refuted, the hold spends on the head's own account again,
/// and is counted again.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_head_held_for_its_stage_is_not_counted_as_its_own_back_off() {
    let f = fixture("heldcount", Retries::up_to(5)).await;
    f.publish(&[message("a", 1)]).await;
    let held_counts = async || {
        f.inbox
            .held_counts(Duration::from_secs(5))
            .await
            .unwrap()
            .len()
    };
    assert_eq!(f.dispatch(|| tenant_down(None)).await, spent(1, false));
    assert_eq!(held_counts().await, 1, "its own failure holds it");
    f.due().await;
    assert_eq!(f.dispatch(|| stage_down(None)).await, held(1));
    assert_eq!(held_counts().await, 0, "the outage holds it now");
    f.due().await;
    assert_eq!(f.dispatch(|| tenant_down(None)).await, spent(2, false));
    assert_eq!(held_counts().await, 1, "its own failure again");
}

/// The operator parks the rows held for a cause, of a channel and, when
/// named, of a Tenant: the rows behind them go on, each row parked under its
/// cause, and the call is told with its principal. A row failing on its
/// Tenant's account is not held, and is left alone.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn an_operator_parks_the_rows_held_for_a_cause_and_their_slots_go_on() {
    let f = fixture("parkheld", Retries::up_to(3)).await;
    f.publish(&[message("a", 1), message("b", 1)]).await;
    assert_eq!(f.dispatch(|| tenant_down(None)).await, spent(1, false));
    assert_eq!(
        f.park_held("kafka://orders", "kms.throttled", None).await,
        0,
        "a Tenant's failure does not hold"
    );
    f.due().await;
    assert_eq!(f.dispatch(|| stage_down(None)).await, held(1));

    assert_eq!(
        f.park_held("kafka://orders", "kms.gone", None).await,
        0,
        "another cause"
    );
    assert_eq!(
        f.park_held("kafka://orders", "kms.unavailable", Some("tenant-2"))
            .await,
        0,
        "another Tenant"
    );
    assert_eq!(
        f.park_held("kafka://other", "kms.unavailable", None).await,
        0,
        "another channel"
    );
    assert_eq!(f.dispatch(ok).await, Outcome::Nothing, "a@1 still holds");
    assert_eq!(
        f.park_held("kafka://orders", "kms.unavailable", Some("tenant-1"))
            .await,
        1
    );
    let parked = f.row("a").await;
    assert_eq!(
        (
            parked.parked,
            parked.parked_cause.as_deref(),
            parked.holding
        ),
        (true, Some("kms.unavailable"), false)
    );
    // the round that found a@1 holding put the slot to sleep for a second
    f.wake().await;
    assert_eq!(f.dispatch(ok).await, Outcome::Processed, "b@1 goes on");
    assert_eq!(
        f.operated().last(),
        Some(&("ops".to_owned(), Operation::ParkedHeld, 1))
    );
}

const REFUSED: Cause = cause!("kms.refused");

/// The re-arm of a refused context's rows, of a channel, parked within the
/// last hour: a canary of one failure, every Tenant.
fn refused_within_an_hour<'a>() -> Unparking<'a> {
    Unparking {
        channel: "kafka://orders",
        cause: ParkedUnder::Cause("kms.refused"),
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
/// a canary of `tries` failures parks again at its `tries`th.
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
    // c@1 as parked before causes were recorded; x@1 as parked for a
    // dependency that never arrived, u@1 for its own values: other grounds,
    // which a bulk unpark never re-arms
    f.execute(
        "UPDATE {table} SET parked_cause = NULL WHERE stream_id->>'id' IN ('c', 'x', 'u'); \
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
            cause: ParkedUnder::Unrecorded,
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
        (a.attempts, a.parked_cause, a.last_cause.as_deref()),
        (4, None, Some("kms.refused")),
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
    f.execute("UPDATE {table} SET parked_at = now(), parked_cause = 'kms.refused', attempts = 1")
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
