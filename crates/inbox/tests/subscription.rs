//! An inbox's subscription registers what its stages depend on, and one
//! subscription is live at a time (seedwork ADR-0044).
//!
//! The registration is the subscription's: no composition root pairs a stage
//! with its dependencies. A refused subscription keeps nothing of its call,
//! and the inbox's one live subscription is let go when its task ends,
//! however it ends.
//!
//! These tests need PostgreSQL:
//!
//! ```text
//! ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
//!     cargo test -p ascetic-ddd-inbox --all-features --test subscription -- --ignored
//! ```

use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ascetic_ddd_bus::{
    BoxError, Bus, Cause, Check, Composition, Dependencies, Dependency, Dependent, Error, Message,
    Stage, Subscription,
};
use ascetic_ddd_inbox::{INBOX_SCHEME, Loops, PgInbox};
use ascetic_ddd_session::pg::Identifier;
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};
use ascetic_ddd_session::{PgSession, PgSessionPool};
use futures::future::BoxFuture;
use tokio::sync::{Notify, mpsc};

const DEFAULT_URL: &str = "postgresql://devel:devel@localhost:5432/devel_karmabot_test";

type Inbox = Arc<PgInbox<PgSessionPool>>;

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
        .max_size(8)
        .build()
        .expect("the pool can be built")
}

/// An inbox in a table of its own, built by `build` before it is shared.
async fn inbox_with(
    name: &str,
    build: impl FnOnce(PgInbox<PgSessionPool>) -> PgInbox<PgSessionPool>,
) -> Inbox {
    let table = format!("inbox_subscription_{name}");
    let inbox = build(
        PgInbox::new(PgSessionPool::new(pool()))
            .with_table(
                Identifier::new(&table).unwrap(),
                Identifier::new(format!("{table}_seq")).unwrap(),
            )
            .with_loops(Loops {
                poll_interval: Duration::from_millis(20),
                ..Loops::default()
            }),
    );
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    Arc::new(inbox)
}

async fn inbox(name: &str) -> Inbox {
    inbox_with(name, |inbox| inbox).await
}

/// A dependency told from another by identity only: never asked here.
struct Kms;

impl Dependency for Kms {
    fn served(&self) -> u64 {
        0
    }

    fn bound(&self) -> Duration {
        Duration::from_secs(1)
    }

    fn answers_for(&self, _: Cause) -> bool {
        true
    }

    fn check<'a>(&'a self, _: Option<&'a Message>) -> BoxFuture<'a, Check> {
        Box::pin(async { Check::Unasked })
    }
}

fn kms() -> Arc<dyn Dependency> {
    Arc::new(Kms)
}

/// Passes every message through, declaring a dependency for each of its
/// destinations, as an opening stage does.
struct Opening(Dependencies);

impl Opening {
    fn of(entries: Vec<(&'static str, &Arc<dyn Dependency>)>) -> Arc<Self> {
        Arc::new(Opening(
            Dependencies::by_destination(entries.into_iter().map(|(d, k)| (d, Arc::clone(k))))
                .expect("one dependency a destination"),
        ))
    }
}

impl Dependent for Opening {
    fn dependencies(&self) -> Dependencies {
        self.0.clone()
    }
}

impl Stage for Opening {
    fn outbound(&self, message: Message) -> BoxFuture<'_, Result<Message, BoxError>> {
        Box::pin(async move { Ok(message) })
    }

    fn inbound(&self, message: Message) -> BoxFuture<'_, Result<Message, BoxError>> {
        Box::pin(async move { Ok(message) })
    }
}

/// Subscribes `inbox` through `stage` with `handler`.
fn subscribe<F, Fut>(inbox: &Inbox, stage: &Arc<Opening>, handler: F) -> Result<Subscription, Error>
where
    F: Fn(PgSession, Vec<u8>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), BoxError>> + Send + 'static,
{
    inbox
        .consumer(|message: &Message| Ok(message.payload().to_vec()))
        .through(Arc::clone(stage) as Arc<dyn ascetic_ddd_bus::InboundStage>)
        .subscribe(handler)
}

/// Subscribes `inbox` through `stage`, handling nothing.
fn idle(inbox: &Inbox, stage: &Arc<Opening>) -> Result<Subscription, Error> {
    subscribe(inbox, stage, |_, _| async { Ok(()) })
}

fn already_subscribed(subscribed: &Result<Subscription, Error>) -> bool {
    matches!(
        subscribed,
        Err(Error::Composition(Composition::AlreadySubscribed))
    )
}

fn conflict_on(subscribed: &Result<Subscription, Error>, on: &str) -> bool {
    matches!(
        subscribed,
        Err(Error::Composition(Composition::Conflict { destination })) if destination == on
    )
}

/// Publishes one order into `inbox`.
async fn publish(inbox: &Inbox, position: i32) {
    let mut bus = Bus::new();
    bus.register(INBOX_SCHEME, inbox.channel()).unwrap();
    bus.producer("inbox://orders", |message: &Message| message.clone())
        .unwrap()
        .publish(
            &Message::new(format!("order {position}"))
                .with_key("order-7")
                .with_header("tenant_id", "t1")
                .with_header("stream_type", "orders.Order")
                .with_header("stream_id", "7")
                .with_header("stream_position", position.to_string())
                .with_header(
                    "message_id",
                    format!("00000000-0000-4000-8000-{position:012}"),
                ),
        )
        .await
        .unwrap();
}

/// A subscription registers every channel its stages declare, keys left
/// aside, and the registration outlives it: a channel's gate is the
/// inbox's (S3). Red when the inbox ignores what it is subscribed with,
/// registers one stage's channel only, or forgets them at the cancel.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_subscription_registers_what_its_stages_depend_on_for_the_inbox_s_life() {
    let inbox = inbox("registers").await;
    let (orders, invoices) = (kms(), kms());
    assert!(inbox.dependent_channels().is_empty());
    let subscription = inbox
        .consumer(|message: &Message| Ok(message.payload().to_vec()))
        .through(Opening::of(vec![("inbox://orders/k1", &orders)]))
        .through(Opening::of(vec![("inbox://invoices", &invoices)]))
        .subscribe(|_: PgSession, _: Vec<u8>| async { Ok::<(), BoxError>(()) })
        .unwrap();
    assert_eq!(
        inbox.dependent_channels(),
        ["inbox://invoices", "inbox://orders"]
    );
    subscription.cancel().await;
    assert_eq!(
        inbox.dependent_channels(),
        ["inbox://invoices", "inbox://orders"],
        "the registration went with the subscription"
    );
    inbox.teardown().await.unwrap();
}

/// A subscription refused — another dependency for a channel the inbox
/// weighs, or no runtime — writes nothing and holds nothing: the channels
/// it declared before the conflict are not registered, and a correct one
/// subscribes after it; the same dependency the inbox was built with is no
/// conflict (S3, S7). Red when the registration is written as it is
/// checked, comes before the runtime, or keeps the subscription taken.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_refused_subscription_keeps_nothing_of_its_call() {
    let orders = kms();
    let built = Dependencies::by_destination([("inbox://orders", Arc::clone(&orders))]).unwrap();
    let inbox = inbox_with("refused", |inbox| {
        inbox.depending_on(&built).expect("registered")
    })
    .await;
    assert_eq!(inbox.dependent_channels(), ["inbox://orders"]);

    // "inbox://invoices" comes first in order, and is not kept.
    let other = kms();
    let invoices = kms();
    let refused = idle(
        &inbox,
        &Opening::of(vec![
            ("inbox://invoices", &invoices),
            ("inbox://orders", &other),
        ]),
    );
    assert!(conflict_on(&refused, "inbox://orders"), "{refused:?}");
    assert_eq!(inbox.dependent_channels(), ["inbox://orders"]);

    // Outside a runtime: refused, nothing written, nothing held.
    let outside = {
        let (inbox, stage) = (
            Arc::clone(&inbox),
            Opening::of(vec![("inbox://invoices", &invoices)]),
        );
        std::thread::spawn(move || idle(&inbox, &stage).map(|_| ()))
            .join()
            .unwrap()
    };
    assert!(matches!(outside, Err(Error::Transport(_))), "{outside:?}");
    assert_eq!(inbox.dependent_channels(), ["inbox://orders"]);

    let subscription = idle(
        &inbox,
        &Opening::of(vec![
            ("inbox://invoices", &invoices),
            ("inbox://orders", &orders),
        ]),
    )
    .expect("the same dependency the inbox was built with, and nothing held");
    assert_eq!(
        inbox.dependent_channels(),
        ["inbox://invoices", "inbox://orders"]
    );
    subscription.cancel().await;
    inbox.teardown().await.unwrap();
}

/// One subscription is live at a time: a second is refused, and registers
/// nothing, while the first's task runs — a cancel waiting for the handler
/// in hand included — and accepted once it has ended (S3b). Red when the
/// guard is gone, or let go when the cancel starts rather than when the
/// task ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_second_subscription_is_refused_while_the_first_s_task_runs() {
    let inbox = inbox("live").await;
    let (orders, invoices) = (kms(), kms());
    let stage = Opening::of(vec![("inbox://orders", &orders)]);
    let (started, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let first = subscribe(&inbox, &stage, {
        let (started, release) = (Arc::clone(&started), Arc::clone(&release));
        move |_, _| {
            let (started, release) = (Arc::clone(&started), Arc::clone(&release));
            async move {
                started.notify_one();
                release.notified().await;
                Ok(())
            }
        }
    })
    .unwrap();
    let second = idle(&inbox, &Opening::of(vec![("inbox://invoices", &invoices)]));
    assert!(already_subscribed(&second), "{second:?}");
    assert_eq!(inbox.dependent_channels(), ["inbox://orders"]);

    // A handler in hand: the cancel waits for it, and so does the guard.
    publish(&inbox, 1).await;
    tokio::time::timeout(Duration::from_secs(20), started.notified())
        .await
        .expect("the order is handed over");
    let cancel = first.cancel();
    tokio::pin!(cancel);
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut cancel)
            .await
            .is_err(),
        "the cancel waits while the handler runs"
    );
    assert!(already_subscribed(&idle(&inbox, &stage)));
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(10), &mut cancel)
        .await
        .expect("the cancel returns once the task has");

    // Once its task has ended, at once and every time.
    for _ in 0..50 {
        idle(&inbox, &stage)
            .expect("the first's task has ended")
            .cancel()
            .await;
    }
    inbox.teardown().await.unwrap();
}

/// A subscription is the task's, not its handle's: dropping the handle
/// leaves the task running and the inbox subscribed, and a task that ends
/// by a handler's panic lets the inbox go (S3b). Red when the guard goes
/// with the handle, or is let go by a statement the panic skips.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_subscription_is_held_by_its_task_to_its_end_however_it_ends() {
    let inbox = inbox("task").await;
    let orders = kms();
    let stage = Opening::of(vec![("inbox://orders", &orders)]);

    let panicking = subscribe(&inbox, &stage, |_, _| async {
        if std::hint::black_box(true) {
            panic!("a handler's defect");
        }
        Ok(())
    })
    .unwrap();
    publish(&inbox, 1).await;
    tokio::time::timeout(Duration::from_secs(20), panicking.cancel())
        .await
        .expect("a task that panicked is over");
    let dropped = idle(&inbox, &stage).expect("the panicked task let the inbox go");

    drop(dropped);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        already_subscribed(&idle(&inbox, &stage)),
        "a dropped handle let the inbox go while its task runs"
    );
    inbox.teardown().await.unwrap();
}

/// A cancel from inside a handler returns at once, the task still running:
/// a subscription made right after it, in that handler, is refused as one
/// already live (S3b). Red when the guard is let go at the cancel's call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_subscription_made_inside_a_handler_after_its_own_cancel_is_refused() {
    let inbox = inbox("inside").await;
    let orders = kms();
    let stage = Opening::of(vec![("inbox://orders", &orders)]);
    let own: Arc<Mutex<Option<Arc<Subscription>>>> = Arc::default();
    let (told, mut heard) = mpsc::unbounded_channel();
    let subscription = Arc::new(
        subscribe(&inbox, &stage, {
            let (inbox, stage, own) = (Arc::clone(&inbox), Arc::clone(&stage), Arc::clone(&own));
            move |_, _| {
                let (inbox, stage, told) = (Arc::clone(&inbox), Arc::clone(&stage), told.clone());
                let own = own.lock().unwrap().clone();
                async move {
                    if let Some(own) = own {
                        own.cancel().await;
                        told.send(already_subscribed(&idle(&inbox, &stage))).ok();
                    }
                    Ok(())
                }
            }
        })
        .unwrap(),
    );
    *own.lock().unwrap() = Some(Arc::clone(&subscription));
    publish(&inbox, 1).await;
    let refused = tokio::time::timeout(Duration::from_secs(20), heard.recv())
        .await
        .expect("the handler subscribed again")
        .unwrap();
    assert!(refused, "subscribed while its own task runs");
    subscription.cancel().await;
    inbox.teardown().await.unwrap();
}
