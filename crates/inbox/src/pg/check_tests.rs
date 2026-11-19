//! A dependency's check started by the shell (ADR-0042 decision 1), inside
//! the crate so that it starts one as a dispatch does, with no tick to wait
//! for: the check is answered whatever the inbox's own pool does, and a check
//! that cannot answer is answered unasked.

use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use ascetic_ddd_bus::{Cause, Check, Dependencies, Dependency, Message};
use ascetic_ddd_session::PgSessionPool;
use ascetic_ddd_session::pg::Identifier;
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};

use super::PgInbox;
use crate::partition::ByStream;

const DEFAULT_URL: &str = "postgresql://devel:devel@localhost:5432/devel_karmabot_test";

/// A pool of one session, so that the test can hold it.
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
        .max_size(1)
        .build()
        .expect("the pool can be built")
}

/// A dependency that serves every check at once, and counts them.
struct Serving(AtomicU32);

impl Dependency for Serving {
    fn served(&self) -> u64 {
        0
    }

    fn bound(&self) -> Duration {
        Duration::from_secs(1)
    }

    fn answers_for(&self, _: Cause) -> bool {
        true
    }

    fn check<'a>(&'a self, _: Option<&'a Message>) -> futures::future::BoxFuture<'a, Check> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Check::Served
        })
    }
}

fn orders(dependency: &Arc<Serving>) -> Dependencies {
    Dependencies::by_destination([(
        "kafka://orders",
        Arc::clone(dependency) as Arc<dyn Dependency>,
    )])
    .expect("one dependency")
}

/// The inbox's one session held — its pool exhausted, as by hung statements —
/// a check of a head is answered all the same, asked without the head's
/// headers once a tick's first step has passed; and a check of a channel with
/// no dependency is answered unasked. Red when the head's read has no bound
/// (the check never starts), or a check that cannot answer is left
/// unanswered.
#[tokio::test]
#[ignore = "requires PostgreSQL; run with --ignored"]
async fn a_check_is_answered_whatever_the_pool_does() {
    let pool = pool();
    let serving = Arc::new(Serving(AtomicU32::new(0)));
    let inbox = PgInbox::new(PgSessionPool::new(pool.clone()))
        .with_table(
            Identifier::new("inbox_check_bound").unwrap(),
            Identifier::new("inbox_check_bound_seq").unwrap(),
        )
        .partitioned_by(ByStream)
        .depending_on(&orders(&serving))
        .expect("registered");
    assert_eq!(inbox.dependent_channels(), ["kafka://orders"]);
    inbox.teardown().await.unwrap();
    inbox.migrate().await.unwrap();
    let held = pool.get().await.unwrap();
    let head = (0_i16, "t1".to_owned());
    tokio::time::timeout(
        Duration::from_secs(5),
        inbox.check(1, "kafka://orders", Some(&head), Duration::from_millis(100)),
    )
    .await
    .expect("the check started though no session came");
    tokio::time::timeout(
        Duration::from_secs(5),
        inbox.check(2, "kafka://payments", None, Duration::from_millis(100)),
    )
    .await
    .expect("a check of no dependency answered at once");
    let began = std::time::Instant::now();
    loop {
        let answers = inbox.checked.lock().unwrap().clone();
        if answers.len() == 2 {
            assert!(answers.contains(&(1, Check::Served)), "{answers:?}");
            assert!(answers.contains(&(2, Check::Unasked)), "{answers:?}");
            break;
        }
        assert!(
            began.elapsed() < Duration::from_secs(5),
            "answers {answers:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(serving.0.load(Ordering::SeqCst), 1);
    drop(held);
    inbox.teardown().await.unwrap();
}
