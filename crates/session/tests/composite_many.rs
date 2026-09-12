//! Tests for a set of same-typed sessions acting as one.

use std::sync::{Arc, Mutex};

use ascetic_ddd_session::observer::{ScopeEnded, ScopeStarted, SessionObserver};
use ascetic_ddd_session::testing::{Journal, MemorySessionPool};
use ascetic_ddd_session::{Many, Session, SessionError, SessionPool};
use futures::executor::block_on;

#[derive(Debug)]
enum AppError {
    Session(SessionError),
    Domain(&'static str),
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppError::Session(error) => write!(f, "session: {error}"),
            AppError::Domain(message) => write!(f, "domain: {message}"),
        }
    }
}

impl From<SessionError> for AppError {
    fn from(error: SessionError) -> Self {
        AppError::Session(error)
    }
}

/// The count comes from the caller, not from the type.
fn shards(count: usize) -> (Many<MemorySessionPool>, Vec<Arc<Journal>>) {
    let pools: Vec<_> = (0..count).map(|_| MemorySessionPool::new()).collect();
    let journals = pools.iter().map(|pool| pool.journal()).collect();
    (Many::new(pools), journals)
}

#[test]
fn every_shard_gets_a_scope() {
    let (pools, journals) = shards(5);

    block_on(pools.session(async |shards| {
        shards
            .atomic(async |shards| {
                shards
                    .get(3)
                    .unwrap()
                    .record("INSERT INTO orders (id) VALUES (1)");
                Ok::<_, AppError>(())
            })
            .await
    }))
    .unwrap();

    assert_eq!(journals.len(), 5);
    assert_eq!(
        journals[3].entries(),
        ["BEGIN", "INSERT INTO orders (id) VALUES (1)", "COMMIT"],
    );
    for untouched in [0, 1, 2, 4] {
        assert_eq!(journals[untouched].entries(), ["BEGIN", "COMMIT"]);
    }
}

/// Delegates open in order and close in reverse, as in a tuple.
#[test]
fn shards_open_and_close_in_order() {
    let order = Arc::new(Mutex::new(Vec::new()));

    struct Trace {
        order: Arc<Mutex<Vec<String>>>,
        name: usize,
    }

    impl SessionObserver for Trace {
        fn on_scope_started(&self, event: &ScopeStarted) {
            if event.depth > 0 {
                self.order
                    .lock()
                    .unwrap()
                    .push(format!("{} open", self.name));
            }
        }

        fn on_scope_ended(&self, event: &ScopeEnded) {
            if event.depth > 0 {
                self.order
                    .lock()
                    .unwrap()
                    .push(format!("{} close", self.name));
            }
        }
    }

    let pools: Many<_> = (0..3)
        .map(|name| {
            MemorySessionPool::new().observed_by(Trace {
                order: Arc::clone(&order),
                name,
            })
        })
        .collect();

    block_on(pools.session(async |shards| shards.atomic(async |_| Ok::<_, AppError>(())).await))
        .unwrap();

    assert_eq!(
        *order.lock().unwrap(),
        [
            "0 open", "1 open", "2 open", "2 close", "1 close", "0 close"
        ],
    );
}

/// Each shard is a transaction of its own: a failure rolls back all of them,
/// but nothing here makes them atomic together.
#[test]
fn a_failure_rolls_every_shard_back() {
    let (pools, journals) = shards(3);

    let outcome: Result<(), AppError> = block_on(pools.session(async |shards| {
        shards
            .atomic(async |shards| {
                shards
                    .get(0)
                    .unwrap()
                    .record("INSERT INTO orders (id) VALUES (1)");
                Err(AppError::Domain("rejected"))
            })
            .await
    }));

    assert!(matches!(outcome, Err(AppError::Domain("rejected"))));
    assert_eq!(
        journals[0].entries(),
        ["BEGIN", "INSERT INTO orders (id) VALUES (1)", "ROLLBACK"],
    );
    for rest in [1, 2] {
        assert_eq!(journals[rest].entries(), ["BEGIN", "ROLLBACK"]);
    }
}

/// The count may be zero: the scope then opens nothing and still runs.
#[test]
fn no_shards_is_a_scope_over_nothing() {
    let (pools, journals) = shards(0);

    let reached = block_on(pools.session(async |shards| {
        shards
            .atomic(async |shards| Ok::<_, AppError>(shards.len()))
            .await
    }))
    .unwrap();

    assert_eq!(reached, 0);
    assert!(journals.is_empty());
}

/// Scopes nest, as they do for any other session.
#[test]
fn shard_scopes_nest() {
    let (pools, journals) = shards(2);

    block_on(pools.session(async |shards| {
        shards
            .atomic(async |shards| {
                shards
                    .get(0)
                    .unwrap()
                    .record("INSERT INTO orders (id) VALUES (1)");
                shards
                    .atomic(async |nested| {
                        nested
                            .get(0)
                            .unwrap()
                            .record("INSERT INTO lines (sku) VALUES ('A-1')");
                        Ok::<_, AppError>(())
                    })
                    .await
            })
            .await
    }))
    .unwrap();

    assert_eq!(
        journals[0].entries(),
        [
            "BEGIN",
            "INSERT INTO orders (id) VALUES (1)",
            "SAVEPOINT sp1",
            "INSERT INTO lines (sku) VALUES ('A-1')",
            "RELEASE SAVEPOINT sp1",
            "COMMIT",
        ],
    );
    assert_eq!(
        journals[1].entries(),
        ["BEGIN", "SAVEPOINT sp1", "RELEASE SAVEPOINT sp1", "COMMIT"],
    );
}

fn assert_send<T: Send>(future: T) -> T {
    future
}

/// The trait promises a `Send` future (ADR-0004); a scope that is not `Send`
/// is refused at compile time, which the doc-test on `Session` states.
#[test]
fn a_send_scope_gives_a_send_future() {
    let (pools, _journals) = shards(3);

    let count = block_on(assert_send(pools.session(async |shards| {
        shards
            .atomic(async |shards| Ok::<_, AppError>(shards.len()))
            .await
    })))
    .unwrap();

    assert_eq!(count, 3);
}

/// What the `Send` future is for.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scope_can_be_spawned_on_a_multi_thread_runtime() {
    let (pools, journals) = shards(3);

    let count = tokio::spawn(async move {
        pools
            .session(async |shards| {
                shards
                    .atomic(async |shards| Ok::<_, AppError>(shards.len()))
                    .await
            })
            .await
    })
    .await
    .unwrap()
    .unwrap();

    assert_eq!(count, 3);
    for journal in &journals {
        assert_eq!(journal.entries(), ["BEGIN", "COMMIT"]);
    }
}
