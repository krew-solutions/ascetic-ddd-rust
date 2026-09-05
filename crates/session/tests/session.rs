//! Tests for the session traits and the in-memory implementation.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ascetic_ddd_session::observer::{
    Outcome, QueryEnded, ScopeEnded, ScopeKind, ScopeStarted, SessionObserver,
};
use ascetic_ddd_session::testing::{MemorySession, MemorySessionPool};
use ascetic_ddd_session::{
    IdentityKey, IdentityMap, IsolationLevel, Lookup, Session, SessionError, SessionPool,
};
use futures::executor::block_on;
use futures::future::BoxFuture;

// ------------------------- application error -------------------------

#[derive(Debug)]
enum AppError {
    /// The session machinery failed; required by `E: From<SessionError>`.
    Session(SessionError),
    /// The domain refused the operation.
    Domain(&'static str),
}

impl From<SessionError> for AppError {
    fn from(error: SessionError) -> Self {
        AppError::Session(error)
    }
}

// ----------------------------- the domain -----------------------------

struct Order {
    id: i64,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct OrderKey(i64);

impl IdentityKey for OrderKey {
    type Entity = Order;
}

/// Port. The session is a type parameter, so the domain never learns what it is.
trait OrderRepository<S: Session>: Sync {
    fn save<'a>(&'a self, session: &'a S, order: &'a Order) -> BoxFuture<'a, Result<(), AppError>>;
}

/// Application service: one transaction, one nested scope inside it.
async fn place_order<S, R>(repository: &R, session: &S, order: Order) -> Result<i64, AppError>
where
    S: Session,
    R: OrderRepository<S>,
{
    session
        .atomic(async |session| {
            repository.save(session, &order).await?;
            session
                .atomic(async |session| repository.save(session, &order).await)
                .await?;
            Ok(order.id)
        })
        .await
}

// -------------------------- the infrastructure --------------------------

/// Infrastructure capability, invisible to the domain.
trait Recorder {
    fn record(&self, statement: &str);
}

impl Recorder for MemorySession {
    fn record(&self, statement: &str) {
        MemorySession::record(self, statement);
    }
}

struct FakeOrderRepository;

impl<S: Session + Recorder> OrderRepository<S> for FakeOrderRepository {
    fn save<'a>(&'a self, session: &'a S, order: &'a Order) -> BoxFuture<'a, Result<(), AppError>> {
        Box::pin(async move {
            session.record(&format!("INSERT INTO orders (id) VALUES ({})", order.id));
            Ok(())
        })
    }
}

// ------------------------------- tests -------------------------------

#[test]
fn nested_scope_opens_a_savepoint() {
    let pool = MemorySessionPool::new();
    let journal = pool.journal();

    let id = block_on(pool.session(async |session| {
        place_order(&FakeOrderRepository, session, Order { id: 7 }).await
    }))
    .unwrap();

    assert_eq!(id, 7);
    assert_eq!(
        journal.entries(),
        [
            "BEGIN",
            "INSERT INTO orders (id) VALUES (7)",
            "SAVEPOINT sp1",
            "INSERT INTO orders (id) VALUES (7)",
            "RELEASE SAVEPOINT sp1",
            "COMMIT",
        ],
    );
}

#[test]
fn failing_nested_scope_leaves_the_outer_transaction_alive() {
    let pool = MemorySessionPool::new();
    let journal = pool.journal();

    block_on(pool.session(async |session| {
        session
            .atomic(async |session| {
                session.record("INSERT INTO orders (id) VALUES (1)");
                let nested: Result<(), AppError> = session
                    .atomic(async |_session| Err(AppError::Domain("rejected")))
                    .await;
                assert!(nested.is_err());
                Ok::<_, AppError>(())
            })
            .await
    }))
    .unwrap();

    assert_eq!(
        journal.entries(),
        [
            "BEGIN",
            "INSERT INTO orders (id) VALUES (1)",
            "SAVEPOINT sp1",
            "ROLLBACK TO SAVEPOINT sp1",
            "COMMIT",
        ],
    );
}

#[test]
fn failing_outer_scope_rolls_back() {
    let pool = MemorySessionPool::new();
    let journal = pool.journal();

    let outcome: Result<(), AppError> = block_on(pool.session(async |session| {
        session
            .atomic(async |session| {
                session.record("INSERT INTO orders (id) VALUES (1)");
                Err(AppError::Domain("rejected"))
            })
            .await
    }));

    assert!(matches!(outcome, Err(AppError::Domain("rejected"))));
    assert_eq!(
        journal.entries(),
        ["BEGIN", "INSERT INTO orders (id) VALUES (1)", "ROLLBACK"],
    );
}

/// Independent work inside one scope runs concurrently — `&self` allows what
/// `&mut self` would forbid.
#[test]
fn independent_work_inside_one_scope_runs_concurrently() {
    let pool = MemorySessionPool::new();
    let journal = pool.journal();

    block_on(pool.session(async |session| {
        session
            .atomic(async |session| {
                let repository = FakeOrderRepository;
                let (a, b) = (Order { id: 1 }, Order { id: 2 });
                futures::try_join!(repository.save(session, &a), repository.save(session, &b),)?;
                Ok::<_, AppError>(())
            })
            .await
    }))
    .unwrap();

    assert_eq!(
        journal.entries(),
        [
            "BEGIN",
            "INSERT INTO orders (id) VALUES (1)",
            "INSERT INTO orders (id) VALUES (2)",
            "COMMIT",
        ],
    );
}

// ----------------------------- observer -----------------------------

#[derive(Default)]
struct Recording {
    scopes: Mutex<Vec<(usize, ScopeKind, Option<Outcome>)>>,
    queries: AtomicUsize,
}

impl SessionObserver for Recording {
    fn on_scope_started(&self, event: &ScopeStarted) {
        self.scopes
            .lock()
            .unwrap()
            .push((event.depth, event.kind, None));
    }

    fn on_scope_ended(&self, event: &ScopeEnded) {
        self.scopes
            .lock()
            .unwrap()
            .push((event.depth, event.kind, Some(event.outcome)));
    }

    fn on_query_ended(&self, _event: &QueryEnded<'_>) {
        self.queries.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn observer_sees_the_whole_lifecycle() {
    let recording = Arc::new(Recording::default());
    let pool = MemorySessionPool::new().observed_by(Arc::clone(&recording));

    block_on(pool.session(async |session| {
        place_order(&FakeOrderRepository, session, Order { id: 7 }).await
    }))
    .unwrap();

    assert_eq!(
        *recording.scopes.lock().unwrap(),
        [
            (0, ScopeKind::Session, None),
            (1, ScopeKind::Transaction, None),
            (2, ScopeKind::Savepoint, None),
            (2, ScopeKind::Savepoint, Some(Outcome::Committed)),
            (1, ScopeKind::Transaction, Some(Outcome::Committed)),
            (0, ScopeKind::Session, Some(Outcome::Committed)),
        ],
    );
    assert_eq!(recording.queries.load(Ordering::SeqCst), 2);
}

/// Two observers compose into one value — the composite signal, without a registry.
#[test]
fn observers_compose() {
    let left = Arc::new(Recording::default());
    let right = Arc::new(Recording::default());
    let pool = MemorySessionPool::new().observed_by((Arc::clone(&left), Arc::clone(&right)));

    block_on(
        pool.session(async |session| session.atomic(async |_session| Ok::<_, AppError>(())).await),
    )
    .unwrap();

    assert_eq!(left.scopes.lock().unwrap().len(), 4);
    assert_eq!(right.scopes.lock().unwrap().len(), 4);
}

// --------------------------- identity map ---------------------------

#[test]
fn identity_map_is_shared_by_nested_scopes_and_cleared_at_the_end() {
    let pool = MemorySessionPool::new().with_isolation(IsolationLevel::Serializable);
    let escaped: Arc<Mutex<Option<Arc<IdentityMap>>>> = Arc::new(Mutex::new(None));
    let sink = Arc::clone(&escaped);

    block_on(pool.session(async |session| {
        // Outside a transaction the map is disabled.
        session
            .identity_map()
            .add(OrderKey(1), Arc::new(Order { id: 1 }));
        assert!(matches!(
            session.identity_map().get(&OrderKey(1)),
            Lookup::Unknown
        ));

        session
            .atomic(async |session| {
                session
                    .identity_map()
                    .add(OrderKey(7), Arc::new(Order { id: 7 }));

                session
                    .atomic(async |nested| {
                        // The nested scope sees what the outer one remembered.
                        assert!(matches!(
                            nested.identity_map().get(&OrderKey(7)),
                            Lookup::Found(_)
                        ));
                        nested.identity_map().add_absent(OrderKey(8));
                        Ok::<_, AppError>(())
                    })
                    .await?;

                // …and the outer one sees what the nested scope remembered.
                assert!(matches!(
                    session.identity_map().get(&OrderKey(8)),
                    Lookup::Absent
                ));
                *sink.lock().unwrap() = Some(session.identity_map_handle());
                Ok::<_, AppError>(())
            })
            .await
    }))
    .unwrap();

    // The map lives exactly as long as the outermost transaction.
    let map = escaped.lock().unwrap().take().unwrap();
    assert!(map.is_empty());
}

/// `E: From<SessionError>` — a failure of the machinery reaches the
/// application error type without the domain knowing about the driver.
#[test]
fn session_errors_convert_into_the_application_error() {
    let error = AppError::from(SessionError::Commit("connection reset".into()));

    let AppError::Session(inner) = error else {
        panic!("expected a session error");
    };
    assert!(inner.to_string().contains("cannot commit"));
}
