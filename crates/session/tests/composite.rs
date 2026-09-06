//! Tests for the composite session.

use std::sync::{Arc, Mutex};

use ascetic_ddd_session::rest::{HttpAccess, RestSession, RestSessionPool};
use ascetic_ddd_session::testing::{MemorySession, MemorySessionPool};
use ascetic_ddd_session::{
    CompositeSession, CompositeSessionPool, Session, SessionError, SessionPool,
};
use futures::executor::block_on;
use futures::future::BoxFuture;

#[derive(Debug)]
enum AppError {
    Session(SessionError),
    Domain(&'static str),
}

impl From<SessionError> for AppError {
    fn from(error: SessionError) -> Self {
        AppError::Session(error)
    }
}

#[derive(Default)]
struct FakeClient {
    calls: Mutex<Vec<String>>,
}

impl FakeClient {
    async fn get(&self, url: &str) -> Result<u16, AppError> {
        self.calls.lock().unwrap().push(url.to_owned());
        Ok(200)
    }
}

/// The application fixes its session type once, in a newtype it owns — both
/// because the delegate offering each capability must be named explicitly, and
/// because the orphan rule forbids implementing a foreign capability for a
/// foreign composite.
#[derive(Clone)]
struct AppSession(CompositeSession<MemorySession, RestSession<FakeClient>>);

impl Session for AppSession {
    async fn atomic<T, E, F>(&self, scope: F) -> Result<T, E>
    where
        F: AsyncFnOnce(&Self) -> Result<T, E>,
        E: From<SessionError>,
    {
        self.0
            .atomic(async |inner| scope(&AppSession(inner.clone())).await)
            .await
    }
}

// ------------------------- capability delegation -------------------------
// Two lines per capability, replacing Python's `__getattr__` search.

trait Recorder {
    fn record(&self, statement: &str);
}

impl Recorder for MemorySession {
    fn record(&self, statement: &str) {
        MemorySession::record(self, statement);
    }
}

impl Recorder for AppSession {
    fn record(&self, statement: &str) {
        self.0.first().record(statement);
    }
}

impl HttpAccess for AppSession {
    type Client = FakeClient;

    fn http(&self) -> &FakeClient {
        self.0.second().http()
    }

    async fn request<T: Send, E: Send>(
        &self,
        method: &str,
        url: &str,
        call: impl Future<Output = Result<T, E>> + Send,
    ) -> Result<T, E> {
        self.0.second().request(method, url, call).await
    }
}

// ----------------------------- the domain -----------------------------

struct Order {
    id: i64,
}

/// A port over the database.
trait OrderRepository<S: Session>: Sync {
    fn save<'a>(&'a self, session: &'a S, order: &'a Order) -> BoxFuture<'a, Result<(), AppError>>;
}

/// A port over the service.
trait Notifier<S: Session>: Sync {
    fn notify<'a>(
        &'a self,
        session: &'a S,
        order: &'a Order,
    ) -> BoxFuture<'a, Result<(), AppError>>;
}

/// The use case sees one session, as it would with a single backend.
async fn place_order<S, R, N>(
    orders: &R,
    notifier: &N,
    session: &S,
    order: Order,
) -> Result<i64, AppError>
where
    S: Session,
    R: OrderRepository<S>,
    N: Notifier<S>,
{
    session
        .atomic(async |session| {
            orders.save(session, &order).await?;
            notifier.notify(session, &order).await?;
            Ok(order.id)
        })
        .await
}

struct DbOrderRepository;

impl<S: Session + Recorder> OrderRepository<S> for DbOrderRepository {
    fn save<'a>(&'a self, session: &'a S, order: &'a Order) -> BoxFuture<'a, Result<(), AppError>> {
        Box::pin(async move {
            session.record(&format!("INSERT INTO orders (id) VALUES ({})", order.id));
            Ok(())
        })
    }
}

struct RestNotifier;

impl<S> Notifier<S> for RestNotifier
where
    S: Session + HttpAccess<Client = FakeClient> + Sync,
{
    fn notify<'a>(
        &'a self,
        session: &'a S,
        order: &'a Order,
    ) -> BoxFuture<'a, Result<(), AppError>> {
        Box::pin(async move {
            let url = format!("https://example.test/orders/{}", order.id);
            session
                .request("POST", &url, session.http().get(&url))
                .await?;
            Ok(())
        })
    }
}

// ------------------------------- tests -------------------------------

fn pools() -> (
    CompositeSessionPool<MemorySessionPool, RestSessionPool<FakeClient>>,
    Arc<ascetic_ddd_session::testing::Journal>,
    Arc<FakeClient>,
) {
    let db = MemorySessionPool::new();
    let journal = db.journal();
    let rest = RestSessionPool::new(FakeClient::default());
    let client = Arc::clone(rest.client());
    (CompositeSessionPool::new(db, rest), journal, client)
}

/// Repositories written against capabilities work against the composite
/// unchanged: each capability reaches the delegate that offers it.
#[test]
fn one_use_case_drives_both_delegates() {
    let (pool, journal, client) = pools();

    let id = block_on(pool.session(async |inner| {
        let session = AppSession(inner.clone());
        place_order(&DbOrderRepository, &RestNotifier, &session, Order { id: 7 }).await
    }))
    .unwrap();

    assert_eq!(id, 7);
    assert_eq!(
        journal.entries(),
        ["BEGIN", "INSERT INTO orders (id) VALUES (7)", "COMMIT"],
    );
    assert_eq!(
        *client.calls.lock().unwrap(),
        ["https://example.test/orders/7"],
    );
}

/// Both delegates open, innermost closes first.
#[test]
fn scopes_nest_across_delegates() {
    let (pool, journal, _client) = pools();

    block_on(pool.session(async |inner| {
        let session = AppSession(inner.clone());
        session
            .atomic(async |session| {
                session.record("INSERT INTO orders (id) VALUES (1)");
                session
                    .atomic(async |session| {
                        session.record("INSERT INTO lines (sku) VALUES ('A-1')");
                        Ok::<_, AppError>(())
                    })
                    .await
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
            "INSERT INTO lines (sku) VALUES ('A-1')",
            "RELEASE SAVEPOINT sp1",
            "COMMIT",
        ],
    );
}

/// A failure inside the composite scope rolls the transactional delegate back.
/// The REST call, of course, cannot be taken back — that is what a saga is for.
#[test]
fn a_failure_rolls_the_transactional_delegate_back() {
    let (pool, journal, client) = pools();

    let outcome: Result<(), AppError> = block_on(pool.session(async |inner| {
        let session = AppSession(inner.clone());
        session
            .atomic(async |session| {
                session.record("INSERT INTO orders (id) VALUES (1)");
                let url = "https://example.test/orders/1".to_owned();
                session
                    .request("POST", &url, session.http().get(&url))
                    .await?;
                Err(AppError::Domain("rejected"))
            })
            .await
    }));

    assert!(matches!(outcome, Err(AppError::Domain("rejected"))));
    assert_eq!(
        journal.entries(),
        ["BEGIN", "INSERT INTO orders (id) VALUES (1)", "ROLLBACK"],
    );
    assert_eq!(
        client.calls.lock().unwrap().len(),
        1,
        "the request was made and cannot be undone",
    );
}

/// The delegates keep their own guards, so the composite needs none.
#[test]
fn a_second_scope_on_the_same_composite_is_refused() {
    let (pool, _journal, _client) = pools();

    block_on(pool.session(async |inner| {
        let session = AppSession(inner.clone());
        session
            .atomic(async |_child| {
                let second: Result<(), AppError> =
                    session.atomic(async |_| Ok::<_, AppError>(())).await;

                assert!(matches!(
                    second,
                    Err(AppError::Session(SessionError::ScopeAlreadyOpen)),
                ));
                Ok::<_, AppError>(())
            })
            .await
    }))
    .unwrap();
}

/// Three delegates nest: `CompositeSession<A, CompositeSession<B, C>>`.
#[test]
fn three_delegates_compose() {
    let first = MemorySessionPool::new();
    let first_journal = first.journal();
    let second = MemorySessionPool::new();
    let second_journal = second.journal();
    let third = MemorySessionPool::new();
    let third_journal = third.journal();

    let pool = CompositeSessionPool::new(first, CompositeSessionPool::new(second, third));

    block_on(pool.session(async |session| session.atomic(async |_| Ok::<_, AppError>(())).await))
        .unwrap();

    for journal in [first_journal, second_journal, third_journal] {
        assert_eq!(journal.entries(), ["BEGIN", "COMMIT"]);
    }
}

/// Each composite level nests one async closure per delegate, so the state
/// machine of a scope grows with delegates × nesting depth. The composite boxes
/// the scope future to cut that growth; without the box this test does not
/// compile at the default `recursion_limit` ("queries overflow the depth
/// limit"), which is the regression it guards against.
///
/// The failure shows only from a clean build: a warm incremental cache hands
/// the compiler intermediate layouts and hides the depth. Judge this test after
/// `cargo clean -p ascetic-ddd-session`.
#[test]
fn five_nested_scopes_compile_through_the_newtype() {
    let (pool, journal, _client) = pools();

    block_on(pool.session(async |inner| {
        let session = AppSession(inner.clone());
        session
            .atomic(async |s| {
                s.atomic(async |s| {
                    s.atomic(async |s| {
                        s.atomic(async |s| s.atomic(async |_| Ok::<_, AppError>(())).await)
                            .await
                    })
                    .await
                })
                .await
            })
            .await
    }))
    .unwrap();

    assert_eq!(
        journal.entries(),
        [
            "BEGIN",
            "SAVEPOINT sp1",
            "SAVEPOINT sp2",
            "SAVEPOINT sp3",
            "SAVEPOINT sp4",
            "RELEASE SAVEPOINT sp4",
            "RELEASE SAVEPOINT sp3",
            "RELEASE SAVEPOINT sp2",
            "RELEASE SAVEPOINT sp1",
            "COMMIT",
        ],
    );
}
