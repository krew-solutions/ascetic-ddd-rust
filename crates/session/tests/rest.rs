//! Tests for the REST session.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ascetic_ddd_session::observer::{
    Outcome, RequestEnded, ScopeEnded, ScopeKind, ScopeStarted, SessionObserver,
};
use ascetic_ddd_session::rest::{HttpAccess, RestSession, RestSessionPool};
use ascetic_ddd_session::{
    IdentityKey, IsolationLevel, Lookup, Session, SessionError, SessionPool,
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

/// Stand-in for reqwest / hyper: the crate depends on neither.
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

// ----------------------------- the domain -----------------------------

struct Customer {
    id: i64,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct CustomerKey(i64);

impl IdentityKey for CustomerKey {
    type Entity = Customer;
}

trait CustomerGateway<S: Session>: Sync {
    fn fetch<'a>(
        &'a self,
        session: &'a S,
        id: i64,
    ) -> BoxFuture<'a, Result<Arc<Customer>, AppError>>;
}

// -------------------------- the infrastructure --------------------------

struct RestCustomerGateway;

// The gateway names the capability, not the concrete session type.
impl<S> CustomerGateway<S> for RestCustomerGateway
where
    S: Session + HttpAccess<Client = FakeClient> + Sync,
{
    fn fetch<'a>(
        &'a self,
        session: &'a S,
        id: i64,
    ) -> BoxFuture<'a, Result<Arc<Customer>, AppError>> {
        Box::pin(async move {
            let url = format!("https://example.test/customers/{id}");
            let status = session
                .request("GET", &url, session.http().get(&url))
                .await?;
            assert_eq!(status, 200);
            Ok(Arc::new(Customer { id }))
        })
    }
}

// ------------------------------- tests -------------------------------

#[test]
fn a_scope_reaches_the_client_through_the_capability() {
    let pool = RestSessionPool::new(FakeClient::default());
    let calls = Arc::clone(pool.client());

    let customer = block_on(pool.session(async |session| {
        session
            .atomic(async |session| RestCustomerGateway.fetch(session, 7).await)
            .await
    }))
    .unwrap();

    assert_eq!(customer.id, 7);
    assert_eq!(
        *calls.calls.lock().unwrap(),
        ["https://example.test/customers/7"],
    );
}

#[derive(Default)]
struct Recording {
    scopes: Mutex<Vec<(usize, ScopeKind, Option<Outcome>)>>,
    requests: Mutex<Vec<String>>,
    timed: AtomicUsize,
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

    fn on_request_ended(&self, event: &RequestEnded<'_>) {
        self.requests
            .lock()
            .unwrap()
            .push(format!("{} {}", event.method, event.url));
        if !event.elapsed.is_zero() {
            self.timed.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// A REST scope reports itself as logical: nothing was committed anywhere.
#[test]
fn scopes_are_reported_as_logical() {
    let recording = Arc::new(Recording::default());
    let pool = RestSessionPool::new(FakeClient::default()).observed_by(Arc::clone(&recording));

    block_on(pool.session(async |session| {
        session
            .atomic(async |session| {
                RestCustomerGateway.fetch(session, 1).await?;
                session.atomic(async |_nested| Ok::<_, AppError>(())).await
            })
            .await
    }))
    .unwrap();

    assert_eq!(
        *recording.scopes.lock().unwrap(),
        [
            (0, ScopeKind::Session, None),
            (1, ScopeKind::Logical, None),
            (2, ScopeKind::Logical, None),
            (2, ScopeKind::Logical, Some(Outcome::Committed)),
            (1, ScopeKind::Logical, Some(Outcome::Committed)),
            (0, ScopeKind::Session, Some(Outcome::Committed)),
        ],
    );
    assert_eq!(
        *recording.requests.lock().unwrap(),
        ["GET https://example.test/customers/1"],
    );
    assert!(
        recording.timed.load(Ordering::SeqCst) > 0,
        "requests are timed"
    );
}

#[test]
fn a_failing_scope_is_reported_as_rolled_back() {
    let recording = Arc::new(Recording::default());
    let pool = RestSessionPool::new(FakeClient::default()).observed_by(Arc::clone(&recording));

    let outcome: Result<(), AppError> = block_on(pool.session(async |session| {
        session
            .atomic(async |_session| Err(AppError::Domain("rejected")))
            .await
    }));

    assert!(matches!(outcome, Err(AppError::Domain("rejected"))));
    assert_eq!(
        recording
            .scopes
            .lock()
            .unwrap()
            .last()
            .copied()
            .map(|s| s.2),
        Some(Some(Outcome::RolledBack)),
    );
}

#[test]
fn the_identity_map_follows_the_scope() {
    let pool =
        RestSessionPool::new(FakeClient::default()).with_isolation(IsolationLevel::Serializable);

    block_on(pool.session(async |session: &RestSession<FakeClient>| {
        // Outside a scope the map is disabled.
        session
            .identity_map()
            .add(CustomerKey(1), Arc::new(Customer { id: 1 }));
        assert!(matches!(
            session.identity_map().get(&CustomerKey(1)),
            Lookup::Unknown,
        ));

        session
            .atomic(async |session| {
                let customer = RestCustomerGateway.fetch(session, 7).await?;
                session.identity_map().add(CustomerKey(7), customer);

                session
                    .atomic(async |nested| {
                        // The nested scope shares the map.
                        assert!(matches!(
                            nested.identity_map().get(&CustomerKey(7)),
                            Lookup::Found(_),
                        ));
                        Ok::<_, AppError>(())
                    })
                    .await
            })
            .await
    }))
    .unwrap();
}

#[test]
fn a_second_scope_on_the_same_session_is_refused() {
    let pool = RestSessionPool::new(FakeClient::default());

    block_on(pool.session(async |session| {
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

/// `RestSession` is the one session whose `Clone` is written by hand, so it is
/// the one that could give a clone a flag of its own. It must not: a clone is
/// refused beside the original, and admitted once the original's scope ends.
#[test]
fn a_clone_cannot_open_a_scope_beside_the_original() {
    let pool = RestSessionPool::new(FakeClient::default());

    block_on(pool.session(async |session| {
        let twin = session.clone();
        session
            .atomic(async |_child| {
                let refused: Result<(), AppError> =
                    twin.atomic(async |_| Ok::<_, AppError>(())).await;

                assert!(matches!(
                    refused,
                    Err(AppError::Session(SessionError::ScopeAlreadyOpen)),
                ));
                Ok::<_, AppError>(())
            })
            .await?;

        twin.atomic(async |_| Ok::<_, AppError>(())).await
    }))
    .unwrap();
}
