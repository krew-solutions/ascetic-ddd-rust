//! REST session: a scope over an HTTP client.
//!
//! There is no transaction behind it. A scope here bounds an identity map and
//! reports itself to the observer — the same shape as a database session, so a
//! use case can be written once against [`Session`] and run against either.
//! What it deliberately does *not* do is pretend that HTTP calls can be rolled
//! back; work that must be undone across services belongs in a saga.
//!
//! # No HTTP dependency
//!
//! Python ties its REST session to `aiohttp` and Go to `net/http`. Here the
//! client is a type parameter, so the crate depends on no HTTP library and any
//! of them fits:
//!
//! ```
//! use ascetic_ddd_session::rest::{HttpAccess, RestSessionPool};
//! use ascetic_ddd_session::{Session, SessionError, SessionPool};
//!
//! struct FakeClient;
//!
//! let sessions = RestSessionPool::new(FakeClient);
//!
//! futures::executor::block_on(sessions.session(async |session| {
//!     session.atomic(async |session| {
//!         let _client: &FakeClient = session.http();
//!         Ok::<_, SessionError>(())
//!     })
//!     .await
//! })).unwrap();
//! ```
//!
//! Requests are timed by wrapping the call, which replaces the transport hooks
//! of the other ports (`aiohttp.TraceConfig`, a custom `http.RoundTripper`):
//!
//! ```ignore
//! let response = session
//!     .request("GET", &url, client.get(&url).send())
//!     .await?;
//! ```

pub mod observer;
pub mod session;

pub use self::observer::{RequestEnded, RequestStarted, RestObserver};
pub use self::session::{HttpAccess, RestSession, RestSessionPool};
