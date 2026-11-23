//! The host's ports for REST (ADR-0047 decision 16): where a step's call goes,
//! and how a source connects to it.

use std::future::Future;
use std::io;

use ascetic_ddd_command_log::record::{Record, Rest};
use tokio::io::{AsyncRead, AsyncWrite};

/// Where a step's call goes: the authority its `host` names, and the
/// endpoint its connections are pooled by.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Route {
    /// The `host` header's value.
    pub authority: String,
    /// What a source's connections are pooled by: one gateway listener, say.
    pub endpoint: String,
}

/// The stand: a route per step.
pub trait Stand: Send + Sync + 'static {
    /// The route of `record`'s call `rest`; its reason if the stand has none.
    fn route(&self, record: &Record, rest: &Rest) -> Result<Route, String>;
}

/// How a source connects: TCP from the source's own address live, an
/// in-memory pipe in tests; TLS, if any, is the host's.
///
/// Its bytes must reach hyper within one turn of the source runtime's driver
/// and one poll of the connection's task — a stream polled in place, never
/// copied through a task of its own (ADR-0047 decision 17): an attempt's look
/// verifies a turn, and an answer a relaying task still holds would read as
/// absent.
pub trait Egress: Send + Sync + 'static {
    /// A connection's bytes.
    type Io: AsyncRead + AsyncWrite + Send + Unpin + 'static;

    /// A new connection from `source` to `route`'s endpoint.
    fn connect(
        &self,
        source: u32,
        route: &Route,
    ) -> impl Future<Output = io::Result<Self::Io>> + Send;
}
