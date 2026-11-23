//! The host's ports for REST (ADR-0047 decision 16): where a step's call goes,
//! and how a source connects to it.

use std::future::Future;
use std::io;

use ascetic_ddd_command_log::fills::Namespace;
use ascetic_ddd_command_log::record::{Header, Record, Rest};
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

/// What a stand's gates see before T0 (ADR-0047 decision 18): the plan, and
/// the run's namespace.
#[derive(Clone, Copy, Debug)]
pub struct Opening<'a> {
    /// The log's header: its profile, its operations.
    pub header: &'a Header,
    /// The run's namespace: one the stand must not have seen.
    pub namespace: Namespace,
}

/// A gate's refusal (decision 18): its name — a dotted name the stand
/// chooses — the journal's; why, the caller's only.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refused {
    /// The gate.
    pub gate: String,
    /// Why, never journaled.
    pub why: String,
}

/// The stand: a route per step, and its gates before T0.
pub trait Stand: Send + Sync + 'static {
    /// The route of `record`'s call `rest`; its reason if the stand has none.
    fn route(&self, record: &Record, rest: &Rest) -> Result<Route, String>;

    /// Its gate `i` before T0, none once past its last (decision 18): the
    /// gate's name once it passed, or its refusal. A gate may wait — the
    /// gateway's buckets refilling — within the bound the run gives it.
    fn gate(
        &self,
        i: usize,
        opening: Opening<'_>,
    ) -> impl Future<Output = Option<Result<String, Refused>>> + Send;
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
