//! TCP from a source's own address (ADR-0047 decision 16): the library's
//! `Egress` for a plaintext stand, its endpoints by name, each source bound
//! to an address of its own; TLS, if any, is the host's own egress.

use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};

use tokio::net::{TcpSocket, TcpStream};

use crate::stand::{Connect, Egress, Route};

/// A socket's own error: descriptors exhausted, or another.
fn socket_error(e: io::Error) -> Connect {
    // EMFILE, ENFILE: no `ErrorKind` of their own.
    let cannot = if matches!(e.raw_os_error(), Some(23 | 24)) {
        "egress.descriptors"
    } else {
        "egress.socket"
    };
    Connect::Harness {
        cannot,
        detail: e.to_string(),
    }
}

/// A bind's error: a source address not this host's, or its ports exhausted
/// (a bound socket's port is chosen at the bind).
fn bind_error(e: io::Error) -> Connect {
    let cannot = match e.kind() {
        io::ErrorKind::AddrNotAvailable => "egress.local",
        io::ErrorKind::AddrInUse => "egress.ports",
        _ => "egress.bind",
    };
    Connect::Harness {
        cannot,
        detail: e.to_string(),
    }
}

/// TCP to each endpoint's address, from each source's own.
#[derive(Clone, Debug)]
pub struct TcpEgress {
    endpoints: BTreeMap<String, SocketAddr>,
    sources: Vec<Option<IpAddr>>,
}

impl TcpEgress {
    /// Connections to `endpoints` by name, source `i` bound to `sources[i]`
    /// (or the system's choice).
    pub fn new(endpoints: BTreeMap<String, SocketAddr>, sources: Vec<Option<IpAddr>>) -> TcpEgress {
        TcpEgress { endpoints, sources }
    }
}

impl Egress for TcpEgress {
    type Io = TcpStream;

    fn connect(
        &self,
        source: u32,
        route: &Route,
    ) -> impl Future<Output = Result<TcpStream, Connect>> + Send {
        let addr = self.endpoints.get(&route.endpoint).copied();
        let local = usize::try_from(source)
            .ok()
            .and_then(|s| self.sources.get(s).copied().flatten());
        let endpoint = route.endpoint.clone();
        async move {
            let Some(addr) = addr else {
                return Err(Connect::Harness {
                    cannot: "egress.endpoint",
                    detail: format!("no address for endpoint {endpoint}"),
                });
            };
            let socket = if addr.is_ipv4() {
                TcpSocket::new_v4()
            } else {
                TcpSocket::new_v6()
            }
            .map_err(socket_error)?;
            // Dropped, it resets (decision 19): no TIME_WAIT on the source's
            // port, nothing queued delivered after.
            socket.set_zero_linger().map_err(socket_error)?;
            if let Some(ip) = local {
                socket.bind(SocketAddr::new(ip, 0)).map_err(bind_error)?;
            }
            let stream = socket.connect(addr).await.map_err(Connect::of_connect)?;
            // A request's bytes leave as hyper flushes them.
            stream.set_nodelay(true).map_err(socket_error)?;
            Ok(stream)
        }
    }
}
