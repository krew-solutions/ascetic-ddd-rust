//! TCP from a source's own address (ADR-0047 decision 16): the library's
//! `Egress` for a plaintext stand, its endpoints by name, each source bound
//! to an address of its own; TLS, if any, is the host's own egress.

use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};

use tokio::net::{TcpSocket, TcpStream};

use crate::stand::{Egress, Route};

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
    ) -> impl Future<Output = io::Result<TcpStream>> + Send {
        let addr = self.endpoints.get(&route.endpoint).copied();
        let local = usize::try_from(source)
            .ok()
            .and_then(|s| self.sources.get(s).copied().flatten());
        let endpoint = route.endpoint.clone();
        async move {
            let Some(addr) = addr else {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("no address for endpoint {endpoint}"),
                ));
            };
            let socket = if addr.is_ipv4() {
                TcpSocket::new_v4()?
            } else {
                TcpSocket::new_v6()?
            };
            if let Some(ip) = local {
                socket.bind(SocketAddr::new(ip, 0))?;
            }
            let stream = socket.connect(addr).await?;
            // A request's bytes leave as hyper flushes them.
            stream.set_nodelay(true)?;
            Ok(stream)
        }
    }
}
