//! TCP from a source's own address (ADR-0047 decision 16): the library's
//! `Egress` for a plaintext stand, its endpoints by name, each source bound
//! to an address of its own; TLS, if any, is the host's own egress.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};

use tokio::net::{TcpSocket, TcpStream};

use crate::stand::{Connect, Egress, Route, Unfit};

fn unfit(cannot: &'static str, detail: String) -> Unfit {
    Unfit { cannot, detail }
}

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
    ports: usize,
}

/// Linux's default local port range, 32768 to 60999: its size.
pub const PORTS: usize = 28_232;

impl TcpEgress {
    /// Connections to `endpoints` by name, source `i` bound to `sources[i]`
    /// (or the system's choice); the ports Linux's default range.
    pub fn new(endpoints: BTreeMap<String, SocketAddr>, sources: Vec<Option<IpAddr>>) -> TcpEgress {
        TcpEgress {
            endpoints,
            sources,
            ports: PORTS,
        }
    }

    /// The same, a bound source holding at most `ports` sockets; the
    /// sources the system binds share them.
    pub fn with_ports(self, ports: usize) -> TcpEgress {
        TcpEgress { ports, ..self }
    }
}

/// The host's local port range's size, from the kernel's setting.
pub fn local_ports() -> io::Result<usize> {
    let range = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range")?;
    let mut bounds = range.split_whitespace().map(str::parse::<usize>);
    match (bounds.next(), bounds.next()) {
        (Some(Ok(low)), Some(Ok(high))) if high >= low => {
            Ok(high.saturating_sub(low).saturating_add(1))
        }
        _ => Err(io::Error::other(format!("a port range not read: {range}"))),
    }
}

impl Egress for TcpEgress {
    type Io = TcpStream;

    /// Every source listed; the bound ones on addresses of their own, each
    /// the host's — a throwaway socket bound to it, no packet sent.
    fn admits(&self, sources: u32) -> Result<(), Unfit> {
        let n = usize::try_from(sources).unwrap_or(usize::MAX);
        if n > self.sources.len() {
            return Err(unfit(
                "egress.source",
                format!("{sources} sources, {} listed", self.sources.len()),
            ));
        }
        let mut bound = BTreeSet::new();
        for ip in self.sources.iter().take(n).flatten() {
            if !bound.insert(*ip) {
                return Err(unfit(
                    "egress.distinct",
                    format!("two sources bound to {ip}: one port space"),
                ));
            }
        }
        for ip in bound {
            std::net::UdpSocket::bind(SocketAddr::new(ip, 0)).map_err(|e| {
                let cannot = if e.kind() == io::ErrorKind::AddrNotAvailable {
                    "egress.local"
                } else {
                    "egress.bind"
                };
                unfit(cannot, format!("{ip}: {e}"))
            })?;
        }
        Ok(())
    }

    /// An address for `endpoint`; a bound source of its family; an unbound
    /// one routed from no bound source's address — its local name after a
    /// UDP connect, which sends nothing.
    fn reaches(&self, source: u32, endpoint: &str) -> Result<(), Unfit> {
        let Some(addr) = self.endpoints.get(endpoint).copied() else {
            return Err(unfit(
                "egress.endpoint",
                format!("no address for endpoint {endpoint}"),
            ));
        };
        let Some(local) = usize::try_from(source)
            .ok()
            .and_then(|s| self.sources.get(s).copied())
        else {
            return Err(unfit("egress.source", format!("no source {source}")));
        };
        match local {
            Some(ip) if ip.is_ipv4() != addr.is_ipv4() => Err(unfit(
                "egress.family",
                format!("source {ip} to endpoint {endpoint} at {addr}"),
            )),
            Some(_) => Ok(()),
            None => {
                let any = if addr.is_ipv4() {
                    SocketAddr::from(([0, 0, 0, 0], 0))
                } else {
                    SocketAddr::from(([0u16; 8], 0))
                };
                let routed = std::net::UdpSocket::bind(any)
                    .and_then(|u| u.connect(addr).and_then(|()| u.local_addr()))
                    .map_err(|e| unfit("egress.route", format!("{endpoint} at {addr}: {e}")))?
                    .ip();
                if self.sources.iter().flatten().any(|b| *b == routed) {
                    return Err(unfit(
                        "egress.shared",
                        format!("source {source} routed from {routed}, a bound source's address"),
                    ));
                }
                Ok(())
            }
        }
    }

    /// A bound source's ports are its own; the unbound share the system's.
    fn ports(&self, source: u32) -> usize {
        let bound = usize::try_from(source)
            .ok()
            .and_then(|s| self.sources.get(s).copied().flatten())
            .is_some();
        if bound {
            self.ports
        } else {
            let unbound = self.sources.iter().filter(|s| s.is_none()).count().max(1);
            self.ports / unbound
        }
    }

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
