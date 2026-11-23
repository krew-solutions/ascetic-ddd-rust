//! The egress's connects (ADR-0047 decision 19): each failure classed as the
//! stand's or the harness's, only the egress knowing which step failed; and
//! every socket reset when dropped, at zero linger.

use std::collections::BTreeMap;
use std::io::{self, Read};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};

use ascetic_ddd_load_run::stand::{Connect, Egress, Route, StandConnect};
use ascetic_ddd_load_run::tcp::TcpEgress;

fn route() -> Route {
    Route {
        authority: "stand.test".to_owned(),
        endpoint: "gateway".to_owned(),
    }
}

fn connect(egress: &TcpEgress) -> Result<tokio::net::TcpStream, Connect> {
    tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap()
        .block_on(egress.connect(0, &route()))
}

fn harness(c: Result<tokio::net::TcpStream, Connect>) -> &'static str {
    match c {
        Err(Connect::Harness { cannot, .. }) => cannot,
        Err(Connect::Stand(s)) => panic!("the stand's: {s:?}"),
        Ok(_) => panic!("connected"),
    }
}

/// A connect's own error, classed: the peer's refusals and the network's
/// losses the stand's; exhausted ports and anything unknown the harness's.
#[test]
fn a_connect_s_error_is_classed() {
    use io::ErrorKind::*;
    for (kind, stand) in [
        (ConnectionRefused, StandConnect::Refused),
        (ConnectionReset, StandConnect::Reset),
        (ConnectionAborted, StandConnect::Aborted),
        (TimedOut, StandConnect::TimedOut),
        (HostUnreachable, StandConnect::Unreachable),
        (NetworkUnreachable, StandConnect::Unreachable),
    ] {
        match Connect::of_connect(io::Error::from(kind)) {
            Connect::Stand(s) => assert_eq!(s, stand, "{kind:?}"),
            other => panic!("{kind:?}: {other:?}"),
        }
    }
    for (kind, cannot) in [
        (AddrNotAvailable, "egress.ports"),
        (PermissionDenied, "egress.connect"),
        (Other, "egress.connect"),
    ] {
        match Connect::of_connect(io::Error::from(kind)) {
            Connect::Harness { cannot: c, .. } => assert_eq!(c, cannot, "{kind:?}"),
            other => panic!("{kind:?}: {other:?}"),
        }
    }
}

/// An endpoint it has no address for, and a source address not this host's:
/// the harness's, nothing sent.
#[test]
fn an_unknown_endpoint_and_a_foreign_source_are_the_harness_s() {
    let none = TcpEgress::new(BTreeMap::new(), vec![None]);
    assert_eq!(harness(connect(&none)), "egress.endpoint");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    // TEST-NET-1: no host's own.
    let foreign = TcpEgress::new(
        BTreeMap::from([("gateway".to_owned(), addr)]),
        vec![Some(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)))],
    );
    assert_eq!(harness(connect(&foreign)), "egress.local");
}

/// A port no one listens on: refused, the stand's.
#[test]
fn a_closed_port_is_refused_by_the_stand() {
    let addr = {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };
    let egress = TcpEgress::new(BTreeMap::from([("gateway".to_owned(), addr)]), vec![None]);
    match connect(&egress) {
        Err(Connect::Stand(StandConnect::Refused)) => {}
        other => panic!("{:?}", other.map(|_| ())),
    }
}

/// A connection dropped resets (decision 19): its peer reads a reset, not an
/// end — no FIN, no TIME_WAIT on the source's port.
#[test]
fn a_dropped_connection_resets() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let egress = TcpEgress::new(BTreeMap::from([("gateway".to_owned(), addr)]), vec![None]);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();
    let stream = rt.block_on(egress.connect(0, &route())).unwrap();
    let (mut peer, _) = listener.accept().unwrap();
    drop(stream);
    let mut buf = [0u8; 16];
    let read = peer.read(&mut buf);
    assert_eq!(
        read.as_ref().map_err(io::Error::kind).err(),
        Some(io::ErrorKind::ConnectionReset),
        "{read:?}"
    );
}
