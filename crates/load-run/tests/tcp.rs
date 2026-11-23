//! The egress's connects (ADR-0047 decision 19): each failure classed as the
//! stand's or the harness's, only the egress knowing which step failed; and
//! every socket reset when dropped, at zero linger.

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

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

/// A bound source's ports are its own; the sources the system binds share
/// the rest.
#[test]
fn a_source_s_ports() {
    let ip = Some(IpAddr::V4(Ipv4Addr::LOCALHOST));
    let egress = TcpEgress::new(BTreeMap::new(), vec![ip, None, None]).with_ports(100);
    assert_eq!(
        (egress.ports(0), egress.ports(1), egress.ports(2)),
        (100, 50, 50)
    );
    assert_eq!(
        TcpEgress::new(BTreeMap::new(), vec![None]).ports(0),
        ascetic_ddd_load_run::tcp::PORTS
    );
    let ports = ascetic_ddd_load_run::tcp::local_ports().unwrap();
    assert!((1..=65_536).contains(&ports), "{ports}");
}

fn unfit<T: std::fmt::Debug>(r: Result<T, ascetic_ddd_load_run::stand::Unfit>) -> &'static str {
    match r {
        Err(u) => u.cannot,
        Ok(v) => panic!("fit: {v:?}"),
    }
}

/// Decision 20: an egress admits its sources and reaches each endpoint with
/// no packet sent — every source listed, the bound on addresses of their
/// own and the host's, an endpoint known and of the source's family, an
/// unbound source not routed from a bound one's address.
#[test]
fn an_egress_admits_its_sources_and_reaches_its_endpoints_with_no_packet() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    let at = |e: &str, a: SocketAddr| BTreeMap::from([(e.to_owned(), a)]);
    let local = |last: u8| Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, last)));
    // Fit: a bound source of its own, reaching its endpoint.
    let own = TcpEgress::new(at("gateway", addr), vec![local(2)]);
    own.admits(1).unwrap();
    own.reaches(0, "gateway").unwrap();
    // More sources than listed; one past the list.
    assert_eq!(unfit(own.admits(2)), "egress.source");
    assert_eq!(unfit(own.reaches(1, "gateway")), "egress.source");
    // Two bound to one address: one port space.
    let twice = TcpEgress::new(at("gateway", addr), vec![local(1), local(1)]);
    assert_eq!(unfit(twice.admits(2)), "egress.distinct");
    // Not the host's.
    let foreign = TcpEgress::new(
        at("gateway", addr),
        vec![Some(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)))],
    );
    assert_eq!(unfit(foreign.admits(1)), "egress.local");
    // No address for it.
    assert_eq!(unfit(own.reaches(0, "nowhere")), "egress.endpoint");
    // A v4 source, a v6 endpoint.
    let v6 = TcpEgress::new(at("gateway", "[::1]:9".parse().unwrap()), vec![local(2)]);
    assert_eq!(unfit(v6.reaches(0, "gateway")), "egress.family");
    // An unbound source routed from the bound one's address.
    let shared = TcpEgress::new(at("gateway", addr), vec![local(1), None]);
    shared.admits(2).unwrap();
    assert_eq!(unfit(shared.reaches(1, "gateway")), "egress.shared");
    // Nothing reached the stand.
    assert_eq!(
        listener.accept().map(|_| ()).map_err(|e| e.kind()),
        Err(io::ErrorKind::WouldBlock)
    );
}

struct Declaring(&'static str);

impl ascetic_ddd_load_run::stand::Stand for Declaring {
    fn route(
        &self,
        _: &ascetic_ddd_command_log::record::Record,
        _: &ascetic_ddd_command_log::record::Rest,
    ) -> Result<Route, String> {
        Ok(route())
    }

    fn endpoints(&self, _: &ascetic_ddd_command_log::record::Header, _: u32) -> Vec<String> {
        vec![self.0.to_owned()]
    }

    fn gate(
        &self,
        _: usize,
        _: ascetic_ddd_load_run::stand::Opening<'_>,
    ) -> impl std::future::Future<
        Output = Option<Result<String, ascetic_ddd_load_run::stand::Refused>>,
    > + Send {
        std::future::ready(None)
    }
}

/// Decision 20: admission refuses an endpoint the stand declares and the
/// egress does not reach — before T0, nothing sent.
#[test]
fn admission_refuses_an_endpoint_unreached() {
    use ascetic_ddd_load_run::clock::VirtualClock;
    use ascetic_ddd_load_run::rest::{Config, RestCarrier};
    use ascetic_ddd_load_run::run::{Inadmissible, RunError, Runtimes, admit};
    use ascetic_ddd_load_run::source::Plan;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let runtimes = Runtimes::assumed(vec![rt.handle().clone()], 1_024);
    let mut h = fixture::header();
    h.channels.clear();
    h.calls.clear();
    let h = std::sync::Arc::new(h);
    let admitted = |declared, source: Option<IpAddr>| {
        let carrier = RestCarrier::new(
            h.clone(),
            Declaring(declared),
            TcpEgress::new(BTreeMap::from([("gateway".to_owned(), addr)]), vec![source])
                .with_ports(100),
            Config::new(
                ascetic_ddd_command_log::fills::Namespace(1),
                ascetic_ddd_command_log::render::RunSecret::new([7; 32]),
            ),
        );
        admit::<VirtualClock, _>(
            Plan::assumed(h.clone(), Default::default()),
            fixture::W,
            std::sync::Arc::new(carrier),
            &runtimes,
        )
        .map(|_| ())
    };
    admitted("gateway", None).unwrap();
    let refused = |declared, source| match admitted(declared, source) {
        Err(RunError::Inadmissible(Inadmissible::Carrier(why))) => why,
        other => panic!("{other:?}"),
    };
    let why = refused("elsewhere", None);
    assert!(why.starts_with("egress.endpoint"), "{why}");
    // A source the egress does not admit: not the host's.
    let why = refused("gateway", Some(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1))));
    assert!(why.starts_with("egress.local"), "{why}");
}
