//! Exhausted ports (ADR-0047 decision 19), measured on purpose (`--ignored`:
//! minutes, the kernel's port search quadratic near exhaustion), in a test
//! binary of its own — the soft limit raised, tens of thousands of sockets
//! held: a bound
//! source's ports run out at its bind (`EADDRINUSE`), an unbound one's at
//! its connect (`EADDRNOTAVAIL`), and the egress calls both the harness's
//! `egress.ports`.
#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};

use ascetic_ddd_load_run::clock::Clock;
use ascetic_ddd_load_run::live::Descriptors;
use ascetic_ddd_load_run::stand::{Connect, Egress, Route};
use ascetic_ddd_load_run::tcp::{TcpEgress, local_ports};

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

/// Both, in turn — a bound source's ports sit in the kernel's bind table,
/// which an unbound connect's search reads too — measured on purpose: the
/// kernel searches its range for each bind and each connect, so near
/// exhaustion the runs are quadratic, some two minutes on the laptop
/// (ADR-0047 § Measured, K2b-8d-1).
#[test]
#[ignore = "two minutes: exhausted ports, measured on purpose"]
fn exhausted_ports_are_the_harness_s() {
    let raised = Descriptors::raise().unwrap();
    let range = local_ports().unwrap();
    assert!(
        raised.soft() > 3 * range as u64,
        "descriptors for the twin: {raised:?}"
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let endpoints = BTreeMap::from([("gateway".to_owned(), addr)]);
    // A bound source: its address's ports taken by sockets bound to it, the
    // last bind refused in use.
    let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 7));
    let mut bound = Vec::new();
    let refused = loop {
        match TcpListener::bind(SocketAddr::new(ip, 0)) {
            Ok(l) => bound.push(l),
            Err(e) => break e,
        }
        assert!(bound.len() <= range, "more binds than the range");
    };
    assert_eq!(refused.kind(), io::ErrorKind::AddrInUse, "{refused}");
    // Others' wildcard listeners in the range take theirs: most, not all.
    assert!(bound.len() + 1_000 >= range, "{} of {range}", bound.len());
    eprintln!(
        "MEASURED bound: {} binds of a range of {range}, then {refused}",
        bound.len()
    );
    let egress = TcpEgress::new(endpoints.clone(), vec![Some(ip)]);
    match connect(&egress) {
        Err(Connect::Harness { cannot, .. }) => assert_eq!(cannot, "egress.ports"),
        other => panic!("{:?}", other.map(|_| ())),
    }
    drop(bound);
    // The system's ports to the endpoint taken by connections accepted and
    // held, the last connect refused.
    let accepted = std::thread::spawn(move || {
        let mut held = Vec::new();
        listener.set_nonblocking(true).unwrap();
        // Done once two seconds pass with nothing to accept.
        let clock = ascetic_ddd_load_run::clock::RealClock::start(2_000).unwrap();
        let mut idle_since = clock.now();
        loop {
            match listener.accept() {
                Ok((s, _)) => {
                    held.push(s);
                    idle_since = clock.now();
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if clock.now() > idle_since + 2_000_000 {
                        return held.len();
                    }
                    std::thread::yield_now();
                }
                Err(e) => panic!("{e}"),
            }
        }
    });
    let mut connected = Vec::new();
    let refused = loop {
        match TcpStream::connect(addr) {
            Ok(s) => connected.push(s),
            Err(e) => break e,
        }
        assert!(connected.len() <= range, "more connects than the range");
    };
    assert_eq!(refused.kind(), io::ErrorKind::AddrNotAvailable, "{refused}");
    assert!(
        connected.len() + 1_000 >= range,
        "{} of {range}",
        connected.len()
    );
    eprintln!(
        "MEASURED unbound: {} connects of a range of {range}, then {refused}",
        connected.len()
    );
    let unbound = TcpEgress::new(endpoints, vec![None]);
    match connect(&unbound) {
        Err(Connect::Harness { cannot, .. }) => assert_eq!(cannot, "egress.ports"),
        other => panic!("{:?}", other.map(|_| ())),
    }
    drop(connected);
    let held = accepted.join().unwrap();
    assert!(held + 1_000 >= range, "{held} accepted");
}
