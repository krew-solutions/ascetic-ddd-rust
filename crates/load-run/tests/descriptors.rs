//! The process's descriptors (ADR-0047 decision 20), in a test binary of
//! their own: the limits are the process's, and this one test lowers them.
//! Read as `/proc/self/fd` lists them; raised to the hard limit; a run whose
//! every source's cap is filled at a soft limit of exactly admission's bound
//! hits no `EMFILE`, and one below it is refused.
#![cfg(target_os = "linux")]

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use std::future::Future;
use std::net::TcpListener;
use std::sync::Arc;

use ascetic_ddd_command_log::fills::Namespace;
use ascetic_ddd_command_log::record::{Record, Rest};
use ascetic_ddd_command_log::render::RunSecret;
use ascetic_ddd_command_log::validate::Peaks;
use ascetic_ddd_load_run::clock::RealClock;
use ascetic_ddd_load_run::live::{Descriptors, Live};
use ascetic_ddd_load_run::rest::{Config, RestCarrier};
use ascetic_ddd_load_run::run::{DESCRIPTORS_RESERVE, Inadmissible, RunError, admit, run};
use ascetic_ddd_load_run::sink::Lines;
use ascetic_ddd_load_run::source::{Plan, Records};
use ascetic_ddd_load_run::stand::{Route, Stand};
use ascetic_ddd_load_run::tcp::TcpEgress;
use ascetic_ddd_scheduler::journal::{End, Line};
use fixture::*;
use nix::sys::resource::{Resource, getrlimit, setrlimit};

struct OneStand;

impl Stand for OneStand {
    fn route(&self, _: &Record, _: &Rest) -> Result<Route, String> {
        Ok(Route {
            authority: "stand.test".to_owned(),
            endpoint: "gateway".to_owned(),
        })
    }

    fn gate(
        &self,
        _: usize,
        _: ascetic_ddd_load_run::stand::Opening<'_>,
    ) -> impl Future<Output = Option<Result<String, ascetic_ddd_load_run::stand::Refused>>> + Send
    {
        std::future::ready(None)
    }

    fn endpoints(&self, _: &ascetic_ddd_command_log::record::Header, _: u32) -> Vec<String> {
        vec!["gateway".to_owned()]
    }
}

/// The sockets a source may hold.
const CAP: usize = 20;

/// Those `/proc/self/fd` lists, less the listing's own.
fn listed() -> u64 {
    std::fs::read_dir("/proc/self/fd").unwrap().count() as u64 - 1
}

#[test]
fn a_run_at_the_descriptors_bound_hits_no_emfile() {
    // Read as listed.
    let d = Descriptors::read().unwrap();
    assert_eq!(d.open(), listed());
    assert!(d.soft() <= d.hard());
    // Raised: the soft limit at the hard one.
    let raised = Descriptors::raise().unwrap();
    assert!(raised.raised());
    let (soft, hard) = getrlimit(Resource::RLIMIT_NOFILE).unwrap();
    assert_eq!((raised.soft(), raised.hard()), (soft, hard));
    if hard != nix::sys::resource::RLIM_INFINITY {
        assert_eq!(soft, hard, "the soft limit at the hard one");
    }
    // The stand: a listener that never accepts — its handshakes in its
    // backlog, no descriptor of this process's beside each socket.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let machine = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let mut header = header();
    header.channels.clear();
    header.calls.clear();
    header
        .operations
        .iter_mut()
        .for_each(|o| o.timeout = 200_000);
    let header = Arc::new(header);
    let records: Vec<Record> = (1..=CAP as u64).map(|g| call("read", g, 0, 0, 0)).collect();
    let plan = || {
        Plan::assumed(
            header.clone(),
            Peaks {
                pending: CAP as u64,
                ..Default::default()
            },
        )
    };
    let carrier = || {
        Arc::new(RestCarrier::new(
            header.clone(),
            OneStand,
            TcpEgress::new(
                [("gateway".to_owned(), addr)].into_iter().collect(),
                vec![None],
            )
            .with_ports(CAP),
            Config::new(Namespace(1), RunSecret::new([7; 32])),
        ))
    };
    // What a `Live` of one source holds with everything else open: its
    // bound, the caps, those open and the reserve.
    let open = {
        let live = Live::for_caps(&[CAP]).unwrap();
        let open = live.runtimes().descriptors().unwrap().open();
        // Read once its runtimes exist: theirs counted.
        assert_eq!(open, listed());
        open
    };
    let bound = CAP as u64 + open + DESCRIPTORS_RESERVE;
    // A soft limit one below: refused before T0.
    setrlimit(Resource::RLIMIT_NOFILE, bound - 1, hard).unwrap();
    {
        let live = Live::for_caps(&[CAP]).unwrap();
        assert_eq!(live.runtimes().descriptors().unwrap().open(), open);
        match admit::<RealClock, _>(plan(), W, carrier(), live.runtimes()) {
            Err(RunError::Inadmissible(Inadmissible::Descriptors { needed, .. })) => {
                assert_eq!(needed, bound);
            }
            other => panic!("{:?}", other.err()),
        }
    }
    // At the bound: admitted, every source's cap filled — each request out,
    // none answered — and no `EMFILE`.
    setrlimit(Resource::RLIMIT_NOFILE, bound, hard).unwrap();
    let live = Live::for_caps(&[CAP]).unwrap();
    let admitted = admit(plan(), W, carrier(), live.runtimes()).unwrap();
    let clock = Arc::new(RealClock::start(2_000).unwrap());
    let ran = machine
        .block_on(run(
            admitted,
            clock,
            Records::new(records.into_iter()),
            Lines::new(),
        ))
        .unwrap();
    let journal = ran.journal;
    assert!(
        !journal.iter().any(|l| matches!(l, Line::Fault(_))),
        "{:?}",
        journal.last()
    );
    let sent = journal
        .iter()
        .filter(|l| matches!(l, Line::Sent(_)))
        .count();
    assert_eq!(sent, CAP);
    let unknown = journal
        .iter()
        .filter(|l| matches!(l, Line::End(e) if e.end == End::Unknown))
        .count();
    assert_eq!(unknown, CAP);
    drop(listener);
}
