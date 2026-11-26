//! A whole process stalled (ADR-0047 § Owed, decision 18's park that
//! blocked), in a test binary of its own: a child stops this process with
//! `SIGSTOP` for 1.5 s mid-run, beside a stand of its own process that
//! answers at once and refuses connects 40 ms in every 100. Every censor is
//! the stall's, so the fold must find no censor a Root — none sent, none
//! never sent. The stand's own resets, and the system's answers in the burst
//! the machine makes catching up, are Roots counted, not asserted.
#![cfg(target_os = "linux")]

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use ascetic_ddd_load_run::sink::Journal;
use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::process::{Child, Command};
use std::sync::Arc;

use ascetic_ddd_command_log::fills::Namespace;
use ascetic_ddd_command_log::record::{Record, Rest};
use ascetic_ddd_command_log::render::RunSecret;
use ascetic_ddd_load_run::clock::{Clock, RealClock};
use ascetic_ddd_load_run::live::{Descriptors, Live};
use ascetic_ddd_load_run::rest::{Config, RestCarrier};
use ascetic_ddd_load_run::run::{Ready, admit, run};
use ascetic_ddd_load_run::sink::Written;
use ascetic_ddd_load_run::source::{Plan, Records};
use ascetic_ddd_load_run::stand::{Route, Stand};
use ascetic_ddd_load_run::tcp::TcpEgress;
use ascetic_ddd_scheduler::journal::{self, Reader};
use ascetic_ddd_scheduler::machine::MACHINE_REVISION;
use ascetic_ddd_scheduler::verdict::{Class, Log, fold};
use fixture::*;

struct OneStand;

impl Stand for OneStand {
    fn route(&self, _: &Record, _: &Rest) -> Result<Route, String> {
        Ok(Route {
            authority: "stand.test".to_owned(),
            endpoint: "gateway".to_owned(),
        })
    }

    fn endpoints(&self, _: &ascetic_ddd_command_log::record::Header, _: u32) -> Vec<String> {
        vec!["gateway".to_owned()]
    }

    fn gate(
        &self,
        _: usize,
        _: ascetic_ddd_load_run::stand::Opening<'_>,
    ) -> impl Future<Output = Option<Result<String, ascetic_ddd_load_run::stand::Refused>>> + Send
    {
        std::future::ready(None)
    }
}

/// The stand, a process of its own: answers each request at once; closes
/// its listener 40 ms in every 100 — attempts refused before the stall, their
/// connect floors spanning it.
const STAND: &str = r#"
import socket, sys, threading, time
port = int(sys.argv[1])
def serve(c):
    buf = b""
    try:
        while True:
            d = c.recv(65536)
            if not d:
                break
            buf += d
            while b"\r\n\r\n" in buf:
                _, _, buf = buf.partition(b"\r\n\r\n")
                c.sendall(b"HTTP/1.1 200 X\r\ncontent-length: 2\r\n\r\n{}")
    except OSError:
        pass
    finally:
        c.close()
while True:
    l = socket.socket()
    l.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    l.bind(("127.0.0.1", port))
    l.listen(4096)
    l.settimeout(0.001)
    until = time.monotonic() + 0.060
    while time.monotonic() < until:
        try:
            c, _ = l.accept()
        except OSError:
            continue
        threading.Thread(target=serve, args=(c,), daemon=True).start()
    l.close()
    time.sleep(0.040)
"#;

/// Kills its child when dropped.
struct Killed(Child);

impl Drop for Killed {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn a_whole_process_stalled_makes_no_root() {
    let _ = Descriptors::raise().unwrap();
    // The stand on a port found free.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let dir = tempfile::tempdir().unwrap();
    let script = dir.path().join("stand.py");
    std::fs::write(&script, STAND).unwrap();
    let _stand = Killed(
        Command::new("python3")
            .arg("-I")
            .arg(&script)
            .arg(port.to_string())
            .spawn()
            .unwrap(),
    );
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let waiting = RealClock::start(2_000).unwrap();
    while std::net::TcpStream::connect(addr).is_err() {
        assert!(waiting.now() < 5_000_000, "the stand never listened");
        std::thread::yield_now();
    }
    // A read every 2 ms for 4 s, each by a connection of its own — retired at
    // once — its deadline 500 ms on.
    let mut header = header();
    header.channels.clear();
    header.calls.clear();
    header.profile.jitter = 2_000;
    header
        .operations
        .iter_mut()
        .for_each(|o| o.timeout = 500_000);
    let records: Vec<Record> = (0..2_000u64)
        .map(|i| call("read", i + 1, 0, i * 2_000, 0))
        .collect();
    let log = String::from_utf8(bytes(header.clone(), &records)).unwrap();
    let sha256 =
        serde_json::from_str::<serde_json::Value>(log.lines().last().unwrap()).unwrap()["trailer"]
            ["sha256"]
            .as_str()
            .unwrap()
            .to_owned();
    let idle = header.profile.idle;
    let header = Arc::new(header);
    const CAP: usize = 4_000;
    let carrier = RestCarrier::new(
        header.clone(),
        OneStand,
        TcpEgress::new(BTreeMap::from([("gateway".to_owned(), addr)]), vec![None]).with_ports(CAP),
        {
            let mut c = Config::new(RunSecret::new([7; 32]));
            c.idle_margin = idle - 1;
            c
        },
    );
    let live = Live::for_caps(&[CAP]).unwrap();
    let admitted = admit(
        Plan::assumed(
            header.clone(),
            ascetic_ddd_command_log::validate::Peaks {
                pending: 2_000,
                ..Default::default()
            },
        ),
        W,
        Arc::new(carrier),
        live.runtimes(),
        Journal::new(64, 0),
    )
    .unwrap();
    let h = journal::Header {
        journal: journal::JOURNAL_VERSION,
        machine: MACHINE_REVISION,
        source: "stall".to_owned(),
        log: sha256.clone(),
        validator: ascetic_ddd_command_log::live::validator(),
        read_ahead: W,
        jitter: header.profile.jitter,
        resend: header.profile.resend,
        catch_up: header.profile.reserve / 2,
        namespace: 1,
        t0: Some(0),
        gates: Vec::new(),
        judging: None,
        admission: None,
    };
    let sink = Written::assumed(Vec::new(), h.clone(), 64).unwrap();
    // The stall: this process stopped from 1 s to 2.5 s into the run.
    let pid = std::process::id();
    let _stopper = Killed(
        Command::new("sh")
            .arg("-c")
            .arg(format!(
                "sleep 1; kill -STOP {pid}; sleep 1.5; kill -CONT {pid}"
            ))
            .spawn()
            .unwrap(),
    );
    let clock = Arc::new(RealClock::start(header.profile.jitter).unwrap());
    let root = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let ran = root
        .block_on(run(
            Ready::assumed(admitted, clock, sink, Namespace(1)),
            Records::new(records.into_iter()),
        ))
        .unwrap();
    let (bytes, _) = ran.journal;
    let open = || Reader::new(std::io::Cursor::new(bytes.clone())).unwrap();
    let folded = fold(
        &h,
        &Log {
            sha256,
            records: 2_000,
        },
        open(),
        open(),
    );
    let count = |c: Class| folded.classes.iter().filter(|k| **k == c).count();
    // Each Root by its path: a censor — sent and censored, or never sent —
    // the stall's to excuse; or an answer the system gave to a step released
    // past J late, in the burst the machine made catching up (decision 17
    // makes it the system's own: owed, decision 21).
    let lines: Vec<journal::Line> = open().collect::<Result<_, _>>().unwrap();
    let j = header.profile.jitter;
    let (mut censors, mut burst, mut other) = (0, 0, 0);
    for (i, c) in folded.classes.iter().enumerate() {
        if *c != Class::Root {
            continue;
        }
        let line = i as u64 + 2;
        let censored = lines.iter().any(|l| {
            matches!(l, journal::Line::Answer(a) if a.line == line && (a.censored || a.unsent.is_some()))
        });
        let late = lines.iter().any(|l| {
            matches!(l, journal::Line::Released(r) if r.line == line && r.released.saturating_sub(r.intended) > j)
        });
        if censored {
            censors += 1;
            // Its path whole, kept for the next sighting: the journal's
            // lines of its step.
            for l in lines.iter().filter(|l| match l {
                journal::Line::Read(x) => x.line == line,
                journal::Line::Released(x) => x.line == line,
                journal::Line::Dispatch(x) => x.line == line,
                journal::Line::Sent(x) => x.line == line,
                journal::Line::Answer(x) => x.line == line,
                journal::Line::End(x) => x.line == line,
                _ => false,
            }) {
                eprintln!("MEASURED stall censor root, line {line}: {l:?}");
            }
        } else if late {
            burst += 1;
        } else {
            other += 1;
        }
    }
    eprintln!(
        "MEASURED stall: verdict {:?}; roots {} (censors {censors}, late-released answers {burst}, other \
         {other}), undecided {}, passed {}; reports {:?}",
        folded.verdict,
        count(Class::Root),
        count(Class::Undecided),
        count(Class::Passed),
        ran.unsent
    );
    // A stand resetting what its listener's backlog held at its close fails
    // on its own: those Roots are true. A censor is the stall's alone.
    assert_eq!(censors, 0, "a censor the stall made a Root");
    assert!(count(Class::Undecided) > 0, "the stall voided nothing");
}
