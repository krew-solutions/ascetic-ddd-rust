//! K2b-1 (ADR-0047 decision 16): the shell's loop, under the virtual clock and
//! a scripted carrier, journals exactly what `sim` journals — every clean
//! corpus log, under the scheduler differential's scripts, at three
//! read-aheads; a line read by the machine's own timer after an idle gap; an
//! attempt dropped unended, or its carrier panicking, a harness fault.

// The command log's fixture reads its corpus's directory.
#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use ascetic_ddd_load_run::sink::Journal;
use std::future::Future;
use std::sync::Arc;

use ascetic_ddd_command_log::record::{Header, Record};
use ascetic_ddd_command_log::validate::NoHost;
use ascetic_ddd_faker::{Key, Seed};
use ascetic_ddd_load_run::carrier::Carrier;
use ascetic_ddd_load_run::clock::{Clock, VirtualClock};
use ascetic_ddd_load_run::run::{Ran, RunEnd, RunError, Runtimes, Stop, admit, run};
use ascetic_ddd_load_run::sink::Lines;
use ascetic_ddd_load_run::source::{LineSource, Records};
use ascetic_ddd_load_run::stamps::{Attempt, Harness};
use ascetic_ddd_scheduler::journal;
use ascetic_ddd_scheduler::machine::{Captured, Judgement, Seen};
use ascetic_ddd_scheduler::sim::{self, run_reading};
use fixture::*;

mod common;

use common::{Script, Scripted};

/// A run stopped (decision 18): why, and its journal, whose last line is
/// its fault's class.
fn stopped(ran: Ran<Vec<journal::Line>>) -> (Stop, Vec<journal::Line>) {
    let RunEnd::Stopped(stop) = ran.end else {
        panic!("ended: {:?}", ran.journal.last())
    };
    match ran.journal.last() {
        Some(journal::Line::Fault(f)) => assert_eq!(f.class, stop.class()),
        other => panic!("{other:?}"),
    }
    (stop, ran.journal)
}

fn shell<X: Carrier<VirtualClock>>(
    header: &Header,
    records: &[Record],
    read_ahead: u64,
    carrier: X,
) -> Result<Ran<Vec<journal::Line>>, RunError> {
    shell_from(
        header,
        Records::new(records.iter().cloned()),
        read_ahead,
        carrier,
    )
}

/// [`shell`] over a line source of its own.
fn shell_from<S: LineSource, X: Carrier<VirtualClock>>(
    header: &Header,
    source: S,
    read_ahead: u64,
    carrier: X,
) -> Result<Ran<Vec<journal::Line>>, RunError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    rt.block_on(async {
        let clock = Arc::new(VirtualClock::start(0));
        let sources = header.profile.sources.max(1) as usize;
        let runtimes = Runtimes::assumed(vec![tokio::runtime::Handle::current(); sources], 1_024);
        // A refusal before T0 comes back as it is: no run, no journal.
        let admitted = admit(
            ascetic_ddd_load_run::source::Plan::assumed(
                Arc::new(header.clone()),
                Default::default(),
            ),
            read_ahead,
            Arc::new(carrier),
            &runtimes,
            Journal::new(64, 0),
        )?;
        clock
            .within(run(
                ascetic_ddd_load_run::run::Ready::assumed(
                    admitted,
                    clock.clone(),
                    Lines::new(),
                    ascetic_ddd_command_log::fills::Namespace(1),
                ),
                source,
            ))
            .await
    })
}

/// The scheduler differential's script (`scheduler/tests/reference.rs`).
fn script(
    seed: u64,
    log: &str,
) -> impl Fn(u64, u32, &Record) -> sim::Attempt + Send + Sync + use<> {
    let root = Key::root(Seed(seed)).child(log);
    move |line, attempt, r| {
        let k = root.at(line).at(u64::from(attempt));
        let draw = |label: &str, m: u64| k.child(label).unit().raw() % m;
        let judgement = match draw("judgement", 20) {
            0 => Judgement::Unexpected,
            1 => Judgement::NotTaken,
            2 => Judgement::Unknown,
            _ if !r.capture.is_empty() && draw("bare", 6) == 0 => Judgement::Expected(vec![]),
            _ => Judgement::Expected(
                r.capture
                    .iter()
                    .map(|c| Captured {
                        subject: c.subject.clone(),
                        slot: c.slot.clone(),
                        value: line.to_string().into_bytes(),
                    })
                    .collect(),
            ),
        };
        let after = match draw("after", 4) {
            0 => None,
            1 => Some(RESEND / 2),
            2 => Some(RESEND * 3 + draw("by", 500)),
            _ => Some(u64::MAX / 4),
        };
        sim::Attempt {
            after,
            sent_after: draw("sent", 300),
            answer_after: (draw("silent", 30) != 0).then(|| draw("answer", 3_000)),
            judgement,
        }
    }
}

/// Every attempt sent at once and answered as expected at its deadline.
fn at_deadline(header: Header) -> impl Fn(u64, u32, &Record) -> sim::Attempt + Send + Sync {
    move |line, _, r| sim::Attempt {
        after: None,
        sent_after: 0,
        answer_after: Some(ascetic_ddd_command_log::plan::timeout(&header, &r.action)),
        judgement: Judgement::Expected(
            r.capture
                .iter()
                .map(|c| Captured {
                    subject: c.subject.clone(),
                    slot: c.slot.clone(),
                    value: line.to_string().into_bytes(),
                })
                .collect(),
        ),
    }
}

/// The shell's journal and `sim`'s, line for line.
fn same(name: &str, header: &Header, records: &[Record], read_ahead: u64, s: Arc<Script>) -> usize {
    let ran = run_reading(header.clone(), records, read_ahead, &|_| 0, &*s).unwrap();
    let shelled = shell(header, records, read_ahead, Scripted::new(s))
        .unwrap_or_else(|e| panic!("{name}/{read_ahead}: {e}"));
    if let Some(i) = (0..ran.journal.len().max(shelled.journal.len()))
        .find(|&i| ran.journal.get(i) != shelled.journal.get(i))
    {
        panic!(
            "{name}/{read_ahead}: line {i} differs: sim {:?}, the shell {:?}",
            ran.journal.get(i),
            shelled.journal.get(i)
        );
    }
    assert_eq!(shelled.held, ran.left, "{name}/{read_ahead}: cells held");
    ran.journal.len()
}

#[test]
fn the_shell_journals_what_sim_journals() {
    let mut runs = 0;
    let mut lines = 0;
    for (name, records) in fixture::corpus::logs() {
        let mut cases = vec![(name.clone(), fixture::corpus::corpus_header())];
        if name.matches('-').count() == 1 {
            cases.push((format!("{name}/short"), fixture::corpus::short_header()));
        }
        for (name, header) in cases {
            let (report, _) = run_bounded(header.clone(), &records, &NoHost, 1_000);
            if !report.violations.is_empty() {
                continue;
            }
            let scripts: Vec<(String, Arc<Script>)> = vec![
                (format!("{name}/1"), Arc::new(script(1, &name))),
                (format!("{name}/2"), Arc::new(script(2, &name))),
                (format!("{name}/3"), Arc::new(script(3, &name))),
                (format!("{name}/at"), Arc::new(at_deadline(header.clone()))),
            ];
            for (name, s) in scripts {
                for read_ahead in [0, W / 2, header.read_ahead] {
                    lines += same(&name, &header, &records, read_ahead, s.clone());
                    runs += 1;
                }
            }
        }
    }
    assert!(runs > 300, "{runs} runs");
    eprintln!("{runs} runs, {lines} journal lines equal");
}

/// A line more than L after the last end: read by the machine's own timer at
/// `at − L`, not left waiting for a `Want` that never comes.
#[test]
fn a_line_after_an_idle_gap_is_read_on_time() {
    let records = vec![call("read", 1, 0, 0, 0), call("read", 2, 0, 1_000_000, 0)];
    let header = header();
    let quick: Arc<Script> = Arc::new(|_, _, _| sim::Attempt {
        after: None,
        sent_after: 5,
        answer_after: Some(20),
        judgement: Judgement::Expected(vec![]),
    });
    same("idle gap", &header, &records, W, quick.clone());
    let ran = shell(&header, &records, W, Scripted::new(quick)).unwrap();
    let read = ran.journal.iter().find_map(|l| match l {
        journal::Line::Read(r) if r.line == 3 => Some(r.at),
        _ => None,
    });
    assert_eq!(read, Some(1_000_000 - W));
}

struct Forgetful {
    panics: bool,
}

impl Carrier<VirtualClock> for Forgetful {
    fn carries(&self, _: &Header) -> Result<(), String> {
        Ok(())
    }

    fn needs(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
        _: u32,
    ) -> Result<ascetic_ddd_load_run::carrier::Needs, String> {
        Ok(ascetic_ddd_load_run::carrier::Needs::Nothing)
    }

    fn source_costs(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
        _: u32,
    ) -> Result<ascetic_ddd_load_run::carrier::SourceCosts, String> {
        Ok(ascetic_ddd_load_run::carrier::SourceCosts::NOTHING)
    }

    fn attempts(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
    ) -> Result<ascetic_ddd_load_run::carrier::AttemptCosts, ascetic_ddd_load_run::carrier::Unstated>
    {
        Ok(ascetic_ddd_load_run::carrier::AttemptCosts::NOTHING)
    }

    fn gate_bytes(&self) -> ascetic_ddd_load_run::stand::GateBytes {
        ascetic_ddd_load_run::stand::GateBytes::NONE
    }

    fn probe(
        &self,
        _: u32,
        _: std::sync::Arc<VirtualClock>,
        _: u64,
    ) -> impl std::future::Future<Output = Vec<ascetic_ddd_load_run::carrier::Unprobed>> + Send + 'static
    {
        std::future::ready(Vec::new())
    }

    fn carry(&self, a: Attempt<VirtualClock>) -> impl Future<Output = ()> + Send + 'static {
        let panics = self.panics;
        async move {
            a.sent();
            if panics {
                panic!("a carrier's own bug");
            }
            drop(a);
        }
    }
}

#[test]
fn an_attempt_dropped_unended_is_a_harness_fault() {
    let records = vec![call("read", 1, 0, 0, 0)];
    for panics in [false, true] {
        let (stop, _) = stopped(shell(&header(), &records, W, Forgetful { panics }).unwrap());
        assert!(
            matches!(
                stop,
                Stop::Harness(Harness::Dropped {
                    line: 2,
                    attempt: 1
                })
            ),
            "{stop:?}"
        );
    }
}

struct Refusing;

impl Carrier<VirtualClock> for Refusing {
    fn carries(&self, _: &Header) -> Result<(), String> {
        Err("no such transport".to_owned())
    }

    fn needs(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
        _: u32,
    ) -> Result<ascetic_ddd_load_run::carrier::Needs, String> {
        Ok(ascetic_ddd_load_run::carrier::Needs::Nothing)
    }

    fn source_costs(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
        _: u32,
    ) -> Result<ascetic_ddd_load_run::carrier::SourceCosts, String> {
        Ok(ascetic_ddd_load_run::carrier::SourceCosts::NOTHING)
    }

    fn attempts(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
    ) -> Result<ascetic_ddd_load_run::carrier::AttemptCosts, ascetic_ddd_load_run::carrier::Unstated>
    {
        Ok(ascetic_ddd_load_run::carrier::AttemptCosts::NOTHING)
    }

    fn gate_bytes(&self) -> ascetic_ddd_load_run::stand::GateBytes {
        ascetic_ddd_load_run::stand::GateBytes::NONE
    }

    fn probe(
        &self,
        _: u32,
        _: std::sync::Arc<VirtualClock>,
        _: u64,
    ) -> impl std::future::Future<Output = Vec<ascetic_ddd_load_run::carrier::Unprobed>> + Send + 'static
    {
        std::future::ready(Vec::new())
    }

    fn carry(&self, a: Attempt<VirtualClock>) -> impl Future<Output = ()> + Send + 'static {
        a.unanswered();
        std::future::ready(())
    }
}

#[test]
fn a_log_the_carrier_cannot_carry_is_refused_before_t0() {
    let records = vec![call("read", 1, 0, 0, 0)];
    let err = shell(&header(), &records, W, Refusing).unwrap_err();
    assert!(
        matches!(err, RunError::Inadmissible(ascetic_ddd_load_run::run::Inadmissible::Carrier(ref why)) if why == "no such transport"),
        "{err:?}"
    );
    let mut wide = header();
    wide.profile.sources = 3;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    // Fewer runtimes than sources: refused at admission, no run begun.
    let err = rt.block_on(async {
        let runtimes = Runtimes::assumed(vec![tokio::runtime::Handle::current()], 1_024);
        admit::<VirtualClock, _>(
            ascetic_ddd_load_run::source::Plan::assumed(Arc::new(wide), Default::default()),
            W,
            Arc::new(Scripted::new(Arc::new(script(1, "refused")))),
            &runtimes,
            Journal::new(64, 0),
        )
        .err()
        .unwrap()
    });
    assert!(matches!(err, RunError::Inadmissible(_)), "{err:?}");
}

/// A clock whose every read on a carrier's thread takes 200 µs after the
/// reading: read under the frontier's lock, as every stamp must be, it only
/// slows the run; read before it, a stamp goes stale while the machine
/// releases past it.
struct Sluggish<C>(C);

impl<C: Clock> Clock for Sluggish<C> {
    fn now(&self) -> u64 {
        let now = self.0.now();
        #[expect(
            clippy::disallowed_methods,
            reason = "a test's clock, slowed on purpose"
        )]
        if std::thread::current()
            .name()
            .is_some_and(|n| n.contains("source-"))
        {
            std::thread::sleep(std::time::Duration::from_micros(200));
        }
        now
    }

    fn wall(&self) -> i64 {
        self.0.wall()
    }

    fn idle_since(&self) -> u64 {
        self.0.idle_since()
    }

    fn sleep_until(&self, at: u64) -> impl Future<Output = ()> + Send + '_ {
        self.0.sleep_until(at)
    }

    fn settled(&self, at: u64) -> impl Future<Output = ()> + Send + '_ {
        self.0.settled(at)
    }
}

/// Four source runtimes, each on a thread of its own named `source-<i>`.
fn source_runtimes() -> (Runtimes, Vec<tokio::sync::oneshot::Sender<()>>) {
    let mut stops = Vec::new();
    let mut sources = Vec::new();
    for i in 0..4 {
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::Builder::new()
            .name(format!("source-{i}"))
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                tx.send(rt.handle().clone()).unwrap();
                rt.block_on(async {
                    let _ = stopped.await;
                });
            })
            .unwrap();
        sources.push(rx.recv().unwrap());
        stops.push(stop);
    }
    (Runtimes::assumed(sources, 1_024), stops)
}

/// The shell on a clock of `make`'s, its attempts on four runtimes of their
/// own threads, answers coming within microseconds of each other: no input
/// pushed below what was released, none taken out of order, no stall, every
/// line ended, and the fold finding the journal coherent.
fn stress<C: Clock>(make: impl Fn(u64) -> C, seeds: u64, calls: u64) {
    use ascetic_ddd_command_log::record::Action;
    use ascetic_ddd_load_run::sink::Written;
    use ascetic_ddd_scheduler::journal::{JOURNAL_VERSION, Reader};
    use ascetic_ddd_scheduler::machine::MACHINE_REVISION;
    use ascetic_ddd_scheduler::verdict::{Log, Verdict, Void, fold};

    // The live composition's runtimes, a source each.
    let live = ascetic_ddd_load_run::live::Live::start(4, 1_024).unwrap();
    let runtimes = live.runtimes().clone();
    let mut header = header();
    header.profile.sources = 4;
    header.profile.jitter = 2_000;
    let records: Vec<Record> = (0..calls)
        .map(|i| {
            let mut r = call("read", i + 1, 0, i * 25, 0);
            if let Action::Rest(rest) = &mut r.action {
                rest.source = (i % 4) as u32;
            }
            r
        })
        .collect();
    let log = String::from_utf8(bytes(header.clone(), &records)).unwrap();
    let identity = serde_json::from_str::<serde_json::Value>(log.lines().last().unwrap()).unwrap()
        ["trailer"]["sha256"]
        .as_str()
        .unwrap()
        .to_owned();
    for seed in 1..=seeds {
        let root = Key::root(Seed(seed)).child("stress");
        let quick: Arc<Script> = Arc::new(move |line, attempt, _| {
            let k = root.at(line).at(u64::from(attempt));
            let draw = |label: &str, m: u64| k.child(label).unit().raw() % m;
            sim::Attempt {
                after: None,
                sent_after: draw("sent", 100),
                answer_after: Some(draw("answer", 400)),
                judgement: match draw("judgement", 10) {
                    0 => Judgement::Unknown,
                    1 => Judgement::Unexpected,
                    _ => Judgement::Expected(vec![]),
                },
            }
        });
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let (bytes, held, kept) = rt.block_on(async {
            let clock = Arc::new(make(header.profile.jitter));
            let journal_header = journal::Header {
                journal: JOURNAL_VERSION,
                machine: MACHINE_REVISION,
                source: "load-run stress".to_owned(),
                log: identity.clone(),
                validator: ascetic_ddd_command_log::live::validator(),
                read_ahead: W,
                jitter: header.profile.jitter,
                catch_up: header.profile.reserve / 2,
                resend: header.profile.resend,
                namespace: 1,
                t0: Some(clock.wall()),
                gates: Vec::new(),
                judging: None,
                admission: None,
            };
            let sink = Written::assumed(Vec::new(), journal_header, 1_024).unwrap();
            let ran = run(
                ascetic_ddd_load_run::run::Ready::assumed(
                    admit(
                        ascetic_ddd_load_run::source::Plan::assumed(
                            Arc::new(header.clone()),
                            Default::default(),
                        ),
                        W,
                        Arc::new(Scripted::new(quick)),
                        &runtimes,
                        Journal::new(64, 0),
                    )
                    .unwrap(),
                    clock,
                    sink,
                    ascetic_ddd_command_log::fills::Namespace(1),
                ),
                Records::new(records.clone().into_iter()),
            )
            .await
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
            (ran.journal.0, ran.held, ran.kept)
        });
        assert_eq!(held, kept, "seed {seed}: cells left behind");
        let open = || Reader::new(std::io::Cursor::new(bytes.clone())).unwrap();
        let journal_header = open().header().clone();
        let log = Log {
            sha256: identity.clone(),
            records: records.len() as u64,
        };
        let folded = fold(&journal_header, &log, open(), open());
        assert!(
            !matches!(
                folded.verdict,
                Verdict::Void(Void::Incoherent { .. } | Void::Fault { .. })
            ),
            "seed {seed}: {:?}",
            folded.verdict
        );
        assert_eq!(
            folded.classes.len(),
            records.len(),
            "seed {seed}: every line ended"
        );
    }
}

#[test]
fn on_the_real_clock_and_four_threads_nothing_is_pushed_below_the_frontier() {
    use ascetic_ddd_load_run::clock::RealClock;
    stress(|j| RealClock::start(j).unwrap(), 8, 400);
}

#[test]
fn a_stamp_read_slowly_under_the_lock_is_never_stale() {
    use ascetic_ddd_load_run::clock::RealClock;
    stress(|j| Sluggish(RealClock::start(j).unwrap()), 3, 200);
}

/// Every stamp the journal carries, in its order.
fn stamps(lines: &[journal::Line]) -> Vec<u64> {
    lines
        .iter()
        .filter_map(|l| match l {
            journal::Line::Read(x) => Some(x.at),
            journal::Line::Released(x) => Some(x.at),
            journal::Line::Dispatch(x) => Some(x.at),
            journal::Line::Sent(x) => Some(x.at),
            journal::Line::Answer(x) => Some(x.at),
            journal::Line::End(x) => Some(x.at),
            journal::Line::Fault(x) => Some(x.at),
            journal::Line::Header(_) | journal::Line::Trailer(_) => None,
        })
        .collect()
}

/// An answer judged for 50 µs holds the machine's own work: a line due
/// meanwhile is read once the answer is taken, at the instant it then is —
/// never before an input stamped earlier.
#[test]
fn an_answer_being_judged_holds_the_machine_s_own_work() {
    let records = vec![call("read", 1, 0, 0, 0), call("read", 2, 0, 120, 0)];
    let quick: Arc<Script> = Arc::new(|_, _, _| sim::Attempt {
        after: None,
        sent_after: 0,
        answer_after: Some(100),
        judgement: Judgement::Expected(vec![]),
    });
    let carrier = Scripted {
        script: quick,
        judging: Arc::new(|line, _| if line == 2 { 50 } else { 0 }),
    };
    let ran = shell(&header(), &records, 0, carrier).unwrap();
    let answer = ran
        .journal
        .iter()
        .position(|l| matches!(l, journal::Line::Answer(a) if a.line == 2));
    let read = ran
        .journal
        .iter()
        .position(|l| matches!(l, journal::Line::Read(r) if r.line == 3));
    assert!(answer < read, "{:?}", ran.journal);
    let read_at = ran.journal.iter().find_map(|l| match l {
        journal::Line::Read(r) if r.line == 3 => Some(r.at),
        _ => None,
    });
    assert_eq!(read_at, Some(150));
    let s = stamps(&ran.journal);
    assert!(s.windows(2).all(|w| w[0] <= w[1]), "{s:?}");
}

/// Judging that takes time, under every script of the differential on the
/// corpus: no input taken below the last, the stamps of every journal
/// nondecreasing.
#[test]
fn judging_that_takes_time_never_reorders_the_machine_s_inputs() {
    let mut runs = 0;
    for (name, records) in fixture::corpus::logs() {
        let header = fixture::corpus::corpus_header();
        let (report, _) = run_bounded(header.clone(), &records, &NoHost, 1_000);
        if !report.violations.is_empty() {
            continue;
        }
        for seed in 1..=2u64 {
            let root = Key::root(Seed(seed)).child("judging").child(&name);
            let carrier = Scripted {
                script: Arc::new(script(seed, &name)),
                judging: Arc::new(move |line, attempt| {
                    root.at(line).at(u64::from(attempt)).unit().raw() % 400
                }),
            };
            let ran = shell(&header, &records, W, carrier)
                .unwrap_or_else(|e| panic!("{name}/{seed}: {e}"));
            let s = stamps(&ran.journal);
            assert!(s.windows(2).all(|w| w[0] <= w[1]), "{name}/{seed}");
            runs += 1;
        }
    }
    assert!(runs > 50, "{runs} runs");
}

/// A carrier that reports its answer arrived after `arrive` µs, then judges
/// for `judging` µs of its clock — forever, if none — or blocks its thread
/// for `block` real µs before ending.
struct Judging {
    arrive: u64,
    judging: Option<u64>,
    block: u64,
}

impl<C: Clock> Carrier<C> for Judging {
    fn carries(&self, _: &Header) -> Result<(), String> {
        Ok(())
    }

    fn needs(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
        _: u32,
    ) -> Result<ascetic_ddd_load_run::carrier::Needs, String> {
        Ok(ascetic_ddd_load_run::carrier::Needs::Nothing)
    }

    fn source_costs(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
        _: u32,
    ) -> Result<ascetic_ddd_load_run::carrier::SourceCosts, String> {
        Ok(ascetic_ddd_load_run::carrier::SourceCosts::NOTHING)
    }

    fn attempts(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
    ) -> Result<ascetic_ddd_load_run::carrier::AttemptCosts, ascetic_ddd_load_run::carrier::Unstated>
    {
        Ok(ascetic_ddd_load_run::carrier::AttemptCosts::NOTHING)
    }

    fn gate_bytes(&self) -> ascetic_ddd_load_run::stand::GateBytes {
        ascetic_ddd_load_run::stand::GateBytes::NONE
    }

    fn probe(
        &self,
        _: u32,
        _: std::sync::Arc<C>,
        _: u64,
    ) -> impl std::future::Future<Output = Vec<ascetic_ddd_load_run::carrier::Unprobed>> + Send + 'static
    {
        std::future::ready(Vec::new())
    }

    fn carry(&self, mut a: Attempt<C>) -> impl Future<Output = ()> + Send + 'static {
        let (arrive, judging, block) = (self.arrive, self.judging, self.block);
        async move {
            a.sent();
            let at = a.dispatched() + arrive;
            a.clock().sleep_until(at).await;
            a.arrived();
            if block > 0 {
                #[expect(
                    clippy::disallowed_methods,
                    reason = "a carrier stuck on its thread, on purpose"
                )]
                std::thread::sleep(std::time::Duration::from_micros(block));
            }
            match judging {
                Some(j) => {
                    let until = a.clock().now() + j;
                    a.clock().sleep_until(until).await;
                    a.ended(Judgement::Expected(vec![]), Seen::default());
                }
                None => std::future::pending::<()>().await,
            }
        }
    }
}

/// Decision 17: an answer judged for the route timeout, still not ended, is
/// a harness fault at exactly the oldest hold plus the route timeout — not
/// the virtual clock's idle panic, not a hang; one judged a microsecond less
/// ends the run cleanly.
#[test]
fn a_hold_past_the_route_timeout_is_a_harness_fault() {
    let mut h = header();
    h.profile.route_timeout = 1_000_000;
    let records = vec![call("read", 1, 0, 0, 0), call("read", 2, 0, 5, 0)];
    let never = Judging {
        arrive: 10,
        judging: None,
        block: 0,
    };
    let (stop, _) = stopped(shell(&h, &records, W, never).unwrap());
    assert!(
        matches!(
            stop,
            Stop::Harness(Harness::Held {
                line: 2,
                attempt: 1,
                since: 10,
                at: 1_000_010
            })
        ),
        "{stop:?}"
    );
    let just = Judging {
        arrive: 10,
        judging: Some(999_999),
        block: 0,
    };
    assert!(matches!(
        shell(&h, &records, W, just).unwrap().end,
        RunEnd::Ended
    ));
}

/// Decision 17, on the real clock: a carrier stuck on its source's thread —
/// its judging synchronous, the virtual clock blind to it — is caught by the
/// machine's own loop, on its own thread, past the route timeout.
#[test]
fn a_carrier_stuck_on_its_thread_is_a_harness_fault() {
    use ascetic_ddd_load_run::clock::RealClock;
    let (runtimes, stops) = source_runtimes();
    let mut h = header();
    h.profile.route_timeout = 20_000;
    let records = [call("read", 1, 0, 0, 0)];
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let err = rt.block_on(async {
        let clock = Arc::new(RealClock::start(2_000).unwrap());
        run(
            ascetic_ddd_load_run::run::Ready::assumed(
                admit(
                    ascetic_ddd_load_run::source::Plan::assumed(
                        Arc::new(h.clone()),
                        Default::default(),
                    ),
                    W,
                    Arc::new(Judging {
                        arrive: 0,
                        judging: Some(0),
                        block: 200_000,
                    }),
                    &Runtimes::assumed(vec![runtimes.sources()[0].clone()], 1_024),
                    Journal::new(64, 0),
                )
                .unwrap(),
                clock,
                Lines::new(),
                ascetic_ddd_command_log::fills::Namespace(1),
            ),
            Records::new(records.iter().cloned()),
        )
        .await
        .unwrap()
    });
    match stopped(err).0 {
        Stop::Harness(Harness::Held { since, at, .. }) => {
            assert!(at >= since + 20_000, "{since} {at}")
        }
        other => panic!("{other:?}"),
    }
    for stop in stops {
        let _ = stop.send(());
    }
}

/// Decision 17, on the real clock: an attempt handed to its task late — the
/// task before it on its source's runtime blocking the thread 5 ms — carries
/// that lateness in its `Sent`'s `handed`: the harness's, from the dispatch.
#[test]
fn a_late_hand_off_is_journaled() {
    use ascetic_ddd_load_run::clock::RealClock;
    let (runtimes, stops) = source_runtimes();
    let records = [call("read", 1, 0, 0, 0), call("read", 2, 0, 0, 0)];
    struct Blocking;
    impl<C: Clock> Carrier<C> for Blocking {
        fn carries(&self, _: &Header) -> Result<(), String> {
            Ok(())
        }

        fn needs(
            &self,
            _: &ascetic_ddd_command_log::validate::Peaks,
            _: u32,
        ) -> Result<ascetic_ddd_load_run::carrier::Needs, String> {
            Ok(ascetic_ddd_load_run::carrier::Needs::Nothing)
        }

        fn source_costs(
            &self,
            _: &ascetic_ddd_command_log::validate::Peaks,
            _: u32,
        ) -> Result<ascetic_ddd_load_run::carrier::SourceCosts, String> {
            Ok(ascetic_ddd_load_run::carrier::SourceCosts::NOTHING)
        }

        fn attempts(
            &self,
            _: &ascetic_ddd_command_log::validate::Peaks,
        ) -> Result<
            ascetic_ddd_load_run::carrier::AttemptCosts,
            ascetic_ddd_load_run::carrier::Unstated,
        > {
            Ok(ascetic_ddd_load_run::carrier::AttemptCosts::NOTHING)
        }

        fn gate_bytes(&self) -> ascetic_ddd_load_run::stand::GateBytes {
            ascetic_ddd_load_run::stand::GateBytes::NONE
        }

        fn probe(
            &self,
            _: u32,
            _: std::sync::Arc<C>,
            _: u64,
        ) -> impl std::future::Future<Output = Vec<ascetic_ddd_load_run::carrier::Unprobed>>
        + Send
        + 'static {
            std::future::ready(Vec::new())
        }
        #[expect(
            clippy::manual_async_fn,
            reason = "the task must not borrow its carrier: it is 'static"
        )]
        fn carry(&self, mut a: Attempt<C>) -> impl Future<Output = ()> + Send + 'static {
            async move {
                if a.line() == 2 {
                    #[expect(
                        clippy::disallowed_methods,
                        reason = "a task holding its thread, on purpose"
                    )]
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                a.sent();
                a.arrived();
                a.ended(Judgement::Expected(vec![]), Seen::default());
            }
        }
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let journal = rt.block_on(async {
        let clock = Arc::new(RealClock::start(2_000).unwrap());
        run(
            ascetic_ddd_load_run::run::Ready::assumed(
                admit(
                    ascetic_ddd_load_run::source::Plan::assumed(
                        Arc::new(header()),
                        Default::default(),
                    ),
                    W,
                    Arc::new(Blocking),
                    &Runtimes::assumed(vec![runtimes.sources()[0].clone()], 1_024),
                    Journal::new(64, 0),
                )
                .unwrap(),
                clock,
                Lines::new(),
                ascetic_ddd_command_log::fills::Namespace(1),
            ),
            Records::new(records.iter().cloned()),
        )
        .await
        .unwrap()
        .journal
    });
    let dispatched = journal.iter().find_map(|l| match l {
        journal::Line::Dispatch(d) if d.line == 3 => Some(d.at),
        _ => None,
    });
    let handed = journal.iter().find_map(|l| match l {
        journal::Line::Sent(s) if s.line == 3 => Some(s.handed),
        _ => None,
    });
    let (d, h) = (dispatched.unwrap(), handed.unwrap());
    assert!(h >= d + 5_000, "handed at {h}, dispatched at {d}");
    for stop in stops {
        let _ = stop.send(());
    }
}

/// Decision 17: a source on a runtime that is not `current_thread` is refused
/// before T0 — its driver may turn while another worker runs a task, and no
/// look of an attempt could verify a turn.
#[test]
fn a_source_not_on_a_current_thread_runtime_is_refused() {
    let pool = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .build()
        .unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let err = rt.block_on(async {
        admit::<VirtualClock, _>(
            ascetic_ddd_load_run::source::Plan::assumed(Arc::new(header()), Default::default()),
            W,
            Arc::new(Scripted::new(Arc::new(script(1, "flavor")))),
            &Runtimes::assumed(vec![pool.handle().clone()], 1_024),
            Journal::new(64, 0),
        )
        .err()
        .unwrap()
    });
    assert!(
        matches!(
            err,
            RunError::Inadmissible(ascetic_ddd_load_run::run::Inadmissible::Flavour { .. })
        ),
        "{err:?}"
    );
}

/// Decision 18: every fault's class in the journal — numbers and dotted
/// names, a carrier's detail left out.
#[test]
fn every_fault_has_its_class() {
    use ascetic_ddd_command_log::LogError;
    use ascetic_ddd_scheduler::frontier::Below;
    use journal::{Code, FaultClass as F, LogFault as L};
    let harness = [
        (
            Harness::Below(Below { at: 1, released: 2 }),
            F::Below {
                stamp: 1,
                released: 2,
            },
        ),
        (
            Harness::Dropped {
                line: 3,
                attempt: 4,
            },
            F::Dropped {
                line: 3,
                attempt: 4,
            },
        ),
        (Harness::Poisoned, F::Poisoned),
        (Harness::Stalled { at: 5 }, F::Stalled),
        (
            Harness::Carrier {
                line: 6,
                attempt: 7,
                cannot: "rest.route",
                detail: "the host's own words".to_owned(),
            },
            F::Carrier {
                line: 6,
                attempt: 7,
                cannot: Code::Named("rest.route".to_owned()),
            },
        ),
        (Harness::Saturated { alive: 8 }, F::Saturated { alive: 8 }),
        (
            Harness::Held {
                line: 9,
                attempt: 10,
                since: 11,
                at: 12,
            },
            F::Held {
                line: 9,
                attempt: 10,
                since: 11,
            },
        ),
        (
            Harness::Backdated { at: 13, now: 14 },
            F::Backdated { stamp: 13, now: 14 },
        ),
        (
            Harness::Disordered { at: 15, last: 16 },
            F::Disordered {
                stamp: 15,
                last: 16,
            },
        ),
    ];
    for (h, class) in harness {
        assert_eq!(Stop::Harness(h.clone()).class(), class, "{h:?}");
    }
    let logs = [
        (LogError::Io(std::io::Error::other("x")), L::Io),
        (
            LogError::Space {
                path: "/".into(),
                needed: 1,
                free: 0,
            },
            L::Io,
        ),
        (
            LogError::Hot {
                part: "x",
                held: 1,
                line: 2,
            },
            L::Io,
        ),
        (LogError::Truncated, L::Truncated),
        (
            LogError::Diverged {
                what: "x".to_owned(),
            },
            L::Diverged,
        ),
        (LogError::Encode("x".to_owned()), L::Malformed),
        (
            LogError::Malformed {
                line: 1,
                reason: "x".to_owned(),
            },
            L::Malformed,
        ),
        (LogError::NotCanonical { line: 1 }, L::Malformed),
        (LogError::Header { line: 1 }, L::Malformed),
        (LogError::Format(9), L::Malformed),
        (LogError::AfterTrailer, L::Malformed),
    ];
    for (e, kind) in logs {
        assert_eq!(Stop::Log(e).class(), F::Log(kind));
    }
}

/// The log's records, then its error in place of its end.
struct Failing {
    records: std::vec::IntoIter<Record>,
    error: Option<ascetic_ddd_command_log::LogError>,
}

impl LineSource for Failing {
    fn poll_next(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<Option<Arc<Record>>, ascetic_ddd_command_log::LogError>> {
        std::task::Poll::Ready(match self.records.next() {
            Some(r) => Ok(Some(Arc::new(r))),
            None => match self.error.take() {
                Some(e) => Err(e),
                None => Ok(None),
            },
        })
    }
}

/// Decision 18: a log failing mid-run stops it — the lines taken, its fault,
/// the trailer; no task left running.
#[test]
fn a_log_failing_mid_run_stops_it() {
    use ascetic_ddd_command_log::LogError;
    let records = vec![call("read", 1, 0, 0, 0), call("read", 2, 0, 50_000, 0)];
    for (error, kind) in [
        (LogError::Truncated, journal::LogFault::Truncated),
        (
            LogError::Diverged {
                what: "chunk 2".to_owned(),
            },
            journal::LogFault::Diverged,
        ),
    ] {
        let source = Failing {
            records: records.clone().into_iter(),
            error: Some(error),
        };
        let quick: Arc<Script> = Arc::new(|_, _, _| sim::Attempt {
            after: None,
            sent_after: 5,
            answer_after: Some(20),
            judgement: Judgement::Expected(vec![]),
        });
        let ran = shell_from(&header(), source, W, Scripted::new(quick)).unwrap();
        assert_eq!(ran.running, 0);
        let (stop, journal) = stopped(ran);
        assert_eq!(stop.class(), journal::FaultClass::Log(kind));
        // Line 2 ended; line 3 read from the log at its instant, the log
        // failing on the read after it, before the machine took it.
        assert!(
            journal
                .iter()
                .any(|l| matches!(l, journal::Line::End(e) if e.line == 2))
        );
        assert!(matches!(
            journal.last(),
            Some(journal::Line::Fault(f)) if f.at == 50_000 - W
        ));
    }
}

/// A clock whose next reading on a thread armed for it panics: under the
/// frontier's lock, it poisons it.
struct Poisoning(Arc<VirtualClock>);

thread_local! {
    static ARMED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

impl Clock for Poisoning {
    fn now(&self) -> u64 {
        if ARMED.with(|a| a.replace(false)) {
            panic!("a clock's reading under the lock");
        }
        self.0.now()
    }

    fn wall(&self) -> i64 {
        self.0.wall()
    }

    fn idle_since(&self) -> u64 {
        self.0.idle_since()
    }

    fn sleep_until(&self, at: u64) -> impl Future<Output = ()> + Send + '_ {
        self.0.sleep_until(at)
    }

    fn settled(&self, at: u64) -> impl Future<Output = ()> + Send + '_ {
        self.0.settled(at)
    }
}

/// Reports its send on a thread armed to panic under the lock.
struct Poisoner;

impl Carrier<Poisoning> for Poisoner {
    fn carries(&self, _: &Header) -> Result<(), String> {
        Ok(())
    }

    fn needs(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
        _: u32,
    ) -> Result<ascetic_ddd_load_run::carrier::Needs, String> {
        Ok(ascetic_ddd_load_run::carrier::Needs::Nothing)
    }

    fn source_costs(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
        _: u32,
    ) -> Result<ascetic_ddd_load_run::carrier::SourceCosts, String> {
        Ok(ascetic_ddd_load_run::carrier::SourceCosts::NOTHING)
    }

    fn attempts(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
    ) -> Result<ascetic_ddd_load_run::carrier::AttemptCosts, ascetic_ddd_load_run::carrier::Unstated>
    {
        Ok(ascetic_ddd_load_run::carrier::AttemptCosts::NOTHING)
    }

    fn gate_bytes(&self) -> ascetic_ddd_load_run::stand::GateBytes {
        ascetic_ddd_load_run::stand::GateBytes::NONE
    }

    fn probe(
        &self,
        _: u32,
        _: std::sync::Arc<Poisoning>,
        _: u64,
    ) -> impl std::future::Future<Output = Vec<ascetic_ddd_load_run::carrier::Unprobed>> + Send + 'static
    {
        std::future::ready(Vec::new())
    }

    #[expect(
        clippy::manual_async_fn,
        reason = "the task must not borrow its carrier: it is 'static"
    )]
    fn carry(&self, a: Attempt<Poisoning>) -> impl Future<Output = ()> + Send + 'static {
        async move {
            ARMED.with(|x| x.set(true));
            a.sent();
            a.unanswered();
        }
    }
}

/// Decision 18: a lock poisoned by a panic under it stops the run, its class
/// the poison's, not the attempt the panic dropped.
#[test]
fn a_poisoned_lock_stops_the_run() {
    let records = vec![call("read", 1, 0, 0, 0)];
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let ran = rt.block_on(async {
        let virtual_clock = Arc::new(VirtualClock::start(0));
        let clock = Arc::new(Poisoning(virtual_clock.clone()));
        let runtimes = Runtimes::assumed(vec![tokio::runtime::Handle::current()], 1_024);
        virtual_clock
            .within(run(
                ascetic_ddd_load_run::run::Ready::assumed(
                    admit(
                        ascetic_ddd_load_run::source::Plan::assumed(
                            Arc::new(header()),
                            Default::default(),
                        ),
                        W,
                        Arc::new(Poisoner),
                        &runtimes,
                        Journal::new(64, 0),
                    )
                    .unwrap(),
                    clock,
                    Lines::new(),
                    ascetic_ddd_command_log::fills::Namespace(1),
                ),
                Records::new(records.into_iter()),
            ))
            .await
    });
    let (stop, _) = stopped(ran.unwrap());
    assert!(matches!(stop, Stop::Harness(Harness::Poisoned)), "{stop:?}");
}

/// Holds its attempt past its censor, then leaves it unanswered.
struct Lingering;

impl Carrier<VirtualClock> for Lingering {
    fn carries(&self, _: &Header) -> Result<(), String> {
        Ok(())
    }

    fn needs(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
        _: u32,
    ) -> Result<ascetic_ddd_load_run::carrier::Needs, String> {
        Ok(ascetic_ddd_load_run::carrier::Needs::Nothing)
    }

    fn source_costs(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
        _: u32,
    ) -> Result<ascetic_ddd_load_run::carrier::SourceCosts, String> {
        Ok(ascetic_ddd_load_run::carrier::SourceCosts::NOTHING)
    }

    fn attempts(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
    ) -> Result<ascetic_ddd_load_run::carrier::AttemptCosts, ascetic_ddd_load_run::carrier::Unstated>
    {
        Ok(ascetic_ddd_load_run::carrier::AttemptCosts::NOTHING)
    }

    fn gate_bytes(&self) -> ascetic_ddd_load_run::stand::GateBytes {
        ascetic_ddd_load_run::stand::GateBytes::NONE
    }

    fn probe(
        &self,
        _: u32,
        _: std::sync::Arc<VirtualClock>,
        _: u64,
    ) -> impl std::future::Future<Output = Vec<ascetic_ddd_load_run::carrier::Unprobed>> + Send + 'static
    {
        std::future::ready(Vec::new())
    }

    #[expect(
        clippy::manual_async_fn,
        reason = "the task must not borrow its carrier: it is 'static"
    )]
    fn carry(&self, a: Attempt<VirtualClock>) -> impl Future<Output = ()> + Send + 'static {
        async move {
            let until = a.deadline() + 60_000_000;
            a.clock().sleep_until(until).await;
            a.unanswered();
        }
    }
}

/// Decision 19: an attempt outliving its deadline by the route timeout — a
/// source that cannot keep up — stops the run there, by age, not count; the
/// stop aborts them, none left running. Thousands alive at one deadline,
/// each leaving at its last look, are no saturation.
#[test]
fn a_saturated_source_stops_the_run() {
    let mut h = header();
    h.operations.iter_mut().for_each(|o| o.timeout = 1_000);
    // Three at once, a fourth five seconds later: the oldest ages first.
    let mut records: Vec<Record> = (0..3).map(|i| call("read", i + 1, 0, 0, 0)).collect();
    records.push(call("read", 4, 0, 5_000_000, 0));
    let ran = shell(&h, &records, W, Lingering).unwrap();
    assert_eq!(ran.running, 0);
    let route_timeout = h.profile.route_timeout;
    let (stop, journal) = stopped(ran);
    assert!(
        matches!(stop, Stop::Harness(Harness::Saturated { alive: 4 })),
        "{stop:?}"
    );
    // The first attempt's deadline, its dispatch's plus its timeout, then
    // the route timeout: not before, nor long after.
    let dispatched = journal
        .iter()
        .find_map(|l| match l {
            journal::Line::Dispatch(d) => Some(d.at),
            _ => None,
        })
        .unwrap();
    let Some(journal::Line::Fault(f)) = journal.last() else {
        panic!("{:?}", journal.last())
    };
    // Its fault stamped once the stop's aborted tasks went: a step of the
    // quiesce's later.
    let saturated = dispatched + 1_000 + route_timeout;
    assert!(
        (saturated..=saturated + 1_000).contains(&f.at),
        "stopped at {}, saturated at {saturated}",
        f.at
    );
    // Five thousand attempts sharing a deadline, each leaving a microsecond
    // past it: no saturation.
    let records: Vec<Record> = (0..5_000).map(|i| call("read", i + 1, 0, 0, 0)).collect();
    let ran = shell(&h, &records, W, Leaving).unwrap();
    assert!(matches!(ran.end, RunEnd::Ended), "{:?}", ran.journal.last());
    // Each closing report counted by where it stood (decision 20).
    assert_eq!(
        ran.unsent,
        std::collections::BTreeMap::from([("sim.unsent".to_owned(), 5_000)])
    );
}

/// Leaves each attempt a microsecond past its deadline, unanswered.
struct Leaving;

impl Carrier<VirtualClock> for Leaving {
    fn carries(&self, _: &Header) -> Result<(), String> {
        Ok(())
    }

    fn needs(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
        _: u32,
    ) -> Result<ascetic_ddd_load_run::carrier::Needs, String> {
        Ok(ascetic_ddd_load_run::carrier::Needs::Nothing)
    }

    fn source_costs(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
        _: u32,
    ) -> Result<ascetic_ddd_load_run::carrier::SourceCosts, String> {
        Ok(ascetic_ddd_load_run::carrier::SourceCosts::NOTHING)
    }

    fn attempts(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
    ) -> Result<ascetic_ddd_load_run::carrier::AttemptCosts, ascetic_ddd_load_run::carrier::Unstated>
    {
        Ok(ascetic_ddd_load_run::carrier::AttemptCosts::NOTHING)
    }

    fn gate_bytes(&self) -> ascetic_ddd_load_run::stand::GateBytes {
        ascetic_ddd_load_run::stand::GateBytes::NONE
    }

    fn probe(
        &self,
        _: u32,
        _: std::sync::Arc<VirtualClock>,
        _: u64,
    ) -> impl std::future::Future<Output = Vec<ascetic_ddd_load_run::carrier::Unprobed>> + Send + 'static
    {
        std::future::ready(Vec::new())
    }

    #[expect(
        clippy::manual_async_fn,
        reason = "the task must not borrow its carrier: it is 'static"
    )]
    fn carry(&self, a: Attempt<VirtualClock>) -> impl Future<Output = ()> + Send + 'static {
        async move {
            a.clock().sleep_until(a.deadline() + 1).await;
            let report = sim::drawn_unsent(a.line(), a.attempt(), a.dispatched());
            a.left(report);
        }
    }
}

/// A record sent from a source with no runtime — a log no validator read —
/// stops the run at its dispatch, a carrier fault named, never a refusal
/// after the journal began.
#[test]
fn a_source_with_no_runtime_stops_the_run_at_its_dispatch() {
    let records = vec![call("read", 1, 0, 0, 1)];
    let quick: Arc<Script> = Arc::new(|_, _, _| sim::Attempt {
        after: None,
        sent_after: 5,
        answer_after: Some(20),
        judgement: Judgement::Expected(vec![]),
    });
    let (stop, _) = stopped(shell(&header(), &records, W, Scripted::new(quick)).unwrap());
    assert!(
        matches!(
            stop,
            Stop::Harness(Harness::Carrier {
                line: 2,
                attempt: 1,
                cannot: "run.source",
                ..
            })
        ),
        "{stop:?}"
    );
}

/// Reports its request connected twice, never handed back between.
struct Twice;

impl Carrier<VirtualClock> for Twice {
    fn carries(&self, _: &Header) -> Result<(), String> {
        Ok(())
    }

    fn needs(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
        _: u32,
    ) -> Result<ascetic_ddd_load_run::carrier::Needs, String> {
        Ok(ascetic_ddd_load_run::carrier::Needs::Nothing)
    }

    fn source_costs(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
        _: u32,
    ) -> Result<ascetic_ddd_load_run::carrier::SourceCosts, String> {
        Ok(ascetic_ddd_load_run::carrier::SourceCosts::NOTHING)
    }

    fn attempts(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
    ) -> Result<ascetic_ddd_load_run::carrier::AttemptCosts, ascetic_ddd_load_run::carrier::Unstated>
    {
        Ok(ascetic_ddd_load_run::carrier::AttemptCosts::NOTHING)
    }

    fn gate_bytes(&self) -> ascetic_ddd_load_run::stand::GateBytes {
        ascetic_ddd_load_run::stand::GateBytes::NONE
    }

    fn probe(
        &self,
        _: u32,
        _: std::sync::Arc<VirtualClock>,
        _: u64,
    ) -> impl std::future::Future<Output = Vec<ascetic_ddd_load_run::carrier::Unprobed>> + Send + 'static
    {
        std::future::ready(Vec::new())
    }

    #[expect(
        clippy::manual_async_fn,
        reason = "the task must not borrow its carrier: it is 'static"
    )]
    fn carry(&self, mut a: Attempt<VirtualClock>) -> impl Future<Output = ()> + Send + 'static {
        async move {
            a.connected();
            a.connected();
            a.unanswered();
        }
    }
}

/// Decision 18: a send path is handed to a connection only from its attempt
/// — a request handed back is moved back first, or its lag would go
/// uncounted; a carrier that reports otherwise is a fault.
#[test]
fn a_path_connected_off_its_attempt_is_a_fault() {
    let records = vec![call("read", 1, 0, 0, 0)];
    let (stop, _) = stopped(shell(&header(), &records, W, Twice).unwrap());
    assert!(
        matches!(
            stop,
            Stop::Harness(Harness::Carrier {
                cannot: "path.connected",
                ..
            })
        ),
        "{stop:?}"
    );
}

/// A read capturing its group's ETag, answered at once as `taken` says.
fn capturing(taken: impl Fn(&Record) -> Vec<Captured> + Send + Sync + 'static) -> Scripted {
    Scripted::new(Arc::new(move |_, _, r: &Record| sim::Attempt {
        after: None,
        sent_after: 5,
        answer_after: Some(20),
        judgement: Judgement::Expected(taken(r)),
    }))
}

/// The `i`th capture spec of `r`, taking `value`.
fn of_spec(r: &Record, i: usize, value: Vec<u8>) -> Captured {
    Captured {
        subject: r.capture[i].subject.clone(),
        slot: r.capture[i].slot.clone(),
        value,
    }
}

/// K2b-7e-2b-2b-3: what a carrier hands the shell is held where it enters —
/// a capture no spec of its step names, one past the specs of its subject
/// and slot, a value whose buffer is past `VALUE`, a value no take makes
/// (K2b-7e-2b-2c-3): each a carrier fault named, the run stopped, its answer
/// never journaled.
#[test]
fn a_carrier_s_captures_are_held_where_they_enter() {
    use ascetic_ddd_scheduler::fits::VALUE;
    let value = VALUE as usize;
    let records = vec![captures_etag(call("read", 1, 0, 0, 0), 1)];
    type Taken = Box<dyn Fn(&Record) -> Vec<Captured> + Send + Sync>;
    let cases: [(&str, Taken); 5] = [
        (
            "shell.capture.not_taken",
            Box::new(|r: &Record| vec![of_spec(r, 0, b" [[[1]]]".to_vec())]),
        ),
        (
            "shell.capture.not_taken",
            Box::new(|r: &Record| vec![of_spec(r, 0, b"\"\\u0000\"".to_vec())]),
        ),
        (
            "shell.capture.unspecified",
            Box::new(|r: &Record| {
                vec![Captured {
                    slot: "nope".into(),
                    ..of_spec(r, 0, b"\"v\"".to_vec())
                }]
            }),
        ),
        (
            "shell.capture.repeated",
            Box::new(|r: &Record| {
                vec![
                    of_spec(r, 0, b"\"v\"".to_vec()),
                    of_spec(r, 0, b"\"w\"".to_vec()),
                ]
            }),
        ),
        (
            "shell.capture.too_large",
            Box::new(move |r: &Record| {
                let mut small = Vec::with_capacity(value + 1);
                small.extend_from_slice(b"\"v\"");
                vec![of_spec(r, 0, small)]
            }),
        ),
    ];
    for (cannot, taken) in cases {
        let ran = shell(&header(), &records, W, capturing(taken)).unwrap();
        let (stop, journal) = stopped(ran);
        assert!(
            matches!(&stop, Stop::Harness(Harness::Carrier { line: 2, attempt: 1, cannot: c, .. }) if *c == cannot),
            "{cannot}: {stop:?}"
        );
        assert!(
            !journal
                .iter()
                .any(|l| matches!(l, journal::Line::Answer(a) if a.line == 2)),
            "{cannot}: its answer journaled"
        );
    }
}

/// A carrier's captures within the specs are taken: in any order, two of one
/// subject and slot where two specs name them, a value's buffer of exactly
/// `VALUE`.
#[test]
fn a_carrier_s_captures_within_the_specs_are_taken() {
    use ascetic_ddd_scheduler::fits::VALUE;
    let value = VALUE as usize;
    // Two specs of one subject and slot, and one of another group.
    let mut r = captures_etag(captures_etag(call("read", 1, 0, 0, 0), 1), 2);
    r.capture.push(r.capture[0].clone());
    let records = vec![r];
    let taken = move |r: &Record| {
        let mut widest = Vec::with_capacity(value);
        widest.extend_from_slice(b"\"v\"");
        vec![
            of_spec(r, 1, b"\"w\"".to_vec()),
            of_spec(r, 2, widest),
            of_spec(r, 0, b"\"x\"".to_vec()),
        ]
    };
    let ran = shell(&header(), &records, W, capturing(taken)).unwrap();
    assert!(matches!(ran.end, RunEnd::Ended), "{:?}", ran.end);
    let captured = ran
        .journal
        .iter()
        .find_map(|l| match l {
            journal::Line::Answer(a) if a.line == 2 => Some(a.captured.len()),
            _ => None,
        })
        .expect("its answer journaled");
    assert_eq!(captured, 3);
}

/// Leaves each attempt a microsecond past its deadline, its closing report
/// under a code of its own line's: a carrier minting a code an attempt.
struct Minting;

impl Carrier<VirtualClock> for Minting {
    fn carries(&self, _: &Header) -> Result<(), String> {
        Ok(())
    }

    fn needs(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
        _: u32,
    ) -> Result<ascetic_ddd_load_run::carrier::Needs, String> {
        Ok(ascetic_ddd_load_run::carrier::Needs::Nothing)
    }

    fn source_costs(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
        _: u32,
    ) -> Result<ascetic_ddd_load_run::carrier::SourceCosts, String> {
        Ok(ascetic_ddd_load_run::carrier::SourceCosts::NOTHING)
    }

    fn attempts(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
    ) -> Result<ascetic_ddd_load_run::carrier::AttemptCosts, ascetic_ddd_load_run::carrier::Unstated>
    {
        Ok(ascetic_ddd_load_run::carrier::AttemptCosts::NOTHING)
    }

    fn gate_bytes(&self) -> ascetic_ddd_load_run::stand::GateBytes {
        ascetic_ddd_load_run::stand::GateBytes::NONE
    }

    fn probe(
        &self,
        _: u32,
        _: std::sync::Arc<VirtualClock>,
        _: u64,
    ) -> impl std::future::Future<Output = Vec<ascetic_ddd_load_run::carrier::Unprobed>> + Send + 'static
    {
        std::future::ready(Vec::new())
    }

    #[expect(
        clippy::manual_async_fn,
        reason = "the task must not borrow its carrier: it is 'static"
    )]
    fn carry(&self, a: Attempt<VirtualClock>) -> impl Future<Output = ()> + Send + 'static {
        async move {
            a.clock().sleep_until(a.deadline() + 1).await;
            let report = ascetic_ddd_scheduler::machine::Unsent {
                code: ascetic_ddd_scheduler::machine::Code::of(&format!("host.line_{}", a.line())),
                ..sim::drawn_unsent(a.line(), a.attempt(), a.dispatched())
            };
            a.left(report);
        }
    }
}

/// K2b-7e-2b-2b-3: a run names at most `UNSENT` codes of its closing
/// reports, a carrier minting more counted apart — every report counted.
#[test]
fn a_run_names_at_most_its_unsent_codes() {
    use ascetic_ddd_load_run::run::UNSENT;
    let n = UNSENT as u64 + 8;
    let records: Vec<Record> = (0..n).map(|i| call("read", i, 0, i * 10, 0)).collect();
    let ran = shell(&header(), &records, W, Minting).unwrap();
    let reports = ran
        .journal
        .iter()
        .filter(|l| matches!(l, journal::Line::Answer(a) if a.unsent.is_some()))
        .count() as u64;
    assert_eq!(reports, n);
    assert_eq!(ran.unsent.len(), UNSENT);
    assert_eq!(ran.unsent_past, 8);
    assert_eq!(ran.unsent.values().sum::<u64>() + ran.unsent_past, n);
}
