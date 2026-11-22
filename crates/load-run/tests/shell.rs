//! K2b-1 (ADR-0047 decision 16): the shell's loop, under the virtual clock and
//! a scripted carrier, journals exactly what `sim` journals — every clean
//! corpus log, under the scheduler differential's scripts, at three
//! read-aheads; a line read by the machine's own timer after an idle gap; an
//! attempt dropped unended, or its carrier panicking, a harness fault.

// The command log's fixture reads its corpus's directory.
#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use std::future::Future;
use std::sync::Arc;

use ascetic_ddd_command_log::record::{Header, Record};
use ascetic_ddd_command_log::validate::NoHost;
use ascetic_ddd_faker::{Key, Seed};
use ascetic_ddd_load_run::carrier::Carrier;
use ascetic_ddd_load_run::clock::{Clock, VirtualClock};
use ascetic_ddd_load_run::run::{Ran, RunError, Runtimes, run};
use ascetic_ddd_load_run::sink::Lines;
use ascetic_ddd_load_run::source::Records;
use ascetic_ddd_load_run::stamps::{Attempt, Harness};
use ascetic_ddd_scheduler::journal;
use ascetic_ddd_scheduler::machine::{Captured, Judgement, Seen};
use ascetic_ddd_scheduler::sim::{self, run_reading};
use fixture::*;

type Script = dyn Fn(u64, u32, &Record) -> sim::Attempt + Send + Sync;

/// `sim`'s scripted handler as a carrier: the same instants, the same
/// judgements, reported through the attempt — each judged in `judging` µs of
/// its clock, a floor held meanwhile (`sim` judges at once).
struct Scripted {
    script: Arc<Script>,
    judging: Arc<dyn Fn(u64, u32) -> u64 + Send + Sync>,
}

impl Scripted {
    fn new(script: Arc<Script>) -> Scripted {
        Scripted {
            script,
            judging: Arc::new(|_, _| 0),
        }
    }
}

impl<C: Clock> Carrier<C> for Scripted {
    fn carries(&self, _: &Header) -> Result<(), String> {
        Ok(())
    }

    fn carry(&self, mut a: Attempt<C>) -> impl Future<Output = ()> + Send + 'static {
        let script = self.script.clone();
        let judging = self.judging.clone();
        async move {
            let s = script(a.line(), a.attempt(), a.record());
            let judging = judging(a.line(), a.attempt());
            let sent = a.dispatched().saturating_add(s.sent_after);
            // An attempt whose bytes never left is answered by nobody.
            if sent >= a.deadline() {
                a.unanswered();
                return;
            }
            a.clock().sleep_until(sent).await;
            a.sent();
            match s.answer_after.map(|d| sent.saturating_add(d)) {
                Some(t) if t <= a.deadline() => {
                    a.clock().sleep_until(t).await;
                    a.arrived();
                    if judging > 0 {
                        let until = a.clock().now().saturating_add(judging);
                        a.clock().sleep_until(until).await;
                    }
                    let seen = Seen {
                        after: s.after,
                        ..Seen::default()
                    };
                    a.ended(s.judgement, seen);
                }
                _ => a.unanswered(),
            }
        }
    }
}

fn shell<X: Carrier<VirtualClock>>(
    header: &Header,
    records: &[Record],
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
        let runtimes = Runtimes {
            sources: vec![tokio::runtime::Handle::current(); sources],
        };
        let records = Records::new(records.iter().cloned());
        clock
            .within(run(
                Arc::new(header.clone()),
                read_ahead,
                clock.clone(),
                records,
                Lines::new(),
                Arc::new(carrier),
                &runtimes,
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
        let err = shell(&header(), &records, W, Forgetful { panics }).unwrap_err();
        assert!(
            matches!(
                err,
                RunError::Harness(Harness::Dropped {
                    line: 2,
                    attempt: 1
                })
            ),
            "{err:?}"
        );
    }
}

struct Refusing;

impl Carrier<VirtualClock> for Refusing {
    fn carries(&self, _: &Header) -> Result<(), String> {
        Err("no such transport".to_owned())
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
        matches!(err, RunError::Refused(ref why) if why == "no such transport"),
        "{err:?}"
    );
    let mut wide = header();
    wide.profile.sources = 3;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let err = rt.block_on(async {
        let clock = Arc::new(VirtualClock::start(0));
        let runtimes = Runtimes {
            sources: vec![tokio::runtime::Handle::current()],
        };
        run(
            Arc::new(wide),
            W,
            clock.clone(),
            Records::new(records.clone().into_iter()),
            Lines::new(),
            Arc::new(Scripted::new(Arc::new(script(1, "refused")))),
            &runtimes,
        )
        .await
        .unwrap_err()
    });
    assert!(matches!(err, RunError::Refused(_)), "{err:?}");
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
            .is_some_and(|n| n.starts_with("source-"))
        {
            std::thread::sleep(std::time::Duration::from_micros(200));
        }
        now
    }

    fn wall(&self) -> i64 {
        self.0.wall()
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
    (Runtimes { sources }, stops)
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

    let (runtimes, stops) = source_runtimes();
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
                resend: header.profile.resend,
                namespace: 1,
                t0: clock.wall(),
            };
            let sink = Written::start(Vec::new(), journal_header, 1_024).unwrap();
            let ran = run(
                Arc::new(header.clone()),
                W,
                clock,
                Records::new(records.clone().into_iter()),
                sink,
                Arc::new(Scripted::new(quick)),
                &runtimes,
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
    for stop in stops {
        let _ = stop.send(());
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
