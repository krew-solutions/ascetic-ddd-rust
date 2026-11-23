//! The stages before T0 (ADR-0047 decision 18): the judging time measured by
//! a stopwatch, the stand's gates in turn, a gate's refusal journaled.

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use std::collections::VecDeque;
use std::future::Future;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

use ascetic_ddd_command_log::fills::Namespace;
use ascetic_ddd_command_log::record::Header;
use ascetic_ddd_load_run::carrier::Carrier;
use ascetic_ddd_load_run::clock::{Clock, VirtualClock};
use ascetic_ddd_load_run::run::{Runtimes, admit};
use ascetic_ddd_load_run::sink::{Lines, Written};
use ascetic_ddd_load_run::stages::{
    self, Identity, RUNS, SHAPES, document, gates, journal_header, judging, measure, refused,
};
use ascetic_ddd_load_run::stamps::Attempt;
use ascetic_ddd_load_run::stand::{Opening, Refused};
use ascetic_ddd_scheduler::journal::{Code, FaultClass, Judging, Line, Reader, Shape};
use ascetic_ddd_scheduler::verdict::{Log, Verdict, Void, fold};
use fixture::*;

/// A stopwatch reading a script: each read the next value.
struct Scripted(Mutex<VecDeque<u64>>);

impl Clock for Scripted {
    fn now(&self) -> u64 {
        self.0
            .lock()
            .unwrap()
            .pop_front()
            .expect("a read past the script")
    }

    fn wall(&self) -> i64 {
        0
    }

    fn idle_since(&self) -> u64 {
        0
    }

    fn sleep_until(&self, _: u64) -> impl Future<Output = ()> + Send + '_ {
        std::future::ready(())
    }

    fn settled(&self, _: u64) -> impl Future<Output = ()> + Send + '_ {
        std::future::ready(())
    }
}

/// The reads a stopwatch gives when each shape's runs take `took`, in
/// [`SHAPES`]' order, the first of each a warm-up.
fn script(took: [[u64; RUNS + 1]; 4]) -> Scripted {
    let mut reads = VecDeque::new();
    let mut t = 0;
    for runs in took {
        for d in runs {
            reads.push_back(t);
            t += d;
            reads.push_back(t);
        }
    }
    Scripted(Mutex::new(reads))
}

/// The worst shape by its median of the runs after the warm-up — not by its
/// mean, its maximum, nor with its warm-up counted — and neither the first
/// nor the last shape measured.
#[test]
fn the_judging_time_is_the_worst_shape_s_median() {
    assert_eq!(SHAPES[2], Shape::Numbers);
    let mut h = header();
    h.profile.answer_cap = 4_096;
    let stopwatch = script([
        // Flat: median 3, mean and maximum the highest, its warm-up longest.
        [1_000, 1, 2, 3, 400, 500],
        [1, 10, 10, 10, 10, 10],
        // Numbers: the highest median.
        [0, 20, 21, 22, 23, 90],
        [500, 5, 5, 5, 5, 5],
    ]);
    assert_eq!(
        judging(&h, &stopwatch),
        Judging {
            cap: 4_096,
            took: 22,
            shape: Shape::Numbers,
        }
    );
    assert!(stopwatch.0.lock().unwrap().is_empty(), "every read taken");
}

/// Each shape's document fills its cap, at most, is JSON, and holds its last
/// member at its pointer: what judging it captures.
#[test]
fn each_shape_fills_its_cap_and_holds_its_last_member() {
    for cap in [64, 4_096, 65_536] {
        for shape in SHAPES {
            let (body, last) = document(shape, cap);
            assert!(
                body.len() <= cap && body.len() + 64 > cap,
                "{shape:?} at {cap}"
            );
            let json = ascetic_ddd_command_log::json::parse(&body).unwrap();
            assert!(
                json.at(&last).unwrap().is_some(),
                "{shape:?} at {cap}: {last}"
            );
        }
    }
}

/// A carrier whose stand's gates pass and refuse as scripted, each after a
/// wait of the virtual clock.
struct Gated {
    clock: Arc<VirtualClock>,
    gates: Vec<(u64, Result<String, Refused>)>,
}

impl Carrier<VirtualClock> for Gated {
    fn carries(&self, _: &Header) -> Result<(), String> {
        Ok(())
    }

    fn carry(&self, a: Attempt<VirtualClock>) -> impl Future<Output = ()> + Send + 'static {
        a.unanswered();
        std::future::ready(())
    }

    fn gate(
        &self,
        i: usize,
        _: Opening<'_>,
    ) -> impl Future<Output = Option<Result<String, Refused>>> + Send {
        let gate = self.gates.get(i).cloned();
        let clock = self.clock.clone();
        async move {
            let (wait, answer) = gate?;
            let until = clock.now() + wait;
            clock.sleep_until(until).await;
            Some(answer)
        }
    }
}

fn pass(name: &str) -> Result<String, Refused> {
    Ok(name.to_owned())
}

fn refuse(gate: &str, why: &str) -> Result<String, Refused> {
    Err(Refused {
        gate: gate.to_owned(),
        why: why.to_owned(),
    })
}

/// The stand's gates against `gates`, each bounded to `bound` µs.
fn gated(
    gates: Vec<(u64, Result<String, Refused>)>,
    bound: u64,
) -> Result<Vec<ascetic_ddd_scheduler::journal::Gate>, Refused> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    rt.block_on(async {
        let clock = Arc::new(VirtualClock::start(0));
        let runtimes = Runtimes {
            sources: vec![tokio::runtime::Handle::current()],
        };
        let h = Arc::new(header());
        let admitted = admit(
            h.clone(),
            W,
            Arc::new(Gated {
                clock: clock.clone(),
                gates,
            }),
            &runtimes,
        )
        .unwrap();
        let opening = Opening {
            header: &h,
            namespace: Namespace(1),
        };
        clock
            .within(gates_of(&admitted, opening, &clock, bound))
            .await
    })
}

async fn gates_of(
    admitted: &ascetic_ddd_load_run::run::Admitted<VirtualClock, Gated>,
    opening: Opening<'_>,
    clock: &Arc<VirtualClock>,
    bound: u64,
) -> Result<Vec<ascetic_ddd_scheduler::journal::Gate>, Refused> {
    gates(admitted, opening, &**clock, bound).await
}

/// The gates in turn: each passed named and timed; the first refusal stops
/// them, its reason the caller's; one unanswered within its bound refused as
/// such; a stand with none passes none.
#[test]
fn the_stand_s_gates_in_turn() {
    let passed = gated(
        vec![
            (10, pass("stand.namespace_unseen")),
            (100, pass("gateway.buckets_refilled")),
        ],
        1_000,
    )
    .unwrap();
    let named: Vec<(Code, u64)> = passed.into_iter().map(|g| (g.name, g.took)).collect();
    assert_eq!(
        named,
        vec![
            (Code::Named("stand.namespace_unseen".to_owned()), 10),
            (Code::Named("gateway.buckets_refilled".to_owned()), 100),
        ]
    );
    let refusal = gated(
        vec![
            (0, pass("stand.namespace_unseen")),
            (5, refuse("stand.clock_skew", "skew 5 ms")),
            (0, pass("never.asked")),
        ],
        1_000,
    )
    .unwrap_err();
    assert_eq!(refusal.gate, "stand.clock_skew");
    let unanswered = gated(vec![(5_000, pass("gateway.buckets_refilled"))], 1_000).unwrap_err();
    assert_eq!(unanswered.gate, "stand.unanswered");
    assert!(gated(vec![], 1_000).unwrap().is_empty());
}

/// A gate's refusal journaled: the header with no T0, its fault the only line
/// at 0, the trailer — void by its fault, its reason nowhere in it.
#[test]
fn a_gate_s_refusal_is_journaled_alone() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let h = Arc::new(header());
    let admitted = rt.block_on(async {
        let runtimes = Runtimes {
            sources: vec![tokio::runtime::Handle::current()],
        };
        let clock = Arc::new(VirtualClock::start(0));
        admit(
            h.clone(),
            W,
            Arc::new(Gated {
                clock,
                gates: vec![],
            }),
            &runtimes,
        )
        .unwrap()
    });
    let identity = Identity {
        source: "stages".to_owned(),
        log: "ab".repeat(32),
        validator: ascetic_ddd_command_log::live::validator(),
        namespace: Namespace(1),
    };
    let jh = journal_header(&admitted, identity, None, vec![], None);
    let why = refuse("stand.clock_skew", "the stand's own words");
    let refusal = why.unwrap_err();
    let lines = refused(Lines::new(), &refusal).unwrap();
    assert_eq!(lines.len(), 1);
    let (bytes, _) = refused(Written::start(Vec::new(), jh.clone(), 4).unwrap(), &refusal).unwrap();
    let text = String::from_utf8(bytes.clone()).unwrap();
    assert!(!text.contains("own words"), "{text}");
    let open = || Reader::new(Cursor::new(bytes.clone())).unwrap();
    let folded = fold(
        &jh,
        &Log {
            sha256: jh.log.clone(),
            records: 1,
        },
        open(),
        open(),
    );
    assert_eq!(
        folded.verdict,
        Verdict::Void(Void::Fault {
            class: FaultClass::Gate {
                name: Code::Named("stand.clock_skew".to_owned()),
            },
        })
    );
    let read: Vec<Line> = open().collect::<Result<_, _>>().unwrap();
    assert_eq!(read.len(), 1);
}

/// The measurement runs on a source's runtime, at the header's cap.
#[test]
fn the_judging_time_is_measured_on_a_source() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let judged = rt.block_on(async {
        let clock = Arc::new(VirtualClock::start(0));
        let runtimes = Runtimes {
            sources: vec![tokio::runtime::Handle::current()],
        };
        let mut h = header();
        h.profile.answer_cap = 2_048;
        let admitted = admit(
            Arc::new(h),
            W,
            Arc::new(Gated {
                clock: clock.clone(),
                gates: vec![],
            }),
            &runtimes,
        )
        .unwrap();
        measure(&admitted, clock).await.unwrap()
    });
    assert_eq!(judged.cap, 2_048);
    let _ = stages::GATES;
}

/// Admission refuses a read-ahead past the header's — the machine's own
/// refusal, before any journal.
#[test]
fn a_read_ahead_past_the_header_s_is_refused_at_admission() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let refused = rt.block_on(async {
        let clock = Arc::new(VirtualClock::start(0));
        let runtimes = Runtimes {
            sources: vec![tokio::runtime::Handle::current()],
        };
        let h = header();
        let past = h.read_ahead + 1;
        admit(
            Arc::new(h),
            past,
            Arc::new(Gated {
                clock,
                gates: vec![],
            }),
            &runtimes,
        )
        .err()
    });
    assert!(
        matches!(refused, Some(ascetic_ddd_load_run::run::RunError::Log(_))),
        "{refused:?}"
    );
}
