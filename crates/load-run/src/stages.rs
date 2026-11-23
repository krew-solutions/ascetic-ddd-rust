//! Before T0 (ADR-0047 decision 18): three stages, by one predicate — a
//! journal exists if and only if something outside the process was touched.
//!
//! 1. [`admit`](crate::run::admit): pure, the plan against its carrier and
//!    runtimes; refused, no journal.
//! 2. [`measure`]: the judging time at the profile's cap, on a source's
//!    runtime, by a stopwatch — never the run's clock, whose start is T0;
//!    evidence for the journal's header, not a refusal, until a plan claims
//!    J's precision (K5).
//! 3. [`gates`]: the stand's gates in turn, each bounded; a refusal journaled
//!    by [`refused`] — the header with no T0, its fault, the trailer.
//!
//! The journal is opened after the third stage, its header built by
//! [`journal_header`]; T0's clock started after it.

use std::sync::Arc;

use ascetic_ddd_command_log::answer::{self, Reply};
use ascetic_ddd_command_log::fills::{Fills, Namespace};
use ascetic_ddd_command_log::record::{
    Action, Capture, Header, Outcome, Record, Rest, Source, StepId, Subject,
};
use ascetic_ddd_command_log::render::RunSecret;
use ascetic_ddd_scheduler::journal::{
    self, Code, Fault, FaultClass, Gate, JournalError, Judging, Line, Shape,
};
use ascetic_ddd_scheduler::judge::{Contract, Transport, judge};
use ascetic_ddd_scheduler::machine::{self, MACHINE_REVISION};

use crate::carrier::{Carrier, Unprobed};
use crate::clock::Clock;
use crate::live::Descriptors;
use crate::run::{Admitted, RunError, descriptors_fit};
use crate::sink::JournalSink;
use crate::stamps::{on_source, on_source_task};
use crate::stand::{Opening, Refused};

/// The shapes a document of the cap is measured in: the worst of them by the
/// judging time a byte (an array of numbers, measured).
pub const SHAPES: [Shape; 4] = [Shape::Flat, Shape::Objects, Shape::Numbers, Shape::Strings];

/// Runs each shape is timed, after one warm-up.
pub const RUNS: usize = 5;

/// A document of `shape`, at most `cap` bytes, and the pointer to its last
/// member.
pub fn document(shape: Shape, cap: usize) -> (Vec<u8>, String) {
    let array = matches!(shape, Shape::Numbers | Shape::Strings);
    let (open, close) = if array { (b'[', b']') } else { (b'{', b'}') };
    let mut body = vec![open];
    let mut last = String::new();
    for i in 0u64.. {
        let member = match shape {
            Shape::Flat => format!("\"k{i}\":{i}"),
            Shape::Objects => format!("\"k{i}\":{{\"v\":{i},\"s\":\"x{i}\"}}"),
            Shape::Numbers => "0".to_owned(),
            Shape::Strings => "\"\\u00e9\\n\\t\\\"\\\\\"".to_owned(),
        };
        let sep = usize::from(i > 0);
        if body.len() + sep + member.len() + 1 > cap {
            break;
        }
        if sep == 1 {
            body.push(b',');
        }
        body.extend_from_slice(member.as_bytes());
        last = match shape {
            Shape::Flat => format!("/k{i}"),
            Shape::Objects => format!("/k{i}/v"),
            Shape::Numbers | Shape::Strings => format!("/{i}"),
        };
    }
    body.push(close);
    (body, last)
}

/// A step that expects a 200 and captures the value at `pointer`: judging it
/// reads the body and looks the pointer up, as a capture of the plan's does.
fn judged_by(pointer: &str) -> Record {
    let subject = Subject {
        kind: "judging".into(),
        scope: vec![],
        index: 0,
    };
    Record {
        step: StepId {
            subject: subject.clone(),
            label: "judging".into(),
            n: 0,
        },
        at: 0,
        think: 0,
        after: vec![],
        action: Action::Rest(Rest {
            op: "judging".into(),
            caller: None,
            source: 0,
            path: vec![],
            query: vec![],
            headers: vec![],
            body: None,
        }),
        expect: vec![Outcome::Response {
            status: 200,
            problem: None,
            body: vec![],
        }],
        capture: vec![Capture {
            subject,
            slot: "value".into(),
            source: Source::Json(pointer.into()),
            secret: false,
        }],
        measure: vec![],
        deviation: None,
    }
}

/// The judging time at `header`'s cap by `stopwatch`: each shape read,
/// judged and dropped `RUNS` times after a warm-up, its median; the worst
/// shape's.
pub fn judging<C: Clock>(header: &Header, stopwatch: &C) -> Judging {
    let cap = usize::try_from(header.profile.answer_cap).unwrap_or(usize::MAX);
    let secret = RunSecret::new([0; 32]);
    let fills = Fills {
        namespace: Namespace(0),
        secret: &secret,
        run_start: 0,
        sent: 0,
        captured: &[],
    };
    let contract = Contract {
        not_run: &[],
        problem: &header.profile.problem,
    };
    let headers = [("content-type".to_owned(), b"application/json".to_vec())];
    let mut worst: Option<Judging> = None;
    for shape in SHAPES {
        let (body, last) = document(shape, cap);
        let record = judged_by(&last);
        let mut took = Vec::with_capacity(RUNS);
        for run in 0..=RUNS {
            let start = stopwatch.now();
            let reply = Reply {
                status: 200,
                headers: &headers,
                body: Some(&body),
            };
            drop(judge(
                &record,
                contract,
                &fills,
                &Transport::Answered(answer::read(&reply)),
            ));
            let t = stopwatch.now().saturating_sub(start);
            // The first warms the allocator and the caches.
            if run > 0 {
                took.push(t);
            }
        }
        took.sort_unstable();
        let median = took.get(RUNS / 2).copied().unwrap_or(0);
        if worst.as_ref().is_none_or(|w| median > w.took) {
            worst = Some(Judging {
                cap: header.profile.answer_cap,
                took: median,
                shape,
            });
        }
    }
    worst.unwrap_or(Judging {
        cap: header.profile.answer_cap,
        took: 0,
        shape: Shape::Flat,
    })
}

/// The judging time at the cap of the run `admitted` admits (the second
/// stage), measured on its first source's runtime by `stopwatch`.
pub async fn measure<C: Clock, X>(
    admitted: &Admitted<C, X>,
    stopwatch: Arc<C>,
) -> Result<Judging, RunError> {
    let Some(on) = admitted.runtimes().sources().first() else {
        return Err(RunError::Measure("no source's runtime".to_owned()));
    };
    let header = admitted.header().clone();
    on_source(on, move || judging(&header, &*stopwatch))
        .await
        .ok_or_else(|| RunError::Measure("the judging measurement failed".to_owned()))
}

/// The carrier's sockets probed (the third stage, decision 20): every
/// source's at once, each on its own runtime — the root's may have no I/O
/// driver — each connect within `bound` µs of `stopwatch`; one gate,
/// `egress.probe`. A failure refused, named by its source, its endpoint's
/// index and its kind (`egress.probe.s0.e1.refused`); every failure in its
/// reason, the caller's.
pub async fn probe<C: Clock, X: Carrier<C>>(
    admitted: &Admitted<C, X>,
    stopwatch: Arc<C>,
    bound: u64,
) -> Result<Gate, Refused> {
    let start = stopwatch.now();
    let sources = admitted.header().profile.sources.max(1);
    let mut probes = Vec::new();
    for source in 0..sources {
        let Some(on) = usize::try_from(source)
            .ok()
            .and_then(|s| admitted.runtimes().sources().get(s))
        else {
            return Err(Refused {
                gate: "egress.probe.runtime".to_owned(),
                why: format!("no runtime for source {source}"),
            });
        };
        let probe = admitted.carrier().probe(source, stopwatch.clone(), bound);
        probes.push((source, on_source_task(on, probe)));
    }
    let mut failed: Vec<(u32, Unprobed)> = Vec::new();
    // Each connect is bounded within its probe; a runtime that never runs
    // it is bounded here, its task aborted.
    let late = start.saturating_add(bound.saturating_mul(2));
    for (source, mut task) in probes {
        tokio::select! {
            biased;
            r = &mut task => match r {
                Ok(unprobed) => failed.extend(unprobed.into_iter().map(|u| (source, u))),
                Err(_) => failed.push((source, Unprobed { endpoint: 0, kind: "failed".to_owned() })),
            },
            () = stopwatch.sleep_until(late) => {
                task.abort();
                failed.push((source, Unprobed { endpoint: 0, kind: "unanswered".to_owned() }));
            }
        }
    }
    let Some((source, first)) = failed.first() else {
        return Ok(Gate {
            name: Code::from(&machine::Code::of("egress.probe")),
            took: stopwatch.now().saturating_sub(start),
        });
    };
    let why = failed
        .iter()
        .map(|(s, u)| format!("source {s} endpoint {}: {}", u.endpoint, u.kind))
        .collect::<Vec<_>>()
        .join("; ");
    Err(Refused {
        gate: format!("egress.probe.s{source}.e{}.{}", first.endpoint, first.kind),
        why,
    })
}

/// The process's descriptors read again as the third stage's last gate
/// (decision 20): what was opened since admission — a gate's client, the
/// host's own — still within the soft limit with the caps and the reserve;
/// one gate, `egress.descriptors`.
pub fn descriptors<C, X>(admitted: &Admitted<C, X>) -> Result<Gate, Refused> {
    let Some(caps) = admitted.caps() else {
        return Ok(Gate {
            name: Code::from(&machine::Code::of("egress.descriptors")),
            took: 0,
        });
    };
    descriptors_fit(caps, Descriptors::read().ok())
        .map(|()| Gate {
            name: Code::from(&machine::Code::of("egress.descriptors")),
            took: 0,
        })
        .map_err(|why| Refused {
            gate: "egress.descriptors".to_owned(),
            why: why.to_string(),
        })
}

/// The most gates a stand may have: past it, one that never says it is done.
pub const GATES: usize = 64;

/// The stand's gates in turn (the third stage), each given `bound` µs of
/// `stopwatch` to pass: the gates passed and their times, or the first
/// refusal — one unanswered within its bound refused as `stand.unanswered`.
pub async fn gates<C: Clock, X: Carrier<C>>(
    admitted: &Admitted<C, X>,
    opening: Opening<'_>,
    stopwatch: &C,
    bound: u64,
) -> Result<Vec<Gate>, Refused> {
    let mut passed = Vec::new();
    for i in 0..GATES {
        let start = stopwatch.now();
        let gate = admitted.carrier().gate(i, opening);
        let answered = tokio::select! {
            biased;
            r = gate => r,
            () = stopwatch.sleep_until(start.saturating_add(bound)) => {
                return Err(Refused {
                    gate: "stand.unanswered".to_owned(),
                    why: format!("gate {i} unanswered within {bound} µs"),
                });
            }
        };
        match answered {
            None => return Ok(passed),
            Some(Ok(name)) => passed.push(Gate {
                name: Code::from(&machine::Code::of(&name)),
                took: stopwatch.now().saturating_sub(start),
            }),
            Some(Err(refused)) => return Err(refused),
        }
    }
    Err(Refused {
        gate: "stand.gates".to_owned(),
        why: format!("more than {GATES} gates"),
    })
}

/// Who the journal's writer is and what it ran.
#[derive(Clone, Debug)]
pub struct Identity {
    /// The writer's own revision.
    pub source: String,
    /// The log's trailer's SHA-256, as its commitment holds it.
    pub log: String,
    /// The validator that committed the log.
    pub validator: String,
    /// The run's namespace.
    pub namespace: Namespace,
}

/// The journal's header for the run `admitted` admits: its identity, T0's
/// wall instant — none if a gate refused it before its clock started — and
/// the stages' evidence (decisions 14, 18).
pub fn journal_header<C, X>(
    admitted: &Admitted<C, X>,
    identity: Identity,
    t0: Option<i64>,
    gates: Vec<Gate>,
    judging: Option<Judging>,
) -> journal::Header {
    let h = admitted.header();
    journal::Header {
        journal: journal::JOURNAL_VERSION,
        machine: MACHINE_REVISION,
        source: identity.source,
        log: identity.log,
        validator: identity.validator,
        read_ahead: admitted.read_ahead(),
        jitter: h.profile.jitter,
        resend: h.profile.resend,
        namespace: identity.namespace.0,
        t0,
        gates,
        judging,
    }
}

/// A gate's refusal journaled into `sink` — opened with a header whose T0 is
/// none — as its only line, then the trailer.
pub fn refused<K: JournalSink>(
    mut sink: K,
    refused: &Refused,
) -> Result<K::Finished, JournalError> {
    sink.line(Line::Fault(Fault {
        at: 0,
        class: FaultClass::Gate {
            name: Code::from(&machine::Code::of(&refused.gate)),
        },
    }))?;
    sink.finish()
}
