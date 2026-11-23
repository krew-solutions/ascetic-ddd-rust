//! A journal that holds every line and every variant of its vocabulary, and
//! the byte form's hard cases: the golden journal (`golden/journal.jsonl`),
//! whose digest is [`JOURNAL`](super::JOURNAL). Its lines are not a run's: the
//! byte form is what it pins, not what a verdict makes of it.

use ascetic_ddd_command_log::record::{On, Safety, Subject};

use super::{
    Action, Answer, Code, Critical, Dep, Dispatch, End, Ended, Fault, FaultClass, Found, Gate,
    Header, Ignored, JOURNAL_VERSION, Judged, Judging, Key, Line, LogFault, Read, Released, Sent,
    Shape, Skip, Slot,
};
use crate::machine::MACHINE_REVISION;

/// The golden journal's header.
pub fn header() -> Header {
    Header {
        journal: JOURNAL_VERSION,
        machine: MACHINE_REVISION,
        source: "golden — every variant".to_owned(),
        log: "34d0604f25ecb8d0fa280db8b55837ff0ba21e52e3ab4d0ceb89b1e5ba24149d".to_owned(),
        validator: "6: golden".to_owned(),
        read_ahead: u64::MAX,
        jitter: 200,
        resend: 100_000,
        namespace: u64::MAX,
        // Before the epoch, past what a float holds exactly.
        t0: Some(i64::MIN),
        gates: vec![
            Gate {
                name: Code::Named("gateway.buckets_refilled".into()),
                took: u64::MAX,
            },
            Gate {
                name: Code::Other,
                took: 0,
            },
        ],
        judging: Some(Judging {
            cap: u64::MAX,
            took: 0,
            shape: Shape::Numbers,
        }),
    }
}

fn key(label: &str, n: u32) -> Key {
    Key {
        kind: "device".to_owned(),
        // Bytes past ASCII and a NUL: base64, padded.
        scope: vec![0, 0xff, b'"'],
        label: label.to_owned(),
        n,
    }
}

fn ended(line: u64, end: End, critical: Option<Critical>, fault: bool) -> Ended {
    Ended {
        at: line.saturating_mul(1_000),
        line,
        end,
        read: 10,
        ready: 20,
        intended: 30,
        released: 40,
        dispatched: Some(50),
        sent: None,
        ended: line.saturating_mul(1_000),
        attempts: 1,
        critical,
        bound: vec![2, u64::MAX],
        fault,
        wake: fault.then_some(u64::MAX),
    }
}

/// The golden journal's lines between its header and its trailer.
pub fn lines() -> Vec<Line> {
    let slot = |slot: &str| Slot {
        subject: Subject {
            kind: "device".to_owned(),
            scope: vec![1, 2, 3],
            index: u64::MAX,
        },
        slot: slot.to_owned(),
    };
    let mut lines = vec![
        Line::Read(Read {
            at: 0,
            line: 2,
            key: key("enroll", 0),
            action: Action::Rest,
            safety: Safety::NotTaken,
            cause: None,
            after: vec![],
            planned: 0,
        }),
        Line::Read(Read {
            at: u64::MAX,
            line: 3,
            key: key("report", 1),
            action: Action::Observe,
            safety: Safety::Never,
            cause: Some(u64::MAX),
            after: vec![
                Dep::Step {
                    on: On::Sent,
                    line: 2,
                },
                Dep::Each {
                    on: On::Expected,
                    key: key("read", u32::MAX),
                    members: u64::MAX,
                },
                Dep::Step {
                    on: On::Settled,
                    line: u64::MAX,
                },
            ],
            planned: u64::MAX,
        }),
        Line::Read(Read {
            at: 1,
            line: 4,
            key: key("publish", 0),
            action: Action::Bus,
            safety: Safety::Idempotent,
            cause: None,
            after: vec![],
            planned: 1,
        }),
        Line::Read(Read {
            at: 1,
            line: 5,
            key: key("onboard", 0),
            action: Action::Host,
            safety: Safety::Idempotent,
            cause: None,
            after: vec![],
            planned: 1,
        }),
        Line::Read(Read {
            at: 2,
            line: 6,
            key: key("watched", 0),
            action: Action::Poll,
            safety: Safety::Never,
            cause: Some(2),
            after: vec![],
            planned: 2,
        }),
        Line::Released(Released {
            at: 1,
            line: 2,
            intended: 2,
            released: 3,
            deadline: u64::MAX,
        }),
        Line::Dispatch(Dispatch {
            at: 3,
            line: 2,
            attempt: 1,
            deadline: u64::MAX,
            due: 2,
        }),
        Line::Sent(Sent {
            at: 4,
            line: 2,
            attempt: u32::MAX,
            handed: 3,
            connected: u64::MAX,
            lag: u64::MAX,
        }),
        Line::Answer(Answer {
            at: 5,
            line: 2,
            attempt: 1,
            judged: Judged::Expected,
            captured: vec![slot("id"), slot("token")],
            ignored: None,
            status: Some(201),
            code: None,
            after: None,
            censored: false,
            found: None,
        }),
    ];
    let judged = [
        (
            Judged::Unexpected,
            Some(Ignored::Ended),
            Some(u16::MAX),
            Some(Code::Other),
            None,
        ),
        (
            Judged::NotTaken,
            Some(Ignored::Stale),
            Some(503),
            Some(Code::Named("service.session_failed".to_owned())),
            Some(u64::MAX),
        ),
        (Judged::Unknown, Some(Ignored::Late), None, None, Some(0)),
    ];
    for (i, (judged, ignored, status, code, after)) in (6u64..).zip(judged) {
        lines.push(Line::Answer(Answer {
            at: i,
            line: 3,
            attempt: 2,
            judged,
            captured: vec![],
            ignored,
            status,
            code,
            after,
            censored: judged == Judged::Unknown,
            found: (judged == Judged::Unexpected).then_some(Found {
                absent: 0,
                deadline: u64::MAX,
            }),
        }));
    }
    let ends = [
        (End::Expected, Some(Critical::Step(2)), false),
        (End::Unexpected, Some(Critical::Member(4)), false),
        (End::Unknown, None, true),
        (End::Skipped(Skip::Dependency(2)), None, false),
        (End::Skipped(Skip::Member(4)), None, false),
        (End::Skipped(Skip::MissingCapture(u64::MAX)), None, false),
    ];
    for (line, (end, critical, fault)) in (2u64..).zip(ends) {
        lines.push(Line::End(Box::new(ended(line, end, critical, fault))));
        if fault {
            lines.push(Line::Fault(Fault {
                at: line.saturating_mul(1_000),
                class: FaultClass::Backstop { line },
            }));
        }
    }
    // An end sent and dispatched at the top of the range.
    lines.push(Line::End(Box::new(Ended {
        dispatched: None,
        sent: Some(u64::MAX),
        attempts: u32::MAX,
        bound: vec![],
        ..ended(8, End::Expected, None, false)
    })));
    // Every class of a run's fault (decision 18): in a run, only one, the
    // last line; here, the byte form of each.
    let stops = [
        FaultClass::Below {
            stamp: 0,
            released: u64::MAX,
        },
        FaultClass::Disordered {
            stamp: 1,
            last: u64::MAX,
        },
        FaultClass::Backdated {
            stamp: 2,
            now: u64::MAX,
        },
        FaultClass::Dropped {
            line: u64::MAX,
            attempt: u32::MAX,
        },
        FaultClass::Carrier {
            line: 2,
            attempt: 1,
            cannot: Code::Named("rest.decorator".to_owned()),
        },
        FaultClass::Carrier {
            line: 3,
            attempt: 2,
            cannot: Code::Other,
        },
        FaultClass::Held {
            line: 4,
            attempt: 1,
            since: u64::MAX,
        },
        FaultClass::Stalled,
        FaultClass::Poisoned,
        FaultClass::Saturated { alive: u64::MAX },
        FaultClass::Clock,
        FaultClass::Log(LogFault::Diverged),
        FaultClass::Log(LogFault::Truncated),
        FaultClass::Log(LogFault::Malformed),
        FaultClass::Log(LogFault::Io),
        FaultClass::Gate {
            name: Code::Named("stand.namespace_unseen".to_owned()),
        },
        FaultClass::Gate { name: Code::Other },
    ];
    for (at, class) in (9_000u64..).zip(stops) {
        lines.push(Line::Fault(Fault { at, class }));
    }
    lines
}
