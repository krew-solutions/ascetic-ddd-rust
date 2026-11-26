//! K2b-6b (ADR-0047 decision 16, K2-3): the shell on the real clock, its
//! sources on `Live`'s runtimes — the stand's requests one for each attempt
//! the journal holds, certified refusals resent and nothing else; a request
//! the stand read and then closed on, unknown; an answer in by its deadline,
//! judged past it on a busy thread, expected; the machine's own work stamped
//! at the instant taken while answers are judged and a burst keeps it busy.
//! And the timed wait's lateness, measured.

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use ascetic_ddd_load_run::sink::Journal;
use std::collections::BTreeMap;
use std::future::Future;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use ascetic_ddd_command_log::fills::Namespace;
use ascetic_ddd_command_log::record::{Header, Record, Rest, Safety};
use ascetic_ddd_command_log::render::{self, RunSecret};
use ascetic_ddd_load_run::carrier::Carrier;
use ascetic_ddd_load_run::clock::{Clock, RealClock};
use ascetic_ddd_load_run::live::Live;
use ascetic_ddd_load_run::rest::{Config, RestCarrier};
use ascetic_ddd_load_run::run::{RunError, admit, run};
use ascetic_ddd_load_run::sink::Lines;
use ascetic_ddd_load_run::source::Records;
use ascetic_ddd_load_run::stamps::Attempt;
use ascetic_ddd_load_run::stand::{Route, Stand};
use ascetic_ddd_load_run::tcp::TcpEgress;
use ascetic_ddd_scheduler::journal::{End, Judged, Line};
use ascetic_ddd_scheduler::machine::{Judgement, Seen};
use fixture::*;

const NAMESPACE: Namespace = Namespace(1);

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

/// What the TCP stub does with a request, by its correlation id.
#[derive(Clone, Copy)]
enum Answer {
    Ok,
    /// A certified refusal, its wait none.
    Refused,
    /// Read, and the connection closed with no answer.
    Closed,
}

fn field(head: &str, name: &str) -> Option<String> {
    head.lines().find_map(|l| {
        let (n, v) = l.split_once(':')?;
        n.trim()
            .eq_ignore_ascii_case(name)
            .then(|| v.trim().to_owned())
    })
}

/// A TCP stub on loopback: its answers by correlation id, and the requests
/// it saw by id.
fn stub(
    answers: BTreeMap<String, Answer>,
) -> (std::net::SocketAddr, Arc<Mutex<BTreeMap<String, u32>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(BTreeMap::new()));
    let (answers, s) = (Arc::new(answers), seen.clone());
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(mut c) = conn else { return };
            let (answers, s) = (answers.clone(), s.clone());
            std::thread::spawn(move || {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    let end = loop {
                        if let Some(i) = buf.windows(4).position(|x| x == b"\r\n\r\n") {
                            break i + 4;
                        }
                        match c.read(&mut chunk) {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..end]).into_owned();
                    buf.drain(..end);
                    let id = field(&head, "x-correlation-id").unwrap_or_default();
                    *s.lock().unwrap().entry(id.clone()).or_insert(0) += 1;
                    let reply: &[u8] = match answers.get(&id).copied().unwrap_or(Answer::Ok) {
                        Answer::Ok => b"HTTP/1.1 200 X\r\ncontent-length: 2\r\n\r\n{}",
                        Answer::Refused => b"HTTP/1.1 503 X\r\ncontent-type: application/problem+json\r\ncontent-length: 30\r\n\r\n{\"code\":\"service.unavailable\"}",
                        Answer::Closed => {
                            let _ = c.shutdown(std::net::Shutdown::Both);
                            return;
                        }
                    };
                    if c.write_all(reply).is_err() {
                        return;
                    }
                }
            });
        }
    });
    (addr, seen)
}

fn rest_header(sources: u32) -> Header {
    let mut h = header();
    h.channels.clear();
    h.calls.clear();
    h.profile.sources = sources;
    h.profile.jitter = 2_000;
    for o in &mut h.operations {
        o.safety = Safety::NotTaken;
        o.not_run = vec!["service.unavailable".into()];
    }
    h
}

/// `records` over TCP to `addr` from one `Live` source: the journal.
fn over_tcp(
    header: Header,
    records: &[Record],
    addr: std::net::SocketAddr,
) -> Result<Vec<Line>, RunError> {
    let live = Live::start(1, 1_024).unwrap();
    let clock = Arc::new(RealClock::start(2_000).unwrap());
    let header = Arc::new(header);
    let carrier = RestCarrier::new(
        header.clone(),
        OneStand,
        TcpEgress::new(
            [("gateway".to_owned(), addr)].into_iter().collect(),
            vec![None],
        )
        .with_ports(1_000),
        Config::new(RunSecret::new([7; 32])),
    );
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(run(
        ascetic_ddd_load_run::run::Ready::assumed(
            admit(
                ascetic_ddd_load_run::source::Plan::assumed(header, Default::default()),
                W,
                Arc::new(carrier),
                live.runtimes(),
                Journal::new(64, 0),
            )
            .unwrap(),
            clock,
            Lines::new(),
            ascetic_ddd_command_log::fills::Namespace(1),
        ),
        Records::new(records.iter().cloned()),
    ))
    .map(|r| r.journal)
}

fn id(r: &Record, attempt: u32) -> String {
    render::correlation(NAMESPACE, &r.step, attempt)
}

/// K2-3: on the real clock, every attempt the journal holds is one request
/// the stand saw, and no request it saw is not one — certified refusals
/// resent, every other answer taken; hyper retries nothing of its own.
#[test]
fn the_stand_sees_each_attempt_once() {
    let records: Vec<Record> = (0..40u64)
        .map(|i| call("update", i + 1, 0, i * 200, 0))
        .collect();
    let answers = records
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 3 == 0)
        .map(|(_, r)| (id(r, 1), Answer::Refused))
        .collect();
    let (addr, seen) = stub(answers);
    let journal = over_tcp(rest_header(1), &records, addr).unwrap();
    let dispatched: Vec<(u64, u32)> = journal
        .iter()
        .filter_map(|l| match l {
            Line::Dispatch(d) => Some((d.line, d.attempt)),
            _ => None,
        })
        .collect();
    let seen = seen.lock().unwrap();
    assert!(seen.values().all(|n| *n == 1), "{seen:?}");
    assert_eq!(seen.len(), dispatched.len());
    for (line, attempt) in &dispatched {
        let r = &records[(*line - 2) as usize];
        assert!(
            seen.contains_key(&id(r, *attempt)),
            "line {line} attempt {attempt} not seen"
        );
    }
    assert_eq!(dispatched.len(), 40 + 14, "fourteen refusals resent");
    let ends = journal
        .iter()
        .filter(|l| matches!(l, Line::End(e) if e.end == End::Expected))
        .count();
    assert_eq!(ends, 40);
}

/// K2-3: a request the stand read, then closed on without answering, is the
/// system's unknown, and is not resent.
#[test]
fn a_request_read_then_closed_on_is_unknown() {
    let records = vec![call("update", 1, 0, 0, 0)];
    let (addr, seen) = stub([(id(&records[0], 1), Answer::Closed)].into_iter().collect());
    let journal = over_tcp(rest_header(1), &records, addr).unwrap();
    let judged = journal.iter().find_map(|l| match l {
        Line::Answer(a) => Some((a.judged, a.status)),
        _ => None,
    });
    assert_eq!(judged, Some((Judged::Unknown, None)));
    assert_eq!(seen.lock().unwrap().len(), 1);
}

/// What the two drills carry: an answer `answer` µs after dispatch, judged
/// at once — or, for `slow`, judged while its thread is held `block` µs.
struct Drill {
    slow: u64,
    answer: u64,
    block: u64,
}

impl<C: Clock> Carrier<C> for Drill {
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
        let (slow, answer, block) = (self.slow, self.answer, self.block);
        async move {
            a.sent();
            let at = a.dispatched() + answer;
            a.clock().sleep_until(at).await;
            a.arrived();
            if a.line() == slow {
                #[expect(
                    clippy::disallowed_methods,
                    reason = "a judging that holds its thread, on purpose"
                )]
                std::thread::sleep(std::time::Duration::from_micros(block));
            }
            a.ended(Judgement::Expected(vec![]), Seen::default());
        }
    }
}

fn drill(records: &[Record], header: Header, carrier: Drill) -> Result<Vec<Line>, RunError> {
    drill_timed(records, header, carrier).map(|(j, _)| j)
}

/// [`drill`], and the clock's reading once the run returned.
fn drill_timed(
    records: &[Record],
    header: Header,
    carrier: Drill,
) -> Result<(Vec<Line>, u64), RunError> {
    let live = Live::start(header.profile.sources as usize, 1_024).unwrap();
    let clock = Arc::new(RealClock::start(2_000).unwrap());
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let ran = rt.block_on(run(
        ascetic_ddd_load_run::run::Ready::assumed(
            admit(
                ascetic_ddd_load_run::source::Plan::assumed(Arc::new(header), Default::default()),
                W,
                Arc::new(carrier),
                live.runtimes(),
                Journal::new(64, 0),
            )
            .unwrap(),
            clock.clone(),
            Lines::new(),
            ascetic_ddd_command_log::fills::Namespace(1),
        ),
        Records::new(records.iter().cloned()),
    ));
    ran.map(|r| (r.journal, clock.now()))
}

/// K2-3: an answer that came 10 ms before its deadline, judged while its
/// thread is held 30 ms past it, is expected — the frontier takes it at its
/// arrival, however late its judging ends. On a loaded box the answer may
/// itself come late: a run whose answer did not arrive by its deadline is
/// retried, never counted.
#[test]
fn an_answer_in_by_its_deadline_judged_past_it_is_expected() {
    let mut header = rest_header(1);
    header
        .operations
        .iter_mut()
        .for_each(|o| o.timeout = 20_000);
    let records = [call("read", 1, 0, 0, 0)];
    for _ in 0..8 {
        let (journal, returned) = drill_timed(
            &records,
            header.clone(),
            Drill {
                slow: 2,
                answer: 10_000,
                block: 30_000,
            },
        )
        .unwrap();
        let deadline = journal.iter().find_map(|l| match l {
            Line::Dispatch(d) => Some(d.deadline),
            _ => None,
        });
        let arrived = journal.iter().find_map(|l| match l {
            Line::Answer(a) if !a.censored && a.ignored.is_none() => Some(a.at),
            _ => None,
        });
        let (Some(deadline), Some(arrived)) = (deadline, arrived) else {
            continue;
        };
        if arrived > deadline {
            continue;
        }
        let end = journal.iter().find_map(|l| match l {
            Line::End(e) => Some((e.end, e.at)),
            _ => None,
        });
        // Stamped at its arrival, as the frontier stamps it; its judging
        // ended past the deadline, the run with it.
        let (end, at) = end.unwrap();
        assert_eq!((end, at), (End::Expected, arrived));
        assert!(
            returned > deadline,
            "the run returned at {returned}, its deadline {deadline}"
        );
        return;
    }
    panic!("no run's answer arrived by its deadline: the box too loaded for the twin");
}

/// The lateness drill, its state placed, past T0's own reading: 4,000 lines
/// planned at 600,000 read from 599,000, the machine busy reading them; twenty
/// calls released at 599,000 first, sending meanwhile below a floor; a slow
/// call's answer at 599,500 on source 1, its judging holding that thread
/// 20 ms; and, due under the floor behind the reading, a release wake and a
/// line readable at 599,200. Taken while the answer is judged, they would be
/// stamped at the floor less one, the clock long past it: a harness fault.
/// Held, they are taken once it ends, at the instant taken, and every line
/// ends.
#[test]
fn the_machine_s_own_work_is_stamped_when_taken() {
    use ascetic_ddd_command_log::record::Action;
    let mut header = rest_header(2);
    header
        .operations
        .iter_mut()
        .for_each(|o| o.timeout = 5_000_000);
    let mut slow = call("read", 1, 0, 598_000, 0);
    if let Action::Rest(rest) = &mut slow.action {
        rest.source = 1;
    }
    let mut records = vec![slow];
    records.extend((0..20u64).map(|i| call("read", i + 2, 0, 599_000, 0)));
    // Released by a wake of its own at 599,200, read ahead.
    records.push(call("read", 100, 0, 599_200, 0));
    records.extend((0..4_000u64).map(|i| call("read", i + 200, 0, 600_000, 0)));
    // Readable at 599,200, behind the reading.
    records.push(call("read", 9_000, 0, 599_200 + W, 0));
    for _ in 0..3 {
        let journal = drill(
            &records,
            header.clone(),
            Drill {
                slow: 2,
                answer: 1_500,
                block: 20_000,
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        let ends = journal.iter().filter(|l| matches!(l, Line::End(_))).count();
        assert_eq!(ends, records.len());
    }
}

/// The timed wait's lateness on this machine (decision 16's owed
/// measurement): sleeps on the real clock's own timer thread, past their
/// instants by how much — p50, p99, p99.9, the worst.
#[test]
#[ignore]
fn the_timed_wait_s_lateness() {
    let clock = Arc::new(RealClock::start(2_000).unwrap());
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let key = ascetic_ddd_faker::Key::root(ascetic_ddd_faker::Seed(5)).child("waits");
    let mut late: Vec<u64> = rt.block_on(async {
        let mut out = Vec::new();
        for n in 0..20_000u64 {
            let wait = key.at(n).unit().raw() % 2_000;
            let at = clock.now() + wait;
            clock.sleep_until(at).await;
            out.push(clock.now() - at);
        }
        out
    });
    late.sort_unstable();
    let q = |p: f64| late[((late.len() as f64 - 1.0) * p) as usize];
    eprintln!(
        "timed wait late by µs: p50 {} p99 {} p99.9 {} max {}",
        q(0.5),
        q(0.99),
        q(0.999),
        late[late.len() - 1]
    );
}

/// The judge's time on answers of a cap (decision 16's owed measurement):
/// documents of four shapes — a flat object, an object of small objects, an
/// array of zeros, an array of strings of escapes — filling each cap, a
/// capture at the last member making each parsed; read, judged and dropped
/// on the real clock, the median of five after one warm-up, and the parse
/// alone.
#[test]
#[ignore]
fn the_judge_s_time_at_the_cap() {
    use ascetic_ddd_command_log::answer::{self, Reply};
    use ascetic_ddd_command_log::fills::Fills;
    use ascetic_ddd_command_log::record::{Capture, Source, Subject};
    use ascetic_ddd_scheduler::judge::{Contract, Transport, judge};
    // A document of `cap` bytes at most, and the pointer to its last member.
    fn shape(kind: &str, cap: usize) -> (String, String) {
        let (open, close) = if kind.starts_with("array") {
            ("[", "]")
        } else {
            ("{", "}")
        };
        let mut body = String::from(open);
        let mut last = String::new();
        for i in 0u64.. {
            let member = match kind {
                "flat" => format!("\"k{i}\":{i}"),
                "objects" => format!("\"k{i}\":{{\"v\":{i},\"s\":\"x{i}\"}}"),
                "array of zeros" => "0".to_owned(),
                _ => "\"\\u00e9\\n\\t\\\"\\\\\"".to_owned(),
            };
            let sep = if i == 0 { "" } else { "," };
            if body.len() + sep.len() + member.len() + close.len() > cap {
                break;
            }
            body.push_str(sep);
            body.push_str(&member);
            last = match kind {
                "flat" => format!("/k{i}"),
                "objects" => format!("/k{i}/v"),
                _ => format!("/{i}"),
            };
        }
        body.push_str(close);
        (body, last)
    }
    let headers = vec![("content-type".to_owned(), b"application/json".to_vec())];
    let clock = RealClock::start(2_000).unwrap();
    let secret = RunSecret::new([7; 32]);
    let fills = Fills {
        namespace: NAMESPACE,
        secret: &secret,
        run_start: 0,
        sent: 0,
        captured: &[],
    };
    let median = |mut v: Vec<u64>| {
        v.sort_unstable();
        v[v.len() / 2]
    };
    for cap in [4usize << 10, 16 << 10, 64 << 10, 256 << 10, 1 << 20] {
        for kind in ["flat", "objects", "array of zeros", "array of escapes"] {
            let (body, last) = shape(kind, cap);
            let mut r = call("read", 1, 0, 0, 0);
            r.capture.push(Capture {
                subject: Subject {
                    kind: "device".into(),
                    scope: vec![],
                    index: 0,
                },
                slot: "group".into(),
                source: Source::Json(last.as_str().into()),
                secret: false,
            });
            let (mut parse, mut took) = (Vec::new(), Vec::new());
            for n in 0..6 {
                let start = clock.now();
                drop(ascetic_ddd_command_log::json::parse(body.as_bytes()).unwrap());
                let parsed = clock.now() - start;
                let start = clock.now();
                let reply = Reply {
                    status: 200,
                    headers: &headers,
                    body: Some(body.as_bytes()),
                };
                let judged = judge(
                    &r,
                    Contract {
                        not_run: &[],
                        problem: "/code",
                    },
                    &fills,
                    &Transport::Answered(answer::read(&reply)),
                )
                .unwrap();
                let expected = matches!(judged.judgement, Judgement::Expected(_));
                drop(judged);
                let t = clock.now() - start;
                assert!(expected, "{kind} at {cap}");
                if n > 0 {
                    parse.push(parsed);
                    took.push(t);
                }
            }
            eprintln!(
                "judge: cap {:>4} KiB {kind:>16} ({:>7} bytes): read+judge+drop p50 {:>6} µs, parse p50 {:>6} µs, {:.1} ns/B",
                cap >> 10,
                body.len(),
                median(took.clone()),
                median(parse),
                median(took) as f64 * 1_000.0 / body.len() as f64
            );
        }
    }
}
