//! M-a of ADR-0047 decision 23 (K2b-7e-4b-3): what judging an answer costs —
//! its read, its judgement and their drop, timed together — by shape, size
//! and reader, against the grammar alone (`json::check`) and the tree
//! (`json::parse`). Its predictions and their refutations were written into
//! the ADR's § Measured before it ran. Measured on purpose (`--ignored`), on
//! an optimised build, one test at a time.
//!
//! - `MA_SIZES`: the sizes in KiB, comma-separated (64, 256, 512, 1024, 4096).
//! - `MA_COLD`: the caches evicted between runs — 64 MiB written outside the
//!   clock —, on the densest shapes at 1 MiB only (the tape against the
//!   grammar alone).
//! - `MA_BUSY=n`: `n` threads spinning beside it, a null judgement its
//!   control, a millisecond's park between runs.
//! - `MA_J`: the run's jitter J in µs (2,000); a row within J/2 at p99 holds.

#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use ascetic_ddd_command_log::answer::{Reply, read};
use ascetic_ddd_command_log::fills::Fills;
use ascetic_ddd_command_log::json;
use ascetic_ddd_command_log::record::{
    Action, Capture, Outcome, Predicate, Record, Rest, Source, StepId, Subject, Test, Value,
};
use ascetic_ddd_command_log::render::RunSecret;
use ascetic_ddd_faker::Namespace;
use ascetic_ddd_scheduler::judge::{Contract, Transport, judge};

const RUNS: usize = 200;

/// A row whose warm-up takes longer runs this many times: its p99 is then
/// its third largest of thirty.
const SLOW_RUNS: usize = 30;
const SLOW: Duration = Duration::from_millis(20);

/// Fifteen pairs of blocks, each pair of one FNV-1a state from the state the
/// blocks before it leave: 2^15 names of one hash (searched by the test's
/// author, `random.seed(23)`).
const COLLIDING: [(&str, &str); 15] = [
    ("sejrfgxu", "3gb2gf23"),
    ("2gosm3ch", "zpxhf9r1"),
    ("klu1pgjh", "0hnaym9h"),
    ("1pn8pmbg", "1zlk81ea"),
    ("flnpopss", "5g0d9949"),
    ("nwwt8tjk", "74i2nhc6"),
    ("mldvgno6", "9qn0opci"),
    ("8f2kbr73", "istxldv7"),
    ("mjdfr77m", "4haxdh4y"),
    ("88clycyl", "ouzgh533"),
    ("jzz6pfst", "bh4ytcbi"),
    ("z8hedq52", "bohwt0c4"),
    ("pf6nd65c", "bypeingv"),
    ("erv4zre0", "2g3vwqsc"),
    ("n3nx459c", "jilooxhc"),
];

fn fnv(s: &str) -> u32 {
    s.bytes().fold(0x811c_9dc5, |h: u32, b| {
        (h ^ u32::from(b)).wrapping_mul(0x0100_0193)
    })
}

fn colliding(i: usize) -> String {
    COLLIDING
        .iter()
        .enumerate()
        .map(|(j, (a, b))| if i >> j & 1 == 0 { *a } else { *b })
        .collect()
}

/// A body of at most `n` bytes, the pointer to its last value and that
/// value's text.
struct Body {
    bytes: Vec<u8>,
    last: String,
    value: String,
    array: bool,
}

fn array(n: usize, item: impl Fn(usize) -> String) -> Body {
    let mut text = String::from("[");
    let mut k = 0;
    let mut value = String::new();
    loop {
        let x = item(k);
        if text.len() + x.len() + 2 > n {
            break;
        }
        if k > 0 {
            text.push(',');
        }
        text.push_str(&x);
        value = x;
        k += 1;
    }
    text.push(']');
    Body {
        bytes: text.into_bytes(),
        last: format!("/{}", k.saturating_sub(1)),
        value,
        array: true,
    }
}

fn object(n: usize, order: &[usize], member: impl Fn(usize) -> (String, String)) -> Body {
    let mut text = String::from("{");
    let mut last = (String::new(), String::new());
    for (k, &i) in order.iter().enumerate() {
        let (name, value) = member(i);
        let m = format!("\"{name}\":{value}");
        if text.len() + m.len() + 2 > n {
            break;
        }
        if k > 0 {
            text.push(',');
        }
        text.push_str(&m);
        last = (name, value);
    }
    text.push('}');
    Body {
        bytes: text.into_bytes(),
        last: format!("/{}", last.0),
        value: last.1,
        array: false,
    }
}

/// SplitMix64's permutation of `0..k`.
fn shuffled(k: usize) -> Vec<usize> {
    let mut s = 0x4b32_6233u64;
    let mut next = || {
        s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = s;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    let mut v: Vec<usize> = (0..k).collect();
    for i in (1..k).rev() {
        v.swap(i, (next() % (i as u64 + 1)) as usize);
    }
    v
}

/// A deep object of wide levels: 127 levels, each `w` members and the next.
fn deep(n: usize) -> Body {
    let levels = 127;
    let w = (n / (levels * 9)).max(1);
    let mut text = String::new();
    for _ in 0..levels {
        text.push('{');
        for j in 0..w {
            text.push_str(&format!("\"a{j:03}\":0,"));
        }
        text.push_str("\"k\":");
    }
    text.push('1');
    text.push_str(&"}".repeat(levels));
    Body {
        bytes: text.into_bytes(),
        last: "/k".repeat(levels),
        value: "1".into(),
        array: false,
    }
}

const SHAPES: &[&str] = &[
    "numbers",
    "zeros",
    "strings",
    "non-ascii",
    "whitespace",
    "records",
    "members",
    "shuffled",
    "colliding",
    "long-names",
    "chains",
    "deep",
];

fn shape(name: &str, n: usize) -> Body {
    let members = n / 8 + 1;
    match name {
        "numbers" => array(n, |_| "12345".into()),
        "zeros" => array(n, |_| "0".into()),
        "strings" => array(n, |_| "\"abcdefghi\"".into()),
        "non-ascii" => array(n, |_| "\"éééé\"".into()),
        "whitespace" => array(n, |i| format!("\n    {}", i % 1000)),
        "records" => array(n, |i| {
            format!("{{\"id\":\"d-{i:06}\",\"name\":\"device\",\"n\":{i}}}")
        }),
        "members" => object(n, &(0..members).collect::<Vec<_>>(), |i| {
            (format!("m{i:010}"), "1".into())
        }),
        "shuffled" => object(n, &shuffled(members), |i| (format!("m{i:010}"), "1".into())),
        "colliding" => object(n, &(0..1 << 15).collect::<Vec<_>>(), |i| {
            (colliding(i), "1".into())
        }),
        "long-names" => object(n, &(0..members).collect::<Vec<_>>(), |i| {
            (format!("{}{i}", "x".repeat(1_000)), "1".into())
        }),
        "chains" => array(n, |_| {
            format!(
                "{}1{}",
                "[".repeat(json::DEPTH - 1),
                "]".repeat(json::DEPTH - 1)
            )
        }),
        "deep" => deep(n),
        _ => unreachable!(),
    }
}

fn device() -> Subject {
    Subject {
        kind: "device".into(),
        scope: vec![1, 2],
        index: 7,
    }
}

fn record(body: Vec<Predicate>, capture: Vec<Capture>) -> Record {
    Record {
        step: StepId {
            subject: device(),
            label: "s".into(),
            n: 0,
        },
        at: 0,
        think: 0,
        after: vec![],
        action: Action::Rest(Rest {
            op: "op".into(),
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
            body,
        }],
        capture,
        measure: vec![],
        deviation: None,
    }
}

fn pred(pointer: &str, test: Test) -> Predicate {
    Predicate {
        pointer: pointer.into(),
        test,
    }
}

/// The readers: a record each, judging the body by it.
fn readers(b: &Body) -> Vec<(String, Record)> {
    let lookups = |q: usize| {
        (0..q)
            .map(|i| {
                let p = if i % 2 == 0 {
                    b.last.clone()
                } else {
                    format!("/missing{i}")
                };
                pred(&p, Test::Exists)
            })
            .collect::<Vec<_>>()
    };
    let mut out = vec![
        ("none".to_owned(), record(vec![], vec![])),
        (
            "exists".to_owned(),
            record(vec![pred("", Test::Exists)], vec![]),
        ),
        (
            "capture".to_owned(),
            record(
                vec![],
                vec![Capture {
                    subject: device(),
                    slot: "v".into(),
                    source: Source::Json(b.last.clone()),
                    secret: false,
                }],
            ),
        ),
        (
            "equals".to_owned(),
            record(
                vec![pred(&b.last, Test::Equals(Value::Literal(b.value.clone())))],
                vec![],
            ),
        ),
    ];
    if b.array {
        out.push((
            "contains-absent".to_owned(),
            record(
                vec![pred(
                    "",
                    Test::Contains(Value::Literal("\"absent\"".into())),
                )],
                vec![],
            ),
        ));
    }
    for q in [1, 4, 16] {
        out.push((format!("lookups-{q}"), record(lookups(q), vec![])));
    }
    out
}

/// The run's p50, p99 and most of `RUNS` timings in µs — `SLOW_RUNS` for a
/// row whose warm-up took past `SLOW` —, `f` timed after a warm-up,
/// `between` outside the clock.
fn timed(mut f: impl FnMut(), mut between: impl FnMut()) -> (f64, f64, f64) {
    let start = Instant::now();
    f();
    let runs = if start.elapsed() > SLOW {
        SLOW_RUNS
    } else {
        RUNS
    };
    let mut took: Vec<Duration> = (0..runs)
        .map(|_| {
            between();
            let start = Instant::now();
            f();
            start.elapsed()
        })
        .collect();
    took.sort_unstable();
    let us = |d: Duration| d.as_secs_f64() * 1e6;
    (us(took[runs / 2]), us(took[runs - 3]), us(took[runs - 1]))
}

fn judged(bytes: &[u8], record: &Record) {
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
        problem: "/code",
    };
    let reply = Reply {
        status: 200,
        headers: &[],
        body: Some(bytes),
    };
    let judgement = judge(record, contract, &fills, &Transport::Answered(read(&reply)));
    assert!(judgement.is_ok());
    drop(judgement);
}

fn env_list(name: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(name).map_or_else(
        |_| default.to_vec(),
        |v| v.split(',').map(|s| s.trim().parse().unwrap()).collect(),
    )
}

#[test]
#[ignore = "a measurement, run on purpose"]
fn the_judging_s_time() {
    assert_eq!(fnv(&colliding(0)), fnv(&colliding((1 << 15) - 1)));
    let sizes: Vec<usize> = env_list("MA_SIZES", &[64, 256, 512, 1_024, 4_096])
        .into_iter()
        .map(|k| k * 1_024)
        .collect();
    let cold = std::env::var_os("MA_COLD").is_some();
    let busy = env_list("MA_BUSY", &[0])[0];
    let half = env_list("MA_J", &[2_000])[0] as f64 / 2.0;
    let stop = Arc::new(AtomicBool::new(false));
    let hogs: Vec<_> = (0..busy)
        .map(|_| {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut x = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    x = std::hint::black_box(x.wrapping_add(1));
                }
            })
        })
        .collect();
    let mut scratch = if cold { vec![0u8; 64 << 20] } else { vec![] };
    let mut between = || {
        if cold {
            for b in scratch.iter_mut().step_by(64) {
                *b = b.wrapping_add(1);
            }
        }
        if busy > 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
    };
    if busy > 0 {
        let (p50, p99, most) = timed(|| {}, &mut between);
        eprintln!(
            "MEASURED M-a control, {busy} threads busy: p50 {p50:.1} µs, p99 {p99:.1} µs, most {most:.1} µs"
        );
    }
    let mut held: Vec<(String, usize, bool)> = Vec::new();
    for name in SHAPES {
        if cold && !matches!(*name, "zeros" | "chains") {
            continue;
        }
        for &n in &sizes {
            if cold && n != 1 << 20 {
                continue;
            }
            let b = shape(name, n);
            let len = b.bytes.len();
            let mut rows: Vec<(String, Box<dyn Fn()>)> = vec![
                ("check".into(), {
                    let bytes = b.bytes.clone();
                    Box::new(move || assert!(json::check(&bytes).is_ok()))
                }),
                ("tree".into(), {
                    let bytes = b.bytes.clone();
                    Box::new(move || drop(json::parse(&bytes).unwrap()))
                }),
            ];
            for (reader, record) in readers(&b) {
                if cold && reader != "exists" {
                    continue;
                }
                let bytes = b.bytes.clone();
                rows.push((reader, Box::new(move || judged(&bytes, &record))));
            }
            for (reader, f) in rows {
                let (p50, p99, most) = timed(&f, &mut between);
                let (_, again, _) = timed(&f, &mut between);
                eprintln!(
                    "MEASURED M-a {name} {len} B {reader}: p50 {p50:.1} µs, p99 {p99:.1} µs, most {most:.1} µs, {:.2} ns/B at p99; again p99 {again:.1} µs",
                    p99 * 1e3 / len as f64
                );
                if reader == "exists" {
                    held.push((name.to_string(), n, p99.max(again) <= half));
                }
            }
        }
    }
    for name in SHAPES {
        let star = held
            .iter()
            .filter(|(s, _, ok)| s == name && *ok)
            .map(|(_, n, _)| *n)
            .max();
        eprintln!("MEASURED M-a S* {name}: {star:?} B within J/2 = {half} µs");
    }
    // The literal's parse: a want of 1, 8 and 64 KiB against a body of one
    // element.
    for k in [1usize, 8, 64] {
        let items: Vec<String> = (0..k * 1_024 / 6).map(|_| "12345".to_owned()).collect();
        let want = format!("[{}]", items.join(","));
        let r = record(
            vec![pred("/0", Test::Equals(Value::Literal(want.clone())))],
            vec![],
        );
        let body = b"[0]".to_vec();
        let (p50, p99, most) = timed(|| judged(&body, &r), &mut between);
        eprintln!(
            "MEASURED M-a want {} B: p50 {p50:.1} µs, p99 {p99:.1} µs, most {most:.1} µs",
            want.len()
        );
    }
    stop.store(true, Ordering::Relaxed);
    for h in hogs {
        h.join().unwrap();
    }
}
