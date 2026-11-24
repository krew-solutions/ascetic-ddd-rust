//! An attempt judged (ADR-0047 decision 15, K2a-3): the judgements of
//! `golden/answers.jsonl` against `golden/judgements.txt`, which
//! `vectors/judge.py` writes outside the crates — the decision's rules in
//! Python, over the command log's JSON by definition (`vectors/json.py`).

use ascetic_ddd_command_log::answer::{Reply, read};
use ascetic_ddd_command_log::fills::{Captured, Fills};
use ascetic_ddd_command_log::record::{
    Action, Capture, Observe, Outcome, Predicate, Probe, Record, Rest, Source, StepId, Subject,
    Test, Value,
};
use ascetic_ddd_command_log::render::RunSecret;
use ascetic_ddd_faker::Namespace;
use ascetic_ddd_scheduler::judge::{Code, Contract, Judged, Transport, Why, judge};
use ascetic_ddd_scheduler::machine::Judgement;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use proptest::prelude::*;

const CASES: &str = include_str!("../golden/answers.jsonl");
const JUDGEMENTS: &str = include_str!("../golden/judgements.txt");
const POINTER: &str = "/code";
const NOT_RUN: &[&str] = &["service.session_failed", "request.rate_limited"];

fn device(index: u64) -> Subject {
    Subject {
        kind: "device".into(),
        scope: vec![1, 2],
        index,
    }
}

fn step(label: &str) -> StepId {
    StepId {
        subject: device(7),
        label: label.into(),
        n: 0,
    }
}

fn call() -> Rest {
    Rest {
        op: "op".into(),
        caller: None,
        source: 0,
        path: vec![],
        query: vec![],
        headers: vec![],
        body: None,
    }
}

fn record(action: Action, expect: Vec<Outcome>, capture: Vec<Capture>) -> Record {
    Record {
        step: step("s"),
        at: 0,
        think: 0,
        after: vec![],
        action,
        expect,
        capture,
        measure: vec![],
        deviation: None,
    }
}

fn rest(expect: Vec<Outcome>, capture: Vec<Capture>) -> Record {
    record(Action::Rest(call()), expect, capture)
}

fn poll(until: Vec<Predicate>, capture: Vec<Capture>) -> Record {
    record(
        Action::Observe(Observe {
            cause: step("s"),
            probe: Probe::Rest(call()),
            until,
            every: 1_000_000,
            timeout: 60_000_000,
        }),
        vec![Outcome::Observed],
        capture,
    )
}

fn response(status: u16, problem: Option<&str>, body: Vec<Predicate>) -> Outcome {
    Outcome::Response {
        status,
        problem: problem.map(str::to_owned),
        body,
    }
}

fn pred(pointer: &str, test: Test) -> Predicate {
    Predicate {
        pointer: pointer.into(),
        test,
    }
}

fn lit(s: &str) -> Value {
    Value::Literal(s.into())
}

fn cap(slot: &str, source: Source) -> Capture {
    Capture {
        subject: device(7),
        slot: slot.into(),
        source,
        secret: false,
    }
}

fn json(slot: &str, pointer: &str) -> Capture {
    cap(slot, Source::Json(pointer.into()))
}

fn header(slot: &str, name: &str) -> Capture {
    cap(slot, Source::Header(name.into()))
}

fn location() -> Capture {
    cap("id", Source::Location)
}

/// What came back: a status, header lines, a body (`None`: past the cap).
enum T {
    Answer(
        u16,
        Vec<(&'static str, &'static [u8])>,
        Option<&'static [u8]>,
    ),
    Broken,
    Deadline,
}

const PROBLEM: (&str, &[u8]) = ("Content-Type", b"application/problem+json");

fn problem_body(code: &str) -> &'static [u8] {
    Box::leak(
        format!("{{\"type\":\"about:blank\",\"status\":503,\"code\":\"{code}\"}}")
            .into_bytes()
            .into_boxed_slice(),
    )
}

/// A body of two numbers: one of 8,192 digits, the most a value taken has,
/// and one of 8,193.
fn numbers() -> &'static [u8] {
    let digits = |n: usize| "7".repeat(n);
    let body = format!("{{\"a\":{},\"b\":{}}}", digits(8_192), digits(8_193));
    Box::leak(body.into_bytes().into_boxed_slice())
}

fn cases() -> Vec<(&'static str, Record, T)> {
    use T::Answer;
    let ok = || response(200, None, vec![]);
    let session = problem_body("service.session_failed");
    vec![
        (
            "a listed answer, its captures taken",
            rest(vec![ok()], vec![json("id", "/id"), header("etag", "ETag"), json("n", "/n")]),
            Answer(200, vec![("ETag", b"\"v-1\""), ("Content-Type", b"application/json")], Some(b"{\"id\":\"d-7\",\"n\":12345678901234567890123}")),
        ),
        (
            "a number at what a use carries, and one past it",
            rest(vec![ok()], vec![json("a", "/a"), json("b", "/b")]),
            Answer(200, vec![("Content-Type", b"application/json")], Some(numbers())),
        ),
        ("a Location path", rest(vec![response(201, None, vec![])], vec![location()]), Answer(201, vec![("Location", b"/v1/roles/r%41-1")], None)),
        (
            "a Location URL with a query and a fragment",
            rest(vec![response(201, None, vec![])], vec![location()]),
            Answer(201, vec![("location", b"https://t.example/v1/roles/r-2/?x=1#f")], None),
        ),
        ("a Location's fragment", rest(vec![response(201, None, vec![])], vec![location()]), Answer(201, vec![("Location", b"/v1/roles/r-3#f?x")], None)),
        ("a Location of a bad escape", rest(vec![response(201, None, vec![])], vec![location()]), Answer(201, vec![("Location", b"/v1/roles/%zz")], None)),
        ("a Location not UTF-8", rest(vec![response(201, None, vec![])], vec![location()]), Answer(201, vec![("Location", b"/v1/roles/%FF")], None)),
        ("a Location's dot segment", rest(vec![response(201, None, vec![])], vec![location()]), Answer(201, vec![("Location", b"/v1/roles/%2E%2E")], None)),
        ("two ETag lines", rest(vec![ok()], vec![header("etag", "etag")]), Answer(200, vec![("ETag", b"\"a\""), ("etag", b"\"b\"")], None)),
        ("a header not UTF-8", rest(vec![ok()], vec![header("etag", "ETag")]), Answer(200, vec![("ETag", b"\"\xff\"")], None)),
        (
            "captures no use can carry",
            rest(vec![ok()], vec![json("a", "/obj"), json("b", "/empty"), json("c", "/dot"), json("d", "/blank"), json("e", "/tab"), json("f", "/null"), json("g", "/absent")]),
            Answer(200, vec![], Some(b"{\"obj\":{\"a\":1},\"empty\":\"\",\"dot\":\".\",\"blank\":\" x\",\"tab\":\"a\\tb\",\"null\":null}")),
        ),
        ("a 503 with Retry-After and no code", rest(vec![ok()], vec![]), Answer(503, vec![("Retry-After", b"5"), ("Content-Type", b"text/plain")], Some(b"upstream connect error"))),
        ("a 503 certified not run", rest(vec![ok()], vec![]), Answer(503, vec![PROBLEM, ("Retry-After", b"5")], Some(session))),
        ("a 503 listed", rest(vec![response(503, None, vec![])], vec![]), Answer(503, vec![("Content-Type", b"text/plain")], Some(b"no healthy upstream"))),
        (
            "a not-run code listed",
            rest(vec![response(503, Some("service.session_failed"), vec![])], vec![]),
            Answer(503, vec![PROBLEM], Some(session)),
        ),
        (
            "a gateway's 404 against a service's",
            rest(vec![response(404, None, vec![])], vec![]),
            Answer(404, vec![PROBLEM], Some(b"{\"code\":\"route.not_found\"}")),
        ),
        ("a 404 without a code", rest(vec![response(404, None, vec![])], vec![]), Answer(404, vec![], Some(b"not found"))),
        (
            "a coded refusal listed",
            rest(vec![response(409, Some("policy.draft_unchanged"), vec![])], vec![]),
            Answer(409, vec![("content-type", b"Application/Problem+JSON; charset=utf-8")], Some(b"{\"code\":\"policy.draft_unchanged\"}")),
        ),
        (
            "a coded refusal against no code",
            rest(vec![response(409, None, vec![])], vec![]),
            Answer(409, vec![PROBLEM], Some(b"{\"code\":\"policy.draft_unchanged\"}")),
        ),
        ("a 2xx problem with a not-run code", rest(vec![response(201, None, vec![])], vec![]), Answer(200, vec![PROBLEM], Some(session))),
        ("another media type", rest(vec![ok()], vec![]), Answer(503, vec![("Content-Type", b"application/problem+jsonx")], Some(session))),
        ("two content types", rest(vec![ok()], vec![]), Answer(503, vec![PROBLEM, PROBLEM], Some(session))),
        ("a code not a string", rest(vec![ok()], vec![]), Answer(429, vec![PROBLEM], Some(b"{\"code\":7}"))),
        ("a problem not an object", rest(vec![ok()], vec![]), Answer(429, vec![PROBLEM], Some(b"[\"request.rate_limited\"]"))),
        ("a code past the journal's", rest(vec![ok()], vec![]), Answer(400, vec![PROBLEM], Some(b"{\"code\":\"a.bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\"}"))),
        ("a code not a dotted name", rest(vec![ok()], vec![]), Answer(400, vec![PROBLEM], Some(b"{\"code\":\"Bad Code\"}"))),
        ("199", rest(vec![ok()], vec![]), Answer(199, vec![], None)),
        ("300", rest(vec![ok()], vec![]), Answer(300, vec![], None)),
        ("399", rest(vec![ok()], vec![]), Answer(399, vec![], None)),
        ("499", rest(vec![ok()], vec![]), Answer(499, vec![], None)),
        ("500", rest(vec![ok()], vec![]), Answer(500, vec![], None)),
        ("999", rest(vec![ok()], vec![]), Answer(999, vec![], None)),
        ("a reset", rest(vec![ok()], vec![]), T::Broken),
        ("no answer by the deadline", rest(vec![ok()], vec![]), T::Deadline),
        ("Retry-After as a date", rest(vec![ok()], vec![]), Answer(503, vec![PROBLEM, ("Retry-After", b"Wed, 21 Oct 2015 07:28:00 GMT")], Some(session))),
        ("Retry-After negative", rest(vec![ok()], vec![]), Answer(503, vec![PROBLEM, ("Retry-After", b"-1")], Some(session))),
        ("Retry-After huge", rest(vec![ok()], vec![]), Answer(503, vec![PROBLEM, ("Retry-After", b"99999999999999999999")], Some(session))),
        ("Retry-After twice", rest(vec![ok()], vec![]), Answer(503, vec![PROBLEM, ("Retry-After", b"1"), ("Retry-After", b"2")], Some(session))),
        (
            "predicates that hold",
            rest(
                vec![response(200, None, vec![
                    pred("/n", Test::Equals(lit("1.0"))),
                    pred("/tags", Test::Contains(lit("\"x\""))),
                    pred("/gone", Test::Exists),
                    pred("", Test::Exists),
                ])],
                vec![],
            ),
            Answer(200, vec![], Some(b"{\"n\":1,\"tags\":[\"y\",\"x\"],\"gone\":null}")),
        ),
        (
            "a predicate that fails",
            rest(vec![response(200, None, vec![pred("/tags", Test::Contains(lit("\"z\"")))]), response(200, None, vec![pred("/n", Test::Equals(lit("2")))])], vec![]),
            Answer(200, vec![], Some(b"{\"n\":1,\"tags\":[\"y\",\"x\"]}")),
        ),
        ("Contains of no array", rest(vec![response(200, None, vec![pred("/n", Test::Contains(lit("1")))])], vec![]), Answer(200, vec![], Some(b"{\"n\":1}"))),
        ("predicates over a body past the cap", rest(vec![response(200, None, vec![pred("", Test::Exists)])], vec![]), Answer(200, vec![], None)),
        ("predicates over a body not JSON", rest(vec![response(200, None, vec![pred("", Test::Exists)])], vec![]), Answer(200, vec![], Some(b"<html>"))),
        (
            "a captured value in a predicate",
            rest(vec![response(200, None, vec![pred("/members", Test::Contains(Value::Hole(ascetic_ddd_command_log::record::Hole::Captured {
                subject: device(7),
                slot: "id".into(),
                from: step("created"),
            })))])], vec![]),
            Answer(200, vec![], Some(b"{\"members\":[\"g-1\",\"g-2\"]}")),
        ),
        ("a poll that holds", poll(vec![pred("/state", Test::Equals(lit("\"done\"")))], vec![json("v", "/version")]), Answer(200, vec![], Some(b"{\"state\":\"done\",\"version\":3}"))),
        ("a poll not yet", poll(vec![pred("/state", Test::Equals(lit("\"done\"")))], vec![]), Answer(200, vec![], Some(b"{\"state\":\"pending\"}"))),
        ("a poll's 404", poll(vec![], vec![]), Answer(404, vec![], None)),
        ("a poll's 304", poll(vec![], vec![]), Answer(304, vec![], None)),
        (
            "a poll with no expectation",
            {
                let mut r = poll(vec![], vec![]);
                r.expect.clear();
                r
            },
            Answer(404, vec![], None),
        ),
        ("a poll's 2xx without until", poll(vec![], vec![]), Answer(204, vec![], None)),
        ("a poll's 503", poll(vec![], vec![]), Answer(503, vec![], None)),
        ("a poll's not-run 503", poll(vec![], vec![]), Answer(503, vec![PROBLEM, ("Retry-After", b"1")], Some(session))),
        ("a sent-only step's 409", rest(vec![], vec![]), Answer(409, vec![], None)),
        ("a sent-only step's 504", rest(vec![], vec![]), Answer(504, vec![], None)),
        ("a sent-only step's not-run code", rest(vec![], vec![]), Answer(429, vec![PROBLEM], Some(b"{\"code\":\"request.rate_limited\"}"))),
    ]
}

fn captured() -> Vec<Captured> {
    vec![Captured {
        subject: device(7),
        slot: "id".into(),
        value: b"\"g-2\"".to_vec(),
    }]
}

fn case_lines() -> String {
    cases()
        .into_iter()
        .map(|(name, record, t)| {
            let transport = match t {
                T::Answer(status, headers, body) => serde_json::json!({"answered": {
                    "status": status,
                    "headers": headers.iter().map(|(n, v)| serde_json::json!([n, STANDARD.encode(v)])).collect::<Vec<_>>(),
                    "body": body.map(|b| STANDARD.encode(b)),
                }}),
                T::Broken => serde_json::json!("broken"),
                T::Deadline => serde_json::json!("deadline"),
            };
            let captured: Vec<serde_json::Value> = captured()
                .iter()
                .map(|c| serde_json::json!({"subject": c.subject, "slot": c.slot, "value": STANDARD.encode(&c.value)}))
                .collect();
            serde_json::json!({
                "name": name, "record": record, "transport": transport, "not_run": NOT_RUN,
                "problem": POINTER, "captured": captured,
            })
            .to_string()
                + "\n"
        })
        .collect()
}

fn line(name: &str, j: &Judged) -> String {
    let (kind, taken) = match &j.judgement {
        Judgement::Expected(c) => ("expected", c.clone()),
        Judgement::Unexpected => ("unexpected", vec![]),
        Judgement::NotTaken => ("not_taken", vec![]),
        Judgement::Unknown => ("unknown", vec![]),
    };
    let why = match j.why {
        Why::Broken => "broken".to_owned(),
        Why::Deadline => "deadline".to_owned(),
        Why::Listed(i) => format!("listed {i}"),
        Why::Polled => "polled".to_owned(),
        Why::NotRun => "not_run".to_owned(),
        Why::Uncertain => "uncertain".to_owned(),
        Why::Sent => "sent".to_owned(),
        Why::Unlisted => "unlisted".to_owned(),
    };
    let opt = |o: Option<String>| o.unwrap_or_else(|| "-".to_owned());
    let code = j.code.as_ref().map(|c| match c {
        Code::Named(n) => format!("named {n}"),
        Code::Other => "other".to_owned(),
    });
    let taken: Vec<String> = taken
        .iter()
        .map(|c| format!("{}={}", c.slot, String::from_utf8_lossy(&c.value)))
        .collect();
    let missing: Vec<&str> = j.missing.iter().map(|(_, s)| s.as_str()).collect();
    format!(
        "case {name}\n{kind} | {why} | status {} | code {} | retry_after {} | taken [{}] | missing [{}]\n",
        opt(j.status.map(|s| s.to_string())),
        opt(code),
        opt(j.retry_after.map(|r| r.to_string())),
        taken.join(" "),
        missing.join(" "),
    )
}

fn computed() -> String {
    let secret = RunSecret::new([0; 32]);
    let captured = captured();
    let fills = Fills {
        namespace: Namespace(1),
        secret: &secret,
        run_start: 0,
        sent: 0,
        captured: &captured,
    };
    let not_run: Vec<String> = NOT_RUN.iter().map(|s| (*s).to_owned()).collect();
    let contract = Contract {
        not_run: &not_run,
        problem: POINTER,
    };
    let mut out = String::new();
    for (name, record, t) in cases() {
        let headers: Vec<(String, Vec<u8>)>;
        let reply;
        let transport = match t {
            T::Answer(status, h, body) => {
                headers = h
                    .iter()
                    .map(|(n, v)| ((*n).to_owned(), v.to_vec()))
                    .collect();
                reply = Reply {
                    status,
                    headers: &headers,
                    body,
                };
                Transport::Answered(read(&reply))
            }
            T::Broken => Transport::Broken,
            T::Deadline => Transport::Deadline,
        };
        let j = judge(&record, contract, &fills, &transport).unwrap();
        out += &line(name, &j);
    }
    out
}

/// The cases the vectors read are these.
#[test]
fn the_golden_cases_are_the_test_s() {
    assert_eq!(
        case_lines(),
        CASES,
        "golden/answers.jsonl is stale: run `cargo test --test judge -- --ignored`"
    );
}

#[test]
#[ignore = "regenerates golden/answers.jsonl"]
#[expect(
    clippy::disallowed_methods,
    reason = "the test writes its golden; the machine opens no file"
)]
fn regenerate() {
    std::fs::write(
        concat!(env!("CARGO_MANIFEST_DIR"), "/golden/answers.jsonl"),
        case_lines(),
    )
    .unwrap();
}

/// Decision 15: every judgement as the rules decide it outside the crates.
#[test]
fn judgements_match_the_independent_vectors() {
    assert_eq!(computed(), JUDGEMENTS);
}

/// A judgement's `Debug` shows no captured value.
#[test]
fn a_judgement_s_debug_shows_no_captured_value() {
    let shown = computed();
    assert!(shown.contains("d-7"), "the golden takes a value");
    let (_, record, _) = cases().into_iter().next().unwrap();
    let headers = vec![("ETag".to_owned(), b"\"v-1\"".to_vec())];
    let reply = Reply {
        status: 200,
        headers: &headers,
        body: Some(b"{\"id\":\"d-7\",\"n\":5}"),
    };
    let secret = RunSecret::new([0; 32]);
    let fills = Fills {
        namespace: Namespace(1),
        secret: &secret,
        run_start: 0,
        sent: 0,
        captured: &[],
    };
    let contract = Contract {
        not_run: &[],
        problem: POINTER,
    };
    let j = judge(
        &record,
        contract,
        &fills,
        &Transport::Answered(read(&reply)),
    )
    .unwrap();
    let debug = format!("{j:?}");
    assert!(
        matches!(&j.judgement, Judgement::Expected(c) if c.len() == 3),
        "{debug}"
    );
    for planted in ["d-7", "v-1"] {
        assert!(!debug.contains(planted), "{planted} in {debug}");
    }
}

const NAMES: &[&str] = &[
    "Content-Type",
    "content-type",
    "Location",
    "Retry-After",
    "ETag",
    "X-Other",
];
const VALUES: &[&[u8]] = &[
    b"application/problem+json",
    b"application/problem+json; charset=utf-8",
    b"text/plain",
    b"/v1/roles/r-1",
    b"https://x/v1/%zz?#",
    b"5",
    b"99999999999999999999",
    b"\"v\"",
    b"\xff\xfe",
    b"",
];

fn bytes() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        prop::collection::vec(any::<u8>(), 0..64),
        prop::sample::select(VALUES).prop_map(<[u8]>::to_vec),
        Just(
            b"{\"code\":\"service.session_failed\",\"id\":\"d\",\"n\":1e400,\"state\":\"done\"}"
                .to_vec()
        ),
        Just(b"{\"code\":[1,{\"a\":-0}],\"members\":[\"g-2\"],\"tags\":\"x\"}".to_vec()),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 4096, ..ProptestConfig::default() })]

    /// No answer a server can send makes the judge fail or panic on a record
    /// whose predicates the validator passes.
    #[test]
    fn the_judge_is_total_over_answers(
        case in 0..cases().len(),
        status in 100u16..=999,
        headers in prop::collection::vec((prop::sample::select(NAMES), bytes()), 0..6),
        body in prop::option::of(bytes()),
    ) {
        let (_, record, _) = cases().swap_remove(case);
        let headers: Vec<(String, Vec<u8>)> = headers.into_iter().map(|(n, v)| (n.to_owned(), v)).collect();
        let reply = Reply { status, headers: &headers, body: body.as_deref() };
        let secret = RunSecret::new([0; 32]);
        let captured = captured();
        let fills = Fills { namespace: Namespace(1), secret: &secret, run_start: 0, sent: 0, captured: &captured };
        let not_run: Vec<String> = NOT_RUN.iter().map(|s| (*s).to_owned()).collect();
        let contract = Contract { not_run: &not_run, problem: POINTER };
        let j = judge(&record, contract, &fills, &Transport::Answered(read(&reply)));
        prop_assert!(j.is_ok());
        let j = j.unwrap();
        prop_assert_eq!(j.status, Some(status));
        if let Judgement::Expected(taken) = &j.judgement {
            prop_assert_eq!(taken.len() + j.missing.len(), record.capture.len());
        }
    }
}
