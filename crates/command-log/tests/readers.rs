//! K2b-7e-4b-2 of ADR-0047 (decision 23): the readers on the tape read every
//! answer as today's did — a problem's code, a predicate's verdict or its
//! error, a captured value's bytes — against a frozen copy of today's, which
//! parsed a body whole when it was read: the lazy against the eager
//! (decision 23's step 1, not built at K2b-7e-4a), over generated answers
//! counted by stratum, in two orders of reading, and over hand rows.

mod frozen;

use ascetic_ddd_command_log::answer::{self, Reply, TAKEN};
use ascetic_ddd_command_log::fills::{Captured, Fills, Namespace};
use ascetic_ddd_command_log::record::{
    Capture, Hole, Predicate, Source, StepId, Subject, Test, Value,
};
use ascetic_ddd_command_log::render::RunSecret;
use sha2::{Digest, Sha256};

/// The frozen copies' digests, as committed.
const FROZEN: [(&str, &[u8], &str); 2] = [
    (
        "json.rs",
        include_bytes!("frozen/json.rs"),
        "1b287b67cb6e052e0761f997cee2dd6047b03f1017719a0db2d37c598e08466b",
    ),
    (
        "answer.rs",
        include_bytes!("frozen/answer.rs"),
        "aff34140b0078bd9c43b09bd4c08323237af0c7115600904df13a1ff83fec273",
    ),
];

/// The oracle is today's: a copy edited to agree with a regression is red.
#[test]
fn the_frozen_readers_are_today_s() {
    for (file, bytes, want) in FROZEN {
        let got: String = Sha256::digest(bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(got, want, "tests/frozen/{file} edited");
    }
}

/// SplitMix64: the draws are the test's, fixed.
struct Draw(u64);

impl Draw {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len())]
    }
}

/// Numbers by value: each class's texts are one value.
const NUMBERS: &[&[&str]] = &[
    &["0", "-0", "0.0", "0e5"],
    &["1", "1.0", "10e-1", "0.1e1", "1E0"],
    &["100", "1e2", "1.00e2", "10E1"],
    &["0.05", "5e-2", "0.050", "50e-3"],
    &["-2.5", "-25e-1", "-2.50"],
    &["12345678901234567890123", "1.2345678901234567890123e22"],
    &["7"],
    &["-1e-400"],
];

/// Names by their decoded text, each with its spellings.
const NAMES: &[(&str, &[&str])] = &[
    ("a", &["a", "\\u0061"]),
    ("code", &["code", "c\\u006fde"]),
    ("é", &["é", "\\u00e9"]),
    ("m9tcqis_", &["m9tcqis_"]),
    ("i7giss29", &["i7giss29"]),
    ("-", &["-"]),
    ("", &[""]),
    ("x/y", &["x/y", "x\\/y"]),
    ("m~n", &["m~n"]),
    ("0", &["0"]),
];

const PIECES: &[&str] = &[
    "a",
    "b",
    "A",
    "é",
    "\\n",
    "\\u0041",
    "\\ud83d\\ude00",
    "\\\\",
    "\\\"",
    " ",
    "/",
    "~",
    ".",
];

const CODES: &[&str] = &[
    "request.rate_limited",
    "r\\u0065quest.rate_limited",
    "service.session_failed",
    "policy.draft_unchanged",
];

/// A value drawn, before it is written.
#[derive(Clone)]
enum V {
    Null,
    Bool(bool),
    Num(usize),
    Str(String),
    Arr(Vec<V>),
    Obj(Vec<(usize, V)>),
}

fn string(d: &mut Draw) -> String {
    match d.below(30) {
        0 => {
            // Around what a use carries, half of it escaped: its text longer
            // than `TAKEN` though its value is not.
            let k = TAKEN - 1 + d.below(3);
            let a = if d.below(2) == 0 { "A" } else { "\\u0041" };
            a.repeat(k)
        }
        1 | 2 => (*d.pick(CODES)).to_owned(),
        _ => (0..d.below(5)).map(|_| *d.pick(PIECES)).collect(),
    }
}

fn draw(d: &mut Draw, depth: usize) -> V {
    if depth >= 4 || d.below(3) == 0 {
        return match d.below(6) {
            0 => V::Null,
            1 => V::Bool(d.below(2) == 0),
            2 | 3 => V::Num(d.below(NUMBERS.len())),
            _ => V::Str(string(d)),
        };
    }
    let n = d.below(5);
    if d.below(2) == 0 {
        V::Arr((0..n).map(|_| draw(d, depth + 1)).collect())
    } else {
        object(d, depth, n)
    }
}

/// An object of `n` members, one in ten naming one twice.
fn object(d: &mut Draw, depth: usize, n: usize) -> V {
    let twice = d.below(10) == 0;
    let mut left: Vec<usize> = (0..NAMES.len()).collect();
    let mut members = Vec::new();
    for _ in 0..n.min(left.len()) {
        let i = d.below(left.len());
        let family = if twice { left[i] } else { left.remove(i) };
        members.push((family, draw(d, depth + 1)));
    }
    V::Obj(members)
}

/// A body: an object most often, a problem's `code` in half of them.
fn body(d: &mut Draw) -> V {
    match d.below(10) {
        0 | 1 => V::Arr((0..d.below(5)).map(|_| draw(d, 1)).collect()),
        2 => draw(d, 4),
        _ => {
            let n = d.below(4);
            let mut o = object(d, 0, n);
            if let V::Obj(members) = &mut o
                && d.below(2) == 0
                && !members.iter().any(|(f, _)| *f == 1)
            {
                members.push((1, V::Str((*d.pick(CODES)).to_owned())));
            }
            o
        }
    }
}

/// A value's text, its numbers, names and members' order drawn: one value
/// written as any of its texts.
fn write(v: &V, d: &mut Draw) -> String {
    match v {
        V::Null => "null".to_owned(),
        V::Bool(b) => b.to_string(),
        V::Num(c) => (*d.pick(NUMBERS[*c])).to_owned(),
        V::Str(s) => format!("\"{s}\""),
        V::Arr(items) => {
            let items: Vec<String> = items.iter().map(|x| write(x, d)).collect();
            format!("[{}]", items.join(","))
        }
        V::Obj(members) => {
            let mut members: Vec<String> = members
                .iter()
                .map(|(f, x)| format!("\"{}\":{}", d.pick(NAMES[*f].1), write(x, d)))
                .collect();
            for i in (1..members.len()).rev() {
                members.swap(i, d.below(i + 1));
            }
            format!("{{{}}}", members.join(","))
        }
    }
}

/// Another value near `v`: a number of another class, a string longer, an
/// array or an object with a member fewer or more.
fn other(v: &V, d: &mut Draw) -> V {
    match v {
        V::Null => V::Bool(false),
        V::Bool(b) => V::Bool(!b),
        V::Num(c) => V::Num((c + 1 + d.below(NUMBERS.len() - 1)) % NUMBERS.len()),
        V::Str(s) => V::Str(format!("{s}z")),
        V::Arr(items) => {
            let mut items = items.clone();
            if items.is_empty() || d.below(2) == 0 {
                items.push(V::Null);
            } else {
                items.pop();
            }
            V::Arr(items)
        }
        V::Obj(members) => {
            let mut members = members.clone();
            let unused = (0..NAMES.len()).find(|f| !members.iter().any(|(g, _)| g == f));
            match unused {
                Some(f) if members.is_empty() || d.below(2) == 0 => members.push((f, V::Null)),
                _ => {
                    members.pop();
                }
            }
            V::Obj(members)
        }
    }
}

/// Every place of a value with its pointer, and pointers beside them.
fn places<'a>(v: &'a V, here: String, out: &mut Vec<(String, Option<&'a V>)>) {
    out.push((here.clone(), Some(v)));
    match v {
        V::Arr(items) => {
            for (i, x) in items.iter().enumerate() {
                places(x, format!("{here}/{i}"), out);
            }
            for t in ["-", "01", &items.len().to_string()] {
                out.push((format!("{here}/{t}"), None));
            }
        }
        V::Obj(members) => {
            for (f, x) in members {
                let token = NAMES[*f].0.replace('~', "~0").replace('/', "~1");
                places(x, format!("{here}/{token}"), out);
            }
            out.push((format!("{here}/missing"), None));
        }
        _ => out.push((format!("{here}/0"), None)),
    }
}

fn subject() -> Subject {
    Subject {
        kind: "device".into(),
        scope: vec![],
        index: 1,
    }
}

fn hole(slot: &str) -> Value {
    Value::Hole(Hole::Captured {
        subject: subject(),
        slot: slot.into(),
        from: StepId {
            subject: subject(),
            label: "made".into(),
            n: 0,
        },
    })
}

fn captured() -> Vec<Captured> {
    vec![
        Captured {
            subject: subject(),
            slot: "s".into(),
            value: b"\"x\"".to_vec(),
        },
        Captured {
            subject: subject(),
            slot: "n".into(),
            value: b"5".to_vec(),
        },
    ]
}

fn capture(source: Source) -> Capture {
    Capture {
        subject: subject(),
        slot: "v".into(),
        source,
        secret: false,
    }
}

/// A predicate drawn on `doc`'s places: present or beside, equal or not,
/// an element or not, written wrong now and then.
fn predicate(d: &mut Draw, ps: &[(String, Option<&V>)]) -> Predicate {
    let (pointer, place) = d.pick(ps).clone();
    let literal = |v: &V, d: &mut Draw| {
        let v = if d.below(2) == 0 {
            v.clone()
        } else {
            other(v, d)
        };
        Value::Literal(write(&v, d))
    };
    let test = match (d.below(12), place) {
        (0, _) => return pointer_wrong(d),
        (1, _) => Test::Equals(Value::Literal("{".to_owned())),
        (2, _) => Test::Equals(hole(d.pick(&["s", "n", "missing"]))),
        (3..=5, Some(v)) => Test::Equals(literal(v, d)),
        (6 | 7, Some(V::Arr(items))) if !items.is_empty() => {
            let x = items[d.below(items.len())].clone();
            Test::Contains(literal(&x, d))
        }
        (6 | 7, Some(v)) => Test::Contains(literal(v, d)),
        _ => Test::Exists,
    };
    Predicate { pointer, test }
}

fn pointer_wrong(d: &mut Draw) -> Predicate {
    Predicate {
        pointer: (*d.pick(&["a", "/~2", "/a~"])).to_owned(),
        test: Test::Exists,
    }
}

/// The strata reached.
#[derive(Default, Debug)]
struct Reached {
    equal: usize,
    unequal: usize,
    contained: usize,
    not_contained: usize,
    pointer_error: usize,
    literal_error: usize,
    hole_error: usize,
    unreadable_read: usize,
    code: usize,
    code_refused: usize,
    long_taken: usize,
    long_refused: usize,
    number_taken: usize,
}

/// One answer read by both, in the judge's order and then the other way
/// round: every result the same.
fn agree(
    status: u16,
    headers: &[(String, Vec<u8>)],
    body: Option<&[u8]>,
    problem: &str,
    predicates: &[Predicate],
    captures: &[Capture],
    reached: &mut Reached,
) {
    let secret = RunSecret::new([0; 32]);
    let captured = captured();
    let fills = Fills {
        namespace: Namespace(0),
        secret: &secret,
        run_start: 0,
        sent: 0,
        captured: &captured,
    };
    let new_reply = Reply {
        status,
        headers,
        body,
    };
    let old_reply = frozen::answer::Reply {
        status,
        headers,
        body,
    };
    let readable = body.is_some_and(|b| frozen::json::parse(b).is_ok());
    let case = || {
        format!(
            "{status} {:?} at {problem:?}, {predicates:?}",
            body.map(String::from_utf8_lossy)
        )
    };
    for order in [false, true] {
        let new = answer::read(&new_reply);
        let old = frozen::answer::read(&old_reply);
        let takes = |reached: &mut Reached| {
            for c in captures {
                let (n, o) = (new.take(c), old.take(c));
                assert_eq!(n, o, "{} take {c:?}", case());
                if let (Some(v), Source::Json(_)) = (&n, &c.source) {
                    reached.long_taken += usize::from(v.len() >= TAKEN);
                    reached.number_taken += usize::from(!v.starts_with(b"\""));
                }
            }
        };
        if order {
            takes(reached);
        }
        let code = new.code(problem);
        assert_eq!(code, old.code(problem), "{} code", case());
        reached.code += usize::from(code.is_some());
        let all = new.holds(predicates, &fills).map_err(|e| format!("{e:?}"));
        let was = old.holds(predicates, &fills).map_err(|e| format!("{e:?}"));
        assert_eq!(all, was, "{} holds", case());
        for p in predicates {
            let one = new.holds(std::slice::from_ref(p), &fills);
            let was = old.holds(std::slice::from_ref(p), &fills);
            assert_eq!(
                one.clone().map_err(|e| format!("{e:?}")),
                was.map_err(|e| format!("{e:?}")),
                "{} holds {p:?}",
                case()
            );
            if order {
                continue;
            }
            match (&p.test, one) {
                (_, Err(answer::PredicateError::Pointer)) => reached.pointer_error += 1,
                (_, Err(answer::PredicateError::Literal)) => reached.literal_error += 1,
                (_, Err(answer::PredicateError::Hole(_))) => reached.hole_error += 1,
                (_, Ok(_)) if !readable => reached.unreadable_read += 1,
                (Test::Equals(_), Ok(true)) => reached.equal += 1,
                (Test::Equals(_), Ok(false)) => reached.unequal += 1,
                (Test::Contains(_), Ok(true)) => reached.contained += 1,
                (Test::Contains(_), Ok(false)) => reached.not_contained += 1,
                (Test::Exists, _) => {}
            }
        }
        if !order {
            takes(reached);
        }
    }
}

const PROBLEM: (&str, &[u8]) = ("Content-Type", b"application/problem+json");

#[test]
fn the_tape_s_readers_read_as_today_s() {
    let mut d = Draw(0x4b32_6232);
    let mut reached = Reached::default();
    for _ in 0..4000 {
        let v = body(&mut d);
        let mut text = write(&v, &mut d);
        if d.below(10) == 0 {
            text.push_str(" x");
        }
        let mut ps = Vec::new();
        places(&v, String::new(), &mut ps);
        let predicates: Vec<Predicate> = (0..1 + d.below(3))
            .map(|_| predicate(&mut d, &ps))
            .collect();
        let mut captures: Vec<Capture> = (0..1 + d.below(3))
            .map(|_| capture(Source::Json(d.pick(&ps).0.clone())))
            .collect();
        captures.push(capture(Source::Header("ETag".into())));
        captures.push(capture(Source::Location));
        // The strata a capture's refusal and a problem's root reach.
        for (pointer, place) in &ps {
            if let Some(V::Str(s)) = place
                && s.len() > TAKEN
                && !s.contains('\\')
            {
                reached.long_refused += 1;
                captures.push(capture(Source::Json(pointer.clone())));
            }
        }
        let status = *d.pick(&[200, 201, 404, 409, 503]);
        let typed = d.below(5);
        let mut headers: Vec<(String, Vec<u8>)> = match typed {
            0 => vec![],
            1 => vec![("content-type".into(), b"application/json".to_vec())],
            2 => vec![(PROBLEM.0.into(), PROBLEM.1.to_vec()); 2],
            _ => vec![(PROBLEM.0.into(), PROBLEM.1.to_vec())],
        };
        headers.push(("ETag".into(), b"\"v-1\"".to_vec()));
        headers.push(("Location".into(), b"/devices/d-7".to_vec()));
        let problem = *d.pick(&["/code", "", "/0", "/a", "/x~1y"]);
        let bytes = text.into_bytes();
        let body = (d.below(20) != 0).then_some(bytes.as_slice());
        // One problem's media type at 400 or past it, a root not an object.
        if status >= 400 && typed >= 3 && body.is_some() && !matches!(v, V::Obj(_)) {
            reached.code_refused += 1;
        }
        agree(
            status,
            &headers,
            body,
            problem,
            &predicates,
            &captures,
            &mut reached,
        );
    }
    eprintln!("REACHED {reached:?}");
    let Reached {
        equal,
        unequal,
        contained,
        not_contained,
        pointer_error,
        literal_error,
        hole_error,
        unreadable_read,
        code,
        code_refused,
        long_taken,
        long_refused,
        number_taken,
    } = reached;
    for (name, n) in [
        ("equal", equal),
        ("unequal", unequal),
        ("contained", contained),
        ("not contained", not_contained),
        ("a pointer wrong", pointer_error),
        ("a literal wrong", literal_error),
        ("a hole unfilled", hole_error),
        ("an unreadable body read", unreadable_read),
        ("a code", code),
        ("a code at a root not an object", code_refused),
        ("a string of TAKEN bytes taken", long_taken),
        ("a string past TAKEN refused", long_refused),
        ("a number taken", number_taken),
    ] {
        assert!(n >= 10, "{name} reached {n} times");
    }
}

/// The rows the generator reaches rarely, each read by both and its verdict
/// stated.
#[test]
fn hand_rows_read_as_today() {
    let problem = vec![(PROBLEM.0.to_owned(), PROBLEM.1.to_vec())];
    let mut r = Reached::default();
    let p = |pointer: &str, test: Test| Predicate {
        pointer: pointer.to_owned(),
        test,
    };
    let eq = |s: &str| Test::Equals(Value::Literal(s.to_owned()));
    let has = |s: &str| Test::Contains(Value::Literal(s.to_owned()));
    let read = |status, headers: &[(String, Vec<u8>)], body: &[u8]| {
        let reply = Reply {
            status,
            headers,
            body: Some(body),
        };
        let secret = RunSecret::new([0; 32]);
        let fills = Fills {
            namespace: Namespace(0),
            secret: &secret,
            run_start: 0,
            sent: 0,
            captured: &[],
        };
        let got = answer::read(&reply);
        (
            got.code("/0"),
            got.code(""),
            got.code("/code"),
            got.holds(&[p("/n", eq("1"))], &fills),
        )
    };
    // A problem's code at a root not an object: none, either pointer.
    let (a, b, ..) = read(404, &problem, br#"["request.rate_limited"]"#);
    assert_eq!((a, b), (None, None));
    let (_, b, ..) = read(404, &problem, br#""request.rate_limited""#);
    assert_eq!(b, None);
    // An escaped code, decoded; a code past what the journal names, whole.
    let (.., c, _) = read(409, &problem, br#"{"code":"request.rate_limited"}"#);
    assert_eq!(c.as_deref(), Some("request.rate_limited"));
    let long = format!(r#"{{"code":"a.{}"}}"#, "b".repeat(100));
    let (.., c, _) = read(409, &problem, long.as_bytes());
    assert_eq!(c.map(|c| c.len()), Some(102));
    // A number by value, either side written longer.
    for (body, want) in [
        (&br#"{"n":1.0}"#[..], true),
        (br#"{"n":100e-2}"#, true),
        (br#"{"n":1.000000000000000000000000001}"#, false),
    ] {
        assert_eq!(read(200, &[], body).3, Ok(want));
    }
    for (body, preds) in [
        (
            &br#"{"n":100}"#[..],
            vec![p("/n", eq("1e2")), p("/n", eq("1.00e2"))],
        ),
        (
            br#"{"o":{"a":1,"b":2}}"#,
            vec![p("/o", eq(r#"{"b":2,"a":1}"#))],
        ),
        (br#"{"o":{"a":1,"b":2}}"#, vec![p("/o", eq(r#"{"a":1}"#))]),
        (br#"{"o":{"a":1}}"#, vec![p("/o", eq(r#"{"a":1,"b":2}"#))]),
        (
            br#"{"o":{"a":1,"b":2}}"#,
            vec![p("/o", eq(r#"{"a":1,"c":2}"#))],
        ),
        (
            br#"{"l":[1,2]}"#,
            vec![p("/l", eq("[1]")), p("/l", eq("[1,2,3]"))],
        ),
        (
            br#"{"l":[1,[2]]}"#,
            vec![p("/l", has("[2.0]")), p("/l", has("2"))],
        ),
        (br#"{"o":{"a":1}}"#, vec![p("/o", has("1"))]),
        (br#"{"a":1}"#, vec![p("/a", eq("1"))]),
        // A false predicate ahead of one written wrong: the error, as today.
        (br#"{"n":1}"#, vec![p("/n", eq("2")), p("/n", eq("{"))]),
        // A literal written wrong over an unreadable body; a pointer wrong
        // beside a literal wrong.
        (br#"{"n":1,"n":2}"#, vec![p("/n", eq("{"))]),
        (br#"{"n":1}"#, vec![p("a", eq("{"))]),
        (br#"{"n":1} x"#, vec![p("/n", Test::Exists)]),
    ] {
        agree(200, &[], Some(body), "/code", &preds, &[], &mut r);
    }
    // Captures: a malformed pointer, a string at and past what a use carries,
    // escaped so its text is longer than its value.
    let at = format!(
        r#"{{"s":"{}","t":"{}"}}"#,
        "\\u0041".repeat(TAKEN),
        "A".repeat(TAKEN + 1)
    );
    let captures = [
        capture(Source::Json("s".into())),
        capture(Source::Json("/s".into())),
        capture(Source::Json("/t".into())),
    ];
    agree(
        200,
        &[],
        Some(at.as_bytes()),
        "/code",
        &[],
        &captures,
        &mut r,
    );
    let reply = Reply {
        status: 200,
        headers: &[],
        body: Some(at.as_bytes()),
    };
    let new = answer::read(&reply);
    assert_eq!(new.take(&captures[0]), None);
    assert_eq!(new.take(&captures[1]).map(|v| v.len()), Some(TAKEN + 2));
    assert_eq!(new.take(&captures[2]), None);
}

/// An object's equality finds each member by search, not by a walk of the
/// want (K2b-7e-4b-2): twenty thousand members equal to themselves within
/// two seconds on an unoptimised build — a walk is two hundred million
/// comparisons, a search some three hundred thousand.
#[test]
#[allow(clippy::disallowed_methods, clippy::disallowed_types)]
fn an_object_s_equality_is_not_quadratic() {
    use ascetic_ddd_command_log::json::{Sorted, parse, tape};
    let members: Vec<String> = (0..20_000).map(|i| format!("\"m{i:08}\":{i}")).collect();
    let doc = format!("{{{}}}", members.join(","));
    let want = Sorted::new(parse(doc.as_bytes()).unwrap());
    let t = tape(doc.as_bytes()).unwrap();
    let start = std::time::Instant::now();
    assert!(t.root().equals(&want));
    let took = start.elapsed();
    assert!(took.as_secs_f64() < 2.0, "{took:?} for 20,000 members");
}
