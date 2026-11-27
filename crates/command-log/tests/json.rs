//! An answer's JSON (ADR-0047 decision 15, K2a-1): a twin for each rule, and
//! the reader against `vectors/json.py` — Python's `json` and `decimal` — on
//! documents drawn with their mutants, on pointers into them, and on pairs of
//! values equal and not.

use std::io::Write;
use std::process::{Command, Stdio};

use ascetic_ddd_command_log::json::{
    DEPTH, Json, JsonError, Kind, Malformed, Node, parse, tape, tokens,
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;

fn read(s: &str) -> Result<Json, JsonError> {
    parse(s.as_bytes())
}

/// Whether a text is readable — the tape reading it so exactly when the
/// tree does (K2b-7e-4b-1).
fn readable(s: &str) -> bool {
    let tree = read(s);
    let taped = tape(s.as_bytes());
    assert_eq!(taped.is_ok(), tree.is_ok(), "{s:?}");
    if let (Ok(v), Ok(t)) = (&tree, &taped) {
        assert!(alike(t.root(), v), "{s:?}");
    }
    tree.is_ok()
}

/// A tape's value is the tree's: numbers by their text, strings decoded,
/// containers member by member in the order written.
fn alike(n: Node<'_>, v: &Json) -> bool {
    match v {
        Json::Null => n.kind() == Kind::Null,
        Json::Bool(b) => n.kind() == Kind::Bool && n.bool() == Some(*b),
        Json::Number(x) => n.number() == Some(x.text()),
        Json::String(s) => n.string().as_deref() == Some(s.as_str()) && n.is(s),
        Json::Array(a) => {
            n.kind() == Kind::Array
                && n.items().count() == a.len()
                && n.items().zip(a).all(|(m, w)| alike(m, w))
        }
        Json::Object(o) => {
            n.kind() == Kind::Object
                && n.members().count() == o.len()
                && n.members().zip(o).all(|((k, m), (l, w))| {
                    k.string().as_deref() == Some(l.as_str()) && k.is(l) && alike(m, w)
                })
        }
    }
}

/// FNV-1a over bytes, the test's own: the names' check's hash.
fn fnv(s: &str) -> u32 {
    s.bytes().fold(0x811c_9dc5, |h: u32, b| {
        (h ^ u32::from(b)).wrapping_mul(0x0100_0193)
    })
}

/// K2b-7e-4b-1 of ADR-0047 (decision 23): a name is one by its decoded
/// bytes, not by its hash nor by its text — two names of one hash are two,
/// a third equal to the first is a duplicate whatever stands between them,
/// an escape names what it decodes to; each object's names apart from its
/// parent's and its siblings'.
#[test]
fn a_name_is_one_by_its_decoded_bytes() {
    assert_eq!(
        fnv("m9tcqis_"),
        fnv("i7giss29"),
        "the pair no longer collides"
    );
    assert_eq!(fnv("m9tcqis_"), 0x1425_5c12);
    for (doc, ok) in [
        (r#"{"m9tcqis_":0,"i7giss29":0}"#, true),
        (r#"{"m9tcqis_":0,"i7giss29":0,"m9tcqis_":0}"#, false),
        (r#"{"i7giss29":0,"m9tcqis_":0,"i7giss29":0}"#, false),
        (r#"{"a":1,"\u0061":2}"#, false),
        (r#"{"c\u006fde":1,"code":2}"#, false),
        (r#"{"\u00e9":1,"é":2}"#, false),
        (r#"{"\/":1,"/":2}"#, false),
        (r#"{"a":1,"b":2}"#, true),
        (r#"{"a":{"a":1},"b":{"a":2}}"#, true),
        (r#"{"x":{"m9tcqis_":0,"i7giss29":0,"m9tcqis_":1}}"#, false),
        (r#"[{"a":1},{"a":1}]"#, true),
        (r#"{"a":[{"a":1,"a":2}]}"#, false),
    ] {
        assert_eq!(readable(doc), ok, "{doc}");
        if !ok {
            assert!(
                matches!(read(doc), Err(JsonError::Duplicate { .. })),
                "{doc}"
            );
        }
    }
}

/// The densest documents read alike on the tape: zeros of both parities,
/// chains as deep as an answer nests, short distinct names.
#[test]
fn the_densest_documents_read_alike() {
    for k in [0usize, 1, 2, 999, 1_000] {
        assert!(readable(&format!("[{}]", vec!["0"; k].join(","))));
        let names: Vec<String> = (0..k).map(|i| format!("\"{i}\":{i}")).collect();
        assert!(readable(&format!("{{{}}}", names.join(","))));
    }
    let chain = format!("{}1{}", "[".repeat(DEPTH), "]".repeat(DEPTH));
    assert!(readable(&chain));
    let objects = format!("{}1{}", "{\"\":".repeat(DEPTH), "}".repeat(DEPTH));
    assert!(readable(&objects));
}

fn equal(a: &str, b: &str) -> bool {
    read(a).unwrap() == read(b).unwrap()
}

#[test]
fn rfc_8259_strictly() {
    for ok in [
        "0",
        "-0",
        "1.5e-3",
        "\"\"",
        "null",
        "true",
        "[]",
        "{}",
        " \t\r\n[1, {\"a\": null}] ",
        "\"\\u0000\"",
        "\"\u{7f}\"",
        "\"\\ud83d\\ude00\"",
        "1E+2",
    ] {
        assert!(readable(ok), "{ok}");
    }
    for bad in [
        "",
        " ",
        "01",
        "-",
        "1.",
        ".1",
        "1e",
        "+1",
        "NaN",
        "Infinity",
        "[1,]",
        "{\"a\":1,}",
        "[1] 2",
        "\u{feff}1",
        "'a'",
        "\"\t\"",
        "\"\\x\"",
        "\"\\u12\"",
        "{a:1}",
        "nul",
        "[",
        "/* */1",
        "\"a",
        "1 /",
    ] {
        assert!(!readable(bad), "{bad:?}");
    }
    assert_eq!(parse(b"\"\xff\""), Err(JsonError::Utf8));
}

#[test]
fn half_of_a_surrogate_pair_is_unreadable() {
    for bad in [
        "\"\\ud800\"",
        "\"\\udc00\"",
        "\"\\ud800\\u0041\"",
        "\"\\ud800x\"",
    ] {
        assert!(
            matches!(read(bad), Err(JsonError::Surrogate { .. })),
            "{bad}"
        );
    }
    assert!(matches!(read("\"\\uD83D\\uDE00\""), Ok(Json::String(s)) if s == "\u{1F600}"));
}

#[test]
fn a_member_named_twice_is_unreadable() {
    assert!(matches!(
        read("{\"code\":\"a\",\"code\":\"b\"}"),
        Err(JsonError::Duplicate { .. })
    ));
    // The same name written two ways.
    assert!(matches!(
        read("{\"a\":1,\"\\u0061\":1}"),
        Err(JsonError::Duplicate { .. })
    ));
    assert!(readable("{\"a\":{\"a\":1}}"));
}

#[test]
fn nesting_past_the_depth_is_unreadable() {
    let nest = |n: usize| format!("{}{}", "[".repeat(n), "]".repeat(n));
    assert!(readable(&nest(DEPTH)));
    assert_eq!(read(&nest(DEPTH + 1)), Err(JsonError::Depth));
    let objects = |n: usize| format!("{}1{}", "{\"a\":".repeat(n), "}".repeat(n));
    assert!(readable(&objects(DEPTH)));
    assert_eq!(read(&objects(DEPTH + 1)), Err(JsonError::Depth));
}

#[test]
fn an_exponent_past_its_digits_is_unreadable() {
    assert!(readable("1e999999999999999999"));
    assert!(readable("1e0000000000000000000000001"));
    assert!(matches!(
        read("1e1000000000000000000"),
        Err(JsonError::Exponent { .. })
    ));
    // Read, where serde_json's default build refuses it.
    assert!(readable("1e400"));
}

#[test]
fn numbers_are_equal_by_their_decimal_values() {
    for (a, b) in [
        ("1", "1.0"),
        ("1", "10e-1"),
        ("100", "1e2"),
        ("0", "-0"),
        ("0", "-0.0e7"),
        ("0.1e1", "1"),
        ("12345678901234567890123", "1.2345678901234567890123e22"),
        ("1e400", "10e399"),
    ] {
        assert!(equal(a, b), "{a} = {b}");
    }
    for (a, b) in [
        ("1", "-1"),
        ("1", "1.0000000000000000000001"),
        ("12345678901234567890123", "12345678901234567890124"),
        ("1e400", "1e401"),
        ("1", "\"1\""),
        ("1", "true"),
        ("0", "null"),
    ] {
        assert!(!equal(a, b), "{a} ≠ {b}");
    }
    let Ok(Json::Number(n)) = read("-0.50e+1") else {
        panic!("a number");
    };
    assert_eq!(n.text(), "-0.50e+1");
}

#[test]
fn values_are_equal_member_by_member_in_any_order() {
    assert!(equal("{\"a\":1,\"b\":[1,2]}", "{\"b\":[1.0,2],\"a\":1}"));
    assert!(!equal("[1,2]", "[2,1]"));
    assert!(!equal("{\"a\":1}", "{\"a\":1,\"b\":2}"));
    assert!(!equal("{\"a\":1,\"b\":2}", "{\"a\":1,\"c\":2}"));
    assert!(equal("\"A\"", "\"\\u0041\""));
    assert!(equal("\"/\"", "\"\\/\""));
}

#[test]
fn pointers_are_rfc_6901() {
    let doc = read("{\"a\":[10,{\"b~c/d\":true}],\"-\":1,\"\":2,\"01\":3}").unwrap();
    let at = |p: &str| doc.at(p).map(|v| v.cloned());
    assert_eq!(at(""), Ok(Some(doc.clone())));
    assert_eq!(at("/a/0"), Ok(Some(read("10").unwrap())));
    assert_eq!(at("/a/1/b~0c~1d"), Ok(Some(Json::Bool(true))));
    assert_eq!(at("/-"), Ok(Some(read("1").unwrap())));
    assert_eq!(at("/"), Ok(Some(read("2").unwrap())));
    assert_eq!(at("/01"), Ok(Some(read("3").unwrap())));
    // An array's element: by an index without leading zeros, never `-`.
    for absent in [
        "/a/-",
        "/a/01",
        "/a/2",
        "/a/+1",
        "/a/0/x",
        "/b",
        "/a/1/b~0c/d",
    ] {
        assert_eq!(at(absent), Ok(None), "{absent}");
    }
    for malformed in ["a", "/a~", "/a~2", "/~x"] {
        assert_eq!(at(malformed), Err(Malformed), "{malformed}");
    }
    assert_eq!(
        tokens("/a~1b/~0"),
        Ok(vec!["a/b".to_owned(), "~".to_owned()])
    );
}

#[test]
fn a_value_s_debug_shows_only_its_shape() {
    let doc = read("{\"token\":\"s3cr3t-value\",\"n\":123456}").unwrap();
    let shown = format!("{doc:?} {:?}", doc.at("/token"));
    assert!(
        !shown.contains("s3cr3t") && !shown.contains("123456"),
        "{shown}"
    );
}

// The differential.

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

const NUMBERS: &[&str] = &[
    "0",
    "-0",
    "0.0",
    "-0.0",
    "1",
    "1.0",
    "10e-1",
    "1E1",
    "100",
    "1e2",
    "0.1e1",
    "1e400",
    "-1e-400",
    "12345678901234567890123",
    "1e000000000000000000001",
    "1e1234567890123456789",
    "0e999999999999999999",
    "-5",
    "2.50",
    "18446744073709551616",
    "-9223372036854775809",
    "3.14159",
    "1e-7",
    "7E+0",
    "0.05",
    "5e-2",
    "-0.0500e1",
    "0.050",
];

const PIECES: &[&str] = &[
    "a",
    "b",
    "code",
    "é",
    "\u{1F600}",
    "\\n",
    "\\u0041",
    "\\ud83d\\ude00",
    "\\ud800",
    "\\udc00",
    "\\/",
    "\\\\",
    "\\\"",
    "~",
    "/",
    "-",
    "0",
    " ",
    "\u{7f}",
];

/// Names as written: some equal only decoded (`a` and `\u0061`), a pair of
/// one hash (`m9tcqis_`, `i7giss29`), not ASCII (K2b-7e-4b-1).
const NAMES: &[&str] = &[
    "a",
    "b",
    "code",
    "-",
    "",
    "x/y",
    "m~n",
    "0",
    "\\u0061",
    "c\\u006fde",
    "é",
    "\\u00e9",
    "m9tcqis_",
    "i7giss29",
];

/// A value drawn, before it is written.
enum G {
    Word(&'static str),
    Number(&'static str),
    String(Vec<&'static str>),
    Array(Vec<G>),
    Object(Vec<(&'static str, G)>),
}

fn draw(d: &mut Draw, depth: usize) -> G {
    if depth >= 4 || d.below(3) == 0 {
        return match d.below(5) {
            0 => G::Word(d.pick(&["null", "true", "false"])),
            1 | 2 => G::Number(d.pick(NUMBERS)),
            _ => G::String((0..d.below(4)).map(|_| *d.pick(PIECES)).collect()),
        };
    }
    let n = d.below(4);
    if d.below(2) == 0 {
        G::Array((0..n).map(|_| draw(d, depth + 1)).collect())
    } else {
        // One object in eight may name a member twice.
        let twice = d.below(8) == 0;
        let mut names: Vec<&'static str> = NAMES.to_vec();
        let mut members = Vec::new();
        for _ in 0..n.min(names.len()) {
            let i = d.below(names.len());
            let name = if twice { names[i] } else { names.remove(i) };
            members.push((name, draw(d, depth + 1)));
        }
        G::Object(members)
    }
}

/// A value's text; with `style`, another text of the same value: numbers
/// rewritten, letters escaped, members reordered.
fn write(g: &G, style: &mut Option<Draw>) -> String {
    match g {
        G::Word(w) => (*w).to_owned(),
        G::Number(n) => match style {
            None => (*n).to_owned(),
            Some(d) => {
                let plain = !n.contains(['e', 'E']);
                match d.below(4) {
                    1 if plain => format!("{n}e0"),
                    2 if plain && !n.contains('.') => format!("{n}.000"),
                    // A digit more and a tenth: the same value for an
                    // integer only.
                    3 if plain && !n.contains('.') => format!("{n}0e-1"),
                    _ => (*n).to_owned(),
                }
            }
        },
        G::String(pieces) => {
            let body: String = pieces
                .iter()
                .map(|p| {
                    let letter = p.len() == 1 && p.as_bytes()[0].is_ascii_lowercase();
                    match style {
                        Some(d) if letter => {
                            if d.below(2) == 0 {
                                format!("\\u{:04x}", p.as_bytes()[0])
                            } else {
                                (*p).to_owned()
                            }
                        }
                        _ => (*p).to_owned(),
                    }
                })
                .collect();
            format!("\"{body}\"")
        }
        G::Array(items) => {
            let items: Vec<String> = items.iter().map(|x| write(x, style)).collect();
            format!("[{}]", items.join(", "))
        }
        G::Object(members) => {
            let mut members: Vec<String> = members
                .iter()
                .map(|(k, v)| format!("\"{k}\": {}", write(v, style)))
                .collect();
            if let Some(d) = style {
                for i in (1..members.len()).rev() {
                    members.swap(i, d.below(i + 1));
                }
            }
            format!("{{{}}}", members.join(","))
        }
    }
}

const MUTANTS: &[&[u8]] = &[
    b",",
    b"]",
    b"}",
    b"\"",
    b"\\",
    b" ",
    b"\x00",
    b"\xff",
    b"-",
    b".",
    b"e",
    b"0",
    b"[",
    b"{",
    b":",
    b"\xef\xbb\xbf",
];

fn mutated(d: &mut Draw, text: &str) -> Vec<u8> {
    let mut bytes = text.as_bytes().to_vec();
    if bytes.is_empty() {
        return bytes;
    }
    let i = d.below(bytes.len());
    match d.below(3) {
        0 => {
            bytes.remove(i);
        }
        1 => {
            let m = d.pick(MUTANTS);
            bytes.splice(i..i, m.iter().copied());
        }
        _ => {
            let m = d.pick(MUTANTS);
            bytes.splice(i..i + 1, m.iter().copied());
        }
    }
    bytes
}

fn hex(s: &str) -> String {
    s.bytes().map(|b| format!("{b:02x}")).collect()
}

/// The canonical form `vectors/json.py` describes.
fn canon(v: &Json) -> String {
    match v {
        Json::Null => "z".to_owned(),
        Json::Bool(true) => "t".to_owned(),
        Json::Bool(false) => "f".to_owned(),
        Json::Number(n) => {
            // Read back from the number's own text by the test, so that the
            // crate's value is judged by its equality only.
            let text = n.text();
            let (negative, rest) = match text.strip_prefix('-') {
                Some(r) => (true, r),
                None => (false, text),
            };
            let (mantissa, exp) = match rest.find(['e', 'E']) {
                Some(i) => (&rest[..i], rest[i + 1..].trim_start_matches('+')),
                None => (rest, "0"),
            };
            let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
            let all = format!("{int}{frac}");
            let lead = all.trim_start_matches('0');
            let digits = lead.trim_end_matches('0');
            if digits.is_empty() {
                return "n:0".to_owned();
            }
            let exp: i128 = exp.parse().unwrap();
            let e = exp - frac.len() as i128 + (lead.len() - digits.len()) as i128;
            format!("n:{}{digits}e{e}", if negative { "-" } else { "" })
        }
        Json::String(s) => format!("s:{}", hex(s)),
        Json::Array(a) => format!("[{}]", a.iter().map(canon).collect::<Vec<_>>().join(",")),
        Json::Object(o) => {
            let mut o: Vec<&(String, Json)> = o.iter().collect();
            o.sort_by(|a, b| a.0.cmp(&b.0));
            let members: Vec<String> = o
                .iter()
                .map(|(k, v)| format!("{}:{}", hex(k), canon(v)))
                .collect();
            format!("{{{}}}", members.join(","))
        }
    }
}

/// The strata `g` reaches: objects naming two members equal only decoded,
/// and objects naming both names of one hash.
fn strata_of(g: &G, counts: &mut [usize; 2]) {
    match g {
        G::Array(items) => items.iter().for_each(|x| strata_of(x, counts)),
        G::Object(members) => {
            let decoded: Vec<Option<Json>> = members
                .iter()
                .map(|(k, _)| parse(format!("\"{k}\"").as_bytes()).ok())
                .collect();
            let twins = members
                .iter()
                .zip(&decoded)
                .enumerate()
                .any(|(i, ((k, _), a))| {
                    members
                        .iter()
                        .zip(&decoded)
                        .skip(i + 1)
                        .any(|((l, _), b)| k != l && a == b)
                });
            counts[0] += usize::from(twins);
            let names: Vec<&str> = members.iter().map(|(k, _)| *k).collect();
            counts[1] += usize::from(names.contains(&"m9tcqis_") && names.contains(&"i7giss29"));
            members.iter().for_each(|(_, x)| strata_of(x, counts));
        }
        _ => {}
    }
}

/// Pointers into a value: its every place, and tokens beside them.
fn pointers(v: &Json, here: String, out: &mut Vec<String>) {
    out.push(here.clone());
    match v {
        Json::Array(a) => {
            for (i, x) in a.iter().enumerate() {
                pointers(x, format!("{here}/{i}"), out);
            }
            for t in ["-", "01", &a.len().to_string(), "x"] {
                out.push(format!("{here}/{t}"));
            }
        }
        Json::Object(o) => {
            for (k, x) in o {
                let escaped = k.replace('~', "~0").replace('/', "~1");
                pointers(x, format!("{here}/{escaped}"), out);
            }
            for t in ["-", "missing", "~2", "m~n"] {
                out.push(format!("{here}/{t}"));
            }
        }
        _ => out.push(format!("{here}/0")),
    }
}

fn by_definition(cases: &[serde_json::Value]) -> String {
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/vectors/json.py");
    let mut child = Command::new("python3")
        .args(["-I", script])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("python3, which the library's gate requires");
    {
        let mut stdin = child.stdin.take().unwrap();
        for c in cases {
            if writeln!(stdin, "{c}").is_err() {
                break;
            }
        }
    }
    let out = child.wait_with_output().unwrap();
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        out.status.success(),
        "{text}{}",
        String::from_utf8_lossy(&out.stderr)
    );
    text
}

#[test]
fn the_reader_agrees_with_python_s_json_and_decimal() {
    let mut d = Draw(0x4b32_6131);
    let mut cases = Vec::new();
    let (mut readable, mut unreadable, mut equal, mut unequal) = (0, 0, 0, 0);
    let mut docs: Vec<(Vec<u8>, Json)> = Vec::new();
    // Objects naming two members equal only decoded, and the pair of one
    // hash (K2b-7e-4b-1).
    let mut strata = [0usize; 2];
    for i in 0..6000 {
        let g = draw(&mut d, 0);
        strata_of(&g, &mut strata);
        let mut text = write(&g, &mut None);
        if i % 40 == 0 {
            let n = DEPTH - 2 + d.below(5);
            text = format!("{}{text}{}", "[".repeat(n), "]".repeat(n));
        }
        if d.below(4) == 0 {
            text = format!(" \n{text}\t\r ");
        }
        let bytes = if d.below(3) == 0 {
            mutated(&mut d, &text)
        } else {
            text.into_bytes()
        };
        let read = parse(&bytes);
        let taped = tape(&bytes);
        assert_eq!(
            taped.is_ok(),
            read.is_ok(),
            "{:?}",
            String::from_utf8_lossy(&bytes)
        );
        let mut at = Vec::new();
        if let (Ok(v), Ok(t)) = (&read, &taped) {
            readable += 1;
            assert!(alike(t.root(), v), "{:?}", String::from_utf8_lossy(&bytes));
            let mut ps = Vec::new();
            pointers(v, String::new(), &mut ps);
            ps.extend(["a", "/a~", "/~2x"].map(str::to_owned));
            for p in &ps {
                let agree = match (v.at(p), t.root().at(p)) {
                    (Err(Malformed), Err(Malformed)) | (Ok(None), Ok(None)) => true,
                    (Ok(Some(x)), Ok(Some(y))) => alike(y, x),
                    _ => false,
                };
                assert!(agree, "{:?} at {p:?}", String::from_utf8_lossy(&bytes));
            }
            for p in ps.into_iter().take(40) {
                let got = match v.at(&p) {
                    Err(Malformed) => "malformed".to_owned(),
                    Ok(None) => "absent".to_owned(),
                    Ok(Some(x)) => canon(x),
                };
                at.push(serde_json::json!([p, got]));
            }
            docs.push((bytes.clone(), v.clone()));
        } else {
            unreadable += 1;
        }
        cases.push(serde_json::json!({
            "doc": STANDARD.encode(&bytes),
            "canon": read.as_ref().ok().map(canon),
            "at": at,
        }));
    }
    // Pairs: one value written two ways, and values drawn apart.
    for _ in 0..3000 {
        let g = draw(&mut d, 0);
        let a = write(&g, &mut None);
        let b = write(&g, &mut Some(Draw(d.next())));
        let (Ok(x), Ok(y)) = (parse(a.as_bytes()), parse(b.as_bytes())) else {
            continue;
        };
        let e = x == y;
        if e {
            equal += 1
        } else {
            unequal += 1
        }
        cases.push(
            serde_json::json!({"a": STANDARD.encode(&a), "b": STANDARD.encode(&b), "equal": e}),
        );
    }
    for _ in 0..3000 {
        let (a, x) = &docs[d.below(docs.len())];
        let (b, y) = &docs[d.below(docs.len())];
        let e = x == y;
        if e {
            equal += 1
        } else {
            unequal += 1
        }
        cases.push(
            serde_json::json!({"a": STANDARD.encode(a), "b": STANDARD.encode(b), "equal": e}),
        );
    }
    for (name, n) in [
        ("readable", readable),
        ("unreadable", unreadable),
        ("equal", equal),
        ("unequal", unequal),
        (
            "objects naming two members equal only decoded",
            strata[0] * 5,
        ),
        ("objects naming the pair of one hash", strata[1] * 5),
    ] {
        assert!(n > 100, "{n} {name}");
    }
    let reached = by_definition(&cases);
    for rule in [
        "duplicate",
        "surrogate",
        "depth",
        "exponent",
        "utf8",
        "syntax",
        "malformed",
        "absent",
        "found",
    ] {
        // Each rule reached ten times or more, not merely once.
        let count = reached
            .split(&format!("'{rule}': "))
            .nth(1)
            .and_then(|r| r.split([',', '}']).next())
            .and_then(|n| n.trim().parse::<usize>().ok());
        assert!(
            count.is_some_and(|n| n >= 10),
            "{rule} reached {count:?} times: {reached}"
        );
    }
    println!("{reached}");
}
