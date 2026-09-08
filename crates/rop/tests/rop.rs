//! Tests for the railway toolkit, function by function.

use ascetic_ddd_rop::{
    Errors, Rop, RopExt, all, and_also, both, compose, either, fail, plus, succeed, switch, tee,
};

#[derive(Clone, Debug, PartialEq, Eq)]
enum Invalid {
    Symbol,
    Side,
    Quantity,
}

fn symbol(s: &str) -> Rop<String, Invalid> {
    if s.is_empty() {
        fail(Invalid::Symbol)
    } else {
        succeed(s.to_uppercase())
    }
}

fn side(s: &str) -> Rop<bool, Invalid> {
    match s {
        "buy" => succeed(true),
        "sell" => succeed(false),
        _ => fail(Invalid::Side),
    }
}

fn quantity(q: i64) -> Rop<u32, Invalid> {
    u32::try_from(q)
        .ok()
        .filter(|q| *q > 0)
        .ok_or_else(|| Errors::new(Invalid::Quantity))
}

// ------------------------------- constructors -------------------------------

#[test]
fn succeed_and_fail_build_the_two_tracks() {
    assert_eq!(succeed::<_, Invalid>(1), Ok(1));
    assert_eq!(
        fail::<i32, _>(Invalid::Side),
        Err(Errors::new(Invalid::Side))
    );
}

/// `?` on a plain `Result` lands on the failure track as one error: OCaml's
/// `of_result`, for free.
#[test]
fn a_plain_result_lifts_through_question_mark() {
    fn parse(s: &str) -> Rop<i64, std::num::ParseIntError> {
        let n = s.parse::<i64>()?;
        succeed(n)
    }
    assert_eq!(parse("7"), Ok(7));
    assert_eq!(parse("x").unwrap_err().count(), 1);
}

// ------------------------------ accumulation ------------------------------

#[test]
fn both_reports_the_errors_of_both_sides_in_order() {
    let ok: Rop<i32, &str> = succeed(1);
    let bad_a: Rop<i32, &str> = fail("a");
    let bad_b: Rop<i32, &str> = fail("b");

    assert_eq!(both(ok.clone(), succeed(2)), Ok((1, 2)));
    assert_eq!(
        both(bad_a.clone(), ok.clone()).unwrap_err().into_vec(),
        ["a"]
    );
    assert_eq!(both(ok, bad_b.clone()).unwrap_err().into_vec(), ["b"]);
    assert_eq!(both(bad_a, bad_b).unwrap_err().into_vec(), ["a", "b"]);
}

/// The applicative half: independent fields, every failure reported.
#[test]
fn all_reports_every_failing_branch() {
    let errors = all!(symbol(""), side("hold"), quantity(0)).unwrap_err();
    assert_eq!(
        errors.into_vec(),
        [Invalid::Symbol, Invalid::Side, Invalid::Quantity]
    );

    let errors = all!(symbol("sber"), side("hold"), quantity(10)).unwrap_err();
    assert_eq!(errors.into_vec(), [Invalid::Side]);
}

#[test]
fn all_yields_a_flat_tuple_when_everything_succeeds() {
    assert_eq!(
        all!(symbol("sber"), side("buy"), quantity(10)),
        Ok(("SBER".to_owned(), true, 10)),
    );
    let eight: Rop<_, Invalid> = all!(
        succeed(1),
        succeed(2),
        succeed(3),
        succeed(4),
        succeed(5),
        succeed(6),
        succeed(7),
        succeed(8)
    );
    assert_eq!(eight, Ok((1, 2, 3, 4, 5, 6, 7, 8)));
}

/// The monadic half: dependent steps, the first failure stops the pipeline.
#[test]
fn question_mark_stops_at_the_first_failure() {
    fn pipeline(s: &str, d: &str) -> Rop<(String, bool), Invalid> {
        let symbol = symbol(s)?;
        let side = side(d)?; // not reached when the symbol is invalid
        succeed((symbol, side))
    }
    assert_eq!(
        pipeline("", "hold").unwrap_err().into_vec(),
        [Invalid::Symbol]
    );
    assert_eq!(pipeline("sber", "buy"), Ok(("SBER".to_owned(), true)));
}

#[test]
fn apply_feeds_a_wrapped_argument_to_a_wrapped_function() {
    let add_one: Rop<fn(i32) -> i32, &str> = succeed(|x| x + 1);
    assert_eq!(add_one.apply(succeed(1)), Ok(2));

    let broken: Rop<fn(i32) -> i32, &str> = fail("no function");
    assert_eq!(
        broken.apply(fail("no argument")).unwrap_err().into_vec(),
        ["no function", "no argument"],
    );
}

// -------------------------------- adapters --------------------------------

#[test]
fn either_picks_the_track() {
    let describe = |r: Rop<i32, &str>| either(|v| format!("ok {v}"), |e| format!("bad {e}"), r);
    assert_eq!(describe(succeed(1)), "ok 1");
    assert_eq!(describe(fail("x")), "bad x");
}

#[test]
fn switch_lifts_a_plain_function() {
    let double = switch::<_, _, Invalid>(|x: i32| x * 2);
    assert_eq!(double(21), Ok(42));
}

#[test]
fn tee_lets_the_value_through_after_the_side_effect() {
    let seen = std::cell::Cell::new(0);
    let log = tee(|x: &i32| seen.set(*x));
    assert_eq!(log(7), 7);
    assert_eq!(seen.get(), 7);
}

#[test]
fn compose_chains_switches() {
    let parse = |s: &str| s.parse::<i64>().map_err(|_| Errors::new("not a number"));
    let positive = |n: i64| {
        if n > 0 {
            succeed(n)
        } else {
            fail("not positive")
        }
    };
    let parse_positive = compose(parse, positive);

    assert_eq!(parse_positive("7"), Ok(7));
    assert_eq!(
        parse_positive("-7").unwrap_err().into_vec(),
        ["not positive"]
    );
    assert_eq!(
        parse_positive("x").unwrap_err().into_vec(),
        ["not a number"]
    );
}

#[test]
fn double_map_transforms_both_tracks() {
    let ok: Rop<i32, &str> = succeed(1);
    assert_eq!(ok.double_map(|v| v + 1, str::len), Ok(2));

    let bad: Rop<i32, &str> = Err(Errors::new("ab").push("cde"));
    assert_eq!(
        bad.double_map(|v| v + 1, str::len).unwrap_err().into_vec(),
        [2, 3]
    );
}

// ---------------------------------- plus ----------------------------------

#[test]
fn plus_joins_two_switches_over_one_input() {
    let non_empty = |s: &String| {
        if s.is_empty() {
            fail("empty")
        } else {
            succeed(s.len())
        }
    };
    let short = |s: &String| {
        if s.len() > 4 {
            fail("long")
        } else {
            succeed(s.len())
        }
    };
    let joined = plus(|a, b| a + b, Errors::merge, non_empty, short);

    assert_eq!(joined(&"abc".to_owned()), Ok(6));
    assert_eq!(joined(&String::new()).unwrap_err().into_vec(), ["empty"]);
    assert_eq!(
        joined(&"abcdef".to_owned()).unwrap_err().into_vec(),
        ["long"]
    );
}

#[test]
fn and_also_keeps_the_first_value_and_reports_all_failures() {
    let non_empty = |s: &String| {
        if s.is_empty() {
            fail("empty")
        } else {
            succeed(s.clone())
        }
    };
    let ascii = |s: &String| {
        if s.is_ascii() {
            succeed(s.clone())
        } else {
            fail("not ascii")
        }
    };
    let validate = and_also(non_empty, ascii);

    assert_eq!(validate(&"abc".to_owned()), Ok("abc".to_owned()));
    let both_failed = and_also(
        |_: &String| fail::<String, _>("first"),
        |_: &String| fail::<String, _>("second"),
    );
    assert_eq!(
        both_failed(&"x".to_owned()).unwrap_err().into_vec(),
        ["first", "second"]
    );
}

// --------------------------------- errors ---------------------------------

#[test]
fn errors_are_never_empty() {
    assert!(Errors::<i32>::from_vec(vec![]).is_none());
    let errors = Errors::from_vec(vec![1, 2]).unwrap();
    assert_eq!(errors.count(), 2);
    assert_eq!(*errors.first(), 1);
    assert_eq!(errors.iter().copied().collect::<Vec<_>>(), [1, 2]);
}

#[test]
fn errors_display_joined_by_semicolons() {
    let errors = Errors::new("a").push("b").merge(Errors::new("c"));
    assert_eq!(errors.to_string(), "a; b; c");
}
