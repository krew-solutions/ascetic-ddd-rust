//! R1 of ADR-0046: the offered-load rules (decision 14), each by a minimal log
//! beside its twin, through the validator. The curve's own boundaries are the
//! unit tests of `src/rate.rs`.

mod fixture;

use ascetic_ddd_command_log::record::{
    By, Caller, Hole, Place, Record, Rest, TimeBase, TimeRendering, Value,
};
use ascetic_ddd_command_log::validate::{NoHost, Report, Rule};
use fixture::*;

fn verdict(report: &Report) -> Vec<(u64, Rule)> {
    let mut v: Vec<(u64, Rule)> = report.violations.iter().map(|v| (v.line, v.rule)).collect();
    v.sort();
    v.dedup();
    v
}

fn check(
    limits: Vec<ascetic_ddd_command_log::record::Limit>,
    records: &[Record],
) -> Vec<(u64, Rule)> {
    verdict(&run_with(limited(limits, 2, 0, 0), records, &NoHost, 1_000))
}

const SECOND: u64 = 1_000_000;

fn all(requests: u32, per_seconds: u32, burst: u32) -> Vec<ascetic_ddd_command_log::record::Limit> {
    vec![limit("all", By::All, requests, per_seconds, burst)]
}

#[test]
fn a_bucket_admits_its_burst_and_refuses_the_next() {
    let three: Vec<Record> = (0..3).map(|n| call("read", 1, n, 0, 0)).collect();
    assert_eq!(check(all(2, 1, 3), &three), vec![]);
    let mut four = three.clone();
    four.push(call("read", 1, 3, 0, 0));
    assert_eq!(check(all(2, 1, 3), &four), vec![(5, Rule::Rate)]);
}

#[test]
fn one_bucket_name_is_one_counter_across_operations() {
    let shared = || vec![limit("shared", By::All, 1, 10, 2)];
    let two = [call("read", 1, 0, 0, 0), call("update", 1, 0, 0, 0)];
    assert_eq!(check(shared(), &two), vec![]);
    let three = [
        call("read", 1, 0, 0, 0),
        call("read", 1, 1, 0, 0),
        call("update", 1, 0, 0, 0),
    ];
    assert_eq!(check(shared(), &three), vec![(4, Rule::Rate)]);
}

#[test]
fn a_bucket_counts_each_value_apart() {
    let per_group = || {
        vec![limit(
            "group",
            By::At(Place::Path("group".into())),
            1,
            10,
            1,
        )]
    };
    assert_eq!(
        check(
            per_group(),
            &[call("read", 1, 0, 0, 0), call("read", 2, 0, 0, 0)]
        ),
        vec![]
    );
    assert_eq!(
        check(
            per_group(),
            &[call("read", 1, 0, 0, 0), call("read", 1, 1, 0, 0)]
        ),
        vec![(3, Rule::Rate)]
    );
    // One subject through a rendered id and a captured value is one value.
    let as_hole = |r: Record, hole: Hole| {
        let mut r = r;
        if let ascetic_ddd_command_log::record::Action::Rest(rest) = &mut r.action {
            rest.path[0] = Value::Hole(hole);
        }
        r
    };
    let first = as_hole(
        call("read", 1, 0, 0, 0),
        Hole::Rendered {
            of: ascetic_ddd_command_log::record::Of::Subject(subject("group", 1)),
            // Not its `id`, which the stand captures: a value that names it.
            role: "name".into(),
            rendering: ascetic_ddd_command_log::record::Rendering::UuidText,
        },
    );
    let h = limited(per_group(), 2, 0, 0);
    let creator = captures_id(step(id("group", 1, "made", 0), 0));
    let second = after(
        as_hole(
            call("read", 1, 1, 1, 0),
            Hole::Captured {
                subject: subject("group", 1),
                slot: "id".into(),
                from: creator.step.clone(),
            },
        ),
        &creator.step,
        ascetic_ddd_command_log::record::On::Expected,
    );
    let report = run_with(h, &[first, creator, second], &NoHost, 1_000);
    assert_eq!(
        verdict(&report),
        vec![(4, Rule::Rate)],
        "{:?}",
        report.violations
    );
}

fn captures_id(mut r: Record) -> Record {
    r.capture.push(ascetic_ddd_command_log::record::Capture {
        subject: subject("group", 1),
        slot: "id".into(),
        source: ascetic_ddd_command_log::record::Source::Location,
        secret: false,
    });
    r
}

#[test]
fn a_source_is_a_value_and_must_be_one_the_profile_has() {
    let per_source = || vec![limit("address", By::Source, 1, 10, 1)];
    assert_eq!(
        check(
            per_source(),
            &[call("read", 1, 0, 0, 0), call("read", 1, 1, 0, 1)]
        ),
        vec![]
    );
    assert_eq!(
        check(per_source(), &[call("read", 1, 0, 0, 2)]),
        vec![(2, Rule::Key)]
    );
}

fn with(r: Record, edit: impl FnOnce(&mut Rest)) -> Record {
    let mut r = r;
    if let ascetic_ddd_command_log::record::Action::Rest(rest) = &mut r.action {
        edit(rest);
    }
    r
}

#[test]
fn a_value_the_call_does_not_give_is_a_key_violation() {
    let at = |place: Place| vec![limit("k", By::At(place), 1, 1, 1)];
    let plain = call("read", 1, 0, 0, 0);
    assert_eq!(
        check(at(Place::Query("dry".into())), std::slice::from_ref(&plain)),
        vec![(2, Rule::Key)]
    );
    assert_eq!(
        check(
            at(Place::Header("X-Tenant".into())),
            std::slice::from_ref(&plain)
        ),
        vec![(2, Rule::Key)]
    );
    assert_eq!(
        check(
            at(Place::Body("/tenant".into())),
            std::slice::from_ref(&plain)
        ),
        vec![(2, Rule::Key)]
    );
    assert_eq!(
        check(
            at(Place::Credential("tenant".into())),
            std::slice::from_ref(&plain)
        ),
        vec![(2, Rule::Key)]
    );
    let timed = with(plain.clone(), |rest| {
        rest.path[0] = Value::Hole(Hole::Time {
            base: TimeBase::Sent,
            offset: 0,
            rendering: TimeRendering::Rfc3339Micros,
        });
    });
    assert_eq!(
        check(at(Place::Path("group".into())), &[timed]),
        vec![(2, Rule::Key)]
    );
    // Two names of the kind are as bad as none; one is a value.
    let named = |names: Vec<ascetic_ddd_command_log::record::Subject>| {
        with(plain.clone(), |rest| {
            rest.caller = Some(Caller {
                principal: subject("principal", 1),
                names,
            });
        })
    };
    let credential = || at(Place::Credential("tenant".into()));
    assert_eq!(
        check(credential(), &[named(vec![subject("tenant", 1)])]),
        vec![]
    );
    assert_eq!(
        check(
            credential(),
            &[named(vec![subject("tenant", 1), subject("tenant", 2)])]
        ),
        vec![(2, Rule::Key)]
    );
    // No deviation exempts a key, and the deviation is then unused.
    assert_eq!(
        check(at(Place::Query("dry".into())), &[deviating(plain, "key")]),
        vec![(2, Rule::Deviation), (2, Rule::Key)]
    );
}

#[test]
fn polls_count_to_the_timeout() {
    // One a ten seconds: only the burst counts.
    let polls = |every: u64, timeout: u64, burst: u32| {
        check(all(1, 10, burst), &[polling(1, 0, 0, every, timeout)])
    };
    // 5 s every 2 s: polls at 0, 2 and 4 s.
    assert_eq!(polls(2 * SECOND, 5 * SECOND, 2), vec![(2, Rule::Rate)]);
    assert_eq!(polls(2 * SECOND, 5 * SECOND, 3), vec![]);
    // 4 s every 2 s: polls at 0, 2 and 4 s — the timeout's own poll too.
    assert_eq!(polls(2 * SECOND, 4 * SECOND, 2), vec![(2, Rule::Rate)]);
    // Every doubled: two polls.
    assert_eq!(polls(4 * SECOND, 5 * SECOND, 2), vec![]);
}

#[test]
fn an_observation_implies_at_most_two_to_the_twenty_polls() {
    let polls = |timeout: u64| verdict(&run(&[polling(1, 0, 0, 1, timeout)]));
    assert_eq!(polls((1 << 20) - 1), vec![]);
    assert_eq!(polls(1 << 20), vec![(2, Rule::Operation)]);
}

#[test]
fn polls_due_arrive_before_a_later_line_at_their_time() {
    // The observation's second poll, at 10 µs, comes before line 3's call at 10.
    let records = [polling(1, 0, 0, 10, 10), call("read", 1, 0, 10, 0)];
    assert_eq!(check(all(1, 10, 2), &records), vec![(3, Rule::Rate)]);
    // Polls still due at the log's end are counted.
    assert_eq!(
        check(all(1, 10, 3), &[polling(1, 0, 0, 1, 5)]),
        vec![(2, Rule::Rate)]
    );
}

#[test]
fn a_deliberate_refusal_is_used_only_if_certain() {
    let limits = || all(1, 1, 1);
    // A burst of one: the third request at 0 is refused even with the lower
    // model's token of slack; the second is not certain.
    let records = [
        call("read", 1, 0, 0, 0),
        deviating(call("read", 1, 1, 0, 0), "rate"),
        deviating(call("read", 1, 2, 0, 0), "rate"),
    ];
    assert_eq!(check(limits(), &records), vec![(3, Rule::Deviation)]);
    // A deliberate refusal spends: the next request at the edge is refused.
    let records = [
        call("read", 1, 0, 0, 0),
        deviating(call("read", 1, 1, 0, 0), "rate"),
        deviating(call("read", 1, 2, 0, 0), "rate"),
        call("read", 1, 3, SECOND, 0),
    ];
    assert_eq!(
        check(limits(), &records),
        vec![(3, Rule::Deviation), (5, Rule::Rate)]
    );
    // A refusal the real bucket would admit, through jitter's slack, is unused.
    let h = limited(all(1, 1, 2), 2, 1_000, 0);
    let records = [
        call("read", 1, 0, 0, 0),
        deviating(call("read", 1, 1, 999_500, 0), "rate"),
    ];
    assert_eq!(
        verdict(&run_with(h, &records, &NoHost, 1_000)),
        vec![(3, Rule::Deviation)]
    );
    // The last line's deliberate refusal, certain only at a later poll, is used.
    let records = [deviating(polling(1, 0, 0, 1, 3), "rate")];
    assert_eq!(check(all(1, 100, 1), &records), vec![]);
}

#[test]
fn a_line_out_of_order_arrives_when_the_cursor_is() {
    // Line 4 is planned before line 3, which breaks the order and no
    // deviation excuses (ADR-0047 decision 13); still, it arrives at 2 s,
    // where the bucket has a token, not at 1 s, where it would not.
    let records = [
        call("read", 1, 0, 0, 0),
        call("read", 1, 1, 2 * SECOND, 0),
        call("read", 1, 2, SECOND, 0),
    ];
    assert_eq!(check(all(1, 1, 2), &records), vec![(4, Rule::Order)]);
    let records = [
        call("read", 1, 0, 0, 0),
        call("read", 1, 1, 2 * SECOND, 0),
        polling(1, 0, SECOND, 10, 10),
    ];
    assert_eq!(
        check(all(1, 1, 2), &records),
        vec![(4, Rule::Order), (4, Rule::Rate)]
    );
}

#[test]
fn jitter_bounds_a_short_window_by_the_burst() {
    let h = || limited(all(1, 1, 2), 2, 1_000, 0);
    let two = [call("read", 1, 0, 0, 0), call("read", 1, 1, 999, 0)];
    assert_eq!(verdict(&run_with(h(), &two, &NoHost, 1_000)), vec![]);
    let three = [
        call("read", 1, 0, 0, 0),
        call("read", 1, 1, 999, 0),
        call("read", 1, 2, 999, 0),
    ];
    assert_eq!(
        verdict(&run_with(h(), &three, &NoHost, 1_000)),
        vec![(4, Rule::Rate)]
    );
}

#[test]
fn a_refused_limit_is_not_evaluated() {
    let records = [call("read", 1, 0, 0, 0), call("read", 1, 1, 0, 0)];
    assert_eq!(check(all(1, 0, 1), &records), vec![(1, Rule::Header)]);
    assert_eq!(
        check(
            vec![limit("p", By::At(Place::Path("tenant".into())), 1, 1, 1)],
            &records
        ),
        vec![(1, Rule::Header)]
    );
    let two_shapes = vec![limit("b", By::All, 1, 1, 1), limit("b", By::All, 1, 1, 2)];
    assert_eq!(check(two_shapes, &records[..1]), vec![(1, Rule::Header)]);
}

/// A place finds its own field among others: two calls whose `tenant` differ
/// while another field is the same are two values.
#[test]
fn a_place_reads_its_own_field() {
    use ascetic_ddd_command_log::record::{Codec, Field, Form, Payload, Span};
    let field = |name: &str, v: &str| Field {
        name: name.into(),
        value: Value::Literal(v.into()),
    };
    let by_query = || vec![limit("q", By::At(Place::Query("tenant".into())), 1, 10, 1)];
    let queried = |n: u32, tenant: &str| {
        with(call("read", 1, n, 0, 0), |rest| {
            rest.query = vec![field("other", "x"), field("tenant", tenant)];
        })
    };
    assert_eq!(
        check(by_query(), &[queried(0, "1"), queried(1, "2")]),
        vec![]
    );
    assert_eq!(
        check(by_query(), &[queried(0, "1"), queried(1, "1")]),
        vec![(3, Rule::Rate)]
    );
    let by_body = || vec![limit("b", By::At(Place::Body("/tenant".into())), 1, 10, 1)];
    let span = |field: &str, g: u64| Span {
        offset: 0,
        len: 0,
        field: field.into(),
        hole: Hole::Rendered {
            of: ascetic_ddd_command_log::record::Of::Subject(subject("tenant", g)),
            role: "name".into(),
            rendering: ascetic_ddd_command_log::record::Rendering::UuidText,
        },
        form: Form::JsonToken,
    };
    let posted = |n: u32, g: u64| {
        with(call("read", 1, n, 0, 0), |rest| {
            rest.body = Some(Payload {
                codec: Codec::Json,
                bytes: vec![],
                holes: vec![span("/other", 9), span("/tenant", g)],
            });
        })
    };
    // Zero-length spans at offset 0: neither overlapping nor past the payload.
    assert_eq!(check(by_body(), &[posted(0, 1), posted(1, 2)]), vec![]);
    assert_eq!(
        check(by_body(), &[posted(0, 1), posted(1, 1)]),
        vec![(3, Rule::Rate)]
    );
}

/// Every parameter must be positive, each refused on its own.
#[test]
fn a_zero_parameter_is_refused_each() {
    let records = [call("read", 1, 0, 0, 0)];
    for (requests, per_seconds, burst) in [(0, 1, 1), (1, 0, 1), (1, 1, 0)] {
        assert_eq!(
            check(all(requests, per_seconds, burst), &records),
            vec![(1, Rule::Header)]
        );
    }
}

/// An observation past the cap arrives once; one whose timeout is shorter than
/// its interval polls once.
#[test]
fn an_observation_polls_no_more_than_it_may() {
    assert_eq!(
        check(all(1, 10, 2), &[polling(1, 0, 0, 1, 1 << 20)]),
        vec![(2, Rule::Operation)]
    );
    assert_eq!(check(all(1, 10, 1), &[polling(1, 0, 0, 10, 5)]), vec![]);
}
