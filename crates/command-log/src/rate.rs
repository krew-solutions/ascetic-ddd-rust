//! Offered load (ADR-0046 decision 14): a limit's key value for a call, and the
//! curve a bucket of planned arrivals must stay under.
//!
//! The validator calls this module, and so will the scheduler: one definition of
//! what a call counts against, and of what a bucket admits.

use std::collections::VecDeque;

use crate::record::{By, Caller, Hole, Limit, Of, Operation, Place, Rest, Value};

/// The value a call counts by under `limit`, as bytes; or why it cannot be
/// evaluated.
pub fn value(limit: &Limit, op: &Operation, rest: &Rest, sources: u32) -> Result<Vec<u8>, String> {
    match &limit.by {
        By::All => Ok(Vec::new()),
        By::Source => {
            if rest.source < sources {
                Ok(format!("source {}", rest.source).into_bytes())
            } else {
                Err(format!(
                    "source {} past the profile's {sources}",
                    rest.source
                ))
            }
        }
        By::At(place) => at(place, op, rest),
    }
}

fn at(place: &Place, op: &Operation, rest: &Rest) -> Result<Vec<u8>, String> {
    match place {
        Place::Path(name) => {
            let index = parameters(&op.path)
                .iter()
                .position(|p| p == name)
                .ok_or_else(|| format!("{{{name}}} is no parameter of {}", op.path))?;
            rest.path
                .get(index)
                .ok_or_else(|| format!("no path value for {{{name}}}"))
                .and_then(reduce)
        }
        Place::Query(name) => rest
            .query
            .iter()
            .find(|f| &f.name == name)
            .ok_or_else(|| format!("no query parameter {name}"))
            .and_then(|f| reduce(&f.value)),
        Place::Header(name) => rest
            .headers
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| format!("no header {name}"))
            .and_then(|f| reduce(&f.value)),
        Place::Body(field) => rest
            .body
            .iter()
            .flat_map(|b| &b.holes)
            .find(|s| &s.field == field)
            .ok_or_else(|| format!("no span for the body's {field}"))
            .and_then(|s| hole(&s.hole)),
        Place::Credential(kind) => credential(rest.caller.as_ref(), kind),
    }
}

fn credential(caller: Option<&Caller>, kind: &str) -> Result<Vec<u8>, String> {
    let caller = caller.ok_or_else(|| "no caller".to_owned())?;
    let mut named = caller.names.iter().filter(|s| s.kind == kind);
    match (named.next(), named.next()) {
        (Some(s), None) => Ok(subject_bytes(s)),
        (None, _) => Err(format!("the caller's credential names no {kind}")),
        (Some(_), Some(_)) => Err(format!("the caller's credential names two of {kind}")),
    }
}

/// The parameters of a path template, `{name}` each, in order.
pub fn parameters(template: &str) -> Vec<String> {
    template
        .split('{')
        .skip(1)
        .filter_map(|rest| rest.split_once('}').map(|(name, _)| name.to_owned()))
        .collect()
}

fn subject_bytes(s: &crate::record::Subject) -> Vec<u8> {
    let mut out = b"subject ".to_vec();
    out.extend(serde_json::to_vec(s).unwrap_or_default());
    out
}

/// A value reduced to what it names: a rendered or captured value of a subject
/// is that subject, of a step that step, a literal its text.
fn reduce(v: &Value) -> Result<Vec<u8>, String> {
    match v {
        Value::Literal(text) => Ok(format!("literal {text}").into_bytes()),
        Value::Hole(h) => hole(h),
    }
}

fn hole(h: &Hole) -> Result<Vec<u8>, String> {
    match h {
        Hole::Rendered {
            of: Of::Subject(s), ..
        }
        | Hole::Captured { subject: s, .. } => Ok(subject_bytes(s)),
        Hole::Rendered {
            of: Of::Step(st), ..
        } => {
            let mut out = b"step ".to_vec();
            out.extend(serde_json::to_vec(st).unwrap_or_default());
            Ok(out)
        }
        Hole::Time { .. } => Err("a time is no key".to_owned()),
    }
}

/// Whether a limit can be evaluated at all: positive parameters.
pub fn valid(limit: &Limit) -> bool {
    limit.requests > 0 && limit.per_seconds > 0 && limit.burst > 0
}

/// The arrival curve of a token bucket full at t = 0, for arrivals each
/// delayed by up to `jitter`, at the rate less the reserve: a window of planned
/// width at most `J` holds at most `B` arrivals, a wider one of width `w` at
/// most `B + R′ (w − J) / P`. Exact integers.
#[derive(Clone, Debug)]
pub struct Curve {
    rate: i128,
    period: i128,
    burst: i128,
    jitter: u64,
    /// Spent arrivals planned within `J` of the latest: `(t, h)`.
    recent: VecDeque<(u64, i128)>,
    /// The least `h` of the spent arrivals older than that.
    low: Option<i128>,
    /// Arrivals spent.
    spent: i128,
}

impl Curve {
    /// The curve of `limit`; `None` for a limit with a zero parameter.
    pub fn new(limit: &Limit, jitter: u64, reserve: u32) -> Option<Curve> {
        if !valid(limit) || reserve >= 1000 {
            return None;
        }
        Some(Curve {
            rate: i128::from(limit.requests)
                .saturating_mul(1000_i128.saturating_sub(i128::from(reserve))),
            period: i128::from(limit.per_seconds).saturating_mul(1_000_000_000),
            burst: i128::from(limit.burst),
            jitter,
            recent: VecDeque::new(),
            low: None,
            spent: 0,
        })
    }

    /// Whether an arrival at `t` stays under the curve; it is spent if so, or
    /// if `forced`. Arrivals come in time order.
    pub fn arrive(&mut self, t: u64, forced: bool) -> bool {
        // Spent arrivals planned `J` or more before `t` start long windows now.
        while let Some(&(at, h)) = self.recent.front() {
            if t.saturating_sub(at) < self.jitter {
                break;
            }
            self.low = Some(self.low.map_or(h, |l| l.min(h)));
            self.recent.pop_front();
        }
        // With `h(m) = m·P′ − R′·t` for the `m`th spent arrival, the window from
        // spent `i` to this one holds `m − mᵢ + 1` arrivals, and stays under
        // `B + R′ (w − J) / P` iff `h − h(i) ≤ (B − 1)·P′ − R′·J`.
        let h = self
            .spent
            .saturating_mul(self.period)
            .saturating_sub(self.rate.saturating_mul(i128::from(t)));
        let bound = self
            .burst
            .saturating_sub(1)
            .saturating_mul(self.period)
            .saturating_sub(self.rate.saturating_mul(i128::from(self.jitter)));
        let short = (self.recent.len() as i128).saturating_add(1) <= self.burst;
        let long = self.low.is_none_or(|l| h.saturating_sub(l) <= bound);
        let admitted = short && long;
        if admitted || forced {
            self.recent.push_back((t, h));
            self.spent = self.spent.saturating_add(1);
        }
        admitted
    }
}

/// The lower model of a deliberate refusal: a plain token bucket at the full
/// rate, with the burst less what jitter and the host's rounding could give
/// back — `B + ⌈R·J/P⌉ + 1` — refusals spending nothing. A planned 429 is
/// certain only if this refuses it.
#[derive(Clone, Debug)]
pub struct Lower {
    rate: i128,
    period: i128,
    tolerance: i128,
    tat: i128,
}

impl Lower {
    /// The lower model of `limit`; `None` for a limit with a zero parameter.
    pub fn new(limit: &Limit, jitter: u64) -> Option<Lower> {
        if !valid(limit) {
            return None;
        }
        let rate = i128::from(limit.requests);
        let period = i128::from(limit.per_seconds).saturating_mul(1_000_000);
        let back = rate
            .saturating_mul(i128::from(jitter))
            .saturating_add(period.saturating_sub(1))
            .checked_div(period)
            .unwrap_or(0);
        let burst = i128::from(limit.burst)
            .saturating_add(back)
            .saturating_add(1);
        Some(Lower {
            rate,
            period,
            tolerance: burst.saturating_sub(1).saturating_mul(period),
            tat: 0,
        })
    }

    /// Whether the bucket admits an arrival at `t`, spending it if so.
    pub fn arrive(&mut self, t: u64) -> bool {
        let now = self.rate.saturating_mul(i128::from(t));
        let admitted = now >= self.tat.saturating_sub(self.tolerance);
        if admitted {
            self.tat = self.tat.max(now).saturating_add(self.period);
        }
        admitted
    }
}

#[cfg(test)]
#[allow(
    clippy::arithmetic_side_effects,
    reason = "the definition the curve is checked against, written as plainly as it reads; debug builds check its overflow"
)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn limit(requests: u32, per_seconds: u32, burst: u32) -> Limit {
        Limit {
            key: "k".into(),
            bucket: "b".into(),
            by: By::All,
            requests,
            per_seconds,
            burst,
        }
    }

    /// The curve by its definition: every window between two spent arrivals,
    /// counted.
    fn by_definition(l: &Limit, jitter: u64, reserve: u32, arrivals: &[(u64, bool)]) -> Vec<bool> {
        let rate = i128::from(l.requests) * (1000 - i128::from(reserve));
        let period = i128::from(l.per_seconds) * 1_000_000_000;
        let burst = i128::from(l.burst);
        let mut spent: Vec<u64> = Vec::new();
        let mut out = Vec::new();
        for &(t, forced) in arrivals {
            let recent = spent.iter().filter(|&&s| t - s < jitter).count() as i128;
            let short = recent < burst;
            let long = spent.iter().enumerate().all(|(i, &s)| {
                if t - s < jitter {
                    return true;
                }
                let n = (spent.len() - i) as i128 + 1;
                n * period <= burst * period + rate * (i128::from(t - s) - i128::from(jitter))
            });
            let admitted = short && long;
            if admitted || forced {
                spent.push(t);
            }
            out.push(admitted);
        }
        out
    }

    proptest! {
        /// R3: the streaming curve is the definition.
        #[test]
        fn the_curve_is_its_definition(
            requests in 1u32..5,
            burst in 1u32..5,
            jitter in prop_oneof![Just(0u64), Just(1), Just(250_000), Just(600_000)],
            reserve in prop_oneof![Just(0u32), Just(250), Just(500)],
            mut times in proptest::collection::vec(0u64..3_000_000, 0..24),
            forced in proptest::collection::vec(proptest::bool::weighted(0.15), 24),
        ) {
            times.sort_unstable();
            let l = limit(requests, 1, burst);
            let arrivals: Vec<(u64, bool)> = times.iter().zip(&forced).map(|(t, f)| (*t, *f)).collect();
            let mut curve = Curve::new(&l, jitter, reserve).unwrap();
            let streamed: Vec<bool> = arrivals.iter().map(|&(t, f)| curve.arrive(t, f)).collect();
            prop_assert_eq!(streamed, by_definition(&l, jitter, reserve, &arrivals));
        }
    }

    /// With no jitter and no reserve the curve is the host's token bucket: two a
    /// second, a burst of three.
    #[test]
    fn with_no_jitter_the_curve_is_a_token_bucket() {
        let l = limit(2, 1, 3);
        let run = |times: &[u64]| {
            let mut c = Curve::new(&l, 0, 0).unwrap();
            times
                .iter()
                .map(|&t| c.arrive(t, false))
                .collect::<Vec<_>>()
        };
        assert_eq!(run(&[0, 0, 0, 0]), [true, true, true, false]);
        assert_eq!(run(&[0, 0, 0, 499_999]), [true, true, true, false]);
        assert_eq!(run(&[0, 0, 0, 500_000]), [true, true, true, true]);
        assert_eq!(
            run(&[0, 0, 0, 0, 3_600_000_000]),
            [true, true, true, false, true]
        );
        assert!(
            run(&[0, 0, 0, 600_000, 1_200_000, 1_800_000, 2_400_000])
                .iter()
                .all(|&a| a)
        );
        // An interval that is no whole microsecond: three a second.
        let l = limit(3, 1, 1);
        let mut c = Curve::new(&l, 0, 0).unwrap();
        assert!(c.arrive(0, false));
        assert!(!c.arrive(333_333, false));
        assert!(c.arrive(333_334, false));
        let l = limit(3, 1, 2);
        let mut c = Curve::new(&l, 0, 0).unwrap();
        assert!([0, 0, 333_334, 666_667].iter().all(|&t| c.arrive(t, false)));
    }

    /// Jitter: `B` arrivals within `J` pass and `B + 1` do not; a wide burst
    /// spread by a long `J` keeps its rate (`readEnrollmentNotice`: 200 a second,
    /// a burst of 400, `J = 3 s`, at one a second).
    #[test]
    fn jitter_bounds_short_windows_by_the_burst() {
        let l = limit(1, 1, 2);
        let mut c = Curve::new(&l, 1_000, 0).unwrap();
        assert!(c.arrive(0, false) && c.arrive(999, false));
        assert!(!c.arrive(999, false));
        let notice = limit(200, 1, 400);
        let mut c = Curve::new(&notice, 3_000_000, 0).unwrap();
        assert!((0..60u64).all(|s| c.arrive(s * 1_000_000, false)));
    }

    /// The reserve: 99 % of the rate passes without it and not with 5 %.
    #[test]
    fn the_reserve_leaves_room() {
        let l = limit(100, 1, 1);
        let gap = 1_000_000 / 99;
        for (reserve, all) in [(0, true), (50, false)] {
            let mut c = Curve::new(&l, 0, reserve).unwrap();
            let admitted = (0..1_000u64).filter(|&i| c.arrive(i * gap, false)).count();
            assert_eq!(admitted == 1_000, all, "reserve {reserve}: {admitted}");
        }
    }

    /// The lower model: a deliberate refusal the real bucket would admit is not
    /// certain.
    #[test]
    fn the_lower_model_refuses_only_what_is_certain() {
        // One a second, a burst of one, no jitter: the third arrival at 0 is refused
        // even with the token of slack.
        let mut lower = Lower::new(&limit(1, 1, 1), 0).unwrap();
        assert!(lower.arrive(0) && lower.arrive(0) && !lower.arrive(0));
        // A burst of two with 1,000 µs of jitter: an arrival 999,500 µs after the
        // first is admitted by the real bucket, so its refusal is not certain.
        let mut lower = Lower::new(&limit(1, 1, 2), 1_000).unwrap();
        assert!(lower.arrive(0) && lower.arrive(999_500));
    }
}
