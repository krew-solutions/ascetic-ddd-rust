//! V2's corpus: small random logs and their one-edit mutants, each with the
//! validator's verdict. The draws come from faker's keys, so it is the same on
//! every run.

use std::collections::BTreeMap;

use super::*;
use ascetic_ddd_command_log::record::{
    Action, Caller, Codec, Dep, Form, Header, Hole, Measure, Of, On, Record, Rendering, StepId,
    Subject, Value,
};
use ascetic_ddd_command_log::validate::NoHost;
use ascetic_ddd_faker::{Key, Seed};

const LOGS: u64 = 120;

fn draw(k: &Key, label: &str, m: u64) -> u64 {
    k.child(label).unit().raw() % m.max(1)
}

/// A log valid by construction: groups written and read through their ETag,
/// Devices managed and telling facts on a gapless stream, a barrier over the
/// managed, retained creations, and edges to earlier steps within the window.
fn base(k: &Key) -> Vec<Record> {
    let mut records: Vec<Record> = Vec::new();
    let mut counts: BTreeMap<(Subject, String), u32> = BTreeMap::new();
    // A group's latest writer, its readers since, and whether it captured the
    // ETag they send.
    let mut writers: BTreeMap<u64, (StepId, Vec<StepId>, bool)> = BTreeMap::new();
    let mut streams: BTreeMap<u64, (u32, StepId)> = BTreeMap::new();
    let mut managed = 0u64;
    let mut at = 0u64;
    let n = 3 + draw(k, "steps", 9);
    for j in 0..n {
        let kj = k.at(j);
        at += [0, 1, 5, 300, 700][draw(&kj, "gap", 5) as usize];
        let what = draw(&kj, "what", 8);
        let (kind, index, label) = match what {
            0 => ("device", draw(&kj, "d", 3), "seen"),
            1 => ("group", draw(&kj, "g", 2), "update"),
            2 => ("group", draw(&kj, "g", 2), "read"),
            3 => ("device", draw(&kj, "d", 3), "fact"),
            4 => ("group", draw(&kj, "g", 2), "created"),
            5 => ("device", draw(&kj, "d", 3), "managed"),
            6 => ("group", draw(&kj, "g", 2), "watched"),
            _ => ("tenant", 1, "report"),
        };
        let s = subject(kind, index);
        let n = counts.entry((s.clone(), label.to_owned())).or_insert(0);
        let step_id = StepId {
            subject: s,
            label: label.into(),
            n: *n,
        };
        *n += 1;
        if let Some(previous) = records.last()
            && (at, &step_id) <= (previous.at, &previous.step)
        {
            at += 1;
        }
        let mut r = step(step_id.clone(), at);
        match label {
            "update" => {
                r.action = rest("update", vec![Value::Literal(index.to_string())]);
                if let Some((w, readers, _)) = writers.get(&index) {
                    r = after(r, w, On::Settled);
                    for reader in readers {
                        r = after(r, reader, On::Settled);
                    }
                }
                let captured = draw(&kj, "capture", 2) == 0;
                if captured {
                    r = captures_etag(r, index);
                }
                writers.insert(index, (step_id.clone(), vec![], captured));
            }
            "read" => match writers.get_mut(&index) {
                // An ETag only from a writer that captured it.
                Some((w, readers, true)) => {
                    r = after(r, w, On::Expected);
                    r.action = read_etag("read", index, w);
                    readers.push(step_id.clone());
                }
                _ => r.action = rest("read", vec![Value::Literal(index.to_string())]),
            },
            "fact" => {
                let (position, previous) = match streams.get(&index) {
                    Some((p, s)) => (p + 1, Some(s.clone())),
                    None => (1, None),
                };
                r = message(r, "lifecycle", position);
                if let Some(p) = previous {
                    r = after(r, &p, On::Expected);
                }
                streams.insert(index, (position, step_id.clone()));
            }
            "managed" => managed += 1,
            "watched" => {
                r = polling(index, step_id.n, at, 300, 1_500);
                r.step = step_id.clone();
                // Caused by the group's latest writer, when it has one within
                // reach; else a watch of its own.
                let reach = |w: &StepId| {
                    records
                        .iter()
                        .rev()
                        .find(|e| &e.step == w)
                        .is_some_and(|e| e.at < at && at - e.at <= W)
                };
                let cause = match writers.get(&index) {
                    Some((w, _, _)) if reach(w) => {
                        r = after(r, w, On::Sent);
                        w.clone()
                    }
                    _ => step_id.clone(),
                };
                if let Action::Observe(o) = &mut r.action {
                    o.cause = cause;
                }
            }
            // A report awaits the managed settled, or every one expected: a
            // member failing while an earlier one runs fails the latter
            // (ADR-0047 K1e-3).
            "report" => r.after.push(Dep::Each {
                kind: "device".into(),
                scope: vec![7],
                label: "managed".into(),
                n: 0,
                on: if draw(&kj, "barrier", 2) == 0 {
                    On::Settled
                } else {
                    On::Expected
                },
                count: Some(managed.min(3)),
            }),
            _ => {}
        }
        // An edge to an earlier step, kept where the window allows it.
        if !records.is_empty() && draw(&kj, "edge", 3) == 0 {
            let e = &records[draw(&kj, "to", records.len() as u64) as usize];
            let reach = at - e.at <= W || e.step.label == "created";
            if e.at < at
                && reach
                && !r
                    .after
                    .iter()
                    .any(|d| matches!(d, Dep::Step { step, .. } if step == &e.step))
            {
                r = after(r, &e.step.clone(), On::Sent);
            }
        }
        // Every call from a source and a caller whose credential names a Tenant.
        let (source, tenant) = (draw(&kj, "source", 2) as u32, draw(&kj, "tenant", 2));
        if let Some(rest) = call_of(&mut r) {
            rest.source = source;
            rest.caller = Some(Caller {
                principal: subject("principal", tenant),
                names: vec![subject("tenant", tenant)],
            });
        }
        // A think the plan's times allow, at most what separates the line from
        // its latest step dependency (ADR-0047 decision 3).
        let latest = r
            .after
            .iter()
            .filter_map(|d| match d {
                Dep::Step { step, .. } => {
                    records.iter().rev().find(|e| &e.step == step).map(|e| e.at)
                }
                Dep::Each { .. } => None,
            })
            .max();
        let barrier = r.after.iter().any(|d| matches!(d, Dep::Each { .. }));
        if let Some(latest) = latest
            && !barrier
            && at > latest
            && draw(&kj, "think", 3) == 0
        {
            r.think = draw(&kj, "thought", at - latest + 1);
        }
        records.push(r);
    }
    // The barrier counts the managed of index 0 to 2 per occurrence 0: count them.
    let mut seen = BTreeMap::new();
    for r in &mut records {
        for d in &mut r.after {
            if let Dep::Each { count, .. } = d {
                *count = Some(*seen.get("managed").unwrap_or(&0));
            }
        }
        if r.step.label == "managed" && r.step.n == 0 {
            *seen.entry("managed").or_insert(0) += 1;
        }
    }
    records
}

/// A step's REST call: its action's, or its observation's probe.
fn call_of(r: &mut Record) -> Option<&mut ascetic_ddd_command_log::record::Rest> {
    match &mut r.action {
        Action::Rest(rest) => Some(rest),
        Action::Observe(o) => match &mut o.probe {
            ascetic_ddd_command_log::record::Probe::Rest(rest) => Some(rest),
            _ => None,
        },
        _ => None,
    }
}

/// The corpus's header: limits of every kind on the group operations, at rates
/// whose intervals are comparable to the plan's gaps, with jitter whose
/// `R · J / P` is no integer and a reserve.
pub fn corpus_header() -> Header {
    use ascetic_ddd_command_log::record::{By, Place};
    limited(
        vec![
            limit("address", By::Source, 4_000, 1, 6),
            limit("all", By::All, 6_000, 1, 10),
            limit("group", By::At(Place::Path("group".into())), 3_000, 1, 6),
            limit(
                "tenant",
                By::At(Place::Credential("tenant".into())),
                2_000,
                1,
                6,
            ),
        ],
        2,
        250,
        100,
    )
}

/// One edit, if the log has what it edits.
fn mutant(records: &[Record], which: &str, k: &Key) -> Option<Vec<Record>> {
    let mut m = records.to_vec();
    let pick = |label: &str, candidates: &[usize]| -> Option<usize> {
        (!candidates.is_empty())
            .then(|| candidates[draw(k, label, candidates.len() as u64) as usize])
    };
    let all: Vec<usize> = (0..m.len()).collect();
    match which {
        "drop-edge" => {
            let i = pick(
                "i",
                &all.iter()
                    .copied()
                    .filter(|&i| !m[i].after.is_empty())
                    .collect::<Vec<_>>(),
            )?;
            let d = draw(k, "d", m[i].after.len() as u64) as usize;
            m[i].after.remove(d);
        }
        "move-at" => {
            let i = pick("i", &all[1..])?;
            m[i].at = m[draw(k, "to", i as u64) as usize].at;
        }
        // A line planned ahead, past twice the window: every line after it is
        // behind the cursor, where the read cursor and the line's own time part
        // (decision 16).
        "jump" => {
            let i = pick("i", &all[..all.len().saturating_sub(1)])?;
            m[i].at += 2 * W + 1 + draw(k, "by", W);
        }
        "duplicate" => {
            let i = pick("i", &all[1..])?;
            m[i].step = m[draw(k, "of", i as u64) as usize].step.clone();
        }
        "renumber" => {
            let i = pick(
                "i",
                &all.iter()
                    .copied()
                    .filter(|&i| matches!(&m[i].action, Action::Bus(_)))
                    .collect::<Vec<_>>(),
            )?;
            if let Action::Bus(b) = &mut m[i].action
                && let Some(s) = &mut b.stream
            {
                s.position =
                    [0, s.position + 1, s.position.saturating_sub(1)][draw(k, "p", 3) as usize];
            }
        }
        "barrier" => {
            let i = pick(
                "i",
                &all.iter()
                    .copied()
                    .filter(|&i| m[i].step.label == "report")
                    .collect::<Vec<_>>(),
            )?;
            for d in &mut m[i].after {
                if let Dep::Each { count: Some(c), .. } = d {
                    *c = if *c == 0 || draw(k, "sign", 2) == 0 {
                        *c + 1
                    } else {
                        *c - 1
                    };
                }
            }
        }
        "swap-producer" => {
            let readers: Vec<usize> = all
                .iter()
                .copied()
                .filter(|&i| m[i].step.label == "read" && !m[i].after.is_empty())
                .collect();
            let i = pick("i", &readers)?;
            let other = m[draw(k, "to", i as u64) as usize].step.clone();
            if let Action::Rest(rest) = &mut m[i].action {
                for h in &mut rest.headers {
                    if let Value::Hole(Hole::Captured { from, .. }) = &mut h.value {
                        *from = other.clone();
                    }
                }
            }
        }
        "unknown-op" => {
            let i = pick(
                "i",
                &all.iter()
                    .copied()
                    .filter(|&i| matches!(&m[i].action, Action::Rest(_)))
                    .collect::<Vec<_>>(),
            )?;
            if let Action::Rest(rest) = &mut m[i].action {
                rest.op = "delete".into();
            }
        }
        "span" => {
            let i = pick(
                "i",
                &all.iter()
                    .copied()
                    .filter(|&i| matches!(&m[i].action, Action::Host(_)))
                    .collect::<Vec<_>>(),
            )?;
            let codec = [Codec::Text, Codec::Json][draw(k, "codec", 2) as usize];
            let spans = vec![
                (0, 2, Form::Raw),
                (1 + draw(k, "o", 4) as u32, 2, Form::Raw),
            ];
            m[i] = with_payload(m[i].clone(), codec, 4, spans);
        }
        "profile" => {
            let calls: Vec<usize> = all
                .iter()
                .copied()
                .filter(|&i| matches!(&m[i].action, Action::Rest(r) if !r.path.is_empty()))
                .collect();
            let i = pick("i", &calls)?;
            let kind = ["group", "device", "tenant"][draw(k, "kind", 3) as usize];
            if let Action::Rest(rest) = &mut m[i].action {
                rest.path[0] = Value::Hole(Hole::Rendered {
                    of: Of::Subject(subject(kind, 1)),
                    role: "id".into(),
                    rendering: Rendering::UuidText,
                });
            }
        }
        "deviation" => {
            let i = pick("i", &all)?;
            // Every rule's name, a rule added later included, and one naming none.
            let names: Vec<&str> = ascetic_ddd_command_log::validate::Rule::ALL
                .iter()
                .map(|r| r.name())
                .chain(["nothing"])
                .collect();
            let rule = names[draw(k, "rule", names.len() as u64) as usize];
            m[i] = deviating(m[i].clone(), rule);
        }
        // A line planned before the one above it, the order deviated: still
        // broken, the deviation unused (ADR-0047 decision 13).
        "order-deviated" => {
            let i = pick(
                "i",
                &all[1..]
                    .iter()
                    .copied()
                    .filter(|&i| m[i - 1].at > 0)
                    .collect::<Vec<_>>(),
            )?;
            m[i].at = m[i - 1].at - 1;
            m[i] = deviating(m[i].clone(), "order");
        }
        // A writer that no longer captures the ETag its readers send.
        "uncaptured-writer" => {
            let read: Vec<usize> = all
                .iter()
                .copied()
                .filter(|&i| {
                    !m[i].capture.is_empty()
                        && m.iter().any(|r| {
                            r.after
                                .iter()
                                .any(|d| matches!(d, Dep::Step { step, on: On::Expected } if step == &m[i].step))
                                && r.step.label == "read"
                        })
                })
                .collect();
            let i = pick("i", &read)?;
            m[i].capture.clear();
        }
        // A think past what the plan's times allow (ADR-0047 decision 3).
        "think" => {
            let i = pick(
                "i",
                &all.iter()
                    .copied()
                    .filter(|&i| m[i].after.iter().any(|d| matches!(d, Dep::Step { .. })))
                    .collect::<Vec<_>>(),
            )?;
            m[i].think = W * 2;
        }
        // A writer that awaits a reader only sent.
        "weaken-reader" => {
            let candidates: Vec<(usize, usize)> = m
                .iter()
                .enumerate()
                .filter(|(_, r)| r.step.label == "update")
                .flat_map(|(i, r)| {
                    r.after
                        .iter()
                        .enumerate()
                        .filter(|(_, d)| {
                            matches!(d, Dep::Step { step, on: On::Settled } if step.label == "read")
                        })
                        .map(move |(j, _)| (i, j))
                })
                .collect();
            let (i, j) = *candidates.get(draw(k, "i", candidates.len() as u64) as usize)?;
            if let Dep::Step { on, .. } = &mut m[i].after[j] {
                *on = On::Sent;
            }
        }
        // A measure from a step no earlier line has: its own.
        "measure-dangling" => {
            let i = pick("i", &all)?;
            let from = m[i].step.clone();
            m[i].measure.push(Measure {
                metric: "m".into(),
                from,
            });
        }
        // An observation whose cause it no longer awaits.
        "cause-dropped" => {
            let caused: Vec<usize> = all
                .iter()
                .copied()
                .filter(|&i| matches!(&m[i].action, Action::Observe(o) if o.cause != m[i].step))
                .collect();
            let i = pick("i", &caused)?;
            let cause = match &m[i].action {
                Action::Observe(o) => o.cause.clone(),
                _ => return None,
            };
            m[i].after
                .retain(|d| !matches!(d, Dep::Step { step, .. } if step == &cause));
        }
        // A measure on a step that awaits a dependency only sent.
        "sent-measure" => {
            let sent: Vec<usize> = all
                .iter()
                .copied()
                .filter(|&i| {
                    !matches!(m[i].action, Action::Observe(_))
                        && m[i]
                            .after
                            .iter()
                            .any(|d| matches!(d, Dep::Step { on: On::Sent, .. }))
                })
                .collect();
            let i = pick("i", &sent)?;
            let from = m[i].after.iter().find_map(|d| match d {
                Dep::Step { step, on: On::Sent } => Some(step.clone()),
                _ => None,
            })?;
            m[i].measure.push(Measure {
                metric: "m".into(),
                from,
            });
        }
        "squeeze" => {
            let calls: Vec<usize> = all[1..]
                .iter()
                .copied()
                .filter(|&i| call_of(&mut m[i].clone()).is_some())
                .collect();
            let i = pick("i", &calls)?;
            m[i].at = m[i - 1].at;
        }
        "source-past" => {
            let calls: Vec<usize> = all
                .iter()
                .copied()
                .filter(|&i| call_of(&mut m[i].clone()).is_some())
                .collect();
            let i = pick("i", &calls)?;
            if let Some(rest) = call_of(&mut m[i]) {
                rest.source = 2;
            }
        }
        "every-halved" => {
            let watched: Vec<usize> = all
                .iter()
                .copied()
                .filter(|&i| matches!(&m[i].action, Action::Observe(_)))
                .collect();
            let i = pick("i", &watched)?;
            if let Action::Observe(o) = &mut m[i].action {
                o.every /= 2;
            }
        }
        "key-time" => {
            let calls: Vec<usize> = all
                .iter()
                .copied()
                .filter(|&i| call_of(&mut m[i].clone()).is_some_and(|r| !r.path.is_empty()))
                .collect();
            let i = pick("i", &calls)?;
            if let Some(rest) = call_of(&mut m[i]) {
                rest.path[0] = Value::Hole(Hole::Time {
                    base: ascetic_ddd_command_log::record::TimeBase::Sent,
                    offset: 0,
                    rendering: ascetic_ddd_command_log::record::TimeRendering::Rfc3339Micros,
                });
            }
        }
        "names-dropped" => {
            let calls: Vec<usize> = all
                .iter()
                .copied()
                .filter(|&i| call_of(&mut m[i].clone()).is_some())
                .collect();
            let i = pick("i", &calls)?;
            if let Some(rest) = call_of(&mut m[i])
                && let Some(c) = &mut rest.caller
            {
                c.names.clear();
            }
        }
        _ => return None,
    }
    (m != records).then_some(m)
}

/// The corpus's logs, by name.
pub fn logs() -> Vec<(String, Vec<Record>)> {
    let root = Key::root(Seed(46)).child("corpus");
    let mut out = Vec::new();
    let kinds = [
        "drop-edge",
        "move-at",
        "jump",
        "duplicate",
        "renumber",
        "barrier",
        "swap-producer",
        "unknown-op",
        "span",
        "profile",
        "deviation",
        "squeeze",
        "order-deviated",
        "uncaptured-writer",
        "think",
        "weaken-reader",
        "measure-dangling",
        "cause-dropped",
        "sent-measure",
        "source-past",
        "every-halved",
        "key-time",
        "names-dropped",
    ];
    for i in 0..LOGS {
        let k = root.at(i);
        let records = base(&k);
        let mut entries = vec![(format!("log-{i}"), records.clone())];
        for which in kinds {
            if let Some(m) = mutant(&records, which, &k.child(which)) {
                entries.push((format!("log-{i}-{which}"), m));
            }
        }
        out.extend(entries);
    }
    // Reports awaiting three or four managed devices' reads expected, 300 µs apart:
    // under the scheduler's scripts a later member fails while an earlier one
    // still runs, which fails the report all the same (ADR-0047 K1e-3).
    for members in [3u64, 3, 3, 4, 4, 4] {
        let i = out.len() as u64;
        // Reads, which are never resent: a refusal ends them; each from a
        // caller of a Tenant, as every corpus call is.
        let mut records: Vec<Record> = (0..members)
            .map(|d| {
                let mut r = Record {
                    action: rest("read", vec![Value::Literal(d.to_string())]),
                    ..step(id("device", i * 10 + d, "managed", 0), d * 300)
                };
                if let Some(rest) = call_of(&mut r) {
                    rest.caller = Some(Caller {
                        principal: subject("principal", 0),
                        names: vec![subject("tenant", 0)],
                    });
                }
                r
            })
            .collect();
        records.push(Record {
            after: vec![Dep::Each {
                kind: "device".into(),
                scope: vec![7],
                label: "managed".into(),
                n: 0,
                on: On::Expected,
                count: Some(members),
            }],
            ..step(id("group", i, "report", 0), members * 300 + 100)
        });
        out.push((format!("barrier-{i}"), records));
    }
    out
}

/// The corpus as JSON lines: a name, a log, the validator's verdict and peaks.
/// The corpus's header with every operation, channel and host call timing out
/// on the window's scale, not in seconds: the plan's bound then turns on its
/// terms within a log's few milliseconds (ADR-0047 K1e-2).
pub fn short_header() -> Header {
    let mut h = corpus_header();
    for o in &mut h.operations {
        o.timeout = 600;
    }
    for c in &mut h.channels {
        c.timeout = 600;
    }
    for c in &mut h.calls {
        c.timeout = 600;
    }
    h
}

pub fn lines() -> Vec<String> {
    let mut out = Vec::new();
    for (name, records) in logs() {
        // The base logs also under the short header.
        let base = name.matches('-').count() == 1;
        let mut headers = vec![(name.clone(), corpus_header())];
        if base {
            headers.push((format!("{name}/short"), short_header()));
        }
        for (name, header) in headers {
            let (report, bounds) = run_bounded(header.clone(), &records, &NoHost, 3);
            let mut verdict: Vec<(u64, &str)> = report
                .violations
                .iter()
                .map(|v| (v.line, v.rule.name()))
                .collect();
            verdict.dedup();
            verdict.sort();
            verdict.dedup();
            let log = String::from_utf8(bytes(header, &records)).unwrap();
            let p = report.peaks;
            let peaks = [
                p.completions,
                p.captures,
                p.polls,
                p.held,
                p.pending,
                p.horizon,
                p.barriers,
            ];
            let line = serde_json::json!({
                "name": name, "log": log, "verdict": verdict, "peaks": peaks, "bounds": bounds,
            });
            out.push(line.to_string());
        }
    }
    out
}
