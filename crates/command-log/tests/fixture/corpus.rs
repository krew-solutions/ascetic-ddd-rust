//! V2's corpus: small random logs and their one-edit mutants, each with the
//! validator's verdict. The draws come from faker's keys, so it is the same on
//! every run.

use std::collections::BTreeMap;

use super::*;
use ascetic_ddd_command_log::record::{
    Action, Codec, Dep, Form, Hole, Of, On, Record, Rendering, StepId, Subject, Value,
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
    let mut writers: BTreeMap<u64, (StepId, Vec<StepId>)> = BTreeMap::new();
    let mut streams: BTreeMap<u64, (u32, StepId)> = BTreeMap::new();
    let mut managed = 0u64;
    let mut at = 0u64;
    let n = 3 + draw(k, "steps", 9);
    for j in 0..n {
        let kj = k.at(j);
        at += [0, 1, 5, 300, 700][draw(&kj, "gap", 5) as usize];
        let what = draw(&kj, "what", 7);
        let (kind, index, label) = match what {
            0 => ("device", draw(&kj, "d", 3), "seen"),
            1 => ("group", draw(&kj, "g", 2), "update"),
            2 => ("group", draw(&kj, "g", 2), "read"),
            3 => ("device", draw(&kj, "d", 3), "fact"),
            4 => ("group", draw(&kj, "g", 2), "created"),
            5 => ("device", draw(&kj, "d", 3), "managed"),
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
                if let Some((w, readers)) = writers.get(&index) {
                    r = after(r, w, On::Settled);
                    for reader in readers {
                        r = after(r, reader, On::Sent);
                    }
                }
                if draw(&kj, "capture", 2) == 0 {
                    r = captures_etag(r, index);
                }
                writers.insert(index, (step_id.clone(), vec![]));
            }
            "read" => match writers.get_mut(&index) {
                Some((w, readers)) => {
                    r = after(r, w, On::Expected);
                    r.action = read_etag("read", index, w);
                    readers.push(step_id.clone());
                }
                None => r.action = rest("read", vec![Value::Literal(index.to_string())]),
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
            "report" => r.after.push(Dep::Each {
                kind: "device".into(),
                scope: vec![7],
                label: "managed".into(),
                n: 0,
                on: On::Settled,
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
            let rule = ["gap", "edge", "order", "stale", "duplicate", "nothing"]
                [draw(k, "rule", 6) as usize];
            m[i] = deviating(m[i].clone(), rule);
        }
        _ => return None,
    }
    (m != records).then_some(m)
}

/// The corpus as JSON lines: a name, a log, the validator's verdict.
pub fn lines() -> Vec<String> {
    let root = Key::root(Seed(46)).child("corpus");
    let mut out = Vec::new();
    let kinds = [
        "drop-edge",
        "move-at",
        "duplicate",
        "renumber",
        "barrier",
        "swap-producer",
        "unknown-op",
        "span",
        "profile",
        "deviation",
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
        for (name, records) in entries {
            let report = run_with(header(), &records, &NoHost, 3);
            let mut verdict: Vec<(u64, &str)> = report
                .violations
                .iter()
                .map(|v| (v.line, v.rule.name()))
                .collect();
            verdict.dedup();
            verdict.sort();
            verdict.dedup();
            let log = String::from_utf8(bytes(header(), &records)).unwrap();
            let line = serde_json::json!({ "name": name, "log": log, "verdict": verdict });
            out.push(line.to_string());
        }
    }
    out
}
