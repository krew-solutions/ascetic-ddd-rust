//! The fold's recheck of admission's evidence (ADR-0047 decision 22,
//! K2b-7e-2b-3d-3b-3): what the journal alone makes a theorem of admission's
//! own output — the term set and its table, each term's arithmetic, the
//! library's constants where a cost is one verbatim, the counts terms share,
//! the sockets against the rows, the phases' sums, the slack and the budget,
//! the parts no enforcer holds, the scope and the limit — never a cost an
//! owner states or a plan's shape prices, which the journal does not hold.
//!
//! Versioned: a version's rules and its table are its own, frozen when the
//! next version lands, so that a later reader judges a journal by the rules
//! it was written under. Every refusal is a closed snake-case code, the same
//! the definition (`vectors/verdict.py`) gives.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::journal::{Admission, Basis, Header, Held, MemoryTerm, Part, Phase, StatedBy};

/// Whether a part is the run's or each source's.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    /// One term for the run.
    Global,
    /// One term a source.
    Source,
}

/// Who holds a part's count, as the table names it: a gate built or not.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HeldBy {
    /// What is built.
    Built,
    /// A bounded channel.
    Channel,
    /// A source's cap.
    Cap,
    /// It is held once.
    Once,
    /// A gate.
    Gate,
    /// The host.
    Host,
}

/// Who may state a part's cost.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stater {
    /// The library alone.
    Library,
    /// The host alone.
    Host,
    /// Its carrier, the library's or a host's.
    Either,
}

/// A part's row in a version's table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Row {
    /// The part.
    pub part: Part,
    /// The run's or each source's.
    pub scope: Scope,
    /// Its phase.
    pub phase: Phase,
    /// What it is counted against.
    pub basis: Basis,
    /// Who holds its count.
    pub held: HeldBy,
    /// Who may state its cost.
    pub stated: Stater,
}

const fn row(
    part: Part,
    scope: Scope,
    phase: Phase,
    basis: Basis,
    held: HeldBy,
    stated: Stater,
) -> Row {
    Row {
        part,
        scope,
        phase,
        basis,
        held,
        stated,
    }
}

/// Version 10's table, one row a part in the journal's order: the data file
/// `schema/parts-10.json` is the same, which `vectors/verdict.py` reads.
pub const PARTS_10: [Row; 26] = {
    use Basis::{Heap, Outside};
    use HeldBy::{Built, Cap, Channel, Gate, Host as ByHost, Once};
    use Phase::{Fixed, Gates, Pool, Run, Stages};
    use Scope::{Global, Source};
    use Stater::{Either, Host, Library};
    [
        row(Part::Reader, Global, Fixed, Heap, Channel, Library),
        row(Part::Decoder, Global, Fixed, Outside, Built, Library),
        row(Part::Runtime, Source, Fixed, Heap, Built, Library),
        row(Part::CarrierSource, Source, Run, Heap, Once, Either),
        row(Part::Keeping, Source, Run, Heap, Once, Library),
        row(Part::Journal, Global, Run, Heap, Channel, Library),
        row(Part::JournalOut, Global, Run, Heap, Once, Host),
        row(Part::Connections, Source, Run, Heap, Cap, Either),
        row(Part::Io, Source, Run, Heap, Cap, Host),
        row(Part::Answers, Source, Run, Heap, Cap, Either),
        row(Part::Judging, Global, Stages, Heap, Once, Library),
        row(Part::Probes, Source, Gates, Heap, Cap, Either),
        row(Part::ProbesIo, Source, Gates, Heap, Cap, Host),
        row(Part::GateStages, Global, Gates, Heap, ByHost, Host),
        row(Part::GateRun, Global, Run, Heap, ByHost, Host),
        row(Part::Cells, Global, Pool, Heap, Gate, Library),
        row(Part::Pending, Global, Pool, Heap, Gate, Library),
        row(Part::Barriers, Global, Pool, Heap, Once, Library),
        row(Part::Records, Global, Pool, Heap, Gate, Library),
        row(Part::BoundTo, Global, Pool, Heap, Gate, Library),
        row(Part::Kept, Global, Pool, Heap, Gate, Library),
        row(Part::Binding, Global, Pool, Heap, Once, Library),
        row(Part::ShellAttempts, Global, Pool, Heap, Gate, Library),
        row(Part::CarrierAttempts, Global, Pool, Heap, Gate, Either),
        row(Part::Decorations, Global, Pool, Heap, Gate, Host),
        row(Part::Evidence, Global, Fixed, Heap, Built, Library),
    ]
};

/// Every code a refusal of version 10's rules gives, in the order the rules
/// are checked.
pub const WHYS_10: [&str; 17] = [
    "admission_kappa",
    "admission_term_set",
    "admission_table",
    "admission_built",
    "admission_bytes",
    "admission_model",
    "admission_evidence",
    "admission_decoder",
    "admission_shared",
    "admission_rows",
    "admission_pool",
    "admission_need",
    "admission_slack",
    "admission_budget",
    "admission_unheld",
    "admission_scope",
    "admission_limit",
];

/// A header's admission rechecked by its version's rules; none passes.
///
/// # Errors
///
/// The first rule it breaks, by its code; `admission_version` for a version
/// with no rules of its own.
pub fn recheck(header: &Header) -> Result<(), &'static str> {
    header
        .admission
        .as_ref()
        .map_or(Ok(()), |a| admission(header.journal, header.catch_up, a))
}

/// An admission journaled in `version` with catch-up `catch_up`, rechecked
/// by that version's rules: what a writer of `version` checks its own
/// evidence by before it journals it.
///
/// # Errors
///
/// The first rule it breaks, by its code; `admission_version` for a version
/// with no rules of its own.
pub fn admission(version: u32, catch_up: u32, a: &Admission) -> Result<(), &'static str> {
    match version {
        10 => admission_10(catch_up, a),
        _ => Err("admission_version"),
    }
}

/// Version 10's rules over an admission journaled with catch-up `catch_up`,
/// in [`WHYS_10`]'s order.
///
/// # Errors
///
/// The first rule it breaks, by its code.
pub fn admission_10(catch_up: u32, a: &Admission) -> Result<(), &'static str> {
    let m = &a.memory;
    let kappa = u128::from(catch_up);
    if kappa >= 1000 {
        return Err("admission_kappa");
    }
    let terms = &m.terms;
    // The term set: each global part once, each source's part once a source,
    // the sources those of the runtimes.
    let sources = terms.iter().filter(|t| t.part == Part::Runtime).count();
    let mut seen = BTreeSet::new();
    for t in terms {
        let r = of(t.part).ok_or("admission_term_set")?;
        let placed = match (r.scope, t.source) {
            (Scope::Global, None) => true,
            (Scope::Source, Some(s)) => usize::try_from(s).is_ok_and(|s| s < sources),
            _ => false,
        };
        if !placed || !seen.insert((t.part, t.source)) {
            return Err("admission_term_set");
        }
    }
    let globals = PARTS_10.iter().filter(|r| r.scope == Scope::Global).count();
    let each = PARTS_10.len().saturating_sub(globals);
    if Some(seen.len())
        != each
            .checked_mul(sources)
            .and_then(|n| n.checked_add(globals))
    {
        return Err("admission_term_set");
    }
    // Each term as its row has it.
    for t in terms {
        let r = of(t.part).ok_or("admission_term_set")?;
        let held = matches!(
            (r.held, t.held),
            (HeldBy::Built, Held::Built)
                | (HeldBy::Channel, Held::Channel)
                | (HeldBy::Cap, Held::Cap)
                | (HeldBy::Once, Held::Once)
                | (HeldBy::Gate, Held::Gate { .. })
                | (HeldBy::Host, Held::Host)
        );
        let stated = matches!(
            (r.stated, t.stated),
            (Stater::Library, StatedBy::Library)
                | (Stater::Host, StatedBy::Host)
                | (Stater::Either, _)
        );
        if t.phase != r.phase || t.basis != r.basis || !held || !stated {
            return Err("admission_table");
        }
    }
    // No version-10 writer has a gate built.
    if terms.iter().any(|t| t.held == (Held::Gate { built: true })) {
        return Err("admission_built");
    }
    if terms.iter().any(|t| {
        w(t.count)
            .saturating_mul(w(t.cost))
            .saturating_add(w(t.rest))
            != w(t.bytes)
    }) {
        return Err("admission_bytes");
    }
    let run = |p: Part| {
        terms
            .iter()
            .find(|t| t.part == p)
            .ok_or("admission_term_set")
    };
    let at = |p: Part, s: u32| {
        terms
            .iter()
            .find(|t| t.part == p && t.source == Some(s))
            .ok_or("admission_term_set")
    };
    let sources = u32::try_from(sources).map_err(|_| "admission_term_set")?;
    // The library's constants, where a cost is one verbatim.
    let model = m.model;
    let pending = model
        .live_cell
        .checked_sub(model.ended_cell)
        .ok_or("admission_model")?;
    let bound = [
        (run(Part::Cells)?.cost, model.ended_cell),
        (run(Part::Pending)?.cost, pending),
        (run(Part::Barriers)?.cost, model.barrier),
        (run(Part::Records)?.cost, model.record),
        (run(Part::ShellAttempts)?.cost, model.attempt),
        (run(Part::Decoder)?.cost, model.decoder),
    ];
    let event = model.event.ok_or("admission_model")?;
    let runtimes = terms.iter().filter(|t| t.part == Part::Runtime);
    if bound.iter().any(|(cost, constant)| cost != constant)
        || runtimes
            .clone()
            .any(|t| t.cost != event || t.rest != model.runtime)
    {
        return Err("admission_model");
    }
    // The evidence itself: two copies at once — admission's and the header's
    // while it is written —, each its terms, a row a source and its unheld
    // parts at the constants' sizes.
    let evidence = run(Part::Evidence)?;
    let unheld_parts = terms
        .iter()
        .filter(|t| matches!(t.held, Held::Gate { .. }))
        .count();
    let copy = [
        (terms.len(), model.term),
        (usize::try_from(sources).unwrap_or(usize::MAX), model.row),
        (unheld_parts, model.part),
    ]
    .iter()
    .try_fold(0u64, |sum, (n, size)| {
        u64::try_from(*n)
            .ok()
            .and_then(|n| n.checked_mul(*size))
            .and_then(|b| sum.checked_add(b))
    });
    if (evidence.count, Some(evidence.cost), evidence.rest) != (2, copy, 0) {
        return Err("admission_evidence");
    }
    if run(Part::Decoder)?.count != u64::from(run(Part::Reader)?.count > 0) {
        return Err("admission_decoder");
    }
    // The counts terms share: a source's sockets its cap, the late steps'
    // records; a probe's costs its connection's and its heap's.
    for s in 0..sources {
        let connections = at(Part::Connections, s)?;
        let io = at(Part::Io, s)?;
        let followers = [
            io.count,
            at(Part::Answers, s)?.count,
            at(Part::Probes, s)?.count,
            at(Part::ProbesIo, s)?.count,
        ];
        if followers.iter().any(|n| *n != connections.count)
            || at(Part::Probes, s)?.cost != connections.cost
            || at(Part::ProbesIo, s)?.cost != io.cost
        {
            return Err("admission_shared");
        }
    }
    let late = run(Part::Records)?.count;
    let followers = [
        run(Part::ShellAttempts)?.count,
        run(Part::CarrierAttempts)?.count,
        run(Part::Decorations)?.count,
    ];
    if followers.iter().any(|n| *n != late) {
        return Err("admission_shared");
    }
    // The rows: one a source at most, each a source's; its sockets its cap,
    // its runtime's events its own but where a test assumed the runtimes; a
    // source with no row holds no socket.
    let mut rowed = BTreeSet::new();
    if a.sources
        .iter()
        .any(|r| r.source >= sources || !rowed.insert(r.source))
    {
        return Err("admission_rows");
    }
    for s in 0..sources {
        let sockets = at(Part::Connections, s)?.count;
        let fits = match a.sources.iter().find(|r| r.source == s) {
            Some(r) => {
                sockets == r.cap && (a.assumed.runtimes || at(Part::Runtime, s)?.count == r.events)
            }
            None => sockets == 0,
        };
        if !fits {
            return Err("admission_rows");
        }
    }
    // The phases' sums, the pool at the catch-up, the need.
    let sum = |phase: Phase| -> u128 {
        terms
            .iter()
            .filter(|t| t.phase == phase)
            .map(|t: &MemoryTerm| w(t.bytes))
            .fold(0u128, u128::saturating_add)
    };
    let (fixed, stages, gates, ran, pool) = (
        sum(Phase::Fixed),
        sum(Phase::Stages),
        sum(Phase::Gates),
        sum(Phase::Run),
        sum(Phase::Pool),
    );
    if pool
        .saturating_mul(1000)
        .div_ceil(1000u128.saturating_sub(kappa))
        != w(m.pool)
    {
        return Err("admission_pool");
    }
    let need = fixed
        .saturating_add(stages)
        .max(fixed.saturating_add(gates))
        .max(fixed.saturating_add(ran).saturating_add(w(m.pool)));
    if need != w(m.need) {
        return Err("admission_need");
    }
    let stated = w(m.stated);
    if stated.checked_sub(need) != Some(w(m.slack)) {
        return Err("admission_slack");
    }
    if stated.checked_sub(fixed.saturating_add(ran)) != Some(w(m.budget)) {
        return Err("admission_budget");
    }
    // The parts no enforcer holds, each once in the terms' order.
    let mut unheld: Vec<Part> = Vec::new();
    for t in terms {
        if matches!(t.held, Held::Gate { .. }) && !unheld.contains(&t.part) {
            unheld.push(t.part);
        }
    }
    if unheld != m.unheld {
        return Err("admission_unheld");
    }
    // A pool no enforcer holds runs in a scope of its own, its `memory.max`
    // finite at the process's own group.
    if !unheld.is_empty() && m.limit.max.is_none_or(|b| b.depth != 0) {
        return Err("admission_scope");
    }
    let within = |b: Option<crate::journal::Bound>| b.is_none_or(|b| m.stated <= b.bytes);
    if !within(m.limit.max) || !within(m.limit.high) {
        return Err("admission_limit");
    }
    Ok(())
}

/// A count in the recheck's arithmetic: none of its sums or products of
/// `u64`s comes near `u128`'s top, so none saturates.
fn w(n: u64) -> u128 {
    u128::from(n)
}

/// A part's row in version 10's table.
fn of(part: Part) -> Option<Row> {
    PARTS_10.iter().copied().find(|r| r.part == part)
}
