//! Consistent admission evidence of versions 10 and 11 and its tampers, each refused
//! by one rule's code (ADR-0047 decision 22, K2b-7e-2b-3d-3b-3): shared by the
//! recheck's tests and the fold's twins against the definition.

#![allow(dead_code)]

use ascetic_ddd_scheduler::journal::{
    self, Admission, Assumed, Bound, Held, LimitOrigin, LimitRead, MemoryFit, MemoryOrigin,
    MemoryTerm, Model, Part, Phase, SourceFit, StatedBy,
};
use ascetic_ddd_scheduler::recheck::{HeldBy, PARTS_11, Row, Stater};

/// What a consistent evidence is made of: the catch-up, stage 2's and the
/// gates' bytes, each source's row by its cap — none for a source with no
/// sockets —, whether a test assumed the runtimes and how far a row's events
/// are from its runtime's.
#[derive(Clone)]
pub struct Base {
    pub kappa: u32,
    pub judging: u64,
    pub gate_stages: u64,
    pub rows: Vec<Option<u64>>,
    pub assumed: bool,
    pub off: u64,
}

pub const RUN: Base = Base {
    kappa: 10,
    judging: 1,
    gate_stages: 1,
    rows: Vec::new(),
    assumed: false,
    off: 0,
};

pub fn base() -> Base {
    Base {
        rows: vec![None, Some(8)],
        ..RUN
    }
}

pub const MODEL: Model = Model {
    ended_cell: 100,
    live_cell: 180,
    barrier: 64,
    record: 300,
    decoder: 2_600,
    attempt: 1_536,
    runtime: 2_600,
    event: Some(12),
    term: 96,
    row: 32,
    part: 1,
};

pub fn row_of(part: Part) -> Row {
    PARTS_11.into_iter().find(|r| r.part == part).unwrap()
}

pub fn term(part: Part, source: Option<u32>, count: u64, cost: u64, rest: u64) -> MemoryTerm {
    let r = row_of(part);
    MemoryTerm {
        part,
        source,
        phase: r.phase,
        count,
        cost,
        rest,
        bytes: count * cost + rest,
        held: match r.held {
            HeldBy::Built => Held::Built,
            HeldBy::Channel => Held::Channel,
            HeldBy::Cap => Held::Cap,
            HeldBy::Once => Held::Once,
            HeldBy::Gate => Held::Gate { built: false },
            HeldBy::Host => Held::Host,
        },
        stated: match (r.stated, source) {
            (Stater::Library, _) => StatedBy::Library,
            (Stater::Host, _) | (Stater::Either, Some(0)) => StatedBy::Host,
            (Stater::Either, _) => StatedBy::Library,
        },
        basis: r.basis,
    }
}

/// The evidence `b` makes, consistent: every term in the journal's order, its
/// sums, its pool at the catch-up, its need, a slack of 1,000, its limit above.
pub fn consistent(b: &Base) -> (u32, Admission) {
    consistent_at(b, ascetic_ddd_scheduler::journal::JOURNAL_VERSION)
}

/// The evidence `b` makes at `version`: version 11's with the host's probe
/// (K2b-7e-2b-3e-2c), a read of 4 KiB stated.
pub fn consistent_at(b: &Base, version: u32) -> (u32, Admission) {
    let m = MODEL;
    let late = 9;
    let mut terms = vec![
        term(Part::Reader, None, 6, 1_000, 500),
        term(Part::Decoder, None, 1, m.decoder, 0),
    ];
    let mut rows = Vec::new();
    for (s, cap) in (0u32..).zip(&b.rows) {
        let source = Some(s);
        let c = cap.unwrap_or(0);
        terms.extend([
            term(Part::Runtime, source, c + 2, 12, m.runtime),
            term(Part::CarrierSource, source, 1, 10, 0),
            term(Part::Keeping, source, 1, 20, 0),
            term(Part::Connections, source, c, 4_096, 0),
            term(Part::Io, source, c, 100, 0),
            term(Part::Answers, source, c, 65_536, 0),
            term(Part::Probes, source, c, 4_096, 0),
            term(Part::ProbesIo, source, c, 100, 0),
        ]);
        if let Some(cap) = cap {
            rows.push(SourceFit {
                source: s,
                needed: 1,
                cap: *cap,
                events: cap + 2 + b.off,
            });
        }
    }
    terms.extend([
        term(Part::Journal, None, 66, 300, 1_000),
        term(Part::JournalOut, None, 1, 123, 0),
        term(Part::Judging, None, 1, b.judging, 0),
        term(Part::GateStages, None, 1, b.gate_stages, 0),
        term(Part::GateRun, None, 1, 50, 0),
        term(Part::Cells, None, 40, m.ended_cell, 0),
        term(Part::Pending, None, 7, m.live_cell - m.ended_cell, 0),
        term(Part::Barriers, None, 2, m.barrier, 40),
        term(Part::Records, None, late, m.record, 77),
        term(Part::BoundTo, None, 1, 0, 10),
        term(Part::Kept, None, 1, 0, 11),
        term(Part::Binding, None, 1, 0, 12),
        term(Part::ShellAttempts, None, late, m.attempt, 5),
        term(Part::CarrierAttempts, None, late, 7, 9),
        term(Part::Decorations, None, late, 3, 0),
    ]);
    // Version 11's probe, a host's read of 4 KiB stated (K2b-7e-2b-3e-2c).
    if version >= 11 {
        terms.push(term(Part::Probe, None, 1, 4_096, 0));
    }
    // The evidence: two copies, each its terms (this one with them), a row a
    // source and the unheld parts.
    let unheld = terms
        .iter()
        .filter(|t| matches!(t.held, Held::Gate { .. }))
        .count() as u64;
    let copy = (terms.len() as u64 + 1) * m.term + b.rows.len() as u64 * m.row + unheld * m.part;
    terms.push(term(Part::Evidence, None, 2, copy, 0));
    let mut memory = MemoryFit {
        stated: 0,
        origin: MemoryOrigin::Declared,
        limit: LimitRead {
            max: None,
            high: None,
            origin: LimitOrigin::Assumed,
        },
        model: m,
        terms,
        unheld: Vec::new(),
        budget: 0,
        pool: 0,
        need: 0,
        slack: 0,
    };
    settle(&mut memory, b.kappa, 1_000);
    let admission = Admission {
        sources: rows,
        descriptors: None,
        assumed: Assumed {
            runtimes: b.assumed,
            ..Assumed::default()
        },
        memory,
    };
    (b.kappa, admission)
}

/// The sums of `m`'s terms made again: the pool at `kappa`, the need, the
/// memory stated `slack` above it, the budget, the unheld parts, the limit
/// above the memory.
pub fn settle(m: &mut MemoryFit, kappa: u32, slack: u64) {
    let sum = |p: Phase| {
        m.terms
            .iter()
            .filter(|t| t.phase == p)
            .map(|t| t.bytes)
            .sum::<u64>()
    };
    let (fixed, stages, gates, run, pool) = (
        sum(Phase::Fixed),
        sum(Phase::Stages),
        sum(Phase::Gates),
        sum(Phase::Run),
        sum(Phase::Pool),
    );
    m.pool = (pool * 1000).div_ceil(u64::from(1000 - kappa));
    m.need = (fixed + stages)
        .max(fixed + gates)
        .max(fixed + run + m.pool);
    m.stated = m.need + slack;
    m.slack = slack;
    m.budget = m.stated - (fixed + run);
    m.unheld = Vec::new();
    for t in &m.terms {
        if matches!(t.held, Held::Gate { .. }) && !m.unheld.contains(&t.part) {
            m.unheld.push(t.part);
        }
    }
    m.limit.max = Some(Bound {
        bytes: m.stated + 5_000,
        depth: 0,
    });
    m.limit.high = Some(Bound {
        bytes: m.stated + 10,
        depth: 2,
    });
}

pub fn at(a: &mut Admission, part: Part, source: Option<u32>) -> &mut MemoryTerm {
    a.memory
        .terms
        .iter_mut()
        .find(|t| t.part == part && t.source == source)
        .unwrap()
}

/// A term's count moved by `d`, its bytes with it.
pub fn count(a: &mut Admission, part: Part, source: Option<u32>, d: i64) {
    let t = at(a, part, source);
    t.count = t.count.checked_add_signed(d).unwrap();
    t.bytes = t.count * t.cost + t.rest;
}

/// A term's cost moved by `d`, its bytes with it.
pub fn cost(a: &mut Admission, part: Part, source: Option<u32>, d: i64) {
    let t = at(a, part, source);
    t.cost = t.cost.checked_add_signed(d).unwrap();
    t.bytes = t.count * t.cost + t.rest;
}

/// The source with a row in the base.
pub const ROW: Option<u32> = Some(1);

pub type Tamper = fn(&mut u32, &mut Admission);

/// Each rule's tampers, the base each is made on, the code each is refused by.
pub fn census() -> Vec<(&'static str, Base, Tamper, &'static str)> {
    let stages = Base {
        judging: 1 << 40,
        ..base()
    };
    let gates = Base {
        gate_stages: 1 << 40,
        ..base()
    };
    vec![
        ("κ at 1000", base(), |k, _| *k = 1000, "admission_kappa"),
        ("κ past 1000", base(), |k, _| *k = 4000, "admission_kappa"),
        (
            "a part missing",
            base(),
            |_, a| a.memory.terms.retain(|t| t.part != Part::Judging),
            "admission_term_set",
        ),
        (
            "a part twice, another missing",
            base(),
            |_, a| at(a, Part::Kept, None).part = Part::Binding,
            "admission_term_set",
        ),
        (
            "a source's part at a source past the runtimes'",
            base(),
            |_, a| at(a, Part::ProbesIo, ROW).source = Some(2),
            "admission_term_set",
        ),
        (
            "a run's part given a source",
            base(),
            |_, a| at(a, Part::Judging, None).source = Some(0),
            "admission_term_set",
        ),
        (
            "a source's part given none",
            base(),
            |_, a| at(a, Part::Keeping, ROW).source = None,
            "admission_term_set",
        ),
        (
            "a part twice, of nothing, beside every other",
            base(),
            |_, a| {
                let mut t = *at(a, Part::Kept, None);
                (t.count, t.rest, t.bytes) = (0, 0, 0);
                a.memory.terms.push(t);
            },
            "admission_term_set",
        ),
        (
            "a part of nothing at a source past the runtimes', beside every other",
            base(),
            |_, a| {
                let mut t = *at(a, Part::Keeping, ROW);
                (t.source, t.count, t.cost, t.bytes) = (Some(2), 0, 0, 0);
                a.memory.terms.push(t);
            },
            "admission_term_set",
        ),
        (
            "a phase moved",
            base(),
            |_, a| at(a, Part::Cells, None).phase = Phase::Stages,
            "admission_table",
        ),
        (
            "a basis moved",
            base(),
            |_, a| at(a, Part::Decoder, None).basis = journal::Basis::Heap,
            "admission_table",
        ),
        (
            "a holder moved",
            base(),
            |_, a| at(a, Part::Cells, None).held = Held::Once,
            "admission_table",
        ),
        (
            "a library's part the host's",
            base(),
            |_, a| at(a, Part::Cells, None).stated = StatedBy::Host,
            "admission_table",
        ),
        (
            "a host's part the library's",
            base(),
            |_, a| at(a, Part::Io, ROW).stated = StatedBy::Library,
            "admission_table",
        ),
        (
            "a gate built",
            base(),
            |_, a| at(a, Part::Cells, None).held = Held::Gate { built: true },
            "admission_built",
        ),
        (
            "bytes one more",
            base(),
            |_, a| at(a, Part::Journal, None).bytes += 1,
            "admission_bytes",
        ),
        (
            "bytes one less",
            base(),
            |_, a| at(a, Part::Journal, None).bytes -= 1,
            "admission_bytes",
        ),
        (
            "a product past 2^64, wrapped",
            base(),
            |_, a| {
                let t = at(a, Part::Journal, None);
                (t.count, t.cost) = (1 << 33, 1 << 33);
                t.bytes = t.rest;
            },
            "admission_bytes",
        ),
        (
            "the cells' cost",
            base(),
            |_, a| cost(a, Part::Cells, None, 1),
            "admission_model",
        ),
        (
            "the pending's cost",
            base(),
            |_, a| cost(a, Part::Pending, None, 1),
            "admission_model",
        ),
        (
            "a key's cost",
            base(),
            |_, a| cost(a, Part::Barriers, None, 1),
            "admission_model",
        ),
        (
            "a record's cost",
            base(),
            |_, a| cost(a, Part::Records, None, 1),
            "admission_model",
        ),
        (
            "the shell's attempt's cost",
            base(),
            |_, a| cost(a, Part::ShellAttempts, None, 1),
            "admission_model",
        ),
        (
            "the decoder's cost",
            base(),
            |_, a| cost(a, Part::Decoder, None, 1),
            "admission_model",
        ),
        (
            "an event's cost",
            base(),
            |_, a| cost(a, Part::Runtime, ROW, 1),
            "admission_model",
        ),
        (
            "a runtime's own",
            base(),
            |_, a| {
                let t = at(a, Part::Runtime, Some(0));
                t.rest += 1;
                t.bytes += 1;
            },
            "admission_model",
        ),
        (
            "an event of no size",
            base(),
            |_, a| a.memory.model.event = None,
            "admission_model",
        ),
        (
            "a live cell below an ended one",
            base(),
            |_, a| a.memory.model.live_cell = 0,
            "admission_model",
        ),
        (
            "a third copy of the evidence",
            base(),
            |_, a| count(a, Part::Evidence, None, 1),
            "admission_evidence",
        ),
        (
            "the evidence a byte short",
            base(),
            |_, a| cost(a, Part::Evidence, None, -1),
            "admission_evidence",
        ),
        (
            "the evidence's own beside it",
            base(),
            |_, a| {
                let t = at(a, Part::Evidence, None);
                t.rest += 1;
                t.bytes += 1;
            },
            "admission_evidence",
        ),
        (
            "a term of the evidence a byte more",
            base(),
            |_, a| a.memory.model.term += 1,
            "admission_evidence",
        ),
        (
            "a row of the evidence a byte more",
            base(),
            |_, a| a.memory.model.row += 1,
            "admission_evidence",
        ),
        (
            "a part of the evidence a byte more",
            base(),
            |_, a| a.memory.model.part += 1,
            "admission_evidence",
        ),
        (
            "the decoder dropped",
            base(),
            |_, a| count(a, Part::Decoder, None, -1),
            "admission_decoder",
        ),
        (
            "the reader dropped",
            base(),
            |_, a| count(a, Part::Reader, None, -6),
            "admission_decoder",
        ),
        (
            "a socket's io off the cap",
            base(),
            |_, a| count(a, Part::Io, ROW, 1),
            "admission_shared",
        ),
        (
            "a socket's answers off the cap",
            base(),
            |_, a| count(a, Part::Answers, ROW, 1),
            "admission_shared",
        ),
        (
            "a probe's off the cap",
            base(),
            |_, a| count(a, Part::Probes, ROW, 1),
            "admission_shared",
        ),
        (
            "a probe's io off the cap",
            base(),
            |_, a| count(a, Part::ProbesIo, ROW, 1),
            "admission_shared",
        ),
        (
            "a probe's cost not the connection's",
            base(),
            |_, a| cost(a, Part::Probes, ROW, -1),
            "admission_shared",
        ),
        (
            "a probe's heap not the connection's",
            base(),
            |_, a| cost(a, Part::ProbesIo, ROW, -1),
            "admission_shared",
        ),
        (
            "the shell's attempts off the late steps",
            base(),
            |_, a| count(a, Part::ShellAttempts, None, 1),
            "admission_shared",
        ),
        (
            "the carrier's attempts off the late steps",
            base(),
            |_, a| count(a, Part::CarrierAttempts, None, 1),
            "admission_shared",
        ),
        (
            "the decorations off the late steps",
            base(),
            |_, a| count(a, Part::Decorations, None, 1),
            "admission_shared",
        ),
        (
            "every socket a cap past the row's",
            base(),
            |_, a| {
                for p in [
                    Part::Connections,
                    Part::Io,
                    Part::Answers,
                    Part::Probes,
                    Part::ProbesIo,
                ] {
                    count(a, p, Some(1), 1);
                }
            },
            "admission_rows",
        ),
        (
            "a runtime's events not the row's",
            Base { off: 1, ..base() },
            |_, _| {},
            "admission_rows",
        ),
        (
            "a source with no row holding sockets",
            base(),
            |_, a| {
                for p in [
                    Part::Connections,
                    Part::Io,
                    Part::Answers,
                    Part::Probes,
                    Part::ProbesIo,
                ] {
                    count(a, p, Some(0), 1);
                }
            },
            "admission_rows",
        ),
        (
            "a row of a source past the runtimes'",
            base(),
            |_, a| a.sources[0].source = 2,
            "admission_rows",
        ),
        (
            "a row twice",
            base(),
            |_, a| {
                let r = a.sources[0].clone();
                a.sources.push(r);
            },
            "admission_rows",
        ),
        (
            "the pool at the floor",
            base(),
            |_, a| a.memory.pool -= 1,
            "admission_pool",
        ),
        ("the pool unscaled", base(), |k, _| *k = 0, "admission_pool"),
        (
            "the need the run's, one more",
            base(),
            |_, a| a.memory.need += 1,
            "admission_need",
        ),
        (
            "the need the run's, one less",
            base(),
            |_, a| a.memory.need -= 1,
            "admission_need",
        ),
        (
            "the need stage 2's, one more",
            stages,
            |_, a| a.memory.need += 1,
            "admission_need",
        ),
        (
            "the need the gates', one more",
            gates,
            |_, a| a.memory.need += 1,
            "admission_need",
        ),
        (
            "the slack one more",
            base(),
            |_, a| a.memory.slack += 1,
            "admission_slack",
        ),
        (
            "a memory below the need, no slack",
            base(),
            |_, a| {
                let m = &mut a.memory;
                m.stated = m.need - 1;
                m.slack = 0;
            },
            "admission_slack",
        ),
        (
            "the budget one more",
            base(),
            |_, a| a.memory.budget += 1,
            "admission_budget",
        ),
        (
            "the budget one less",
            base(),
            |_, a| a.memory.budget -= 1,
            "admission_budget",
        ),
        (
            "no part unheld",
            base(),
            |_, a| a.memory.unheld.clear(),
            "admission_unheld",
        ),
        (
            "a part held once unheld",
            base(),
            |_, a| a.memory.unheld.push(Part::Binding),
            "admission_unheld",
        ),
        (
            "an unheld pool and no max",
            base(),
            |_, a| a.memory.limit.max = None,
            "admission_scope",
        ),
        (
            "an unheld pool and a max above the process's group",
            base(),
            |_, a| {
                if let Some(b) = a.memory.limit.max.as_mut() {
                    b.depth = 1;
                }
            },
            "admission_scope",
        ),
        (
            "a memory past the max",
            base(),
            |_, a| {
                let stated = a.memory.stated;
                if let Some(b) = a.memory.limit.max.as_mut() {
                    b.bytes = stated - 1;
                }
            },
            "admission_limit",
        ),
        (
            "a memory past the high, below the max",
            base(),
            |_, a| {
                let stated = a.memory.stated;
                if let Some(b) = a.memory.limit.high.as_mut() {
                    b.bytes = stated - 1;
                }
            },
            "admission_limit",
        ),
    ]
}

/// The consistent bases, by name: whichever phase binds, rows apart and in
/// any order, no source, a test's runtimes whose rows' events are another's.
pub fn bases() -> Vec<(&'static str, Base)> {
    vec![
        ("the run binds", base()),
        (
            "stage 2 binds",
            Base {
                judging: 1 << 40,
                ..base()
            },
        ),
        (
            "the gates bind",
            Base {
                gate_stages: 1 << 40,
                ..base()
            },
        ),
        (
            "rows apart",
            Base {
                rows: vec![Some(3), None, Some(0)],
                ..base()
            },
        ),
        (
            "no source",
            Base {
                rows: vec![],
                ..base()
            },
        ),
        (
            "a test's runtimes",
            Base {
                assumed: true,
                off: 1_000,
                ..base()
            },
        ),
    ]
}
