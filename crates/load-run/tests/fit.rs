//! K2b-7e-2b-3c-1 of ADR-0047 (decision 22): admission against a stated
//! memory, a pure function. Against a shadow written from the decision's own
//! sentence in u128 — the largest of the stages', the gates' and the run's
//! phases, each with the fixed parts, the run's pool at its catch-up's rate —
//! over arbitrary parts: what a run needs, the least memory admitting it
//! and one byte less refused, every part moving the need when its phase
//! binds, a saturated part refused, a runtime of no stated events refused.

use ascetic_ddd_load_run::carrier::StatedBy;
use ascetic_ddd_load_run::memory::{Failed, Memory, Model, Part, Phase, Planned, SourcePart, fit};
use ascetic_ddd_load_run::stand::GateBytes;
use ascetic_ddd_scheduler::fits::{Counts, Priced};
use proptest::prelude::*;

/// A source's parts of these values: its runtime's bytes its events, each a
/// byte (the builder's model prices an event at 1 and a runtime's own at 0).
fn source(v: [u64; 7]) -> SourcePart {
    SourcePart {
        events: Some(v[0]),
        cap: v[1],
        connection: v[2],
        io: v[3],
        answer: v[4],
        carrier: v[5],
        carrier_stated: StatedBy::Library,
        keeping: v[6],
    }
}

/// One of a cost: what a part held once is priced at.
fn one(cost: u64) -> Priced {
    Priced {
        count: 1,
        cost,
        rest: 0,
    }
}

/// A run's parts of these values, each part's bytes the value its index
/// names (K2b-7e-2b-3d-3a): the counts the pool shares one, the costs that
/// are constants the model's — drawn, not the library's.
fn planned(sources: Vec<SourcePart>, v: [u64; 16], kappa: u64) -> Planned {
    Planned {
        sources,
        reader: Some(one(v[0])),
        journal: one(v[2]),
        journal_out: 0,
        judging: v[3],
        gates: GateBytes {
            stages: v[4],
            run: v[5],
        },
        machine: Counts {
            held: 1,
            pending: 0,
            barriers: 0,
            barrier_bytes: v[7],
            bound_to: v[9],
            keeping: v[10],
            binding: v[11],
            late: 1,
            late_parts: 0,
        },
        carrier_attempts: (v[14], 0),
        shell_parts: 0,
        decorations: 0,
        carrier_stated: StatedBy::Library,
        model: Model {
            ended_cell: v[6],
            live_cell: v[6] + 1,
            barrier: 0,
            record: v[12],
            decoder: v[1],
            attempt: v[13],
            runtime: 0,
            event: Some(1),
            term: 0,
            row: 0,
            part: 0,
        },
        kappa: kappa.min(999),
    }
}

/// The decision's sentence, in u128: each part's count times its cost and
/// its rest, the constants the model's — what the run needs.
fn shadow(p: &Planned) -> u128 {
    phases(p).need
}

/// The phases' sums the decision has, in u128.
struct Phases {
    fixed: u128,
    run: u128,
    /// The pool at the catch-up's rate.
    pool: u128,
    need: u128,
}

fn phases(p: &Planned) -> Phases {
    let w = u128::from;
    let priced = |q: &Priced| w(q.count) * w(q.cost) + w(q.rest);
    let m = p.model;
    let c = p.machine;
    let reader = p.reader.as_ref().map_or(0, priced);
    let decoder = if p.reader.is_some() { w(m.decoder) } else { 0 };
    // The evidence: two copies, each its terms — the run's 18 with it, 8 a
    // source —, a row a source and the pool's 8 unheld parts.
    let n = w(p.sources.len() as u64);
    let evidence = 2 * ((18 + 8 * n) * w(m.term) + n * w(m.row) + 8 * w(m.part));
    let fixed = reader
        + decoder
        + evidence
        + p.sources
            .iter()
            .map(|s| w(s.events.unwrap()) * w(m.event.unwrap()) + w(m.runtime))
            .sum::<u128>();
    let stages = w(p.judging);
    let gates = w(p.gates.stages)
        + p.sources
            .iter()
            .map(|s| w(s.cap) * (w(s.connection) + w(s.io)))
            .sum::<u128>();
    let run = priced(&p.journal)
        + w(p.journal_out)
        + w(p.gates.run)
        + p.sources
            .iter()
            .map(|s| {
                w(s.carrier) + w(s.keeping) + w(s.cap) * (w(s.connection) + w(s.io) + w(s.answer))
            })
            .sum::<u128>();
    let late = w(c.late);
    let pool = w(c.held) * w(m.ended_cell)
        + w(c.pending) * (w(m.live_cell) - w(m.ended_cell))
        + w(c.barriers) * w(m.barrier)
        + w(c.barrier_bytes)
        + late * w(m.record)
        + w(c.late_parts)
        + w(c.bound_to)
        + w(c.keeping)
        + w(c.binding)
        + late * w(m.attempt)
        + w(p.shell_parts)
        + late * w(p.carrier_attempts.0)
        + w(p.carrier_attempts.1)
        + late * w(p.decorations);
    let k = 1000 - w(p.kappa);
    let pool = (pool * 1000).div_ceil(k);
    Phases {
        fixed,
        run,
        pool,
        need: (fixed + stages).max(fixed + gates).max(fixed + run + pool),
    }
}

/// The parts no enforcer holds yet, as the decision has them: the pool's,
/// but the barrier keys' and one record's bindings, each held once.
fn gate_held(part: Part) -> bool {
    phase(part) == Phase::Pool && !matches!(part, Part::Barriers | Part::Binding)
}

/// Who states each part, as the decision has them: the host its connections'
/// own heap, its journal's output, its gates and its decorations; the
/// carrier a source's own, its connections and answers, its probes' and its
/// attempts'; the library the rest.
fn stated(p: &Planned, part: Part, source: Option<u32>) -> StatedBy {
    let carrier = || p.sources[source.unwrap() as usize].carrier_stated;
    match part {
        Part::Io
        | Part::ProbesIo
        | Part::JournalOut
        | Part::GateStages
        | Part::GateRun
        | Part::Decorations => StatedBy::Host,
        Part::CarrierSource | Part::Connections | Part::Answers | Part::Probes => carrier(),
        Part::CarrierAttempts => p.carrier_stated,
        Part::Reader
        | Part::Decoder
        | Part::Runtime
        | Part::Keeping
        | Part::Journal
        | Part::Judging
        | Part::Cells
        | Part::Pending
        | Part::Barriers
        | Part::Records
        | Part::BoundTo
        | Part::Kept
        | Part::Binding
        | Part::ShellAttempts
        | Part::Evidence => StatedBy::Library,
    }
}

/// The phase each part's bytes are held in, as the decision has them.
fn phase(part: Part) -> Phase {
    match part {
        Part::Reader | Part::Decoder | Part::Runtime | Part::Evidence => Phase::Fixed,
        Part::Judging => Phase::Stages,
        Part::Probes | Part::ProbesIo | Part::GateStages => Phase::Gates,
        Part::CarrierSource
        | Part::Keeping
        | Part::Journal
        | Part::JournalOut
        | Part::Connections
        | Part::Io
        | Part::Answers
        | Part::GateRun => Phase::Run,
        Part::Cells
        | Part::Pending
        | Part::Barriers
        | Part::Records
        | Part::BoundTo
        | Part::Kept
        | Part::Binding
        | Part::ShellAttempts
        | Part::CarrierAttempts
        | Part::Decorations => Phase::Pool,
    }
}

/// `p` with `part`'s bytes raised by `d` — every part, none left to `_`.
fn raised(mut p: Planned, part: Part, d: u64) -> Planned {
    let s0 = &mut p.sources[0];
    match part {
        Part::Reader => {
            p.reader = p.reader.map(|r| Priced {
                cost: r.cost + d,
                ..r
            })
        }
        Part::Decoder => p.model.decoder += d,
        Part::Runtime => s0.events = s0.events.map(|e| e + d),
        Part::CarrierSource => s0.carrier += d,
        Part::Keeping => s0.keeping += d,
        Part::Journal => p.journal.cost += d,
        Part::JournalOut => p.journal_out += d,
        // A count's part raised by its cost: a socket more.
        Part::Connections => s0.connection += d,
        Part::Io => s0.io += d,
        Part::Answers => s0.answer += d,
        Part::Judging => p.judging += d,
        Part::Probes => s0.cap += d,
        Part::ProbesIo => s0.io += d,
        Part::GateStages => p.gates.stages += d,
        Part::GateRun => p.gates.run += d,
        // The ended cell raised with the live, the pending's cost unmoved.
        Part::Cells => {
            p.model.ended_cell += d;
            p.model.live_cell += d;
        }
        Part::Pending => p.machine.pending += d,
        Part::Barriers => p.machine.barrier_bytes += d,
        // The machine's own records are not the pool's: the late steps' are.
        Part::Records => p.model.record += d,
        Part::BoundTo => p.machine.bound_to += d,
        Part::Kept => p.machine.keeping += d,
        Part::Binding => p.machine.binding += d,
        Part::ShellAttempts => p.model.attempt += d,
        Part::CarrierAttempts => p.carrier_attempts.0 += d,
        Part::Decorations => p.decorations += d,
        // A term of the evidence a byte more: every term of both copies.
        Part::Evidence => p.model.term += d,
    }
    p
}

/// A run of drawn parts: each count, each cost and each rest its own draw,
/// the model's constants too — not the library's —, a reader or none, each
/// stater the library or the host.
fn arbitrary() -> impl Strategy<Value = Planned> {
    let small = || 0u64..1 << 12;
    let cost = || 0u64..1 << 20;
    let stater = || prop::sample::select(vec![StatedBy::Library, StatedBy::Host]);
    let sources = prop::collection::vec((prop::array::uniform7(cost()), small(), stater()), 1..4)
        .prop_map(|v| {
            v.into_iter()
                .map(|(c, cap, by)| SourcePart {
                    cap,
                    carrier_stated: by,
                    ..source(c)
                })
                .collect::<Vec<_>>()
        });
    let priced = move || {
        (small(), cost(), cost()).prop_map(|(count, cost, rest)| Priced { count, cost, rest })
    };
    let counts = (
        prop::array::uniform5(small()),
        prop::array::uniform4(cost()),
    )
        .prop_map(|(n, b)| Counts {
            held: n[0],
            pending: n[1],
            barriers: n[2],
            barrier_bytes: b[0],
            bound_to: b[1],
            keeping: b[2],
            binding: b[3],
            late: n[3],
            late_parts: n[4] << 8,
        });
    let model = (
        prop::array::uniform8(cost()),
        prop::array::uniform3(0u64..256),
    )
        .prop_map(|(c, e)| Model {
            ended_cell: c[0],
            live_cell: c[0] + c[1],
            barrier: c[2],
            record: c[3],
            decoder: c[4],
            attempt: c[5],
            runtime: c[6],
            event: Some(c[7] >> 8),
            term: e[0],
            row: e[1],
            part: e[2],
        });
    (
        (
            sources,
            prop::option::of(priced().prop_map(|r| Priced {
                // A reader's records are those ahead and the two beside: never
                // none (`Reader::priced`), which the journal tells from no reader.
                count: r.count.max(2),
                ..r
            })),
            priced(),
            counts,
            model,
        ),
        (prop::array::uniform8(cost()), stater()),
        prop::sample::select(vec![0u64, 1, 10, 500, 999]),
    )
        .prop_map(
            |((sources, reader, journal, machine, model), (v, by), kappa)| Planned {
                sources,
                reader,
                journal,
                journal_out: v[0],
                judging: v[1],
                gates: GateBytes {
                    stages: v[2],
                    run: v[3],
                },
                machine,
                carrier_attempts: (v[4], v[5]),
                shell_parts: v[6],
                decorations: v[7],
                carrier_stated: by,
                model,
                kappa,
            },
        )
}

/// `p` with its `i`th count raised by `d`: a source's cap, its runtime's
/// events, the reader's records, the journal's lines, the cells held, the
/// steps pending, the barrier keys, the late steps.
const COUNTS: usize = 8;

fn counted(mut p: Planned, i: usize, d: u64) -> Planned {
    let s0 = &mut p.sources[0];
    match i {
        0 => s0.cap += d,
        1 => s0.events = s0.events.map(|e| e + d),
        2 => {
            p.reader = p.reader.map(|r| Priced {
                count: r.count + d,
                ..r
            })
        }
        3 => p.journal.count += d,
        4 => p.machine.held += d,
        5 => p.machine.pending += d,
        6 => p.machine.barriers += d,
        _ => p.machine.late += d,
    }
    p
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, failure_persistence: None, ..ProptestConfig::default() })]

    /// What a run needs is the shadow's, its pool at the catch-up's rate and
    /// its budget what the fixed and the run's parts leave, exactly; the
    /// least memory admits it with no slack, one byte less refuses it naming
    /// that least; the parts no enforcer holds the pool's but the keys' and
    /// the bindings'; each term `count · cost + rest`, stated as the decision
    /// has it.
    #[test]
    fn a_run_needs_the_largest_phase(p in arbitrary()) {
        let want = phases(&p);
        let need = u64::try_from(want.need).unwrap();
        let fits = fit(&p, Memory::declared(need)).unwrap();
        prop_assert_eq!(fits.need, need);
        prop_assert_eq!(fits.slack, 0);
        prop_assert_eq!(u128::from(fits.pool), want.pool);
        prop_assert_eq!(u128::from(fits.budget), want.need - (want.fixed + want.run));
        let gate: Vec<Part> = Part::ALL.into_iter().filter(|q| gate_held(*q)).collect();
        prop_assert_eq!(&fits.unheld, &gate);
        for t in &fits.terms {
            prop_assert_eq!(
                u128::from(t.bytes),
                u128::from(t.count) * u128::from(t.cost) + u128::from(t.rest),
                "{:?}", t
            );
            prop_assert_eq!(t.phase, phase(t.part), "{:?}", t);
            prop_assert_eq!(t.stated, stated(&p, t.part, t.source), "{:?}", t);
        }
        if need > 0 {
            let refused = fit(&p, Memory::declared(need - 1)).unwrap_err();
            prop_assert_eq!(refused.least, Some(need));
            prop_assert!(matches!(refused.failed, Failed::Memory | Failed::Pool));
            let b = refused.binding.unwrap();
            prop_assert!(refused.terms[b].bytes > 0 || need == 0);
        }
        let more = fit(&p, Memory::declared(need.saturating_add(1_000))).unwrap();
        prop_assert_eq!(more.slack, 1_000);
        prop_assert_eq!(u128::from(more.budget), want.need + 1_000 - (want.fixed + want.run));
    }

    /// Every part moves the need as the shadow does: by its bytes where its
    /// phase binds, a pool's at the catch-up's rate; none where another
    /// phase binds by more.
    #[test]
    fn every_part_moves_the_need_where_its_phase_binds(p in arbitrary(), d in 1u64..1 << 20) {
        for part in Part::ALL {
            let q = raised(p.clone(), part, d);
            let want = u64::try_from(shadow(&q)).unwrap();
            let got = fit(&q, Memory::declared(u64::MAX / 2)).unwrap().need;
            prop_assert_eq!(got, want, "{:?} in {:?} raised by {}", part, phase(part), d);
        }
    }

    /// Every count moves the need as the shadow does: by its costs, each
    /// count's every term at once where its phase binds.
    #[test]
    fn every_count_moves_the_need_by_its_costs(p in arbitrary(), d in 1u64..64) {
        for i in 0..COUNTS {
            let q = counted(p.clone(), i, d);
            let want = u64::try_from(shadow(&q)).unwrap();
            let got = fit(&q, Memory::declared(u64::MAX / 2)).unwrap().need;
            prop_assert_eq!(got, want, "count {} raised by {}", i, d);
        }
    }
}

/// The evidence admission would journal of `p` fitted at `stated`, its fit's
/// `fits` and the memory at `memory`: a row a source, its cap and its events
/// as fitted, the limit the stated memory at the process's own group.
fn evidence(
    p: &Planned,
    fits: &ascetic_ddd_load_run::memory::Fit,
    memory: u64,
) -> Result<ascetic_ddd_scheduler::journal::Admission, ascetic_ddd_load_run::run::Inadmissible> {
    use ascetic_ddd_scheduler::journal::{Assumed, SourceFit};
    let rows = (0u32..)
        .zip(&p.sources)
        .map(|(source, s)| SourceFit {
            source,
            needed: 0,
            cap: s.cap,
            events: s.events.unwrap(),
        })
        .collect();
    ascetic_ddd_load_run::run::admission(
        fits,
        Memory::declared(memory),
        ascetic_ddd_load_run::live::Limit::assumed(Some((memory.max(fits.need), 0)), None),
        p.model,
        rows,
        None,
        Assumed::default(),
        u32::try_from(p.kappa).unwrap(),
    )
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, failure_persistence: None, ..ProptestConfig::default() })]

    /// K2b-7e-2b-3d-3b-3: admission's evidence of any run it fits passes the
    /// fold's recheck — its terms are the table's, their arithmetic and their
    /// shared counts and constants the recheck's —; of a memory a byte below
    /// the need, refused by its slack, as `fit` refuses the run.
    #[test]
    fn admission_s_evidence_passes_the_fold_s_recheck(p in arbitrary(), more in 0u64..1 << 20) {
        let need = u64::try_from(shadow(&p)).unwrap();
        let fits = fit(&p, Memory::declared(need + more)).unwrap();
        prop_assert!(evidence(&p, &fits, need + more).is_ok(), "{:?}", evidence(&p, &fits, need + more).err());
        if need > 0 {
            prop_assert!(fit(&p, Memory::declared(need - 1)).is_err());
            let at_need = fit(&p, Memory::declared(need)).unwrap();
            prop_assert_eq!(
                evidence(&p, &at_need, need - 1).err(),
                Some(ascetic_ddd_load_run::run::Inadmissible::Evidence { why: "admission_slack" })
            );
        }
    }
}

/// The pool at a catch-up of 1 per mille, its bytes not divided by 999: the
/// prediction rounded up, exactly — 1,000 bytes predicted at 1,002 — and a
/// byte less than the least refused as the pool's.
#[test]
fn the_pool_is_rounded_up_at_its_catch_up() {
    let mut v = [0u64; 16];
    v[6] = 1_000; // cells
    let p = planned(vec![source([0; 7])], v, 1);
    let fits = fit(&p, Memory::declared(u64::MAX / 2)).unwrap();
    assert_eq!(fits.pool, 1_002);
    assert_eq!(fits.need, 1_002);
    assert_eq!(
        fit(&p, Memory::declared(1_001)).unwrap_err().failed,
        Failed::Pool
    );
}

/// A run whose stage 2 holds its largest term while the run binds by more:
/// the binding term the run's, not stage 2's.
#[test]
fn the_binding_term_is_the_binding_phase_s() {
    let mut v = [0u64; 16];
    v[3] = 900; // judging
    v[6] = 500; // cells
    v[13] = 500; // the shell's attempts
    let p = planned(vec![source([10, 0, 0, 0, 0, 0, 0])], v, 0);
    let refused = fit(&p, Memory::declared(500)).unwrap_err();
    assert_eq!(refused.least, Some(1_010));
    let b = refused.terms[refused.binding.unwrap()];
    assert_ne!(b.phase, Phase::Stages, "{b:?}");
}

/// The stages bind a run with no sockets and a small pool: its need the fixed
/// parts and stage 2's judging, exactly.
#[test]
fn stage_2_binds_a_run_with_no_sockets() {
    let mut v = [0u64; 16];
    v[0] = 100; // the reader
    v[3] = 10_000; // judging
    v[6] = 50; // cells
    let p = planned(vec![source([7, 0, 0, 0, 0, 0, 0])], v, 0);
    assert_eq!(
        fit(&p, Memory::declared(u64::MAX / 2)).unwrap().need,
        10_107
    );
}

/// A part past any count is refused, never admitted — however much is stated;
/// a runtime whose events have no stated size, refused as unstated.
#[test]
fn a_part_past_any_count_or_unstated_is_refused() {
    let v = [0u64; 16];
    let p = planned(vec![source([0, u64::MAX / 2, 4, 0, 0, 0, 0])], v, 0);
    assert!(matches!(
        fit(&p, Memory::declared(u64::MAX)).unwrap_err().failed,
        Failed::Overflow(_)
    ));
    // Two parts each within a count, their sum past it.
    let mut w = [0u64; 16];
    w[0] = u64::MAX / 2 + 1;
    w[1] = u64::MAX / 2 + 1;
    let p = planned(vec![source([0; 7])], w, 0);
    assert!(matches!(
        fit(&p, Memory::declared(u64::MAX)).unwrap_err().failed,
        Failed::Overflow(_)
    ));
    // Rule 1: a cost past any count refused whatever its count — a cap of 0
    // whose connection saturated.
    let p = planned(vec![source([0, 0, u64::MAX, 0, 0, 0, 0])], v, 0);
    assert_eq!(
        fit(&p, Memory::declared(u64::MAX)).unwrap_err().failed,
        Failed::Overflow(Part::Connections)
    );
    // A pending cell's cost below the ended one's: no cost, refused.
    let mut p = planned(vec![source([0; 7])], v, 0);
    p.model.live_cell = 0;
    p.model.ended_cell = 1;
    assert_eq!(
        fit(&p, Memory::declared(u64::MAX)).unwrap_err().failed,
        Failed::Overflow(Part::Pending)
    );
    // An event's size no driver states: the runtime unstated.
    let mut p = planned(vec![source([0; 7])], v, 0);
    p.model.event = None;
    assert_eq!(
        fit(&p, Memory::declared(u64::MAX)).unwrap_err().failed,
        Failed::Unstated(Part::Runtime)
    );
    let mut s = source([0; 7]);
    s.events = None;
    let p = planned(vec![s], v, 0);
    assert_eq!(
        fit(&p, Memory::declared(u64::MAX)).unwrap_err().failed,
        Failed::Unstated(Part::Runtime)
    );
}

/// The pool's prediction at the catch-up's rate: twice its bytes at half the
/// reserve gone; refused as the pool's when the fixed parts fit and it does
/// not.
#[test]
fn the_pool_is_priced_at_the_catch_up() {
    let mut v = [0u64; 16];
    v[6] = 1_000;
    v[15] = 500;
    let p = planned(vec![source([0; 7])], v, 500);
    assert_eq!(fit(&p, Memory::declared(u64::MAX / 2)).unwrap().pool, 2_000);
    assert_eq!(
        fit(&p, Memory::declared(1_999)).unwrap_err().failed,
        Failed::Pool
    );
}

/// A run whose parts follow its knobs: a source's cap its ports, the reader's
/// records ahead, the journal's lines behind, the answer cap.
fn knobbed(k: &ascetic_ddd_load_run::memory::Knobs) -> Planned {
    let mut v = [0u64; 16];
    v[0] = 100 * k.ahead as u64;
    v[2] = 50 * k.behind as u64;
    v[3] = 10 * k.answer_cap;
    v[12] = 7_000; // the late records
    planned(
        vec![source([500, k.ports as u64, 1_000, 0, k.answer_cap, 0, 0])],
        v,
        0,
    )
}

/// K2b-7e-2b-3c-2: each knob's remedy the largest of it that admits the run,
/// one past it refused; the least memory at the knobs and at the floors'
/// ports; a knob whose floor does not admit the run no remedy; the binding
/// pool's slice named.
#[test]
fn each_remedy_is_its_knob_s_largest_that_fits() {
    use ascetic_ddd_load_run::memory::{Knobs, Remedy, remedies};
    let knobs = Knobs {
        ports: 400,
        ahead: 64,
        behind: 64,
        answer_cap: 4_096,
    };
    let floors = Knobs {
        ports: 10,
        ahead: 1,
        behind: 1,
        answer_cap: 2,
    };
    let need_at = |k: &Knobs| fit(&knobbed(k), Memory::declared(u64::MAX)).unwrap().need;
    let memory = Memory::declared(need_at(&knobs) / 2);
    let r = remedies(memory, knobs, floors, knobbed);
    assert_eq!(r.least, Some(need_at(&knobs)));
    assert_eq!(
        r.least_at_needed,
        Some(need_at(&Knobs { ports: 10, ..knobs }))
    );
    let fits = |k: Knobs| fit(&knobbed(&k), memory).is_ok();
    let mut seen = 0;
    for remedy in &r.remedies {
        match *remedy {
            Remedy::Memory { at_least } => {
                assert!(fit(&knobbed(&knobs), Memory::declared(at_least)).is_ok());
                assert!(fit(&knobbed(&knobs), Memory::declared(at_least - 1)).is_err());
            }
            Remedy::Ports { at_most } => {
                assert!(fits(Knobs {
                    ports: at_most,
                    ..knobs
                }));
                assert!(!fits(Knobs {
                    ports: at_most + 1,
                    ..knobs
                }));
                seen += 1;
            }
            Remedy::Ahead { at_most } => {
                assert!(
                    fits(Knobs {
                        ahead: at_most,
                        ..knobs
                    }) || at_most == knobs.ahead
                );
                assert!(!fits(Knobs {
                    ahead: at_most + 1,
                    ..knobs
                }));
                seen += 1;
            }
            Remedy::Behind { at_most } => {
                assert!(fits(Knobs {
                    behind: at_most,
                    ..knobs
                }));
                assert!(!fits(Knobs {
                    behind: at_most + 1,
                    ..knobs
                }));
                seen += 1;
            }
            Remedy::AnswerCap { at_most } => {
                assert!(fits(Knobs {
                    answer_cap: at_most,
                    ..knobs
                }));
                assert!(!fits(Knobs {
                    answer_cap: at_most + 1,
                    ..knobs
                }));
                seen += 1;
            }
            Remedy::Slice { part, .. } => assert_ne!(part, Part::Reader),
        }
    }
    // The ports and the answer cap each admit the run alone; the reader's and
    // the journal's are too small to.
    assert!(r.remedies.iter().any(|x| matches!(x, Remedy::Ports { .. })));
    assert!(
        r.remedies
            .iter()
            .any(|x| matches!(x, Remedy::AnswerCap { .. }))
    );
    assert!(!r.remedies.iter().any(|x| matches!(x, Remedy::Ahead { .. })));
    assert!(seen >= 2);
    // No remedy below its knob's floor; none where the floor itself does not
    // fit — the ports' floor the sockets the plan needs.
    for remedy in &r.remedies {
        if let Remedy::Ports { at_most } = *remedy {
            assert!(at_most >= floors.ports, "{at_most} below the floor");
        }
    }
    let high = Knobs {
        ports: 300,
        ..floors
    };
    let short = Memory::declared(
        need_at(&Knobs {
            ports: 300,
            ..knobs
        }) - 1,
    );
    let r = remedies(short, knobs, high, knobbed);
    assert!(
        !r.remedies.iter().any(|x| matches!(x, Remedy::Ports { .. })),
        "{:?}",
        r.remedies
    );
    // The pool binding: its slice named.
    let pooled = |_: &Knobs| {
        let mut v = [0u64; 16];
        v[12] = 1_000_000;
        planned(vec![source([10, 0, 0, 0, 0, 0, 0])], v, 0)
    };
    let r = remedies(Memory::declared(1_000), knobs, floors, pooled);
    assert!(r.remedies.iter().any(|x| matches!(
        x,
        Remedy::Slice {
            part: Part::Records,
            ..
        }
    )));
}
