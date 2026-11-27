//! K2b-7e-2b-3c-1 of ADR-0047 (decision 22): admission against a stated
//! memory, a pure function. Against a shadow written from the decision's own
//! sentence in u128 — the largest of the stages', the gates' and the run's
//! phases, each with the fixed parts, the run's pool at its catch-up's rate —
//! over arbitrary parts: what a run needs, the least memory admitting it
//! and one byte less refused, every part moving the need when its phase
//! binds, a saturated part refused, a runtime of no stated events refused.

use ascetic_ddd_load_run::carrier::StatedBy;
use ascetic_ddd_load_run::memory::{Failed, Memory, Part, Phase, Planned, SourcePart, fit};
use ascetic_ddd_load_run::stand::GateBytes;
use ascetic_ddd_scheduler::fits::Terms;
use proptest::prelude::*;

/// A source's parts of these values.
fn source(v: [u64; 7]) -> SourcePart {
    SourcePart {
        runtime: Some(v[0]),
        cap: v[1],
        connection: v[2],
        io: v[3],
        answer: v[4],
        carrier: v[5],
        carrier_stated: StatedBy::Library,
        keeping: v[6],
    }
}

/// A run's parts of these values.
fn planned(sources: Vec<SourcePart>, v: [u64; 16], kappa: u64) -> Planned {
    Planned {
        sources,
        reader: v[0],
        decoder: v[1],
        journal: v[2],
        judging: v[3],
        gates: GateBytes {
            stages: v[4],
            run: v[5],
        },
        machine: Terms {
            cells: v[6],
            barriers: v[7],
            records: v[8],
            bound_to: v[9],
            keeping: v[10],
            binding: v[11],
        },
        records: v[12],
        shell_attempts: v[13],
        carrier_attempts: v[14],
        carrier_stated: StatedBy::Library,
        kappa: kappa.min(999),
    }
}

/// The decision's sentence, in u128.
fn shadow(p: &Planned) -> u128 {
    let w = u128::from;
    let fixed = w(p.reader)
        + w(p.decoder)
        + p.sources
            .iter()
            .map(|s| w(s.runtime.unwrap()))
            .sum::<u128>();
    let stages = w(p.judging);
    let gates = w(p.gates.stages)
        + p.sources
            .iter()
            .map(|s| w(s.cap) * (w(s.connection) + w(s.io)))
            .sum::<u128>();
    let run = w(p.journal)
        + w(p.gates.run)
        + p.sources
            .iter()
            .map(|s| {
                w(s.carrier) + w(s.keeping) + w(s.cap) * (w(s.connection) + w(s.io) + w(s.answer))
            })
            .sum::<u128>();
    // The machine's records are priced at the late steps' instead.
    let m = p.machine;
    let pool = w(m.cells)
        + w(m.barriers)
        + w(p.records)
        + w(m.bound_to)
        + w(m.keeping)
        + w(m.binding)
        + w(p.shell_attempts)
        + w(p.carrier_attempts);
    let c = 1000 - w(p.kappa);
    let pool = (pool * 1000).div_ceil(c);
    (fixed + stages).max(fixed + gates).max(fixed + run + pool)
}

/// The phase each part's bytes are held in, as the decision has them.
fn phase(part: Part) -> Phase {
    match part {
        Part::Reader | Part::Decoder | Part::Runtime => Phase::Fixed,
        Part::Judging => Phase::Stages,
        Part::Probes | Part::GateStages => Phase::Gates,
        Part::CarrierSource
        | Part::Keeping
        | Part::Journal
        | Part::Connections
        | Part::Io
        | Part::Answers
        | Part::GateRun => Phase::Run,
        Part::Cells
        | Part::Barriers
        | Part::Records
        | Part::BoundTo
        | Part::Kept
        | Part::Binding
        | Part::ShellAttempts
        | Part::CarrierAttempts => Phase::Pool,
    }
}

/// `p` with `part`'s bytes raised by `d` — every part, none left to `_`.
fn raised(mut p: Planned, part: Part, d: u64) -> Planned {
    let s0 = &mut p.sources[0];
    match part {
        Part::Reader => p.reader += d,
        Part::Decoder => p.decoder += d,
        Part::Runtime => s0.runtime = s0.runtime.map(|r| r + d),
        Part::CarrierSource => s0.carrier += d,
        Part::Keeping => s0.keeping += d,
        Part::Journal => p.journal += d,
        // A count's part raised by its cost: a socket more.
        Part::Connections => s0.connection += d,
        Part::Io => s0.io += d,
        Part::Answers => s0.answer += d,
        Part::Judging => p.judging += d,
        Part::Probes => s0.cap += d,
        Part::GateStages => p.gates.stages += d,
        Part::GateRun => p.gates.run += d,
        Part::Cells => p.machine.cells += d,
        Part::Barriers => p.machine.barriers += d,
        // The machine's own records are not the pool's: the late steps' are.
        Part::Records => p.records += d,
        Part::BoundTo => p.machine.bound_to += d,
        Part::Kept => p.machine.keeping += d,
        Part::Binding => p.machine.binding += d,
        Part::ShellAttempts => p.shell_attempts += d,
        Part::CarrierAttempts => p.carrier_attempts += d,
    }
    p
}

fn arbitrary() -> impl Strategy<Value = Planned> {
    (
        prop::collection::vec(prop::array::uniform7(0u64..1 << 20), 1..4),
        prop::array::uniform16(0u64..1 << 24),
        prop::sample::select(vec![0u64, 1, 10, 500, 999]),
    )
        .prop_map(|(sources, v, kappa)| {
            planned(sources.into_iter().map(source).collect(), v, kappa)
        })
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, failure_persistence: None, ..ProptestConfig::default() })]

    /// What a run needs is the shadow's; the least memory admits it with no
    /// slack, one byte less refuses it naming that least; the pool's budget
    /// what the run's fixed parts leave.
    #[test]
    fn a_run_needs_the_largest_phase(p in arbitrary()) {
        let need = u64::try_from(shadow(&p)).unwrap();
        let fits = fit(&p, Memory::declared(need)).unwrap();
        prop_assert_eq!(fits.need, need);
        prop_assert_eq!(fits.slack, 0);
        prop_assert!(fits.pool <= fits.budget);
        prop_assert!(!fits.held, "the pool's count is held by nothing yet");
        if need > 0 {
            let refused = fit(&p, Memory::declared(need - 1)).unwrap_err();
            prop_assert_eq!(refused.least, Some(need));
            prop_assert!(matches!(refused.failed, Failed::Memory | Failed::Pool));
            let b = refused.binding.unwrap();
            prop_assert!(refused.terms[b].bytes > 0 || need == 0);
        }
        let more = fit(&p, Memory::declared(need.saturating_add(1_000))).unwrap();
        prop_assert_eq!(more.slack, 1_000);
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
    let mut s = source([0; 7]);
    s.runtime = None;
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
