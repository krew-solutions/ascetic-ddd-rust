//! K2b-7e-2b-3d-3b-3 of ADR-0047 (decision 22): the fold's recheck of
//! admission's evidence. Version 10's table is its data file's; consistent
//! evidence passes, whichever phase binds, a source with no row before one
//! with; each rule refuses its tampers by its own code — every code reached —,
//! each tamper otherwise consistent, so that no earlier rule takes it; a
//! header that disagrees with itself folds void.

mod admission;

use admission::*;
use ascetic_ddd_scheduler::journal::{self, Admission, Header, Part, Phase};
use ascetic_ddd_scheduler::recheck::{self, PARTS_10, PARTS_11, Row, WHYS_10};

/// The versions version 10's rules serve (K2b-7e-2b-3e): each runs them
/// over its own table, so that every census runs at each.
const VERSIONS: [u32; 2] = [10, 11];
use ascetic_ddd_scheduler::verdict::{Log, Verdict, Void, fold};

/// The table is its data file's, row by row.
#[test]
fn the_table_is_its_data_file_s() {
    let ten: Vec<Row> = serde_json::from_str(include_str!("../schema/parts-10.json")).unwrap();
    assert_eq!(ten, PARTS_10);
    let eleven: Vec<Row> = serde_json::from_str(include_str!("../schema/parts-11.json")).unwrap();
    assert_eq!(eleven, PARTS_11);
}

/// Consistent evidence passes whichever phase binds; a row matched by its
/// source, not its place; a runtime's events another's only where a test
/// assumed the runtimes.
#[test]
fn consistent_evidence_passes() {
    for (name, b) in bases() {
        for v in VERSIONS {
            let (k, a) = consistent_at(&b, v);
            assert_eq!(recheck::admission(v, k, &a), Ok(()), "{name} at {v}");
        }
    }
    // The pool does not divide at the base's catch-up: its ceiling is read.
    let (k, a) = consistent(&base());
    let pool: u64 = a
        .memory
        .terms
        .iter()
        .filter(|t| t.phase == Phase::Pool)
        .map(|t| t.bytes)
        .sum();
    assert_ne!(pool * 1000 % u64::from(1000 - k), 0);
    // Each binding phase binds where it is meant to.
    let binds = |b: &Base| {
        let (_, a) = consistent(b);
        let m = &a.memory;
        m.need - m.pool
            == m.terms
                .iter()
                .filter(|t| matches!(t.phase, Phase::Fixed | Phase::Run))
                .map(|t| t.bytes)
                .sum::<u64>()
    };
    assert!(binds(&base()));
    assert!(!binds(&Base {
        judging: 1 << 40,
        ..base()
    }));
    assert!(!binds(&Base {
        gate_stages: 1 << 40,
        ..base()
    }));
}

/// Each rule refuses its tampers by its own code, and every code is reached.
#[test]
fn each_rule_refuses_its_tampers_by_its_code() {
    let mut reached = std::collections::BTreeSet::new();
    for (name, b, tamper, why) in census() {
        for v in VERSIONS {
            let (mut k, mut a) = consistent_at(&b, v);
            tamper(&mut k, &mut a);
            assert_eq!(recheck::admission(v, k, &a), Err(why), "{name} at {v}");
        }
        reached.insert(why);
    }
    assert_eq!(reached, WHYS_10.into_iter().collect(), "every rule reached");
    // Another version's rules are no version 10's.
    let (k, a) = consistent(&base());
    assert_eq!(recheck::admission(9, k, &a), Err("admission_version"));
}

/// The fold voids a header whose admission disagrees with itself, at line 0
/// by the rule's code; consistent, it reads on.
#[test]
fn a_header_that_disagrees_with_itself_folds_void() {
    let folded = |a: Admission, k: u32| {
        let header = Header {
            catch_up: k,
            admission: Some(a),
            ..journal::sample::header()
        };
        let log = Log {
            sha256: header.log.clone(),
            records: 0,
        };
        fold(&header, &log, std::iter::empty(), std::iter::empty()).verdict
    };
    let (k, a) = consistent(&base());
    assert_eq!(folded(a.clone(), k), Verdict::Pass);
    let mut tampered = a;
    tampered.memory.need += 1;
    assert_eq!(
        folded(tampered, k),
        Verdict::Void(Void::Incoherent {
            line: 0,
            why: "admission_need"
        })
    );
}

/// What a single edit of a term moves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Edit {
    Count,
    Cost,
    Rest,
    Stater,
}

/// The edits the journal alone cannot tell from another run's (K2b-7e-2b-3d-3b-3b):
/// a count no enforcer's other count or row binds, a cost no constant or
/// other cost binds, any rest but a runtime's own, a carrier's part's stater.
fn free(part: Part, edit: Edit, source: Option<u32>, assumed: bool) -> bool {
    use Part::*;
    match edit {
        Edit::Count => {
            matches!(
                part,
                Reader
                    | Journal
                    | JournalOut
                    | Judging
                    | GateStages
                    | GateRun
                    | CarrierSource
                    | Keeping
                    | BoundTo
                    | Kept
                    | Binding
                    | Cells
                    | Pending
                    | Barriers
                    | Probe
            ) || (part == Runtime && (source == Some(0) || assumed))
        }
        Edit::Cost => matches!(
            part,
            Reader
                | Journal
                | JournalOut
                | Judging
                | GateStages
                | GateRun
                | CarrierSource
                | Keeping
                | Answers
                | BoundTo
                | Kept
                | Binding
                | CarrierAttempts
                | Decorations
                | Probe
        ),
        Edit::Rest => !matches!(part, Runtime | Evidence),
        Edit::Stater => matches!(
            part,
            CarrierSource | Connections | Answers | Probes | CarrierAttempts
        ),
    }
}

/// K2b-7e-2b-3d-3b-3b: every single edit of a consistent evidence — a term's
/// count, cost or rest one more, its stater the other —, everything
/// downstream made again, is refused but those on the pinned free list: a
/// rule's binding lost shows as an edit refused no more.
#[test]
fn every_single_edit_is_refused_but_the_free_ones() {
    use ascetic_ddd_scheduler::journal::StatedBy;
    for b in [
        base(),
        Base {
            assumed: true,
            ..base()
        },
    ] {
        for v in VERSIONS {
            let (k, consistent) = consistent_at(&b, v);
            for i in 0..consistent.memory.terms.len() {
                for edit in [Edit::Count, Edit::Cost, Edit::Rest, Edit::Stater] {
                    let mut a = consistent.clone();
                    let t = &mut a.memory.terms[i];
                    match edit {
                        Edit::Count => t.count += 1,
                        Edit::Cost => t.cost += 1,
                        Edit::Rest => t.rest += 1,
                        Edit::Stater => {
                            t.stated = match t.stated {
                                StatedBy::Library => StatedBy::Host,
                                StatedBy::Host => StatedBy::Library,
                            }
                        }
                    }
                    t.bytes = t.count * t.cost + t.rest;
                    let (part, source) = (t.part, t.source);
                    settle(&mut a.memory, k, 1_000);
                    let got = recheck::admission(v, k, &a);
                    assert_eq!(
                        got.is_ok(),
                        free(part, edit, source, b.assumed),
                        "{part:?} at {source:?}, {edit:?}, version {v}: {got:?}"
                    );
                }
            }
        }
    }
}

/// K2b-7e-2b-3e-2c: the probe is version 11's part — its evidence refused at
/// version 10, version 10's without it refused at 11, each by its term set;
/// a probe stated by the library refused by the table.
#[test]
fn the_probe_is_version_11_s() {
    let b = base();
    let (k, eleven) = consistent_at(&b, 11);
    let (_, ten) = consistent_at(&b, 10);
    assert_eq!(recheck::admission(11, k, &eleven), Ok(()));
    assert_eq!(recheck::admission(10, k, &ten), Ok(()));
    assert_eq!(
        recheck::admission(10, k, &eleven),
        Err("admission_term_set")
    );
    assert_eq!(recheck::admission(11, k, &ten), Err("admission_term_set"));
    let mut library = eleven.clone();
    let t = library
        .memory
        .terms
        .iter_mut()
        .find(|t| t.part == Part::Probe)
        .unwrap();
    t.stated = journal::StatedBy::Library;
    assert_eq!(recheck::admission(11, k, &library), Err("admission_table"));
}
