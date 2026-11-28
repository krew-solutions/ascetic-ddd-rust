//! K2b-7e-2b-3d-1 of ADR-0047 (decision 22): admission binds what it prices.
//! A fit made of the plan, its carrier, its journal and its read-ahead is
//! the only way to the runtimes and to `admit`, which takes nothing else:
//! what was fitted runs; every refusal comes at once; a refused fit builds
//! nothing; the least memory it names admits and a byte less does not. A
//! binary of its own: its allocator counts each thread's bytes.

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

mod pinned;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

use ascetic_ddd_command_log::record::Header;
use ascetic_ddd_command_log::validate::Peaks;
use ascetic_ddd_load_run::carrier::{
    AttemptCosts, Carrier, Costs, Needs, SourceCosts, Unprobed, Unstated,
};
use ascetic_ddd_load_run::clock::{Clock, RealClock};
use ascetic_ddd_load_run::live::{Limit, Live};
use ascetic_ddd_load_run::memory::{Memory, Part};
use ascetic_ddd_load_run::run::{Fitted, Inadmissible, RunError, admit, fit};
use ascetic_ddd_load_run::sink::Journal;
use ascetic_ddd_load_run::source::Plan;
use ascetic_ddd_load_run::stamps::Attempt;
use ascetic_ddd_load_run::stand::GateBytes;

/// The system allocator, counting the bytes each thread holds and their peak.
struct Counting;

thread_local! {
    static NOW: Cell<isize> = const { Cell::new(0) };
    static PEAK: Cell<isize> = const { Cell::new(0) };
}

fn counted(by: isize) {
    let _ = NOW.try_with(|now| {
        let n = now.get().saturating_add(by);
        now.set(n);
        let _ = PEAK.try_with(|peak| peak.set(peak.get().max(n)));
    });
}

fn size(bytes: usize) -> isize {
    isize::try_from(bytes).unwrap_or(isize::MAX)
}

// SAFETY: every call goes to the system allocator unchanged; this only counts
// the sizes it was asked for.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract, passed on.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            counted(size(layout.size()));
        }
        p
    }

    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract, passed on.
        unsafe { System.dealloc(p, layout) };
        counted(-size(layout.size()));
    }

    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: the caller's contract, passed on.
        let q = unsafe { System.realloc(p, layout, size) };
        if !q.is_null() {
            counted(self::size(size));
            counted(-self::size(layout.size()));
        }
        q
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The most `f` held on this thread at once past what it held before.
fn peak_of<T>(f: impl FnOnce() -> T) -> u64 {
    let base = NOW.with(Cell::get);
    PEAK.with(|p| p.set(base));
    drop(f());
    u64::try_from(PEAK.with(Cell::get) - base).unwrap()
}

/// A carrier stating `cap` sockets a source, `needed` of them at once under
/// the plan's bound; it carries nothing — no run is made.
struct Capped {
    cap: usize,
    needed: u64,
}

impl<C: Clock> Carrier<C> for Capped {
    fn carries(&self, _: &Header) -> Result<(), String> {
        Ok(())
    }

    fn needs(&self, _: &Peaks, _: u32) -> Result<Needs, String> {
        Ok(Needs::Sockets {
            needed: self.needed,
            cap: self.cap,
            costs: Costs {
                connection: 4_096,
                io: 0,
                answer: 65_536,
            },
        })
    }

    fn source_costs(&self, _: &Peaks, _: u32) -> Result<SourceCosts, String> {
        Ok(SourceCosts::NOTHING)
    }

    fn attempts(&self, _: &Peaks) -> Result<AttemptCosts, Unstated> {
        Ok(AttemptCosts::NOTHING)
    }

    fn gate_bytes(&self) -> GateBytes {
        GateBytes::NONE
    }

    fn probe(
        &self,
        _: u32,
        _: Arc<C>,
        _: u64,
    ) -> impl std::future::Future<Output = Vec<Unprobed>> + Send + 'static {
        std::future::ready(Vec::new())
    }

    fn carry(&self, _: Attempt<C>) -> impl std::future::Future<Output = ()> + Send + 'static {
        std::future::ready(())
    }
}

/// A memory no fit here exceeds.
const PLENTY: u64 = 1 << 40;

fn fitted(
    cap: usize,
    needed: u64,
    sources: u32,
    memory: u64,
) -> Result<Fitted<RealClock, Capped>, RunError> {
    let mut header = fixture::header();
    header.profile.sources = sources;
    fit(
        Plan::assumed(Arc::new(header), Peaks::default()),
        0,
        Arc::new(Capped { cap, needed }),
        Journal::new(64, 0),
        Memory::declared(memory),
        Limit::assumed(Some((PLENTY, 0)), None),
    )
}

fn refused<T>(r: Result<T, RunError>) -> Inadmissible {
    match r {
        Err(RunError::Inadmissible(why)) => why,
        Err(other) => panic!("refused otherwise: {other}"),
        Ok(_) => panic!("admitted"),
    }
}

/// What was fitted runs: the runtimes `Live::fitted` builds are the ones
/// priced, and another fit's are refused, naming the source.
#[test]
fn what_was_fitted_runs() {
    let a = fitted(8, 1, 1, PLENTY).unwrap();
    let live = Live::fitted(&a).unwrap();
    assert!(admit(a, live.runtimes()).is_ok());
    let b = fitted(64, 1, 1, PLENTY).unwrap();
    let why = refused(admit(b, live.runtimes()));
    assert!(
        why.each().iter().any(
            |w| matches!(w, Inadmissible::Runtime { source: 0, built, fitted } if built != fitted)
        ),
        "{why}"
    );
}

/// Every refusal at once: each source past its cap, and the memory.
#[test]
fn every_refusal_comes_at_once() {
    let why = refused(fitted(4, 9, 2, 1_000));
    let each = why.each();
    for source in [0, 1] {
        assert!(
            each.iter()
                .any(|w| matches!(w, Inadmissible::Sockets { source: s, needed: 9, cap: 4 } if *s == source)),
            "source {source}: {why}"
        );
    }
    assert!(
        each.iter().any(|w| matches!(w, Inadmissible::Memory(_))),
        "{why}"
    );
    assert!(
        !each.iter().any(|w| matches!(w, Inadmissible::All(_))),
        "nested: {why}"
    );
}

/// A fit refused at a cap a driver sizes at its most — its runtime's events
/// some tens of gigabytes — builds nothing: what it held is its terms, not a
/// runtime; it names the runtime's term as the one that binds and a least
/// memory past the stated.
#[test]
fn a_refused_fit_builds_nothing() {
    let cap = usize::try_from(i32::MAX - 2).unwrap();
    let mut result = None;
    let held = peak_of(|| result = Some(fitted(cap, 1, 1, 1 << 30)));
    let why = refused(result.unwrap());
    let memory = why
        .each()
        .iter()
        .find_map(|w| match w {
            Inadmissible::Memory(r) => Some(r.clone()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no memory refusal: {why}"));
    let binding = memory
        .unfit
        .binding
        .and_then(|i| memory.unfit.terms.get(i))
        .map(|t| t.part);
    assert!(
        matches!(
            binding,
            Some(Part::Runtime | Part::Connections | Part::Answers)
        ),
        "{binding:?}"
    );
    assert!(memory.unfit.least.is_some_and(|l| l > 1 << 30));
    assert!(held < 1 << 20, "{held} B held by a refused fit");
}

/// The least memory a refusal names admits; a byte less does not.
#[test]
fn the_least_memory_admits_and_a_byte_less_does_not() {
    // A cap whose sockets and answers make the run's phase the largest.
    let why = refused(fitted(256, 1, 1, 1_000));
    let least = why
        .each()
        .iter()
        .find_map(|w| match w {
            Inadmissible::Memory(r) => r.unfit.least,
            _ => None,
        })
        .unwrap_or_else(|| panic!("no least memory: {why}"));
    assert!(fitted(256, 1, 1, least).is_ok());
    let why = refused(fitted(256, 1, 1, least - 1));
    assert!(
        why.each()
            .iter()
            .any(|w| matches!(w, Inadmissible::Memory(_))),
        "{why}"
    );
    assert!(
        why.to_string().contains("remedies: memory at least"),
        "{why}"
    );
    // The journal's lines behind, the library's knob, lowered alone admits it.
    let behind = why.each().iter().find_map(|w| match w {
        Inadmissible::Memory(r) => r.remedies.remedies.iter().find_map(|m| match m {
            ascetic_ddd_load_run::memory::Remedy::Behind { at_most } => Some(*at_most),
            _ => None,
        }),
        _ => None,
    });
    assert!(behind.is_some_and(|b| b < 64), "{behind:?}: {why}");
}

/// K2b-7e-2b-3d-2: the cgroup tree's limits walked from the process's own up
/// to the root — the least `memory.max` and `memory.high`, each with its
/// depth; `max` and a missing file no limit.
#[test]
fn the_limit_is_the_least_over_the_tree() {
    let root = tempfile::tempdir().unwrap();
    let leaf = root.path().join("a/b/c");
    std::fs::create_dir_all(&leaf).unwrap();
    let write = |dir: &str, file: &str, v: &str| {
        std::fs::write(root.path().join(dir).join(file), v).unwrap();
    };
    // Every level `max`, the root's files missing: no limit.
    for dir in ["a", "a/b", "a/b/c"] {
        write(dir, "memory.max", "max\n");
        write(dir, "memory.high", "max\n");
    }
    let read = || Limit::read_from(root.path(), "/a/b/c");
    assert_eq!((read().max(), read().high()), (None, None));
    assert_eq!(
        read().origin(),
        ascetic_ddd_load_run::live::LimitOrigin::Tree
    );
    // The first finite above the leaf, two up.
    write("a", "memory.max", "5000\n");
    assert_eq!(read().max(), Some((5000, 2)));
    // A lesser one nearer: the least, and its depth.
    write("a/b", "memory.max", "3000\n");
    assert_eq!(read().max(), Some((3000, 1)));
    // The leaf's own, lesser still.
    write("a/b/c", "memory.max", "1000\n");
    assert_eq!(read().max(), Some((1000, 0)));
    // A greater one at the root changes nothing.
    std::fs::write(root.path().join("memory.max"), "9000\n").unwrap();
    assert_eq!(read().max(), Some((1000, 0)));
    // `memory.high` apart; the least of both is the lesser.
    write("a/b", "memory.high", "500\n");
    assert_eq!(read().high(), Some((500, 1)));
    assert_eq!(read().least(), Some((500, 1, "memory.high")));
}

/// The memory stated within the tree's least limit admitted, a byte past it
/// refused, naming the file and its depth.
#[test]
fn a_memory_past_the_limit_is_refused() {
    let fit_at = |stated: u64, limit: Limit| {
        let mut header = fixture::header();
        header.profile.sources = 1;
        fit::<RealClock, _>(
            Plan::assumed(Arc::new(header), Peaks::default()),
            0,
            Arc::new(Capped { cap: 8, needed: 1 }),
            Journal::new(64, 0),
            Memory::declared(stated),
            limit,
        )
    };
    let at = 1u64 << 32;
    assert!(fit_at(at, Limit::assumed(Some((at, 0)), None)).is_ok());
    let why = refused(fit_at(at + 1, Limit::assumed(Some((at, 0)), None)));
    assert!(
        why.each().contains(&Inadmissible::Limit {
            stated: at + 1,
            limit: at,
            file: "memory.max",
            depth: 0,
        }),
        "{why}"
    );
    // The scope rule: the pool's count unheld until the read gate is held by
    // the process's own cgroup — none refused, one up refused, its own
    // admitted.
    for limit in [
        Limit::assumed(None, None),
        Limit::assumed(Some((1 << 40, 1)), None),
    ] {
        let why = refused(fit_at(at, limit));
        assert!(
            why.each()
                .contains(&Inadmissible::Scope { max: limit.max() }),
            "{why}"
        );
        assert!(why.to_string().contains("MemoryMax"), "{why}");
    }
    assert!(fit_at(at, Limit::assumed(Some((1 << 40, 0)), None)).is_ok());
}

/// A carrier with a knob: `ports` in total, each source the whole of a total.
struct Knobbed {
    ports: usize,
    needed: u64,
}

impl<C: Clock> Carrier<C> for Knobbed {
    fn carries(&self, _: &Header) -> Result<(), String> {
        Ok(())
    }

    fn needs(&self, _: &Peaks, _: u32) -> Result<Needs, String> {
        Ok(Needs::Sockets {
            needed: self.needed,
            cap: self.ports,
            costs: Costs {
                connection: 4_096,
                io: 0,
                answer: 65_536,
            },
        })
    }

    fn source_costs(&self, _: &Peaks, _: u32) -> Result<SourceCosts, String> {
        Ok(SourceCosts::NOTHING)
    }

    fn attempts(&self, _: &Peaks) -> Result<AttemptCosts, Unstated> {
        Ok(AttemptCosts::NOTHING)
    }

    fn gate_bytes(&self) -> GateBytes {
        GateBytes::NONE
    }

    fn ports(&self) -> Option<usize> {
        Some(self.ports)
    }

    fn caps_at(&self, ports: usize, sources: u32) -> Option<Vec<usize>> {
        Some(vec![ports; sources as usize])
    }

    fn probe(
        &self,
        _: u32,
        _: Arc<C>,
        _: u64,
    ) -> impl std::future::Future<Output = Vec<Unprobed>> + Send + 'static {
        std::future::ready(Vec::new())
    }

    fn carry(&self, _: Attempt<C>) -> impl std::future::Future<Output = ()> + Send + 'static {
        std::future::ready(())
    }
}

/// The carrier's knob a remedy (K2b-7e-2b-3d-2): its ports at most a total
/// no less than the plan needs of each source; refitted at it, admitted.
#[test]
fn the_carrier_s_ports_are_a_remedy() {
    let fit_at = |ports: usize, memory: u64| {
        let mut header = fixture::header();
        header.profile.sources = 2;
        fit::<RealClock, _>(
            Plan::assumed(Arc::new(header), Peaks::default()),
            0,
            Arc::new(Knobbed { ports, needed: 16 }),
            Journal::new(64, 0),
            Memory::declared(memory),
            Limit::assumed(Some((PLENTY, 0)), None),
        )
    };
    // Its least memory at 1,024 ports, and half way to the needed 16's.
    let least = |ports| {
        refused(fit_at(ports, 1_000))
            .each()
            .iter()
            .find_map(|w| match w {
                Inadmissible::Memory(r) => r.unfit.least,
                _ => None,
            })
            .unwrap()
    };
    let memory = (least(1_024) + least(16)) / 2;
    let why = refused(fit_at(1_024, memory));
    let ports = why.each().iter().find_map(|w| match w {
        Inadmissible::Memory(r) => r.remedies.remedies.iter().find_map(|m| match m {
            ascetic_ddd_load_run::memory::Remedy::Ports { at_most } => Some(*at_most),
            _ => None,
        }),
        _ => None,
    });
    let ports = ports.unwrap_or_else(|| panic!("no ports remedy: {why}"));
    // The least memory at the needed sockets: the floor's, 16 ports.
    let at_needed = why.each().iter().find_map(|w| match w {
        Inadmissible::Memory(r) => r.remedies.least_at_needed,
        _ => None,
    });
    assert_eq!(at_needed, Some(least(16)), "{why}");
    assert!((16..1_024).contains(&ports), "{ports}");
    assert!(fit_at(ports, memory).is_ok(), "refitted at {ports} ports");
    assert!(fit_at(ports + 1, memory).is_err(), "one past {ports} ports");
}

/// A carrier whose egress divides its own total otherwise than its needs
/// state the caps: refused — a remedy would price caps no build makes.
#[test]
fn a_carrier_s_map_is_its_needs_at_its_own_total() {
    struct Askew(Knobbed);
    impl<C: Clock> Carrier<C> for Askew {
        fn carries(&self, h: &Header) -> Result<(), String> {
            Carrier::<C>::carries(&self.0, h)
        }
        fn needs(&self, p: &Peaks, s: u32) -> Result<Needs, String> {
            Carrier::<C>::needs(&self.0, p, s)
        }
        fn source_costs(&self, p: &Peaks, s: u32) -> Result<SourceCosts, String> {
            Carrier::<C>::source_costs(&self.0, p, s)
        }
        fn attempts(&self, p: &Peaks) -> Result<AttemptCosts, Unstated> {
            Carrier::<C>::attempts(&self.0, p)
        }
        fn gate_bytes(&self) -> GateBytes {
            GateBytes::NONE
        }
        fn ports(&self) -> Option<usize> {
            Some(self.0.ports)
        }
        // Half of what its needs give each source.
        fn caps_at(&self, ports: usize, sources: u32) -> Option<Vec<usize>> {
            Some(vec![ports / 2; sources as usize])
        }
        fn probe(
            &self,
            _: u32,
            _: Arc<C>,
            _: u64,
        ) -> impl std::future::Future<Output = Vec<Unprobed>> + Send + 'static {
            std::future::ready(Vec::new())
        }
        fn carry(&self, _: Attempt<C>) -> impl std::future::Future<Output = ()> + Send + 'static {
            std::future::ready(())
        }
    }
    let mut header = fixture::header();
    header.profile.sources = 1;
    let why = refused(fit::<RealClock, _>(
        Plan::assumed(Arc::new(header), Peaks::default()),
        0,
        Arc::new(Askew(Knobbed {
            ports: 64,
            needed: 1,
        })),
        Journal::new(64, 0),
        Memory::declared(PLENTY),
        Limit::assumed(Some((PLENTY, 0)), None),
    ));
    assert!(
        why.each()
            .iter()
            .any(|w| matches!(w, Inadmissible::Carrier(t) if t.contains("divides its 64 ports"))),
        "{why}"
    );
}

/// K2b-7e-2b-3d-3a: κ by one call — the fit's and the header's `catch_up`
/// the same at κ 1 and 999 —, and a reserve whose κ is 1000 refused by name.
#[test]
fn kappa_is_one_call_and_refused_by_name() {
    use ascetic_ddd_command_log::fills::Namespace;
    use ascetic_ddd_load_run::stages::{Identity, journal_header};
    let fitted_at = |reserve: u32| {
        let mut header = fixture::header();
        header.profile.sources = 1;
        header.profile.reserve = reserve;
        fit::<RealClock, _>(
            Plan::assumed(Arc::new(header), Peaks::default()),
            0,
            Arc::new(Capped { cap: 8, needed: 1 }),
            Journal::new(64, 0),
            Memory::declared(PLENTY),
            Limit::assumed(Some((PLENTY, 0)), None),
        )
    };
    for kappa in [1u32, 999] {
        let fitted = fitted_at(2 * kappa).unwrap();
        let live = Live::fitted(&fitted).unwrap();
        let admitted = admit(fitted, live.runtimes()).unwrap();
        let header = journal_header(
            &admitted,
            Identity {
                source: "test".into(),
                log: "ab".repeat(32),
                validator: "v".into(),
                namespace: Namespace(0),
            },
            None,
            vec![],
            None,
        );
        assert_eq!(header.catch_up, kappa, "the header's catch-up at κ {kappa}");
        assert_eq!(
            ascetic_ddd_load_run::run::kappa(&admitted.header().profile),
            Some(u64::from(kappa))
        );
    }
    let why = refused(fitted_at(2_000));
    assert!(
        why.each()
            .contains(&Inadmissible::Reserve { reserve: 2_000 }),
        "{why}"
    );
}

/// A limit's origin journaled apart: read, a test's tree, a test's
/// assumption; the parts no enforcer holds yet, the gate's, each once in
/// their order.
#[test]
fn a_limit_s_origin_and_the_unheld_parts() {
    use ascetic_ddd_load_run::live::LimitOrigin;
    use ascetic_ddd_load_run::memory::{Held, Part};
    if let Ok(read) = Limit::read() {
        assert_eq!(read.origin(), LimitOrigin::Read);
    }
    assert_eq!(Limit::assumed(None, None).origin(), LimitOrigin::Assumed);
    let fitted = fitted(8, 1, 1, PLENTY).unwrap();
    let unheld = &fitted.fit().unheld;
    let gate: Vec<Part> = fitted
        .fit()
        .terms
        .iter()
        .filter(|t| matches!(t.held, Held::Gate { built: None }))
        .map(|t| t.part)
        .collect();
    assert!(!unheld.is_empty());
    assert_eq!(unheld, &gate);
}

/// K2b-7e-2b-3d-3a: each part's count is its enforcer's — a source's cap,
/// its runtime's events, the journal's lines behind and the two beside, the
/// cells held, the steps pending, the barrier keys, the late steps (here not
/// the pending) —, its costs the library's model; the journal's output the
/// host's term apart; no reader, neither its records nor its decoder; a
/// host's carrier's connections, answers and probes the host's word.
#[test]
fn each_part_s_count_is_its_enforcer_s() {
    use ascetic_ddd_command_log::live::DECODER;
    use ascetic_ddd_command_log::shape::{ARC, Shape};
    use ascetic_ddd_load_run::carrier::StatedBy;
    use ascetic_ddd_load_run::memory::Term;
    use ascetic_ddd_load_run::run::{ATTEMPT, DRIVER_EVENTS, EVENT, RUNTIME};
    use ascetic_ddd_scheduler::fits::{BARRIER, ENDED_CELL, LIVE_CELL};
    let peaks = Peaks {
        held: 5,
        pending: 3,
        late: 7,
        late_shape: Shape {
            records: 7,
            ..Shape::default()
        },
        barriers: 2,
        barrier_bytes: 40,
        ..Peaks::default()
    };
    assert_ne!(peaks.late, peaks.pending);
    let mut header = fixture::header();
    header.profile.sources = 2;
    let fitted = fit::<RealClock, _>(
        Plan::assumed(Arc::new(header), peaks),
        0,
        Arc::new(Capped { cap: 8, needed: 1 }),
        Journal::new(64, 123),
        Memory::declared(PLENTY),
        Limit::assumed(Some((PLENTY, 0)), None),
    )
    .unwrap();
    let terms = &fitted.fit().terms;
    let each =
        |part: Part| -> Vec<Term> { terms.iter().filter(|t| t.part == part).copied().collect() };
    let one = |part: Part| -> (u64, u64, u64) {
        let v = each(part);
        assert_eq!(v.len(), 1, "{part:?}");
        (v[0].count, v[0].cost, v[0].rest)
    };
    let sockets = [
        (Part::Connections, 4_096),
        (Part::Io, 0),
        (Part::Answers, 65_536),
        (Part::Probes, 4_096),
        (Part::ProbesIo, 0),
    ];
    for (part, cost) in sockets {
        let v = each(part);
        assert_eq!(v.len(), 2, "{part:?}");
        assert!(v.iter().all(|t| (t.count, t.cost) == (8, cost)), "{v:?}");
        assert!(v.iter().all(|t| t.stated == StatedBy::Host), "{v:?}");
    }
    let runtimes = each(Part::Runtime);
    assert_eq!(runtimes.len(), 2);
    assert!(
        runtimes
            .iter()
            .all(|t| (t.count, Some(t.cost), t.rest) == (8 + DRIVER_EVENTS as u64, EVENT, RUNTIME)),
        "{runtimes:?}"
    );
    assert_eq!(one(Part::Reader), (0, 0, 0));
    assert_eq!(one(Part::Decoder), (0, DECODER, 0));
    assert_eq!(one(Part::Journal).0, 64 + 2);
    assert_eq!(one(Part::JournalOut), (1, 123, 0));
    assert_eq!(one(Part::Cells), (5, ENDED_CELL, 0));
    assert_eq!(one(Part::Pending), (3, LIVE_CELL - ENDED_CELL, 0));
    assert_eq!(one(Part::Barriers), (2, BARRIER, 40));
    assert_eq!(one(Part::Records), (7, ARC as u64, 0));
    assert_eq!(one(Part::ShellAttempts), (7, ATTEMPT, 0));
    assert_eq!(one(Part::CarrierAttempts).0, 7);
    assert_eq!(one(Part::Decorations).0, 7);
}

/// K2b-7e-2b-3d-3a: a carrier's attempts priced by their count — each one's
/// part a late step, the classes' bytes and holes beside — and their
/// decorations apart, the decorator's: together the price, every class
/// counted once.
#[test]
fn a_carrier_s_attempts_are_its_price_by_their_count() {
    use ascetic_ddd_command_log::shape::Shape;
    use ascetic_ddd_load_run::carrier::StatedBy;
    let costs = AttemptCosts {
        each: 3,
        target: 5,
        target_hole: 7,
        header: 11,
        header_hole: 13,
        field: 17,
        body: 19,
        body_hole: 23,
        hole: 29,
        hole_bytes: 31,
        decorations: 37,
        stated_by: StatedBy::Library,
    };
    let mut s = Shape {
        records: 41,
        target: 43,
        target_holes: 47,
        headers: 53,
        header_holes: 59,
        body: 61,
        body_holes: 67,
        holes: 71,
        hole_bytes: 73,
        ..Shape::default()
    };
    s.fields.elements = 79;
    let (each, rest) = costs.priced(&s);
    assert_eq!(each, 3);
    assert_eq!(
        s.records * (each + costs.decorations) + rest,
        costs.price(&s)
    );
    assert_eq!(
        rest,
        5 * 43 + 7 * 47 + 11 * 53 + 13 * 59 + 17 * 79 + 19 * 61 + 23 * 67 + 29 * 71 + 31 * 73
    );
}

/// K2b-7e-2b-3d-3b-2: admission's evidence is its fit journaled — the memory
/// stated, the limit and its origin, the library's constants, every term by
/// its count, cost and rest with its phase, holder, stater and basis, the
/// unheld parts, the budget, the pool, the need and the slack —, and a header
/// carrying it reads back as itself.
#[test]
fn the_admission_is_the_fit_journaled() {
    use ascetic_ddd_command_log::fills::Namespace;
    use ascetic_ddd_load_run::memory::Model;
    use ascetic_ddd_load_run::stages::{Identity, journal_header};
    use ascetic_ddd_scheduler::journal::{
        self, Bound, LimitOrigin, LimitRead, MemoryOrigin, Reader, Writer,
    };
    let mut header = fixture::header();
    header.profile.sources = 2;
    let fitted = fit::<RealClock, _>(
        Plan::assumed(Arc::new(header), Peaks::default()),
        0,
        Arc::new(Capped { cap: 8, needed: 1 }),
        Journal::new(64, 123),
        Memory::declared(PLENTY),
        Limit::assumed(Some((PLENTY, 0)), Some((PLENTY + 1, 3))),
    )
    .unwrap();
    let fit = fitted.fit().clone();
    let live = Live::fitted(&fitted).unwrap();
    let admitted = admit(fitted, live.runtimes()).unwrap();
    let memory = &admitted.admission().memory;
    assert_eq!(
        (memory.stated, memory.origin),
        (PLENTY, MemoryOrigin::Declared)
    );
    assert_eq!(
        memory.limit,
        LimitRead {
            max: Some(Bound {
                bytes: PLENTY,
                depth: 0
            }),
            high: Some(Bound {
                bytes: PLENTY + 1,
                depth: 3
            }),
            origin: LimitOrigin::Assumed,
        }
    );
    assert_eq!(memory.model, journal::Model::from(Model::LIBRARY));
    assert_eq!(
        (memory.need, memory.slack, memory.budget, memory.pool),
        (fit.need, fit.slack, fit.budget, fit.pool)
    );
    assert_ne!(fit.need, fit.slack);
    assert_eq!(memory.terms.len(), fit.terms.len());
    for (j, t) in memory.terms.iter().zip(&fit.terms) {
        assert_eq!(
            (j.source, j.count, j.cost, j.rest, j.bytes),
            (t.source, t.count, t.cost, t.rest, t.bytes),
            "{t:?}"
        );
        assert_eq!(j.part, t.part.into());
        assert_eq!(j.phase, t.phase.into());
        assert_eq!(j.held, t.held.into());
        assert_eq!(j.stated, t.stated.into());
        assert_eq!(j.basis, t.basis.into());
    }
    assert!(
        memory
            .terms
            .iter()
            .any(|t| t.held == journal::Held::Gate { built: false })
    );
    assert!(!memory.unheld.is_empty());
    assert_eq!(
        memory.unheld,
        fit.unheld.iter().map(|p| (*p).into()).collect::<Vec<_>>()
    );
    let header = journal_header(
        &admitted,
        Identity {
            source: "test".into(),
            log: "ab".repeat(32),
            validator: "v".into(),
            namespace: Namespace(0),
        },
        None,
        vec![],
        None,
    );
    let bytes = Writer::new(Vec::new(), header.clone())
        .unwrap()
        .finish()
        .unwrap()
        .0;
    let read = Reader::new(std::io::Cursor::new(bytes)).unwrap();
    assert_eq!(*read.header(), header);
}

/// K2b-7e-2b-3d-3b-2: each of the fit's names is journaled as itself — its
/// own name in snake case, not another part's, phase's, holder's, stater's,
/// basis' or origin's.
#[test]
fn each_name_is_journaled_as_itself() {
    use ascetic_ddd_load_run::carrier::StatedBy;
    use ascetic_ddd_load_run::live::LimitOrigin;
    use ascetic_ddd_load_run::memory::{Basis, Held, Phase};
    use ascetic_ddd_scheduler::journal;
    fn snake(debug: &str) -> String {
        let mut out = String::new();
        for (i, c) in debug.chars().enumerate() {
            if c.is_ascii_uppercase() {
                if i > 0 {
                    out.push('_');
                }
                out.push(c.to_ascii_lowercase());
            } else {
                out.push(c);
            }
        }
        format!("{out:?}")
    }
    fn same<A: std::fmt::Debug>(a: A, journaled: serde_json::Result<String>) {
        assert_eq!(journaled.unwrap(), snake(&format!("{a:?}")));
    }
    let json = serde_json::to_string::<journal::Part>;
    for p in Part::ALL {
        same(p, json(&p.into()));
    }
    for p in [
        Phase::Fixed,
        Phase::Stages,
        Phase::Gates,
        Phase::Run,
        Phase::Pool,
    ] {
        same(p, serde_json::to_string(&journal::Phase::from(p)));
    }
    for h in [
        Held::Built,
        Held::Channel,
        Held::Cap,
        Held::Once,
        Held::Host,
    ] {
        same(h, serde_json::to_string(&journal::Held::from(h)));
    }
    assert_eq!(
        journal::Held::from(Held::Gate { built: None }),
        journal::Held::Gate { built: false }
    );
    for b in [Basis::Heap, Basis::Outside] {
        same(b, serde_json::to_string(&journal::Basis::from(b)));
    }
    for s in [StatedBy::Library, StatedBy::Host] {
        same(s, serde_json::to_string(&journal::StatedBy::from(s)));
    }
    let origin = |l: Limit| journal::LimitRead::from(l).origin;
    let dir = tempfile::tempdir().unwrap();
    for (limit, named) in [
        (Limit::assumed(None, None), LimitOrigin::Assumed),
        (Limit::read_from(dir.path(), "/"), LimitOrigin::Tree),
    ] {
        assert_eq!(limit.origin(), named);
        same(named, serde_json::to_string(&origin(limit)));
    }
    if let Ok(read) = Limit::read() {
        same(LimitOrigin::Read, serde_json::to_string(&origin(read)));
    }
}

/// K2b-7e-2b-3d-3b-3: admission checks its own evidence by the fold's rules
/// before it journals it — a fit whose need is off by a byte, or whose
/// unheld parts are lost, is refused by the rule it breaks, a defect of
/// admission's.
#[test]
fn admission_refuses_evidence_that_disagrees_with_itself() {
    use ascetic_ddd_load_run::memory::Model;
    use ascetic_ddd_load_run::run::admission;
    let fitted = fitted(8, 1, 2, PLENTY).unwrap();
    let fit = fitted.fit().clone();
    let live = Live::fitted(&fitted).unwrap();
    let admitted = admit(fitted, live.runtimes()).unwrap();
    let a = admitted.admission();
    let journal = |fit: &ascetic_ddd_load_run::memory::Fit| {
        admission(
            fit,
            Memory::declared(PLENTY),
            Limit::assumed(Some((PLENTY, 0)), None),
            Model::LIBRARY,
            a.sources.clone(),
            a.descriptors.clone(),
            a.assumed,
            admitted.header().profile.reserve / 2,
        )
    };
    assert_eq!(journal(&fit).as_ref(), Ok(a));
    let mut off = fit.clone();
    off.need += 1;
    assert_eq!(
        journal(&off).err(),
        Some(Inadmissible::Evidence {
            why: "admission_need"
        })
    );
    let mut lost = fit;
    lost.unheld.clear();
    assert_eq!(
        journal(&lost).err(),
        Some(Inadmissible::Evidence {
            why: "admission_unheld"
        })
    );
}

/// A stand of one endpoint and no gate: what a REST carrier routes to, for a
/// fit — no run is made.
struct Routed;

impl ascetic_ddd_load_run::stand::Stand for Routed {
    fn route(
        &self,
        _: &ascetic_ddd_command_log::record::Record,
        _: &ascetic_ddd_command_log::record::Rest,
    ) -> Result<ascetic_ddd_load_run::stand::Route, String> {
        Ok(ascetic_ddd_load_run::stand::Route {
            authority: "stand.test".to_owned(),
            endpoint: "gateway".to_owned(),
        })
    }

    fn endpoints(&self, _: &Header, _: u32) -> Vec<String> {
        vec!["gateway".to_owned()]
    }

    fn gate(
        &self,
        _: usize,
        _: ascetic_ddd_load_run::stand::Opening<'_>,
    ) -> impl std::future::Future<
        Output = Option<Result<String, ascetic_ddd_load_run::stand::Refused>>,
    > + Send {
        std::future::ready(None)
    }

    fn gate_bytes(&self) -> GateBytes {
        GateBytes::NONE
    }
}

/// An egress of `ports` a source that connects nowhere: for a fit.
struct Nowhere {
    ports: usize,
}

impl ascetic_ddd_load_run::stand::Egress for Nowhere {
    type Io = tokio::io::DuplexStream;

    fn ports(&self, _: u32) -> usize {
        self.ports
    }

    fn io_bytes(&self) -> u64 {
        2_048
    }

    fn caps(&self, ports: usize, sources: u32) -> Vec<usize> {
        vec![ports; sources as usize]
    }

    fn total(&self) -> usize {
        self.ports
    }

    fn admits(&self, _: u32) -> Result<(), ascetic_ddd_load_run::stand::Unfit> {
        Ok(())
    }

    fn reaches(&self, _: u32, _: &str) -> Result<(), ascetic_ddd_load_run::stand::Unfit> {
        Ok(())
    }

    fn connect(
        &self,
        _: u32,
        _: &ascetic_ddd_load_run::stand::Route,
    ) -> impl std::future::Future<Output = Result<Self::Io, ascetic_ddd_load_run::stand::Connect>> + Send
    {
        std::future::ready(Err(ascetic_ddd_load_run::stand::Connect::Harness {
            cannot: "egress.nowhere",
            detail: String::new(),
        }))
    }
}

/// K2b-7e-2b-3d-3b-4b: the headers admission journals, pinned — of a plan
/// with no reader carried by a host's carrier over two sources, of nine
/// sources, of one REST source carried by the library's carrier —, each
/// rechecked by the fold and settled at every change to the section by its
/// diff. The identity is fixed and the descriptors, the process's as read,
/// are replaced by fixed ones; the rest is admission's own.
#[test]
fn admission_s_headers_are_pinned() {
    for (name, bytes) in admitted_headers() {
        pinned::check(name, &bytes);
    }
}

/// Writes `golden/admitted/*.jsonl` from the headers as admission makes them
/// now. Run only on purpose, after settling.
#[test]
#[ignore = "regenerates golden/admitted"]
fn regenerate_admitted() {
    for (name, bytes) in admitted_headers() {
        pinned::write(name, &bytes);
    }
}

fn admitted_headers() -> Vec<(&'static str, Vec<u8>)> {
    use ascetic_ddd_command_log::render::RunSecret;
    use ascetic_ddd_load_run::rest::{Config, RestCarrier};
    let capped = |sources: u32| {
        let mut header = fixture::header();
        header.profile.sources = sources;
        header.profile.reserve = 20;
        // A pool to price at the catch-up: cells held and pending, keys, the
        // late steps.
        let peaks = Peaks {
            held: 5,
            pending: 3,
            late: 7,
            late_shape: ascetic_ddd_command_log::shape::Shape {
                records: 7,
                ..Default::default()
            },
            barriers: 2,
            barrier_bytes: 40,
            ..Peaks::default()
        };
        fit::<RealClock, _>(
            Plan::assumed(Arc::new(header), peaks),
            0,
            Arc::new(Capped { cap: 8, needed: 1 }),
            Journal::new(64, 4_096),
            Memory::declared(PLENTY),
            Limit::assumed(Some((PLENTY, 0)), Some((PLENTY, 2))),
        )
        .unwrap()
    };
    let mut out = vec![
        ("no-reader-host-carrier", pinned::header(capped(2))),
        ("nine-sources", pinned::header(capped(9))),
    ];
    // The bus and host calls are K3's: a REST source's log carries neither.
    let mut header = fixture::header();
    header.profile.sources = 1;
    header.channels.clear();
    header.calls.clear();
    let header = Arc::new(header);
    let rest = fit::<RealClock, _>(
        Plan::assumed(header.clone(), Peaks::default()),
        0,
        Arc::new(RestCarrier::new(
            header,
            Routed,
            Nowhere { ports: 16 },
            Config::new(RunSecret::new([7; 32])),
        )),
        Journal::new(64, 0),
        Memory::declared(PLENTY),
        Limit::assumed(Some((PLENTY, 0)), None),
    )
    .unwrap();
    out.push(("one-rest-source", pinned::header(rest)));
    out
}

/// K2b-7e-2b-3d-3b-4c: the evidence's own part prices its copies — the heap
/// of one copy, as a clone of admission's allocates it, its cost exactly,
/// at one source and at nine; two copies at once.
#[test]
fn the_evidence_prices_its_copies() {
    for sources in [1, 9] {
        let fitted = fitted(8, 1, sources, PLENTY).unwrap();
        let live = Live::fitted(&fitted).unwrap();
        let admitted = admit(fitted, live.runtimes()).unwrap();
        let a = admitted.admission();
        let evidence = a
            .memory
            .terms
            .iter()
            .find(|t| t.part == ascetic_ddd_scheduler::journal::Part::Evidence)
            .copied()
            .unwrap();
        assert_eq!(a.sources.len(), sources as usize, "a row a source");
        // Admission's own copy holds no more than a clone: each vector's
        // capacity its length.
        let m = &a.memory;
        assert_eq!(
            (
                a.sources.capacity(),
                m.terms.capacity(),
                m.unheld.capacity()
            ),
            (a.sources.len(), m.terms.len(), m.unheld.len())
        );
        let copy = peak_of(|| a.clone());
        assert!(copy > 0);
        assert_eq!(
            (evidence.count, evidence.cost, evidence.rest),
            (2, copy, 0),
            "{sources} sources"
        );
    }
}
