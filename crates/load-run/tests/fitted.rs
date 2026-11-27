//! K2b-7e-2b-3d-1 of ADR-0047 (decision 22): admission binds what it prices.
//! A fit made of the plan, its carrier, its journal and its read-ahead is
//! the only way to the runtimes and to `admit`, which takes nothing else:
//! what was fitted runs; every refusal comes at once; a refused fit builds
//! nothing; the least memory it names admits and a byte less does not. A
//! binary of its own: its allocator counts each thread's bytes.

#[path = "../../command-log/tests/fixture/mod.rs"]
#[allow(unused_imports, clippy::disallowed_methods, clippy::disallowed_types)]
mod fixture;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

use ascetic_ddd_command_log::record::Header;
use ascetic_ddd_command_log::validate::Peaks;
use ascetic_ddd_load_run::carrier::{
    AttemptCosts, Carrier, Costs, Needs, SourceCosts, Unprobed, Unstated,
};
use ascetic_ddd_load_run::clock::{Clock, RealClock};
use ascetic_ddd_load_run::live::Live;
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
