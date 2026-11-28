//! K2b-7e-2b-3e-2a of ADR-0047 (decision 22): the run's memory sampled by
//! its journal's writer. A sample after the header and before the trailer;
//! one due each period on a silent channel and on a channel never empty, the
//! due instant checked after every line; a writer held past several dues
//! samples once on its return, the dues it passed counted, none caught up.
//! The readers over a test's trees: every field, a field a kernel lacks
//! none alone, a group unread by its anchor, a hierarchy not unified, the
//! pressure's figures, the binding group's events, a group of ten thousand
//! processes counted through the buffer. The live counts read as one
//! instant's. The sampler's bytes priced in the journal's term.

use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use ascetic_ddd_load_run::clock::RealClock;
use ascetic_ddd_load_run::sample::{
    FLOOR, Gauges, Heap, Probe, Roots, SAMPLE, Sampler, Stated, Turn,
};
use ascetic_ddd_load_run::sink::{Journal, JournalSink, Written};
use ascetic_ddd_scheduler::journal::{self, Line, Reader, Reading, Released, SampleWhy, Unread};

/// A test's tree: the process's status and cgroup line, its group's files,
/// and an ancestor two above with its own events.
fn tree(dir: &Path, cgroup_line: &str, files: &[(&str, &str)]) -> Roots {
    let proc = dir.join("proc");
    let cgroup = dir.join("cgroup");
    std::fs::create_dir_all(proc.join("self")).unwrap();
    std::fs::create_dir_all(proc.join("pressure")).unwrap();
    std::fs::write(
        proc.join("self/status"),
        "Name:\tx\nVmHWM:\t  2048 kB\nVmRSS:\t  1024 kB\nRssAnon:\t   512 kB\nRssFile:\t   256 kB\nRssShmem:\t     4 kB\nVmSwap:\t     0 kB\nThreads:\t7\n",
    )
    .unwrap();
    std::fs::write(proc.join("self/cgroup"), cgroup_line).unwrap();
    std::fs::write(
        proc.join("pressure/memory"),
        "some avg10=1.23 avg60=0.00 avg300=0.00 total=456\nfull avg10=0.50 avg60=0.00 avg300=0.00 total=78\n",
    )
    .unwrap();
    let own = cgroup.join("a/b/run.scope");
    std::fs::create_dir_all(&own).unwrap();
    std::fs::write(
        cgroup.join("a/memory.events"),
        "low 0\nhigh 9\nmax 8\noom 1\noom_kill 1\noom_group_kill 0\n",
    )
    .unwrap();
    for (name, body) in files {
        std::fs::write(own.join(name), body).unwrap();
    }
    Roots { proc, cgroup }
}

const FULL: &[(&str, &str)] = &[
    ("memory.current", "4096\n"),
    ("memory.peak", "8192\n"),
    ("memory.swap.current", "0\n"),
    (
        "memory.stat",
        "anon 1000\nfile 2000\nkernel_stack 30\npagetables 40\nsock 50\nshmem 0\nanon_thp 0\nslab 60\n",
    ),
    (
        "memory.events",
        "low 0\nhigh 1\nmax 2\noom 0\noom_kill 0\noom_group_kill 0\n",
    ),
    (
        "memory.pressure",
        "some avg10=0.00 avg60=0.00 avg300=0.00 total=10\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=5\n",
    ),
    ("cgroup.procs", "1\n2\n"),
];

fn sampler(roots: Roots, gauges: Arc<Gauges>, period: u64) -> Sampler {
    Sampler::new(
        Arc::new(RealClock::start(0).unwrap()),
        gauges,
        roots,
        Some(2),
        period,
        7,
    )
}

/// The readers over a test's trees.
#[test]
fn the_readers_read_what_is_there_and_say_why_not() {
    let dir = tempfile::tempdir().unwrap();
    let roots = tree(dir.path(), "0::/a/b/run.scope\n", FULL);
    let s = sampler(roots, Arc::default(), FLOOR).sample(SampleWhy::Baseline);
    let Reading::Read(p) = s.process else {
        panic!("{:?}", s.process)
    };
    assert_eq!(
        (p.rss, p.rss_anon, p.hwm, p.threads),
        (1024 * 1024, Some(512 * 1024), Some(2048 * 1024), Some(7))
    );
    let Reading::Read(c) = s.cgroup else {
        panic!("{:?}", s.cgroup)
    };
    assert_eq!(
        (c.current, c.peak, c.anon, c.file, c.sock, c.slab, c.procs),
        (
            4096,
            Some(8192),
            Some(1000),
            Some(2000),
            Some(50),
            Some(60),
            Some(2)
        )
    );
    assert_eq!(c.events.map(|e| (e.high, e.max)), Some((1, 2)));
    let b = c.binding.unwrap();
    assert_eq!(
        (b.depth, b.events.map(|e| (e.high, e.oom_kill))),
        (2, Some((9, 1)))
    );
    let Reading::Read(h) = s.host else {
        panic!("{:?}", s.host)
    };
    assert_eq!(
        (h.some_avg10, h.some_total, h.full_avg10, h.full_total),
        (123, 456, 50, 78)
    );
    // A field a kernel lacks, none alone.
    let dir = tempfile::tempdir().unwrap();
    let older: Vec<_> = FULL
        .iter()
        .copied()
        .filter(|(n, _)| *n != "memory.peak")
        .collect();
    let s = sampler(
        tree(dir.path(), "0::/a/b/run.scope\n", &older),
        Arc::default(),
        FLOOR,
    )
    .sample(SampleWhy::Baseline);
    let Reading::Read(c) = s.cgroup else {
        panic!("{:?}", s.cgroup)
    };
    assert_eq!((c.peak, c.current, c.sock), (None, 4096, Some(50)));
    // The anchor absent: the group unread, by its reason.
    let dir = tempfile::tempdir().unwrap();
    let headless: Vec<_> = FULL
        .iter()
        .copied()
        .filter(|(n, _)| *n != "memory.current")
        .collect();
    let s = sampler(
        tree(dir.path(), "0::/a/b/run.scope\n", &headless),
        Arc::default(),
        FLOOR,
    )
    .sample(SampleWhy::Baseline);
    assert_eq!(s.cgroup, Reading::Unread(Unread::Absent));
    // No unified hierarchy.
    let dir = tempfile::tempdir().unwrap();
    let s = sampler(
        tree(dir.path(), "1:memory:/x\n", FULL),
        Arc::default(),
        FLOOR,
    )
    .sample(SampleWhy::Baseline);
    assert_eq!(s.cgroup, Reading::Unread(Unread::NoCgroupV2));
    // Ten thousand processes, counted as they stream through the buffer.
    let dir = tempfile::tempdir().unwrap();
    let many: String = (0..10_000).map(|i| format!("{i}\n")).collect();
    let mut files: Vec<_> = FULL
        .iter()
        .copied()
        .filter(|(n, _)| *n != "cgroup.procs")
        .collect();
    files.push(("cgroup.procs", &many));
    let s = sampler(
        tree(dir.path(), "0::/a/b/run.scope\n", &files),
        Arc::default(),
        FLOOR,
    )
    .sample(SampleWhy::Baseline);
    let Reading::Read(c) = s.cgroup else {
        panic!("{:?}", s.cgroup)
    };
    assert_eq!(c.procs, Some(10_000));
}

/// An output shared with the test, its writes held while `hold` is set,
/// each taking `slow`'s µs of its clock where it has one: an output slower
/// than its sender, whose channel then is never empty.
#[derive(Clone, Default)]
struct Out {
    bytes: Arc<Mutex<Vec<u8>>>,
    hold: Arc<AtomicBool>,
    slow: Option<(Arc<RealClock>, u64)>,
}

impl Write for Out {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        while self.hold.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        if let Some((clock, micros)) = &self.slow {
            for_micros(clock, *micros, std::thread::yield_now);
        }
        self.bytes.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn line(at: u64) -> Line {
    Line::Released(Released {
        at,
        line: 2,
        intended: at,
        released: at,
        deadline: at,
    })
}

/// The samples a finished journal holds.
fn samples(out: &Out) -> Vec<journal::Sample> {
    let bytes = out.bytes.lock().unwrap().clone();
    let r = Reader::new(std::io::Cursor::new(bytes)).unwrap();
    r.map(Result::unwrap)
        .filter_map(|l| match l {
            Line::Sample(s) => Some(*s),
            _ => None,
        })
        .collect()
}

/// `f` for `micros` of the real clock, in rounds.
fn for_micros(clock: &RealClock, micros: u64, mut f: impl FnMut()) {
    use ascetic_ddd_load_run::clock::Clock as _;
    let end = clock.now() + micros;
    while clock.now() < end {
        f();
    }
}

/// A sample after the header and before the trailer; on a silent channel,
/// one due each period.
#[test]
fn a_silent_channel_is_sampled_each_period() {
    let dir = tempfile::tempdir().unwrap();
    let roots = tree(dir.path(), "0::/a/b/run.scope\n", FULL);
    let out = Out::default();
    let s = sampler(roots, Arc::default(), FLOOR);
    let clock = s.clock().clone();
    let w = Written::sampled(out.clone(), journal::sample::header(), 4, s).unwrap();
    for_micros(&clock, 4 * FLOOR + FLOOR / 2, std::thread::yield_now);
    let until = {
        use ascetic_ddd_load_run::clock::Clock as _;
        clock.now()
    };
    w.finish().unwrap();
    let got = samples(&out);
    assert_eq!(got.first().map(|s| s.why), Some(SampleWhy::Baseline));
    assert_eq!(got.last().map(|s| s.why), Some(SampleWhy::Finish));
    assert_paced(&got, until);
}

/// The due samples before `until`: at least one, each at least a period
/// after the last — the period, or a hundred times a sample's cost where
/// that is longer (a loaded box's sample costs more), never sooner.
fn assert_paced(got: &[journal::Sample], until: u64) {
    let due: Vec<u64> = got
        .iter()
        .filter(|s| s.why == SampleWhy::Due)
        .map(|s| s.at)
        .collect();
    assert!(
        due.iter().any(|at| *at < until),
        "no due sample before {until}: {due:?}"
    );
    assert!(
        due.windows(2).all(|w| w[1] - w[0] >= FLOOR),
        "due samples sooner than a period apart: {due:?}"
    );
}

/// A channel never empty for several periods is sampled each period: the due
/// instant is checked after every line taken, not on a timeout alone.
#[test]
fn a_channel_never_empty_is_sampled_each_period() {
    let dir = tempfile::tempdir().unwrap();
    let roots = tree(dir.path(), "0::/a/b/run.scope\n", FULL);
    let s = sampler(roots, Arc::default(), FLOOR);
    let clock = s.clock().clone();
    let out = Out {
        slow: Some((clock.clone(), 200)),
        ..Out::default()
    };
    let mut w = Written::sampled(out.clone(), journal::sample::header(), 4, s).unwrap();
    let mut at = 0;
    for_micros(&clock, 4 * FLOOR + FLOOR / 2, || {
        at += 1;
        w.line(line(at)).unwrap();
    });
    // While the feeder sent, the channel was full: a due sample before its
    // last send is one taken after a line, not on a timeout.
    let until = {
        use ascetic_ddd_load_run::clock::Clock as _;
        clock.now()
    };
    w.finish().unwrap();
    let got = samples(&out);
    // Each line held 200 µs by the output: the writer the bottleneck, its
    // channel full whenever it takes the next.
    assert!(at > 500, "{at} lines: the channel was not kept full");
    assert_paced(&got, until);
}

/// A writer held past several dues samples once on its return, the dues it
/// passed counted; the next one a period after it, none caught up.
#[test]
fn a_writer_held_samples_once_on_its_return() {
    let dir = tempfile::tempdir().unwrap();
    let roots = tree(dir.path(), "0::/a/b/run.scope\n", FULL);
    let out = Out::default();
    let s = sampler(roots, Arc::default(), FLOOR);
    let clock = s.clock().clone();
    let mut w = Written::sampled(out.clone(), journal::sample::header(), 4, s).unwrap();
    out.hold.store(true, Ordering::Release);
    w.line(line(1)).unwrap();
    for_micros(&clock, 3 * FLOOR + FLOOR / 2, std::thread::yield_now);
    out.hold.store(false, Ordering::Release);
    for_micros(&clock, FLOOR / 2, std::thread::yield_now);
    w.finish().unwrap();
    let due: Vec<_> = samples(&out)
        .into_iter()
        .filter(|s| s.why == SampleWhy::Due)
        .collect();
    assert_eq!(due.len(), 1, "{due:?}");
    assert!(due[0].skipped >= 2, "{:?}", due[0]);
}

/// The live counts read as one instant's: a writer storing a turn whose
/// counts all equal it, a reader never seeing two turns mixed.
#[test]
fn the_live_counts_read_as_one_instant() {
    let gauges = Arc::new(Gauges::default());
    let done = Arc::new(AtomicBool::new(false));
    let writer = {
        let (g, done) = (gauges.clone(), done.clone());
        std::thread::spawn(move || {
            let mut n = 0u64;
            while !done.load(Ordering::Relaxed) {
                n += 1;
                g.store(Turn {
                    turn: n,
                    wakes: Some(n),
                    pending: n,
                    past_deadline: n,
                    alive: n,
                    sockets: Some(n),
                });
            }
            n
        })
    };
    let (torn, read) = (AtomicU64::new(0), AtomicU64::new(0));
    for _ in 0..200_000 {
        let Some(l) = gauges.try_load() else {
            continue;
        };
        read.fetch_add(1, Ordering::Relaxed);
        let all = [l.pending, l.past_deadline, l.alive];
        if all.iter().any(|v| *v != l.turn) || (l.turn != 0 && l.wakes != Some(l.turn)) {
            torn.fetch_add(1, Ordering::Relaxed);
        }
    }
    done.store(true, Ordering::Relaxed);
    let stores = writer.join().unwrap();
    assert!(stores > 1_000, "{stores}");
    assert_eq!(torn.load(Ordering::Relaxed), 0);
    assert!(read.load(Ordering::Relaxed) > 1_000, "{read:?}");
}

/// The sampler's bytes priced in the journal's term, only where it samples.
#[test]
fn the_sampler_is_priced_where_it_samples() {
    let record = ascetic_ddd_command_log::shape::Shape::default();
    let plain = Journal::new(64, 0).unsampled();
    let sampled = plain.clone().sampling(1);
    assert_eq!(
        Journal::new(64, 0).period(),
        Some(ascetic_ddd_load_run::sink::PERIOD)
    );
    assert_eq!(sampled.period(), Some(FLOOR));
    assert_eq!(
        sampled.priced(&record).rest - plain.priced(&record).rest,
        SAMPLE
    );
    assert_eq!(plain.period(), None);
}

/// A send that finds the channel full is counted and its wait timed, as the
/// samples carry them: a slow output, one line behind.
#[test]
fn a_full_channel_s_sends_are_counted() {
    let dir = tempfile::tempdir().unwrap();
    let roots = tree(dir.path(), "0::/a/b/run.scope\n", FULL);
    let s = sampler(roots, Arc::default(), FLOOR);
    let clock = s.clock().clone();
    let out = Out {
        slow: Some((clock.clone(), 500)),
        ..Out::default()
    };
    let mut w = Written::sampled(out.clone(), journal::sample::header(), 1, s).unwrap();
    for at in 0..50 {
        w.line(line(at)).unwrap();
    }
    w.finish().unwrap();
    let last = samples(&out).pop().unwrap();
    assert_eq!(last.why, SampleWhy::Finish);
    assert!(last.live.full >= 10, "{:?}", last.live);
    assert!(last.live.waited >= 5_000, "{:?}", last.live);
}

/// How a test's probe answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum How {
    Reads,
    Gives,
    Panics,
    PanicsStating,
}

/// A test's allocator probe, its reads counted.
struct Probed {
    how: How,
    reads: AtomicU64,
}

impl Probe for Probed {
    fn allocator(&self) -> &str {
        assert!(self.how != How::PanicsStating, "a probe's own fault");
        "test.probe"
    }

    fn bytes(&self) -> u64 {
        assert!(self.how != How::PanicsStating, "a probe's own fault");
        4_096
    }

    fn read(&self) -> Option<Heap> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        match self.how {
            How::Reads | How::PanicsStating => Some(Heap {
                in_use: 10,
                free: 20,
                mapped: 30,
                chunks: Some(4),
            }),
            How::Gives => None,
            How::Panics => panic!("a probe's own fault"),
        }
    }
}

/// K2b-7e-2b-3e-2c: the host's probe is read at the baseline and the finish
/// only — a due sample skips it, named —, its figures where it reads them; a
/// probe that gives none, panics reading or panics stating itself fails
/// there, and the journal ends whole; none stated, unstated.
#[test]
fn the_probe_is_read_at_the_baseline_and_the_finish() {
    use ascetic_ddd_scheduler::journal::{AllocatorState, Code};
    let dir = tempfile::tempdir().unwrap();
    let roots = tree(dir.path(), "0::/a/b/run.scope\n", FULL);
    let run = |probe: Option<Arc<Probed>>| {
        let out = Out::default();
        let s = sampler(roots.clone(), Arc::default(), FLOOR)
            .probing(probe.clone().map(|p| Stated::of(p as Arc<dyn Probe>)));
        let clock = s.clock().clone();
        let w = Written::sampled(out.clone(), journal::sample::header(), 4, s).unwrap();
        for_micros(&clock, 4 * FLOOR + FLOOR / 2, std::thread::yield_now);
        w.finish().unwrap();
        samples(&out)
    };
    let named = Some(Code::Named("test.probe".to_owned()));
    for (how, state, name) in [
        (How::Reads, AllocatorState::Read, named.clone()),
        (How::Gives, AllocatorState::Failed, named.clone()),
        (How::Panics, AllocatorState::Failed, named.clone()),
        (How::PanicsStating, AllocatorState::Failed, None),
    ] {
        let probe = Arc::new(Probed {
            how,
            reads: AtomicU64::new(0),
        });
        let got = run(Some(probe.clone()));
        let ends = [got.first().unwrap(), got.last().unwrap()];
        assert_eq!(
            ends.map(|s| s.why),
            [SampleWhy::Baseline, SampleWhy::Finish]
        );
        for s in ends {
            let a = &s.allocator;
            assert_eq!((a.state, &a.name), (state, &name), "{how:?}");
            let figures = (a.in_use, a.free, a.mapped, a.chunks);
            if state == AllocatorState::Read {
                assert_eq!(figures, (Some(10), Some(20), Some(30), Some(4)));
            } else {
                assert_eq!(figures, (None, None, None, None), "{how:?}");
            }
        }
        let dues: Vec<_> = got.iter().filter(|s| s.why == SampleWhy::Due).collect();
        assert!(!dues.is_empty(), "{how:?}: no due sample");
        for s in dues {
            assert_eq!(
                (s.allocator.state, &s.allocator.name, s.allocator.in_use),
                (AllocatorState::Skipped, &name, None),
                "{how:?}"
            );
        }
        assert_eq!(probe.reads.load(Ordering::Relaxed), 2, "{how:?}");
    }
    let unstated = run(None);
    assert!(
        unstated
            .iter()
            .all(|s| (s.allocator.state, &s.allocator.name) == (AllocatorState::Unstated, &None)),
        "{:?}",
        unstated.first().map(|s| &s.allocator)
    );
    // A probe that panics stating itself states nothing.
    let stated = Stated::of(Arc::new(Probed {
        how: How::PanicsStating,
        reads: AtomicU64::new(0),
    }));
    assert_eq!(stated.bytes(), 0);
}
