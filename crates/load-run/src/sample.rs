//! The run's memory, sampled by its journal's writer (ADR-0047 decision 22,
//! K2b-7e-2b-3e): evidence no fold reads. The process's own, its cgroup's
//! and the group's whose limit binds, the host's pressure, the shell's live
//! counts as its loop last stored them, each group read through one buffer
//! of a fixed size or not read, and why. A sample is due when its instant
//! passed, checked after every line the writer takes and every timeout; the
//! next one the larger of the period and a hundred times the sample's cost
//! past it, offset by a draw from the run's namespace, never caught up.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering, fence};

use ascetic_ddd_scheduler::journal::{
    Allocator, AllocatorState, Binds, Cgroup, Events, Live, Pressure, Process, Reading, Sample,
    SampleWhy, Unread,
};

use crate::clock::{Clock, RealClock};

/// The buffer every file is read through: its size the most a sample holds
/// of a file at once.
pub const BUFFER: usize = 8_192;

/// What a sampler holds on its writer's thread beside the writer: its
/// buffer, a sample boxed, its paths (ADR-0047 decision 22, K2b-7e-2b-3e) —
/// priced in the journal's term.
pub const SAMPLE: u64 = BUFFER as u64 + 4_096 + 2_048;

/// The longest period's floor, microseconds: a sample at most ten a second.
pub const FLOOR: u64 = 100_000;

/// The shell's live counts, stored by its loop each turn and loaded by the
/// sampler as one instant's: a sequence lock, one writer.
#[derive(Debug, Default)]
pub struct Gauges {
    seq: AtomicU64,
    turn: AtomicU64,
    wakes: AtomicU64,
    pending: AtomicU64,
    past_deadline: AtomicU64,
    alive: AtomicU64,
    sockets: AtomicU64,
    full: AtomicU64,
    waited: AtomicU64,
}

/// What a turn stores.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Turn {
    /// The clock's now under the loop's lock.
    pub turn: u64,
    /// The instant the loop sleeps until, if it sleeps.
    pub wakes: Option<u64>,
    /// The lines read and not ended.
    pub pending: u64,
    /// The handles past their deadline.
    pub past_deadline: u64,
    /// The attempts alive.
    pub alive: u64,
    /// The carrier's sockets open, by its own word.
    pub sockets: Option<u64>,
}

const NONE: u64 = u64::MAX;

impl Gauges {
    /// A turn's counts, stored as one: by the loop alone.
    pub fn store(&self, t: Turn) {
        let s = self.seq.load(Ordering::Relaxed);
        self.seq.store(s.wrapping_add(1), Ordering::Relaxed);
        fence(Ordering::Release);
        self.turn.store(t.turn, Ordering::Relaxed);
        self.wakes.store(t.wakes.unwrap_or(NONE), Ordering::Relaxed);
        self.pending.store(t.pending, Ordering::Relaxed);
        self.past_deadline.store(t.past_deadline, Ordering::Relaxed);
        self.alive.store(t.alive, Ordering::Relaxed);
        self.sockets
            .store(t.sockets.unwrap_or(NONE), Ordering::Relaxed);
        self.seq.store(s.wrapping_add(2), Ordering::Release);
    }

    /// A line sent while the journal's channel was full, after a wait of
    /// `waited` µs: counters, apart from a turn's.
    pub fn waited(&self, waited: u64) {
        self.full.fetch_add(1, Ordering::Relaxed);
        self.waited.fetch_add(waited, Ordering::Relaxed);
    }

    /// The last turn's counts, one instant's, or none when a store was under
    /// way through each of its 1,024 tries — never two turns' mixed.
    pub fn try_load(&self) -> Option<Live> {
        for _ in 0..1_024 {
            let before = self.seq.load(Ordering::Acquire);
            if before % 2 == 1 {
                std::hint::spin_loop();
                continue;
            }
            let live = self.read();
            fence(Ordering::Acquire);
            if self.seq.load(Ordering::Relaxed) == before {
                return Some(live);
            }
        }
        None
    }

    fn read(&self) -> Live {
        let some = |v: u64| (v != NONE).then_some(v);
        Live {
            turn: self.turn.load(Ordering::Relaxed),
            wakes: some(self.wakes.load(Ordering::Relaxed)),
            pending: self.pending.load(Ordering::Relaxed),
            past_deadline: self.past_deadline.load(Ordering::Relaxed),
            alive: self.alive.load(Ordering::Relaxed),
            sockets: some(self.sockets.load(Ordering::Relaxed)),
            full: self.full.load(Ordering::Relaxed),
            waited: self.waited.load(Ordering::Relaxed),
        }
    }
}

/// Where a sampler reads: the process's and the host's files under `proc`
/// (`/proc`), the cgroups' under `cgroup` (`/sys/fs/cgroup`) — a test's tree
/// elsewhere.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Roots {
    /// `/proc`'s place.
    pub proc: PathBuf,
    /// The unified cgroup hierarchy's mount.
    pub cgroup: PathBuf,
}

impl Roots {
    /// The system's.
    pub fn system() -> Roots {
        Roots {
            proc: PathBuf::from("/proc"),
            cgroup: PathBuf::from("/sys/fs/cgroup"),
        }
    }
}

/// The run's sampler, on its journal's writer's thread.
pub struct Sampler {
    clock: Arc<RealClock>,
    gauges: Arc<Gauges>,
    roots: Roots,
    binds: Option<u32>,
    period: u64,
    due: u64,
    draw: u64,
    buf: Vec<u8>,
    live: Live,
}

impl Sampler {
    /// A sampler on `clock` every `period` µs (at least [`FLOOR`]), reading
    /// `gauges` and the files under `roots`, the binding limit `binds`
    /// groups above its own; its draws from `namespace`.
    pub fn new(
        clock: Arc<RealClock>,
        gauges: Arc<Gauges>,
        roots: Roots,
        binds: Option<u32>,
        period: u64,
        namespace: u64,
    ) -> Sampler {
        let period = period.max(FLOOR);
        let now = clock.now();
        Sampler {
            clock,
            gauges,
            roots,
            binds,
            period,
            due: now.saturating_add(period),
            draw: namespace,
            buf: Vec::with_capacity(BUFFER),
            live: Live {
                turn: 0,
                wakes: None,
                pending: 0,
                past_deadline: 0,
                alive: 0,
                sockets: None,
                full: 0,
                waited: 0,
            },
        }
    }

    /// The instant the next sample is due.
    pub fn due(&self) -> u64 {
        self.due
    }

    /// The clock it samples on.
    pub fn clock(&self) -> &Arc<RealClock> {
        &self.clock
    }

    /// A sample now, for `why`; a due one sets the next due instant past it,
    /// the dues it passed counted, none caught up.
    pub fn sample(&mut self, why: SampleWhy) -> Sample {
        let at = self.clock.now();
        let due = if why == SampleWhy::Due { self.due } else { at };
        let skipped = if why == SampleWhy::Due {
            at.saturating_sub(self.due) / self.period
        } else {
            0
        };
        let process = self.process();
        let cgroup = self.cgroup();
        let host = self
            .timed(|s| s.pressure_at(&s.roots.proc.join("pressure/memory")))
            .0
            .map_or_else(Reading::Unread, Reading::Read);
        // A store under way through every try: the last instant read, whose
        // `turn` says which.
        let live = self.gauges.try_load().unwrap_or(self.live);
        self.live = live;
        let took = self.clock.now().saturating_sub(at);
        if why == SampleWhy::Due {
            self.draw = splitmix(self.draw);
            let offset = self.draw % (self.period / 4).max(1);
            self.due = at
                .saturating_add(self.period.max(took.saturating_mul(100)))
                .saturating_add(offset);
        }
        Sample {
            why,
            at,
            due,
            skipped,
            process,
            cgroup,
            host,
            live,
            allocator: Allocator {
                state: AllocatorState::Unstated,
                name: None,
                in_use: None,
                free: None,
                mapped: None,
                chunks: None,
                took: 0,
            },
        }
    }

    fn timed<T>(&mut self, f: impl FnOnce(&mut Sampler) -> T) -> (T, u64) {
        let at = self.clock.now();
        let v = f(self);
        (v, self.clock.now().saturating_sub(at))
    }

    fn process(&mut self) -> Reading<Process> {
        let (read, took) = self.timed(|s| {
            let path = s.roots.proc.join("self/status");
            s.read(&path)?;
            let kb = |field: &str| field_kb(&s.buf, field);
            let rss = kb("VmRSS:").ok_or(Unread::Failed)?;
            Ok(Process {
                rss,
                rss_anon: kb("RssAnon:"),
                rss_file: kb("RssFile:"),
                rss_shmem: kb("RssShmem:"),
                swap: kb("VmSwap:"),
                hwm: kb("VmHWM:"),
                threads: field(&s.buf, "Threads:"),
                took: 0,
            })
        });
        match read {
            Ok(p) => Reading::Read(Process { took, ..p }),
            Err(u) => Reading::Unread(u),
        }
    }

    fn cgroup(&mut self) -> Reading<Cgroup> {
        let (read, took) = self.timed(Sampler::cgroup_read);
        match read {
            Ok(c) => Reading::Read(Cgroup { took, ..c }),
            Err(u) => Reading::Unread(u),
        }
    }

    fn cgroup_read(&mut self) -> Result<Cgroup, Unread> {
        let own = self.own()?;
        let file = |name: &str| own.join(name);
        self.read(&file("memory.current"))?;
        let current = number(&self.buf).ok_or(Unread::Failed)?;
        let peak = self.number_at(&file("memory.peak"));
        let swap = self.number_at(&file("memory.swap.current"));
        let stat = self.read(&file("memory.stat")).is_ok();
        let key = |k: &str| if stat { keyed(&self.buf, k) } else { None };
        let (anon, anon_thp, cache, sock, kernel_stack, pagetables, slab) = (
            key("anon"),
            key("anon_thp"),
            key("file"),
            key("sock"),
            key("kernel_stack"),
            key("pagetables"),
            key("slab"),
        );
        let events = self.events_at(&file("memory.events"));
        let pressure = self.pressure_at(&file("memory.pressure")).ok();
        let procs = self.lines_at(&file("cgroup.procs"));
        let binding = match self.binds {
            Some(depth) if depth > 0 => {
                let mut dir = own.clone();
                for _ in 0..depth {
                    dir.pop();
                }
                Some(Binds {
                    depth,
                    events: self.events_at(&dir.join("memory.events")),
                    pressure: self.pressure_at(&dir.join("memory.pressure")).ok(),
                })
            }
            _ => None,
        };
        Ok(Cgroup {
            procs,
            current,
            peak,
            anon,
            anon_thp,
            file: cache,
            sock,
            kernel_stack,
            pagetables,
            slab,
            swap,
            events,
            pressure,
            binding,
            took: 0,
        })
    }

    /// The process's own group's directory, from its `0::` line.
    fn own(&mut self) -> Result<PathBuf, Unread> {
        let path = self.roots.proc.join("self/cgroup");
        self.read(&path)?;
        let text = std::str::from_utf8(&self.buf).map_err(|_| Unread::Failed)?;
        let rel = text
            .lines()
            .find_map(|l| l.strip_prefix("0::"))
            .ok_or(Unread::NoCgroupV2)?;
        Ok(self.roots.cgroup.join(rel.trim().trim_start_matches('/')))
    }

    fn number_at(&mut self, path: &Path) -> Option<u64> {
        self.read(path).ok()?;
        number(&self.buf)
    }

    fn events_at(&mut self, path: &Path) -> Option<Events> {
        self.read(path).ok()?;
        Some(Events {
            high: keyed(&self.buf, "high")?,
            max: keyed(&self.buf, "max")?,
            oom: keyed(&self.buf, "oom")?,
            oom_kill: keyed(&self.buf, "oom_kill")?,
        })
    }

    fn pressure_at(&mut self, path: &Path) -> Result<Pressure, Unread> {
        self.read(path)?;
        let text = std::str::from_utf8(&self.buf).map_err(|_| Unread::Failed)?;
        let line = |kind: &str| -> Option<(u32, u64)> {
            let l = text.lines().find(|l| l.starts_with(kind))?;
            let value = |name: &str| {
                l.split_whitespace()
                    .find_map(|w| w.strip_prefix(name))
                    .map(str::to_owned)
            };
            let avg10 = value("avg10=")?;
            let (whole, part) = avg10.split_once('.').unwrap_or((&avg10, "0"));
            let hundredths = whole
                .parse::<u32>()
                .ok()?
                .checked_mul(100)?
                .checked_add(part.get(..2).unwrap_or(part).parse::<u32>().ok()?)?;
            Some((hundredths, value("total=")?.parse().ok()?))
        };
        let (some_avg10, some_total) = line("some").ok_or(Unread::Failed)?;
        let (full_avg10, full_total) = line("full").unwrap_or((0, 0));
        Ok(Pressure {
            some_avg10,
            some_total,
            full_avg10,
            full_total,
        })
    }

    /// A file's lines, counted as it streams through the buffer: however
    /// long, it holds no more than the buffer.
    fn lines_at(&mut self, path: &Path) -> Option<u64> {
        let mut f = std::fs::File::open(path).ok()?;
        self.buf.clear();
        self.buf.resize(BUFFER, 0);
        let mut lines = 0u64;
        loop {
            let n = f.read(&mut self.buf).ok()?;
            if n == 0 {
                break;
            }
            let chunk = self.buf.get(..n)?;
            lines = lines.saturating_add(chunk.iter().filter(|b| **b == b'\n').count() as u64);
        }
        self.buf.clear();
        Some(lines)
    }

    /// A file read into the buffer, at most [`BUFFER`] of it.
    fn read(&mut self, path: &Path) -> Result<(), Unread> {
        let mut f = std::fs::File::open(path).map_err(unread)?;
        self.buf.clear();
        self.buf.resize(BUFFER, 0);
        let mut n = 0;
        while n < BUFFER {
            let Some(rest) = self.buf.get_mut(n..) else {
                break;
            };
            match f.read(rest) {
                Ok(0) => break,
                Ok(k) => n = n.saturating_add(k),
                Err(e) => {
                    self.buf.clear();
                    return Err(unread(e));
                }
            }
        }
        self.buf.truncate(n);
        Ok(())
    }
}

fn unread(e: std::io::Error) -> Unread {
    match e.kind() {
        std::io::ErrorKind::NotFound => Unread::Absent,
        std::io::ErrorKind::PermissionDenied => Unread::Permission,
        _ => Unread::Failed,
    }
}

/// A whole file's number.
fn number(buf: &[u8]) -> Option<u64> {
    std::str::from_utf8(buf).ok()?.trim().parse().ok()
}

/// A `key value` line's value.
fn keyed(buf: &[u8], key: &str) -> Option<u64> {
    std::str::from_utf8(buf).ok()?.lines().find_map(|l| {
        let (k, v) = l.split_once(' ')?;
        (k == key).then(|| v.trim().parse().ok()).flatten()
    })
}

/// A `/proc` status field's number.
fn field(buf: &[u8], name: &str) -> Option<u64> {
    std::str::from_utf8(buf).ok()?.lines().find_map(|l| {
        l.strip_prefix(name)?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    })
}

/// A `/proc` status field in kB, in bytes.
fn field_kb(buf: &[u8], name: &str) -> Option<u64> {
    field(buf, name)?.checked_mul(1024)
}

/// The next of a sequence of draws.
fn splitmix(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}
