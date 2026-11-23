//! The live composition (ADR-0047 decisions 16, 17): a `current_thread`
//! runtime per source, each on a thread of its own, its driver taking at
//! least as many events a turn as the source holds descriptors — an
//! attempt's look verifies one turn, which must see every socket ready.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

use tokio::sync::oneshot;

use crate::run::{DRIVER_EVENTS, Runtimes};

/// The process's descriptors (ADR-0047 decision 20): its soft and hard
/// limits and those open — read by `Live` once its runtimes exist, so their
/// own are counted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Descriptors {
    soft: u64,
    hard: u64,
    open: u64,
    raised: bool,
}

/// Whether the process raised its soft limit to its hard one.
static RAISED: AtomicBool = AtomicBool::new(false);

impl Descriptors {
    /// The soft limit: what the process may hold open.
    pub fn soft(&self) -> u64 {
        self.soft
    }

    /// The hard limit: what the soft one may be raised to.
    pub fn hard(&self) -> u64 {
        self.hard
    }

    /// Those open when read.
    pub fn open(&self) -> u64 {
        self.open
    }

    /// Whether the soft limit was raised to the hard one before.
    pub fn raised(&self) -> bool {
        self.raised
    }

    /// The process's, now (Linux: its limits and `/proc/self/fd`).
    #[cfg(target_os = "linux")]
    pub fn read() -> io::Result<Descriptors> {
        use nix::sys::resource::{Resource, getrlimit};
        let (soft, hard) = getrlimit(Resource::RLIMIT_NOFILE).map_err(io::Error::from)?;
        // The listing's own descriptor is open while it is read.
        let listed = std::fs::read_dir("/proc/self/fd")?.count();
        let open = u64::try_from(listed.saturating_sub(1)).unwrap_or(u64::MAX);
        Ok(Descriptors {
            soft,
            hard,
            open,
            raised: RAISED.load(Ordering::SeqCst),
        })
    }

    /// Unknown off Linux: a run needing sockets is refused.
    #[cfg(not(target_os = "linux"))]
    pub fn read() -> io::Result<Descriptors> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "the process's descriptors are read on Linux alone",
        ))
    }

    /// The soft limit raised to the hard one — or to the kernel's own
    /// ceiling, an infinite hard limit taking none (decision 20): the
    /// process's, and its children's, a choice of the composition root's.
    #[cfg(target_os = "linux")]
    pub fn raise() -> io::Result<Descriptors> {
        use nix::sys::resource::{RLIM_INFINITY, Resource, getrlimit, setrlimit};
        let (_, hard) = getrlimit(Resource::RLIMIT_NOFILE).map_err(io::Error::from)?;
        let to = if hard == RLIM_INFINITY {
            std::fs::read_to_string("/proc/sys/fs/nr_open")?
                .trim()
                .parse::<u64>()
                .map_err(io::Error::other)?
        } else {
            hard
        };
        setrlimit(Resource::RLIMIT_NOFILE, to, hard).map_err(io::Error::from)?;
        RAISED.store(true, Ordering::SeqCst);
        Descriptors::read()
    }

    /// Unknown off Linux.
    #[cfg(not(target_os = "linux"))]
    pub fn raise() -> io::Result<Descriptors> {
        Descriptors::read()
    }

    /// A test's descriptors: `soft`, `hard`, `open`, assumed.
    #[cfg(feature = "testing")]
    pub fn assumed(soft: u64, hard: u64, open: u64) -> Descriptors {
        Descriptors {
            soft,
            hard,
            open,
            raised: false,
        }
    }
}

/// The sources' runtimes, running until dropped.
pub struct Live {
    runtimes: Runtimes,
    stops: Vec<oneshot::Sender<()>>,
    threads: Vec<JoinHandle<()>>,
}

impl Live {
    /// `sources` runtimes, each taking `events` a driver turn — no fewer than
    /// the source's cap and its driver's own (decision 20).
    pub fn start(sources: usize, events: usize) -> io::Result<Live> {
        Live::with_events(&vec![events; sources])
    }

    /// A runtime per source, each taking its cap and its driver's own events
    /// a turn (decision 20): `caps` by source — a carrier's
    /// [`Needs`](crate::carrier::Needs).
    pub fn for_caps(caps: &[usize]) -> io::Result<Live> {
        let events = caps
            .iter()
            .map(|cap| {
                cap.checked_add(DRIVER_EVENTS)
                    // mio sizes its buffer to an `i32`.
                    .filter(|e| i32::try_from(*e).is_ok())
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("a cap of {cap} sockets: past a driver's events"),
                        )
                    })
            })
            .collect::<io::Result<Vec<usize>>>()?;
        Live::with_events(&events)
    }

    fn with_events(events: &[usize]) -> io::Result<Live> {
        let sources = events.len();
        let mut handles = Vec::with_capacity(sources);
        let mut stops = Vec::with_capacity(sources);
        let mut threads = Vec::with_capacity(sources);
        for (i, &events) in events.iter().enumerate() {
            // Its parks seen: whether each blocked (decision 18).
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .max_io_events_per_tick(events.max(1))
                .on_thread_park(crate::clock::idle::parking)
                .on_thread_unpark(crate::clock::idle::unparked)
                .build()?;
            handles.push(rt.handle().clone());
            let (stop, stopped) = oneshot::channel::<()>();
            stops.push(stop);
            threads.push(
                std::thread::Builder::new()
                    .name(format!("load-run-source-{i}"))
                    .spawn(move || {
                        rt.block_on(async {
                            let _ = stopped.await;
                        });
                    })?,
            );
        }
        // Read once the runtimes exist: their own counted (decision 20).
        let descriptors = Descriptors::read().ok();
        Ok(Live {
            runtimes: Runtimes::new(handles, events.to_vec(), descriptors),
            stops,
            threads,
        })
    }

    /// Where attempts run.
    pub fn runtimes(&self) -> &Runtimes {
        &self.runtimes
    }
}

impl Drop for Live {
    fn drop(&mut self) {
        for stop in self.stops.drain(..) {
            let _ = stop.send(());
        }
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}
