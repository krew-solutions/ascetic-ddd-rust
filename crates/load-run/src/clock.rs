//! The shell's one clock (ADR-0047 decision 16).
//!
//! Every instant the shell stamps is a read of its [`Clock`], in microseconds
//! since T0; only this module reads the system's or tokio's clock (the crate's
//! `clippy.toml`, and `tests/lint.rs` that no other module allows its way past
//! it).
//!
//! - [`RealClock`]: the monotonic clock from T0, its wall instant bracketed;
//!   its sleeps a timed wait on a timer thread of its own — no spin, and none of
//!   tokio's rounding of a timer up to its millisecond.
//! - `VirtualClock` (feature `testing`): a counter of its own, moved only when
//!   every task on its runtime is idle — tokio's paused time, documented to
//!   auto-advance when nothing can run, is the idle signal, never the counter.
#![expect(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "the one module that reads the system's and tokio's clocks (ADR-0047 decision 16)"
)]

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::task::{Context, Poll, Waker};
use std::thread::Thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The shell's clock. Microseconds since T0, never going back.
pub trait Clock: Send + Sync + 'static {
    /// Microseconds since T0.
    fn now(&self) -> u64;

    /// T0's wall instant: microseconds since the Unix epoch.
    fn wall(&self) -> i64;

    /// When the calling thread last ended a park that blocked — its runtime
    /// idle, nothing ready (ADR-0047 decision 18): a wake ready before it was
    /// runnable at it. The virtual clock's now: its time passes only while
    /// every task is idle. A thread whose parks no hook saw: T0, so its lag is
    /// over-counted, never under.
    fn idle_since(&self) -> u64;

    /// Ready once [`Clock::now`] reached `at`.
    fn sleep_until(&self, at: u64) -> impl Future<Output = ()> + Send + '_;

    /// Ready once nothing more can be stamped at `at`: once `now() > at`
    /// live; once every task is idle at `at`, virtually.
    fn settled(&self, at: u64) -> impl Future<Output = ()> + Send + '_;
}

/// A clock that would not start.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClockError {
    /// The wall clock read before the Unix epoch.
    BeforeEpoch,
    /// No bracket `wall, now, wall` within J in so many tries: the wall
    /// instant of T0 is not known to J.
    Bracket {
        /// The narrowest bracket seen, in microseconds.
        narrowest: u64,
    },
    /// Its timer thread would not start.
    Thread(String),
}

impl fmt::Display for ClockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClockError::BeforeEpoch => write!(f, "the wall clock reads before the Unix epoch"),
            ClockError::Bracket { narrowest } => write!(
                f,
                "T0's wall instant not bracketed within J: the narrowest bracket {narrowest} µs"
            ),
            ClockError::Thread(e) => write!(f, "the clock's timer thread would not start: {e}"),
        }
    }
}

impl std::error::Error for ClockError {}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // A clock's state holds wakers and instants, each whole after any panic.
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn micros(d: Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX)
}

/// Sleepers by `(instant, id)`: a waker each.
#[derive(Default)]
struct Sleepers {
    at: BTreeMap<(u64, u64), Waker>,
    next: u64,
    closed: bool,
}

impl Sleepers {
    fn id(&mut self) -> u64 {
        self.next = self.next.wrapping_add(1);
        self.next
    }
}

struct Timer {
    t0: Instant,
    sleepers: Mutex<Sleepers>,
    thread: OnceLock<Thread>,
}

/// A thread's idleness, as its runtime's park hooks see it (decision 18): a
/// park that blocked — its thread switched out of its own accord, as only a
/// wait in the driver's poll does — ends at an instant every event ready
/// before it was delivered; one that returned at once proves nothing, and a
/// park's length cannot tell them apart (measured: beside CPU hogs, one park
/// in a thousand that returned at once lasted past a millisecond, preempted).
/// `Live` installs these on every source runtime.
pub mod idle {
    use std::cell::Cell;
    use std::time::Instant;

    thread_local! {
        static PARKING: Cell<Option<i64>> = const { Cell::new(None) };
        static IDLE: Cell<Option<Instant>> = const { Cell::new(None) };
    }

    /// The thread's voluntary context switches so far.
    #[cfg(target_os = "linux")]
    fn switches() -> Option<i64> {
        nix::sys::resource::getrusage(nix::sys::resource::UsageWho::RUSAGE_THREAD)
            .ok()
            .map(|u| u.voluntary_context_switches())
    }

    /// Unknown off Linux: no park counts as blocked, and the lag is
    /// over-counted.
    #[cfg(not(target_os = "linux"))]
    fn switches() -> Option<i64> {
        None
    }

    /// The runtime's thread about to park.
    pub fn parking() {
        PARKING.with(|p| p.set(switches()));
    }

    /// Its park over: the instant it ended, if it blocked.
    pub fn unparked() {
        let before = PARKING.with(Cell::take);
        if let (Some(before), Some(after)) = (before, switches())
            && after > before
        {
            IDLE.with(|i| i.set(Some(Instant::now())));
        }
    }

    /// When the calling thread last ended a park that blocked.
    pub(crate) fn last() -> Option<Instant> {
        IDLE.with(Cell::get)
    }
}

/// The monotonic clock from T0 (see the [module documentation](self)).
pub struct RealClock {
    timer: Arc<Timer>,
    wall: i64,
}

/// How many times T0 is redrawn before its bracket is refused.
const TRIES: u32 = 16;

/// The longest wait `settled` spins rather than sleeps: about the timer
/// slack a timed wait has anyway.
const SPIN: u64 = 50;

impl RealClock {
    /// T0, now: bracketed `wall, now, wall` and redrawn while the bracket is
    /// wider than `jitter` µs; its sleeps served by a thread of its own.
    pub fn start(jitter: u64) -> Result<RealClock, ClockError> {
        let mut narrowest = u64::MAX;
        for _ in 0..TRIES {
            let before = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| ClockError::BeforeEpoch)?;
            let t0 = Instant::now();
            let after = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| ClockError::BeforeEpoch)?;
            let width = micros(after.saturating_sub(before));
            narrowest = narrowest.min(width);
            if width <= jitter {
                let mid = micros(before).saturating_add(width / 2);
                let wall = i64::try_from(mid).unwrap_or(i64::MAX);
                return RealClock::from(t0, wall);
            }
        }
        Err(ClockError::Bracket { narrowest })
    }

    fn from(t0: Instant, wall: i64) -> Result<RealClock, ClockError> {
        let timer = Arc::new(Timer {
            t0,
            sleepers: Mutex::new(Sleepers::default()),
            thread: OnceLock::new(),
        });
        let served = timer.clone();
        let handle = std::thread::Builder::new()
            .name("load-run-clock".to_owned())
            .spawn(move || serve(&served))
            .map_err(|e| ClockError::Thread(e.to_string()))?;
        let _ = timer.thread.set(handle.thread().clone());
        Ok(RealClock { timer, wall })
    }
}

impl RealClock {
    /// What `rx` gives until the clock reads `at`: a second waiter on the
    /// same clock, the journal's writer's (ADR-0047 decision 22,
    /// K2b-7e-2b-3e) — the only timed wait outside this module's timer,
    /// which the lint holds here.
    ///
    /// # Errors
    ///
    /// [`std::sync::mpsc::RecvTimeoutError::Timeout`] once `at` is reached
    /// with nothing given; `Disconnected` when its senders are gone.
    pub fn recv_until<T>(
        &self,
        rx: &std::sync::mpsc::Receiver<T>,
        at: u64,
    ) -> Result<T, std::sync::mpsc::RecvTimeoutError> {
        let wait = at.saturating_sub(self.timer.now());
        #[expect(
            clippy::disallowed_methods,
            reason = "the clock's own timed wait: a waiter on its one clock (ADR-0047 decision 16)"
        )]
        rx.recv_timeout(Duration::from_micros(wait))
    }
}

impl Timer {
    fn now(&self) -> u64 {
        micros(self.t0.elapsed())
    }

    fn unpark(&self) {
        if let Some(t) = self.thread.get() {
            t.unpark();
        }
    }
}

/// The timer thread: wakes each sleeper at its instant, parked until the
/// next one or until a sleeper earlier than it arrives.
fn serve(timer: &Timer) {
    loop {
        let next = {
            let mut s = lock(&timer.sleepers);
            if s.closed {
                return;
            }
            let now = timer.now();
            while let Some(entry) = s.at.first_entry() {
                if entry.key().0 > now {
                    break;
                }
                entry.remove().wake();
            }
            s.at.first_key_value().map(|(k, _)| k.0.saturating_sub(now))
        };
        match next {
            #[expect(
                clippy::disallowed_methods,
                reason = "the timer's own wait: its one clock's sleepers (ADR-0047 decision 16)"
            )]
            Some(wait) => std::thread::park_timeout(Duration::from_micros(wait)),
            None => std::thread::park(),
        }
    }
}

impl Drop for RealClock {
    fn drop(&mut self) {
        lock(&self.timer.sleepers).closed = true;
        self.timer.unpark();
    }
}

/// A sleep on the real clock: registered with the timer thread while pending,
/// removed when dropped.
struct RealSleep<'a> {
    timer: &'a Timer,
    at: u64,
    id: Option<u64>,
}

impl Future for RealSleep<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let at = self.at;
        if self.timer.now() >= at {
            if let Some(id) = self.id.take() {
                lock(&self.timer.sleepers).at.remove(&(at, id));
            }
            return Poll::Ready(());
        }
        let earliest = {
            let mut s = lock(&self.timer.sleepers);
            let id = match self.id {
                Some(id) => id,
                None => s.id(),
            };
            s.at.insert((at, id), cx.waker().clone());
            self.id = Some(id);
            s.at.first_key_value().is_some_and(|(k, _)| *k == (at, id))
        };
        if earliest {
            self.timer.unpark();
        }
        Poll::Pending
    }
}

impl Drop for RealSleep<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            lock(&self.timer.sleepers).at.remove(&(self.at, id));
        }
    }
}

impl Clock for RealClock {
    fn now(&self) -> u64 {
        self.timer.now()
    }

    fn wall(&self) -> i64 {
        self.wall
    }

    fn idle_since(&self) -> u64 {
        idle::last().map_or(0, |at| micros(at.saturating_duration_since(self.timer.t0)))
    }

    fn sleep_until(&self, at: u64) -> impl Future<Output = ()> + Send + '_ {
        RealSleep {
            timer: &self.timer,
            at,
            id: None,
        }
    }

    async fn settled(&self, at: u64) {
        loop {
            let now = self.now();
            if now > at {
                return;
            }
            // Within the timer's own slack, a bounded spin; past it, a timed
            // wait.
            if at.saturating_add(1).saturating_sub(now) <= SPIN {
                while self.now() <= at {
                    std::hint::spin_loop();
                }
                return;
            }
            self.sleep_until(at.saturating_add(1)).await;
        }
    }
}

#[cfg(feature = "testing")]
pub use virtual_clock::VirtualClock;

#[cfg(feature = "testing")]
mod virtual_clock {
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, Weak};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use super::{Clock, lock};

    #[derive(Default)]
    struct Waiters {
        /// Sleepers by `(instant, id)`.
        sleepers: BTreeMap<(u64, u64), Waker>,
        /// Settle waiters by id: a waker, and whether an idle settled them.
        settlers: BTreeMap<u64, (Option<Waker>, bool)>,
        /// Who waits to hear that the clock went idle with nothing to wake.
        stuck: Option<Waker>,
        is_stuck: bool,
        next: u64,
    }

    impl Waiters {
        fn id(&mut self) -> u64 {
            self.next = self.next.wrapping_add(1);
            self.next
        }
    }

    struct Inner {
        now: AtomicU64,
        wall: i64,
        waiters: Mutex<Waiters>,
    }

    /// A counter of its own, moved only when every task on its runtime is
    /// idle (see the [module documentation](super)).
    #[derive(Clone)]
    pub struct VirtualClock {
        inner: Arc<Inner>,
    }

    impl VirtualClock {
        /// T0 at 0, its wall instant `wall`. Pauses tokio's clock and spawns
        /// the idle watcher: call it on a `current_thread` runtime, whose
        /// every task the clock then sees (tokio panics on any other).
        pub fn start(wall: i64) -> VirtualClock {
            tokio::time::pause();
            let inner = Arc::new(Inner {
                now: AtomicU64::new(0),
                wall,
                waiters: Mutex::new(Waiters::default()),
            });
            let weak: Weak<Inner> = Arc::downgrade(&inner);
            tokio::spawn(async move {
                loop {
                    // Paused, tokio moves its own time only when nothing on
                    // the runtime can run: this sleep ends exactly then.
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    let Some(inner) = weak.upgrade() else {
                        return;
                    };
                    inner.idle();
                }
            });
            VirtualClock { inner }
        }

        /// Runs `f`, panicking if the clock goes idle with nothing to wake —
        /// a run waiting on what can never come — rather than hanging.
        pub async fn within<F: Future>(&self, f: F) -> F::Output {
            let stuck = Stuck { inner: &self.inner };
            tokio::select! {
                biased;
                out = f => out,
                () = stuck => panic!(
                    "the virtual clock went idle at {} µs with nothing to wake",
                    self.inner.now.load(Ordering::SeqCst)
                ),
            }
        }
    }

    impl Inner {
        /// Every task idle: settle every waiter at the instant; else move to
        /// the earliest sleeper and wake every one due then; else stuck.
        fn idle(&self) {
            let mut w = lock(&self.waiters);
            if !w.settlers.is_empty() {
                for (waker, settled) in w.settlers.values_mut() {
                    *settled = true;
                    if let Some(waker) = waker.take() {
                        waker.wake();
                    }
                }
                return;
            }
            let Some(&(at, _)) = w.sleepers.keys().next() else {
                w.is_stuck = true;
                if let Some(waker) = w.stuck.take() {
                    waker.wake();
                }
                return;
            };
            self.now.fetch_max(at, Ordering::SeqCst);
            while let Some(entry) = w.sleepers.first_entry() {
                if entry.key().0 > at {
                    break;
                }
                entry.remove().wake();
            }
        }
    }

    struct Stuck<'a> {
        inner: &'a Inner,
    }

    impl Future for Stuck<'_> {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            let mut w = lock(&self.inner.waiters);
            if w.is_stuck {
                return Poll::Ready(());
            }
            w.stuck = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    struct Sleep<'a> {
        inner: &'a Inner,
        at: u64,
        id: Option<u64>,
    }

    impl Future for Sleep<'_> {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            let at = self.at;
            let mut w = lock(&self.inner.waiters);
            if self.inner.now.load(Ordering::SeqCst) >= at {
                if let Some(id) = self.id.take() {
                    w.sleepers.remove(&(at, id));
                }
                return Poll::Ready(());
            }
            let id = match self.id {
                Some(id) => id,
                None => w.id(),
            };
            w.sleepers.insert((at, id), cx.waker().clone());
            drop(w);
            self.id = Some(id);
            Poll::Pending
        }
    }

    impl Drop for Sleep<'_> {
        fn drop(&mut self) {
            if let Some(id) = self.id.take() {
                lock(&self.inner.waiters).sleepers.remove(&(self.at, id));
            }
        }
    }

    struct Settle<'a> {
        inner: &'a Inner,
        at: u64,
        id: Option<u64>,
    }

    impl Future for Settle<'_> {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            let mut w = lock(&self.inner.waiters);
            if self.inner.now.load(Ordering::SeqCst) > self.at {
                if let Some(id) = self.id.take() {
                    w.settlers.remove(&id);
                }
                return Poll::Ready(());
            }
            if let Some(id) = self.id {
                if w.settlers.get(&id).is_some_and(|(_, settled)| *settled) {
                    w.settlers.remove(&id);
                    drop(w);
                    self.id = None;
                    return Poll::Ready(());
                }
                w.settlers.insert(id, (Some(cx.waker().clone()), false));
                return Poll::Pending;
            }
            let id = w.id();
            w.settlers.insert(id, (Some(cx.waker().clone()), false));
            drop(w);
            self.id = Some(id);
            Poll::Pending
        }
    }

    impl Drop for Settle<'_> {
        fn drop(&mut self) {
            if let Some(id) = self.id.take() {
                lock(&self.inner.waiters).settlers.remove(&id);
            }
        }
    }

    impl Clock for VirtualClock {
        fn now(&self) -> u64 {
            self.inner.now.load(Ordering::SeqCst)
        }

        fn wall(&self) -> i64 {
            self.inner.wall
        }

        fn idle_since(&self) -> u64 {
            self.now()
        }

        fn sleep_until(&self, at: u64) -> impl Future<Output = ()> + Send + '_ {
            Sleep {
                inner: &self.inner,
                at,
                id: None,
            }
        }

        fn settled(&self, at: u64) -> impl Future<Output = ()> + Send + '_ {
            Settle {
                inner: &self.inner,
                at,
                id: None,
            }
        }
    }
}
