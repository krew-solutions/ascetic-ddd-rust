//! The clocks' probe (ADR-0047 decision 16): the virtual clock wakes a sleep
//! at its exact microsecond and settles an instant only once every task is
//! idle at it, and says so when it is idle with nothing to wake; the real
//! clock wakes no sleep early and settles past the instant.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use ascetic_ddd_load_run::clock::{Clock, RealClock, VirtualClock};

fn paused() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
}

#[test]
fn a_virtual_sleep_wakes_at_its_microsecond() {
    paused().block_on(async {
        let clock = Arc::new(VirtualClock::start(0));
        let woke = Arc::new(AtomicU64::new(0));
        let (c, w) = (clock.clone(), woke.clone());
        let task = tokio::spawn(async move {
            c.sleep_until(137).await;
            w.store(c.now(), Ordering::SeqCst);
        });
        clock.within(task).await.unwrap();
        assert_eq!(woke.load(Ordering::SeqCst), 137);
        // Two sleepers: the earlier first, each at its own instant.
        let (c, d) = (clock.clone(), clock.clone());
        let late = tokio::spawn(async move {
            d.sleep_until(1_000).await;
            d.now()
        });
        let early = tokio::spawn(async move {
            c.sleep_until(500).await;
            c.now()
        });
        assert_eq!(clock.within(early).await.unwrap(), 500);
        assert_eq!(clock.within(late).await.unwrap(), 1_000);
    });
}

#[test]
fn an_instant_settles_only_once_every_task_is_idle() {
    paused().block_on(async {
        let clock = Arc::new(VirtualClock::start(0));
        let done = Arc::new(AtomicBool::new(false));
        // A chain of tasks, each spawning the next after yielding: the last
        // marks the instant.
        fn chain(n: u32, done: Arc<AtomicBool>) {
            tokio::spawn(async move {
                tokio::task::yield_now().await;
                if n == 0 {
                    done.store(true, Ordering::SeqCst);
                } else {
                    chain(n - 1, done);
                }
            });
        }
        chain(64, done.clone());
        clock.within(clock.settled(0)).await;
        assert!(done.load(Ordering::SeqCst));
        assert_eq!(clock.now(), 0);
    });
}

#[test]
#[should_panic(expected = "idle at 250 µs with nothing to wake")]
fn a_virtual_clock_idle_with_nothing_to_wake_says_so() {
    paused().block_on(async {
        let clock = Arc::new(VirtualClock::start(0));
        clock.within(clock.sleep_until(250)).await;
        clock.within(std::future::pending::<()>()).await;
    });
}

#[test]
fn a_real_sleep_wakes_no_earlier_than_its_instant() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(async {
        let clock = RealClock::start(1_000).unwrap();
        for wait in [0u64, 1, 200, 2_000] {
            let at = clock.now() + wait;
            clock.sleep_until(at).await;
            let now = clock.now();
            assert!(now >= at, "{now} before {at}");
            // Generous: no millisecond rounding, but a loaded box's slack.
            assert!(now < at + 50_000, "{now} long after {at}");
        }
        let at = clock.now();
        clock.settled(at).await;
        assert!(clock.now() > at);
        // A sleep dropped before its instant leaves nothing behind it.
        let far = clock.now() + 1_000_000;
        tokio::select! {
            biased;
            () = clock.sleep_until(far) => panic!("woke a second early"),
            () = clock.sleep_until(clock.now() + 100) => {}
        }
        assert!(clock.wall() > 1_700_000_000_000_000);
    });
}
