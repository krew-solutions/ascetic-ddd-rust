//! The live composition (ADR-0047 decisions 16, 17): a `current_thread`
//! runtime per source, each on a thread of its own, its driver taking at
//! least as many events a turn as the source holds descriptors — an
//! attempt's look verifies one turn, which must see every socket ready.

use std::io;
use std::thread::JoinHandle;

use tokio::sync::oneshot;

use crate::run::Runtimes;

/// The sources' runtimes, running until dropped.
pub struct Live {
    runtimes: Runtimes,
    stops: Vec<oneshot::Sender<()>>,
    threads: Vec<JoinHandle<()>>,
}

impl Live {
    /// `sources` runtimes, each taking `events` a driver turn — no fewer than
    /// the descriptors the source may hold.
    pub fn start(sources: usize, events: usize) -> io::Result<Live> {
        let mut handles = Vec::with_capacity(sources);
        let mut stops = Vec::with_capacity(sources);
        let mut threads = Vec::with_capacity(sources);
        for i in 0..sources {
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
        Ok(Live {
            runtimes: Runtimes { sources: handles },
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
