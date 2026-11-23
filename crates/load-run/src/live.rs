//! The live composition (ADR-0047 decisions 16, 17): a `current_thread`
//! runtime per source, each on a thread of its own, its driver taking at
//! least as many events a turn as the source holds descriptors — an
//! attempt's look verifies one turn, which must see every socket ready.

use std::io;
use std::thread::JoinHandle;

use tokio::sync::oneshot;

use crate::run::{DRIVER_EVENTS, Runtimes};

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
        Ok(Live {
            runtimes: Runtimes::new(handles, events.to_vec()),
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
