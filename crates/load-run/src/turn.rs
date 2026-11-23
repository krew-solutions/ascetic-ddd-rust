//! One turn of the source runtime's I/O driver, verified (ADR-0047 decision
//! 17).
//!
//! On a `current_thread` runtime a task woken from outside — by the clock's
//! own timer thread — can be polled before the driver has turned and seen a
//! socket's readiness: a read then returns Pending on bytes already in the
//! host. `yield_now` defers its waker to the list the runtime wakes only after
//! a driver turn (tokio 1.53.1, `runtime/scheduler/current_thread/mod.rs`,
//! `park` and `park_yield`); but it returns on its next poll whoever woke the
//! task. So the yield is polled under a waker of its own, a flag only that list
//! holds: the turn is over only when the flag fired, and a wake from anyone else
//! — hyper's want, a channel — yields again.

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Wake, Waker};

#[derive(Default)]
struct Flag {
    fired: AtomicBool,
    task: Mutex<Option<Waker>>,
}

impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.fired.store(true, Ordering::SeqCst);
        let task = self
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(w) = task {
            w.wake();
        }
    }
}

/// Ready once the runtime's driver has turned since it was first polled.
pub(crate) async fn turn() {
    loop {
        let flag = Arc::new(Flag::default());
        let waker = Waker::from(flag.clone());
        let mut yielded = pin!(tokio::task::yield_now());
        let turned = poll_fn(|cx| {
            *flag.task.lock().unwrap_or_else(PoisonError::into_inner) = Some(cx.waker().clone());
            let mut own = Context::from_waker(&waker);
            match yielded.as_mut().poll(&mut own) {
                Poll::Ready(()) => Poll::Ready(flag.fired.load(Ordering::SeqCst)),
                Poll::Pending => Poll::Pending,
            }
        })
        .await;
        if turned {
            return;
        }
    }
}

/// `f`'s output if it is ready now, after a verified turn; none if not.
pub(crate) async fn after_turn<F: Future + Unpin>(f: &mut F) -> Option<F::Output> {
    turn().await;
    poll_fn(|cx| match std::pin::Pin::new(&mut *f).poll(cx) {
        Poll::Ready(v) => Poll::Ready(Some(v)),
        Poll::Pending => Poll::Ready(None),
    })
    .await
}
