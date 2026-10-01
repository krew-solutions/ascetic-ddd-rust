//! The calls of a subscription's handler in flight, and the loops that serve
//! a subscription: what [`Subscription::cancel`] waits for.
//!
//! A transport calls a handler from tasks of its own. Detaching the handler
//! stops the calls to come; a call already made goes on, and with it
//! whatever it holds — a transaction, a connection, a message not yet
//! acknowledged. So a subscription cancelled is a handler that is not
//! running and will not be run: a transport counts every call through an
//! [`InFlight`] and ends its cancel with [`InFlight::quiesce`]; a transport
//! that serves a subscription with a loop of its own makes it with
//! [`serve`], and its cancel waits for the loop to return — the batch in
//! hand committed, the connection given back. From inside a call, a handler
//! cancelling its own subscription, the wait returns at once, told by a
//! task-local mark the calls are made under; a handler would otherwise wait
//! for itself. The protocol of the OCaml port's ADR-0016, carried back.
//!
//! A call is admitted under the lock the transport detaches handlers under,
//! so that it is either seen by the wait or not made; it is counted out when
//! its future is dropped, on return, error or panic alike.

use std::future::Future;
use std::sync::Arc;

use futures::future::BoxFuture;
use tokio::runtime::Handle;
use tokio::sync::{Notify, watch};

use crate::adapter::Subscription;

tokio::task_local! {
    static INSIDE: ();
}

/// Runs `call` as a call of a handler: inside it, a cancel does not wait.
pub fn within<F: Future>(call: F) -> impl Future<Output = F::Output> {
    INSIDE.scope((), call)
}

/// Whether this task is inside a call of a handler.
pub fn inside() -> bool {
    INSIDE.try_with(|()| ()).is_ok()
}

/// The calls of one subscription's handler that are in flight.
#[derive(Clone)]
pub struct InFlight {
    count: Arc<watch::Sender<usize>>,
    idle: watch::Receiver<usize>,
}

impl Default for InFlight {
    fn default() -> Self {
        InFlight::new()
    }
}

impl InFlight {
    /// Nothing in flight.
    pub fn new() -> Self {
        let (count, idle) = watch::channel(0);
        InFlight {
            count: Arc::new(count),
            idle,
        }
    }

    /// One more call in flight, until the guard is dropped. Take it under
    /// the lock handlers are detached under.
    pub fn admit(&self) -> Admitted {
        self.count.send_modify(|n| *n += 1);
        Admitted(Arc::clone(&self.count))
    }

    /// Returns when no call is in flight; at once from inside a call.
    pub async fn quiesce(&self) {
        if inside() {
            return;
        }
        let mut idle = self.idle.clone();
        // An error means the sender is gone, and so is every call.
        idle.wait_for(|n| *n == 0).await.ok();
    }

    /// Whether `other` counts the same calls.
    pub fn same(&self, other: &InFlight) -> bool {
        Arc::ptr_eq(&self.count, &other.count)
    }
}

impl std::fmt::Debug for InFlight {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InFlight")
            .field("count", &*self.idle.borrow())
            .finish()
    }
}

/// A call admitted; dropping it counts the call out.
pub struct Admitted(Arc<watch::Sender<usize>>);

impl Drop for Admitted {
    fn drop(&mut self) {
        self.0.send_modify(|n| *n = n.saturating_sub(1));
    }
}

/// A subscription served by `task`, spawned on `runtime`: cancelling it
/// notifies `stop` once and waits for the task to return. A task that
/// finishes what it has in hand when told to stop is a subscription that
/// ends in good order. A task that panics is over as well, and nobody is
/// left waiting for it.
pub fn serve(
    runtime: &Handle,
    stop: Arc<Notify>,
    task: impl Future<Output = ()> + Send + 'static,
) -> Subscription {
    let (done, finished) = watch::channel(false);
    runtime.spawn(async move {
        task.await;
        done.send_replace(true);
    });
    Subscription::with_quiesce(
        move || stop.notify_one(),
        move || {
            let mut finished = finished.clone();
            Box::pin(async move {
                finished.wait_for(|done| *done).await.ok();
            }) as BoxFuture<'static, ()>
        },
    )
}
