//! An observer that has to do IO hands the work to a channel.
//!
//! The narrative version of this test is `OBSERVERS.md`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ascetic_ddd_session::observer::{Outcome, ScopeEnded, ScopeKind, SessionObserver};
use ascetic_ddd_session::testing::MemorySessionPool;
use ascetic_ddd_session::{Session, SessionError, SessionPool};
use tokio::sync::mpsc;

/// What the metrics backend needs to know. Copied out of the event, because the
/// event borrows and the sample outlives the scope.
#[derive(Debug, PartialEq)]
struct Sample {
    depth: usize,
    kind: ScopeKind,
    outcome: Outcome,
}

/// Synchronous and infallible, as every observer is: it only enqueues.
struct Metrics {
    samples: mpsc::Sender<Sample>,
    dropped: Arc<AtomicUsize>,
}

impl SessionObserver for Metrics {
    fn on_scope_ended(&self, event: &ScopeEnded) {
        let sample = Sample {
            depth: event.depth,
            kind: event.kind,
            outcome: event.outcome,
        };

        // `try_send` never waits. A full queue means the consumer is behind;
        // dropping a sample is the right answer, and counting the drops keeps
        // that visible. Blocking here would put the metrics backend on the
        // completion path of a transaction.
        if self.samples.try_send(sample).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[tokio::test]
async fn an_observer_that_needs_io_enqueues_instead() {
    let (samples, mut inbox) = mpsc::channel(64);
    let dropped = Arc::new(AtomicUsize::new(0));

    // The consumer does what the observer must not: it awaits.
    let consumer = tokio::spawn(async move {
        let mut pushed = Vec::new();
        while let Some(sample) = inbox.recv().await {
            // A real one would POST to a pushgateway here.
            tokio::task::yield_now().await;
            pushed.push(sample);
        }
        pushed
    });

    let pool = MemorySessionPool::new().observed_by(Metrics {
        samples,
        dropped: Arc::clone(&dropped),
    });

    pool.session(async |session| {
        session
            .atomic(async |session| session.atomic(async |_| Ok::<_, SessionError>(())).await)
            .await
    })
    .await
    .unwrap();

    // Dropping the pool drops the observer, and with it the sender, so the
    // consumer sees the end of the stream.
    drop(pool);

    assert_eq!(
        consumer.await.unwrap(),
        [
            Sample {
                depth: 2,
                kind: ScopeKind::Savepoint,
                outcome: Outcome::Succeeded
            },
            Sample {
                depth: 1,
                kind: ScopeKind::Transaction,
                outcome: Outcome::Succeeded
            },
            Sample {
                depth: 0,
                kind: ScopeKind::Session,
                outcome: Outcome::Succeeded
            },
        ],
    );
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
}

/// A slow consumer costs samples, not latency: the scope never waits.
#[tokio::test]
async fn a_full_queue_drops_samples_instead_of_blocking() {
    let (samples, _inbox) = mpsc::channel(1);
    let dropped = Arc::new(AtomicUsize::new(0));

    let pool = MemorySessionPool::new().observed_by(Metrics {
        samples,
        dropped: Arc::clone(&dropped),
    });

    pool.session(async |session| {
        session
            .atomic(async |session| session.atomic(async |_| Ok::<_, SessionError>(())).await)
            .await
    })
    .await
    .unwrap();

    // One sample fits the queue; nobody is draining it, so the rest are counted
    // and discarded — and the session finished regardless.
    assert_eq!(dropped.load(Ordering::Relaxed), 2);
}
