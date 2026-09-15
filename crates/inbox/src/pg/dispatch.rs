//! The dispatcher: one message inside one transaction, and the loop of
//! workers around it. The SQL is in `store`.

use std::sync::atomic::Ordering;

use ascetic_ddd_session::{PgAccess, Session, SessionPool};
use futures::future::join_all;
use tokio::sync::watch;

use super::{PgInbox, Worker, Workers};
use crate::error::{BoxError, Error};
use crate::message::InboxMessage;
use crate::observer::{Dispatched, Fetched, Handled, InboxObserver, Marked, Skipped};

impl<P, O> PgInbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: InboxObserver,
{
    /// Processes the next eligible message with `subscriber`, as `worker`.
    ///
    /// Returns whether there was one. The subscriber runs inside the
    /// transaction that marks the message processed, and is given that
    /// transaction; if it fails, nothing is marked and the message is
    /// retried.
    ///
    /// Several calls of one worker may run at once, `FOR UPDATE SKIP LOCKED`
    /// keeps them apart; the observer tells their events apart by the call
    /// number.
    pub async fn dispatch<F, Fut>(&self, subscriber: F, worker: Worker) -> Result<bool, Error>
    where
        F: Fn(&P::Session, &InboxMessage) -> Fut + Send + Sync,
        Fut: Future<Output = Result<(), BoxError>> + Send,
    {
        let call = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
        let outcome = self
            .pool
            .session(async |session| {
                session
                    .atomic(async |tx| {
                        let taken = self.next_processable(&tx, worker, call).await?;
                        self.observer.on_fetched(&Fetched {
                            worker,
                            call,
                            message: taken.as_ref(),
                        });
                        let Some(message) = taken else {
                            return Ok(false);
                        };
                        let handled = subscriber(&tx, &message).await;
                        self.observer.on_handled(&Handled {
                            worker,
                            call,
                            message: &message,
                            outcome: handled.as_ref().map(|_| ()),
                        });
                        handled.map_err(Error::Subscriber)?;
                        let processed_position = self.mark_processed(&tx, &message).await?;
                        self.observer.on_marked(&Marked {
                            worker,
                            call,
                            message: &message,
                            processed_position,
                        });
                        Ok(true)
                    })
                    .await
            })
            .await;
        self.observer.on_dispatched(&Dispatched {
            worker,
            call,
            outcome: outcome.as_ref().map(|dispatched| *dispatched),
        });
        outcome
    }

    /// Processes messages until `shutdown` completes, with `workers`.
    ///
    /// A loop that finds nothing waits `poll_interval`. Shutdown is
    /// cooperative: a loop finishes its message, commits, and only then
    /// stops. If a loop fails, the others are stopped the same way and its
    /// error is returned.
    pub async fn run<F, Fut>(
        &self,
        subscriber: F,
        workers: Workers,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), Error>
    where
        F: Fn(&P::Session, &InboxMessage) -> Fut + Send + Sync,
        Fut: Future<Output = Result<(), BoxError>> + Send,
    {
        let (stop, _) = watch::channel(false);
        let concurrency = workers.concurrency.max(1);
        let total = workers.num_processes.max(1) * concurrency;
        let loops = (0..concurrency).map(|local| {
            let worker = Worker {
                id: workers.process_id * concurrency + local,
                of: total,
            };
            let mut stopped = stop.subscribe();
            let stop = stop.clone();
            let subscriber = &subscriber;
            async move {
                loop {
                    if *stopped.borrow() {
                        return Ok(());
                    }
                    match self.dispatch(subscriber, worker).await {
                        Ok(true) => {}
                        Ok(false) => {
                            tokio::select! {
                                _ = stopped.changed() => return Ok(()),
                                _ = tokio::time::sleep(workers.poll_interval) => {}
                            }
                        }
                        Err(error) => {
                            let _ = stop.send(true);
                            return Err(error);
                        }
                    }
                }
            }
        });
        let loops = join_all(loops);
        tokio::pin!(loops);
        let outcomes = tokio::select! {
            outcomes = &mut loops => outcomes,
            _ = shutdown => {
                let _ = stop.send(true);
                loops.await
            }
        };
        outcomes.into_iter().collect::<Result<Vec<()>, Error>>()?;
        Ok(())
    }

    /// The oldest unprocessed message whose dependencies are processed.
    /// Messages that are not yet eligible are stepped over, and the
    /// observer is told of each.
    async fn next_processable(
        &self,
        session: &P::Session,
        worker: Worker,
        call: u64,
    ) -> Result<Option<InboxMessage>, Error> {
        let mut skipped = 0i64;
        loop {
            let Some(message) = self.unprocessed(session, skipped, worker).await? else {
                return Ok(None);
            };
            if self.dependencies_processed(session, &message).await? {
                return Ok(Some(message));
            }
            self.observer.on_skipped(&Skipped {
                worker,
                call,
                message: &message,
            });
            skipped += 1;
        }
    }
}
