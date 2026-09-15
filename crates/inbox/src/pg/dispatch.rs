//! The dispatcher: one message inside one transaction, and the loop of
//! workers around it. The SQL is in `store`.

use std::sync::atomic::Ordering;

use ascetic_ddd_session::{PgAccess, Session, SessionPool};
use futures::future::join_all;
use tokio::sync::watch;

use super::{Outcome, PgInbox, Worker, Workers};
use crate::error::{BoxError, Error};
use crate::message::InboxMessage;
use crate::observer::{
    Deferred, Dispatched, Failed, Fetched, Handled, InboxObserver, Marked, Resolved, Skipped,
    Unparked,
};
use crate::snapshot::Snapshot;

impl<P, O> PgInbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: InboxObserver,
{
    /// Processes the next eligible message with `subscriber`, as `worker`.
    ///
    /// The subscriber runs inside the transaction that marks the message
    /// processed, and is given that transaction, in a savepoint of its own:
    /// if it fails, its writes are rolled back to the savepoint, and the
    /// attempt is recorded and committed instead of the mark. `Err` is for
    /// the database; a failing subscriber is an [`Outcome`].
    ///
    /// Several calls of one worker may run at once, `FOR UPDATE SKIP LOCKED`
    /// keeps them apart; the observer tells their events apart by the call
    /// number.
    pub async fn dispatch<F, Fut>(&self, subscriber: F, worker: Worker) -> Result<Outcome, Error>
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
                        let (taken, snapshot) = self.next_processable(&tx, worker, call).await?;
                        self.observer.on_fetched(&Fetched {
                            worker,
                            call,
                            message: taken.as_ref(),
                            snapshot: &snapshot,
                        });
                        let Some(message) = taken else {
                            return Ok(Outcome::Nothing);
                        };
                        let attempt = tx
                            .atomic(async |inner| {
                                subscriber(&inner, &message)
                                    .await
                                    .map_err(Error::Subscriber)
                            })
                            .await;
                        let failure = match attempt {
                            Ok(()) => None,
                            Err(Error::Subscriber(error)) => Some(error),
                            Err(other) => return Err(other),
                        };
                        self.observer.on_handled(&Handled {
                            worker,
                            call,
                            message: &message,
                            outcome: failure.as_ref().map_or(Ok(()), Err),
                        });
                        match failure {
                            None => {
                                let processed_position = self.mark_processed(&tx, &message).await?;
                                self.observer.on_marked(&Marked {
                                    worker,
                                    call,
                                    message: &message,
                                    processed_position,
                                });
                                Ok(Outcome::Processed)
                            }
                            Some(error) => {
                                let retry_after = (self.retries.backoff)(message.attempts + 1);
                                let (attempts, parked) = self
                                    .record_failure(&tx, &message, &error, retry_after)
                                    .await?;
                                log::warn!(
                                    "inbox: attempt {attempts} on {}/{}/{}/{} failed{}: {error}",
                                    message.tenant_id,
                                    message.stream_type,
                                    message.stream_id,
                                    message.stream_position,
                                    if parked { ", parked" } else { "" },
                                );
                                self.observer.on_failed(&Failed {
                                    worker,
                                    call,
                                    message: &message,
                                    error: &error,
                                    attempts,
                                    parked,
                                    retry_after,
                                });
                                Ok(Outcome::Failed { attempts, parked })
                            }
                        }
                    })
                    .await
            })
            .await;
        self.observer.on_dispatched(&Dispatched {
            worker,
            call,
            outcome: outcome.as_ref().map(|outcome| *outcome),
        });
        outcome
    }

    /// Processes messages until `shutdown` completes, with `workers`.
    ///
    /// A loop that finds nothing waits `poll_interval`. Shutdown is
    /// cooperative: a loop finishes its message, commits, and only then
    /// stops. If a loop fails, the others are stopped the same way and its
    /// error is returned; a failing subscriber is not such a failure.
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
                        Ok(Outcome::Processed | Outcome::Failed { .. }) => {}
                        Ok(Outcome::Nothing) => {
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

    /// The parked messages, oldest first, with their attempts and last error.
    pub async fn parked(&self, session: &P::Session) -> Result<Vec<InboxMessage>, Error> {
        self.parked_rows(session).await
    }

    /// Gives a parked message another go: attempts reset, due at once.
    /// Returns whether the message was parked.
    pub async fn unpark(
        &self,
        session: &P::Session,
        message: &InboxMessage,
    ) -> Result<bool, Error> {
        let unparked = self.unpark_row(session, message).await?;
        if unparked {
            self.observer.on_unparked(&Unparked { message });
        }
        Ok(unparked)
    }

    /// Marks a parked message processed by hand, without the subscriber's
    /// effects; what depends on it may go ahead. Returns whether the message
    /// was parked.
    pub async fn resolve(
        &self,
        session: &P::Session,
        message: &InboxMessage,
    ) -> Result<bool, Error> {
        let resolved = self.resolve_row(session, message).await?;
        if let Some(processed_position) = resolved {
            self.observer.on_resolved(&Resolved {
                message,
                processed_position,
            });
        }
        Ok(resolved.is_some())
    }

    /// The oldest unprocessed message whose dependencies are processed, or
    /// nothing when the oldest row still waits for its backoff; with the
    /// snapshot of the statement that decided. Rows whose dependencies are
    /// not processed are stepped over, and the observer is told of each.
    async fn next_processable(
        &self,
        session: &P::Session,
        worker: Worker,
        call: u64,
    ) -> Result<(Option<InboxMessage>, Snapshot), Error> {
        let mut skipped = 0i64;
        loop {
            let step = self.unprocessed(session, skipped, worker).await?;
            let Some((message, deferred)) = step.row else {
                return Ok((None, step.snapshot));
            };
            if deferred {
                self.observer.on_deferred(&Deferred {
                    worker,
                    call,
                    message: &message,
                    snapshot: &step.snapshot,
                });
                return Ok((None, step.snapshot));
            }
            if self.dependencies_processed(session, &message).await? {
                return Ok((Some(message), step.snapshot));
            }
            self.observer.on_skipped(&Skipped {
                worker,
                call,
                message: &message,
                snapshot: &step.snapshot,
            });
            skipped += 1;
        }
    }
}
