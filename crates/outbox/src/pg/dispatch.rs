//! The dispatcher: a batch inside one transaction, and the loop of workers
//! around it. The SQL is in `store`.

use ascetic_ddd_session::{PgAccess, Session, SessionPool};
use futures::future::join_all;
use tokio::sync::watch;

use super::{PgOutbox, Selection, Worker, Workers};
use crate::error::{BoxError, Error};
use crate::message::{OutboxMessage, Position};
use crate::observer::{Acked, Dispatched, Fetched, Handled, OutboxObserver};

impl<P, O> PgOutbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: OutboxObserver,
{
    /// Dispatches the next batch of `selection` to `subscriber`, as `worker`.
    ///
    /// Returns whether there was anything to dispatch. The subscriber runs
    /// inside the dispatcher's transaction, under the lock of the group's
    /// position; if it fails, the batch is rolled back and redelivered.
    pub async fn dispatch<F, Fut>(
        &self,
        subscriber: F,
        selection: &Selection,
        worker: Worker,
    ) -> Result<bool, Error>
    where
        F: Fn(&OutboxMessage) -> Fut + Send + Sync,
        Fut: Future<Output = Result<(), BoxError>> + Send,
    {
        let group = effective_group(&selection.consumer_group, worker);
        self.pool
            .session(async |session| self.ensure_group(&session, &group, &selection.uri).await)
            .await?;
        let outcome = self
            .pool
            .session(async |session| {
                session
                    .atomic(async |tx| {
                        let batch = self.fetch(&tx, &group, &selection.uri, worker).await?;
                        self.observer.on_fetched(&Fetched {
                            group: &group,
                            worker,
                            horizon: batch.horizon,
                            limit: self.batch_size as usize,
                            messages: &batch.messages,
                        });
                        let Some(last) = batch.messages.last() else {
                            return Ok(false);
                        };
                        for message in &batch.messages {
                            let handled = subscriber(message).await;
                            self.observer.on_handled(&Handled {
                                group: &group,
                                worker,
                                message,
                                outcome: handled.as_ref().map(|_| ()),
                            });
                            handled.map_err(Error::Subscriber)?;
                        }
                        let acked = Position {
                            transaction_id: last.transaction_id.unwrap_or_default(),
                            offset: last.position.unwrap_or_default(),
                        };
                        self.ack(&tx, &group, &selection.uri, acked).await?;
                        self.observer.on_acked(&Acked {
                            group: &group,
                            worker,
                            position: acked,
                        });
                        Ok(true)
                    })
                    .await
            })
            .await;
        self.observer.on_dispatched(&Dispatched {
            group: &group,
            worker,
            outcome: outcome.as_ref().map(|dispatched| *dispatched),
        });
        outcome
    }

    /// Dispatches `selection` until `shutdown` completes, with `workers`.
    ///
    /// A loop that finds nothing waits `poll_interval`. Shutdown is
    /// cooperative: a loop finishes its batch, commits, and only then stops,
    /// so no transaction is cut in the middle. If a loop fails, the others
    /// are stopped the same way and its error is returned.
    pub async fn run<F, Fut>(
        &self,
        subscriber: F,
        selection: &Selection,
        workers: Workers,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), Error>
    where
        F: Fn(&OutboxMessage) -> Fut + Send + Sync,
        Fut: Future<Output = Result<(), BoxError>> + Send,
    {
        let (stop, _) = watch::channel(false);
        let total = workers.num_processes.max(1) * workers.concurrency.max(1);
        let loops = (0..workers.concurrency.max(1)).map(|local| {
            let worker = Worker {
                id: workers.process_id * workers.concurrency.max(1) + local,
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
                    match self.dispatch(subscriber, selection, worker).await {
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

    /// Where `selection` is: the last acknowledged transaction and offset,
    /// zero before anything was acknowledged.
    pub async fn position(
        &self,
        session: &P::Session,
        selection: &Selection,
    ) -> Result<Position, Error> {
        self.read_position(session, &selection.consumer_group, &selection.uri)
            .await
    }

    /// Moves `selection` to `position`: back to zero to replay, or ahead
    /// to skip.
    pub async fn set_position(
        &self,
        session: &P::Session,
        selection: &Selection,
        position: Position,
    ) -> Result<(), Error> {
        self.ack(session, &selection.consumer_group, &selection.uri, position)
            .await
    }
}

/// Several workers of one group each keep a position of their own.
fn effective_group(group: &str, worker: Worker) -> String {
    if worker.of > 1 {
        format!("{group}:{}", worker.id)
    } else {
        group.to_owned()
    }
}
