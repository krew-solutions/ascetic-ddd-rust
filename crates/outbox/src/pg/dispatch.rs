//! The dispatcher: a batch inside one transaction, and the loops of a
//! process around it. The SQL is in `store`.

use ascetic_ddd_session::{PgAccess, Session, SessionPool};
use futures::future::join_all;
use tokio::sync::watch;

use super::{Loops, PgOutbox, Selection};
use ascetic_ddd_session::SessionError;

use crate::error::{BoxError, Error};
use crate::message::{OutboxMessage, Position};
use crate::observer::{Acked, Dispatched, Fetched, Handled, OutboxObserver};

/// What ended a dispatch transaction in failure, with the slot it had taken,
/// so that the observer learns which batch rolled back.
struct Failed {
    slot: Option<u32>,
    error: Error,
}

impl From<SessionError> for Failed {
    fn from(error: SessionError) -> Self {
        Failed {
            slot: None,
            error: error.into(),
        }
    }
}

impl<P, O> PgOutbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: OutboxObserver,
{
    /// Dispatches the next batch of `selection` to `subscriber`: the batch of
    /// whichever slot has visible work and is held by nobody, least recently
    /// served first.
    ///
    /// Returns whether there was anything to dispatch. The subscriber runs
    /// inside the dispatcher's transaction, under the lock of the slot's
    /// position; if it fails, the batch is rolled back and redelivered. A
    /// dispatcher has no identity: any number of them, in any number of
    /// processes, share a selection through the locks alone, and one that
    /// dies releases its slot with its transaction.
    pub async fn dispatch<F, Fut>(
        &self,
        subscriber: F,
        selection: &Selection,
    ) -> Result<bool, Error>
    where
        F: Fn(&OutboxMessage) -> Fut + Send + Sync,
        Fut: Future<Output = Result<(), BoxError>> + Send,
    {
        let (group, uri) = (&selection.consumer_group, &selection.uri);
        let before_slot = |error: Error| Failed { slot: None, error };
        let outcome: Result<Option<u32>, Failed> = self
            .pool
            .session(async |session| {
                session
                    .atomic(async |tx| {
                        let mut batch = self.fetch(&tx, group, uri).await.map_err(before_slot)?;
                        if !batch.known {
                            // the first contact of this selection: its position rows
                            self.ensure_positions(&tx, group, uri)
                                .await
                                .map_err(before_slot)?;
                            batch = self.fetch(&tx, group, uri).await.map_err(before_slot)?;
                        }
                        self.observer.on_fetched(&Fetched {
                            group,
                            slot: batch.slot,
                            slots: batch.slots,
                            horizon: batch.horizon,
                            limit: self.batch_size as usize,
                            messages: &batch.messages,
                        });
                        let (Some(slot), Some(last)) = (batch.slot, batch.messages.last()) else {
                            return Ok(None);
                        };
                        let in_slot = |error: Error| Failed {
                            slot: Some(slot),
                            error,
                        };
                        for message in &batch.messages {
                            let handled = subscriber(message).await;
                            self.observer.on_handled(&Handled {
                                group,
                                slot,
                                message,
                                outcome: handled.as_ref().map(|_| ()),
                            });
                            handled.map_err(Error::Subscriber).map_err(in_slot)?;
                        }
                        let acked = Position {
                            transaction_id: last.transaction_id.unwrap_or_default(),
                            offset: last.position.unwrap_or_default(),
                        };
                        self.ack(&tx, group, uri, slot, acked)
                            .await
                            .map_err(in_slot)?;
                        self.observer.on_acked(&Acked {
                            group,
                            slot,
                            position: acked,
                        });
                        Ok(Some(slot))
                    })
                    .await
            })
            .await;
        self.observer.on_dispatched(&Dispatched {
            group,
            slot: match &outcome {
                Ok(slot) => *slot,
                Err(failed) => failed.slot,
            },
            outcome: outcome
                .as_ref()
                .map(|slot| slot.is_some())
                .map_err(|failed| &failed.error),
        });
        outcome
            .map(|slot| slot.is_some())
            .map_err(|failed| failed.error)
    }

    /// Dispatches `selection` until `shutdown` completes, with `loops.concurrency`
    /// loops. A loop that finds nothing waits `loops.poll_interval`. Shutdown
    /// is cooperative: a loop finishes its batch, commits, and only then
    /// stops. A loop whose subscriber failed — the batch rolled back, to be
    /// delivered again — or that met an error of the moment in the database
    /// — a connection lost, a server going down — waits, longer with each
    /// failure in a row up to `loops.max_pause`, and goes on. On any other
    /// error of the database, a defect, the loops are all stopped and the
    /// error is returned (ADR-0010).
    pub async fn run<F, Fut>(
        &self,
        subscriber: F,
        selection: &Selection,
        loops: Loops,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), Error>
    where
        F: Fn(&OutboxMessage) -> Fut + Send + Sync,
        Fut: Future<Output = Result<(), BoxError>> + Send,
    {
        let (stop, _) = watch::channel(false);
        let futures = (0..loops.concurrency.max(1)).map(|_| {
            let mut stopped = stop.subscribe();
            let stop = stop.clone();
            let subscriber = &subscriber;
            async move {
                // failures met in a row, for the length of the pause
                let mut failures: u32 = 0;
                loop {
                    if *stopped.borrow() {
                        return Ok(());
                    }
                    let pause = match self.dispatch(subscriber, selection).await {
                        Ok(true) => {
                            failures = 0;
                            continue;
                        }
                        Ok(false) => {
                            failures = 0;
                            loops.poll_interval
                        }
                        Err(error)
                            if matches!(error, Error::Subscriber(_)) || error.is_transient() =>
                        {
                            failures += 1;
                            let pause = loops.pause_after(failures);
                            log::warn!("outbox: a loop failed, waiting {pause:?}: {error}");
                            pause
                        }
                        Err(error) => {
                            let _ = stop.send(true);
                            return Err(error);
                        }
                    };
                    tokio::select! {
                        _ = stopped.changed() => return Ok(()),
                        _ = tokio::time::sleep(pause) => {}
                    }
                }
            }
        });
        let futures = join_all(futures);
        tokio::pin!(futures);
        let outcomes = tokio::select! {
            outcomes = &mut futures => outcomes,
            _ = shutdown => {
                let _ = stop.send(true);
                futures.await
            }
        };
        outcomes.into_iter().collect::<Result<Vec<()>, Error>>()?;
        Ok(())
    }

    /// Where `selection` is, by slot: the last acknowledged transaction and
    /// offset of each, zero before anything was acknowledged; empty before
    /// the first dispatch. The minimum over the slots is a watermark for the
    /// whole selection.
    pub async fn positions(
        &self,
        session: &P::Session,
        selection: &Selection,
    ) -> Result<Vec<Position>, Error> {
        self.read_positions(session, &selection.consumer_group, &selection.uri)
            .await
    }

    /// Moves every slot of `selection` to `position`: back to zero to replay,
    /// or ahead to skip. Creates the position rows if the selection has none.
    pub async fn set_position(
        &self,
        session: &P::Session,
        selection: &Selection,
        position: Position,
    ) -> Result<(), Error> {
        let (group, uri) = (&selection.consumer_group, &selection.uri);
        self.ensure_positions(session, group, uri).await?;
        self.move_all(session, group, uri, position).await
    }
}
