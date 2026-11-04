//! The dispatcher: a batch inside one transaction, and the loops of a
//! process around it. The SQL is in `store`.

use ascetic_ddd_session::{PgAccess, Session, SessionPool};
use futures::future::join_all;
use tokio::sync::watch;

use super::{Loops, PgOutbox, Selection};
use ascetic_ddd_session::SessionError;

use crate::error::{BoxError, Error};
use crate::message::{Epoch, Move, OutboxMessage, Position};
use crate::observer::{Acked, Dispatched, EpochStarted, Fetched, Handled, OutboxObserver, Receipt};

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
    /// whichever slot has work and is held by nobody, least recently served
    /// first (ADR-0035).
    ///
    /// One transaction: the look that takes the slot's position row; the
    /// epoch's start, unless one is in flight; its next batch, by position;
    /// the subscriber, given each message; the position moved past the batch,
    /// and, when the batch is shorter than its size, `delivered` moved to the
    /// epoch. Every read after the lock runs under a snapshot taken after it,
    /// so a stored epoch is read whole. An epoch with nothing of this
    /// selection's is delivered at once.
    ///
    /// Returns whether a position moved. If the subscriber fails, the batch
    /// is rolled back and comes again. A dispatcher has no identity: any
    /// number of them, in any number of processes, share a selection through
    /// the locks alone, and one that dies releases its slot with its
    /// transaction.
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
        let limit = self.batch_size as usize;
        let before_slot = |error: Error| Failed { slot: None, error };
        let outcome: Result<Option<u32>, Failed> = self
            .pool
            .session(async |session| {
                session
                    .atomic(async |tx| {
                        let mut taken = self.take(&tx, group, uri).await.map_err(before_slot)?;
                        if !taken.known {
                            // the first contact of this selection: its position rows
                            self.ensure_positions(&tx, group, uri)
                                .await
                                .map_err(before_slot)?;
                            taken = self.take(&tx, group, uri).await.map_err(before_slot)?;
                        }
                        // a counter reset: refused until an operator rebases (decision 12)
                        if let Some(reason) = taken.refused {
                            return Err(before_slot(Error::CounterReset(reason)));
                        }
                        if taken.reset {
                            let reason = "a position delivered past the server's transaction \
                                          ids: the database was moved by dump and restore"
                                .to_owned();
                            self.refuse(&reason).await.map_err(before_slot)?;
                            return Err(before_slot(Error::CounterReset(reason)));
                        }
                        let Some(slot) = taken.slot else {
                            self.observer.on_fetched(&Fetched {
                                group,
                                slot: None,
                                slots: taken.slots,
                                horizon: 0,
                                statement: None,
                                delivered: None,
                                epoch: None,
                                epoch_offset: 0,
                                epoch_end: 0,
                                limit,
                                messages: &[],
                            });
                            return Ok(None);
                        };
                        let in_slot = |error: Error| Failed {
                            slot: Some(slot),
                            error,
                        };
                        let delivered = taken.position.delivered;
                        let (epoch, started) = match taken.position.epoch {
                            Some(epoch @ Epoch { end: Some(_), .. }) => (epoch, false),
                            Some(Epoch {
                                snapshot, offset, ..
                            }) => {
                                // converted from ADR-0007's positions: its end, now
                                let start = self
                                    .start_epoch(&tx, slot, uri, &delivered, Some(&snapshot))
                                    .await
                                    .map_err(in_slot)?;
                                let end = start.last.map_or(offset, |last| last.max(offset));
                                (
                                    Epoch {
                                        snapshot,
                                        offset,
                                        end: Some(end),
                                    },
                                    false,
                                )
                            }
                            None => {
                                let start = self
                                    .start_epoch(&tx, slot, uri, &delivered, None)
                                    .await
                                    .map_err(in_slot)?;
                                let (Some(first), Some(last)) = (start.first, start.last) else {
                                    // nothing of this selection's is new: delivered at once,
                                    // so that the next look does not walk the same rows
                                    self.observer.on_fetched(&Fetched {
                                        group,
                                        slot: Some(slot),
                                        slots: taken.slots,
                                        horizon: start.epoch.xmin,
                                        statement: Some(&start.epoch),
                                        delivered: Some(&delivered),
                                        epoch: Some(&start.epoch),
                                        epoch_offset: 0,
                                        epoch_end: 0,
                                        limit,
                                        messages: &[],
                                    });
                                    self.ack(
                                        &tx,
                                        group,
                                        uri,
                                        slot,
                                        Some(&start.epoch),
                                        None,
                                        false,
                                    )
                                    .await
                                    .map_err(in_slot)?;
                                    self.observer.on_acked(&Acked {
                                        group,
                                        slot,
                                        last: None,
                                        drained: true,
                                        short: false,
                                    });
                                    return Ok(Some(slot));
                                };
                                self.observer.on_epoch_started(&EpochStarted {
                                    group,
                                    slot,
                                    epoch: &start.epoch,
                                    delivered: &delivered,
                                    first,
                                    last,
                                });
                                (
                                    Epoch {
                                        snapshot: start.epoch,
                                        offset: first - 1,
                                        end: Some(last),
                                    },
                                    true,
                                )
                            }
                        };
                        let end = epoch.end.unwrap_or(epoch.offset);
                        let read = self
                            .read_batch(
                                &tx,
                                slot,
                                uri,
                                epoch.offset,
                                end,
                                &epoch.snapshot,
                                &delivered,
                            )
                            .await
                            .map_err(in_slot)?;
                        self.observer.on_fetched(&Fetched {
                            group,
                            slot: Some(slot),
                            slots: taken.slots,
                            horizon: read.statement.xmin,
                            statement: Some(&read.statement),
                            delivered: Some(&delivered),
                            epoch: Some(&epoch.snapshot),
                            epoch_offset: epoch.offset,
                            epoch_end: end,
                            limit,
                            messages: &read.messages,
                        });
                        for message in &read.messages {
                            let handled = subscriber(message).await;
                            self.observer.on_handled(&Handled {
                                group,
                                slot,
                                message,
                                outcome: handled.as_ref().map(|_| ()),
                            });
                            handled.map_err(Error::Subscriber).map_err(in_slot)?;
                        }
                        let last = read.messages.last().map(|message| Receipt {
                            transaction_id: message.transaction_id.unwrap_or_default(),
                            position: message.position.unwrap_or_default(),
                        });
                        // a short batch: nothing of the epoch is left, since its
                        // snapshot is contained in the batch's
                        let drained = read.messages.len() < limit;
                        if drained {
                            self.ack(&tx, group, uri, slot, Some(&epoch.snapshot), None, started)
                                .await
                                .map_err(in_slot)?;
                        } else {
                            let moved = Epoch {
                                offset: last.map_or(epoch.offset, |last| last.position),
                                ..epoch
                            };
                            self.ack(&tx, group, uri, slot, None, Some(&moved), started)
                                .await
                                .map_err(in_slot)?;
                        }
                        // drained before its end: its last row was not found
                        let reached = last.map_or(epoch.offset, |last| last.position);
                        self.observer.on_acked(&Acked {
                            group,
                            slot,
                            last,
                            drained,
                            short: drained && reached < end,
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

    /// Where `selection` is, by slot: what it delivered, and the epoch in
    /// flight; empty before the first dispatch. Snapshots have no order, so
    /// the positions of several slots have no minimum.
    pub async fn positions(
        &self,
        session: &P::Session,
        selection: &Selection,
    ) -> Result<Vec<Position>, Error> {
        self.read_positions(session, &selection.consumer_group, &selection.uri)
            .await
    }

    /// Moves every slot of `selection` (ADR-0035 decision 11): back to the
    /// start, back to a transaction, or ahead to now; see [`Move`]. The epoch
    /// in flight is cleared, and each row's lock waited for. Creates the
    /// position rows if the selection has none.
    pub async fn set_position(
        &self,
        session: &P::Session,
        selection: &Selection,
        to: Move,
    ) -> Result<(), Error> {
        let (group, uri) = (&selection.consumer_group, &selection.uri);
        self.ensure_positions(session, group, uri).await?;
        self.move_all(session, group, uri, to).await
    }
}
