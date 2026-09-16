//! The dispatcher: one message inside one transaction, and the loops of a
//! process around it. The SQL is in `store`.

use ascetic_ddd_session::{PgAccess, Session, SessionError, SessionPool};
use futures::future::join_all;
use tokio::sync::watch;

use super::{Loops, Outcome, PgInbox};
use crate::error::{BoxError, Error};
use crate::message::{CausalDependency, InboxMessage};
use crate::observer::{
    Dispatched, Expired, Failed, Fetched, Handled, InboxObserver, Marked, Resolved, Unparked,
    Waiting,
};

/// What ended a dispatch transaction in failure, with the slot it held, so
/// that the observer learns which slot's work rolled back.
struct Rolled {
    slot: Option<u32>,
    error: Error,
}

impl From<SessionError> for Rolled {
    fn from(error: SessionError) -> Self {
        Rolled {
            slot: None,
            error: error.into(),
        }
    }
}

impl<P, O> PgInbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: InboxObserver,
{
    /// Processes the next eligible message with `subscriber`, in one
    /// transaction.
    ///
    /// Takes whichever slot has a due head and is held by nobody, least
    /// recently served first, and holds it for the length of the transaction,
    /// so that one dispatcher at a time works a slot and the order of arrival
    /// within it is kept. A head whose dependency is not processed is set
    /// aside to wait for it, and that is the whole transaction: the wait is
    /// committed at once, under a lock on the dependency's identity that its
    /// mark takes too, so that neither misses the other (ADR-0009); the next
    /// call takes the slot's next head. Otherwise the subscriber runs inside
    /// the transaction that marks the message processed, and is given that
    /// transaction, in a savepoint of its own: if it fails, its writes are
    /// rolled back to the savepoint, and the attempt is recorded and
    /// committed instead of the mark. `Err` is for the database; a failing
    /// subscriber is an [`Outcome`]. A dispatcher has no identity: any number
    /// of them, in any number of processes, share the slots through the locks
    /// alone.
    pub async fn dispatch<F, Fut>(&self, subscriber: F) -> Result<Outcome, Error>
    where
        F: Fn(&P::Session, &InboxMessage) -> Fut + Send + Sync,
        Fut: Future<Output = Result<(), BoxError>> + Send,
    {
        if let Some(max_wait) = self.max_wait {
            // A statement of its own, outside the dispatch transaction: a
            // transaction holding expired rows must never wait for a lock a
            // marker holds (ADR-0009).
            let expired = self
                .pool
                .session(async |session| self.expire_waiting(&session, max_wait).await)
                .await?;
            if !expired.is_empty() {
                self.observer.on_expired(&Expired { messages: &expired });
            }
        }
        let before_slot = |error: Error| Rolled { slot: None, error };
        let outcome: Result<(Option<u32>, Outcome), Rolled> = self
            .pool
            .session(async |session| {
                session
                    .atomic(async |tx| {
                        let taken = self.take(&tx).await.map_err(before_slot)?;
                        let Some(slot) = taken.slot else {
                            self.observer.on_fetched(&Fetched {
                                slot: None,
                                slots: taken.slots,
                                message: None,
                                snapshot: &taken.snapshot,
                            });
                            return Ok((None, Outcome::Nothing));
                        };
                        let in_slot = |error: Error| Rolled {
                            slot: Some(slot),
                            error,
                        };
                        let step = self.head_of(&tx, slot).await.map_err(in_slot)?;
                        let snapshot = step.snapshot;
                        let message = match step.row {
                            Some((message, false)) => message,
                            // the previous holder changed the head after the
                            // take chose the slot: nothing due in it now
                            Some((_, true)) | None => {
                                self.observer.on_fetched(&Fetched {
                                    slot: Some(slot),
                                        slots: taken.slots,
                                    message: None,
                                    snapshot: &snapshot,
                                });
                                return Ok((Some(slot), Outcome::Nothing));
                            }
                        };
                        if let Some(dependency) = self
                            .first_unprocessed_dependency(&tx, &message)
                            .await
                            .map_err(in_slot)?
                        {
                            self.lock_identity(&tx, &dependency)
                                .await
                                .map_err(in_slot)?;
                            let set = self
                                .set_waiting_unless_processed(&tx, &message, &dependency)
                                .await
                                .map_err(in_slot)?;
                            if !set {
                                // marked between the check and the lock: nothing
                                // to wait for, and the lock is the one this
                                // transaction may hold; the next call takes the
                                // head again
                                return Ok((Some(slot), Outcome::Nothing));
                            }
                            self.observer.on_waiting(&Waiting {
                                slot,
                                message: &message,
                                dependency: &dependency,
                                snapshot: &snapshot,
                            });
                            return Ok((Some(slot), Outcome::SetAside));
                        }
                        self.observer.on_fetched(&Fetched {
                            slot: Some(slot),
                            slots: taken.slots,
                            message: Some(&message),
                            snapshot: &snapshot,
                        });
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
                            Err(other) => return Err(in_slot(other)),
                        };
                        self.observer.on_handled(&Handled {
                            slot,
                            message: &message,
                            outcome: failure.as_ref().map_or(Ok(()), Err),
                        });
                        match failure {
                            None => {
                                if self.slots > 1 {
                                    // with one slot every dispatcher serializes on
                                    // it, and no wait can race the mark
                                    self.lock_identity(&tx, &CausalDependency::on(&message))
                                        .await
                                        .map_err(in_slot)?;
                                }
                                let (processed_position, woken) = self
                                    .mark_processed(&tx, &message)
                                    .await
                                    .map_err(in_slot)?;
                                self.observer.on_marked(&Marked {
                                    slot,
                                    message: &message,
                                    processed_position,
                                    woken: &woken,
                                });
                                Ok((Some(slot), Outcome::Processed))
                            }
                            Some(error) => {
                                let retry_after = (self.retries.backoff)(message.attempts + 1);
                                let (attempts, parked) = self
                                    .record_failure(&tx, &message, &error, retry_after)
                                    .await
                                    .map_err(in_slot)?;
                                // The `log` facade is the observer everyone has: a
                                // parked message must not be silent when no
                                // observer of ours is attached. An attempt is
                                // expected and repeats; parking needs a person.
                                let identity = format!(
                                    "{}/{}/{}/{}",
                                    message.tenant_id,
                                    message.stream_type,
                                    message.stream_id,
                                    message.stream_position
                                );
                                if parked {
                                    log::error!(
                                        "inbox: {identity} parked after {attempts} failed attempts: {error}"
                                    );
                                } else {
                                    log::warn!(
                                        "inbox: attempt {attempts} on {identity} failed, next in {retry_after:?}: {error}"
                                    );
                                }
                                self.observer.on_failed(&Failed {
                                    slot,
                                    message: &message,
                                    error: &error,
                                    attempts,
                                    parked,
                                    retry_after,
                                });
                                Ok((Some(slot), Outcome::Failed { attempts, parked }))
                            }
                        }
                    })
                    .await
            })
            .await;
        self.observer.on_dispatched(&Dispatched {
            slot: match &outcome {
                Ok((slot, _)) => *slot,
                Err(rolled) => rolled.slot,
            },
            outcome: outcome
                .as_ref()
                .map(|(_, outcome)| *outcome)
                .map_err(|rolled| &rolled.error),
        });
        outcome
            .map(|(_, outcome)| outcome)
            .map_err(|rolled| rolled.error)
    }

    /// Processes messages until `shutdown` completes, with `loops.concurrency`
    /// loops. A loop that finds nothing waits `loops.poll_interval`; one that
    /// processed, failed or set a message aside goes on at once. Shutdown is
    /// cooperative: a loop finishes its message, commits, and only then
    /// stops. If a loop fails, the others are stopped the same way and its
    /// error is returned; a failing subscriber is not such a failure.
    pub async fn run<F, Fut>(
        &self,
        subscriber: F,
        loops: Loops,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), Error>
    where
        F: Fn(&P::Session, &InboxMessage) -> Fut + Send + Sync,
        Fut: Future<Output = Result<(), BoxError>> + Send,
    {
        let (stop, _) = watch::channel(false);
        let futures = (0..loops.concurrency.max(1)).map(|_| {
            let mut stopped = stop.subscribe();
            let stop = stop.clone();
            let subscriber = &subscriber;
            async move {
                loop {
                    if *stopped.borrow() {
                        return Ok(());
                    }
                    match self.dispatch(subscriber).await {
                        Ok(Outcome::Processed | Outcome::Failed { .. } | Outcome::SetAside) => {}
                        Ok(Outcome::Nothing) => {
                            tokio::select! {
                                _ = stopped.changed() => return Ok(()),
                                _ = tokio::time::sleep(loops.poll_interval) => {}
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
    /// effects, and puts what waited for it back into the queue. Runs in a
    /// transaction of `session`, under the lock a mark takes (ADR-0009), so
    /// a wait for this message set beside it is woken all the same. Returns
    /// whether the message was parked.
    pub async fn resolve(
        &self,
        session: &P::Session,
        message: &InboxMessage,
    ) -> Result<bool, Error> {
        let resolved = session
            .atomic(async |tx| {
                self.lock_identity(&tx, &CausalDependency::on(message))
                    .await?;
                self.resolve_row(&tx, message).await
            })
            .await?;
        if let Some((processed_position, woken)) = &resolved {
            self.observer.on_resolved(&Resolved {
                message,
                processed_position: *processed_position,
                woken,
            });
        }
        Ok(resolved.is_some())
    }
}
