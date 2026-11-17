//! The dispatcher: one message inside one transaction, and the loops of a
//! process around it. The SQL is in `store`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ascetic_ddd_bus::uri;
use ascetic_ddd_session::{PgAccess, Session, SessionError, SessionPool};
use futures::future::join_all;
use tokio::sync::watch;

use super::store::read_committed;
use super::{Ground, HeldCount, Hold, Loops, Outcome, Parked, ParkedCount, PgInbox};
use crate::breaker::{Before, Breaker, Effect, Flight, Told, Trial, Verdict};
use crate::error::{Error, Failure};
use crate::intake;
use crate::message::InboxMessage;
use crate::observer::{
    BreakerChanged, Dispatched, Failed, Fetched, Handled, InboxObserver, Marked, ParkedAtOnce,
    Resolved, Unparked, Waiting, Withheld,
};

/// The outcome of a statement over a row's values, classified (ADR-0037
/// decision 6): its value; the reason, when PostgreSQL refused it for the
/// row's own values, which parks the row; or the environment's error.
fn own<T>(
    result: Result<T, Error>,
    table: &str,
    reading: bool,
) -> Result<Result<T, &'static str>, Error> {
    match result {
        Ok(value) => Ok(Ok(value)),
        Err(Error::Database(error)) => {
            let own = error.as_db_error().and_then(|db| {
                intake::unreadable(db.code().code(), db.constraint(), table, reading)
            });
            match own {
                Some(reason) => Ok(Err(reason)),
                None => Err(Error::Database(error)),
            }
        }
        Err(other) => Err(other),
    }
}

/// An ask the breaker let through, released on every exit of the dispatch
/// that carries it: a verdict tells it, any other exit — a rollback, a
/// connection lost, a dropped future — abandons it (ADR-0042 decision 2).
struct Asked<'a> {
    breaker: &'a std::sync::Mutex<Breaker>,
    flight: Option<Flight>,
}

impl Asked<'_> {
    /// The verdict told, once: the guard is consumed, and its drop then
    /// finds nothing to abandon.
    fn told(mut self, verdict: Verdict) -> Told {
        let flight = self
            .flight
            .take()
            .expect("a guard is built with its flight");
        lock(self.breaker).told(now(), flight, verdict)
    }
}

impl Drop for Asked<'_> {
    fn drop(&mut self) {
        if let Some(flight) = self.flight.take() {
            lock(self.breaker).abandoned(&flight);
        }
    }
}

fn lock(breaker: &std::sync::Mutex<Breaker>) -> std::sync::MutexGuard<'_, Breaker> {
    breaker
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The breaker's clock: tokio's, so that a paused runtime moves it.
fn now() -> std::time::Instant {
    tokio::time::Instant::now().into_std()
}

/// A subscriber's verdict as the breaker weighs it.
fn verdict_of(failure: Option<&Failure>) -> Verdict {
    match failure {
        None => Verdict::Processed,
        Some(Failure::Unavailable {
            cause,
            scope,
            retry_after,
            ..
        }) => Verdict::Unavailable {
            cause: *cause,
            scope: *scope,
            retry_after: *retry_after,
        },
        Some(_) => Verdict::Own,
    }
}

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
    /// alone. A dispatch expires no waiting row: [`PgInbox::run`] does, on a
    /// timer, and a host that calls `dispatch` itself calls
    /// [`PgInbox::expire`] (ADR-0042 decision 7).
    pub async fn dispatch<F, Fut>(&self, subscriber: F) -> Result<Outcome, Error>
    where
        F: Fn(&P::Session, &InboxMessage) -> Fut + Send + Sync,
        Fut: Future<Output = Result<(), Failure>> + Send,
    {
        // The breaker's tick: a trial due is this call's work, and the
        // channels paused are the walk's (ADR-0042 decision 1). Every
        // dispatch ticks it, a host's own as well as `run`'s.
        let (effects, paused) = {
            let mut breaker = self.breaker();
            let effects = breaker.tick(now());
            (effects, breaker.paused())
        };
        self.tell(&effects);
        if let Some(Effect::Trial {
            id,
            channel,
            slot,
            tenant,
            ..
        }) = effects.iter().find(|e| matches!(e, Effect::Trial { .. }))
        {
            return self
                .trial(&subscriber, *id, channel, *slot as u32, tenant)
                .await;
        }
        let before_slot = |error: Error| Rolled { slot: None, error };
        let outcome: Result<(Option<u32>, Outcome), Rolled> = self
            .pool
            .session(async |session| {
                session
                    .atomic(async |tx| {
                        let taken = self.take(&tx).await.map_err(before_slot)?;
                        read_committed(&taken.isolation).map_err(before_slot)?;
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
                        let located = self
                            .locate_head(&tx, slot, &paused)
                            .await
                            .map_err(in_slot)?;
                        let snapshot = located.snapshot;
                        if !located.met.is_empty() {
                            let mut breaker = self.breaker();
                            for (channel, tenant) in &located.met {
                                breaker.met(now(), channel, &[(slot as i16, tenant.clone())]);
                            }
                        }
                        let ((received_position, place), (tenant, channel, holds)) =
                            match (located.row, located.head) {
                                (Some(row), Some(head)) => (row, head),
                                // the walk found no due head among the Tenants it
                                // examined: it passed them, or ended its round and
                                // the slot sleeps, or found only heads of channels
                                // this process paused, or the queue was empty by now
                                _ => {
                                    self.observer.on_fetched(&Fetched {
                                        slot: Some(slot),
                                        slots: taken.slots,
                                        message: None,
                                        snapshot: &snapshot,
                                    });
                                    let outcome = if located.paused {
                                        Outcome::Paused
                                    } else if located.passed {
                                        Outcome::Passed
                                    } else {
                                        Outcome::Nothing
                                    };
                                    return Ok((Some(slot), outcome));
                                }
                            };
                        // Before the row is read: a head the breaker holds is
                        // held without its payload read or its subscriber called.
                        let before = self
                            .breaker()
                            .before(now(), &channel, &tenant, slot as i16, holds);
                        let flight = match before {
                            Before::Hold { wait, cause } => {
                                self.hold_unasked(&tx, &place, cause, wait)
                                    .await
                                    .map_err(in_slot)?;
                                self.observer.on_withheld(&Withheld {
                                    slot,
                                    channel: &channel,
                                    cause,
                                    wait,
                                });
                                return Ok((Some(slot), Outcome::Held));
                            }
                            Before::Ask(flight) => flight,
                        };
                        let asked = Asked {
                            breaker: &self.breaker,
                            flight: Some(flight),
                        };
                        // The row's own failure, parked at once in this
                        // transaction, under the slot's lease and the row's
                        // lock, by a statement that binds none of its values
                        // (ADR-0037 decision 6).
                        let park = async |reason: &'static str| {
                            let channel = self.park_at(&tx, &place, reason).await.map_err(in_slot)?;
                            log::error!(
                                "inbox: the row of {channel} at {received_position} in slot {slot} parked at once: {reason}"
                            );
                            self.observer.on_parked_at_once(&ParkedAtOnce {
                                slot,
                                received_position,
                                channel: &channel,
                                reason,
                                snapshot: &snapshot,
                            });
                            Ok((Some(slot), Outcome::Parked))
                        };
                        let table = self.table.as_str();
                        let message = match own(
                            tx.atomic(async |inner| self.read_at(&inner, &place).await).await,
                            table,
                            true,
                        )
                        .map_err(in_slot)?
                        {
                            Ok(Ok(message)) => message,
                            Ok(Err(reason)) | Err(reason) => return park(reason).await,
                        };
                        // A list the intake would refuse — in a row it did not
                        // write, or past a bound a later release tightened — is
                        // refused for good, never waited on (ADR-0037 decision 4).
                        let dependencies = match message.checked_causal_dependencies() {
                            Ok(dependencies) => dependencies,
                            Err(why) => return park(why).await,
                        };
                        if let Some(dependency) = self
                            .first_unprocessed_dependency(&tx, &dependencies)
                            .await
                            .map_err(in_slot)?
                        {
                            self.lock_identity(&tx, &dependency)
                                .await
                                .map_err(in_slot)?;
                            let set = match own(
                                tx.atomic(async |inner| {
                                    self.set_waiting_unless_processed(&inner, &place, &dependency)
                                        .await
                                })
                                .await,
                                table,
                                false,
                            )
                            .map_err(in_slot)?
                            {
                                Ok(set) => set,
                                Err(reason) => return park(reason).await,
                            };
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
                            Err(Error::Subscriber(failure)) => Some(failure),
                            Err(other) => return Err(in_slot(other)),
                        };
                        self.observer.on_handled(&Handled {
                            slot,
                            message: &message,
                            outcome: failure.as_ref().map_or(Ok(()), |failure| Err(failure.error())),
                        });
                        let outcome = self
                            .conclude(&tx, slot, &place, &message, failure, asked)
                            .await
                            .map_err(in_slot)?;
                        Ok((Some(slot), outcome))
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

    /// The subscriber's verdict carried out, in its dispatch transaction:
    /// processed, the row marked; failed, the failure recorded, an
    /// `Unavailable` held for the breaker's interval. The breaker is told
    /// before the record, for that interval.
    async fn conclude(
        &self,
        tx: &P::Session,
        slot: u32,
        place: &str,
        message: &InboxMessage,
        failure: Option<Failure>,
        asked: Asked<'_>,
    ) -> Result<Outcome, Error> {
        let told = asked.told(verdict_of(failure.as_ref()));
        self.tell(&told.effects);
        let Some(failure) = failure else {
            if self.slots > 1 {
                // with one slot every dispatcher serializes on it, and no
                // wait can race the mark
                self.lock_own_identity(tx, place).await?;
            }
            // No scope of its own: the mark binds none of the row's values,
            // and after the subscriber's scope a lost connection poisons the
            // session, which a new scope would report as a defect rather than
            // the moment's (ADR-0037 decision 6).
            let (processed_position, woken) = self.mark_processed(tx, place).await?;
            self.observer.on_marked(&Marked {
                slot,
                message,
                processed_position,
                woken: &woken,
            });
            return Ok(Outcome::Processed);
        };
        // No scope of its own either: the record binds the place and the
        // failure's text made storable (ADR-0037 decision 7).
        let recorded = self
            .record_failure(tx, place, &failure, message.attempts, told.hold)
            .await?;
        let (attempts, parked, held, retry_after) = (
            recorded.attempts,
            recorded.parked,
            recorded.held,
            recorded.retry_after,
        );
        let cause = failure.cause();
        // The `log` facade is the observer everyone has: a parked message
        // must not be silent when no observer of ours is attached. An attempt
        // is expected and repeats; parking needs a person; a hold for an
        // `Unavailable` is the breaker's to report, and logs at `debug`. A
        // line names the row by its channel and its place in the inbox: never
        // by its stream, whose id and Tenant are the producer's values, and
        // never with the subscriber's text, which the row keeps and the
        // observer is handed (ADR-0036 R9).
        let place = format!(
            "{} at {} in slot {slot}",
            intake::sanitized(
                Some(uri::without_key(&message.uri).as_bytes()),
                intake::CHANNEL_WIDTH,
            )
            .unwrap_or_default(),
            message
                .received_position
                .map_or_else(|| "?".to_owned(), |position| position.to_string()),
        );
        if failure.is_permanent() {
            log::error!(
                "inbox: the message of {place} parked, the subscriber's verdict is permanent ({cause})"
            );
        } else if parked {
            log::error!(
                "inbox: the message of {place} parked after {attempts} failed attempts ({cause})"
            );
        } else if held {
            log::debug!(
                "inbox: the message of {place} held, unavailable ({cause}), next in {retry_after:?}"
            );
        } else {
            log::warn!(
                "inbox: attempt {attempts} on the message of {place} failed ({cause}), next in {retry_after:?}"
            );
        }
        self.observer.on_failed(&Failed {
            slot,
            message,
            error: failure.error(),
            cause,
            permanent: failure.is_permanent(),
            attempts,
            parked,
            held,
            retry_after,
        });
        Ok(Outcome::Failed {
            attempts,
            parked,
            held,
        })
    }

    /// A trial the breaker asked for (ADR-0042 decision 1): the witness's
    /// slot locked as `take` locks one, passed by when another holds it; the
    /// Tenant's head in it located in a statement after the lock, so that
    /// what the previous holder committed is in view, and asked if it is
    /// still a head of the paused channel; its subscriber run under the
    /// trial's wait, past which it is dropped and the transaction rolled
    /// back. The cursor is never moved; a processed trial clears the slot's
    /// round, as a take does. Statements sent as text: a trial is at most
    /// one a tick a channel.
    async fn trial<F, Fut>(
        &self,
        subscriber: &F,
        id: u64,
        channel: &str,
        slot: u32,
        tenant: &str,
    ) -> Result<Outcome, Error>
    where
        F: Fn(&P::Session, &InboxMessage) -> Fut + Send + Sync,
        Fut: Future<Output = Result<(), Failure>> + Send,
    {
        let wait = self.breaker().settings().trial_wait;
        let table = self.table.as_str();
        let ran_out = AtomicBool::new(false);
        let tried = self
            .pool
            .session(async |session| {
                session
                    .atomic(async |tx| -> Result<(Trial, Outcome), Error> {
                        let connection = tx.connection();
                        read_committed(&self.isolation(&tx).await?)?;
                        let locked = connection
                            .query_opt(
                                &format!(
                                    "SELECT slot FROM {table}_slots WHERE slot = $1 \
                                     FOR UPDATE SKIP LOCKED"
                                ),
                                &[&(slot as i16)],
                            )
                            .await?;
                        if locked.is_none() {
                            return Ok((Trial::Gone, Outcome::Nothing));
                        }
                        let head = connection
                            .query_opt(
                                &format!(
                                    "SELECT ctid::text, {channel_of} = $3 FROM {table} \
                                     WHERE slot = $1 AND tenant_id = $2 AND {queue} \
                                     ORDER BY received_position LIMIT 1 FOR UPDATE",
                                    channel_of = super::store::CHANNEL_OF_ROW,
                                    queue = super::store::QUEUE,
                                ),
                                &[&(slot as i16), &tenant, &channel],
                            )
                            .await?;
                        let Some(place) = head
                            .filter(|row| row.get::<_, bool>(1))
                            .map(|row| row.get::<_, String>(0))
                        else {
                            return Ok((Trial::Gone, Outcome::Nothing));
                        };
                        let message = match own(
                            tx.atomic(async |inner| self.read_at(&inner, &place).await)
                                .await,
                            table,
                            true,
                        )? {
                            Ok(Ok(message)) => message,
                            Ok(Err(_)) | Err(_) => return Ok((Trial::Gone, Outcome::Nothing)),
                        };
                        let dependencies = message.checked_causal_dependencies().ok();
                        let waits = match &dependencies {
                            Some(dependencies) => self
                                .first_unprocessed_dependency(&tx, dependencies)
                                .await?
                                .is_some(),
                            None => true,
                        };
                        if waits {
                            return Ok((Trial::Gone, Outcome::Nothing));
                        }
                        let attempt = tokio::time::timeout(
                            wait,
                            tx.atomic(async |inner| {
                                subscriber(&inner, &message)
                                    .await
                                    .map_err(Error::Subscriber)
                            }),
                        )
                        .await;
                        let failure = match attempt {
                            // The transaction rolls back: what the subscriber
                            // began is cut off mid-call, never committed.
                            Err(_elapsed) => {
                                ran_out.store(true, Ordering::Relaxed);
                                return Err(Error::Refused("the trial's wait ran out"));
                            }
                            Ok(Ok(())) => None,
                            Ok(Err(Error::Subscriber(failure))) => Some(failure),
                            Ok(Err(other)) => return Err(other),
                        };
                        let verdict = verdict_of(failure.as_ref());
                        let told = self.breaker().tried(now(), id, Trial::Told(verdict));
                        self.tell(&told.effects);
                        let outcome = match failure {
                            None => {
                                if self.slots > 1 {
                                    self.lock_own_identity(&tx, &place).await?;
                                }
                                let (processed_position, woken) =
                                    self.mark_processed(&tx, &place).await?;
                                connection
                                    .execute(
                                        &format!(
                                            "UPDATE {table}_slots SET round_from = NULL, \
                                             dormant_until = NULL, dormant_after = NULL \
                                             WHERE slot = $1"
                                        ),
                                        &[&(slot as i16)],
                                    )
                                    .await?;
                                self.observer.on_marked(&Marked {
                                    slot,
                                    message: &message,
                                    processed_position,
                                    woken: &woken,
                                });
                                Outcome::Processed
                            }
                            Some(failure) => {
                                let recorded = self
                                    .record_failure(
                                        &tx,
                                        &place,
                                        &failure,
                                        message.attempts,
                                        told.hold,
                                    )
                                    .await?;
                                Outcome::Failed {
                                    attempts: recorded.attempts,
                                    parked: recorded.parked,
                                    held: recorded.held,
                                }
                            }
                        };
                        Ok((Trial::Told(verdict), outcome))
                    })
                    .await
            })
            .await;
        match tried {
            Ok((Trial::Gone, outcome)) => {
                let told = self.breaker().tried(now(), id, Trial::Gone);
                self.tell(&told.effects);
                Ok(outcome)
            }
            Ok((_, outcome)) => Ok(outcome),
            Err(_) if ran_out.load(Ordering::Relaxed) => {
                let told = self.breaker().tried(now(), id, Trial::TimedOut);
                self.tell(&told.effects);
                Ok(Outcome::Nothing)
            }
            Err(error) => Err(error),
        }
    }

    /// The breaker's effects told: to the observer, and to the log — a pause
    /// and a resume once each, a stall at its first tick and every
    /// [`crate::breaker::REMINDER`], by channel and cause and never a Tenant
    /// (ADR-0042 decision 6).
    fn tell(&self, effects: &[Effect]) {
        for effect in effects {
            match effect {
                Effect::Paused { channel, cause } => {
                    log::warn!("inbox: the channel {channel} paused in this process ({cause})");
                }
                Effect::Stalled {
                    channel,
                    cause,
                    since,
                    remind: true,
                    ..
                } => {
                    log::error!(
                        "inbox: the channel {channel} stalled in this process ({cause}), paused for {:?}",
                        now().saturating_duration_since(*since)
                    );
                }
                Effect::Resumed { channel, cause, .. } => {
                    log::warn!(
                        "inbox: the channel {channel} asked again in this process ({cause})"
                    );
                }
                Effect::Stalled { .. } | Effect::Trial { .. } => {}
            }
            self.observer.on_breaker(&BreakerChanged { effect });
        }
    }

    /// Processes messages until `shutdown` completes, with `loops.concurrency`
    /// loops. A loop that finds nothing waits `loops.poll_interval`; one that
    /// processed, failed or set a message aside goes on at once. Shutdown is
    /// cooperative: a loop finishes its message, commits, and only then
    /// stops. A loop that meets an error of the moment — a lock cycle the
    /// server broke, a connection lost, a server going down — waits, longer
    /// with each such error in a row up to `loops.max_pause`, and goes on:
    /// its transaction was rolled back and the message comes back. On any
    /// other error of the database, a defect, the loops are all stopped and
    /// the error is returned (ADR-0010). A failing subscriber is neither: it
    /// is an [`Outcome`].
    ///
    /// Beside the loops, on a session of its own, the sweep forgets the
    /// processed rows whose channel's window has passed, once an interval of
    /// [`InboxRetention`](super::InboxRetention); see [`PgInbox::sweep`].
    /// And the expiry parks the rows whose dependency never arrived, a pass
    /// a tenth of `max_wait`, on sessions of its own: off the dispatch path,
    /// so that a dispatch's work is its slot's (ADR-0042 decision 7); see
    /// [`PgInbox::expire`]. Its defect stops the loops as theirs does.
    pub async fn run<F, Fut>(
        &self,
        subscriber: F,
        loops: Loops,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), Error>
    where
        F: Fn(&P::Session, &InboxMessage) -> Fut + Send + Sync,
        Fut: Future<Output = Result<(), Failure>> + Send,
    {
        let (stop, _) = watch::channel(false);
        let futures = (0..loops.concurrency.max(1)).map(|_| {
            let mut stopped = stop.subscribe();
            let stop = stop.clone();
            let subscriber = &subscriber;
            async move {
                // errors of the moment met in a row, for the length of the pause
                let mut passing: u32 = 0;
                loop {
                    if *stopped.borrow() {
                        return Ok(());
                    }
                    let pause = match self.dispatch(subscriber).await {
                        Ok(
                            Outcome::Processed
                            | Outcome::Failed { .. }
                            | Outcome::SetAside
                            | Outcome::Parked
                            | Outcome::Passed
                            | Outcome::Held,
                        ) => {
                            passing = 0;
                            continue;
                        }
                        Ok(Outcome::Nothing | Outcome::Paused) => {
                            passing = 0;
                            loops.poll_interval
                        }
                        Err(error) if error.is_transient() => {
                            passing += 1;
                            let pause = loops.pause_after(passing);
                            log::warn!("inbox: a loop met an error of the moment, waiting {pause:?}: {error}");
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
        let sweeper = self.sweep_until(stop.subscribe());
        let expirer = self.expire_until(&stop, loops);
        let futures = futures::future::join3(join_all(futures), sweeper, expirer);
        tokio::pin!(futures);
        let (outcomes, (), expired) = tokio::select! {
            outcomes = &mut futures => outcomes,
            _ = shutdown => {
                let _ = stop.send(true);
                futures.await
            }
        };
        outcomes.into_iter().collect::<Result<Vec<()>, Error>>()?;
        expired
    }

    /// A page of the parked rows, oldest first, after `after`'s order of
    /// arrival and at most `limit` of them, each by its place and its
    /// identity when that reads ([`Parked`]).
    pub async fn parked(
        &self,
        session: &P::Session,
        after: Option<i64>,
        limit: u32,
    ) -> Result<Vec<Parked>, Error> {
        self.parked_rows(session, after, limit).await
    }

    /// The parked rows, by channel and ground, with the oldest park: what a
    /// host's gauge pages on (ADR-0036 decision 6). The rows are read from
    /// the table, so the count holds while a row stays parked, whatever the
    /// process that parked it did since, and falls when a row leaves the
    /// state with no event a host could count: a supersede, a dependency's
    /// wake. In a transaction of its own on the inbox's pool, bounded by
    /// `within`: parked rows are never swept, so their number is the
    /// table's to grow, and a count past the bound fails rather than hold a
    /// session and the table's lock for a migration to wait behind.
    ///
    /// # Errors
    /// [`Error`] when the database cannot be read within `within`.
    pub async fn parked_counts(&self, within: Duration) -> Result<Vec<ParkedCount>, Error> {
        let sql = super::store::parked_counts_sql(self.table.as_str());
        let millis = i32::try_from(within.as_millis()).unwrap_or(i32::MAX).max(1);
        let rows = self
            .pool
            .session(async |session: P::Session| {
                session
                    .atomic(async |tx| {
                        tx.connection()
                            .batch_execute(&format!("SET LOCAL statement_timeout = {millis}"))
                            .await?;
                        Ok::<_, Error>(tx.connection().query(&sql, &[]).await?)
                    })
                    .await
            })
            .await?;
        rows.iter()
            .map(|row| {
                let ground: String = row.try_get(1)?;
                Ok(ParkedCount {
                    channel: row.try_get(0)?,
                    ground: Ground::parse(&ground).ok_or_else(|| {
                        Error::Malformed(format!(
                            "the parked ground `{ground}` is not the library's"
                        ))
                    })?,
                    cause: row.try_get(2)?,
                    open: u64::try_from(row.try_get::<_, i64>(3)?).unwrap_or_default(),
                    oldest: row.try_get(4)?,
                })
            })
            .collect()
    }

    /// The heads held, by channel, kind and cause, with how many Tenants and
    /// heads and the oldest hold (ADR-0041 decision 5, ADR-0042 decision 6).
    /// Read from the table, as [`Self::parked_counts`] reads the parked rows,
    /// in a transaction of its own bounded by `within`.
    ///
    /// # Errors
    /// [`Error`] when the database cannot be read within `within`.
    pub async fn held_counts(&self, within: Duration) -> Result<Vec<HeldCount>, Error> {
        let sql = super::store::held_counts_sql(self.table.as_str());
        let millis = i32::try_from(within.as_millis()).unwrap_or(i32::MAX).max(1);
        let rows = self
            .pool
            .session(async |session: P::Session| {
                session
                    .atomic(async |tx| {
                        tx.connection()
                            .batch_execute(&format!("SET LOCAL statement_timeout = {millis}"))
                            .await?;
                        Ok::<_, Error>(tx.connection().query(&sql, &[]).await?)
                    })
                    .await
            })
            .await?;
        rows.iter()
            .map(|row| {
                let hold: String = row.try_get(1)?;
                Ok(HeldCount {
                    channel: row.try_get(0)?,
                    hold: Hold::parse(&hold).ok_or_else(|| {
                        Error::Malformed(format!("the hold `{hold}` is not the library's"))
                    })?,
                    cause: row.try_get(2)?,
                    tenants: u64::try_from(row.try_get::<_, i64>(3)?).unwrap_or_default(),
                    heads: u64::try_from(row.try_get::<_, i64>(4)?).unwrap_or_default(),
                    oldest: row.try_get(5)?,
                })
            })
            .collect()
    }

    /// Gives the parked row at `received_position` another go: attempts
    /// reset, due at once. By its place, so that a row whose values do not
    /// read can be given one (ADR-0037 decision 6). Returns whether it was
    /// parked.
    pub async fn unpark(
        &self,
        session: &P::Session,
        received_position: i64,
    ) -> Result<bool, Error> {
        let unparked = self.unpark_at(session, received_position).await?;
        if let Some(message) = &unparked {
            self.observer.on_unparked(&Unparked { message });
        }
        Ok(unparked.is_some())
    }

    /// Marks the parked row at `received_position` processed by hand,
    /// without the subscriber's effects, and puts what waited for it back
    /// into the queue. Runs in a transaction of `session`, under the lock a
    /// mark takes (ADR-0009), its identity read by the server from the row,
    /// so a wait for it set beside is woken all the same. Returns whether it
    /// was parked.
    pub async fn resolve(
        &self,
        session: &P::Session,
        received_position: i64,
    ) -> Result<bool, Error> {
        let resolved = session
            .atomic(async |tx| {
                read_committed(&self.isolation(&tx).await?)?;
                self.lock_parked_identity(&tx, received_position).await?;
                self.resolve_at(&tx, received_position).await
            })
            .await?;
        if let Some((processed_position, message, woken)) = &resolved {
            self.observer.on_resolved(&Resolved {
                message,
                processed_position: *processed_position,
                woken,
            });
        }
        Ok(resolved.is_some())
    }
}
