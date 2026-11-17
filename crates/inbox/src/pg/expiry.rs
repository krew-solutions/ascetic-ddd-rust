//! The expiry of the rows that wait for a dependency (ADR-0042 decision 7):
//! off the dispatch path, a timer of `run` and a call a host that drives
//! `dispatch` itself makes on its own. A waiter is parked when its
//! dependency never arrived, is parked, or reaches no queued row within the
//! chain's hops, once it has waited `max_wait`; a waiter whose dependency is
//! there and on its way is spared, and costs a pass nothing but a read.

use std::time::{Duration, SystemTime};

use ascetic_ddd_session::{PgAccess, SessionPool};
use tokio::sync::watch;

use super::store::EXPIRY_BATCH;
use super::{Loops, PgInbox};
use crate::error::Error;
use crate::observer::{Expired, InboxObserver};

/// What one statement of the expiry did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Expiry {
    /// The rows it parked.
    pub parked: usize,
    /// Whether it examined a whole batch, so that its pass goes on from the
    /// row it stopped at: call again at once. Otherwise the pass has reached
    /// the waiters past `max_wait` now, and the next call begins another.
    pub more: bool,
}

/// Where this process's pass of the expiry stands: the key `(waiting_since,
/// received_position)` of the last row it examined, none between passes.
pub(super) type Cursor = std::sync::Mutex<Option<(SystemTime, i64)>>;

/// The rest between two passes of the expiry's timer: a tenth of
/// `max_wait`, so that a waiter whose dependency never arrived is parked
/// within `max_wait`, a tenth of it, and the statements of one pass.
pub(super) fn rest(max_wait: Duration) -> Duration {
    (max_wait / 10).max(Duration::from_millis(10))
}

impl<P, O> PgInbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: InboxObserver,
{
    /// One statement of the expiry, on a session of its own and outside any
    /// dispatch: it examines at most a batch of the rows that have waited
    /// longer than `max_wait` for a dependency, from where this process's
    /// pass stopped, and parks those whose dependency never arrived, is
    /// parked, or reaches no queued row within 8 hops of waiting rows, each
    /// with its reason in `last_error` and the identity it waited for kept,
    /// so that that identity's mark still wakes it (ADR-0036 R8). A waiter
    /// whose dependency is queued — held, untried, behind a held head or a
    /// long backlog — or waits on one so spared is neither written nor
    /// locked: `max_wait` means that the dependency never arrived, which
    /// none of those is (ADR-0042 decision 7). The rows parked are told to
    /// the observer, [`Expired`].
    ///
    /// [`PgInbox::run`] calls it on a timer: again at once while
    /// [`Expiry::more`], so that a waiter past any number of spared ones is
    /// reached, and after a tenth of `max_wait` otherwise. A host that drives
    /// [`PgInbox::dispatch`] itself calls it the same way. Nothing to do
    /// without a `max_wait`.
    ///
    /// # Errors
    /// [`Error`] when the database fails; the pass stands where it stood.
    pub async fn expire(&self) -> Result<Expiry, Error> {
        let Some(max_wait) = self.max_wait else {
            return Ok(Expiry {
                parked: 0,
                more: false,
            });
        };
        let after = *self
            .expired_to
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let examined = self
            .pool
            .session(async |session| self.expire_batch(&session, max_wait, after).await)
            .await?;
        let more = examined.examined >= i64::from(EXPIRY_BATCH);
        *self
            .expired_to
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            if more { examined.last } else { None };
        if !examined.parked.is_empty() {
            self.observer.on_expired(&Expired {
                messages: &examined.parked,
            });
        }
        Ok(Expiry {
            parked: examined.parked.len(),
            more,
        })
    }

    /// The expiry of `run`, until `stop` is sent: a pass at the start, its
    /// statements one after another while each examined a whole batch, then
    /// a rest of a tenth of `max_wait`. An error of the moment waits as a
    /// loop's does and goes on; a defect stops every loop and is returned,
    /// as when the expiry ran in the dispatch (ADR-0010).
    pub(super) async fn expire_until(
        &self,
        stop: &watch::Sender<bool>,
        loops: Loops,
    ) -> Result<(), Error> {
        let Some(max_wait) = self.max_wait else {
            return Ok(());
        };
        let mut stopped = stop.subscribe();
        // errors of the moment met in a row, for the length of the pause
        let mut passing: u32 = 0;
        loop {
            if *stopped.borrow() {
                return Ok(());
            }
            let pause = match self.expire().await {
                Ok(Expiry { more: true, .. }) => {
                    // the pass's next statement at once: a sleep of nothing
                    // waits for the timer's next tick, a millisecond
                    passing = 0;
                    tokio::task::yield_now().await;
                    continue;
                }
                Ok(Expiry { more: false, .. }) => {
                    passing = 0;
                    rest(max_wait)
                }
                Err(error) if error.is_transient() => {
                    passing += 1;
                    let pause = loops.error_wait_after(passing);
                    log::warn!(
                        "inbox: the expiry met an error of the moment, waiting {pause:?}: {error}"
                    );
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
}
