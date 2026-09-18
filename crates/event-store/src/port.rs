//! The port: what a command workflow sees.

use ascetic_ddd_session::Session;

use crate::domain::{Appended, EventMeta, Recorded, StreamId};
use crate::error::Error;

/// A stream of events per aggregate, in the caller's transaction.
///
/// The workflow loads the stream, folds it into the aggregate's state,
/// decides, and appends what it decided after the position it loaded up to:
/// `load`, fold, decide, `append`. Another writer's events in between are
/// [`Error::ConcurrentUpdate`], and the workflow starts over. Publishing
/// what was appended — through the outbox, in the same transaction — is the
/// workflow's too; the store stores.
pub trait EventStore<S: Session, E>: Send + Sync {
    /// Appends `events` to `stream` after position `expected` — the last
    /// position loaded, zero for a stream that does not exist yet — with
    /// `meta` on each: an id is minted per event, and each event's cause is
    /// the one before it, the first's being what `meta` names. Returns what
    /// was recorded, in order.
    fn append(
        &self,
        session: &S,
        stream: &StreamId,
        expected: i32,
        events: &[E],
        meta: &EventMeta,
    ) -> impl Future<Output = Result<Vec<Appended>, Error>> + Send;

    /// The events of `stream` after position `since`, in order; zero for
    /// the whole stream.
    fn load(
        &self,
        session: &S,
        stream: &StreamId,
        since: i32,
    ) -> impl Future<Output = Result<Vec<Recorded<E>>, Error>> + Send;
}
