//! Watching an inbox: what it receives and what its dispatchers do.
//!
//! The same shape as the session and outbox observers: synchronous,
//! infallible values, composed as tuples, fixed when the inbox is built. An
//! observer observes; one that has to do IO enqueues and lets a task of its
//! own do the awaiting.
//!
//! The events are the actions of the protocol model in
//! `verify/tla/Inbox.tla` — receive, wait, fetch, fail, expire, and the close
//! of the dispatcher's transaction, plus the operator's unpark and resolve —
//! with the steps the model folds into one made visible: the row that holds
//! the slot while it waits for its backoff, the subscriber's outcome,
//! the mark and what it woke.
//!
//! A dispatcher's events name the slot it holds, and nothing else: one
//! dispatcher at a time works a slot, so the slot is its identity.
//!
//! A receipt names the transaction that stored a row, and every step of a
//! walk over the table carries the [`Snapshot`] its statement ran under, so
//! that a recorded run says which rows each walk could see. The order events
//! were logged in is the order the observer was called in, on one thread;
//! two tasks' events may be logged in the other order than the database saw
//! them, and the snapshots settle it.

use std::sync::Arc;
use std::time::Duration;

use crate::error::{BoxError, Error};
use crate::intake::Reason;
use crate::message::{CausalDependency, InboxMessage};
use crate::pg::Outcome;
use crate::snapshot::Snapshot;

/// What the inbox knows about a row once it is stored: the transaction that
/// wrote it and its order of arrival.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Receipt {
    /// `pg_current_xact_id()` of the storing transaction.
    pub transaction_id: u64,
    /// The row's order of arrival.
    pub received_position: i64,
    /// The slot the row landed in, by the table's cut.
    pub slot: u32,
}

/// A message was handed to the inbox, in a transaction of its own.
pub struct Received<'a> {
    /// The message as handed over.
    pub message: &'a InboxMessage,
    /// Where it landed; `None` when a message of the same identity was
    /// already stored and this one was ignored.
    pub receipt: Option<Receipt>,
    /// When this one was ignored, the `message_id` of the one stored under
    /// its identity (ADR-0030): this one's own for a redelivery, another's
    /// for a second message the producer gave one identity — which a
    /// channel whose producers never reuse an identity takes for a defect,
    /// and one that republishes for a repeat. `None` when stored, or when
    /// the stored row names no id. A holder gone before it could be read
    /// is no longer reported: the message is tried again (ADR-0036 R2).
    pub held_by: Option<&'a str>,
}

/// A call of the operator's surface of the rows set aside, reads included,
/// told once it has committed, so that a host audits who did what
/// (ADR-0036 R7).
pub struct Operated<'a> {
    /// Who called, as the host named them.
    pub principal: &'a str,
    /// What was done.
    pub operation: Operation,
    /// The channel, when the call named one.
    pub channel: Option<&'a str>,
    /// The row's digest, when the call named one.
    pub digest: Option<&'a str>,
    /// How many rows the call read or changed.
    pub rows: u64,
}

/// What a call of the operator's surface did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    /// Counted the open rows set aside.
    Counted,
    /// Read a page of them.
    Paged,
    /// Read one row's kept bytes.
    Read,
    /// Took a row's message through the intake again.
    Readmitted,
    /// Closed a row as discarded, a tombstone.
    Discarded,
}

/// The intake set a message aside beside the inbox, never storing it as a
/// row (ADR-0036): told once the row set aside commits.
pub struct Rejected<'a> {
    /// The channel it arrived on, without its key.
    pub channel: &'a str,
    /// Its Tenant's header, as the derived column keeps it: UTF-8, no NUL,
    /// bounded; none when it was missing or not UTF-8. It is the producer's
    /// claim: an observer that names it checks it is a Tenant's id first.
    pub tenant_id: Option<&'a str>,
    /// Its stream type's header, likewise.
    pub stream_type: Option<&'a str>,
    /// Why it was set aside.
    pub reason: Reason,
    /// The digest it is kept under.
    pub digest: &'a str,
    /// Whether the same bytes had been set aside before.
    pub repeat: bool,
}

/// A dispatcher found the head of its queue depending on a message not yet
/// processed, and set it aside to wait for that one, out of the queue; the
/// transaction that marks the dependency processed puts it back.
pub struct Waiting<'a> {
    /// The slot the dispatcher holds.
    pub slot: u32,
    /// The row.
    pub message: &'a InboxMessage,
    /// The first of its dependencies found unprocessed.
    pub dependency: &'a CausalDependency,
    /// What the statement that returned the row could see.
    pub snapshot: &'a Snapshot,
}

/// A dispatcher took a row, locked for the length of its transaction; or
/// took nothing.
pub struct Fetched<'a> {
    /// The slot the dispatcher holds; `None` when no slot had a due head
    /// that nobody held.
    pub slot: Option<u32>,
    /// How many slots the table is cut into.
    pub slots: u32,
    /// The row about to be handed to the subscriber; `None` when no slot was
    /// taken, or when the slot taken had no due head once it was read after
    /// the lock. A head set aside for a dependency is a [`Waiting`], not a
    /// fetch.
    pub message: Option<&'a InboxMessage>,
    /// What the statement that returned the row, or found none, could see.
    pub snapshot: &'a Snapshot,
}

/// The subscriber was given the message and the dispatcher's transaction.
pub struct Handled<'a> {
    /// The slot the dispatcher holds.
    pub slot: u32,
    /// The message.
    pub message: &'a InboxMessage,
    /// What the subscriber returned.
    pub outcome: Result<(), &'a BoxError>,
}

/// The message was marked processed, inside the dispatcher's transaction:
/// not durable until [`Dispatched`] reports a commit.
pub struct Marked<'a> {
    /// The slot the dispatcher holds.
    pub slot: u32,
    /// The message.
    pub message: &'a InboxMessage,
    /// Its order of processing.
    pub processed_position: i64,
    /// The rows that waited for this message, back in the queue by the same
    /// statement.
    pub woken: &'a [InboxMessage],
}

/// The head of a slot was parked at once in the dispatcher's transaction,
/// under its lock: its values could not be read, or its wait, its failure or
/// its mark could not be written, for the values themselves (ADR-0037
/// decision 6). Its subscriber did not run, or its outcome was not kept.
/// It is named by its place, since its values may be what could not be
/// read.
pub struct ParkedAtOnce<'a> {
    /// The slot the dispatcher holds.
    pub slot: u32,
    /// The row's order of arrival.
    pub received_position: i64,
    /// The channel it arrived on, without its key: the inbox's own name,
    /// read by the statement that parked it, never from the row's values.
    pub channel: &'a str,
    /// Why, in the library's words.
    pub reason: &'static str,
    /// The snapshot the head was located under.
    pub snapshot: &'a Snapshot,
}

/// Rows whose wait for a dependency ran out were parked, with the dependency
/// named in their `last_error`, inside the dispatcher's transaction.
pub struct Expired<'a> {
    /// The rows, oldest first.
    pub messages: &'a [InboxMessage],
}

/// The subscriber failed: its writes were rolled back to the savepoint and
/// the attempt was recorded, inside the dispatcher's transaction. Not
/// durable until [`Dispatched`] reports a commit.
pub struct Failed<'a> {
    /// The slot the dispatcher holds.
    pub slot: u32,
    /// The message.
    pub message: &'a InboxMessage,
    /// What the subscriber returned.
    pub error: &'a BoxError,
    /// The subscriber's verdict that no retry will succeed (ADR-0010).
    pub permanent: bool,
    /// Failed attempts so far, this one included.
    pub attempts: u32,
    /// Whether this attempt was the last: the message is parked, its
    /// attempts run out or the failure permanent.
    pub parked: bool,
    /// How long the message waits before it may be taken again.
    pub retry_after: Duration,
}

/// The dispatcher's transaction closed: committed, with what it did, or
/// rolled back, with the error.
pub struct Dispatched<'a> {
    /// The slot the dispatcher held; `None` when it took none.
    pub slot: Option<u32>,
    /// `Ok`: what was committed; `Err`: nothing was, and whatever row was
    /// held is free again.
    pub outcome: Result<Outcome, &'a Error>,
}

/// An operator gave a parked message another go.
pub struct Unparked<'a> {
    /// The message.
    pub message: &'a InboxMessage,
}

/// An operator marked a parked message processed, without the subscriber's
/// effects.
pub struct Resolved<'a> {
    /// The message.
    pub message: &'a InboxMessage,
    /// Its order of processing.
    pub processed_position: i64,
    /// The rows that waited for this message, back in the queue by the same
    /// statement.
    pub woken: &'a [InboxMessage],
}

/// The sweep forgot processed rows of a channel whose window had passed
/// (ADR-0033 decision 4).
pub struct Swept<'a> {
    /// The channel: the destination without its key.
    pub channel: &'a str,
    /// How many rows it forgot in this pass.
    pub forgotten: u64,
}

/// Declares the observer's trait and its compositions from one list of
/// methods, so that a method of the trait cannot be left out of a composed
/// observer, which would drop its events silently (ADR-0036 R9).
macro_rules! observer {
    (
        $(#[$meta:meta])*
        pub trait $trait:ident {
            $($(#[$doc:meta])* fn $method:ident(&self, _event: &$event:ident<'_>);)*
        }
    ) => {
        $(#[$meta])*
        pub trait $trait: Send + Sync {
            $($(#[$doc])* fn $method(&self, _event: &$event<'_>) {})*
        }

        /// Two observers, told in order.
        impl<A: $trait, B: $trait> $trait for (A, B) {
            $(fn $method(&self, event: &$event<'_>) {
                self.0.$method(event);
                self.1.$method(event);
            })*
        }

        impl<O: $trait + ?Sized> $trait for Arc<O> {
            $(fn $method(&self, event: &$event<'_>) {
                (**self).$method(event);
            })*
        }

        /// Any number of observers, told in order.
        impl<O: $trait> $trait for Vec<O> {
            $(fn $method(&self, event: &$event<'_>) {
                self.iter().for_each(|o| o.$method(event));
            })*
        }
    };
}

observer! {
    /// What an inbox reports. Every method has an empty default, so an
    /// observer implements only what it cares about.
    pub trait InboxObserver {
    /// A message was handed to the inbox.
    fn on_received(&self, _event: &Received<'_>);
    /// The head of the queue was set aside to wait for a dependency.
    fn on_waiting(&self, _event: &Waiting<'_>);
    /// A dispatcher took a row, or found none.
    fn on_fetched(&self, _event: &Fetched<'_>);
    /// The subscriber returned.
    fn on_handled(&self, _event: &Handled<'_>);
    /// The message was marked processed.
    fn on_marked(&self, _event: &Marked<'_>);
    /// The attempt was recorded after the subscriber failed.
    fn on_failed(&self, _event: &Failed<'_>);
    /// The head was parked at once for its own values.
    fn on_parked_at_once(&self, _event: &ParkedAtOnce<'_>);
    /// Waits ran out; the rows were parked.
    fn on_expired(&self, _event: &Expired<'_>);
    /// The dispatcher's transaction closed.
    fn on_dispatched(&self, _event: &Dispatched<'_>);
    /// A parked message was given another go.
    fn on_unparked(&self, _event: &Unparked<'_>);
    /// A parked message was marked processed by hand.
    fn on_resolved(&self, _event: &Resolved<'_>);
    /// The sweep forgot processed rows of a channel.
    fn on_swept(&self, _event: &Swept<'_>);
    /// The intake set a message aside.
    fn on_rejected(&self, _event: &Rejected<'_>);
    /// The operator's surface of the rows set aside was called.
    fn on_operated(&self, _event: &Operated<'_>);
}
}

/// Observes nothing.
impl InboxObserver for () {}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "tests")]
mod tests {
    use std::sync::Mutex;

    use super::*;

    /// Notes the name of every method it is told, after its own label.
    struct Noting(&'static str, Arc<Mutex<Vec<String>>>);

    macro_rules! noting {
        ($($method:ident: $event:ident),* $(,)?) => {
            impl InboxObserver for Noting {
                $(fn $method(&self, _event: &$event<'_>) {
                    self.1.lock().unwrap().push(format!("{}.{}", self.0, stringify!($method)));
                })*
            }
        };
    }

    noting! {
        on_received: Received, on_waiting: Waiting, on_fetched: Fetched,
        on_handled: Handled, on_marked: Marked, on_failed: Failed,
        on_expired: Expired, on_dispatched: Dispatched, on_unparked: Unparked,
        on_resolved: Resolved, on_swept: Swept, on_rejected: Rejected,
        on_operated: Operated, on_parked_at_once: ParkedAtOnce,
    }

    /// Tells `observer` one event of each kind, in the trait's order.
    fn tell_each(observer: &impl InboxObserver) {
        let message = InboxMessage::new("t1", "orders.Order", "o-1".into(), 1, "inbox://o", "p");
        let snapshot: Snapshot = "10:10:".parse().unwrap();
        let dependency = CausalDependency::on(&message);
        let error: BoxError = "refused".into();
        let none = std::slice::from_ref(&message);
        observer.on_received(&Received {
            message: &message,
            receipt: None,
            held_by: None,
        });
        observer.on_waiting(&Waiting {
            slot: 0,
            message: &message,
            dependency: &dependency,
            snapshot: &snapshot,
        });
        observer.on_fetched(&Fetched {
            slot: None,
            slots: 1,
            message: None,
            snapshot: &snapshot,
        });
        observer.on_handled(&Handled {
            slot: 0,
            message: &message,
            outcome: Ok(()),
        });
        observer.on_marked(&Marked {
            slot: 0,
            message: &message,
            processed_position: 1,
            woken: none,
        });
        observer.on_failed(&Failed {
            slot: 0,
            message: &message,
            error: &error,
            permanent: false,
            attempts: 1,
            parked: false,
            retry_after: Duration::ZERO,
        });
        observer.on_expired(&Expired { messages: none });
        observer.on_dispatched(&Dispatched {
            slot: None,
            outcome: Ok(Outcome::Nothing),
        });
        observer.on_unparked(&Unparked { message: &message });
        observer.on_resolved(&Resolved {
            message: &message,
            processed_position: 1,
            woken: none,
        });
        observer.on_swept(&Swept {
            channel: "inbox://o",
            forgotten: 1,
        });
        observer.on_rejected(&Rejected {
            channel: "inbox://o",
            tenant_id: None,
            stream_type: None,
            reason: Reason::Malformed,
            digest: "d",
            repeat: false,
        });
        observer.on_operated(&Operated {
            principal: "p",
            operation: Operation::Counted,
            channel: None,
            digest: None,
            rows: 0,
        });
        observer.on_parked_at_once(&ParkedAtOnce {
            slot: 0,
            received_position: 1,
            channel: "inbox://o",
            reason: "unreadable",
            snapshot: &snapshot,
        });
    }

    const METHODS: [&str; 14] = [
        "on_received",
        "on_waiting",
        "on_fetched",
        "on_handled",
        "on_marked",
        "on_failed",
        "on_expired",
        "on_dispatched",
        "on_unparked",
        "on_resolved",
        "on_swept",
        "on_rejected",
        "on_operated",
        "on_parked_at_once",
    ];

    /// What `labels`, told in order, note for each event in turn.
    fn expected(labels: &[&str]) -> Vec<String> {
        METHODS
            .iter()
            .flat_map(|method| labels.iter().map(move |label| format!("{label}.{method}")))
            .collect()
    }

    /// Every event reaches every observer of a tuple, an `Arc` and a `Vec`,
    /// in order (ADR-0036 R9).
    #[test]
    fn a_composed_observer_tells_each_of_its_own_every_event() {
        let noted = Arc::new(Mutex::new(Vec::new()));
        let note = |label| Noting(label, Arc::clone(&noted));
        let taken = |noted: &Arc<Mutex<Vec<String>>>| std::mem::take(&mut *noted.lock().unwrap());

        tell_each(&(note("a"), note("b")));
        assert_eq!(taken(&noted), expected(&["a", "b"]));

        tell_each(&Arc::new(note("a")));
        assert_eq!(taken(&noted), expected(&["a"]));

        tell_each(&vec![note("a"), note("b"), note("c")]);
        assert_eq!(taken(&noted), expected(&["a", "b", "c"]));

        let nested: Arc<dyn InboxObserver> = Arc::new((note("a"), vec![note("b")]));
        tell_each(&nested);
        assert_eq!(taken(&noted), expected(&["a", "b"]));
    }
}
